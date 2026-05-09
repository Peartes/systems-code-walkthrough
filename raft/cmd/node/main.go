package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/peartes/raft/internal/raft"
	"github.com/peartes/raft/internal/telemetry"
	"github.com/peartes/raft/internal/transport"
	raftpb "github.com/peartes/raft/proto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func main() {
	var (
		id         int
		configPath string
		dataDir    string
	)
	flag.IntVar(&id, "id", -1, "this node's id (must match a peer.id in config.json)")
	flag.StringVar(&configPath, "config", "config.json", "path to config.json")
	flag.StringVar(&dataDir, "data-dir", "data", "directory for persisted state files")
	flag.Parse()

	if id < 0 {
		log.Fatal("--id is required and must be >= 0")
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	self := cfg.peerByID(id)
	if self == nil {
		log.Fatalf("--id %d not found in %s", id, configPath)
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	// ---- partition flag ----
	// One atomic.Bool shared by both ends of the partition simulation:
	//   - PartitionInterceptor reads it on every inbound Raft.* RPC
	//     and rejects when true.
	//   - Every GRPCPeer reads it before sending and short-circuits when
	//     true.
	// Toggled by RaftControl.SetPartitioned. The single shared variable
	// is what makes the drop symmetric (both sides go silent at once).
	var partitioned atomic.Bool

	// ---- peers slice ----
	// Build []raft.Peer where peers[id] == nil and every other entry is a
	// GRPCPeer dialing the configured address. grpc.NewClient is lazy, so
	// missing peers don't block startup — they just fail their first RPC
	// and the engine retries.
	peers := make([]raft.Peer, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.ID == id {
			continue
		}
		gp, err := transport.NewGRPCPeer(p.GRPCAddr, &partitioned)
		if err != nil {
			log.Fatalf("dial peer %d at %s: %v", p.ID, p.GRPCAddr, err)
		}
		peers[p.ID] = gp
	}

	// ---- persister ----
	persister, err := raft.NewFilePersister(
		filepath.Join(dataDir, fmt.Sprintf("node-%d.raft", id)),
		filepath.Join(dataDir, fmt.Sprintf("node-%d.snap", id)),
	)
	if err != nil {
		log.Fatalf("init persister: %v", err)
	}

	// ---- raft instance ----
	applyChan := make(chan raft.ApplyMsg, 256)
	rf := raft.Make(peers, id, applyChan, persister)

	// ---- telemetry ----
	// The current telemetry/provider.go uses the OTel Prometheus exporter,
	// which registers metrics in the default Prometheus registry. We then
	// expose them via promhttp on metrics_addr. Conversation 4 will swap
	// this to a richer setup with a dedicated /metrics binary; for now this
	// is enough to scrape against.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider, shutdownMetrics, err := telemetry.NewMeterProvider("raft-"+strconv.Itoa(id), ctx, 2*time.Second)
	if err != nil {
		log.Fatalf("telemetry: %v", err)
	}
	defer shutdownMetrics(context.Background())
	rm, err := telemetry.NewRaftMetrics(provider, strconv.Itoa(id))
	if err != nil {
		log.Fatalf("raft metrics: %v", err)
	}
	rf.SetMetrics(rm)

	// ---- applyChan drainer ----
	// Real state-machine integration lives in the kv/ package; for the
	// node binary we just log applied entries so a human can see them
	// flow during the manual cluster test.
	go func() {
		for msg := range applyChan {
			if msg.Snapshot != nil {
				log.Printf("[node %d] APPLY snapshot @ idx=%d term=%d (%dB)", id, msg.Index, msg.Term, len(msg.Snapshot))
				continue
			}
			log.Printf("[node %d] APPLY idx=%d term=%d cmd=%q", id, msg.Index, msg.Term, msg.Entry.Entry)
		}
	}()

	// ---- HTTP /metrics endpoint ----
	if self.MetricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			log.Printf("[node %d] metrics on http://%s/metrics", id, self.MetricsAddr)
			if err := http.ListenAndServe(self.MetricsAddr, mux); err != nil {
				log.Printf("[node %d] metrics server: %v", id, err)
			}
		}()
	}

	// ---- gRPC server ----
	listener, err := net.Listen("tcp", self.GRPCAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", self.GRPCAddr, err)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(transport.PartitionInterceptor(&partitioned)))
	raftpb.RegisterRaftServer(server, transport.NewRaftServer(rf))
	raftpb.RegisterRaftControlServer(server, transport.NewControlServer(rf, &partitioned))

	go func() {
		log.Printf("[node %d] gRPC on %s (api=%s)", id, self.GRPCAddr, self.APIAddr)
		if err := server.Serve(listener); err != nil {
			log.Printf("[node %d] gRPC server: %v", id, err)
		}
	}()

	// ---- graceful shutdown ----
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("[node %d] shutting down", id)

	server.GracefulStop()
	rf.Kill()
	for _, p := range peers {
		if gp, ok := p.(*transport.GRPCPeer); ok {
			_ = gp.Close()
		}
	}
}
