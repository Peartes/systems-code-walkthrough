// Dashboard is a thin HTTP/JSON proxy that fronts each node's gRPC
// RaftControl service so a browser app can poll, submit commands, and
// toggle partition state without speaking gRPC-Web.
//
// Routes (all under /api):
//
//	GET  /api/peers                    -> list peers from config.json
//	POST /api/seed   { count }         -> rewrite config.json with N peers
//	GET  /api/nodes/{id}/state         -> RaftControl.GetState
//	POST /api/nodes/{id}/command       -> RaftControl.SubmitCommand
//	POST /api/nodes/{id}/partition     -> RaftControl.SetPartitioned
//
// The dashboard does NOT supervise node processes — start/stop those
// via `make nodes-up` / `make nodes-down`. The /api/seed endpoint only
// rewrites config.json so a freshly-cloned repo can boot the cluster
// shape from the UI before the user runs the lifecycle Make targets.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	raftpb "github.com/peartes/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type peerEntry struct {
	ID          int    `json:"id"`
	GRPCAddr    string `json:"grpc_addr"`
	APIAddr     string `json:"api_addr"`
	MetricsAddr string `json:"metrics_addr"`
}

type config struct {
	Peers []peerEntry `json:"peers"`
}

// nodeRouter caches gRPC clients per node ID. Connections are created
// lazily on the first request that targets a node and reused for the
// lifetime of the dashboard process. grpc.NewClient handles reconnects
// transparently if the node restarts.
//
// configPath is retained so /api/seed can rewrite the file in place; the
// router also resets its in-memory client cache after a successful seed
// so subsequent /api/nodes/{id}/* hits dial the new addresses cleanly.
//
// promTargetsPath, when non-empty, is the JSON file Prometheus reads via
// file_sd. We rewrite it on every seed so the cluster's scrape targets
// stay in sync with config.json without static caps or container reloads.
type nodeRouter struct {
	mu              sync.Mutex
	cfg             *config
	configPath      string
	promTargetsPath string
	clients         map[int]raftpb.RaftControlClient
	conns           map[int]*grpc.ClientConn
}

func newNodeRouter(cfg *config, configPath, promTargetsPath string) *nodeRouter {
	return &nodeRouter{
		cfg:             cfg,
		configPath:      configPath,
		promTargetsPath: promTargetsPath,
		clients:         make(map[int]raftpb.RaftControlClient),
		conns:           make(map[int]*grpc.ClientConn),
	}
}

func (r *nodeRouter) clientFor(id int) (raftpb.RaftControlClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clients[id]; ok {
		return c, nil
	}
	var addr string
	for _, p := range r.cfg.Peers {
		if p.ID == id {
			addr = p.GRPCAddr
			break
		}
	}
	if addr == "" {
		return nil, fmt.Errorf("unknown node id %d", id)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial node %d: %w", id, err)
	}
	c := raftpb.NewRaftControlClient(conn)
	r.clients[id] = c
	r.conns[id] = conn
	return c, nil
}

// ----- HTTP handlers -----

type stateResp struct {
	ID          int      `json:"id"`
	Term        int64    `json:"term"`
	Role        int32    `json:"role"`
	CommitIndex int64    `json:"commitIndex"`
	LastApplied int64    `json:"lastApplied"`
	Logs        []logEnt `json:"logs"`
	Reachable   bool     `json:"reachable"`
}

type logEnt struct {
	Term  int64  `json:"term"`
	Entry string `json:"entry"` // utf-8 string view of the bytes
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parseNodeID(path string) (int, string, bool) {
	// expects /api/nodes/<id>/<rest>
	const prefix = "/api/nodes/"
	if !strings.HasPrefix(path, prefix) {
		return 0, "", false
	}
	rest := path[len(prefix):]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return 0, "", false
	}
	id, err := strconv.Atoi(rest[:slash])
	if err != nil {
		return 0, "", false
	}
	return id, rest[slash+1:], true
}

func (r *nodeRouter) handlePeers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, r.cfg.Peers)
}

func (r *nodeRouter) handleState(w http.ResponseWriter, req *http.Request, id int) {
	c, err := r.clientFor(id)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), 1500*time.Millisecond)
	defer cancel()
	resp, err := c.GetState(ctx, &raftpb.GetStateReq{})
	if err != nil {
		// Treat as unreachable rather than an error — the UI fades the
		// circle when reachable=false instead of showing a red error.
		writeJSON(w, http.StatusOK, stateResp{ID: id, Reachable: false})
		return
	}
	logs := make([]logEnt, len(resp.Logs))
	for i, l := range resp.Logs {
		logs[i] = logEnt{Term: l.Term, Entry: string(l.Entry)}
	}
	writeJSON(w, http.StatusOK, stateResp{
		ID:          id,
		Term:        resp.Term,
		Role:        resp.Role,
		CommitIndex: resp.CommitIndex,
		LastApplied: resp.LastApplied,
		Logs:        logs,
		Reachable:   true,
	})
}

func (r *nodeRouter) handleCommand(w http.ResponseWriter, req *http.Request, id int) {
	var body struct {
		Command string `json:"command"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	c, err := r.clientFor(id)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
	defer cancel()
	resp, err := c.SubmitCommand(ctx, &raftpb.SubmitCommandReq{Command: []byte(body.Command)})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"index":    resp.Index,
		"term":     resp.Term,
		"isLeader": resp.IsLeader,
	})
}

func (r *nodeRouter) handlePartition(w http.ResponseWriter, req *http.Request, id int) {
	var body struct {
		Partitioned bool `json:"partitioned"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	c, err := r.clientFor(id)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
	defer cancel()
	if _, err := c.SetPartitioned(ctx, &raftpb.SetPartitionedReq{Partitioned: body.Partitioned}); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"partitioned": body.Partitioned})
}

// MaxSeedCount caps cluster size at a value that's safely below the
// port-overlap point. With our addressing scheme (grpc :18500+id, api
// :18600+id, metrics :18700+id) collisions begin at id=100, so 25 is a
// generous demo headroom. Prometheus uses file-based service discovery
// and doesn't need a static target list — it picks up whatever the seed
// handler writes.
const MaxSeedCount = 25

func (r *nodeRouter) handleSeed(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if body.Count < 1 || body.Count > MaxSeedCount {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("count must be between 1 and %d", MaxSeedCount),
		})
		return
	}

	// Build N peer entries with sequential, predictable port allocation:
	//   id=N → grpc :18500+N, api :18600+N, metrics :18700+N
	// matches the existing convention used by the demo cluster.
	peers := make([]peerEntry, body.Count)
	for i := 0; i < body.Count; i++ {
		peers[i] = peerEntry{
			ID:          i,
			GRPCAddr:    fmt.Sprintf("127.0.0.1:%d", 18500+i),
			APIAddr:     fmt.Sprintf("127.0.0.1:%d", 18600+i),
			MetricsAddr: fmt.Sprintf("127.0.0.1:%d", 18700+i),
		}
	}
	newCfg := &config{Peers: peers}

	// Write atomically so a crash mid-write can't leave the file in a
	// half-formed state that breaks the next dashboard restart.
	data, err := json.MarshalIndent(newCfg, "", "  ")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	tmp := r.configPath + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := os.Rename(tmp, r.configPath); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Reset cached gRPC clients — old addresses might no longer be valid
	// (different node count, different port spread).
	r.mu.Lock()
	r.cfg = newCfg
	for _, conn := range r.conns {
		_ = conn.Close()
	}
	r.clients = make(map[int]raftpb.RaftControlClient)
	r.conns = make(map[int]*grpc.ClientConn)
	r.mu.Unlock()

	// Update Prometheus's file_sd target list. Best-effort: if we're
	// running outside the repo or the directory isn't writable, log and
	// continue — config.json is the canonical source, and Prometheus is
	// optional infrastructure.
	if err := writePromTargets(r.promTargetsPath, peers); err != nil {
		log.Printf("[dashboard] WARN write prom targets %q: %v", r.promTargetsPath, err)
	}

	log.Printf("[dashboard] seeded config.json with %d peers", body.Count)
	writeJSON(w, http.StatusOK, peers)
}

// writePromTargets emits a Prometheus file_sd JSON document at path.
// When the file is mounted into the prometheus container (see
// deploy/observability/docker-compose.yml) the new targets are picked
// up within `refresh_interval` seconds — no Prometheus reload required.
//
// `127.0.0.1` and `localhost` in metrics_addr are rewritten to
// `host.docker.internal` because Prometheus runs in a container; the
// Raft nodes run on the host.
func writePromTargets(path string, peers []peerEntry) error {
	if path == "" {
		return nil
	}
	targets := make([]string, len(peers))
	for i, p := range peers {
		host := strings.Replace(p.MetricsAddr, "127.0.0.1", "host.docker.internal", 1)
		host = strings.Replace(host, "localhost", "host.docker.internal", 1)
		targets[i] = host
	}
	doc := []map[string]any{
		{
			"targets": targets,
			"labels":  map[string]string{"job_seeded_at": time.Now().UTC().Format(time.RFC3339)},
		},
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// withCORS wraps a handler with permissive CORS for the Vite dev server.
// Tightened later if/when the UI is served from the same origin.
func withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h(w, r)
	}
}

func main() {
	var (
		listen          string
		configPath      string
		promTargetsPath string
	)
	flag.StringVar(&listen, "listen", ":8080", "HTTP listen address")
	flag.StringVar(&configPath, "config", "config.json", "path to cluster config.json")
	flag.StringVar(&promTargetsPath, "prom-targets",
		"deploy/observability/prometheus/targets/raft.json",
		"path to write Prometheus file_sd targets (\"\" disables)")
	flag.Parse()

	// A missing config.json is OK — the user will seed it from the UI.
	// Anything else (perms error, malformed JSON) is fatal as before.
	var cfg config
	if b, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(b, &cfg); err != nil {
			log.Fatalf("parse config: %v", err)
		}
	} else if !os.IsNotExist(err) {
		log.Fatalf("read config: %v", err)
	}

	router := newNodeRouter(&cfg, configPath, promTargetsPath)

	// On startup, if config.json already has peers, mirror them to the
	// Prometheus targets file so a `make observe` after a previous seed
	// scrapes the right ports without requiring a re-seed click.
	if len(cfg.Peers) > 0 {
		if err := writePromTargets(promTargetsPath, cfg.Peers); err != nil {
			log.Printf("[dashboard] WARN seed prom targets on startup: %v", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/peers", withCORS(router.handlePeers))
	mux.HandleFunc("/api/seed", withCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		router.handleSeed(w, r)
	}))
	mux.HandleFunc("/api/nodes/", withCORS(func(w http.ResponseWriter, r *http.Request) {
		id, sub, ok := parseNodeID(r.URL.Path)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
			return
		}
		switch {
		case sub == "state" && r.Method == http.MethodGet:
			router.handleState(w, r, id)
		case sub == "command" && r.Method == http.MethodPost:
			router.handleCommand(w, r, id)
		case sub == "partition" && r.Method == http.MethodPost:
			router.handlePartition(w, r, id)
		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		}
	}))

	log.Printf("[dashboard] listening on %s, %d peers in %s", listen, len(cfg.Peers), configPath)
	if err := http.ListenAndServe(listen, mux); err != nil {
		log.Fatalf("listen: %v", err)
	}
}
