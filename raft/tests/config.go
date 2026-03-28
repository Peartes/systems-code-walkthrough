package tests

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/peartes/raft/internal/raft"
	"github.com/peartes/raft/tests/rpc"
)

// config is the test harness for a Raft cluster.
// It owns the simulated network, all Raft instances, and the
// RPC servers that expose them. Tests drive the cluster through
// this object — they never touch Raft internals directly except
// through GetState() and Start().
type config struct {
	t         *testing.T
	mu        sync.Mutex
	n         int
	net       *rpc.Network
	rafts     []*raft.Raft
	servers   []*rpc.Server    // one rpc.Server per Raft instance
	endnames  [][]string       // endnames[i][j] = name of the ClientEnd that server i uses to reach server j
	peers     [][]*rpc.ClientEnd // peers[i][j] = the actual ClientEnd object (nil when i==j)
	connected []bool           // connected[i] = is server i currently participating in the network?
}

// make_config creates an n-node Raft cluster wired through a simulated network.
// If unreliable is true the network will randomly drop and delay messages.
// Call cfg.cleanup() at the end of every test.
func make_config(t *testing.T, n int, unreliable bool) *config {
	cfg := &config{
		t:         t,
		n:         n,
		net:       rpc.NewNetwork(),
		rafts:     make([]*raft.Raft, n),
		servers:   make([]*rpc.Server, n),
		endnames:  make([][]string, n),
		peers:     make([][]*rpc.ClientEnd, n),
		connected: make([]bool, n),
	}
	cfg.net.Reliable(!unreliable)

	// Phase 1: create all ClientEnds up-front.
	//
	// endnames[i][j] is the name server i uses to call server j.
	// We register the owner (i) immediately so DisableServer works correctly
	// when we later crash or partition server i.
	for i := 0; i < n; i++ {
		cfg.endnames[i] = make([]string, n)
		cfg.peers[i] = make([]*rpc.ClientEnd, n)
		for j := 0; j < n; j++ {
			if i == j {
				continue // a server never calls itself via RPC
			}
			name := fmt.Sprintf("end-%d-%d", i, j)
			cfg.endnames[i][j] = name
			cfg.peers[i][j] = cfg.net.MakeEnd(name)
			cfg.net.SetEndOwner(name, fmt.Sprintf("s%d", i))
		}
	}

	// Phase 2: start all Raft instances and register them as RPC services.
	// Servers must exist in the network before we wire ClientEnds to them.
	for i := 0; i < n; i++ {
		cfg.startRaft(i)
	}

	// Phase 3: wire every ClientEnd to its destination server.
	// We do this after Phase 2 because rpc.Network.Connect fatals if
	// the destination server does not yet exist.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			cfg.net.Connect(cfg.endnames[i][j], fmt.Sprintf("s%d", j))
		}
	}

	// All servers start connected.
	for i := 0; i < n; i++ {
		cfg.connected[i] = true
	}

	return cfg
}

// startRaft creates a fresh Raft instance for server i, registers it as an
// RPC service, and adds it to the network. Safe to call both on initial
// setup and after a crash to simulate a restart.
//
// On restart the ClientEnds in cfg.peers[i] are reused — they are permanent
// fixtures in the network. Only the Raft instance and its rpc.Server are new.
func (cfg *config) startRaft(i int) {
	serverName := fmt.Sprintf("s%d", i)

	// Build the []raft.Peer slice. cfg.peers[i][j] is a *rpc.ClientEnd,
	// which satisfies the raft.Peer interface via its Call method.
	// peers[i] (self) remains nil — Raft never sends RPCs to itself.
	peers := make([]raft.Peer, cfg.n)
	for j := 0; j < cfg.n; j++ {
		if j != i {
			peers[j] = cfg.peers[i][j]
		}
	}

	cfg.rafts[i] = raft.Make(peers, i)

	// Register the Raft instance as an RPC service so that incoming
	// RequestVote and AppendEntries calls dispatched by rpc.Network
	// reach the right handler methods.
	srv := rpc.NewServer(serverName)
	srv.AddService(rpc.NewService(cfg.rafts[i]))
	cfg.servers[i] = srv
	cfg.net.AddServer(serverName, srv)
}

// crash simulates a server failure.
//
// The difference from disconnect:
//   - crash removes the server from the network entirely (inbound fails at routing)
//   - crash kills the Raft instance (its goroutines stop)
//   - restart re-creates the Raft instance from scratch, simulating a reboot
//
// disconnect merely severs the network links but leaves the Raft instance
// running — simulating a live-but-isolated node (network partition).
func (cfg *config) crash(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cfg.connected[i] = false
	serverName := fmt.Sprintf("s%d", i)

	// Remove from routing table so inbound RPCs fail immediately.
	cfg.net.DeleteServer(serverName)

	// Disable all ClientEnds owned by i (outbound) and all ClientEnds
	// pointing to i (inbound) — belt-and-suspenders with DeleteServer.
	cfg.net.DisableServer(serverName)

	// Stop the Raft instance's goroutines.
	if cfg.rafts[i] != nil {
		cfg.rafts[i].Kill()
	}
}

// restart brings server i back after a crash.
// It creates a new Raft instance (simulating a reboot) and re-enables
// the server's network connections.
func (cfg *config) restart(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cfg.startRaft(i)
	cfg.connected[i] = true
	cfg.net.EnableServer(fmt.Sprintf("s%d", i))
}

// disconnect partitions server i from the rest of the cluster.
// The Raft instance keeps running but cannot send or receive messages.
// Use connect(i) to heal the partition.
func (cfg *config) disconnect(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cfg.connected[i] = false
	cfg.net.DisableServer(fmt.Sprintf("s%d", i))
}

// connect restores server i's network connectivity after a partition.
func (cfg *config) connect(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cfg.connected[i] = true
	cfg.net.EnableServer(fmt.Sprintf("s%d", i))
}

// checkOneLeader verifies that exactly one leader is elected among the
// connected servers. It retries for up to ~5 seconds to give elections
// time to complete (including split-vote retries).
//
// Returns the index of the current leader.
// Calls t.Fatalf if two leaders exist in the same term, or if no leader
// emerges within the retry window.
func (cfg *config) checkOneLeader() int {
	for attempt := 0; attempt < 10; attempt++ {
		// Wait long enough for an election to complete.
		// Election timeout is 150-300 ms; 450-550 ms is a comfortable margin.
		ms := 450 + rand.Int63()%100
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Snapshot which server is leader at which term.
		leadersByTerm := make(map[int]int) // term → server index
		for i := 0; i < cfg.n; i++ {
			if !cfg.connected[i] {
				continue
			}
			term, isLeader := cfg.rafts[i].GetState()
			if isLeader {
				if prev, exists := leadersByTerm[term]; exists {
					cfg.t.Fatalf(
						"election safety violated: two leaders in term %d — server %d and server %d",
						term, prev, i,
					)
				}
				leadersByTerm[term] = i
			}
		}

		// Return the leader with the highest term (the one the cluster
		// has converged to after any previous leader failures).
		bestTerm := -1
		bestLeader := -1
		for term, leader := range leadersByTerm {
			if term > bestTerm {
				bestTerm = term
				bestLeader = leader
			}
		}
		if bestLeader != -1 {
			return bestLeader
		}
	}

	cfg.t.Fatalf("no leader elected after 10 attempts (~5 seconds)")
	return -1
}

// checkTerms verifies that all connected servers agree on the same current term.
// Returns the agreed term. Calls t.Fatalf on disagreement.
//
// Note: this is a point-in-time check. Call it only after the cluster has
// had time to stabilise (e.g. after checkOneLeader returns).
func (cfg *config) checkTerms() int {
	term := -1
	firstServer := -1
	for i := 0; i < cfg.n; i++ {
		if !cfg.connected[i] {
			continue
		}
		t, _ := cfg.rafts[i].GetState()
		if term == -1 {
			term = t
			firstServer = i
		} else if t != term {
			cfg.t.Fatalf(
				"term disagreement: server %d has term %d, server %d has term %d",
				firstServer, term, i, t,
			)
		}
	}
	return term
}

// checkNoLeader asserts that no connected server believes it is the leader.
// Use this to verify that a minority partition cannot elect a leader.
func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if !cfg.connected[i] {
			continue
		}
		_, isLeader := cfg.rafts[i].GetState()
		if isLeader {
			cfg.t.Fatalf("server %d claims to be leader but no leader should exist", i)
		}
	}
}

// cleanup kills all Raft instances and destroys the network.
// Must be deferred at the top of every test: defer cfg.cleanup()
func (cfg *config) cleanup() {
	for i := 0; i < cfg.n; i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	cfg.net.Destroy()
}
