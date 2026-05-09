package transport

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/peartes/raft/internal/raft"
	raftpb "github.com/peartes/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCPeer is the production implementation of raft.Peer. It satisfies the
// interface declared at internal/raft/state.go:20 by translating
// serviceMethod-keyed Call invocations into typed gRPC calls against a
// remote Raft node.
//
// Connection management uses grpc.NewClient, which dials lazily on the first
// RPC and reconnects automatically on transport failure — exactly matching
// the "unreliable, drop-and-retry" semantics the engine already expects from
// the test harness.
type GRPCPeer struct {
	conn    *grpc.ClientConn
	client  raftpb.RaftClient
	timeout time.Duration
	// partitioned, when non-nil and Load()==true, causes Call to return
	// false without touching the network — symmetric with how
	// PartitionInterceptor drops inbound traffic. Together they
	// simulate a real network partition rather than a half-open one.
	partitioned *atomic.Bool
}

// defaultPeerTimeout sits between the heartbeat cadence (50ms) and the
// election timeout floor (150ms) so a slow follower trips the engine's
// existing retry loop without inducing a spurious election.
const defaultPeerTimeout = 200 * time.Millisecond

// NewGRPCPeer builds a peer dialing addr (e.g. "localhost:5001"). The
// connection is established lazily on the first Call.
//
// `partitioned` is the shared atomic flag toggled by RaftControl
// .SetPartitioned. Pass nil if you want a peer that never self-blocks
// (rare — production code should always pass it so partition is
// symmetric).
func NewGRPCPeer(addr string, partitioned *atomic.Bool) (*GRPCPeer, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GRPCPeer{
		conn:        conn,
		client:      raftpb.NewRaftClient(conn),
		timeout:     defaultPeerTimeout,
		partitioned: partitioned,
	}, nil
}

// Close releases the underlying gRPC connection.
func (p *GRPCPeer) Close() error { return p.conn.Close() }

// Call satisfies raft.Peer. The engine passes args by value and reply by
// pointer (see internal/raft/election.go:84 and internal/raft/node.go:117,188);
// this method asserts those exact shapes. Returning false on any error
// matches the test harness ClientEnd semantics: the engine treats a false
// return as a dropped packet and retries on the next tick.
func (p *GRPCPeer) Call(serviceMethod string, args any, reply any) bool {
	// Self-partition short-circuit: a partitioned node refuses to send.
	// This pairs with PartitionInterceptor (which drops inbound) to
	// produce a symmetric drop. Without it the partitioned node could
	// still ship VoteRequests outward and trigger spurious elections.
	if p.partitioned != nil && p.partitioned.Load() {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	switch serviceMethod {
	case "Raft.RequestVote":
		in, ok := args.(raft.RequestVoteReq)
		if !ok {
			return false
		}
		out, err := p.client.RequestVote(ctx, requestVoteToProto(in))
		if err != nil {
			return false
		}
		dst, ok := reply.(*raft.RequestVoteRes)
		if !ok {
			return false
		}
		*dst = requestVoteFromProto(out)
		return true

	case "Raft.AppendEntries":
		in, ok := args.(raft.AppendEntryReq)
		if !ok {
			return false
		}
		out, err := p.client.AppendEntries(ctx, appendEntriesToProto(in))
		if err != nil {
			return false
		}
		dst, ok := reply.(*raft.AppendEntryRes)
		if !ok {
			return false
		}
		*dst = appendEntriesFromProto(out)
		return true

	case "Raft.InstallSnapshot":
		in, ok := args.(raft.InstallSnapshotReq)
		if !ok {
			return false
		}
		out, err := p.client.InstallSnapshot(ctx, installSnapshotToProto(in))
		if err != nil {
			return false
		}
		dst, ok := reply.(*raft.InstallSnapshotRes)
		if !ok {
			return false
		}
		*dst = installSnapshotFromProto(out)
		return true
	}
	return false
}

// Compile-time check that *GRPCPeer satisfies raft.Peer.
var _ raft.Peer = (*GRPCPeer)(nil)
