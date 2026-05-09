package transport

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/peartes/raft/internal/raft"
	raftpb "github.com/peartes/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// raftServicePrefix is the gRPC FullMethod prefix for the Raft service —
// used by PartitionInterceptor to selectively reject peer-to-peer RPCs
// while leaving the RaftControl service reachable so a dashboard can still
// poll a partitioned node.
const raftServicePrefix = "/raft.v1.Raft/"

// ControlServer implements raftpb.RaftControlServer for operator/dashboard
// use. The single `partitioned` flag is shared with PartitionInterceptor —
// flipping it via SetPartitioned takes effect on the next incoming Raft
// RPC.
type ControlServer struct {
	raftpb.UnimplementedRaftControlServer
	rf          *raft.Raft
	partitioned *atomic.Bool
}

// NewControlServer takes the same atomic.Bool that the partition
// interceptor reads from, so a SetPartitioned call updates both views
// without any locking dance.
func NewControlServer(rf *raft.Raft, partitioned *atomic.Bool) *ControlServer {
	return &ControlServer{rf: rf, partitioned: partitioned}
}

func (s *ControlServer) GetState(_ context.Context, _ *raftpb.GetStateReq) (*raftpb.GetStateRes, error) {
	st := s.rf.Inspect()
	return &raftpb.GetStateRes{
		Term:        int64(st.Term),
		Role:        int32(st.Role),
		Logs:        logsToProto(st.Logs),
		CommitIndex: int64(st.CommitIndex),
		LastApplied: int64(st.LastApplied),
	}, nil
}

func (s *ControlServer) SubmitCommand(_ context.Context, in *raftpb.SubmitCommandReq) (*raftpb.SubmitCommandRes, error) {
	idx, term, isLeader := s.rf.Start(in.GetCommand())
	return &raftpb.SubmitCommandRes{
		Index:    int64(idx),
		Term:     int64(term),
		IsLeader: isLeader,
	}, nil
}

func (s *ControlServer) SetPartitioned(_ context.Context, in *raftpb.SetPartitionedReq) (*raftpb.SetPartitionedRes, error) {
	s.partitioned.Store(in.GetPartitioned())
	return &raftpb.SetPartitionedRes{}, nil
}

// PartitionInterceptor returns a gRPC UnaryServerInterceptor that rejects
// inbound calls to the Raft service (RequestVote / AppendEntries /
// InstallSnapshot) when partitioned.Load() is true. RaftControl calls fall
// through unconditionally so dashboards can still see and unpartition the
// node.
//
// This is an asymmetric simulation — the node may still successfully send
// outbound RPCs to peers. That mirrors what the parent plan describes
// (incoming-only drop) and is enough for the demo. For symmetric drops the
// GRPCPeer would also need a per-node flag.
func PartitionInterceptor(partitioned *atomic.Bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if partitioned.Load() && strings.HasPrefix(info.FullMethod, raftServicePrefix) {
			return nil, status.Error(codes.Unavailable, "partitioned")
		}
		return handler(ctx, req)
	}
}
