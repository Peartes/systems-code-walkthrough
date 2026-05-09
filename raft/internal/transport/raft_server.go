package transport

import (
	"context"

	"github.com/peartes/raft/internal/raft"
	raftpb "github.com/peartes/raft/proto"
)

// RaftServer adapts the existing *raft.Raft RPC methods (RequestVote,
// AppendEntries, InstallSnapshot — all defined as
// (rf *Raft) Method(req T, res *T) in election.go and replication.go) to
// the gRPC service signature generated from proto/raft.proto.
//
// Partition simulation is enforced by the UnaryInterceptor in this package
// (see PartitionInterceptor) — handlers themselves are partition-unaware,
// keeping the concern in one place.
type RaftServer struct {
	raftpb.UnimplementedRaftServer
	rf *raft.Raft
}

// NewRaftServer wraps a running Raft instance for gRPC service registration.
func NewRaftServer(rf *raft.Raft) *RaftServer {
	return &RaftServer{rf: rf}
}

func (s *RaftServer) RequestVote(_ context.Context, in *raftpb.RequestVoteReq) (*raftpb.RequestVoteRes, error) {
	var res raft.RequestVoteRes
	s.rf.RequestVote(requestVoteReqFromProto(in), &res)
	return requestVoteResToProto(res), nil
}

func (s *RaftServer) AppendEntries(_ context.Context, in *raftpb.AppendEntryReq) (*raftpb.AppendEntryRes, error) {
	var res raft.AppendEntryRes
	s.rf.AppendEntries(appendEntriesReqFromProto(in), &res)
	return appendEntriesResToProto(res), nil
}

func (s *RaftServer) InstallSnapshot(_ context.Context, in *raftpb.InstallSnapshotReq) (*raftpb.InstallSnapshotRes, error) {
	var res raft.InstallSnapshotRes
	s.rf.InstallSnapshot(installSnapshotReqFromProto(in), &res)
	return installSnapshotResToProto(res), nil
}
