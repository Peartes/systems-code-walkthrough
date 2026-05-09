package transport

import (
	"github.com/peartes/raft/internal/raft"
	raftpb "github.com/peartes/raft/proto"
)

// Bridge between the wire types in proto/ and the in-memory types in
// internal/raft/state.go. The wire uses int64 throughout; Go uses int.

func logsToProto(in []raft.Log) []*raftpb.LogEntry {
	out := make([]*raftpb.LogEntry, len(in))
	for i, e := range in {
		out[i] = &raftpb.LogEntry{Term: int64(e.Term), Entry: e.Entry}
	}
	return out
}

func logsFromProto(in []*raftpb.LogEntry) []raft.Log {
	out := make([]raft.Log, len(in))
	for i, e := range in {
		out[i] = raft.Log{Term: int(e.GetTerm()), Entry: e.GetEntry()}
	}
	return out
}

func requestVoteToProto(in raft.RequestVoteReq) *raftpb.RequestVoteReq {
	return &raftpb.RequestVoteReq{
		Term:         int64(in.Term),
		LastLogIndex: int64(in.LastLogIndex),
		LastLogTerm:  int64(in.LastLogTerm),
		Id:           int64(in.Id),
	}
}

func requestVoteFromProto(in *raftpb.RequestVoteRes) raft.RequestVoteRes {
	return raft.RequestVoteRes{
		Term:        int(in.GetTerm()),
		VoteGranted: in.GetVoteGranted(),
	}
}

func appendEntriesToProto(in raft.AppendEntryReq) *raftpb.AppendEntryReq {
	return &raftpb.AppendEntryReq{
		Id:                int64(in.Id),
		Term:              int64(in.Term),
		PrevLogIndex:      int64(in.PrevLogIndex),
		PrevLogTerm:       int64(in.PrevLogTerm),
		LeaderCommitIndex: int64(in.LeaderCommitIndex),
		Entries:           logsToProto(in.Entries),
	}
}

func appendEntriesFromProto(in *raftpb.AppendEntryRes) raft.AppendEntryRes {
	return raft.AppendEntryRes{
		Term:     int(in.GetTerm()),
		Appended: in.GetAppended(),
	}
}

func installSnapshotToProto(in raft.InstallSnapshotReq) *raftpb.InstallSnapshotReq {
	return &raftpb.InstallSnapshotReq{
		Term:              int64(in.Term),
		LeaderId:          int64(in.LeaderId),
		LastIncludedIndex: int64(in.LastIncludedIndex),
		LastIncludedTerm:  int64(in.LastIncludedTerm),
		Data:              in.Data,
		Offset:            int64(in.Offset),
		Done:              in.Done,
	}
}

func installSnapshotFromProto(in *raftpb.InstallSnapshotRes) raft.InstallSnapshotRes {
	return raft.InstallSnapshotRes{Term: int(in.GetTerm())}
}

// ===== Inbound: proto request -> raft request, raft response -> proto response =====
// Used by the server-side handlers in raft_server.go.

func requestVoteReqFromProto(in *raftpb.RequestVoteReq) raft.RequestVoteReq {
	return raft.RequestVoteReq{
		Term:         int(in.GetTerm()),
		LastLogIndex: int(in.GetLastLogIndex()),
		LastLogTerm:  int(in.GetLastLogTerm()),
		Id:           int(in.GetId()),
	}
}

func requestVoteResToProto(in raft.RequestVoteRes) *raftpb.RequestVoteRes {
	return &raftpb.RequestVoteRes{
		Term:        int64(in.Term),
		VoteGranted: in.VoteGranted,
	}
}

func appendEntriesReqFromProto(in *raftpb.AppendEntryReq) raft.AppendEntryReq {
	return raft.AppendEntryReq{
		Id:                int(in.GetId()),
		Term:              int(in.GetTerm()),
		PrevLogIndex:      int(in.GetPrevLogIndex()),
		PrevLogTerm:       int(in.GetPrevLogTerm()),
		LeaderCommitIndex: int(in.GetLeaderCommitIndex()),
		Entries:           logsFromProto(in.GetEntries()),
	}
}

func appendEntriesResToProto(in raft.AppendEntryRes) *raftpb.AppendEntryRes {
	return &raftpb.AppendEntryRes{
		Term:     int64(in.Term),
		Appended: in.Appended,
	}
}

func installSnapshotReqFromProto(in *raftpb.InstallSnapshotReq) raft.InstallSnapshotReq {
	return raft.InstallSnapshotReq{
		Term:              int(in.GetTerm()),
		LeaderId:          int(in.GetLeaderId()),
		LastIncludedIndex: int(in.GetLastIncludedIndex()),
		LastIncludedTerm:  int(in.GetLastIncludedTerm()),
		Data:              in.GetData(),
		Offset:            int(in.GetOffset()),
		Done:              in.GetDone(),
	}
}

func installSnapshotResToProto(in raft.InstallSnapshotRes) *raftpb.InstallSnapshotRes {
	return &raftpb.InstallSnapshotRes{Term: int64(in.Term)}
}
