package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// Peer represents a connection to another Raft node.
// The test harness satisfies this interface with *rpc.ClientEnd.
// A production deployment would satisfy it with a gRPC stub.
// Keeping this interface here means the raft package has zero knowledge
// of how the transport works — it only knows it can Call a remote method.
type Peer interface {
	Call(serviceMethod string, args any, reply any) bool
}

// Log represent individual log entry stored in each raft servers logs
type Log struct {
	Term  int    // term this entry belongs
	Entry string //
}

// This is the state of each raft server
const (
	Follower = iota
	Candidate
	Leader
)

type RequestVoteReq struct {
	Term         int // requesting server current term
	LastLogIndex int // the index of the last log written to requesting server
	LastLogTerm  int // the term of the last log written in requesting server's log
	Id           int // the id of the requesting server
}

type RequestVoteRes struct {
	Term        int  // the term of the receiving server
	VoteGranted bool // did the receiving server vote for the requesting server
}

type AppendEntryReq struct {
	Id                int   // leader id
	Term              int   // the leaders current term for followers to update themselves
	PrevLogIndex      int   // the index of the last log entry before the entries leader is sending
	PrevLogTerm       int   // the term of the prevLogEntry
	LeaderCommitIndex int   // the commit index of the leader for followers to update themselves
	Entries           []Log // the entries to add
}

type AppendEntryRes struct {
	Term     int  // the followers term for leader to update itself
	Appended bool // was the log entry successfull appended ?
}

// Raft is the consensus module. One instance runs per server.
// Fields will be filled in as you implement each component.
type Raft struct {
	peers []Peer // handles to all other Raft servers; peers[me] is nil
	me    int    // this server's index in the peers slice
	dead  int32  // set atomically by Kill(); checked by killed()
	mu    sync.Mutex
	state int // leader, candidate or follower

	// persistent state no the server
	currentTerm int // the current term of this server
	votedFor    int // who this server voted for in this term
	logs        []Log

	// non-persistent state
	commitIndex int // the index of the last commited log entry for this server
	lastApplied int // index of the last log entry applied to state machine
	// non-persistent on leaders
	nextIndex  []int // for each server, index of the next log entry to be sent. initialized to leader lastLogIndex+1
	matchIndex []int // for each server, index of the highest log entry known to be replicated on the server

	lastHeartbeat   int64
	electionTimeout time.Duration
}

// Make creates and starts a new Raft instance.
//
// peers: one Peer per server in the cluster; peers[me] must be nil
// me:    this server's index
//
// This is the constructor your tests will call. As you implement
// election and replication, you will add fields to Raft and initialise
// them here, then launch the background goroutines that drive the algorithm.
func Make(peers []Peer, me int) *Raft {
	rf := &Raft{
		peers: peers,
		me:    me,
	}
	// TODO: initialise persistent state
	rf.currentTerm = 0                        // read from persistent storage
	rf.votedFor = -1                          // read from persistent storage
	rf.logs = []Log{Log{Term: -1, Entry: ""}} // read from persistent storage; we initiated with a default state to prevent out of bounds error
	// initialize volatile state
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// start the election timer
	go func() {
		for {
			rf.mu.Lock()
			if !rf.killed() {
				rf.mu.Unlock()
				time.Sleep(time.Millisecond * 50)
				rf.checkElectionTimeout()
			} else {
				rf.mu.Unlock()
				return // end the routine if raft instance ended
			}
		}
	}()
	return rf
}

// GetState returns this server's current term and whether it believes
// it is the leader. Called by the test harness to verify invariants.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start submits a command to the Raft log. If this server is the leader,
// it appends the command and returns the index at which it will appear,
// the current term, and true. If not the leader, returns -1, -1, false.
//
// Start must return quickly — it must not wait for replication to complete.
// The test harness will poll for commitment separately.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// if we are not the leader, reject the append request
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	} else {
		// append the log entry to logs, call append entry and return the index
		rf.logs = append(rf.logs, command.(Log))
		// create a goroutine to send append entries so we can respond quicly to client
		go func(term int) {
			rf.sendEntries(rf.currentTerm, false)
		}(rf.currentTerm)
		return len(rf.logs) - 1, rf.currentTerm, rf.state == Leader
	}
}

// Kill signals this Raft instance to stop. All background goroutines
// must check killed() and exit cleanly. Called by the harness on crash.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed returns true if Kill has been called.
// Every long-running goroutine in your implementation should check this.
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
