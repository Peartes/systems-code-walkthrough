package raft

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	gob "github.com/peartes/raft/labgob"
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
	Entry []byte //
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

type InstallSnapshotReq struct {
	Term              int // leader term
	LeaderId          int
	LastIncludedIndex int // for the follower to update it's state
	LastIncludedTerm  int
	Data              []byte // a chunk of the snapshot
	Offset            int    // byte offset where the chunk is positioned in the snapshot
	Done              bool   // true if this is the last chunk of the snapshot
}

type InstallSnapshotRes struct {
	Term int // followers term for leader to update itself
}

// ApplyMsg is the message streamed in the applyChan channel to the state machine
// the state machine is notified on the entries to apply from this channel
type ApplyMsg struct {
	Index    int // the index of this message in the channel
	Term     int // what term was this message finalized
	Entry    Log
	Snapshot []byte // snapshot message for state machine
}

// Persister persists raft data (into memeory for now)
// the struct implements a save([]byte) and a load() method
type Persister struct {
	data []byte // data is serialized as [votedFor] - [term] - [logs]
	// 										 1 byte		16byte
	snapShot             Snapshot // the complete raft instance snapshot
	snapShotIntermediate Snapshot // the intermediate snapshot file while it's been streamed from leader
}

type Snapshot struct {
	lastIncludedIndex int // the lastIncludedIndex of this snapshot
	lastIncludedTerm  int
	data              []byte
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
	commitIndex int             // the index of the last commited log entry for this server
	lastApplied int             // index of the last log entry applied to state machine
	applyChan   chan<- ApplyMsg // channel to send apply entries on
	applyCond   *sync.Cond      // the condition to trigger applying entries to state machine

	// non-persistent on leaders
	nextIndex  []int // for each server, index of the next log entry to be sent. initialized to leader lastLogIndex+1
	matchIndex []int // for each server, index of the highest log entry known to be replicated on the server

	lastHeartbeat   int64
	electionTimeout time.Duration

	persister *Persister // pointer to the persister for a raft node

	lastIncludedIndex int // index of the last entry included in the most recent snapshot
	lastIncludedTerm  int // term of the last entry included in the most recent snapshot
}

// Make creates and starts a Raft instance.
//
// peers: one Peer per server in the cluster; peers[me] must be nil
// me:    this server's index
//
// This is the constructor tests will call
func Make(peers []Peer, me int, applyChan chan<- ApplyMsg, persister *Persister) *Raft {
	rf := &Raft{
		peers: peers,
		me:    me,
	}
	// TODO: initialise persistent state
	rf.currentTerm = 0                           // read from persistent storage
	rf.votedFor = -1                             // read from persistent storage
	rf.logs = []Log{{Term: -1, Entry: []byte{}}} // read from persistent storage; we initiated with a default state to prevent out of bounds error
	// initialize volatile state
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyChan = applyChan
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.persister = persister
	// load the last saved state
	rfBz := rf.persister.load()
	if len(rfBz) != 0 {
		currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm, logs := rf.readPersist()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
	rf.mu.Lock()
	rf.setNewElectionTimeout() // so that a newly restarted raft instance does not start an election immediately causing the old leader to step down (if the raft instance has a larger term)
	rf.mu.Unlock()
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
	// start the applier routine
	rf.applier()
	return rf
}

// GetState returns this server's current term and whether it believes
// it is the leader. Called by the test harness to verify invariants.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) GetLog(idx int) (bool, Log) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if idx >= len(rf.logs) {
		return false, Log{}
	}
	return true, rf.logs[idx]
}

// Start submits a command to the Raft log. If this server is the leader,
// it appends the command and returns the index at which it will appear,
// the current term, and true. If not the leader, returns -1, -1, false.
//
// Start must return quickly — it must not wait for replication to complete.
// The test harness will poll for commitment separately.
func (rf *Raft) Start(command []byte) (index int, term int, isLeader bool) {
	// if we are not the leader, reject the append request
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	} else {
		// append the log entry to logs, call append entry and return the index
		rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Entry: command})
		rf.persist()
		// create a goroutine to send append entries so we can respond quicly to client
		go func(term int) {
			rf.sendEntries(term)
		}(rf.currentTerm)
		return len(rf.logs) - 1, rf.currentTerm, rf.state == Leader
	}
}

// Kill signals this Raft instance to stop. All background goroutines
// must check killed() and exit cleanly. Called by the harness on crash.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Broadcast()
}

// killed returns true if Kill has been called.
// Every long-running goroutine in your implementation should check this.
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Applies all entries between lastApplied and commitIndex to state machine
// does this by streaming the entries to the applyChan
// Note: make sure you don't hold the rf.mu before calling this method
func (rf *Raft) applier() {
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			for rf.lastApplied >= rf.commitIndex {
				rf.applyCond.Wait()
			}
			if rf.lastApplied < rf.commitIndex {
				var snapShot []byte
				if rf.lastApplied < rf.lastIncludedIndex {
					// send snapshot to state machine to update it to lastIncludedIndex
					snapShot = rf.persister.snapShot.data
					rf.lastApplied = rf.lastIncludedIndex
				}
				lastApplied := rf.lastApplied
				lastIncludedTerm := rf.lastIncludedTerm
				entries := rf.logs[rf.lastApplied-rf.lastIncludedIndex+1 : rf.commitIndex-rf.lastIncludedIndex+1]
				rf.lastApplied = rf.commitIndex
				fmt.Printf("[APPLY] S%d: snapShot_nil=%v lastApplied=%d commitIdx=%d lastIncluded=%d entries=%d\n",
					rf.me, snapShot == nil, lastApplied, rf.commitIndex, rf.lastIncludedIndex, len(entries))
				rf.mu.Unlock() // we don't want to hold the lock and block on channel
				// send the snapshot first if there is any
				if snapShot != nil {
					rf.applyChan <- ApplyMsg{
						Index:    lastApplied,
						Term:     lastIncludedTerm,
						Snapshot: snapShot,
					}
				}
				for i, entry := range entries {
					rf.applyChan <- ApplyMsg{
						Index: lastApplied + 1 + i,
						Term:  entry.Term,
						Entry: entry,
					}
				}
			}
		}
	}()
}

// persist encodes votedFor, currentTerm and logs using labgob
// then hands them over to the persister to persist them
func (rf *Raft) persist() {
	rfBz := new(bytes.Buffer)
	enc := gob.NewEncoder(rfBz)
	raftData := struct {
		VotedFor          int
		CurrentTerm       int
		LastIncludedIndex int
		LastIncludedTerm  int
		Logs              []Log
	}{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Logs:              rf.logs,
	}
	if err := enc.Encode(raftData); err != nil {
		panic("cannot encode raft data")
	}
	rfBzb := rfBz.Bytes()
	rf.persister.store(&rfBzb)

}

// readPersist reads the content of the raft persister struct
// this should contain the raft instance persisted non-volatile state
// it returns the (currentTerm, votedFor, logs)
func (rf *Raft) readPersist() (int, int, int, int, []Log) {
	rfBz := rf.persister.load()
	dec := gob.NewDecoder(bytes.NewBuffer(rfBz))
	var rfData struct {
		CurrentTerm       int
		VotedFor          int
		LastIncludedIndex int
		LastIncludedTerm  int
		Logs              []Log
	}
	dec.Decode(&rfData)
	return rfData.CurrentTerm, rfData.VotedFor, rfData.LastIncludedIndex, rfData.LastIncludedTerm, rfData.Logs
}

// Snapshot trims the raft logs up to and including index
// persist the last included index and term
// moves the commit index to the log just after the last inlcuded index
func (rf *Raft) Snapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastIncludedTerm = rf.logs[index-rf.lastIncludedIndex].Term
	rf.logs = rf.logs[index-rf.lastIncludedIndex:] // slice from the index to the end and replace the index with the snapshot msg
	rf.lastIncludedIndex = index
	rf.logs[0].Term = rf.lastIncludedTerm
	rf.logs[0].Entry = []byte{}
	rf.persister.saveSnapshot(&data, 0, true, rf.lastIncludedIndex, rf.lastIncludedTerm)

	rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(index)))
	rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(index)))
	rf.persist()
}
