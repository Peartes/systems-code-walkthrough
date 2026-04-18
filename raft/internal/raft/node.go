package raft

import (
	"math"
	"time"
)

// becomeLeader set this raft instance as the leader for a term
func (rf *Raft) becomeLeader(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if we're not on the same term ignore
	// if we're not candidate when this call is made, then it's stale
	if rf.currentTerm != term || rf.state != Candidate {
		return // stale election must have called this method
	}

	// we're now leader, set our state to leader
	rf.state = Leader
	// initialize nextIndex to lastLogIndex + 1
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}

	// now create goroutine to send an empty appendEntry/heartbeat
	// to all peers at intervals
	// to establish we're leader
	go func(term int) {
		for {
			rf.mu.Lock()
			if !rf.killed() {
				rf.mu.Unlock()
				time.Sleep(time.Duration(50) * time.Millisecond)
				rf.mu.Lock()
				// if we're not leader, this is a stale hearbeat goroutine
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				// send hearbeats as empty appendentries to all peers
				rf.mu.Unlock()
				rf.sendEntries(term)
			} else {
				rf.mu.Unlock()
				return // server is killed so kill this heartbeat
			}
		}
	}(rf.currentTerm)
}

// sendEntries sends append entries to all peers.
// always includes any log entries the peer is missing (logs[nextIndex:]).
// when a follower is fully caught up this naturally becomes an empty slice,
// which is what the Raft paper calls a heartbeat.
func (rf *Raft) sendEntries(term int) {
	rf.mu.Lock()
	if !rf.killed() {
		// if our term is now higher than term
		// this is a stale request
		if rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		for i, peer := range rf.peers {
			if peer == nil {
				continue
			}
			go func(peer Peer, peerId int) {
				// we can use the hearbeat to update the peers
				// next index and not waste the call
				rf.mu.Lock()
				if !rf.killed() {
					req := AppendEntryReq{
						Id:                rf.me,
						PrevLogIndex:      rf.nextIndex[peerId] - 1,
						PrevLogTerm:       rf.logs[rf.nextIndex[peerId]-1].Term,
						LeaderCommitIndex: rf.commitIndex,
						Term:              rf.currentTerm,
					}
					// always include any entries the peer is missing.
					// this covers both normal replication and catch-up after restart/reconnect.
					// heartbeats become naturally empty when the follower is fully caught up
					// (nextIndex == len(logs), so logs[nextIndex:] == []).
					req.Entries = rf.logs[req.PrevLogIndex+1:]
					rf.mu.Unlock()
					res := new(AppendEntryRes)
					ok := peer.Call("Raft.AppendEntries", req, res)
					if !ok {
						// something is wrong with our clientEnd
						return
					} else {
						rf.mu.Lock()
						// if we're not leader, ignore this stale request
						if rf.state != Leader {
							rf.mu.Unlock()
							return
							// if peer term is higher than ours, we're stale
							// revert to follower
						} else if res.Term > rf.currentTerm {
							rf.currentTerm = res.Term
							rf.votedFor = -1
							rf.state = Follower
							rf.persist()
						} else {
							if !res.Appended {
								// the peer is stale on their logs
								// decrement the next index for the peer
								if rf.nextIndex[peerId] > 1 {
									rf.nextIndex[peerId]--
								}
							} else {
								// the peer replicated this log
								// we can increase the next index for the peer
								// because of race conditions, it's possible for multiple goroutines to modify these
								// at the same time. A sample scenario
								// When you call Start() three times in quick succession, each one spawns a goroutine
								// that calls sendEntries. All three goroutines run concurrently and all read
								// nextIndex[peerId] = 1 before any of them has updated it. Each computes
								// entries = rf.logs[1:] — all three see the full 3-entry slice. All three RPCs succeed.
								// Then each goroutine does:
								// rf.nextIndex[peerId] += len(req.Entries)
								// to avoid this problem, the nextIndex[peerId] is the prevLogIndex+len(entries)
								rf.nextIndex[peerId] = max(rf.nextIndex[peerId], req.PrevLogIndex+len(req.Entries)+1)
								// we can also increase the match index for this peer
								// to the current replicated index
								// using a max function here just in case another goroutine
								// sending out non-empty append entries already updated this
								// value to an higher one
								rf.matchIndex[peerId] = int(math.Max(float64(rf.matchIndex[peerId]), float64(req.PrevLogIndex+len(req.Entries)))) // Todo: is there a better way ?
								// let's update the leaders commit index also
								// we find the highest index replicated on the majority of the servers
								// and also greater than the last commit index
								// Todo: use an efficient algo
								commitCount := 0
								for _, idx := range rf.matchIndex {
									for _, j := range rf.matchIndex {
										if j >= idx { // we count how many servers have replicated at least up to j i.e.
											// how many servers have the log at j replicated and if a server has a higher index
											// by the invariant, they must have the log at j
											commitCount++
										}
									}
									if commitCount >= len(rf.peers)/2 {
										if idx > rf.commitIndex && rf.logs[idx].Term == rf.currentTerm {
											// we have a new higher match index
											rf.commitIndex = idx
											// signal applier to apply entries to state machine
											rf.applyCond.Signal()
										}
									}
									commitCount = 0
								}
							}
						}
						rf.mu.Unlock()
					}
				} else {
					rf.mu.Unlock()
				}
			}(peer, i)
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}
}
