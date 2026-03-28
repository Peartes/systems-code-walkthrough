package raft

import (
	rng "crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"
)

// checkElectionTimeout checks if election timeout has elapsed
// and calls startElection if so
func (rf *Raft) checkElectionTimeout() {
	rf.mu.Lock()
	// if we're leader, reset election timeout and retun
	if rf.state == Leader {
		rf.setNewElectionTimeout()
		rf.mu.Unlock()
		return
		// check if timeout has elapsed
	} else if time.Now().After(time.UnixMicro(rf.lastHeartbeat).Add(rf.electionTimeout)) {
		// timeout elapsed, we need to start a new election
		rf.state = Candidate
		// update the election timeout
		rf.setNewElectionTimeout()
		rf.mu.Unlock()
		rf.startElection()
	} else {
		rf.mu.Unlock()
	}
}

// setElectionTimeout creates a new election timeout for the raft instance
// because this is a convinient method and is to be run inline of other functions
// make sure the lock on the raft object is acquired before calling this function
func (rf *Raft) setNewElectionTimeout() {
	rf.lastHeartbeat = time.Now().UnixMicro()
	randomTimeout, err := rng.Int(rng.Reader, big.NewInt(150))
	if err != nil {
		panic(fmt.Errorf("cannot set election timeout for raft instance %d", rf.me))
	}
	rf.electionTimeout = time.Millisecond * time.Duration((150 + randomTimeout.Int64()))
}

// startEelction begins a new round of election for a raft instance
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// only start election if we're candidate
	if rf.state != Candidate {
		return
	} else {
		// increment out current term
		// Todo: persist current term and votedFor
		rf.currentTerm += 1
		rf.votedFor = rf.me

		// keep track of vote count
		var voteCount int32 = 1
		// prepare args for rpc
		req := RequestVoteReq{
			Id:           rf.me,
			Term:         rf.currentTerm,
			LastLogIndex: len(rf.logs) - 1,
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		}

		// send request vote rpc to all peers
		for _, peer := range rf.peers {
			if peer == nil {
				continue
			}
			go func(peer Peer, term int) {
				rf.mu.Lock()
				if !rf.killed() {
					rf.mu.Unlock()

					res := new(RequestVoteRes)
					ok := peer.Call("Raft.RequestVote", req, res)
					if !ok {
						// something is wrong with the connection
						return
					} else {
						rf.mu.Lock()
						if !rf.killed() {
							// if the term of the response is higher than ours,
							// switch to follower immediately
							// since that means we're stale
							// clear the votedFor also and update current term
							if res.Term > rf.currentTerm {
								rf.state = Follower
								rf.currentTerm = res.Term
								rf.votedFor = -1
								rf.mu.Unlock()
							} else {
								// if we received the vote, atomically update th
								// votedFor, then count the number of vote we have
								// if it is >= len(peers)/2 + 1, call become leader
								if res.VoteGranted {
									atomic.AddInt32(&voteCount, 1)
								}
								if int(atomic.LoadInt32(&voteCount)) >= len(rf.peers)/2+1 {
									// we have the majority vote and can become leader
									rf.mu.Unlock()
									rf.becomeLeader(term)
								} else {
									rf.mu.Unlock()
								}
							}
						} else {
							rf.mu.Unlock()
						}
					}
				}
			}(peer, rf.currentTerm)
		}
	}
}

// ReqeustVote rpc is called by a candidate to request for vote
// for a particula term
func (rf *Raft) RequestVote(req RequestVoteReq, res *RequestVoteRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.killed() {
		// first we check the term of the candidate requesting
		// a lower term shows a stale candidate that needs to update itself
		// also check if we have not voted in this term
		if req.Term < rf.currentTerm {
			res.Term = rf.currentTerm
			res.VoteGranted = false
			return
		}
		// if the candidates term is higher than ours, set out current term because we're stale
		if req.Term > rf.currentTerm {
			rf.currentTerm = req.Term
			// reset our votedFor since we were stale and haven't voted in this term
			rf.votedFor = -1
			// convert to follower also since we're stale
			rf.state = Follower
			// claude: do we need to return here ?
		}
		if rf.votedFor != -1 && rf.votedFor != req.Id {
			res.Term = rf.currentTerm
			res.VoteGranted = false
			return
		}
		// reset the election timer since we confirm there's a live leader
		rf.setNewElectionTimeout()
		// now we check for log recency
		// if the candidates lastLogIndex has a term lesser than ours, it's a stale candidate
		if req.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
			res.Term = rf.currentTerm
			res.VoteGranted = false
			return
		} else if req.LastLogTerm > rf.logs[len(rf.logs)-1].Term {
			// the candidate has a more recent log term
			res.Term = rf.currentTerm
			res.VoteGranted = true
			rf.votedFor = req.Id
			return
		} else {
			// now we check who has the longer log
			if req.LastLogIndex < len(rf.logs)-1 {
				// candidate's log is not as recent as ours
				res.Term = rf.currentTerm
				res.VoteGranted = false
			} else {
				// grant the vote to the candidate
				res.Term = rf.currentTerm
				res.VoteGranted = true
				// update our votedFor and current term
				rf.votedFor = req.Id
				rf.currentTerm = req.Term
			}
			return
		}
	}
}
