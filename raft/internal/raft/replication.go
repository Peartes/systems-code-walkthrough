package raft

import (
	"fmt"
	"math"
)

func (rf *Raft) AppendEntries(req AppendEntryReq, res *AppendEntryRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if server is killed reject
	if !rf.killed() {
		fmt.Printf("[AE-RECV] S%d ← S%d: prevLogIdx=%d entries=%d leaderCommit=%d lastIncluded=%d\n",
			rf.me, req.Id, req.PrevLogIndex, len(req.Entries), req.LeaderCommitIndex, rf.lastIncludedIndex)
		// make sure leader term is at least the same as ours
		if req.Term < rf.currentTerm {
			// leader is stale
			res.Term = rf.currentTerm
			res.Appended = false
			return
		}
		// now reset the election timeout
		// we do this early to prevent an election since the term check
		// already shows there's a legitimate leader and it's alive
		// update the election timeout
		rf.setNewElectionTimeout()
		// set ourself to follower just in case we are a candidate
		rf.state = Follower
		// now we check if we have the same entry at the prevLogIndex
		if req.PrevLogIndex >= len(rf.logs)+rf.lastIncludedIndex {
			// bound check
			// we don't have the log at this index so we can reply false immediately
			res.Term = rf.currentTerm
			res.Appended = false
			return
		} else if req.PrevLogIndex < rf.lastIncludedIndex {
			// the leader sent some stale entries that the state machine has applied already
			// we only need the entries not alreasdy in snapshot
			skipCount := rf.lastIncludedIndex - req.PrevLogIndex
			// set the previous log index and term to the snapshot's index
			req.PrevLogIndex = rf.lastIncludedIndex
			req.PrevLogTerm = rf.lastIncludedTerm
			if skipCount > len(req.Entries) {
				// all entries are compacted already
				res.Term = rf.currentTerm
				res.Appended = true
				return
			}
			req.Entries = req.Entries[skipCount:]
		}
		// if the leader's term is higher, we need to update ours also
		if req.Term > rf.currentTerm {
			rf.currentTerm = req.Term
			rf.votedFor = -1
			rf.persist()
		}
		if rf.logs[req.PrevLogIndex-rf.lastIncludedIndex].Term == req.PrevLogTerm {
			// we have the same entry, accept the append entry
			// we check the entries to be appended and log and only truncate where they differ
			// otherwise we append non-existing entries
			convergeIndex := 0

			if len(req.Entries) > 0 {
				// this isn't an heartbeat
				for i, j := 0, req.PrevLogIndex-rf.lastIncludedIndex+1; j < len(rf.logs) && i < len(req.Entries); j++ {
					if rf.logs[j].Term != req.Entries[i].Term {
						// there's a divergent log here
						// delete all the log entries from this index to the end
						rf.logs = rf.logs[:j]
						// append the leader's entries from this index to the end
						rf.logs = append(rf.logs, req.Entries[i:]...)
						rf.persist()
						convergeIndex = len(req.Entries) // to make sure we don't re-append the entry below
						break
					} else {
						i++
						convergeIndex = i
					}
				}
				if convergeIndex < len(req.Entries) {
					// there are un-appended logs
					rf.logs = append(rf.logs, req.Entries[convergeIndex:]...)
					rf.persist()
				}
			}
			// now we set our commit index
			if req.LeaderCommitIndex > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(req.LeaderCommitIndex), float64(len(rf.logs)-1+rf.lastIncludedIndex)))
				// let's signal the applier to apply entries to the state machine
				rf.applyCond.Signal()
			}
			res.Term = rf.currentTerm
			res.Appended = true

		} else {
			// we don't have the same log at the prev index so we reject this entry
			res.Term = rf.currentTerm
			res.Appended = false
			return
		}
	}
}

// This rpc call installs the leaders snapshot of entries up to index i
// it updates the raft instance lastIncludedIndex and lastIncludedTerm
// it persist the change to the raft instance
// it updates the commit index
// trims the rafts logs to [i:]
func (rf *Raft) InstallSnapshot(req InstallSnapshotReq, res *InstallSnapshotRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.killed() {
		fmt.Printf("[IS-RECV] S%d ← S%d: lastIncludedIndex=%d currentLastIncluded=%d\n",
			rf.me, req.LeaderId, req.LastIncludedIndex, rf.lastIncludedIndex)
		if req.Term < rf.currentTerm {
			// the leader is stale return immediately
			res.Term = rf.currentTerm
			return
		}
		rf.setNewElectionTimeout()
		// update our state
		rf.state = Follower
		rf.currentTerm = req.Term
		// now we delete all entries from 0 up to the lastIncludedLogIndex
		if req.LastIncludedIndex > rf.lastIncludedIndex {
			if req.LastIncludedIndex-rf.lastIncludedIndex >= len(rf.logs) {
				// we don't have some logs that are in this new snapshot
				// so we reset our log to sentinel and install snapshot
				rf.logs = []Log{{Term: req.LastIncludedTerm, Entry: []byte{}}}
			} else {
				// we keep all logs after the lasIncludedlog index
				rf.logs = rf.logs[req.LastIncludedIndex-rf.lastIncludedIndex:]
				// set the dummy entry as first entry in logs
				rf.logs[0] = Log{Term: req.LastIncludedTerm, Entry: []byte{}}
			}
		} else {
			// if the snapshot is stale, return immediately
			res.Term = rf.currentTerm
			return
		}
		rf.lastIncludedIndex = req.LastIncludedIndex
		rf.lastIncludedTerm = req.LastIncludedTerm
		// persist state
		rf.persist()
		// save the snapshot
		rf.persister.saveSnapshot(&req.Data, req.Offset, req.Done, req.LastIncludedIndex, req.LastIncludedTerm)
		res.Term = rf.currentTerm
		// we need to update our commit index
		rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(req.LastIncludedIndex)))
		// we need to apply this to the state machine
		rf.applyCond.Signal()
		return
	}
}
