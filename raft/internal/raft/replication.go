package raft

import (
	"math"
)

func (rf *Raft) AppendEntries(req AppendEntryReq, res *AppendEntryRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if server is killed reject
	if !rf.killed() {
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
		// if the leader's term is higher, we need to update ours also
		if req.Term > rf.currentTerm {
			rf.currentTerm = req.Term
		}
		// now we check if we have the same entry at the prevLogIndex
		if req.PrevLogIndex >= len(rf.logs) {
			// bound check
			// we don't have the log at this index so we can reply false immediately
			res.Term = rf.currentTerm
			res.Appended = false
			return
		}
		if rf.logs[req.PrevLogIndex].Term == req.PrevLogTerm {
			// we have the same entry, accept the append entry
			// we check the entries to be appended and log and only truncate where they differ
			// otherwise we append non-existing entries
			convergeIndex := 0

			if len(req.Entries) > 0 {
				// this isn't an heartbeat
				for i, j := 0, req.PrevLogIndex+1; j < len(rf.logs) && i < len(req.Entries); j++ {
					if rf.logs[j].Term != req.Entries[i].Term {
						// there's a divergent log here
						// delete all the log entries from this index to the end
						rf.logs = rf.logs[:j]
						// append the leader's entries from this index to the end
						rf.logs = append(rf.logs, req.Entries[i:]...)
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
				}
			}
			// now we set our commit index
			if req.LeaderCommitIndex > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(req.LeaderCommitIndex), float64(len(rf.logs)-1)))
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
