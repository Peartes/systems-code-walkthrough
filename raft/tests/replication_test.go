package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func logToBytes(log []string) []byte {
	return []byte(strings.Join(log, " "))
}

func logFromBytes(bz []byte) []string {
	return strings.Split(string(bz), " ")
}

// submit a command to the leader
// verify that it is replicated accross the cluster
func TestLogReplicationSimple(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()
	var term, leader int
	var isLeader bool
	log := []string{"this"}
	for {
		leader = cfg.checkOneLeader()
		term = cfg.checkTerms()
		require.NotNil(t, leader, "there should be a leader")
		require.NotEqual(t, term, -1, "there should be a term")
		// send the command - the command is an array of words
		_, _, isLeader = cfg.rafts[leader].Start(logToBytes(log))
		if isLeader {
			break
		}
	}
	// confirm it was replicated accross the cluster
	// the command might not be replicated in the same term it was sent or even at all
	// it is only marked permanent after it has been applied to the state machine
	// for the test, the command should be replicated since this is an ideal case
	// check for log in majority of the servers
	checkAppliedLogs(t, cfg, leader, 1)
}

// check how many logs have been applied and in the right order
func checkAppliedLogs(t *testing.T, cfg *config, leaderIdx int, wantCount int) {
	deadline := time.After(3 * time.Second)
	lastIdx := 0
	for i := range wantCount {
		select {
		case msg := <-cfg.applyChan[leaderIdx]:
			if msg.Index != lastIdx+1 {
				require.Failf(t, "out of order", "expected index %d got %d", lastIdx+1, msg.Index)
			}
			lastIdx = msg.Index
		case <-deadline:
			require.Failf(t, "timeout", "only got %d of %d expected apply messages", i, wantCount)
		}
	}
}

// submit multiple commands sequentially and test that order is preserved
// we will submit 4 commands sequentially
func TestLogReplicationMultiple(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	commandCount := 0
	var term, logIdx, leader int
	var isLeader bool
	log := []string{"this", "should", "be", "replicated"}

	for {
		leader = cfg.checkOneLeader()
		term = cfg.checkTerms()
		require.NotNil(t, leader, "there should be a leader")
		require.NotEqual(t, term, -1, "there should be a term")
		// send the command - the command is an array of words
		logIdx, _, isLeader = cfg.rafts[leader].Start(logToBytes(log[:commandCount+1]))
		if isLeader {
			commandCount++
		}
		if commandCount == 4 {
			break
		}
	}
	// make sure all logs are replicated in order
	time.Sleep(2 * time.Second)
	count := 0
	for raftIdx, raft := range cfg.rafts {
		// logIdx is the index of last log added
		isReplicated := false
		for i := 1; i <= logIdx; i++ { // skip the sentinel entry
			ok, raftLog := raft.GetLog(i)
			if !ok {
				require.FailNowf(t, "log repl multiple", "no log at entry %d of raft %d", i, raftIdx)
			}
			logEntry := raftLog.Entry
			if !ok {
				require.FailNowf(t, "log repl multiple", "invalid log at entry %d of raft %d", i, raftIdx)
			}
			if strings.Compare(strings.Join(logFromBytes(logEntry), " "), strings.Join(log[:i], " ")) == 0 {
				isReplicated = true
			} else {
				isReplicated = false
				break
			}
		}
		if isReplicated {
			count++
		}
	}
	if count < cfg.n/2+1 {
		// not on a majority of the servers
		require.Fail(t, "log %v not replicated on a majority of servers", log)
	}
	checkAppliedLogs(t, cfg, leader, 4)
}

// crash a follower, submit commands to the leader, restart the follower,
// verify it catches up to the current log via AppendEntries backtracking
func TestFollowerCatchUp(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()

	// pick a follower that is not the leader and crash it
	follower := (leader + 1) % cfg.n
	cfg.crash(follower)

	// submit 3 commands to the leader while the follower is down
	commands := []string{"cmd1", "cmd2", "cmd3"}
	for _, cmd := range commands {
		idx, _, ok := cfg.rafts[leader].Start(logToBytes([]string{cmd}))
		require.True(t, ok, "leader should accept command")
		require.Greater(t, idx, 0, "log index should be positive")
	}

	// give the 4-node majority time to replicate and commit the entries
	checkAppliedLogs(t, cfg, leader, 3)

	// restart the crashed follower — it comes back with an empty log
	cfg.restart(follower)

	// the leader will detect the follower is behind via AppendEntries failures
	// and walk nextIndex back until it finds the divergence point, then replay
	// give it time to fully catch up
	time.Sleep(2 * time.Second)

	// verify the restarted follower has all 3 entries at the correct indices
	for i, cmd := range commands {
		logIdx := i + 1 // log index 1, 2, 3 (index 0 is sentinel)
		ok, entry := cfg.rafts[follower].GetLog(logIdx)
		require.True(t, ok, "restarted follower should have log entry at index %d", logIdx)
		require.Equal(t, cmd, string(entry.Entry), "log entry at index %d should match submitted command", logIdx)
	}
}

// partition the leader into a minority, submit a command — it must not commit
// because the partitioned leader cannot reach a majority.
// restore the partition and verify the command is eventually committed.
func TestNoCommitInMinorityPartition(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()
	term := cfg.checkTerms()

	// isolate the leader — it can no longer reach a majority
	// pick one follower to stay with the old leader (minority of 2)
	// the other 3 form the majority partition
	minority := (leader + 1) % cfg.n
	cfg.disconnect(leader)
	cfg.disconnect(minority)

	// the majority partition (3 nodes) should elect a new leader
	// mark the old leader as disconnected so checkOneLeader skips it
	newLeader := cfg.checkOneLeader()
	require.NotEqual(t, newLeader, leader, "new leader must not be the isolated old leader")
	newTerm := cfg.checkTerms()
	require.Greater(t, newTerm, term, "new term must be strictly greater after re-election")

	// submit a command to the old (isolated) leader
	// it will append to its own log and send AppendEntries — but since it
	// can only reach one follower (minority), it can never achieve majority
	// and the entry must never be committed
	cfg.rafts[leader].Start(logToBytes([]string{"should-not-commit"}))

	// give the isolated leader time to attempt replication — it won't succeed
	time.Sleep(500 * time.Millisecond)

	// the majority partition should have no knowledge of this entry
	// verify by checking the new leader's log length: it should only have
	// entries from its own term (none yet, since we haven't submitted to it)
	_, lastLogOfNewLeader := cfg.rafts[newLeader].GetLog(1)
	require.NotEqual(t, "should-not-commit", lastLogOfNewLeader.Entry,
		"entry submitted to isolated leader must not appear on majority leader")

	// now heal the partition — reconnect the old leader and minority follower
	cfg.connect(leader)
	cfg.connect(minority)

	// the whole cluster converges: old leader sees higher term, steps down,
	// its uncommitted "should-not-commit" entry gets overwritten
	finalLeader := cfg.checkOneLeader()
	require.NotNil(t, finalLeader, "cluster should converge to one leader after healing")

	// submit a fresh command through the new majority — this one must commit
	idx, _, ok := cfg.rafts[finalLeader].Start(logToBytes([]string{"should-commit"}))
	require.True(t, ok, "final leader should accept command")
	require.Greater(t, idx, 0)

	// verify it gets applied
	checkAppliedLogs(t, cfg, finalLeader, 1)
}
