package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// submit a command to the leader, wait for it to commit,
// crash the leader, restart it in isolation, and verify
// that the log entry and term survived the restart via persistence.
func TestPersistBasic(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()
	termBefore, _ := cfg.rafts[leader].GetState()

	cmd := []byte("hello")
	idx, _, ok := cfg.rafts[leader].Start(cmd)
	require.True(t, ok, "leader should accept command")
	require.Greater(t, idx, 0)

	checkAppliedLogs(t, cfg, leader, 1)

	cfg.crash(leader)

	newLeader := cfg.checkOneLeader()
	require.NotEqual(t, newLeader, leader, "new leader must not be the crashed node")

	// restart in isolation — anything in its log came purely from the persister
	cfg.restart(leader)
	cfg.disconnect(leader)
	time.Sleep(100 * time.Millisecond)

	ok, entry := cfg.rafts[leader].GetLog(idx)
	require.True(t, ok, "log entry at index %d should exist after restart", idx)
	require.Equal(t, string(cmd), string(entry.Entry),
		"log entry content should match the original command")

	termAfter, _ := cfg.rafts[leader].GetState()
	require.GreaterOrEqual(t, termAfter, termBefore,
		"term must not regress after restart")
}

// crash every server simultaneously, restart them all, and verify
// that every committed entry is intact and the cluster makes progress.
func TestPersistAllCrash(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()

	// submit several commands and wait for all to be applied on the leader
	commands := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	indices := make([]int, len(commands))
	for i, cmd := range commands {
		idx, _, ok := cfg.rafts[leader].Start(cmd)
		require.True(t, ok, "leader should accept command %d", i)
		indices[i] = idx
	}
	checkAppliedLogs(t, cfg, leader, len(commands))

	// kill every node — full cluster outage
	for i := 0; i < cfg.n; i++ {
		cfg.crash(i)
	}

	// bring every node back — each loads state from its persister
	for i := 0; i < cfg.n; i++ {
		cfg.restart(i)
	}

	// cluster must converge to a single leader
	newLeader := cfg.checkOneLeader()

	// every committed entry must be present at its original index on the new leader
	for i, cmd := range commands {
		ok, entry := cfg.rafts[newLeader].GetLog(indices[i])
		require.True(t, ok, "entry %d should survive full cluster crash", indices[i])
		require.Equal(t, string(cmd), string(entry.Entry),
			"entry content at index %d must match original command", indices[i])
	}

	// cluster must still accept new commands after recovery
	newCmd := []byte("post-crash")
	newIdx, _, ok := cfg.rafts[newLeader].Start(newCmd)
	require.True(t, ok, "cluster should accept commands after recovery")
	require.Greater(t, newIdx, 0)
	checkAppliedLogs(t, cfg, newLeader, 1)
}

// partition the leader with one follower, replicate an entry to just those two
// (minority — cannot commit), crash both, let the majority elect a new leader,
// then reconnect and restart the crashed pair. The stale entry must be overwritten
// and the cluster must make correct progress afterward.
func TestPersistLeaderCrashMidReplication(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()

	// choose one follower to stay with the leader in the minority partition
	minority := (leader + 1) % cfg.n

	// isolate the other three — leader and minority follower are now cut off
	majority := []int{}
	for i := 0; i < cfg.n; i++ {
		if i != leader && i != minority {
			cfg.disconnect(i)
			majority = append(majority, i)
		}
	}

	// send a command to the isolated leader.
	// it can replicate to the minority follower but never reach majority — cannot commit.
	staleCmd := []byte("stale-entry")
	staleIdx, _, ok := cfg.rafts[leader].Start(staleCmd)
	require.True(t, ok, "isolated leader should accept the command")

	// wait long enough for the entry to reach the minority follower via heartbeat
	time.Sleep(300 * time.Millisecond)

	// verify the entry landed on the minority follower (it's in-log but not committed)
	ok, entry := cfg.rafts[minority].GetLog(staleIdx)
	require.True(t, ok, "minority follower should have the stale entry in-log")
	require.Equal(t, string(staleCmd), string(entry.Entry))

	// crash both nodes that hold the stale entry
	cfg.crash(leader)
	cfg.crash(minority)

	// reconnect the majority — they can now communicate and elect a leader
	for _, i := range majority {
		cfg.connect(i)
	}

	// majority elects a new leader; it has no knowledge of the stale entry
	newLeader := cfg.checkOneLeader()
	require.NotEqual(t, newLeader, leader)
	require.NotEqual(t, newLeader, minority)

	// commit a fresh command through the new leader to advance its log
	freshCmd := []byte("fresh-entry")
	freshIdx, _, ok := cfg.rafts[newLeader].Start(freshCmd)
	require.True(t, ok)
	checkAppliedLogs(t, cfg, newLeader, 1)

	// restart the old leader and minority follower — they rejoin the cluster
	cfg.restart(leader)
	cfg.restart(minority)

	// give them time to receive AppendEntries from the new leader and overwrite
	// their stale logs
	time.Sleep(time.Second)

	// the stale entry must have been replaced on both restarted nodes.
	// at staleIdx, they should now hold the new leader's entry (or nothing if
	// freshIdx < staleIdx, which cannot happen since freshIdx >= staleIdx).
	// simplest invariant: the new leader's fresh entry exists at freshIdx on
	// both restarted nodes, proving their logs were reconciled.
	for _, node := range []int{leader, minority} {
		ok, e := cfg.rafts[node].GetLog(freshIdx)
		require.True(t, ok,
			"restarted node %d should have caught up to fresh entry at index %d", node, freshIdx)
		require.Equal(t, string(freshCmd), string(e.Entry),
			"restarted node %d log must match new leader's committed entry", node)
	}
}

// submit a command, wait for commit, crash the leader — repeat several rounds.
// after multiple crash/restart cycles, crash every server and restart them all.
// every entry that was committed before a crash must survive.
func TestPersistRepeatedLeaderCrash(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	type committed struct {
		idx int
		cmd []byte
	}
	log := []committed{}

	for round := 0; round < 4; round++ {
		leader := cfg.checkOneLeader()
		cmd := []byte(fmt.Sprintf("round-%d", round))
		idx, _, ok := cfg.rafts[leader].Start(cmd)
		require.True(t, ok, "leader should accept command in round %d", round)
		require.Greater(t, idx, 0)

		// wait for this entry to be applied before crashing
		checkAppliedLogs(t, cfg, leader, 1)
		log = append(log, committed{idx, cmd})

		// crash the leader — a new one will be elected in the next iteration
		cfg.crash(leader)
		cfg.restart(leader)
	}

	// now crash every node simultaneously
	for i := 0; i < cfg.n; i++ {
		cfg.crash(i)
	}

	// restart every node — each reloads its persisted state
	for i := 0; i < cfg.n; i++ {
		cfg.restart(i)
	}

	// cluster must elect a leader after full restart
	finalLeader := cfg.checkOneLeader()

	// every entry committed across all rounds must exist at its recorded index
	for _, entry := range log {
		ok, e := cfg.rafts[finalLeader].GetLog(entry.idx)
		require.True(t, ok,
			"entry at index %d should survive repeated crashes", entry.idx)
		require.Equal(t, string(entry.cmd), string(e.Entry),
			"entry content at index %d must match original", entry.idx)
	}
}
