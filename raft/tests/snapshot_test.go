package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/peartes/raft/internal/raft"
	"github.com/stretchr/testify/require"
)

// collectMsgs drains exactly n ApplyMsg values from ch within deadline.
// It accepts both snapshot messages and regular entry messages.
// The test fatals if the deadline is exceeded.
func collectMsgs(t *testing.T, ch chan raft.ApplyMsg, n int, deadline time.Duration) []raft.ApplyMsg {
	t.Helper()
	msgs := make([]raft.ApplyMsg, 0, n)
	timer := time.After(deadline)
	for len(msgs) < n {
		select {
		case msg := <-ch:
			msgs = append(msgs, msg)
		case <-timer:
			t.Fatalf("collectMsgs timeout: collected %d of %d messages", len(msgs), n)
		}
	}
	return msgs
}

// waitForSnapshot drains the channel until a snapshot ApplyMsg (Snapshot != nil) arrives
// or the deadline expires. Returns the snapshot message and true on success.
func waitForSnapshot(t *testing.T, ch chan raft.ApplyMsg, deadline time.Duration) (raft.ApplyMsg, bool) {
	t.Helper()
	timer := time.After(deadline)
	for {
		select {
		case msg := <-ch:
			if msg.Snapshot != nil {
				return msg, true
			}
		case <-timer:
			return raft.ApplyMsg{}, false
		}
	}
}

// TC-S1: Basic snapshot path.
//
// The leader applies 5 entries, the state machine takes a snapshot, and then
// 3 more entries are submitted. The post-snapshot entries must still be
// replicated and delivered with contiguous, monotonically increasing indices.
func TestSnapshotBasic(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()

	// Submit 5 commands and wait for all to be applied on the leader.
	const batchSize = 5
	for i := 0; i < batchSize; i++ {
		_, _, ok := cfg.rafts[leader].Start([]byte(fmt.Sprintf("cmd-%d", i)))
		require.True(t, ok, "leader must accept command %d", i)
	}
	preMsgs := collectMsgs(t, cfg.applyChan[leader], batchSize, 5*time.Second)
	require.Len(t, preMsgs, batchSize)

	// State machine hands the snapshot down to Raft.
	snapshotIdx := preMsgs[len(preMsgs)-1].Index
	cfg.rafts[leader].Snapshot(snapshotIdx, []byte("snapshot-of-first-5-entries"))

	// Submit 3 more commands after compaction.
	for i := 0; i < 3; i++ {
		_, _, ok := cfg.rafts[leader].Start([]byte(fmt.Sprintf("post-snap-%d", i)))
		require.True(t, ok, "leader must accept command after snapshot")
	}
	postMsgs := collectMsgs(t, cfg.applyChan[leader], 3, 5*time.Second)
	require.Len(t, postMsgs, 3)

	// Indices must continue exactly from where the snapshot left off.
	for i, msg := range postMsgs {
		want := snapshotIdx + 1 + i
		require.Equal(t, want, msg.Index,
			"post-snapshot entry %d must have index %d", i, want)
		require.Nil(t, msg.Snapshot,
			"post-snapshot ApplyMsg must not carry snapshot data")
	}
}

// TC-S2: A lagging follower (crashed before any entries were replicated) must
// receive InstallSnapshot from the leader when it restarts — because the
// entries it needs have already been compacted away.
//
// We use crash/restart rather than disconnect/connect. A disconnected node
// keeps running, fires elections, and inflates its term; when it reconnects
// with a higher term the current leader steps down and the new leader may not
// have the snapshot. A crashed node's goroutines stop immediately, so it
// cannot run elections, and after restart it becomes a follower without
// disrupting the existing leader.
//
// Stabilization window: the network's broadcast channel is unbuffered, so a
// goroutine calling peer.Call() blocks until the network dispatcher reads it.
// If a goroutine built its AppendEntries request before the snapshot was taken
// (reading lastIncludedIndex=0) and is still waiting in the broadcast queue
// when restart() re-enables the endpoint, the dispatcher will see connected=true
// and deliver the stale AE to the new follower instance.  That lets the
// follower catch up via regular entries before InstallSnapshot arrives, so it
// never emits a snapshot ApplyMsg.
//
// The fix is to sleep long enough after taking the snapshot (and before
// restarting the laggard) for the network dispatcher to drain all queued
// requests while connected=false.  Any goroutine that built a stale request
// will have its call fail immediately (connected=false) during that window, so
// no stale AE can race with the restart.
func TestSnapshotLaggingFollowerInstallsSnapshot(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()

	// Crash one follower before any entries are written.
	// Its goroutines stop — no elections, no term inflation.
	laggard := (leader + 1) % cfg.n
	cfg.crash(laggard)

	// Stabilization: give the network dispatcher time to drain any goroutines
	// that were mid-flight to the laggard when crash() was called.  All calls
	// to the laggard now fail immediately (connected=false), so a brief wait
	// is sufficient — the dispatcher processes each queued request in
	// microseconds.
	time.Sleep(200 * time.Millisecond)

	// Commit 6 entries on the 4-node majority.
	const batchSize = 6
	for i := 0; i < batchSize; i++ {
		_, _, ok := cfg.rafts[leader].Start([]byte(fmt.Sprintf("entry-%d", i)))
		require.True(t, ok, "leader must accept command %d", i)
	}
	msgs := collectMsgs(t, cfg.applyChan[leader], batchSize, 5*time.Second)
	snapshotIdx := msgs[len(msgs)-1].Index

	// Leader snapshots all 6 entries. The laggard never replicated any of
	// them, so nextIndex[laggard] = 1 <= lastIncludedIndex = snapshotIdx.
	cfg.rafts[leader].Snapshot(snapshotIdx, []byte("snap-after-6-entries"))

	// Stabilization: goroutines spawned during the replication phase read
	// lastIncludedIndex=0 (before the snapshot) and would route to
	// AppendEntries.  Sleep long enough for the dispatcher to read and fail
	// all of them while connected=false, before we re-enable the laggard.
	time.Sleep(200 * time.Millisecond)

	// Restart the laggard. It comes back at its persisted term (no inflation),
	// becomes a follower, and on the next heartbeat the leader sends
	// InstallSnapshot because nextIndex[laggard] (= 1) <= lastIncludedIndex.
	cfg.restart(laggard)

	// The laggard's applyChan must receive a snapshot ApplyMsg.
	snapMsg, ok := waitForSnapshot(t, cfg.applyChan[laggard], 5*time.Second)
	require.True(t, ok, "lagging follower must receive a snapshot ApplyMsg after restart")
	require.Equal(t, snapshotIdx, snapMsg.Index,
		"snapshot ApplyMsg.Index must equal lastIncludedIndex (%d)", snapshotIdx)
	require.NotNil(t, snapMsg.Snapshot,
		"snapshot ApplyMsg must carry the snapshot bytes")
}

// TC-S3: Snapshot survives a leader crash and enables the restarted node to
// replay the snapshot before applying subsequent entries.
//
// Only the original leader takes a snapshot. After it crashes and restarts,
// it reloads lastIncludedIndex from the persister. When the cluster
// (led by the new leader) commits a fresh entry, the restarted node's applier
// detects lastApplied < lastIncludedIndex and delivers the snapshot ApplyMsg
// first, then the new entry. We check the original leader's applyChan
// specifically because it is the only node guaranteed to have lastIncludedIndex > 0.
func TestSnapshotSurvivesFullCrash(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()

	// Commit 4 entries then snapshot them all on the leader.
	const batchSize = 4
	for i := 0; i < batchSize; i++ {
		_, _, ok := cfg.rafts[leader].Start([]byte(fmt.Sprintf("pre-snap-%d", i)))
		require.True(t, ok)
	}
	preMsgs := collectMsgs(t, cfg.applyChan[leader], batchSize, 5*time.Second)
	snapshotIdx := preMsgs[len(preMsgs)-1].Index
	cfg.rafts[leader].Snapshot(snapshotIdx, []byte("persisted-snapshot"))

	// Crash only the leader — the remaining 4 nodes form a majority and
	// elect a new leader without losing any committed state.
	cfg.crash(leader)

	newLeader := cfg.checkOneLeader()
	require.NotEqual(t, newLeader, leader)

	// Submit a fresh command through the new leader and wait for it to commit.
	_, _, ok := cfg.rafts[newLeader].Start([]byte("post-crash-cmd"))
	require.True(t, ok, "new leader must accept commands after original leader crash")
	collectMsgs(t, cfg.applyChan[newLeader], 1, 5*time.Second)

	// Restart the original leader. It reloads lastIncludedIndex = snapshotIdx
	// from the persister and becomes a follower of the new leader.
	cfg.restart(leader)

	// The new leader will send heartbeats/AppendEntries to the restarted node.
	// When the restarted node's commitIndex advances past lastIncludedIndex,
	// its applier delivers: (1) the snapshot ApplyMsg, then (2) the new entry.
	all := collectMsgs(t, cfg.applyChan[leader], 2, 5*time.Second)

	// First message must be the snapshot replay.
	require.NotNil(t, all[0].Snapshot,
		"first ApplyMsg on restarted node must be the snapshot replay")
	require.Equal(t, snapshotIdx, all[0].Index,
		"snapshot replay index must equal lastIncludedIndex (%d)", snapshotIdx)

	// Second message must be the post-crash command.
	require.Nil(t, all[1].Snapshot,
		"second ApplyMsg must be the new log entry, not a snapshot")
	require.Equal(t, "post-crash-cmd", string(all[1].Entry.Entry),
		"second ApplyMsg must carry the newly submitted command")
	require.Equal(t, snapshotIdx+1, all[1].Index,
		"new entry index must immediately follow the snapshot index")
}

// TC-S4: Repeated snapshots — leader snapshots after each batch of entries.
//
// Over 3 rounds the leader: submits entries, waits for commit, snapshots.
// After all rounds a final entry is submitted. Verifies the Raft machinery
// stays healthy across multiple compactions: indices remain monotonic and
// the final entry is accepted and applied.
func TestSnapshotRepeated(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()

	totalApplied := 0
	lastSnapshotIdx := 0

	for round := 0; round < 3; round++ {
		// Submit 3 entries per round.
		for i := 0; i < 3; i++ {
			_, _, ok := cfg.rafts[leader].Start([]byte(fmt.Sprintf("r%d-e%d", round, i)))
			require.True(t, ok, "round %d: leader must accept entry %d", round, i)
		}
		msgs := collectMsgs(t, cfg.applyChan[leader], 3, 5*time.Second)
		totalApplied += 3
		lastSnapshotIdx = msgs[len(msgs)-1].Index

		// Snapshot after each round — compact everything committed so far.
		cfg.rafts[leader].Snapshot(lastSnapshotIdx, []byte(fmt.Sprintf("snap-round-%d", round)))
	}

	// Submit one final entry — must be accepted and applied with the correct index.
	_, _, ok := cfg.rafts[leader].Start([]byte("final"))
	require.True(t, ok, "leader must accept final command after 3 rounds of snapshotting")

	finalMsgs := collectMsgs(t, cfg.applyChan[leader], 1, 5*time.Second)
	require.Len(t, finalMsgs, 1)
	require.Nil(t, finalMsgs[0].Snapshot,
		"final ApplyMsg must be a log entry, not a snapshot")
	require.Equal(t, lastSnapshotIdx+1, finalMsgs[0].Index,
		"final entry index must immediately follow the last snapshot index (%d)", lastSnapshotIdx)
	require.Equal(t, "final", string(finalMsgs[0].Entry.Entry),
		"final entry content must match submitted command")
}
