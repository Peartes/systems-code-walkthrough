package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/peartes/raft/internal/raft"
)

func TestSnapshotS2Debug(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()
	laggard := (leader + 1) % cfg.n
	t.Logf("leader=%d laggard=%d", leader, laggard)

	cfg.crash(laggard)

	const batchSize = 6
	for i := 0; i < batchSize; i++ {
		_, _, ok := cfg.rafts[leader].Start([]byte(fmt.Sprintf("entry-%d", i)))
		if !ok {
			t.Fatalf("leader rejected entry %d", i)
		}
	}
	msgs := collectMsgs(t, cfg.applyChan[leader], batchSize, 5*time.Second)
	snapshotIdx := msgs[len(msgs)-1].Index
	t.Logf("committed %d entries; snapshotIdx=%d", batchSize, snapshotIdx)

	cfg.rafts[leader].Snapshot(snapshotIdx, []byte("snap"))
	t.Logf("snapshot taken")

	cfg.restart(laggard)
	t.Logf("laggard restarted; applyChan=%p", cfg.applyChan[laggard])

	// Drain with per-message logging
	timer := time.After(5 * time.Second)
	msgCount := 0
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	ch := cfg.applyChan[laggard]
	for {
		select {
		case msg := <-ch:
			msgCount++
			t.Logf("  msg #%d: index=%d snapshot_nil=%v", msgCount, msg.Index, msg.Snapshot == nil)
			if msg.Snapshot != nil {
				t.Logf("SUCCESS: snapshot received at index %d", msg.Index)
				// verify
				term, isLeader := cfg.rafts[leader].GetState()
				t.Logf("leader state: term=%d isLeader=%v", term, isLeader)
				return
			}
		case <-tick.C:
			// Print leader state every 500ms
			for i, r := range cfg.rafts {
				if r == nil { continue }
				term, isLeader := r.GetState()
				if isLeader {
					t.Logf("  [tick] server %d is leader at term %d", i, term)
				} else if i == laggard {
					term2, _ := r.GetState()
					t.Logf("  [tick] laggard(%d) term=%d", i, term2)
				}
			}
		case <-timer:
			// Check raft state on laggard
			lTerm, lIsLeader := cfg.rafts[laggard].GetState()
			t.Logf("TIMEOUT: msgCount=%d laggard term=%d isLeader=%v", msgCount, lTerm, lIsLeader)
			t.Fatal("lagging follower never received snapshot ApplyMsg")
		}
	}
}

func waitForSnapshotDebug(ch chan raft.ApplyMsg, deadline time.Duration, logf func(string, ...any)) (raft.ApplyMsg, bool) {
	timer := time.After(deadline)
	for {
		select {
		case msg := <-ch:
			if msg.Snapshot != nil {
				return msg, true
			}
			logf("discarding non-snapshot msg: index=%d", msg.Index)
		case <-timer:
			return raft.ApplyMsg{}, false
		}
	}
}
