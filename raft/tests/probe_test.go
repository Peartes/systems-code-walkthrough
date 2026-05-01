package tests

import (
	"fmt"
	"testing"
	"time"
)

func TestSnapshotProbeA(t *testing.T) {
	cfg := make_config(t, 5, false)
	defer cfg.cleanup()

	leader := cfg.checkOneLeader()
	laggard := (leader + 1) % cfg.n
	cfg.crash(laggard)

	for i := 0; i < 6; i++ {
		_, _, ok := cfg.rafts[leader].Start([]byte(fmt.Sprintf("entry-%d", i)))
		if !ok { t.Fatal("rejected") }
	}
	msgs := collectMsgs(t, cfg.applyChan[leader], 6, 5*time.Second)
	snapshotIdx := msgs[len(msgs)-1].Index

	cfg.rafts[leader].Snapshot(snapshotIdx, []byte("snap"))
	cfg.restart(laggard)

	// background monitor: every 100ms print who is leader
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(100 * time.Millisecond):
				for i, r := range cfg.rafts {
					if r == nil { continue }
					term, isLeader := r.GetState()
					if isLeader {
						fmt.Printf("  t=%.1fs: server %d is leader in term %d\n",
							float64(time.Now().UnixMilli()%100000)/1000, i, term)
					}
					_ = term
				}
			}
		}
	}()

	snapMsg, ok := waitForSnapshot(t, cfg.applyChan[laggard], 5*time.Second)
	close(done)
	fmt.Printf("ok=%v snap_nil=%v\n", ok, snapMsg.Snapshot == nil)
}
