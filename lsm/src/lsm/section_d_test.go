package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// countWALs scans the directory and returns the number of wal-*.log files present.
func countWALs(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	count := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".log") {
			count++
		}
	}
	return count
}

// ----------------------------------------------------------------------------
// Threshold triggers rollover
// ----------------------------------------------------------------------------

func TestRollover_TriggeredByThreshold(t *testing.T) {
	dir := t.TempDir()
	// 100-byte threshold: a few Puts will exceed it.
	db, err := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	startWALSeqno := db.walSeqno
	if got := countWALs(t, dir); got != 1 {
		t.Fatalf("expected 1 WAL file on fresh Open, got %d", got)
	}

	// Write enough keys to force at least one rollover.
	// Each Put(k=8 bytes, v=8 bytes) adds ~16 bytes → threshold at 100 = ~7 Puts.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val := fmt.Sprintf("val-%03d", i)
		if err := db.Put(key, []byte(val)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Post-rollover: WAL count must have grown.
	if got := countWALs(t, dir); got < 2 {
		t.Errorf("expected multiple WAL files after rollovers, got %d", got)
	}

	// db.walSeqno advanced.
	if db.walSeqno <= startWALSeqno {
		t.Errorf("walSeqno should advance after rollover: start=%d, now=%d", startWALSeqno, db.walSeqno)
	}

	// The queue has at least one entry (the frozen memtable).
	snap := db.queue.snapshot()
	if len(snap) == 0 {
		t.Errorf("expected at least 1 frozen memtable in the queue after rollover")
	}
}

func TestRollover_ActiveMemtableIsEmptyAfterRollover(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Push over the threshold.
	for i := 0; i < 15; i++ {
		db.Put(fmt.Sprintf("k-%03d", i), []byte("some-value"))
	}
	// After that flurry, the active memtable should be smaller than threshold
	// (rollover reset it; may have accumulated a few post-rollover writes).
	if sz := db.active.SizeBytes(); sz > 100 {
		t.Errorf("expected active memtable below threshold post-rollover, got SizeBytes=%d", sz)
	}
}

func TestRollover_FrozenMemtableStillReadable(t *testing.T) {
	// The frozen memtable moves to the queue; Get must still find its keys
	// via the queue snapshot.
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	defer db.Close()

	// Write enough to force a rollover, then verify the earliest key is still findable.
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("k-%03d", i)
		val := fmt.Sprintf("v-%03d", i)
		db.Put(key, []byte(val))
	}

	// The queue must contain at least one entry now.
	if len(db.queue.snapshot()) == 0 {
		t.Fatalf("expected rollover to populate the queue")
	}

	// All 15 keys are queryable across active + queued memtables.
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("k-%03d", i)
		wantVal := fmt.Sprintf("v-%03d", i)
		v, found, _ := db.Get(key)
		if !found {
			t.Errorf("key %s missing after rollover", key)
		} else if string(v) != wantVal {
			t.Errorf("key %s: expected %s, got %s", key, wantVal, v)
		}
	}
}

// ----------------------------------------------------------------------------
// Multiple rollovers
// ----------------------------------------------------------------------------

func TestRollover_MultipleRolloversInOrder(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	defer db.Close()

	// Force multiple rollovers.
	for i := 0; i < 60; i++ {
		db.Put(fmt.Sprintf("k-%03d", i), []byte("some-medium-value-here"))
	}

	// Queue should have several entries. Their seqnos must be strictly ascending.
	snap := db.queue.snapshot()
	if len(snap) < 2 {
		t.Fatalf("expected multiple rollovers, queue has %d entries", len(snap))
	}
	// snap is newest-first — reverse for order-check.
	prev := uint64(0)
	for i := len(snap) - 1; i >= 0; i-- {
		if snap[i].seqno <= prev && prev != 0 {
			t.Errorf("queue entries not in ascending seqno order: %d then %d", prev, snap[i].seqno)
		}
		prev = snap[i].seqno
	}
}

func TestRollover_ManifestNextSeqNoAdvances(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	defer db.Close()

	startNextSeqNo := db.manifest.NextSeqNo
	for i := 0; i < 40; i++ {
		db.Put(fmt.Sprintf("k-%03d", i), []byte("medium-sized-value"))
	}

	if db.manifest.NextSeqNo <= startNextSeqNo {
		t.Errorf("NextSeqNo should advance across rollovers: start=%d, now=%d",
			startNextSeqNo, db.manifest.NextSeqNo)
	}
}

// ----------------------------------------------------------------------------
// Delete also triggers rollover
// ----------------------------------------------------------------------------

func TestRollover_DeleteAlsoTriggersRollover(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 50, MaxQueue: 100})
	defer db.Close()

	// Delete-only workload where each Delete adds keylen bytes to the memtable.
	for i := 0; i < 20; i++ {
		db.Delete(fmt.Sprintf("deleted-key-%03d", i))
	}
	if len(db.queue.snapshot()) == 0 {
		t.Errorf("expected Delete calls to trigger at least one rollover")
	}
}

// ----------------------------------------------------------------------------
// Durability across rollover — WAL files preserve data
// ----------------------------------------------------------------------------

func TestRollover_DurabilityAcrossReopen(t *testing.T) {
	// Write, force rollover, close, reopen. All data must be recoverable from the
	// still-existing WAL segments (Section E hasn't deleted them yet).
	dir := t.TempDir()

	db1, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	const N = 50
	for i := 0; i < N; i++ {
		if err := db1.Put(fmt.Sprintf("k-%03d", i), []byte(fmt.Sprintf("val-%03d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Trigger at least one rollover; queue snapshot should be non-empty.
	if len(db1.queue.snapshot()) == 0 {
		t.Fatalf("expected at least one rollover")
	}
	db1.Close()

	// Reopen. WAL segments from before the close are replayed.
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	// Verify all N keys are recoverable.
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%03d", i)
		want := fmt.Sprintf("val-%03d", i)
		v, found, _ := db2.Get(key)
		if !found {
			t.Errorf("key %s missing after reopen", key)
		} else if string(v) != want {
			t.Errorf("key %s: expected %s, got %s", key, want, v)
		}
	}
}

// ----------------------------------------------------------------------------
// New WAL file is created and named by seqno
// ----------------------------------------------------------------------------

func TestRollover_NewWALFileNamedBySeqno(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	defer db.Close()

	firstSeqno := db.walSeqno
	firstWALPath := filepath.Join(dir, fmt.Sprintf("wal-%d.log", firstSeqno))

	// Force one rollover.
	for i := 0; i < 15; i++ {
		db.Put(fmt.Sprintf("k-%03d", i), []byte("some-value"))
	}

	if db.walSeqno == firstSeqno {
		t.Fatalf("expected walSeqno to change after rollover")
	}

	// Both the old WAL file (still needed until Section E flushes it) and the new
	// WAL file should exist on disk.
	if _, err := os.Stat(firstWALPath); err != nil {
		t.Errorf("old WAL file should still exist (not deleted until flush): %v", err)
	}
	newWALPath := filepath.Join(dir, fmt.Sprintf("wal-%d.log", db.walSeqno))
	if _, err := os.Stat(newWALPath); err != nil {
		t.Errorf("expected new WAL file at %s: %v", newWALPath, err)
	}
}

// ----------------------------------------------------------------------------
// Rollover doesn't affect SSTable-backed keys
// ----------------------------------------------------------------------------

func TestRollover_ExistingSSTablesRemainQueryable(t *testing.T) {
	dir := t.TempDir()

	// Pre-populate an SSTable.
	buildSSTable(t, dir, 1, []string{"sst-only-key"}, map[string][]byte{
		"sst-only-key": []byte("sst-value"),
	})
	saveTestManifest(t, dir, 2, []uint64{1})

	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 100})
	defer db.Close()

	// Force rollovers with unrelated keys.
	for i := 0; i < 30; i++ {
		db.Put(fmt.Sprintf("mt-key-%03d", i), []byte("medium-sized-value"))
	}

	// The SSTable key must still be reachable.
	v, found, _ := db.Get("sst-only-key")
	if !found {
		t.Errorf("SSTable key should still be findable after rollover")
	}
	if string(v) != "sst-value" {
		t.Errorf("expected sst-value, got %q", v)
	}
}
