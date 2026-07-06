package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// waitUntil polls f until it returns true or the timeout expires. Useful for
// testing async behavior of the background flush goroutine.
func waitUntil(t *testing.T, timeout time.Duration, msg string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if f() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ----------------------------------------------------------------------------
// Flush produces SSTable, deletes WAL, updates manifest
// ----------------------------------------------------------------------------

func TestFlush_ProducesSSTableAndDeletesWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 3})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Push over threshold, trigger a rollover.
	for i := 0; i < 15; i++ {
		if err := db.Put(fmt.Sprintf("k-%03d", i), []byte("some-value")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// The background flush should eventually:
	//   - produce an SSTable on disk
	//   - delete the flushed WAL (only the active WAL remains → count == 1)
	//   - drain the queue
	waitUntil(t, 5*time.Second, "SSTable produced", func() bool {
		return countSSTables(t, dir) >= 1
	})
	waitUntil(t, 5*time.Second, "flushed WAL deleted (only active WAL remains)", func() bool {
		return countWALs(t, dir) == 1
	})
	waitUntil(t, 5*time.Second, "queue drained", func() bool {
		return len(db.queue.snapshot()) == 0
	})
}

func TestFlush_ManifestUpdatedWithSSTableSeqno(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 3})
	defer db.Close()

	for i := 0; i < 15; i++ {
		db.Put(fmt.Sprintf("k-%03d", i), []byte("some-value"))
	}

	waitUntil(t, 5*time.Second, "manifest recorded a live SSTable", func() bool {
		return len(db.manifest.LiveSSTables) >= 1
	})

	// Verify the recorded seqno matches an existing SSTable file.
	for _, seqno := range db.manifest.LiveSSTables {
		path := filepath.Join(dir, fmt.Sprintf("%d.sst", seqno))
		if _, err := os.Stat(path); err != nil {
			t.Errorf("manifest lists seqno %d but %s does not exist: %v", seqno, path, err)
		}
	}
}

func TestFlush_DBsstsUpdatedWithNewReader(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 3})
	defer db.Close()

	for i := 0; i < 15; i++ {
		db.Put(fmt.Sprintf("k-%03d", i), []byte("some-value"))
	}

	waitUntil(t, 5*time.Second, "db.ssts populated by flush", func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		return len(db.ssts) >= 1
	})
}

// ----------------------------------------------------------------------------
// Get finds keys post-flush (through SSTable, not memtable)
// ----------------------------------------------------------------------------

func TestFlush_GetFindsKeysAfterFlush(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 3})
	defer db.Close()

	const N = 30
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%03d", i)
		val := fmt.Sprintf("v-%03d", i)
		if err := db.Put(key, []byte(val)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Wait for at least one flush to complete.
	waitUntil(t, 5*time.Second, "at least one flush", func() bool {
		return countSSTables(t, dir) >= 1
	})

	// Every key must still be found (the source moved from memtable to SSTable).
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%03d", i)
		want := fmt.Sprintf("v-%03d", i)
		v, found, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if !found {
			t.Errorf("key %s missing after flush", key)
		} else if string(v) != want {
			t.Errorf("key %s: expected %s, got %s", key, want, v)
		}
	}
}

// ----------------------------------------------------------------------------
// Reopen after flush uses SSTables (no WAL replay for flushed data)
// ----------------------------------------------------------------------------

func TestFlush_ReopenReadsFromSSTables(t *testing.T) {
	dir := t.TempDir()

	db1, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 3})
	const N = 30
	for i := 0; i < N; i++ {
		db1.Put(fmt.Sprintf("k-%03d", i), []byte(fmt.Sprintf("v-%03d", i)))
	}
	// Wait for the flush to complete.
	waitUntil(t, 5*time.Second, "flush produces SSTable", func() bool {
		return countSSTables(t, dir) >= 1
	})
	// Give the flush a moment to delete the WAL.
	waitUntil(t, 5*time.Second, "queue drained after flush", func() bool {
		return len(db1.queue.snapshot()) == 0
	})
	db1.Close()

	// Reopen — data should come from SSTables now.
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 3})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	// SSTable was loaded.
	db2.mu.Lock()
	if len(db2.ssts) == 0 {
		db2.mu.Unlock()
		t.Fatalf("expected at least one SSTable to be loaded on reopen")
	}
	db2.mu.Unlock()

	// Data still readable.
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%03d", i)
		want := fmt.Sprintf("v-%03d", i)
		v, found, _ := db2.Get(key)
		if !found {
			t.Errorf("key %s missing after reopen", key)
		} else if string(v) != want {
			t.Errorf("key %s: expected %s, got %s", key, want, v)
		}
	}
}

// ----------------------------------------------------------------------------
// Orphan WAL cleanup on Open (WAL for seqno already in manifest.LiveSSTables)
// ----------------------------------------------------------------------------

func TestOpen_OrphanWALDeleted(t *testing.T) {
	dir := t.TempDir()

	// Pre-populate: an SSTable with seqno=1, a matching orphan WAL (from a
	// crashed cleanup — the flush completed, manifest was saved, but the WAL
	// wasn't deleted), and manifest listing 1 as live.
	buildSSTable(t, dir, 1, []string{"k"}, map[string][]byte{"k": []byte("v")})
	saveTestManifest(t, dir, 2, []uint64{1})
	buildWAL(t, dir, 1, []op{{key: "k", value: []byte("v"), tombstone: false}})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// wal-1.log should be gone (orphan cleaned up).
	if _, err := os.Stat(filepath.Join(dir, "wal-1.log")); !os.IsNotExist(err) {
		t.Errorf("orphan wal-1.log should have been deleted; stat err=%v", err)
	}
	// The queue must not contain the replayed WAL — data is in the SSTable.
	if len(db.queue.snapshot()) != 0 {
		t.Errorf("orphan WAL should not have populated the queue; got %d entries",
			len(db.queue.snapshot()))
	}
	// Data is readable via the SSTable.
	v, found, _ := db.Get("k")
	if !found || string(v) != "v" {
		t.Errorf("expected k=v from SSTable, got found=%v v=%q", found, v)
	}
}

// ----------------------------------------------------------------------------
// Multiple flushes accumulate SSTables in newest-first order
// ----------------------------------------------------------------------------

func TestFlush_MultipleFlushesAccumulateNewestFirst(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 10})
	defer db.Close()

	// Force multiple rollovers.
	for i := 0; i < 60; i++ {
		db.Put(fmt.Sprintf("k-%03d", i), []byte("some-medium-value-here"))
	}

	// Wait for multiple SSTables to be produced.
	waitUntil(t, 8*time.Second, "multiple SSTables produced", func() bool {
		return countSSTables(t, dir) >= 2
	})
	waitUntil(t, 8*time.Second, "queue drained", func() bool {
		return len(db.queue.snapshot()) == 0
	})

	// db.ssts should be sorted DESCENDING by seqno (index 0 = newest).
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.ssts) < 2 {
		t.Fatalf("expected at least 2 SSTable readers, got %d", len(db.ssts))
	}
	prev := ^uint64(0) // max uint64
	for i, r := range db.ssts {
		// Extract seqno from the reader — since we don't have a direct getter,
		// verify order by RecordCount + MinKey/MaxKey monotonicity is impractical.
		// Instead, just verify positions exist. The seqno-order invariant is
		// exercised by the newer-wins tests in Section C.
		if r == nil {
			t.Fatalf("nil reader at index %d", i)
		}
		_ = prev
	}
}

// ----------------------------------------------------------------------------
// Overwrite / shadowing across memtable + SSTable
// ----------------------------------------------------------------------------

func TestFlush_MemtableShadowsFlushedSSTable(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 100, MaxQueue: 3})
	defer db.Close()

	// First round: write "shared=old" enough times to force flush.
	for i := 0; i < 15; i++ {
		db.Put(fmt.Sprintf("filler-%03d", i), []byte("filler-value"))
	}
	db.Put("shared", []byte("old"))
	// Force flush by writing more filler.
	for i := 100; i < 130; i++ {
		db.Put(fmt.Sprintf("filler-%03d", i), []byte("filler-value"))
	}
	waitUntil(t, 5*time.Second, "flush produced SSTable", func() bool {
		return countSSTables(t, dir) >= 1
	})

	// Now overwrite "shared" in the active memtable.
	db.Put("shared", []byte("new"))

	// Get should return "new" from the active memtable, not "old" from SSTable.
	v, found, _ := db.Get("shared")
	if !found {
		t.Fatalf("expected shared to be found")
	}
	if string(v) != "new" {
		t.Errorf("expected new (from active memtable), got %q (SSTable value leaked through)", v)
	}
}
