package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// Hard-crash simulations
// ----------------------------------------------------------------------------

// TestCrash_NoCloseDataStillDurable simulates a hard crash by NOT calling Close.
// All Puts must be recoverable on reopen because the WAL is fsynced per Append.
func TestCrash_NoCloseDataStillDurable(t *testing.T) {
	dir := t.TempDir()

	db1, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const N = 100
	for i := 0; i < N; i++ {
		if err := db1.Put(fmt.Sprintf("k-%03d", i), []byte(fmt.Sprintf("v-%03d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// "Crash": release only the active WAL fd; don't call Close (which would
	// wait for flush drain + close SSTables + save manifest cleanly).
	// The OS releases everything on process exit; we simulate that release
	// by closing just the WAL fd so the next Open can access the files.
	db1.mu.Lock()
	db1.wal.Close()
	db1.mu.Unlock()

	// Reopen: WAL is replayed.
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("reopen after simulated crash: %v", err)
	}
	defer db2.Close()

	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%03d", i)
		want := fmt.Sprintf("v-%03d", i)
		v, found, _ := db2.Get(key)
		if !found {
			t.Errorf("key %s missing after simulated crash", key)
		} else if string(v) != want {
			t.Errorf("key %s: expected %s, got %s", key, want, v)
		}
	}
}

// TestCrash_MidFlushRecoversFromWAL simulates a crash where the flush had
// started but the SSTable hadn't been committed to the manifest. The pending
// WAL segments (not orphaned) get replayed. Data must survive.
func TestCrash_MidFlushRecoversFromWAL(t *testing.T) {
	dir := t.TempDir()

	// Small threshold + small queue → many rollovers, some may not have flushed.
	db1, _ := Open(dir, testSeed, Options{FlushThreshold: 80, MaxQueue: 5}, false)
	const N = 60
	for i := 0; i < N; i++ {
		if err := db1.Put(fmt.Sprintf("k-%03d", i), []byte(fmt.Sprintf("val-%03d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Simulate crash immediately — no waiting for background flush.
	db1.mu.Lock()
	db1.wal.Close()
	db1.mu.Unlock()

	db2, err := Open(dir, testSeed, Options{FlushThreshold: 80, MaxQueue: 5}, false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	// All keys recoverable: some via SSTables (if their flush completed), the
	// rest via replayed WAL segments (which Open pushes onto the queue).
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%03d", i)
		want := fmt.Sprintf("val-%03d", i)
		v, found, _ := db2.Get(key)
		if !found {
			t.Errorf("key %s missing after mid-flush crash simulation", key)
		} else if string(v) != want {
			t.Errorf("key %s: expected %s, got %s", key, want, v)
		}
	}
}

// TestCrash_OrphanWALAndOrphanSSTableCleaned simulates the mid-flush crash
// state on disk directly: an SSTable listed in the manifest whose WAL was
// never deleted (orphan WAL) AND a partial SSTable whose seqno never made
// it into the manifest (orphan SSTable). Open must clean both up.
func TestCrash_OrphanWALAndOrphanSSTableCleaned(t *testing.T) {
	dir := t.TempDir()

	// Setup: seqno=1 was flushed successfully — SSTable in manifest, but WAL
	// wasn't deleted (crashed at Phase 3). Seqno=2's flush wrote a partial
	// SSTable file but never got listed in the manifest.
	buildSSTable(t, dir, 1, []string{"live"}, map[string][]byte{"live": []byte("committed")})
	buildWAL(t, dir, 1, []op{{key: "live", value: []byte("committed"), tombstone: false}}) // orphan
	buildSSTable(t, dir, 2, []string{"orphan"}, map[string][]byte{"orphan": []byte("uncommitted")})
	saveTestManifest(t, dir, 3, []uint64{1}) // only 1 is live; 2 is orphan

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Orphan WAL should be gone.
	if _, err := os.Stat(filepath.Join(dir, "wal-1.log")); !os.IsNotExist(err) {
		t.Errorf("orphan wal-1.log should have been deleted; stat err=%v", err)
	}
	// Orphan SSTable should be gone.
	if _, err := os.Stat(filepath.Join(dir, "2.sst")); !os.IsNotExist(err) {
		t.Errorf("orphan 2.sst should have been deleted; stat err=%v", err)
	}
	// Live SSTable's data is queryable.
	v, found, _ := db.Get("live")
	if !found || string(v) != "committed" {
		t.Errorf("committed data missing: found=%v v=%q", found, v)
	}
	// Orphan SSTable's data is NOT queryable.
	if _, found, _ := db.Get("orphan"); found {
		t.Errorf("orphan SSTable's data should not be accessible after cleanup")
	}
}

// ----------------------------------------------------------------------------
// Multi-generation shadowing across flushes
// ----------------------------------------------------------------------------

func TestShadowing_PutFlushDeleteFlushReopen(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 80, MaxQueue: 3}, false)

	// Round 1: Put target=v1 and enough filler to force a flush.
	db.Put("target", []byte("v1"))
	for i := 0; i < 10; i++ {
		db.Put(fmt.Sprintf("filler-a-%d", i), []byte("filler-value"))
	}
	waitUntil(t, 5*time.Second, "first flush", func() bool { return countSSTables(t, dir) >= 1 })

	// Round 2: Delete target and force another flush.
	db.Delete("target")
	for i := 0; i < 10; i++ {
		db.Put(fmt.Sprintf("filler-b-%d", i), []byte("filler-value"))
	}
	waitUntil(t, 5*time.Second, "second flush", func() bool { return countSSTables(t, dir) >= 2 })

	// While still open — should be gone.
	if _, found, _ := db.Get("target"); found {
		t.Errorf("target should be tombstoned across two flushes")
	}
	db.Close()

	// Reopen: same story, all from SSTables.
	db2, _ := Open(dir, testSeed, Options{FlushThreshold: 80, MaxQueue: 3}, false)
	defer db2.Close()
	if _, found, _ := db2.Get("target"); found {
		t.Errorf("target should stay tombstoned after reopen")
	}
}

func TestShadowing_OverwriteAcrossFlushes(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 80, MaxQueue: 3}, false)

	// Round 1: k=v1, flush.
	db.Put("k", []byte("v1"))
	for i := 0; i < 10; i++ {
		db.Put(fmt.Sprintf("filler-a-%d", i), []byte("filler-value"))
	}
	waitUntil(t, 5*time.Second, "first flush", func() bool { return countSSTables(t, dir) >= 1 })

	// Round 2: k=v2, flush.
	db.Put("k", []byte("v2"))
	for i := 0; i < 10; i++ {
		db.Put(fmt.Sprintf("filler-b-%d", i), []byte("filler-value"))
	}
	waitUntil(t, 5*time.Second, "second flush", func() bool { return countSSTables(t, dir) >= 2 })

	// Round 3: k=v3, keep in active memtable (no flush needed).
	db.Put("k", []byte("v3"))

	// Newest wins (memtable).
	v, _, _ := db.Get("k")
	if string(v) != "v3" {
		t.Errorf("expected v3 from memtable, got %q", v)
	}
	db.Close()

	// Reopen: newest wins (SSTable) — active memtable's v3 is in the WAL
	// and gets replayed onto the queue.
	db2, _ := Open(dir, testSeed, Options{FlushThreshold: 80, MaxQueue: 3}, false)
	defer db2.Close()
	v, _, _ = db2.Get("k")
	if string(v) != "v3" {
		t.Errorf("after reopen expected v3, got %q", v)
	}
}

// ----------------------------------------------------------------------------
// Concurrent readers + writer (data race check — run with -race)
// ----------------------------------------------------------------------------

func TestConcurrent_ReadersAndWriter(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 200, MaxQueue: 5}, false)
	defer db.Close()

	const N = 300
	// Writer: pushes N keys.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < N; i++ {
			key := fmt.Sprintf("k-%04d", i)
			val := fmt.Sprintf("v-%04d", i)
			if err := db.Put(key, []byte(val)); err != nil {
				t.Errorf("Put %d: %v", i, err)
				return
			}
		}
	}()

	// Readers: repeatedly query random-looking keys, don't assert on found —
	// the writer may not have gotten there yet — but if found, value must match.
	const readers = 4
	var readerWG sync.WaitGroup
	var readCount atomic.Int64
	stop := make(chan struct{})
	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func(seed int) {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Rotate over keys deterministically.
				key := fmt.Sprintf("k-%04d", (int(readCount.Load())*7+seed*13)%N)
				v, found, err := db.Get(key)
				if err != nil {
					t.Errorf("Get(%s): %v", key, err)
					return
				}
				if found {
					want := fmt.Sprintf("v-%s", key[2:])
					if string(v) != want {
						t.Errorf("torn read: key=%s got %q want %q", key, v, want)
						return
					}
				}
				readCount.Add(1)
			}
		}(r)
	}

	<-writerDone
	close(stop)
	readerWG.Wait()

	// Final: every key must be visible to a Get now.
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%04d", i)
		want := fmt.Sprintf("v-%04d", i)
		v, found, _ := db.Get(key)
		if !found {
			t.Errorf("post-run key %s missing", key)
		} else if string(v) != want {
			t.Errorf("post-run key %s: expected %s, got %s", key, want, v)
		}
	}
}

// ----------------------------------------------------------------------------
// Backpressure — MaxQueue=1 forces Puts to block when flush lags
// ----------------------------------------------------------------------------

func TestBackpressure_QueueFullBlocksPut(t *testing.T) {
	// MaxQueue=1 + small threshold → after 2 rapid rollovers, the third Put's
	// rollover attempts to push into a full queue → blocks until flush drains.
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 60, MaxQueue: 1}, false)
	defer db.Close()

	// Force many rollovers in a tight loop. The test succeeds if it completes
	// (i.e., backpressure eventually unblocks) and all data is queryable.
	const N = 40
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%03d", i)
		val := fmt.Sprintf("val-%03d", i)
		if err := db.Put(key, []byte(val)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Wait for the last flush to catch up.
	waitUntil(t, 10*time.Second, "queue drained", func() bool {
		return len(db.queue.snapshot()) == 0
	})

	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%03d", i)
		want := fmt.Sprintf("val-%03d", i)
		v, found, _ := db.Get(key)
		if !found {
			t.Errorf("post-backpressure key %s missing", key)
		} else if string(v) != want {
			t.Errorf("post-backpressure key %s: expected %s, got %s", key, want, v)
		}
	}
}

// ----------------------------------------------------------------------------
// End-to-end: many keys, many flushes, close + reopen, everything queryable
// ----------------------------------------------------------------------------

func TestEndToEnd_ManyKeysManyFlushesRoundTrip(t *testing.T) {
	dir := t.TempDir()

	const N = 500
	db1, _ := Open(dir, testSeed, Options{FlushThreshold: 500, MaxQueue: 5}, false)
	for i := 0; i < N; i++ {
		if err := db1.Put(fmt.Sprintf("k-%04d", i), []byte(fmt.Sprintf("v-%04d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Overwrite half of them.
	for i := 0; i < N/2; i++ {
		db1.Put(fmt.Sprintf("k-%04d", i), []byte(fmt.Sprintf("v2-%04d", i)))
	}
	// Delete the last quarter.
	for i := N * 3 / 4; i < N; i++ {
		db1.Delete(fmt.Sprintf("k-%04d", i))
	}
	// Wait for background flushes to catch up meaningfully.
	waitUntil(t, 10*time.Second, "some SSTables produced", func() bool {
		return countSSTables(t, dir) >= 1
	})
	db1.Close()

	// Reopen — everything through the LSM's cross-layer read path.
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 500, MaxQueue: 5}, false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	// First half: overwrites should win.
	for i := 0; i < N/2; i++ {
		key := fmt.Sprintf("k-%04d", i)
		want := fmt.Sprintf("v2-%04d", i)
		v, found, _ := db2.Get(key)
		if !found {
			t.Errorf("overwrite key %s missing", key)
		} else if string(v) != want {
			t.Errorf("overwrite key %s: expected %s, got %s", key, want, v)
		}
	}
	// Middle quarter: original values.
	for i := N / 2; i < N*3/4; i++ {
		key := fmt.Sprintf("k-%04d", i)
		want := fmt.Sprintf("v-%04d", i)
		v, found, _ := db2.Get(key)
		if !found {
			t.Errorf("original key %s missing", key)
		} else if string(v) != want {
			t.Errorf("original key %s: expected %s, got %s", key, want, v)
		}
	}
	// Last quarter: deleted.
	for i := N * 3 / 4; i < N; i++ {
		key := fmt.Sprintf("k-%04d", i)
		if _, found, _ := db2.Get(key); found {
			t.Errorf("deleted key %s should not be findable", key)
		}
	}
}
