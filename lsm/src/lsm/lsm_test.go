package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	wl "github.com/peartes/lsm/src/wal"
)

const testSeed int64 = 42

func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return db, dir
}

// ----------------------------------------------------------------------------
// Open
// ----------------------------------------------------------------------------

func TestOpen_CreatesDir(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "deeply", "nested", "dir")

	db, err := Open(nested, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(nested); err != nil {
		t.Fatalf("expected dir to be created: %v", err)
	}
}

func TestOpen_EmptyDirSucceeds(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	// Should be able to do basic ops on a fresh DB.
	if err := db.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := db.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(v) != "v" {
		t.Fatalf("expected found=true v=v, got found=%v v=%q", found, v)
	}
}

// ----------------------------------------------------------------------------
// Put / Get / Delete basic
// ----------------------------------------------------------------------------

func TestPutGet_SingleKey(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	if err := db.Put("hello", []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := db.Get("hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatalf("expected found=true")
	}
	if string(v) != "world" {
		t.Fatalf("expected world, got %q", v)
	}
}

func TestGet_MissingKey_NoError(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	v, found, err := db.Get("nothing-here")
	if err != nil {
		t.Fatalf("Get on missing key should not error, got: %v", err)
	}
	if found {
		t.Fatalf("expected found=false")
	}
	if v != nil {
		t.Fatalf("expected nil value, got %q", v)
	}
}

func TestDelete_ThenGet_ReturnsNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	db.Put("foo", []byte("alive"))
	if err := db.Delete("foo"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	v, found, err := db.Get("foo")
	if err != nil {
		t.Fatalf("Get after Delete should not error: %v", err)
	}
	if found {
		t.Fatalf("deleted key should not appear as found; got value %q", v)
	}
}

func TestDelete_OnMissingKey(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	// Deleting a key that never existed should be a no-op, not an error.
	if err := db.Delete("never-existed"); err != nil {
		t.Fatalf("Delete on missing key should not error: %v", err)
	}
	_, found, _ := db.Get("never-existed")
	if found {
		t.Fatalf("expected found=false after deleting missing key")
	}
}

func TestPut_OverwritesExisting(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	db.Put("k", []byte("v1"))
	db.Put("k", []byte("v2"))
	db.Put("k", []byte("v3"))

	v, found, _ := db.Get("k")
	if !found {
		t.Fatalf("expected found")
	}
	if string(v) != "v3" {
		t.Fatalf("expected latest value v3, got %q", v)
	}
}

func TestDelete_ThenPut_Resurrects(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	db.Put("k", []byte("v1"))
	db.Delete("k")
	db.Put("k", []byte("v2"))

	v, found, _ := db.Get("k")
	if !found {
		t.Fatalf("expected resurrected key to be found")
	}
	if string(v) != "v2" {
		t.Fatalf("expected v2, got %q", v)
	}
}

func TestManyKeys_ReadBackCorrectly(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()

	const N = 500
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := []byte(fmt.Sprintf("value-%04d", i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%04d", i)
		wantVal := fmt.Sprintf("value-%04d", i)
		v, found, _ := db.Get(key)
		if !found {
			t.Fatalf("expected to find %s", key)
		}
		if string(v) != wantVal {
			t.Fatalf("for %s: expected %s, got %s", key, wantVal, v)
		}
	}
}

// ----------------------------------------------------------------------------
// Crash recovery — the killer test
// ----------------------------------------------------------------------------

func TestCrashRecovery_ReadbackAcrossClose(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write 100 keys, delete 20.
	db1, err := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	for i := 0; i < 100; i++ {
		db1.Put(fmt.Sprintf("k-%03d", i), []byte(fmt.Sprintf("v-%03d", i)))
	}
	// Delete keys 50..69.
	for i := 50; i < 70; i++ {
		db1.Delete(fmt.Sprintf("k-%03d", i))
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: reopen and verify all surviving + deleted state.
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	survivingKeys := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k-%03d", i)
		v, found, _ := db2.Get(key)
		if i >= 50 && i < 70 {
			// Deleted range — should not be found.
			if found {
				t.Fatalf("deleted key %s appeared after reopen with value %q", key, v)
			}
		} else {
			// Live key — should still be there.
			if !found {
				t.Fatalf("expected to find key %s after reopen", key)
			}
			wantVal := fmt.Sprintf("v-%03d", i)
			if string(v) != wantVal {
				t.Fatalf("for %s after reopen: expected %s, got %s", key, wantVal, v)
			}
			survivingKeys++
		}
	}
	if survivingKeys != 80 {
		t.Fatalf("expected 80 surviving keys, got %d", survivingKeys)
	}
}

func TestCrashRecovery_OverwriteHistoryReplayed(t *testing.T) {
	// Multiple Puts to the same key, then close + reopen. Final value should win.
	dir := t.TempDir()

	db1, _ := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	for i := 0; i < 10; i++ {
		db1.Put("k", []byte(fmt.Sprintf("version-%d", i)))
	}
	db1.Close()

	db2, _ := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	defer db2.Close()
	v, found, _ := db2.Get("k")
	if !found {
		t.Fatalf("expected found")
	}
	if string(v) != "version-9" {
		t.Fatalf("expected version-9 (final write), got %q", v)
	}
}

func TestCrashRecovery_DeletedKeyStaysDeletedAfterReopen(t *testing.T) {
	dir := t.TempDir()

	db1, _ := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	db1.Put("alive", []byte("yes"))
	db1.Put("dead", []byte("temp"))
	db1.Delete("dead")
	db1.Close()

	db2, _ := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	defer db2.Close()
	_, foundAlive, _ := db2.Get("alive")
	if !foundAlive {
		t.Fatalf("alive key should still be found after reopen")
	}
	_, foundDead, _ := db2.Get("dead")
	if foundDead {
		t.Fatalf("dead key should remain deleted after reopen")
	}
}

// TestCrashRecovery_NoClose simulates a hard crash by skipping the graceful
// Close — the WAL is fsynced after each Append, so all data should still be
// recoverable. This is the test that proves Put's durability guarantee.
func TestCrashRecovery_NoClose_StillDurable(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write data WITHOUT calling Close.
	db1, _ := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	for i := 0; i < 50; i++ {
		if err := db1.Put(fmt.Sprintf("k-%02d", i), []byte(fmt.Sprintf("v-%02d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// NB: deliberately not calling db1.Close() — simulating a process crash.
	// However, we still need to release the file handle so we can reopen.
	// In a real crash the OS would do this; in test we close the WAL handle
	// manually to avoid the "already open" condition on some platforms.
	db1.wal.Close()

	// Phase 2: open a fresh DB instance from the same directory and verify.
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("reopen after simulated crash: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k-%02d", i)
		wantVal := fmt.Sprintf("v-%02d", i)
		v, found, _ := db2.Get(key)
		if !found {
			t.Fatalf("expected key %s to survive simulated crash", key)
		}
		if string(v) != wantVal {
			t.Fatalf("for %s after crash: expected %s, got %s", key, wantVal, v)
		}
	}
}

// ----------------------------------------------------------------------------
// Corruption tolerance
// ----------------------------------------------------------------------------

func TestOpen_WithCorruptedWAL_ReturnsError(t *testing.T) {
	// Section C's LSM Open is strict: a corrupt WAL causes Open to return an
	// error rather than a partially-recovered DB. This matches the design
	// choice in lsm.go where truncated/CRC-failed WAL replays are surfaced
	// as errors so the caller decides how to proceed. (Compare with the WAL
	// package's own replay tests, which verify partial-recovery semantics.)
	dir := t.TempDir()

	// Phase 1: write some records, close cleanly.
	db1, _ := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	for i := 0; i < 5; i++ {
		db1.Put(fmt.Sprintf("k-%d", i), []byte(fmt.Sprintf("v-%d", i)))
	}
	db1.Close()

	// Find the WAL file (naming is now wal-<seqno>.log).
	matches, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("no WAL files found in %s: %v (matches=%v)", dir, err, matches)
	}
	walPath := matches[0]

	// Corrupt the tail of the WAL — flip the very last byte.
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	// Phase 2: reopen. Should fail with a recoverable-WAL error.
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if db2 != nil {
		defer db2.Close()
		t.Fatalf("expected nil DB on corrupt WAL, got a usable DB")
	}
	if err == nil {
		t.Fatalf("expected error on corrupt WAL, got nil")
	}
	if !errors.Is(err, wl.ErrWALCRCCheckFail) && !errors.Is(err, wl.ErrTruncatedFile) {
		t.Fatalf("expected WAL corruption error, got: %v", err)
	}
}

// The old permissive-recovery test body is retained but skipped below so
// the intention is preserved if the LSM design ever swings back to allowing
// partial recovery. Delete this once you're sure Section C's strict recovery
// is the final design.
func TestOpen_WithCorruptedWAL_Permissive_DISABLED(t *testing.T) {
	t.Skip("Section C uses strict WAL recovery; permissive path removed")
	dir := t.TempDir()
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	_ = err
	if db2 == nil {
		return
	}
	defer db2.Close()

	if err := db2.Put("new-after-recovery", []byte("works")); err != nil {
		t.Fatalf("Put on post-recovery DB: %v", err)
	}
	v2, found, _ := db2.Get("new-after-recovery")
	if !found || string(v2) != "works" {
		t.Fatalf("new write after recovery not retrievable; got found=%v v=%q", found, v2)
	}
}

// ----------------------------------------------------------------------------
// Close
// ----------------------------------------------------------------------------

func TestClose_ReleasesFile(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	db.Put("k", []byte("v"))
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Reopening should work and find the data.
	db2, err := Open(dir, testSeed, Options{FlushThreshold: 1<<20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("reopen after Close: %v", err)
	}
	defer db2.Close()
	v, found, _ := db2.Get("k")
	if !found || string(v) != "v" {
		t.Fatalf("expected k=v after Close+reopen, got found=%v v=%q", found, v)
	}
}
