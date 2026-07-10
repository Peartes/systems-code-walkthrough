package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/peartes/lsm/src/sstable"
	wl "github.com/peartes/lsm/src/wal"
)

// buildWAL writes a WAL segment with the given ops at seqno N (filename wal-N.log).
type op struct {
	key       string
	value     []byte
	tombstone bool
}

func buildWAL(t *testing.T, dir string, seqno uint64, ops []op) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("wal-%d.log", seqno))
	w, err := wl.Open(path)
	if err != nil {
		t.Fatalf("wal.Open(%s): %v", path, err)
	}
	for _, o := range ops {
		if err := w.Append(o.key, o.value, o.tombstone); err != nil {
			t.Fatalf("wal.Append(%q): %v", o.key, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}
}

// buildSSTable creates an SSTable file at seqno.sst with sorted-in-order entries.
func buildSSTable(t *testing.T, dir string, seqno uint64, sortedKeys []string, entries map[string][]byte) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("%d.sst", seqno))
	w, err := sstable.NewWriter(path, len(sortedKeys), 0.01)
	if err != nil {
		t.Fatalf("sstable.NewWriter(%s): %v", path, err)
	}
	for _, k := range sortedKeys {
		v := entries[k]
		tombstone := v == nil
		if err := w.Add(k, v, tombstone); err != nil {
			t.Fatalf("sstable.Add(%q): %v", k, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("sstable.Finish: %v", err)
	}
}

func saveTestManifest(t *testing.T, dir string, nextSeqNo uint64, live []uint64) {
	t.Helper()
	m := &Manifest{dir: dir, NextSeqNo: nextSeqNo, LiveSSTables: live}
	if err := m.Save(); err != nil {
		t.Fatalf("manifest.Save: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Multi-WAL replay
// ----------------------------------------------------------------------------

func TestOpen_ReplaysExistingWALSegments(t *testing.T) {
	dir := t.TempDir()

	// Pre-populate two WAL segments as if we crashed with unflushed data.
	buildWAL(t, dir, 1, []op{
		{key: "alpha", value: []byte("A1"), tombstone: false},
		{key: "bravo", value: []byte("B1"), tombstone: false},
	})
	buildWAL(t, dir, 2, []op{
		{key: "alpha", value: []byte("A2"), tombstone: false}, // overwrite of alpha
		{key: "charlie", value: []byte("C2"), tombstone: false},
	})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 5}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// alpha should reflect the newer WAL's value (seqno=2 is newer than seqno=1).
	v, found, _ := db.Get("alpha")
	if !found || string(v) != "A2" {
		t.Errorf("alpha: expected v=A2 (newer WAL wins), got found=%v v=%q", found, v)
	}

	// bravo only exists in the older WAL.
	v, found, _ = db.Get("bravo")
	if !found || string(v) != "B1" {
		t.Errorf("bravo: expected v=B1 (from older WAL), got found=%v v=%q", found, v)
	}

	// charlie only exists in the newer WAL.
	v, found, _ = db.Get("charlie")
	if !found || string(v) != "C2" {
		t.Errorf("charlie: expected v=C2 (from newer WAL), got found=%v v=%q", found, v)
	}
}

func TestOpen_ReplayedWALTombstoneShadowsOlderValue(t *testing.T) {
	dir := t.TempDir()

	// Older WAL has a value; newer WAL tombstones it.
	buildWAL(t, dir, 1, []op{
		{key: "victim", value: []byte("alive"), tombstone: false},
	})
	buildWAL(t, dir, 2, []op{
		{key: "victim", value: nil, tombstone: true},
	})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 5}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, found, _ := db.Get("victim")
	if found {
		t.Errorf("expected victim to be tombstoned by the newer WAL; got found=true")
	}
}

// ----------------------------------------------------------------------------
// SSTable loading + manifest
// ----------------------------------------------------------------------------

func TestOpen_LoadsSSTablesFromManifest(t *testing.T) {
	dir := t.TempDir()

	buildSSTable(t, dir, 3, []string{"apple", "banana", "cherry"}, map[string][]byte{
		"apple":  []byte("A"),
		"banana": []byte("B"),
		"cherry": []byte("C"),
	})
	saveTestManifest(t, dir, 4, []uint64{3})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// All three keys should be readable from the pre-existing SSTable.
	for _, k := range []string{"apple", "banana", "cherry"} {
		v, found, _ := db.Get(k)
		if !found {
			t.Errorf("expected to find %s in SSTable-3", k)
		}
		if string(v) != string([]byte{k[0] - 32}) { // first char uppercased
			t.Errorf("%s: expected %s, got %s", k, string([]byte{k[0] - 32}), v)
		}
	}
}

func TestOpen_MultipleSSTables_NewestWins(t *testing.T) {
	// Two SSTables with the same key; newer seqno wins.
	dir := t.TempDir()

	buildSSTable(t, dir, 1, []string{"conflict"}, map[string][]byte{
		"conflict": []byte("OLD"),
	})
	buildSSTable(t, dir, 5, []string{"conflict"}, map[string][]byte{
		"conflict": []byte("NEW"),
	})
	saveTestManifest(t, dir, 6, []uint64{1, 5})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	v, found, _ := db.Get("conflict")
	if !found {
		t.Fatalf("expected to find conflict")
	}
	if string(v) != "NEW" {
		t.Errorf("expected NEW (from newer SSTable seqno=5), got %q", v)
	}
}

func TestOpen_SSTableTombstoneShadowsOlderSSTable(t *testing.T) {
	dir := t.TempDir()

	buildSSTable(t, dir, 1, []string{"victim"}, map[string][]byte{
		"victim": []byte("alive"),
	})
	buildSSTable(t, dir, 5, []string{"victim"}, map[string][]byte{
		"victim": nil, // tombstone
	})
	saveTestManifest(t, dir, 6, []uint64{1, 5})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, found, _ := db.Get("victim")
	if found {
		t.Errorf("expected victim tombstoned by newer SSTable; got found=true")
	}
}

// ----------------------------------------------------------------------------
// Orphan cleanup
// ----------------------------------------------------------------------------

func TestOpen_DeletesOrphanSSTables(t *testing.T) {
	dir := t.TempDir()

	// Create three SSTables, but only two are listed as live in the manifest.
	buildSSTable(t, dir, 1, []string{"k1"}, map[string][]byte{"k1": []byte("live1")})
	buildSSTable(t, dir, 2, []string{"k2"}, map[string][]byte{"k2": []byte("orphan")}) // ← orphan
	buildSSTable(t, dir, 3, []string{"k3"}, map[string][]byte{"k3": []byte("live3")})
	saveTestManifest(t, dir, 4, []uint64{1, 3}) // 2 is missing

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Orphan should be deleted from disk.
	if _, err := os.Stat(filepath.Join(dir, "2.sst")); !os.IsNotExist(err) {
		t.Errorf("orphan 2.sst should have been deleted; stat err=%v", err)
	}

	// Data from live SSTables is readable; orphan data is gone.
	if _, found, _ := db.Get("k2"); found {
		t.Errorf("orphan key k2 should not be findable")
	}
	if v, found, _ := db.Get("k1"); !found || string(v) != "live1" {
		t.Errorf("expected k1=live1 from live SSTable, got found=%v v=%q", found, v)
	}
	if v, found, _ := db.Get("k3"); !found || string(v) != "live3" {
		t.Errorf("expected k3=live3 from live SSTable, got found=%v v=%q", found, v)
	}
}

// ----------------------------------------------------------------------------
// Cross-layer shadowing: active memtable > queued memtable > SSTable
// ----------------------------------------------------------------------------

func TestGet_ActiveMemtableShadowsQueuedMemtable(t *testing.T) {
	dir := t.TempDir()

	// Pre-populate WAL segment (will be replayed into a queued memtable).
	buildWAL(t, dir, 1, []op{
		{key: "shared", value: []byte("from-queued"), tombstone: false},
	})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write the same key into the active memtable — should shadow the queued value.
	if err := db.Put("shared", []byte("from-active")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	v, found, _ := db.Get("shared")
	if !found {
		t.Fatalf("expected found")
	}
	if string(v) != "from-active" {
		t.Errorf("active memtable should shadow queued memtable; expected from-active, got %q", v)
	}
}

func TestGet_QueuedMemtableShadowsSSTable(t *testing.T) {
	dir := t.TempDir()

	// SSTable holds an old value.
	buildSSTable(t, dir, 1, []string{"shared"}, map[string][]byte{
		"shared": []byte("from-sst"),
	})
	saveTestManifest(t, dir, 2, []uint64{1})

	// WAL segment has a newer value (would be replayed into the queue).
	buildWAL(t, dir, 3, []op{
		{key: "shared", value: []byte("from-queued"), tombstone: false},
	})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	v, found, _ := db.Get("shared")
	if !found {
		t.Fatalf("expected found")
	}
	if string(v) != "from-queued" {
		t.Errorf("queued memtable should shadow SSTable; expected from-queued, got %q", v)
	}
}

func TestGet_ActiveTombstoneShadowsAllLayers(t *testing.T) {
	dir := t.TempDir()

	// SSTable + WAL both contain "victim" as a live value.
	buildSSTable(t, dir, 1, []string{"victim"}, map[string][]byte{
		"victim": []byte("in-sst"),
	})
	saveTestManifest(t, dir, 2, []uint64{1})
	buildWAL(t, dir, 3, []op{
		{key: "victim", value: []byte("in-queued"), tombstone: false},
	})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Delete via the active memtable.
	if err := db.Delete("victim"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, _ := db.Get("victim")
	if found {
		t.Errorf("active tombstone should shadow queued + SSTable; got found=true")
	}
}

// ----------------------------------------------------------------------------
// SeqNo continuity — the new active WAL uses a fresh seqno
// ----------------------------------------------------------------------------

func TestOpen_NewActiveWALSeqnoIsFresh(t *testing.T) {
	dir := t.TempDir()

	buildWAL(t, dir, 5, []op{
		{key: "k", value: []byte("v"), tombstone: false},
	})

	db, err := Open(dir, testSeed, Options{FlushThreshold: 1 << 20, MaxQueue: 3}, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// The active WAL seqno must be > 5.
	if db.walSeqno <= 5 {
		t.Errorf("new active WAL seqno should exceed discovered max (5), got %d", db.walSeqno)
	}

	// The new active WAL file exists.
	newWAL := filepath.Join(dir, fmt.Sprintf("wal-%d.log", db.walSeqno))
	if _, err := os.Stat(newWAL); err != nil {
		t.Errorf("expected new active WAL at %s; stat err=%v", newWAL, err)
	}
}
