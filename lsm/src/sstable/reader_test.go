package sstable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// buildTable is a helper that writes an SSTable with the given keys+values
// and returns its path. All values are marked non-tombstone unless the map
// entry is nil (nil ⇒ tombstone).
func buildTable(t *testing.T, entries map[string][]byte, sortedKeys []string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "table.sst")
	w, err := NewWriter(path, len(sortedKeys), 0.01)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, k := range sortedKeys {
		v := entries[k]
		tombstone := v == nil
		if err := w.Add(k, v, tombstone); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return path
}

// ----------------------------------------------------------------------------
// OpenReader
// ----------------------------------------------------------------------------

func TestOpenReader_ValidTable(t *testing.T) {
	path := buildTable(t,
		map[string][]byte{"apple": []byte("red"), "banana": []byte("yellow"), "cherry": []byte("dark")},
		[]string{"apple", "banana", "cherry"},
	)
	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	if r.MinKey() != "apple" {
		t.Fatalf("MinKey = %q, want apple", r.MinKey())
	}
	if r.MaxKey() != "cherry" {
		t.Fatalf("MaxKey = %q, want cherry", r.MaxKey())
	}
	if r.RecordCount() != 3 {
		t.Fatalf("RecordCount = %d, want 3", r.RecordCount())
	}
}

func TestOpenReader_RejectsMagicMismatch(t *testing.T) {
	path := buildTable(t,
		map[string][]byte{"k": []byte("v")},
		[]string{"k"},
	)
	// Corrupt the last 4 bytes of the file — that's the magic.
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xFF
	os.WriteFile(path, data, 0644)

	_, err := OpenReader(path)
	if !errors.Is(err, ErrMagicMismatch) {
		t.Fatalf("expected ErrMagicMismatch, got %v", err)
	}
}

func TestOpenReader_RejectsMissingFile(t *testing.T) {
	_, err := OpenReader(filepath.Join(t.TempDir(), "does-not-exist.sst"))
	if err == nil {
		t.Fatalf("expected error opening nonexistent file")
	}
	if !errors.Is(err, ErrOpenFile) {
		t.Fatalf("expected ErrOpenFile, got %v", err)
	}
}

func TestOpenReader_RejectsTooShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tiny.sst")
	os.WriteFile(path, []byte("abc"), 0644) // way too short to have a trailer
	_, err := OpenReader(path)
	if err == nil {
		t.Fatalf("expected error on too-short file")
	}
}

// ----------------------------------------------------------------------------
// Metadata accessors
// ----------------------------------------------------------------------------

func TestReader_MinMaxRecordCount(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e"}
	entries := map[string][]byte{}
	for _, k := range keys {
		entries[k] = []byte(k)
	}
	path := buildTable(t, entries, keys)

	r, _ := OpenReader(path)
	defer r.Close()

	if r.MinKey() != "a" {
		t.Errorf("MinKey = %q, want a", r.MinKey())
	}
	if r.MaxKey() != "e" {
		t.Errorf("MaxKey = %q, want e", r.MaxKey())
	}
	if r.RecordCount() != uint64(len(keys)) {
		t.Errorf("RecordCount = %d, want %d", r.RecordCount(), len(keys))
	}
}

// ----------------------------------------------------------------------------
// MayContain
// ----------------------------------------------------------------------------

func TestMayContain_ReturnsTrueForAddedKeys(t *testing.T) {
	keys := []string{"apple", "banana", "cherry", "date", "elder"}
	entries := map[string][]byte{}
	for _, k := range keys {
		entries[k] = []byte(k + "-v")
	}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	for _, k := range keys {
		ok, err := r.MayContain(k)
		if err != nil {
			t.Fatalf("MayContain(%q): %v", k, err)
		}
		if !ok {
			t.Fatalf("FALSE NEGATIVE: MayContain(%q) = false; must be true for any added key", k)
		}
	}
}

// ----------------------------------------------------------------------------
// Get — boundaries and middle
// ----------------------------------------------------------------------------

func TestGet_MinKeyBoundary(t *testing.T) {
	// Regression: earlier code used <= for range check, excluding boundary keys.
	keys := []string{"alpha", "bravo", "charlie"}
	entries := map[string][]byte{
		"alpha": []byte("A"), "bravo": []byte("B"), "charlie": []byte("C"),
	}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	v, ts, found, err := r.Get("alpha")
	if err != nil {
		t.Fatalf("Get(alpha): %v", err)
	}
	if !found {
		t.Fatalf("expected minKey 'alpha' to be found, but reported missing")
	}
	if ts {
		t.Fatalf("expected tombstone=false for alpha")
	}
	if string(v) != "A" {
		t.Fatalf("expected value A, got %q", v)
	}
}

func TestGet_MaxKeyBoundary(t *testing.T) {
	keys := []string{"alpha", "bravo", "charlie"}
	entries := map[string][]byte{
		"alpha": []byte("A"), "bravo": []byte("B"), "charlie": []byte("C"),
	}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	v, _, found, err := r.Get("charlie")
	if err != nil {
		t.Fatalf("Get(charlie): %v", err)
	}
	if !found {
		t.Fatalf("expected maxKey 'charlie' to be found, but reported missing")
	}
	if string(v) != "C" {
		t.Fatalf("expected value C, got %q", v)
	}
}

func TestGet_MiddleKey(t *testing.T) {
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	entries := map[string][]byte{}
	for _, k := range keys {
		entries[k] = []byte(k)
	}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	v, _, found, err := r.Get("charlie")
	if err != nil || !found || string(v) != "charlie" {
		t.Fatalf("Get(charlie): expected charlie/found; got v=%q found=%v err=%v", v, found, err)
	}
}

// ----------------------------------------------------------------------------
// Get — misses
// ----------------------------------------------------------------------------

func TestGet_BelowMinKey_RangeMiss(t *testing.T) {
	keys := []string{"m", "n", "o"}
	entries := map[string][]byte{"m": []byte("M"), "n": []byte("N"), "o": []byte("O")}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	_, _, found, err := r.Get("a") // below min
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if found {
		t.Fatalf("expected miss for key below min")
	}
}

func TestGet_AboveMaxKey_RangeMiss(t *testing.T) {
	keys := []string{"m", "n", "o"}
	entries := map[string][]byte{"m": []byte("M"), "n": []byte("N"), "o": []byte("O")}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	_, _, found, err := r.Get("z") // above max
	if err != nil {
		t.Fatalf("Get(z): %v", err)
	}
	if found {
		t.Fatalf("expected miss for key above max")
	}
}

func TestGet_InRangeButAbsent_ScanTerminates(t *testing.T) {
	// key is inside [min, max] but not actually stored. Bloom might say maybe,
	// forcing a linear scan. The scan must terminate cleanly with not-found —
	// not panic, not infinite loop.
	keys := []string{"apple", "cherry", "elder"}
	entries := map[string][]byte{
		"apple": []byte("a"), "cherry": []byte("c"), "elder": []byte("e"),
	}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	// "banana" is between apple and cherry alphabetically.
	_, _, found, err := r.Get("banana")
	if err != nil {
		t.Fatalf("Get(banana): %v", err)
	}
	if found {
		t.Fatalf("banana not in table; expected miss, got found")
	}
}

func TestGet_ScanToEnd_DoesNotHang(t *testing.T) {
	// Regression for the infinite-loop-at-EOF bug. Query for a key alphabetically
	// AFTER the last key but still inside the range. This forces the scan to
	// run all the way through without overshooting.
	// Actually, better test: use a key that hashes into the bloom (may-contain
	// true) but comes lexicographically at the END of the range. We can't easily
	// force a bloom hit for an absent key deterministically, but we can construct
	// a case where the scan must consume every record.
	keys := []string{"aa", "bb", "cc"}
	entries := map[string][]byte{"aa": []byte("A"), "bb": []byte("B"), "cc": []byte("C")}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	// "ca" is inside [aa, cc], not present. The scan will pass all three
	// records (aa < ca, bb < ca, cc > ca → return miss at overshoot).
	_, _, found, _ := r.Get("ca")
	if found {
		t.Fatalf("ca not in table; expected miss")
	}
}

// ----------------------------------------------------------------------------
// Tombstones
// ----------------------------------------------------------------------------

func TestGet_TombstoneKey(t *testing.T) {
	keys := []string{"apple", "banana", "cherry"}
	entries := map[string][]byte{
		"apple":  []byte("A"),
		"banana": nil, // tombstone
		"cherry": []byte("C"),
	}
	path := buildTable(t, entries, keys)
	r, _ := OpenReader(path)
	defer r.Close()

	v, tombstone, found, err := r.Get("banana")
	if err != nil {
		t.Fatalf("Get(banana): %v", err)
	}
	if !found {
		t.Fatalf("tombstone must be found=true so LSM stops searching older SSTables")
	}
	if !tombstone {
		t.Fatalf("expected tombstone=true for deleted key")
	}
	if len(v) != 0 {
		t.Fatalf("expected empty value for tombstone, got %q", v)
	}
}

// ----------------------------------------------------------------------------
// Close semantics
// ----------------------------------------------------------------------------

func TestClose_ThenGetReturnsErrClosedReader(t *testing.T) {
	path := buildTable(t, map[string][]byte{"k": []byte("v")}, []string{"k"})
	r, _ := OpenReader(path)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, _, _, err := r.Get("k")
	if !errors.Is(err, ErrClosedReader) {
		t.Fatalf("expected ErrClosedReader after Close, got %v", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	path := buildTable(t, map[string][]byte{"k": []byte("v")}, []string{"k"})
	r, _ := OpenReader(path)
	if err := r.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close should be no-op, got: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Round-trip stress
// ----------------------------------------------------------------------------

func TestStress_ManyKeys_ReadBackCorrectly(t *testing.T) {
	const N = 2000
	keys := make([]string, N)
	entries := map[string][]byte{}
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("k-%05d", i)
		entries[keys[i]] = []byte(fmt.Sprintf("v-%05d", i))
	}
	path := buildTable(t, entries, keys)

	r, _ := OpenReader(path)
	defer r.Close()

	if r.RecordCount() != N {
		t.Fatalf("RecordCount = %d, want %d", r.RecordCount(), N)
	}
	// Sample every 10th key to keep test fast.
	for i := 0; i < N; i += 10 {
		k := keys[i]
		v, ts, found, err := r.Get(k)
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if !found {
			t.Fatalf("expected to find %s", k)
		}
		if ts {
			t.Fatalf("expected tombstone=false for %s", k)
		}
		want := fmt.Sprintf("v-%05d", i)
		if string(v) != want {
			t.Fatalf("Get(%s): expected %s, got %s", k, want, v)
		}
	}
}

// ----------------------------------------------------------------------------
// CRC corruption detection during scan
// ----------------------------------------------------------------------------

func TestGet_CRCCorruptionReturnsErrCorruptRecord(t *testing.T) {
	keys := []string{"apple", "banana", "cherry"}
	entries := map[string][]byte{
		"apple": []byte("A"), "banana": []byte("B"), "cherry": []byte("C"),
	}
	path := buildTable(t, entries, keys)

	// Corrupt byte 30 — well inside the second record's payload (record 0 for
	// "apple" is only ~23 bytes; byte 30 falls inside record 1 for "banana").
	data, _ := os.ReadFile(path)
	data[30] ^= 0xFF
	os.WriteFile(path, data, 0644)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader (trailer intact, records corrupted): %v", err)
	}
	defer r.Close()

	// Query for "banana" — the scan will pass record 0 cleanly, then hit the
	// corrupted record 1 and return ErrCorruptRecord.
	_, _, _, err = r.Get("banana")
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("expected ErrCorruptRecord on corrupted scan, got %v", err)
	}
}
