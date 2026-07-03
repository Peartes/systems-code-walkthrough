package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/peartes/lsm/src/bloom"
)

const magicExpected uint32 = 0x53535442 // "SSTB"

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "table.sst")
}

// parsedTrailer holds the fields extracted from a written SSTable file.
type parsedTrailer struct {
	bloomBytes  []byte
	minKey      string
	maxKey      string
	recordCount uint64
	magic       uint32
}

// readTrailer opens the file, seeks to the last 8 bytes to find the trailer
// size and magic, then parses the full trailer.
func readTrailer(t *testing.T, path string) parsedTrailer {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) < 8 {
		t.Fatalf("file too short to hold a trailer footer: %d bytes", len(data))
	}
	// Last 8 bytes: [trailer_size:4][magic:4]
	trailerSize := binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4])
	magic := binary.LittleEndian.Uint32(data[len(data)-4:])
	if int(trailerSize)+8 > len(data) {
		t.Fatalf("trailer_size %d + 8 exceeds file size %d", trailerSize, len(data))
	}
	// Trailer body starts at len(data) - 8 - trailerSize.
	start := len(data) - 8 - int(trailerSize)
	trailer := data[start : len(data)-8]

	// Parse forward:
	off := 0
	bloomLen := binary.LittleEndian.Uint32(trailer[off:])
	off += 4
	bloomBytes := trailer[off : off+int(bloomLen)]
	off += int(bloomLen)
	minLen := binary.LittleEndian.Uint32(trailer[off:])
	off += 4
	minKey := string(trailer[off : off+int(minLen)])
	off += int(minLen)
	maxLen := binary.LittleEndian.Uint32(trailer[off:])
	off += 4
	maxKey := string(trailer[off : off+int(maxLen)])
	off += int(maxLen)
	recordCount := binary.LittleEndian.Uint64(trailer[off:])

	return parsedTrailer{
		bloomBytes:  bloomBytes,
		minKey:      minKey,
		maxKey:      maxKey,
		recordCount: recordCount,
		magic:       magic,
	}
}

// ----------------------------------------------------------------------------
// NewWriter
// ----------------------------------------------------------------------------

func TestNewWriter_CreatesFile(t *testing.T) {
	path := tempPath(t)
	w, err := NewWriter(path, 100, 0.01)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Finish()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected sstable file at %s: %v", path, err)
	}
}

func TestNewWriter_TruncatesExistingFile(t *testing.T) {
	path := tempPath(t)
	// Pre-populate the file with garbage.
	if err := os.WriteFile(path, []byte("stale bytes that should not survive"), 0644); err != nil {
		t.Fatalf("preload: %v", err)
	}

	w, _ := NewWriter(path, 10, 0.01)
	w.Add("only-key", []byte("v"), false)
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	trailer := readTrailer(t, path)
	if trailer.recordCount != 1 {
		t.Fatalf("expected 1 record after truncate; got %d (stale bytes survived?)", trailer.recordCount)
	}
	if trailer.minKey != "only-key" || trailer.maxKey != "only-key" {
		t.Fatalf("expected min/max=only-key, got min=%q max=%q", trailer.minKey, trailer.maxKey)
	}
}

// ----------------------------------------------------------------------------
// Add / sort order
// ----------------------------------------------------------------------------

func TestAdd_SortedKeysSucceed(t *testing.T) {
	w, _ := NewWriter(tempPath(t), 100, 0.01)
	defer w.Finish()

	for _, k := range []string{"alpha", "bravo", "charlie", "delta"} {
		if err := w.Add(k, []byte(k), false); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}
}

func TestAdd_OutOfOrderPanics(t *testing.T) {
	w, _ := NewWriter(tempPath(t), 100, 0.01)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on out-of-order Add")
		}
	}()

	w.Add("bravo", []byte("v"), false)
	w.Add("alpha", []byte("v"), false) // out of order → panic
}

func TestAdd_DuplicateKeyPanics(t *testing.T) {
	w, _ := NewWriter(tempPath(t), 100, 0.01)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on duplicate key (keys must be strictly increasing)")
		}
	}()

	w.Add("foo", []byte("v1"), false)
	w.Add("foo", []byte("v2"), false) // duplicate → panic
}

func TestAdd_FirstKeyIsSetAsMin(t *testing.T) {
	path := tempPath(t)
	w, _ := NewWriter(path, 100, 0.01)
	w.Add("apple", []byte("a"), false)
	w.Add("banana", []byte("b"), false)
	w.Add("cherry", []byte("c"), false)
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	trailer := readTrailer(t, path)
	if trailer.minKey != "apple" {
		t.Fatalf("expected minKey=apple, got %q", trailer.minKey)
	}
	if trailer.maxKey != "cherry" {
		t.Fatalf("expected maxKey=cherry, got %q", trailer.maxKey)
	}
}

// ----------------------------------------------------------------------------
// Finish semantics
// ----------------------------------------------------------------------------

func TestFinish_WritesTrailerToDisk(t *testing.T) {
	path := tempPath(t)
	w, _ := NewWriter(path, 10, 0.01)
	w.Add("k", []byte("v"), false)
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	trailer := readTrailer(t, path)
	if trailer.magic != magicExpected {
		t.Fatalf("expected magic 0x%08X, got 0x%08X", magicExpected, trailer.magic)
	}
	if trailer.recordCount != 1 {
		t.Fatalf("expected recordCount=1, got %d", trailer.recordCount)
	}
	if trailer.minKey != "k" || trailer.maxKey != "k" {
		t.Fatalf("expected minKey=maxKey=k; got min=%q max=%q", trailer.minKey, trailer.maxKey)
	}
	if len(trailer.bloomBytes) == 0 {
		t.Fatalf("expected non-empty bloom filter bytes")
	}
}

func TestFinish_Idempotent(t *testing.T) {
	w, _ := NewWriter(tempPath(t), 10, 0.01)
	w.Add("k", []byte("v"), false)
	if err := w.Finish(); err != nil {
		t.Fatalf("first Finish: %v", err)
	}
	// Second Finish should be a no-op — no error, no double trailer written.
	if err := w.Finish(); err != nil {
		t.Fatalf("second Finish should be no-op, got: %v", err)
	}
}

func TestFinish_AddAfterFinishRejected(t *testing.T) {
	w, _ := NewWriter(tempPath(t), 10, 0.01)
	w.Add("k", []byte("v"), false)
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	err := w.Add("later", []byte("v"), false)
	if err == nil {
		t.Fatalf("expected error on Add after Finish")
	}
	if !errors.Is(err, ErrAddingToFinishedSSTable) {
		t.Fatalf("expected ErrAddingToFinishedSSTable, got %v", err)
	}
}

// ----------------------------------------------------------------------------
// The critical bug we just fixed: bloom filter must actually be populated.
// ----------------------------------------------------------------------------

func TestBloom_ContainsAllAddedKeys(t *testing.T) {
	path := tempPath(t)
	w, _ := NewWriter(path, 100, 0.01)

	keys := []string{"apple", "banana", "cherry", "date", "elder", "fig", "grape", "honeydew"}
	for _, k := range keys {
		w.Add(k, []byte(k), false)
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Read the bloom bytes back from the trailer and deserialize.
	trailer := readTrailer(t, path)
	b, err := bloom.Deserialize(trailer.bloomBytes)
	if err != nil {
		t.Fatalf("Deserialize bloom: %v", err)
	}
	// Every key added must MayContain=true. Any false = the bug is back.
	for _, k := range keys {
		ok, err := b.MayContain(k)
		if err != nil {
			t.Fatalf("MayContain(%q): %v", k, err)
		}
		if !ok {
			t.Fatalf("FALSE NEGATIVE: bloom does not contain added key %q", k)
		}
	}
}

// ----------------------------------------------------------------------------
// Tombstones
// ----------------------------------------------------------------------------

func TestAdd_TombstoneRecordsCounted(t *testing.T) {
	path := tempPath(t)
	w, _ := NewWriter(path, 10, 0.01)
	w.Add("a", []byte("live"), false)
	w.Add("b", nil, true) // tombstone
	w.Add("c", []byte("live"), false)
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	trailer := readTrailer(t, path)
	if trailer.recordCount != 3 {
		t.Fatalf("expected 3 records (incl. tombstone), got %d", trailer.recordCount)
	}
}

func TestBloom_ContainsTombstoneKey(t *testing.T) {
	path := tempPath(t)
	w, _ := NewWriter(path, 10, 0.01)
	w.Add("dead-key", nil, true)
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	trailer := readTrailer(t, path)
	b, _ := bloom.Deserialize(trailer.bloomBytes)
	ok, _ := b.MayContain("dead-key")
	if !ok {
		t.Fatalf("bloom must contain tombstone keys — reader needs to know 'this SSTable claims to have this key (as deleted)'")
	}
}

// ----------------------------------------------------------------------------
// Trailer size correctness
// ----------------------------------------------------------------------------

func TestFinish_TrailerSizeMatchesTrailerBytes(t *testing.T) {
	path := tempPath(t)
	w, _ := NewWriter(path, 10, 0.01)
	for i := 0; i < 20; i++ {
		w.Add(fmt.Sprintf("k-%02d", i), []byte(fmt.Sprintf("v-%02d", i)), false)
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	data, _ := os.ReadFile(path)
	trailerSize := binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4])
	magic := binary.LittleEndian.Uint32(data[len(data)-4:])

	if magic != magicExpected {
		t.Fatalf("magic mismatch: expected 0x%08X, got 0x%08X", magicExpected, magic)
	}
	// The trailer body should be exactly trailerSize bytes.
	// If trailerSize is wrong, our forward parse would go past valid data or short of it.
	if int(trailerSize)+8 > len(data) {
		t.Fatalf("trailer_size %d + 8 exceeds file size %d", trailerSize, len(data))
	}
}

// ----------------------------------------------------------------------------
// Many records — stress round-trip
// ----------------------------------------------------------------------------

func TestStress_ManyRecords(t *testing.T) {
	path := tempPath(t)
	const N = 5000
	w, _ := NewWriter(path, N, 0.01)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%06d", i)
		val := []byte(fmt.Sprintf("v-%06d", i))
		if err := w.Add(key, val, false); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	trailer := readTrailer(t, path)
	if trailer.recordCount != N {
		t.Fatalf("expected recordCount=%d, got %d", N, trailer.recordCount)
	}
	if trailer.minKey != "k-000000" {
		t.Fatalf("expected minKey=k-000000, got %q", trailer.minKey)
	}
	if trailer.maxKey != fmt.Sprintf("k-%06d", N-1) {
		t.Fatalf("expected maxKey=k-%06d, got %q", N-1, trailer.maxKey)
	}

	// Bloom filter contains everything.
	b, _ := bloom.Deserialize(trailer.bloomBytes)
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k-%06d", i)
		ok, _ := b.MayContain(key)
		if !ok {
			t.Fatalf("FALSE NEGATIVE for %s", key)
		}
	}
}
