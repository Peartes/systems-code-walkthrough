package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// record is a convenience type for collecting Replay output.
type record struct {
	key       string
	value     []byte
	tombstone bool
}

func collect(w *WAL) ([]record, error) {
	var out []record
	err := w.Replay(func(key string, value []byte, tombstone bool) {
		// copy value because the underlying slice is reused by the WAL.
		v := append([]byte(nil), value...)
		out = append(out, record{key: key, value: v, tombstone: tombstone})
	})
	return out, err
}

func tempWALPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "wal.log")
}

// ----------------------------------------------------------------------------
// Open / Close
// ----------------------------------------------------------------------------

func TestOpen_CreatesFileIfMissing(t *testing.T) {
	path := tempWALPath(t)
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected WAL file to exist at %s, got: %v", path, err)
	}
}

func TestOpen_ReopensExisting(t *testing.T) {
	path := tempWALPath(t)
	w1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := w1.Append("k", []byte("v"), false); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	got, err := collect(w2)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 || got[0].key != "k" || string(got[0].value) != "v" {
		t.Fatalf("expected 1 record (k=v), got %+v", got)
	}
}

func TestClose_Idempotent_Errors(t *testing.T) {
	w, err := Open(tempWALPath(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second Close should not crash — Go's os.File.Close returns an error on
	// double-close but should not panic. We just verify behavior is deterministic.
	_ = w.Close()
}

// ----------------------------------------------------------------------------
// Append / Replay round-trip
// ----------------------------------------------------------------------------

func TestAppendReplay_SingleRecord(t *testing.T) {
	w, _ := Open(tempWALPath(t))
	defer w.Close()
	if err := w.Append("hello", []byte("world"), false); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := collect(w)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d (%+v)", len(got), got)
	}
	if got[0].key != "hello" {
		t.Fatalf("expected key=hello, got %q", got[0].key)
	}
	if string(got[0].value) != "world" {
		t.Fatalf("expected value=world, got %q", got[0].value)
	}
	if got[0].tombstone {
		t.Fatalf("expected tombstone=false")
	}
}

func TestAppendReplay_MultipleRecordsInOrder(t *testing.T) {
	w, _ := Open(tempWALPath(t))
	defer w.Close()
	records := []record{
		{key: "a", value: []byte("1"), tombstone: false},
		{key: "bb", value: []byte("22"), tombstone: false},
		{key: "ccc", value: []byte("333"), tombstone: false},
	}
	for _, r := range records {
		if err := w.Append(r.key, r.value, r.tombstone); err != nil {
			t.Fatalf("Append %q: %v", r.key, err)
		}
	}
	got, err := collect(w)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(got))
	}
	for i, r := range records {
		if got[i].key != r.key || string(got[i].value) != string(r.value) || got[i].tombstone != r.tombstone {
			t.Fatalf("at index %d: expected %+v, got %+v", i, r, got[i])
		}
	}
}

func TestAppendReplay_Tombstone(t *testing.T) {
	w, _ := Open(tempWALPath(t))
	defer w.Close()
	if err := w.Append("dead", nil, true); err != nil {
		t.Fatalf("Append tombstone: %v", err)
	}
	got, err := collect(w)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if !got[0].tombstone {
		t.Fatalf("expected tombstone=true")
	}
	if got[0].key != "dead" {
		t.Fatalf("expected key=dead, got %q", got[0].key)
	}
	if len(got[0].value) != 0 {
		t.Fatalf("expected empty value for tombstone, got %q", got[0].value)
	}
}

func TestAppendReplay_TombstoneAndLiveMixed(t *testing.T) {
	w, _ := Open(tempWALPath(t))
	defer w.Close()
	w.Append("alpha", []byte("A"), false)
	w.Append("bravo", nil, true)
	w.Append("charlie", []byte("C"), false)
	w.Append("delta", nil, true)

	got, _ := collect(w)
	if len(got) != 4 {
		t.Fatalf("expected 4 records, got %d", len(got))
	}
	wantTombstones := []bool{false, true, false, true}
	for i, want := range wantTombstones {
		if got[i].tombstone != want {
			t.Fatalf("at index %d: expected tombstone=%v, got %v", i, want, got[i].tombstone)
		}
	}
}

// ----------------------------------------------------------------------------
// Durability across close + reopen
// ----------------------------------------------------------------------------

func TestDurability_AppendCloseReopenReplay(t *testing.T) {
	path := tempWALPath(t)

	// Phase 1: write 5 records, close.
	w1, _ := Open(path)
	for i, k := range []string{"k1", "k2", "k3", "k4", "k5"} {
		if err := w1.Append(k, []byte{byte('A' + i)}, false); err != nil {
			t.Fatalf("Append %q: %v", k, err)
		}
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: reopen, append more, close.
	w2, _ := Open(path)
	w2.Append("k6", []byte("F"), false)
	w2.Append("k7", []byte("G"), false)
	w2.Close()

	// Phase 3: reopen, replay, verify all 7.
	w3, _ := Open(path)
	defer w3.Close()
	got, err := collect(w3)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 7 {
		t.Fatalf("expected 7 records after two-phase write, got %d (%+v)", len(got), got)
	}
	for i, key := range []string{"k1", "k2", "k3", "k4", "k5", "k6", "k7"} {
		if got[i].key != key {
			t.Fatalf("at %d: expected key=%s, got %s", i, key, got[i].key)
		}
	}
}

// ----------------------------------------------------------------------------
// Empty file
// ----------------------------------------------------------------------------

func TestReplay_EmptyFileIsCleanNil(t *testing.T) {
	w, _ := Open(tempWALPath(t))
	defer w.Close()
	got, err := collect(w)
	if err != nil {
		t.Fatalf("expected nil error on empty file, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 records on empty file, got %d", len(got))
	}
}

// ----------------------------------------------------------------------------
// CRC corruption detection
// ----------------------------------------------------------------------------

func TestReplay_CRCCorruption(t *testing.T) {
	path := tempWALPath(t)
	w, _ := Open(path)
	if err := w.Append("k", []byte("untouched"), false); err != nil {
		t.Fatalf("Append: %v", err)
	}
	w.Close()

	// Flip a byte in the payload region (somewhere past the length+CRC header).
	// Layout: [len:4][crc:4][tombstone:1][keylen:4][key][vallen:4][value]
	// Flip a byte deep in the value to make sure it's payload not header.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) < 20 {
		t.Fatalf("file too short to corrupt: %d bytes", len(data))
	}
	data[len(data)-1] ^= 0xFF // flip last byte (deep in value)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write back: %v", err)
	}

	w2, _ := Open(path)
	defer w2.Close()
	_, err = collect(w2)
	if !errors.Is(err, ErrWALCRCCheckFail) {
		t.Fatalf("expected ErrWALCRCCheckFail, got %v", err)
	}
}

// ----------------------------------------------------------------------------
// Torn write detection (file truncated mid-record)
// ----------------------------------------------------------------------------

func TestReplay_TornLengthPrefix(t *testing.T) {
	path := tempWALPath(t)
	w, _ := Open(path)
	w.Append("k", []byte("v"), false)
	w.Close()

	// Truncate to 2 bytes — partial length prefix.
	if err := os.Truncate(path, 2); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	w2, _ := Open(path)
	defer w2.Close()
	_, err := collect(w2)
	if !errors.Is(err, ErrTruncatedFile) {
		t.Fatalf("expected ErrTruncatedFile on partial length prefix, got %v", err)
	}
}

func TestReplay_TornMidPayload(t *testing.T) {
	path := tempWALPath(t)
	w, _ := Open(path)
	w.Append("k", []byte("a-fairly-long-value-here"), false)
	w.Close()

	// Truncate the file so the length prefix is complete but the payload is cut short.
	stat, _ := os.Stat(path)
	if stat.Size() < 10 {
		t.Fatalf("file too short to truncate: %d", stat.Size())
	}
	if err := os.Truncate(path, stat.Size()-5); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	w2, _ := Open(path)
	defer w2.Close()
	_, err := collect(w2)
	if !errors.Is(err, ErrTruncatedFile) {
		t.Fatalf("expected ErrTruncatedFile on torn payload, got %v", err)
	}
}

func TestReplay_PartiallyValidThenTorn(t *testing.T) {
	// One full valid record, then a torn second record. The valid record
	// should still be delivered to the callback before the truncation error.
	path := tempWALPath(t)
	w, _ := Open(path)
	w.Append("first", []byte("complete"), false)
	w.Append("second", []byte("also-complete"), false)
	w.Close()

	// Lop off the last 3 bytes to make the second record torn.
	stat, _ := os.Stat(path)
	os.Truncate(path, stat.Size()-3)

	w2, _ := Open(path)
	defer w2.Close()

	var got []record
	err := w2.Replay(func(key string, value []byte, tombstone bool) {
		got = append(got, record{
			key:       key,
			value:     append([]byte(nil), value...),
			tombstone: tombstone,
		})
	})
	if !errors.Is(err, ErrTruncatedFile) {
		t.Fatalf("expected ErrTruncatedFile after partial recovery, got %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 recovered record before truncation, got %d (%+v)", len(got), got)
	}
	if got[0].key != "first" || string(got[0].value) != "complete" {
		t.Fatalf("expected first/complete, got %+v", got[0])
	}
}

// ----------------------------------------------------------------------------
// Edge: large values
// ----------------------------------------------------------------------------

func TestAppendReplay_LargeValue(t *testing.T) {
	w, _ := Open(tempWALPath(t))
	defer w.Close()
	big := make([]byte, 64*1024) // 64 KB
	for i := range big {
		big[i] = byte(i % 251)
	}
	if err := w.Append("big", big, false); err != nil {
		t.Fatalf("Append big: %v", err)
	}
	got, err := collect(w)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 || got[0].key != "big" {
		t.Fatalf("expected 1 record key=big, got %+v", got)
	}
	if len(got[0].value) != len(big) {
		t.Fatalf("value length mismatch: expected %d, got %d", len(big), len(got[0].value))
	}
	for i := range big {
		if got[0].value[i] != big[i] {
			t.Fatalf("value byte %d mismatch: expected %d, got %d", i, big[i], got[0].value[i])
		}
	}
}
