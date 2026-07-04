package lsm

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// ----------------------------------------------------------------------------
// Load — fresh / missing / degenerate states
// ----------------------------------------------------------------------------

func TestLoad_MissingFileReturnsDefault(t *testing.T) {
	dir := t.TempDir()
	m, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m.NextSeqNo != 1 {
		t.Errorf("expected NextSeqNo=1 on fresh Load, got %d", m.NextSeqNo)
	}
	if len(m.LiveSSTables) != 0 {
		t.Errorf("expected empty LiveSSTables on fresh Load, got %v", m.LiveSSTables)
	}
}

func TestLoad_EmptyFileReturnsDefault(t *testing.T) {
	dir := t.TempDir()
	// Create an empty MANIFEST file (e.g. from a save that was killed before writing).
	if err := os.WriteFile(filepath.Join(dir, MANIFEST), nil, 0644); err != nil {
		t.Fatalf("preload empty file: %v", err)
	}
	m, err := Load(dir)
	if err != nil {
		t.Fatalf("Load on empty file: %v", err)
	}
	if m.NextSeqNo != 1 || len(m.LiveSSTables) != 0 {
		t.Fatalf("expected default state; got NextSeqNo=%d LiveSSTables=%v", m.NextSeqNo, m.LiveSSTables)
	}
}

func TestLoad_TruncatedFileReturnsDefault(t *testing.T) {
	dir := t.TempDir()
	// File exists but is shorter than the 8-byte nextSeqNo header — treat as fresh.
	if err := os.WriteFile(filepath.Join(dir, MANIFEST), []byte{1, 2, 3}, 0644); err != nil {
		t.Fatalf("preload short file: %v", err)
	}
	m, err := Load(dir)
	if err != nil {
		t.Fatalf("Load on truncated file: %v", err)
	}
	if m.NextSeqNo != 1 || len(m.LiveSSTables) != 0 {
		t.Fatalf("expected default state; got NextSeqNo=%d LiveSSTables=%v", m.NextSeqNo, m.LiveSSTables)
	}
}

// ----------------------------------------------------------------------------
// Save + Load round-trip
// ----------------------------------------------------------------------------

func TestSaveLoad_EmptyManifest(t *testing.T) {
	dir := t.TempDir()
	m := &Manifest{dir: dir, NextSeqNo: 42, LiveSSTables: nil}
	if err := m.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	m2, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m2.NextSeqNo != 42 {
		t.Errorf("NextSeqNo round-trip: expected 42, got %d", m2.NextSeqNo)
	}
	if len(m2.LiveSSTables) != 0 {
		t.Errorf("expected empty LiveSSTables, got %v", m2.LiveSSTables)
	}
}

func TestSaveLoad_WithSSTables(t *testing.T) {
	dir := t.TempDir()
	m := &Manifest{dir: dir, NextSeqNo: 100, LiveSSTables: []uint64{1, 3, 5, 7}}
	if err := m.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	m2, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m2.NextSeqNo != 100 {
		t.Errorf("NextSeqNo round-trip: expected 100, got %d", m2.NextSeqNo)
	}
	if !reflect.DeepEqual(m2.LiveSSTables, []uint64{1, 3, 5, 7}) {
		t.Errorf("LiveSSTables round-trip: expected [1 3 5 7], got %v", m2.LiveSSTables)
	}
}

func TestSaveLoad_ManySSTables(t *testing.T) {
	dir := t.TempDir()
	seqnos := make([]uint64, 100)
	for i := range seqnos {
		seqnos[i] = uint64(i + 1)
	}
	m := &Manifest{dir: dir, NextSeqNo: 500, LiveSSTables: seqnos}
	if err := m.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	m2, _ := Load(dir)
	if !reflect.DeepEqual(m2.LiveSSTables, seqnos) {
		t.Errorf("100-entry LiveSSTables round-trip mismatch")
	}
	if m2.NextSeqNo != 500 {
		t.Errorf("NextSeqNo mismatch: expected 500, got %d", m2.NextSeqNo)
	}
}

func TestSaveLoad_LargeSeqNo(t *testing.T) {
	// Verify uint64 range works — this would fail if we accidentally used uint32.
	dir := t.TempDir()
	big := uint64(1) << 40 // ~1 trillion; fits in uint64, overflows uint32
	m := &Manifest{dir: dir, NextSeqNo: big, LiveSSTables: []uint64{big - 1, big - 2}}
	if err := m.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	m2, _ := Load(dir)
	if m2.NextSeqNo != big {
		t.Errorf("large NextSeqNo mismatch: expected %d, got %d", big, m2.NextSeqNo)
	}
	if m2.LiveSSTables[0] != big-1 || m2.LiveSSTables[1] != big-2 {
		t.Errorf("large SSTable seqno mismatch: got %v", m2.LiveSSTables)
	}
}

// ----------------------------------------------------------------------------
// AllocSeqno
// ----------------------------------------------------------------------------

func TestAllocSeqno_ReturnsPreIncrementValue(t *testing.T) {
	// Regression: the earlier bug incremented FIRST then returned, so the first
	// call returned 2 and the value 1 was never used.
	m := &Manifest{NextSeqNo: 1}
	first := m.AllocSeqno()
	if first != 1 {
		t.Fatalf("first AllocSeqno should return 1 (starting NextSeqNo), got %d", first)
	}
	if m.NextSeqNo != 2 {
		t.Fatalf("after alloc, NextSeqNo should be 2, got %d", m.NextSeqNo)
	}
}

func TestAllocSeqno_Monotonic(t *testing.T) {
	m := &Manifest{NextSeqNo: 10}
	seen := make(map[uint64]bool)
	prev := uint64(0)
	for i := 0; i < 100; i++ {
		s := m.AllocSeqno()
		if seen[s] {
			t.Fatalf("duplicate seqno %d", s)
		}
		if s <= prev && i > 0 {
			t.Fatalf("non-monotonic: prev=%d, got=%d", prev, s)
		}
		seen[s] = true
		prev = s
	}
}

// ----------------------------------------------------------------------------
// AddSSTable
// ----------------------------------------------------------------------------

func TestAddSSTable_PersistsAndAppends(t *testing.T) {
	dir := t.TempDir()
	m := &Manifest{dir: dir, NextSeqNo: 5, LiveSSTables: []uint64{1, 3}}

	if err := m.AddSSTable(4); err != nil {
		t.Fatalf("AddSSTable: %v", err)
	}
	if !reflect.DeepEqual(m.LiveSSTables, []uint64{1, 3, 4}) {
		t.Errorf("expected in-memory LiveSSTables=[1 3 4], got %v", m.LiveSSTables)
	}
	// Reload and verify the addition was persisted.
	m2, _ := Load(dir)
	if !reflect.DeepEqual(m2.LiveSSTables, []uint64{1, 3, 4}) {
		t.Errorf("persisted LiveSSTables mismatch: got %v", m2.LiveSSTables)
	}
}

func TestAddSSTable_MultipleCalls(t *testing.T) {
	dir := t.TempDir()
	m := &Manifest{dir: dir, NextSeqNo: 1, LiveSSTables: nil}
	for i := uint64(1); i <= 5; i++ {
		if err := m.AddSSTable(i); err != nil {
			t.Fatalf("AddSSTable(%d): %v", i, err)
		}
	}
	m2, _ := Load(dir)
	want := []uint64{1, 2, 3, 4, 5}
	if !reflect.DeepEqual(m2.LiveSSTables, want) {
		t.Errorf("expected %v, got %v", want, m2.LiveSSTables)
	}
}

// ----------------------------------------------------------------------------
// Save atomicity — MANIFEST is never partial
// ----------------------------------------------------------------------------

func TestSave_MainFileNotPartialUnderNormalOperation(t *testing.T) {
	// This tests the standard case: after Save, MANIFEST exists and is fully valid.
	// We can't easily simulate a mid-save crash in unit tests, but we CAN verify
	// that (a) the tmp file is cleaned up (renamed away, so it shouldn't remain)
	// and (b) the MANIFEST is a valid state after Save.
	dir := t.TempDir()
	m := &Manifest{dir: dir, NextSeqNo: 99, LiveSSTables: []uint64{7, 8, 9}}
	if err := m.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Tmp file should not remain (rename should have moved it).
	tmpPath := filepath.Join(dir, MANIFEST+".tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("MANIFEST.tmp should not exist after successful Save; got err=%v", err)
	}

	// MANIFEST should exist and be loadable.
	if _, err := os.Stat(filepath.Join(dir, MANIFEST)); err != nil {
		t.Errorf("MANIFEST should exist after Save: %v", err)
	}

	// The loaded state must equal what we saved.
	m2, err := Load(dir)
	if err != nil {
		t.Fatalf("Load after Save: %v", err)
	}
	if m2.NextSeqNo != 99 {
		t.Errorf("NextSeqNo mismatch: expected 99, got %d", m2.NextSeqNo)
	}
	if !reflect.DeepEqual(m2.LiveSSTables, []uint64{7, 8, 9}) {
		t.Errorf("LiveSSTables mismatch: got %v", m2.LiveSSTables)
	}
}

func TestSave_OverwritesPreviousManifest(t *testing.T) {
	// Regression: verify save-then-save-then-load returns the LATEST state,
	// not any mix of the two. This shakes out any O_TRUNC or file-position bugs.
	dir := t.TempDir()
	m := &Manifest{dir: dir, NextSeqNo: 10, LiveSSTables: []uint64{1, 2, 3, 4, 5}}
	if err := m.Save(); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	// Now mutate to a SMALLER state and save again.
	m.NextSeqNo = 20
	m.LiveSSTables = []uint64{99}
	if err := m.Save(); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	m2, _ := Load(dir)
	if m2.NextSeqNo != 20 {
		t.Errorf("expected NextSeqNo=20 after overwrite, got %d", m2.NextSeqNo)
	}
	if !reflect.DeepEqual(m2.LiveSSTables, []uint64{99}) {
		t.Errorf("expected LiveSSTables=[99] after overwrite, got %v", m2.LiveSSTables)
	}
}
