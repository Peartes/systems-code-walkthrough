package memtable

import (
	"math/rand"
	"sort"
	"testing"

	sk "github.com/peartes/lsm/src/skiplist"
)

// newTestMemtable builds a deterministic memtable for tests.
func newTestMemtable(seed int64) *Memtable {
	sl := sk.NewSkipList(16, 0.5, rand.New(rand.NewSource(seed)))
	return NewMemtable(&sl)
}

// ----------------------------------------------------------------------------
// Constructor sanity
// ----------------------------------------------------------------------------

func TestNew_IsEmpty(t *testing.T) {
	m := newTestMemtable(1)
	if m.Len() != 0 {
		t.Fatalf("expected Len=0 on new memtable, got %d", m.Len())
	}
	if m.SizeBytes() != 0 {
		t.Fatalf("expected SizeBytes=0 on new memtable, got %d", m.SizeBytes())
	}
	if m.IsFrozen() {
		t.Fatalf("expected IsFrozen=false on new memtable")
	}
}

// ----------------------------------------------------------------------------
// Put: size accounting
// ----------------------------------------------------------------------------

func TestPut_NewKey_AddsKeyAndValueBytes(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("foo", []byte("hello")) // 3 + 5 = 8
	if got, want := m.SizeBytes(), 8; got != want {
		t.Fatalf("after Put(foo, hello): expected SizeBytes=%d, got %d", want, got)
	}
	if got, want := m.Len(), 1; got != want {
		t.Fatalf("expected Len=%d, got %d", want, got)
	}
}

func TestPut_MultipleNewKeys_AccumulatesSize(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("a", []byte("11"))    // 1 + 2 = 3
	m.Put("bb", []byte("222"))  // 2 + 3 = 5
	m.Put("ccc", []byte("333")) // 3 + 3 = 6
	if got, want := m.SizeBytes(), 14; got != want {
		t.Fatalf("expected SizeBytes=%d, got %d", want, got)
	}
	if got, want := m.Len(), 3; got != want {
		t.Fatalf("expected Len=%d, got %d", want, got)
	}
}

func TestPut_OverwriteSameSize_NoSizeChange(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("abcde"))
	before := m.SizeBytes()
	m.Put("k", []byte("xyzwv")) // same length
	if got := m.SizeBytes(); got != before {
		t.Fatalf("overwrite with same-length value should not change size; before=%d after=%d", before, got)
	}
	if m.Len() != 1 {
		t.Fatalf("Len should stay 1 after overwrite, got %d", m.Len())
	}
}

func TestPut_OverwriteLarger_IncreasesSize(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("hi")) // 1 + 2 = 3
	m.Put("k", []byte("hello world!")) // value 12 bytes, key unchanged
	// expected: 1 + 12 = 13
	if got, want := m.SizeBytes(), 13; got != want {
		t.Fatalf("expected SizeBytes=%d, got %d", want, got)
	}
}

func TestPut_OverwriteSmaller_DecreasesSize(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("supercalifragilistic")) // 1 + 20 = 21
	m.Put("k", []byte("xx"))                   // 1 + 2 = 3
	if got, want := m.SizeBytes(), 3; got != want {
		t.Fatalf("expected SizeBytes=%d, got %d", want, got)
	}
}

// ----------------------------------------------------------------------------
// Delete: size accounting
// ----------------------------------------------------------------------------

func TestDelete_NewKey_AddsKeyBytesOnly(t *testing.T) {
	m := newTestMemtable(1)
	m.Delete("foo") // 3-byte key, nil value
	if got, want := m.SizeBytes(), 3; got != want {
		t.Fatalf("Delete of new key: expected SizeBytes=%d (just key bytes), got %d", want, got)
	}
	if m.Len() != 1 {
		t.Fatalf("Delete creates a tombstone entry; expected Len=1, got %d", m.Len())
	}
}

func TestDelete_OverwriteExisting_DropsValueBytes(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("alive")) // 1 + 5 = 6
	m.Delete("k")               // tombstone overwrites; value bytes drop, key stays
	if got, want := m.SizeBytes(), 1; got != want {
		t.Fatalf("after Put then Delete: expected SizeBytes=%d (just key), got %d", want, got)
	}
	if m.Len() != 1 {
		t.Fatalf("expected Len=1 (entry still present as tombstone), got %d", m.Len())
	}
}

func TestDelete_ThenPutResurrect(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("v1")) // 1 + 2 = 3
	m.Delete("k")            // 1
	m.Put("k", []byte("v2")) // value 2 bytes
	if got, want := m.SizeBytes(), 3; got != want {
		t.Fatalf("expected SizeBytes=%d after resurrect, got %d", want, got)
	}
	v, tombstone, found := m.Get("k")
	if !found || tombstone {
		t.Fatalf("expected found=true, tombstone=false after resurrect; got found=%v tombstone=%v", found, tombstone)
	}
	if string(v) != "v2" {
		t.Fatalf("expected v=v2 after resurrect, got %q", v)
	}
}

// ----------------------------------------------------------------------------
// Get: three-state semantics
// ----------------------------------------------------------------------------

func TestGet_MissingKey(t *testing.T) {
	m := newTestMemtable(1)
	v, tombstone, found := m.Get("nope")
	if found {
		t.Fatalf("expected found=false for missing key")
	}
	if tombstone {
		t.Fatalf("expected tombstone=false for missing key")
	}
	if v != nil {
		t.Fatalf("expected nil value for missing key, got %q", v)
	}
}

func TestGet_LiveEntry(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("v"))
	v, tombstone, found := m.Get("k")
	if !found || tombstone {
		t.Fatalf("expected (found=true, tombstone=false); got found=%v tombstone=%v", found, tombstone)
	}
	if string(v) != "v" {
		t.Fatalf("expected value=v, got %q", v)
	}
}

func TestGet_TombstonedEntry(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("v"))
	m.Delete("k")
	v, tombstone, found := m.Get("k")
	if !found {
		t.Fatalf("tombstones must report found=true so the LSM stops searching older layers")
	}
	if !tombstone {
		t.Fatalf("expected tombstone=true")
	}
	if v != nil {
		t.Fatalf("expected nil value on tombstone, got %q", v)
	}
}

// ----------------------------------------------------------------------------
// Iterate
// ----------------------------------------------------------------------------

func TestIterate_SortedOrder_IncludesTombstones(t *testing.T) {
	m := newTestMemtable(42)
	m.Put("delta", []byte("d"))
	m.Put("alpha", []byte("a"))
	m.Put("charlie", []byte("c"))
	m.Delete("bravo") // tombstone, no prior Put
	m.Put("echo", []byte("e"))

	got := []string{}
	seenTombstones := map[string]bool{}
	m.Iterate(func(key string, value []byte, tombstone bool) bool {
		got = append(got, key)
		seenTombstones[key] = tombstone
		return true
	})

	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if len(got) != len(want) {
		t.Fatalf("expected %d entries, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d: expected %q, got %q (full: %v)", i, want[i], got[i], got)
		}
	}
	if !seenTombstones["bravo"] {
		t.Fatalf("bravo should be a tombstone in iteration")
	}
}

func TestIterate_StopsEarly(t *testing.T) {
	m := newTestMemtable(1)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		m.Put(k, []byte(k))
	}
	calls := 0
	m.Iterate(func(key string, value []byte, tombstone bool) bool {
		calls++
		return key != "c"
	})
	if calls != 3 {
		t.Fatalf("expected iteration to stop after 'c' (3 calls), got %d", calls)
	}
}

// ----------------------------------------------------------------------------
// Freeze semantics
// ----------------------------------------------------------------------------

func TestFreeze_SetsFlag(t *testing.T) {
	m := newTestMemtable(1)
	m.Freeze()
	if !m.IsFrozen() {
		t.Fatalf("expected IsFrozen=true after Freeze()")
	}
}

func TestFreeze_Idempotent(t *testing.T) {
	m := newTestMemtable(1)
	m.Freeze()
	m.Freeze() // should not panic
	if !m.IsFrozen() {
		t.Fatalf("expected IsFrozen=true after double Freeze()")
	}
}

func TestFrozen_PutPanics(t *testing.T) {
	m := newTestMemtable(1)
	m.Freeze()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected Put on frozen memtable to panic")
		}
	}()
	m.Put("k", []byte("v"))
}

func TestFrozen_DeletePanics(t *testing.T) {
	m := newTestMemtable(1)
	m.Freeze()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected Delete on frozen memtable to panic")
		}
	}()
	m.Delete("k")
}

// Regression test for the bug where Delete on a frozen memtable mutated the
// skip list before checking the frozen flag.
func TestFrozen_DeleteDoesNotMutateOnPanic(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("v")) // 1 + 1 = 2
	sizeBefore := m.SizeBytes()
	lenBefore := m.Len()
	m.Freeze()

	func() {
		defer func() { _ = recover() }()
		m.Delete("k") // should panic without mutating
	}()

	if m.SizeBytes() != sizeBefore {
		t.Fatalf("frozen Delete must not change size; before=%d after=%d", sizeBefore, m.SizeBytes())
	}
	if m.Len() != lenBefore {
		t.Fatalf("frozen Delete must not change Len; before=%d after=%d", lenBefore, m.Len())
	}
	v, tombstone, found := m.Get("k")
	if !found || tombstone {
		t.Fatalf("frozen Delete must not have tombstoned the entry; got found=%v tombstone=%v", found, tombstone)
	}
	if string(v) != "v" {
		t.Fatalf("frozen Delete must not have changed value; got %q", v)
	}
}

func TestFrozen_PutDoesNotMutateOnPanic(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("original")) // 1 + 8 = 9
	sizeBefore := m.SizeBytes()
	m.Freeze()

	func() {
		defer func() { _ = recover() }()
		m.Put("k", []byte("changed"))
	}()

	if m.SizeBytes() != sizeBefore {
		t.Fatalf("frozen Put must not change size; before=%d after=%d", sizeBefore, m.SizeBytes())
	}
	v, _, _ := m.Get("k")
	if string(v) != "original" {
		t.Fatalf("frozen Put must not change value; got %q", v)
	}
}

func TestFrozen_GetStillWorks(t *testing.T) {
	m := newTestMemtable(1)
	m.Put("k", []byte("v"))
	m.Freeze()
	v, _, found := m.Get("k")
	if !found || string(v) != "v" {
		t.Fatalf("Get must work on frozen memtable; got found=%v v=%q", found, v)
	}
}

func TestFrozen_IterateStillWorks(t *testing.T) {
	m := newTestMemtable(1)
	for _, k := range []string{"a", "b", "c"} {
		m.Put(k, []byte(k))
	}
	m.Freeze()

	got := []string{}
	m.Iterate(func(key string, value []byte, tombstone bool) bool {
		got = append(got, key)
		return true
	})
	want := []string{"a", "b", "c"}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("expected %d entries, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d: expected %q got %q", i, want[i], got[i])
		}
	}
}
