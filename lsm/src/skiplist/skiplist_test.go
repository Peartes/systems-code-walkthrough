package skiplist

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// newTestList builds a deterministic skip list for tests.
// Always seed the rand source so test failures are reproducible.
func newTestList(seed int64) *SkipList {
	sl := NewSkipList(16, 0.5, rand.New(rand.NewSource(seed)))
	return &sl
}

// ----------------------------------------------------------------------------
// Empty list
// ----------------------------------------------------------------------------

func TestEmptyList_LenIsZero(t *testing.T) {
	sl := newTestList(1)
	if sl.Len() != 0 {
		t.Fatalf("expected Len=0 on a fresh list, got %d", sl.Len())
	}
}

func TestEmptyList_GetReturnsNotFound(t *testing.T) {
	sl := newTestList(1)
	value, tombstone, found := sl.Get("anything")
	if found {
		t.Fatalf("expected found=false on empty list, got found=true value=%q tombstone=%v", value, tombstone)
	}
	if value != nil {
		t.Fatalf("expected nil value on miss, got %q", value)
	}
	if tombstone {
		t.Fatalf("expected tombstone=false on miss, got true")
	}
}

func TestEmptyList_IterateDoesNothing(t *testing.T) {
	sl := newTestList(1)
	called := 0
	sl.Iterate(func(key string, value []byte, tombstone bool) bool {
		called++
		return true
	})
	if called != 0 {
		t.Fatalf("expected fn to be called 0 times on empty list, called %d times", called)
	}
}

// ----------------------------------------------------------------------------
// Basic insert/get
// ----------------------------------------------------------------------------

func TestInsertGet_SingleEntry(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("foo", []byte("bar"), false)

	value, tombstone, found := sl.Get("foo")
	if !found {
		t.Fatalf("expected found=true for inserted key")
	}
	if string(value) != "bar" {
		t.Fatalf("expected value=bar, got %q", value)
	}
	if tombstone {
		t.Fatalf("expected tombstone=false, got true")
	}
	if sl.Len() != 1 {
		t.Fatalf("expected Len=1 after one insert, got %d", sl.Len())
	}
}

func TestInsertGet_ManyEntries(t *testing.T) {
	sl := newTestList(42)
	entries := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "dark-red",
		"date":   "brown",
		"elder":  "purple",
	}
	for k, v := range entries {
		sl.Insert(k, []byte(v), false)
	}
	if sl.Len() != len(entries) {
		t.Fatalf("expected Len=%d, got %d", len(entries), sl.Len())
	}
	for k, want := range entries {
		got, tombstone, found := sl.Get(k)
		if !found {
			t.Fatalf("expected to find %q", k)
		}
		if tombstone {
			t.Fatalf("expected tombstone=false for %q", k)
		}
		if string(got) != want {
			t.Fatalf("for %q: expected %q, got %q", k, want, got)
		}
	}
}

// ----------------------------------------------------------------------------
// Overwrite semantics
// ----------------------------------------------------------------------------

func TestInsert_OverwritesExistingKey(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("foo", []byte("v1"), false)
	sl.Insert("foo", []byte("v2"), false)
	sl.Insert("foo", []byte("v3"), false)

	if sl.Len() != 1 {
		t.Fatalf("expected Len=1 after 3 inserts of same key, got %d (duplicate nodes spliced?)", sl.Len())
	}
	value, _, found := sl.Get("foo")
	if !found {
		t.Fatalf("expected found=true")
	}
	if string(value) != "v3" {
		t.Fatalf("expected latest value v3, got %q", value)
	}
}

// ----------------------------------------------------------------------------
// Tombstones
// ----------------------------------------------------------------------------

func TestInsert_TombstoneStored(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("foo", nil, true)

	value, tombstone, found := sl.Get("foo")
	if !found {
		t.Fatalf("tombstone should still be found in the skip list")
	}
	if !tombstone {
		t.Fatalf("expected tombstone=true")
	}
	if value != nil {
		t.Fatalf("expected nil value for tombstone, got %q", value)
	}
}

func TestInsert_TombstoneOverwritesValue(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("foo", []byte("v1"), false)
	sl.Insert("foo", nil, true) // tombstone the key

	value, tombstone, found := sl.Get("foo")
	if !found {
		t.Fatalf("expected found=true (tombstone is a stored entry)")
	}
	if !tombstone {
		t.Fatalf("expected tombstone=true after tombstone overwrite")
	}
	if value != nil {
		t.Fatalf("expected nil value after tombstone, got %q", value)
	}
	if sl.Len() != 1 {
		t.Fatalf("expected Len=1 (tombstone is an overwrite, not a new node), got %d", sl.Len())
	}
}

func TestInsert_ResurrectAfterTombstone(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("foo", []byte("v1"), false)
	sl.Insert("foo", nil, true)
	sl.Insert("foo", []byte("v2"), false) // resurrect

	value, tombstone, found := sl.Get("foo")
	if !found {
		t.Fatalf("expected found=true")
	}
	if tombstone {
		t.Fatalf("expected tombstone=false after resurrection")
	}
	if string(value) != "v2" {
		t.Fatalf("expected v2 after resurrect, got %q", value)
	}
	if sl.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", sl.Len())
	}
}

// ----------------------------------------------------------------------------
// Missing-key reads at boundaries
// ----------------------------------------------------------------------------

func TestGet_MissingKey_BetweenEntries(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("a", []byte("1"), false)
	sl.Insert("c", []byte("3"), false)
	sl.Insert("e", []byte("5"), false)

	value, tombstone, found := sl.Get("b")
	if found {
		t.Fatalf("expected not found for missing key 'b' between 'a' and 'c'; got value=%q tombstone=%v", value, tombstone)
	}
	if value != nil {
		t.Fatalf("expected nil value on miss, got %q (predecessor leaked!)", value)
	}
}

func TestGet_MissingKey_BeforeAllEntries(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("m", []byte("v"), false)
	sl.Insert("z", []byte("v"), false)

	_, _, found := sl.Get("a")
	if found {
		t.Fatalf("expected not found for 'a' which precedes all entries")
	}
}

func TestGet_MissingKey_AfterAllEntries(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("a", []byte("v"), false)
	sl.Insert("b", []byte("v"), false)

	_, _, found := sl.Get("z")
	if found {
		t.Fatalf("expected not found for 'z' which follows all entries")
	}
}

// ----------------------------------------------------------------------------
// Iterate
// ----------------------------------------------------------------------------

func TestIterate_InSortedOrder(t *testing.T) {
	sl := newTestList(42)
	keys := []string{"delta", "alpha", "charlie", "bravo", "echo"}
	for _, k := range keys {
		sl.Insert(k, []byte(k+"-val"), false)
	}

	got := []string{}
	sl.Iterate(func(key string, value []byte, tombstone bool) bool {
		got = append(got, key)
		return true
	})

	wantSorted := make([]string, len(keys))
	copy(wantSorted, keys)
	sort.Strings(wantSorted)

	if len(got) != len(wantSorted) {
		t.Fatalf("expected %d entries, iterated %d", len(wantSorted), len(got))
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			t.Fatalf("at index %d: expected %q, got %q (full order: %v)", i, wantSorted[i], got[i], got)
		}
	}
}

func TestIterate_IncludesTombstones(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("a", []byte("v1"), false)
	sl.Insert("b", nil, true) // tombstone
	sl.Insert("c", []byte("v3"), false)

	seen := []string{}
	tombstones := map[string]bool{}
	sl.Iterate(func(key string, value []byte, tombstone bool) bool {
		seen = append(seen, key)
		tombstones[key] = tombstone
		return true
	})

	if len(seen) != 3 {
		t.Fatalf("expected 3 entries (including tombstone), got %d", len(seen))
	}
	if !tombstones["b"] {
		t.Fatalf("expected b to be a tombstone in iteration")
	}
	if tombstones["a"] || tombstones["c"] {
		t.Fatalf("expected a and c to be live, got tombstone flags %v", tombstones)
	}
}

func TestIterate_StopsEarlyOnFalseReturn(t *testing.T) {
	sl := newTestList(1)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		sl.Insert(k, []byte(k), false)
	}

	calls := 0
	sl.Iterate(func(key string, value []byte, tombstone bool) bool {
		calls++
		if key == "c" {
			return false // stop
		}
		return true
	})

	if calls != 3 {
		t.Fatalf("expected iteration to stop after returning false at 'c' (3 calls), got %d", calls)
	}
}

// ----------------------------------------------------------------------------
// Len semantics
// ----------------------------------------------------------------------------

func TestLen_IncrementsOnUniqueInsertOnly(t *testing.T) {
	sl := newTestList(1)
	sl.Insert("a", []byte("1"), false)
	sl.Insert("b", []byte("2"), false)
	sl.Insert("a", []byte("3"), false) // overwrite, should not bump Len
	sl.Insert("c", []byte("4"), false)
	if sl.Len() != 3 {
		t.Fatalf("expected Len=3 (3 unique keys), got %d", sl.Len())
	}
}

// ----------------------------------------------------------------------------
// MaxHeight (height generator) properties
// ----------------------------------------------------------------------------

func TestMaxHeight_NeverZero(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	const trials = 100_000
	for i := 0; i < trials; i++ {
		h := MaxHeight(r, 16, 0.5)
		if h < 1 || h > 16 {
			t.Fatalf("MaxHeight returned %d out of [1,16] on trial %d", h, i)
		}
	}
}

func TestMaxHeight_Deterministic(t *testing.T) {
	r1 := rand.New(rand.NewSource(99))
	r2 := rand.New(rand.NewSource(99))
	for i := 0; i < 100; i++ {
		h1 := MaxHeight(r1, 16, 0.5)
		h2 := MaxHeight(r2, 16, 0.5)
		if h1 != h2 {
			t.Fatalf("seeded rand should be deterministic: trial %d got %d vs %d", i, h1, h2)
		}
	}
}

func TestMaxHeight_DistributionRoughlyGeometric(t *testing.T) {
	// Not a strict statistical test — just sanity-check the geometric shape.
	// With p=0.5, level 1 should be ~50% of nodes, level 2 ~25%, level 3 ~12.5%, etc.
	// We allow generous tolerance because trials are limited and we want to avoid flakes.
	r := rand.New(rand.NewSource(7))
	const trials = 100_000
	counts := make([]int, 17) // index 0 unused, 1..16
	for i := 0; i < trials; i++ {
		h := MaxHeight(r, 16, 0.5)
		counts[h]++
	}
	// Level 1 should dominate. Cap level should be very rare.
	if counts[1] < trials/3 {
		t.Fatalf("expected level 1 to receive at least ~33%% of nodes, got %d/%d", counts[1], trials)
	}
	if counts[1] <= counts[2] {
		t.Fatalf("expected count[1] > count[2], got %d vs %d", counts[1], counts[2])
	}
	if counts[2] <= counts[3] {
		t.Fatalf("expected count[2] > count[3], got %d vs %d", counts[2], counts[3])
	}
}

// ----------------------------------------------------------------------------
// Randomized stress: bulk insert + verify
// ----------------------------------------------------------------------------

func TestStress_RandomInsertsAreRetrievable(t *testing.T) {
	sl := newTestList(2026)
	// Generate keys with a separate rand source so the skip list's source isn't disturbed.
	keyGen := rand.New(rand.NewSource(13))
	const N = 5_000
	want := make(map[string]string, N)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k-%08d-%d", keyGen.Intn(1_000_000), i)
		v := fmt.Sprintf("v-%d", i)
		sl.Insert(k, []byte(v), false)
		want[k] = v
	}
	if sl.Len() != len(want) {
		t.Fatalf("expected Len=%d, got %d", len(want), sl.Len())
	}
	for k, v := range want {
		got, _, found := sl.Get(k)
		if !found {
			t.Fatalf("missing key after bulk insert: %q", k)
		}
		if string(got) != v {
			t.Fatalf("wrong value for %q: expected %q, got %q", k, v, got)
		}
	}
}

func TestStress_IterateMatchesSortedKeys(t *testing.T) {
	sl := newTestList(2026)
	keyGen := rand.New(rand.NewSource(13))
	const N = 1_000
	want := make([]string, 0, N)
	seen := make(map[string]bool, N)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k-%08d", keyGen.Intn(N*10))
		if seen[k] {
			continue // skip duplicates to keep `want` size predictable
		}
		seen[k] = true
		sl.Insert(k, []byte("v"), false)
		want = append(want, k)
	}
	sort.Strings(want)

	got := make([]string, 0, len(want))
	sl.Iterate(func(key string, value []byte, tombstone bool) bool {
		got = append(got, key)
		return true
	})

	if len(got) != len(want) {
		t.Fatalf("expected %d entries from Iterate, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sort order mismatch at index %d: expected %q, got %q", i, want[i], got[i])
		}
	}
}

func TestStress_OverwriteAllKeys(t *testing.T) {
	sl := newTestList(7)
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%03d", i)
		sl.Insert(keys[i], []byte("first"), false)
	}
	// Overwrite all
	for _, k := range keys {
		sl.Insert(k, []byte("second"), false)
	}
	if sl.Len() != len(keys) {
		t.Fatalf("expected Len=%d after overwrite, got %d", len(keys), sl.Len())
	}
	for _, k := range keys {
		got, _, found := sl.Get(k)
		if !found {
			t.Fatalf("missing key after overwrite: %q", k)
		}
		if string(got) != "second" {
			t.Fatalf("expected 'second' for %q, got %q", k, got)
		}
	}
}
