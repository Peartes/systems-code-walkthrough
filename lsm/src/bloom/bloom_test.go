package bloom

import (
	"errors"
	"fmt"
	"testing"
)

// ----------------------------------------------------------------------------
// Constructor: sizing math
// ----------------------------------------------------------------------------

func TestNew_BasicConstruction(t *testing.T) {
	b, err := New(1000, 0.01)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b == nil {
		t.Fatalf("New returned nil filter")
	}
}

func TestNew_MIsMultipleOf8(t *testing.T) {
	cases := []struct {
		n   int
		fpr float64
	}{
		{100, 0.01},
		{500, 0.001},
		{10000, 0.05},
		{1, 0.1},
	}
	for _, c := range cases {
		b, err := New(c.n, c.fpr)
		if err != nil {
			t.Fatalf("New(%d, %g): %v", c.n, c.fpr, err)
		}
		if b.m%8 != 0 {
			t.Errorf("New(%d, %g): m=%d is not a multiple of 8", c.n, c.fpr, b.m)
		}
		if b.m/8 != len(b.table) {
			t.Errorf("New(%d, %g): m=%d but table has %d bytes; expected %d", c.n, c.fpr, b.m, len(b.table), b.m/8)
		}
	}
}

func TestNew_KIsAtLeast1(t *testing.T) {
	b, err := New(1000, 0.01)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.k < 1 {
		t.Fatalf("expected k >= 1, got %d", b.k)
	}
}

// Verify the m formula approximately matches expected sizes.
// m = -n * ln(p) / (ln 2)^2 → 10 bits per key at p=0.01.
func TestNew_BitsPerKeyMatchesRuleOfThumb(t *testing.T) {
	b, err := New(10000, 0.01)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Expected: ~9.6 bits per key (rounded up + to multiple of 8).
	bitsPerKey := float64(b.m) / 10000.0
	if bitsPerKey < 9.0 || bitsPerKey > 11.5 {
		t.Fatalf("expected ~10 bits per key for p=0.01; got %.2f (m=%d, n=10000)", bitsPerKey, b.m)
	}
	// Expected: k ≈ 7 at 10 bits/key.
	if b.k < 6 || b.k > 8 {
		t.Fatalf("expected k≈7 for ~10 bits/key, got k=%d", b.k)
	}
}

// ----------------------------------------------------------------------------
// Empty filter behavior
// ----------------------------------------------------------------------------

func TestEmpty_MayContainAlwaysFalse(t *testing.T) {
	b, _ := New(1000, 0.01)
	for _, key := range []string{"a", "b", "anything", "", "hello world"} {
		ok, err := b.MayContain(key)
		if err != nil {
			t.Fatalf("MayContain(%q): %v", key, err)
		}
		if ok {
			t.Errorf("empty filter should never return true; got true for %q", key)
		}
	}
}

// ----------------------------------------------------------------------------
// No false negatives
// ----------------------------------------------------------------------------

func TestAdd_NoFalseNegatives(t *testing.T) {
	b, _ := New(1000, 0.01)
	keys := []string{}
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("inserted-key-%05d", i)
		keys = append(keys, k)
		if err := b.Add(k); err != nil {
			t.Fatalf("Add(%q): %v", k, err)
		}
	}
	// Every inserted key MUST report MayContain=true. No exceptions.
	for _, k := range keys {
		ok, err := b.MayContain(k)
		if err != nil {
			t.Fatalf("MayContain(%q): %v", k, err)
		}
		if !ok {
			t.Fatalf("FALSE NEGATIVE: inserted key %q reported MayContain=false", k)
		}
	}
}

func TestAdd_SingleKeyContains(t *testing.T) {
	b, _ := New(100, 0.01)
	if err := b.Add("hello"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	ok, _ := b.MayContain("hello")
	if !ok {
		t.Fatalf("expected MayContain(hello)=true after Add(hello)")
	}
}

// ----------------------------------------------------------------------------
// False positive rate matches target
// ----------------------------------------------------------------------------

func TestFPR_MatchesTarget(t *testing.T) {
	const n = 5000
	const target = 0.01

	b, _ := New(n, target)

	// Insert n keys.
	for i := 0; i < n; i++ {
		b.Add(fmt.Sprintf("inserted-%06d", i))
	}

	// Query 50,000 keys that were NOT inserted; count how many give false positives.
	const probes = 50_000
	falsePositives := 0
	for i := 0; i < probes; i++ {
		key := fmt.Sprintf("absent-%06d", i)
		ok, _ := b.MayContain(key)
		if ok {
			falsePositives++
		}
	}

	observed := float64(falsePositives) / probes
	// Allow generous tolerance: target=1%, accept up to 2.5% observed.
	// (Tight test would be ~1%, but stochastic variance + Kirsch-Mitzenmacher
	// approximation can push slightly above the formula.)
	if observed > 0.025 {
		t.Fatalf("observed FPR %.4f exceeds tolerance (target=%.4f, max accepted=0.025); m=%d k=%d",
			observed, target, b.m, b.k)
	}
	t.Logf("observed FPR: %.4f (target %.4f); m=%d k=%d", observed, target, b.m, b.k)
}

// ----------------------------------------------------------------------------
// Serialize / Deserialize round-trip
// ----------------------------------------------------------------------------

func TestSerializeDeserialize_RoundTrip(t *testing.T) {
	b, _ := New(1000, 0.01)
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"}
	for _, k := range keys {
		b.Add(k)
	}

	data := b.Serialize()
	b2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if b2.m != b.m {
		t.Fatalf("m mismatch: original=%d, deserialized=%d", b.m, b2.m)
	}
	if b2.k != b.k {
		t.Fatalf("k mismatch: original=%d, deserialized=%d", b.k, b2.k)
	}
	if len(b2.table) != len(b.table) {
		t.Fatalf("table length mismatch: original=%d, deserialized=%d", len(b.table), len(b2.table))
	}
	// All inserted keys must still report MayContain=true on the rebuilt filter.
	for _, k := range keys {
		ok, _ := b2.MayContain(k)
		if !ok {
			t.Fatalf("FALSE NEGATIVE after deserialize for key %q", k)
		}
	}
	// And a key never inserted should very likely not be present.
	ok, _ := b2.MayContain("never-inserted-xyz-12345")
	_ = ok // could be a false positive at ~1%, don't assert; the FPR test covers this.
}

func TestSerializeDeserialize_PreservesFilterBehavior(t *testing.T) {
	// Stronger test: build a filter, serialize, deserialize, and verify
	// MayContain returns the same answer for both filters across a broader key set.
	b, _ := New(500, 0.05)
	for i := 0; i < 200; i++ {
		b.Add(fmt.Sprintf("k-%d", i))
	}

	b2, err := Deserialize(b.Serialize())
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		a, _ := b.MayContain(key)
		c, _ := b2.MayContain(key)
		if a != c {
			t.Fatalf("MayContain disagreement after round-trip for %q: original=%v, deserialized=%v", key, a, c)
		}
	}
}

// ----------------------------------------------------------------------------
// Deserialize validation
// ----------------------------------------------------------------------------

func TestDeserialize_RejectsShortInput(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{0x01},
		make([]byte, 11), // one byte short of header
	}
	for i, data := range cases {
		_, err := Deserialize(data)
		if err == nil {
			t.Errorf("case %d (len=%d): expected error on short input, got nil", i, len(data))
		}
		if err != nil && !errors.Is(err, ErrDeserializingBloom) {
			t.Errorf("case %d: expected ErrDeserializingBloom, got %v", i, err)
		}
	}
}

func TestDeserialize_RejectsZeroM(t *testing.T) {
	// Construct a header with m=0.
	data := make([]byte, 12)
	// m=0, k=7, bits_len=0
	data[4] = 7
	_, err := Deserialize(data)
	if err == nil {
		t.Fatalf("expected error when m=0, got nil")
	}
	if !errors.Is(err, ErrDeserializingBloom) {
		t.Fatalf("expected ErrDeserializingBloom, got %v", err)
	}
}

func TestDeserialize_RejectsZeroK(t *testing.T) {
	data := make([]byte, 12)
	// m=8, k=0, bits_len=1
	data[0] = 8
	data[8] = 1
	data = append(data, 0x00) // one bit byte
	_, err := Deserialize(data)
	if err == nil {
		t.Fatalf("expected error when k=0, got nil")
	}
}

func TestDeserialize_RejectsTruncatedBits(t *testing.T) {
	// Header claims bits_len=100, but only 10 bits-bytes follow.
	data := make([]byte, 12)
	// m=800, k=7, bits_len=100
	data[0] = 0x20
	data[1] = 0x03 // 800 LE = 0x0320
	data[4] = 7
	data[8] = 100
	data = append(data, make([]byte, 10)...) // only 10 bytes of payload
	_, err := Deserialize(data)
	if err == nil {
		t.Fatalf("expected error on truncated bits, got nil")
	}
}

func TestDeserialize_RejectsBitsLenMismatchingM(t *testing.T) {
	// m=80 (10 bytes), bits_len=20 (claims 20 bytes follow, inconsistent with m/8=10)
	data := make([]byte, 12)
	data[0] = 80 // m=80
	data[4] = 7  // k=7
	data[8] = 20 // bits_len=20
	data = append(data, make([]byte, 20)...)
	_, err := Deserialize(data)
	if err == nil {
		t.Fatalf("expected error on bits_len/m inconsistency, got nil")
	}
}

// ----------------------------------------------------------------------------
// SizeByte
// ----------------------------------------------------------------------------

func TestSizeByte_MatchesTableLength(t *testing.T) {
	b, _ := New(1000, 0.01)
	if b.SizeByte() != len(b.table) {
		t.Fatalf("SizeByte=%d but len(table)=%d", b.SizeByte(), len(b.table))
	}
	if b.SizeByte() != b.m/8 {
		t.Fatalf("SizeByte=%d but m/8=%d", b.SizeByte(), b.m/8)
	}
}

// ----------------------------------------------------------------------------
// Edge: empty string key
// ----------------------------------------------------------------------------

func TestAdd_EmptyStringKey(t *testing.T) {
	b, _ := New(100, 0.01)
	if err := b.Add(""); err != nil {
		t.Fatalf("Add(empty): %v", err)
	}
	ok, _ := b.MayContain("")
	if !ok {
		t.Fatalf("MayContain(empty) should be true after Add(empty)")
	}
}
