package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// keyFor produces a stable, sortable string key for record index i.
// The zero-padded numeric form ensures lexicographic order matches numeric
// order (useful for the LSM's Bloom + range filters).
func keyFor(i uint64) string {
	return fmt.Sprintf("user%010d", i)
}

// randValue fills a byte slice with random data. We use crypto/rand so the
// bytes are unpredictable (no accidental compressibility) and so the load
// stresses the LSM's Bloom filter with a realistic key distribution.
func randValue(buf []byte) {
	if _, err := rand.Read(buf); err != nil {
		panic("crypto/rand failed: " + err.Error()) // unreachable in practice
	}
}

// loadPhase inserts cfg.RecordCount records with fresh random values. Runs
// cfg.Threads workers in parallel to speed things up — a serial load of
// 100k records with fsync-on-every-Put would take several minutes.
func loadPhase(a *LSMAdapter, cfg Config) error {
	tasks := make(chan uint64, cfg.Threads*4)
	var wg sync.WaitGroup
	var loadErr atomic.Value // holds the first error, if any

	for w := 0; w < cfg.Threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, cfg.ValueSize)
			for i := range tasks {
				randValue(buf)
				if err := a.Load(keyFor(i), buf); err != nil {
					loadErr.CompareAndSwap(nil, err)
					return
				}
			}
		}()
	}

	for i := uint64(0); i < uint64(cfg.RecordCount); i++ {
		tasks <- i
	}
	close(tasks)
	wg.Wait()

	if err, ok := loadErr.Load().(error); ok {
		return fmt.Errorf("load worker failed: %w", err)
	}
	return nil
}

// runPhase drives the YCSB-B read/update mix against the LSM for cfg.Duration.
// Every worker independently:
//   - picks the next op by flipping a coin against cfg.ReadMix
//   - samples the target record with a Zipfian (hot-key) distribution
//   - executes and counts the op
//
// Zipfian details: math/rand's Zipf gives P(k) ∝ (v+k)^(-s). With s=1.01,
// v=1, imax=recordCount-1 that produces heavy-tail skew similar to
// YCSB's default zipfconstant=0.99 — key 0 gets thousands of times more
// hits than key (recordCount-1). Realistic for cache-like workloads.
func runPhase(a *LSMAdapter, cfg Config) Report {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var (
		readOps    atomic.Int64
		updateOps  atomic.Int64
		readErrs   atomic.Int64
		updateErrs atomic.Int64
	)

	var wg sync.WaitGroup
	for w := 0; w < cfg.Threads; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker has its own RNG so we don't contend on a shared
			// math/rand.Source mutex. Seed with workerID + wall time so
			// runs aren't bit-identical.
			seed := int64(workerID)*1_000_003 + time.Now().UnixNano()
			rng := mathrand.New(mathrand.NewSource(seed))
			zipf := mathrand.NewZipf(rng, cfg.ZipfianS, 1.0, uint64(cfg.RecordCount-1))
			valueBuf := make([]byte, cfg.ValueSize)

			for {
				if ctx.Err() != nil {
					return
				}
				key := keyFor(zipf.Uint64())
				if rng.Float64() < cfg.ReadMix {
					if _, err := a.Read(key); err != nil {
						readErrs.Add(1)
					} else {
						readOps.Add(1)
					}
				} else {
					randValueFromRNG(rng, valueBuf)
					if err := a.Update(key, valueBuf); err != nil {
						updateErrs.Add(1)
					} else {
						updateOps.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	return Report{
		Duration:   cfg.Duration,
		ReadOps:    readOps.Load(),
		UpdateOps:  updateOps.Load(),
		ReadErrs:   readErrs.Load(),
		UpdateErrs: updateErrs.Load(),
	}
}

// randValueFromRNG fills buf using a math/rand source. We reuse the worker's
// existing RNG here (no per-op crypto/rand syscall in the hot path).
func randValueFromRNG(rng *mathrand.Rand, buf []byte) {
	// rng.Read never returns error and always fills the whole buffer.
	_, _ = rng.Read(buf)
}

type Report struct {
	Duration   time.Duration
	ReadOps    int64
	UpdateOps  int64
	ReadErrs   int64
	UpdateErrs int64
}

func (r Report) Print(w io.Writer) {
	total := r.ReadOps + r.UpdateOps
	sec := r.Duration.Seconds()
	fmt.Fprintln(w, "===== run-phase report =====")
	fmt.Fprintf(w, "duration:            %v\n", r.Duration)
	fmt.Fprintf(w, "read ops:            %-10d  (%.0f/s)\n", r.ReadOps, float64(r.ReadOps)/sec)
	fmt.Fprintf(w, "update ops:          %-10d  (%.0f/s)\n", r.UpdateOps, float64(r.UpdateOps)/sec)
	fmt.Fprintf(w, "total ops:           %-10d  (%.0f/s)\n", total, float64(total)/sec)
	if r.ReadErrs > 0 || r.UpdateErrs > 0 {
		fmt.Fprintf(w, "read errors:         %d\n", r.ReadErrs)
		fmt.Fprintf(w, "update errors:       %d\n", r.UpdateErrs)
	}
	fmt.Fprintln(w, "============================")
}
