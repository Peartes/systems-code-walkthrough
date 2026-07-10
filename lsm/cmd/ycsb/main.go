// Small YCSB-B workload driver for the LSM key-value store.
//
// Not a full YCSB port — just enough to produce a realistic read-heavy load
// with Zipfian key skew, so we can capture p50/p99/p999 latency and
// throughput baselines from Prometheus + Grafana.
//
// Usage:
//
//	go run ./cmd/ycsb \
//	    -dir /tmp/lsm-ycsb-run \
//	    -records 10000 \
//	    -duration 10m \
//	    -threads 4 \
//	    -read-mix 0.95
//
// The LSM binds :9090/metrics; make sure Prometheus + Grafana are already
// running via `docker compose -f deploy/observability/docker-compose.yml up -d`
// so the run gets scraped.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/peartes/lsm/src/lsm"
)

func main() {
	cfg := parseFlags()

	// Open the LSM with metrics enabled so /metrics is served on :9090.
	// recordMetric=true is the whole point of this run.
	db, err := lsm.Open(
		cfg.Dir,
		42, // deterministic skip-list seed; keeps the shape of the tree reproducible across runs
		lsm.Options{FlushThreshold: cfg.FlushThreshold, MaxQueue: cfg.MaxQueue},
		true,
	)
	if err != nil {
		log.Fatalf("open LSM: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("close LSM: %v", err)
		}
	}()

	adapter := &LSMAdapter{db: db}

	// Load phase: insert `recordcount` records so the run phase has data to
	// read. Parallelized because a serial load of 100k records with fsync
	// on every append is painful.
	log.Printf("load phase: inserting %d records (%d bytes each, %d threads)...",
		cfg.RecordCount, cfg.ValueSize, cfg.Threads)
	loadStart := time.Now()
	if err := loadPhase(adapter, cfg); err != nil {
		log.Fatalf("load phase: %v", err)
	}
	log.Printf("load phase: done in %v (%.0f ops/s)",
		time.Since(loadStart),
		float64(cfg.RecordCount)/time.Since(loadStart).Seconds())

	// Give the background flush a moment to catch up so we're not just
	// measuring memtable-only reads. This isn't strictly required but the
	// baseline numbers are more representative once at least one SSTable
	// exists.
	log.Printf("warmup: sleeping %v so background flush can drain", cfg.WarmupSleep)
	time.Sleep(cfg.WarmupSleep)

	log.Printf("run phase: %v with read/update mix = %.2f/%.2f, %d threads",
		cfg.Duration, cfg.ReadMix, 1-cfg.ReadMix, cfg.Threads)
	report := runPhase(adapter, cfg)

	fmt.Fprintln(os.Stdout)
	report.Print(os.Stdout)
	fmt.Fprintln(os.Stdout)
	fmt.Fprintf(os.Stdout, "For p50/p99/p999 latency, check the Grafana dashboard at http://localhost:13000\n")
}

type Config struct {
	Dir            string
	RecordCount    int
	Duration       time.Duration
	Threads        int
	ValueSize      int
	ReadMix        float64
	ZipfianS       float64 // s parameter for math/rand.Zipf (>1; higher = more skew)
	FlushThreshold int64
	MaxQueue       int
	WarmupSleep    time.Duration
}

func parseFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.Dir, "dir", "/tmp/lsm-ycsb-run", "directory for the LSM data files")
	flag.IntVar(&cfg.RecordCount, "records", 10_000, "number of records to load and select from")
	flag.DurationVar(&cfg.Duration, "duration", 10*time.Minute, "run-phase duration")
	flag.IntVar(&cfg.Threads, "threads", 4, "concurrent worker goroutines")
	flag.IntVar(&cfg.ValueSize, "value-size", 100, "size of each value in bytes")
	flag.Float64Var(&cfg.ReadMix, "read-mix", 0.95, "fraction of run-phase ops that are reads (YCSB-B default: 0.95)")
	flag.Float64Var(&cfg.ZipfianS, "zipf-s", 1.01, "Zipfian s parameter for math/rand.Zipf (>1; higher = more skew)")
	flag.Int64Var(&cfg.FlushThreshold, "flush-threshold", 1<<20, "memtable flush threshold in bytes")
	flag.IntVar(&cfg.MaxQueue, "max-queue", 5, "max frozen memtables in the flush queue")
	flag.DurationVar(&cfg.WarmupSleep, "warmup", 5*time.Second, "sleep between load and run phases")
	flag.Parse()

	if cfg.ReadMix < 0 || cfg.ReadMix > 1 {
		log.Fatalf("-read-mix must be in [0, 1]")
	}
	if cfg.ZipfianS <= 1 {
		log.Fatalf("-zipf-s must be > 1 (math/rand.Zipf requirement)")
	}
	return cfg
}
