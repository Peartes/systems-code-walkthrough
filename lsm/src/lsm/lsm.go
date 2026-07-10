package lsm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/peartes/lsm/src/memtable"
	"github.com/peartes/lsm/src/sstable"
	"github.com/peartes/lsm/src/telemetry"
	"github.com/peartes/lsm/src/wal"
	wl "github.com/peartes/lsm/src/wal"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"

	mt "github.com/peartes/lsm/src/memtable"

	sk "github.com/peartes/lsm/src/skiplist"
)

const scope = "github.com/peartes/lsm/"

type DB struct {
	dir            string
	flushThreshold int64

	mu       sync.Mutex
	active   *memtable.Memtable
	wal      *wal.WAL
	walSeqno uint64 // seqno of the currently-active WAL segment

	queue    *flushQueue
	ssts     []*sstable.Reader // sorted DESCENDING by seqno (index 0 = newest)
	manifest *Manifest

	flushWG sync.WaitGroup

	tele *LMetric
}

type Options struct {
	FlushThreshold int64 // default 1 << 20 (1 MB)
	MaxQueue       int   // default 3
}

type DBInfo struct {
	FlushThreshold int64
	MaxQueue       int64
	SkipList       *sk.Options
}

// Open creates a new db using WAL
//
// It creates a memtable, checks if there are any WAL files in the dir
// and loads them into the memtable propagating any warning on any io error and propagatin any other
func Open(dir string, seed int64, opt Options, recordMetric bool) (*DB, error) {
	// first let's make sure the dir exists
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, ErrLSMFileOpen
	}
	db := &DB{
		dir:  dir,
		ssts: []*sstable.Reader{},
	}
	// load the manifest file
	man, err := Load(dir)
	if err != nil {
		return nil, err
	}
	db.manifest = man
	db.flushThreshold = opt.FlushThreshold
	// load all WAL files and sstables
	walsSeqNo := []uint64{}
	sstsSeqNo := []uint64{}
	dirFiles, err := os.ReadDir(dir)
	for _, entry := range dirFiles {
		if entry.IsDir() {
			// skip directories
			continue
		}
		if ok, _ := filepath.Match("wal-*.log", entry.Name()); ok {
			walSeqNo, err := strconv.Atoi(strings.Split(strings.Split(entry.Name(), "-")[1], ".")[0])
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrLSMReadWals, err)
			}
			// any WAL file that has an sstable written out and in the manifest is an orphan WAL
			if slices.Contains(man.LiveSSTables, uint64(walSeqNo)) {
				// This WAL's data is already durably in <walSeqNo>.sst. Orphan.
				if err = os.Remove(filepath.Join(dir, entry.Name())); err != nil {
					fmt.Printf("lsm: cannot delete orphaned WAL file %s", entry.Name())
				}
				continue
			}
			walsSeqNo = append(walsSeqNo, uint64(walSeqNo))
		}
		if ok, _ := filepath.Match("*.sst", entry.Name()); ok {
			sstSeqNo, err := strconv.Atoi(strings.Split(entry.Name(), ".")[0])
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrLSMReadSST, err)
			}
			sstsSeqNo = append(sstsSeqNo, uint64(sstSeqNo))
		}
	}
	// remove all orphaned sstables
	// TODO: move this to a goroutine, need not be part of the init pipeline
	for _, seqNo := range sstsSeqNo {
		if slices.Contains(man.LiveSSTables, seqNo) {
			continue
		}
		// remove this file, it's an orphaned sstable
		if err = os.Remove(filepath.Join(dir, fmt.Sprintf("%d.sst", seqNo))); err != nil {
			fmt.Printf("warn: could not delete orphaned sstable file %d.sst", seqNo)
		}
	}
	// open all sstables but first sort the seqno in descending order
	// also we only want to open the live sstables so we copy the live sstables
	liveSSTables := make([]uint64, len(man.LiveSSTables))
	copy(liveSSTables, man.LiveSSTables)
	slices.SortFunc(liveSSTables, func(a, b uint64) int {
		return cmp.Compare(b, a)
	})
	for _, sst := range liveSSTables {
		sr, err := sstable.OpenReader(filepath.Join(dir, fmt.Sprintf("%d.sst", sst)))
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrLSMFileOpen, err)
		}
		db.ssts = append(db.ssts, sr)
	}
	// create a flushqueue
	db.queue = newFlushQueue(opt.MaxQueue)
	// now read each WAL file into memtables
	slices.Sort(walsSeqNo)
	// for now we assume we cannot have more than MAXQUEUE wal files
	// let's create a rand generator for the memtable skiplist
	rnd := rand.New(rand.NewSource(seed))
	for _, walSeqNo := range walsSeqNo {
		// create a new memtable for this wal
		// TODO: move WALs to optionally store the details of the skiptable in their trailer
		sl := sk.NewSkipList(16, 0.5, rnd)
		memTable := mt.NewMemtable(&sl)

		// open the WAL for reading if it exists
		walPath := filepath.Join(dir, fmt.Sprintf("wal-%d.log", walSeqNo))
		wal, err := wl.Open(walPath)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrLSMFileOpen, err)
		}

		// now let's read the content of the wal into the memtable
		err = wal.Replay(func(key string, value []byte, tombstone bool) {
			if tombstone {
				// this value has been deleted, append into memtable using the delete method
				memTable.Delete(key)
			} else {
				memTable.Put(key, value)
			}

		})
		if err != nil {
			if errors.Is(err, wl.ErrTruncatedFile) || errors.Is(err, wl.ErrWALCRCCheckFail) {
				// there seem to be some errors replayig some part of the log
				// to avoid inconsistent records, we send back an erorr and have
				// the caller decide to what to do
				return nil, fmt.Errorf("%w: %w", ErrLSMWALError, err)
			} else {
				// some other error has occured
				// let's surface it
				return nil, err
			}
		}
		// freeze this memtable
		memTable.Freeze()
		// write this out into the flush queue
		if err = db.queue.push(&flushEntry{
			memtable: memTable,
			walPath:  walPath,
			seqno:    walSeqNo,
		}); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrLSM, err)
		}
		// close this WAL file
		if err := wal.Close(); err != nil {
			fmt.Printf("warn: could not close opened wal file wal-%d.log", walSeqNo)
		}
	}
	// update the manifest seqno in case it's behind max(sstSeqNo) or max(walSeqNo)
	if len(walsSeqNo) > 0 && walsSeqNo[len(walsSeqNo)-1]+1 > man.NextSeqNo {
		man.NextSeqNo = slices.Max(walsSeqNo) + 1
	}
	// alocate a new sequence number for this sessions memtable
	walSeqNo := man.AllocSeqno()
	// save the manifest file
	if err = man.Save(); err != nil {
		fmt.Printf("warn: could not save manifest file %s", MANIFEST)
	}
	// now create a new mematable and WAL file for this session
	// create a new memtable for this wal
	// TODO: move WALs to optionally store the details of the skiptable in their trailer
	sl := sk.NewSkipList(16, 0.5, rnd)
	memTable := mt.NewMemtable(&sl)

	// open the WAL for writing
	walPath := filepath.Join(dir, fmt.Sprintf("wal-%d.log", walSeqNo))
	wal, err := wl.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrLSMFileOpen, err)
	}
	db.active = memTable
	db.wal = wal
	db.walSeqno = walSeqNo
	// create tele instruments to record all operations
	dbInfo := db.GetInfo()
	teleProvider, err := telemetry.NewMeterProvider(attribute.KeyValue{Key: "name", Value: attribute.StringValue("swiftDb")}, attribute.KeyValue{Key: "mt_ds", Value: attribute.StringValue("skiplist")}, attribute.KeyValue{Key: "sl_height", Value: attribute.Int64Value(int64(dbInfo.SkipList.MaxHeight))}, attribute.KeyValue{Key: "sl_prob", Value: attribute.Int64Value(int64(dbInfo.SkipList.Prob))})
	if err != nil {
		fmt.Printf("lsm: cannot creating otel meteric provider")
	}
	db.tele, err = NewLMetric(teleProvider, scope)
	if err != nil {
		fmt.Printf("lsm: cannot creating otel meteric provider")
	}
	if recordMetric {
		http.Handle("/metrics", promhttp.HandlerFor(prom.DefaultGatherer, promhttp.HandlerOpts{}))
		go http.ListenAndServe(":9090", nil)
	}
	db.flushWG.Add(1)
	go db.flushLoop()
	return db, nil
}

// flushLoop writes out the memtables in flush queue into sstables
func (db *DB) flushLoop() {
	defer db.flushWG.Done()
	for {
		entry := db.queue.waitForHead()
		if entry == nil {
			return // queue closed
		}
		for {
			reader, err := db.flushOneP1(entry)
			if err == nil {
				for {
					if err = db.flushOneP2(entry.seqno, reader, entry.walPath); err == nil {
						break
					}
					// Retry with backoff.
					time.Sleep(500 * time.Millisecond)
				}
				break
			} else {
				// Retry with backoff.
				time.Sleep(500 * time.Millisecond)
			}
		}
		// delete the WAL file; best effort try
		// if it fails it doesn't break anything
		if err := os.Remove(entry.walPath); err != nil {
			fmt.Printf("%s: %s", ErrLSMWALError, err)
		}

	}
}

// flsuhOneP1 is the first phase of writing out a memtable to sstable
//
// the first phase writes the memtable into sstable durably then opens a reader for the sstable
//
// this assures that the sstable is commited and that the flush loop does not have to re-write this to disk
func (db *DB) flushOneP1(entry *flushEntry) (*sstable.Reader, error) {
	// open the sstable writer
	sstPath := filepath.Join(db.dir, fmt.Sprintf("%d.sst", entry.seqno))
	writer, err := sstable.NewWriter(sstPath, entry.memtable.Len(), 0.01)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrLSMWriteSSTable, err)
	}
	if ok := entry.memtable.Iterate(func(key string, value []byte, tombstone bool) bool {
		// write out to the sstable
		if err = writer.Add(key, value, tombstone); err != nil {
			return false
		}
		return true
	}); !ok {
		return nil, ErrLSMWriteSSTable
	}
	// write out to disk
	if err = writer.Finish(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrManifestFileSave, err)
	}
	// open the sstable for reading so we can use it in the session
	reader, err := sstable.OpenReader(sstPath)
	if err != nil {
		// if we cannot open a reader into this sstable, we cannot remove it from memory
		return nil, fmt.Errorf("%w: %w", ErrLSMFileOpen, err)
	}
	return reader, nil
}

// flushOneP2 is the second phase of writing out a memtable into an sstable
//
// this is the cleanup phase that makes sure the written sstable is saved into the manifest,
// the WAL deleted and the db now uses the sstables
//
// seqno is the sequence number of the last sstable that's been written out
func (db *DB) flushOneP2(seqNo uint64, reader *sstable.Reader, walPath string) error {
	// update the manifest
	db.manifest.AddSSTable(seqNo)
	if err := db.manifest.Save(); err != nil {
		// we error on this even though it does not affect the running process
		// but if we carry out writing out the next WAL, this might break logic in Get operations
		// where we write out WAL-2 but not WAL-1 and on next db init, we will read WAL-1 first
		// before reading SSTable-2 which inverse the flow
		return fmt.Errorf("%w: %w", ErrManifestFileSave, err)
	}

	// now update the ssts in the db
	db.mu.Lock()
	// we need to prepend this sst to the list of ssts
	db.ssts = append([]*sstable.Reader{reader}, db.ssts...)
	// now pop the flush queue
	db.queue.popHead()
	db.mu.Unlock()
	return nil
}

// Put inserts a key into the LSM
//
// first we write to the WAL, upon success we write that into the memtable
//
// if the put takes the memtable above threshold then we need to write it out into an sstable
func (db *DB) Put(key string, value []byte) error {
	db.mu.Lock()
	// record tele tp
	start := time.Now()
	defer func() {
		elapsedMs := float64(time.Since(start).Microseconds()) / 1000.0
		db.tele.writeL.Record(context.Background(), elapsedMs)
		db.tele.writeTP.Add(context.Background(), 1)
	}()
	// check the memtable size
	if db.active.SizeBytes() >= int(db.flushThreshold) {
		if err := db.rolloverLocked(); err != nil {
			return err
		}
	}
	// write to WAL
	err := db.wal.Append(key, value, false)
	if err != nil {
		db.mu.Unlock()
		return fmt.Errorf("%w: %w", ErrLSMAppend, err)
	}
	// now write to memtable
	db.active.Put(key, value)
	db.mu.Unlock()
	return nil
}

// rolloverLocked locks a memtable and pushes it into the flushqueue for writing out to an sstable
//
// Note: call this when you have a lock on the db mutex
func (db *DB) rolloverLocked() error {
	// 1. Close the current active WAL cleanly (no fsync needed — Appends already fsync).
	// 2. Build a flushEntry with the active memtable, current WAL path, current walSeqno.
	// 3. Freeze the active memtable.
	// 4. Release db.mu, call db.queue.push(entry), reacquire db.mu.
	//    (push may block; must not hold db.mu while blocked.)
	// 5. Allocate a fresh seqno via db.manifest.AllocSeqno(). Save manifest.
	// 6. Open a new WAL at walPath(db.dir, newSeqno).
	// 7. Create a new empty active memtable.
	// 8. Update db.wal, db.walSeqno, db.active.
	if err := db.wal.Close(); err != nil {
		fmt.Printf("%s: %s", ErrLSMFileClose, err)
	}
	// freeze the memtable
	db.active.Freeze()
	// build the flush entry
	entry := &flushEntry{
		memtable: db.active,
		walPath:  filepath.Join(db.dir, fmt.Sprintf("wal-%d.log", db.walSeqno)),
		seqno:    db.walSeqno,
	}

	// allocate a fresh seqno
	seqno := db.manifest.AllocSeqno()
	if err := db.manifest.Save(); err != nil {
		fmt.Printf("%s: %s", ErrLSM, err)
	}
	// open a new WAL
	wal, err := wal.Open(filepath.Join(db.dir, fmt.Sprintf("wal-%d.log", seqno)))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLSMFileOpen, err)
	}
	// create a new memtable
	slOpts, ok := db.active.GetDSParams().(*sk.Options)
	if !ok {
		return fmt.Errorf("lsm: malformed internal memtable ds structure ")
	}
	sl := sk.NewSkipList(slOpts.MaxHeight, slOpts.Prob, slOpts.Rand)
	memTable := mt.NewMemtable(&sl)
	db.wal = wal
	db.active = memTable
	db.walSeqno = seqno

	db.mu.Unlock()
	err = db.queue.push(entry)
	if err != nil {
		// queue was closed while we were pushing; propagate
		return err
	}
	db.mu.Lock()
	return nil
}

// Delete marks a log as removed/tombstoned
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	// record tele tp
	start := time.Now()
	defer func() {
		elapsedMs := float64(time.Since(start).Microseconds()) / 1000.0
		db.tele.deleteL.Record(context.Background(), elapsedMs)
		db.tele.deleteTP.Add(context.Background(), 1)
	}()
	// check the memtable size
	if db.active.SizeBytes() >= int(db.flushThreshold) {
		if err := db.rolloverLocked(); err != nil {
			return err
		}
	}
	err := db.wal.Append(key, nil, true)
	if err != nil {
		db.mu.Unlock()
		return fmt.Errorf("%w", ErrLSMDelete)
	}

	db.active.Delete(key)
	db.mu.Unlock()
	return nil
}

// Get finds a key in the LSM tree
// starts by querying the memtable, and the sstables if not
// found in the memtable
func (db *DB) Get(key string) (value []byte, found bool, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// record tele tp
	start := time.Now()
	defer func() {
		elapsedMs := float64(time.Since(start).Microseconds()) / 1000.0
		db.tele.readL.Record(context.Background(), elapsedMs)
		db.tele.readTP.Add(context.Background(), 1)
	}()
	// first we check the memtable for the key
	if v, ts, found := db.active.Get(key); found {
		if ts {
			return nil, false, nil
		}
		return v, true, nil
	} else {
		// we search the queued memtables first
		for _, entry := range db.queue.snapshot() {
			if v, ts, found := entry.memtable.Get(key); found {
				if ts {
					return nil, false, nil
				}
				return v, true, nil
			}
		}
		// now we search the sstables from newest to oldest
		for _, sstable := range db.ssts {
			v, ts, found, err := sstable.Get(key)
			if found {
				if ts {
					return nil, false, err
				}
				return v, found, err
			} else if err != nil {
				return v, found, err
			}
		}
	}
	return
}

// return attributes of this lsm tree
func (db *DB) GetInfo() *DBInfo {
	return &DBInfo{
		FlushThreshold: db.flushThreshold,
		MaxQueue:       int64(db.queue.maxLen),
		SkipList:       (db.active.GetDSParams()).(*sk.Options),
	}
}

// Close function closes connection to WAL file
func (db *DB) Close() error {
	db.mu.Lock()
	// 1. Close the active WAL.
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrLSMFileClose, err)
	}
	db.mu.Unlock()

	// 2. Signal the flush goroutine to exit.
	db.queue.close()

	// 3. Wait for it to finish any in-flight flush.
	db.flushWG.Wait()

	// 4. Close all SSTable readers.
	db.mu.Lock()
	for _, r := range db.ssts {
		r.Close()
	}
	db.mu.Unlock()

	return nil
}
