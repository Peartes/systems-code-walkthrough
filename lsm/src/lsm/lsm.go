package lsm

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	wl "github.com/peartes/lsm/src/wal"

	mt "github.com/peartes/lsm/src/memtable"

	sk "github.com/peartes/lsm/src/skiplist"
)

type DB struct {
	wal *wl.WAL
	mt  *mt.Memtable
	dir string
}

// Open creates a new db using WAL
// It creates a memtable, checks if there are any WAL files in the dir
// and loads them into the memtable propagating any warning on any io error and propagatin any other
func Open(dir string, seed int64) (*DB, error) {
	// first let's make sure the dir exists
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, ErrLSMFileOpen
	}
	// now create a new memtable
	rnd := rand.New(rand.NewSource(seed))
	sl := sk.NewSkipList(16, 0.5, rnd)
	memTable := mt.NewMemtable(&sl)

	// open the WAL for reading if it exists
	wal, err := wl.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrLSMFileOpen, err)
	}

	// now let's read the content of the wal into memtables
	err = wal.Replay(func(key string, value []byte, tombstone bool) {
		if tombstone {
			// this value has been deleted, append into memtable using the delete method
			memTable.Delete(key)
		} else {
			memTable.Put(key, value)
		}

	})
	DB := &DB{
		wal: wal,
		dir: dir,
		mt:  memTable,
	}
	if err != nil {
		if errors.Is(err, wl.ErrTruncatedFile) || errors.Is(err, wl.ErrWALCRCCheckFail) {
			// there seem to be some errors replayig some part of the log
			// ignore them and warn
			return DB, fmt.Errorf("%w: %w", ErrLSMWALError, err)
		} else {
			// some other error has occured
			// let's surface it
			return nil, err
		}
	} else {
		return DB, nil
	}

}

// Put inserts a key into the LSM
// first we write to the WAL, upon success we write that into the memtable
func (db *DB) Put(key string, value []byte) error {
	// write to WAL
	err := db.wal.Append(key, value, false)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLSMAppend, err)
	}
	// now write to memtable
	db.mt.Put(key, value)
	return nil
}

// Delete marks a log as removed/tombstoned
func (db *DB) Delete(key string) error {
	err := db.wal.Append(key, nil, true)
	if err != nil {
		return fmt.Errorf("%w", ErrLSMDelete)
	}

	db.mt.Delete(key)
	return nil
}

// Get finds a key in the LSM tree
func (db *DB) Get(key string) (value []byte, found bool, err error) {
	// first we check the memtable for the key
	err = nil
	value, ts, found := db.mt.Get(key)
	if found {
		// we have the key in memtable
		// if it's tombstoned, we returned not found
		if ts {
			return nil, false, nil
		} else {
			return value, true, nil
		}
	}
	return
}

// Close function closes connection to WAL file
func (db *DB) Close() error {
	err := db.wal.Close()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLSMFileClose, err)
	}
	return nil
}
