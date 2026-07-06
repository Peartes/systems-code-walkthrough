package sstable

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/peartes/lsm/src/bloom"
	"github.com/peartes/lsm/src/wal"
)

const magic uint32 = 0x53535442 // "SSTB" as little-endian bytes
// an SSTable is a sorted string data structure written into a file
//
// this is the file the lsm writes out data into once the memtable is large enough
// on the RAM that it becomes inefficient to keep it there
//
// once an sstable is written to storage, it becomes immutable
type Writer struct {
	f        *os.File
	blm      *bloom.Bloom
	minKey   string // minimum key in this sstable - this allows us to know if a key is contained in the table
	maxKey   string // maximum key
	count    int    // number of logs saved into this sstable
	finished bool   // is this table's trailer written ? true means sstable becomes immutable
}

// create a writer into an sstable
//
// this function creates a new sstable file and it's bloom filter
//
// path is the filepath to open
// expectedItems is the number of keys expected to be written into the sstable
// this allows us to be able to create a bloom filter that can adequately handle
// without degradation
//
// fpr is the expected false positive rate for the bloom filter
func NewWriter(path string, expectedItems int, fpr float64) (*Writer, error) {
	// open the file for writing
	fd, err := os.OpenFile(path, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOpenFile, err)
	}
	// create a bloom filter for this sstable
	blm, err := bloom.New(expectedItems, fpr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCreatingBloomFilter, err)
	}
	return &Writer{
		f:   fd,
		blm: blm,
	}, nil
}

// Add adds a log into an sstable
//
// # Note that only sorted keys should be added into the sstable
//
// The sstable stores record from the lsm i.e. key, value and tombstone
//
// # These values are serialized and stored similar to the WAL check wal for details
//
// After writing all logs, a trailer is added to the sstable which contains the sstable, min/max keys and a magic
func (st *Writer) Add(key string, value []byte, tombstone bool) error {
	if st.finished {
		return ErrAddingToFinishedSSTable
	}
	if st.count == 0 {
		st.minKey = key
	}
	// compare the key with the the last max key
	if st.count > 0 && key <= st.maxKey {
		panic(ErrNonSortedKey)
	}
	// serialize the data
	log := wal.Serialize(key, value, tombstone)
	// write this out into the file
	_, err := st.f.Write(log)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrWritingLogToSSTable, err)
	}
	// update the maxkey
	st.maxKey = key
	// update the record count
	st.count += 1
	// update the bloom filter
	if err := st.blm.Add(key); err != nil {
		return fmt.Errorf("%w: %w", ErrWritingLogToSSTable, err)
	}
	return nil
}

// Finish writes the trailer for this sstable and marks it immutable
//
// the trailer for an sstable contains the bloom filter, the min/max key and a magic
//
/*
[bloom_bytes_len:4][bloom_bytes:B]
[min_key_len:4][min_key:M1]
[max_key_len:4][max_key:M2]
[record_count:8]
[trailer_size:4]     ← total bytes from bloom_bytes_len through record_count
[magic:4]
*/
// It closes the file also
func (st *Writer) Finish() error {
	if st.finished {
		// we already write the trailer
		return nil
	}
	trailer := make([]byte, 0)

	// write the length and bytes of the bloom filter
	if st.blm.SizeByte() > math.MaxUint32 {
		panic("sstable: bloom filter is larger than 4bytes")
	}
	blmSz := st.blm.Serialize()
	trailer = binary.LittleEndian.AppendUint32(trailer, uint32(len(blmSz)))
	trailer = append(trailer, blmSz...)

	// write the min and max key
	if len(st.minKey) > math.MaxUint32 {
		panic("sstable: sstable min key is larger that 4byte")
	}
	trailer = binary.LittleEndian.AppendUint32(trailer, uint32(len(st.minKey)))
	trailer = append(trailer, []byte(st.minKey)...)
	if len(st.maxKey) > math.MaxUint32 {
		panic("sstable: sstable max key is larger that 4byte")
	}
	trailer = binary.LittleEndian.AppendUint32(trailer, uint32(len(st.maxKey)))
	trailer = append(trailer, []byte(st.maxKey)...)

	// write the record count
	trailer = binary.LittleEndian.AppendUint64(trailer, uint64(st.count))
	// and the triler size
	trailer = binary.LittleEndian.AppendUint32(trailer, uint32(len(trailer)))
	// finally the magic is a special byte that is written to every sstable to identify it as such
	trailer = binary.LittleEndian.AppendUint32(trailer, magic)
	// now write to file
	_, err := st.f.Write(trailer)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrWritingTrailer, err)
	}
	// sync file write so disk write happens immediately
	err = st.f.Sync()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSyncTrailerWrite, err)
	}
	// close the file
	err = st.f.Close()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrClosingSSTableFile, err)
	}
	// mark the sstable as immutable
	st.finished = true
	return nil
}
