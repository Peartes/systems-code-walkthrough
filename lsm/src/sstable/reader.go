package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/peartes/lsm/src/bloom"
	"github.com/peartes/lsm/src/wal"
)

// Reader holds and read from an sstable
// it loads an sstable file, reads the trailer and can search through the file
type Reader struct {
	f           *os.File
	blm         *bloom.Bloom
	minKey      string
	maxKey      string
	recordCount uint64
	logSize     int64 // the size of the logs minus the trailer
	closed      bool
}

// OpenReader takes a path to an sstable file and opens it for reading
// reads the trailer for the file to recreate the bloom filter and get some other metadata
func OpenReader(path string) (*Reader, error) {
	// openn the file for reading
	fd, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOpenFile, err)
	}
	// now we read the trailer by seeking to the end of the file
	// reading the last 8 byte which corresponds to the trailer size and magic
	// we confirm the magic matches and then seek to the begining of the trailer
	_, err = fd.Seek(-8, io.SeekEnd) // position pointer to 8 bytes before file end
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReadingFile, err)
	}
	// read the trailer size and magic
	tMetadata := make([]byte, 8)
	_, err = io.ReadFull(fd, tMetadata)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformedTrailer, err)
	}
	// check magic matches
	if binary.LittleEndian.Uint32(tMetadata[4:]) != magic {
		return nil, ErrMagicMismatch
	}
	// now seek to the begining of trailer
	trailerSize := int64(binary.LittleEndian.Uint32(tMetadata))
	logSize, err := fd.Seek(-(trailerSize + 8), io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReadingFile, err)
	}

	// read the trailer
	trailerPos := 0
	trailer := make([]byte, trailerSize)
	_, err = io.ReadFull(fd, trailer)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformedTrailer, err)
	}
	// read the bloom bytes and create the bloom filter
	bloomLen := int(binary.LittleEndian.Uint32(trailer))
	trailerPos += 4
	bloomBz := trailer[trailerPos : trailerPos+bloomLen]
	trailerPos += bloomLen
	blmFilter, err := bloom.Deserialize(bloomBz)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformedTrailer, err)
	}

	// read the min key
	minKeyLen := int(binary.LittleEndian.Uint32(trailer[trailerPos : trailerPos+4]))
	trailerPos += 4
	minKey := trailer[trailerPos : trailerPos+minKeyLen]
	trailerPos += minKeyLen
	// read the max key
	maxKeyLen := int(binary.LittleEndian.Uint32(trailer[trailerPos : trailerPos+4]))
	trailerPos += 4
	maxKey := trailer[trailerPos : trailerPos+maxKeyLen]
	trailerPos += maxKeyLen
	// and the record count
	recordCount := binary.LittleEndian.Uint64(trailer[trailerPos : trailerPos+8])

	return &Reader{
		f:           fd,
		blm:         blmFilter,
		minKey:      string(minKey),
		maxKey:      string(maxKey),
		recordCount: recordCount,
		logSize:     logSize,
	}, nil
}

// MayContain is the bloom filter check. It checks to see if a key might be in an sstable
// by checking the bloom filter
func (r *Reader) MayContain(key string) (bool, error) {
	return r.blm.MayContain(key)
}

// Get searches an sstable to retreive a key
//
// # Make sure the sstable is opened before searching so avoid a panic
//
// Before a linear scan on the sstable, we first perform a range check then a bloom filter check
func (r *Reader) Get(key string) (value []byte, tombstone bool, found bool, err error) {
	if r.closed {
		return nil, false, false, ErrClosedReader
	}
	// perform a range check
	if key < r.minKey || key > r.maxKey {
		// the record is not in this sstable
		return nil, false, false, nil
	}
	// now check the bloom filter
	mayContain, err := r.MayContain(key)
	if err != nil {
		return nil, false, false, err
	}
	if !mayContain {
		// the key is not in this sstable
		return nil, false, false, nil
	}
	// the key might be in the record so we perform a linear search
	// seek to the beginnig of the file
	_, err = r.f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, false, false, fmt.Errorf("%w: %w", ErrReadingFile, err)
	}
	// now wrap our reader in a limit reader to avoid reading past the logs
	lReader := io.LimitReader(r.f, r.logSize)
	// TODO: implement a sparse index on the records
	for {
		k, val, tombstone, err := wal.ReadLog(lReader)
		if errors.Is(err, io.EOF) {
			return nil, false, false, nil // end of file, record not found
		}
		if errors.Is(err, wal.ErrWALCRCCheckFail) {
			return nil, false, false, ErrCorruptRecord
		}
		if err != nil {
			return nil, false, false, fmt.Errorf("%w: %w", ErrReadingFile, err) // the wal error might throw debugging off
		}
		if k == key {
			return val, tombstone, true, nil
		}
		// check if the key is still less than the current key
		if k > key {
			// the key cannot be in this sstable
			return nil, false, false, nil
		}
	}
}

func (r *Reader) MinKey() string      { return r.minKey }
func (r *Reader) MaxKey() string      { return r.maxKey }
func (r *Reader) RecordCount() uint64 { return r.recordCount }

// Idempotent; closes the file handle.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if err := r.f.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrClosingSSTableFile, err)
	}
	return nil
}
