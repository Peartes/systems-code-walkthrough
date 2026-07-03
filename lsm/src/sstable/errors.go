package sstable

import "errors"

var (
	ErrOpenFile                = errors.New("sstable: could not open the sstable file")
	ErrReadingFile             = errors.New("sstable: error reading from sstable")
	ErrCreatingBloomFilter     = errors.New("sstable: error creating bloom filter")
	ErrNonSortedKey            = errors.New("sstable: tried to insert keys in unsorted order into this sstable")
	ErrWritingLogToSSTable     = errors.New("sstable: error writing log into sstable")
	ErrWritingTrailer          = errors.New("sstable: error writing sstable trailer")
	ErrSyncTrailerWrite        = errors.New("sstable: error writing sstable trailer to disk")
	ErrClosingSSTableFile      = errors.New("sstable: error closing file")
	ErrAddingToFinishedSSTable = errors.New("sstable: attempt to add to a finshed sstable")
	ErrMalformedTrailer        = errors.New("sstable: trailer is malformed or truncated")
	ErrMagicMismatch           = errors.New("sstable: magic bytes do not match SSTB — not a valid sstable")
	ErrClosedReader            = errors.New("sstable: reader is closed")
	ErrCorruptRecord           = errors.New("sstable: CRC mismatch on record during scan")
)
