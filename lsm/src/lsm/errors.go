package lsm

import "errors"

var (
	ErrLSMFileOpen         = errors.New("lsm: cannot open file")
	ErrLSM                 = errors.New("lsm: error in lsm")
	ErrLSMWALError         = errors.New("lsm: error in WAL")
	ErrLSMDelete           = errors.New("lsm: could not delete key")
	ErrLSMAppend           = errors.New("lsm: could not append new key")
	ErrLSMFileClose        = errors.New("lsm: cannot close file")
	ErrManifestFileCorrupt = errors.New("lsm: manifest file corrupted")
	ErrManifestFileSave    = errors.New("lsm: could not save manifest file")
	ErrQueueClosed         = errors.New("queue: queue closed with active listeners")
	ErrLSMReadWals         = errors.New("lsm: error reading wal files in directory")
	ErrLSMReadSST          = errors.New("lsm: error reading sstables files in directory")
)
