package wal

import "errors"

var (
	ErrTruncatedFile   = errors.New("wal: cannot read log data, file is probably truncated")
	ErrWALOpen         = errors.New("wal: could not open WAL file")
	ErrWALRead         = errors.New("wal: error reading WAL file")
	ErrWALCRCCheckFail = errors.New("wal: crc check failed")
	ErrWALClose        = errors.New("wal: error closing WAL file")
)
