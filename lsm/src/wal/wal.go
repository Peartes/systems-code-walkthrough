// / Write Ahead Log
// / this package describes a write ahead log which is simply a file that is only appended to and not overwritten
// / this file serves as the basis for constructing our memtable
// / why a WAL ? it is disk write efficient
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type WAL struct {
	f *os.File // the write ahead log descriptor
}

// open opens a write ahead log for appending and reading
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, ErrWALOpen
	}
	return &WAL{
		f,
	}, nil
}

// Append writes a log to the end of the WAL
// the structure of each log is
// total_lenght(4 byte) - crc (4 byte) - tombstone (1 byte) - key_len(4byte) - key (N) - value_len (4 byte) - valur (N)
// the crc is the cyclic redundancy check to confirm that the log was properly appended during a log update
func (w *WAL) Append(key string, value []byte, tombstone bool) error {
	log := make([]byte, 0)
	// let's build the payload
	payload := make([]byte, 0)
	// first the tombstone
	if tombstone {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	// now the key_length
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(key)))
	// and then the key itself
	payload = append(payload, []byte(key)...)
	// add the value length
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(value)))
	// and the value
	payload = append(payload, value...)
	// now let's get the crc for the payload
	crc := crc32.ChecksumIEEE(payload)
	// let's get the total legth
	totalLength := len(payload) + 4 // 4 byte for the crc
	// now we write the full log
	log = binary.LittleEndian.AppendUint32(log, uint32(totalLength))
	log = binary.LittleEndian.AppendUint32(log, uint32(crc))
	log = append(log, payload...)

	// now we write the log to the WAL
	n, err := w.f.Write(log)
	if err != nil || n != len(log) {
		return err
	}
	// now we have to call fsync to write the log immediately
	err = w.f.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Iterate builds a memtable from a WAL. It does this by reading each log in the file and
// applying the function on the data
// we assume the file has been opened for reading
func (w *WAL) Replay(fn func(key string, value []byte, tombstone bool)) error {
	// set the seek to the begining of the file
	_, err := w.f.Seek(0, io.SeekStart)
	if err != nil {
		return ErrWALOpen
	}
	// the algorithm is simple;
	// first, read the first 4 bytes which is the length of a chunk of the whole log
	// then read the whole chunk
	// read the crc from the chunk
	// calculate the crc on the rest which is the payload
	// compare and make sure the crc matches
	// then read each of the payload at a time decoding them
	// call the function after all parameters are extracted
	for {
		totalLength := make([]byte, 4)

		r, err := io.ReadFull(w.f, totalLength)
		if err == io.EOF && r == 0 {
			return nil // clean EOF — no more records
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return ErrTruncatedFile // partial length prefix
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrWALRead, err)
		}
		// read the total chunk of a log
		log := make([]byte, binary.LittleEndian.Uint32(totalLength))
		r, err = io.ReadFull(w.f, log)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return ErrTruncatedFile
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrWALRead, err)
		}
		// read the 4byte crc
		crc := binary.LittleEndian.Uint32(log)
		// compute the crc of the payload
		crcCheck := crc32.ChecksumIEEE(log[4:])
		// make sure they match - this confirms the payload hasn't been corrupted
		if crc != crcCheck {
			return ErrWALCRCCheckFail
		}
		// read the tombstone
		tomb := log[4:5] // just one byte
		var tombStone bool
		if tomb[0] == 0 {
			tombStone = false
		} else {
			tombStone = true
		}
		// read the key length
		keyLength := binary.LittleEndian.Uint32(log[5:])
		// read the key
		key := string(log[9 : 9+keyLength])
		// read the length of the value
		// valLength := binary.LittleEndian.Uint32(log[9 + keyLength:])
		// read the value - we don't really need the value length because everything else is the value
		val := log[9+keyLength+4:]

		// now let's call the function
		fn(key, val, tombStone)

	}
}

// Close closes the WAL file
func (w *WAL) Close() error {
	if w.f.Close() != nil {
		return ErrWALClose
	}
	return nil
}
