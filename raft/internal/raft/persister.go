package raft

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// store updates the in-memory copy and, if file-backed, the on-disk copy.
func (p *Persister) store(b *[]byte) {
	p.data = *b
	if p.dataPath != "" {
		_ = atomicWriteFile(p.dataPath, *b)
	}
}

// load returns the in-memory copy. NewFilePersister is responsible for
// hydrating data from disk on cold start, so steady-state reads stay cheap.
func (p *Persister) load() []byte {
	return p.data
}

func (p *Persister) saveSnapshot(ss *[]byte, offset int, done bool, lastIncludedIndex int, lastIncludedTerm int) {
	if offset == 0 {
		// this is a new snapshot
		p.snapShotIntermediate = Snapshot{
			lastIncludedIndex: lastIncludedIndex,
			lastIncludedTerm:  lastIncludedTerm,
			data:              make([]byte, 0, len(*ss)),
		}
	}
	if done == false {
		// this is an ongoing snapshot stream
		// we append to the intermediate snapshot state
		// we assume that the logs are sent in order
		p.snapShotIntermediate.data = append(p.snapShotIntermediate.data, *(ss)...)
	} else {
		// this snapshot is done
		p.snapShotIntermediate.data = append(p.snapShotIntermediate.data, *(ss)...)
		p.snapShot = p.snapShotIntermediate
		p.snapShotIntermediate.data = make([]byte, 0)
		p.snapShotIntermediate.lastIncludedIndex = 0
		p.snapShotIntermediate.lastIncludedTerm = 0
		if p.snapPath != "" {
			_ = atomicWriteFile(p.snapPath, encodeSnapshot(p.snapShot))
		}
	}
}

// NewFilePersister returns a Persister that mirrors all writes to dataPath
// (Raft log/term/votedFor) and snapPath (snapshot). On construction it loads
// any existing on-disk state into memory so the engine sees the same shape
// it would after an in-memory restart.
//
// If either path is missing it is created on first write. Atomic writes via
// write-tmp-then-rename guard against partial writes from a crash mid-store.
func NewFilePersister(dataPath, snapPath string) (*Persister, error) {
	p := &Persister{dataPath: dataPath, snapPath: snapPath}

	if data, err := os.ReadFile(dataPath); err == nil {
		p.data = data
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if snapBytes, err := os.ReadFile(snapPath); err == nil {
		snap, derr := decodeSnapshot(snapBytes)
		if derr != nil {
			return nil, derr
		}
		p.snapShot = snap
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	return p, nil
}

// snapshot wire format on disk:
//
//	[8 bytes BE: lastIncludedIndex]
//	[8 bytes BE: lastIncludedTerm]
//	[remaining bytes: snapshot payload]
//
// We need our own framing because Snapshot's fields are unexported and we
// don't want gob in this hot path.
func encodeSnapshot(s Snapshot) []byte {
	out := make([]byte, 16+len(s.data))
	binary.BigEndian.PutUint64(out[0:8], uint64(s.lastIncludedIndex))
	binary.BigEndian.PutUint64(out[8:16], uint64(s.lastIncludedTerm))
	copy(out[16:], s.data)
	return out
}

func decodeSnapshot(b []byte) (Snapshot, error) {
	if len(b) < 16 {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	return Snapshot{
		lastIncludedIndex: int(binary.BigEndian.Uint64(b[0:8])),
		lastIncludedTerm:  int(binary.BigEndian.Uint64(b[8:16])),
		data:              append([]byte{}, b[16:]...),
	}, nil
}

func atomicWriteFile(path string, b []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
