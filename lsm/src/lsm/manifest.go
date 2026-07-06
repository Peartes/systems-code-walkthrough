package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// Manifest struct keeps track of the manifest file and allows append only write to the file
//
// the manifest file keeps track of the next sequence number for the WAL and SSTable
// and the active SSTable seqno
//
// active seqnos are saved as 8byte integers and the next seq no as a 8 byte integer also
type Manifest struct {
	mu sync.Mutex

	dir          string // the manifest file directory
	NextSeqNo    uint64
	LiveSSTables []uint64
}

const MANIFEST = "MANIFEST"

// Load loads the manifest file in the dir or creates a new one if none
//
// manifest file are stored with the name manifest
//
// we load the manifest as readOnly because an append to it involves writing out to a temp file
// then renaming to manifest to avoid partial write corruption
func Load(dir string) (*Manifest, error) {
	fileContent, err := os.ReadFile(filepath.Join(dir, MANIFEST))
	if errors.Is(err, os.ErrNotExist) || len(fileContent) < 8 {
		return &Manifest{dir: dir, NextSeqNo: 1, LiveSSTables: nil}, nil
	}
	if err != nil {
		return nil, err
	}
	nextSeqNo := binary.LittleEndian.Uint64(fileContent)

	active := []uint64{}
	currPos := 8
	for {
		if currPos >= len(fileContent) {
			break
		}
		seq := binary.LittleEndian.Uint64(fileContent[currPos : currPos+8])
		currPos += 8
		active = append(active, seq)
	}
	return &Manifest{
		dir:          dir,
		NextSeqNo:    nextSeqNo,
		LiveSSTables: active,
	}, nil
}

// Save persists the manifest file into storage
func (m *Manifest) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// create a temp manifest file manifest.tmp
	tmpManifest := fmt.Sprintf("%s.%s", MANIFEST, "tmp")
	fd, err := os.OpenFile(filepath.Join(m.dir, tmpManifest), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrLSMFileOpen, tmpManifest)
	}
	defer fd.Close()
	buf := make([]byte, 0, 8+len(m.LiveSSTables)*8)
	buf = binary.LittleEndian.AppendUint64(buf, m.NextSeqNo)
	for _, seqno := range m.LiveSSTables {
		buf = binary.LittleEndian.AppendUint64(buf, seqno)
	}
	if _, err := fd.Write(buf); err != nil {
		return fmt.Errorf("%w: %w", ErrManifestFileSave, err)
	}
	// fsycn to ensure immediate write to disk
	err = fd.Sync()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrManifestFileSave, err)
	}
	// now we need to rename this tmp file to MANIFEST
	err = os.Rename(filepath.Join(m.dir, tmpManifest), filepath.Join(m.dir, MANIFEST))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrManifestFileSave, err)
	}
	return nil
}

// Returns current NextSeqno; increments the field but does NOT persist.
// Caller must call Save() at the right moment to make it durable.
func (m *Manifest) AllocSeqno() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	seqno := m.NextSeqNo
	m.NextSeqNo += 1
	return seqno
}

// Appends seqno to LiveSSTables, keeps sorted, then Save()s.
func (m *Manifest) AddSSTable(seqno uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if slices.Contains(m.LiveSSTables, seqno) {
		return // already present, no-op
	}
	m.LiveSSTables = append(m.LiveSSTables, seqno)
}
