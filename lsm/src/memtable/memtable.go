package memtable

import sk "github.com/peartes/lsm/src/skiplist"

// A memtable is a data encapsulation layer over a skiplist or any other efficient data structure
// that an LSM tree uses to keep data in-memory. A memtable makes sure data are flushed into SSTables
// allows a simple API to interact with the underlying structure and can be frozen during flushing
type Memtable struct {
	sl     *sk.SkipList
	size   int
	frozen bool
}

func NewMemtable(sk *sk.SkipList) *Memtable {
	return &Memtable{
		sl: sk,
	}
}

func (m *Memtable) Get(key string) (value []byte, tombstone bool, found bool) {

	return m.sl.Get(key)
}

func (m *Memtable) Put(key string, value []byte) {
	// make sure the memtable is not frozen
	if m.frozen {
		panic("memtable frozen; not accepting edit")
	}
	oldValueLen, overwritten := m.sl.Insert(key, value, false)
	if !overwritten {
		// this is a new addition
		m.size += len(key) + len(value)
	} else {
		m.size += len(value) - oldValueLen
	}
}

func (m *Memtable) Delete(key string) {
	if m.frozen {
		panic("memtable frozen; not accepting deletes")
	}
	oldValueLen, overwritten := m.sl.Insert(key, nil, true)
	if !overwritten {
		// this is a new addition
		m.size += len(key)
	} else {
		m.size -= oldValueLen
	}
}

// The Iterate method walks through each item in the underlying data structure applying
// the fn to each of them
func (m *Memtable) Iterate(f func(key string, value []byte, tombstone bool) bool) bool {
	return m.sl.Iterate(f)
}

func (m *Memtable) SizeBytes() int {
	return m.size
}
func (m *Memtable) Freeze() {
	m.frozen = true
}

func (m *Memtable) IsFrozen() bool { return m.frozen }

func (m *Memtable) Len() int {
	return m.sl.Len()
}
