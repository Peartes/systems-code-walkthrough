package raft

func (p *Persister) store(b *[]byte) {
	p.data = *b
}

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
	}

}
