package raft

func (p *Persister) store(b *[]byte) {
	p.data = *b
}

func (p *Persister) load() []byte {
	return p.data
}
