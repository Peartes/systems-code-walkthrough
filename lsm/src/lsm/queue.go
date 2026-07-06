package lsm

import (
	"slices"
	"sync"

	"github.com/peartes/lsm/src/memtable"
)

type flushEntry struct {
	memtable *memtable.Memtable // frozen
	walPath  string             // WAL segment to delete after successful flush
	seqno    uint64
}

type flushQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond    // signaled on push and pop
	items  []*flushEntry // FIFO: index 0 = head (oldest, next to flush)
	maxLen int           // cap on queued items (e.g. 3)
	closed bool
}

// Creates a queue that will block push() when it holds maxLen items.
func newFlushQueue(maxLen int) *flushQueue {
	queue := &flushQueue{}
	queue.cond = sync.NewCond(&queue.mu)
	queue.maxLen = maxLen
	return queue
}

// Blocks until there's space (len(items) < maxLen) OR the queue is closed.
// Returns an error (or false, your choice) if the queue was closed while waiting.
func (q *flushQueue) push(e *flushEntry) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) >= q.maxLen {
		if q.closed {
			return ErrQueueClosed
		}
		q.cond.Wait()
	}
	if q.closed {
		return ErrQueueClosed
	}
	q.items = append(q.items, e)
	q.cond.Broadcast()
	return nil
}

// Blocks until the queue is non-empty OR closed.
// Returns items[0] without removing it (peek). Returns nil if closed.
// The flush goroutine uses this to look at what to flush next, then calls
// popHead() only if the flush succeeded.
func (q *flushQueue) waitForHead() *flushEntry {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 {
		if q.closed {
			return nil
		}
		q.cond.Wait()
	}
	return q.items[0]
}

// Removes items[0] and signals push waiters. Called after a successful flush.
func (q *flushQueue) popHead() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return
	}
	q.items = q.items[1:]
	q.cond.Broadcast()
}

// Returns a copy of items in NEWEST-FIRST order (reverse of FIFO).
// The Get path uses this to consult queued memtables from newest to oldest.
func (q *flushQueue) snapshot() []*flushEntry {
	q.mu.Lock()
	defer q.mu.Unlock()

	items := make([]*flushEntry, len(q.items))
	copy(items, q.items)

	slices.Reverse(items)
	return items
}

// Wakes all waiters; subsequent push/waitForHead unblock.
func (q *flushQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}
