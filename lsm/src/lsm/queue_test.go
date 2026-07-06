package lsm

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// Basic push/pop
// ----------------------------------------------------------------------------

func TestFlushQueue_PushAndWaitForHead_FIFO(t *testing.T) {
	q := newFlushQueue(10)

	entries := []*flushEntry{
		{seqno: 1},
		{seqno: 2},
		{seqno: 3},
	}
	for _, e := range entries {
		if err := q.push(e); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	for _, want := range entries {
		got := q.waitForHead()
		if got == nil {
			t.Fatalf("waitForHead returned nil unexpectedly")
		}
		if got.seqno != want.seqno {
			t.Errorf("expected head seqno=%d, got %d", want.seqno, got.seqno)
		}
		q.popHead()
	}
}

func TestFlushQueue_WaitForHead_PeeksNoRemove(t *testing.T) {
	q := newFlushQueue(10)
	q.push(&flushEntry{seqno: 42})

	first := q.waitForHead()
	second := q.waitForHead()
	if first == nil || second == nil {
		t.Fatalf("waitForHead should not return nil while item exists")
	}
	if first != second {
		t.Fatalf("waitForHead should peek — successive calls should return the SAME entry until popHead")
	}
}

// ----------------------------------------------------------------------------
// Snapshot returns newest-first
// ----------------------------------------------------------------------------

func TestFlushQueue_SnapshotReturnsNewestFirst(t *testing.T) {
	q := newFlushQueue(10)
	q.push(&flushEntry{seqno: 1}) // oldest
	q.push(&flushEntry{seqno: 2})
	q.push(&flushEntry{seqno: 3}) // newest

	snap := q.snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 entries in snapshot, got %d", len(snap))
	}
	want := []uint64{3, 2, 1} // newest → oldest
	for i, e := range snap {
		if e.seqno != want[i] {
			t.Errorf("snapshot[%d].seqno = %d, want %d", i, e.seqno, want[i])
		}
	}
}

func TestFlushQueue_SnapshotEmptyQueue(t *testing.T) {
	q := newFlushQueue(10)
	snap := q.snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected empty snapshot on fresh queue, got %d entries", len(snap))
	}
}

func TestFlushQueue_SnapshotIsCopy(t *testing.T) {
	// Snapshot mutations must not affect the queue's internal state.
	q := newFlushQueue(10)
	q.push(&flushEntry{seqno: 1})
	q.push(&flushEntry{seqno: 2})

	snap := q.snapshot()
	snap[0] = &flushEntry{seqno: 999}

	// Re-snapshot; verify original entries survived.
	snap2 := q.snapshot()
	if snap2[0].seqno == 999 {
		t.Fatalf("snapshot returned a reference into internal storage; mutations leaked into queue state")
	}
}

// ----------------------------------------------------------------------------
// Backpressure — push blocks when full, unblocks on popHead
// ----------------------------------------------------------------------------

func TestFlushQueue_PushBlocksWhenFull(t *testing.T) {
	q := newFlushQueue(2)
	q.push(&flushEntry{seqno: 1})
	q.push(&flushEntry{seqno: 2})

	pushed := make(chan error, 1)
	go func() {
		pushed <- q.push(&flushEntry{seqno: 3})
	}()

	// The goroutine should still be blocked after a beat.
	select {
	case err := <-pushed:
		t.Fatalf("push returned early with %v; should have blocked", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Popping should unblock it.
	q.popHead()

	select {
	case err := <-pushed:
		if err != nil {
			t.Fatalf("unblocked push returned unexpected error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("push did not unblock within 500ms after popHead")
	}
}

// ----------------------------------------------------------------------------
// waitForHead blocks when empty, unblocks on push
// ----------------------------------------------------------------------------

func TestFlushQueue_WaitForHeadBlocksWhenEmpty(t *testing.T) {
	q := newFlushQueue(10)

	ready := make(chan *flushEntry, 1)
	go func() {
		ready <- q.waitForHead()
	}()

	// Should still be blocked.
	select {
	case e := <-ready:
		t.Fatalf("waitForHead returned early with %v; should have blocked", e)
	case <-time.After(50 * time.Millisecond):
	}

	// Pushing should unblock it.
	q.push(&flushEntry{seqno: 7})

	select {
	case e := <-ready:
		if e == nil || e.seqno != 7 {
			t.Fatalf("expected unblocked waitForHead to return seqno=7, got %v", e)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("waitForHead did not unblock within 500ms after push")
	}
}

// ----------------------------------------------------------------------------
// Close semantics
// ----------------------------------------------------------------------------

func TestFlushQueue_CloseUnblocksWaitForHead(t *testing.T) {
	q := newFlushQueue(10)

	result := make(chan *flushEntry, 1)
	go func() {
		result <- q.waitForHead()
	}()

	// Verify goroutine is actually blocked before we close.
	time.Sleep(50 * time.Millisecond)

	q.close()

	select {
	case e := <-result:
		if e != nil {
			t.Fatalf("waitForHead after close should return nil, got %v", e)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("waitForHead did not unblock within 500ms after close")
	}
}

func TestFlushQueue_CloseUnblocksPush(t *testing.T) {
	q := newFlushQueue(2)
	q.push(&flushEntry{seqno: 1})
	q.push(&flushEntry{seqno: 2})

	result := make(chan error, 1)
	go func() {
		result <- q.push(&flushEntry{seqno: 3})
	}()

	time.Sleep(50 * time.Millisecond)

	q.close()

	select {
	case err := <-result:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("push after close should return ErrQueueClosed, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("push did not unblock within 500ms after close")
	}
}

func TestFlushQueue_PushAfterCloseReturnsErr(t *testing.T) {
	q := newFlushQueue(10)
	q.close()

	err := q.push(&flushEntry{seqno: 1})
	if !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("push after close should return ErrQueueClosed, got %v", err)
	}
}

func TestFlushQueue_WaitForHeadAfterCloseReturnsNil(t *testing.T) {
	q := newFlushQueue(10)
	q.close()

	if got := q.waitForHead(); got != nil {
		t.Fatalf("waitForHead after close should return nil, got %v", got)
	}
}

// ----------------------------------------------------------------------------
// popHead defensive
// ----------------------------------------------------------------------------

func TestFlushQueue_PopHeadOnEmptyIsNoOp(t *testing.T) {
	q := newFlushQueue(10)
	// Should not panic.
	q.popHead()
	q.popHead()
	// Subsequent operations must still work.
	if err := q.push(&flushEntry{seqno: 1}); err != nil {
		t.Fatalf("push after empty popHead: %v", err)
	}
}

// ----------------------------------------------------------------------------
// Stress: many concurrent producers + one consumer
// ----------------------------------------------------------------------------

func TestFlushQueue_StressProducerConsumer(t *testing.T) {
	q := newFlushQueue(3)

	const producers = 5
	const perProducer = 20
	totalItems := producers * perProducer

	var wg sync.WaitGroup
	// Producers
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				seqno := uint64(pid*perProducer + i)
				if err := q.push(&flushEntry{seqno: seqno}); err != nil {
					t.Errorf("producer %d push: %v", pid, err)
					return
				}
			}
		}(p)
	}

	// Single consumer
	consumed := 0
	consumerDone := make(chan struct{})
	go func() {
		for {
			e := q.waitForHead()
			if e == nil {
				close(consumerDone)
				return
			}
			q.popHead()
			consumed++
		}
	}()

	wg.Wait()

	// Wait for consumer to drain everything.
	// It's still blocked in waitForHead — close it to let it exit.
	deadline := time.After(2 * time.Second)
	for consumed < totalItems {
		select {
		case <-deadline:
			t.Fatalf("consumer did not drain: consumed=%d, expected=%d", consumed, totalItems)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	q.close()
	<-consumerDone

	if consumed != totalItems {
		t.Errorf("consumed=%d, expected=%d", consumed, totalItems)
	}
}
