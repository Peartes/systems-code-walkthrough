// Package raft implements the Raft consensus algorithm, as described in
// "In Search of an Understandable Consensus Algorithm" by Ongaro and
// Ousterhout (2014).
//
// Raft is a protocol for keeping a cluster of servers in agreement on a
// shared log of commands — even when some servers crash or messages are
// lost. Once a command is committed to the log, every server in the
// cluster will eventually apply it to its state machine in the same order.
//
// # Starting a node
//
// Call Make to create and start a Raft instance:
//
//	applyChan := make(chan ApplyMsg, 256)
//	rf := raft.Make(peers, myID, applyChan, persister)
//
// peers is a slice of [Peer] — one per server in the cluster, with the
// entry for this server set to nil. The caller drains applyChan in a
// goroutine; each message represents a log entry or snapshot that has
// been committed and is ready to be applied to the state machine.
//
// # Submitting commands
//
// Only the leader can accept new commands. Call [Raft.Start] to submit
// one. If this node is not the leader, Start returns false immediately.
// On success it returns the index at which the command will appear in the
// log once committed.
//
// # Snapshotting
//
// When the state machine has processed enough entries, it can compact the
// log by calling [Raft.Snapshot]. Raft discards all log entries up to
// that index and stores the snapshot so lagging followers can catch up
// without replaying the full history.
//
// # Transport
//
// The package is transport-agnostic. The [Peer] interface has a single
// method, Call, that the engine uses to reach other nodes. The test
// harness satisfies it with an in-memory network; the production binary
// satisfies it with [internal/transport.GRPCPeer].
package raft
