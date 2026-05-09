// Package transport provides the gRPC layer that connects real Raft nodes
// running as separate processes.
//
// The Raft engine (internal/raft) talks to other nodes through a [raft.Peer]
// interface — one method, Call, that takes a service name and arguments and
// returns a bool indicating success. In tests this interface is satisfied by
// an in-memory network. In production it is satisfied by [GRPCPeer], which
// translates each Call into a typed gRPC request over the network.
//
// # Server side
//
// Two gRPC services are exposed by each node:
//
//   - RaftServer handles the core protocol RPCs (RequestVote, AppendEntries,
//     InstallSnapshot). It forwards each call directly into the Raft engine.
//
//   - ControlServer handles operator and dashboard RPCs (GetState,
//     SubmitCommand, SetPartitioned). It is how the UI reads node state and
//     sends commands.
//
// Both services share the same gRPC server and listen on the same port.
//
// # Partition simulation
//
// Each node holds an atomic flag, partitioned. When the flag is true,
// RaftServer rejects all incoming Raft RPCs with an error — simulating a
// network partition without stopping the process. The flag is toggled via
// ControlServer.SetPartitioned, which the UI calls when you click the
// Partition button.
//
// # Client side
//
// NewGRPCPeer dials a remote address and returns a *GRPCPeer. Dialing is
// lazy — the connection is established on the first RPC, and reconnected
// automatically if it drops. This matches the drop-and-retry behaviour the
// Raft engine already expects from the test harness.
package transport
