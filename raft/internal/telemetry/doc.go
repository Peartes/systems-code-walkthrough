// Package telemetry instruments the Raft engine with OpenTelemetry metrics.
//
// It exposes two things: a way to create a metrics provider, and a
// collection of Raft-specific instruments that record what the cluster
// is doing in real time.
//
// # What gets measured
//
// Elections — how many each node has started, won, and lost.
// Node state — the current role (follower, candidate, leader) and term.
// Log health — log length, commit index, and last-applied index.
// RPCs — how many AppendEntries and InstallSnapshot calls were sent and
// received, and whether they succeeded.
//
// # How to use it
//
// Create a provider, build the metrics object, and attach it to the node:
//
//	provider, shutdown, err := telemetry.NewMeterProvider("raft-0", ctx, 2*time.Second)
//	defer shutdown(ctx)
//
//	m, err := telemetry.NewRaftMetrics(provider, "0")
//	rf.SetMetrics(m)
//
// After that the node records metrics automatically — no further calls
// needed from application code.
//
// # Backend
//
// NewMeterProvider currently wires up the Prometheus exporter, which
// registers metrics in the default Prometheus registry. Expose them by
// serving promhttp.Handler() on an HTTP endpoint and pointing Prometheus
// at it. Swapping the backend (e.g. to stdout for local debugging) only
// requires changing NewMeterProvider — nothing else in the codebase
// needs to change.
//
// # Nil safety
//
// Every method on [RaftMetrics] is nil-safe. Nodes that are created
// without calling SetMetrics silently skip all recording.
package telemetry
