package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// scope is the instrumentation library name — by convention the import path
// of the package being instrumented.
const scope = "github.com/peartes/raft"

// keep metric of a raft instance
type RaftMetrics struct {
	serverAttr attribute.KeyValue

	electionsStarted metric.Int64Counter // numnber of election an instance has started
	electionsWon     metric.Int64Counter
	electionsLost    metric.Int64Counter

	// Node state — Int64Gauge holds the current value of something
	// (unlike a counter it can go up and down).
	currentTerm  metric.Int64Gauge
	currentState metric.Int64Gauge // 0=follower 1=candidate 2=leader

	// Log health
	logLength   metric.Int64Gauge
	commitIndex metric.Int64Gauge
	lastApplied metric.Int64Gauge

	// RPC counters — an "outcome" attribute distinguishes success from
	// failure on the same counter so dashboards can compute success-rate
	// with a single query.
	aeRPCSent     metric.Int64Counter // AppendEntry RPC sent
	aeRPCReceived metric.Int64Counter
	isRPCSent     metric.Int64Counter // Install snapshot RPC sent
	isRPCReceived metric.Int64Counter
}

func NewRaftMetrics(p metric.MeterProvider, serverID string) (*RaftMetrics, error) {
	meter := p.Meter(scope)
	m := &RaftMetrics{
		serverAttr: attribute.String("server_id", serverID),
	}
	var err error
	if m.electionsStarted, err = meter.Int64Counter("raft.election.started", metric.WithDescription("Total elections started by this node"), metric.WithUnit("{election}")); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}
	if m.electionsWon, err = meter.Int64Counter(
		"raft.elections.won",
		metric.WithDescription("Total elections won (node became leader)"),
		metric.WithUnit("{election}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.electionsLost, err = meter.Int64Counter(
		"raft.elections.lost",
		metric.WithDescription("Total elections lost or abandoned"),
		metric.WithUnit("{election}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	// Int64Gauge — current point-in-time value, recorded at transition points.
	if m.currentTerm, err = meter.Int64Gauge(
		"raft.node.term",
		metric.WithDescription("Current Raft term"),
		metric.WithUnit("{term}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.currentState, err = meter.Int64Gauge(
		"raft.node.state",
		metric.WithDescription("Current role: 0=follower 1=candidate 2=leader"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.logLength, err = meter.Int64Gauge(
		"raft.log.length",
		metric.WithDescription("Entries in the in-memory log (including sentinel)"),
		metric.WithUnit("{entry}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.commitIndex, err = meter.Int64Gauge(
		"raft.log.commit_index",
		metric.WithDescription("Index of the last committed log entry"),
		metric.WithUnit("{entry}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.lastApplied, err = meter.Int64Gauge(
		"raft.log.last_applied",
		metric.WithDescription("Index of the last entry applied to the state machine"),
		metric.WithUnit("{entry}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.aeRPCSent, err = meter.Int64Counter(
		"raft.rpc.append_entries.sent",
		metric.WithDescription("AppendEntries RPCs sent by this node as leader"),
		metric.WithUnit("{rpc}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.aeRPCReceived, err = meter.Int64Counter(
		"raft.rpc.append_entries.received",
		metric.WithDescription("AppendEntries RPCs received as follower"),
		metric.WithUnit("{rpc}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.isRPCSent, err = meter.Int64Counter(
		"raft.rpc.install_snapshot.sent",
		metric.WithDescription("InstallSnapshot RPCs sent by this node as leader"),
		metric.WithUnit("{rpc}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	if m.isRPCReceived, err = meter.Int64Counter(
		"raft.rpc.install_snapshot.received",
		metric.WithDescription("InstallSnapshot RPCs received as follower"),
		metric.WithUnit("{rpc}"),
	); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}

	return m, nil
}

// RecordElectionStarted records that this node started an election.
func (m *RaftMetrics) RecordElectionStarted(ctx context.Context) {
	if m == nil {
		return
	}
	m.electionsStarted.Add(ctx, 1, metric.WithAttributes(m.serverAttr))
}

// RecordElectionWon records that this node won an election and became leader.
func (m *RaftMetrics) RecordElectionWon(ctx context.Context) {
	if m == nil {
		return
	}
	m.electionsWon.Add(ctx, 1, metric.WithAttributes(m.serverAttr))
}

// RecordElectionLost records that this node lost or abandoned an election.
func (m *RaftMetrics) RecordElectionLost(ctx context.Context) {
	if m == nil {
		return
	}
	m.electionsLost.Add(ctx, 1, metric.WithAttributes(m.serverAttr))
}

// RecordState snapshots the node's current role and term. Call this at every
// state transition (follower→candidate, candidate→leader, leader→follower).
func (m *RaftMetrics) RecordState(ctx context.Context, state, term int) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(m.serverAttr)
	m.currentState.Record(ctx, int64(state), attrs)
	m.currentTerm.Record(ctx, int64(term), attrs)
}

// RecordLogState snapshots the three log-health indices. Call this in the
// applier after each apply batch and after each commit-index advance.
func (m *RaftMetrics) RecordLogState(ctx context.Context, logLen, commitIdx, lastApplied int) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(m.serverAttr)
	m.logLength.Record(ctx, int64(logLen), attrs)
	m.commitIndex.Record(ctx, int64(commitIdx), attrs)
	m.lastApplied.Record(ctx, int64(lastApplied), attrs)
}

// RecordAESent records an AppendEntries RPC sent by this leader.
// success=false means the call returned ok=false (network failure).
func (m *RaftMetrics) RecordAESent(ctx context.Context, success bool) {
	if m == nil {
		return
	}
	m.aeRPCSent.Add(ctx, 1, metric.WithAttributes(
		m.serverAttr,
		attribute.String("outcome", outcomeLabel(success)),
	))
}

// RecordAEReceived records an AppendEntries RPC received by this follower.
func (m *RaftMetrics) RecordAEReceived(ctx context.Context) {
	if m == nil {
		return
	}
	m.aeRPCReceived.Add(ctx, 1, metric.WithAttributes(m.serverAttr))
}

// RecordISSent records an InstallSnapshot RPC sent by this leader.
func (m *RaftMetrics) RecordISSent(ctx context.Context, success bool) {
	if m == nil {
		return
	}
	m.isRPCSent.Add(ctx, 1, metric.WithAttributes(
		m.serverAttr,
		attribute.String("outcome", outcomeLabel(success)),
	))
}

// RecordISReceived records an InstallSnapshot RPC received by this follower.
func (m *RaftMetrics) RecordISReceived(ctx context.Context) {
	if m == nil {
		return
	}
	m.isRPCReceived.Add(ctx, 1, metric.WithAttributes(m.serverAttr))
}

func outcomeLabel(success bool) string {
	if success {
		return "success"
	}
	return "failure"
}
