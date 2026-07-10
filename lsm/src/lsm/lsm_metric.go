package lsm

import (
	"fmt"

	metric "go.opentelemetry.io/otel/metric"
	sdkMetric "go.opentelemetry.io/otel/sdk/metric"
)

type LMetric struct {
	// operation throughput
	readTP   metric.Int64Counter // read throughput i.e. how many read op/time
	writeTP  metric.Int64Counter
	deleteTP metric.Int64Counter

	// latency - histogram so we can calcualte percentile
	readL   metric.Float64Histogram
	writeL  metric.Float64Histogram
	deleteL metric.Float64Histogram
}

func NewLMetric(p *sdkMetric.MeterProvider, serviceName string) (*LMetric, error) {
	meter := p.Meter(serviceName)
	// throughput instruments
	readTP, err := meter.Int64Counter("lsm.read", metric.WithUnit("{operations}"), metric.WithDescription("total read operations"))
	if err != nil {
		return nil, fmt.Errorf("%w lsm.read: %w", ErrLSMOTelInstrument, err)
	}
	writeTP, err := meter.Int64Counter("lsm.write", metric.WithUnit("{operations}"), metric.WithDescription("total write operations"))
	if err != nil {
		return nil, fmt.Errorf("%w lsm.write: %w", ErrLSMOTelInstrument, err)
	}
	deleteTP, err := meter.Int64Counter("lsm.delete", metric.WithUnit("{operations}"), metric.WithDescription("total delete operations"))
	if err != nil {
		return nil, fmt.Errorf("%w lsm.delete: %w", ErrLSMOTelInstrument, err)
	}
	// latency instrument
	readL, err := meter.Float64Histogram("lsm.read.latency", metric.WithUnit("ms"), metric.WithDescription("read latency in milliseconds"), metric.WithExplicitBucketBoundaries(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000))
	if err != nil {
		return nil, fmt.Errorf("%w lsm.read.latency: %w", ErrLSMOTelInstrument, err)
	}
	writeL, err := meter.Float64Histogram("lsm.write.latency", metric.WithUnit("ms"), metric.WithDescription("write latency in milliseconds"), metric.WithExplicitBucketBoundaries(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000))
	if err != nil {
		return nil, fmt.Errorf("%w lsm.write.latency: %w", ErrLSMOTelInstrument, err)
	}
	deleteL, err := meter.Float64Histogram("lsm.delete.latency", metric.WithUnit("ms"), metric.WithDescription("delete latency in milliseconds"), metric.WithExplicitBucketBoundaries(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000))
	if err != nil {
		return nil, fmt.Errorf("%w lsm.delete.latency: %w", ErrLSMOTelInstrument, err)
	}
	metric := &LMetric{
		readTP,
		writeTP,
		deleteTP,
		readL,
		writeL,
		deleteL,
	}
	return metric, nil
}
