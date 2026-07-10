package telemetry

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	metric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

func NewMeterProvider(lsmAttr ...attribute.KeyValue) (*metric.MeterProvider, error) {
	// create a prometheus reader of the metric
	exporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("error creating prometheus provider: %w", err)
	}

	return metric.NewMeterProvider(
		metric.WithResource(resource.NewWithAttributes("", lsmAttr...)),
		metric.WithReader(exporter),
	), nil
}
