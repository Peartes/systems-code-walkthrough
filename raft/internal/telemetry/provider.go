package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkMetric "go.opentelemetry.io/otel/sdk/metric"
)

// create a new metric provider
// we return back a meter provider, shutdown function to clean things up
// and any error during the process
func NewMeterProvider(serviceName string, ctx context.Context, interval time.Duration) (*sdkMetric.MeterProvider, func(context.Context) error, error) {
	// we need an exporter that dictates where a log goes
	// we're using stdout
	exporter, err := prometheus.New(prometheus.WithNamespace("Raft"))
	if err != nil {
		return nil, nil, err
	}
	// now we need a resource to attach to the provider
	// the resource is sort of the identifier for all meteric telemetry
	// for a particular provider
	// res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(serviceName)))
	// if err != nil {
	// 	return nil, nil, err
	// }
	provider := sdkMetric.NewMeterProvider(sdkMetric.WithReader(exporter))
	return provider, provider.Shutdown, nil
}
