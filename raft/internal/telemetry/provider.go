package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkMetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// create a new metric provider
// we return back a meter provider, shutdown function to clean things up
// and any error during the process
func NewMeterProvider(serviceName string, ctx context.Context, interval time.Duration) (*sdkMetric.MeterProvider, func(context.Context) error, error) {
	// we need an exporter that dictates where a log goes
	// we're using stdout
	exporter, err := stdoutmetric.New()
	if err != nil {
		return nil, nil, err
	}
	// now we need a resource to attach to the provider
	// the resource is sort of the identifier for all meteric telemetry
	// for a particular provider
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, nil, err
	}
	provider := sdkMetric.NewMeterProvider(sdkMetric.WithResource(res), sdkMetric.WithReader(sdkMetric.NewPeriodicReader(exporter, sdkMetric.WithInterval(interval))))
	return provider, provider.Shutdown, nil
}
