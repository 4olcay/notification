package tracing

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func sampleRate() float64 {
	if v := os.Getenv("OTEL_TRACES_SAMPLE_RATIO"); v != "" {
		if r, err := strconv.ParseFloat(v, 64); err == nil && r >= 0 && r <= 1 {
			return r
		}
	}
	return 0.1
}

func Init(ctx context.Context, serviceName, endpoint string) (func(context.Context) error, error) {
	if endpoint == "" {
		endpoint = "http://localhost:4318"
	}

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build OTel resource: %w", err)
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRate()))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	return tp.Shutdown, nil
}
