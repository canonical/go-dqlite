package tracing

import "context"

type contextKey string

const (
	traceContextKey contextKey = "trace"
)

// TracerFromContext returns a tracer from the context. If no tracer is found,
// an empty tracer is returned.
func TracerFromContext(ctx context.Context) Tracer {
	value := ctx.Value(traceContextKey)
	if value == nil {
		return noopTracer{}
	}
	tracer, ok := value.(Tracer)
	if !ok {
		return noopTracer{}
	}
	return tracer
}

// WithTracer returns a new context with the given tracer.
func WithTracer(ctx context.Context, tracer Tracer) context.Context {
	if tracer == nil {
		tracer = noopTracer{}
	}
	return context.WithValue(ctx, traceContextKey, tracer)
}

// Start returns a new context with the given trace.
func Start(ctx context.Context, name, query string) (context.Context, Span) {
	// Tracer is always guaranteed to be returned here. If there is no tracer
	// available it will return a noop tracer.
	tracer := TracerFromContext(ctx)
	return tracer.Start(ctx, name, query)
}

// Tracer is the interface that all tracers must implement.
type Tracer interface {
	// Start creates a span and a context.Context containing the newly-created
	// span.
	//
	// If the context.Context provided in `ctx` contains a Span then the
	// newly-created Span will be a child of that span, otherwise it will be a
	// root span.
	//
	// Any Span that is created MUST also be ended. This is the responsibility
	// of the user. Implementations of this API may leak memory or other
	// resources if Spans are not ended.
	Start(context.Context, string, string) (context.Context, Span)
}

// Span is the individual component of a trace. It represents a single named
// and timed operation of a workflow that is traced. A Tracer is used to
// create a Span and it is then up to the operation the Span represents to
// properly end the Span when the operation itself ends.
type Span interface {
	// End completes the Span. The Span is considered complete and ready to be
	// delivered through the rest of the telemetry pipeline after this method
	// is called. Therefore, updates to the Span are not allowed after this
	// method has been called.
	End()
}

// noopTracer is a tracer that does nothing.
type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, name, query string) (context.Context, Span) {
	return ctx, noopSpan{}
}

// noopSpan is a span that does nothing.
type noopSpan struct{}

// End completes the Span. The Span is considered complete and ready to be
// delivered through the rest of the telemetry pipeline after this method
// is called. Therefore, updates to the Span are not allowed after this
// method has been called.
func (noopSpan) End() {}
