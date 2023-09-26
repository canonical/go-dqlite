package tracing

import "context"

type contextKey string

const (
	traceContextKey contextKey = "trace"
)

// WithTracer returns a context with the tracer embedded in the context
// under the context key.
func WithTracer(ctx context.Context, tracer Tracer) context.Context {
	return context.WithValue(ctx, traceContextKey, tracer)
}

// Start returns a new context with the given trace.
// A valid span is always returned, even if the context does not contain a
// tracer. In that case, the span is a noop span.
func Start(ctx context.Context, name, query string) (context.Context, Span) {
	value := ctx.Value(traceContextKey)
	if value == nil {
		return ctx, noopSpan{}
	}
	tracer, ok := value.(Tracer)
	if !ok {
		return ctx, noopSpan{}
	}
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

// noopSpan is a span that does nothing.
type noopSpan struct{}

// End completes the Span. The Span is considered complete and ready to be
// delivered through the rest of the telemetry pipeline after this method
// is called. Therefore, updates to the Span are not allowed after this
// method has been called.
func (noopSpan) End() {}
