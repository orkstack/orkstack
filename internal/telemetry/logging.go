package telemetry

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/trace"
)

// NewLogger builds the process logger. It is the ONLY constructor either
// binary uses, so the zero-config promise is checkable in one place: with the
// zero Config it returns slog.New(slog.NewTextHandler(w, nil)) in effect —
// text, info level, no source — which is the line both mains had before.
//
// ORK_LOG_FORMAT=json switches the encoding; ORK_LOG_LEVEL the floor; and
// OTEL_LOGS_EXPORTER=otlp adds a second handler that forwards every record to
// the log provider, so the same line an operator tails on stderr also lands in
// Loki or Datadog with the process's resource attributes on it. The Temporal
// SDK's own logging rides this logger too (client.Options.Logger), so its
// lines get the same treatment.
func NewLogger(cfg Config, providers *Providers, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	var local slog.Handler
	if cfg.LogFormat == LogFormatJSON {
		local = slog.NewJSONHandler(w, opts)
	} else {
		local = slog.NewTextHandler(w, opts)
	}
	if providers == nil || !cfg.LogsEnabled() {
		return slog.New(local)
	}
	bridge := otelslog.NewHandler(ScopeName, otelslog.WithLoggerProvider(providers.Logs))
	return slog.New(&fanoutHandler{handlers: []slog.Handler{local, bridge}})
}

// fanoutHandler hands each record to every handler. A record is emitted when
// ANY handler wants it, so the OTLP bridge's own level decision is not
// pre-empted by the local floor and vice versa.
type fanoutHandler struct {
	handlers []slog.Handler
}

func (h *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *fanoutHandler) Handle(ctx context.Context, record slog.Record) error {
	var first error
	for _, handler := range h.handlers {
		if !handler.Enabled(ctx, record.Level) {
			continue
		}
		if err := handler.Handle(ctx, record.Clone()); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (h *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		next[i] = handler.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: next}
}

func (h *fanoutHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		next[i] = handler.WithGroup(name)
	}
	return &fanoutHandler{handlers: next}
}

// TraceAttrs returns the trace_id and span_id of the span on ctx as slog
// attributes, or nothing when there is no valid span. A log line that carries
// them is the jump from a Loki query to the Tempo trace it belongs to.
func TraceAttrs(ctx context.Context) []any {
	span := trace.SpanContextFromContext(ctx)
	if !span.IsValid() {
		return nil
	}
	return []any{
		slog.String("trace_id", span.TraceID().String()),
		slog.String("span_id", span.SpanID().String()),
	}
}
