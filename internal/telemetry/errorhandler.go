package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ExportErrorsMetric counts every error the OpenTelemetry SDK reports, by the
// signal whose export failed. A collector that cannot be reached shows here,
// on the default sink, instead of only on stderr.
const ExportErrorsMetric = "ork.telemetry.export_errors"

// AttrSignal is the signal an export error, or a dropped record, belongs to.
const AttrSignal = "ork.telemetry.signal"

// The values of AttrSignal. SignalOther is an error the SDK reported that no
// exporter of this package tagged: a processor's, a reader's, a callback's.
const (
	SignalTraces  = "traces"
	SignalMetrics = "metrics"
	SignalLogs    = "logs"
	SignalOther   = "other"
)

// exportErrorLogInterval is how often one signal's export failures are
// logged: the first at once, then at most one line per interval, naming how
// many were suppressed. A collector that is down for an hour is 60 lines, not
// one per batch.
const exportErrorLogInterval = time.Minute

// signalError says which signal's exporter returned err. The SDK passes an
// exporter's error to otel.Handle as it is, or wrapped; errors.As finds it.
type signalError struct {
	sink, signal string
	err          error
}

func (e *signalError) Error() string { return e.err.Error() }
func (e *signalError) Unwrap() error { return e.err }

func tagSignal(sink, signal string, err error) error {
	if err == nil {
		return nil
	}
	return &signalError{sink: sink, signal: signal, err: err}
}

// signalSpanExporter and signalMetricExporter tag their exporter's errors
// with the signal; countingLogExporter does the same for logs. They change
// nothing else: the SDK still retries, drops and reports as it did.
type signalSpanExporter struct {
	sdktrace.SpanExporter
	sink string
}

func (e signalSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return tagSignal(e.sink, SignalTraces, e.SpanExporter.ExportSpans(ctx, spans))
}

type signalMetricExporter struct {
	sdkmetric.Exporter
	sink string
}

func (e signalMetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	return tagSignal(e.sink, SignalMetrics, e.Exporter.Export(ctx, rm))
}

// exportErrorHandler is the process's otel.ErrorHandler. It counts and,
// rate-limited, logs; it never blocks, retries or fails anything (AGENTS.md
// §11: telemetry is best-effort).
type exportErrorHandler struct {
	counter metric.Int64Counter
	logger  atomic.Pointer[slog.Logger]
	now     func() time.Time

	mu         sync.Mutex
	lastLog    map[string]time.Time
	suppressed map[string]int
}

func newExportErrorHandler(provider metric.MeterProvider) (*exportErrorHandler, error) {
	counter, err := provider.Meter(ScopeName).Int64Counter(ExportErrorsMetric,
		metric.WithDescription("Errors the telemetry SDK reported, by the signal whose export failed."),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, fmt.Errorf("telemetry instruments: %w", err)
	}
	h := &exportErrorHandler{
		counter:    counter,
		now:        time.Now,
		lastLog:    map[string]time.Time{},
		suppressed: map[string]int{},
	}
	// Until the process logger is attached (Sinks.WithLogger), failures are
	// still logged, to stderr, as the SDK's own handler would have.
	h.logger.Store(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	return h, nil
}

// urlUserinfo and urlQuery find what an export error's text may carry of a
// destination's credentials: a URL's user and password, and its query and
// fragment, where some collectors take a token. The scheme, host and path
// stay, so the log still says which destination failed.
var (
	urlUserinfo = regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://)[^/\s"']*@`)
	urlQuery    = regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://[^\s?#"']*)[?#][^\s"']*`)
)

// exportErrorText is err's text with any URL's credentials removed. A
// *url.Error's own URL is redacted by parsing it; the patterns then catch a
// URL anywhere else in the text.
func exportErrorText(err error) string {
	text := err.Error()
	var ue *url.Error
	if errors.As(err, &ue) {
		if u, parseErr := url.Parse(ue.URL); parseErr == nil {
			u.User, u.RawQuery, u.Fragment, u.RawFragment = nil, "", "", ""
			text = strings.Replace(text, ue.URL, u.String(), 1)
		}
	}
	text = urlUserinfo.ReplaceAllString(text, "${1}")
	return urlQuery.ReplaceAllString(text, "${1}?…")
}

// setLogger attaches the process logger, replacing the stderr one. A nil
// logger keeps the one there is.
func (h *exportErrorHandler) setLogger(logger *slog.Logger) {
	if logger != nil {
		h.logger.Store(logger)
	}
}

// Handle implements otel.ErrorHandler. An error an exporter tagged is one
// sink's failed export, logged under that sink and signal; any other is the
// SDK's own (a reader's, a callback's), counted and logged as SignalOther.
// Each sink and signal is rate-limited on its own, so one collector that is
// down never hides another's failure.
func (h *exportErrorHandler) Handle(err error) {
	if err == nil {
		return
	}
	sink, signal, message := "", SignalOther, "telemetry error"
	var tagged *signalError
	if errors.As(err, &tagged) {
		sink, signal, message = tagged.sink, tagged.signal, "telemetry export failed"
	}
	h.counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String(AttrSignal, signal)))
	logger := h.logger.Load()
	if logger == nil {
		return
	}
	key := sink + "\x00" + signal
	h.mu.Lock()
	now := h.now()
	if last, seen := h.lastLog[key]; seen && now.Sub(last) < exportErrorLogInterval {
		h.suppressed[key]++
		h.mu.Unlock()
		return
	}
	suppressed := h.suppressed[key]
	h.suppressed[key] = 0
	h.lastLog[key] = now
	h.mu.Unlock()
	attrs := []any{"signal", signal, "error", exportErrorText(err), "suppressed", suppressed}
	if sink != "" {
		attrs = append([]any{"sink", sink}, attrs...)
	}
	logger.Warn(message, attrs...)
}

// installErrorHandler makes h the process's otel.ErrorHandler. It is called
// only when some signal is on, so a zero-config process touches no global.
func installErrorHandler(h *exportErrorHandler) { otel.SetErrorHandler(h) }
