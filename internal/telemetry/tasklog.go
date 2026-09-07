package telemetry

import (
	"context"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/logstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
)

// defaultMaxLine is the ceiling TaskLogExporter falls back to when its caller
// asks for none or a nonsense one. It sits far above a normal line so it only
// ever catches the pathological case a container that never terminates a
// line.
const defaultMaxLine = 1 << 20

// Stream names TaskLogWriter puts on ork.stream.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
	StreamOrk    = "ork"
)

// TaskLogExporter is the Orker-side path that turns a task's stdout/stderr
// lines into OTel log records, ADDITIONAL to the durable chunk store a task's
// output already lands in (RunService.StreamTaskLogs and the artifact store):
// this is best-effort, dropped under backpressure, and never the record of
// truth a resumed stream or an audit reads back.
//
// A TaskLogExporter routes each attempt independently: Open reads the
// attempt's own ExecuteTaskInput.GetTelemetry() to decide both whether this
// attempt's lines are exported at all, and which sink they go to. This is a
// per-run signal (§ package doc): the Orker's own [ork] lifecycle notices and
// its process-level slog output are not task log lines and never pass through
// here at all — they stay on whatever the binary's own logger is wired to.
type TaskLogExporter struct {
	sinks        *Sinks
	maxLine      int
	globalExport bool

	cache providerCache[*taskLogRoute]
}

// taskLogRoute is one sink's cached logger and instruments, built at most
// once per sink no matter how many attempts Open sees. emittedOpt and
// truncatedOpt are TaskLogRecords' two possible outcome attribute sets,
// precomputed once here rather than on every Write: metric.WithAttributeSet
// builds an attribute.Set (a sort plus a dedupe pass) from the attributes
// given it, and a route only ever records one of these two exact sets.
type taskLogRoute struct {
	logger       log.Logger
	inst         *OrkerInstruments
	emittedOpt   metric.MeasurementOption
	truncatedOpt metric.MeasurementOption
}

// NewTaskLogExporterForSinks always returns non-nil: unlike the single-sink
// NewTaskLogExporter, there is no single "logs are off" answer to give up
// front — a deployment can have logs off globally (globalExport false) yet a
// namespace's TelemetrySelection.TaskLogs can still turn them on for its own
// runs, or the reverse, and that can only be decided per attempt, in Open.
//
// globalExport is ORK_ORKER_TASK_LOG_EXPORT, the deployment-wide switch a
// selection's TaskLogs ENABLED/DISABLED overrides either way (Open documents
// the exact precedence). maxLineBytes <= 0 uses defaultMaxLine.
func NewTaskLogExporterForSinks(s *Sinks, maxLineBytes int, globalExport bool) *TaskLogExporter {
	if maxLineBytes <= 0 {
		maxLineBytes = defaultMaxLine
	}
	return &TaskLogExporter{
		sinks:        s,
		maxLine:      maxLineBytes,
		globalExport: globalExport,
	}
}

// NewTaskLogExporter is NewTaskLogExporterForSinks with one sink: p is both
// the only destination and the global switch's own home (globalExport is
// implicitly true — every call this constructor ever received predates
// per-run selection, so its whole existence means "export"). It keeps
// returning nil when p's Config has logs off, exactly as before this
// package had a Sinks: a caller that always calls Open and always gets a
// working writer back would otherwise need its own "is telemetry on" branch.
func NewTaskLogExporter(p *Providers, maxLineBytes int) *TaskLogExporter {
	if p == nil || !p.cfg.LogsEnabled() {
		return nil
	}
	return NewTaskLogExporterForSinks(sinksOf(p), maxLineBytes, true)
}

// routeFor returns p's cached logger and instruments, building them once.
func (e *TaskLogExporter) routeFor(p *Providers) *taskLogRoute {
	r, _ := e.cache.getOrBuild(p, func() (*taskLogRoute, error) {
		r := &taskLogRoute{
			logger:       p.Logs.Logger(ScopeName),
			emittedOpt:   metric.WithAttributeSet(attribute.NewSet(attribute.String(AttrOutcome, OutcomeEmitted))),
			truncatedOpt: metric.WithAttributeSet(attribute.NewSet(attribute.String(AttrOutcome, OutcomeTruncated))),
		}
		if inst, err := NewOrkerInstruments(p); err == nil {
			r.inst = inst
		}
		return r, nil
	})
	return r
}

// Open starts exporting one attempt's stream, or declines to. The decision
// has two independent parts:
//
//  1. Is this attempt's export on at all? Start from globalExport, then let
//     the attempt's own selection override it either way:
//     TASK_LOG_EXPORT_ENABLED forces it on even if the deployment default is
//     off, TASK_LOG_EXPORT_DISABLED forces it off even if the deployment
//     default is on, and TASK_LOG_EXPORT_UNSPECIFIED leaves globalExport as
//     the answer. Off means Open returns nil.
//  2. If it is on, which sink? sel.GetSink() through Sinks.For — "" or an
//     unrecognized name is the default sink. If that sink's own Providers has
//     logs switched off (!p.Config().LogsEnabled(), e.g. a named sink that
//     names no logs_endpoint), Open still returns nil: a selection can choose
//     a destination, not force logs on for a sink that was never configured
//     to carry them.
//
// A nil receiver — no exporter was ever built — returns a nil *TaskLogWriter,
// and every method on a nil *TaskLogWriter is a no-op: the Orker holds it
// behind an interface, so a nil pointer inside a non-nil interface value must
// still be safe to call.
//
// ctx is captured with cancellation stripped (context.WithoutCancel): a task
// that is cancelled or times out still emits its teardown lines, and this
// exporter must never be the reason those lines are lost.
func (e *TaskLogExporter) Open(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32) *TaskLogWriter {
	if e == nil {
		return nil
	}
	sel := in.GetTelemetry()
	enabled := e.globalExport
	switch sel.GetTaskLogs() {
	case orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED:
		enabled = true
	case orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED:
		enabled = false
	}
	if !enabled {
		return nil
	}

	p := e.sinks.For(sel.GetSink())
	if p == nil || !p.Config().LogsEnabled() {
		return nil
	}
	route := e.routeFor(p)

	attrs := []attribute.KeyValue{
		attribute.String(AttrRunID, in.GetRunId()),
		attribute.String(AttrTaskID, in.GetTaskId()),
		attribute.Int64(AttrAttempt, int64(attempt)),
		attribute.String(AttrNamespace, in.GetNamespace()),
		attribute.String(AttrOrkflowName, in.GetOrkflowName()),
		attribute.String(AttrOrkflowVersion, in.GetOrkflowVersion()),
		attribute.String(AttrOrkflowDigest, in.GetOrkflowDefinitionDigest()),
		attribute.String(AttrPool, in.GetPool()),
	}
	// A selection's attributes ride every log record verbatim: unlike a
	// metric series, one more label on a log line costs nothing extra per
	// distinct value, so there is no cardinality trade-off to gate here (contrast
	// EngineObserver and OrkerObserver, which only add them to metrics under
	// Cardinality.High).
	attrs = append(attrs, selectionAttrs(sel.GetAttributes())...)

	return &TaskLogWriter{
		ctx:          context.WithoutCancel(ctx),
		logger:       route.logger,
		maxLine:      e.maxLine,
		inst:         route.inst,
		attrs:        attrs,
		emittedOpt:   route.emittedOpt,
		truncatedOpt: route.truncatedOpt,
	}
}

// TaskLogWriter exports one attempt's lines as they arrive. It is not safe
// for concurrent use by multiple goroutines writing interleaved lines to the
// SAME writer — the Orker gives each attempt exactly one, matching the single
// LOG stream logstream.Logger already assumes.
type TaskLogWriter struct {
	ctx     context.Context
	logger  log.Logger
	maxLine int
	inst    *OrkerInstruments
	attrs   []attribute.KeyValue
	closed  bool

	// emittedOpt and truncatedOpt are the route's two precomputed outcome
	// attribute sets for TaskLogRecords — built once per sink (routeFor),
	// not once per Write.
	emittedOpt   metric.MeasurementOption
	truncatedOpt metric.MeasurementOption
}

// Write hands one line of the attempt's output to the log provider. It never
// blocks beyond the SDK's own enqueue (a BatchProcessor's Emit drops on a full
// queue rather than wait) and never panics — a malformed or oversized line is
// truncated, not refused, because telemetry must never fail a task.
func (w *TaskLogWriter) Write(line string) {
	if w == nil || w.closed || w.logger == nil {
		return
	}
	line = strings.TrimSuffix(line, "\n")

	stream := StreamStdout
	text := line
	if rest, ok := strings.CutPrefix(line, orkLinePrefix); ok {
		stream = StreamOrk
		text = rest
	} else if decoded, stderr := logstream.DecodeStreamLine(line); stderr {
		stream = StreamStderr
		text = decoded
	} else {
		text = decoded
	}

	opt := w.emittedOpt
	if len(text) > w.maxLine {
		text = text[:w.maxLine]
		opt = w.truncatedOpt
	}

	now := time.Now()
	var rec log.Record
	rec.SetTimestamp(now)
	rec.SetObservedTimestamp(now)
	rec.SetSeverity(log.SeverityInfo)
	rec.SetSeverityText("INFO")
	rec.SetBody(attribute.StringValue(text))
	rec.AddAttributes(w.attrs...)
	rec.AddAttributes(attribute.String(AttrStream, stream))

	w.logger.Emit(w.ctx, rec)
	if w.inst != nil {
		w.inst.TaskLogRecords.Add(w.ctx, 1, opt)
	}
}

// Close is idempotent and needs no flush: the BatchProcessor behind the log
// provider owns its own flushing and Providers.Shutdown drains it, so Close
// just stops this writer from emitting after the attempt is done.
func (w *TaskLogWriter) Close() {
	if w == nil {
		return
	}
	w.closed = true
}

// orkLinePrefix is the Orker's own voice on a task's log stream — its
// lifecycle notices, never a marked stdout/stderr line (pkg/logstream).
const orkLinePrefix = "[ork] "
