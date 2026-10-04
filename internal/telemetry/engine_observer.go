package telemetry

import (
	"context"
	"log/slog"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// EngineObserver turns the engine's lifecycle events into metrics and one
// structured log line per event. It implements engine.EventObserver
// structurally: this package does not import internal/engine (workflow code
// must never import OpenTelemetry, AGENTS.md, and a dependency the other
// direction would make that easy to get wrong by accident), so the interface
// is satisfied by shape, not by declaration. cmd/ork's wiring asserts the
// shape at compile time with `var _ engine.EventObserver = (*EngineObserver)(nil)`.
//
// Every method must not block: ObserveEvent runs on the projection activity a
// run's progress is waiting on (engine.EventActivities.WithEventObserver).
//
// A run's metrics and log line are a per-run signal (§ package doc): they go
// to whatever sink run.GetTelemetry().GetSink() names, not necessarily the
// process default. The engine's OWN process-level signals — its slog output
// on any other logger, the Temporal SDK's own metrics, /metrics — are not
// touched by this type at all and stay on the default sink exclusively.
type EngineObserver struct {
	sinks  *Sinks
	logger *slog.Logger

	cache providerCache[*EngineInstruments]
}

// NewEngineObserverForSinks builds an EngineObserver that picks s.For(run's
// sink) fresh for every event, caching each sink's EngineInstruments the
// first time it is needed (never once per event — a run's events arrive one
// at a time over the run's whole lifetime, and building instruments is not
// free). logger is used directly (not through any Providers' Logs) because
// the whole point of the log line this type emits is to be findable by a
// human grepping stdout or Loki with run_id and event_type, the same logger
// every other engine log line already goes through — nil selects
// slog.Default().
//
// The default sink's instruments are built eagerly, here, so a construction
// error (an instrument name collision, in practice) surfaces at boot instead
// of on the first event — matching NewEngineObserver's existing contract.
func NewEngineObserverForSinks(s *Sinks, logger *slog.Logger) (*EngineObserver, error) {
	if logger == nil {
		logger = slog.Default()
	}
	o := &EngineObserver{sinks: s, logger: logger}
	if _, err := o.instFor(s.Default()); err != nil {
		return nil, err
	}
	return o, nil
}

// NewEngineObserver is NewEngineObserverForSinks with one sink: every event's
// metrics and log line go to p regardless of what run.GetTelemetry() names,
// because there is nowhere else to send them.
func NewEngineObserver(p *Providers, logger *slog.Logger) (*EngineObserver, error) {
	return NewEngineObserverForSinks(sinksOf(p), logger)
}

// instFor returns p's cached EngineInstruments, building them once.
func (o *EngineObserver) instFor(p *Providers) (*EngineInstruments, error) {
	return o.cache.getOrBuild(p, func() (*EngineInstruments, error) { return NewEngineInstruments(p) })
}

// sinkFor is the Providers a run's own telemetry selection names — "" or an
// unrecognized sink is the default, and a nil run is also the default
// (Sinks.For and the proto getters are all nil-safe).
func (o *EngineObserver) sinkFor(run *orkstackv1.Run) *Providers {
	return o.sinks.For(run.GetTelemetry().GetSink())
}

// ObserveEvent records metrics for a terminal run or task transition and logs
// every event, of every type. It never panics on a nil run or a task id
// missing from run.Tasks: metrics for that dimension are simply skipped, and
// the log line still carries whatever it has.
func (o *EngineObserver) ObserveEvent(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run) {
	if o == nil {
		return
	}
	o.recordMetric(ctx, event, run)
	o.log(ctx, event, run)
}

func (o *EngineObserver) recordMetric(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run) {
	inst, err := o.instFor(o.sinkFor(run))
	if err != nil || inst == nil {
		// A sink whose instruments failed to build once keeps failing the
		// same way forever (the meter name collision that causes it is
		// static), so there is nothing left to retry; this event's metrics
		// are simply skipped, same as a nil run already was before sinks
		// existed.
		return
	}
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		o.recordRun(ctx, run, inst)
	case orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:
		o.recordTask(ctx, event, run, inst)
	default:
		// RUN_CREATED, TASK_SCHEDULED, TASK_STARTED and TASK_LOG are not
		// terminal states of anything: they are logged below, but they do
		// not move a counter or close out a duration.
	}
}

// selectionMetricAttrs is a run's telemetry selection's attributes, gated by
// the SAME rule Cardinality already applies to run_id and task_id: a
// namespace admin's chosen attributes are bounded by validation (at most 16
// keys, 32 bytes each — see the DSL validator), but each distinct VALUE is
// still its own metric time series, exactly the cost c.High exists to make
// opt-in. So these ride a metric only when inst.Cardinality.High; the log
// line below carries them unconditionally instead (log.log).
func selectionMetricAttrs(inst *EngineInstruments, run *orkstackv1.Run) []attribute.KeyValue {
	if inst == nil || !inst.Cardinality.High {
		return nil
	}
	return selectionAttrs(run.GetTelemetry().GetAttributes())
}

func (o *EngineObserver) recordRun(ctx context.Context, run *orkstackv1.Run, inst *EngineInstruments) {
	if run == nil || inst == nil {
		return
	}
	attrs := inst.Cardinality.RunAttrs(run, "",
		attribute.String(AttrStatus, statusLabel(run.GetStatus())))
	attrs = append(attrs, selectionMetricAttrs(inst, run)...)
	inst.Runs.Add(ctx, 1, metric.WithAttributes(attrs...))

	started := run.GetStartedAt()
	if started == nil {
		started = run.GetCreatedAt()
	}
	finished := run.GetFinishedAt()
	if started != nil && finished != nil {
		seconds := finished.AsTime().Sub(started.AsTime()).Seconds()
		if seconds >= 0 {
			inst.RunDuration.Record(ctx, seconds, metric.WithAttributes(attrs...))
		}
	}
}

func (o *EngineObserver) recordTask(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run, inst *EngineInstruments) {
	if run == nil || inst == nil {
		return
	}
	// Resolved once and reused below: RunAttrs would otherwise re-scan
	// run.Tasks for the same event, once per call. TaskRunAttrs takes the
	// already-resolved task instead.
	task := findTaskRun(run, event.GetTaskId())
	if task == nil {
		return
	}
	status := statusLabel(task.GetStatus())
	category := ""
	if taskErr := task.GetError(); taskErr != nil {
		category = failureCategoryLabel(taskErr.GetCategory())
	}
	selAttrs := selectionMetricAttrs(inst, run)

	attrs := inst.Cardinality.TaskRunAttrs(run, task,
		attribute.String(AttrStatus, status),
		attribute.String(AttrFailureCategory, category))
	attrs = append(attrs, selAttrs...)
	inst.Tasks.Add(ctx, 1, metric.WithAttributes(attrs...))

	durationAttrs := inst.Cardinality.TaskRunAttrs(run, task,
		attribute.String(AttrStatus, status))
	durationAttrs = append(durationAttrs, selAttrs...)
	// Dispatch to finish: finished_at − scheduled_at. started_at is the
	// Orker's start of the deciding attempt, and measuring from it would
	// change this instrument's meaning under an unchanged name; it is the
	// fallback only for a snapshot with no scheduled_at (recorded before that
	// field existed), where it still held the dispatch instant.
	started := task.GetScheduledAt()
	if started == nil {
		started = task.GetStartedAt()
	}
	if finished := task.GetFinishedAt(); started != nil && finished != nil {
		seconds := finished.AsTime().Sub(started.AsTime()).Seconds()
		if seconds >= 0 {
			inst.TaskDuration.Record(ctx, seconds, metric.WithAttributes(durationAttrs...))
		}
	}
	// A fan-out's own row has no attempt — its items do. Record one
	// observation per item, from the snapshot the node's terminal event
	// carries, under the node's attributes (never an item label: a series per
	// item is the cardinality TaskAttrs exists to prevent).
	if task.GetEach() != nil {
		for _, item := range task.GetItems() {
			if item.GetAttempt() > 0 {
				inst.TaskAttempts.Record(ctx, int64(item.GetAttempt()), metric.WithAttributes(durationAttrs...))
			}
		}
		return
	}
	inst.TaskAttempts.Record(ctx, int64(task.GetAttempt()), metric.WithAttributes(durationAttrs...))
}

// ObserveRefusal records one admission refusal on the sink telemetry names
// — o.sinks.For(telemetry.GetSink()), the lookup sinkFor performs on
// run.GetTelemetry() for a run, applied to the selection the refused run
// would have carried (engine.RefusalTelemetry). It moves RunsRefused and
// nothing else: no Runs increment, no duration, no log line (the API logs
// the refusal itself, once, at Info). Nil-safe on o and telemetry (a nil
// selection is the default sink with no selection attributes, exactly as a
// nil run is to sinkFor), and never blocks. It satisfies
// engine.AdmissionObserver by shape.
//
// telemetry's attributes are already stamped ork.ns.<key> / ork.label.<key>
// by the engine, so they are added verbatim through selectionAttrs — under
// the same Cardinality.High gate they ride Runs under — and this method
// spells no prefix of its own.
func (o *EngineObserver) ObserveRefusal(
	ctx context.Context,
	namespace string,
	telemetry *orkstackv1.TelemetrySelection,
	orkflow string,
	reason string,
) {
	if o == nil {
		return
	}
	inst, err := o.instFor(o.sinks.For(telemetry.GetSink()))
	if err != nil || inst == nil {
		// Same reasoning as recordMetric: a sink whose instruments failed to
		// build once keeps failing, so there is nothing to retry.
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String(AttrNamespace, namespace),
		attribute.String(AttrRefusalReason, refusalReasonLabel(reason)),
	}
	if inst.Cardinality.High {
		attrs = append(attrs, attribute.String(AttrOrkflowName, orkflow))
		attrs = append(attrs, selectionAttrs(telemetry.GetAttributes())...)
	}
	inst.RunsRefused.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// refusalReasonLabel turns NAMESPACE_HELD into "namespace_held": the one-rule
// lowercasing statusLabel and failureCategoryLabel apply to their enums, so
// the code an operator reads in the log line and the value they filter on
// differ by case alone.
func refusalReasonLabel(reason string) string {
	return strings.ToLower(reason)
}

// log emits one structured line per event, at Debug for TASK_LOG (a run can
// carry thousands of these; they belong in the log body, not shouted at Info)
// and, for everything else, at Info only when the run's resolved sink has
// logs switched on — a deployment that turned on only metrics or traces
// should not gain a per-event stderr line it never asked for — Debug
// otherwise, so the line still exists for an operator who raised the process
// log level to see it.
func (o *EngineObserver) log(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run) {
	level := slog.LevelDebug
	if event.GetType() != orkstackv1.EventType_EVENT_TYPE_TASK_LOG {
		if p := o.sinkFor(run); p != nil && p.Config().LogsEnabled() {
			level = slog.LevelInfo
		}
	}
	if !o.logger.Enabled(ctx, level) {
		return
	}
	args := []any{
		slog.String("run_id", event.GetRunId()),
		slog.String("event_type", eventTypeLabel(event.GetType())),
		slog.String("status", statusLabel(event.GetStatus())),
	}
	if taskID := event.GetTaskId(); taskID != "" {
		args = append(args, slog.String("task_id", taskID))
	}
	if run != nil {
		args = append(args,
			slog.String("namespace", run.GetNamespace()),
			slog.String("orkflow", run.GetOrkflowName()))
	}
	// Unlike the metric attributes above, a selection's attributes ride the
	// log line unconditionally: one more field on one log record has none of
	// a metric label's per-value time-series cost, so there is no
	// cardinality trade-off here to gate on (selectionMetricAttrs).
	for _, kv := range selectionAttrs(run.GetTelemetry().GetAttributes()) {
		args = append(args, slog.String(string(kv.Key), kv.Value.AsString()))
	}
	for _, attr := range TraceAttrs(ctx) {
		args = append(args, attr)
	}
	o.logger.Log(ctx, level, "run event", args...)
}

// statusLabel turns Status_STATUS_SUCCEEDED into "succeeded": the enum's own
// short name, lowercased, which is what a status= label should read like on a
// dashboard rather than the wire's shouting SCREAMING_CASE.
func statusLabel(status orkstackv1.Status) string {
	return trimEnumPrefix(status.String(), "STATUS_")
}

// eventTypeLabel turns EventType_EVENT_TYPE_RUN_STARTED into "run_started".
func eventTypeLabel(eventType orkstackv1.EventType) string {
	return trimEnumPrefix(eventType.String(), "EVENT_TYPE_")
}

// failureCategoryLabel turns FailureCategory_FAILURE_CATEGORY_TIMED_OUT into
// "timed_out", and the unspecified zero value into "" — a successful task has
// no category, and neither does one whose Error carried none.
func failureCategoryLabel(category orkstackv1.FailureCategory) string {
	if category == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		return ""
	}
	return trimEnumPrefix(category.String(), "FAILURE_CATEGORY_")
}

func trimEnumPrefix(name, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}
