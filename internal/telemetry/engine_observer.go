package telemetry

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/enumname"
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
	// twins are the operator twins, on the default sink (twins.go).
	twins *TwinInstruments
	// instWarned dedupes instrumentsOf's Warn: one per sink.
	instWarned sync.Map
	// policy routes the namespaced records no run selection names (a
	// continuation); nil routes them only when there is one sink.
	policy *PolicyCache
}

// WithPolicyCache makes c the destination of o's namespaced records that no
// run selection names. It returns o.
func (o *EngineObserver) WithPolicyCache(c *PolicyCache) *EngineObserver {
	if o != nil {
		o.policy = c
	}
	return o
}

// namespaceSink is the Providers a namespaced record no run selection names
// goes to: the policy cache's answer, or with no cache the one sink there is.
// It never falls back to the default sink; a drop is counted under signal.
func (o *EngineObserver) namespaceSink(ctx context.Context, namespace, signal string) (*Providers, bool) {
	if o.policy != nil {
		return o.policy.For(ctx, namespace, signal)
	}
	if o.sinks.anyName {
		return o.sinks.Default(), true
	}
	o.sinks.DropRecords(ctx, signal, DropReasonUnrouted, 1)
	return nil, false
}

// NewEngineObserverForSinks builds an EngineObserver that picks s.ForNamespaced(run's
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
	twins, err := NewTwinInstruments(s.Default())
	if err != nil {
		return nil, err
	}
	o.twins = twins
	return o, nil
}

// NewEngineObserver is NewEngineObserverForSinks with one sink: every event's
// metrics and log line go to p regardless of what run.GetTelemetry() names,
// because there is nowhere else to send them.
func NewEngineObserver(p *Providers, logger *slog.Logger) (*EngineObserver, error) {
	return NewEngineObserverForSinks(sinksOf(p), logger)
}

// instrumentsOf is p's instruments for one record, or nil when they cannot
// be built: the record is then dropped and counted (instruments_failed), and
// the first failure for p is logged at Warn. It never fails the caller.
func (o *EngineObserver) instrumentsOf(ctx context.Context, p *Providers) *EngineInstruments {
	inst, err := o.instFor(p)
	if err == nil && inst != nil {
		return inst
	}
	o.sinks.DropRecords(ctx, SignalMetrics, DropReasonInstrumentsFailed, 1)
	if _, warned := o.instWarned.LoadOrStore(p, struct{}{}); !warned {
		o.logger.Warn("telemetry: a sink's engine instruments could not be built; its records are dropped and counted",
			"error", err)
	}
	return nil
}

// instFor returns p's cached EngineInstruments, building them once.
func (o *EngineObserver) instFor(p *Providers) (*EngineInstruments, error) {
	return o.cache.getOrBuild(p, func() (*EngineInstruments, error) { return NewEngineInstruments(p) })
}

// sinkFor is the Providers a run's own telemetry selection names: "" is the
// default, and so is a nil run (Sinks.ForNamespaced and the proto getters are
// all nil-safe). A sink this process does not declare is refused (ok false):
// the run's records are dropped and counted, never sent to the default sink.
func (o *EngineObserver) sinkFor(run *orkstackv1.Run) (*Providers, bool) {
	return o.sinks.ForNamespaced(run.GetTelemetry().GetSink())
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
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED,
		orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:
	default:
		// RUN_CREATED, TASK_SCHEDULED, TASK_STARTED and TASK_LOG are not
		// terminal states of anything: they are logged below, but they do
		// not move a counter or close out a duration.
		return
	}
	// The twin first: it is recorded for every namespace, whichever sink its
	// own records go to or whether they can go anywhere.
	o.recordTwin(ctx, event, run)
	p, ok := o.sinkFor(run)
	if !ok {
		o.sinks.DropRecords(ctx, SignalMetrics, DropReasonSinkUnknown, 1)
		return
	}
	inst := o.instrumentsOf(ctx, p)
	if inst == nil {
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
	}
}

// recordTwin records a terminal run or task event's operator twin: its
// status, and for a task its pool, failure category and cache outcome; never
// its namespace or a name.
func (o *EngineObserver) recordTwin(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run) {
	if o.twins == nil || run == nil {
		return
	}
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		o.twins.Runs.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrStatus, statusLabel(run.GetStatus()))))
	default:
		task := findTaskRun(run, event.GetTaskId())
		if task == nil {
			return
		}
		category := ""
		if taskErr := task.GetError(); taskErr != nil {
			category = failureCategoryLabel(taskErr.GetCategory())
		}
		attrs := []attribute.KeyValue{
			attribute.String(AttrStatus, statusLabel(task.GetStatus())),
			attribute.String(AttrFailureCategory, category),
		}
		if pool := task.GetPool(); pool != "" {
			attrs = append(attrs, attribute.String(AttrPool, pool))
		}
		if outcome := CacheOutcomeLabel(task.GetCache()); outcome != "" {
			attrs = append(attrs, attribute.String(AttrCacheOutcome, outcome))
		}
		o.twins.Tasks.Add(ctx, 1, metric.WithAttributes(attrs...))
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
	attrs = append(attrs, inst.Cardinality.orkflowAttr(run.GetOrkflowName())...)
	attrs = append(attrs, selectionMetricAttrs(inst, run)...)
	inst.Runs.Add(ctx, 1, metric.WithAttributes(attrs...))
	recordRunTimeout(ctx, run, inst)

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
	attrs = append(attrs, inst.Cardinality.orkflowAttr(run.GetOrkflowName())...)
	durationAttrs := inst.Cardinality.TaskRunAttrs(run, task,
		attribute.String(AttrStatus, status))
	durationAttrs = append(durationAttrs, selAttrs...)
	// A cached task's memo decision, ONLY when set: a hit's duration is its
	// lookup, and this label is what keeps it from reading as a fast
	// container. A deployment with no cached task keeps its series exactly.
	if outcome := CacheOutcomeLabel(task.GetCache()); outcome != "" {
		attrs = append(attrs, attribute.String(AttrCacheOutcome, outcome))
		durationAttrs = append(durationAttrs, attribute.String(AttrCacheOutcome, outcome))
	}
	inst.Tasks.Add(ctx, 1, metric.WithAttributes(attrs...))

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
	// A composed task has no attempt: it starts a child run, whose own tasks
	// are observed as that run's. Recording its 0 would pull the histogram
	// toward a number that describes nothing.
	if task.GetChild() != nil {
		return
	}
	inst.TaskAttempts.Record(ctx, int64(task.GetAttempt()), metric.WithAttributes(durationAttrs...))
}

// ObserveRefusal records one admission refusal on the sink telemetry names
// — o.sinks.ForNamespaced(telemetry.GetSink()), the lookup sinkFor performs on
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
	if o.twins != nil {
		o.twins.RunsRefused.Add(ctx, 1, metric.WithAttributes(
			attribute.String(AttrRefusalReason, refusalReasonLabel(reason))))
	}
	p, ok := o.sinks.ForNamespaced(telemetry.GetSink())
	if !ok {
		o.sinks.DropRecords(ctx, SignalMetrics, DropReasonSinkUnknown, 1)
		return
	}
	inst := o.instrumentsOf(ctx, p)
	if inst == nil {
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
	attrs = append(attrs, inst.Cardinality.orkflowAttr(orkflow)...)
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
		if p, ok := o.sinkFor(run); ok && p != nil && p.Config().LogsEnabled() {
			level = slog.LevelInfo
		}
	}
	if !o.logger.Enabled(ctx, level) {
		return
	}
	// sequence is the event's number in its run's stream: records can reach
	// a collector out of order (a retried publish, and a fan-out's items and
	// a plain task's start events, which are published without the run
	// waiting), so a consumer orders a run's records by it.
	args := []any{
		slog.String("run_id", event.GetRunId()),
		slog.Int64("sequence", event.GetSequence()),
		slog.String("event_type", eventTypeLabel(event.GetType())),
		slog.String("status", statusLabel(event.GetStatus())),
	}
	// The names the namespace's users chose — its task's id, its Orkflow's
	// name and its selection's attributes — ride this line, which goes to the
	// default sink with every process log, only when the run's own records
	// do too: a namespace on another sink keeps them there (OPEN-25, decided
	// 2026-11-16), and its line carries the namespace and opaque ids alone.
	var authored []any
	if taskID := event.GetTaskId(); taskID != "" {
		authored = append(authored, slog.String("task_id", taskID))
	}
	if run != nil {
		authored = append(authored, slog.String("orkflow", run.GetOrkflowName()))
	}
	for _, kv := range selectionAttrs(run.GetTelemetry().GetAttributes()) {
		authored = append(authored, slog.String(string(kv.Key), kv.Value.AsString()))
	}
	sink := run.GetTelemetry().GetSink()
	onDefault := o.sinks.anyName || sink == "" || sink == DefaultSink
	if run != nil {
		args = append(args, tenantAttrs(onDefault, run.GetNamespace(), authored...)...)
	} else if onDefault {
		args = append(args, authored...)
	}
	args = append(args, TraceAttrs(ctx)...)
	o.logger.Log(ctx, level, "run event", args...)
}

// statusLabel turns Status_STATUS_SUCCEEDED into "succeeded": the enum's own
// short name, lowercased, which is what a status= label should read like on a
// dashboard rather than the wire's shouting SCREAMING_CASE.
func statusLabel(status orkstackv1.Status) string {
	return strings.ToLower(enumname.Of(status))
}

// eventTypeLabel turns EventType_EVENT_TYPE_RUN_STARTED into "run_started".
func eventTypeLabel(eventType orkstackv1.EventType) string {
	return strings.ToLower(enumname.Of(eventType))
}

// failureCategoryLabel turns FailureCategory_FAILURE_CATEGORY_TIMED_OUT into
// "timed_out", and the unspecified zero value into "" — a successful task has
// no category, and neither does one whose Error carried none.
func failureCategoryLabel(category orkstackv1.FailureCategory) string {
	if category == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		return ""
	}
	return strings.ToLower(enumname.Of(category))
}

// CacheOutcomeLabel renders a memo decision for AttrCacheOutcome: "hit",
// "miss", "bypassed", or "" for none.
func CacheOutcomeLabel(c *orkstackv1.CacheResult) string {
	switch c.GetOutcome() {
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT:
		return "hit"
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS:
		return "miss"
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED:
		return "bypassed"
	}
	return ""
}

// The run-timeout counters (docs/DEPLOY.md §7, "Run and task metrics").
const (
	RunTimeoutsMetric        = "ork.run.timeouts"
	RunTimeoutHardEndsMetric = "ork.run.timeout.hard_ends"
)

// recordRunTimeout counts a terminal run its own timeout ended, and one
// whose failure handlers its hard end stopped, from the run's own
// Run.timeout. Only the namespace labels them (and a run id with high
// cardinality), as on every run metric; a run that declares no timeout
// records nothing.
func recordRunTimeout(ctx context.Context, run *orkstackv1.Run, inst *EngineInstruments) {
	timeout := run.GetTimeout()
	if timeout == nil || (!timeout.GetTimedOut() && !timeout.GetHardEndReached()) {
		return
	}
	attrs := metric.WithAttributes(inst.Cardinality.RunAttrs(run, "")...)
	if timeout.GetTimedOut() {
		inst.RunTimeouts.Add(ctx, 1, attrs)
	}
	if timeout.GetHardEndReached() {
		inst.RunTimeoutHardEnds.Add(ctx, 1, attrs)
	}
}
