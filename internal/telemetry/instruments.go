package telemetry

import (
	"sort"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// AttrOutcome is the label every ork_* counter in this package carries: what
// happened to the thing it counts, never an identifier of the thing itself
// (that would be the high-cardinality mistake this package exists to avoid).
const AttrOutcome = "outcome"

// AttrStatus and AttrFailureCategory label EngineInstruments' run and task
// counters. Unlike the ork.* keys in setup.go, these are not OrkStack-specific
// identity — "status" and "failure_category" are the vocabulary a dashboard
// author already reaches for — so they are spelled without the ork. prefix.
const (
	AttrStatus          = "status"
	AttrFailureCategory = "failure_category"
)

// AttrRefusalReason labels ork.runs.refused with why the gate said no. It
// takes the ork. prefix, unlike status and failure_category above, on the
// rule those two state for themselves: they are the vocabulary a dashboard
// author already reaches for, while a refusal reason is an OrkStack code
// (NAMESPACE_HELD, MAX_RUNNING_REACHED) — the same kind of value the Orker
// labels ork.failure.reason with (orker_spans.go), and it follows that
// precedent, not the bare one.
const AttrRefusalReason = "ork.refusal.reason"

// Outcomes recorded on TaskLogExporter's counters.
const (
	OutcomeEmitted   = "emitted"
	OutcomeTruncated = "truncated"
	OutcomeOK        = "ok"
	OutcomeError     = "error"
)

// Cardinality decides which per-task labels an ork_* metric carries. High is
// the ORK_TELEMETRY_HIGH_CARDINALITY switch (Config.HighCardinality): off by
// default because a run_id or task_id label is a new time series per run, a
// cost only an operator who wants per-run metrics should pay.
type Cardinality struct{ High bool }

// TaskAttrs returns base plus the identity of in as attributes, at the
// cardinality c allows. ork.pool and ork.namespace are always included — both
// are bounded sets, drawn from a deployment's own pool and namespace names —
// while ork.run_id, ork.task_id, ork.orkflow.name and ork.orkflow.version are
// added only when c.High, because each is effectively unbounded.
func (c Cardinality) TaskAttrs(in *orkstackv1.ExecuteTaskInput, base ...attribute.KeyValue) []attribute.KeyValue {
	attrs := append([]attribute.KeyValue{}, base...)
	if in == nil {
		return attrs
	}
	if c.High {
		attrs = append(attrs,
			attribute.String(AttrRunID, in.GetRunId()),
			attribute.String(AttrTaskID, in.GetTaskId()),
			attribute.String(AttrOrkflowName, in.GetOrkflowName()),
			attribute.String(AttrOrkflowVersion, in.GetOrkflowVersion()),
		)
	}
	attrs = append(attrs,
		attribute.String(AttrPool, in.GetPool()),
		attribute.String(AttrNamespace, in.GetNamespace()),
	)
	return attrs
}

// OrkerInstruments is the Orker's own metrics — as opposed to the Temporal
// SDK's, which ride TemporalMetricsHandler. Every field is safe to use on
// disabled Providers: the meter behind it is the no-op implementation, so
// Add costs a function call and produces no series.
type OrkerInstruments struct {
	// TaskLogRecords counts task log lines TaskLogWriter handed to the log
	// provider, labelled outcome=emitted|truncated.
	TaskLogRecords metric.Int64Counter
	// TaskPhaseDuration records how long one §7.1 phase within a task attempt
	// took, labelled ork.phase plus Cardinality's task identity. Recorded by
	// OrkerObserver each time a phase ends (orker_spans.go).
	TaskPhaseDuration metric.Float64Histogram
	// Tasks counts completed task attempts, labelled outcome=succeeded|
	// failed|canceled|adopted and ork.failure.reason (empty on success).
	Tasks metric.Int64Counter
	// TaskDuration records one whole task attempt's wall-clock duration, from
	// OrkerObserver.Begin to its End, labelled the same as Tasks.
	TaskDuration metric.Float64Histogram
	// CommitAdopted counts attempts that found an existing durable commit and
	// returned it without running the container (§5.4).
	CommitAdopted metric.Int64Counter
}

// meterFor returns p's meter, or the no-op meter when p is nil — the one
// place every instrument constructor in this package (and newOrkerRoute's
// nil branch, orker_spans.go) gets its meter from.
func meterFor(p *Providers) metric.Meter {
	if p == nil {
		return metricnoop.NewMeterProvider().Meter(ScopeName)
	}
	return p.Meter.Meter(ScopeName)
}

// NewOrkerInstruments creates the Orker's instruments from p's meter
// provider. It works, as a no-op, on disabled Providers: the returned
// counters simply record nothing.
func NewOrkerInstruments(p *Providers) (*OrkerInstruments, error) {
	meter := meterFor(p)
	records, err := meter.Int64Counter("ork.orker.task_log.records",
		metric.WithDescription("Task log lines exported as OTel log records, by outcome."),
		metric.WithUnit("{line}"))
	if err != nil {
		return nil, err
	}
	phaseDuration, err := meter.Float64Histogram("ork.orker.task_phase.duration",
		metric.WithDescription("Duration of one §7.1 phase within a task attempt, by phase."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	tasks, err := meter.Int64Counter("ork.orker.tasks",
		metric.WithDescription("Completed task attempts, by outcome and failure reason."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	taskDuration, err := meter.Float64Histogram("ork.orker.task.duration",
		metric.WithDescription("Duration of one whole task attempt, from Begin to End."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	commitAdopted, err := meter.Int64Counter("ork.orker.commit_adopted",
		metric.WithDescription("Attempts that adopted an existing durable commit instead of running the container."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	return &OrkerInstruments{
		TaskLogRecords:    records,
		TaskPhaseDuration: phaseDuration,
		Tasks:             tasks,
		TaskDuration:      taskDuration,
		CommitAdopted:     commitAdopted,
	}, nil
}

// TaskRunAttrs is RunAttrs' building block: it takes the resolved *TaskRun
// directly instead of a taskID it would have to look up itself, so a caller
// that already scanned run.Tasks for another reason (recordTask needs the
// task for its status and failure category too) does that scan once per
// event, not once per RunAttrs call. task may be nil (no such task, or none
// asked for), matching a taskID that resolved to nothing.
func (c Cardinality) TaskRunAttrs(run *orkstackv1.Run, task *orkstackv1.TaskRun, base ...attribute.KeyValue) []attribute.KeyValue {
	attrs := append([]attribute.KeyValue{}, base...)
	if run == nil {
		return attrs
	}
	var pool, taskID string
	if task != nil {
		pool = task.GetPool()
		taskID = task.GetTaskId()
	}
	if c.High {
		attrs = append(attrs,
			attribute.String(AttrRunID, run.GetId()),
			attribute.String(AttrOrkflowName, run.GetOrkflowName()),
			attribute.String(AttrOrkflowVersion, run.GetOrkflowVersion()),
		)
		if taskID != "" {
			attrs = append(attrs, attribute.String(AttrTaskID, taskID))
		}
	}
	attrs = append(attrs, attribute.String(AttrNamespace, run.GetNamespace()))
	if pool != "" {
		attrs = append(attrs, attribute.String(AttrPool, pool))
	}
	return attrs
}

// RunAttrs returns base plus a run's identity as attributes, at the
// cardinality c allows. It is TaskAttrs' counterpart for the engine side,
// where the identity in hand is a *orkstackv1.Run and a taskID rather than an
// ExecuteTaskInput: ork.pool and ork.namespace are always included, while
// ork.run_id, ork.task_id, ork.orkflow.name and ork.orkflow.version are added
// only when c.High. taskID is looked up in run.Tasks fresh, here — a caller
// that already holds the *TaskRun (recordTask) calls TaskRunAttrs directly
// instead, to avoid scanning run.Tasks twice for the same event.
func (c Cardinality) RunAttrs(run *orkstackv1.Run, taskID string, base ...attribute.KeyValue) []attribute.KeyValue {
	return c.TaskRunAttrs(run, findTaskRun(run, taskID), base...)
}

// selectionAttrs turns a TelemetrySelection's Attributes map into attribute
// pairs, sorted by key so a log line or metric series is deterministic
// regardless of Go's map iteration order. The engine has already stamped
// every key with ork.ns. or ork.label. (a namespace admin never writes an
// unprefixed key), so this adds them verbatim; it is not this helper's job to
// decide WHERE they land — a log record always carries them, a metric only
// under Cardinality.High — that trade-off is each router's own call
// (engine_observer.go, orker_spans.go, tasklog.go).
func selectionAttrs(attributes map[string]string) []attribute.KeyValue {
	if len(attributes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(attributes))
	for k := range attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	attrs := make([]attribute.KeyValue, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, attribute.String(k, attributes[k]))
	}
	return attrs
}

// findTaskRun looks up a run's TaskRun by task id. Run.Tasks is a slice, not a
// map, so this is a linear scan; a run's task count is small enough (the
// activities of one Orkflow) that this is not a hot path worth indexing.
func findTaskRun(run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	if run == nil || taskID == "" {
		return nil
	}
	for _, task := range run.GetTaskRuns() {
		if task.GetTaskId() == taskID {
			return task
		}
	}
	return nil
}

// EngineInstruments is the engine's own run and task metrics — as opposed to
// OrkerInstruments, which the Orker records for the task logs it exports.
// Every field is safe to use on disabled Providers: the meter behind it is
// the no-op implementation, so Add and Record cost a function call and
// produce no series.
type EngineInstruments struct {
	// Runs counts terminal run transitions, labelled ork.namespace and
	// status=succeeded|failed|canceled.
	Runs metric.Int64Counter
	// RunDuration records a run's wall-clock seconds from StartedAt (falling
	// back to CreatedAt for a run that never started) to FinishedAt, labelled
	// the same as Runs.
	RunDuration metric.Float64Histogram
	// Tasks counts terminal task transitions, labelled ork.namespace,
	// ork.pool, status=succeeded|failed and failure_category (empty for a
	// success).
	Tasks metric.Int64Counter
	// TaskDuration records a task's wall-clock seconds from StartedAt to
	// FinishedAt, labelled ork.namespace, ork.pool and status.
	TaskDuration metric.Float64Histogram
	// TaskAttempts records the attempt number a terminal task event carries,
	// labelled ork.namespace and ork.pool — the width of the retry tail a
	// namespace or pool is paying for.
	TaskAttempts metric.Int64Histogram
	// RunsRefused counts submissions the admission gate refused, labelled
	// ork.namespace and ork.refusal.reason. A refused submission is not a
	// run and is never also counted on Runs.
	RunsRefused metric.Int64Counter
	// Continuations counts a run's continuations, labelled ork.namespace and
	// outcome=written|write_failed|over_cap|restored, or a failed restore's
	// RUN_STATE_* code. Recorded by the run's own local activities, never by
	// workflow code.
	Continuations metric.Int64Counter
	// ContinuationStateBytes records the stored size of what a switch wrote,
	// labelled ork.namespace and kind=checkpoint|events|snapshot.
	ContinuationStateBytes metric.Int64Histogram
	// ContinuationDuration records the seconds a switch's write or a
	// bootstrap's whole restore took, labelled ork.namespace and
	// phase=write|restore.
	ContinuationDuration metric.Float64Histogram
	// ContinuationHoldDuration records how long a run held new dispatch
	// before it switched (0 for a natural idle point), labelled
	// ork.namespace.
	ContinuationHoldDuration metric.Float64Histogram
	// ContinuationReadAttempts records the attempt each successful bootstrap
	// read succeeded on, labelled ork.namespace.
	ContinuationReadAttempts metric.Int64Histogram
	// Cardinality is the label policy EngineObserver applies through
	// RunAttrs above.
	Cardinality Cardinality
}

// AttrContinuationKind and AttrContinuationPhase label the continuation
// instruments: which object a switch wrote, and whether a duration is a
// write's or a restore's.
const (
	AttrContinuationKind  = "kind"
	AttrContinuationPhase = "phase"
)

// NewEngineInstruments creates the engine's instruments from p's meter
// provider. It works, as a no-op, on disabled Providers: the returned
// instruments simply record nothing.
func NewEngineInstruments(p *Providers) (*EngineInstruments, error) {
	var (
		meter    metric.Meter
		highCard Cardinality
	)
	if p != nil {
		meter = p.Meter.Meter(ScopeName)
		highCard = Cardinality{High: p.cfg.HighCardinality}
	} else {
		meter = metricnoop.NewMeterProvider().Meter(ScopeName)
	}
	runs, err := meter.Int64Counter("ork.runs",
		metric.WithDescription("Runs reaching a terminal state, by namespace and status."),
		metric.WithUnit("{run}"))
	if err != nil {
		return nil, err
	}
	runDuration, err := meter.Float64Histogram("ork.run.duration",
		metric.WithDescription("Run wall-clock duration from start to finish, by namespace and status."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	tasks, err := meter.Int64Counter("ork.tasks",
		metric.WithDescription("Task attempts reaching a terminal state, by namespace, pool, status and failure category."),
		metric.WithUnit("{task}"))
	if err != nil {
		return nil, err
	}
	taskDuration, err := meter.Float64Histogram("ork.task.duration",
		metric.WithDescription("Task wall-clock duration from start to finish, by namespace, pool and status."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	taskAttempts, err := meter.Int64Histogram("ork.task.attempts",
		metric.WithDescription("Attempt number of a terminal task event, by namespace and pool."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	runsRefused, err := meter.Int64Counter("ork.runs.refused",
		metric.WithDescription("Submissions the admission gate refused, by namespace and reason."),
		metric.WithUnit("{submission}"))
	if err != nil {
		return nil, err
	}
	continuations, err := meter.Int64Counter("ork.run.continuations",
		metric.WithDescription("Run continuations into a fresh history, by namespace and outcome."),
		metric.WithUnit("{switch}"))
	if err != nil {
		return nil, err
	}
	stateBytes, err := meter.Int64Histogram("ork.run.continuation.state.bytes",
		metric.WithDescription("Stored size of what a run's continuation wrote, by namespace and kind."),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	continuationDuration, err := meter.Float64Histogram("ork.run.continuation.duration",
		metric.WithDescription("Seconds a continuation's write or a restore took, by namespace and phase."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	holdDuration, err := meter.Float64Histogram("ork.run.continuation.hold.duration",
		metric.WithDescription("Seconds a run held new dispatch before it continued, by namespace."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	readAttempts, err := meter.Int64Histogram("ork.run.continuation.read.attempts",
		metric.WithDescription("Attempt a restore's successful read succeeded on, by namespace."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	return &EngineInstruments{
		Continuations:            continuations,
		ContinuationStateBytes:   stateBytes,
		ContinuationDuration:     continuationDuration,
		ContinuationHoldDuration: holdDuration,
		ContinuationReadAttempts: readAttempts,
		Runs:                     runs,
		RunDuration:              runDuration,
		Tasks:                    tasks,
		TaskDuration:             taskDuration,
		TaskAttempts:             taskAttempts,
		RunsRefused:              runsRefused,
		Cardinality:              highCard,
	}, nil
}
