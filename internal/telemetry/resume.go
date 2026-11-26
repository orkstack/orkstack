package telemetry

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The resume counters (docs/DEPLOY.md §7, "Run and task metrics").
const (
	ResumeAnomaliesMetric        = "ork.run.resume.anomalies"
	ResumesMetric                = "ork.run.resumes"
	ResumedMetric                = "ork.run.resumed"
	ResumeTasksMetric            = "ork.run.resume.tasks"
	ResumeHeadroomMetric         = "ork.run.resume.preflight.headroom"
	ResumeConfirmDurationMetric  = "ork.run.resume.confirm.duration"
	ResumeOrdinalMetric          = "ork.run.resume.ordinal"
	ResumeSinceFailureMetric     = "ork.run.resume.since_failure"
	ResumeReadBackDurationMetric = "ork.run.resume.readback.duration"
	ResumeReadBackAttemptsMetric = "ork.run.resume.readback.attempts"
	ResumeOutcomeMetric          = "ork.run.resume.outcome"
	ResumeActiveDurationMetric   = "ork.run.resume.active.duration"
	ResumeReadingBackMetric      = "ork.run.resume.reading_back"
	ResumeUnconfirmedMetric      = "ork.run.resume.unconfirmed"
	ResumeStalledMetric          = "ork.run.resume.stalled"

	// AttrResumeFromState is how the ending a resume set aside came about:
	// failed, or canceled_force for a force cancel.
	AttrResumeFromState = "from_state"
	// AttrResumeNth is which resume of its run an ending is of: 1, 2 or 3+.
	AttrResumeNth = "nth"
	// AttrResumeBeforeRerun is whether a resumed run ended before anything
	// ran again: true or false.
	AttrResumeBeforeRerun = "before_anything_reran"
	// AttrResumeAnomalyKind is what an anomaly was, or why a task or item of
	// a resume runs again or is kept.
	AttrResumeAnomalyKind = "kind"
	// AttrRunResumption is "true" on a run's ending that a resume led to, and
	// absent on its first one: a resumed run is counted once per ending (E85
	// C9), and this label is what tells the endings from the runs.
	AttrRunResumption = "ork.run.resumption"
)

// The endings of a resume whose plan could not be run: they leave the base's
// rows as they were (internal/engine's ReasonResumePlan*, which this package
// cannot import; TestResumeEndingReasons_AreTheEngines holds them equal).
const (
	resumePlanInvalid    = "RESUME_PLAN_INVALID"
	resumePlanUnreadable = "RESUME_PLAN_UNREADABLE"
)

// resumptionAttr is AttrRunResumption when run, at its ending, had been
// resumed. A resume gives every row a disposition before anything else, so
// one row of a resumed run says it; a plan that could not be run says it by
// its reason.
func resumptionAttr(run *orkstackv1.Run) []attribute.KeyValue {
	resumed := slices.ContainsFunc(run.GetTaskRuns(), func(row *orkstackv1.TaskRun) bool {
		return row.GetResumeDisposition() != orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED
	})
	switch run.GetError().GetCode() {
	case resumePlanInvalid, resumePlanUnreadable:
		resumed = true
	}
	if !resumed {
		return nil
	}
	return []attribute.KeyValue{attribute.String(AttrRunResumption, "true")}
}

// RecordResumeAnomaly counts one anomaly a resumed run met, of kind (for
// example reset_unreadable: a reset run that could not read the reset that
// made it, and failed with no event).
func (o *EngineObserver) RecordResumeAnomaly(ctx context.Context, namespace, kind string) {
	if o == nil {
		return
	}
	inst := o.namespaceInstruments(ctx, namespace)
	if inst == nil {
		return
	}
	inst.ResumeAnomalies.Add(ctx, 1, metric.WithAttributes(
		attribute.String(AttrNamespace, namespace), attribute.String(AttrResumeAnomalyKind, kind)))
}

// Bounds of the resume histograms.
var (
	resumeDurations    = []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}
	resumeSinceFailure = []float64{60, 300, 900, 3600, 21600, 86400, 604800}
	resumeOrdinals     = []float64{1, 2, 3, 5, 10, 20}
	resumeTaskCounts   = []float64{1, 2, 5, 10, 25, 50, 100, 500, 1000, 5000, 10000}
	resumeAttempts     = []float64{1, 2, 3, 5, 10, 20}
	resumeHeadroom     = []float64{0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}
	resumeActive       = []float64{1, 10, 60, 300, 900, 3600, 21600, 86400, 604800}
)

// resumeInstruments are one sink's resume instruments (docs/DEPLOY.md §7).
type resumeInstruments struct {
	resumes, resumed, outcome                  metric.Int64Counter
	tasks, ordinal, attempts                   metric.Int64Histogram
	headroom, confirm, since, readBack, active metric.Float64Histogram
	readingBack                                metric.Float64Gauge
	unconfirmed, stalled                       metric.Int64Gauge
}

func newResumeInstruments(meter metric.Meter) (*resumeInstruments, error) {
	r := &resumeInstruments{}
	var errs []error
	keep := func(err error) { errs = append(errs, err) }
	var err error
	r.resumes, err = meter.Int64Counter(ResumesMetric,
		metric.WithDescription("Requests to resume a run, by namespace, action, what the run's ending was, and outcome."), metric.WithUnit("{resume}"))
	keep(err)
	r.resumed, err = meter.Int64Counter(ResumedMetric,
		metric.WithDescription("Resumes that got going (RUN_RESUMED), by namespace, action and what the run's ending was."), metric.WithUnit("{resume}"))
	keep(err)
	r.outcome, err = meter.Int64Counter(ResumeOutcomeMetric,
		metric.WithDescription("Endings of resumed runs, by namespace, outcome, which resume of the run it was, and whether anything had run again."), metric.WithUnit("{run}"))
	keep(err)
	r.tasks, err = meter.Int64Histogram(ResumeTasksMetric,
		metric.WithDescription("Tasks and items of a confirmed resume, by namespace and why each runs again or is kept."), metric.WithUnit("{task}"),
		metric.WithExplicitBucketBoundaries(resumeTaskCounts...))
	keep(err)
	r.ordinal, err = meter.Int64Histogram(ResumeOrdinalMetric,
		metric.WithDescription("Which resume of its run a confirmed resume is."), metric.WithUnit("{resume}"),
		metric.WithExplicitBucketBoundaries(resumeOrdinals...))
	keep(err)
	r.attempts, err = meter.Int64Histogram(ResumeReadBackAttemptsMetric,
		metric.WithDescription("Tries one read of a resumed run's kept results took, recorded when it answered or failed for good."), metric.WithUnit("{attempt}"),
		metric.WithExplicitBucketBoundaries(resumeAttempts...))
	keep(err)
	r.headroom, err = meter.Float64Histogram(ResumeHeadroomMetric,
		metric.WithDescription("The history a planned resume would start with, as a share of the run's hold line."), metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(resumeHeadroom...))
	keep(err)
	r.confirm, err = meter.Float64Histogram(ResumeConfirmDurationMetric,
		metric.WithDescription("Seconds a confirmed resume took to answer its caller."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(resumeDurations...))
	keep(err)
	r.since, err = meter.Float64Histogram(ResumeSinceFailureMetric,
		metric.WithDescription("Seconds between a run's ending and the confirmed resume of it."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(resumeSinceFailure...))
	keep(err)
	r.readBack, err = meter.Float64Histogram(ResumeReadBackDurationMetric,
		metric.WithDescription("Seconds a resumed run took to read its kept results back, by outcome."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(resumeDurations...))
	keep(err)
	r.active, err = meter.Float64Histogram(ResumeActiveDurationMetric,
		metric.WithDescription("Seconds from a resume to the run's ending, by status."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(resumeActive...))
	keep(err)
	r.readingBack, err = meter.Float64Gauge(ResumeReadingBackMetric,
		metric.WithDescription("How long the oldest resumed run still reading its kept results back has done so; 0 when none is."), metric.WithUnit("s"))
	keep(err)
	r.unconfirmed, err = meter.Int64Gauge(ResumeUnconfirmedMetric,
		metric.WithDescription("Resumes recorded whose outcome is not confirmed."), metric.WithUnit("{run}"))
	keep(err)
	r.stalled, err = meter.Int64Gauge(ResumeStalledMetric,
		metric.WithDescription("Resumed runs that have made no progress."), metric.WithUnit("{run}"))
	keep(err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return r, nil
}

// resumeInst is the resume instruments of namespace's sink, or nil.
func (o *EngineObserver) resumeInst(ctx context.Context, namespace string) *resumeInstruments {
	if o == nil {
		return nil
	}
	inst := o.namespaceInstruments(ctx, namespace)
	if inst == nil {
		return nil
	}
	return inst.resume
}

// ResumeRequest is one request to resume a run, as the API answered it.
type ResumeRequest struct {
	Namespace string
	Action    orkstackv1.ResumeAction
	// ForceCanceled is whether the ending to set aside was a force cancel.
	ForceCanceled bool
	// Outcome is one of resumeOutcomes; any other is counted as "other".
	Outcome string
	// Refusal is a refused request's reason; empty otherwise.
	Refusal string
}

// RecordResumeRequest counts one request to resume a run.
func (o *EngineObserver) RecordResumeRequest(ctx context.Context, r ResumeRequest) {
	inst := o.resumeInst(ctx, r.Namespace)
	if inst == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String(AttrNamespace, r.Namespace), attribute.String(AttrAction, resumeActionLabel(r.Action)),
		attribute.String(AttrResumeFromState, resumeFromState(r.ForceCanceled)), attribute.String(AttrOutcome, closedLabel(resumeOutcomes, r.Outcome)),
	}
	if r.Refusal != "" {
		attrs = append(attrs, attribute.String(AttrRefusalReason, refusalReasonLabel(r.Refusal)))
	}
	inst.resumes.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordResumePreflight records the history a planned resume would start
// with, as a share of the run's hold line; a preview's as well as a
// confirmed one's.
func (o *EngineObserver) RecordResumePreflight(ctx context.Context, namespace string, share float64) {
	if inst := o.resumeInst(ctx, namespace); inst != nil {
		inst.headroom.Record(ctx, share, metric.WithAttributes(attribute.String(AttrNamespace, namespace)))
	}
}

// ResumeConfirmed is a resume that was confirmed.
type ResumeConfirmed struct {
	Namespace string
	// Tasks is how many tasks and items the plan has of each category
	// (ResumeCategory, lowercase).
	Tasks map[string]int
	// Ordinal is which resume of its run this is, from 1.
	Ordinal int
	// Took is how long the request took to answer, and SinceFailure the time
	// from the run's ending to it.
	Took, SinceFailure time.Duration
}

// RecordResumeConfirmed records a confirmed resume.
func (o *EngineObserver) RecordResumeConfirmed(ctx context.Context, r ResumeConfirmed) {
	inst := o.resumeInst(ctx, r.Namespace)
	if inst == nil {
		return
	}
	ns := attribute.String(AttrNamespace, r.Namespace)
	for kind, n := range r.Tasks {
		inst.tasks.Record(ctx, int64(n), metric.WithAttributes(ns, attribute.String(AttrResumeAnomalyKind, kind)))
	}
	inst.ordinal.Record(ctx, int64(r.Ordinal), metric.WithAttributes(ns))
	inst.confirm.Record(ctx, r.Took.Seconds(), metric.WithAttributes(ns))
	// A clock that ran backwards between two engines is no age.
	if r.SinceFailure >= 0 {
		inst.since.Record(ctx, r.SinceFailure.Seconds(), metric.WithAttributes(ns))
	}
}

// RecordReadBackCall records the tries one read of kept results took.
func (o *EngineObserver) RecordReadBackCall(ctx context.Context, namespace string, attempts int) {
	if inst := o.resumeInst(ctx, namespace); inst != nil {
		inst.attempts.Record(ctx, int64(attempts), metric.WithAttributes(attribute.String(AttrNamespace, namespace)))
	}
}

// ResumeEnding is how a resumed run ended.
type ResumeEnding struct {
	Namespace string
	// Outcome is one of resumeEndings; any other is counted as "other".
	Outcome string
	// Nth is which resume of its run led to this ending, from 1.
	Nth int
	// BeforeAnythingReran: the run ended before any task ran again.
	BeforeAnythingReran bool
	// Active is the time from the resume to the ending; negative is unknown.
	Active time.Duration
}

// RecordResumeEnding records how a resumed run ended.
func (o *EngineObserver) RecordResumeEnding(ctx context.Context, e ResumeEnding) {
	inst := o.resumeInst(ctx, e.Namespace)
	if inst == nil {
		return
	}
	ns, outcome := attribute.String(AttrNamespace, e.Namespace), closedLabel(resumeEndings, e.Outcome)
	inst.outcome.Add(ctx, 1, metric.WithAttributes(ns, attribute.String(AttrOutcome, outcome),
		attribute.String(AttrResumeNth, resumeNth(e.Nth)), attribute.String(AttrResumeBeforeRerun, strconv.FormatBool(e.BeforeAnythingReran))))
	if e.Active >= 0 {
		inst.active.Record(ctx, e.Active.Seconds(), metric.WithAttributes(ns, attribute.String(AttrStatus, outcome)))
	}
}

// ResumeSweepPass is what one pass of the sweep found of resumed runs.
type ResumeSweepPass struct {
	// OldestReadingBack is how long the oldest run still reading its kept
	// results back has done so; 0 when none is.
	OldestReadingBack time.Duration
	// Unconfirmed is the resumes recorded whose outcome is not confirmed.
	Unconfirmed int64
	// Stalled is the resumed runs that have made no progress.
	Stalled int64
}

// RecordResumeSweep records a pass on the default sink. Every gauge is
// recorded on every pass, so none keeps a stale value.
func (o *EngineObserver) RecordResumeSweep(ctx context.Context, pass ResumeSweepPass) {
	inst := o.defaultInstruments()
	if inst == nil {
		return
	}
	inst.resume.readingBack.Record(ctx, pass.OldestReadingBack.Seconds())
	inst.resume.unconfirmed.Record(ctx, pass.Unconfirmed)
	inst.resume.stalled.Record(ctx, pass.Stalled)
}

// recordResume records a resumed run's own events: RUN_RESUMED, and
// RUN_RESULTS_KEPT with how long the read-back took.
func (o *EngineObserver) recordResume(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run, inst *EngineInstruments) {
	if run == nil || inst == nil {
		return
	}
	attrs := inst.Cardinality.RunAttrs(run, "")
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED:
		attrs = append(attrs, inst.Cardinality.orkflowAttr(run.GetOrkflowName())...)
		inst.resume.resumed.Add(ctx, 1, metric.WithAttributes(append(attrs,
			attribute.String(AttrAction, resumeActionLabel(event.GetResumed().GetAction())),
			attribute.String(AttrResumeFromState, resumeFromState(event.GetResumed().GetSupersededReason() == runForceCanceled)))...))
	case orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT:
		inst.resume.readBack.Record(ctx, event.GetResultsKept().GetReadBackSeconds(),
			metric.WithAttributes(append(attrs, attribute.String(AttrOutcome, "kept"))...))
	}
}

// runForceCanceled is the ending of a force-canceled run.
const runForceCanceled = "RUN_FORCE_CANCELED"

// The closed label values of a resume request's and a resumed run's outcome.
var (
	resumeOutcomes = []string{"resumed", "already", "refused", "preview_refused", "unavailable", "unconfirmed"}
	resumeEndings  = []string{"succeeded", "failed", "canceled", "terminated"}
)

// closedLabel is v when it is one of allowed, and "other" otherwise: a label
// never takes a value its series were not planned for.
func closedLabel(allowed []string, v string) string {
	if slices.Contains(allowed, v) {
		return v
	}
	return "other"
}

func resumeFromState(forceCanceled bool) string {
	if forceCanceled {
		return "canceled_force"
	}
	return "failed"
}

func resumeActionLabel(a orkstackv1.ResumeAction) string {
	return strings.ToLower(strings.TrimPrefix(a.String(), "RESUME_ACTION_"))
}

func resumeNth(n int) string {
	if n >= 3 {
		return "3+"
	}
	return strconv.Itoa(max(n, 1))
}
