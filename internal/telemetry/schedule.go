package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Schedule instruments (docs/DEPLOY.md, "Schedules").
const (
	ScheduleFiresMetric    = "ork.schedule.fires"
	ScheduleFireLagMetric  = "ork.schedule.fire.lag"
	ScheduleRequestsMetric = "ork.schedule.requests"
	// AttrSchedule is the schedule's name, recorded only with object labels
	// on (telemetry.object_labels): an authored name is not a bounded label.
	AttrSchedule = "ork.schedule"
	AttrAction   = "action"
)

// The outcomes of one fire's preparation, a closed set. A fire is counted at
// least once: a preparation its local activity retries is counted per try.
const (
	ScheduleFireStarted     = "started"
	ScheduleFireRefused     = "refused"
	ScheduleFireUnavailable = "unavailable"
	// A concurrency limit kept the fire from starting a run (E88 §7.2):
	// a skip limit's full key, or a full line.
	ScheduleFireSkipped    = "skipped"
	ScheduleFireNotStarted = "not_started"
)

// The schedule API's actions, a closed set.
const (
	ScheduleActionCreate   = "create"
	ScheduleActionUpdate   = "update"
	ScheduleActionDelete   = "delete"
	ScheduleActionPause    = "pause"
	ScheduleActionResume   = "resume"
	ScheduleActionFire     = "fire"
	ScheduleActionBackfill = "backfill"
)

var (
	scheduleFireOutcomes = []string{ScheduleFireStarted, ScheduleFireRefused, ScheduleFireUnavailable,
		ScheduleFireSkipped, ScheduleFireNotStarted}
	scheduleActions = []string{ScheduleActionCreate, ScheduleActionUpdate, ScheduleActionDelete,
		ScheduleActionPause, ScheduleActionResume, ScheduleActionFire, ScheduleActionBackfill}
	// scheduleRefusalReasons are the refusals a fire's preparation names:
	// the Connect codes submit refuses with, and internal for a preparation
	// this engine could not encode or bound.
	scheduleRefusalReasons = []string{"invalid_argument", "not_found", "failed_precondition",
		"resource_exhausted", "permission_denied", "internal"}
	// scheduleLagBuckets span an on-time fire (well under a second) to a
	// backfill a day late.
	scheduleLagBuckets = []float64{0.1, 0.5, 1, 5, 15, 60, 300, 900, 3600, 21600, 86400}
)

// ScheduleFireOutcomes, ScheduleActions and ScheduleRefusalReasons are the
// closed sets, each a fresh copy.
func ScheduleFireOutcomes() []string   { return slices.Clone(scheduleFireOutcomes) }
func ScheduleActions() []string        { return slices.Clone(scheduleActions) }
func ScheduleRefusalReasons() []string { return slices.Clone(scheduleRefusalReasons) }

type scheduleInstruments struct {
	fires    metric.Int64Counter
	lag      metric.Float64Histogram
	requests metric.Int64Counter
}

// ScheduleMetrics records schedule fires and the schedule API's requests, on
// each namespace's sink. A nil *ScheduleMetrics records nothing.
type ScheduleMetrics struct {
	sinks        *areaSinks[*scheduleInstruments]
	objectLabels bool
}

// NewScheduleMetrics builds the schedule instruments. objectLabels is
// telemetry.object_labels: whether the schedule's name is a label.
func NewScheduleMetrics(p *Providers, route GCRouter, objectLabels bool, logger *slog.Logger) *ScheduleMetrics {
	return &ScheduleMetrics{objectLabels: objectLabels, sinks: newAreaSinks(p, route, "schedule", logger,
		func(p *Providers) (*scheduleInstruments, error) {
			meter := meterFor(p)
			var inst scheduleInstruments
			var errs []error
			var err error
			inst.fires, err = meter.Int64Counter(ScheduleFiresMetric,
				metric.WithDescription("Schedule fires, by namespace and outcome, with the refusal's reason; counted at least once."),
				metric.WithUnit("{fire}"))
			errs = append(errs, err)
			inst.lag, err = meter.Float64Histogram(ScheduleFireLagMetric,
				metric.WithDescription("How long after its nominal time a fire that started or was refused was prepared, by namespace; a backfill is as late as the time it stands in for."),
				metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(scheduleLagBuckets...))
			errs = append(errs, err)
			inst.requests, err = meter.Int64Counter(ScheduleRequestsMetric,
				metric.WithDescription("Schedule API requests that changed or fired a schedule, by namespace and action."),
				metric.WithUnit("{request}"))
			errs = append(errs, err)
			if err := errors.Join(errs...); err != nil {
				return nil, err
			}
			return &inst, nil
		})}
}

// Fire counts one fire's preparation. reason is the refusal's, and only a
// refused fire carries one; lag is how late after scheduledAt it was
// prepared, recorded for a fire that started or was refused.
func (m *ScheduleMetrics) Fire(ctx context.Context, namespace, schedule, outcome, reason string, lag time.Duration) {
	if m == nil {
		return
	}
	if !slices.Contains(scheduleFireOutcomes, outcome) {
		m.sinks.warnOnce("fire:"+outcome, "a schedule fire outcome outside the metric contract is not recorded", "outcome", outcome)
		return
	}
	inst, ok := m.sinks.forNamespace(ctx, namespace)
	if !ok {
		return
	}
	base := m.base(namespace, schedule)
	attrs := append(slices.Clone(base), attribute.String(AttrOutcome, outcome))
	if outcome == ScheduleFireRefused {
		if !slices.Contains(scheduleRefusalReasons, reason) {
			m.sinks.warnOnce("refusal:"+reason, "a schedule refusal reason outside the metric contract is recorded as internal", "reason", reason)
			reason = "internal"
		}
		attrs = append(attrs, attribute.String(AttrRefusalReason, reason))
	}
	inst.fires.Add(ctx, 1, metric.WithAttributes(attrs...))
	// An unavailable try is retried, and its lag would be sampled once per
	// try: only a fire that started or was refused is a lag sample.
	if outcome != ScheduleFireUnavailable {
		inst.lag.Record(ctx, max(lag.Seconds(), 0), metric.WithAttributes(base...))
	}
}

// Request counts one schedule API request that succeeded.
func (m *ScheduleMetrics) Request(ctx context.Context, namespace, action string) {
	if m == nil {
		return
	}
	if !slices.Contains(scheduleActions, action) {
		m.sinks.warnOnce("action:"+action, "a schedule action outside the metric contract is not recorded", "action", action)
		return
	}
	if inst, ok := m.sinks.forNamespace(ctx, namespace); ok {
		inst.requests.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrNamespace, namespace), attribute.String(AttrAction, action)))
	}
}

func (m *ScheduleMetrics) base(namespace, schedule string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String(AttrNamespace, namespace)}
	if m.objectLabels && schedule != "" {
		attrs = append(attrs, attribute.String(AttrSchedule, schedule))
	}
	return attrs
}
