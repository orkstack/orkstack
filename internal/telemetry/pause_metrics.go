package telemetry

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A run's pauses (E103, docs/DEPLOY.md "Pause metrics"): one count per
// transition, keyed only on the event's pause_transition, which marks exactly
// one event per transition, and recorded on first publication, so a
// redelivered event counts once and a lost one zero times.
const (
	PausesMetric        = "ork.runs.pauses"
	PauseDurationMetric = "ork.runs.pause.duration"
	// AttrPauseCause is self (a person's pause) or parent (the parent run's).
	AttrPauseCause = "ork.pause.cause"
	// AttrPauseOutcome is what the transition did.
	AttrPauseOutcome = "ork.pause.outcome"
)

// pauseDurationBuckets are the histogram's bounds in seconds: a minute to
// the 31-day maximum.
var pauseDurationBuckets = []float64{60, 300, 900, 3600, 14400, 86400, 259200, 604800, 1209600, 2678400}

var pauseOutcomes = map[orkstackv1.PauseTransition]string{
	orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED:          "paused",
	orkstackv1.PauseTransition_PAUSE_TRANSITION_CAUSES_CHANGED:  "causes_changed",
	orkstackv1.PauseTransition_PAUSE_TRANSITION_UNPAUSED:        "unpaused",
	orkstackv1.PauseTransition_PAUSE_TRANSITION_NOT_PAUSED:      "not_paused",
	orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_TIMED_OUT: "timed_out",
	orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_RUN_ENDED: "run_ended",
}

// pauseInstruments are the two pause instruments, built with the engine's.
type pauseInstruments struct {
	pauses   metric.Int64Counter
	duration metric.Float64Histogram
	// zeroed records the namespaces whose outcome series this replica has
	// started at 0, so an alert on an increase sees its first one.
	zeroed sync.Map
}

func newPauseInstruments(meter metric.Meter) (*pauseInstruments, error) {
	pauses, err := meter.Int64Counter(PausesMetric,
		metric.WithDescription("Pause transitions of runs, by namespace, cause and outcome."),
		metric.WithUnit("{pause}"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram(PauseDurationMetric,
		metric.WithDescription("Seconds a run's own pause lasted, by namespace and how it ended."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(pauseDurationBuckets...))
	if err != nil {
		return nil, err
	}
	return &pauseInstruments{pauses: pauses, duration: duration}, nil
}

// pauseCause says whose pause an event moved: an API request's (it carries
// a request id) or, at a run's end, the run's own when its last pause is
// recorded; otherwise its parent's.
func pauseCause(event *orkstackv1.Event, run *orkstackv1.Run) string {
	switch event.GetPauseTransition() {
	case orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_TIMED_OUT, orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_RUN_ENDED:
		if end := run.GetLastPause().GetEnd(); end == orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT || end == orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED {
			return "self"
		}
		return "parent"
	}
	if event.GetRequestId() != "" {
		return "self"
	}
	return "parent"
}

// recordPause counts one pause transition and, when it ended the run's own
// pause, how long that lasted.
func (o *EngineObserver) recordPause(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run, inst *EngineInstruments) {
	outcome, ok := pauseOutcomes[event.GetPauseTransition()]
	if !ok || inst == nil || inst.pause == nil {
		return
	}
	base := inst.Cardinality.RunAttrs(run, "")
	base = append(base, inst.Cardinality.orkflowAttr(run.GetOrkflowName())...)
	set := attribute.NewSet(base...)
	if _, seen := inst.pause.zeroed.LoadOrStore(set.Equivalent(), true); !seen {
		for _, name := range pauseOutcomes {
			for _, cause := range []string{"self", "parent"} {
				inst.pause.pauses.Add(ctx, 0, metric.WithAttributes(append(append([]attribute.KeyValue(nil), base...),
					attribute.String(AttrPauseCause, cause), attribute.String(AttrPauseOutcome, name))...))
			}
		}
	}
	cause := pauseCause(event, run)
	inst.pause.pauses.Add(ctx, 1, metric.WithAttributes(append(append([]attribute.KeyValue(nil), base...),
		attribute.String(AttrPauseCause, cause), attribute.String(AttrPauseOutcome, outcome))...))
	switch outcome {
	case "unpaused", "timed_out", "run_ended":
	default:
		return
	}
	last := run.GetLastPause()
	if cause != "self" || last.GetPausedAt() == nil || last.GetEndedAt() == nil {
		return
	}
	seconds := last.GetEndedAt().AsTime().Sub(last.GetPausedAt().AsTime()).Seconds()
	if seconds < 0 {
		return
	}
	inst.pause.duration.Record(ctx, seconds, metric.WithAttributes(append(append([]attribute.KeyValue(nil), base...),
		attribute.String(AttrPauseOutcome, outcome))...))
}
