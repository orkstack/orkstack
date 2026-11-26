package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A concurrency limit's arrivals and exits (E88 §9.9.2): how each request
// for a slot was answered, and how each wait in line ended and how long it
// took. Labels are the closed set of §9.9.1: no rendered key is ever one.
const (
	ConcurrencyArrivalsMetric  = "ork.concurrency.arrivals"
	ConcurrencyExitsMetric     = "ork.concurrency.exits"
	ConcurrencyQueueWaitMetric = "ork.concurrency.queue.wait"

	AttrConcurrencyStrategy = "ork.concurrency.strategy"
	AttrConcurrencyOutcome  = "ork.concurrency.outcome"
)

// The closed sets of the arrivals' and exits' outcomes, and the strategies.
var (
	concurrencyArrivalOutcomes = []string{"granted", "queued", "refused", "skipped", "full", "unavailable", "unsupported", "fenced"}
	concurrencyExitOutcomes    = []string{"started", "canceled", "timed_out", "superseded", "full", "unavailable", "unsupported", "fenced"}
	concurrencyStrategies      = []string{"queue", "refuse", "skip", "cancel_oldest", "replace"}
	concurrencyLevels          = []string{"run", "task"}
	// concurrencyWaitBuckets are §9.9.2's, in seconds: a second to a day.
	concurrencyWaitBuckets = []float64{1, 5, 15, 30, 60, 300, 900, 1800, 3600, 7200, 21600, 86400}
)

// ConcurrencyArrivalOutcomes and ConcurrencyExitOutcomes are the closed
// sets, each a fresh copy.
func ConcurrencyArrivalOutcomes() []string { return slices.Clone(concurrencyArrivalOutcomes) }
func ConcurrencyExitOutcomes() []string    { return slices.Clone(concurrencyExitOutcomes) }

// waitOutcomes are the exits' outcomes by the event's enum: every value but
// UNSPECIFIED maps (TestWaitOutcomes_EveryValueMaps).
var waitOutcomes = map[orkstackv1.ConcurrencyWaitOutcome]string{
	orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED:     "started",
	orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_CANCELED:    "canceled",
	orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_TIMED_OUT:   "timed_out",
	orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_SUPERSEDED:  "superseded",
	orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_FULL:        "full",
	orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNAVAILABLE: "unavailable",
	orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNSUPPORTED: "unsupported",
	orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_FENCED:      "fenced",
}

var waitStrategies = map[orkstackv1.ConcurrencyStrategy]string{
	orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE:         "queue",
	orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REFUSE:        "refuse",
	orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP:          "skip",
	orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_CANCEL_OLDEST: "cancel_oldest",
	orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REPLACE:       "replace",
}

// waitEndArrival is an event's wait end as the exits' labels; a value
// outside a closed set is left empty, which Exit does not record.
func waitEndArrival(end *orkstackv1.ConcurrencyWaitEnd, run *orkstackv1.Run) ConcurrencyArrival {
	level := "run"
	if end.GetTaskLevel() {
		level = "task"
	}
	return ConcurrencyArrival{Namespace: run.GetNamespace(), Orkflow: run.GetOrkflowName(), Limit: end.GetLimitLabel(),
		Level: level, Strategy: waitStrategies[end.GetStrategy()], Outcome: waitOutcomes[end.GetOutcome()]}
}

// ConcurrencyArrival is one request for a slot, answered: the limit's
// label (CensusLimitLabel's form: never the rendered key).
type ConcurrencyArrival struct {
	Namespace, Orkflow, Limit, Level, Strategy, Outcome string
}

type concurrencyInstruments struct {
	arrivals, exits                                 metric.Int64Counter
	wait                                            metric.Float64Histogram
	reclaimed, verifyErrors, rebuilt, gates, resets metric.Int64Counter
}

// ConcurrencyMetrics records arrivals, exits and what the gates report on
// each namespace's sink. A nil *ConcurrencyMetrics records nothing.
type ConcurrencyMetrics struct {
	sinks        *areaSinks[*concurrencyInstruments]
	orkflowLabel bool
}

// NewConcurrencyMetrics builds the instruments. orkflowLabel is
// telemetry.orkflow_label: whether the Orkflow's name is a label.
func NewConcurrencyMetrics(p *Providers, route GCRouter, orkflowLabel bool, logger *slog.Logger) *ConcurrencyMetrics {
	return &ConcurrencyMetrics{orkflowLabel: orkflowLabel, sinks: newAreaSinks(p, route, "concurrency", logger,
		func(p *Providers) (*concurrencyInstruments, error) {
			meter := meterFor(p)
			var inst concurrencyInstruments
			var errs []error
			var err error
			inst.arrivals, err = meter.Int64Counter(ConcurrencyArrivalsMetric,
				metric.WithDescription("Requests for a concurrency slot, by how the limit answered."),
				metric.WithUnit("{arrival}"))
			errs = append(errs, err)
			inst.exits, err = meter.Int64Counter(ConcurrencyExitsMetric,
				metric.WithDescription("Waits in a concurrency limit's line, by how they ended."),
				metric.WithUnit("{exit}"))
			errs = append(errs, err)
			inst.wait, err = meter.Float64Histogram(ConcurrencyQueueWaitMetric,
				metric.WithDescription("Seconds a run or task waited in a concurrency limit's line, by how the wait ended."),
				metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(concurrencyWaitBuckets...))
			errs = append(errs, err)
			errs = append(errs, inst.buildGateReport(meter))
			if err := errors.Join(errs...); err != nil {
				return nil, err
			}
			return &inst, nil
		})}
}

// attrs is a record's labels, or false when one is outside the contract.
func (m *ConcurrencyMetrics) attrs(a ConcurrencyArrival, outcomes []string) ([]attribute.KeyValue, bool) {
	for _, c := range []struct {
		name, value string
		set         []string
	}{{"outcome", a.Outcome, outcomes}, {"strategy", a.Strategy, concurrencyStrategies}, {"level", a.Level, concurrencyLevels}} {
		if !slices.Contains(c.set, c.value) {
			m.sinks.warnOnce(c.name+":"+c.value, "a concurrency "+c.name+" outside the metric contract is not recorded", c.name, c.value)
			return nil, false
		}
	}
	out := []attribute.KeyValue{attribute.String(AttrNamespace, a.Namespace), attribute.String(AttrConcurrencyLimit, a.Limit),
		attribute.String(AttrConcurrencyLevel, a.Level), attribute.String(AttrConcurrencyStrategy, a.Strategy),
		attribute.String(AttrConcurrencyOutcome, a.Outcome)}
	if m.orkflowLabel && a.Orkflow != "" {
		out = append(out, attribute.String(AttrOrkflowName, a.Orkflow))
	}
	return out, true
}

// Arrival counts one answered request for a slot.
func (m *ConcurrencyMetrics) Arrival(ctx context.Context, a ConcurrencyArrival) {
	if m == nil {
		return
	}
	attrs, ok := m.attrs(a, concurrencyArrivalOutcomes)
	if !ok {
		return
	}
	if inst, ok := m.sinks.forNamespace(ctx, a.Namespace); ok {
		inst.arrivals.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// Exit counts one wait in line that ended, and how long it took.
func (m *ConcurrencyMetrics) Exit(ctx context.Context, a ConcurrencyArrival, waited time.Duration) {
	if m == nil {
		return
	}
	attrs, ok := m.attrs(a, concurrencyExitOutcomes)
	if !ok {
		return
	}
	if inst, ok := m.sinks.forNamespace(ctx, a.Namespace); ok {
		inst.exits.Add(ctx, 1, metric.WithAttributes(attrs...))
		inst.wait.Record(ctx, max(waited.Seconds(), 0), metric.WithAttributes(attrs...))
	}
}
