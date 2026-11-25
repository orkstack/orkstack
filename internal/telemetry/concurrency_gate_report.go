package telemetry

import (
	"context"
	"errors"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// What a concurrency gate's work is counted as (E88 §9.9.2): the entries it
// took back because their run no longer holds or waits for them, the runs it
// could not check, its rebuilds and its starts. The gate is a workflow, which
// records nothing itself: the activities that do its work, and one that
// reports what it took back, hand the counts over.
const (
	ConcurrencyReclaimedMetric    = "ork.concurrency.reclaimed"
	ConcurrencyVerifyErrorsMetric = "ork.concurrency.verify_errors"
	ConcurrencyRebuiltMetric      = "ork.concurrency.rebuilt"
	ConcurrencyGatesStartedMetric = "ork.concurrency.gates_started"
)

var (
	concurrencyReclaimOutcomes = []string{"closed", "not_found", "stale", "missing_slot", "expired"}
	concurrencyRebuiltOutcomes = []string{"skipped", "ok", "failed_attempt"}
)

// ConcurrencyReclaimOutcomes and ConcurrencyRebuiltOutcomes are the closed
// sets, each a fresh copy.
func ConcurrencyReclaimOutcomes() []string { return slices.Clone(concurrencyReclaimOutcomes) }
func ConcurrencyRebuiltOutcomes() []string { return slices.Clone(concurrencyRebuiltOutcomes) }

// ConcurrencyGateReport is one report of a namespace's gate. A limit is its
// label (CensusLimitLabel's form: never the rendered key).
type ConcurrencyGateReport struct {
	Namespace    string
	Started      bool
	Reclaimed    []ConcurrencyReclaim
	VerifyErrors []ConcurrencyVerifyErrors
	Rebuilt      []ConcurrencyRebuild
}

// ConcurrencyReclaim counts entries removed, ConcurrencyVerifyErrors runs
// that could not be checked, and ConcurrencyRebuild rebuilds.
type ConcurrencyReclaim struct {
	Limit, Outcome string
	Entries        int
}

type ConcurrencyVerifyErrors struct {
	Limit string
	Calls int
}

type ConcurrencyRebuild struct {
	Outcome string
	Times   int
}

func (inst *concurrencyInstruments) buildGateReport(meter metric.Meter) error {
	var errs []error
	var err error
	inst.reclaimed, err = meter.Int64Counter(ConcurrencyReclaimedMetric, metric.WithUnit("{entry}"),
		metric.WithDescription("Slots and places in line a concurrency limit took back, by why."))
	errs = append(errs, err)
	inst.verifyErrors, err = meter.Int64Counter(ConcurrencyVerifyErrorsMetric, metric.WithUnit("{call}"),
		metric.WithDescription("Checks of a holding or waiting run that failed; its slot or place is kept."))
	errs = append(errs, err)
	inst.rebuilt, err = meter.Int64Counter(ConcurrencyRebuiltMetric, metric.WithUnit("{gate}"),
		metric.WithDescription("Rebuilds of a concurrency limit's record, by outcome."))
	errs = append(errs, err)
	inst.gates, err = meter.Int64Counter(ConcurrencyGatesStartedMetric, metric.WithUnit("{gate}"),
		metric.WithDescription("Concurrency keys that became active."))
	errs = append(errs, err)
	return errors.Join(errs...)
}

// Report counts what a gate reported. An outcome outside its closed set, or
// a count below one, is not recorded.
func (m *ConcurrencyMetrics) Report(ctx context.Context, r ConcurrencyGateReport) {
	if m == nil {
		return
	}
	inst, ok := m.sinks.forNamespace(ctx, r.Namespace)
	if !ok {
		return
	}
	add := func(counter metric.Int64Counter, n int, attrs ...attribute.KeyValue) {
		if n > 0 {
			counter.Add(ctx, int64(n), metric.WithAttributes(append(attrs, attribute.String(AttrNamespace, r.Namespace))...))
		}
	}
	known := func(outcome string, set []string) bool {
		if slices.Contains(set, outcome) {
			return true
		}
		m.sinks.warnOnce("report outcome:"+outcome, "a concurrency outcome outside the metric contract is not recorded", "outcome", outcome)
		return false
	}
	if r.Started {
		add(inst.gates, 1)
	}
	for _, c := range r.Reclaimed {
		if known(c.Outcome, concurrencyReclaimOutcomes) {
			add(inst.reclaimed, c.Entries, attribute.String(AttrConcurrencyLimit, c.Limit), attribute.String(AttrConcurrencyOutcome, c.Outcome))
		}
	}
	for _, c := range r.VerifyErrors {
		add(inst.verifyErrors, c.Calls, attribute.String(AttrConcurrencyLimit, c.Limit))
	}
	for _, c := range r.Rebuilt {
		if known(c.Outcome, concurrencyRebuiltOutcomes) {
			add(inst.rebuilt, c.Times, attribute.String(AttrConcurrencyOutcome, c.Outcome))
		}
	}
}
