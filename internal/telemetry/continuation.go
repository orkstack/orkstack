package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The engine's continuation metrics (docs/DEPLOY.md §7, "Lifecycle
// metrics"): measured by a run's own local activities around a switch and a
// restore — never by workflow code, which imports no OpenTelemetry — and
// handed here through cmd/ork's adapter, since this package does not import
// internal/engine. Every record goes to the default sink: an observation
// carries the namespace, never a run's own telemetry selection.

// RecordContinuationWrite records one switch write: its stored size by kind
// (unless it failed), its duration, and — for the checkpoint, which decides
// the switch — the outcome and how long the run held new dispatch first.
func (o *EngineObserver) RecordContinuationWrite(ctx context.Context, namespace, kind, outcome string,
	stateBytes int64, seconds, holdSeconds float64,
) {
	inst := o.defaultInstruments()
	if inst == nil {
		return
	}
	ns := attribute.String(AttrNamespace, namespace)
	if outcome != "write_failed" && stateBytes > 0 {
		inst.ContinuationStateBytes.Record(ctx, stateBytes,
			metric.WithAttributes(ns, attribute.String(AttrContinuationKind, kind)))
	}
	inst.ContinuationDuration.Record(ctx, seconds,
		metric.WithAttributes(ns, attribute.String(AttrContinuationPhase, "write")))
	if outcome == "" {
		return
	}
	inst.Continuations.Add(ctx, 1, metric.WithAttributes(ns, attribute.String(AttrOutcome, outcome)))
	if outcome == "written" {
		inst.ContinuationHoldDuration.Record(ctx, holdSeconds, metric.WithAttributes(ns))
	}
}

// RecordContinuationRestore records one bootstrap read: the attempt it
// succeeded on, and — on the read that completed the saved state, or once
// for a restore that failed — the outcome and, when restored, the whole
// restore's duration.
func (o *EngineObserver) RecordContinuationRestore(ctx context.Context, namespace, outcome string,
	attempt int64, final bool, seconds float64,
) {
	inst := o.defaultInstruments()
	if inst == nil {
		return
	}
	ns := attribute.String(AttrNamespace, namespace)
	if attempt > 0 {
		inst.ContinuationReadAttempts.Record(ctx, attempt, metric.WithAttributes(ns))
	}
	if outcome == "" {
		return
	}
	inst.Continuations.Add(ctx, 1, metric.WithAttributes(ns, attribute.String(AttrOutcome, outcome)))
	if final && outcome == "restored" {
		inst.ContinuationDuration.Record(ctx, seconds,
			metric.WithAttributes(ns, attribute.String(AttrContinuationPhase, "restore")))
	}
}

// defaultInstruments is the default sink's instruments, or nil when they
// could not be built (never at run time: NewEngineObserverForSinks built
// them at boot).
func (o *EngineObserver) defaultInstruments() *EngineInstruments {
	if o == nil {
		return nil
	}
	inst, err := o.instFor(o.sinks.Default())
	if err != nil {
		return nil
	}
	return inst
}

// RecordArchiveCleanup records one cleanup pass: the runs it retired, the
// runs it failed on, the runs it kept because their execution was missing
// before their retention (not_found_not_due: alert on any), and the due runs
// with a missing execution it held because too many were missing early.
func (o *EngineObserver) RecordArchiveCleanup(ctx context.Context, retired, failed, notFoundNotDue, notFoundHeld int64) {
	inst := o.defaultInstruments()
	if inst == nil {
		return
	}
	for _, o := range [...]struct {
		outcome string
		count   int64
	}{{"retired", retired}, {"failed", failed}, {"not_found_not_due", notFoundNotDue}, {"not_found_held", notFoundHeld}} {
		if o.count > 0 {
			inst.ArchiveCleanupRuns.Add(ctx, o.count, metric.WithAttributes(attribute.String(AttrOutcome, o.outcome)))
		}
	}
}

// ArchiveSweepPass is one pass of the archive sweep, as the engine reports it.
type ArchiveSweepPass struct {
	// Archived, Failed and Expired are what became of the finished runs with
	// no archive the pass came to.
	Archived, Failed, Expired int64
	// Unconfirmed is how many finished runs it could not confirm have an
	// archive.
	Unconfirmed int64
	// SecondsToExpiry is how long until the first of those expires; 0 when
	// there is none or the pass could not date them, so an undated risk reads
	// as imminent.
	SecondsToExpiry float64
	// ListFailed: the cluster's index could not be read to its end.
	ListFailed bool
	// StoreFailed: object storage stopped answering during the pass.
	StoreFailed bool
	// IntervalTooLong: two cleanup intervals reach the cluster's retention.
	IntervalTooLong bool
	// At is when the pass ended.
	At time.Time
}

// RecordArchiveSweep records one pass of the archive sweep. Every gauge is
// recorded on every pass, so none keeps a stale value, and every outcome is
// added even at zero, so each series exists from the first pass. The pass's
// end is its last success only when it saw everything: the cluster's index
// listed to its end and object storage answered.
func (o *EngineObserver) RecordArchiveSweep(ctx context.Context, pass ArchiveSweepPass) {
	inst := o.defaultInstruments()
	if inst == nil {
		return
	}
	for _, c := range [...]struct {
		outcome string
		count   int64
	}{{"archived", pass.Archived}, {"failed", pass.Failed}, {"expired", pass.Expired}} {
		inst.ArchiveSweepRuns.Add(ctx, c.count, metric.WithAttributes(attribute.String(AttrOutcome, c.outcome)))
	}
	inst.ArchiveSweepUnconfirmed.Record(ctx, pass.Unconfirmed)
	inst.ArchiveSweepEarliestExpiry.Record(ctx, pass.SecondsToExpiry)
	inst.ArchiveSweepListFailed.Record(ctx, boolGauge(pass.ListFailed))
	inst.ArchiveSweepIntervalTooLong.Record(ctx, boolGauge(pass.IntervalTooLong))
	if !pass.ListFailed && !pass.StoreFailed {
		inst.ArchiveSweepLastSuccess.Record(ctx, pass.At.Unix())
	}
}

// boolGauge is a 0/1 gauge value.
func boolGauge(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
