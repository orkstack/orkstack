package telemetry

import (
	"context"

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

// RecordArchiveSweep records one pass of the archive sweep: what became of the
// finished runs it came to, and how many it could not confirm have an archive.
// secondsToExpiry is recorded only when dated says the pass could date them; a
// reader takes it as current only while the unconfirmed count is above zero.
func (o *EngineObserver) RecordArchiveSweep(ctx context.Context, archived, failed, expired, unconfirmed int64,
	secondsToExpiry float64, dated bool,
) {
	inst := o.defaultInstruments()
	if inst == nil {
		return
	}
	for outcome, count := range map[string]int64{"archived": archived, "failed": failed, "expired": expired} {
		if count > 0 {
			inst.ArchiveSweepRuns.Add(ctx, count, metric.WithAttributes(attribute.String(AttrOutcome, outcome)))
		}
	}
	inst.ArchiveSweepUnconfirmed.Record(ctx, unconfirmed)
	if dated {
		inst.ArchiveSweepEarliestExpiry.Record(ctx, secondsToExpiry)
	}
}
