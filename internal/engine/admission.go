package engine

import (
	"context"
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Admission is the decision a namespace makes about a submission before a run
// id exists: whether it is accepting new runs at all (hold), and whether it
// has room for one more (max_running). It is decided once, at SubmitRun's
// gate (internal/api, admit), and never revisited — a run already accepted is
// never touched by a later hold or a lowered cap (AGENTS.md §15). Nothing here
// is workflow code: the functions below run in the API process before any
// workflow exists, and no workflow file calls them.

// AdmissionReason is the machine-readable half of a refusal (AGENTS.md §15).
type AdmissionReason string

const (
	// AdmissionHeld is a refusal by the namespace's hold.
	AdmissionHeld AdmissionReason = "NAMESPACE_HELD"
	// AdmissionMaxRunning is a refusal by the namespace's max_running.
	AdmissionMaxRunning AdmissionReason = "MAX_RUNNING_REACHED"
)

// AdmissionRefusedError reports a submission the namespace would not admit.
// It is the sibling of PoolEmptyError: a well-formed request the deployment
// is not accepting right now, so it maps to FailedPrecondition.
type AdmissionRefusedError struct {
	Namespace  string
	Reason     AdmissionReason
	Hold       *orkstackv1.NamespaceHold // set for AdmissionHeld
	Running    int64                     // set for AdmissionMaxRunning
	MaxRunning int32                     // set for AdmissionMaxRunning
}

// Error is the human-readable half: the cause and the fix, in one sentence
// each.
func (e *AdmissionRefusedError) Error() string {
	switch e.Reason {
	case AdmissionHeld:
		since := ""
		if ts := e.Hold.GetSince(); ts != nil {
			since = " since " + ts.AsTime().UTC().Format(time.RFC3339)
		}
		return fmt.Sprintf("namespace %q is on hold%s: %q. Release it with `orkctl namespace update %s --release-hold`",
			e.Namespace, since, e.Hold.GetReason(), e.Namespace)
	case AdmissionMaxRunning:
		return fmt.Sprintf("namespace %q already has %d runs in flight, which is its max_running. "+
			"Wait for one to finish, or raise the cap with `orkctl namespace update %s --max-running N`",
			e.Namespace, e.Running, e.Namespace)
	default:
		return fmt.Sprintf("namespace %q refused the submission (%s)", e.Namespace, e.Reason)
	}
}

// HoldRefusal is the refusal a namespace's hold implies, or nil when it is
// accepting. Pure: it reads the document and nothing else.
func HoldRefusal(ns *orkstackv1.Namespace) *AdmissionRefusedError {
	hold := ns.GetHold()
	if hold == nil {
		return nil
	}
	return &AdmissionRefusedError{
		Namespace: namespaceName(ns),
		Reason:    AdmissionHeld,
		Hold:      hold,
	}
}

// MaxRunningRefusal is the refusal a namespace's cap implies given the runs
// in flight, or nil when running < max_running or max_running is 0.
func MaxRunningRefusal(ns *orkstackv1.Namespace, running int64) *AdmissionRefusedError {
	limit := ns.GetMaxRunning()
	if limit <= 0 || running < int64(limit) {
		return nil
	}
	return &AdmissionRefusedError{
		Namespace:  namespaceName(ns),
		Reason:     AdmissionMaxRunning,
		Running:    running,
		MaxRunning: limit,
	}
}

// AdmissionObserver sees every refusal the admission gate makes. It is the
// refusal-side counterpart to EventObserver, and a separate method on
// purpose: EventObserver takes an Event and a Run, and a refusal has
// neither — it is made before a run id is minted — so reusing ObserveEvent
// would mean synthesizing a RUN_FAILED for something that was never a run
// and counting it on ork.runs beside ork.runs.refused.
//
// Its arguments are three fields of the run record the submission would
// have produced, and nothing a run record does not carry: namespace is what
// Run.namespace would have been; telemetry is what Run.telemetry would have
// been — the selection RefusalTelemetry built for this submission, whose
// sink is where the refusal is recorded and whose attributes are already
// stamped ork.ns.<key> / ork.label.<key> by the engine, nil when the
// namespace has no policy and the orkflow no labels, exactly as
// Run.telemetry is unset for such a run; orkflow is what Run.orkflow_name
// would have been; reason is the AdmissionReason as a string. A refusal is
// therefore labelled from the same already-stamped shape a run's signals
// are read from, never from a raw Namespace document.
//
// It is declared here rather than in internal/telemetry for the reason
// EventObserver is: internal/telemetry does not import internal/engine, so
// the telemetry type satisfies it by shape, and every parameter is a type
// both packages already see.
//
// Implementations must not block: it is called synchronously on the
// SubmitRun request path, on the terms EventObserver already states.
// telemetry.EngineObserver satisfies it structurally.
type AdmissionObserver interface {
	ObserveRefusal(ctx context.Context, namespace string, telemetry *orkstackv1.TelemetrySelection, orkflow string, reason string)
}

// RefusalTelemetry is the telemetry selection a run of this submission
// would have carried had it been admitted: telemetryFor(ns, wf) — the call
// PoolResolver.Plan makes for an admitted submission (pools.go) — rendered
// through TelemetrySelection.Proto (plan.go). It is the ONE way a refusal's
// attributes are stamped: telemetryFor prefixes the namespace's own
// attributes ork.ns.<key> and the orkflow's labels ork.label.<key>, bounded
// and sanitized as for a run, so ork.runs.refused carries the same keys,
// spelled the same way, as ork.runs would have for the run that was
// refused. Nil when the namespace set no policy and the orkflow carries no
// labels, as Run.telemetry is unset for such a run. Never an error:
// telemetryFor's error (its signature carries one; no path returns it
// today) yields nil, so the refusal is still counted, on the default sink,
// with no selection attributes — telemetry never fails a request
// (AGENTS.md §11).
func RefusalTelemetry(ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) *orkstackv1.TelemetrySelection {
	selection, err := telemetryFor(ns, wf)
	if err != nil {
		return nil
	}
	return selection.Proto()
}

// AdmissionReader is the one question admission asks of the cluster.
type AdmissionReader interface {
	// RunningCount is how many runs of namespace are in flight NOW — accepted
	// and not finished, the population GetStatsResponse.running lists. It is
	// the cluster's own answer, so any replica gets the same number.
	RunningCount(ctx context.Context, namespace string) (int64, error)
}
