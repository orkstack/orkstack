package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// errNoAdmissionReader is the cause an Unavailable carries when a namespace
// sets a cap and this engine's binding has no reader to count against. It is
// the same answer an unreadable index gets: admitting on a count nobody took
// would be the recovery magic AGENTS.md §14 ranks below explicit failure.
var errNoAdmissionReader = errors.New("this engine has no run reader configured")

// admit is the admission gate: the one place a namespace's hold, cap and
// enforced budget are enforced. It is called from prepare, after the
// namespace, orkflow and binding have resolved and before the pool plan, so
// a refusal here has the same "nothing left behind" property every other
// refusal in prepare has.
//
// Order is hold, cap, budget: cheapest first. A hold is answered from the
// document already in hand and costs nothing; the cap costs one read of the
// cluster's index and is skipped entirely when max_running is 0; the budget
// is a usage measurement over its trailing period, shared with GetUsage and
// cached for ORK_USAGE_CACHE_TTL, and is skipped entirely unless the budget
// is enforced — a soft budget never reads usage at submit.
//
// Every refusal it makes is logged once through s.logger and announced once
// to s.admissions; wf is the stored orkflow document prepare has already
// read, carried only for those two signals — its name on the log line, and
// engine.RefusalTelemetry(ns, wf) for the counter — the decision itself reads
// ns, the count and the usage alone.
func (s *Service) admit(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	wf *orkstackv1.Orkflow,
	binding engine.Binding,
) error {
	if refusal := engine.HoldRefusal(ns); refusal != nil {
		return s.refuse(ctx, ns, wf, refusal)
	}
	name := artifacts.ResolveNamespace(ns.GetName())
	if ns.GetMaxRunning() > 0 {
		running, err := s.runningCount(ctx, binding, name, int64(ns.GetMaxRunning()))
		if err != nil {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"namespace %q max_running could not be checked against the execution index: %w; ask again",
				name, err))
		}
		if refusal := engine.MaxRunningRefusal(ns, running); refusal != nil {
			return s.refuse(ctx, ns, wf, refusal)
		}
	}
	if budget := ns.GetBudget(); budget != nil && budget.GetEnforce() {
		usage, err := s.usage.BudgetUsage(ctx, ns)
		var refusal *engine.AdmissionRefusedError
		var connectErr *connect.Error
		switch {
		case usageUnmeasurable(err):
			refusal = engine.BudgetUnmeasurableRefusal(ns, err)
		case errors.As(err, &connectErr):
			// The caller's own context ended while the measurement it joined
			// was still running: Unavailable, as is.
			return err
		case err != nil:
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"namespace %q budget could not be checked against its usage: %w; ask again", name, err))
		default:
			refusal = engine.BudgetRefusal(ns, usage)
		}
		if refusal != nil {
			return s.refuse(ctx, ns, wf, refusal)
		}
	}
	return nil
}

// runningCount asks the binding's reader — the cluster the run would start
// on — how many runs of the namespace are in flight, exact at ceiling (the
// namespace's max_running).
func (s *Service) runningCount(ctx context.Context, binding engine.Binding, namespace string, ceiling int64) (int64, error) {
	if binding.Reader == nil {
		return 0, errNoAdmissionReader
	}
	return binding.Reader.RunningCount(ctx, namespace, ceiling)
}

// refuse logs and announces one refusal, then returns it as the Connect
// error the caller receives. Neither signal is on the path to the refusal:
// the error is returned whether or not a logger or observer is wired.
func (s *Service) refuse(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	wf *orkstackv1.Orkflow,
	refusal *engine.AdmissionRefusedError,
) error {
	attrs := []any{
		"namespace", refusal.Namespace,
		"orkflow", wf.GetName(),
		"reason", string(refusal.Reason),
	}
	switch refusal.Reason {
	case engine.AdmissionHeld:
		if since := refusal.Hold.GetSince(); since != nil {
			attrs = append(attrs, "hold_since", since.AsTime().UTC().Format(time.RFC3339))
		}
	case engine.AdmissionMaxRunning:
		attrs = append(attrs, "running", refusal.Running, "max_running", refusal.MaxRunning)
	case engine.AdmissionBudgetExceeded:
		attrs = append(attrs,
			"period", refusal.Budget.GetPeriod().AsDuration().String(),
			"cpu_seconds_used", fmt.Sprintf("%.0f", refusal.Usage.CPUSeconds),
			"cpu_seconds_budget", refusal.Budget.GetCpuSeconds(),
			"gib_seconds_used", fmt.Sprintf("%.0f", refusal.Usage.GiBSeconds),
			"gib_seconds_budget", refusal.Budget.GetGibSeconds(),
			"measured_at", refusal.Usage.MeasuredAt.UTC().Format(time.RFC3339))
	case engine.AdmissionBudgetUnmeasurable:
		attrs = append(attrs,
			"period", refusal.Budget.GetPeriod().AsDuration().String(),
			"error", fmt.Sprint(refusal.Unmeasured))
	}
	// Unconditional and at Info: a hold refusing a tenant's traffic is an
	// operator decision taking effect, the same class of line as the
	// empty-pool override, not a per-event lifecycle record.
	s.logger.InfoContext(ctx, "run refused", attrs...)
	if s.admissions != nil {
		s.admissions.ObserveRefusal(ctx, refusal.Namespace, engine.RefusalTelemetry(ns, wf),
			wf.GetName(), string(refusal.Reason))
	}
	return connect.NewError(connect.CodeFailedPrecondition, refusal)
}
