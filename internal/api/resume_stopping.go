package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/runstatus"
)

// The causes a STILL_STOPPING refusal carries (docs/REASONS.md).
const (
	// causeContainersStopping: a container of the run may still be stopping.
	causeContainersStopping = "containers_stopping"
	// causeChildRunsOpen: a run below it is still open.
	causeChildRunsOpen = "child_runs_open"
	// causeChildrenUnconfirmed: whether one is cannot be told yet.
	causeChildrenUnconfirmed = "children_unconfirmed"
	// causeChildRunning: in a run whose results the engine records, the child
	// of a composed task its outputs may settle is still open (design §28.12
	// F8).
	causeChildRunning = string(engine.ComposedChildRunning)
)

// maxNamedChildRuns bounds how many open runs a refusal names.
const maxNamedChildRuns = 5

// stillStopping refuses a resume while something of the base is, or may be,
// still running, cheapest check first (design §26.4, phase 2), and returns
// what nothing could answer for.
func (s *Service) stillStopping(ctx context.Context, base *resumeBase) ([]*orkstackv1.ResumeWarning, error) {
	if until := base.history.ContainersStoppedBy(); s.now().Before(until) {
		return nil, waitStopping(causeContainersStopping, until.Sub(s.now()),
			"run %s's containers may still be stopping until %s; try again then", base.run.GetId(), until.UTC().Format(time.RFC3339))
	}
	if err := s.refuseOpenChildren(ctx, base); err != nil {
		return nil, err
	}
	return s.openAttempts(ctx, base)
}

func waitStopping(cause string, retry time.Duration, format string, args ...any) *resumeRefusal {
	r := refuseResume(resumeStillStopping, engine.RefusalWait, format, args...)
	r.Cause, r.RetryAfter = cause, retry
	return r
}

// refuseOpenChildren refuses while a run below the base is open (design
// §28.22, E85-16), so no attempt of one runs beside the resumed run. A child
// the base did not see end is described, which is strongly consistent; a run
// further down, whose own parent closed without it, is found by the root it
// recorded, in an index that is trusted once it has had time to show it.
func (s *Service) refuseOpenChildren(ctx context.Context, base *resumeBase) error {
	if len(base.history.Children) == 0 {
		return nil
	}
	id := base.run.GetId()
	latest := base.history.CloseTime
	var open []string
	var since time.Time              // when the longest-open of them started
	composing := map[string]string{} // child run id → its composed task
	if base.start.Plan.Identity != nil {
		forceCanceled := base.planInput(nil).Class == engine.ResumeBaseForceCanceled
		for _, row := range base.run.GetTaskRuns() {
			c := base.start.Plan.Children[row.GetTaskId()]
			if c != nil && c.RunID != "" && engine.ComposedOnRecordCandidate(row, forceCanceled) {
				composing[c.RunID] = row.GetTaskId()
			}
		}
	}
	for _, child := range base.history.Children {
		if child.Closed {
			continue
		}
		e, err := base.binding.Resume.DescribeResumeExecution(ctx, child.RunID, "")
		switch {
		case errors.Is(err, engine.ErrResumeExecutionNotFound):
			continue // past the cluster's retention: closed
		case err != nil:
			s.logger.WarnContext(ctx, "resume: a run below a run could not be read", "run_id", id, "child_run_id", child.RunID, "error", err)
			return waitStopping(causeChildrenUnconfirmed, resumeCheckRetry,
				"whether run %s, below run %s, is still open can't be told right now; try again shortly", child.RunID, id)
		case e.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || e.Status == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
			if task, ok := composing[child.RunID]; ok {
				// Its outputs may settle the task once it ends: wait for it.
				return s.childRunning(ctx, base, task, child.RunID, e.StartTime)
			}
			open = append(open, child.RunID)
			since = earliest(since, e.StartTime)
		case e.CloseTime.After(latest):
			latest = e.CloseTime
		}
	}
	if len(open) > 0 {
		return s.refuseOpenRuns(base, open, since)
	}
	if trusted := latest.Add(s.resumeSettings.VisibilityLagFloor); s.now().Before(trusted) {
		return waitStopping(causeChildrenUnconfirmed, trusted.Sub(s.now()),
			"whether a run below run %s is still open can't be told until %s; try again then", id, trusted.UTC().Format(time.RFC3339))
	}
	below, err := base.binding.Reader.ListRuns(ctx, engine.RunQuery{
		Namespace: base.namespace, RootRunID: id, Exhaustive: true,
		Keep: func(run *orkstackv1.Run) bool {
			return run.GetOrigin().GetRootRunId() == id && runstatus.IsLive(run.GetStatus())
		},
	})
	if err != nil {
		s.logger.WarnContext(ctx, "resume: the runs below a run could not be listed", "run_id", id, "error", err)
		return waitStopping(causeChildrenUnconfirmed, resumeCheckRetry,
			"whether a run below run %s is still open can't be told right now; try again shortly", id)
	}
	for _, run := range below {
		open = append(open, run.GetId())
		if at := run.GetCreatedAt(); at.IsValid() {
			since = earliest(since, at.AsTime())
		}
	}
	if len(open) > 0 {
		return s.refuseOpenRuns(base, open, since)
	}
	return nil
}

// refuseOpenRuns refuses while runs below the base are open, saying how to
// end them: a force-cancelled base's with a force cancel of its own, which
// reaches every one (design §23.4). Once one has been open longer than
// ChildWaitHint, the refusal leads with ending them (§28.12 F8).
func (s *Service) refuseOpenRuns(base *resumeBase, open []string, since time.Time) *resumeRefusal {
	id := base.run.GetId()
	slices.Sort(open)
	named := strings.Join(open[:min(len(open), maxNamedChildRuns)], ", ")
	if more := len(open) - maxNamedChildRuns; more > 0 {
		named += fmt.Sprintf(" and %d more", more)
	}
	end := "orkctl run cancel"
	if base.class == engine.ResumeBaseForceCanceled {
		end = "orkctl run cancel --force --cascade " + id
	}
	if !since.IsZero() && s.now().Sub(since) > s.resumeSettings.ChildWaitHint {
		return waitStopping(causeChildRunsOpen, resumeCheckRetry,
			"runs below run %s have been open since %s (%s); end them with %s, then try again, or wait for them",
			id, since.UTC().Format(time.RFC3339), named, end)
	}
	return waitStopping(causeChildRunsOpen, resumeCheckRetry,
		"runs below run %s are still open (%s); wait for them, or end them with %s, then try again", id, named, end)
}

// earliest is the earlier of a and b, ignoring a zero time.
func earliest(a, b time.Time) time.Time {
	if a.IsZero() || !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}

// causePinLeasesHeld: every pin host a pin would run again on is full, and
// the run's own old lease holds one.
const causePinLeasesHeld = "pin_leases_held"

// refusePinLeases refuses while a pin that runs again could open nowhere in
// its pool because the run's own old pin still holds a host (design §24.14).
// That ends by itself: a host counts the pins it is running, and frees a
// closed run's at the pin's next answered heartbeat, about ten seconds, long
// before its lease, so the caller is asked to try again within
// resumeCheckRetry, or at the lease if that comes first. It is best effort:
// a pool it cannot read, or one that other runs fill, refuses nothing here.
// rerun's tasks name the pools they were resolved to.
func (s *Service) refusePinLeases(ctx context.Context, base *resumeBase, rerun *orkstackv1.Orkflow, p *resumePlanned) error {
	if s.fleet == nil || !slices.ContainsFunc(rerun.GetTasks(), func(t *orkstackv1.Task) bool { return t.GetPin() != "" }) {
		return nil
	}
	rows, err := s.orkerInventory(ctx)
	if err != nil {
		s.logger.WarnContext(ctx, "resume: the pin hosts could not be read", "run_id", base.run.GetId(), "error", err)
		return nil
	}
	now := s.now()
	seen := map[string]bool{}
	for _, t := range rerun.GetTasks() {
		// A pin is one slot whichever of its tasks runs again: it is warned
		// of, or refused for, once.
		if t.GetPin() == "" || seen[t.GetPin()] {
			continue
		}
		seen[t.GetPin()] = true
		pool := t.GetPool()
		free, own := false, time.Time{}
		for _, host := range rows {
			if host.GetPool() != pool || host.GetCordoned() || host.GetMaxConcurrentPins() == 0 || host.GetUnavailableReason() != "" {
				continue
			}
			free = free || host.GetOpenPins() < host.GetMaxConcurrentPins()
			for _, pin := range host.GetPins() {
				at := pin.GetExpiresAt().AsTime()
				if pin.GetRunId() == base.run.GetId() && pin.GetExpiresAt() != nil && at.After(now) && (own.IsZero() || at.Before(own)) {
					own = at
				}
			}
		}
		if free && !own.IsZero() {
			// A host is free, so the pin opens there, which may be the old
			// pin's own host; that one may still count the slot for a moment.
			p.warnings = append(p.warnings, &orkstackv1.ResumeWarning{
				Kind: orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_PIN_LEASE_HELD, Pin: t.GetPin(), Until: timestamppb.New(own),
				Message: fmt.Sprintf("pin %s's old host may still hold its slot for a moment; the pin opens on a host of pool %s with one free", t.GetPin(), pool),
			})
			continue
		}
		if !free && !own.IsZero() {
			r := waitStopping(causePinLeasesHeld, min(own.Sub(now), resumeCheckRetry),
				"pin %s of run %s still holds its old slot on the last free pin host of pool %s, which frees it shortly; try again then",
				t.GetPin(), base.run.GetId(), pool)
			r.Tasks = []string{t.GetId()}
			return r
		}
	}
	return nil
}

// childRunning refuses, as a wait, the resume of a run whose results the engine
// records while child, the child run of its composed task, is still open: once
// it ends, a resume keeps the outputs it succeeded with (design §28.12 F8). It
// says what the child is waiting on, as far as its state can tell, and once the
// child has been open longer than engine.resume.child_wait_hint it leads with
// ending it.
func (s *Service) childRunning(ctx context.Context, base *resumeBase, task, child string, started time.Time) *resumeRefusal {
	what := "is still running"
	if run, err := base.binding.Reader.GetRun(ctx, child); err == nil {
		what = childWaitsOn(run)
	} else {
		s.logger.WarnContext(ctx, "resume: a composed task's child run's state could not be read", "run_id", base.run.GetId(), "child_run_id", child, "error", err)
	}
	const wait = "resume again once it ends; if it succeeds, its outputs are kept"
	end := fmt.Sprintf("or end it with orkctl run cancel %s, then Run again", child)
	advice := wait + ", " + end
	if !started.IsZero() && s.now().Sub(started) > s.resumeSettings.ChildWaitHint {
		advice = fmt.Sprintf("it has been open since %s: end it with orkctl run cancel %s, then Run again; or %s", started.UTC().Format(time.RFC3339), child, wait)
	}
	r := waitStopping(causeChildRunning, resumeCheckRetry, "child run %s of task %s of run %s %s; %s", child, task, base.run.GetId(), what, advice)
	r.Tasks = []string{task}
	return r
}

// childWaitsOn says what run, a child still open, is waiting on.
func childWaitsOn(run *orkstackv1.Run) string {
	switch run.GetStatus() {
	case orkstackv1.Status_STATUS_PAUSED:
		return "is paused"
	case orkstackv1.Status_STATUS_PENDING:
		return "has not started yet"
	}
	running := 0
	for _, row := range run.GetTaskRuns() {
		switch row.GetStatus() {
		case orkstackv1.Status_STATUS_AWAITING:
			if at := row.GetStartedAt(); at != nil {
				return fmt.Sprintf("is awaiting task %s since %s", row.GetTaskId(), at.AsTime().UTC().Format(time.RFC3339))
			}
			return fmt.Sprintf("is awaiting task %s", row.GetTaskId())
		case orkstackv1.Status_STATUS_RUNNING:
			running++
		}
	}
	return fmt.Sprintf("is running %d tasks", running)
}
