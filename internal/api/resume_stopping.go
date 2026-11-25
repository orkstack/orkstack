package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

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
			open = append(open, child.RunID)
		case e.CloseTime.After(latest):
			latest = e.CloseTime
		}
	}
	if len(open) > 0 {
		return refuseOpenRuns(id, open)
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
	}
	if len(open) > 0 {
		return refuseOpenRuns(id, open)
	}
	return nil
}

func refuseOpenRuns(id string, open []string) *resumeRefusal {
	slices.Sort(open)
	named := strings.Join(open[:min(len(open), maxNamedChildRuns)], ", ")
	if more := len(open) - maxNamedChildRuns; more > 0 {
		named += fmt.Sprintf(" and %d more", more)
	}
	return waitStopping(causeChildRunsOpen, resumeCheckRetry,
		"runs below run %s are still open (%s); wait for them, or end them with orkctl run cancel, then try again", id, named)
}

// causePinLeasesHeld: every pin host a pin would run again on is full, and
// the run's own old lease holds one.
const causePinLeasesHeld = "pin_leases_held"

// refusePinLeases refuses while a pin that runs again could open nowhere in
// its pool because the run's own old pin still holds a host (design §24.14):
// that ends by itself, at the lease. It is best effort: a pool it cannot
// read, or one that other runs fill, refuses nothing here. rerun's tasks name
// the pools they were resolved to.
func (s *Service) refusePinLeases(ctx context.Context, base *resumeBase, rerun *orkstackv1.Orkflow) error {
	if s.fleet == nil || !slices.ContainsFunc(rerun.GetTasks(), func(t *orkstackv1.Task) bool { return t.GetPin() != "" }) {
		return nil
	}
	rows, err := s.orkerInventory(ctx)
	if err != nil {
		s.logger.WarnContext(ctx, "resume: the pin hosts could not be read", "run_id", base.run.GetId(), "error", err)
		return nil
	}
	now := s.now()
	for _, t := range rerun.GetTasks() {
		if t.GetPin() == "" {
			continue
		}
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
		if !free && !own.IsZero() {
			r := waitStopping(causePinLeasesHeld, own.Sub(now),
				"pin %s of run %s still holds its old slot until %s, and no other pin host of pool %s is free; try again then",
				t.GetPin(), base.run.GetId(), own.UTC().Format(time.RFC3339), pool)
			r.Tasks = []string{t.GetId()}
			return r
		}
	}
	return nil
}
