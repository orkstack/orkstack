package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	// resumeProbeTimeout bounds one Orker's answer, as submit's probes are.
	resumeProbeTimeout = 5 * time.Second
	// resumeProbes bounds how many Orkers are asked at once.
	resumeProbes = 8
	// maxNamedAttempts bounds how many attempts a refusal's sentence names.
	maxNamedAttempts = 5
)

// causeContainerStillRunning: an Orker still has a container of the run.
const causeContainerStillRunning = "container_still_running"

// openAttempts asks about every attempt of the run that was claimed and
// never released (design §26.1): the Orker that claimed it still has it, and
// the resume waits; it does not, and the attempt is over; or nothing says,
// and the attempt is returned as a warning a confirm must acknowledge. It is
// asked only past the containers' bound, when a live Orker has had the time
// to stop them.
func (s *Service) openAttempts(ctx context.Context, base *resumeBase) ([]*orkstackv1.ResumeWarning, error) {
	id := base.run.GetId()
	claims, err := s.runClaims(ctx, base)
	if errors.Is(err, artifacts.ErrTooManyOpenClaims) {
		return nil, refuseResume(engine.RefuseTooLarge, engine.RefusalPermanent,
			"too many earlier attempts of run %s may still be running to accept each (%v). Run again", id, err)
	}
	if ctx.Err() != nil {
		return nil, ctxError(ctx)
	}
	if err != nil {
		s.logger.WarnContext(ctx, "resume: a run's attempt claims could not be read", "run_id", id, "error", err)
		r := refuseResume(engine.RefuseCheckUnavailable, engine.RefusalWait,
			"whether an earlier attempt of run %s is still running can't be checked right now; try again shortly", id)
		r.RetryAfter = resumeCheckRetry
		return nil, r
	}
	if len(claims) == 0 {
		return nil, nil
	}
	answers := s.askOrkers(ctx, claims)
	seen := s.orkersLastSeen(ctx)
	var running []artifacts.OpenAttemptClaim
	var warnings []*orkstackv1.ResumeWarning
	for _, c := range claims {
		answer := answers[c.OrkerID]
		switch {
		case answer != nil && slices.ContainsFunc(answer.GetRunning(), func(a *orkstackv1.RunningAttempt) bool { return runs(a, c.Ref) }):
			running = append(running, c)
		case answer == nil || answer.GetRunningTruncated():
			warnings = append(warnings, unknownAttempt(c, seen[c.OrkerID]))
		}
	}
	if len(running) > 0 {
		s.resumeAnomaly(ctx, base.namespace, anomalyContainerAfterBound)
		r := waitStopping(causeContainerStillRunning, resumeCheckRetry,
			"a container of run %s is still running past its time (%s); cordon that Orker and restart it, then try again",
			id, nameAttempts(running))
		r.Tasks = claimTasks(running)
		return nil, r
	}
	return warnings, nil
}

// listsClaims reports whether the attempt store can say which attempts are
// open.
func (s *Service) listsClaims() bool {
	_, ok := s.attempts.(artifacts.RunClaimReader)
	return ok
}

// runClaims are the open claims of the run's tasks, whichever of its
// executions dispatched the attempt: an earlier execution's are not in the
// base's history. A claim of no task of the definition is no attempt of the
// run, and an Orker's name that could be none is unknown.
func (s *Service) runClaims(ctx context.Context, base *resumeBase) ([]artifacts.OpenAttemptClaim, error) {
	reader, ok := s.attempts.(artifacts.RunClaimReader)
	if !ok {
		return nil, errors.New("the attempt store lists no claims")
	}
	open, err := reader.OpenAttemptClaims(ctx, base.run.GetId())
	if err != nil {
		return nil, err
	}
	tasks := map[string]bool{}
	for _, t := range base.definition.GetTasks() {
		tasks[t.GetId()] = true
	}
	open = slices.DeleteFunc(open, func(c artifacts.OpenAttemptClaim) bool { return !tasks[c.Ref.TaskID] })
	for i := range open {
		if task.ValidOrkerID(open[i].OrkerID) != nil {
			open[i].OrkerID = ""
		}
	}
	return open, nil
}

// askOrkers is the answer of each Orker that claimed one of claims, on its
// own control channel: nil for one that cannot be asked or does not answer
// as itself.
func (s *Service) askOrkers(ctx context.Context, claims []artifacts.OpenAttemptClaim) map[string]*orkstackv1.DescribeOrkerResult {
	ids := map[string]bool{}
	for _, c := range claims {
		ids[c.OrkerID] = true
	}
	answers := make(map[string]*orkstackv1.DescribeOrkerResult, len(ids))
	if s.fleet == nil {
		return answers
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	slots := make(chan struct{}, resumeProbes)
	for id := range maps.Keys(ids) {
		if id == "" {
			continue
		}
		wg.Go(func() {
			slots <- struct{}{}
			defer func() { <-slots }()
			probe, cancel := context.WithTimeout(ctx, resumeProbeTimeout)
			defer cancel()
			answer, err := s.fleet.DescribeOrkerByID(probe, id)
			if err != nil || answer.GetOrkerId() != id {
				s.logger.InfoContext(ctx, "resume: an Orker did not answer for its attempts", "orker_id", id, "error", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			answers[id] = answer
		})
	}
	wg.Wait()
	return answers
}

// orkersLastSeen is when each Orker was last seen, for a warning to say; it
// decides nothing, so a failed read says nothing.
func (s *Service) orkersLastSeen(ctx context.Context) map[string]time.Time {
	if s.fleet == nil {
		return nil
	}
	presence, err := s.fleet.ObserveOrkers(ctx)
	if err != nil {
		return nil
	}
	seen := make(map[string]time.Time, len(presence))
	for _, p := range presence {
		seen[p.Identity] = p.LastSeen
	}
	return seen
}

// runs reports whether a is the attempt ref names.
func runs(a *orkstackv1.RunningAttempt, ref artifacts.AttemptRef) bool {
	return a.GetRunId() == ref.RunID && a.GetTaskId() == ref.TaskID && a.GetOrdinal() == ref.Attempt &&
		(a.Item != nil) == ref.Item.Set && a.GetItem() == ref.Item.Index
}

// unknownAttempt is the warning of an attempt nothing answers for.
func unknownAttempt(c artifacts.OpenAttemptClaim, seen time.Time) *orkstackv1.ResumeWarning {
	w := &orkstackv1.ResumeWarning{
		Kind:    orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_ATTEMPT_OUTCOME_UNKNOWN,
		OrkerId: c.OrkerID, TaskId: c.Ref.TaskID, Ordinal: c.Ref.Attempt, AcknowledgeKey: acknowledgeKey(c),
		Message: fmt.Sprintf("%s may still be running: its Orker does not answer for it", nameAttempt(c)),
	}
	if c.Ref.Item.Set {
		item := c.Ref.Item.Index
		w.Item = &item
	}
	if !seen.IsZero() {
		w.LastSeenAt = timestamppb.New(seen)
	}
	return w
}

// acknowledgeKey is an attempt's stable identity: no time and no state, so an
// acknowledgement outlives both (design §26.6).
func acknowledgeKey(c artifacts.OpenAttemptClaim) string {
	item := ""
	if c.Ref.Item.Set {
		item = strconv.Itoa(int(c.Ref.Item.Index))
	}
	return fmt.Sprintf("%s/%s/%s/%d", c.OrkerID, c.Ref.TaskID, item, c.Ref.Attempt)
}

func nameAttempt(c artifacts.OpenAttemptClaim) string {
	unit := engine.ResumeUnit{Task: c.Ref.TaskID, Item: -1}
	if c.Ref.Item.Set {
		unit.Item = int(c.Ref.Item.Index)
	}
	return fmt.Sprintf("attempt %d of %s on Orker %s", c.Ref.Attempt, unit, cmp.Or(c.OrkerID, "(unknown)"))
}

func nameAttempts(claims []artifacts.OpenAttemptClaim) string {
	var names []string
	for _, c := range claims[:min(len(claims), maxNamedAttempts)] {
		names = append(names, nameAttempt(c))
	}
	out := strings.Join(names, ", ")
	if more := len(claims) - maxNamedAttempts; more > 0 {
		out += fmt.Sprintf(" and %d more", more)
	}
	return out
}

// claimTasks are the tasks of claims, each once, in order.
func claimTasks(claims []artifacts.OpenAttemptClaim) []string {
	var out []string
	for _, c := range claims {
		if !slices.Contains(out, c.Ref.TaskID) {
			out = append(out, c.Ref.TaskID)
		}
	}
	return out
}

// refuseUnacknowledged refuses a confirm that does not accept every attempt
// whose outcome is unknown now; accepting more than are unknown refuses
// nothing (design §26.6).
func refuseUnacknowledged(runID string, warnings []*orkstackv1.ResumeWarning, acknowledged []string) error {
	accepted := make(map[string]bool, len(acknowledged))
	for _, key := range acknowledged {
		accepted[key] = true
	}
	var missing, tasks []string
	for _, w := range warnings {
		if w.GetKind() != orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_ATTEMPT_OUTCOME_UNKNOWN || accepted[w.GetAcknowledgeKey()] {
			continue
		}
		missing = append(missing, w.GetMessage())
		if !slices.Contains(tasks, w.GetTaskId()) {
			tasks = append(tasks, w.GetTaskId())
		}
	}
	if len(missing) == 0 {
		return nil
	}
	named := strings.Join(missing[:min(len(missing), maxNamedAttempts)], "; ")
	if more := len(missing) - maxNamedAttempts; more > 0 {
		named += fmt.Sprintf("; and %d more", more)
	}
	r := refuseResume(resumeUnknownAttempts, engine.RefusalReplan,
		"run %s has attempts whose outcome is unknown (%s); preview again and accept them to resume anyway", runID, named)
	r.Tasks = tasks
	return r
}
