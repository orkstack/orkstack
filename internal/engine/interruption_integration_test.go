//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// AGENTS.md §11's interruption coverage (E92 PR 7c), on a real cluster.

// TestIntegration_ACommitWrittenBeforeALossIsAdoptedWithoutAnAttempt: a task
// with no retry: whose Orker wrote commit.json and was then killed before it
// reported runs again — an interruption, after the loss's fence — on another
// Orker, which adopts the commit and starts no container. The run succeeds on
// the task's one attempt. Real Orkers, real containers, the real store.
func TestIntegration_ACommitWrittenBeforeALossIsAdoptedWithoutAnAttempt(t *testing.T) {
	commitThenLoss(t, "interruption/commit-then-loss", false)
}

// TestIntegration_ACommitIsAdoptedAfterALossEvenWhenTheOrdinalClaimFails: the
// same loss, with Orker B's attempt store refusing every ordinal claim. The
// Orker checks for a commit before it claims (docs/OCI_ACTIVITY_CONTRACT.md
// §7.1), so B still adopts the commit and the run succeeds; B writes no
// attempt record, having no ordinal to write it under.
func TestIntegration_ACommitIsAdoptedAfterALossEvenWhenTheOrdinalClaimFails(t *testing.T) {
	commitThenLoss(t, "interruption/commit-then-loss-no-claim", true)
}

// refusingClaims is an attempt store whose every ordinal claim fails: a
// hand-written fake for a fault case, since a real object store refuses a
// claim only when it is down or overloaded at that very request, which no
// test brings about on demand. Everything else is the real store's.
type refusingClaims struct {
	artifacts.AttemptStore
	release artifacts.AttemptClaimer
}

func (r refusingClaims) ClaimAttempt(context.Context, artifacts.AttemptRef, string) (artifacts.AttemptClaimed, error) {
	return artifacts.AttemptClaimed{}, errors.New("503 Slow Down: the attempt store refuses the claim")
}

func (r refusingClaims) ReleaseAttempt(ctx context.Context, ref artifacts.AttemptRef, orkerID string) error {
	return r.release.ReleaseAttempt(ctx, ref, orkerID)
}

func commitThenLoss(t *testing.T, scenario string, refuseClaims bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, scenario)
	stores := s.realStores()
	realQueue := mustQueueForPool(t, "real")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	committed := make(chan string, 1)
	var once sync.Once
	proxyA := s.temporalProxy()
	orkerA, idA := s.startRealOrker("orker-a", "real", realQueue,
		hookedCommitStores(stores, func(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
			record, err := stores.commits.Create(ctx, uri, c)
			if err != nil {
				return record, err
			}
			once.Do(func() { committed <- uri })
			<-release
			return artifacts.CommitRecord{}, errors.New("orker A was killed after its commit")
		}), s.dial("orker-a", proxyA.Addr()), nil)

	// No retry: one attempt of its own, so only an interruption runs it again.
	wf := realTaskDocument(t, "interruption-commit-then-loss", pinnedAlpineForCommitE2E(t),
		`printf '{"ok":true}' > /orkstack/output/values.json`, "")
	for _, tk := range wf.GetTasks() {
		tk.CancellationGracePeriodSeconds = 1
	}
	plan := realTaskPlan(t, s, realQueue)
	budgets := engine.DefaultInterruptionBudgets
	plan.Interruptions = &budgets
	runID := s.newRunID()
	s.transcript.run(runID)
	starts := watchContainerStarts(t, runID)
	s.start(runID, wf, nil, plan)
	stream := s.openStream(s.reader, runID, 0)

	select {
	case uri := <-committed:
		s.transcript.step("Orker A wrote %s and is held before it reports completion", uri)
	case <-time.After(3 * time.Minute):
		t.Fatal("Orker A never wrote the task's commit.json")
	}
	proxyA.Cut()
	orkerA.stop()
	storesB := stores
	if refuseClaims {
		claimer, ok := stores.attempts.(artifacts.AttemptClaimer)
		if !ok {
			t.Fatalf("the stack's attempt store %T claims no ordinals", stores.attempts)
		}
		refusing := *stores
		refusing.attempts = refusingClaims{AttemptStore: stores.attempts, release: claimer}
		storesB = &refusing
	}
	_, idB := s.startRealOrker("orker-b", "real", realQueue, storesB, nil, nil)
	s.transcript.step("Orker A killed after its commit; Orker B started (claims refused: %v)", refuseClaims)

	status, err := s.wait(runID, 5*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v (a task with no retry: must survive its Orker's loss)", status, err)
	}
	s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	run, err := s.reader.GetRun(ctx, runID)
	must.OK(t, err)
	st := taskRunOfRun(run, "work").GetInterruptions()
	if st.GetLost() != 1 || st.GetOwnAttemptsUsed() != 0 || st.GetLast().GetReason() != "ORKER_HEARTBEAT_LOST" {
		t.Fatalf("interruptions = %v, want one loss and no own attempt", st)
	}
	records, err := stores.attempts.ListAttempts(ctx, runID, "work", artifacts.NoItem)
	must.OK(t, err)
	var adopted, byB bool
	for _, r := range records {
		byB = byB || r.GetOrkerId() == idB
		if r.GetOrkerId() == idB && r.GetCommitAdopted() && r.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
			adopted = true
			requireNoContainerPhase(t, r)
		}
	}
	switch {
	case refuseClaims && byB:
		t.Fatalf("Orker B wrote an attempt record with every claim refused: %v", records)
	case refuseClaims:
		// No ordinal, so no record: the commit row and the run say it.
		if taskRunOfRun(run, "work").GetCommit().GetUri() == "" {
			t.Fatal("the run's task row names no commit")
		}
	case !adopted:
		t.Fatalf("no attempt on %s adopted the commit: %v", idB, records)
	}
	if n := commitObjectCount(t, s.cfg, runID); n != 1 {
		t.Fatalf("%d commit.json objects, want exactly the one Orker A wrote", n)
	}
	starts.awaitBy(t, idA, 1, time.Minute)
	starts.requireLive(t)
	if a, b := starts.by(idA), starts.by(idB); a != 1 || b != 0 {
		t.Fatalf("containers started for the run: %d by Orker A, %d by Orker B; want 1 and 0", a, b)
	}
}

// fenceEnd is when the loss fence a history armed ends: its longest Timer's
// start plus its duration.
func fenceEnd(h *historypb.History) (time.Time, time.Duration) {
	var end time.Time
	var longest time.Duration
	for _, e := range h.GetEvents() {
		if attrs := e.GetTimerStartedEventAttributes(); attrs != nil && attrs.GetStartToFireTimeout().AsDuration() > longest {
			longest = attrs.GetStartToFireTimeout().AsDuration()
			end = e.GetEventTime().AsTime().Add(longest)
		}
	}
	return end, longest
}

// TestIntegration_ALossWhoseFenceSpansAContinuationWaitsItsRecordedInstant: a
// task lost with its container started waits out its fence; the run carries
// on meanwhile, and the next generation dispatches the task again no sooner
// than the instant the first recorded.
func TestIntegration_ALossWhoseFenceSpansAContinuationWaitsItsRecordedInstant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	orker := &scriptedOrker{steps: map[string][]orkerStep{
		"work": {{beat: &task.HeartbeatProgress{Phase: "run", OrkerID: "orker-lost", Started: true}}},
	}, calls: map[string]int{}}
	s := startPacedCANStackKeeping(t, ctx, "ir-fence", orker.behavior, orker.losing)
	runID := fmt.Sprintf("ork-ir-fence-%d", time.Now().UnixNano())
	budget := engine.DefaultContinuationBudget(30, 1<<20, 3*time.Minute)
	must.OKf(t, s.starter.StartRun(s.ctx, runID, compile(t, interruptionFixtureDocument), nil,
		interruptionFixturePlan(s, &budget, nil)), "StartRun")
	must.OKf(t, s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil), "the run failed")
	described, err := describeExecution(s.client, s.ctx, runID, "")
	must.OK(t, err)
	chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) < 2 {
		t.Fatalf("the run had %d generations, want the fence to span a continuation", len(chain))
	}
	g0 := s.history(runID, chain[0])
	// The fence ends no sooner than 31 s after the cluster recorded the loss.
	var end time.Time
	for _, e := range g0.GetEvents() {
		if e.GetEventType() == enums.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT {
			end = e.GetEventTime().AsTime().Add(31 * time.Second)
		}
	}
	if end.IsZero() {
		t.Fatal("generation 0 recorded no heartbeat timeout")
	}
	if timerEnd, fence := fenceEnd(g0); fence == 0 || timerEnd.Before(end) {
		t.Fatalf("generation 0's fence timer ends at %v (%v long), want no sooner than %v", timerEnd, fence, end)
	}
	var dispatched []time.Time
	for _, execution := range chain[1:] {
		for _, e := range s.history(runID, execution).GetEvents() {
			if attrs := e.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
				dispatched = append(dispatched, e.GetEventTime().AsTime())
			}
		}
	}
	if len(dispatched) != 1 {
		t.Fatalf("later generations dispatched the task %d times, want once", len(dispatched))
	}
	if dispatched[0].Before(end) {
		t.Fatalf("the task was dispatched again at %v, before its fence ended at %v", dispatched[0], end)
	}
	if got := orker.count("work"); got != 2 {
		t.Fatalf("work ran %d times, want 2", got)
	}
}

// TestIntegration_AnInterruptionStormStaysWithinTheHistoryBound: forty items,
// each handed back before it started three times, all run and succeed; every
// generation stays within the run's history limit, and the run carries on
// rather than being terminated by the cluster.
func TestIntegration_AnInterruptionStormStaysWithinTheHistoryBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	const items, bounces = 40, 3
	steps := map[string][]orkerStep{}
	parts := make([]any, items)
	for i := range items {
		key := fmt.Sprintf("spread#%d", i)
		parts[i] = fmt.Sprintf("p%d", i)
		for range bounces {
			steps[key] = append(steps[key], orkerStep{err: stopBounceError()})
		}
	}
	orker := &scriptedOrker{steps: steps, calls: map[string]int{}}
	s := startCANStack(t, ctx, "ir-storm", true, orker.behavior)
	const maxEvents = 2000
	budget := engine.DefaultContinuationBudget(600, 4<<20, 3*time.Minute)
	plan := interruptionFixturePlan(s, &budget, nil)
	plan.History.MaxEvents = maxEvents
	runID := fmt.Sprintf("ork-ir-storm-%d", time.Now().UnixNano())
	must.OKf(t, s.starter.StartRun(s.ctx, runID, compile(t, strings.Replace(interruptionItemsDocument, "max_items: 10", "max_items: 100", 1)),
		payload(t, map[string]any{"parts": parts}), plan), "StartRun")
	must.OKf(t, s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil), "the storm run failed")
	described, err := describeExecution(s.client, s.ctx, runID, "")
	must.OK(t, err)
	chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	for i, execution := range chain {
		if n := len(s.history(runID, execution).GetEvents()); n > maxEvents {
			t.Fatalf("generation %d holds %d events, over the run's limit of %d", i, n, maxEvents)
		}
	}
	for i := range items {
		if got := orker.count(fmt.Sprintf("spread#%d", i)); got != bounces+1 {
			t.Fatalf("item %d ran %d times, want %d", i, got, bounces+1)
		}
	}
	t.Logf("%d items × %d bounces across %d generations, each within %d events", items, bounces, len(chain), maxEvents)
}
