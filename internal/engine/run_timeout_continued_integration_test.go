//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// runTimeoutCANBehavior: "slow" heartbeats until it is cancelled; every
// other task succeeds at once.
func runTimeoutCANBehavior(ctx context.Context, _ *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetTaskId() == "slow" {
		for {
			activity.RecordHeartbeat(ctx)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
		ValuesJson: valuesJSON(map[string]any{"task": in.GetTaskId()})}, nil
}

// chainDocument is a chain of n quick tasks c1 → … → cn, then slow, under a
// run timeout; parallel puts slow beside the chain instead of after it.
func chainDocument(name, timeout string, n int, parallel bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntimeout: %s\ntasks:\n", name, timeout)
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "  - id: c%d\n    uses: shell\n    with: {op: exec}\n", i)
		if i > 1 {
			fmt.Fprintf(&b, "    needs: [c%d]\n", i-1)
		}
	}
	b.WriteString("  - id: slow\n    uses: shell\n    with: {op: exec}\n")
	if !parallel {
		fmt.Fprintf(&b, "    needs: [c%d]\n", n)
	}
	return b.String()
}

// timeoutPlanOn is the stack's plan with a run timeout of grace seconds.
func timeoutPlanOn(s *canStack, maxEvents int64, budget *engine.ContinuationBudget) engine.RunPlan {
	plan := s.plan(maxEvents, budget)
	plan.RunTimeout = &engine.RunTimeoutPlan{HandlerGraceSeconds: 2, ResolutionsPerPass: engine.DefaultResolutionsPerPass}
	return plan
}

// closedTimedOut reports whether a history closed failed RUN_TIMED_OUT.
func closedTimedOut(h *historypb.History) bool {
	events := h.GetEvents()
	attrs := events[len(events)-1].GetWorkflowExecutionFailedEventAttributes()
	return attrs != nil && attrs.GetFailure().GetApplicationFailureInfo().GetType() == engine.RunTimedOutCode
}

// longTimers counts the timers of at least a second a history started: the
// run timeout's deadline timer, never a bootstrap yield.
func longTimers(h *historypb.History) int {
	n := 0
	for _, event := range h.GetEvents() {
		if attrs := event.GetTimerStartedEventAttributes(); attrs != nil && attrs.GetStartToFireTimeout().AsDuration() >= time.Second {
			n++
		}
	}
	return n
}

// dispatchedTasks is every task id a history scheduled ExecuteTask for.
func dispatchedTasks(t *testing.T, h *historypb.History) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, event := range h.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityType().GetName() == "ExecuteTask" {
			out[recordedPayload(t, attrs.GetInput(), 0)["taskId"].(string)] = true
		}
	}
	return out
}

func continuedAsNew(h *historypb.History) bool {
	events := h.GetEvents()
	return events[len(events)-1].GetEventType() == enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
}

// recordTwoGenerations runs document under plan and returns its two
// generations once it has ended.
func recordTwoGenerations(t *testing.T, s *canStack, label, document string, plan engine.RunPlan) (string, *historypb.History, *historypb.History, error) {
	t.Helper()
	runID := fmt.Sprintf("ork-run-timeout-%s-%d", label, time.Now().UnixNano())
	must.OKf(t, s.starter.StartRun(s.ctx, runID, compile(t, document), nil, plan), "StartRun")
	runErr := s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil)
	described, err := s.client.DescribeWorkflowExecution(s.ctx, runID, "")
	must.OK(t, err)
	chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) != 2 {
		return runID, nil, nil, fmt.Errorf("%d generations, want 2 (ended %v)", len(chain), runErr)
	}
	return runID, s.history(runID, chain[0]), s.history(runID, chain[1]), nil
}

// TestIntegration_RecordRunTimeoutContinuedHistories records the run-timeout
// fixtures whose commands depend on the continuation's carry and hold
// (runTimeoutContinuedFixtures, replay_run_timeout_test.go), each from a real
// run on a real cluster by a versioned worker:
//
//   - carried: a chain that switches mid-way; generation 1 arms the
//     deadline it carried, with what remains of it, and times out in slow;
//   - past-deadline: the same chain, generation 1's saved-state reads slowed
//     until the deadline has passed, so its first pass takes the timeout and
//     its arm site issues no timer;
//   - hold: slow beside a long chain under a small history limit, so the run
//     crosses its budget with slow running, holds new dispatch at its hold
//     line and times out while held.
func TestIntegration_RecordRunTimeoutContinuedHistories(t *testing.T) {
	// It never skips: by default the recordings land under ORK_PROOF_DIR and
	// the committed fixtures are left alone; ORK_RECORD_GOLDEN_HISTORY=1 also
	// writes them. It fails unless every shape lands and replays.
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	files := map[string]*historypb.History{}

	// carried.
	{
		s := startCANStack(t, ctx, "rt-carried", true, runTimeoutCANBehavior)
		budget := engine.DefaultContinuationBudget(100, 1<<20, 3*time.Minute)
		runID, g0, g1, err := recordTwoGenerations(t, s, "carried", chainDocument("rt-carried", "20s", 8, false),
			timeoutPlanOn(s, engine.DefaultHistoryMaxEvents, &budget))
		must.OKf(t, err, "carried")
		switch {
		case !continuedAsNew(g0) || longTimers(g0) != 1:
			t.Fatalf("carried g0: switched %v, deadline timers %d; want a switch after arming one", continuedAsNew(g0), longTimers(g0))
		case !closedTimedOut(g1) || longTimers(g1) != 1 || !dispatchedTasks(t, g1)["slow"]:
			t.Fatalf("carried g1: timed out %v, deadline timers %d, slow dispatched %v; want the carried deadline armed and taken in slow",
				closedTimedOut(g1), longTimers(g1), dispatchedTasks(t, g1)["slow"])
		}
		for name, h := range map[string]*historypb.History{"g0": g0, "g1": g1} {
			must.OKf(t, replayRecorded(h, runID), "carried %s just recorded does not replay", name)
		}
		files[runTimeoutCarriedG0] = g0
		files[runTimeoutCarriedG1] = g1
	}

	// past-deadline: every chunk read waits 4s, one chunk per workflow task.
	{
		s := startCANStackReading(t, ctx, "rt-past", true, runTimeoutCANBehavior,
			func(ctx context.Context, _ int64) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(4 * time.Second):
					return nil
				}
			})
		budget := engine.DefaultContinuationBudget(100, 1<<20, 5*time.Minute)
		budget.ChunkBytes = 128
		budget.ChunksPerTask = 1
		runID, g0, g1, err := recordTwoGenerations(t, s, "past", chainDocument("rt-past", "10s", 8, false),
			timeoutPlanOn(s, engine.DefaultHistoryMaxEvents, &budget))
		must.OKf(t, err, "past-deadline")
		switch {
		case !continuedAsNew(g0):
			t.Fatal("past-deadline g0 did not switch")
		case !closedTimedOut(g1) || longTimers(g1) != 0 || len(dispatchedTasks(t, g1)) != 0:
			t.Fatalf("past-deadline g1: timed out %v, deadline timers %d, dispatched %v; want a timeout on its first pass with no timer and no dispatch",
				closedTimedOut(g1), longTimers(g1), dispatchedTasks(t, g1))
		}
		for name, h := range map[string]*historypb.History{"g0": g0, "g1": g1} {
			must.OKf(t, replayRecorded(h, runID), "past-deadline %s just recorded does not replay", name)
		}
		files[runTimeoutPastG0] = g0
		files[runTimeoutPastG1] = g1
	}

	// hold: the limits that land a hold depend on the cluster's own event
	// accounting; each candidate is tried in turn.
	{
		s := startCANStack(t, ctx, "rt-hold", true, runTimeoutCANBehavior)
		var tried []string
		for _, maxEvents := range []int64{700, 800, 600, 1000} {
			budget := engine.DefaultContinuationBudget(20, 1<<20, 3*time.Minute)
			runID := fmt.Sprintf("ork-run-timeout-hold-%d", time.Now().UnixNano())
			must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, chainDocument("rt-hold", "30s", 20, true)), nil,
				timeoutPlanOn(s, maxEvents, &budget)), "StartRun")
			runErr := s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil)
			h := s.history(runID, "")
			ran := dispatchedTasks(t, h)
			var applicationErr *temporal.ApplicationError
			if errors.As(runErr, &applicationErr) && applicationErr.Type() == engine.RunTimedOutCode &&
				!continuedAsNew(h) && ran["slow"] && ran["c5"] && !ran["c20"] {
				must.OKf(t, replayRecorded(h, runID), "hold just recorded does not replay")
				files[runTimeoutHold] = h
				t.Logf("the hold landed at history.max_events=%d (tried before: %v)", maxEvents, tried)
				break
			}
			tried = append(tried, fmt.Sprintf("%d: ended %v, %d dispatched, switched %v", maxEvents, runErr, len(ran), continuedAsNew(h)))
		}
		if files[runTimeoutHold] == nil {
			t.Fatalf("no candidate landed the hold:\n%s", strings.Join(tried, "\n"))
		}
	}

	for path, h := range files {
		data := encodeHistory(t, h)
		proof.Write(t, "run-timeout", filepath.Base(path), data)
		if write {
			must.OKf(t, os.WriteFile(path, data, 0o644), "write %s", path)
		}
		t.Logf("recorded %d events for %s (fixture written: %v)", len(h.GetEvents()), filepath.Base(path), write)
	}
}
