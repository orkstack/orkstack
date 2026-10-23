//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The continuation chaos suite: the continue-as-new scenarios C1–C31
// (specs/continue-as-new T3), against real Temporal and real MinIO, with the
// table TC's settings. Each scenario writes its transcript, the figures E3
// quotes and its runs' workflow log lines under
// artifacts/continuation-chaos/C<n>/. A scenario that proves something did
// NOT happen in a window also observes, in that window, that the run was
// making progress (AGENTS.md §11.1).

// flatAndChain is a definition with width tasks w1..wN that start together
// and a chain c1..cM beside them.
func flatAndChain(name string, width, chain int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	for i := 1; i <= width; i++ {
		fmt.Fprintf(&b, "  - id: w%d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	b.WriteString(guardChain("c", chain, ""))
	return b.String()
}

func taskKeys(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i+1)
	}
	return out
}

// TestIntegration_ContinuationChaosC1_HoldWithTwelveTasksRunning: a run
// crosses its budget with 12 tasks running and reaches its hold line. No
// running task is cancelled or dispatched twice, the run switches only after
// the last of them finishes, and its stream has no gap or duplicate.
func TestIntegration_ContinuationChaosC1_HoldWithTwelveTasksRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C1")
	g := newGates()
	running := taskKeys("w", 12)
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, running...))
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, flatAndChain("c1-hold", 12, 12)), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	hold := s.logs.wait(t, ctx, logQuiesce, runID, 2*time.Minute)
	for _, key := range running {
		g.waitEntered(t, key, time.Minute)
	}
	s.transcript.step("hold reached with %v in flight (history %v events, reserve %v)",
		hold.Fields["in_flight"], hold.Fields["history_events"], hold.Fields["reserve_events"])
	// Every one of the 12 was running when the hold began: each entered its
	// gate, and none is released before the hold.
	if inFlight, ok := hold.Fields["in_flight"].(int); !ok || inFlight < 12 {
		t.Fatalf("the hold began with %v in flight, want the 12 running tasks among them", hold.Fields["in_flight"])
	}

	// Release the running tasks one at a time. While any still runs the run
	// must not switch — and it is observed to be making progress meanwhile:
	// each release lands as a completion in its history.
	for i, key := range running {
		g.open(key)
		deadline := time.Now().Add(time.Minute)
		for {
			execution, _ := s.current(runID)
			facts := factsOf(t, s.history(runID, execution))
			if len(facts.completedAt[key]) == 1 {
				if i < len(running)-1 && (facts.continuedAt != 0 || len(s.chain(runID)) != 1) {
					t.Fatalf("the run switched with %d of the 12 still running", len(running)-1-i)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s's completion never landed", key)
			}
			time.Sleep(100 * time.Millisecond)
		}
		s.transcript.step("%s released and completed; %d still running, no switch", key, len(running)-1-i)
	}
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}

	chain, histories := s.histories(runID)
	if len(chain) < 2 {
		t.Fatalf("%d executions: the run never switched", len(chain))
	}
	g0 := factsOf(t, histories[0])
	if g0.continuedAt == 0 {
		t.Fatal("generation 0 did not continue as new")
	}
	if last := g0.lastCompletion(running...); last == 0 || last > g0.continuedAt {
		t.Fatalf("generation 0 switched at event %d, before the last running task completed at %d", g0.continuedAt, last)
	}
	for _, key := range running {
		attempts, canceled := s.attemptsOf(key)
		if attempts != 1 || canceled != 0 || len(g0.canceledAt[key]) != 0 || len(g0.startedAttempt[key]) != 1 ||
			g0.startedAttempt[key][0] != 1 {
			t.Fatalf("%s: %d attempts handed out (%d canceled), history attempts %v, canceled events %v: want one attempt, "+
				"never cancelled", key, attempts, canceled, g0.startedAttempt[key], g0.canceledAt[key])
		}
		for i := 1; i < len(histories); i++ {
			if later := factsOf(t, histories[i]); len(later.scheduledAt[key]) != 0 {
				t.Fatalf("%s was dispatched again in generation %d", key, i)
			}
		}
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.transcript.figure("generations", len(chain))
	s.transcript.figure("switch_event", g0.continuedAt)
	s.transcript.figure("last_running_completion_event", g0.lastCompletion(running...))
	s.transcript.figure("events", len(events))
	t.Logf("C1: hold with 12 running; switch at event %d after the last completion at %d; %d generations; %d events contiguous",
		g0.continuedAt, g0.lastCompletion(running...), len(chain), len(events))
}

// c2Run is C2's run: "slow", a real container on the real Orkers' pool with
// three attempts, beside a 40-task chain on the stack's own Orker that grows
// the run's history to its hold line while slow runs.
func (s *chaosStack) c2Run(t *testing.T, image, command, realQueue string) (string, *streamCollector) {
	t.Helper()
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: c2-orker-killed\nversion: \"1.0.0\"\ntasks:\n" +
		"  - id: slow\n    uses: shell\n    needs: []\n    pool: real\n    retry: {maximum_attempts: 3}\n    with: {op: exec}\n" +
		guardChain("c", 40, "")
	wf := compile(t, doc)
	for _, tk := range wf.GetTasks() {
		if tk.GetId() == "slow" {
			tk.Uses = image
			tk.Image = &orkstackv1.ImageRef{Reference: image, Pinned: image}
			tk.Command = []string{"/bin/sh", "-c", command}
			tk.TimeoutSeconds = 120
			tk.Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE
		}
	}
	plan, _ := s.tcPlan()
	plan.Routes["real"] = realQueue
	root, err := artifacts.CommitRoot(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	plan.CommitRoot = root
	plan.Limits = engine.DefaultLimits()
	plan.TransferBudgetSeconds = 300
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, wf, nil, plan)
	return runID, s.openStream(s.reader, runID, 0)
}

// retriedAfterHeartbeatTimeout reports the attempt slow's last start
// recorded, and whether it retried a heartbeat timeout.
func retriedAfterHeartbeatTimeout(t *testing.T, h *historypb.History, key string) (attempt int32, heartbeat bool) {
	t.Helper()
	facts := factsOf(t, h)
	for _, event := range h.GetEvents() {
		attrs := event.GetActivityTaskStartedEventAttributes()
		if attrs == nil || facts.tasks[attrs.GetScheduledEventId()] != key {
			continue
		}
		attempt = attrs.GetAttempt()
		heartbeat = attrs.GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType() == enums.TIMEOUT_TYPE_HEARTBEAT
	}
	return attempt, heartbeat
}

// TestIntegration_ContinuationChaosC2_OrkerKilledMidHoldRetriesElsewhere: a
// real Orker is killed mid-hold with its container running — it becomes
// unreachable, as a dead host is, and its worker stops. The heartbeat
// timeout retries the task on another Orker, the hold waits for that retry,
// and only then does the run switch; it completes.
func TestIntegration_ContinuationChaosC2_OrkerKilledMidHoldRetriesElsewhere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C2/killed-running")
	stores := s.realStores()
	image := pinnedAlpineForCommitE2E(t)
	realQueue := mustQueueForPool(t, "real")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-main", instant)

	// Orker A dials Temporal through a proxy: cutting it is the host dying.
	proxyA := s.temporalProxy()
	clientA := s.dial("orker-a", proxyA.Addr())
	slowEntered := make(chan struct{})
	var enteredOnce sync.Once
	orkerA, idA := s.startRealOrker("orker-a", "real", realQueue, stores, clientA, func(next orkerCall) orkerCall {
		return func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			enteredOnce.Do(func() { close(slowEntered) })
			return next(ctx, in)
		}
	})
	runID, stream := s.c2Run(t, image, `sleep 20; printf '{"ok":true}' > /orkstack/output/values.json`, realQueue)
	select {
	case <-slowEntered:
	case <-time.After(2 * time.Minute):
		t.Fatal("slow never reached Orker A")
	}
	hold := s.logs.wait(t, ctx, logQuiesce, runID, 2*time.Minute)
	s.transcript.step("hold reached at %v events with slow running on %s", hold.Fields["history_events"], idA)
	// slow's container runs for 20 s; on a loaded host it may still be
	// starting when the hold is logged, so wait for it rather than sample.
	awaitRunningContainersForCommitE2E(t, ctx, idA, 1, 15*time.Second)

	killedAt := time.Now()
	proxyA.Cut()
	orkerA.stop()
	s.transcript.step("Orker A killed: its frontend connection cut and its worker stopped, container running")
	s.startRealOrker("orker-b", "real", realQueue, stores, nil, nil)

	status, err := s.wait(runID, 5*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	chain, histories := s.histories(runID)
	g0 := factsOf(t, histories[0])
	if len(chain) < 2 || g0.continuedAt == 0 {
		t.Fatalf("%d executions: the run never switched", len(chain))
	}
	attempt, heartbeat := retriedAfterHeartbeatTimeout(t, histories[0], "slow")
	if attempt != 2 || !heartbeat {
		t.Fatalf("slow's recorded start is attempt %d (after a heartbeat timeout: %t), want attempt 2 after one", attempt, heartbeat)
	}
	if done := g0.lastCompletion("slow"); done == 0 || done > g0.continuedAt {
		t.Fatalf("the switch (event %d) did not wait for slow's retry to complete (event %d)", g0.continuedAt, done)
	}
	if on := s.orkersOf("slow"); len(on) != 2 || on[0] != "orker-a" || on[1] != "orker-b" {
		t.Fatalf("slow's attempts ran on %v, want orker-a then orker-b", on)
	}
	records, err := stores.attempts.ListAttempts(ctx, runID, "slow", artifacts.NoItem)
	if err != nil {
		t.Fatal(err)
	}
	retryAt := time.Duration(0)
	for _, r := range records {
		s.transcript.step("attempt record %d on %s: %v exit %d adopted %t", r.GetAttempt(), r.GetOrkerId(),
			r.GetOutcome(), r.GetExitCode(), r.GetCommitAdopted())
		if r.GetAttempt() == 2 {
			retryAt = r.GetStartedAt().AsTime().Sub(killedAt)
		}
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.transcript.figure("retry_started_after_kill", retryAt.String())
	s.transcript.figure("generations", len(chain))
	t.Logf("C2: Orker A killed mid-hold; slow retried on B after the heartbeat timeout (%s after the kill); switch at %d "+
		"after slow completed; %d generations, %d events", retryAt, g0.continuedAt, len(chain), len(events))
}

// TestIntegration_ContinuationChaosC2_OrkerKilledAfterCommitIsAdopted: a real
// Orker writes slow's commit.json during a hold and dies before it reports
// the completion. The retry, after the heartbeat timeout, on another Orker
// adopts that commit: the container ran once (the attempt records), and the
// run switches and completes.
func TestIntegration_ContinuationChaosC2_OrkerKilledAfterCommitIsAdopted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C2/commit-adopted")
	stores := s.realStores()
	image := pinnedAlpineForCommitE2E(t)
	realQueue := mustQueueForPool(t, "real")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-main", instant)

	proxyA := s.temporalProxy()
	clientA := s.dial("orker-a", proxyA.Addr())
	g := newGates()
	committed := make(chan *orkstackv1.ExecuteTaskResult, 1)
	dead := make(chan struct{})
	orkerA, idA := s.startRealOrker("orker-a", "real", realQueue, stores, clientA, func(next orkerCall) orkerCall {
		return func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			// Held (heartbeating) until the run holds; then the container
			// runs and commits; then the process dies before it reports.
			if err := g.hold(ctx, "slow"); err != nil {
				return nil, err
			}
			result, err := next(ctx, in)
			if err != nil {
				return nil, err
			}
			committed <- result
			<-dead
			return result, nil
		}
	})
	runID, stream := s.c2Run(t, image, `printf '{"ok":true}' > /orkstack/output/values.json`, realQueue)
	g.waitEntered(t, "slow", 2*time.Minute)
	s.logs.wait(t, ctx, logQuiesce, runID, 2*time.Minute)
	g.open("slow")
	var first *orkstackv1.ExecuteTaskResult
	select {
	case first = <-committed:
	case <-time.After(2 * time.Minute):
		t.Fatal("Orker A never committed slow")
	}
	s.transcript.step("Orker A committed slow (%s) during the hold", first.GetCommit().GetUri())
	proxyA.Cut()
	close(dead)
	orkerA.stop()
	s.transcript.step("Orker A killed after commit.json, before reporting the completion")
	s.startRealOrker("orker-b", "real", realQueue, stores, nil, nil)

	status, err := s.wait(runID, 5*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	chain, histories := s.histories(runID)
	g0 := factsOf(t, histories[0])
	if len(chain) < 2 || g0.continuedAt == 0 {
		t.Fatalf("%d executions: the run never switched", len(chain))
	}
	attempt, heartbeat := retriedAfterHeartbeatTimeout(t, histories[0], "slow")
	if attempt != 2 || !heartbeat {
		t.Fatalf("slow's recorded start is attempt %d (after a heartbeat timeout: %t), want attempt 2 after one", attempt, heartbeat)
	}
	if done := g0.lastCompletion("slow"); done == 0 || done > g0.continuedAt {
		t.Fatalf("the switch (event %d) did not wait for slow (event %d)", g0.continuedAt, done)
	}
	records, err := stores.attempts.ListAttempts(ctx, runID, "slow", artifacts.NoItem)
	if err != nil {
		t.Fatal(err)
	}
	ran, adopted := 0, 0
	for _, r := range records {
		s.transcript.step("attempt record %d on %s: %v exit %d adopted %t", r.GetAttempt(), r.GetOrkerId(),
			r.GetOutcome(), r.GetExitCode(), r.GetCommitAdopted())
		if r.GetCommitAdopted() {
			adopted++
			if r.GetAttempt() != 2 || r.GetOrkerId() == idA {
				t.Fatalf("the adopting record is attempt %d on %s, want attempt 2 on Orker B", r.GetAttempt(), r.GetOrkerId())
			}
		} else {
			ran++
		}
	}
	if ran != 1 || adopted != 1 {
		t.Fatalf("%d records ran the container and %d adopted, want the container run once and adopted once", ran, adopted)
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := taskRunOfRun(run, "slow").GetCommit().GetUri(); got != first.GetCommit().GetUri() {
		t.Fatalf("slow's row names commit %q, Orker A committed %q", got, first.GetCommit().GetUri())
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.transcript.figure("attempt_records", len(records))
	t.Logf("C2 variant: slow committed on A during the hold, A killed before completing; attempt 2 on B adopted the "+
		"commit; the container ran once; switch at %d; %d generations, %d events", g0.continuedAt, len(chain), len(events))
}

// awaitChainDocument is an await opened at the start and needed only by the
// last task, beside a chain whose growth crosses the budget: the run's
// natural idle points come while the await is open.
func awaitChainDocument(name string, chain int, awaitTimeout int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	fmt.Fprintf(&b, "  - id: approve\n    needs: []\n    await: {timeout_seconds: %d}\n", awaitTimeout)
	b.WriteString(guardChain("c", chain, ""))
	fmt.Fprintf(&b, "  - id: merge\n    uses: shell\n    needs: [c%02d, approve]\n    with: {op: exec}\n", chain-1)
	return b.String()
}

// raceTheSwitch starts an await-and-chain run on a replica whose checkpoint
// write blocks at the first switch, waits for the hook's entry, runs act
// (the racing resolution or cancel, each returning once the cluster recorded
// it) and releases the hook within 5 s of its entry (2.11's window), so the
// race is on the switching workflow task itself.
func (s *chaosStack) raceTheSwitch(t *testing.T, name string, act func(runID string)) (string, *streamCollector, time.Duration) {
	t.Helper()
	hook := newBlockingHook(func(kind string, _ int64) bool { return kind == "checkpoint" })
	t.Cleanup(func() { hook.open(nil) })
	s.startReplica("replica-a", replicaOptions{beforeWrite: hook.write})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, awaitChainDocument(name, 12, 3600)), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	entered := hook.waitEntered(t, 2*time.Minute)
	s.transcript.step("the switch's checkpoint write entered its hook")
	act(runID)
	hook.open(nil)
	window := time.Since(entered)
	s.transcript.step("hook released %s after its entry", window)
	if window >= 5*time.Second {
		t.Fatalf("the hook was released %s after its entry, outside the 5 s window", window)
	}
	return runID, stream, window
}

// requireUnhandledCommand fails unless h recorded the race: a workflow task
// that failed UNHANDLED_COMMAND. It returns that event's id.
func (s *chaosStack) requireUnhandledCommand(t *testing.T, h *historypb.History) int64 {
	t.Helper()
	failures := factsOf(t, h).unhandledCommandFailures()
	if len(failures) == 0 {
		t.Fatal("the race did not happen: no workflow task failed UNHANDLED_COMMAND, so the racing event was seen " +
			"before the switching task completed (a valid outcome, but not the race under test)")
	}
	e := failures[0]
	s.transcript.step("UNHANDLED_COMMAND: event %d %s cause %s (%s)", e.GetEventId(), e.GetEventType(),
		e.GetWorkflowTaskFailedEventAttributes().GetCause(), e.GetWorkflowTaskFailedEventAttributes().GetFailure().GetMessage())
	s.transcript.figure("unhandled_command_event", map[string]any{"event_id": e.GetEventId(),
		"cause":   e.GetWorkflowTaskFailedEventAttributes().GetCause().String(),
		"message": e.GetWorkflowTaskFailedEventAttributes().GetFailure().GetMessage()})
	return e.GetEventId()
}

// TestIntegration_ContinuationChaosC3_ResolutionRacingTheSwitch: a
// resolution recorded while the switching workflow task runs makes the
// server reject the continue-as-new (UNHANDLED_COMMAND); the retried task
// applies the resolution, exactly once, and nothing is ignored.
func TestIntegration_ContinuationChaosC3_ResolutionRacingTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C3")
	runID, stream, _ := s.raceTheSwitch(t, "c3-race", func(runID string) {
		if err := s.starter.SignalAwait(ctx, runID, completion("approve", `{"approved":true}`, "chaos")); err != nil {
			t.Fatalf("resolve approve: %v", err)
		}
		s.transcript.step("approve's resolution recorded while the switching task was open")
	})
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	chain, histories := s.histories(runID)
	failedAt := s.requireUnhandledCommand(t, histories[0])
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	applied, ignored := 0, 0
	for _, e := range events {
		if e.GetTaskId() == "approve" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED {
			applied++
		}
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && strings.HasPrefix(e.GetMessage(), "resolution ignored") {
			ignored++
		}
	}
	if applied != 1 || ignored != 0 {
		t.Fatalf("approve succeeded %d times and %d resolutions were ignored, want exactly one applied and none ignored", applied, ignored)
	}
	signals := 0
	for _, h := range histories {
		signals += len(factsOf(t, h).signaledAt)
	}
	if signals != 1 {
		t.Fatalf("%d resolution signals recorded across the chain, want the one sent", signals)
	}
	s.transcript.figure("generations", len(chain))
	t.Logf("C3: UNHANDLED_COMMAND at event %d of generation 0; approve applied once, none ignored; %d generations, %d events",
		failedAt, len(chain), len(events))
}

// TestIntegration_ContinuationChaosC4_CancelRacingTheSwitch: an operator
// cancel recorded while the switching workflow task runs makes the server
// reject the continue-as-new; the retried task sees the cancel and the run
// ends RUN_CANCELED in that generation, with no successor.
func TestIntegration_ContinuationChaosC4_CancelRacingTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C4/racing")
	runID, stream, _ := s.raceTheSwitch(t, "c4-race", func(runID string) {
		if err := s.starter.CancelRun(ctx, runID, ""); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		s.transcript.step("the operator's cancel recorded while the switching task was open")
	})
	status, _ := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the run closed %v, want Canceled", status)
	}
	chain, histories := s.histories(runID)
	if len(chain) != 1 || factsOf(t, histories[0]).continuedAt != 0 {
		t.Fatalf("%d executions: the cancel did not end the run in the generation it raced", len(chain))
	}
	failedAt := s.requireUnhandledCommand(t, histories[0])
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_CANCELED, "RUN_CANCELED")
	if last := events[len(events)-1]; last.GetTaskId() != "" {
		t.Fatalf("the run ended %v owned by task %q (%q), want the run's own cancel", last.GetType(), last.GetTaskId(), last.GetMessage())
	}
	t.Logf("C4: UNHANDLED_COMMAND at event %d; RUN_CANCELED in generation 0, no successor; %d events", failedAt, len(events))
}

// TestIntegration_ContinuationChaosC4_CancelAfterTheSwitch: a cancel after
// the switch ends the run RUN_CANCELED in the generation it reached.
func TestIntegration_ContinuationChaosC4_CancelAfterTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C4/after")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, awaitChainDocument("c4-after", 12, 3600)), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	s.waitChain(runID, 1, 2*time.Minute)
	// The await keeps the run alive in its new generation; cancel it there,
	// once that generation has restored its state.
	s.logs.wait(t, ctx, logRestored, runID, time.Minute)
	if err := s.starter.CancelRun(ctx, runID, ""); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	status, _ := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the run closed %v, want Canceled", status)
	}
	chain, histories := s.histories(runID)
	last := factsOf(t, histories[len(histories)-1])
	if len(chain) < 2 || last.cancelAskedAt == 0 || last.closed != enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED {
		t.Fatalf("%d executions; the last recorded the cancel at %d and closed %v", len(chain), last.cancelAskedAt, last.closed)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_CANCELED, "RUN_CANCELED")
	t.Logf("C4 variant: cancel after the switch ends the run RUN_CANCELED in generation %d; %d events", len(chain)-1, len(events))
}

// TestIntegration_ContinuationChaosC5_AwaitDeadlineCrossesTheSwitch: an
// await opened in generation 0 whose deadline falls after the switch fires
// in the later generation, at the instant generation 0 recorded (±1 s).
func TestIntegration_ContinuationChaosC5_AwaitDeadlineCrossesTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C5/deadline")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, awaitChainDocument("c5-deadline", 12, 40)), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	s.waitChain(runID, 1, 2*time.Minute)
	switched := time.Now()
	status, _ := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed by the await's deadline", status)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, "")
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	approve := taskRunOfRun(run, "approve")
	deadline := approve.GetAwait().GetDeadline().AsTime()
	if approve.GetStatus() != orkstackv1.Status_STATUS_FAILED || deadline.IsZero() || !deadline.After(switched) {
		t.Fatalf("approve %v, deadline %v (the run switched at %v): want a deadline after the switch that expired",
			approve.GetStatus(), deadline, switched)
	}
	var expired *orkstackv1.Event
	for _, e := range events {
		if e.GetTaskId() == "approve" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED {
			expired = e
		}
	}
	if expired == nil {
		t.Fatal("no TASK_FAILED for approve")
	}
	skew := expired.GetTimestamp().AsTime().Sub(deadline)
	if skew < -time.Second || skew > time.Second {
		t.Fatalf("the deadline fired at %v, %s from the recorded instant %v", expired.GetTimestamp().AsTime(), skew, deadline)
	}
	chain, histories := s.histories(runID)
	fired := -1
	for i, h := range histories {
		for _, event := range h.GetEvents() {
			if event.GetEventType() == enums.EVENT_TYPE_TIMER_FIRED {
				fired = i
			}
		}
	}
	if fired < 1 {
		t.Fatalf("the deadline timer fired in generation %d, want a carried one", fired)
	}
	s.transcript.figure("deadline", deadline.Format(time.RFC3339Nano))
	s.transcript.figure("fired_at", expired.GetTimestamp().AsTime().Format(time.RFC3339Nano))
	s.transcript.figure("skew", skew.String())
	t.Logf("C5 deadline: recorded %v, fired in generation %d of %d at %v (skew %s)", deadline, fired, len(chain),
		expired.GetTimestamp().AsTime(), skew)
}

// c5DoorsYAML is two awaits, one resolved through the API and one through
// its callback, open from the start beside a chain that crosses the budget.
func c5DoorsYAML() string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: c5-doors\nversion: \"1.0.0\"\ntasks:\n")
	b.WriteString("  - id: approve-api\n    needs: []\n    await: {timeout_seconds: 3600}\n")
	b.WriteString("  - id: approve-callback\n    needs: []\n    await: {timeout_seconds: 3600, callback: true}\n")
	b.WriteString(guardChain("c", 12, ""))
	b.WriteString("  - id: merge\n    uses: shell\n    needs: [c11, approve-api, approve-callback]\n    with: {op: exec}\n")
	return b.String()
}

// TestIntegration_ContinuationChaosC5_ResolutionsAfterTheSwitch: a
// resolution through the API door and one through the callback door, both
// sent after the run switched, are accepted and applied by the generation
// the run reached.
func TestIntegration_ContinuationChaosC5_ResolutionsAfterTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C5/resolution")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	a := s.serveAPI(tcAPIConfig())
	a.apply(t, ctx, c5DoorsYAML())
	submitted := a.submit(t, ctx, "c5-doors")
	runID := submitted.GetRun().GetId()
	s.transcript.run(runID)
	if len(submitted.GetAwaitCallbacks()) != 1 {
		t.Fatalf("callbacks %v, want approve-callback's", submitted.GetAwaitCallbacks())
	}
	stream := s.openStream(s.reader, runID, 0)
	s.waitChain(runID, 1, 2*time.Minute)
	s.logs.wait(t, ctx, logRestored, runID, time.Minute)
	s.transcript.step("the run switched; resolving both awaits in its carried generation")
	if _, err := a.runs.ResolveAwait(ctx, connect.NewRequest(&orkstackv1.ResolveAwaitRequest{
		RunId: runID, TaskId: "approve-api", ValuesJson: `{"approved":true}`,
	})); err != nil {
		t.Fatalf("the API door refused a resolution after the switch: %v", err)
	}
	callback := submitted.GetAwaitCallbacks()[0]
	if code, body := a.post(t, ctx, callback.GetPath(), callback.GetToken(), `{"approved":true}`); code != http.StatusAccepted {
		t.Fatalf("the callback door answered %d after the switch: %s", code, body)
	}
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	for id, via := range map[string]orkstackv1.AwaitResolvedVia{
		"approve-api":      orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API,
		"approve-callback": orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK,
	} {
		row := taskRunOfRun(run, id)
		if row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || row.GetAwait().GetResolvedVia() != via {
			t.Fatalf("%s: %v via %v, want SUCCEEDED via %v", id, row.GetStatus(), row.GetAwait().GetResolvedVia(), via)
		}
	}
	chain, histories := s.histories(runID)
	if signals := len(factsOf(t, histories[0]).signaledAt); signals != 0 {
		t.Fatalf("generation 0 recorded %d resolutions, want both after the switch", signals)
	}
	later := 0
	for _, h := range histories[1:] {
		later += len(factsOf(t, h).signaledAt)
	}
	if later != 2 {
		t.Fatalf("%d resolutions recorded after the switch, want 2", later)
	}
	t.Logf("C5 resolution: both doors accepted after the switch and applied in a carried generation (%d generations)", len(chain))
}

// c6PinDocument is a pin whose second member needs an unpinned task that
// needs the first (p1 → u → p2), beside a 40-task chain that grows the run
// to its hold line while the pin is open.
func c6PinDocument() string {
	return "apiVersion: orkstack/v1\nkind: Orkflow\nname: c6-pin\nversion: \"1.0.0\"\npins:\n  build: {}\ntasks:\n" +
		"  - id: p1\n    uses: shell\n    pin: build\n    needs: []\n    with: {op: exec}\n" +
		"  - id: u\n    uses: shell\n    needs: [p1]\n    with: {op: exec}\n" +
		"  - id: p2\n    uses: shell\n    pin: build\n    needs: [u]\n    with: {op: exec}\n" +
		guardChain("c", 40, "")
}

// c6AwaitPinDocument is a pin whose second member waits on an await after
// its first, beside a 20-task chain: an open pin that cannot reach idle
// until a person resolves the await. The pin keeps the run in one history
// from its first task, so the chain is short enough to finish in it (C13 is
// the run whose other work never does).
func c6AwaitPinDocument() string {
	return "apiVersion: orkstack/v1\nkind: Orkflow\nname: c6-await-pin\nversion: \"1.0.0\"\npins:\n  deploy: {}\ntasks:\n" +
		"  - id: q1\n    uses: shell\n    pin: deploy\n    needs: []\n    with: {op: exec}\n" +
		"  - id: approve\n    needs: [q1]\n    await: {timeout_seconds: 3600}\n" +
		"  - id: q2\n    uses: shell\n    pin: deploy\n    needs: [approve]\n    with: {op: exec}\n" +
		guardChain("c", 20, "")
}

// pinHost is one in-process pin host whose process can be killed: it dials
// Temporal through a proxy of its own, so cutting it is the host going
// silent — no heartbeat, no late report — as a dead host does.
type pinHost struct {
	orker *chaosOrker
	proxy *faultproxy.Proxy
}

// kill makes the host unreachable and stops its worker.
func (h *pinHost) kill() {
	h.proxy.Cut()
	h.orker.stop()
}

// startPinHosts starts three pin hosts on the stack's pool.
func (s *chaosStack) startPinHosts(behavior chaosBehavior) []*pinHost {
	var hosts []*pinHost
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("pin-host-%d", i)
		proxy := s.temporalProxy()
		hosts = append(hosts, &pinHost{proxy: proxy,
			orker: s.startOrkerWith(name, s.orkerQueue, s.dial(name, proxy.Addr()), behavior)})
	}
	return hosts
}

// requireOneHost fails unless every member ran on exactly one Orker, and
// the run's rows name it; it returns that Orker.
func (s *chaosStack) requireOneHost(t *testing.T, run *orkstackv1.Run, members ...string) string {
	t.Helper()
	host := ""
	for _, m := range members {
		on := s.orkersOf(m)
		row := taskRunOfRun(run, m).GetOrkerId()
		if len(on) != 1 || row != on[0] || (host != "" && on[0] != host) {
			t.Fatalf("pin member %s ran on %v (row says %q), want one host shared by %v", m, on, row, members)
		}
		host = on[0]
	}
	s.transcript.step("every member of %v ran on %s", members, host)
	return host
}

// TestIntegration_ContinuationChaosC6_OpenPinHoldsTheSwitch: with three pin
// hosts polling, a run reaches its hold line while a pin is open. The hold
// still lets the pin's needs-closure run — an unpinned task one member needs
// completes — the run never switches until the pin has closed, and every
// member runs on one Orker.
func TestIntegration_ContinuationChaosC6_OpenPinHoldsTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C6/open-pin")
	g := newGates()
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startPinHosts(gated(g, "p1"))
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, c6PinDocument()), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	g.waitEntered(t, "p1", time.Minute)
	hold := s.logs.wait(t, ctx, logQuiesce, runID, 2*time.Minute)
	s.transcript.step("hold reached at %v events with pin build open", hold.Fields["history_events"])
	if a, _ := s.attemptsOf("u"); a != 0 {
		t.Fatal("u ran before p1 finished")
	}
	g.open("p1")
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	chain, histories := s.histories(runID)
	g0 := factsOf(t, histories[0])
	if g0.continuedAt == 0 {
		t.Fatal("generation 0 never switched")
	}
	for _, m := range []string{"p1", "u", "p2"} {
		if done := g0.lastCompletion(m); done == 0 || done > g0.continuedAt {
			t.Fatalf("%s completed at event %d, not before the switch at %d: the switch did not wait for the pin", m, done, g0.continuedAt)
		}
	}
	// The pin closed before the switch: its session's release happened in
	// generation 0, and nothing of the pin ran in a later generation.
	for i := 1; i < len(histories); i++ {
		later := factsOf(t, histories[i])
		for _, m := range []string{"p1", "u", "p2"} {
			if len(later.scheduledAt[m]) != 0 {
				t.Fatalf("%s was dispatched in generation %d", m, i)
			}
		}
	}
	// u dispatched while the run held: the chain's next task was held back
	// at that moment, and u was not.
	holdAt := hold.At
	uDispatched := s.firstEventAt(t, runID, "u", orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	if uDispatched.Before(holdAt) {
		t.Fatalf("u was dispatched at %v, before the hold at %v: the scenario did not exercise the pin exception", uDispatched, holdAt)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	run, _ := s.reader.GetRun(ctx, runID)
	host := s.requireOneHost(t, run, "p1", "p2")
	s.transcript.figure("pin_host", host)
	s.transcript.figure("switch_event", g0.continuedAt)
	t.Logf("C6: hold with pin open; u ran during the hold; pin closed on %s before the switch at %d; %d generations, %d events",
		host, g0.continuedAt, len(chain), len(events))
}

// firstEventAt is when the stream first recorded typ for taskID.
func (s *chaosStack) firstEventAt(t *testing.T, runID, taskID string, typ orkstackv1.EventType) time.Time {
	t.Helper()
	events, err := s.reader.Events(s.ctx, runID, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if e.GetTaskId() == taskID && e.GetType() == typ {
			return e.GetTimestamp().AsTime()
		}
	}
	t.Fatalf("no %v for %s", typ, taskID)
	return time.Time{}
}

// TestIntegration_ContinuationChaosC6_PinBehindAnAwaitGivesTheHoldUp: an
// open pin whose remaining member waits on an await cannot reach idle until
// a person resolves it, so the hold is given up and held work goes on; the
// await resolved later, the pin closes on its one host and the run
// completes.
func TestIntegration_ContinuationChaosC6_PinBehindAnAwaitGivesTheHoldUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C6/await-pin")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startPinHosts(instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, c6AwaitPinDocument()), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	gaveUp := s.logs.wait(t, ctx, logGiveUp, runID, 2*time.Minute)
	s.transcript.step("hold given up: %v", gaveUp.Fields["reason"])
	reason := fmt.Sprint(gaveUp.Fields["reason"])
	if !strings.Contains(reason, "deploy") || !strings.Contains(reason, "approve") {
		t.Fatalf("the give-up's reason %q does not name the pin and the await", reason)
	}
	// Held work goes on after the give-up: the chain's tasks complete
	// while the await is still open.
	deadline := time.Now().Add(time.Minute)
	for {
		if a, _ := s.attemptsOf("c19"); a > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the chain did not finish after the hold was given up")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := s.starter.SignalAwait(ctx, runID, completion("approve", `{"approved":true}`, "chaos")); err != nil {
		t.Fatalf("resolve approve: %v", err)
	}
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	run, _ := s.reader.GetRun(ctx, runID)
	host := s.requireOneHost(t, run, "q1", "q2")
	chain := s.chain(runID)
	s.transcript.figure("give_up_reason", reason)
	t.Logf("C6 await: hold given up (%s); pin closed on %s after the resolution; %d generations, %d events",
		reason, host, len(chain), len(events))
}

const c7ChildDocument = "apiVersion: orkstack/v1\nkind: Orkflow\nname: release-block\nversion: \"2.1.0\"\ninputs:\n  image: {type: string, required: true}\ntasks:\n"

// TestIntegration_ContinuationChaosC7_ComposedChildHoldsTheSwitch: a run
// fired by a schedule reaches its hold line while a composed child is in
// flight, and does not switch until the child has ended; the child itself
// continues as new, and is still one execution to its parent and the fire.
func TestIntegration_ContinuationChaosC7_ComposedChildHoldsTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C7")
	g := newGates()
	var (
		mu     sync.Mutex
		runIDs []string
	)
	parentDoc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: c7-parent\nversion: \"1.0.0\"\ntasks:\n" +
		"  - id: release\n    uses: orkflow:release-block@2.1.0\n    needs: []\n    with: {image: fixed}\n" +
		guardChain("c", 30, "") +
		"  - id: post\n    uses: shell\n    needs: [release, c29]\n    with: {op: exec}\n"
	prepare := func(_ context.Context, rec *orkstackv1.ScheduleRecord, at time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		parent, err := dsl.Compile([]byte(parentDoc))
		if err != nil {
			return nil, err
		}
		parent.Namespace = "default"
		child, err := dsl.Compile([]byte(c7ChildDocument + guardChain("k", 12, "")))
		if err != nil {
			return nil, err
		}
		child.Namespace = "default"
		child.DefinitionDigest = childDigest
		for _, tk := range parent.GetTasks() {
			if tk.GetOrkflow() != nil {
				tk.Orkflow.DefinitionDigest = childDigest
			}
		}
		runID := fmt.Sprintf("ork-chaos-c7-%d", time.Now().UnixNano())
		mu.Lock()
		runIDs = append(runIDs, runID)
		mu.Unlock()
		plan, _ := s.tcPlan()
		childPlan := rawPlan(s.orkerQueue)
		childPlan.CommitRoot = "mem://commits"
		cp, err := engine.NewChildRunPlan(child, childPlan, false, false)
		if err != nil {
			return nil, err
		}
		cp.RunID = runID + "-child"
		plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
		plan.Origin = &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SCHEDULE", ID: rec.GetScheduleId(),
			Reference: at.UTC().Format(time.RFC3339), Description: rec.GetName()}
		encoded, err := json.Marshal(plan)
		if err != nil {
			return nil, err
		}
		return &orkstackv1.ScheduledRunPreparation{
			Run: &orkstackv1.Run{Id: runID, Namespace: "default", OrkflowName: parent.GetName(),
				OrkflowVersion: parent.GetVersion(), Status: orkstackv1.Status_STATUS_PENDING, CreatedAt: timestamppb.Now()},
			Orkflow: parent, Plan: encoded,
		}, nil
	}
	s.startReplica("replica-a", replicaOptions{extra: []func(worker.Worker){func(w worker.Worker) {
		w.RegisterActivityWithOptions(prepare, activity.RegisterOptions{Name: engine.ActivityPrepareScheduledRun})
	}}})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, "k00"))
	scheduler := engine.NewTemporalScheduler(s.control, s.runQueue)
	rec := &orkstackv1.ScheduleRecord{
		ScheduleId: "sch-00000000" + fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff),
		Name:       fmt.Sprintf("c7-%d", time.Now().UnixNano()), Namespace: "default", OrkflowName: "c7-parent",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL},
	}
	if _, err := scheduler.Create(ctx, rec, true); err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	defer func() { _ = scheduler.Delete(context.Background(), "default", rec.GetName()) }()
	if err := scheduler.Fire(ctx, "default", rec.GetName(), orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED); err != nil {
		t.Fatalf("fire: %v", err)
	}
	g.waitEntered(t, "k00", 2*time.Minute)
	mu.Lock()
	runID := runIDs[0]
	mu.Unlock()
	s.transcript.run(runID)
	s.transcript.run(runID + "-child")
	hold := s.logs.wait(t, ctx, logQuiesce, runID, 2*time.Minute)
	s.transcript.step("parent held at %v events with its composed child in flight", hold.Fields["history_events"])
	if chain := s.chain(runID); len(chain) != 1 {
		t.Fatalf("the parent switched with its child in flight (%d executions)", len(chain))
	}
	g.open("k00")
	status, err := s.wait(runID, 4*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the parent closed %v: %v", status, err)
	}
	chain, histories := s.histories(runID)
	g0 := histories[0]
	childDone, switched := int64(0), int64(0)
	for _, event := range g0.GetEvents() {
		if event.GetChildWorkflowExecutionCompletedEventAttributes() != nil {
			childDone = event.GetEventId()
		}
		if event.GetWorkflowExecutionContinuedAsNewEventAttributes() != nil {
			switched = event.GetEventId()
		}
	}
	if childDone == 0 || switched == 0 || switched < childDone {
		t.Fatalf("parent generation 0: child completed at %d, switch at %d: want the switch after the child ended", childDone, switched)
	}
	childChain := s.chain(runID + "-child")
	if len(childChain) < 2 {
		t.Fatalf("the child ran %d generations, want it to continue as new itself", len(childChain))
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if taskRunOfRun(run, "release").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("release %v", taskRunOfRun(run, "release").GetStatus())
	}
	fires := scheduleFireIDs(t, ctx, s.control, rec.GetScheduleId(), 1)
	if err := s.control.GetWorkflow(ctx, fires[0], "").Get(ctx, nil); err != nil {
		t.Fatalf("the fire did not complete with its run: %v", err)
	}
	s.requireOneRunRead(runID, nil, orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.transcript.figure("parent_generations", len(chain))
	s.transcript.figure("child_generations", len(childChain))
	t.Logf("C7: parent held with its child in flight; child (%d generations) completed at event %d, parent switched at %d; "+
		"parent %d generations; fire %s completed", len(childChain), childDone, switched, len(chain), fires[0])
}

// identitiesAfter is every worker identity that started a workflow task of
// h after event id from.
func identitiesAfter(h *historypb.History, from int64) map[string]int {
	out := map[string]int{}
	for _, event := range h.GetEvents() {
		if attrs := event.GetWorkflowTaskStartedEventAttributes(); attrs != nil && event.GetEventId() > from {
			out[attrs.GetIdentity()]++
		}
	}
	return out
}

// requireStreamFromBoundary opens a stream on reader from a sequence before
// the run's first switch and checks it resumes with no gap: exactly the
// events Events(0) holds after that sequence.
func (s *chaosStack) requireStreamFromBoundary(t *testing.T, reader *engine.TemporalRunReader, runID string) int64 {
	t.Helper()
	_, histories := s.histories(runID)
	boundary := carryOf(t, histories[1]).GetEventSequence()
	from := max(1, boundary-3)
	stream := s.openStream(reader, runID, from)
	got := stream.result(t, time.Minute)
	all, err := s.reader.Events(s.ctx, runID, 0)
	if err != nil {
		t.Fatal(err)
	}
	var want []*orkstackv1.Event
	for _, e := range all {
		if e.GetSequence() > from { // a stream delivers what comes after its sequence
			want = append(want, e)
		}
	}
	if len(got) == 0 || got[0].GetSequence() != from+1 ||
		strings.Join(eventKeys(got), "\n") != strings.Join(eventKeys(want), "\n") {
		t.Fatalf("a stream from %d (boundary %d) read\n%s\nwant\n%s", from, boundary,
			strings.Join(eventKeys(got), "\n"), strings.Join(eventKeys(want), "\n"))
	}
	s.transcript.step("a stream opened from sequence %d (the first switch carried %d) read %d events with no gap",
		from, boundary, len(got))
	return boundary
}

// TestIntegration_ContinuationChaosC8_ReplicaStoppedMidHold: engine replica
// A's run worker is stopped while the run holds; replica B finishes the
// hold, switches and completes the run, and a stream opened on B from before
// the switch resumes with no gap.
func TestIntegration_ContinuationChaosC8_ReplicaStoppedMidHold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C8/mid-hold")
	g := newGates()
	running := taskKeys("w", 12)
	a := s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, running...))
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, flatAndChain("c8-hold", 12, 12)), nil, plan)
	hold := s.logs.wait(t, ctx, logQuiesce, runID, 2*time.Minute)
	if hold.Source != "replica-a" {
		t.Fatalf("the hold was decided by %s, want replica-a", hold.Source)
	}
	b := s.startReplica("replica-b", replicaOptions{})
	a.stop()
	stoppedAt, _ := s.current(runID)
	s.transcript.step("replica-a stopped mid-hold (execution %s)", stoppedAt)
	lastBefore := int64(len(s.history(runID, stoppedAt).GetEvents()))
	for _, key := range running {
		g.open(key)
	}
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	chain, histories := s.histories(runID)
	if len(chain) < 2 {
		t.Fatalf("%d executions: the run never switched", len(chain))
	}
	after := identitiesAfter(histories[0], lastBefore)
	if after["replica-b"] == 0 || after["replica-a"] != 0 {
		t.Fatalf("generation 0's workflow tasks after the stop were started by %v, want replica-b only", after)
	}
	s.requireOneRunRead(runID, nil, orkstackv1.Status_STATUS_SUCCEEDED, "")
	boundary := s.requireStreamFromBoundary(t, b.reader, runID)
	s.transcript.figure("tasks_after_stop", after)
	t.Logf("C8 mid-hold: replica-b finished the hold and the run (%d generations); stream from before %d had no gap", len(chain), boundary)
}

// TestIntegration_ContinuationChaosC8_ReplicaStoppedRightAfterASwitch:
// replica A's run worker is stopped as soon as the run's next generation
// exists; replica B starts that generation (or finishes its start) and
// completes the run, and a stream opened on B from before the switch
// resumes with no gap.
func TestIntegration_ContinuationChaosC8_ReplicaStoppedRightAfterASwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C8/after-switch")
	a := s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c8-after\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, "")), nil, plan)
	chain := s.waitChain(runID, 1, 2*time.Minute)
	b := s.startReplica("replica-b", replicaOptions{})
	a.stop()
	s.transcript.step("replica-a stopped right after the switch into %s", chain[len(chain)-1])
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	all, histories := s.histories(runID)
	served := map[string]int{}
	for _, h := range histories[1:] {
		for who, n := range identitiesAfter(h, 0) {
			served[who] += n
		}
	}
	if served["replica-b"] == 0 {
		t.Fatalf("no carried generation was served by replica-b: %v", served)
	}
	s.requireOneRunRead(runID, nil, orkstackv1.Status_STATUS_SUCCEEDED, "")
	boundary := s.requireStreamFromBoundary(t, b.reader, runID)
	s.transcript.figure("carried_tasks_by_replica", served)
	t.Logf("C8 after a switch: replica-b served %v of the carried generations' tasks (%d generations); stream from before %d had no gap",
		served, len(all), boundary)
}

// countingClient counts the describes a reader issues, so a poll can quote
// how many executions a liveness question had to ask about one by one.
type countingClient struct {
	client.Client
	describes atomic.Int64
}

func (c *countingClient) DescribeWorkflowExecution(ctx context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.describes.Add(1)
	return c.Client.DescribeWorkflowExecution(ctx, id, run)
}

// livenessPoll is one 100 ms reading of the liveness questions a switching
// chain must never be lost from.
type livenessPoll struct {
	Start, End        time.Time
	ReleaseRuns       int64  `json:"release_runs"`
	ReleaseErr        string `json:"release_err,omitempty"`
	ReleaseDescribes  int64  `json:"release_describes"`
	Running           int64  `json:"running"`
	RunningErr        string `json:"running_err,omitempty"`
	RunningDescribes  int64  `json:"running_describes"`
	Drained           bool   `json:"drained"`
	DrainState        string `json:"drain_state"`
	ReleasesErr       string `json:"releases_err,omitempty"`
	OnlyContinued     bool   `json:"only_continued_as_new"`
	VisibilityRecords int    `json:"visibility_records"`
}

// onlyContinuedAsNew reports whether the cluster's index shows runID only
// as a closed ContinuedAsNew record — a chain caught between its two records.
func (s *chaosStack) onlyContinuedAsNew(runID string) (bool, int) {
	resp, err := s.control.ListWorkflow(s.ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("WorkflowId = %q", runID),
	})
	if err != nil || len(resp.GetExecutions()) == 0 {
		return false, 0
	}
	var latest *workflowpb.WorkflowExecutionInfo
	for _, info := range resp.GetExecutions() {
		if latest == nil || info.GetStartTime().AsTime().After(latest.GetStartTime().AsTime()) {
			latest = info
		}
	}
	return latest.GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, len(resp.GetExecutions())
}

// TestIntegration_ContinuationChaosC9_RolloutWhileAChainSwitches: build v2
// becomes current while a run pinned to v1 is about to switch. Every
// generation's workflow tasks stay on v1, and from before the first switch
// until the chain ends, polled every 100 ms, ReleaseRuns(v1) and RunningRuns
// never read the run as gone and v1 never reports Drained; it does once the
// chain has ended.
func TestIntegration_ContinuationChaosC9_RolloutWhileAChainSwitches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C9")
	g := newGates()
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	v1 := task.Deployment{Name: "ork-chaos-c9-" + suffix, BuildID: "v1"}
	v2 := task.Deployment{Name: v1.Name, BuildID: "v2"}
	s.startReplica("replica-v1", replicaOptions{deployment: v1})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-v1", s.orkerQueue, s.control, v1, gated(g, "gate"))
	setCurrentVersion(t, ctx, s.control, v1, s.runQueue, s.orkerQueue)
	s.transcript.step("v1 up and current")

	counting := &countingClient{Client: s.control}
	reader := engine.NewTemporalRunReader(counting).WithSnapshotStore(s.store)
	releases, err := engine.NewTemporalReleaseCluster(counting, reader, s.namespace, v1.Name, s.runQueue, s.orkerQueue)
	if err != nil {
		t.Fatal(err)
	}
	svc := api.NewService(api.Config{Reader: reader, Starter: s.starter, Releases: releases, Fleet: s.starter,
		Orkflows: engine.NewMemoryOrkflowStore()})

	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c9-rollout\nversion: \"1.0.0\"\ntasks:\n"+
		"  - id: gate\n    uses: shell\n    needs: []\n    with: {op: exec}\n"+guardChain("c", 30, "gate")), nil, plan)
	g.waitEntered(t, "gate", 2*time.Minute)

	var (
		pollMu sync.Mutex
		polls  []livenessPoll
	)
	stopPolling := make(chan struct{})
	pollingDone := make(chan struct{})
	go func() {
		defer close(pollingDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			p := livenessPoll{Start: time.Now()}
			before := counting.describes.Load()
			n, _, err := reader.ReleaseRuns(ctx, v1.Name, v1.BuildID, 0)
			p.ReleaseRuns, p.ReleaseDescribes = n, counting.describes.Load()-before
			if err != nil {
				p.ReleaseErr = err.Error()
			}
			before = counting.describes.Load()
			n, err = reader.RunningRuns(ctx)
			p.Running, p.RunningDescribes = n, counting.describes.Load()-before
			if err != nil {
				p.RunningErr = err.Error()
			}
			listed, err := svc.ListReleases(ctx, connect.NewRequest(&orkstackv1.ListReleasesRequest{}))
			if err != nil {
				p.ReleasesErr = err.Error()
			} else {
				for _, release := range listed.Msg.GetReleases() {
					if release.GetBuildId() == v1.BuildID {
						p.Drained, p.DrainState = release.GetDrained(), release.GetDrainState().String()
					}
				}
			}
			p.OnlyContinued, p.VisibilityRecords = s.onlyContinuedAsNew(runID)
			p.End = time.Now()
			pollMu.Lock()
			polls = append(polls, p)
			pollMu.Unlock()
			select {
			case <-stopPolling:
				return
			case <-ticker.C:
			}
		}
	}()

	s.startReplica("replica-v2", replicaOptions{deployment: v2})
	s.startOrkerVersioned("orker-v2", s.orkerQueue, s.control, v2, instant)
	setCurrentVersion(t, ctx, s.control, v2, s.runQueue, s.orkerQueue)
	s.transcript.step("v2 up and current while the v1 run waits at its gate, before its first switch")
	g.open("gate")
	status, err := s.wait(runID, 4*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	chain, histories := s.histories(runID)
	described, err := s.control.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	ended := described.GetWorkflowExecutionInfo().GetCloseTime().AsTime()
	s.transcript.step("the chain ended at %s after %d generations; polling on until v1 reports Drained", ended.Format(time.RFC3339Nano), len(chain))

	// v1 reports Drained once the chain has ended — the cluster's drainage
	// check runs on its own schedule.
	drainedAt := time.Time{}
	deadline := time.Now().Add(7 * time.Minute)
	for drainedAt.IsZero() && time.Now().Before(deadline) {
		pollMu.Lock()
		for _, p := range polls {
			if p.Drained && drainedAt.IsZero() {
				drainedAt = p.End
			}
		}
		pollMu.Unlock()
		time.Sleep(time.Second)
	}
	close(stopPolling)
	<-pollingDone
	if drainedAt.IsZero() {
		t.Fatal("v1 never reported Drained after its last chain ended")
	}

	if len(chain) < 2 {
		t.Fatalf("%d executions: the run never switched", len(chain))
	}
	for i, h := range histories {
		for _, event := range h.GetEvents() {
			attrs := event.GetWorkflowTaskCompletedEventAttributes()
			if attrs == nil {
				continue
			}
			if build := attrs.GetDeploymentVersion().GetBuildId(); build != v1.BuildID {
				t.Fatalf("generation %d's workflow task %d completed on build %q, want v1", i, event.GetEventId(), build)
			}
		}
	}

	pollMu.Lock()
	defer pollMu.Unlock()
	minRelease, minRunning := int64(-1), int64(-1)
	onlyContinued, open, maxDescribes := 0, 0, int64(0)
	for _, p := range polls {
		if !p.End.Before(ended) {
			continue // answered after the chain ended
		}
		open++
		if p.ReleaseErr != "" || p.RunningErr != "" || p.ReleasesErr != "" {
			t.Fatalf("a poll at %s failed: %q %q %q", p.Start.Format(time.RFC3339Nano), p.ReleaseErr, p.RunningErr, p.ReleasesErr)
		}
		if p.ReleaseRuns == 0 || p.Running == 0 || p.Drained {
			t.Fatalf("a poll ending at %s, with the chain still open, read ReleaseRuns(v1)=%d RunningRuns=%d Drained=%t",
				p.End.Format(time.RFC3339Nano), p.ReleaseRuns, p.Running, p.Drained)
		}
		if minRelease < 0 || p.ReleaseRuns < minRelease {
			minRelease = p.ReleaseRuns
		}
		if minRunning < 0 || p.Running < minRunning {
			minRunning = p.Running
		}
		if p.OnlyContinued {
			onlyContinued++
		}
		maxDescribes = max(maxDescribes, p.ReleaseDescribes, p.RunningDescribes)
	}
	if open == 0 {
		t.Fatal("no poll answered while the chain was open")
	}
	if maxDescribes > 1 {
		t.Fatalf("a liveness question described %d executions while one chain was live", maxDescribes)
	}
	proof.WriteJSON(t, s.transcript.group, "polls.json", polls)
	s.transcript.figure("polls_while_open", open)
	s.transcript.figure("min_release_runs_v1", minRelease)
	s.transcript.figure("min_running_runs", minRunning)
	s.transcript.figure("polls_seeing_only_continued_as_new", onlyContinued)
	s.transcript.figure("max_describes_per_question", maxDescribes)
	s.transcript.figure("drained_after_end", drainedAt.Sub(ended).String())
	t.Logf("C9: %d generations all on v1; %d polls while open: min ReleaseRuns(v1) %d, min RunningRuns %d, %d saw only "+
		"ContinuedAsNew, at most %d describe per question; v1 Drained %s after the chain ended",
		len(chain), open, minRelease, minRunning, onlyContinued, maxDescribes, drainedAt.Sub(ended))
}

// deleteExecution deletes one execution of runID by its execution id, and
// waits until its history reads NotFound.
func (s *chaosStack) deleteExecution(t *testing.T, runID, execution string) {
	t.Helper()
	if _, err := s.control.WorkflowService().DeleteWorkflowExecution(s.ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: execution},
	}); err != nil {
		t.Fatalf("delete execution %s of %s: %v", execution, runID, err)
	}
	s.waitExecutionGone(t, runID, execution, time.Minute)
	s.transcript.step("execution %s of %s deleted", execution, runID)
}

// waitExecutionGone waits until one execution's history reads NotFound.
func (s *chaosStack) waitExecutionGone(t *testing.T, runID, execution string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		iter := s.control.GetWorkflowHistory(s.ctx, runID, execution, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		_, err := iter.Next()
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution %s of %s still reads (%v) %s after its deletion", execution, runID, err, within)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestIntegration_ContinuationChaosC10_FirstGenerationDeletedWhileTheRunContinues:
// generation 0's execution is deleted while the run continues in a later
// one; GetRun, Events, a stream opened at submit, a stream opened after the
// deletion and the final archive are all complete.
func TestIntegration_ContinuationChaosC10_FirstGenerationDeletedWhileTheRunContinues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C10")
	g := newGates()
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, "c20"))
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c10-delete\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, "")), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	g.waitEntered(t, "c20", 2*time.Minute)
	chain := s.chain(runID)
	if len(chain) < 2 {
		t.Fatalf("%d executions when c20 started: want the run past its first switch", len(chain))
	}
	s.deleteExecution(t, runID, chain[0])
	late := s.openStream(s.reader, runID, 0)
	g.open("c20")
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	if got := late.result(t, time.Minute); strings.Join(eventKeys(got), "\n") != strings.Join(eventKeys(events), "\n") {
		t.Fatalf("a stream opened after generation 0 was deleted read %d events, Events(0) %d", len(got), len(events))
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if want := carryOf(t, s.history(runID, s.chain(runID)[len(s.chain(runID))-1])).GetCreatedAt().AsTime(); !run.GetCreatedAt().AsTime().Equal(want) {
		t.Fatalf("created_at %v after generation 0 was deleted, want the carried %v", run.GetCreatedAt().AsTime(), want)
	}
	t.Logf("C10: generation 0 (%s) deleted mid-run; GetRun, Events(0) (%d), both streams and the archive complete", chain[0], len(events))
}

// TestIntegration_ContinuationChaosC11_RetireDeletesEveryGeneration: the
// cleanup worker, with a test clock past the run's retention, retires a run
// that continued as new: every generation's execution is deleted, no list or
// count shows a phantom live row, and GetRun answers from the archive.
func TestIntegration_ContinuationChaosC11_RetireDeletesEveryGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cfg, _ := privateCleanupBucket(t, ctx)
	runArchive, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := newChaosStackWith(t, ctx, "C11", cfg, runArchive)
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	const orkNamespace = "chaos-retire"
	plan, _ := s.tcPlan()
	plan.Namespace = orkNamespace
	wf := compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c11-retire\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, ""))
	wf.Namespace = orkNamespace
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, wf, nil, plan)
	if status, err := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	s.requireOneRunRead(runID, nil, orkstackv1.Status_STATUS_SUCCEEDED, "")
	chain := s.chain(runID)
	if len(chain) < 2 {
		t.Fatalf("%d executions: the run never switched", len(chain))
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	policies, err := artifacts.NewS3RetentionPolicyStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	retention := archivecleanup.NewRetentionController(policies, s.reader, s.namespace, archivecleanup.DefaultRetentionBuffer, logger)
	if _, err := retention.SetMax(ctx, 999*time.Hour); err != nil {
		t.Fatalf("SetMax: %v", err)
	}
	namespaces, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := namespaces.Put(ctx, &orkstackv1.Namespace{Name: orkNamespace, RunRetention: durationpb.New(time.Minute)},
		artifacts.PutConditions{}); err != nil {
		t.Fatalf("write the namespace document: %v", err)
	}
	cleaner := archivecleanup.NewCleaner(archivecleanup.CleanupConfig{
		TemporalNamespace: s.namespace, Interval: time.Hour, RetentionBuffer: archivecleanup.DefaultRetentionBuffer, BatchSize: 100,
	}, runArchive, s.reader, namespaces, retention, logger).WithClock(func() time.Time {
		return time.Now().UTC().Add(2 * time.Minute)
	})
	stats, err := cleaner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsCleaned != 1 {
		t.Fatalf("cleanup %+v, want the one run retired", stats)
	}
	for _, execution := range chain {
		s.waitExecutionGone(t, runID, execution, time.Minute)
	}
	s.transcript.step("every one of the %d executions reads NotFound", len(chain))
	running, err := s.reader.ListRuns(ctx, engine.RunQuery{Status: orkstackv1.Status_STATUS_RUNNING, Complete: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range running {
		if r.GetId() == runID {
			t.Fatal("ListRuns(status RUNNING) still shows the retired run")
		}
	}
	if n, err := s.reader.RunningCount(ctx, orkNamespace, 1); err != nil || n != 0 {
		t.Fatalf("RunningCount(%s) = %d, %v after the run was retired", orkNamespace, n, err)
	}
	if _, err := s.reader.GetRun(ctx, runID); err == nil {
		t.Fatal("the cluster still answers GetRun for the retired run: the read below would not be the archive's")
	}
	a := s.serveAPI(api.Config{Namespaces: namespaces})
	got, err := a.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: orkNamespace}))
	if err != nil {
		t.Fatalf("GetRun of the retired run: %v", err)
	}
	if got.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || got.Msg.GetRun().GetId() != runID {
		t.Fatalf("GetRun from the archive = %s %v", got.Msg.GetRun().GetId(), got.Msg.GetRun().GetStatus())
	}
	s.transcript.figure("generations_retired", len(chain))
	t.Logf("C11: retired %d generations; no live row; GetRun answered %v from the archive", len(chain), got.Msg.GetRun().GetStatus())
}

// visibilityPoll is one 100 ms reading of every list, count and gate a
// switching run must stay visible to.
type visibilityPoll struct {
	Start, End     time.Time
	Count          int64  `json:"running_count"`
	CountErr       string `json:"count_err,omitempty"`
	RunningRows    int    `json:"running_rows"`
	CompleteRows   int    `json:"complete_rows"`
	Status         string `json:"status"`
	CreatedAt      string `json:"created_at"`
	OrderOK        bool   `json:"order_ok"`
	ListErr        string `json:"list_err,omitempty"`
	GateRefused    bool   `json:"gate_refused"`
	GateMessage    string `json:"gate_message"`
	OnlyContinued  bool   `json:"only_continued_as_new"`
	Describes      int64  `json:"describes"`
	MiddleRunsLive bool   `json:"middle_live"`
}

// TestIntegration_ContinuationChaosC12_ARunIsNeverInvisibleWhileItSwitches:
// from before the first switch until the run ends, polled every 100 ms,
// RunningCount, ListRuns(status RUNNING), the user-facing ListRuns and the
// Orkflow deletion gate all see the run once, live, with its original
// created_at and in created_at order among a newer run — never absent. A
// submit at max_running − 1 while the run is mid-switch is admitted. After
// the run ends, ListRuns(status RUNNING) drops it with no describe.
func TestIntegration_ContinuationChaosC12_ARunIsNeverInvisibleWhileItSwitches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C12")
	hook := newBlockingHook(func(_ string, offset int64) bool { return offset == 0 })
	t.Cleanup(func() { hook.open(nil) })
	s.startReplica("replica-a", replicaOptions{beforeRead: hook.read})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)

	namespaces, err := artifacts.NewS3NamespaceStore(ctx, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	ns := fmt.Sprintf("chaos-c12-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	if _, err := namespaces.Put(ctx, &orkstackv1.Namespace{Name: ns, MaxRunning: 3}, artifacts.PutConditions{}); err != nil {
		t.Fatalf("write the namespace document: %v", err)
	}
	t.Cleanup(func() { _ = namespaces.Delete(context.Background(), ns) })
	counting := &countingClient{Client: s.control}
	reader := engine.NewTemporalRunReader(counting).WithSnapshotStore(s.store)
	cfg := tcAPIConfig()
	cfg.Namespaces, cfg.Reader = namespaces, reader
	a := s.serveAPI(cfg)
	apply := func(yaml string) {
		if _, err := a.svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml, Namespace: ns})); err != nil {
			t.Fatalf("ApplyOrkflow: %v", err)
		}
	}
	submit := func(name string) (*orkstackv1.SubmitRunResponse, error) {
		resp, err := a.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name, Namespace: ns}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: c12-chain\nversion: \"1.0.0\"\ntasks:\n" + guardChain("c", 30, ""))
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: c12-middle\nversion: \"1.0.0\"\ntasks:\n  - id: wait\n    await: {timeout_seconds: 3600}\n")
	apply("apiVersion: orkstack/v1\nkind: Orkflow\nname: c12-quick\nversion: \"1.0.0\"\ntasks:\n  - id: one\n    uses: shell\n    with: {op: exec}\n")

	chainRun, err := submit("c12-chain")
	if err != nil {
		t.Fatalf("submit the chain: %v", err)
	}
	runID := chainRun.GetRun().GetId()
	s.transcript.run(runID)
	// The run's created_at is its first execution's start, which every read
	// reports for the whole chain (MemoCreatedAt), at the index's precision.
	createdAt := s.history(runID, "").GetEvents()[0].GetEventTime().AsTime().Truncate(time.Microsecond)
	// A run created after the chain but started before its next
	// generation: by StartTime the chain looks newer, by created_at it is
	// older, and every user-facing page must say the latter.
	middleRun, err := submit("c12-middle")
	if err != nil {
		t.Fatalf("submit the middle run: %v", err)
	}
	middleID := middleRun.GetRun().GetId()
	s.transcript.step("chain %s and middle %s submitted (max_running 3)", runID, middleID)
	// The polls below judge the chain's switch, so they start once the
	// cluster's index lists both runs: a poll in the first moments after the
	// middle run's submit can find it not yet indexed, which is the index's
	// lag on a fresh start, not a chain that went invisible (a feature proof
	// of the task-ceiling defaults read count 1 a tenth of a second after the submit).
	indexed := time.Now().Add(30 * time.Second)
	for {
		count, err := reader.RunningCount(ctx, ns, 3)
		if err == nil && count >= 2 {
			break
		}
		if time.Now().After(indexed) {
			t.Fatalf("the cluster's index never listed the chain and the middle run (count %d, %v)", count, err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	s.transcript.step("the index lists the chain and the middle run")

	var (
		pollMu sync.Mutex
		polls  []visibilityPoll
	)
	stopPolling := make(chan struct{})
	pollingDone := make(chan struct{})
	go func() {
		defer close(pollingDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			p := visibilityPoll{Start: time.Now()}
			before := counting.describes.Load()
			p.Count, err = reader.RunningCount(ctx, ns, 3)
			if err != nil {
				p.CountErr = err.Error()
			}
			running, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: ns, Status: orkstackv1.Status_STATUS_RUNNING, Complete: true})
			if err != nil {
				p.ListErr = err.Error()
			}
			for _, r := range running {
				if r.GetId() == runID {
					p.RunningRows++
				}
			}
			complete, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: ns, Complete: true})
			if err != nil {
				p.ListErr += err.Error()
			}
			chainAt, middleAt := -1, -1
			for i, r := range complete {
				switch r.GetId() {
				case runID:
					p.CompleteRows++
					chainAt = i
					p.Status = r.GetStatus().String()
					p.CreatedAt = r.GetCreatedAt().AsTime().Truncate(time.Microsecond).Format(time.RFC3339Nano)
				case middleID:
					middleAt = i
					p.MiddleRunsLive = r.GetStatus() == orkstackv1.Status_STATUS_AWAITING || r.GetStatus() == orkstackv1.Status_STATUS_RUNNING
				}
			}
			p.OrderOK = chainAt >= 0 && middleAt >= 0 && middleAt < chainAt // newest created_at first
			_, err = a.svc.DeleteOrkflow(ctx, connect.NewRequest(&orkstackv1.DeleteOrkflowRequest{Name: "c12-chain", Namespace: ns}))
			if err != nil {
				p.GateRefused, p.GateMessage = connect.CodeOf(err) == connect.CodeFailedPrecondition, err.Error()
			}
			p.Describes = counting.describes.Load() - before
			p.OnlyContinued, _ = s.onlyContinuedAsNew(runID)
			p.End = time.Now()
			pollMu.Lock()
			polls = append(polls, p)
			pollMu.Unlock()
			select {
			case <-stopPolling:
				return
			case <-ticker.C:
			}
		}
	}()

	hook.waitEntered(t, 2*time.Minute)
	s.transcript.step("the run's next generation is starting (its first saved-state read held): submit at max_running − 1")
	quick, err := submit("c12-quick")
	if err != nil {
		t.Fatalf("a submit at max_running − 1 while the run switched was refused: %v", err)
	}
	s.transcript.step("submit admitted: %s", quick.GetRun().GetId())
	hook.open(nil)
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	described, err := s.control.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	ended := described.GetWorkflowExecutionInfo().GetCloseTime().AsTime()
	close(stopPolling)
	<-pollingDone

	// After the run: ListRuns(status RUNNING) drops it, with no describe.
	before := counting.describes.Load()
	running, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: ns, Status: orkstackv1.Status_STATUS_RUNNING, Complete: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range running {
		if r.GetId() == runID {
			t.Fatal("ListRuns(status RUNNING) still lists the finished run")
		}
	}
	if d := counting.describes.Load() - before; d != 0 {
		t.Fatalf("dropping the finished run from ListRuns(status RUNNING) described %d executions", d)
	}
	_ = s.control.CancelWorkflow(ctx, middleID, "")

	pollMu.Lock()
	defer pollMu.Unlock()
	open, onlyContinued, minCount, maxDescribes := 0, 0, int64(-1), int64(0)
	for _, p := range polls {
		if !p.End.Before(ended) {
			continue
		}
		open++
		maxDescribes = max(maxDescribes, p.Describes)
		if p.CountErr != "" || p.ListErr != "" {
			t.Fatalf("a poll at %s failed: %q %q", p.Start.Format(time.RFC3339Nano), p.CountErr, p.ListErr)
		}
		if p.Count < 2 || p.RunningRows != 1 || p.CompleteRows != 1 || p.Status != orkstackv1.Status_STATUS_RUNNING.String() ||
			p.CreatedAt != createdAt.Format(time.RFC3339Nano) || !p.OrderOK {
			t.Fatalf("a poll ending at %s, with the run live, read count %d, %d RUNNING row(s), %d row(s) in the "+
				"complete list (%s, created %s, in created_at order: %t)", p.End.Format(time.RFC3339Nano), p.Count,
				p.RunningRows, p.CompleteRows, p.Status, p.CreatedAt, p.OrderOK)
		}
		// Live, or — once its terminal event is archived, a moment before
		// its execution closes — finished within the window: either way the
		// one run blocks the deletion, never none.
		if !p.GateRefused || !strings.Contains(p.GateMessage, "1 run(s) block deletion") {
			t.Fatalf("the deletion gate at %s answered %q, want a refusal naming the run", p.End.Format(time.RFC3339Nano), p.GateMessage)
		}
		if minCount < 0 || p.Count < minCount {
			minCount = p.Count
		}
		if p.OnlyContinued {
			onlyContinued++
		}
	}
	if open == 0 {
		t.Fatal("no poll answered while the run was live")
	}
	// A poll asks two liveness questions (RunningCount, ListRuns RUNNING),
	// each of which may describe the one chain while it is mid-switch.
	if maxDescribes > 2 {
		t.Fatalf("a poll described %d executions while one chain was live", maxDescribes)
	}
	proof.WriteJSON(t, s.transcript.group, "polls.json", polls)
	s.transcript.figure("polls_while_open", open)
	s.transcript.figure("min_running_count", minCount)
	s.transcript.figure("polls_seeing_only_continued_as_new", onlyContinued)
	s.transcript.figure("max_describes_per_poll", maxDescribes)
	t.Logf("C12: %d polls while live: min RunningCount %d (the chain and the middle run), one row each, created_at kept, "+
		"order kept, deletion refused; %d saw only ContinuedAsNew; submit at max_running − 1 admitted mid-switch",
		open, minCount, onlyContinued)
}

// s3Key is the bucket key of an s3:// URI.
func s3Key(t *testing.T, uri string) (bucket, key string) {
	t.Helper()
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		t.Fatalf("%q is not an s3:// URI", uri)
	}
	bucket, key, _ = strings.Cut(rest, "/")
	return bucket, key
}

// TestIntegration_ContinuationChaosC13_NeverIdleFailsBeforeTheLimit: an open
// pin whose remaining member waits on an await that is never resolved, while
// a long chain of other work runs: the hold is given up, and the run ends
// RUN_HISTORY_LIMIT, naming the pin and the await, below the cluster's limit
// — Failed, never Terminated.
func TestIntegration_ContinuationChaosC13_NeverIdleFailsBeforeTheLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C13")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	const maxEvents = 3000
	budget := engine.DefaultContinuationBudget(300, 1<<20, 5*time.Minute)
	plan := s.plan(maxEvents, engine.DefaultHistoryMaxBytes, &budget)
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: c13-never-idle\nversion: \"1.0.0\"\npins:\n  deploy: {}\ntasks:\n" +
		"  - id: q1\n    uses: shell\n    pin: deploy\n    needs: []\n    with: {op: exec}\n" +
		"  - id: approve\n    needs: [q1]\n    await: {timeout_seconds: 3600}\n" +
		"  - id: q2\n    uses: shell\n    pin: deploy\n    needs: [approve]\n    with: {op: exec}\n" +
		guardChain("c", 250, "")
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, doc), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	gaveUp := s.logs.wait(t, ctx, logGiveUp, runID, 3*time.Minute)
	s.transcript.step("hold given up: %v", gaveUp.Fields["reason"])
	status, _ := s.wait(runID, 6*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed (never Terminated)", status)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, engine.RunHistoryLimitCode)
	run, _ := s.reader.GetRun(ctx, runID)
	message := run.GetError().GetMessage() + " " + run.GetError().GetDetails()
	if !strings.Contains(message, "deploy") || !strings.Contains(message, "approve") {
		t.Fatalf("RUN_HISTORY_LIMIT says %q: want it to name the pin and the await", message)
	}
	chain, histories := s.histories(runID)
	last := histories[len(histories)-1]
	if n := len(last.GetEvents()); n >= maxEvents {
		t.Fatalf("the final history holds %d events, at or over the %d limit", n, maxEvents)
	}
	if failureType(t, last) != engine.RunHistoryLimitCode {
		t.Fatalf("the execution failed with type %q", failureType(t, last))
	}
	s.transcript.figure("final_history_events", len(last.GetEvents()))
	s.transcript.figure("message", message)
	t.Logf("C13: hold given up; RUN_HISTORY_LIMIT (%q) at %d events of %d in generation %d; %d events",
		message, len(last.GetEvents()), maxEvents, len(chain)-1, len(events))
}

// switchThenHoldTheRead starts a chain that switches, on a replica whose
// next generation's first saved-state read blocks in its hook, and returns
// once the hook is entered, with the carry that generation started from.
func (s *chaosStack) switchThenHoldTheRead(t *testing.T, name string, budget engine.ContinuationBudget, o replicaOptions,
	hook *blockingHook,
) (string, *streamCollector, *orkstackv1.RunContinuation) {
	t.Helper()
	o.beforeRead = hook.read
	s.startReplica("replica-a", o)
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan := s.plan(1000, engine.DefaultHistoryMaxBytes, &budget)
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: "+name+"\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, "")), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	hook.waitEntered(t, 2*time.Minute)
	execution, _ := s.current(runID)
	carry := carryOf(t, s.history(runID, execution))
	if carry == nil {
		t.Fatal("the read hook was entered by a generation with no carry")
	}
	s.transcript.step("generation %d's first saved-state read is held (checkpoint %s, %d bytes)",
		carry.GetGeneration(), carry.GetCheckpoint().GetUri(), carry.GetCheckpoint().GetSizeBytes())
	return runID, stream, carry
}

// TestIntegration_ContinuationChaosC16_OverwrittenCheckpointIsCorrupt: the
// checkpoint is overwritten with other bytes between the switch and the next
// generation's read. The run ends RUN_STATE_CORRUPT: a published RUN_FAILED
// whose snapshot is the switch snapshot with only its unfinished rows
// CANCELED, an archive body, and Temporal Failed with that type.
func TestIntegration_ContinuationChaosC16_OverwrittenCheckpointIsCorrupt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C16")
	hook := newBlockingHook(func(_ string, offset int64) bool { return offset == 0 })
	t.Cleanup(func() { hook.open(nil) })
	_, budget := s.tcPlan()
	runID, stream, carry := s.switchThenHoldTheRead(t, "c16-corrupt", budget, replicaOptions{}, hook)
	raw := newRawS3ClientForCommitE2E(t, s.cfg)
	bucket, key := s3Key(t, carry.GetCheckpoint().GetUri())
	forged := bytes.Repeat([]byte("not the run's saved state "), int(carry.GetCheckpoint().GetSizeBytes())/26+1)
	forged = forged[:carry.GetCheckpoint().GetSizeBytes()] // same size: only the hashes can tell
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
		Body: bytes.NewReader(forged), ContentLength: aws.Int64(int64(len(forged)))}); err != nil {
		t.Fatalf("overwrite the checkpoint: %v", err)
	}
	s.transcript.step("checkpoint overwritten with %d other bytes", len(forged))
	hook.open(nil)
	status, _ := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed", status)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, engine.RunStateCorruptCode)
	if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("the last event is %v, want a published RUN_FAILED", last.GetType())
	}
	_, histories := s.histories(runID)
	if got := failureType(t, histories[len(histories)-1]); got != engine.RunStateCorruptCode {
		t.Fatalf("the execution failed with type %q, want %s", got, engine.RunStateCorruptCode)
	}
	s.requireTerminalIsTheSwitchSnapshot(t, runID, carry)
	t.Logf("C16: RUN_STATE_CORRUPT with a published RUN_FAILED at %d, archived; the terminal snapshot is the switch snapshot "+
		"with its unfinished rows CANCELED", events[len(events)-1].GetSequence())
}

// requireTerminalIsTheSwitchSnapshot checks the run's terminal snapshot is
// the switch snapshot carry names, every terminal row unchanged and every
// other row CANCELED.
func (s *chaosStack) requireTerminalIsTheSwitchSnapshot(t *testing.T, runID string, carry *orkstackv1.RunContinuation) {
	t.Helper()
	data, err := s.store.ReadSnapshot(s.ctx, carry.GetSnapshot().GetUri())
	if err != nil {
		t.Fatal(err)
	}
	switched, err := engine.DecodeSwitchSnapshot(data, carry.GetSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.reader.GetRun(s.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(run.GetTaskRuns()) != len(switched.GetTaskRuns()) {
		t.Fatalf("the terminal snapshot has %d rows, the switch snapshot %d", len(run.GetTaskRuns()), len(switched.GetTaskRuns()))
	}
	canceled := 0
	for i, before := range switched.GetTaskRuns() {
		after := run.GetTaskRuns()[i]
		terminal := before.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED || before.GetStatus() == orkstackv1.Status_STATUS_SKIPPED ||
			before.GetStatus() == orkstackv1.Status_STATUS_FAILED || before.GetStatus() == orkstackv1.Status_STATUS_CANCELED
		switch {
		case terminal && !proto.Equal(before, after):
			t.Fatalf("terminal row %s changed: %v -> %v", before.GetTaskId(), before, after)
		case !terminal && after.GetStatus() != orkstackv1.Status_STATUS_CANCELED:
			t.Fatalf("unfinished row %s is %v, want CANCELED", before.GetTaskId(), after.GetStatus())
		case !terminal:
			canceled++
		}
	}
	s.transcript.step("terminal snapshot: %d rows as switched, %d unfinished rows CANCELED", len(switched.GetTaskRuns())-canceled, canceled)
}

// TestIntegration_ContinuationChaosC17_DeletedCheckpointIsUnavailable: the
// checkpoint is deleted before the next generation reads it. The reads are
// retried by Temporal until the restore timeout (1 minute), then the run
// ends RUN_STATE_UNAVAILABLE with a terminal event, and that generation's
// whole history stays within RestoreHistoryBound of its plan.
func TestIntegration_ContinuationChaosC17_DeletedCheckpointIsUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C17/deleted")
	hook := newBlockingHook(func(_ string, offset int64) bool { return offset == 0 })
	t.Cleanup(func() { hook.open(nil) })
	budget := engine.DefaultContinuationBudget(120, 1<<20, time.Minute)
	runID, stream, carry := s.switchThenHoldTheRead(t, "c17-deleted", budget, replicaOptions{}, hook)
	raw := newRawS3ClientForCommitE2E(t, s.cfg)
	bucket, key := s3Key(t, carry.GetCheckpoint().GetUri())
	if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatalf("delete the checkpoint: %v", err)
	}
	deletedAt := time.Now()
	s.transcript.step("checkpoint deleted")
	hook.open(nil)
	status, _ := s.wait(runID, 4*time.Minute)
	took := time.Since(deletedAt)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed", status)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, engine.RunStateUnavailableCode)
	if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("the last event is %v, want a published RUN_FAILED", last.GetType())
	}
	_, histories := s.histories(runID)
	last := histories[len(histories)-1]
	bound := engine.RestoreHistoryBound(budget)
	if n := int64(len(last.GetEvents())); n > bound {
		t.Fatalf("the failed generation's history holds %d events, over RestoreHistoryBound %d", n, bound)
	}
	// Temporal retries a read until its next backoff would fall past the
	// restore deadline: with the pinned policy (1 s doubling, at most 60 s)
	// and a one-minute timeout, the last retry that fits starts 31 s in.
	giveUp := lastRetryWithin(budget.Restore)
	if took < giveUp-2*time.Second || took > time.Duration(budget.Restore.TimeoutSeconds)*time.Second+15*time.Second {
		t.Fatalf("the run gave up after %s: want it at the last retry inside its restore timeout (%s), never later than "+
			"the timeout", took, giveUp)
	}
	s.transcript.figure("last_retry_inside_the_timeout", giveUp.String())
	s.transcript.figure("bootstrap_events", len(last.GetEvents()))
	s.transcript.figure("restore_history_bound", bound)
	s.transcript.figure("ended_after", took.String())
	t.Logf("C17: RUN_STATE_UNAVAILABLE %s after the delete with a terminal event; the generation's history %d events ≤ bound %d",
		took, len(last.GetEvents()), bound)
}

// lastRetryWithin is when the last retry of a read that keeps failing at
// once starts under policy: Temporal does not start a retry whose backoff
// would carry it past the restore deadline.
func lastRetryWithin(policy engine.RestorePolicy) time.Duration {
	deadline := time.Duration(policy.TimeoutSeconds) * time.Second
	backoff := time.Duration(policy.BackoffInitialMillis) * time.Millisecond
	maxBackoff := time.Duration(policy.BackoffMaxSeconds) * time.Second
	at := time.Duration(0)
	for at+backoff <= deadline {
		at += backoff
		backoff = min(2*backoff, maxBackoff)
	}
	return at
}

// TestIntegration_ContinuationChaosC17_StoreBackWithinTheWindowContinues:
// object storage is unreachable when the next generation starts reading its
// saved state and comes back within the restore timeout: the reads succeed
// on a retry, and the run continues and succeeds.
func TestIntegration_ContinuationChaosC17_StoreBackWithinTheWindowContinues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C17/restored")
	proxy := s.minioProxy()
	hook := newBlockingHook(func(_ string, offset int64) bool { return offset == 0 })
	hook.onEntry = proxy.Cut
	t.Cleanup(func() { hook.open(nil) })
	budget := engine.DefaultContinuationBudget(120, 1<<20, time.Minute)
	runID, stream, _ := s.switchThenHoldTheRead(t, "c17-restored", budget,
		replicaOptions{store: s.snapshotStoreVia(proxy)}, hook)
	s.transcript.step("object storage cut at the first saved-state read")
	hook.open(nil)
	time.Sleep(10 * time.Second) // an outage of a known length, inside the one-minute window
	proxy.Restore()
	s.transcript.step("object storage restored after 10 s (%+v)", proxy.Stats())
	status, err := s.wait(runID, 4*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	if proxy.Stats().Refused == 0 {
		t.Fatal("no read met the cut: the outage was not exercised")
	}
	t.Logf("C17 variant: storage back within the window; the run continued and succeeded (%d events, %d refused connections)",
		len(events), proxy.Stats().Refused)
}

// TestIntegration_ContinuationChaosC18_ForgedCarriesEndExplicitly: a run
// started directly with a forged fifth argument of an unknown format ends
// RUN_STATE_UNSUPPORTED, and a format-1 carry under a plan with no
// continuation budget ends RUN_STATE_CORRUPT; each publishes a terminal
// event under its workflow id and is archived, with no workflow task failed.
func TestIntegration_ContinuationChaosC18_ForgedCarriesEndExplicitly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C18")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	withBudget, _ := s.tcPlan()
	withoutBudget := s.plan(1000, engine.DefaultHistoryMaxBytes, nil)
	for _, tc := range []struct {
		name string
		plan engine.RunPlan
		code string
	}{
		{"unknown format", withBudget, engine.RunStateUnsupportedCode},
		{"no budget", withoutBudget, engine.RunStateCorruptCode},
	} {
		version := int32(2)
		if tc.code == engine.RunStateCorruptCode {
			version = 1
		}
		runID := s.newRunID()
		s.transcript.run(runID)
		wf := compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c18-forged\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 3, ""))
		wf.Namespace = "default"
		carry := &orkstackv1.RunContinuation{Version: version, Generation: 1, EventSequence: 0}
		if _, err := s.control.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: runID, TaskQueue: s.runQueue},
			engine.RunWorkflowType, "", wf, (*orkstackv1.Payload)(nil), tc.plan, carry); err != nil {
			t.Fatalf("%s: start: %v", tc.name, err)
		}
		status, _ := s.wait(runID, 2*time.Minute)
		if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
			t.Fatalf("%s: closed %v, want Failed", tc.name, status)
		}
		h := s.history(runID, "")
		if got := failureType(t, h); got != tc.code {
			t.Fatalf("%s: failed with type %q, want %s", tc.name, got, tc.code)
		}
		if failures := factsOf(t, h).taskFailures; len(failures) != 0 {
			t.Fatalf("%s: %d workflow tasks failed: a malformed carry must end the run, never fail a task", tc.name, len(failures))
		}
		events := awaitArchivedEvents(t, ctx, s.archive, runID)
		if len(events) == 0 || events[len(events)-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED ||
			events[len(events)-1].GetRunId() != runID {
			t.Fatalf("%s: archived events %v, want the published RUN_FAILED under the run id", tc.name, eventKeys(events))
		}
		// The archive answers the run: a forged carry names no earlier
		// events a reader could verify, so the cluster's record is read
		// through it only to refuse it explicitly.
		run, err := s.archive.LoadRun(ctx, runID)
		if err != nil || run.GetError().GetCode() != tc.code || run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
			t.Fatalf("%s: the archived run = %v %v %v", tc.name, run.GetStatus(), run.GetError(), err)
		}
		if _, err := s.reader.GetRun(ctx, runID); err == nil {
			t.Fatalf("%s: the cluster's record of a forged carry read as a run", tc.name)
		}
		s.transcript.step("%s: %s, published and archived (%s)", tc.name, tc.code, run.GetError().GetMessage())
	}
	t.Log("C18: a forged unknown carry ends RUN_STATE_UNSUPPORTED and a carry without a budget RUN_STATE_CORRUPT, each published and archived")
}

// TestIntegration_ContinuationChaosC19_CancelWhileReadsBackOff: an
// operator's cancel while the next generation's reads fail and Temporal backs
// off between them ends the run RUN_CANCELED within 30 s, never RUN_STATE_*.
func TestIntegration_ContinuationChaosC19_CancelWhileReadsBackOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C19/backoff")
	proxy := s.minioProxy()
	hook := newBlockingHook(func(_ string, offset int64) bool { return offset == 0 })
	hook.onEntry = proxy.Cut
	t.Cleanup(func() { hook.open(nil) })
	_, budget := s.tcPlan()
	runID, _, _ := s.switchThenHoldTheRead(t, "c19-backoff", budget, replicaOptions{store: s.snapshotStoreVia(proxy)}, hook)
	hook.open(nil)
	// Long enough that Temporal is backing off between failing reads.
	time.Sleep(20 * time.Second)
	canceledAt := time.Now()
	if err := s.starter.CancelRun(ctx, runID, ""); err != nil {
		t.Fatal(err)
	}
	// Storage stays down until the cancel has reached the retrying read —
	// its marker records the read canceled — and comes back for the run's
	// terminal event, so the latency measured is the cancel's.
	reached := s.waitReadCanceled(t, runID, time.Minute)
	reachedAfter := reached.Sub(canceledAt)
	proxy.Restore()
	status, _ := s.wait(runID, 2*time.Minute)
	latency := time.Since(canceledAt)
	s.transcript.figure("cancel_reached_the_read_after", reachedAfter.String())
	if status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the run closed %v, want Canceled", status)
	}
	if latency > 30*time.Second {
		t.Fatalf("the run ended %s after the cancel, over 30 s", latency)
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil || run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("GetRun = %v %v, want RUN_CANCELED", run.GetError(), err)
	}
	s.transcript.figure("cancel_latency", latency.String())
	s.transcript.figure("proxy", proxy.Stats())
	t.Logf("C19: canceled while reads backed off: RUN_CANCELED %s after the cancel", latency)
}

// waitReadCanceled waits until the run's current execution records a
// saved-state read that ended canceled, and returns when the cluster
// recorded it.
func (s *chaosStack) waitReadCanceled(t *testing.T, runID string, within time.Duration) time.Time {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		execution, _ := s.current(runID)
		for _, event := range s.history(runID, execution).GetEvents() {
			attrs := event.GetMarkerRecordedEventAttributes()
			if attrs == nil || attrs.GetMarkerName() != "LocalActivity" || attrs.GetFailure() == nil {
				continue
			}
			var data struct{ ActivityType string }
			_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data)
			if data.ActivityType == engine.ActivityReadContinuationChunk && attrs.GetFailure().GetCanceledFailureInfo() != nil {
				return event.GetEventTime().AsTime()
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cancel never reached the run's retrying read within %s", within)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestIntegration_ContinuationChaosC19_CancelWhileAReadIsBlocked: an
// operator's cancel while a read attempt is blocked ends the run
// RUN_CANCELED within 30 s, the blocked attempt cancelled without its hook
// ever returning.
func TestIntegration_ContinuationChaosC19_CancelWhileAReadIsBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C19/blocked")
	hook := newBlockingHook(func(_ string, offset int64) bool { return offset == 0 })
	t.Cleanup(func() { hook.open(nil) })
	_, budget := s.tcPlan()
	runID, _, carry := s.switchThenHoldTheRead(t, "c19-blocked", budget, replicaOptions{}, hook)
	canceledAt := time.Now()
	if err := s.starter.CancelRun(ctx, runID, ""); err != nil {
		t.Fatal(err)
	}
	status, _ := s.wait(runID, 2*time.Minute)
	latency := time.Since(canceledAt)
	hook.mu.Lock()
	returned := hook.released
	hook.mu.Unlock()
	if returned {
		t.Fatal("the blocked read's hook returned before the run ended")
	}
	if status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED || latency > 30*time.Second {
		t.Fatalf("the run closed %v %s after the cancel, want Canceled within 30 s", status, latency)
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil || run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("GetRun = %v %v, want RUN_CANCELED", run.GetError(), err)
	}
	s.requireTerminalIsTheSwitchSnapshot(t, runID, carry)
	s.transcript.figure("cancel_latency", latency.String())
	t.Logf("C19 variant: canceled with a read blocked: RUN_CANCELED %s after the cancel; the hook never returned", latency)
}

// TestIntegration_ContinuationChaosC20_PinHostLostDuringAHold: the host of
// an open pin is killed while the run holds for that pin: the run fails
// PIN_HOST_LOST, never switches and never hangs, and the surviving hosts
// were never handed the pin's tasks.
func TestIntegration_ContinuationChaosC20_PinHostLostDuringAHold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C20")
	g := newGates()
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	hosts := s.startPinHosts(gated(g, "p1"))
	plan, _ := s.tcPlan()
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: c20-pin-lost\nversion: \"1.0.0\"\npins:\n  build: {}\ntasks:\n" +
		"  - id: p1\n    uses: shell\n    pin: build\n    needs: []\n    with: {op: exec}\n" +
		"  - id: p2\n    uses: shell\n    pin: build\n    needs: [p1]\n    with: {op: exec}\n" +
		guardChain("c", 40, "")
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, doc), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	g.waitEntered(t, "p1", time.Minute)
	s.logs.wait(t, ctx, logQuiesce, runID, 2*time.Minute)
	host := s.orkersOf("p1")[0]
	for _, h := range hosts {
		if h.orker.name == host {
			h.kill()
		}
	}
	s.transcript.step("pin host %s killed while the run held for its pin", host)
	status, _ := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed", status)
	}
	if chain := s.chain(runID); len(chain) != 1 {
		t.Fatalf("the run switched (%d executions) with its pin's host lost", len(chain))
	}
	s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, "")
	run, _ := s.reader.GetRun(ctx, runID)
	if taskRunOfRun(run, "p1").GetError().GetCode() != "PIN_HOST_LOST" {
		t.Fatalf("run error %v, p1 %v: want PIN_HOST_LOST", run.GetError(), taskRunOfRun(run, "p1").GetError())
	}
	for _, m := range []string{"p1", "p2"} {
		for _, on := range s.orkersOf(m) {
			if on != host {
				t.Fatalf("surviving host %s was handed %s", on, m)
			}
		}
	}
	t.Logf("C20: pin host %s lost during the hold: PIN_HOST_LOST, no switch, the survivors never ran the pin's tasks", host)
}

// requireCarriedObjects checks every object the run's chain names — each
// generation's segment, switch snapshot and checkpoint — reads back from
// object storage at its content key: its bytes hash to the digest its key
// and its reference carry. It also checks the run's continuation prefix
// holds exactly one segment per closed generation.
func (s *chaosStack) requireCarriedObjects(t *testing.T, runID string) {
	t.Helper()
	_, histories := s.histories(runID)
	checked := 0
	for i := 1; i < len(histories); i++ {
		carry := carryOf(t, histories[i])
		generation := carry.GetGeneration() - 1
		objects := map[string]*orkstackv1.ContinuationObject{
			artifacts.ContinuationKindCheckpoint: carry.GetCheckpoint(),
			artifacts.ContinuationKindSnapshot:   carry.GetSnapshot(),
			artifacts.ContinuationKindEvents:     carry.GetSegments()[generation].GetObject(),
		}
		for kind, obj := range objects {
			data, err := s.store.ReadSnapshot(s.ctx, obj.GetUri())
			if err != nil {
				t.Fatalf("generation %d's %s %s does not read: %v", generation, kind, obj.GetUri(), err)
			}
			if artifacts.ContinuationSHA256(data) != obj.GetSha256() ||
				!artifacts.ContinuationURIMatches(obj.GetUri(), "default", runID, generation, kind, obj.GetSha256()) {
				t.Fatalf("generation %d's %s at %s is not at its content key", generation, kind, obj.GetUri())
			}
			checked++
		}
	}
	raw := newRawS3ClientForCommitE2E(t, s.cfg)
	prefix, err := artifacts.RunSnapshotPrefix("default", runID)
	if err != nil {
		t.Fatal(err)
	}
	segments := map[string]int{}
	page, err := raw.ListObjectsV2(s.ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.S3Bucket), Prefix: aws.String(prefix + "continuations/")})
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range page.Contents {
		name := aws.ToString(object.Key)[strings.LastIndex(aws.ToString(object.Key), "/")+1:]
		if generation, rest, ok := strings.Cut(name, "-"); ok && strings.HasPrefix(rest, artifacts.ContinuationKindEvents+"-") {
			segments[generation]++
		}
	}
	for generation, n := range segments {
		if n != 1 {
			t.Fatalf("generation %s has %d segment objects, want one", generation, n)
		}
	}
	if len(segments) != len(histories)-1 {
		t.Fatalf("%d generations wrote segments, %d closed", len(segments), len(histories)-1)
	}
	s.transcript.step("%d carried objects read back at their content keys; one segment per closed generation", checked)
}

// TestIntegration_ContinuationChaosC14_StorageCutDuringTheSwitchWrites:
// object storage becomes unreachable just as a switch writes its objects and
// comes back 30 s later. The run never switches while storage is cut, and
// then either switches later or fails explicitly — never succeeds without
// its objects.
func TestIntegration_ContinuationChaosC14_StorageCutDuringTheSwitchWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C14")
	proxy := s.minioProxy()
	hook := newBlockingHook(func(kind string, _ int64) bool { return kind == "events" })
	hook.onEntry = proxy.Cut
	t.Cleanup(func() { hook.open(nil) })
	s.startReplica("replica-a", replicaOptions{store: s.snapshotStoreVia(proxy), beforeWrite: hook.write})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c14-store-cut\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, "")), nil, plan)
	hook.waitEntered(t, 2*time.Minute)
	cutAt := time.Now()
	s.transcript.step("object storage cut as the switch began writing")
	hook.open(nil)
	// For the outage, the run is watched: it must not switch, and it is
	// seen doing something — its failed write recorded, or its end.
	for time.Since(cutAt) < 30*time.Second {
		if chain := s.chain(runID); len(chain) != 1 {
			t.Fatalf("the run switched (%d executions) while object storage was cut", len(chain))
		}
		time.Sleep(500 * time.Millisecond)
	}
	proxy.Restore()
	s.transcript.step("object storage restored after 30 s (%+v)", proxy.Stats())
	retried := len(s.logs.matching(logRetry, runID))
	status, _ := s.wait(runID, 3*time.Minute)
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	switch status {
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("Temporal Completed, GetRun %v", run.GetStatus())
		}
		s.requireCarriedObjects(t, runID)
		s.transcript.step("the run switched later and succeeded, every object present")
	case enums.WORKFLOW_EXECUTION_STATUS_FAILED:
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() == "" || run.GetError().GetMessage() == "" {
			t.Fatalf("Temporal Failed, GetRun %v %v: want an explicit failure", run.GetStatus(), run.GetError())
		}
		s.transcript.step("the run failed explicitly: %s %s", run.GetError().GetCode(), run.GetError().GetMessage())
	default:
		t.Fatalf("the run closed %v", status)
	}
	if retried == 0 {
		t.Fatal("the run never recorded its failed switch: the outage window saw no progress")
	}
	s.transcript.figure("outcome", status.String())
	s.transcript.figure("failed_switch_attempts", retried)
	t.Logf("C14: no switch while storage was cut (%d failed attempts recorded); then %v (%s)", retried, status, run.GetError().GetCode())
}

// TestIntegration_ContinuationChaosC21_ReplicaKilledBetweenTheWritesAndTheCommit:
// replica A's run worker is stopped while its switch's checkpoint write is
// held (a hook that ignores its context); replica B takes the workflow task
// over and completes the switch; only then is A's write released. Exactly
// one next generation exists, every object is at its content key with one
// segment per generation, no failed write from A is in history, and every
// {seq}.pb snapshot agrees with the stream.
func TestIntegration_ContinuationChaosC21_ReplicaKilledBetweenTheWritesAndTheCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C21")
	hook := newBlockingHook(func(kind string, _ int64) bool { return kind == "checkpoint" })
	t.Cleanup(func() { hook.open(nil) })
	a := s.startReplica("replica-a", replicaOptions{beforeWrite: hook.write})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c21-killed\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, "")), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	entered := hook.waitEntered(t, 2*time.Minute)
	execution, _ := s.current(runID)
	before := int64(len(s.history(runID, execution).GetEvents()))
	s.startReplica("replica-b", replicaOptions{})
	a.stop()
	if since := time.Since(entered); since > 5*time.Second {
		t.Fatalf("replica-a was stopped %s after the hook's entry, outside the 5 s window", since)
	}
	s.transcript.step("replica-a stopped with its checkpoint write held")
	deadline := time.Now().Add(time.Minute)
	for identitiesAfter(s.history(runID, execution), before)["replica-b"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("replica-b never started a workflow task of the switching execution")
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.transcript.step("replica-b started a workflow task of the switching execution; releasing replica-a's late write")
	hook.open(nil)
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	chain, histories := s.histories(runID)
	g := -1
	for i, id := range chain {
		if id == execution {
			g = i
		}
	}
	if g < 0 || g+1 >= len(chain) {
		t.Fatalf("the switching execution %s is not followed by one next generation in %v", execution, chain)
	}
	listed, err := s.control.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: fmt.Sprintf("WorkflowId = %q", runID)})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(listed.GetExecutions()); n != len(chain) {
		t.Fatalf("the cluster lists %d executions of the run, its chain names %d: a second successor exists", n, len(chain))
	}
	for _, event := range histories[g].GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "LocalActivity" || attrs.GetFailure() == nil {
			continue
		}
		t.Fatalf("the switching execution recorded a failed local activity at event %d: %v", event.GetEventId(), attrs.GetFailure().GetMessage())
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.requireCarriedObjects(t, runID)
	checked := checkSequenceSnapshots(t, ctx, s.store, runID, events)
	s.transcript.figure("failed_local_activity_markers_in_the_switching_execution", 0)
	s.transcript.figure("sequence_snapshots_checked", checked)
	t.Logf("C21: replica-b completed the switch of generation %d; one successor; no failed write of A's in history; "+
		"objects at content keys; %d {seq}.pb snapshots agree with the stream", g, checked)
}

// TestIntegration_ContinuationChaosC22_CurrentGenerationTerminated: the
// run's current generation is terminated from outside. GetRun reads
// CANCELED/RUN_TERMINATED, and the stream ends in the terminal event the
// reader synthesizes at the carried sequence plus that generation's own
// events plus one.
func TestIntegration_ContinuationChaosC22_CurrentGenerationTerminated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C22")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, awaitChainDocument("c22-terminated", 12, 3600)), nil, plan)
	s.waitChain(runID, 1, 2*time.Minute)
	// Terminate once the run is idle on its await in its latest generation.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if a, _ := s.attemptsOf("c11"); a > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the chain did not finish")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// ...and the run has nothing pending: every event of its last
	// completion published.
	for {
		described, err := s.control.DescribeWorkflowExecution(ctx, runID, "")
		if err == nil && len(described.GetPendingActivities()) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the run never went idle on its await")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := s.control.TerminateWorkflow(ctx, runID, "", "chaos C22: terminated from outside"); err != nil {
		t.Fatal(err)
	}
	status, _ := s.wait(runID, time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		t.Fatalf("the run closed %v, want Terminated", status)
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_TERMINATED" {
		t.Fatalf("GetRun = %v %v, want CANCELED RUN_TERMINATED", run.GetStatus(), run.GetError())
	}
	events, err := s.reader.Events(ctx, runID, 0)
	if err != nil {
		t.Fatal(err)
	}
	requireContiguous(t, events)
	chain, histories := s.histories(runID)
	last := histories[len(histories)-1]
	want := carryOf(t, last).GetEventSequence() + int64(len(publishedOf(t, last))) + 1
	terminal := events[len(events)-1]
	if terminal.GetSequence() != want {
		t.Fatalf("the synthesized terminal event is at %d, want the carried %d + %d published + 1 = %d", terminal.GetSequence(),
			carryOf(t, last).GetEventSequence(), len(publishedOf(t, last)), want)
	}
	s.transcript.figure("terminal_sequence", terminal.GetSequence())
	t.Logf("C22: generation %d terminated; GetRun CANCELED/RUN_TERMINATED; terminal event %v at %d", len(chain)-1,
		terminal.GetType(), terminal.GetSequence())
}

// frontendCutRun starts a chain on a replica and projection that dial
// Temporal through a proxy, cutting it when hook is entered and releasing
// the hook at once, and restoring it 30 s later.
func (s *chaosStack) frontendCutRun(t *testing.T, name string, hook *blockingHook, read bool) (string, *streamCollector, *faultproxy.Proxy) {
	t.Helper()
	proxy := s.temporalProxy()
	engineClient := s.dial("replica-a", proxy.Addr())
	o := replicaOptions{client: engineClient}
	if read {
		o.beforeRead = hook.read
	} else {
		o.beforeWrite = hook.write
	}
	hook.onEntry = proxy.Cut
	s.startReplica("replica-a", o)
	s.startProjection(engineClient)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: "+name+"\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, "")), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	hook.waitEntered(t, 2*time.Minute)
	s.transcript.step("the engine's connection to the Temporal frontend cut at the hook's entry")
	hook.open(nil)
	time.Sleep(30 * time.Second) // the outage
	proxy.Restore()
	s.transcript.step("the frontend reachable again after 30 s (%d connections refused meanwhile)", proxy.Stats().Refused)
	return runID, stream, proxy
}

// TestIntegration_ContinuationChaosC28_FrontendUnreachableDuringTheSwitch:
// the engine cannot reach the Temporal frontend from the switch's checkpoint
// write until 30 s later. The switch completes once it can: exactly one next
// generation, every object at its content key, a contiguous stream, and the
// run completes.
func TestIntegration_ContinuationChaosC28_FrontendUnreachableDuringTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C28")
	hook := newBlockingHook(func(kind string, _ int64) bool { return kind == "checkpoint" })
	t.Cleanup(func() { hook.open(nil) })
	runID, stream, proxy := s.frontendCutRun(t, "c28-frontend", hook, false)
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	listed, err := s.control.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: fmt.Sprintf("WorkflowId = %q", runID)})
	if err != nil {
		t.Fatal(err)
	}
	chain := s.chain(runID)
	if n := len(listed.GetExecutions()); n != len(chain) {
		t.Fatalf("the cluster lists %d executions of the run, its chain names %d", n, len(chain))
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.requireCarriedObjects(t, runID)
	if proxy.Stats().Refused == 0 {
		t.Fatal("no connection met the cut: the outage was not exercised")
	}
	s.transcript.figure("refused_connections", proxy.Stats().Refused)
	t.Logf("C28: the switch completed after the frontend came back; %d generations, %d events", len(chain), len(events))
}

// TestIntegration_ContinuationChaosC29_FrontendUnreachableDuringTheStart:
// the engine cannot reach the Temporal frontend from the next generation's
// first saved-state read until 30 s later. Its start completes, no
// RUN_STATE_* code is recorded, and the run completes.
func TestIntegration_ContinuationChaosC29_FrontendUnreachableDuringTheStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C29")
	hook := newBlockingHook(func(_ string, offset int64) bool { return offset == 0 })
	t.Cleanup(func() { hook.open(nil) })
	runID, stream, proxy := s.frontendCutRun(t, "c29-frontend", hook, true)
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	for _, e := range events {
		if strings.HasPrefix(e.GetMessage(), "RUN_STATE_") || strings.Contains(e.GetMessage(), "saved state") {
			t.Fatalf("event %d mentions the saved state: %q", e.GetSequence(), e.GetMessage())
		}
	}
	if proxy.Stats().Refused == 0 {
		t.Fatal("no connection met the cut: the outage was not exercised")
	}
	t.Logf("C29: the next generation's start completed after the frontend came back; %d generations, %d events",
		len(s.chain(runID)), len(events))
}

// TestIntegration_ContinuationChaosC15_LongItemsStillSwitch: a fan-out at
// max_parallel 4 whose items each run 150 s — longer than any fixed hold
// window would allow — reaches the hold line, holds until its running items
// finish, switches, and completes.
func TestIntegration_ContinuationChaosC15_LongItemsStillSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C15")
	const itemTime = 150 * time.Second
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", func(actx context.Context, s *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach() != nil {
			done := time.After(itemTime)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for waiting := true; waiting; {
				select {
				case <-done:
					waiting = false
				case <-actx.Done():
					return nil, actx.Err()
				case <-ticker.C:
					activity.RecordHeartbeat(actx)
				}
			}
		}
		return instant(actx, s, orker, in)
	})
	plan, _ := s.tcPlan()
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: c15-long-items\nversion: \"1.0.0\"\ntasks:\n" +
		"  - id: list8\n    uses: shell\n    needs: []\n    with: {op: exec}\n" +
		"  - id: convert\n    uses: shell\n    needs: [list8]\n" +
		"    each: {over: \"{{ .tasks.list8.output.parts }}\", as: part, max_items: 50, max_parallel: 4}\n" +
		"    with: {op: exec, name: \"{{ .each.part.name }}\"}\n" +
		guardChain("c", 30, "")
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, doc), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	hold := s.logs.wait(t, ctx, logQuiesce, runID, 3*time.Minute)
	s.transcript.step("hold at %v events with items running", hold.Fields["history_events"])
	status, err := s.wait(runID, 10*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	var holds []string
	longest := 0.0
	for _, e := range s.logs.matching(logSwitched, runID) {
		seconds, _ := e.Fields["hold_seconds"].(float64)
		holds = append(holds, fmt.Sprintf("%.1fs", seconds))
		longest = max(longest, seconds)
	}
	if longest < (itemTime - 30*time.Second).Seconds() {
		t.Fatalf("the switches held %v: want one that held while 150 s items finished", holds)
	}
	s.transcript.figure("hold_durations", holds)
	t.Logf("C15: held for the running 150 s items (switch holds %v); %d generations; %d events", holds, len(s.chain(runID)), len(events))
}

// pendingPeaks is the most activities one execution had scheduled and not
// yet closed: ExecuteTask alone (what the in-flight cap counts) and every
// activity (what the cluster counts).
func pendingPeaks(h *historypb.History) (tasks, all int) {
	open := map[int64]bool{}
	isTask := map[int64]bool{}
	nTasks := 0
	for _, event := range h.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			open[event.GetEventId()] = true
			if attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
				isTask[event.GetEventId()] = true
				nTasks++
			}
		}
		for _, id := range []int64{
			event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskFailedEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId(),
		} {
			if open[id] {
				delete(open, id)
				if isTask[id] {
					nTasks--
				}
			}
		}
		tasks, all = max(tasks, nTasks), max(all, len(open))
	}
	return tasks, all
}

// TestIntegration_ContinuationChaosC25_InFlightCapBoundsAWideRun: 1,200
// fan-out items without max_parallel beside 300 tasks that start together,
// under a 500 in-flight cap and every other default: pending activities
// never exceed the cap plus one pass's dispatches, no workflow task is ever
// refused for too many pending activities, and the run completes across
// generations (carrying on to the task after the wide stage).
func TestIntegration_ContinuationChaosC25_InFlightCapBoundsAWideRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C25")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	// Every task takes a few seconds, so the run's work piles up against its
	// cap instead of draining as fast as it is dispatched.
	s.startOrker("orker-1", func(actx context.Context, s *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if !strings.HasPrefix(in.GetTaskId(), "list") {
			done := time.After(40 * time.Second)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for waiting := true; waiting; {
				select {
				case <-done:
					waiting = false
				case <-ticker.C:
					activity.RecordHeartbeat(actx)
				case <-actx.Done():
					return nil, actx.Err()
				}
			}
		}
		return instant(actx, s, orker, in)
	})
	budget := engine.DefaultContinuationBudget(engine.DefaultContinuationEvents, engine.DefaultContinuationBytes,
		engine.DefaultContinuationRestoreTimeout)
	plan := s.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, &budget)
	limit := engine.DefaultInFlightLimit(500)
	plan.InFlight = &limit
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: c25-wide\nversion: \"1.0.0\"\ntasks:\n" +
		"  - id: list1200\n    uses: shell\n    needs: []\n    with: {op: exec}\n" +
		"  - id: spread\n    uses: shell\n    needs: [list1200]\n" +
		"    each: {over: \"{{ .tasks.list1200.output.parts }}\", as: part, max_items: 1000}\n" +
		"    with: {op: exec, name: \"{{ .each.part.name }}\"}\n")
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&b, "  - id: f%03d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	// Past its budget a run carries on at its next moment with nothing in
	// flight, and the wide stage has none until it ends: the task after it
	// is what the run carries on to.
	needs := []string{"spread"}
	for i := 0; i < 300; i++ {
		needs = append(needs, fmt.Sprintf("f%03d", i))
	}
	fmt.Fprintf(&b, "  - id: merge\n    uses: shell\n    needs: [%s]\n    with: {op: exec}\n", strings.Join(needs, ", "))
	wf := compile(t, b.String())
	// The DSL's item ceiling is 1,000 until a later release raises it; the
	// run is built past it here to drive the in-flight cap with more work
	// than it admits at once.
	for _, tk := range wf.GetTasks() {
		if tk.GetEach() != nil {
			tk.Each.MaxItems = 1200
		}
	}
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, wf, nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	status, err := s.wait(runID, 13*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	chain, histories := s.histories(runID)
	if len(chain) < 2 {
		t.Fatalf("%d executions: the run never carried on", len(chain))
	}
	peakTasks, peakAll := 0, 0
	for i, h := range histories {
		tasks, all := pendingPeaks(h)
		peakTasks, peakAll = max(peakTasks, tasks), max(peakAll, all)
		for _, e := range factsOf(t, h).taskFailures {
			if e.GetWorkflowTaskFailedEventAttributes().GetCause() == enums.WORKFLOW_TASK_FAILED_CAUSE_PENDING_ACTIVITIES_LIMIT_EXCEEDED {
				t.Fatalf("generation %d: a workflow task failed PENDING_ACTIVITIES_LIMIT_EXCEEDED at event %d", i, e.GetEventId())
			}
		}
	}
	run, _ := s.reader.GetRun(ctx, runID)
	items := len(taskRunOfRun(run, "spread").GetItems())
	if items != 1200 {
		t.Fatalf("the fan-out ran %d items, want 1200", items)
	}
	// The cap holds only while something is in flight, so a pass that starts
	// with nothing in flight may dispatch past it: here the first, with the
	// list and the 300 tasks.
	const firstPass = 301
	if peakTasks > int(limit.Max)+firstPass {
		t.Fatalf("%d tasks pending at once, over the %d cap plus the first pass's %d", peakTasks, limit.Max, firstPass)
	}
	if peakTasks < int(limit.Max) || len(s.logs.matching("run reached its in-flight cap", runID)) == 0 {
		t.Fatalf("%d tasks pending at most: the run never reached its %d cap, so the cap was not exercised", peakTasks, limit.Max)
	}
	s.transcript.figure("peak_pending_tasks", peakTasks)
	s.transcript.figure("peak_pending_activities", peakAll)
	s.transcript.figure("generations", len(chain))
	t.Logf("C25: 1,200 items + 300 tasks under a 500 cap: peak %d tasks and %d activities pending; no "+
		"PENDING_ACTIVITIES_LIMIT_EXCEEDED; %d generations; %d events", peakTasks, peakAll, len(chain), len(events))
}

// blobInput is a run input of n incompressible bytes, as text.
func blobInput(t *testing.T, n int) *orkstackv1.Payload {
	t.Helper()
	raw := make([]byte, n*3/4)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return payload(t, map[string]any{"blob": base64.StdEncoding.EncodeToString(raw)})
}

// generationGrowth is one closed generation's growth past its start: the
// events after its last saved-state read, and their encoded size.
func generationGrowth(t *testing.T, h *historypb.History) (events int, bytes int64) {
	t.Helper()
	base := bootstrapEvents(t, h)
	for _, event := range h.GetEvents()[base:] {
		bytes += int64(proto.Size(event))
	}
	return len(h.GetEvents()) - base, bytes
}

// TestIntegration_ContinuationChaosC26_BytesDrivenSwitch: a 40-task chain
// whose every dispatch carries a 128 KiB input switches when its history
// bytes cross the budget, its events still far under theirs; one run id, a
// contiguous stream, and the run completes.
func TestIntegration_ContinuationChaosC26_BytesDrivenSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C26")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	const budgetBytes = 2 << 20
	budget := engine.DefaultContinuationBudget(engine.DefaultContinuationEvents, budgetBytes, engine.DefaultContinuationRestoreTimeout)
	plan := s.plan(engine.DefaultHistoryMaxEvents, 10<<20, &budget)
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c26-bytes\nversion: \"1.0.0\"\ninputs:\n  blob: {type: string}\ntasks:\n"+guardChain("c", 40, "")),
		blobInput(t, 128<<10), plan)
	stream := s.openStream(s.reader, runID, 0)
	status, err := s.wait(runID, 6*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	chain, histories := s.histories(runID)
	if len(chain) < 2 {
		t.Fatalf("%d executions: the run never carried on", len(chain))
	}
	type record struct {
		Events int   `json:"growth_events"`
		Bytes  int64 `json:"growth_bytes"`
	}
	var records []record
	for i, h := range histories[:len(histories)-1] {
		n, b := generationGrowth(t, h)
		records = append(records, record{n, b})
		if b < budgetBytes*95/100 || int64(n) >= budget.Events {
			t.Fatalf("generation %d switched after %d events and %d bytes of growth: want the bytes budget (%d) crossed "+
				"with the events (%d) still under theirs", i, n, b, budgetBytes, budget.Events)
		}
	}
	proof.WriteJSON(t, s.transcript.group, "generations.json", records)
	s.transcript.figure("generations", records)
	t.Logf("C26: %d bytes-driven switches %+v; one run id; %d events contiguous", len(records), records, len(events))
}

// TestIntegration_ContinuationChaosC27_BytesDrivenGuard: with continuation
// off, a 40-task stage that starts together, each dispatch carrying a
// 128 KiB input, stops RUN_HISTORY_LIMIT on history bytes below a 4 MiB
// limit — Failed, never Terminated.
func TestIntegration_ContinuationChaosC27_BytesDrivenGuard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C27")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	const maxBytes = 4 << 20
	plan := s.plan(engine.DefaultHistoryMaxEvents, maxBytes, nil)
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: c27-guard\nversion: \"1.0.0\"\ninputs:\n  blob: {type: string}\ntasks:\n")
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&b, "  - id: w%02d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, b.String()), blobInput(t, 128<<10), plan)
	stream := s.openStream(s.reader, runID, 0)
	status, _ := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed (never Terminated)", status)
	}
	s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, engine.RunHistoryLimitCode)
	run, _ := s.reader.GetRun(ctx, runID)
	if !strings.Contains(run.GetError().GetMessage(), "MiB") {
		t.Fatalf("RUN_HISTORY_LIMIT says %q: want it to name the history's MiB", run.GetError().GetMessage())
	}
	described, err := s.control.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	size := described.GetWorkflowExecutionInfo().GetHistorySizeBytes()
	if size >= maxBytes {
		t.Fatalf("the final history is %d bytes, at or over the %d limit", size, maxBytes)
	}
	if got := failureType(t, s.history(runID, "")); got != engine.RunHistoryLimitCode {
		t.Fatalf("the execution failed with type %q", got)
	}
	s.transcript.figure("final_history_bytes", size)
	s.transcript.figure("message", run.GetError().GetMessage())
	t.Logf("C27: RUN_HISTORY_LIMIT (%q) at %d bytes of a %d-byte limit", run.GetError().GetMessage(), size, maxBytes)
}

// TestIntegration_ContinuationChaosC30_LostSegmentIsAnExplicitGap: after the
// first switch, that generation's event segment is deleted from object
// storage, and the run finishes. The run still SUCCEEDED; its live events
// fail Unavailable, never NotFound; it is archived with a gap line naming
// the segment's range; once its cluster record is retired GetRun answers
// from the archive, a read of its events over the gap fails DataLoss naming
// the range, and its evidence bundle is schema version 2 carrying the gap,
// which `orkctl run evidence verify` passes with a notice.
func TestIntegration_ContinuationChaosC30_LostSegmentIsAnExplicitGap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C30")
	g := newGates()
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	// Values inline only: the evidence bundle reads every recorded object
	// back from object storage, and this Orker writes none.
	s.startOrker("orker-1", func(actx context.Context, _ *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "c20" {
			if err := g.hold(actx, "c20"); err != nil {
				return nil, err
			}
		}
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: orker,
			ValuesJson: valuesJSON(map[string]any{"task": in.GetTaskId()}), ValuesInlined: true}, nil
	})
	a := s.serveAPI(api.Config{})
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c30-gap\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, "")), nil, plan)
	g.waitEntered(t, "c20", 2*time.Minute)
	execution, _ := s.current(runID)
	carry := carryOf(t, s.history(runID, execution))
	if carry == nil {
		t.Fatal("the run had not switched when c20 started")
	}
	segment := carry.GetSegments()[0]
	bucket, key := s3Key(t, segment.GetObject().GetUri())
	raw := newRawS3ClientForCommitE2E(t, s.cfg)
	if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
	s.transcript.step("generation 0's segment (events %d–%d) deleted", segment.GetFirstSequence(), segment.GetLastSequence())
	g.open("c20")
	status, err := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("GetRun = %v %v, want SUCCEEDED", run.GetStatus(), err)
	}

	// Live reads of the events fail Unavailable, never NotFound.
	if _, err := s.reader.Events(ctx, runID, 0); !errors.Is(err, engine.ErrEventSegmentUnavailable) {
		t.Fatalf("live Events = %v, want the segment unavailable", err)
	}
	streamCode := func() connect.Code {
		stream, err := a.runs.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID}))
		if err != nil {
			return connect.CodeOf(err)
		}
		defer stream.Close()
		for stream.Receive() {
		}
		return connect.CodeOf(stream.Err())
	}
	if code := streamCode(); code != connect.CodeUnavailable {
		t.Fatalf("the live stream ended %v, want Unavailable", code)
	}

	// The archive records the gap.
	var body []byte
	deadline := time.Now().Add(time.Minute)
	for {
		if body, err = s.archive.LoadRunBody(ctx, runID); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run was never archived: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	gaps, err := engine.UnmarshalRunArchiveGaps(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 1 || gaps[0].First != segment.GetFirstSequence() || gaps[0].Last != segment.GetLastSequence() {
		t.Fatalf("archived gaps %+v, want the one segment %d–%d", gaps, segment.GetFirstSequence(), segment.GetLastSequence())
	}
	gapLine := ""
	for _, line := range strings.Split(string(body), "\n") {
		if strings.Contains(line, `"kind":"gap"`) {
			gapLine = line
		}
	}
	s.transcript.step("archive gap line: %s", gapLine)

	// Retired from the cluster: GetRun from the archive, events over the gap
	// DataLoss naming the range. Every execution of the chain — it carried on
	// again after the segment was deleted — reads NotFound first.
	executions := s.chain(runID)
	if err := s.reader.DeleteWorkflow(ctx, s.namespace, runID); err != nil {
		t.Fatal(err)
	}
	for _, id := range executions {
		s.waitExecutionGone(t, runID, id, time.Minute)
	}
	got, err := a.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil || got.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("GetRun from the archive = %v %v", got, err)
	}
	stream, err := a.runs.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID}))
	if err != nil {
		t.Fatal(err)
	}
	for stream.Receive() {
	}
	streamErr := stream.Err()
	stream.Close()
	rangeText := fmt.Sprintf("events %d–%d", segment.GetFirstSequence(), segment.GetLastSequence())
	if connect.CodeOf(streamErr) != connect.CodeDataLoss || !strings.Contains(streamErr.Error(), rangeText) {
		t.Fatalf("archived events over the gap = %v, want DataLoss naming %s", streamErr, rangeText)
	}
	s.transcript.step("archived events: %v", streamErr)

	// The evidence bundle carries the gap, and the CLI verifies it.
	export, err := a.runs.ExportRunEvidence(ctx, connect.NewRequest(&orkstackv1.ExportRunEvidenceRequest{RunId: runID}))
	if err != nil {
		t.Fatal(err)
	}
	var bundle bytes.Buffer
	for export.Receive() {
		bundle.Write(export.Msg().GetData())
	}
	if err := export.Err(); err != nil {
		t.Fatalf("export the evidence bundle: %v", err)
	}
	report, err := evidence.Verify(bytes.NewReader(bundle.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK() || report.Manifest.SchemaVersion != evidence.SchemaVersionEventGaps || len(report.EventGaps) != 1 ||
		report.EventGaps[0].First != segment.GetFirstSequence() || report.EventGaps[0].Last != segment.GetLastSequence() {
		t.Fatalf("the bundle verifies %t, schema %d, gaps %+v (failures %+v)", report.OK(), report.Manifest.SchemaVersion,
			report.EventGaps, report.Failures)
	}
	path := filepath.Join(t.TempDir(), "evidence.tar.gz")
	if err := os.WriteFile(path, bundle.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, testbin.Path(t, "./cmd/orkctl"), "run", "evidence", "verify", path)
	cmd.Env = append(os.Environ(), "ORK_SERVER=", "ORK_NAMESPACE=", "ORK_CONFIG="+filepath.Join(t.TempDir(), "config.json"))
	out, err := cmd.CombinedOutput()
	notice := rangeText + " of this run were lost from object storage before it was archived"
	if err != nil || !strings.Contains(string(out), notice) {
		t.Fatalf("orkctl run evidence verify: %v\n%s\nwant it to pass with the notice %q", err, out, notice)
	}
	s.transcript.step("orkctl run evidence verify:\n%s", out)
	s.transcript.figure("gap_line", gapLine)
	s.transcript.figure("event_gaps", report.EventGaps)
	t.Logf("C30: a lost segment (%s): run SUCCEEDED; live events Unavailable; archive gap line; DataLoss after retirement; "+
		"bundle schema %d with the gap; orkctl verify passes with the notice", rangeText, report.Manifest.SchemaVersion)
}

// splitStore reads saved state through one store and does everything else
// through another: C31's outage reaches the next generation's saved-state
// reads only, so its terminal event can still be written.
type splitStore struct {
	engine.SnapshotStore
	reads engine.SnapshotStore
}

func (s splitStore) ReadContinuationRange(ctx context.Context, uri string, wantSize, offset, length int64) ([]byte, error) {
	return s.reads.ReadContinuationRange(ctx, uri, wantSize, offset, length)
}

// TestIntegration_ContinuationChaosC31_RestoreRetriesStayWithinTheirBound:
// after a switch, object storage is unreachable for the next generation's
// reads for its whole 5-minute restore timeout — every attempt blocking its
// full 30 s for the first half, failing at once for the second. The run ends
// RUN_STATE_UNAVAILABLE with a terminal event, and that generation's history
// stays within RestoreHistoryBound of its plan and below the guard.
func TestIntegration_ContinuationChaosC31_RestoreRetriesStayWithinTheirBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C31")
	proxy := s.minioProxy()
	var (
		mu        sync.Mutex
		attempts  []time.Time
		firstRead time.Time
	)
	beforeRead := func(_ context.Context, _ int64) error {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, time.Now())
		if firstRead.IsZero() {
			firstRead = time.Now()
			proxy.Stall()
		}
		return nil
	}
	_, budget := s.tcPlan()
	s.startReplica("replica-a", replicaOptions{beforeRead: beforeRead,
		store: splitStore{SnapshotStore: s.store, reads: s.snapshotStoreVia(proxy)}})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan := s.plan(1000, engine.DefaultHistoryMaxBytes, &budget)
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: c31-restore\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, "")), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	window := time.Duration(budget.Restore.TimeoutSeconds) * time.Second
	deadline := time.Now().Add(3 * time.Minute)
	for {
		mu.Lock()
		started := firstRead
		mu.Unlock()
		if !started.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the next generation never read its saved state")
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.transcript.step("storage stalled for the next generation's reads: attempts block their full 30 s")
	time.Sleep(window / 2)
	proxy.Cut()
	s.transcript.step("storage cut for the second half of the window: attempts fail at once")
	status, _ := s.wait(runID, window)
	proxy.Restore()
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed", status)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, engine.RunStateUnavailableCode)
	if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("the last event is %v, want a published RUN_FAILED", last.GetType())
	}
	_, histories := s.histories(runID)
	last := histories[len(histories)-1]
	bound := engine.RestoreHistoryBound(budget)
	growth := int64(len(last.GetEvents()))
	if growth > bound {
		t.Fatalf("the failed generation's history holds %d events, over RestoreHistoryBound %d", growth, bound)
	}
	guard := plan.History.MaxEvents - plan.History.Reserve.Fixed
	if growth >= guard {
		t.Fatalf("the failed generation's history (%d events) reached the guard's threshold (%d)", growth, guard)
	}
	heartbeats, timers := 0, 0
	for _, event := range last.GetEvents() {
		switch event.GetEventType() {
		case enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			heartbeats++
		case enums.EVENT_TYPE_TIMER_STARTED:
			timers++
		}
	}
	mu.Lock()
	n := len(attempts)
	mu.Unlock()
	s.transcript.figure("read_attempts", n)
	s.transcript.figure("workflow_task_completions", heartbeats)
	s.transcript.figure("server_timers", timers)
	s.transcript.figure("bootstrap_events", growth)
	s.transcript.figure("restore_history_bound", bound)
	s.transcript.figure("proxy", proxy.Stats())
	t.Logf("C31: RUN_STATE_UNAVAILABLE with a terminal event after %d read attempts; the generation's %d events (%d workflow "+
		"task completions, %d server timers) ≤ RestoreHistoryBound %d, below the guard's %d", n, growth, heartbeats, timers, bound, guard)
}

// waitPending waits until runID's current execution has exactly n
// activities pending.
func (s *chaosStack) waitPending(t *testing.T, runID string, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		described, err := s.control.DescribeWorkflowExecution(s.ctx, runID, "")
		if err == nil && len(described.GetPendingActivities()) == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never had exactly %d activities pending", runID, n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// pendingPublish is one PublishEvent scheduled and not yet closed.
type pendingPublish struct {
	event       *orkstackv1.Event
	snapshotURI string
}

// pendingPublishes is every publish runID's current execution has
// scheduled and not closed, decoded.
func (s *chaosStack) pendingPublishes(t *testing.T, runID string) []pendingPublish {
	t.Helper()
	execution, _ := s.current(runID)
	open := map[int64]pendingPublish{}
	var order []int64
	for _, event := range s.history(runID, execution).GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityType().GetName() == engine.ActivityPublishEvent {
			var published *orkstackv1.Event
			var uri string
			payloads := attrs.GetInput().GetPayloads()
			if err := converter.GetDefaultDataConverter().FromPayload(payloads[0], &published); err != nil {
				t.Fatal(err)
			}
			if err := converter.GetDefaultDataConverter().FromPayload(payloads[1], &uri); err != nil {
				t.Fatal(err)
			}
			open[event.GetEventId()] = pendingPublish{event: published, snapshotURI: uri}
			order = append(order, event.GetEventId())
		}
		for _, id := range []int64{
			event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskFailedEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId(),
		} {
			delete(open, id)
		}
	}
	var out []pendingPublish
	for _, id := range order {
		if p, ok := open[id]; ok {
			out = append(out, p)
		}
	}
	return out
}

// TestIntegration_ContinuationChaosC23_ProjectionDownDuringTheSwitchDrain:
// the projection worker is stopped while a switch waits for its run's
// detached item publishes, then restarted. The switch waits for them, then
// completes, and the stream and archive are contiguous.
func TestIntegration_ContinuationChaosC23_ProjectionDownDuringTheSwitchDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C23")
	g := newGates()
	var segmentWrites atomic.Int64
	s.startReplica("replica-a", replicaOptions{beforeWrite: func(_ context.Context, kind string) error {
		if kind == artifacts.ContinuationKindEvents {
			segmentWrites.Add(1)
		}
		return nil
	}})
	projection := s.startProjection(nil)
	// The fan-out's first two items are the ones in flight when the run
	// holds; their completions are released one at a time.
	s.startOrker("orker-1", gated(g, "convert[0]", "convert[1]"))
	plan, _ := s.tcPlan()
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: c23-drain\nversion: \"1.0.0\"\ntasks:\n" +
		"  - id: list100\n    uses: shell\n    needs: []\n    with: {op: exec}\n" +
		"  - id: convert\n    uses: shell\n    needs: [list100]\n" +
		"    each: {over: \"{{ .tasks.list100.output.parts }}\", as: part, max_items: 100, max_parallel: 2}\n" +
		"    with: {op: exec, name: \"{{ .each.part.name }}\"}\n" +
		guardChain("c", 30, "")
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, doc), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	s.logs.wait(t, ctx, logQuiesce, runID, 2*time.Minute)
	g.waitEntered(t, "convert[0]", time.Minute)
	g.waitEntered(t, "convert[1]", time.Minute)
	// Item 0 completes while the projection is up: its events publish.
	g.open("convert[0]")
	s.waitPending(t, runID, 1, time.Minute)
	if chain := s.chain(runID); len(chain) != 1 {
		t.Fatal("the run switched with an item in flight")
	}
	projection.Stop()
	s.transcript.step("projection worker stopped; releasing the last item in flight")
	g.open("convert[1]")
	// The run reaches its idle point and its switch waits on the detached
	// publish of the last item's event: the item's completion lands, its
	// publish stays pending, and no segment is written.
	execution, _ := s.current(runID)
	deadline := time.Now().Add(30 * time.Second)
	for len(factsOf(t, s.history(runID, execution)).completedAt["convert[1]"]) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the last item's completion never landed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	stallFrom := time.Now()
	for time.Since(stallFrom) < 10*time.Second {
		if chain := s.chain(runID); len(chain) != 1 || segmentWrites.Load() != 0 {
			t.Fatalf("the switch went ahead (%d executions, %d segment writes) with the projection down", len(chain), segmentWrites.Load())
		}
		time.Sleep(250 * time.Millisecond)
	}
	pending := s.pendingPublishes(t, runID)
	if len(pending) == 0 {
		t.Fatal("no publish is pending: the switch was not waiting on the projection")
	}
	for _, p := range pending {
		if p.event.Item == nil || p.snapshotURI != "" {
			t.Fatalf("a pending publish (event %d, item %v, snapshot %q) is not a detached item publish", p.event.GetSequence(),
				p.event.Item, p.snapshotURI)
		}
	}
	s.transcript.step("for 10 s the switch waited on %d detached item publish(es); no segment written", len(pending))
	restartedAt := time.Now()
	s.startProjection(nil)
	status, err := s.wait(runID, 4*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	_, histories := s.histories(runID)
	switchedAt := time.Time{}
	for _, event := range histories[0].GetEvents() {
		if event.GetEventType() == enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
			switchedAt = event.GetEventTime().AsTime()
		}
	}
	if switchedAt.IsZero() || switchedAt.Before(restartedAt) {
		t.Fatalf("generation 0 switched at %v, before the projection came back at %v", switchedAt, restartedAt)
	}
	s.transcript.figure("pending_detached_publishes", len(pending))
	s.transcript.figure("switched_after_restart", switchedAt.Sub(restartedAt).String())
	t.Logf("C23: the switch waited on %d detached publish(es) with the projection down, then completed %s after it came back; "+
		"%d events contiguous in stream and archive", len(pending), switchedAt.Sub(restartedAt), len(events))
}

// c24Run starts a chain of n tasks under a continuation budget whose bytes
// (and saved-state cap) is capBytes, each dispatch carrying input.
func (s *chaosStack) c24Run(t *testing.T, name string, n int, capBytes int64, input *orkstackv1.Payload) string {
	t.Helper()
	budget := engine.DefaultContinuationBudget(120, capBytes, engine.DefaultContinuationRestoreTimeout)
	plan := s.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, &budget)
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: "+name+"\nversion: \"1.0.0\"\ninputs:\n  blob: {type: string}\ntasks:\n"+guardChain("c", n, "")),
		input, plan)
	return runID
}

// markersPerWorkflowTask is the most saved-state reads one workflow task of
// h recorded.
func markersPerWorkflowTask(t *testing.T, h *historypb.History) int {
	t.Helper()
	most, current := 0, 0
	for _, event := range h.GetEvents() {
		if event.GetEventType() == enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			current = 0
		}
		if attrs := event.GetMarkerRecordedEventAttributes(); attrs != nil && attrs.GetMarkerName() == "LocalActivity" {
			var data struct{ ActivityType string }
			_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data)
			if data.ActivityType == engine.ActivityReadContinuationChunk {
				current++
				most = max(most, current)
			}
		}
	}
	return most
}

// TestIntegration_ContinuationChaosC24_SavedStateAtItsCap: a run whose saved
// state is carried by its 128 KiB input is measured, then run with
// continuation.bytes (its saved-state cap) just above and just below that
// size. Just above, it switches, its next generation reads the state back in
// batches of at most 8 reads per workflow task, and it completes. Just
// below, it never switches, and ends RUN_HISTORY_LIMIT naming the size of
// its saved state.
func TestIntegration_ContinuationChaosC24_SavedStateAtItsCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C24")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", func(actx context.Context, _ *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: orker,
			ValuesJson: valuesJSON(map[string]any{"ok": true}), ValuesInlined: true}, nil
	})
	input := blobInput(t, 128<<10)

	// Measure: the first switch's saved state, at the point a cap near it
	// would switch (two dispatches of the input past the start).
	calibration := s.c24Run(t, "c24-measure", 6, 192<<10, input)
	if status, err := s.wait(calibration, 4*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the measuring run closed %v: %v", status, err)
	}
	_, measured := s.histories(calibration)
	if len(measured) < 2 {
		t.Fatal("the measuring run never switched")
	}
	size := carryOf(t, measured[1]).GetCheckpoint().GetSizeBytes()
	if size < 16<<10 {
		t.Fatalf("the measured saved state is %d bytes, under continuation.bytes' 16 KiB floor", size)
	}
	const margin = 2 << 10
	s.transcript.figure("measured_saved_state_bytes", size)

	// Just above: it switches and completes.
	under := s.c24Run(t, "c24-under", 40, size+margin, input)
	if status, err := s.wait(under, 6*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run under its cap closed %v: %v", status, err)
	}
	chain, histories := s.histories(under)
	if len(chain) < 2 {
		t.Fatalf("the run with its cap %d bytes above its saved state never switched", margin)
	}
	most := 0
	for i := 1; i < len(histories); i++ {
		if got := carryOf(t, histories[i]).GetCheckpoint().GetSizeBytes(); got > size+margin {
			t.Fatalf("generation %d carried %d bytes, over its cap %d", i, got, size+margin)
		}
		most = max(most, markersPerWorkflowTask(t, histories[i]))
	}
	if most < 1 || most > int(engine.DefaultContinuationChunksPerTask) {
		t.Fatalf("a workflow task read %d chunks of saved state, want 1 to %d", most, engine.DefaultContinuationChunksPerTask)
	}
	s.requireOneRunRead(under, nil, orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.transcript.step("cap %d (saved state %d + %d): %d generations, at most %d reads per workflow task", size+margin, size,
		margin, len(chain), most)

	// Just below: it never switches, and the guard ends it naming the size.
	over := s.c24Run(t, "c24-over", 400, size-margin, input)
	status, _ := s.wait(over, 12*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run over its cap closed %v, want Failed", status)
	}
	if chain := s.chain(over); len(chain) != 1 {
		t.Fatalf("the run with its cap %d bytes below its saved state switched (%d executions)", margin, len(chain))
	}
	s.requireOneRunRead(over, nil, orkstackv1.Status_STATUS_FAILED, engine.RunHistoryLimitCode)
	run, _ := s.reader.GetRun(ctx, over)
	if !strings.Contains(run.GetError().GetMessage(), "its saved state is") ||
		!strings.Contains(run.GetError().GetMessage(), "more than the") {
		t.Fatalf("RUN_HISTORY_LIMIT says %q: want the size of its saved state", run.GetError().GetMessage())
	}
	if got := failureType(t, s.history(over, "")); got != engine.RunHistoryLimitCode {
		t.Fatalf("the execution failed with type %q", got)
	}
	s.transcript.figure("over_message", run.GetError().GetMessage())
	t.Logf("C24: saved state %d bytes; cap +%d switched (%d generations, ≤ %d reads per task); cap −%d never switched: %q",
		size, margin, len(chain), most, margin, run.GetError().GetMessage())
}
