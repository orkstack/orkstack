//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The task-timing-breakdown integration scenarios (I1, I2, I4, I5, I6): real
// `ork` and `orker` binaries, a real Temporal cluster and the MinIO bucket, and
// `orkctl run timing` read back through the built CLI. Each test owns a
// private Temporal namespace and a private Orker channel (AGENTS.md §11.1).

// timingStack is one topology: N engine replicas sharing a namespace, a queue
// and the bucket, and (once started) one Orker.
type timingStack struct {
	ctx         context.Context
	t           *testing.T
	address     string
	namespace   string
	queue       string
	temporal    client.Client
	root        string
	orkctlPath  string
	orkerPath   string
	replicas    []*engineReplica
	artifactCfg artifacts.Config
	transcript  strings.Builder
}

func newTimingStack(ctx context.Context, t *testing.T, replicas int) *timingStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	s := &timingStack{
		ctx:         ctx,
		t:           t,
		address:     address,
		namespace:   isolatedNamespace(),
		queue:       fmt.Sprintf("ork-timing-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000),
		root:        repoRoot(t),
		artifactCfg: artifactCfg,
	}
	t.Logf("temporal namespace %s, orker channel %s", s.namespace, s.queue)
	s.temporal = dialIsolatedNamespace(ctx, t, address, s.namespace)
	binDir := t.TempDir()
	s.orkctlPath = buildBinary(t, s.root, binDir, "orkctl")
	enginePath := buildBinary(t, s.root, binDir, "ork")
	s.orkerPath = buildBinary(t, s.root, binDir, "orker")
	for i := 0; i < replicas; i++ {
		replica := startReplica(ctx, t, fmt.Sprintf("ork-%c", 'A'+i), enginePath, address, s.namespace, s.queue)
		replica.awaitReady(ctx, t)
		s.replicas = append(s.replicas, replica)
	}
	return s
}

func (s *timingStack) startOrker() {
	s.t.Helper()
	startIdempotencyOrker(s.ctx, s.t, s.temporal, s.orkerPath, s.address, s.namespace, s.queue)
}

// cli runs the built orkctl against one replica and records the exchange.
func (s *timingStack) cli(replica int, args ...string) (string, string, int) {
	s.t.Helper()
	full := append(append([]string(nil), args...), "--server", "http://"+s.replicas[replica].addr)
	stdout, stderr, code := runBinary(s.ctx, s.t, s.orkctlPath, s.root, full...)
	fmt.Fprintf(&s.transcript, "$ orkctl %s\n%s", strings.Join(args, " "), stdout)
	if stderr != "" {
		fmt.Fprintf(&s.transcript, "(stderr) %s", stderr)
	}
	fmt.Fprintf(&s.transcript, "(exit %d)\n\n", code)
	return stdout, stderr, code
}

func (s *timingStack) mustCLI(replica int, args ...string) string {
	s.t.Helper()
	stdout, stderr, code := s.cli(replica, args...)
	if code != 0 {
		s.t.Fatalf("orkctl %s exit = %d\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), code, stdout, stderr)
	}
	return stdout
}

func (s *timingStack) apply(yaml string) {
	s.t.Helper()
	if _, err := s.replicas[0].flows.ApplyOrkflow(s.ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: yaml,
	})); err != nil {
		s.t.Fatalf("ApplyOrkflow: %v", err)
	}
}

// submit starts a run through the CLI and returns its id.
func (s *timingStack) submit(name string, extra ...string) string {
	s.t.Helper()
	stdout := s.mustCLI(0, append([]string{"run", "submit", name}, extra...)...)
	return parseRunID(s.t, stdout)
}

func (s *timingStack) writeTranscript(name string) {
	proof.Write(s.t, "orkctl", name, []byte(s.transcript.String()))
}

// timingDocument is the subset of `run timing --json` these tests read.
type timingDocument struct {
	RunID                   string  `json:"runId"`
	AttemptRecordsAvailable bool    `json:"attemptRecordsAvailable"`
	WaitSumSeconds          float64 `json:"waitSumSeconds"`
	Tasks                   []struct {
		TaskID           string   `json:"taskId"`
		Status           string   `json:"status"`
		Attempt          int      `json:"attempt"`
		ScheduledAt      string   `json:"scheduledAt"`
		StartedAt        string   `json:"startedAt"`
		FinishedAt       string   `json:"finishedAt"`
		WaitSeconds      *float64 `json:"waitSeconds"`
		WaitReason       string   `json:"waitReason"`
		RunSeconds       *float64 `json:"runSeconds"`
		TotalSeconds     *float64 `json:"totalSeconds"`
		QueueWaitSeconds *float64 `json:"queueWaitSeconds"`
		QueueWaitReason  string   `json:"queueWaitReason"`
		Attempts         []struct {
			Attempt      int    `json:"attempt"`
			Outcome      string `json:"outcome"`
			StartedAt    string `json:"startedAt"`
			FinishedAt   string `json:"finishedAt"`
			PhasesReason string `json:"phasesReason"`
			Phases       []struct {
				Phase   string   `json:"phase"`
				Seconds *float64 `json:"seconds"`
			} `json:"phases"`
		} `json:"attempts"`
	} `json:"tasks"`
}

func decodeTiming(t *testing.T, stdout string) *timingDocument {
	t.Helper()
	var doc timingDocument
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("run timing --json is not JSON: %v\n%s", err, stdout)
	}
	return &doc
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

// awaitTaskScheduled blocks on the run's live event stream until
// TASK_SCHEDULED for taskID arrives, and returns that event.
func awaitTaskScheduled(ctx context.Context, t *testing.T, replica *engineReplica, runID, taskID string) *orkstackv1.Event {
	t.Helper()
	streamCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	stream, err := replica.runs.StreamEvents(streamCtx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()
	for stream.Receive() {
		event := stream.Msg()
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED && event.GetTaskId() == taskID {
			return event
		}
	}
	t.Fatalf("no TASK_SCHEDULED for %q on run %s: %v", taskID, runID, stream.Err())
	return nil
}

func twoTaskTimingOrkflow(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: task timing probe — queue wait, then run.
tasks:
  - id: first
    uses: shell
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo first"]
  - id: second
    uses: shell
    needs: [first]
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo second"]
`, name)
}

// TestIntegration_QueueWaitIsVisibleWhenNoOrkerIsPolling is I1: a run
// submitted while no Orker polls its pool waits, and `run timing` says so —
// WAIT is the wait, RUN is the container, and every number is one a process
// actually measured.
func TestIntegration_QueueWaitIsVisibleWhenNoOrkerIsPolling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	defer s.writeTranscript("run-timing-queue-wait.txt")

	name := fmt.Sprintf("timing-wait-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(twoTaskTimingOrkflow(name))
	runID := s.submit(name, "--allow-empty-pool")

	scheduled := awaitTaskScheduled(ctx, t, s.replicas[0], runID, "first")
	t.Logf("TASK_SCHEDULED(first) at %s; no Orker is polling", scheduled.GetTimestamp().AsTime().Format(time.RFC3339Nano))
	// The stimulus, not synchronization: the five seconds IS what is measured.
	stimulus := time.NewTimer(5 * time.Second)
	<-stimulus.C
	s.startOrker()
	awaitTerminalOnB(ctx, t, s.replicas[0], runID)

	s.mustCLI(0, "run", "timing", runID)
	doc := decodeTiming(t, s.mustCLI(0, "--json", "run", "timing", runID))
	if len(doc.Tasks) != 2 {
		t.Fatalf("timing lists %d tasks, want 2", len(doc.Tasks))
	}
	first := doc.Tasks[0]
	if first.WaitSeconds == nil || first.RunSeconds == nil {
		t.Fatalf("first task wait=%v run=%v, want both measured (%s)", first.WaitSeconds, first.RunSeconds, first.WaitReason)
	}
	t.Logf("I1 first: waitSeconds=%.3f runSeconds=%.3f", *first.WaitSeconds, *first.RunSeconds)
	if *first.WaitSeconds < 5 {
		t.Errorf("waitSeconds = %.3f, want >= 5: the task waited at least the stimulus", *first.WaitSeconds)
	}
	if *first.RunSeconds >= *first.WaitSeconds {
		t.Errorf("runSeconds = %.3f >= waitSeconds = %.3f; the container is the short part here",
			*first.RunSeconds, *first.WaitSeconds)
	}

	// scheduledAt IS the TASK_SCHEDULED event's timestamp, as StreamEvents
	// replays it for the finished run.
	replayed := newEventCollector()
	if err := collectStream(ctx, s.replicas[0], runID, 0, replayed); err != nil {
		t.Fatalf("replay the finished run's events: %v", err)
	}
	var replayedScheduled *orkstackv1.Event
	for _, event := range replayed.events() {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED && event.GetTaskId() == "first" {
			replayedScheduled = event
		}
	}
	if replayedScheduled == nil {
		t.Fatal("the replayed stream has no TASK_SCHEDULED for first")
	}
	if got, want := mustTime(t, first.ScheduledAt), replayedScheduled.GetTimestamp().AsTime(); !got.Equal(want) {
		t.Errorf("scheduledAt = %s, TASK_SCHEDULED timestamp = %s", got, want)
	}
	t.Logf("I1 scheduledAt=%s TASK_SCHEDULED.timestamp=%s", first.ScheduledAt,
		replayedScheduled.GetTimestamp().AsTime().Format(time.RFC3339Nano))

	// startedAt IS attempt 1's start, as `run attempts --json` reports it.
	var attempts struct {
		Attempts []struct {
			TaskID    string `json:"taskId"`
			Attempt   int    `json:"attempt"`
			StartedAt string `json:"startedAt"`
		} `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(s.mustCLI(0, "--json", "run", "attempts", runID)), &attempts); err != nil {
		t.Fatalf("run attempts --json: %v", err)
	}
	for _, attempt := range attempts.Attempts {
		if attempt.TaskID == "first" && attempt.Attempt == 1 {
			if !mustTime(t, attempt.StartedAt).Equal(mustTime(t, first.StartedAt)) {
				t.Errorf("snapshot startedAt = %s, attempt 1 startedAt = %s", first.StartedAt, attempt.StartedAt)
			}
		}
	}

	// Every task: 11 phases, summing to the attempt's own duration.
	for _, task := range doc.Tasks {
		if len(task.Attempts) != 1 {
			t.Fatalf("%s has %d attempts, want 1", task.TaskID, len(task.Attempts))
		}
		attempt := task.Attempts[0]
		if len(attempt.Phases) != 11 {
			t.Errorf("%s attempt 1 has %d phases, want 11 (%s)", task.TaskID, len(attempt.Phases), attempt.PhasesReason)
			continue
		}
		var sum float64
		for _, phase := range attempt.Phases {
			if phase.Seconds == nil {
				t.Fatalf("%s phase %s has no duration", task.TaskID, phase.Phase)
			}
			sum += *phase.Seconds
		}
		whole := mustTime(t, attempt.FinishedAt).Sub(mustTime(t, attempt.StartedAt)).Seconds()
		if math.Abs(whole-sum) > 0.005 {
			t.Errorf("%s: phases sum to %.6fs, the attempt took %.6fs (> 5ms apart)", task.TaskID, sum, whole)
		}
		t.Logf("I1 %s: 11 phases summing to %.6fs of %.6fs", task.TaskID, sum, whole)
	}
}

// TestIntegration_RetriedTaskTimingSplitsAttempts is I2: a task that fails
// once and succeeds on attempt 2 — the deciding attempt's start is on the
// snapshot, and the queue wait comes from attempt 1's record.
func TestIntegration_RetriedTaskTimingSplitsAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// The flaky origin: the FIRST request to /flaky has its connection
	// destroyed (activity-http fails the attempt), every later one is a 200.
	// No clock and no race decides which attempt fails.
	var flakyHits atomic.Int32
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	origin := &httptest.Server{Listener: listener, Config: &http.Server{Handler: http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/flaky" && flakyHits.Add(1) == 1 {
				if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
					_ = conn.Close()
				}
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":true}`)
		})}}
	origin.Start()
	defer origin.Close()
	base := fmt.Sprintf("http://host.docker.internal:%d", listener.Addr().(*net.TCPAddr).Port)
	requireContainerCanReach(ctx, t, base+"/probe")

	s := newTimingStack(ctx, t, 1)
	defer s.writeTranscript("run-timing-retried.txt")
	s.startOrker()

	name := fmt.Sprintf("timing-flaky-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(fmt.Sprintf(`name: %s
version: "0.1.0"
description: task timing probe — a task that fails once.
tasks:
  - id: flaky
    uses: http
    timeout_seconds: 120
    retry:
      maximum_attempts: 2
      initial_interval_seconds: 1
    with:
      op: get
      url: "%s/flaky"
`, name, base))
	runID := s.submit(name)
	finished := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
	if finished.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s = %s, want SUCCEEDED on attempt 2", runID, finished.GetStatus())
	}

	s.mustCLI(0, "run", "timing", runID, "--task", "flaky")
	doc := decodeTiming(t, s.mustCLI(0, "--json", "run", "timing", runID))
	flaky := doc.Tasks[0]
	if flaky.Attempt != 2 || len(flaky.Attempts) != 2 {
		t.Fatalf("flaky attempt=%d with %d attempt rows, want 2 and 2", flaky.Attempt, len(flaky.Attempts))
	}
	second, first := flaky.Attempts[0], flaky.Attempts[1]
	lastPhase := func(phases []struct {
		Phase   string   `json:"phase"`
		Seconds *float64 `json:"seconds"`
	}) string {
		if len(phases) == 0 {
			return ""
		}
		return phases[len(phases)-1].Phase
	}
	if first.Attempt != 1 || lastPhase(first.Phases) != "exit" {
		t.Errorf("attempt 1 ends in %q, want exit", lastPhase(first.Phases))
	}
	if second.Attempt != 2 || lastPhase(second.Phases) != "commit" {
		t.Errorf("attempt 2 ends in %q, want commit", lastPhase(second.Phases))
	}
	t.Logf("I2 last spans: attempt 1=%s attempt 2=%s", lastPhase(first.Phases), lastPhase(second.Phases))
	if !mustTime(t, flaky.StartedAt).Equal(mustTime(t, second.StartedAt)) {
		t.Errorf("snapshot startedAt = %s, want attempt 2's %s", flaky.StartedAt, second.StartedAt)
	}
	if flaky.QueueWaitSeconds == nil || flaky.WaitSeconds == nil {
		t.Fatalf("queueWait=%v (%s) wait=%v, want both measured", flaky.QueueWaitSeconds, flaky.QueueWaitReason, flaky.WaitSeconds)
	}
	want := mustTime(t, first.StartedAt).Sub(mustTime(t, flaky.ScheduledAt)).Seconds()
	if math.Abs(*flaky.QueueWaitSeconds-want) > 1e-6 {
		t.Errorf("queueWaitSeconds = %.6f, want attempt 1's start - scheduledAt = %.6f", *flaky.QueueWaitSeconds, want)
	}
	if *flaky.WaitSeconds <= *flaky.QueueWaitSeconds {
		t.Errorf("waitSeconds = %.3f <= queueWaitSeconds = %.3f; the retry is inside WAIT", *flaky.WaitSeconds, *flaky.QueueWaitSeconds)
	}
	t.Logf("I2 waitSeconds=%.3f queueWaitSeconds=%.3f", *flaky.WaitSeconds, *flaky.QueueWaitSeconds)
}

// TestIntegration_AFailedTaskCarriesItsStart is I4: a task whose container
// exits non-zero is FAILED with the Orker's start on its snapshot, carried on
// ActivityFailureDetail.started_at — so its wait is a number.
func TestIntegration_AFailedTaskCarriesItsStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	defer s.writeTranscript("run-timing-failed.txt")
	s.startOrker()

	name := fmt.Sprintf("timing-fail-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(fmt.Sprintf(`name: %s
version: "0.1.0"
description: task timing probe — a task that fails.
tasks:
  - id: doomed
    uses: shell
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo failing; exit 3"]
`, name))
	runID := s.submit(name)
	finished := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
	if finished.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run %s = %s, want FAILED", runID, finished.GetStatus())
	}
	doomed := finished.GetTaskRuns()[0]
	if doomed.GetStartedAt() == nil {
		t.Fatal("the FAILED task has no started_at; the failure detail did not carry the Orker's start")
	}

	s.mustCLI(0, "run", "timing", runID)
	doc := decodeTiming(t, s.mustCLI(0, "--json", "run", "timing", runID))
	task := doc.Tasks[0]
	if task.Status != "FAILED" || task.StartedAt == "" || task.WaitSeconds == nil || task.RunSeconds == nil {
		t.Fatalf("doomed status=%s startedAt=%q wait=%v run=%v (%s)", task.Status, task.StartedAt,
			task.WaitSeconds, task.RunSeconds, task.WaitReason)
	}
	t.Logf("I4 FAILED doomed: startedAt=%s waitSeconds=%.3f runSeconds=%.3f", task.StartedAt, *task.WaitSeconds, *task.RunSeconds)
}

// TestIntegration_ReclaimedAttemptRecordsRenderUnavailable is I5: after the
// cleanup pass reclaims a finished run's storage — its attempt records and
// logs deleted, its execution retired so reads come from the archive — the
// snapshot still gives wait and run, and the attempts block says the records
// were reclaimed, never 0s.
func TestIntegration_ReclaimedAttemptRecordsRenderUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	defer s.writeTranscript("run-timing-reclaimed.txt")
	s.startOrker()

	name := fmt.Sprintf("timing-reclaim-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(fmt.Sprintf(`name: %s
version: "0.1.0"
description: task timing probe — records reclaimed.
tasks:
  - id: only
    uses: shell
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo only"]
`, name))
	runID := s.submit(name)
	finished := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
	awaitArchivedStream(ctx, t, s.artifactCfg, runID)
	before := s.mustCLI(0, "run", "timing", runID)
	if !strings.Contains(before, "only  attempt 1  SUCCEEDED") {
		t.Fatalf("before cleanup the attempt record is not there:\n%s", before)
	}

	// The cleanup pass, as internal/archive.Cleaner performs it for one run:
	// reclaim the run-scoped objects, then retire the execution.
	archive, err := artifacts.NewRunArchive(s.artifactCfg)
	if err != nil {
		t.Fatalf("open run archive: %v", err)
	}
	namespace := finished.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}
	reclaimed, err := archive.DeleteRunArtifacts(ctx, namespace, runID)
	if err != nil {
		t.Fatalf("DeleteRunArtifacts: %v", err)
	}
	t.Logf("reclaimed %d objects (%d bytes) of run %s", reclaimed.ObjectsDeleted, reclaimed.BytesReclaimed, runID)
	if _, err := s.temporal.WorkflowService().DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runID},
	}); err != nil {
		t.Fatalf("retire the execution: %v", err)
	}

	var after string
	deadline := time.Now().Add(90 * time.Second)
	for {
		stdout, _, code := s.cli(0, "run", "timing", runID)
		if code == 0 && strings.Contains(stdout, reasonReclaimed) {
			after = stdout
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run timing never reported reclaimed records (exit %d):\n%s", code, stdout)
		}
		time.Sleep(time.Second)
	}
	if !strings.Contains(after, "only  attempt 1  unavailable (attempt records reclaimed)") {
		t.Errorf("attempts block:\n%s", after)
	}
	if standaloneZero.MatchString(after) {
		t.Errorf("reclaimed records rendered a zero duration:\n%s", after)
	}
	doc := decodeTiming(t, s.mustCLI(0, "--json", "run", "timing", runID))
	only := doc.Tasks[0]
	if only.WaitSeconds == nil || only.RunSeconds == nil {
		t.Errorf("after cleanup wait=%v run=%v; the snapshot half must survive", only.WaitSeconds, only.RunSeconds)
	}
	if only.QueueWaitReason != reasonReclaimed {
		t.Errorf("queueWaitReason = %q, want %q", only.QueueWaitReason, reasonReclaimed)
	}
	for _, line := range strings.Split(after, "\n") {
		if strings.Contains(line, "unavailable (attempt records reclaimed)") {
			t.Logf("I5 %s", strings.TrimSpace(line))
		}
	}
}

// TestIntegration_AReplicaThatStartedNothingAnswersRunTiming is I6: the
// timing document is derived from shared state alone, so a replica that never
// accepted the run prints it byte for byte as the one that did.
func TestIntegration_AReplicaThatStartedNothingAnswersRunTiming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 2)
	defer s.writeTranscript("run-timing-two-replicas.txt")
	s.startOrker()

	name := fmt.Sprintf("timing-replica-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(twoTaskTimingOrkflow(name))
	awaitOrkflowVisible(ctx, t, s.replicas[1].flows, name)
	runID := s.submit(name)
	awaitTerminalOnB(ctx, t, s.replicas[1], runID)

	fromA := s.mustCLI(0, "--json", "run", "timing", runID)
	fromB := s.mustCLI(1, "--json", "run", "timing", runID)
	if fromA != fromB {
		t.Fatalf("the replicas disagree:\n--- A ---\n%s\n--- B ---\n%s", fromA, fromB)
	}
	humanA := s.mustCLI(0, "run", "timing", runID)
	humanB := s.mustCLI(1, "run", "timing", runID)
	if humanA != humanB {
		t.Fatalf("the replicas' human output differs:\n--- A ---\n%s\n--- B ---\n%s", humanA, humanB)
	}
	t.Logf("I6 run timing --json is byte-identical from both replicas (%d bytes)", len(fromA))
}
