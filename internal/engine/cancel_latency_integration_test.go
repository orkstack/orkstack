//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_RunFailureStopsASiblingWithinTheHeartbeatInterval measures
// the one number a unit test cannot: how long a container keeps running after
// the engine asked for it to stop.
//
// It has to be an integration test because the thing being fixed lives in the
// Temporal Go SDK's client, not in this repository's logic. The SDK batches
// RecordHeartbeat calls into windows of
// min(0.8 × HeartbeatTimeout, worker.Options.MaxHeartbeatThrottleInterval) and
// delivers cancellation ONLY in a heartbeat RPC's response, so with the cap
// unset the answer is ~24 s however fast the Orker's loop ticks. The workflow
// test environment has neither behavior: it delivers cancellation immediately,
// which is why every existing cancellation test passed while a real run took
// 23.9 s. Measured on the run that prompted this: cancellation requested 52 ms
// after the sibling's failure, observed 23.86 s later, container dead 296 ms
// after that.
//
// Both halves of the pairing are set below, deliberately and together — a
// harness that sets only orker.Options.HeartbeatInterval measures the SDK's
// default and proves nothing.
func TestIntegration_RunFailureStopsASiblingWithinTheHeartbeatInterval(t *testing.T) {
	const tick = orker.DefaultHeartbeatInterval

	cfg := s3Config()
	requireMinIO(t, cfg)
	requireDockerForCommitE2E(t)

	orkflows := newDurableOrkflowStore(t)
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	namespace := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if namespace == "" {
		namespace = task.DefaultNamespace
	}
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()
	if err := task.EnsureNamespace(ctx, temporalClient, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, namespace)

	token := fmt.Sprintf("%d", time.Now().UnixNano())
	orkerQueue := "ork-cancel-latency-" + token

	store, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("open artifact store: %v", err)
	}
	commitStore, err := artifacts.NewCommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("open commit store: %v", err)
	}
	logStore, err := artifacts.NewLogStore(cfg)
	if err != nil {
		t.Fatalf("open log store: %v", err)
	}
	refs, ok := store.(artifacts.RefPredictor)
	if !ok {
		t.Fatalf("artifact backend %T cannot predict content-addressed references", store)
	}

	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Logs:     logStore,
		Commits:  commitStore,
		Refs:     refs,
		Pool:     task.DefaultPool,
		OrkerID:  "cancel-latency-" + token,
		WorkRoot: bindableWorkRootForCommitE2E(t),
		// HALF ONE of the pairing: how often the loop beats.
		HeartbeatInterval: tick,
	})
	if err := activities.Boot(ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}

	// SIGTERM has to be observed at the WORKLOAD, not just at the daemon: a
	// fast cancellation that SIGKILLs a well-behaved container would be a
	// regression dressed as an improvement. The task echoes a marker from its
	// own TERM trap and this reads it off the live log sink.
	var logMu sync.Mutex
	var siblingLog []string
	activities.Dispatcher().LogSinkObserver = func(in *orkstackv1.ExecuteTaskInput, line string) {
		if in.GetTaskId() != "sibling" {
			return
		}
		logMu.Lock()
		siblingLog = append(siblingLog, line)
		logMu.Unlock()
	}

	bus, runs := engine.NewEventBus(), engine.NewRunStore()

	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	taskWorker := worker.New(temporalClient, orkerQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
		// HALF TWO of the pairing, and the one that was missing: without it
		// the SDK batches the beats above into 24 s windows and this test
		// measures Temporal's default rather than OrkStack's setting. This is
		// exactly what cmd/orker's workerOptions does.
		MaxHeartbeatThrottleInterval: tick,
	})
	taskWorker.RegisterActivityWithOptions(
		activities.ExecuteTask,
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	taskWorker.RegisterActivityWithOptions(
		activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker},
	)

	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	for name, w := range map[string]worker.Worker{
		"run": runWorker, "task": taskWorker, "event": eventWorker,
	} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	// Two tasks, no edges, so both are dispatched in the first scheduling pass
	// and the sibling is genuinely inside its container run when the failure
	// lands. `doomed` sleeps first for exactly that reason.
	pinned := pinnedAlpineForCommitE2E(t)
	orkflowName := "cancel-latency-probe"
	definition := &orkstackv1.Orkflow{
		Name:    orkflowName,
		Version: "1.0." + token,
		Tasks: []*orkstackv1.Task{
			{
				Id:             "doomed",
				Uses:           pinned,
				Image:          &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
				Command:        []string{"/bin/sh", "-c", "sleep 3; exit 1"},
				TimeoutSeconds: 120,
				Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
			},
			{
				Id:      "sibling",
				Uses:    pinned,
				Image:   &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
				Command: []string{"/bin/sh", "-c", `trap 'echo caught-term; exit 0' TERM; echo ready; while true; do sleep 0.2; done`},
				// Long enough that the task's OWN timeout can never be what
				// ends it: only the run's failure may.
				TimeoutSeconds: 300,
				// A real grace budget, so a SIGKILL would be visible as a
				// missing caught-term rather than hidden behind a zero window.
				CancellationGracePeriodSeconds: 10,
				Network:                        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
			},
		},
	}
	if _, _, err := orkflows.Put(ctx, definition, artifacts.HeadExpectation{}); err != nil {
		t.Fatalf("store the definition: %v", err)
	}

	svc := api.NewService(api.Config{
		Orkflows:  orkflows,
		Runs:      runs,
		Bus:       bus,
		Starter:   engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, orkerQueue),
		RawQueue:  orkerQueue,
		Artifacts: cfg,
		Limits:    engine.DefaultLimits(),
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(svc, api.MuxOptions{}))
	go func() { _ = server.Serve(listener) }()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	defer func() {
		transport.CloseIdleConnections()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	runClient := orkstackv1connect.NewRunServiceClient(
		&http.Client{Transport: transport}, "http://"+listener.Addr().String())

	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName:    orkflowName,
		OrkflowVersion: definition.GetVersion(),
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	t.Logf("EVIDENCE run_id=%s heartbeat_interval=%s", runID, tick)

	// Drain rather than sleep: the stream closes at the run's terminal event.
	stream, err := runClient.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	for stream.Receive() {
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream: %v", err)
	}
	stream.Close()

	// The measurement comes from the ENGINE's own event timestamps, not from
	// wall clock in this process: they are what a user reads on the run page.
	var doomedFailed, siblingEnded, runFailed time.Time
	for _, event := range bus.Events(runID, 0) {
		at := event.GetTimestamp().AsTime()
		switch {
		case event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED && event.GetTaskId() == "doomed":
			doomedFailed = at
		case event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED && event.GetTaskId() == "sibling":
			siblingEnded = at
		case event.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_FAILED:
			runFailed = at
		}
	}
	if doomedFailed.IsZero() {
		t.Fatal("doomed never failed; this run did not exercise the cancellation path")
	}
	if siblingEnded.IsZero() {
		t.Fatal("sibling never reached a terminal event; it was left running after the run failed")
	}

	latency := siblingEnded.Sub(doomedFailed)
	// The budget: one whole heartbeat interval to deliver the cancellation,
	// plus a generous allowance for SIGTERM, container exit, docker rm, log
	// sink close and event publication (measured at ~0.45 s in total). Before
	// the throttle cap this measured ~24 s and failed here, which is the whole
	// point of the assertion.
	budget := tick + 5*time.Second
	if latency > budget {
		t.Errorf("the sibling kept running for %s after the run's failure, want <= %s "+
			"(one %s heartbeat interval plus teardown). A latency near 24s means the SDK's "+
			"MaxHeartbeatThrottleInterval is not capped to the Orker's tick",
			latency, budget, tick)
	}

	run, ok := runs.Get(runID)
	if !ok {
		t.Fatal("run read model was not populated")
	}
	sibling := taskRunOfIntegration(t, run, "sibling")
	if sibling.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("sibling status = %v, want CANCELED", sibling.GetStatus())
	}
	// It really RAN, so it carries a dispatch stamp — the fact the run page
	// uses to tell it from a task the run ended before it ever started.
	if sibling.GetStartedAt() == nil {
		t.Error("sibling started_at is unset, but it was dispatched and killed")
	}
	if code := sibling.GetError().GetCode(); code != "" && code != "TASK_CANCELED" {
		t.Logf("sibling error code = %q", code)
	}
	if !runFailed.IsZero() && runFailed.Before(siblingEnded) {
		t.Errorf("RUN_FAILED at %s precedes the sibling's terminal event at %s; the run must not "+
			"read FAILED while its containers are still being torn down",
			runFailed.Format(time.RFC3339Nano), siblingEnded.Format(time.RFC3339Nano))
	}

	logMu.Lock()
	lines := strings.Join(siblingLog, "\n")
	logMu.Unlock()
	caughtTerm := strings.Contains(lines, "caught-term")
	if !caughtTerm {
		t.Errorf("the sibling's log never shows caught-term: it was killed rather than asked to "+
			"stop, so a workload's cleanup budget is not being honored. log:\n%s", lines)
	}

	t.Logf("EVIDENCE cancel_latency=%s budget=%s sigterm_reached_workload=%v",
		latency, budget, caughtTerm)
	proof.WriteJSON(t, "cancel-latency", "sibling", map[string]any{
		"run_id":                   runID,
		"heartbeat_interval":       tick.String(),
		"doomed_failed_at":         doomedFailed.Format(time.RFC3339Nano),
		"sibling_terminal_at":      siblingEnded.Format(time.RFC3339Nano),
		"run_failed_at":            runFailed.Format(time.RFC3339Nano),
		"cancel_latency":           latency.String(),
		"budget":                   budget.String(),
		"sigterm_reached_workload": caughtTerm,
		"sibling_status":           sibling.GetStatus().String(),
		"sibling_started_at_set":   sibling.GetStartedAt() != nil,
	})
}

// taskRunOfIntegration is the integration suite's own reader for one task run.
func taskRunOfIntegration(t *testing.T, run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	t.Helper()
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == taskID {
			return tr
		}
	}
	t.Fatalf("run %s has no task run for %q", run.GetId(), taskID)
	return nil
}
