//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
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

// countingReadClient is the real SDK client behind the reader's narrowed
// seam, counting every call the reader makes of it — the shape
// TestGetStats_ReadsIndexKeysAndNeverARunBody counts reads with.
type countingReadClient struct {
	client.Client
	mu        sync.Mutex
	describes int
	histories int
}

func (c *countingReadClient) DescribeWorkflowExecution(
	ctx context.Context, workflowID, runID string,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.mu.Lock()
	c.describes++
	c.mu.Unlock()
	return c.Client.DescribeWorkflowExecution(ctx, workflowID, runID)
}

func (c *countingReadClient) GetWorkflowHistory(
	ctx context.Context, workflowID, runID string, isLongPoll bool, filter enumspb.HistoryEventFilterType,
) client.HistoryEventIterator {
	c.mu.Lock()
	c.histories++
	c.mu.Unlock()
	return c.Client.GetWorkflowHistory(ctx, workflowID, runID, isLongPoll, filter)
}

// reset zeroes the counters and returns what they held.
func (c *countingReadClient) reset() (describes, histories int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	describes, histories = c.describes, c.histories
	c.describes, c.histories = 0, 0
	return describes, histories
}

var _ engine.RunReadClient = (*countingReadClient)(nil)

// TestIntegration_InspectRunReadsHistoryOnlyForALiveRunWithPendingWork:
// against a real cluster and a real in-process Orker, InspectRun's cluster
// read costs exactly one describe for a finished run and for a live run with
// nothing pending, and one describe plus ONE history read only when a task is
// pending.
//
// "Between tasks" is held open deterministically rather than caught by timing:
// the run's projection activity for TASK_SUCCEEDED(first) is gated, and
// RunWorkflow awaits that activity before it schedules the next task, so while
// the gate is shut the run is live with no ExecuteTask pending.
func TestIntegration_InspectRunReadsHistoryOnlyForALiveRunWithPendingWork(t *testing.T) {
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

	namespace := privateNamespaceForCommitE2E(t)
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
	orkerQueue := "ork-live-reads-" + token

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
	const tick = time.Second
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Logs:              logStore,
		Commits:           commitStore,
		Refs:              refs,
		Pool:              task.DefaultPool,
		OrkerID:           "live-reads-" + token,
		WorkRoot:          bindableWorkRootForCommitE2E(t),
		HeartbeatInterval: tick,
	})
	if err := activities.Boot(ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
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
		DisableRegistrationAliasing:  true,
		DisableWorkflowWorker:        true,
		MaxHeartbeatThrottleInterval: tick,
	})
	taskWorker.RegisterActivityWithOptions(activities.ExecuteTask,
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
	taskWorker.RegisterActivityWithOptions(activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker})

	// The gate: PublishEvent for TASK_SUCCEEDED(first) reports that it was
	// reached and waits to be released. Every other event projects as usual.
	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce sync.Once
	project := engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent
	gatedPublish := func(ctx context.Context, event *orkstackv1.Event, snapshotURI, dedupeKey string) error {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED && event.GetTaskId() == "first" {
			reachedOnce.Do(func() { close(reached) })
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return project(ctx, event, snapshotURI, dedupeKey)
	}
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(gatedPublish,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})

	for name, w := range map[string]worker.Worker{"run": runWorker, "task": taskWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	pinned := pinnedAlpineForCommitE2E(t)
	orkflowName := "live-reads-probe"
	definition := &orkstackv1.Orkflow{
		Name:    orkflowName,
		Version: "1.0." + token,
		Tasks: []*orkstackv1.Task{
			{
				Id: "first", Uses: pinned,
				Image:          &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
				Command:        []string{"/bin/sh", "-c", "echo first; echo '{}' > /orkstack/output/values.json"},
				TimeoutSeconds: 60,
				Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
			},
			{
				Id: "second", Uses: pinned, Needs: []string{"first"},
				Image:          &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
				Command:        []string{"/bin/sh", "-c", "for i in 1 2 3 4 5 6 7 8; do echo tick; sleep 0.5; done; echo '{}' > /orkstack/output/values.json"},
				TimeoutSeconds: 60,
				Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
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
	runClient := orkstackv1connect.NewRunServiceClient(&http.Client{Transport: transport}, "http://"+listener.Addr().String())
	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName:    orkflowName,
		OrkflowVersion: definition.GetVersion(),
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()

	counting := &countingReadClient{Client: temporalClient}
	reader := engine.NewRunReaderOver(counting)
	var transcript strings.Builder
	record := func(state string, live engine.LiveRun, describes, histories int) {
		line := fmt.Sprintf("%s: closed=%v attempts=%d describes=%d history_reads=%d",
			state, live.Closed, len(live.Attempts), describes, histories)
		t.Log(line)
		transcript.WriteString(line + "\n")
	}

	// 1. Live, between tasks: first succeeded, second not yet scheduled.
	reachDeadline := time.NewTimer(2 * time.Minute)
	defer reachDeadline.Stop()
	select {
	case <-reached:
	case <-reachDeadline.C:
		events := bus.Events(runID, 0)
		for _, event := range events {
			t.Logf("event %d %s task=%s status=%s %s", event.GetSequence(), event.GetType(), event.GetTaskId(), event.GetStatus(), event.GetMessage())
		}
		t.Fatalf("first never reached TASK_SUCCEEDED within 2m (%d events projected)", len(events))
	}
	counting.reset()
	between, err := reader.LiveAttempts(ctx, runID, time.Now())
	if err != nil {
		t.Fatalf("LiveAttempts between tasks: %v", err)
	}
	describes, histories := counting.reset()
	record("live run between tasks", between, describes, histories)
	if between.Closed || len(between.Attempts) != 0 || describes != 1 || histories != 0 {
		t.Fatalf("between tasks: closed=%v attempts=%d describes=%d histories=%d, want open, none, 1, 0",
			between.Closed, len(between.Attempts), describes, histories)
	}
	close(release)

	// 2. Live with a pending task: poll until second is pending; the answer
	// that carries it cost exactly one describe and one history read.
	deadline := time.Now().Add(90 * time.Second)
	var pending engine.LiveRun
	for {
		counting.reset()
		pending, err = reader.LiveAttempts(ctx, runID, time.Now())
		if err != nil {
			t.Fatalf("LiveAttempts: %v", err)
		}
		describes, histories = counting.reset()
		if len(pending.Attempts) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second never became a pending attempt")
		}
		time.Sleep(200 * time.Millisecond)
	}
	record("live run with a pending task", pending, describes, histories)
	if pending.Attempts[0].GetTaskId() != "second" || describes != 1 || histories != 1 {
		t.Fatalf("pending: task=%q describes=%d histories=%d, want second, 1, 1",
			pending.Attempts[0].GetTaskId(), describes, histories)
	}

	// 3. Finished: drain the stream to the terminal event, then read once.
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
	deadline = time.Now().Add(30 * time.Second)
	var finished engine.LiveRun
	for {
		counting.reset()
		finished, err = reader.LiveAttempts(ctx, runID, time.Now())
		if err != nil {
			t.Fatalf("LiveAttempts after the run: %v", err)
		}
		describes, histories = counting.reset()
		if finished.Closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the execution never reported closed")
		}
		time.Sleep(200 * time.Millisecond)
	}
	record("finished run", finished, describes, histories)
	if describes != 1 || histories != 0 || len(finished.Attempts) != 0 {
		t.Fatalf("finished: describes=%d histories=%d attempts=%d, want 1, 0, 0", describes, histories, len(finished.Attempts))
	}
	proof.Write(t, "engine", "inspect-run-read-counts.txt", []byte(transcript.String()))
}
