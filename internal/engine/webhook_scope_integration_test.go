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
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// scopedCluster is one private Temporal namespace for one acceptance test.
// Temporal task queues are namespace-scoped, so a private namespace keeps the
// shared ork-engine-events channel of every other process on this machine away
// from this test's projection activities (AGENTS.md §11.1).
func scopedCluster(t *testing.T, ctx context.Context) client.Client {
	t.Helper()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
	namespace := privateNamespaceForCommitE2E(t)
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	t.Cleanup(temporalClient.Close)
	if err := task.EnsureNamespace(ctx, temporalClient, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, namespace)
	t.Logf("temporal namespace %s", namespace)
	return temporalClient
}

// scopedEngine is one engine replica as `ork` wires it: the run worker, the
// projection worker on ork-engine-events announcing to a webhook notifier, and
// the API behind a real listener. A replica built with a nil notifier runs no
// projection worker at all — it only serves the management API.
type scopedEngine struct {
	bus        *engine.EventBus
	runs       *engine.RunStore
	registry   *webhooks.Registry
	notifier   *webhooks.Notifier
	httpClient *http.Client
	origin     string
}

type scopedEngineConfig struct {
	temporal client.Client
	// runQueue is the channel the run's workflow is started on.
	runQueue string
	// config is completed with Runs, Bus, Webhooks and Notifier.
	config api.Config
	// registry is the webhook registry. Nil builds an in-memory one.
	registry *webhooks.Registry
	// project runs the projection worker. A management-only replica sets it
	// false.
	project bool
	// snapshots is shared with the run worker.
	snapshots *testSnapshotStore
}

func startScopedEngine(t *testing.T, cfg scopedEngineConfig) *scopedEngine {
	t.Helper()
	e := &scopedEngine{bus: engine.NewEventBus(), runs: engine.NewRunStore(), registry: cfg.registry}
	if e.registry == nil {
		e.registry = webhooks.NewRegistry()
	}
	var runNotifier engine.RunNotifier
	if cfg.project {
		e.notifier = webhooks.NewNotifier(e.registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
			webhooks.NotifierOptions{})
		runNotifier = api.NewWebhookNotifier(e.notifier, nil)

		runWorker := worker.New(cfg.temporal, cfg.runQueue, worker.Options{
			DisableRegistrationAliasing: true,
			LocalActivityWorkerOnly:     true,
		})
		engine.RegisterWorkflows(runWorker)
		runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(cfg.snapshots).WriteSnapshot,
			activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
		eventWorker := worker.New(cfg.temporal, engine.EventQueue, worker.Options{
			DisableRegistrationAliasing: true,
			DisableWorkflowWorker:       true,
		})
		eventWorker.RegisterActivityWithOptions(
			engine.NewEventActivities(e.bus, e.runs).WithRunNotifier(runNotifier).WithSnapshotStore(cfg.snapshots).PublishEvent,
			activity.RegisterOptions{Name: engine.ActivityPublishEvent})
		for name, w := range map[string]worker.Worker{"run": runWorker, "event": eventWorker} {
			if err := w.Start(); err != nil {
				t.Fatalf("start %s worker: %v", name, err)
			}
			t.Cleanup(w.Stop)
		}
	}

	config := cfg.config
	config.Runs = e.runs
	config.Bus = e.bus
	config.Webhooks = e.registry
	if runNotifier != nil {
		config.Notifier = runNotifier
	}
	svc := api.NewService(config)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(svc, api.MuxOptions{}))
	go func() { _ = server.Serve(listener) }()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	e.httpClient = &http.Client{Transport: transport}
	e.origin = "http://" + listener.Addr().String()
	return e
}

func (e *scopedEngine) orkflowClient() orkstackv1connect.OrkflowServiceClient {
	return orkstackv1connect.NewOrkflowServiceClient(e.httpClient, e.origin)
}

func (e *scopedEngine) runClient() orkstackv1connect.RunServiceClient {
	return orkstackv1connect.NewRunServiceClient(e.httpClient, e.origin)
}

// drainRun follows a run's event stream to its terminal event.
func (e *scopedEngine) drainRun(t *testing.T, ctx context.Context, runID, namespace string) *orkstackv1.Run {
	t.Helper()
	stream, err := e.runClient().StreamEvents(ctx, connect.NewRequest(
		&orkstackv1.StreamEventsRequest{RunId: runID, Namespace: namespace}))
	if err != nil {
		t.Fatalf("StreamEvents %s: %v", runID, err)
	}
	for stream.Receive() { //nolint:revive // draining the stream is the wait
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream %s: %v", runID, err)
	}
	stream.Close()
	got, err := e.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: namespace}))
	if err != nil {
		t.Fatalf("GetRun %s: %v", runID, err)
	}
	return got.Msg.GetRun()
}

// settle waits for every queued delivery, so a count taken afterwards is a
// count of everything this notifier will ever send for the run.
func (e *scopedEngine) settle(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := e.notifier.Wait(ctx); err != nil {
		t.Fatalf("deliveries did not settle: %v", err)
	}
}

// committingTaskWorker stands in for the Orker on a private pool: every task
// commits a values reference and reports the Temporal attempt that produced
// it, exactly the two fields the real Orker returns.
func committingTaskWorker(t *testing.T, temporalClient client.Client, queue string) {
	t.Helper()
	w := worker.New(temporalClient, queue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	w.RegisterActivityWithOptions(
		func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{
				TaskId:     in.GetTaskId(),
				ValuesJson: valuesJSON(map[string]any{"echoed_task": in.GetTaskId()}),
				ValuesRef: &orkstackv1.ArtifactRef{
					Name: "values.json",
					Uri:  fmt.Sprintf("s3://ork-artifacts/test/%s/%s/values.json", in.GetRunId(), in.GetTaskId()),
				},
				Attempt: activity.GetInfo(ctx).Attempt,
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	if err := w.Start(); err != nil {
		t.Fatalf("start task worker: %v", err)
	}
	t.Cleanup(w.Stop)
}

func uniquePool(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// TestIntegration_TaskOutcomeWebhookFiresWhenTheTaskCommits is the research
// narrative on a real cluster: a subscriber asks for TASK_SUCCEEDED of one task
// of one orkflow, and receives it — signed, naming the task and its attempt,
// embedding the committed values reference — the moment that task commits,
// and nothing for the run's other tasks.
func TestIntegration_TaskOutcomeWebhookFiresWhenTheTaskCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	temporalClient := scopedCluster(t, ctx)
	pool := uniquePool("webhook-task")
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map pool: %v", err)
	}
	committingTaskWorker(t, temporalClient, queue)
	subscriber := newWebhookSubscriber(t)
	// The registry production wires (cmd/ork): durable, over a real store, so
	// the signature asserted below is the one a production delivery carries.
	webhookStore, err := artifacts.NewLocalWebhookStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	e := startScopedEngine(t, scopedEngineConfig{
		temporal: temporalClient, runQueue: queue, project: true, snapshots: newTestSnapshotStore(),
		registry: webhooks.NewDurableRegistry(webhookStore),
		config: api.Config{
			Orkflows: engine.NewMemoryOrkflowStore(),
			Starter:  engine.NewTemporalStarter(temporalClient, queue),
		},
	})
	e.notifier.Observe(subscriber.record)

	hook := registerWebhookOverHTTP(t, e.httpClient, e.origin, map[string]any{
		"url":         subscriber.server.URL,
		"events":      []string{"TASK_SUCCEEDED", "RUN_SUCCEEDED"},
		"orkflowName": webhookOrkflowName,
		"taskId":      "transform",
	})
	if hook.OrkflowName != webhookOrkflowName || hook.TaskID != "transform" {
		t.Fatalf("registered %+v, want the orkflow and task scope", hook)
	}

	if _, err := e.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: webhookOrkflowYAML(pool)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	submitted, err := e.runClient().SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: webhookOrkflowName}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	if run := e.drainRun(t, ctx, runID, ""); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s ended %v (%v), want SUCCEEDED", runID, run.GetStatus(), run.GetError())
	}
	subscriber.awaitCount(t, runID, 2)
	e.settle(t)

	deliveries := subscriber.deliveries(runID)
	if len(deliveries) != 2 {
		t.Fatalf("received %d deliveries, want TASK_SUCCEEDED of transform then RUN_SUCCEEDED: %+v", len(deliveries), deliveries)
	}
	// Each delivery is its own goroutine and the durable lookup is
	// asynchronous, so arrival order is not part of the contract: find each
	// by its event.
	taskDelivery, runDelivery := deliveries[0], deliveries[1]
	if taskDelivery.event == "RUN_SUCCEEDED" {
		taskDelivery, runDelivery = runDelivery, taskDelivery
	}
	if taskDelivery.event != "TASK_SUCCEEDED" || runDelivery.event != "RUN_SUCCEEDED" {
		t.Fatalf("delivered %s and %s, want one TASK_SUCCEEDED and one RUN_SUCCEEDED", taskDelivery.event, runDelivery.event)
	}
	for _, got := range deliveries {
		if !webhooks.Verify(hook.Secret, got.body, got.signature) {
			t.Errorf("%s signature does not verify against its body", got.event)
		}
	}
	if taskDelivery.payload.TaskID != "transform" || taskDelivery.taskHeader != "transform" {
		t.Errorf("task delivery taskId=%q %s=%q, want transform", taskDelivery.payload.TaskID,
			webhooks.TaskHeader, taskDelivery.taskHeader)
	}
	if taskDelivery.payload.Attempt != 1 {
		t.Errorf("task delivery attempt = %d, want 1", taskDelivery.payload.Attempt)
	}
	if taskDelivery.payload.Status != "SUCCEEDED" {
		t.Errorf("task delivery status = %q, want SUCCEEDED", taskDelivery.payload.Status)
	}
	var transformCommitted bool
	for _, tr := range taskDelivery.payload.Run.TaskRuns {
		if tr.TaskID == "transform" {
			transformCommitted = tr.Status == "SUCCEEDED" && strings.Contains(string(tr.ValuesRef), "transform/values.json")
		}
	}
	if !transformCommitted {
		t.Errorf("the task delivery's embedded run does not read transform SUCCEEDED with its valuesRef: %s", taskDelivery.body)
	}
	if runDelivery.taskHeader != "" || runDelivery.payload.TaskID != "" || runDelivery.payload.Attempt != 0 {
		t.Errorf("the run delivery carries task coordinates: header=%q body=%s", runDelivery.taskHeader, runDelivery.body)
	}
	for _, got := range deliveries {
		if got.payload.TaskID == "fetch" || got.payload.TaskID == "save" {
			t.Errorf("a delivery went out for %s, outside the subscription's task scope", got.payload.TaskID)
		}
	}
	t.Logf("EVIDENCE run=%s task_delivery=%s taskId=%s attempt=%d x-ork-task-id=%s",
		runID, taskDelivery.event, taskDelivery.payload.TaskID, taskDelivery.payload.Attempt, taskDelivery.taskHeader)
	writeWebhookProofNamed(t, "webhook-task-outcome", runID, hook, deliveries, subscriber.results(runID))
}

// TestIntegration_CanceledTaskProducesNoTaskDeliveryOnlyTheRunsTerminalOne is
// the review-round-1 finding on real containers: a task that was running when
// its run ended comes back from Temporal as a canceled activity, which the
// interpreter records as TASK_FAILED carrying CANCELED. That event must never
// reach a subscriber as a task failure; the run's terminal delivery is where
// the cancellation is read. The canceled error only exists when Temporal
// actually cancels an in-flight activity, so this needs a real Orker.
func TestIntegration_CanceledTaskProducesNoTaskDeliveryOnlyTheRunsTerminalOne(t *testing.T) {
	cfg := s3Config()
	requireMinIO(t, cfg)
	requireDockerForCommitE2E(t)
	pinned := pinnedAlpineForCommitE2E(t)

	loop := []string{"/bin/sh", "-c", `trap 'echo caught-term; exit 0' TERM; echo ready; while true; do sleep 0.2; done`}
	cases := []struct {
		name     string
		partner  *orkstackv1.Task
		cancel   bool
		terminal string
	}{
		{
			name: "run canceled by an operator",
			partner: &orkstackv1.Task{Id: "partner", Command: loop, TimeoutSeconds: 300,
				CancellationGracePeriodSeconds: 10},
			cancel:   true,
			terminal: "RUN_CANCELED",
		},
		{
			name: "sibling canceled because another task failed",
			partner: &orkstackv1.Task{Id: "doomed", Command: []string{"/bin/sh", "-c", "sleep 3; exit 1"},
				TimeoutSeconds: 120},
			terminal: "RUN_FAILED",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
			defer cancel()
			temporalClient := scopedCluster(t, ctx)
			orkflows := newDurableOrkflowStore(t)
			token := fmt.Sprintf("%d", time.Now().UnixNano())
			orkerQueue := "ork-webhook-cancel-" + token

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
				Logs: logStore, Commits: commitStore, Refs: refs, Pool: task.DefaultPool,
				OrkerID: "webhook-cancel-" + token, WorkRoot: bindableWorkRootForCommitE2E(t),
				HeartbeatInterval: orker.DefaultHeartbeatInterval,
			})
			if err := activities.Boot(ctx); err != nil {
				infraAbsent(t, "orker boot checks failed: %v", err)
			}
			ready := make(chan struct{})
			var readyOnce sync.Once
			activities.Dispatcher().LogSinkObserver = func(in *orkstackv1.ExecuteTaskInput, line string) {
				if in.GetTaskId() == "sibling" && strings.Contains(line, "ready") {
					readyOnce.Do(func() { close(ready) })
				}
			}
			taskWorker := worker.New(temporalClient, orkerQueue, worker.Options{
				DisableRegistrationAliasing:  true,
				DisableWorkflowWorker:        true,
				MaxHeartbeatThrottleInterval: orker.DefaultHeartbeatInterval,
			})
			taskWorker.RegisterActivityWithOptions(activities.ExecuteTask, activity.RegisterOptions{Name: task.ActivityExecuteTask})
			taskWorker.RegisterActivityWithOptions(activities.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
			if err := taskWorker.Start(); err != nil {
				t.Fatalf("start task worker: %v", err)
			}
			defer taskWorker.Stop()

			subscriber := newWebhookSubscriber(t)
			e := startScopedEngine(t, scopedEngineConfig{
				temporal: temporalClient, runQueue: task.DefaultQueue, project: true, snapshots: newTestSnapshotStore(),
				config: api.Config{
					Orkflows:  orkflows,
					Starter:   engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, orkerQueue),
					RawQueue:  orkerQueue,
					Artifacts: cfg,
					Limits:    engine.DefaultLimits(),
				},
			})
			e.notifier.Observe(subscriber.record)

			partner := tc.partner
			partner.Uses = pinned
			partner.Image = &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned}
			partner.Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE
			orkflowName := "webhook-cancel-probe"
			definition := &orkstackv1.Orkflow{
				Name:    orkflowName,
				Version: "1.0." + token,
				Tasks: []*orkstackv1.Task{
					partner,
					{
						Id: "sibling", Uses: pinned, Image: &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
						Command: loop, TimeoutSeconds: 300, CancellationGracePeriodSeconds: 10,
						Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE,
					},
				},
			}
			if _, _, err := orkflows.Put(ctx, definition, artifacts.HeadExpectation{}); err != nil {
				t.Fatalf("store the definition: %v", err)
			}

			hook := registerWebhookOverHTTP(t, e.httpClient, e.origin, map[string]any{
				"url":    subscriber.server.URL,
				"events": []string{"TASK_STARTED", "TASK_FAILED", "RUN_FAILED", "RUN_CANCELED"},
				"taskId": "sibling",
			})

			submitted, err := e.runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
				OrkflowName: orkflowName, OrkflowVersion: definition.GetVersion(),
			}))
			if err != nil {
				t.Fatalf("SubmitRun: %v", err)
			}
			runID := submitted.Msg.GetRun().GetId()
			t.Logf("EVIDENCE run_id=%s case=%q", runID, tc.name)

			// The subscriber's own TASK_STARTED for sibling, then the
			// container's own "ready": sibling is provably running.
			subscriber.awaitCount(t, runID, 1)
			select {
			case <-ready:
			case <-time.After(3 * time.Minute):
				t.Fatal("sibling's container never printed ready")
			}
			if tc.cancel {
				if _, err := e.runClient().CancelRun(ctx, connect.NewRequest(&orkstackv1.CancelRunRequest{
					RunId: runID, Reason: "scoped-subscriptions acceptance",
				})); err != nil {
					t.Fatalf("CancelRun: %v", err)
				}
			}
			e.drainRun(t, ctx, runID, "")

			// The bus holds the event the guard refused: the interpreter
			// emitted TASK_FAILED carrying CANCELED for sibling.
			//
			// When a task fails, sibling is always collected by
			// cancelInFlight on a live context, so the event is always there.
			// When the operator cancels, it is there only if partner's
			// cancellation was collected first: whichever task the main loop
			// collects first is collected on the already-canceled workflow
			// context, where emit's publish activity cannot be scheduled, so
			// that task's TASK_FAILED never reaches the bus at all — which
			// of the two it is depends on which cancellation Temporal reports
			// first. Either way no sibling TASK_FAILED may carry FAILED, and
			// the delivery counts below are the contract.
			var refused *orkstackv1.Event
			for _, event := range e.bus.Events(runID, 0) {
				if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED && event.GetTaskId() == "sibling" {
					refused = event
				}
			}
			switch {
			case refused != nil && refused.GetStatus() != orkstackv1.Status_STATUS_CANCELED:
				t.Fatalf("the bus holds sibling's TASK_FAILED as %v, want one carrying CANCELED", refused)
			case refused == nil && !tc.cancel:
				t.Fatalf("the bus holds no TASK_FAILED for sibling, want one carrying CANCELED: cancelInFlight collects it on a live context")
			}
			busStatus := "absent"
			if refused != nil {
				busStatus = refused.GetStatus().String()
			}

			// Every projection before the terminal one completed before it
			// started (emit awaits each), so once the terminal delivery has
			// arrived and the notifier has settled, the count is final.
			subscriber.awaitCount(t, runID, 2)
			e.settle(t)
			deliveries := subscriber.deliveries(runID)
			results := subscriber.results(runID)
			var started, failed, terminal []receivedDelivery
			for _, got := range deliveries {
				switch got.event {
				case "TASK_STARTED":
					started = append(started, got)
				case "TASK_FAILED":
					failed = append(failed, got)
				case tc.terminal:
					terminal = append(terminal, got)
				default:
					t.Errorf("unexpected delivery %s", got.event)
				}
			}
			var failedResults int
			for _, result := range results {
				if result.Event == webhooks.EventTaskFailed {
					failedResults++
				}
			}
			if len(started) != 1 || started[0].taskHeader != "sibling" {
				t.Errorf("TASK_STARTED deliveries = %+v, want exactly one for sibling", started)
			}
			if len(failed) != 0 || failedResults != 0 {
				t.Errorf("%d TASK_FAILED POSTs and %d TASK_FAILED results for a task that was canceled, want 0",
					len(failed), failedResults)
			}
			if len(terminal) != 1 {
				t.Fatalf("%s deliveries = %d, want exactly 1", tc.terminal, len(terminal))
			}
			if len(results) != 2 {
				t.Errorf("Observe seam recorded %d results for the run, want 2 (TASK_STARTED and %s)", len(results), tc.terminal)
			}
			// scheduledAt is the dispatch stamp (task-timing-breakdown):
			// sibling was dispatched and then canceled, so it carries one.
			// startedAt is the Orker's own start, which a plain context
			// cancellation does not report, so it is not asserted here.
			statuses := map[string]string{}
			scheduledAt := map[string]string{}
			for _, tr := range terminal[0].payload.Run.TaskRuns {
				statuses[tr.TaskID] = tr.Status
				scheduledAt[tr.TaskID] = tr.ScheduledAt
			}
			if statuses["sibling"] != "CANCELED" || scheduledAt["sibling"] == "" {
				t.Errorf("%s run reads sibling as %q (scheduledAt %q), want CANCELED with a scheduledAt",
					tc.terminal, statuses["sibling"], scheduledAt["sibling"])
			}
			if !tc.cancel && statuses["doomed"] != "FAILED" {
				t.Errorf("%s run reads doomed as %q, want FAILED", tc.terminal, statuses["doomed"])
			}
			t.Logf("EVIDENCE case=%q bus_task_failed_status=%s deliveries=%d task_failed_posts=%d terminal=%s sibling=%s",
				tc.name, busStatus, len(deliveries), len(failed), tc.terminal, statuses["sibling"])
			prefix := "webhook-canceled-operator"
			if !tc.cancel {
				prefix = "webhook-canceled-sibling"
			}
			writeWebhookProofNamed(t, prefix, runID, hook, deliveries, results)
		})
	}
}

// TestIntegration_ScopedWebhookReceivesOnlyItsNamespace is tenancy on the
// real path: a subscription scoped to one namespace observes no result for a
// run in another, while an unscoped one observes both.
func TestIntegration_ScopedWebhookReceivesOnlyItsNamespace(t *testing.T) {
	cfg := s3Config()
	requireMinIO(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	temporalClient := scopedCluster(t, ctx)
	pool := uniquePool("webhook-ns")
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map pool: %v", err)
	}
	committingTaskWorker(t, temporalClient, queue)

	namespaces, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		infraAbsent(t, "cannot open the namespace registry: %v", err)
	}
	analytics := fmt.Sprintf("analytics-%d", time.Now().UnixNano())
	if _, err := namespaces.Put(ctx, &orkstackv1.Namespace{
		Name: analytics, Description: "scoped webhook acceptance",
		DefaultPool: task.DefaultPool, AllowedPools: []string{task.DefaultPool, pool},
	}, artifacts.PutConditions{}); err != nil {
		infraAbsent(t, "cannot write the namespace document: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := namespaces.Delete(cleanupCtx, analytics); err != nil {
			t.Logf("could not remove namespace %s: %v", analytics, err)
		}
	})

	subscriber := newWebhookSubscriber(t)
	e := startScopedEngine(t, scopedEngineConfig{
		temporal: temporalClient, runQueue: queue, project: true, snapshots: newTestSnapshotStore(),
		config: api.Config{
			Orkflows:   engine.NewMemoryOrkflowStore(),
			Starter:    engine.NewTemporalStarter(temporalClient, queue),
			Namespaces: namespaces,
		},
	})
	e.notifier.Observe(subscriber.record)

	scoped := registerWebhookOverHTTP(t, e.httpClient, e.origin, map[string]any{
		"url": subscriber.server.URL + "/scoped", "namespace": analytics,
	})
	unscoped := registerWebhookOverHTTP(t, e.httpClient, e.origin, map[string]any{
		"url": subscriber.server.URL + "/unscoped",
	})

	runIDs := map[string]string{}
	for _, namespace := range []string{analytics, ""} {
		if _, err := e.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: webhookOrkflowYAML(pool), Namespace: namespace,
		})); err != nil {
			t.Fatalf("Apply in %q: %v", namespace, err)
		}
		submitted, err := e.runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: webhookOrkflowName, Namespace: namespace,
		}))
		if err != nil {
			t.Fatalf("SubmitRun in %q: %v", namespace, err)
		}
		runID := submitted.Msg.GetRun().GetId()
		runIDs[namespace] = runID
		if run := e.drainRun(t, ctx, runID, namespace); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %s in %q ended %v (%v)", runID, namespace, run.GetStatus(), run.GetError())
		}
	}
	// analytics: scoped + unscoped, RUN_STARTED and RUN_SUCCEEDED each.
	subscriber.awaitCount(t, runIDs[analytics], 4)
	// default: only the unscoped subscription.
	subscriber.awaitCount(t, runIDs[""], 2)
	e.settle(t)

	byHook := map[string][]string{}
	for _, runID := range runIDs {
		for _, result := range subscriber.results(runID) {
			byHook[result.WebhookID] = append(byHook[result.WebhookID], result.RunID)
		}
	}
	for _, runID := range byHook[scoped.ID] {
		if runID != runIDs[analytics] {
			t.Errorf("the %s-scoped subscription observed run %s of another namespace", analytics, runID)
		}
	}
	if len(byHook[scoped.ID]) != 2 {
		t.Errorf("the scoped subscription observed %d results, want 2 (its namespace's RUN_STARTED and RUN_SUCCEEDED): %v",
			len(byHook[scoped.ID]), byHook[scoped.ID])
	}
	seen := map[string]int{}
	for _, runID := range byHook[unscoped.ID] {
		seen[runID]++
	}
	if seen[runIDs[analytics]] != 2 || seen[runIDs[""]] != 2 {
		t.Errorf("the unscoped subscription observed %v, want both runs twice", seen)
	}
	t.Logf("EVIDENCE namespace=%s scoped=%s results=%v unscoped=%s results=%v",
		analytics, scoped.ID, byHook[scoped.ID], unscoped.ID, seen)
	proof.WriteJSON(t, "engine", "webhook-namespace-scope.json", map[string]any{
		"namespace":          analytics,
		"runs":               map[string]string{analytics: runIDs[analytics], "default": runIDs[""]},
		"scopedWebhook":      scoped.ID,
		"scopedObservedRuns": byHook[scoped.ID],
		"unscopedWebhook":    unscoped.ID,
		"unscopedObserved":   seen,
	})
}

// TestIntegration_ScopedWebhookRegisteredOnOneReplicaMatchesOnAnother proves
// the scope is read from the shared record, never from process memory:
// replica A registers a scoped task-event subscription through its API, and a
// run projected by replica B's event worker — a different process's registry
// over the same bucket — delivers to it.
func TestIntegration_ScopedWebhookRegisteredOnOneReplicaMatchesOnAnother(t *testing.T) {
	cfg := s3Config()
	requireMinIO(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	temporalClient := scopedCluster(t, ctx)
	pool := uniquePool("webhook-replica")
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map pool: %v", err)
	}
	committingTaskWorker(t, temporalClient, queue)

	durable := func() *webhooks.Registry {
		store, err := artifacts.NewS3WebhookStore(ctx, cfg)
		if err != nil {
			infraAbsent(t, "cannot open the webhook registry: %v", err)
		}
		return webhooks.NewDurableRegistry(store)
	}
	replicaA := startScopedEngine(t, scopedEngineConfig{
		temporal: temporalClient, runQueue: queue, registry: durable(),
		config: api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: engine.NewTemporalStarter(temporalClient, queue)},
	})
	replicaB := startScopedEngine(t, scopedEngineConfig{
		temporal: temporalClient, runQueue: queue, registry: durable(), project: true, snapshots: newTestSnapshotStore(),
		config: api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: engine.NewTemporalStarter(temporalClient, queue)},
	})
	subscriber := newWebhookSubscriber(t)
	replicaB.notifier.Observe(subscriber.record)

	orkflowName := webhookOrkflowName
	hook := registerWebhookOverHTTP(t, replicaA.httpClient, replicaA.origin, map[string]any{
		"url":         subscriber.server.URL,
		"events":      []string{"TASK_SUCCEEDED"},
		"namespace":   "default",
		"orkflowName": orkflowName,
		"taskId":      "transform",
	})
	t.Cleanup(func() {
		doWebhookRequest(t, replicaA.httpClient, http.MethodDelete,
			replicaA.origin+strings.Replace(api.WebhookItemPath, "{id}", hook.ID, 1), nil)
	})

	// Replica B never saw the registration; it must read it from the bucket.
	var listedOnB bool
	for _, listed := range listWebhooksOverHTTP(t, replicaB.httpClient, replicaB.origin) {
		if listed.ID == hook.ID {
			listedOnB = listed.Namespace == "default" && listed.OrkflowName == orkflowName && listed.TaskID == "transform"
		}
	}
	if !listedOnB {
		t.Fatalf("replica B does not list %s with its scope", hook.ID)
	}

	if _, err := replicaB.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: webhookOrkflowYAML(pool)})); err != nil {
		t.Fatalf("Apply on B: %v", err)
	}
	submitted, err := replicaB.runClient().SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: orkflowName}))
	if err != nil {
		t.Fatalf("SubmitRun on B: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	if run := replicaB.drainRun(t, ctx, runID, ""); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s ended %v (%v)", runID, run.GetStatus(), run.GetError())
	}
	subscriber.awaitCount(t, runID, 1)
	replicaB.settle(t)

	var mine []receivedDelivery
	for _, got := range subscriber.deliveries(runID) {
		if got.webhookID == hook.ID {
			mine = append(mine, got)
		}
	}
	if len(mine) != 1 || mine[0].event != "TASK_SUCCEEDED" || mine[0].payload.TaskID != "transform" {
		t.Fatalf("deliveries to %s = %+v, want exactly TASK_SUCCEEDED of transform", hook.ID, mine)
	}
	if !webhooks.Verify(hook.Secret, mine[0].body, mine[0].signature) {
		t.Error("the delivery replica B sent does not verify with the secret replica A issued")
	}
	t.Logf("EVIDENCE registered_on=A projected_on=B webhook=%s run=%s event=%s taskId=%s",
		hook.ID, runID, mine[0].event, mine[0].payload.TaskID)
	writeWebhookProofNamed(t, "webhook-two-replica", runID, hook, mine, subscriber.results(runID))
}
