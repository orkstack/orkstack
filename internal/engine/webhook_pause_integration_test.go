//go:build integration

package engine_test

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net"
	"net/http"
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
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/webhooktest"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// TestIntegration_WebhookRunPausedUnpaused is rule W5's test for E103's two
// events on a real cluster: two subscriptions, each naming only one of them;
// a run whose first task is held open while the run is paused and unpaused;
// exactly one delivery to each, with its event, a sequence, the status the
// event carries (PAUSED, RUNNING), its pauseTransition and the embedded
// run's status.
func TestIntegration_WebhookRunPausedUnpaused(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: client.DefaultNamespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()

	pool := fmt.Sprintf("webhook-pause-%d", time.Now().UnixNano())
	runQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map webhook pool: %v", err)
	}
	subscriber := newWebhookSubscriber(t)
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	registry := webhooks.NewRegistry(webhooks.WithDestinationPolicy(webhooktest.LoopbackPolicy()))
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: webhooktest.LoopbackPolicy()}),
		webhooks.NotifierOptions{})
	notifier.Observe(subscriber.record)
	runNotifier := api.NewWebhookNotifier(notifier, nil)

	runWorker := worker.New(temporalClient, runQueue, worker.Options{DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true})
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	// fetch is held open until release closes, so the run is paused with
	// work in flight; every other task returns at once.
	release, started := make(chan struct{}), make(chan struct{}, 1)
	taskWorker := worker.New(temporalClient, runQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	taskWorker.RegisterActivityWithOptions(
		func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			if in.TaskId == "fetch" {
				select {
				case started <- struct{}{}:
				default:
				}
				select {
				case <-release:
				case <-actx.Done():
					return nil, actx.Err()
				}
			}
			return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"task": in.TaskId})}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithRunNotifier(runNotifier).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "task": taskWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	starter := engine.NewTemporalStarter(temporalClient, runQueue).WithClusterIdentity(client.DefaultNamespace, "webhook-pause-test")
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Runs: runs, Bus: bus, Starter: starter,
		Webhooks: registry, Notifier: runNotifier,
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
	httpClient := &http.Client{Transport: transport}
	origin := "http://" + listener.Addr().String()
	defer func() {
		transport.CloseIdleConnections()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	pausedHook := registerWebhookOverHTTP(t, httpClient, origin, map[string]any{
		"url": subscriber.server.URL + "/paused", "events": []string{"RUN_PAUSED"}})
	unpausedHook := registerWebhookOverHTTP(t, httpClient, origin, map[string]any{
		"url": subscriber.server.URL + "/unpaused", "events": []string{"RUN_UNPAUSED"}})

	orkflowClient := orkstackv1connect.NewOrkflowServiceClient(httpClient, origin)
	runClient := orkstackv1connect.NewRunServiceClient(httpClient, origin)
	if _, err := orkflowClient.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: webhookOrkflowYAML(pool)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: webhookOrkflowName}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("task fetch never started")
	}
	signal := func(pause bool, id string) {
		t.Helper()
		if err := starter.SignalPause(ctx, runID, &orkstackv1.RunPauseSignal{
			Format: 1, Pause: pause, Principal: "dana@acme", RequestId: id, TimeoutSeconds: 3600,
		}); err != nil {
			t.Fatalf("signal pause=%t: %v", pause, err)
		}
	}
	signal(true, "pause-webhook-1")
	subscriber.awaitCount(t, runID, 1)
	close(release)
	signal(false, "unpause-webhook-1")

	stream, err := runClient.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	for stream.Receive() { //nolint:revive // draining the stream is the wait
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream: %v", err)
	}
	stream.Close()
	subscriber.awaitCount(t, runID, 2)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer drainCancel()
	if err := notifier.Wait(drainCtx); err != nil {
		t.Fatalf("deliveries did not settle: %v", err)
	}

	deliveries := subscriber.deliveries(runID)
	if len(deliveries) != 2 {
		t.Fatalf("received %d deliveries, want one RUN_PAUSED and one RUN_UNPAUSED: %+v", len(deliveries), deliveries)
	}
	want := map[string]struct{ hook, status, transition, runStatus string }{
		"RUN_PAUSED":   {pausedHook.ID, "PAUSED", "PAUSED", "PAUSED"},
		"RUN_UNPAUSED": {unpausedHook.ID, "RUNNING", "UNPAUSED", "RUNNING"},
	}
	for _, got := range deliveries {
		w, ok := want[got.event]
		if !ok {
			t.Fatalf("unexpected delivery %s", got.event)
		}
		delete(want, got.event)
		var body struct {
			Sequence        int64  `json:"sequence"`
			PauseTransition string `json:"pauseTransition"`
		}
		if err := json.Unmarshal(got.body, &body); err != nil {
			t.Fatalf("decode %s: %v", got.event, err)
		}
		if got.webhookID != w.hook || got.payload.Status != w.status || body.PauseTransition != w.transition ||
			body.Sequence <= 0 || got.payload.Run.Status != w.runStatus {
			t.Errorf("%s delivery: hook %s status %s transition %q sequence %d run %s; want hook %s status %s transition %s run %s",
				got.event, got.webhookID, got.payload.Status, body.PauseTransition, body.Sequence, got.payload.Run.Status,
				w.hook, w.status, w.transition, w.runStatus)
		}
		if !webhooks.Verify(map[string]string{pausedHook.ID: pausedHook.Secret, unpausedHook.ID: unpausedHook.Secret}[got.webhookID], got.body, got.signature) {
			t.Errorf("%s delivery's signature does not verify", got.event)
		}
	}
	t.Logf("EVIDENCE webhook_run_paused_unpaused run=%s deliveries=%d", runID, len(deliveries))
}
