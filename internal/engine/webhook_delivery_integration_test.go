//go:build integration

package engine_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// deliveryReplica is the delivery half of one engine replica as `ork` wires
// it: the run worker hosting WebhookDeliveryWorkflow and the engine-channel
// worker running its activities, with a notifier whose settled results it
// collects.
type deliveryReplica struct {
	notifier *webhooks.Notifier
	workers  []worker.Worker

	mu      sync.Mutex
	settled []webhooks.Result
}

func startDeliveryReplica(t *testing.T, c client.Client, runQueue string, bodies artifacts.DeliveryBodyStore,
	registry *webhooks.Registry, policy engine.WebhookDeliveryPolicy,
) *deliveryReplica {
	t.Helper()
	r := &deliveryReplica{}
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{})
	r.notifier = webhooks.NewNotifier(registry, dispatcher, webhooks.NotifierOptions{
		Launcher: engine.NewWebhookLauncher(c, runQueue, bodies, policy),
	})
	r.notifier.Observe(func(result webhooks.Result) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.settled = append(r.settled, result)
	})
	runWorker := worker.New(c, runQueue, worker.Options{DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true})
	engine.RegisterWorkflows(runWorker)
	eventWorker := worker.New(c, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	engine.RegisterWebhookDeliveryActivities(eventWorker, engine.NewWebhookDeliveryActivities(bodies, registry, dispatcher, r.notifier, nil))
	for _, w := range []worker.Worker{runWorker, eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start worker: %v", err)
		}
		r.workers = append(r.workers, w)
	}
	t.Cleanup(r.stop)
	return r
}

// stop stops the replica's workers; a worker stops once its running activity
// returns. Stopping twice is a no-op.
func (r *deliveryReplica) stop() {
	r.mu.Lock()
	workers := r.workers
	r.workers = nil
	r.mu.Unlock()
	for _, w := range workers {
		w.Stop()
	}
}

// awaitSettled waits for the replica to settle one delivery.
func (r *deliveryReplica) awaitSettled(t *testing.T, ctx context.Context) webhooks.Result {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		r.mu.Lock()
		if len(r.settled) > 0 {
			result := r.settled[0]
			r.mu.Unlock()
			return result
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			t.Fatal("no delivery settled before the deadline")
		case <-ticker.C:
		}
	}
}

// deliveryFixtureRun is the delivery every test here makes: one run
// transition to one subscriber.
func deliveryFixtureRun(runID string) webhooks.Delivery {
	return webhooks.Delivery{
		Event: webhooks.EventRunSucceeded, RunID: runID, OrkflowName: "hello-ork", Status: "SUCCEEDED",
		Timestamp: time.Now().UTC(), Namespace: "default",
		Run: stablejson.RawJSON(`{"id":"` + runID + `","status":"SUCCEEDED"}`),
	}
}

// TestIntegration_WebhookRetriesUntilTheReceiverAccepts is the retry policy on
// the real path: a receiver that rejects the first two attempts with 503 still
// gets the delivery, Temporal spacing the retries by the pinned ladder, and
// the one record holds all three attempts.
func TestIntegration_WebhookRetriesUntilTheReceiverAccepts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := scopedCluster(t, ctx)
	receiver := newDeliveryReceiver(t, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK)
	registry := webhooks.NewRegistry()
	hook, err := registry.Register(webhooks.Registration{URL: receiver.server.URL, Events: []webhooks.Event{webhooks.EventRunSucceeded}})
	if err != nil {
		t.Fatal(err)
	}
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	replica := startDeliveryReplica(t, c, uniquePool("webhook-delivery"), bodies, registry, engine.DefaultWebhookDeliveryPolicy())
	replica.notifier.Notify(ctx, deliveryFixtureRun("ork-webhook-retry"))
	settled := replica.awaitSettled(t, ctx)

	requests, headers := receiver.requests()
	if len(requests) != 3 || !settled.Delivered || len(settled.Attempts) != 3 || settled.WebhookID != hook.ID {
		t.Fatalf("receiver saw %d requests; settled %+v; want delivered on attempt 3", len(requests), settled)
	}
	for i := range headers {
		if got := headers[i].Get(webhooks.AttemptHeader); got != strconv.Itoa(i+1) {
			t.Errorf("request %d reported attempt %q", i+1, got)
		}
	}
	// Temporal waited out the ladder between attempts: at least 100ms, then
	// at least 200ms.
	if settled.Attempts[1].Backoff < webhooks.DefaultInitialBackoff || settled.Attempts[2].Backoff < 2*webhooks.DefaultInitialBackoff {
		t.Errorf("backoffs = %v, %v; want at least %v and %v", settled.Attempts[1].Backoff, settled.Attempts[2].Backoff,
			webhooks.DefaultInitialBackoff, 2*webhooks.DefaultInitialBackoff)
	}
	if _, err := bodies.GetDeliveryBody(ctx, settled.DeliveryID, settled.BodySHA256); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("the settled delivery's body = %v, want it deleted", err)
	}
	t.Logf("EVIDENCE delivery %s landed on attempt 3 after backing off %v then %v",
		settled.DeliveryID, settled.Attempts[1].Backoff, settled.Attempts[2].Backoff)
}

// TestIntegration_WebhookDeliveryOutlivesTheReplicaThatQueuedIt: the replica
// that queued a delivery and made its first attempt stops before the retry;
// another replica, which never saw the transition, makes the retry and
// records both attempts. A crash between attempts no longer loses the
// delivery's remaining retries.
func TestIntegration_WebhookDeliveryOutlivesTheReplicaThatQueuedIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := scopedCluster(t, ctx)
	receiver := newDeliveryReceiver(t, http.StatusServiceUnavailable, http.StatusOK)
	registry := webhooks.NewRegistry()
	if _, err := registry.Register(webhooks.Registration{URL: receiver.server.URL}); err != nil {
		t.Fatal(err)
	}
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A pause long enough to stop the first replica inside it.
	policy := engine.DefaultWebhookDeliveryPolicy()
	policy.InitialInterval = 3 * time.Second
	queue := uniquePool("webhook-outlive")
	first := startDeliveryReplica(t, c, queue, bodies, registry, policy)
	first.notifier.Notify(ctx, deliveryFixtureRun("ork-webhook-outlive"))
	for {
		if requests, _ := receiver.requests(); len(requests) == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the first attempt never reached the receiver")
		case <-time.After(20 * time.Millisecond):
		}
	}
	first.stop()
	if requests, _ := receiver.requests(); len(requests) != 1 {
		t.Fatalf("the first replica made %d attempts before it stopped, want 1", len(requests))
	}
	second := startDeliveryReplica(t, c, queue, bodies, registry, policy)
	settled := second.awaitSettled(t, ctx)
	if !settled.Delivered || len(settled.Attempts) != 2 || settled.Attempts[0].StatusCode != http.StatusServiceUnavailable ||
		settled.Attempts[1].Number != 2 {
		t.Fatalf("settled %+v, want the first replica's 503 and the second's acceptance", settled)
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	if len(first.settled) != 0 {
		t.Errorf("the stopped replica settled %d deliveries", len(first.settled))
	}
	t.Logf("EVIDENCE delivery %s: attempt 1 on the stopped replica (503), attempt 2 on another after %v (200)",
		settled.DeliveryID, settled.Attempts[1].Backoff)
}

// TestIntegration_RecordWebhookDeliveryHistory records the delivery
// workflow's fixture from a REAL delivery on a real cluster: a receiver that
// answers 503 then 200, so the history holds one retried send and a settle.
// By default the recording lands under ORK_PROOF_DIR; only
// ORK_RECORD_GOLDEN_HISTORY=1 also writes the committed fixture. It FAILS
// unless the recording replays against current code.
func TestIntegration_RecordWebhookDeliveryHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := scopedCluster(t, ctx)
	receiver := newDeliveryReceiver(t, http.StatusServiceUnavailable, http.StatusOK)
	registry := webhooks.NewRegistry()
	if _, err := registry.Register(webhooks.Registration{URL: receiver.server.URL, Secret: "fixture-secret"}); err != nil {
		t.Fatal(err)
	}
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	replica := startDeliveryReplica(t, c, uniquePool("webhook-fixture"), bodies, registry, engine.DefaultWebhookDeliveryPolicy())
	replica.notifier.Notify(ctx, deliveryFixtureRun("ork-webhook-fixture"))
	settled := replica.awaitSettled(t, ctx)

	run := c.GetWorkflow(ctx, settled.DeliveryID, "")
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("the delivery workflow ended %v", err)
	}
	recorded := &historypb.History{}
	iter := c.GetWorkflowHistory(ctx, settled.DeliveryID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		recorded.Events = append(recorded.Events, event)
	}
	if err := engine.ReplayWebhookDeliveryHistory(recorded); err != nil {
		t.Fatalf("the history just recorded does not replay against current code: %v", err)
	}
	encoded := encodeHistory(t, recorded)
	proof.Write(t, "webhook-delivery", filepath.Base(webhookDeliveryFixture), encoded)
	written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
		if err := os.WriteFile(webhookDeliveryFixture, encoded, 0o644); err != nil {
			t.Fatalf("write %s: %v", webhookDeliveryFixture, err)
		}
		written = webhookDeliveryFixture
	}
	t.Logf("recorded %d events of delivery %s into %s, replayed clean", len(recorded.GetEvents()), settled.DeliveryID, written)
}
