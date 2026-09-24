//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
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
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// webhookOrkflowName is the orkflow the acceptance test runs.
const webhookOrkflowName = "webhook-ork"

// webhookOrkflowYAML is a three-task orkflow assigned to one private pool.
// Pinning matters: the shared execution channel is polled by every other worker
// on the machine, and a stolen activity would look like a webhook bug
// (AGENTS.md §11.1).
func webhookOrkflowYAML(pool string) string {
	return fmt.Sprintf(`name: %s
version: "1.0.0"
description: Three tasks whose run transitions must reach a registered webhook.
tasks:
  - id: fetch
    uses: http
    pool: %q
    with:
      op: get
  - id: transform
    uses: shell
    needs: [fetch]
    pool: %q
    with:
      op: exec
  - id: save
    uses: file
    needs: [transform]
    pool: %q
    with:
      op: copy
`, webhookOrkflowName, pool, pool, pool)
}

// deliveryOrder ranks a delivery by the transition it reports, so the
// assertions read a run's transitions in lifecycle order however the
// concurrent deliveries happened to land.
func deliveryOrder(d receivedDelivery) int {
	for i, event := range webhooks.KnownEvents() {
		if string(event) == d.event {
			return i
		}
	}
	return len(webhooks.KnownEvents())
}

// TestIntegration_WebhookFiresOnRunStateTransitions is the webhook acceptance
// on a real cluster: an external system registers a callback through the
// engine's own API, a run executes on real Temporal, and the engine POSTs a
// signed payload for each of that run's state transitions.
//
// Nothing here is stubbed on the webhook path: the registration goes through
// the HTTP endpoint, the delivery goes through the real dispatcher, and the
// receiver verifies the HMAC with the secret registration handed it.
func TestIntegration_WebhookFiresOnRunStateTransitions(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()

	// A private task queue keeps this run's activities away from any other
	// worker polling the shared queue (AGENTS.md §11.1).
	pool := fmt.Sprintf("webhook-%d", time.Now().UnixNano())
	runQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map webhook pool: %v", err)
	}

	subscriber := newWebhookSubscriber(t)

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	registry := webhooks.NewRegistry()
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{})
	notifier.Observe(subscriber.record)
	runNotifier := api.NewWebhookNotifier(notifier, nil)

	runWorker := worker.New(temporalClient, runQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	taskWorker := worker.New(temporalClient, runQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	taskWorker.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{
				TaskId:     in.TaskId,
				ValuesJson: valuesJSON(map[string]any{"echoed_task": in.TaskId}),
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)

	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithRunNotifier(runNotifier).WithSnapshotStore(snapshots).PublishEvent,
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

	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     runs,
		Bus:      bus,
		Starter:  engine.NewTemporalStarter(temporalClient, runQueue),
		Webhooks: registry,
		Notifier: runNotifier,
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

	// 1. Acceptance 1: an external system registers a callback.
	registered := registerWebhookOverHTTP(t, httpClient, origin, map[string]any{
		"url":    subscriber.server.URL,
		"events": []string{"RUN_STARTED", "RUN_SUCCEEDED"},
	})
	if registered.Secret == "" {
		t.Fatal("registration disclosed no signing secret")
	}
	t.Logf("registered webhook %s -> %s for %v", registered.ID, registered.URL, registered.Events)

	// 2. Acceptance 2: it is listed back, without its secret.
	listed := listWebhooksOverHTTP(t, httpClient, origin)
	if len(listed) != 1 || listed[0].ID != registered.ID {
		t.Fatalf("webhook list = %+v, want the registered callback", listed)
	}
	if listed[0].Secret != "" {
		t.Error("the list disclosed the signing secret")
	}

	// 3. A real run on a real cluster. Every task names the private queue, so
	// no other worker on the shared queue can steal this run's activities
	// (AGENTS.md §11.1).
	orkflowClient := orkstackv1connect.NewOrkflowServiceClient(httpClient, origin)
	runClient := orkstackv1connect.NewRunServiceClient(httpClient, origin)
	if _, err := orkflowClient.ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: webhookOrkflowYAML(pool)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: webhookOrkflowName}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	t.Logf("submitted run %s against %s", runID, origin)

	stream, err := runClient.StreamEvents(ctx, connect.NewRequest(
		&orkstackv1.StreamEventsRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	for stream.Receive() { //nolint:revive // draining the stream is the wait
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream error: %v", err)
	}
	stream.Close()

	// The run must actually have succeeded. Saying so here turns a stolen
	// activity (AGENTS.md §11.1) into a diagnosis instead of a webhook mystery.
	got, err := runClient.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if status := got.Msg.GetRun().GetStatus(); status != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s ended %v (%v), want SUCCEEDED", runID, status, got.Msg.GetRun().GetError())
	}

	// 4. Acceptance 3 and 4: the transitions arrived, signed, with the
	// contracted payload.
	subscriber.awaitCount(t, runID, 2)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer drainCancel()
	if err := notifier.Wait(drainCtx); err != nil {
		t.Fatalf("deliveries did not settle: %v", err)
	}

	deliveries := subscriber.deliveries(runID)
	if len(deliveries) != 2 {
		t.Fatalf("received %d deliveries, want RUN_STARTED then RUN_SUCCEEDED: %+v",
			len(deliveries), deliveries)
	}
	wantEvents := []string{"RUN_STARTED", "RUN_SUCCEEDED"}
	wantStatus := []string{"RUNNING", "SUCCEEDED"}
	for i, got := range deliveries {
		if got.event != wantEvents[i] {
			t.Errorf("delivery %d event = %q, want %q", i+1, got.event, wantEvents[i])
		}
		if !webhooks.Verify(registered.Secret, got.body, got.signature) {
			t.Errorf("delivery %d signature %q does not verify against its body",
				i+1, got.signature)
		}
		if got.webhookID != registered.ID {
			t.Errorf("delivery %d names webhook %q, want %q", i+1, got.webhookID, registered.ID)
		}
		if got.payload.RunID != runID {
			t.Errorf("delivery %d runId = %q, want %q", i+1, got.payload.RunID, runID)
		}
		if got.payload.OrkflowName != webhookOrkflowName {
			t.Errorf("delivery %d orkflowName = %q, want %q",
				i+1, got.payload.OrkflowName, webhookOrkflowName)
		}
		if got.payload.Status != wantStatus[i] {
			t.Errorf("delivery %d status = %q, want %q", i+1, got.payload.Status, wantStatus[i])
		}
		if got.payload.Timestamp == "" {
			t.Errorf("delivery %d carries no timestamp", i+1)
		}
		if got.payload.Run.ID != runID {
			t.Errorf("delivery %d embeds run %q, want %q", i+1, got.payload.Run.ID, runID)
		}
		t.Logf("delivery %d %s status=%s attempt=%s signature=%s…",
			i+1, got.event, got.payload.Status, got.attempt, truncate(got.signature, 24))
	}
	if deliveries[1].payload.Run.Status != "SUCCEEDED" {
		t.Errorf("the terminal delivery embeds run status %q, want SUCCEEDED",
			deliveries[1].payload.Run.Status)
	}
	if len(deliveries[1].payload.Run.TaskRuns) != 3 {
		t.Errorf("the terminal delivery embeds %d task runs, want 3",
			len(deliveries[1].payload.Run.TaskRuns))
	}

	// 5. A run's transitions reach only the subscribers that asked for them.
	results := subscriber.results(runID)
	for _, result := range results {
		if !result.Delivered {
			t.Errorf("delivery %s to %s never landed: %+v", result.DeliveryID, result.URL, result.Attempts)
		}
		if len(result.Attempts) != 1 {
			t.Errorf("delivery %s took %d attempts against a healthy receiver",
				result.DeliveryID, len(result.Attempts))
		}
	}

	writeWebhookProof(t, runID, registered, deliveries, results)
}

// TestIntegration_WebhookRetriesUntilTheReceiverAccepts is the retry policy on
// the real path: a receiver that rejects the first two deliveries with 503 still
// gets the run, after exponential backoff.
func TestIntegration_WebhookRetriesUntilTheReceiverAccepts(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	var (
		mu       sync.Mutex
		attempts []string
		accepted = make(chan struct{}, 1)
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts = append(attempts, r.Header.Get(webhooks.AttemptHeader))
		count := len(attempts)
		mu.Unlock()
		if count < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		select {
		case accepted <- struct{}{}:
		default:
		}
	}))
	defer receiver.Close()

	registry := webhooks.NewRegistry()
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{})
	hook, err := registry.Register(webhooks.Registration{
		URL:    receiver.URL,
		Events: []webhooks.Event{webhooks.EventRunSucceeded},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	var settled webhooks.Result
	var settledMu sync.Mutex
	notifier.Observe(func(result webhooks.Result) {
		settledMu.Lock()
		settled = result
		settledMu.Unlock()
	})

	notifier.Notify(context.Background(), webhooks.Delivery{
		Event:       webhooks.EventRunSucceeded,
		RunID:       "ork-1755302400123456789-1",
		OrkflowName: "hello-ork",
		Status:      "SUCCEEDED",
		Timestamp:   time.Now().UTC(),
		Run:         json.RawMessage(`{"id":"ork-1755302400123456789-1","status":"SUCCEEDED"}`),
	})

	select {
	case <-accepted:
	case <-time.After(30 * time.Second):
		t.Fatal("the receiver never accepted the delivery")
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := notifier.Wait(drainCtx); err != nil {
		t.Fatalf("delivery did not settle: %v", err)
	}

	mu.Lock()
	got := append([]string(nil), attempts...)
	mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("receiver saw %d requests, want 3: two 503s then the acceptance", len(got))
	}
	for i, attempt := range got {
		if attempt != strconv.Itoa(i+1) {
			t.Errorf("request %d reported attempt %q", i+1, attempt)
		}
	}

	settledMu.Lock()
	defer settledMu.Unlock()
	if !settled.Delivered {
		t.Errorf("result %+v does not record the accepted delivery", settled)
	}
	if settled.WebhookID != hook.ID {
		t.Errorf("result names webhook %q, want %q", settled.WebhookID, hook.ID)
	}
	if len(settled.Attempts) != 3 {
		t.Fatalf("result records %d attempts, want 3", len(settled.Attempts))
	}
	if settled.Attempts[1].Backoff != webhooks.DefaultInitialBackoff {
		t.Errorf("first retry backed off %v, want %v",
			settled.Attempts[1].Backoff, webhooks.DefaultInitialBackoff)
	}
	if settled.Attempts[2].Backoff != 2*webhooks.DefaultInitialBackoff {
		t.Errorf("second retry backed off %v, want %v (exponential)",
			settled.Attempts[2].Backoff, 2*webhooks.DefaultInitialBackoff)
	}
	t.Logf("delivery %s landed on attempt 3 after backing off %v then %v",
		settled.DeliveryID, settled.Attempts[1].Backoff, settled.Attempts[2].Backoff)
}

// ---------------------------------------------------------------------------
// subscriber
// ---------------------------------------------------------------------------

// receivedDelivery is one POST as the subscriber saw it.
type receivedDelivery struct {
	event      string
	taskHeader string
	webhookID  string
	deliveryID string
	attempt    string
	signature  string
	body       []byte
	payload    webhookPayload
}

// webhookPayload is the contracted body: {runId, orkflowName, status,
// timestamp, [taskId, attempt,] run}. taskId and attempt appear only on a task
// outcome delivery.
type webhookPayload struct {
	RunID       string `json:"runId"`
	OrkflowName string `json:"orkflowName"`
	Status      string `json:"status"`
	Timestamp   string `json:"timestamp"`
	TaskID      string `json:"taskId"`
	Attempt     int32  `json:"attempt"`
	Run         struct {
		ID        string `json:"id"`
		Namespace string `json:"namespace"`
		Status    string `json:"status"`
		TaskRuns  []struct {
			TaskID    string          `json:"taskId"`
			Status    string          `json:"status"`
			StartedAt string          `json:"startedAt"`
			ValuesRef json.RawMessage `json:"valuesRef"`
		} `json:"taskRuns"`
	} `json:"run"`
}

// webhookSubscriber is the external system: an HTTP endpoint that records
// every delivery it is sent.
type webhookSubscriber struct {
	server *httptest.Server

	mu       sync.Mutex
	received []receivedDelivery
	settled  []webhooks.Result
	signal   chan struct{}
}

func newWebhookSubscriber(t *testing.T) *webhookSubscriber {
	t.Helper()
	s := &webhookSubscriber{signal: make(chan struct{}, 32)}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var payload webhookPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.received = append(s.received, receivedDelivery{
			event:      r.Header.Get(webhooks.EventHeader),
			taskHeader: r.Header.Get(webhooks.TaskHeader),
			webhookID:  r.Header.Get(webhooks.WebhookHeader),
			deliveryID: r.Header.Get(webhooks.DeliveryHeader),
			attempt:    r.Header.Get(webhooks.AttemptHeader),
			signature:  r.Header.Get(webhooks.SignatureHeader),
			body:       body,
			payload:    payload,
		})
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		select {
		case s.signal <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *webhookSubscriber) record(result webhooks.Result) {
	s.mu.Lock()
	s.settled = append(s.settled, result)
	s.mu.Unlock()
}

// deliveries returns what arrived for one run, in lifecycle order. Filtering
// by run matters: another test's orphaned workflow can still be scheduling
// projection activities on the shared event queue, and its transitions would
// otherwise be counted as this run's.
func (s *webhookSubscriber) deliveries(runID string) []receivedDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]receivedDelivery, 0, len(s.received))
	for _, got := range s.received {
		if got.payload.RunID == runID {
			out = append(out, got)
		}
	}
	// Transitions can settle on different goroutines; order them the way the
	// run produced them.
	sort.SliceStable(out, func(i, j int) bool {
		return deliveryOrder(out[i]) < deliveryOrder(out[j])
	})
	return out
}

// results returns the dispatcher's record for one run's deliveries.
func (s *webhookSubscriber) results(runID string) []webhooks.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]webhooks.Result, 0, len(s.settled))
	for _, result := range s.settled {
		if result.RunID == runID {
			out = append(out, result)
		}
	}
	return out
}

// awaitCount waits until want deliveries for runID have arrived.
func (s *webhookSubscriber) awaitCount(t *testing.T, runID string, want int) {
	t.Helper()
	deadline := time.After(60 * time.Second)
	for len(s.deliveries(runID)) < want {
		select {
		case <-s.signal:
		case <-deadline:
			t.Fatalf("received %d deliveries for run %s, want %d",
				len(s.deliveries(runID)), runID, want)
		}
	}
}

// ---------------------------------------------------------------------------
// management API helpers
// ---------------------------------------------------------------------------

func registerWebhookOverHTTP(
	t *testing.T,
	httpClient *http.Client,
	origin string,
	body map[string]any,
) webhooks.Webhook {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode registration: %v", err)
	}
	resp := doWebhookRequest(t, httpClient, http.MethodPost, origin+api.WebhookCollectionPath, encoded)
	var envelope struct {
		Webhook webhooks.Webhook `json:"webhook"`
	}
	if err := json.Unmarshal(resp, &envelope); err != nil {
		t.Fatalf("decode registration: %v (%s)", err, resp)
	}
	return envelope.Webhook
}

func listWebhooksOverHTTP(t *testing.T, httpClient *http.Client, origin string) []webhooks.Webhook {
	t.Helper()
	resp := doWebhookRequest(t, httpClient, http.MethodGet, origin+api.WebhookCollectionPath, nil)
	var envelope struct {
		Webhooks []webhooks.Webhook `json:"webhooks"`
	}
	if err := json.Unmarshal(resp, &envelope); err != nil {
		t.Fatalf("decode list: %v (%s)", err, resp)
	}
	return envelope.Webhooks
}

func doWebhookRequest(t *testing.T, httpClient *http.Client, method, url string, body []byte) []byte {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, url, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read response: %v", method, url, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("%s %s: status %d: %s", method, url, resp.StatusCode, payload)
	}
	return payload
}

func truncate(value string, size int) string {
	if len(value) <= size {
		return value
	}
	return value[:size]
}

// writeWebhookProof records the registration, the signed payloads the receiver
// saw, and the delivery log as evidence.
func writeWebhookProof(
	t *testing.T,
	runID string,
	hook webhooks.Webhook,
	deliveries []receivedDelivery,
	results []webhooks.Result,
) {
	t.Helper()
	writeWebhookProofNamed(t, "webhook", runID, hook, deliveries, results)
}

// writeWebhookProofNamed is writeWebhookProof under its own file prefix, so
// each acceptance test keeps its own transcript rather than overwriting
// another's.
func writeWebhookProofNamed(
	t *testing.T,
	prefix string,
	runID string,
	hook webhooks.Webhook,
	deliveries []receivedDelivery,
	results []webhooks.Result,
) {
	t.Helper()
	// The registration, with the secret redacted to a fingerprint: the proof
	// must be reproducible without leaking a live signing key.
	registration := map[string]any{
		"id":     hook.ID,
		"url":    hook.URL,
		"events": hook.Events,
		"scope": map[string]string{
			"namespace": hook.Namespace, "orkflowName": hook.OrkflowName, "taskId": hook.TaskID,
		},
		"createdAt": hook.CreatedAt.UTC().Format(time.RFC3339Nano),
		"secret":    "<redacted, " + strconv.Itoa(len(hook.Secret)) + " hex chars>",
	}
	proof.WriteJSON(t, "engine", prefix+"-registration.json", registration)

	type deliveryProof struct {
		Event      string          `json:"event"`
		TaskHeader string          `json:"taskIdHeader,omitempty"`
		WebhookID  string          `json:"webhookId"`
		DeliveryID string          `json:"deliveryId"`
		Attempt    string          `json:"attempt"`
		Signature  string          `json:"signature"`
		Verified   bool            `json:"signatureVerified"`
		Body       json.RawMessage `json:"body"`
	}
	proofs := make([]deliveryProof, 0, len(deliveries))
	for _, got := range deliveries {
		proofs = append(proofs, deliveryProof{
			Event:      got.event,
			TaskHeader: got.taskHeader,
			WebhookID:  got.webhookID,
			DeliveryID: got.deliveryID,
			Attempt:    got.attempt,
			Signature:  got.signature,
			Verified:   webhooks.Verify(hook.Secret, got.body, got.signature),
			Body:       json.RawMessage(got.body),
		})
	}
	proof.WriteJSON(t, "engine", prefix+"-signed-payloads.json", map[string]any{
		"runId":      runID,
		"captured":   time.Now().UTC().Format(time.RFC3339),
		"deliveries": proofs,
	})
	proof.WriteJSON(t, "engine", prefix+"-delivery-log.json", map[string]any{
		"runId":    runID,
		"captured": time.Now().UTC().Format(time.RFC3339),
		"results":  results,
	})
}
