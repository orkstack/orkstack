package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// deliveryReceiver answers each request with the next status, then repeats
// the last, and keeps every request.
type deliveryReceiver struct {
	mu       sync.Mutex
	statuses []int
	bodies   []string
	headers  []http.Header
	server   *httptest.Server
}

func newDeliveryReceiver(t *testing.T, statuses ...int) *deliveryReceiver {
	t.Helper()
	r := &deliveryReceiver{statuses: statuses}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		i := len(r.bodies)
		r.bodies = append(r.bodies, string(body))
		r.headers = append(r.headers, req.Header.Clone())
		status := r.statuses[min(i, len(r.statuses)-1)]
		r.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *deliveryReceiver) requests() ([]string, []http.Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.bodies...), append([]http.Header{}, r.headers...)
}

// deliveryHarness is one delivery workflow with its real activities, a local
// body store, an in-memory registry, and a notifier whose record and
// observers it watches.
type deliveryHarness struct {
	env      *testsuite.TestWorkflowEnvironment
	bodies   *artifacts.BlobDeliveryBodyStore
	registry *webhooks.Registry
	options  *activityOptionsCapture

	mu        sync.Mutex
	recorded  []webhooks.Result
	observed  []webhooks.Result
	failWrite int
}

func newDeliveryHarness(t *testing.T) *deliveryHarness {
	t.Helper()
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bodies.Close() })
	h := &deliveryHarness{bodies: bodies, registry: webhooks.NewRegistry(), options: &activityOptionsCapture{}}
	notifier := webhooks.NewNotifier(h.registry, nil, webhooks.NotifierOptions{
		Record: func(_ context.Context, result webhooks.Result) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.failWrite > 0 {
				h.failWrite--
				return errors.New("the store is down")
			}
			h.recorded = append(h.recorded, result)
			return nil
		},
	})
	notifier.Observe(func(result webhooks.Result) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.observed = append(h.observed, result)
	})
	var suite testsuite.WorkflowTestSuite
	h.env = suite.NewTestWorkflowEnvironment()
	h.env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{h.options}})
	h.env.RegisterWorkflowWithOptions(engine.WebhookDeliveryWorkflow, workflow.RegisterOptions{Name: engine.WebhookDeliveryWorkflowType})
	activities := engine.NewWebhookDeliveryActivities(bodies, h.registry, nil, notifier, nil)
	h.env.RegisterActivityWithOptions(activities.SendWebhookAttempt, activity.RegisterOptions{Name: engine.ActivitySendWebhookAttempt})
	h.env.RegisterActivityWithOptions(activities.SettleWebhookDelivery, activity.RegisterOptions{Name: engine.ActivitySettleWebhookDelivery})
	return h
}

// queue registers a subscription to url and stores a delivery's body as the
// launcher would, returning the workflow's input.
func (h *deliveryHarness) queue(t *testing.T, url string) (engine.WebhookDeliveryInput, []byte) {
	t.Helper()
	hook, err := h.registry.Register(webhooks.Registration{URL: url, Secret: "s3cr3t"})
	if err != nil {
		t.Fatal(err)
	}
	queuedAt := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	id, err := webhooks.NewDeliveryID(queuedAt)
	if err != nil {
		t.Fatal(err)
	}
	delivery := webhooks.Delivery{
		ID: id, Event: webhooks.EventRunSucceeded, RunID: "ork-1", OrkflowName: "deploy",
		Status: "SUCCEEDED", Timestamp: queuedAt, Namespace: "default", QueuedAt: queuedAt,
		Sequence: 3, Run: []byte(`{"id":"ork-1"}`),
	}
	body, err := delivery.Body()
	if err != nil {
		t.Fatal(err)
	}
	sum, err := h.bodies.PutDeliveryBody(context.Background(), id, body)
	if err != nil {
		t.Fatal(err)
	}
	delivery.Run = nil
	return engine.WebhookDeliveryInput{
		Delivery: delivery, WebhookID: hook.ID, URL: hook.URL,
		BodySHA256: sum, BodyBytes: len(body), Policy: engine.DefaultWebhookDeliveryPolicy(),
	}, body
}

func (h *deliveryHarness) run(t *testing.T, in engine.WebhookDeliveryInput) webhooks.Result {
	t.Helper()
	h.env.ExecuteWorkflow(engine.WebhookDeliveryWorkflowType, in)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the delivery workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the delivery workflow failed: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.recorded) != 1 || len(h.observed) != 1 {
		t.Fatalf("recorded %d and observed %d results, want one of each", len(h.recorded), len(h.observed))
	}
	return h.recorded[0]
}

// activityOptionsCapture records the options each activity was scheduled with.
type activityOptionsCapture struct {
	interceptor.WorkerInterceptorBase
	mu      sync.Mutex
	options map[string]workflow.ActivityOptions
}

func (c *activityOptionsCapture) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &optionsInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, capture: c}
}

type optionsInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	capture *activityOptionsCapture
}

func (i *optionsInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&optionsOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, capture: i.capture})
}

type optionsOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	capture *activityOptionsCapture
}

func (o *optionsOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	o.capture.mu.Lock()
	if o.capture.options == nil {
		o.capture.options = map[string]workflow.ActivityOptions{}
	}
	o.capture.options[activityType] = workflow.GetActivityOptions(ctx)
	o.capture.mu.Unlock()
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

// TestWebhookDelivery_TemporalRetriesA5xxUntilTheReceiverAccepts: the POST is
// retried by Temporal under the pinned ladder, every attempt sends the same
// signed body under the same delivery id and says which try it is, and the
// one record holds every attempt.
func TestWebhookDelivery_TemporalRetriesA5xxUntilTheReceiverAccepts(t *testing.T) {
	h := newDeliveryHarness(t)
	receiver := newDeliveryReceiver(t, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusNoContent)
	in, body := h.queue(t, receiver.server.URL)
	result := h.run(t, in)

	if !result.Delivered || len(result.Attempts) != 3 {
		t.Fatalf("result = %+v, want delivered after 3 attempts", result)
	}
	bodies, headers := receiver.requests()
	if len(bodies) != 3 {
		t.Fatalf("receiver saw %d requests, want 3", len(bodies))
	}
	for i := range bodies {
		if bodies[i] != string(body) {
			t.Errorf("attempt %d changed the body, which would invalidate the signature", i+1)
		}
		if !webhooks.Verify("s3cr3t", []byte(bodies[i]), headers[i].Get(webhooks.SignatureHeader)) {
			t.Errorf("attempt %d is not signed with the subscription's secret", i+1)
		}
		if got := headers[i].Get(webhooks.AttemptHeader); got != strconv.Itoa(i+1) {
			t.Errorf("attempt %d reported attempt %q", i+1, got)
		}
		if got := headers[i].Get(webhooks.DeliveryHeader); got != in.Delivery.ID {
			t.Errorf("attempt %d changed the delivery id to %q", i+1, got)
		}
		if got := headers[i].Get(webhooks.SequenceHeader); got != "3" {
			t.Errorf("attempt %d sent sequence %q, want the event's 3, carried through the workflow's input", i+1, got)
		}
		if result.Attempts[i].Number != i+1 {
			t.Errorf("recorded attempt %d numbered %d", i+1, result.Attempts[i].Number)
		}
	}
	if result.BodySHA256 != in.BodySHA256 || result.Signature == "" || result.SettledAt.IsZero() || result.Sequence != 3 {
		t.Errorf("record = %+v, want the body digest, the signature, a settled instant and the event's sequence 3", result)
	}
	// The ladder is Temporal's retry policy, read from the delivery's pinned
	// numbers, on the engine's own channel.
	h.options.mu.Lock()
	send := h.options.options[engine.ActivitySendWebhookAttempt]
	h.options.mu.Unlock()
	policy := send.RetryPolicy
	if policy == nil || policy.MaximumAttempts != 4 || policy.InitialInterval != 100*time.Millisecond ||
		policy.BackoffCoefficient != 2 || send.TaskQueue != engine.EventQueue {
		t.Errorf("send scheduled with %+v (retry %+v), want 4 attempts from 100ms ×2 on %s", send, policy, engine.EventQueue)
	}
	// Settled: the body is gone.
	if _, err := h.bodies.GetDeliveryBody(context.Background(), in.Delivery.ID, in.BodySHA256); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("the body after settling = %v, want it deleted", err)
	}
}

// A receiver that only ever answers 5xx gets the four requests the policy
// allows, and the record says it was not delivered.
func TestWebhookDelivery_GivesUpAfterTheLastAttemptThePolicyAllows(t *testing.T) {
	h := newDeliveryHarness(t)
	receiver := newDeliveryReceiver(t, http.StatusInternalServerError)
	in, _ := h.queue(t, receiver.server.URL)
	result := h.run(t, in)
	bodies, _ := receiver.requests()
	if result.Delivered || len(result.Attempts) != 4 || len(bodies) != 4 {
		t.Fatalf("result = %+v after %d requests, want 4 undelivered attempts", result, len(bodies))
	}
	for i, attempt := range result.Attempts {
		if attempt.StatusCode != http.StatusInternalServerError || attempt.Number != i+1 {
			t.Errorf("attempt %d = %+v", i+1, attempt)
		}
	}
}

// A 4xx is the sender's fault: no retry.
func TestWebhookDelivery_NeverRetriesARejectionTheSenderCaused(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound, http.StatusGone} {
		h := newDeliveryHarness(t)
		receiver := newDeliveryReceiver(t, status)
		in, _ := h.queue(t, receiver.server.URL)
		result := h.run(t, in)
		if bodies, _ := receiver.requests(); len(bodies) != 1 || len(result.Attempts) != 1 || result.Delivered {
			t.Errorf("status %d: %d requests, result %+v; want one undelivered attempt", status, len(bodies), result)
		}
	}
}

// A subscription deleted before the attempt is not sent to, and not retried.
func TestWebhookDelivery_SendsNothingForASubscriptionDeletedSinceItWasQueued(t *testing.T) {
	h := newDeliveryHarness(t)
	receiver := newDeliveryReceiver(t, http.StatusOK)
	in, _ := h.queue(t, receiver.server.URL)
	h.registry.Delete(in.WebhookID)
	result := h.run(t, in)
	if bodies, _ := receiver.requests(); len(bodies) != 0 {
		t.Fatalf("a deleted subscription got %d requests", len(bodies))
	}
	if result.Delivered || len(result.Attempts) != 1 || !strings.Contains(result.Attempts[0].Error, "subscription was deleted") {
		t.Errorf("result = %+v, want one attempt naming the deleted subscription", result)
	}
}

// A record that cannot be written is retried by Temporal, and the delivery is
// observed once, after the record exists; the POST is not repeated.
func TestWebhookDelivery_RetriesTheRecordWithoutResendingTheDelivery(t *testing.T) {
	h := newDeliveryHarness(t)
	h.failWrite = 2
	receiver := newDeliveryReceiver(t, http.StatusOK)
	in, _ := h.queue(t, receiver.server.URL)
	result := h.run(t, in)
	if bodies, _ := receiver.requests(); len(bodies) != 1 || !result.Delivered {
		t.Fatalf("%d requests, result %+v; want one delivered request", len(bodies), result)
	}
}

// The record of a delivery whose last attempt never reported: the attempts
// the earlier ones kept, and one row in OrkStack's words — never the
// cluster's error.
func TestWebhookDelivery_RecordsALastAttemptThatNeverReported(t *testing.T) {
	in := engine.WebhookDeliveryInput{
		Delivery:  webhooks.Delivery{ID: "dlv-20261108T120000.000000000Z-0011223344556677", RunID: "ork-1", Sequence: 6},
		WebhookID: "wh-0123456789abcdef", URL: "https://example.test/hook",
		BodySHA256: strings.Repeat("a", 64), BodyBytes: 10, Policy: engine.DefaultWebhookDeliveryPolicy(),
	}
	now := time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC)
	kept := []webhooks.Attempt{{Number: 1, StatusCode: 502, At: now.Add(-time.Second)}}
	timeout := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, nil, kept)
	result := engine.UnsentWebhookResultForTest(in, timeout, now)
	if result.Delivered || len(result.Attempts) != 2 || result.Attempts[0].StatusCode != 502 {
		t.Fatalf("result = %+v, want the kept attempt and one more", result)
	}
	last := result.Attempts[1]
	if last.Number != 2 || !strings.Contains(last.Error, "did not finish within") || strings.Contains(strings.ToLower(last.Error), "activity") {
		t.Errorf("last attempt = %+v, want it numbered 2 and named in OrkStack's words", last)
	}
	other := engine.UnsentWebhookResultForTest(in, errors.New("activity error (type: SendWebhookAttempt)"), now)
	if len(other.Attempts) != 1 || strings.Contains(other.Attempts[0].Error, "SendWebhookAttempt") {
		t.Errorf("result = %+v, want one attempt that names no cluster internals", other)
	}
	if result.SettledAt != now || result.BodySHA256 != in.BodySHA256 || result.Sequence != 6 {
		t.Errorf("result = %+v, want it settled now with the body's digest and the event's sequence 6", result)
	}
}

// TestWebhookLauncher_StartsOneWorkflowPerDeliveryWithoutTheBody: the
// launcher stores the body, starts the delivery's workflow under the delivery
// id on the run channel with the policy pinned, and carries neither the body
// nor the secret in its input.
func TestWebhookLauncher_StartsOneWorkflowPerDeliveryWithoutTheBody(t *testing.T) {
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := &mocks.Client{}
	var (
		options client.StartWorkflowOptions
		input   engine.WebhookDeliveryInput
	)
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, engine.WebhookDeliveryWorkflowType, mock.Anything).
		Run(func(args mock.Arguments) {
			options = args.Get(1).(client.StartWorkflowOptions)
			input = args.Get(3).(engine.WebhookDeliveryInput)
		}).Return(&mocks.WorkflowRun{}, nil)
	policy := engine.DefaultWebhookDeliveryPolicy()
	launcher := engine.NewWebhookLauncher(c, "ork-tasks", bodies, policy)
	delivery := webhooks.Delivery{
		ID: "dlv-20261108T120000.000000000Z-0011223344556677", Event: webhooks.EventRunSucceeded,
		RunID: "ork-1", Sequence: 11, Run: []byte(`{"id":"ork-1","secretlooking":"x"}`),
	}
	hook := webhooks.Webhook{ID: "wh-0123456789abcdef", URL: "https://example.test/hook", Secret: "never-in-history"}
	if err := launcher.Launch(context.Background(), hook, delivery); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if options.ID != delivery.ID || options.TaskQueue != "ork-tasks" ||
		options.WorkflowIDReusePolicy != enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		t.Errorf("started with %+v, want the delivery id on the run channel, never reused", options)
	}
	if input.Delivery.Run != nil || input.URL != hook.URL || input.WebhookID != hook.ID || input.Policy != policy ||
		input.Delivery.Sequence != 11 {
		t.Errorf("input = %+v, want no run, the subscription's id and URL, the pinned policy and the event's sequence 11", input)
	}
	encoded, _ := stablejson.Marshal(input)
	if strings.Contains(string(encoded), hook.Secret) || strings.Contains(string(encoded), "secretlooking") {
		t.Errorf("the workflow input carries the secret or the body: %s", encoded)
	}
	if !strings.Contains(string(encoded), `"Sequence":11`) {
		t.Errorf("the workflow input does not carry the event's sequence 11: %s", encoded)
	}
	body, _ := delivery.Body()
	got, err := bodies.GetDeliveryBody(context.Background(), delivery.ID, input.BodySHA256)
	if err != nil || string(got) != string(body) {
		t.Errorf("stored body = %q, %v; want the signed body", got, err)
	}
	if !strings.Contains(string(got), `"sequence":11`) {
		t.Errorf("stored body = %s, want it to carry the event's sequence 11", got)
	}
}

// A workflow that cannot start leaves no body behind, and the refusal names
// what happened without the cluster's words.
func TestWebhookLauncher_DeletesTheBodyOfADeliveryThatCouldNotStart(t *testing.T) {
	dir := t.TempDir()
	bodies, err := artifacts.NewLocalDeliveryBodyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := &mocks.Client{}
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("workflow execution already started"))
	launcher := engine.NewWebhookLauncher(c, "ork-tasks", bodies, engine.DefaultWebhookDeliveryPolicy())
	delivery := webhooks.Delivery{ID: "dlv-20261108T120000.000000000Z-0011223344556677", RunID: "ork-1", Run: []byte(`{}`)}
	err = launcher.Launch(context.Background(), webhooks.Webhook{ID: "wh-0123456789abcdef", URL: "https://example.test"}, delivery)
	var refused *webhooks.NotQueuedError
	if !errors.As(err, &refused) || strings.Contains(refused.Reason, "workflow") {
		t.Fatalf("Launch = %v, want a NotQueuedError whose reason names no cluster internals", err)
	}
	result := webhooks.NotQueued(webhooks.Webhook{ID: "wh-0123456789abcdef"}, delivery, err)
	if strings.Contains(result.Attempts[0].Error, "workflow") {
		t.Errorf("the record says %q, which names the cluster's internals", result.Attempts[0].Error)
	}
	body, _ := delivery.Body()
	sum := sha256.Sum256(body)
	if _, err := bodies.GetDeliveryBody(context.Background(), delivery.ID, hex.EncodeToString(sum[:])); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("the body of a delivery that never started = %v, want it deleted", err)
	}
}
