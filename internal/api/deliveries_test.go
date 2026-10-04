package api_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// deliveryReceiver is a subscriber whose answer a test can change between
// requests, and which keeps every request it read.
type deliveryReceiver struct {
	mu      sync.Mutex
	status  int
	bodies  [][]byte
	headers []http.Header
	server  *httptest.Server
}

func newDeliveryReceiver(t *testing.T, status int) *deliveryReceiver {
	t.Helper()
	r := &deliveryReceiver{status: status}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.bodies = append(r.bodies, body)
		r.headers = append(r.headers, req.Header.Clone())
		status := r.status
		r.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *deliveryReceiver) answer(status int) {
	r.mu.Lock()
	r.status = status
	r.mu.Unlock()
}

func (r *deliveryReceiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func (r *deliveryReceiver) request(i int) ([]byte, http.Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bodies[i], r.headers[i]
}

// sleepRecorder stands in for the dispatcher's backoff.
type sleepRecorder struct {
	mu    sync.Mutex
	waits []time.Duration
}

func (s *sleepRecorder) sleep(_ context.Context, d time.Duration) error {
	s.mu.Lock()
	s.waits = append(s.waits, d)
	s.mu.Unlock()
	return nil
}

// deliveryFixture is the production shape in one process: a registry, a
// notifier whose Observe writes records through the recorder, and a Service
// reading the same store.
type deliveryFixture struct {
	store    *artifacts.MemoryDeliveryStore
	registry *webhooks.Registry
	spaces   *fakeRegistry
	notifier *webhooks.Notifier
	adapter  *api.WebhookNotifier
	runs     *engine.RunStore
	svc      *api.Service
	receiver *deliveryReceiver
	sleeper  *sleepRecorder
	logs     *lockedBuffer
	now      time.Time
}

func newDeliveryFixture(t *testing.T, receiverStatus int) *deliveryFixture {
	t.Helper()
	f := &deliveryFixture{
		store:    artifacts.NewMemoryDeliveryStore(),
		registry: webhooks.NewRegistry(),
		spaces:   newFakeRegistry(&orkstackv1.Namespace{Name: "payments"}),
		runs:     engine.NewRunStore(),
		receiver: newDeliveryReceiver(t, receiverStatus),
		sleeper:  &sleepRecorder{},
		logs:     &lockedBuffer{},
		// The service's clock runs ahead of the wall clock the notifier
		// stamps deliveries with, so every delivery a test makes falls inside
		// the default window [now-7d, now).
		now: time.Now().UTC().Add(time.Hour),
	}
	logger := slog.New(slog.NewTextHandler(f.logs, nil))
	f.notifier = webhooks.NewNotifier(f.registry,
		webhooks.NewDispatcher(webhooks.DispatcherOptions{Sleep: f.sleeper.sleep}),
		webhooks.NotifierOptions{Logger: logger})
	f.notifier.Observe(api.NewDeliveryRecorder(f.store, logger).Record)
	f.adapter = api.NewWebhookNotifier(f.notifier, logger)
	f.svc = api.NewService(api.Config{
		Runs:       f.runs,
		Namespaces: f.spaces,
		Webhooks:   f.registry,
		Deliveries: f.store,
		Logger:     logger,
		Now:        func() time.Time { return f.now },
	})
	return f
}

func (f *deliveryFixture) register(t *testing.T, events ...webhooks.Event) webhooks.Webhook {
	t.Helper()
	hook, err := f.registry.Register(webhooks.Registration{URL: f.receiver.server.URL, Events: events, Secret: "s3cr3t"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return hook
}

func (f *deliveryFixture) drain(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.notifier.Wait(ctx); err != nil {
		t.Fatalf("deliveries did not settle: %v", err)
	}
}

// sampleRun is a finished run the fixture's RunStore holds.
func (f *deliveryFixture) sampleRun(t *testing.T, id string, status orkstackv1.Status) *orkstackv1.Run {
	t.Helper()
	run := &orkstackv1.Run{
		Id:             id,
		Namespace:      "payments",
		OrkflowName:    "reconcile",
		OrkflowVersion: "0.1.0",
		Status:         status,
		CreatedAt:      timestamppb.New(f.now.Add(-time.Hour)),
		TaskRuns: []*orkstackv1.TaskRun{{
			TaskId: "extract", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 2,
		}},
	}
	f.runs.Save(run)
	return run
}

func (f *deliveryFixture) announce(t *testing.T, run *orkstackv1.Run, eventType orkstackv1.EventType, status orkstackv1.Status, taskID string) {
	t.Helper()
	f.adapter.NotifyRunEvent(context.Background(), &orkstackv1.Event{
		RunId:     run.GetId(),
		Type:      eventType,
		Status:    status,
		TaskId:    taskID,
		Timestamp: timestamppb.New(f.now.Add(-time.Minute)),
	}, run)
	f.drain(t)
}

func (f *deliveryFixture) list(t *testing.T, req *orkstackv1.ListDeliveriesRequest) *orkstackv1.ListDeliveriesResponse {
	t.Helper()
	resp, err := f.svc.ListDeliveries(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("ListDeliveries(%v): %v", req, err)
	}
	return resp.Msg
}

func wantCode(t *testing.T, err error, code connect.Code, what string) {
	t.Helper()
	if connect.CodeOf(err) != code {
		t.Errorf("%s: error = %v, want %s", what, err, code)
	}
}

func TestDeliveryRecorder_WritesOneRecordPerSettledDelivery(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusOK)
	hook := f.register(t, webhooks.EventRunSucceeded)
	run := f.sampleRun(t, "ork-2026-09-21-8f31", orkstackv1.Status_STATUS_SUCCEEDED)
	f.announce(t, run, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, "")

	listed := f.list(t, &orkstackv1.ListDeliveriesRequest{}).GetDeliveries()
	if len(listed) != 1 || f.receiver.count() != 1 {
		t.Fatalf("recorded %d, received %d; want one of each", len(listed), f.receiver.count())
	}
	rec := listed[0]
	body, headers := f.receiver.request(0)
	sum := sha256.Sum256(body)
	for name, pair := range map[string][2]string{
		"id":           {rec.GetId(), headers.Get(webhooks.DeliveryHeader)},
		"webhook_id":   {rec.GetWebhookId(), hook.ID},
		"run_id":       {rec.GetRunId(), run.GetId()},
		"namespace":    {rec.GetNamespace(), "payments"},
		"orkflow_name": {rec.GetOrkflowName(), "reconcile"},
		"url":          {rec.GetUrl(), hook.URL},
		"signature":    {rec.GetSignature(), headers.Get(webhooks.SignatureHeader)},
		"body_sha256":  {rec.GetBodySha256(), hex.EncodeToString(sum[:])},
		"event":        {rec.GetEvent().String(), "EVENT_TYPE_RUN_SUCCEEDED"},
		"status":       {rec.GetStatus().String(), "STATUS_SUCCEEDED"},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s = %q, want %q", name, pair[0], pair[1])
		}
	}
	if rec.GetBodyBytes() != int64(len(body)) || !rec.GetDelivered() || rec.GetSchemaVersion() != 1 {
		t.Errorf("record = %v, want delivered, schema 1, %d body bytes", rec, len(body))
	}
	if !webhooks.Verify("s3cr3t", body, rec.GetSignature()) {
		t.Error("the recorded signature does not verify against the body the receiver read")
	}
	if len(rec.GetAttempts()) != 1 || rec.GetAttempts()[0].GetStatusCode() != 200 {
		t.Errorf("attempts = %v, want one 200", rec.GetAttempts())
	}
	if !rec.GetEventAt().AsTime().Equal(f.now.Add(-time.Minute)) {
		t.Errorf("event_at = %s, want the transition's timestamp", rec.GetEventAt().AsTime())
	}
	if rec.GetCreatedAt() == nil || rec.GetSettledAt() == nil || rec.GetSettledAt().AsTime().Before(rec.GetCreatedAt().AsTime()) {
		t.Errorf("created_at %v / settled_at %v, want both set and ordered", rec.GetCreatedAt(), rec.GetSettledAt())
	}
	// The body is nowhere in the store.
	for key, document := range f.store.Documents() {
		if strings.Contains(string(document), `"orkflowVersion"`) || strings.Contains(string(document), string(body)) {
			t.Errorf("record %s carries the body", key)
		}
	}
}

func TestDeliveryRecorder_RecordsAFailedDeliveryWithEveryAttempt(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusBadGateway)
	f.register(t, webhooks.EventRunFailed)
	run := f.sampleRun(t, "ork-2026-09-21-8f31", orkstackv1.Status_STATUS_FAILED)
	f.announce(t, run, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED, "")

	listed := f.list(t, &orkstackv1.ListDeliveriesRequest{
		Outcome: orkstackv1.DeliveryOutcome_DELIVERY_OUTCOME_FAILED,
	}).GetDeliveries()
	if len(listed) != 1 {
		t.Fatalf("recorded %d failed deliveries, want 1", len(listed))
	}
	rec := listed[0]
	if rec.GetDelivered() || len(rec.GetAttempts()) != 4 || f.receiver.count() != 4 {
		t.Fatalf("record delivered=%t with %d attempts; receiver saw %d; want false, 4, 4",
			rec.GetDelivered(), len(rec.GetAttempts()), f.receiver.count())
	}
	wantBackoffs := []time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	for i, attempt := range rec.GetAttempts() {
		if attempt.GetNumber() != int32(i+1) || attempt.GetStatusCode() != 502 {
			t.Errorf("attempt %d = %v, want number %d status 502", i, attempt, i+1)
		}
		if attempt.GetBackoff().AsDuration() != wantBackoffs[i] {
			t.Errorf("attempt %d backoff = %s, want %s", i+1, attempt.GetBackoff().AsDuration(), wantBackoffs[i])
		}
		if attempt.GetError() != "receiver answered 502" {
			t.Errorf("attempt %d error = %q", i+1, attempt.GetError())
		}
	}
	if last := rec.GetAttempts()[3].GetAt().AsTime(); rec.GetSettledAt().AsTime().Before(last) {
		t.Errorf("settled_at %s is before the last attempt %s", rec.GetSettledAt().AsTime(), last)
	}
	if got := f.list(t, &orkstackv1.ListDeliveriesRequest{
		Outcome: orkstackv1.DeliveryOutcome_DELIVERY_OUTCOME_DELIVERED,
	}).GetDeliveries(); len(got) != 0 {
		t.Errorf("the delivered-only listing holds %d records", len(got))
	}
}

func TestDeliveryRecorder_StoreFailureIsLoggedAndNotFatal(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusOK)
	f.store.FailPuts(errors.New("bucket is on fire"))
	f.register(t, webhooks.EventRunSucceeded)
	run := f.sampleRun(t, "ork-2026-09-21-8f31", orkstackv1.Status_STATUS_SUCCEEDED)
	f.announce(t, run, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, "")

	if f.receiver.count() != 1 {
		t.Fatalf("receiver saw %d requests, want the delivery to land regardless of the record", f.receiver.count())
	}
	_, headers := f.receiver.request(0)
	logs := f.logs.String()
	if !strings.Contains(logs, "webhook delivery record dropped") ||
		!strings.Contains(logs, "delivery_id="+headers.Get(webhooks.DeliveryHeader)) ||
		!strings.Contains(logs, "bucket is on fire") {
		t.Errorf("the engine log does not name the dropped record:\n%s", logs)
	}
	if !strings.Contains(logs, "level=WARN") {
		t.Errorf("the drop was not logged at warn:\n%s", logs)
	}
}

func TestDeliveryRecorder_RecordsTaskOutcomeCoordinates(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusOK)
	f.register(t, webhooks.EventTaskSucceeded)
	run := f.sampleRun(t, "ork-2026-09-21-8f31", orkstackv1.Status_STATUS_RUNNING)
	f.announce(t, run, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, "extract")

	listed := f.list(t, &orkstackv1.ListDeliveriesRequest{Event: orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED}).GetDeliveries()
	if len(listed) != 1 {
		t.Fatalf("recorded %d task deliveries, want 1", len(listed))
	}
	rec := listed[0]
	if rec.GetTaskId() != "extract" || rec.GetAttempt() != 2 || rec.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("record task=%q attempt=%d status=%s, want extract, 2, SUCCEEDED", rec.GetTaskId(), rec.GetAttempt(), rec.GetStatus())
	}
}

func TestSubmitRun_RunThatNeverReachedTemporalIsRecorded(t *testing.T) {
	f := newNotifierFixture(t)
	store := artifacts.NewMemoryDeliveryStore()
	f.notifier.Observe(api.NewDeliveryRecorder(store, nil).Record)
	if _, err := f.registry.Register(webhooks.Registration{
		URL: f.server.URL, Events: []webhooks.Event{webhooks.EventRunFailed}, Secret: "s3cr3t",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	fixture := newFixtureWithNotifier(t, f.adapter)
	fixture.starter.err = fmt.Errorf("temporal is unreachable")
	ctx := context.Background()
	if _, err := fixture.orkflowClient().ApplyOrkflow(ctx,
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := fixture.runClient().SubmitRun(ctx,
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"})); err == nil {
		t.Fatal("SubmitRun succeeded even though the starter failed")
	}
	_, headers := f.awaitDelivery(t)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := f.notifier.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	rec, ok, err := store.GetDelivery(ctx, headers.Get(webhooks.DeliveryHeader))
	if err != nil || !ok {
		t.Fatalf("the synthesized RUN_FAILED was not recorded: %v, %v", ok, err)
	}
	if rec.GetEvent() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || rec.GetNamespace() != "default" {
		t.Errorf("record = %v, want a RUN_FAILED in default", rec)
	}
}

func TestListDeliveries_WindowDefaultsAndValidation(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusOK)
	f.now = time.Date(2026, 9, 21, 9, 30, 12, 0, time.UTC)
	run := f.sampleRun(t, "ork-2026-09-21-8f31", orkstackv1.Status_STATUS_SUCCEEDED)

	resp := f.list(t, &orkstackv1.ListDeliveriesRequest{})
	if !resp.GetUntil().AsTime().Equal(f.now) || !resp.GetSince().AsTime().Equal(f.now.Add(-7*24*time.Hour)) {
		t.Errorf("default window = [%s, %s), want [now-7d, now)", resp.GetSince().AsTime(), resp.GetUntil().AsTime())
	}
	resp = f.list(t, &orkstackv1.ListDeliveriesRequest{RunId: run.GetId()})
	if !resp.GetSince().AsTime().Equal(run.GetCreatedAt().AsTime()) {
		t.Errorf("since with a known run = %s, want the run's created_at %s", resp.GetSince().AsTime(), run.GetCreatedAt().AsTime())
	}
	resp = f.list(t, &orkstackv1.ListDeliveriesRequest{RunId: "ork-unknown"})
	if !resp.GetSince().AsTime().Equal(f.now.Add(-7 * 24 * time.Hour)) {
		t.Errorf("since with an unknown run = %s, want the 7-day default", resp.GetSince().AsTime())
	}

	ctx := context.Background()
	for name, req := range map[string]*orkstackv1.ListDeliveriesRequest{
		"inverted window": {Since: timestamppb.New(f.now), Until: timestamppb.New(f.now.Add(-time.Hour))},
		"empty window":    {Since: timestamppb.New(f.now), Until: timestamppb.New(f.now)},
		"page_size 201":   {PageSize: 201},
		"page_size -1":    {PageSize: -1},
		"TASK_LOG":        {Event: orkstackv1.EventType_EVENT_TYPE_TASK_LOG},
		"RUN_CREATED":     {Event: orkstackv1.EventType_EVENT_TYPE_RUN_CREATED},
		"Bad Name":        {Namespace: "Bad Name"},
		"bad webhook id":  {WebhookId: "not-a-webhook"},
		"garbage token":   {PageToken: "!!!"},
	} {
		_, err := f.svc.ListDeliveries(ctx, connect.NewRequest(req))
		wantCode(t, err, connect.CodeInvalidArgument, name)
	}
}

// seed stores records directly, as the recorder would have.
func seed(t *testing.T, store artifacts.DeliveryStore, at time.Time, mutate func(i int, rec *orkstackv1.Delivery), n int) []*orkstackv1.Delivery {
	t.Helper()
	var out []*orkstackv1.Delivery
	for i := range n {
		queued := at.Add(time.Duration(i) * time.Second)
		id, err := webhooks.NewDeliveryID(queued)
		if err != nil {
			t.Fatalf("NewDeliveryID: %v", err)
		}
		rec := &orkstackv1.Delivery{
			Id: id, WebhookId: "wh-4a1c0e2b7d3f8159", Event: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
			RunId: "ork-2026-09-21-8f31", Namespace: "payments", OrkflowName: "reconcile",
			Status: orkstackv1.Status_STATUS_FAILED, Url: "https://payments.example/hooks",
			CreatedAt: timestamppb.New(queued), EventAt: timestamppb.New(queued), SettledAt: timestamppb.New(queued),
			Attempts: []*orkstackv1.DeliveryAttempt{{Number: 1, StatusCode: 502, At: timestamppb.New(queued)}},
		}
		if mutate != nil {
			mutate(i, rec)
		}
		if err := store.PutDelivery(context.Background(), rec); err != nil {
			t.Fatalf("PutDelivery: %v", err)
		}
		out = append(out, rec)
	}
	return out
}

func TestListDeliveries_CursorBindsFilters(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusOK)
	seed(t, f.store, f.now.Add(-time.Hour), nil, 5)
	first := f.list(t, &orkstackv1.ListDeliveriesRequest{WebhookId: "wh-4a1c0e2b7d3f8159", PageSize: 2})
	if first.GetNextPageToken() == "" {
		t.Fatal("no next page token after a full page")
	}
	_, err := f.svc.ListDeliveries(context.Background(), connect.NewRequest(&orkstackv1.ListDeliveriesRequest{
		WebhookId: "wh-0000000000000000", PageSize: 2, PageToken: first.GetNextPageToken(),
	}))
	wantCode(t, err, connect.CodeInvalidArgument, "token replayed with another webhook_id")
	if err != nil && !strings.Contains(err.Error(), "page_token does not match these delivery filters") {
		t.Errorf("refusal %q does not say the filters changed", err)
	}

	seen := map[string]bool{}
	req := &orkstackv1.ListDeliveriesRequest{WebhookId: "wh-4a1c0e2b7d3f8159", PageSize: 2}
	for page := 0; page < 5; page++ {
		// The clock moving between pages must not move the window.
		f.now = f.now.Add(time.Minute)
		resp := f.list(t, req)
		for _, rec := range resp.GetDeliveries() {
			if seen[rec.GetId()] {
				t.Errorf("delivery %s listed twice", rec.GetId())
			}
			seen[rec.GetId()] = true
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		req.PageToken = resp.GetNextPageToken()
		req.PageSize = 3 // page size is not bound
	}
	if len(seen) != 5 {
		t.Errorf("paged through %d records, want 5", len(seen))
	}
}

func TestListDeliveries_NamespaceEmptyMeansEvery(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusOK)
	seed(t, f.store, f.now.Add(-time.Hour), func(i int, rec *orkstackv1.Delivery) {
		rec.Namespace = []string{"default", "analytics"}[i]
	}, 2)
	if got := f.list(t, &orkstackv1.ListDeliveriesRequest{}).GetDeliveries(); len(got) != 2 {
		t.Errorf("empty namespace listed %d records, want both namespaces", len(got))
	}
	got := f.list(t, &orkstackv1.ListDeliveriesRequest{Namespace: "default"}).GetDeliveries()
	if len(got) != 1 || got[0].GetNamespace() != "default" {
		t.Errorf("namespace default listed %v, want the one default record", got)
	}
}

func TestListDeliveries_NoStoreIsFailedPrecondition(t *testing.T) {
	svc := api.NewService(api.Config{})
	ctx := context.Background()
	_, err := svc.ListDeliveries(ctx, connect.NewRequest(&orkstackv1.ListDeliveriesRequest{}))
	wantCode(t, err, connect.CodeFailedPrecondition, "ListDeliveries")
	_, err = svc.GetDelivery(ctx, connect.NewRequest(&orkstackv1.GetDeliveryRequest{Id: "dlv-20260921T021406.891204113Z-9c22a1f04e7b3d58"}))
	wantCode(t, err, connect.CodeFailedPrecondition, "GetDelivery")
	_, err = svc.RedeliverWebhook(ctx, connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: "dlv-20260921T021406.891204113Z-9c22a1f04e7b3d58"}))
	wantCode(t, err, connect.CodeFailedPrecondition, "RedeliverWebhook")
}

// budgetStore refuses every listing the way a store past its key budget does.
type budgetStore struct{ artifacts.DeliveryStore }

func (budgetStore) ListDeliveries(context.Context, artifacts.DeliveryQuery) (artifacts.DeliveryPage, error) {
	return artifacts.DeliveryPage{}, fmt.Errorf("walk: %w", artifacts.ErrDeliveryScanBudget)
}

func TestListDeliveries_ScanBudgetIsResourceExhausted(t *testing.T) {
	svc := api.NewService(api.Config{Deliveries: budgetStore{artifacts.NewMemoryDeliveryStore()}})
	_, err := svc.ListDeliveries(context.Background(), connect.NewRequest(&orkstackv1.ListDeliveriesRequest{}))
	wantCode(t, err, connect.CodeResourceExhausted, "over budget")
	if err != nil && !strings.Contains(err.Error(), "100000 delivery keys") {
		t.Errorf("refusal %q does not name the budget", err)
	}
}

func TestGetDelivery_NotFoundAndInvalidId(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusOK)
	recs := seed(t, f.store, f.now.Add(-time.Hour), nil, 1)
	ctx := context.Background()
	got, err := f.svc.GetDelivery(ctx, connect.NewRequest(&orkstackv1.GetDeliveryRequest{Id: recs[0].GetId()}))
	if err != nil || got.Msg.GetDelivery().GetId() != recs[0].GetId() {
		t.Fatalf("GetDelivery = %v, %v", got, err)
	}
	_, err = f.svc.GetDelivery(ctx, connect.NewRequest(&orkstackv1.GetDeliveryRequest{Id: "dlv-20200101T000000.000000000Z-0000000000000000"}))
	wantCode(t, err, connect.CodeNotFound, "unknown id")
	for _, bad := range []string{"", "dlv-0123456789abcdef01234567", "../x"} {
		_, err = f.svc.GetDelivery(ctx, connect.NewRequest(&orkstackv1.GetDeliveryRequest{Id: bad}))
		wantCode(t, err, connect.CodeInvalidArgument, fmt.Sprintf("id %q", bad))
	}
}

// failedOriginal delivers one RUN_FAILED to a failing receiver and returns
// its record.
func (f *deliveryFixture) failedOriginal(t *testing.T) (*orkstackv1.Delivery, *orkstackv1.Run, webhooks.Webhook) {
	t.Helper()
	hook := f.register(t, webhooks.EventRunFailed)
	run := f.sampleRun(t, "ork-2026-09-21-8f31", orkstackv1.Status_STATUS_FAILED)
	f.announce(t, run, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED, "")
	listed := f.list(t, &orkstackv1.ListDeliveriesRequest{}).GetDeliveries()
	if len(listed) != 1 || listed[0].GetDelivered() {
		t.Fatalf("original = %v, want one failed record", listed)
	}
	return listed[0], run, hook
}

func TestRedeliverWebhook_PostsOnceSignedWithTheCurrentSecretAndRecordsANewDelivery(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusBadGateway)
	original, run, _ := f.failedOriginal(t)
	before := f.receiver.count()
	originalBytes := f.store.Documents()

	// Today's run differs from the one the original embedded.
	run = proto.Clone(run).(*orkstackv1.Run)
	run.OrkflowVersion = "0.2.0-today"
	f.runs.Save(run)
	f.receiver.answer(http.StatusOK)

	resp, err := f.svc.RedeliverWebhook(context.Background(),
		connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: original.GetId()}))
	if err != nil {
		t.Fatalf("RedeliverWebhook: %v", err)
	}
	if got := f.receiver.count() - before; got != 1 {
		t.Fatalf("receiver saw %d more requests, want exactly 1", got)
	}
	body, headers := f.receiver.request(before)
	rec := resp.Msg.GetDelivery()
	for header, want := range map[string]string{
		webhooks.RedeliveryHeader: original.GetId(),
		webhooks.DeliveryHeader:   rec.GetId(),
		webhooks.AttemptHeader:    "1",
		webhooks.TimestampHeader:  original.GetEventAt().AsTime().UTC().Format(time.RFC3339Nano),
	} {
		if got := headers.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if rec.GetId() == original.GetId() {
		t.Error("the redelivery reused the original id")
	}
	if !webhooks.Verify("s3cr3t", body, headers.Get(webhooks.SignatureHeader)) {
		t.Error("the redelivery signature does not verify under the subscription's secret")
	}
	var payload struct {
		RunID       string `json:"runId"`
		OrkflowName string `json:"orkflowName"`
		Status      string `json:"status"`
		Timestamp   string `json:"timestamp"`
		Run         struct {
			OrkflowVersion string `json:"orkflowVersion"`
		} `json:"run"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if payload.RunID != original.GetRunId() || payload.OrkflowName != "reconcile" || payload.Status != "FAILED" ||
		payload.Timestamp != original.GetEventAt().AsTime().UTC().Format(time.RFC3339Nano) {
		t.Errorf("body header = %+v, want the original transition's", payload)
	}
	if payload.Run.OrkflowVersion != "0.2.0-today" {
		t.Errorf("embedded run version = %q, want the run as it reads today", payload.Run.OrkflowVersion)
	}
	sum := sha256.Sum256(body)
	if rec.GetRedeliveryOf() != original.GetId() || len(rec.GetAttempts()) != 1 || !rec.GetDelivered() ||
		rec.GetBodySha256() != hex.EncodeToString(sum[:]) || rec.GetBodySha256() == original.GetBodySha256() {
		t.Errorf("new record = %v, want redelivery_of, one delivered attempt, its own body digest", rec)
	}
	stored, err := f.svc.GetDelivery(context.Background(), connect.NewRequest(&orkstackv1.GetDeliveryRequest{Id: rec.GetId()}))
	if err != nil || !proto.Equal(stored.Msg.GetDelivery(), rec) {
		t.Errorf("GetDelivery(new) = %v, %v; want the returned record", stored, err)
	}
	for key, document := range originalBytes {
		if now := f.store.Documents()[key]; string(now) != string(document) {
			t.Errorf("original record %s changed after the redelivery", key)
		}
	}
	listed := f.list(t, &orkstackv1.ListDeliveriesRequest{Until: timestamppb.New(f.now.Add(time.Second))}).GetDeliveries()
	if len(listed) != 2 || listed[0].GetId() != rec.GetId() {
		t.Errorf("listing = %v, want the redelivery first", listed)
	}
}

func TestRedeliverWebhook_NeverRetries(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusBadGateway)
	original, _, _ := f.failedOriginal(t)
	before := f.receiver.count()
	f.receiver.answer(http.StatusServiceUnavailable)
	resp, err := f.svc.RedeliverWebhook(context.Background(),
		connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: original.GetId()}))
	if err != nil {
		t.Fatalf("RedeliverWebhook: %v (a failed POST is a recorded outcome, not an RPC failure)", err)
	}
	if got := f.receiver.count() - before; got != 1 {
		t.Errorf("receiver saw %d requests for one redelivery, want exactly 1", got)
	}
	if rec := resp.Msg.GetDelivery(); rec.GetDelivered() || len(rec.GetAttempts()) != 1 || rec.GetAttempts()[0].GetStatusCode() != 503 {
		t.Errorf("record = %v, want one undelivered 503 attempt", rec)
	}
}

// A fan-out's items stay out of the redelivery body exactly as they stay out
// of the original's (each-fan-out §5.13): the receiver sees one shape, with
// the node's each and status, however many items the run expanded.
func TestRedeliverWebhook_BodyCarriesNoItems(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusBadGateway)
	f.register(t, webhooks.EventRunFailed)
	run := f.sampleRun(t, "ork-2026-09-21-each", orkstackv1.Status_STATUS_FAILED)
	run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{
		TaskId: "convert", Status: orkstackv1.Status_STATUS_FAILED,
		Each: &orkstackv1.TaskEach{Expanded: true, Count: 3},
		Items: []*orkstackv1.TaskRun{
			{TaskId: "convert", Item: proto.Int32(0), Status: orkstackv1.Status_STATUS_SUCCEEDED},
			{TaskId: "convert", Item: proto.Int32(1), Status: orkstackv1.Status_STATUS_FAILED},
			{TaskId: "convert", Item: proto.Int32(2), Status: orkstackv1.Status_STATUS_CANCELED},
		},
	})
	f.runs.Save(run)
	f.announce(t, run, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED, "")
	listed := f.list(t, &orkstackv1.ListDeliveriesRequest{}).GetDeliveries()
	if len(listed) != 1 {
		t.Fatalf("original = %v, want one record", listed)
	}
	before := f.receiver.count() // the original's attempts against a failing receiver
	f.receiver.answer(http.StatusOK)
	if _, err := f.svc.RedeliverWebhook(context.Background(),
		connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: listed[0].GetId()})); err != nil {
		t.Fatalf("RedeliverWebhook: %v", err)
	}
	if got := f.receiver.count(); got != before+1 {
		t.Fatalf("receiver saw %d requests, want %d", got, before+1)
	}
	for label, i := range map[string]int{"original": 0, "redelivery": before} {
		body, _ := f.receiver.request(i)
		var payload struct {
			Run json.RawMessage `json:"run"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("%s: decode body: %v", label, err)
		}
		if strings.Contains(string(payload.Run), `"items"`) || !strings.Contains(string(payload.Run), `"each"`) {
			t.Errorf("%s body run = %s, want each without items", label, payload.Run)
		}
	}
	stored, err := f.svc.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: run.GetId(), Namespace: run.GetNamespace()}))
	if err != nil || len(stored.Msg.GetRun().GetTaskRuns()[1].GetItems()) != 3 {
		t.Errorf("the run's own read lost its items: %v, %v", stored, err)
	}
}

func TestRedeliverWebhook_RefusesADeletedWebhookWithoutPosting(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusBadGateway)
	original, _, hook := f.failedOriginal(t)
	before, recordsBefore := f.receiver.count(), len(f.store.Documents())
	if !f.registry.Delete(hook.ID) {
		t.Fatal("Delete reported the webhook unknown")
	}
	_, err := f.svc.RedeliverWebhook(context.Background(),
		connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: original.GetId()}))
	wantCode(t, err, connect.CodeFailedPrecondition, "deleted webhook")
	if err != nil && !strings.Contains(err.Error(), "nothing was sent") {
		t.Errorf("refusal %q does not say nothing was sent", err)
	}
	if f.receiver.count() != before || len(f.store.Documents()) != recordsBefore {
		t.Error("a refused redelivery POSTed or recorded something")
	}
}

func TestRedeliverWebhook_UnknownDeliveryAndUnreadableRun(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusBadGateway)
	original, run, _ := f.failedOriginal(t)
	before := f.receiver.count()
	ctx := context.Background()
	_, err := f.svc.RedeliverWebhook(ctx, connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{
		Id: "dlv-20200101T000000.000000000Z-0000000000000000"}))
	wantCode(t, err, connect.CodeNotFound, "unknown delivery")
	_, err = f.svc.RedeliverWebhook(ctx, connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: "nope"}))
	wantCode(t, err, connect.CodeInvalidArgument, "malformed id")

	f.runs.Delete(run.GetId())
	_, err = f.svc.RedeliverWebhook(ctx, connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: original.GetId()}))
	wantCode(t, err, connect.CodeNotFound, "unreadable run")
	if f.receiver.count() != before {
		t.Errorf("a refused redelivery reached the receiver %d times", f.receiver.count()-before)
	}
}

// putFailingStore reads like the real store and refuses every write.
type putFailingStore struct{ *artifacts.MemoryDeliveryStore }

func (putFailingStore) PutDelivery(context.Context, *orkstackv1.Delivery) error {
	return errors.New("bucket is read-only")
}

func TestRedeliverWebhook_UnwritableRecordIsUnavailableNamingTheOutcome(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusBadGateway)
	original, _, _ := f.failedOriginal(t)
	f.receiver.answer(http.StatusOK)
	svc := api.NewService(api.Config{
		Runs: f.runs, Namespaces: f.spaces, Webhooks: f.registry, Deliveries: putFailingStore{f.store},
		Now: func() time.Time { return f.now },
	})
	before := f.receiver.count()
	_, err := svc.RedeliverWebhook(context.Background(),
		connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: original.GetId()}))
	wantCode(t, err, connect.CodeUnavailable, "unwritable record")
	if f.receiver.count() != before+1 {
		t.Errorf("receiver saw %d requests, want the one POST that did happen", f.receiver.count()-before)
	}
	if err != nil && (!strings.Contains(err.Error(), "delivered=true") || !strings.Contains(err.Error(), "status 200") ||
		!strings.Contains(err.Error(), original.GetId())) {
		t.Errorf("refusal %q does not state the POST's outcome", err)
	}
}

func TestAuditInterceptor_RecordsRedeliverWithoutSecrets(t *testing.T) {
	f := newDeliveryFixture(t, http.StatusBadGateway)
	original, _, _ := f.failedOriginal(t)
	f.receiver.answer(http.StatusOK)
	audit := artifacts.NewMemoryAuditStore()
	svc := api.NewService(api.Config{
		Runs: f.runs, Namespaces: f.spaces, Webhooks: f.registry, Deliveries: f.store, AuditStore: audit,
		Now: func() time.Time { return f.now },
	})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	t.Cleanup(server.Close)
	client := orkstackv1connect.NewWebhookServiceClient(server.Client(), server.URL)
	resp, err := client.RedeliverWebhook(context.Background(),
		connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: original.GetId()}))
	if err != nil {
		t.Fatalf("RedeliverWebhook: %v", err)
	}
	if _, err := client.ListDeliveries(context.Background(), connect.NewRequest(&orkstackv1.ListDeliveriesRequest{})); err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	page, err := audit.ListAuditRecords(context.Background(), artifacts.AuditQuery{
		Since: f.now.Add(-time.Hour), Until: f.now.Add(time.Hour), Limit: 100,
	})
	if err != nil {
		t.Fatalf("ListAuditRecords: %v", err)
	}
	if len(page.Records) != 1 {
		t.Fatalf("audit holds %d records, want exactly the one redeliver (reads are never recorded)", len(page.Records))
	}
	rec := page.Records[0]
	newID := resp.Msg.GetDelivery().GetId()
	if rec.GetAction() != "webhook.redeliver" || rec.GetTarget() != original.GetId() || rec.GetNamespace() != "" {
		t.Errorf("audit record = %v, want webhook.redeliver of %s, deployment-wide", rec, original.GetId())
	}
	want := fmt.Sprintf("webhook=%s run=%s event=RUN_FAILED -> %s delivered=true status=200",
		original.GetWebhookId(), original.GetRunId(), newID)
	if rec.GetDetail() != want {
		t.Errorf("detail = %q, want %q", rec.GetDetail(), want)
	}
	encoded, _ := json.Marshal(rec)
	for _, secret := range []string{"s3cr3t", resp.Msg.GetDelivery().GetSignature(), "sha256="} {
		if strings.Contains(string(encoded), secret) {
			t.Errorf("audit record carries %q: %s", secret, encoded)
		}
	}
}
