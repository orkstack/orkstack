package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// webhookJSON performs one management call and returns the status and body.
func (f *fixture) webhookJSON(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode request: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, f.baseURL+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, payload
}

func TestWebhookAPI_RegisterThenListShowsTheSubscriptionWithoutItsSecret(t *testing.T) {
	f := newFixture(t)

	status, body := f.webhookJSON(t, http.MethodPost, api.WebhookCollectionPath, map[string]any{
		"url":    "https://example.test/hooks",
		"events": []string{"RUN_SUCCEEDED", "RUN_FAILED"},
	})
	if status != http.StatusCreated {
		t.Fatalf("register status = %d, want 201: %s", status, body)
	}
	var registered struct {
		Webhook webhooks.Webhook `json:"webhook"`
	}
	if err := json.Unmarshal(body, &registered); err != nil {
		t.Fatalf("decode register response: %v (%s)", err, body)
	}
	hook := registered.Webhook
	if hook.ID == "" {
		t.Error("registration returned no id")
	}
	if hook.URL != "https://example.test/hooks" {
		t.Errorf("url = %q", hook.URL)
	}
	if len(hook.Events) != 2 {
		t.Errorf("events = %v, want the two requested", hook.Events)
	}
	if hook.Secret == "" {
		t.Error("registration disclosed no signing secret; the subscriber could never verify a delivery")
	}
	if hook.CreatedAt.IsZero() {
		t.Error("registration recorded no createdAt")
	}

	status, body = f.webhookJSON(t, http.MethodGet, api.WebhookCollectionPath, nil)
	if status != http.StatusOK {
		t.Fatalf("list status = %d, want 200: %s", status, body)
	}
	var listed struct {
		Webhooks []webhooks.Webhook `json:"webhooks"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("decode list response: %v (%s)", err, body)
	}
	if len(listed.Webhooks) != 1 {
		t.Fatalf("list returned %d webhooks, want 1: %s", len(listed.Webhooks), body)
	}
	if listed.Webhooks[0].ID != hook.ID || listed.Webhooks[0].URL != hook.URL {
		t.Errorf("listed %+v, want the registered webhook", listed.Webhooks[0])
	}
	if listed.Webhooks[0].Secret != "" {
		t.Error("list disclosed the signing secret")
	}
	if strings.Contains(string(body), hook.Secret) {
		t.Error("the list response body contains the signing secret")
	}
}

func TestWebhookAPI_ListIsEmptyBeforeAnythingIsRegistered(t *testing.T) {
	f := newFixture(t)
	status, body := f.webhookJSON(t, http.MethodGet, api.WebhookCollectionPath, nil)
	if status != http.StatusOK {
		t.Fatalf("list status = %d, want 200: %s", status, body)
	}
	var listed struct {
		Webhooks []webhooks.Webhook `json:"webhooks"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("decode list response: %v (%s)", err, body)
	}
	if len(listed.Webhooks) != 0 {
		t.Errorf("list returned %+v, want nothing", listed.Webhooks)
	}
}

func TestWebhookAPI_RejectsAnUnreachableURLAndAnUnknownEvent(t *testing.T) {
	f := newFixture(t)
	cases := []struct {
		label string
		body  map[string]any
		want  string
	}{
		{"no url", map[string]any{"events": []string{"RUN_FAILED"}}, "url is required"},
		{"relative url", map[string]any{"url": "/hooks"}, "scheme must be http or https"},
		{"unknown event", map[string]any{
			"url": "https://example.test/hooks", "events": []string{"RUN_EXPLODED"},
		}, "unknown event"},
	}
	for _, tc := range cases {
		status, body := f.webhookJSON(t, http.MethodPost, api.WebhookCollectionPath, tc.body)
		if status != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400: %s", tc.label, status, body)
		}
		if !strings.Contains(string(body), tc.want) {
			t.Errorf("%s: body %s does not explain %q", tc.label, body, tc.want)
		}
	}
	if got := f.svc.Webhooks().List(); len(got) != 0 {
		t.Errorf("a rejected registration was stored: %+v", got)
	}
}

func TestWebhookAPI_DeleteRemovesTheSubscriptionAndReportsAnUnknownID(t *testing.T) {
	f := newFixture(t)
	hook, err := f.svc.Webhooks().Register(webhooks.Registration{URL: "https://example.test/hooks"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	itemPath := strings.Replace(api.WebhookItemPath, "{id}", hook.ID, 1)

	status, body := f.webhookJSON(t, http.MethodGet, itemPath, nil)
	if status != http.StatusOK {
		t.Fatalf("get status = %d, want 200: %s", status, body)
	}
	if status, body := f.webhookJSON(t, http.MethodDelete, itemPath, nil); status != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204: %s", status, body)
	}
	if got := f.svc.Webhooks().List(); len(got) != 0 {
		t.Errorf("after delete the registry still holds %+v", got)
	}
	status, body = f.webhookJSON(t, http.MethodDelete, itemPath, nil)
	if status != http.StatusNotFound {
		t.Errorf("deleting the same webhook twice = %d, want 404: %s", status, body)
	}
	status, body = f.webhookJSON(t, http.MethodGet,
		strings.Replace(api.WebhookItemPath, "{id}", "wh-never", 1), nil)
	if status != http.StatusNotFound {
		t.Errorf("get unknown id = %d, want 404: %s", status, body)
	}
}

// notifierFixture wires a real registry, dispatcher and notifier to a test
// receiver, which is as close to production as a unit test gets.
type notifierFixture struct {
	registry *webhooks.Registry
	notifier *webhooks.Notifier
	adapter  *api.WebhookNotifier
	server   *httptest.Server

	mu       sync.Mutex
	bodies   [][]byte
	headers  []http.Header
	received chan struct{}
}

func newNotifierFixture(t *testing.T) *notifierFixture {
	t.Helper()
	f := &notifierFixture{received: make(chan struct{}, 16)}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.bodies = append(f.bodies, body)
		f.headers = append(f.headers, r.Header.Clone())
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		f.received <- struct{}{}
	}))
	t.Cleanup(f.server.Close)

	f.registry = webhooks.NewRegistry()
	f.notifier = webhooks.NewNotifier(f.registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{})
	f.adapter = api.NewWebhookNotifier(f.notifier, nil)
	return f
}

func (f *notifierFixture) awaitDelivery(t *testing.T) ([]byte, http.Header) {
	t.Helper()
	select {
	case <-f.received:
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery arrived")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	last := len(f.bodies) - 1
	return f.bodies[last], f.headers[last]
}

func (f *notifierFixture) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.bodies)
}

func TestWebhookNotifier_PostsASignedPayloadNamingTheRunAndItsStatus(t *testing.T) {
	f := newNotifierFixture(t)
	hook, err := f.registry.Register(webhooks.Registration{
		URL:    f.server.URL,
		Events: []webhooks.Event{webhooks.EventRunSucceeded},
		Secret: "s3cr3t",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	finished := time.Unix(1755302400, 0).UTC()
	input, err := structpb.NewStruct(map[string]any{"url": "https://httpbin.org/json"})
	if err != nil {
		t.Fatalf("build input: %v", err)
	}
	run := &orkstackv1.Run{
		Id:             "ork-1755302400123456789-1",
		OrkflowName:    "hello-ork",
		OrkflowVersion: "0.1.0",
		Status:         orkstackv1.Status_STATUS_SUCCEEDED,
		Input:          &orkstackv1.Payload{Data: input},
		FinishedAt:     timestamppb.New(finished),
		TaskRuns: []*orkstackv1.TaskRun{{
			TaskId: "fetch",
			Plugin: "http",
			Status: orkstackv1.Status_STATUS_SUCCEEDED,
		}},
	}
	f.adapter.NotifyRunTransition(context.Background(), &orkstackv1.Event{
		RunId:     run.GetId(),
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		Timestamp: timestamppb.New(finished),
	}, run)

	body, headers := f.awaitDelivery(t)
	if !webhooks.Verify("s3cr3t", body, headers.Get(webhooks.SignatureHeader)) {
		t.Errorf("the delivery signature does not verify against the body")
	}
	if headers.Get(webhooks.WebhookHeader) != hook.ID {
		t.Errorf("%s = %q, want %q", webhooks.WebhookHeader, headers.Get(webhooks.WebhookHeader), hook.ID)
	}
	if headers.Get(webhooks.EventHeader) != "RUN_SUCCEEDED" {
		t.Errorf("%s = %q", webhooks.EventHeader, headers.Get(webhooks.EventHeader))
	}

	var payload struct {
		RunID       string          `json:"runId"`
		OrkflowName string          `json:"orkflowName"`
		Status      string          `json:"status"`
		Timestamp   string          `json:"timestamp"`
		Run         json.RawMessage `json:"run"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode payload: %v (%s)", err, body)
	}
	if payload.RunID != run.GetId() {
		t.Errorf("runId = %q, want %q", payload.RunID, run.GetId())
	}
	if payload.OrkflowName != "hello-ork" {
		t.Errorf("orkflowName = %q, want hello-ork", payload.OrkflowName)
	}
	if payload.Status != "SUCCEEDED" {
		t.Errorf("status = %q, want SUCCEEDED without its proto prefix", payload.Status)
	}
	if payload.Timestamp != "2025-08-16T00:00:00Z" {
		t.Errorf("timestamp = %q, want the transition's", payload.Timestamp)
	}

	// The embedded run is the same JSON the rest of the API emits, enum
	// dialect included, and it is the whole run rather than a summary.
	wantRun, err := api.MarshalJSON(run)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var got, want map[string]any
	if err := json.Unmarshal(payload.Run, &got); err != nil {
		t.Fatalf("decode embedded run: %v", err)
	}
	if err := json.Unmarshal(wantRun, &want); err != nil {
		t.Fatalf("decode expected run: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("embedded run =\n%v\nwant\n%v", got, want)
	}
	if got["status"] != "SUCCEEDED" {
		t.Errorf("embedded run status = %v, want the short enum dialect", got["status"])
	}
	if got["taskRuns"] == nil {
		t.Error("embedded run carries no task runs")
	}
}

func TestWebhookNotifier_IgnoresEventsThatAreNotRunTransitions(t *testing.T) {
	f := newNotifierFixture(t)
	if _, err := f.registry.Register(webhooks.Registration{URL: f.server.URL}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	run := &orkstackv1.Run{Id: "ork-1-1", OrkflowName: "hello-ork"}
	for _, eventType := range []orkstackv1.EventType{
		orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
	} {
		f.adapter.NotifyRunTransition(context.Background(),
			&orkstackv1.Event{RunId: run.GetId(), Type: eventType}, run)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.notifier.Wait(ctx); err != nil {
		t.Fatalf("deliveries did not settle: %v", err)
	}
	if f.count() != 0 {
		t.Errorf("%d deliveries went out for events that are not run transitions", f.count())
	}
}

func TestWebhookNotifier_NilAdapterIsInert(t *testing.T) {
	var adapter *api.WebhookNotifier
	if api.NewWebhookNotifier(nil, nil) != nil {
		t.Error("NewWebhookNotifier(nil) returned a live adapter")
	}
	// Calling through a nil adapter must not panic: it is what "webhooks are
	// switched off" looks like at every call site.
	adapter.NotifyRunTransition(context.Background(),
		&orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED},
		&orkstackv1.Run{Id: "ork-1-1"})
}

func TestSubmitRun_AnnouncesARunThatNeverReachedTemporal(t *testing.T) {
	// A run that fails to start is still a run that failed. A subscriber must
	// see it, because nothing downstream will ever report it.
	f := newNotifierFixture(t)
	if _, err := f.registry.Register(webhooks.Registration{
		URL:    f.server.URL,
		Events: []webhooks.Event{webhooks.EventRunFailed},
		Secret: "s3cr3t",
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

	body, headers := f.awaitDelivery(t)
	if !webhooks.Verify("s3cr3t", body, headers.Get(webhooks.SignatureHeader)) {
		t.Error("the delivery signature does not verify against the body")
	}
	var payload struct {
		Status string `json:"status"`
		Run    struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"run"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode payload: %v (%s)", err, body)
	}
	if payload.Status != "FAILED" {
		t.Errorf("status = %q, want FAILED", payload.Status)
	}
	if payload.Run.Error.Code != "RUN_START_FAILED" {
		t.Errorf("run error code = %q, want RUN_START_FAILED", payload.Run.Error.Code)
	}
}
