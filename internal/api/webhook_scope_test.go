package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

func (f *fixture) webhookClient() orkstackv1connect.WebhookServiceClient {
	return orkstackv1connect.NewWebhookServiceClient(f.client, f.baseURL)
}

func TestWebhookAPI_RegisterWithScopeListsTheScope(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	registered, err := f.webhookClient().RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
		Url:         "https://example.test/scoped",
		Events:      []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED},
		Namespace:   "analytics",
		OrkflowName: "nightly-etl",
		TaskId:      "extract",
	}))
	if err != nil {
		t.Fatalf("RegisterWebhook: %v", err)
	}
	hook := registered.Msg.GetWebhook()
	if hook.GetNamespace() != "analytics" || hook.GetOrkflowName() != "nightly-etl" || hook.GetTaskId() != "extract" {
		t.Errorf("registered scope = %q/%q/%q, want analytics/nightly-etl/extract",
			hook.GetNamespace(), hook.GetOrkflowName(), hook.GetTaskId())
	}
	if _, err := f.webhookClient().RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
		Url: "https://example.test/plain",
	})); err != nil {
		t.Fatalf("RegisterWebhook unscoped: %v", err)
	}

	listed, err := f.webhookClient().ListWebhooks(ctx, connect.NewRequest(&orkstackv1.ListWebhooksRequest{}))
	if err != nil {
		t.Fatalf("ListWebhooks: %v", err)
	}
	var sawScoped bool
	for _, w := range listed.Msg.GetWebhooks() {
		if w.GetId() == hook.GetId() {
			sawScoped = w.GetNamespace() == "analytics" && w.GetOrkflowName() == "nightly-etl" && w.GetTaskId() == "extract"
		}
	}
	if !sawScoped {
		t.Errorf("ListWebhooks = %v, want %s with its scope", listed.Msg.GetWebhooks(), hook.GetId())
	}

	status, body := f.webhookJSON(t, http.MethodGet, api.WebhookCollectionPath, nil)
	if status != http.StatusOK {
		t.Fatalf("GET %s = %d: %s", api.WebhookCollectionPath, status, body)
	}
	var envelope struct {
		Webhooks []map[string]any `json:"webhooks"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if len(envelope.Webhooks) != 2 {
		t.Fatalf("GET listed %d webhooks, want 2: %s", len(envelope.Webhooks), body)
	}
	for _, w := range envelope.Webhooks {
		if w["id"] == hook.GetId() {
			if w["namespace"] != "analytics" || w["orkflowName"] != "nightly-etl" || w["taskId"] != "extract" {
				t.Errorf("JSON adapter row %v, want namespace/orkflowName/taskId keys", w)
			}
			continue
		}
		for _, key := range []string{"namespace", "orkflowName", "taskId"} {
			if _, ok := w[key]; ok {
				t.Errorf("an unscoped registration's JSON carries %q: %v", key, w)
			}
		}
	}

	// The JSON adapter accepts the same spelling its GET returns.
	status, body = f.webhookJSON(t, http.MethodPost, api.WebhookCollectionPath, map[string]any{
		"url": "https://example.test/json", "events": []string{"TASK_FAILED"},
		"namespace": "payments", "orkflowName": "charge", "taskId": "capture",
	})
	if status != http.StatusCreated {
		t.Fatalf("POST scoped = %d: %s", status, body)
	}
	var created struct {
		Webhook webhooks.Webhook `json:"webhook"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.Webhook.Namespace != "payments" || created.Webhook.OrkflowName != "charge" || created.Webhook.TaskID != "capture" {
		t.Errorf("POST returned %+v, want the scope it was given", created.Webhook)
	}
}

func TestWebhookAPI_RejectsAnInvalidScope(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	taskSucceeded := []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED}
	cases := []struct {
		name  string
		req   *orkstackv1.RegisterWebhookRequest
		json  map[string]any
		wants string
	}{
		{"url not absolute", &orkstackv1.RegisterWebhookRequest{Url: "/hooks"},
			map[string]any{"url": "/hooks"}, "scheme must be http or https"},
		{"TASK_LOG", &orkstackv1.RegisterWebhookRequest{Url: "https://example.test/h", Events: []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_TASK_LOG}},
			map[string]any{"url": "https://example.test/h", "events": []string{"TASK_LOG"}}, "unknown event"},
		{"namespace not a slug", &orkstackv1.RegisterWebhookRequest{Url: "https://example.test/h", Namespace: "Bad Name"},
			map[string]any{"url": "https://example.test/h", "namespace": "Bad Name"}, "namespace"},
		{"orkflow not a slug", &orkstackv1.RegisterWebhookRequest{Url: "https://example.test/h", OrkflowName: "-x"},
			map[string]any{"url": "https://example.test/h", "orkflowName": "-x"}, "orkflow"},
		{"task overlong", &orkstackv1.RegisterWebhookRequest{Url: "https://example.test/h", TaskId: "a" + strings.Repeat("b", 200), Events: taskSucceeded},
			map[string]any{"url": "https://example.test/h", "taskId": "a" + strings.Repeat("b", 200), "events": []string{"TASK_SUCCEEDED"}}, "task"},
		{"task without a task event", &orkstackv1.RegisterWebhookRequest{Url: "https://example.test/h", TaskId: "extract",
			Events: []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_RUN_FAILED}},
			map[string]any{"url": "https://example.test/h", "taskId": "extract", "events": []string{"RUN_FAILED"}}, "task event"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.webhookClient().RegisterWebhook(ctx, connect.NewRequest(tc.req))
			var connectErr *connect.Error
			if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeInvalidArgument {
				t.Errorf("Connect RegisterWebhook = %v, want InvalidArgument", err)
			} else if !strings.Contains(connectErr.Message(), tc.wants) {
				t.Errorf("Connect message %q does not explain %q", connectErr.Message(), tc.wants)
			}
			status, body := f.webhookJSON(t, http.MethodPost, api.WebhookCollectionPath, tc.json)
			if status != http.StatusBadRequest || !strings.Contains(string(body), "invalid_argument") {
				t.Errorf("JSON POST = %d %s, want 400 invalid_argument", status, body)
			}
			if !strings.Contains(string(body), tc.wants) {
				t.Errorf("JSON body %s does not explain %q", body, tc.wants)
			}
		})
	}
	listed, err := f.webhookClient().ListWebhooks(ctx, connect.NewRequest(&orkstackv1.ListWebhooksRequest{}))
	if err != nil {
		t.Fatalf("ListWebhooks: %v", err)
	}
	if len(listed.Msg.GetWebhooks()) != 0 {
		t.Errorf("refused registrations were stored: %v", listed.Msg.GetWebhooks())
	}
}

func TestWebhookNotifier_DeliversATaskOutcomeWithTaskIdAndAttempt(t *testing.T) {
	f := newNotifierFixture(t)
	if _, err := f.registry.Register(webhooks.Registration{
		URL: f.server.URL, Events: []webhooks.Event{webhooks.EventTaskSucceeded}, Secret: "s3cr3t", TaskID: "extract",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	run := &orkstackv1.Run{
		Id: "ork-1755302400123456789-1", OrkflowName: "nightly-etl", Status: orkstackv1.Status_STATUS_RUNNING,
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "extract", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 2, ValuesRef: &orkstackv1.ArtifactRef{Name: "values.json", Uri: "s3://bucket/values.json"}},
			{TaskId: "load", Status: orkstackv1.Status_STATUS_PENDING},
		},
	}
	f.adapter.NotifyRunEvent(context.Background(), &orkstackv1.Event{
		RunId: run.GetId(), Type: orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, TaskId: "extract",
		Status: orkstackv1.Status_STATUS_SUCCEEDED, Timestamp: timestamppb.New(time.Unix(1755302400, 0)),
	}, run)
	body, headers := f.awaitDelivery(t)
	if !webhooks.Verify("s3cr3t", body, headers.Get(webhooks.SignatureHeader)) {
		t.Error("the task delivery's signature does not verify")
	}
	if headers.Get(webhooks.TaskHeader) != "extract" || headers.Get(webhooks.EventHeader) != "TASK_SUCCEEDED" {
		t.Errorf("headers %s=%q %s=%q, want extract and TASK_SUCCEEDED", webhooks.TaskHeader,
			headers.Get(webhooks.TaskHeader), webhooks.EventHeader, headers.Get(webhooks.EventHeader))
	}
	var payload struct {
		TaskID  string `json:"taskId"`
		Attempt int    `json:"attempt"`
		Status  string `json:"status"`
		Run     struct {
			TaskRuns []struct {
				TaskID    string `json:"taskId"`
				ValuesRef struct {
					URI string `json:"uri"`
				} `json:"valuesRef"`
			} `json:"taskRuns"`
		} `json:"run"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if payload.TaskID != "extract" || payload.Attempt != 2 || payload.Status != "SUCCEEDED" {
		t.Errorf("body taskId=%q attempt=%d status=%q, want extract 2 SUCCEEDED: %s", payload.TaskID, payload.Attempt, payload.Status, body)
	}
	if len(payload.Run.TaskRuns) == 0 || payload.Run.TaskRuns[0].ValuesRef.URI != "s3://bucket/values.json" {
		t.Errorf("embedded run task runs %+v, want extract's valuesRef", payload.Run.TaskRuns)
	}
}

func TestWebhookNotifier_OmitsAttemptWhenTheTaskHasNone(t *testing.T) {
	f := newNotifierFixture(t)
	if _, err := f.registry.Register(webhooks.Registration{
		URL: f.server.URL, Events: []webhooks.Event{webhooks.EventTaskScheduled},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	run := &orkstackv1.Run{Id: "ork-1-1", OrkflowName: "nightly-etl",
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "extract", Status: orkstackv1.Status_STATUS_RUNNING}}}
	f.adapter.NotifyRunEvent(context.Background(), &orkstackv1.Event{
		RunId: run.GetId(), Type: orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, TaskId: "extract",
		Status: orkstackv1.Status_STATUS_RUNNING,
	}, run)
	body, _ := f.awaitDelivery(t)
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := decoded["attempt"]; ok {
		t.Errorf("a task with no numbered attempt was delivered with one: %s", body)
	}
	if string(decoded["taskId"]) != `"extract"` || string(decoded["status"]) != `"RUNNING"` {
		t.Errorf("taskId=%s status=%s, want extract RUNNING", decoded["taskId"], decoded["status"])
	}
}

func TestWebhookNotifier_ScopesByNamespaceTreatingEmptyAsDefault(t *testing.T) {
	f := newNotifierFixture(t)
	results := &observed{}
	f.notifier.Observe(results.add)
	defaultHook, err := f.registry.Register(webhooks.Registration{URL: f.server.URL + "/default", Namespace: "default"})
	if err != nil {
		t.Fatalf("Register default: %v", err)
	}
	analyticsHook, err := f.registry.Register(webhooks.Registration{URL: f.server.URL + "/analytics", Namespace: "analytics"})
	if err != nil {
		t.Fatalf("Register analytics: %v", err)
	}
	for _, run := range []*orkstackv1.Run{
		{Id: "ork-1-1", OrkflowName: "hello-ork", Namespace: ""},
		{Id: "ork-1-2", OrkflowName: "hello-ork", Namespace: "analytics"},
	} {
		f.adapter.NotifyRunEvent(context.Background(), &orkstackv1.Event{
			RunId: run.GetId(), Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, Status: orkstackv1.Status_STATUS_RUNNING,
		}, run)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.notifier.Wait(ctx); err != nil {
		t.Fatalf("deliveries did not settle: %v", err)
	}
	got := results.pairs()
	want := map[string]bool{defaultHook.ID + " ork-1-1": true, analyticsHook.ID + " ork-1-2": true}
	if len(got) != len(want) {
		t.Fatalf("delivered %v, want exactly %v", got, want)
	}
	for _, pair := range got {
		if !want[pair] {
			t.Errorf("unexpected delivery %s", pair)
		}
	}
}

func TestWebhookNotifier_IgnoresEventsThatAreNotSubscribable(t *testing.T) {
	cases := []struct {
		name   string
		event  orkstackv1.EventType
		status orkstackv1.Status
	}{
		{"TASK_LOG running", orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_RUNNING},
		{"TASK_LOG skipped", orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_SKIPPED},
		{"TASK_LOG canceled", orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_CANCELED},
		{"TASK_FAILED canceled", orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, orkstackv1.Status_STATUS_CANCELED},
		{"RUN_CREATED", orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, orkstackv1.Status_STATUS_PENDING},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newNotifierFixture(t)
			if _, err := f.registry.Register(webhooks.Registration{
				URL: f.server.URL, Events: webhooks.KnownEvents(), TaskID: "sibling",
			}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			run := &orkstackv1.Run{Id: "ork-1-1", OrkflowName: "hello-ork",
				TaskRuns: []*orkstackv1.TaskRun{{TaskId: "sibling", Status: tc.status}}}
			f.adapter.NotifyRunEvent(context.Background(),
				&orkstackv1.Event{RunId: run.GetId(), Type: tc.event, Status: tc.status, TaskId: "sibling"}, run)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := f.notifier.Wait(ctx); err != nil {
				t.Fatalf("deliveries did not settle: %v", err)
			}
			if f.count() != 0 {
				t.Errorf("%d deliveries went out for %s/%s", f.count(), tc.event, tc.status)
			}
		})
	}
}

func TestWebhookNotifier_RunTransitionBodyUnchangedForUnscopedSubscription(t *testing.T) {
	f := newNotifierFixture(t)
	if _, err := f.registry.Register(webhooks.Registration{URL: f.server.URL}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	run := &orkstackv1.Run{Id: "ork-1-1", OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "fetch", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1}}}
	f.adapter.NotifyRunEvent(context.Background(), &orkstackv1.Event{
		RunId: run.GetId(), Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, Status: orkstackv1.Status_STATUS_SUCCEEDED,
	}, run)
	body, headers := f.awaitDelivery(t)
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"taskId", "attempt"} {
		if _, ok := decoded[key]; ok {
			t.Errorf("a run transition's body carries %q: %s", key, body)
		}
	}
	if len(decoded) != 5 {
		t.Errorf("run body has %d keys, want the five it always had: %s", len(decoded), body)
	}
	if len(headers.Values(webhooks.TaskHeader)) != 0 {
		t.Errorf("a run transition carries %s", webhooks.TaskHeader)
	}
}

// observed collects (webhook, run) pairs off the notifier's Observe seam, so a
// negative is proven by counting results rather than by a timeout passing.
type observed struct {
	mu      sync.Mutex
	results []webhooks.Result
}

func (o *observed) add(result webhooks.Result) {
	o.mu.Lock()
	o.results = append(o.results, result)
	o.mu.Unlock()
}

func (o *observed) pairs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, 0, len(o.results))
	for _, result := range o.results {
		out = append(out, result.WebhookID+" "+result.RunID)
	}
	return out
}
