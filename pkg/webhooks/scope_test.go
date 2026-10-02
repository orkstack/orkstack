package webhooks_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

var taskEvents = []webhooks.Event{
	webhooks.EventTaskScheduled, webhooks.EventTaskStarted,
	webhooks.EventTaskSucceeded, webhooks.EventTaskFailed,
}

func TestWebhook_MatchesScopeExactlyAndEmptyMeansEvery(t *testing.T) {
	event := webhooks.EventTaskSucceeded
	match := webhooks.Match{Event: event, Namespace: "analytics", OrkflowName: "nightly-etl", TaskID: "extract"}
	cases := []struct {
		name string
		hook webhooks.Webhook
		m    webhooks.Match
		want bool
	}{
		{"unscoped matches everything", webhooks.Webhook{Events: []webhooks.Event{event}}, match, true},
		{"namespace equal", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "analytics"}, match, true},
		{"namespace differs", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "payments"}, match, false},
		{"default versus another namespace", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "default"}, match, false},
		{"default matches default", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "default"},
			webhooks.Match{Event: event, Namespace: "default", OrkflowName: "nightly-etl", TaskID: "extract"}, true},
		{"orkflow equal", webhooks.Webhook{Events: []webhooks.Event{event}, OrkflowName: "nightly-etl"}, match, true},
		{"orkflow alone matches in every namespace", webhooks.Webhook{Events: []webhooks.Event{event}, OrkflowName: "nightly-etl"},
			webhooks.Match{Event: event, Namespace: "marketing", OrkflowName: "nightly-etl", TaskID: "extract"}, true},
		{"orkflow prefix is not a match", webhooks.Webhook{Events: []webhooks.Event{event}, OrkflowName: "etl"},
			webhooks.Match{Event: event, Namespace: "analytics", OrkflowName: "etl-nightly", TaskID: "extract"}, false},
		{"orkflow differs", webhooks.Webhook{Events: []webhooks.Event{event}, OrkflowName: "hourly"}, match, false},
		{"task equal", webhooks.Webhook{Events: []webhooks.Event{event}, TaskID: "extract"}, match, true},
		{"task differs", webhooks.Webhook{Events: []webhooks.Event{event}, TaskID: "load"}, match, false},
		{"task prefix is not a match", webhooks.Webhook{Events: []webhooks.Event{event}, TaskID: "ext"}, match, false},
		{"all three equal", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "analytics", OrkflowName: "nightly-etl", TaskID: "extract"}, match, true},
		{"all three but namespace", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "payments", OrkflowName: "nightly-etl", TaskID: "extract"}, match, false},
		{"all three but orkflow", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "analytics", OrkflowName: "hourly", TaskID: "extract"}, match, false},
		{"all three but task", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "analytics", OrkflowName: "nightly-etl", TaskID: "load"}, match, false},
		{"namespace and task", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "analytics", TaskID: "extract"}, match, true},
		{"case never matches", webhooks.Webhook{Events: []webhooks.Event{event}, Namespace: "Analytics"}, match, false},
		{"event not subscribed", webhooks.Webhook{Events: []webhooks.Event{webhooks.EventTaskFailed}}, match, false},
		{"empty events never matches a task event", webhooks.Webhook{Namespace: "analytics"}, match, false},
		{"empty events matches a run transition in scope", webhooks.Webhook{Namespace: "analytics"},
			webhooks.Match{Event: webhooks.EventRunFailed, Namespace: "analytics", OrkflowName: "nightly-etl"}, true},
		{"empty events and out of scope", webhooks.Webhook{Namespace: "payments"},
			webhooks.Match{Event: webhooks.EventRunFailed, Namespace: "analytics", OrkflowName: "nightly-etl"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.hook.Matches(tc.m); got != tc.want {
				t.Errorf("%+v.Matches(%+v) = %v, want %v", tc.hook, tc.m, got, tc.want)
			}
		})
	}
}

func TestWebhook_TaskScopeIsIgnoredForRunTransitions(t *testing.T) {
	hook := webhooks.Webhook{
		TaskID: "extract",
		Events: []webhooks.Event{webhooks.EventTaskSucceeded, webhooks.EventRunFailed},
	}
	if !hook.Matches(webhooks.Match{Event: webhooks.EventRunFailed, Namespace: "default", OrkflowName: "etl"}) {
		t.Error("a task-scoped subscription did not match the run's RUN_FAILED")
	}
	if !hook.Matches(webhooks.Match{Event: webhooks.EventTaskSucceeded, Namespace: "default", OrkflowName: "etl", TaskID: "extract"}) {
		t.Error("a task-scoped subscription did not match TASK_SUCCEEDED of its own task")
	}
	if hook.Matches(webhooks.Match{Event: webhooks.EventTaskSucceeded, Namespace: "default", OrkflowName: "etl", TaskID: "load"}) {
		t.Error("a task-scoped subscription matched TASK_SUCCEEDED of another task")
	}
}

func TestWebhook_EmptyEventsMeansRunTransitionsOnly(t *testing.T) {
	hook := webhooks.Webhook{}
	for _, event := range webhooks.RunEvents() {
		if !hook.Subscribes(event) {
			t.Errorf("an event-less subscription does not want %s", event)
		}
	}
	for _, event := range taskEvents {
		if hook.Subscribes(event) {
			t.Errorf("an event-less subscription wants %s; task events are opt-in", event)
		}
	}
	want := []webhooks.Event{webhooks.EventRunStarted, webhooks.EventRunSucceeded, webhooks.EventRunFailed, webhooks.EventRunCanceled}
	if got := webhooks.RunEvents(); !equalEvents(got, want) {
		t.Errorf("RunEvents() = %v, want %v", got, want)
	}
}

func TestParseEvent_AcceptsTaskOutcomesAndRefusesTaskLog(t *testing.T) {
	for _, known := range webhooks.KnownEvents() {
		for _, raw := range []string{string(known), "EVENT_TYPE_" + string(known), strings.ToLower(string(known))} {
			got, err := webhooks.ParseEvent(raw)
			if err != nil || got != known {
				t.Errorf("ParseEvent(%q) = (%q, %v), want %q", raw, got, err, known)
			}
		}
	}
	for _, raw := range []string{"TASK_LOG", "EVENT_TYPE_TASK_LOG", "task_log"} {
		_, err := webhooks.ParseEvent(raw)
		if err == nil {
			t.Errorf("ParseEvent(%q) accepted TASK_LOG; a log line is never subscribable", raw)
			continue
		}
		for _, known := range webhooks.KnownEvents() {
			if !strings.Contains(err.Error(), string(known)) {
				t.Errorf("ParseEvent(%q) error %q does not name %s", raw, err, known)
			}
		}
	}
}

func TestKnownEvents_KeepsRunTransitionsInTheirExistingRelativeOrder(t *testing.T) {
	want := []webhooks.Event{
		webhooks.EventRunStarted,
		webhooks.EventTaskScheduled, webhooks.EventTaskStarted,
		webhooks.EventTaskAwaiting, webhooks.EventTaskResolved,
		webhooks.EventTaskSucceeded, webhooks.EventTaskFailed,
		webhooks.EventRunSucceeded, webhooks.EventRunFailed, webhooks.EventRunCanceled,
	}
	if got := webhooks.KnownEvents(); !equalEvents(got, want) {
		t.Fatalf("KnownEvents() = %v, want %v", got, want)
	}
	var runs []webhooks.Event
	for _, event := range webhooks.KnownEvents() {
		if strings.HasPrefix(string(event), "RUN_") {
			runs = append(runs, event)
		}
	}
	if !equalEvents(runs, webhooks.RunEvents()) {
		t.Errorf("run transitions in KnownEvents() = %v, want the order RunEvents() keeps %v", runs, webhooks.RunEvents())
	}
}

func TestRegistry_NormalizesAMixedRunAndTaskListToLifecycleOrder(t *testing.T) {
	registry := webhooks.NewRegistry()
	hook := mustRegister(t, registry, webhooks.Registration{
		URL: "https://example.test/hooks",
		Events: []webhooks.Event{
			"RUN_FAILED", "task_failed", "EVENT_TYPE_TASK_SCHEDULED", "RUN_STARTED", "TASK_FAILED", "TASK_SUCCEEDED",
		},
	})
	want := []webhooks.Event{
		webhooks.EventRunStarted, webhooks.EventTaskScheduled, webhooks.EventTaskSucceeded,
		webhooks.EventTaskFailed, webhooks.EventRunFailed,
	}
	if !equalEvents(hook.Events, want) {
		t.Errorf("events = %v, want %v", hook.Events, want)
	}
}

func TestValidateScope_RefusesANonSlugAnOverlongComponentAndATaskWithoutATaskEvent(t *testing.T) {
	long := "a" + strings.Repeat("b", 200)
	success := []webhooks.Event{webhooks.EventTaskSucceeded}
	cases := []struct {
		name, namespace, orkflow, task string
		events                         []webhooks.Event
		field                          string
	}{
		{"upper case namespace", "Analytics", "", "", nil, "namespace"},
		{"leading hyphen orkflow", "", "-x", "", nil, "orkflow"},
		{"space in task", "", "", "a b", success, "task"},
		{"overlong namespace", long, "", "", nil, "namespace"},
		{"dots orkflow", "", "..", "", nil, "orkflow"},
		{"task with no events", "", "", "extract", nil, "task"},
		{"task with run events only", "", "", "extract", []webhooks.Event{webhooks.EventRunFailed}, "task"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := webhooks.ValidateScope(tc.namespace, tc.orkflow, tc.task, tc.events)
			if err == nil {
				t.Fatalf("ValidateScope(%q, %q, %q) accepted an invalid scope", tc.namespace, tc.orkflow, tc.task)
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Errorf("error %q does not name the %s field", err, tc.field)
			}
		})
	}
	for _, ok := range []struct{ namespace, orkflow, task string }{
		{"", "", ""},
		{"analytics", "nightly-etl", "extract"},
		{"default", "", ""},
		{"a" + strings.Repeat("b", 199), "", ""},
	} {
		if err := webhooks.ValidateScope(ok.namespace, ok.orkflow, ok.task, success); err != nil {
			t.Errorf("ValidateScope(%q, %q, %q) = %v, want accepted", ok.namespace, ok.orkflow, ok.task, err)
		}
	}
}

func TestRegistry_RefusesAnInvalidScopeAndStoresNothing(t *testing.T) {
	registry := webhooks.NewRegistry()
	if _, err := registry.Register(webhooks.Registration{URL: "https://example.test/hooks", Namespace: "Bad Name"}); err == nil {
		t.Fatal("Register accepted a namespace that is not a slug")
	}
	if _, err := registry.Register(webhooks.Registration{URL: "https://example.test/hooks", TaskID: "extract"}); err == nil {
		t.Fatal("Register accepted a task scope with no task event")
	}
	if got := registry.List(); len(got) != 0 {
		t.Errorf("rejected registrations were stored: %+v", got)
	}
}

func TestRegistry_MatchingSelectsByEventAndScope(t *testing.T) {
	registry := webhooks.NewRegistry()
	analytics := mustRegister(t, registry, webhooks.Registration{URL: "https://example.test/analytics", Namespace: "analytics"})
	etl := mustRegister(t, registry, webhooks.Registration{URL: "https://example.test/etl", OrkflowName: "nightly-etl",
		Events: []webhooks.Event{webhooks.EventTaskSucceeded, webhooks.EventRunSucceeded}})
	extract := mustRegister(t, registry, webhooks.Registration{URL: "https://example.test/extract", Namespace: "analytics",
		OrkflowName: "nightly-etl", TaskID: "extract", Events: []webhooks.Event{webhooks.EventTaskSucceeded}})
	all := mustRegister(t, registry, webhooks.Registration{URL: "https://example.test/all"})

	cases := []struct {
		m    webhooks.Match
		want []string
	}{
		{webhooks.Match{Event: webhooks.EventRunSucceeded, Namespace: "analytics", OrkflowName: "nightly-etl"},
			[]string{analytics.ID, etl.ID, all.ID}},
		{webhooks.Match{Event: webhooks.EventRunSucceeded, Namespace: "payments", OrkflowName: "charge"},
			[]string{all.ID}},
		{webhooks.Match{Event: webhooks.EventTaskSucceeded, Namespace: "analytics", OrkflowName: "nightly-etl", TaskID: "extract"},
			[]string{etl.ID, extract.ID}},
		{webhooks.Match{Event: webhooks.EventTaskSucceeded, Namespace: "analytics", OrkflowName: "nightly-etl", TaskID: "load"},
			[]string{etl.ID}},
		{webhooks.Match{Event: webhooks.EventTaskStarted, Namespace: "analytics", OrkflowName: "nightly-etl", TaskID: "extract"},
			nil},
	}
	for _, tc := range cases {
		var got []string
		for _, hook := range registry.Matching(tc.m) {
			got = append(got, hook.ID)
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("Matching(%+v) = %v, want %v", tc.m, got, tc.want)
		}
	}
}

func taskDelivery() webhooks.Delivery {
	delivery := sampleDelivery()
	delivery.Event = webhooks.EventTaskSucceeded
	delivery.Namespace = "analytics"
	delivery.TaskID = "extract"
	delivery.Attempt = 2
	return delivery
}

func TestDelivery_BodyCarriesTaskIdAndAttemptOnlyForTaskEvents(t *testing.T) {
	body, err := taskDelivery().Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	want := `{"runId":"ork-1755302400123456789-1","orkflowName":"hello-ork","status":"SUCCEEDED",` +
		`"timestamp":"2025-08-16T00:00:00Z","taskId":"extract","attempt":2,` +
		`"run":{"id":"ork-1755302400123456789-1","status":"SUCCEEDED"}}`
	if string(body) != want {
		t.Errorf("task body =\n%s\nwant\n%s", body, want)
	}
	if bytes.Contains(body, []byte(`"namespace"`)) {
		t.Errorf("the body carries the match-only namespace: %s", body)
	}

	noAttempt := taskDelivery()
	noAttempt.Attempt = 0
	body, err = noAttempt.Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := decoded["attempt"]; ok {
		t.Errorf("an unknown attempt was written as a number: %s", body)
	}
	if string(decoded["taskId"]) != `"extract"` {
		t.Errorf("taskId = %s, want \"extract\"", decoded["taskId"])
	}

	body, err = sampleDelivery().Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	if bytes.Contains(body, []byte("taskId")) || bytes.Contains(body, []byte("attempt")) {
		t.Errorf("a run delivery carries task keys: %s", body)
	}
}

func TestDelivery_RunTransitionBodyIsByteIdenticalToBeforeScope(t *testing.T) {
	// The golden was written by the encoder as it stood before the scope and
	// task fields existed; a run delivery must still produce those bytes, or
	// every receiver verifying a stored signature would break.
	golden, err := os.ReadFile("testdata/run-succeeded-body.golden")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	delivery := sampleDelivery()
	delivery.Namespace = "default" // match-only: must not reach the body
	body, err := delivery.Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	if !bytes.Equal(body, golden) {
		t.Errorf("run body =\n%s\nwant the pre-scope bytes\n%s", body, golden)
	}
}

func TestDispatcher_SetsTheTaskHeaderOnlyForTaskDeliveries(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	hook := webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s3cr3t"}
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{})
	if _, err := dispatcher.Deliver(context.Background(), hook, taskDelivery()); err != nil {
		t.Fatalf("Deliver task: %v", err)
	}
	if _, err := dispatcher.Deliver(context.Background(), hook, sampleDelivery()); err != nil {
		t.Fatalf("Deliver run: %v", err)
	}
	body, headers := rec.request(0)
	if got := headers.Get(webhooks.TaskHeader); got != "extract" {
		t.Errorf("%s = %q on a task delivery, want extract", webhooks.TaskHeader, got)
	}
	if got := headers.Get(webhooks.EventHeader); got != "TASK_SUCCEEDED" {
		t.Errorf("%s = %q, want TASK_SUCCEEDED", webhooks.EventHeader, got)
	}
	if !webhooks.Verify("s3cr3t", body, headers.Get(webhooks.SignatureHeader)) {
		t.Error("the task delivery's signature does not verify over its body")
	}
	if webhooks.TaskHeader != "X-Ork-Task-Id" {
		t.Errorf("TaskHeader = %q, want X-Ork-Task-Id", webhooks.TaskHeader)
	}
	if _, headers := rec.request(1); len(headers.Values(webhooks.TaskHeader)) != 0 {
		t.Errorf("a run delivery carries %s: %v", webhooks.TaskHeader, headers.Values(webhooks.TaskHeader))
	}
}

func TestNotifier_FansOutByScope(t *testing.T) {
	inScope := newReceiver(t, http.StatusOK)
	outOfScope := newReceiver(t, http.StatusOK)
	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{URL: inScope.server.URL, Namespace: "analytics"})
	mustRegister(t, registry, webhooks.Registration{URL: outOfScope.server.URL, Namespace: "payments"})

	results := &collector{}
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}), webhooks.NotifierOptions{})
	notifier.Observe(results.observe)
	delivery := sampleDelivery()
	delivery.Namespace = "analytics"
	if queued := notifier.Notify(context.Background(), delivery); queued != 1 {
		t.Errorf("queued %d deliveries, want exactly 1 (the analytics subscription)", queued)
	}
	drain(t, notifier)
	if got := results.urls(); len(got) != 1 || got[0] != inScope.server.URL {
		t.Errorf("delivered to %v, want only %s", got, inScope.server.URL)
	}
	if outOfScope.count() != 0 {
		t.Errorf("the payments subscription received %d deliveries for an analytics run", outOfScope.count())
	}
}

func equalEvents(got, want []webhooks.Event) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestKnownEvents_ListsAwaitEventsInLifecycleOrder(t *testing.T) {
	known := webhooks.KnownEvents()
	index := map[webhooks.Event]int{}
	for i, event := range known {
		index[event] = i
	}
	order := []webhooks.Event{
		webhooks.EventTaskStarted, webhooks.EventTaskAwaiting, webhooks.EventTaskResolved, webhooks.EventTaskSucceeded,
	}
	for i := 1; i < len(order); i++ {
		a, okA := index[order[i-1]]
		b, okB := index[order[i]]
		if !okA || !okB || a >= b {
			t.Fatalf("KnownEvents() = %v: want %s before %s", known, order[i-1], order[i])
		}
	}
	for _, raw := range []string{"TASK_AWAITING", "event_type_task_resolved"} {
		if _, err := webhooks.ParseEvent(raw); err != nil {
			t.Fatalf("ParseEvent(%q): %v", raw, err)
		}
	}
}
