package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// recordingDeliveryStore is the real in-memory store with every query the
// engine made of it remembered, so a test can see what the CLI sent.
type recordingDeliveryStore struct {
	*artifacts.MemoryDeliveryStore
	mu      sync.Mutex
	queries []artifacts.DeliveryQuery
}

func (r *recordingDeliveryStore) ListDeliveries(ctx context.Context, q artifacts.DeliveryQuery) (artifacts.DeliveryPage, error) {
	r.mu.Lock()
	r.queries = append(r.queries, q)
	r.mu.Unlock()
	return r.MemoryDeliveryStore.ListDeliveries(ctx, q)
}

func (r *recordingDeliveryStore) lastQuery(t *testing.T) artifacts.DeliveryQuery {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queries) == 0 {
		t.Fatal("the delivery store was never queried")
	}
	return r.queries[len(r.queries)-1]
}

// cliReceiver is the subscriber a redelivery reaches.
type cliReceiver struct {
	mu      sync.Mutex
	status  int
	headers []http.Header
	server  *httptest.Server
}

func (r *cliReceiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.headers)
}

const (
	deliveriesRunID   = "ork-2026-01-01-8f31"
	deliveriesWebhook = "wh-4a1c0e2b7d3f8159"
)

// deliveriesFixture is the real API over a seeded delivery store, one
// registered subscription whose URL is a local receiver, and the run the
// records name, created an hour before the fixture's clock.
func deliveriesFixture(t *testing.T) (*fixture, *recordingDeliveryStore, *cliReceiver) {
	t.Helper()
	receiver := &cliReceiver{status: http.StatusOK}
	receiver.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		receiver.mu.Lock()
		receiver.headers = append(receiver.headers, req.Header.Clone())
		status := receiver.status
		receiver.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(receiver.server.Close)

	registry := webhooks.NewRegistry(webhooks.WithIDGenerator(func() (string, error) { return deliveriesWebhook, nil }))
	if _, err := registry.Register(webhooks.Registration{
		URL: receiver.server.URL, Secret: "s3cr3t",
		Events: []webhooks.Event{webhooks.EventRunStarted, webhooks.EventRunFailed, webhooks.EventTaskFailed},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	store := &recordingDeliveryStore{MemoryDeliveryStore: artifacts.NewMemoryDeliveryStore()}
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Deliveries = store
		cfg.Webhooks = registry
	})
	if _, err := f.namespaces.Put(context.Background(), &orkstackv1.Namespace{Name: "payments"}, artifacts.PutConditions{}); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	f.runs.Save(&orkstackv1.Run{
		Id: deliveriesRunID, Namespace: "payments", OrkflowName: "reconcile", OrkflowVersion: "0.1.0",
		Status: orkstackv1.Status_STATUS_FAILED, CreatedAt: timestamppb.New(fixedNow.Add(-time.Hour)),
	})

	start := fixedNow.Add(-50 * time.Minute)
	seed := []struct {
		offset    time.Duration
		hex       string
		event     orkstackv1.EventType
		status    orkstackv1.Status
		task      string
		attempt   int32
		codes     []int32
		delivered bool
	}{
		{0, "71b0cc19e2d4a6f3", orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING, "", 0, []int32{200}, true},
		{time.Minute, "0d9e5b3a7c61f284", orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, orkstackv1.Status_STATUS_FAILED, "settle", 2, []int32{400}, false},
		{3 * time.Minute, "9c22a1f04e7b3d58", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED, "", 0, []int32{502, 502, 502, 502}, false},
	}
	backoffs := []time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	for _, s := range seed {
		queued := start.Add(s.offset).Add(891204113)
		rec := &orkstackv1.Delivery{
			Id:        "dlv-" + queued.Format(webhooks.DeliveryIDLayout) + "-" + s.hex,
			WebhookId: deliveriesWebhook, Event: s.event, RunId: deliveriesRunID, Namespace: "payments",
			OrkflowName: "reconcile", TaskId: s.task, Attempt: s.attempt, Status: s.status,
			Url: "https://payments.example/hooks/orkstack", Delivered: s.delivered,
			Signature: "sha256=3f9a" + strings.Repeat("0", 60), BodySha256: "7c1e" + strings.Repeat("0", 60), BodyBytes: 1284,
			EventAt: timestamppb.New(queued.Add(-7 * time.Millisecond)), CreatedAt: timestamppb.New(queued),
		}
		at := queued.Add(11 * time.Millisecond)
		for i, code := range s.codes {
			at = at.Add(backoffs[i])
			attempt := &orkstackv1.DeliveryAttempt{
				Number: int32(i + 1), StatusCode: code, At: timestamppb.New(at),
				Backoff: durationpb.New(backoffs[i]), Duration: durationpb.New(time.Duration(84-10*i) * time.Millisecond),
			}
			if code >= 300 {
				attempt.Error = fmt.Sprintf("receiver answered %d", code)
			}
			rec.Attempts = append(rec.Attempts, attempt)
			at = at.Add(attempt.GetDuration().AsDuration())
		}
		rec.SettledAt = timestamppb.New(at)
		if err := store.PutDelivery(context.Background(), rec); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return f, store, receiver
}

const failedDeliveryID = "dlv-20260102T021705.891204113Z-9c22a1f04e7b3d58"

func TestWebhookDeliveries_Golden(t *testing.T) {
	f, _, _ := deliveriesFixture(t)

	human := f.run("webhook", "deliveries").requireExit(t, exitOK)
	requireGolden(t, "webhook-deliveries.golden", human.stdout)

	machine := f.run("--json", "webhook", "deliveries").requireExit(t, exitOK)
	requireGolden(t, "webhook-deliveries.json.golden", machine.stdout)
	var document struct {
		Deliveries []struct {
			Event string `json:"event"`
		} `json:"deliveries"`
	}
	if err := json.Unmarshal([]byte(machine.stdout), &document); err != nil || len(document.Deliveries) != 3 ||
		document.Deliveries[0].Event != "RUN_FAILED" {
		t.Fatalf("--json is not the raw response with three records in the JSON dialect: %v\n%s", err, machine.stdout)
	}

	empty := f.run("webhook", "deliveries", "--event", "RUN_CANCELED").requireExit(t, exitOK)
	requireGolden(t, "webhook-deliveries-empty.golden", empty.stdout)

	paged := f.run("webhook", "deliveries", "--limit", "2", "--webhook", deliveriesWebhook,
		"--since", "2026-01-01").requireExit(t, exitOK)
	requireGolden(t, "webhook-deliveries-next-page.golden", paged.stdout)
	line := paged.stdout[strings.Index(paged.stdout, "Next page: orkctl ")+len("Next page: orkctl "):]
	line = strings.TrimSpace(strings.SplitN(line, "\n", 2)[0])
	next := f.run(strings.Fields(line)...).requireExit(t, exitOK)
	if !strings.Contains(next.stdout, "71b0cc19e2d4a6f3") || strings.Contains(next.stdout, "Next page") ||
		strings.Contains(next.stdout, "9c22a1f04e7b3d58") {
		t.Errorf("the next-page command did not continue the listing without overlap:\n%s", next.stdout)
	}
}

func TestWebhookDeliveries_NamespaceOnlyFromCommandLine(t *testing.T) {
	f, store, _ := deliveriesFixture(t)
	t.Setenv(namespaceEnvVar, "payments")

	every := f.run("webhook", "deliveries").requireExit(t, exitOK)
	if q := store.lastQuery(t); q.Namespace != "" {
		t.Errorf("with only ORK_NAMESPACE set the CLI sent namespace %q, want empty (every namespace)", q.Namespace)
	}
	if !strings.Contains(every.stdout, "Scope: every namespace") {
		t.Errorf("scope line missing:\n%s", every.stdout)
	}
	one := f.run("-n", "payments", "webhook", "deliveries").requireExit(t, exitOK)
	if q := store.lastQuery(t); q.Namespace != "payments" {
		t.Errorf("-n payments sent namespace %q", q.Namespace)
	}
	if !strings.Contains(one.stdout, "Scope: namespace payments") {
		t.Errorf("scope line does not name the namespace:\n%s", one.stdout)
	}
}

func TestWebhookDeliveries_RunDefaultsSinceToTheRunsStart(t *testing.T) {
	f, store, _ := deliveriesFixture(t)
	out := f.run("webhook", "deliveries", "--run", deliveriesRunID).requireExit(t, exitOK)
	if q := store.lastQuery(t); !q.Since.Equal(fixedNow.Add(-time.Hour)) || q.RunID != deliveriesRunID {
		t.Errorf("query = %+v, want since = the run's created_at", q)
	}
	if !strings.Contains(out.stdout, "Scope: every namespace, 2026-01-02T02:04:05Z (the run's start) to 2026-01-02T03:04:05Z") {
		t.Errorf("scope line does not name the run's start:\n%s", out.stdout)
	}
}

func TestWebhookDeliveries_RefusesBadFiltersLocally(t *testing.T) {
	f, _, _ := deliveriesFixture(t)
	for _, args := range [][]string{
		{"--event", "TASK_LOG"},
		{"--event", "RUN_CREATED"},
		{"--delivered", "maybe"},
		{"--until", "24h"},
		{"--since", "yesterday"},
		{"--limit", "201"},
	} {
		before := len(f.requests())
		res := f.run(append([]string{"webhook", "deliveries"}, args...)...)
		if res.code == exitOK {
			t.Errorf("orkctl webhook deliveries %v succeeded", args)
		}
		if len(f.requests()) != before {
			t.Errorf("orkctl webhook deliveries %v sent a request before refusing", args)
		}
	}
	before := len(f.requests())
	if res := f.run("webhook", "delivery", "dlv-not-an-id"); res.code == exitOK || len(f.requests()) != before {
		t.Errorf("a malformed delivery id reached the server or succeeded: %+v", res)
	}
}

func TestWebhookDelivery_Golden(t *testing.T) {
	f, _, _ := deliveriesFixture(t)
	human := f.run("webhook", "delivery", failedDeliveryID).requireExit(t, exitOK)
	requireGolden(t, "webhook-delivery.golden", human.stdout)
	machine := f.run("--json", "webhook", "delivery", failedDeliveryID).requireExit(t, exitOK)
	requireGolden(t, "webhook-delivery.json.golden", machine.stdout)

	task := f.run("webhook", "delivery", "dlv-20260102T021505.891204113Z-0d9e5b3a7c61f284").requireExit(t, exitOK)
	task.requireContains(t, "TASK", "settle, attempt 2")
}

// normalizeRedelivery replaces what a real POST makes vary — the new id, the
// wall-clock instants, the durations — so the rest can be a golden file.
func normalizeRedelivery(out, newID string) string {
	out = strings.ReplaceAll(out, newID, "dlv-<new>")
	out = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z`).ReplaceAllStringFunc(out, func(s string) string {
		if strings.HasPrefix(s, "2026-01-0") {
			return s
		}
		return "<now>"
	})
	out = regexp.MustCompile(`\b\d+(\.\d+)?(ms|µs|ns|s)\b`).ReplaceAllString(out, "<dur>")
	out = regexp.MustCompile(`http://127\.0\.0\.1:\d+`).ReplaceAllString(out, "http://<receiver>")
	out = regexp.MustCompile(` {2,}`).ReplaceAllString(out, "  ")
	return out
}

func TestWebhookRedeliver_ReportsTheOutcomeAndExitsNonzeroWhenNotDelivered(t *testing.T) {
	f, store, receiver := deliveriesFixture(t)

	ok := f.run("webhook", "redeliver", failedDeliveryID).requireExit(t, exitOK)
	ok.requireContains(t, "Redelivered "+failedDeliveryID+" as dlv-", ": delivered, 200 in ", "REDELIVERY OF")
	if receiver.count() != 1 || receiver.headers[0].Get(webhooks.RedeliveryHeader) != failedDeliveryID {
		t.Fatalf("receiver saw %d requests (%v), want one carrying %s", receiver.count(), receiver.headers, webhooks.RedeliveryHeader)
	}
	newID := receiver.headers[0].Get(webhooks.DeliveryHeader)
	requireGolden(t, "webhook-redeliver.golden", normalizeRedelivery(ok.stdout, newID))
	if _, found, err := store.GetDelivery(context.Background(), newID); err != nil || !found {
		t.Errorf("the redelivery was not recorded: %v, %v", found, err)
	}

	receiver.mu.Lock()
	receiver.status = http.StatusBadGateway
	receiver.mu.Unlock()
	failed := f.run("webhook", "redeliver", failedDeliveryID).requireExit(t, exitError)
	failed.requireContains(t, ": not delivered, 502 in ")
	if !strings.Contains(failed.stderr, "was not delivered") {
		t.Errorf("stderr does not say the redelivery failed: %s", failed.stderr)
	}
	newID = receiver.headers[1].Get(webhooks.DeliveryHeader)
	requireGolden(t, "webhook-redeliver-failed.golden", normalizeRedelivery(failed.stdout, newID))

	machine := f.run("--json", "webhook", "redeliver", failedDeliveryID).requireExit(t, exitError)
	var document struct {
		Delivery struct {
			Delivered    bool   `json:"delivered"`
			RedeliveryOf string `json:"redeliveryOf"`
		} `json:"delivery"`
	}
	if err := json.Unmarshal([]byte(machine.stdout), &document); err != nil || document.Delivery.RedeliveryOf != failedDeliveryID {
		t.Errorf("--json did not print the response before exiting 1: %v\n%s", err, machine.stdout)
	}
}
