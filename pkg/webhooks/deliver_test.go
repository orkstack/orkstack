package webhooks_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// sampleDelivery is one finished run on its way to a subscriber.
func sampleDelivery() webhooks.Delivery {
	return webhooks.Delivery{
		ID:          "dlv-1",
		Event:       webhooks.EventRunSucceeded,
		RunID:       "ork-1755302400123456789-1",
		OrkflowName: "hello-ork",
		Status:      "SUCCEEDED",
		Timestamp:   time.Unix(1755302400, 0).UTC(),
		Run:         json.RawMessage(`{"id":"ork-1755302400123456789-1","status":"SUCCEEDED"}`),
	}
}

func TestDelivery_BodyCarriesRunIdOrkflowNameStatusTimestampAndRun(t *testing.T) {
	body, err := sampleDelivery().Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, body)
	}
	for _, field := range []string{"runId", "orkflowName", "status", "timestamp", "run"} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("payload has no %q field: %s", field, body)
		}
	}
	if len(decoded) != 5 {
		t.Errorf("payload has %d fields, want exactly the five in the contract: %s", len(decoded), body)
	}

	var flat struct {
		RunID       string `json:"runId"`
		OrkflowName string `json:"orkflowName"`
		Status      string `json:"status"`
		Timestamp   string `json:"timestamp"`
		Run         struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"run"`
	}
	if err := json.Unmarshal(body, &flat); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if flat.RunID != "ork-1755302400123456789-1" {
		t.Errorf("runId = %q", flat.RunID)
	}
	if flat.OrkflowName != "hello-ork" {
		t.Errorf("orkflowName = %q", flat.OrkflowName)
	}
	if flat.Status != "SUCCEEDED" {
		t.Errorf("status = %q, want the enum without its proto prefix", flat.Status)
	}
	if flat.Timestamp != "2025-08-16T00:00:00Z" {
		t.Errorf("timestamp = %q, want RFC3339 in UTC", flat.Timestamp)
	}
	if flat.Run.ID != flat.RunID || flat.Run.Status != "SUCCEEDED" {
		t.Errorf("run = %+v, want the full run the transition produced", flat.Run)
	}
}

// receiver records the requests a dispatcher makes and answers with a
// caller-supplied sequence of statuses.
type receiver struct {
	mu       sync.Mutex
	statuses []int
	bodies   [][]byte
	headers  []http.Header
	server   *httptest.Server
}

func newReceiver(t *testing.T, statuses ...int) *receiver {
	t.Helper()
	r := &receiver{statuses: statuses}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		index := len(r.bodies)
		r.bodies = append(r.bodies, body)
		r.headers = append(r.headers, req.Header.Clone())
		status := http.StatusOK
		if index < len(r.statuses) {
			status = r.statuses[index]
		} else if len(r.statuses) > 0 {
			status = r.statuses[len(r.statuses)-1]
		}
		r.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func (r *receiver) request(i int) ([]byte, http.Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bodies[i], r.headers[i]
}

// recordingSleeper stands in for the backoff wait so the retry ladder is
// observable without spending real time.
type recordingSleeper struct {
	mu       sync.Mutex
	recorded []time.Duration
}

func (s *recordingSleeper) sleep(_ context.Context, d time.Duration) error {
	s.mu.Lock()
	s.recorded = append(s.recorded, d)
	s.mu.Unlock()
	return nil
}

func (s *recordingSleeper) durations() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration{}, s.recorded...)
}

func TestDispatcher_SignsTheBodyItPostsAndLabelsTheDelivery(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	hook := webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s3cr3t"}
	delivery := sampleDelivery()

	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{}).
		Deliver(context.Background(), hook, delivery)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if !result.Delivered || len(result.Attempts) != 1 {
		t.Fatalf("result = %+v, want delivered in one attempt", result)
	}

	body, headers := rec.request(0)
	signature := headers.Get(webhooks.SignatureHeader)
	if !webhooks.Verify("s3cr3t", body, signature) {
		t.Errorf("the receiver could not verify %s=%q over the body it read", webhooks.SignatureHeader, signature)
	}
	if webhooks.Verify("wrong-secret", body, signature) {
		t.Error("a receiver holding the wrong secret verified the delivery")
	}
	for header, expected := range map[string]string{
		"Content-Type":           "application/json",
		"User-Agent":             webhooks.UserAgent,
		webhooks.EventHeader:     "RUN_SUCCEEDED",
		webhooks.DeliveryHeader:  "dlv-1",
		webhooks.WebhookHeader:   "wh-1",
		webhooks.RunHeader:       delivery.RunID,
		webhooks.TimestampHeader: "2025-08-16T00:00:00Z",
		webhooks.AttemptHeader:   "1",
	} {
		if got := headers.Get(header); got != expected {
			t.Errorf("header %s = %q, want %q", header, got, expected)
		}
	}
}

func TestDispatcher_OmitsTheSignatureWhenTheSubscriptionHasNoSecret(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	hook := webhooks.Webhook{ID: "wh-1", URL: rec.server.URL}

	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{}).
		Deliver(context.Background(), hook, sampleDelivery())
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.Signature != "" {
		t.Errorf("result signature = %q, want none", result.Signature)
	}
	if _, headers := rec.request(0); headers.Get(webhooks.SignatureHeader) != "" {
		t.Error("an unsigned delivery carried a signature header, which a receiver would trust")
	}
}

func TestDispatcher_RetriesA5xxWithExponentialBackoffAndGivesUpAfterThree(t *testing.T) {
	rec := newReceiver(t, http.StatusInternalServerError)
	sleeper := &recordingSleeper{}
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{Sleep: sleeper.sleep})

	result, err := dispatcher.Deliver(context.Background(),
		webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s"}, sampleDelivery())
	if err == nil {
		t.Fatal("Deliver reported success against a receiver that only ever answered 500")
	}
	if result.Delivered {
		t.Error("result claims delivery against a receiver that only ever answered 500")
	}
	// One attempt plus the three retries the contract allows.
	if rec.count() != 4 {
		t.Errorf("receiver saw %d requests, want 4 (1 attempt + %d retries)",
			rec.count(), webhooks.DefaultMaxRetries)
	}
	if len(result.Attempts) != 4 {
		t.Fatalf("recorded %d attempts, want 4: %+v", len(result.Attempts), result.Attempts)
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	got := sleeper.durations()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("backoff ladder = %v, want exponential %v", got, want)
	}
	for i, attempt := range result.Attempts {
		if attempt.Number != i+1 {
			t.Errorf("attempt %d numbered %d", i, attempt.Number)
		}
		if attempt.StatusCode != http.StatusInternalServerError {
			t.Errorf("attempt %d status = %d, want 500", i+1, attempt.StatusCode)
		}
	}
	// Every attempt sends the identical signed body, and says which try it is.
	firstBody, _ := rec.request(0)
	for i := 1; i < rec.count(); i++ {
		body, headers := rec.request(i)
		if string(body) != string(firstBody) {
			t.Errorf("retry %d changed the body, which would invalidate the signature", i+1)
		}
		if headers.Get(webhooks.AttemptHeader) != strconv.Itoa(i+1) {
			t.Errorf("retry %d reported attempt %q", i+1, headers.Get(webhooks.AttemptHeader))
		}
		if headers.Get(webhooks.DeliveryHeader) != "dlv-1" {
			t.Errorf("retry %d changed the delivery id, so a receiver cannot deduplicate", i+1)
		}
	}
}

func TestDispatcher_StopsRetryingOnceTheReceiverAccepts(t *testing.T) {
	rec := newReceiver(t, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusNoContent)
	sleeper := &recordingSleeper{}
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{Sleep: sleeper.sleep})

	result, err := dispatcher.Deliver(context.Background(),
		webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s"}, sampleDelivery())
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if !result.Delivered {
		t.Error("result does not record the delivery the receiver accepted")
	}
	if rec.count() != 3 {
		t.Errorf("receiver saw %d requests, want 3: two rejections then the acceptance", rec.count())
	}
	if got := result.LastAttempt().StatusCode; got != http.StatusNoContent {
		t.Errorf("last attempt status = %d, want 204", got)
	}
	if len(sleeper.durations()) != 2 {
		t.Errorf("backed off %d times, want 2", len(sleeper.durations()))
	}
}

func TestDispatcher_DoesNotRetryARejectionTheSenderCaused(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusGone,
	} {
		rec := newReceiver(t, status)
		sleeper := &recordingSleeper{}
		dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{Sleep: sleeper.sleep})

		result, err := dispatcher.Deliver(context.Background(),
			webhooks.Webhook{ID: "wh-1", URL: rec.server.URL}, sampleDelivery())
		if err == nil {
			t.Errorf("status %d: Deliver reported success", status)
		}
		if rec.count() != 1 {
			t.Errorf("status %d: receiver saw %d requests, want 1 (a 4xx is the sender's fault)",
				status, rec.count())
		}
		if len(result.Attempts) != 1 {
			t.Errorf("status %d: recorded %d attempts, want 1", status, len(result.Attempts))
		}
		if len(sleeper.durations()) != 0 {
			t.Errorf("status %d: backed off before giving up on an unretryable answer", status)
		}
	}
}

func TestDispatcher_RetriesAReceiverItCannotReach(t *testing.T) {
	// A port nothing listens on: the request fails in transport, with no status.
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := closed.URL
	closed.Close()

	sleeper := &recordingSleeper{}
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{
		Sleep:          sleeper.sleep,
		RequestTimeout: 2 * time.Second,
	})
	result, err := dispatcher.Deliver(context.Background(),
		webhooks.Webhook{ID: "wh-1", URL: url}, sampleDelivery())
	if err == nil {
		t.Fatal("Deliver reported success against a receiver that is not listening")
	}
	if len(result.Attempts) != 4 {
		t.Fatalf("recorded %d attempts, want 4: a transport failure is retryable", len(result.Attempts))
	}
	for _, attempt := range result.Attempts {
		if attempt.Error == "" {
			t.Errorf("attempt %d recorded no error", attempt.Number)
		}
		if attempt.StatusCode != 0 {
			t.Errorf("attempt %d recorded status %d, but no response arrived", attempt.Number, attempt.StatusCode)
		}
	}
}

func TestDispatcher_HonoursACanceledContextMidLadder(t *testing.T) {
	rec := newReceiver(t, http.StatusInternalServerError)
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{
		Sleep: func(context.Context, time.Duration) error {
			cancel()
			return context.Canceled
		},
	})

	result, err := dispatcher.Deliver(ctx,
		webhooks.Webhook{ID: "wh-1", URL: rec.server.URL}, sampleDelivery())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Deliver error = %v, want context.Canceled", err)
	}
	if rec.count() != 1 {
		t.Errorf("receiver saw %d requests, want the one sent before cancellation", rec.count())
	}
	if result.Delivered {
		t.Error("a canceled delivery reported success")
	}
}

func TestDispatcher_ARetryBudgetOfZeroSendsOnce(t *testing.T) {
	rec := newReceiver(t, http.StatusInternalServerError)
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{MaxRetries: -1})

	if _, err := dispatcher.Deliver(context.Background(),
		webhooks.Webhook{ID: "wh-1", URL: rec.server.URL}, sampleDelivery()); err == nil {
		t.Fatal("Deliver reported success against a 500")
	}
	if rec.count() != 1 {
		t.Errorf("receiver saw %d requests, want exactly 1", rec.count())
	}
}
