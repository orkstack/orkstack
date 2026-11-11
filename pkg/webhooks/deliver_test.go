package webhooks_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
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
		Sequence:    7,
		Run:         stablejson.RawJSON(`{"id":"ork-1755302400123456789-1","status":"SUCCEEDED"}`),
	}
}

// backslash is one backslash, so the escapes below read as written.
const backslash = `\`

func TestDelivery_BodyRefusesARunAReceiverCannotRead(t *testing.T) {
	for _, tc := range []struct{ name, run, why string }{
		{"invalid UTF-8", "{\"note\":\"\xd2\"}", "the string at /note is not valid UTF-8"},
		{"half of a surrogate pair", `{"note":"` + backslash + `ud800"}`, "the string at /note holds half of a surrogate pair, not a character"},
		{"a key twice", `{"id":"a","id":"b"}`, `the key "id" appears twice in the top-level object`},
		{"not one value", `{"id":"a"} {}`, "after top-level value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivery := sampleDelivery()
			delivery.Run = stablejson.RawJSON(tc.run)
			body, err := delivery.Body()
			if err == nil {
				t.Fatalf("Body accepted the run %q: %s", tc.run, body)
			}
			if want := "webhooks: encode delivery dlv-1: the run is not one valid JSON value: "; !strings.HasPrefix(err.Error(), want) || !strings.Contains(err.Error(), tc.why) {
				t.Fatalf("Body(run %q) = %v; want %q naming %q", tc.run, err, want, tc.why)
			}
		})
	}
}

// The run is carried as the bytes its encoder wrote: an escape it holds is
// kept, so the body is the one 0.2.0 sent for the same run and a receiver
// that stored a signature for it still sees the same bytes.
func TestDelivery_BodyKeepsTheRunsEscapes(t *testing.T) {
	delivery := sampleDelivery()
	delivery.Run = stablejson.RawJSON(`{"note":"a` + backslash + `u007f` + backslash + `/b<"}`)
	body, err := delivery.Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	if want := `"run":{"note":"a` + backslash + `u007f` + backslash + `/b` + backslash + `u003c"}}`; !strings.HasSuffix(string(body), want) {
		t.Fatalf("Body = %s, want it to end %s", body, want)
	}
}

func TestDelivery_BodyCarriesRunIdOrkflowNameStatusTimestampSequenceAndRun(t *testing.T) {
	body, err := sampleDelivery().Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}

	var decoded map[string]stablejson.RawJSON
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, body)
	}
	for _, field := range []string{"runId", "orkflowName", "status", "timestamp", "sequence", "run"} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("payload has no %q field: %s", field, body)
		}
	}
	if len(decoded) != 6 {
		t.Errorf("payload has %d fields, want exactly the six in the contract: %s", len(decoded), body)
	}

	var flat struct {
		RunID       string `json:"runId"`
		OrkflowName string `json:"orkflowName"`
		Status      string `json:"status"`
		Timestamp   string `json:"timestamp"`
		Sequence    int64  `json:"sequence"`
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
	if flat.Sequence != 7 {
		t.Errorf("sequence = %d, want the run event's sequence 7", flat.Sequence)
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
		webhooks.SequenceHeader:  "7",
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

// The dispatcher makes ONE request and never retries: a retry is Temporal's
// (internal/engine, WebhookDeliveryWorkflow), so a crash cannot lose one.
func TestDispatcher_MakesOneAttemptAndNeverRetries(t *testing.T) {
	rec := newReceiver(t, http.StatusInternalServerError)
	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{}).Deliver(context.Background(),
		webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s"}, sampleDelivery())
	if err == nil || result.Delivered {
		t.Fatalf("Deliver = %+v, %v; want a failure against a 500", result, err)
	}
	if rec.count() != 1 || len(result.Attempts) != 1 {
		t.Fatalf("receiver saw %d requests and %d attempts were recorded, want exactly 1", rec.count(), len(result.Attempts))
	}
	if attempt := result.Attempts[0]; attempt.Number != 1 || attempt.StatusCode != http.StatusInternalServerError || !attempt.Retryable() {
		t.Errorf("attempt = %+v, want number 1, status 500, retryable", attempt)
	}
}

// Retryable is the one rule a retry policy keys on: a transport failure and a
// 5xx are retried, a 2xx is delivered, and anything else — a 4xx — is the
// sender's fault and never retried.
func TestAttempt_RetryableOnlyForTransportFailuresAndServerErrors(t *testing.T) {
	for _, tc := range []struct {
		attempt   webhooks.Attempt
		delivered bool
		retryable bool
	}{
		{attempt: webhooks.Attempt{StatusCode: http.StatusOK}, delivered: true},
		{attempt: webhooks.Attempt{StatusCode: http.StatusNoContent}, delivered: true},
		{attempt: webhooks.Attempt{StatusCode: http.StatusInternalServerError}, retryable: true},
		{attempt: webhooks.Attempt{StatusCode: http.StatusBadGateway}, retryable: true},
		{attempt: webhooks.Attempt{StatusCode: http.StatusServiceUnavailable}, retryable: true},
		{attempt: webhooks.Attempt{Error: "connection refused"}, retryable: true},
		{attempt: webhooks.Attempt{StatusCode: http.StatusBadRequest}},
		{attempt: webhooks.Attempt{StatusCode: http.StatusUnauthorized}},
		{attempt: webhooks.Attempt{StatusCode: http.StatusForbidden}},
		{attempt: webhooks.Attempt{StatusCode: http.StatusNotFound}},
		{attempt: webhooks.Attempt{StatusCode: http.StatusGone}},
		{attempt: webhooks.Attempt{StatusCode: http.StatusFound}},
	} {
		if got := tc.attempt.Delivered(); got != tc.delivered {
			t.Errorf("%+v Delivered = %v, want %v", tc.attempt, got, tc.delivered)
		}
		if got := tc.attempt.Retryable(); got != tc.retryable {
			t.Errorf("%+v Retryable = %v, want %v", tc.attempt, got, tc.retryable)
		}
	}
}

func TestDispatcher_RecordsAReceiverItCannotReach(t *testing.T) {
	// A port nothing listens on: the request fails in transport, with no status.
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := closed.URL
	closed.Close()

	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{RequestTimeout: 2 * time.Second}).
		Deliver(context.Background(), webhooks.Webhook{ID: "wh-1", URL: url}, sampleDelivery())
	if err == nil {
		t.Fatal("Deliver reported success against a receiver that is not listening")
	}
	attempt := result.LastAttempt()
	if attempt.Error == "" || attempt.StatusCode != 0 || !attempt.Retryable() {
		t.Errorf("attempt = %+v, want a transport error, no status, retryable", attempt)
	}
}

// Send numbers the attempt it is told to, so a retry Temporal schedules says
// which try it is, over the same body and delivery id.
func TestDispatcher_SendNumbersTheAttemptAndKeepsTheBody(t *testing.T) {
	rec := newReceiver(t, http.StatusInternalServerError)
	hook := webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s"}
	delivery := sampleDelivery()
	body, err := delivery.Body()
	if err != nil {
		t.Fatal(err)
	}
	signature := webhooks.Sign(hook.Secret, body)
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{})
	for n := 1; n <= 3; n++ {
		if attempt := dispatcher.Send(context.Background(), hook, delivery, body, signature, n); attempt.Number != n || attempt.At.IsZero() {
			t.Fatalf("Send(%d) = %+v", n, attempt)
		}
	}
	for i := 0; i < rec.count(); i++ {
		got, headers := rec.request(i)
		if string(got) != string(body) {
			t.Errorf("attempt %d changed the body, which would invalidate the signature", i+1)
		}
		if headers.Get(webhooks.AttemptHeader) != strconv.Itoa(i+1) {
			t.Errorf("attempt %d reported attempt %q", i+1, headers.Get(webhooks.AttemptHeader))
		}
		if headers.Get(webhooks.DeliveryHeader) != delivery.ID {
			t.Errorf("attempt %d changed the delivery id, so a receiver cannot deduplicate", i+1)
		}
		if headers.Get(webhooks.SequenceHeader) != "7" {
			t.Errorf("attempt %d sent sequence %q, want the event's 7 on every attempt", i+1, headers.Get(webhooks.SequenceHeader))
		}
	}
}

// A delivery whose event carried no sequence — a redelivery of a record
// written before deliveries carried one — says nothing about order rather
// than claiming 0.
func TestDispatcher_OmitsASequenceTheEventDidNotCarry(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	hook := webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s"}
	delivery := sampleDelivery()
	delivery.Sequence = 0
	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{}).Deliver(context.Background(), hook, delivery)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	body, headers := rec.request(0)
	var decoded map[string]stablejson.RawJSON
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, body)
	}
	if raw, present := decoded["sequence"]; present {
		t.Errorf("body carries sequence %s for an event that had none: %s", raw, body)
	}
	if _, present := headers[http.CanonicalHeaderKey(webhooks.SequenceHeader)]; present {
		t.Errorf("header %s sent for an event that had none: %q", webhooks.SequenceHeader, headers.Get(webhooks.SequenceHeader))
	}
	if result.Sequence != 0 {
		t.Errorf("result records sequence %d, want none", result.Sequence)
	}
}

// The engine's delivery workflow carries a Delivery in its input. A zero
// sequence is omitted there, so an input queued before deliveries carried one
// encodes exactly as its history recorded it.
func TestDelivery_InputEncodingCarriesASequenceOnlyWhenItHasOne(t *testing.T) {
	delivery := sampleDelivery()
	delivery.Run = nil
	encoded, err := stablejson.Marshal(delivery)
	if err != nil {
		t.Fatal(err)
	}
	var decoded webhooks.Delivery
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Sequence != 7 {
		t.Fatalf("decoded sequence %d (%v) from %s, want 7", decoded.Sequence, err, encoded)
	}
	delivery.Sequence = 0
	encoded, err = stablejson.Marshal(delivery)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "Sequence") {
		t.Errorf("a delivery with no sequence encodes one: %s", encoded)
	}
}

func TestNewResult_RecordsTheDeliverysSequence(t *testing.T) {
	delivery := sampleDelivery()
	result := webhooks.NewResult(webhooks.Webhook{ID: "wh-1", URL: "https://example.test/hook"}, delivery, []byte("{}"))
	if result.Sequence != 7 {
		t.Fatalf("result records sequence %d, want the delivery's 7", result.Sequence)
	}
	if unqueued := webhooks.NotQueued(webhooks.Webhook{ID: "wh-1"}, delivery, errors.New("store down")); unqueued.Sequence != 7 {
		t.Errorf("a delivery that could not be queued records sequence %d, want the delivery's 7", unqueued.Sequence)
	}
}
