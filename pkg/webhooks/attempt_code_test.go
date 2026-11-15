package webhooks_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// canary is in every URL these tests send to: an attempt's record must hold
// none of it, because a webhook URL often carries a credential and an
// engine's attempt record travels through the execution cluster (P4d).
const canary = "canary-c4n4ry"

// closedPort returns a loopback address nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func sendOnce(t *testing.T, d *webhooks.Dispatcher, url string) webhooks.Attempt {
	t.Helper()
	hook := webhooks.Webhook{ID: "wh-0123456789abcdef", URL: url, Secret: "s"}
	delivery := webhooks.Delivery{ID: "dlv-1", Event: webhooks.EventRunSucceeded, RunID: "ork-1", Timestamp: time.Now()}
	return d.Send(context.Background(), hook, delivery, []byte(`{}`), "sig", 1)
}

// A failed request is recorded as a code and OrkStack's own sentence: never
// the URL, never the transport's words (which quote the URL).
func TestSend_AFailedRequestRecordsACodeAndNoTransportWords(t *testing.T) {
	// Only the timeout case has a short bound: the others must fail for their
	// own reason, never because a slow resolver or handshake outran it.
	loopback := webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: policy(t, "127.0.0.0/8"), RequestTimeout: 10 * time.Second})
	short := webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: policy(t, "127.0.0.0/8"), RequestTimeout: 300 * time.Millisecond})
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer slow.Close()
	defer close(release)
	untrusted := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer untrusted.Close()

	for _, tc := range []struct {
		name       string
		dispatcher *webhooks.Dispatcher
		url        string
		code       string
		retryable  bool
	}{
		{"connect", loopback, "http://" + closedPort(t) + "/" + canary + "?token=" + canary, webhooks.AttemptCodeConnect, true},
		{"timeout", short, slow.URL + "/" + canary + "?token=" + canary, webhooks.AttemptCodeTimeout, true},
		{"tls", loopback, untrusted.URL + "/" + canary + "?token=" + canary, webhooks.AttemptCodeTLS, true},
		{"blocked", webhooks.NewDispatcher(webhooks.DispatcherOptions{}), "http://" + closedPort(t) + "/" + canary, webhooks.AttemptCodeDestinationBlocked, false},
		{"dns", loopback, "http://no-such-receiver.invalid/" + canary + "?token=" + canary, webhooks.AttemptCodeDNS, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt := sendOnce(t, tc.dispatcher, tc.url)
			if attempt.Code != tc.code {
				t.Fatalf("Code = %q (error %q), want %q", attempt.Code, attempt.Error, tc.code)
			}
			if attempt.Error == "" || strings.Contains(attempt.Error, canary) || strings.Contains(attempt.Error, "127.0.0.1") {
				t.Errorf("Error = %q, want OrkStack's sentence with no URL and no address", attempt.Error)
			}
			for _, word := range []string{"dial", "Post", "x509", "connection refused", "tcp", "lookup"} {
				if strings.Contains(attempt.Error, word) {
					t.Errorf("Error = %q holds the transport's word %q", attempt.Error, word)
				}
			}
			if attempt.Retryable() != tc.retryable {
				t.Errorf("Retryable = %v, want %v", attempt.Retryable(), tc.retryable)
			}
			if attempt.Delivered() {
				t.Error("a failed attempt reads as delivered")
			}
			if cause := attempt.Cause(); cause == nil || strings.Contains(cause.Error(), canary) {
				t.Errorf("Cause = %v, want the transport's error without the URL", cause)
			}
		})
	}
}

// A refused destination still leads its error with the reason, so a record
// read by an older reader is judged the same way.
func TestSend_ARefusedDestinationLeadsWithItsReason(t *testing.T) {
	attempt := sendOnce(t, webhooks.NewDispatcher(webhooks.DispatcherOptions{}), "http://"+closedPort(t)+"/x")
	if !strings.HasPrefix(attempt.Error, webhooks.ReasonDestinationBlocked) {
		t.Errorf("Error = %q, want it to lead with %s", attempt.Error, webhooks.ReasonDestinationBlocked)
	}
}

// An attempt recorded by an earlier release carries no code: it is judged
// by its text, as it always was.
func TestAttempt_RecordWithoutACodeIsJudgedByItsText(t *testing.T) {
	old := webhooks.Attempt{Number: 1, Error: webhooks.ReasonDestinationBlocked + ": 10.0.0.1 is internal"}
	if old.Retryable() {
		t.Error("an earlier release's refused destination reads as retryable")
	}
	if !(webhooks.Attempt{Number: 1, Error: "dial tcp: connection refused"}).Retryable() {
		t.Error("an earlier release's transport failure reads as final")
	}
	if (webhooks.Attempt{Number: 1, StatusCode: 404}).Retryable() {
		t.Error("a 4xx reads as retryable")
	}
}

// A response is recorded as its status, with no code.
func TestSend_AnAnsweredRequestHasNoCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }))
	defer server.Close()
	d := webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: policy(t, "127.0.0.0/8")})
	attempt := sendOnce(t, d, server.URL+"/"+canary)
	if attempt.Code != "" || attempt.Error != "" || attempt.StatusCode != http.StatusBadGateway || !attempt.Retryable() {
		t.Errorf("attempt = %+v, want status 502, no code, no error, retryable", attempt)
	}
}
