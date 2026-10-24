package webhooks_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// collector accumulates delivery results off the notifier's goroutines.
type collector struct {
	mu      sync.Mutex
	results []webhooks.Result
}

func (c *collector) observe(result webhooks.Result) {
	c.mu.Lock()
	c.results = append(c.results, result)
	c.mu.Unlock()
}

func (c *collector) urls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.results))
	for _, result := range c.results {
		out = append(out, result.URL)
	}
	sort.Strings(out)
	return out
}

func drain(t *testing.T, notifier *webhooks.Notifier) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := notifier.Wait(ctx); err != nil {
		t.Fatalf("deliveries did not settle: %v", err)
	}
}

func TestNotifier_FansOneTransitionOutToEverySubscriberThatWantsIt(t *testing.T) {
	succeededOnly := newReceiver(t, http.StatusOK)
	failedOnly := newReceiver(t, http.StatusOK)
	everything := newReceiver(t, http.StatusOK)

	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{
		URL: succeededOnly.server.URL, Events: []webhooks.Event{webhooks.EventRunSucceeded},
	})
	mustRegister(t, registry, webhooks.Registration{
		URL: failedOnly.server.URL, Events: []webhooks.Event{webhooks.EventRunFailed},
	})
	mustRegister(t, registry, webhooks.Registration{URL: everything.server.URL})

	results := &collector{}
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{})
	notifier.Observe(results.observe)

	if queued := notifier.Notify(context.Background(), sampleDelivery()); queued != 2 {
		t.Errorf("queued %d deliveries, want 2 (the RUN_SUCCEEDED subscriber and the catch-all)", queued)
	}
	drain(t, notifier)

	want := []string{everything.server.URL, succeededOnly.server.URL}
	sort.Strings(want)
	if strings.Join(results.urls(), ",") != strings.Join(want, ",") {
		t.Errorf("delivered to %v, want %v", results.urls(), want)
	}
	if failedOnly.count() != 0 {
		t.Errorf("the RUN_FAILED subscriber received %d deliveries for a RUN_SUCCEEDED transition",
			failedOnly.count())
	}
	if succeededOnly.count() != 1 || everything.count() != 1 {
		t.Errorf("delivery counts = %d and %d, want one each",
			succeededOnly.count(), everything.count())
	}
}

func TestNotifier_GivesEachSubscriberItsOwnDeliveryID(t *testing.T) {
	first := newReceiver(t, http.StatusOK)
	second := newReceiver(t, http.StatusOK)
	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{URL: first.server.URL})
	mustRegister(t, registry, webhooks.Registration{URL: second.server.URL})

	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{})
	notifier.Notify(context.Background(), sampleDelivery())
	drain(t, notifier)

	_, firstHeaders := first.request(0)
	_, secondHeaders := second.request(0)
	firstID := firstHeaders.Get(webhooks.DeliveryHeader)
	secondID := secondHeaders.Get(webhooks.DeliveryHeader)
	if firstID == "" || secondID == "" {
		t.Fatalf("delivery ids = %q and %q, want both set", firstID, secondID)
	}
	if firstID == secondID {
		t.Errorf("both subscribers received delivery id %q; ids must address one delivery", firstID)
	}
}

func TestNotifier_DeliversNothingWhenNobodySubscribed(t *testing.T) {
	notifier := webhooks.NewNotifier(webhooks.NewRegistry(), nil, webhooks.NotifierOptions{})
	if queued := notifier.Notify(context.Background(), sampleDelivery()); queued != 0 {
		t.Errorf("queued %d deliveries with an empty registry", queued)
	}
	drain(t, notifier)
}

func TestNotifier_OutlivesTheContextThatObservedTheTransition(t *testing.T) {
	// The engine's projection activity returns as soon as it has recorded the
	// transition, canceling its context. A delivery in flight must survive.
	release := make(chan struct{})
	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		received <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{URL: server.URL})
	results := &collector{}
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{})
	notifier.Observe(results.observe)

	ctx, cancel := context.WithCancel(context.Background())
	notifier.Notify(ctx, sampleDelivery())
	cancel()
	close(release)

	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("the delivery was dropped when the observing context ended")
	}
	drain(t, notifier)
	results.mu.Lock()
	defer results.mu.Unlock()
	if len(results.results) != 1 || !results.results[0].Delivered {
		t.Errorf("results = %+v, want one settled delivery", results.results)
	}
}

func TestNotifier_WaitReturnsWhenTheCallerGivesUp(t *testing.T) {
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-block
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(block)

	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{URL: server.URL})
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{})
	notifier.Notify(context.Background(), sampleDelivery())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := notifier.Wait(ctx); err == nil {
		t.Error("Wait returned success while a delivery was still hanging")
	}
}

func TestNotifier_StampsADeliveryThatArrivedWithoutATimestamp(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{URL: rec.server.URL})
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{})

	delivery := sampleDelivery()
	delivery.Timestamp = time.Time{}
	notifier.Notify(context.Background(), delivery)
	drain(t, notifier)

	_, headers := rec.request(0)
	stamped, err := time.Parse(time.RFC3339Nano, headers.Get(webhooks.TimestampHeader))
	if err != nil {
		t.Fatalf("timestamp header %q: %v", headers.Get(webhooks.TimestampHeader), err)
	}
	if stamped.IsZero() {
		t.Error("delivery went out with a zero timestamp")
	}
}

// syncBuffer is a bytes.Buffer safe to write from the notifier's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A stored subscription always has a secret, so a matched hook without one
// means the read path lost it — the symptom of the durable registry reading
// the public List. The notifier says so, and still delivers: refusing would
// turn a diagnostic into a silent drop.
func TestNotifier_WarnsWhenAMatchedSubscriptionHasNoSecret(t *testing.T) {
	target := newReceiver(t, http.StatusOK)
	// The in-memory Registry always generates a secret, so the secretless
	// hook is served by a durable store, which is also where this can happen.
	store := &countingStore{hooks: []webhooks.Webhook{{ID: "whk-nosecret", URL: target.server.URL}}}
	logs := &syncBuffer{}
	notifier := webhooks.NewNotifier(webhooks.NewDurableRegistry(store), webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{Logger: slog.New(slog.NewTextHandler(logs, nil))})

	notifier.Notify(context.Background(), webhooks.Delivery{
		Event: webhooks.EventRunSucceeded, RunID: "ork-1-1", OrkflowName: "etl", Status: "SUCCEEDED",
		Run: []byte(`{}`),
	})
	drain(t, notifier)

	out := logs.String()
	for _, want := range []string{"webhook delivery unsigned", "webhook_id=whk-nosecret", "event=RUN_SUCCEEDED", "run_id=ork-1-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("log does not contain %q:\n%s", want, out)
		}
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if len(target.headers) != 1 {
		t.Fatalf("receiver got %d POSTs, want 1: an unsigned hook is still delivered", len(target.headers))
	}
	if sig := target.headers[0].Get(webhooks.SignatureHeader); sig != "" {
		t.Errorf("a secretless delivery carried %s: %q", webhooks.SignatureHeader, sig)
	}
}
