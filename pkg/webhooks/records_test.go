package webhooks_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The tests in this file prove what a delivery RECORD needs from the
// dispatcher: a sortable id, the coordinates and digest of what was sent, and
// timestamps on every return path. They are consumers of the delivery path,
// never a change to it.

func TestNewDeliveryID_ShapeAndOrder(t *testing.T) {
	at := time.Date(2026, 9, 21, 2, 14, 6, 891204113, time.UTC)
	first, err := webhooks.NewDeliveryID(at)
	if err != nil {
		t.Fatalf("NewDeliveryID: %v", err)
	}
	if !webhooks.ValidDeliveryID(first) {
		t.Fatalf("id %q is not accepted by ValidDeliveryID", first)
	}
	if want := "dlv-20260921T021406.891204113Z-"; !strings.HasPrefix(first, want) {
		t.Errorf("id %q does not carry the queued instant; want prefix %q", first, want)
	}
	second, err := webhooks.NewDeliveryID(at.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("NewDeliveryID: %v", err)
	}
	if !(first < second) {
		t.Errorf("ids minted one nanosecond apart do not sort in time order: %q then %q", first, second)
	}
	// A non-UTC instant renders in UTC.
	local, _ := webhooks.NewDeliveryID(at.In(time.FixedZone("x", 3600)))
	if !strings.HasPrefix(local, "dlv-20260921T021406.891204113Z-") {
		t.Errorf("a non-UTC instant rendered %q, want the UTC stamp", local)
	}
	for _, bad := range []string{
		"dlv-0123456789abcdef01234567",                      // the old shape
		"dlv-20260921T021406.89120411Z-9c22a1f04e7b3d58",    // stamp one digit short
		"dlv-20260921T021406.891204113Z-9c22a1f04e7b3d5",    // hex one short
		"dlv-20260921T021406.891204113Z-9C22A1F04E7B3D58",   // upper-case hex
		"aud-20260921T021406.891204113Z-9c22a1f04e7b3d58",   // another family
		"dlv-20260921T021406.891204113Z-9c22a1f04e7b3d58/x", // a path
		"",
	} {
		if webhooks.ValidDeliveryID(bad) {
			t.Errorf("ValidDeliveryID(%q) = true, want false", bad)
		}
	}
}

func TestNotifier_MintsEveryDeliveryOfOneEventFromOneInstant(t *testing.T) {
	registry := webhooks.NewRegistry()
	for range 3 {
		mustRegister(t, registry, webhooks.Registration{URL: newReceiver(t, http.StatusOK).server.URL})
	}
	var mu sync.Mutex
	var instants []time.Time
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{NewDeliveryID: func(queuedAt time.Time) (string, error) {
			mu.Lock()
			instants = append(instants, queuedAt)
			mu.Unlock()
			return webhooks.NewDeliveryID(queuedAt)
		}})
	results := &collector{}
	notifier.Observe(results.observe)

	if queued := notifier.Notify(context.Background(), sampleDelivery()); queued != 3 {
		t.Fatalf("queued %d, want 3", queued)
	}
	drain(t, notifier)

	if len(instants) != 3 {
		t.Fatalf("generator called %d times, want 3", len(instants))
	}
	results.mu.Lock()
	defer results.mu.Unlock()
	if len(results.results) != 3 {
		t.Fatalf("observed %d results, want 3", len(results.results))
	}
	for _, result := range results.results {
		if !result.QueuedAt.Equal(instants[0]) {
			t.Errorf("delivery %s queued at %s, want the one instant %s", result.DeliveryID, result.QueuedAt, instants[0])
		}
		// The test, and only the test, reads the stamp back out of the id.
		stamp := strings.TrimPrefix(result.DeliveryID, "dlv-")[:len(webhooks.DeliveryIDLayout)]
		parsed, err := time.Parse(webhooks.DeliveryIDLayout, stamp)
		if err != nil {
			t.Fatalf("id %q stamp: %v", result.DeliveryID, err)
		}
		if !parsed.Equal(result.QueuedAt) {
			t.Errorf("id %q carries %s, but the delivery was queued at %s", result.DeliveryID, parsed, result.QueuedAt)
		}
	}
}

func TestDispatcher_ResultCarriesCoordinatesDigestAndTimestamps(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	delivery := sampleDelivery()
	delivery.Namespace = "payments"
	delivery.QueuedAt = time.Date(2025, 8, 16, 0, 0, 1, 5, time.UTC)

	before := time.Now().UTC()
	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{}).
		Deliver(context.Background(), webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s"}, delivery)
	after := time.Now().UTC()
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	body, _ := rec.request(0)
	sum := sha256.Sum256(body)
	for name, pair := range map[string][2]string{
		"Namespace":   {result.Namespace, "payments"},
		"OrkflowName": {result.OrkflowName, "hello-ork"},
		"Status":      {result.Status, "SUCCEEDED"},
		"BodySHA256":  {result.BodySHA256, hex.EncodeToString(sum[:])},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s = %q, want %q", name, pair[0], pair[1])
		}
	}
	if result.BodyBytes != len(body) {
		t.Errorf("BodyBytes = %d, want the %d bytes the receiver read", result.BodyBytes, len(body))
	}
	if !result.EventAt.Equal(delivery.Timestamp) {
		t.Errorf("EventAt = %s, want the transition's %s", result.EventAt, delivery.Timestamp)
	}
	if !result.QueuedAt.Equal(delivery.QueuedAt) {
		t.Errorf("QueuedAt = %s, want %s", result.QueuedAt, delivery.QueuedAt)
	}
	if result.SettledAt.IsZero() || result.SettledAt.Before(result.Attempts[0].At) {
		t.Errorf("SettledAt = %s, want set and not before the first attempt %s", result.SettledAt, result.Attempts[0].At)
	}
	if at := result.Attempts[0].At; at.Before(before) || at.After(after) {
		t.Errorf("Attempts[0].At = %s, want between %s and %s", at, before, after)
	}
	// The record's JSON tags keep the existing keys and add the new ones.
	encoded, _ := stablejson.Marshal(result)
	for _, key := range []string{`"deliveryId"`, `"bodySha256"`, `"settledAt"`, `"queuedAt"`, `"eventAt"`, `"at"`} {
		if !strings.Contains(string(encoded), key) {
			t.Errorf("encoded result has no %s key: %s", key, encoded)
		}
	}
}

func TestDispatcher_SettledAtIsStampedOnEveryReturnPath(t *testing.T) {
	unencodable := sampleDelivery()
	unencodable.Run = stablejson.RawJSON(`{not json`)
	cases := map[string]struct {
		statuses []int
		delivery webhooks.Delivery
	}{
		"success":      {statuses: []int{http.StatusOK}, delivery: sampleDelivery()},
		"5xx":          {statuses: []int{http.StatusBadGateway}, delivery: sampleDelivery()},
		"4xx stop":     {statuses: []int{http.StatusBadRequest}, delivery: sampleDelivery()},
		"body failure": {statuses: []int{http.StatusOK}, delivery: unencodable},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := newReceiver(t, tc.statuses...)
			result, _ := webhooks.NewDispatcher(webhooks.DispatcherOptions{}).
				Deliver(context.Background(), webhooks.Webhook{ID: "wh-1", URL: rec.server.URL}, tc.delivery)
			if result.SettledAt.IsZero() {
				t.Errorf("SettledAt is zero on the %s path", name)
			}
			for _, attempt := range result.Attempts {
				if attempt.At.IsZero() {
					t.Errorf("attempt %d has no At on the %s path", attempt.Number, name)
				}
			}
		})
	}
}

func TestDispatcher_SetsTheRedeliveryHeaderOnlyForRedeliveries(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{})
	hook := webhooks.Webhook{ID: "wh-1", URL: rec.server.URL}

	plain := sampleDelivery()
	if _, err := dispatcher.Deliver(context.Background(), hook, plain); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	again := sampleDelivery()
	again.ID = "dlv-20250816T000002.000000000Z-00112233aabbccdd"
	again.RedeliveryOf = "dlv-20250816T000001.000000000Z-8899aabbccddeeff"
	result, err := dispatcher.Deliver(context.Background(), hook, again)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.RedeliveryOf != again.RedeliveryOf {
		t.Errorf("result RedeliveryOf = %q, want %q", result.RedeliveryOf, again.RedeliveryOf)
	}

	_, plainHeaders := rec.request(0)
	if got, present := plainHeaders[http.CanonicalHeaderKey(webhooks.RedeliveryHeader)]; present {
		t.Errorf("an engine-made delivery carried %s = %v", webhooks.RedeliveryHeader, got)
	}
	_, headers := rec.request(1)
	for header, want := range map[string]string{
		webhooks.RedeliveryHeader: again.RedeliveryOf,
		webhooks.DeliveryHeader:   again.ID,
		webhooks.AttemptHeader:    "1",
		webhooks.TimestampHeader:  again.Timestamp.UTC().Format(time.RFC3339Nano),
	} {
		if got := headers.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
}

func TestRegistry_LookupForDeliveryFindsOneSubscriptionWithItsSecret(t *testing.T) {
	ctx := context.Background()
	t.Run("durable", func(t *testing.T) {
		store := &countingStore{hooks: []webhooks.Webhook{
			{ID: "wh-a", URL: "https://a.example/hook", Secret: "secret-a"},
			{ID: "wh-b", URL: "https://b.example/hook", Secret: "secret-b"},
		}}
		registry := webhooks.NewDurableRegistry(store)
		hook, ok, err := registry.LookupForDelivery(ctx, "wh-b")
		if err != nil || !ok {
			t.Fatalf("LookupForDelivery(wh-b) = %v, %v", ok, err)
		}
		if hook.Secret != "secret-b" || hook.URL != "https://b.example/hook" {
			t.Errorf("hook = %+v, want wh-b with its secret", hook)
		}
		lists, deliveryReads := store.calls()
		if lists != 0 || deliveryReads != 1 || store.getCalls() != 0 {
			t.Errorf("lookup made %d List, %d ListForDelivery and %d Get calls, want 0, 1 and 0",
				lists, deliveryReads, store.getCalls())
		}
		if hook, ok, err := registry.LookupForDelivery(ctx, "wh-gone"); err != nil || ok || hook.ID != "" {
			t.Errorf("unknown id = (%+v, %v, %v), want (zero, false, nil)", hook, ok, err)
		}
	})
	t.Run("durable store error", func(t *testing.T) {
		boom := errors.New("bucket unreachable")
		registry := webhooks.NewDurableRegistry(&countingStore{deliveryErr: boom})
		_, ok, err := registry.LookupForDelivery(ctx, "wh-a")
		if ok || !errors.Is(err, boom) || !strings.Contains(err.Error(), "wh-a") {
			t.Errorf("store error = (%v, %v), want a wrapped error naming wh-a", ok, err)
		}
	})
	t.Run("in memory", func(t *testing.T) {
		registry := webhooks.NewRegistry()
		registered := mustRegister(t, registry, webhooks.Registration{URL: "https://a.example/hook", Secret: "mine"})
		hook, ok, err := registry.LookupForDelivery(ctx, registered.ID)
		if err != nil || !ok || hook.Secret != "mine" {
			t.Errorf("LookupForDelivery = (%+v, %v, %v), want the hook with its secret", hook, ok, err)
		}
		if hook, ok, err := registry.LookupForDelivery(ctx, "wh-gone"); err != nil || ok || hook.ID != "" {
			t.Errorf("unknown id = (%+v, %v, %v), want (zero, false, nil)", hook, ok, err)
		}
	})
}

// Guardrail clause 1: the record is of a SETTLED delivery. Without a
// launcher the notifier makes its one attempt in-process; the record is
// written first and the observer then sees the final result once.
func TestObserve_IsCalledAfterTheRecordWithTheSettledResult(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{URL: rec.server.URL})
	var mu sync.Mutex
	var order []string
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}), webhooks.NotifierOptions{
		Record: func(_ context.Context, result webhooks.Result) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, "record "+result.DeliveryID)
			return nil
		},
	})
	var seen []webhooks.Result
	notifier.Observe(func(result webhooks.Result) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, result)
		order = append(order, "observe "+result.DeliveryID)
	})
	notifier.Notify(context.Background(), sampleDelivery())
	drain(t, notifier)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("observer called %d times, want once", len(seen))
	}
	result := seen[0]
	if !result.Delivered || len(result.Attempts) != 1 || rec.count() != 1 {
		t.Errorf("observed %+v after %d requests, want delivered in one attempt", result, rec.count())
	}
	if result.SettledAt.IsZero() || result.SettledAt.Before(result.Attempts[0].At) {
		t.Errorf("SettledAt %s is not after the attempt %s", result.SettledAt, result.Attempts[0].At)
	}
	if len(order) != 2 || !strings.HasPrefix(order[0], "record ") || !strings.HasPrefix(order[1], "observe ") {
		t.Errorf("order = %q, want the record written before the observer runs", order)
	}
}

// A record that cannot be written is Settle's error, and no observer sees the
// delivery: the engine's settle activity retries until the record exists.
func TestNotifier_SettleReturnsTheRecordErrorAndObservesNothing(t *testing.T) {
	refused := errors.New("store is down")
	notifier := webhooks.NewNotifier(webhooks.NewRegistry(), nil, webhooks.NotifierOptions{
		Record: func(context.Context, webhooks.Result) error { return refused },
	})
	observed := 0
	notifier.Observe(func(webhooks.Result) { observed++ })
	if err := notifier.Settle(context.Background(), webhooks.Result{DeliveryID: "dlv-1"}); !errors.Is(err, refused) {
		t.Fatalf("Settle = %v, want the record's error", err)
	}
	if observed != 0 {
		t.Errorf("an observer saw a delivery whose record was not written")
	}
}

// fakeLauncher records what it was handed, or refuses.
type fakeLauncher struct {
	mu       sync.Mutex
	launched []webhooks.Delivery
	refuse   error
}

func (l *fakeLauncher) Launch(_ context.Context, _ webhooks.Webhook, delivery webhooks.Delivery) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.refuse != nil {
		return l.refuse
	}
	l.launched = append(l.launched, delivery)
	return nil
}

// With a launcher the notifier sends nothing itself: each delivery is handed
// off, whole, to be sent and retried durably.
func TestNotifier_HandsEachDeliveryToTheLauncherAndSendsNothing(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{URL: rec.server.URL})
	mustRegister(t, registry, webhooks.Registration{URL: rec.server.URL})
	launcher := &fakeLauncher{}
	notifier := webhooks.NewNotifier(registry, nil, webhooks.NotifierOptions{Launcher: launcher})
	notifier.Notify(context.Background(), sampleDelivery())
	drain(t, notifier)

	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	if len(launcher.launched) != 2 || rec.count() != 0 {
		t.Fatalf("launched %d and sent %d, want 2 handed off and none sent", len(launcher.launched), rec.count())
	}
	for _, delivery := range launcher.launched {
		if !webhooks.ValidDeliveryID(delivery.ID) || delivery.QueuedAt.IsZero() || len(delivery.Run) == 0 {
			t.Errorf("handed off %+v, want an id, a queued instant and the run", delivery)
		}
	}
}

// A delivery the launcher could not queue is never sent, and its record says
// so: one attempt row, no status, naming why.
func TestNotifier_RecordsADeliveryThatCouldNotBeQueued(t *testing.T) {
	registry := webhooks.NewRegistry()
	mustRegister(t, registry, webhooks.Registration{URL: "https://example.test/hook"})
	var recorded []webhooks.Result
	var mu sync.Mutex
	notifier := webhooks.NewNotifier(registry, nil, webhooks.NotifierOptions{
		Launcher: &fakeLauncher{refuse: errors.New("cluster unreachable")},
		Record: func(_ context.Context, result webhooks.Result) error {
			mu.Lock()
			defer mu.Unlock()
			recorded = append(recorded, result)
			return nil
		},
	})
	notifier.Notify(context.Background(), sampleDelivery())
	drain(t, notifier)

	mu.Lock()
	defer mu.Unlock()
	if len(recorded) != 1 {
		t.Fatalf("recorded %d results, want 1", len(recorded))
	}
	result := recorded[0]
	if result.Delivered || len(result.Attempts) != 1 || result.Attempts[0].StatusCode != 0 ||
		!strings.Contains(result.Attempts[0].Error, "could not be queued") || !strings.Contains(result.Attempts[0].Error, "cluster unreachable") ||
		result.SettledAt.IsZero() || result.BodySHA256 == "" {
		t.Errorf("recorded %+v, want an undelivered record naming why it was not queued", result)
	}
}
