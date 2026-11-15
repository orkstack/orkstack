package engine_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/testsuite"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// urlCanary is in the path and the query of every URL these tests subscribe:
// a webhook URL often carries a credential, so nothing a delivery puts in the
// execution cluster may hold any of it (P4d).
const urlCanary = "canary-u7l"

// payloadCapture keeps every value a delivery hands the cluster, encoded as
// the cluster would hold it: each activity's arguments, each heartbeat, each
// result, and each failure's message.
type payloadCapture struct {
	mu       sync.Mutex
	payloads []string
}

func (c *payloadCapture) add(kind string, raw []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.payloads = append(c.payloads, kind+": "+string(raw))
}

func (c *payloadCapture) values(kind string, values converter.EncodedValues, n int) {
	if values == nil || !values.HasValues() {
		return
	}
	decoded := make([]any, n)
	for i := range decoded {
		decoded[i] = new(any)
	}
	if err := values.Get(decoded...); err != nil {
		c.add(kind+" (undecodable)", []byte(err.Error()))
		return
	}
	for _, value := range decoded {
		c.addValue(kind, *value.(*any))
	}
}

// addValue keeps value re-encoded: every string the payload held, as it
// held it.
func (c *payloadCapture) addValue(kind string, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		raw = []byte("unencodable: " + err.Error())
	}
	c.add(kind, raw)
}

func capturePayloads(h *deliveryHarness) *payloadCapture {
	c := &payloadCapture{}
	h.env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, args converter.EncodedValues) {
		n := 1
		if info.ActivityType.Name == engine.ActivitySettleWebhookDelivery {
			n = 2
		}
		c.values("args of "+info.ActivityType.Name, args, n)
	})
	h.env.SetOnActivityHeartbeatListener(func(info *activity.Info, details converter.EncodedValues) {
		c.values("heartbeat of "+info.ActivityType.Name, details, 1)
	})
	h.env.SetOnActivityCompletedListener(func(info *activity.Info, result converter.EncodedValue, err error) {
		if err != nil {
			c.add("failure of "+info.ActivityType.Name, []byte(err.Error()))
		}
		if result != nil && result.HasValue() {
			var value any
			if decodeErr := result.Get(&value); decodeErr != nil {
				c.add("result of "+info.ActivityType.Name+" (undecodable)", []byte(decodeErr.Error()))
			} else {
				c.addValue("result of "+info.ActivityType.Name, value)
			}
		}
	})
	return c
}

func (c *payloadCapture) assertNoURL(t *testing.T, transportWords ...string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.payloads) == 0 {
		t.Fatal("no payload was captured, so nothing was checked")
	}
	for _, payload := range c.payloads {
		if strings.Contains(payload, urlCanary) || strings.Contains(payload, "127.0.0.1") {
			t.Errorf("a payload holds the webhook's URL: %s", payload)
		}
		for _, word := range transportWords {
			if strings.Contains(payload, word) {
				t.Errorf("a payload holds the transport's word %q: %s", word, payload)
			}
		}
	}
}

// Every value a delivery hands the cluster — its input, both activities'
// arguments, each heartbeat, each result and each failure — holds neither
// the webhook's URL nor the transport's words, for a delivery that lands
// after retries and for one whose receiver cannot be reached at all. The
// record, which lives in object storage, still names the URL.
func TestWebhookDelivery_KeepsTheURLOutOfEveryPayload(t *testing.T) {
	t.Run("lands after retries", func(t *testing.T) {
		h := newDeliveryHarness(t)
		capture := capturePayloads(h)
		receiver := newDeliveryReceiver(t, http.StatusBadGateway, http.StatusNoContent)
		url := receiver.server.URL + "/" + urlCanary + "?token=" + urlCanary
		in, _ := h.queue(t, url)
		encoded, err := stablejson.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		capture.add("workflow input", encoded)
		result := h.run(t, in)
		if !result.Delivered || result.URL != url || result.Signature == "" {
			t.Fatalf("record = %+v, want delivered, naming the subscription's URL and the signature sent", result)
		}
		// The signature, with the stored body, would let a holder of history
		// guess the secret: only the record holds it.
		capture.assertNoURL(t, result.Signature)
	})
	t.Run("never reaches the receiver", func(t *testing.T) {
		h := newDeliveryHarness(t)
		capture := capturePayloads(h)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		_ = l.Close()
		url := "http://" + addr + "/" + urlCanary + "?token=" + urlCanary
		in, _ := h.queue(t, url)
		result := h.run(t, in)
		if result.Delivered || len(result.Attempts) != 4 {
			t.Fatalf("record = %+v, want 4 undelivered attempts", result)
		}
		for _, attempt := range result.Attempts {
			if attempt.Code != webhooks.AttemptCodeConnect {
				t.Errorf("attempt %d = %+v, want code %s", attempt.Number, attempt, webhooks.AttemptCodeConnect)
			}
		}
		capture.assertNoURL(t, "dial", "connection refused", "Post")
	})
}

// A delivery queued by the release before, whose input still carries the
// URL, completes: the attempt reads the subscription from the registry.
func TestWebhookDelivery_ADeliveryQueuedByTheReleaseBeforeStillCompletes(t *testing.T) {
	h := newDeliveryHarness(t)
	receiver := newDeliveryReceiver(t, http.StatusOK)
	in, _ := h.queue(t, receiver.server.URL)
	in.URL = receiver.server.URL
	result := h.run(t, in)
	if bodies, _ := receiver.requests(); !result.Delivered || len(bodies) != 1 || result.URL != receiver.server.URL {
		t.Fatalf("record = %+v after %d requests, want one delivered request to the subscription", result, len(bodies))
	}
}

// Each attempt sends to the subscription's URL as the registry holds it when
// the attempt runs, as it reads the signing secret.
func TestWebhookDelivery_EachAttemptReadsTheURLFromTheRegistry(t *testing.T) {
	h := newDeliveryHarness(t)
	before := newDeliveryReceiver(t, http.StatusOK)
	after := newDeliveryReceiver(t, http.StatusOK)
	in, _ := h.queue(t, before.server.URL)
	moved := after.server.URL
	if _, found, err := h.registry.UpdateContext(context.Background(), in.WebhookID, func(w *webhooks.Webhook) error {
		w.URL = moved
		return nil
	}); err != nil || !found {
		t.Fatalf("moving the subscription: found %v, %v", found, err)
	}
	result := h.run(t, in)
	beforeBodies, _ := before.requests()
	afterBodies, _ := after.requests()
	if !result.Delivered || len(beforeBodies) != 0 || len(afterBodies) != 1 || result.URL != moved {
		t.Fatalf("record = %+v; %d requests to the old URL, %d to the new; want one to the new", result, len(beforeBodies), len(afterBodies))
	}
}

// launcherInput starts one delivery through a launcher wired to checker and
// returns the workflow's input.
func launcherInput(t *testing.T, checker engine.CapabilityChecker) engine.WebhookDeliveryInput {
	t.Helper()
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := &mocks.Client{}
	var input engine.WebhookDeliveryInput
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, engine.WebhookDeliveryWorkflowType, mock.Anything).
		Run(func(args mock.Arguments) { input = args.Get(3).(engine.WebhookDeliveryInput) }).
		Return(&mocks.WorkflowRun{}, nil)
	launcher := engine.NewWebhookLauncher(c, "ork-tasks", bodies, engine.DefaultWebhookDeliveryPolicy())
	launcher.Capabilities = checker
	delivery := webhooks.Delivery{ID: "dlv-20261108T120000.000000000Z-0011223344556677", RunID: "ork-1", Run: []byte(`{}`)}
	hook := webhooks.Webhook{ID: "wh-0123456789abcdef", URL: "https://example.test/" + urlCanary}
	if err := launcher.Launch(context.Background(), hook, delivery); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	return input
}

// fleet is a capability checker answering one fixed verdict.
type fleet struct {
	mu      sync.Mutex
	lacking []string
	err     error
	asked   []engine.CapabilityScope
}

func (f *fleet) MissingCapability(_ context.Context, capability string, scope engine.CapabilityScope) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if capability != engine.CapabilityWebhookByID {
		return nil, errors.New("asked for " + capability)
	}
	f.asked = append(f.asked, scope)
	return f.lacking, f.err
}

// The launcher leaves the URL out of the delivery's input unless an engine
// process that could run its attempts is of a release that reads the URL
// only from there; such a delivery carries it, so it still lands.
func TestWebhookLauncher_LeavesTheURLOutUnlessAnOlderEngineCouldSendIt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		checker engine.CapabilityChecker
		wantURL bool
	}{
		{"every engine reads the registry", &fleet{}, false},
		{"an older engine polls", &fleet{lacking: []string{"1@old@"}}, true},
		{"the fleet cannot be read", &fleet{err: errors.New("describe failed")}, false},
		{"no fleet check is wired", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := launcherInput(t, tc.checker)
			if got := in.URL != ""; got != tc.wantURL {
				t.Errorf("input carries the URL = %v, want %v", got, tc.wantURL)
			}
			if f, ok := tc.checker.(*fleet); ok {
				if len(f.asked) != 1 || f.asked[0] != engine.ScopeWorkflowAndEvents {
					t.Errorf("asked the fleet %v, want once, for the workflow and events channels", f.asked)
				}
			}
		})
	}
}

// A failed fleet check, or one still in flight, leaves the last good answer
// deciding, holds up no other Launch, and is asked again soon after.
func TestWebhookLauncher_KeepsTheLastAnswerWhileTheFleetCannotBeRead(t *testing.T) {
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := &mocks.Client{}
	var (
		mu     sync.Mutex
		inputs []engine.WebhookDeliveryInput
	)
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, engine.WebhookDeliveryWorkflowType, mock.Anything).
		Run(func(args mock.Arguments) {
			mu.Lock()
			defer mu.Unlock()
			inputs = append(inputs, args.Get(3).(engine.WebhookDeliveryInput))
		}).Return(&mocks.WorkflowRun{}, nil)
	launcher := engine.NewWebhookLauncher(c, "ork-tasks", bodies, engine.DefaultWebhookDeliveryPolicy())
	check := &fleet{lacking: []string{"1@old@"}}
	launcher.Capabilities = check
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	engine.SetWebhookLauncherClockForTest(launcher, func() time.Time { return now })
	n := 0
	launch := func() bool {
		t.Helper()
		n++
		id := fmt.Sprintf("dlv-20261114T120000.000000000Z-%016x", n)
		delivery := webhooks.Delivery{ID: id, RunID: "ork-1", Run: []byte(`{}`)}
		if err := launcher.Launch(context.Background(), webhooks.Webhook{ID: "wh-0123456789abcdef", URL: "https://example.test/x"}, delivery); err != nil {
			t.Fatalf("Launch: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		return inputs[len(inputs)-1].URL != ""
	}
	if !launch() {
		t.Fatal("an older engine polls, yet the delivery left its URL out")
	}
	now = now.Add(time.Minute)
	check.mu.Lock()
	check.lacking, check.err = nil, errors.New("describe failed")
	check.mu.Unlock()
	if !launch() {
		t.Error("a failed check dropped the last good answer")
	}
	if !launch() || len(check.asked) != 2 {
		t.Errorf("asked %d times, want a failed check not asked again at once", len(check.asked))
	}
	now = now.Add(10 * time.Second)
	check.mu.Lock()
	check.err = nil
	check.mu.Unlock()
	if launch() || len(check.asked) != 3 {
		t.Errorf("after the fleet is readable again: carried the URL, asked %d times; want a fresh answer", len(check.asked))
	}
}

// slowFleet answers its first check at once and every later one only once
// released, so a refresh stays in flight.
type slowFleet struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
}

func (f *slowFleet) MissingCapability(ctx context.Context, _ string, _ engine.CapabilityScope) ([]string, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.mu.Unlock()
	if call == 1 {
		return []string{"1@old@"}, nil
	}
	if call == 2 {
		close(f.started)
	}
	<-f.release
	return []string{"1@old@"}, nil
}

// Once the fleet has answered, a slow refresh holds up no other Launch, and
// the last answer decides it.
func TestWebhookLauncher_ASlowFleetCheckHoldsUpNoOtherLaunch(t *testing.T) {
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := &mocks.Client{}
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, engine.WebhookDeliveryWorkflowType, mock.Anything).
		Return(&mocks.WorkflowRun{}, nil)
	launcher := engine.NewWebhookLauncher(c, "ork-tasks", bodies, engine.DefaultWebhookDeliveryPolicy())
	slow := &slowFleet{started: make(chan struct{}), release: make(chan struct{})}
	launcher.Capabilities = slow
	var clockMu sync.Mutex
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	engine.SetWebhookLauncherClockForTest(launcher, func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	})
	hook := webhooks.Webhook{ID: "wh-0123456789abcdef", URL: "https://example.test/x"}
	if err := launcher.Launch(context.Background(), hook, webhooks.Delivery{ID: "dlv-20261114T120000.000000000Z-0000000000000000", RunID: "ork-1", Run: []byte(`{}`)}); err != nil {
		t.Fatalf("the first Launch: %v", err)
	}
	clockMu.Lock()
	now = now.Add(time.Minute)
	clockMu.Unlock()
	first := make(chan error, 1)
	go func() {
		first <- launcher.Launch(context.Background(), hook, webhooks.Delivery{ID: "dlv-20261114T120000.000000000Z-0000000000000001", RunID: "ork-1", Run: []byte(`{}`)})
	}()
	<-slow.started
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := launcher.Launch(ctx, hook, webhooks.Delivery{ID: "dlv-20261114T120000.000000000Z-0000000000000002", RunID: "ork-1", Run: []byte(`{}`)}); err != nil {
		t.Fatalf("a Launch during a slow check: %v", err)
	}
	close(slow.release)
	if err := <-first; err != nil {
		t.Fatalf("the Launch that made the check: %v", err)
	}
	if slow.calls != 2 {
		t.Errorf("the fleet was asked %d times, want once to answer and once to refresh", slow.calls)
	}
}

// downStore is a durable registry store whose every read fails.
type downStore struct{ webhooks.Store }

func (downStore) ListForDelivery(context.Context) ([]webhooks.Webhook, error) {
	return nil, errors.New("the store is down")
}

// settleOnce runs SettleWebhookDelivery once, as its first attempt (the
// test environment's), and returns its error and what it recorded.
func settleOnce(t *testing.T, registry *webhooks.Registry, bodies artifacts.DeliveryBodyStore, in engine.WebhookDeliveryInput,
	result webhooks.Result,
) ([]webhooks.Result, error) {
	t.Helper()
	var recorded []webhooks.Result
	notifier := webhooks.NewNotifier(registry, nil, webhooks.NotifierOptions{
		Record: func(_ context.Context, r webhooks.Result) error { recorded = append(recorded, r); return nil },
	})
	activities := engine.NewWebhookDeliveryActivities(bodies, registry, nil, notifier, nil)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.SetTestTimeout(10 * time.Second)
	env.RegisterActivityWithOptions(activities.SettleWebhookDelivery, activity.RegisterOptions{Name: engine.ActivitySettleWebhookDelivery})
	_, err := env.ExecuteActivity(engine.ActivitySettleWebhookDelivery, in, result)
	return recorded, err
}

// The record is not lost to a registry outage: settle retries the read, and
// its last attempt writes the record without the URL and the signature.
func TestWebhookSettle_ARegistryOutageStillWritesTheRecordOnTheLastAttempt(t *testing.T) {
	bodies, err := artifacts.NewLocalDeliveryBodyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	registry := webhooks.NewDurableRegistry(downStore{})
	in := engine.WebhookDeliveryInput{
		Delivery:  webhooks.Delivery{ID: "dlv-20261114T120000.000000000Z-0011223344556677", RunID: "ork-1"},
		WebhookID: "wh-0123456789abcdef", BodySHA256: strings.Repeat("a", 64), Policy: engine.DefaultWebhookDeliveryPolicy(),
	}
	result := webhooks.Result{DeliveryID: in.Delivery.ID, WebhookID: in.WebhookID, Delivered: true,
		Attempts: []webhooks.Attempt{{Number: 1, StatusCode: 204}}}
	if recorded, err := settleOnce(t, registry, bodies, in, result); err == nil || len(recorded) != 0 {
		t.Fatalf("an early settle = %v with %d records, want it retried", err, len(recorded))
	}
	in.Policy.SettleAttempts = 1 // so the first attempt is the last
	recorded, err := settleOnce(t, registry, bodies, in, result)
	if err != nil || len(recorded) != 1 || !recorded[0].Delivered || recorded[0].URL != "" || recorded[0].Signature != "" {
		t.Fatalf("last settle = %v, records %+v; want the record written, delivered, without URL or signature", err, recorded)
	}
}

// A delivery that made no request records no signature: nothing was signed
// and sent.
func TestWebhookSettle_NoRequestMeansNoSignature(t *testing.T) {
	receiver := newDeliveryReceiver(t, http.StatusOK)
	for _, tc := range []struct {
		name     string
		attempts []webhooks.Attempt
		signed   bool
	}{
		{"answered", []webhooks.Attempt{{Number: 1, StatusCode: 503}, {Number: 2, StatusCode: 204}}, true},
		{"refused before a response", []webhooks.Attempt{{Number: 1, Code: webhooks.AttemptCodeConnect, Error: "x"}}, true},
		{"never sent", []webhooks.Attempt{
			{Number: 1, Code: webhooks.AttemptCodeStoreUnavailable, Error: "x"},
			{Number: 2, Code: webhooks.AttemptCodeUnreported, Error: "x"},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDeliveryHarness(t)
			in, _ := h.queue(t, receiver.server.URL)
			result := webhooks.Result{DeliveryID: in.Delivery.ID, WebhookID: in.WebhookID, Attempts: tc.attempts}
			recorded, err := settleOnce(t, h.registry, h.bodies, in, result)
			if err != nil || len(recorded) != 1 {
				t.Fatalf("settle = %v with %d records", err, len(recorded))
			}
			if got := recorded[0].Signature != ""; got != tc.signed || recorded[0].URL != receiver.server.URL {
				t.Errorf("record = %+v; signed %v, want %v, naming the URL", recorded[0], got, tc.signed)
			}
		})
	}
}
