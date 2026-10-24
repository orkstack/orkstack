package webhooks_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

func TestSign_ProducesAnHMACTheReceiverCanVerify(t *testing.T) {
	body := []byte(`{"runId":"ork-1-1","status":"SUCCEEDED"}`)
	signature := webhooks.Sign("s3cr3t", body)

	if !strings.HasPrefix(signature, webhooks.SignaturePrefix) {
		t.Fatalf("signature %q does not name its algorithm", signature)
	}
	if !webhooks.Verify("s3cr3t", body, signature) {
		t.Errorf("a receiver holding the secret could not verify %q", signature)
	}
	// The bare hex, as a defensive receiver might have stripped it.
	if !webhooks.Verify("s3cr3t", body, strings.TrimPrefix(signature, webhooks.SignaturePrefix)) {
		t.Error("verification requires the algorithm prefix, but a receiver may strip it")
	}
	if again := webhooks.Sign("s3cr3t", body); again != signature {
		t.Errorf("signing is not deterministic: %q then %q", signature, again)
	}
}

func TestSign_RejectsATamperedBodyAWrongSecretAndNoSecret(t *testing.T) {
	body := []byte(`{"runId":"ork-1-1","status":"SUCCEEDED"}`)
	signature := webhooks.Sign("s3cr3t", body)

	if webhooks.Verify("s3cr3t", []byte(`{"runId":"ork-1-1","status":"FAILED"}`), signature) {
		t.Error("a rewritten body still verified: the signature does not cover it")
	}
	if webhooks.Verify("guessed", body, signature) {
		t.Error("the wrong secret verified")
	}
	if webhooks.Sign("", body) != "" {
		t.Error("an unsigned webhook must produce no signature rather than a forgeable one")
	}
	if webhooks.Verify("", body, signature) {
		t.Error("an empty secret verified a signature")
	}
	if webhooks.Verify("s3cr3t", body, "") {
		t.Error("an empty signature verified")
	}
}

func TestRegistry_RegisterAssignsAnIDAndSecretAndDisclosesTheSecretOnce(t *testing.T) {
	registry := webhooks.NewRegistry(
		webhooks.WithIDGenerator(func() (string, error) { return "wh-fixed", nil }),
		webhooks.WithSecretGenerator(func() (string, error) { return "generated-secret", nil }),
		webhooks.WithClock(func() time.Time { return time.Unix(1755302400, 0).UTC() }),
	)

	hook, err := registry.Register(webhooks.Registration{
		URL:    "https://example.test/hooks",
		Events: []webhooks.Event{webhooks.EventRunSucceeded},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if hook.ID != "wh-fixed" {
		t.Errorf("id = %q, want the generated one", hook.ID)
	}
	if hook.Secret != "generated-secret" {
		t.Errorf("secret = %q, want it disclosed at registration", hook.Secret)
	}
	if !hook.CreatedAt.Equal(time.Unix(1755302400, 0).UTC()) {
		t.Errorf("createdAt = %s, want the registry clock", hook.CreatedAt)
	}

	listed := registry.List()
	if len(listed) != 1 {
		t.Fatalf("list returned %d webhooks, want 1", len(listed))
	}
	if listed[0].Secret != "" {
		t.Errorf("list disclosed the secret %q; it is disclosed only at registration", listed[0].Secret)
	}
	if listed[0].ID != "wh-fixed" || listed[0].URL != "https://example.test/hooks" {
		t.Errorf("listed webhook = %+v, want the registered one", listed[0])
	}
	got, ok := registry.Get("wh-fixed")
	if !ok || got.Secret != "" {
		t.Errorf("get returned (%+v, %v), want the webhook without its secret", got, ok)
	}
	// The dispatcher's read is the one that still carries it.
	matching := registry.Matching(webhooks.Match{Event: webhooks.EventRunSucceeded})
	if len(matching) != 1 || matching[0].Secret != "generated-secret" {
		t.Errorf("Matching returned %+v, want the secret a delivery signs with", matching)
	}
}

func TestRegistry_RegisterKeepsACallerSuppliedSecret(t *testing.T) {
	registry := webhooks.NewRegistry()
	hook, err := registry.Register(webhooks.Registration{
		URL:    "https://example.test/hooks",
		Secret: "mine",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if hook.Secret != "mine" {
		t.Errorf("secret = %q, want the caller's", hook.Secret)
	}
}

func TestRegistry_RejectsAURLNoDeliveryCouldReach(t *testing.T) {
	registry := webhooks.NewRegistry()
	for _, target := range []string{"", "   ", "example.test/hooks", "ftp://example.test", "https://"} {
		if _, err := registry.Register(webhooks.Registration{URL: target}); err == nil {
			t.Errorf("Register(%q) succeeded, want a rejection", target)
		}
	}
	if got := registry.List(); len(got) != 0 {
		t.Errorf("rejected registrations were stored: %+v", got)
	}
}

func TestRegistry_RejectsAnUnknownEvent(t *testing.T) {
	registry := webhooks.NewRegistry()
	_, err := registry.Register(webhooks.Registration{
		URL:    "https://example.test/hooks",
		Events: []webhooks.Event{"RUN_EXPLODED"},
	})
	if err == nil {
		t.Fatal("Register accepted an unknown event")
	}
	if !strings.Contains(err.Error(), "RUN_SUCCEEDED") {
		t.Errorf("error %q does not name the valid events", err)
	}
}

func TestRegistry_DeleteRemovesASubscriptionAndReportsAnUnknownID(t *testing.T) {
	registry := webhooks.NewRegistry()
	first := mustRegister(t, registry, webhooks.Registration{URL: "https://example.test/one"})
	second := mustRegister(t, registry, webhooks.Registration{URL: "https://example.test/two"})

	if !registry.Delete(first.ID) {
		t.Fatalf("Delete(%q) reported the webhook was not registered", first.ID)
	}
	if registry.Delete(first.ID) {
		t.Error("deleting the same webhook twice reported success the second time")
	}
	if registry.Delete("wh-never") {
		t.Error("deleting an unknown id reported success")
	}
	listed := registry.List()
	if len(listed) != 1 || listed[0].ID != second.ID {
		t.Errorf("after delete, list = %+v, want only %q", listed, second.ID)
	}
	if got := registry.Matching(webhooks.Match{Event: webhooks.EventRunSucceeded}); len(got) != 1 {
		t.Errorf("a deleted webhook still matches: %+v", got)
	}
}

func TestRegistry_ListKeepsRegistrationOrder(t *testing.T) {
	registry := webhooks.NewRegistry()
	var want []string
	for _, target := range []string{"https://a.test", "https://b.test", "https://c.test", "https://d.test"} {
		want = append(want, mustRegister(t, registry, webhooks.Registration{URL: target}).ID)
	}
	var got []string
	for _, hook := range registry.List() {
		got = append(got, hook.ID)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("list order = %v, want registration order %v", got, want)
	}
}

func TestParseEvent_AcceptsShortProtoAndLowerCaseSpellings(t *testing.T) {
	for _, raw := range []string{
		"RUN_SUCCEEDED", "run_succeeded", "EVENT_TYPE_RUN_SUCCEEDED", " event_type_run_succeeded ",
	} {
		got, err := webhooks.ParseEvent(raw)
		if err != nil {
			t.Errorf("ParseEvent(%q): %v", raw, err)
			continue
		}
		if got != webhooks.EventRunSucceeded {
			t.Errorf("ParseEvent(%q) = %q, want RUN_SUCCEEDED", raw, got)
		}
	}
	if _, err := webhooks.ParseEvent("TASK_LOG"); err == nil {
		t.Error("ParseEvent accepted TASK_LOG; a log line is never subscribable")
	}
}

func TestRegistry_NormalizesEventsToLifecycleOrderWithoutDuplicates(t *testing.T) {
	registry := webhooks.NewRegistry()
	hook := mustRegister(t, registry, webhooks.Registration{
		URL: "https://example.test/hooks",
		Events: []webhooks.Event{
			"RUN_FAILED", "event_type_run_started", "RUN_FAILED", "RUN_SUCCEEDED",
		},
	})
	want := []webhooks.Event{
		webhooks.EventRunStarted, webhooks.EventRunSucceeded, webhooks.EventRunFailed,
	}
	if len(hook.Events) != len(want) {
		t.Fatalf("events = %v, want %v", hook.Events, want)
	}
	for i, event := range want {
		if hook.Events[i] != event {
			t.Errorf("events[%d] = %q, want %q", i, hook.Events[i], event)
		}
	}
}

func mustRegister(t *testing.T, registry *webhooks.Registry, reg webhooks.Registration) webhooks.Webhook {
	t.Helper()
	hook, err := registry.Register(reg)
	if err != nil {
		t.Fatalf("Register(%q): %v", reg.URL, err)
	}
	return hook
}

// countingStore is a durable webhooks.Store held in memory. Its List answers
// with Public copies, exactly as both production backends do, its
// ListForDelivery with the secrets, and it counts calls to each so a test can
// say which read a registry used.
type countingStore struct {
	mu                        sync.Mutex
	hooks                     []webhooks.Webhook
	lists, deliveryRead, gets int
	updates                   int
	// deliveryErr, when set, is what ListForDelivery answers with.
	deliveryErr error
}

func (s *countingStore) Register(context.Context, webhooks.Registration) (webhooks.Webhook, error) {
	panic("countingStore is read-only")
}

func (s *countingStore) List(context.Context) ([]webhooks.Webhook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	out := make([]webhooks.Webhook, 0, len(s.hooks))
	for _, hook := range s.hooks {
		out = append(out, hook.Public())
	}
	return out, nil
}

func (s *countingStore) ListForDelivery(context.Context) ([]webhooks.Webhook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveryRead++
	if s.deliveryErr != nil {
		return nil, s.deliveryErr
	}
	return append([]webhooks.Webhook(nil), s.hooks...), nil
}

func (s *countingStore) Get(_ context.Context, id string) (webhooks.Webhook, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	for _, hook := range s.hooks {
		if hook.ID == id {
			return hook.Public(), true, nil
		}
	}
	return webhooks.Webhook{}, false, nil
}

func (s *countingStore) Delete(context.Context, string) (bool, error) {
	panic("countingStore is read-only")
}

// Update applies change with the package's own rules, so a durable registry
// over this store behaves as one over a production backend would.
func (s *countingStore) Update(_ context.Context, id string, change func(*webhooks.Webhook) error) (webhooks.Webhook, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates++
	for i, hook := range s.hooks {
		if hook.ID != id {
			continue
		}
		next, err := webhooks.ApplyChange(hook, change)
		if err != nil {
			return webhooks.Webhook{}, true, err
		}
		s.hooks[i] = next
		return next.Public(), true, nil
	}
	return webhooks.Webhook{}, false, nil
}

func (s *countingStore) updateCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updates
}

func (s *countingStore) getCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func (s *countingStore) calls() (lists, deliveryReads int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists, s.deliveryRead
}

// The production registry is the durable one, and a delivery can only be
// signed if its lookup carries the secret. List is the API's read and strips
// it; the dispatcher's read must be ListForDelivery, and the API's reads must
// never reach for it.
func TestRegistry_DurableMatchingReadsTheDeliveryListNeverThePublicOne(t *testing.T) {
	store := &countingStore{hooks: []webhooks.Webhook{
		{ID: "whk-a", URL: "https://a.example/hook", Secret: "secret-a"},
		{ID: "whk-b", URL: "https://b.example/hook", Secret: "secret-b", Events: []webhooks.Event{webhooks.EventRunSucceeded}},
	}}
	registry := webhooks.NewDurableRegistry(store)
	ctx := context.Background()

	matched, err := registry.MatchingContext(ctx, webhooks.Match{Event: webhooks.EventRunSucceeded})
	if err != nil {
		t.Fatalf("MatchingContext: %v", err)
	}
	if len(matched) != 2 {
		t.Fatalf("matched %d subscriptions, want 2: %+v", len(matched), matched)
	}
	for _, hook := range matched {
		if hook.Secret == "" {
			t.Errorf("delivery lookup returned %s without its secret: its delivery would go out unsigned", hook.ID)
		}
	}
	if lists, deliveryReads := store.calls(); lists != 0 || deliveryReads != 1 {
		t.Errorf("delivery lookup made %d List and %d ListForDelivery calls, want 0 and 1", lists, deliveryReads)
	}

	listed, err := registry.ListContext(ctx)
	if err != nil {
		t.Fatalf("ListContext: %v", err)
	}
	for _, hook := range listed {
		if hook.Secret != "" {
			t.Errorf("ListContext disclosed the secret of %s", hook.ID)
		}
	}
	got, ok, err := registry.GetContext(ctx, "whk-a")
	if err != nil || !ok {
		t.Fatalf("GetContext(whk-a) = %v, %v", ok, err)
	}
	if got.Secret != "" {
		t.Errorf("GetContext disclosed the secret of %s", got.ID)
	}
	if _, deliveryReads := store.calls(); deliveryReads != 1 {
		t.Errorf("a management read reached ListForDelivery: %d delivery reads, want still 1", deliveryReads)
	}
}

func TestValidateName_RequiresANamespaceScope(t *testing.T) {
	err := webhooks.ValidateName("x", "")
	if err == nil || !strings.Contains(err.Error(), "a name requires a namespace scope") {
		t.Fatalf("ValidateName(x, \"\") = %v, want the namespace-scope refusal", err)
	}
	if err := webhooks.ValidateName("etl-loader", "payments"); err != nil {
		t.Fatalf("ValidateName with a namespace = %v", err)
	}
	if err := webhooks.ValidateName("", ""); err != nil {
		t.Fatalf("an empty name is unnamed and valid: %v", err)
	}
	for _, bad := range []string{"Etl", "-x", "a b", "a" + strings.Repeat("b", 200)} {
		if err := webhooks.ValidateName(bad, "payments"); err == nil {
			t.Errorf("ValidateName(%q) accepted a non-slug", bad)
		}
	}
}

func TestRegistry_UpdateInMemoryKeepsIDAndSecret(t *testing.T) {
	ctx := context.Background()
	registry := webhooks.NewRegistry()
	created, err := registry.RegisterContext(ctx, webhooks.Registration{
		URL: "https://a.example/hook", Secret: "original", Namespace: "payments",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	updated, ok, err := registry.UpdateContext(ctx, created.ID, func(w *webhooks.Webhook) error {
		w.Name = "etl-loader"
		w.URL = "https://b.example/hook"
		w.OrkflowName = "nightly-etl"
		w.Events = []webhooks.Event{webhooks.EventRunFailed, webhooks.EventTaskSucceeded, webhooks.EventRunFailed}
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("UpdateContext = %v, %v", ok, err)
	}
	if updated.Secret != "" {
		t.Fatal("UpdateContext returned the signing secret; it must answer with a Public copy")
	}
	if updated.ID != created.ID || updated.Name != "etl-loader" || updated.URL != "https://b.example/hook" {
		t.Errorf("updated = %+v", updated)
	}
	if got := updated.Events; len(got) != 2 || got[0] != webhooks.EventTaskSucceeded || got[1] != webhooks.EventRunFailed {
		t.Errorf("events = %v, want lifecycle order without duplicates", got)
	}
	matched, err := registry.MatchingContext(ctx, webhooks.Match{
		Event: webhooks.EventRunFailed, Namespace: "payments", OrkflowName: "nightly-etl",
	})
	if err != nil || len(matched) != 1 {
		t.Fatalf("MatchingContext = %v, %v; want the updated hook", matched, err)
	}
	if matched[0].Secret != "original" || matched[0].URL != "https://b.example/hook" {
		t.Errorf("delivery read %+v, want the original secret and the new scope", matched[0])
	}
	if miss, _ := registry.MatchingContext(ctx, webhooks.Match{Event: webhooks.EventRunFailed, Namespace: "payments", OrkflowName: "other"}); len(miss) != 0 {
		t.Errorf("the narrowed scope still matches another orkflow: %v", miss)
	}
	if _, ok, err := registry.UpdateContext(ctx, "wh-absent", func(*webhooks.Webhook) error { return nil }); ok || err != nil {
		t.Errorf("UpdateContext of an absent id = %v, %v; want absent", ok, err)
	}
	_, _, err = registry.UpdateContext(ctx, created.ID, func(w *webhooks.Webhook) error { w.Secret = "rotated"; return nil })
	if err == nil {
		t.Error("an update that changed the secret was accepted")
	}
}

func TestRegistry_UpdateRefusesATakenNameInTheScope(t *testing.T) {
	ctx := context.Background()
	registry := webhooks.NewRegistry()
	if _, err := registry.RegisterContext(ctx, webhooks.Registration{URL: "https://a.example/", Namespace: "payments", Name: "taken"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := registry.RegisterContext(ctx, webhooks.Registration{URL: "https://a.example/", Namespace: "payments", Name: "taken"}); !errors.Is(err, webhooks.ErrNameTaken) {
		t.Fatalf("a second registration of the name = %v, want ErrNameTaken", err)
	}
	if _, err := registry.RegisterContext(ctx, webhooks.Registration{URL: "https://a.example/", Namespace: "billing", Name: "taken"}); err != nil {
		t.Fatalf("the same name in another namespace scope: %v", err)
	}
	second, err := registry.RegisterContext(ctx, webhooks.Registration{URL: "https://b.example/", Namespace: "payments"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, _, err = registry.UpdateContext(ctx, second.ID, func(w *webhooks.Webhook) error { w.Name = "taken"; return nil })
	if !errors.Is(err, webhooks.ErrNameTaken) {
		t.Fatalf("rename onto a taken name = %v, want ErrNameTaken", err)
	}
	_, _, err = registry.UpdateContext(ctx, second.ID, func(w *webhooks.Webhook) error {
		w.Name = "fresh"
		w.Namespace = ""
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "namespace scope") {
		t.Fatalf("a name without a namespace = %v, want the scope refusal", err)
	}
}

func TestRegistry_DurableUpdateCallsStoreUpdate(t *testing.T) {
	store := &countingStore{hooks: []webhooks.Webhook{
		{ID: "wh-0011223344556677", URL: "https://a.example/hook", Secret: "secret-a", Namespace: "payments"},
	}}
	registry := webhooks.NewDurableRegistry(store)
	updated, ok, err := registry.UpdateContext(context.Background(), "wh-0011223344556677", func(w *webhooks.Webhook) error {
		w.Name = "etl-loader"
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("UpdateContext = %v, %v", ok, err)
	}
	if store.updateCalls() != 1 {
		t.Fatalf("store.Update calls = %d, want 1", store.updateCalls())
	}
	if updated.Secret != "" || updated.Name != "etl-loader" {
		t.Errorf("updated = %+v, want the name and no secret", updated)
	}
}
