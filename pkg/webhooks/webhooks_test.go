package webhooks_test

import (
	"strings"
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
