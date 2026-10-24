package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// newWebhookFixture pins the registry's ids, secrets and clock so a golden
// file can hold the human output byte for byte, and hands the registry back
// so a test can read what reached the server.
func newWebhookFixture(t *testing.T) (*fixture, *webhooks.Registry) {
	t.Helper()
	next := 0
	registry := webhooks.NewRegistry(
		webhooks.WithIDGenerator(func() (string, error) {
			next++
			return fmt.Sprintf("wh-%016x", next), nil
		}),
		webhooks.WithSecretGenerator(func() (string, error) { return "fixed-secret", nil }),
		webhooks.WithClock(func() time.Time { return time.Unix(1755302400, 0).UTC() }),
	)
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.Webhooks = registry })
	return f, registry
}

func TestWebhookRegister_ScopesBySubscriptionFlags(t *testing.T) {
	f, registry := newWebhookFixture(t)
	out := f.run("webhook", "register", "https://example.test/hooks",
		"-n", "analytics", "--orkflow", "nightly-etl", "--task", "extract",
		"--event", "TASK_SUCCEEDED").requireExit(t, exitOK)
	requireGolden(t, "webhook-register.golden", out.stdout)

	stored := registry.List()
	if len(stored) != 1 {
		t.Fatalf("server holds %d webhooks, want 1", len(stored))
	}
	if stored[0].Namespace != "analytics" || stored[0].OrkflowName != "nightly-etl" || stored[0].TaskID != "extract" {
		t.Errorf("server received scope %q/%q/%q, want analytics/nightly-etl/extract",
			stored[0].Namespace, stored[0].OrkflowName, stored[0].TaskID)
	}

	jsonOut := f.run("--json", "webhook", "register", "https://example.test/json",
		"-n", "analytics", "--orkflow", "nightly-etl", "--task", "extract",
		"--event", "TASK_SUCCEEDED").requireExit(t, exitOK)
	var envelope struct {
		Webhook map[string]any `json:"webhook"`
	}
	if err := json.Unmarshal([]byte(jsonOut.stdout), &envelope); err != nil {
		t.Fatalf("register --json is not JSON: %v\n%s", err, jsonOut.stdout)
	}
	if envelope.Webhook["namespace"] != "analytics" || envelope.Webhook["orkflowName"] != "nightly-etl" ||
		envelope.Webhook["taskId"] != "extract" {
		t.Errorf("register --json = %v, want namespace, orkflowName and taskId", envelope.Webhook)
	}
}

func TestWebhookRegister_IgnoresTheSavedNamespaceUnlessExplicit(t *testing.T) {
	f, registry := newWebhookFixture(t)
	// A saved default and an environment selection: neither narrows a
	// subscription, whose default is every namespace.
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	f.run("namespace", "use", "payments").requireExit(t, exitOK)
	t.Setenv(namespaceEnvVar, "payments")

	f.run("webhook", "register", "https://example.test/audit").requireExit(t, exitOK).
		requireContains(t, "SCOPE", "ALL")
	f.run("webhook", "register", "https://example.test/payments", "-n", "payments").requireExit(t, exitOK).
		requireContains(t, "payments/*/*")

	stored := registry.List()
	if len(stored) != 2 {
		t.Fatalf("server holds %d webhooks, want 2", len(stored))
	}
	if stored[0].Namespace != "" {
		t.Errorf("without -n the request carried namespace %q, want empty (every namespace)", stored[0].Namespace)
	}
	if stored[1].Namespace != "payments" {
		t.Errorf("with -n payments the request carried namespace %q", stored[1].Namespace)
	}
}

func TestWebhookRegister_RefusesATaskScopeWithoutATaskEvent(t *testing.T) {
	f, _ := newWebhookFixture(t)
	before := len(f.requests())
	outcome := f.run("webhook", "register", "https://example.test/hooks", "--task", "extract", "--event", "RUN_FAILED")
	if outcome.code == exitOK {
		t.Fatalf("a task scope with no task event was accepted:\n%s", outcome.stdout)
	}
	want := webhooks.ValidateScope("", "", "extract", []webhooks.Event{webhooks.EventRunFailed}).Error()
	if !strings.Contains(outcome.stderr, want) {
		t.Errorf("stderr does not carry the ValidateScope message %q:\n%s", want, outcome.stderr)
	}
	if got := f.requests()[before:]; len(got) != 0 {
		t.Errorf("the refusal was not local: %v crossed the wire", got)
	}

	for _, bad := range [][]string{{"-n", "Bad Name"}, {"--orkflow", "-x"}} {
		before := len(f.requests())
		args := append([]string{"webhook", "register", "https://example.test/hooks"}, bad...)
		if outcome := f.run(args...); outcome.code == exitOK {
			t.Errorf("%v was accepted", bad)
		}
		if got := f.requests()[before:]; len(got) != 0 {
			t.Errorf("%v: the refusal was not local: %v crossed the wire", bad, got)
		}
	}
}

func TestWebhookRegister_AcceptsTaskEventsAndRefusesTaskLog(t *testing.T) {
	f, _ := newWebhookFixture(t)
	f.run("webhook", "register", "https://example.test/hooks",
		"--event", "TASK_SUCCEEDED,TASK_FAILED").requireExit(t, exitOK).
		requireContains(t, "TASK_SUCCEEDED,TASK_FAILED")

	outcome := f.run("webhook", "register", "https://example.test/hooks", "--event", "TASK_LOG")
	if outcome.code == exitOK {
		t.Fatalf("TASK_LOG was accepted:\n%s", outcome.stdout)
	}
	for _, event := range webhooks.KnownEvents() {
		if !strings.Contains(outcome.stderr, string(event)) {
			t.Errorf("the TASK_LOG refusal does not name %s:\n%s", event, outcome.stderr)
		}
	}
}

func TestWebhookList_RendersTheScopeColumn(t *testing.T) {
	f, _ := newWebhookFixture(t)
	f.run("webhook", "register", "https://example.test/audit").requireExit(t, exitOK)
	f.run("webhook", "register", "https://example.test/etl",
		"-n", "analytics", "--orkflow", "nightly-etl", "--task", "extract",
		"--event", "TASK_SUCCEEDED", "--name", "etl-loader").requireExit(t, exitOK)

	requireGolden(t, "webhook-list.golden", f.run("webhook", "list").requireExit(t, exitOK).stdout)
	requireGolden(t, "webhook-list.json.golden", f.run("--json", "webhook", "list").requireExit(t, exitOK).stdout)
}

func TestWebhookScopeCell_IsPositionalWithAStarForEachUnsetComponent(t *testing.T) {
	for _, tc := range []struct {
		hook webhooks.Webhook
		want string
	}{
		{webhooks.Webhook{}, "ALL"},
		{webhooks.Webhook{Namespace: "analytics", OrkflowName: "nightly-etl", TaskID: "extract"}, "analytics/nightly-etl/extract"},
		{webhooks.Webhook{Namespace: "analytics"}, "analytics/*/*"},
		{webhooks.Webhook{OrkflowName: "nightly-etl"}, "*/nightly-etl/*"},
		{webhooks.Webhook{Namespace: "analytics", TaskID: "extract"}, "analytics/*/extract"},
	} {
		if got := webhookScopeCell(tc.hook); got != tc.want {
			t.Errorf("webhookScopeCell(%+v) = %q, want %q", tc.hook, got, tc.want)
		}
	}
}
