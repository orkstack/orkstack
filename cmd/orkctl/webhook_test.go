package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// webhookIDPattern extracts the id `webhook register` reports.
var webhookIDPattern = regexp.MustCompile(`wh-[0-9a-f]+`)

func TestWebhookRegister_RegistersACallbackAndListShowsIt(t *testing.T) {
	f := newFixture(t)

	registered := f.run("webhook", "register", "https://example.test/hooks",
		"--event", "RUN_SUCCEEDED", "--event", "RUN_FAILED").requireExit(t, exitOK)
	registered.requireContains(t,
		"Registered webhook wh-",
		"https://example.test/hooks",
		"RUN_SUCCEEDED,RUN_FAILED",
		"SECRET",
	)
	id := webhookIDPattern.FindString(registered.stdout)
	if id == "" {
		t.Fatalf("register did not report a webhook id:\n%s", registered.stdout)
	}

	listed := f.run("webhook", "list").requireExit(t, exitOK)
	listed.requireContains(t, "ID", "URL", "EVENTS", "CREATED",
		id, "https://example.test/hooks", "RUN_SUCCEEDED,RUN_FAILED")
}

func TestWebhookRegister_WithoutEventsSubscribesToEveryTransition(t *testing.T) {
	f := newFixture(t)

	f.run("webhook", "register", "https://example.test/audit").requireExit(t, exitOK)
	f.run("webhook", "list").requireExit(t, exitOK).
		requireContains(t, "https://example.test/audit", "ALL")
}

func TestWebhookRegister_ReportsTheSigningSecretOnceInJSON(t *testing.T) {
	f := newFixture(t)

	out := f.run("webhook", "register", "https://example.test/hooks",
		"--event", "RUN_SUCCEEDED", "--json").requireExit(t, exitOK)

	var registered struct {
		Webhook struct {
			ID        string   `json:"id"`
			URL       string   `json:"url"`
			Events    []string `json:"events"`
			Secret    string   `json:"secret"`
			CreatedAt string   `json:"createdAt"`
		} `json:"webhook"`
	}
	if err := json.Unmarshal([]byte(out.stdout), &registered); err != nil {
		t.Fatalf("register --json is not JSON: %v\n%s", err, out.stdout)
	}
	if registered.Webhook.ID == "" || registered.Webhook.Secret == "" {
		t.Fatalf("register --json = %+v, want an id and a secret", registered.Webhook)
	}
	if len(registered.Webhook.Events) != 1 || registered.Webhook.Events[0] != "RUN_SUCCEEDED" {
		t.Errorf("events = %v, want [RUN_SUCCEEDED]", registered.Webhook.Events)
	}

	// The secret is disclosed at registration and never again.
	listed := f.run("webhook", "list", "--json").requireExit(t, exitOK)
	if strings.Contains(listed.stdout, registered.Webhook.Secret) {
		t.Errorf("list disclosed the signing secret:\n%s", listed.stdout)
	}
}

func TestWebhookRegister_UsesACallerSuppliedSecret(t *testing.T) {
	f := newFixture(t)
	out := f.run("webhook", "register", "https://example.test/hooks",
		"--secret", "shared-with-the-receiver").requireExit(t, exitOK)
	out.requireContains(t, "shared-with-the-receiver")
}

func TestWebhookRegister_RejectsAnUnknownEvent(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("webhook", "register", "https://example.test/hooks", "--event", "RUN_EXPLODED")
	if outcome.code == exitOK {
		t.Fatalf("registering an unknown event succeeded:\n%s", outcome.stdout)
	}
	if !strings.Contains(outcome.stderr, "RUN_SUCCEEDED") {
		t.Errorf("stderr does not name the valid events:\n%s", outcome.stderr)
	}
	f.run("webhook", "list").requireExit(t, exitOK).requireContains(t, "No webhooks.")
}

func TestWebhookRegister_RejectsAURLNoDeliveryCouldReach(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("webhook", "register", "example.test/hooks")
	if outcome.code == exitOK {
		t.Fatalf("registering a scheme-less URL succeeded:\n%s", outcome.stdout)
	}
	if !strings.Contains(outcome.stderr, "scheme must be http or https") {
		t.Errorf("stderr does not explain the rejection:\n%s", outcome.stderr)
	}
}

func TestWebhookList_SaysSoWhenNothingIsRegistered(t *testing.T) {
	f := newFixture(t)
	f.run("webhook", "list").requireExit(t, exitOK).requireContains(t, "No webhooks.")
}

func TestWebhookDelete_RemovesTheSubscription(t *testing.T) {
	f := newFixture(t)
	registered := f.run("webhook", "register", "https://example.test/hooks").requireExit(t, exitOK)
	id := webhookIDPattern.FindString(registered.stdout)
	if id == "" {
		t.Fatalf("register did not report a webhook id:\n%s", registered.stdout)
	}

	f.run("webhook", "delete", id).requireExit(t, exitOK).
		requireContains(t, "Deleted webhook "+id)
	f.run("webhook", "list").requireExit(t, exitOK).requireContains(t, "No webhooks.")

	outcome := f.run("webhook", "delete", id)
	if outcome.code == exitOK {
		t.Fatal("deleting the same webhook twice succeeded")
	}
	if !strings.Contains(outcome.stderr, "not_found") {
		t.Errorf("stderr does not report the missing webhook:\n%s", outcome.stderr)
	}
}

func TestWebhookRegister_AcceptsACommaSeparatedEventList(t *testing.T) {
	f := newFixture(t)
	f.run("webhook", "register", "https://example.test/hooks",
		"--event", "run_started,RUN_FAILED").requireExit(t, exitOK).
		requireContains(t, "RUN_STARTED,RUN_FAILED")
}

func TestWebhookCommand_IsDiscoverableFromHelp(t *testing.T) {
	// Help needs no engine, so it runs Execute directly rather than through
	// the fixture, which appends --server to every invocation.
	for _, tc := range []struct {
		args     []string
		fragment []string
	}{
		{[]string{"--help"}, []string{"webhook", "Register and inspect run-event callbacks"}},
		{[]string{"webhook", "--help"}, []string{"register", "list", "delete", "deliveries", "delivery", "redeliver",
			"What each delivery did is recorded"}},
		{[]string{"webhook", "deliveries", "--help"}, []string{"--webhook", "--run", "--event", "--delivered",
			"--since", "--until", "--limit", "--page-token", "every namespace unless -n"}},
		{[]string{"webhook", "delivery", "--help"}, []string{"delivery <id>"}},
		{[]string{"webhook", "redeliver", "--help"}, []string{"once, now", "you are the retry policy", "exits 1"}},
		{[]string{"webhook", "register", "--help"}, []string{"--event", "--secret", "RUN_SUCCEEDED"}},
	} {
		var stdout, stderr bytes.Buffer
		if code := Execute(tc.args, &stdout, &stderr); code != exitOK {
			t.Fatalf("orkctl %v exited %d: %s", tc.args, code, stderr.String())
		}
		for _, fragment := range tc.fragment {
			if !strings.Contains(stdout.String(), fragment) {
				t.Errorf("orkctl %v help does not mention %q:\n%s", tc.args, fragment, stdout.String())
			}
		}
	}
}
