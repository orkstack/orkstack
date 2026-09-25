package artifacts_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

func TestWebhookStore_RoundTripsScopeThroughTheLocalRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalWebhookStore(dir)
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	created, err := store.Register(ctx, webhooks.Registration{
		URL:         "https://example.test/hooks",
		Events:      []webhooks.Event{webhooks.EventTaskSucceeded},
		Secret:      "s3cr3t",
		Namespace:   "analytics",
		OrkflowName: "nightly-etl",
		TaskID:      "extract",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if created.Secret != "s3cr3t" || created.Namespace != "analytics" || created.OrkflowName != "nightly-etl" || created.TaskID != "extract" {
		t.Fatalf("Register returned %+v, want the scope and the secret", created)
	}

	got, ok, err := store.Get(ctx, created.ID)
	if err != nil || !ok {
		t.Fatalf("Get(%s) = %v, %v", created.ID, ok, err)
	}
	if got.Namespace != "analytics" || got.OrkflowName != "nightly-etl" || got.TaskID != "extract" {
		t.Errorf("Get returned scope %q/%q/%q, want analytics/nightly-etl/extract", got.Namespace, got.OrkflowName, got.TaskID)
	}
	if got.Secret != "" {
		t.Errorf("Get disclosed the secret %q", got.Secret)
	}
	listed, err := store.List(ctx)
	if err != nil || len(listed) != 1 {
		t.Fatalf("List = %+v, %v", listed, err)
	}
	if listed[0].Namespace != "analytics" || listed[0].OrkflowName != "nightly-etl" || listed[0].TaskID != "extract" || listed[0].Secret != "" {
		t.Errorf("List returned %+v, want the scope without the secret", listed[0])
	}

	raw, err := os.ReadFile(filepath.Join(dir, artifacts.WebhookPrefix, created.ID+".json"))
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	for _, key := range []string{`"namespace": "analytics"`, `"orkflowName": "nightly-etl"`, `"taskId": "extract"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("record does not contain %s:\n%s", key, raw)
		}
	}

	// An unscoped registration's record carries no scope keys.
	plain, err := store.Register(ctx, webhooks.Registration{URL: "https://example.test/plain"})
	if err != nil {
		t.Fatalf("Register unscoped: %v", err)
	}
	raw, err = os.ReadFile(filepath.Join(dir, artifacts.WebhookPrefix, plain.ID+".json"))
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	for _, key := range []string{"namespace", "orkflowName", "taskId"} {
		if strings.Contains(string(raw), `"`+key+`"`) {
			t.Errorf("an unscoped record carries %q:\n%s", key, raw)
		}
	}
}

// preScopeRecord is a record exactly as the store wrote one before scope
// existed: no namespace, orkflowName or taskId keys.
const preScopeRecord = `{
  "webhook": {
    "id": "wh-00112233445566778899",
    "url": "https://example.test/legacy",
    "events": [
      "EVENT_TYPE_RUN_SUCCEEDED"
    ],
    "createdAt": "2025-08-16T00:00:00Z"
  },
  "secret": "legacy-secret"
}
`

func TestWebhookStore_ReadsAPreScopeRecordAsDeploymentWide(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalWebhookStore(dir)
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	path := filepath.Join(dir, artifacts.WebhookPrefix, "wh-00112233445566778899.json")
	if err := os.WriteFile(path, []byte(preScopeRecord), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	listed, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("List = %+v, want the one pre-scope record", listed)
	}
	hook := listed[0]
	if hook.ID != "wh-00112233445566778899" || hook.URL != "https://example.test/legacy" {
		t.Errorf("listed %+v, want the legacy record", hook)
	}
	if hook.Namespace != "" || hook.OrkflowName != "" || hook.TaskID != "" {
		t.Errorf("a pre-scope record decoded with scope %q/%q/%q, want deployment-wide", hook.Namespace, hook.OrkflowName, hook.TaskID)
	}
	if !hook.Matches(webhooks.Match{Event: webhooks.EventRunSucceeded, Namespace: "marketing", OrkflowName: "anything"}) {
		t.Error("a pre-scope record does not match a run in an arbitrary namespace")
	}
}

func TestWebhookStore_RefusesAnInvalidScopeOnRegisterAndOnRead(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalWebhookStore(dir)
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	_, err = store.Register(ctx, webhooks.Registration{URL: "https://example.test/hooks", Namespace: "Bad Name"})
	if err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("Register with a non-slug namespace = %v, want the ValidateScope error", err)
	}
	_, err = store.Register(ctx, webhooks.Registration{URL: "https://example.test/hooks", TaskID: "extract",
		Events: []webhooks.Event{webhooks.EventRunFailed}})
	if err == nil || !strings.Contains(err.Error(), "task") {
		t.Fatalf("Register with a task scope and no task event = %v, want the ValidateScope error", err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, artifacts.WebhookPrefix))
	if err != nil {
		t.Fatalf("read registry dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("a refused registration wrote %d object(s)", len(entries))
	}

	// A record edited by hand into an invalid scope is refused on read, never
	// silently widened to deployment-wide.
	edited := strings.Replace(preScopeRecord, `"url"`, `"namespace": "Bad Name",
    "url"`, 1)
	path := filepath.Join(dir, artifacts.WebhookPrefix, "wh-00112233445566778899.json")
	if err := os.WriteFile(path, []byte(edited), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if hook, ok, err := store.Get(ctx, "wh-00112233445566778899"); err == nil {
		t.Fatalf("Get of a record with namespace \"Bad Name\" = (%+v, %v), want an error", hook, ok)
	}
}

// TestWebhookStore_DurableMatchingCarriesTheSigningSecret pins the
// dispatcher's read on the durable path: a delivery is signed with the secret
// the registration issued, so Matching must return it even though List and
// Get never do. Reading Public copies here sent every production delivery
// unsigned.
func TestWebhookStore_DurableMatchingCarriesTheSigningSecret(t *testing.T) {
	store, err := artifacts.NewLocalWebhookStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	registry := webhooks.NewDurableRegistry(store)
	hook, err := registry.RegisterContext(context.Background(), webhooks.Registration{
		URL: "https://example.test/hooks", Secret: "s3cr3t", Namespace: "analytics",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	matching, err := registry.MatchingContext(context.Background(),
		webhooks.Match{Event: webhooks.EventRunSucceeded, Namespace: "analytics", OrkflowName: "etl"})
	if err != nil {
		t.Fatalf("MatchingContext: %v", err)
	}
	if len(matching) != 1 || matching[0].ID != hook.ID || matching[0].Secret != "s3cr3t" {
		t.Fatalf("durable Matching = %+v, want %s with the secret a delivery signs with", matching, hook.ID)
	}
	listed, err := registry.ListContext(context.Background())
	if err != nil {
		t.Fatalf("ListContext: %v", err)
	}
	if len(listed) != 1 || listed[0].Secret != "" {
		t.Errorf("List = %+v, want the subscription without its secret", listed)
	}
}
