package artifacts_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The tests in this file prove the declarative-automation additions to the
// trigger and webhook records: an optional name, and an in-place update that
// keeps the credential an external system holds.

func TestTriggerStore_RoundTripsNameThroughTheLocalRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalTriggerStore(dir)
	if err != nil {
		t.Fatalf("NewLocalTriggerStore: %v", err)
	}
	created, _ := mustCreate(t, store, triggers.Registration{Namespace: "payments", OrkflowName: "deploy", Name: "github-deploy"})
	raw, err := os.ReadFile(filepath.Join(dir, artifacts.TriggerPrefix, created.ID+".json"))
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	if !strings.Contains(string(raw), `"name": "github-deploy"`) {
		t.Errorf("record does not carry the name:\n%s", raw)
	}
	// A fresh store over the same directory reads it back.
	reopened, err := artifacts.NewLocalTriggerStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, ok, err := reopened.Get(ctx, created.ID)
	if err != nil || !ok || got.Name != "github-deploy" {
		t.Fatalf("Get = %+v, %v, %v; want the name", got, ok, err)
	}
	listed, err := reopened.List(ctx, "payments", "")
	if err != nil || len(listed) != 1 || listed[0].Name != "github-deploy" {
		t.Fatalf("List = %+v, %v", listed, err)
	}
}

// preNameTriggerRecord is a trigger record in the shape written before names
// existed. Nothing is migrated: it must read as an unnamed trigger.
const preNameTriggerRecord = `{
  "id": "trg-00112233445566778899",
  "namespace": "payments",
  "orkflowName": "deploy",
  "createdAt": "2026-01-02T03:04:05Z",
  "secretSalt": "00112233",
  "secretHash": "aabbccdd"
}
`

func TestTriggerStore_ReadsAPreNameRecordAsUnnamed(t *testing.T) {
	dir := t.TempDir()
	store, err := artifacts.NewLocalTriggerStore(dir)
	if err != nil {
		t.Fatalf("NewLocalTriggerStore: %v", err)
	}
	path := filepath.Join(dir, artifacts.TriggerPrefix, "trg-00112233445566778899.json")
	if err := os.WriteFile(path, []byte(preNameTriggerRecord), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	got, ok, err := store.Get(context.Background(), "trg-00112233445566778899")
	if err != nil || !ok {
		t.Fatalf("Get = %v, %v", ok, err)
	}
	if got.Name != "" || got.OrkflowName != "deploy" {
		t.Errorf("pre-name record decoded as %+v, want an unnamed trigger of deploy", got)
	}
	// A hand-edited invalid name is refused on read, as a bad namespace is.
	bad := strings.Replace(preNameTriggerRecord, `"namespace"`, `"name": "Not A Slug", "namespace"`, 1)
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if _, _, err := store.Get(context.Background(), "trg-00112233445566778899"); err == nil {
		t.Error("a record with an invalid name was accepted on read")
	}
}

func TestLocalTriggerStore_UpdateKeepsTokenAndIDAndNeverResurrects(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalTriggerStore(dir)
	if err != nil {
		t.Fatalf("NewLocalTriggerStore: %v", err)
	}
	created, secret := mustCreate(t, store, triggers.Registration{Namespace: "payments", OrkflowName: "deploy"})
	updated, ok, err := store.Update(ctx, created.ID, "payments", func(tr *triggers.Trigger) error {
		tr.Name = "github-deploy"
		tr.OrkflowVersion = "1.4.0"
		return nil
	})
	if err != nil || !ok || updated.ID != created.ID || updated.Name != "github-deploy" {
		t.Fatalf("Update = %+v, %v, %v", updated, ok, err)
	}
	stored, _, err := store.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !triggers.VerifySecret(stored, secret) {
		t.Fatal("the original token no longer verifies after an update")
	}
	if stored.OrkflowVersion != "1.4.0" || stored.Name != "github-deploy" {
		t.Errorf("stored = %+v", stored)
	}
	if _, ok, err := store.Update(ctx, created.ID, "billing", func(*triggers.Trigger) error { return nil }); ok || err != nil {
		t.Errorf("Update in another namespace = %v, %v; want absent", ok, err)
	}
	if ok, err := store.Delete(ctx, created.ID, "payments"); err != nil || !ok {
		t.Fatalf("Delete = %v, %v", ok, err)
	}
	if _, ok, err := store.Update(ctx, created.ID, "payments", func(tr *triggers.Trigger) error { tr.Description = "back"; return nil }); ok || err != nil {
		t.Fatalf("Update after delete = %v, %v; want absent", ok, err)
	}
	if _, err := os.Stat(filepath.Join(dir, artifacts.TriggerPrefix, created.ID+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the deleted record's key exists again: %v", err)
	}
}

func TestLocalTriggerStore_NameIsUniqueInNamespace(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalTriggerStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalTriggerStore: %v", err)
	}
	mustCreate(t, store, triggers.Registration{Namespace: "payments", OrkflowName: "deploy", Name: "taken"})
	if _, _, err := store.Create(ctx, triggers.Registration{Namespace: "payments", OrkflowName: "deploy", Name: "taken"}); !errors.Is(err, triggers.ErrNameTaken) {
		t.Fatalf("Create of a taken name = %v, want ErrNameTaken", err)
	}
	mustCreate(t, store, triggers.Registration{Namespace: "billing", OrkflowName: "deploy", Name: "taken"})
	other, _ := mustCreate(t, store, triggers.Registration{Namespace: "payments", OrkflowName: "deploy"})
	if _, _, err := store.Update(ctx, other.ID, "payments", func(tr *triggers.Trigger) error { tr.Name = "taken"; return nil }); !errors.Is(err, triggers.ErrNameTaken) {
		t.Fatalf("rename onto a taken name = %v, want ErrNameTaken", err)
	}
}

func TestWebhookStore_RoundTripsNameThroughTheLocalRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalWebhookStore(dir)
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	created, err := store.Register(ctx, webhooks.Registration{URL: "https://example.test/etl", Namespace: "payments", Name: "etl-loader"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	reopened, err := artifacts.NewLocalWebhookStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, ok, err := reopened.Get(ctx, created.ID)
	if err != nil || !ok || got.Name != "etl-loader" {
		t.Fatalf("Get = %+v, %v, %v; want the name", got, ok, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, artifacts.WebhookPrefix, created.ID+".json"))
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	if !strings.Contains(string(raw), `"name": "etl-loader"`) {
		t.Errorf("record does not carry webhook.name:\n%s", raw)
	}
}

func TestWebhookStore_ReadsAPreNameRecordAsUnnamed(t *testing.T) {
	dir := t.TempDir()
	store, err := artifacts.NewLocalWebhookStore(dir)
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	path := filepath.Join(dir, artifacts.WebhookPrefix, "wh-00112233445566778899.json")
	if err := os.WriteFile(path, []byte(preScopeRecord), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	got, ok, err := store.Get(context.Background(), "wh-00112233445566778899")
	if err != nil || !ok || got.Name != "" {
		t.Fatalf("Get = %+v, %v, %v; want an unnamed subscription", got, ok, err)
	}
}

func TestLocalWebhookStore_UpdateKeepsSecretAndID(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalWebhookStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	created, err := store.Register(ctx, webhooks.Registration{URL: "https://example.test/a", Namespace: "payments"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	disclosed := created.Secret
	updated, ok, err := store.Update(ctx, created.ID, func(w *webhooks.Webhook) error {
		w.Name = "etl-loader"
		w.URL = "https://example.test/b"
		w.OrkflowName = "nightly-etl"
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("Update = %v, %v", ok, err)
	}
	if updated.Secret != "" || updated.ID != created.ID {
		t.Errorf("Update returned %+v, want the same id and no secret", updated)
	}
	delivery, err := store.ListForDelivery(ctx)
	if err != nil || len(delivery) != 1 {
		t.Fatalf("ListForDelivery = %v, %v", delivery, err)
	}
	if delivery[0].Secret != disclosed || delivery[0].URL != "https://example.test/b" || delivery[0].Name != "etl-loader" {
		t.Errorf("delivery read %+v, want the disclosed secret and the new URL", delivery[0])
	}
	listed, _ := store.List(ctx)
	got, _, _ := store.Get(ctx, created.ID)
	if listed[0].Secret != "" || got.Secret != "" {
		t.Error("List or Get carries the secret after an update")
	}
	if ok, _ := store.Delete(ctx, created.ID); !ok {
		t.Fatal("Delete reported absent")
	}
	if _, ok, err := store.Update(ctx, created.ID, func(w *webhooks.Webhook) error { return nil }); ok || err != nil {
		t.Fatalf("Update after delete = %v, %v; want absent", ok, err)
	}
	if _, found, _ := store.Get(ctx, created.ID); found {
		t.Fatal("an update resurrected a deleted subscription")
	}
}

func TestWebhookStore_NamedRegisterRequiresAScopeAndAFreeName(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalWebhookStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	if _, err := store.Register(ctx, webhooks.Registration{URL: "https://example.test/", Name: "etl"}); err == nil ||
		!strings.Contains(err.Error(), "namespace scope") {
		t.Fatalf("a deployment-wide named register = %v, want the scope refusal", err)
	}
	if _, err := store.Register(ctx, webhooks.Registration{URL: "https://example.test/", Namespace: "payments", Name: "etl"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := store.Register(ctx, webhooks.Registration{URL: "https://example.test/", Namespace: "payments", Name: "etl"}); !errors.Is(err, webhooks.ErrNameTaken) {
		t.Fatalf("a taken name = %v, want ErrNameTaken", err)
	}
	if _, err := store.Register(ctx, webhooks.Registration{URL: "https://example.test/", Namespace: "billing", Name: "etl"}); err != nil {
		t.Fatalf("the same name in another namespace: %v", err)
	}
}
