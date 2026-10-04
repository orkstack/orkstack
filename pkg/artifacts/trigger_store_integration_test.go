//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// TestS3TriggerStore_RoundTripsARecordThatNeverHoldsThePlaintextSecret proves
// the S3 half of the registry against real object storage: the same contract
// the local store is held to in trigger_store_test.go, plus the two things
// only a bucket can show — the flat key layout, and that a second replica
// reading the same bucket resolves a trigger it never created.
func TestS3TriggerStore_RoundTripsARecordThatNeverHoldsThePlaintextSecret(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()

	store, err := artifacts.NewS3TriggerStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3TriggerStore: %v", err)
	}
	// A second store over the same bucket stands in for another engine
	// replica: it shares no memory with the first one.
	replica, err := artifacts.NewS3TriggerStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3TriggerStore (replica): %v", err)
	}
	raw := newRawS3Client(t, cfg)

	// uniqueToken is lowercase hex, so the name is a slug the store accepts.
	namespace := "trg-ns-" + uniqueToken(t)
	created, secret, err := store.Create(ctx, triggers.Registration{
		Namespace:      namespace,
		OrkflowName:    "hello-ork",
		OrkflowVersion: "0.2.0",
		Description:    "integration",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	key := artifacts.TriggerPrefix + "/" + created.ID + ".json"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, key) })

	keys := listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.TriggerPrefix+"/")
	found := false
	for _, got := range keys {
		if got == key {
			found = true
		}
	}
	if !found {
		t.Fatalf("trigger keys = %v, want the flat key %s", keys, key)
	}

	record, ok, err := replica.Get(ctx, created.ID)
	if err != nil || !ok {
		t.Fatalf("replica Get(%s) = %v, %v", created.ID, ok, err)
	}
	if record.Namespace != namespace || record.OrkflowName != "hello-ork" ||
		record.OrkflowVersion != "0.2.0" || record.Description != "integration" {
		t.Errorf("replica read %+v, want the created coordinates", record)
	}
	if !triggers.VerifySecret(record, secret) {
		t.Error("the record another replica read does not verify the issued token")
	}
	if triggers.VerifySecret(record, secret+"x") {
		t.Error("a wrong token verified")
	}

	listed, err := replica.List(ctx, namespace, "hello-ork")
	if err != nil {
		t.Fatalf("replica List: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("replica List = %+v, want the created trigger", listed)
	}
	if len(listed[0].SecretHash) != 0 || len(listed[0].SecretSalt) != 0 {
		t.Error("a listing carried the secret's verification material")
	}

	if deleted, err := replica.Delete(ctx, created.ID, "default"); err != nil || deleted {
		t.Errorf("delete from another namespace = %v, %v; want false, nil", deleted, err)
	}
	if deleted, err := replica.Delete(ctx, created.ID, namespace); err != nil || !deleted {
		t.Fatalf("replica Delete = %v, %v; want true, nil", deleted, err)
	}
	if _, ok, err := store.Get(ctx, created.ID); err != nil || ok {
		t.Errorf("after another replica's delete Get = %v, %v; want absent", ok, err)
	}
}

// TestIntegration_S3TriggerStoreUpdateNeverResurrects: a delete landing
// between an update's read and its conditional write leaves the trigger
// absent. The write names the ETag it read, so it cannot recreate the key;
// the re-read finds nothing and the update reports false.
func TestIntegration_S3TriggerStoreUpdateNeverResurrects(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3TriggerStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3TriggerStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	namespace := "trg-ns-" + uniqueToken(t)
	created, secret, err := store.Create(ctx, triggers.Registration{Namespace: namespace, OrkflowName: "hello-ork"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	key := artifacts.TriggerPrefix + "/" + created.ID + ".json"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, key) })

	// An ordinary update first: the token keeps verifying.
	updated, ok, err := store.Update(ctx, created.ID, namespace, func(tr *triggers.Trigger) error {
		tr.Name = "github-deploy"
		tr.Description = "renamed in place"
		return nil
	})
	if err != nil || !ok || updated.Name != "github-deploy" {
		t.Fatalf("Update = %+v, %v, %v", updated, ok, err)
	}
	record, _, err := store.Get(ctx, created.ID)
	if err != nil || !triggers.VerifySecret(record, secret) {
		t.Fatalf("the original token does not verify after an update (%v)", err)
	}

	// Now delete in the window between the update's read and its write.
	artifacts.SetTriggerUpdateHookForTest(store, func() {
		artifacts.SetTriggerUpdateHookForTest(store, nil)
		if ok, err := store.Delete(ctx, created.ID, namespace); err != nil || !ok {
			t.Errorf("Delete in the window = %v, %v", ok, err)
		}
	})
	_, ok, err = store.Update(ctx, created.ID, namespace, func(tr *triggers.Trigger) error {
		tr.Description = "should never land"
		return nil
	})
	if err != nil || ok {
		t.Fatalf("Update across a delete = %v, %v; want absent", ok, err)
	}
	for _, got := range listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.TriggerPrefix+"/") {
		if got == key {
			t.Fatalf("the update resurrected %s", key)
		}
	}
	t.Logf("update across a concurrent delete: absent, key %s stays absent", key)
}

// TestIntegration_S3TriggerStoreNameIsUniqueInNamespace: a second create of
// one name in one namespace is ErrNameTaken; another namespace may hold it.
func TestIntegration_S3TriggerStoreNameIsUniqueInNamespace(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3TriggerStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3TriggerStore: %v", err)
	}
	replica, err := artifacts.NewS3TriggerStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3TriggerStore (replica): %v", err)
	}
	raw := newRawS3Client(t, cfg)
	namespace := "trg-ns-" + uniqueToken(t)
	other := "trg-ns-" + uniqueToken(t)
	created, _, err := store.Create(ctx, triggers.Registration{Namespace: namespace, OrkflowName: "hello-ork", Name: "taken"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, artifacts.TriggerPrefix+"/"+created.ID+".json")
	})
	if _, _, err := replica.Create(ctx, triggers.Registration{Namespace: namespace, OrkflowName: "hello-ork", Name: "taken"}); !errors.Is(err, triggers.ErrNameTaken) {
		t.Fatalf("a second create of the name through another replica = %v, want ErrNameTaken", err)
	}
	elsewhere, _, err := replica.Create(ctx, triggers.Registration{Namespace: other, OrkflowName: "hello-ork", Name: "taken"})
	if err != nil {
		t.Fatalf("the same name in another namespace: %v", err)
	}
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, artifacts.TriggerPrefix+"/"+elsewhere.ID+".json")
	})
	unnamed, _, err := store.Create(ctx, triggers.Registration{Namespace: namespace, OrkflowName: "hello-ork"})
	if err != nil {
		t.Fatalf("unnamed Create: %v", err)
	}
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, artifacts.TriggerPrefix+"/"+unnamed.ID+".json")
	})
	if _, _, err := replica.Update(ctx, unnamed.ID, namespace, func(tr *triggers.Trigger) error { tr.Name = "taken"; return nil }); !errors.Is(err, triggers.ErrNameTaken) {
		t.Fatalf("a rename onto the taken name = %v, want ErrNameTaken", err)
	}
}
