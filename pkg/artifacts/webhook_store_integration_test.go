//go:build integration

package artifacts_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// TestIntegration_S3WebhookStoreRoundTripsScope proves the S3 half of the
// scoped registry against real object storage: the scope lands in the flat
// record, a second store over the same bucket (another replica) reads it back,
// and a record written by hand in the pre-scope shape lists as
// deployment-wide.
func TestIntegration_S3WebhookStoreRoundTripsScope(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()

	store, err := artifacts.NewS3WebhookStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3WebhookStore: %v", err)
	}
	replica, err := artifacts.NewS3WebhookStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3WebhookStore (replica): %v", err)
	}
	raw := newRawS3Client(t, cfg)

	namespace := "wh-ns-" + uniqueToken(t)
	created, err := store.Register(ctx, webhooks.Registration{
		URL:         "https://example.test/scoped",
		Events:      []webhooks.Event{webhooks.EventTaskSucceeded, webhooks.EventRunFailed},
		Namespace:   namespace,
		OrkflowName: "nightly-etl",
		TaskID:      "extract",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	key := artifacts.WebhookPrefix + "/" + created.ID + ".json"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, key) })

	object, err := raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject %s: %v", key, err)
	}
	body, err := io.ReadAll(object.Body)
	_ = object.Body.Close()
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	for _, want := range []string{`"namespace": "` + namespace + `"`, `"orkflowName": "nightly-etl"`, `"taskId": "extract"`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("object %s does not contain %s:\n%s", key, want, body)
		}
	}

	got, ok, err := replica.Get(ctx, created.ID)
	if err != nil || !ok {
		t.Fatalf("replica Get(%s) = %v, %v", created.ID, ok, err)
	}
	if got.Namespace != namespace || got.OrkflowName != "nightly-etl" || got.TaskID != "extract" || got.Secret != "" {
		t.Errorf("replica Get = %+v, want the scope and no secret", got)
	}
	listedScoped := false
	listed, err := replica.List(ctx)
	if err != nil {
		t.Fatalf("replica List: %v", err)
	}
	for _, hook := range listed {
		if hook.ID == created.ID {
			listedScoped = hook.Namespace == namespace && hook.OrkflowName == "nightly-etl" && hook.TaskID == "extract"
		}
	}
	if !listedScoped {
		t.Errorf("replica List did not return %s with its scope", created.ID)
	}

	// A record PUT by hand in today's pre-scope shape.
	legacyID := "wh-" + uniqueToken(t)
	legacyKey := artifacts.WebhookPrefix + "/" + legacyID + ".json"
	legacy := strings.Replace(preScopeRecord, "wh-00112233445566778899", legacyID, 1)
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Bucket), Key: aws.String(legacyKey),
		Body: bytes.NewReader([]byte(legacy)), ContentType: aws.String("application/json"),
	}); err != nil {
		t.Fatalf("PutObject %s: %v", legacyKey, err)
	}
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, legacyKey) })

	listed, err = replica.List(ctx)
	if err != nil {
		t.Fatalf("replica List after the legacy record: %v", err)
	}
	found := false
	for _, hook := range listed {
		if hook.ID != legacyID {
			continue
		}
		found = true
		if hook.Namespace != "" || hook.OrkflowName != "" || hook.TaskID != "" {
			t.Errorf("the pre-scope record listed with scope %q/%q/%q, want deployment-wide", hook.Namespace, hook.OrkflowName, hook.TaskID)
		}
	}
	if !found {
		t.Errorf("the pre-scope record %s was not listed", legacyID)
	}
}
