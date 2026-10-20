//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func versionETag(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string) string {
	t.Helper()
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("head %q: %v", key, err)
	}
	return aws.ToString(head.ETag)
}

// TestIntegration_S3OrkflowStore_AnySourceChangeIsRefused: the version is
// immutable and its source is compared whole, the header included. A comment
// added above it and a header moved from line 1 to just above the version key
// are both rewrites, refused with the plain advice and with the stored object
// untouched.
func TestIntegration_S3OrkflowStore_AnySourceChangeIsRefused(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	const body = "name: " + "header-store" + "\nversion: \"0.1.0\"\ntasks: []\n"
	stored := testOrkflow(t, name, "0.1.0", time.Now().UTC().Truncate(time.Millisecond))
	stored.SourceYaml = dsl.OrkflowHeader + body
	if _, created, err := store.Put(ctx, stored, artifacts.NoHeadExpectation()); err != nil || !created {
		t.Fatalf("put: created=%v, %v", created, err)
	}
	key, err := artifacts.OrkflowKey("", name, "0.1.0")
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, beforeTag := readKey(t, ctx, raw, cfg.S3Bucket, key), versionETag(t, ctx, raw, cfg.S3Bucket, key)
	put := func(source string) error {
		again := testOrkflow(t, name, "0.1.0", time.Now().UTC())
		again.SourceYaml = source
		_, _, err := store.Put(ctx, again, artifacts.NoHeadExpectation())
		return err
	}
	for what, source := range map[string]string{
		"a comment added":    "# a comment-only edit\n" + dsl.OrkflowHeader + body,
		"the header moved":   "# moved above the header\n" + dsl.OrkflowHeader + body,
		"the header below":   strings.Replace(dsl.OrkflowHeader+body, "version:", dsl.OrkflowHeader+"version:", 1),
		"a reordered header": "kind: Orkflow\napiVersion: orkstack/v1\n" + body,
	} {
		err := put(source)
		if !errors.Is(err, artifacts.ErrVersionExists) ||
			!strings.Contains(err.Error(), "; a version is immutable, so publish a new version: ") {
			t.Fatalf("%s = %v, want ErrVersionExists with the plain advice", what, err)
		}
		if got := readKey(t, ctx, raw, cfg.S3Bucket, key); string(got) != string(beforeBytes) {
			t.Fatalf("%s: the version object's bytes changed", what)
		}
		if got := versionETag(t, ctx, raw, cfg.S3Bucket, key); got != beforeTag {
			t.Fatalf("%s: ETag %s, want %s", what, got, beforeTag)
		}
	}
}
