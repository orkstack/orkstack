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

// legacySource is a version's source as it was stored before the header
// existed: leading comments, then its first key.
func legacySource(name string) string {
	return "# OrkStack orkflow DSL\n# stored before the header\nname: " + name + "\nversion: \"0.1.0\"\ntasks: []\n"
}

func versionETag(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string) string {
	t.Helper()
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("head %q: %v", key, err)
	}
	return aws.ToString(head.ETag)
}

// TestIntegration_S3OrkflowStore_AHeaderOnlyReapplyIsANoOp: a version stored
// without the header, put again with the two lines added, is the stored
// document — nothing is written, and the stored bytes come back.
func TestIntegration_S3OrkflowStore_AHeaderOnlyReapplyIsANoOp(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	legacy := testOrkflow(t, name, "0.1.0", time.Now().UTC().Truncate(time.Millisecond))
	legacy.SourceYaml = legacySource(name)
	if _, created, err := store.Put(ctx, legacy, artifacts.NoHeadExpectation()); err != nil || !created {
		t.Fatalf("put the legacy version: created=%v, %v", created, err)
	}
	key, err := artifacts.OrkflowKey("", name, "0.1.0")
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, beforeTag := readKey(t, ctx, raw, cfg.S3Bucket, key), versionETag(t, ctx, raw, cfg.S3Bucket, key)

	for what, source := range map[string]string{
		"after the comments": mustAddHeader(t, legacySource(name)),
		"on line 1":          dsl.OrkflowHeader + legacySource(name),
	} {
		again := testOrkflow(t, name, "0.1.0", time.Now().UTC().Add(time.Hour))
		again.SourceYaml = source
		stored, created, err := store.Put(ctx, again, artifacts.NoHeadExpectation())
		if err != nil || created {
			t.Fatalf("header %s: created=%v, %v; want a no-op", what, created, err)
		}
		if stored.GetSourceYaml() != legacySource(name) {
			t.Fatalf("header %s: the no-op returned source %q, want the stored bytes", what, stored.GetSourceYaml())
		}
		if got := readKey(t, ctx, raw, cfg.S3Bucket, key); string(got) != string(beforeBytes) {
			t.Fatalf("header %s: the version object's bytes changed", what)
		}
		if got := versionETag(t, ctx, raw, cfg.S3Bucket, key); got != beforeTag {
			t.Fatalf("header %s: ETag %s, want %s", what, got, beforeTag)
		}
	}
}

// TestIntegration_S3OrkflowStore_AnyOtherSourceChangeIsStillRefused: every
// other change to a stored version's source is a rewrite, and a header
// written in another form is refused with the sentence that says where the
// two lines go.
func TestIntegration_S3OrkflowStore_AnyOtherSourceChangeIsStillRefused(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	legacy := testOrkflow(t, name, "0.1.0", time.Now().UTC())
	legacy.SourceYaml = legacySource(name)
	if _, _, err := store.Put(ctx, legacy, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("put: %v", err)
	}
	put := func(source string) error {
		again := testOrkflow(t, name, "0.1.0", time.Now().UTC())
		again.SourceYaml = source
		_, _, err := store.Put(ctx, again, artifacts.NoHeadExpectation())
		return err
	}
	commented := put("# a comment-only edit\n" + legacySource(name))
	if !errors.Is(commented, artifacts.ErrVersionExists) ||
		!strings.Contains(commented.Error(), "; a version is immutable, so publish a new version: ") {
		t.Fatalf("a comment-only edit = %v, want ErrVersionExists with today's advice", commented)
	}
	afterName := put(strings.Replace(legacySource(name), "version:", dsl.OrkflowHeader+"version:", 1))
	if !errors.Is(afterName, artifacts.ErrVersionExists) ||
		!strings.Contains(afterName.Error(), `its header is not the two lines "apiVersion: orkstack/v1" and "kind: Orkflow" written first (after any leading comments)`) {
		t.Fatalf("a header after name: = %v, want ErrVersionExists naming where the header goes", afterName)
	}
}

func mustAddHeader(t *testing.T, source string) string {
	t.Helper()
	added, ok := dsl.AddHeader(source)
	if !ok {
		t.Fatalf("AddHeader refused %q", source)
	}
	return added
}
