//go:build integration

// Integration coverage for the S3 memo store against real MinIO
// (`make infra-up`). Skips when MinIO is unreachable unless ORK_REQUIRE_INFRA
// is set — see infraAbsent in infra_required_test.go.
package artifacts_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestS3MemoStore_PutGetListDeleteAndLastWriterWins(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	store, err := artifacts.NewS3MemoStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3MemoStore: %v", err)
	}
	ctx := context.Background()
	// A private namespace segment per run keeps the shared bucket clean and
	// the listing exact.
	namespace := "memo-it-" + uniqueToken(t)
	t.Cleanup(func() {
		client := newRawS3Client(t, cfg)
		prefix, _ := artifacts.MemoScopePrefix(artifacts.MemoScopeRef{Namespace: namespace})
		for _, key := range listKeys(t, context.Background(), client, cfg.S3Bucket, prefix) {
			_, _ = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
		}
	})
	hex := func(c byte) string { return strings.Repeat(string(c), 64) }
	memo := func(key, run string, scope orkstackv1.CacheScope) *orkstackv1.CacheMemo {
		return &orkstackv1.CacheMemo{
			SchemaVersion: artifacts.MemoSchemaVersion,
			Key:           key,
			Namespace:     namespace,
			Scope:         scope,
			OrkflowName:   "cached-pipeline",
			TaskId:        "featurize",
			Source: &orkstackv1.CacheSource{
				RunId: run, TaskId: "featurize", CommitSha256: hex('c'),
				FinishedAt: timestamppb.Now(),
			},
		}
	}
	ref := artifacts.MemoRef{
		MemoScopeRef: artifacts.MemoScopeRef{Namespace: namespace, Orkflow: "cached-pipeline", TaskID: "featurize"},
		Key:          hex('1'),
	}
	if _, err := store.GetMemo(ctx, ref); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("absent memo: err = %v, want ErrNotFound", err)
	}
	for _, c := range []byte{'3', '1', '2'} {
		if err := store.PutMemo(ctx, memo(hex(c), "ork-1-1", orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW)); err != nil {
			t.Fatalf("PutMemo: %v", err)
		}
	}
	if err := store.PutMemo(ctx, memo(hex('9'), "ork-1-1", orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE)); err != nil {
		t.Fatalf("PutMemo namespace-scoped: %v", err)
	}
	if err := store.PutMemo(ctx, memo(hex('1'), "ork-2-2", orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW)); err != nil {
		t.Fatalf("PutMemo rewrite: %v", err)
	}
	got, err := store.GetMemo(ctx, ref)
	if err != nil || got.GetSource().GetRunId() != "ork-2-2" {
		t.Fatalf("last writer: %v, %v", got.GetSource().GetRunId(), err)
	}
	// A foreign object under the prefix is skipped.
	client := newRawS3Client(t, cfg)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String("metadata/memo/" + namespace + "/orkflow/cached-pipeline/featurize/foreign.json"),
		Body:   strings.NewReader("{}"),
	}); err != nil {
		t.Fatal(err)
	}
	page1, cursor, err := store.ListMemos(ctx, ref.MemoScopeRef, "", 2)
	if err != nil || len(page1) != 2 || cursor == "" {
		t.Fatalf("page 1 = %d entries, cursor %q, %v", len(page1), cursor, err)
	}
	page2, cursor2, err := store.ListMemos(ctx, ref.MemoScopeRef, cursor, 2)
	if err != nil || len(page2) != 1 || cursor2 != "" {
		t.Fatalf("page 2 = %d entries, cursor %q, %v", len(page2), cursor2, err)
	}
	all := append(page1, page2...)
	for i, want := range []byte{'1', '2', '3'} {
		if all[i].Ref.Key != hex(want) {
			t.Fatalf("entry %d = %s", i, all[i].Ref.Key)
		}
		if time.Since(all[i].LastModified) > time.Hour || all[i].Size <= 0 {
			t.Fatalf("entry %d listing facts: %+v", i, all[i])
		}
	}
	whole, _, err := store.ListMemos(ctx, artifacts.MemoScopeRef{Namespace: namespace}, "", 100)
	if err != nil || len(whole) != 4 {
		t.Fatalf("namespace listing = %d, %v; want 4", len(whole), err)
	}
	if err := store.DeleteMemo(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteMemo(ctx, ref); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
	if _, err := store.GetMemo(ctx, ref); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	// Garbage at a memo key is undecodable.
	key, _ := artifacts.MemoKey(artifacts.MemoRef{MemoScopeRef: ref.MemoScopeRef, Key: hex('2')})
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key), Body: strings.NewReader("garbage"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetMemo(ctx, artifacts.MemoRef{MemoScopeRef: ref.MemoScopeRef, Key: hex('2')}); !errors.Is(err, artifacts.ErrMemoUndecodable) {
		t.Fatalf("garbage: err = %v", err)
	}
}
