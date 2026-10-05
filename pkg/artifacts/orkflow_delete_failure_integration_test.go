//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestS3OrkflowStore_DeleteFailureKeepsPointersConsistentAndRetryRepairs
// exercises the storage boundary rather than a production test hook. The
// proxy refuses the second DELETE request once, reproducing a temporary object
// store failure after Delete has already changed one object.
func TestS3OrkflowStore_DeleteFailureKeepsPointersConsistentAndRetryRepairs(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	wf := testOrkflow(t, name, "1.0.0", time.Now().UTC().Truncate(time.Millisecond))
	if _, _, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := store.SetCurrentVersion(ctx, "", name, "1.0.0", artifacts.NoCurrentExpectation()); err != nil {
		t.Fatalf("SetCurrentVersion: %v", err)
	}

	failedCfg := failSecondDeleteOnce(t, cfg)
	failing, err := artifacts.NewS3OrkflowStore(ctx, failedCfg)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore through failure proxy: %v", err)
	}
	if err := failing.Delete(ctx, "", name); err == nil {
		t.Fatal("Delete through failure proxy = nil, want the injected object-store failure")
	}

	catalog, err := store.Versions(ctx, "", name)
	if err != nil {
		t.Fatalf("Versions after failed Delete: %v", err)
	}
	if len(catalog.All) == 0 && catalog.Current != "" {
		t.Fatalf("Versions after failed Delete = All:[] Current:%q; a pointer names no stored version", catalog.Current)
	}

	if err := store.Delete(ctx, "", name); err != nil {
		t.Fatalf("retry Delete after temporary failure = %v, want success", err)
	}
	if keys := sortedKeys(t, ctx, raw, cfg.S3Bucket, prefix); len(keys) != 0 {
		t.Fatalf("objects after retry = %v, want none", keys)
	}
	if err := store.Delete(ctx, "", name); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Delete genuinely unknown orkflow = %v, want ErrNotFound", err)
	}
}

// TestS3OrkflowStore_DeleteRepairsOrphanedPointersFromEarlierPartialDelete
// pins the recovery distinction: pointers under a name mean there is partial
// deletion state to clean, while a prefix with neither versions nor pointers
// is a genuinely unknown orkflow and remains ErrNotFound.
func TestS3OrkflowStore_DeleteRepairsOrphanedPointersFromEarlierPartialDelete(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	wf := testOrkflow(t, name, "1.0.0", time.Now().UTC().Truncate(time.Millisecond))
	if _, _, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := store.SetCurrentVersion(ctx, "", name, "1.0.0", artifacts.NoCurrentExpectation()); err != nil {
		t.Fatalf("SetCurrentVersion: %v", err)
	}
	versionKey, err := artifacts.OrkflowKey("", name, "1.0.0")
	if err != nil {
		t.Fatalf("OrkflowKey: %v", err)
	}
	if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(versionKey),
	}); err != nil {
		t.Fatalf("create pointers-only partial state: %v", err)
	}

	before, err := store.Versions(ctx, "", name)
	if err != nil {
		t.Fatalf("Versions before repair: %v", err)
	}
	if len(before.All) != 0 || before.Current != "1.0.0" {
		t.Fatalf("partial state = All:%v Current:%q, want no versions and orphaned current pointer",
			before.All, before.Current)
	}
	if err := store.Delete(ctx, "", name); err != nil {
		t.Fatalf("Delete pointers-only partial state = %v, want repair success", err)
	}
	if keys := sortedKeys(t, ctx, raw, cfg.S3Bucket, prefix); len(keys) != 0 {
		t.Fatalf("objects after repair = %v, want none", keys)
	}
	if err := store.Delete(ctx, "", name); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Delete after repaired prefix = %v, want ErrNotFound", err)
	}
}

// failSecondDeleteOnce passes requests to MinIO except for the second DELETE,
// which receives a non-retryable AccessDenied response. It preserves Host so
// the SigV4 signature computed for MinIO remains valid.
func failSecondDeleteOnce(t *testing.T, cfg artifacts.Config) artifacts.Config {
	t.Helper()
	target, err := url.Parse(cfg.S3Endpoint)
	if err != nil {
		t.Fatalf("parse %q: %v", cfg.S3Endpoint, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	deletes := 0
	failed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Method == http.MethodDelete {
			deletes++
			if deletes == 2 && !failed {
				failed = true
				mu.Unlock()
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>injected temporary delete failure</Message></Error>`))
				return
			}
		}
		mu.Unlock()
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	cfg.S3Endpoint = strings.TrimSuffix(server.URL, "/")
	return cfg
}
