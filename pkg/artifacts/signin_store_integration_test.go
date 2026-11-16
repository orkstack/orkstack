//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestSigninStore_S3CreateReplaceDelete runs the sign-in records' rules
// against the real store: create only if absent, replace only under the
// etag read, delete idempotent, and a replace after a delete refused, so a
// refresh never brings back a sign-in that was revoked meanwhile.
func TestSigninStore_S3CreateReplaceDelete(t *testing.T) {
	cfg := privateIdentityBucket(t)
	ctx := context.Background()
	store, err := artifacts.NewS3SigninStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("ab", 32)
	etag, err := store.Create(ctx, id, []byte("v1.sealed-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, id, []byte("again")); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a second create = %v; want ErrPreconditionFailed", err)
	}
	body, got, err := store.Get(ctx, id)
	if err != nil || string(body) != "v1.sealed-1" || got != etag {
		t.Fatalf("Get = %q %q %v", body, got, err)
	}
	next, err := store.Replace(ctx, id, []byte("v1.sealed-2"), etag)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Replace(ctx, id, []byte("stale"), etag); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a replace under a stale etag = %v; want ErrPreconditionFailed", err)
	}
	ids, more, err := store.List(ctx, "")
	if err != nil || more || len(ids) != 1 || ids[0] != id {
		t.Fatalf("List = %v %v %v", ids, more, err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("deleting a record already gone = %v", err)
	}
	if _, err := store.Replace(ctx, id, []byte("after"), next); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a replace after delete = %v; want ErrPreconditionFailed", err)
	}
	if _, _, err := store.Get(ctx, id); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get after delete = %v; want ErrNotFound", err)
	}
}
