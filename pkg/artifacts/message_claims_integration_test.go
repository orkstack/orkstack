//go:build integration

package artifacts_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// On a real store: of many deliveries that claim one message at once, one
// claim holds and every delivery is answered with it.
func TestIntegration_MessageClaim_OneOfManyHolds(t *testing.T) {
	_, cfg := newS3Store(t)
	ctx := context.Background()
	claims, err := artifacts.NewMessageClaimStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope := artifacts.ClaimScope{Namespace: "payments", Source: "orders", Orkflow: "settle"}
	identity := "message-" + uniqueToken(t)
	const n = 16
	got := make([]artifacts.MessageClaim, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { got[i], errs[i] = claims.Create(ctx, scope, identity, "ork-"+string(rune('a'+i))) })
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil || got[i] != got[0] {
			t.Fatalf("delivery %d: %+v, %v; want the claim that holds, %+v", i, got[i], errs[i], got[0])
		}
	}
	started := time.Now().UTC().Truncate(time.Millisecond)
	for _, at := range []time.Time{started, started.Add(time.Hour)} {
		if err := claims.MarkStarted(ctx, scope, identity, got[0].RunID, at); err != nil {
			t.Fatal(err)
		}
	}
	read, found, err := claims.Get(ctx, scope, identity)
	if err != nil || !found || read.RunID != got[0].RunID || read.Nonce != got[0].Nonce || !read.Started.Equal(started) {
		t.Fatalf("read back %+v, %v, %v", read, found, err)
	}
}

// On a real store: a copy inside it lands on the predicted artifact with its
// bytes, digest, type and provenance, repeats as one object, and refuses a
// staged part that is missing or of another size.
func TestIntegration_CopyStaged_FromStagingToARunsKey(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	staging, err := artifacts.NewEventSourceRecordStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	copier := st.(artifacts.StagedCopier)
	body := bytes.Repeat([]byte("0123456789abcdef"), 64<<10)
	sum := sha256.Sum256(body)
	part := artifacts.StagedPart{Namespace: "payments", SourceID: "evs-0123456789abcdef", Identity: "message-" + uniqueToken(t),
		Digest: hex.EncodeToString(sum[:]), Size: int64(len(body))}
	prov := testRun(t, "settle", "_inputs")
	if _, err := copier.CopyStaged(ctx, part, prov, "order", "application/octet-stream"); !errors.Is(err, artifacts.ErrStagedMissing) {
		t.Fatalf("nothing staged: %v", err)
	}
	key, err := artifacts.StagingKey(part.Namespace, part.SourceID, part.Identity, part.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := staging.PutStaged(ctx, key, body); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"order", "order body.json"} {
		want := st.(artifacts.RefPredictor).RefFor(prov, name, part.Digest, part.Size, "application/octet-stream")
		for range 2 {
			got, err := copier.CopyStaged(ctx, part, prov, name, "application/octet-stream")
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("copied to %+v, %v; want %+v", got, err, want)
			}
		}
		r, err := st.Get(ctx, want.URI)
		if err != nil {
			t.Fatal(err)
		}
		read, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil || !bytes.Equal(read, body) {
			t.Fatalf("the copy reads %d bytes, %v", len(read), err)
		}
		at, err := st.Stat(ctx, want.URI)
		if err != nil || at.SHA256 != part.Digest || at.SizeBytes != part.Size || at.Name != name ||
			at.ContentType != "application/octet-stream" || artifacts.ProvenanceFromMetadata(at.Metadata) != prov {
			t.Fatalf("stat %+v, %v", at, err)
		}
	}
	wrong := part
	wrong.Size--
	if _, err := copier.CopyStaged(ctx, wrong, prov, "order", "application/octet-stream"); !errors.Is(err, artifacts.ErrStagedMismatch) {
		t.Errorf("another size: %v", err)
	}
	if err := staging.RemoveStaged(ctx, part.Namespace, part.SourceID, part.Identity); err != nil {
		t.Fatal(err)
	}
	if _, err := copier.CopyStaged(ctx, part, prov, "order", "application/octet-stream"); !errors.Is(err, artifacts.ErrStagedMissing) {
		t.Errorf("after its removal: %v", err)
	}
}
