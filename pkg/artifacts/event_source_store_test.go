package artifacts_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

func newEventSourceStore(t *testing.T) (*artifacts.BlobEventSourceStore, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := artifacts.NewLocalEventSourceStore(dir)
	if err != nil {
		t.Fatalf("NewLocalEventSourceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dir
}

func storedSource(ns, name, id string) *orkstackv1.EventSource {
	return &orkstackv1.EventSource{Id: id, Namespace: ns, Name: name, OrkflowName: "flow", Uses: "ghcr.io/acme/s:1", ServiceRevision: 1, JudgingRevision: 1}
}

func TestEventSourceStore_RoundTrip(t *testing.T) {
	ctx := context.Background()
	store, _ := newEventSourceStore(t)
	created, err := store.Create(ctx, storedSource("payments", "orders", "evs-0123456789abcdef"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.GetEtag() == "" || created.GetId() != "evs-0123456789abcdef" {
		t.Fatalf("created %+v", created)
	}
	if _, err := store.Create(ctx, storedSource("payments", "orders", "evs-1111111111111111")); !errors.Is(err, eventsources.ErrNameTaken) {
		t.Fatalf("a second create under the name: %v, want ErrNameTaken", err)
	}
	if _, err := store.Create(ctx, storedSource("other", "orders", "evs-1111111111111111")); err != nil {
		t.Fatalf("the same name in another namespace: %v", err)
	}

	next := created
	next.Description = "changed"
	next.Pause = &orkstackv1.EventSourcePause{Reason: "r"}
	replaced, err := store.Replace(ctx, next, created.GetEtag())
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if replaced.GetDescription() != "changed" || replaced.GetEtag() == created.GetEtag() || !replaced.GetPaused() {
		t.Fatalf("replaced %+v", replaced)
	}
	if _, err := store.Replace(ctx, next, created.GetEtag()); !errors.Is(err, eventsources.ErrChanged) {
		t.Fatalf("a replace under a stale etag: %v, want ErrChanged", err)
	}

	docs, skipped, err := store.List(ctx, "payments")
	if err != nil || len(docs) != 1 || len(skipped) != 0 {
		t.Fatalf("List = %d %d %v", len(docs), len(skipped), err)
	}
	n, err := store.Count(ctx)
	if err != nil || n != 2 {
		t.Fatalf("Count = %d, %v", n, err)
	}
	holds, err := store.NamespaceHolds(ctx, "payments")
	if err != nil || !holds {
		t.Fatalf("NamespaceHolds = %v, %v", holds, err)
	}
	if holds, _ := store.NamespaceHolds(ctx, "empty"); holds {
		t.Fatal("an empty namespace holds no source")
	}
}

func TestEventSourceStore_DeleteByID(t *testing.T) {
	ctx := context.Background()
	store, dir := newEventSourceStore(t)
	const id = "evs-0123456789abcdef"
	if _, err := store.Create(ctx, storedSource("payments", "orders", id)); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	if err := store.PutSpec(ctx, id, digest, []byte(`{"format":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSpec(ctx, id, digest, []byte(`{"format":1}`)); err != nil {
		t.Fatalf("a spec written again is success: %v", err)
	}
	if err := store.Delete(ctx, "payments", "orders", "evs-ffffffffffffffff"); !errors.Is(err, eventsources.ErrChanged) {
		t.Fatalf("delete of another id: %v, want ErrChanged", err)
	}
	if _, err := store.Get(ctx, "payments", "orders"); err != nil {
		t.Fatalf("a refused delete removed the source: %v", err)
	}
	if err := store.Delete(ctx, "payments", "orders", id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "payments", "orders"); !errors.Is(err, eventsources.ErrNotFound) {
		t.Fatalf("Get after delete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, artifacts.EventSourceSpecPrefix, id, digest+".json")); !os.IsNotExist(err) {
		t.Fatalf("the source's spec outlived it: %v", err)
	}
	if err := store.Delete(ctx, "payments", "orders", id); !errors.Is(err, eventsources.ErrNotFound) {
		t.Fatalf("a second delete: %v, want ErrNotFound", err)
	}
}

func TestEventSourceStore_ReadsStrictly(t *testing.T) {
	ctx := context.Background()
	store, dir := newEventSourceStore(t)
	write := func(name, body string) {
		path := filepath.Join(dir, artifacts.EventSourcePrefix, "payments", name+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("newer", `{"format":2,"eventSource":{"namespace":"payments","name":"newer"}}`)
	write("unknown-field", `{"format":1,"eventSource":{"namespace":"payments","name":"unknown-field","future":1}}`)
	write("moved", `{"format":1,"eventSource":{"namespace":"payments","name":"elsewhere"}}`)
	write("good", `{"format":1,"eventSource":{"namespace":"payments","name":"good","orkflowName":"f"}}`)
	for _, name := range []string{"newer", "unknown-field", "moved"} {
		var unreadable *eventsources.UnreadableError
		if _, err := store.Get(ctx, "payments", name); !errors.As(err, &unreadable) {
			t.Errorf("Get(%s) = %v, want unreadable", name, err)
		}
	}
	docs, skipped, err := store.List(ctx, "payments")
	if err != nil || len(docs) != 1 || len(skipped) != 3 {
		t.Fatalf("List = %d docs, %d skipped, %v; want 1 and 3", len(docs), len(skipped), err)
	}
}

func TestEventSourceStore_DeletesAnUnreadableDocumentByID(t *testing.T) {
	ctx := context.Background()
	store, dir := newEventSourceStore(t)
	path := filepath.Join(dir, artifacts.EventSourcePrefix, "payments", "broken.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ body, wrongID string }{
		{`{"format":9,"eventSource":{"id":"evs-0123456789abcdef","future":1}}`, "evs-ffffffffffffffff"},
		{`garbage`, ""},
	} {
		if err := os.WriteFile(path, []byte(tt.body), 0o644); err != nil {
			t.Fatal(err)
		}
		if tt.wrongID != "" {
			if err := store.Delete(ctx, "payments", "broken", tt.wrongID); !errors.Is(err, eventsources.ErrChanged) {
				t.Fatalf("an unreadable document whose id reads as another: %v, want ErrChanged", err)
			}
		}
		if err := store.Delete(ctx, "payments", "broken", "evs-0123456789abcdef"); err != nil {
			t.Fatalf("delete of an unreadable document %q: %v", tt.body, err)
		}
		if _, err := store.Get(ctx, "payments", "broken"); !errors.Is(err, eventsources.ErrNotFound) {
			t.Fatalf("still there: %v", err)
		}
	}
}

func TestEventSourceStore_ACallersIDNeverRemovesAnotherSourcesSpecs(t *testing.T) {
	ctx := context.Background()
	store, dir := newEventSourceStore(t)
	const healthy = "evs-1111111111111111"
	if _, err := store.Create(ctx, storedSource("payments", "healthy", healthy)); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("b", 64)
	if err := store.PutSpec(ctx, healthy, digest, []byte(`{"format":1}`)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, artifacts.EventSourcePrefix, "payments", "broken.json")
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "payments", "broken", healthy); err != nil {
		t.Fatalf("delete of an unreadable document: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, artifacts.EventSourceSpecPrefix, healthy, digest+".json")); err != nil {
		t.Fatalf("an id the document did not carry removed another source's spec: %v", err)
	}
}

func TestUnmarshalEventSourceRecord_RefusesTrailingTokens(t *testing.T) {
	for _, raw := range []string{`{"format":1,"eventSource":{}}}`, `{"format":1,"eventSource":{}}]`, `{"format":1,"eventSource":{}} {}`} {
		if _, err := artifacts.UnmarshalEventSourceRecord([]byte(raw)); err == nil {
			t.Errorf("%s: want refused", raw)
		}
	}
}

// FuzzEventSourceRecord: a stored document read back never panics the
// decoder, and the pause it reads is what its paused flag says.
func FuzzEventSourceRecord(f *testing.F) {
	f.Add([]byte(`{"format":1,"eventSource":{"namespace":"a","name":"b"}}`))
	f.Add([]byte(`{"format":2}`))
	f.Add([]byte(`{"format":1,"eventSource":{"pause":{"reason":"x"}},"x":1}`))
	f.Add([]byte(`[]`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		src, err := artifacts.UnmarshalEventSourceRecord(raw)
		if err != nil {
			return
		}
		if src.GetPaused() != (src.GetPause() != nil) {
			t.Fatal("paused is derived from the pause")
		}
	})
}
