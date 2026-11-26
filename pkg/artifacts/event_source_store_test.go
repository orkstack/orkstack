package artifacts_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
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

// Every document is named, in order, and nothing else is.
func TestEventSourceStore_Names(t *testing.T) {
	ctx := context.Background()
	store, dir := newEventSourceStore(t)
	for _, key := range []eventsources.Key{{Namespace: "b", Name: "x"}, {Namespace: "a", Name: "y"}, {Namespace: "a", Name: "x"}} {
		if _, err := store.Create(ctx, storedSource(key.Namespace, key.Name, "evs-0123456789abcdef")); err != nil {
			t.Fatal(err)
		}
	}
	for _, stray := range []string{"a/Bad.json", "a/x.txt", "x.json", "a/b/c.json"} {
		path := filepath.Join(dir, artifacts.EventSourcePrefix, stray)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Names(ctx)
	if want := []eventsources.Key{{Namespace: "a", Name: "x"}, {Namespace: "a", Name: "y"}, {Namespace: "b", Name: "x"}}; err != nil || !slices.Equal(got, want) {
		t.Fatalf("%v, %v; want %v", got, err, want)
	}
}

// A document is removed only as it was read.
func TestEventSourceStore_Remove(t *testing.T) {
	ctx := context.Background()
	store, _ := newEventSourceStore(t)
	const id = "evs-0123456789abcdef"
	src, err := store.Create(ctx, storedSource("payments", "orders", id))
	if err != nil {
		t.Fatal(err)
	}
	changed, err := store.Replace(ctx, src, src.GetEtag())
	if err == nil {
		changed.Description = "d"
		changed, err = store.Replace(ctx, changed, changed.GetEtag())
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, etag := range []string{"", src.GetEtag(), "x"} {
		if err := store.Remove(ctx, "payments", "orders", etag); !errors.Is(err, eventsources.ErrChanged) {
			t.Fatalf("remove of %q: %v, want ErrChanged", etag, err)
		}
	}
	if err := store.Remove(ctx, "payments", "orders", changed.GetEtag()); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := store.Get(ctx, "payments", "orders"); !errors.Is(err, eventsources.ErrNotFound) {
		t.Fatalf("Get after it: %v", err)
	}
	if err := store.Remove(ctx, "payments", "orders", changed.GetEtag()); err != nil {
		t.Fatalf("a second: %v", err)
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

// A document that cannot be decoded is named by its id only where that can
// be read, and only where it is its key's own: a copy of another source's has
// that source's id.
func TestEventSourceStore_AnUnreadableDocumentsID(t *testing.T) {
	ctx := context.Background()
	store, dir := newEventSourceStore(t)
	path := filepath.Join(dir, artifacts.EventSourcePrefix, "payments", "broken.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	for body, id := range map[string]string{
		`{"format":9,"eventSource":{"id":"evs-0123456789abcdef","namespace":"payments","name":"broken","future":1}}`: "evs-0123456789abcdef",
		`{"format":1,"eventSource":{"id":"evs-0123456789abcdef","namespace":"payments","name":"orders"}}`:            "",
		`{"format":9,"eventSource":{"id":"evs-0123456789abcdef","namespace":"other","name":"broken","future":1}}`:    "",
		`{"format":9,"eventSource":{"id":"x","namespace":"payments","name":"broken"}}`:                               "",
		`garbage`: "",
	} {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := store.Get(ctx, "payments", "broken")
		unreadable, ok := errors.AsType[*eventsources.UnreadableError](err)
		if !ok || unreadable.ID != id || unreadable.Etag == "" {
			t.Errorf("%s: %+v, %v; want the id %q", body, unreadable, err, id)
			continue
		}
		if err := store.Remove(ctx, "payments", "broken", unreadable.Etag); err != nil {
			t.Errorf("remove of %s: %v", body, err)
		}
		if _, err := store.Get(ctx, "payments", "broken"); !errors.Is(err, eventsources.ErrNotFound) {
			t.Errorf("still there: %v", err)
		}
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

// TestEventSourceStore_ReadSpec: a spec's bytes hash to the digest that names
// them, or they are no spec; only "not found" is absence.
func TestEventSourceStore_ReadSpec(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := artifacts.NewLocalEventSourceStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const id = "evs-0123456789abcdef"
	body := []byte(`{"format":1}`)
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if _, found, err := store.ReadSpec(ctx, id, digest); found || err != nil {
		t.Fatalf("none stored: %v, %v", found, err)
	}
	if err := store.PutSpec(ctx, id, digest, body); err != nil {
		t.Fatal(err)
	}
	if got, found, err := store.ReadSpec(ctx, id, digest); err != nil || !found || string(got) != string(body) {
		t.Fatalf("%s, %v, %v", got, found, err)
	}
	if err := os.WriteFile(filepath.Join(dir, artifacts.EventSourceSpecPrefix, id, digest+".json"), []byte(`{"format":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, found, err := store.ReadSpec(ctx, id, digest); !errors.Is(err, eventsources.ErrSpecCorrupt) || found || got != nil {
		t.Fatalf("other bytes: %s, %v, %v", got, found, err)
	}
	if _, _, err := store.ReadSpec(ctx, id, "../x"); err == nil {
		t.Fatal("a digest that is not one was read")
	}
}
