package orker

import (
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// linesKey is the memo key of a cached lines item.
func linesKey(t *testing.T, in *orkstackv1.ExecuteTaskInput, item *resolvedEachItem) string {
	t.Helper()
	k, ok := computeMemoKey(in, []byte("{}\n"), []byte("{}\n"), nil, item, nil)
	if !ok {
		t.Fatal("not keyable")
	}
	return k.Key
}

func linesKeyInput(sourceSHA string) *orkstackv1.ExecuteTaskInput {
	in, _ := pinnedKeyInput()
	in = proto.Clone(in).(*orkstackv1.ExecuteTaskInput)
	in.Each = &orkstackv1.EachContext{Index: 3, Count: 10, As: "item",
		Source: &orkstackv1.EachSource{Artifact: &orkstackv1.EachArtifactSource{Producer: "list", Name: "a.jsonl"}},
		Lines: &orkstackv1.EachLines{Source: &orkstackv1.ArtifactRef{Sha256: sourceSHA},
			Index: &orkstackv1.ArtifactRef{Sha256: sourceSHA}, Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL}}
	return in
}

// TestMemoKey_LinesComponents: an unbatched line's key is its value, the
// format, its index and count and as — never the source's digest, so a source
// with an unrelated line changed and the count unchanged still hits.
func TestMemoKey_LinesComponents(t *testing.T) {
	item := func(mutate func(*resolvedEachItem)) *resolvedEachItem {
		r := &resolvedEachItem{Bytes: stablejson.RawJSON(`{"n":3}`), As: "item", Index: 3, Count: 10, Format: "jsonl"}
		if mutate != nil {
			mutate(r)
		}
		return r
	}
	base := linesKey(t, linesKeyInput("aa"), item(nil))
	if got := linesKey(t, linesKeyInput("bb"), item(nil)); got != base {
		t.Fatal("another source digest with the same line and position changed the key")
	}
	for name, mutate := range map[string]func(*resolvedEachItem){
		"another value":  func(r *resolvedEachItem) { r.Bytes = stablejson.RawJSON(`{"n":4}`) },
		"another format": func(r *resolvedEachItem) { r.Format = "lines" },
		"another index":  func(r *resolvedEachItem) { r.Index = 4 },
		"another count":  func(r *resolvedEachItem) { r.Count = 11 },
		"another as":     func(r *resolvedEachItem) { r.As = "row" },
	} {
		if linesKey(t, linesKeyInput("aa"), item(mutate)) == base {
			t.Errorf("%s left the key unchanged", name)
		}
	}
	// An array item (no format) keys as it always did.
	array := &resolvedEachItem{Bytes: stablejson.RawJSON(`{"n":3}`), As: "item", Index: 3, Count: 10}
	if linesKey(t, linesKeyInput("aa"), array) == base {
		t.Error("an array item and a lines item with the same bytes share a key")
	}
}

// TestMemoKey_BatchComponents: a batch's key is its slice file's digest, the
// format, the batch size, its index and count.
func TestMemoKey_BatchComponents(t *testing.T) {
	item := func(mutate func(*resolvedEachItem)) *resolvedEachItem {
		r := &resolvedEachItem{Digest: "d1", Index: 3, Count: 10, Format: "jsonl", Batch: 500}
		if mutate != nil {
			mutate(r)
		}
		return r
	}
	base := linesKey(t, linesKeyInput("aa"), item(nil))
	if got := linesKey(t, linesKeyInput("bb"), item(nil)); got != base {
		t.Fatal("another source digest with the same slice changed the key")
	}
	for name, mutate := range map[string]func(*resolvedEachItem){
		"another slice":  func(r *resolvedEachItem) { r.Digest = "d2" },
		"another format": func(r *resolvedEachItem) { r.Format = "lines" },
		"another batch":  func(r *resolvedEachItem) { r.Batch = 400 },
		"another index":  func(r *resolvedEachItem) { r.Index = 4 },
		"another count":  func(r *resolvedEachItem) { r.Count = 11 },
	} {
		if linesKey(t, linesKeyInput("aa"), item(mutate)) == base {
			t.Errorf("%s left the key unchanged", name)
		}
	}
}
