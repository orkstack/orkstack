package orker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The namespace guard over a fan-out over an artifact's lines (docs/DEPLOY.md,
// "Trust zones: a deployment per zone"): an Orker that lists the namespaces it
// serves neither indexes the lines of a namespace it does not serve nor reads
// a line index that is not its item's own, and asks no store anything first.

// countingRanges counts every ranged read that reaches the artifact store.
type countingRanges struct {
	artifacts.Store
	calls atomic.Int32
}

func (c *countingRanges) GetRange(ctx context.Context, uri string, offset, length int64) (io.ReadCloser, error) {
	c.calls.Add(1)
	return c.Store.GetRange(ctx, uri, offset, length)
}

// countingIndexes counts every request that reaches the line-index store.
type countingIndexes struct {
	inner *artifacts.EachIndexStore
	calls atomic.Int32
}

func (c *countingIndexes) CreateIfAbsent(ctx context.Context, ref artifacts.EachLinesIndexRef, body []byte, maxBytes int64) (artifacts.ArtifactRef, bool, error) {
	c.calls.Add(1)
	return c.inner.CreateIfAbsent(ctx, ref, body, maxBytes)
}

func (c *countingIndexes) Get(ctx context.Context, uri string, maxBytes int64) ([]byte, artifacts.ArtifactRef, error) {
	c.calls.Add(1)
	if c.inner == nil {
		return nil, artifacts.ArtifactRef{}, errors.New("no line index here")
	}
	return c.inner.Get(ctx, uri, maxBytes)
}

// countedIndex runs the indexer of in on an Orker serving served (nil: every
// namespace), over rig's stores, counting what reaches each of them.
func countedIndex(t *testing.T, rig *linesRig, served []string, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, *countingRanges, *countingIndexes, error) {
	t.Helper()
	ranges := &countingRanges{Store: rig.store}
	indexes := &countingIndexes{inner: rig.indexes}
	got, err := indexEachLines(context.Background(), serving(t, served...), ranges, indexes, in, slog.New(slog.DiscardHandler), nil)
	return got, ranges, indexes, err
}

// TestIndexEachLines_RefusesANamespaceItDoesNotServe: the indexer of a
// namespace this Orker does not serve fails NAMESPACE_NOT_SERVED, never
// retried, before it reads the source or writes or reads an index.
func TestIndexEachLines_RefusesANamespaceItDoesNotServe(t *testing.T) {
	for _, tc := range []struct{ name, ns string }{
		{"another namespace", "team-b"},
		{"the default namespace when the list does not name it", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newLinesRig(t, fiveRows)
			in := rig.input(1, 10)
			in.Namespace = tc.ns
			_, ranges, indexes, err := countedIndex(t, rig, []string{"team-a"}, in)
			if err == nil {
				t.Fatal("IndexEachLines succeeded for a namespace this Orker does not serve")
			}
			if reason, retryable := applicationError(t, err); reason != ReasonNamespaceNotServed || retryable {
				t.Fatalf("error = %s (retryable %v), want %s, never retried", reason, retryable, ReasonNamespaceNotServed)
			}
			if n := ranges.calls.Load(); n != 0 {
				t.Fatalf("the source was read %d times; the guard refuses before any read", n)
			}
			if n := indexes.calls.Load(); n != 0 {
				t.Fatalf("the line-index store was asked %d times; the guard refuses before any read or write", n)
			}
		})
	}
}

// TestIndexEachLines_ServesItsOwnNamespace is the positive control: an Orker
// that lists a namespace indexes that namespace's lines, and one with no list
// indexes any.
func TestIndexEachLines_ServesItsOwnNamespace(t *testing.T) {
	for _, tc := range []struct {
		name   string
		served []string
		ns     string
	}{
		{"a namespace the list names", []string{"team-a"}, "team-a"},
		{"the default namespace when the list names it", []string{"default"}, ""},
		{"any namespace when the list is unset", nil, "team-z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newLinesRig(t, fiveRows)
			in := rig.input(1, 10)
			in.Namespace = tc.ns
			got, ranges, _, err := countedIndex(t, rig, tc.served, in)
			must.OKf(t, err, "index the lines of a namespace this Orker serves")
			if got.GetCount() != 5 || ranges.calls.Load() == 0 {
				t.Fatalf("count = %d after %d reads; want 5 items read from the source", got.GetCount(), ranges.calls.Load())
			}
		})
	}
}

// lineItemInput is item 0 of task "work" of runID in namespace ns, a fan-out
// over the lines of a source with digest sum, reading the index at indexURI.
func lineItemInput(t *testing.T, ns, runID, sum, indexURI string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	in := guardedInput(t, ns, runID)
	in.IdempotencyKey = task.ItemIdempotencyKeyFor(artifacts.ResolveNamespace(ns), runID, "work", 0)
	in.Each = &orkstackv1.EachContext{
		Index: 0, Count: 1, As: "item",
		Source: &orkstackv1.EachSource{Artifact: &orkstackv1.EachArtifactSource{Producer: "list", Name: "rows.jsonl"}},
		Lines: &orkstackv1.EachLines{
			Source: &orkstackv1.ArtifactRef{Name: "rows.jsonl", Uri: "mem://artifacts/rows", Sha256: sum, SizeBytes: 10},
			Index:  &orkstackv1.ArtifactRef{Uri: indexURI, Sha256: strings.Repeat("e", 64)},
			Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL,
		},
	}
	return in
}

func lineIndexURIOf(t *testing.T, root string, ref artifacts.EachLinesIndexRef) string {
	t.Helper()
	key, err := artifacts.EachLinesIndexKey(ref)
	must.OK(t, err)
	return root + "/" + key
}

// TestExecute_RefusesALineIndexThatIsNotItsOwn: an item of a fan-out over
// lines reads only the index its own namespace, run, task, source and format
// name under this Orker's store root. Any other — or a uri that is no line
// index at all — fails NAMESPACE_NOT_SERVED before any store is asked.
func TestExecute_RefusesALineIndexThatIsNotItsOwn(t *testing.T) {
	const run = "ork-ns-lines"
	sum := strings.Repeat("a", 64)
	own := artifacts.EachLinesIndexRef{Namespace: "team-a", RunID: run, Task: "work", SourceSHA256: sum, Format: "jsonl"}
	with := func(change func(*artifacts.EachLinesIndexRef)) string {
		ref := own
		change(&ref)
		return lineIndexURIOf(t, cacheRoot, ref)
	}
	commitKey, err := artifacts.CommitKey("team-a", run, "work")
	must.OK(t, err)
	ownKey, err := artifacts.EachLinesIndexKey(own)
	must.OK(t, err)
	for _, tc := range []struct{ name, uri string }{
		{"an index of another namespace", with(func(r *artifacts.EachLinesIndexRef) { r.Namespace = "team-b" })},
		{"an index of another run", with(func(r *artifacts.EachLinesIndexRef) { r.RunID = "ork-other" })},
		{"an index of another task", with(func(r *artifacts.EachLinesIndexRef) { r.Task = "other" })},
		{"an index of another source", with(func(r *artifacts.EachLinesIndexRef) { r.SourceSHA256 = strings.Repeat("b", 64) })},
		{"an index of another format", with(func(r *artifacts.EachLinesIndexRef) { r.Format = "lines" })},
		{"its own index under another store root", "mem://elsewhere/" + ownKey},
		{"a commit, not a line index", cacheRoot + "/" + commitKey},
		{"its own index with a query", lineIndexURIOf(t, cacheRoot, own) + "?x"},
		{"its own index with a fragment", lineIndexURIOf(t, cacheRoot, own) + "#f"},
		{"its own index spelled with an escape", strings.Replace(lineIndexURIOf(t, cacheRoot, own), "/work/", "/wor%6b/", 1)},
		{"its own index under the root with a doubled slash", cacheRoot + "//" + ownKey},
		{"a uri that does not parse", "mem://commits/%zz"},
		{"no uri", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCacheHarness(t)
			commits := &countingCommits{CommitStore: h.commits}
			indexes := &countingIndexes{}
			ranges := &countingRanges{Store: h.dispatcher.Store}
			h.dispatcher.Commits = commits
			h.dispatcher.EachIndexes = indexes
			h.dispatcher.Store = ranges
			h.dispatcher.Namespaces = serving(t, "team-a")
			h.dispatcher.CommitRoot = cacheRoot
			pulls, creates := h.runtime.counts()

			_, err := h.dispatcher.Execute(context.Background(), lineItemInput(t, "team-a", run, sum, tc.uri))

			failure := failureOf(err)
			if failure == nil || failure.Reason != ReasonNamespaceNotServed {
				t.Fatalf("Execute = %v, want %s", err, ReasonNamespaceNotServed)
			}
			if got := failure.Category(); got != CategoryContractViolation {
				t.Fatalf("category = %s, want %s", got, CategoryContractViolation)
			}
			if n := commits.calls.Load() + indexes.calls.Load() + ranges.calls.Load(); n != 0 {
				t.Fatalf("a store was asked %d times; the guard refuses before any read", n)
			}
			if p, c := h.runtime.counts(); p != pulls || c != creates {
				t.Fatalf("the refused attempt pulled (%d→%d) or created (%d→%d)", pulls, p, creates, c)
			}
		})
	}
}

// TestExecute_ALineItemOfAServedNamespaceReadsItsOwnIndex is the positive
// control: an Orker that lists the item's namespace reads the index its own
// fan-out wrote and runs the item.
func TestExecute_ALineItemOfAServedNamespaceReadsItsOwnIndex(t *testing.T) {
	h := newEachHarness(t)
	indexes, err := artifacts.NewLocalEachIndexStore(t.TempDir())
	must.OK(t, err)
	h.dispatcher.EachIndexes = indexes
	source, err := h.store.Put(context.Background(), "files.jsonl", strings.NewReader(fiveRows), artifacts.PutOptions{})
	must.OK(t, err)
	rig := &linesRig{store: h.store, indexes: indexes, source: source}
	got, err := rig.index(t, rig.input(1, 10))
	must.OK(t, err)
	key, err := artifacts.EachLinesIndexKey(artifacts.EachLinesIndexRef{
		Namespace: "default", RunID: "ork-run", Task: "convert", SourceSHA256: source.SHA256, Format: "jsonl"})
	must.OK(t, err)
	root, ok := strings.CutSuffix(got.GetIndex().GetUri(), "/"+key)
	if !ok {
		t.Fatalf("index uri %s does not end in its key %s", got.GetIndex().GetUri(), key)
	}
	h.dispatcher.Namespaces = serving(t, "default")
	h.dispatcher.CommitRoot = root

	lines := rig.itemInput(got, 2, 1).GetEach()
	in := itemInput(2, got.GetCount(), lines.GetSource())
	in.RunId = "ork-run"
	in.IdempotencyKey = task.ItemIdempotencyKeyFor("default", "ork-run", "convert", 2)
	in.Each.As = "item"
	in.Each.Lines = lines.GetLines()
	_, err = h.dispatcher.Execute(context.Background(), in)
	must.OKf(t, err, "an item of a served namespace reading its own line index")
	if item := h.runtime.task[0]["each"].(map[string]any)["item"].(map[string]any); item["n"] != float64(2) {
		t.Fatalf("each.item = %v, want row 2", item)
	}
}
