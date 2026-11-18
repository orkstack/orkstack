package orker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// linesRig is a local artifact store holding one source, and a local
// line-index store.
type linesRig struct {
	store   *artifacts.LocalStore
	indexes *artifacts.EachIndexStore
	source  artifacts.ArtifactRef
}

func newLinesRig(t *testing.T, source string) *linesRig {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := artifacts.NewLocalEachIndexStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), "files.jsonl", strings.NewReader(source), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return &linesRig{store: store, indexes: indexes, source: ref}
}

func (r *linesRig) input(k, maxItems int32) *orkstackv1.IndexEachLinesInput {
	return &orkstackv1.IndexEachLinesInput{
		RunId: "ork-run", Namespace: "default", TaskId: "convert", DefinitionDigest: "d",
		Source: refPtr(r.source), Producer: "list", Name: "files.jsonl",
		Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL, Batch: k, MaxItems: maxItems,
		ValuesMaxBytes: 1 << 10, SliceMaxBytes: 1 << 20, ReadCap: 1 << 30,
	}
}

func (r *linesRig) index(t *testing.T, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
	t.Helper()
	return indexEachLines(context.Background(), artifacts.Namespaces{}, r.store, r.indexes, in, slog.New(slog.DiscardHandler))
}

// applicationError is err's Temporal application error: its type and
// whether it may be retried.
func applicationError(t *testing.T, err error) (string, bool) {
	t.Helper()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) {
		t.Fatalf("error %v is not an application error", err)
	}
	return app.Type(), !app.NonRetryable()
}

// fiveRows has a CRLF, an empty line and no final LF.
const fiveRows = "{\"n\":0}\n{\"n\":1}\r\n\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}"

func TestIndexEachLines_Batches(t *testing.T) {
	for _, tc := range []struct {
		k          int32
		count      int32
		totalLines int64
	}{{1, 5, 5}, {2, 3, 5}, {5, 1, 5}, {7, 1, 5}} {
		rig := newLinesRig(t, fiveRows)
		got, err := rig.index(t, rig.input(tc.k, 10))
		if err != nil {
			t.Fatalf("K=%d: %v", tc.k, err)
		}
		if got.GetCount() != tc.count || got.GetTotalLines() != tc.totalLines {
			t.Fatalf("K=%d: count %d lines %d; want %d, %d", tc.k, got.GetCount(), got.GetTotalLines(), tc.count, tc.totalLines)
		}
		body, ref, err := rig.indexes.Get(context.Background(), got.GetIndex().GetUri(), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if ref.SHA256 != got.GetIndex().GetSha256() {
			t.Fatalf("K=%d: the stored index's digest %s is not the result's %s", tc.k, ref.SHA256, got.GetIndex().GetSha256())
		}
		table, err := eachlines.Decode(body)
		if err != nil || int32(table.Batch) != tc.k {
			t.Fatalf("K=%d: decode %v, batch %d", tc.k, err, table.Batch)
		}
	}
}

func TestIndexEachLines_DigestMismatch(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	in := rig.input(1, 10)
	sum := sha256.Sum256([]byte("another source"))
	in.Source.Sha256 = hex.EncodeToString(sum[:])
	_, err := rig.index(t, in)
	if typ, retry := applicationError(t, err); typ != eachlines.ReasonExpansionFailed || retry {
		t.Fatalf("a source whose bytes are not the committed ones = %s retryable=%v; want EACH_EXPANSION_FAILED, not retried", typ, retry)
	}
}

func TestIndexEachLines_RefusalsAreNotRetried(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		maxItems     int32
		reason       string
	}{
		{"invalid line", "{\"n\":0}\nnot json\n", 10, eachlines.ReasonLineInvalid},
		{"too many items", fiveRows, 4, eachlines.ReasonTooManyItems},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newLinesRig(t, tc.source)
			_, err := rig.index(t, rig.input(1, tc.maxItems))
			if typ, retry := applicationError(t, err); typ != tc.reason || retry {
				t.Fatalf("= %s retryable=%v; want %s, not retried", typ, retry, tc.reason)
			}
		})
	}
}

func TestIndexEachLines_CreateIfAbsentRetryIsIdempotent(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	first, err := rig.index(t, rig.input(2, 10))
	if err != nil {
		t.Fatal(err)
	}
	second, err := rig.index(t, rig.input(2, 10))
	if err != nil {
		t.Fatalf("a retry of the same index: %v", err)
	}
	if first.GetIndex().GetUri() != second.GetIndex().GetUri() || first.GetIndex().GetSha256() != second.GetIndex().GetSha256() {
		t.Fatalf("a retry wrote another index: %v vs %v", first.GetIndex(), second.GetIndex())
	}
}

func TestIndexEachLines_DifferentExistingIndex(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	ref := artifacts.EachLinesIndexRef{Namespace: "default", RunID: "ork-run", Task: "convert",
		SourceSHA256: rig.source.SHA256, Format: "jsonl"}
	if _, _, err := rig.indexes.CreateIfAbsent(context.Background(), ref, []byte("not this index"), 1<<20); err != nil {
		t.Fatal(err)
	}
	_, err := rig.index(t, rig.input(1, 10))
	if typ, retry := applicationError(t, err); typ != eachlines.ReasonExpansionFailed || retry {
		t.Fatalf("= %s retryable=%v; want EACH_EXPANSION_FAILED, not retried", typ, retry)
	}
}

func TestIndexEachLines_StoreErrorIsRetried(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	failing := &rangeFault{Store: rig.store, err: errors.New("store down")}
	_, err := indexEachLines(context.Background(), artifacts.Namespaces{}, failing, rig.indexes, rig.input(1, 10), slog.New(slog.DiscardHandler))
	if typ, retry := applicationError(t, err); typ != eachlines.ReasonExpansionFailed || !retry {
		t.Fatalf("= %s retryable=%v; want EACH_EXPANSION_FAILED, retried", typ, retry)
	}
}

func TestIndexEachLines_StopsOnCancel(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := indexEachLines(ctx, artifacts.Namespaces{}, rig.store, rig.indexes, rig.input(1, 10), slog.New(slog.DiscardHandler))
	// A stop is retried, never read as the run's own cancel.
	if typ, retry := applicationError(t, err); typ != eachlines.ReasonExpansionFailed || !retry {
		t.Fatalf("a stopped index = %s retryable=%v; want EACH_EXPANSION_FAILED, retried", typ, retry)
	}
}

// rangeFault is a store whose ranged reads fail, or return other bytes than
// the store holds: two faults a real store cannot be made to produce on
// demand.
type rangeFault struct {
	artifacts.Store
	err     error
	replace func([]byte) []byte
}

func (f *rangeFault) GetRange(ctx context.Context, uri string, offset, length int64) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	body, err := f.Store.GetRange(ctx, uri, offset, length)
	if err != nil || f.replace == nil {
		return body, err
	}
	b, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(f.replace(b))), nil
}

// itemInput is item i's ExecuteTaskInput for the index result got.
func (r *linesRig) itemInput(got *orkstackv1.IndexEachLinesResult, i, k int32) *orkstackv1.ExecuteTaskInput {
	return &orkstackv1.ExecuteTaskInput{
		TaskId: "convert",
		Limits: &orkstackv1.WorkspaceLimits{ValuesMaxBytes: 1 << 10},
		Each: &orkstackv1.EachContext{
			Index: i, Count: got.GetCount(), As: "item",
			Source: &orkstackv1.EachSource{Artifact: &orkstackv1.EachArtifactSource{Producer: "list", Name: "files.jsonl"}},
			Lines: &orkstackv1.EachLines{Source: refPtr(r.source), Index: got.GetIndex(),
				Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL, Batch: k},
		},
	}
}

func (r *linesRig) resolver(t *testing.T) *linesResolver {
	return &linesResolver{store: r.store, indexes: r.indexes, cache: newEachIndexCache(t.TempDir(), 1<<20)}
}

func TestExecute_LinesItemTaskJSON(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	got, err := rig.index(t, rig.input(1, 10))
	if err != nil {
		t.Fatal(err)
	}
	ws := &Workspace{Input: t.TempDir()}
	item, failure := rig.resolver(t).resolve(context.Background(), rig.itemInput(got, 2, 0), ws)
	if failure != nil {
		t.Fatal(failure)
	}
	if string(item.Value) != `{"n":2}` {
		t.Fatalf("item 2's value = %s; want {\"n\":2}", item.Value)
	}
	doc, err := json.Marshal(item.Doc)
	if err != nil {
		t.Fatal(err)
	}
	// Line 4 of the file: the empty line 3 counts toward file lines.
	size, _ := json.Marshal(rig.source.SizeBytes)
	want := `{"producer":"list","name":"files.jsonl","nameB64":"ZmlsZXMuanNvbmw","sha256":"` + rig.source.SHA256 +
		`","sizeBytes":` + string(size) + `,"format":"jsonl","line":4}`
	if string(doc) != want {
		t.Fatalf("each.lines = %s\nwant      %s", doc, want)
	}
	if entries, _ := os.ReadDir(ws.Input); len(entries) != 0 {
		t.Fatalf("an unbatched item wrote input files: %v", entries)
	}
}

func TestExecute_BatchSliceFile(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	got, err := rig.index(t, rig.input(2, 10))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		item  int32
		slice string
		first int64
		count int64
		line  int64
	}{
		{0, "{\"n\":0}\n{\"n\":1}\n", 0, 2, 1},
		{1, "{\"n\":2}\n{\"n\":3}\n", 2, 2, 4},
		{2, "{\"n\":4}\n", 4, 1, 6},
	} {
		ws := &Workspace{Input: t.TempDir()}
		item, failure := rig.resolver(t).resolve(context.Background(), rig.itemInput(got, tc.item, 2), ws)
		if failure != nil {
			t.Fatalf("item %d: %v", tc.item, failure)
		}
		b, err := os.ReadFile(filepath.Join(ws.Input, "each", "lines.jsonl"))
		if err != nil || string(b) != tc.slice {
			t.Fatalf("item %d slice = %q, %v; want %q", tc.item, b, err, tc.slice)
		}
		sum := sha256.Sum256(b)
		if item.SliceDigest != hex.EncodeToString(sum[:]) || item.Value != nil {
			t.Fatalf("item %d: digest %s value %s", tc.item, item.SliceDigest, item.Value)
		}
		d := item.Doc
		if d.Path != "/orkstack/input/each/lines.jsonl" || d.Batch != 2 || *d.First != tc.first || *d.Count != tc.count || d.Line != tc.line {
			t.Fatalf("item %d each.lines = %+v", tc.item, d)
		}
	}
}

func TestExecute_LinesFailures(t *testing.T) {
	ctx := context.Background()
	rig := newLinesRig(t, fiveRows)
	got, err := rig.index(t, rig.input(1, 10))
	if err != nil {
		t.Fatal(err)
	}
	batchedIn := rig.input(2, 10)
	batchedIn.TaskId = "convert-batched" // one task has one batch size
	batched, err := rig.index(t, batchedIn)
	if err != nil {
		t.Fatal(err)
	}
	reasonOf := func(f *Failure) string {
		if f == nil {
			return ""
		}
		return f.Reason
	}
	t.Run("span digest mismatch", func(t *testing.T) {
		r := rig.resolver(t)
		r.store = &rangeFault{Store: rig.store, replace: func(b []byte) []byte { return bytes.ToUpper(b) }}
		_, f := r.resolve(ctx, rig.itemInput(got, 1, 0), &Workspace{Input: t.TempDir()})
		if reasonOf(f) != ReasonEachItemUnavailable {
			t.Fatalf("= %v; want EACH_ITEM_UNAVAILABLE", f)
		}
	})
	t.Run("batch span digest mismatch removes the slice", func(t *testing.T) {
		r := rig.resolver(t)
		r.store = &rangeFault{Store: rig.store, replace: func(b []byte) []byte { return bytes.ToUpper(b) }}
		ws := &Workspace{Input: t.TempDir()}
		_, f := r.resolve(ctx, rig.itemInput(batched, 0, 2), ws)
		if reasonOf(f) != ReasonEachItemUnavailable {
			t.Fatalf("= %v; want EACH_ITEM_UNAVAILABLE", f)
		}
		if _, err := os.Stat(filepath.Join(ws.Input, "each", "lines.jsonl")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the slice of a mismatched span was left: %v", err)
		}
	})
	t.Run("index header mismatch", func(t *testing.T) {
		in := rig.itemInput(got, 1, 0)
		in.Each.Count = 4
		_, f := rig.resolver(t).resolve(ctx, in, &Workspace{Input: t.TempDir()})
		if reasonOf(f) != ReasonEachItemUnavailable || !strings.Contains(f.Error(), "engine counted 4") {
			t.Fatalf("= %v; want EACH_ITEM_UNAVAILABLE naming the count", f)
		}
	})
	t.Run("batch size mismatch", func(t *testing.T) {
		_, f := rig.resolver(t).resolve(ctx, rig.itemInput(got, 1, 2), &Workspace{Input: t.TempDir()})
		if reasonOf(f) != ReasonEachItemUnavailable {
			t.Fatalf("= %v; want EACH_ITEM_UNAVAILABLE", f)
		}
	})
	t.Run("index digest mismatch", func(t *testing.T) {
		in := rig.itemInput(got, 1, 0)
		in.Each.Lines.Index = cloneRef(in.Each.Lines.Index)
		sum := sha256.Sum256([]byte("x"))
		in.Each.Lines.Index.Sha256 = hex.EncodeToString(sum[:])
		_, f := rig.resolver(t).resolve(ctx, in, &Workspace{Input: t.TempDir()})
		if reasonOf(f) != ReasonEachItemUnavailable {
			t.Fatalf("= %v; want EACH_ITEM_UNAVAILABLE", f)
		}
	})
	t.Run("source deleted is EACH_ITEM_UNAVAILABLE", func(t *testing.T) {
		r := rig.resolver(t)
		r.store = &rangeFault{Store: rig.store, err: artifacts.ErrNotFound}
		_, f := r.resolve(ctx, rig.itemInput(got, 1, 0), &Workspace{Input: t.TempDir()})
		if reasonOf(f) != ReasonEachItemUnavailable || !strings.Contains(f.Error(), "gone") {
			t.Fatalf("= %v; want EACH_ITEM_UNAVAILABLE, the source gone", f)
		}
	})
	t.Run("index missing is retried until the last attempt", func(t *testing.T) {
		in := rig.itemInput(got, 1, 0)
		in.Each.Lines.Index = cloneRef(in.Each.Lines.Index)
		in.Each.Lines.Index.Uri = strings.Replace(in.Each.Lines.Index.Uri, "jsonl-", "lines-", 1)
		r := rig.resolver(t)
		r.last = func(context.Context) bool { return false }
		if _, f := r.resolve(ctx, in, &Workspace{Input: t.TempDir()}); reasonOf(f) != ReasonInputUnavailable || !RetryableReason(f.Reason) {
			t.Fatalf("an earlier attempt = %v; want a retried INPUT_ARTIFACT_UNAVAILABLE", f)
		}
		r.last = func(context.Context) bool { return true }
		if _, f := r.resolve(ctx, in, &Workspace{Input: t.TempDir()}); reasonOf(f) != ReasonEachItemUnavailable {
			t.Fatalf("the last attempt = %v; want EACH_ITEM_UNAVAILABLE", f)
		}
	})
	t.Run("the source is never staged", func(t *testing.T) {
		ws := &Workspace{Input: t.TempDir()}
		if _, f := rig.resolver(t).resolve(ctx, rig.itemInput(got, 0, 0), ws); f != nil {
			t.Fatal(f)
		}
		err := filepath.WalkDir(ws.Input, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				t.Errorf("an unbatched item staged %s", p)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func cloneRef(ref *orkstackv1.ArtifactRef) *orkstackv1.ArtifactRef {
	return proto.Clone(ref).(*orkstackv1.ArtifactRef)
}

// linesExecute runs item i (batch k) of a fan-out over fiveRows through the
// whole dispatcher.
func linesExecute(t *testing.T, k, i int32, with map[string]any) *eachHarness {
	t.Helper()
	h := newEachHarness(t)
	indexes, err := artifacts.NewLocalEachIndexStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.dispatcher.EachIndexes = indexes
	source, err := h.store.Put(context.Background(), "files.jsonl", strings.NewReader(fiveRows), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rig := &linesRig{store: h.store, indexes: indexes, source: source}
	got, err := rig.index(t, rig.input(k, 10))
	if err != nil {
		t.Fatal(err)
	}
	lines := rig.itemInput(got, i, k)
	in := itemInput(i, got.GetCount(), lines.GetEach().GetSource())
	in.Each.As = "item"
	in.Each.Lines = lines.GetEach().GetLines()
	w, _ := structpb.NewStruct(with)
	in.With = w
	if _, err := h.dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return h
}

func TestExecute_LinesItemThroughTheDispatcher(t *testing.T) {
	h := linesExecute(t, 0, 3, map[string]any{"n": "{{ .each.item.n }} {{ .each.index }}/{{ .each.count }}"})
	each := h.runtime.task[0]["each"].(map[string]any)
	if item := each["item"].(map[string]any); item["n"] != float64(3) || each["as"] != "item" {
		t.Fatalf("each = %v", each)
	}
	if lines := each["lines"].(map[string]any); lines["line"] != float64(5) || lines["format"] != "jsonl" || lines["producer"] != "list" {
		t.Fatalf("each.lines = %v", lines)
	}
	if got := h.runtime.config[0]["n"]; got != "3 3/5" {
		t.Fatalf("rendered = %v", got)
	}
	for _, path := range h.runtime.staged[0] {
		if strings.Contains(path, "files.jsonl") || strings.HasPrefix(path, "each/") {
			t.Fatalf("an unbatched item staged %s", path)
		}
	}
}

func TestExecute_BatchThroughTheDispatcher(t *testing.T) {
	h := linesExecute(t, 2, 1, map[string]any{"n": "{{ .each.index }}/{{ .each.count }}"})
	each := h.runtime.task[0]["each"].(map[string]any)
	if _, has := each["item"]; has {
		t.Fatalf("a batch's task.json has each.item: %v", each)
	}
	if _, has := each["as"]; has {
		t.Fatalf("a batch's task.json has each.as: %v", each)
	}
	lines := each["lines"].(map[string]any)
	if lines["path"] != "/orkstack/input/each/lines.jsonl" || lines["batch"] != float64(2) || lines["first"] != float64(2) || lines["count"] != float64(2) {
		t.Fatalf("each.lines = %v", lines)
	}
	if !slices.Contains(h.runtime.staged[0], "each/lines.jsonl") {
		t.Fatalf("the slice file was not in the input tree: %v", h.runtime.staged[0])
	}
	if got := h.runtime.config[0]["n"]; got != "1/3" {
		t.Fatalf("rendered = %v", got)
	}
}

// shortRange is a store whose ranges end early: the fault MEDIUM-1 guards,
// which no real store produces on demand.
type shortRange struct{ artifacts.Store }

func (s *shortRange) GetRange(ctx context.Context, uri string, offset, length int64) (io.ReadCloser, error) {
	body, err := s.Store.GetRange(ctx, uri, offset, length)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b[:len(b)/2])), nil
}

func TestIndexEachLines_ShortRangeIsAnErrorNotALoop(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	_, err := indexEachLines(context.Background(), artifacts.Namespaces{}, &shortRange{rig.store}, rig.indexes, rig.input(1, 10), slog.New(slog.DiscardHandler))
	if typ, retry := applicationError(t, err); typ != eachlines.ReasonExpansionFailed || !retry {
		t.Fatalf("a short range = %s retryable=%v; want EACH_EXPANSION_FAILED, retried", typ, retry)
	}
}

func TestIndexEachLines_UppercaseDigestIsRefused(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	in := rig.input(1, 10)
	in.Source.Sha256 = strings.ToUpper(in.Source.Sha256)
	_, err := rig.index(t, in)
	if typ, retry := applicationError(t, err); typ != eachlines.ReasonExpansionFailed || retry {
		t.Fatalf("= %s retryable=%v; want EACH_EXPANSION_FAILED, not retried", typ, retry)
	}
}

// An input that names no namespace is the default namespace's, as a task's
// is (artifacts.ResolveNamespace): the index lands where "default" puts it.
func TestIndexEachLines_NoNamespaceIsTheDefault(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	named, err := rig.index(t, rig.input(2, 10))
	if err != nil {
		t.Fatal(err)
	}
	in := rig.input(2, 10)
	in.Namespace = ""
	unnamed, err := rig.index(t, in)
	if err != nil {
		t.Fatalf("an input naming no namespace: %v", err)
	}
	if unnamed.GetIndex().GetUri() != named.GetIndex().GetUri() {
		t.Fatalf("the index landed at %s, want the default namespace's %s", unnamed.GetIndex().GetUri(), named.GetIndex().GetUri())
	}
}
