package orker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The producer-built line index (fanout-artifacts design §3, b′): an Orker
// asked to index an artifact it commits writes the same index the fan-out's
// own indexer would, and never fails its task for it.

// producerInput is task list writing rows.jsonl, asked to index it for
// fan-out convert.
func producerInput(edit func(*orkstackv1.LineIndexRequest)) *orkstackv1.ExecuteTaskInput {
	in := baseExportInput("list")
	in.RunId = "ork-run"
	in.IdempotencyKey = "default/ork-run/list"
	req := &orkstackv1.LineIndexRequest{TaskId: "convert", Name: "rows.jsonl", Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL,
		Batch: 1, MaxItems: 10, ValuesMaxBytes: 1 << 10, SliceMaxBytes: 1 << 20, ReadCap: 1 << 30}
	if edit != nil {
		edit(req)
	}
	in.IndexLines = []*orkstackv1.LineIndexRequest{req}
	return in
}

func producerHarness(t *testing.T, rows string) (*eachHarness, *artifacts.EachIndexStore) {
	t.Helper()
	h := newEachHarness(t)
	h.runtime.outputs = map[string]string{"rows.jsonl": rows}
	indexes, err := artifacts.NewLocalEachIndexStore(t.TempDir())
	must.OK(t, err)
	h.dispatcher.EachIndexes = indexes
	return h, indexes
}

// TestCommit_BuildsTheRequestedLineIndex: the producer's result names the
// index it built, under the fan-out's own key, byte for byte the index the
// fan-out's indexer writes over the committed artifact.
func TestCommit_BuildsTheRequestedLineIndex(t *testing.T) {
	h, indexes := producerHarness(t, fiveRows)
	got, err := h.dispatcher.Execute(context.Background(), producerInput(nil))
	must.OKf(t, err, "Execute the producer")
	if len(got.GetLineIndexes()) != 1 {
		t.Fatalf("line indexes = %v; want the one asked for", got.GetLineIndexes())
	}
	built := got.GetLineIndexes()[0]
	if built.GetTaskId() != "convert" || built.GetName() != "rows.jsonl" || built.GetCount() != 5 || built.GetTotalLines() != 5 {
		t.Fatalf("built = %v; want convert's index of rows.jsonl, 5 items of 5 lines", built)
	}
	var source *orkstackv1.ArtifactRef
	for _, a := range got.GetArtifacts() {
		if a.GetName() == "rows.jsonl" {
			source = a
		}
	}
	if source == nil {
		t.Fatalf("artifacts = %v; want rows.jsonl", got.GetArtifacts())
	}
	// The fan-out's own indexer over the committed artifact, into a store
	// of its own: the same key and the same bytes.
	own, err := artifacts.NewLocalEachIndexStore(t.TempDir())
	must.OK(t, err)
	want, err := indexEachLines(context.Background(), artifacts.Namespaces{}, h.store, own, &orkstackv1.IndexEachLinesInput{
		RunId: "ork-run", Namespace: "default", TaskId: "convert", Source: source, Producer: "list", Name: "rows.jsonl",
		Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL, Batch: 1, MaxItems: 10,
		ValuesMaxBytes: 1 << 10, SliceMaxBytes: 1 << 20, ReadCap: 1 << 30,
	}, slog.New(slog.DiscardHandler), nil)
	must.OKf(t, err, "the fan-out's own index")
	a, _, err := indexes.Get(context.Background(), built.GetIndex().GetUri(), 1<<20)
	must.OKf(t, err, "read the producer's index")
	b, _, err := own.Get(context.Background(), want.GetIndex().GetUri(), 1<<20)
	must.OKf(t, err, "read the fan-out's index")
	if !bytes.Equal(a, b) || built.GetIndex().GetSha256() != want.GetIndex().GetSha256() {
		t.Fatal("the producer's index differs from the one the fan-out's indexer writes")
	}
	if strings.TrimPrefix(built.GetIndex().GetUri(), indexRootOf(t, built.GetIndex().GetUri())) !=
		strings.TrimPrefix(want.GetIndex().GetUri(), indexRootOf(t, want.GetIndex().GetUri())) {
		t.Fatalf("keys differ: %s and %s", built.GetIndex().GetUri(), want.GetIndex().GetUri())
	}
}

// indexRootOf is the part of an index URI before its metadata/ key.
func indexRootOf(t *testing.T, uri string) string {
	t.Helper()
	i := strings.Index(uri, "metadata/commits/")
	if i < 0 {
		t.Fatalf("%s is not a line index URI", uri)
	}
	return uri[:i]
}

// TestCommit_LeavesOutAnIndexItCannotBuild: a line index the producer cannot
// build — an invalid line, a source over the read cap, an artifact it did not
// write, a request it cannot read, no index store — is left out of its
// result, and the task still succeeds: the fan-out then indexes itself.
func TestCommit_LeavesOutAnIndexItCannotBuild(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rows   string
		edit   func(*orkstackv1.LineIndexRequest)
		noLIdx bool
	}{
		{name: "an invalid line", rows: "{\"n\":0}\nnot json\n"},
		{name: "over the read cap", rows: fiveRows, edit: func(r *orkstackv1.LineIndexRequest) { r.ReadCap = 4 }},
		{name: "an artifact it did not write", rows: fiveRows, edit: func(r *orkstackv1.LineIndexRequest) { r.Name = "other.jsonl" }},
		{name: "an unknown format", rows: fiveRows, edit: func(r *orkstackv1.LineIndexRequest) { r.Format = 9 }},
		{name: "no batch bound", rows: fiveRows, edit: func(r *orkstackv1.LineIndexRequest) { r.Batch = 0; r.MaxItems = 0 }},
		{name: "a fan-out id that is no slug", rows: fiveRows, edit: func(r *orkstackv1.LineIndexRequest) { r.TaskId = "../x" }},
		{name: "too many lines", rows: fiveRows, edit: func(r *orkstackv1.LineIndexRequest) { r.MaxItems = 2 }},
		{name: "no index store", rows: fiveRows, noLIdx: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := producerHarness(t, tc.rows)
			if tc.noLIdx {
				h.dispatcher.EachIndexes = nil
			}
			got, err := h.dispatcher.Execute(context.Background(), producerInput(tc.edit))
			must.OKf(t, err, "the producer must succeed whatever its index does")
			if len(got.GetLineIndexes()) != 0 {
				t.Fatalf("line indexes = %v; want none", got.GetLineIndexes())
			}
		})
	}
}

// failingIndexes is a line-index store whose writes fail: an object-store
// outage at the producer's index write, which a real store cannot be made to
// have on demand.
type failingIndexes struct{ eachIndexReader }

func (failingIndexes) CreateIfAbsent(context.Context, artifacts.EachLinesIndexRef, []byte, int64) (artifacts.ArtifactRef, bool, error) {
	return artifacts.ArtifactRef{}, false, errors.New("the object store is unreachable")
}

// TestCommit_AnIndexStoreFaultNeverFailsTheProducer: an index write that
// fails, and a different index already at its key, leave the index out; the
// producer commits and succeeds either way.
func TestCommit_AnIndexStoreFaultNeverFailsTheProducer(t *testing.T) {
	t.Run("the write fails", func(t *testing.T) {
		h, indexes := producerHarness(t, fiveRows)
		h.dispatcher.EachIndexes = failingIndexes{indexes}
		got, err := h.dispatcher.Execute(context.Background(), producerInput(nil))
		must.OKf(t, err, "the producer must succeed whatever its index store does")
		if len(got.GetLineIndexes()) != 0 || len(got.GetArtifacts()) != 1 {
			t.Fatalf("line indexes %v, artifacts %v; want none and the committed artifact", got.GetLineIndexes(), got.GetArtifacts())
		}
	})
	t.Run("a different index at its key", func(t *testing.T) {
		h, indexes := producerHarness(t, fiveRows)
		sum := sha256.Sum256([]byte(fiveRows))
		_, created, err := indexes.CreateIfAbsent(context.Background(), artifacts.EachLinesIndexRef{
			Namespace: "default", RunID: "ork-run", Task: "convert", SourceSHA256: hex.EncodeToString(sum[:]), Format: "jsonl",
		}, []byte("ORKLIDX not the index"), 1<<20)
		if err != nil || !created {
			t.Fatalf("seed the key: created %v, %v", created, err)
		}
		got, err := h.dispatcher.Execute(context.Background(), producerInput(nil))
		must.OKf(t, err, "the producer must succeed whatever is at its index's key")
		if len(got.GetLineIndexes()) != 0 {
			t.Fatalf("line indexes = %v; want none over a different index", got.GetLineIndexes())
		}
	})
}
