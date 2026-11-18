package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.temporal.io/sdk/activity"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ContainerEachDir is where a batched item's slice file is, inside the
// container: /orkstack/input/each/lines.jsonl or lines.txt
// (docs/OCI_ACTIVITY_CONTRACT.md §3.1).
const ContainerEachDir = ContainerInput + "/each"

// eachIndexReader is what an item needs of the line-index store.
type eachIndexReader interface {
	Get(ctx context.Context, uri string, maxBytes int64) ([]byte, artifacts.ArtifactRef, error)
}

// linesItem is what one item of a fan-out over an artifact's lines resolved
// to: for an unbatched item, its value; for a batch, its slice file, written
// into the attempt's input tree and hashed as it was written.
type linesItem struct {
	// Value is an unbatched item's .each.<as> and task.json each.item.
	Value stablejson.RawJSON
	// SliceDigest is a batch's slice file's SHA-256 (lowercase hex), which
	// the memo key holds instead of its bytes; SlicePath is its container
	// path. Both are empty for an unbatched item.
	SliceDigest string
	SlicePath   string
	// Doc is task.json's each.lines.
	Doc *taskJSONEachLines
}

// taskJSONEachLines is task.json's each.lines (§3.2): where the item's line
// or lines came from. First, Count and Path are a batch's alone; Batch is
// set for a batch only.
type taskJSONEachLines struct {
	Producer  string `json:"producer"`
	Name      string `json:"name"`
	NameB64   string `json:"nameB64"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"sizeBytes"`
	Format    string `json:"format"`
	Batch     int32  `json:"batch,omitzero"`
	First     *int64 `json:"first,omitempty"`
	Count     *int64 `json:"count,omitempty"`
	Line      int64  `json:"line"`
	Path      string `json:"path,omitempty"`
}

// linesResolver resolves the items of fan-outs over an artifact's lines.
type linesResolver struct {
	store   artifacts.Store
	indexes eachIndexReader
	cache   *eachIndexCache
	// last reports whether the attempt is its policy's last; nil means
	// isLastAttempt.
	last func(context.Context) bool
}

// resolve reads item each.index of a fan-out over an artifact's lines: the
// index (through the cache, re-verified), its header held to what the engine
// recorded, the item's span by a ranged read held to the entry's digest, and
// then the line's value or, for a batch, the slice file at
// <input>/each/lines.<ext>. The source itself is never staged.
//
// A contradiction — the index, its header, a span or its line count not
// what the engine recorded — is EACH_ITEM_UNAVAILABLE and not retried, as is
// a source the store no longer holds: it was committed before the fan-out
// was expanded. An index the store does not yet show is an infrastructure
// error, retried, until the item's last attempt, where it is
// EACH_ITEM_UNAVAILABLE: a store with several filers may show a new object
// late. Any other store error is retried.
func (r *linesResolver) resolve(ctx context.Context, in *orkstackv1.ExecuteTaskInput, ws *Workspace) (*linesItem, *Failure) {
	each := in.GetEach()
	lines := each.GetLines()
	art := each.GetSource().GetArtifact()
	label := art.GetProducer() + "/" + art.GetName()
	unavailable := func(format string, args ...any) *Failure {
		return Fail(ReasonEachItemUnavailable, "task %q item %d of the lines of %s: %s",
			in.GetTaskId(), each.GetIndex(), label, fmt.Sprintf(format, args...))
	}
	format, ok := formatOf(lines.GetFormat())
	if !ok {
		return nil, unavailable("the line format %v is not jsonl or lines", lines.GetFormat())
	}
	k := max(lines.GetBatch(), 1)
	vmax := in.GetLimits().GetValuesMaxBytes()
	if vmax <= 0 {
		return nil, unavailable("the attempt carries no values_max_bytes")
	}
	if r.indexes == nil {
		return nil, unavailable("this orker has no line-index store")
	}

	table, failure := r.table(ctx, in, label)
	if failure != nil {
		return nil, failure
	}
	source := lines.GetSource()
	switch {
	case hex.EncodeToString(table.SourceSHA256[:]) != source.GetSha256() || int64(table.SourceSize) != source.GetSizeBytes():
		return nil, unavailable("the index is of another source (sha256 %x, %d bytes)", table.SourceSHA256, table.SourceSize)
	case table.Format != format:
		return nil, unavailable("the index is of format %s, the fan-out's is %s", table.Format, format)
	case int64(table.Batch) != int64(k):
		return nil, unavailable("the index has batches of %d lines, the fan-out's are of %d", table.Batch, k)
	case int64(table.Count) != int64(each.GetCount()):
		return nil, unavailable("the index holds %d items, the engine counted %d", table.Count, each.GetCount())
	case each.GetIndex() < 0 || each.GetIndex() >= each.GetCount():
		return nil, unavailable("item %d is outside the %d items", each.GetIndex(), each.GetCount())
	}
	if err := table.CheckLimits(vmax); err != nil {
		return nil, unavailable("%v", err)
	}
	entry := table.Entries[each.GetIndex()]

	doc := &taskJSONEachLines{
		Producer:  art.GetProducer(),
		Name:      art.GetName(),
		NameB64:   EncodeArtifactName(art.GetName()),
		SHA256:    source.GetSha256(),
		SizeBytes: source.GetSizeBytes(),
		Format:    format.String(),
		Line:      int64(entry.FirstLine),
	}
	span, err := r.store.GetRange(ctx, source.GetUri(), int64(entry.Offset), int64(entry.Length))
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			return nil, unavailable("the source artifact is gone from the store: %v", err)
		}
		return nil, FailCause(ReasonInputUnavailable, err, "task %q item %d: read its span of %s: %v",
			in.GetTaskId(), each.GetIndex(), label, err)
	}
	defer func() { _ = span.Close() }() // a read-only body: its close decides nothing

	if k == 1 {
		raw, err := io.ReadAll(io.LimitReader(span, int64(entry.Length)+1))
		if err != nil {
			return nil, FailCause(ReasonInputUnavailable, err, "task %q item %d: read its line of %s: %v",
				in.GetTaskId(), each.GetIndex(), label, err)
		}
		if sum := sha256.Sum256(raw); sum != entry.SHA256 {
			return nil, unavailable("its line's bytes do not match the index")
		}
		value, err := eachlines.Value(raw, format)
		if err != nil {
			return nil, unavailable("its line no longer parses: %v", err)
		}
		if int64(len(value)) > vmax {
			return nil, unavailable("its value is %d bytes, over values_max_bytes %d", len(value), vmax)
		}
		return &linesItem{Value: stablejson.RawJSON(value), Doc: doc}, nil
	}

	name := "lines.txt"
	if format == eachlines.FormatJSONL {
		name = "lines.jsonl"
	}
	dir := filepath.Join(ws.Input, "each")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "task %q item %d: create %s: %v", in.GetTaskId(), each.GetIndex(), ContainerEachDir, err)
	}
	host := filepath.Join(dir, name)
	file, err := os.OpenFile(host, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "task %q item %d: create its slice file: %v", in.GetTaskId(), each.GetIndex(), err)
	}
	spanSum, sliceSum := sha256.New(), sha256.New()
	n, serr := eachlines.SliceTo(io.MultiWriter(file, sliceSum), io.TeeReader(io.LimitReader(span, int64(entry.Length)), spanSum), int(k))
	cerr := file.Close()
	discard := func() { _ = os.Remove(host) } // the attempt fails; its workspace goes with it
	switch {
	case serr != nil && eachlines.ReasonOf(serr) == "":
		discard()
		return nil, FailCause(ReasonInputUnavailable, serr, "task %q item %d: read its span of %s: %v",
			in.GetTaskId(), each.GetIndex(), label, serr)
	case serr != nil:
		discard()
		return nil, unavailable("%v", serr)
	case cerr != nil:
		discard()
		return nil, FailCause(ReasonWorkdirFailed, cerr, "task %q item %d: write its slice file: %v", in.GetTaskId(), each.GetIndex(), cerr)
	case string(spanSum.Sum(nil)) != string(entry.SHA256[:]):
		discard()
		return nil, unavailable("its span's bytes do not match the index")
	case int64(n) != int64(entry.Lines):
		discard()
		return nil, unavailable("its span holds %d lines, the index says %d", n, entry.Lines)
	}
	first := int64(each.GetIndex()) * int64(k)
	count := int64(entry.Lines)
	doc.Batch, doc.First, doc.Count = k, &first, &count
	doc.Path = ContainerEachDir + "/" + name
	return &linesItem{SliceDigest: hex.EncodeToString(sliceSum.Sum(nil)), SlicePath: doc.Path, Doc: doc}, nil
}

// table reads and decodes the item's line index.
func (r *linesResolver) table(ctx context.Context, in *orkstackv1.ExecuteTaskInput, label string) (*eachlines.Table, *Failure) {
	each := in.GetEach()
	ref := each.GetLines().GetIndex()
	// The largest index any fan-out writes: one whose count disagrees with
	// the engine's is then read and refused by its header, not by its size.
	maxBytes := int64(eachlines.HeaderSize) + int64(eachlines.EntrySize)*int64(dsl.MaxEachItems)
	body, err := r.cache.load(ctx, ref.GetSha256(), func(ctx context.Context) ([]byte, error) {
		body, _, err := r.indexes.Get(ctx, ref.GetUri(), maxBytes)
		return body, err
	})
	switch {
	case err == nil:
	case errors.Is(err, artifacts.ErrNotFound) && !r.isLast(ctx):
		return nil, FailCause(ReasonInputUnavailable, err,
			"task %q item %d: the line index of %s is not yet readable (%v); retried", in.GetTaskId(), each.GetIndex(), label, err)
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, Fail(ReasonEachItemUnavailable, "task %q item %d: the line index of %s is missing from the store: %v",
			in.GetTaskId(), each.GetIndex(), label, err)
	case errors.Is(err, errEachIndexDigest), errors.Is(err, artifacts.ErrInvalidURI):
		return nil, Fail(ReasonEachItemUnavailable, "task %q item %d: the line index of %s at %s: %v",
			in.GetTaskId(), each.GetIndex(), label, ref.GetUri(), err)
	default:
		return nil, FailCause(ReasonInputUnavailable, err, "task %q item %d: read the line index of %s: %v",
			in.GetTaskId(), each.GetIndex(), label, err)
	}
	table, err := eachlines.Decode(body)
	if err != nil {
		return nil, Fail(ReasonEachItemUnavailable, "task %q item %d: the line index of %s: %v",
			in.GetTaskId(), each.GetIndex(), label, err)
	}
	return table, nil
}

func (r *linesResolver) isLast(ctx context.Context) bool {
	if r.last != nil {
		return r.last(ctx)
	}
	return isLastAttempt(ctx)
}

// eachIndexLateAttempts bounds how many attempts of one item a missing line
// index is taken to be the store showing a new object late.
const eachIndexLateAttempts = 5

// isLastAttempt reports whether this activity attempt is its retry policy's
// last: the one a missing index is reported EACH_ITEM_UNAVAILABLE on rather
// than retried. Outside an activity (orkctl task run) every attempt is.
func isLastAttempt(ctx context.Context) bool {
	if !activity.IsActivity(ctx) {
		return true
	}
	info := activity.GetInfo(ctx)
	// Never unbounded: a policy without a maximum tolerates a late index
	// for eachIndexLateAttempts attempts, then the index is missing.
	limit := int32(eachIndexLateAttempts)
	if info.RetryPolicy != nil && info.RetryPolicy.MaximumAttempts > 0 {
		limit = min(limit, info.RetryPolicy.MaximumAttempts)
	}
	return info.Attempt >= limit
}

// eachItemOf resolves this attempt's fan-out item: an array's element, or
// an artifact's line or batch. It returns the item's value for .each and
// task.json (nil for a batch), what its memo key holds, and task.json's
// each.lines (nil for an array's item).
func (d *OCIDispatcher) eachItemOf(ctx context.Context, in *orkstackv1.ExecuteTaskInput, ws *Workspace) (stablejson.RawJSON, *resolvedEachItem, *taskJSONEachLines, *Failure) {
	each := in.GetEach()
	if each.GetLines() == nil {
		value, failure := d.resolveEachItem(in, ws)
		if failure != nil {
			return nil, nil, nil, failure
		}
		return value, &resolvedEachItem{Bytes: value, As: each.GetAs(), Index: each.GetIndex(), Count: each.GetCount()}, nil, nil
	}
	r := &linesResolver{store: d.Store, indexes: d.EachIndexes, cache: d.eachIndexCache()}
	item, failure := r.resolve(ctx, in, ws)
	if failure != nil {
		return nil, nil, nil, failure
	}
	resolved := &resolvedEachItem{Index: each.GetIndex(), Count: each.GetCount(), Format: item.Doc.Format}
	if item.SliceDigest != "" {
		resolved.Batch, resolved.Digest = item.Doc.Batch, item.SliceDigest
	} else {
		resolved.Bytes, resolved.As = item.Value, each.GetAs()
	}
	return item.Value, resolved, item.Doc, nil
}

// eachIndexCache is the process's line-index cache, under this identity's
// part of ork-stage.
func (d *OCIDispatcher) eachIndexCache() *eachIndexCache {
	d.indexCacheOnce.Do(func() {
		root := d.WorkRoot
		if root == "" {
			root = os.TempDir()
		}
		d.indexCache = newEachIndexCache(filepath.Join(d.scratchDir(root, StageDirName), eachIndexCacheDir), eachIndexCacheMaxBytes)
	})
	return d.indexCache
}

// eachIndexReaderOf is s as the resolver's interface, nil for a nil store.
func eachIndexReaderOf(s *artifacts.EachIndexStore) eachIndexReader {
	if s == nil {
		return nil
	}
	return s
}
