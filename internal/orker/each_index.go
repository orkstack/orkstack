package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// eachIndexRangeBytes is how much of the source one ranged read asks for: a
// few requests for a large source, never one request for all of it.
const eachIndexRangeBytes = 8 << 20

// eachIndexProgressEvery is how often the indexer logs how far it has read.
const eachIndexProgressEvery = 10 * time.Second

// PhaseIndexing is the phase an IndexEachLines attempt's heartbeats name.
const PhaseIndexing = "indexing"

// eachIndexStore is what the indexer needs of the line-index store.
type eachIndexStore interface {
	CreateIfAbsent(ctx context.Context, ref artifacts.EachLinesIndexRef, body []byte, maxBytes int64) (artifacts.ArtifactRef, bool, error)
}

// IndexEachLines is the internal activity a fan-out over an artifact's lines
// expands with (AGENTS.md §5): it reads the source the producer committed,
// in ranged reads and never past the run's read cap, holds every byte to the
// committed SHA-256, splits and validates every line, and writes one
// immutable line index under the run's commit prefix, create-if-absent. It
// starts no container. Nothing is written before the whole source has been
// read: an invalid line anywhere fails the fan-out before any item starts.
//
// A refusal the source decides (an invalid line, too many items, a source
// or a batch too large) is not retried. A source whose bytes are not the
// committed ones, and an index already at the key with other bytes, are
// EACH_EXPANSION_FAILED and not retried either; a store that cannot be read
// or written is EACH_EXPANSION_FAILED and retried, within the plan's
// attempts.
func (a *Activities) IndexEachLines(ctx context.Context, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
	beat := NewHeartbeater(ctx)
	beat.Phase(PhaseIndexing)
	beat.Start(ctx)
	defer beat.Stop()
	return indexEachLines(ctx, a.dispatcher.Store, a.eachIndexes, in, a.dispatcher.logger())
}

// indexEachLines is IndexEachLines without its heartbeat loop: the part a
// unit test drives with a store of its own.
func indexEachLines(ctx context.Context, store artifacts.Store, indexes eachIndexStore, in *orkstackv1.IndexEachLinesInput, log *slog.Logger) (*orkstackv1.IndexEachLinesResult, error) {
	opts, readCap, k, err := indexOptions(in)
	if err != nil {
		// The engine built this input: a contradiction in it is a bug, and
		// retrying it would read the same input again.
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), eachlines.ReasonExpansionFailed, nil, expansionDetail(eachlines.ReasonExpansionFailed))
	}
	if indexes == nil {
		return nil, retryableExpansion("this orker has no line-index store")
	}
	label := in.GetProducer() + "/" + in.GetName()
	var read, lines atomic.Int64
	opts.Progress = func(bytesRead, nonEmpty int64) {
		read.Store(bytesRead)
		lines.Store(nonEmpty)
	}
	stopLogging := logProgress(ctx, log, in, label, &read, &lines)
	r := &rangeReader{ctx: ctx, store: store, uri: in.GetSource().GetUri(), size: opts.SourceSize}
	table, err := eachlines.Index(ctx, r, readCap, k, opts)
	r.close()
	stopLogging()
	if err != nil {
		return nil, indexError(ctx, label, err)
	}
	body := table.Encode()
	ref, created, err := indexes.CreateIfAbsent(ctx, artifacts.EachLinesIndexRef{
		Namespace:    artifacts.ResolveNamespace(in.GetNamespace()),
		RunID:        in.GetRunId(),
		Task:         in.GetTaskId(),
		SourceSHA256: in.GetSource().GetSha256(),
		Format:       opts.Format.String(),
	}, body, int64(eachlines.HeaderSize)+int64(eachlines.EntrySize)*int64(max(in.GetMaxItems(), 1)))
	if err != nil {
		if ctx.Err() != nil {
			return nil, stoppedExpansion(ctx)
		}
		return nil, retryableExpansion(fmt.Sprintf("write the line index of %s: %v", label, err))
	}
	if sum := sha256.Sum256(body); !created && ref.SHA256 != hex.EncodeToString(sum[:]) {
		msg := fmt.Sprintf("a different line index of %s is already stored at %s; an index is "+
			"deterministic, so the store holds bytes no indexer of this source wrote", label, ref.URI)
		return nil, temporal.NewNonRetryableApplicationError(msg, eachlines.ReasonExpansionFailed, nil, expansionDetail(eachlines.ReasonExpansionFailed))
	}
	log.Info("each index written", "run_id", in.GetRunId(), "task_id", in.GetTaskId(), "source", label,
		"items", table.Count, "lines", table.TotalLines, "bytes", table.SourceSize, "index", ref.URI, "created", created)
	return &orkstackv1.IndexEachLinesResult{
		Count:      int32(table.Count),
		TotalLines: int64(table.TotalLines),
		Index:      refPtr(ref),
	}, nil
}

// indexOptions checks the engine's input and turns it into Index's options.
func indexOptions(in *orkstackv1.IndexEachLinesInput) (eachlines.Options, int64, int, error) {
	src := in.GetSource()
	sum, err := hex.DecodeString(src.GetSha256())
	var digest [32]byte
	switch {
	case src == nil || src.GetUri() == "":
		return eachlines.Options{}, 0, 0, errors.New("the line index input names no source")
	case err != nil || len(sum) != len(digest) || src.GetSha256() != strings.ToLower(src.GetSha256()):
		return eachlines.Options{}, 0, 0, fmt.Errorf("the source's digest %q is not a SHA-256", src.GetSha256())
	case src.GetSizeBytes() < 0:
		return eachlines.Options{}, 0, 0, fmt.Errorf("the source's size %d is negative", src.GetSizeBytes())
	case in.GetRunId() == "" || in.GetTaskId() == "":
		// No namespace is the default one, as for a task (artifacts.ResolveNamespace).
		return eachlines.Options{}, 0, 0, errors.New("the line index input names no run or task")
	}
	copy(digest[:], sum)
	format, ok := formatOf(in.GetFormat())
	if !ok {
		return eachlines.Options{}, 0, 0, fmt.Errorf("the line format %v is not jsonl or lines", in.GetFormat())
	}
	k := int(max(in.GetBatch(), 1))
	return eachlines.Options{
		Format:         format,
		MaxItems:       int(in.GetMaxItems()),
		ValuesMaxBytes: in.GetValuesMaxBytes(),
		SliceMaxBytes:  in.GetSliceMaxBytes(),
		SourceSize:     src.GetSizeBytes(),
		SourceSHA256:   &digest,
	}, in.GetReadCap(), k, nil
}

// formatOf is the line format a proto enum names.
func formatOf(f orkstackv1.EachLineFormat) (eachlines.Format, bool) {
	switch f {
	case orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL:
		return eachlines.FormatJSONL, true
	case orkstackv1.EachLineFormat_EACH_LINE_FORMAT_LINES:
		return eachlines.FormatLines, true
	}
	return 0, false
}

// indexError is the activity's error for what Index returned.
func indexError(ctx context.Context, label string, err error) error {
	if reason := eachlines.ReasonOf(err); reason != "" {
		msg := fmt.Sprintf("%s: %v", label, err)
		return temporal.NewNonRetryableApplicationError(msg, reason, nil, expansionDetail(reason))
	}
	if ctx.Err() != nil {
		return stoppedExpansion(ctx)
	}
	return retryableExpansion(fmt.Sprintf("read %s: %v", label, err))
}

// stoppedExpansion is an index whose attempt ended before it finished: the
// run's cancel, or this Orker stopping or losing the cluster. Either way a
// retry may index again, so it is never reported as the run's own cancel; a
// cancelled run's engine ignores what its index answered.
func stoppedExpansion(ctx context.Context) error {
	return retryableExpansion(fmt.Sprintf("the line index was stopped before it finished: %v", context.Cause(ctx)))
}

// retryableExpansion is a store failure the plan's attempts retry; the last
// one's is what the fan-out fails with.
func retryableExpansion(msg string) error {
	return temporal.NewApplicationError(msg, eachlines.ReasonExpansionFailed, expansionDetail(eachlines.ReasonExpansionFailed))
}

// expansionDetail is the failure detail the engine reads a reason from.
func expansionDetail(reason string) *orkstackv1.ActivityFailureDetail {
	return &orkstackv1.ActivityFailureDetail{ReasonCode: reason, Category: CategoryEnum(CategoryFor(reason))}
}

// logProgress logs how far the indexer has read every eachIndexProgressEvery
// until the returned stop is called.
func logProgress(ctx context.Context, log *slog.Logger, in *orkstackv1.IndexEachLinesInput, label string, read, lines *atomic.Int64) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(eachIndexProgressEvery)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Info("each index progress", "run_id", in.GetRunId(), "task_id", in.GetTaskId(), "source", label,
					"bytes_read", read.Load(), "bytes", in.GetSource().GetSizeBytes(), "lines", lines.Load())
			}
		}
	}()
	return func() { close(done); <-stopped }
}

// rangeReader reads an artifact from the store in eachIndexRangeBytes ranges,
// so no one request holds the whole source, and never asks past size.
type rangeReader struct {
	ctx   context.Context
	store artifacts.Store
	uri   string
	size  int64

	off  int64
	end  int64 // the open range's end: a range that ends short is an error
	body io.ReadCloser
}

func (r *rangeReader) Read(p []byte) (int, error) {
	for {
		if r.body == nil {
			if r.off >= r.size {
				return 0, io.EOF
			}
			n := min(int64(eachIndexRangeBytes), r.size-r.off)
			body, err := r.store.GetRange(r.ctx, r.uri, r.off, n)
			if err != nil {
				return 0, fmt.Errorf("read bytes %d-%d of %s: %w", r.off, r.off+n-1, r.uri, err)
			}
			r.body, r.end = body, r.off+n
		}
		n, err := r.body.Read(p)
		r.off += int64(n)
		if errors.Is(err, io.EOF) {
			_ = r.body.Close() // a fully read range: its close says nothing new
			r.body = nil
			if r.off < r.end {
				// A store that ends a range early would otherwise be asked
				// for the same bytes forever.
				return n, fmt.Errorf("read bytes up to %d of %s: the range ended at %d: %w", r.end, r.uri, r.off, io.ErrUnexpectedEOF)
			}
			if n == 0 {
				continue
			}
			return n, nil
		}
		return n, err
	}
}

func (r *rangeReader) close() {
	if r.body != nil {
		_ = r.body.Close() // the read ended early; what it returned already decided
		r.body = nil
	}
}

// eachIndexStoreOf is s as the indexer's interface, nil for a nil store, so
// a missing store is seen as one rather than as a typed nil.
func eachIndexStoreOf(s *artifacts.EachIndexStore) eachIndexStore {
	if s == nil {
		return nil
	}
	return s
}
