package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"time"

	"go.temporal.io/sdk/activity"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// producedArtifact is one artifact a committing attempt has hashed and
// uploaded: its committed reference and a way to read its bytes again from
// the attempt's own workspace.
type producedArtifact struct {
	ref  *orkstackv1.ArtifactRef
	open func() (*io.SectionReader, *Failure)
}

// buildLineIndexes builds the line indexes the engine asked this attempt for
// (ExecuteTaskInput.index_lines; fanout-artifacts design §3, b′): for each
// fan-out over one of its artifacts, the index that fan-out's own indexer
// would write — the same key, the same limits, the same bytes — read from the
// workspace file it has just committed rather than from the store. An index
// it cannot build for any reason is left out, and never fails the task: the
// fan-out then indexes the lines itself and reports what it finds.
func (d *OCIDispatcher) buildLineIndexes(ctx context.Context, in *orkstackv1.ExecuteTaskInput, produced []producedArtifact) []*orkstackv1.LineIndexBuilt {
	if len(in.GetIndexLines()) == 0 {
		return nil
	}
	writer, ok := d.EachIndexes.(eachIndexStore)
	if !ok {
		return nil
	}
	// A quarter of what is left of the attempt, at most: an index never
	// costs the task its own deadline, and one not built in time is left out.
	if activity.IsActivity(ctx) {
		if left := time.Until(activity.GetInfo(ctx).Deadline); left > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, left/4)
			defer cancel()
		}
	}
	var out []*orkstackv1.LineIndexBuilt
	for _, req := range in.GetIndexLines() {
		built, why, fault := d.buildLineIndex(ctx, in, req, produced, writer)
		if built == nil {
			log := d.logger().Info
			if fault {
				// The store, not the artifact: worth an operator's eye.
				log = d.logger().Warn
			}
			log("each index not built by its producer; the fan-out indexes it",
				"run_id", in.GetRunId(), "task_id", in.GetTaskId(), "fan_out", req.GetTaskId(), "artifact", req.GetName(), "why", why)
			continue
		}
		out = append(out, built)
	}
	return out
}

func (d *OCIDispatcher) buildLineIndex(ctx context.Context, in *orkstackv1.ExecuteTaskInput, req *orkstackv1.LineIndexRequest,
	produced []producedArtifact, writer eachIndexStore,
) (built *orkstackv1.LineIndexBuilt, why string, fault bool) {
	format, ok := formatOf(req.GetFormat())
	switch {
	case !ok:
		return nil, "the request names no line format", false
	case dsl.ValidSlug("task", req.GetTaskId()) != nil:
		return nil, "the request names no fan-out task", false
	case req.GetBatch() < 1 || req.GetBatch() > eachlines.MaxBatch || req.GetMaxItems() < 1 ||
		req.GetValuesMaxBytes() < 1 || req.GetSliceMaxBytes() < 1 || req.GetReadCap() < 1:
		return nil, "the request's limits are out of range", false
	}
	var source *producedArtifact
	for i := range produced {
		if produced[i].ref.GetName() == req.GetName() {
			source = &produced[i]
		}
	}
	if source == nil {
		return nil, "the task wrote no such artifact", false
	}
	ref := source.ref
	if ref.GetSizeBytes() > req.GetReadCap() {
		return nil, "the artifact is over the fan-out's read cap", false
	}
	sum, err := hex.DecodeString(ref.GetSha256())
	if err != nil || len(sum) != sha256.Size {
		return nil, "the artifact has no SHA-256", false
	}
	var digest [sha256.Size]byte
	copy(digest[:], sum)
	body, failure := source.open()
	if failure != nil {
		return nil, failure.Error(), true
	}
	table, err := eachlines.Index(ctx, body, req.GetReadCap(), int(req.GetBatch()), eachlines.Options{
		Format:         format,
		MaxItems:       int(req.GetMaxItems()),
		ValuesMaxBytes: req.GetValuesMaxBytes(),
		SliceMaxBytes:  req.GetSliceMaxBytes(),
		SourceSize:     ref.GetSizeBytes(),
		SourceSHA256:   &digest,
	})
	if err != nil {
		return nil, err.Error(), ctx.Err() != nil
	}
	index := table.Encode()
	stored, created, err := writer.CreateIfAbsent(ctx, artifacts.EachLinesIndexRef{
		Namespace:    artifacts.ResolveNamespace(in.GetNamespace()),
		RunID:        in.GetRunId(),
		Task:         req.GetTaskId(),
		SourceSHA256: ref.GetSha256(),
		Format:       format.String(),
	}, index, int64(eachlines.HeaderSize)+int64(eachlines.EntrySize)*int64(req.GetMaxItems()))
	if err != nil {
		return nil, "write the index: " + err.Error(), true
	}
	if want := sha256.Sum256(index); !created && stored.SHA256 != hex.EncodeToString(want[:]) {
		return nil, "a different index is already stored at its key", true
	}
	return &orkstackv1.LineIndexBuilt{
		TaskId:     req.GetTaskId(),
		Name:       req.GetName(),
		Index:      refPtr(stored),
		Count:      int32(table.Count),
		TotalLines: int64(table.TotalLines),
	}, "", false
}
