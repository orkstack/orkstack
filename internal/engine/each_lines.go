package engine

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/internal/task"
)

// EachLinesPolicy is how a run expands its fan-outs over an artifact's lines
// (docs/DSL.md, "Fan-out over an artifact's lines"): pinned in the plan at
// submit, set only for a run tree declaring one, so every number that decides
// the index activity's command is the run's own (AGENTS.md §7.2).
type EachLinesPolicy struct {
	// IndexMinThroughputBytesPerSecond is the slowest read an index is
	// allowed: its StartToClose is ceil(read cap / this) + 60 s. A quarter of
	// the slower throughput measured on the reference host (96 MB/s).
	IndexMinThroughputBytesPerSecond int64 `json:"index_min_throughput_bytes_per_second"`
	// IndexHeartbeatSeconds is the index activity's heartbeat timeout: an
	// Orker lost while indexing is noticed after it.
	IndexHeartbeatSeconds int32 `json:"index_heartbeat_seconds"`
	// IndexMaxAttempts is the index activity's attempts: a store outage, and an
	// Orker stopped or lost while it indexes, each spend one, and the fan-out
	// fails EACH_EXPANSION_FAILED once they are spent. The index is not a
	// task, so the interruption budgets (AGENTS.md §4) do not cover it.
	IndexMaxAttempts int32 `json:"index_max_attempts"`
	// SliceMaxBytes bounds one batch's span, and so its slice file.
	SliceMaxBytes int64 `json:"slice_max_bytes"`
}

// The line policy's defaults (fanout-artifacts design §12 B-1, §15 M3).
const (
	DefaultEachIndexMinThroughput   = 24_000_000
	DefaultEachIndexHeartbeatSecs   = 30
	DefaultEachIndexMaxAttempts     = 5
	DefaultEachSliceMaxBytes        = 64 << 20
	eachIndexStartToCloseMarginSecs = 60
)

// DeclaresEachLines reports whether wf declares a fan-out over an artifact's
// lines.
func DeclaresEachLines(wf *orkstackv1.Orkflow) bool {
	return slices.ContainsFunc(wf.GetTasks(), func(t *orkstackv1.Task) bool { return dsl.IsEachLines(t.GetEach()) })
}

// DefaultEachLinesPolicy is the policy a submit pins for a run that declares
// a fan-out over an artifact's lines.
func DefaultEachLinesPolicy() *EachLinesPolicy {
	return &EachLinesPolicy{
		IndexMinThroughputBytesPerSecond: DefaultEachIndexMinThroughput,
		IndexHeartbeatSeconds:            DefaultEachIndexHeartbeatSecs,
		IndexMaxAttempts:                 DefaultEachIndexMaxAttempts,
		SliceMaxBytes:                    DefaultEachSliceMaxBytes,
	}
}

// indexStartToClose is the index activity's StartToClose for a source read
// of at most readCap bytes.
func (p *EachLinesPolicy) indexStartToClose(readCap int64) time.Duration {
	throughput := max(p.IndexMinThroughputBytesPerSecond, 1)
	seconds := (readCap+throughput-1)/throughput + eachIndexStartToCloseMarginSecs
	return time.Duration(seconds) * time.Second
}

// eachLinesBounds are one lines fan-out's limits: K (lines per item), the
// largest span of one item, and the most bytes its index reads.
func (s *runState) eachLinesBounds(spec *orkstackv1.EachSpec) (k int, sliceMax, readCap int64) {
	limits := s.plan.Limits.Proto()
	k = int(max(spec.GetBatch(), 1))
	sliceMax = eachlines.SliceMax(k, limits.GetValuesMaxBytes(), s.plan.EachLines.SliceMaxBytes)
	readCap = eachlines.ReadCap(int(spec.GetMaxItems()), sliceMax, limits.GetArtifactMaxBytes())
	return k, sliceMax, readCap
}

// isIndexing reports whether fan-out id's line index is in flight: the node
// is RUNNING, not expanded, and its in-flight entry under its own id is the
// IndexEachLines activity, never a task's container.
func (s *runState) isIndexing(id string) bool {
	e := s.eaches[id]
	return e != nil && e.indexing
}

// startEachLines begins expanding a fan-out over an artifact's lines: it
// finds the source among its producer's committed artifacts, refuses one
// larger than the fan-out can read, and schedules the IndexEachLines activity
// on an Orker of the fan-out's pool, in flight under the node's own id. The
// node is RUNNING, not expanded, until collectEachLines.
func (s *runState) startEachLines(ctx, runCtx workflow.Context, id string, inFlight map[string]workflow.Future) error {
	t := s.tasks[id]
	e := s.eaches[id]
	spec := e.spec
	art := spec.GetSource().GetArtifact()
	label := art.GetProducer() + "/" + art.GetName()
	if s.plan.EachLines == nil {
		return s.failExpansion(ctx, id, InvalidEachCode, fmt.Sprintf(
			"task %q is a fan-out over the lines of %s, and this run's plan carries no line policy", id, label))
	}
	// A producer joined with none_failed may be reached not succeeded: the
	// author's wiring, a ContractViolation naming it.
	if run := s.taskRuns[art.GetProducer()]; run != nil && run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		return s.failExpansion(ctx, id, EachOverMissingCode, fmt.Sprintf(
			"each.over_artifact %s: task %q is %s, so its artifacts do not exist — a fan-out joined with "+
				"none_failed reads the lines of a task that always runs", label, art.GetProducer(), statusWord(run.GetStatus())))
	}
	source := findArtifact(s.results[art.GetProducer()].GetArtifacts(), art.GetName())
	if source == nil {
		return s.failExpansion(ctx, id, EachOverMissingCode, fmt.Sprintf(
			"each.over_artifact %s: task %q wrote no artifact %q%s", label, art.GetProducer(), art.GetName(),
			artifactNamesSentence(s.results[art.GetProducer()].GetArtifacts())))
	}
	if source.GetSha256() == "" || source.GetUri() == "" {
		return s.failExpansion(ctx, id, EachExpansionFailedCode, fmt.Sprintf(
			"each.over_artifact %s: the artifact has no committed digest or address in this run", label))
	}
	k, sliceMax, readCap := s.eachLinesBounds(spec)
	if source.GetSizeBytes() > readCap {
		return s.failExpansion(ctx, id, eachlines.ReasonSourceTooLarge, fmt.Sprintf(
			"each.over_artifact %s: the artifact is %d bytes, over the %d bytes a fan-out of %d items of %d "+
				"lines reads; the limit counts bytes, so long runs of empty lines count too",
			label, source.GetSizeBytes(), readCap, spec.GetMaxItems(), k))
	}
	queue, err := s.queueForTask(t)
	if err != nil {
		code, message := ReasonInvalidPool, err.Error()
		if failure := categoryOf(err); failure.isApplication() {
			code, message = failure.appType(), failure.appMessage()
		}
		return s.failExpansion(ctx, id, code, message)
	}
	input := &orkstackv1.IndexEachLinesInput{
		RunId:            s.runID,
		Namespace:        s.wf.GetNamespace(),
		TaskId:           id,
		DefinitionDigest: s.wf.GetDefinitionDigest(),
		Source:           source,
		Producer:         art.GetProducer(),
		Name:             art.GetName(),
		Format:           spec.GetFormat(),
		Batch:            int32(k),
		MaxItems:         spec.GetMaxItems(),
		ValuesMaxBytes:   s.plan.Limits.Proto().GetValuesMaxBytes(),
		SliceMaxBytes:    sliceMax,
		ReadCap:          readCap,
	}
	// The history guard before anything changes, as before any dispatch: a
	// run with no room left fails as the run, not as the fan-out.
	inputBytes := int64(proto.Size(input))
	if err := s.historyExhaustedBefore(ctx, inFlight, inputBytes); err != nil {
		return err
	}
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(runCtx, 1, inputBytes+limit.Reserve.CommandBytes)
		s.chargeFanOutExpand()
		s.chargeItemDispatch() // the index is in flight until it is collected
	}
	pool := s.poolForTask(t)
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_RUNNING
	taskRun.ScheduledAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Pool = pool
	taskRun.Each = &orkstackv1.TaskEach{}
	e.indexing = true
	activityCtx := workflow.WithActivityOptions(s.dispatchContext(runCtx, id), workflow.ActivityOptions{
		TaskQueue:           queue,
		StartToCloseTimeout: s.plan.EachLines.indexStartToClose(readCap),
		HeartbeatTimeout:    time.Duration(s.plan.EachLines.IndexHeartbeatSeconds) * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: max(s.plan.EachLines.IndexMaxAttempts, 1),
		},
		Priority: s.priorityFor(runCtx),
	})
	inFlight[id] = workflow.ExecuteActivity(activityCtx, task.ActivityIndexEachLines, input)
	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id, orkstackv1.Status_STATUS_RUNNING,
		fmt.Sprintf("indexing the lines of %s (%s) on pool %s", label, humanBytes(source.GetSizeBytes()), pool))
}

// collectEachLines takes the line index's answer: the fan-out expands with
// its count, or fails with the index's reason — before any item started.
func (s *runState) collectEachLines(ctx, runCtx workflow.Context, id string, future workflow.Future, inFlight map[string]workflow.Future) error {
	e := s.eaches[id]
	e.indexing = false
	spec := e.spec
	art := spec.GetSource().GetArtifact()
	label := art.GetProducer() + "/" + art.GetName()
	result := &orkstackv1.IndexEachLinesResult{}
	if err := future.Get(ctx, result); err != nil {
		failure := categoryOf(err)
		// A cancel is the run's only when the run was cancelled; anything
		// else an index answered is a failure of the expansion.
		if failure.canceled && ctx.Err() != nil {
			return s.cancelExpansion(ctx, id, fmt.Sprintf("indexing the lines of %s was cancelled", label))
		}
		code := failure.reasonCode()
		message := failure.appMessage()
		if message == "" {
			message = err.Error()
		}
		if !eachLinesReason(code) {
			// Kept in the message: the index's own code, or what ended it.
			if code != "" {
				message = code + ": " + message
			}
			code = EachExpansionFailedCode
		}
		return s.failExpansion(ctx, id, code, fmt.Sprintf("each.over_artifact %s: %s", label, message))
	}
	k := int64(max(spec.GetBatch(), 1))
	switch idx := result.GetIndex(); {
	case idx.GetUri() == "" || idx.GetSha256() == "":
		return s.failExpansion(ctx, id, EachExpansionFailedCode, fmt.Sprintf(
			"each.over_artifact %s: the line index answered no index", label))
	case result.GetCount() < 0 || int64(result.GetCount()) != (result.GetTotalLines()+k-1)/k:
		return s.failExpansion(ctx, id, EachExpansionFailedCode, fmt.Sprintf(
			"each.over_artifact %s: the line index counted %d items of %d lines, which %d lines do not make",
			label, result.GetCount(), k, result.GetTotalLines()))
	}
	e.lines = &orkstackv1.EachLines{
		Source: findArtifact(s.results[art.GetProducer()].GetArtifacts(), art.GetName()),
		Index:  result.GetIndex(),
		Format: spec.GetFormat(),
		Batch:  spec.GetBatch(),
	}
	return s.expandCounted(ctx, runCtx, id, result.GetCount(), inFlight)
}

// cancelExpansion records a fan-out whose expansion was cancelled before it
// had items: the node CANCELED, no item, TASK_FAILED with that status.
func (s *runState) cancelExpansion(ctx workflow.Context, id, message string) error {
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_CANCELED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Error = &orkstackv1.Error{
		Code:     task.CategoryCancelled,
		Message:  message,
		Details:  fmt.Sprintf("task %q", id),
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED,
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id, orkstackv1.Status_STATUS_CANCELED, message); err != nil {
		workflow.GetLogger(ctx).Error("emit fan-out cancellation failed", "run_id", s.runID, "task_id", id, "error", err)
	}
	return temporal.NewCanceledError(message)
}

// eachLinesReason reports whether code is one the line index may end a
// fan-out with; anything else it reported is EACH_EXPANSION_FAILED.
func eachLinesReason(code string) bool {
	switch code {
	case eachlines.ReasonLineInvalid, eachlines.ReasonTooManyItems, eachlines.ReasonSourceTooLarge,
		eachlines.ReasonBatchTooLarge, eachlines.ReasonExpansionFailed:
		return true
	}
	return false
}

// findArtifact is the artifact named name, or nil.
func findArtifact(refs []*orkstackv1.ArtifactRef, name string) *orkstackv1.ArtifactRef {
	for _, ref := range refs {
		if ref.GetName() == name {
			return ref
		}
	}
	return nil
}

// artifactNamesSentence names up to ten of the artifacts a producer wrote,
// for EACH_OVER_MISSING.
func artifactNamesSentence(refs []*orkstackv1.ArtifactRef) string {
	if len(refs) == 0 {
		return "; it wrote no artifacts"
	}
	names := make([]string, 0, min(len(refs), 10))
	for _, ref := range refs[:min(len(refs), 10)] {
		names = append(names, fmt.Sprintf("%q", ref.GetName()))
	}
	more := ""
	if len(refs) > 10 {
		more = fmt.Sprintf(" and %d more", len(refs)-10)
	}
	return "; it wrote " + strings.Join(names, ", ") + more
}

// humanBytes renders n bytes as people read a size: "18.2 MiB".
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// dropSource removes a lines fan-out's source from its producer's upstream
// references (decision 4): the source is never staged, each item reads only
// its own span. The references are cloned, never edited in place.
func dropSource(refs []*orkstackv1.UpstreamRef, art *orkstackv1.EachArtifactSource) []*orkstackv1.UpstreamRef {
	if art == nil {
		return refs
	}
	out := make([]*orkstackv1.UpstreamRef, 0, len(refs))
	for _, ref := range refs {
		if ref.GetTaskId() != art.GetProducer() {
			out = append(out, ref)
			continue
		}
		kept := proto.Clone(ref).(*orkstackv1.UpstreamRef)
		kept.Artifacts = slices.DeleteFunc(kept.Artifacts, func(a *orkstackv1.ArtifactRef) bool { return a.GetName() == art.GetName() })
		out = append(out, kept)
	}
	return out
}
