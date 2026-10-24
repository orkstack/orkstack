package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// restoreCanceled is restore's answer when the operator's cancel reached the
// generation while it read its saved state: the run ends RUN_CANCELED from
// the switch snapshot, never a RUN_STATE_* code.
const restoreCanceled = "RUN_CANCELED"

// corruptStateMessage is RUN_STATE_CORRUPT's sentence, in run vocabulary.
func corruptStateMessage(what string) string {
	return fmt.Sprintf("its saved state failed verification (%s) and was not used; the run has stopped", what)
}

// unavailableStateMessage is RUN_STATE_UNAVAILABLE's sentence.
func unavailableStateMessage(timeout time.Duration, last error) string {
	cause := "none"
	if last != nil {
		cause = last.Error()
	}
	return fmt.Sprintf("its saved state could not be read from object storage for %s (last error: %s); "+
		"the run has stopped", timeout, cause)
}

// restore rebuilds this generation's state from the carry and the saved
// state it names, emitting nothing for the crossing. It returns "" when the
// run carries on, restoreCanceled when the operator's cancel arrived during
// a read, or a RUN_STATE_* code and its sentence. Every read runs on the
// connected ctx, so a cancel cancels Temporal's own retrying of it.
//
// No plan pointer is dereferenced before step 0 has checked it, and every
// carry field is read through its nil-safe getter: a malformed carry or plan
// ends the run explicitly, never panics the workflow task into a retry loop
// (a stuck run, AGENTS.md §11).
func (s *runState) restore(ctx, runCtx workflow.Context, carry *orkstackv1.RunContinuation) (code, message string) {
	// 0. Plan and carry shape.
	if s.plan.Continuation == nil || s.plan.History == nil || !s.plan.Continuation.valid() {
		return RunStateCorruptCode, corruptStateMessage("the run's plan names no valid budget to carry it with")
	}
	budget, limit := *s.plan.Continuation, *s.plan.History
	// 1. Version.
	if carry.GetVersion() != ContinuationCarryVersion {
		return RunStateUnsupportedCode, unsupportedCarryMessage(carry.GetVersion())
	}
	// 2. Consistency, before anything is read.
	namespace := s.wf.GetNamespace()
	if err := validateCarry(s.runID, namespace, carry); err != nil {
		return RunStateCorruptCode, corruptStateMessage(err.Error())
	}
	if carry.GetCreatedAt() == nil || carry.GetStartedAt() == nil {
		return RunStateCorruptCode, corruptStateMessage("it does not record when the run was created and started")
	}
	checkpoint := carry.GetCheckpoint()
	if what := copyOverCap(budget, s.plan, carry); what != "" {
		return RunStateCorruptCode, corruptStateMessage(what)
	}

	// 3. Scalars.
	s.generation = carry.GetGeneration()
	s.eventSeq = int(carry.GetEventSequence())
	s.createdAt = carry.GetCreatedAt()
	s.startedAt = carry.GetStartedAt()
	s.status = orkstackv1.Status_STATUS_RUNNING
	s.versions = map[string]workflow.Version{}
	for change, v := range carry.GetVersionDecisions() {
		s.versions[change] = workflow.Version(v)
	}
	s.segments = carry.GetSegments()
	s.genFirstSeq = carry.GetEventSequence() + 1
	if v, ok := s.versions[leanEventPathChange]; ok {
		s.leanDecided = true
		s.lean = v >= 1
	}

	// 4–5. Read the checkpoint in batches, each read retried by Temporal,
	// verifying every part as it completes and the whole object at the end.
	parts, code, message := s.readCheckpoint(ctx, budget, limit, checkpoint)
	if code != "" {
		return code, message
	}
	run, err := CheckpointRun(parts...)
	if err != nil {
		return RunStateCorruptCode, corruptStateMessage(verificationDetail(err))
	}
	// 6–7. Validate the rows, then rebuild.
	if what := s.adoptRows(ctx, runCtx, run); what != "" {
		return RunStateCorruptCode, corruptStateMessage(what)
	}
	// 8. Held fan-outs.
	s.heldFanOuts = s.anyFanOutHeld()
	return "", ""
}

// copyOverCap says how a carried generation's copy exceeds its cap — one cap
// per generation, shared by the checkpoint and a definition passed by
// reference (AGENTS.md §2.2), since this generation copies both into its
// history — or "" when it fits. Checked before anything is read.
func copyOverCap(budget ContinuationBudget, plan RunPlan, carry *orkstackv1.RunContinuation) string {
	if copied := carry.GetCheckpoint().GetSizeBytes() + plan.definitionBytes(); copied > budget.CheckpointMaxBytes {
		return fmt.Sprintf("it is %d bytes, over the %d the run carries forward", copied, budget.CheckpointMaxBytes)
	}
	return ""
}

// verificationDetail is a verification failure's short detail, without the
// subject a continuationInvalidError prefixes.
func verificationDetail(err error) string {
	var invalid *continuationInvalidError
	if errors.As(err, &invalid) {
		return invalid.detail
	}
	return err.Error()
}

// readCheckpoint reads the checkpoint's bytes in chunks of the budget's
// size, at most ChunksPerTask per workflow task with a timer yield between
// batches, so one workflow-task completion carries at most
// ChunkBytes·ChunksPerTask of markers. Each chunk is ONE local activity
// whose retries are Temporal's local-activity retry policy, bounded by the
// bootstrap's deadline; workflow code has no attempt loop, timer or backoff
// arithmetic of its own.
func (s *runState) readCheckpoint(
	ctx workflow.Context,
	budget ContinuationBudget,
	limit HistoryLimit,
	checkpoint *orkstackv1.ContinuationObject,
) ([]*orkstackv1.RunCheckpointPart, string, string) {
	var parts []*orkstackv1.RunCheckpointPart
	code, detail := s.readBootstrapObject(ctx, budget, limit, bootstrapObject{
		loc:   ContinuationLocator{URI: checkpoint.GetUri()},
		ref:   checkpoint,
		final: true,
		decode: func(index int, plain []byte) error {
			part := &orkstackv1.RunCheckpointPart{}
			if err := proto.Unmarshal(plain, part); err != nil {
				return invalidSavedState("part %d: %v", index, err)
			}
			parts = append(parts, part)
			return nil
		},
	})
	switch code {
	case "":
		return parts, "", ""
	case restoreCanceled:
		return nil, restoreCanceled, "run canceled"
	case RunStateCorruptCode:
		return nil, code, corruptStateMessage(detail)
	default:
		return nil, code, unavailableStateMessage(bootstrapTimeout(budget), s.restoreErr)
	}
}

// bootstrapObject is one object a generation's bootstrap copies into its
// history: where it is, its reference (size, whole and per-part SHA-256 and
// sizes), whether its last chunk completes the bootstrap (the checkpoint's
// does), and what to do with each verified, gunzipped part.
type bootstrapObject struct {
	loc    ContinuationLocator
	ref    *orkstackv1.ContinuationObject
	final  bool
	decode func(index int, plain []byte) error
}

// bootstrapTimeout is the bootstrap's one read deadline, from its start.
func bootstrapTimeout(budget ContinuationBudget) time.Duration {
	return time.Duration(budget.Restore.TimeoutSeconds) * time.Second
}

// bootstrapClock is the bootstrap's start and its one deadline: set by its
// first read, and shared by every later one of the same generation — a
// definition passed by reference is read first, and the checkpoint shares
// its deadline.
func (s *runState) bootstrapClock(ctx workflow.Context, budget ContinuationBudget) (started, deadline time.Time) {
	if s.bootstrapStarted.IsZero() {
		s.bootstrapStarted = workflow.Now(ctx)
		s.bootstrapDeadline = s.bootstrapStarted.Add(bootstrapTimeout(budget))
	}
	return s.bootstrapStarted, s.bootstrapDeadline
}

// readBootstrapObject copies one object into this generation's history in
// chunks of the budget's size, at most ChunksPerTask per workflow task — the
// count shared by every object of the bootstrap, so one workflow-task
// completion carries at most ChunkBytes·ChunksPerTask of markers — with a
// timer yield between batches. It verifies and decodes every part as it
// completes, then the whole object's SHA-256 and sizes. It returns "" when
// the object was read, restoreCanceled, RunStateCorruptCode with what
// failed verification, or RunStateUnavailableCode (s.restoreErr holds the
// last read's error).
func (s *runState) readBootstrapObject(
	ctx workflow.Context,
	budget ContinuationBudget,
	limit HistoryLimit,
	obj bootstrapObject,
) (code, detail string) {
	started, deadline := s.bootstrapClock(ctx, budget)
	size := obj.ref.GetSizeBytes()
	obs := ContinuationObservationInput{Namespace: s.wf.GetNamespace(), Generation: s.generation, RestoreStartedAt: started}

	whole := sha256.New()
	decoder := &partDecoder{refs: obj.ref.GetParts(), decode: obj.decode}
	for off := int64(0); off < size; {
		if s.bootstrapBatch == budget.ChunksPerTask {
			// The yield ends the workflow task, so its completion carries at
			// most one batch of markers. A cancel during it returns at once.
			_ = workflow.Sleep(ctx, continuationReadYield)
			if ctx.Err() != nil {
				return restoreCanceled, ""
			}
			s.bootstrapBatch = 0
		}
		length := min(budget.ChunkBytes, size-off)
		chunkObs := obs
		chunkObs.Final = obj.final && off+length >= size
		data, code := s.readChunkBefore(ctx, deadline, budget, limit, obj.loc, size, off, length, chunkObs)
		switch code {
		case "":
		case RunStateCorruptCode:
			return code, errorText(s.restoreErr)
		default:
			return code, ""
		}
		whole.Write(data)
		if err := decoder.feed(data); err != nil {
			return RunStateCorruptCode, verificationDetail(err)
		}
		off += length
		s.bootstrapBatch++
	}
	// Verify the whole object before any of it is used.
	if hex.EncodeToString(whole.Sum(nil)) != obj.ref.GetSha256() {
		return RunStateCorruptCode, "SHA-256 mismatch"
	}
	if decoder.decoded != len(obj.ref.GetParts()) || len(decoder.pending) != 0 {
		return RunStateCorruptCode, "its parts do not cover it"
	}
	var raw int64
	for _, ref := range obj.ref.GetParts() {
		raw += ref.GetRawBytes()
	}
	if raw != obj.ref.GetRawBytes() {
		return RunStateCorruptCode, "its parts decode to another size than its reference records"
	}
	return "", ""
}

// readChunkBefore reads one chunk of the saved state as ONE local activity,
// retried by Temporal's local-activity retry policy (the plan's restore
// policy) until deadline. The only arithmetic here is that deterministic
// deadline, handed to Temporal as ScheduleToClose: a deadline already passed
// issues no read at all. ctx.Err() after the read maps any outcome to
// restoreCanceled — a cancel is never mistaken for RUN_STATE_*.
func (s *runState) readChunkBefore(
	ctx workflow.Context,
	deadline time.Time,
	budget ContinuationBudget,
	limit HistoryLimit,
	loc ContinuationLocator,
	size, off, length int64,
	obs ContinuationObservationInput,
) ([]byte, string) {
	if ctx.Err() != nil {
		return nil, restoreCanceled
	}
	remaining := deadline.Sub(workflow.Now(ctx))
	if remaining <= 0 {
		return nil, RunStateUnavailableCode // the deadline passed between chunks
	}
	lao := workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: remaining, // Temporal stops retrying at the deadline
		StartToCloseTimeout:    min(time.Duration(budget.Restore.AttemptSeconds)*time.Second, remaining),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Duration(budget.Restore.BackoffInitialMillis) * time.Millisecond,
			BackoffCoefficient:     2,
			MaximumInterval:        time.Duration(budget.Restore.BackoffMaxSeconds) * time.Second,
			NonRetryableErrorTypes: []string{RunStateCorruptCode},
		},
	}
	var data []byte
	err := workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, lao),
		ActivityReadContinuationChunk, loc, size, off, length, obs).Get(ctx, &data)
	// The chunk's marker is part of this workflow task's commands, success
	// or failure: counted so the guard and the hold line see it.
	s.noteCommand(ctx, 1, int64(len(data))+limit.Reserve.CommandBytes)
	var applicationErr *temporal.ApplicationError
	switch {
	case ctx.Err() != nil:
		return nil, restoreCanceled // the cancel cancelled the read, whatever err says
	case err == nil:
		return data, ""
	case errors.As(err, &applicationErr) && applicationErr.Type() == RunStateCorruptCode:
		s.restoreErr = err
		return nil, RunStateCorruptCode
	default:
		s.restoreErr = err // Temporal retried until the deadline
		return nil, RunStateUnavailableCode
	}
}

func errorText(err error) string {
	if err == nil {
		return "unknown"
	}
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		return applicationErr.Message()
	}
	return err.Error()
}

// partDecoder verifies and decodes an object's parts as their bytes
// arrive: each part's SHA-256 and its gunzipped size, then decode's own
// message check. Pure CPU, no IO; at most one part (≤ 1 MiB raw unless a
// single row or task is larger) is decoded at a time.
type partDecoder struct {
	refs    []*orkstackv1.ContinuationPart
	decode  func(index int, plain []byte) error
	pending []byte
	decoded int
}

func (d *partDecoder) feed(data []byte) error {
	d.pending = append(d.pending, data...)
	for d.decoded < len(d.refs) {
		ref := d.refs[d.decoded]
		if int64(len(d.pending)) < ref.GetSizeBytes() {
			return nil
		}
		member := d.pending[:ref.GetSizeBytes()]
		if artifacts.ContinuationSHA256(member) != ref.GetSha256() {
			return invalidSavedState("part %d SHA-256 mismatch", d.decoded)
		}
		plain, err := gunzipBounded(member, ref.GetRawBytes())
		if err != nil {
			return invalidSavedState("part %d: %v", d.decoded, err)
		}
		if err := d.decode(d.decoded, plain); err != nil {
			return err
		}
		d.decoded++
		d.pending = append([]byte(nil), d.pending[ref.GetSizeBytes():]...)
	}
	if len(d.pending) > 0 {
		return invalidSavedState("it holds bytes past its last part")
	}
	return nil
}

// adoptRows validates the checkpoint's rows against this definition and
// rebuilds the run's state from them. It returns what failed, or "" when
// the rows were adopted. A switch never happens in the failing phase or
// with work in flight, so a RUNNING, FAILED or CANCELED row could only be an
// orphan, and a row like that is never adopted.
func (s *runState) adoptRows(ctx, runCtx workflow.Context, run *orkstackv1.Run) string {
	// 6. Validate.
	if run.GetId() != s.runID {
		return "it belongs to another run"
	}
	rows := run.GetTaskRuns()
	if len(rows) != len(s.order) {
		return fmt.Sprintf("it holds %d tasks, the run declares %d", len(rows), len(s.order))
	}
	for i, row := range rows {
		id := s.order[i]
		if row.GetTaskId() != id {
			return fmt.Sprintf("its task %d is %q, the run declares %q", i, row.GetTaskId(), id)
		}
		if what := s.validRestoredRow(id, row); what != "" {
			return what
		}
	}
	// Pins: every pin starts unopened, so one with a started member must
	// have every member terminal.
	for _, name := range s.pinOrder {
		p := s.pins[name]
		started, unfinished := false, false
		for i, id := range s.order {
			if s.tasks[id].GetPin() != name {
				continue
			}
			status := rows[i].GetStatus()
			if status != orkstackv1.Status_STATUS_PENDING {
				started = true
			}
			if !isTerminal(status) {
				unfinished = true
			}
		}
		if started && unfinished {
			return fmt.Sprintf("pin %q was left open", p.name)
		}
	}

	// 7. Rebuild. Every row was validated above, so nothing here can fail:
	// a refusal never leaves a recreated await timer behind it.
	for i, id := range s.order {
		row := rows[i]
		s.taskRuns[id] = row
		e := s.eaches[id]
		if row.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED && e == nil {
			s.results[id] = &orkstackv1.ExecuteTaskResult{
				TaskId:    id,
				ValuesRef: row.GetValuesRef(),
				Commit:    row.GetCommit(),
				Artifacts: row.GetArtifacts(),
			}
			s.outputs[id] = payloadMap(row.GetOutput())
		}
		if e != nil && row.GetEach().GetExpanded() && row.GetEach().GetCount() > 0 {
			e.expanded = true
			e.count = row.GetEach().GetCount()
			e.next, e.settled = dispatchedAndSettled(row)
			e.inFlight = 0
			e.limit, e.stride = eachPacing(e.spec, e.count)
		} else if e != nil && row.GetEach().GetExpanded() {
			e.expanded = true
		}
		if a := s.awaits[id]; a != nil && row.GetStatus() == orkstackv1.Status_STATUS_AWAITING {
			a.open = true
			a.timeout = time.Duration(a.spec.GetTimeoutSeconds()) * time.Second
			a.deadlineAt = row.GetAwait().GetDeadline().AsTime()
			if ctx.Err() == nil {
				// openAwait's timer recipe exactly: resolve cancels it
				// unconditionally. A deadline already passed is a ready
				// future with no command, so expire fires at once with the
				// original deadline in its message.
				timerCtx, cancel := workflow.WithCancel(runCtx)
				a.deadline = workflow.NewTimer(timerCtx, a.deadlineAt.Sub(workflow.Now(ctx)))
				a.cancelDeadline = cancel
				if a.deadlineAt.After(workflow.Now(ctx)) {
					if limit := s.historyLimit(); limit != nil {
						s.noteCommand(ctx, 1, limit.Reserve.CommandBytes)
					}
				}
			}
		}
	}
	return ""
}

// validRestoredRow checks one restored row's status against its task's kind.
func (s *runState) validRestoredRow(id string, row *orkstackv1.TaskRun) string {
	status := row.GetStatus()
	if e := s.eaches[id]; e != nil {
		switch status {
		case orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_SUCCEEDED:
		case orkstackv1.Status_STATUS_RUNNING:
			if !row.GetEach().GetExpanded() {
				return fmt.Sprintf("fan-out %q is RUNNING but not expanded", id)
			}
		default:
			return fmt.Sprintf("task %q is %s", id, statusWord(status))
		}
		if row.GetEach().GetExpanded() {
			if int32(len(row.GetItems())) != row.GetEach().GetCount() {
				return fmt.Sprintf("fan-out %q holds %d of its %d items", id, len(row.GetItems()), row.GetEach().GetCount())
			}
			dispatched := true
			for i, item := range row.GetItems() {
				if item.GetItem() != int32(i) {
					return fmt.Sprintf("fan-out %q item %d is out of order", id, i)
				}
				switch {
				case item.GetStatus() == orkstackv1.Status_STATUS_RUNNING:
					return fmt.Sprintf("fan-out %q item %d is RUNNING", id, i)
				case item.GetScheduledAt() != nil:
					if !dispatched || !isTerminal(item.GetStatus()) {
						return fmt.Sprintf("fan-out %q item %d is out of dispatch order", id, i)
					}
				default:
					dispatched = false
					if item.GetStatus() != orkstackv1.Status_STATUS_PENDING {
						return fmt.Sprintf("fan-out %q item %d never dispatched but is %s", id, i, statusWord(item.GetStatus()))
					}
				}
			}
			if next, settled := dispatchedAndSettled(row); settled != next {
				return fmt.Sprintf("fan-out %q has %d items dispatched and %d settled", id, next, settled)
			}
		}
		return ""
	}
	switch status {
	case orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_SKIPPED:
		return ""
	case orkstackv1.Status_STATUS_AWAITING:
		if s.awaits[id] != nil && row.GetAwait().GetDeadline() != nil {
			return ""
		}
	}
	return fmt.Sprintf("task %q is %s", id, statusWord(status))
}

// dispatchedAndSettled counts a restored fan-out's items dispatched (those
// with a scheduled time) and settled (terminal): its next and settled.
func dispatchedAndSettled(row *orkstackv1.TaskRun) (next, settled int32) {
	for _, item := range row.GetItems() {
		if item.GetScheduledAt() != nil {
			next++
		}
		if isTerminal(item.GetStatus()) {
			settled++
		}
	}
	return next, settled
}

// eachPacing is a fan-out's pacing from its spec and count: its window
// (max_parallel, or count) and its snapshot stride. Shared by expandEach and
// restore, so a restored fan-out paces exactly as an expanded one.
func eachPacing(spec *orkstackv1.EachSpec, count int32) (limit, stride int32) {
	limit = count
	if mp := spec.GetMaxParallel(); mp > 0 && mp < count {
		limit = mp
	}
	stride = (count + eachSnapshotsPerFanOut - 1) / eachSnapshotsPerFanOut
	if stride < 1 {
		stride = 1
	}
	return limit, stride
}
