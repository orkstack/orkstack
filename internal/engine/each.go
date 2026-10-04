package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Fan-out (spec each-fan-out §5): a task that declares `each:` is ONE node of
// the DAG that runs as one item per element of an array. The interpreter
// counts the array once — from the run input, or through a local activity
// whose result is recorded as a marker — and dispatches every item as its own
// ExecuteTask activity, on the node's pool, with its own idempotency key,
// commit key, retry budget and timeout. Nothing here is a scheduler: items are
// ordinary activities in the interpreter's own in-flight set, and the only
// per-item state is the interpreter's, rebuilt from history on replay.

const (
	// Codes recorded on a fan-out's own TaskRun.error.
	EachOverMissingCode     = "EACH_OVER_MISSING"     // ContractViolation, not retryable
	EachOverNotArrayCode    = "EACH_OVER_NOT_ARRAY"   // ContractViolation, not retryable
	EachTooManyItemsCode    = "EACH_TOO_MANY_ITEMS"   // ContractViolation, not retryable
	EachExpansionFailedCode = "EACH_EXPANSION_FAILED" // InfrastructureError
	InvalidEachCode         = "INVALID_EACH"          // a hand-built definition the DSL refuses

	// ActivityCountEachItems is the local activity that counts a task
	// source's array. Internal: no user-facing surface names it.
	ActivityCountEachItems = "CountEachItems"

	eachItemInputBudget    = 16 << 20
	eachItemArtifactBudget = 16384
	// eachDispatchBytes bounds the item inputs one workflow task may schedule
	// (spec §5.4): batch = max(1, eachDispatchBytes / item input bound).
	eachDispatchBytes = 2 << 20
	// eachDispatchYield is the timer that ends a dispatch batch's workflow
	// task when more items are due. A Temporal timer, not a scheduler.
	eachDispatchYield = time.Millisecond
	// eachSnapshotsPerFanOut bounds how many item events of one fan-out write
	// a fresh snapshot (spec §5.6).
	eachSnapshotsPerFanOut = 20
	// The count activity's budget mirrors the snapshot write's: 10s per
	// attempt, 3 attempts, 60s end to end.
	eachCountAttemptTimeout = 10 * time.Second
	eachCountTimeout        = 60 * time.Second
)

// eachDispatchDescending is a TEST-ONLY switch, the precedent of
// awaitDeadlineAfterEmit: TestEachHistory_CatchesAReorderedDispatch flips it
// to prove the fan-out fixture can see a reordered dispatch. Never set in
// production code.
var eachDispatchDescending = false

// eachState is one fan-out's life in this run. Every field is a pure function
// of the workflow input and recorded history, like pinState.
type eachState struct {
	spec     *orkstackv1.EachSpec
	expanded bool
	count    int32 // set at expansion
	next     int32 // lowest index not yet dispatched
	inFlight int32
	settled  int32 // items in a terminal status
	limit    int32 // max_parallel, or count when 0; never above count
	stride   int32 // max(1, ceil(count / eachSnapshotsPerFanOut))
}

// itemFlightKey is an item's key in the loop's in-flight map. A task id is a
// slug (^[a-z][a-z0-9-]*$), so it can never contain "#" and no item key can
// equal a task key.
func itemFlightKey(id string, index int32) string {
	return id + "#" + strconv.Itoa(int(index))
}

// eachBounds are the two per-item bounds of spec §5.1, derived from the
// Orkflow's declared item total alone, so they are known at apply and pinned
// by the definition on replay. A run with no fan-out computes neither.
func eachBounds(wf *orkstackv1.Orkflow, limits WorkspaceLimits) (share int32, inputLimit int64, artifactMax int32) {
	share = dsl.EachItemsDeclared(wf)
	if share <= 0 {
		return 0, 0, 0
	}
	proto := limits.Proto()
	inputLimit = proto.GetValuesMaxBytes()
	if inputLimit <= 0 || inputLimit > task.MaxActivityInputBytes {
		inputLimit = task.MaxActivityInputBytes
	}
	if shared := int64(eachItemInputBudget) / int64(share); shared < inputLimit {
		inputLimit = shared
	}
	artifactMax = proto.GetArtifactMaxCount()
	if shared := int32(eachItemArtifactBudget / int(share)); shared < artifactMax {
		artifactMax = shared
	}
	return share, inputLimit, artifactMax
}

// EachItemBounds exposes eachBounds to internal/api, which checks a fan-out's
// own share of every item's input at apply (spec §7.1).
func EachItemBounds(wf *orkstackv1.Orkflow, limits WorkspaceLimits) (inputLimit int64, artifactMax int32) {
	_, inputLimit, artifactMax = eachBounds(wf, limits)
	return inputLimit, artifactMax
}

// EachError is a failed expansion, as data.
type EachError struct{ Code, Message string }

func (e *EachError) Error() string { return e.Code + ": " + e.Message }

// EachArrayLength walks root by path and returns the length of the array it
// reaches, or an *EachError whose Code is EACH_OVER_MISSING (a key is absent,
// or a value on the way is not an object — the error names the key and what
// it found) or EACH_OVER_NOT_ARRAY (the value reached is null, an object, a
// string, a number or a boolean — named). root is decoded JSON: map[string]any
// / []any / json.Number / string / bool / nil (a structpb AsMap value is the
// same shapes with float64 numbers, which the count ignores). Never mutates
// root. Pure: shared by the workflow, the local activity and Simulate, so the
// three cannot disagree.
func EachArrayLength(root any, path []string, over string) (int32, *EachError) {
	cur := root
	walked := ""
	for _, key := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			where := "the value"
			if walked != "" {
				where = strconv.Quote(walked)
			}
			return 0, &EachError{Code: EachOverMissingCode, Message: fmt.Sprintf(
				"each.over %s: %s is %s, not an object, so it has no key %q", over, where, jsonKind(cur), key)}
		}
		next, present := obj[key]
		if !present {
			return 0, &EachError{Code: EachOverMissingCode, Message: fmt.Sprintf(
				"each.over %s: key %q is absent (found keys: %s)", over, key, keysOf(obj))}
		}
		if walked == "" {
			walked = key
		} else {
			walked += "." + key
		}
		cur = next
	}
	list, ok := cur.([]any)
	if !ok {
		return 0, &EachError{Code: EachOverNotArrayCode, Message: fmt.Sprintf(
			"each.over %s resolved to %s, not an array", over, jsonKind(cur))}
	}
	if len(list) > 1<<31-1 {
		return 0, &EachError{Code: EachTooManyItemsCode, Message: fmt.Sprintf(
			"each.over %s resolved to %d items", over, len(list))}
	}
	return int32(len(list)), nil
}

// jsonKind names a decoded JSON value's kind for a refusal sentence.
func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "an object"
	case []any:
		return "an array"
	case string:
		return "a string"
	case bool:
		return "a boolean"
	case json.Number, float64, float32, int, int32, int64:
		return "a number"
	default:
		return fmt.Sprintf("a %T", v)
	}
}

func keysOf(obj map[string]any) string {
	if len(obj) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, strconv.Quote(k))
	}
	sort.Strings(keys)
	if len(keys) > 10 {
		keys = append(keys[:10], "…")
	}
	return strings.Join(keys, ", ")
}

// eachCountOfInput counts an input source's array from the defaulted run
// input. Shared by the interpreter and Simulate, so the two say one sentence.
func eachCountOfInput(inputs map[string]any, spec *orkstackv1.EachSpec) (int32, *EachError) {
	source := spec.GetSource()
	root, present := inputs[source.GetInput()]
	if !present {
		return 0, &EachError{Code: EachOverMissingCode, Message: fmt.Sprintf(
			"each.over %s: the run input has no %q", spec.GetOver(), source.GetInput())}
	}
	return EachArrayLength(root, source.GetPath(), spec.GetOver())
}

// eachTooMany is the EACH_TOO_MANY_ITEMS refusal, or nil within the bound.
func eachTooMany(count int32, spec *orkstackv1.EachSpec, taskIndex int) *EachError {
	if count <= spec.GetMaxItems() {
		return nil
	}
	return &EachError{Code: EachTooManyItemsCode, Message: fmt.Sprintf(
		"each.over resolved to %d items, over max_items %d declared at tasks[%d].each.max_items",
		count, spec.GetMaxItems(), taskIndex)}
}

// eachErrorCategory is the failure category of an expansion code.
func eachErrorCategory(code string) (string, orkstackv1.FailureCategory) {
	if code == EachExpansionFailedCode {
		return task.CategoryInfrastructureError, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	}
	return task.CategoryContractViolation, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
}

// ---------------------------------------------------------------- the count

// EachValuesReader is the one method of artifacts.Store the count needs.
type EachValuesReader interface {
	Get(ctx context.Context, uri string) (io.ReadCloser, error)
}

// EachActivities owns the local activity that counts a task source's array.
type EachActivities struct{ store EachValuesReader }

// NewEachActivities builds the count activity over the store the run's
// values.json objects live in.
func NewEachActivities(store EachValuesReader) *EachActivities {
	return &EachActivities{store: store}
}

// eachActivityError is a non-retryable expansion refusal with the reason-code
// detail recordFailure's reading already understands.
func eachActivityError(code, message string) error {
	categoryName, category := eachErrorCategory(code)
	return temporal.NewNonRetryableApplicationError(message, categoryName, nil,
		&orkstackv1.ActivityFailureDetail{ReasonCode: code, Category: category})
}

// CountEachItems reads the committed values.json at ref.uri, at most
// maxBytes+1 bytes; refuses bytes whose sha256 is not ref.sha256
// (non-retryable EACH_EXPANSION_FAILED: the store contradicts the commit, and
// no retry makes it agree); decodes with json.Decoder.UseNumber; and returns
// EachArrayLength(value, path, over). A read error is returned as-is and is
// retried by the local activity's policy.
func (a *EachActivities) CountEachItems(
	ctx context.Context,
	ref *orkstackv1.ArtifactRef,
	path []string,
	over string,
	maxBytes int64,
) (int32, error) {
	if a == nil || a.store == nil {
		return 0, eachActivityError(EachExpansionFailedCode,
			fmt.Sprintf("each.over %s: this engine has no artifact store to read the array from", over))
	}
	if maxBytes <= 0 {
		maxBytes = DefaultValuesMaxBytes
	}
	reader, err := a.store.Get(ctx, ref.GetUri())
	if err != nil {
		return 0, fmt.Errorf("each.over %s: read the committed values.json at %s: %w", over, ref.GetUri(), err)
	}
	defer func() { _ = reader.Close() }()
	raw, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return 0, fmt.Errorf("each.over %s: read the committed values.json at %s: %w", over, ref.GetUri(), err)
	}
	if int64(len(raw)) > maxBytes {
		return 0, eachActivityError(EachExpansionFailedCode, fmt.Sprintf(
			"each.over %s: the committed values.json at %s is over the %d-byte values limit",
			over, ref.GetUri(), maxBytes))
	}
	sum := sha256.Sum256(raw)
	if want := ref.GetSha256(); want != "" && hex.EncodeToString(sum[:]) != want {
		return 0, eachActivityError(EachExpansionFailedCode, fmt.Sprintf(
			"each.over %s: the values.json at %s hashes to %s but its commit records %s",
			over, ref.GetUri(), hex.EncodeToString(sum[:]), want))
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return 0, eachActivityError(EachOverNotArrayCode, fmt.Sprintf(
			"each.over %s: the committed values.json is not JSON: %v", over, err))
	}
	n, eerr := EachArrayLength(value, path, over)
	if eerr != nil {
		return 0, eachActivityError(eerr.Code, eerr.Message)
	}
	return n, nil
}

// ---------------------------------------------------------------- expansion

// expandEach counts a ready fan-out's array once and creates its items. It is
// called from the main loop for a task classify decided to run. The returned
// error is always a non-retryable ApplicationError carrying the node's own
// sentence, so Run.error.message is the node's however the array was counted.
func (s *runState) expandEach(ctx, runCtx workflow.Context, id string, inFlight map[string]workflow.Future) error {
	t := s.tasks[id]
	e := s.eaches[id]
	spec := e.spec
	source := spec.GetSource()

	// Refusals a hand-built definition could reach; the DSL refuses both.
	if t.GetPin() != "" || source == nil {
		why := "it names a pin"
		if source == nil {
			why = "its each.source is unset"
		}
		return s.failExpansion(ctx, id, InvalidEachCode, fmt.Sprintf(
			"task %q is a fan-out the DSL refuses: %s (docs/DSL.md rules 19 and 23)", id, why))
	}

	var count int32
	if source.GetInput() != "" {
		// The input is a workflow argument: no IO, no command.
		n, eerr := eachCountOfInput(s.inputs, spec)
		if eerr != nil {
			return s.failExpansion(ctx, id, eerr.Code, eerr.Message)
		}
		count = n
	} else {
		ref := s.results[source.GetTaskId()].GetValuesRef()
		if s.plan.CommitRoot == "" || ref == nil {
			return s.failExpansion(ctx, id, EachExpansionFailedCode, fmt.Sprintf(
				"each.over %s: the values of task %q are not addressable in this run", spec.GetOver(), source.GetTaskId()))
		}
		localCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout:    eachCountAttemptTimeout,
			ScheduleToCloseTimeout: eachCountTimeout,
			RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
		})
		// Recorded as a marker: a replay reads it and never calls the activity,
		// so the array cannot change between replays.
		if err := workflow.ExecuteLocalActivity(localCtx, ActivityCountEachItems,
			ref, source.GetPath(), spec.GetOver(), s.plan.Limits.Proto().GetValuesMaxBytes(),
		).Get(localCtx, &count); err != nil {
			var applicationErr *temporal.ApplicationError
			var detail *orkstackv1.ActivityFailureDetail
			if errors.As(err, &applicationErr) && applicationErr.Details(&detail) == nil && detail.GetReasonCode() != "" {
				return s.failExpansion(ctx, id, detail.GetReasonCode(), applicationErr.Message())
			}
			return s.failExpansion(ctx, id, EachExpansionFailedCode, fmt.Sprintf(
				"each.over %s: the array could not be counted: %v", spec.GetOver(), err))
		}
	}

	if eerr := eachTooMany(count, spec, s.taskIndex(id)); eerr != nil {
		return s.failExpansion(ctx, id, eerr.Code, eerr.Message)
	}

	taskRun := s.taskRuns[id]
	now := timestamppb.New(workflow.Now(ctx))
	e.expanded = true
	e.count = count
	taskRun.Each = &orkstackv1.TaskEach{Expanded: true, Count: count}

	if count == 0 {
		taskRun.Status = orkstackv1.Status_STATUS_SUCCEEDED
		taskRun.FinishedAt = now
		return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
			orkstackv1.Status_STATUS_SUCCEEDED, "fan-out over 0 items: nothing to run")
	}

	if _, err := s.queueForTask(t); err != nil {
		return err
	}
	pool := s.poolForTask(t)
	e.limit = count
	if mp := spec.GetMaxParallel(); mp > 0 && mp < count {
		e.limit = mp
	}
	e.stride = (count + eachSnapshotsPerFanOut - 1) / eachSnapshotsPerFanOut
	if e.stride < 1 {
		e.stride = 1
	}
	taskRun.Status = orkstackv1.Status_STATUS_RUNNING
	taskRun.ScheduledAt = now
	taskRun.Pool = pool
	taskRun.Items = make([]*orkstackv1.TaskRun, count)
	for i := int32(0); i < count; i++ {
		taskRun.Items[i] = &orkstackv1.TaskRun{
			TaskId: id,
			Item:   proto.Int32(i),
			Status: orkstackv1.Status_STATUS_PENDING,
		}
	}
	scheduled := fmt.Sprintf("fan-out of %d items scheduled on pool %s", count, pool)
	if e.limit < count {
		scheduled += fmt.Sprintf(", at most %d at a time", e.limit)
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id,
		orkstackv1.Status_STATUS_RUNNING, scheduled); err != nil {
		return err
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, id,
		orkstackv1.Status_STATUS_RUNNING, fmt.Sprintf("dispatching %d items to orkers", count)); err != nil {
		return err
	}
	return s.dispatchItems(ctx, runCtx, id, inFlight)
}

// taskIndex is id's position in the definition, for a rule-anchored sentence.
func (s *runState) taskIndex(id string) int {
	for i, candidate := range s.order {
		if candidate == id {
			return i
		}
	}
	return -1
}

// failExpansion records a fan-out that could not be expanded: the node FAILED
// with code and message, no item created, TASK_FAILED emitted, and the
// non-retryable error the loop hands to fail().
func (s *runState) failExpansion(ctx workflow.Context, id, code, message string) error {
	categoryName, category := eachErrorCategory(code)
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Error = &orkstackv1.Error{
		Code:     code,
		Message:  message,
		Details:  fmt.Sprintf("task %q", id),
		Category: category,
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, message); err != nil {
		workflow.GetLogger(ctx).Error("emit fan-out expansion failure failed",
			"run_id", s.runID, "task_id", id, "error", err)
	}
	return temporal.NewNonRetryableApplicationError(message, categoryName, nil)
}

// ---------------------------------------------------------------- dispatch

// dispatchItems schedules items in ascending index order while a slot is free
// and an item is due, at most batch per workflow task; a yield timer ends the
// workflow task between batches, so no completion carries more than
// eachDispatchBytes of item inputs (spec §5.1).
func (s *runState) dispatchItems(ctx, runCtx workflow.Context, id string, inFlight map[string]workflow.Future) error {
	e := s.eaches[id]
	batch := int64(eachDispatchBytes) / s.itemInputLimit
	if batch < 1 {
		batch = 1
	}
	var sent int64
	for e.inFlight < e.limit && e.next < e.count {
		if sent == batch {
			if err := workflow.Sleep(runCtx, eachDispatchYield); err != nil {
				return err
			}
			sent = 0
		}
		index := e.next
		if eachDispatchDescending {
			index = e.count - 1 - e.next
		}
		e.next++
		future, err := s.scheduleItem(runCtx, id, index)
		if err != nil {
			e.settled++
			return s.itemRefused(ctx, id, index, err)
		}
		inFlight[itemFlightKey(id, index)] = future
		e.inFlight++
		sent++
	}
	return nil
}

// itemRefused records an item the engine refused before scheduling it
// (INPUT_TOO_LARGE), then fails the node as a failed item does.
func (s *runState) itemRefused(ctx workflow.Context, id string, index int32, cause error) error {
	item := s.taskRuns[id].Items[index]
	item.Status = orkstackv1.Status_STATUS_FAILED
	item.FinishedAt = timestamppb.New(workflow.Now(ctx))
	code := InvalidEachCode
	var applicationErr *temporal.ApplicationError
	if errors.As(cause, &applicationErr) {
		code = applicationErr.Type()
		if code == task.CategoryContractViolation {
			code = task.ReasonInputTooLarge
		}
	}
	item.Error = &orkstackv1.Error{
		Code:      code,
		Message:   cause.Error(),
		Details:   fmt.Sprintf("task %q item %d", id, index),
		Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
		Retryable: false,
	}
	if err := s.emitItem(ctx, id, index, item.Status, fmt.Sprintf("item %d failed: %s", index, cause.Error()), true); err != nil {
		workflow.GetLogger(ctx).Error("emit item failure failed", "run_id", s.runID, "task_id", id, "error", err)
	}
	return s.failNodeOnItem(ctx, id, index)
}

// scheduleItem is schedule for one item, with exactly the differences of spec
// §5.4: the item's idempotency key and commit URI, the per-item artifact
// limit, and EachContext. No event is emitted at dispatch.
func (s *runState) scheduleItem(runCtx workflow.Context, id string, index int32) (workflow.Future, error) {
	t := s.tasks[id]
	e := s.eaches[id]
	item := s.taskRuns[id].Items[index]
	queue, err := s.queueForTask(t)
	if err != nil {
		return nil, err
	}
	pool := s.poolForTask(t)
	item.Status = orkstackv1.Status_STATUS_RUNNING
	item.ScheduledAt = timestamppb.New(workflow.Now(runCtx))
	item.Pool = pool

	commitURI := ""
	if s.plan.CommitRoot != "" {
		key, err := artifacts.ItemCommitKey(s.wf.GetNamespace(), s.runID, id, index)
		if err != nil {
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("task %q item %d: compose the commit uri: %v", id, index, err), "INVALID_COMMIT_KEY", err)
		}
		commitURI = strings.TrimSuffix(s.plan.CommitRoot, "/") + "/" + key
	}
	input := s.executeTaskInput(t, id, pool, nil)
	input.IdempotencyKey = task.ItemIdempotencyKeyFor(s.wf.GetNamespace(), s.runID, id, index)
	input.CommitUri = commitURI
	input.Limits.ArtifactMaxCount = s.itemArtifactMax
	input.Each = &orkstackv1.EachContext{
		Index:  index,
		Count:  e.count,
		As:     e.spec.GetAs(),
		Source: e.spec.GetSource(),
	}
	if err := s.checkItemInput(id, index, input); err != nil {
		return nil, err
	}
	activityCtx := s.taskActivityContext(runCtx, t, queue)
	return workflow.ExecuteActivity(activityCtx, task.ActivityExecuteTask, input), nil
}

// checkItemInput is CheckInputSize at the per-item bound, with the
// shared-budget sentence when the bound is the fan-outs' share rather than the
// plan's own limit.
func (s *runState) checkItemInput(id string, index int32, input *orkstackv1.ExecuteTaskInput) error {
	planLimit := s.plan.Limits.Proto().GetValuesMaxBytes()
	if planLimit <= 0 || planLimit > task.MaxActivityInputBytes {
		planLimit = task.MaxActivityInputBytes
	}
	if s.itemInputLimit >= planLimit {
		if err := task.CheckInputSize(input, planLimit); err != nil {
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("%v (item %d)", err, index), task.CategoryContractViolation, err)
		}
		return nil
	}
	size, err := task.InputSize(input)
	if err != nil {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: task %q item %d: encode the activity input: %v", task.ReasonInputTooLarge, id, index, err),
			task.CategoryContractViolation, err)
	}
	if size <= s.itemInputLimit {
		return nil
	}
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(
		"%s: task %q item %d: the activity input serializes to %d bytes, over this Orkflow's %d-byte "+
			"per-item limit: its fan-outs declare %d items, and the items of one run share a 16 MiB "+
			"history budget. Declare fewer items, or move large with: values and run inputs into an artifact.",
		task.ReasonInputTooLarge, id, index, size, s.itemInputLimit, s.eachShare),
		task.CategoryContractViolation, nil)
}

// ---------------------------------------------------------------- collection

// collectItem awaits one item future and records its outcome; when not
// draining, it refills the freed slot and decides the node.
func (s *runState) collectItem(
	ctx, runCtx workflow.Context,
	id string,
	index int32,
	future workflow.Future,
	inFlight map[string]workflow.Future,
	draining bool,
) error {
	e := s.eaches[id]
	node := s.taskRuns[id]
	item := node.Items[index]
	e.inFlight--
	e.settled++

	result := &orkstackv1.ExecuteTaskResult{}
	cause := future.Get(ctx, result)
	if cause != nil {
		s.recordFailureOn(ctx, item, fmt.Sprintf("task %q item %d", id, index), cause)
	} else {
		item.Status = orkstackv1.Status_STATUS_SUCCEEDED
		item.FinishedAt = timestamppb.New(workflow.Now(ctx))
		item.Artifacts = result.GetArtifacts()
		item.SystemArtifacts = result.GetSystemArtifacts()
		item.ValuesRef = result.GetValuesRef()
		item.Commit = result.GetCommit()
		item.ExitCode = result.GetExitCode()
		item.OrkerId = result.GetOrkerId()
		item.StartedAt = result.GetStartedAt()
		if result.GetAttempt() > 0 {
			item.Attempt = result.GetAttempt()
		}
		// No output and no s.outputs entry: nothing reads an item's values
		// in workflow code (docs/DSL.md rule 24).
	}
	if started := item.GetStartedAt(); started != nil &&
		(node.GetStartedAt() == nil || started.AsTime().Before(node.GetStartedAt().AsTime())) {
		node.StartedAt = started
	}

	if !draining && cause == nil {
		// First, so a snapshot written by the event below already shows the
		// freed slot's replacement as RUNNING.
		if err := s.dispatchItems(ctx, runCtx, id, inFlight); err != nil {
			return err
		}
	}

	var message string
	switch item.GetStatus() {
	case orkstackv1.Status_STATUS_SUCCEEDED:
		message = fmt.Sprintf("item %d succeeded", index)
	case orkstackv1.Status_STATUS_CANCELED:
		message = fmt.Sprintf("item %d canceled: %s", index, item.GetError().GetMessage())
	default:
		message = fmt.Sprintf("item %d failed: %s", index, item.GetError().GetMessage())
	}
	fresh := e.settled == e.count || e.settled%e.stride == 0
	if err := s.emitItem(ctx, id, index, item.GetStatus(), message, fresh); err != nil {
		if !draining {
			return err
		}
		workflow.GetLogger(ctx).Error("emit item event failed", "run_id", s.runID, "task_id", id, "error", err)
	}
	if draining {
		return nil
	}

	switch item.GetStatus() {
	case orkstackv1.Status_STATUS_FAILED:
		return s.failNodeOnItem(ctx, id, index)
	case orkstackv1.Status_STATUS_CANCELED:
		// Only the operator's run cancellation reaches an item's context
		// while the run is not ending; fail() records RUN_CANCELED.
		return cause
	}
	if e.settled == e.count && s.allItemsSucceeded(id) {
		node.Status = orkstackv1.Status_STATUS_SUCCEEDED
		node.FinishedAt = timestamppb.New(workflow.Now(ctx))
		return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
			orkstackv1.Status_STATUS_SUCCEEDED, fmt.Sprintf("all %d items succeeded", e.count))
	}
	return nil
}

func (s *runState) allItemsSucceeded(id string) bool {
	for _, item := range s.taskRuns[id].GetItems() {
		if item.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			return false
		}
	}
	return true
}

// failNodeOnItem fails a fan-out because its item index failed for good: the
// node takes the item's code, category and retry state, its message names the
// item, TASK_FAILED is emitted, and the non-retryable error goes to fail().
func (s *runState) failNodeOnItem(ctx workflow.Context, id string, index int32) error {
	e := s.eaches[id]
	node := s.taskRuns[id]
	item := node.Items[index]
	itemErr := item.GetError()
	message := fmt.Sprintf("item %d of %d failed: %s", index, e.count, itemErr.GetMessage())
	details := fmt.Sprintf("task %q item %d", id, index)
	if d := itemErr.GetDetails(); d != "" && d != details {
		details += "; " + strings.TrimPrefix(d, details+"; ")
	}
	node.Status = orkstackv1.Status_STATUS_FAILED
	node.FinishedAt = timestamppb.New(workflow.Now(ctx))
	node.Error = &orkstackv1.Error{
		Code:      itemErr.GetCode(),
		Message:   message,
		Details:   details,
		Retryable: itemErr.GetRetryable(),
		Category:  itemErr.GetCategory(),
	}
	node.RetryState = item.GetRetryState()
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, message); err != nil {
		workflow.GetLogger(ctx).Error("emit fan-out failure failed",
			"run_id", s.runID, "task_id", id, "error", err)
	}
	return temporal.NewNonRetryableApplicationError(message, categoryNameOf(itemErr.GetCategory()), nil)
}

// categoryNameOf maps a FailureCategory back to its ApplicationError type.
func categoryNameOf(category orkstackv1.FailureCategory) string {
	switch category {
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED:
		return task.CategoryActivityFailed
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR:
		return task.CategoryInfrastructureError
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION:
		return task.CategoryContractViolation
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT:
		return task.CategoryTimedOut
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED:
		return task.CategoryCancelled
	default:
		return "TASK_FAILED"
	}
}

// finishFanOutOnRunEnd is cancelInFlight's second half for one expanded
// fan-out that is not SUCCEEDED: its never-dispatched items become CANCELED
// with no event each; then the node SUCCEEDED (every item had), stays FAILED,
// or becomes CANCELED, with one node summary TASK_LOG when items were
// unfinished.
func (s *runState) finishFanOutOnRunEnd(ctx workflow.Context, id string) {
	e := s.eaches[id]
	node := s.taskRuns[id]
	if e == nil || !e.expanded || e.count == 0 || node.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
		return
	}
	now := timestamppb.New(workflow.Now(ctx))
	for _, item := range node.GetItems() {
		if item.GetStatus() == orkstackv1.Status_STATUS_PENDING {
			item.Status = orkstackv1.Status_STATUS_CANCELED
			item.FinishedAt = now
			e.settled++
		}
	}
	if node.GetStatus() != orkstackv1.Status_STATUS_FAILED && s.allItemsSucceeded(id) {
		node.Status = orkstackv1.Status_STATUS_SUCCEEDED
		node.FinishedAt = now
		if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
			orkstackv1.Status_STATUS_SUCCEEDED, fmt.Sprintf("all %d items succeeded", e.count)); err != nil {
			workflow.GetLogger(ctx).Error("emit fan-out success failed", "run_id", s.runID, "task_id", id, "error", err)
		}
		return
	}
	var running, never int32
	for _, item := range node.GetItems() {
		if item.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
			continue
		}
		if item.GetScheduledAt() != nil {
			running++
		} else {
			never++
		}
	}
	status := orkstackv1.Status_STATUS_CANCELED
	verb := "canceled"
	if node.GetStatus() == orkstackv1.Status_STATUS_FAILED {
		status = orkstackv1.Status_STATUS_FAILED
		verb = "failed"
	} else {
		node.Status = orkstackv1.Status_STATUS_CANCELED
		node.FinishedAt = now
	}
	if running+never == 0 {
		return
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, id, status, fmt.Sprintf(
		"task %s: run ended with %d of %d items unfinished (%d were running, %d never started)",
		verb, running+never, e.count, running, never)); err != nil {
		workflow.GetLogger(ctx).Error("emit fan-out summary failed", "run_id", s.runID, "task_id", id, "error", err)
	}
}

// ---------------------------------------------------------------- events

// emitItem publishes one item event. It numbers the event from the same
// counter emit uses (history, the bus and the archive stay identical,
// AGENTS.md §7) and writes a fresh snapshot only when fresh is true; otherwise
// it schedules PublishEvent with an EMPTY snapshot URI, and the event carries
// no snapshot of its own.
func (s *runState) emitItem(ctx workflow.Context, id string, index int32,
	status orkstackv1.Status, message string, fresh bool,
) error {
	s.eventSeq++
	event := &orkstackv1.Event{
		RunId:     s.runID,
		Sequence:  int64(s.eventSeq),
		Type:      orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
		TaskId:    id,
		Status:    status,
		Message:   message,
		Timestamp: timestamppb.New(workflow.Now(ctx)),
		Item:      proto.Int32(index),
	}
	dedupeKey := fmt.Sprintf("%s#%d", s.runID, s.eventSeq)
	snapshotURI := ""
	if fresh {
		localCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout:    snapshotAttemptTimeout,
			ScheduleToCloseTimeout: snapshotActivityTimeout,
			RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
		})
		if err := workflow.ExecuteLocalActivity(
			localCtx, ActivityWriteSnapshot, s.snapshot(), int64(s.eventSeq),
		).Get(localCtx, &snapshotURI); err != nil {
			return fmt.Errorf("write snapshot for event %d: %w", s.eventSeq, err)
		}
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           EventQueue,
		StartToCloseTimeout: eventActivityTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	if err := workflow.ExecuteActivity(activityCtx, ActivityPublishEvent, event, snapshotURI, dedupeKey).
		Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("publish item event failed",
			"run_id", s.runID, "task_id", id, "item", index, "error", err)
	}
	return nil
}

// ---------------------------------------------------------------- bodies

// WithoutItems returns a clone of run with every TaskRun.items cleared and
// everything else kept: the shape of the workflow's own result, a ListRuns
// page and a webhook body (spec §5.13). For a run with no fan-out the clone
// equals the input.
func WithoutItems(run *orkstackv1.Run) *orkstackv1.Run {
	if run == nil {
		return nil
	}
	has := false
	for _, taskRun := range run.GetTaskRuns() {
		if len(taskRun.GetItems()) > 0 {
			has = true
			break
		}
	}
	if !has {
		return run
	}
	clone := proto.Clone(run).(*orkstackv1.Run)
	for _, taskRun := range clone.GetTaskRuns() {
		taskRun.Items = nil
	}
	return clone
}
