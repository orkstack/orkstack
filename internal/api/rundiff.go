package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// diffFields is the canonical order, and the whole vocabulary, of
// RunFieldDiff.field plus the run-level names DiffRunsRequest.only accepts.
var diffFields = []string{
	"definition", "input", // run-level
	"presence", "status", "attempt", "pool", "pin", "orker",
	"exit_code", "error", "duration", "image", "platform_digest", "cached", // task-level
}

// taskDiffFields is the task-level half of diffFields, in row order.
var taskDiffFields = diffFields[2:]

// The reason tokens a side carries when its evidence is gone. The list is
// closed and documented (docs/UI_CLI_PARITY.md "Run diff"); both clients map
// each to a sentence.
const (
	reasonDefinitionUnavailable = "definition_unavailable"
	reasonDefinitionMismatch    = "definition_digest_mismatch"
	reasonTaskNotInDefinition   = "task_not_in_definition"
	reasonNoAttemptStore        = "no_attempt_store"
	reasonNoOutcomeAttempt      = "no_outcome_attempt"
	reasonAttemptRecordMissing  = "attempt_record_missing"
	reasonTimestampsMissing     = "timestamps_missing"
	reasonTimestampsInverted    = "timestamps_inverted"
)

// diffValue is one side of one field: a rendered value, or the reason the
// evidence for it is gone. available with an empty value is "does not apply",
// which a client prints as "-".
type diffValue struct {
	value     string
	available bool
	reason    string
}

func valueOf(value string) diffValue { return diffValue{value: value, available: true} }

func unavailableBecause(reason string) diffValue { return diffValue{reason: reason} }

// pinnedDefinition is one side's pinned definition, reduced to what the diff
// reads from it: the image each task was dispatched with.
type pinnedDefinition struct {
	images map[string]string
	// reason is set when the document cannot answer for the run.
	reason string
}

// diffSide is everything read for one run, shared by every field of every
// task of that side. records is filled lazily, once per task.
type diffSide struct {
	run        *orkstackv1.Run
	definition pinnedDefinition
	records    map[string][]*orkstackv1.TaskAttempt
}

// DiffRuns compares two runs of one namespace field by field.
//
// It is a read over what GetRun, GetOrkflow and ListTaskAttempts already
// serve, computed on every call and stored nowhere. Every coordinate it reads
// comes off the two runs; nothing is taken from the caller but the two ids,
// the namespace and the field selection. A store error fails the call rather
// than degrading to "unavailable": a transient error counted as missing
// evidence would make the same question answer differently twice.
func (s *Service) DiffRuns(
	ctx context.Context,
	req *connect.Request[orkstackv1.DiffRunsRequest],
) (*connect.Response[orkstackv1.DiffRunsResponse], error) {
	msg := req.Msg
	switch {
	case msg.GetFromRunId() == "":
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("from_run_id is required"))
	case msg.GetToRunId() == "":
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("to_run_id is required"))
	case msg.GetFromRunId() == msg.GetToRunId():
		// Byte equality: a run id is never parsed (AGENTS.md §7).
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("from_run_id and to_run_id name the same run %q", msg.GetFromRunId()))
	}
	fields, err := resolveDiffFields(msg.GetOnly())
	if err != nil {
		return nil, err
	}
	compared := func(field string) bool { return slices.Contains(fields, field) }

	ns, err := s.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	fromRun, err := s.lookupRunInResolved(ctx, msg.GetFromRunId(), ns.GetName())
	if err != nil {
		return nil, sideError("from", err)
	}
	toRun, err := s.lookupRunInResolved(ctx, msg.GetToRunId(), ns.GetName())
	if err != nil {
		return nil, sideError("to", err)
	}

	res := &orkstackv1.DiffRunsResponse{
		From:                 shallowRun(fromRun),
		To:                   shallowRun(toRun),
		SameOrkflow:          fromRun.GetOrkflowName() == toRun.GetOrkflowName(),
		SameDefinitionDigest: fromRun.GetOrkflowDefinitionDigest() == toRun.GetOrkflowDefinitionDigest(),
		FieldsCompared:       fields,
	}

	if compared("definition") {
		from, to := definitionLabel(fromRun), definitionLabel(toRun)
		if from != to {
			res.Differences = append(res.Differences, &orkstackv1.RunFieldDiff{
				Field: "definition", FromValue: from, ToValue: to, FromAvailable: true, ToAvailable: true,
			})
		}
	}
	if compared("input") {
		diff, err := inputDiff(fromRun, toRun)
		if err != nil {
			return nil, err
		}
		res.InputUnifiedDiff = diff
	}

	from := &diffSide{run: fromRun, records: map[string][]*orkstackv1.TaskAttempt{}}
	to := &diffSide{run: toRun, records: map[string][]*orkstackv1.TaskAttempt{}}
	if compared("image") {
		for _, side := range []*diffSide{from, to} {
			definition, err := s.pinnedDefinition(ctx, ns.GetName(), side.run)
			if err != nil {
				return nil, err
			}
			side.definition = definition
		}
	}

	universe, fromTasks, toTasks := taskUniverse(fromRun, toRun)
	res.TasksCompared = int32(len(universe))
	for _, taskID := range universe {
		fromTask, toTask := fromTasks[taskID], toTasks[taskID]
		if fromTask == nil || toTask == nil {
			// A one-sided task yields exactly its presence row, under every
			// `only`: its other fields would all be one-sided rows saying the
			// same thing, and nothing is read on its behalf.
			res.Differences = append(res.Differences, diffRow(taskID, "presence",
				valueOf(presenceOf(fromTask)), valueOf(presenceOf(toTask))))
			continue
		}
		// A fan-out's own row is compared on the fields it owns only:
		// attempt, orker, exit_code, image and platform_digest belong to its
		// items, and comparing the node's unset values would report missing
		// evidence where there is none.
		fanOut := fromTask.GetEach() != nil || toTask.GetEach() != nil
		// A composed task's row likewise: attempt, orker, exit_code, image
		// and platform_digest are its child's facts, and child.run_id is not
		// compared — two runs' children always have two ids, and the
		// definition they ran is compared through the parent's digest.
		composed := fromTask.GetChild() != nil || toTask.GetChild() != nil
		fieldsFor := func(item bool) []string {
			if composed {
				return composedDiffFields
			}
			if fanOut && !item {
				return fanOutNodeDiffFields
			}
			return taskDiffFields
		}
		rows, err := s.diffPair(ctx, taskID, nil, fieldsFor(false), compared, from, to, fromTask, toTask)
		if err != nil {
			return nil, err
		}
		identical := len(rows) == 0
		res.Differences = append(res.Differences, rows...)
		if fanOut && fromTask.GetEach().GetExpanded() && toTask.GetEach().GetExpanded() {
			// Items pairwise by index, after the node's rows, each row
			// carrying its item; an index one run lacks is a presence row.
			fromItems, toItems := fromTask.GetItems(), toTask.GetItems()
			n := len(fromItems)
			if len(toItems) > n {
				n = len(toItems)
			}
			for i := 0; i < n; i++ {
				index := int32(i)
				var fromItem, toItem *orkstackv1.TaskRun
				if i < len(fromItems) {
					fromItem = fromItems[i]
				}
				if i < len(toItems) {
					toItem = toItems[i]
				}
				if fromItem == nil || toItem == nil {
					row := diffRow(taskID, "presence", valueOf(presenceOf(fromItem)), valueOf(presenceOf(toItem)))
					row.Item = &index
					res.Differences = append(res.Differences, row)
					identical = false
					continue
				}
				itemRows, err := s.diffPair(ctx, taskID, &index, fieldsFor(true), compared, from, to, fromItem, toItem)
				if err != nil {
					return nil, err
				}
				if len(itemRows) > 0 {
					identical = false
					res.Differences = append(res.Differences, itemRows...)
				}
			}
		}
		if identical {
			res.TasksIdentical++
		}
	}
	for _, row := range res.GetDifferences() {
		if !row.GetFromAvailable() || !row.GetToAvailable() {
			res.EvidenceUnavailable++
		}
	}
	return connect.NewResponse(res), nil
}

// composedDiffFields are the fields a composed task's row is compared on.
var composedDiffFields = []string{"presence", "status", "error", "duration"}

// fanOutNodeDiffFields are the fields a fan-out's own row is compared on.
var fanOutNodeDiffFields = []string{"presence", "status", "pool", "pin", "error", "duration"}

// diffPair compares one pair of rows — two tasks, or two items of one
// fan-out (item set) — on fields, and returns the rows that differ.
func (s *Service) diffPair(
	ctx context.Context,
	taskID string,
	item *int32,
	fields []string,
	compared func(string) bool,
	from, to *diffSide,
	fromTask, toTask *orkstackv1.TaskRun,
) ([]*orkstackv1.RunFieldDiff, error) {
	var rows []*orkstackv1.RunFieldDiff
	for _, field := range fields {
		if field == "presence" || !compared(field) {
			continue
		}
		fromValue, err := s.taskField(ctx, field, from, fromTask)
		if err != nil {
			return nil, err
		}
		toValue, err := s.taskField(ctx, field, to, toTask)
		if err != nil {
			return nil, err
		}
		if fromValue.available && toValue.available && fromValue.value == toValue.value {
			continue
		}
		row := diffRow(taskID, field, fromValue, toValue)
		if item != nil {
			index := *item
			row.Item = &index
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// resolveDiffFields validates `only` and returns the resolved field list in
// canonical order: every field when empty, else the named ones plus presence,
// which is always compared so narrowing can never make an added or removed
// task count as identical.
func resolveDiffFields(only []string) ([]string, error) {
	if len(only) == 0 {
		return slices.Clone(diffFields), nil
	}
	named := map[string]bool{"presence": true}
	for _, field := range only {
		if !slices.Contains(diffFields, field) {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("unknown diff field %q; valid fields: %s", field, strings.Join(diffFields, ", ")))
		}
		named[field] = true
	}
	resolved := make([]string, 0, len(named))
	for _, field := range diffFields {
		if named[field] {
			resolved = append(resolved, field)
		}
	}
	return resolved, nil
}

// sideError names the side a run lookup failed for, keeping its code.
func sideError(side string, err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connect.NewError(connectErr.Code(), fmt.Errorf("%s run: %w", side, connectErr.Unwrap()))
	}
	return connect.NewError(connect.CodeOf(err), fmt.Errorf("%s run: %w", side, err))
}

// shallowRun is the run as GetRun carries it, less the three fields the diff
// already answers for (task_runs, input) or that may be large (output). It is
// a copy: the run may be the reader's own.
func shallowRun(run *orkstackv1.Run) *orkstackv1.Run {
	shallow := proto.Clone(run).(*orkstackv1.Run)
	shallow.TaskRuns, shallow.Input, shallow.Output = nil, nil, nil
	return shallow
}

// definitionLabel is the run-level definition value: name@version digest.
func definitionLabel(run *orkstackv1.Run) string {
	label := run.GetOrkflowName() + "@" + run.GetOrkflowVersion()
	if digest := run.GetOrkflowDefinitionDigest(); digest != "" {
		label += " " + digest
	}
	return label
}

// inputDiff is dsl.Unified over the two inputs' canonical JSON.
func inputDiff(from, to *orkstackv1.Run) (string, error) {
	fromText, err := dsl.CanonicalInputJSON(from.GetInput().GetData())
	if err != nil {
		return "", connect.NewError(connect.CodeInternal, fmt.Errorf("render input of run %q: %w", from.GetId(), err))
	}
	toText, err := dsl.CanonicalInputJSON(to.GetInput().GetData())
	if err != nil {
		return "", connect.NewError(connect.CodeInternal, fmt.Errorf("render input of run %q: %w", to.GetId(), err))
	}
	// Each text gets its final newline back for the diff: CanonicalInputJSON
	// ends without one, and dsl.Unified would otherwise mark the closing brace
	// "\ No newline at end of file" in every input diff — a fact about no file
	// anyone wrote.
	return dsl.Unified(from.GetId()+"/input.json", to.GetId()+"/input.json", fromText+"\n", toText+"\n"), nil
}

// pinnedDefinition reads the definition document a run pinned. A document
// that is gone, or that does not hash to the digest the run recorded, cannot
// answer for the run and says why; any other store error fails the call.
func (s *Service) pinnedDefinition(ctx context.Context, namespace string, run *orkstackv1.Run) (pinnedDefinition, error) {
	wf, err := s.orkflows.Get(ctx, namespace, run.GetOrkflowName(), run.GetOrkflowVersion())
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		// The version was deleted; runs pinned to it keep their snapshot.
		return pinnedDefinition{reason: reasonDefinitionUnavailable}, nil
	case err != nil:
		return pinnedDefinition{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read definition %s@%s of run %q: %w",
				run.GetOrkflowName(), run.GetOrkflowVersion(), run.GetId(), err))
	case wf.GetDefinitionDigest() != run.GetOrkflowDefinitionDigest():
		// Content hashes over implicit identity (AGENTS.md §14): the run says
		// which bytes it ran, and a document that does not hash to them is not
		// those bytes, whatever its coordinates say.
		return pinnedDefinition{reason: reasonDefinitionMismatch}, nil
	}
	images := make(map[string]string, len(wf.GetTasks()))
	for _, task := range wf.GetTasks() {
		images[task.GetId()] = task.GetImage().GetPinned()
	}
	return pinnedDefinition{images: images}, nil
}

// taskUniverse is every task id of both runs: the from-run's in its order,
// then the to-run's that the from-run lacks, in the to-run's order.
func taskUniverse(from, to *orkstackv1.Run) ([]string, map[string]*orkstackv1.TaskRun, map[string]*orkstackv1.TaskRun) {
	index := func(run *orkstackv1.Run) map[string]*orkstackv1.TaskRun {
		tasks := make(map[string]*orkstackv1.TaskRun, len(run.GetTaskRuns()))
		for _, task := range run.GetTaskRuns() {
			if _, seen := tasks[task.GetTaskId()]; !seen {
				tasks[task.GetTaskId()] = task
			}
		}
		return tasks
	}
	fromTasks, toTasks := index(from), index(to)
	universe := make([]string, 0, len(fromTasks)+len(toTasks))
	seen := map[string]bool{}
	for _, run := range []*orkstackv1.Run{from, to} {
		for _, task := range run.GetTaskRuns() {
			if !seen[task.GetTaskId()] {
				seen[task.GetTaskId()] = true
				universe = append(universe, task.GetTaskId())
			}
		}
	}
	return universe, fromTasks, toTasks
}

func presenceOf(task *orkstackv1.TaskRun) string {
	if task == nil {
		return "absent"
	}
	return "present"
}

func diffRow(taskID, field string, from, to diffValue) *orkstackv1.RunFieldDiff {
	return &orkstackv1.RunFieldDiff{
		TaskId:        taskID,
		Field:         field,
		FromValue:     from.value,
		ToValue:       to.value,
		FromAvailable: from.available,
		ToAvailable:   to.available,
		FromReason:    from.reason,
		ToReason:      to.reason,
	}
}

// taskField renders one task-level field of one side, per the normative table
// of the run-diff contract (docs/UI_CLI_PARITY.md "Run diff").
func (s *Service) taskField(ctx context.Context, field string, side *diffSide, task *orkstackv1.TaskRun) (diffValue, error) {
	switch field {
	case "status":
		return valueOf(strings.TrimPrefix(task.GetStatus().String(), "STATUS_")), nil
	case "attempt":
		return valueOf(strconv.Itoa(int(task.GetAttempt()))), nil
	case "pool":
		return valueOf(task.GetPool()), nil
	case "pin":
		return valueOf(task.GetPin()), nil
	case "orker":
		return valueOf(task.GetOrkerId()), nil
	case "exit_code":
		if task.GetOrkerId() == "" {
			// No Orker reported: a 0 here would read as "exited 0".
			return valueOf(""), nil
		}
		return valueOf(strconv.Itoa(int(task.GetExitCode()))), nil
	case "error":
		return valueOf(errorLabel(task.GetError())), nil
	case "duration":
		return durationOf(task), nil
	case "image":
		return s.imageOf(ctx, side, task)
	case "platform_digest":
		return s.platformDigestOf(ctx, side, task)
	case "cached":
		return valueOf(cacheDiffValue(task)), nil
	}
	return diffValue{}, connect.NewError(connect.CodeInternal, fmt.Errorf("diff field %q has no renderer", field))
}

// cacheDiffValue renders a task's memo decision for the `cached` field:
// "hit <source run id>", "miss", "bypassed", or "" for a row with no cache.
// Both sides are always available: the value is the run body's.
func cacheDiffValue(t *orkstackv1.TaskRun) string {
	switch t.GetCache().GetOutcome() {
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT:
		return "hit " + t.GetCache().GetSource().GetRunId()
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS:
		return "miss"
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED:
		return "bypassed"
	}
	return ""
}

// errorLabel is "{code} ({category})": the message and details are never
// compared, because they carry ids and timings.
func errorLabel(e *orkstackv1.Error) string {
	if e == nil {
		return ""
	}
	if e.GetCategory() == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		return e.GetCode()
	}
	return e.GetCode() + " (" + strings.TrimPrefix(e.GetCategory().String(), "FAILURE_CATEGORY_") + ")"
}

// durationOf is the span the snapshot recorded, in Go's own spelling.
func durationOf(task *orkstackv1.TaskRun) diffValue {
	if !isTerminalStatus(task.GetStatus()) || task.GetStatus() == orkstackv1.Status_STATUS_SKIPPED {
		return valueOf("")
	}
	started, finished := task.GetStartedAt(), task.GetFinishedAt()
	if task.GetOrkerId() == "" && started == nil {
		// Never reached an Orker: there is no span to report.
		return valueOf("")
	}
	if started == nil || finished == nil {
		return unavailableBecause(reasonTimestampsMissing)
	}
	span := finished.AsTime().Sub(started.AsTime())
	if span < 0 {
		// Two clocks; never printed negative.
		return unavailableBecause(reasonTimestampsInverted)
	}
	return valueOf(span.Round(time.Millisecond).String())
}

// imageOf is the pinned reference the task was dispatched with: from the
// pinned definition or, when that document cannot answer, from the outcome
// attempt record, which agrees with it by construction (the Orker refuses to
// run anything else).
func (s *Service) imageOf(ctx context.Context, side *diffSide, task *orkstackv1.TaskRun) (diffValue, error) {
	if side.definition.reason == "" {
		pinned, ok := side.definition.images[task.GetTaskId()]
		if !ok {
			return unavailableBecause(reasonTaskNotInDefinition), nil
		}
		return valueOf(pinned), nil
	}
	record, _, err := s.outcomeAttempt(ctx, side, task)
	if err != nil {
		return diffValue{}, err
	}
	if pinned := record.GetImage().GetPinned(); pinned != "" {
		return valueOf(pinned), nil
	}
	return unavailableBecause(side.definition.reason), nil
}

// platformDigestOf is the manifest the Orker recorded having run.
func (s *Service) platformDigestOf(ctx context.Context, side *diffSide, task *orkstackv1.TaskRun) (diffValue, error) {
	if task.GetOrkerId() == "" {
		// Never reached an Orker: nothing ran, and nothing is read.
		return valueOf(""), nil
	}
	record, reason, err := s.outcomeAttempt(ctx, side, task)
	if err != nil {
		return diffValue{}, err
	}
	if record == nil {
		return unavailableBecause(reason), nil
	}
	return valueOf(record.GetImage().GetPlatformDigest()), nil
}

// outcomeAttempt finds the record that speaks for the task's outcome, or the
// reason there is none. It reads only for a task that reached an Orker, and
// reads once per (run, task): image and platform_digest share the records.
func (s *Service) outcomeAttempt(ctx context.Context, side *diffSide, task *orkstackv1.TaskRun) (*orkstackv1.TaskAttempt, string, error) {
	switch {
	case task.GetOrkerId() == "":
		return nil, reasonAttemptRecordMissing, nil
	case s.attempts == nil:
		return nil, reasonNoAttemptStore, nil
	case task.GetAttempt() == 0:
		return nil, reasonNoOutcomeAttempt, nil
	}
	// Keyed by task and, for an item of a fan-out, by item: an item's
	// records live under its own key (ListAttempts(…, ItemAt(i))).
	item := artifacts.ItemOf(task.Item)
	key := task.GetTaskId()
	if item.Set {
		key += "#" + item.String()
	}
	records, read := side.records[key]
	if !read {
		var err error
		records, err = s.attempts.ListAttempts(ctx, side.run.GetId(), task.GetTaskId(), item)
		if err != nil {
			return nil, "", connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read attempt records of task %q in run %q: %w", task.GetTaskId(), side.run.GetId(), err))
		}
		side.records[key] = records
	}
	record, ok := outcomeRecord(records, task.GetAttempt())
	if !ok {
		return nil, reasonAttemptRecordMissing, nil
	}
	return record, "", nil
}

// outcomeRecord picks the record that describes the bytes behind a task's
// outcome.
//
//  1. The record whose attempt is TaskRun.attempt — which, for an adopted
//     commit, is the COMMITTING attempt's number.
//  2. Failing that, the newest record that adopted a commit and succeeded.
//     An adopting retry files its record under its OWN attempt number,
//     carrying the committing attempt's executed image out of commit.json; a
//     crash after commit.json takes the committing record with it while the
//     commit survives, and the adopter's record is what is left that tells
//     the truth. Every adopter of one commit carries the same bytes
//     (commit.json is immutable under a deterministic key), so "newest" only
//     makes the choice deterministic.
//
// Any other record — an earlier failure, a cancellation — resolved its image
// on its own host and is never used.
func outcomeRecord(records []*orkstackv1.TaskAttempt, attempt int32) (*orkstackv1.TaskAttempt, bool) {
	var adopter *orkstackv1.TaskAttempt
	for _, record := range records {
		if record.GetAttempt() == attempt {
			return record, true
		}
		if record.GetCommitAdopted() &&
			record.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED &&
			(adopter == nil || record.GetAttempt() > adopter.GetAttempt()) {
			adopter = record
		}
	}
	return adopter, adopter != nil
}
