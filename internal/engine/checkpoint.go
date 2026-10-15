package engine

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sort"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ErrContinuationObjectInvalid reports a continuation object (a checkpoint, a
// switch snapshot or an event segment) whose bytes failed verification: a
// size, SHA-256, gzip, protobuf or structure check. It is never a
// NotFound, and never a degraded answer: an object that fails it is not used.
//
// Its text, and every error that matches it, speaks only the run's own
// vocabulary — "saved state", "earlier events" — and never names the object
// (whose key spells its generation and kind): these errors reach a client
// unchanged, and an archived gap keeps its reason forever (AGENTS.md §7).
var ErrContinuationObjectInvalid = errors.New("a stored part of the run failed verification")

// Subjects of a verification failure, as a user reads them.
const (
	subjectSavedState    = "saved state"    // a checkpoint or a switch snapshot
	subjectEarlierEvents = "earlier events" // an event segment
	subjectDefinition    = "definition"     // a definition passed by reference
)

// continuationInvalidError is one verification failure: what failed (a
// subject above) and the short detail of how, which a lost segment's gap
// records as its reason. It matches ErrContinuationObjectInvalid.
type continuationInvalidError struct {
	subject string
	detail  string
}

func (e *continuationInvalidError) Error() string {
	return "the run's " + e.subject + " failed verification: " + e.detail
}

func (e *continuationInvalidError) Is(target error) bool {
	return target == ErrContinuationObjectInvalid
}

func invalidContinuation(subject, format string, args ...any) error {
	return &continuationInvalidError{subject: subject, detail: fmt.Sprintf(format, args...)}
}

func invalidSavedState(format string, args ...any) error {
	return invalidContinuation(subjectSavedState, format, args...)
}

// CheckpointRun assembles the run a checkpoint holds from its parts, in
// stored order. The first part carries the header (the Run with task_runs
// cleared) and no other part may; task rows append in definition order, and
// each item row is attached to the fan-out row with the same task_id, in index
// order. A checkpoint whose parts were merged into one message (the whole
// object gunzipped as one multistream and unmarshalled once) passes as a
// single part.
//
// It refuses a missing header, a header on any part but the first, a
// duplicate task row, a duplicate (task_id, item), an item whose task is not
// an expanded fan-out, an index outside [0, count), and an expanded fan-out
// missing one of its items. That last refusal goes beyond the spec's list
// (1.1) and is sound because of two facts: an expansion creates all count
// item rows at once (each.go, expandEach) and nothing removes one from the
// run's state, and the switch writes its checkpoint over s.snapshot() with
// items included (spec 2.7 step 5). A checkpoint short of an item row is
// therefore never what a writer meant, and adopting it would restore a
// fan-out with an item no row tracks. Stage 2's writer must keep both facts.
func CheckpointRun(parts ...*orkstackv1.RunCheckpointPart) (*orkstackv1.Run, error) {
	if len(parts) == 0 || parts[0] == nil || parts[0].GetHeader() == nil {
		return nil, invalidSavedState("it has no run header")
	}
	run := proto.Clone(parts[0].GetHeader()).(*orkstackv1.Run)
	run.TaskRuns = nil
	rows := map[string]*orkstackv1.TaskRun{}
	var items []*orkstackv1.TaskRun
	for i, part := range parts {
		if part == nil {
			return nil, invalidSavedState("part %d is empty", i)
		}
		if i > 0 && part.GetHeader() != nil {
			return nil, invalidSavedState("part %d holds a run header; only part 0 may", i)
		}
		for _, row := range part.GetTaskRuns() {
			id := row.GetTaskId()
			if id == "" || row.Item != nil {
				return nil, invalidSavedState("task row %q is not a task row", id)
			}
			if _, dup := rows[id]; dup {
				return nil, invalidSavedState("it repeats task %q", id)
			}
			row = proto.Clone(row).(*orkstackv1.TaskRun)
			row.Items = nil
			rows[id] = row
			run.TaskRuns = append(run.TaskRuns, row)
		}
		items = append(items, part.GetItems()...)
	}
	seen := map[string]map[int32]bool{}
	for _, item := range items {
		id := item.GetTaskId()
		if item.Item == nil {
			return nil, invalidSavedState("an item row of task %q has no index", id)
		}
		index := item.GetItem()
		row, ok := rows[id]
		if !ok || !row.GetEach().GetExpanded() {
			return nil, invalidSavedState("item %d names task %q, which is not an expanded fan-out",
				index, id)
		}
		if index < 0 || index >= row.GetEach().GetCount() {
			return nil, invalidSavedState("item %d of task %q is outside [0, %d)",
				index, id, row.GetEach().GetCount())
		}
		if seen[id] == nil {
			seen[id] = map[int32]bool{}
		}
		if seen[id][index] {
			return nil, invalidSavedState("it repeats item %d of task %q", index, id)
		}
		seen[id][index] = true
		row.Items = append(row.Items, proto.Clone(item).(*orkstackv1.TaskRun))
	}
	for _, row := range run.TaskRuns {
		if !row.GetEach().GetExpanded() {
			continue
		}
		if count := row.GetEach().GetCount(); int32(len(row.Items)) != count {
			return nil, invalidSavedState("it holds %d of the %d items of task %q",
				len(row.Items), count, row.GetTaskId())
		}
		sort.Slice(row.Items, func(i, j int) bool { return row.Items[i].GetItem() < row.Items[j].GetItem() })
	}
	return run, nil
}

// DecodeCheckpoint verifies and decodes a whole stored checkpoint object
// against the reference that names it: its size, the SHA-256 of the whole
// object, then each part's offset, size, SHA-256 and decoded size, before any
// row is used. Every failure wraps ErrContinuationObjectInvalid.
//
// It decodes part by part rather than as the one multistream unmarshal spec
// 1.1 describes. The rows are the same — concatenated parts merge into one
// message — but a single unmarshal would merge a stray header on a later part
// into part 0's silently, so CheckpointRun could never refuse it, and it
// could not check each part's own SHA-256 either.
func DecodeCheckpoint(data []byte, obj *orkstackv1.ContinuationObject) (*orkstackv1.Run, error) {
	if err := verifyContinuationBytes(data, obj, subjectSavedState); err != nil {
		return nil, err
	}
	parts := obj.GetParts()
	if len(parts) == 0 {
		return nil, invalidSavedState("its reference names no parts")
	}
	decoded := make([]*orkstackv1.RunCheckpointPart, 0, len(parts))
	var offset, raw int64
	for i, part := range parts {
		if part.GetOffset() != offset || part.GetSizeBytes() <= 0 || part.GetOffset()+part.GetSizeBytes() > int64(len(data)) {
			return nil, invalidSavedState("part %d is not contiguous from offset 0", i)
		}
		member := data[part.GetOffset() : part.GetOffset()+part.GetSizeBytes()]
		if artifacts.ContinuationSHA256(member) != part.GetSha256() {
			return nil, invalidSavedState("part %d SHA-256 mismatch", i)
		}
		plain, err := gunzipBounded(member, part.GetRawBytes())
		if err != nil {
			return nil, invalidSavedState("part %d: %v", i, err)
		}
		message := &orkstackv1.RunCheckpointPart{}
		if err := proto.Unmarshal(plain, message); err != nil {
			return nil, invalidSavedState("part %d: %v", i, err)
		}
		decoded = append(decoded, message)
		offset += part.GetSizeBytes()
		raw += part.GetRawBytes()
	}
	if offset != int64(len(data)) {
		return nil, invalidSavedState("its parts cover %d of %d bytes", offset, len(data))
	}
	if obj.GetRawBytes() != raw {
		return nil, invalidSavedState("its parts decode to %d bytes, its reference records %d",
			raw, obj.GetRawBytes())
	}
	return CheckpointRun(decoded...)
}

// DecodeSwitchSnapshot verifies a stored switch snapshot against the
// reference that names it (size, SHA-256, decoded size), gunzips it and
// unmarshals one Run.
func DecodeSwitchSnapshot(data []byte, obj *orkstackv1.ContinuationObject) (*orkstackv1.Run, error) {
	if err := verifyContinuationBytes(data, obj, subjectSavedState); err != nil {
		return nil, err
	}
	plain, err := gunzipBounded(data, obj.GetRawBytes())
	if err != nil {
		return nil, invalidSavedState("%v", err)
	}
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(plain, run); err != nil {
		return nil, invalidSavedState("%v", err)
	}
	return run, nil
}

// DecodeEventSegment verifies a stored event segment against the reference
// that names it, gunzips it and unmarshals the RunEventSegment. The caller
// checks its run, generation and sequences.
func DecodeEventSegment(data []byte, obj *orkstackv1.ContinuationObject) (*orkstackv1.RunEventSegment, error) {
	if err := verifyContinuationBytes(data, obj, subjectEarlierEvents); err != nil {
		return nil, err
	}
	plain, err := gunzipBounded(data, obj.GetRawBytes())
	if err != nil {
		return nil, invalidContinuation(subjectEarlierEvents, "%v", err)
	}
	segment := &orkstackv1.RunEventSegment{}
	if err := proto.Unmarshal(plain, segment); err != nil {
		return nil, invalidContinuation(subjectEarlierEvents, "%v", err)
	}
	return segment, nil
}

// verifyContinuationBytes checks a whole stored object's size and SHA-256
// against its reference. It never names the object: subject says what failed.
func verifyContinuationBytes(data []byte, obj *orkstackv1.ContinuationObject, subject string) error {
	if obj == nil {
		return invalidContinuation(subject, "no reference names it")
	}
	if int64(len(data)) != obj.GetSizeBytes() {
		return invalidContinuation(subject, "%d bytes stored, its reference records %d", len(data), obj.GetSizeBytes())
	}
	if artifacts.ContinuationSHA256(data) != obj.GetSha256() {
		return invalidContinuation(subject, "SHA-256 mismatch")
	}
	return nil
}

// gunzipBounded decodes every gzip member of data (Go's multistream default)
// and requires exactly want decoded bytes, reading at most one byte more.
func gunzipBounded(data []byte, want int64) ([]byte, error) {
	if want < 0 {
		return nil, fmt.Errorf("negative decoded size %d", want)
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gunzip: %w", err)
	}
	plain, err := io.ReadAll(io.LimitReader(reader, want+1))
	if err != nil {
		return nil, fmt.Errorf("gunzip: %w", err)
	}
	if int64(len(plain)) != want {
		return nil, fmt.Errorf("decodes to more or fewer than the %d bytes recorded", want)
	}
	return plain, nil
}
