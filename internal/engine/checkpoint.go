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
var ErrContinuationObjectInvalid = errors.New("continuation object failed verification")

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
// missing one of its items.
func CheckpointRun(parts ...*orkstackv1.RunCheckpointPart) (*orkstackv1.Run, error) {
	if len(parts) == 0 || parts[0] == nil || parts[0].GetHeader() == nil {
		return nil, fmt.Errorf("%w: checkpoint has no header", ErrContinuationObjectInvalid)
	}
	run := proto.Clone(parts[0].GetHeader()).(*orkstackv1.Run)
	run.TaskRuns = nil
	rows := map[string]*orkstackv1.TaskRun{}
	var items []*orkstackv1.TaskRun
	for i, part := range parts {
		if part == nil {
			return nil, fmt.Errorf("%w: checkpoint part %d is empty", ErrContinuationObjectInvalid, i)
		}
		if i > 0 && part.GetHeader() != nil {
			return nil, fmt.Errorf("%w: checkpoint part %d carries a header; only part 0 may", ErrContinuationObjectInvalid, i)
		}
		for _, row := range part.GetTaskRuns() {
			id := row.GetTaskId()
			if id == "" || row.Item != nil {
				return nil, fmt.Errorf("%w: checkpoint task row %q is not a task row", ErrContinuationObjectInvalid, id)
			}
			if _, dup := rows[id]; dup {
				return nil, fmt.Errorf("%w: checkpoint repeats task %q", ErrContinuationObjectInvalid, id)
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
			return nil, fmt.Errorf("%w: checkpoint item row of task %q has no index", ErrContinuationObjectInvalid, id)
		}
		index := item.GetItem()
		row, ok := rows[id]
		if !ok || !row.GetEach().GetExpanded() {
			return nil, fmt.Errorf("%w: checkpoint item %d names task %q, which is not an expanded fan-out",
				ErrContinuationObjectInvalid, index, id)
		}
		if index < 0 || index >= row.GetEach().GetCount() {
			return nil, fmt.Errorf("%w: checkpoint item %d of task %q is outside [0, %d)",
				ErrContinuationObjectInvalid, index, id, row.GetEach().GetCount())
		}
		if seen[id] == nil {
			seen[id] = map[int32]bool{}
		}
		if seen[id][index] {
			return nil, fmt.Errorf("%w: checkpoint repeats item %d of task %q", ErrContinuationObjectInvalid, index, id)
		}
		seen[id][index] = true
		row.Items = append(row.Items, proto.Clone(item).(*orkstackv1.TaskRun))
	}
	for _, row := range run.TaskRuns {
		if !row.GetEach().GetExpanded() {
			continue
		}
		if count := row.GetEach().GetCount(); int32(len(row.Items)) != count {
			return nil, fmt.Errorf("%w: checkpoint holds %d of the %d items of task %q",
				ErrContinuationObjectInvalid, len(row.Items), count, row.GetTaskId())
		}
		sort.Slice(row.Items, func(i, j int) bool { return row.Items[i].GetItem() < row.Items[j].GetItem() })
	}
	return run, nil
}

// DecodeCheckpoint verifies and decodes a whole stored checkpoint object
// against the reference that names it: its size, the SHA-256 of the whole
// object, then each part's offset, size, SHA-256 and decoded size, before any
// row is used. Every failure wraps ErrContinuationObjectInvalid.
func DecodeCheckpoint(data []byte, obj *orkstackv1.ContinuationObject) (*orkstackv1.Run, error) {
	if err := verifyContinuationBytes(data, obj); err != nil {
		return nil, err
	}
	parts := obj.GetParts()
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: checkpoint reference names no parts", ErrContinuationObjectInvalid)
	}
	decoded := make([]*orkstackv1.RunCheckpointPart, 0, len(parts))
	var offset, raw int64
	for i, part := range parts {
		if part.GetOffset() != offset || part.GetSizeBytes() <= 0 || part.GetOffset()+part.GetSizeBytes() > int64(len(data)) {
			return nil, fmt.Errorf("%w: checkpoint part %d is not contiguous from offset 0", ErrContinuationObjectInvalid, i)
		}
		member := data[part.GetOffset() : part.GetOffset()+part.GetSizeBytes()]
		if artifacts.ContinuationSHA256(member) != part.GetSha256() {
			return nil, fmt.Errorf("%w: checkpoint part %d SHA-256 mismatch", ErrContinuationObjectInvalid, i)
		}
		plain, err := gunzipBounded(member, part.GetRawBytes())
		if err != nil {
			return nil, fmt.Errorf("%w: checkpoint part %d: %v", ErrContinuationObjectInvalid, i, err)
		}
		message := &orkstackv1.RunCheckpointPart{}
		if err := proto.Unmarshal(plain, message); err != nil {
			return nil, fmt.Errorf("%w: checkpoint part %d: %v", ErrContinuationObjectInvalid, i, err)
		}
		decoded = append(decoded, message)
		offset += part.GetSizeBytes()
		raw += part.GetRawBytes()
	}
	if offset != int64(len(data)) {
		return nil, fmt.Errorf("%w: checkpoint parts cover %d of %d bytes", ErrContinuationObjectInvalid, offset, len(data))
	}
	if obj.GetRawBytes() != raw {
		return nil, fmt.Errorf("%w: checkpoint parts decode to %d bytes, the reference records %d",
			ErrContinuationObjectInvalid, raw, obj.GetRawBytes())
	}
	return CheckpointRun(decoded...)
}

// DecodeSwitchSnapshot verifies a stored switch snapshot against the
// reference that names it (size, SHA-256, decoded size), gunzips it and
// unmarshals one Run.
func DecodeSwitchSnapshot(data []byte, obj *orkstackv1.ContinuationObject) (*orkstackv1.Run, error) {
	if err := verifyContinuationBytes(data, obj); err != nil {
		return nil, err
	}
	plain, err := gunzipBounded(data, obj.GetRawBytes())
	if err != nil {
		return nil, fmt.Errorf("%w: switch snapshot: %v", ErrContinuationObjectInvalid, err)
	}
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(plain, run); err != nil {
		return nil, fmt.Errorf("%w: switch snapshot: %v", ErrContinuationObjectInvalid, err)
	}
	return run, nil
}

// DecodeEventSegment verifies a stored event segment against the reference
// that names it, gunzips it and unmarshals the RunEventSegment. The caller
// checks its run, generation and sequences.
func DecodeEventSegment(data []byte, obj *orkstackv1.ContinuationObject) (*orkstackv1.RunEventSegment, error) {
	if err := verifyContinuationBytes(data, obj); err != nil {
		return nil, err
	}
	plain, err := gunzipBounded(data, obj.GetRawBytes())
	if err != nil {
		return nil, fmt.Errorf("%w: event segment: %v", ErrContinuationObjectInvalid, err)
	}
	segment := &orkstackv1.RunEventSegment{}
	if err := proto.Unmarshal(plain, segment); err != nil {
		return nil, fmt.Errorf("%w: event segment: %v", ErrContinuationObjectInvalid, err)
	}
	return segment, nil
}

// verifyContinuationBytes checks a whole stored object's size and SHA-256
// against its reference.
func verifyContinuationBytes(data []byte, obj *orkstackv1.ContinuationObject) error {
	if obj == nil {
		return fmt.Errorf("%w: no reference", ErrContinuationObjectInvalid)
	}
	if int64(len(data)) != obj.GetSizeBytes() {
		return fmt.Errorf("%w: %s is %d bytes, its reference records %d",
			ErrContinuationObjectInvalid, obj.GetUri(), len(data), obj.GetSizeBytes())
	}
	if artifacts.ContinuationSHA256(data) != obj.GetSha256() {
		return fmt.Errorf("%w: %s SHA-256 mismatch", ErrContinuationObjectInvalid, obj.GetUri())
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
