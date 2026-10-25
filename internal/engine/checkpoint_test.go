package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// checkpointRunForTest is a run with a plain task, an expanded fan-out of
// three items (one succeeded, two pending) and a pending task.
func checkpointRunForTest() *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:          "ork-1-2",
		Namespace:   "team-a",
		OrkflowName: "pipeline",
		Status:      orkstackv1.Status_STATUS_RUNNING,
		CreatedAt:   timestamppb.New(timestamppb.Now().AsTime().Truncate(1e9)),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED, ValuesRef: &orkstackv1.ArtifactRef{Uri: "s3://x/values.json"}},
			{TaskId: "fan", Status: orkstackv1.Status_STATUS_RUNNING,
				Each: &orkstackv1.TaskEach{Expanded: true, Count: 3},
				Items: []*orkstackv1.TaskRun{
					{TaskId: "fan", Item: proto.Int32(0), Status: orkstackv1.Status_STATUS_SUCCEEDED},
					{TaskId: "fan", Item: proto.Int32(1), Status: orkstackv1.Status_STATUS_PENDING},
					{TaskId: "fan", Item: proto.Int32(2), Status: orkstackv1.Status_STATUS_PENDING},
				}},
			{TaskId: "publish", Status: orkstackv1.Status_STATUS_PENDING},
		},
	}
}

func TestCheckpointRun_ConcatenatedPartsDecodeAsOne(t *testing.T) {
	run := checkpointRunForTest()
	for _, rowsPerPart := range []int{1, 2, 100} {
		parts := checkpointPartsForTest(run, rowsPerPart)
		data, obj := checkpointObjectForTest(t, parts, "team-a", "ork-1-2", 0)

		// Readers: the whole object gunzipped as one multistream and
		// unmarshalled once.
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		plain, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		merged := &orkstackv1.RunCheckpointPart{}
		if err := proto.Unmarshal(plain, merged); err != nil {
			t.Fatal(err)
		}
		whole, err := CheckpointRun(merged)
		if err != nil {
			t.Fatalf("CheckpointRun(merged): %v", err)
		}
		// A generation: part by part.
		byPart, err := CheckpointRun(parts...)
		if err != nil {
			t.Fatalf("CheckpointRun(parts...): %v", err)
		}
		decoded, err := DecodeCheckpoint(data, obj)
		if err != nil {
			t.Fatalf("DecodeCheckpoint: %v", err)
		}
		for name, got := range map[string]*orkstackv1.Run{"merged": whole, "by part": byPart, "decoded": decoded} {
			if !proto.Equal(got, run) {
				t.Fatalf("%d rows per part, %s: decoded run differs from the original\n got %v\nwant %v", rowsPerPart, name, got, run)
			}
		}
	}
}

func TestCheckpointRun_RefusesDuplicateOrOrphanItems(t *testing.T) {
	header := &orkstackv1.Run{Id: "ork-1-2"}
	fan := func() *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: "fan", Each: &orkstackv1.TaskEach{Expanded: true, Count: 2}}
	}
	item := func(task string, i int32) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: task, Item: proto.Int32(i), Status: orkstackv1.Status_STATUS_PENDING}
	}
	cases := []struct {
		name string
		part *orkstackv1.RunCheckpointPart
		want string
	}{
		{name: "duplicate item", want: "repeats item",
			part: &orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{fan()}, Items: []*orkstackv1.TaskRun{item("fan", 0), item("fan", 0)}}},
		{name: "item of a plain task", want: "not an expanded fan-out",
			part: &orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{{TaskId: "plain"}}, Items: []*orkstackv1.TaskRun{item("plain", 0)}}},
		{name: "item of an unknown task", want: "not an expanded fan-out",
			part: &orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{fan()}, Items: []*orkstackv1.TaskRun{item("other", 0)}}},
		{name: "index at count", want: "outside [0, 2)",
			part: &orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{fan()}, Items: []*orkstackv1.TaskRun{item("fan", 0), item("fan", 2)}}},
		{name: "negative index", want: "outside [0, 2)",
			part: &orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{fan()}, Items: []*orkstackv1.TaskRun{item("fan", -1)}}},
		{name: "missing item", want: "holds 1 of the 2 items",
			part: &orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{fan()}, Items: []*orkstackv1.TaskRun{item("fan", 1)}}},
		{name: "duplicate task", want: "repeats task",
			part: &orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{{TaskId: "a"}, {TaskId: "a"}}}},
		{name: "item without index", want: "has no index",
			part: &orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{fan()}, Items: []*orkstackv1.TaskRun{{TaskId: "fan"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CheckpointRun(tc.part)
			if !errors.Is(err, ErrContinuationObjectInvalid) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckpointRun err = %v, want ErrContinuationObjectInvalid naming %q", err, tc.want)
			}
		})
	}
	// The items arrive out of order: they are attached in index order.
	run, err := CheckpointRun(&orkstackv1.RunCheckpointPart{Header: header, TaskRuns: []*orkstackv1.TaskRun{fan()}, Items: []*orkstackv1.TaskRun{item("fan", 1), item("fan", 0)}})
	if err != nil {
		t.Fatal(err)
	}
	if got := run.GetTaskRuns()[0].GetItems(); got[0].GetItem() != 0 || got[1].GetItem() != 1 {
		t.Fatalf("items attached out of index order: %v", got)
	}
}

func TestCheckpointRun_HeaderOnlyInPartZero(t *testing.T) {
	run := checkpointRunForTest()
	parts := checkpointPartsForTest(run, 1)
	if len(parts) < 3 {
		t.Fatalf("want several parts, got %d", len(parts))
	}
	if _, err := CheckpointRun(parts[1:]...); !errors.Is(err, ErrContinuationObjectInvalid) {
		t.Fatalf("no header: err = %v", err)
	}
	parts[2].Header = &orkstackv1.Run{Id: "ork-1-2"}
	if _, err := CheckpointRun(parts...); err == nil || !strings.Contains(err.Error(), "part 2 holds a run header") {
		t.Fatalf("header on part 2: err = %v", err)
	}
	// The stored object is refused the same way.
	data, obj := checkpointObjectForTest(t, parts, "team-a", "ork-1-2", 0)
	if _, err := DecodeCheckpoint(data, obj); err == nil || !strings.Contains(err.Error(), "part 2 holds a run header") {
		t.Fatalf("DecodeCheckpoint with a header on part 2: err = %v", err)
	}
}

func TestDecodeCheckpoint_VerifiesSizeHashAndParts(t *testing.T) {
	run := checkpointRunForTest()
	data, obj := checkpointObjectForTest(t, checkpointPartsForTest(run, 2), "team-a", "ork-1-2", 0)
	mutate := func(f func(o *orkstackv1.ContinuationObject)) *orkstackv1.ContinuationObject {
		c := proto.Clone(obj).(*orkstackv1.ContinuationObject)
		f(c)
		return c
	}
	flipped := bytes.Clone(data)
	flipped[len(flipped)/2] ^= 0xff
	// The checks overlap on purpose (a gap is also a digest mismatch), so each
	// case names the refusal that must fire FIRST: the run's terminal message
	// says what was wrong, and a guard that silently stopped checking would
	// otherwise hand its case to the next one.
	cases := map[string]struct {
		data []byte
		obj  *orkstackv1.ContinuationObject
		want string
	}{
		"short":             {data: data[:len(data)-1], obj: obj, want: fmt.Sprintf("%d bytes stored, its reference records %d", len(data)-1, len(data))},
		"a byte changed":    {data: flipped, obj: obj, want: "failed verification: SHA-256 mismatch"},
		"no reference":      {data: data, obj: nil, want: "no reference names it"},
		"no parts":          {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts = nil }), want: "its reference names no parts"},
		"part gap":          {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts[1].Offset++ }), want: "part 1 is not contiguous from offset 0"},
		"empty part":        {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts[0].SizeBytes = 0 }), want: "part 0 is not contiguous from offset 0"},
		"part past the end": {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts[len(o.Parts)-1].SizeBytes++ }), want: fmt.Sprintf("part %d is not contiguous from offset 0", len(obj.GetParts())-1)},
		"part digest":       {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts[0].Sha256 = strings.Repeat("0", 64) }), want: "part 0 SHA-256 mismatch"},
		"part raw size":     {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts[0].RawBytes-- }), want: "part 0: decodes to more or fewer than the"},
		"parts short":       {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts = o.Parts[:len(o.Parts)-1] }), want: fmt.Sprintf("its parts cover %d of %d bytes", obj.GetParts()[len(obj.GetParts())-1].GetOffset(), len(data))},
		"recorded raw size": {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.RawBytes++ }), want: fmt.Sprintf("its parts decode to %d bytes, its reference records %d", obj.GetRawBytes(), obj.GetRawBytes()+1)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeCheckpoint(tc.data, tc.obj)
			if !errors.Is(err, ErrContinuationObjectInvalid) {
				t.Fatalf("DecodeCheckpoint err = %v, want ErrContinuationObjectInvalid", err)
			}
			if errors.Is(err, artifacts.ErrNotFound) {
				t.Fatalf("a verification failure reads as NotFound: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeCheckpoint err = %q, want the refusal %q", err, tc.want)
			}
		})
	}
}

func TestDecodeSwitchSnapshot_VerifiesSizeAndHash(t *testing.T) {
	run := checkpointRunForTest()
	data, obj := gzipObjectForTest(t, run, "team-a", "ork-1-2", 0, artifacts.ContinuationKindSnapshot)
	got, err := DecodeSwitchSnapshot(data, obj)
	if err != nil {
		t.Fatalf("DecodeSwitchSnapshot: %v", err)
	}
	if !proto.Equal(got, run) {
		t.Fatalf("decoded snapshot differs: %v", got)
	}
	flipped := bytes.Clone(data)
	flipped[len(flipped)-1] ^= 0xff
	other := proto.Clone(obj).(*orkstackv1.ContinuationObject)
	other.RawBytes++
	for name, tc := range map[string]struct {
		data []byte
		obj  *orkstackv1.ContinuationObject
		want string
	}{
		"size":          {data: append(bytes.Clone(data), 0), obj: obj, want: fmt.Sprintf("%d bytes stored, its reference records %d", len(data)+1, len(data))},
		"hash":          {data: flipped, obj: obj, want: "SHA-256 mismatch"},
		"decoded size":  {data: data, obj: other, want: "decodes to more or fewer than the"},
		"no reference":  {data: data, obj: nil, want: "no reference names it"},
		"not gzip data": {data: []byte("plain"), obj: &orkstackv1.ContinuationObject{SizeBytes: 5, Sha256: artifacts.ContinuationSHA256([]byte("plain"))}, want: "gunzip:"},
	} {
		_, err := DecodeSwitchSnapshot(tc.data, tc.obj)
		if !errors.Is(err, ErrContinuationObjectInvalid) {
			t.Fatalf("%s: err = %v, want ErrContinuationObjectInvalid", name, err)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %q, want the refusal %q", name, err, tc.want)
		}
	}
}

// checkpointParts is what bounds each part of a saved state (§2.2, §7.3): a
// part closes before a row that would take it past checkpointPartRawBytes,
// a row larger than that goes alone, no part is empty, only part 0 holds the
// header, and the parts reassemble into exactly the run they came from.
func TestCheckpointParts_BoundsEveryPartAndReassembles(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-1-2", Namespace: "team-a", Status: orkstackv1.Status_STATUS_RUNNING}
	uri := func(i, n int) string { return fmt.Sprintf("s3://x/%06d/", i) + strings.Repeat("v", n) }
	// Many small rows, so an accounting error per row adds up past the bound.
	for i := 0; i < 30000; i++ {
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{TaskId: fmt.Sprintf("t%05d", i),
			Status: orkstackv1.Status_STATUS_SUCCEEDED, ValuesRef: &orkstackv1.ArtifactRef{Uri: uri(i, 40)}})
	}
	huge := &orkstackv1.TaskRun{TaskId: "huge", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		ValuesRef: &orkstackv1.ArtifactRef{Uri: uri(0, checkpointPartRawBytes+1)}}
	run.TaskRuns = append(run.TaskRuns, huge)
	fan := &orkstackv1.TaskRun{TaskId: "fan", Status: orkstackv1.Status_STATUS_RUNNING,
		Each: &orkstackv1.TaskEach{Expanded: true, Count: 3000}}
	for i := int32(0); i < 3000; i++ {
		fan.Items = append(fan.Items, &orkstackv1.TaskRun{TaskId: "fan", Item: proto.Int32(i),
			Status: orkstackv1.Status_STATUS_PENDING, ValuesRef: &orkstackv1.ArtifactRef{Uri: uri(int(i), 300)}})
	}
	run.TaskRuns = append(run.TaskRuns, fan)

	parts := checkpointParts(run)
	if len(parts) < 4 {
		t.Fatalf("%d parts for a run of %d bytes, want at least 4", len(parts), proto.Size(run))
	}
	alone := 0
	for i, part := range parts {
		if (part.GetHeader() != nil) != (i == 0) {
			t.Fatalf("part %d header present = %v; only part 0 holds it", i, part.GetHeader() != nil)
		}
		rows := len(part.GetTaskRuns()) + len(part.GetItems())
		if rows == 0 && i > 0 {
			t.Fatalf("part %d is empty", i)
		}
		if size := proto.Size(part); size > checkpointPartRawBytes {
			if rows != 1 || len(part.GetTaskRuns()) != 1 || part.GetTaskRuns()[0].GetTaskId() != "huge" {
				t.Fatalf("part %d is %d bytes with %d rows, over the %d-byte bound", i, size, rows, checkpointPartRawBytes)
			}
			alone++
		}
	}
	if alone != 1 {
		t.Fatalf("%d parts over the bound, want exactly the one holding the oversized row alone", alone)
	}
	before := make([]*orkstackv1.RunCheckpointPart, len(parts))
	for i, part := range parts {
		before[i] = proto.Clone(part).(*orkstackv1.RunCheckpointPart)
	}
	back, err := CheckpointRun(parts...)
	if err != nil {
		t.Fatalf("CheckpointRun: %v", err)
	}
	if !proto.Equal(back, run) {
		t.Fatal("the parts do not reassemble into the run they came from")
	}
	for i := range parts {
		if !proto.Equal(parts[i], before[i]) {
			t.Fatalf("CheckpointRun changed part %d it was given", i)
		}
	}
}

// Each malformed part list is refused by its own check, with the run still
// named in the run's own vocabulary: a missing header (no parts at all, or a
// nil first part), a header on any part but the first, a nil part, and a
// task row that is not one.
func TestCheckpointRun_RefusesMalformedParts(t *testing.T) {
	parts := checkpointPartsForTest(checkpointRunForTest(), 1)
	clone := func() []*orkstackv1.RunCheckpointPart {
		out := make([]*orkstackv1.RunCheckpointPart, len(parts))
		for i, part := range parts {
			out[i] = proto.Clone(part).(*orkstackv1.RunCheckpointPart)
		}
		return out
	}
	headerOnOne := clone()
	headerOnOne[1].Header = &orkstackv1.Run{Id: "ork-1-2"}
	nilPart := clone()
	nilPart[2] = nil
	noID := clone()
	noID[1].TaskRuns[0].TaskId = ""
	itemAsTask := clone()
	itemAsTask[1].TaskRuns[0].Item = proto.Int32(0)
	var itemPart int
	for i, part := range parts {
		if len(part.GetItems()) > 0 {
			itemPart = i
			break
		}
	}
	noIndex := clone()
	noIndex[itemPart].Items[0].Item = nil
	for name, tc := range map[string]struct {
		parts []*orkstackv1.RunCheckpointPart
		want  string
	}{
		"no parts":              {parts: nil, want: "it has no run header"},
		"a nil first part":      {parts: append([]*orkstackv1.RunCheckpointPart{nil}, parts[1:]...), want: "it has no run header"},
		"a header on part 1":    {parts: headerOnOne, want: "part 1 holds a run header; only part 0 may"},
		"a nil part":            {parts: nilPart, want: "part 2 is empty"},
		"a row with no id":      {parts: noID, want: `task row "" is not a task row`},
		"an item as a task row": {parts: itemAsTask, want: fmt.Sprintf("task row %q is not a task row", parts[1].GetTaskRuns()[0].GetTaskId())},
		"an item with no index": {parts: noIndex, want: "has no index"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := CheckpointRun(tc.parts...)
			if !errors.Is(err, ErrContinuationObjectInvalid) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckpointRun err = %v, want ErrContinuationObjectInvalid naming %q", err, tc.want)
			}
		})
	}
}

// An event segment is verified against its reference before it is decoded:
// a digest that differs while the size and decoded size still match is
// refused as SHA-256, not decoded, and a negative decoded size is refused as
// such rather than read.
func TestDecodeEventSegment_VerifiesSizeAndHash(t *testing.T) {
	segment := &orkstackv1.RunEventSegment{RunId: "ork-1-2"}
	data, obj := gzipObjectForTest(t, segment, "team-a", "ork-1-2", 0, artifacts.ContinuationKindEvents)
	got, err := DecodeEventSegment(data, obj)
	if err != nil || !proto.Equal(got, segment) {
		t.Fatalf("DecodeEventSegment = %v, %v; want the segment", got, err)
	}
	digest := proto.Clone(obj).(*orkstackv1.ContinuationObject)
	digest.Sha256 = strings.Repeat("0", 64)
	negative := proto.Clone(obj).(*orkstackv1.ContinuationObject)
	negative.RawBytes = -1
	for name, tc := range map[string]struct {
		data []byte
		obj  *orkstackv1.ContinuationObject
		want string
	}{
		"digest":                {data: data, obj: digest, want: "the run's earlier events failed verification: SHA-256 mismatch"},
		"size":                  {data: data[1:], obj: obj, want: fmt.Sprintf("%d bytes stored, its reference records %d", len(data)-1, len(data))},
		"no reference":          {data: data, obj: nil, want: "no reference names it"},
		"negative decoded size": {data: data, obj: negative, want: "negative decoded size -1"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeEventSegment(tc.data, tc.obj)
			if !errors.Is(err, ErrContinuationObjectInvalid) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeEventSegment err = %v, want ErrContinuationObjectInvalid naming %q", err, tc.want)
			}
		})
	}
}

// The cap is inclusive: a checkpoint of exactly capBytes is written, one
// byte over it is not (and comes back with its size and no URI).
func TestWriteCheckpoint_TheCapIsInclusive(t *testing.T) {
	run := checkpointRunForTest()
	in := ContinuationObservationInput{Namespace: "team-a"}
	sized, err := NewSnapshotActivities(newMemSnapshotStore()).WriteCheckpoint(context.Background(), run, 1, 1<<30, in)
	if err != nil || sized.GetUri() == "" {
		t.Fatalf("WriteCheckpoint = %v, %v; want it written", sized, err)
	}
	at, err := NewSnapshotActivities(newMemSnapshotStore()).WriteCheckpoint(context.Background(), run, 1, sized.GetSizeBytes(), in)
	if err != nil || at.GetUri() == "" || at.GetSizeBytes() != sized.GetSizeBytes() {
		t.Fatalf("a checkpoint of exactly its cap = %v, %v; want it written", at, err)
	}
	over, err := NewSnapshotActivities(newMemSnapshotStore()).WriteCheckpoint(context.Background(), run, 1, sized.GetSizeBytes()-1, in)
	if err != nil || over.GetUri() != "" || over.GetSizeBytes() != sized.GetSizeBytes() {
		t.Fatalf("a checkpoint one byte over its cap = %v, %v; want its size and no URI", over, err)
	}
}
