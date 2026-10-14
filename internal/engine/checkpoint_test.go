package engine

import (
	"bytes"
	"compress/gzip"
	"errors"
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
	cases := map[string]struct {
		data []byte
		obj  *orkstackv1.ContinuationObject
	}{
		"short":             {data: data[:len(data)-1], obj: obj},
		"a byte changed":    {data: flipped, obj: obj},
		"no parts":          {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts = nil })},
		"part gap":          {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts[1].Offset++ })},
		"part digest":       {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts[0].Sha256 = strings.Repeat("0", 64) })},
		"part raw size":     {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts[0].RawBytes-- })},
		"parts short":       {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.Parts = o.Parts[:len(o.Parts)-1] })},
		"recorded raw size": {data: data, obj: mutate(func(o *orkstackv1.ContinuationObject) { o.RawBytes++ })},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeCheckpoint(tc.data, tc.obj); !errors.Is(err, ErrContinuationObjectInvalid) {
				t.Fatalf("DecodeCheckpoint err = %v, want ErrContinuationObjectInvalid", err)
			} else if errors.Is(err, artifacts.ErrNotFound) {
				t.Fatalf("a verification failure reads as NotFound: %v", err)
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
	}{
		"size":          {data: append(bytes.Clone(data), 0), obj: obj},
		"hash":          {data: flipped, obj: obj},
		"decoded size":  {data: data, obj: other},
		"no reference":  {data: data, obj: nil},
		"not gzip data": {data: []byte("plain"), obj: &orkstackv1.ContinuationObject{SizeBytes: 5, Sha256: artifacts.ContinuationSHA256([]byte("plain"))}},
	} {
		if _, err := DecodeSwitchSnapshot(tc.data, tc.obj); !errors.Is(err, ErrContinuationObjectInvalid) {
			t.Fatalf("%s: err = %v, want ErrContinuationObjectInvalid", name, err)
		}
	}
}
