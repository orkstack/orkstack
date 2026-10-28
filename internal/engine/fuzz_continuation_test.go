package engine

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A run's saved state, its earlier events and a definition passed by
// reference are objects read back from object storage and used only after
// verification (AGENTS.md §7.3). The contract is that a bad object is refused
// by name — every failure wraps ErrContinuationObjectInvalid and names no
// object — and that a good one reads back as what was written. These targets
// state both against the writer the engine uses.

// requireNamedRefusal checks err is the continuation refusal in the run's
// vocabulary.
func requireNamedRefusal(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, ErrContinuationObjectInvalid) {
		t.Fatalf("%s: err = %T %v, want one that wraps ErrContinuationObjectInvalid", what, err, err)
	}
	for _, leak := range []string{"s3://", "file://", "continuation", ".json", "/"} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("%s: %q names an object (%q): errors speak only the run's own vocabulary", what, err, leak)
		}
	}
}

// savedStateRun is a run with plain tasks and an expanded fan-out whose shape
// the fuzz arguments vary.
func savedStateRun(plain, items uint8) *orkstackv1.Run {
	run := checkpointRunForTest()
	run.TaskRuns = nil
	for i := 0; i < int(plain%4); i++ {
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{TaskId: "plain" + string(rune('a'+i)), Status: orkstackv1.Status_STATUS_SUCCEEDED})
	}
	if count := int32(items % 4); count > 0 {
		fan := &orkstackv1.TaskRun{TaskId: "fan", Status: orkstackv1.Status_STATUS_RUNNING, Each: &orkstackv1.TaskEach{Expanded: true, Count: count}}
		for i := int32(0); i < count; i++ {
			fan.Items = append(fan.Items, &orkstackv1.TaskRun{TaskId: "fan", Item: proto.Int32(i), Status: orkstackv1.Status_STATUS_PENDING})
		}
		run.TaskRuns = append(run.TaskRuns, fan)
	}
	return run
}

// FuzzDecodeCheckpoint: a checkpoint written by the engine's part format
// reads back as the run it holds, however its rows are split into parts; one
// byte changed anywhere in it is refused whether or not its reference is
// made to agree; a reference that lies about its sizes or parts is refused; and
// arbitrary bytes dressed in an honest reference are refused or yield a run
// whose fan-outs are whole.
func FuzzDecodeCheckpoint(f *testing.F) {
	f.Add(uint8(2), uint8(3), uint8(2), uint8(0), uint32(0), byte(1), []byte(nil))
	f.Add(uint8(0), uint8(0), uint8(1), uint8(1), uint32(7), byte(0xff), []byte(nil))
	f.Add(uint8(1), uint8(2), uint8(4), uint8(2), uint32(40), byte(1), []byte(nil))
	f.Add(uint8(1), uint8(2), uint8(1), uint8(3), uint32(0), byte(3), []byte(nil))
	f.Add(uint8(1), uint8(2), uint8(1), uint8(4), uint32(2), byte(1), []byte{0x0a, 0x00})
	f.Add(uint8(1), uint8(2), uint8(1), uint8(4), uint32(0), byte(1), []byte{0x12, 0x02, 0x0a, 0x00, 0x0a, 0x00})
	f.Add(uint8(1), uint8(2), uint8(1), uint8(4), uint32(0), byte(1), []byte{0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, plain, items, rowsPerPart, mode uint8, at uint32, delta byte, junk []byte) {
		run := savedStateRun(plain, items)
		parts := checkpointPartsForTest(run, int(rowsPerPart%5)+1)
		data, obj := checkpointObjectForTest(t, parts, "team-a", "ork-1-2", 0)
		delta |= 1

		switch mode % 5 {
		case 0: // as written
			got, err := DecodeCheckpoint(data, obj)
			if err != nil || !proto.Equal(got, run) {
				t.Fatalf("a checkpoint of %v read back as %v (%v)", run, got, err)
			}
		case 1: // a byte changed, the reference unchanged
			altered := append([]byte(nil), data...)
			altered[int(at)%len(altered)] ^= delta
			_, err := DecodeCheckpoint(altered, obj)
			requireNamedRefusal(t, "a changed byte", err)
		case 2: // a byte changed and the reference made to agree with the whole object
			altered := append([]byte(nil), data...)
			altered[int(at)%len(altered)] ^= delta
			fixed := proto.Clone(obj).(*orkstackv1.ContinuationObject)
			fixed.Sha256 = artifacts.ContinuationSHA256(altered)
			_, err := DecodeCheckpoint(altered, fixed)
			requireNamedRefusal(t, "a changed byte with a matching whole-object digest", err)
		case 3: // the reference lies
			lied := proto.Clone(obj).(*orkstackv1.ContinuationObject)
			switch int(at) % 5 {
			case 0:
				lied.RawBytes += int64(delta)
			case 1:
				lied.SizeBytes += int64(delta)
			case 2:
				lied.Parts[0].RawBytes += int64(delta)
			case 3:
				lied.Parts[len(lied.Parts)-1].Offset += int64(delta)
			case 4:
				lied.Parts = nil
			}
			_, err := DecodeCheckpoint(data, lied)
			requireNamedRefusal(t, "a reference that lies", err)
			if _, err := DecodeCheckpoint(data, nil); err == nil {
				t.Fatal("a checkpoint with no reference was read")
			}
		case 4: // arbitrary bytes as one part, under an honest reference
			member := gzipForTest(t, junk)
			honest := &orkstackv1.ContinuationObject{
				SizeBytes: int64(len(member)), RawBytes: int64(len(junk)), Sha256: artifacts.ContinuationSHA256(member),
				Parts: []*orkstackv1.ContinuationPart{{SizeBytes: int64(len(member)), RawBytes: int64(len(junk)), Sha256: artifacts.ContinuationSHA256(member)}},
			}
			got, err := DecodeCheckpoint(member, honest)
			if err != nil {
				requireNamedRefusal(t, "arbitrary bytes", err)
				return
			}
			for _, row := range got.GetTaskRuns() {
				if row.GetEach().GetExpanded() && int32(len(row.GetItems())) != row.GetEach().GetCount() {
					t.Fatalf("a restored fan-out %q holds %d of its %d items", row.GetTaskId(), len(row.GetItems()), row.GetEach().GetCount())
				}
			}
		}
	})
}

// FuzzDecodeDefinitionObject: a definition passed by reference reads back as
// itself against its stub; a byte changed in it, a reference that lies, or
// another version's stub is refused by name.
func FuzzDecodeDefinitionObject(f *testing.F) {
	f.Add(uint8(3), uint8(0), uint32(0), byte(1))
	f.Add(uint8(1), uint8(1), uint32(9), byte(0xff))
	f.Add(uint8(2), uint8(2), uint32(1), byte(1))
	f.Add(uint8(2), uint8(3), uint32(0), byte(4))
	f.Add(uint8(2), uint8(4), uint32(0), byte(1))
	f.Fuzz(func(t *testing.T, tasks, mode uint8, at uint32, delta byte) {
		planned := largeDefinition(t, int(tasks%5)+1, 64)
		planned.Tasks[0].With, _ = structpb.NewStruct(map[string]any{"op": "exec"})
		data, ref, err := EncodeDefinitionObject(planned)
		if err != nil {
			t.Fatal(err)
		}
		stub := DefinitionStub(planned)
		delta |= 1
		switch mode % 5 {
		case 0:
			got, err := DecodeDefinitionObject(data, &ref, stub)
			if err != nil || !proto.Equal(got, planned) {
				t.Fatalf("a definition read back as %v (%v)", got, err)
			}
		case 1:
			altered := append([]byte(nil), data...)
			altered[int(at)%len(altered)] ^= delta
			_, err := DecodeDefinitionObject(altered, &ref, stub)
			requireNamedRefusal(t, "a changed byte", err)
		case 2:
			lied := ref
			lied.Parts = append([]DefinitionPartRef(nil), ref.Parts...)
			switch int(at) % 4 {
			case 0:
				lied.RawBytes += int64(delta)
			case 1:
				lied.SizeBytes += int64(delta)
			case 2:
				lied.Parts[0].SizeBytes += int64(delta)
			case 3:
				lied.SHA256 = strings.Repeat("0", 64)
			}
			_, err := DecodeDefinitionObject(data, &lied, stub)
			requireNamedRefusal(t, "a reference that lies", err)
		case 3:
			other := proto.Clone(stub).(*orkstackv1.Orkflow)
			switch int(at) % 3 {
			case 0:
				other.Version += "x"
			case 1:
				other.Namespace += "x"
			case 2:
				other.DefinitionDigest = strings.Repeat("0", 64)
			}
			_, err := DecodeDefinitionObject(data, &ref, other)
			requireNamedRefusal(t, "another definition's stub", err)
		case 4:
			_, err := DecodeDefinitionObject(data, nil, stub)
			requireNamedRefusal(t, "no reference", err)
		}
	})
}
