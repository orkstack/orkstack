package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// v031Lacks is, per message an Orker returns to the engine, the field numbers
// v0.3.1's proto does not have (git show v0.3.1:proto/orkstack/v1/activity.proto).
// An answer a newer Orker sets them in is what an older engine decodes.
var v031Lacks = map[string][]int32{
	"DescribeOrkerResult":   {24, 26, 27, 28, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43},
	"ActivityFailureDetail": {11, 12, 13, 14},
}

// olderType is md as a build that predates the fields in lacks knows it: the
// same message with those fields removed, so a payload that sets one carries a
// field it has never heard of.
func olderType(t *testing.T, md protoreflect.MessageDescriptor, lacks []int32) protoreflect.MessageDescriptor {
	t.Helper()
	file := protodesc.ToFileDescriptorProto(md.ParentFile())
	var msg *descriptorpb.DescriptorProto
	for _, m := range file.GetMessageType() {
		if m.GetName() == string(md.Name()) {
			msg = m
		}
	}
	if msg == nil {
		t.Fatalf("%s is not a top-level message of %s", md.FullName(), file.GetName())
	}
	msg.Field = slices.DeleteFunc(msg.Field, func(f *descriptorpb.FieldDescriptorProto) bool {
		return slices.Contains(lacks, f.GetNumber())
	})
	// A oneof left with no field (an optional field's synthetic one) goes too,
	// and the fields that remain point at their oneof's new index.
	used := map[int32]bool{}
	for _, f := range msg.Field {
		if f.OneofIndex != nil {
			used[f.GetOneofIndex()] = true
		}
	}
	var oneofs []*descriptorpb.OneofDescriptorProto
	index := map[int32]int32{}
	for i, o := range msg.OneofDecl {
		if used[int32(i)] {
			index[int32(i)] = int32(len(oneofs))
			oneofs = append(oneofs, o)
		}
	}
	msg.OneofDecl = oneofs
	for _, f := range msg.Field {
		if f.OneofIndex != nil {
			f.OneofIndex = proto.Int32(index[f.GetOneofIndex()])
		}
	}
	// The older file stands in a registry of its own, beside the files it
	// imports, so it keeps its name and package and resolves its own types.
	set := &descriptorpb.FileDescriptorSet{}
	seen := map[string]bool{}
	var addImports func(fd protoreflect.FileDescriptor)
	addImports = func(fd protoreflect.FileDescriptor) {
		imports := fd.Imports()
		for i := range imports.Len() {
			dep := imports.Get(i).FileDescriptor
			if seen[dep.Path()] {
				continue
			}
			seen[dep.Path()] = true
			addImports(dep)
			set.File = append(set.File, protodesc.ToFileDescriptorProto(dep))
		}
	}
	addImports(md.ParentFile())
	set.File = append(set.File, file)
	files, err := protodesc.NewFiles(set)
	must.OKf(t, err, "build the older %s", md.FullName())
	older, err := files.FindDescriptorByName(md.FullName())
	must.OKf(t, err, "find the older %s", md.FullName())
	return older.(protoreflect.MessageDescriptor)
}

// TestDataConverter_AnEngineDecodesAnswersFromANewerOrker: an Orker that
// leads its engines in a rollout answers with fields the engine's build has
// never heard of. The SDK's default converter refuses such an answer whole,
// which is how a v0.3.1 engine loses every v0.4.0 Orker's DescribeOrker; the
// engine's converter drops the unknown fields and keeps every field it knows.
func TestDataConverter_AnEngineDecodesAnswersFromANewerOrker(t *testing.T) {
	stamp := timestamppb.New(time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC))
	ordinal := int32(3)
	answers := []proto.Message{
		&orkstackv1.DescribeOrkerResult{
			OrkerId: "orker-a", BuildId: "build-new", Pool: "gpu", MaxConcurrent: 4,
			EachSupported: true, CacheSupported: true, JoinSupported: true,
			AttemptOrdinalsSupported: true, MasksSecrets: true, IdentitySupported: true,
			RunContextSupported: true, EachLinesSupported: true,
			MetadataReachable:     []string{"169.254.169.254"},
			StoreScopeDetail:      "prefix",
			StoreScopeConfirmedAt: stamp,
			Stopping:              &orkstackv1.OrkerStopping{Since: stamp, GraceEndsAt: stamp},
			ProfileKindsSupported: []orkstackv1.ProfileKind{orkstackv1.ProfileKind_PROFILE_KIND_GPU},
			Profiles: []*orkstackv1.OrkerProfile{{
				Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU, Units: 1, IdentityDigest: "abc",
			}},
			ProfileCordons: []*orkstackv1.ProfileCordon{{
				Cause: orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE, Profile: "amd",
				Reason: orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT, Since: stamp,
			}},
		},
		&orkstackv1.ActivityFailureDetail{
			ReasonCode:   "ATTEMPT_STORE_UNAVAILABLE",
			NeverStarted: true, Ordinal: &ordinal, StoreError: "503 SlowDown",
		},
	}
	for _, answer := range answers {
		md := answer.ProtoReflect().Descriptor()
		t.Run(string(md.Name()), func(t *testing.T) {
			payload, err := converter.GetDefaultDataConverter().ToPayload(answer)
			must.OKf(t, err, "encode as an Orker does")
			older := olderType(t, md, v031Lacks[string(md.Name())])

			strict := dynamicpb.NewMessage(older)
			if err := converter.GetDefaultDataConverter().FromPayload(payload, &strict); err == nil {
				t.Fatalf("the SDK default decoded a %s with fields its type lacks; "+
					"this test no longer shows the hazard the engine's converter exists for", md.Name())
			}

			lenient := dynamicpb.NewMessage(older)
			must.OKf(t, engine.DataConverter().FromPayload(payload, &lenient),
				"the engine's converter refused a %s from a newer Orker", md.Name())
			// Every field the older type knows reads as the Orker sent it,
			// compared on the wire so a nested message is compared whole.
			want := dynamicpb.NewMessage(older)
			raw, err := proto.Marshal(answer)
			must.OKf(t, err, "marshal the answer")
			must.OKf(t, proto.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, want), "binary decode into the older type")
			a, _ := proto.MarshalOptions{Deterministic: true}.Marshal(want)
			b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(lenient)
			if !bytes.Equal(a, b) {
				t.Errorf("the engine's converter decoded %v, want every field the older type knows: %v", lenient, want)
			}
			if lenient.Get(older.Fields().ByName("orker_id")).String() == "" &&
				lenient.Get(older.Fields().ByName("reason_code")).String() == "" {
				t.Errorf("decoded %v holds none of the fields it knows", lenient)
			}
		})
	}
}

// TestDataConverter_EncodesAsTheSDKDefaultDoes: the engine's converter changes
// only how an unknown field is decoded, never a byte it writes, so every
// payload already in a history and every payload a newer engine writes are
// the ones the SDK default would have written.
func TestDataConverter_EncodesAsTheSDKDefaultDoes(t *testing.T) {
	values := []any{
		&orkstackv1.ExecuteTaskInput{RunId: "ork-1", TaskId: "build",
			Secrets: []*orkstackv1.SecretRef{{Name: "db", Source: "env:DB", Unmasked: true}}},
		&orkstackv1.DescribeOrkerResult{OrkerId: "orker-a", MasksSecrets: true},
		&orkstackv1.ExecuteTaskResult{TaskId: "build", ValuesJson: []byte(`{"a":1}`)},
		map[string]any{"run": "ork-1", "n": 2},
		[]byte("raw"),
		nil,
		"text",
	}
	for _, v := range values {
		want, err := converter.GetDefaultDataConverter().ToPayload(v)
		must.OKf(t, err, "default encode %T", v)
		got, err := engine.DataConverter().ToPayload(v)
		must.OKf(t, err, "engine encode %T", v)
		if !proto.Equal(want, got) {
			t.Errorf("%T encodes as %s with the engine's converter, want %s as the SDK default", v, got, want)
		}
	}
}

// TestDataConverter_IsByteIdenticalWithEncryptionOff: with no key configured
// the engine's converters, payload encryption's among them, write every
// payload and failure byte for byte as the SDK defaults do.
func TestDataConverter_IsByteIdenticalWithEncryptionOff(t *testing.T) {
	values := []any{
		&orkstackv1.ExecuteTaskInput{RunId: "ork-1", TaskId: "build"},
		&orkstackv1.ExecuteTaskResult{TaskId: "build", ValuesJson: []byte(`{"a":1}`)},
		map[string]any{"run": "ork-1", "n": 2},
		[]byte("raw"),
		nil,
		"text",
	}
	det := proto.MarshalOptions{Deterministic: true}
	for _, v := range values {
		want, err := converter.GetDefaultDataConverter().ToPayload(v)
		must.OKf(t, err, "default encode %T", v)
		got, err := engine.DataConverter().ToPayload(v)
		must.OKf(t, err, "engine encode %T", v)
		wantB, _ := det.Marshal(want)
		gotB, _ := det.Marshal(got)
		if !bytes.Equal(wantB, gotB) {
			t.Errorf("%T: the engine writes %x, the SDK default %x", v, gotB, wantB)
		}
	}
	for _, err := range []error{
		errors.New("plain"),
		temporal.NewApplicationErrorWithCause("m", "ActivityFailed", errors.New("cause"), &orkstackv1.ActivityFailureDetail{}),
		temporal.NewNonRetryableApplicationError("m", "ContractViolation", nil, "detail"),
		temporal.NewCanceledError("why"),
	} {
		want := temporal.GetDefaultFailureConverter().ErrorToFailure(err)
		got := engine.FailureConverter().ErrorToFailure(err)
		wantB, _ := det.Marshal(want)
		gotB, _ := det.Marshal(got)
		if !bytes.Equal(wantB, gotB) {
			t.Errorf("%v: the engine writes failure %v, the SDK default %v", err, got, want)
		}
	}
}

// TestDataConverter_GuardsASealedRun: an engine that holds no payload key,
// which is every engine of this release, does not run workflow code of a run
// whose memo carries one; the converter panics at the first payload it reads,
// failing the workflow task rather than deciding anything
// (payload-encryption.md §4.1 rule 3).
func TestDataConverter_GuardsASealedRun(t *testing.T) {
	ek, err := payloadcrypt.NewKey(bytes.Repeat([]byte{1}, payloadcrypt.KeySize))
	must.OK(t, err)
	_, memo, err := payloadcrypt.NewRunTree(context.Background(), payloadcrypt.NewStaticDataKeys("pk_t", ek), ek, "ns", "ns_1", "ork-guard")
	must.OK(t, err)

	run := func(withMemo bool) error {
		var s testsuite.WorkflowTestSuite
		env := s.NewTestWorkflowEnvironment()
		env.SetDataConverter(engine.DataConverter())
		env.SetFailureConverter(engine.FailureConverter())
		if withMemo {
			must.OK(t, env.SetMemoOnStart(map[string]any{payloadcrypt.MemoField: memo}))
		}
		env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in string) (string, error) { return in, nil },
			workflow.RegisterOptions{Name: "guarded"})
		env.ExecuteWorkflow("guarded", "input")
		return env.GetWorkflowError()
	}
	if err := run(false); err != nil {
		t.Fatalf("a run with no payload key failed: %v", err)
	}
	err = run(true)
	var panicked *temporal.PanicError
	if !errors.As(err, &panicked) || !strings.Contains(err.Error(), payloadcrypt.ErrEngineKeyMissing.Error()) {
		t.Fatalf("a run whose memo carries a payload key ended %v, want the guard's panic", err)
	}
}

// TestDataConverter_RecordedHistoriesReplayAsUnderTheSDKDefault: every
// recorded RunWorkflow history replays under the engine's converter with
// exactly the outcome it has under the SDK default the engine dialed with
// before, so swapping the converter is no new command path.
func TestDataConverter_RecordedHistoriesReplayAsUnderTheSDKDefault(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "history", "run-*.json"))
	must.OKf(t, err, "list the recorded histories")
	if len(paths) == 0 {
		t.Fatal("no recorded RunWorkflow history under testdata/history")
	}
	replay := func(dc converter.DataConverter, path string) error {
		replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: dc})
		must.OKf(t, err, "new replayer")
		replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
		return replayer.ReplayWorkflowHistory(nil, loadHistoryFile(t, path))
	}
	replayed := 0
	for _, path := range paths {
		want := replay(converter.GetDefaultDataConverter(), path)
		got := replay(engine.DataConverter(), path)
		if errString(want) != errString(got) {
			t.Errorf("%s replays to %q under the engine's converter, want %q as under the SDK default",
				filepath.Base(path), errString(got), errString(want))
		}
		if want == nil {
			replayed++
		}
	}
	// The comparison is worth something only if most histories replay at all.
	if replayed < len(paths)/2 {
		t.Fatalf("only %d of %d recorded histories replay cleanly under the SDK default; "+
			"the comparison above holds too little", replayed, len(paths))
	}
	t.Logf("%d of %d recorded histories replay cleanly under both converters", replayed, len(paths))
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

// withKey is v's ProtoJSON payload with one more key a newer build would
// write: the field this build does not know.
func withKey(t *testing.T, v proto.Message, key, value string) *commonpb.Payload {
	t.Helper()
	payload, err := converter.GetDefaultDataConverter().ToPayload(v)
	must.OKf(t, err, "encode %T", v)
	data := bytes.TrimSuffix(bytes.TrimSpace(payload.GetData()), []byte("}"))
	if len(bytes.TrimSpace(data)) > 1 {
		data = append(data, ',')
	}
	payload.Data = append(data, []byte(fmt.Sprintf("%q:%s}", key, value))...)
	return payload
}

// TestDataConverter_EveryOtherMessageStaysStrict: only an Orker's answer is
// decoded leniently. A run's definition, its carry, a task's input or an
// await's resolution that a newer engine wrote with a field this build lacks
// is refused, as the SDK default refuses it, so an older engine never runs a
// run with one of its settings silently gone.
func TestDataConverter_EveryOtherMessageStaysStrict(t *testing.T) {
	for _, v := range []proto.Message{
		&orkstackv1.Orkflow{Name: "release"},
		&orkstackv1.RunContinuation{Version: 1, Generation: 2},
		&orkstackv1.ExecuteTaskInput{RunId: "ork-1", TaskId: "build"},
		&orkstackv1.AwaitSignal{TaskId: "approve"},
	} {
		name := v.ProtoReflect().Descriptor().Name()
		// A pointer to a nil pointer of v's own type, as the SDK decodes into.
		known := reflect.New(reflect.TypeOf(v))
		knownPayload, err := converter.GetDefaultDataConverter().ToPayload(v)
		must.OKf(t, err, "encode %s", name)
		must.OKf(t, engine.DataConverter().FromPayload(knownPayload, known.Interface()), "decode a %s this build wrote", name)
		out := reflect.New(reflect.TypeOf(v))
		if err := engine.DataConverter().FromPayload(withKey(t, v, "fieldFromTheFuture", `"locked-down"`), out.Interface()); err == nil {
			t.Errorf("%s with a field this build lacks decoded; want it refused", name)
		}
	}
}

// TestDataConverter_AnAnswersUnknownEnumValueReadsAsZero: an enum value a
// newer Orker names and this build lacks reads as the enum's zero, keeping
// the rest of the answer; the strict decode would have lost all of it.
func TestDataConverter_AnAnswersUnknownEnumValueReadsAsZero(t *testing.T) {
	payload := withKey(t, &orkstackv1.DescribeOrkerResult{OrkerId: "orker-a"}, "resultsScope", `"RESULTS_SCOPE_FROM_THE_FUTURE"`)
	var strict *orkstackv1.DescribeOrkerResult
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &strict); err == nil {
		t.Fatal("the SDK default decoded an unknown enum value; this test no longer shows the hazard")
	}
	var got *orkstackv1.DescribeOrkerResult
	must.OKf(t, engine.DataConverter().FromPayload(payload, &got), "decode an answer with an unknown enum value")
	if got.GetOrkerId() != "orker-a" || got.GetResultsScope() != 0 {
		t.Fatalf("decoded %v, want orker-a with results_scope at its zero", got)
	}
}
