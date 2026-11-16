package engine_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// The mixed-fleet tests for profiles: what a release built before profiles
// does with the bytes this one writes. "Older" is today's descriptor with the
// profile additions removed, which is the v0.3.1 shape: ExecuteTaskInput 30
// and 31, Task 26 and 27 and NETWORK_MODE_EGRESS do not exist there.

// olderMessage is m's type as a release before profiles knew it: every
// orkstack.v1 file m's file reaches is rebuilt under a renamed package, with
// the fields strip names removed (message name to field numbers) and
// NETWORK_MODE_EGRESS removed from NetworkMode, wherever they are declared.
func olderMessage(t *testing.T, m proto.Message, strip map[string][]int32) protoreflect.MessageType {
	t.Helper()
	const from, to = ".orkstack.v1.", ".older.orkstack.v1."
	local := new(protoregistry.Files)
	resolver := olderResolver{local: local}
	var build func(file protoreflect.FileDescriptor) error
	build = func(file protoreflect.FileDescriptor) error {
		if file.Package() != "orkstack.v1" {
			return nil
		}
		if _, err := local.FindFileByPath("older/" + file.Path()); err == nil {
			return nil
		}
		imports := file.Imports()
		for i := 0; i < imports.Len(); i++ {
			if err := build(imports.Get(i).FileDescriptor); err != nil {
				return err
			}
		}
		fdp := protodesc.ToFileDescriptorProto(file)
		fdp.Name = proto.String("older/" + fdp.GetName())
		fdp.Package = proto.String("older.orkstack.v1")
		for i, dep := range fdp.Dependency {
			if strings.HasPrefix(dep, "orkstack/v1/") {
				fdp.Dependency[i] = "older/" + dep
			}
		}
		var fix func(msgs []*descriptorpb.DescriptorProto)
		fix = func(msgs []*descriptorpb.DescriptorProto) {
			for _, msg := range msgs {
				if numbers, ok := strip[msg.GetName()]; ok {
					msg.Field = slices.DeleteFunc(msg.Field, func(f *descriptorpb.FieldDescriptorProto) bool {
						return slices.Contains(numbers, f.GetNumber())
					})
				}
				for _, f := range msg.GetField() {
					if strings.HasPrefix(f.GetTypeName(), from) {
						f.TypeName = proto.String(to + strings.TrimPrefix(f.GetTypeName(), from))
					}
				}
				fix(msg.GetNestedType())
			}
		}
		fix(fdp.GetMessageType())
		for _, enum := range fdp.GetEnumType() {
			if enum.GetName() == "NetworkMode" {
				enum.Value = slices.DeleteFunc(enum.Value, func(v *descriptorpb.EnumValueDescriptorProto) bool {
					return v.GetName() == "NETWORK_MODE_EGRESS"
				})
			}
		}
		// Services refer to messages too; a decode needs none of them.
		fdp.Service = nil
		older, err := protodesc.NewFile(fdp, resolver)
		if err != nil {
			return fmt.Errorf("%s: %w", file.Path(), err)
		}
		return local.RegisterFile(older)
	}
	desc := m.ProtoReflect().Descriptor()
	if err := build(desc.ParentFile()); err != nil {
		t.Fatalf("build the older descriptors: %v", err)
	}
	found, err := local.FindDescriptorByName(protoreflect.FullName("older." + string(desc.FullName())))
	if err != nil {
		t.Fatalf("find the older %s: %v", desc.FullName(), err)
	}
	return dynamicpb.NewMessageType(found.(protoreflect.MessageDescriptor))
}

// olderResolver resolves the rebuilt files first and everything else (the
// well-known types) from the global registry.
type olderResolver struct{ local *protoregistry.Files }

func (r olderResolver) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if fd, err := r.local.FindFileByPath(path); err == nil {
		return fd, nil
	}
	return protoregistry.GlobalFiles.FindFileByPath(path)
}

func (r olderResolver) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	if d, err := r.local.FindDescriptorByName(name); err == nil {
		return d, nil
	}
	return protoregistry.GlobalFiles.FindDescriptorByName(name)
}

// TestOlderOrker_DecodesProfiledInput: a release before profiles decodes the
// input of a task that names none exactly as before, and REFUSES the input of
// one that does, because an Orker decodes its input strictly (Temporal's
// default converter). So an older Orker never runs a profiled task without
// its profile: it fails the attempt explicitly, and submit's capability check
// (profile_kinds_supported) keeps such tasks off its pool in the first place.
func TestOlderOrker_DecodesProfiledInput(t *testing.T) {
	older := olderMessage(t, &orkstackv1.ExecuteTaskInput{}, map[string][]int32{"ExecuteTaskInput": {30, 31}})
	plan := &engine.RunPlan{ProfileIdentities: map[string]map[string]string{"p": {"amd": "id", "github-only": "id2"}}}
	encode := func(task *orkstackv1.Task) []byte {
		t.Helper()
		devices, egress, err := plan.ProfileRefsFor("p", task)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := protojson.Marshal(&orkstackv1.ExecuteTaskInput{
			RunId: "r", TaskId: "t", Network: task.GetNetwork(), Devices: devices, Egress: egress,
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	plain := encode(&orkstackv1.Task{Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE})
	if err := protojson.Unmarshal(plain, older.New().Interface()); err != nil {
		t.Fatalf("an older Orker refuses a task that names no profile: %v\n%s", err, plain)
	}

	for name, task := range map[string]*orkstackv1.Task{
		"devices": {Devices: []string{"amd"}},
		"egress":  {Network: orkstackv1.NetworkMode_NETWORK_MODE_EGRESS, NetworkProfile: "github-only"},
	} {
		t.Run(name, func(t *testing.T) {
			raw := encode(task)
			if err := protojson.Unmarshal(raw, older.New().Interface()); err == nil {
				t.Fatalf("an older Orker accepted a profiled input it cannot honour:\n%s", raw)
			}
		})
	}
}

// TestComposedChild_KeepsProfiledTaskFields: a child's planned definition is
// decoded with DiscardUnknown (ChildRunPlan.Definition) by the release that
// built it, which knows every profile field, so nothing a child's task names
// is dropped on the way.
func TestComposedChild_KeepsProfiledTaskFields(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "child", Tasks: []*orkstackv1.Task{{
		Id: "a", Devices: []string{"amd"}, Network: orkstackv1.NetworkMode_NETWORK_MODE_EGRESS, NetworkProfile: "github-only",
	}}}
	raw, err := protojson.Marshal(wf)
	if err != nil {
		t.Fatal(err)
	}
	got, err := (&engine.ChildRunPlan{Orkflow: raw}).Definition()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, wf) {
		t.Fatalf("the child definition lost profile fields: %v", got)
	}
}

// TestOlderEngine_SubmitsProfiledDefinition records what a release before
// profiles does with a stored definition that names them: it reads stored
// Orkflows with DiscardUnknown and re-validates nothing at submit, so it does
// not refuse. It drops the profile fields and reads NETWORK_MODE_EGRESS as
// unspecified, which is the default network: an egress task submitted by
// such an engine runs with the default network's full egress, silently.
// That risk is accepted and documented, not guarded: no engine of an older
// release may serve submits once an Orkflow names a profile, so a deployment
// finishes its engine upgrade before creating one and never rolls back
// across it (docs/DEPLOY.md). If this test fails because the older shape now
// refuses, the behaviour changed and the documentation should say so.
func TestOlderEngine_SubmitsProfiledDefinition(t *testing.T) {
	older := olderMessage(t, &orkstackv1.Orkflow{}, map[string][]int32{"Task": {26, 27}})
	stored, err := protojson.Marshal(&orkstackv1.Orkflow{Name: "render", Tasks: []*orkstackv1.Task{{
		Id: "fetch", Devices: []string{"amd"}, Network: orkstackv1.NetworkMode_NETWORK_MODE_EGRESS, NetworkProfile: "github-only",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	decoded := older.New().Interface()
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(stored, decoded); err != nil {
		t.Fatalf("the older engine refused the stored definition (a guard is now in place; update this test): %v", err)
	}
	tasks := decoded.ProtoReflect().Get(decoded.ProtoReflect().Descriptor().Fields().ByName("tasks")).List()
	task := tasks.Get(0).Message()
	if task.Descriptor().Fields().ByName("devices") != nil || task.Descriptor().Fields().ByName("network_profile") != nil {
		t.Fatal("the older shape is not older: it still has the profile fields")
	}
	if network := task.Get(task.Descriptor().Fields().ByName("network")).Enum(); network != 0 {
		t.Fatalf("network = %d; the older shape no longer drops EGRESS (update this test)", network)
	}
}
