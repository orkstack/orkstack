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
// and Task 26 do not exist there.

// olderMessage is m's type as a release before profiles knew it: every
// orkstack.v1 file m's file reaches is rebuilt under a renamed package, with
// the fields strip names removed (message name to field numbers), wherever
// they are declared.
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
	older := olderMessage(t, &orkstackv1.ExecuteTaskInput{}, map[string][]int32{"ExecuteTaskInput": {30}})
	plan := &engine.RunPlan{ProfileIdentities: map[string]map[string]string{"p": {"amd": "id"}}}
	encode := func(task *orkstackv1.Task) []byte {
		t.Helper()
		devices, err := plan.ProfileRefsFor("p", task)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := protojson.Marshal(&orkstackv1.ExecuteTaskInput{
			RunId: "r", TaskId: "t", Network: task.GetNetwork(), Devices: devices,
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

	raw := encode(&orkstackv1.Task{Devices: []string{"amd"}})
	if err := protojson.Unmarshal(raw, older.New().Interface()); err == nil {
		t.Fatalf("an older Orker accepted a profiled input it cannot honour:\n%s", raw)
	}
}

// TestComposedChild_KeepsProfiledTaskFields: a child's planned definition is
// decoded with DiscardUnknown (ChildRunPlan.Definition) by the release that
// built it, which knows every profile field, so nothing a child's task names
// is dropped on the way.
func TestComposedChild_KeepsProfiledTaskFields(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "child", Tasks: []*orkstackv1.Task{{
		Id: "a", Devices: []string{"amd"},
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
