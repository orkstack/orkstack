package dsl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The interface label a released engine reads.
//
// testdata/activity_interface_v0.1.0.binpb is a FileDescriptorProto holding
// only ActivityInterface, OpSchema and FieldSchema (with their map entries),
// copied from the generated descriptor of the v0.1.0 tree; its one import is
// google/protobuf/struct.proto. It was written, in a worktree of v0.1.0, by a
// program that took protodesc.ToFileDescriptorProto of
// orkstackv1.File_orkstack_v1_activity_service_proto, kept those three
// messages, named the file orkstack/v1/activity_interface_v0.1.0.proto with
// struct.proto as its dependency, and marshalled it deterministically. It is
// never regenerated: it is what v0.1.0 decodes, and its digest is pinned
// below.
const releasedInterfaceDescriptor = "testdata/activity_interface_v0.1.0.binpb"

const releasedInterfaceDescriptorSHA256 = "d55be74f04bbdf980672515a4e214e078984885d2ba93bf1834a575378421a14"

// releasedInterfaceReader reads an interface label exactly as a v0.1.0
// engine does: its bound, protojson into v0.1.0's message with unknown
// fields refused, then validateInterface, whose rules v0.1.0 shares.
func releasedInterfaceReader(t *testing.T) func(label string) error {
	t.Helper()
	raw, err := os.ReadFile(releasedInterfaceDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != releasedInterfaceDescriptorSHA256 {
		t.Fatalf("%s is not the descriptor v0.1.0 shipped", releasedInterfaceDescriptor)
	}
	file := &descriptorpb.FileDescriptorProto{}
	if err := proto.Unmarshal(raw, file); err != nil {
		t.Fatal(err)
	}
	descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	message := descriptor.Messages().ByName("ActivityInterface")
	if message == nil {
		t.Fatal("the released descriptor holds no ActivityInterface")
	}
	return func(label string) error {
		if len(label) > MaxInterfaceLabelBytes {
			return fmt.Errorf("label is %d bytes, limit is %d", len(label), MaxInterfaceLabelBytes)
		}
		released := dynamicpb.NewMessage(message)
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(label), released); err != nil {
			return err
		}
		wire, err := proto.Marshal(released)
		if err != nil {
			return err
		}
		iface := &orkstackv1.ActivityInterface{}
		if err := proto.Unmarshal(wire, iface); err != nil {
			return err
		}
		return validateInterface(iface)
	}
}

// TestReleasedEngine_ReadsEveryInterfaceThisTreePacks: an image built from
// this tree — icon, descriptions and all — declares an interface a v0.1.0
// engine reads whole. The icon and the descriptions are labels of their own,
// which that engine never reads; nothing new is in the interface label.
// A failure here means a word was added to ActivityInterface, OpSchema or
// FieldSchema: a released engine would refuse every label that uses it and
// lose the whole interface, so a new word needs a label of its own.
func TestReleasedEngine_ReadsEveryInterfaceThisTreePacks(t *testing.T) {
	read := releasedInterfaceReader(t)
	for path, source := range interfaceDocuments(t) {
		decl, err := ParseActivityDocument(source)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		label, err := PackInterfaceLabel(decl.Interface)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if err := read(label); err != nil {
			t.Errorf("%s: a v0.1.0 engine cannot read its interface label: %v", path, err)
		}
	}
}

// TestReleasedEngine_RefusesTheRejectedAlternatives: the two designs not
// taken — a description written into the interface label, and a version 2
// interface label — each cost a v0.1.0 engine the whole interface.
func TestReleasedEngine_RefusesTheRejectedAlternatives(t *testing.T) {
	read := releasedInterfaceReader(t)
	if err := read(`{"schemaVersion":1,"fields":{"url":{"type":"string"}}}`); err != nil {
		t.Fatalf("the released reader refuses a plain interface: %v", err)
	}
	for _, tc := range []struct{ name, label, want string }{
		{"a description in the interface label", `{"schemaVersion":1,"fields":{"url":{"type":"string","description":"x"}}}`,
			`unknown field "description"`},
		{"an icon in the interface label", `{"schemaVersion":1,"icon":{"paths":[]}}`, `unknown field "icon"`},
		{"a version 2 interface label", `{"schemaVersion":2,"fields":{"url":{"type":"string"}}}`, "schemaVersion 2 is not supported"},
	} {
		if err := read(tc.label); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: the released reader answered %v, want an error containing %q", tc.name, err, tc.want)
		}
		if _, err := ParseInterfaceLabel(tc.label); err == nil {
			t.Errorf("%s: this engine reads a label a v0.1.0 engine refuses", tc.name)
		}
	}
}
