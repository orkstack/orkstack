package enumname_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/enumname"
)

// orkstackEnums is every enum the generated orkstack.v1 files declare,
// nested ones included.
func orkstackEnums(t *testing.T) []protoreflect.EnumDescriptor {
	t.Helper()
	_ = orkstackv1.Status_STATUS_SUCCEEDED // link the generated files into the registry
	var out []protoreflect.EnumDescriptor
	var walk func(protoreflect.MessageDescriptors)
	walk = func(messages protoreflect.MessageDescriptors) {
		for i := 0; i < messages.Len(); i++ {
			md := messages.Get(i)
			for j := 0; j < md.Enums().Len(); j++ {
				out = append(out, md.Enums().Get(j))
			}
			walk(md.Messages())
		}
	}
	protoregistry.GlobalFiles.RangeFilesByPackage("orkstack.v1", func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Enums().Len(); i++ {
			out = append(out, fd.Enums().Get(i))
		}
		walk(fd.Messages())
		return true
	})
	if len(out) < 10 {
		t.Fatalf("found %d orkstack.v1 enums; the generated files are not linked", len(out))
	}
	return out
}

func TestShortAndProto_RoundTripEveryDeclaredValue(t *testing.T) {
	for _, enum := range orkstackEnums(t) {
		values := enum.Values()
		for i := 0; i < values.Len(); i++ {
			declared := string(values.Get(i).Name())
			short := enumname.Short(enum, declared)
			for _, spelling := range []string{declared, short, strings.ToLower(short)} {
				if got := enumname.Proto(enum, spelling); got != declared {
					t.Errorf("%s: Proto(%q) = %q; want %q (Short gave %q)",
						enum.FullName(), spelling, got, declared, short)
				}
			}
		}
	}
}

func TestProto_ReadsDashAsUnderscore(t *testing.T) {
	origin := orkstackv1.RunOriginKind(0).Descriptor()
	status := orkstackv1.Status(0).Descriptor()
	for _, tc := range []struct {
		enum protoreflect.EnumDescriptor
		in   string
		want string
	}{
		{origin, "parent-run", "RUN_ORIGIN_KIND_PARENT_RUN"},
		{origin, "PARENT-RUN", "RUN_ORIGIN_KIND_PARENT_RUN"},
		{origin, "run-origin-kind-run-again", "RUN_ORIGIN_KIND_RUN_AGAIN"},
		{status, "failed", "STATUS_FAILED"},
		{status, "STATUS_FAILED", "STATUS_FAILED"},
		{status, "Failed", "STATUS_FAILED"},
		{status, "nope", "nope"},
	} {
		if got := enumname.Proto(tc.enum, tc.in); got != tc.want {
			t.Errorf("Proto(%s, %q) = %q; want %q", tc.enum.Name(), tc.in, got, tc.want)
		}
	}
}

func TestPrefix(t *testing.T) {
	for _, tc := range []struct {
		enum protoreflect.EnumDescriptor
		want string
	}{
		{orkstackv1.Status(0).Descriptor(), "STATUS_"},
		{orkstackv1.EventType(0).Descriptor(), "EVENT_TYPE_"},
		{orkstackv1.FilterOperator(0).Descriptor(), "FILTER_OPERATOR_"},
	} {
		if got := enumname.Prefix(tc.enum); got != tc.want {
			t.Errorf("Prefix(%s) = %q; want %q", tc.enum.Name(), got, tc.want)
		}
	}
}
