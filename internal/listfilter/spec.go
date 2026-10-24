// Package listfilter is OrkStack's one structured list filter: the allowlist
// a list request declares in proto ((orkstack.v1.filterable)), the CLI
// grammar of a filter, the compiler that validates and canonicalizes a
// request's filters, and the matcher that applies them to a row.
//
// docs/UI_CLI_PARITY.md "Filtering lists" is the contract. Every filter of a
// request must hold (AND); the operator set is closed; there is no OR, no
// grouping and no expression (AGENTS.md §13). The engine and orkctl share
// this package, and the browser reads the same declarations from its own
// generated descriptors, so one allowlist decides what every client may
// filter.
package listfilter

import (
	"fmt"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/enumname"
)

// Kind is what a filterable field holds, which decides the operators it may
// declare and how its values are spelled.
type Kind int

const (
	KindText   Kind = iota + 1 // string
	KindEnum                   // a proto enum, by short value name
	KindBool                   // true or false
	KindTime                   // google.protobuf.Timestamp, an RFC 3339 instant
	KindLabels                 // map<string, string>, addressed labels.<key>
)

// Spec is one list request's allowlist, resolved against the listed message.
type Spec struct {
	// Resource is the listed message.
	Resource protoreflect.MessageDescriptor
	// Fields are the filterable fields, in declaration order.
	Fields []*Field
	// Unavailable is the string field that is non-empty on a row the server
	// could not measure, or nil when the list has no such rows.
	Unavailable protoreflect.FieldDescriptor

	request protoreflect.MessageDescriptor
	byPath  map[string]*Field
}

// Field is one filterable field of a Spec.
type Field struct {
	Path, Label string
	Kind        Kind
	Operators   []orkstackv1.FilterOperator
	// AllowedValues, for an enum field: the canonical short names a filter
	// may name, in declaration order.
	AllowedValues       []string
	KeptWhenUnavailable bool
	Requires            string
	ValueRule           orkstackv1.FilterValueRule

	chain []protoreflect.FieldDescriptor
	enum  protoreflect.EnumDescriptor
}

// takes reports whether the field declares op.
func (f *Field) takes(op orkstackv1.FilterOperator) bool {
	return slices.Contains(f.Operators, op)
}

// display is how the field is named to a person: labels.KEY for a labels
// map, the path otherwise.
func (f *Field) display() string {
	if f.Kind == KindLabels {
		return f.Path + ".KEY"
	}
	return f.Path
}

var specs sync.Map // protoreflect.FullName -> *Spec

// SpecFor reads the (orkstack.v1.filterable) option of request's message and
// resolves it. The answer is cached per message. An error is a declaration
// bug: a request with no option, a path that names nothing, or operators the
// field's kind cannot take.
func SpecFor(request proto.Message) (*Spec, error) {
	return SpecForDescriptor(request.ProtoReflect().Descriptor())
}

// SpecForDescriptor is SpecFor for a message descriptor.
func SpecForDescriptor(request protoreflect.MessageDescriptor) (*Spec, error) {
	if cached, ok := specs.Load(request.FullName()); ok {
		return cached.(*Spec), nil
	}
	spec, err := resolve(request)
	if err != nil {
		return nil, err
	}
	specs.Store(request.FullName(), spec)
	return spec, nil
}

// MustSpec is SpecFor for package-level variables; it panics only on a
// declaration bug, which TestSpec_EveryDeclarationResolves catches first.
func MustSpec(request proto.Message) *Spec {
	spec, err := SpecFor(request)
	if err != nil {
		panic(err)
	}
	return spec
}

// Declared reports whether request's message carries the option.
func Declared(request protoreflect.MessageDescriptor) bool {
	return proto.HasExtension(request.Options(), orkstackv1.E_Filterable)
}

func resolve(request protoreflect.MessageDescriptor) (*Spec, error) {
	if !Declared(request) {
		return nil, fmt.Errorf("%s declares no (orkstack.v1.filterable) option", request.FullName())
	}
	declared := proto.GetExtension(request.Options(), orkstackv1.E_Filterable).(*orkstackv1.ListFilterSpec)
	filters := request.Fields().ByName("filters")
	if filters == nil || !filters.IsList() || filters.Message() == nil ||
		filters.Message().FullName() != (&orkstackv1.FieldFilter{}).ProtoReflect().Descriptor().FullName() {
		return nil, fmt.Errorf("%s is filterable but has no `repeated FieldFilter filters`", request.FullName())
	}
	found, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(declared.GetResource()))
	if err != nil {
		return nil, fmt.Errorf("%s: resource %q: %w", request.FullName(), declared.GetResource(), err)
	}
	resource, ok := found.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s: resource %q is not a message", request.FullName(), declared.GetResource())
	}
	spec := &Spec{Resource: resource, request: request, byPath: map[string]*Field{}}
	if name := declared.GetUnavailableField(); name != "" {
		fd := resource.Fields().ByName(protoreflect.Name(name))
		if fd == nil || fd.Kind() != protoreflect.StringKind || fd.Cardinality() == protoreflect.Repeated {
			return nil, fmt.Errorf("%s: unavailable_field %q is not a string field of %s",
				request.FullName(), name, resource.FullName())
		}
		spec.Unavailable = fd
	}
	for _, decl := range declared.GetFields() {
		field, err := resolveField(spec, decl)
		if err != nil {
			return nil, fmt.Errorf("%s: field %q: %w", request.FullName(), decl.GetPath(), err)
		}
		if _, dup := spec.byPath[field.Path]; dup {
			return nil, fmt.Errorf("%s: field %q is declared twice", request.FullName(), field.Path)
		}
		spec.byPath[field.Path] = field
		spec.Fields = append(spec.Fields, field)
	}
	for _, field := range spec.Fields {
		if field.Requires == "" {
			continue
		}
		required, ok := spec.byPath[field.Requires]
		if !ok || !required.takes(orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS) {
			return nil, fmt.Errorf("%s: field %q requires %q, which is not a declared path taking equals",
				request.FullName(), field.Path, field.Requires)
		}
	}
	return spec, nil
}

// operatorsByKind is what each kind may declare (§4.1).
var operatorsByKind = map[Kind][]orkstackv1.FilterOperator{
	KindText: {orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_IN, orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX},
	KindLabels: {orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_IN, orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX},
	KindEnum: {orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_IN},
	KindBool: {orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS},
	KindTime: {orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER, orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE},
}

func resolveField(spec *Spec, decl *orkstackv1.FilterableField) (*Field, error) {
	field := &Field{
		Path:                decl.GetPath(),
		Label:               decl.GetLabel(),
		Operators:           decl.GetOperators(),
		KeptWhenUnavailable: decl.GetKeptWhenUnavailable(),
		Requires:            decl.GetRequires(),
		ValueRule:           decl.GetValueRule(),
	}
	if field.Path == "" || field.Label == "" {
		return nil, fmt.Errorf("a field needs a path and a label")
	}
	md := spec.Resource
	parts := splitPath(field.Path)
	for i, part := range parts {
		if md == nil {
			return nil, fmt.Errorf("%q is not a message, so it has no field %q", parts[i-1], part)
		}
		fd := md.Fields().ByName(protoreflect.Name(part))
		if fd == nil {
			return nil, fmt.Errorf("%s has no field %q", md.FullName(), part)
		}
		field.chain = append(field.chain, fd)
		last := i == len(parts)-1
		switch {
		case fd.IsMap():
			if !last || fd.MapKey().Kind() != protoreflect.StringKind || fd.MapValue().Kind() != protoreflect.StringKind {
				return nil, fmt.Errorf("only a map<string, string> can be filtered, and only as the whole path")
			}
			field.Kind = KindLabels
		case fd.IsList():
			return nil, fmt.Errorf("a repeated field is never filterable")
		case fd.Kind() == protoreflect.MessageKind:
			if fd.Message().FullName() == "google.protobuf.Timestamp" {
				if !last {
					return nil, fmt.Errorf("a timestamp has no filterable fields")
				}
				field.Kind = KindTime
				break
			}
			if last {
				return nil, fmt.Errorf("a message field is filtered through one of its fields")
			}
			md = fd.Message()
			continue
		case fd.Kind() == protoreflect.StringKind:
			field.Kind = KindText
		case fd.Kind() == protoreflect.EnumKind:
			field.Kind = KindEnum
			field.enum = fd.Enum()
		case fd.Kind() == protoreflect.BoolKind:
			field.Kind = KindBool
		default:
			return nil, fmt.Errorf("a %s field is never filterable", fd.Kind())
		}
		if !last {
			return nil, fmt.Errorf("%q is not a message, so the path cannot continue", part)
		}
		md = nil
	}
	if len(field.Operators) == 0 {
		return nil, fmt.Errorf("declares no operators")
	}
	for _, op := range field.Operators {
		if !slices.Contains(operatorsByKind[field.Kind], op) {
			return nil, fmt.Errorf("a %s field cannot take %s", kindName(field.Kind), OperatorLabel(op))
		}
	}
	if field.KeptWhenUnavailable && spec.Unavailable == nil {
		return nil, fmt.Errorf("kept_when_unavailable needs the list's unavailable_field")
	}
	if field.ValueRule != orkstackv1.FilterValueRule_FILTER_VALUE_RULE_UNSPECIFIED && field.Kind != KindText {
		return nil, fmt.Errorf("value_rule applies to a text field only")
	}
	if field.Kind == KindEnum {
		allowed, err := allowedValues(field.enum, decl.GetAllowedValues())
		if err != nil {
			return nil, err
		}
		field.AllowedValues = allowed
	} else if len(decl.GetAllowedValues()) > 0 {
		return nil, fmt.Errorf("allowed_values applies to an enum field only")
	}
	return field, nil
}

// allowedValues resolves an enum field's declared short names, or every
// declared value but UNSPECIFIED when it declares none.
func allowedValues(enum protoreflect.EnumDescriptor, declared []string) ([]string, error) {
	var out []string
	if len(declared) == 0 {
		values := enum.Values()
		for i := 0; i < values.Len(); i++ {
			if values.Get(i).Number() == 0 {
				continue
			}
			out = append(out, enumname.Short(enum, string(values.Get(i).Name())))
		}
		return out, nil
	}
	for _, name := range declared {
		value := enum.Values().ByName(protoreflect.Name(enumname.Proto(enum, name)))
		if value == nil || value.Number() == 0 {
			return nil, fmt.Errorf("allowed value %q is not a declared value of %s", name, enum.FullName())
		}
		short := enumname.Short(enum, string(value.Name()))
		if short != name {
			return nil, fmt.Errorf("allowed value %q must be spelled %q", name, short)
		}
		out = append(out, short)
	}
	return out, nil
}

func kindName(k Kind) string {
	switch k {
	case KindText:
		return "text"
	case KindEnum:
		return "enum"
	case KindBool:
		return "bool"
	case KindTime:
		return "timestamp"
	case KindLabels:
		return "labels"
	}
	return "unknown"
}

// splitPath splits a declared path on its dots.
func splitPath(path string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	return append(parts, path[start:])
}

// field finds the declared field a filter's path addresses, and the label
// key when it addresses a labels map. ok is false for an unknown path;
// keyMissing is true for a labels path with no key.
func (s *Spec) field(path string) (field *Field, key string, keyMissing, ok bool) {
	if f, found := s.byPath[path]; found {
		if f.Kind == KindLabels {
			return f, "", true, true
		}
		return f, "", false, true
	}
	for _, f := range s.Fields {
		if f.Kind != KindLabels {
			continue
		}
		prefix := f.Path + "."
		if len(path) >= len(prefix) && path[:len(prefix)] == prefix {
			key = path[len(prefix):]
			return f, key, key == "", true
		}
	}
	return nil, "", false, false
}
