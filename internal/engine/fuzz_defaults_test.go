package engine

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// FuzzApplyInputDefaults: the declared defaults fill what the caller omitted
// and nothing else. A caller's field wins, including an explicit null;
// undeclared fields survive; the input is never changed; the result is a
// fixed point; and the same inputs always give the same output, since this
// runs in workflow code.
func FuzzApplyInputDefaults(f *testing.F) {
	declared, err := structpb.NewStruct(map[string]any{
		"url":     map[string]any{"type": "string", "default": "https://example.test"},
		"retries": map[string]any{"type": "number", "default": 3.0},
		"flag":    map[string]any{"type": "boolean", "default": false},
		"nulled":  map[string]any{"type": "string", "default": nil},
		"name":    map[string]any{"type": "string", "required": true},
		"broken":  "not a declaration",
	})
	if err != nil {
		f.Fatal(err)
	}
	wf := &orkstackv1.Orkflow{Inputs: declared}
	for _, input := range []map[string]any{
		nil, {}, {"url": "https://other.test"}, {"retries": nil}, {"flag": true, "extra": []any{1.0, "x"}}, {"name": "n", "nulled": "v"},
	} {
		payload := &orkstackv1.Payload{}
		if input != nil {
			data, err := structpb.NewStruct(input)
			if err != nil {
				f.Fatal(err)
			}
			payload.Data = data
		}
		wire, err := proto.Marshal(payload)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(wire, true)
		f.Add(wire, false)
	}
	f.Fuzz(func(t *testing.T, wire []byte, present bool) {
		var input *orkstackv1.Payload
		if present {
			input = &orkstackv1.Payload{}
			if err := proto.Unmarshal(wire, input); err != nil {
				return
			}
		}
		before := proto.Clone(input)
		got := ApplyInputDefaults(wf, input)
		if !proto.Equal(before, input) {
			t.Fatalf("ApplyInputDefaults changed its input: %v became %v", before, input)
		}
		fields := got.GetData().GetFields()
		for name, value := range input.GetData().GetFields() {
			if !proto.Equal(fields[name], value) {
				t.Fatalf("the caller's %q = %v became %v", name, value, fields[name])
			}
		}
		for name, declaration := range declared.GetFields() {
			spec := declaration.GetStructValue()
			def, hasDefault := spec.GetFields()["default"]
			_, provided := input.GetData().GetFields()[name]
			switch {
			case provided:
			case hasDefault:
				if !proto.Equal(fields[name], def) {
					t.Fatalf("the omitted %q = %v, want its default %v", name, fields[name], def)
				}
			default:
				if _, filled := fields[name]; filled {
					t.Fatalf("%q has no default but was filled with %v", name, fields[name])
				}
			}
		}
		if len(fields) != len(input.GetData().GetFields())+countFilled(declared, input) {
			t.Fatalf("the result holds %d fields; input %v and defaults account for %d", len(fields), input, len(input.GetData().GetFields())+countFilled(declared, input))
		}
		again := ApplyInputDefaults(wf, got)
		if !proto.Equal(again, got) {
			t.Fatalf("ApplyInputDefaults is not a fixed point: %v then %v", got, again)
		}
		if second := ApplyInputDefaults(wf, input); !proto.Equal(second, got) {
			t.Fatalf("ApplyInputDefaults is not deterministic: %v then %v", got, second)
		}
	})
}

// countFilled counts the declared defaults the input leaves unset.
func countFilled(declared *structpb.Struct, input *orkstackv1.Payload) int {
	n := 0
	for name, declaration := range declared.GetFields() {
		if _, hasDefault := declaration.GetStructValue().GetFields()["default"]; !hasDefault {
			continue
		}
		if _, provided := input.GetData().GetFields()[name]; !provided {
			n++
		}
	}
	return n
}
