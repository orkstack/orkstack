package engine

import (
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ApplyInputDefaults clones input and fills omitted fields from the orkflow's
// declared input defaults. Caller fields always win, including explicit null;
// undeclared fields are preserved unchanged.
//
// This helper is safe in workflow code: it has no side effects and sorts map
// keys before traversing them.
func ApplyInputDefaults(wf *orkstackv1.Orkflow, input *orkstackv1.Payload) *orkstackv1.Payload {
	var out *orkstackv1.Payload
	if input == nil {
		out = &orkstackv1.Payload{}
	} else {
		out = proto.Clone(input).(*orkstackv1.Payload)
	}

	declarations := wf.GetInputs().GetFields()
	if len(declarations) == 0 {
		return out
	}

	keys := make([]string, 0, len(declarations))
	for name := range declarations {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	for _, name := range keys {
		if out.GetData() != nil {
			if _, provided := out.GetData().GetFields()[name]; provided {
				continue
			}
		}
		declaration := declarations[name].GetStructValue()
		if declaration == nil {
			continue
		}
		defaultValue, hasDefault := declaration.GetFields()["default"]
		if !hasDefault {
			continue
		}
		if out.Data == nil {
			out.Data = &structpb.Struct{Fields: map[string]*structpb.Value{}}
		}
		if out.Data.Fields == nil {
			out.Data.Fields = map[string]*structpb.Value{}
		}
		out.Data.Fields[name] = proto.Clone(defaultValue).(*structpb.Value)
	}
	return out
}
