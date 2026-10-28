package listfilter_test

import (
	"encoding/json"
	"os"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

// runsRequest is a request message built at test time that declares the
// allowlist the runs list will (enum allowed values, requires, a time
// interval) — features no stage-1 list exercises. It lives in its own test
// package so it can never be confused with a served request.
var (
	runsOnce sync.Once
	runsDesc protoreflect.MessageDescriptor
)

func runsSpec(t testing.TB) *listfilter.Spec {
	t.Helper()
	runsOnce.Do(func() {
		S5 := []orkstackv1.FilterOperator{1, 2, 3, 4, 5}
		E3 := []orkstackv1.FilterOperator{1, 2, 3}
		EQ := []orkstackv1.FilterOperator{1}
		T2 := []orkstackv1.FilterOperator{6, 7}
		opts := &descriptorpb.MessageOptions{}
		proto.SetExtension(opts, orkstackv1.E_Filterable, &orkstackv1.ListFilterSpec{
			Resource: "orkstack.v1.Run",
			Fields: []*orkstackv1.FilterableField{
				{Path: "orkflow_name", Label: "Orkflow", Operators: S5, ValueRule: orkstackv1.FilterValueRule_FILTER_VALUE_RULE_NAME},
				{Path: "status", Label: "Status", Operators: E3,
					AllowedValues: []string{"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED", "SKIPPED"}},
				{Path: "created_at", Label: "Created", Operators: T2},
				{Path: "origin.kind", Label: "Started by", Operators: EQ,
					AllowedValues: []string{"SUBMIT", "TRIGGER", "SCHEDULE", "PARENT_RUN", "RUN_AGAIN"}},
				{Path: "origin.id", Label: "Origin id", Operators: EQ, Requires: "origin.kind"},
			},
		})
		file := &descriptorpb.FileDescriptorProto{
			Name:       proto.String("listfilter_test/runs.proto"),
			Package:    proto.String("listfilter.test"),
			Syntax:     proto.String("proto3"),
			Dependency: []string{"orkstack/v1/filter.proto"},
			MessageType: []*descriptorpb.DescriptorProto{{
				Name:    proto.String("ListRunsTestRequest"),
				Options: opts,
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:     proto.String("filters"),
					JsonName: proto.String("filters"),
					Number:   proto.Int32(1),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: proto.String(".orkstack.v1.FieldFilter"),
				}},
			}},
		}
		fd, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
		if err != nil {
			panic(err)
		}
		runsDesc = fd.Messages().Get(0)
	})
	spec, err := listfilter.SpecForDescriptor(runsDesc)
	if err != nil {
		t.Fatalf("runs spec: %v", err)
	}
	return spec
}

func specOf(t testing.TB, request proto.Message) *listfilter.Spec {
	t.Helper()
	spec, err := listfilter.SpecFor(request)
	if err != nil {
		t.Fatalf("SpecFor(%T): %v", request, err)
	}
	return spec
}

// vectors is testdata/vectors.json, which ui/test/listFilter.test.ts reads
// too.
type vectors struct {
	Grammar []struct {
		Text   string `json:"text"`
		Filter *struct {
			Field  string   `json:"field"`
			Op     string   `json:"op"`
			Values []string `json:"values"`
		} `json:"filter"`
		Formatted string `json:"formatted"`
		Error     string `json:"error"`
	} `json:"grammar"`
	Canonical []struct {
		Request string   `json:"request"`
		Filters []string `json:"filters"`
		Texts   []string `json:"texts"`
		Key     string   `json:"key"`
	} `json:"canonical"`
}

func loadVectors(t testing.TB) vectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Grammar) < 40 {
		t.Fatalf("vectors.json holds %d grammar rows; the contract asks for at least 40", len(v.Grammar))
	}
	return v
}

func opOf(t *testing.T, short string) orkstackv1.FilterOperator {
	t.Helper()
	op, ok := orkstackv1.FilterOperator_value["FILTER_OPERATOR_"+short]
	if !ok {
		t.Fatalf("unknown operator %q in vectors", short)
	}
	return orkstackv1.FilterOperator(op)
}

// parseAll parses CLI texts into wire filters, failing the test on a
// syntax error.
func parseAll(t *testing.T, texts ...string) []*orkstackv1.FieldFilter {
	t.Helper()
	out := make([]*orkstackv1.FieldFilter, len(texts))
	for i, text := range texts {
		f, err := listfilter.Parse(text)
		if err != nil {
			t.Fatalf("Parse(%q): %v", text, err)
		}
		out[i] = f
	}
	return out
}

// compile parses texts and compiles them, failing on any error.
func compile(t *testing.T, spec *listfilter.Spec, texts ...string) listfilter.Set {
	t.Helper()
	set, err := spec.Compile(parseAll(t, texts...))
	if err != nil {
		t.Fatalf("Compile(%q): %v", texts, err)
	}
	return set
}

// refusal compiles texts and returns the refusal's message, failing when
// the set compiles.
func refusal(t *testing.T, spec *listfilter.Spec, texts ...string) string {
	t.Helper()
	_, err := spec.Compile(parseAll(t, texts...))
	if err == nil {
		t.Fatalf("Compile(%q) succeeded; want a refusal", texts)
	}
	var lf *listfilter.Error
	if !asError(err, &lf) {
		t.Fatalf("Compile(%q) = %T %v; want *listfilter.Error", texts, err, err)
	}
	return lf.Message
}
