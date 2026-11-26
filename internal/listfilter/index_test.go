package listfilter_test

import (
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

// servedRunsSpec is the allowlist ListRunsRequest serves, indexed fields
// included.
func servedRunsSpec(t *testing.T) *listfilter.Spec {
	t.Helper()
	return specOf(t, &orkstackv1.ListRunsRequest{})
}

// declare resolves an allowlist of fields over Run on a request message built
// for the one call, and returns the declaration error.
func declare(t *testing.T, name string, fields ...*orkstackv1.FilterableField) error {
	t.Helper()
	opts := &descriptorpb.MessageOptions{}
	proto.SetExtension(opts, orkstackv1.E_Filterable, &orkstackv1.ListFilterSpec{Resource: "orkstack.v1.Run", Fields: fields})
	file := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("listfilter_test/" + name + ".proto"),
		Package:    proto.String("listfilter.test." + name),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"orkstack/v1/filter.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:    proto.String("Request"),
			Options: opts,
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("filters"), JsonName: proto.String("filters"), Number: proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".orkstack.v1.FieldFilter"),
			}},
		}},
	}
	fd, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	_, err = listfilter.SpecForDescriptor(fd.Messages().Get(0))
	return err
}

// TestSpec_AnIndexedDeclarationIsHeldToItsRules: an indexed field takes
// equals only, carries none of a resource field's extras, and flag means
// nothing without indexed.
func TestSpec_AnIndexedDeclarationIsHeldToItsRules(t *testing.T) {
	EQ := []orkstackv1.FilterOperator{orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS}
	NE := []orkstackv1.FilterOperator{orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS}
	cases := []struct {
		name  string
		field *orkstackv1.FilterableField
		want  string
	}{
		{"notequals", &orkstackv1.FilterableField{Path: "x", Label: "X", Operators: NE, Indexed: true}, "an indexed field takes equals only"},
		{"flagalone", &orkstackv1.FilterableField{Path: "status", Label: "S", Operators: EQ, Flag: true}, "flag and ended_matches apply to an indexed field only"},
		{"endedalone", &orkstackv1.FilterableField{Path: "status", Label: "S", Operators: EQ, EndedMatches: true}, "flag and ended_matches apply to an indexed field only"},
		{"requires", &orkstackv1.FilterableField{Path: "x", Label: "X", Operators: EQ, Indexed: true, Requires: "status"}, "an indexed field cannot declare requires, kept_when_unavailable, allowed_values or value_rule"},
		{"allowed", &orkstackv1.FilterableField{Path: "x", Label: "X", Operators: EQ, Indexed: true, AllowedValues: []string{"A"}}, "an indexed field cannot declare requires, kept_when_unavailable, allowed_values or value_rule"},
		{"dotted", &orkstackv1.FilterableField{Path: "x.y", Label: "X", Operators: EQ, Indexed: true}, "an indexed path is one name, with no dot"},
		{"valuerule", &orkstackv1.FilterableField{Path: "x", Label: "X", Operators: EQ, Indexed: true, ValueRule: orkstackv1.FilterValueRule_FILTER_VALUE_RULE_NAME}, "an indexed field cannot declare requires, kept_when_unavailable, allowed_values or value_rule"},
		{"nooperator", &orkstackv1.FilterableField{Path: "x", Label: "X", Indexed: true}, "an indexed field takes equals only"},
		{"shadowscalar", &orkstackv1.FilterableField{Path: "orkflow_name", Label: "X", Operators: EQ, Indexed: true}, "an indexed path cannot name a filterable field of orkstack.v1.Run"},
		{"shadowtime", &orkstackv1.FilterableField{Path: "created_at", Label: "X", Operators: EQ, Indexed: true}, "an indexed path cannot name a filterable field of orkstack.v1.Run"},
	}
	for _, c := range cases {
		err := declare(t, c.name, c.field)
		if err == nil || !strings.HasSuffix(err.Error(), c.want) {
			t.Errorf("%s: declaration error %v, want one ending %q", c.name, err, c.want)
		}
	}
	if err := declare(t, "fine", &orkstackv1.FilterableField{Path: "anything", Label: "A", Operators: EQ, Indexed: true}); err != nil {
		t.Errorf("a plain indexed field was refused: %v", err)
	}
}

// TestSpec_IndexedFieldsResolveWithoutAResourceField: queued, queued_on,
// holding and results_recorded name no field of Run (queued is even a message
// field), yet resolve, because they are declared indexed; only
// results_recorded matches an ended row too.
func TestSpec_IndexedFieldsResolveWithoutAResourceField(t *testing.T) {
	spec := servedRunsSpec(t)
	want := map[string]listfilter.Kind{"queued": listfilter.KindBool, "queued_on": listfilter.KindText, "holding": listfilter.KindText,
		"results_recorded": listfilter.KindBool}
	for _, f := range spec.Fields {
		if f.Indexed && f.EndedMatches != (f.Path == "results_recorded") {
			t.Errorf("%s: ended_matches = %v", f.Path, f.EndedMatches)
		}
	}
	for _, f := range spec.Fields {
		kind, ok := want[f.Path]
		if !ok {
			if f.Indexed {
				t.Errorf("%s is indexed; only %v are", f.Path, want)
			}
			continue
		}
		if !f.Indexed || f.Kind != kind {
			t.Errorf("%s: indexed %v, kind %v; want indexed, kind %v", f.Path, f.Indexed, f.Kind, kind)
		}
		if !slices.Equal(f.Operators, []orkstackv1.FilterOperator{orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS}) {
			t.Errorf("%s takes %v; an indexed field takes equals only", f.Path, f.Operators)
		}
		delete(want, f.Path)
	}
	if len(want) != 0 {
		t.Errorf("ListRuns does not declare %v", want)
	}
}

func TestCompile_IndexedFieldsTakeEqualsOnly(t *testing.T) {
	spec := servedRunsSpec(t)
	expectRefusal(t, spec, "filters[0]: holding does not take not equals (!=); it takes equals (=)", "holding!=db")
	expectRefusal(t, spec, "filters[0]: queued_on does not take in (=a,b); it takes equals (=)", "queued_on=a,b")
	expectRefusal(t, spec, "filters[0]: queued_on does not take prefix (^); it takes equals (=)", "queued_on^db")
}

func TestCompile_AFlagTakesOnlyTrue(t *testing.T) {
	spec := servedRunsSpec(t)
	for _, v := range []string{"false", "yes", "True", ""} {
		expectRefusal(t, spec, `filters[0]: queued takes only true, not "`+v+`"`, "queued="+v)
	}
	compile(t, spec, "queued=true")
}

func TestCompile_AnIndexedKeyIsAnyNonEmptyText(t *testing.T) {
	spec := servedRunsSpec(t)
	expectRefusal(t, spec, "filters[0]: holding needs a non-empty value", "holding=")
	set := compile(t, spec, "queued_on=db-migrate:prod")
	if got := set.Indexed(); !slices.Equal(got, []listfilter.IndexedFilter{{Path: "queued_on", Value: "db-migrate:prod"}}) {
		t.Errorf("Indexed() = %v", got)
	}
}

// TestSet_IndexedSplitsTheIndexFromTheRest: Indexed names every indexed
// filter, in canonical order, two on one path included, and WithoutIndexed is
// the rest, the part a row's own fields decide.
func TestSet_IndexedSplitsTheIndexFromTheRest(t *testing.T) {
	set := compile(t, servedRunsSpec(t), "status=RUNNING", "holding=b", "holding=a", "queued=true")
	want := []listfilter.IndexedFilter{{Path: "holding", Value: "a"}, {Path: "holding", Value: "b"}, {Path: "queued", Value: "true"}}
	if got := set.Indexed(); !slices.Equal(got, want) {
		t.Errorf("Indexed() = %v, want %v", got, want)
	}
	if got := set.WithoutIndexed().Texts(); !slices.Equal(got, []string{"status=RUNNING"}) {
		t.Errorf("WithoutIndexed() = %v, want [status=RUNNING]", got)
	}
	if got := compile(t, servedRunsSpec(t), "status=RUNNING").Indexed(); len(got) != 0 {
		t.Errorf("Indexed() of a set with no indexed filter = %v", got)
	}
}

// TestMatch_ARowWithoutIndexValuesNeverMatchesAnIndexedFilter is the rule
// that keeps a finished or archived run out of queued= and holding=: Match
// reads only the row's own fields, and an indexed field is none of them.
func TestMatch_ARowWithoutIndexValuesNeverMatchesAnIndexedFilter(t *testing.T) {
	spec := servedRunsSpec(t)
	rows := []proto.Message{
		&orkstackv1.Run{Id: "finished", Status: orkstackv1.Status_STATUS_SUCCEEDED},
		// Even a row that carries the queued message: the index decides, not the row.
		&orkstackv1.Run{Id: "archived-queued", Status: orkstackv1.Status_STATUS_FAILED, Queued: &orkstackv1.ConcurrencyQueued{Key: "db"}},
	}
	for _, text := range []string{"queued=true", "queued_on=db", "holding=db"} {
		expectRows(t, matching(t, spec, rows, text))
		set := compile(t, spec, text)
		for _, row := range rows {
			if set.MatchIndexed(row, nil) {
				t.Errorf("MatchIndexed(%s, nil) on %s = true", text, rowName(row))
			}
		}
	}
	// The row's own fields still decide the rest.
	expectRows(t, matching(t, spec, rows, "status=SUCCEEDED"), "finished")
}

// TestMatchIndexed_AsksTheIndexForEveryIndexedFilter: every indexed filter
// must hold (AND), each answered by holds, and the row's own fields decide
// the rest.
func TestMatchIndexed_AsksTheIndexForEveryIndexedFilter(t *testing.T) {
	spec := servedRunsSpec(t)
	row := &orkstackv1.Run{Id: "live", Status: orkstackv1.Status_STATUS_RUNNING}
	index := map[string][]string{"queued": {"true"}, "holding": {"a", "b"}}
	holds := func(path, value string) bool { return slices.Contains(index[path], value) }
	cases := []struct {
		texts []string
		want  bool
	}{
		{[]string{"queued=true"}, true},
		{[]string{"holding=a"}, true},
		{[]string{"holding=a", "holding=b"}, true},
		{[]string{"holding=a", "holding=c"}, false},
		{[]string{"queued_on=a"}, false},
		{[]string{"queued=true", "status=SUCCEEDED"}, false},
		{[]string{"queued=true", "status=RUNNING"}, true},
		{[]string{"status=RUNNING"}, true},
	}
	for _, c := range cases {
		if got := compile(t, spec, c.texts...).MatchIndexed(row, holds); got != c.want {
			t.Errorf("MatchIndexed(%q) = %v, want %v", c.texts, got, c.want)
		}
	}
}
