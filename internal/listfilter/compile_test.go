package listfilter_test

import (
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

func namespacesSpec(t *testing.T) *listfilter.Spec {
	return specOf(t, &orkstackv1.ListNamespacesRequest{})
}

func expectRefusal(t *testing.T, spec *listfilter.Spec, want string, texts ...string) {
	t.Helper()
	if got := refusal(t, spec, texts...); got != want {
		t.Errorf("Compile(%q):\n got %q\nwant %q", texts, got, want)
	}
}

func TestCompile_RefusesUnknownFieldNamingTheAllowlist(t *testing.T) {
	expectRefusal(t, runsSpec(t),
		`filters[1]: "statuss" is not a filterable field of Run; filterable: orkflow_name, status, created_at, origin.kind, origin.id`,
		"orkflow_name=a", "statuss=FAILED")
}

func TestCompile_UnknownFieldListsLabelsAsLabelsKEY(t *testing.T) {
	expectRefusal(t, namespacesSpec(t),
		`filters[0]: "nope" is not a filterable field of Namespace; filterable: name, description, labels.KEY, default_pool, hold.reason, require_signed_by, telemetry.sink, created_at, updated_at`,
		"nope=x")
}

func TestCompile_CamelCasePathIsUnknown(t *testing.T) {
	for _, path := range []string{"orkflowName", "createdAt", "originKind"} {
		got := refusal(t, runsSpec(t), path+"=x")
		want := fmt.Sprintf(`filters[0]: %q is not a filterable field of Run; filterable: orkflow_name, status, created_at, origin.kind, origin.id`, path)
		if got != want {
			t.Errorf("%s: got %q; want %q", path, got, want)
		}
	}
}

func TestCompile_RefusesLabelsWithoutAKey(t *testing.T) {
	expectRefusal(t, namespacesSpec(t), "filters[0]: name a key: labels.<key>", "labels=x")
	expectRefusal(t, namespacesSpec(t), "filters[0]: name a key: labels.<key>", "labels.=x")
}

func TestCompile_RefusesUnspecifiedOperator(t *testing.T) {
	_, err := runsSpec(t).Compile([]*orkstackv1.FieldFilter{{Field: "orkflow_name", Values: []string{"a"}}})
	if err == nil || err.Error() != "filters[0]: orkflow_name: no operator" {
		t.Fatalf("got %v; want filters[0]: orkflow_name: no operator", err)
	}
}

func TestCompile_RefusesOperatorTheFieldDoesNotTake(t *testing.T) {
	expectRefusal(t, runsSpec(t),
		"filters[0]: status does not take contains (~); it takes equals (=), not equals (!=), in (=a,b)",
		"status~FAIL")
	expectRefusal(t, runsSpec(t),
		"filters[0]: created_at does not take equals (=); it takes at or after (>=), before (<)",
		"created_at=2026-10-09T00:00:00Z")
}

func TestCompile_RefusesWrongValueCount(t *testing.T) {
	spec := namespacesSpec(t)
	for _, tc := range []struct {
		filter *orkstackv1.FieldFilter
		want   string
	}{
		{&orkstackv1.FieldFilter{Field: "name", Op: 1}, "filters[0]: equals (=) takes exactly one value"},
		{&orkstackv1.FieldFilter{Field: "name", Op: 1, Values: []string{"a", "b"}}, "filters[0]: equals (=) takes exactly one value"},
		{&orkstackv1.FieldFilter{Field: "name", Op: 2, Values: []string{"a", "b"}}, "filters[0]: not equals (!=) takes exactly one value"},
		{&orkstackv1.FieldFilter{Field: "created_at", Op: 6}, "filters[0]: at or after (>=) takes exactly one value"},
		{&orkstackv1.FieldFilter{Field: "name", Op: 3}, "filters[0]: in (=a,b) takes 1 to 64 values"},
	} {
		_, err := spec.Compile([]*orkstackv1.FieldFilter{tc.filter})
		if err == nil || err.Error() != tc.want {
			t.Errorf("%v: got %v; want %q", tc.filter, err, tc.want)
		}
	}
}

func TestCompile_RefusesInWithMoreThan64OrAnEmptyValue(t *testing.T) {
	spec := namespacesSpec(t)
	many := make([]string, 65)
	for i := range many {
		many[i] = fmt.Sprintf("n%d", i)
	}
	_, err := spec.Compile([]*orkstackv1.FieldFilter{{Field: "name", Op: 3, Values: many}})
	if err == nil || err.Error() != "filters[0]: in (=a,b) takes 1 to 64 values" {
		t.Errorf("65 values: got %v", err)
	}
	if _, err := spec.Compile([]*orkstackv1.FieldFilter{{Field: "name", Op: 3, Values: many[:64]}}); err != nil {
		t.Errorf("64 values: %v", err)
	}
	_, err = spec.Compile([]*orkstackv1.FieldFilter{{Field: "name", Op: 3, Values: []string{"a", ""}}})
	if err == nil || err.Error() != "filters[0]: in (=a,b) cannot hold an empty value" {
		t.Errorf("empty value: got %v", err)
	}
}

func TestCompile_RefusesEmptyContainsAndPrefix(t *testing.T) {
	expectRefusal(t, namespacesSpec(t), "filters[0]: contains (~) needs a non-empty value", "name~")
	expectRefusal(t, namespacesSpec(t), "filters[0]: prefix (^) needs a non-empty value", "description^")
}

func TestCompile_RefusesValueOver1KiBOrNotUTF8(t *testing.T) {
	spec := namespacesSpec(t)
	want := "filters[0]: a value is at most 1024 bytes of UTF-8"
	for _, value := range []string{strings.Repeat("x", 1025), "\xff\xfe"} {
		_, err := spec.Compile([]*orkstackv1.FieldFilter{{Field: "description", Op: 4, Values: []string{value}}})
		if err == nil || err.Error() != want {
			t.Errorf("%.20q: got %v; want %q", value, err, want)
		}
	}
	if _, err := spec.Compile([]*orkstackv1.FieldFilter{{Field: "description", Op: 4, Values: []string{strings.Repeat("x", 1024)}}}); err != nil {
		t.Errorf("1024 bytes: %v", err)
	}
}

func TestCompile_RefusesEnumValueNotAllowedOrUnspecified(t *testing.T) {
	const list = "PENDING, RUNNING, SUCCEEDED, FAILED, CANCELED, SKIPPED"
	expectRefusal(t, runsSpec(t), `filters[0]: "AWAITING" is not a status this list can select; status is one of `+list, "status=AWAITING")
	expectRefusal(t, runsSpec(t), `filters[0]: "UNSPECIFIED" is not a status this list can select; status is one of `+list, "status=UNSPECIFIED")
	expectRefusal(t, runsSpec(t), `filters[0]: "nonsense" is not a status this list can select; status is one of `+list, "status!=nonsense")
	expectRefusal(t, specOf(t, &orkstackv1.ListReleasesRequest{}),
		`filters[0]: "BOGUS" is not a role this list can select; role is one of CURRENT, RAMPING, PREVIOUS, CANDIDATE`, "role=CURRENT,BOGUS")
}

func TestCompile_RefusesBoolSpelling(t *testing.T) {
	for _, v := range []string{"yes", "True", "1", ""} {
		expectRefusal(t, specOf(t, &orkstackv1.ListReleasesRequest{}),
			fmt.Sprintf("filters[0]: drained is true or false, not %q", v), "drained="+v)
	}
}

func TestCompile_RefusesTimeWithoutAZone(t *testing.T) {
	for _, v := range []string{"2026-10-09", "2026-10-09T00:00:00", "24h", ""} {
		expectRefusal(t, namespacesSpec(t),
			fmt.Sprintf("filters[0]: %q is not an RFC 3339 instant with a zone (2026-10-09T00:00:00Z)", v), "created_at>="+v)
	}
}

func TestCompile_RefusesUnmetRequires(t *testing.T) {
	expectRefusal(t, runsSpec(t), "filters[0]: origin.id needs an equals (=) filter on origin.kind", "origin.id=x")
	compile(t, runsSpec(t), "origin.id=x", "origin.kind=trigger")
}

func TestCompile_RefusesEmptyInterval(t *testing.T) {
	want := "created_at >= 2026-10-09T00:00:00Z and < 2026-10-08T00:00:00Z select no instant"
	got := refusal(t, runsSpec(t), "created_at>=2026-10-09T00:00:00Z", "created_at<2026-10-08T00:00:00Z")
	if got != want {
		t.Errorf("got %q; want %q", got, want)
	}
	refusal(t, runsSpec(t), "created_at>=2026-10-09T00:00:00Z", "created_at<2026-10-09T00:00:00Z")
	compile(t, runsSpec(t), "created_at>=2026-10-09T00:00:00Z", "created_at<2026-10-09T00:00:00.000001Z")
}

func TestCompile_RefusesMoreThan16Filters(t *testing.T) {
	texts := make([]string, 17)
	for i := range texts {
		texts[i] = fmt.Sprintf("name~%d", i)
	}
	expectRefusal(t, namespacesSpec(t), "at most 16 filters; got 17", texts...)
	compile(t, namespacesSpec(t), texts[:16]...)
}

func TestCompile_AcceptsEveryEnumSpelling(t *testing.T) {
	want := compile(t, runsSpec(t), "status=FAILED").Key()
	for _, spelling := range []string{"failed", "FAILED", "STATUS_FAILED", "Failed"} {
		if got := compile(t, runsSpec(t), "status="+spelling).Key(); got != want {
			t.Errorf("status=%s compiles to another set than status=FAILED", spelling)
		}
	}
	set := compile(t, runsSpec(t), "origin.kind=parent-run")
	if got := set.Texts(); len(got) != 1 || got[0] != "origin.kind=PARENT_RUN" {
		t.Errorf("origin.kind=parent-run canonical = %q; want origin.kind=PARENT_RUN", got)
	}
}

func TestCompile_InOfOneIsEquals(t *testing.T) {
	set := compile(t, namespacesSpec(t), "name=a,a")
	got := set.Proto()
	if len(got) != 1 || got[0].GetOp() != orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS {
		t.Fatalf("name=a,a = %v; want one equals filter", got)
	}
	if v, ok := set.Equality("name"); !ok || v != "a" {
		t.Errorf("Equality(name) = %q, %v; want a, true", v, ok)
	}
}

func TestCompile_CollapsesBoundsToTheTightest(t *testing.T) {
	set := compile(t, runsSpec(t),
		"created_at>=2026-10-01T00:00:00Z", "created_at>=2026-10-03T00:00:00Z", "created_at>=2026-10-02T00:00:00Z",
		"created_at<2026-11-01T00:00:00Z", "created_at<2026-10-20T00:00:00+02:00")
	from, before := set.Interval("created_at")
	if got := from.UTC().Format("2006-01-02T15:04:05Z07:00"); got != "2026-10-03T00:00:00Z" {
		t.Errorf("at or after = %s; want the latest, 2026-10-03T00:00:00Z", got)
	}
	if got := before.UTC().Format("2006-01-02T15:04:05Z07:00"); got != "2026-10-19T22:00:00Z" {
		t.Errorf("before = %s; want the earliest, 2026-10-19T22:00:00Z", got)
	}
	if got := set.Texts(); strings.Join(got, " ") != "created_at<2026-10-19T22:00:00Z created_at>=2026-10-03T00:00:00Z" {
		t.Errorf("texts = %q", got)
	}
}

func TestCompile_NameRuleOnWholeValues(t *testing.T) {
	webhooks := specOf(t, &orkstackv1.ListWebhooksRequest{})
	expectRefusal(t, webhooks, `filters[0]: namespace "Payments" is not a slug (^[a-z][a-z0-9-]*$)`, "namespace=Payments")
	expectRefusal(t, webhooks, `filters[0]: namespace "Bad" is not a slug (^[a-z][a-z0-9-]*$)`, "namespace=good,Bad")
	expectRefusal(t, webhooks, `filters[1]: orkflow_name "x y" is not a slug (^[a-z][a-z0-9-]*$)`, "name=a", "orkflow_name!=x y")
	expectRefusal(t, webhooks, fmt.Sprintf("filters[0]: name of 201 bytes exceeds the 200-byte limit (%.32q…)", strings.Repeat("a", 201)),
		"name="+strings.Repeat("a", 201))
	compile(t, webhooks, "namespace=")
	compile(t, webhooks, "name~-1")
	compile(t, webhooks, "name^Pay")
	compile(t, webhooks, "url=Not A Name") // url carries no rule
}

func TestCompile_OrkerIDRuleOnWholeValues(t *testing.T) {
	orkers := specOf(t, &orkstackv1.ListOrkersRequest{})
	expectRefusal(t, orkers, "filters[0]: orker_id: invalid orker id: empty", "orker_id=")
	expectRefusal(t, orkers, `filters[0]: orker_id: invalid orker id: "a b" contains whitespace or a control character at byte 1`, "orker_id=a b")
	expectRefusal(t, orkers, `filters[0]: orker_id: invalid orker id: "a b" contains whitespace or a control character at byte 1`, "orker_id!=a b")
	compile(t, orkers, "orker_id~a b")
	compile(t, orkers, "orker_id=host-1")
}

func TestCompile_ErrorCarriesTheIndex(t *testing.T) {
	_, err := namespacesSpec(t).Compile(parseAll(t, "name=a", "name~"))
	var lf *listfilter.Error
	if !asError(err, &lf) || lf.Index != 1 {
		t.Fatalf("got %#v; want an *Error at index 1", err)
	}
	_, err = namespacesSpec(t).Compile(parseAll(t, "created_at>=2026-10-09T00:00:00Z", "created_at<2026-10-01T00:00:00Z"))
	if !asError(err, &lf) || lf.Index != -1 {
		t.Fatalf("got %#v; want a set-level *Error", err)
	}
}
