package listfilter_test

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

func TestEquality_OnlyOneNonEmptyEquals(t *testing.T) {
	runs := runsSpec(t)
	for _, tc := range []struct {
		texts []string
		path  string
		want  string
		ok    bool
	}{
		{[]string{"orkflow_name=deploy"}, "orkflow_name", "deploy", true},
		{[]string{"status=failed,failed"}, "status", "FAILED", true}, // an in of one
		{[]string{"status=FAILED", "orkflow_name=x"}, "status", "FAILED", true},
		{[]string{"status=FAILED", "status=RUNNING"}, "status", "", false},
		{[]string{"orkflow_name="}, "orkflow_name", "", false},
		{[]string{"orkflow_name=a", "orkflow_name^b"}, "orkflow_name", "", false},
		{[]string{"orkflow_name!=a"}, "orkflow_name", "", false},
		{[]string{"status=FAILED,RUNNING"}, "status", "", false},
		{[]string{"orkflow_name=a"}, "status", "", false},
		{nil, "status", "", false},
	} {
		got, ok := compile(t, runs, tc.texts...).Equality(tc.path)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%q Equality(%s) = %q, %v; want %q, %v", tc.texts, tc.path, got, ok, tc.want, tc.ok)
		}
	}
}

func TestKey_SameSetAnyOrderSameKey(t *testing.T) {
	v := loadVectors(t)
	byRequest := map[string]*listfilter.Spec{}
	for _, row := range v.Canonical {
		spec := byRequest[row.Request]
		if spec == nil {
			md, err := findMessage(row.Request)
			if err != nil {
				t.Fatal(err)
			}
			if spec, err = listfilter.SpecForDescriptor(md); err != nil {
				t.Fatal(err)
			}
			byRequest[row.Request] = spec
		}
		set := compile(t, spec, row.Filters...)
		if got := set.Texts(); !slices.Equal(got, row.Texts) && !(len(got) == 0 && len(row.Texts) == 0) {
			t.Errorf("%s %q: texts %q; want %q", row.Request, row.Filters, got, row.Texts)
		}
		if got := set.Key(); got != row.Key {
			t.Errorf("%s %q: key %q; want %q", row.Request, row.Filters, got, row.Key)
		}
		reversed := slices.Clone(row.Filters)
		slices.Reverse(reversed)
		if got := compile(t, spec, reversed...).Key(); got != row.Key {
			t.Errorf("%s %q reversed: key %q; want %q", row.Request, row.Filters, got, row.Key)
		}
	}
}

func TestKey_DifferentSetDifferentKey(t *testing.T) {
	spec := namespacesSpec(t)
	seen := map[string][]string{}
	for _, texts := range [][]string{
		{"name=a"}, {"name=b"}, {"name!=a"}, {"name~a"}, {"name^a"}, {"name=a,b"},
		{"name=a", "name=b"}, {"description=a"}, {"labels.a=a"}, {"labels.b=a"},
		{"created_at>=2026-10-09T00:00:00Z"}, {"created_at<2026-10-09T00:00:00Z"},
	} {
		key := compile(t, spec, texts...).Key()
		if other, dup := seen[key]; dup {
			t.Errorf("%q and %q share key %s", texts, other, key)
		}
		seen[key] = texts
	}
}

func TestKey_EmptySetIsEmpty(t *testing.T) {
	set := compile(t, namespacesSpec(t))
	if !set.Empty() || set.Key() != "" {
		t.Errorf("empty set: Empty=%v Key=%q", set.Empty(), set.Key())
	}
	one := compile(t, namespacesSpec(t), "name=a")
	sum := sha256.Sum256([]byte("name=a"))
	if one.Key() != hex.EncodeToString(sum[:]) {
		t.Errorf("key of name=a = %s; want sha256 of its canonical text", one.Key())
	}
}

func TestRestrict(t *testing.T) {
	set := compile(t, namespacesSpec(t), "name^t", "labels.team=payments", "description~x")
	if got := set.Restrict("name").Texts(); !slices.Equal(got, []string{"name^t"}) {
		t.Errorf("Restrict(name) = %q", got)
	}
	if got := set.Restrict("labels", "name").Texts(); !slices.Equal(got, []string{"labels.team=payments", "name^t"}) {
		t.Errorf("Restrict(labels, name) = %q", got)
	}
	if set.Restrict("name").Len() == set.Len() {
		t.Errorf("Restrict kept every filter")
	}
}

func ns(name string, mutate ...func(*orkstackv1.Namespace)) *orkstackv1.Namespace {
	n := &orkstackv1.Namespace{Name: name}
	for _, m := range mutate {
		m(n)
	}
	return n
}

func matching(t *testing.T, spec *listfilter.Spec, rows []proto.Message, texts ...string) []string {
	t.Helper()
	set := compile(t, spec, texts...)
	var out []string
	for _, row := range rows {
		if set.Match(row) {
			out = append(out, rowName(row))
		}
	}
	return out
}

func rowName(row proto.Message) string {
	m := row.ProtoReflect()
	for _, name := range []string{"name", "id", "orker_id", "build_id"} {
		if fd := m.Descriptor().Fields().ByName(protoreflect.Name(name)); fd != nil && m.Get(fd).String() != "" {
			return m.Get(fd).String()
		}
	}
	return "?"
}

func expectRows(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) && !(len(got) == 0 && len(want) == 0) {
		t.Errorf("matched %q; want %q", got, want)
	}
}

var at = func(s string) *timestamppb.Timestamp {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return timestamppb.New(t)
}

func namespaceRows() []proto.Message {
	return []proto.Message{
		ns("alpha", func(n *orkstackv1.Namespace) {
			n.Labels = map[string]string{"team": "payments", "a.b": "dots"}
			n.Description = "Alpha team"
			n.CreatedAt = at("2026-10-01T00:00:00Z")
		}),
		ns("beta", func(n *orkstackv1.Namespace) {
			n.Labels = map[string]string{"team": "search"}
			n.Hold = &orkstackv1.NamespaceHold{Reason: "billing review"}
			n.CreatedAt = at("2026-10-05T12:00:00.000001Z")
		}),
		ns("default"),
	}
}

func TestMatch_Equals(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "name=beta"), "beta")
	expectRows(t, matching(t, spec, rows, "labels.team=payments"), "alpha")
	expectRows(t, matching(t, spec, rows, "labels.a.b=dots"), "alpha")
	expectRows(t, matching(t, spec, rows, "description="), "beta", "default")
	expectRows(t, matching(t, spec, rows, "name=beta", "name=alpha")) // AND of two values: nothing
	rel := specOf(t, &orkstackv1.ListReleasesRequest{})
	releases := []proto.Message{
		&orkstackv1.Release{BuildId: "b1", Role: orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT, Drained: false},
		&orkstackv1.Release{BuildId: "b2", Role: orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS, Drained: true},
		&orkstackv1.Release{BuildId: "b3"},
	}
	expectRows(t, matching(t, rel, releases, "role=current"), "b1")
	expectRows(t, matching(t, rel, releases, "drained=true"), "b2")
	expectRows(t, matching(t, rel, releases, "drained=false"), "b1", "b3")
}

func TestMatch_NotEquals(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "name!=beta"), "alpha", "default")
	expectRows(t, matching(t, spec, rows, "labels.team!=payments"), "beta", "default")
	rel := specOf(t, &orkstackv1.ListReleasesRequest{})
	releases := []proto.Message{
		&orkstackv1.Release{BuildId: "b1", Role: orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT},
		&orkstackv1.Release{BuildId: "b3"}, // UNSPECIFIED: != always holds
	}
	expectRows(t, matching(t, rel, releases, "role!=current"), "b3")
	expectRows(t, matching(t, rel, releases, "role!=previous"), "b1", "b3")
}

func TestMatch_In(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "name=default,alpha"), "alpha", "default")
	expectRows(t, matching(t, spec, rows, "labels.team=search,payments"), "alpha", "beta")
	rel := specOf(t, &orkstackv1.ListReleasesRequest{})
	releases := []proto.Message{
		&orkstackv1.Release{BuildId: "b1", Role: orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT},
		&orkstackv1.Release{BuildId: "b2", Role: orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING},
		&orkstackv1.Release{BuildId: "b3"}, // UNSPECIFIED: = and in never match
	}
	expectRows(t, matching(t, rel, releases, "role=current,ramping"), "b1", "b2")
}

func TestMatch_Contains(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "name~ta"), "beta")
	expectRows(t, matching(t, spec, rows, "description~team"), "alpha")
	expectRows(t, matching(t, spec, rows, "description~Team")) // case-sensitive
	expectRows(t, matching(t, spec, rows, "labels.team~pay"), "alpha")
}

func TestMatch_Prefix(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "name^de"), "default")
	expectRows(t, matching(t, spec, rows, "hold.reason^billing"), "beta")
	expectRows(t, matching(t, spec, rows, "labels.team^se"), "beta")
}

func TestMatch_AtOrAfter(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "created_at>=2026-10-01T00:00:00Z"), "alpha", "beta")
	expectRows(t, matching(t, spec, rows, "created_at>=2026-10-01T00:00:00.000001Z"), "beta")
}

func TestMatch_Before(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "created_at<2026-10-05T12:00:00.000001Z"), "alpha")
	expectRows(t, matching(t, spec, rows, "created_at<2026-10-05T12:00:00.000002Z"), "alpha", "beta")
}

func TestMatch_UnsetNestedMessageReadsZero(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "hold.reason!="), "beta")
	expectRows(t, matching(t, spec, rows, "hold.reason="), "alpha", "default")
	expectRows(t, matching(t, spec, rows, "telemetry.sink="), "alpha", "beta", "default")
}

func TestMatch_AbsentLabelReadsEmpty(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "labels.team="), "default")
	expectRows(t, matching(t, spec, rows, "labels.nope="), "alpha", "beta", "default")
}

func TestMatch_UnsetTimeNeverMatches(t *testing.T) {
	spec, rows := namespacesSpec(t), namespaceRows()
	expectRows(t, matching(t, spec, rows, "created_at<2100-01-01T00:00:00Z"), "alpha", "beta")
	expectRows(t, matching(t, spec, rows, "updated_at>=1970-01-01T00:00:00Z"))
}

func TestMatch_MicrosecondPrecision(t *testing.T) {
	spec := namespacesSpec(t)
	rows := []proto.Message{ns("x", func(n *orkstackv1.Namespace) { n.CreatedAt = at("2026-10-09T00:00:00.000000900Z") })}
	// Both sides truncate to the microsecond: 900ns reads as .000000.
	expectRows(t, matching(t, spec, rows, "created_at<2026-10-09T00:00:00.0000005Z"))
	expectRows(t, matching(t, spec, rows, "created_at>=2026-10-09T00:00:00.0000009Z"), "x")
	expectRows(t, matching(t, spec, rows, "created_at<2026-10-09T00:00:00.000001Z"), "x")
}

func TestMatch_UnavailableRowKeptOnlyOnUnmeasuredFields(t *testing.T) {
	spec := specOf(t, &orkstackv1.ListOrkersRequest{})
	rows := []proto.Message{
		&orkstackv1.Orker{OrkerId: "a", Pool: "build", BuildId: "r1"},
		&orkstackv1.Orker{OrkerId: "b", Pool: "gpu", Cordoned: true},
		&orkstackv1.Orker{OrkerId: "c", UnavailableReason: "did not answer"},
	}
	expectRows(t, matching(t, spec, rows, "pool=build"), "a", "c")
	expectRows(t, matching(t, spec, rows, "cordoned=true"), "b", "c")
	expectRows(t, matching(t, spec, rows, "build_id=r1", "pool=build"), "a", "c")
	// orker_id is always measured: the unreachable row filters normally.
	expectRows(t, matching(t, spec, rows, "orker_id=c"), "c")
	expectRows(t, matching(t, spec, rows, "orker_id!=c", "pool=build"), "a")
}

func TestMatch_EmptySetMatchesEverything(t *testing.T) {
	var zero listfilter.Set
	if !zero.Match(&orkstackv1.Namespace{}) || !zero.Empty() || zero.Key() != "" {
		t.Fatal("the zero Set must hold no filter and match every row")
	}
	if strings.Join(zero.Texts(), "") != "" {
		t.Fatal("zero Set has texts")
	}
}
