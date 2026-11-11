package dsl_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

func sharedDocument(namespace, sharedWith string) string {
	ns := ""
	if namespace != "" {
		ns = "namespace: " + namespace + "\n"
	}
	return `apiVersion: orkstack/v1
kind: Orkflow
` + ns + `name: deploy
version: 1.2.3
` + sharedWith + `
tasks:
  - id: ship
    uses: shell
`
}

func TestCompile_SharedWithCompilesEveryLevel(t *testing.T) {
	wf, err := dsl.Compile([]byte(sharedDocument("team-a", `shared_with:
  - namespace: "*"
  - namespace: team-b
  - orkflow: team-c/release`)))
	if err != nil {
		t.Fatal(err)
	}
	got := wf.GetSharedWith()
	want := []*orkstackv1.OrkflowShare{{Namespace: "*"}, {Namespace: "team-b"}, {Orkflow: "team-c/release"}}
	if len(got) != len(want) {
		t.Fatalf("shared_with = %v", got)
	}
	for i := range want {
		if got[i].GetNamespace() != want[i].GetNamespace() || got[i].GetOrkflow() != want[i].GetOrkflow() {
			t.Fatalf("shared_with[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestCompile_SharedWithRefusesAMalformedEntry(t *testing.T) {
	cases := []struct {
		name, entries, field, want string
	}{
		{"both keys", "  - namespace: team-b\n    orkflow: team-b/release", "shared_with[0]", "exactly one of namespace and orkflow"},
		{"no key", "  - {}", "shared_with[0]", "exactly one of namespace and orkflow"},
		{"namespace not a slug", "  - namespace: Team", "shared_with[0].namespace", "not a slug"},
		{"orkflow without a namespace", "  - orkflow: release", "shared_with[0].orkflow", "<namespace>/<name>"},
		{"orkflow with an empty name", "  - orkflow: team-b/", "shared_with[0].orkflow", "<namespace>/<name>"},
		{"orkflow wildcard", "  - orkflow: team-b/*", "shared_with[0].orkflow", "not a slug"},
		{"orkflow every namespace", "  - orkflow: \"*/release\"", "shared_with[0].orkflow", "not a slug"},
		{"duplicate", "  - namespace: team-b\n  - namespace: team-b", "shared_with[1]", "appears twice"},
		{"own namespace", "  - namespace: team-a", "shared_with[0].namespace", "needs no entry"},
		{"own namespace orkflow", "  - orkflow: team-a/release", "shared_with[0].orkflow", "needs no entry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(sharedDocument("team-a", "shared_with:\n"+tc.entries)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Compile = %v, want an error naming %q", err, tc.want)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != tc.field {
				field := ""
				if verr != nil {
					field = verr.Field
				}
				t.Fatalf("field = %q, want %q (%v)", field, tc.field, err)
			}
		})
	}
}

func TestCompile_SharedWithIsBounded(t *testing.T) {
	var entries strings.Builder
	for i := range dsl.MaxSharedWith + 1 {
		fmt.Fprintf(&entries, "  - namespace: team-%d\n", i)
	}
	_, err := dsl.Compile([]byte(sharedDocument("team-a", "shared_with:\n"+entries.String())))
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("at most %d", dsl.MaxSharedWith)) {
		t.Fatalf("Compile = %v, want the bound named", err)
	}
}

func TestCanonicalOrkflow_SharedWithIsInTheDigestOnlyWhenSet(t *testing.T) {
	plain, err := dsl.Compile([]byte(sharedDocument("team-a", "")))
	if err != nil {
		t.Fatal(err)
	}
	before, err := dsl.CanonicalOrkflow(plain)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(before), "sharedWith") || strings.Contains(string(before), "shared_with") {
		t.Fatalf("an unshared Orkflow's canonical form names sharing: %s", before)
	}
	shared, err := dsl.Compile([]byte(sharedDocument("team-a", "shared_with:\n  - namespace: team-b")))
	if err != nil {
		t.Fatal(err)
	}
	after, err := dsl.CanonicalOrkflow(shared)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) == string(before) {
		t.Fatal("shared_with does not change the canonical form, so a new version could not change who may call it")
	}
	other, err := dsl.Compile([]byte(sharedDocument("team-a", "shared_with:\n  - namespace: team-c")))
	if err != nil {
		t.Fatal(err)
	}
	third, err := dsl.CanonicalOrkflow(other)
	if err != nil {
		t.Fatal(err)
	}
	if string(third) == string(after) {
		t.Fatal("two different shared_with lists have one canonical form")
	}
}

func TestCanonicalOrkflow_AQualifiedRefDiffersFromAnUnqualifiedOne(t *testing.T) {
	compile := func(uses string) []byte {
		wf, err := dsl.Compile([]byte(`apiVersion: orkstack/v1
kind: Orkflow
namespace: team-b
name: release
tasks:
  - id: deploy
    uses: ` + uses + `
`))
		if err != nil {
			t.Fatal(err)
		}
		out, err := dsl.CanonicalOrkflow(wf)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	own, foreign := compile("orkflow:deploy@1"), compile("orkflow:team-a/deploy@1")
	if strings.Contains(string(own), `"namespace":"team-a"`) {
		t.Fatalf("an unqualified ref names a namespace: %s", own)
	}
	if string(own) == string(foreign) {
		t.Fatal("orkflow:deploy@1 and orkflow:team-a/deploy@1 have one canonical form")
	}
}

func TestParseShare(t *testing.T) {
	accepted := []struct {
		share           *orkstackv1.OrkflowShare
		kind            dsl.ShareKind
		namespace, name string
		rendered        string
	}{
		{&orkstackv1.OrkflowShare{Namespace: "*"}, dsl.ShareEveryNamespace, "", "", "namespace:*"},
		{&orkstackv1.OrkflowShare{Namespace: "team-b"}, dsl.ShareNamespace, "team-b", "", "namespace:team-b"},
		{&orkstackv1.OrkflowShare{Orkflow: "team-b/release"}, dsl.ShareOrkflow, "team-b", "release", "orkflow:team-b/release"},
	}
	for _, tc := range accepted {
		got, err := dsl.ParseShare(tc.share)
		if err != nil || got.Kind != tc.kind || got.Namespace != tc.namespace || got.Name != tc.name || got.String() != tc.rendered {
			t.Errorf("ParseShare(%v) = %+v, %v", tc.share, got, err)
		}
	}
}
