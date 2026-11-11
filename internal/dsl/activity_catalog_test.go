package dsl

import (
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestCatalogResolveUses_AppliesTheFourRulesInOrder(t *testing.T) {
	catalog := BuiltinActivityCatalog()
	for _, tc := range []struct {
		name string
		uses string
		want string
		code string
	}{
		{"catalog name resolves to its reference", "shell",
			"ghcr.io/orkstack/activity-shell:0.2", ""},
		{"http", "http", "ghcr.io/orkstack/activity-http:0.2", ""},
		{"file", "file", "ghcr.io/orkstack/activity-file:0.2", ""},
		{"json", "json", "ghcr.io/orkstack/activity-json:0.2", ""},
		{"git", "git", "ghcr.io/orkstack/activity-git:0.2", ""},
		{"template", "template", "ghcr.io/orkstack/activity-template:0.2", ""},
		{"a full reference is itself", "ghcr.io/acme/migrate:v3",
			"ghcr.io/acme/migrate:v3", ""},
		{"a pinned reference is itself", "ghcr.io/acme/m@sha256:" + strings.Repeat("a", 64),
			"ghcr.io/acme/m@sha256:" + strings.Repeat("a", 64), ""},
		{"a bare docker hub name is a reference", "alpine:3.20", "alpine:3.20", ""},
		{"a template can never be pinned", "{{ .inputs.image }}", "", ReasonImageNotPinned},
		{"an unknown bare name names nothing", "migrate", "", ReasonUnknownActivity},
		{"a name with a template inside", "ghcr.io/acme/{{ .x }}:v1", "", ReasonImageNotPinned},
		{"an empty-ish garbage value", "NOT A NAME", "", ReasonUnknownActivity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := catalog.ResolveUses("t1", tc.uses)
			if tc.code != "" {
				var usesErr *UsesError
				if !errors.As(err, &usesErr) {
					t.Fatalf("uses %q: want *UsesError, got %v", tc.uses, err)
				}
				if usesErr.Code != tc.code {
					t.Fatalf("uses %q: code = %q, want %q", tc.uses, usesErr.Code, tc.code)
				}
				return
			}
			must.OKf(t, err, "uses %q: unexpected error", tc.uses)
			if got != tc.want {
				t.Fatalf("uses %q = %q, want %q", tc.uses, got, tc.want)
			}
		})
	}
}

// Rule 2 runs before rule 3, so a catalog name can never be read as a Docker
// Hub repository. This is the reason the rules are ordered at all.
// TestBuiltinActivityCatalog_IsTheSixFirstPartyImages pins the built-in key
// set: every place that names the first-party images as a set (the config
// default, images/Makefile, the local test catalog) is held to it by
// TestNames_AreEveryFirstPartyImage and the config completeness tests.
func TestBuiltinActivityCatalog_IsTheSixFirstPartyImages(t *testing.T) {
	want := map[string]string{
		"shell":    "ghcr.io/orkstack/activity-shell:0.2",
		"http":     "ghcr.io/orkstack/activity-http:0.2",
		"file":     "ghcr.io/orkstack/activity-file:0.2",
		"json":     "ghcr.io/orkstack/activity-json:0.2",
		"git":      "ghcr.io/orkstack/activity-git:0.2",
		"template": "ghcr.io/orkstack/activity-template:0.2",
	}
	got := BuiltinActivityCatalog()
	if len(got) != len(want) {
		t.Fatalf("built-in catalog has %d entries %v, want the six first-party images %v", len(got), got.Names(), keysOf(want))
	}
	for name, ref := range want {
		if got[name] != ref {
			t.Errorf("built-in catalog %q = %q, want %q", name, got[name], ref)
		}
	}
}

func TestCatalogResolveUses_CatalogNameBeatsDockerHubRepository(t *testing.T) {
	catalog := BuiltinActivityCatalog()
	got, err := catalog.ResolveUses("t1", "shell")
	must.OKf(t, err, "resolve shell")
	if got == "shell" || got == "library/shell" {
		t.Fatalf("`uses: shell` resolved to %q — rule 3 beat rule 2", got)
	}
	if got != "ghcr.io/orkstack/activity-shell:0.2" {
		t.Fatalf("resolve shell = %q", got)
	}
}

// `container` is not in the catalog and must not be resolvable: every task
// already is a container, so a workload names the image it wants directly.
func TestCatalogResolveUses_ContainerIsNotInTheCatalog(t *testing.T) {
	if _, ok := BuiltinActivityCatalog()["container"]; ok {
		t.Fatal("`container` is in the builtin catalog")
	}
	if _, err := BuiltinActivityCatalog().ResolveUses("t1", "container"); err == nil {
		t.Fatal("`uses: container` resolved through the catalog")
	}
}

func TestParseCatalog(t *testing.T) {
	t.Run("empty selects the builtin table", func(t *testing.T) {
		got, err := ParseCatalog(nil)
		must.OKf(t, err, "parse")
		if got["shell"] != BuiltinActivityCatalog()["shell"] {
			t.Fatalf("empty document did not select the builtin table: %v", got)
		}
	})

	t.Run("an explicit empty mapping is an empty catalog", func(t *testing.T) {
		got, err := ParseCatalog([]byte("activities: {}\n"))
		must.OKf(t, err, "parse")
		if len(got) != 0 {
			t.Fatalf("`activities: {}` = %v, want an empty catalog", got)
		}
	})

	t.Run("an override replaces the table", func(t *testing.T) {
		got, err := ParseCatalog([]byte(
			"activities:\n  shell: localhost:5001/orkstack/activity-shell:dev\n"))
		must.OKf(t, err, "parse")
		if got["shell"] != "localhost:5001/orkstack/activity-shell:dev" {
			t.Fatalf("shell = %q", got["shell"])
		}
		if _, ok := got["http"]; ok {
			t.Fatal("an override merged with the builtin table instead of replacing it")
		}
	})

	for _, tc := range []struct{ name, doc string }{
		{"no activities key", "something: else\n"},
		{"a non-slug name", "activities:\n  Shell: ghcr.io/x/y:1\n"},
		{"an empty reference", "activities:\n  shell: \"\"\n"},
		{"a reference that is not one", "activities:\n  shell: notareference\n"},
		// The reference grammar is checked when the catalog is read, not at the
		// first apply that names the activity: an operator's typo stops boot.
		{"a reference a runtime would read as an option", "activities:\n  shell: --cap-add=ALL.example/x:1\n"},
		{"a reference with upper-case path", "activities:\n  shell: ghcr.io/Acme/Shell:1\n"},
		{"a reference with a space", "activities:\n  shell: \"ghcr.io/acme/shell:1 --privileged\"\n"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			if _, err := ParseCatalog([]byte(tc.doc)); err == nil {
				t.Fatalf("ParseCatalog(%q) succeeded", tc.doc)
			}
		})
	}
}

// The compiler consults the catalog, so an unknown bare name is an authoring
// error rather than a run-time surprise.
func TestCompile_RejectsAnUnknownBareUses(t *testing.T) {
	_, err := Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: demo
tasks:
  - id: one
    uses: migrate
`))
	if err == nil {
		t.Fatal("compiled a document naming an activity the catalog has no entry for")
	}
	if !strings.Contains(err.Error(), "migrate") {
		t.Fatalf("error does not name the value: %v", err)
	}
}

func TestCompile_AcceptsAThirdPartyReferenceWithNoCatalogEntry(t *testing.T) {
	// This is the property that makes "any OCI image can become a durable
	// production activity" true: no catalog entry, no allow-list, no
	// registration step.
	if _, err := Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: demo
tasks:
  - id: one
    uses: ghcr.io/acme/migrate:v3
`)); err != nil {
		t.Fatalf("a third-party reference was refused: %v", err)
	}
}

func TestCompileWithOptions_HonoursAConfiguredCatalog(t *testing.T) {
	source := []byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: demo
tasks:
  - id: one
    uses: bespoke
`)
	if _, err := Compile(source); err == nil {
		t.Fatal("`bespoke` compiled against the builtin catalog")
	}
	wf, err := CompileWithOptions(source, Options{
		Catalog: Catalog{"bespoke": "ghcr.io/acme/bespoke:v1"},
	})
	must.OKf(t, err, "configured catalog")
	if wf.GetTasks()[0].GetUses() != "bespoke" {
		t.Fatalf("uses = %q, want the author's spelling preserved", wf.GetTasks()[0].GetUses())
	}
}
