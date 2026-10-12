package main

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

// files builds a side of the diff from name/content pairs.
func files(pairs ...string) map[string]srcFile {
	out := map[string]srcFile{}
	for i := 0; i+1 < len(pairs); i += 2 {
		out[pairs[i]] = srcFile{Name: pairs[i], Content: []byte(pairs[i+1])}
	}
	return out
}

func all(m map[string]srcFile, extra ...srcFile) []srcFile {
	var out []srcFile
	for _, f := range m {
		out = append(out, f)
	}
	return append(out, extra...)
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

const integrationHelpers = `//go:build integration

package engine

import "testing"

// bootStack boots an engine.
func bootStack(t *testing.T) string { return dial(t) + "-stack" }

func dial(t *testing.T) string { return "addr" }

var defaultPool = "main"

func TestIntegration_Boots(t *testing.T) { _ = bootStack(t) }

func TestIntegration_Dials(t *testing.T) { _ = dial(t) }

func TestIntegration_Pools(t *testing.T) { _ = defaultPool }

func TestIntegration_Alone(t *testing.T) {}
`

func TestAnalyzeDir(t *testing.T) {
	unitFile := "package engine\n\nimport \"testing\"\n\nfunc TestUnit(t *testing.T) { _ = helper() }\n\nfunc helper() int { return 1 }\n"
	prod := "package engine\n\n// Run runs.\nfunc Run() int { return 1 }\n"
	cases := []struct {
		name       string
		cfg        string
		old, new   map[string]srcFile
		unchanged  []srcFile // the rest of the package on the new side
		tests      []string
		removed    []string
		whole      bool
		production bool
	}{
		{
			name: "a comment-only change needs no evidence",
			cfg:  cfgIntegration,
			old:  files("live_integration_test.go", integrationHelpers),
			new: files("live_integration_test.go", strings.Replace(integrationHelpers,
				"// bootStack boots an engine.", "// bootStack boots an engine, and says so at length.\n// More.", 1)),
		},
		{
			name: "reformatting and moving code is not a change",
			cfg:  cfgIntegration,
			old:  files("live_integration_test.go", integrationHelpers),
			new: files("live_integration_test.go", strings.Replace(
				strings.Replace(integrationHelpers, "func TestIntegration_Alone(t *testing.T) {}\n", "", 1),
				"var defaultPool = \"main\"", "func TestIntegration_Alone(t *testing.T) {\n}\n\nvar defaultPool = \"main\"", 1)),
		},
		{
			name:  "a changed test is selected",
			cfg:   cfgIntegration,
			old:   files("live_integration_test.go", integrationHelpers),
			new:   files("live_integration_test.go", strings.Replace(integrationHelpers, "func TestIntegration_Alone(t *testing.T) {}", "func TestIntegration_Alone(t *testing.T) { t.Log(1) }", 1)),
			tests: []string{"TestIntegration_Alone"},
		},
		{
			name:  "a changed helper selects every test that reaches it, transitively",
			cfg:   cfgIntegration,
			old:   files("live_integration_test.go", integrationHelpers),
			new:   files("live_integration_test.go", strings.Replace(integrationHelpers, `return "addr"`, `return "127.0.0.1:7233"`, 1)),
			tests: []string{"TestIntegration_Boots", "TestIntegration_Dials"},
		},
		{
			name:  "a changed package-level var selects its readers",
			cfg:   cfgIntegration,
			old:   files("live_integration_test.go", integrationHelpers),
			new:   files("live_integration_test.go", strings.Replace(integrationHelpers, `"main"`, `"batch"`, 1)),
			tests: []string{"TestIntegration_Pools"},
		},
		{
			name: "a helper in another file of the package is reached too",
			cfg:  cfgIntegration,
			old:  files("helpers_test.go", "package engine\n\nfunc shared() int { return 1 }\n"),
			new:  files("helpers_test.go", "package engine\n\nfunc shared() int { return 2 }\n"),
			unchanged: []srcFile{{Name: "live_integration_test.go", Content: []byte(
				"//go:build integration\n\npackage engine\n\nimport \"testing\"\n\nfunc TestIntegration_Shared(t *testing.T) { _ = shared() }\n\nfunc TestIntegration_Other(t *testing.T) {}\n")}},
			tests: []string{"TestIntegration_Shared"},
		},
		{
			name:    "a deleted test is removed, not selected",
			cfg:     cfgIntegration,
			old:     files("live_integration_test.go", integrationHelpers),
			new:     files("live_integration_test.go", strings.Replace(integrationHelpers, "func TestIntegration_Alone(t *testing.T) {}\n", "", 1)),
			removed: []string{"TestIntegration_Alone"},
		},
		{
			name:    "a deleted file's tests are removed",
			cfg:     cfgIntegration,
			old:     files("live_integration_test.go", integrationHelpers),
			new:     files(),
			removed: []string{"TestIntegration_Alone", "TestIntegration_Boots", "TestIntegration_Dials", "TestIntegration_Pools"},
		},
		{
			name: "a deleted helper still maps, through the tests that changed with it",
			cfg:  cfgIntegration,
			old:  files("live_integration_test.go", integrationHelpers),
			new: files("live_integration_test.go", strings.Replace(
				strings.Replace(integrationHelpers, "func dial(t *testing.T) string { return \"addr\" }\n", "", 1),
				"_ = dial(t)", "_ = 1", 1)),
			tests: []string{"TestIntegration_Boots", "TestIntegration_Dials"},
		},
		{
			name:  "a new file's tests are selected",
			cfg:   cfgIntegration,
			old:   files(),
			new:   files("new_integration_test.go", "//go:build integration\n\npackage engine\n\nimport \"testing\"\n\nfunc TestIntegration_New(t *testing.T) {}\n"),
			tests: []string{"TestIntegration_New"},
		},
		{
			name:  "TestMain selects the whole package",
			cfg:   cfgIntegration,
			old:   files("main_integration_test.go", "//go:build integration\n\npackage engine\n\nimport \"testing\"\n\nfunc TestMain(m *testing.M) { m.Run() }\n"),
			new:   files("main_integration_test.go", "//go:build integration\n\npackage engine\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\nfunc TestMain(m *testing.M) { os.Exit(m.Run()) }\n"),
			whole: true,
		},
		{
			name:  "an integration-tagged file does not exist in the unit configuration",
			cfg:   cfgUnit,
			old:   files("live_integration_test.go", integrationHelpers),
			new:   files("live_integration_test.go", strings.Replace(integrationHelpers, `"main"`, `"batch"`, 1)),
			tests: nil,
		},
		{
			name:  "a changed build constraint selects the file's tests",
			cfg:   cfgIntegration,
			old:   files("x_test.go", "package engine\n\nimport \"testing\"\n\nfunc TestX(t *testing.T) {}\n"),
			new:   files("x_test.go", "//go:build integration\n\npackage engine\n\nimport \"testing\"\n\nfunc TestX(t *testing.T) {}\n"),
			tests: []string{"TestX"},
		},
		{
			name:       "a production change is production, and selects no test by itself",
			cfg:        cfgUnit,
			old:        files("run.go", prod),
			new:        files("run.go", strings.Replace(prod, "return 1", "return 2", 1)),
			unchanged:  []srcFile{{Name: "run_test.go", Content: []byte("package engine\n\nimport \"testing\"\n\nfunc TestRun(t *testing.T) { _ = Run() }\n")}},
			production: true,
		},
		{
			name: "a production comment is not production",
			cfg:  cfgUnit,
			old:  files("run.go", prod),
			new:  files("run.go", strings.Replace(prod, "// Run runs.", "// Run runs, quickly.", 1)),
		},
		{
			name:  "unit helpers select unit tests in the unit configuration",
			cfg:   cfgUnit,
			old:   files("u_test.go", unitFile),
			new:   files("u_test.go", strings.Replace(unitFile, "return 1", "return 2", 1)),
			tests: []string{"TestUnit"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ch := analyzeDir(c.cfg, "internal/engine", c.old, c.new, all(c.new, c.unchanged...))
			if got := keys(ch.Tests); !reflect.DeepEqual(got, orEmpty(c.tests)) {
				t.Errorf("tests = %v, want %v (%v)", got, c.tests, ch.Tests)
			}
			if !reflect.DeepEqual(orEmpty(ch.Removed), orEmpty(c.removed)) {
				t.Errorf("removed = %v, want %v", ch.Removed, c.removed)
			}
			if (ch.Whole != "") != c.whole {
				t.Errorf("whole = %q, want %v", ch.Whole, c.whole)
			}
			if ch.Production != c.production {
				t.Errorf("production = %v (%v), want %v", ch.Production, ch.ProductionWhy, c.production)
			}
		})
	}
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func TestTestsDeclaredSplitsUnitFromIntegration(t *testing.T) {
	fs := []srcFile{
		{Name: "a_test.go", Content: []byte("package p\n\nimport \"testing\"\n\nfunc TestUnitOnly(t *testing.T) {}\n")},
		{Name: "b_integration_test.go", Content: []byte("//go:build integration\n\npackage p\n\nimport \"testing\"\n\nfunc TestIntegration_X(t *testing.T) {}\n\nfunc testhelper(t *testing.T) {}\n\nfunc Testable() {}\n")},
		{Name: "c_test.go", Content: []byte("//go:build !integration\n\npackage p\n\nimport \"testing\"\n\nfunc TestNotIntegration(t *testing.T) {}\n")},
		{Name: "d.go", Content: []byte("package p\n\nfunc TestLooksLikeATest() {}\n")},
	}
	unit, integ := testsDeclared("p", fs)
	if !unit["TestUnitOnly"] || !unit["TestNotIntegration"] || len(unit) != 2 {
		t.Errorf("unit = %v", unit)
	}
	if !integ["TestIntegration_X"] || len(integ) != 1 {
		t.Errorf("integration = %v (Testable is not a test: its next rune is lower case)", integ)
	}
}

func TestTestsReferencingFollowsHelpers(t *testing.T) {
	fs := []srcFile{{Name: "x_integration_test.go", Content: []byte(`//go:build integration

package main

import "testing"

func signer(t *testing.T) string { return build(t, "./test/tools/cosign-sign") }

func build(t *testing.T, p string) string { return p }

func TestIntegration_Signs(t *testing.T) { _ = signer(t) }

func TestIntegration_Direct(t *testing.T) { _ = build(t, "./test/tools/cosign-sign") }

func TestIntegration_Unrelated(t *testing.T) { _ = build(t, "./cmd/ork") }
`)}}
	got := testsReferencing("cmd/orkctl", fs, func(s string) bool { return strings.Contains(s, "test/tools/cosign-sign") })
	if want := []string{"TestIntegration_Direct", "TestIntegration_Signs"}; !reflect.DeepEqual(keys(got), want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
