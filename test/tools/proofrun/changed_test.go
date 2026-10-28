package main

import (
	"fmt"
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

const fuzzFile = `package engine

import "testing"

func FuzzDecode(f *testing.F) {
	f.Add("a")
	f.Fuzz(func(t *testing.T, s string) { _ = decode(s) })
}
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
		reached    []string // the tests in `tests` selected only by reaching a changed declaration
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
			name:    "a changed helper selects every test that reaches it, transitively",
			cfg:     cfgIntegration,
			old:     files("live_integration_test.go", integrationHelpers),
			new:     files("live_integration_test.go", strings.Replace(integrationHelpers, `return "addr"`, `return "127.0.0.1:7233"`, 1)),
			tests:   []string{"TestIntegration_Boots", "TestIntegration_Dials"},
			reached: []string{"TestIntegration_Boots", "TestIntegration_Dials"},
		},
		{
			name:    "a changed package-level var selects its readers",
			cfg:     cfgIntegration,
			old:     files("live_integration_test.go", integrationHelpers),
			new:     files("live_integration_test.go", strings.Replace(integrationHelpers, `"main"`, `"batch"`, 1)),
			tests:   []string{"TestIntegration_Pools"},
			reached: []string{"TestIntegration_Pools"},
		},
		{
			name: "a helper in another file of the package is reached too",
			cfg:  cfgIntegration,
			old:  files("helpers_test.go", "package engine\n\nfunc shared() int { return 1 }\n"),
			new:  files("helpers_test.go", "package engine\n\nfunc shared() int { return 2 }\n"),
			unchanged: []srcFile{{Name: "live_integration_test.go", Content: []byte(
				"//go:build integration\n\npackage engine\n\nimport \"testing\"\n\nfunc TestIntegration_Shared(t *testing.T) { _ = shared() }\n\nfunc TestIntegration_Other(t *testing.T) {}\n")}},
			tests:   []string{"TestIntegration_Shared"},
			reached: []string{"TestIntegration_Shared"},
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
			tests:   []string{"TestIntegration_Boots", "TestIntegration_Dials"},
			reached: []string{"TestIntegration_Boots"},
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
			name:       "a file that no longer parses is a change to the package, not a list of removed tests",
			cfg:        cfgIntegration,
			old:        files("live_integration_test.go", integrationHelpers),
			new:        files("live_integration_test.go", integrationHelpers+"\nfunc broken( {\n"),
			production: true,
		},
		{
			name:    "unit helpers select unit tests in the unit configuration",
			cfg:     cfgUnit,
			old:     files("u_test.go", unitFile),
			new:     files("u_test.go", strings.Replace(unitFile, "return 1", "return 2", 1)),
			tests:   []string{"TestUnit"},
			reached: []string{"TestUnit"},
		},
		{
			name:  "a changed fuzz target is selected",
			cfg:   cfgUnit,
			old:   files("fuzz_test.go", fuzzFile),
			new:   files("fuzz_test.go", strings.Replace(fuzzFile, `f.Add("a")`, `f.Add("b")`, 1)),
			tests: []string{"FuzzDecode"},
		},
		{
			name:      "a helper reached only by a fuzz target selects it",
			cfg:       cfgUnit,
			old:       files("helper_test.go", "package engine\n\nfunc decode(s string) int { return len(s) }\n"),
			new:       files("helper_test.go", "package engine\n\nfunc decode(s string) int { return len(s) + 1 }\n"),
			unchanged: []srcFile{{Name: "fuzz_test.go", Content: []byte(fuzzFile)}},
			tests:     []string{"FuzzDecode"},
			reached:   []string{"FuzzDecode"},
		},
		{
			name:    "a deleted fuzz target is removed, not selected",
			cfg:     cfgUnit,
			old:     files("fuzz_test.go", fuzzFile),
			new:     files(),
			removed: []string{"FuzzDecode"},
		},
		{
			name:  "a new fuzz target is selected",
			cfg:   cfgUnit,
			old:   files(),
			new:   files("fuzz_test.go", fuzzFile),
			tests: []string{"FuzzDecode"},
		},
		{
			name:  "a changed build constraint selects the file's fuzz targets",
			cfg:   cfgIntegration,
			old:   files("fuzz_test.go", fuzzFile),
			new:   files("fuzz_test.go", "//go:build integration\n\n"+fuzzFile),
			tests: []string{"FuzzDecode"},
		},
		{
			name: "a function that only looks like a fuzz target is not a test",
			cfg:  cfgUnit,
			old:  files("helper_test.go", "package engine\n\nimport \"testing\"\n\nfunc Fuzzy(f *testing.F) {}\n\nfunc FuzzHelper() int { return 1 }\n\nfunc FuzzOther(t *testing.T) {}\n"),
			new:  files("helper_test.go", "package engine\n\nimport \"testing\"\n\nfunc Fuzzy(f *testing.F) {}\n\nfunc FuzzHelper() int { return 2 }\n\nfunc FuzzOther(t *testing.T) { t.Log(1) }\n"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ch := analyzeDir(c.cfg, "internal/engine", c.old, c.new, all(c.new, c.unchanged...))
			if got := keys(ch.Tests); !reflect.DeepEqual(got, orEmpty(c.tests)) {
				t.Errorf("tests = %v, want %v (%v)", got, c.tests, ch.Tests)
			}
			if got := sortedKeys(ch.Reached); !reflect.DeepEqual(got, orEmpty(c.reached)) {
				t.Errorf("reached = %v, want %v", got, c.reached)
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

// methodFixture is a package whose methods share their names with unrelated
// identifiers: another type's method, a function, a local variable and a
// struct field. The pin-close change to a method named setup (and a new one
// named count) reached every test that mentioned any setup or count at all.
const methodFixture = `package engine

import (
	"testing"

	"example.com/elsewhere"
)

type order struct{ n int }

func (o *order) setup(h int) { o.n = h }

func (o *order) count() int { return o.n }

func (o *order) Name() string { return "order" }

func (o *order) name() string { return "order" }

func (o *order) behavior(in int) int { return in }

type rig struct{ o *order }

func newRig() *rig { return &rig{o: newOrder()} }

type other struct{}

func (other) setup() {}

func (other) Name() string { return "other" }

func count() int { return 0 }

func newOrder() *order { return &order{} }

type namer interface{ name() string }

func nameOf(n namer) string { return n.name() }

type registered interface{ run() }

var registry []registered

func init() { registry = append(registry, &stage{}) }

type stage struct{}

func (stage) run() {}

type alias = order

func (a *alias) tweak() {}

func TestUsesOrderThroughAConstructor(t *testing.T) {
	o := newOrder()
	o.setup(1)
	_ = o.count()
}

func TestUsesOrderDirectly(t *testing.T) {
	o := &order{}
	o.setup(2)
}

func TestUsesOrderThroughAField(t *testing.T) {
	r := newRig()
	r.o.setup(3)
}

func TestHandsOrderToAnotherPackage(t *testing.T) { elsewhere.Use(&order{}) }

func TestPassesTheMethodValue(t *testing.T) { elsewhere.Run(newOrder().behavior) }

func TestHasABehaviorVariable(t *testing.T) {
	behavior := func(int) int { return 0 }
	_ = behavior(1)
}

func TestCallsOrderThroughAnInterface(t *testing.T) { _ = nameOf(&order{}) }

func TestCallsOtherSetup(t *testing.T) {
	var x other
	x.setup()
	_ = x.Name()
}

func TestHasLocalIdentifiers(t *testing.T) {
	setup := func() {}
	setup()
	count := 1
	_ = count
}

func TestCallsCountFunction(t *testing.T) { _ = count() }

func TestReadsAField(t *testing.T) {
	s := struct{ setup, count, name int }{}
	_ = s.setup + s.count + s.name
}

func TestRunsRegisteredStages(t *testing.T) {
	for _, r := range registry {
		r.run()
	}
}

func TestTweaksThroughTheTarget(t *testing.T) {
	o := &order{}
	o.tweak()
}

func TestUnrelated(t *testing.T) {}
`

func TestAnalyzeDirResolvesMethodsByReceiver(t *testing.T) {
	change := func(from, to string) map[string]srcFile {
		if !strings.Contains(methodFixture, from) {
			t.Fatalf("fixture has no %q", from)
		}
		return files("methods_test.go", strings.Replace(methodFixture, from, to, 1))
	}
	cases := []struct {
		name string
		new  map[string]srcFile
		want map[string]string // test -> the helper it reaches
	}{
		{
			name: "a changed unexported method reaches the tests that call it on its type, and no other that mentions the name",
			new:  change("func (o *order) setup(h int) { o.n = h }", "func (o *order) setup(h int) { o.n = h + 1 }"),
			want: map[string]string{
				"TestUsesOrderThroughAConstructor": "order.setup",
				"TestUsesOrderDirectly":            "order.setup",
				"TestUsesOrderThroughAField":       "order.setup",
			},
		},
		{
			name: "a changed method reaches a test that passes it as a method value, not one with a variable of its name",
			new:  change("func (o *order) behavior(in int) int { return in }", "func (o *order) behavior(in int) int { return in + 1 }"),
			want: map[string]string{"TestPassesTheMethodValue": "order.behavior"},
		},
		{
			name: "a new method named like a function and a local variable reaches only the tests that call it on its type",
			new:  change("func (o *order) count() int { return o.n }", "func (o *order) count() int { return o.n + 1 }"),
			want: map[string]string{"TestUsesOrderThroughAConstructor": "order.count"},
		},
		{
			name: "a changed unexported method is reached through an interface that selects its name",
			new:  change(`func (o *order) name() string { return "order" }`, `func (o *order) name() string { return "ORDER" }`),
			want: map[string]string{"TestCallsOrderThroughAnInterface": "order.name"},
		},
		{
			name: "a changed exported method reaches the tests that reach its type, even with no selector: another package may call it",
			new:  change(`func (o *order) Name() string { return "order" }`, `func (o *order) Name() string { return "ORDER" }`),
			want: map[string]string{
				"TestUsesOrderThroughAConstructor": "order.Name",
				"TestUsesOrderDirectly":            "order.Name",
				"TestUsesOrderThroughAField":       "order.Name",
				"TestHandsOrderToAnotherPackage":   "order.Name",
				"TestPassesTheMethodValue":         "order.Name",
				"TestCallsOrderThroughAnInterface": "order.Name",
				"TestTweaksThroughTheTarget":       "order.Name",
			},
		},
		{
			name: "a method of a type only an init function mentions is reached by every test that selects it",
			new:  change("func (stage) run() {}", "func (stage) run() { _ = 1 }"),
			want: map[string]string{"TestRunsRegisteredStages": "stage.run"},
		},
		{
			name: "a method declared on an alias is resolved by its selector alone",
			new:  change("func (a *alias) tweak() {}", "func (a *alias) tweak() { _ = 1 }"),
			want: map[string]string{"TestTweaksThroughTheTarget": "alias.tweak"},
		},
		{
			name: "a changed function keeps its name key: every mention of the name reaches it",
			new:  change("func count() int { return 0 }", "func count() int { return 1 }"),
			want: map[string]string{
				"TestUsesOrderThroughAConstructor": "count",
				"TestHasLocalIdentifiers":          "count",
				"TestCallsCountFunction":           "count",
				"TestReadsAField":                  "count",
			},
		},
		{
			name: "a deleted method still maps through the tests that select its name on a type they mention",
			new:  change("func (o *order) setup(h int) { o.n = h }\n", ""),
			want: map[string]string{
				"TestUsesOrderThroughAConstructor": "order.setup",
				"TestUsesOrderDirectly":            "order.setup",
				"TestUsesOrderThroughAField":       "order.setup",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			old := files("methods_test.go", methodFixture)
			ch := analyzeDir(cfgUnit, "internal/engine", old, c.new, all(c.new))
			got := map[string]string{}
			for test, why := range ch.Tests {
				got[test] = strings.TrimPrefix(why, "reaches changed ")
				if !ch.Reached[test] {
					t.Errorf("%s: selected as changed, not as reached (%s)", test, why)
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("tests = %v, want %v", got, c.want)
			}
		})
	}
}

// A method promoted through an embedded field, and a method of a generic
// type, are reached through the type the test mentions.
func TestAnalyzeDirReachesPromotedAndGenericMethods(t *testing.T) {
	const src = `package engine

import "testing"

type base struct{}

func (base) setup() {}

type wrapper struct{ base }

func newWrapper() wrapper { return wrapper{} }

type box[T any] struct{ v T }

func (b *box[T]) put(v T) { b.v = v }

func newBox() *box[int] { return &box[int]{} }

func setup() {}

func TestCallsThePromotedMethod(t *testing.T) { newWrapper().setup() }

func TestCallsTheGenericMethod(t *testing.T) { newBox().put(1) }

func TestCallsAFunctionOfThatName(t *testing.T) { setup() }
`
	for _, c := range []struct {
		name     string
		from, to string
		want     map[string]string
	}{
		{"a promoted method", "func (base) setup() {}", "func (base) setup() { _ = 1 }",
			map[string]string{"TestCallsThePromotedMethod": "base.setup"}},
		{"a method of a generic type", "func (b *box[T]) put(v T) { b.v = v }", "func (b *box[T]) put(v T) { b.v = v; _ = 1 }",
			map[string]string{"TestCallsTheGenericMethod": "box.put"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			updated := strings.Replace(src, c.from, c.to, 1)
			if updated == src {
				t.Fatalf("fixture has no %q", c.from)
			}
			ch := analyzeDir(cfgUnit, "internal/engine", files("promoted_test.go", src), files("promoted_test.go", updated), all(files("promoted_test.go", updated)))
			got := map[string]string{}
			for name, why := range ch.Tests {
				got[name] = strings.TrimPrefix(why, "reaches changed ")
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("tests = %v, want %v", got, c.want)
			}
		})
	}
}

// A package of many tests that each mention a name some method shares: the
// change to the method selects the tests that call it on its type, not the
// package.
func TestAnalyzeDirMethodNamesDoNotSelectThePackage(t *testing.T) {
	var src strings.Builder
	src.WriteString("package engine\n\nimport \"testing\"\n\ntype order struct{}\n\nfunc (order) setup() {}\n\n")
	src.WriteString("type other struct{}\n\nfunc (other) setup() {}\n\nfunc setup() {}\n\n")
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&src, "func TestOther%d(t *testing.T) { var o other; o.setup(); setup() }\n\n", i)
	}
	src.WriteString("func TestOrder(t *testing.T) { var o order; o.setup() }\n")
	oldSrc := src.String()
	newSrc := strings.Replace(oldSrc, "func (order) setup() {}", "func (order) setup() { _ = 1 }", 1)
	ch := analyzeDir(cfgUnit, "internal/engine", files("many_test.go", oldSrc), files("many_test.go", newSrc), []srcFile{{Name: "many_test.go", Content: []byte(newSrc)}})
	if got := keys(ch.Tests); !reflect.DeepEqual(got, []string{"TestOrder"}) {
		t.Errorf("%d tests selected, want only TestOrder: %v", len(got), got[:min(len(got), 5)])
	}
}

func TestTestsReferencingResolvesMethodsByReceiver(t *testing.T) {
	fs := []srcFile{{Name: "x_integration_test.go", Content: []byte(`//go:build integration

package main

import "testing"

type signer struct{}

func (signer) run() string { return "./test/tools/cosign-sign" }

type other struct{}

func (other) run() {}

func run() {}

func TestIntegration_RunsTheSigner(t *testing.T) { var s signer; _ = s.run() }

func TestIntegration_RunsAnother(t *testing.T) { var o other; o.run(); run() }
`)}}
	got := testsReferencing("cmd/orkctl", fs, func(s string) bool { return strings.Contains(s, "test/tools/cosign-sign") })
	if want := []string{"TestIntegration_RunsTheSigner"}; !reflect.DeepEqual(keys(got), want) {
		t.Errorf("got %v, want %v", keys(got), want)
	}
}

// A string literal anywhere in a const or var group makes every name the
// group declares reference what it names: a group is read as one declaration
// for what a test reads, which can only over-select.
func TestTestsReferencingReadsAGroupAsOneDeclaration(t *testing.T) {
	fs := []srcFile{{Name: "x_test.go", Content: []byte(`package main

import "testing"

const (
	signer = "./test/tools/cosign-sign"
	other  = "./cmd/ork"
)

func TestReadsSigner(t *testing.T) { _ = signer }

func TestReadsOther(t *testing.T) { _ = other }

func TestReadsNeither(t *testing.T) {}
`)}}
	got := testsReferencing("cmd/orkctl", fs, func(s string) bool { return strings.Contains(s, "test/tools/cosign-sign") })
	if want := []string{"TestReadsOther", "TestReadsSigner"}; !reflect.DeepEqual(keys(got), want) {
		t.Errorf("got %v, want %v", keys(got), want)
	}
}

// A method a _test.go file adds to a production type (an export_test.go hook)
// is called by test code only, and every call selects its name: the mention
// of its type says nothing about it, exported or not, and a value of the type
// may come from a package this one cannot see into. Another package cannot
// call it through an interface, because the method exists only in the test
// build. An exported method the production file declares stays reached by its
// type alone, since a test may hand the value to another package.
func TestAnalyzeDirTestFileMethodOfAProductionTypeNeedsASelector(t *testing.T) {
	const prod = `package engine

type Service struct{ store store }

type store interface{ get() string }

func NewService() *Service { return &Service{} }

func (s *Service) Run() string { return s.store.get() }
`
	const test = `package engine

import (
	"testing"

	"example.com/elsewhere"
)

func (s *Service) HookForTest() int { return 1 }

func (s *Service) hook() int { return 1 }

type fakeStore struct{}

func (fakeStore) get() string { return "" }

func TestCallsTheHook(t *testing.T) { _ = NewService().HookForTest() }

func TestCallsTheUnexportedHook(t *testing.T) { _ = NewService().hook() }

func TestCallsTheHookOnAForeignValue(t *testing.T) { _ = elsewhere.Make().HookForTest() }

func TestOnlyBuildsAService(t *testing.T) { _ = NewService() }

func TestHandsTheServiceToAnotherPackage(t *testing.T) { elsewhere.Use(&Service{store: fakeStore{}}) }

func TestUnrelated(t *testing.T) {}
`
	for _, c := range []struct {
		name     string
		from, to string
		want     map[string]string
	}{
		{
			name: "an exported hook, whether or not the test names its type",
			from: "func (s *Service) HookForTest() int { return 1 }", to: "func (s *Service) HookForTest() int { return 2 }",
			want: map[string]string{
				"TestCallsTheHook":                "Service.HookForTest",
				"TestCallsTheHookOnAForeignValue": "Service.HookForTest",
			},
		},
		{
			name: "a deleted exported hook",
			from: "func (s *Service) HookForTest() int { return 1 }\n", to: "",
			want: map[string]string{
				"TestCallsTheHook":                "Service.HookForTest",
				"TestCallsTheHookOnAForeignValue": "Service.HookForTest",
			},
		},
		{
			name: "an unexported hook",
			from: "func (s *Service) hook() int { return 1 }", to: "func (s *Service) hook() int { return 2 }",
			want: map[string]string{"TestCallsTheUnexportedHook": "Service.hook"},
		},
		{
			name: "a fake the production type calls through its interface",
			from: `func (fakeStore) get() string { return "" }`, to: `func (fakeStore) get() string { return "x" }`,
			want: map[string]string{"TestHandsTheServiceToAnotherPackage": "fakeStore.get"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			updated := strings.Replace(test, c.from, c.to, 1)
			if updated == test {
				t.Fatalf("fixture has no %q", c.from)
			}
			prodFile := srcFile{Name: "service.go", Content: []byte(prod)}
			ch := analyzeDir(cfgUnit, "internal/engine", files("export_test.go", test), files("export_test.go", updated),
				all(files("export_test.go", updated), prodFile))
			got := map[string]string{}
			for name, why := range ch.Tests {
				got[name] = strings.TrimPrefix(why, "reaches changed ")
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("tests = %v, want %v", got, c.want)
			}
		})
	}
}

// Only a test file's init function is a root of every reach. Production code
// cannot mention a declaration of a test file, so what its init functions
// mention that a helper shares a name with is a coincidence: a helper called
// runIDs was reached by every test of pkg/artifacts through five production
// init functions that name some other runIDs. Each init is a node of its own,
// so that init#1 of one file is not init#1 of another.
func TestAnalyzeDirOnlyATestFilesInitIsARoot(t *testing.T) {
	const prod = `package engine

func init() { register(build) }

func register(f func()) {}

func build() {
	var runIDs []string
	_ = runIDs
}
`
	const head = `package engine

import "testing"

func runIDs() []string { return nil }

func TestReadsRunIDs(t *testing.T) { _ = runIDs() }
`
	const withInit = `
type stage struct{}

func (stage) Run() {}

var stages []interface{ Run() }

func init() { stages = append(stages, &stage{}) }

func TestRunsTheStages(t *testing.T) {
	for _, s := range stages {
		s.Run()
	}
}
`
	for _, c := range []struct {
		name string
		test string
	}{
		{"a production init beside tests that mention no init", head},
		{"a production init numbered like the test file's", head + withInit},
	} {
		t.Run(c.name, func(t *testing.T) {
			var src strings.Builder
			src.WriteString(c.test)
			for i := 0; i < 50; i++ {
				fmt.Fprintf(&src, "\nfunc TestUnrelated%d(t *testing.T) {}\n", i)
			}
			oldSrc := src.String()
			newSrc := strings.Replace(oldSrc, "func runIDs() []string { return nil }", "func runIDs() []string { return []string{} }", 1)
			if newSrc == oldSrc {
				t.Fatal("fixture has no runIDs to change")
			}
			prodFile := srcFile{Name: "register.go", Content: []byte(prod)}
			ch := analyzeDir(cfgUnit, "pkg/artifacts", files("init_test.go", oldSrc), files("init_test.go", newSrc),
				all(files("init_test.go", newSrc), prodFile))
			if got, want := keys(ch.Tests), []string{"TestReadsRunIDs"}; !reflect.DeepEqual(got, want) {
				t.Errorf("%d tests selected, want only TestReadsRunIDs: %v", len(got), got[:min(len(got), 5)])
			}
		})
	}
}

// An init function of a test file runs before every test, so a string it
// holds is read by every test of the package; one of a production file is
// not a root.
func TestTestsReferencingReadsATestFilesInit(t *testing.T) {
	const prod = `package main

func init() { other = "./test/tools/cosign-sign" }

var other string
`
	const test = `package main

import "testing"

var tool string

func init() { tool = "./test/tools/cosign-sign" }

func TestA(t *testing.T) {}

func TestB(t *testing.T) { _ = tool }
`
	needle := func(s string) bool { return strings.Contains(s, "test/tools/cosign-sign") }
	got := testsReferencing("cmd/orkctl", []srcFile{{Name: "x_test.go", Content: []byte(test)}}, needle)
	if want := []string{"TestA", "TestB"}; !reflect.DeepEqual(keys(got), want) {
		t.Errorf("a test file's init: got %v, want %v", keys(got), want)
	}
	got = testsReferencing("cmd/orkctl", []srcFile{
		{Name: "x_test.go", Content: []byte(strings.Replace(test, `func init() { tool = "./test/tools/cosign-sign" }`, "", 1))},
		{Name: "y.go", Content: []byte(prod)},
	}, needle)
	if len(got) != 0 {
		t.Errorf("a production file's init: got %v, want none", keys(got))
	}
}

// Every init function is a node of its own, named by its file as well as its
// number.
func TestParseGoNamesEachInitByItsFile(t *testing.T) {
	src := "package p\n\nfunc init() {}\n\nfunc init() {}\n"
	var nodes []string
	for _, name := range []string{"a_test.go", "b.go"} {
		pf := parseGo(srcFile{Name: name, Content: []byte(src)})
		for _, d := range pf.Decls {
			if !strings.HasPrefix(d.Key, "init#") {
				t.Fatalf("decl = %+v", d)
			}
			nodes = append(nodes, d.Node)
			if d.InTest != (name == "a_test.go") {
				t.Errorf("%s: InTest = %v", name, d.InTest)
			}
		}
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		if seen[n] {
			t.Errorf("node %q is not unique among %v", n, nodes)
		}
		seen[n] = true
	}
	if len(nodes) != 4 {
		t.Errorf("nodes = %v, want four", nodes)
	}
}

// A func declared without a body (an assembly or linkname stub) has nothing
// to mention but its signature.
func TestParseGoReadsAFuncWithoutABody(t *testing.T) {
	pf := parseGo(srcFile{Name: "stub.go", Content: []byte("package p\n\nfunc stub(x widget) gadget\n\nfunc (w *widget) stub()\n")})
	if !pf.OK || len(pf.Decls) != 2 {
		t.Fatalf("parsed = %+v", pf)
	}
	if d := pf.Decls[0]; d.Node != "stub" || !d.Refs["widget"] || !d.Refs["gadget"] || d.Refs["stub"] {
		t.Errorf("func decl = %+v", d)
	}
	if d := pf.Decls[1]; d.Node != "widget.stub" || d.Recv != "widget" || !d.Refs["widget"] || d.Refs["stub"] {
		t.Errorf("method decl = %+v", d)
	}
}

// A native fuzz target is a test: its seeds run in every unit gate, so the
// evidence selector treats it as one wherever it treats a TestXxx function.
func TestParseGo_AFuzzTargetIsATest(t *testing.T) {
	src := `package p

import "testing"

func FuzzParse(f *testing.F)       {}
func FuzzParse_Round_Trips(f *testing.F) {}
func Fuzz(f *testing.F)            {}
func Fuzzy(f *testing.F)           {}
func FuzzNoParams()                {}
func FuzzWrongType(t *testing.T)   {}
func FuzzTwoParams(f *testing.F, n int) {}
func TestKept(t *testing.T)        {}

type holder struct{}

func (holder) FuzzMethod(f *testing.F) {}
`
	pf := parseGo(srcFile{Name: "x_test.go", Content: []byte(src)})
	if !pf.OK {
		t.Fatal("did not parse")
	}
	got := append([]string(nil), pf.Tests...)
	sort.Strings(got)
	want := []string{"Fuzz", "FuzzParse", "FuzzParse_Round_Trips", "TestKept"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tests = %v, want %v", got, want)
	}
	for _, d := range pf.Decls {
		if d.Test != (d.Key != "method holder.FuzzMethod" && contains(want, d.Name)) {
			t.Errorf("%s: Test = %v", d.Key, d.Test)
		}
	}
	// A file that is not a test file declares none.
	plain := parseGo(srcFile{Name: "x.go", Content: []byte(src)})
	if len(plain.Tests) != 0 {
		t.Errorf("a non-test file declares tests %v", plain.Tests)
	}
}

func TestTestsDeclaredCountsFuzzTargets(t *testing.T) {
	fs := []srcFile{
		{Name: "a_test.go", Content: []byte(fuzzFile)},
		{Name: "b_integration_test.go", Content: []byte("//go:build integration\n\npackage engine\n\nimport \"testing\"\n\nfunc FuzzIntegrationOnly(f *testing.F) {}\n")},
	}
	unit, integ := testsDeclared("internal/engine", fs)
	if !unit["FuzzDecode"] || len(unit) != 1 {
		t.Errorf("unit = %v", unit)
	}
	if !integ["FuzzIntegrationOnly"] || len(integ) != 1 {
		t.Errorf("integration = %v", integ)
	}
}
