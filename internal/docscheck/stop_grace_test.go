package docscheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// An Orker defaults to a day's stop grace (orker.stop.grace), so a test or a
// harness that stops one while it runs a task would wait it out. A proof
// gives every Orker a zero grace (test/tools/proofrun); everything that starts
// one itself must do the same or name its own, so a by-hand run behaves like
// a proof (elastic-orkers design R10-H3).

// stopGraceExempt are the files that start an Orker on purpose with the
// shipped default, or only ever under the proof runner, which exports the
// zero grace for the whole proof.
var stopGraceExempt = map[string]string{
	"scripts/proof-observability.sh": "runs only as a proof gate, under the proof's exported grace",
	"scripts/proof.sh":               "exports ORK_ORKER_STOP_GRACE for the whole proof",
}

// TestDocs_OrkerSpawnsSetAStopGrace: every Go test that starts ./cmd/orker
// passes its environment through testbin.OrkerEnv or names the grace; every
// Playwright spawn of the Orker binary carries orkerEnv or the grace; and
// every script that runs bin/orker names a grace. The shipped artifacts
// (Dockerfile, deploy bases, compose.yaml) keep the default on purpose and
// are held to their stop timeouts elsewhere (internal/dist).
func TestDocs_OrkerSpawnsSetAStopGrace(t *testing.T) {
	if testbin.StopGraceVar != orker.StopGraceVar {
		t.Fatalf("testbin.StopGraceVar = %q, want the Orker's %q", testbin.StopGraceVar, orker.StopGraceVar)
	}
	root := repoRoot(t)
	starters := orkerStartingPackages(t, root)
	var problems []string
	goSites, tsSites, shSites := 0, 0, 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "proof", ".data", "gen", "testdata", "build", ".svelte-kit":
				return filepath.SkipDir
			}
			return nil
		}
		if _, exempt := stopGraceExempt[filepath.ToSlash(rel)]; exempt {
			return nil
		}
		switch {
		case strings.HasSuffix(path, "_test.go") && starters[filepath.Dir(path)]:
			found, bad := goOrkerSpawns(t, path)
			goSites += found
			for _, b := range bad {
				problems = append(problems, rel+":"+b)
			}
		case strings.HasPrefix(filepath.ToSlash(rel), "ui/e2e/") && strings.HasSuffix(path, ".ts"):
			found, bad := tsOrkerSpawns(readFile(t, path))
			tsSites += found
			for _, b := range bad {
				problems = append(problems, rel+":"+b)
			}
		case strings.HasPrefix(filepath.ToSlash(rel), "scripts/") && strings.HasSuffix(path, ".sh"):
			text := readFile(t, path)
			if runsOrker(text) {
				shSites++
				if !strings.Contains(text, orker.StopGraceVar) && !strings.Contains(text, "grace:") {
					problems = append(problems, rel+": runs bin/orker without naming a stop grace (ORK_ORKER_STOP_GRACE or orker.stop.grace)")
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The walk must find what this repository is known to have: the Go
	// harnesses of cmd/orkctl and cmd/orker, the Playwright specs and the dev
	// stack. Fewer means the matchers went blind, not that the rule holds.
	if goSites < 8 || tsSites < 40 || shSites < 1 {
		t.Fatalf("found %d Go, %d Playwright and %d script places that start an Orker, want at least 8, 40 and 1: "+
			"a matcher no longer sees them", goSites, tsSites, shSites)
	}
	for _, p := range problems {
		t.Error(p)
	}
	for rel := range stopGraceExempt {
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			t.Errorf("stopGraceExempt names %s, which does not exist", rel)
		}
	}
	// A build-only file must exist and start nothing that may be the Orker.
	for rel := range orkerBuildOnly {
		path := filepath.Join(root, rel)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("orkerBuildOnly names %s, which does not exist", rel)
			continue
		}
		if found, _ := goOrkerSpawns(t, path); found != 0 {
			t.Errorf("orkerBuildOnly names %s, which starts %d program(s) that may be the Orker: remove it from the list", rel, found)
		}
	}
}

// shRunsOrker matches a script line that names this tree's bin/orker.
var shRunsOrker = regexp.MustCompile(`(?m)^[^#\n]*(?:"\$REPO/bin/orker"|\./bin/orker|\$\{?REPO\}?/bin/orker)\b`)

// runsOrker: a line of the script names bin/orker as something it runs, not
// as a case pattern (followed by ")") or a word in a comment.
func runsOrker(text string) bool {
	for _, m := range shRunsOrker.FindAllStringIndex(text, -1) {
		if rest := strings.TrimLeft(text[m[1]:], "\" \t"); !strings.HasPrefix(rest, ")") && !strings.HasPrefix(rest, "|") {
			return true
		}
	}
	return false
}

// orkerBuildOnly are test files that build ./cmd/orker only to copy the
// binary into an image and never start it on the host, so they do not make
// their package one whose handed programs may be the Orker.
var orkerBuildOnly = map[string]string{
	"internal/orker/gpuhost_fixture_gpuhost_test.go": "copies the Orker into the GPU probe image it builds; never runs it",
}

// orkerStartingPackages are the directories one of whose tests starts
// ./cmd/orker: only there may a program a helper was handed be the Orker.
func orkerStartingPackages(t *testing.T, root string) map[string]bool {
	t.Helper()
	dirs := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && slices.Contains([]string{".git", "node_modules", "proof", ".data", "ui", "site"}, d.Name()) {
			return filepath.SkipDir
		}
		rel, _ := filepath.Rel(root, path)
		if _, buildOnly := orkerBuildOnly[filepath.ToSlash(rel)]; buildOnly {
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(path, "_test.go") && strings.Contains(readFile(t, path), `"./cmd/orker"`) {
			dirs[filepath.Dir(path)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return dirs
}

// tsOrkerSpawns finds every spawn of the Orker binary in a Playwright file
// (bin('orker') inside a call) and reports each call that carries neither
// orkerEnv(...) nor the grace.
func tsOrkerSpawns(src string) (found int, bad []string) {
	for _, m := range regexp.MustCompile(`bin\(['"]orker['"]\)`).FindAllStringIndex(src, -1) {
		found++
		call := src[m[0]:closingParen(src, enclosingParen(src, m[0]))]
		if !strings.Contains(call, "orkerEnv(") && !strings.Contains(call, orker.StopGraceVar) {
			line := strings.Count(src[:m[0]], "\n") + 1
			bad = append(bad, strconv.Itoa(line)+": starts the Orker without orkerEnv(...) or ORK_ORKER_STOP_GRACE")
		}
	}
	return found, bad
}

// enclosingParen is the index of the open parenthesis of the call at enclosing
// position at, or 0 when there is none.
func enclosingParen(src string, at int) int {
	depth := 0
	for i := at - 1; i >= 0; i-- {
		switch src[i] {
		case ')':
			depth++
		case '(':
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return 0
}

// closingParen is the index just past the parenthesis that closes the one
// at open, or the end of src.
func closingParen(src string, open int) int {
	depth := 0
	for i := open; i < len(src); i++ {
		switch src[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return len(src)
}

// orkerProgram says whether an exec.Command's program expression starts the
// Orker: yes for testbin.Path(_, "./cmd/orker") and for a name that says
// orker, maybe for a program the function was handed (a parameter, or
// testbin.Path of a variable), no for a literal, any other call or a name
// that says another binary.
type programKind int

const (
	notOrker programKind = iota
	isOrker
	mayBeOrker
)

func classifyProgram(e ast.Expr, locals map[string]programKind, params map[string]bool) programKind {
	switch x := e.(type) {
	case *ast.BasicLit:
		return notOrker
	case *ast.CallExpr:
		if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Path" && isIdent(sel.X, "testbin") && len(x.Args) == 2 {
			if lit, ok := x.Args[1].(*ast.BasicLit); ok {
				if s, _ := strconv.Unquote(lit.Value); s == "./cmd/orker" {
					return isOrker
				}
				return notOrker
			}
			return mayBeOrker
		}
		return notOrker
	case *ast.Ident:
		if k, ok := locals[x.Name]; ok {
			return k
		}
		return byName(x.Name, params[x.Name])
	case *ast.SelectorExpr:
		return byName(x.Sel.Name, false)
	}
	return notOrker
}

func byName(name string, param bool) programKind {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "orkctl"), strings.Contains(lower, "engine"):
		return notOrker
	case strings.Contains(lower, "orker"):
		return isOrker
	case param:
		return mayBeOrker
	}
	return notOrker
}

func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}

// goOrkerSpawns parses a test file and, in every top-level function (its
// closures included, which share its names), pairs each exec.Command(Context)
// with the Env assignments to the same command. A command that starts (or may
// start) the Orker must have one that calls testbin.OrkerEnv or names
// ORK_ORKER_STOP_GRACE; one whose result is used directly (.Run(), .Start(),
// returned, a struct field) can have none and is refused.
func goOrkerSpawns(t *testing.T, path string) (found int, bad []string) {
	t.Helper()
	return goSpawns(t, path, orkerRule)
}

// spawnRule is what goSpawns holds an exec.Command to: which programs it is
// about, what an Env assignment must do, and how a refusal reads.
type spawnRule struct {
	classify  func(e ast.Expr, locals map[string]programKind, params map[string]bool) programKind
	satisfied func(env ast.Expr) bool
	what      func(kind programKind) string
	missing   string          // after what(kind), for a command whose Env does not satisfy
	noEnv     string          // after what(kind), for a command no Env is ever set on
	skip      map[string]bool // top-level functions not to check, by name
}

// orkerRule: an Orker started by a test has a stop grace (testbin.OrkerEnv).
var orkerRule = spawnRule{
	classify:  classifyProgram,
	satisfied: namesGrace,
	what:      spawnWhat,
	missing:   " without testbin.OrkerEnv or ORK_ORKER_STOP_GRACE in its Env",
	noEnv:     " with a command no Env is ever set on; assign it and set Env through testbin.OrkerEnv",
}

// goSpawns parses a test file and, in every top-level function (its closures
// included, which share its names), pairs each exec.Command(Context) with the
// Env assignments to the same command, and reports each command rule is about
// whose Env does not satisfy it, or that has none.
func goSpawns(t *testing.T, path string, rule spawnRule) (found int, bad []string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if !strings.Contains(readFile(t, path), "exec.Command") {
		return 0, nil
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil && !rule.skip[fn.Name.Name] {
			f, b := spawnsIn(fset, fn, rule)
			found += f
			bad = append(bad, b...)
		}
	}
	return found, bad
}

func spawnsIn(fset *token.FileSet, fn *ast.FuncDecl, rule spawnRule) (found int, bad []string) {
	params := map[string]bool{}
	addParams := func(ft *ast.FuncType) {
		for _, field := range ft.Params.List {
			for _, name := range field.Names {
				params[name.Name] = true
			}
		}
	}
	addParams(fn.Type)
	locals := map[string]programKind{}
	type command struct {
		pos  token.Pos
		kind programKind
		ok   bool
	}
	commands := map[string]*command{}
	var order []string
	assigned := map[*ast.CallExpr]bool{}
	var direct []*ast.CallExpr
	// values are what a plain name was last assigned, so an Env taken from a
	// variable is judged by what built the variable.
	values := map[string]ast.Expr{}
	bind := func(lhs ast.Expr, rhs ast.Expr) {
		name := exprString(lhs)
		if call, ok := rhs.(*ast.CallExpr); ok && isExecCommand(call) {
			assigned[call] = true
			kind := rule.classify(programOf(call), locals, params)
			c, seen := commands[name]
			if !seen {
				order = append(order, name)
				c = &command{pos: call.Pos()}
				commands[name] = c
			}
			// A name reused for another command keeps the Orker's claim.
			if kind > c.kind || c.kind == notOrker {
				c.kind, c.pos = kind, call.Pos()
			}
			return
		}
		if strings.HasSuffix(name, ".Env") {
			env := rhs
			if id, ok := rhs.(*ast.Ident); ok && values[id.Name] != nil {
				env = values[id.Name]
			}
			if c, ok := commands[strings.TrimSuffix(name, ".Env")]; ok && rule.satisfied(env) {
				c.ok = true
			}
			return
		}
		if id, ok := lhs.(*ast.Ident); ok {
			values[id.Name] = rhs
			if k := rule.classify(rhs, locals, params); k != notOrker {
				locals[id.Name] = k
			}
		}
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			addParams(x.Type)
		case *ast.AssignStmt:
			if len(x.Lhs) == len(x.Rhs) {
				for i := range x.Lhs {
					bind(x.Lhs[i], x.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			if len(x.Names) == len(x.Values) {
				for i := range x.Names {
					bind(x.Names[i], x.Values[i])
				}
			}
		case *ast.CallExpr:
			if isExecCommand(x) {
				direct = append(direct, x)
			}
		}
		return true
	})
	for _, name := range order {
		c := commands[name]
		if c.kind == notOrker {
			continue
		}
		found++
		if !c.ok {
			bad = append(bad, strconv.Itoa(fset.Position(c.pos).Line)+": "+rule.what(c.kind)+rule.missing)
		}
	}
	for _, call := range direct {
		if assigned[call] {
			continue
		}
		if kind := rule.classify(programOf(call), locals, params); kind != notOrker {
			found++
			bad = append(bad, strconv.Itoa(fset.Position(call.Pos()).Line)+": "+rule.what(kind)+rule.noEnv)
		}
	}
	return found, bad
}

func spawnWhat(kind programKind) string {
	if kind == mayBeOrker {
		return "starts a program it was handed, which may be the Orker,"
	}
	return "starts the Orker"
}

// programOf is an exec.Command's program argument (after the context for
// CommandContext).
func programOf(call *ast.CallExpr) ast.Expr {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "CommandContext" && len(call.Args) > 1 {
		return call.Args[1]
	}
	return call.Args[0]
}

func isExecCommand(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && isIdent(sel.X, "exec") && slices.Contains([]string{"Command", "CommandContext"}, sel.Sel.Name) && len(call.Args) > 0
}

// namesGrace: the expression calls testbin.OrkerEnv or names the setting.
func namesGrace(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			if x.Sel.Name == "OrkerEnv" || x.Sel.Name == "StopGraceVar" {
				found = true
			}
		case *ast.BasicLit:
			if strings.Contains(x.Value, orker.StopGraceVar) {
				found = true
			}
		}
		return !found
	})
	return found
}

func exprString(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return exprString(x.X) + "." + x.Sel.Name
	}
	return ""
}

// TestStopGraceCheck_SeesWhatItMustAndPassesWhatItShould holds the matchers
// themselves to known snippets, so the rule above cannot pass by going blind.
func TestStopGraceCheck_SeesWhatItMustAndPassesWhatItShould(t *testing.T) {
	for _, tc := range []struct {
		name      string
		src       string
		found     int
		violation bool
	}{
		{"an orker without a grace", `cmd := exec.CommandContext(ctx, testbin.Path(t, "./cmd/orker")); cmd.Env = os.Environ()`, 1, true},
		{"an orker through OrkerEnv", `cmd := exec.CommandContext(ctx, testbin.Path(t, "./cmd/orker")); cmd.Env = testbin.OrkerEnv(os.Environ())`, 1, false},
		{"an orker naming its grace", `cmd := exec.Command(orkerPath); cmd.Env = append(os.Environ(), "ORK_ORKER_STOP_GRACE=10m")`, 1, false},
		{"a local holding the orker", `bin := testbin.Path(t, "./cmd/orker"); cmd := exec.CommandContext(ctx, bin)`, 1, true},
		{"a handed program", `cmd := exec.CommandContext(ctx, path); cmd.Env = os.Environ()`, 1, true},
		{"orkctl", `cmd := exec.CommandContext(ctx, orkctl); cmd.Env = os.Environ()`, 0, false},
		{"docker", `cmd := exec.Command("docker", "ps")`, 0, false},
		{"another testbin binary", `cmd := exec.Command(testbin.Path(t, "./cmd/ork"))`, 0, false},
		{"a field", `p.cmd = exec.CommandContext(ctx, path); p.cmd.Env = testbin.OrkerEnv(os.Environ())`, 1, false},
		{"a var declaration", `var cmd = exec.Command(testbin.Path(t, "./cmd/orker")); cmd.Env = os.Environ()`, 1, true},
		{"run directly", `_ = exec.Command(testbin.Path(t, "./cmd/orker")).Run()`, 1, true},
		{"a closure over the program", `bin := testbin.Path(t, "./cmd/orker"); t.Run("x", func(t *testing.T) { cmd := exec.Command(bin); cmd.Env = os.Environ() })`, 1, true},
		{"a name reused for docker", `cmd := exec.Command(orkerPath); cmd = exec.Command("docker", "ps")`, 1, true},
	} {
		dir := t.TempDir()
		path := filepath.Join(dir, "x_test.go")
		src := "package x\nfunc f(ctx context.Context, t *testing.T, path, orkctl string) {\n" + tc.src + "\n}\n"
		if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
		found, bad := goOrkerSpawns(t, path)
		if found != tc.found || (len(bad) > 0) != tc.violation {
			t.Errorf("%s: found %d, violations %q; want %d, violation %v", tc.name, found, bad, tc.found, tc.violation)
		}
	}
	ts := "spawnLogged(out(`${id}.log`), bin('orker'), [], {\n\t...cluster,\n\t...orkerEnv('a')\n});\n" +
		"spawnLogged(out('b.log'), bin('orker'), [], {\n\t...cluster\n});\n" +
		"spawnLogged(out('c.log'), bin('ork'), [], {});\n"
	if found, bad := tsOrkerSpawns(ts); found != 2 || len(bad) != 1 || !strings.HasPrefix(bad[0], "5:") {
		t.Errorf("tsOrkerSpawns = %d, %q; want 2 found, one violation at line 5", found, bad)
	}
	if !runsOrker(`		start orker "$REPO/bin/orker" --config x`) || runsOrker(`# "$REPO/bin/orker"`) ||
		runsOrker(`	"$REPO/bin/ork" | "$REPO/bin/orkerx")`) || runsOrker(`	"$REPO/bin/ork" | "$REPO/bin/orker") stray="$p" ;;`) {
		t.Error("runsOrker does not tell a run of bin/orker from a comment, a case pattern or another name")
	}
}
