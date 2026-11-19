package docscheck

import (
	"go/ast"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// A test that starts ork, orker or orkctl with the inherited environment
// hands the process under test whatever a developer's shell holds for its own
// deployment: a configuration file, an admin listener, a TLS Temporal.
// testbin.StackEnv drops those and passes what the proof and CI hand a stack
// on purpose; TestDocs_StackSpawnsScrubTheShellsEnv holds every starter to it.

// stackEnvExempt are the test files whose handed program is never a stack
// binary, or (as file:function) the one function of a file whose handed
// environment its callers build through testbin.StackEnv, with the reason.
var stackEnvExempt = map[string]string{
	"internal/engine/runid_process_test.go":                    "re-execs the test binary itself in its helper role",
	"cmd/ork/healthcheck_integration_test.go:runHealthcheck":   "runs --healthcheck in the environ each caller builds with testbin.StackEnv",
	"cmd/orker/healthcheck_integration_test.go:runHealthcheck": "runs --healthcheck in the environ each caller builds with testbin.StackEnv",
}

// exemptFuncs are the functions stackEnvExempt names in rel.
func exemptFuncs(rel string) map[string]bool {
	funcs := map[string]bool{}
	for key := range stackEnvExempt {
		if file, fn, ok := strings.Cut(key, ":"); ok && file == rel {
			funcs[fn] = true
		}
	}
	return funcs
}

// stackBinaries are the programs whose children read the shell's deployment.
var stackBinaries = []string{"./cmd/ork", "./cmd/orker", "./cmd/orkctl"}

// stackRule: a stack process a test starts gets its Env from
// testbin.StackEnv, or from no inherited environment at all.
var stackRule = spawnRule{
	classify:  classifyStackProgram,
	satisfied: scrubsTheShell,
	what: func(kind programKind) string {
		if kind == mayBeOrker {
			return "starts a program it was handed, which may be ork, orker or orkctl,"
		}
		return "starts ork, orker or orkctl"
	},
	missing: " with the shell's environment: build its Env with testbin.StackEnv(os.Environ())",
	noEnv:   " with a command no Env is ever set on, so it inherits the shell's: set Env through testbin.StackEnv",
}

// TestDocs_StackSpawnsScrubTheShellsEnv: in every package whose tests start
// ork, orker or orkctl, each exec.Command of one (or of a program a helper was
// handed) sets Env through testbin.StackEnv or builds it without os.Environ().
func TestDocs_StackSpawnsScrubTheShellsEnv(t *testing.T) {
	root := repoRoot(t)
	starters := stackStartingPackages(t, root)
	found := 0
	var problems []string
	for dir := range starters {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, e.Name())
			rel, _ := filepath.Rel(root, path)
			if _, exempt := stackEnvExempt[filepath.ToSlash(rel)]; exempt {
				continue
			}
			rule := stackRule
			rule.skip = exemptFuncs(filepath.ToSlash(rel))
			n, bad := goSpawns(t, path, rule)
			found += n
			for _, b := range bad {
				problems = append(problems, filepath.ToSlash(rel)+":"+b)
			}
		}
	}
	// The walk must see the starters this repository is known to have (the
	// cmd/orkctl and cmd/ork harnesses among them); fewer means the matcher
	// went blind, not that the rule holds.
	if found < 20 {
		t.Fatalf("found %d places that start ork, orker or orkctl, want at least 20: the matcher no longer sees them", found)
	}
	// And each of these known starters on its own, so half the walk cannot go
	// blind behind the total.
	for _, rel := range []string{
		"cmd/orkctl/integration_test.go", "cmd/orkctl/temporal_tls_integration_test.go",
		"cmd/orkctl/task_run_integration_test.go", "cmd/ork/restart_integration_test.go",
		"cmd/orker/healthcheck_integration_test.go",
	} {
		if n, _ := goSpawns(t, filepath.Join(root, rel), stackRule); n == 0 {
			t.Errorf("%s starts ork, orker or orkctl, but the matcher sees no start there", rel)
		}
	}
	slices.Sort(problems)
	for _, p := range problems {
		t.Error(p)
	}
	for key := range stackEnvExempt {
		rel, fn, _ := strings.Cut(key, ":")
		src, err := os.ReadFile(filepath.Join(root, rel))
		switch {
		case err != nil:
			t.Errorf("stackEnvExempt names %s, which does not exist", rel)
		case fn != "" && !strings.Contains(string(src), "\nfunc "+fn+"("):
			t.Errorf("stackEnvExempt names %s, which %s does not declare", fn, rel)
		}
	}
}

// stackStartingPackages are the directories one of whose tests names a stack
// binary to testbin: only there may a program a helper was handed be one.
func stackStartingPackages(t *testing.T, root string) map[string]bool {
	t.Helper()
	dirs := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && slices.Contains([]string{".git", "node_modules", "proof", ".data", "ui", "site"}, d.Name()) {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src := readFile(t, path)
		for _, b := range stackBinaries {
			if strings.Contains(src, strconv.Quote(b)) {
				dirs[filepath.Dir(path)] = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return dirs
}

// classifyStackProgram: testbin.Path or StaticPath of a stack binary is one,
// of another binary is not, of a variable may be; a name that says ork,
// orker, orkctl or engine is one; a parameter may be; a literal is not.
func classifyStackProgram(e ast.Expr, locals map[string]programKind, params map[string]bool) programKind {
	switch x := e.(type) {
	case *ast.BasicLit:
		return notOrker
	case *ast.CallExpr:
		sel, ok := x.Fun.(*ast.SelectorExpr)
		if !ok || !isIdent(sel.X, "testbin") || (sel.Sel.Name != "Path" && sel.Sel.Name != "StaticPath") || len(x.Args) != 2 {
			return notOrker
		}
		if lit, ok := x.Args[1].(*ast.BasicLit); ok {
			if s, _ := strconv.Unquote(lit.Value); slices.Contains(stackBinaries, s) {
				return isOrker
			}
			return notOrker
		}
		return mayBeOrker
	case *ast.Ident:
		if k, ok := locals[x.Name]; ok {
			return k
		}
		return byStackName(x.Name, params[x.Name])
	case *ast.SelectorExpr:
		return byStackName(x.Sel.Name, false)
	}
	return notOrker
}

func byStackName(name string, param bool) programKind {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "docker"), strings.Contains(lower, "podman"), lower == "cli":
		return notOrker // the container CLI a test drives
	case lower == "ork", strings.Contains(lower, "orkctl"), strings.Contains(lower, "orker"), strings.Contains(lower, "engine"),
		strings.HasPrefix(lower, "orkpath"), strings.HasPrefix(lower, "orkbin"):
		return isOrker
	case param:
		return mayBeOrker
	}
	return notOrker
}

// scrubsTheShell: the Env expression calls testbin.StackEnv, or reads no
// inherited environment (os.Environ) at all. A bare name the walker could not
// trace to its value (a parameter, a field) says nothing either way, and is
// refused: the function that builds it is the place for StackEnv.
func scrubsTheShell(env ast.Expr) bool {
	switch env.(type) {
	case *ast.Ident, *ast.SelectorExpr:
		return false
	}
	stack, environ := false, false
	ast.Inspect(env, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			switch {
			case sel.Sel.Name == "StackEnv" && isIdent(sel.X, "testbin"):
				stack = true
			case sel.Sel.Name == "Environ" && isIdent(sel.X, "os"):
				environ = true
			}
		}
		return true
	})
	return stack || !environ
}

// TestStackEnvCheck_SeesWhatItMustAndPassesWhatItShould holds the rule to
// known snippets, so it cannot pass by going blind: a new starter that execs
// a stack binary with the shell's environment, or with none set, is refused.
func TestStackEnvCheck_SeesWhatItMustAndPassesWhatItShould(t *testing.T) {
	tests := []struct {
		name      string
		src       string
		found     int
		violation bool
	}{
		{"a new starter with the shell's env", `cmd := exec.CommandContext(ctx, testbin.Path(t, "./cmd/ork")); cmd.Env = append(os.Environ(), "ORK_HTTP_ADDR=x")`, 1, true},
		{"a new starter with no env", `cmd := exec.CommandContext(ctx, testbin.Path(t, "./cmd/orkctl"), "version"); _ = cmd.Run()`, 1, true},
		{"run directly", `_ = exec.Command(testbin.Path(t, "./cmd/orker")).Run()`, 1, true},
		{"through StackEnv", `cmd := exec.CommandContext(ctx, testbin.Path(t, "./cmd/ork")); cmd.Env = testbin.OrkerEnv(append(testbin.StackEnv(os.Environ()), "A=1"))`, 1, false},
		{"from scratch", `cmd := exec.Command(testbin.Path(t, "./cmd/orker")); cmd.Env = []string{"HOME=" + home}`, 1, false},
		{"a local holding the engine", `bin := testbin.Path(t, "./cmd/ork"); cmd := exec.Command(bin); cmd.Env = os.Environ()`, 1, true},
		{"a handed program", `cmd := exec.CommandContext(ctx, path); cmd.Env = os.Environ()`, 1, true},
		{"a named engine", `cmd := exec.Command(enginePath); cmd.Env = testbin.StackEnv(os.Environ())`, 1, false},
		{"an env built earlier from the shell", `env := append(os.Environ(), "A=1"); cmd := exec.Command(testbin.Path(t, "./cmd/ork")); cmd.Env = env`, 1, true},
		{"an env built earlier through StackEnv", `env := testbin.StackEnv(os.Environ()); cmd := exec.Command(testbin.Path(t, "./cmd/ork")); cmd.Env = env`, 1, false},
		{"an env it was handed", `cmd := exec.Command(testbin.Path(t, "./cmd/ork")); cmd.Env = home`, 1, true},
		{"docker", `cmd := exec.Command("docker", "ps")`, 0, false},
		{"a handed container CLI", `cmd := exec.CommandContext(ctx, cli, "ps"); _ = cmd.Run()`, 0, false},
		{"another testbin binary", `cmd := exec.Command(testbin.Path(t, "./test/tools/deaf-poller")); cmd.Env = os.Environ()`, 0, false},
		{"a work directory is not a stack binary", `cmd := exec.Command(workDir); cmd.Env = os.Environ()`, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "x_test.go")
			src := "package x\nfunc f(ctx context.Context, t *testing.T, path, home, cli string) {\n" + tt.src + "\n}\n"
			if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
				t.Fatal(err)
			}
			found, bad := goSpawns(t, path, stackRule)
			if found != tt.found || (len(bad) > 0) != tt.violation {
				t.Errorf("found %d, violations %q; want %d, violation %v", found, bad, tt.found, tt.violation)
			}
		})
	}
}
