package docscheck

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// concurrencyKeyCallers are every place that judges or renders a concurrency
// key, and who made the key it has. A key OrkStack planned may be its own
// (ork:orkflow:…, ork:task:…), which the authored rule refuses: three places
// held one to that rule before each said which it had.
var concurrencyKeyCallers = map[string]string{
	"internal/dsl/concurrency.go:ValidateConcurrency CheckConcurrencyKey":             "KeyAuthored", // a literal key, as written
	"internal/dsl/concurrency_render.go:RenderConcurrencyKey CheckConcurrencyKey":     "KeyAuthored", // what a template rendered to
	"internal/api/concurrency.go:planRunLimit RenderConcurrencyKey":                   "",            // a submit's, of a declared key
	"internal/engine/concurrency_child.go:RenderConcurrencyKeys RenderConcurrencyKey": "",            // a child's, of a declared template
	"internal/engine/concurrency_child.go:RenderConcurrencyKeys CheckConcurrencyKey":  "KeyPlanned",  // a child's key with no template
	"internal/api/concurrency_service.go:concurrencyLimit CheckConcurrencyKey":        "KeyPlanned",  // a key read back
	"internal/api/concurrency_admin.go:ResetConcurrencyLimit CheckConcurrencyKey":     "KeyPlanned",  // a key acted on
}

// concurrencyOwnKeyFiles make or judge OrkStack's own keys; no other file
// spells their prefix.
var concurrencyOwnKeyFiles = []string{"internal/dsl/concurrency.go", "internal/engine/concurrency_acquire.go"}

// TestConcurrencyKeys_EveryCallerSaysWhoMadeTheKey: the places above are all
// there are, each with the maker listed, and no other code spells the prefix of
// OrkStack's own keys, so no second rule can be written beside the one.
func TestConcurrencyKeys_EveryCallerSaysWhoMadeTheKey(t *testing.T) {
	found, repo := map[string]string{}, repoRoot(t)
	for _, dir := range []string{"internal", "pkg", "cmd"} {
		root := filepath.Join(repo, dir)
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			rel, err := filepath.Rel(repo, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			for _, decl := range file.Decls {
				ast.Inspect(decl, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.BasicLit:
						text, err := strconv.Unquote(n.Value)
						if n.Kind == token.STRING && err == nil && strings.HasPrefix(text, "ork:") && !strings.ContainsAny(text, " \n") &&
							(text == "ork:" || strings.HasPrefix(text, "ork:orkflow:") || strings.HasPrefix(text, "ork:task:")) &&
							!slices.Contains(concurrencyOwnKeyFiles, rel) {
							t.Errorf("%s spells %q: OrkStack's own concurrency keys are made and judged by dsl.CheckConcurrencyKey and engine.Default*ConcurrencyKey alone", rel, text)
						}
					case *ast.CallExpr:
						name := ""
						switch fun := n.Fun.(type) {
						case *ast.Ident:
							name = fun.Name
						case *ast.SelectorExpr:
							name = fun.Sel.Name
						}
						if name != "CheckConcurrencyKey" && name != "RenderConcurrencyKey" {
							return true
						}
						in := "(a package variable)"
						if fn, ok := decl.(*ast.FuncDecl); ok {
							in = fn.Name.Name
						}
						maker := ""
						if name == "CheckConcurrencyKey" && len(n.Args) == 2 {
							switch arg := n.Args[1].(type) {
							case *ast.Ident:
								maker = arg.Name
							case *ast.SelectorExpr:
								maker = arg.Sel.Name
							default:
								maker = fmt.Sprintf("%T", arg)
							}
						}
						found[rel+":"+in+" "+name] = maker
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, site := range slices.Sorted(maps.Keys(found)) {
		if want, ok := concurrencyKeyCallers[site]; !ok || want != found[site] {
			t.Errorf("%s says the key is %q; listed %q: list it with who made its key", site, found[site], want)
		}
	}
	for _, site := range slices.Sorted(maps.Keys(concurrencyKeyCallers)) {
		if _, ok := found[site]; !ok {
			t.Errorf("%s is listed and judges no key", site)
		}
	}
}
