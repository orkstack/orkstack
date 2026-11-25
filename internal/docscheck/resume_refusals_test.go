package docscheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestReasons_ResumeSectionListsEveryRefusal: docs/REASONS.md's "Resuming a
// run" has a row for every ResumeRefusalCode the engine and the API declare,
// and no other.
func TestReasons_ResumeSectionListsEveryRefusal(t *testing.T) {
	root := repoRoot(t)
	var want []string
	for _, dir := range []string{"internal/engine", "internal/api"} {
		files, err := filepath.Glob(filepath.Join(root, dir, "resume*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("%s: %d files, %v", dir, len(files), err)
		}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				spec, ok := n.(*ast.ValueSpec)
				if !ok || !isResumeRefusalCode(spec.Type) || len(spec.Values) != 1 {
					return true
				}
				if lit, ok := spec.Values[0].(*ast.BasicLit); ok {
					value, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatal(err)
					}
					want = append(want, value)
				}
				return true
			})
		}
	}
	doc := readFile(t, filepath.Join(root, "docs", "REASONS.md"))
	_, section, ok := strings.Cut(doc, "\n## Resuming a run\n")
	if !ok {
		t.Fatal(`docs/REASONS.md has no "Resuming a run"`)
	}
	section, _, _ = strings.Cut(section, "\n## ")
	got := listedReasons(section)
	slices.Sort(want)
	slices.Sort(got)
	if len(want) < 20 || !slices.Equal(got, want) {
		t.Fatalf("docs/REASONS.md lists %q; the code declares %q", got, want)
	}
}

func isResumeRefusalCode(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name == "ResumeRefusalCode"
	case *ast.SelectorExpr:
		return e.Sel.Name == "ResumeRefusalCode"
	}
	return false
}
