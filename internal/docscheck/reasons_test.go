package docscheck

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// reasonName is the naming convention a machine-readable reason follows.
var reasonName = regexp.MustCompile(`^(Reason[A-Za-z0-9_]*|[A-Za-z0-9_]*Code|Admission[A-Za-z0-9_]*|MemoMiss[A-Za-z0-9_]*|Event[A-Za-z0-9_]*)$`)

// reasonPackages are the packages whose reason constants reach a client.
var reasonPackages = []string{
	"internal/task", "internal/orker", "internal/engine", "internal/dsl",
	"internal/evidence", "pkg/webhooks",
}

type reasonConst struct {
	Value string
	Name  string
	File  string
}

// TestReasons_EveryReasonIsDocumented: every machine-readable reason a
// client can see appears as a whole word in some docs/*.md. The scan is over
// parsed source, not grep, so a value assembled from constants is skipped
// rather than misread.
func TestReasons_EveryReasonIsDocumented(t *testing.T) {
	root := repoRoot(t)
	var reasons []reasonConst
	fset := token.NewFileSet()
	for _, pkg := range reasonPackages {
		entries, err := os.ReadDir(filepath.Join(root, pkg))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(root, pkg, name)
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				spec, ok := n.(*ast.ValueSpec)
				if !ok {
					return true
				}
				for i, id := range spec.Names {
					if !reasonName.MatchString(id.Name) || i >= len(spec.Values) {
						continue
					}
					lit, ok := spec.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					value, err := strconv.Unquote(lit.Value)
					if err != nil || value == "" || !strings.ContainsAny(value, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
						continue
					}
					reasons = append(reasons, reasonConst{Value: value, Name: id.Name, File: filepath.ToSlash(path)})
				}
				return true
			})
		}
	}
	sort.Slice(reasons, func(i, j int) bool { return reasons[i].Value < reasons[j].Value })

	docs, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	for _, doc := range docs {
		text.WriteString(readFile(t, doc))
		text.WriteString("\n")
	}
	all := text.String()
	var missing []string
	for _, r := range reasons {
		pattern := `(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(r.Value) + `([^A-Za-z0-9_]|$)`
		if !regexp.MustCompile(pattern).MatchString(all) {
			missing = append(missing, fmt.Sprintf("%s (%s, %s)", r.Value, r.Name, r.File))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("%d reason constants appear in no docs/*.md:\n%s", len(missing), strings.Join(missing, "\n"))
	}
	t.Logf("reasons: %d documented constants in %d packages", len(reasons), len(reasonPackages))
}
