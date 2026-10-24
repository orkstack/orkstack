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

// scanReasons returns the reason constants of reasonPackages, sorted by value.
// The scan is over parsed source, not grep, so a value assembled from
// constants is skipped rather than misread.
func scanReasons(t *testing.T) []reasonConst {
	t.Helper()
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
	return reasons
}

// TestReasons_EveryReasonIsDocumented: every machine-readable reason a
// client can see appears as a whole word in some docs/*.md.
func TestReasons_EveryReasonIsDocumented(t *testing.T) {
	root := repoRoot(t)
	reasons := scanReasons(t)

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

// listedReasons returns the code span in the first cell of every table row of
// a document, in document order. A header or separator row has none.
func listedReasons(doc string) []string {
	var out []string
	for _, line := range strings.Split(doc, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			continue
		}
		if cells := strings.Split(line, "|"); len(cells) > 1 {
			out = append(out, codeSpans(cells[1])...)
		}
	}
	return out
}

// literalPackages are the directories whose string literals may spell a reason
// that no constant declares: the reason packages, plus the API that sets an
// Error.code and the CLI that prints one.
var literalPackages = append(append([]string{}, reasonPackages...), "internal/api", "cmd/orkctl")

// eventPackage is where the run-event types live. Its Event* constants are
// event types, not failures; an Event* name anywhere else is a reason like
// any other, so the exemption is by place, not by name alone.
const eventPackage = "pkg/webhooks"

// stringLiterals returns the value of every string literal in the non-test Go
// files directly in dirs. An Event* declaration of eventPackage is left out, as
// the reason scan leaves it out: its value is an event type, not a failure.
func stringLiterals(t *testing.T, dirs []string) map[string]bool {
	t.Helper()
	root := repoRoot(t)
	literals := map[string]bool{}
	fset := token.NewFileSet()
	for _, dir := range dirs {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(root, dir, name)
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				if spec, ok := n.(*ast.ValueSpec); ok && dir == eventPackage && isEventSpec(spec) {
					return false
				}
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				if value, err := strconv.Unquote(lit.Value); err == nil {
					literals[value] = true
				}
				return true
			})
		}
	}
	return literals
}

// isEventSpec reports whether a declaration names an Event constant.
func isEventSpec(spec *ast.ValueSpec) bool {
	for _, id := range spec.Names {
		if strings.HasPrefix(id.Name, "Event") {
			return true
		}
	}
	return false
}

// TestReasons_ReferenceListsEveryReason: docs/REASONS.md has exactly one table
// row for each reason the scan finds. A row for a value that no reason constant
// declares is accepted only when the value is spelled as a string literal in
// literalPackages, because some reasons are written inline (RUN_FAILED) or as
// lowercase tokens (object_missing) that the constant scan cannot see. The
// Event constants are event types, not failures, and are left out of both
// sides.
func TestReasons_ReferenceListsEveryReason(t *testing.T) {
	root := repoRoot(t)
	want := map[string]string{}
	for _, r := range scanReasons(t) {
		if strings.HasPrefix(r.Name, "Event") && strings.Contains(filepath.ToSlash(r.File), eventPackage+"/") {
			continue
		}
		if _, ok := want[r.Value]; !ok {
			want[r.Value] = fmt.Sprintf("%s, %s", r.Name, r.File)
		}
	}
	literals := stringLiterals(t, literalPackages)

	listed := map[string]int{}
	for _, code := range listedReasons(readFile(t, filepath.Join(root, "docs", "REASONS.md"))) {
		listed[code]++
	}
	var missing, stale, repeated []string
	for value, where := range want {
		if listed[value] == 0 {
			missing = append(missing, fmt.Sprintf("%s (%s)", value, where))
		}
	}
	for code, n := range listed {
		if _, ok := want[code]; !ok && !literals[code] {
			stale = append(stale, code)
		}
		if n > 1 {
			repeated = append(repeated, fmt.Sprintf("%s (%d rows)", code, n))
		}
	}
	if len(missing)+len(stale)+len(repeated) > 0 {
		sort.Strings(missing)
		sort.Strings(stale)
		sort.Strings(repeated)
		t.Fatalf("docs/REASONS.md is out of step with the reason constants:\n"+
			"missing, no table row (%d):\n%s\n"+
			"stale, no reason constant or string literal has this value (%d):\n%s\n"+
			"repeated (%d):\n%s",
			len(missing), strings.Join(missing, "\n"),
			len(stale), strings.Join(stale, "\n"),
			len(repeated), strings.Join(repeated, "\n"))
	}
	t.Logf("docs/REASONS.md lists %d reasons", len(listed))
}
