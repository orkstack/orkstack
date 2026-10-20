package dsl

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestOrkflowSchema_EveryKeyIsDocumented: every key the generated schema
// accepts appears as a code span or a `key:` line in the document that owns
// it: a `with:` key in its first-party image's README, every other key in
// docs/DSL.md.
func TestOrkflowSchema_EveryKeyIsDocumented(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "schemas", "orkflow.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	dslDoc := readDoc(t, "..", "..", "docs", "DSL.md")
	missing := map[string][]string{}
	defs, _ := schema["definitions"].(map[string]any)
	for name, def := range defs {
		if strings.HasPrefix(name, "orkflow.with.") {
			image := strings.TrimPrefix(name, "orkflow.with.")
			docPath := filepath.Join("..", "..", "images", "activity-"+image, "README.md")
			doc := readDoc(t, docPath)
			for _, key := range propertyNames(def) {
				if !documented(doc, key) {
					missing[docPath] = append(missing[docPath], key)
				}
			}
			continue
		}
		for _, key := range allPropertyNames(def) {
			if !documented(dslDoc, key) {
				missing["docs/DSL.md"] = append(missing["docs/DSL.md"], key)
			}
		}
	}
	for _, key := range propertyNames(schema) {
		if !documented(dslDoc, key) {
			missing["docs/DSL.md"] = append(missing["docs/DSL.md"], key)
		}
	}
	if len(missing) > 0 {
		var lines []string
		for doc, keys := range missing {
			sort.Strings(keys)
			lines = append(lines, fmt.Sprintf("%s: %v", doc, keys))
		}
		sort.Strings(lines)
		t.Fatalf("schema keys documented nowhere:\n%s", strings.Join(lines, "\n"))
	}
}

func readDoc(t *testing.T, parts ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func documented(doc, key string) bool {
	if strings.Contains(doc, "`"+key+"`") {
		return true
	}
	return regexp.MustCompile(`(^|\W)` + regexp.QuoteMeta(key) + `:`).MatchString(doc)
}

func propertyNames(node any) []string {
	m, ok := node.(map[string]any)
	if !ok {
		return nil
	}
	props, _ := m["properties"].(map[string]any)
	var out []string
	for key := range props {
		if key != "op" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// allPropertyNames walks every nested schema object for properties, so a
// key inside a definition (an input, a pin, a retry) is checked too.
func allPropertyNames(node any) []string {
	seen := map[string]bool{}
	var walk func(any)
	walk = func(n any) {
		switch v := n.(type) {
		case map[string]any:
			if props, ok := v["properties"].(map[string]any); ok {
				for key := range props {
					seen[key] = true
				}
			}
			for key, child := range v {
				if key == "properties" {
					for _, c := range child.(map[string]any) {
						walk(c)
					}
					continue
				}
				walk(child)
			}
		case []any:
			for _, child := range v {
				walk(child)
			}
		}
	}
	walk(node)
	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// TestDSLDoc_RulesAreNumberedAndCited: DSL.md's validation rules are
// contiguous, every rule has an enforcement comment, and every citation
// names a rule that exists.
func TestDSLDoc_RulesAreNumberedAndCited(t *testing.T) {
	doc := readDoc(t, "..", "..", "docs", "DSL.md")
	start := strings.Index(doc, "## Validation rules")
	if start < 0 {
		t.Fatal("docs/DSL.md has no ## Validation rules section")
	}
	section := doc[start:]
	if end := strings.Index(section[1:], "\n## "); end >= 0 {
		section = section[:end+1]
	}
	var nums []int
	for _, m := range regexp.MustCompile(`(?m)^(\d+)\.\s`).FindAllStringSubmatch(section, -1) {
		n, _ := strconv.Atoi(m[1])
		nums = append(nums, n)
	}
	if len(nums) == 0 {
		t.Fatal("the rule list is empty")
	}
	for i, n := range nums {
		if n != i+1 {
			t.Fatalf("rule numbers are not contiguous from 1: %v", nums)
		}
	}
	enforced := map[int]bool{}
	cited := map[int][]string{}
	scan := func(dir string, docs bool) {
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			name := d.Name()
			if !docs && (!strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go")) {
				return nil
			}
			if docs && !strings.HasSuffix(name, ".md") {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, m := range regexp.MustCompile(`(?i)\brule (\d+)\b`).FindAllStringSubmatch(string(body), -1) {
				n, _ := strconv.Atoi(m[1])
				cited[n] = append(cited[n], path)
				if strings.Contains(m[0], "Rule") {
					enforced[n] = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	scan("..", false)
	scan("../../docs", true)
	var missing []int
	for _, n := range nums {
		if !enforced[n] {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("rules with no `// Rule N` enforcement comment: %v", missing)
	}
	var over []string
	for n, paths := range cited {
		if n > len(nums) {
			over = append(over, fmt.Sprintf("rule %d in %s", n, strings.Join(paths, ", ")))
		}
	}
	if len(over) > 0 {
		sort.Strings(over)
		t.Fatalf("citations of rules that do not exist:\n%s", strings.Join(over, "\n"))
	}
	t.Logf("DSL rules: %d numbered and enforced", len(nums))
}
