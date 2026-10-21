package automation

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestAutomationSchema_EveryKeyIsInAutomationDoc: every key an automation
// document accepts — at the top level, under settings, and in a schedule,
// trigger or webhook — appears in docs/AUTOMATION.md as a code span or a
// `key:` line (plan §8.3). The schema is generated from the parser's key
// tables, so a key added there and not documented fails here.
func TestAutomationSchema_EveryKeyIsInAutomationDoc(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "AUTOMATION.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	keys := automationPropertyNames(automationSchemaTree(t))
	if len(keys) < 20 {
		t.Fatalf("the automation schema declares %d keys; the walk is missing them", len(keys))
	}
	var missing []string
	for _, key := range keys {
		if !keyIsDocumented(doc, key) {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("docs/AUTOMATION.md does not mention %d automation key(s): %v", len(missing), missing)
	}
	t.Logf("%d automation keys, every one in docs/AUTOMATION.md", len(keys))

	// The check bites: a key the document never names is reported.
	if keyIsDocumented(doc, "definitely_not_an_automation_key") {
		t.Error("keyIsDocumented accepts a key the document never names")
	}
}

func keyIsDocumented(doc, key string) bool {
	if strings.Contains(doc, "`"+key+"`") {
		return true
	}
	return regexp.MustCompile(`(^|\W)` + regexp.QuoteMeta(key) + `:`).MatchString(doc)
}

// automationPropertyNames walks every schema object for its properties, so a
// key nested in a definition (a budget's fields, a webhook's) is checked too.
func automationPropertyNames(node any) []string {
	seen := map[string]bool{}
	var walk func(any)
	walk = func(n any) {
		switch v := n.(type) {
		case map[string]any:
			for key, child := range v {
				if key == "properties" {
					if props, ok := child.(map[string]any); ok {
						for name, sub := range props {
							seen[name] = true
							walk(sub)
						}
						continue
					}
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
