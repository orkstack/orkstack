package dsl

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// t16Witnesses returns the function each action of the activity contract
// fixture's T16 template calls first: the template that section renders in
// two containers with identical inputs and shows to differ on every line
// (test/fixtures/first_party_contract.sh, "T16 the caching inventory's
// witness"). The first name of an action is the ambient state the line
// witnesses; what follows it (time.Parse, base64.Encode, the arguments) is
// scaffolding. A fixture this cannot read is a failure, never a skip.
func t16Witnesses(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "test", "fixtures", "first_party_contract.sh"))
	if err != nil {
		t.Fatalf("read the activity contract fixture: %v", err)
	}
	_, after, ok := strings.Cut(string(raw), `section "T16 `)
	if !ok {
		t.Fatal("the fixture has no `section \"T16 ` header: the witness moved, and this test must follow it")
	}
	_, after, ok = strings.Cut(after, "local tmpl=$'")
	if !ok {
		t.Fatal("the fixture's T16 section declares no `local tmpl=$'...'` template")
	}
	template, _, ok := strings.Cut(after, "'\n")
	if !ok {
		t.Fatal("the fixture's T16 template is not closed")
	}
	var names []string
	action := regexp.MustCompile(`^\{\{\s*([A-Za-z_][A-Za-z0-9_.]*)`)
	// The template is a bash $'...' string: its line breaks are written \n.
	for line := range strings.SplitSeq(template, `\n`) {
		if m := action.FindStringSubmatch(line); m != nil {
			names = append(names, m[1])
		}
	}
	if len(names) < 5 {
		t.Fatalf("read only %d witnessed functions from the T16 template %q: the fixture changed shape", len(names), template)
	}
	return names
}

// The activity-template README's caching inventory is the author's guide to
// which functions make a `cache:` assertion false, and the contract fixture's
// T16 section is its proof: every function it renders twice and shows to
// differ is a function the README must list as unsafe to cache. A function
// proved ambient in the fixture but missing from the table, or listed only
// among those covered by the memo key, is a README that tells an author a
// non-repeatable template is safe. (The table may list more than the fixture
// proves; the fixture is a witness, not the inventory.)
func TestActivityTemplate_CachingInventoryListsEveryT16Witness(t *testing.T) {
	caching := markdownSection(t, readImageFile(t, "activity-template", "README.md"), "Caching")
	unsafe, _, ok := strings.Cut(caching, "**Covered by the memo key")
	if !ok {
		t.Fatal("the Caching section has no `**Covered by the memo key` list after the unsafe table: it moved, and this test must follow it")
	}
	for _, name := range t16Witnesses(t) {
		if !regexp.MustCompile("`" + regexp.QuoteMeta(name) + "`").MatchString(unsafe) {
			t.Errorf("the Caching section's unsafe-to-cache table does not list `%s`, which the T16 witness proves differs between two containers with identical inputs", name)
		}
	}
}
