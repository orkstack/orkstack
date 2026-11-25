package dist_test

import (
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

var (
	smokeExampleCase = regexp.MustCompile(`(?m)^\t\t([a-z][a-z0-9-]*)\)`)
	smokeExampleTask = regexp.MustCompile(`([a-z][a-z0-9-]*)=(?:\$S|[A-Z]+)`)
	exampleTaskID    = regexp.MustCompile(`(?m)^  - id: ([a-z][a-z0-9-]*)\s*$`)
)

// TestQuickstartSmoke_StatesEveryExamplesResult: the example run of
// scripts/quickstart-smoke.sh states a result for every shipped example, for
// no other, and names only tasks that example has. No proof runs that part of
// the script (it reaches the network), so without this a new example is found
// missing only by the nightly.
func TestQuickstartSmoke_StatesEveryExamplesResult(t *testing.T) {
	smoke := string(readRepoFile(t, "scripts/quickstart-smoke.sh"))
	_, rest, found := strings.Cut(smoke, "\t\tcase \"$name\" in\n")
	body, _, closed := strings.Cut(rest, "\n\t\tesac\n")
	if !found || !closed {
		t.Fatal("scripts/quickstart-smoke.sh: no example case block")
	}
	stated := map[string]string{} // example -> the text of its case
	starts := smokeExampleCase.FindAllStringSubmatchIndex(body, -1)
	for i, m := range starts {
		end := len(body)
		if i+1 < len(starts) {
			end = starts[i+1][0]
		}
		// The last case's text runs on into the refusal of an unknown name.
		text, _, _ := strings.Cut(body[m[1]:end], "\n\t\t*)")
		stated[body[m[2]:m[3]]] = text
	}

	files, err := filepath.Glob(filepath.Join(repoRoot(t), "examples", "*.ork.yaml"))
	must.OK(t, err)
	if len(files) == 0 {
		t.Fatal("no examples found")
	}
	var shipped []string
	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".ork.yaml")
		shipped = append(shipped, name)
		text, ok := stated[name]
		if !ok {
			t.Errorf("scripts/quickstart-smoke.sh states no result for examples/%s.ork.yaml", name)
			continue
		}
		var ids []string
		for _, m := range exampleTaskID.FindAllStringSubmatch(string(readRepoFile(t, "examples/"+name+".ork.yaml")), -1) {
			ids = append(ids, m[1])
		}
		tasks := smokeExampleTask.FindAllStringSubmatch(text, -1)
		if len(tasks) == 0 {
			t.Errorf("scripts/quickstart-smoke.sh states no task of %s", name)
		}
		for _, m := range tasks {
			if !slices.Contains(ids, m[1]) {
				t.Errorf("scripts/quickstart-smoke.sh states a result for task %q, which examples/%s.ork.yaml does not have (%v)", m[1], name, ids)
			}
		}
	}
	for name := range stated {
		if !slices.Contains(shipped, name) {
			t.Errorf("scripts/quickstart-smoke.sh states a result for %q, which is no example", name)
		}
	}
}
