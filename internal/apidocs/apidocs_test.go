package apidocs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/apidocs"
)

const root = "../.."

func render(t *testing.T) string {
	t.Helper()
	doc, err := apidocs.Render(root)
	if err != nil {
		t.Fatal(err)
	}
	return string(doc)
}

// TestAPIDocs_AreGenerated: the committed reference is byte for byte what
// the generator writes from the proto sources.
func TestAPIDocs_AreGenerated(t *testing.T) {
	got, err := os.ReadFile(filepath.Join(root, apidocs.Path))
	if err != nil {
		t.Fatal(err)
	}
	if want := render(t) + "\n"; string(got) != want {
		t.Fatalf("%s is stale: run go run ./internal/apidocs/cmd/genapidocs", apidocs.Path)
	}
}

// TestAPIDocs_NameNoClusterMechanism: the reference renders client-facing
// proto comments, a user-facing surface, so it names none of the cluster's
// mechanisms AGENTS.md §7 keeps internal. "Signal" and "Timer" are matched
// capitalised only: a telemetry signal is OrkStack's own word.
func TestAPIDocs_NameNoClusterMechanism(t *testing.T) {
	forbidden := []*regexp.Regexp{
		regexp.MustCompile(`(?i)task queue`),
		regexp.MustCompile(`(?i)\bsessions?\b`),
		regexp.MustCompile(`(?i)worker deployment`),
		regexp.MustCompile(`\bSignals?\b`),
		regexp.MustCompile(`\bTimers?\b`),
		regexp.MustCompile(`(?i)child workflow`),
		regexp.MustCompile(`(?i)parent-close`),
		regexp.MustCompile(`(?i)local activit`),
		regexp.MustCompile(`(?i)temporal memo`),
		regexp.MustCompile(`(?i)search attribute`),
		regexp.MustCompile(`(?i)visibility quer`),
		regexp.MustCompile(`(?i)temporal schedule id`),
		regexp.MustCompile(`(?i)workflow id`),
		regexp.MustCompile(`(?i)\bchannels?\b`),
		regexp.MustCompile(`(?i)\bgenerations?\b`),
		regexp.MustCompile(`(?i)\bcheckpoints?\b`),
	}
	// The rules must match what they are for, wrapped across lines too, or
	// a mistyped pattern would pass everything.
	for _, bad := range []string{
		"a task\nqueue", "the session", "Worker Deployment Version", "a Signal", "its Timer",
		"a child\nworkflow", "parent-close policy", "a local activity", "a Temporal memo",
		"a search attribute", "a visibility query", "a Temporal Schedule id", "the workflow id",
		"its own channel", "the next generation", "a checkpoint",
	} {
		if !matchesAny(forbidden, join(bad)) {
			t.Errorf("no rule matches the internal phrase %q", bad)
		}
	}
	var found []string
	heading := ""
	// A paragraph is matched whole, joined onto one line, so a phrase the
	// proto wraps across two lines is still found.
	for _, block := range strings.Split(render(t), "\n\n") {
		if strings.HasPrefix(block, "### ") {
			heading = strings.TrimPrefix(strings.SplitN(block, "\n", 2)[0], "### ")
		}
		text := join(block)
		for _, re := range forbidden {
			if m := re.FindString(text); m != "" {
				found = append(found, heading+": "+m)
			}
		}
	}
	if len(found) > 0 {
		t.Fatalf("the API reference names internal mechanisms (reword the proto comment):\n  %s", strings.Join(found, "\n  "))
	}
}

func join(s string) string { return strings.Join(strings.Fields(s), " ") }

func matchesAny(res []*regexp.Regexp, s string) bool {
	for _, re := range res {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// TestAPIDocs_NoRawHTML: a placeholder in a comment (<run id>) is escaped,
// so nothing in the reference outside a code span reads as an HTML tag but
// the line breaks the tables use.
func TestAPIDocs_NoRawHTML(t *testing.T) {
	tag := regexp.MustCompile(`<[A-Za-z/]`)
	for i, line := range strings.Split(render(t), "\n") {
		parts := strings.Split(strings.ReplaceAll(line, "<br>", ""), "`")
		for j := 0; j < len(parts); j += 2 {
			if m := tag.FindString(parts[j]); m != "" {
				t.Errorf("line %d has raw HTML %q outside a code span: %s", i+1, m, line)
			}
		}
	}
}

// TestAPIDocs_EveryLinkResolves: every in-page link names a heading the
// page has, so a type the reachability walk missed shows as a dangling link.
func TestAPIDocs_EveryLinkResolves(t *testing.T) {
	doc := render(t)
	slugs := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^### (.+)$`).FindAllStringSubmatch(doc, -1) {
		slugs[regexp.MustCompile(`[^a-z0-9_-]+`).ReplaceAllString(strings.ToLower(m[1]), "")] = true
	}
	links := regexp.MustCompile(`\]\(#([^)]+)\)`).FindAllStringSubmatch(doc, -1)
	if len(links) == 0 {
		t.Fatal("the reference has no in-page links: the check is vacuous")
	}
	for _, m := range links {
		if !slugs[m[1]] {
			t.Errorf("link #%s names no heading", m[1])
		}
	}
}

// TestAPIDocs_RenderOnlyWhatAClientReaches: a message no method takes or
// returns is engine-internal and stays out; one every client sees is in.
func TestAPIDocs_RenderOnlyWhatAClientReaches(t *testing.T) {
	doc := render(t)
	for _, want := range []string{"\n### RunService\n", "\n### SubmitRunRequest\n", "\n### Run\n", "\n### Status\n"} {
		if !strings.Contains(doc, want) {
			t.Errorf("the reference lacks %q", strings.TrimSpace(want))
		}
	}
	for _, internal := range []string{"\n### RunContinuation\n", "\n### ExecuteTaskInput\n", "\n### ExecuteTaskResult\n"} {
		if strings.Contains(doc, internal) {
			t.Errorf("the reference renders the engine-internal %q", strings.TrimSpace(internal))
		}
	}
}
