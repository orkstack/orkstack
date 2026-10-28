package docscheck

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

// repoRoot is the module root: this package lives at internal/docscheck. It is
// resolved through symbolic links, because the walk never follows one at its
// root and git names the tree's top by its real path.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if root, err = filepath.EvalSymlinks(root); err != nil {
		t.Fatal(err)
	}
	return root
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func codeSpans(doc string) []string {
	var out []string
	for {
		i := strings.Index(doc, "`")
		if i < 0 {
			return out
		}
		doc = doc[i+1:]
		j := strings.Index(doc, "`")
		if j < 0 {
			return out
		}
		out = append(out, doc[:j])
		doc = doc[j+1:]
	}
}

var (
	linkRE     = regexp.MustCompile(`\[[^\]]*\]\(([^)\s]+)\)`)
	headingRE  = regexp.MustCompile(`(?m)^#{1,6}\s+(.+?)\s*#*\s*$`)
	makeRE     = regexp.MustCompile(`^make(?:\s+-C\s+images)?\s+([A-Za-z0-9_.-]+)(?:\s|$)`)
	agentsRef  = regexp.MustCompile(`AGENTS\.md\s*§(\d+(?:\.\d+)?)`)
	agentsHead = regexp.MustCompile(`(?m)^#{2,3}\s+(\d+(?:\.\d+)?)\.?\s`)
	wordRE     = regexp.MustCompile(`[^A-Za-z0-9_]`)
)

// TestDocs_RelativeLinksResolve: every repo-relative markdown link names a
// file that exists, and a `.md#anchor` target's heading exists.
func TestDocs_RelativeLinksResolve(t *testing.T) {
	var bad []string
	for _, file := range markdownFiles(t) {
		doc := readFile(t, file)
		for _, m := range linkRE.FindAllStringSubmatch(doc, -1) {
			target := m[1]
			if target == "" || strings.HasPrefix(target, "#") ||
				strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") ||
				strings.HasPrefix(target, "mailto:") {
				continue
			}
			path, anchor, _ := strings.Cut(target, "#")
			if path == "" {
				continue
			}
			resolved := filepath.Join(filepath.Dir(file), path)
			if !strings.HasPrefix(path, "/") {
				if _, err := os.Stat(resolved); err != nil {
					bad = append(bad, fmt.Sprintf("%s: %s", filepath.ToSlash(file), target))
					continue
				}
			}
			if anchor != "" && strings.HasSuffix(path, ".md") {
				if !hasAnchor(readFile(t, resolved), anchor) {
					bad = append(bad, fmt.Sprintf("%s: %s (#%s has no heading)", filepath.ToSlash(file), target, anchor))
				}
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d relative links do not resolve:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

func hasAnchor(doc, anchor string) bool {
	for _, m := range headingRE.FindAllStringSubmatch(doc, -1) {
		if slug(m[1]) == anchor {
			return true
		}
	}
	return false
}

func slug(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == ' ' || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	return strings.ReplaceAll(b.String(), " ", "-")
}

// TestDocs_EveryMakeTargetExists: every `make <target>` a document writes as
// a code span or a fenced line names a target that exists.
func TestDocs_EveryMakeTargetExists(t *testing.T) {
	root := repoRoot(t)
	targets := map[string]bool{}
	for _, mk := range []string{"Makefile", "images/Makefile"} {
		body := readFile(t, filepath.Join(root, mk))
		for _, m := range regexp.MustCompile(`(?m)^([A-Za-z0-9_.-]+):`).FindAllStringSubmatch(body, -1) {
			targets[m[1]] = true
		}
		for _, m := range regexp.MustCompile(`([A-Za-z0-9_.-]+%[A-Za-z0-9_.-]*):`).FindAllStringSubmatch(body, -1) {
			targets[m[1]] = true
		}
	}
	var bad []string
	for _, file := range markdownFiles(t) {
		for _, span := range invocations(readFile(t, file)) {
			m := makeRE.FindStringSubmatch(span)
			if m == nil {
				continue
			}
			if strings.Contains(m[1], "$") || strings.Contains(m[1], "(") {
				continue
			}
			if !targetExists(targets, m[1]) {
				bad = append(bad, fmt.Sprintf("%s: make %s", filepath.ToSlash(file), m[1]))
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d make targets do not exist:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

// invocations returns code spans and fenced lines, where a command is
// written as code rather than as prose.
// targetExists matches a concrete name against a Makefile pattern rule
// (`image-%` covers `image-shell`).
func targetExists(targets map[string]bool, name string) bool {
	if targets[name] {
		return true
	}
	for pattern := range targets {
		if i := strings.Index(pattern, "%"); i >= 0 {
			if strings.HasPrefix(name, pattern[:i]) && strings.HasSuffix(name, pattern[i+1:]) {
				return true
			}
		}
	}
	return false
}

func invocations(doc string) []string {
	var out []string
	inFence := false
	for _, line := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			out = append(out, trimmed)
			continue
		}
		out = append(out, codeSpans(line)...)
	}
	return out
}

// TestReadme_DocsTableListsEveryDoc: every docs/*.md is a link target in
// README's Docs section.
func TestReadme_DocsTableListsEveryDoc(t *testing.T) {
	root := repoRoot(t)
	readme := readFile(t, filepath.Join(root, "README.md"))
	start := strings.Index(readme, "## Docs")
	if start < 0 {
		t.Fatal("README.md has no ## Docs section")
	}
	section := readme[start:]
	if end := strings.Index(section[1:], "\n## "); end >= 0 {
		section = section[:end+1]
	}
	linked := map[string]bool{}
	for _, m := range linkRE.FindAllStringSubmatch(section, -1) {
		linked[strings.TrimPrefix(m[1], "./")] = true
	}
	var missing []string
	for _, doc := range docsFiles(t) {
		if name := "docs/" + filepath.Base(doc); !linked[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("README's Docs table omits %v", missing)
	}
}

// TestReadme_OrkflowIsTheShippedExample: the Orkflow the README shows is
// examples/each-fan-out.ork.yaml from its apiVersion line on, byte for byte,
// so the front page never shows a document the engine would not accept.
func TestReadme_OrkflowIsTheShippedExample(t *testing.T) {
	root := repoRoot(t)
	readme := readFile(t, filepath.Join(root, "README.md"))
	example := readFile(t, filepath.Join(root, "examples", "each-fan-out.ork.yaml"))
	const fence = "```yaml\n"
	start := strings.Index(readme, fence)
	if start < 0 {
		t.Fatal("README.md has no yaml code block")
	}
	block := readme[start+len(fence):]
	end := strings.Index(block, "```")
	if end < 0 {
		t.Fatal("README.md's yaml code block is not closed")
	}
	block = block[:end]
	at := strings.Index(example, "apiVersion:")
	if at < 0 {
		t.Fatal("examples/each-fan-out.ork.yaml has no apiVersion line")
	}
	if want := example[at:]; block != want {
		t.Fatalf("README.md's Orkflow differs from examples/each-fan-out.ork.yaml from its apiVersion line on:\n--- README\n%s--- example\n%s", block, want)
	}
}

// TestChangelog_HasAnUnreleasedSection: the changelog starts with an
// Unreleased section, and its subsections are Keep a Changelog's.
func TestChangelog_HasAnUnreleasedSection(t *testing.T) {
	doc := readFile(t, filepath.Join(repoRoot(t), "CHANGELOG.md"))
	if !strings.Contains(doc, "## [Unreleased]") {
		t.Fatal("CHANGELOG.md has no ## [Unreleased] section")
	}
	allowed := map[string]bool{"Added": true, "Changed": true, "Deprecated": true, "Removed": true, "Fixed": true, "Security": true}
	section := doc[strings.Index(doc, "## [Unreleased]"):]
	if end := strings.Index(section[1:], "\n## "); end >= 0 {
		section = section[:end+1]
	}
	for _, m := range regexp.MustCompile(`(?m)^###\s+(.+)$`).FindAllStringSubmatch(section, -1) {
		if !allowed[strings.TrimSpace(m[1])] {
			t.Errorf("CHANGELOG.md [Unreleased] has a subsection %q; Keep a Changelog's are Added, Changed, Deprecated, Removed, Fixed and Security", m[1])
		}
	}
}

// TestDocs_AgentsSectionReferencesResolve: every "AGENTS.md §N" names a
// heading that exists.
func TestDocs_AgentsSectionReferencesResolve(t *testing.T) {
	root := repoRoot(t)
	agents := readFile(t, filepath.Join(root, "AGENTS.md"))
	heads := map[string]bool{}
	for _, m := range agentsHead.FindAllStringSubmatch(agents, -1) {
		heads[m[1]] = true
	}
	var bad []string
	for _, file := range markdownFiles(t) {
		for _, m := range agentsRef.FindAllStringSubmatch(readFile(t, file), -1) {
			if !heads[m[1]] {
				bad = append(bad, fmt.Sprintf("%s: §%s", filepath.ToSlash(file), m[1]))
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d AGENTS.md section references do not resolve:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

// TestRepositoryNeverKillsByNameOrPattern: the repository's own rules must
// not spell a command that stops every process of a name or a pattern.
func TestRepositoryNeverKillsByNameOrPattern(t *testing.T) {
	words := []string{"pkill", "killall", "pgrep", "xargs kill"}
	var bad []string
	for _, file := range markdownFiles(t) {
		doc := readFile(t, file)
		for _, word := range words {
			if strings.Contains(doc, word) {
				bad = append(bad, filepath.ToSlash(file)+" contains "+word)
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("a document names a by-name or by-pattern stop command; use scripts/proof-stop.sh or a PID:\n%s", strings.Join(bad, "\n"))
	}
}

// TestDocs_EnumerationsAreCacheVisible: a drift test that runs git reads an
// index go test's cache cannot see, so it must open the index file itself
// first, asking git for its path; the cache keys on it where the git directory
// lies inside the module, and make check runs the package uncached for the
// rest (TestMakeCheck_RunsGitStatePackagesUncached). treeFiles and
// TestRepositoryScopeTable_MapsEveryTrackedPath do; this keeps them, and any
// future one, honest. It matches the call, not a mention of the flag, so a
// file that only names it, as this one does, is not taken for one that opens
// the index.
func TestDocs_EnumerationsAreCacheVisible(t *testing.T) {
	root := repoRoot(t)
	files := []string{
		filepath.Join(root, "internal/docscheck/docs_test.go"),
		filepath.Join(root, "internal/docscheck/tree_test.go"),
		filepath.Join(root, "internal/docscheck/reasons_test.go"),
		filepath.Join(root, "cmd/orkctl/cli_reference_test.go"),
		filepath.Join(root, "test/tools/proofrun/docs_test.go"),
		filepath.Join(root, "test/tools/proofrun/scope_test.go"),
	}
	gitExec := regexp.MustCompile(`exec\.Command(?:Context\([^,]+,\s*|\()"git"`)
	opensIndex := regexp.MustCompile(`"rev-parse",\s*"--git-path",\s*"index"`)
	for _, file := range files {
		doc := readFile(t, file)
		if gitExec.MatchString(doc) && !opensIndex.MatchString(doc) {
			t.Errorf("%s runs git without opening the index file: a cached pass could hide a new file", filepath.ToSlash(file))
		}
	}
}

// TestMakeCheck_RunsGitStatePackagesUncached: a unit test that runs git reads
// state go test's cache cannot see. The cache records only the files the test
// process itself opens inside the module, and git reads HEAD, the index and
// the ignore rules in a subprocess, so a change to what git tracks or ignores
// alone leaves a cached pass standing. Every package whose unit tests run git
// is in the Makefile's GIT_STATE_PACKAGES, which `make check`, the one target
// that keeps the cache, runs with -count=1; and the list names no other
// package. The integration suites run only in proofs, which never use the
// cache.
func TestMakeCheck_RunsGitStatePackagesUncached(t *testing.T) {
	root := repoRoot(t)
	makefile := readFile(t, filepath.Join(root, "Makefile"))
	m := regexp.MustCompile(`(?m)^GIT_STATE_PACKAGES :=(.*)$`).FindStringSubmatch(makefile)
	if m == nil {
		t.Fatal("the Makefile declares no GIT_STATE_PACKAGES")
	}
	listed := map[string]bool{}
	for _, p := range strings.Fields(m[1]) {
		listed[p] = true
	}
	if !regexp.MustCompile(`(?m)^\tgo test -short -count=1 \$\(GIT_STATE_PACKAGES\)$`).MatchString(makefile) {
		t.Error("make check does not run $(GIT_STATE_PACKAGES) with -count=1")
	}
	runsGit := regexp.MustCompile(`exec\.(?:Command(?:Context\([^,]+,\s*|\()|LookPath\()"git"`)
	integration := regexp.MustCompile(`(?m)^//go:build integration$`)
	runs := map[string]bool{}
	for rel := range treeFiles(t, root) {
		if !strings.HasSuffix(rel, "_test.go") {
			continue
		}
		text := readFile(t, filepath.Join(root, filepath.FromSlash(rel)))
		if runsGit.MatchString(text) && !integration.MatchString(text) {
			runs["./"+path.Dir(rel)] = true
		}
	}
	if len(runs) == 0 {
		t.Fatal("no unit test runs git: the call shape this test looks for has drifted")
	}
	for p := range runs {
		if !listed[p] {
			t.Errorf("%s has a unit test that runs git, and GIT_STATE_PACKAGES does not list it", p)
		}
	}
	for p := range listed {
		if !runs[p] {
			t.Errorf("GIT_STATE_PACKAGES lists %s, and no unit test there runs git", p)
		}
	}
}

// TestDocs_EveryExportedReasonIsDocumented lives in reasons_test.go.

var _ = wordRE
