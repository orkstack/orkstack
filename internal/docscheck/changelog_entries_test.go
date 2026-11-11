package docscheck

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

// ChangelogEntryMaxChars is the most characters one CHANGELOG.md entry may
// say (AGENTS.md §11.1, step 7). It is counted without the entry's "- "
// bullet, its **BREAKING** mark and the links that close it, so a long URL or
// a pull request reference costs nothing.
const ChangelogEntryMaxChars = 200

// changelogTrailingLinks is the parenthesised list of Markdown links an entry
// ends with: its doc, and its pull request once changes land through them,
// as in ` ([DSL.md](https://…), [#123](https://…))`.
var changelogTrailingLinks = regexp.MustCompile(`\s*\((?:\[[^\]]*\]\([^)\s]+\)(?:,\s*)?)+\)\.?$`)

// changelogUpgrading opens the one line a version's Features may close with:
// what to read before upgrading, which is not a feature.
const changelogUpgrading = "**Upgrading:** "

// changelogEntryText is what an entry says: the line without its bullet, its
// **BREAKING** or **Upgrading:** mark and its closing links.
func changelogEntryText(line string) string {
	text := strings.TrimPrefix(line, "- ")
	text = strings.TrimPrefix(text, "**BREAKING** ")
	text = strings.TrimPrefix(text, changelogUpgrading)
	text = changelogTrailingLinks.ReplaceAllString(text, "")
	return strings.TrimSpace(text)
}

// changelogEntryProblems holds every entry of CHANGELOG.md to one line of at
// most ChangelogEntryMaxChars characters (changelogEntryText). Under a
// subsection every non-blank line is an entry, so an entry cannot run on to a
// second line; the one exception is a "**Upgrading:** " line closing the
// Features, held to the same limit. Prose before a version's first "### "
// subsection is not an entry. A line inside a code fence is text.
func changelogEntryProblems(doc string) []string {
	var (
		p          []string
		fenced     bool
		section    string
		subsection string
	)
	for n, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(strings.TrimLeft(line, " "), "```") {
			fenced = !fenced
			continue
		}
		if fenced {
			continue
		}
		switch {
		case strings.HasPrefix(line, "## "):
			section, subsection = line, ""
		case strings.HasPrefix(line, "### "):
			subsection = line
		case subsection == "" || strings.TrimSpace(line) == "":
		case !strings.HasPrefix(line, "- ") && !(subsection == "### Features" && strings.HasPrefix(line, changelogUpgrading)):
			p = append(p, fmt.Sprintf("line %d, in %s %s, is not an entry: an entry is one line starting \"- \", and only the Features may close with one %q line", n+1, section, subsection, strings.TrimSpace(changelogUpgrading)))
		default:
			text := changelogEntryText(line)
			if size := utf8.RuneCountInString(text); size > ChangelogEntryMaxChars {
				p = append(p, fmt.Sprintf("line %d, in %s %s, says %d characters, over the %d an entry may say without its closing links: %.60q…",
					n+1, section, subsection, size, ChangelogEntryMaxChars, text))
			}
		}
	}
	return p
}

// TestChangelog_EntriesAreOneShortLine: every entry of CHANGELOG.md is one
// line a person reads at a glance, its detail left to the doc it links.
func TestChangelog_EntriesAreOneShortLine(t *testing.T) {
	for _, problem := range changelogEntryProblems(readFile(t, filepath.Join(repoRoot(t), "CHANGELOG.md"))) {
		t.Error("CHANGELOG.md: " + problem)
	}
}

// TestChangelogEntryProblems_HoldsEachEntryToOneShortLine: an entry over the
// limit is found, its closing links, bullet and BREAKING mark are not
// counted, prose before a version's first subsection and a fenced block are
// not entries, the Features may close with one Upgrading line, and an
// entry that runs on to a second line is refused.
func TestChangelogEntryProblems_HoldsEachEntryToOneShortLine(t *testing.T) {
	short := strings.Repeat("a", ChangelogEntryMaxChars)
	long := strings.Repeat("a", ChangelogEntryMaxChars+1)
	links := " ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#a-long-anchor), [#123](https://github.com/orkstack/orkstack/pull/123))"
	highlights := strings.Repeat("Prose before the first subsection runs as long as it needs. ", 20)
	for _, tc := range []struct {
		name, doc, want string
	}{
		{"at the limit, with links and a mark", "## [Unreleased]\n\n" + highlights + "\n\n### Changed\n\n- **BREAKING** " + short + links + "\n- " + short + " ([x](https://x.test/y)).\n", ""},
		{"a fenced block", "## [Unreleased]\n\n### Added\n\n```text\n" + long + "\nnot an entry\n```\n", ""},
		{"one character over", "## [Unreleased]\n\n### Fixed\n\n- " + long + links + "\n", "201 characters"},
		{"a link inside the text counts", "## [0.2.0] - 2026-11-05\n\n### Added\n\n- " + short[:150] + " [see](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md) and more.\n", "over the 200"},
		{"an entry on two lines", "## [Unreleased]\n\n### Added\n\n- first line\n  runs on\n", "line 6"},
		{"prose under a subsection", "## [Unreleased]\n\n### Added\n\nSome prose.\n", "is not an entry"},
		{"an Upgrading line closing the Features", "## [Unreleased]\n\n### Features\n\n- Sleeping tasks.\n\n**Upgrading:** " + short + links + ".\n\n### Added\n\n- a\n", ""},
		{"an Upgrading line over the limit", "## [Unreleased]\n\n### Features\n\n- Sleeping tasks.\n\n**Upgrading:** " + long + "\n", "201 characters"},
		{"an Upgrading line outside the Features", "## [Unreleased]\n\n### Changed\n\n**Upgrading:** read the guide.\n", "is not an entry"},
	} {
		got := strings.Join(changelogEntryProblems(tc.doc), "\n")
		switch {
		case tc.want == "" && got != "":
			t.Errorf("%s: problems %q, want none", tc.name, got)
		case tc.want != "" && !strings.Contains(got, tc.want):
			t.Errorf("%s: problems %q do not mention %q", tc.name, got, tc.want)
		}
	}
}
