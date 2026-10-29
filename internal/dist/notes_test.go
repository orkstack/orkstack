package dist_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
)

const sampleChangelog = `# Changelog

Preamble that no release owns.

## [Unreleased]

### Added

- Something not yet released.

## [1.2.3] - 2026-10-24

### Added

- The 1.2.3 feature.

### Fixed

- The 1.2.3 fix.

## [1.2.30] - 2026-10-20

### Added

- A different release whose number starts the same.

## [1.2.2]

- No date on this one.

## [0.9.0] - 2026-01-01

- The last one.

[Unreleased]: https://example.invalid/compare/v1.2.3...HEAD
[1.2.3]: https://example.invalid/compare/v1.2.2...v1.2.3
[0.9.0]: https://example.invalid/releases/tag/v0.9.0
`

func notesFor(t *testing.T, changelog, version string) (string, error) {
	t.Helper()
	return dist.ReleaseNotes([]byte(changelog), mustVersion(t, version))
}

// TestReleaseNotes_ExtractsOnlyThatVersionsSection: from after its heading to
// the next release heading, nothing of its neighbours, no heading of its own
// (the release has a title) and one trailing newline.
func TestReleaseNotes_ExtractsOnlyThatVersionsSection(t *testing.T) {
	t.Parallel()
	got, err := notesFor(t, sampleChangelog, "v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	want := "### Added\n\n- The 1.2.3 feature.\n\n### Fixed\n\n- The 1.2.3 fix.\n"
	if got != want {
		t.Fatalf("notes\n got: %q\nwant: %q", got, want)
	}
}

// TestReleaseNotes_Headings: a heading with a date, without one, the first
// release and the last (whose section runs to the end, minus link
// definitions).
func TestReleaseNotes_Headings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		version string
		want    string
	}{
		{"v1.2.30", "### Added\n\n- A different release whose number starts the same.\n"},
		{"v1.2.2", "- No date on this one.\n"},
		{"v0.9.0", "- The last one.\n"},
	}
	for _, tc := range tests {
		got, err := notesFor(t, sampleChangelog, tc.version)
		if err != nil {
			t.Errorf("%s: %v", tc.version, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s notes\n got: %q\nwant: %q", tc.version, got, tc.want)
		}
	}
}

// TestReleaseNotes_RefusesWhatIsNotARelease: the Unreleased section is not a
// release, and a version with no section, or an empty one, has no notes. The
// refusal names the version and the file.
func TestReleaseNotes_RefusesWhatIsNotARelease(t *testing.T) {
	t.Parallel()
	empty := "## [Unreleased]\n\n## [2.0.0] - 2026-11-01\n\n   \n\n## [1.0.0]\n\n- one\n"
	tests := []struct {
		name      string
		changelog string
		version   string
		wantInErr string
	}{
		{"a version with no section", sampleChangelog, "v2.0.0", "[2.0.0]"},
		{"a prefix of another version", sampleChangelog, "v1.2.0", "[1.2.0]"},
		{"a longer number", sampleChangelog, "v1.2.300", "[1.2.300]"},
		{"an empty section", empty, "v2.0.0", "empty"},
		{"an empty changelog", "", "v1.0.0", "[1.0.0]"},
		{"only Unreleased", "## [Unreleased]\n\n- x\n", "v0.0.1", "[0.0.1]"},
	}
	for _, tc := range tests {
		got, err := notesFor(t, tc.changelog, tc.version)
		if err == nil {
			t.Errorf("%s: notes %q, want a refusal", tc.name, got)
			continue
		}
		if got != "" {
			t.Errorf("%s: returned notes %q with its refusal", tc.name, got)
		}
		for _, want := range []string{tc.wantInErr, "CHANGELOG.md"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: refusal %q does not mention %q", tc.name, err, want)
			}
		}
	}
}

// TestReleaseNotes_KeepsAFencedHeadingInside: a line that starts with "## [" in
// a code fence is the section's text, not the next section's heading.
func TestReleaseNotes_KeepsAFencedHeadingInside(t *testing.T) {
	t.Parallel()
	changelog := "## [1.0.0] - 2026-01-01\n\n" +
		"Write the heading like this:\n\n" +
		"```markdown\n## [9.9.9] - never released\n```\n\n" +
		"- after the fence\n\n" +
		"## [0.9.0]\n\n- old\n"
	got, err := notesFor(t, changelog, "v1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"## [9.9.9] - never released", "- after the fence"} {
		if !strings.Contains(got, want) {
			t.Errorf("notes %q lost %q", got, want)
		}
	}
	if strings.Contains(got, "old") {
		t.Errorf("notes %q run into the next release", got)
	}
	if _, err := notesFor(t, changelog, "v9.9.9"); err == nil {
		t.Error("a heading inside a fence was taken for a release")
	}
}

// TestReleaseNotes_NestedFences: a fence closes only on a run of its own
// character at least as long as its opener, with nothing after it, so a
// shorter fence inside a longer one is the longer one's text (CommonMark).
func TestReleaseNotes_NestedFences(t *testing.T) {
	t.Parallel()
	changelog := "## [1.0.0] - 2026-01-01\n\n" +
		"````markdown\n```markdown\n## [9.9.9] - inside\n```\n## [9.9.8] - still inside\n````\n\n" +
		"~~~\n```\n## [9.9.7] - a backtick run does not close a tilde fence\n~~~ not a close\n~~~\n\n" +
		"- after the fences\n\n" +
		"## [0.9.0]\n\n- old\n"
	got, err := notesFor(t, changelog, "v1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"## [9.9.9] - inside", "## [9.9.8] - still inside", "## [9.9.7]", "- after the fences"} {
		if !strings.Contains(got, want) {
			t.Errorf("notes %q lost %q", got, want)
		}
	}
	if strings.Contains(got, "old") {
		t.Errorf("notes %q run into the next release", got)
	}
}

// TestReleaseNotes_LineEndings: a CHANGELOG saved with CRLF gives the same notes.
func TestReleaseNotes_LineEndings(t *testing.T) {
	t.Parallel()
	crlf := strings.ReplaceAll(sampleChangelog, "\n", "\r\n")
	want, err := notesFor(t, sampleChangelog, "v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	got, err := notesFor(t, crlf, "v1.2.3")
	if err != nil || got != want {
		t.Fatalf("CRLF notes %q (%v), want %q", got, err, want)
	}
}

// laterSection is a release section as a later release writes it: Keep a
// Changelog's subsections, a **BREAKING** entry, a nested list, a second
// paragraph, a link and a code fence. The notes are exactly these bytes.
const laterSection = "### Added\n\n" +
	"- `orkctl run tail` follows a run's events ([docs/CLI.md](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md)).\n" +
	"  - It resumes by sequence from any replica.\n\n" +
	"### Changed\n\n" +
	"- **BREAKING** `orkctl apply` refuses a file with no `kind:`. Give every\n" +
	"  file one:\n\n" +
	"  ```yaml\n  kind: Orkflow\n  ```\n\n" +
	"### Fixed\n\n" +
	"- A cancelled task's container hears SIGTERM before SIGKILL.\n"

// TestReleaseNotes_AreTheSectionWordForWord: what the release page shows is
// the version's section as written, without its heading, for the first
// release's one line and for a later release's full section; a version the
// changelog does not name has no notes.
func TestReleaseNotes_AreTheSectionWordForWord(t *testing.T) {
	t.Parallel()
	changelog := "# Changelog\n\nA preamble.\n\n" +
		"## [Unreleased]\n\n" +
		"## [0.2.0] - 2026-11-20\n\n" + laterSection + "\n" +
		"## [0.1.0] - 2026-10-29\n\nFirst release.\n"
	tests := []struct {
		name      string
		version   string
		want      string
		wantInErr string
	}{
		{"the first release", "v0.1.0", "First release.\n", ""},
		{"a later release", "v0.2.0", laterSection, ""},
		{"a version the changelog does not name", "v0.3.0", "", "[0.3.0]"},
	}
	for _, tc := range tests {
		got, err := notesFor(t, changelog, tc.version)
		if tc.wantInErr != "" {
			if err == nil || got != "" || !strings.Contains(err.Error(), tc.wantInErr) {
				t.Errorf("%s: notes %q, error %v; want no notes and a refusal naming %s", tc.name, got, err, tc.wantInErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: notes\n got: %q\nwant: %q", tc.name, got, tc.want)
		}
	}
}

// TestCheckNothingUnreleased: a release leaves nothing under [Unreleased],
// because what is there would ship without being in its notes. The refusal
// names the section, the entry it found and the version's section.
func TestCheckNothingUnreleased(t *testing.T) {
	t.Parallel()
	release := "## [1.0.0] - 2026-12-01\n\n- one\n"
	tests := []struct {
		name      string
		changelog string
		wantInErr []string
	}{
		{"an empty Unreleased", "# Changelog\n\n## [Unreleased]\n\n" + release, nil},
		{"only blank lines", "## [Unreleased]\n\n   \n\n" + release, nil},
		{"no Unreleased section", release, nil},
		{"Unreleased last", release + "\n## [Unreleased]\n", nil},
		{"an Unreleased heading inside a fence",
			"## [1.0.0]\n\n```markdown\n## [Unreleased]\n\n- an example\n```\n", nil},
		{"an entry left behind", "## [Unreleased]\n\n### Added\n\n- not in 1.0.0\n\n" + release,
			[]string{`"## [Unreleased]"`, `"### Added"`, `"## [1.0.0]"`, "CHANGELOG.md"}},
		{"a line with no subsection", "## [Unreleased]\n- stray\n" + release, []string{`"- stray"`}},
	}
	for _, tc := range tests {
		err := dist.CheckNothingUnreleased([]byte(tc.changelog), mustVersion(t, "v1.0.0"))
		if tc.wantInErr == nil {
			if err != nil {
				t.Errorf("%s: %v", tc.name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: accepted, want a refusal", tc.name)
			continue
		}
		for _, want := range tc.wantInErr {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: refusal %q does not mention %s", tc.name, err, want)
			}
		}
	}
}

// TestRepository_TheFirstReleaseSaysFirstRelease: 0.1.0's notes are the one
// line the first release says, so the changes folded into it at merge are
// dropped, not moved under its heading.
func TestRepository_TheFirstReleaseSaysFirstRelease(t *testing.T) {
	t.Parallel()
	got, err := dist.ReleaseNotes(readRepoFile(t, dist.ChangelogFile), mustVersion(t, "v0.1.0"))
	if err != nil || got != "First release.\n" {
		t.Fatalf("CHANGELOG.md's 0.1.0 notes are %q (%v), want %q", got, err, "First release.\n")
	}
}
