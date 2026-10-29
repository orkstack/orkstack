package dist

import (
	"fmt"
	"regexp"
	"strings"
)

// ChangelogFile is the changelog a release's notes come from.
const ChangelogFile = "CHANGELOG.md"

// linkDefinition is a Markdown reference-link definition, which Keep a
// Changelog puts at the bottom of the file, after the last release.
var linkDefinition = regexp.MustCompile(`^\[[^\]]+\]:\s+\S`)

// UnreleasedHeading is the heading of the section a change's entry lands in
// until a release takes it into a section of its own.
const UnreleasedHeading = "## [Unreleased]"

// ReleaseNotes returns the text of the "## [X.Y.Z]" section of the changelog
// for version: everything after that heading up to the next "## " heading, with
// the surrounding blank lines and any trailing link definitions removed, and a
// single final newline. The heading itself is left out, because the release
// has its own title. The "## [Unreleased]" section is never a release.
//
// It refuses a changelog with no such section, and one whose section is empty,
// so a release cannot be published with nothing to say for it. A line that
// starts with "## " inside a code fence belongs to the section's text.
func ReleaseNotes(changelog []byte, version Version) (string, error) {
	heading := "## [" + version.Plain() + "]"
	body, found := section(changelog, heading)
	if !found {
		return "", fmt.Errorf("%s has no %q section; a release needs one, and it becomes the release notes",
			ChangelogFile, heading)
	}
	if len(body) == 0 {
		return "", fmt.Errorf("the %q section of %s is empty; a release needs notes", heading, ChangelogFile)
	}
	return strings.Join(body, "\n") + "\n", nil
}

// CheckNothingUnreleased refuses a changelog whose "## [Unreleased]" section
// still holds entries. At a release they become the version's own section,
// which is its notes, so an entry left behind would ship in the release
// without being in them. A changelog with no such section leaves nothing
// behind.
func CheckNothingUnreleased(changelog []byte, version Version) error {
	body, _ := section(changelog, UnreleasedHeading)
	if len(body) == 0 {
		return nil
	}
	return fmt.Errorf("the %q section of %s still holds entries, starting %q; at a release they move into its %q section, "+
		"which becomes the release notes, and %q is left empty",
		UnreleasedHeading, ChangelogFile, body[0], "## ["+version.Plain()+"]", UnreleasedHeading)
}

// section returns the lines under heading, the first "## " line that is
// heading or heading followed by a space, up to the next "## " heading outside
// a code fence, with the blank lines at both ends and any trailing link
// definitions removed. found is false when no line is that heading.
func section(changelog []byte, heading string) (body []string, found bool) {
	text := strings.ReplaceAll(string(changelog), "\r\n", "\n")
	var open fenceRun // the fence the line is inside, or none
	for _, line := range strings.Split(text, "\n") {
		if run := fenceMarker(line); run.length > 0 {
			switch {
			case open.length == 0:
				open = run
			case run.closes(open):
				open = fenceRun{}
			}
		} else if open.length == 0 && strings.HasPrefix(line, "## ") {
			if found {
				break
			}
			if line == heading || strings.HasPrefix(line, heading+" ") {
				found = true
				continue
			}
		}
		if found {
			body = append(body, line)
		}
	}
	body = trimBlank(body)
	for len(body) > 0 && linkDefinition.MatchString(body[len(body)-1]) {
		body = trimBlank(body[:len(body)-1])
	}
	return body, found
}

// fenceRun is a line's run of three or more backticks or tildes, which opens
// or closes a fenced code block, and what follows the run on that line.
type fenceRun struct {
	char   byte
	length int
	rest   string
}

// closes reports whether run closes the fence open: CommonMark closes a fence
// only with a run of the same character at least as long as the one that
// opened it and nothing but spaces after it.
func (run fenceRun) closes(open fenceRun) bool {
	return run.char == open.char && run.length >= open.length && strings.TrimSpace(run.rest) == ""
}

// fenceMarker returns the run of backticks or tildes that starts line, when it
// is a fence line (indented at most three spaces), and a zero fenceRun
// otherwise.
func fenceMarker(line string) fenceRun {
	trimmed := strings.TrimLeft(line, " ")
	if len(line)-len(trimmed) > 3 || trimmed == "" || (trimmed[0] != '`' && trimmed[0] != '~') {
		return fenceRun{}
	}
	n := 0
	for n < len(trimmed) && trimmed[n] == trimmed[0] {
		n++
	}
	if n < 3 {
		return fenceRun{}
	}
	return fenceRun{char: trimmed[0], length: n, rest: trimmed[n:]}
}

// trimBlank drops the blank lines at both ends of lines.
func trimBlank(lines []string) []string {
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}
