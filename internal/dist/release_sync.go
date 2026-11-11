package dist

import (
	"fmt"
	"strings"
)

// releaseFooter opens the footer ReleasePage writes below a release's notes:
// the rule and the Images heading. A "---" in the notes alone is not it.
const releaseFooter = "\n---\n\n### Images\n"

// SplitReleaseBody returns a published release body's notes, the text above
// the footer ReleasePage writes, and the footer from its rule on, with the
// body's line endings normalised to "\n", so notes+footer is that body.
// found is false when the body has no footer, as v0.1.0's page has none, and
// then notes is the whole body.
func SplitReleaseBody(body string) (notes, footer string, found bool) {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	at := strings.Index(body, releaseFooter)
	if at < 0 {
		return body, "", false
	}
	// ReleasePage writes notes that end in a newline, then the footer, whose
	// first newline makes the blank line above its rule.
	return body[:at], body[at:], true
}

// CheckReleaseNotesInSync refuses a published release body whose notes are
// not want, the version's CHANGELOG section as ReleaseNotes returns it. Line
// endings and the newlines a body ends with do not count: GitHub may hand
// either back changed. The refusal names the version, the first line that
// differs, both sides of it, and how to republish.
func CheckReleaseNotesInSync(version Version, body, want string) error {
	notes, _, _ := SplitReleaseBody(body)
	got := strings.Split(strings.TrimRight(notes, "\n"), "\n")
	section := strings.Split(strings.TrimRight(strings.ReplaceAll(want, "\r\n", "\n"), "\n"), "\n")
	for i := 0; i < len(got) || i < len(section); i++ {
		var diff string
		switch {
		case i >= len(got):
			diff = fmt.Sprintf("the published notes end, where the CHANGELOG section goes on with %q", section[i])
		case i >= len(section):
			diff = fmt.Sprintf("the published notes say %q, where the CHANGELOG section ends", got[i])
		case got[i] != section[i]:
			diff = fmt.Sprintf("the published notes say %q, where the CHANGELOG section says %q", got[i], section[i])
		default:
			continue
		}
		return fmt.Errorf("%s: the release notes differ from the %q section of %s at line %d: %s; "+
			"republish them from the section with make release-notes-publish VERSION=%s, never by editing the release",
			version, "## ["+version.Plain()+"]", ChangelogFile, i+1, diff, version)
	}
	return nil
}

// RepublishedBody returns the body a republish of version sends: want, its
// CHANGELOG section's notes, above the footer the published body already
// has, kept byte for byte, so the digests the release published stay. It
// refuses a body with no footer, which has no digests to keep.
func RepublishedBody(version Version, published, want string) (string, error) {
	_, footer, found := SplitReleaseBody(published)
	if !found {
		return "", fmt.Errorf("%s: the published release body has no image footer (a %q rule followed by %q), "+
			"so a republish has no digests to keep; edit that release by hand only with the maintainer",
			version, "---", "### Images")
	}
	return want + footer, nil
}
