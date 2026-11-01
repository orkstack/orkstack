package dist

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf16"

	"github.com/orkstack/orkstack/internal/dsl"
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

// MaxReleaseBody is the most characters GitHub accepts in a release's body.
// publish creates the release last, after the tag and the images are out,
// so a longer page is refused before anything is: by validate, and by the
// artifacts job that writes the page.
const MaxReleaseBody = 125000

// placeholderDigest stands in for a digest before any image is built: the
// length of every sha256 digest, so a page written with placeholders is
// exactly as long as the page publish creates.
const placeholderDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

// ReleasePage is the GitHub release's page: notes, the version's changelog
// section (ReleaseNotes), and then a footer that names each image the
// release publishes by its digest, the tags it is published under, the key
// that signed it, and the commands that verify each image and the
// checksums. With images nil, before any image is built, every digest is a
// placeholder of a digest's length; an empty, non-nil list is a list that
// names no image, and is refused. It refuses images that are not exactly
// the release's, each once with a well-formed digest, and a page over
// MaxReleaseBody characters, naming its size and how much of it is the
// footer. Characters are counted as UTF-16 code units (bodyLength).
func ReleasePage(notes string, version Version, images []Image) (string, error) {
	digests := map[string]string{}
	if images == nil {
		for _, name := range Images() {
			digests[name] = placeholderDigest
		}
	} else {
		for _, image := range images {
			switch {
			case !slices.Contains(Images(), image.Name):
				return "", fmt.Errorf("the release page names the image %q, which a release does not publish", image.Name)
			case digests[image.Name] != "":
				return "", fmt.Errorf("the release page names %s twice", image.Name)
			case !dsl.ValidDigest(image.Digest):
				return "", fmt.Errorf("the release page gives %s the digest %q, which is not sha256:<64 lowercase hex>", image.Name, image.Digest)
			}
			digests[image.Name] = image.Digest
		}
		var missing []string
		for _, name := range Images() {
			if digests[name] == "" {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			return "", fmt.Errorf("the release page names no digest for %s", strings.Join(missing, ", "))
		}
	}
	// The platform's images first, then the activities in catalog order.
	order := ServiceImages()
	for _, name := range ActivityNames() {
		order = append(order, ActivityPrefix+name)
	}
	ref := func(name string) string { return "ghcr.io/" + ImageNamespace + "/" + name }

	var footer strings.Builder
	fmt.Fprintf(&footer, "\n---\n\n### Images\n\n"+
		"Each image is published to `ghcr.io/%s/<image>`, tagged `%s` and `%s`, and signed by its digest with the "+
		"release's key. Its public half is `cosign.pub` below, the same file as `deploy/cosign.pub` at the tag `%s`.\n\n"+
		"| Image | Digest |\n| --- | --- |\n",
		ImageNamespace, version.Plain(), version.Series(), version)
	for _, name := range order {
		fmt.Fprintf(&footer, "| `%s` | `%s` |\n", ref(name), digests[name])
	}
	footer.WriteString("\n### Verify\n\n```bash\n")
	for _, name := range order {
		fmt.Fprintf(&footer, "cosign verify --key cosign.pub --insecure-ignore-tlog=true %s@%s\n", ref(name), digests[name])
	}
	footer.WriteString("cosign verify-blob --key cosign.pub --insecure-ignore-tlog=true --bundle SHA256SUMS.sigstore.json SHA256SUMS\n" +
		"sha256sum --ignore-missing -c SHA256SUMS\n```\n")

	page := notes + footer.String()
	if size := bodyLength(page); size > MaxReleaseBody {
		return "", fmt.Errorf("the release page is %d characters, over GitHub's limit of %d for a release body; "+
			"the images footer is %d of them, and the rest is the %q section of %s, which must lose %d characters",
			size, MaxReleaseBody, bodyLength(footer.String()), "## ["+version.Plain()+"]", ChangelogFile, size-MaxReleaseBody)
	}
	return page, nil
}

// bodyLength is the length a release body is held to: its UTF-16 code units,
// which are never fewer than its code points and are what a browser counts.
// A character outside the Basic Multilingual Plane counts twice, so whichever
// of the two GitHub counts, the check errs only toward refusing.
func bodyLength(s string) int {
	return len(utf16.Encode([]rune(s)))
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
