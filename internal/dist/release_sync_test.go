package dist_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
)

// syncNotes is a version's notes as ReleaseNotes returns them: its Features,
// an Upgrading line and an entry, ending in one newline.
const syncNotes = "### Features\n\n- A new web UI.\n\n" +
	"**Upgrading:** read the **BREAKING** entries first ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md)).\n\n" +
	"### Fixed\n\n- A fix.\n"

// syncBody is the body a release published for syncNotes: the page
// ReleasePage writes, its image footer included.
func syncBody(t *testing.T, notes string) string {
	t.Helper()
	page, err := dist.ReleasePage(notes, mustVersion(t, "v0.2.0"), nil)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

// TestSplitReleaseBody_FindsTheImageFooter: a published body is its notes and
// the footer ReleasePage writes below them, whatever line endings GitHub hands
// back; a body with no footer, as v0.1.0's page has, is all notes.
func TestSplitReleaseBody_FindsTheImageFooter(t *testing.T) {
	t.Parallel()
	body := syncBody(t, syncNotes)
	notes, footer, ok := dist.SplitReleaseBody(body)
	if !ok || notes != syncNotes || !strings.HasPrefix(footer, "\n---\n\n### Images\n") || notes+footer != body {
		t.Fatalf("split %q / %q (footer %v); want the notes and the footer, together the body", notes, footer, ok)
	}
	if crlf, _, ok := dist.SplitReleaseBody(strings.ReplaceAll(body, "\n", "\r\n")); !ok || crlf != syncNotes {
		t.Fatalf("a CRLF body: notes %q (footer %v), want the same notes", crlf, ok)
	}
	// A "---" in the notes themselves is not the footer: only the rule
	// followed by the Images heading is.
	ruled := "- An entry.\n\n---\n\n- Another.\n"
	if notes, _, ok := dist.SplitReleaseBody(syncBody(t, ruled)); !ok || notes != ruled {
		t.Fatalf("notes holding a rule: %q (footer %v), want them whole", notes, ok)
	}
	if notes, footer, ok := dist.SplitReleaseBody("First release.\n"); ok || footer != "" || notes != "First release.\n" {
		t.Fatalf("a body with no footer: %q / %q (footer %v), want it all as notes", notes, footer, ok)
	}
}

// TestCheckReleaseNotesInSync: a published body matches its CHANGELOG section
// when its notes are the section's word for word, line endings and a missing
// final newline aside; any other difference is refused, naming the version,
// the first line that differs and both sides of it.
func TestCheckReleaseNotesInSync(t *testing.T) {
	t.Parallel()
	version := mustVersion(t, "v0.2.0")
	body := syncBody(t, syncNotes)
	for _, tc := range []struct {
		name, body string
		want       []string // in the error; nil means in sync
	}{
		{"the published page", body, nil},
		{"CRLF line endings", strings.ReplaceAll(body, "\n", "\r\n"), nil},
		{"no final newline", strings.TrimRight(body, "\n"), nil},
		{"a body with no footer", "First release.", nil},
		{"an entry edited by hand", strings.Replace(body, "- A fix.", "- A fix, edited on GitHub.", 1),
			[]string{"v0.2.0", "line 9", `"- A fix, edited on GitHub."`, `"- A fix."`, "make release-notes-publish VERSION=v0.2.0"}},
		{"an entry the section lacks", strings.Replace(body, "- A fix.\n", "- A fix.\n- Another fix.\n", 1),
			[]string{"v0.2.0", "line 10", `"- Another fix."`, "section ends"}},
		{"an entry the body lacks", strings.Replace(body, "\n### Fixed\n\n- A fix.\n", "", 1),
			[]string{"v0.2.0", "line 6", "published notes end"}},
	} {
		want := syncNotes
		if tc.name == "a body with no footer" {
			want = "First release.\n"
		}
		err := dist.CheckReleaseNotesInSync(version, tc.body, want)
		switch {
		case tc.want == nil && err != nil:
			t.Errorf("%s: %v, want in sync", tc.name, err)
		case tc.want != nil && err == nil:
			t.Errorf("%s: in sync, want a refusal", tc.name)
		case tc.want != nil:
			for _, part := range tc.want {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("%s: %q does not name %q", tc.name, err, part)
				}
			}
		}
	}
}

// TestRepublishedBody_KeepsThePublishedFooter: a republish sends the section's
// notes above the footer the release already has, byte for byte, and refuses
// a body with no footer, which has no digests to keep.
func TestRepublishedBody_KeepsThePublishedFooter(t *testing.T) {
	t.Parallel()
	version := mustVersion(t, "v0.2.0")
	published := syncBody(t, "- What the release said before.\n")
	_, footer, _ := dist.SplitReleaseBody(published)
	got, err := dist.RepublishedBody(version, published, syncNotes)
	if err != nil || got != syncNotes+footer {
		t.Fatalf("republished %q, %v; want the new notes above the published footer", got, err)
	}
	if err := dist.CheckReleaseNotesInSync(version, got, syncNotes); err != nil {
		t.Fatalf("the republished body is not in sync: %v", err)
	}
	if _, err := dist.RepublishedBody(version, "First release.\n", syncNotes); err == nil ||
		!strings.Contains(err.Error(), "v0.2.0") || !strings.Contains(err.Error(), "no image footer") {
		t.Fatalf("a body with no footer: %v, want a refusal naming the version and the footer", err)
	}
}

// FuzzSplitReleaseBody: whatever a release body holds, its notes and footer
// together are the body with its line endings normalised, the footer is empty
// exactly when none was found, and a found footer opens with the rule and the
// Images heading ReleasePage writes.
func FuzzSplitReleaseBody(f *testing.F) {
	for _, seed := range []string{
		"", "First release.\n", "- An entry.\n\n---\n\n- Another.\n",
		syncNotes + "\n---\n\n### Images\n\n| Image | Digest |\n",
		"a\r\n\r\n---\r\n\r\n### Images\r\n", "\n---\n\n### Images\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		notes, footer, ok := dist.SplitReleaseBody(body)
		if notes+footer != strings.ReplaceAll(body, "\r\n", "\n") {
			t.Fatalf("notes %q + footer %q are not the body %q", notes, footer, body)
		}
		if ok != (footer != "") || (ok && !strings.HasPrefix(footer, "\n---\n\n### Images\n")) {
			t.Fatalf("footer %q, found %v", footer, ok)
		}
	})
}
