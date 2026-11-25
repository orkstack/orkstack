package evidence

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// gappedFixture is the fixture bundle of a run that lost events 1201–2400
// before it was archived.
func gappedFixture() fixtureBundle {
	f := newFixture()
	f.manifest.EventGaps = []EventGap{{First: 1201, Last: 2400, Reason: "segment object missing"}}
	return f
}

// A bundle may name event gaps or none; a gap is a range.
func TestParseManifest_EventGaps(t *testing.T) {
	for _, f := range []fixtureBundle{newFixture(), gappedFixture()} {
		document, err := f.manifest.MarshalJSON()
		must.OK(t, err)
		parsed, err := ParseManifest(document)
		must.OKf(t, err, "ParseManifest(%d gaps)", len(f.manifest.EventGaps))
		if parsed.SchemaVersion != 3 || len(parsed.EventGaps) != len(f.manifest.EventGaps) {
			t.Fatalf("parsed schema %d, %d gaps", parsed.SchemaVersion, len(parsed.EventGaps))
		}
	}
	document, err := gappedFixture().manifest.MarshalJSON()
	must.OK(t, err)
	backwards := bytes.Replace(document, []byte(`"first": 1201`), []byte(`"first": 2401`), 1)
	if _, err := ParseManifest(backwards); err == nil {
		t.Fatal("a gap whose first is after its last was accepted")
	}
}

// Only the version an engine writes is read: a bundle of another, earlier or
// later, with gaps or without, is refused by name with what does read it.
func TestVerify_RefusesEveryOtherSchemaVersion(t *testing.T) {
	for _, version := range []int{0, 1, 2, 4} {
		for _, f := range []fixtureBundle{newFixture(), gappedFixture()} {
			f.manifest.SchemaVersion = version
			report, err := Verify(bytes.NewReader(writeFixture(t, f)))
			if err == nil || report != nil {
				t.Fatalf("version %d with %d gaps was read: %+v", version, len(f.manifest.EventGaps), report)
			}
			for _, want := range []string{fmt.Sprintf("schema_version %d", version), "reads only schema_version 3",
				"verify it with the orkctl that exported it"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("version %d: %q does not say %q", version, err, want)
				}
			}
		}
	}
}

func TestVerify_ReportsEventGapsAsANotice(t *testing.T) {
	report, err := Verify(bytes.NewReader(writeFixture(t, gappedFixture())))
	must.OKf(t, err, "Verify")
	if !report.OK() {
		t.Fatalf("a gapped bundle failed verification: %+v", report.Failures)
	}
	if len(report.EventGaps) != 1 || report.EventGaps[0].Sentence() !=
		"events 1201–2400 of this run were lost from object storage before it was archived (segment object missing)" {
		t.Fatalf("report gaps = %+v", report.EventGaps)
	}
}

// TestEventGapSentence_NamesTheCause: a gap's notice names its cause — never
// published for the engine's unpublished reason, lost from object storage
// for every other — so a gap the run never published is never said to be
// lost.
func TestEventGapSentence_NamesTheCause(t *testing.T) {
	for _, tc := range []struct {
		gap  EventGap
		want string
	}{
		{EventGap{First: 1, Last: 1, Reason: UnpublishedEventsReason},
			"events 1–1 of this run were never published (the run's history holds no event under these numbers)"},
		{EventGap{First: 1201, Last: 2400, Reason: "segment object missing"},
			"events 1201–2400 of this run were lost from object storage before it was archived (segment object missing)"},
		{EventGap{First: 3, Last: 4, Reason: "the run's history holds no event under these numbers, and more"},
			"events 3–4 of this run were lost from object storage before it was archived " +
				"(the run's history holds no event under these numbers, and more)"},
	} {
		if got := tc.gap.Sentence(); got != tc.want {
			t.Errorf("Sentence(%+v) = %q, want %q", tc.gap, got, tc.want)
		}
	}
}
