package evidence

import (
	"bytes"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// gappedFixture is the fixture bundle of a run that lost events 1201–2400
// before it was archived, as an earlier engine wrote it: schema_version 2.
func gappedFixture() fixtureBundle {
	f := newFixture()
	f.manifest.SchemaVersion = SchemaVersionEventGaps
	f.manifest.EventGaps = []EventGap{{First: 1201, Last: 2400, Reason: "segment object missing"}}
	return f
}

func TestParseManifest_AcceptsSchemaTwoOnlyWithEventGaps(t *testing.T) {
	f := gappedFixture()
	document, err := f.manifest.MarshalJSON()
	must.OK(t, err)
	parsed, err := ParseManifest(document)
	must.OKf(t, err, "ParseManifest(schema 2 with gaps)")
	if len(parsed.EventGaps) != 1 || parsed.EventGaps[0].First != 1201 {
		t.Fatalf("parsed gaps = %+v", parsed.EventGaps)
	}
	f.manifest.EventGaps = nil
	empty, _ := f.manifest.MarshalJSON()
	if _, err := ParseManifest(empty); err == nil || !strings.Contains(err.Error(), "names no event_gaps") {
		t.Fatalf("schema 2 without gaps = %v, want a refusal", err)
	}
	if _, err := ParseManifest(bytes.Replace(document, []byte(`"schema_version": 2`), []byte(`"schema_version": 4`), 1)); err == nil ||
		!strings.Contains(err.Error(), "schema_version 4") {
		t.Fatalf("schema 4 = %v, want a refusal naming it", err)
	}
	backwards := bytes.Replace(document, []byte(`"first": 1201`), []byte(`"first": 2401`), 1)
	if _, err := ParseManifest(backwards); err == nil {
		t.Fatal("a gap whose first is after its last was accepted")
	}
}

func TestParseManifest_SchemaOneForbidsEventGaps(t *testing.T) {
	f := gappedFixture()
	f.manifest.SchemaVersion = SchemaVersionNoGaps
	document, err := f.manifest.MarshalJSON()
	must.OK(t, err)
	if _, err := ParseManifest(document); err == nil || !strings.Contains(err.Error(), "carries event_gaps") {
		t.Fatalf("schema 1 with gaps = %v, want a refusal", err)
	}
}

// Version 3 is every bundle an engine writes: with or without event gaps.
func TestParseManifest_SchemaThreeMayNameEventGaps(t *testing.T) {
	for _, f := range []fixtureBundle{newFixture(), gappedFixture()} {
		f.manifest.SchemaVersion = SchemaVersion
		document, err := f.manifest.MarshalJSON()
		must.OK(t, err)
		parsed, err := ParseManifest(document)
		must.OKf(t, err, "ParseManifest(schema 3, %d gaps)", len(f.manifest.EventGaps))
		if parsed.SchemaVersion != 3 || len(parsed.EventGaps) != len(f.manifest.EventGaps) {
			t.Fatalf("parsed schema %d, %d gaps", parsed.SchemaVersion, len(parsed.EventGaps))
		}
		report, err := Verify(bytes.NewReader(writeFixture(t, f)))
		must.OKf(t, err, "Verify")
		if !report.OK() || len(report.EventGaps) != len(f.manifest.EventGaps) {
			t.Fatalf("report = %+v", report)
		}
	}
}

// A bundle an earlier engine wrote is still read: version 1 without gaps.
func TestVerify_ReadsAVersionOneBundle(t *testing.T) {
	f := newFixture()
	f.manifest.SchemaVersion = SchemaVersionNoGaps
	report, err := Verify(bytes.NewReader(writeFixture(t, f)))
	must.OKf(t, err, "Verify")
	if !report.OK() || report.Manifest.SchemaVersion != 1 {
		t.Fatalf("report = %+v", report)
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
