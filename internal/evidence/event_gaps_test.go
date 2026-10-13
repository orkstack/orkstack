package evidence

import (
	"bytes"
	"strings"
	"testing"
)

// gappedFixture is the fixture bundle of a run that lost events 1201–2400
// before it was archived: schema_version 2 with event_gaps.
func gappedFixture() fixtureBundle {
	f := newFixture()
	f.manifest.SchemaVersion = SchemaVersionEventGaps
	f.manifest.EventGaps = []EventGap{{First: 1201, Last: 2400, Reason: "segment object missing"}}
	return f
}

func TestParseManifest_AcceptsSchemaTwoOnlyWithEventGaps(t *testing.T) {
	f := gappedFixture()
	document, err := f.manifest.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseManifest(document)
	if err != nil {
		t.Fatalf("ParseManifest(schema 2 with gaps): %v", err)
	}
	if len(parsed.EventGaps) != 1 || parsed.EventGaps[0].First != 1201 {
		t.Fatalf("parsed gaps = %+v", parsed.EventGaps)
	}
	f.manifest.EventGaps = nil
	empty, _ := f.manifest.MarshalJSON()
	if _, err := ParseManifest(empty); err == nil || !strings.Contains(err.Error(), "names no event_gaps") {
		t.Fatalf("schema 2 without gaps = %v, want a refusal", err)
	}
	if _, err := ParseManifest(bytes.Replace(document, []byte(`"schema_version": 2`), []byte(`"schema_version": 3`), 1)); err == nil ||
		!strings.Contains(err.Error(), "schema_version 3") {
		t.Fatalf("schema 3 = %v, want a refusal naming it", err)
	}
	backwards := bytes.Replace(document, []byte(`"first": 1201`), []byte(`"first": 2401`), 1)
	if _, err := ParseManifest(backwards); err == nil {
		t.Fatal("a gap whose first is after its last was accepted")
	}
}

func TestParseManifest_SchemaOneForbidsEventGaps(t *testing.T) {
	f := gappedFixture()
	f.manifest.SchemaVersion = SchemaVersion
	document, err := f.manifest.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseManifest(document); err == nil || !strings.Contains(err.Error(), "carries event_gaps") {
		t.Fatalf("schema 1 with gaps = %v, want a refusal", err)
	}
}

func TestVerify_ReportsEventGapsAsANotice(t *testing.T) {
	report, err := Verify(bytes.NewReader(writeFixture(t, gappedFixture())))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("a gapped bundle failed verification: %+v", report.Failures)
	}
	if len(report.EventGaps) != 1 || report.EventGaps[0].Sentence() !=
		"events 1201–2400 of this run were lost from object storage before it was archived (segment object missing)" {
		t.Fatalf("report gaps = %+v", report.EventGaps)
	}
}
