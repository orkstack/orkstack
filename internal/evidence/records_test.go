package evidence

import (
	"bytes"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// recordsFixture is the golden fixture with the records of a run whose results
// the engine records: an approval recorded in time and never applied, with its
// detail; a composed task's record the store says is not there while the run
// says the task succeeded (the gap); and outputs reclaimed with the run.
func recordsFixture() fixtureBundle {
	f := newFixture()
	detail := []byte(`{"schemaVersion":1,"namespace":"payments","runId":"ork-20260910-1","taskId":"approve","principal":"alice"}`)
	resolution := []byte(`{"schemaVersion":1,"kind":"RECORDED_VALUES_KIND_RESOLUTION","taskId":"approve","detailSha256":"` + digestOf(detail) + `"}`)
	f.bytes["tasks/approve/records/resolution.json"] = resolution
	f.bytes["tasks/approve/records/resolution-detail.json"] = detail
	attested := "metadata/attested/payments/ork-20260910-1/"
	approval := presentMember("tasks/approve/records/resolution.json", KindResolution, "approve", 0, "", attested+"approve/resolution.json", resolution)
	approval.RecordState = RecordNotApplied
	f.manifest.Members = append(f.manifest.Members,
		approval,
		presentMember("tasks/approve/records/resolution-detail.json", KindResolutionDetail, "approve", 0, "", "metadata/details/approve", detail),
		Member{Path: "tasks/ship/records/composed.json", Kind: KindComposed, TaskID: "ship", Origin: attested + "ship/composed.json",
			State: StateMissing, Reason: ReasonRecordMissing},
		Member{Path: "records/outputs.json", Kind: KindOutputs, Origin: attested + "outputs.json",
			State: StateReclaimed, Reason: ReasonRecordReclaimed},
	)
	f.manifest.Counts = CountMembers(f.manifest.Members)
	return f
}

// TestVerify_Records: a record not applied is checked as any member is, its
// detail is the one it names, and a record the store does not hold while the
// run says its task succeeded is a gap the report names, never a pass.
func TestVerify_Records(t *testing.T) {
	f := recordsFixture()
	if f.manifest.Counts.Missing != 1 {
		t.Fatalf("counts = %+v, want one missing", f.manifest.Counts)
	}
	report, err := Verify(bytes.NewReader(writeFixture(t, f)))
	must.OKf(t, err, "Verify")
	if !report.OK() {
		t.Fatalf("failures: %+v", report.Failures)
	}
	if len(report.Missing) != 1 || report.Missing[0].Path != "tasks/ship/records/composed.json" {
		t.Fatalf("missing = %+v, want the composed record", report.Missing)
	}
	verified := map[string]bool{}
	for _, m := range report.Verified {
		verified[m.Path] = true
	}
	if !verified["tasks/approve/records/resolution.json"] || !verified["tasks/approve/records/resolution-detail.json"] {
		t.Fatalf("verified = %v, want the resolution and its detail", verified)
	}

	t.Run("a detail other than the one its record names", func(t *testing.T) {
		f := recordsFixture()
		other := []byte(`{"schemaVersion":1,"taskId":"approve","principal":"mallory"}`)
		f.bytes["tasks/approve/records/resolution-detail.json"] = other
		for i, m := range f.manifest.Members {
			if m.Kind == KindResolutionDetail {
				f.manifest.Members[i] = presentMember(m.Path, m.Kind, m.TaskID, 0, "", m.Origin, other)
			}
		}
		report, err := Verify(bytes.NewReader(writeFixture(t, f)))
		must.OKf(t, err, "Verify")
		if len(report.Failures) != 1 || report.Failures[0].Reason != FailureDetailMismatch ||
			report.Failures[0].Path != "tasks/approve/records/resolution-detail.json" {
			t.Fatalf("failures = %+v, want the detail named", report.Failures)
		}
	})
	t.Run("a resolution with no detail", func(t *testing.T) {
		f := recordsFixture()
		var kept []Member
		for _, m := range f.manifest.Members {
			if m.Kind != KindResolutionDetail {
				kept = append(kept, m)
			}
		}
		f.manifest.Members = kept
		report, err := Verify(bytes.NewReader(writeFixture(t, f)))
		must.OKf(t, err, "Verify")
		if len(report.Failures) != 1 || report.Failures[0].Reason != FailureDetailMismatch ||
			report.Failures[0].Path != "tasks/approve/records/resolution-detail.json" {
			t.Fatalf("failures = %+v, want the absent detail named", report.Failures)
		}
	})
	t.Run("a resolution whose bytes name no detail", func(t *testing.T) {
		f := recordsFixture()
		broken := []byte(`{"schemaVersion":1,"taskId":"approve"}`)
		f.bytes["tasks/approve/records/resolution.json"] = broken
		for i, m := range f.manifest.Members {
			if m.Kind == KindResolution {
				f.manifest.Members[i] = presentMember(m.Path, m.Kind, m.TaskID, 0, "", m.Origin, broken)
				f.manifest.Members[i].RecordState = RecordNotApplied
			}
		}
		report, err := Verify(bytes.NewReader(writeFixture(t, f)))
		must.OKf(t, err, "Verify")
		if len(report.Failures) != 1 || report.Failures[0].Reason != FailureDetailMismatch {
			t.Fatalf("failures = %+v, want the detail mismatch", report.Failures)
		}
	})
}

// TestVerify_AResolutionTooLargeToCheckFails: a resolution over the bound the
// engine writes is hashed but cannot have its detail checked, so it fails
// rather than pass unchecked.
func TestVerify_AResolutionTooLargeToCheckFails(t *testing.T) {
	f := recordsFixture()
	big := append([]byte(`{"detailSha256":"x","pad":"`), bytes.Repeat([]byte("p"), maxResolutionMember)...)
	big = append(big, '"', '}')
	f.bytes["tasks/approve/records/resolution.json"] = big
	for i, m := range f.manifest.Members {
		if m.Kind == KindResolution {
			f.manifest.Members[i] = presentMember(m.Path, m.Kind, m.TaskID, 0, "", m.Origin, big)
		}
	}
	report, err := Verify(bytes.NewReader(writeFixture(t, f)))
	must.OKf(t, err, "Verify")
	if len(report.Failures) != 1 || report.Failures[0].Reason != FailureDetailMismatch {
		t.Fatalf("failures = %+v", report.Failures)
	}
}
