package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordedEvidence is an evidence fixture whose engine keeps identity records:
// a run whose await gate was rejected in time and never applied (the run
// timed it out), and whose composed task ship succeeded with no record.
type recordedEvidence struct {
	*evidenceFixture
	records *faultyRecords
	run     *orkstackv1.Run
}

const recordedEvidenceRun = "ork-recorded"

func newRecordedEvidence(t *testing.T) *recordedEvidence {
	t.Helper()
	store, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	records := &faultyRecords{RecordStore: store}
	fast := &artifacts.RecordSpacings{MissConfirm: time.Millisecond, ReaskInitial: time.Millisecond, ReaskMax: 2 * time.Millisecond, AttemptMargin: time.Millisecond}
	f := newEvidenceFixture(t, withConfig(func(c *api.Config) {
		c.RecordedResults, c.AwaitSpacings = records, fast
	}))
	sha := strings.Repeat("a", 64)
	// The store stamps a record with its own clock, so this run's instants
	// are on that clock too: the answer is stored before its deadline and the
	// run's end.
	now := time.Now().UTC()
	deadline := now.Add(time.Hour)
	run := &orkstackv1.Run{Id: recordedEvidenceRun, Namespace: "default", Status: orkstackv1.Status_STATUS_FAILED, TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "gate", Status: orkstackv1.Status_STATUS_FAILED, Await: &orkstackv1.TaskAwait{Deadline: timestamppb.New(deadline)},
			Error: &orkstackv1.Error{Code: "AWAIT_TIMED_OUT"}},
		{TaskId: "ship", Status: orkstackv1.Status_STATUS_SUCCEEDED, Child: &orkstackv1.TaskChild{RunId: "ork-child"}},
	}}
	f.reader.setStart(recordedEvidenceRun, &engine.RunStartRecord{Plan: engine.RunPlan{Namespace: "default",
		Identity: &engine.IdentityPlan{AdmissionSHA256: sha}}})
	ctx := context.Background()
	detail := &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "default", RunId: recordedEvidenceRun, TaskId: "gate",
		Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice", Reason: "not today"}
	detailSHA, err := store.WriteResolutionDetail(ctx, detail)
	must.OK(t, err)
	_, err = store.WriteRecordedValues(ctx, &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
		Namespace: "default", RunId: recordedEvidenceRun, TaskId: "gate", AdmissionSha256: sha, Rejected: true, DetailSha256: detailSHA,
		DefinitionDigest: "sha256:" + strings.Repeat("d", 64), InputBinding: make([]byte, 32),
		Recorded: &orkstackv1.CommitRecording{AdmissionSha256: sha, Time: timestamppb.New(time.Now())}})
	must.OK(t, err)
	run.OrkflowName, run.OrkflowVersion, run.OrkflowDefinitionDigest = "evidence", "0.1.0", f.digest
	run.CreatedAt, run.FinishedAt = timestamppb.New(now.Add(-time.Minute)), timestamppb.New(now.Add(2*time.Hour))
	f.reader.setRun(run)
	must.OK(t, f.archiver.Archive(ctx, run, []*orkstackv1.Event{{RunId: run.GetId(), Sequence: 1,
		Type: orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, Status: orkstackv1.Status_STATUS_PENDING, Timestamp: run.GetCreatedAt()}}))
	return &recordedEvidence{evidenceFixture: f, records: records, run: run}
}

// TestExportRunEvidence_HoldsTheRunsRecords: a bundle of a run whose results
// the engine records holds every record found under its keys, an answer the
// run never applied among them, with what became of it and its detail; a
// record the run's state says should be there and is not is a missing member,
// which the verifier states as a gap.
func TestExportRunEvidence_HoldsTheRunsRecords(t *testing.T) {
	f := newRecordedEvidence(t)
	got := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: recordedEvidenceRun})
	resolution := memberAt(t, got.manifest, "tasks/gate/records/resolution.json")
	if resolution.State != evidence.StatePresent || resolution.Kind != evidence.KindResolution || resolution.RecordState != evidence.RecordNotApplied {
		t.Fatalf("resolution member = %+v; want present and not applied", resolution)
	}
	detail := memberAt(t, got.manifest, "tasks/gate/records/resolution-detail.json")
	if detail.State != evidence.StatePresent || detail.Kind != evidence.KindResolutionDetail {
		t.Fatalf("detail member = %+v", detail)
	}
	if !strings.Contains(string(tarMembers(t, got.data)["tasks/gate/records/resolution-detail.json"]), "alice") {
		t.Fatal("the detail's bytes are not the stored detail")
	}
	composed := memberAt(t, got.manifest, "tasks/ship/records/composed.json")
	if composed.State != evidence.StateMissing || composed.Reason != evidence.ReasonRecordMissing {
		t.Fatalf("composed member = %+v; want missing", composed)
	}
	if got.manifest.Counts.Missing != 1 {
		t.Fatalf("counts = %+v", got.manifest.Counts)
	}
	report := verifyBundle(t, got.data)
	if len(report.Missing) != 1 || report.Missing[0].Path != composed.Path {
		t.Fatalf("verify missing = %+v", report.Missing)
	}

	t.Run("one task's", func(t *testing.T) {
		got := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: recordedEvidenceRun, TaskId: "ship"})
		if hasMember(got.manifest, "tasks/gate/records/resolution.json") || !hasMember(got.manifest, "tasks/ship/records/composed.json") {
			t.Fatalf("members = %v", memberPaths(got.manifest))
		}
	})
}

// TestExportRunEvidence_ARecordThatCannotBeReadFailsTheExport: a record the
// store does not answer for has no state, so the export fails unavailable,
// never a bundle that calls it missing.
func TestExportRunEvidence_ARecordThatCannotBeReadFailsTheExport(t *testing.T) {
	f := newRecordedEvidence(t)
	f.records.readErr = &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("store away")}
	if got := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: recordedEvidenceRun}); got.code() != connect.CodeUnavailable {
		t.Fatalf("export = %v; want unavailable", got.err)
	}
	f.records.readErr = nil
	f.records.listErr = &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredTimeout, Err: errors.New("slow")}
	if got := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: recordedEvidenceRun}); got.code() != connect.CodeUnavailable {
		t.Fatalf("export = %v; want unavailable", got.err)
	}
}

// TestExportRunEvidence_ARecordListedAndThenGoneFailsTheExport: the store
// listed a record and then said it is not there: an answer in flux, never a
// bundle without it and never a gap.
func TestExportRunEvidence_ARecordListedAndThenGoneFailsTheExport(t *testing.T) {
	f := newRecordedEvidence(t)
	f.records.none = true
	if got := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: recordedEvidenceRun}); got.code() != connect.CodeUnavailable {
		t.Fatalf("export = %v; want unavailable", got.err)
	}
}

// TestExportRunEvidence_APlainRunReadsNoRecords: a run of container tasks
// whose results name no record is exported as before, from the archive, with
// no read of the cluster or the records, even where the engine keeps records.
func TestExportRunEvidence_APlainRunReadsNoRecords(t *testing.T) {
	f := newRecordedEvidence(t)
	f.records.listErr = errors.New("the records must not be listed for a plain run")
	plain := f.succeeded("ork-plain")
	got := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: plain.run.GetId()})
	for _, p := range memberPaths(got.manifest) {
		if strings.Contains(p, "/records/") {
			t.Fatalf("a plain run's bundle holds %s", p)
		}
	}
}
