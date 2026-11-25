package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestRunArchiver_AForceCancelIsWhoItsRecordSays: an archived run a force
// cancel ended names someone as verified only where the record it carries
// verifies, whatever its body says; and says what that record says. Anything
// else is shown as asserted, and no read of it fails.
func TestRunArchiver_AForceCancelIsWhoItsRecordSays(t *testing.T) {
	ctx, keys, other := context.Background(), recordKeys(t), recordKeys(t)
	closedAt := time.UnixMilli(time.Now().UnixMilli()).UTC()
	enforce := func() string { return "enforce" }
	ended := func(t *testing.T, details *commonpb.Payloads, mode string) *orkstackv1.Run {
		t.Helper()
		e := terminated(t, details)
		e.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
		e.EventTime = timestamppb.New(closedAt)
		closed := ReadForceCancelClose(ctx, keys, mode, "payments", "ork-r", "exec-1", e, closedAt)
		if !closed.IsForceCancel() {
			t.Fatalf("the close reads %v", closed.Verification)
		}
		_, run := ForceCanceledTerminalEvent(&orkstackv1.Run{Id: "ork-r", Namespace: "payments", OrkflowName: "pay",
			CreatedAt: timestamppb.New(closedAt.Add(-time.Hour))}, closed, 9, closedAt)
		return run
	}
	detailsOf := func(t *testing.T, p *commonpb.Payloads) string {
		t.Helper()
		var d forceCancelDetails
		must.OK(t, DataConverter().FromPayload(p.GetPayloads()[0], &d))
		return `{"signed":"` + d.Signed + `"}`
	}
	record := sampleRecord()
	record.At = closedAt.Add(-time.Second)
	signed, err := SignForceCancelRecord(keys, record)
	must.OK(t, err)
	forged := record
	forged.Principal = "user:corp:mallory"
	byOther, err := SignForceCancelRecord(other, forged)
	must.OK(t, err)
	otherRun := record
	otherRun.RunID, otherRun.WorkflowID = "ork-other", ForceCancelWorkflowIDPrefix("payments", "ork-other")+"fc-1"
	elsewhere, err := SignForceCancelRecord(keys, otherRun)
	must.OK(t, err)
	var none *identity.Issuer

	for _, tt := range []struct {
		name     string
		run      *orkstackv1.Run
		change   func(*orkstackv1.Run)
		verifier ForceCancelVerifier
		mode     func() string
		by       string
		verified bool
	}{
		{"as it was archived", ended(t, signed, "enforce"), nil, keys, enforce, "user:corp:alice", true},
		{"a body that names someone else", ended(t, signed, "enforce"), func(r *orkstackv1.Run) {
			r.CanceledBy, r.Error.Message = "user:corp:bob", "force-canceled by user:corp:bob"
		}, keys, enforce, "user:corp:alice", true},
		{"without its record", ended(t, signed, "enforce"), func(r *orkstackv1.Run) { r.ForceCancelRecord = "" }, keys, enforce, "user:corp:alice", false},
		{"a record nobody here signed", ended(t, signed, "enforce"), func(r *orkstackv1.Run) {
			r.CanceledBy, r.ForceCancelRecord = "user:corp:mallory", detailsOf(t, byOther)
		}, keys, enforce, "user:corp:mallory", false},
		{"another run's record", ended(t, signed, "enforce"), func(r *orkstackv1.Run) { r.ForceCancelRecord = detailsOf(t, elsewhere) }, keys, enforce, "user:corp:alice", false},
		{"an ending of the run long after that one", ended(t, signed, "enforce"), func(r *orkstackv1.Run) { r.FinishedAt = timestamppb.New(closedAt.Add(time.Hour)) }, keys, enforce, "user:corp:alice", false},
		{"one long before it", ended(t, signed, "enforce"), func(r *orkstackv1.Run) { r.FinishedAt = timestamppb.New(closedAt.Add(-time.Hour)) }, keys, enforce, "user:corp:alice", false},
		{"no time of its ending", ended(t, unsignedRecord(t, record), "asserted"), func(r *orkstackv1.Run) { r.FinishedAt, r.CanceledBy = nil, "user:corp:bob" }, keys, func() string { return "asserted" }, "user:corp:bob", false},
		{"a run that did not end canceled", ended(t, signed, "enforce"), func(r *orkstackv1.Run) { r.Status = orkstackv1.Status_STATUS_FAILED }, keys, enforce, "user:corp:alice", false},
		{"a name that hides text, and no record", ended(t, signed, "enforce"), func(r *orkstackv1.Run) { r.ForceCancelRecord, r.CanceledBy = "", "user:\u202ebob" }, keys, enforce, "", false},
		{"a name of any length, and no record", ended(t, signed, "enforce"), func(r *orkstackv1.Run) { r.ForceCancelRecord, r.CanceledBy = "", strings.Repeat("a", 4096) }, keys, enforce, "", false},
		{"a record that is none", ended(t, signed, "enforce"), func(r *orkstackv1.Run) { r.ForceCancelRecord = "{" }, keys, enforce, "user:corp:alice", false},
		{"an unsigned record where callers are verified", ended(t, unsignedRecord(t, record), "asserted"), func(r *orkstackv1.Run) { r.CanceledByVerified = true }, keys, enforce, "user:corp:alice", false},
		{"an unsigned record in asserted mode", ended(t, unsignedRecord(t, record), "asserted"), func(r *orkstackv1.Run) { r.CanceledByVerified = true }, keys, func() string { return "asserted" }, "user:corp:alice", false},
		{"keys that cannot be read", ended(t, signed, "enforce"), nil, none, enforce, "user:corp:alice", false},
		{"nothing to verify with", ended(t, signed, "enforce"), nil, nil, nil, "user:corp:alice", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.run.GetForceCancelRecord() == "" {
				t.Fatal("the run carries no record")
			}
			if tt.change != nil {
				tt.change(tt.run)
			}
			store, err := artifacts.NewRunArchive(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
			must.OK(t, err)
			archive := NewRunArchiver(store)
			if tt.verifier != nil {
				archive = archive.WithForceCancelVerifier(tt.verifier, tt.mode)
			}
			must.OK(t, archive.Archive(ctx, tt.run, nil))
			loaded, err := archive.LoadRun(ctx, "ork-r")
			must.OK(t, err)
			whole, _, err := archive.Load(ctx, "ork-r")
			must.OK(t, err)
			listed, err := archive.ListRuns(ctx, ArchiveQuery{Namespace: "payments"})
			must.OK(t, err)
			if len(listed) != 1 || !proto.Equal(loaded, whole) || !proto.Equal(loaded, listed[0]) {
				t.Fatalf("the archive's reads differ: %v\n%v\n%v", loaded, whole, listed)
			}
			if loaded.GetCanceledBy() != tt.by || loaded.GetCanceledByVerified() != tt.verified || loaded.GetError().GetCode() != RunForceCanceledCode {
				t.Errorf("canceled by %q, verified %v, %v; want %q, %v", loaded.GetCanceledBy(), loaded.GetCanceledByVerified(), loaded.GetError(), tt.by, tt.verified)
			}
			if m := loaded.GetError().GetMessage(); !strings.Contains(m, tt.by) || strings.Contains(m, "(asserted)") == tt.verified {
				t.Errorf("message %q", m)
			}
			if loaded.GetForceCancelRecord() != "" {
				t.Error("a read of the archive answers the record itself, which says more than who")
			}
			// What is stored stays as it was written: the evidence bundle's.
			body, err := archive.LoadRunBody(ctx, "ork-r")
			must.OK(t, err)
			raw, err := UnmarshalRunLine(body)
			must.OK(t, err)
			if !proto.Equal(raw, tt.run) {
				t.Errorf("the stored body changed: %v", raw)
			}
		})
	}
	t.Run("no other ending names anyone or answers a record", func(t *testing.T) {
		for _, code := range []string{"RUN_TERMINATED", RunForceCancelUnverifiedCode, "TASK_FAILED"} {
			store, err := artifacts.NewRunArchive(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
			must.OK(t, err)
			run := ended(t, signed, "enforce")
			run.Error = &orkstackv1.Error{Code: code}
			archive := NewRunArchiver(store).WithForceCancelVerifier(keys, enforce)
			must.OK(t, archive.Archive(ctx, run, nil))
			got, err := archive.LoadRun(ctx, "ork-r")
			must.OK(t, err)
			if got.GetCanceledBy() != "" || got.GetCanceledByVerified() || got.GetForceCancelRecord() != "" || got.GetError().GetCode() != code {
				t.Errorf("%s reads %v", code, got)
			}
		}
	})
	t.Run("an ordinary cancel is left alone", func(t *testing.T) {
		store, err := artifacts.NewRunArchive(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
		must.OK(t, err)
		run := &orkstackv1.Run{Id: "ork-r", Namespace: "payments", OrkflowName: "pay", Status: orkstackv1.Status_STATUS_CANCELED, CanceledBy: "user:corp:bob", CanceledByVerified: true,
			Error: &orkstackv1.Error{Code: "RUN_CANCELED", Message: "canceled"}, CreatedAt: timestamppb.New(closedAt)}
		archive := NewRunArchiver(store).WithForceCancelVerifier(keys, enforce)
		must.OK(t, archive.Archive(ctx, run, nil))
		if got, err := archive.LoadRun(ctx, "ork-r"); err != nil || !proto.Equal(got, run) {
			t.Errorf("loaded %v, %v", got, err)
		}
	})
}

// FuzzArchivedForceCancel: whatever record an archived run carries, showing it
// never panics, never changes that the run was force-canceled, and names no
// one as verified but whom a record that verifies does.
func FuzzArchivedForceCancel(f *testing.F) {
	keys := recordKeys(f)
	signed, err := SignForceCancelRecord(keys, sampleRecord())
	must.OK(f, err)
	f.Add(string(signed.GetPayloads()[0].GetData()), "enforce")
	unsigned, err := UnsignedForceCancelRecord(sampleRecord())
	must.OK(f, err)
	f.Add(string(unsigned.GetPayloads()[0].GetData()), "asserted")
	f.Add("{", "")
	f.Fuzz(func(t *testing.T, record, mode string) {
		run := &orkstackv1.Run{Id: "ork-r", Namespace: "payments", CanceledBy: "user:corp:bob", CanceledByVerified: true, ForceCancelRecord: record,
			FinishedAt: timestamppb.Now(), Error: &orkstackv1.Error{Code: RunForceCanceledCode}}
		showForceCancel(context.Background(), keys, mode, run)
		if run.GetError().GetCode() != RunForceCanceledCode || run.GetCanceledByVerified() && run.GetCanceledBy() != "user:corp:alice" {
			t.Fatalf("%q reads %v", record, run)
		}
	})
}

// TestWithoutItems_CarriesNoForceCancelRecord: what a list page, a webhook and
// a run's own result answer says who, never the record behind it.
func TestWithoutItems_CarriesNoForceCancelRecord(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-r", CanceledBy: "user:corp:alice", CanceledByVerified: true, ForceCancelRecord: "{}"}
	got := WithoutItems(run)
	if got.GetForceCancelRecord() != "" || got.GetCanceledBy() != "user:corp:alice" || !got.GetCanceledByVerified() || run.GetForceCancelRecord() == "" {
		t.Fatalf("%v of %v", got, run)
	}
}
