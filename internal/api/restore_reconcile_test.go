package api_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// recordingReconciler is the API's RestoreReconciler seam: it records the
// page asked for and answers a fixed result. The reconcile itself is
// archive.ReconcileRestore's, tested there.
type recordingReconciler struct {
	calls  []archive.RestoreRequest
	result archive.RestoreResult
	err    error
}

func (r *recordingReconciler) ReconcileRestore(_ context.Context, req archive.RestoreRequest) (archive.RestoreResult, error) {
	r.calls = append(r.calls, req)
	return r.result, r.err
}

func reconcileRequest(at time.Time, dryRun bool) *connect.Request[orkstackv1.ReconcileRestoreRequest] {
	return connect.NewRequest(&orkstackv1.ReconcileRestoreRequest{TemporalRestoredAt: timestamppb.New(at), DryRun: dryRun})
}

// A request is checked before anything is read: the instant is required and
// not in the future, the page within its bound, the cursor one a page gave.
func TestReconcileRestore_RefusesABadRequest(t *testing.T) {
	rec := &recordingReconciler{}
	svc := api.NewService(api.Config{Reader: &fakeReader{}, RestoreReconciler: rec})
	past := time.Now().Add(-time.Hour)
	for name, msg := range map[string]*orkstackv1.ReconcileRestoreRequest{
		"no instant":   {DryRun: true},
		"future":       {TemporalRestoredAt: timestamppb.New(time.Now().Add(time.Hour)), DryRun: true},
		"limit high":   {TemporalRestoredAt: timestamppb.New(past), DryRun: true, Limit: archive.MaxRestorePage + 1},
		"limit < 0":    {TemporalRestoredAt: timestamppb.New(past), DryRun: true, Limit: -1},
		"stray cursor": {TemporalRestoredAt: timestamppb.New(past), DryRun: true, Cursor: "metadata/runs/ork-1"},
	} {
		_, err := svc.ReconcileRestore(context.Background(), connect.NewRequest(msg))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: err = %v; want InvalidArgument", name, err)
		}
	}
	if len(rec.calls) != 0 {
		t.Fatalf("a refused request reached the reconcile: %+v", rec.calls)
	}
}

// While this engine's retention cleanup is on, a reconcile that deletes is
// refused, because cleanup would race it; a dry run is still answered.
func TestReconcileRestore_RefusesToDeleteWhileCleanupIsOn(t *testing.T) {
	rec := &recordingReconciler{}
	svc := api.NewService(api.Config{Reader: &fakeReader{}, RestoreReconciler: rec, RetentionEnforced: true})
	at := time.Now().Add(-time.Hour)
	_, err := svc.ReconcileRestore(context.Background(), reconcileRequest(at, false))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("err = %v; want FailedPrecondition", err)
	}
	if len(rec.calls) != 0 {
		t.Fatal("the refused reconcile reached the store")
	}
	_, err = svc.ReconcileRestore(context.Background(), reconcileRequest(at, true))
	must.OKf(t, err, "dry run with cleanup on")
	if len(rec.calls) != 1 || !rec.calls[0].DryRun {
		t.Fatalf("calls = %+v; want one dry run", rec.calls)
	}
}

// With cleanup off, the page asked for reaches the reconcile, 0 is the
// largest page, and the answer carries every row.
func TestReconcileRestore_PassesThePageAndAnswersEveryRow(t *testing.T) {
	retired := time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second)
	rec := &recordingReconciler{result: archive.RestoreResult{
		Retired: []archive.RestoreRetired{
			{RunID: "ork-a", RetiredAt: retired, Archived: true, Deleted: true},
			{RunID: "ork-b", RetiredAt: retired, Archived: true, Open: true},
		},
		Finished: []string{"ork-c"}, FinishedTruncated: true, Listed: 7, Next: "metadata/cleanup/runs/ork-b.json",
	}}
	svc := api.NewService(api.Config{Reader: &fakeReader{}, RestoreReconciler: rec})
	at := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	resp, err := svc.ReconcileRestore(context.Background(), connect.NewRequest(&orkstackv1.ReconcileRestoreRequest{
		TemporalRestoredAt: timestamppb.New(at), Cursor: "metadata/cleanup/runs/ork-0.json",
	}))
	must.OKf(t, err, "ReconcileRestore")
	if c := rec.calls[0]; !c.RestoredAt.Equal(at) || c.DryRun || c.Limit != archive.MaxRestorePage || c.After != "metadata/cleanup/runs/ork-0.json" {
		t.Fatalf("request = %+v", c)
	}
	m := resp.Msg
	if len(m.GetRetired()) != 2 || !m.GetRetired()[0].GetDeleted() || !m.GetRetired()[1].GetOpen() ||
		!m.GetRetired()[0].GetRetiredAt().AsTime().Equal(retired) {
		t.Fatalf("retired = %+v", m.GetRetired())
	}
	if len(m.GetFinishedRunIds()) != 1 || !m.GetFinishedTruncated() || m.GetListed() != 7 || m.GetNextCursor() != "metadata/cleanup/runs/ork-b.json" {
		t.Fatalf("response = %+v", m)
	}
}

// An engine with no reconciler, and a reconcile that cannot read, each answer
// explicitly.
func TestReconcileRestore_AnswersWhenItCannotReconcile(t *testing.T) {
	at := time.Now().Add(-time.Hour)
	svc := api.NewService(api.Config{Reader: &fakeReader{}})
	if _, err := svc.ReconcileRestore(context.Background(), reconcileRequest(at, true)); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("no reconciler: err = %v; want FailedPrecondition", err)
	}
	svc = api.NewService(api.Config{Reader: &fakeReader{}, RestoreReconciler: &recordingReconciler{err: errors.New("store down")}})
	if _, err := svc.ReconcileRestore(context.Background(), reconcileRequest(at, true)); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("failing reconcile: err = %v; want Unavailable", err)
	}
}
