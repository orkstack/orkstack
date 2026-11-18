package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// restoreCluster is the cluster after a restore: fakeCluster's lookup and
// delete, and the runs the restored cluster holds open. A unit test cannot
// restore a real cluster to an instant on demand.
type restoreCluster struct {
	*fakeCluster
	live    []string
	openErr error
	asked   int
}

func (c *restoreCluster) OpenRunIDs(_ context.Context, limit int) ([]string, bool, error) {
	c.asked = limit
	if c.openErr != nil {
		return nil, false, c.openErr
	}
	if len(c.live) > limit {
		return c.live[:limit], true, nil
	}
	return c.live, false, nil
}

// restoreWorld is a real local run archive and a restored cluster, with T_t
// an hour ago.
type restoreWorld struct {
	store    *artifacts.LocalRunArchive
	root     string
	cluster  *restoreCluster
	restored time.Time
}

func newRestoreWorld(t *testing.T) *restoreWorld {
	t.Helper()
	root := t.TempDir()
	store, err := artifacts.NewLocalRunArchive(root)
	must.OK(t, err)
	return &restoreWorld{store: store, root: root, cluster: &restoreCluster{fakeCluster: newFakeCluster()}, restored: time.Now().Add(-time.Hour)}
}

// written sets the store's write time of runID's cleanup record.
func (w *restoreWorld) written(t *testing.T, runID string, at time.Time) {
	t.Helper()
	key, err := artifacts.RunCleanupKey(runID)
	must.OK(t, err)
	must.OK(t, os.Chtimes(filepath.Join(w.root, filepath.FromSlash(key)), at, at))
}

// archived writes runID's archive body.
func (w *restoreWorld) archived(t *testing.T, runID string) {
	t.Helper()
	must.OK(t, w.store.PutRun(context.Background(), artifacts.RunRecord{
		RunID: runID, Namespace: "default", Orkflow: "nightly", Status: "succeeded",
		Time: w.restored.Add(-48 * time.Hour), JSONL: []byte(`{"run_id":"` + runID + `"}` + "\n"),
	}))
}

// retired writes runID's cleanup record, retired at at.
func (w *restoreWorld) retired(t *testing.T, runID string, at time.Time) {
	t.Helper()
	must.OK(t, w.store.MarkArtifactsCleaned(context.Background(), runID, at))
}

func (w *restoreWorld) reconcile(t *testing.T, req RestoreRequest) RestoreResult {
	t.Helper()
	if req.RestoredAt.IsZero() {
		req.RestoredAt = w.restored
	}
	if req.Limit == 0 {
		req.Limit = MaxRestorePage
	}
	res, err := ReconcileRestore(context.Background(), w.store, w.cluster, "ork-main", req)
	must.OK(t, err)
	return res
}

func retiredIDs(res RestoreResult) []string {
	var out []string
	for _, r := range res.Retired {
		out = append(out, r.RunID)
	}
	return out
}

// Only a run cleanup retired after T_t, and whose execution the restored
// cluster holds, is listed; the rest are left alone.
func TestReconcileRestore_ListsOnlyRunsRetiredAfterTheRestoreWithAnExecution(t *testing.T) {
	w := newRestoreWorld(t)
	for _, id := range []string{"ork-after", "ork-before", "ork-gone"} {
		w.archived(t, id)
	}
	w.retired(t, "ork-after", w.restored.Add(30*time.Minute))
	w.retired(t, "ork-before", w.restored.Add(-30*time.Minute))
	w.retired(t, "ork-gone", w.restored.Add(30*time.Minute))
	w.cluster.found["ork-after"] = true
	w.cluster.found["ork-before"] = true
	w.cluster.closeTime["ork-after"] = w.restored
	w.cluster.closeTime["ork-before"] = w.restored

	res := w.reconcile(t, RestoreRequest{DryRun: true})
	if got := retiredIDs(res); !slices.Equal(got, []string{"ork-after"}) {
		t.Fatalf("retired = %v; want [ork-after]", got)
	}
	if r := res.Retired[0]; !r.Archived || r.Deleted || !r.RetiredAt.Equal(w.restored.Add(30*time.Minute)) {
		t.Fatalf("row = %+v; want archived, not deleted, retired 30m after the restore", r)
	}
	if res.Listed != 3 || res.Next != "" {
		t.Fatalf("listed %d, next %q; want 3 and none", res.Listed, res.Next)
	}
}

// A dry run deletes nothing; the same world without it deletes exactly the
// listed run's executions, in the deployment's Temporal namespace.
func TestReconcileRestore_DryRunDeletesNothingAndTheRunDeletes(t *testing.T) {
	w := newRestoreWorld(t)
	w.archived(t, "ork-after")
	w.retired(t, "ork-after", w.restored.Add(time.Minute))
	w.cluster.found["ork-after"] = true
	w.cluster.closeTime["ork-after"] = w.restored

	w.reconcile(t, RestoreRequest{DryRun: true})
	if n := len(w.cluster.deleteCalls); n != 0 {
		t.Fatalf("a dry run deleted %d runs' executions", n)
	}
	res := w.reconcile(t, RestoreRequest{})
	if !res.Retired[0].Deleted || w.cluster.deleteCalls["ork-after"] != 1 {
		t.Fatalf("row %+v, deletes %v; want ork-after's executions deleted once", res.Retired[0], w.cluster.deleteCalls)
	}
	if w.cluster.deleteNamespaces[0] != "ork-main" {
		t.Fatalf("deleted in %q; want the deployment's Temporal namespace", w.cluster.deleteNamespaces[0])
	}
}

// An execution whose run has no archive body is listed but never deleted:
// nothing else would answer for the run.
func TestReconcileRestore_NeverDeletesARunWithNoArchive(t *testing.T) {
	w := newRestoreWorld(t)
	w.retired(t, "ork-unarchived", w.restored.Add(time.Minute))
	w.cluster.found["ork-unarchived"] = true
	w.cluster.closeTime["ork-unarchived"] = w.restored

	res := w.reconcile(t, RestoreRequest{})
	if len(res.Retired) != 1 || res.Retired[0].Archived || res.Retired[0].Deleted {
		t.Fatalf("retired = %+v; want one unarchived row, not deleted", res.Retired)
	}
	if len(w.cluster.deleteCalls) != 0 {
		t.Fatalf("deleted %v; want nothing", w.cluster.deleteCalls)
	}
}

// Runs the restored cluster shows live whose finished archive the bucket
// holds are listed on the first page, and only there.
func TestReconcileRestore_ListsRunsFinishedAfterTheRestore(t *testing.T) {
	w := newRestoreWorld(t)
	w.archived(t, "ork-finished")
	w.cluster.live = []string{"ork-finished", "ork-still-running"}
	w.retired(t, "ork-a", w.restored.Add(time.Minute))
	w.retired(t, "ork-b", w.restored.Add(time.Minute))

	first := w.reconcile(t, RestoreRequest{DryRun: true, Limit: 1})
	if !slices.Equal(first.Finished, []string{"ork-finished"}) {
		t.Fatalf("finished = %v; want [ork-finished]", first.Finished)
	}
	if first.Next == "" {
		t.Fatal("a page of 1 of 2 cleanup records has no next cursor")
	}
	second := w.reconcile(t, RestoreRequest{DryRun: true, Limit: 1, After: first.Next})
	if second.Finished != nil || second.Listed != 1 || second.Next != "" {
		t.Fatalf("second page: finished %v, listed %d, next %q; want none, 1, none", second.Finished, second.Listed, second.Next)
	}
}

// Pages cover every cleanup record once, in key order.
func TestReconcileRestore_PagesCoverEveryRecordOnce(t *testing.T) {
	w := newRestoreWorld(t)
	var want []string
	for _, id := range []string{"ork-1", "ork-2", "ork-3", "ork-4", "ork-5"} {
		w.archived(t, id)
		w.retired(t, id, w.restored.Add(time.Minute))
		w.cluster.found[id] = true
		w.cluster.closeTime[id] = w.restored
		want = append(want, id)
	}
	var got []string
	after := ""
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("paging does not end")
		}
		res := w.reconcile(t, RestoreRequest{DryRun: true, Limit: 2, After: after})
		got = append(got, retiredIDs(res)...)
		if res.Next == "" {
			break
		}
		after = res.Next
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paged = %v; want %v", got, want)
	}
}

// Every read it cannot make fails the page: a run is never skipped because
// the cluster or the store did not answer.
func TestReconcileRestore_FailsClosed(t *testing.T) {
	boom := errors.New("unavailable")
	cases := []struct {
		name string
		set  func(w *restoreWorld)
		want string
	}{
		{"lookup", func(w *restoreWorld) { w.cluster.lookupErr["ork-x"] = boom }, "look up its execution"},
		{"delete", func(w *restoreWorld) { w.cluster.deleteErr["ork-x"] = boom }, "delete its executions"},
		{"open runs", func(w *restoreWorld) { w.cluster.openErr = boom }, "list the open runs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newRestoreWorld(t)
			w.archived(t, "ork-x")
			w.retired(t, "ork-x", w.restored.Add(time.Minute))
			w.cluster.found["ork-x"] = true
			w.cluster.closeTime["ork-x"] = w.restored
			tc.set(w)
			_, err := ReconcileRestore(context.Background(), w.store, w.cluster, "ork-main",
				RestoreRequest{RestoredAt: w.restored, Limit: 10})
			if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v; want %q wrapping the cause", err, tc.want)
			}
		})
	}
}

// A request without the restore instant, or with a page outside its bound,
// is refused before anything is read.
func TestReconcileRestore_RefusesABadRequest(t *testing.T) {
	w := newRestoreWorld(t)
	for _, req := range []RestoreRequest{
		{Limit: 10},
		{RestoredAt: w.restored, Limit: 0},
		{RestoredAt: w.restored, Limit: MaxRestorePage + 1},
	} {
		if _, err := ReconcileRestore(context.Background(), w.store, w.cluster, "ork-main", req); err == nil {
			t.Errorf("request %+v was accepted", req)
		}
	}
	if len(w.cluster.lookupNamespaces) != 0 {
		t.Fatal("a refused request looked something up")
	}
}

// A restored execution that is running is listed open and never deleted: a
// delete would terminate it, a resumed run's new work included. The same run
// closed is deleted, so the guard is what holds it.
func TestReconcileRestore_NeverDeletesARunningExecution(t *testing.T) {
	w := newRestoreWorld(t)
	w.archived(t, "ork-running")
	w.retired(t, "ork-running", w.restored.Add(time.Minute))
	w.cluster.found["ork-running"] = true

	res := w.reconcile(t, RestoreRequest{})
	if len(res.Retired) != 1 || !res.Retired[0].Open || res.Retired[0].Deleted || len(w.cluster.deleteCalls) != 0 {
		t.Fatalf("row %+v, deletes %v; want one open row, nothing deleted", res.Retired, w.cluster.deleteCalls)
	}
	w.cluster.closeTime["ork-running"] = time.Now()
	if res := w.reconcile(t, RestoreRequest{}); !res.Retired[0].Deleted {
		t.Fatalf("the closed execution was not deleted: %+v", res.Retired[0])
	}
}

// The open-run listing is bounded, and a cut-short listing says so.
func TestReconcileRestore_OpenRunListingIsBounded(t *testing.T) {
	w := newRestoreWorld(t)
	for i := range MaxRestoreOpenRuns + 1 {
		w.cluster.live = append(w.cluster.live, fmt.Sprintf("ork-open-%05d", i))
	}
	res := w.reconcile(t, RestoreRequest{DryRun: true})
	if w.cluster.asked != MaxRestoreOpenRuns || !res.FinishedTruncated {
		t.Fatalf("asked for %d open runs, truncated %v; want %d and true", w.cluster.asked, res.FinishedTruncated, MaxRestoreOpenRuns)
	}
}

// A cleanup record the store wrote more than a day before the restore
// instant is not read; one inside the margin is. The same run with a recent
// write time is listed, so the filter is what skips it.
func TestReconcileRestore_SkipsRecordsWrittenLongBeforeTheRestore(t *testing.T) {
	w := newRestoreWorld(t)
	for _, id := range []string{"ork-old", "ork-margin"} {
		w.archived(t, id)
		w.retired(t, id, w.restored.Add(time.Minute))
		w.cluster.found[id] = true
		w.cluster.closeTime[id] = w.restored
	}
	w.written(t, "ork-old", w.restored.Add(-restoreListMargin-time.Hour))
	w.written(t, "ork-margin", w.restored.Add(-restoreListMargin+time.Hour))

	res := w.reconcile(t, RestoreRequest{DryRun: true})
	if got := retiredIDs(res); !slices.Equal(got, []string{"ork-margin"}) {
		t.Fatalf("retired = %v; want [ork-margin]", got)
	}
	if res.Listed != 2 {
		t.Fatalf("listed %d; want 2: a skipped record still counts and advances the page", res.Listed)
	}
	w.written(t, "ork-old", time.Now())
	if got := retiredIDs(w.reconcile(t, RestoreRequest{DryRun: true})); !slices.Equal(got, []string{"ork-margin", "ork-old"}) {
		t.Fatalf("retired = %v; want both once ork-old's record is recent", got)
	}
}

// A cleanup record that names a run other than its key's is refused, not
// acted on.
func TestReconcileRestore_RefusesARecordNamingAnotherRun(t *testing.T) {
	w := newRestoreWorld(t)
	w.retired(t, "ork-real", w.restored.Add(time.Minute))
	realKey, err := artifacts.RunCleanupKey("ork-real")
	must.OK(t, err)
	otherKey, err := artifacts.RunCleanupKey("ork-other")
	must.OK(t, err)
	body, err := os.ReadFile(filepath.Join(w.root, filepath.FromSlash(realKey)))
	must.OK(t, err)
	must.OK(t, os.WriteFile(filepath.Join(w.root, filepath.FromSlash(otherKey)), body, 0o644))
	_, err = ReconcileRestore(context.Background(), w.store, w.cluster, "ork-main", RestoreRequest{RestoredAt: w.restored, Limit: 10})
	if err == nil || !strings.Contains(err.Error(), "names run") {
		t.Fatalf("err = %v; want the mismatched record refused", err)
	}
}

// A delete that fails ends the page, and the result still names the runs it
// deleted before, so the caller can say what it did.
func TestReconcileRestore_AFailedDeleteReturnsWhatItDeleted(t *testing.T) {
	w := newRestoreWorld(t)
	for _, id := range []string{"ork-1", "ork-2"} {
		w.archived(t, id)
		w.retired(t, id, w.restored.Add(time.Minute))
		w.cluster.found[id] = true
		w.cluster.closeTime[id] = w.restored
	}
	w.cluster.deleteErr["ork-2"] = errors.New("unavailable")
	res, err := ReconcileRestore(context.Background(), w.store, w.cluster, "ork-main", RestoreRequest{RestoredAt: w.restored, Limit: 10})
	if err == nil {
		t.Fatal("a failed delete was not an error")
	}
	if len(res.Retired) != 1 || res.Retired[0].RunID != "ork-1" || !res.Retired[0].Deleted {
		t.Fatalf("retired = %+v; want ork-1 deleted before the failure", res.Retired)
	}
}

// The run archive the engine wires is a RestoreStore: were it not, cmd/ork's
// type assertion would leave the RPC unwired, answering FAILED_PRECONDITION.
var _ RestoreStore = (*artifacts.BlobRunArchive)(nil)
