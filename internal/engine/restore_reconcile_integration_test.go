//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"slices"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// A restore reconcile against a real cluster and object store: a finished,
// archived run whose cleanup record says cleanup retired it after the
// restore instant (as a restore of Temporal to that instant brings its
// execution back) has its execution deleted; a running run with the same
// record is kept, and, its finished archive already in the bucket, is listed
// among the runs that will run their tail again. A dry run first deletes
// nothing. The restore itself is simulated by writing the cleanup records
// cleanup writes after a retirement, without the retirement.
func TestIntegration_RestoreReconcile_DeletesRestoredExecutionsAndKeepsRunningOnes(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg, _ := privateCleanupBucket(t, ctx)
	namespace := fmt.Sprintf("%s-restore-%d-%d", baseNamespace(), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	temporalClient := cleanupTemporalClient(t, ctx, address, namespace)
	reader := engine.NewTemporalRunReader(temporalClient)
	runArchive, err := artifacts.NewS3RunArchive(ctx, cfg)
	must.OKf(t, err, "NewS3RunArchive")
	snapshots, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	must.OKf(t, err, "snapshot store")
	reader.WithSnapshotStore(snapshots)

	queue := fmt.Sprintf("ork-restore-e2e-%d", time.Now().UnixNano())
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	taskWorker := worker.New(temporalClient, queue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	taskWorker.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "hold" {
			select {
			case <-release:
			case <-actx.Done():
				return nil, actx.Err()
			}
		}
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`)}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	bus, runs := engine.NewEventBus(), engine.NewRunStore()
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	eventWorker.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(bus, runs, engine.NewRunArchiver(runArchive)).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	for name, w := range map[string]worker.Worker{"run": runWorker, "task": taskWorker, "event": eventWorker} {
		must.OKf(t, w.Start(), "start %s worker", name)
		defer w.Stop()
	}
	start := func(label, taskID string) string {
		runID := fmt.Sprintf("ork-restore-%s-%d", label, time.Now().UnixNano())
		definition := &orkstackv1.Orkflow{
			Name: "restore-" + label, Namespace: "default", Version: "1.0.0",
			Tasks: []*orkstackv1.Task{{Id: taskID, Uses: "restore-test", Image: &orkstackv1.ImageRef{Reference: "restore-test", Pinned: "restore-test"}, TimeoutSeconds: 120}},
		}
		must.OKf(t, engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, queue).StartRun(ctx, runID, definition, nil, engine.RunPlan{
			Namespace: "default", Routes: map[string]string{task.DefaultPool: queue},
		}), "StartRun(%s)", label)
		return runID
	}

	restoredAt := time.Now().Add(-time.Hour)
	doneID := start("done", "build")
	must.OKf(t, temporalClient.GetWorkflow(ctx, doneID, "").Get(ctx, nil), "wait for the done run")
	awaitArchivedRun(t, ctx, runArchive, doneID)
	runningID := start("running", "hold")
	// The bucket already holds the running run's finished archive, as after a
	// restore of Temporal to before it finished. The body is written by hand:
	// the run is held open on purpose, so no real finish can write it.
	must.OKf(t, runArchive.PutRun(ctx, artifacts.RunRecord{
		RunID: runningID, Namespace: "default", Orkflow: "restore-running", Status: "succeeded",
		Time: time.Now(), JSONL: []byte(`{"run_id":"` + runningID + `"}` + "\n"),
	}), "archive the running run")
	for _, id := range []string{doneID, runningID} {
		must.OKf(t, runArchive.MarkArtifactsCleaned(ctx, id, restoredAt.Add(30*time.Minute)), "cleanup record of %s", id)
	}
	reconciler := archivecleanup.NewRestoreReconciler(runArchive, reader, namespace)

	// The running run reaches the cluster's index before the open listing can
	// see it: poll on the test's deadline.
	var dry archivecleanup.RestoreResult
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		dry, err = reconciler.ReconcileRestore(ctx, archivecleanup.RestoreRequest{RestoredAt: restoredAt, DryRun: true, Limit: 100})
		must.OKf(t, err, "dry run")
		if slices.Contains(dry.Finished, runningID) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the running run never appeared among the runs finished in the archive: %+v", dry)
		case <-ticker.C:
		}
	}
	rows := map[string]archivecleanup.RestoreRetired{}
	for _, r := range dry.Retired {
		rows[r.RunID] = r
	}
	if r := rows[doneID]; !r.Archived || r.Open || r.Deleted {
		t.Fatalf("dry run: done row %+v; want archived, closed, not deleted", r)
	}
	if r := rows[runningID]; !r.Open || r.Deleted {
		t.Fatalf("dry run: running row %+v; want open, not deleted", r)
	}
	found, _, err := reader.LookupWorkflow(ctx, namespace, doneID)
	must.OKf(t, err, "LookupWorkflow after the dry run")
	if !found {
		t.Fatal("the dry run deleted an execution")
	}

	res, err := reconciler.ReconcileRestore(ctx, archivecleanup.RestoreRequest{RestoredAt: restoredAt, Limit: 100})
	must.OKf(t, err, "reconcile")
	for _, r := range res.Retired {
		if r.RunID == runningID && r.Deleted {
			t.Fatal("the running run's execution was deleted")
		}
	}
	awaitWorkflowGone(t, ctx, reader, namespace, doneID)
	found, closedAt, err := reader.LookupWorkflow(ctx, namespace, runningID)
	must.OKf(t, err, "LookupWorkflow of the running run")
	if !found || !closedAt.IsZero() {
		t.Fatalf("the running run: found %v, closed %v; want it still running", found, closedAt)
	}
	released = true
	close(release)
	must.OKf(t, temporalClient.GetWorkflow(ctx, runningID, "").Get(ctx, nil), "the kept run finishes")
}
