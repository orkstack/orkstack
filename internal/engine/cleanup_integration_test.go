//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	commonpb "go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_CleanupAfterTemporalRetention proves the cleanup boundary
// against a live Temporal cluster and a private real MinIO bucket. Temporal's
// retention period is too long for a test, so the test deterministically
// performs the two effects retention would eventually produce: it deletes one
// closed execution through Temporal's service and advances only the cleanup
// eligibility clock beyond namespace retention plus the safety buffer.
func TestIntegration_CleanupAfterTemporalRetention(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg, raw := privateCleanupBucket(t, ctx)

	namespace := fmt.Sprintf("%s-cleanup-%d-%d", baseNamespace(), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	// The OrkStack namespace is an application coordinate and deliberately
	// does not name a Temporal namespace. Cleanup must use namespace only for
	// Temporal calls and orkNamespace only for archive/commit coordinates.
	orkNamespace := "cleanup-e2e"
	temporalClient := cleanupTemporalClient(t, ctx, address, namespace)
	reader := engine.NewTemporalRunReader(temporalClient)
	retention, err := reader.NamespaceRetention(ctx, namespace)
	if err != nil {
		t.Fatalf("NamespaceRetention: %v", err)
	}

	runArchive, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	logStore, err := artifacts.NewLogStore(cfg)
	if err != nil {
		t.Fatalf("NewLogStore: %v", err)
	}
	objectStore, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("artifact store: %v", err)
	}
	snapshots := engine.NewSnapshotStore(objectStore)
	reader.WithSnapshotStore(snapshots)

	queue := fmt.Sprintf("ork-cleanup-e2e-%d", time.Now().UnixNano())
	bus, runs := engine.NewEventBus(), engine.NewRunStore()
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	var seededMu sync.Mutex
	seeded := make(map[string]artifacts.ArtifactRef)
	taskWorker := worker.New(temporalClient, queue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	taskWorker.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if err := logStore.PutLogChunk(actx, artifacts.LogChunk{
			LogRef: artifacts.LogRef{RunID: in.GetRunId(), TaskID: in.GetTaskId(), Attempt: 1},
			Seq:    1,
			Data:   []byte("cleanup integration log\n"),
		}); err != nil {
			return nil, err
		}
		commitKey, err := artifacts.CommitKey(orkNamespace, in.GetRunId(), in.GetTaskId())
		if err != nil {
			return nil, err
		}
		if _, err := raw.PutObject(actx, &s3.PutObjectInput{
			Bucket: aws.String(cfg.S3Bucket), Key: aws.String(commitKey),
			Body: bytes.NewReader([]byte(`{"schemaVersion":1}`)), ContentLength: aws.Int64(19),
		}); err != nil {
			return nil, err
		}
		ref, err := objectStore.Put(actx, "shared-cleanup-output", strings.NewReader("shared bytes"), artifacts.PutOptions{
			Metadata: artifacts.Provenance{RunID: in.GetRunId(), TaskID: in.GetTaskId(), Orkflow: in.GetOrkflowName()}.Metadata(nil),
		})
		if err != nil {
			return nil, err
		}
		seededMu.Lock()
		seeded[in.GetRunId()] = ref
		seededMu.Unlock()
		return &orkstackv1.ExecuteTaskResult{
			TaskId: in.GetTaskId(), ValuesJson: []byte(`{"ok":true}`),
			Artifacts: []*orkstackv1.ArtifactRef{{Name: ref.Name, Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}},
		}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})

	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(bus, runs, engine.NewRunArchiver(runArchive)).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	for name, w := range map[string]worker.Worker{"run": runWorker, "task": taskWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	start := func(label string) string {
		runID := fmt.Sprintf("ork-cleanup-%s-%d", label, time.Now().UnixNano())
		definition := &orkstackv1.Orkflow{
			Name: "cleanup-" + label, Namespace: orkNamespace, Version: "1.0.0",
			Tasks: []*orkstackv1.Task{{
				Id: "build", Uses: "cleanup-test", Image: &orkstackv1.ImageRef{Reference: "cleanup-test", Pinned: "cleanup-test"},
				TimeoutSeconds: 30,
			}},
		}
		if err := engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, queue).StartRun(ctx, runID, definition, nil, engine.RunPlan{
			Namespace: orkNamespace, Routes: map[string]string{task.DefaultPool: queue},
		}); err != nil {
			t.Fatalf("StartRun(%s): %v", label, err)
		}
		if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
			t.Fatalf("wait for %s run: %v", label, err)
		}
		awaitArchivedRun(t, ctx, runArchive, runID)
		return runID
	}

	goneID := start("gone")
	retainedID := start("retained")
	assertRunStoragePresent(t, ctx, raw, cfg.S3Bucket, orkNamespace, goneID)
	assertRunStoragePresent(t, ctx, raw, cfg.S3Bucket, orkNamespace, retainedID)

	seededMu.Lock()
	goneObject, ok := seeded[goneID]
	seededMu.Unlock()
	if !ok {
		t.Fatal("cleaned run did not publish a content-addressed object")
	}

	if _, err := temporalClient.WorkflowService().DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: goneID},
	}); err != nil {
		t.Fatalf("DeleteWorkflowExecution: %v", err)
	}
	awaitWorkflowGone(t, ctx, reader, namespace, goneID)

	cleaner := archivecleanup.NewCleaner(archivecleanup.CleanupConfig{
		TemporalNamespace: namespace, Interval: time.Hour, RetentionBuffer: archivecleanup.DefaultRetentionBuffer, BatchSize: 100,
	}, runArchive, reader, slog.New(slog.NewTextHandler(io.Discard, nil))).WithClock(func() time.Time {
		return time.Now().UTC().Add(retention + archivecleanup.DefaultRetentionBuffer + time.Hour)
	})
	stats, err := cleaner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsCleaned != 1 || stats.ObjectsDeleted < 2 {
		t.Fatalf("cleanup metrics = %+v, want one run and at least log+commit", stats)
	}

	assertNoRunStorage(t, ctx, raw, cfg.S3Bucket, orkNamespace, goneID)
	assertRunStoragePresent(t, ctx, raw, cfg.S3Bucket, orkNamespace, retainedID)
	if _, err := objectStore.Stat(ctx, goneObject.URI); err != nil {
		t.Fatalf("shared content-addressed object %q was deleted: %v", goneObject.URI, err)
	}
	body, err := runArchive.GetRun(ctx, goneID)
	if err != nil || len(body) == 0 {
		t.Fatalf("cleaned run archive body missing: bytes=%d error=%v", len(body), err)
	}
	listed, err := runArchive.ListRuns(ctx, artifacts.RunQuery{Namespace: orkNamespace, Orkflow: "cleanup-gone"})
	if err != nil || !containsArchivedRun(listed, goneID) {
		t.Fatalf("cleaned run absent from archive index: entries=%v error=%v", listed, err)
	}
	state, err := runArchive.CleanupState(ctx, goneID)
	if err != nil || !state.ArtifactsCleaned || state.TemporalDeletedAt.IsZero() {
		t.Fatalf("cleanup state = %+v, error=%v", state, err)
	}
	if _, err := runArchive.CleanupState(ctx, retainedID); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("retained run cleanup state error = %v, want ErrNotFound", err)
	}

	second, err := cleaner.RunOnce(ctx)
	if err != nil || second.RunsCleaned != 0 || second.ObjectsDeleted != 0 {
		t.Fatalf("second cleanup = %+v, error=%v", second, err)
	}
	if _, err := runArchive.GetRun(ctx, goneID); err != nil {
		t.Fatalf("archive body missing after idempotent pass: %v", err)
	}
	t.Logf("EVIDENCE distinct_namespaces=true temporal_history_gone=true logs_gone=true commits_gone=true archive_body_present=true archive_index_present=true cleanup_state=true shared_object_present=true retained_run_untouched=true idempotent=true runs_cleaned=%d objects_deleted=%d bytes_reclaimed=%d",
		stats.RunsCleaned, stats.ObjectsDeleted, stats.BytesReclaimed)
}

func cleanupTemporalClient(t *testing.T, ctx context.Context, address, namespace string) client.Client {
	t.Helper()
	bootstrap, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: client.DefaultNamespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	if err := task.EnsureNamespace(ctx, bootstrap, namespace); err != nil {
		bootstrap.Close()
		t.Fatalf("provision namespace %q: %v", namespace, err)
	}
	bootstrap.Close()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
		if err == nil {
			if _, probe := c.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: namespace}); probe == nil {
				t.Cleanup(c.Close)
				return c
			}
			c.Close()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("namespace %q did not become usable: %v", namespace, ctx.Err())
		case <-ticker.C:
		}
	}
}

func privateCleanupBucket(t *testing.T, ctx context.Context) (artifacts.Config, *s3.Client) {
	t.Helper()
	cfg := s3Config()
	requireMinIO(t, cfg)
	raw := newRawS3ClientForCommitE2E(t, cfg)
	cfg.S3Bucket = fmt.Sprintf("orkstack-cleanup-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		t.Fatalf("create private cleanup bucket: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for {
			page, err := raw.ListObjectsV2(cleanupCtx, &s3.ListObjectsV2Input{Bucket: aws.String(cfg.S3Bucket)})
			if err != nil || len(page.Contents) == 0 {
				break
			}
			for _, object := range page.Contents {
				_, _ = raw.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: object.Key})
			}
		}
		_, _ = raw.DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(cfg.S3Bucket)})
	})
	return cfg, raw
}

func awaitArchivedRun(t *testing.T, ctx context.Context, archive artifacts.RunArchive, runID string) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := archive.GetRun(ctx, runID); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run %q was not archived: %v", runID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func awaitWorkflowGone(t *testing.T, ctx context.Context, reader *engine.TemporalRunReader, namespace, runID string) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		exists, err := reader.WorkflowExists(ctx, namespace, runID)
		if err != nil {
			t.Fatalf("WorkflowExists after deletion: %v", err)
		}
		if !exists {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("workflow %q was not deleted: %v", runID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func runStorageKeys(t *testing.T, ctx context.Context, raw *s3.Client, bucket, namespace, runID string) (logs, commits []string) {
	t.Helper()
	logPrefix, err := artifacts.RunLogPrefix(runID)
	if err != nil {
		t.Fatalf("RunLogPrefix: %v", err)
	}
	commitPrefix, err := artifacts.RunCommitPrefix(namespace, runID)
	if err != nil {
		t.Fatalf("RunCommitPrefix: %v", err)
	}
	list := func(prefix string) []string {
		page, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
		if err != nil {
			t.Fatalf("list %q: %v", prefix, err)
		}
		out := make([]string, 0, len(page.Contents))
		for _, object := range page.Contents {
			out = append(out, aws.ToString(object.Key))
		}
		return out
	}
	return list(logPrefix), list(commitPrefix)
}

func assertRunStoragePresent(t *testing.T, ctx context.Context, raw *s3.Client, bucket, namespace, runID string) {
	t.Helper()
	logs, commits := runStorageKeys(t, ctx, raw, bucket, namespace, runID)
	if len(logs) == 0 || len(commits) == 0 {
		t.Fatalf("run %q storage before cleanup: logs=%v commits=%v; test would be vacuous", runID, logs, commits)
	}
}

func assertNoRunStorage(t *testing.T, ctx context.Context, raw *s3.Client, bucket, namespace, runID string) {
	t.Helper()
	logs, commits := runStorageKeys(t, ctx, raw, bucket, namespace, runID)
	if len(logs) != 0 || len(commits) != 0 {
		t.Fatalf("run %q storage after cleanup: logs=%v commits=%v", runID, logs, commits)
	}
}

func containsArchivedRun(entries []artifacts.RunIndexEntry, runID string) bool {
	for _, entry := range entries {
		if entry.RunID == runID {
			return true
		}
	}
	return false
}

// cancelAfterFirstDelete wraps a real archive and trips a cancel func the
// moment one run's storage has been durably deleted, so cancellation lands
// between two runs instead of inside one. Everything else is the real S3
// archive: only the timing of the cancellation is synthetic.
type cancelAfterFirstDelete struct {
	artifacts.RunArchive
	cancel context.CancelFunc
	once   sync.Once
}

func (a *cancelAfterFirstDelete) DeleteRunArtifacts(ctx context.Context, namespace, runID string) (artifacts.CleanupResult, error) {
	result, err := a.RunArchive.DeleteRunArtifacts(ctx, namespace, runID)
	if err == nil {
		a.once.Do(a.cancel)
	}
	return result, err
}

// TestIntegration_CleanupCancellationLeavesConsistentState is the chaos
// scenario the goal calls "Orker crash simulation during cleanup (Temporal
// cancellation)". Stating precisely what is simulated: cleanup is a background
// worker, NOT a Temporal activity, so there is no activity cancellation token
// to deliver. What is exercised is a cancelled in-flight cleanup iteration —
// the shape an engine shutdown or a killed replica produces — running against a
// live Temporal namespace and a private live MinIO bucket.
//
// The assertions are deliberately narrow: cleanup stops promptly, surfaces a
// cancellation-shaped error, leaves every run it touched either fully cleaned
// or wholly untouched, and converges on restart. Restarting is resumption, not
// cross-restart idempotence, and nothing here claims more.
func TestIntegration_CleanupCancellationLeavesConsistentState(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	ctx, cancelAll := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelAll()
	cfg, raw := privateCleanupBucket(t, ctx)

	namespace := fmt.Sprintf("%s-cancel-%d-%d", baseNamespace(), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	orkNamespace := "cleanup-cancel"
	temporalClient := cleanupTemporalClient(t, ctx, address, namespace)
	reader := engine.NewTemporalRunReader(temporalClient)

	runArchive, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	objectStore, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("artifact store: %v", err)
	}

	// Four archived runs, each owning a real object and a real log chunk, all
	// well past retention. None ever ran in Temporal, so the live
	// WorkflowExists calls legitimately report them gone.
	archived := time.Now().UTC().Add(-200 * time.Hour)
	var runIDs []string
	objectIDs := map[string]string{}
	for i := range 4 {
		runID := fmt.Sprintf("ork-cancel-%d-%d", time.Now().UnixNano(), i)
		ref, err := objectStore.Put(ctx, fmt.Sprintf("payload-%d", i),
			bytes.NewBufferString(fmt.Sprintf("payload %d", i)), artifacts.PutOptions{})
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		objectID, ok := artifacts.ObjectIDFromURI(ref.URI)
		if !ok {
			t.Fatalf("ObjectIDFromURI(%q) failed", ref.URI)
		}
		objectIDs[runID] = objectID
		logKey, err := artifacts.RunLogPrefix(runID)
		if err != nil {
			t.Fatalf("RunLogPrefix: %v", err)
		}
		if _, err := raw.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(cfg.S3Bucket), Key: aws.String(logKey + "task/1/chunk"),
			Body: bytes.NewReader([]byte("log bytes")),
		}); err != nil {
			t.Fatalf("seed log chunk: %v", err)
		}
		if err := runArchive.PutRun(ctx, artifacts.RunRecord{
			RunID: runID, Namespace: orkNamespace, Orkflow: "cleanup-cancel", Status: "succeeded",
			Time: archived.Add(time.Duration(i) * time.Minute), ObjectIDs: []string{objectID},
			JSONL: []byte(runID + "\n"),
		}); err != nil {
			t.Fatalf("PutRun(%s): %v", runID, err)
		}
		runIDs = append(runIDs, runID)
	}
	// Prove the seeded storage exists, so no assertion below can pass vacuously.
	for _, runID := range runIDs {
		logs, _ := runStorageKeys(t, ctx, raw, cfg.S3Bucket, orkNamespace, runID)
		if len(logs) == 0 {
			t.Fatalf("run %q has no seeded log storage", runID)
		}
	}

	iterationCtx, cancelIteration := context.WithCancel(ctx)
	defer cancelIteration()
	wrapped := &cancelAfterFirstDelete{RunArchive: runArchive, cancel: cancelIteration}
	config := archivecleanup.CleanupConfig{
		Enabled: true, TemporalNamespace: namespace, Interval: time.Hour,
		RetentionBuffer: 24 * time.Hour, BatchSize: len(runIDs),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	stats, err := archivecleanup.NewCleaner(config, wrapped, reader, logger).RunOnce(iterationCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled iteration error = %v, want context.Canceled", err)
	}
	// Prompt stop means exactly one run was TOUCHED, not that one run reached
	// every step. Cancellation can land between the delete and the sidecar
	// write, which is the interesting case, so it is classified rather than
	// assumed.
	if stats.RunsScanned != 1 {
		t.Fatalf("cancellation did not stop promptly: %+v", stats)
	}

	// Every run is in one of exactly two states, and neither is corrupt:
	// fully cleaned (storage gone, sidecar written), or wholly untouched.
	// A third shape — storage gone with no sidecar — is the cancelled-between-
	// steps case: recoverable, because deletion is idempotent and the next pass
	// re-runs it and writes the sidecar. What must never appear is a run whose
	// archive body or index row is damaged.
	cleaned, untouched, interrupted := 0, 0, 0
	for _, runID := range runIDs {
		state, stateErr := runArchive.CleanupState(ctx, runID)
		logs, commits := runStorageKeys(t, ctx, raw, cfg.S3Bucket, orkNamespace, runID)
		marked := stateErr == nil && state.ArtifactsCleaned
		if stateErr != nil && !errors.Is(stateErr, artifacts.ErrNotFound) {
			t.Fatalf("run %q cleanup state = %+v, %v", runID, state, stateErr)
		}
		switch {
		case marked && len(logs) == 0 && len(commits) == 0:
			cleaned++
		case !marked && len(logs) != 0:
			untouched++
		case !marked && len(logs) == 0:
			interrupted++
		default:
			t.Fatalf("run %q is corrupt: marked=%v logs=%v commits=%v", runID, marked, logs, commits)
		}
		if _, err := runArchive.GetRun(ctx, runID); err != nil {
			t.Fatalf("archive body for %q lost: %v", runID, err)
		}
		if !containsArchivedRun(mustListRuns(t, ctx, runArchive, orkNamespace), runID) {
			t.Fatalf("archive index row for %q lost", runID)
		}
	}
	if cleaned+interrupted != 1 || untouched != len(runIDs)-1 {
		t.Fatalf("inconsistent partial state: cleaned=%d interrupted=%d untouched=%d",
			cleaned, interrupted, untouched)
	}

	// Restart on a fresh context and converge: cleaned runs are skipped through
	// their sidecars, the interrupted remainder is retried and completes.
	resumed, err := archivecleanup.NewCleaner(config, runArchive, reader, logger).RunOnce(ctx)
	if err != nil {
		t.Fatalf("resumed iteration: %v", err)
	}
	// The interrupted run is cleaned AGAIN — its deletion never got a sidecar,
	// so the next pass repeats an idempotent delete and then marks it. That is
	// the goal's "not idempotent across restarts, but not corrupted", asserted
	// literally rather than glossed as a clean skip.
	if resumed.RunsCleaned != untouched+interrupted {
		t.Fatalf("resume cleaned %d, want %d (untouched=%d interrupted=%d)",
			resumed.RunsCleaned, untouched+interrupted, untouched, interrupted)
	}
	for _, runID := range runIDs {
		state, err := runArchive.CleanupState(ctx, runID)
		if err != nil || !state.ArtifactsCleaned {
			t.Fatalf("run %q did not converge: %+v, %v", runID, state, err)
		}
		assertNoRunStorage(t, ctx, raw, cfg.S3Bucket, orkNamespace, runID)
	}

	// Reference accounting survived the interruption: every run's markers are
	// gone, so each object is now reclaimable exactly once.
	var allObjects []string
	for _, runID := range runIDs {
		allObjects = append(allObjects, objectIDs[runID])
	}
	reclaimed, err := runArchive.DeleteUnreferencedObjects(ctx, allObjects)
	if err != nil || reclaimed.ObjectsDeleted != len(runIDs) {
		t.Fatalf("reclaim after resume = %+v, %v", reclaimed, err)
	}
	again, err := runArchive.DeleteUnreferencedObjects(ctx, allObjects)
	if err != nil || again != (artifacts.CleanupResult{}) {
		t.Fatalf("repeat reclaim = %+v, %v", again, err)
	}

	t.Logf("EVIDENCE cancellation_shaped_error=true stopped_promptly=true runs_scanned_before_cancel=%d "+
		"partial_state_consistent=true corrupt_runs=0 archive_bodies_present=true archive_index_present=true "+
		"resumed_and_converged=true reference_markers_released=true objects_reclaimed=%d repeat_reclaim_no_op=true "+
		"cleaned=%d interrupted_recoverable=%d untouched=%d",
		stats.RunsScanned, reclaimed.ObjectsDeleted, cleaned, interrupted, untouched)
}

func mustListRuns(t *testing.T, ctx context.Context, archive artifacts.RunArchive, namespace string) []artifacts.RunIndexEntry {
	t.Helper()
	entries, err := archive.ListRuns(ctx, artifacts.RunQuery{Namespace: namespace})
	if err != nil {
		t.Fatalf("ListRuns(%q): %v", namespace, err)
	}
	return entries
}
