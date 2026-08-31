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

	queue := fmt.Sprintf("ork-cleanup-e2e-%d", time.Now().UnixNano())
	bus, runs := engine.NewEventBus(), engine.NewRunStore()
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)

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
		engine.NewArchivingEventActivities(bus, runs, engine.NewRunArchiver(runArchive)).PublishEvent,
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
