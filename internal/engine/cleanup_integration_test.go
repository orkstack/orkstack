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
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// emptyNamespaceLister answers with no OrkStack namespace documents unless a
// test seeds one, so every entry falls back to the deployment maximum.
type staticNamespaceLister struct{ docs []*orkstackv1.Namespace }

func (l staticNamespaceLister) List(context.Context) ([]*orkstackv1.Namespace, error) {
	return l.docs, nil
}

// TestIntegration_CleanupRetiresARunAtItsNamespaceRetention proves the
// ACTIVE retirement path against a live Temporal cluster and a private real
// MinIO bucket: cleanup itself deletes a finished execution once it is past
// its OWN OrkStack namespace's run retention — never by waiting on Temporal's
// own passive TTL. Two OrkStack namespaces make the point: "short" has a
// one-minute run retention and is retired; "long" inherits a much larger
// deployment maximum and is untouched. An independent DescribeNamespace call
// after the pass proves the underlying cluster's own backstop TTL is still
// far above the one-minute window that actually did the work — i.e. OrkStack,
// not the passive TTL, retired the run.
func TestIntegration_CleanupRetiresARunAtItsNamespaceRetention(t *testing.T) {
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
	// The OrkStack namespaces are application coordinates and deliberately do
	// not name a Temporal namespace. Cleanup must use `namespace` only for
	// Temporal calls and each run's own OrkStack namespace only for
	// archive/commit coordinates and its own retention.
	const (
		orkNamespaceShort = "cleanup-e2e-short"
		orkNamespaceLong  = "cleanup-e2e-long"
	)
	temporalClient := cleanupTemporalClient(t, ctx, address, namespace)
	reader := engine.NewTemporalRunReader(temporalClient)

	policies, err := artifacts.NewS3RetentionPolicyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RetentionPolicyStore: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retentionCtrl := archivecleanup.NewRetentionController(policies, reader, namespace, archivecleanup.DefaultRetentionBuffer, logger)
	// A large deployment maximum, reconciled onto the real cluster's backstop
	// TTL: this is what the independent check below proves was NOT what
	// retired the short-lived run.
	const deploymentMax = 999 * time.Hour
	if _, err := retentionCtrl.SetMax(ctx, deploymentMax); err != nil {
		t.Fatalf("SetMax: %v", err)
	}
	const shortRetention = time.Minute
	namespaces := staticNamespaceLister{docs: []*orkstackv1.Namespace{
		{Name: orkNamespaceShort, RunRetention: durationpb.New(shortRetention)},
		{Name: orkNamespaceLong}, // inherits deploymentMax
	}}

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
	snapshots, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		t.Fatalf("snapshot store: %v", err)
	}
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
	var runNamespacesMu sync.Mutex
	runNamespaces := make(map[string]string)
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
		runNamespacesMu.Lock()
		orkNamespace := runNamespaces[in.GetRunId()]
		runNamespacesMu.Unlock()
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

	start := func(label, orkNamespace string) string {
		runID := fmt.Sprintf("ork-cleanup-%s-%d", label, time.Now().UnixNano())
		runNamespacesMu.Lock()
		runNamespaces[runID] = orkNamespace
		runNamespacesMu.Unlock()
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

	// "gone" belongs to the short-retention namespace and is retired by
	// cleanup itself; "retained" belongs to the long (inherited, 999h)
	// namespace and must survive the same pass untouched.
	goneID := start("gone", orkNamespaceShort)
	retainedID := start("retained", orkNamespaceLong)
	assertRunStoragePresent(t, ctx, raw, cfg.S3Bucket, orkNamespaceShort, goneID)
	assertRunStoragePresent(t, ctx, raw, cfg.S3Bucket, orkNamespaceLong, retainedID)

	seededMu.Lock()
	goneObject, ok := seeded[goneID]
	seededMu.Unlock()
	if !ok {
		t.Fatal("cleaned run did not publish a content-addressed object")
	}

	// Independent proof, BEFORE cleanup runs, that the underlying cluster's
	// own backstop TTL is nowhere near what is about to retire "gone": it was
	// just reconciled to deploymentMax + the enforcement margin, far longer
	// than the test's lifetime.
	backstopBefore, err := reader.NamespaceRetention(ctx, namespace)
	if err != nil {
		t.Fatalf("NamespaceRetention (independent check, before): %v", err)
	}
	if backstopBefore < deploymentMax {
		t.Fatalf("backstop TTL before cleanup = %s, want at least the %s deployment maximum", backstopBefore, deploymentMax)
	}

	cleaner := archivecleanup.NewCleaner(archivecleanup.CleanupConfig{
		TemporalNamespace: namespace, Interval: time.Hour, RetentionBuffer: archivecleanup.DefaultRetentionBuffer, BatchSize: 100,
	}, runArchive, reader, namespaces, retentionCtrl, logger).WithClock(func() time.Time {
		// Past "gone"'s one-minute namespace retention, nowhere near "retained"'s
		// 999h inherited one.
		return time.Now().UTC().Add(2 * shortRetention)
	})
	stats, err := cleaner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsCleaned != 1 || stats.ObjectsDeleted < 3 {
		t.Fatalf("cleanup metrics = %+v, want one run and at least log+commit+snapshot", stats)
	}

	// THE assertion: the execution is gone, proven by a lookup independent of
	// the cleanup worker's own call — DeleteWorkflowExecution is
	// asynchronous, so this polls briefly rather than asserting instantly.
	awaitWorkflowGone(t, ctx, reader, namespace, goneID)

	// And independent proof AFTER cleanup that the backstop TTL never moved
	// down to anything close to the one-minute window that actually did the
	// work: OrkStack's active retirement did this, not Temporal's own TTL.
	backstopAfter, err := reader.NamespaceRetention(ctx, namespace)
	if err != nil {
		t.Fatalf("NamespaceRetention (independent check, after): %v", err)
	}
	if backstopAfter < deploymentMax {
		t.Fatalf("backstop TTL after cleanup = %s, want it unchanged at >= %s: "+
			"the passive TTL must never be what retires a run", backstopAfter, deploymentMax)
	}

	assertNoRunStorage(t, ctx, raw, cfg.S3Bucket, orkNamespaceShort, goneID)
	assertRunStoragePresent(t, ctx, raw, cfg.S3Bucket, orkNamespaceLong, retainedID)
	if _, err := objectStore.Stat(ctx, goneObject.URI); err != nil {
		t.Fatalf("shared content-addressed object %q was deleted: %v", goneObject.URI, err)
	}
	body, err := runArchive.GetRun(ctx, goneID)
	if err != nil || len(body) == 0 {
		t.Fatalf("cleaned run archive body missing: bytes=%d error=%v", len(body), err)
	}
	listed, err := runArchive.ListRuns(ctx, artifacts.RunQuery{Namespace: orkNamespaceShort, Orkflow: "cleanup-gone"})
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
	t.Logf("EVIDENCE distinct_namespaces=true temporal_history_gone=true logs_gone=true commits_gone=true snapshots_gone=true archive_body_present=true archive_index_present=true cleanup_state=true shared_object_present=true retained_run_untouched=true idempotent=true runs_cleaned=%d objects_deleted=%d bytes_reclaimed=%d",
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
		found, _, err := reader.LookupWorkflow(ctx, namespace, runID)
		if err != nil {
			t.Fatalf("LookupWorkflow after deletion: %v", err)
		}
		if !found {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("workflow %q was not deleted: %v", runID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func runStorageKeys(t *testing.T, ctx context.Context, raw *s3.Client, bucket, namespace, runID string) (logs, commits, snapshots []string) {
	t.Helper()
	logPrefix, err := artifacts.RunLogPrefix(runID)
	if err != nil {
		t.Fatalf("RunLogPrefix: %v", err)
	}
	commitPrefix, err := artifacts.RunCommitPrefix(namespace, runID)
	if err != nil {
		t.Fatalf("RunCommitPrefix: %v", err)
	}
	snapshotPrefix, err := artifacts.RunSnapshotPrefix(namespace, runID)
	if err != nil {
		t.Fatalf("RunSnapshotPrefix: %v", err)
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
	return list(logPrefix), list(commitPrefix), list(snapshotPrefix)
}

func assertRunStoragePresent(t *testing.T, ctx context.Context, raw *s3.Client, bucket, namespace, runID string) {
	t.Helper()
	logs, commits, snapshots := runStorageKeys(t, ctx, raw, bucket, namespace, runID)
	if len(logs) == 0 || len(commits) == 0 || len(snapshots) == 0 {
		t.Fatalf("run %q storage before cleanup: logs=%v commits=%v snapshots=%v; test would be vacuous",
			runID, logs, commits, snapshots)
	}
}

func assertNoRunStorage(t *testing.T, ctx context.Context, raw *s3.Client, bucket, namespace, runID string) {
	t.Helper()
	logs, commits, snapshots := runStorageKeys(t, ctx, raw, bucket, namespace, runID)
	if len(logs) != 0 || len(commits) != 0 || len(snapshots) != 0 {
		t.Fatalf("run %q storage after cleanup: logs=%v commits=%v snapshots=%v",
			runID, logs, commits, snapshots)
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
	// LookupWorkflow calls legitimately report them gone (found=false), and
	// cleanup reclaims their storage without ever calling DeleteWorkflow.
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
		logs, _, _ := runStorageKeys(t, ctx, raw, cfg.S3Bucket, orkNamespace, runID)
		if len(logs) == 0 {
			t.Fatalf("run %q has no seeded log storage", runID)
		}
	}

	policies, err := artifacts.NewS3RetentionPolicyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RetentionPolicyStore: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retentionCtrl := archivecleanup.NewRetentionController(policies, reader, namespace, 24*time.Hour, logger)
	if _, err := retentionCtrl.SetMax(ctx, 72*time.Hour); err != nil {
		t.Fatalf("SetMax: %v", err)
	}
	namespaces := staticNamespaceLister{} // no docs: "cleanup-cancel" inherits the 72h maximum

	iterationCtx, cancelIteration := context.WithCancel(ctx)
	defer cancelIteration()
	wrapped := &cancelAfterFirstDelete{RunArchive: runArchive, cancel: cancelIteration}
	config := archivecleanup.CleanupConfig{
		Enabled: true, TemporalNamespace: namespace, Interval: time.Hour,
		RetentionBuffer: 24 * time.Hour, BatchSize: len(runIDs),
	}

	stats, err := archivecleanup.NewCleaner(config, wrapped, reader, namespaces, retentionCtrl, logger).RunOnce(iterationCtx)
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
		logs, commits, _ := runStorageKeys(t, ctx, raw, cfg.S3Bucket, orkNamespace, runID)
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
	resumed, err := archivecleanup.NewCleaner(config, runArchive, reader, namespaces, retentionCtrl, logger).RunOnce(ctx)
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

// TestIntegration_ArtifactProvenanceReportsReleasedAfterCleanup is the
// RELEASED state observed end to end: a real run on a live cluster, archived
// to a private MinIO bucket, answers HELD through the API; the cleanup pass
// retires it at its namespace retention; the same question then answers
// RELEASED with released_at equal to the cleanup sidecar's
// temporal_deleted_at, and the object's marker prefix is empty. The run is
// read back from the archive by then — its Temporal history is gone — so the
// answer also proves the read survives retirement.
func TestIntegration_ArtifactProvenanceReportsReleasedAfterCleanup(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg, raw := privateCleanupBucket(t, ctx)

	namespace := fmt.Sprintf("%s-provenance-%d-%d", baseNamespace(), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	const orkNamespace = "provenance-e2e"
	temporalClient := cleanupTemporalClient(t, ctx, address, namespace)
	reader := engine.NewTemporalRunReader(temporalClient)

	policies, err := artifacts.NewS3RetentionPolicyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RetentionPolicyStore: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retentionCtrl := archivecleanup.NewRetentionController(policies, reader, namespace, archivecleanup.DefaultRetentionBuffer, logger)
	if _, err := retentionCtrl.SetMax(ctx, 999*time.Hour); err != nil {
		t.Fatalf("SetMax: %v", err)
	}
	const shortRetention = time.Minute
	nsDoc := &orkstackv1.Namespace{Name: orkNamespace, RunRetention: durationpb.New(shortRetention)}

	registry, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	if _, err := registry.Put(ctx, nsDoc, artifacts.PutConditions{IfAbsent: true}); err != nil {
		t.Fatalf("register namespace: %v", err)
	}
	runArchive, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	objectStore, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("artifact store: %v", err)
	}
	snapshots, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		t.Fatalf("snapshot store: %v", err)
	}
	reader.WithSnapshotStore(snapshots)
	archiver := engine.NewRunArchiver(runArchive)

	queue := fmt.Sprintf("ork-provenance-e2e-%d", time.Now().UnixNano())
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	taskWorker := worker.New(temporalClient, queue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	taskWorker.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		ref, err := objectStore.Put(actx, "report.txt", strings.NewReader("REPORT"), artifacts.PutOptions{
			Metadata: artifacts.Provenance{RunID: in.GetRunId(), TaskID: in.GetTaskId(), Orkflow: in.GetOrkflowName()}.Metadata(nil),
		})
		if err != nil {
			return nil, err
		}
		return &orkstackv1.ExecuteTaskResult{
			TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`),
			Artifacts: []*orkstackv1.ArtifactRef{{Name: ref.Name, Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}},
		}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(engine.NewEventBus(), engine.NewRunStore(), archiver).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	for name, w := range map[string]worker.Worker{"run": runWorker, "task": taskWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	runID := fmt.Sprintf("ork-provenance-%d", time.Now().UnixNano())
	definition := &orkstackv1.Orkflow{
		Name: "provenance-cleanup", Namespace: orkNamespace, Version: "1.0.0",
		Tasks: []*orkstackv1.Task{{
			Id: "completed", Uses: "provenance-test", Image: &orkstackv1.ImageRef{Reference: "provenance-test", Pinned: "provenance-test"},
			TimeoutSeconds: 30,
		}},
	}
	if err := engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, queue).StartRun(ctx, runID, definition, nil, engine.RunPlan{
		Namespace: orkNamespace, Routes: map[string]string{task.DefaultPool: queue},
	}); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("wait for run: %v", err)
	}
	awaitArchivedRun(t, ctx, runArchive, runID)

	// The API as `ork` wires it: the cluster reader first, the archive behind
	// it, the shared object store — nothing from this process's memory.
	svc := api.NewService(api.Config{
		Reader:        reader,
		Namespaces:    registry,
		Archive:       archiver,
		ArtifactStore: objectStore,
		Logger:        logger,
	})
	ask := func() *orkstackv1.GetArtifactProvenanceResponse {
		t.Helper()
		resp, err := svc.GetArtifactProvenance(ctx, connect.NewRequest(&orkstackv1.GetArtifactProvenanceRequest{
			RunId: runID, Namespace: orkNamespace, TaskId: "completed",
			Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "report.txt",
		}))
		if err != nil {
			t.Fatalf("GetArtifactProvenance: %v", err)
		}
		return resp.Msg
	}

	before := ask()
	if before.GetOwnReference() != orkstackv1.ReferenceState_REFERENCE_STATE_HELD || !before.GetBytesPresent() ||
		len(before.GetReferences()) != 1 || before.GetReferences()[0].GetRunId() != runID {
		t.Fatalf("before cleanup: own=%s bytes=%t references=%v; want HELD, present, exactly %s",
			before.GetOwnReference(), before.GetBytesPresent(), before.GetReferences(), runID)
	}
	objectID, ok := artifacts.ObjectIDFromURI(before.GetArtifact().GetUri())
	if !ok {
		t.Fatalf("ObjectIDFromURI(%s)", before.GetArtifact().GetUri())
	}

	cleaner := archivecleanup.NewCleaner(archivecleanup.CleanupConfig{
		TemporalNamespace: namespace, Interval: time.Hour, RetentionBuffer: archivecleanup.DefaultRetentionBuffer, BatchSize: 100,
	}, runArchive, reader, staticNamespaceLister{docs: []*orkstackv1.Namespace{nsDoc}}, retentionCtrl, logger).WithClock(func() time.Time {
		return time.Now().UTC().Add(2 * shortRetention)
	})
	stats, err := cleaner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsCleaned != 1 {
		t.Fatalf("cleanup metrics = %+v, want the one run retired", stats)
	}
	awaitWorkflowGone(t, ctx, reader, namespace, runID)

	state, err := runArchive.CleanupState(ctx, runID)
	if err != nil || state.TemporalDeletedAt.IsZero() {
		t.Fatalf("cleanup state = %+v, %v", state, err)
	}
	after := ask()
	if after.GetOwnReference() != orkstackv1.ReferenceState_REFERENCE_STATE_RELEASED {
		t.Fatalf("after cleanup: own_reference = %s; want RELEASED", after.GetOwnReference())
	}
	if after.GetReleasedAt() == nil || !after.GetReleasedAt().AsTime().Equal(state.TemporalDeletedAt) {
		t.Fatalf("released_at = %v; want the sidecar's temporal_deleted_at %v", after.GetReleasedAt(), state.TemporalDeletedAt)
	}
	if len(after.GetReferences()) != 0 || after.GetReferencesUnreadable() != 0 {
		t.Fatalf("after cleanup: references = %v unreadable = %d; want none", after.GetReferences(), after.GetReferencesUnreadable())
	}
	markerPrefix := path.Dir(mustRunObjectRefKey(t, objectID, runID)) + "/"
	page, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(cfg.S3Bucket), Prefix: aws.String(markerPrefix)})
	if err != nil {
		t.Fatalf("list marker prefix: %v", err)
	}
	if len(page.Contents) != 0 {
		t.Fatalf("marker prefix %s still holds %d keys after cleanup", markerPrefix, len(page.Contents))
	}
	t.Logf("EVIDENCE provenance_after_cleanup run=%s before=%s after=%s released_at=%s temporal_deleted_at=%s marker_prefix_keys=%d bytes_present=%t",
		runID, before.GetOwnReference(), after.GetOwnReference(), after.GetReleasedAt().AsTime().Format(time.RFC3339Nano),
		state.TemporalDeletedAt.Format(time.RFC3339Nano), len(page.Contents), after.GetBytesPresent())
}

func mustRunObjectRefKey(t *testing.T, objectID, runID string) string {
	t.Helper()
	key, err := artifacts.RunObjectRefKey(objectID, runID)
	if err != nil {
		t.Fatalf("RunObjectRefKey: %v", err)
	}
	return key
}

// TestIntegration_RunEvidenceReflectsCleanupAsReclaimed is run-evidence-bundle
// I2: a real run on a live cluster, archived to a private MinIO bucket, with a
// real commit record, a real attempt record and a real uploaded stdout.log.
// The cleanup pass retires it; the evidence manifest then lists commit.json
// commit_missing and the attempt record unrecorded, while the outcome
// attempt's stdout.log — an artifact object, which cleanup never deletes —
// and run.jsonl are still present, and a fresh export verifies.
func TestIntegration_RunEvidenceReflectsCleanupAsReclaimed(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg, _ := privateCleanupBucket(t, ctx)

	namespace := fmt.Sprintf("%s-evidence-%d-%d", baseNamespace(), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	const orkNamespace = "evidence-e2e"
	temporalClient := cleanupTemporalClient(t, ctx, address, namespace)
	reader := engine.NewTemporalRunReader(temporalClient)

	policies, err := artifacts.NewS3RetentionPolicyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RetentionPolicyStore: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retentionCtrl := archivecleanup.NewRetentionController(policies, reader, namespace, archivecleanup.DefaultRetentionBuffer, logger)
	if _, err := retentionCtrl.SetMax(ctx, 999*time.Hour); err != nil {
		t.Fatalf("SetMax: %v", err)
	}
	const shortRetention = time.Minute
	nsDoc := &orkstackv1.Namespace{Name: orkNamespace, RunRetention: durationpb.New(shortRetention)}
	registry, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	if _, err := registry.Put(ctx, nsDoc, artifacts.PutConditions{IfAbsent: true}); err != nil {
		t.Fatalf("register namespace: %v", err)
	}
	runArchive, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	objectStore, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("artifact store: %v", err)
	}
	commits, err := artifacts.NewS3CommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("commit store: %v", err)
	}
	attempts, err := artifacts.NewS3AttemptStore(ctx, cfg)
	if err != nil {
		t.Fatalf("attempt store: %v", err)
	}
	snapshots, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		t.Fatalf("snapshot store: %v", err)
	}
	reader.WithSnapshotStore(snapshots)
	archiver := engine.NewRunArchiver(runArchive)

	queue := fmt.Sprintf("ork-evidence-e2e-%d", time.Now().UnixNano())
	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	taskWorker := worker.New(temporalClient, queue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	taskWorker.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		attempt := activity.GetInfo(actx).Attempt
		provenance := artifacts.Provenance{RunID: in.GetRunId(), TaskID: in.GetTaskId(), Orkflow: in.GetOrkflowName()}.Metadata(nil)
		report, err := objectStore.Put(actx, "report.txt", strings.NewReader("REPORT "+in.GetRunId()), artifacts.PutOptions{Metadata: provenance})
		if err != nil {
			return nil, err
		}
		stdout, err := objectStore.Put(actx, "stdout.log", strings.NewReader("evidence attempt output\n"), artifacts.PutOptions{Metadata: provenance})
		if err != nil {
			return nil, err
		}
		reportRef := &orkstackv1.ArtifactRef{Name: report.Name, Uri: report.URI, Sha256: report.SHA256, SizeBytes: report.SizeBytes}
		stdoutRef := &orkstackv1.ArtifactRef{Name: stdout.Name, Uri: stdout.URI, Sha256: stdout.SHA256, SizeBytes: stdout.SizeBytes}
		key, err := artifacts.CommitKey(orkNamespace, in.GetRunId(), in.GetTaskId())
		if err != nil {
			return nil, err
		}
		uri := "s3://" + cfg.S3Bucket + "/" + key
		rec, err := commits.Create(actx, uri, &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: orkNamespace, RunId: in.GetRunId(), TaskId: in.GetTaskId(),
			OrkflowName: in.GetOrkflowName(), OrkflowVersion: in.GetOrkflowVersion(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			Artifacts: []*orkstackv1.ArtifactRef{reportRef}, SystemArtifacts: []*orkstackv1.ArtifactRef{stdoutRef}, Attempt: attempt,
		})
		if err != nil {
			return nil, err
		}
		if err := attempts.PutAttempt(actx, &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion, RunId: in.GetRunId(), TaskId: in.GetTaskId(), Attempt: attempt,
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, SystemArtifacts: []*orkstackv1.ArtifactRef{stdoutRef},
		}); err != nil {
			return nil, err
		}
		return &orkstackv1.ExecuteTaskResult{
			TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`),
			Commit:          &orkstackv1.CommitRef{Uri: uri, Sha256: rec.SHA256, Etag: rec.ETag},
			Artifacts:       []*orkstackv1.ArtifactRef{reportRef},
			SystemArtifacts: []*orkstackv1.ArtifactRef{stdoutRef},
		}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(engine.NewEventBus(), engine.NewRunStore(), archiver).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	for name, w := range map[string]worker.Worker{"run": runWorker, "task": taskWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	runID := fmt.Sprintf("ork-evidence-%d", time.Now().UnixNano())
	definition := &orkstackv1.Orkflow{
		Name: "evidence-cleanup", Namespace: orkNamespace, Version: "1.0.0",
		SourceYaml: "name: evidence-cleanup\nversion: 1.0.0\n",
		Tasks: []*orkstackv1.Task{{
			Id: "completed", Uses: "evidence-test", Image: &orkstackv1.ImageRef{Reference: "evidence-test", Pinned: "evidence-test"},
			TimeoutSeconds: 30,
		}},
	}
	digest, err := dsl.DefinitionDigest(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition.DefinitionDigest = digest
	orkflows := engine.NewMemoryOrkflowStore()
	if _, _, err := orkflows.Put(ctx, definition, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
	if err := engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, queue).StartRun(ctx, runID, definition, nil, engine.RunPlan{
		Namespace: orkNamespace, Routes: map[string]string{task.DefaultPool: queue},
	}); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("wait for run: %v", err)
	}
	awaitArchivedRun(t, ctx, runArchive, runID)

	svc := api.NewService(api.Config{
		Reader:        reader,
		Namespaces:    registry,
		Orkflows:      orkflows,
		Archive:       archiver,
		ArtifactStore: objectStore,
		Attempts:      attempts,
		Commits:       commits,
		Logger:        logger,
	})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	defer server.Close()
	client := orkstackv1connect.NewRunServiceClient(server.Client(), server.URL)
	export := func(manifestOnly bool) ([]byte, *evidence.Manifest) {
		t.Helper()
		stream, err := client.ExportRunEvidence(ctx, connect.NewRequest(&orkstackv1.ExportRunEvidenceRequest{
			RunId: runID, Namespace: orkNamespace, ManifestOnly: manifestOnly,
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		var data bytes.Buffer
		last := false
		for stream.Receive() {
			data.Write(stream.Msg().GetData())
			last = last || stream.Msg().GetLast()
		}
		if err := stream.Err(); err != nil || !last {
			t.Fatalf("ExportRunEvidence: %v (last %v)", err, last)
		}
		var m *evidence.Manifest
		if manifestOnly {
			m, err = evidence.ParseManifest(data.Bytes())
		} else {
			m, err = evidence.ReadManifest(bytes.NewReader(data.Bytes()))
		}
		if err != nil {
			t.Fatal(err)
		}
		return data.Bytes(), m
	}
	state := func(m *evidence.Manifest, path string) evidence.Member {
		t.Helper()
		for _, member := range m.Members {
			if member.Path == path {
				return member
			}
		}
		t.Fatalf("no member %s in %v", path, m.Members)
		return evidence.Member{}
	}

	_, before := export(true)
	for _, path := range []string{"run.jsonl", "orkflow/source.yaml", "tasks/completed/commit.json",
		"tasks/completed/attempts/1.json", "tasks/completed/attempts/1/stdout.log", "tasks/completed/artifacts/report.txt"} {
		if m := state(before, path); m.State != evidence.StatePresent {
			t.Fatalf("before cleanup %s is %s", path, m.State)
		}
	}

	cleaner := archivecleanup.NewCleaner(archivecleanup.CleanupConfig{
		TemporalNamespace: namespace, Interval: time.Hour, RetentionBuffer: archivecleanup.DefaultRetentionBuffer, BatchSize: 100,
	}, runArchive, reader, staticNamespaceLister{docs: []*orkstackv1.Namespace{nsDoc}}, retentionCtrl, logger).WithClock(func() time.Time {
		return time.Now().UTC().Add(2 * shortRetention)
	})
	stats, err := cleaner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsCleaned != 1 {
		t.Fatalf("cleanup metrics = %+v, want the one run retired", stats)
	}
	awaitWorkflowGone(t, ctx, reader, namespace, runID)

	_, after := export(true)
	commit := state(after, "tasks/completed/commit.json")
	record := state(after, "tasks/completed/attempts/1.json")
	stdout := state(after, "tasks/completed/attempts/1/stdout.log")
	runBody := state(after, "run.jsonl")
	if commit.State != evidence.StateReclaimed || commit.Reason != evidence.ReasonCommitMissing ||
		commit.SHA256 != state(before, "tasks/completed/commit.json").SHA256 {
		t.Fatalf("commit after cleanup = %+v", commit)
	}
	if record.State != evidence.StateUnrecorded || record.Reason != evidence.ReasonAttemptRecordMissing {
		t.Fatalf("attempt record after cleanup = %+v", record)
	}
	if stdout.State != evidence.StatePresent || runBody.State != evidence.StatePresent {
		t.Fatalf("stdout.log %s, run.jsonl %s after cleanup; both must stay present", stdout.State, runBody.State)
	}
	bundle, _ := export(false)
	report, err := evidence.Verify(bytes.NewReader(bundle))
	if err != nil || len(report.Failures) != 0 {
		t.Fatalf("verify of the post-cleanup bundle: %v %+v", err, report)
	}
	t.Logf("EVIDENCE run_evidence_after_cleanup run=%s commit.json=%s(%s sha256 %s) attempts/1.json=%s(%s) attempts/1/stdout.log=%s run.jsonl=%s verified=%d reclaimed=%d unrecorded=%d",
		runID, commit.State, commit.Reason, commit.SHA256, record.State, record.Reason, stdout.State, runBody.State,
		len(report.Verified), len(report.Reclaimed), len(report.Unrecorded))
}
