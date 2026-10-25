//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_ProductionSubmitPathCommitsDurablyAndAdoptsOnRetry is the
// end-to-end proof that the durable commit protocol engages on a REAL run.
//
// It guards the production submit path's RunPlan durability fields
// (CommitRoot, Limits, TransferBudgetSeconds): without them every real
// activity would receive an empty commit_uri and the §5.4 protocol would never
// run outside tests.
//
// Everything below the orkflow store is production code: internal/api resolves
// the plan, the real RunWorkflow composes the URI, and a real OCIDispatcher
// runs a real container against a real Docker daemon and a real MinIO bucket.
// Only the stored definition is written directly rather than through
// ApplyOrkflow, so the test needs no registry to resolve `uses:` into
// Task.image (§4.3).
func TestIntegration_ProductionSubmitPathCommitsDurablyAndAdoptsOnRetry(t *testing.T) {
	e := startCommitE2E(t)
	ctx, cfg, in, key, wantURI, run := e.ctx, e.cfg, e.in, e.key, e.wantURI, e.run
	activities, attempts, commitStore := e.activities, e.attempts, e.commitStore

	// (a) The activity received a non-empty, correctly composed commit URI.
	if in.GetCommitUri() != wantURI {
		t.Fatalf("commit_uri = %q, want %q — the production submit path did not pin CommitRoot",
			in.GetCommitUri(), wantURI)
	}
	if in.GetLimits().GetCommitMaxBytes() != engine.DefaultCommitMaxBytes {
		t.Errorf("limits.commit_max_bytes = %d, want the deployment's %d",
			in.GetLimits().GetCommitMaxBytes(), engine.DefaultCommitMaxBytes)
	}
	t.Logf("EVIDENCE commit_uri=%s", in.GetCommitUri())

	// (b) A commit.json object really exists in MinIO at the composed key.
	raw := newRawS3ClientForCommitE2E(t, cfg)
	head, err := raw.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("no object in MinIO at %s: %v", key, err)
	}
	t.Logf("EVIDENCE minio_object bucket=%s key=%s bytes=%d etag=%s",
		cfg.S3Bucket, key, aws.ToInt64(head.ContentLength), aws.ToString(head.ETag))

	taskRun := run.GetTaskRuns()[0]
	if taskRun.GetCommit().GetUri() != wantURI {
		t.Errorf("TaskRun.commit.uri = %q, want %q", taskRun.GetCommit().GetUri(), wantURI)
	}

	// (c) A replacement attempt ADOPTS that commit instead of re-running the
	//     container. This is the invariant the empty URI silently disabled:
	//     "a durably committed activity result cannot become pending again".
	before := runningContainerCountForCommitE2E(t, e.orkerID)
	adopted, err := activities.ExecuteTask(ctx, in)
	if err != nil {
		t.Fatalf("replacement attempt: %v", err)
	}
	if !adopted.GetCommitAdopted() {
		t.Fatal("the replacement attempt re-ran the container instead of adopting the durable commit")
	}
	if adopted.GetCommit().GetUri() != wantURI {
		t.Errorf("adopted commit uri = %q, want %q", adopted.GetCommit().GetUri(), wantURI)
	}
	if after := runningContainerCountForCommitE2E(t, e.orkerID); after != before {
		t.Errorf("running containers went %d -> %d across an adopted attempt; adoption must start none",
			before, after)
	}
	t.Logf("EVIDENCE commit_adopted=%t committing_attempt=%d exit_code=%d",
		adopted.GetCommitAdopted(), adopted.GetAttempt(), adopted.GetExitCode())

	// (d) Task timing (docs/UI_CLI_PARITY.md "Task timing"): the adopted attempt's record lists
	//     exactly the phases it passed through — it returned at the commit
	//     check, so [start, commit-check] — and the snapshot's started_at is
	//     the COMMITTING attempt's start, read back out of commit.json.
	adoptedRecord := attempts.last()
	if adoptedRecord == nil || !adoptedRecord.GetCommitAdopted() {
		t.Fatalf("no adopted attempt record was written: %v", adoptedRecord)
	}
	var phaseNames []string
	for _, span := range adoptedRecord.GetPhases() {
		phaseNames = append(phaseNames, span.GetPhase())
	}
	if strings.Join(phaseNames, ",") != "start,commit-check" {
		t.Errorf("adopted attempt phases = %v, want [start commit-check]", phaseNames)
	}
	commitRecord, err := commitStore.Get(ctx, wantURI)
	if err != nil {
		t.Fatalf("read commit.json: %v", err)
	}
	commitStart := commitRecord.Commit.GetStartedAt()
	if commitStart == nil || taskRun.GetStartedAt() == nil ||
		!taskRun.GetStartedAt().AsTime().Equal(commitStart.AsTime()) {
		t.Errorf("TaskRun.started_at = %v, want commit.started_at %v", taskRun.GetStartedAt(), commitStart)
	}
	if !adopted.GetStartedAt().AsTime().Equal(commitStart.AsTime()) {
		t.Errorf("the adopted result's started_at = %v, want the committing attempt's %v",
			adopted.GetStartedAt(), commitStart)
	}
	t.Logf("EVIDENCE I3 adopted_phases=%v snapshot_started_at=%s commit_started_at=%s",
		phaseNames, taskRun.GetStartedAt().AsTime().Format(time.RFC3339Nano),
		commitStart.AsTime().Format(time.RFC3339Nano))
}

// TestIntegration_RunEvidenceCarriesAnAdoptedCommitHonestly: the evidence bundle of a run whose task was adopted
// says so, instead of hiding the adoption behind one attempt.
//
// The run is the production one of startCommitE2E: attempt 1 commits through
// the real OCIDispatcher. The replacement attempt is then numbered the way
// Temporal numbers a retry — a probe workflow's attempt 1 stands in for the
// committing attempt that died after commit.json, and its attempt 2 runs the
// real dispatcher against the same input, which must adopt the durable
// commit and file its own record under attempt 2. The run is archived the
// way the projection archives it and the bundle is read through the
// production handler over the real MinIO stores.
func TestIntegration_RunEvidenceCarriesAnAdoptedCommitHonestly(t *testing.T) {
	e := startCommitE2E(t)
	ctx := e.ctx
	taskRun := e.run.GetTaskRuns()[0]
	if taskRun.GetCommit().GetUri() != e.wantURI {
		t.Fatalf("TaskRun.commit.uri = %q, want %q", taskRun.GetCommit().GetUri(), e.wantURI)
	}

	var suite testsuite.WorkflowTestSuite
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(2 * time.Minute)
	env.RegisterWorkflow(evidenceAdoptionProbeWorkflow)
	env.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if activity.GetInfo(actx).Attempt < 2 {
			return nil, temporal.NewApplicationError("stand-in for the committing attempt that died after commit.json", "PROBE")
		}
		return e.activities.ExecuteTask(actx, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	before := runningContainerCountForCommitE2E(t, e.orkerID)
	env.ExecuteWorkflow(evidenceAdoptionProbeWorkflow, e.in)
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("replacement attempt: completed=%t err=%v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	var adopted *orkstackv1.ExecuteTaskResult
	if err := env.GetWorkflowResult(&adopted); err != nil {
		t.Fatalf("replacement attempt result: %v", err)
	}
	if after := runningContainerCountForCommitE2E(t, e.orkerID); after != before {
		t.Fatalf("running containers %d -> %d across the replacement attempt; adoption starts none", before, after)
	}
	if !adopted.GetCommitAdopted() || adopted.GetAttempt() != taskRun.GetAttempt() {
		t.Fatalf("the replacement attempt did not adopt attempt %d's commit: adopted=%t attempt=%d",
			taskRun.GetAttempt(), adopted.GetCommitAdopted(), adopted.GetAttempt())
	}
	adoptedRecord := e.attempts.last()
	if adoptedRecord == nil || !adoptedRecord.GetCommitAdopted() || adoptedRecord.GetAttempt() != 2 {
		t.Fatalf("the adopting attempt's record = %v, want attempt 2 with commit_adopted", adoptedRecord)
	}

	runArchive, err := artifacts.NewS3RunArchive(ctx, e.cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	archiver := engine.NewRunArchiver(runArchive)
	if err := archiver.Archive(ctx, e.run, nil); err != nil {
		t.Fatalf("archive run: %v", err)
	}
	evidenceSvc := api.NewService(api.Config{
		Orkflows:      e.orkflows,
		Runs:          e.runs,
		Bus:           e.bus,
		Archive:       archiver,
		ArtifactStore: e.store,
		Attempts:      e.attemptStore,
		Commits:       e.commitStore,
	})
	evidenceServer := httptest.NewServer(api.NewMux(evidenceSvc, api.MuxOptions{}))
	defer evidenceServer.Close()
	exported, err := orkstackv1connect.NewRunServiceClient(evidenceServer.Client(), evidenceServer.URL).ExportRunEvidence(ctx,
		connect.NewRequest(&orkstackv1.ExportRunEvidenceRequest{RunId: e.runID}))
	if err != nil {
		t.Fatalf("ExportRunEvidence: %v", err)
	}
	var bundle bytes.Buffer
	last := false
	for exported.Receive() {
		bundle.Write(exported.Msg().GetData())
		last = last || exported.Msg().GetLast()
	}
	if err := exported.Err(); err != nil || !last {
		t.Fatalf("ExportRunEvidence stream: %v (last %v)", err, last)
	}
	_ = exported.Close()
	report, err := evidence.Verify(bytes.NewReader(bundle.Bytes()))
	if err != nil || len(report.Failures) != 0 {
		t.Fatalf("verify the adoption bundle: %v %+v", err, report)
	}
	manifest := report.Manifest
	if len(manifest.Tasks) != 1 {
		t.Fatalf("manifest tasks = %+v", manifest.Tasks)
	}
	entry := manifest.Tasks[0]
	if entry.OutcomeAttempt != taskRun.GetAttempt() || entry.OutcomeAttempt != adopted.GetAttempt() {
		t.Fatalf("outcome_attempt = %d, want the committing attempt %d (adopted result says %d)",
			entry.OutcomeAttempt, taskRun.GetAttempt(), adopted.GetAttempt())
	}
	rows := map[int32]evidence.AttemptEntry{}
	for _, a := range entry.Attempts {
		rows[a.Attempt] = a
	}
	committing, adopter := rows[entry.OutcomeAttempt], rows[2]
	if committing.State != evidence.StatePresent || committing.CommitAdopted || committing.Outcome != "SUCCEEDED" {
		t.Fatalf("committing attempt row = %+v, want present, SUCCEEDED, not adopted", committing)
	}
	if adopter.State != evidence.StatePresent || !adopter.CommitAdopted {
		t.Fatalf("adopting attempt row = %+v, want present with commit_adopted (all rows %+v)", adopter, entry.Attempts)
	}
	members := map[string]evidence.Member{}
	for _, m := range manifest.Members {
		members[m.Path] = m
	}
	commitMember := members["tasks/build/commit.json"]
	if commitMember.State != evidence.StatePresent || commitMember.SHA256 != strings.ToLower(taskRun.GetCommit().GetSha256()) {
		t.Fatalf("commit.json member = %+v, want present with the snapshot's CommitRef.sha256 %s", commitMember, taskRun.GetCommit().GetSha256())
	}
	for _, stream := range []string{"stdout.log", "stderr.log"} {
		mine := members[fmt.Sprintf("tasks/build/attempts/%d/%s", entry.OutcomeAttempt, stream)]
		theirs := members["tasks/build/attempts/2/"+stream]
		if mine.SHA256 == "" || mine.SHA256 != theirs.SHA256 {
			t.Fatalf("%s: committing attempt %+v, adopter %+v — the adopter's record names the committing attempt's logs",
				stream, mine, theirs)
		}
	}
	t.Logf("EVIDENCE I4 run_evidence_adoption outcome_attempt=%d attempts=%+v commit.json sha256=%s snapshot CommitRef.sha256=%s",
		entry.OutcomeAttempt, entry.Attempts, commitMember.SHA256, taskRun.GetCommit().GetSha256())
}

// evidenceAdoptionProbeWorkflow schedules one ExecuteTask with two attempts,
// so the replacement attempt carries the number Temporal gives a retry.
func evidenceAdoptionProbeWorkflow(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2, InitialInterval: time.Millisecond},
	})
	var result *orkstackv1.ExecuteTaskResult
	err := workflow.ExecuteActivity(ctx, task.ActivityExecuteTask, in).Get(ctx, &result)
	return result, err
}

// startCommitE2E brings up the production submit path of
// TestIntegration_ProductionSubmitPathCommitsDurablyAndAdoptsOnRetry — real
// Temporal, real MinIO, a real OCIDispatcher — runs the one-task probe
// Orkflow to SUCCEEDED and hands back everything a scenario reads after it.
// Every worker, server and client it starts is stopped by t.Cleanup.
func startCommitE2E(t *testing.T) *commitE2E {
	t.Helper()
	cfg := s3Config()
	requireObjectStore(t, cfg)
	requireDockerForCommitE2E(t)

	orkflows := newDurableOrkflowStore(t)
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

	namespace := privateNamespaceForCommitE2E(t)
	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort: address, Namespace: namespace,
	})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	t.Cleanup(temporalClient.Close)
	if err := task.EnsureNamespace(ctx, temporalClient, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, namespace)

	// A private execution channel, so this test never takes work from — or
	// loses work to — another suite or a stray Orker (AGENTS.md §11.1).
	token := fmt.Sprintf("%d", time.Now().UnixNano())
	orkerQueue := "ork-commit-e2e-" + token

	store, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("open artifact store: %v", err)
	}
	commitStore, err := artifacts.NewCommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("open commit store: %v", err)
	}
	if err := artifacts.ProbeCommitStore(ctx, commitStore); err != nil {
		infraAbsent(t, "the object store's conditional create is not usable: %v", err)
	}
	logStore, err := artifacts.NewLogStore(cfg)
	if err != nil {
		t.Fatalf("open log store: %v", err)
	}
	refs, ok := store.(artifacts.RefPredictor)
	if !ok {
		t.Fatalf("artifact backend %T cannot predict content-addressed references", store)
	}
	attemptStore, err := artifacts.NewAttemptStore(cfg)
	if err != nil {
		t.Fatalf("open attempt store: %v", err)
	}
	// A tap on the real attempt store: every record still lands in MinIO, and
	// the test also keeps the last one written, which is how it reads the
	// adopted attempt's record below without it being overwritten by key.
	attempts := &tappedAttemptStore{AttemptStore: attemptStore}

	// The real Orker: real Docker runtime, real commit store, real bucket.
	orkerID := proof.OrkerID("commit-e2e-" + token)
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Logs:     logStore,
		Commits:  commitStore,
		Attempts: attempts,
		Refs:     refs,
		Pool:     task.DefaultPool,
		OrkerID:  orkerID,
		// ORK_ORKER_WORKDIR must be bind-mountable by the daemon; on macOS
		// /var/folders is not shared with a VM daemon (§3.1).
		WorkRoot: bindableWorkRootForCommitE2E(t),
	})
	if err := activities.Boot(ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}

	// A tap on the production activity: it records exactly what the workflow
	// sent, then delegates to the real dispatcher. Nothing is simulated.
	var sentMu sync.Mutex
	sent := map[string]*orkstackv1.ExecuteTaskInput{}

	bus, runs := engine.NewEventBus(), engine.NewRunStore()

	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	taskWorker := worker.New(temporalClient, orkerQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	taskWorker.RegisterActivityWithOptions(
		func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			sentMu.Lock()
			sent[in.GetTaskId()] = in
			sentMu.Unlock()
			return activities.ExecuteTask(actx, in)
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	taskWorker.RegisterActivityWithOptions(
		activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker},
	)

	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	for name, w := range map[string]worker.Worker{
		"run": runWorker, "task": taskWorker, "event": eventWorker,
	} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		t.Cleanup(w.Stop)
	}

	// The definition, with its image already pinned to a digest: exactly what
	// apply-time resolution of `uses:` writes into Task.image.
	pinned := pinnedAlpineForCommitE2E(t)
	orkflowName := "commit-e2e-probe"
	definition := &orkstackv1.Orkflow{
		Name:    orkflowName,
		Version: "1.0." + token,
		Tasks: []*orkstackv1.Task{{
			Id:             "build",
			Uses:           pinned,
			Image:          &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
			Command:        []string{"/bin/sh", "-c", `printf '{"ok":true}' > /orkstack/output/values.json`},
			TimeoutSeconds: 120,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}},
	}
	if _, _, err := orkflows.Put(ctx, definition, artifacts.HeadExpectation{}); err != nil {
		t.Fatalf("store the pinned definition: %v", err)
	}

	// The production API, configured the way cmd/ork configures it.
	svc := api.NewService(api.Config{
		Orkflows:  orkflows,
		Runs:      runs,
		Bus:       bus,
		Starter:   engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, orkerQueue),
		RawQueue:  orkerQueue,
		Artifacts: cfg,
		Limits:    engine.DefaultLimits(),
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(svc, api.MuxOptions{}))
	go func() { _ = server.Serve(listener) }()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	})
	runClient := orkstackv1connect.NewRunServiceClient(
		&http.Client{Transport: transport}, "http://"+listener.Addr().String())

	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName:    orkflowName,
		OrkflowVersion: definition.GetVersion(),
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	t.Logf("EVIDENCE run_id=%s", runID)

	// Drain the event stream rather than sleeping: it closes when the run ends.
	stream, err := runClient.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	for stream.Receive() {
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream: %v", err)
	}
	stream.Close()

	got, err := runClient.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	run := got.Msg.GetRun()
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run status = %v, want SUCCEEDED (error: %v)", run.GetStatus(), run.GetError())
	}

	sentMu.Lock()
	in := sent["build"]
	sentMu.Unlock()
	if in == nil {
		t.Fatal("the build task never reached the Orker")
	}
	key, err := artifacts.CommitKey("default", runID, "build")
	if err != nil {
		t.Fatal(err)
	}
	return &commitE2E{
		ctx: ctx, cfg: cfg, orkflows: orkflows, store: store, commitStore: commitStore,
		attemptStore: attemptStore, attempts: attempts, activities: activities,
		bus: bus, runs: runs, runClient: runClient,
		runID: runID, run: run, in: in, key: key,
		wantURI: "s3://" + cfg.S3Bucket + "/" + key,
		orkerID: orkerID,
	}
}

// commitE2E is what startCommitE2E leaves running and what it observed.
type commitE2E struct {
	// orkerID labels every container this test's Orker starts (ork.orker).
	orkerID      string
	ctx          context.Context
	cfg          artifacts.Config
	orkflows     *artifacts.S3OrkflowStore
	store        artifacts.Store
	commitStore  artifacts.CommitStore
	attemptStore artifacts.AttemptStore
	attempts     *tappedAttemptStore
	activities   *orker.Activities
	bus          *engine.EventBus
	runs         *engine.RunStore
	runClient    orkstackv1connect.RunServiceClient
	runID        string
	run          *orkstackv1.Run
	// in is the ExecuteTaskInput the workflow sent for "build": a replacement
	// attempt must be the SAME input.
	in      *orkstackv1.ExecuteTaskInput
	key     string
	wantURI string
}

// tappedAttemptStore delegates to a real AttemptStore and remembers the last
// record written.
type tappedAttemptStore struct {
	artifacts.AttemptStore
	mu     sync.Mutex
	latest *orkstackv1.TaskAttempt
}

func (s *tappedAttemptStore) PutAttempt(ctx context.Context, record *orkstackv1.TaskAttempt) error {
	s.mu.Lock()
	s.latest = record
	s.mu.Unlock()
	return s.AttemptStore.PutAttempt(ctx, record)
}

func (s *tappedAttemptStore) last() *orkstackv1.TaskAttempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

// privateNamespaceForCommitE2E names a Temporal namespace unique to this test
// run.
//
// The run and event workers below poll the shared task.DefaultQueue and
// engine.EventQueue exactly as `ork` does, and neither queue name is
// test-configurable. Temporal task queues are namespace-scoped, so without a
// private namespace a dev stack (or another test process) polling the
// operator's actual namespace would steal this run's workflow or event tasks
// and reproduce "engine: publish event: read snapshot: artifact not found"
// against a snapshot store this test never wrote to (AGENTS.md §11.1). The
// operator's ORK_TEMPORAL_NAMESPACE, if set, stays as the prefix so the
// choice is still visible in `temporal workflow list`.
func privateNamespaceForCommitE2E(t *testing.T) string {
	t.Helper()
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	return fmt.Sprintf("%s-e2e-%d-%d", base, os.Getpid(), time.Now().UnixNano())
}

// awaitNamespaceReadyForCommitE2E blocks until a namespace-scoped call stops
// reporting NotFound.
//
// RegisterNamespace returns before the frontend's namespace cache has the
// entry, so DescribeNamespace can succeed while StartWorkflow still fails with
// "Namespace ... is not found". This polls the operation that actually has to
// work, against a deadline — never a fixed sleep.
func awaitNamespaceReadyForCommitE2E(
	t *testing.T,
	ctx context.Context,
	temporalClient client.Client,
	namespace string,
) {
	t.Helper()
	if namespace == "" || namespace == client.DefaultNamespace {
		return
	}
	deadline, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	var last error
	for {
		_, last = temporalClient.CountWorkflow(deadline, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: namespace,
		})
		if last == nil {
			return
		}
		select {
		case <-deadline.Done():
			infraAbsent(t, "temporal namespace %q never became usable: %v", namespace, last)
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func requireDockerForCommitE2E(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "the docker CLI is not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}").CombinedOutput(); err != nil {
		infraAbsent(t, "the docker daemon is not reachable: %v: %s", err, out)
	}
}

// pinnedAlpineForCommitE2E resolves alpine:3.20 to the digest form the
// dispatcher accepts. The dispatcher refuses a tag outright: a running
// execution must never silently switch images because a tag moved.
func pinnedAlpineForCommitE2E(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const tag = "alpine:3.20"
	if _, err := exec.CommandContext(ctx, "docker", "image", "inspect", tag).CombinedOutput(); err != nil {
		if out, pullErr := exec.CommandContext(ctx, "docker", "pull", "--quiet", tag).CombinedOutput(); pullErr != nil {
			infraAbsent(t, "cannot pull %s: %v: %s", tag, pullErr, out)
		}
	}
	if out, err := exec.CommandContext(ctx, "docker", "image", "inspect",
		"--format", "{{index .RepoDigests 0}}", tag).Output(); err == nil {
		return strings.TrimSpace(string(out))
	}
	id, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", tag).Output()
	if err != nil {
		infraAbsent(t, "cannot resolve a digest for %s: %v", tag, err)
	}
	return tag + "@" + strings.TrimSpace(string(id))
}

// bindableWorkRootForCommitE2E returns this test's own Orker work root: a
// fresh directory under ORK_ORKER_WORKDIR, or under the repository's
// .data/work, that the daemon can bind-mount (t.TempDir() is /var/folders on
// macOS, which a VM daemon does not share).
//
// Never that directory itself. An Orker's Boot reaps every pin, spool and
// stage directory under its root, so two Orkers on one root — two tests run
// concurrently, or a test and the dev stack, which uses .data/work — delete
// each other's in-flight work (AGENTS.md §11.1). One test gets one root, so a
// test that boots its Orker twice still finds its own predecessor's state.
func bindableWorkRootForCommitE2E(t *testing.T) string {
	t.Helper()
	commitE2ERootsMu.Lock()
	defer commitE2ERootsMu.Unlock()
	if root, ok := commitE2ERoots[t.Name()]; ok {
		return root
	}
	parent := os.Getenv(orker.WorkRootVar)
	if parent == "" {
		abs, err := filepath.Abs(filepath.Join("..", "..", ".data", "work"))
		if err != nil {
			t.Fatalf("resolve work root: %v", err)
		}
		parent = abs
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatalf("create work root parent %s: %v", parent, err)
	}
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			return r
		default:
			return '-'
		}
	}, t.Name())
	if len(safe) > 80 {
		safe = safe[:80]
	}
	root, err := os.MkdirTemp(parent, proof.WorkRootPattern(safe))
	if err != nil {
		t.Fatalf("create work root under %s: %v", parent, err)
	}
	commitE2ERoots[t.Name()] = root
	name := t.Name()
	t.Cleanup(func() {
		commitE2ERootsMu.Lock()
		delete(commitE2ERoots, name)
		commitE2ERootsMu.Unlock()
		// Best effort: a container may leave files the test user cannot
		// remove; scripts/proof-clean.sh lists what is left.
		_ = os.RemoveAll(root)
	})
	return root
}

var (
	commitE2ERootsMu sync.Mutex
	commitE2ERoots   = map[string]string{}
)

// runningContainerCountForCommitE2E counts the running containers THIS test's
// Orker started — by its ork.orker label, never host-wide: other tests and
// other proofs start and stop containers on the same daemon concurrently
// (AGENTS.md §11.1).
func runningContainerCountForCommitE2E(t *testing.T, orkerID string) int {
	t.Helper()
	out, err := exec.Command("docker", "ps", "--quiet", "--filter", "label=ork.orker="+orkerID).Output()
	if err != nil {
		t.Fatalf("docker ps: %v", err)
	}
	return len(strings.Fields(string(out)))
}

// awaitRunningContainersForCommitE2E waits, until the deadline, for exactly
// want of THIS test's Orker's containers to be running. A task reaches the
// Orker before its container starts — staging, image resolution and
// `docker create` come first — so a count taken the moment the task arrived
// can read 0 on a loaded host; the deadline, not the moment, is the bound.
func awaitRunningContainersForCommitE2E(t *testing.T, ctx context.Context, orkerID string, want int, within time.Duration) {
	t.Helper()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		n := runningContainerCountForCommitE2E(t, orkerID)
		if n == want {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("Orker %s has %d containers running after %s, want %d", orkerID, n, within, want)
		case <-ctx.Done():
			t.Fatalf("Orker %s: %v while waiting for %d running containers (have %d)", orkerID, ctx.Err(), want, n)
		}
	}
}

func newRawS3ClientForCommitE2E(t *testing.T, cfg artifacts.Config) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	})
}
