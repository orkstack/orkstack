//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
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
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_ProductionSubmitPathCommitsDurablyAndAdoptsOnRetry is the
// end-to-end proof that the durable commit protocol engages on a REAL run.
//
// Before this test the three RunPlan durability fields were populated only by
// test harnesses: internal/engine/pools.go built `RunPlan{Namespace, Routes}`
// and nothing on the production submit path ever set CommitRoot, Limits or
// TransferBudgetSeconds. Every real activity therefore received an empty
// commit_uri, so §5.4's protocol — which Phase 2 implemented and proved in
// isolation — never ran outside tests.
//
// Everything below the orkflow store is production code: internal/api resolves
// the plan, the real RunWorkflow composes the URI, and a real OCIDispatcher
// runs a real container against a real Docker daemon and a real MinIO bucket.
// Only the stored definition is written directly rather than through
// ApplyOrkflow, because apply-time registry resolution of `uses:` into
// Task.image is Phase 3 (§4.3) and does not exist yet.
func TestIntegration_ProductionSubmitPathCommitsDurablyAndAdoptsOnRetry(t *testing.T) {
	cfg := s3Config()
	requireMinIO(t, cfg)
	requireDockerForCommitE2E(t)

	orkflows := newDurableOrkflowStore(t)
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	namespace := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if namespace == "" {
		namespace = task.DefaultNamespace
	}
	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort: address, Namespace: namespace,
	})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()
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

	// The real Orker: real Docker runtime, real commit store, real bucket.
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Logs:    logStore,
		Commits: commitStore,
		Refs:    refs,
		Pool:    task.DefaultPool,
		OrkerID: "commit-e2e-" + token,
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
		defer w.Stop()
	}

	// The definition, with its image already pinned to a digest. Apply-time
	// resolution of `uses:` is Phase 3; the wire field it fills is Phase 1's,
	// and this is what it will contain.
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
	defer func() {
		transport.CloseIdleConnections()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()
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

	// (a) The activity received a non-empty, correctly composed commit URI.
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
	wantURI := "s3://" + cfg.S3Bucket + "/" + key
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
	before := runningContainerCountForCommitE2E(t)
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
	if after := runningContainerCountForCommitE2E(t); after != before {
		t.Errorf("running containers went %d -> %d across an adopted attempt; adoption must start none",
			before, after)
	}
	t.Logf("EVIDENCE commit_adopted=%t committing_attempt=%d exit_code=%d",
		adopted.GetCommitAdopted(), adopted.GetAttempt(), adopted.GetExitCode())
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

// bindableWorkRootForCommitE2E returns a directory the daemon can bind-mount.
// t.TempDir() is /var/folders on macOS, which a VM daemon does not share.
func bindableWorkRootForCommitE2E(t *testing.T) string {
	t.Helper()
	if root := os.Getenv(orker.WorkRootVar); root != "" {
		return root
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".data", "work"))
	if err != nil {
		t.Fatalf("resolve work root: %v", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create work root %s: %v", root, err)
	}
	return root
}

func runningContainerCountForCommitE2E(t *testing.T) int {
	t.Helper()
	out, err := exec.Command("docker", "ps", "--quiet").Output()
	if err != nil {
		t.Fatalf("docker ps: %v", err)
	}
	return len(strings.Fields(string(out)))
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
