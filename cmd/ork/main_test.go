package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestLoadConfigThreadsTemporalNamespaceIntoCleanup(t *testing.T) {
	t.Setenv("ORK_TEMPORAL_NAMESPACE", "temporal-runtime")
	cfg := loadConfig()
	if cfg.cleanup.TemporalNamespace != "temporal-runtime" {
		t.Fatalf("cleanup Temporal namespace = %q, want temporal-runtime", cfg.cleanup.TemporalNamespace)
	}
}

func TestLoadConfigReadsRunPlanDurabilitySettings(t *testing.T) {
	t.Setenv("ORK_VALUES_MAX_BYTES", "131072")
	t.Setenv("ORK_ARTIFACT_MAX_COUNT", "12")
	t.Setenv("ORK_ARTIFACT_MAX_BYTES", "1048576")
	t.Setenv("ORK_LOG_MAX_BYTES", "8192")
	t.Setenv("ORK_COMMIT_MAX_BYTES", "32768")
	t.Setenv("ORK_TRANSFER_BUDGET_SECONDS", "901")
	t.Setenv("ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC", "20971520")

	cfg := loadConfig()
	want := engine.WorkspaceLimits{
		ValuesMaxBytes: 131072, ArtifactMaxCount: 12, ArtifactMaxBytes: 1048576,
		LogMaxBytes: 8192, CommitMaxBytes: 32768,
	}
	if cfg.contractLimits != want {
		t.Errorf("contract limits = %+v, want %+v", cfg.contractLimits, want)
	}
	if cfg.transferBudgetSeconds != 901 {
		t.Errorf("transfer budget = %d, want 901", cfg.transferBudgetSeconds)
	}
	if cfg.floorThroughputBytesPerSecond != 20971520 {
		t.Errorf("floor throughput = %d, want 20971520", cfg.floorThroughputBytesPerSecond)
	}
}

func TestLoadConfigUsesDocumentedRunPlanDefaults(t *testing.T) {
	for _, key := range []string{
		"ORK_VALUES_MAX_BYTES", "ORK_ARTIFACT_MAX_COUNT", "ORK_ARTIFACT_MAX_BYTES",
		"ORK_LOG_MAX_BYTES", "ORK_COMMIT_MAX_BYTES", "ORK_TRANSFER_BUDGET_SECONDS",
		"ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC",
	} {
		t.Setenv(key, "")
	}
	cfg := loadConfig()
	if cfg.contractLimits != engine.DefaultLimits() {
		t.Errorf("default limits = %+v, want %+v", cfg.contractLimits, engine.DefaultLimits())
	}
	if cfg.transferBudgetSeconds != 0 {
		t.Errorf("default explicit transfer budget = %d, want derived (zero)", cfg.transferBudgetSeconds)
	}
	if cfg.floorThroughputBytesPerSecond != api.DefaultFloorThroughputBytesPerSecond {
		t.Errorf("default floor throughput = %d, want %d", cfg.floorThroughputBytesPerSecond, api.DefaultFloorThroughputBytesPerSecond)
	}
}

func TestLoadConfigAcceptsExplicitZeroForDerivedLimits(t *testing.T) {
	t.Setenv("ORK_TRANSFER_BUDGET_SECONDS", "0")
	t.Setenv("ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC", "0")

	cfg := loadConfig()
	if cfg.limitsErr != nil {
		t.Fatalf("explicit derive values refused at boot: %v", cfg.limitsErr)
	}
	if cfg.transferBudgetSeconds != 0 {
		t.Errorf("transfer budget = %d, want derived (zero)", cfg.transferBudgetSeconds)
	}
	if cfg.floorThroughputBytesPerSecond != 0 {
		t.Errorf("floor throughput = %d, want derived (zero)", cfg.floorThroughputBytesPerSecond)
	}
}

func TestLoadConfigRefusesInvalidDerivedLimitsAtBoot(t *testing.T) {
	for _, tc := range []struct {
		key   string
		value string
	}{
		{key: "ORK_TRANSFER_BUDGET_SECONDS", value: "1MB"},
		{key: "ORK_TRANSFER_BUDGET_SECONDS", value: "-1"},
		{key: "ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC", value: "1MB"},
		{key: "ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC", value: "-1"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			cfg := loadConfig()
			if cfg.limitsErr == nil {
				t.Fatalf("%s=%q silently fell back to a default", tc.key, tc.value)
			}
			if message := cfg.limitsErr.Error(); !strings.Contains(message, tc.key) || !strings.Contains(message, tc.value) {
				t.Errorf("boot error %q must name %s and its rejected value %q", message, tc.key, tc.value)
			}
		})
	}
}

func TestLoadConfigRefusesInvalidRunPlanLimitsAtBoot(t *testing.T) {
	for _, tc := range []struct {
		key   string
		value string
	}{
		{key: "ORK_VALUES_MAX_BYTES", value: "1MB"},
		{key: "ORK_ARTIFACT_MAX_BYTES", value: "-1"},
		{key: "ORK_LOG_MAX_BYTES", value: "0"},
		{key: "ORK_ARTIFACT_MAX_COUNT", value: "2147483648"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)

			cfg := loadConfig()
			if cfg.limitsErr == nil {
				t.Fatalf("%s=%q silently fell back to a default", tc.key, tc.value)
			}
			if message := cfg.limitsErr.Error(); !strings.Contains(message, tc.key) || !strings.Contains(message, tc.value) {
				t.Errorf("boot error %q must name %s and its rejected value %q", message, tc.key, tc.value)
			}
			if err := run(nil, telemetry.Config{}, nil, cfg); err == nil {
				t.Fatalf("engine booted with %s=%q", tc.key, tc.value)
			}
		})
	}
}

func TestWorkerOptions_KeepSharedQueueEngineFromExecutingOrkerActivities(t *testing.T) {
	runOptions := runWorkerOptions(config{})
	if !runOptions.LocalActivityWorkerOnly {
		t.Fatal("run worker must be workflow/local-activity only on the shared Orker queue")
	}
	if runOptions.DisableWorkflowWorker {
		t.Fatal("run worker must execute RunWorkflow")
	}
	if !runOptions.DisableRegistrationAliasing {
		t.Fatal("run worker must disable registration aliases")
	}

	eventOptions := eventWorkerOptions(config{})
	if !eventOptions.DisableWorkflowWorker {
		t.Fatal("event worker must be activity-only")
	}
	if eventOptions.LocalActivityWorkerOnly {
		t.Fatal("event worker must execute the PublishEvent regular activity")
	}
	if !eventOptions.DisableRegistrationAliasing {
		t.Fatal("event worker must disable registration aliases")
	}
}

// TestQueues_OrkerQueueMovesTaskExecutionOnly holds the runbook to what the
// engine really polls.
//
// ORK_ORKER_QUEUE names where task activities are DISPATCHED. RunWorkflow and
// the idempotency claims stay on task.DefaultQueue whatever it is set to. With
// the variable unset the two queues are the same name, which is how
// docs/DEPLOY.md came to claim the run worker polls ORK_ORKER_QUEUE — true
// only by coincidence, and false for every operator who moves their Orker
// pool. Under versioning that claim is load-bearing: it decides which queue
// names an operator expects in `temporal worker deployment describe-version`,
// and the runbook told them to abort a rollout when the expected name was
// missing.
func TestQueues_OrkerQueueMovesTaskExecutionOnly(t *testing.T) {
	t.Setenv("ORK_ORKER_QUEUE", "ork-tasks-gpu-pool")

	q := queuesFor(loadConfig())
	if q.run != task.DefaultQueue {
		t.Errorf("the engine's run worker polls %q, want %q: ORK_ORKER_QUEUE moved the queue that hosts RunWorkflow",
			q.run, task.DefaultQueue)
	}
	if q.orker != "ork-tasks-gpu-pool" {
		t.Errorf("task activities go to %q, want the configured ork-tasks-gpu-pool", q.orker)
	}
	if q.events != engine.EventQueue {
		t.Errorf("projection worker polls %q, want %q", q.events, engine.EventQueue)
	}
	if q.run == q.orker {
		t.Error("a configured Orker pool collapsed back onto the engine's own queue")
	}
}

// TestQueues_DefaultCollapsesTheOrkerPoolOntoTheEngineQueue is the other half:
// with no configuration one queue carries both, which is what makes a
// single-process dev run work — and what hid the runbook's error.
func TestQueues_DefaultCollapsesTheOrkerPoolOntoTheEngineQueue(t *testing.T) {
	t.Setenv("ORK_ORKER_QUEUE", "")

	q := queuesFor(loadConfig())
	if q.run != task.DefaultQueue || q.orker != task.DefaultQueue {
		t.Fatalf("unconfigured queues = run %q orker %q, want both %q", q.run, q.orker, task.DefaultQueue)
	}
}

// TestQueues_RefuseAnExecutionChannelThatIsTheEnginesOwnEventQueue is the
// third configuration, and the one that must not boot at all.
//
// ORK_ORKER_QUEUE=ork-engine-events points every task activity at the queue
// carrying PublishEvent: tasks would be dispatched to the projection worker,
// and `pool: main` — the pool a document that names none runs on — would
// resolve to the engine's own internals. task.QueueForPool refuses the channel
// too, but by then a run has already started; this is where an operator learns
// it, with the variable named.
func TestQueues_RefuseAnExecutionChannelThatIsTheEnginesOwnEventQueue(t *testing.T) {
	t.Setenv("ORK_ORKER_QUEUE", engine.EventQueue)

	err := queuesFor(loadConfig()).validate()
	if err == nil {
		t.Fatal("ork accepted ORK_ORKER_QUEUE=ork-engine-events: every task would be " +
			"dispatched onto the engine's projection queue")
	}
	if !strings.Contains(err.Error(), "ORK_ORKER_QUEUE") {
		t.Errorf("boot error is %q; it must name the variable an operator has to change", err)
	}

	// And the two legitimate shapes still boot: the unconfigured default,
	// where one queue carries the engine's workflows and its tasks, and a
	// parallel agent's isolation queue (AGENTS.md §11.1).
	for _, channel := range []string{"", task.DefaultQueue, "ork-tasks-private", "ork-pool-gpu"} {
		t.Setenv("ORK_ORKER_QUEUE", channel)
		if err := queuesFor(loadConfig()).validate(); err != nil {
			t.Errorf("ORK_ORKER_QUEUE=%q refused at boot: %v", channel, err)
		}
	}
}

// TestRunWorkerOptions_CarryTheConfiguredDeploymentVersion: the engine
// process that hosts RunWorkflow registers with Worker
// Deployment Versioning when — and only when — an operator configured a build
// id.
func TestRunWorkerOptions_CarryTheConfiguredDeploymentVersion(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "orkstack-prod")
	t.Setenv(task.EnvBuildID, "v2")

	options := runWorkerOptions(loadConfig())
	if !options.DeploymentOptions.UseVersioning {
		t.Fatal("run worker is unversioned with ORK_BUILD_ID set: an engine rollout would move in-flight runs")
	}
	want := worker.WorkerDeploymentVersion{DeploymentName: "orkstack-prod", BuildID: "v2"}
	if options.DeploymentOptions.Version != want {
		t.Fatalf("run worker version = %+v, want %+v", options.DeploymentOptions.Version, want)
	}
	if !options.LocalActivityWorkerOnly {
		t.Error("versioning dropped LocalActivityWorkerOnly")
	}
}

// TestWorkerOptions_UnsetDeploymentEnvLeavesBothWorkersUnversioned: `make
// run-ork` with no ORK_DEPLOYMENT_NAME/ORK_BUILD_ID produces unversioned run
// and event workers, with no DefaultVersioningBehavior set (the SDK panics on
// that pair).
func TestWorkerOptions_UnsetDeploymentEnvLeavesBothWorkersUnversioned(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "")
	t.Setenv(task.EnvBuildID, "")
	cfg := loadConfig()

	if cfg.deployment.Versioned() {
		t.Fatalf("deployment %v is versioned with no configuration", cfg.deployment)
	}
	for name, options := range map[string]worker.Options{
		"run": runWorkerOptions(cfg), "event": eventWorkerOptions(cfg),
	} {
		if options.DeploymentOptions.UseVersioning {
			t.Errorf("%s worker uses versioning with no configuration", name)
		}
		if options.DeploymentOptions.DefaultVersioningBehavior != workflow.VersioningBehaviorUnspecified {
			t.Errorf("%s worker sets DefaultVersioningBehavior while unversioned; the SDK panics on that pair", name)
		}
	}
}

// TestEventWorkerOptions_StayUnversionedEvenWhenConfigured pins the one
// deliberate exception. PublishEvent must remain executable by any live
// replica of any version, which is what keeps a run readable from every
// replica; versioning that worker would make a pinned run's projections wait
// for its own build's pollers.
func TestEventWorkerOptions_StayUnversionedEvenWhenConfigured(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "orkstack-prod")
	t.Setenv(task.EnvBuildID, "v2")
	cfg := loadConfig()

	if !cfg.deployment.Versioned() {
		t.Fatal("test setup: the deployment should be versioned here")
	}
	if options := eventWorkerOptions(cfg); options.DeploymentOptions.UseVersioning {
		t.Fatalf("event worker joined deployment version %+v; the projection worker must stay unversioned",
			options.DeploymentOptions.Version)
	}
}

// recordedRegistration is one call engine.RegisterWorkflows made.
type recordedRegistration struct {
	name     string
	behavior workflow.VersioningBehavior
}

// recordingRegistrar captures what the binary registers, without a worker.
type recordingRegistrar []recordedRegistration

func (r *recordingRegistrar) RegisterWorkflowWithOptions(_ any, options workflow.RegisterOptions) {
	*r = append(*r, recordedRegistration{name: options.Name, behavior: options.VersioningBehavior})
}

// TestRegisterEngineWorkflows_DeclaresPinnedForRunsAndAutoUpgradeForClaims
// reads the list `ork` itself registers at boot
// (engine.RegisterWorkflows, the binary's only call site), so dropping a
// behaviour fails here — asserting against a copy of the registration would
// prove only that the test still compiles. It is not the only such guard: the
// versioning integration tests now stand their workers up through the same
// function, so a flipped behaviour also fails the rollout against a cluster.
func TestRegisterEngineWorkflows_DeclaresPinnedForRunsAndAutoUpgradeForClaims(t *testing.T) {
	var registered recordingRegistrar
	engine.RegisterWorkflows(&registered)

	want := map[string]workflow.VersioningBehavior{
		engine.RunWorkflowType:          workflow.VersioningBehaviorPinned,
		engine.ClaimWorkflowType:        workflow.VersioningBehaviorAutoUpgrade,
		engine.ScheduleFireWorkflowType: workflow.VersioningBehaviorPinned,
	}
	if len(registered) != len(want) {
		t.Fatalf("registered %d workflow types (%+v), want %d", len(registered), registered, len(want))
	}
	for _, got := range registered {
		expected, known := want[got.name]
		if !known {
			t.Errorf("workflow %q is registered but declares no considered behaviour", got.name)
			continue
		}
		if got.behavior != expected {
			t.Errorf("workflow %q registers behaviour %v, want %v", got.name, got.behavior, expected)
		}
		if got.behavior == workflow.VersioningBehaviorUnspecified {
			t.Errorf("workflow %q declares no versioning behaviour; a versioned worker panics on it", got.name)
		}
	}
}

// TestRegisterWorkflows_ScheduleFireIsPinned: a schedule's fire lives exactly
// as long as the run it starts, so it is Pinned with that run
// (docs/ORKFLOW_SCHEDULES.md).
func TestRegisterWorkflows_ScheduleFireIsPinned(t *testing.T) {
	var registered recordingRegistrar
	engine.RegisterWorkflows(&registered)
	for _, got := range registered {
		if got.name != engine.ScheduleFireWorkflowType {
			continue
		}
		if got.behavior != workflow.VersioningBehaviorPinned {
			t.Fatalf("%s registers behaviour %v, want Pinned", got.name, got.behavior)
		}
		return
	}
	t.Fatalf("%s is not registered: %+v", engine.ScheduleFireWorkflowType, registered)
}

// TestRegisterEngineWorkflows_SurvivesAVersionedWorker is the same acceptance
// checked against the SDK rather than against this test's own expectations: a
// versioned worker with no DefaultVersioningBehavior PANICS on a workflow type
// registered without one (sdk@v1.47.0/internal/internal_worker.go:1317-1322).
// Registering the shipped list on a real (undialled) worker built from this
// binary's own options is therefore a live check that both types declare one.
func TestRegisterEngineWorkflows_SurvivesAVersionedWorker(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "orkstack-prod")
	t.Setenv(task.EnvBuildID, "v2")

	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewLazyClient: %v", err)
	}
	defer c.Close()

	cfg := loadConfig()
	if !cfg.deployment.Versioned() {
		t.Fatal("test setup: the worker under test must be versioned for the SDK guard to apply")
	}
	engine.RegisterWorkflows(worker.New(c, task.DefaultQueue, runWorkerOptions(cfg)))
}

// TestLoadConfig_IdempotencyTTL: the claim TTL is the only thing that ever
// frees an idempotency key, so a mistyped value must not silently become zero
// (a key freed immediately) and must not stop the engine booting either.
func TestLoadConfig_IdempotencyTTL(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  string
		want time.Duration
	}{
		{"unset", "", engine.DefaultClaimTTL},
		{"explicit", "30m", 30 * time.Minute},
		{"unparsable", "half an hour", engine.DefaultClaimTTL},
		{"zero", "0s", engine.DefaultClaimTTL},
		{"negative", "-1h", engine.DefaultClaimTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set != "" {
				t.Setenv("ORK_IDEMPOTENCY_TTL", tc.set)
			}
			if got := loadConfig().idempotencyTTL; got != tc.want {
				t.Errorf("ORK_IDEMPOTENCY_TTL=%q gave %v, want %v", tc.set, got, tc.want)
			}
		})
	}
}

func TestServerShutdownCancelsActiveEventStream(t *testing.T) {
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{Id: "active", Status: orkstackv1.Status_STATUS_RUNNING})
	svc := api.NewService(api.Config{Runs: runs, Bus: bus})
	server := api.NewServer("127.0.0.1:0", api.NewMux(svc, api.MuxOptions{}))
	cancelHandlers := installServerShutdownContext(server)
	defer cancelHandlers()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	defer transport.CloseIdleConnections()
	// Seed one event so the server flushes stream response headers; the stream
	// remains active after this nonterminal backlog item is consumed.
	bus.Publish(&orkstackv1.Event{
		RunId:  "active",
		Type:   orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
		Status: orkstackv1.Status_STATUS_RUNNING,
	}, "started")

	client := orkstackv1connect.NewRunServiceClient(
		&http.Client{Transport: transport}, "http://"+listener.Addr().String())
	stream, err := client.StreamEvents(context.Background(),
		connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: "active"}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	if !stream.Receive() {
		t.Fatalf("receive initial event: %v", stream.Err())
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown with active StreamEvents: %v", err)
	}
	if stream.Receive() {
		t.Fatalf("received event after shutdown: %v", stream.Msg())
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve: %v", err)
	}
}

// TestQueues_ThePoolTheEngineDispatchesToIsTheOneItsOrkersJoin is the engine's
// half of the pool contract: both binaries resolve ORK_ORKER_POOL and
// ORK_ORKER_QUEUE through the same function, so a pool named on one and a
// channel named on the other cannot silently diverge.
func TestQueues_ThePoolTheEngineDispatchesToIsTheOneItsOrkersJoin(t *testing.T) {
	gpuQueue, err := task.QueueForPool("gpu", "")
	if err != nil {
		t.Fatalf("QueueForPool(gpu): %v", err)
	}
	t.Setenv("ORK_ORKER_POOL", "gpu")
	t.Setenv("ORK_ORKER_QUEUE", "")

	cfg := loadConfig()
	if cfg.poolErr != nil {
		t.Fatalf("ORK_ORKER_POOL=gpu refused: %v", cfg.poolErr)
	}
	if q := queuesFor(cfg); q.orker != gpuQueue {
		t.Errorf("task activities go to %q, want pool gpu's channel %q", q.orker, gpuQueue)
	} else if q.run != task.DefaultQueue {
		t.Errorf("the engine's run worker moved to %q; a pool moves task execution only", q.run)
	}
	// The default route is pinned only because an operator asked for it. With
	// neither variable set it stays empty, which is what lets a namespace's own
	// default_pool decide where an unpinned task runs.
	if cfg.defaultRouteOverride != gpuQueue {
		t.Errorf("default route override = %q, want %q", cfg.defaultRouteOverride, gpuQueue)
	}
}

// TestLoadConfig_LeavesTheDefaultRouteToTheNamespaceWhenNothingIsSet is the
// other half. An engine that pinned its default route unconditionally would
// override every namespace's default_pool with `main`, which is the setting
// silently winning over the document.
func TestLoadConfig_LeavesTheDefaultRouteToTheNamespaceWhenNothingIsSet(t *testing.T) {
	t.Setenv("ORK_ORKER_POOL", "")
	t.Setenv("ORK_ORKER_QUEUE", "")

	cfg := loadConfig()
	if cfg.defaultRouteOverride != "" {
		t.Errorf("an unconfigured engine pinned the default route to %q; a namespace's "+
			"own default_pool would then never be reachable", cfg.defaultRouteOverride)
	}
	if q := queuesFor(cfg); q.orker != task.DefaultQueue {
		t.Errorf("unconfigured orker channel = %q, want %q", q.orker, task.DefaultQueue)
	}
}

// TestLoadConfig_RefusesAPoolAndAChannelThatDisagree keeps the engine from
// booting into the split-brain AGENTS.md §11.1 describes.
func TestLoadConfig_RefusesAPoolAndAChannelThatDisagree(t *testing.T) {
	t.Setenv("ORK_ORKER_POOL", "gpu")
	t.Setenv("ORK_ORKER_QUEUE", "ork-tasks-private")

	cfg := loadConfig()
	if cfg.poolErr == nil {
		t.Fatal("a disagreeing pool and channel booted")
	}
	if err := run(nil, telemetry.Config{}, nil, cfg); err == nil {
		t.Fatal("run() started with a disagreeing pool and channel")
	}
}

// TestSystemInfo_CarriesTheProcessConfigurationAndNoCredential is the wiring
// half of the admin surface's system read: the handler is a pure echo
// (internal/api/system.go), so what an operator sees is decided HERE.
//
// Two claims, and the second is the one worth a test. The values come from
// the configuration this process loaded — not from a second read of the
// environment, which is what would let the answer drift from what the engine
// is actually doing. And nothing secret travels: the artifact location names
// the bucket, never the access key that opens it.
func TestSystemInfo_CarriesTheProcessConfigurationAndNoCredential(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "orkstack-prod")
	t.Setenv(task.EnvBuildID, "build-7")
	t.Setenv("ORK_TEMPORAL_ADDRESS", "cluster.internal:7233")

	artifactCfg := artifacts.Config{
		Backend:     artifacts.BackendS3,
		S3Bucket:    "orkstack-artifacts",
		S3Endpoint:  "http://minio.internal:9000",
		S3AccessKey: "AKIA-NOT-FOR-OPERATORS",
		S3SecretKey: "s3cr3t",
	}
	info := systemInfo(loadConfig(), artifactCfg)

	if info.DeploymentName != "orkstack-prod" || info.BuildID != "build-7" {
		t.Errorf("deployment identity = %q/%q, want orkstack-prod/build-7",
			info.DeploymentName, info.BuildID)
	}
	if !info.VersioningActive() {
		t.Error("versioning_active is false while ORK_DEPLOYMENT_NAME and ORK_BUILD_ID are both set")
	}
	if info.TemporalAddress != "cluster.internal:7233" {
		t.Errorf("temporal address = %q, want the configured one", info.TemporalAddress)
	}
	if info.ArtifactBackend != artifacts.BackendS3 {
		t.Errorf("artifact backend = %q, want s3", info.ArtifactBackend)
	}
	if info.ArtifactLocation != "s3://orkstack-artifacts" {
		t.Errorf("artifact location = %q, want s3://orkstack-artifacts", info.ArtifactLocation)
	}
	if info.EngineVersion == "" || info.GoVersion == "" {
		t.Errorf("build identity is empty (%q, %q); an operator cannot tell two builds apart",
			info.EngineVersion, info.GoVersion)
	}
	// The credential check, over the whole struct rather than one field: a
	// value added later must not reintroduce it.
	rendered := fmt.Sprintf("%+v", info)
	for _, secret := range []string{artifactCfg.S3AccessKey, artifactCfg.S3SecretKey} {
		if strings.Contains(rendered, secret) {
			t.Errorf("system information carries the credential %q: %s", secret, rendered)
		}
	}
}

// TestSystemInfo_LeavesVersioningOffWhenNoBuildIDIsConfigured pins the
// documented default: one variable turns versioning on, and with neither set
// `make run-ork` reports an unversioned deployment rather than a fabricated
// version (AGENTS.md §7.2).
func TestSystemInfo_LeavesVersioningOffWhenNoBuildIDIsConfigured(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "")
	t.Setenv(task.EnvBuildID, "")

	info := systemInfo(loadConfig(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: "./.data/artifacts"})

	if info.VersioningActive() {
		t.Error("versioning_active is true with no build id configured")
	}
	if info.DeploymentName != task.DefaultDeploymentName {
		t.Errorf("deployment name = %q, want the default %q", info.DeploymentName, task.DefaultDeploymentName)
	}
	if info.ArtifactLocation != "./.data/artifacts" {
		t.Errorf("artifact location = %q, want the configured directory", info.ArtifactLocation)
	}
}

// TestTraceAPIRequest_ExcludesWebSocketUpgrades pins apiHandler's filter: a
// /ws/ route's ServeHTTP blocks for as long as the viewer keeps the tab open
// (internal/api/server.go mounts /ws/runs/{id} and api.TaskLogSocketPath
// there), so tracing it would produce a span, and an
// http.server.request.duration sample, as long as the connection stays
// open — polluting API latency metrics and traces with numbers that describe
// a browser session, not a request. Every other route is a normal
// request/response and must still be traced.
func TestTraceAPIRequest_ExcludesWebSocketUpgrades(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		want bool
	}{
		{"run event bridge", "/ws/runs/abc123", false},
		{"task log socket", "/ws/runs/abc123/tasks/build/logs", false},
		{"connect rpc", "/orkstack.v1.SystemService/GetSystemInfo", true},
		{"admin healthz", "/healthz", true},
		{"root", "/", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, tc.path, nil)
			if got := traceAPIRequest(r); got != tc.want {
				t.Errorf("traceAPIRequest(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

// TestApiHandler_WrapsOnlyWhenSignalsAreOn is apiHandler's zero-config
// identity check plus its enabled-path sanity check: with both traces and
// metrics off it must hand back the exact *http.ServeMux this binary always
// served (no wrapper, no dependency on sinks at all — sinks is nil here and
// must never be touched), and with tracing on it must return something other
// than the bare mux.
func TestApiHandler_WrapsOnlyWhenSignalsAreOn(t *testing.T) {
	mux := http.NewServeMux()

	t.Run("off", func(t *testing.T) {
		got := apiHandler(telemetry.Config{}, nil, mux)
		if got != http.Handler(mux) {
			t.Fatalf("apiHandler with no signal enabled did not return mux unchanged")
		}
	})

	t.Run("on", func(t *testing.T) {
		tcfg := telemetry.Config{TracesExporter: telemetry.ExporterOTLP}
		sinks, err := telemetry.SetupSinks(context.Background(), tcfg)
		if err != nil {
			t.Fatalf("SetupSinks: %v", err)
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = sinks.Shutdown(shutdownCtx)
		}()

		got := apiHandler(tcfg, sinks, mux)
		if got == http.Handler(mux) {
			t.Fatalf("apiHandler with tracing enabled returned the bare mux, want an instrumented wrapper")
		}
	})
}

// TestBindAndServe_ReturnsABootErrorWhenThePortIsTaken is the readiness-race
// regression test: binding must happen synchronously, in bindAndServe itself,
// not in the background goroutine that used to race Ready.Set(true). An
// EADDRINUSE here must come back as a returned error naming the address, the
// same turn the caller can still decide not to flip readiness.
func TestBindAndServe_ReturnsABootErrorWhenThePortIsTaken(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer blocker.Close()

	server := &http.Server{Addr: blocker.Addr().String()}
	serverErr := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := bindAndServe(server, logger, serverErr); err == nil {
		t.Fatal("bindAndServe bound an address already in use")
	} else if !strings.Contains(err.Error(), blocker.Addr().String()) {
		t.Errorf("boot error %q does not name the taken address %s", err, blocker.Addr().String())
	}
}

// TestBindAndServe_AcceptsConnectionsAsSoonAsItReturns proves the port is
// actually bound and serving the moment bindAndServe hands control back —
// the property main's readiness flip now depends on: a caller that sets
// Ready.Set(true) right after this call returns is not guessing.
func TestBindAndServe_AcceptsConnectionsAsSoonAsItReturns(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: "127.0.0.1:0", Handler: mux}
	serverErr := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ln, err := bindAndServe(server, logger, serverErr)
	if err != nil {
		t.Fatalf("bindAndServe: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve: %v", err)
		}
	}()

	resp, err := http.Get("http://" + ln.Addr().String() + "/ping")
	if err != nil {
		t.Fatalf("GET /ping right after bindAndServe returned: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

// ORK_EACH_MAX_ITEMS has a deployment default of 1000, which a deployment
// lowers, or raises up to the built-in 10000 with continuation on (the
// default): 0 and 10001 refuse boot, 1 through 10000 are accepted, unset is
// the default.
func TestLoadConfig_EachMaxItemsBounds(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    int32
		refused bool
	}{
		{"", 1000, false}, {"1", 1, false}, {"1000", 1000, false}, {"1001", 1001, false},
		{"10000", 10000, false},
		{"0", 1000, true}, {"10001", 1000, true}, {"-3", 1000, true}, {"many", 1000, true},
	} {
		t.Run("ORK_EACH_MAX_ITEMS="+tc.raw, func(t *testing.T) {
			t.Setenv("ORK_EACH_MAX_ITEMS", tc.raw)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused)
			}
			if !tc.refused && cfg.eachMaxItems != tc.want {
				t.Fatalf("eachMaxItems = %d, want %d", cfg.eachMaxItems, tc.want)
			}
		})
	}
}

// TestEachMaxItemsFromEnv_RaisesOnlyWithContinuation: above the deployment
// default of 1000 a run's items span more than one history, so a raise is
// refused with continuation off, naming both keys; above the built-in
// 10000 it is refused whatever continuation says, with the new message.
func TestEachMaxItemsFromEnv_RaisesOnlyWithContinuation(t *testing.T) {
	t.Setenv("ORK_CONTINUATION_ENABLED", "false")
	t.Setenv("ORK_EACH_MAX_ITEMS", "5000")
	err := loadConfig().limitsErr
	if err == nil || !strings.Contains(err.Error(), "ORK_EACH_MAX_ITEMS=5000") ||
		!strings.Contains(err.Error(), "ORK_CONTINUATION_ENABLED") {
		t.Fatalf("5000 with continuation off = %v, want a refusal naming both keys", err)
	}
	t.Setenv("ORK_EACH_MAX_ITEMS", "1000")
	if cfg := loadConfig(); cfg.limitsErr != nil || cfg.eachMaxItems != 1000 {
		t.Fatalf("1000 with continuation off = %d, %v", cfg.eachMaxItems, cfg.limitsErr)
	}
	t.Setenv("ORK_CONTINUATION_ENABLED", "true")
	t.Setenv("ORK_EACH_MAX_ITEMS", "5000")
	if cfg := loadConfig(); cfg.limitsErr != nil || cfg.eachMaxItems != 5000 {
		t.Fatalf("5000 with continuation on = %d, %v", cfg.eachMaxItems, cfg.limitsErr)
	}
	t.Setenv("ORK_EACH_MAX_ITEMS", "10001")
	if err := loadConfig().limitsErr; err == nil ||
		!strings.Contains(err.Error(), "ORK_EACH_MAX_ITEMS=10001 is above the built-in maximum of 10000") {
		t.Fatalf("10001 = %v", err)
	}
}

// TestLoadConfig_HistoryLimitRefusesTinyValues: ORK_HISTORY_MAX_EVENTS and
// ORK_HISTORY_MAX_BYTES default to the cluster's own defaults, accept any
// value at or above their minimum, and refuse boot naming the variable below
// it or when malformed — never a silent fallback.
func TestLoadConfig_HistoryLimitRefusesTinyValues(t *testing.T) {
	// The guard alone: continuation, on by default, has keys of its own that
	// a tiny history limit cannot hold (TestLoadConfig_ContinuationIsOnByDefault).
	t.Setenv("ORK_CONTINUATION_ENABLED", "false")
	cfg := loadConfig()
	if cfg.limitsErr != nil || cfg.historyMaxEvents != engine.DefaultHistoryMaxEvents ||
		cfg.historyMaxBytes != engine.DefaultHistoryMaxBytes {
		t.Fatalf("defaults = %d events, %d bytes (%v); want %d and %d", cfg.historyMaxEvents,
			cfg.historyMaxBytes, cfg.limitsErr, engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes)
	}
	for _, tc := range []struct {
		key, raw string
		want     int64
		refused  bool
	}{
		{"ORK_HISTORY_MAX_EVENTS", "100", 100, false},
		{"ORK_HISTORY_MAX_EVENTS", "200000", 200000, false},
		{"ORK_HISTORY_MAX_EVENTS", "99", 0, true},
		{"ORK_HISTORY_MAX_EVENTS", "0", 0, true},
		{"ORK_HISTORY_MAX_EVENTS", "many", 0, true},
		{"ORK_HISTORY_MAX_BYTES", "65536", 65536, false},
		{"ORK_HISTORY_MAX_BYTES", "2097152", 2097152, false},
		{"ORK_HISTORY_MAX_BYTES", "65535", 0, true},
		{"ORK_HISTORY_MAX_BYTES", "-1", 0, true},
	} {
		t.Run(tc.key+"="+tc.raw, func(t *testing.T) {
			t.Setenv(tc.key, tc.raw)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused)
			}
			if tc.refused {
				if !strings.Contains(cfg.limitsErr.Error(), tc.key) {
					t.Fatalf("refusal %v does not name %s", cfg.limitsErr, tc.key)
				}
				return
			}
			got := cfg.historyMaxEvents
			if tc.key == "ORK_HISTORY_MAX_BYTES" {
				got = cfg.historyMaxBytes
			}
			if got != tc.want {
				t.Fatalf("%s = %d, want %d", tc.key, got, tc.want)
			}
		})
	}
}

// ORK_ORKFLOW_MAX_TASKS is a deployment-configurable default: unset is 1000,
// any whole number of tasks, 1 or more, lowers or raises it, and anything
// else refuses boot naming the variable rather than falling back.
func TestLoadConfig_OrkflowMaxTasks(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    int32
		refused bool
	}{
		{"", 1000, false}, {"1", 1, false}, {"200", 200, false}, {"5000", 5000, false},
		{"0", 0, true}, {"-3", 0, true}, {"many", 0, true}, {"1.5", 0, true},
	} {
		t.Run("ORK_ORKFLOW_MAX_TASKS="+tc.raw, func(t *testing.T) {
			t.Setenv("ORK_ORKFLOW_MAX_TASKS", tc.raw)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused)
			}
			if tc.refused && !strings.Contains(cfg.limitsErr.Error(), "ORK_ORKFLOW_MAX_TASKS") {
				t.Fatalf("refusal %v does not name the variable", cfg.limitsErr)
			}
			if !tc.refused && cfg.orkflowMaxTasks != tc.want {
				t.Fatalf("orkflowMaxTasks = %d, want %d", cfg.orkflowMaxTasks, tc.want)
			}
		})
	}
}

// ORK_COMPOSE_MAX_DEPTH lowers the built-in composition ceiling and never
// raises it: 0 and 5 refuse boot, 1 and 4 are accepted, unset is the ceiling.
func TestLoadConfig_ComposeMaxDepthBounds(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    int32
		refused bool
	}{
		{"", 4, false}, {"1", 1, false}, {"4", 4, false},
		{"0", 4, true}, {"5", 4, true}, {"-1", 4, true}, {"deep", 4, true},
	} {
		t.Run("ORK_COMPOSE_MAX_DEPTH="+tc.raw, func(t *testing.T) {
			t.Setenv("ORK_COMPOSE_MAX_DEPTH", tc.raw)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused)
			}
			if !tc.refused && cfg.composeMaxDepth != tc.want {
				t.Fatalf("composeMaxDepth = %d, want %d", cfg.composeMaxDepth, tc.want)
			}
		})
	}
	t.Setenv("ORK_COMPOSE_MAX_DEPTH", "6")
	if err := loadConfig().limitsErr; err == nil || !strings.Contains(err.Error(), "ORK_COMPOSE_MAX_DEPTH=6 is above the 4 ceiling; it may only lower it") {
		t.Fatalf("6 = %v", err)
	}
}

// ORK_CACHE_MAX_TTL bounds how old a memoized result a cached task may reuse:
// 0s, a negative duration and garbage refuse boot; 1s and a year are taken;
// unset is engine.DefaultCacheMaxTTL.
func TestLoadConfig_CacheMaxTTLBounds(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    time.Duration
		refused bool
	}{
		{"", 168 * time.Hour, false}, {"1s", time.Second, false}, {"8760h", 8760 * time.Hour, false},
		{"0s", 0, true}, {"-1h", 0, true}, {"abc", 0, true}, {"500ms", 0, true},
	} {
		t.Run("ORK_CACHE_MAX_TTL="+tc.raw, func(t *testing.T) {
			t.Setenv("ORK_CACHE_MAX_TTL", tc.raw)
			cfg := loadConfig()
			if refused := cfg.limitsErr != nil; refused != tc.refused {
				t.Fatalf("refused = %v (%v), want %v", refused, cfg.limitsErr, tc.refused)
			}
			if !tc.refused && cfg.cacheMaxTTL != tc.want {
				t.Fatalf("cacheMaxTTL = %s, want %s", cfg.cacheMaxTTL, tc.want)
			}
		})
	}
	t.Setenv("ORK_CACHE_MAX_TTL", "0s")
	if err := loadConfig().limitsErr; err == nil ||
		!strings.Contains(err.Error(), "ORK_CACHE_MAX_TTL=0s must be at least 1s: it bounds how old a memoized result a cached task may reuse") {
		t.Fatalf("0s = %v", err)
	}
}
