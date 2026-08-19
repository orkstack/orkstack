package main

import (
	"context"
	"errors"
	"net"
	"net/http"
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
)

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
	// parallel agent's isolation queue (AGENTS.md §6).
	for _, channel := range []string{"", task.DefaultQueue, "ork-tasks-m7-9", "ork-pool-gpu"} {
		t.Setenv("ORK_ORKER_QUEUE", channel)
		if err := queuesFor(loadConfig()).validate(); err != nil {
			t.Errorf("ORK_ORKER_QUEUE=%q refused at boot: %v", channel, err)
		}
	}
}

// TestRunWorkerOptions_CarryTheConfiguredDeploymentVersion is acceptance 1 for
// the engine: the process that hosts RunWorkflow registers with Worker
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

// TestWorkerOptions_UnsetDeploymentEnvLeavesBothWorkersUnversioned is
// acceptance 6 at the binary's own seam: `make run-ork` with no configuration
// must produce exactly the workers it produced before this milestone.
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

// TestRegisterEngineWorkflows_DeclaresPinnedForRunsAndAutoUpgradeForClaims is
// acceptance 2. It reads the list `ork` itself registers at boot
// (engine.RegisterWorkflows, the binary's only call site), so dropping a
// behaviour fails here — asserting against a copy of the registration would
// prove only that the test still compiles. It is not the only such guard: the
// versioning integration tests now stand their workers up through the same
// function, so a flipped behaviour also fails the rollout against a cluster.
func TestRegisterEngineWorkflows_DeclaresPinnedForRunsAndAutoUpgradeForClaims(t *testing.T) {
	var registered recordingRegistrar
	engine.RegisterWorkflows(&registered)

	want := map[string]workflow.VersioningBehavior{
		engine.RunWorkflowType:   workflow.VersioningBehaviorPinned,
		engine.ClaimWorkflowType: workflow.VersioningBehaviorAutoUpgrade,
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
