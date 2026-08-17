package main

import (
	"context"
	"errors"
	"net"
	"net/http"
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

// recordedRegistration is one call registerEngineWorkflows made.
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
// acceptance 2. It reads what the binary registers, so dropping the behaviour
// from cmd/ork fails here — asserting against a copy of the registration would
// prove only that the test still compiles.
func TestRegisterEngineWorkflows_DeclaresPinnedForRunsAndAutoUpgradeForClaims(t *testing.T) {
	var registered recordingRegistrar
	registerEngineWorkflows(&registered)

	want := map[string]workflow.VersioningBehavior{
		engine.WorkflowName:      workflow.VersioningBehaviorPinned,
		engine.ClaimWorkflowName: workflow.VersioningBehaviorAutoUpgrade,
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
// Registering through the binary's own function on a real (undialled) worker
// is therefore a live check that both types declare one.
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
	registerEngineWorkflows(worker.New(c, task.DefaultQueue, runWorkerOptions(cfg)))
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
