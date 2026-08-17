package main

import (
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/task"
)

// TestWorkerOptions_StayActivityOnly guards the property the shared queue
// depends on: an Orker never competes with the engine for workflow tasks.
func TestWorkerOptions_StayActivityOnly(t *testing.T) {
	options := workerOptions(config{})
	if !options.DisableWorkflowWorker {
		t.Error("an Orker must not poll for workflow tasks on the shared queue")
	}
	if !options.DisableRegistrationAliasing {
		t.Error("an Orker must disable registration aliases")
	}
}

// TestWorkerOptions_CarryTheConfiguredDeploymentVersion is acceptance 1 for the
// Orker, and the precondition for acceptance 3. An Orker that does not carry
// the engine's deployment version is not in a pinned run's version at all:
// Temporal's exception clause then routes that run's tasks to whatever is
// current on the queue, which is exactly the mid-run plugin swap this
// milestone exists to prevent.
func TestWorkerOptions_CarryTheConfiguredDeploymentVersion(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "orkstack-prod")
	t.Setenv(task.EnvBuildID, "v2")

	options := workerOptions(loadConfig())
	if !options.DeploymentOptions.UseVersioning {
		t.Fatal("Orker is unversioned with ORK_BUILD_ID set; a pinned run's tasks cannot reach it by version")
	}
	want := worker.WorkerDeploymentVersion{DeploymentName: "orkstack-prod", BuildID: "v2"}
	if options.DeploymentOptions.Version != want {
		t.Fatalf("Orker version = %+v, want %+v — the same pair the engine registers",
			options.DeploymentOptions.Version, want)
	}
	if !options.DisableWorkflowWorker {
		t.Error("versioning dropped DisableWorkflowWorker")
	}
}

// TestWorkerOptions_UnsetDeploymentEnvLeavesVersioningOff is acceptance 6 for
// the Orker: `make run-orker` with no configuration behaves as it always did.
func TestWorkerOptions_UnsetDeploymentEnvLeavesVersioningOff(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "")
	t.Setenv(task.EnvBuildID, "")

	cfg := loadConfig()
	if cfg.deployment.Versioned() {
		t.Fatalf("deployment %v is versioned with no configuration", cfg.deployment)
	}
	options := workerOptions(cfg)
	if options.DeploymentOptions.UseVersioning {
		t.Error("Orker uses versioning with no configuration")
	}
	if options.DeploymentOptions.DefaultVersioningBehavior != workflow.VersioningBehaviorUnspecified {
		t.Error("Orker sets DefaultVersioningBehavior while unversioned; the SDK panics on that pair")
	}
}

// TestLoadConfig_RejectsADeploymentNameTheClusterWould keeps a typo from
// becoming a confusing server rejection on the first poll.
func TestLoadConfig_RejectsADeploymentNameTheClusterWould(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "ork.stack")
	t.Setenv(task.EnvBuildID, "v2")

	if err := loadConfig().deployment.Validate(); err == nil {
		t.Fatal("a deployment name containing \".\" was accepted; the cluster reserves that character")
	}
}
