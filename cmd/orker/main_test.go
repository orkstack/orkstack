package main

import (
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
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

// TestWorkerOptions_CarryTheConfiguredDeploymentVersion: an Orker that does
// not carry the engine's deployment version is not in a pinned run's version
// at all. Temporal's exception clause then routes that run's tasks to whatever
// build is current on the queue — the mid-run version swap Worker Deployment
// Versioning exists to prevent.
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

// TestWorkerOptions_UnsetDeploymentEnvLeavesVersioningOff: `make run-orker`
// with no ORK_DEPLOYMENT_NAME/ORK_BUILD_ID produces an unversioned worker.
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

// TestLoadConfig_JoinsThePoolItWasToldToServe is the Orker's half of the pool
// contract, at its own boot seam.
//
// ORK_ORKER_POOL is the OrkStack setting: which set of Orkers is this.
// ORK_ORKER_QUEUE is the operator escape hatch to the channel underneath, used
// by the test harnesses to isolate concurrent runs (AGENTS.md §11.1). The engine
// resolves the same two variables through the same function, because an Orker
// polling one channel while the engine dispatches to another strands every run
// in RUNNING with nothing to say why.
func TestLoadConfig_JoinsThePoolItWasToldToServe(t *testing.T) {
	gpuQueue, err := task.QueueForPool("gpu", "")
	if err != nil {
		t.Fatalf("QueueForPool(gpu): %v", err)
	}

	t.Run("zero config serves the built-in pool", func(t *testing.T) {
		t.Setenv("ORK_ORKER_POOL", "")
		t.Setenv("ORK_ORKER_QUEUE", "")
		cfg := loadConfig()
		if cfg.poolErr != nil {
			t.Fatalf("unconfigured boot refused: %v", cfg.poolErr)
		}
		if cfg.pool != task.DefaultPool || cfg.queue != task.DefaultQueue {
			t.Errorf("unconfigured Orker serves pool %q on %q, want %q on %q",
				cfg.pool, cfg.queue, task.DefaultPool, task.DefaultQueue)
		}
		if cfg.rawOverride {
			t.Error("unconfigured Orker reports a hand-pinned channel")
		}
	})

	t.Run("a pool resolves to its own channel", func(t *testing.T) {
		t.Setenv("ORK_ORKER_POOL", "gpu")
		t.Setenv("ORK_ORKER_QUEUE", "")
		cfg := loadConfig()
		if cfg.poolErr != nil {
			t.Fatalf("ORK_ORKER_POOL=gpu refused: %v", cfg.poolErr)
		}
		if cfg.pool != "gpu" || cfg.queue != gpuQueue {
			t.Errorf("Orker serves pool %q on %q, want gpu on %q", cfg.pool, cfg.queue, gpuQueue)
		}
	})

	t.Run("a raw channel is nobody's pool", func(t *testing.T) {
		t.Setenv("ORK_ORKER_POOL", "")
		t.Setenv("ORK_ORKER_QUEUE", "ork-tasks-private")
		cfg := loadConfig()
		if cfg.poolErr != nil {
			t.Fatalf("ORK_ORKER_QUEUE alone refused: %v", cfg.poolErr)
		}
		if cfg.queue != "ork-tasks-private" {
			t.Errorf("Orker polls %q, want the operator's channel verbatim", cfg.queue)
		}
		if cfg.pool != task.DefaultPool || !cfg.rawOverride {
			t.Errorf("Orker reports pool %q (raw=%v), want the built-in default and a "+
				"flag saying the channel was pinned by hand", cfg.pool, cfg.rawOverride)
		}
	})

	t.Run("a pool and a channel that disagree refuse to boot", func(t *testing.T) {
		t.Setenv("ORK_ORKER_POOL", "gpu")
		t.Setenv("ORK_ORKER_QUEUE", "ork-tasks-private")
		cfg := loadConfig()
		if cfg.poolErr == nil {
			t.Fatalf("a disagreeing pair booted, serving pool %q on %q", cfg.pool, cfg.queue)
		}
		if err := run(nil, telemetry.Config{}, nil, cfg); err == nil {
			t.Fatal("run() started with a disagreeing pool and channel")
		}
	})
}
