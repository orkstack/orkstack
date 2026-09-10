package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/orker"
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

// TestWorkerOptions_CapTheHeartbeatThrottleToTheTick is the half of the
// heartbeat contract that is invisible until somebody cancels.
//
// The Go SDK does not send every RecordHeartbeat: it batches them into windows
// of min(0.8 × HeartbeatTimeout, MaxHeartbeatThrottleInterval), and Temporal
// delivers cancellation ONLY in a heartbeat RPC's response. With the cap unset
// that window is 24 s however fast the Orker's own loop ticks — measured on a
// real cluster, a failing task's still-running sibling was asked to stop 52 ms
// after the failure and learned of it 23.86 s later. So the assertion is not
// "the option is set" but "it is set to the SAME number the loop ticks at".
func TestWorkerOptions_CapTheHeartbeatThrottleToTheTick(t *testing.T) {
	t.Run("an operator's interval reaches the SDK", func(t *testing.T) {
		t.Setenv(orker.HeartbeatIntervalVar, "3s")
		cfg := loadConfig()
		if cfg.heartbeatErr != nil {
			t.Fatalf("3s refused: %v", cfg.heartbeatErr)
		}
		if got := workerOptions(cfg).MaxHeartbeatThrottleInterval; got != 3*time.Second {
			t.Errorf("MaxHeartbeatThrottleInterval = %v, want 3s; anything else means the SDK "+
				"batches the Orker's ticks and cancellation waits for the batch", got)
		}
	})

	t.Run("unset is the documented default, not the SDK's", func(t *testing.T) {
		t.Setenv(orker.HeartbeatIntervalVar, "")
		cfg := loadConfig()
		if got := workerOptions(cfg).MaxHeartbeatThrottleInterval; got != orker.DefaultHeartbeatInterval {
			t.Errorf("MaxHeartbeatThrottleInterval = %v, want %v; zero here is the SDK's 60 s "+
				"option default, which leaves the 0.8 × timeout window in place",
				got, orker.DefaultHeartbeatInterval)
		}
	})

	t.Run("versioning does not drop it", func(t *testing.T) {
		t.Setenv(task.EnvDeploymentName, "orkstack-prod")
		t.Setenv(task.EnvBuildID, "v2")
		t.Setenv(orker.HeartbeatIntervalVar, "2s")
		options := workerOptions(loadConfig())
		if !options.DeploymentOptions.UseVersioning {
			t.Fatal("versioning did not apply; this subtest asserts nothing")
		}
		if options.MaxHeartbeatThrottleInterval != 2*time.Second {
			t.Errorf("Deployment.Apply dropped MaxHeartbeatThrottleInterval (%v)",
				options.MaxHeartbeatThrottleInterval)
		}
	})
}

// TestOrkerOptions_TickTheSameNumberTheWorkerThrottlesAt is the pairing itself.
// Either half alone is useless: a fast loop whose beats the SDK batches
// changes nothing, and a small throttle window with no beat inside it has
// nothing to carry. One config value must reach both.
func TestOrkerOptions_TickTheSameNumberTheWorkerThrottlesAt(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(orker.HeartbeatIntervalVar, "4s")
	cfg := loadConfig()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceOrker)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}
	opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger)

	if opts.HeartbeatInterval != 4*time.Second {
		t.Errorf("orker.Options.HeartbeatInterval = %v, want the configured 4s", opts.HeartbeatInterval)
	}
	if throttle := workerOptions(cfg).MaxHeartbeatThrottleInterval; opts.HeartbeatInterval != throttle {
		t.Errorf("the dispatcher ticks every %v while the SDK batches every %v; the two must be "+
			"one number or the tick is not the cancellation latency",
			opts.HeartbeatInterval, throttle)
	}
}

// TestLoadConfig_RefusesAHeartbeatIntervalTheTimeoutCannotCover: a value this
// process cannot honor is a configuration error, not a quiet default. Both
// bounds matter for a different reason — under the SDK's RPC floor the tick
// cannot be delivered at all, and over a third of HeartbeatTimeout Temporal
// starts declaring live activities dead — so the refusal names the variable.
func TestLoadConfig_RefusesAHeartbeatIntervalTheTimeoutCannotCover(t *testing.T) {
	for _, raw := range []string{"11s", "500ms", "0s", "1h", "garbage", "5"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(orker.HeartbeatIntervalVar, raw)
			cfg := loadConfig()
			if cfg.heartbeatErr == nil {
				t.Fatalf("%s=%s booted, ticking every %v", orker.HeartbeatIntervalVar, raw, cfg.heartbeatInterval)
			}
			if !strings.Contains(cfg.heartbeatErr.Error(), orker.HeartbeatIntervalVar) {
				t.Errorf("refusal %q does not name %s", cfg.heartbeatErr, orker.HeartbeatIntervalVar)
			}
			if err := run(nil, telemetry.Config{}, nil, cfg); err == nil {
				t.Fatal("run() started with a heartbeat interval it cannot honor")
			}
		})
	}
}

// TestLoadConfig_AcceptsAnIntervalInsideTheBounds is the other side: the
// documented range really is accepted, so the refusal above is a bound and not
// a rejection of everything.
func TestLoadConfig_AcceptsAnIntervalInsideTheBounds(t *testing.T) {
	for raw, want := range map[string]time.Duration{
		"1s":     time.Second,
		"2s":     2 * time.Second,
		"1500ms": 1500 * time.Millisecond,
		"10s":    10 * time.Second,
	} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(orker.HeartbeatIntervalVar, raw)
			cfg := loadConfig()
			if cfg.heartbeatErr != nil {
				t.Fatalf("%s refused: %v", raw, cfg.heartbeatErr)
			}
			if cfg.heartbeatInterval != want {
				t.Errorf("interval = %v, want %v", cfg.heartbeatInterval, want)
			}
		})
	}
}
