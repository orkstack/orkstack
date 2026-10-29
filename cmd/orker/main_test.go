package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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

// TestWorkerOptions_SetAnExplicitPollerCount: a count left at the SDK default
// opts the task worker, and a pin host's session worker with it, into poller
// autoscaling whenever the Temporal namespace advertises it (orker.TaskPollers).
func TestWorkerOptions_SetAnExplicitPollerCount(t *testing.T) {
	for _, maxPins := range []int32{0, 2} {
		options := workerOptions(config{maxPins: maxPins})
		if options.MaxConcurrentActivityTaskPollers != orker.TaskPollers || options.ActivityTaskPollerBehavior != nil {
			t.Errorf("max_pins=%d: pollers = %d, behavior = %v; want exactly %d and no behavior",
				maxPins, options.MaxConcurrentActivityTaskPollers, options.ActivityTaskPollerBehavior, orker.TaskPollers)
		}
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

// TestWorkerOptions_BecomeAPinHostOnlyWhenAsked: an
// Orker that never set ORK_ORKER_MAX_PINS must build the same worker.Options it
// built before pins existed, plus two zero-valued fields.
//
// The zero matters more than the non-zero. EnableSessionWorker starts two more
// pollers and two more task queues per process (the SDK's session creation
// worker and its per-host session activity worker), and the SDK builds no
// session worker at all when the flag is false — so "off" really is "acquires
// nothing", which is what lets the default be 0 rather than on.
func TestWorkerOptions_BecomeAPinHostOnlyWhenAsked(t *testing.T) {
	t.Run("unset acquires no session worker", func(t *testing.T) {
		t.Setenv(MaxPinsVar, "")
		cfg := loadConfig()
		if cfg.maxPinsErr != nil {
			t.Fatalf("an unset pin cap was refused: %v", cfg.maxPinsErr)
		}
		if cfg.maxPins != 0 {
			t.Errorf("maxPins = %d with nothing set, want 0", cfg.maxPins)
		}
		options := workerOptions(cfg)
		if options.EnableSessionWorker {
			t.Error("an Orker that never asked for pins enabled the session worker, and with it " +
				"two pollers and two task queues it did not have before")
		}
		if options.MaxConcurrentSessionExecutionSize != 0 {
			t.Errorf("MaxConcurrentSessionExecutionSize = %d, want 0",
				options.MaxConcurrentSessionExecutionSize)
		}
	})

	t.Run("a pin host polls for as many pins as it was given", func(t *testing.T) {
		t.Setenv(MaxPinsVar, "3")
		cfg := loadConfig()
		if cfg.maxPinsErr != nil {
			t.Fatalf("3 was refused: %v", cfg.maxPinsErr)
		}
		options := workerOptions(cfg)
		if !options.EnableSessionWorker {
			t.Fatal("ORK_ORKER_MAX_PINS=3 did not enable the session worker, so no pin could ever open")
		}
		if options.MaxConcurrentSessionExecutionSize != 3 {
			t.Errorf("MaxConcurrentSessionExecutionSize = %d, want 3 — anything larger lets a "+
				"fourth pin open on a host the operator capped at three",
				options.MaxConcurrentSessionExecutionSize)
		}
		// The pin cap must not disturb what the shared queue depends on.
		if !options.DisableWorkflowWorker || !options.DisableRegistrationAliasing {
			t.Error("a pin host stopped being activity-only")
		}
	})

	t.Run("versioning does not drop it", func(t *testing.T) {
		t.Setenv(task.EnvDeploymentName, "orkstack-prod")
		t.Setenv(task.EnvBuildID, "v2")
		t.Setenv(MaxPinsVar, "2")
		options := workerOptions(loadConfig())
		if !options.DeploymentOptions.UseVersioning {
			t.Fatal("versioning did not apply; this subtest asserts nothing")
		}
		if !options.EnableSessionWorker || options.MaxConcurrentSessionExecutionSize != 2 {
			t.Errorf("Deployment.Apply dropped the session options (%v, %d)",
				options.EnableSessionWorker, options.MaxConcurrentSessionExecutionSize)
		}
	})
}

// TestWorkerOptions_APinHostOnAVersionedDeploymentIsAcceptedByTheSDK is the
// cheap half of the pins-on-a-versioned-deployment contract (docs/DEPLOY.md
// "Pins on a versioned deployment"): does the SDK accept the one configuration whose
// two halves its own documentation says cannot be combined?
//
// The SDK's doc comment on `DeploymentOptions.UseVersioning` says it "NOTE:
// Cannot be enabled at the same time as [WorkerOptions.EnableSessionWorker]"
// (sdk@v1.47.0/internal/worker.go:80), but the only enforced panic covers the
// DEPRECATED flag — `options.EnableSessionWorker && options.UseBuildIDForVersioning`
// (internal_worker.go:2312-2316, citing temporalio/sdk-go#1227) — which
// task.Deployment.Apply never sets. So a pin host on a versioned deployment
// builds a real worker, and this test is what says so without a cluster:
// worker.New is where every WorkerOptions panic lives, and a lazy client makes
// reaching it free.
//
// It is a REGRESSION guard, not a claim about routing. Routing is measured
// against a real cluster by internal/engine's
// TestIntegration_AnOpenPinStaysOnItsHostAcrossAVersionRollout; if a future SDK
// starts panicking here, that measurement is unreachable and docs/DEPLOY.md's
// "Pins on a versioned deployment" has to become a refusal to boot instead.
func TestWorkerOptions_APinHostOnAVersionedDeploymentIsAcceptedByTheSDK(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "orkstack-prod")
	t.Setenv(task.EnvBuildID, "v2")
	t.Setenv(MaxPinsVar, "2")

	options := workerOptions(loadConfig())
	if !options.DeploymentOptions.UseVersioning || !options.EnableSessionWorker {
		t.Fatalf("this test asserts nothing: UseVersioning=%v EnableSessionWorker=%v",
			options.DeploymentOptions.UseVersioning, options.EnableSessionWorker)
	}
	//lint:ignore SA1019 asserting the deprecated flag stays unset is the point
	if options.UseBuildIDForVersioning {
		t.Fatal("Deployment.Apply set the deprecated UseBuildIDForVersioning; the SDK panics on " +
			"that flag together with EnableSessionWorker, so a pin host could not boot versioned")
	}

	// Lazy: no dial, no poller, and still the whole of worker.New — which is
	// where the SDK validates WorkerOptions and panics if it refuses them.
	c, err := client.NewLazyClient(client.Options{
		HostPort:  "127.0.0.1:1",
		Namespace: task.DefaultNamespace,
	})
	if err != nil {
		t.Fatalf("lazy client: %v", err)
	}
	defer c.Close()

	// run()'s own registrations, in run()'s order: a pin host registers
	// ReleasePin beside ExecuteTask, and registration is another panic path.
	w := worker.New(c, task.DefaultQueue, options)
	w.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return nil, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	w.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.ReleasePinInput) error { return nil },
		activity.RegisterOptions{Name: task.ActivityReleasePin},
	)
}

// TestLoadConfig_RefusesAPinCapItCannotRead applies the same rule as the
// heartbeat interval and ORK_ORKER_TASK_LOG_EXPORT: a value this process cannot
// read is a configuration error, not a quiet default.
//
// The consequence of getting this wrong is specific and confusing. A silently
// zero cap leaves an Orker that looks healthy and refuses every pinned submit
// to its pool with a message about no pin host existing — an error that names
// the symptom and never the typo.
func TestLoadConfig_RefusesAPinCapItCannotRead(t *testing.T) {
	for _, raw := range []string{"two", "-1", "1.5", "3 pins", "0x2"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(MaxPinsVar, raw)
			cfg := loadConfig()
			if cfg.maxPinsErr == nil {
				t.Fatalf("%s=%s booted, hosting %d pins", MaxPinsVar, raw, cfg.maxPins)
			}
			if !strings.Contains(cfg.maxPinsErr.Error(), MaxPinsVar) {
				t.Errorf("refusal %q does not name %s", cfg.maxPinsErr, MaxPinsVar)
			}
			if err := run(nil, telemetry.Config{}, nil, cfg); err == nil {
				t.Fatal("run() started with a pin cap it cannot read")
			}
		})
	}
}

// TestLoadConfig_AcceptsAPinCapItCanHonor is the other side, so the refusal
// above is a bound rather than a rejection of everything.
func TestLoadConfig_AcceptsAPinCapItCanHonor(t *testing.T) {
	for raw, want := range map[string]int32{"0": 0, "1": 1, "16": 16} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(MaxPinsVar, raw)
			cfg := loadConfig()
			if cfg.maxPinsErr != nil {
				t.Fatalf("%s refused: %v", raw, cfg.maxPinsErr)
			}
			if cfg.maxPins != want {
				t.Errorf("maxPins = %d, want %d", cfg.maxPins, want)
			}
			// An explicit 0 is exactly an unset one: it hosts no pins.
			if (cfg.maxPins > 0) != workerOptions(cfg).EnableSessionWorker {
				t.Errorf("maxPins = %d but EnableSessionWorker = %v", cfg.maxPins,
					workerOptions(cfg).EnableSessionWorker)
			}
		})
	}
}

// TestOrkerOptions_AdvertiseThePinCapTheWorkerPollsFor is the pairing, and it
// is the same shape as the heartbeat pairing above: either half alone is
// misleading.
//
// DescribeOrker's max_concurrent_pins is what submit reads to decide whether a
// pinned run's pool could ever open its pin. An Orker that advertised a
// capacity its worker does not poll for would accept the submit and then leave
// the run waiting out CreationTimeout for a slot that does not exist — the one
// outcome the submit-time probe exists to prevent. One config value must reach
// both.
func TestOrkerOptions_AdvertiseThePinCapTheWorkerPollsFor(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(MaxPinsVar, "5")
	cfg := loadConfig()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceOrker)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}
	opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger)

	if opts.MaxPins != 5 {
		t.Errorf("orker.Options.MaxPins = %d, want the configured 5", opts.MaxPins)
	}
	options := workerOptions(cfg)
	if int(opts.MaxPins) != options.MaxConcurrentSessionExecutionSize {
		t.Errorf("DescribeOrker will advertise %d pins while the worker polls for %d; the two "+
			"must be one number or submit accepts a run that can never open its pin",
			opts.MaxPins, options.MaxConcurrentSessionExecutionSize)
	}
}

// TestWorkerOptions_ZeroCordon_IsTheSameWorker: the cordon's Tuner runs the
// task worker at exactly the SDK defaults it ran at before a Tuner was set, a
// clear cordon reserves immediately, and a future edit that sets one of the
// four MaxConcurrent*ExecutionSize fields beside it is a named refusal rather
// than a bare SDK panic at boot.
func TestWorkerOptions_ZeroCordon_IsTheSameWorker(t *testing.T) {
	activities := orker.NewActivitiesWithOptions(nil, orker.Options{OrkerID: "orker-test", WorkRoot: t.TempDir()})
	cfg := loadConfig()
	options, err := taskWorkerOptions(cfg, activities)
	if err != nil {
		t.Fatal(err)
	}
	if options.Identity != "orker-test" {
		t.Fatalf("Identity = %q; want the orker id, which is what the presence channel lists", options.Identity)
	}
	if len(options.Interceptors) != 1 {
		t.Fatalf("Interceptors = %v; want the cordon interceptor", options.Interceptors)
	}
	tuner := options.Tuner
	if tuner == nil {
		t.Fatal("no Tuner: the cordon's slot supplier IS the Tuner")
	}
	for name, supplier := range map[string]worker.SlotSupplier{
		"workflow":         tuner.GetWorkflowTaskSlotSupplier(),
		"local activity":   tuner.GetLocalActivitySlotSupplier(),
		"nexus":            tuner.GetNexusSlotSupplier(),
		"session activity": tuner.GetSessionActivitySlotSupplier(),
	} {
		if supplier.MaxSlots() != 1000 {
			t.Errorf("%s supplier MaxSlots() = %d; want the SDK default 1000", name, supplier.MaxSlots())
		}
	}
	// The activity supplier alone is orker.max_concurrent, whose default is a
	// sizing decision (16), not the SDK's 1000.
	if got := tuner.GetActivityTaskSlotSupplier().MaxSlots(); got != orker.DefaultMaxConcurrent {
		t.Errorf("activity supplier MaxSlots() = %d; want orker.max_concurrent's default %d", got, orker.DefaultMaxConcurrent)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	permit, err := tuner.GetActivityTaskSlotSupplier().ReserveSlot(ctx, nil)
	if err != nil || permit == nil {
		t.Fatalf("a clear cordon did not reserve at once: %v", err)
	}

	if err := checkTunerCompatible(workerOptions(cfg)); err != nil {
		t.Fatalf("today's workerOptions refused: %v", err)
	}
	for field, set := range map[string]func(*worker.Options){
		"MaxConcurrentWorkflowTaskExecutionSize":  func(o *worker.Options) { o.MaxConcurrentWorkflowTaskExecutionSize = 1 },
		"MaxConcurrentActivityExecutionSize":      func(o *worker.Options) { o.MaxConcurrentActivityExecutionSize = 1 },
		"MaxConcurrentLocalActivityExecutionSize": func(o *worker.Options) { o.MaxConcurrentLocalActivityExecutionSize = 1 },
		"MaxConcurrentNexusTaskExecutionSize":     func(o *worker.Options) { o.MaxConcurrentNexusTaskExecutionSize = 1 },
	} {
		o := workerOptions(cfg)
		set(&o)
		err := checkTunerCompatible(o)
		if err == nil || !strings.Contains(err.Error(), field) || !strings.Contains(err.Error(), "ORKER_WORKER_OPTIONS_INVALID") {
			t.Errorf("checkTunerCompatible with %s set = %v; want ORKER_WORKER_OPTIONS_INVALID naming it", field, err)
		}
	}

	// The whole of worker.New for the three workers, on a lazy client: the
	// SDK validates its options (and panics on a Tuner beside a size) here.
	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1", Namespace: task.DefaultNamespace})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = worker.New(c, task.DefaultQueue, options)
	control, presence, err := orker.NewChannelWorkers(c, activities)
	if err != nil || control == nil || presence == nil {
		t.Fatalf("NewChannelWorkers = %v, %v, %v", control, presence, err)
	}
}

// TestControlChannelOptions_AreUnversionedAndOneProcess: the control and
// presence workers carry the orker id as identity, one poller, and no
// deployment options even when the task worker is versioned.
func TestControlChannelOptions_AreUnversionedAndOneProcess(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "orkstack-prod")
	t.Setenv(task.EnvBuildID, "v2")
	if !workerOptions(loadConfig()).DeploymentOptions.UseVersioning {
		t.Fatal("this test asserts nothing: the task worker is not versioned")
	}
	options := orker.ChannelWorkerOptions("orker-test")
	if options.Identity != "orker-test" || options.MaxConcurrentActivityTaskPollers != 1 ||
		!options.DisableWorkflowWorker || !options.DisableRegistrationAliasing {
		t.Fatalf("control options = %+v", options)
	}
	if options.DeploymentOptions.UseVersioning || options.DeploymentOptions.Version.BuildID != "" {
		t.Fatalf("the control channel is versioned: %+v", options.DeploymentOptions)
	}
}

// TestLoadConfig_MaxConcurrent is F-orker-storage-perf-1's boot half: an
// unset ceiling is the documented default, a readable one is carried to both
// the tuner and DescribeOrker through orker.Options, and one this process
// cannot honor is refused at boot naming the variable rather than clamped.
func TestLoadConfig_MaxConcurrent(t *testing.T) {
	clearTelemetryEnv(t)
	t.Run("unset is the default", func(t *testing.T) {
		cfg := loadConfig()
		// 16, spelled out rather than read back from the constant: an Orker
		// that sets nothing must boot with the sized default docs/DEPLOY.md
		// documents, not the Temporal SDK's 1000.
		if cfg.maxConcurrentErr != nil || cfg.maxConcurrent != 16 {
			t.Fatalf("maxConcurrent = %d, %v; want 16", cfg.maxConcurrent, cfg.maxConcurrentErr)
		}
	})
	for _, raw := range []string{"0", "-1", "two", "1.5"} {
		t.Run("refuses "+raw, func(t *testing.T) {
			t.Setenv(MaxConcurrentVar, raw)
			cfg := loadConfig()
			if cfg.maxConcurrentErr == nil || !strings.Contains(cfg.maxConcurrentErr.Error(), MaxConcurrentVar) {
				t.Fatalf("%s=%s: err = %v; want a refusal naming the variable", MaxConcurrentVar, raw, cfg.maxConcurrentErr)
			}
			if err := run(nil, telemetry.Config{}, nil, cfg); err == nil {
				t.Fatal("run() started with a ceiling it cannot read")
			}
		})
	}
	t.Run("carried to the activities", func(t *testing.T) {
		t.Setenv(MaxConcurrentVar, "3")
		cfg := loadConfig()
		if cfg.maxConcurrentErr != nil || cfg.maxConcurrent != 3 {
			t.Fatalf("maxConcurrent = %d, %v; want 3", cfg.maxConcurrent, cfg.maxConcurrentErr)
		}
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		tcfg, sinks, err := telemetry.Boot(telemetry.ServiceOrker)
		if err != nil {
			t.Fatal(err)
		}
		opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger)
		opts.OrkerID, opts.WorkRoot = "orker-test", t.TempDir()
		activities := orker.NewActivitiesWithOptions(nil, opts)
		options, err := taskWorkerOptions(cfg, activities)
		if err != nil {
			t.Fatal(err)
		}
		if got := options.Tuner.GetActivityTaskSlotSupplier().MaxSlots(); got != 3 {
			t.Fatalf("activity slots = %d; want 3", got)
		}
	})
}
