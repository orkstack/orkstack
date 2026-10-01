// Command orker runs an OrkStack Orker: a Temporal worker that executes tasks
// as OCI images. It polls its pool's execution channel for the ExecuteTask
// activity, materializes upstream artifacts, runs the image under the security
// profile, streams its log, and commits produced artifacts through
// pkg/artifacts.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	cfgfile "github.com/orkstack/orkstack/internal/config"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// version is the orker release. Release builds override it with
// -ldflags "-X main.version=vX.Y.Z" (see the Makefile's build target, which
// stamps every binary's own main.version with the same flag); a plain
// `go build ./cmd/orker` reports "dev". Same pattern, and the same variable
// name, as cmd/ork.
var version = "dev"

type config struct {
	temporalAddress   string
	temporalNamespace string
	// pool is the OrkStack name for the set of Orkers this process joins
	// (ORK_ORKER_POOL); queue is the raw execution channel it polls, which an
	// operator can pin directly with ORK_ORKER_QUEUE. The five ways the two
	// combine, and the one that is refused, live in task.ResolvePoolBinding —
	// both binaries resolve them there, or an Orker listens to one channel
	// while the engine dispatches to another and every run strands in RUNNING.
	pool  string
	queue string
	// rawOverride reports that queue was pinned by hand rather than derived
	// from pool, so the boot log can say which of the two was believed.
	rawOverride bool
	// poolErr is a refused pool/channel pair, reported at boot.
	poolErr error
	// deployment is this release's Worker Deployment identity. It MUST match
	// the engine's, or a pinned run's tasks are not routed to this Orker's
	// version at all — see workerOptions.
	deployment task.Deployment
	// imageLayoutRoot is ORK_ORKER_IMAGE_LAYOUT (§4.6): the OCI image layouts
	// this Orker may serve a PINNED digest from when a pull fails. It is a
	// cache with a proof, not a second source of truth — an image can only be
	// served from it if its digest was already resolved from a registry at
	// apply time.
	imageLayoutRoot string
	// registryAuthFile is ORK_ORKER_REGISTRY_AUTH_FILE: a docker config.json
	// used only for the manifest and blob GETs that verify a cosign signature
	// when a run's image policy names a signer. Empty verifies anonymously,
	// which public registries allow. Never mounted into a container.
	registryAuthFile string
	// taskLogExport is TaskLogExportVar: whether a task's stdout/stderr lines
	// are additionally exported as OTel log records, beside the durable chunk
	// store every attempt already writes to.
	taskLogExport bool
	// telemetryErr is a malformed TaskLogExportVar, reported at boot rather
	// than silently treated as false — the same rule this config applies to
	// poolErr.
	telemetryErr error
	// heartbeatInterval is orker.HeartbeatIntervalVar: how often a running
	// task's heartbeat actually reaches Temporal, and therefore how long a
	// cancellation waits before the Orker acts on it. It is used TWICE, and
	// both uses must carry the same number — see workerOptions.
	heartbeatInterval time.Duration
	// heartbeatErr is a malformed or out-of-range heartbeat interval, refused
	// at boot rather than clamped, by the same rule as the two errors above.
	heartbeatErr error
	// maxPins is MaxPinsVar: how many task pins this Orker hosts at once, and
	// therefore whether it is a PIN HOST at all. Zero — the default — leaves an
	// Orker that runs exactly as it did before pins existed: no session
	// pollers, no extra task queue, and DescribeOrker reporting that a pinned
	// submit to its pool must be refused rather than left waiting (§7.1, §7.2).
	maxPins int32
	// maxPinsErr is a malformed or negative pin cap, refused at boot by the
	// same rule as the three errors above.
	maxPinsErr error
}

// TaskLogExportVar turns on the OTel log export path for a task's
// stdout/stderr, over and above the durable chunk store every attempt
// already writes to (pkg/artifacts, via LogStore). It has no effect unless
// OTEL_LOGS_EXPORTER=otlp is also set — telemetry.NewTaskLogExporter answers
// nil otherwise, and this Orker logs that combination rather than silently
// exporting nothing.
const TaskLogExportVar = "ORK_ORKER_TASK_LOG_EXPORT"

// MaxPinsVar is orker.pins.max_concurrent: how many task pins this Orker hosts
// at once (docs/DEPLOY.md §1).
//
// The default is 0 rather than "on" because EnableSessionWorker adds two
// pollers and two task queues to the process (the SDK's session creation
// worker and its per-host session activity worker), and a deployment that never
// set the key must not acquire them.
const MaxPinsVar = "ORK_ORKER_MAX_PINS"

func loadConfig() config {
	binding, err := task.ResolvePoolBinding(env("ORK_ORKER_POOL", ""), env("ORK_ORKER_QUEUE", ""))
	taskLogExport, taskLogExportErr := envBool(TaskLogExportVar, false)
	heartbeatInterval, heartbeatErr := envHeartbeatInterval()
	maxPins, maxPinsErr := envMaxPins()
	return config{
		imageLayoutRoot:   env("ORK_ORKER_IMAGE_LAYOUT", orker.DefaultImageLayoutRoot),
		registryAuthFile:  env(orker.RegistryAuthFileVar, ""),
		temporalAddress:   env("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
		temporalNamespace: env("ORK_TEMPORAL_NAMESPACE", task.DefaultNamespace),
		pool:              binding.Pool,
		queue:             binding.Queue,
		rawOverride:       binding.RawOverride,
		poolErr:           err,
		taskLogExport:     taskLogExport,
		telemetryErr:      taskLogExportErr,
		heartbeatInterval: heartbeatInterval,
		heartbeatErr:      heartbeatErr,
		maxPins:           maxPins,
		maxPinsErr:        maxPinsErr,
		deployment:        task.DeploymentFromEnv(),
	}
}

// envMaxPins reads ORK_ORKER_MAX_PINS.
//
// Unset is the documented default of 0 — an Orker that hosts no pins. A value
// this process cannot read, or a negative one, comes back as an error for run()
// to refuse boot with, by the same rule envHeartbeatInterval applies: an
// operator who wrote `two` or `-1` believing they had opted a pool into pins
// must not get a silent 0, because every pinned submit to that pool would then
// be refused with a message about no pin host existing — the one error that
// would not name the cause.
func envMaxPins() (int32, error) {
	raw := env(MaxPinsVar, "")
	if raw == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf(
			"%s=%q is not a whole number of pins (want e.g. 0, 1, 4)", MaxPinsVar, raw)
	}
	if parsed < 0 {
		return 0, fmt.Errorf(
			"%s=%q must be zero or more; 0 hosts no pins and is the default", MaxPinsVar, raw)
	}
	return int32(parsed), nil
}

// envHeartbeatInterval reads ORK_ORKER_HEARTBEAT_INTERVAL.
//
// Unset is the documented default. A value this process cannot read, or one
// outside the bounds the heartbeat contract can honor, comes back as an error
// for run() to refuse boot with: an operator who wrote `500ms` believing they
// had bought a half-second cancellation must not get a silently clamped 5 s
// instead, and one who wrote `1h` must not get an Orker Temporal declares dead
// on every task.
func envHeartbeatInterval() (time.Duration, error) {
	raw := env(orker.HeartbeatIntervalVar, "")
	if raw == "" {
		return orker.DefaultHeartbeatInterval, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return orker.DefaultHeartbeatInterval, fmt.Errorf(
			"%s=%q is not a duration (want e.g. 5s, 2s, 1500ms)", orker.HeartbeatIntervalVar, raw)
	}
	// A written-out zero is not "unset": orker.ValidateHeartbeatInterval reads
	// zero as "the caller filled in no field", which is the right answer for a
	// Go struct and the wrong one for a variable somebody typed.
	if parsed <= 0 {
		return orker.DefaultHeartbeatInterval, fmt.Errorf(
			"%s=%q must be a positive duration between %s and %s",
			orker.HeartbeatIntervalVar, raw, orker.MinHeartbeatInterval, orker.MaxHeartbeatInterval)
	}
	if err := orker.ValidateHeartbeatInterval(parsed); err != nil {
		return orker.DefaultHeartbeatInterval, err
	}
	return parsed, nil
}

// tick is the heartbeat interval this Orker runs at, normalized. A zero field
// — a config nobody filled in — is the documented default, so the two places
// that must carry the SAME number (the worker's throttle cap and the
// dispatcher's loop) can never disagree about what "unset" means.
func (c config) tick() time.Duration {
	if c.heartbeatInterval <= 0 {
		return orker.DefaultHeartbeatInterval
	}
	return c.heartbeatInterval
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

// envBool reads a boolean switch. An unset variable is fallback with no
// error; a set-but-unparsable one is fallback WITH an error naming the
// variable, for the caller to refuse boot with rather than silently treat as
// off.
func envBool(key string, fallback bool) (bool, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback, fmt.Errorf("%s=%q must be true or false", key, raw)
	}
	return parsed, nil
}

// applyConfigFile is main()'s entire configuration-file mechanism
// (docs/DEPLOY.md §1); see cmd/ork's function of the same name for the full
// rationale (shared design, one package each side of it can Setenv/LookupEnv
// through fakes in a test rather than the real process environment).
func applyConfigFile(
	args []string, binary cfgfile.Binary,
	lookup func(string) (string, bool), setenv func(string, string) error,
) (doc *cfgfile.Document, path string, check bool, set []string, err error) {
	path, check, _, err = cfgfile.ParseArgs(args, lookup)
	if err != nil {
		return nil, "", check, nil, err
	}
	if path == "" {
		if check {
			return nil, "", true, nil, fmt.Errorf("--check requires --config or %s", cfgfile.ConfigFileVar)
		}
		return nil, "", false, nil, nil
	}
	doc, err = cfgfile.Load(path, binary)
	if err != nil {
		return nil, path, check, nil, err
	}
	if check {
		return doc, path, true, nil, nil
	}
	set, err = doc.Apply(lookup, setenv)
	if err != nil {
		return doc, path, false, nil, err
	}
	return doc, path, false, set, nil
}

func main() {
	// --defaults needs no config file, no environment, and nothing else
	// this func does below: print config.RenderDefaults() and stop — see
	// cmd/ork's main for the full rationale (shared design).
	if _, _, showDefaults, err := cfgfile.ParseArgs(os.Args[1:], os.LookupEnv); err == nil && showDefaults {
		fmt.Print(cfgfile.RenderDefaults())
		os.Exit(0)
	}

	// The configuration file, if any, is resolved before loadConfig reads a
	// single environment variable — see applyConfigFile — so every existing
	// *FromEnv reader below (loadConfig included) works unchanged whether its
	// value came from a file or, as always, straight from the environment.
	cfgDoc, cfgPath, cfgCheck, cfgSet, err := applyConfigFile(
		os.Args[1:], cfgfile.BinaryOrker, os.LookupEnv, os.Setenv)
	if err != nil {
		if cfgDoc == nil && cfgPath == "" {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "orker: %v\n", err)
		os.Exit(1)
	}
	if cfgCheck {
		fmt.Print(cfgDoc.Effective(cfgfile.BinaryOrker, os.LookupEnv))
		os.Exit(0)
	}

	cfg := loadConfig()
	// Telemetry configuration is resolved before the process logger exists,
	// for the same reason as cmd/ork: the logger's own encoding is itself a
	// telemetry setting, so building it first would mean rebuilding it the
	// moment ORK_LOG_FORMAT or ORK_LOG_LEVEL turned out to say otherwise.
	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceOrker, append(
		telemetry.ResourceAttrs(version, cfg.deployment.Name, cfg.deployment.BuildID, cfg.temporalNamespace),
		attribute.String(telemetry.AttrPool, cfg.pool))...)
	if err != nil {
		fallback := slog.New(slog.NewTextHandler(os.Stderr, nil))
		fallback.Error("orker stopped", "error", err)
		os.Exit(1)
	}
	logger := telemetry.NewLogger(tcfg, sinks.Default(), os.Stderr)
	// Attached as soon as the logger exists, so a task naming a sink
	// ORK_TELEMETRY_SINKS never declared logs a Warn from the very first
	// call, rather than only from whichever caller happens to attach one.
	sinks.WithLogger(logger)
	// Logged only when a file was actually given, so a zero-config boot's
	// log is unchanged from before this package existed.
	if cfgPath != "" {
		logger.Info("configuration file applied", "path", cfgPath, "applied", len(cfgSet))
	}
	if err := run(logger, tcfg, sinks, cfg); err != nil {
		logger.Error("orker stopped", "error", err)
		os.Exit(1)
	}
}

// workerOptions mirror the engine's expectation: an Orker is activity-only, so
// it never competes for workflow tasks on the shared queue.
//
// The deployment identity is the load-bearing half. Temporal sends a PINNED
// workflow's activities to the workflow's own Deployment Version, EXCEPT when
// no worker of that version polls the activity's task queue — then they go to
// the current version of that queue instead. So an Orker that does not carry
// the engine's exact (ORK_DEPLOYMENT_NAME, ORK_BUILD_ID) is invisible to a
// pinned run's routing, and a rollout would move an in-flight run's tasks onto
// a newer Orker build. `ork` and `orker` ship as one version: docs/DEPLOY.md.
//
// MaxHeartbeatThrottleInterval is the OTHER half of the heartbeat contract, and
// it is invisible until somebody cancels. The SDK does not send every
// RecordHeartbeat: it batches them into windows of
// min(0.8 × HeartbeatTimeout, MaxHeartbeatThrottleInterval) and delivers
// cancellation only in a heartbeat RPC's response. Left unset, that window is
// 24 s whatever rate the dispatcher's loop ticks at — measured: a failing run
// took 23.9 s to stop its still-running sibling, all of it waiting for one
// heartbeat. Capping it to the SAME number orkerOptions gives the dispatcher is
// what makes the tick mean anything.
// EnableSessionWorker is what makes this Orker a pin host, and it is the reason
// ORK_ORKER_MAX_PINS defaults to 0. With it set the SDK starts two more pollers
// beside the ordinary activity worker — a creation worker on
// <queue>__internal_session_creation and a session activity worker on
// <uuid>@<hostname> — and the second of those queue names is why no pin can
// survive a restart: the uuid is minted fresh per worker start. With it unset
// the SDK builds no session worker at all, so an Orker that never asked for
// pins acquires no poller and no queue it did not have before this feature.
func workerOptions(cfg config) worker.Options {
	return cfg.deployment.Apply(worker.Options{
		DisableRegistrationAliasing:  true,
		DisableWorkflowWorker:        true,
		MaxHeartbeatThrottleInterval: cfg.tick(),

		EnableSessionWorker:               cfg.maxPins > 0,
		MaxConcurrentSessionExecutionSize: int(cfg.maxPins),
	})
}

// taskWorkerOptions is workerOptions plus the three things only a booted
// Orker knows (orker-cordon-drain §5.7):
//
//   - Identity is the orker id, so the poller identity Temporal records for
//     every worker of this process — and therefore the presence channel's
//     poller list — is a list of Orker ids;
//   - Tuner is the cordon's: the SDK's default sizes, with the activity slot
//     supplier refusing to reserve while cordoned;
//   - Interceptors carries the cordon interceptor, which counts running tasks
//     and open pins and refuses a new pin while cordoned.
//
// checkTunerCompatible runs first, so a future edit that sets a size the SDK
// refuses beside a Tuner is a named refusal and not a panic in worker.New.
func taskWorkerOptions(cfg config, activities *orker.Activities) (worker.Options, error) {
	options := workerOptions(cfg)
	if err := checkTunerCompatible(options); err != nil {
		return worker.Options{}, err
	}
	tuner, err := activities.WorkerTuner()
	if err != nil {
		return worker.Options{}, fmt.Errorf("build the cordon's worker tuner: %w", err)
	}
	options.Identity = activities.Dispatcher().OrkerID
	options.Tuner = tuner
	options.Interceptors = []interceptor.WorkerInterceptor{activities.WorkerInterceptor()}
	return options, nil
}

// checkTunerCompatible refuses the options worker.New would PANIC on beside a
// Tuner (go.temporal.io/sdk internal_worker.go setWorkerOptionsDefaults,
// v1.47.0). MaxConcurrentSessionExecutionSize is not in that set and stays.
func checkTunerCompatible(opts worker.Options) error {
	for _, field := range []struct {
		name string
		set  bool
	}{
		{"MaxConcurrentWorkflowTaskExecutionSize", opts.MaxConcurrentWorkflowTaskExecutionSize != 0},
		{"MaxConcurrentActivityExecutionSize", opts.MaxConcurrentActivityExecutionSize != 0},
		{"MaxConcurrentLocalActivityExecutionSize", opts.MaxConcurrentLocalActivityExecutionSize != 0},
		{"MaxConcurrentNexusTaskExecutionSize", opts.MaxConcurrentNexusTaskExecutionSize != 0},
	} {
		if field.set {
			return fmt.Errorf("ORKER_WORKER_OPTIONS_INVALID: workerOptions sets %s, which the SDK refuses "+
				"beside a Tuner; the cordon's slot supplier IS the Tuner", field.name)
		}
	}
	return nil
}

func run(logger *slog.Logger, tcfg telemetry.Config, sinks *telemetry.Sinks, cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The admin listener opens before the Temporal dial, so /healthz answers
	// during boot rather than only once the task worker is polling. It is
	// wired to the default sink only, like every other process-level signal.
	adminAddr, stopAdmin, err := telemetry.StartAdmin(tcfg, sinks.Default(), logger)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), telemetry.ShutdownTimeout)
		defer cancel()
		if err := stopAdmin(shutdownCtx); err != nil {
			logger.Warn("admin listener shutdown", "error", err)
		}
	}()
	// Registered here, before the taskWorker.Stop defer below, so it runs
	// AFTER it: defers are LIFO, and every sink must keep flushing while the
	// worker still holds in-flight spans, metrics and task log records, not
	// before.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), telemetry.ShutdownTimeout)
		defer cancel()
		if err := sinks.Shutdown(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown", "error", err)
		}
	}()
	telemetry.LogBoot(logger, tcfg, sinks, adminAddr)

	// Fail on a deployment identity the cluster would reject later, while the
	// error can still name the variable that caused it.
	if err := cfg.deployment.Validate(); err != nil {
		return err
	}
	// Fail on a pool and a raw channel that disagree, before this Orker starts
	// polling somewhere the engine does not dispatch to.
	if cfg.poolErr != nil {
		return cfg.poolErr
	}
	// Fail on a malformed ORK_ORKER_TASK_LOG_EXPORT for the same reason: a
	// value this process cannot read is a configuration error, not a quiet
	// "task logs are not exported".
	if cfg.telemetryErr != nil {
		return cfg.telemetryErr
	}
	// Same rule again for the heartbeat tick. It is refused rather than
	// clamped because both the cancellation latency and the frontend-outage
	// tolerance an operator is choosing depend on it, and a silently corrected
	// value would make the boot log below disagree with what they asked for.
	if cfg.heartbeatErr != nil {
		return cfg.heartbeatErr
	}
	// And once more for the pin cap. An operator who asked for pins and got a
	// silently-zero cap would have an Orker that refuses every pinned submit to
	// its pool with a message about no pin host existing, which names the
	// symptom and not the typo.
	if cfg.maxPinsErr != nil {
		return cfg.maxPinsErr
	}

	store, err := artifacts.NewFromEnv()
	if err != nil {
		return err
	}
	commitStore, err := artifacts.NewCommitStoreFromEnv(ctx)
	if err != nil {
		return err
	}
	// Prove the backend's conditional create with simultaneous writers before
	// accepting activity work; "S3-compatible" alone does not promise this.
	if err := artifacts.ProbeCommitStore(ctx, commitStore); err != nil {
		return fmt.Errorf("COMMIT_STORE_NOT_CONDITIONAL: %w", err)
	}
	refPredictor, ok := store.(artifacts.RefPredictor)
	if !ok {
		return fmt.Errorf("artifact backend %T cannot predict content-addressed references", store)
	}

	// The live log sink's destination. It is the SAME backend the artifacts go
	// to, so an engine replica reading a task's log reads the bucket this
	// process wrote it into — no side channel between the two, and therefore
	// no run whose logs only one replica can serve.
	logStore, err := artifacts.NewLogStoreFromEnv()
	if err != nil {
		return err
	}
	logOptions := orker.LogOptionsFromEnv()

	// The per-attempt records, in the SAME backend as everything else — one
	// ORK_ARTIFACT_BACKEND, no setting of its own. An engine replica reading a
	// task's retry history reads the bucket this process wrote it into.
	attemptStore, err := artifacts.NewAttemptStoreFromEnv()
	if err != nil {
		return err
	}

	opts := orkerOptions(cfg, tcfg, sinks, logStore, logOptions, attemptStore, commitStore, refPredictor, logger)
	logger.Info("task log export ready", "enabled", opts.TaskLogs != nil)

	temporalOptions, err := telemetry.ClientOptions(cfg.temporalAddress, cfg.temporalNamespace, sinks.Default(), logger)
	if err != nil {
		return err
	}
	temporalClient, err := client.DialContext(ctx, temporalOptions)
	if err != nil {
		return err
	}
	defer temporalClient.Close()
	if err := task.EnsureNamespace(ctx, temporalClient, cfg.temporalNamespace); err != nil {
		return err
	}
	logger.Info("connected to temporal",
		"address", cfg.temporalAddress, "namespace", cfg.temporalNamespace)

	// One dispatcher runs every activity. There is no registry and no name
	// lookup: an activity is an OCI image (§6.1).
	activities := orker.NewActivitiesWithOptions(store, opts)
	// Refusing to start is the only honest response to an unbindable
	// workspace root or an orphaned container this Orker cannot account for:
	// starting anyway would fail every task later with an error naming the
	// container rather than the cause (§3.1, §5.5).
	if err := activities.Boot(ctx); err != nil {
		return fmt.Errorf("orker boot checks failed: %w", err)
	}
	// The pin lease sweep: the backstop for a pin directory whose run was
	// terminated from outside, where Temporal frees the session and no OrkStack
	// code ever runs (§8.4). It ticks on ctx — the process's own signal context
	// — so it stops at shutdown rather than outliving run(), and it never
	// blocks this function's return.
	//
	// Started unconditionally, not only for a pin host. See
	// orker.Activities.SweepPinLeases for why: it is one ReadDir a minute, the
	// boot reap above is unconditional for the same reason, and gating it would
	// leave the one case nothing else collects — a pinned input that reached a
	// non-pin-hosting Orker's ordinary queue — with no collector at all.
	go activities.SweepPinLeases(ctx)
	taskOptions, err := taskWorkerOptions(cfg, activities)
	if err != nil {
		return err
	}
	taskWorker := worker.New(temporalClient, cfg.queue, taskOptions)
	taskWorker.RegisterActivityWithOptions(
		activities.ExecuteTask,
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	taskWorker.RegisterActivityWithOptions(
		activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker},
	)
	// Registered unconditionally, on the ordinary worker. The engine schedules
	// it on the pin's own SESSION context, which is what routes it to the host
	// that owns the directory (§8.5); an Orker with no pins registers a name
	// nobody will ever schedule, which costs nothing and keeps the registration
	// block one shape.
	taskWorker.RegisterActivityWithOptions(
		activities.ReleasePin,
		activity.RegisterOptions{Name: task.ActivityReleasePin},
	)

	// The control and presence workers are built before anything starts, so a
	// refusal leaves nothing polling.
	controlWorker, presenceWorker, err := orker.NewChannelWorkers(temporalClient, activities)
	if err != nil {
		return err
	}
	if err := taskWorker.Start(); err != nil {
		return err
	}
	// Started after the task worker; stopped AFTER it too, in one defer, so the
	// control channel keeps answering DescribeOrker (and the presence channel
	// keeps this Orker in the inventory) while the task worker stops and
	// drains its in-flight work on SIGTERM.
	var controlStarted, presenceStarted bool
	defer func() {
		taskWorker.Stop()
		if controlStarted {
			controlWorker.Stop()
		}
		if presenceStarted {
			presenceWorker.Stop()
		}
	}()
	if err := controlWorker.Start(); err != nil {
		return err
	}
	controlStarted = true
	if err := presenceWorker.Start(); err != nil {
		return err
	}
	presenceStarted = true
	// /readyz flips to 200 only once all three workers are actually polling —
	// the point past which this replica should be counted on to pick up work
	// and to answer on its own channel.
	sinks.Default().Ready.Set(true)

	logger.Info("orker started",
		"pool", cfg.pool, "task_queue", cfg.queue,
		"activity", task.ActivityExecuteTask,
		"probe", task.ActivityDescribeOrker,
		"orker_id", activities.Dispatcher().OrkerID,
		// Every Orker boots ready; a cordon is process-local and does not
		// survive a restart.
		"cordon", false,
		"security_profile", activities.Dispatcher().Policy.ProfileID(),
		// Whether this Orker is a pin host, and for how many pins at once. 0 is
		// said out loud rather than omitted: it is the answer to "why was my
		// pinned run refused at submit".
		"max_pins", cfg.maxPins,
		// The cancellation latency an operator is running with, said out loud:
		// this is how long a running container can keep running after the
		// engine asks for it to stop, and the longest Temporal outage one
		// survives.
		"heartbeat_interval", cfg.tick())
	logger.Info("task log streaming ready",
		"flush_bytes", logOptions.FlushBytes,
		"flush_interval", logOptions.FlushInterval,
		"max_bytes_per_attempt", logOptions.MaxBytes)
	if cfg.rawOverride {
		// The channel was pinned by hand. Say so, because a raw channel this
		// process did not derive from a pool name is not necessarily any
		// pool's, and "pool" above is then a label rather than an answer.
		logger.Info("execution channel pinned by ORK_ORKER_QUEUE",
			"task_queue", cfg.queue, "reported_pool", cfg.pool)
	}
	// The engine prints the same pair. They must match, or this Orker is not
	// in a pinned run's deployment version and its tasks are routed elsewhere.
	logger.Info("worker deployment versioning",
		"enabled", cfg.deployment.Versioned(),
		"deployment", cfg.deployment.Name,
		"build_id", cfg.deployment.BuildID,
		"version", cfg.deployment.String())

	<-ctx.Done()
	sinks.Default().Ready.Set(false)
	logger.Info("shutting down")
	return nil
}

// orkerOptions builds the orker.Options this Orker's activities run with,
// including the three telemetry-derived seams: TaskLogs, Phases and
// TraceContext. It is a single seam so a test can assert what those three
// fields would be for a given (cfg, tcfg) without booting Temporal, the
// artifact stores or the container runtime.
func orkerOptions(
	cfg config, tcfg telemetry.Config, sinks *telemetry.Sinks,
	logStore artifacts.LogStore, logOptions orker.LogOptions,
	attemptStore artifacts.AttemptStore,
	commitStore artifacts.CommitStore, refPredictor artifacts.RefPredictor,
	logger *slog.Logger,
) orker.Options {
	opts := orker.Options{
		Logs:     logStore,
		Log:      logOptions,
		Attempts: attemptStore,
		Commits:  commitStore,
		Refs:     refPredictor,
		Pool:     cfg.pool,
		BuildID:  cfg.deployment.BuildID,
		// What DescribeOrker reports, and therefore what submit reads to decide
		// whether a pinned task's pool could ever open its pin (§7.3, §7.4). It
		// is the SAME number workerOptions turns EnableSessionWorker on with, so
		// an Orker cannot advertise a capacity its worker does not poll for.
		MaxPins: cfg.maxPins,
		// The embedded OCI-layout fallback (§4.6). It is used ONLY when a
		// pull of an already-pinned digest fails, and only after OrkStack has
		// hashed the layout's root blob itself and matched it against the
		// pinned index digest — `docker load` verifies nothing about the root
		// (§14.15), so a filename is a claim rather than a digest. An absent
		// directory simply disables the fallback.
		LayoutRoot: cfg.imageLayoutRoot,
		// The image policy's signature check (image-supply-chain-policy
		// §6.2): always built — an empty auth file is anonymous access,
		// which is what a public registry needs — over the engine's one
		// resolver, so the Orker reads a signature exactly as apply did.
		Signatures: &dsl.SignatureVerifier{
			Resolver:    dsl.NewResolver(cfg.registryAuthFile),
			AuthFileVar: orker.RegistryAuthFileVar,
		},
		// The dispatcher's half of the pairing workerOptions describes. Both
		// read cfg.tick(), so there is one number and no way for the loop and
		// the SDK's batch window to drift apart.
		HeartbeatInterval: cfg.tick(),
		Logger:            logger,
	}

	// Task log export: an ADDITIONAL best-effort path from a task's
	// stdout/stderr to a log provider, beside the durable chunk store above.
	// Built whenever logs are on globally OR a named sink exists at all —
	// NewTaskLogExporterForSinks always returns non-nil, because a
	// namespace's own TelemetrySelection.TaskLogs can turn logs on for its
	// own runs on a named sink even with the deployment-wide switch off, or
	// the reverse, and only Open (per attempt) can tell which. cfg.taskLogExport
	// is carried through as globalExport — the switch Open falls back to when
	// an attempt names no selection of its own.
	if tcfg.LogsEnabled() || tcfg.Sinks.Len() > 0 {
		exporter := telemetry.NewTaskLogExporterForSinks(sinks, logOptions.MaxLineBytes, cfg.taskLogExport)
		opts.TaskLogs = orker.TaskLogExporterFunc(
			func(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32) orker.TaskLogWriter {
				return exporter.Open(ctx, in, attempt)
			})
	} else if cfg.taskLogExport {
		// The switch was asked for, but nothing is configured to carry it:
		// neither the deployment-wide OTLP logs exporter nor a single named
		// sink. Say so — an operator who set ORK_ORKER_TASK_LOG_EXPORT=true
		// believing it worked would otherwise never learn it silently did
		// nothing.
		logger.Warn(TaskLogExportVar + "=true but OTEL_LOGS_EXPORTER is not otlp and no sink is configured; task logs are not exported")
	}

	// Traces and metrics: an Orker attempt's spans and ork.orker.* metrics,
	// routed per-attempt to whatever sink ExecuteTaskInput.GetTelemetry()
	// names (OrkerObserver.Begin). TraceContext is wired only under tracing,
	// because it is what writes the optional `trace` key into task.json —
	// a key naming a span nobody collects is worse than an absent key.
	if tcfg.TracesEnabled() || tcfg.MetricsEnabled() {
		obs := telemetry.NewOrkerObserverForSinks(sinks, logger)
		opts.Phases = obs
		if tcfg.TracesEnabled() {
			opts.TraceContext = obs.TraceContext
		}
	}

	return opts
}
