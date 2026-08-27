// Command orker runs an OrkStack Orker: a generic Temporal worker that executes
// tasks by dispatching to registered plugins. It polls the task queue for the
// ExecuteTask activity, renders task config, materializes upstream artifacts,
// runs the plugin, and uploads produced artifacts through pkg/artifacts.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

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
}

func loadConfig() config {
	binding, err := task.ResolvePoolBinding(env("ORK_ORKER_POOL", ""), env("ORK_ORKER_QUEUE", ""))
	return config{
		temporalAddress:   env("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
		temporalNamespace: env("ORK_TEMPORAL_NAMESPACE", task.DefaultNamespace),
		pool:              binding.Pool,
		queue:             binding.Queue,
		rawOverride:       binding.RawOverride,
		poolErr:           err,
		deployment:        task.DeploymentFromEnv(),
	}
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger, loadConfig()); err != nil {
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
// new plugin code. `ork` and `orker` ship as one version: docs/DEPLOY.md.
func workerOptions(cfg config) worker.Options {
	return cfg.deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
}

func run(logger *slog.Logger, cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort:  cfg.temporalAddress,
		Namespace: cfg.temporalNamespace,
		Logger:    logger,
	})
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
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Logs:    logStore,
		Log:     logOptions,
		Commits: commitStore,
		Refs:    refPredictor,
		Pool:    cfg.pool,
		BuildID: cfg.deployment.BuildID,
	})
	// Refusing to start is the only honest response to an unbindable
	// workspace root or an orphaned container this Orker cannot account for:
	// starting anyway would fail every task later with an error naming the
	// container rather than the cause (§3.1, §5.5).
	if err := activities.Boot(ctx); err != nil {
		return fmt.Errorf("orker boot checks failed: %w", err)
	}
	taskWorker := worker.New(temporalClient, cfg.queue, workerOptions(cfg))
	taskWorker.RegisterActivityWithOptions(
		activities.ExecuteTask,
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	taskWorker.RegisterActivityWithOptions(
		activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker},
	)

	if err := taskWorker.Start(); err != nil {
		return err
	}
	defer taskWorker.Stop()

	logger.Info("orker started",
		"pool", cfg.pool, "task_queue", cfg.queue,
		"activity", task.ActivityExecuteTask,
		"probe", task.ActivityDescribeOrker,
		"orker_id", activities.Dispatcher().OrkerID,
		"security_profile", activities.Dispatcher().Policy.ProfileID())
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
	logger.Info("shutting down")
	return nil
}
