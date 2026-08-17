// Command orker runs an OrkStack Orker: a generic Temporal worker that executes
// tasks by dispatching to registered plugins. It polls the task queue for the
// ExecuteTask activity, renders task config, materializes upstream artifacts,
// runs the plugin, and uploads produced artifacts through pkg/artifacts.
package main

import (
	"context"
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
	queue             string
	// deployment is this release's Worker Deployment identity. It MUST match
	// the engine's, or a pinned run's tasks are not routed to this Orker's
	// version at all — see workerOptions.
	deployment task.Deployment
}

func loadConfig() config {
	return config{
		temporalAddress:   env("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
		temporalNamespace: env("ORK_TEMPORAL_NAMESPACE", task.DefaultNamespace),
		queue:             env("ORK_ORKER_QUEUE", task.DefaultQueue),
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

	store, err := artifacts.NewFromEnv()
	if err != nil {
		return err
	}

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

	registry := orker.BuiltinRegistry()
	activities := orker.NewActivities(registry, store)
	taskWorker := worker.New(temporalClient, cfg.queue, workerOptions(cfg))
	taskWorker.RegisterActivityWithOptions(
		activities.ExecuteTask,
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	taskWorker.RegisterActivityWithOptions(
		activities.ListPlugins,
		activity.RegisterOptions{Name: task.ActivityListPlugins},
	)

	if err := taskWorker.Start(); err != nil {
		return err
	}
	defer taskWorker.Stop()

	names := make([]string, 0, len(registry.List()))
	for _, descriptor := range registry.List() {
		names = append(names, descriptor.Name)
	}
	logger.Info("orker started",
		"task_queue", cfg.queue, "activity", task.ActivityExecuteTask, "plugins", names)
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
