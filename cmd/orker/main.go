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
}

func loadConfig() config {
	return config{
		temporalAddress:   env("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
		temporalNamespace: env("ORK_TEMPORAL_NAMESPACE", client.DefaultNamespace),
		queue:             env("ORK_ORKER_QUEUE", task.DefaultQueue),
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
func workerOptions() worker.Options {
	return worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}
}

func run(logger *slog.Logger, cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
	logger.Info("connected to temporal",
		"address", cfg.temporalAddress, "namespace", cfg.temporalNamespace)

	registry := orker.BuiltinRegistry()
	activities := orker.NewActivities(registry, store)
	taskWorker := worker.New(temporalClient, cfg.queue, workerOptions())
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

	<-ctx.Done()
	logger.Info("shutting down")
	return nil
}
