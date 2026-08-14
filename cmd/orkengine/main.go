// Command orkengine runs the OrkStack control plane: the Temporal worker
// hosting RunWorkflow, the engine's event projection worker, and the Connect
// API plus WebSocket event bridge.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

const shutdownTimeout = 10 * time.Second

type config struct {
	temporalAddress   string
	temporalNamespace string
	httpAddr          string
	allowedOrigins    []string
}

func loadConfig() config {
	cfg := config{
		temporalAddress:   env("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
		temporalNamespace: env("ORK_TEMPORAL_NAMESPACE", client.DefaultNamespace),
		httpAddr:          env("ORK_HTTP_ADDR", api.DefaultHTTPAddr),
	}
	if origins := env("ORK_WS_ALLOWED_ORIGINS", ""); origins != "" {
		cfg.allowedOrigins = strings.Split(origins, ",")
	}
	return cfg
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
		logger.Error("orkengine stopped", "error", err)
		os.Exit(1)
	}
}

func runWorkerOptions() worker.Options {
	return worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}
}

func eventWorkerOptions() worker.Options {
	return worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}
}

// installServerShutdownContext gives all HTTP handlers a shared lifetime and
// cancels it when Shutdown begins. Long-lived Connect and WebSocket streams
// then return before http.Server waits for active connections to drain.
func installServerShutdownContext(server *http.Server) context.CancelFunc {
	handlerCtx, cancel := context.WithCancel(context.Background())
	server.BaseContext = func(net.Listener) context.Context { return handlerCtx }
	server.RegisterOnShutdown(cancel)
	return cancel
}

func run(logger *slog.Logger, cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()

	// Workflow worker: hosts RunWorkflow on the same queue Orkers poll for
	// task activities.
	runWorker := worker.New(temporalClient, task.DefaultQueue, runWorkerOptions())
	runWorker.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{
		Name: engine.WorkflowName,
	})

	// Projection worker: engine-internal activities on a dedicated queue so
	// Orkers never receive them.
	eventWorker := worker.New(temporalClient, engine.EventQueue, eventWorkerOptions())
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	if err := runWorker.Start(); err != nil {
		return err
	}
	defer runWorker.Stop()
	if err := eventWorker.Start(); err != nil {
		return err
	}
	defer eventWorker.Stop()
	logger.Info("temporal workers started",
		"task_queue", task.DefaultQueue, "event_queue", engine.EventQueue)

	svc := api.NewService(api.Config{
		Workflows: engine.NewWorkflowStore(),
		Runs:      runs,
		Bus:       bus,
		Starter:   engine.NewTemporalStarter(temporalClient, task.DefaultQueue),
	})
	server := api.NewServer(cfg.httpAddr, api.NewMux(svc, api.MuxOptions{
		AllowedOrigins: cfg.allowedOrigins,
	}))
	cancelServerHandlers := installServerShutdownContext(server)
	defer cancelServerHandlers()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("serving connect api (h2c) and websocket bridge", "addr", cfg.httpAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		logger.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return <-serverErr
}
