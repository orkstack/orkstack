// Command ork runs the OrkStack control plane: the Temporal worker hosting
// RunWorkflow, the engine's event projection worker, and the Connect API plus
// WebSocket event bridge.
//
// State lives outside the process. Runs live in Temporal; orkflow definitions
// live in the S3 bucket (metadata/orkflows/{name}/{version}.json), read on
// every call. Restarting ork therefore loses nothing.
package main

import (
	"context"
	"errors"
	"fmt"
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

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

const (
	shutdownTimeout = 10 * time.Second
	// webhookDrainTimeout bounds how long shutdown waits for in-flight
	// webhook deliveries, so one unresponsive subscriber cannot hold the
	// engine open.
	webhookDrainTimeout = 15 * time.Second
)

type config struct {
	temporalAddress   string
	temporalNamespace string
	httpAddr          string
	allowedOrigins    []string
	orkerQueue        string
	idempotencyTTL    time.Duration
}

func loadConfig() config {
	cfg := config{
		temporalAddress:   env("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
		temporalNamespace: env("ORK_TEMPORAL_NAMESPACE", task.DefaultNamespace),
		httpAddr:          env("ORK_HTTP_ADDR", api.DefaultHTTPAddr),
		orkerQueue:        env("ORK_ORKER_QUEUE", task.DefaultQueue),
		idempotencyTTL:    envDuration("ORK_IDEMPOTENCY_TTL", engine.DefaultClaimTTL),
	}
	if origins := env("ORK_WS_ALLOWED_ORIGINS", ""); origins != "" {
		cfg.allowedOrigins = strings.Split(origins, ",")
	}
	return cfg
}

// envDuration reads a Go duration ("24h", "30m"). An unparsable value falls
// back rather than failing boot: the engine still runs correctly with the
// default, and a refused start would be a worse answer to a typo.
func envDuration(key string, fallback time.Duration) time.Duration {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

// archiveLocation renders where archived runs land, for the boot log.
func archiveLocation(cfg artifacts.Config) string {
	if cfg.Backend == artifacts.BackendS3 {
		return "s3://" + cfg.S3Bucket + "/" + artifacts.RunArchiveNamespace
	}
	return cfg.LocalDir + "/" + artifacts.RunArchiveNamespace
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger, loadConfig()); err != nil {
		logger.Error("ork stopped", "error", err)
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

// openOrkflowStore connects the engine to the bucket that holds orkflow
// definitions and reloads the catalog from it.
//
// The reload is the restart path: the store keeps no cache, so listing it here
// both proves the bucket is reachable before the API accepts an apply and logs
// exactly which orkflows this engine came up with. An unreachable bucket is
// fatal — an engine that cannot read orkflows cannot run them, and answering
// from memory is what this milestone removed.
func openOrkflowStore(ctx context.Context, logger *slog.Logger) (*artifacts.S3OrkflowStore, error) {
	cfg := artifacts.ConfigFromEnv()
	store, err := artifacts.NewS3OrkflowStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open orkflow store: %w", err)
	}
	stored, err := store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("reload orkflows from %s (endpoint %s): %w",
			store.Location(), cfg.S3Endpoint, err)
	}
	logger.Info("reloaded orkflows from durable store",
		"store", store.Location(),
		"endpoint", cfg.S3Endpoint,
		"orkflows", len(stored),
		"names", orkflowNames(stored))
	return store, nil
}

// orkflowNames renders a catalog as name@version, for the boot log line.
func orkflowNames(stored []*orkstackv1.Orkflow) []string {
	names := make([]string, 0, len(stored))
	for _, wf := range stored {
		names = append(names, wf.GetName()+"@"+wf.GetVersion())
	}
	return names
}

func run(logger *slog.Logger, cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	orkflows, err := openOrkflowStore(ctx, logger)
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

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()

	// Webhooks: the outbound half of the API. The registry holds who is
	// subscribed, the notifier fans a run's state transitions out to them.
	webhookRegistry := webhooks.NewRegistry()
	webhookNotifier := webhooks.NewNotifier(
		webhookRegistry,
		webhooks.NewDispatcher(webhooks.DispatcherOptions{}),
		webhooks.NotifierOptions{Logger: logger},
	)
	runNotifier := api.NewWebhookNotifier(webhookNotifier, logger)
	defer func() {
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), webhookDrainTimeout)
		defer cancelDrain()
		if err := webhookNotifier.Wait(drainCtx); err != nil {
			logger.Warn("webhook deliveries still in flight at shutdown", "error", err)
		}
	}()

	// Run archive: the record for a finished run. Temporal is the record for a
	// live one (engine.TemporalRunReader below), and the archive outlives its
	// retention, so between them nothing durable lives in this process.
	archiveConfig := artifacts.ConfigFromEnv()
	archiveStore, err := artifacts.NewRunArchive(archiveConfig)
	if err != nil {
		return err
	}
	archive := engine.NewRunArchiver(archiveStore)
	logger.Info("run archive ready",
		"backend", archiveConfig.Backend, "location", archiveLocation(archiveConfig))

	// The read model. Every run read leaves this process: a run's history is
	// its event log, and Temporal hands the same answer to every replica, for
	// runs this one neither started nor projected.
	reader := engine.NewTemporalRunReader(temporalClient)

	// Workflow worker: hosts RunWorkflow on the same queue Orkers poll for
	// task activities, and the idempotency claims, which are engine state no
	// Orker ever sees.
	runWorker := worker.New(temporalClient, task.DefaultQueue, runWorkerOptions())
	runWorker.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{
		Name: engine.WorkflowName,
	})
	runWorker.RegisterWorkflowWithOptions(engine.IdempotencyClaimWorkflow, workflow.RegisterOptions{
		Name: engine.ClaimWorkflowName,
	})

	// Projection worker: engine-internal activities on a dedicated queue so
	// Orkers never receive them. The archive body's event log is read back
	// from Temporal, because with more than one replica polling this queue no
	// single process executes all of a run's events.
	eventWorker := worker.New(temporalClient, engine.EventQueue, eventWorkerOptions())
	eventWorker.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(bus, runs, archive).
			WithRunNotifier(runNotifier).
			WithEventSource(reader).
			PublishEvent,
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
		"task_queue", task.DefaultQueue, "event_queue", engine.EventQueue,
		"orker_queue", cfg.orkerQueue)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, cfg.orkerQueue)

	// Idempotency: the cluster decides which run a key names, so every replica
	// behind this address gets the same answer. Nothing about dedupe lives in
	// this process.
	idempotency := engine.NewTemporalIdempotency(temporalClient, task.DefaultQueue, cfg.idempotencyTTL)
	logger.Info("idempotency claims ready",
		"workflow", engine.ClaimWorkflowName, "ttl", cfg.idempotencyTTL)

	svc := api.NewService(api.Config{
		Workflows:   orkflows,
		Reader:      reader,
		Runs:        runs,
		Bus:         bus,
		Starter:     starter,
		Lister:      starter,
		Archive:     archive,
		Idempotency: idempotency,
		Webhooks:    webhookRegistry,
		Notifier:    runNotifier,
	})
	server := api.NewServer(cfg.httpAddr, api.NewMux(svc, api.MuxOptions{
		AllowedOrigins: cfg.allowedOrigins,
	}))
	cancelServerHandlers := installServerShutdownContext(server)
	defer cancelServerHandlers()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("serving connect api (h2c), websocket bridge and webhooks",
			"addr", cfg.httpAddr, "webhooks", api.WebhookCollectionPath)
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
