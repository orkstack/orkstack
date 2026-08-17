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
	// deployment is this release's Worker Deployment identity. It carries no
	// build id unless an operator configures one, which leaves versioning off
	// and the workers exactly as they were before M8.4.
	deployment task.Deployment
}

func loadConfig() config {
	cfg := config{
		temporalAddress:   env("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
		temporalNamespace: env("ORK_TEMPORAL_NAMESPACE", task.DefaultNamespace),
		httpAddr:          env("ORK_HTTP_ADDR", api.DefaultHTTPAddr),
		orkerQueue:        env("ORK_ORKER_QUEUE", task.DefaultQueue),
		idempotencyTTL:    envDuration("ORK_IDEMPOTENCY_TTL", engine.DefaultClaimTTL),
		deployment:        task.DeploymentFromEnv(),
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

// runWorkerOptions configures the worker that hosts RunWorkflow and the
// idempotency claims. It carries the deployment identity, because the run
// workflow is what a version pins: an in-flight run must keep executing on the
// build that started it.
func runWorkerOptions(cfg config) worker.Options {
	return cfg.deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
}

// eventWorkerOptions configures the projection worker, and deliberately does
// NOT version it — which is why it ignores the config it is handed.
//
// PublishEvent carries no run semantics: it writes the read model and the
// archive from arguments the workflow already computed. It must stay
// executable by ANY live replica whatever version that replica is, which is
// what keeps a run readable from every replica (M8.3). With no versioned
// poller on ork-engine-events, Temporal's pinned-activity exception clause
// routes these activities to that queue's own current version — the
// unversioned pollers. The rollout integration test asserts exactly this: a
// v1-pinned run's remaining events are published by a replica that came up
// after v1 stopped projecting.
func eventWorkerOptions(config) worker.Options {
	return worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}
}

// workflowRegistrar is the slice of worker.Worker that registers workflow
// types. Naming it lets a test register the engine's workflows through the
// same code the binary runs, so a lost VersioningBehavior fails a test rather
// than only a production rollout.
type workflowRegistrar interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
}

// registerEngineWorkflows registers the engine's workflow types, each with the
// versioning behaviour it needs. Under Worker Deployment Versioning a workflow
// type with no declared behaviour panics at registration, so this list is also
// the statement that both types have been considered.
func registerEngineWorkflows(w workflowRegistrar) {
	// Pinned: an in-flight run finishes on the code that started it.
	//
	// RunWorkflow interprets a DAG whose scheduling order, event sequence and
	// activity arguments are all decided by current code. Change readyTasks'
	// ordering, the emit counter, or the shape of task.ExecuteTaskInput, and
	// replaying a half-finished history against the new build produces a
	// different command stream — a non-determinism error that kills a run the
	// operator did nothing to. AutoUpgrade would hand exactly that history to
	// the new build on its next workflow task. Pinned keeps the run on its own
	// version; the price is that v1 workers must stay up until the deployment
	// version reports Drained (docs/DEPLOY.md).
	w.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{
		Name:               engine.WorkflowName,
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})

	// AutoUpgrade: the claim is a long-lived key holder, not a run.
	//
	// Its history is one update and one timer, and its command stream is
	// independent of run semantics — replaying it against a newer build issues
	// the same commands — so the determinism argument above does not apply. It
	// lives for ORK_IDEMPOTENCY_TTL (default 24h), so pinning it would strand
	// a key holder on a version an operator wants to decommission, and that
	// version would never report Drained.
	w.RegisterWorkflowWithOptions(engine.IdempotencyClaimWorkflow, workflow.RegisterOptions{
		Name:               engine.ClaimWorkflowName,
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
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

	// Fail on a deployment identity the cluster would reject later, while the
	// error can still name the variable that caused it.
	if err := cfg.deployment.Validate(); err != nil {
		return err
	}

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
	runWorker := worker.New(temporalClient, task.DefaultQueue, runWorkerOptions(cfg))
	registerEngineWorkflows(runWorker)

	// Projection worker: engine-internal activities on a dedicated queue so
	// Orkers never receive them. The archive body's event log is read back
	// from Temporal, because with more than one replica polling this queue no
	// single process executes all of a run's events.
	eventWorker := worker.New(temporalClient, engine.EventQueue, eventWorkerOptions(cfg))
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
	// The versioning line is separate and always printed, because "which
	// version am I" is the first question of any rollout. The Orker must
	// report the same pair, or a pinned run's tasks fall through to the task
	// queue's current version and land on the wrong plugin code.
	logger.Info("worker deployment versioning",
		"enabled", cfg.deployment.Versioned(),
		"deployment", cfg.deployment.Name,
		"build_id", cfg.deployment.BuildID,
		"version", cfg.deployment.String(),
		"run_workflow_behavior", "Pinned",
		"claim_workflow_behavior", "AutoUpgrade",
		"event_worker", "unversioned")
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
