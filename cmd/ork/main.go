// Command ork runs the OrkStack control plane: the Temporal worker hosting
// RunWorkflow, the engine's event projection worker, and the Connect API plus
// WebSocket event bridge.
//
// State lives outside the process. Runs live in Temporal; orkflow definitions
// live in the S3 bucket (metadata/orkflows/{namespace}/{name}/{version}.json),
// read on every call. Restarting ork therefore loses nothing.
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
	// pool is the OrkStack name for where this engine dispatches a task that
	// names no pool of its own; orkerQueue is the raw execution channel that
	// pool resolves to. See task.ResolvePoolBinding for the five ways the two
	// environment variables combine, and for the one that is refused.
	pool       string
	orkerQueue string
	// defaultRouteOverride is the channel an operator pinned the default route
	// to, or empty when neither variable was set. Empty is what keeps a
	// namespace's own default_pool meaningful: with no override, the default
	// pool resolves to its own channel like every other pool.
	defaultRouteOverride string
	// poolErr is a refused ORK_ORKER_POOL/ORK_ORKER_QUEUE pair, reported at
	// boot rather than resolved by picking a winner.
	poolErr        error
	idempotencyTTL time.Duration
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
		idempotencyTTL:    envDuration("ORK_IDEMPOTENCY_TTL", engine.DefaultClaimTTL),
		deployment:        task.DeploymentFromEnv(),
	}
	rawPool := env("ORK_ORKER_POOL", "")
	rawQueue := env("ORK_ORKER_QUEUE", "")
	binding, err := task.ResolvePoolBinding(rawPool, rawQueue)
	cfg.pool, cfg.orkerQueue, cfg.poolErr = binding.Pool, binding.Queue, err
	if err == nil && (rawPool != "" || rawQueue != "") {
		cfg.defaultRouteOverride = binding.Queue
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

// logLocation renders where task log chunks land, for the boot log.
func logLocation(cfg artifacts.Config) string {
	if cfg.Backend == artifacts.BackendS3 {
		return "s3://" + cfg.S3Bucket + "/" + artifacts.LogPrefix
	}
	return cfg.LocalDir + "/" + artifacts.LogPrefix
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger, loadConfig()); err != nil {
		logger.Error("ork stopped", "error", err)
		os.Exit(1)
	}
}

// queues names every Temporal queue this engine touches. It exists because
// which queue carries what is an operational claim (docs/DEPLOY.md §2) that
// used to live only in a comment and a log line, where it was wrong: the run
// worker polls task.DefaultQueue, ALWAYS, and ORK_ORKER_QUEUE moves task
// EXECUTION only. Naming the split lets a test hold the runbook to it.
type queues struct {
	// run carries RunWorkflow and the idempotency claims: the engine's own
	// workflows, hosted by every replica of a release. Fixed, because a run
	// belongs to the engine, not to whichever Orker pool executes its tasks.
	run string
	// events carries PublishEvent, engine-internal so no Orker receives it.
	events string
	// orker is where task activities are dispatched — the one an operator
	// moves with ORK_ORKER_QUEUE to reach a dedicated Orker pool.
	orker string
	// binding is a refused ORK_ORKER_POOL/ORK_ORKER_QUEUE pair, carried here
	// so one call answers "may this process boot on these channels".
	binding error
}

func queuesFor(cfg config) queues {
	return queues{
		run:     task.DefaultQueue,
		events:  engine.EventQueue,
		orker:   cfg.orkerQueue,
		binding: cfg.poolErr,
	}
}

// validate refuses a configuration whose task execution channel is a queue the
// engine keeps for itself, or whose two variables disagree about which channel
// that is.
//
// Only the event queue qualifies as reserved. ORK_ORKER_QUEUE=ork-tasks is the
// ordinary default, where one queue carries the engine's workflows and its
// tasks; but ORK_ORKER_QUEUE=ork-engine-events points every task activity at
// the queue that carries PublishEvent, so tasks would be dispatched to the
// projection worker and `pool: main` would name the engine's own internals.
// Refusing at boot is what makes task.QueueForPool's promise — no pool reaches
// the event queue — true of a running deployment and not just of a named pool:
// the function is handed the channel, it does not choose it.
//
// task.ResolvePoolBinding refuses the same channel one layer earlier, which is
// what catches it when ORK_ORKER_POOL is how the operator spelled it. Both are
// kept: the resolution refuses what it can name, and this refuses the channel
// whatever produced it.
func (q queues) validate() error {
	if q.binding != nil {
		return q.binding
	}
	if q.orker == q.events {
		return fmt.Errorf("ORK_ORKER_QUEUE=%q is the engine's own event queue, "+
			"which carries %s; point it at a queue Orkers poll for task execution",
			q.orker, engine.ActivityPublishEvent)
	}
	return nil
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
	stored, err := store.List(ctx, artifacts.DefaultNamespace)
	if err != nil {
		return nil, fmt.Errorf("reload orkflows from %s (endpoint %s): %w",
			store.Location(), cfg.S3Endpoint, err)
	}
	// The boot log names the default namespace's catalog. It is a reachability
	// check and a human breadcrumb, not an inventory: listing every namespace
	// at boot would scan the whole prefix to log something nobody reads.
	logger.Info("reloaded orkflows from durable store",
		"store", store.Location(),
		"endpoint", cfg.S3Endpoint,
		"namespace", artifacts.DefaultNamespace,
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

// openNamespaceStore connects the engine to the registry that holds namespace
// documents: pool defaults and allow-lists.
//
// It is opened at boot for the same reason the orkflow store is — an engine
// that cannot read a namespace cannot decide where a task runs, and answering
// from a built-in default would silently ignore an operator's allow-list. The
// default namespace resolves whether or not a document was ever written, so a
// fresh deployment needs no bootstrap step.
func openNamespaceStore(ctx context.Context, logger *slog.Logger) (*artifacts.S3NamespaceStore, error) {
	cfg := artifacts.ConfigFromEnv()
	store, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open namespace registry: %w", err)
	}
	registered, err := store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("read namespaces from %s (endpoint %s): %w",
			store.Location(), cfg.S3Endpoint, err)
	}
	names := make([]string, 0, len(registered))
	for _, ns := range registered {
		names = append(names, ns.GetName())
	}
	logger.Info("namespace registry ready",
		"store", store.Location(), "namespaces", len(registered), "names", names)
	return store, nil
}

func run(logger *slog.Logger, cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Fail on a deployment identity the cluster would reject later, while the
	// error can still name the variable that caused it.
	if err := cfg.deployment.Validate(); err != nil {
		return err
	}
	// Fail on a pool and a raw execution channel that disagree, for the same
	// reason: an engine dispatching to one channel while its Orkers poll
	// another strands every run in RUNNING with nothing to say why
	// (AGENTS.md §6).
	if cfg.poolErr != nil {
		return cfg.poolErr
	}

	namespaces, err := openNamespaceStore(ctx, logger)
	if err != nil {
		return err
	}

	// Same reason: an execution channel that is one of the engine's own queues
	// is refused here, where the error can name the variable, rather than at
	// the first task dispatch.
	if err := queuesFor(cfg).validate(); err != nil {
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
	// The run index, provisioned beside the namespace: it is what makes "list
	// this namespace's runs" a question the cluster answers, instead of a
	// filter over whatever page walk another tenant's traffic left room for.
	// A cluster that will not register it is not a reason to refuse to boot —
	// everything else works, and the reader falls back to reading the whole
	// index — but it IS worth a line, because that fallback is slow and its
	// limit is an error a client would otherwise have to explain.
	namespaceIndexed, err := task.EnsureRunSearchAttributes(ctx, temporalClient, cfg.temporalNamespace)
	if err != nil {
		logger.Warn("visibility does not index a run's orkflow namespace",
			"search_attribute", task.SearchAttrOrkflowNamespace,
			"consequence", "listing a namespace's live runs scans the whole index",
			"error", err)
	}
	logger.Info("connected to temporal",
		"address", cfg.temporalAddress, "namespace", cfg.temporalNamespace,
		"run_namespace_index", namespaceIndexed)

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

	// Task logs: the chunks an Orker wrote while a task ran. They live in the
	// same bucket, so this engine serves the logs of a run it never started
	// and of one whose Orker has long exited — the same reason the archive is
	// not in this process either.
	logStore, err := artifacts.NewLogStore(archiveConfig)
	if err != nil {
		return err
	}
	logger.Info("task log store ready",
		"backend", archiveConfig.Backend, "location", logLocation(archiveConfig))

	// The read model. Every run read leaves this process: a run's history is
	// its event log, and Temporal hands the same answer to every replica, for
	// runs this one neither started nor projected.
	reader := engine.NewTemporalRunReader(temporalClient).WithNamespaceIndex(namespaceIndexed)

	// Workflow worker: hosts RunWorkflow and the idempotency claims on the
	// engine's own queue — NOT ORK_ORKER_QUEUE, which moves task execution
	// only. Under versioning the two show up separately in
	// `temporal worker deployment describe-version`: this queue as the
	// version's workflow poller, the Orker's as its activity poller.
	q := queuesFor(cfg)
	runWorker := worker.New(temporalClient, q.run, runWorkerOptions(cfg))
	engine.RegisterWorkflows(runWorker)

	// Projection worker: engine-internal activities on a dedicated queue so
	// Orkers never receive them. The archive body's event log is read back
	// from Temporal, because with more than one replica polling this queue no
	// single process executes all of a run's events.
	eventWorker := worker.New(temporalClient, q.events, eventWorkerOptions(cfg))
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
		"task_queue", q.run, "event_queue", q.events,
		"orker_queue", q.orker, "pool", cfg.pool)
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
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, q.run, q.orker).
		WithNamespaceIndex(namespaceIndexed)

	// Pool routing: the cluster answers whether a pool has Orkers, so a submit
	// onto a pool nothing serves fails at submit rather than leaving a run in
	// RUNNING forever. The engine never guesses this — it asks.
	pools := engine.NewTemporalPoolProbe(temporalClient)
	if cfg.defaultRouteOverride != "" {
		logger.Info("default pool route overridden",
			"pool", cfg.pool, "orker_queue", cfg.defaultRouteOverride)
	}

	// Idempotency: the cluster decides which run a key names, so every replica
	// behind this address gets the same answer. Nothing about dedupe lives in
	// this process.
	idempotency := engine.NewTemporalIdempotency(temporalClient, q.run, cfg.idempotencyTTL)
	logger.Info("idempotency claims ready",
		"workflow", engine.ClaimWorkflowType, "ttl", cfg.idempotencyTTL)

	svc := api.NewService(api.Config{
		Orkflows:    orkflows,
		Namespaces:  namespaces,
		Reader:      reader,
		Runs:        runs,
		Bus:         bus,
		Starter:     starter,
		Lister:      starter,
		Pools:       pools,
		PoolPlugins: starter,
		RawQueue:    cfg.defaultRouteOverride,
		Archive:     archive,
		Logs:        logStore,
		Idempotency: idempotency,
		Webhooks:    webhookRegistry,
		Notifier:    runNotifier,
		Logger:      logger,
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
