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
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	cfgfile "github.com/orkstack/orkstack/internal/config"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
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
	// limitsErr is malformed contract-limit configuration, reported at boot
	// rather than silently replaced with a default.
	limitsErr error
	// poolErr is a refused ORK_ORKER_POOL/ORK_ORKER_QUEUE pair, reported at
	// boot rather than resolved by picking a winner.
	poolErr                       error
	idempotencyTTL                time.Duration
	contractLimits                engine.WorkspaceLimits
	transferBudgetSeconds         int32
	floorThroughputBytesPerSecond int64
	// deployment is this release's Worker Deployment identity. It carries no
	// build id unless an operator configures one, which leaves versioning off
	// and the workers unversioned.
	deployment task.Deployment
	cleanup    archivecleanup.CleanupConfig

	// activityCatalog is the deployment's §4.2 table, from
	// ORK_ACTIVITY_CATALOG. catalogErr is a malformed one, reported at boot
	// rather than at the first apply — a catalog this process cannot read is
	// a configuration error, and discovering it when an author applies makes
	// it look like their document is wrong.
	activityCatalog dsl.Catalog
	catalogErr      error

	// registryAuthFile is ORK_REGISTRY_AUTH_FILE (§4.5): a docker
	// config.json used ONLY for manifest GETs during apply-time resolution.
	// It grants no push and no execution.
	registryAuthFile string
}

func loadConfig() config {
	valuesMaxBytes, valuesErr := envInt64("ORK_VALUES_MAX_BYTES", engine.DefaultValuesMaxBytes)
	artifactMaxCount, artifactCountErr := envInt32("ORK_ARTIFACT_MAX_COUNT", engine.DefaultArtifactMaxCount)
	artifactMaxBytes, artifactBytesErr := envInt64("ORK_ARTIFACT_MAX_BYTES", engine.DefaultArtifactMaxBytes)
	logMaxBytes, logErr := envInt64("ORK_LOG_MAX_BYTES", engine.DefaultLogMaxBytes)
	commitMaxBytes, commitErr := envInt64("ORK_COMMIT_MAX_BYTES", engine.DefaultCommitMaxBytes)
	transferBudgetSeconds, transferErr := envInt32AllowZero("ORK_TRANSFER_BUDGET_SECONDS", 0)
	floorThroughput, throughputErr := envInt64AllowZero("ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC", api.DefaultFloorThroughputBytesPerSecond)

	cfg := config{
		temporalAddress:   env("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
		temporalNamespace: env("ORK_TEMPORAL_NAMESPACE", task.DefaultNamespace),
		httpAddr:          env("ORK_HTTP_ADDR", api.DefaultHTTPAddr),
		idempotencyTTL:    envDuration("ORK_IDEMPOTENCY_TTL", engine.DefaultClaimTTL),
		contractLimits: engine.WorkspaceLimits{
			ValuesMaxBytes:   valuesMaxBytes,
			ArtifactMaxCount: artifactMaxCount,
			ArtifactMaxBytes: artifactMaxBytes,
			LogMaxBytes:      logMaxBytes,
			CommitMaxBytes:   commitMaxBytes,
		},
		transferBudgetSeconds:         transferBudgetSeconds,
		floorThroughputBytesPerSecond: floorThroughput,
		limitsErr: errors.Join(valuesErr, artifactCountErr, artifactBytesErr,
			logErr, commitErr, transferErr, throughputErr),
		deployment:       task.DeploymentFromEnv(),
		registryAuthFile: env("ORK_REGISTRY_AUTH_FILE", ""),
		cleanup:          archivecleanup.CleanupConfigFromEnv(),
	}
	cfg.cleanup.TemporalNamespace = cfg.temporalNamespace
	cfg.activityCatalog, cfg.catalogErr = dsl.ParseCatalog([]byte(env("ORK_ACTIVITY_CATALOG", "")))
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

func envInt64(key string, fallback int64) (int64, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback, fmt.Errorf("%s=%q must be a positive base-10 integer: %w", key, raw, err)
	}
	if parsed <= 0 {
		return fallback, fmt.Errorf("%s=%q must be a positive base-10 integer", key, raw)
	}
	return parsed, nil
}

func envInt32(key string, fallback int32) (int32, error) {
	parsed, err := envInt64(key, int64(fallback))
	if err != nil {
		return fallback, err
	}
	if parsed > math.MaxInt32 {
		raw := env(key, "")
		return fallback, fmt.Errorf("%s=%q exceeds the maximum value %d", key, raw, int64(math.MaxInt32))
	}
	return int32(parsed), nil
}

// envInt64AllowZero reads limits whose zero value means "derive". Negative
// and malformed values remain configuration errors; only the explicit zero
// spelling differs from envInt64.
func envInt64AllowZero(key string, fallback int64) (int64, error) {
	raw := env(key, "")
	if raw == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback, fmt.Errorf("%s=%q must be a non-negative base-10 integer: %w", key, raw, err)
	}
	if parsed < 0 {
		return fallback, fmt.Errorf("%s=%q must be a non-negative base-10 integer", key, raw)
	}
	return parsed, nil
}

func envInt32AllowZero(key string, fallback int32) (int32, error) {
	parsed, err := envInt64AllowZero(key, int64(fallback))
	if err != nil {
		return fallback, err
	}
	if parsed > math.MaxInt32 {
		raw := env(key, "")
		return fallback, fmt.Errorf("%s=%q exceeds the maximum value %d", key, raw, int64(math.MaxInt32))
	}
	return int32(parsed), nil
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

// artifactLocation renders the root of the artifact backend — the bucket or
// the directory, and never a credential.
//
// It is what the admin surface reports (api.SystemInfo.ArtifactLocation), so
// it names the store itself rather than one of its prefixes: archiveLocation
// and logLocation above answer "where does THIS kind of object land", which
// is a different question and one an operator asks of a log line, not of a
// deployment summary.
func artifactLocation(cfg artifacts.Config) string {
	if cfg.Backend == artifacts.BackendS3 {
		return "s3://" + cfg.S3Bucket
	}
	return cfg.LocalDir
}

// version is the ork release. Release builds override it with
// -ldflags "-X main.version=vX.Y.Z" (see the Makefile's build target); a
// plain `go build ./cmd/ork` reports "dev". Same pattern, and the same
// variable name, as cmd/orkctl/version.go.
var version = "dev"

// engineVersion resolves what this binary calls itself, most deliberate
// first: the ldflags stamp, then the version the module was built as (set for
// `go install module@version`), then the source revision a VCS-stamped build
// carries, and finally "dev".
//
// The fallbacks matter because the admin surface reports this to an operator
// judging a rollout: "dev" for a binary that WAS built from a known commit
// would make two different builds look like the same one.
func engineVersion() string {
	if version != "" && version != "dev" {
		return version
	}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	if main := build.Main.Version; main != "" && main != "(devel)" {
		return main
	}
	for _, setting := range build.Settings {
		if setting.Key == "vcs.revision" && setting.Value != "" {
			return setting.Value
		}
	}
	return version
}

// systemInfo is the admin surface's view of this process: what it was built
// as, and what it was configured with.
//
// It is assembled ONCE, here, from configuration that is already loaded — the
// API handler then reads no environment of its own and is a pure function of
// this struct (internal/api/system.go). Nothing in it is observed from the
// cluster: an operator reading `deployment` or `artifacts` is reading what
// this replica was told, which is exactly what they need when two replicas
// disagree.
func systemInfo(cfg config, artifactCfg artifacts.Config) api.SystemInfo {
	return api.SystemInfo{
		EngineVersion:    engineVersion(),
		GoVersion:        runtime.Version(),
		DeploymentName:   cfg.deployment.Name,
		BuildID:          cfg.deployment.BuildID,
		ArtifactBackend:  artifactCfg.Backend,
		ArtifactLocation: artifactLocation(artifactCfg),
		TemporalAddress:  cfg.temporalAddress,
	}
}

// engine.TemporalRunReader satisfies both cluster seams archive's retention
// machinery needs, over the same Temporal client every other read on it
// already uses — never a second, parallel connection.
var (
	_ archivecleanup.RetentionAdmin = (*engine.TemporalRunReader)(nil)
	_ archivecleanup.RunRetirer     = (*engine.TemporalRunReader)(nil)
)

// applyConfigFile is main()'s entire configuration-file mechanism
// (docs/DEPLOY.md §1), factored out so a test can drive it against fake
// lookup/setenv maps instead of the real process environment —
// cfgfile.Document.Apply calls setenv, and this must not leak into other
// tests' environment. It parses args the same way cfgfile.ParseArgs does,
// loads the file for binary when one was named, and — unless --check was
// given — applies it via setenv before returning, so loadConfig(), called
// right after, sees exactly what the file set with the environment's own
// values always winning (cfgfile.Document.Apply's whole precedence rule).
// --check itself never calls Apply: it reports origins against the
// environment as main found it, matching cfgfile.Document.Effective's own
// tests (internal/config/effective_test.go), never against an environment
// this call had already mutated.
//
// The two error shapes main must tell apart are distinguished by the
// returned doc and path: doc == nil && path == "" is a usage error (a bad
// flag, or --check with no file named) — exit 2, print err.Error() verbatim.
// Anything else is a bad file or a setenv failure — exit 1, prefixed
// "ork: " (the loader already prefixes "config:").
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
	// this func does below: print config.RenderDefaults() and stop, before
	// applyConfigFile's own cfgfile.ParseArgs call (which also sees this
	// flag but, per its own doc, otherwise ignores it) does anything that
	// could fail on a bad --config path irrelevant to this request.
	if _, _, showDefaults, err := cfgfile.ParseArgs(os.Args[1:], os.LookupEnv); err == nil && showDefaults {
		fmt.Print(cfgfile.RenderDefaults())
		os.Exit(0)
	}

	// The configuration file, if any, is resolved before loadConfig reads a
	// single environment variable — see applyConfigFile — so every existing
	// *FromEnv reader below (loadConfig included) works unchanged whether its
	// value came from a file or, as always, straight from the environment.
	cfgDoc, cfgPath, cfgCheck, cfgSet, err := applyConfigFile(
		os.Args[1:], cfgfile.BinaryOrk, os.LookupEnv, os.Setenv)
	if err != nil {
		if cfgDoc == nil && cfgPath == "" {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "ork: %v\n", err)
		os.Exit(1)
	}
	if cfgCheck {
		fmt.Print(cfgDoc.Effective(cfgfile.BinaryOrk, os.LookupEnv))
		os.Exit(0)
	}

	cfg := loadConfig()
	// Telemetry configuration is resolved BEFORE the process logger exists,
	// even though every other config error is reported through that logger:
	// the logger's own encoding (text vs json, its level, whether OTLP logs
	// are bridged in) is itself a telemetry setting. Building the logger
	// first would mean rebuilding it the moment telemetry config turned out
	// to say otherwise, and a malformed OTEL_* variable would then be
	// reported through a logger that config never actually asked for.
	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceEngine, telemetry.ResourceAttrs(
		engineVersion(), cfg.deployment.Name, cfg.deployment.BuildID, cfg.temporalNamespace)...)
	if err != nil {
		fallback := slog.New(slog.NewTextHandler(os.Stderr, nil))
		fallback.Error("ork stopped", "error", err)
		os.Exit(1)
	}
	logger := telemetry.NewLogger(tcfg, sinks.Default(), os.Stderr)
	// Attached as soon as the logger exists, so a namespace naming a sink
	// ORK_TELEMETRY_SINKS never declared logs a Warn from the very first
	// call, rather than only from whichever caller happens to attach one.
	sinks.WithLogger(logger)
	// Logged only when a file was actually given, so a zero-config boot's
	// log is unchanged from before this package existed.
	if cfgPath != "" {
		logger.Info("configuration file applied", "path", cfgPath, "applied", len(cfgSet))
	}
	if err := run(logger, tcfg, sinks, cfg); err != nil {
		logger.Error("ork stopped", "error", err)
		os.Exit(1)
	}
}

// queues names every Temporal queue this engine touches. It exists because
// which queue carries what is an operational claim (docs/DEPLOY.md §2), and a
// claim that lives only in a comment and a log line can be wrong unnoticed:
// the run worker polls task.DefaultQueue, ALWAYS, and ORK_ORKER_QUEUE moves
// task EXECUTION only. Naming the split lets a test hold the runbook to it.
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
// what keeps a run readable from every replica. With no versioned
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

// bindAndServe binds server.Addr synchronously — so a port already in use,
// or any other bind failure, comes back as an error naming the address
// before the caller can act on it — then serves on a background goroutine
// exactly as server.ListenAndServe would, reporting its outcome onto
// serverErr the same way ListenAndServe's error would have. It exists so a
// caller can safely mark the process ready immediately after it returns: the
// listener already holds the port by then, which a bare `go
// server.ListenAndServe()` never guaranteed.
func bindAndServe(server *http.Server, logger *slog.Logger, serverErr chan<- error) (net.Listener, error) {
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", server.Addr, err)
	}
	go func() {
		logger.Info("serving connect api (h2c), websocket bridge and webhooks",
			"addr", server.Addr, "webhooks", api.WebhookCollectionPath)
		if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()
	return ln, nil
}

// openOrkflowStore connects the engine to the bucket that holds orkflow
// definitions and reloads the catalog from it.
//
// The reload is the restart path: the store keeps no cache, so listing it here
// both proves the bucket is reachable before the API accepts an apply and logs
// exactly which orkflows this engine came up with. An unreachable bucket is
// fatal — an engine that cannot read orkflows cannot run them, and there is no
// in-memory copy to answer from.
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

// wireEventObserver decides whether the engine's telemetry observer is wired
// into the projection worker and the API. It is gated on SignalsEnabled, not
// Enabled: ORK_ADMIN_ADDR alone must not change a single thing about
// lifecycle logging, only whether /healthz, /readyz and /metrics answer.
func wireEventObserver(tcfg telemetry.Config) bool {
	return tcfg.SignalsEnabled()
}

// warnUndeclaredNamespaceSinks cross-checks every registered namespace's
// telemetry policy against the sinks this deployment actually declared: a
// namespace naming a sink ORK_TELEMETRY_SINKS does not carry is not a boot
// failure — telemetry.Sinks.For already falls back to the default sink at
// run time — but it is a misconfiguration an operator should learn about
// before a run silently exports to the wrong destination, so one Warn is
// logged per such namespace.
//
// A store error here is itself only a Warn: this check is a best-effort
// cross-reference, and telemetry must never fail the deployment (§11).
func warnUndeclaredNamespaceSinks(
	ctx context.Context, logger *slog.Logger, namespaces artifacts.NamespaceStore, sinks *telemetry.Sinks,
) {
	registered, err := namespaces.List(ctx)
	if err != nil {
		logger.Warn("cross-check namespace telemetry sinks: list namespaces failed", "error", err)
		return
	}
	known := sinks.Names()
	for _, ns := range registered {
		sink := ns.GetTelemetry().GetSink()
		if sink == "" || sinks.Has(sink) {
			continue
		}
		logger.Warn("namespace names a telemetry sink this deployment does not declare; its runs export to the default sink",
			"namespace", ns.GetName(), "sink", sink, "known_sinks", known)
	}
}

// wsPathPrefix is the prefix common to every WebSocket upgrade route NewMux
// registers (internal/api/server.go): "/ws/runs/{id}" and
// api.TaskLogSocketPath ("/ws/runs/{id}/tasks/{task}/logs") both start with
// it. internal/api exports no constant naming just the prefix, so this
// literal has to stay in lockstep with NewMux's own routes by hand.
const wsPathPrefix = "/ws/"

// traceAPIRequest is apiHandler's otelhttp filter: it reports whether a
// request should carry a span and an http.server.request.duration sample. A
// WebSocket upgrade's ServeHTTP blocks for the life of the connection, so
// tracing it would produce a span — and a duration sample — as long as the
// viewer keeps the run's log stream open, polluting API latency metrics and
// traces with numbers that describe a browsing session, not a request. Every
// other route is an ordinary request/response and is traced.
func traceAPIRequest(r *http.Request) bool {
	return !strings.HasPrefix(r.URL.Path, wsPathPrefix)
}

// apiHandler wraps mux with OpenTelemetry HTTP instrumentation when traces or
// metrics are on, so a request into the Connect API and WebSocket bridge
// carries a span and RED metrics on the same default-sink providers every
// other process-level signal uses. With both off it returns mux itself,
// unchanged — the zero-config path serves the exact *http.ServeMux this
// binary always served, not a handler wrapping one. traceAPIRequest keeps the
// two long-lived WebSocket routes out of tracing and metrics regardless: see
// its doc comment.
func apiHandler(tcfg telemetry.Config, sinks *telemetry.Sinks, mux *http.ServeMux) http.Handler {
	if !tcfg.TracesEnabled() && !tcfg.MetricsEnabled() {
		return mux
	}
	providers := sinks.Default()
	return otelhttp.NewHandler(mux, "orkstack.api",
		otelhttp.WithTracerProvider(providers.Tracer),
		otelhttp.WithMeterProvider(providers.Meter),
		otelhttp.WithPropagators(providers.Propagator),
		otelhttp.WithFilter(traceAPIRequest))
}

func run(logger *slog.Logger, tcfg telemetry.Config, sinks *telemetry.Sinks, cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The admin listener opens before the Temporal dial, so /healthz answers
	// during boot rather than only once the workers and the API are up —
	// exactly when a slow S3 or Temporal endpoint most needs a liveness probe
	// that already responds. It is wired to the default sink only: /healthz,
	// /readyz and /metrics describe this process, never a namespace's own
	// destination.
	adminAddr, stopAdmin, err := telemetry.StartAdmin(tcfg, sinks.Default(), logger)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := stopAdmin(shutdownCtx); err != nil {
			logger.Warn("admin listener shutdown", "error", err)
		}
	}()
	// Registered here, before the worker Stop defers below, so it runs AFTER
	// them: defers are LIFO, and every sink must keep flushing while the
	// workers still hold in-flight spans and metrics, not before.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := sinks.Shutdown(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown", "error", err)
		}
	}()
	telemetry.LogBoot(logger, tcfg, sinks, adminAddr)

	// Refuse malformed contract-limit configuration before opening any
	// infrastructure. Silently substituting a default would make an operator
	// believe a ceiling is active when the process discarded it.
	if cfg.limitsErr != nil {
		return cfg.limitsErr
	}
	// Fail on a deployment identity the cluster would reject later, while the
	// error can still name the variable that caused it.
	if err := cfg.deployment.Validate(); err != nil {
		return err
	}
	// Fail on a pool and a raw execution channel that disagree, for the same
	// reason: an engine dispatching to one channel while its Orkers poll
	// another strands every run in RUNNING with nothing to say why
	// (AGENTS.md §11.1).
	if cfg.poolErr != nil {
		return cfg.poolErr
	}
	// A malformed ORK_ACTIVITY_CATALOG is a boot failure for the same reason:
	// an engine that cannot read its catalog would refuse every `uses: shell`
	// as an unknown activity, which reads as the author's mistake rather than
	// the operator's.
	if cfg.catalogErr != nil {
		return cfg.catalogErr
	}

	namespaces, err := openNamespaceStore(ctx, logger)
	if err != nil {
		return err
	}
	// Cheap, so it always runs rather than only when a sink is configured:
	// a namespace's stored policy can name a sink this deployment dropped
	// from ORK_TELEMETRY_SINKS since it was set, and that should surface
	// even on a deployment that otherwise declares none at all.
	warnUndeclaredNamespaceSinks(ctx, logger, namespaces, sinks)

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
	// Run snapshots are a platform record, filed under metadata/snapshots
	// beside commits, logs and run bodies — never in the workload artifact
	// namespace — so run-scoped cleanup reclaims them by prefix.
	snapshots, err := artifacts.NewRunSnapshotStore(archiveConfig)
	if err != nil {
		return fmt.Errorf("open snapshot store: %w", err)
	}

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
	reader := engine.NewTemporalRunReader(temporalClient).
		WithSnapshotStore(snapshots).
		WithNamespaceIndex(namespaceIndexed)

	// The retention policy store and controller: the deployment's maximum run
	// retention (never a user-facing Temporal setting, AGENTS.md §7) and the
	// invariant that keeps the cluster's own retention TTL pinned to that
	// maximum plus an enforcement margin, always at least as generous as any
	// namespace's own run_retention. Built over the same reader as every
	// other cluster read — never a second connection.
	retentionPolicies, err := artifacts.NewS3RetentionPolicyStore(ctx, artifacts.ConfigFromEnv())
	if err != nil {
		return fmt.Errorf("open retention policy store: %w", err)
	}
	retention := archivecleanup.NewRetentionController(
		retentionPolicies, reader, cfg.temporalNamespace, cfg.cleanup.RetentionBuffer, logger)
	policy, err := retention.Ensure(ctx)
	if err != nil {
		// Fail closed: an engine that cannot establish the ceiling must not
		// start deleting run history under one it never confirmed.
		return fmt.Errorf("establish retention policy: %w", err)
	}
	logger.Info("retention policy ready",
		"max_run_retention", policy.GetMaxRunRetention().AsDuration(),
		"namespace_retention_enforced", cfg.cleanup.Enabled)

	if cfg.cleanup.Enabled {
		cleaner := archivecleanup.NewCleaner(cfg.cleanup, archiveStore, reader, namespaces, retention, logger)
		go cleaner.Run(ctx)
		logger.Info("artifact cleanup worker started",
			"interval", cfg.cleanup.Interval,
			"retention_buffer", cfg.cleanup.RetentionBuffer,
			"batch_size", cfg.cleanup.BatchSize)
	}

	// Workflow worker: hosts RunWorkflow and the idempotency claims on the
	// engine's own queue — NOT ORK_ORKER_QUEUE, which moves task execution
	// only. Under versioning the two show up separately in
	// `temporal worker deployment describe-version`: this queue as the
	// version's workflow poller, the Orker's as its activity poller.
	q := queuesFor(cfg)
	runWorker := worker.New(temporalClient, q.run, runWorkerOptions(cfg))
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	// The engine's telemetry observer: a run's spans, metrics and one log
	// line per lifecycle event, routed per-run to whatever sink
	// run.GetTelemetry() names. Built unconditionally — SetupSinks' own
	// zero-config Sinks builds it against noop instruments at no real cost —
	// but wired into the projection worker and the API only when telemetry
	// is actually on, so the byte-identical zero-config activity wiring this
	// binary always had is untouched.
	observer, err := telemetry.NewEngineObserverForSinks(sinks, logger)
	if err != nil {
		return err
	}
	var eventObserver engine.EventObserver
	if wireEventObserver(tcfg) {
		eventObserver = observer
	}

	// Projection worker: engine-internal activities on a dedicated queue so
	// Orkers never receive them. The archive body's event log is read back
	// from Temporal, because with more than one replica polling this queue no
	// single process executes all of a run's events.
	eventActivities := engine.NewArchivingEventActivities(bus, runs, archive).
		WithRunNotifier(runNotifier).
		WithEventSource(reader).
		WithSnapshotStore(snapshots)
	if eventObserver != nil {
		eventActivities = eventActivities.WithEventObserver(eventObserver)
	}
	eventWorker := worker.New(temporalClient, q.events, eventWorkerOptions(cfg))
	eventWorker.RegisterActivityWithOptions(
		eventActivities.PublishEvent,
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
	// queue's current version and land on a different build's activity code.
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

	// The admin surface's view of this replica: assembled from configuration
	// that is already loaded, so the handler reads no environment of its own.
	system := systemInfo(cfg, archiveConfig)
	logger.Info("system information ready",
		"engine_version", system.EngineVersion,
		"go", system.GoVersion,
		"deployment", system.DeploymentName,
		"build_id", system.BuildID,
		"versioning_active", system.VersioningActive(),
		"artifact_backend", system.ArtifactBackend,
		"artifact_location", system.ArtifactLocation)

	// The two halves of §4.1, built once: the catalog turns a short name into
	// a reference, and the resolver turns a reference into a digest. The
	// lister shares the resolver so `orkctl activity list` reports the same
	// digests an apply would pin.
	resolver := dsl.NewResolver(cfg.registryAuthFile)
	activities := &api.CatalogLister{Catalog: cfg.activityCatalog, Resolver: resolver}

	svc := api.NewService(api.Config{
		Orkflows:    orkflows,
		Namespaces:  namespaces,
		Reader:      reader,
		Runs:        runs,
		Bus:         bus,
		Starter:     starter,
		Pools:       pools,
		PoolOrkers:  starter,
		Orkers:      starter,
		Catalog:     cfg.activityCatalog,
		Resolver:    resolver,
		Activities:  activities,
		RawQueue:    cfg.defaultRouteOverride,
		Archive:     archive,
		Logs:        logStore,
		Idempotency: idempotency,
		Webhooks:    webhookRegistry,
		Notifier:    runNotifier,
		Logger:      logger,
		System:      system,
		// Retention backs SystemService's
		// GetRetentionPolicy/UpdateRetentionPolicy, and also validates a
		// namespace's own run_retention against the ceiling
		// (NamespaceService). RetentionEnforced mirrors whether the cleanup
		// worker above was actually started: without it, a namespace's own
		// value has no effect yet, and GetRetentionPolicy says so.
		Retention:                     retention,
		RetentionEnforced:             cfg.cleanup.Enabled,
		Artifacts:                     archiveConfig,
		Limits:                        cfg.contractLimits,
		TransferBudgetSeconds:         cfg.transferBudgetSeconds,
		FloorThroughputBytesPerSecond: cfg.floorThroughputBytesPerSecond,
		Observer:                      eventObserver,
		TelemetrySinks:                tcfg.Sinks.Names(),
	})
	mux := api.NewMux(svc, api.MuxOptions{
		AllowedOrigins: cfg.allowedOrigins,
	})
	server := api.NewServer(cfg.httpAddr, apiHandler(tcfg, sinks, mux))
	cancelServerHandlers := installServerShutdownContext(server)
	defer cancelServerHandlers()

	serverErr := make(chan error, 1)
	if _, err := bindAndServe(server, logger, serverErr); err != nil {
		return err
	}
	// /readyz flips to 200 only once the API is actually serving — the point
	// past which a load balancer should send this replica traffic.
	// bindAndServe binds the port synchronously before returning, so this can
	// never race an EADDRINUSE (or any other bind failure) that hasn't
	// happened yet: by the time we get here, the listener already holds
	// cfg.httpAddr.
	sinks.Default().Ready.Set(true)

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		sinks.Default().Ready.Set(false)
		logger.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return <-serverErr
}
