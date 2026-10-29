// Package config implements the OrkStack configuration file: an optional
// YAML file that seeds the same environment variables every reader in this
// codebase (internal/telemetry.ConfigFromEnv, internal/dsl.ParseCatalog, and
// every ORK_*/OTEL_* constant scattered through internal/api and
// internal/orker) already reads via os.LookupEnv.
//
// The file changes nothing about how a setting is consumed — only how it
// gets into the environment, and only when the ambient environment has not
// already supplied it (see Document.Apply). A deployment that sets no file
// is byte-identical to one running this package's predecessor.
package config

import (
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Binary names one of the two OrkStack daemons a schema entry applies to.
// "both" in the specification and docs table is represented as a Var whose
// Binaries lists both constants, rather than a third value, so a caller can
// always range over Binaries without a special case.
type Binary string

const (
	BinaryOrk   Binary = "ork"
	BinaryOrker Binary = "orker"
)

// Kind is the shape a schema leaf's YAML value must have, and therefore how
// Load renders it into the string an environment variable carries.
type Kind int

const (
	// KindString is a plain scalar, carried through verbatim.
	KindString Kind = iota
	// KindInt is a scalar that must parse as a base-10 integer.
	KindInt
	// KindBool is a scalar that must parse as true/false.
	KindBool
	// KindDuration is a scalar that must parse with time.ParseDuration.
	KindDuration
	// KindList is a YAML sequence, rendered comma-separated.
	KindList
	// KindMap is a YAML mapping, rendered "k=v,k2=v2" with each value
	// percent-encoded per the OTel baggage rules (',', '=', '%').
	KindMap
	// KindSecret is a KindString leaf that may additionally be written as
	// {file: /path}. Every KindSecret is also eligible for {file:}; some
	// KindString leaves are too (see Var.Secret), but a KindSecret leaf is
	// ALWAYS redacted by Effective regardless of how its value arrived.
	KindSecret
	// KindDocument is one of the two YAML subtrees (activities,
	// telemetry.sinks) that round-trip as YAML text rather than a scalar.
	KindDocument
)

// Var describes one mapped setting: the dotted key path a document uses to
// name it, the environment variable it feeds, which binaries read that
// variable, and the shape (Kind) its YAML value must have.
type Var struct {
	// Path is the dotted key, e.g. "storage.s3.secret_key" or "activities".
	Path string
	// Env is the environment variable this key feeds, e.g. "ORK_S3_SECRET_KEY".
	Env string
	// Binaries lists which daemon(s) read Env. A leaf used by both carries
	// both constants; ParseArgs and the docs table both call that "both".
	Binaries []Binary
	// Kind is the leaf's YAML shape.
	Kind Kind
	// Secret marks a value Effective must redact and a leaf that may be
	// written as {file: /path} even though its Kind is KindString or
	// KindMap (KindSecret is always eligible; Secret extends the same
	// {file:} mechanism to a handful of KindString/KindMap leaves such as
	// telemetry.otlp.headers).
	Secret bool
	// Default is the value docs/DEPLOY.md documents for Env: either a real
	// literal (bound to the same code constant the binary itself falls
	// back to — see defaults.go), or one of exactly two standardized
	// placeholders, "(empty)" or "(derived)", for a leaf whose default is
	// genuinely not a fixed literal (see DerivedNote). Effective reports it
	// as the value's origin when neither the file nor the environment set
	// the variable.
	Default string
	// DerivedNote is the human sentence explaining a placeholder Default —
	// "derived from the pool", "hostname-<hash of the work dir>", "SDK
	// default" — and is only meaningful when Default is "(empty)" or
	// "(derived)". Empty otherwise.
	DerivedNote string
}

// usesBoth is Var.Binaries for a leaf both binaries read.
var usesBoth = []Binary{BinaryOrk, BinaryOrker}
var usesOrk = []Binary{BinaryOrk}
var usesOrker = []Binary{BinaryOrker}

// Schema is the complete, ordered table this package understands: every key
// path the configuration file accepts, in the order docs/DEPLOY.md's
// "## 1. Configuration" table and the specification list them. It is
// exported so a test or a doc generator can enumerate it rather than this
// package's internals.
var Schema = []Var{
	// temporal.address is bound to the Temporal Go SDK's own
	// client.DefaultHostPort — the same constant cmd/ork and cmd/orker
	// themselves fall back to.
	{Path: "temporal.address", Env: "ORK_TEMPORAL_ADDRESS", Binaries: usesBoth, Kind: KindString, Default: client.DefaultHostPort},
	{Path: "temporal.namespace", Env: "ORK_TEMPORAL_NAMESPACE", Binaries: usesBoth, Kind: KindString, Default: task.DefaultNamespace},

	{Path: "deployment.name", Env: "ORK_DEPLOYMENT_NAME", Binaries: usesBoth, Kind: KindString, Default: task.DefaultDeploymentName},
	{Path: "deployment.build_id", Env: "ORK_BUILD_ID", Binaries: usesBoth, Kind: KindString, Default: EmptyDefault, DerivedNote: "empty means versioning is off"},

	{Path: "pool.name", Env: "ORK_ORKER_POOL", Binaries: usesBoth, Kind: KindString, Default: task.DefaultPool},
	{Path: "pool.queue", Env: "ORK_ORKER_QUEUE", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "derived from the pool"},

	{Path: "storage.backend", Env: "ORK_ARTIFACT_BACKEND", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultBackend},
	{Path: "storage.dir", Env: "ORK_ARTIFACT_DIR", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultLocalDir},
	{Path: "storage.s3.endpoint", Env: "ORK_S3_ENDPOINT", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultS3Endpoint},
	{Path: "storage.s3.region", Env: "ORK_S3_REGION", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultS3Region},
	{Path: "storage.s3.bucket", Env: "ORK_S3_BUCKET", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultS3Bucket},
	{Path: "storage.s3.access_key", Env: "ORK_S3_ACCESS_KEY", Binaries: usesBoth, Kind: KindSecret, Secret: true, Default: artifacts.DefaultS3AccessKey},
	{Path: "storage.s3.secret_key", Env: "ORK_S3_SECRET_KEY", Binaries: usesBoth, Kind: KindSecret, Secret: true, Default: artifacts.DefaultS3SecretKey},
	{Path: "storage.s3.session_token", Env: "ORK_S3_SESSION_TOKEN", Binaries: usesBoth, Kind: KindSecret, Secret: true, Default: EmptyDefault, DerivedNote: "for temporary credentials"},
	{Path: "storage.s3.path_style", Env: "ORK_S3_PATH_STYLE", Binaries: usesBoth, Kind: KindBool, Default: formatBool(artifacts.DefaultS3PathStyle)},

	{Path: "engine.http_addr", Env: "ORK_HTTP_ADDR", Binaries: usesOrk, Kind: KindString, Default: api.DefaultHTTPAddr},
	{Path: "engine.ws_allowed_origins", Env: "ORK_WS_ALLOWED_ORIGINS", Binaries: usesOrk, Kind: KindList, Default: EmptyDefault},
	// ORK_IDEMPOTENCY_TTL's fallback is engine.DefaultClaimTTL, the same
	// constant idempotency.go binds a non-positive ttl to.
	{Path: "engine.idempotency_ttl", Env: "ORK_IDEMPOTENCY_TTL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultClaimTTL)},
	{Path: "engine.registry_auth_file", Env: "ORK_REGISTRY_AUTH_FILE", Binaries: usesOrk, Kind: KindString, Default: EmptyDefault},
	{Path: "engine.stats_cache_ttl", Env: "ORK_STATS_CACHE_TTL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(api.DefaultStatsCacheTTL)},
	// ORK_EACH_MAX_ITEMS' fallback is engine.DefaultEachMaxItems: a
	// deployment default of 1000, which a deployment lowers, or raises up to
	// the built-in 10000 (dsl.MaxEachItems) with continuation on.
	{Path: "engine.each_max_items", Env: "ORK_EACH_MAX_ITEMS", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultEachMaxItems))},
	// ORK_ORKFLOW_MAX_TASKS' fallback is engine.DefaultOrkflowMaxTasks: the
	// most tasks one Orkflow may declare, the largest size the task soak
	// measured, a default a deployment may lower or raise (warned above it,
	// and above the flat-stage bound); with continuation off cmd/ork lowers
	// an unset key to history.max_events / 50 and refuses more.
	{Path: "engine.orkflow_max_tasks", Env: "ORK_ORKFLOW_MAX_TASKS", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultOrkflowMaxTasks))},
	// ORK_HISTORY_MAX_EVENTS and ORK_HISTORY_MAX_BYTES fall back to
	// engine.DefaultHistoryMaxEvents and DefaultHistoryMaxBytes: the Temporal
	// server's own per-execution limits, which a run stops RUN_HISTORY_LIMIT
	// before reaching.
	{Path: "engine.history.max_events", Env: "ORK_HISTORY_MAX_EVENTS", Binaries: usesOrk, Kind: KindInt, Default: formatInt(engine.DefaultHistoryMaxEvents)},
	{Path: "engine.history.max_bytes", Env: "ORK_HISTORY_MAX_BYTES", Binaries: usesOrk, Kind: KindInt, Default: formatBytes(engine.DefaultHistoryMaxBytes)},
	// ORK_CONTINUATION_* fall back to engine.DefaultContinuation*: whether a
	// run carries on into a fresh history past its budget (events or bytes),
	// and how long its next history waits for its saved state. Pinned into
	// each top-level plan at submit; validated only when enabled.
	{Path: "engine.continuation.enabled", Env: "ORK_CONTINUATION_ENABLED", Binaries: usesOrk, Kind: KindBool, Default: formatBool(engine.DefaultContinuationEnabled)},
	{Path: "engine.continuation.events", Env: "ORK_CONTINUATION_EVENTS", Binaries: usesOrk, Kind: KindInt, Default: formatInt(engine.DefaultContinuationEvents)},
	{Path: "engine.continuation.bytes", Env: "ORK_CONTINUATION_BYTES", Binaries: usesOrk, Kind: KindInt, Default: formatBytes(engine.DefaultContinuationBytes)},
	{Path: "engine.continuation.restore_timeout", Env: "ORK_CONTINUATION_RESTORE_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultContinuationRestoreTimeout)},
	// ORK_DEFINITION_BY_REFERENCE's fallback is
	// engine.DefaultDefinitionByReference — on, when unset, only where
	// continuation is on (cmd/ork's definitionByReferenceFromEnv).
	{Path: "engine.definition_by_reference", Env: "ORK_DEFINITION_BY_REFERENCE", Binaries: usesOrk, Kind: KindBool, Default: formatBool(engine.DefaultDefinitionByReference)},
	// ORK_SNAPSHOT_OVERLAYS falls back to engine.DefaultSnapshotOverlays:
	// whether a large run writes snapshot overlays for its non-terminal
	// events.
	{Path: "engine.snapshot_overlays", Env: "ORK_SNAPSHOT_OVERLAYS", Binaries: usesOrk, Kind: KindBool, Default: formatBool(engine.DefaultSnapshotOverlays)},
	// ORK_RUN_MAX_IN_FLIGHT's fallback is engine.DefaultRunMaxInFlight: the
	// most activities, child runs and pins one run keeps pending.
	{Path: "engine.run_max_in_flight", Env: "ORK_RUN_MAX_IN_FLIGHT", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultRunMaxInFlight))},
	// ORK_PIN_CREATION_TIMEOUT's fallback is engine.DefaultPinCreationTimeout:
	// how long a pin waits for a pin host's free slot before its first task
	// fails PIN_UNAVAILABLE. Pinned, in seconds, into each top-level plan.
	{Path: "engine.pin_creation_timeout", Env: "ORK_PIN_CREATION_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultPinCreationTimeout)},
	// ORK_COMPOSE_MAX_DEPTH's fallback is engine.DefaultComposeMaxDepth, the
	// built-in ceiling (engine.MaxComposeDepth) it may lower and never raise.
	{Path: "engine.compose_max_depth", Env: "ORK_COMPOSE_MAX_DEPTH", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultComposeMaxDepth))},
	// ORK_CACHE_MAX_TTL's fallback is engine.DefaultCacheMaxTTL: the oldest
	// memoized result a cached task (cache:) may reuse.
	{Path: "engine.cache_max_ttl", Env: "ORK_CACHE_MAX_TTL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultCacheMaxTTL)},
	{Path: "engine.usage_cache_ttl", Env: "ORK_USAGE_CACHE_TTL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(api.DefaultUsageCacheTTL)},

	{Path: "engine.limits.values_max_bytes", Env: "ORK_VALUES_MAX_BYTES", Binaries: usesOrk, Kind: KindInt, Default: formatBytes(engine.DefaultValuesMaxBytes)},
	{Path: "engine.limits.artifact_max_count", Env: "ORK_ARTIFACT_MAX_COUNT", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultArtifactMaxCount))},
	{Path: "engine.limits.artifact_max_bytes", Env: "ORK_ARTIFACT_MAX_BYTES", Binaries: usesOrk, Kind: KindInt, Default: formatBytes(engine.DefaultArtifactMaxBytes)},
	{Path: "engine.limits.log_max_bytes", Env: "ORK_LOG_MAX_BYTES", Binaries: usesOrk, Kind: KindInt, Default: formatBytes(engine.DefaultLogMaxBytes)},
	{Path: "engine.limits.commit_max_bytes", Env: "ORK_COMMIT_MAX_BYTES", Binaries: usesOrk, Kind: KindInt, Default: formatBytes(engine.DefaultCommitMaxBytes)},
	{Path: "engine.limits.transfer_budget_seconds", Env: "ORK_TRANSFER_BUDGET_SECONDS", Binaries: usesOrk, Kind: KindInt, Default: DerivedDefault, DerivedNote: "max(300, artifact_max_count × artifact_max_bytes ÷ floor throughput)"},
	{Path: "engine.limits.floor_throughput_bytes_per_sec", Env: "ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC", Binaries: usesOrk, Kind: KindInt, Default: formatBytes(api.DefaultFloorThroughputBytesPerSecond) + "/s"},

	{Path: "engine.cleanup.enabled", Env: "ORK_CLEANUP_ENABLED", Binaries: usesOrk, Kind: KindBool, Default: "false"},
	{Path: "engine.cleanup.interval", Env: "ORK_CLEANUP_INTERVAL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(archive.DefaultCleanupInterval)},
	{Path: "engine.cleanup.retention_buffer", Env: "ORK_CLEANUP_RETENTION_BUFFER", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(archive.DefaultRetentionBuffer)},
	{Path: "engine.cleanup.batch_size", Env: "ORK_CLEANUP_BATCH_SIZE", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(archive.DefaultBatchSize))},

	// activities has no single scalar default: it is the built-in catalog
	// baked into internal/dsl, not a value this package can format.
	{Path: "activities", Env: "ORK_ACTIVITY_CATALOG", Binaries: usesOrk, Kind: KindDocument, Default: "built-in shell/http/file/json/git/template → ghcr.io/orkstack/activity-*:0.1"},

	// orker.id and orker.workdir are genuinely dynamic (a hostname hash, and
	// os.TempDir()) — real prose, not one of the two standardized
	// placeholders, since docs/DEPLOY.md documents them the same way.
	{Path: "orker.id", Env: "ORK_ORKER_ID", Binaries: usesOrker, Kind: KindString, Default: "hostname-<hash of the work dir>"},
	{Path: "orker.workdir", Env: "ORK_ORKER_WORKDIR", Binaries: usesOrker, Kind: KindString, Default: "temp dir"},
	{Path: "orker.image_layout", Env: "ORK_ORKER_IMAGE_LAYOUT", Binaries: usesOrker, Kind: KindString, Default: orker.DefaultImageLayoutRoot},
	{Path: "orker.env_allowlist", Env: "ORK_ORKER_ENV_ALLOWLIST", Binaries: usesOrker, Kind: KindList, Default: EmptyDefault},
	{Path: "orker.secret_schemes", Env: "ORK_ORKER_SECRET_SCHEMES", Binaries: usesOrker, Kind: KindList, Default: formatList(orker.DefaultSecretSchemes)},
	{Path: "orker.secret_allow", Env: "ORK_ORKER_SECRET_ALLOW", Binaries: usesOrker, Kind: KindList, Default: EmptyDefault},
	{Path: "orker.registry_auth_file", Env: "ORK_ORKER_REGISTRY_AUTH_FILE", Binaries: usesOrker, Kind: KindString, Default: EmptyDefault},
	{Path: "orker.task_log_export", Env: "ORK_ORKER_TASK_LOG_EXPORT", Binaries: usesOrker, Kind: KindBool, Default: "false"},
	{Path: "orker.heartbeat_interval", Env: "ORK_ORKER_HEARTBEAT_INTERVAL", Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultHeartbeatInterval)},
	{Path: "orker.max_concurrent", Env: orker.MaxConcurrentVar, Binaries: usesOrker, Kind: KindInt, Default: formatInt(int64(orker.DefaultMaxConcurrent))},

	// orker.pins.max_concurrent is 0 (no default constant) rather than a
	// derived-from-code literal: hosting zero pins is the deliberate
	// zero-config default (docs/DEPLOY.md "Pin hosting is a pool-wide setting")
	// — an Orker that never sets this starts no session pollers and behaves
	// exactly as it did before pins existed.
	{Path: "orker.pins.max_concurrent", Env: "ORK_ORKER_MAX_PINS", Binaries: usesOrker, Kind: KindInt, Default: "0"},

	{Path: "orker.log.flush_bytes", Env: "ORK_ORKER_LOG_FLUSH_BYTES", Binaries: usesOrker, Kind: KindInt, Default: formatBytes(int64(orker.DefaultLogFlushBytes))},
	{Path: "orker.log.flush_interval", Env: "ORK_ORKER_LOG_FLUSH_INTERVAL", Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultLogFlushInterval)},
	{Path: "orker.log.max_bytes", Env: "ORK_ORKER_LOG_MAX_BYTES", Binaries: usesOrker, Kind: KindInt, Default: formatBytes(int64(orker.DefaultLogMaxBytes))},

	{Path: "orker.resources.default_cpu", Env: "ORK_ORKER_DEFAULT_CPU", Binaries: usesOrker, Kind: KindString, Default: orker.DefaultCPU},
	{Path: "orker.resources.default_memory", Env: "ORK_ORKER_DEFAULT_MEMORY", Binaries: usesOrker, Kind: KindString, Default: orker.DefaultMemory},
	{Path: "orker.resources.max_cpu", Env: "ORK_ORKER_MAX_CPU", Binaries: usesOrker, Kind: KindString, Default: orker.DefaultMaxCPU},
	{Path: "orker.resources.max_memory", Env: "ORK_ORKER_MAX_MEMORY", Binaries: usesOrker, Kind: KindString, Default: orker.DefaultMaxMemory},
	{Path: "orker.resources.pids_limit", Env: "ORK_ORKER_PIDS_LIMIT", Binaries: usesOrker, Kind: KindInt, Default: formatInt(orker.DefaultPidsLimit)},
	{Path: "orker.resources.tmpfs_bytes", Env: "ORK_ORKER_TMPFS_BYTES", Binaries: usesOrker, Kind: KindInt, Default: formatBytes(orker.DefaultTmpfsBytes)},
	{Path: "orker.resources.force_user", Env: "ORK_ORKER_FORCE_USER", Binaries: usesOrker, Kind: KindBool, Default: formatBool(orker.DefaultForceUser)},

	// ORK_DOCKER_BIN's documented default is orker.DefaultDockerBin plus
	// the "on PATH" resolution note; the constant itself is what bin()
	// falls back to.
	{Path: "orker.docker.bin", Env: "ORK_DOCKER_BIN", Binaries: usesOrker, Kind: KindString, Default: orker.DefaultDockerBin + " on PATH"},
	{Path: "orker.docker.cli", Env: "ORK_DOCKER_CLI", Binaries: usesOrker, Kind: KindString, Default: orker.DefaultDockerCLI},
	{Path: "orker.docker.pull_timeout", Env: "ORK_DOCKER_PULL_TIMEOUT", Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultPullTimeout)},
	{Path: "orker.docker.bind_probe_timeout", Env: "ORK_DOCKER_BIND_PROBE_TIMEOUT", Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultBindProbeTimeout)},

	{Path: "telemetry.log_format", Env: "ORK_LOG_FORMAT", Binaries: usesBoth, Kind: KindString, Default: telemetry.LogFormatText},
	{Path: "telemetry.log_level", Env: "ORK_LOG_LEVEL", Binaries: usesBoth, Kind: KindString, Default: telemetry.DefaultLogLevel},
	{Path: "telemetry.admin_addr", Env: "ORK_ADMIN_ADDR", Binaries: usesBoth, Kind: KindString, Default: EmptyDefault, DerivedNote: "no listener when unset"},
	{Path: "telemetry.pprof_enabled", Env: "ORK_PPROF_ENABLED", Binaries: usesBoth, Kind: KindBool, Default: "false"},
	{Path: "telemetry.high_cardinality", Env: "ORK_TELEMETRY_HIGH_CARDINALITY", Binaries: usesBoth, Kind: KindBool, Default: "false"},

	{Path: "telemetry.service_name", Env: "OTEL_SERVICE_NAME", Binaries: usesBoth, Kind: KindString, Default: "ork / orker"},
	{Path: "telemetry.resource_attributes", Env: "OTEL_RESOURCE_ATTRIBUTES", Binaries: usesBoth, Kind: KindMap, Default: EmptyDefault},

	{Path: "telemetry.exporters.traces", Env: "OTEL_TRACES_EXPORTER", Binaries: usesBoth, Kind: KindString, Default: telemetry.ExporterNone},
	{Path: "telemetry.exporters.metrics", Env: "OTEL_METRICS_EXPORTER", Binaries: usesBoth, Kind: KindList, Default: telemetry.ExporterNone},
	{Path: "telemetry.exporters.logs", Env: "OTEL_LOGS_EXPORTER", Binaries: usesBoth, Kind: KindString, Default: telemetry.ExporterNone},

	{Path: "telemetry.sampler.name", Env: "OTEL_TRACES_SAMPLER", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "SDK default: parentbased_always_on"},
	{Path: "telemetry.sampler.arg", Env: "OTEL_TRACES_SAMPLER_ARG", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "SDK default"},

	{Path: "telemetry.otlp.endpoint", Env: "OTEL_EXPORTER_OTLP_ENDPOINT", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.protocol", Env: "OTEL_EXPORTER_OTLP_PROTOCOL", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.headers", Env: "OTEL_EXPORTER_OTLP_HEADERS", Binaries: usesBoth, Kind: KindMap, Secret: true, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.timeout", Env: "OTEL_EXPORTER_OTLP_TIMEOUT", Binaries: usesBoth, Kind: KindInt, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.certificate", Env: "OTEL_EXPORTER_OTLP_CERTIFICATE", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "SDK defaults"},

	{Path: "telemetry.otlp.traces.endpoint", Env: "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.traces.headers", Env: "OTEL_EXPORTER_OTLP_TRACES_HEADERS", Binaries: usesBoth, Kind: KindMap, Secret: true, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.metrics.endpoint", Env: "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.metrics.headers", Env: "OTEL_EXPORTER_OTLP_METRICS_HEADERS", Binaries: usesBoth, Kind: KindMap, Secret: true, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.logs.endpoint", Env: "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "SDK defaults"},
	{Path: "telemetry.otlp.logs.headers", Env: "OTEL_EXPORTER_OTLP_LOGS_HEADERS", Binaries: usesBoth, Kind: KindMap, Secret: true, Default: DerivedDefault, DerivedNote: "SDK defaults"},

	{Path: "telemetry.batch.metric_export_interval", Env: "OTEL_METRIC_EXPORT_INTERVAL", Binaries: usesBoth, Kind: KindInt, Default: DerivedDefault, DerivedNote: "SDK default"},

	// telemetry.sinks is a document: it re-serializes to ORK_TELEMETRY_SINKS
	// exactly like a standalone telemetry.ParseSinks document. Its `ork` and
	// `orker` sink NAMES (a deployment may name a sink either string, and
	// often does — see docs/DEPLOY.md §7, "Named sinks") are ordinary map
	// keys within that document, unrelated to the {ork:, orker:} per-binary
	// SCALAR-LEAF form the rest of this schema uses: a document is never
	// filtered per binary, so both binaries see every sink a file defines,
	// whatever it is named.
	{Path: "telemetry.sinks", Env: "ORK_TELEMETRY_SINKS", Binaries: usesBoth, Kind: KindDocument, Default: EmptyDefault, DerivedNote: "no named sinks"},
}

// byEnv indexes Schema by environment variable name, built once at init so
// Lookup and Apply need not scan the table.
var byEnv = func() map[string]Var {
	index := make(map[string]Var, len(Schema))
	for _, v := range Schema {
		index[v.Env] = v
	}
	return index
}()

// Lookup returns the Schema entry mapped to env, if any.
func Lookup(env string) (Var, bool) {
	v, ok := byEnv[env]
	return v, ok
}

// usesBinary reports whether v applies to binary.
func (v Var) usesBinary(binary Binary) bool {
	for _, b := range v.Binaries {
		if b == binary {
			return true
		}
	}
	return false
}
