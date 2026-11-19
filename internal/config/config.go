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
	"strconv"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/procguard"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
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
	// KindDocument is one of the YAML subtrees (activities,
	// telemetry.sinks, auth.issuers, auth.orkers) that round-trip as YAML text rather than a scalar.
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
	// File marks a setting whose value names a file (a list, each item one):
	// the Orker holds every such file as its own, whichever binary the key
	// belongs to, so no task's secret names one (FileSettings,
	// Document.ForeignFiles; docs/SECURITY_MODEL.md, "The Orker's own
	// sources"). FileField names, for a KindDocument, the field that holds
	// a file in any entry of the document; FileSourceField the field that
	// holds a secret source in any entry, a file when it is file:/PATH.
	File            bool
	FileField       string
	FileSourceField string
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
	// The connection's TLS and API key (internal/temporalconn). enabled has
	// no default of its own: unset, TLS is on exactly when a file or a server
	// name below is set, and false beside one of them is refused. A string,
	// since a boolean has no empty value: temporalconn.SettingsFromEnv refuses
	// anything but true, false, 1 or 0, naming the setting.
	{Path: "temporal.tls.enabled", Env: "ORK_TEMPORAL_TLS_ENABLED", Binaries: usesBoth, Kind: KindString, Default: EmptyDefault, DerivedNote: "empty is off unless a file or server_name below is set; false beside one is refused"},
	{Path: "temporal.tls.ca_file", Env: "ORK_TEMPORAL_TLS_CA_FILE", Binaries: usesBoth, Kind: KindString, File: true, Default: EmptyDefault, DerivedNote: "empty verifies against the system roots"},
	{Path: "temporal.tls.cert_file", Env: "ORK_TEMPORAL_TLS_CERT_FILE", Binaries: usesBoth, Kind: KindString, File: true, Default: EmptyDefault, DerivedNote: "set with key_file, for mutual TLS"},
	{Path: "temporal.tls.key_file", Env: "ORK_TEMPORAL_TLS_KEY_FILE", Binaries: usesBoth, Kind: KindString, File: true, Default: EmptyDefault, DerivedNote: "set with cert_file"},
	{Path: "temporal.tls.server_name", Env: "ORK_TEMPORAL_TLS_SERVER_NAME", Binaries: usesBoth, Kind: KindString, Default: EmptyDefault, DerivedNote: "empty is the address's host"},
	{Path: "temporal.api_key_file", Env: "ORK_TEMPORAL_API_KEY_FILE", Binaries: usesBoth, Kind: KindString, File: true, Default: EmptyDefault, DerivedNote: "a file holding the key, never the key"},

	{Path: "deployment.name", Env: "ORK_DEPLOYMENT_NAME", Binaries: usesBoth, Kind: KindString, Default: task.DefaultDeploymentName},
	{Path: "deployment.build_id", Env: "ORK_BUILD_ID", Binaries: usesBoth, Kind: KindString, Default: EmptyDefault, DerivedNote: "empty means versioning is off"},
	// Both binaries make themselves undumpable at start unless this is set
	// (internal/procguard).
	{Path: procguard.SettingAllowDebug, Env: procguard.AllowDebugVar, Binaries: usesBoth, Kind: KindBool, Default: formatBool(procguard.DefaultAllowDebug)},

	{Path: "pool.name", Env: "ORK_ORKER_POOL", Binaries: usesBoth, Kind: KindString, Default: task.DefaultPool},
	{Path: "pool.queue", Env: "ORK_ORKER_QUEUE", Binaries: usesBoth, Kind: KindString, Default: DerivedDefault, DerivedNote: "derived from the pool"},

	{Path: "storage.backend", Env: "ORK_ARTIFACT_BACKEND", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultBackend},
	{Path: "storage.dir", Env: "ORK_ARTIFACT_DIR", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultLocalDir},
	{Path: "storage.s3.endpoint", Env: "ORK_S3_ENDPOINT", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultS3Endpoint},
	{Path: "storage.s3.region", Env: "ORK_S3_REGION", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultS3Region},
	{Path: "storage.s3.bucket", Env: "ORK_S3_BUCKET", Binaries: usesBoth, Kind: KindString, Default: artifacts.DefaultS3Bucket},
	{Path: "storage.s3.access_key", Env: "ORK_S3_ACCESS_KEY", Binaries: usesBoth, Kind: KindSecret, Secret: true, Default: EmptyDefault, DerivedNote: "set with secret_key; empty uses the default credential chain, such as an instance or pod role"},
	{Path: "storage.s3.secret_key", Env: "ORK_S3_SECRET_KEY", Binaries: usesBoth, Kind: KindSecret, Secret: true, Default: EmptyDefault, DerivedNote: "set with access_key; empty uses the default credential chain, such as an instance or pod role"},
	{Path: "storage.s3.session_token", Env: "ORK_S3_SESSION_TOKEN", Binaries: usesBoth, Kind: KindSecret, Secret: true, Default: EmptyDefault, DerivedNote: "for temporary credentials"},
	{Path: "storage.s3.path_style", Env: "ORK_S3_PATH_STYLE", Binaries: usesBoth, Kind: KindBool, Default: formatBool(artifacts.DefaultS3PathStyle)},

	{Path: "engine.http_addr", Env: "ORK_HTTP_ADDR", Binaries: usesOrk, Kind: KindString, Default: api.DefaultHTTPAddr},
	{Path: "engine.ws_allowed_origins", Env: "ORK_WS_ALLOWED_ORIGINS", Binaries: usesOrk, Kind: KindList, Default: EmptyDefault},
	// The API listener's settings; its fixed bounds are internal/api
	// constants (ReadHeaderTimeout, MaxHeaderBytes). ORK_REQUEST_MAX_BYTES
	// falls back to api.RequestMaxBytesFor(engine.orkflow_max_tasks), so it
	// scales with the task ceiling, and cmd/ork refuses a value under that.
	{Path: "engine.http_idle_timeout", Env: "ORK_HTTP_IDLE_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(api.DefaultIdleTimeout)},
	{Path: "engine.request_max_bytes", Env: "ORK_REQUEST_MAX_BYTES", Binaries: usesOrk, Kind: KindInt, Default: DerivedDefault,
		DerivedNote: formatBytes(api.RequestBaseBytes) + " + " + formatBytes(api.RequestBytesPerTask) +
			" × engine.orkflow_max_tasks, " + formatInt(api.DefaultRequestMaxBytes) + " at the default ceiling; never set under that"},
	{Path: "engine.allowed_hosts", Env: "ORK_ALLOWED_HOSTS", Binaries: usesOrk, Kind: KindList, Default: EmptyDefault,
		DerivedNote: "an IP address, localhost and *.localhost, a name without a dot, and the host ORK_HTTP_ADDR names are always admitted"},
	{Path: "engine.ui.enabled", Env: "ORK_UI_ENABLED", Binaries: usesOrk, Kind: KindBool, Default: formatBool(api.DefaultUIEnabled)},
	{Path: "engine.ui.dir", Env: "ORK_UI_DIR", Binaries: usesOrk, Kind: KindString, Default: EmptyDefault, DerivedNote: "the build compiled into ork"},
	// ORK_IDEMPOTENCY_TTL's fallback is engine.DefaultClaimTTL, the same
	// constant idempotency.go binds a non-positive ttl to.
	{Path: "engine.idempotency_ttl", Env: "ORK_IDEMPOTENCY_TTL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultClaimTTL)},
	{Path: "engine.registry_auth_file", Env: "ORK_REGISTRY_AUTH_FILE", Binaries: usesOrk, Kind: KindString, File: true, Default: EmptyDefault},
	// Event sources (docs/EVENT_SOURCES.md): each fallback is the
	// pkg/eventsources constant SettingsFromEnv binds an unset variable to.
	{Path: "event_sources.enabled", Env: eventsources.EnvEnabled, Binaries: usesOrk, Kind: KindBool, Default: formatBool(eventsources.DefaultEnabled)},
	{Path: "event_sources.allow_asserted", Env: eventsources.EnvAllowAsserted, Binaries: usesOrk, Kind: KindBool, Default: formatBool(eventsources.DefaultAllowAsserted)},
	{Path: "event_sources.max_message_bytes", Env: eventsources.EnvMaxMessageBytes, Binaries: usesOrk, Kind: KindInt, Default: formatInt(eventsources.DefaultMaxMessageBytes)},
	{Path: "event_sources.max_input_bytes", Env: eventsources.EnvMaxInputBytes, Binaries: usesOrk, Kind: KindInt, Default: formatInt(eventsources.DefaultMaxInputBytes)},
	{Path: "event_sources.max_rate", Env: eventsources.EnvMaxRate, Binaries: usesOrk, Kind: KindInt, Default: formatInt(eventsources.DefaultMaxRate)},
	{Path: "event_sources.namespace_rate", Env: eventsources.EnvNamespaceRate, Binaries: usesOrk, Kind: KindInt, Default: formatInt(eventsources.DefaultNamespaceRate)},
	{Path: "event_sources.max_sources", Env: eventsources.EnvMaxSources, Binaries: usesOrk, Kind: KindInt, Default: formatInt(eventsources.DefaultMaxSources)},
	{Path: "event_sources.max_per_namespace", Env: eventsources.EnvMaxPerNamespace, Binaries: usesOrk, Kind: KindInt, Default: formatInt(eventsources.DefaultMaxPerNamespace)},
	{Path: "event_sources.max_instances", Env: eventsources.EnvMaxInstances, Binaries: usesOrk, Kind: KindInt, Default: formatInt(eventsources.DefaultMaxInstances)},
	{Path: "event_sources.namespace_instances", Env: eventsources.EnvNamespaceInstances, Binaries: usesOrk, Kind: KindInt, Default: formatInt(eventsources.DefaultNamespaceInstances)},
	{Path: "webhooks.allowed_networks", Env: "ORK_WEBHOOK_ALLOWED_NETWORKS", Binaries: usesOrk, Kind: KindList, Default: EmptyDefault,
		DerivedNote: "empty admits public addresses only"},
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
	// engine.interruptions.* (E92): the budgets every accepted run pins in
	// its plan; each default is engine.DefaultInterruptionBudgets'.
	{Path: "engine.interruptions.lost", Env: "ORK_INTERRUPTIONS_LOST", Binaries: usesOrk, Kind: KindInt, Default: strconv.Itoa(int(engine.DefaultInterruptionBudgets.Lost))},
	{Path: "engine.interruptions.lost_run_cap", Env: "ORK_INTERRUPTIONS_LOST_RUN_CAP", Binaries: usesOrk, Kind: KindInt, Default: strconv.Itoa(int(engine.DefaultInterruptionBudgets.LostRunCap))},
	{Path: "engine.interruptions.handed_back", Env: "ORK_INTERRUPTIONS_HANDED_BACK", Binaries: usesOrk, Kind: KindInt, Default: strconv.Itoa(int(engine.DefaultInterruptionBudgets.HandedBack))},
	{Path: "engine.interruptions.never_started", Env: "ORK_INTERRUPTIONS_NEVER_STARTED", Binaries: usesOrk, Kind: KindInt, Default: strconv.Itoa(int(engine.DefaultInterruptionBudgets.NeverStarted))},
	{Path: "engine.interruptions.never_started_faults", Env: "ORK_INTERRUPTIONS_NEVER_STARTED_FAULTS", Binaries: usesOrk, Kind: KindInt, Default: strconv.Itoa(int(engine.DefaultInterruptionBudgets.NeverStartedFaults))},
	{Path: "engine.interruptions.never_started_backoff", Env: "ORK_INTERRUPTIONS_NEVER_STARTED_BACKOFF", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(time.Duration(engine.DefaultInterruptionBudgets.NeverStartedBackoffMillis) * time.Millisecond)},
	// engine.pool_metrics.* (E92): the pools beside the default one whose
	// backlog each replica reads for the ork.pool.* gauges, and how often.
	{Path: "engine.pool_metrics.pools", Env: "ORK_POOL_METRICS_POOLS", Binaries: usesOrk, Kind: KindList, Default: EmptyDefault},
	{Path: "engine.pool_metrics.interval", Env: "ORK_POOL_METRICS_INTERVAL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultPoolMetricsInterval)},
	// ORK_RUN_TIMEOUT_HANDLER_GRACE's fallback is
	// engine.DefaultRunTimeoutHandlerGrace: how long after a run's deadline
	// its failure handlers may still run. Pinned, in seconds, into the plan
	// of every run whose definition declares `timeout:`.
	{Path: "engine.run_timeout_handler_grace", Env: "ORK_RUN_TIMEOUT_HANDLER_GRACE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultRunTimeoutHandlerGrace)},
	// ORK_RUN_PAUSE_TIMEOUT's and ORK_RUN_PAUSE_TIMEOUT_MAX's fallbacks are
	// engine.DefaultRunPauseTimeout and engine.DefaultRunPauseTimeoutMax: a
	// pause's default length and its longest, pinned into every run's plan.
	{Path: "engine.run_pause_timeout", Env: "ORK_RUN_PAUSE_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultRunPauseTimeout)},
	{Path: "engine.run_pause_timeout_max", Env: "ORK_RUN_PAUSE_TIMEOUT_MAX", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultRunPauseTimeoutMax)},
	// engine.concurrency.* (E88): the concurrency gates' own settings, decided
	// by the acquire that starts a gate and carried by it until it closes, and
	// the census's. Each default is engine.DefaultGateTunables' or
	// engine.DefaultCensusSettings'.
	{Path: "engine.concurrency.max_entries", Env: "ORK_CONCURRENCY_MAX_ENTRIES", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultGateTunables().MaxEntries))},
	{Path: "engine.concurrency.max_tombstones", Env: "ORK_CONCURRENCY_MAX_TOMBSTONES", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultGateTunables().MaxTombstones))},
	{Path: "engine.concurrency.max_labels", Env: "ORK_CONCURRENCY_MAX_LABELS", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultGateTunables().MaxLabels))},
	{Path: "engine.concurrency.acquire_timeout", Env: "ORK_CONCURRENCY_ACQUIRE_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().AcquireTimeout)},
	{Path: "engine.concurrency.clock_grace", Env: "ORK_CONCURRENCY_CLOCK_GRACE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().ClockGrace)},
	{Path: "engine.concurrency.tombstone_margin", Env: "ORK_CONCURRENCY_TOMBSTONE_MARGIN", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().TombstoneMargin)},
	{Path: "engine.concurrency.verify_min_age", Env: "ORK_CONCURRENCY_VERIFY_MIN_AGE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().VerifyMinAge)},
	{Path: "engine.concurrency.verify_debounce", Env: "ORK_CONCURRENCY_VERIFY_DEBOUNCE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().VerifyDebounce)},
	{Path: "engine.concurrency.verify_await", Env: "ORK_CONCURRENCY_VERIFY_AWAIT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().VerifyAwait)},
	{Path: "engine.concurrency.verify_parallel", Env: "ORK_CONCURRENCY_VERIFY_PARALLEL", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultGateTunables().VerifyParallel))},
	{Path: "engine.concurrency.verify_batch", Env: "ORK_CONCURRENCY_VERIFY_BATCH", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultGateTunables().VerifyBatch))},
	{Path: "engine.concurrency.verify_timeout", Env: "ORK_CONCURRENCY_VERIFY_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().VerifyTimeout)},
	{Path: "engine.concurrency.task_ack_grace", Env: "ORK_CONCURRENCY_TASK_ACK_GRACE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().TaskAckGrace)},
	{Path: "engine.concurrency.task_confirm_batch", Env: "ORK_CONCURRENCY_TASK_CONFIRM_BATCH", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultGateTunables().TaskConfirmBatch))},
	{Path: "engine.concurrency.stalled_after", Env: "ORK_CONCURRENCY_STALLED_AFTER", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().StalledAfter)},
	{Path: "engine.concurrency.holder_sweep_interval", Env: "ORK_CONCURRENCY_HOLDER_SWEEP_INTERVAL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().HolderSweepInterval)},
	{Path: "engine.concurrency.grant_backoff_min", Env: "ORK_CONCURRENCY_GRANT_BACKOFF_MIN", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().GrantBackoffMin)},
	{Path: "engine.concurrency.grant_backoff_max", Env: "ORK_CONCURRENCY_GRANT_BACKOFF_MAX", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().GrantBackoffMax)},
	{Path: "engine.concurrency.rebuild_page_timeout", Env: "ORK_CONCURRENCY_REBUILD_PAGE_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().RebuildPageTimeout)},
	{Path: "engine.concurrency.rebuild_settle", Env: "ORK_CONCURRENCY_REBUILD_SETTLE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().RebuildSettle)},
	{Path: "engine.concurrency.rebuild_backoff_min", Env: "ORK_CONCURRENCY_REBUILD_BACKOFF_MIN", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().RebuildBackoffMin)},
	{Path: "engine.concurrency.rebuild_backoff_max", Env: "ORK_CONCURRENCY_REBUILD_BACKOFF_MAX", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().RebuildBackoffMax)},
	{Path: "engine.concurrency.rebuild_timeout", Env: "ORK_CONCURRENCY_REBUILD_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().RebuildTimeout)},
	{Path: "engine.concurrency.freeze_timeout", Env: "ORK_CONCURRENCY_FREEZE_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().FreezeTimeout)},
	{Path: "engine.concurrency.drain_margin", Env: "ORK_CONCURRENCY_DRAIN_MARGIN", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().DrainMargin)},
	{Path: "engine.concurrency.gate_continue_events", Env: "ORK_CONCURRENCY_GATE_CONTINUE_EVENTS", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultGateTunables().ContinueEvents))},
	{Path: "engine.concurrency.gate_continue_updates", Env: "ORK_CONCURRENCY_GATE_CONTINUE_UPDATES", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultGateTunables().ContinueUpdates))},
	{Path: "engine.concurrency.gate_linger", Env: "ORK_CONCURRENCY_GATE_LINGER", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultGateTunables().Linger)},
	{Path: "engine.concurrency.census_interval", Env: "ORK_CONCURRENCY_CENSUS_INTERVAL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultCensusSettings().Interval)},
	{Path: "engine.concurrency.census_timeout", Env: "ORK_CONCURRENCY_CENSUS_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(engine.DefaultCensusSettings().Timeout)},
	{Path: "engine.concurrency.census_max_gates", Env: "ORK_CONCURRENCY_CENSUS_MAX_GATES", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultCensusSettings().MaxGates))},
	{Path: "engine.concurrency.census_parallel", Env: "ORK_CONCURRENCY_CENSUS_PARALLEL", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(engine.DefaultCensusSettings().Parallel))},
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
	// The artifact GC (docs/STORAGE.md, "Reclaiming space"): off, dry_run
	// or delete; it runs only with cleanup. ork refuses any other mode, and
	// a grace below archive.MinReclaimGrace, at boot.
	{Path: "engine.cleanup.artifact_gc", Env: "ORK_CLEANUP_ARTIFACT_GC", Binaries: usesOrk, Kind: KindString, Default: string(archive.DefaultReclaimMode)},
	{Path: "engine.cleanup.artifact_gc_grace", Env: "ORK_CLEANUP_ARTIFACT_GC_GRACE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(archive.DefaultReclaimGrace)},
	{Path: "engine.cleanup.artifact_gc_batch", Env: "ORK_CLEANUP_ARTIFACT_GC_BATCH", Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(archive.DefaultReclaimBatch))},

	// activities has no single scalar default: it is the built-in catalog
	// baked into internal/dsl, not a value this package can format.
	{Path: "activities", Env: "ORK_ACTIVITY_CATALOG", Binaries: usesOrk, Kind: KindDocument, Default: "built-in shell/http/file/json/git/template → ghcr.io/orkstack/activity-*:0.3"},

	// orker.id and orker.workdir are genuinely dynamic (a hostname hash, and
	// os.TempDir()) — real prose, not one of the two standardized
	// placeholders, since docs/DEPLOY.md documents them the same way.
	{Path: "orker.id", Env: "ORK_ORKER_ID", Binaries: usesOrker, Kind: KindString, Default: "hostname-<hash of the work dir>"},
	{Path: "orker.workdir", Env: "ORK_ORKER_WORKDIR", Binaries: usesOrker, Kind: KindString, Default: "temp dir"},
	{Path: "orker.image_layout", Env: "ORK_ORKER_IMAGE_LAYOUT", Binaries: usesOrker, Kind: KindString, Default: orker.DefaultImageLayoutRoot},
	{Path: "orker.env_allowlist", Env: "ORK_ORKER_ENV_ALLOWLIST", Binaries: usesOrker, Kind: KindList, Default: EmptyDefault},
	{Path: "orker.secret_schemes", Env: "ORK_ORKER_SECRET_SCHEMES", Binaries: usesOrker, Kind: KindList, Default: formatList(orker.DefaultSecretSchemes)},
	{Path: "orker.secret_allow", Env: "ORK_ORKER_SECRET_ALLOW", Binaries: usesOrker, Kind: KindList, Default: EmptyDefault},
	{Path: "orker.namespaces", Env: "ORK_ORKER_NAMESPACES", Binaries: usesOrker, Kind: KindList, Default: EmptyDefault},
	{Path: "orker.registry_auth_file", Env: "ORK_ORKER_REGISTRY_AUTH_FILE", Binaries: usesOrker, Kind: KindString, File: true, Default: EmptyDefault},
	{Path: "orker.task_log_export", Env: "ORK_ORKER_TASK_LOG_EXPORT", Binaries: usesOrker, Kind: KindBool, Default: "false"},
	{Path: "orker.heartbeat_interval", Env: "ORK_ORKER_HEARTBEAT_INTERVAL", Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultHeartbeatInterval)},
	{Path: "orker.usage_sample_interval", Env: orker.UsageSampleIntervalVar, Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultUsageSampleInterval)},
	{Path: "orker.usage_log_interval", Env: orker.UsageLogIntervalVar, Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultUsageLogInterval)},
	{Path: "orker.stop.grace", Env: orker.StopGraceVar, Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultStopGrace)},
	{Path: "orker.max_concurrent", Env: orker.MaxConcurrentVar, Binaries: usesOrker, Kind: KindInt, Default: formatInt(int64(orker.DefaultMaxConcurrent))},
	{Path: "orker.store_check.enabled", Env: orker.StoreCheckEnabledVar, Binaries: usesOrker, Kind: KindBool, Default: formatBool(orker.DefaultStoreCheckEnabled)},
	{Path: "orker.store_check.interval", Env: orker.StoreCheckIntervalVar, Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultStoreCheckInterval)},
	{Path: "orker.store_check.timeout", Env: orker.StoreCheckTimeoutVar, Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultStoreCheckTimeout)},
	{Path: "orker.store_check.failures", Env: orker.StoreCheckFailuresVar, Binaries: usesOrker, Kind: KindInt, Default: formatInt(int64(orker.DefaultStoreCheckFailures))},
	{Path: "orker.store_check.recoveries", Env: orker.StoreCheckRecoveriesVar, Binaries: usesOrker, Kind: KindInt, Default: formatInt(int64(orker.DefaultStoreCheckRecoveries))},
	{Path: "orker.store_scope_recheck", Env: orker.StoreScopeRecheckVar, Binaries: usesOrker, Kind: KindDuration, Default: formatDuration(orker.DefaultStoreScopeRecheck)},
	// orker.capacity is empty by default: no declared capacity admits by
	// max_concurrent alone, as every Orker did before capacity existed.
	{Path: "orker.capacity.cpu", Env: orker.CapacityCPUVar, Binaries: usesOrker, Kind: KindString, Default: EmptyDefault},
	{Path: "orker.capacity.memory", Env: orker.CapacityMemoryVar, Binaries: usesOrker, Kind: KindString, Default: EmptyDefault},

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
	{Path: "orker.resources.max_gpu", Env: orker.MaxGPUVar, Binaries: usesOrker, Kind: KindInt, Default: formatInt(orker.DefaultMaxGPU)},
	// The trusted CDI spec directories a gpu profile's audit reads
	// (internal/orker/cdi_audit.go); the defaults are the CDI library's.
	{Path: "orker.cdi.spec_dirs", Env: orker.CDISpecDirsVar, Binaries: usesOrker, Kind: KindList, Default: formatList(orker.DefaultCDISpecDirs)},
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
	// The boot probe of the host's instance metadata endpoints: off, warn or
	// refuse (internal/orker/metadata_probe.go).
	{Path: "orker.metadata_probe", Env: orker.MetadataProbeVar, Binaries: usesOrker, Kind: KindString, Default: string(orker.DefaultMetadataProbe)},
	// The profiles this Orker offers tasks by name (internal/orker profiles.go),
	// one document: probe_image, the two intervals and each named profile.
	{Path: "orker.profiles", Env: orker.ProfilesVar, Binaries: usesOrker, Kind: KindDocument, Default: EmptyDefault, DerivedNote: "no profiles"},
	// Secret masking in a task's logs, and the refusal of a values.json that
	// holds a secret (internal/orker/mask.go).
	{Path: "orker.log.mask_secrets", Env: orker.MaskSecretsVar, Binaries: usesOrker, Kind: KindBool, Default: formatBool(orker.DefaultMaskSecrets)},
	{Path: "orker.values.refuse_secrets", Env: orker.RefuseSecretValuesVar, Binaries: usesOrker, Kind: KindBool, Default: formatBool(orker.DefaultRefuseSecretVals)},
	{Path: "orker.refuse_readable_engine_keys", Env: orker.RefuseReadableEngineKeysVar, Binaries: usesOrker, Kind: KindBool, Default: formatBool(orker.DefaultRefuseReadableEngineKeys)},

	{Path: "telemetry.log_format", Env: "ORK_LOG_FORMAT", Binaries: usesBoth, Kind: KindString, Default: telemetry.LogFormatText},
	{Path: "telemetry.log_level", Env: "ORK_LOG_LEVEL", Binaries: usesBoth, Kind: KindString, Default: telemetry.DefaultLogLevel},
	{Path: "telemetry.admin_addr", Env: "ORK_ADMIN_ADDR", Binaries: usesBoth, Kind: KindString, Default: EmptyDefault, DerivedNote: "no listener when unset"},
	{Path: "telemetry.admin_allowed_hosts", Env: "ORK_ADMIN_ALLOWED_HOSTS", Binaries: usesBoth, Kind: KindList, Default: EmptyDefault,
		DerivedNote: "an IP address, localhost and *.localhost, a name without a dot, and the host ORK_ADMIN_ADDR names are always admitted"},
	{Path: "telemetry.pprof_enabled", Env: "ORK_PPROF_ENABLED", Binaries: usesBoth, Kind: KindBool, Default: "false"},
	{Path: "telemetry.high_cardinality", Env: "ORK_TELEMETRY_HIGH_CARDINALITY", Binaries: usesBoth, Kind: KindBool, Default: "false"},
	{Path: "telemetry.orkflow_label", Env: telemetry.OrkflowLabelVar, Binaries: usesOrk, Kind: KindBool, Default: formatBool(telemetry.DefaultOrkflowLabel)},
	{Path: "telemetry.object_labels", Env: telemetry.ObjectLabelsVar, Binaries: usesOrk, Kind: KindBool, Default: formatBool(telemetry.DefaultObjectLabels)},
	{Path: "telemetry.metric_cardinality_limit", Env: telemetry.MetricCardinalityLimitVar, Binaries: usesBoth, Kind: KindInt, Default: formatInt(int64(telemetry.DefaultMetricCardinalityLimit))},
	{Path: "telemetry.census_requests_per_minute", Env: telemetry.CensusRequestsPerMinuteVar, Binaries: usesOrk, Kind: KindInt, Default: formatInt(int64(telemetry.DefaultCensusRequestsPerMinute))},

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
	{Path: "telemetry.otlp.certificate", Env: "OTEL_EXPORTER_OTLP_CERTIFICATE", Binaries: usesBoth, Kind: KindString, File: true, Default: DerivedDefault, DerivedNote: "SDK defaults"},

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

	// orker.engine.* is how an Orker reaches the engine's API as itself
	// (docs/AUTHENTICATION.md, "An Orker's credential"); empty, it reaches
	// none.
	{Path: "orker.engine.url", Env: orker.EngineURLVar, Binaries: usesOrker, Kind: KindString, Default: EmptyDefault, DerivedNote: "no engine: no identities"},
	{Path: "orker.engine.ca_file", Env: orker.EngineCAFileVar, Binaries: usesOrker, Kind: KindString, File: true, Default: EmptyDefault, DerivedNote: "the system's roots"},
	{Path: "orker.engine.auth.method", Env: orker.EngineAuthMethodVar, Binaries: usesOrker, Kind: KindString, Default: EmptyDefault, DerivedNote: "token_file, service_account or mtls"},
	{Path: "orker.engine.auth.service_account", Env: orker.EngineAuthServiceAccountVar, Binaries: usesOrker, Kind: KindString, Default: EmptyDefault},
	{Path: "orker.engine.auth.token_file", Env: orker.EngineAuthTokenFileVar, Binaries: usesOrker, Kind: KindString, File: true, Default: EmptyDefault},
	{Path: "orker.engine.auth.cert_file", Env: orker.EngineAuthCertFileVar, Binaries: usesOrker, Kind: KindString, File: true, Default: EmptyDefault},
	{Path: "orker.engine.auth.key_file", Env: orker.EngineAuthKeyFileVar, Binaries: usesOrker, Kind: KindString, File: true, Default: EmptyDefault},

	// auth.* is the engine's alone (docs/AUTHENTICATION.md). auth.issuers is
	// a document: it re-serializes to ORK_AUTH_ISSUERS exactly like a
	// standalone authn.ParseIssuers document, under one `issuers:` key.
	{Path: "auth.mode", Env: "ORK_AUTH_MODE", Binaries: usesOrk, Kind: KindString, Default: authn.ModeAsserted},
	{Path: "auth.public_url", Env: "ORK_AUTH_PUBLIC_URL", Binaries: usesOrk, Kind: KindString, Default: EmptyDefault, DerivedNote: "no break-glass audience"},
	{Path: "auth.issuers", Env: "ORK_AUTH_ISSUERS", Binaries: usesOrk, Kind: KindDocument, File: true, FileField: "jwks_file", FileSourceField: "source", Default: EmptyDefault, DerivedNote: "no trusted issuer"},
	{Path: "auth.admins", Env: "ORK_AUTH_ADMINS", Binaries: usesOrk, Kind: KindList, Default: EmptyDefault},
	{Path: "auth.orkers", Env: "ORK_AUTH_ORKERS", Binaries: usesOrk, Kind: KindDocument, Default: EmptyDefault, DerivedNote: "no Orker bound by configuration"},
	{Path: "auth.break_glass_key_file", Env: "ORK_AUTH_BREAK_GLASS_KEY_FILE", Binaries: usesOrk, Kind: KindString, File: true, Default: EmptyDefault, DerivedNote: "break-glass off"},
	// auth.signin.* is web sign-in (docs/AUTHENTICATION.md, "Sign-in for the
	// web UI"): the key set that seals its cookie and records, and its limits.
	{Path: "auth.signin.key_files", Env: "ORK_AUTH_SIGNIN_KEY_FILES", Binaries: usesOrk, Kind: KindList, File: true, Default: EmptyDefault, DerivedNote: "no sign-in"},
	{Path: "auth.signin.max_age", Env: "ORK_AUTH_SIGNIN_MAX_AGE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(authn.DefaultSigninMaxAge)},
	{Path: "auth.signin.idle_timeout", Env: "ORK_AUTH_SIGNIN_IDLE_TIMEOUT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(authn.DefaultSigninIdleTimeout)},
	{Path: "auth.signin.revalidate_interval", Env: "ORK_AUTH_SIGNIN_REVALIDATE_INTERVAL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(authn.DefaultSigninRevalidateInterval)},
	{Path: "auth.clock_skew", Env: "ORK_AUTH_CLOCK_SKEW", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(authn.DefaultClockSkew)},
	{Path: "auth.policy_refresh", Env: "ORK_AUTH_POLICY_REFRESH", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(api.DefaultPolicyRefresh)},
	{Path: "auth.policy_stale_limit", Env: "ORK_AUTH_POLICY_STALE_LIMIT", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(api.DefaultPolicyStaleLimit)},

	// identity.* is the task identity issuer, the engine's alone
	// (docs/TASK_IDENTITY.md). With identity.issuer unset nothing of it
	// runs.
	{Path: "identity.issuer", Env: "ORK_IDENTITY_ISSUER", Binaries: usesOrk, Kind: KindString, Default: EmptyDefault, DerivedNote: "task identity off"},
	{Path: "identity.serve_discovery", Env: "ORK_IDENTITY_SERVE_DISCOVERY", Binaries: usesOrk, Kind: KindBool, Default: formatBool(identity.DefaultServeDiscovery)},
	{Path: "identity.signing.key_files", Env: "ORK_IDENTITY_SIGNING_KEY_FILES", Binaries: usesOrk, Kind: KindList, File: true, Default: EmptyDefault},
	{Path: "identity.signing.algorithm", Env: "ORK_IDENTITY_SIGNING_ALGORITHM", Binaries: usesOrk, Kind: KindString, Default: string(identity.DefaultAlgorithm)},
	{Path: "identity.signing.activation_delay", Env: "ORK_IDENTITY_SIGNING_ACTIVATION_DELAY", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(identity.DefaultActivationDelay)},
	{Path: "identity.signing.verify_published", Env: "ORK_IDENTITY_SIGNING_VERIFY_PUBLISHED", Binaries: usesOrk, Kind: KindBool, Default: formatBool(identity.DefaultVerifyPublished)},
	{Path: "identity.keys_copy_file", Env: "ORK_IDENTITY_KEYS_COPY_FILE", Binaries: usesOrk, Kind: KindString, File: true, Default: EmptyDefault, DerivedNote: "no copy on disk"},
	{Path: "identity.keys_copy_max_age", Env: "ORK_IDENTITY_KEYS_COPY_MAX_AGE", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(identity.DefaultKeysCopyMaxAge)},
	{Path: "identity.subject_format", Env: "ORK_IDENTITY_SUBJECT_FORMAT", Binaries: usesOrk, Kind: KindString, Default: string(identity.DefaultSubjectFormat)},
	{Path: "identity.spiffe.trust_domain", Env: "ORK_IDENTITY_SPIFFE_TRUST_DOMAIN", Binaries: usesOrk, Kind: KindString, Default: EmptyDefault, DerivedNote: "no SPIFFE ID on any token"},
	{Path: "identity.attempts.extra", Env: "ORK_IDENTITY_ATTEMPTS_EXTRA", Binaries: usesOrk, Kind: KindInt, Default: formatInt(identity.DefaultAttemptsExtra)},
	{Path: "identity.attempts.max_extra", Env: "ORK_IDENTITY_ATTEMPTS_MAX_EXTRA", Binaries: usesOrk, Kind: KindInt, Default: formatInt(identity.DefaultAttemptsMaxExtra)},
	{Path: "identity.attempts.max", Env: "ORK_IDENTITY_ATTEMPTS_MAX", Binaries: usesOrk, Kind: KindInt, Default: formatInt(identity.DefaultAttemptsMax)},
	{Path: "identity.admission_cache_bytes", Env: "ORK_IDENTITY_ADMISSION_CACHE_BYTES", Binaries: usesOrk, Kind: KindInt, Default: formatInt(identity.DefaultAdmissionCacheBytes)},
	{Path: "identity.token.ttl", Env: "ORK_IDENTITY_TOKEN_TTL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(identity.DefaultTokenTTL)},
	{Path: "identity.token.min_ttl", Env: "ORK_IDENTITY_TOKEN_MIN_TTL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(identity.DefaultTokenMinTTL)},
	{Path: "identity.token.max_ttl", Env: "ORK_IDENTITY_TOKEN_MAX_TTL", Binaries: usesOrk, Kind: KindDuration, Default: formatDuration(identity.DefaultTokenMaxTTL)},
	{Path: "identity.issuance.records", Env: "ORK_IDENTITY_ISSUANCE_RECORDS", Binaries: usesOrk, Kind: KindString, Default: identity.IssuanceRequired},
	{Path: "identity.unscoped_orkers", Env: "ORK_IDENTITY_UNSCOPED_ORKERS", Binaries: usesOrk, Kind: KindString, Default: identity.UnscopedRefuse},
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
