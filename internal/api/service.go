package api

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/typeid"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
	"github.com/orkstack/orkstack/pkg/webhooks"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RunStarter is the seam between the API and Temporal. Tests substitute a fake
// so the Connect surface can be exercised without a cluster.
//
// It is an alias for the engine's declaration, not a second copy of it: a
// namespace router hands out bindings carrying a starter, and one type keeps
// what the router returns and what the API accepts from drifting apart.
type RunStarter = engine.RunStarter

// RunArchive is the seam between the API and durable run history. Temporal is
// the record for a live run; the archive is the record for a finished one, and
// the only record left once history ages out of retention. Tests substitute a
// fake.
type RunArchive interface {
	// Archive writes a finished run and its events, with one gap record per
	// range missing from the stream, lost before archiving or never
	// published.
	Archive(ctx context.Context, run *orkstackv1.Run, events []*orkstackv1.Event, gaps ...engine.EventGap) error
	// LoadRun returns an archived run, or an error matching
	// artifacts.ErrNotFound when the run was never archived.
	LoadRun(ctx context.Context, runID string) (*orkstackv1.Run, error)
	// LoadRunEvents returns an archived run's events, in sequence order. A
	// body that records gaps returns the events it holds and an
	// *engine.ArchivedEventsLostError naming every range.
	LoadRunEvents(ctx context.Context, runID string) ([]*orkstackv1.Event, error)
	// ListRuns returns archived runs matching q, newest first.
	ListRuns(ctx context.Context, q engine.ArchiveQuery) ([]*orkstackv1.Run, error)
}

// RunReader is the seam between the API and the system of record for live
// runs. `ork` wires engine.TemporalRunReader, so a read is answered from
// Temporal and any replica answers for any run — including one it did not
// start and whose projection activities it never executed.
//
// A missing run is reported with an error matching artifacts.ErrNotFound, and
// only ever before the first event of a stream has been sent, so a caller can
// fall back to the archive without duplicating anything.
//
// It is an alias for the engine's declaration, for the same reason RunStarter
// is: the namespace router hands one out with every binding.
type RunReader = engine.RunReader

// Config wires a Service to its stores and Temporal seams.
type Config struct {
	// Orkflows is the durable orkflow store. `ork` wires the S3 store
	// (cmd/ork/main.go); leaving it nil is a unit-test convenience that
	// substitutes the in-memory store, never a fallback for a configured one.
	Orkflows engine.OrkflowStore
	Starter  RunStarter

	// Reader is the system of record for live runs. `ork` wires
	// engine.TemporalRunReader, and every read then leaves the process. Nil
	// falls back to Runs/Bus below, which is one process answering for the
	// runs it happens to have seen — correct only when there is one process,
	// and therefore for unit tests only.
	Reader RunReader

	// Runs and Bus are the write-side helpers: the projection activity fills
	// them so an archive body can be assembled and a webhook notification
	// deduplicated. They answer a read only when no Reader is wired.
	Runs *engine.RunStore
	Bus  *engine.EventBus

	// Idempotency decides which run an idempotency key names. `ork` wires
	// engine.TemporalIdempotency, so the decision is the cluster's and every
	// replica gets the same answer. Nil substitutes the process-local
	// claimer, which is correct for one process and therefore for unit tests
	// only.
	Idempotency IdempotencyClaimer

	// OnLegacyPending is called each time a key claimed by an earlier
	// release is answered with a pending run that could not be found
	// (ork.idempotency.legacy_pending). Nil counts nothing.
	OnLegacyPending func(context.Context)

	// Archive is the durable run history. Nil keeps runs in process only.
	Archive RunArchive

	// Logs is where a task attempt's output chunks live. `ork` wires the same
	// artifact backend the Orker writes them to, which is what makes any
	// replica able to serve any run's logs — including one it never started
	// and one whose Orker is gone. Nil answers StreamTaskLogs with
	// FailedPrecondition rather than an empty stream: an operator who never
	// configured a store must be told, not shown a quiet task.
	Logs artifacts.LogStore
	// ArtifactStore holds committed activity outputs, values and retained
	// stdout/stderr. It is deliberately separate from Logs because log chunks
	// have their own attempt index and streaming read contract.
	ArtifactStore artifacts.Store

	// Attempts is where the Orker filed one immutable record per task attempt.
	// It is the same bucket, wired from the same ORK_ARTIFACT_BACKEND, for the
	// same reason Logs is: the process that observed a retry is not the process
	// asked about it later, and any replica must answer for any run. Nil
	// answers ListTaskAttempts with FailedPrecondition rather than an empty
	// timeline — an operator who configured no store must be told that, not
	// shown a task that looks like it never retried.
	Attempts artifacts.AttemptStore

	// Commits is the durable completion-record store, READ by
	// ExportRunEvidence and by nothing else in this process: the engine never
	// composes a commit uri here, it only ever opens the uri a run snapshot
	// recorded. `ork` wires it from the same ORK_ARTIFACT_BACKEND the Orker
	// writes through. Nil answers ExportRunEvidence with FailedPrecondition.
	Commits artifacts.CommitStore

	// LogPoll tunes how often a log follower re-lists the store. The zero
	// value is the documented default.
	LogPoll LogPollOptions

	// Webhooks holds the registered callbacks. Nil builds an empty registry,
	// so the management endpoints exist whether or not anything is wired.
	Webhooks *webhooks.Registry

	// Triggers holds the inbound run starters. `ork` wires the durable store
	// on the same shared backend the webhook registry uses, so a trigger
	// created on one replica is invocable on every other one. Nil builds the
	// in-process store, which is correct for one process and therefore for
	// unit tests only — exactly the terms Webhooks above is on.
	Triggers triggers.Store

	// Schedules is the schedule seam (schedule.go): engine.TemporalScheduler
	// in `ork`, so every schedule field is read from and written to the
	// cluster and nothing is stored here. Nil answers every ScheduleService
	// RPC with Unimplemented and skips DeleteOrkflow's schedule check — a
	// unit-test posture, never `ork`'s.
	Schedules ScheduleController

	// Deliveries is where settled webhook deliveries are recorded. `ork` wires
	// the durable store on the same shared backend the webhook registry uses,
	// so a delivery made through one replica is readable on every other one.
	// Nil answers ListDeliveries, GetDelivery and RedeliverWebhook with
	// FailedPrecondition rather than an empty listing — an operator who
	// configured no store must be told, not shown a subscriber that looks like
	// it was never called (unit-test convenience, never a production fallback).
	Deliveries artifacts.DeliveryStore
	// Redelivery sends RedeliverWebhook's one POST. Nil builds
	// webhooks.NewDispatcher(webhooks.DispatcherOptions{}): one
	// request, the default 10 s timeout, no ladder. Tests inject a client.
	Redelivery *webhooks.Dispatcher

	// AuditStore is where the control-plane audit trail lives: one immutable
	// record per call of a write RPC (audit.go), in the same bucket, through
	// the same ORK_ARTIFACT_BACKEND, as attempts and triggers — so any replica
	// lists a write another replica recorded. Nil records nothing and answers
	// ListAuditRecords with FailedPrecondition; that is a unit-test
	// convenience, never a production fallback: `ork` refuses to boot without
	// one.
	AuditStore artifacts.AuditStore
	// AuditCounter counts every audit write by outcome (written, dropped).
	// Nil counts nothing.
	AuditCounter AuditCounter

	// Notifier announces run state transitions this service observes itself.
	// The engine announces every transition of a run that reached Temporal;
	// this covers the one that never did, when StartRun fails.
	Notifier engine.RunNotifier

	// Observer is the telemetry seam's counterpart to Notifier: it is
	// announced the same synthesized RUN_FAILED event, so a run that never
	// reached Temporal is still counted and logged like any other. The
	// engine's own projection activity covers every run that did reach it.
	Observer engine.EventObserver

	// Admissions is announced every submission the admission gate refuses
	// (admission.go). It is Observer's counterpart for a submission that never
	// became a run: Observer is handed an Event and a Run, and a refusal has
	// neither, so it is its own seam rather than a synthesized RUN_FAILED.
	// Nil counts nothing; the refusal is returned to the caller either way.
	Admissions engine.AdmissionObserver

	// UsageCacheTTL is how long one whole usage measurement is reused, by
	// GetUsage and by the admission gate alike. Used verbatim: zero or
	// negative switches the answer cache off. `ork` fills it from
	// UsageCacheTTLFromEnv (ORK_USAGE_CACHE_TTL, default 5m); a unit test
	// that leaves it zero measures on every call.
	UsageCacheTTL time.Duration

	// Activities is the deployment's activity catalog, served by
	// ActivityService.ListActivities. Nil answers with an empty catalog:
	// which images exist is engine configuration (§4.2), and an
	// unconfigured engine genuinely has none.
	Activities ActivityLister

	// Orkers answers ActivityService.DescribeOrker — the pool-liveness probe
	// (§6.5).
	Orkers OrkerDescriber

	// Fleet answers ActivityService.ListOrkers and CordonOrker: the presence
	// channel's poller records and one Orker's own control channel
	// (docs/DEPLOY.md "Removing an Orker safely: cordon and drain"). Nil answers both Unavailable.
	// The same inventory is what a submit asks of a pool whose channel shows
	// no poller, and where its capability probes go; nil asks neither.
	Fleet engine.OrkerFleet

	// Releases answers SystemService's ListReleases, RampRelease and
	// PromoteRelease: the cluster's routing of new runs between releases, and
	// the one write that changes it (docs/DEPLOY.md §3). `ork` wires
	// engine.TemporalReleaseCluster over the client it already holds. Nil
	// answers all three Unavailable.
	Releases engine.ReleaseCluster

	// Capabilities answers the fleet check (docs/DEPLOY.md §5, "Engine
	// capabilities and the fleet check"): which engine processes that could
	// decide a run started now lack a capability that run needs. `ork` wires
	// the same engine.TemporalReleaseCluster as Releases. It is read only by
	// a submit that needs a capability — today a run tree declaring more
	// than engine.LegacyEachMaxItems fan-out items — and nil refuses such a
	// submit Unavailable: an unanswered question fails closed.
	Capabilities engine.CapabilityChecker

	// Catalog is the deployment's activity table (§4.2): the short names
	// `uses:` may spell. Nil selects dsl.BuiltinActivityCatalog.
	Catalog dsl.Catalog

	// Resolver performs apply-time image resolution (§4.3). Nil applies
	// WITHOUT pinning a digest, which is what an air-gapped engine and a
	// hermetic unit test both need; a configured engine always has one, and
	// cmd/ork builds it from ORK_REGISTRY_AUTH_FILE.
	Resolver ImageResolver

	// Namespaces is the registry: routing reads pool defaults and allow-lists
	// from it, and NamespaceService creates, updates and deletes through it.
	// Nil serves the built-in default namespace only and refuses every write,
	// which is what an unconfigured Service has to work with — inventing a
	// process-local registry would lose every namespace at the first restart
	// and answer differently on each replica.
	Namespaces engine.NamespaceRegistry

	// Pools answers whether a pool has live Orkers, so a submit onto a pool
	// nothing serves fails at submit instead of stranding a run in RUNNING.
	// Nil disables the check: there is no authority to ask, and inventing an
	// answer would either refuse every submit or vouch for every pool.
	Pools engine.PoolProbe

	// RawQueue is the operator escape hatch (ORK_ORKER_QUEUE): the execution
	// channel a task that names no pool is dispatched to. Empty routes the
	// default pool to its own channel, which is the zero-config behaviour.
	RawQueue string

	// Artifacts and Limits are deployment configuration pinned into RunPlan at
	// submit. Workflow code must not read either from the process environment.
	// Zero values select the documented local backend and contract limits.
	Artifacts artifacts.Config
	Limits    engine.WorkspaceLimits

	// EachMaxItems is ORK_EACH_MAX_ITEMS: the most items one Orkflow's
	// fan-outs (each:) may declare in total. Zero means a deployment default
	// of 1000 (engine.DefaultEachMaxItems), which a deployment lowers, or
	// raises up to the built-in 10000 (dsl.MaxEachItems) with continuation
	// on; cmd/ork refuses anything else at boot. Checked at validate/apply
	// and again at submit.
	EachMaxItems int32

	// OrkflowMaxTasks is ORK_ORKFLOW_MAX_TASKS (engine.orkflow_max_tasks):
	// the most tasks one Orkflow may declare. Zero means the default,
	// engine.DefaultOrkflowMaxTasks; a deployment may lower or raise it.
	// Checked at validate/apply/plan and again at submit.
	OrkflowMaxTasks int32

	// HistoryMaxEvents and HistoryMaxBytes are engine.history.max_events and
	// max_bytes: the cluster's per-execution history limits, which a run
	// stops RUN_HISTORY_LIMIT before reaching. Zero means the defaults,
	// engine.DefaultHistoryMaxEvents and DefaultHistoryMaxBytes. Pinned into
	// each top-level plan at submit (pinRunTreeLimits).
	HistoryMaxEvents int64
	HistoryMaxBytes  int64

	// ContinuationEnabled, ContinuationEvents, ContinuationBytes and
	// ContinuationRestoreTimeout are engine.continuation.*: whether a run
	// carries on into a fresh history past its budget, and the budget. Zero
	// numbers mean the engine defaults. Pinned into each top-level plan at
	// submit (pinRunTreeLimits), only when enabled.
	ContinuationEnabled        bool
	ContinuationEvents         int64
	ContinuationBytes          int64
	ContinuationRestoreTimeout time.Duration

	// RunMaxInFlight is engine.run_max_in_flight: the most activities, child
	// runs and pins one run keeps pending. Zero means
	// engine.DefaultRunMaxInFlight. Pinned into every top-level plan.
	RunMaxInFlight int32

	// PinCreationTimeout is engine.pin_creation_timeout: how long a pin waits
	// for a pin host of its pool to have a free slot before its first task
	// fails PIN_UNAVAILABLE. Zero (or negative) means
	// engine.DefaultPinCreationTimeout; the boot check in cmd/ork refuses a
	// value that is not whole seconds, and a smaller one here is raised to one
	// second. Pinned, in whole seconds, into every top-level plan.
	PinCreationTimeout time.Duration

	// DefinitionByReference is engine.definition_by_reference's effective
	// value: a top-level run whose planned definition is over a start
	// payload's 256 KiB travels by reference (definition_ref.go). It needs
	// ContinuationEnabled, which cmd/ork enforces at boot; without it this
	// is ignored. DefinitionObjects is where the definition objects are
	// written: the run snapshot store every reader resolves them from.
	DefinitionByReference bool
	DefinitionObjects     DefinitionObjectWriter

	// SnapshotOverlays is engine.snapshot_overlays: a run tree in which some
	// run declares more than engine.DefaultOverlayMinRows rows gets the
	// overlay policy on its plan (snapshot_overlay.go), while every engine
	// process that could decide it or project its events can read an
	// overlay; otherwise its runs write full snapshots, as before.
	SnapshotOverlays bool

	// ComposeMaxDepth is ORK_COMPOSE_MAX_DEPTH: how many levels of child runs
	// one run may start (a run is level 0). Zero means the built-in ceiling,
	// engine.DefaultComposeMaxDepth; it may lower that ceiling and never
	// raise it. Checked at validate/apply and again at submit.
	ComposeMaxDepth int32

	// CacheMaxTTL is ORK_CACHE_MAX_TTL: the oldest memoized result a cached
	// task may reuse, measured from when it was committed; it caps every
	// task's ttl_seconds and bounds a task that sets none. Zero means
	// engine.DefaultCacheMaxTTL. Read at submit onto RunPlan.Cache.
	CacheMaxTTL time.Duration
	// Memos is the memo store (docs/DSL.md "Caching"), in the same backend
	// as commits and attempts. Nil answers CacheService with
	// FailedPrecondition; it never affects a run.
	Memos artifacts.MemoStore

	// TransferBudgetSeconds overrides the derived data-transfer allowance.
	// When zero, submit derives it from Limits and
	// FloorThroughputBytesPerSecond (§6.3).
	TransferBudgetSeconds         int32
	FloorThroughputBytesPerSecond int64

	// System is what this replica was built and configured as, for
	// SystemService.GetSystemInfo. `ork` fills it at boot from the
	// configuration it already loaded; the zero value answers with empty
	// fields, which is what an unconfigured Service honestly knows about
	// itself.
	System SystemInfo

	// TelemetrySinks names every sink this engine's ORK_TELEMETRY_SINKS
	// declared — `ork` wires cfg.Sinks.Names() (internal/telemetry.SinkTable)
	// — for SystemService.ListTelemetrySinks and for validating a namespace's
	// telemetry.sink at write time (validNamespaceTelemetry). "default" is
	// always answered even when this is empty: an operator who configured no
	// table still has the OTEL_* environment as the default destination.
	TelemetrySinks []string

	// Router resolves an ork namespace to the cluster serving it. Nil builds
	// the one-binding router from Reader, Starter and Pools above, which is
	// every deployment there is today.
	Router engine.NamespaceRouter

	// Retention owns the deployment's maximum run retention: the ceiling on
	// every namespace's own run_retention (SystemService's
	// GetRetentionPolicy/UpdateRetentionPolicy), and it is what keeps the
	// underlying Temporal namespace's own retention TTL synced to that
	// ceiling plus an enforcement margin. `ork` builds it over the same
	// engine.TemporalRunReader wired as Reader above — one Temporal client,
	// never a second path to the cluster. Nil answers both RPCs with
	// Unimplemented.
	//
	// This is a deployment-wide ceiling, not a per-namespace value: each
	// orkstack.v1.Namespace carries its own run_retention (Namespaces below);
	// this is the bound on all of them, and — unlike that field — the word
	// "Temporal" never reaches a user through this seam (AGENTS.md §7).
	Retention *archive.RetentionController

	// RetentionEnforced reports whether this deployment runs the cleanup
	// worker that retires runs at their namespace's own retention
	// (ORK_CLEANUP_ENABLED). It only ever WIDENS what GetRetentionPolicy
	// says: when false, every run is kept for the maximum plus the
	// enforcement margin regardless of a tighter namespace value.
	RetentionEnforced bool

	// ImagePolicies is the deployment's image supply-chain floor store,
	// backing SystemService.GetImagePolicy/UpdateImagePolicy and read at
	// apply, validate and submit. Nil answers both RPCs with Unimplemented
	// and treats the floor as empty — the unit-test configuration, never a
	// fallback for a configured one.
	ImagePolicies artifacts.ImagePolicyStore

	// Signatures verifies cosign signatures for namespaces that require one.
	// `ork` builds it over the same Resolver wired above — one registry
	// client, one credential file. Nil refuses any selection that names a
	// signer with FailedPrecondition.
	Signatures *dsl.SignatureVerifier

	// Logger records the decisions a submission makes that an operator would
	// otherwise have to infer, such as an empty-pool override. Nil uses the
	// process default.
	Logger *slog.Logger

	// Now and NewRunID are injectable for deterministic tests.
	Now      func() time.Time
	NewRunID func() string
}

// Service implements OrkflowService, RunService, NamespaceService,
// ActivityService and SystemService.
type Service struct {
	orkflows engine.OrkflowStore
	// namespaces is the durable registry. Nil means no registry is wired:
	// reads answer for the built-in default only and writes are refused,
	// because a namespace this process invented would be gone at the next
	// restart and absent from every other replica.
	namespaces engine.NamespaceRegistry
	runs       *engine.RunStore
	bus        *engine.EventBus
	activities ActivityLister
	orkers     OrkerDescriber
	// fleet is the Orker inventory and cordon seam. It holds no Orker list:
	// every answer is read from the cluster and from the Orker itself.
	fleet engine.OrkerFleet
	// inventory is the Orker inventory InspectRun's capacity answer reads,
	// shared by every caller on this replica (capacity_inventory.go); nil
	// without a fleet.
	inventory *sharedInventory
	// releases is the release routing seam (Config.Releases).
	releases engine.ReleaseCluster
	// capabilities is the fleet check (Config.Capabilities).
	capabilities engine.CapabilityChecker
	// catalog and resolver are the two halves of §4.1: the catalog turns a
	// short name into a reference, and the resolver turns a reference into a
	// digest. Both live here because ApplyOrkflow is where §4.3 puts them.
	catalog  dsl.Catalog
	resolver ImageResolver
	// pinnedActivities answers InspectActivity for a digest-pinned reference
	// this process has already read (activity_inspect_cache.go).
	pinnedActivities *pinnedActivityCache
	// router resolves which cluster answers for a namespace or a run. It
	// holds one binding today; every read and write path goes through it, so
	// partitioning namespaces across clusters later is configuration rather
	// than a change to this file.
	router engine.NamespaceRouter
	// pools is the routing decision: pool resolution, allow-list enforcement
	// and the liveness check, all of which happen at submit.
	pools    *engine.PoolResolver
	rawQueue string
	archive  RunArchive
	// logs is the durable task-log store, read by StreamTaskLogs. It is the
	// same bucket the Orker wrote into and belongs to no process.
	logs          artifacts.LogStore
	artifactStore artifacts.Store
	// attempts is the durable per-attempt record store, read by
	// ListTaskAttempts. Written by Orkers, owned by no process.
	attempts artifacts.AttemptStore
	// commits is the completion-record store, read only by
	// ExportRunEvidence and only at the uri a run snapshot recorded.
	commits     artifacts.CommitStore
	logPoll     LogPollOptions
	idempotency IdempotencyClaimer
	webhooks    *webhooks.Registry
	triggers    triggers.Store
	// schedules is the cluster's schedule seam; nil answers Unimplemented.
	schedules ScheduleController
	// deliveries is the webhook delivery record store, read by ListDeliveries
	// and GetDelivery; deliveryRecorder writes RedeliverWebhook's record into
	// it and redelivery sends that one POST. Nil deliveries refuses all three.
	deliveries       artifacts.DeliveryStore
	deliveryRecorder *DeliveryRecorder
	redelivery       *webhooks.Dispatcher
	// audit writes the control-plane audit trail and auditStore reads it
	// back. Both are nil when no store is wired: nothing is recorded and
	// ListAuditRecords is refused.
	audit      *AuditRecorder
	auditStore artifacts.AuditStore
	notifier   engine.RunNotifier
	observer   engine.EventObserver
	admissions engine.AdmissionObserver
	// usage measures a namespace's reserved consumption for GetUsage and for
	// an enforced budget's admission check — one measurement both share
	// (usage.go). It is a measuring component, not a map of answers: its
	// cache is unexported inside it and reachable only through Measure.
	usage *UsageMeter
	// poolScope memoizes each namespace's pools-named-by-orkflows answer
	// (PoolScopeCacheTTL); ApplyOrkflow and DeleteOrkflow invalidate it.
	poolScope *poolScopeCache
	// runReads shares one run read between the quiet log followers of a run
	// (taskIsTerminal) for taskTerminalMemoTTL.
	runReads *runReadMemo
	// system is this replica's own build and configuration, echoed by
	// GetSystemInfo. Injected once, never read from the environment here.
	system SystemInfo
	// telemetrySinks names every sink this deployment declared (see
	// Config.TelemetrySinks). Read by ListTelemetrySinks and by
	// validNamespaceTelemetry; never by anything on the run-submission path,
	// which only ever selects a name — the endpoint and headers stay inside
	// internal/telemetry.
	telemetrySinks []string
	artifacts      artifacts.Config
	limits         engine.WorkspaceLimits
	// eachMaxItems is Config.EachMaxItems, defaulted.
	eachMaxItems int32
	// orkflowMaxTasks is Config.OrkflowMaxTasks, defaulted.
	orkflowMaxTasks int32
	// historyMaxEvents and historyMaxBytes are Config.HistoryMaxEvents and
	// HistoryMaxBytes, defaulted.
	historyMaxEvents, historyMaxBytes int64
	// continuation is the budget pinRunTreeLimits pins, nil when
	// continuation is off; runMaxInFlight is Config.RunMaxInFlight,
	// defaulted.
	continuation   *engine.ContinuationBudget
	runMaxInFlight int32
	// pinCreationSeconds is Config.PinCreationTimeout in whole
	// seconds, defaulted: what pinRunTreeLimits pins.
	pinCreationSeconds int32
	// definitionByReference is Config.DefinitionByReference, only with
	// continuation on; definitionObjects is Config.DefinitionObjects.
	definitionByReference bool
	definitionObjects     DefinitionObjectWriter
	// snapshotOverlays is Config.SnapshotOverlays; overlaysOff rate-limits
	// the Warn logged while the fleet keeps a run tree off overlays.
	snapshotOverlays bool
	overlaysOff      warnLimiter
	// composeMaxDepth is Config.ComposeMaxDepth, defaulted.
	composeMaxDepth int
	// cacheMaxTTL is Config.CacheMaxTTL, defaulted; memos is Config.Memos.
	cacheMaxTTL time.Duration
	memos       artifacts.MemoStore
	// retention owns the deployment's maximum run retention (see
	// Config.Retention). Nil answers SystemService's retention RPCs with
	// Unimplemented.
	retention         *archive.RetentionController
	retentionEnforced bool
	// imagePolicies is the deployment's image supply-chain floor (see
	// Config.ImagePolicies); signatures verifies a namespace's signer.
	imagePolicies   artifacts.ImagePolicyStore
	signatures      *dsl.SignatureVerifier
	transferBudget  int32
	floorThroughput int64
	logger          *slog.Logger
	now             func() time.Time
	newRunID        func() string

	// starts stands in for the cluster's first-start read when no reader
	// is wired (a unit test); retentionRead caches the cluster's reported
	// retention; onLegacyPending counts a legacy key answered PENDING.
	starts          processStarts
	retentionRead   retentionReading
	onLegacyPending func(context.Context)
}

// NewService builds a Service, filling in defaults for anything omitted.
func NewService(cfg Config) *Service {
	svc := &Service{
		orkflows:              cfg.Orkflows,
		namespaces:            cfg.Namespaces,
		rawQueue:              cfg.RawQueue,
		runs:                  cfg.Runs,
		bus:                   cfg.Bus,
		activities:            cfg.Activities,
		orkers:                cfg.Orkers,
		fleet:                 cfg.Fleet,
		releases:              cfg.Releases,
		capabilities:          cfg.Capabilities,
		catalog:               cfg.Catalog,
		resolver:              cfg.Resolver,
		pinnedActivities:      newPinnedActivityCache(),
		system:                cfg.System,
		telemetrySinks:        cfg.TelemetrySinks,
		router:                cfg.Router,
		archive:               cfg.Archive,
		logs:                  cfg.Logs,
		artifactStore:         cfg.ArtifactStore,
		attempts:              cfg.Attempts,
		commits:               cfg.Commits,
		logPoll:               cfg.LogPoll,
		idempotency:           cfg.Idempotency,
		onLegacyPending:       cfg.OnLegacyPending,
		webhooks:              cfg.Webhooks,
		triggers:              cfg.Triggers,
		schedules:             cfg.Schedules,
		deliveries:            cfg.Deliveries,
		redelivery:            cfg.Redelivery,
		notifier:              cfg.Notifier,
		observer:              cfg.Observer,
		admissions:            cfg.Admissions,
		artifacts:             cfg.Artifacts,
		limits:                cfg.Limits,
		eachMaxItems:          eachMaxItemsOf(cfg.EachMaxItems),
		orkflowMaxTasks:       orkflowMaxTasksOf(cfg.OrkflowMaxTasks),
		historyMaxEvents:      positiveOr(cfg.HistoryMaxEvents, engine.DefaultHistoryMaxEvents),
		continuation:          continuationBudgetFor(cfg),
		definitionByReference: cfg.DefinitionByReference && cfg.ContinuationEnabled,
		snapshotOverlays:      cfg.SnapshotOverlays,
		definitionObjects:     cfg.DefinitionObjects,
		runMaxInFlight:        int32(positiveOr(int64(cfg.RunMaxInFlight), int64(engine.DefaultRunMaxInFlight))),
		pinCreationSeconds:    pinCreationTimeoutSecondsOf(cfg.PinCreationTimeout),
		historyMaxBytes:       positiveOr(cfg.HistoryMaxBytes, engine.DefaultHistoryMaxBytes),
		composeMaxDepth:       composeMaxDepthOf(cfg.ComposeMaxDepth),
		cacheMaxTTL:           cacheMaxTTLOf(cfg.CacheMaxTTL),
		memos:                 cfg.Memos,
		retention:             cfg.Retention,
		retentionEnforced:     cfg.RetentionEnforced,
		imagePolicies:         cfg.ImagePolicies,
		signatures:            cfg.Signatures,
		transferBudget:        cfg.TransferBudgetSeconds,
		floorThroughput:       cfg.FloorThroughputBytesPerSecond,
		logger:                cfg.Logger,
		now:                   cfg.Now,
		newRunID:              cfg.NewRunID,
	}
	if svc.router == nil {
		// One binding: this cluster, every namespace. Reader, Starter and
		// Pools are the flat form of exactly that, kept because almost every
		// caller has one cluster and nothing to route between.
		svc.router = engine.NewSingleRouter(engine.Binding{
			Reader:  cfg.Reader,
			Starter: cfg.Starter,
			Pools:   cfg.Pools,
		})
	}
	if svc.logger == nil {
		svc.logger = slog.Default()
	}
	svc.pools = &engine.PoolResolver{
		Namespaces: cfg.Namespaces,
		// The inventory ListOrkers lists is the one submit's pool checks
		// ask and ListPools counts (engine.ReadFleet).
		Fleet:    cfg.Fleet,
		RawQueue: cfg.RawQueue,
		Logger:   svc.logger,
		// The same clock the rest of the Service stamps with. Pool liveness
		// is judged against it — an observation is live only if it is recent
		// — so a test that fixes the clock and states an observation at that
		// instant gets a live pool, instead of one that went stale between
		// the fixture and time.Now(). Nil is time.Now, which is production.
		Now: cfg.Now,
	}
	// Only an unconfigured Service gets the in-memory store, and only so a
	// unit test can exercise runs without a bucket. A configured store is
	// never bypassed: a failing durable store surfaces as an error.
	if svc.orkflows == nil {
		svc.orkflows = engine.NewMemoryOrkflowStore()
	}
	if svc.runs == nil {
		svc.runs = engine.NewRunStore()
	}
	if svc.bus == nil {
		svc.bus = engine.NewEventBus()
	}
	if svc.idempotency == nil {
		// The process-local claimer, which is only ever correct because an
		// unconfigured Service is the only process there is. `ork` always
		// wires the Temporal claimer, so this line is unreachable from it.
		svc.idempotency = newInMemoryClaimer()
	}
	if svc.webhooks == nil {
		svc.webhooks = webhooks.NewRegistry()
	}
	if svc.triggers == nil {
		svc.triggers = triggers.NewMemoryStore()
	}
	if svc.now == nil {
		svc.now = time.Now
	}
	svc.usage = newUsageMeter(cfg.Archive, cfg.Attempts, cfg.UsageCacheTTL, svc.now)
	svc.poolScope = newPoolScopeCache(PoolScopeCacheTTL, svc.now)
	if svc.fleet != nil {
		svc.inventory = newSharedInventory(svc.fleet, svc.now)
	}
	svc.runReads = newRunReadMemo(taskTerminalMemoTTL)
	if svc.deliveries != nil {
		svc.deliveryRecorder = NewDeliveryRecorder(svc.deliveries, svc.logger)
	}
	if svc.redelivery == nil {
		// One request, no ladder: the operator is the retry policy.
		svc.redelivery = webhooks.NewDispatcher(webhooks.DispatcherOptions{})
	}
	if cfg.AuditStore != nil {
		svc.auditStore = cfg.AuditStore
		svc.audit = NewAuditRecorder(cfg.AuditStore, cfg.AuditCounter, svc.logger, svc.now)
	}
	if svc.newRunID == nil {
		// One generator serves the whole process, so its nonce covers every
		// Service wired inside it.
		svc.newRunID = engine.NewRunID
	}
	return svc
}

// binding resolves which cluster serves one namespace.
func (s *Service) binding(namespace string) (engine.Binding, error) {
	binding, err := s.router.For(namespace)
	if err != nil {
		return engine.Binding{}, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return binding, nil
}

// locate resolves which cluster owns a run id, for the reads that carry no
// namespace. With one binding it is total and free — see engine.SingleRouter.
func (s *Service) locate(ctx context.Context, runID string) (engine.Binding, error) {
	binding, err := s.router.Locate(ctx, runID)
	if err != nil {
		return engine.Binding{}, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("run %q: %w", runID, err))
	}
	return binding, nil
}

// resolverFor is the pool resolver bound to one cluster. The routing rules are
// the service's and identical everywhere; the liveness probe belongs to the
// cluster that would run the work, so it comes from the binding.
func (s *Service) resolverFor(binding engine.Binding) *engine.PoolResolver {
	resolver := *s.pools
	resolver.Probe = binding.Pools
	return &resolver
}

// Bus exposes the event bus. It is the write side of the read model: the
// projection activity fills it, and it answers a read only when no RunReader
// is wired.
func (s *Service) Bus() *engine.EventBus { return s.bus }

// Runs exposes the in-process run store, on the same terms as Bus.
func (s *Service) Runs() *engine.RunStore { return s.runs }

// Webhooks exposes the callback registry (used by the management endpoints).
func (s *Service) Webhooks() *webhooks.Registry { return s.webhooks }

// Triggers exposes the inbound run-starter registry, on the same terms: the
// management RPCs and the plain-HTTP invoke adapter both read it, and it is
// the shared store rather than anything this process remembers.
func (s *Service) Triggers() triggers.Store { return s.triggers }

// ApplyOrkflow compiles a DSL document and writes the resulting orkflow to the
// durable store, so it outlives this process.
//
// A document is applied into exactly one namespace, and there are two ways to
// say which: the document's own `namespace:`, and the request's selector
// (`orkctl -n`). An empty selector honours the document; a selector over a
// document that names none adopts it, which is how one portable file is
// applied into staging and into production. The two disagreeing is refused —
// see applyNamespace.
//
// It never rewrites a stored version. Re-applying the same document writes
// nothing and answers with what is stored (created=false); a DIFFERENT
// document under a stored version is AlreadyExists, because a run pinned to
// that version reads its bytes long after the apply. An edit therefore always
// arrives as a new version — one is minted here when the document pins none and
// its content has none yet (assignVersion).
func (s *Service) ApplyOrkflow(
	ctx context.Context,
	req *connect.Request[orkstackv1.ApplyOrkflowRequest],
) (*connect.Response[orkstackv1.ApplyOrkflowResponse], error) {
	source := []byte(req.Msg.GetYaml())
	// issues is WARNING only here: an interface ERROR is already the
	// InvalidArgument returned above, before any token read or write.
	wf, ns, issues, err := s.prepareOrkflow(ctx, source, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	expected, err := s.checkEditTokens(ctx, ns.GetName(), wf.GetName(),
		req.Msg.GetBaseVersion(), req.Msg.GetExpectedHeadVersion())
	if err != nil {
		return nil, err
	}
	// The one write an apply makes before its version lands, and it comes after
	// every refusal a read can make. A claim left by a Put that then fails is
	// inert: the next apply of this content stores the version it names.
	if wf.GetVersion() == "" {
		if err := s.assignVersion(ctx, source, ns.GetName(), wf); err != nil {
			return nil, err
		}
	}
	// The interface comparison must see the store as it was BEFORE this
	// version lands, or an unpromoted orkflow's new head would be compared
	// with itself. WARNINGs only: applying is never refused for a consumer's
	// sake (docs/UI_CLI_PARITY.md "Interface compatibility").
	issues = append(issues, s.interfaceChangeIssues(ctx, ns.GetName(), wf)...)
	// The stamp is applied here, but it does NOT decide whether this apply is a
	// repeat: the store compares everything except created_at and hands back
	// the stored document untouched when they match, so a re-apply cannot
	// churn the object merely by arriving at a different time. Stamping first
	// and comparing bytes afterwards is what made every apply a rewrite.
	//
	// Nor does it decide which version is the head. This clock is one
	// replica's, and the head is a fact about the deployment: the store owns
	// it, and holds the stamp above it (artifacts.AppliedStamp) so a slow
	// replica cannot publish a version that reads as older than the one it
	// just extended.
	wf.CreatedAt = timestamppb.New(s.now().UTC())
	stored, created, err := s.orkflows.Put(ctx, wf, expected)
	// Whatever the outcome — a failed write may still have landed — the
	// namespace's memoized pool scope may now be stale.
	s.poolScope.invalidate(ns.GetName())
	if err != nil {
		var moved *artifacts.HeadMovedError
		if errors.As(err, &moved) {
			// The edit lost the commit, not the check: a competing apply
			// landed between the read above and the write. Same refusal, in
			// the same words — the caller cannot tell the two apart, and
			// should not have to. What the store did with the version it
			// wrote (left it, removed it, put it back) rides along, and the
			// store's whole error is logged.
			s.logger.Warn("apply refused: the head moved while it was written",
				"namespace", ns.GetName(), "orkflow", wf.GetName(), "version", wf.GetVersion(), "error", err)
			return nil, withStoreNote(editRaceRefusal(ns.GetName(), wf.GetName(),
				req.Msg.GetBaseVersion(), req.Msg.GetExpectedHeadVersion(), moved.Head), storeNote(err, moved))
		}
		return nil, orkflowStoreError(err, "store orkflow %s@%s in namespace %q",
			wf.GetName(), wf.GetVersion(), ns.GetName())
	}
	resp := &orkstackv1.ApplyOrkflowResponse{
		Orkflow: stored,
		Created: created,
	}
	for _, issue := range issues { // WARNING only: an ERROR already returned
		resp.Warnings = append(resp.Warnings, issue.GetField()+": "+issue.GetMessage())
	}
	return connect.NewResponse(resp), nil
}

// assignVersion gives a document that pins none and whose content has no version
// yet the version it lands under, and the digest that covers it.
//
// The candidate is minted here because this is the one path that stores anything:
// `orkctl orkflow validate` and a plan report no version for such a document
// rather than one that would not be the one applied. The store decides whether
// the candidate is used, and a concurrent apply of the same content, on any
// replica, gets the same answer (artifacts.S3OrkflowStore.ClaimDerivedVersion).
func (s *Service) assignVersion(ctx context.Context, source []byte, namespace string, wf *orkstackv1.Orkflow) error {
	contentHash, err := dsl.ContentHash(source, wf)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	version, err := s.orkflows.ClaimDerivedVersion(ctx, namespace, wf.GetName(), contentHash, typeid.New())
	if err != nil {
		return orkflowStoreError(err, "assign a version to orkflow %q in namespace %q", wf.GetName(), namespace)
	}
	wf.Version = version
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	wf.DefinitionDigest = digest
	return nil
}

// ValidateOrkflow performs exactly ApplyOrkflow's preparation without reading
// edit tokens or writing a definition. A document error is a returned issue;
// unavailable registries and namespace stores remain RPC failures.
//
// The orkflow it returns names the version the document would land on when its
// content already has one. When it has none, version and definition_digest are
// empty: the version is minted by the apply that stores it.
func (s *Service) ValidateOrkflow(ctx context.Context, req *connect.Request[orkstackv1.ValidateOrkflowRequest]) (*connect.Response[orkstackv1.ValidateOrkflowResponse], error) {
	wf, ns, issues, err := s.prepareOrkflow(ctx, []byte(req.Msg.GetYaml()), req.Msg.GetNamespace())
	if err == nil {
		// The preparation's warnings first, then the declared-interface
		// change against what runs now, then the pin advice, as one list.
		issues = append(issues, s.interfaceChangeIssues(ctx, ns.GetName(), wf)...)
		return connect.NewResponse(&orkstackv1.ValidateOrkflowResponse{
			Orkflow: wf,
			Issues:  append(issues, pinUserParityWarnings(wf)...),
		}), nil
	}
	// A with: block that fails an image's declared interface is reported
	// whole — every ERROR and WARNING at once — not as its first line.
	var refusal *interfaceRefusal
	if errors.As(err, &refusal) {
		return connect.NewResponse(&orkstackv1.ValidateOrkflowResponse{Issues: refusal.Issues}), nil
	}
	if connect.CodeOf(err) == connect.CodeInvalidArgument {
		field := ""
		var validation *dsl.ValidationError
		if errors.As(err, &validation) {
			field = validation.Field
		}
		var requestField *validationFieldError
		if errors.As(err, &requestField) {
			field = requestField.Field
		}
		// The issue's message is the refusal's own sentence. The Connect code
		// is already said by the severity, and a "invalid_argument: " prefix
		// in front of a field-anchored issue only pushes the sentence right.
		message := err.Error()
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			message = connectErr.Message()
		}
		return connect.NewResponse(&orkstackv1.ValidateOrkflowResponse{Issues: []*orkstackv1.ValidationIssue{{Field: field, Message: message, Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR}}}), nil
	}
	return nil, err
}

// pinUserParityWarnings reports every pin whose tasks all leave
// run_as_orker_user false, as a WARNING and never as an error.
//
// It is what is LEFT of rule 11 after the compiler has done its half. Rule 11
// refuses a pin whose tasks DISAGREE about the flag, because
// /orkstack/shared is 1777 with the sticky bit and two members running as
// different uids can each create files the other cannot delete or overwrite.
// Agreeing on false does not remove that hazard, it only hides it: with the
// flag false each container keeps its image's own baked-in USER, so a pin of
// three images can still run as three different uids and hit the same
// sticky-bit failure on its second task. The platform cannot know an image's
// USER at compile time — that is the registry's answer, and only after a pull
// — so this is advice and not a refusal (docs/DSL.md "Pins": a documented
// author responsibility, not a platform guarantee).
//
// A document with no pin produces nothing, so an unpinned author's validate
// response is exactly what it was before pins existed.
func pinUserParityWarnings(wf *orkstackv1.Orkflow) []*orkstackv1.ValidationIssue {
	if len(wf.GetPins()) == 0 {
		return nil
	}
	// Counted per pin, so the condition reads as what it is — every member
	// leaves the flag false, and there is at least one member. Rule 11 has
	// already made "every member" and "the first member" the same answer, and
	// this does not need to depend on that.
	type tally struct{ members, orkerUser int }
	counts := make(map[string]*tally, len(wf.GetPins()))
	for _, pin := range wf.GetPins() {
		counts[pin.GetName()] = &tally{}
	}
	for _, task := range wf.GetTasks() {
		count, declared := counts[task.GetPin()]
		if !declared {
			continue
		}
		count.members++
		if task.GetRunAsOrkerUser() {
			count.orkerUser++
		}
	}
	var issues []*orkstackv1.ValidationIssue
	// wf.Pins is sorted by name at compile, so the issues come out in one
	// order for one document.
	for _, pin := range wf.GetPins() {
		count := counts[pin.GetName()]
		if count.members == 0 || count.orkerUser > 0 {
			continue
		}
		issues = append(issues, &orkstackv1.ValidationIssue{
			Field: fmt.Sprintf("pins.%s", pin.GetName()),
			Message: fmt.Sprintf("pin %q: every task leaves run_as_orker_user false, so each "+
				"container keeps its image's own USER — two members with different baked-in "+
				"users can each write files under the sticky-bit /orkstack/shared that the "+
				"other cannot replace. Set run_as_orker_user: true across the pin.",
				pin.GetName()),
			Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING,
		})
	}
	return issues
}

type validationFieldError struct {
	Field string
	Err   error
}

func (e *validationFieldError) Error() string { return e.Err.Error() }
func (e *validationFieldError) Unwrap() error { return e.Err }

// interfaceRefusal is a document whose with: blocks fail the interface an
// image declares. It carries EVERY issue, errors and warnings, so
// ValidateOrkflow can return all of them at once instead of the first,
// and ApplyOrkflow's message names the first error and the count.
type interfaceRefusal struct{ Issues []*orkstackv1.ValidationIssue }

func (e *interfaceRefusal) Error() string {
	first := e.Issues[0]
	for _, issue := range e.Issues {
		if issue.GetSeverity() == orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
			first = issue
			break
		}
	}
	message := first.GetField() + ": " + first.GetMessage()
	if len(e.Issues) > 1 {
		message += fmt.Sprintf(" (and %d more issues; run orkctl orkflow validate to see them all)", len(e.Issues)-1)
	}
	return message
}

// hasError reports whether any issue is an ERROR.
func hasError(issues []*orkstackv1.ValidationIssue) bool {
	for _, issue := range issues {
		if issue.GetSeverity() == orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
			return true
		}
	}
	return false
}

// prepareOrkflow is the common apply/validate pipeline. Keep all mutation
// after this function so validation cannot create a pointer or a definition.
func (s *Service) prepareOrkflow(ctx context.Context, source []byte, requestedNamespace string) (*orkstackv1.Orkflow, *orkstackv1.Namespace, []*orkstackv1.ValidationIssue, error) {
	wf, err := dsl.CompileWithOptions(source, dsl.Options{Catalog: s.activityCatalog()})
	if err != nil {
		// A composed task with no version names the versions that exist
		// in the namespace; the compiler is pure and cannot.
		return nil, nil, nil, connect.NewError(connect.CodeInvalidArgument,
			s.enrichVersionRequired(ctx, requestedNamespace, err))
	}
	pinsVersion, err := dsl.DocumentPinsVersion(source)
	if err != nil {
		return nil, nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	// Compile's provisional hash ends here: no version is ever stored under it,
	// so no refusal below may name it. The document's version is decided after
	// the images are resolved.
	if !pinsVersion {
		wf.Version = ""
	}
	// The deployment's task ceiling (ORK_ORKFLOW_MAX_TASKS), before anything
	// is looked up or resolved: a document over it is refused without one
	// registry request per task.
	if err := s.checkTaskCeiling(wf); err != nil {
		return nil, nil, nil, err
	}
	selected, err := applyNamespace(requestedNamespace, wf.GetNamespace())
	if err != nil {
		return nil, nil, nil, err
	}
	ns, err := s.namespace(ctx, selected)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := s.pools.CheckAllowedPools(ns, wf); err != nil {
		return nil, nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	// The namespace's secret policy, with the file in hand: anchored to the
	// exact tasks[i].secrets[j].source so validate reports it as a field
	// issue. Plan repeats it at submit, the gate no client can bypass.
	if err := engine.CheckAllowedSecrets(ns, wf); err != nil {
		var refused *engine.SecretNotAllowedError
		if errors.As(err, &refused) {
			return nil, nil, nil, connect.NewError(connect.CodeInvalidArgument,
				&validationFieldError{Field: refused.Field(), Err: err})
		}
		return nil, nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	wf.Namespace = ns.GetName()
	resolutions, err := s.resolveImages(ctx, wf)
	if err != nil {
		return nil, nil, nil, applyResolveError(err)
	}
	// The deployment's fan-out bounds: ORK_EACH_MAX_ITEMS, and each fan-out's
	// own share of every item's input — measured
	// after resolution, so the image every item carries is counted.
	if err := s.checkEachLimits(wf); err != nil {
		return nil, nil, nil, err
	}
	// The image policy, with the file in hand and every image resolved:
	// anchored to the exact tasks[i].uses so validate reports it as a field
	// issue. It runs before the interface check: a forbidden image's
	// interface is never consulted. prepare repeats it at submit, which
	// catches a definition stored before the policy tightened.
	sel, err := s.imagePolicySelection(ctx, ns)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := s.checkImagePolicy(ctx, ns, sel, wf); err != nil {
		return nil, nil, nil, err
	}
	// Rule 14: every task's with: against the interface its resolved image
	// declares. After resolution, because the interface arrives from the
	// registry and Compile stays pure. An ERROR refuses before anything is
	// written; WARNINGs travel with the prepared document.
	issues := dsl.CheckOrkflowInterfaces(wf, resolutions)
	// Rules 31 and 32: every composed task resolved in this namespace, its
	// definition digest stamped (before ContentHash and DefinitionDigest,
	// which both hash it), its tree loaded and checked, and its interface
	// honoured. One refusal carries the interface and composition issues
	// together, in that order.
	composeIssues, err := s.resolveComposition(ctx, ns, wf)
	if err != nil {
		return nil, nil, nil, err // a store outage: Unavailable, never an issue
	}
	issues = append(issues, composeIssues...)
	// Caching's apply warnings: never an ERROR, so an apply is never refused
	// on them (docs/DSL.md "Caching").
	issues = append(issues, cacheIssues(wf, s.cacheMaxTTL)...)
	// Join policies' apply warning (W1): an unguarded read of an ancestor a
	// join can leave absent. Never an ERROR; rules 37–42 already refused in
	// Compile.
	issues = append(issues, dsl.JoinWarnings(wf)...)
	if hasError(issues) {
		return nil, nil, issues, connect.NewError(connect.CodeInvalidArgument, &interfaceRefusal{Issues: issues})
	}
	// A document that pins no version names the one its content was given, if it
	// was given one. Otherwise it has none yet, and only ApplyOrkflow mints: this
	// function is validation's too, and validation writes nothing.
	if !pinsVersion {
		contentHash, err := dsl.ContentHash(source, wf)
		if err != nil {
			return nil, nil, nil, connect.NewError(connect.CodeInternal, err)
		}
		known, err := s.orkflows.FindDerivedVersion(ctx, ns.GetName(), wf.GetName(), contentHash)
		if err != nil {
			return nil, nil, nil, orkflowStoreError(err, "find the version of orkflow %q in namespace %q",
				wf.GetName(), ns.GetName())
		}
		wf.Version = known
	}
	// The size a run would carry is measured with a version and a digest in
	// place even when the document has neither yet, so validation refuses what
	// apply would.
	unassigned := wf.GetVersion() == ""
	if unassigned {
		wf.Version = typeid.New()
	}
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		return nil, nil, nil, connect.NewError(connect.CodeInternal, err)
	}
	wf.DefinitionDigest = digest
	// A definition too large for a start payload must fit what a run may
	// carry by reference, when the deployment enables it.
	if err := s.checkDefinitionCarriable(wf); err != nil {
		return nil, nil, nil, err
	}
	if unassigned {
		wf.Version, wf.DefinitionDigest = "", ""
	}
	return wf, ns, issues, nil
}

// checkEditTokens reads the two things an edit says about itself — which
// stored version it was DERIVED from (base) and which head the editor SAW when
// it fetched (expectedHead) — and returns the head condition the write is made
// under.
//
// They are two facts, so they are two tokens. Any stored version is a valid
// base — editing an old one is how a user forks forward from it, and the fork
// lands as the new head with the `current` pointer untouched — while the head
// the editor saw is the only thing a competing apply can invalidate. Under one
// token the server could not tell an edit derived from an old version on
// purpose from one derived from a head that has since moved, and would have
// to refuse the first to catch the second.
//
//	base    expected  condition
//	----    --------  ---------
//	""      ""        none — a plain `orkctl orkflow create file.yaml`
//	any     set       head must be expected; base, when set, must name a
//	                  stored version (NotFound if not)
//	set     ""        InvalidArgument naming both fields
//
// The last row is refused rather than guessed at. A base is provenance, not a
// head claim: reading it as one refuses every deliberate fork from an old
// version, and reading it as nothing stores an edit whose author plainly
// fetched a head that nobody then checked. Every editor (`orkctl orkflow
// edit`, the UI's editor) sends both tokens, so a base arriving alone is a
// client bug and is answered as one.
//
// What this function does NOT do is decide the race. It reads the store to
// answer the questions a read can answer — is there anything stored at all,
// does the base exist, what should the refusal say — and hands the condition
// to the store, which applies it in the same operation that writes. A check
// here followed by a write there is a window, and the loser of that window
// would succeed: it checks head=2.0.0, a competing 3.0.0 lands, and its own
// version is stored anyway with nothing refused.
func (s *Service) checkEditTokens(
	ctx context.Context,
	namespace, name, base, expectedHead string,
) (artifacts.HeadExpectation, error) {
	if expectedHead == "" {
		if base == "" {
			return artifacts.NoHeadExpectation(), nil
		}
		return artifacts.NoHeadExpectation(), connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"base_version %q was sent without expected_head_version: base_version says "+
				"which version the edit was derived from, and only expected_head_version "+
				"says which head it was fetched under; send both",
			base))
	}
	expected := artifacts.ExpectHead(expectedHead)

	versions, err := s.orkflows.Versions(ctx, namespace, name)
	if err != nil {
		return expected, orkflowStoreError(err,
			"read versions of orkflow %q in namespace %q", name, namespace)
	}
	if versions.Head == "" {
		// Nothing is stored, so the head the editor claims to have seen
		// cannot be true of anything. Say so rather than inventing a version
		// to name.
		return expected, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"this edit expected version %q to be the newest applied one of orkflow %q, "+
				"but that orkflow has no stored version in namespace %q",
			expectedHead, name, namespace))
	}
	// Provenance is a claim about the store, so it is checked against the
	// store: a base nobody applied is a typo, answered where it was made
	// rather than recorded as the ancestry of a version that never existed.
	if base != "" && !storesVersion(versions, base) {
		return expected, connect.NewError(connect.CodeNotFound, fmt.Errorf(
			"this edit says it was derived from version %q of orkflow %q, "+
				"but no such version is stored in namespace %q",
			base, name, namespace))
	}
	if versions.Head != expectedHead {
		return expected, editRaceRefusal(namespace, name, base, expectedHead, versions.Head)
	}
	return expected, nil
}

// editRaceRefusal is the one refusal an edit gets when the head is not the one
// it was written against, wherever that was discovered — reading the store
// before the write, or losing the store's own compare-and-set.
//
// It says three things — where the edit came from, the head it was fetched
// at, and the head that is there now — because the base is not the problem
// and must not read as one.
func editRaceRefusal(namespace, name, base, expectedHead, head string) error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"orkflow %q in namespace %q changed while this edit was open: %s"+
			"the newest applied version was %q when the document was fetched "+
			"and is now %q; run `orkctl orkflow diff %s %s %s` and re-apply",
		name, namespace, derivedFrom(base), expectedHead, head,
		name, expectedHead, head))
}

// storeNote is what a store added to the refusal it returned: the text after
// the refusal's own, such as what a rollback did with the version the apply
// wrote. "" when it added nothing.
func storeNote(err, refusal error) string {
	return strings.TrimSpace(strings.TrimPrefix(err.Error(), refusal.Error()))
}

// withStoreNote appends a store's note to a refusal, keeping its code.
func withStoreNote(refusal error, note string) error {
	var connectErr *connect.Error
	if note == "" || !errors.As(refusal, &connectErr) {
		return refusal
	}
	return connect.NewError(connectErr.Code(), fmt.Errorf("%s %s", connectErr.Message(), note))
}

// storesVersion reports whether one version is among the stored ones.
func storesVersion(versions artifacts.OrkflowVersions, version string) bool {
	for _, wf := range versions.All {
		if wf.GetVersion() == version {
			return true
		}
	}
	return false
}

// storedVersion returns the stored document of one version, or nil.
func storedVersion(versions artifacts.OrkflowVersions, version string) *orkstackv1.Orkflow {
	for _, wf := range versions.All {
		if wf.GetVersion() == version {
			return wf
		}
	}
	return nil
}

// derivedFrom names the edit's base in a refusal, so a user who deliberately
// forked from an old version is not told their base is the problem. It is
// silent when there is no base to name.
func derivedFrom(base string) string {
	if base == "" {
		return ""
	}
	return fmt.Sprintf("it was derived from version %q, and ", base)
}

// applyNamespace reconciles the two ways a request names a namespace.
//
// A selector that contradicts the document is a mistake one of the two made,
// and there is no safe guess: taking the flag stores the definition where its
// author will not look for it, and taking the file ignores what the operator
// typed. Refusing names both, which is the only answer that tells the caller
// what to change.
func applyNamespace(selector, document string) (string, error) {
	if selector == "" {
		return document, nil
	}
	if document == "" || artifacts.ResolveNamespace(document) == artifacts.ResolveNamespace(selector) {
		return selector, nil
	}
	return "", connect.NewError(connect.CodeInvalidArgument, &validationFieldError{Field: "namespace", Err: fmt.Errorf(
		"the document names namespace %q but it was applied into %q; "+
			"remove one of the two so there is a single answer",
		document, selector)})
}

// GetOrkflow reads a stored orkflow from one namespace; an empty version means
// the most recently applied one there, and an empty namespace is the default.
func (s *Service) GetOrkflow(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetOrkflowRequest],
) (*connect.Response[orkstackv1.GetOrkflowResponse], error) {
	if req.Msg.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	wf, err := s.orkflows.Get(ctx, ns.GetName(), req.Msg.GetName(), req.Msg.GetVersion())
	if err != nil {
		return nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q",
			req.Msg.GetName(), req.Msg.GetVersion(), ns.GetName())
	}
	return connect.NewResponse(&orkstackv1.GetOrkflowResponse{Orkflow: wf}), nil
}

// ListOrkflows returns the latest version of every orkflow in one namespace,
// read from the durable store on every call, narrowed by filters and paged by
// name.
//
// It is one prefix scan of that namespace, not a scan of everything followed
// by a filter: another namespace's documents are never read. The filters
// apply to the current-version documents that scan reads, BEFORE the page is
// cut, so a filtered listing past one page carries a next_page_token like an
// unfiltered one.
func (s *Service) ListOrkflows(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListOrkflowsRequest],
) (*connect.Response[orkstackv1.ListOrkflowsResponse], error) {
	size := int(req.Msg.GetPageSize())
	if size < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size cannot be negative"))
	}
	if size == 0 {
		size = engine.DefaultRunPageSize
	}
	set, err := compileFilters(req.Msg)
	if err != nil {
		return nil, err
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	cursor := ""
	if token := req.Msg.GetPageToken(); token != "" {
		if cursor, err = decodeOrkflowCursor(token, ns.GetName(), set); err != nil {
			return nil, err
		}
	}
	orkflows, err := s.orkflows.List(ctx, ns.GetName())
	if err != nil {
		return nil, orkflowStoreError(err, "list orkflows in namespace %q", ns.GetName())
	}
	matched := orkflows[:0]
	for _, wf := range orkflows {
		if wf.GetName() > cursor && set.Match(wf) {
			matched = append(matched, wf)
		}
	}
	response := &orkstackv1.ListOrkflowsResponse{Orkflows: matched}
	if len(matched) > size {
		response.Orkflows = matched[:size]
		token, err := encodeOrkflowCursor(ns.GetName(), set, response.Orkflows[size-1].GetName())
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		response.NextPageToken = token
	}
	return connect.NewResponse(response), nil
}

// orkflowCursor is a ListOrkflows page token: the last name a page held,
// bound to the namespace and the canonical filter set it was issued under.
type orkflowCursor struct {
	Version   int    `json:"v"`
	Namespace string `json:"n"`
	Filters   string `json:"f"`
	Name      string `json:"name"`
}

// orkflowCursorVersion is 2: version 1 carried no filter set and is refused.
const orkflowCursorVersion = 2

func encodeOrkflowCursor(namespace string, set listfilter.Set, name string) (string, error) {
	raw, err := stablejson.Marshal(orkflowCursor{Version: orkflowCursorVersion, Namespace: namespace, Filters: set.Key(), Name: name})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeOrkflowCursor(token, namespace string, set listfilter.Set) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	var c orkflowCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != orkflowCursorVersion || c.Name == "" {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	if c.Namespace != namespace {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_token does not match this namespace"))
	}
	if err := checkFilterKey(c.Filters, set); err != nil {
		return "", err
	}
	return c.Name, nil
}

// orkflowStoreError maps a store failure onto a Connect code. A missing
// orkflow is NotFound. Anything else means the durable store could not be
// reached: it is reported as Unavailable, never answered from memory.
func orkflowStoreError(err error, format string, args ...any) error {
	subject := fmt.Sprintf(format, args...)
	if errors.Is(err, artifacts.ErrNotFound) {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("%s not found", subject))
	}
	if errors.Is(err, artifacts.ErrInvalidOrkflow) {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s: %w", subject, err))
	}
	// A version already stored with different content is a collision, not an
	// outage: AlreadyExists, so a client can tell "publish a new version" from
	// "the bucket is unreachable" without reading the message.
	if errors.Is(err, artifacts.ErrVersionExists) {
		return connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("%s: %w", subject, err))
	}
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", subject, err))
}

// submission is one accepted request after routing has been decided: which
// cluster serves it, which definition it will execute, and where every task in
// that definition goes.
//
// It travels as one value because its parts must not be separable. The plan
// was computed FROM this definition; starting one with the other's routing
// would dispatch a task to a pool the plan never checked.
type submission struct {
	binding engine.Binding
	// wf is the planned definition: a clone of the stored one with every
	// task's pool filled in. The stored document is untouched.
	wf    *orkstackv1.Orkflow
	input *orkstackv1.Payload
	plan  engine.RunPlan
	// namespace is what the request resolved to, as prepare resolved it. It
	// is the run's tenant, and half of the claim coordinate below.
	namespace string
	// key is the caller's idempotency key, "" when they sent none. It is the
	// caller's own word and what an error quotes back to them; the coordinate
	// the claim authority is asked about is claimKey(), which is a different
	// string.
	key string
	// origin is what started this run, decided and validated by the door
	// that accepted it before prepare ran. It rides the plan into history and
	// the pending run into the response, the claim and a failed start's
	// archive record.
	origin *orkstackv1.RunOrigin
	// callbacks are the plaintext callback tokens minted for this
	// submission's `callback: true` awaits, held in memory only until the
	// response that starts the run discloses them. Only their salted digests
	// ride the plan.
	callbacks []awaitCallbackSecret
	// start is the definition the run's start carries: wf itself, or — for
	// a definition passed by reference — its stub, with definitionObject the
	// encoded object written under the final run id before each start
	// (definition_ref.go).
	start            *orkstackv1.Orkflow
	definitionObject []byte
}

// startDefinition is the definition the run's start carries.
func (s submission) startDefinition() *orkstackv1.Orkflow {
	if s.start != nil {
		return s.start
	}
	return s.wf
}

// claimKey is the coordinate one idempotency key occupies: the caller's key,
// scoped by the namespace it was submitted into.
//
// The scope is load-bearing. engine.ClaimWorkflowID hashes this string into a
// Temporal workflow id, and one id is one binding for the whole deployment, so
// an unscoped key makes "nightly" in payments and "nightly" in staging the
// same claim: the second tenant is answered with the first tenant's run id,
// reported as success, and the run they asked for is never started. A key is
// a caller's private word for one submission, so it is a coordinate WITHIN a
// namespace exactly as an orkflow name is.
//
// A namespace name is a slug (^[a-z][a-z0-9-]*$, pkg/artifacts), so it can
// never contain "/": the first separator splits the pair back apart, and no
// two (namespace, key) pairs can compose to one coordinate.
func (s submission) claimKey() string {
	if s.key == "" {
		return ""
	}
	return s.namespace + "/" + s.key
}

// SubmitRun creates a pending run and starts its Temporal workflow.
func (s *Service) SubmitRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.SubmitRunRequest],
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	// originForRequest validates origin_reference, then checks source_run_id,
	// then builds the origin. It is the Connect door's half of "every door
	// validates its own reference before submitFrom"; the trigger door
	// (triggers.go) is the other half.
	origin, err := s.originForRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return s.submitFrom(ctx, req.Msg, origin)
}

// submitFrom is SubmitRun with the origin already decided AND already
// validated by the door that decided it. It is the seam the trigger door
// (TRIGGER) and, later, schedules (SCHEDULE) start runs through: an origin's
// kind is a fact about the door, so the door supplies it and the request
// cannot. submitFrom validates nothing about the origin itself; every check
// prepare makes — namespace, orkflow, version, admission, pool plan — runs
// here unchanged.
func (s *Service) submitFrom(
	ctx context.Context,
	msg *orkstackv1.SubmitRunRequest,
	origin *orkstackv1.RunOrigin,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	sub, err := s.prepare(ctx, msg, origin)
	if err != nil {
		return nil, err
	}

	// A keyed submission asks the claimer which run the key names before it
	// publishes an event or calls Temporal. `ork` wires a claimer backed by
	// the cluster, so concurrent requests converge on one run whether they
	// reach one replica or several, and a failed start stays deduplicated to
	// its recorded failed run because a claim is never released.
	if sub.key != "" {
		return s.submitKeyed(ctx, sub)
	}
	return s.submitUnkeyed(ctx, sub)
}

// prepare turns a request into a routed submission, or refuses it.
//
// This is where a run's routing is decided, and it is the last place that may
// decide it: everything below is Temporal, and workflow code is deterministic
// and may not read a namespace document, this process's configuration, or the
// cluster (AGENTS.md §2.1). Three things happen here and nowhere else — a
// task's pool is resolved (its own, then its namespace's default, then the
// built-in "main"), the namespace's allow-list is enforced, and each resolved
// pool is checked for someone to run the work.
//
// It refuses before minting a run id, so a refused submission leaves nothing
// behind: no id burnt, no claim taken, no run recorded as failed.
//
// The origin it is handed was decided and validated by the door; prepare
// only carries it onto the plan, and never checks it a second time.
func (s *Service) prepare(
	ctx context.Context,
	msg *orkstackv1.SubmitRunRequest,
	origin *orkstackv1.RunOrigin,
) (submission, error) {
	name := msg.GetOrkflowName()
	if name == "" {
		return submission{}, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("orkflow_name is required"))
	}
	// The namespace is resolved BEFORE the orkflow is read, and it is the
	// request's, not the stored document's. A name is unique within a
	// namespace, so the pair is the orkflow's address: reading first and
	// asking afterwards would have to pick a namespace to read from, which is
	// the question being answered. It also puts the actionable error first —
	// a mistyped namespace says so, instead of reporting a missing orkflow in
	// a namespace that does not exist either.
	ns, err := s.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return submission{}, err
	}
	stored, err := s.orkflows.Get(ctx, ns.GetName(), name, msg.GetOrkflowVersion())
	if err != nil {
		return submission{}, orkflowStoreError(err, "orkflow %q version %q in namespace %q",
			name, msg.GetOrkflowVersion(), ns.GetName())
	}
	// The declared fan-out total against the deployment's cap NOW: a
	// definition applied before ORK_EACH_MAX_ITEMS was lowered is refused
	// before any id is minted.
	if err := s.checkEachCapAtSubmit(stored); err != nil {
		return submission{}, err
	}
	// The task ceiling likewise: lowering ORK_ORKFLOW_MAX_TASKS refuses new
	// runs of a version applied above the new value.
	if err := s.checkTaskCeilingAtSubmit(stored); err != nil {
		return submission{}, err
	}
	binding, err := s.binding(ns.GetName())
	if err != nil {
		return submission{}, err
	}
	if binding.Starter == nil {
		return submission{}, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("no run starter configured"))
	}
	// Admission is decided here, after everything the request itself could
	// have got wrong and before routing: a hold or a full namespace refuses
	// before any pool is looked at, so allow_empty_pool cannot bypass it, and
	// before a key is claimed or an id minted, so a refusal leaves nothing.
	if err := s.admit(ctx, ns, stored, binding); err != nil {
		return submission{}, err
	}

	planned, plan, err := s.resolverFor(binding).Plan(ctx, ns, stored, msg.GetAllowEmptyPool())
	if err != nil {
		return submission{}, poolPlanError(err)
	}
	// The image policy in force NOW, over the stored definition's resolved
	// images, before anything is minted or claimed: a definition applied
	// before the policy tightened is refused here, and allow_empty_pool does
	// not bypass it. The selection is then pinned on the plan, so every
	// attempt of every task is judged by this rule and this key, whatever the
	// policy says later.
	sel, err := s.imagePolicySelection(ctx, ns)
	if err != nil {
		return submission{}, err
	}
	if err := s.checkImagePolicy(ctx, ns, sel, planned); err != nil {
		return submission{}, submitPolicyError(err)
	}
	plan.ImagePolicy = engine.ImagePolicySelectionFromProto(sel)
	if err := s.pinDurabilityPlan(&plan); err != nil {
		return submission{}, connect.NewError(connect.CodeInternal,
			fmt.Errorf("resolve durable activity configuration: %w", err))
	}
	// The run-tree limits, once per tree: on the top-level plan only, and
	// never per child (planChildren pins only the durability plan per child);
	// the child's start copies them (startChild). Its own step, so
	// pinDurabilityPlan's transfer-budget early return can never skip it.
	s.pinRunTreeLimits(&plan)
	// The caching plan: nil for an Orkflow with no cached task, so no_cache
	// on such a run changes nothing — not even the plan's bytes.
	plan.Cache = s.cachePlanFor(planned, msg.GetNoCache())
	// Every child run this submission can start, planned recursively under
	// the parent's namespace, resolver, allow_empty_pool, image-policy
	// selection and boot answers. It refuses
	// and mints nothing.
	nsIdx, orIdx := indexAnswers(binding.Starter)
	plan.RootRunIndexed = rootIndexAnswer(binding.Starter)
	if err := s.planChildren(ctx, composeRequest{
		ns: ns, resolver: s.resolverFor(binding), allowEmpty: msg.GetAllowEmptyPool(),
		sel: sel, namespaceIndexed: nsIdx, originIndexed: orIdx, noCache: msg.GetNoCache(),
	}, planned, &plan, 0, nil); err != nil {
		return submission{}, err
	}
	// The fleet check, once the whole tree is planned and before anything is
	// minted: a run tree that needs a capability is refused while a process
	// that could decide it lacks one (fleetCheck).
	if err := s.fleetCheck(ctx, planned, &plan); err != nil {
		return submission{}, err
	}
	// The snapshot-overlay policy, a run-tree limit like those
	// pinRunTreeLimits pins, but decided here, once every child definition
	// is known: only a tree with a run large enough to use overlays asks the
	// fleet, and a fleet that cannot read them leaves the policy off — never
	// a refusal (pinSnapshotOverlay).
	s.pinSnapshotOverlay(ctx, planned, &plan)
	// The origin travels on the replay-pinned plan, as CommitRoot does: the
	// door that accepted the submission is the only thing that knows it, and
	// workflow code may not ask.
	plan.Origin = engine.OriginFromProto(origin)
	// The run input and the definition each travel as one payload of the
	// workflow start; refuse either over AGENTS.md §2.2's bound before
	// anything is minted.
	input := engine.ApplyInputDefaults(planned, msg.GetInput())
	// A definition over that bound travels by reference when the
	// deployment enables it: the start then carries a stub, which is what
	// the bound measures (definitionAtStart).
	start, definitionObject, err := s.definitionAtStart(ctx, planned, &plan, input)
	if err != nil {
		return submission{}, err
	}
	if err := checkRunStartPayloads(start, input); err != nil {
		return submission{}, err
	}
	// Callback tokens are minted LAST, after every refusal prepare makes, so
	// a refused submission mints nothing. Only the salted digests ride the
	// plan; the plaintexts stay in this submission until the one response
	// that starts the run.
	callbacks, err := mintAwaitCallbacks(planned, &plan)
	if err != nil {
		return submission{}, connect.NewError(connect.CodeInternal,
			fmt.Errorf("mint await callback tokens: %w", err))
	}
	// Every child's callback awaits, minted and discarded; then the plan's
	// size, measured with placeholder ids; then, last, the child run ids.
	if err := s.mintChildren(planned, &plan); err != nil {
		return submission{}, err
	}
	// The submission's fingerprint rides the plan of a keyed run, so a retry
	// under the key can tell the key's own run from a workflow something
	// else started under its id. A run with no key carries none, and its
	// plan encodes exactly as before.
	if msg.GetIdempotencyKey() != "" {
		fingerprint, err := submissionFingerprint(msg)
		if err != nil {
			return submission{}, connect.NewError(connect.CodeInvalidArgument, err)
		}
		plan.IdempotencyFingerprint = fingerprint
	}
	// The plan is the start's third payload. mintChildren measures it only
	// when the run composes children; every plan is measured here, after the
	// callback digests it carries have been minted.
	if err := checkRunPlanSize(plan); err != nil {
		return submission{}, err
	}
	return submission{
		callbacks:        callbacks,
		binding:          binding,
		wf:               planned,
		input:            input,
		plan:             plan,
		namespace:        ns.GetName(),
		key:              msg.GetIdempotencyKey(),
		origin:           origin,
		start:            start,
		definitionObject: definitionObject,
	}, nil
}

// checkRunStartPayloads refuses a run whose input or definition would be a
// workflow-start payload over the 256 KiB AGENTS.md §2.2 allows, measured the
// way task.CheckInputSize measures an activity input: as the bytes the data
// converter hands Temporal. An oversized input is the caller's to fix
// (INVALID_ARGUMENT, INPUT_TOO_LARGE); an oversized definition is the stored
// Orkflow's (FAILED_PRECONDITION). Without this the start itself fails, or
// worse, lands a history Temporal later refuses to grow.
func checkRunStartPayloads(wf *orkstackv1.Orkflow, input *orkstackv1.Payload) error {
	limit := int64(task.MaxActivityInputBytes)
	size, err := task.PayloadSize(input)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("encode the run input: %w", err))
	}
	if size > limit {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"%s: the run input serializes to %d bytes, over the %d KiB (%d bytes) a run's start may carry; "+
				"pass a reference to large data (a URL or an object key), not the bytes",
			task.ReasonInputTooLarge, size, limit/1024, limit))
	}
	size, err = task.PayloadSize(wf)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("encode the orkflow definition: %w", err))
	}
	if size > limit {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"orkflow %s@%s serializes to %d bytes, over the %d KiB (%d bytes) a run's start may carry; "+
				"split it into composed Orkflows or move large `with:` values into artifacts",
			wf.GetName(), wf.GetVersion(), size, limit/1024, limit))
	}
	return nil
}

// checkRunPlanSize refuses a run whose plan encodes over engine.MaxPlanBytes,
// with or without composed children.
func checkRunPlanSize(plan engine.RunPlan) error {
	encoded, err := stablejson.Marshal(plan)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("encode the run's plan: %w", err))
	}
	if len(encoded) > engine.MaxPlanBytes {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"the run's plan encodes to %d KiB, over the %d KiB a run may carry",
			kib(len(encoded)), engine.MaxPlanBytes/1024))
	}
	return nil
}

const (
	// DefaultFloorThroughputBytesPerSecond is 10 MiB/s (§6.3).
	DefaultFloorThroughputBytesPerSecond int64 = 10 * 1024 * 1024
	// MinimumTransferBudgetSeconds prevents a small deployment limit from
	// making image pull and fixed staging overhead unrealistically short.
	MinimumTransferBudgetSeconds int64 = 300
)

// pinDurabilityPlan resolves process configuration once, at submit, onto the
// replay-pinned workflow argument. No workflow code reads environment or IO.
func (s *Service) pinDurabilityPlan(plan *engine.RunPlan) error {
	root, err := artifacts.CommitRoot(s.artifacts)
	if err != nil {
		return err
	}
	plan.CommitRoot = root

	// Proto applies the contract defaults field-by-field. Copying the result
	// back keeps the plan self-contained rather than relying on Orker defaults.
	effective := s.limits.Proto()
	plan.Limits = engine.WorkspaceLimits{
		ValuesMaxBytes:   effective.GetValuesMaxBytes(),
		ArtifactMaxCount: effective.GetArtifactMaxCount(),
		ArtifactMaxBytes: effective.GetArtifactMaxBytes(),
		LogMaxBytes:      effective.GetLogMaxBytes(),
		CommitMaxBytes:   effective.GetCommitMaxBytes(),
	}

	if s.transferBudget > 0 {
		plan.TransferBudgetSeconds = s.transferBudget
		return nil
	}
	floor := s.floorThroughput
	if floor <= 0 {
		floor = DefaultFloorThroughputBytesPerSecond
	}
	seconds := int64(plan.Limits.ArtifactMaxCount) * plan.Limits.ArtifactMaxBytes / floor
	if seconds < MinimumTransferBudgetSeconds {
		seconds = MinimumTransferBudgetSeconds
	}
	if seconds > math.MaxInt32 {
		seconds = math.MaxInt32
	}
	plan.TransferBudgetSeconds = int32(seconds)
	return nil
}

// pinRunTreeLimits sets the run-tree limits on a top-level plan: the
// deployment's history ceiling and the reserve the guard keeps back below it
// (internal/engine/history_guard.go). Decided at submit, because workflow
// code may not read configuration (AGENTS.md §2.1), and carried so a later
// release that re-measures the reserve moves only new runs. Set once per run
// tree: every ChildRunPlan embeds a full plan inside its parent's, which is
// bounded at 256 KiB, so per-child limits would cost every composed task its
// bytes and refuse composed Orkflows that fit today.
//
// The continuation budget (when continuation is enabled) and the in-flight
// cap (always) travel the same way: every number the writer, the hold and
// the bootstrap decide with is carried, so a release that changes a default
// moves only new runs.
func (s *Service) pinRunTreeLimits(plan *engine.RunPlan) {
	plan.History = &engine.HistoryLimit{
		MaxEvents: s.historyMaxEvents,
		MaxBytes:  s.historyMaxBytes,
		Reserve:   engine.DefaultHistoryReserve,
	}
	if s.continuation != nil {
		budget := *s.continuation
		plan.Continuation = &budget
	}
	limit := engine.DefaultInFlightLimit(s.runMaxInFlight)
	plan.InFlight = &limit
	plan.PinCreationTimeoutSeconds = s.pinCreationSeconds
	plan.RetryMaxIntervalSeconds = int32(engine.DefaultRetryMaxInterval / time.Second)
	plan.RecordRootRun = true
}

// pinCreationTimeoutSecondsOf is the pin creation timeout a plan carries, in
// whole seconds: the engine default for an unset or negative value, and
// never below one second (cmd/ork refuses a sub-second value at boot, so a
// smaller one is only a caller that built a Config by hand).
func pinCreationTimeoutSecondsOf(timeout time.Duration) int32 {
	if timeout <= 0 {
		timeout = engine.DefaultPinCreationTimeout
	}
	return int32(max(int64(1), min(int64(timeout/time.Second), math.MaxInt32)))
}

// continuationBudgetFor is the budget every new top-level plan carries, or
// nil when continuation is off.
func continuationBudgetFor(cfg Config) *engine.ContinuationBudget {
	if !cfg.ContinuationEnabled {
		return nil
	}
	timeout := cfg.ContinuationRestoreTimeout
	if timeout <= 0 {
		timeout = engine.DefaultContinuationRestoreTimeout
	}
	budget := engine.DefaultContinuationBudget(
		positiveOr(cfg.ContinuationEvents, engine.DefaultContinuationEvents),
		positiveOr(cfg.ContinuationBytes, engine.DefaultContinuationBytes),
		timeout)
	return &budget
}

// positiveOr is value, or fallback when value is not positive.
func positiveOr(value, fallback int64) int64 {
	if value > 0 {
		return value
	}
	return fallback
}

// poolPlanError maps a routing refusal onto a Connect code.
//
// Three answers, and each sends the caller somewhere different. A pool the
// namespace forbids is the caller's mistake: InvalidArgument, fix the
// document. A pool with no Orkers is not — the request is well formed and the
// deployment is not ready for it — so it is FailedPrecondition, retryable once
// someone starts one. A pool that could not be checked at all is a fact about
// this engine's cluster and about nothing the caller did: Unavailable, ask
// again. Reporting that third case as InvalidArgument would tell every client
// to fix a request that has nothing wrong with it, which is the wrong fleet
// debugging the wrong thing for as long as the cluster is unwell.
func poolPlanError(err error) error {
	var notAllowed *engine.PoolNotAllowedError
	if errors.As(err, &notAllowed) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	// A secret source the namespace does not allow is the caller's document
	// to fix, exactly as a forbidden pool is.
	var secretRefused *engine.SecretNotAllowedError
	if errors.As(err, &secretRefused) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	// A pin whose tasks resolved onto two pools is the caller's own document
	// contradicting itself — the same class as a forbidden pool, so the same
	// code: InvalidArgument, fix the document.
	var pinMismatch *engine.PinPoolMismatchError
	if errors.As(err, &pinMismatch) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	var empty *engine.PoolEmptyError
	if errors.As(err, &empty) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	// A live pool that hosts no pins is PoolEmptyError's sibling and gets its
	// code: the request is well formed and the deployment is not
	// ready for it, so it is retryable once an operator sets
	// ORK_ORKER_MAX_PINS on that pool's Orkers.
	var noPinHost *engine.PoolNoPinHostError
	if errors.As(err, &noPinHost) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	// A pool whose Orker predates fan-out: the request is well formed and
	// the deployment is not ready for it (docs/DEPLOY.md §1).
	var noEach *engine.PoolNoEachSupportError
	if errors.As(err, &noEach) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	// A pool whose Orker predates task caching: it could not decode a cached
	// task's input, so the run is refused before any id is minted.
	var noCache *engine.PoolNoCacheSupportError
	if errors.As(err, &noCache) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	// A pool whose Orker predates join policies: it could not decode a
	// failure handler's input, so the run is refused before any id is minted.
	// A pool holding an Orker that predates attempt ordinals beside ones that
	// claim them: its records would overwrite theirs.
	var noOrdinals *engine.PoolNoAttemptOrdinalsError
	if errors.As(err, &noOrdinals) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	var noJoin *engine.PoolNoJoinSupportError
	if errors.As(err, &noJoin) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	// A task larger than any Orker of its pool runs: the request is well
	// formed and the pool cannot hold it (RESOURCES_UNSATISFIABLE).
	var unsatisfiable *engine.PoolResourcesUnsatisfiableError
	if errors.As(err, &unsatisfiable) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	var unreachable *engine.PoolUnreachableError
	if errors.As(err, &unreachable) {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewError(connect.CodeInvalidArgument, err)
}

// claimedSnapshot unpacks a claim's run. A claim written by an older engine
// may carry none, which is not an error: it means this replica has no answer
// from the claim and says so.
func claimedSnapshot(claim engine.Claim) (*orkstackv1.Run, error) {
	if len(claim.Snapshot) == 0 {
		return nil, nil
	}
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(claim.Snapshot, run); err != nil {
		return nil, fmt.Errorf("decode claimed run: %w", err)
	}
	return run, nil
}

// newPendingRun is the run a submission reports before its workflow has
// published anything. It carries the origin, so SubmitRunResponse.run, the
// idempotency claim's snapshot and a failed start's archive record all say
// what started it.
func (s *Service) newPendingRun(
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	origin *orkstackv1.RunOrigin,
) *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:             runID,
		Namespace:      wf.GetNamespace(),
		OrkflowName:    wf.GetName(),
		OrkflowVersion: wf.GetVersion(),
		Status:         orkstackv1.Status_STATUS_PENDING,
		Input:          input,
		CreatedAt:      timestamppb.New(s.now().UTC()),
		Origin:         origin,
	}
}

// recordFailedStart ends a run that never reached Temporal. It is the only
// place that can: the engine's projection activity covers every run that did
// start, and nothing covers one that did not.
//
// It is also the only place that publishes a run's whole stream itself, and so
// the only place that has to number it: this run has no history, so nothing
// else will ever produce its RUN_CREATED. Both events are numbered explicitly,
// 1 and 2, so a stream read back from the archive is indistinguishable from one
// a workflow produced.
func (s *Service) recordFailedStart(
	ctx context.Context,
	run *orkstackv1.Run,
	cause error,
) error {
	return s.recordFailedStartWith(ctx, run, cause, connect.CodeInternal)
}

// recordFailedStartWith is recordFailedStart answering the caller with code.
func (s *Service) recordFailedStartWith(
	ctx context.Context,
	run *orkstackv1.Run,
	cause error,
	code connect.Code,
) error {
	runID := run.GetId()
	failedAt := timestamppb.New(s.now().UTC())
	createdAt := run.GetCreatedAt()
	if createdAt == nil {
		createdAt = failedAt
	}
	run.Status = orkstackv1.Status_STATUS_FAILED
	run.Error = &orkstackv1.Error{Code: "RUN_START_FAILED", Message: cause.Error()}
	run.FinishedAt = failedAt
	created := &orkstackv1.Event{
		RunId:    runID,
		Sequence: 1,
		Type:     orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		Status:   orkstackv1.Status_STATUS_PENDING,
		Message: fmt.Sprintf("run created for orkflow %s@%s",
			run.GetOrkflowName(), run.GetOrkflowVersion()),
		Timestamp: createdAt,
	}
	failed := &orkstackv1.Event{
		RunId:     runID,
		Sequence:  runFailedToStartSequence,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		Status:    orkstackv1.Status_STATUS_FAILED,
		Message:   cause.Error(),
		Timestamp: failedAt,
	}
	// The archive first: it is the only durable record of a run that never
	// started, and what every replica and a retry under the key read. If it
	// cannot be written, nothing is said anywhere — no event, no webhook — so
	// a retry that then starts the id is never contradicted by a failure
	// that was announced and not recorded.
	if s.archive != nil {
		if err := s.archive.Archive(ctx, run, []*orkstackv1.Event{created, failed}); err != nil {
			s.forget(runID)
			return connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("the start was refused (%w) and could not be recorded: %w; submit again", cause, err))
		}
	}
	s.runs.Save(run)
	s.bus.Publish(created, runID+"#created")
	s.bus.Publish(failed, runID+"#failed")
	s.notifyRunFailedToStart(ctx, runID, failedAt, cause)
	return connect.NewError(code, cause)
}

// runFailedToStartSequence is the sequence of the RUN_FAILED a run that never
// reached Temporal publishes, after its RUN_CREATED at 1.
const runFailedToStartSequence = 2

// notifyRunFailedToStart announces the terminal transition of a run that never
// reached Temporal, so a subscriber — and telemetry — sees the same
// RUN_FAILED it would have seen had the run started and then failed.
func (s *Service) notifyRunFailedToStart(
	ctx context.Context,
	runID string,
	failedAt *timestamppb.Timestamp,
	cause error,
) {
	if s.notifier == nil && s.observer == nil {
		return
	}
	run, ok := s.runs.Get(runID)
	if !ok {
		return
	}
	event := &orkstackv1.Event{
		RunId:     runID,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		Status:    orkstackv1.Status_STATUS_FAILED,
		Message:   cause.Error(),
		Timestamp: failedAt,
	}
	if s.notifier != nil {
		// A delivery carries the sequence the run's stream shows for it.
		delivered := proto.Clone(event).(*orkstackv1.Event)
		delivered.Sequence = runFailedToStartSequence
		s.notifier.NotifyRunEvent(ctx, delivered, run)
	}
	if s.observer != nil {
		s.observer.ObserveEvent(ctx, event, run)
	}
}

// ArchivedEvents returns the events archived for a run, after fromSequence. It
// is the read path for a run this process never executed, and it reports the
// same Connect errors the live path would: NotFound when nothing was archived
// under that id, Internal when the archive itself is unreachable. When the
// requested range overlaps a gap the archive records — events lost from
// object storage before the run was archived, or numbers the run never
// published — it is DataLoss, naming the range.
func (s *Service) ArchivedEvents(
	ctx context.Context,
	runID string,
	fromSequence int64,
) ([]*orkstackv1.Event, error) {
	events, lost, err := s.archivedEventsBeforeLoss(ctx, runID, fromSequence)
	if err != nil {
		return nil, err
	}
	if lost != nil {
		return nil, lost
	}
	return events, nil
}

// archivedEventsBeforeLoss is ArchivedEvents' body: the archived events after
// fromSequence, and — when the requested range overlaps a gap the archive
// records (events lost from object storage before the run was archived, or
// numbers it never published) —
// only the events below the first overlapping gap, with a CodeDataLoss error
// naming every overlapping range. A gap entirely at or below fromSequence is
// not part of the request and costs nothing.
func (s *Service) archivedEventsBeforeLoss(
	ctx context.Context,
	runID string,
	fromSequence int64,
) ([]*orkstackv1.Event, *connect.Error, error) {
	if s.archive == nil {
		return nil, nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
	}
	events, err := s.archive.LoadRunEvents(ctx, runID)
	var lost *engine.ArchivedEventsLostError
	switch {
	case errors.As(err, &lost):
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
	case err != nil:
		return nil, nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("read run %q from archive: %w", runID, err))
	}
	var overlapping []engine.EventGap
	if lost != nil {
		for _, gap := range lost.Gaps {
			if gap.Last > fromSequence {
				overlapping = append(overlapping, gap)
			}
		}
	}
	firstLost := int64(math.MaxInt64)
	for _, gap := range overlapping {
		firstLost = min(firstLost, max(gap.First, fromSequence+1))
	}
	after := make([]*orkstackv1.Event, 0, len(events))
	for _, event := range events {
		if event.GetSequence() > fromSequence && event.GetSequence() < firstLost {
			after = append(after, event)
		}
	}
	if len(overlapping) == 0 {
		return after, nil, nil
	}
	return after, connect.NewError(connect.CodeDataLoss, archivedLossError(runID, overlapping)), nil
}

// archivedLossError is the error a read over a gap receives, archived or
// live: a *streamGapError, whose sentence names each range for its cause.
func archivedLossError(runID string, gaps []engine.EventGap) error {
	return &streamGapError{runID: runID, gaps: gaps}
}

// streamGapError is a read over gaps in a run's stream: events lost from
// object storage before the run was archived, or numbers it never published.
// Its message is the one sentence every such read gives, in run vocabulary
// only; closeReason is a shortened form of it that fits a WebSocket close
// frame.
type streamGapError struct {
	runID string
	gaps  []engine.EventGap
}

// gapClauses names the ranges for their causes, "events … of <subject>".
func (e *streamGapError) gapClauses(subject string, withReasons bool) []string {
	var lostRanges, lostReasons, unpublished []string
	for _, gap := range e.gaps {
		if gap.Unpublished() {
			unpublished = append(unpublished, fmt.Sprintf("%d–%d", gap.First, gap.Last))
			continue
		}
		lostRanges = append(lostRanges, fmt.Sprintf("%d–%d", gap.First, gap.Last))
		lostReasons = append(lostReasons, gap.Reason)
	}
	var clauses []string
	if len(lostRanges) > 0 {
		clause := fmt.Sprintf("events %s of %s were lost from object storage before the run was archived",
			strings.Join(lostRanges, ", "), subject)
		if withReasons {
			clause += " (" + strings.Join(lostReasons, "; ") + ")"
		}
		clauses = append(clauses, clause)
	}
	if len(unpublished) > 0 {
		clause := fmt.Sprintf("events %s of %s were never published", strings.Join(unpublished, ", "), subject)
		if withReasons {
			clause += " (" + engine.UnpublishedEventsReason + ")"
		}
		clauses = append(clauses, clause)
	}
	return clauses
}

func (e *streamGapError) Error() string {
	return strings.Join(e.gapClauses("run "+e.runID, true), "; ") + "; the run's outcome and its other events are intact"
}

// maxCloseReason is the most a WebSocket close frame's reason holds
// (RFC 6455 §5.5: a 125-byte control payload, 2 of them the status code).
const maxCloseReason = 123

// closeReason is the sentence for the socket that streams this one run: the
// run is the socket's own, so it is "this run". It never carries the
// reasons, which no frame can hold beside a range. When the frame cannot
// hold every range, the closing clause goes first; then each cause keeps its
// first range and counts the rest; and last, for ranges too long to name
// beside both causes, each cause counts its ranges. Every form names each
// cause, and the last fits the frame for any count a slice can hold. A tier
// is built only when the one before it does not fit.
func (e *streamGapError) closeReason() string {
	if len(e.gaps) == 0 {
		return "the stream of this run ended at a gap it could not name"
	}
	every := strings.Join(e.gapClauses("this run", false), "; ")
	for _, reason := range []func() string{
		func() string { return every + "; its outcome and other events are intact" },
		func() string { return every },
		e.firstRangesReason,
	} {
		if r := reason(); len(r) <= maxCloseReason {
			return r
		}
	}
	return e.countedReason()
}

// gapsByCause splits the gaps into those lost from object storage and those
// never published, each in sequence order.
func (e *streamGapError) gapsByCause() (lost, unpublished []engine.EventGap) {
	for _, gap := range e.gaps {
		if gap.Unpublished() {
			unpublished = append(unpublished, gap)
		} else {
			lost = append(lost, gap)
		}
	}
	return lost, unpublished
}

// firstRangesReason names each cause with its first range and counts the
// rest: "events 1201–2400 and 3 more ranges of this run were lost from
// object storage". With both causes, the one whose first range comes first
// leads.
func (e *streamGapError) firstRangesReason() string {
	lost, unpublished := e.gapsByCause()
	first := func(gaps []engine.EventGap) string {
		named := fmt.Sprintf("%d–%d", gaps[0].First, gaps[0].Last)
		if len(gaps) > 1 {
			named += " and " + rangeCount(len(gaps)-1, "more ")
		}
		return named
	}
	switch {
	case len(unpublished) == 0:
		return "events " + first(lost) + " of this run were lost from object storage"
	case len(lost) == 0:
		return "events " + first(unpublished) + " of this run were never published"
	case unpublished[0].First < lost[0].First:
		return "events " + first(unpublished) + " of this run were never published, and " + first(lost) +
			" were lost from object storage"
	default:
		return "events " + first(lost) + " of this run were lost from object storage, and " + first(unpublished) +
			" were never published"
	}
}

// countedReason names each cause with how many ranges it has, and no range.
func (e *streamGapError) countedReason() string {
	lost, unpublished := e.gapsByCause()
	switch {
	case len(unpublished) == 0:
		return "events of this run in " + rangeCount(len(lost), "") + " were lost from object storage"
	case len(lost) == 0:
		return "events of this run in " + rangeCount(len(unpublished), "") + " were never published"
	default:
		return "events of this run in " + rangeCount(len(lost), "") + " were lost from object storage, in " +
			rangeCount(len(unpublished), "") + " never published"
	}
}

// rangeCount is "1 range" or "n ranges", with an adjective between.
func rangeCount(n int, adjective string) string {
	if n == 1 {
		return "1 " + adjective + "range"
	}
	return fmt.Sprintf("%d %sranges", n, adjective)
}

// LookupRun resolves a run through the read model, in the order the system of
// record demands: Temporal first, because it holds every run that ever
// started, live or finished; then the archive, which is the record for a run
// whose history has aged out of retention and for one whose start failed and
// so has no history at all.
//
// With no reader wired — a unit test — the process-local store answers instead.
// It is never consulted behind a reader: a cache that can turn a NotFound into
// a run, or a run into a NotFound, is not a cache.
//
// Errors are already Connect-coded, so a caller can branch on
// connect.CodeNotFound.
func (s *Service) LookupRun(ctx context.Context, runID string) (*orkstackv1.Run, error) {
	binding, err := s.locate(ctx, runID)
	if err != nil {
		return nil, err
	}
	if binding.Reader != nil {
		run, err := binding.Reader.GetRun(ctx, runID)
		switch {
		case err == nil:
			return run, nil
		case !errors.Is(err, artifacts.ErrNotFound):
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read run %q: %w", runID, err))
		}
	} else if run, ok := s.runs.Get(runID); ok {
		return run, nil
	}
	if s.archive != nil {
		run, err := s.archive.LoadRun(ctx, runID)
		switch {
		case err == nil:
			return run, nil
		case !errors.Is(err, artifacts.ErrNotFound):
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("read run %q from archive: %w", runID, err))
		}
	}
	return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
}

// LookupRunIn resolves a run by id INSIDE one namespace, and is the read every
// request carrying a run id goes through.
//
// A run id is an address inside a namespace, not a capability over the
// deployment. It is unguessable, but unguessable is not a tenancy boundary: an
// id travels into logs, tickets and CI transcripts, and CancelRun is a WRITE —
// so before this check, `orkctl -n alpha run cancel <a-bravo-run-id>`
// succeeded, and the tenant whose run it was learned about it from the run.
//
// The namespace is checked against the run the read model returned, never
// against the id: nothing parses a run id (AGENTS.md §7), and the run's own
// namespace is a coordinate it carries (`Run.namespace`). A run of another
// namespace is NotFound rather than PermissionDenied, on the same grounds the
// WebSocket bridge checks its origin before it resolves the run: telling the
// two apart would confirm the id exists to a caller who may not name the
// namespace holding it.
func (s *Service) LookupRunIn(ctx context.Context, runID, namespace string) (*orkstackv1.Run, error) {
	ns, err := s.namespace(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return s.lookupRunInResolved(ctx, runID, ns.GetName())
}

// lookupRunInResolved is LookupRunIn for a namespace already resolved through
// s.namespace, so a request that addresses two runs resolves its one
// namespace once and reports a namespace error once, unattributed to a side.
func (s *Service) lookupRunInResolved(ctx context.Context, runID, namespace string) (*orkstackv1.Run, error) {
	run, err := s.LookupRun(ctx, runID)
	if connect.CodeOf(err) == connect.CodeNotFound {
		// The same words as a run of another namespace: the message says
		// where it looked, and the two cases stay indistinguishable.
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("run %q not found in namespace %q", runID, namespace))
	}
	if err != nil {
		return nil, err
	}
	if artifacts.ResolveNamespace(run.GetNamespace()) != namespace {
		return nil, errRunNotInNamespace(runID, namespace)
	}
	return run, nil
}

// errRunNotInNamespace is the NotFound for a run of another namespace, worded
// once for LookupRunIn and runNamespaceIn alike.
func errRunNotInNamespace(runID, namespace string) error {
	return connect.NewError(connect.CodeNotFound,
		fmt.Errorf("run %q not found in namespace %q", runID, namespace))
}

// runNamespaceIn answers LookupRunIn's question — does this run exist in this
// namespace? — without reading the run, for a caller that needs only the
// answer: the WebSocket bridges' 404 before the upgrade, StreamRunEvents, and
// a source-run check at submit.
//
// It reads the run's start event alone (engine.RunReader.RunStart, one
// history event), whose definition names the run's namespace, where
// LookupRunIn replays the run's whole history. A run the cluster no longer
// holds is answered from the archive's run line, as LookupRun answers it. The
// errors are LookupRunIn's, word for word: NotFound for a missing run and for
// a run of another namespace alike.
func (s *Service) runNamespaceIn(ctx context.Context, runID, namespace string) error {
	_, err := s.runFactsIn(ctx, runID, namespace)
	return err
}

// runFacts is what runFactsIn learns about a run beside its namespace: the
// namespace that owns the Orkflow it ran (empty when its own) and its root —
// or, for a child whose plan recorded no root, its parent.
type runFacts struct {
	Orkflow          string
	OrkflowNamespace string
	RootRunID        string
}

// rootOrParent is a child run's root, or its parent when no root was recorded.
func rootOrParent(origin *orkstackv1.RunOrigin) string {
	if root := origin.GetRootRunId(); root != "" {
		return root
	}
	if origin.GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN {
		return origin.GetId()
	}
	return ""
}

// runFactsIn is runNamespaceIn returning the facts its one read already holds.
func (s *Service) runFactsIn(ctx context.Context, runID, namespace string) (runFacts, error) {
	ns, err := s.namespace(ctx, namespace)
	if err != nil {
		return runFacts{}, err
	}
	resolved := ns.GetName()
	binding, err := s.locate(ctx, runID)
	if err != nil {
		return runFacts{}, err
	}
	found, runNamespace, facts := false, "", runFacts{}
	if binding.Reader != nil {
		start, err := binding.Reader.RunStart(ctx, runID)
		switch {
		case err == nil:
			found, runNamespace = true, start.Orkflow.GetNamespace()
			facts = runFacts{Orkflow: start.Orkflow.GetName(), OrkflowNamespace: start.Orkflow.GetDefinitionNamespace()}
			if start.Plan.Origin != nil {
				facts.RootRunID = start.Plan.Origin.RootRunID
				if facts.RootRunID == "" && start.Plan.Origin.Kind == engine.RunOriginKindParentRun {
					facts.RootRunID = start.Plan.Origin.ID
				}
			}
		case !errors.Is(err, artifacts.ErrNotFound):
			return runFacts{}, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read run %q: %w", runID, err))
		}
	} else if run, ok := s.runs.Get(runID); ok {
		found, runNamespace = true, run.GetNamespace()
		facts = runFacts{Orkflow: run.GetOrkflowName(), OrkflowNamespace: run.GetOrkflowNamespace(), RootRunID: rootOrParent(run.GetOrigin())}
	}
	if !found && s.archive != nil {
		run, err := s.archive.LoadRun(ctx, runID)
		switch {
		case err == nil:
			found, runNamespace = true, run.GetNamespace()
			facts = runFacts{Orkflow: run.GetOrkflowName(), OrkflowNamespace: run.GetOrkflowNamespace(), RootRunID: rootOrParent(run.GetOrigin())}
		case !errors.Is(err, artifacts.ErrNotFound):
			return runFacts{}, connect.NewError(connect.CodeInternal,
				fmt.Errorf("read run %q from archive: %w", runID, err))
		}
	}
	if !found || artifacts.ResolveNamespace(runNamespace) != resolved {
		// An unknown id and another namespace's run read the same, as in
		// lookupRunInResolved: the message names where it looked.
		return runFacts{}, errRunNotInNamespace(runID, resolved)
	}
	return facts, nil
}

// GetRun returns a run of one namespace, from whichever replica the request
// reached.
func (s *Service) GetRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetRunRequest],
) (*connect.Response[orkstackv1.GetRunResponse], error) {
	runID := req.Msg.GetRunId()
	if runID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	run, err := s.LookupRunIn(ctx, runID, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	SetRunTiming(run, time.Now())
	return connect.NewResponse(&orkstackv1.GetRunResponse{Run: run}), nil
}

// MaxRunPageSize is the most runs one ListRuns page returns. A larger
// page_size is INVALID_ARGUMENT, on the deliveries precedent: an archived row
// on a page costs one body read, so the ceiling bounds what one request reads.
const MaxRunPageSize = 200

// ListRuns returns runs of one namespace, newest first, narrowed by the
// request's filters (docs/UI_CLI_PARITY.md "Filtering lists").
//
// The two halves are Temporal's visibility index, which holds every run that
// started and is the only source for one still executing, and the archive,
// which holds every run that finished. A run in both is reported from the
// archive: visibility knows a run's id, orkflow, status and times, while the
// archive holds its task runs, input and output, and a finished run's outcome
// is settled, so the richer record is also the correct one.
//
// BOTH halves are namespace-scoped by their own index, and neither by a filter
// over the merged list. The archive's key begins with the namespace, so naming
// one narrows the scan; visibility is asked with the namespace as a query
// predicate, on the search attribute the engine registers at boot
// (engine.TemporalRunReader.ListRuns). Scoped after the fact instead — the
// trade the reader still makes for an orkflow name — a tenancy boundary would
// be answered out of whatever page budget another namespace's traffic left,
// and a quiet namespace beside a busy one is told it has no runs. An orkflow
// name inside one namespace is not a tenancy boundary and stays a filter.
//
// Both halves get the SAME filter set. The single-value equalities and the
// created_at bounds are pushdowns that narrow what each half reads (engine.RunListPlan);
// the whole set is then applied to every row read, on engine.RunFilterView —
// so a pushdown can never widen or change an answer, and a run that moves from
// live to archived between pages is still deduplicated by id.
//
// Visibility is eventually consistent. A run submitted a moment ago may not be
// listed yet; it is readable by id throughout, because GetRun reads history
// rather than the index. A run that has just finished can still read RUNNING
// in visibility while its archive record exists: under a status filter that
// excludes its final status, the live half keeps the stale RUNNING row and the
// archive half rejects the settled copy, so it is listed once, as RUNNING,
// until visibility catches up — exactly what a status=RUNNING filter has
// always answered. Correcting it would cost a body read per live row.
func (s *Service) ListRuns(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListRunsRequest],
) (*connect.Response[orkstackv1.ListRunsResponse], error) {
	set, err := compileFilters(req.Msg)
	if err != nil {
		return nil, err
	}
	size := int(req.Msg.GetPageSize())
	switch {
	case size < 0:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size cannot be negative"))
	case size > MaxRunPageSize:
		// Refused, never clamped: a page is a body read per archived row, so
		// the ceiling is what bounds one request's cost.
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("page_size %d is above the maximum %d", size, MaxRunPageSize))
	case size == 0:
		size = engine.DefaultRunPageSize
	}
	plan := engine.RunListPlan(set)
	var (
		runs       []*orkstackv1.Run
		fromMemory bool
	)
	// Two questions, two answers: the router says which cluster serves this
	// namespace, and the namespace itself scopes what is listed off it.
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	cursor, err := decodeRunCursor(req.Msg.GetPageToken(), ns.GetName(), set)
	if err != nil {
		return nil, err
	}
	beforeCreated, beforeID := time.Time{}, ""
	if cursor != nil {
		beforeCreated, beforeID = parseCursorTime(cursor.LastCreated), cursor.LastID
	}
	binding, err := s.binding(ns.GetName())
	if err != nil {
		return nil, err
	}
	if binding.Reader != nil {
		query := plan
		query.Namespace = ns.GetName()
		// Visibility does not promise ID ordering inside equal timestamps.
		// Complete collects its bounded metadata scan before applying Limit,
		// so a later higher-ID tie cannot disappear behind this cursor.
		query.BeforeCreated, query.BeforeID, query.Limit, query.Complete = beforeCreated, beforeID, size+1, true
		listed, err := binding.Reader.ListRuns(ctx, query)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("list runs in namespace %q: %w", ns.GetName(), err))
		}
		runs = listed
	} else {
		runs = s.runs.List(engine.ListFilter{
			Namespace:   ns.GetName(),
			OrkflowName: plan.OrkflowName,
			Status:      plan.Status,
			OriginKind:  plan.OriginKind,
			OriginID:    plan.OriginID,
			Keep:        plan.Keep,
		})
		if cursor != nil {
			runs = afterRunCursor(runs, cursor.LastCreated, cursor.LastID)
		}
		fromMemory = true
	}
	if s.archive != nil {
		archived, err := s.archive.ListRuns(ctx, engine.ArchiveQuery{
			Namespace:     ns.GetName(),
			OrkflowName:   plan.OrkflowName,
			Status:        plan.Status,
			OriginKind:    plan.OriginKind,
			OriginID:      plan.OriginID,
			RootRunID:     plan.RootRunID,
			Since:         plan.Since,
			Until:         plan.Until,
			Keep:          plan.Keep,
			BeforeCreated: beforeCreated,
			BeforeID:      beforeID,
			Limit:         size + 1,
		})
		// Every archive key segment a query builds comes from the caller's
		// filter: the namespace and Orkflow names are validated before this,
		// so one no key can hold is an origin.id, the caller's to change.
		if errors.Is(err, artifacts.ErrInvalidRunKey) {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("list runs in namespace %q: filter origin.id is %d bytes, longer than any origin's id can be: %w",
					ns.GetName(), len(plan.OriginID), err))
		}
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("list runs in namespace %q from archive: %w", ns.GetName(), err))
		}
		if fromMemory {
			// This process executed those runs, so its snapshots are at least
			// as fresh as the archive's.
			runs = mergeRuns(runs, archived)
		} else {
			runs = mergeRuns(archived, runs)
		}
	}
	response := &orkstackv1.ListRunsResponse{}
	if len(runs) > size {
		page := runs[:size]
		last := page[len(page)-1]
		token, err := encodeRunCursor(runCursor{
			Version: runCursorVersion, Namespace: ns.GetName(), Filters: set.Key(),
			LastCreated: engine.RunIndexTime(last).UTC().Format(time.RFC3339Nano), LastID: last.GetId(),
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode run page token: %w", err))
		}
		response.Runs, response.NextPageToken = page, token
	} else {
		response.Runs = runs
	}
	// A list page never carries a fan-out's items: GetRun is the read that
	// does, and a page of archived fan-out runs must not carry thousands of
	// items per run.
	for i, run := range response.Runs {
		response.Runs[i] = engine.WithoutItems(run)
	}
	return connect.NewResponse(response), nil
}

// runCursorVersion is 2: a version-1 token carried the removed typed filters
// and is refused as invalid.
const runCursorVersion = 2

// runCursor is a ListRuns page token: the namespace and canonical filter set
// it was minted under, and the created_at/id of the last run its page held.
// A cursor is a position, not a snapshot: a run whose status changes out of
// the filter between pages simply stops matching.
type runCursor struct {
	Version     int    `json:"v"`
	Namespace   string `json:"n"`
	Filters     string `json:"f"`
	LastCreated string `json:"c"`
	LastID      string `json:"i"`
}

func encodeRunCursor(c runCursor) (string, error) {
	raw, err := stablejson.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// decodeRunCursor reads a ListRuns page token, refusing one minted for
// another namespace or another filter set.
func decodeRunCursor(token, namespace string, set listfilter.Set) (*runCursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	var c runCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != runCursorVersion || c.LastCreated == "" || c.LastID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	if _, err := time.Parse(time.RFC3339Nano, c.LastCreated); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	if c.Namespace != namespace {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_token does not match this namespace"))
	}
	if err := checkFilterKey(c.Filters, set); err != nil {
		return nil, err
	}
	return &c, nil
}
func afterRunCursor(runs []*orkstackv1.Run, created, id string) []*orkstackv1.Run {
	at, _ := time.Parse(time.RFC3339Nano, created)
	out := runs[:0]
	for _, run := range runs {
		t := engine.RunIndexTime(run)
		if t.Before(at) || (t.Equal(at) && run.GetId() < id) {
			out = append(out, run)
		}
	}
	return out
}
func parseCursorTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

// mergeRuns folds two sources of runs into one page, newest first. preferred
// wins on a duplicate id, and equal timestamps fall back to the id so the order
// of a page never depends on map iteration.
func mergeRuns(preferred, others []*orkstackv1.Run) []*orkstackv1.Run {
	merged := make([]*orkstackv1.Run, 0, len(preferred)+len(others))
	seen := make(map[string]struct{}, len(preferred))
	for _, run := range preferred {
		seen[run.GetId()] = struct{}{}
		merged = append(merged, run)
	}
	for _, run := range others {
		if _, duplicate := seen[run.GetId()]; duplicate {
			continue
		}
		merged = append(merged, run)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		a, b := engine.RunIndexTime(merged[i]), engine.RunIndexTime(merged[j])
		if !a.Equal(b) {
			return a.After(b)
		}
		return merged[i].GetId() > merged[j].GetId()
	})
	return merged
}

// CancelRun requests cancellation of a run's Temporal workflow. The run is
// resolved through the read model and inside the request's namespace, so a
// replica can cancel a run it did not start and no namespace can cancel
// another's.
//
// The Ork-Principal header, bounded as an origin's principal is, travels with
// the cancellation, and the run records it as who canceled it
// (Run.canceled_by): asserted by the gateway, never authenticated.
func (s *Service) CancelRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.CancelRunRequest],
) (*connect.Response[orkstackv1.CancelRunResponse], error) {
	if req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	run, err := s.LookupRunIn(ctx, req.Msg.GetRunId(), req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	binding, err := s.locate(ctx, req.Msg.GetRunId())
	if err != nil {
		return nil, err
	}
	if binding.Starter == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no run starter configured"))
	}
	request := engine.CancelRequest{
		Reason:    req.Msg.GetReason(),
		Principal: boundOriginAssertion(req.Header().Get(AuditPrincipalHeader)),
	}
	if err := binding.Starter.CancelRun(ctx, req.Msg.GetRunId(), request); err != nil {
		return nil, connect.NewError(cancelErrorCode(err), err)
	}
	return connect.NewResponse(&orkstackv1.CancelRunResponse{Run: run}), nil
}

// cancelErrorCode is the code a refused cancellation answers with: a cluster
// that was unavailable or rate-limited, even after the starter asked again,
// is worth asking again later, and a caller that gave up is told so. Anything
// else — engine.ErrCancelNotWired, a mis-wired process, included — is the
// internal error it always was.
func cancelErrorCode(err error) connect.Code {
	var (
		unavailable *serviceerror.Unavailable
		exhausted   *serviceerror.ResourceExhausted
		canceled    *serviceerror.Canceled
		deadline    *serviceerror.DeadlineExceeded
	)
	switch {
	// The cluster's own spellings of the two, which unwrap to neither.
	case errors.Is(err, context.Canceled), errors.As(err, &canceled):
		return connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &deadline):
		return connect.CodeDeadlineExceeded
	case errors.As(err, &unavailable):
		return connect.CodeUnavailable
	case errors.As(err, &exhausted):
		return connect.CodeResourceExhausted
	default:
		return connect.CodeInternal
	}
}

// StreamEvents server-streams a run's events: everything after from_sequence,
// ending at the run's terminal event.
func (s *Service) StreamEvents(
	ctx context.Context,
	req *connect.Request[orkstackv1.StreamEventsRequest],
	stream *connect.ServerStream[orkstackv1.Event],
) error {
	runID := req.Msg.GetRunId()
	if runID == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	return s.StreamRunEvents(ctx, runID, req.Msg.GetNamespace(), req.Msg.GetFromSequence(), stream.Send)
}

// ErrEngineStopping is the cause an engine cancels its handlers' context with
// when it starts to shut down, which tells a stream cut short by the replica
// from one whose caller went away.
var ErrEngineStopping = errors.New("this engine replica is shutting down")

// engineStopping reports whether ctx ended because the replica is stopping.
func engineStopping(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrEngineStopping)
}

// stoppedShort is a stream's answer when the replica stopped under it before
// its end: Unavailable, the one answer every client resumes on, instead of the
// clean end a run or a log would be taken to have reached. err is what the
// stream itself returned. Any other failure stands, as does a clean end once
// the stream's end was delivered, which the caller decides by not asking.
func stoppedShort(ctx context.Context, what string, err error) error {
	if engineStopping(ctx) && (err == nil || errors.Is(err, context.Canceled)) {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", what, ErrEngineStopping))
	}
	return err
}

// StreamRunEvents delivers a run's events after fromSequence to send, and
// returns when the run reaches its terminal event. It is the one streaming
// path: the Connect handler and the WebSocket bridge both go through it, so
// they cannot drift apart.
//
// Temporal is read first and answers for every run that started, live or
// finished — on any replica, whoever started it. A run it does not know is
// replayed from the archive: its history has aged out of retention, or its
// start failed and there is no history at all. The reader reports a missing run
// before it sends anything, so the fallback cannot duplicate an event.
//
// With no reader wired — a unit test — the process-local bus answers instead;
// it knows only this process's runs, which is all a unit test has.
//
// The namespace is resolved here rather than in the two entry points, for the
// same reason the streaming is: a scope one caller enforces and the other
// forgets is not a scope. A run of another namespace is NotFound before a
// single event is sent.
//
// A replica that stops before the run's terminal event answers Unavailable, so
// the caller reads the rest from another (stoppedShort).
//
// The check reads the run's start event (runNamespaceIn), never its whole
// history: the stream that follows reads the history once, and that is the
// only full read a stream costs.
func (s *Service) StreamRunEvents(
	ctx context.Context,
	runID string,
	namespace string,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	if err := s.runNamespaceIn(ctx, runID, namespace); err != nil {
		return err
	}
	return s.streamCheckedRunEvents(ctx, runID, fromSequence, send)
}

// streamCheckedRunEvents is StreamRunEvents after its namespace check, for
// the one caller that has just made that same check itself: the WebSocket
// bridge, which must answer a run of another namespace with an HTTP 404
// BEFORE the upgrade, and would otherwise pay the check twice per socket.
func (s *Service) streamCheckedRunEvents(
	ctx context.Context,
	runID string,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	finished := false
	err := s.streamRunEventsFrom(ctx, runID, fromSequence, func(event *orkstackv1.Event) error {
		if err := send(event); err != nil {
			return err
		}
		finished = finished || IsTerminalEvent(event)
		return nil
	})
	if finished {
		return err
	}
	return stoppedShort(ctx, fmt.Sprintf("stream run %q", runID), err)
}

func (s *Service) streamRunEventsFrom(
	ctx context.Context,
	runID string,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	binding, err := s.locate(ctx, runID)
	if err != nil {
		return err
	}
	if binding.Reader != nil {
		err := binding.Reader.StreamEvents(ctx, runID, fromSequence, send)
		switch {
		case err == nil:
			return nil
		case ctx.Err() != nil:
			// The caller went away mid-stream. That is not a failure of the
			// run, and the old bus path reported it as a clean end too.
			return nil
		case !errors.Is(err, artifacts.ErrNotFound):
			return connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("stream run %q: %w", runID, err))
		}
		return s.replayArchivedEvents(ctx, runID, fromSequence, send)
	}
	if _, ok := s.runs.Get(runID); !ok {
		return s.replayArchivedEvents(ctx, runID, fromSequence, send)
	}

	events, cancel, terminalAlreadyReached, streamErr := s.bus.Subscribe(runID, fromSequence)
	defer cancel()
	if terminalAlreadyReached {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, open := <-events:
			if !open {
				// A number the bus waited for too long ends the stream with
				// its own error, never a silent stall (EventBus.Subscribe).
				if err := streamErr(); err != nil {
					return connect.NewError(connect.CodeUnavailable, err)
				}
				return nil
			}
			if err := send(event); err != nil {
				return err
			}
			if IsTerminalEvent(event) {
				return nil
			}
		}
	}
}

// replayArchivedEvents sends an archived run's stream and ends. It is finished
// by definition — nothing archives before its terminal event — so the replay is
// the whole stream.
func (s *Service) replayArchivedEvents(
	ctx context.Context,
	runID string,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	archived, lost, err := s.archivedEventsBeforeLoss(ctx, runID, fromSequence)
	if err != nil {
		return err
	}
	// Every event below the first gap is sent, then the gap — lost or never
	// published — is the stream's error: a reader never mistakes a gapped
	// stream for a whole one.
	for _, event := range archived {
		if err := send(event); err != nil {
			return err
		}
	}
	if lost != nil {
		return lost
	}
	return nil
}

// IsTerminalEvent reports whether an event ends a run's stream.
func IsTerminalEvent(event *orkstackv1.Event) bool {
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		return true
	default:
		return false
	}
}
