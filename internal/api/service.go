package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
	"github.com/orkstack/orkstack/pkg/webhooks"
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
	// Archive writes a finished run and its events.
	Archive(ctx context.Context, run *orkstackv1.Run, events []*orkstackv1.Event) error
	// LoadRun returns an archived run, or an error matching
	// artifacts.ErrNotFound when the run was never archived.
	LoadRun(ctx context.Context, runID string) (*orkstackv1.Run, error)
	// LoadRunEvents returns an archived run's events, in sequence order.
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

	// Activities is the deployment's activity catalog, served by
	// ActivityService.ListActivities. Nil answers with an empty catalog:
	// which images exist is engine configuration (§4.2), and an
	// unconfigured engine genuinely has none.
	Activities ActivityLister

	// Orkers answers ActivityService.DescribeOrker — the pool-liveness probe
	// (§6.5).
	Orkers OrkerDescriber

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

	// PoolOrkers describes a pool's Orkers, for ListPools' liveness report.
	PoolOrkers engine.PoolOrkerDescriber

	// RawQueue is the operator escape hatch (ORK_ORKER_QUEUE): the execution
	// channel a task that names no pool is dispatched to. Empty routes the
	// default pool to its own channel, which is the zero-config behaviour.
	RawQueue string

	// Artifacts and Limits are deployment configuration pinned into RunPlan at
	// submit. Workflow code must not read either from the process environment.
	// Zero values select the documented local backend and contract limits.
	Artifacts artifacts.Config
	Limits    engine.WorkspaceLimits

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
	// catalog and resolver are the two halves of §4.1: the catalog turns a
	// short name into a reference, and the resolver turns a reference into a
	// digest. Both live here because ApplyOrkflow is where §4.3 puts them.
	catalog  dsl.Catalog
	resolver ImageResolver
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
	attempts    artifacts.AttemptStore
	logPoll     LogPollOptions
	idempotency IdempotencyClaimer
	webhooks    *webhooks.Registry
	triggers    triggers.Store
	// audit writes the control-plane audit trail and auditStore reads it
	// back. Both are nil when no store is wired: nothing is recorded and
	// ListAuditRecords is refused.
	audit      *AuditRecorder
	auditStore artifacts.AuditStore
	notifier   engine.RunNotifier
	observer   engine.EventObserver
	admissions engine.AdmissionObserver
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
	// retention owns the deployment's maximum run retention (see
	// Config.Retention). Nil answers SystemService's retention RPCs with
	// Unimplemented.
	retention         *archive.RetentionController
	retentionEnforced bool
	transferBudget    int32
	floorThroughput   int64
	logger            *slog.Logger
	now               func() time.Time
	newRunID          func() string
}

// NewService builds a Service, filling in defaults for anything omitted.
func NewService(cfg Config) *Service {
	svc := &Service{
		orkflows:          cfg.Orkflows,
		namespaces:        cfg.Namespaces,
		rawQueue:          cfg.RawQueue,
		runs:              cfg.Runs,
		bus:               cfg.Bus,
		activities:        cfg.Activities,
		orkers:            cfg.Orkers,
		catalog:           cfg.Catalog,
		resolver:          cfg.Resolver,
		system:            cfg.System,
		telemetrySinks:    cfg.TelemetrySinks,
		router:            cfg.Router,
		archive:           cfg.Archive,
		logs:              cfg.Logs,
		artifactStore:     cfg.ArtifactStore,
		attempts:          cfg.Attempts,
		logPoll:           cfg.LogPoll,
		idempotency:       cfg.Idempotency,
		webhooks:          cfg.Webhooks,
		triggers:          cfg.Triggers,
		notifier:          cfg.Notifier,
		observer:          cfg.Observer,
		admissions:        cfg.Admissions,
		artifacts:         cfg.Artifacts,
		limits:            cfg.Limits,
		retention:         cfg.Retention,
		retentionEnforced: cfg.RetentionEnforced,
		transferBudget:    cfg.TransferBudgetSeconds,
		floorThroughput:   cfg.FloorThroughputBytesPerSecond,
		logger:            cfg.Logger,
		now:               cfg.Now,
		newRunID:          cfg.NewRunID,
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
		Orkers:     cfg.PoolOrkers,
		RawQueue:   cfg.RawQueue,
		Logger:     svc.logger,
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
// arrives as a new version — the DSL derives one from the content when the
// document pins none.
func (s *Service) ApplyOrkflow(
	ctx context.Context,
	req *connect.Request[orkstackv1.ApplyOrkflowRequest],
) (*connect.Response[orkstackv1.ApplyOrkflowResponse], error) {
	source := []byte(req.Msg.GetYaml())
	wf, ns, err := s.prepareOrkflow(ctx, source, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	expected, err := s.checkEditTokens(ctx, ns.GetName(), wf.GetName(),
		req.Msg.GetBaseVersion(), req.Msg.GetExpectedHeadVersion())
	if err != nil {
		return nil, err
	}
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
	if err != nil {
		var moved *artifacts.HeadMovedError
		if errors.As(err, &moved) {
			// The edit lost the commit, not the check: a competing apply
			// landed between the read above and the write. Same refusal, in
			// the same words — the caller cannot tell the two apart, and
			// should not have to.
			return nil, editRaceRefusal(ns.GetName(), wf.GetName(),
				req.Msg.GetBaseVersion(), req.Msg.GetExpectedHeadVersion(), moved.Head)
		}
		return nil, orkflowStoreError(err, "store orkflow %s@%s in namespace %q",
			wf.GetName(), wf.GetVersion(), ns.GetName())
	}
	return connect.NewResponse(&orkstackv1.ApplyOrkflowResponse{
		Orkflow: stored,
		Created: created,
	}), nil
}

// ValidateOrkflow performs exactly ApplyOrkflow's preparation without reading
// edit tokens or writing a definition. A document error is a returned issue;
// unavailable registries and namespace stores remain RPC failures.
func (s *Service) ValidateOrkflow(ctx context.Context, req *connect.Request[orkstackv1.ValidateOrkflowRequest]) (*connect.Response[orkstackv1.ValidateOrkflowResponse], error) {
	wf, _, err := s.prepareOrkflow(ctx, []byte(req.Msg.GetYaml()), req.Msg.GetNamespace())
	if err == nil {
		return connect.NewResponse(&orkstackv1.ValidateOrkflowResponse{
			Orkflow: wf,
			Issues:  pinUserParityWarnings(wf),
		}), nil
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
		return connect.NewResponse(&orkstackv1.ValidateOrkflowResponse{Issues: []*orkstackv1.ValidationIssue{{Field: field, Message: err.Error(), Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR}}}), nil
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
// — so this is advice and not a refusal (docs/design/task-pinning-worker-
// session.md §4.4, §12.9: a documented author responsibility, not a platform
// guarantee).
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

// prepareOrkflow is the common apply/validate pipeline. Keep all mutation
// after this function so validation cannot create a pointer or a definition.
func (s *Service) prepareOrkflow(ctx context.Context, source []byte, requestedNamespace string) (*orkstackv1.Orkflow, *orkstackv1.Namespace, error) {
	wf, err := dsl.CompileWithOptions(source, dsl.Options{Catalog: s.activityCatalog()})
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	pinsVersion, err := dsl.DocumentPinsVersion(source)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	selected, err := applyNamespace(requestedNamespace, wf.GetNamespace())
	if err != nil {
		return nil, nil, err
	}
	ns, err := s.namespace(ctx, selected)
	if err != nil {
		return nil, nil, err
	}
	if err := s.pools.CheckAllowedPools(ns, wf); err != nil {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	wf.Namespace = ns.GetName()
	if err := s.resolveImages(ctx, wf); err != nil {
		return nil, nil, applyResolveError(err)
	}
	if !pinsVersion {
		derived, err := dsl.DerivedVersion(source, wf)
		if err != nil {
			return nil, nil, connect.NewError(connect.CodeInternal, err)
		}
		wf.Version = derived
	}
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInternal, err)
	}
	wf.DefinitionDigest = digest
	return wf, ns, nil
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
//	""      ""        none — a plain `orkctl apply file.yaml`
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

// storesVersion reports whether one version is among the stored ones.
func storesVersion(versions artifacts.OrkflowVersions, version string) bool {
	for _, wf := range versions.All {
		if wf.GetVersion() == version {
			return true
		}
	}
	return false
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
// read from the durable store on every call.
//
// It is one prefix scan of that namespace, not a scan of everything followed
// by a filter: another namespace's documents are never read.
func (s *Service) ListOrkflows(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListOrkflowsRequest],
) (*connect.Response[orkstackv1.ListOrkflowsResponse], error) {
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	orkflows, err := s.orkflows.List(ctx, ns.GetName())
	if err != nil {
		return nil, orkflowStoreError(err, "list orkflows in namespace %q", ns.GetName())
	}
	size := int(req.Msg.GetPageSize())
	if size < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size cannot be negative"))
	}
	if size == 0 {
		size = engine.DefaultRunPageSize
	}
	if token := req.Msg.GetPageToken(); token != "" {
		cursor, err := decodeOrkflowCursor(token, ns.GetName())
		if err != nil {
			return nil, err
		}
		filtered := orkflows[:0]
		for _, wf := range orkflows {
			if wf.GetName() > cursor {
				filtered = append(filtered, wf)
			}
		}
		orkflows = filtered
	}
	response := &orkstackv1.ListOrkflowsResponse{}
	if len(orkflows) > size {
		response.Orkflows = orkflows[:size]
		token, err := encodeOrkflowCursor(ns.GetName(), response.Orkflows[len(response.Orkflows)-1].GetName())
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		response.NextPageToken = token
	} else {
		response.Orkflows = orkflows
	}
	return connect.NewResponse(response), nil
}

type orkflowCursor struct {
	Version   int    `json:"v"`
	Namespace string `json:"n"`
	Name      string `json:"name"`
}

func encodeOrkflowCursor(namespace, name string) (string, error) {
	raw, err := json.Marshal(orkflowCursor{Version: 1, Namespace: namespace, Name: name})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
func decodeOrkflowCursor(token, namespace string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	var c orkflowCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != 1 || c.Namespace != namespace || c.Name == "" {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_token does not match this namespace"))
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
	sub, err := s.prepare(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// A keyed submission asks the claimer which run the key names before it
	// publishes an event or calls Temporal. `ork` wires a claimer backed by
	// the cluster, so concurrent requests converge on one run whether they
	// reach one replica or several, and a failed start stays deduplicated to
	// its recorded failed run because a claim is never released.
	if sub.key != "" {
		return s.submitClaimedRun(ctx, sub)
	}

	run := s.newPendingRun(s.newRunID(), sub.wf, sub.input)
	s.runs.Save(run)
	return s.startPreparedRun(ctx, run, sub)
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
func (s *Service) prepare(
	ctx context.Context,
	msg *orkstackv1.SubmitRunRequest,
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
	if err := s.pinDurabilityPlan(&plan); err != nil {
		return submission{}, connect.NewError(connect.CodeInternal,
			fmt.Errorf("resolve durable activity configuration: %w", err))
	}
	return submission{
		binding:   binding,
		wf:        planned,
		input:     engine.ApplyInputDefaults(planned, msg.GetInput()),
		plan:      plan,
		namespace: ns.GetName(),
		key:       msg.GetIdempotencyKey(),
	}, nil
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
	// code (design §7.4): the request is well formed and the deployment is not
	// ready for it, so it is retryable once an operator sets
	// ORK_ORKER_MAX_PINS on that pool's Orkers.
	var noPinHost *engine.PoolNoPinHostError
	if errors.As(err, &noPinHost) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	var unreachable *engine.PoolUnreachableError
	if errors.As(err, &unreachable) {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewError(connect.CodeInvalidArgument, err)
}

// submitClaimedRun runs a keyed submission: claim the key, then either start
// the run this caller proposed or report the one that already holds the key.
//
// The candidate run is built inside the claim, not before it, because a
// process-local claimer can answer a known key without minting a run id at
// all — which is what keeps a repeated key from burning ids in a single-engine
// deployment. A cluster-backed claimer has to propose first; there the burnt
// id costs nothing, because nothing outside this process ever saw it.
func (s *Service) submitClaimedRun(
	ctx context.Context,
	sub submission,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	var candidate *orkstackv1.Run
	winner, err := s.idempotency.Claim(ctx, sub.claimKey(), func() (engine.Claim, error) {
		candidate = s.newPendingRun(s.newRunID(), sub.wf, sub.input)
		return newClaim(candidate)
	})
	if err != nil {
		// The claim is the dedupe authority: starting a run without it would
		// be exactly the duplicate the claim exists to prevent, so an
		// unreachable authority fails the submission instead.
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("claim idempotency key %q: %w", sub.key, err))
	}
	if candidate == nil || winner.RunID != candidate.GetId() {
		return s.claimedRun(ctx, sub.key, winner)
	}
	s.runs.Save(candidate)
	return s.startPreparedRun(ctx, candidate, sub)
}

// claimedRun answers a caller whose candidate lost, with the run that won.
//
// The run is read the same way any other read is, so a loser answers for a run
// another replica started and is still executing — which is exactly the case a
// process-local read model could not cover. The claim's own snapshot is the
// last resort, for a winner whose run has not reached the cluster yet.
func (s *Service) claimedRun(
	ctx context.Context,
	key string,
	winner engine.Claim,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	switch run, err := s.LookupRun(ctx, winner.RunID); {
	case err == nil:
		return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: run}), nil
	case connect.CodeOf(err) != connect.CodeNotFound:
		return nil, err
	}
	run, err := claimedSnapshot(winner)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("idempotency key %q refers to run %q: %w", key, winner.RunID, err))
	}
	if run == nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("idempotency key %q refers to missing run %q", key, winner.RunID))
	}
	return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: run}), nil
}

// newClaim packs a pending run into the claim that binds a key to it. The
// snapshot travels so a replica that loses the claim can answer with the
// winner's run rather than with a plausible reconstruction of its own request.
func newClaim(run *orkstackv1.Run) (engine.Claim, error) {
	snapshot, err := proto.Marshal(run)
	if err != nil {
		return engine.Claim{}, fmt.Errorf("encode run %s: %w", run.GetId(), err)
	}
	return engine.Claim{RunID: run.GetId(), Snapshot: snapshot}, nil
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

func (s *Service) newPendingRun(
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
) *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:             runID,
		Namespace:      wf.GetNamespace(),
		OrkflowName:    wf.GetName(),
		OrkflowVersion: wf.GetVersion(),
		Status:         orkstackv1.Status_STATUS_PENDING,
		Input:          input,
		CreatedAt:      timestamppb.New(s.now().UTC()),
	}
}

// startPreparedRun starts the run's Temporal workflow.
//
// It publishes no RUN_CREATED. That event is emitted by RunWorkflow itself, as
// event 1 of the run, so it reaches Temporal history and every replica can
// read it; a copy published here would exist only in this process's memory and
// would leave a history-derived stream starting at 2, with a gap at 1.
//
// A run id Temporal reports as taken is the one start failure that is not
// about this run: another replica already owns that workflow id. The candidate
// is discarded and one fresh id is tried. Only one — a second collision is a
// broken generator rather than bad luck, and a submission that kept drawing
// ids would hide it.
func (s *Service) startPreparedRun(
	ctx context.Context,
	run *orkstackv1.Run,
	sub submission,
) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	for attempt := 0; ; attempt++ {
		runID := run.GetId()
		err := sub.binding.Starter.StartRun(ctx, runID, sub.wf, sub.input, sub.plan)
		if err == nil {
			// The response is the run this call created, not a re-read of it.
			// A read here would race the run's own first events — the workflow
			// is already executing — and answer PENDING or RUNNING depending
			// on which won. What a submission has to report is what it
			// submitted; everything after that is a read.
			return connect.NewResponse(&orkstackv1.SubmitRunResponse{Run: run}), nil
		}
		if errors.Is(err, engine.ErrRunIDTaken) && attempt == 0 {
			next, reissueErr := s.reissueRunID(ctx, run, sub)
			run = next
			if reissueErr != nil {
				// The key could not be moved onto the fresh id, so starting
				// under it would leave the key naming a run that never
				// existed. The submission ends here instead.
				return nil, s.recordFailedStart(ctx, run, reissueErr)
			}
			continue
		}
		return nil, s.recordFailedStart(ctx, run, err)
	}
}

// reissueRunID discards a candidate run whose id Temporal refused and rebuilds
// it under a fresh one. Nothing outside this process has seen the discarded id
// — SubmitRun has not returned — so it is removed rather than left behind as a
// run that never existed.
//
// A keyed submission also moves its claim, and moves it *before* the fresh id
// is started, so the key names a run at every instant: first the discarded
// candidate, then the one that runs. Releasing it in between would let a
// concurrent submission of the same key start a second run.
func (s *Service) reissueRunID(
	ctx context.Context,
	discarded *orkstackv1.Run,
	sub submission,
) (*orkstackv1.Run, error) {
	s.runs.Delete(discarded.GetId())
	s.bus.Discard(discarded.GetId())

	next := s.newPendingRun(s.newRunID(), sub.wf, sub.input)
	s.runs.Save(next)
	if sub.key == "" {
		return next, nil
	}
	claim, err := newClaim(next)
	if err != nil {
		return next, err
	}
	if err := s.idempotency.Rebind(ctx, sub.claimKey(), claim, discarded.GetId()); err != nil {
		return next, fmt.Errorf("move idempotency key %q from %s to %s: %w",
			sub.key, discarded.GetId(), next.GetId(), err)
	}
	return next, nil
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
	runID := run.GetId()
	failedAt := timestamppb.New(s.now().UTC())
	createdAt := run.GetCreatedAt()
	if createdAt == nil {
		createdAt = failedAt
	}
	run.Status = orkstackv1.Status_STATUS_FAILED
	run.Error = &orkstackv1.Error{Code: "RUN_START_FAILED", Message: cause.Error()}
	run.FinishedAt = failedAt
	s.runs.Save(run)
	s.bus.Publish(&orkstackv1.Event{
		RunId:    runID,
		Sequence: 1,
		Type:     orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		Status:   orkstackv1.Status_STATUS_PENDING,
		Message: fmt.Sprintf("run created for orkflow %s@%s",
			run.GetOrkflowName(), run.GetOrkflowVersion()),
		Timestamp: createdAt,
	}, runID+"#created")
	s.bus.Publish(&orkstackv1.Event{
		RunId:     runID,
		Sequence:  2,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		Status:    orkstackv1.Status_STATUS_FAILED,
		Message:   cause.Error(),
		Timestamp: failedAt,
	}, runID+"#failed")
	s.notifyRunFailedToStart(ctx, runID, failedAt, cause)
	s.archiveRun(ctx, runID)
	return connect.NewError(connect.CodeInternal, cause)
}

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
		s.notifier.NotifyRunEvent(ctx, event, run)
	}
	if s.observer != nil {
		s.observer.ObserveEvent(ctx, event, run)
	}
}

// archiveRun writes a terminal run to the archive. Archiving is durability, not
// orchestration: a failure here must not turn a recorded outcome into a
// different error for the caller, so it is reported through the run's own
// error and otherwise dropped.
func (s *Service) archiveRun(ctx context.Context, runID string) {
	if s.archive == nil {
		return
	}
	run, ok := s.runs.Get(runID)
	if !ok {
		return
	}
	_ = s.archive.Archive(ctx, run, s.bus.Events(runID, 0))
}

// ArchivedEvents returns the events archived for a run, after fromSequence. It
// is the read path for a run this process never executed, and it reports the
// same Connect errors the live path would: NotFound when nothing was archived
// under that id, Internal when the archive itself is unreachable.
func (s *Service) ArchivedEvents(
	ctx context.Context,
	runID string,
	fromSequence int64,
) ([]*orkstackv1.Event, error) {
	if s.archive == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
	}
	events, err := s.archive.LoadRunEvents(ctx, runID)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", runID))
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("read run %q from archive: %w", runID, err))
	}
	after := make([]*orkstackv1.Event, 0, len(events))
	for _, event := range events {
		if event.GetSequence() > fromSequence {
			after = append(after, event)
		}
	}
	return after, nil
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
	if err != nil {
		return nil, err
	}
	if artifacts.ResolveNamespace(run.GetNamespace()) != namespace {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("run %q not found in namespace %q", runID, namespace))
	}
	return run, nil
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
	return connect.NewResponse(&orkstackv1.GetRunResponse{Run: run}), nil
}

// ListRuns returns runs of one namespace, newest first, with optional filters.
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
// Visibility is eventually consistent. A run submitted a moment ago may not be
// listed yet; it is readable by id throughout, because GetRun reads history
// rather than the index.
func (s *Service) ListRuns(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListRunsRequest],
) (*connect.Response[orkstackv1.ListRunsResponse], error) {
	size := int(req.Msg.GetPageSize())
	if size < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size cannot be negative"))
	}
	if size == 0 {
		size = engine.DefaultRunPageSize
	}
	since, until, err := listRunBounds(req.Msg.GetCreatedAfter(), req.Msg.GetCreatedBefore())
	if err != nil {
		return nil, err
	}
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
	cursor, err := decodeRunCursor(req.Msg.GetPageToken(), ns.GetName(), req.Msg.GetOrkflowName(), req.Msg.GetStatus(), since, until)
	if err != nil {
		return nil, err
	}
	binding, err := s.binding(ns.GetName())
	if err != nil {
		return nil, err
	}
	if binding.Reader != nil {
		beforeCreated, beforeID := time.Time{}, ""
		if cursor != nil {
			beforeCreated, beforeID = parseCursorTime(cursor.LastCreated), cursor.LastID
		}
		listed, err := binding.Reader.ListRuns(ctx, engine.RunQuery{
			Namespace:   ns.GetName(),
			OrkflowName: req.Msg.GetOrkflowName(),
			Status:      req.Msg.GetStatus(),
			// Visibility does not promise ID ordering inside equal timestamps.
			// Complete collects its bounded metadata scan before applying Limit,
			// so a later higher-ID tie cannot disappear behind this cursor.
			Since: since, Until: until, BeforeCreated: beforeCreated, BeforeID: beforeID, Limit: size + 1, Complete: true,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("list runs in namespace %q: %w", ns.GetName(), err))
		}
		runs = listed
	} else {
		runs = s.runs.List(engine.ListFilter{
			Namespace:   ns.GetName(),
			OrkflowName: req.Msg.GetOrkflowName(),
			Status:      req.Msg.GetStatus(),
		})
		runs = filterRunTimes(runs, since, until)
		if cursor != nil {
			runs = afterRunCursor(runs, cursor.LastCreated, cursor.LastID)
		}
		fromMemory = true
	}
	if s.archive != nil {
		beforeCreated, beforeID := time.Time{}, ""
		if cursor != nil {
			beforeCreated, beforeID = parseCursorTime(cursor.LastCreated), cursor.LastID
		}
		archived, err := s.archive.ListRuns(ctx, engine.ArchiveQuery{
			Namespace:   ns.GetName(),
			OrkflowName: req.Msg.GetOrkflowName(),
			Status:      req.Msg.GetStatus(),
			Since:       since, Until: until, BeforeCreated: beforeCreated, BeforeID: beforeID, Limit: size + 1,
		})
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
		token, err := encodeRunCursor(runCursor{Version: 1, Namespace: ns.GetName(), Orkflow: req.Msg.GetOrkflowName(), Status: int32(req.Msg.GetStatus()), Since: timeString(since), Until: timeString(until), LastCreated: engine.RunIndexTime(last).UTC().Format(time.RFC3339Nano), LastID: last.GetId()})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode run page token: %w", err))
		}
		response.Runs, response.NextPageToken = page, token
	} else {
		response.Runs = runs
	}
	return connect.NewResponse(response), nil
}

type runCursor struct {
	Version     int    `json:"v"`
	Namespace   string `json:"n"`
	Orkflow     string `json:"o"`
	Status      int32  `json:"s"`
	Since       string `json:"a,omitempty"`
	Until       string `json:"b,omitempty"`
	LastCreated string `json:"c"`
	LastID      string `json:"i"`
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
func listRunBounds(after, before *timestamppb.Timestamp) (time.Time, time.Time, error) {
	var a, b time.Time
	if after != nil {
		if !after.IsValid() {
			return a, b, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("created_after is invalid"))
		}
		a = after.AsTime()
	}
	if before != nil {
		if !before.IsValid() {
			return a, b, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("created_before is invalid"))
		}
		b = before.AsTime()
	}
	if !a.IsZero() && !b.IsZero() && !a.Before(b) {
		return a, b, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("created_after must be before created_before"))
	}
	return a, b, nil
}
func encodeRunCursor(c runCursor) (string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
func decodeRunCursor(token, namespace, orkflow string, status orkstackv1.Status, since, until time.Time) (*runCursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	var c runCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != 1 || c.Namespace != namespace || c.Orkflow != orkflow || c.Status != int32(status) || c.Since != timeString(since) || c.Until != timeString(until) || c.LastCreated == "" || c.LastID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_token does not match these run filters"))
	}
	if _, err := time.Parse(time.RFC3339Nano, c.LastCreated); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
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
func filterRunTimes(runs []*orkstackv1.Run, since, until time.Time) []*orkstackv1.Run {
	out := runs[:0]
	for _, run := range runs {
		t := engine.RunIndexTime(run)
		if (!since.IsZero() && t.Before(since)) || (!until.IsZero() && !t.Before(until)) {
			continue
		}
		out = append(out, run)
	}
	return out
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
	if err := binding.Starter.CancelRun(ctx, req.Msg.GetRunId(), req.Msg.GetReason()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&orkstackv1.CancelRunResponse{Run: run}), nil
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
func (s *Service) StreamRunEvents(
	ctx context.Context,
	runID string,
	namespace string,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	if _, err := s.LookupRunIn(ctx, runID, namespace); err != nil {
		return err
	}
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

	events, cancel, terminalAlreadyReached := s.bus.Subscribe(runID, fromSequence)
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
	archived, err := s.ArchivedEvents(ctx, runID, fromSequence)
	if err != nil {
		return err
	}
	for _, event := range archived {
		if err := send(event); err != nil {
			return err
		}
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
