package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
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

// PluginLister is the seam between the API and a live Orker: ListPlugins
// returns descriptors for every plugin a connected Orker can run. Tests
// substitute a fake; production uses engine.TemporalStarter.
type PluginLister interface {
	ListPlugins(ctx context.Context) ([]plugin.Descriptor, error)
}

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
	Lister   PluginLister

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

	// Webhooks holds the registered callbacks. Nil builds an empty registry,
	// so the management endpoints exist whether or not anything is wired.
	Webhooks *webhooks.Registry

	// Notifier announces run state transitions this service observes itself.
	// The engine announces every transition of a run that reached Temporal;
	// this covers the one that never did, when StartRun fails.
	Notifier engine.RunNotifier

	// Plugins is the fallback discovery set for PluginService.ListPlugins
	// when no live Orker lister is configured.
	Plugins []*orkstackv1.PluginInfo

	// Namespaces is the registry routing reads pool defaults and allow-lists
	// from. Nil serves the built-in default namespace only, which is what an
	// unconfigured Service has to work with.
	Namespaces engine.NamespaceReader

	// Pools answers whether a pool has live Orkers, so a submit onto a pool
	// nothing serves fails at submit instead of stranding a run in RUNNING.
	// Nil disables the check: there is no authority to ask, and inventing an
	// answer would either refuse every submit or vouch for every pool.
	Pools engine.PoolProbe

	// PoolPlugins reports what one pool's Orkers can run, for ListPools.
	PoolPlugins engine.PoolPluginLister

	// RawQueue is the operator escape hatch (ORK_ORKER_QUEUE): the execution
	// channel a task that names no pool is dispatched to. Empty routes the
	// default pool to its own channel, which is the zero-config behaviour.
	RawQueue string

	// Router resolves an ork namespace to the cluster serving it. Nil builds
	// the one-binding router from Reader, Starter and Pools above, which is
	// every deployment there is today.
	Router engine.NamespaceRouter

	// Logger records the decisions a submission makes that an operator would
	// otherwise have to infer, such as an empty-pool override. Nil uses the
	// process default.
	Logger *slog.Logger

	// Now and NewRunID are injectable for deterministic tests.
	Now      func() time.Time
	NewRunID func() string
}

// Service implements OrkflowService, RunService, NamespaceService and
// PluginService.
type Service struct {
	orkflows engine.OrkflowStore
	runs     *engine.RunStore
	bus      *engine.EventBus
	lister   PluginLister
	// router resolves which cluster answers for a namespace or a run. It
	// holds one binding today; every read and write path goes through it, so
	// partitioning namespaces across clusters later is configuration rather
	// than a change to this file.
	router engine.NamespaceRouter
	// pools is the routing decision: pool resolution, allow-list enforcement
	// and the liveness check, all of which happen at submit.
	pools       *engine.PoolResolver
	archive     RunArchive
	idempotency IdempotencyClaimer
	webhooks    *webhooks.Registry
	notifier    engine.RunNotifier
	plugins     []*orkstackv1.PluginInfo
	logger      *slog.Logger
	now         func() time.Time
	newRunID    func() string
}

// NewService builds a Service, filling in defaults for anything omitted.
func NewService(cfg Config) *Service {
	svc := &Service{
		orkflows:    cfg.Orkflows,
		runs:        cfg.Runs,
		bus:         cfg.Bus,
		lister:      cfg.Lister,
		router:      cfg.Router,
		archive:     cfg.Archive,
		idempotency: cfg.Idempotency,
		webhooks:    cfg.Webhooks,
		notifier:    cfg.Notifier,
		plugins:     cfg.Plugins,
		logger:      cfg.Logger,
		now:         cfg.Now,
		newRunID:    cfg.NewRunID,
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
		Plugins:    cfg.PoolPlugins,
		RawQueue:   cfg.RawQueue,
		Logger:     svc.logger,
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
	if svc.now == nil {
		svc.now = time.Now
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

// ApplyOrkflow compiles a DSL document and writes the resulting orkflow to the
// durable store, so it outlives this process.
//
// The namespace comes from the document. Selecting one per request, and
// reading one back by name, arrives with the namespace-aware API (M10.3); this
// slice makes the store, the archive index and the DSL namespace-aware, and
// every read below still asks for the default namespace — which is why a
// document naming another one is refused here rather than stored where nothing
// could reach it.
func (s *Service) ApplyOrkflow(
	ctx context.Context,
	req *connect.Request[orkstackv1.ApplyOrkflowRequest],
) (*connect.Response[orkstackv1.ApplyOrkflowResponse], error) {
	wf, err := dsl.Compile([]byte(req.Msg.GetYaml()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	// Interim guard, M10.1 -> M10.3. GetOrkflow, ListOrkflows and SubmitRun
	// below are still pinned to the default namespace, so storing under
	// another one would write an object no read path can reach and no run can
	// execute. Refusing is the honest answer until the reads catch up:
	// Unimplemented, not InvalidArgument, because the document is correct and
	// will be accepted verbatim one slice later.
	//
	// M10.3 adds the namespace to those paths and DELETES this block;
	// TestApplyOrkflow_RejectsNonDefaultNamespaceUntilM103 is what makes the
	// deletion a deliberate act.
	if ns := wf.GetNamespace(); ns != "" && ns != artifacts.DefaultNamespace {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf(
			"namespace %q cannot be applied yet: orkflow reads and run submission "+
				"are scoped to the %q namespace until M10.3", ns, artifacts.DefaultNamespace))
	}
	// Gate one of two on allowed_pools: the authoring gate, with the file in
	// hand, so an author is told now rather than at the first submit. It is
	// deliberately not the only gate — SubmitRun enforces the same rule again
	// (submitPlan), because a client can submit a definition applied when the
	// allow-list was wider, and because nothing forces a client through
	// apply at all.
	//
	// Not in the DSL compiler either: the compiler has no namespace registry
	// to consult, and it is one `curl` away from being skipped.
	ns, err := s.namespace(ctx, wf.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := engine.CheckAllowedPools(ns, wf); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	wf.CreatedAt = timestamppb.New(s.now().UTC())
	if err := s.orkflows.Put(ctx, wf); err != nil {
		return nil, orkflowStoreError(err, "store orkflow %s@%s", wf.GetName(), wf.GetVersion())
	}
	return connect.NewResponse(&orkstackv1.ApplyOrkflowResponse{Orkflow: wf}), nil
}

// GetOrkflow reads a stored orkflow; an empty version means the most recently
// applied one.
func (s *Service) GetOrkflow(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetOrkflowRequest],
) (*connect.Response[orkstackv1.GetOrkflowResponse], error) {
	if req.Msg.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	wf, err := s.orkflows.Get(ctx, artifacts.DefaultNamespace, req.Msg.GetName(), req.Msg.GetVersion())
	if err != nil {
		return nil, orkflowStoreError(err, "orkflow %q version %q",
			req.Msg.GetName(), req.Msg.GetVersion())
	}
	return connect.NewResponse(&orkstackv1.GetOrkflowResponse{Orkflow: wf}), nil
}

// ListOrkflows returns the latest version of every stored orkflow, read from
// the durable store on every call.
func (s *Service) ListOrkflows(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListOrkflowsRequest],
) (*connect.Response[orkstackv1.ListOrkflowsResponse], error) {
	orkflows, err := s.orkflows.List(ctx, artifacts.DefaultNamespace)
	if err != nil {
		return nil, orkflowStoreError(err, "list orkflows")
	}
	if size := int(req.Msg.GetPageSize()); size > 0 && size < len(orkflows) {
		orkflows = orkflows[:size]
	}
	return connect.NewResponse(&orkstackv1.ListOrkflowsResponse{Orkflows: orkflows}), nil
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
	key   string
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
// cluster (AGENTS.md §1.3). Three things happen here and nowhere else — a
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
	stored, err := s.orkflows.Get(ctx, artifacts.DefaultNamespace, name, msg.GetOrkflowVersion())
	if err != nil {
		return submission{}, orkflowStoreError(err, "orkflow %q version %q",
			name, msg.GetOrkflowVersion())
	}
	ns, err := s.namespace(ctx, stored.GetNamespace())
	if err != nil {
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

	planned, plan, err := s.resolverFor(binding).Plan(ctx, ns, stored, msg.GetAllowEmptyPool())
	if err != nil {
		return submission{}, poolPlanError(err)
	}
	return submission{
		binding: binding,
		wf:      planned,
		input:   engine.ApplyInputDefaults(planned, msg.GetInput()),
		plan:    plan,
		key:     msg.GetIdempotencyKey(),
	}, nil
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
	var empty *engine.PoolEmptyError
	if errors.As(err, &empty) {
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
	winner, err := s.idempotency.Claim(ctx, sub.key, func() (engine.Claim, error) {
		candidate = s.newPendingRun(s.newRunID(), sub.wf, sub.input)
		return newClaim(candidate)
	})
	if err != nil {
		// The claim is the dedupe authority: starting a run without it would
		// be the duplicate this milestone removes, so an unreachable authority
		// fails the submission instead.
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
	if err := s.idempotency.Rebind(ctx, sub.key, claim, discarded.GetId()); err != nil {
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
// reached Temporal, so a subscriber sees the same RUN_FAILED it would have
// seen had the run started and then failed.
func (s *Service) notifyRunFailedToStart(
	ctx context.Context,
	runID string,
	failedAt *timestamppb.Timestamp,
	cause error,
) {
	if s.notifier == nil {
		return
	}
	run, ok := s.runs.Get(runID)
	if !ok {
		return
	}
	s.notifier.NotifyRunTransition(ctx, &orkstackv1.Event{
		RunId:     runID,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		Status:    orkstackv1.Status_STATUS_FAILED,
		Message:   cause.Error(),
		Timestamp: failedAt,
	}, run)
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

// GetRun returns a run, from whichever replica the request reached.
func (s *Service) GetRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetRunRequest],
) (*connect.Response[orkstackv1.GetRunResponse], error) {
	runID := req.Msg.GetRunId()
	if runID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	run, err := s.LookupRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.GetRunResponse{Run: run}), nil
}

// ListRuns returns runs, newest first, with optional filters.
//
// The two halves are Temporal's visibility index, which holds every run that
// started and is the only source for one still executing, and the archive,
// which holds every run that finished. A run in both is reported from the
// archive: visibility knows a run's id, orkflow, status and times, while the
// archive holds its task runs, input and output, and a finished run's outcome
// is settled, so the richer record is also the correct one.
//
// Visibility is eventually consistent. A run submitted a moment ago may not be
// listed yet; it is readable by id throughout, because GetRun reads history
// rather than the index.
func (s *Service) ListRuns(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListRunsRequest],
) (*connect.Response[orkstackv1.ListRunsResponse], error) {
	size := int(req.Msg.GetPageSize())
	var (
		runs       []*orkstackv1.Run
		fromMemory bool
	)
	binding, err := s.binding(artifacts.DefaultNamespace)
	if err != nil {
		return nil, err
	}
	if binding.Reader != nil {
		listed, err := binding.Reader.ListRuns(ctx, engine.RunQuery{
			Namespace:   artifacts.DefaultNamespace,
			OrkflowName: req.Msg.GetOrkflowName(),
			Status:      req.Msg.GetStatus(),
			Limit:       size,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("list runs: %w", err))
		}
		runs = listed
	} else {
		runs = s.runs.List(engine.ListFilter{
			Namespace:   artifacts.DefaultNamespace,
			OrkflowName: req.Msg.GetOrkflowName(),
			Status:      req.Msg.GetStatus(),
		})
		fromMemory = true
	}
	if s.archive != nil {
		archived, err := s.archive.ListRuns(ctx, engine.ArchiveQuery{
			Namespace:   artifacts.DefaultNamespace,
			OrkflowName: req.Msg.GetOrkflowName(),
			Status:      req.Msg.GetStatus(),
			Limit:       size,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("list runs from archive: %w", err))
		}
		if fromMemory {
			// This process executed those runs, so its snapshots are at least
			// as fresh as the archive's.
			runs = mergeRuns(runs, archived)
		} else {
			runs = mergeRuns(archived, runs)
		}
	}
	if size > 0 && size < len(runs) {
		runs = runs[:size]
	}
	return connect.NewResponse(&orkstackv1.ListRunsResponse{Runs: runs}), nil
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
// resolved through the read model, so a replica can cancel a run it did not
// start.
func (s *Service) CancelRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.CancelRunRequest],
) (*connect.Response[orkstackv1.CancelRunResponse], error) {
	if req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	run, err := s.LookupRun(ctx, req.Msg.GetRunId())
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
	return s.StreamRunEvents(ctx, runID, req.Msg.GetFromSequence(), stream.Send)
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
// With no reader wired — a unit test — the process-local bus answers instead,
// on the same terms as before this milestone.
func (s *Service) StreamRunEvents(
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

// ListPlugins returns the plugins a live Orker can run, discovered through the
// PluginLister seam (an ExecuteTask-queue activity). When no lister is wired
// (tests, or no Orker), it falls back to the configured static set.
func (s *Service) ListPlugins(
	ctx context.Context,
	_ *connect.Request[orkstackv1.ListPluginsRequest],
) (*connect.Response[orkstackv1.ListPluginsResponse], error) {
	plugins := append([]*orkstackv1.PluginInfo(nil), s.plugins...)
	if s.lister != nil {
		descriptors, err := s.lister.ListPlugins(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("discover plugins: %w", err))
		}
		plugins = plugins[:0]
		for _, d := range descriptors {
			plugins = append(plugins, &orkstackv1.PluginInfo{
				Name:        d.Name,
				Version:     d.Version,
				Description: d.Description,
			})
		}
	}
	sort.Slice(plugins, func(i, j int) bool { return plugins[i].GetName() < plugins[j].GetName() })
	return connect.NewResponse(&orkstackv1.ListPluginsResponse{Plugins: plugins}), nil
}
