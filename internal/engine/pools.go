package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/quantity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ErrNoNamespaceRegistry reports a namespace asked of a process that has no
// registry wired. It is not "the namespace does not exist": it is "this
// process cannot tell", which is a different answer and a different fix.
var ErrNoNamespaceRegistry = fmt.Errorf("no namespace registry is configured")

// DefaultPollerFreshness bounds how recently an Orker must have been observed
// to count as serving its pool.
//
// The window is deliberately generous, and the asymmetry is the reason.
// Temporal keeps poller records for minutes after a worker dies, so a stale
// record can still say "live" shortly after an Orker stopped — that degrades
// to a run that waits. A window too
// tight would do the opposite and refuse a healthy submit, which is a new
// failure invented by the check itself.
//
// Its floor is a LIVE worker's polling cycle, because that is how old a
// healthy Orker's record gets. Temporal stamps a poller when its long poll
// starts; an idle worker's poll is held for the cluster's long-poll interval
// (one minute by default) and can never outlast the SDK's own 70-second poll
// deadline, so a healthy record ages up to that cycle plus the moment the next
// poll takes to arrive. Measured on the dev cluster over ten minutes, an idle
// worker's record reached 60.0–63.3 seconds once a minute. A window equal to
// the cycle (it was once 60 seconds) therefore reported a healthy Orker as
// gone for a second or more every minute: a release promotion refused because
// no Orker of the release served a pool its Orker was serving, a submit
// refused into a pool that was not empty. 90 seconds is the SDK's 70-second
// ceiling plus 20 seconds of margin (TestLivePollers_AnIdleOrkerBetweenTwoPollsIsLive,
// TestIntegration_AnIdleOrkerStaysLiveAcrossItsPollingCycle).
const DefaultPollerFreshness = 90 * time.Second

// poolDescribeTimeout bounds one census read (poolCensus): the presence read
// and every live Orker's answer on its own channel. It is short: a live Orker
// answers immediately, and neither a submit nor a report is worth holding a
// request open for.
const poolDescribeTimeout = 10 * time.Second

// PoolLiveness is what the cluster says about the Orkers serving one pool.
type PoolLiveness struct {
	// Orkers is how many distinct Orkers were still polling when the question
	// was asked. A probe reports the ones inside the freshness window and no
	// others: a worker Temporal still remembers but that stopped polling is
	// not an Orker this pool has (see livePollers).
	Orkers int
	// LastSeen is the most recent moment one of them was observed.
	LastSeen time.Time
}

// Fresh reports whether this observation still counts as live at now.
//
// It is the seam's own gate rather than a repeat of the probe's: a PoolProbe
// is an interface, and an implementation that hands back a count it never
// bounded in time must not be able to vouch for a pool by saying a number.
//
// An observation with no Orkers is never fresh; one with Orkers and no
// timestamp is trusted, because a cluster that reports a poller without a
// last-access time has still reported a poller.
func (l PoolLiveness) Fresh(now time.Time, within time.Duration) bool {
	if l.Orkers <= 0 {
		return false
	}
	if l.LastSeen.IsZero() {
		return true
	}
	return now.Sub(l.LastSeen) <= within
}

// PoolProbe answers "is anything serving this pool" from the cluster.
//
// It takes the Temporal task queue rather than the pool, because it is below
// the seam: the mapping happened one layer up, in internal/task. Production
// wires TemporalPoolProbe; a test states the fact directly.
//
// The contract is a count of Orkers polling NOW: one per worker, and none the
// cluster merely remembers. An implementation that cannot honour it must
// report LastSeen so PoolLiveness.Fresh can, because everything above reads
// the number as live — a submit is refused on it and an operator sizes a pool
// by it.
type PoolProbe interface {
	ObservePool(ctx context.Context, queue string) (PoolLiveness, error)
}

// NamespaceReader is the engine's view of the namespace registry: the one
// question routing asks of it.
type NamespaceReader interface {
	Get(ctx context.Context, name string) (*orkstackv1.Namespace, error)
}

// NamespaceRegistry is the whole registry: the read routing needs, plus the
// writes the namespace API performs.
//
// It is split from NamespaceReader because the two have different audiences.
// Routing reads one document on the submit path and must not be able to write
// anything; the API's namespace surface creates, replaces and removes them.
// artifacts.S3NamespaceStore satisfies both, structurally, without pkg/
// importing internal/.
//
// Put carries artifacts.PutConditions because the arbitration between two
// replicas is the store's own: a conditional write is refused by the backend,
// so nothing above it invents a lock (AGENTS.md §7).
type NamespaceRegistry interface {
	NamespaceReader
	List(ctx context.Context) ([]*orkstackv1.Namespace, error)
	// ListNames is every registered name plus `default`, sorted, read from
	// the registry's key listing without reading a document — what a paged
	// listing walks, so a page reads only the documents it returns.
	ListNames(ctx context.Context) ([]string, error)
	Put(
		ctx context.Context,
		ns *orkstackv1.Namespace,
		cond artifacts.PutConditions,
	) (*orkstackv1.Namespace, error)
	Delete(ctx context.Context, name string) error
}

// PoolNotAllowedError reports a task routed onto a pool its namespace does not
// allow. It carries the three coordinates that make the refusal actionable:
// who forbade it, which task asked, and what it asked for.
//
// Pool is the pool the task would be DISPATCHED to, which is the only name
// worth refusing on: it is what the Orkers running the work answer to and what
// every report calls it. Requested records the name the document wrote when
// this deployment resolves it to another — `pool: main` under an engine whose
// default channel is gpu's own queue — because an operator handed the refusal
// has to be able to find the line it is about.
type PoolNotAllowedError struct {
	Namespace string
	Task      string
	Pool      string
	Requested string
	Allowed   []string
}

func (e *PoolNotAllowedError) Error() string {
	msg := fmt.Sprintf("namespace %q does not allow pool %q for task %q (allowed: %s)",
		e.Namespace, e.Pool, e.Task, strings.Join(e.Allowed, ", "))
	if e.Requested != "" && e.Requested != e.Pool {
		msg += fmt.Sprintf("; this deployment dispatches pool %q onto %q's Orkers, "+
			"so allow %q or drop the override", e.Requested, e.Pool, e.Pool)
	}
	return msg
}

// PoolEmptyError reports a submit refused because the pool it resolved to has
// no live Orkers.
//
// This is the stranded run of AGENTS.md §11.1, converted into an error at the
// moment the routing decision is made. Both remedies are named, because a
// caller who cannot act on the error is left exactly where they were.
type PoolEmptyError struct {
	Namespace string
	Pool      string
	Tasks     []string
}

func (e *PoolEmptyError) Error() string {
	return fmt.Sprintf(
		"pool %q (namespace %q, tasks: %s) has no live Orkers, so nothing would pick these tasks up. "+
			"Start one with ORK_ORKER_POOL=%s, or submit with allow_empty_pool to wait for one",
		e.Pool, e.Namespace, strings.Join(e.Tasks, ", "), e.Pool)
}

// PinPoolMismatchError reports a pin whose tasks resolved onto two different
// pools.
//
// A pin is ONE Orker process sharing ONE directory, and a session is created
// on one task queue, so a pin that spans two pools is not
// something the engine could implement — it is a document that contradicts
// itself.
//
// The compiler already refuses this as rule 10, on the LITERAL `pool:` values.
// This is the same invariant enforced again on the pool each task actually
// RESOLVED to, after the namespace default and this deployment's route
// collapse have been applied — mirroring how rule 7 is split between compiler
// and server today. The compile-time check cannot stand alone:
// it is skipped entirely by a hand-built SubmitRun that never went through the
// DSL, and this is the path such a request cannot avoid.
type PinPoolMismatchError struct {
	Namespace string
	Pin       string
	Task      string
	Pool      string
	OtherTask string
	OtherPool string
}

func (e *PinPoolMismatchError) Error() string {
	return fmt.Sprintf(
		"pin %q (namespace %q) resolves onto two pools: task %q onto %q and task %q onto %q. "+
			"A pin is one Orker process, so every task of one pin must name the same pool",
		e.Pin, e.Namespace, e.Task, e.Pool, e.OtherTask, e.OtherPool)
}

// PoolNoPinHostError reports a submit refused because the pool a pinned task
// resolved to has live Orkers, but not one of them hosts pins.
//
// It is PoolEmptyError's sibling, for the narrower question a pin asks: not
// "is anything serving this pool" but "can anything serving this pool OPEN a
// pin". Without the check the run would be accepted, the session's creation
// activity would sit on `<queue>__internal_session_creation` that nobody
// polls, and the pin's tasks would wait out the pin creation timeout before
// failing with PIN_UNAVAILABLE — a stranded run, which is exactly what a
// submit-time probe exists to prevent (docs/DEPLOY.md "Pin hosting is a pool-wide setting").
//
// Pin hosting is a POOL-WIDE property: one Orker answers the probe, and the
// documented deployment rule is that every Orker of a pool sets the same
// ORK_ORKER_MAX_PINS.
type PoolNoPinHostError struct {
	Namespace string
	Pool      string
	Pins      []string
	Tasks     []string
}

func (e *PoolNoPinHostError) Error() string {
	pins := fmt.Sprintf("pin %q", strings.Join(e.Pins, ", "))
	if len(e.Pins) > 1 {
		pins = fmt.Sprintf("pins: %s", strings.Join(e.Pins, ", "))
	}
	return fmt.Sprintf(
		"pool %q (namespace %q, %s, tasks: %s) has live Orkers but none of them hosts pins, "+
			"so the pin could never open. Start one with ORK_ORKER_POOL=%s ORK_ORKER_MAX_PINS=1, "+
			"or submit with allow_empty_pool to wait for one",
		e.Pool, e.Namespace, pins, strings.Join(e.Tasks, ", "), e.Pool)
}

// PoolNoEachSupportError refuses a submit whose fan-out — or a direct
// dependent of one — would run on a pool whose Orker predates fan-out
// (docs/DEPLOY.md §1). Such an Orker drops ExecuteTaskInput.each and
// UpstreamRef.each, which would run an item without its item or give a
// dependent no items to read: silently wrong, which AGENTS.md §14 ranks below
// explicit failure. allow_empty_pool does NOT downgrade it — that override
// exists to wait for an Orker, and this Orker is present and wrong.
type PoolNoEachSupportError struct {
	Namespace string
	Pool      string
	Tasks     []string
	OrkerID   string
	BuildID   string
	// Dependent is the first direct dependent of a fan-out on this pool, or
	// empty when only fan-outs themselves land here.
	Dependent string
}

func (e *PoolNoEachSupportError) Error() string {
	consequence := "it would run items without their item"
	if e.Dependent != "" {
		consequence += fmt.Sprintf(" and give %s no items to read", e.Dependent)
	}
	return fmt.Sprintf(
		"pool %q (namespace %q, tasks: %s) is served by Orker %q (build %q), which predates fan-out "+
			"(each:) support: %s. Upgrade the pool's Orkers before submitting this Orkflow.",
		e.Pool, e.Namespace, strings.Join(e.Tasks, ", "), e.OrkerID, e.BuildID, consequence)
}

// PoolNoCacheSupportError refuses a submit whose cached task would run on a
// pool whose Orker predates task caching (docs/DSL.md "Caching"). Such an
// Orker cannot even decode a cached task's input — the SDK's default payload
// converter refuses ExecuteTaskInput.cache as an unknown field — so it would
// fail every attempt for a reason unrelated to the task. allow_empty_pool
// does NOT downgrade it (the Orker is present and wrong), and neither does
// no_cache (a bypass still sends a CacheContext).
type PoolNoCacheSupportError struct {
	Namespace string
	Pool      string
	Tasks     []string
	OrkerID   string
	BuildID   string
}

func (e *PoolNoCacheSupportError) Error() string {
	return fmt.Sprintf(
		"pool %q (namespace %q, tasks: %s) is served by Orker %q (build %q), which predates task caching "+
			"(cache:): it cannot read a cached task's input and would fail it. Upgrade the pool's Orkers "+
			"before submitting this Orkflow.",
		e.Pool, e.Namespace, strings.Join(e.Tasks, ", "), e.OrkerID, e.BuildID)
}

// PoolNoJoinSupportError refuses a submit whose container failure handler
// (join any_failed or all_done, docs/DSL.md "Joins and failure handlers")
// would run on a pool whose Orker predates join policies. Such an Orker
// cannot even decode a failure handler's input — the SDK's default payload
// converter refuses ExecuteTaskInput.join as an unknown field — so it would
// fail the compensation step of a run that is already failing.
// allow_empty_pool does NOT downgrade it (the Orker is present and wrong).
type PoolNoJoinSupportError struct {
	Namespace string
	Pool      string
	Tasks     []string
	OrkerID   string
	BuildID   string
}

func (e *PoolNoJoinSupportError) Error() string {
	return fmt.Sprintf(
		"pool %q (namespace %q, tasks: %s) is served by Orker %q (build %q), which predates join policies "+
			"(join:): it cannot read a failure handler's input and would fail it. Upgrade the pool's Orkers "+
			"before submitting this Orkflow.",
		e.Pool, e.Namespace, strings.Join(e.Tasks, ", "), e.OrkerID, e.BuildID)
}

// ReasonResourcesUnsatisfiable refuses a submit whose task asks for more than
// any Orker of its pool runs (PoolResourcesUnsatisfiableError).
const ReasonResourcesUnsatisfiable = "RESOURCES_UNSATISFIABLE"

// PoolResourcesUnsatisfiableError refuses a submit whose task requests more
// cpu or memory than the per-task cap of every Orker of its pool that
// answered (DescribeOrkerResult.max_request). Such a task could never run
// there: an Orker refuses a request above its cap with
// RESOURCE_REQUEST_REFUSED after dispatch, terminally, so the run would fail
// on its first attempt. The cap is a pool-wide setting (docs/DEPLOY.md), so
// the refusal names the largest cap the pool reported. allow_empty_pool does
// not downgrade it: the Orkers are present, and none of them can hold it.
type PoolResourcesUnsatisfiableError struct {
	Namespace string
	Pool      string
	Task      string
	CPU       string // as the task wrote it; empty when it asked for none
	Memory    string
	Largest   *orkstackv1.ResourceQuantity
}

func (e *PoolResourcesUnsatisfiableError) Error() string {
	var asked []string
	if e.CPU != "" {
		asked = append(asked, fmt.Sprintf("cpu %s", e.CPU))
	}
	if e.Memory != "" {
		asked = append(asked, fmt.Sprintf("memory %s", e.Memory))
	}
	return fmt.Sprintf("%s: task %q requests %s, which no Orker of pool %q (namespace %q) runs: its largest "+
		"per-task caps are %g cpu and %d bytes of memory, not always on one Orker. Ask for less, or run the task "+
		"on a pool whose Orkers set larger caps (orker.resources.max_cpu, orker.resources.max_memory)",
		ReasonResourcesUnsatisfiable, e.Task, strings.Join(asked, " and "), e.Pool, e.Namespace,
		e.Largest.GetCpu(), e.Largest.GetMemoryBytes())
}

// PoolUnreachableError reports that the cluster could not be asked whether a
// pool has Orkers.
//
// It is deliberately not a PoolEmptyError. "Nobody serves this pool" is a fact
// about the deployment, and the caller acts on it by starting an Orker or
// taking the override. "Nothing answered" is not a fact about the pool at all:
// the submission may be perfectly good, and the only true instruction is to
// ask again. Collapsing the two would tell a client to fix a request that has
// nothing wrong with it, for as long as a Temporal hiccup lasts.
type PoolUnreachableError struct {
	Pool string
	Err  error
}

func (e *PoolUnreachableError) Error() string {
	return fmt.Sprintf("cannot tell whether pool %q has live Orkers: %v", e.Pool, e.Err)
}

func (e *PoolUnreachableError) Unwrap() error { return e.Err }

// PoolResolver decides where a run's tasks go, at submit.
//
// It is the whole of the routing decision and it lives outside workflow code
// on purpose: it reads a namespace document, it reads process configuration
// (RawQueue) and it asks the cluster a question, none of which a deterministic
// workflow may do (AGENTS.md §2.1). What it produces — a definition whose
// tasks all carry a pool, and a pool→channel route table — is bound into the
// workflow input and is all the workflow ever needs.
type PoolResolver struct {
	// Namespaces is the registry routing reads defaults and allow-lists from.
	// Nil serves only the built-in default namespace, which is what an
	// unconfigured process has.
	Namespaces NamespaceReader

	// Probe answers whether a pool has live Orkers. Nil disables the check:
	// there is no authority to ask, and inventing an answer would either
	// refuse every submit or vouch for every pool.
	Probe PoolProbe

	// Fleet is the Orker inventory (ReadFleet), read as a poolCensus: asked
	// whether a pool whose channel shows no poller is busy rather than empty,
	// for each capability probe a submit needs, and for the busy Orkers a pool
	// report counts. Nil asks nothing, the same reason a nil Probe asks
	// nothing: there is no authority to ask.
	Fleet FleetReader

	// RawQueue is the operator escape hatch (ORK_ORKER_QUEUE): the channel a
	// task that names no pool is dispatched to. Empty means the pool's own.
	RawQueue string

	// Freshness overrides DefaultPollerFreshness.
	Freshness time.Duration

	// Logger records the two things this type does quietly: an empty-pool
	// override taken at submit, and each source a report could not reach
	// (Pools). Both narrow an answer without failing it, so the log is the
	// only place the narrowing is recorded. Nil uses the process default.
	Logger *slog.Logger

	// Now is injectable for tests.
	Now func() time.Time
}

func (r *PoolResolver) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}

func (r *PoolResolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *PoolResolver) freshness() time.Duration {
	if r.Freshness > 0 {
		return r.Freshness
	}
	return DefaultPollerFreshness
}

// Namespace reads one namespace document. With no registry wired it answers
// for the built-in default and refuses every other name, which is the honest
// answer from a process that has no registry to read.
func (r *PoolResolver) Namespace(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	if r.Namespaces != nil {
		return r.Namespaces.Get(ctx, name)
	}
	if name == "" || name == artifacts.DefaultNamespace {
		return artifacts.DefaultNamespaceDocument(), nil
	}
	return nil, fmt.Errorf("namespace %q: %w", name, ErrNoNamespaceRegistry)
}

// PoolFor resolves one task's pool: the task's own, then the namespace's
// default, then the built-in "main". It is the resolution order, in the one
// place that implements it.
func PoolFor(ns *orkstackv1.Namespace, t *orkstackv1.Task) string {
	if pool := t.GetPool(); pool != "" {
		return pool
	}
	return DefaultPoolFor(ns)
}

// DefaultPoolFor is where a task of this namespace runs when it names no pool.
func DefaultPoolFor(ns *orkstackv1.Namespace) string {
	if pool := ns.GetDefaultPool(); pool != "" {
		return pool
	}
	return task.DefaultPool
}

// DefaultPool is where a task of this namespace runs when it names no pool,
// under THIS deployment.
//
// It is DefaultPoolFor put through the route every task takes, which is what
// makes it answerable to a client: with ORK_ORKER_POOL=gpu on the engine, a
// namespace whose default_pool is the built-in `main` dispatches its unpinned
// tasks to gpu's Orkers, so `main` is not the name to report — gpu is. The
// name and the machines have to agree, or an operator reading the report acts
// on a pool that is not the one running the work.
//
// A default_pool this engine cannot route to reports the stored name: the
// document says it, and Plan refuses the submit with the reason.
func (r *PoolResolver) DefaultPool(ns *orkstackv1.Namespace) string {
	route, err := r.routeFor(DefaultPoolFor(ns))
	if err != nil {
		return DefaultPoolFor(ns)
	}
	return route.Pool
}

// CheckAllowedPools enforces a namespace's allow-list over a definition,
// without asking the cluster anything.
//
// It is the authoring-time gate: ApplyOrkflow calls it with the file in hand,
// so an author is told at apply rather than at the first submit. It is not the
// only gate — see Plan, which cannot be bypassed by a client that never
// applied, or by a definition stored while the list was wider.
//
// It is a method, and it has to be: the list is checked in the vocabulary the
// tasks are DISPATCHED in, and that vocabulary is a property of the
// deployment. routeFor collapses a pool whose channel is this deployment's
// bound channel onto that channel's name (ORK_ORKER_POOL=gpu on the engine,
// which docs/DEPLOY.md recommends), so `pool: main` there is dispatched to,
// stamped as, and reported on `gpu`. Checking the name before the collapse
// asked about a word nothing downstream uses: it let a namespace allowing only
// `main` dispatch onto gpu's Orkers, and refused `pool: main` in a namespace
// that allows `gpu` even though gpu's Orkers are where it was going. The check
// and the dispatch are one name now.
func (r *PoolResolver) CheckAllowedPools(ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) error {
	permitted := permittedPools(ns)
	for _, t := range wf.GetTasks() {
		// An awaited task runs on no pool: nothing to allow. Nor does a
		// composed task: its child's pools are checked by checking the child.
		if t.GetAwait() != nil || t.GetOrkflow() != nil {
			continue
		}
		if err := r.checkPoolAllowed(ns, t.GetId(), PoolFor(ns, t), permitted); err != nil {
			return err
		}
	}
	return nil
}

// permittedPools is a namespace's allow-list as a set, and nil when it has
// none: an empty allow-list is no restriction, not a restriction to nothing.
func permittedPools(ns *orkstackv1.Namespace) map[string]struct{} {
	allowed := ns.GetAllowedPools()
	if len(allowed) == 0 {
		return nil
	}
	permitted := make(map[string]struct{}, len(allowed))
	for _, pool := range allowed {
		permitted[pool] = struct{}{}
	}
	return permitted
}

// checkPoolAllowed holds one task's requested pool against the allow-list, in
// the name that pool answers to under this deployment.
//
// A pool this engine cannot route at all keeps its own spelling: Plan refuses
// the submit with the routing error, and an allow-list refusal that renamed it
// would describe a pool nobody wrote.
func (r *PoolResolver) checkPoolAllowed(
	ns *orkstackv1.Namespace,
	taskID, requested string,
	permitted map[string]struct{},
) error {
	if permitted == nil {
		return nil
	}
	dispatched := r.dispatchedPool(requested)
	if _, ok := permitted[dispatched]; ok {
		return nil
	}
	return &PoolNotAllowedError{
		Namespace: namespaceName(ns),
		Task:      taskID,
		Pool:      dispatched,
		Requested: requested,
		Allowed:   append([]string(nil), ns.GetAllowedPools()...),
	}
}

// dispatchedPool is the name a requested pool answers to under this
// deployment: the pool stamped on the task, carried by every event, and
// reported by ListPools. It is routeFor's answer, and the whole reason the
// allow-list asks for it — a check on the requested name would be a check on
// a word that stops existing one line later (Plan stamps route.Pool).
//
// A pool this engine cannot route keeps its own spelling; Plan refuses that
// submit with the routing error.
func (r *PoolResolver) dispatchedPool(requested string) string {
	route, err := r.routeFor(requested)
	if err != nil {
		return requested
	}
	return route.Pool
}

// Plan resolves every task's pool, enforces the namespace's allow-list, checks
// that each resolved pool has someone to run its work, and returns the
// definition to execute together with the routes it needs.
//
// The returned orkflow is a clone with every task's pool filled in. The stored
// document is never written to: it is the user's, and where this particular
// run went is the run's.
//
// allowEmpty submits into a pool with no live Orkers anyway. It is never the
// default, and the caller logs it.
func (r *PoolResolver) Plan(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	wf *orkstackv1.Orkflow,
	allowEmpty bool,
) (*orkstackv1.Orkflow, RunPlan, error) {
	if wf == nil || len(wf.GetTasks()) == 0 {
		return nil, RunPlan{}, fmt.Errorf("orkflow has no tasks")
	}
	// The namespace's secret policy, first: it needs no pool resolved and no
	// question asked of the cluster, and this is the gate a definition stored
	// while the list was wider cannot get past (Namespace.allowed_secrets).
	if err := CheckAllowedSecrets(ns, wf); err != nil {
		return nil, RunPlan{}, err
	}
	permitted := permittedPools(ns)

	planned := proto.Clone(wf).(*orkstackv1.Orkflow)
	// The source is not what runs; the stored version keeps it
	// (NewChildRunPlan precedent). Every start — SubmitRun, the claim path
	// and a schedule's fire — begins from `planned`, so none carries the
	// YAML, and the start definition is measured without it.
	planned.SourceYaml = ""
	routes := map[string]string{}
	// Ordered, so the error a submit reports is the same one every time.
	var order []string
	tasksByPool := map[string][]string{}
	// The pin view of the same walk, in first-seen order for the same reason.
	// pinPool answers rule 10 on the RESOLVED pool; pinsByPool and
	// pinnedTasksByPool are what the pin-host refusal below names.
	pinPool := map[string]string{}
	pinFirstTask := map[string]string{}
	pinsByPool := map[string][]string{}
	pinnedTasksByPool := map[string][]string{}
	// The fan-out view: every fan-out and every DIRECT dependent of one, per
	// resolved pool — the pools whose Orker must implement fan-out.
	eachRelated, dependents := eachRelatedTasks(planned)
	eachTasksByPool := map[string][]string{}
	eachDependentByPool := map[string]string{}
	// The caching view: every cached task per resolved pool (a cached
	// fan-out's items run on the fan-out's pool) — the pools whose Orker must
	// implement the memo step.
	cachedTasksByPool := map[string][]string{}
	// The join view: every container failure handler per resolved pool — the
	// pools whose Orker must read ExecuteTaskInput.join. An awaited or
	// composed handler never reaches this loop's routing, so is never counted.
	handlerTasksByPool := map[string][]string{}
	// The sizing view: every task that declares a resource request, per
	// resolved pool — the requests a pool's per-task cap must hold. A task
	// that declares none gets its Orker's default, which every Orker's boot
	// holds within its cap, so it is never asked about.
	sizedTasksByPool := map[string][]*orkstackv1.Task{}

	for _, t := range planned.GetTasks() {
		// An awaited task is never routed: no route, no allow-list check, no
		// liveness probe, no pin bookkeeping, and Task.pool stays empty on the
		// planned definition. An Orkflow whose every task is an await plans
		// with empty Routes and never asks the cluster anything. A composed
		// task likewise: its child's pools are planned by planning the child.
		if t.GetAwait() != nil || t.GetOrkflow() != nil {
			continue
		}
		requested := PoolFor(ns, t)
		route, err := r.routeFor(requested)
		if err != nil {
			return nil, RunPlan{}, err
		}
		// The allow-list, held against the name the work is dispatched under
		// rather than the one the document wrote. The two differ exactly when
		// this deployment's bound channel collapses them, and the dispatched
		// name is what the Orkers, the events and every report answer to.
		if err := r.checkPoolAllowed(ns, t.GetId(), requested, permitted); err != nil {
			return nil, RunPlan{}, err
		}
		// The route's pool, not the requested one: routeFor collapses a pool
		// whose channel IS this deployment's bound channel onto that channel's
		// name, so the pool stamped on the task — the pool every client is
		// told, from TASK_SCHEDULED to TaskRun.pool — is the pool whose Orkers
		// actually take the work.
		t.Pool = route.Pool
		if _, seen := routes[route.Pool]; !seen {
			routes[route.Pool] = route.Queue
			order = append(order, route.Pool)
		}
		tasksByPool[route.Pool] = append(tasksByPool[route.Pool], t.GetId())
		if t.GetCache() != nil && t.GetPin() == "" {
			cachedTasksByPool[route.Pool] = append(cachedTasksByPool[route.Pool], t.GetId())
		}
		if isFailureHandler(t) {
			handlerTasksByPool[route.Pool] = append(handlerTasksByPool[route.Pool], t.GetId())
		}
		if res := t.GetResources(); strings.TrimSpace(res.GetCpu()) != "" || strings.TrimSpace(res.GetMemory()) != "" {
			sizedTasksByPool[route.Pool] = append(sizedTasksByPool[route.Pool], t)
		}
		if eachRelated[t.GetId()] {
			eachTasksByPool[route.Pool] = append(eachTasksByPool[route.Pool], t.GetId())
			if dependents[t.GetId()] && eachDependentByPool[route.Pool] == "" {
				eachDependentByPool[route.Pool] = t.GetId()
			}
		}

		// Rule 10, on the resolved pool (see PinPoolMismatchError). Every task
		// of one pin must have landed on the same pool, because a pin is one
		// process out of one pool's set.
		pin := t.GetPin()
		if pin == "" {
			continue
		}
		if first, seen := pinPool[pin]; seen {
			if first != route.Pool {
				return nil, RunPlan{}, &PinPoolMismatchError{
					Namespace: namespaceName(ns),
					Pin:       pin,
					Task:      pinFirstTask[pin],
					Pool:      first,
					OtherTask: t.GetId(),
					OtherPool: route.Pool,
				}
			}
		} else {
			pinPool[pin] = route.Pool
			pinFirstTask[pin] = t.GetId()
			pinsByPool[route.Pool] = append(pinsByPool[route.Pool], pin)
		}
		pinnedTasksByPool[route.Pool] = append(pinnedTasksByPool[route.Pool], t.GetId())
	}

	plan := RunPlan{Namespace: namespaceName(ns), Routes: routes, Fairness: fairnessFor(ns)}
	telemetry, err := telemetryFor(ns, wf)
	if err != nil {
		return nil, RunPlan{}, err
	}
	plan.Telemetry = telemetry
	if r.Probe == nil {
		return planned, plan, nil
	}
	// One census for the whole submit, read only if a question below needs
	// it, so a submit whose pools' channels are polled and that needs no
	// capability probe asks the inventory nothing.
	census := newPoolCensus(r.Fleet)
	for _, pool := range order {
		live, err := r.Probe.ObservePool(ctx, routes[pool])
		if err != nil {
			// Not an empty pool: nothing was measured. The distinction
			// survives all the way to the client, where it is the difference
			// between "start an Orker" and "try again".
			return nil, RunPlan{}, &PoolUnreachableError{Pool: pool, Err: err}
		}
		present, err := r.present(ctx, census, pool, live)
		if err != nil {
			if !allowEmpty {
				return nil, RunPlan{}, &PoolUnreachableError{Pool: pool, Err: err}
			}
			// The channel WAS measured, and is empty; only whether its
			// Orkers are busy is unknown, and the override accepts the pool
			// either way. The gap goes on the record beside the override.
			r.logger().Warn("submit: whether a pool with no poller is busy is unknown, "+
				"and allow_empty_pool accepts it either way",
				"namespace", namespaceName(ns),
				"pool", pool,
				"orkflow", wf.GetName(),
				"error", err)
		}
		if present {
			// A live pool that carries a pinned task is asked one more
			// question: can anything here OPEN a pin. Asked only of a
			// live pool with a pinned task, so an unpinned submit runs no
			// describe call it did not run before pins existed, and a deserted pool
			// has already failed below with PoolEmptyError — what its Orkers
			// host is moot when it has none.
			if err := r.checkPinHost(ctx, census, checkPinHostRequest{
				namespace:  namespaceName(ns),
				orkflow:    wf.GetName(),
				pool:       pool,
				pins:       pinsByPool[pool],
				tasks:      pinnedTasksByPool[pool],
				allowEmpty: allowEmpty,
			}); err != nil {
				return nil, RunPlan{}, err
			}
			// And, for a pool that carries a fan-out or a direct dependent of
			// one: does its Orker implement fan-out at all.
			if err := r.checkEachSupport(ctx, census, namespaceName(ns), wf.GetName(), pool,
				eachTasksByPool[pool], eachDependentByPool[pool]); err != nil {
				return nil, RunPlan{}, err
			}
			// And, for a pool that carries a cached task: does its Orker
			// implement the memo step at all.
			if err := r.checkCacheSupport(ctx, census, namespaceName(ns), wf.GetName(), pool,
				cachedTasksByPool[pool]); err != nil {
				return nil, RunPlan{}, err
			}
			// And, for a pool that carries a container failure handler: does
			// its Orker read join policies at all.
			if err := r.checkJoinSupport(ctx, census, namespaceName(ns), wf.GetName(), pool,
				handlerTasksByPool[pool]); err != nil {
				return nil, RunPlan{}, err
			}
			// And, for a pool that carries a task declaring a request: can
			// any of its Orkers hold it.
			if err := r.checkResourcesFit(ctx, census, namespaceName(ns), wf.GetName(), pool,
				sizedTasksByPool[pool]); err != nil {
				return nil, RunPlan{}, err
			}
			continue
		}
		empty := &PoolEmptyError{
			Namespace: namespaceName(ns),
			Pool:      pool,
			Tasks:     tasksByPool[pool],
		}
		if unanswered := census.unansweredCount(); unanswered > 0 {
			// An Orker that did not answer said nothing about its pool, so
			// it is not counted; the decision is recorded with that gap.
			r.logger().Warn("submit: a pool reads empty while some live Orkers did not "+
				"answer when asked, so they were not counted",
				"namespace", empty.Namespace,
				"pool", empty.Pool,
				"orkflow", wf.GetName(),
				"unanswered", unanswered)
		}
		if !allowEmpty {
			return nil, RunPlan{}, empty
		}
		// The override is taken, so the pool is still checked and the fact is
		// on the record. An override that suppressed the question would leave
		// the operator with a run in RUNNING and nothing to explain it.
		r.logger().Warn("submit into empty pool",
			"namespace", empty.Namespace,
			"pool", empty.Pool,
			"orkflow", wf.GetName(),
			"tasks", strings.Join(empty.Tasks, ","),
			"override", "allow_empty_pool")
	}
	return planned, plan, nil
}

// present decides whether a pool has an Orker that would pick its work up.
//
// The pool's own channel answers first: a poller inside the freshness window
// is an Orker with a free slot, and that is the whole of the question for an
// idle pool, so such a submit asks nothing more. A channel with no poller is
// also what a pool whose every Orker is busy looks like (poolCensus), so it is
// asked of the inventory before the pool is called empty: an Orker of the pool
// that answered on its own channel and is not cordoned keeps it live.
//
// Only a failed presence read is an error — nothing was measured, the same
// verdict a failed channel read gets. An Orker that does not answer says
// nothing about which pool it serves, so it is not counted, and Plan records
// that gap when the pool then reads empty. Calling such a pool "cannot tell"
// instead would be wrong far more often than right: a stopped Orker's
// presence record stays fresh for DefaultPollerFreshness and never answers,
// so every restart or scale-down anywhere in the deployment would turn a
// genuinely empty pool's "start an Orker" into "ask again" for that long. A
// busy Orker answers on its own channel within milliseconds on a healthy
// cluster, so the case it would cover is a busy pool on a cluster too slow to
// answer a describe in orkerDescribeTimeout, which reads empty exactly as it
// did before the census existed.
func (r *PoolResolver) present(ctx context.Context, census *poolCensus, pool string, live PoolLiveness) (bool, error) {
	if live.Fresh(r.now(), r.freshness()) {
		return true, nil
	}
	if r.Fleet == nil {
		return false, nil
	}
	orkers, err := census.taking(ctx, pool)
	if err != nil {
		return false, err
	}
	return len(orkers) > 0, nil
}

// probeOrkers is a live pool's Orkers for one capability probe: the census's
// Orkers of the pool that would take a new task, each of which answered on
// its own channel. That channel has its own slots, so a pool whose every task
// slot is taken still answers.
//
// Nil means there is nothing to judge, and the probe declines to refuse:
// nothing was measured. Either the inventory could not be read, or no Orker
// of the pool answered — it did not answer in time, it is cordoned, or the
// pool's only pollers are not Orkers the inventory lists. Each is logged with
// what the probe could not learn, because a narrowed answer that says nothing
// is indistinguishable from a complete one.
func (r *PoolResolver) probeOrkers(ctx context.Context, census *poolCensus, probe capabilityProbe) []*orkstackv1.DescribeOrkerResult {
	orkers, err := census.taking(ctx, probe.pool)
	if err == nil && len(orkers) > 0 {
		described := make([]*orkstackv1.DescribeOrkerResult, len(orkers))
		for i, orker := range orkers {
			described[i] = orker.Described
		}
		return described
	}
	attrs := []any{
		"namespace", probe.namespace,
		"pool", probe.pool,
		"orkflow", probe.orkflow,
		"tasks", strings.Join(probe.tasks, ","),
		"unanswered", census.unansweredCount(),
	}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	r.logger().Warn("submit: no uncordoned Orker of a live pool with "+probe.carrying+
		" answered on its own channel, so whether "+probe.question+" is unknown", attrs...)
	return nil
}

// capabilityProbe is one capability question asked of one live pool: who
// asked, for which tasks, and the words its log line uses when nothing could
// answer it.
type capabilityProbe struct {
	namespace string
	orkflow   string
	pool      string
	tasks     []string
	carrying  string
	question  string
}

// checkPinHostRequest is one pool's pin question, gathered while Plan walked
// the tasks. It is a struct rather than seven parameters because every field
// is a coordinate of the same refusal message.
type checkPinHostRequest struct {
	namespace  string
	orkflow    string
	pool       string
	pins       []string
	tasks      []string
	allowEmpty bool
}

// checkPinHost refuses a live pool that hosts no pins but was asked to.
//
// It asks the pool's Orkers, each on its own control channel (probeOrkers),
// and reads DescribeOrkerResult.max_concurrent_pins — each Orker's
// ORK_ORKER_MAX_PINS. Zero means the process starts no session pollers at
// all, so the pin's session could never be created there; one Orker above
// zero is a host the pin can open on.
//
// Three ways it declines to refuse, and each is a different fact:
//
//   - No pinned task on this pool: there is no question to ask, and asking
//     would add a census read to an unpinned submit.
//   - No fleet wired (r.Fleet == nil): no authority to ask, the same reason a
//     nil Probe disables the liveness check rather than inventing an answer.
//     A process with no seam must not refuse every pinned submit.
//   - No Orker of the pool answered: nothing was MEASURED. The pool's
//     liveness was, and stands; only the Orkers' self-descriptions were lost,
//     which is how Pools() treats the same failure. Turning that into a
//     refused submit would invent a new failure, and the in-workflow backstop
//     already covers a pool that genuinely cannot open a pin: the session's
//     creation activity times out at the run's pin creation timeout and the
//     task fails with PIN_UNAVAILABLE.
//
// allowEmpty downgrades a real refusal to a warning exactly as it does for an
// empty pool: the operator who overrides has been told what they are waiting
// for.
func (r *PoolResolver) checkPinHost(ctx context.Context, census *poolCensus, req checkPinHostRequest) error {
	if len(req.pins) == 0 || r.Fleet == nil {
		return nil
	}
	orkers := r.probeOrkers(ctx, census, capabilityProbe{
		namespace: req.namespace, orkflow: req.orkflow, pool: req.pool, tasks: req.tasks,
		carrying: "a pinned task", question: "it hosts pins",
	})
	if orkers == nil {
		return nil
	}
	for _, described := range orkers {
		if described.GetMaxConcurrentPins() > 0 {
			return nil
		}
	}
	noHost := &PoolNoPinHostError{
		Namespace: req.namespace,
		Pool:      req.pool,
		Pins:      req.pins,
		Tasks:     req.tasks,
	}
	if !req.allowEmpty {
		return noHost
	}
	r.logger().Warn("submit into a pool with no pin host",
		"namespace", noHost.Namespace,
		"pool", noHost.Pool,
		"orkflow", req.orkflow,
		"pins", strings.Join(noHost.Pins, ","),
		"tasks", strings.Join(noHost.Tasks, ","),
		"override", "allow_empty_pool")
	return nil
}

// eachRelatedTasks is the set of fan-out tasks and of their direct
// dependents (tasks naming a fan-out in their own needs:), and the subset that
// are dependents. An awaited task is never routed, so it is never in either.
func eachRelatedTasks(wf *orkstackv1.Orkflow) (related, dependents map[string]bool) {
	related, dependents = map[string]bool{}, map[string]bool{}
	fanOut := map[string]bool{}
	for _, t := range wf.GetTasks() {
		if t.GetEach() != nil {
			fanOut[t.GetId()] = true
			related[t.GetId()] = true
		}
	}
	if len(fanOut) == 0 {
		return related, dependents
	}
	for _, t := range wf.GetTasks() {
		if t.GetAwait() != nil || t.GetOrkflow() != nil {
			continue
		}
		for _, need := range t.GetNeeds() {
			if fanOut[need] {
				related[t.GetId()] = true
				dependents[t.GetId()] = true
				break
			}
		}
	}
	return related, dependents
}

// lacking is the first of a pool's Orkers, in identity order, that does not
// have a capability: any one of them may take the task, so every one must
// have it. Nil when all of them do.
func lacking(orkers []*orkstackv1.DescribeOrkerResult, has func(*orkstackv1.DescribeOrkerResult) bool) *orkstackv1.DescribeOrkerResult {
	for _, described := range orkers {
		if !has(described) {
			return described
		}
	}
	return nil
}

// checkEachSupport refuses a live pool with an Orker that predates fan-out
// when a fan-out, or a direct dependent of one, would run there. Asked only
// of such a pool, so a submit with no fan-out reads no census for it. No
// Orker answering is logged and not refused — nothing was measured — on
// checkPinHost's precedent; allow_empty_pool never downgrades it.
func (r *PoolResolver) checkEachSupport(ctx context.Context, census *poolCensus, namespace, orkflow, pool string, tasks []string, dependent string) error {
	if len(tasks) == 0 || r.Fleet == nil {
		return nil
	}
	old := lacking(r.probeOrkers(ctx, census, capabilityProbe{
		namespace: namespace, orkflow: orkflow, pool: pool, tasks: tasks,
		carrying: "a fan-out", question: "it supports fan-out",
	}), (*orkstackv1.DescribeOrkerResult).GetEachSupported)
	if old == nil {
		return nil
	}
	return &PoolNoEachSupportError{
		Namespace: namespace,
		Pool:      pool,
		Tasks:     tasks,
		OrkerID:   old.GetOrkerId(),
		BuildID:   old.GetBuildId(),
		Dependent: dependent,
	}
}

// checkCacheSupport refuses a live pool with an Orker that predates task
// caching when a cached task would run there. Asked only of such a pool, so a
// submit with no cached task reads no census for it. No Orker answering is
// logged and not refused — nothing was measured — on checkPinHost's
// precedent; allow_empty_pool never downgrades it.
func (r *PoolResolver) checkCacheSupport(ctx context.Context, census *poolCensus, namespace, orkflow, pool string, tasks []string) error {
	if len(tasks) == 0 || r.Fleet == nil {
		return nil
	}
	old := lacking(r.probeOrkers(ctx, census, capabilityProbe{
		namespace: namespace, orkflow: orkflow, pool: pool, tasks: tasks,
		carrying: "a cached task", question: "it supports caching",
	}), (*orkstackv1.DescribeOrkerResult).GetCacheSupported)
	if old == nil {
		return nil
	}
	return &PoolNoCacheSupportError{
		Namespace: namespace,
		Pool:      pool,
		Tasks:     tasks,
		OrkerID:   old.GetOrkerId(),
		BuildID:   old.GetBuildId(),
	}
}

// checkJoinSupport is checkCacheSupport for failure handlers: asked only of a
// live pool that hosts a container failure handler, so a submit without one
// reads no census for it. No Orker answering is logged and not refused —
// nothing was measured.
func (r *PoolResolver) checkJoinSupport(ctx context.Context, census *poolCensus, namespace, orkflow, pool string, tasks []string) error {
	if len(tasks) == 0 || r.Fleet == nil {
		return nil
	}
	old := lacking(r.probeOrkers(ctx, census, capabilityProbe{
		namespace: namespace, orkflow: orkflow, pool: pool, tasks: tasks,
		carrying: "a failure handler", question: "it reads join policies",
	}), (*orkstackv1.DescribeOrkerResult).GetJoinSupported)
	if old == nil {
		return nil
	}
	return &PoolNoJoinSupportError{
		Namespace: namespace,
		Pool:      pool,
		Tasks:     tasks,
		OrkerID:   old.GetOrkerId(),
		BuildID:   old.GetBuildId(),
	}
}

// checkResourcesFit refuses a live pool none of whose Orkers could hold a
// task's request: each declared dimension above every answering Orker's
// per-task cap (DescribeOrkerResult.max_request). Asked only of a pool that
// carries a task declaring a request, so any other submit reads no census for
// it. No Orker answering, and an Orker built before it reported its cap, are
// logged and not refused — nothing was measured — on checkPinHost's
// precedent; allow_empty_pool never downgrades it.
func (r *PoolResolver) checkResourcesFit(ctx context.Context, census *poolCensus, namespace, orkflow, pool string, tasks []*orkstackv1.Task) error {
	if len(tasks) == 0 || r.Fleet == nil {
		return nil
	}
	ids := make([]string, len(tasks))
	for i, t := range tasks {
		ids[i] = t.GetId()
	}
	orkers := r.probeOrkers(ctx, census, capabilityProbe{
		namespace: namespace, orkflow: orkflow, pool: pool, tasks: ids,
		carrying: "a sized task", question: "it can hold the request",
	})
	var caps []*orkstackv1.ResourceQuantity
	for _, described := range orkers {
		if c := described.GetMaxRequest(); c != nil {
			caps = append(caps, c)
		}
	}
	if len(caps) == 0 {
		if len(orkers) > 0 {
			r.logger().Warn("submit: no Orker of a live pool with a sized task reports its per-task cap, "+
				"so whether it can hold the request is unknown",
				"namespace", namespace, "pool", pool, "orkflow", orkflow, "tasks", strings.Join(ids, ","))
		}
		return nil
	}
	largest := &orkstackv1.ResourceQuantity{}
	for _, c := range caps {
		largest.Cpu = max(largest.Cpu, c.GetCpu())
		largest.MemoryBytes = max(largest.MemoryBytes, c.GetMemoryBytes())
	}
	for _, t := range tasks {
		cpu := strings.TrimSpace(t.GetResources().GetCpu())
		memory := strings.TrimSpace(t.GetResources().GetMemory())
		if requestFitsOne(cpu, memory, caps) {
			continue
		}
		return &PoolResourcesUnsatisfiableError{
			Namespace: namespace, Pool: pool, Task: t.GetId(), CPU: cpu, Memory: memory, Largest: largest,
		}
	}
	return nil
}

// requestFitsOne reports whether some cap holds every declared dimension of a
// request. A dimension that does not read is left to the Orker, which refuses
// it at dispatch by name; validate already refused it for an applied document.
func requestFitsOne(cpu, memory string, caps []*orkstackv1.ResourceQuantity) bool {
	cores, cpuErr := quantity.CPU(cpu)
	bytes, memErr := quantity.Memory(memory)
	for _, c := range caps {
		cpuFits := cpu == "" || cpuErr != nil || cores <= c.GetCpu()
		memFits := memory == "" || memErr != nil || bytes <= float64(c.GetMemoryBytes())
		if cpuFits && memFits {
			return true
		}
	}
	return false
}

// routeFor resolves one pool against the channel this deployment bound as its
// default, and is the single mapping for BOTH questions this type answers:
// where a pool's work is dispatched (Plan), and where its Orkers are looked
// for (Pools). Asked through two mappings they can disagree inside one
// process — a submit accepted onto the operator's channel while the report
// calls that same pool deserted, which is a worse answer than either alone.
//
// It is one call to task.RouteForPool, which is where the collapse lives: a
// pool whose queue IS the bound channel is reported under the name that
// channel answers to. Resolving the two halves here instead is how one set of
// Orkers came to be reported as two pools — `ORK_ORKER_POOL=gpu` on the
// engine binds ork-pool-gpu, and `main` and `gpu` then both dispatch there
// while ListPools counted the same Orkers twice and a task that named no pool
// was reported running on `main`.
//
// RawQueue is the operator escape hatch (ORK_ORKER_QUEUE, or the pool
// ORK_ORKER_POOL names). It moves the DEFAULT route only: a task that named a
// pool keeps its own, or `pool: gpu` would silently run wherever an unrelated
// environment variable pointed — the failure the pool name exists to prevent.
func (r *PoolResolver) routeFor(pool string) (task.Route, error) {
	return task.RouteForPool(pool, r.RawQueue)
}

// Pools reports the pools a namespace may dispatch onto, each with the Orkers
// serving it now.
//
// Scope: the namespace's allow-list when it has one, otherwise its default
// plus whatever extra pools the caller knows about (the pools its orkflows
// name). Everything comes from the cluster, so any replica answers — including
// one that started nothing.
//
// Each pool is looked for on the channel a submit would dispatch it to — the
// operator's raw channel included, since that is where its Orkers are when one
// is configured. See routeFor.
//
// A pool is listed once. Two names that resolve to one channel are one set of
// Orkers, and routeFor collapses them onto the name that channel answers to
// before anything is counted: reporting `main` and `gpu` each with the same
// two Orkers would be a report a client cannot act on — two pools to size, two
// pools to drain, one set of machines.
//
// IT NARROWS RATHER THAN REFUSING. A pool the cluster does not answer for
// keeps its row and carries UnavailableReason instead of a count; an inventory
// that cannot be read, or an Orker that does not answer in it, narrows a
// pool's count to its channel's. Both are logged with the cause, because a
// narrowed answer that says nothing is indistinguishable from a complete one.
//
// The alternative was one error for the whole call, and it was wrong twice
// over. An operator opens pool health BECAUSE part of the deployment is
// misbehaving, and a refusal hid every healthy pool beside the broken one. And
// the two states need opposite reactions: "no Orkers" is fixed by starting
// one, "could not be asked" by asking again. Reporting the second as a zero
// would send an operator to scale a pool that may be perfectly healthy, which
// is the same distinction PoolUnreachableError keeps on the submit path.
func (r *PoolResolver) Pools(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	extra []string,
) ([]*orkstackv1.Pool, error) {
	candidates := []string{DefaultPoolFor(ns)}
	candidates = append(candidates, ns.GetAllowedPools()...)
	if len(ns.GetAllowedPools()) == 0 {
		candidates = append(candidates, extra...)
	}

	defaultRoute, err := r.routeFor(DefaultPoolFor(ns))
	if err != nil {
		// The namespace's own default cannot be routed, so nothing in this
		// namespace can run. Say so rather than reporting a catalogue of
		// pools around a hole.
		return nil, err
	}

	// name -> channel, collapsed. An unroutable name keeps its own spelling
	// and no channel: a namespace document can hold a pool this engine cannot
	// map, and the operator asked what is out there.
	queues := map[string]string{}
	var ordered []string
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		name := candidate
		queue := ""
		if route, err := r.routeFor(candidate); err == nil {
			name, queue = route.Pool, route.Queue
		}
		if _, seen := queues[name]; seen {
			continue
		}
		queues[name] = queue
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)

	// One cluster round-trip per pool, in flight together and bounded (see
	// poolProbeFanout), and one census for the whole report. Each call writes
	// only its own slot, so the report is in name order whatever order the
	// answers arrive in.
	census := newPoolCensus(r.Fleet)
	pools := make([]*orkstackv1.Pool, len(ordered))
	probeTogether(len(ordered), func(i int) {
		name := ordered[i]
		pools[i] = r.observedPool(ctx, ns, census, name, queues[name], name == defaultRoute.Pool)
	})
	// Once for the report, not once per pool: the gap is the inventory's.
	if census.err != nil {
		r.logger().Warn("list pools: the orker inventory could not be read, so busy "+
			"Orkers are not counted", "namespace", namespaceName(ns), "error", census.err)
	} else if unanswered := census.unansweredCount(); unanswered > 0 {
		r.logger().Warn("list pools: some live Orkers did not answer on their own "+
			"channels, so they are not counted", "namespace", namespaceName(ns), "unanswered", unanswered)
	}
	return pools, nil
}

// observedPool is one row of Pools: the pool, asked of the cluster on its
// channel and of the census. A pool with no channel (a name this engine cannot
// route) is reported as it is, unasked.
func (r *PoolResolver) observedPool(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	census *poolCensus,
	name, queue string,
	isDefault bool,
) *orkstackv1.Pool {
	pool := &orkstackv1.Pool{Name: name, IsDefault: isDefault}
	if queue == "" {
		return pool
	}
	if r.Probe != nil {
		live, err := r.Probe.ObservePool(ctx, queue)
		switch {
		case err != nil:
			// NOT a zero count: nothing was measured. The row survives
			// carrying the reason, so a reader is told "could not ask"
			// rather than "nobody is there" — one is fixed by waiting,
			// the other by starting an Orker.
			unreachable := &PoolUnreachableError{Pool: name, Err: err}
			pool.UnavailableReason = unreachable.Error()
			r.logger().Warn("list pools: a pool could not be probed, so it is "+
				"reported unavailable rather than empty",
				"namespace", namespaceName(ns),
				"pool", name,
				"error", err)
			return pool
		case live.Fresh(r.now(), r.freshness()):
			pool.LiveOrkerCount = int32(live.Orkers)
			if !live.LastSeen.IsZero() {
				pool.LastSeen = timestamppb.New(live.LastSeen.UTC())
			}
		}
	}
	if r.Fleet == nil {
		return pool
	}
	// The channel counts the Orkers with a free slot; the census also counts
	// the busy ones (poolCensus), the same Orkers a submit counts. The larger
	// count is reported, because each source misses Orkers the other sees: a
	// channel's poller that is not an Orker the inventory lists, a busy Orker
	// the channel no longer shows. A census that could not be read narrows the
	// row to the channel's count, and Pools logs it once.
	orkers, err := census.taking(ctx, name)
	if err != nil || len(orkers) <= int(pool.GetLiveOrkerCount()) {
		return pool
	}
	pool.LiveOrkerCount = int32(len(orkers))
	for _, orker := range orkers {
		if seen := orker.Presence.LastSeen; !seen.IsZero() &&
			(pool.LastSeen == nil || seen.After(pool.GetLastSeen().AsTime())) {
			pool.LastSeen = timestamppb.New(seen.UTC())
		}
	}
	return pool
}

// poolProbeFanout bounds how many pool probes one Pools report has in flight
// at once — the same bound internal/api's storeFanout gives every other
// admin read's round-trips, for the same reason.
//
// An unrestricted namespace's report covers every pool its orkflows have ever
// named, so a serial walk costs one cluster round-trip per pool: 277 pools
// against a loaded cluster took two minutes, and every caller with a shorter
// deadline was told "context canceled" for each pool not yet reached. In
// flight together the cost is round-trips DIVIDED by this number; the bound
// keeps one report from opening a request per pool at once.
const poolProbeFanout = 16

// probeTogether runs fn for every index below n, at most poolProbeFanout at a
// time, and returns once every call has finished. fn must write only to the
// slot its index owns.
func probeTogether(n int, fn func(i int)) {
	limit := min(n, poolProbeFanout)
	if limit <= 1 {
		for i := 0; i < n; i++ {
			fn(i)
		}
		return
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}()
	}
	wg.Wait()
}

// maxExportedLabels bounds how many of an orkflow's `labels:` telemetryFor
// exports, taken in sorted-key order. Labels themselves are free-form and
// unbounded at the DSL layer (docs/DSL.md) — this is the export boundary's
// own limit, chosen so the namespace's attributes (already held to 16 by
// internal/api and pkg/artifacts's write-path validation) plus the exported
// labels can never together approach the 256 KiB ExecuteTaskInput wire cap
// (AGENTS.md §2.2), regardless of how many labels an orkflow document
// declares.
const maxExportedLabels = 32

// telemetryFor decides a run's telemetry selection at SUBMIT, from the
// namespace's TelemetryPolicy and the orkflow's own labels. It lives here
// rather than in workflow code for the reason CommitRoot does (RunPlan): which
// sink a namespace chose is registry state a deterministic workflow may not
// read.
//
// Nil, not a zero-value selection, when the namespace set no policy and the
// orkflow carries no labels — this is what lets ExecuteTaskInput.Telemetry and
// Run.Telemetry stay unset for such runs, and the golden history fixture
// (internal/engine/testdata/history) match without re-recording.
//
// Attributes are the union of the namespace's own (stamped ork.ns.<key>) and
// the orkflow's labels (stamped ork.label.<key>): the namespace's operator
// and the orkflow's author both get to add signal, under keys that cannot
// collide with each other by construction. On a collision within one side —
// there is none across sides — the namespace's value wins, because the
// operator's telemetry policy is deployment state and the orkflow is the
// tenant's own document; the operator's word about their own deployment
// outranks it.
//
// Labels are free-form (docs/DSL.md does not shape or bound them), so this is
// also the boundary that makes them safe as telemetry: at most
// maxExportedLabels of them, taken in sorted-key order so which labels
// survive a large document is deterministic and not iteration-order noise;
// each key folded into the attribute alphabet by dsl.SanitizeAttributeKey
// (lowercased, non-alphabet bytes replaced, a non-letter first character
// prefixed, capped at 64 bytes) and each value by dsl.SanitizeAttributeValue
// (truncated to 256 bytes, control characters stripped). Two distinct label
// keys can fold to the same sanitized key (`Team` and `team`, say); the
// first one in sorted order wins and the rest are dropped rather than
// silently overwriting it later in a way that depends on map iteration
// order.
func telemetryFor(ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) (*TelemetrySelection, error) {
	policy := ns.GetTelemetry()
	labels := wf.GetLabels()
	if policy == nil && len(labels) == 0 {
		return nil, nil
	}

	exportedLabels := len(labels)
	if exportedLabels > maxExportedLabels {
		exportedLabels = maxExportedLabels
	}
	attributes := make(map[string]string, len(policy.GetAttributes())+exportedLabels)
	if len(labels) > 0 {
		keys := make([]string, 0, len(labels))
		for key := range labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if len(keys) > maxExportedLabels {
			keys = keys[:maxExportedLabels]
		}
		for _, key := range keys {
			sanitized := "ork.label." + dsl.SanitizeAttributeKey(key)
			if _, collision := attributes[sanitized]; collision {
				continue
			}
			attributes[sanitized] = dsl.SanitizeAttributeValue(labels[key])
		}
	}
	// After the labels, so the namespace's own attributes win any collision
	// (there can be one only between the two prefixes' set of keys mapping to
	// the same string, which does not happen here since the prefixes differ —
	// stated anyway because it is the rule a reader should not have to derive
	// from iteration order).
	for key, value := range policy.GetAttributes() {
		attributes["ork.ns."+key] = value
	}
	if len(attributes) == 0 {
		attributes = nil
	}

	return &TelemetrySelection{
		Sink:       policy.GetSink(),
		TaskLogs:   policy.GetTaskLogs().String(),
		Attributes: attributes,
	}, nil
}

func namespaceName(ns *orkstackv1.Namespace) string {
	if name := ns.GetName(); name != "" {
		return name
	}
	return artifacts.DefaultNamespace
}
