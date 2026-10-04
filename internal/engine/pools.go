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
const DefaultPollerFreshness = 60 * time.Second

// poolDescribeTimeout bounds one DescribeOrker probe. It is short: a pool with
// live Orkers answers immediately, and a report is not worth holding a request
// open for.
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

// PoolOrkerDescriber reports the identity of an Orker serving one pool. It is
// asked only of a pool that has live Orkers — a pool with none has nobody to
// answer, and asking would only time out.
//
// Answering at all is the liveness evidence the inventory wants (§6.5).
type PoolOrkerDescriber interface {
	OrkerOnPool(ctx context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error)
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
// so nothing above it invents a lock (design §5, AGENTS.md §7).
type NamespaceRegistry interface {
	NamespaceReader
	List(ctx context.Context) ([]*orkstackv1.Namespace, error)
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
// on one task queue (design §4.3), so a pin that spans two pools is not
// something the engine could implement — it is a document that contradicts
// itself.
//
// The compiler already refuses this as rule 10, on the LITERAL `pool:` values.
// This is the same invariant enforced again on the pool each task actually
// RESOLVED to, after the namespace default and this deployment's route
// collapse have been applied — mirroring how rule 7 is split between compiler
// and server today (design §4.3). The compile-time check cannot stand alone:
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
// polls, and the pin's tasks would wait out pinCreationTimeout before failing
// with PIN_UNAVAILABLE — a stranded run, which is exactly what a submit-time
// probe exists to prevent (design §7.4).
//
// Pin hosting is a POOL-WIDE property: one Orker answers the probe, and the
// documented deployment rule is that every Orker of a pool sets the same
// ORK_ORKER_MAX_PINS (design §12.6).
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

	// Orkers describes a pool's Orkers, for reporting only.
	Orkers PoolOrkerDescriber

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
		// An awaited task runs on no pool: nothing to allow.
		if t.GetAwait() != nil {
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

	for _, t := range planned.GetTasks() {
		// An awaited task is never routed: no route, no allow-list check, no
		// liveness probe, no pin bookkeeping, and Task.pool stays empty on the
		// planned definition. An Orkflow whose every task is an await plans
		// with empty Routes and never asks the cluster anything.
		if t.GetAwait() != nil {
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

	plan := RunPlan{Namespace: namespaceName(ns), Routes: routes}
	telemetry, err := telemetryFor(ns, wf)
	if err != nil {
		return nil, RunPlan{}, err
	}
	plan.Telemetry = telemetry
	if r.Probe == nil {
		return planned, plan, nil
	}
	for _, pool := range order {
		live, err := r.Probe.ObservePool(ctx, routes[pool])
		if err != nil {
			// Not an empty pool: nothing was measured. The distinction
			// survives all the way to the client, where it is the difference
			// between "start an Orker" and "try again".
			return nil, RunPlan{}, &PoolUnreachableError{Pool: pool, Err: err}
		}
		if live.Fresh(r.now(), r.freshness()) {
			// A live pool that carries a pinned task is asked one more
			// question: can anything here OPEN a pin (§7.4). Asked only of a
			// live pool with a pinned task, so an unpinned submit runs no
			// describe call it did not run before (§10.4), and a deserted pool
			// has already failed below with PoolEmptyError — what its Orkers
			// host is moot when it has none.
			if err := r.checkPinHost(ctx, checkPinHostRequest{
				namespace:  namespaceName(ns),
				orkflow:    wf.GetName(),
				pool:       pool,
				queue:      routes[pool],
				pins:       pinsByPool[pool],
				tasks:      pinnedTasksByPool[pool],
				allowEmpty: allowEmpty,
			}); err != nil {
				return nil, RunPlan{}, err
			}
			continue
		}
		empty := &PoolEmptyError{
			Namespace: namespaceName(ns),
			Pool:      pool,
			Tasks:     tasksByPool[pool],
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

// checkPinHostRequest is one pool's pin question, gathered while Plan walked
// the tasks. It is a struct rather than seven parameters because every field
// is a coordinate of the same refusal message.
type checkPinHostRequest struct {
	namespace  string
	orkflow    string
	pool       string
	queue      string
	pins       []string
	tasks      []string
	allowEmpty bool
}

// checkPinHost refuses a live pool that hosts no pins but was asked to.
//
// It asks the pool's own Orker, through the same OrkerOnPool seam the
// inventory uses, and reads DescribeOrkerResult.max_concurrent_pins — the
// Orker's ORK_ORKER_MAX_PINS. Zero means the process starts no session
// pollers at all, so the pin's session could never be created there.
//
// Three ways it declines to refuse, and each is a different fact:
//
//   - No pinned task on this pool: there is no question to ask, and asking
//     would add a describe call to an unpinned submit (§10.4).
//   - No describer wired (r.Orkers == nil): no authority to ask, the same
//     reason a nil Probe disables the liveness check rather than inventing an
//     answer. A process with no seam must not refuse every pinned submit.
//   - The describe call failed: nothing was MEASURED. The pool's liveness was,
//     and stands; only the Orker's self-description was lost, which is how
//     Pools() treats the same failure. Turning a describe blip into a refused
//     submit would invent a new failure, and the in-workflow backstop already
//     covers a pool that genuinely cannot open a pin: the session's creation
//     activity times out at pinCreationTimeout and the task fails with
//     PIN_UNAVAILABLE.
//
// allowEmpty downgrades a real refusal to a warning exactly as it does for an
// empty pool: the operator who overrides has been told what they are waiting
// for (§7.4).
func (r *PoolResolver) checkPinHost(ctx context.Context, req checkPinHostRequest) error {
	if len(req.pins) == 0 || r.Orkers == nil {
		return nil
	}
	// Bounded here as well as inside the production seam: a hand-wired
	// describer is not obliged to bound itself, and a submit must not be held
	// open by one.
	describeCtx, cancel := context.WithTimeout(ctx, poolDescribeTimeout)
	defer cancel()
	described, err := r.Orkers.OrkerOnPool(describeCtx, req.queue)
	if err != nil {
		r.logger().Warn("submit: a live pool with a pinned task did not answer the "+
			"describe-orker probe, so whether it hosts pins is unknown",
			"namespace", req.namespace,
			"pool", req.pool,
			"orkflow", req.orkflow,
			"pins", strings.Join(req.pins, ","),
			"error", err)
		return nil
	}
	if described.GetMaxConcurrentPins() > 0 {
		return nil
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
// keeps its row and carries UnavailableReason instead of a count; a pool whose
// Orkers do not answer keeps its count and loses its Orker description. Both are
// logged with the pool and the cause, because a narrowed answer that says
// nothing is indistinguishable from a complete one.
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
	// poolProbeFanout). Each call writes only its own slot, so the report is
	// in name order whatever order the answers arrive in.
	pools := make([]*orkstackv1.Pool, len(ordered))
	probeTogether(len(ordered), func(i int) {
		name := ordered[i]
		pools[i] = r.observedPool(ctx, ns, name, queues[name], name == defaultRoute.Pool)
	})
	return pools, nil
}

// observedPool is one row of Pools: the pool, asked of the cluster on its
// channel. A pool with no channel (a name this engine cannot route) is
// reported as it is, unasked.
func (r *PoolResolver) observedPool(
	ctx context.Context,
	ns *orkstackv1.Namespace,
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
		case live.Fresh(r.now(), r.freshness()):
			pool.LiveOrkerCount = int32(live.Orkers)
			if !live.LastSeen.IsZero() {
				pool.LastSeen = timestamppb.New(live.LastSeen.UTC())
			}
		}
	}
	if pool.GetLiveOrkerCount() > 0 && r.Orkers != nil {
		// The probe is asked for liveness evidence only: which images
		// exist is engine configuration (§4.2). The call is kept because
		// answering at all is the evidence a probe nobody makes cannot
		// give.
		if _, err := r.Orkers.OrkerOnPool(ctx, queue); err != nil {
			// The pool's liveness was measured from the cluster and
			// stands; only the Orker's self-description was lost.
			r.logger().Warn("list pools: a live pool did not answer the "+
				"describe-orker probe",
				"namespace", namespaceName(ns),
				"pool", name,
				"error", err)
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
