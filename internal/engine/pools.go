package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
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
// to the behaviour this milestone replaces, a run that waits. A window too
// tight would do the opposite and refuse a healthy submit, which is a new
// failure invented by the check itself.
const DefaultPollerFreshness = 60 * time.Second

// poolPluginTimeout bounds the plugin question asked of one pool. It is short:
// a pool with live Orkers answers immediately, and a report is not worth
// holding a request open for.
const poolPluginTimeout = 10 * time.Second

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

// PoolPluginLister reports what the Orkers of one pool can run. It is asked
// only of a pool that has live Orkers — a pool with none has nobody to answer,
// and asking would only time out.
//
// It answers with the Orkers' own DESCRIPTORS rather than a list of names,
// because two callers ask this one question and they need different halves of
// the answer: pool health renders the names, and the plugin inventory renders
// the version, the description and the input/output schemas. One seam, asked
// once per pool; a second seam for the same activity would be a second thing
// to keep true (AGENTS.md §7, the duplicated-helper lesson).
type PoolPluginLister interface {
	PluginsOnPool(ctx context.Context, queue string) ([]plugin.Descriptor, error)
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
// so nothing above it invents a lock (design §5, AGENTS.md §1.7).
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
// This is the stranded run of AGENTS.md §6, converted into an error at the
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
// workflow may do (AGENTS.md §1.3). What it produces — a definition whose
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

	// Plugins reports what a pool's Orkers can run, for reporting only.
	Plugins PoolPluginLister

	// RawQueue is the operator escape hatch (ORK_ORKER_QUEUE): the channel a
	// task that names no pool is dispatched to. Empty means the pool's own.
	RawQueue string

	// Freshness overrides DefaultPollerFreshness.
	Freshness time.Duration

	// Logger records an empty-pool override. Nil uses the process default.
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
	permitted := permittedPools(ns)

	planned := proto.Clone(wf).(*orkstackv1.Orkflow)
	routes := map[string]string{}
	// Ordered, so the error a submit reports is the same one every time.
	var order []string
	tasksByPool := map[string][]string{}

	for _, t := range planned.GetTasks() {
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
	}

	plan := RunPlan{Namespace: namespaceName(ns), Routes: routes}
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
// serving it now and the plugins those Orkers run.
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

	pools := make([]*orkstackv1.Pool, 0, len(ordered))
	for _, name := range ordered {
		pool := &orkstackv1.Pool{Name: name, IsDefault: name == defaultRoute.Pool}
		queue := queues[name]
		if queue == "" {
			pools = append(pools, pool)
			continue
		}
		if r.Probe != nil {
			live, err := r.Probe.ObservePool(ctx, queue)
			if err != nil {
				return nil, &PoolUnreachableError{Pool: name, Err: err}
			}
			if live.Fresh(r.now(), r.freshness()) {
				pool.LiveOrkerCount = int32(live.Orkers)
				if !live.LastSeen.IsZero() {
					pool.LastSeen = timestamppb.New(live.LastSeen.UTC())
				}
			}
		}
		if pool.GetLiveOrkerCount() > 0 && r.Plugins != nil {
			// A failed read is REPORTED, never zero-filled. A live pool with
			// an empty plugin list is a fact — those Orkers run nothing — and
			// it is the same bytes on the wire as "we could not ask them".
			// An operator reading the second as the first goes looking for a
			// missing plugin that is installed, so the two states stay apart
			// even though keeping them apart costs the whole report.
			descriptors, err := r.Plugins.PluginsOnPool(ctx, queue)
			if err != nil {
				return nil, &PoolUnreachableError{Pool: name, Err: err}
			}
			pool.Plugins = descriptorNames(descriptors)
		}
		pools = append(pools, pool)
	}
	return pools, nil
}

// descriptorNames renders a pool's descriptors as the sorted plugin names the
// Pool message carries.
func descriptorNames(descriptors []plugin.Descriptor) []string {
	names := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		names = append(names, descriptor.Name)
	}
	sort.Strings(names)
	return names
}

// Inventory reports what the Orkers of these pools can run, for the
// deployment-wide plugin inventory.
//
// Three rules, and each is a claim the inventory would otherwise get wrong:
//
//   - ONE ask per execution channel. Two pool names that resolve to one
//     channel are one set of Orkers (see routeFor), and every namespace has a
//     default pool, so a naive loop would run the discovery activity once per
//     namespace against the same machines.
//   - The deployment's DEFAULT channel is never asked here. PluginService
//     reads that one through its own lister — the seam that exists whether or
//     not a cluster is wired — and asking it twice would execute one
//     activity twice for one report.
//   - LIVE pools only, and a live pool that cannot be asked is an ERROR. A
//     pool with no Orkers has nobody to answer and would cost the activity's
//     whole timeout to learn nothing; a pool with Orkers that does not answer
//     is a hole in the inventory, and an inventory quietly missing a plugin
//     is what sends an operator to install one that is already there.
//
// A pool name this engine cannot route is skipped: a namespace document can
// hold one, and it names no channel to ask.
func (r *PoolResolver) Inventory(
	ctx context.Context,
	pools []string,
) ([]plugin.Descriptor, error) {
	if r.Plugins == nil || r.Probe == nil {
		// Nothing to ask, or no way to tell who is live. Either way this is
		// not a narrowed answer to be logged: the caller's own lister is the
		// whole inventory such a deployment has.
		return nil, nil
	}
	deploymentDefault, err := r.routeFor(task.DefaultPool)
	if err != nil {
		return nil, err
	}

	asked := map[string]struct{}{deploymentDefault.Queue: {}}
	var order []string
	byQueue := map[string]string{}
	for _, pool := range pools {
		if pool == "" {
			continue
		}
		route, err := r.routeFor(pool)
		if err != nil {
			continue
		}
		if _, seen := asked[route.Queue]; seen {
			continue
		}
		asked[route.Queue] = struct{}{}
		byQueue[route.Queue] = route.Pool
		order = append(order, route.Queue)
	}
	sort.Slice(order, func(i, j int) bool { return byQueue[order[i]] < byQueue[order[j]] })

	var found []plugin.Descriptor
	for _, queue := range order {
		live, err := r.Probe.ObservePool(ctx, queue)
		if err != nil {
			return nil, &PoolUnreachableError{Pool: byQueue[queue], Err: err}
		}
		if !live.Fresh(r.now(), r.freshness()) {
			continue
		}
		descriptors, err := r.Plugins.PluginsOnPool(ctx, queue)
		if err != nil {
			return nil, &PoolUnreachableError{Pool: byQueue[queue], Err: err}
		}
		found = append(found, descriptors...)
	}
	return found, nil
}

func namespaceName(ns *orkstackv1.Namespace) string {
	if name := ns.GetName(); name != "" {
		return name
	}
	return artifacts.DefaultNamespace
}
