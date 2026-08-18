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
type PoolPluginLister interface {
	PluginsOnPool(ctx context.Context, queue string) ([]string, error)
}

// NamespaceReader is the engine's view of the namespace registry: the one
// question routing asks of it.
type NamespaceReader interface {
	Get(ctx context.Context, name string) (*orkstackv1.Namespace, error)
}

// PoolNotAllowedError reports a task routed onto a pool its namespace does not
// allow. It carries the three coordinates that make the refusal actionable:
// who forbade it, which task asked, and what it asked for.
type PoolNotAllowedError struct {
	Namespace string
	Task      string
	Pool      string
	Allowed   []string
}

func (e *PoolNotAllowedError) Error() string {
	return fmt.Sprintf("namespace %q does not allow pool %q for task %q (allowed: %s)",
		e.Namespace, e.Pool, e.Task, strings.Join(e.Allowed, ", "))
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

// CheckAllowedPools enforces a namespace's allow-list over a definition,
// without asking the cluster anything.
//
// It is the authoring-time gate: ApplyOrkflow calls it with the file in hand,
// so an author is told at apply rather than at the first submit. It is not the
// only gate — see Plan, which cannot be bypassed by a client that never
// applied, or by a definition stored while the list was wider.
func CheckAllowedPools(ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) error {
	allowed := ns.GetAllowedPools()
	if len(allowed) == 0 {
		return nil
	}
	permitted := make(map[string]struct{}, len(allowed))
	for _, pool := range allowed {
		permitted[pool] = struct{}{}
	}
	for _, t := range wf.GetTasks() {
		pool := PoolFor(ns, t)
		if _, ok := permitted[pool]; ok {
			continue
		}
		return &PoolNotAllowedError{
			Namespace: namespaceName(ns),
			Task:      t.GetId(),
			Pool:      pool,
			Allowed:   append([]string(nil), allowed...),
		}
	}
	return nil
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
	if err := CheckAllowedPools(ns, wf); err != nil {
		return nil, RunPlan{}, err
	}

	planned := proto.Clone(wf).(*orkstackv1.Orkflow)
	defaultPool := DefaultPoolFor(ns)
	routes := map[string]string{}
	// Ordered, so the error a submit reports is the same one every time.
	var order []string
	tasksByPool := map[string][]string{}

	for _, t := range planned.GetTasks() {
		pool := PoolFor(ns, t)
		t.Pool = pool
		if _, seen := routes[pool]; !seen {
			queue, err := r.queueFor(pool, defaultPool)
			if err != nil {
				return nil, RunPlan{}, err
			}
			routes[pool] = queue
			order = append(order, pool)
		}
		tasksByPool[pool] = append(tasksByPool[pool], t.GetId())
	}

	plan := RunPlan{Namespace: namespaceName(ns), Routes: routes}
	if r.Probe == nil {
		return planned, plan, nil
	}
	for _, pool := range order {
		live, err := r.Probe.ObservePool(ctx, routes[pool])
		if err != nil {
			return nil, RunPlan{}, fmt.Errorf("check pool %q for live orkers: %w", pool, err)
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

// queueFor maps one pool onto the channel that carries it, honouring the
// operator escape hatch for the run's default pool only.
//
// The override moves the DEFAULT route. A task that named a pool keeps its
// own, or `pool: gpu` would silently run wherever an unrelated environment
// variable pointed — the failure the pool name exists to prevent.
func (r *PoolResolver) queueFor(pool, defaultPool string) (string, error) {
	if r.RawQueue != "" && pool == defaultPool {
		return r.RawQueue, nil
	}
	queue, err := task.QueueForPool(pool)
	if err != nil {
		return "", err
	}
	return queue, nil
}

// Pools reports the pools a namespace may dispatch onto, each with the Orkers
// serving it now and the plugins those Orkers run.
//
// Scope: the namespace's allow-list when it has one, otherwise its default
// plus whatever extra pools the caller knows about (the pools its orkflows
// name). Everything comes from the cluster, so any replica answers — including
// one that started nothing.
func (r *PoolResolver) Pools(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	extra []string,
) ([]*orkstackv1.Pool, error) {
	defaultPool := DefaultPoolFor(ns)
	names := map[string]struct{}{defaultPool: {}}
	for _, pool := range ns.GetAllowedPools() {
		names[pool] = struct{}{}
	}
	if len(ns.GetAllowedPools()) == 0 {
		for _, pool := range extra {
			if pool != "" {
				names[pool] = struct{}{}
			}
		}
	}

	ordered := make([]string, 0, len(names))
	for pool := range names {
		ordered = append(ordered, pool)
	}
	sort.Strings(ordered)

	pools := make([]*orkstackv1.Pool, 0, len(ordered))
	for _, name := range ordered {
		queue, err := task.QueueForPool(name)
		if err != nil {
			// A namespace document can hold a pool name this engine cannot
			// map. Report it as unserved rather than failing the whole
			// listing: the operator asked what is out there.
			pools = append(pools, &orkstackv1.Pool{Name: name, IsDefault: name == defaultPool})
			continue
		}
		pool := &orkstackv1.Pool{Name: name, IsDefault: name == defaultPool}
		if r.Probe != nil {
			live, err := r.Probe.ObservePool(ctx, queue)
			if err != nil {
				return nil, fmt.Errorf("observe pool %q: %w", name, err)
			}
			if live.Fresh(r.now(), r.freshness()) {
				pool.LiveOrkerCount = int32(live.Orkers)
				if !live.LastSeen.IsZero() {
					pool.LastSeen = timestamppb.New(live.LastSeen.UTC())
				}
			}
		}
		if pool.GetLiveOrkerCount() > 0 && r.Plugins != nil {
			plugins, err := r.Plugins.PluginsOnPool(ctx, queue)
			if err == nil {
				sort.Strings(plugins)
				pool.Plugins = plugins
			}
		}
		pools = append(pools, pool)
	}
	return pools, nil
}

func namespaceName(ns *orkstackv1.Namespace) string {
	if name := ns.GetName(); name != "" {
		return name
	}
	return artifacts.DefaultNamespace
}
