package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// livePools is a stand-in for the cluster's answer to "who is serving this
// channel". The real one asks Temporal for poller information
// (engine.TemporalPoolProbe); this one is a map, so a test can state the fact
// it is testing instead of arranging workers.
type livePools map[string]int

func (l livePools) ObservePool(_ context.Context, queue string) (engine.PoolLiveness, error) {
	orkers := l[queue]
	live := engine.PoolLiveness{Orkers: orkers}
	if orkers > 0 {
		live.LastSeen = time.Now()
	}
	return live, nil
}

// everyPoolLive answers "yes" for any channel, for tests that are not about
// liveness.
type everyPoolLive struct{}

func (everyPoolLive) ObservePool(context.Context, string) (engine.PoolLiveness, error) {
	return engine.PoolLiveness{Orkers: 1, LastSeen: time.Now()}, nil
}

const twoPoolDocument = `
name: pool-plan
tasks:
  - id: crunch
    uses: shell
    pool: gpu
    with: {command: "echo crunch"}
  - id: tidy
    uses: shell
    needs: [crunch]
    with: {command: "echo tidy"}
`

// TestPoolResolver_ResolvesTaskPoolThenNamespaceDefaultThenMain is the
// resolution order, stated once and checked in one place.
//
// A task that names a pool IS that pool. A task that names none falls to its
// namespace's default_pool, and a namespace that names none falls to the
// built-in "main". Every task in the planned orkflow carries the answer, so
// the workflow reads a pool off its input and never resolves anything itself.
func TestPoolResolver_ResolvesTaskPoolThenNamespaceDefaultThenMain(t *testing.T) {
	resolver := &engine.PoolResolver{Probe: everyPoolLive{}}

	t.Run("namespace default fills a task that names none", func(t *testing.T) {
		ns := &orkstackv1.Namespace{Name: "payments", DefaultPool: "batch"}
		planned, plan, err := resolver.Plan(context.Background(), ns, compile(t, twoPoolDocument), false)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		want := map[string]string{"crunch": "gpu", "tidy": "batch"}
		for _, tk := range planned.GetTasks() {
			if got := tk.GetPool(); got != want[tk.GetId()] {
				t.Errorf("task %q planned onto pool %q, want %q", tk.GetId(), got, want[tk.GetId()])
			}
		}
		if plan.Namespace != "payments" {
			t.Errorf("plan namespace = %q, want %q", plan.Namespace, "payments")
		}
		// Exactly the pools this run uses, and nothing else: a route table is
		// the run's own routing record, not a copy of the deployment.
		gpuQueue, _ := task.QueueForPool("gpu", "")
		batchQueue, _ := task.QueueForPool("batch", "")
		wantRoutes := map[string]string{"gpu": gpuQueue, "batch": batchQueue}
		if len(plan.Routes) != len(wantRoutes) {
			t.Fatalf("routes = %v, want exactly %v", plan.Routes, wantRoutes)
		}
		for pool, queue := range wantRoutes {
			if plan.Routes[pool] != queue {
				t.Errorf("route for pool %q = %q, want %q", pool, plan.Routes[pool], queue)
			}
		}
	})

	t.Run("a namespace with no default falls to main", func(t *testing.T) {
		planned, plan, err := resolver.Plan(context.Background(),
			&orkstackv1.Namespace{Name: "default"}, compile(t, twoPoolDocument), false)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		for _, tk := range planned.GetTasks() {
			if tk.GetId() == "tidy" && tk.GetPool() != task.DefaultPool {
				t.Errorf("task tidy planned onto %q, want the built-in %q", tk.GetPool(), task.DefaultPool)
			}
		}
		if plan.Routes[task.DefaultPool] != task.DefaultQueue {
			t.Errorf("route for %q = %q, want the channel a zero-config stack polls (%q)",
				task.DefaultPool, plan.Routes[task.DefaultPool], task.DefaultQueue)
		}
	})

	t.Run("the definition is not mutated", func(t *testing.T) {
		source := compile(t, twoPoolDocument)
		if _, _, err := resolver.Plan(context.Background(),
			&orkstackv1.Namespace{Name: "default", DefaultPool: "batch"}, source, false); err != nil {
			t.Fatalf("Plan: %v", err)
		}
		for _, tk := range source.GetTasks() {
			if tk.GetId() == "tidy" && tk.GetPool() != "" {
				t.Errorf("planning wrote pool %q back onto the stored definition; "+
					"the document is the user's and a run's routing is the run's", tk.GetPool())
			}
		}
	})
}

// TestPoolResolverPlan_ClearsSourceYAML: a run starts without its Orkflow's
// source. The source is not what runs and the stored version keeps it, so
// the start definition — one §2.2 payload — is measured without it; the
// stored document is left untouched.
func TestPoolResolverPlan_ClearsSourceYAML(t *testing.T) {
	resolver := &engine.PoolResolver{Probe: everyPoolLive{}}
	source := compile(t, twoPoolDocument)
	if source.GetSourceYaml() == "" {
		t.Fatal("the compiled definition carries no source; this test's premise does not hold")
	}
	planned, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "default"}, source, false)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if planned.GetSourceYaml() != "" {
		t.Fatalf("the planned definition carries %d bytes of source YAML, want none", len(planned.GetSourceYaml()))
	}
	if source.GetSourceYaml() == "" {
		t.Fatal("planning cleared the stored definition's source; the document is the user's")
	}
}

// TestPoolResolver_SendsTheDefaultRouteToTheOperatorsRawChannel keeps the
// escape hatch working: an engine started with ORK_ORKER_QUEUE dispatches a
// task that names no pool there, which is how a test harness isolates itself
// (AGENTS.md §11.1) with no change to any document.
//
// Dispatch and reporting must read the same map. One process holding two
// answers about one pool — accepting a submit because the operator's channel
// is served, and reporting that same pool deserted — is worse than either
// answer alone, because the number an operator reads is the wrong one.
func TestPoolResolver_SendsTheDefaultRouteToTheOperatorsRawChannel(t *testing.T) {
	const raw = "ork-tasks-private-agent"
	gpuQueue, _ := task.QueueForPool("gpu", "")
	// Three Orkers on the operator's channel, one on gpu's own, and nobody on
	// the channel the default pool would have used without the override.
	resolver := &engine.PoolResolver{
		Probe:    livePools{raw: 3, gpuQueue: 1},
		Orkers:   fakePoolOrkers{raw: true, gpuQueue: true},
		RawQueue: raw,
	}
	ns := &orkstackv1.Namespace{Name: "default"}

	_, plan, err := resolver.Plan(context.Background(), ns, compile(t, twoPoolDocument), false)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Routes[task.DefaultPool] != raw {
		t.Errorf("route for %q = %q, want the operator's channel %q",
			task.DefaultPool, plan.Routes[task.DefaultPool], raw)
	}
	// A task that named a pool is NOT redirected: the override moves the
	// default route only, or `pool: gpu` would silently run somewhere else.
	if plan.Routes["gpu"] != gpuQueue {
		t.Errorf("route for pool gpu = %q, want %q", plan.Routes["gpu"], gpuQueue)
	}

	pools, err := resolver.Pools(context.Background(), ns, []string{"gpu"})
	if err != nil {
		t.Fatalf("Pools: %v", err)
	}
	byName := map[string]*orkstackv1.Pool{}
	for _, p := range pools {
		byName[p.GetName()] = p
	}
	if got := byName[task.DefaultPool].GetLiveOrkerCount(); got != 3 {
		t.Errorf("pool %q reports %d live Orkers, want the 3 serving %q — the channel "+
			"this very resolver dispatches the pool's work to", task.DefaultPool, got, raw)
	}
	// The override moved one route, so it moves one report.
	if got := byName["gpu"].GetLiveOrkerCount(); got != 1 {
		t.Errorf("pool gpu reports %d live Orkers, want 1", got)
	}
}

// TestPoolResolver_OneSetOfOrkersIsOnePool is the configuration
// docs/DEPLOY.md documents as the preferred one — ORK_ORKER_POOL=gpu on BOTH
// binaries — where the engine's bound channel IS a pool's own queue.
//
// Then `main` and `gpu` are two names for one set of Orkers, and the report
// must say so once. Counting them twice tells an operator there are two pools
// to size and two to drain when there is one; and a task that named no pool
// would be reported to every client (TASK_SCHEDULED, TaskRun.pool, the Orker's
// own key) as running on `main` while gpu's Orkers ran it.
//
// This is the defect the collapse in task.RouteForPool closes, asserted where
// a client would read it.
func TestPoolResolver_OneSetOfOrkersIsOnePool(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	resolver := &engine.PoolResolver{
		Probe:    livePools{gpuQueue: 2},
		Orkers:   fakePoolOrkers{gpuQueue: true},
		RawQueue: gpuQueue, // what ORK_ORKER_POOL=gpu binds on the engine
	}
	ns := &orkstackv1.Namespace{Name: "default"}

	planned, plan, err := resolver.Plan(context.Background(), ns, compile(t, twoPoolDocument), false)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	// One route, because it is one set of Orkers: `pool: gpu` and the task
	// that named none are the same route under this deployment.
	if len(plan.Routes) != 1 || plan.Routes["gpu"] != gpuQueue {
		t.Fatalf("routes = %v, want exactly {gpu: %s}", plan.Routes, gpuQueue)
	}
	for _, tk := range planned.GetTasks() {
		if got := tk.GetPool(); got != "gpu" {
			t.Errorf("task %q is reported on pool %q while it runs on gpu's Orkers; the pool a "+
				"client reads must be the pool the work reached", tk.GetId(), got)
		}
	}

	pools, err := resolver.Pools(context.Background(), ns, []string{"gpu"})
	if err != nil {
		t.Fatalf("Pools: %v", err)
	}
	if len(pools) != 1 {
		names := make([]string, 0, len(pools))
		for _, p := range pools {
			names = append(names, fmt.Sprintf("%s(orkers=%d)", p.GetName(), p.GetLiveOrkerCount()))
		}
		t.Fatalf("ListPools reports %d pools (%s) for one set of Orkers; two names for one "+
			"channel is one pool", len(pools), strings.Join(names, " "))
	}
	only := pools[0]
	if only.GetName() != "gpu" {
		t.Errorf("the pool is reported as %q, want %q — the name its channel answers to",
			only.GetName(), "gpu")
	}
	if !only.GetIsDefault() {
		t.Errorf("pool %q is not marked default, but it is where a task that names no pool runs",
			only.GetName())
	}
	if got := only.GetLiveOrkerCount(); got != 2 {
		t.Errorf("pool %q reports %d live Orkers, want the 2 polling %q", only.GetName(), got, gpuQueue)
	}
}

// TestPoolResolver_RefusesAPoolTheNamespaceDoesNotAllow is the enforcement
// half of allowed_pools: a list nothing checks is a comment.
func TestPoolResolver_RefusesAPoolTheNamespaceDoesNotAllow(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name:         "payments",
		DefaultPool:  "main",
		AllowedPools: []string{"main", "batch"},
	}
	resolver := &engine.PoolResolver{Probe: everyPoolLive{}}

	_, _, err := resolver.Plan(context.Background(), ns, compile(t, twoPoolDocument), false)
	var notAllowed *engine.PoolNotAllowedError
	if !errors.As(err, &notAllowed) {
		t.Fatalf("Plan error = %v, want a PoolNotAllowedError", err)
	}
	// The three coordinates that make the error actionable: who forbade it,
	// which task asked, and what it asked for.
	for _, want := range []string{"payments", "crunch", "gpu", "main", "batch"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	// An empty allow-list is no restriction, not a restriction to nothing.
	open := &orkstackv1.Namespace{Name: "payments"}
	if _, _, err := resolver.Plan(context.Background(), open, compile(t, twoPoolDocument), false); err != nil {
		t.Fatalf("Plan with an unrestricted namespace: %v", err)
	}
}

// TestPoolResolver_HoldsTheAllowListAgainstThePoolItDispatchesTo pairs the two
// halves that were each tested alone: a namespace with an allow-list, and a
// deployment whose default execution channel is bound to another pool's own
// queue (ORK_ORKER_POOL=gpu on the engine, which docs/DEPLOY.md recommends and
// which every parallel-agent harness runs).
//
// Apart, both halves passed. Together, the allow-list was asked about the name
// the document wrote while the dispatch used the name that channel answers to,
// so a namespace allowing only `main` planned its tasks onto gpu's Orkers —
// docs/DEPLOY.md calls the list a bound on "what it may dispatch onto" — and
// the mirror refused `pool: main` in a namespace that allows `gpu`, which is
// where it was going anyway.
//
// The invariant, in one sentence: the pool stamped on a planned task is one
// the namespace allows, or there is no planned task.
func TestPoolResolver_HoldsTheAllowListAgainstThePoolItDispatchesTo(t *testing.T) {
	gpuQueue, err := task.QueueForPool("gpu", "")
	if err != nil {
		t.Fatalf("QueueForPool: %v", err)
	}
	resolver := &engine.PoolResolver{RawQueue: gpuQueue, Probe: everyPoolLive{}}

	t.Run("the default pool collapsed onto a pool the namespace forbids", func(t *testing.T) {
		ns := &orkstackv1.Namespace{Name: "payments", AllowedPools: []string{"main"}}
		doc := compile(t, onePoolDocument) // `settle` names no pool

		planned, plan, err := resolver.Plan(context.Background(), ns, doc, false)
		var notAllowed *engine.PoolNotAllowedError
		if !errors.As(err, &notAllowed) {
			t.Fatalf("Plan = (%v, %v, %v), want a PoolNotAllowedError: the task would be "+
				"dispatched to gpu's Orkers, and this namespace allows only main",
				stampedPools(planned), plan.Routes, err)
		}
		if notAllowed.Pool != "gpu" {
			t.Errorf("the refusal names pool %q, want the pool it would have dispatched to (gpu)",
				notAllowed.Pool)
		}
		// Printed, because the README quotes it: a refusal is only actionable
		// if it names both the pool it would have run on and the pool the
		// document asked for.
		t.Logf("the refusal, as a client is handed it: %v", err)
		// The name the document wrote is what the operator has to go and find.
		if !strings.Contains(err.Error(), `"main"`) {
			t.Errorf("the refusal %q does not name the pool the document asked for", err)
		}
		// Apply says exactly what submit says. An author told "yes" at apply
		// and "no" at submit learns the rule twice.
		if applyErr := resolver.CheckAllowedPools(ns, doc); !errors.As(applyErr, &notAllowed) {
			t.Errorf("CheckAllowedPools = %v, want the same refusal Plan makes (%v)", applyErr, err)
		}
	})

	t.Run("`pool: main` inside a namespace that allows the bound pool", func(t *testing.T) {
		ns := &orkstackv1.Namespace{Name: "payments", AllowedPools: []string{"gpu"}}
		doc := compile(t, mainPoolDocument) // `settle` names `pool: main`

		if err := resolver.CheckAllowedPools(ns, doc); err != nil {
			t.Fatalf("CheckAllowedPools refused `pool: main` under a channel bound to "+
				"gpu's queue: %v — that is where it dispatches", err)
		}
		planned, plan, err := resolver.Plan(context.Background(), ns, doc, false)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if got := planned.GetTasks()[0].GetPool(); got != "gpu" {
			t.Errorf("`pool: main` was stamped %q, want gpu — the name its channel answers to", got)
		}
		if plan.Routes["gpu"] != gpuQueue {
			t.Errorf("routes = %v, want gpu on %q", plan.Routes, gpuQueue)
		}
	})

	t.Run("every planned pool is one the namespace allows", func(t *testing.T) {
		for _, allowed := range [][]string{{"main"}, {"gpu"}, {"batch"}, {"gpu", "batch"}} {
			ns := &orkstackv1.Namespace{Name: "payments", AllowedPools: allowed}
			for _, doc := range []string{onePoolDocument, mainPoolDocument, twoPoolDocument} {
				planned, _, err := resolver.Plan(context.Background(), ns, compile(t, doc), false)
				if err != nil {
					continue // refused, which is the other permitted outcome
				}
				permitted := map[string]struct{}{}
				for _, pool := range allowed {
					permitted[pool] = struct{}{}
				}
				for _, stamped := range stampedPools(planned) {
					if _, ok := permitted[stamped]; !ok {
						t.Errorf("namespace allows %v, but a task was planned onto pool %q",
							allowed, stamped)
					}
				}
			}
		}
	})
}

// mainPoolDocument names the default pool the long way, which is the spelling
// a bound channel collapses.
const mainPoolDocument = `
name: main-pool
tasks:
  - id: settle
    uses: shell
    pool: main
    with: {command: "echo settle"}
`

// stampedPools is the pool each planned task carries: what the run reports and
// what its dispatch follows.
func stampedPools(planned *orkstackv1.Orkflow) []string {
	pools := make([]string, 0, len(planned.GetTasks()))
	for _, t := range planned.GetTasks() {
		pools = append(pools, t.GetPool())
	}
	return pools
}

// TestPoolResolver_RefusesASubmitOntoAPoolWithNoLiveOrkers converts the
// stranded-RUNNING failure AGENTS.md §11.1 describes into an error at submit.
//
// Before this, a task dispatched to a pool nothing serves left the run in
// RUNNING until a human noticed. The pool is asked, once, at the moment the
// routing decision is made.
func TestPoolResolver_RefusesASubmitOntoAPoolWithNoLiveOrkers(t *testing.T) {
	gpuQueue, err := task.QueueForPool("gpu", "")
	if err != nil {
		t.Fatalf("QueueForPool: %v", err)
	}
	// main is served; gpu is not.
	resolver := &engine.PoolResolver{Probe: livePools{task.DefaultQueue: 2}}
	ns := &orkstackv1.Namespace{Name: "payments"}

	_, _, err = resolver.Plan(context.Background(), ns, compile(t, twoPoolDocument), false)
	var empty *engine.PoolEmptyError
	if !errors.As(err, &empty) {
		t.Fatalf("Plan error = %v, want a PoolEmptyError", err)
	}
	if empty.Pool != "gpu" || empty.Namespace != "payments" {
		t.Errorf("PoolEmptyError = %+v, want pool gpu in namespace payments", empty)
	}
	if len(empty.Tasks) != 1 || empty.Tasks[0] != "crunch" {
		t.Errorf("PoolEmptyError.Tasks = %v, want the tasks that would have stranded", empty.Tasks)
	}
	// The error has to say what to do about it. Both remedies, named.
	for _, want := range []string{"ORK_ORKER_POOL=gpu", "allow_empty_pool"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	if strings.Contains(strings.ToLower(err.Error()), "queue") || strings.Contains(err.Error(), gpuQueue) {
		t.Errorf("error %q leaks the engine's channel name to a client", err)
	}

	// The override exists and is not the default.
	if _, _, err := resolver.Plan(context.Background(), ns, compile(t, twoPoolDocument), true); err != nil {
		t.Fatalf("Plan with allowEmpty: %v", err)
	}
}

// unreachableCluster is a probe that cannot answer at all: an unreachable
// Temporal, or one slower than the probe's own five-second bound.
type unreachableCluster struct{}

func (unreachableCluster) ObservePool(context.Context, string) (engine.PoolLiveness, error) {
	return engine.PoolLiveness{}, errors.New("context deadline exceeded")
}

// TestPoolResolver_SeparatesCannotAskFromNobodyIsThere keeps a broken cluster
// out of the caller's lap.
//
// "Nobody serves this pool" is a fact, and the submission is answerable on it.
// "I could not ask" is not a fact about the pool at all: the request may be
// perfectly good and the only true instruction is to try again. Handed up as
// one error the two are indistinguishable, and everything above has to guess —
// so Plan says which it is, in the type.
func TestPoolResolver_SeparatesCannotAskFromNobodyIsThere(t *testing.T) {
	resolver := &engine.PoolResolver{Probe: unreachableCluster{}}
	ns := &orkstackv1.Namespace{Name: "payments"}

	_, _, err := resolver.Plan(context.Background(), ns, compile(t, onePoolDocument), false)
	var unreachable *engine.PoolUnreachableError
	if !errors.As(err, &unreachable) {
		t.Fatalf("Plan error = %v (%T), want a PoolUnreachableError", err, err)
	}
	if errors.As(err, new(*engine.PoolEmptyError)) {
		t.Error("a probe that could not answer is reported as an empty pool; the two " +
			"need opposite reactions — start an Orker, or wait for the cluster")
	}
	if unreachable.Pool != task.DefaultPool {
		t.Errorf("PoolUnreachableError.Pool = %q, want %q", unreachable.Pool, task.DefaultPool)
	}

	// allow_empty_pool answers "nobody is there", not "nobody answered". It
	// cannot vouch for a pool nothing could ask about.
	if _, _, err := resolver.Plan(context.Background(),
		ns, compile(t, onePoolDocument), true); !errors.As(err, &unreachable) {
		t.Errorf("Plan with allowEmpty over an unreachable cluster = %v, want the same "+
			"refusal", err)
	}
}

// TestPoolResolver_TreatsAStaleOrkerAsGone bounds the liveness answer in time.
// Temporal keeps poller history for minutes after a worker dies, so freshness
// is what stops a submit being accepted into a pool that stopped serving.
//
// It is checked at both ends: on the value, and on what the resolver does with
// a probe that hands one back. The Temporal probe drops a stale record itself
// (TestLivePollers_CountsOnlyTheOrkersStillInsideTheWindow); a PoolProbe is an
// interface, so the seam must not publish a number it was given on trust.
func TestPoolResolver_TreatsAStaleOrkerAsGone(t *testing.T) {
	live := engine.PoolLiveness{Orkers: 1, LastSeen: time.Now().Add(-10 * time.Minute)}
	if live.Fresh(time.Now(), engine.DefaultPollerFreshness) {
		t.Errorf("an Orker last seen 10 minutes ago counts as live within %v",
			engine.DefaultPollerFreshness)
	}
	live.LastSeen = time.Now().Add(-5 * time.Second)
	if !live.Fresh(time.Now(), engine.DefaultPollerFreshness) {
		t.Errorf("an Orker seen 5 seconds ago does not count as live")
	}

	// A probe that reports three Orkers nobody has seen for ten minutes.
	resolver := &engine.PoolResolver{Probe: stalePool{Orkers: 3, Age: 10 * time.Minute}}
	ns := &orkstackv1.Namespace{Name: "payments", AllowedPools: []string{"main"}}

	if _, _, err := resolver.Plan(context.Background(),
		ns, compile(t, onePoolDocument), false); !errors.As(err, new(*engine.PoolEmptyError)) {
		t.Fatalf("Plan error = %v, want a PoolEmptyError: three Orkers last seen ten "+
			"minutes ago would strand the run", err)
	}

	pools, err := resolver.Pools(context.Background(), ns, nil)
	if err != nil {
		t.Fatalf("Pools: %v", err)
	}
	if got := pools[0].GetLiveOrkerCount(); got != 0 {
		t.Errorf("ListPools would report %d Orkers on a pool everyone left, want 0", got)
	}
}

// stalePool answers with Orkers that stopped polling Age ago.
type stalePool struct {
	Orkers int
	Age    time.Duration
}

func (s stalePool) ObservePool(context.Context, string) (engine.PoolLiveness, error) {
	return engine.PoolLiveness{Orkers: s.Orkers, LastSeen: time.Now().Add(-s.Age)}, nil
}

const onePoolDocument = `
name: one-pool
tasks:
  - id: settle
    uses: shell
    with: {command: "echo settle"}
`

// TestPoolResolver_ReportsPoolsWithTheirLiveOrkers is the observability
// half: an operator asks which pools exist and how many Orkers serve each.
func TestPoolResolver_ReportsPoolsWithTheirLiveOrkers(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	resolver := &engine.PoolResolver{
		Probe:  livePools{task.DefaultQueue: 2, gpuQueue: 1},
		Orkers: fakePoolOrkers{gpuQueue: true, task.DefaultQueue: true},
	}
	ns := &orkstackv1.Namespace{Name: "payments", AllowedPools: []string{"main", "gpu", "cold"}}

	pools, err := resolver.Pools(context.Background(), ns, nil)
	if err != nil {
		t.Fatalf("Pools: %v", err)
	}
	byName := map[string]*orkstackv1.Pool{}
	for _, p := range pools {
		byName[p.GetName()] = p
	}
	if len(byName) != 3 {
		t.Fatalf("Pools returned %d pools (%v), want the namespace's three", len(pools), byName)
	}
	if got := byName["main"].GetLiveOrkerCount(); got != 2 {
		t.Errorf("pool main has %d live Orkers, want 2", got)
	}
	if !byName["main"].GetIsDefault() {
		t.Error("pool main is not marked as the namespace's default")
	}
	// A pool nothing serves is reported, not hidden: "nobody is there" is the
	// answer an operator came for.
	if got := byName["cold"].GetLiveOrkerCount(); got != 0 {
		t.Errorf("pool cold has %d live Orkers, want 0", got)
	}
}

// fakePoolOrkers answers the DescribeOrker probe for the channels it maps; a
// channel it does not map is a pool nobody serves.
type fakePoolOrkers map[string]bool

func (f fakePoolOrkers) OrkerOnPool(_ context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error) {
	if _, serving := f[queue]; !serving {
		return nil, fmt.Errorf("no orker is polling %q", queue)
	}
	return &orkstackv1.DescribeOrkerResult{OrkerId: "orker-" + queue, RuntimeName: "docker"}, nil
}

// ---------------------------------------------------------------------------
// Task pins at submit (docs/DSL.md "Pins", docs/DEPLOY.md "Pin hosting is a
// pool-wide setting")
// ---------------------------------------------------------------------------

// pinHostPools answers the DescribeOrker probe with each channel's
// ORK_ORKER_MAX_PINS, which is the one thing the submit-time pin check reads.
// A channel it does not map has no Orker to answer at all.
type pinHostPools map[string]int32

func (p pinHostPools) OrkerOnPool(_ context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error) {
	maxPins, serving := p[queue]
	if !serving {
		return nil, fmt.Errorf("no orker is polling %q", queue)
	}
	return &orkstackv1.DescribeOrkerResult{
		OrkerId:           "orker-" + queue,
		RuntimeName:       "docker",
		MaxConcurrentPins: maxPins,
	}, nil
}

// refusingDescriber fails the test if it is asked anything. It is how a test
// proves the describe call was NOT made — the promise that an unpinned
// submit runs no probe it did not run before this feature.
type refusingDescriber struct{ t *testing.T }

func (r refusingDescriber) OrkerOnPool(_ context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error) {
	r.t.Errorf("the describe-orker probe was made for %q, want no describe call at all", queue)
	return nil, errors.New("unexpected describe")
}

const pinnedSubmitDocument = `
name: pinned-submit
pins:
  build: {}
tasks:
  - id: clone
    uses: shell
    pin: build
    pool: gpu
    run_as_orker_user: true
    with: {command: "echo clone"}
  - id: compile
    uses: shell
    pin: build
    pool: gpu
    needs: [clone]
    run_as_orker_user: true
    with: {command: "echo compile"}
  - id: publish
    uses: shell
    pool: gpu
    needs: [compile]
    with: {command: "echo publish"}
`

// TestPoolResolver_RefusesAPinnedSubmitOntoAPoolThatHostsNoPins is the
// no-pin-host refusal.
//
// The pool is LIVE — an Orker is polling it — so PoolEmptyError has nothing to
// say. The narrower question a pin asks is whether anything serving the pool
// could OPEN one, and the answer is on the Orker's own DescribeOrker:
// ORK_ORKER_MAX_PINS. Zero means the process starts no session pollers, so the
// pin's creation request would sit on a channel nobody polls until
// pinCreationTimeout ran out and the run failed with PIN_UNAVAILABLE. That is
// the stranded run the submit-time probe exists to prevent, so it is refused
// here instead — with both remedies named, because a caller who cannot act on
// an error is left where they were.
func TestPoolResolver_RefusesAPinnedSubmitOntoAPoolThatHostsNoPins(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	resolver := &engine.PoolResolver{
		Probe:  livePools{gpuQueue: 2},
		Orkers: pinHostPools{gpuQueue: 0},
	}
	ns := &orkstackv1.Namespace{Name: "ci"}

	_, _, err := resolver.Plan(context.Background(), ns, compile(t, pinnedSubmitDocument), false)
	var noHost *engine.PoolNoPinHostError
	if !errors.As(err, &noHost) {
		t.Fatalf("Plan error = %v, want PoolNoPinHostError", err)
	}
	if noHost.Pool != "gpu" || noHost.Namespace != "ci" {
		t.Errorf("refusal names pool %q of namespace %q, want gpu of ci", noHost.Pool, noHost.Namespace)
	}
	if fmt.Sprint(noHost.Pins) != fmt.Sprint([]string{"build"}) {
		t.Errorf("refusal names pins %v, want [build]", noHost.Pins)
	}
	// The PINNED tasks, not every task on the pool: `publish` runs on gpu too
	// and is perfectly happy there.
	if fmt.Sprint(noHost.Tasks) != fmt.Sprint([]string{"clone", "compile"}) {
		t.Errorf("refusal names tasks %v, want the pinned ones in declaration order", noHost.Tasks)
	}
	message := err.Error()
	for _, want := range []string{
		"has live Orkers but none of them hosts pins",
		"ORK_ORKER_POOL=gpu ORK_ORKER_MAX_PINS=1",
		"allow_empty_pool",
	} {
		if !strings.Contains(message, want) {
			t.Errorf("refusal message = %q, want it to contain %q", message, want)
		}
	}
}

// TestPoolResolver_AcceptsAPinnedSubmitWhenThePoolHostsPins is the other side
// of the same probe: an Orker with ORK_ORKER_MAX_PINS above zero is a pin
// host, and nothing about the submit changes.
func TestPoolResolver_AcceptsAPinnedSubmitWhenThePoolHostsPins(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	resolver := &engine.PoolResolver{
		Probe:  livePools{gpuQueue: 1},
		Orkers: pinHostPools{gpuQueue: 1},
	}
	ns := &orkstackv1.Namespace{Name: "ci"}

	planned, plan, err := resolver.Plan(context.Background(), ns, compile(t, pinnedSubmitDocument), false)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Routes["gpu"] != gpuQueue {
		t.Errorf("plan routes gpu to %q, want %q", plan.Routes["gpu"], gpuQueue)
	}
	for _, task := range planned.GetTasks() {
		if task.GetPool() != "gpu" {
			t.Errorf("task %q resolved to pool %q, want gpu", task.GetId(), task.GetPool())
		}
	}
}

// TestPoolResolver_DowngradesTheNoPinHostRefusalUnderAllowEmptyPool holds the
// override to the same shape it has for an empty pool: the operator who
// overrides is TOLD what they are waiting for, and the fact goes on the record
// rather than being suppressed.
func TestPoolResolver_DowngradesTheNoPinHostRefusalUnderAllowEmptyPool(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	var logged strings.Builder
	resolver := &engine.PoolResolver{
		Probe:  livePools{gpuQueue: 1},
		Orkers: pinHostPools{gpuQueue: 0},
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	}
	ns := &orkstackv1.Namespace{Name: "ci"}

	if _, _, err := resolver.Plan(
		context.Background(), ns, compile(t, pinnedSubmitDocument), true); err != nil {
		t.Fatalf("Plan with allow_empty_pool = %v, want the refusal downgraded", err)
	}
	record := logged.String()
	for _, want := range []string{"no pin host", "pool=gpu", "pins=build", "allow_empty_pool"} {
		if !strings.Contains(record, want) {
			t.Errorf("override was not recorded with %q: %s", want, record)
		}
	}
}

// TestPoolResolver_AsksNothingAboutPinsForAnUnpinnedSubmit is "an unpinned
// submit is unchanged" stated as
// a test: pinning must cost a deployment that does not use it nothing at all,
// including one extra round trip on every submit.
func TestPoolResolver_AsksNothingAboutPinsForAnUnpinnedSubmit(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	resolver := &engine.PoolResolver{
		Probe:  livePools{gpuQueue: 1, task.DefaultQueue: 1},
		Orkers: refusingDescriber{t: t},
	}
	ns := &orkstackv1.Namespace{Name: "ci"}

	if _, _, err := resolver.Plan(context.Background(), ns, compile(t, twoPoolDocument), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
}

// TestPoolResolver_KeepsTheEmptyPoolAnswerForAPinnedSubmit keeps the two
// refusals in the right order. A pool with NO live Orkers is answered by
// PoolEmptyError, and what its absent Orkers would have hosted is moot: the
// remedy is to start one, and telling a caller about pin hosting first would
// send them to configure a process that does not exist.
func TestPoolResolver_KeepsTheEmptyPoolAnswerForAPinnedSubmit(t *testing.T) {
	resolver := &engine.PoolResolver{
		Probe:  livePools{},
		Orkers: pinHostPools{},
	}
	ns := &orkstackv1.Namespace{Name: "ci"}

	_, _, err := resolver.Plan(context.Background(), ns, compile(t, pinnedSubmitDocument), false)
	var empty *engine.PoolEmptyError
	if !errors.As(err, &empty) {
		t.Fatalf("Plan error = %v, want PoolEmptyError for a deserted pool", err)
	}
}

// TestPoolResolver_DoesNotRefusePinsWithNoAuthorityToAsk covers the two ways
// the check declines to have an opinion, and why each is right.
//
// A process with no describer wired has no authority to ask, exactly as one
// with no Probe has none about liveness — inventing an answer would refuse
// every pinned submit in a deployment that simply never wired the seam. And a
// describe call that FAILED measured nothing: the pool's liveness was measured
// and stands, and turning a describe blip into a refused submit would invent a
// failure the deployment does not have. The in-workflow backstop still covers
// a pool that genuinely cannot open a pin (PIN_UNAVAILABLE).
func TestPoolResolver_DoesNotRefusePinsWithNoAuthorityToAsk(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu", "")
	ns := &orkstackv1.Namespace{Name: "ci"}
	wf := compile(t, pinnedSubmitDocument)

	t.Run("no describer wired", func(t *testing.T) {
		resolver := &engine.PoolResolver{Probe: livePools{gpuQueue: 1}}
		if _, _, err := resolver.Plan(context.Background(), ns, wf, false); err != nil {
			t.Fatalf("Plan = %v, want no refusal from a process with nothing to ask", err)
		}
	})

	t.Run("the describe call failed", func(t *testing.T) {
		resolver := &engine.PoolResolver{
			Probe: livePools{gpuQueue: 1},
			// Maps no channel, so every describe fails.
			Orkers: pinHostPools{},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		if _, _, err := resolver.Plan(context.Background(), ns, wf, false); err != nil {
			t.Fatalf("Plan = %v, want a describe failure to narrow the answer, not refuse it", err)
		}
	})
}

// TestPoolResolver_RefusesAPinWhoseTasksRESOLVEOntoTwoPools is rule 10's
// second half, and it is not a duplicate of the compiler's.
//
// The compiler compares the LITERAL `pool:` values and treats an absent pool
// and `main` as one, because QueueForPool does. This document therefore
// compiles: `alpha` says `pool: main` and `beta` says nothing. But `beta`
// resolves through its NAMESPACE's default_pool, so the two land on different
// pools — and a pin is one Orker process out of one pool's set, which two
// pools cannot be. Only this check, on the resolved route, can see it.
func TestPoolResolver_RefusesAPinWhoseTasksRESOLVEOntoTwoPools(t *testing.T) {
	const document = `
name: pin-split-by-default
pins:
  build: {}
tasks:
  - id: alpha
    uses: shell
    pin: build
    pool: main
    run_as_orker_user: true
    with: {command: "echo alpha"}
  - id: beta
    uses: shell
    pin: build
    needs: [alpha]
    run_as_orker_user: true
    with: {command: "echo beta"}
`
	// It really does compile: the literal values agree under the compiler's
	// normalization, which is what makes this check load-bearing rather than
	// defensive.
	wf := compile(t, document)

	gpuQueue, _ := task.QueueForPool("gpu", "")
	resolver := &engine.PoolResolver{
		Probe:  livePools{gpuQueue: 1, task.DefaultQueue: 1},
		Orkers: pinHostPools{gpuQueue: 1, task.DefaultQueue: 1},
	}
	ns := &orkstackv1.Namespace{Name: "ci", DefaultPool: "gpu"}

	_, _, err := resolver.Plan(context.Background(), ns, wf, false)
	var mismatch *engine.PinPoolMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Plan error = %v, want PinPoolMismatchError", err)
	}
	if mismatch.Pin != "build" || mismatch.Namespace != "ci" {
		t.Errorf("refusal names pin %q of namespace %q, want build of ci", mismatch.Pin, mismatch.Namespace)
	}
	if mismatch.Task != "alpha" || mismatch.Pool != "main" {
		t.Errorf("refusal's first task = %q on %q, want alpha on main", mismatch.Task, mismatch.Pool)
	}
	if mismatch.OtherTask != "beta" || mismatch.OtherPool != "gpu" {
		t.Errorf("refusal's second task = %q on %q, want beta on gpu",
			mismatch.OtherTask, mismatch.OtherPool)
	}
	if !strings.Contains(err.Error(), "every task of one pin must name the same pool") {
		t.Errorf("refusal message = %q, want it to say what the caller must do", err.Error())
	}
}

// TestPoolResolver_RefusesAPinSpanningTwoPoolsOnAHandBuiltRequest is the same
// invariant on the path that skips the compiler entirely.
//
// SubmitRun accepts an Orkflow message, and a client that never applied a
// document — or one built by a harness — never ran validate.go. Every rule
// that protects an invariant rather than an author's convenience has to be
// enforced somewhere a request cannot go around, which is what this is.
func TestPoolResolver_RefusesAPinSpanningTwoPoolsOnAHandBuiltRequest(t *testing.T) {
	wf := &orkstackv1.Orkflow{
		Name:    "hand-built",
		Version: "0.1.0",
		Pins:    []*orkstackv1.Pin{{Name: "build"}},
		Tasks: []*orkstackv1.Task{
			{Id: "alpha", Pin: "build", Pool: "gpu"},
			{Id: "beta", Pin: "build", Pool: "cpu"},
		},
	}
	gpuQueue, _ := task.QueueForPool("gpu", "")
	cpuQueue, _ := task.QueueForPool("cpu", "")
	resolver := &engine.PoolResolver{
		Probe:  livePools{gpuQueue: 1, cpuQueue: 1},
		Orkers: pinHostPools{gpuQueue: 1, cpuQueue: 1},
	}

	_, _, err := resolver.Plan(context.Background(), &orkstackv1.Namespace{Name: "ci"}, wf, false)
	var mismatch *engine.PinPoolMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Plan error = %v, want PinPoolMismatchError", err)
	}
	if mismatch.Pool != "gpu" || mismatch.OtherPool != "cpu" {
		t.Errorf("refusal names pools %q and %q, want gpu and cpu", mismatch.Pool, mismatch.OtherPool)
	}
}

// countingProbe records every question Plan asks the cluster.
type countingProbe struct{ calls int }

func (p *countingProbe) ObservePool(context.Context, string) (engine.PoolLiveness, error) {
	p.calls++
	return engine.PoolLiveness{Orkers: 1}, nil
}

// TestPoolResolver_PlanRefusesASecretTheNamespaceDoesNotAllow: the submit
// gate refuses the secret before asking the cluster anything and before any
// pool is resolved — a document that would ALSO be refused for its pool is
// refused for its secret, the first refusal in Plan's order.
func TestPoolResolver_PlanRefusesASecretTheNamespaceDoesNotAllow(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name:           "sandbox",
		AllowedPools:   []string{"main"},
		AllowedSecrets: []string{"env:SANDBOX_*"},
	}
	wf := compile(t, `
name: exfil
version: "1"
tasks:
  - id: grab
    uses: shell
    pool: gpu
    secrets:
      - name: db
        source: env:PROD_DB_PASSWORD
    with:
      op: exec
      command: ["true"]
`)
	probe := &countingProbe{}
	resolver := &engine.PoolResolver{Probe: probe}
	_, _, err := resolver.Plan(context.Background(), ns, wf, false)
	var refused *engine.SecretNotAllowedError
	if !errors.As(err, &refused) {
		t.Fatalf("Plan = %v, want a *SecretNotAllowedError", err)
	}
	var pool *engine.PoolNotAllowedError
	if errors.As(err, &pool) {
		t.Fatalf("Plan refused the pool first: %v", err)
	}
	if probe.calls != 0 {
		t.Fatalf("Plan asked the probe %d times before refusing, want 0", probe.calls)
	}

	// allow_empty_pool is consulted later and never bypasses the check.
	if _, _, err := resolver.Plan(context.Background(), ns, wf, true); !errors.As(err, &refused) {
		t.Fatalf("Plan(allowEmpty) = %v, want a *SecretNotAllowedError", err)
	}

	// A permitted source passes on to the pool rules.
	wf.Tasks[0].Secrets[0].Source = "env:SANDBOX_DB"
	if _, _, err := resolver.Plan(context.Background(), ns, wf, false); !errors.As(err, &pool) {
		t.Fatalf("Plan with a permitted secret = %v, want the pool refusal next", err)
	}
}

// concurrentProbe holds its FIRST call open until a second call is in flight
// beside it, and records the most calls it ever saw in flight at once. A
// resolver that probes one pool at a time never sends that second call while
// the first is open, so the first gives up after its deadline and the high-water
// mark stays at one.
type concurrentProbe struct {
	mu       sync.Mutex
	inFlight int
	most     int
	first    bool
	second   chan struct{}
	arrived  bool
}

func (p *concurrentProbe) ObservePool(_ context.Context, queue string) (engine.PoolLiveness, error) {
	p.mu.Lock()
	p.inFlight++
	if p.inFlight > p.most {
		p.most = p.inFlight
	}
	if p.inFlight == 2 && !p.arrived {
		p.arrived = true
		close(p.second)
	}
	holdOpen := !p.first
	p.first = true
	p.mu.Unlock()
	if holdOpen {
		select {
		case <-p.second:
		case <-time.After(5 * time.Second):
		}
	}
	p.mu.Lock()
	p.inFlight--
	p.mu.Unlock()
	return engine.PoolLiveness{Orkers: len(queue), LastSeen: time.Now()}, nil
}

// TestPoolResolver_PoolsProbesItsPoolsTogether: one cluster probe per pool is
// sent with others in flight, not one at a time. An unrestricted namespace's
// report covers every pool its orkflows have ever named, so a serial walk's
// latency is the number of pools times one cluster round-trip: 277 pools
// against a loaded cluster took two minutes and every caller with a shorter
// deadline was answered "context canceled" for each pool not yet reached. The
// report itself — every pool once, in name order, each with its own count —
// is unchanged.
func TestPoolResolver_PoolsProbesItsPoolsTogether(t *testing.T) {
	var extra []string
	for i := 0; i < 40; i++ {
		extra = append(extra, fmt.Sprintf("pool-%02d", i))
	}
	probe := &concurrentProbe{second: make(chan struct{})}
	resolver := &engine.PoolResolver{Probe: probe, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ns := &orkstackv1.Namespace{Name: "default"}

	pools, err := resolver.Pools(context.Background(), ns, extra)
	if err != nil {
		t.Fatalf("Pools: %v", err)
	}
	probe.mu.Lock()
	most := probe.most
	probe.mu.Unlock()
	if most < 2 {
		t.Fatalf("at most %d probe in flight at once, want the pools probed together", most)
	}
	// The same bound every other admin read's round-trips keep (api's
	// storeFanout): together, never all at once.
	if most > 16 {
		t.Fatalf("%d probes in flight at once, want at most 16", most)
	}
	if len(pools) != len(extra)+1 {
		t.Fatalf("%d pools reported, want %d", len(pools), len(extra)+1)
	}
	for i, pool := range pools {
		if i > 0 && pools[i-1].GetName() >= pool.GetName() {
			t.Fatalf("pools out of name order at %d: %q then %q", i, pools[i-1].GetName(), pool.GetName())
		}
		queue, err := task.QueueForPool(pool.GetName(), "")
		if err != nil {
			t.Fatalf("queue for %q: %v", pool.GetName(), err)
		}
		if got, want := pool.GetLiveOrkerCount(), int32(len(queue)); got != want {
			t.Errorf("pool %q reports %d live Orkers, want its own probe's %d", pool.GetName(), got, want)
		}
	}
}
