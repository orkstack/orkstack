package engine_test

import (
	"context"
	"errors"
	"strings"
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
    plugin: shell
    pool: gpu
    with: {command: "echo crunch"}
  - id: tidy
    plugin: shell
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
		gpuQueue, _ := task.QueueForPool("gpu")
		batchQueue, _ := task.QueueForPool("batch")
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

// TestPoolResolver_SendsTheDefaultRouteToTheOperatorsRawChannel keeps the
// escape hatch working: an engine started with ORK_ORKER_QUEUE dispatches a
// task that names no pool there, which is how a test harness isolates itself
// (AGENTS.md §6) with no change to any document.
//
// Dispatch and reporting must read the same map. One process holding two
// answers about one pool — accepting a submit because the operator's channel
// is served, and reporting that same pool deserted — is worse than either
// answer alone, because the number an operator reads is the wrong one.
func TestPoolResolver_SendsTheDefaultRouteToTheOperatorsRawChannel(t *testing.T) {
	const raw = "ork-tasks-m10-2-agent"
	gpuQueue, _ := task.QueueForPool("gpu")
	// Three Orkers on the operator's channel, one on gpu's own, and nobody on
	// the channel the default pool would have used without the override.
	resolver := &engine.PoolResolver{
		Probe:    livePools{raw: 3, gpuQueue: 1},
		Plugins:  fakePoolPlugins{raw: {"shell"}, gpuQueue: {"http"}},
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
	if got := strings.Join(byName[task.DefaultPool].GetPlugins(), ","); got != "shell" {
		t.Errorf("pool %q reports plugins %q, want the ones its Orkers run",
			task.DefaultPool, got)
	}
	// The override moved one route, so it moves one report.
	if got := byName["gpu"].GetLiveOrkerCount(); got != 1 {
		t.Errorf("pool gpu reports %d live Orkers, want 1", got)
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

// TestPoolResolver_RefusesASubmitOntoAPoolWithNoLiveOrkers converts the
// stranded-RUNNING failure AGENTS.md §6 describes into an error at submit.
//
// Before this, a task dispatched to a pool nothing serves left the run in
// RUNNING until a human noticed. The pool is asked, once, at the moment the
// routing decision is made.
func TestPoolResolver_RefusesASubmitOntoAPoolWithNoLiveOrkers(t *testing.T) {
	gpuQueue, err := task.QueueForPool("gpu")
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
    plugin: shell
    with: {command: "echo settle"}
`

// TestPoolResolver_ReportsPoolsWithTheirOrkersAndPlugins is the observability
// half: an operator asks which pools exist, how many Orkers serve each, and
// what those Orkers can run.
func TestPoolResolver_ReportsPoolsWithTheirOrkersAndPlugins(t *testing.T) {
	gpuQueue, _ := task.QueueForPool("gpu")
	resolver := &engine.PoolResolver{
		Probe:   livePools{task.DefaultQueue: 2, gpuQueue: 1},
		Plugins: fakePoolPlugins{gpuQueue: {"shell"}, task.DefaultQueue: {"http", "shell"}},
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
	if got := strings.Join(byName["gpu"].GetPlugins(), ","); got != "shell" {
		t.Errorf("pool gpu reports plugins %q, want shell", got)
	}
	// A pool nothing serves is reported, not hidden: "nobody is there" is the
	// answer an operator came for.
	if got := byName["cold"].GetLiveOrkerCount(); got != 0 {
		t.Errorf("pool cold has %d live Orkers, want 0", got)
	}
	if len(byName["cold"].GetPlugins()) != 0 {
		t.Errorf("pool cold reports plugins %v; there is nobody to ask", byName["cold"].GetPlugins())
	}
}

type fakePoolPlugins map[string][]string

func (f fakePoolPlugins) PluginsOnPool(_ context.Context, queue string) ([]string, error) {
	return f[queue], nil
}
