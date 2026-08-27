package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// registry is a namespace registry in a map: the documents an operator wrote,
// with the built-in default always resolving.
//
// It answers reads only. The pool tests never write through it, and a double
// that accepted a conditional write would vouch for arbitration it does not
// implement — the registry double that does implement it is fakeRegistry, in
// namespace_test.go.
type registry map[string]*orkstackv1.Namespace

func (r registry) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	if name == "" {
		name = "default"
	}
	if ns, ok := r[name]; ok {
		return ns, nil
	}
	if name == "default" {
		return &orkstackv1.Namespace{Name: "default"}, nil
	}
	return nil, artifacts.ErrNotFound
}

func (r registry) List(_ context.Context) ([]*orkstackv1.Namespace, error) {
	names := make([]string, 0, len(r))
	for name := range r {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.Namespace, 0, len(names))
	for _, name := range names {
		out = append(out, r[name])
	}
	return out, nil
}

func (r registry) Put(
	_ context.Context,
	_ *orkstackv1.Namespace,
	_ artifacts.PutConditions,
) (*orkstackv1.Namespace, error) {
	return nil, errors.New("registry: this double answers reads only")
}

func (r registry) Delete(_ context.Context, _ string) error {
	return errors.New("registry: this double answers reads only")
}

// livePools stands in for the cluster's poller information.
type livePools map[string]int

func (l livePools) ObservePool(_ context.Context, queue string) (engine.PoolLiveness, error) {
	orkers := l[queue]
	live := engine.PoolLiveness{Orkers: orkers}
	if orkers > 0 {
		live.LastSeen = time.Now()
	}
	return live, nil
}

type observedPools map[string]engine.PoolLiveness

func (p observedPools) ObservePool(_ context.Context, queue string) (engine.PoolLiveness, error) {
	return p[queue], nil
}

// poolOrkers answers the DescribeOrker probe for a set of channels.
//
// A channel present in the map has an Orker that answers; one absent does not.
// The VALUE is unused: under the OCI Activity Contract an Orker has no plugin
// list to report — which images exist is engine configuration (§4.2) — so
// answering AT ALL is the whole content of the probe (§6.5).
type poolOrkers map[string][]string

func (p poolOrkers) OrkerOnPool(
	_ context.Context,
	queue string,
) (*orkstackv1.DescribeOrkerResult, error) {
	if _, serving := p[queue]; !serving {
		return nil, fmt.Errorf("no orker is polling %q", queue)
	}
	return &orkstackv1.DescribeOrkerResult{OrkerId: "orker-" + queue, RuntimeName: "docker"}, nil
}

// gpuOrkflow names a pool on one of its two tasks.
const gpuOrkflow = `
name: gpu-job
version: 0.1.0
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

func poolQueue(t *testing.T, pool string) string {
	t.Helper()
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("QueueForPool(%q): %v", pool, err)
	}
	return queue
}

// TestApplyOrkflow_RefusesAPoolTheNamespaceDoesNotAllow is the authoring gate.
// An allow-list nothing enforces is a comment; this is the half that catches
// it with the file in hand.
func TestApplyOrkflow_RefusesAPoolTheNamespaceDoesNotAllow(t *testing.T) {
	svc := api.NewService(api.Config{
		Starter: &fakeStarter{},
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{"main", "batch"},
		}},
	})

	_, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: gpuOrkflow}))
	if err == nil {
		t.Fatal("applying an orkflow onto a forbidden pool succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("apply refused with %v, want InvalidArgument", code)
	}
	for _, want := range []string{"default", "crunch", "gpu", "batch"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q; the namespace, the task and the pool "+
				"are what make it fixable", err, want)
		}
	}
}

// TestSubmitRun_RefusesAPoolTheNamespaceDoesNotAllow is the gate that cannot
// be bypassed.
//
// It catches a definition stored while the allow-list was wider, and it runs
// on every submission path — including one from a client that never applied
// anything. The store is loaded directly here, exactly as a definition applied
// before the list tightened would be.
func TestSubmitRun_RefusesAPoolTheNamespaceDoesNotAllow(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{Orkflows: orkflows, Starter: starter})

	// Applied while nothing was forbidden.
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: gpuOrkflow})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// The list tightens afterwards. Same store, same definition.
	tightened := api.NewService(api.Config{
		Orkflows: orkflows,
		Starter:  starter,
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{"main"},
		}},
	})
	_, err := tightened.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "gpu-job"}))
	if err == nil {
		t.Fatal("submitting onto a forbidden pool succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("submit refused with %v, want InvalidArgument", code)
	}
	for _, want := range []string{"default", "crunch", "gpu"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	if len(starter.startedRuns()) != 0 {
		t.Errorf("a refused submission still started %v; nothing may reach Temporal", starter.startedRuns())
	}
}

// TestApplyAndSubmit_AgreeOnTheAllowListUnderABoundChannel is the two gates
// held against each other on a deployment that has an execution channel bound
// to a pool's own queue — ORK_ORKER_POOL=gpu on the engine (docs/DEPLOY.md),
// which is also the shape every parallel-agent harness runs.
//
// The allow-list bounds what a namespace may dispatch onto, so it is checked
// against the pool the tasks are dispatched to and reported on, not against
// the word the document happened to use for it. Checked before the collapse,
// apply and submit both accepted a document a namespace allowing only `main`
// then ran on gpu's Orkers, and both refused `pool: main` in a namespace that
// allows `gpu` — the same channel, refused for its other name.
func TestApplyAndSubmit_AgreeOnTheAllowListUnderABoundChannel(t *testing.T) {
	// A task that names no pool at all; under the bound channel it is gpu's.
	const unpinned = `
name: unpinned
version: 0.1.0
tasks:
  - id: settle
    uses: shell
    with: {command: "echo settle"}
`
	// The same channel, named the other way round.
	const mainPinned = `
name: main-pinned
version: 0.1.0
tasks:
  - id: settle
    uses: shell
    pool: main
    with: {command: "echo settle"}
`
	gpuQueue := poolQueue(t, "gpu")
	serviceFor := func(orkflows engine.OrkflowStore, starter *fakeStarter, allowed ...string) *api.Service {
		return api.NewService(api.Config{
			Orkflows: orkflows,
			Starter:  starter,
			Pools:    livePools{gpuQueue: 1},
			RawQueue: gpuQueue,
			Namespaces: registry{"default": {
				Name:         "default",
				AllowedPools: allowed,
			}},
		})
	}

	t.Run("a namespace that allows only main cannot reach gpu's Orkers", func(t *testing.T) {
		starter := &fakeStarter{}
		orkflows := engine.NewMemoryOrkflowStore()
		svc := serviceFor(orkflows, starter, "main")

		if _, err := svc.ApplyOrkflow(context.Background(),
			connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: unpinned})); err == nil {
			t.Error("apply accepted a document whose tasks would run on gpu's Orkers")
		}
		// Stored anyway — a definition applied before the channel was bound,
		// or written by a client that never applied.
		if _, err := api.NewService(api.Config{Orkflows: orkflows, Starter: starter}).ApplyOrkflow(
			context.Background(),
			connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: unpinned})); err != nil {
			t.Fatalf("Apply into an unrestricted namespace: %v", err)
		}
		_, err := svc.SubmitRun(context.Background(),
			connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "unpinned"}))
		if err == nil {
			t.Fatal("submitting onto gpu's Orkers succeeded in a namespace that allows only main")
		}
		if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
			t.Errorf("submit refused with %v, want InvalidArgument", code)
		}
		if !strings.Contains(err.Error(), "gpu") {
			t.Errorf("the refusal %q does not name the pool it would have dispatched to", err)
		}
		if len(starter.startedRuns()) != 0 {
			t.Errorf("a refused submission still started %v", starter.startedRuns())
		}
	})

	t.Run("a namespace that allows gpu accepts the channel's other name", func(t *testing.T) {
		starter := &fakeStarter{}
		svc := serviceFor(engine.NewMemoryOrkflowStore(), starter, "gpu")

		if _, err := svc.ApplyOrkflow(context.Background(),
			connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: mainPinned})); err != nil {
			t.Fatalf("apply refused `pool: main` in a namespace that allows gpu, which is "+
				"where this deployment dispatches it: %v", err)
		}
		if _, err := svc.SubmitRun(context.Background(),
			connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "main-pinned"})); err != nil {
			t.Fatalf("SubmitRun: %v", err)
		}
		started := starter.startedOrkflows()
		if len(started) != 1 {
			t.Fatalf("started %d runs, want 1", len(started))
		}
		if got := started[0].GetTasks()[0].GetPool(); got != "gpu" {
			t.Errorf("the task was dispatched on pool %q, want gpu", got)
		}
	})
}

// TestSubmitRun_FailsFastWhenTheResolvedPoolHasNoOrkers converts the stranded
// run of AGENTS.md §6 into an error at submit.
//
// Before this, the submission was accepted, the run reached RUNNING, and its
// tasks sat on a channel nobody polled until someone noticed. The submission
// is refused instead, and nothing is left behind: no run id, no workflow.
func TestSubmitRun_FailsFastWhenTheResolvedPoolHasNoOrkers(t *testing.T) {
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{
		Starter: starter,
		// main is served; gpu is not.
		Pools: livePools{task.DefaultQueue: 1},
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: gpuOrkflow})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	_, err := svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "gpu-job"}))
	if err == nil {
		t.Fatal("submitting into a pool with no Orkers succeeded; the run would strand in RUNNING")
	}
	if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
		t.Errorf("submit refused with %v, want FailedPrecondition — the request is well formed, "+
			"the deployment is not ready for it", code)
	}
	for _, want := range []string{"gpu", "crunch", "ORK_ORKER_POOL=gpu"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
	if len(starter.startedRuns()) != 0 {
		t.Errorf("a refused submission still started %v", starter.startedRuns())
	}
}

// unreachablePools is a probe that cannot answer: an unreachable cluster, or
// one slower than the probe's own bound.
type unreachablePools struct{}

func (unreachablePools) ObservePool(context.Context, string) (engine.PoolLiveness, error) {
	return engine.PoolLiveness{}, errors.New("dial tcp 127.0.0.1:7233: connection refused")
}

// TestSubmitRun_ReportsAnUncheckablePoolAsUnavailableNotAsABadRequest keeps a
// broken deployment from being reported as a broken request.
//
// InvalidArgument means "fix this and stop retrying". A Temporal hiccup makes
// every submission uncheckable for as long as it lasts, and every one of those
// requests is perfectly well formed — telling their authors to fix them sends
// the whole fleet to debug the wrong thing. Unavailable is the answer, and it
// is the same distinction the namespace lookup two functions away already
// makes, for the same reason (pools.go: NotFound vs Unavailable).
func TestSubmitRun_ReportsAnUncheckablePoolAsUnavailableNotAsABadRequest(t *testing.T) {
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{Starter: starter, Pools: unreachablePools{}})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: gpuOrkflow})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	_, err := svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "gpu-job"}))
	if err == nil {
		t.Fatal("submitting while no pool could be checked succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeUnavailable {
		t.Errorf("submit refused with %v, want Unavailable — the request is well formed, "+
			"and the instruction is 'ask again', not 'fix your request'", code)
	}
	if len(starter.startedRuns()) != 0 {
		t.Errorf("a submission nothing could check still started %v", starter.startedRuns())
	}
}

// TestSubmitRun_EmptyPoolOverrideIsNotTheDefaultAndIsLogged covers the escape
// hatch on both counts the acceptance asks for: off unless asked for, and on
// the record when taken.
func TestSubmitRun_EmptyPoolOverrideIsNotTheDefaultAndIsLogged(t *testing.T) {
	var logged bytes.Buffer
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{
		Starter: starter,
		Pools:   livePools{task.DefaultQueue: 1},
		Logger:  slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: gpuOrkflow})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Default: refused, and nothing logged about an override that was not taken.
	if _, err := svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "gpu-job"})); err == nil {
		t.Fatal("the empty-pool check is off by default")
	}
	if logged.Len() != 0 {
		t.Errorf("a refused submission logged an override: %q", logged.String())
	}

	// Asked for: accepted, and said out loud.
	if _, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName:    "gpu-job",
		AllowEmptyPool: true,
	})); err != nil {
		t.Fatalf("SubmitRun with allow_empty_pool: %v", err)
	}
	if len(starter.startedRuns()) != 1 {
		t.Fatalf("the override started %d runs, want 1", len(starter.startedRuns()))
	}
	record := logged.String()
	for _, want := range []string{"WARN", "empty pool", "pool=gpu", "allow_empty_pool"} {
		if !strings.Contains(record, want) {
			t.Errorf("the override log %q does not carry %q; an operator cannot explain "+
				"a waiting run from a log that does not mention the override", record, want)
		}
	}
}

// TestSubmitRun_BindsEveryTasksResolvedPoolAndItsRoute is the routing itself,
// read off what reached the starter.
//
// Two properties in one: every task carries a pool by the time it is started
// (its own, then the namespace's default, then the built-in "main"), and the
// route table covers exactly the pools the run uses. The workflow needs both,
// and can compute neither.
func TestSubmitRun_BindsEveryTasksResolvedPoolAndItsRoute(t *testing.T) {
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{
		Starter:    starter,
		Namespaces: registry{"default": {Name: "default", DefaultPool: "batch"}},
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: gpuOrkflow})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "gpu-job"})); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}

	started := starter.startedOrkflows()
	if len(started) != 1 {
		t.Fatalf("started %d runs, want 1", len(started))
	}
	wantPools := map[string]string{"crunch": "gpu", "tidy": "batch"}
	for _, tk := range started[0].GetTasks() {
		if got := tk.GetPool(); got != wantPools[tk.GetId()] {
			t.Errorf("task %q started on pool %q, want %q", tk.GetId(), got, wantPools[tk.GetId()])
		}
	}

	plan := starter.startedPlans()[0]
	want := map[string]string{"gpu": poolQueue(t, "gpu"), "batch": poolQueue(t, "batch")}
	if len(plan.Routes) != len(want) {
		t.Fatalf("plan routes = %v, want exactly %v", plan.Routes, want)
	}
	for pool, queue := range want {
		if plan.Routes[pool] != queue {
			t.Errorf("route for pool %q = %q, want %q", pool, plan.Routes[pool], queue)
		}
	}
}

// TestListPools_ReportsEveryPoolWithItsOrkersAndPlugins is the observability
// acceptance: name, live Orker count, and the plugins those Orkers run.
//
// Every number comes from the cluster, so a replica that started nothing gives
// the same answer as the one that started everything — which is why the test
// asks a Service with no runs, no bus and no history of its own.
func TestListPools_ReportsEveryPoolWithItsOrkersAndPlugins(t *testing.T) {
	gpu := poolQueue(t, "gpu")
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {
			Name:         "default",
			AllowedPools: []string{"main", "gpu", "cold"},
		}},
		Pools:      livePools{task.DefaultQueue: 2, gpu: 1},
		PoolOrkers: poolOrkers{task.DefaultQueue: {"http", "shell"}, gpu: {"shell"}},
	})

	resp, err := svc.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	if got := resp.Msg.GetDefaultPool(); got != task.DefaultPool {
		t.Errorf("default pool = %q, want %q", got, task.DefaultPool)
	}
	byName := map[string]*orkstackv1.Pool{}
	for _, pool := range resp.Msg.GetPools() {
		byName[pool.GetName()] = pool
	}
	if len(byName) != 3 {
		t.Fatalf("ListPools returned %v, want the namespace's three pools", byName)
	}
	if got := byName["main"].GetLiveOrkerCount(); got != 2 {
		t.Errorf("pool main reports %d live Orkers, want 2", got)
	}
	if !byName["main"].GetIsDefault() {
		t.Error("pool main is not marked as the default")
	}
	// A pool nothing serves is reported as empty rather than omitted: "nobody
	// is there" is the answer the question was asked for.
	if byName["cold"].GetLiveOrkerCount() != 0 || len(byName["cold"].GetPlugins()) != 0 {
		t.Errorf("pool cold = %v, want no Orkers and no plugins", byName["cold"])
	}
	if byName["main"].GetLastSeen() == nil {
		t.Error("a served pool reports no last_seen; an operator cannot judge staleness without it")
	}
}

// TestListPools_ReportsAStalePoolAsEmpty pins what a pool whose Orkers have
// stopped looks like on the wire.
//
// Temporal remembers a poller for minutes after its worker died, so "the
// cluster still lists someone" is not "someone is serving this pool". Past the
// freshness window the count is zero, and last_seen is absent — the production
// probe drops a stale poller record before it can contribute a timestamp
// (internal/engine/pool_probe.go, livePollers), so a last_seen for a dead pool
// is a value nothing observed. The wire contract says exactly this.
func TestListPools_ReportsAStalePoolAsEmpty(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	lastSeen := now.Add(-2 * engine.DefaultPollerFreshness)
	queue := poolQueue(t, task.DefaultPool)
	svc := api.NewService(api.Config{
		Namespaces: registry{"default": {Name: "default"}},
		Pools:      observedPools{queue: {Orkers: 1, LastSeen: lastSeen}},
		Now:        func() time.Time { return now },
	})
	resp, err := svc.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	if len(resp.Msg.GetPools()) != 1 {
		t.Fatalf("pools = %v, want one default pool", resp.Msg.GetPools())
	}
	pool := resp.Msg.GetPools()[0]
	if pool.GetLiveOrkerCount() != 0 {
		t.Errorf("stale pool count = %d, want zero", pool.GetLiveOrkerCount())
	}
	if pool.GetLastSeen() != nil {
		t.Errorf("stale pool last_seen = %v, want none: no fresh poller was observed",
			pool.GetLastSeen().AsTime())
	}
}

// TestListPools_IncludesPoolsFromEveryStoredVersion holds the scope of the
// report to every version that can still be submitted.
//
// An explicit-version submit can run ANY stored version, so a pool named only
// by an older one is a route this deployment still dispatches to. Reading the
// current version alone would drop `gpu` here, and an operator would size a
// pool they were told does not exist.
func TestListPools_IncludesPoolsFromEveryStoredVersion(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	gpu := poolQueue(t, "gpu")
	mainVersion := strings.Replace(gpuOrkflow, "version: 0.1.0", "version: 0.2.0", 1)
	mainVersion = strings.Replace(mainVersion, "    pool: gpu\n", "", 1)
	svc := api.NewService(api.Config{
		Orkflows:   store,
		Pools:      livePools{task.DefaultQueue: 1, gpu: 1},
		PoolOrkers: poolOrkers{task.DefaultQueue: {"shell"}, gpu: {"container"}},
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: gpuOrkflow})); err != nil {
		t.Fatalf("apply old gpu version: %v", err)
	}
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: mainVersion})); err != nil {
		t.Fatalf("apply current main version: %v", err)
	}
	// No allow-list, so the pools its stored definitions name are what "in
	// scope" means — which is the set this test is about.
	reader := api.NewService(api.Config{
		Orkflows:   store,
		Namespaces: registry{"default": {Name: "default"}},
		Pools:      livePools{task.DefaultQueue: 1, gpu: 1},
		PoolOrkers: poolOrkers{task.DefaultQueue: {"shell"}, gpu: {"container"}},
	})
	resp, err := reader.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	if got := strings.Join(poolNames(resp.Msg.GetPools()), ","); got != "gpu,main" {
		t.Fatalf("pools = %q, want gpu,main from both stored versions", got)
	}
}

// TestListPools_ReportsThePoolTheEngineActuallyDispatchesTo covers the
// configuration AGENTS.md §6 mandates for every parallel agent and
// docs/DEPLOY.md documents for a single-pool deployment: an engine started
// with ORK_ORKER_QUEUE.
//
// There the default pool's work goes to the operator's channel, so that is
// where its Orkers are. Both halves are asserted against one Service, because
// the failure is that they disagree: a submit accepted because the channel it
// dispatches to is served, and a report of the same pool, in the same process,
// in the same second, saying nobody is there. An operator reading that report
// scales up a pool that is already running their work.
func TestListPools_ReportsThePoolTheEngineActuallyDispatchesTo(t *testing.T) {
	const raw = "ork-tasks-m10-2-agent"
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{
		Starter:    starter,
		RawQueue:   raw,
		Pools:      livePools{raw: 3},
		PoolOrkers: poolOrkers{raw: {"http", "shell"}},
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// The submit is accepted: the pool's work goes to the operator's channel,
	// and three Orkers are polling it.
	if _, err := svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"})); err != nil {
		t.Fatalf("SubmitRun into the overridden default pool: %v", err)
	}
	if routes := starter.startedPlans()[0].Routes; routes[task.DefaultPool] != raw {
		t.Fatalf("the run's route for %q = %q, want the operator's channel %q",
			task.DefaultPool, routes[task.DefaultPool], raw)
	}

	resp, err := svc.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	var main *orkstackv1.Pool
	for _, pool := range resp.Msg.GetPools() {
		if pool.GetName() == task.DefaultPool {
			main = pool
		}
	}
	if main == nil {
		t.Fatalf("ListPools returned %v, without the default pool", resp.Msg.GetPools())
	}
	if got := main.GetLiveOrkerCount(); got != 3 {
		t.Errorf("pool %q reports %d live Orkers, want the 3 this same Service just "+
			"accepted a run onto", task.DefaultPool, got)
	}
}

// TestListPools_OneSetOfOrkersIsReportedOnce covers the other shape of the
// same configuration, the one docs/DEPLOY.md recommends: ORK_ORKER_POOL=gpu on
// both binaries, so the engine's bound channel IS gpu's own queue.
//
// Through the RPC a client actually calls, one set of Orkers must appear once,
// under the name its channel answers to, and a run that named no pool must be
// reported on that name. Two entries here is a client-visible lie: two pools
// to size, two to drain, one set of machines — and a run reported on `main`
// that gpu's Orkers executed.
func TestListPools_OneSetOfOrkersIsReportedOnce(t *testing.T) {
	gpuQueue, err := task.QueueForPool("gpu", "")
	if err != nil {
		t.Fatalf("QueueForPool(gpu): %v", err)
	}
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{
		Starter:    starter,
		RawQueue:   gpuQueue, // what ORK_ORKER_POOL=gpu binds on the engine
		Pools:      livePools{gpuQueue: 2},
		PoolOrkers: poolOrkers{gpuQueue: {"shell"}},
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"})); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	routes := starter.startedPlans()[0].Routes
	if len(routes) != 1 || routes["gpu"] != gpuQueue {
		t.Fatalf("the run's routes = %v, want exactly {gpu: %s}: an orkflow that names no pool "+
			"runs on the pool this deployment bound", routes, gpuQueue)
	}

	resp, err := svc.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	if n := len(resp.Msg.GetPools()); n != 1 {
		t.Fatalf("ListPools returned %d pools (%v) for one set of Orkers", n, resp.Msg.GetPools())
	}
	only := resp.Msg.GetPools()[0]
	if only.GetName() != "gpu" || !only.GetIsDefault() {
		t.Errorf("the pool is reported as %q (default=%v), want gpu marked default",
			only.GetName(), only.GetIsDefault())
	}
	if got := only.GetLiveOrkerCount(); got != 2 {
		t.Errorf("pool %q reports %d live Orkers, want the 2 polling its channel", only.GetName(), got)
	}
	if got := resp.Msg.GetDefaultPool(); got != "gpu" {
		t.Errorf("ListPools says the default pool is %q, want %q — a task that names none runs "+
			"on gpu's Orkers under this deployment", got, "gpu")
	}
}

// downOrkflowStore is the durable orkflow store during a bucket wobble: every
// call fails the way a client reports a refused connection.
//
// It implements the whole of engine.OrkflowStore, version operations
// included: a wobble takes the version catalog and the current pointer down
// with the definitions, because all four live in the same bucket. Only Get
// and List are exercised below — the failure this test measures is a read
// that ListPools makes — but a double that answered a promotion while
// refusing a list would describe a store that cannot exist.
type downOrkflowStore struct{ err error }

func (d downOrkflowStore) Put(
	context.Context, *orkstackv1.Orkflow, artifacts.HeadExpectation,
) (*orkstackv1.Orkflow, bool, error) {
	return nil, false, d.err
}

func (d downOrkflowStore) Get(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
	return nil, d.err
}

func (d downOrkflowStore) List(context.Context, string) ([]*orkstackv1.Orkflow, error) {
	return nil, d.err
}

func (d downOrkflowStore) Versions(context.Context, string, string) (artifacts.OrkflowVersions, error) {
	return artifacts.OrkflowVersions{}, d.err
}

func (d downOrkflowStore) SetCurrentVersion(context.Context, string, string, string) (string, error) {
	return "", d.err
}

// TestListPools_FallsBackToTheNamespacesOwnPoolsWhenTheOrkflowStoreIsDown is
// the degradation the code promises, measured.
//
// ListPools answers two questions from two sources: WHICH pools are in scope
// (the namespace document, plus — for a namespace with no allow-list — the
// pools its stored definitions name) and WHO is serving them (the cluster).
// Only the first needs the orkflow store, and it is the less important half.
// Pool health during a bucket wobble is precisely when an operator is looking,
// so a store that cannot be listed narrows the answer instead of replacing it
// with an error, and says so in the log.
func TestListPools_FallsBackToTheNamespacesOwnPoolsWhenTheOrkflowStoreIsDown(t *testing.T) {
	batch := poolQueue(t, "batch")
	gpu := poolQueue(t, "gpu")
	payments := registry{"payments": {Name: "payments", DefaultPool: "batch"}}
	live := livePools{batch: 2, gpu: 1}
	plugins := poolOrkers{batch: {"shell"}, gpu: {"shell"}}

	// The control: with the store up, the pool an orkflow names is in scope.
	healthy := engine.NewMemoryOrkflowStore()
	up := api.NewService(api.Config{
		Orkflows:   healthy,
		Namespaces: payments,
		Pools:      live,
		PoolOrkers: plugins,
	})
	if _, err := up.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml:      gpuOrkflow,
			Namespace: "payments",
		})); err != nil {
		t.Fatalf("Apply into payments: %v", err)
	}
	upResp, err := up.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{Namespace: "payments"}))
	if err != nil {
		t.Fatalf("ListPools with a healthy store: %v", err)
	}
	if got := poolNames(upResp.Msg.GetPools()); strings.Join(got, ",") != "batch,gpu" {
		t.Fatalf("ListPools with a healthy store = %v, want [batch gpu]: the fallback below "+
			"is only meaningful if the store contributes gpu here", got)
	}

	// The degradation.
	logs := &bytes.Buffer{}
	refused := errors.New("dial tcp 127.0.0.1:9000: connect: connection refused")
	down := api.NewService(api.Config{
		Orkflows:   downOrkflowStore{err: refused},
		Namespaces: payments,
		Pools:      live,
		PoolOrkers: plugins,
		Logger:     slog.New(slog.NewTextHandler(logs, nil)),
	})

	resp, err := down.ListPools(context.Background(),
		connect.NewRequest(&orkstackv1.ListPoolsRequest{Namespace: "payments"}))
	if err != nil {
		t.Fatalf("ListPools with an unreachable orkflow store: %v — a store failure must "+
			"narrow the report, not replace it", err)
	}
	if got := poolNames(resp.Msg.GetPools()); strings.Join(got, ",") != "batch" {
		t.Fatalf("ListPools = %v, want [batch]: the namespace's own pools are what is left "+
			"when the definitions cannot be read", got)
	}
	// The half that never needed the store still has to be right: liveness
	// comes from the cluster.
	only := resp.Msg.GetPools()[0]
	if only.GetLiveOrkerCount() != 2 || !only.GetIsDefault() {
		t.Errorf("pool batch = %v, want 2 live Orkers and the default mark", only)
	}
	if got := resp.Msg.GetDefaultPool(); got != "batch" {
		t.Errorf("default pool = %q, want batch", got)
	}
	// A narrowed answer that says nothing is indistinguishable from a complete
	// one, so the degradation is logged with what could not be read. The
	// namespace is asserted as its own ATTRIBUTE rather than as a substring of
	// the record: the wrapped error names it too, so a substring passes for a
	// log that dropped the field an operator filters on.
	for _, want := range []string{
		"namespace=payments",
		"orkflow store could not be read",
		"connection refused",
	} {
		if !strings.Contains(logs.String(), want) {
			// The assertion names the missing attribute and nothing else. The
			// record itself is evidence for a human, and it goes on its own
			// line: it carries a wall clock and its own `level=`, so a failure
			// line containing it can be neither quoted by a proof that
			// survives a rerun nor told apart from a subprocess's log.
			t.Errorf("the degradation log does not carry %q", want)
		}
	}
	if t.Failed() {
		t.Logf("the log said: %s", logs.String())
	}
}

func poolNames(pools []*orkstackv1.Pool) []string {
	names := make([]string, 0, len(pools))
	for _, pool := range pools {
		names = append(names, pool.GetName())
	}
	sort.Strings(names)
	return names
}
