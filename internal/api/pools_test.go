package api_test

import (
	"bytes"
	"context"
	"log/slog"
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

type poolPlugins map[string][]string

func (p poolPlugins) PluginsOnPool(_ context.Context, queue string) ([]string, error) {
	return p[queue], nil
}

// gpuOrkflow names a pool on one of its two tasks.
const gpuOrkflow = `
name: gpu-job
version: 0.1.0
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

func poolQueue(t *testing.T, pool string) string {
	t.Helper()
	queue, err := task.QueueForPool(pool)
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
		Pools:       livePools{task.DefaultQueue: 2, gpu: 1},
		PoolPlugins: poolPlugins{task.DefaultQueue: {"http", "shell"}, gpu: {"shell"}},
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
	if got := strings.Join(byName["gpu"].GetPlugins(), ","); got != "shell" {
		t.Errorf("pool gpu reports plugins %q, want shell", got)
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
