//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// orkctl-release §5 against a real cluster. Each test provisions its own
// namespace and deployment name: a deployment's routing is namespace-wide
// state, and the pool channels (ork-pool-build) are fixed names.

// releaseWorld is one test's namespace, deployment and channels.
type releaseWorld struct {
	t          *testing.T
	ctx        context.Context
	c          client.Client
	namespace  string
	deployment string
	runQueue   string
	orkerQueue string
	snapshots  engine.SnapshotStore
	cluster    *engine.TemporalReleaseCluster
	starter    *engine.TemporalStarter
	transcript []string
	started    time.Time
}

func newReleaseWorld(t *testing.T, timeout time.Duration) *releaseWorld {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	namespace := versioningNamespace() + "-rel-" + suffix
	w := &releaseWorld{
		t:          t,
		ctx:        ctx,
		c:          dialVersioningCluster(t, ctx, namespace),
		namespace:  namespace,
		deployment: "ork-release-" + suffix,
		runQueue:   "ork-release-run-" + suffix,
		orkerQueue: "ork-release-tasks-" + suffix,
		snapshots:  newTestSnapshotStore(),
		started:    time.Now(),
	}
	cluster, err := engine.NewTemporalReleaseCluster(w.c, engine.NewTemporalRunReader(w.c),
		namespace, w.deployment, w.runQueue, w.orkerQueue)
	if err != nil {
		t.Fatalf("NewTemporalReleaseCluster: %v", err)
	}
	w.cluster = cluster
	w.starter = engine.NewTemporalStarterWithOrkerQueue(w.c, w.runQueue, w.orkerQueue)
	w.step("namespace " + namespace + ", deployment " + w.deployment)
	return w
}

func (w *releaseWorld) version(build string) task.Deployment {
	return task.Deployment{Name: w.deployment, BuildID: build}
}

func (w *releaseWorld) step(line string) {
	w.transcript = append(w.transcript, fmt.Sprintf("[%7.2fs] %s", time.Since(w.started).Seconds(), line))
	w.t.Log(line)
}

func (w *releaseWorld) write(name string) {
	proof.Write(w.t, "engine", name, []byte(strings.Join(w.transcript, "\n")+"\n"))
}

// releaseOrker is one stub Orker of a release on a named pool's channel. It
// records which build ran each task and can hold a task open.
type releaseOrker struct {
	w        *releaseWorld
	build    string
	worker   worker.Worker
	mu       sync.Mutex
	ran      map[string]string // runID/taskID -> build
	holdTask string
	hold     chan struct{}
	held     chan struct{}
	heldOnce sync.Once
}

func (w *releaseWorld) startPoolOrker(build, pool string, holdTask string) *releaseOrker {
	w.t.Helper()
	route, err := task.RouteForPool(pool, w.orkerQueue)
	if err != nil {
		w.t.Fatalf("route %s: %v", pool, err)
	}
	o := &releaseOrker{w: w, build: build, ran: map[string]string{}, holdTask: holdTask,
		hold: make(chan struct{}), held: make(chan struct{})}
	o.worker = worker.New(w.c, route.Queue, w.version(build).Apply(worker.Options{
		// Every stub Orker lives in this one test process, so each is given
		// an identity of its own: the cluster counts processes by identity,
		// and two workers sharing the SDK's pid@host default are one poller.
		Identity:                    fmt.Sprintf("orker-%s-%s@%s", build, pool, w.deployment),
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}))
	o.worker.RegisterActivityWithOptions(
		func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			o.mu.Lock()
			o.ran[in.RunId+"/"+in.TaskId] = build
			o.mu.Unlock()
			if in.TaskId == o.holdTask {
				o.heldOnce.Do(func() { close(o.held) })
				select {
				case <-o.hold:
				case <-actCtx.Done():
					return nil, actCtx.Err()
				}
			}
			return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId,
				ValuesJson: valuesJSON(map[string]any{"executed_by_build": build})}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	if err := o.worker.Start(); err != nil {
		w.t.Fatalf("start %s orker on %s: %v", build, pool, err)
	}
	w.t.Cleanup(o.worker.Stop)
	return o
}

func (o *releaseOrker) ranBy(key string) (string, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	build, ok := o.ran[key]
	return build, ok
}

// startEngine starts one release's engine: the run worker on the run channel
// and an unversioned projection worker.
func (w *releaseWorld) startEngine(build string) {
	w.t.Helper()
	run := worker.New(w.c, w.runQueue, w.version(build).Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	engine.RegisterWorkflows(run)
	run.RegisterActivityWithOptions(engine.NewSnapshotActivities(w.snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	if err := run.Start(); err != nil {
		w.t.Fatalf("start engine %s: %v", build, err)
	}
	w.t.Cleanup(run.Stop)
}

// startProjection starts the unversioned projection worker once per world.
func (w *releaseWorld) startProjection() {
	w.t.Helper()
	events := worker.New(w.c, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	inner := engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(w.snapshots)
	events.RegisterActivityWithOptions(inner.PublishEvent, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	if err := events.Start(); err != nil {
		w.t.Fatalf("start projection worker: %v", err)
	}
	w.t.Cleanup(events.Stop)
}

// awaitRegistered waits until the cluster has seen build poll every channel.
func (w *releaseWorld) awaitRegistered(build string, queues ...string) {
	w.t.Helper()
	handle := w.c.WorkerDeploymentClient().GetHandle(w.deployment)
	deadline := time.Now().Add(60 * time.Second)
	for {
		described, err := handle.DescribeVersion(w.ctx, client.WorkerDeploymentDescribeVersionOptions{BuildID: build})
		if err == nil && pollsEveryQueue(described, queues) {
			return
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("release %s never registered %v (last error %v)", build, queues, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// observe reads the deployment until want holds or 60 s pass.
func (w *releaseWorld) observe(what string, want func(engine.ReleaseObservation) bool) engine.ReleaseObservation {
	w.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var obs engine.ReleaseObservation
	var err error
	for {
		obs, err = w.cluster.ObserveReleases(w.ctx)
		if err == nil && want(obs) {
			return obs
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("%s: never observed (last %+v, error %v)", what, obs, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func releaseOf(obs engine.ReleaseObservation, build string) (engine.ObservedRelease, bool) {
	for _, r := range obs.Releases {
		if r.BuildID == build {
			return r, true
		}
	}
	return engine.ObservedRelease{}, false
}

// setCurrent makes build current through the seam, guards on.
func (w *releaseWorld) setCurrent(build string) {
	w.t.Helper()
	obs, err := w.cluster.ObserveReleases(w.ctx)
	if err != nil {
		w.t.Fatalf("observe before promoting %s: %v", build, err)
	}
	want := engine.Routing{Current: build, Ramping: obs.Routing.Ramping, RampPercent: obs.Routing.RampPercent}
	if obs.Routing.Ramping == build {
		want.Ramping, want.RampPercent = "", 0
	}
	if err := w.cluster.SetRouting(w.ctx, obs, want); err != nil {
		w.t.Fatalf("promote %s: %v", build, err)
	}
	w.step("SetRouting: current = " + build)
}

func buildTaskOrkflow(name string) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name: name, Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "first", Uses: "shell", TimeoutSeconds: 300, With: mustStruct(map[string]any{"run": "echo first"})},
			{Id: "on-build", Uses: "shell", Pool: "build", Needs: []string{"first"}, TimeoutSeconds: 300,
				With: mustStruct(map[string]any{"run": "echo build"})},
		},
	}
}

func heldOrkflow(name, taskID string) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name: name, Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: taskID, Uses: "shell", TimeoutSeconds: 300, With: mustStruct(map[string]any{"run": "echo held"})},
		},
	}
}

func TestIntegration_ObserveReleasesSeesRolesEnginesAndRegisteredPools(t *testing.T) {
	w := newReleaseWorld(t, 4*time.Minute)
	w.startProjection()
	w.startEngine("v1")
	w.startPoolOrker("v1", "main", "")
	w.startPoolOrker("v1", "build", "")
	w.awaitRegistered("v1", w.runQueue, w.orkerQueue, "ork-pool-build")
	w.setCurrent("v1")
	w.startEngine("v2")
	w.startPoolOrker("v2", "main", "")
	w.awaitRegistered("v2", w.runQueue, w.orkerQueue)

	// Ramp v2 through the cluster directly: v2 has no build Orker, which is
	// exactly what OrkStack's own check would refuse, and this test is about
	// what the seam OBSERVES of a ramping release, not about the check.
	handle := w.c.WorkerDeploymentClient().GetHandle(w.deployment)
	if _, err := handle.SetRampingVersion(w.ctx, client.WorkerDeploymentSetRampingVersionOptions{
		BuildID: "v2", Percentage: 20, IgnoreMissingTaskQueues: true}); err != nil {
		t.Fatalf("ramp v2: %v", err)
	}
	w.step("v1 current (engine, main and build Orkers); v2 ramping at 20% (engine, main Orker)")

	obs := w.observe("roles, engines and pools", func(o engine.ReleaseObservation) bool {
		v1, _ := releaseOf(o, "v1")
		return o.EngineReplicas["v1"] == 1 && o.EngineReplicas["v2"] == 1 &&
			len(v1.RegisteredPools) == 2 && o.PoolUse["build"].LivePollers == 1
	})
	if want := (engine.Routing{Current: "v1", Ramping: "v2", RampPercent: 20}); obs.Routing != want {
		t.Fatalf("routing = %+v, want %+v", obs.Routing, want)
	}
	v1, _ := releaseOf(obs, "v1")
	v2, _ := releaseOf(obs, "v2")
	if !reflect.DeepEqual(v1.RegisteredPools, []string{"build", "main"}) {
		t.Fatalf("v1 registered pools = %v", v1.RegisteredPools)
	}
	if !reflect.DeepEqual(v2.RegisteredPools, []string{"main"}) {
		t.Fatalf("v2 registered pools = %v", v2.RegisteredPools)
	}
	if obs.CurrentSince.IsZero() || obs.RampingSince.IsZero() || !v1.HasServed {
		t.Fatalf("since/served: %v %v %v", obs.CurrentSince, obs.RampingSince, v1.HasServed)
	}
	w.step(fmt.Sprintf("observed routing=%+v engines=%v v1 pools=%v v2 pools=%v pool use build=%+v",
		obs.Routing, obs.EngineReplicas, v1.RegisteredPools, v2.RegisteredPools, obs.PoolUse["build"]))
	w.write("releases-observe.txt")
}

func TestIntegration_ReleaseRunsCountsARunExecutingOnIt(t *testing.T) {
	w := newReleaseWorld(t, 4*time.Minute)
	w.startProjection()
	w.startEngine("v1")
	held := w.startPoolOrker("v1", "main", "held")
	w.awaitRegistered("v1", w.runQueue, w.orkerQueue)
	w.setCurrent("v1")
	w.startEngine("v2")
	w.startPoolOrker("v2", "main", "")
	w.awaitRegistered("v2", w.runQueue, w.orkerQueue)

	runID := "ork-release-runs-" + fmt.Sprint(time.Now().UnixNano())
	if err := w.starter.StartRun(w.ctx, runID, heldOrkflow("release-runs", "held"), nil, rawPlan(w.orkerQueue)); err != nil {
		t.Fatalf("start run: %v", err)
	}
	defer close(held.hold)
	select {
	case <-held.held:
	case <-w.ctx.Done():
		t.Fatal("the run never reached its held task")
	}
	w.step("run " + runID + " is executing on v1 (its task is held)")

	deadline := time.Now().Add(10 * time.Second)
	var n int64
	var runs []*orkstackv1.Run
	var err error
	for {
		n, runs, err = w.cluster.ReleaseRuns(w.ctx, "v1", 10)
		if err == nil && n == 1 && len(runs) == 1 && runs[0].GetId() == runID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ReleaseRuns(v1) = %d, %v, %v; want 1 listing %s within 10s", n, runs, err, runID)
		}
		time.Sleep(250 * time.Millisecond)
	}
	indexed, _ := task.IndexedVersion(w.deployment, "v1")
	w.step(fmt.Sprintf("ReleaseRuns(v1) counts %d and lists %s — the run index spells the release %s=%q",
		n, runs[0].GetId(), task.SearchAttrDeploymentVersion, indexed))
	n2, runs2, err := w.cluster.ReleaseRuns(w.ctx, "v2", 10)
	if err != nil || n2 != 0 || len(runs2) != 0 {
		t.Fatalf("ReleaseRuns(v2) = %d, %v, %v; want 0", n2, runs2, err)
	}
	w.step("ReleaseRuns(v2) counts 0")
	total, err := w.cluster.RunningRuns(w.ctx)
	if err != nil || total != 1 {
		t.Fatalf("RunningRuns = %d, %v", total, err)
	}
	w.step(fmt.Sprintf("RunningRuns counts %d", total))
	w.write("releases-runs-count.txt")
}

func TestIntegration_PoolUseSeesWaitingWorkWithNoPoller(t *testing.T) {
	w := newReleaseWorld(t, 5*time.Minute)
	w.cluster.Freshness = 10 * time.Second
	w.startProjection()
	w.startEngine("v1")
	w.startPoolOrker("v1", "main", "")
	build := w.startPoolOrker("v1", "build", "")
	w.awaitRegistered("v1", w.runQueue, w.orkerQueue, "ork-pool-build")
	w.setCurrent("v1")

	build.worker.Stop()
	w.step("stopped v1's build Orker")
	runID := "ork-release-backlog-" + fmt.Sprint(time.Now().UnixNano())
	if err := w.starter.StartRun(w.ctx, runID, buildTaskOrkflow("release-backlog"), nil,
		rawPlan(w.orkerQueue, "build")); err != nil {
		t.Fatalf("start run: %v", err)
	}
	w.step("submitted " + runID + ": its second task names pool build")

	deadline := time.Now().Add(90 * time.Second)
	var use engine.PoolUse
	for {
		obs, err := w.cluster.ObserveReleases(w.ctx)
		if err == nil {
			use = obs.PoolUse["build"]
			if use.Err == nil && use.LivePollers == 0 && use.Backlog >= 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool build never showed waiting work with no poller: last %+v (err %v)", use, err)
		}
		time.Sleep(time.Second)
	}
	w.step(fmt.Sprintf("PoolUse[build] = live pollers %d, backlog %d, add rate %g — stats come back in the enhanced description",
		use.LivePollers, use.Backlog, use.AddRate))
	w.write("releases-pool-use.txt")
}

func TestIntegration_TheClustersOwnGuardAdmitsAReleaseMissingAQuietPool(t *testing.T) {
	w := newReleaseWorld(t, 5*time.Minute)
	w.startProjection()
	w.startEngine("v1")
	w.startPoolOrker("v1", "main", "")
	v1Build := w.startPoolOrker("v1", "build", "")
	w.awaitRegistered("v1", w.runQueue, w.orkerQueue, "ork-pool-build")
	w.setCurrent("v1")
	w.step("v1 current with Orkers on main and build; no task was ever sent to build")

	w.startEngine("v2")
	v2Main := w.startPoolOrker("v2", "main", "")
	w.awaitRegistered("v2", w.runQueue, w.orkerQueue)
	w.step("v2 has an engine and a main Orker only")

	// The cluster's own guard, ON: IgnoreMissingTaskQueues false.
	handle := w.c.WorkerDeploymentClient().GetHandle(w.deployment)
	if _, err := handle.SetCurrentVersion(w.ctx, client.WorkerDeploymentSetCurrentVersionOptions{BuildID: "v2"}); err != nil {
		t.Fatalf("the cluster's guard refused v2 (missing build): %v — the hazard this test measures did not occur", err)
	}
	w.step("SetCurrentVersion(v2) with the cluster's guard ON succeeded although v2 has no Orker on build")

	runID := "ork-release-quiet-" + fmt.Sprint(time.Now().UnixNano())
	if err := w.starter.StartRun(w.ctx, runID, buildTaskOrkflow("release-quiet"), nil,
		rawPlan(w.orkerQueue, "build")); err != nil {
		t.Fatalf("start run: %v", err)
	}
	w.step("submitted " + runID + " after the promotion; its second task names pool build")

	key := runID + "/on-build"
	deadline := time.Now().Add(30 * time.Second)
	outcome := "waited 30s: no Orker ran it"
	for time.Now().Before(deadline) {
		if build, ok := v1Build.ranBy(key); ok {
			outcome = "ran on v1's build Orker (build " + build + "), not on its own release's"
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, ok := v2Main.ranBy(key); ok {
		t.Fatalf("the build task ran on v2's main Orker, which does not poll build")
	}
	first, _ := v2Main.ranBy(runID + "/first")
	w.step(fmt.Sprintf("the run's first task ran on build %q; its build task: %s", first, outcome))
	w.write("releases-cluster-guard-hazard.txt")
}

func TestIntegration_SetRoutingIsConfirmedOnTheEngineChannel(t *testing.T) {
	w := newReleaseWorld(t, 5*time.Minute)
	w.startProjection()
	w.startEngine("v1")
	w.startPoolOrker("v1", "main", "")
	w.awaitRegistered("v1", w.runQueue, w.orkerQueue)
	w.setCurrent("v1")
	w.startEngine("v2")
	v2 := w.startPoolOrker("v2", "main", "")
	w.awaitRegistered("v2", w.runQueue, w.orkerQueue)

	// A ramp, confirmed.
	obs, err := w.cluster.ObserveReleases(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	ramp := engine.Routing{Current: "v1", Ramping: "v2", RampPercent: 30}
	if err := w.cluster.SetRouting(w.ctx, obs, ramp); err != nil {
		t.Fatalf("ramp: %v", err)
	}
	start := time.Now()
	ok, err := w.cluster.AwaitRouting(w.ctx, ramp, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("AwaitRouting(ramp) = %v, %v", ok, err)
	}
	w.step(fmt.Sprintf("ramp v2 30%%: routing confirmed on the engine's channel after %s", time.Since(start).Round(time.Millisecond)))

	// A promote, confirmed, and the next run executes on the promoted release.
	obs, err = w.cluster.ObserveReleases(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	promote := engine.Routing{Current: "v2"}
	if err := w.cluster.SetRouting(w.ctx, obs, promote); err != nil {
		t.Fatalf("promote: %v", err)
	}
	start = time.Now()
	ok, err = w.cluster.AwaitRouting(w.ctx, promote, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("AwaitRouting(promote) = %v, %v", ok, err)
	}
	w.step(fmt.Sprintf("promote v2: routing confirmed on the engine's channel after %s", time.Since(start).Round(time.Millisecond)))

	runID := "ork-release-confirmed-" + fmt.Sprint(time.Now().UnixNano())
	if err := w.starter.StartRun(w.ctx, runID, heldOrkflow("release-confirmed", "only"), nil, rawPlan(w.orkerQueue)); err != nil {
		t.Fatalf("start run: %v", err)
	}
	if err := w.c.GetWorkflow(w.ctx, runID, "").Get(w.ctx, nil); err != nil {
		t.Fatalf("run %s: %v", runID, err)
	}
	if build, ok := v2.ranBy(runID + "/only"); !ok || build != "v2" {
		t.Fatalf("the run submitted right after the promote did not execute on v2 (%q, %v)", build, ok)
	}
	assertRunVersionPlain(t, w.ctx, w.c, runID, w.version("v2"))
	w.step("run " + runID + ", submitted immediately afterwards, executed on v2")
	w.write("releases-routing-confirmed.txt")
}

// assertRunVersionPlain reads the cluster's own record of which release a run
// executes on.
func assertRunVersionPlain(t *testing.T, ctx context.Context, c client.Client, runID string, want task.Deployment) {
	t.Helper()
	described, err := c.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatalf("describe %s: %v", runID, err)
	}
	version := described.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion()
	if version.GetBuildId() != want.BuildID || version.GetDeploymentName() != want.Name {
		t.Fatalf("run %s is on %s:%s, want %s", runID, version.GetDeploymentName(), version.GetBuildId(), want)
	}
}

// TestIntegration_ObservePoolCountsAReleasesOrkers holds the pool liveness
// probe — what a submit asks before it accepts a run — to the Orkers of a
// release. With every Orker of a pool started with ORK_BUILD_ID, the enhanced
// channel read this probe used before reported the pool empty, and every
// submit onto it was refused.
func TestIntegration_ObservePoolCountsAReleasesOrkers(t *testing.T) {
	w := newReleaseWorld(t, 3*time.Minute)
	w.startProjection()
	w.startEngine("v1")
	w.startPoolOrker("v1", "main", "")
	w.awaitRegistered("v1", w.runQueue, w.orkerQueue)
	w.setCurrent("v1")
	w.startPoolOrker("v2", "main", "")
	w.awaitRegistered("v2", w.orkerQueue)

	probe := engine.NewTemporalPoolProbe(w.c)
	deadline := time.Now().Add(60 * time.Second)
	var live engine.PoolLiveness
	var err error
	for {
		live, err = probe.ObservePool(w.ctx, w.orkerQueue)
		if err == nil && live.Orkers == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ObservePool = %+v, %v; want the two release Orkers (v1 current, v2 a candidate)", live, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	w.step(fmt.Sprintf("ObservePool counts %d Orkers on a pool served only by Orkers of releases v1 and v2", live.Orkers))
	w.write("releases-pool-probe.txt")
}
