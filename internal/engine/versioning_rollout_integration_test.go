//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// versioningNamespace isolates this suite. AGENTS.md §6: a namespace is a hard
// partition, and this test sets a deployment's CURRENT version — a
// namespace-wide routing change no other agent's runs may see.
func versioningNamespace() string {
	if ns := os.Getenv("ORK_TEMPORAL_NAMESPACE"); ns != "" {
		return ns
	}
	return "ork-m8-4-worker-versioning"
}

// dialVersioningCluster connects to a real Temporal, provisioning the isolated
// namespace, and skips cleanly when no cluster is there.
func dialVersioningCluster(t *testing.T, ctx context.Context, namespace string) client.Client {
	t.Helper()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	bootstrap, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	if err := task.EnsureNamespace(ctx, bootstrap, namespace); err != nil {
		bootstrap.Close()
		t.Fatalf("provision namespace %q: %v", namespace, err)
	}
	bootstrap.Close()

	// A freshly registered namespace is not immediately in the frontend's
	// cache, so the first dial is retried rather than failed.
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
		if err == nil {
			if _, probe := c.DescribeTaskQueue(ctx, "ork-versioning-probe", 0); probe == nil {
				t.Cleanup(c.Close)
				return c
			}
			c.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %q did not become usable: %v", namespace, err)
		}
		time.Sleep(time.Second)
	}
}

// setCurrentVersion makes deployment's version the Current Version of its
// deployment, first waiting until the server has seen this version polling
// every queue named. Waiting is what lets the call run without AllowNoPollers:
// routing tasks to a version with no pollers is exactly the operator mistake
// the server's default guards against.
func setCurrentVersion(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	deployment task.Deployment,
	queues ...string,
) {
	t.Helper()
	handle := c.WorkerDeploymentClient().GetHandle(deployment.Name)

	deadline := time.Now().Add(60 * time.Second)
	for {
		described, err := handle.DescribeVersion(ctx, client.WorkerDeploymentDescribeVersionOptions{
			BuildID: deployment.BuildID,
		})
		if err == nil && pollsEveryQueue(described, queues) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deployment version %s never reported pollers on %v (last error: %v)",
				deployment, queues, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	if _, err := handle.SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{
		BuildID: deployment.BuildID,
		// The new version legitimately polls fewer queues than every queue the
		// previous current version ever touched (docs/DEPLOY.md says to check
		// describe-version instead), and the wait above already proved pollers
		// on the queues this test cares about.
		IgnoreMissingTaskQueues: true,
	}); err != nil {
		t.Fatalf("set current version to %s: %v", deployment, err)
	}
	t.Logf("current version of deployment %q is now %q", deployment.Name, deployment.BuildID)
}

func pollsEveryQueue(described client.WorkerDeploymentVersionDescription, queues []string) bool {
	seen := map[string]struct{}{}
	for _, info := range described.Info.TaskQueuesInfos {
		seen[info.Name] = struct{}{}
	}
	for _, queue := range queues {
		if _, ok := seen[queue]; !ok {
			return false
		}
	}
	return true
}

// replica is one OrkStack release running as a pair of workers: the engine's
// run worker and an Orker, both carrying the SAME deployment version, plus an
// unversioned projection worker. That pairing is the mechanism under test —
// Temporal routes a pinned run's activities to the workflow's own version, so
// an Orker outside it would never receive them.
type replica struct {
	deployment task.Deployment
	run        worker.Worker
	orker      worker.Worker
	events     worker.Worker

	// The Orker is the one worker a test stops and starts on its own (see
	// startOrker): docs/DEPLOY.md §3 step 6 is an operator stopping the old
	// version's workers, and what that does to a run still pinned to it is a
	// property, not a footnote. Its inputs are kept so it can be rebuilt.
	client     client.Client
	orkerQueue string
	release    <-chan struct{}

	mu sync.Mutex
	// executed records which build actually ran each task, stamped by the
	// stub itself: "<runID>/<taskID>" -> build id.
	executed map[string]string
	// projected records which replica published each event, proving the
	// projection worker is not tied to a version.
	projected map[string]string
	// started closes when a gate task begins, so the test knows the run is
	// genuinely in flight before it rolls the deployment forward.
	started chan struct{}
	once    sync.Once
}

// gateTaskID is the task the v1 Orker holds open while the rollout happens.
const gateTaskID = "gate"

// newReplica wires one release's workers. The projection worker polls
// engine.EventQueue, which the workflow hard-codes: it is engine-internal, so
// the namespace is the isolation here. release is closed by the test to let
// the gate task finish.
func newReplica(
	t *testing.T,
	c client.Client,
	deployment task.Deployment,
	runQueue, orkerQueue string,
	release <-chan struct{},
	snapshots engine.SnapshotStore,
) *replica {
	t.Helper()
	r := &replica{
		deployment: deployment,
		client:     c,
		orkerQueue: orkerQueue,
		release:    release,
		executed:   map[string]string{},
		projected:  map[string]string{},
		started:    make(chan struct{}),
	}

	r.run = worker.New(c, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	// Registered through the SHIPPED list, not a copy of it: this test's whole
	// claim is that a run stays on the build that started it, and that claim
	// belongs to whatever behaviour `bin/ork` declares. Declaring Pinned here
	// would have made the test pass while the binary registered AutoUpgrade —
	// measured: flipping engine.RegisterWorkflows used to leave this green.
	engine.RegisterWorkflows(r.run)
	r.run.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	// Unversioned on purpose: PublishEvent must stay executable by any live
	// replica of any version. See eventWorkerOptions in cmd/ork.
	r.events = worker.New(c, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	inner := engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots)
	r.events.RegisterActivityWithOptions(
		func(actCtx context.Context, event *orkstackv1.Event, snapshotURI string, dedupeKey string) error {
			r.mu.Lock()
			r.projected[fmt.Sprintf("%s/%d", event.GetRunId(), event.GetSequence())] = deployment.BuildID
			r.mu.Unlock()
			return inner.PublishEvent(actCtx, event, snapshotURI, dedupeKey)
		},
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	for name, w := range map[string]worker.Worker{"run": r.run, "event": r.events} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker of %s: %v", name, deployment, err)
		}
	}
	r.startOrker(t)
	return r
}

// startOrker builds and starts this release's Orker. It is separate from
// newReplica because an Orker is the half of a release an operator stops
// first, and a test that wants to know what that does to a pinned run has to
// be able to stop it and bring it back.
func (r *replica) startOrker(t *testing.T) {
	t.Helper()
	r.orker = worker.New(r.client, r.orkerQueue, r.deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}))
	r.orker.RegisterActivityWithOptions(
		func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			r.mu.Lock()
			r.executed[in.RunId+"/"+in.TaskId] = r.deployment.BuildID
			r.mu.Unlock()

			// The gate stands in for a long task: it is running when the
			// rollout happens, so the run is unambiguously in flight.
			if in.TaskId == gateTaskID {
				r.once.Do(func() { close(r.started) })
				select {
				case <-r.release:
				case <-actCtx.Done():
					return &orkstackv1.ExecuteTaskResult{}, actCtx.Err()
				}
			}
			return &orkstackv1.ExecuteTaskResult{
				TaskId: in.TaskId,
				ValuesJson: valuesJSON(map[string]any{
					// The stamp is the plugin-code-level evidence: it says
					// which build's task implementation produced this result,
					// and it is recorded in the run's own history.
					"executed_by_build": r.deployment.BuildID,
					"echoed_task":       in.TaskId,
				}),
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	if err := r.orker.Start(); err != nil {
		t.Fatalf("start orker worker of %s: %v", r.deployment, err)
	}
}

// stopOrker stops this release's Orker and leaves the rest of it running,
// which is the state docs/DEPLOY.md §3 step 6 puts a version into.
func (r *replica) stopOrker() {
	r.orker.Stop()
}

// stop stops every worker. Worker.Stop is idempotent (SDK v1.47.0
// internal/internal_worker.go:1614 returns early once stopC is closed), so a
// test that already stopped the Orker may still defer this.
func (r *replica) stop() {
	r.run.Stop()
	r.orker.Stop()
	r.events.Stop()
}

func (r *replica) ranTasks() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]string{}
	for k, v := range r.executed {
		out[k] = v
	}
	return out
}

func (r *replica) projectedEvents() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]string{}
	for k, v := range r.projected {
		out[k] = v
	}
	return out
}

// TestIntegration_PinnedRunFinishesOnItsOwnVersionWhileNewRunsMoveOn is the
// milestone's central proof, against a real cluster.
//
// A run starts while build v1 is current. Mid-flight — with one of its tasks
// still executing — a v2 release comes up and becomes the Current Version.
// Three things must then be true, and none of them may be assumed:
//
//  1. the in-flight run stays PINNED to v1 and completes there;
//  2. every task of that run executes on v1's Orker, INCLUDING the tasks
//     scheduled after v2 became current — that is the "an Orker rollout cannot
//     move an in-flight run's tasks onto new plugin code" property, read back
//     from the run's own history rather than from this test's bookkeeping;
//  3. a run submitted after the switch executes wholly on v2.
//
// It also pins the deliberate exception: the projection worker is unversioned,
// so the v1 run's remaining events are published by a replica the v1 release
// does not contain — which is what keeps every replica able to read every run.
func TestIntegration_PinnedRunFinishesOnItsOwnVersionWhileNewRunsMoveOn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)

	// Private names: the deployment is namespace-wide state, and the queues
	// must not be polled by anything but this test's two replicas.
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	deploymentName := "ork-rollout-" + suffix
	runQueue := "ork-rollout-run-" + suffix
	orkerQueue := "ork-rollout-tasks-" + suffix
	// engine.EventQueue is engine-internal and hard-coded in the workflow, so
	// the namespace — not a private name — is what isolates the projections.
	v1 := task.Deployment{Name: deploymentName, BuildID: "v1"}
	v2 := task.Deployment{Name: deploymentName, BuildID: "v2"}

	release := make(chan struct{})
	releaseOnce := sync.Once{}
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	snapshots := newTestSnapshotStore()

	transcript := newRolloutTranscript(t, namespace, deploymentName, runQueue, orkerQueue)

	// --- v1 is the whole world -------------------------------------------
	first := newReplica(t, c, v1, runQueue, orkerQueue, release, snapshots)
	defer first.stop()
	setCurrentVersion(t, ctx, c, v1, runQueue, orkerQueue)
	transcript.step("v1 workers up; current version = v1")

	starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, orkerQueue)
	pinnedRun := "ork-rollout-pinned-" + suffix
	if err := starter.StartRun(ctx, pinnedRun, gatedOrkflow(), nil, rawPlan(orkerQueue)); err != nil {
		t.Fatalf("start the run that must stay on v1: %v", err)
	}
	transcript.step("submitted run " + pinnedRun + " while v1 was current")

	select {
	case <-first.started:
	case <-ctx.Done():
		t.Fatalf("run %s never reached its gate task: %v", pinnedRun, ctx.Err())
	}
	transcript.step("run " + pinnedRun + " is in flight: its gate task is executing on v1")

	// --- v2 arrives and takes over ---------------------------------------
	second := newReplica(t, c, v2, runQueue, orkerQueue, release, snapshots)
	defer second.stop()
	setCurrentVersion(t, ctx, c, v2, runQueue, orkerQueue)
	transcript.step("v2 workers up; current version = v2, while " + pinnedRun + " is still executing")

	// Stop v1's projection worker. Whatever the pinned run emits from here on
	// can only be published by v2's — which is the point: the read model must
	// not be tied to the version that started the run.
	first.events.Stop()
	transcript.step("stopped v1's projection worker; only v2's remains")

	newRun := "ork-rollout-current-" + suffix
	if err := starter.StartRun(ctx, newRun, plainOrkflow(), nil, rawPlan(orkerQueue)); err != nil {
		t.Fatalf("start the run that must use v2: %v", err)
	}
	transcript.step("submitted run " + newRun + " after the switch")

	releaseGate()
	transcript.step("released the gate task")

	if err := c.GetWorkflow(ctx, pinnedRun, "").Get(ctx, nil); err != nil {
		t.Fatalf("the pinned run %s did not complete on v1: %v", pinnedRun, err)
	}
	if err := c.GetWorkflow(ctx, newRun, "").Get(ctx, nil); err != nil {
		t.Fatalf("the run submitted after the switch (%s) did not complete: %v", newRun, err)
	}
	transcript.step("both runs completed")

	// --- 1. the in-flight run is pinned to v1 -----------------------------
	assertRunVersion(t, ctx, c, transcript, pinnedRun, v1)
	assertRunVersion(t, ctx, c, transcript, newRun, v2)

	// --- 2 and 3. every task ran on its run's own version ------------------
	pinnedTasks := assertTaskBuilds(t, ctx, c, transcript, pinnedRun, "v1")
	newTasks := assertTaskBuilds(t, ctx, c, transcript, newRun, "v2")

	// The gate is the reason this is a rollout test and not a routing test:
	// tasks downstream of it were scheduled AFTER v2 became current, and they
	// still had to land on v1's plugin code.
	if _, ok := pinnedTasks["after-gate"]; !ok {
		t.Fatal("the pinned run never ran a task downstream of the gate; " +
			"nothing was scheduled after the version switch, so the rollout was not exercised")
	}
	if len(newTasks) == 0 {
		t.Fatal("the run submitted after the switch executed no tasks")
	}

	// This test's own bookkeeping, from inside the activity, must agree with
	// what history says. If the two ever disagree, the history reading is
	// wrong and every assertion above is worthless.
	assertReplicaBookkeepingAgrees(t, first, second, pinnedRun, newRun, transcript)

	// --- the unversioned projection worker --------------------------------
	projectedByV2 := 0
	for key, build := range second.projectedEvents() {
		if strings.HasPrefix(key, pinnedRun+"/") && build == "v2" {
			projectedByV2++
		}
	}
	if projectedByV2 == 0 {
		t.Error("no event of the v1-pinned run was published by v2's projection worker, " +
			"even though v1's was stopped: the projection worker has become version-bound, " +
			"which would make a run unreadable from a replica of another version")
	}
	transcript.result(fmt.Sprintf("events of %s published by v2's unversioned projection worker: %d",
		pinnedRun, projectedByV2))

	transcript.write(t)
}

// assertRunVersion reads the server's own routing verdict for a run.
func assertRunVersion(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	transcript *rolloutTranscript,
	runID string,
	want task.Deployment,
) {
	t.Helper()
	described, err := c.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatalf("describe %s: %v", runID, err)
	}
	info := described.GetWorkflowExecutionInfo().GetVersioningInfo()
	if info == nil {
		t.Fatalf("run %s carries no versioning info: it was executed by an unversioned worker", runID)
	}
	if info.GetBehavior() != enums.VERSIONING_BEHAVIOR_PINNED {
		t.Errorf("run %s reports behaviour %v, want PINNED — RunWorkflow's registration lost its behaviour",
			runID, info.GetBehavior())
	}
	version := info.GetDeploymentVersion()
	if version.GetBuildId() != want.BuildID || version.GetDeploymentName() != want.Name {
		t.Errorf("run %s is on deployment version %s.%s, want %s",
			runID, version.GetDeploymentName(), version.GetBuildId(), want)
	}
	transcript.result(fmt.Sprintf("run %s: behavior=%v version=%s.%s",
		runID, info.GetBehavior(), version.GetDeploymentName(), version.GetBuildId()))
}

// assertTaskBuilds reads the run's history and proves, per task, which build
// executed it. Three independent readings must agree:
//
//   - ActivityTaskStarted.WorkerVersion.BuildId — the server's record of which
//     worker it dispatched the task to;
//   - ActivityTaskStarted.WorkerVersion.UseVersioning — that the worker it
//     chose was a versioned one at all, not an unversioned poller that happened
//     to be on the queue;
//   - the build stamp the task implementation itself wrote into its result,
//     which is the actual "did new plugin code run" question.
//
// It also checks the deliberate exception in the same pass: the run's
// PublishEvent activities must be executed by UNVERSIONED workers, which is
// what keeps the read model readable from a replica of any version.
//
// It returns taskID -> build id.
func assertTaskBuilds(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	transcript *rolloutTranscript,
	runID, wantBuild string,
) map[string]string {
	t.Helper()

	events := runHistory(t, ctx, c, runID)
	activityByScheduledID := map[int64]string{} // scheduled event id -> activity type
	taskByScheduledID := map[int64]string{}     // scheduled event id -> ork task id
	dispatchedTo := map[string]string{}         // ork task id -> build from ActivityTaskStarted
	versionedDispatch := map[string]bool{}      // ork task id -> the worker used versioning
	stamped := map[string]string{}              // ork task id -> build the task implementation reported
	versionedProjections := 0
	projections := 0

	for _, event := range events {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			activityByScheduledID[event.GetEventId()] = attrs.GetActivityType().GetName()
			if attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
				taskByScheduledID[event.GetEventId()] = executeTaskIDFrom(t, attrs.GetInput())
			}
			continue
		}
		if attrs := event.GetActivityTaskStartedEventAttributes(); attrs != nil {
			// The deprecated stamp is the only per-activity worker identity
			// Temporal records in history; ActivityTaskStarted has no
			// replacement field (api@v1.63.4/history/v1/message.pb.go).
			//lint:ignore SA1019 no replacement field exists
			stamp := attrs.GetWorkerVersion()
			switch activityByScheduledID[attrs.GetScheduledEventId()] {
			case task.ActivityExecuteTask:
				taskID := taskByScheduledID[attrs.GetScheduledEventId()]
				dispatchedTo[taskID] = stamp.GetBuildId()
				versionedDispatch[taskID] = stamp.GetUseVersioning()
			case engine.ActivityPublishEvent:
				projections++
				if stamp.GetUseVersioning() {
					versionedProjections++
				}
			}
			continue
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
			if activityByScheduledID[attrs.GetScheduledEventId()] != task.ActivityExecuteTask {
				continue
			}
			taskID, build := executeTaskStampFrom(t, attrs.GetResult())
			if taskID != "" {
				stamped[taskID] = build
			}
		}
	}

	if len(stamped) == 0 {
		t.Fatalf("run %s completed no ExecuteTask activity; there is nothing to prove", runID)
	}
	for _, taskID := range sortedKeys(stamped) {
		if stamped[taskID] != wantBuild {
			t.Errorf("run %s task %q was executed by build %q, want %q: an in-flight run's task "+
				"reached the wrong plugin code", runID, taskID, stamped[taskID], wantBuild)
		}
		if got := dispatchedTo[taskID]; got != wantBuild {
			t.Errorf("run %s task %q was dispatched to worker build %q per ActivityTaskStarted, want %q",
				runID, taskID, got, wantBuild)
		}
		if !versionedDispatch[taskID] {
			t.Errorf("run %s task %q was dispatched to a worker that is not using versioning; "+
				"the routing that keeps it on its own build is not in effect", runID, taskID)
		}
		transcript.result(fmt.Sprintf("run %s task %-11s dispatched_to=%s (versioned) executed_by=%s",
			runID, taskID, dispatchedTo[taskID], stamped[taskID]))
	}

	if projections == 0 {
		t.Errorf("run %s published no events; the projection half of the rollout is untested", runID)
	}
	if versionedProjections != 0 {
		t.Errorf("run %s had %d of %d PublishEvent activities executed by a VERSIONED worker; "+
			"the projection worker must stay unversioned so any replica can serve any run",
			runID, versionedProjections, projections)
	}
	transcript.result(fmt.Sprintf("run %s: %d PublishEvent activities, all executed by unversioned workers",
		runID, projections))
	return stamped
}

// assertReplicaBookkeepingAgrees cross-checks history against what the two
// replicas observed in-process.
func assertReplicaBookkeepingAgrees(
	t *testing.T,
	first, second *replica,
	pinnedRun, newRun string,
	transcript *rolloutTranscript,
) {
	t.Helper()
	for key, build := range first.ranTasks() {
		if strings.HasPrefix(key, newRun+"/") {
			t.Errorf("v1's Orker executed %s, a task of the run submitted after the switch", key)
		}
		if build != "v1" {
			t.Errorf("v1's Orker reported build %q for %s", build, key)
		}
	}
	ranPinnedOnV2 := 0
	for key, build := range second.ranTasks() {
		if strings.HasPrefix(key, pinnedRun+"/") {
			ranPinnedOnV2++
			t.Errorf("v2's Orker executed %s, a task of the run pinned to v1: a rollout moved an "+
				"in-flight run onto new plugin code", key)
		}
		if build != "v2" {
			t.Errorf("v2's Orker reported build %q for %s", build, key)
		}
	}
	transcript.result(fmt.Sprintf("v1 orker executed %d tasks, v2 orker executed %d tasks, "+
		"tasks of the pinned run seen by v2: %d",
		len(first.ranTasks()), len(second.ranTasks()), ranPinnedOnV2))
}

func runHistory(t *testing.T, ctx context.Context, c client.Client, runID string) []*historypb.HistoryEvent {
	t.Helper()
	var events []*historypb.HistoryEvent
	iter := c.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history of %s: %v", runID, err)
		}
		events = append(events, event)
	}
	if len(events) == 0 {
		t.Fatalf("run %s has an empty history", runID)
	}
	return events
}

// executeTaskIDFrom decodes the engine's activity input to name the ork task.
func executeTaskIDFrom(t *testing.T, payloads *commonpb.Payloads) string {
	t.Helper()
	var in *orkstackv1.ExecuteTaskInput
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &in); err != nil {
		t.Fatalf("decode ExecuteTask input from history: %v", err)
	}
	return in.TaskId
}

// executeTaskStampFrom decodes the Orker's result and returns the task id with
// the build its implementation stamped.
func executeTaskStampFrom(t *testing.T, payloads *commonpb.Payloads) (string, string) {
	t.Helper()
	result := &orkstackv1.ExecuteTaskResult{}
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, result); err != nil {
		t.Fatalf("decode ExecuteTask result from history: %v", err)
	}
	// The stamp is inside the workload's values.json bytes, not a structured
	// field: ExecuteTaskResult carries the workload's EXACT bytes so the
	// committed sha256 is the sha256 of what the workload wrote (§3.4).
	build, _ := decodeValuesJSON(t, result.GetValuesJson())["executed_by_build"].(string)
	return result.GetTaskId(), build
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// gatedOrkflow holds its middle task open until the test releases it, so the
// tasks after it are scheduled while the deployment is being rolled forward.
func gatedOrkflow() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:    "rollout-gated",
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "prepare", Uses: "shell", TimeoutSeconds: 120,
				With: mustStruct(map[string]any{"run": "echo prepare"})},
			{Id: gateTaskID, Uses: "shell", Needs: []string{"prepare"}, TimeoutSeconds: 200,
				With: mustStruct(map[string]any{"run": "echo gate"})},
			{Id: "after-gate", Uses: "shell", Needs: []string{gateTaskID}, TimeoutSeconds: 120,
				With: mustStruct(map[string]any{"run": "echo after"})},
			{Id: "publish", Uses: "file", Needs: []string{"after-gate"}, TimeoutSeconds: 120,
				With: mustStruct(map[string]any{"path": "/tmp/rollout.txt"})},
		},
	}
}

// plainOrkflow is the run submitted after the switch: nothing blocks it.
func plainOrkflow() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:    "rollout-plain",
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "fetch", Uses: "http", TimeoutSeconds: 120,
				With: mustStruct(map[string]any{"url": "https://example.invalid/x"})},
			{Id: "report", Uses: "shell", Needs: []string{"fetch"}, TimeoutSeconds: 120,
				With: mustStruct(map[string]any{"run": "echo report"})},
		},
	}
}

func mustStruct(pairs map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(pairs)
	if err != nil {
		panic(err)
	}
	return s
}

// rolloutTranscript records the run of the rollout for proof/M8.4.
type rolloutTranscript struct {
	lines   []string
	started time.Time
}

func newRolloutTranscript(t *testing.T, namespace, deployment, runQueue, orkerQueue string) *rolloutTranscript {
	tr := &rolloutTranscript{started: time.Now()}
	tr.lines = append(tr.lines,
		"OrkStack M8.4 — worker deployment versioning rollout transcript",
		"namespace:  "+namespace,
		"deployment: "+deployment,
		"queues:     run="+runQueue+" orker="+orkerQueue+" events="+engine.EventQueue+" (unversioned)",
		"",
		"--- rollout ---",
	)
	return tr
}

func (tr *rolloutTranscript) step(message string) {
	tr.lines = append(tr.lines, fmt.Sprintf("[%6.2fs] %s", time.Since(tr.started).Seconds(), message))
}

func (tr *rolloutTranscript) result(message string) {
	tr.lines = append(tr.lines, "         "+message)
}

// logOnly prints the transcript into the test log and nowhere else.
//
// proof/M8.4's rollout transcript belongs to the rollout test; a second writer
// would leave whichever test finished last, and the file names one rollout.
func (tr *rolloutTranscript) logOnly(t *testing.T) {
	t.Helper()
	t.Log("\n" + strings.Join(tr.lines, "\n") + "\n")
}

// write saves the transcript when ORK_PROOF_ROLLOUT_FILE names a destination.
func (tr *rolloutTranscript) write(t *testing.T) {
	t.Helper()
	body := strings.Join(tr.lines, "\n") + "\n"
	t.Log("\n" + body)
	path := os.Getenv("ORK_PROOF_ROLLOUT_FILE")
	if path == "" {
		return
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write rollout transcript to %s: %v", path, err)
	}
}
