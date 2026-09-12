//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// Task pinning under Worker Deployment Versioning — design §3 "Versioning" and
// §12.1, the one part of the pinning design that was a PREDICTION rather than a
// fact. This file is the measurement.
//
// What the SDK v1.47.0 source actually says, read rather than paraphrased:
//
//   - `worker.DeploymentOptions.UseVersioning`'s doc comment: "NOTE: Cannot be
//     enabled at the same time as [WorkerOptions.EnableSessionWorker]"
//     (internal/worker.go:80).
//   - The only enforced panic covers the DEPRECATED flag:
//     `if options.EnableSessionWorker && options.UseBuildIDForVersioning`
//     (internal/internal_worker.go:2312-2316, citing
//     https://github.com/temporalio/sdk-go/issues/1227). The combination
//     OrkStack actually produces — `DeploymentOptions.UseVersioning` plus
//     `EnableSessionWorker` — is not covered by it. See
//     TestWorkerOptions_APinHostOnAVersionedDeploymentIsAcceptedByTheSDK in
//     cmd/orker for the construction-level half of that finding.
//   - `newSessionWorker` (internal/internal_worker.go:521-564) deliberately
//     STRIPS versioning from the session ACTIVITY worker
//     ("Disable versioning for activity worker within session, but still send
//     deployment name for debug purpose") and restores it for the session
//     CREATION worker.
//
// The design's prediction was therefore: under a `Pinned` `RunWorkflow`, a
// pin's session CREATION activity participates in version routing normally,
// while the pin's MEMBER activities ride the session's own unversioned
// `<uuid>@<hostname>` queue and reach the one process that polls it — whatever
// the current version of the pool's queue is by then.
//
// This suite measures exactly that, against a real cluster, with the SAME
// worker-options path production takes (task.Deployment.Apply over the
// EnableSessionWorker pair, mirroring cmd/orker's workerOptions), and it
// exercises the case the prediction could only have failed in: a version
// rollout while a pin is OPEN.

const (
	// pinVersionPin is the declared pin every member below belongs to.
	pinVersionPin = "shared-build"
	// pinVersionGate is the pinned member held open across the rollout, so the
	// pin is genuinely open while a newer version becomes Current.
	pinVersionGate = "pin-gate"
	// pinVersionAfter is the pinned member SCHEDULED AFTER the rollout. It is
	// the whole point: §6.5 says a pin never moves, and this is the task that
	// would move if the session's unversioned queue fell through to the current
	// version of anything.
	pinVersionAfter = "pin-after"
	// The unpinned members of the same run, which must keep the ordinary
	// version-pinned routing the existing versioning suite proves.
	pinVersionPrepare = "prepare"
	pinVersionReport  = "report"

	// The SDK's own internal session activities, by the names it registers them
	// under (sdk@v1.47.0/internal/session.go:113-114). They appear in the run's
	// history, which is how this test reads the routing of the pin's creation
	// half without instrumenting the SDK.
	pinVersionCreationActivity   = "internalSessionCreationActivity"
	pinVersionCompletionActivity = "internalSessionCompletionActivity"

	// pinVersionMaxPins is the pin cap both replicas host with, i.e. what
	// ORK_ORKER_MAX_PINS would be set to on a real pin host.
	pinVersionMaxPins = 4
)

// pinVersionExecution is one activity execution as the Orker process that ran
// it saw it: which build it was, which process, which task queue Temporal
// actually delivered it on, and the pin it belonged to.
//
// The delivered queue is the load-bearing field. It is read from
// activity.GetInfo(ctx).TaskQueue inside the activity, so it is the worker's own
// answer to "which queue did this arrive on" — not an inference from the
// engine's intent.
type pinVersionExecution struct {
	build          string
	orkerID        string
	deliveredQueue string
	pin            string
	pinInstance    string
}

// pinVersionReplica is one OrkStack release, as newReplica builds one, with the
// one difference this suite exists for: its Orker is a PIN HOST
// (EnableSessionWorker, MaxConcurrentSessionExecutionSize), wrapped in the same
// task.Deployment.Apply call cmd/orker's workerOptions wraps it in.
//
// It is a separate type from `replica` rather than a flag on it because the
// rollout suite's replica is the fixture of a proof that has nothing to do with
// pins, and a pin host acquires two extra pollers and two extra task queues per
// process — the thing ORK_ORKER_MAX_PINS=0 exists to avoid.
type pinVersionReplica struct {
	deployment task.Deployment
	// orkerID identifies this PROCESS, the way orker.Dispatcher.OrkerID does for
	// a real Orker. "one pin, one Orker" is a statement about a process, so the
	// evidence has to name one.
	orkerID string

	run    worker.Worker
	orker  worker.Worker
	events worker.Worker

	release <-chan struct{}

	mu sync.Mutex
	// executed is "<runID>/<taskID>" -> what this process saw.
	executed map[string]pinVersionExecution
	// released records every ReleasePin this process was asked for, as
	// "<runID>/<pin instance id>". It is the §8.5/§12.2 half: the release rides a
	// DISCONNECTED session context, and if that context lost the session it would
	// land on some other Orker of the pool — or, under versioning, on the current
	// version's Orker. It is keyed by run because a pin host legitimately
	// releases the pins of its OWN runs, and this suite has two runs.
	released []string

	gateStarted chan struct{}
	gateOnce    sync.Once
}

// newPinVersionReplica wires one release: a versioned run worker, a versioned
// PIN-HOSTING Orker, and the deliberately unversioned projection worker.
func newPinVersionReplica(
	t *testing.T,
	c client.Client,
	deployment task.Deployment,
	runQueue, orkerQueue string,
	release <-chan struct{},
	snapshots engine.SnapshotStore,
) *pinVersionReplica {
	t.Helper()
	r := &pinVersionReplica{
		deployment:  deployment,
		orkerID:     fmt.Sprintf("orker-%s-%d", deployment.BuildID, os.Getpid()),
		release:     release,
		executed:    map[string]pinVersionExecution{},
		gateStarted: make(chan struct{}),
	}

	r.run = worker.New(c, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	// Through the SHIPPED registration list, so RunWorkflow carries whatever
	// versioning behaviour `bin/ork` declares (Pinned) rather than a copy of it.
	engine.RegisterWorkflows(r.run)
	r.run.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	// Unversioned on purpose, exactly as cmd/ork's eventWorkerOptions: the read
	// model must stay readable from a replica of any version.
	r.events = worker.New(c, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	r.events.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).
			WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	// THE configuration under test. Every field is the one cmd/orker's
	// workerOptions sets, and the deployment identity is applied through the same
	// task.Deployment.Apply — so a change to either side of that pair reaches
	// this test rather than passing beside it.
	r.orker = worker.New(c, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing:  true,
		DisableWorkflowWorker:        true,
		MaxHeartbeatThrottleInterval: orker.DefaultHeartbeatInterval,

		EnableSessionWorker:               true,
		MaxConcurrentSessionExecutionSize: pinVersionMaxPins,
	}))
	r.orker.RegisterActivityWithOptions(r.executeTask,
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
	r.orker.RegisterActivityWithOptions(r.releasePin,
		activity.RegisterOptions{Name: task.ActivityReleasePin})

	for name, w := range map[string]worker.Worker{"run": r.run, "orker": r.orker, "event": r.events} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker of %s: %v", name, deployment, err)
		}
	}
	t.Cleanup(r.stop)
	return r
}

// executeTask is the Orker stub: it records what it saw and, for the gate,
// holds the pin open until the test releases it.
func (r *pinVersionReplica) executeTask(
	actCtx context.Context, in *orkstackv1.ExecuteTaskInput,
) (*orkstackv1.ExecuteTaskResult, error) {
	info := activity.GetInfo(actCtx)
	r.mu.Lock()
	r.executed[in.GetRunId()+"/"+in.GetTaskId()] = pinVersionExecution{
		build:          r.deployment.BuildID,
		orkerID:        r.orkerID,
		deliveredQueue: info.TaskQueue,
		pin:            in.GetPin().GetName(),
		pinInstance:    in.GetPin().GetInstanceId(),
	}
	r.mu.Unlock()

	if in.GetTaskId() == pinVersionGate {
		r.gateOnce.Do(func() { close(r.gateStarted) })
		// Heartbeat while held. The engine schedules every task activity with
		// HeartbeatTimeout = task.HeartbeatTimeout (30 s), so a stub that simply
		// blocked would be declared dead mid-rollout and this test would measure
		// a heartbeat timeout instead of routing.
		beat := time.NewTicker(2 * time.Second)
		defer beat.Stop()
		for {
			select {
			case <-r.release:
				return r.result(in, info), nil
			case <-actCtx.Done():
				return nil, actCtx.Err()
			case <-beat.C:
				activity.RecordHeartbeat(actCtx, "holding the pin open")
			}
		}
	}
	return r.result(in, info), nil
}

// result stamps the answer with the identity of the build and process that
// produced it, so the same claim can be read back out of the run's own history
// rather than only out of this test's memory.
func (r *pinVersionReplica) result(
	in *orkstackv1.ExecuteTaskInput, info activity.Info,
) *orkstackv1.ExecuteTaskResult {
	return &orkstackv1.ExecuteTaskResult{
		TaskId:  in.GetTaskId(),
		OrkerId: r.orkerID,
		ValuesJson: valuesJSON(map[string]any{
			"executed_by_build": r.deployment.BuildID,
			"executed_by_orker": r.orkerID,
			"delivered_on":      info.TaskQueue,
			"pin_instance":      in.GetPin().GetInstanceId(),
		}),
	}
}

func (r *pinVersionReplica) releasePin(_ context.Context, in *orkstackv1.ReleasePinInput) error {
	r.mu.Lock()
	r.released = append(r.released, in.GetRunId()+"/"+in.GetPin().GetInstanceId())
	r.mu.Unlock()
	return nil
}

func (r *pinVersionReplica) stop() {
	r.run.Stop()
	r.orker.Stop()
	r.events.Stop()
}

func (r *pinVersionReplica) ran() map[string]pinVersionExecution {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]pinVersionExecution{}
	for k, v := range r.executed {
		out[k] = v
	}
	return out
}

func (r *pinVersionReplica) releases() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.released...)
}

// TestIntegration_AnOpenPinStaysOnItsHostAcrossAVersionRollout settles design
// §12.1 against a real cluster.
//
// The shape is versioning_rollout's, with a pin: v1 is the whole world, a run
// with a pin starts and opens it, one pinned member holds the pin open, v2 —
// itself a pin host — comes up and becomes the Current Version, and only then
// is the gate released, so the pin's SECOND member is scheduled while a newer
// version is current.
//
// Four things must be true, and the design could only assume them:
//
//  1. the pin opens at all on a versioned deployment: the SDK does not reject
//     EnableSessionWorker together with real Worker Deployment Versioning, and
//     the pin's session CREATION activity routes to a same-version pin host,
//     participating in version routing normally;
//  2. every member of the pin — INCLUDING the one scheduled after v2 became
//     Current — executes on the ONE process that opened the pin, on the
//     session's own `<uuid>@<hostname>` queue;
//  3. the run's unpinned tasks keep the ordinary version-pinned routing;
//  4. a pinned run submitted AFTER the switch opens its pin on v2 — so v2 was
//     a live, pin-capable host all along, and (2) is a routing property rather
//     than v2 being unable to take work.
func TestIntegration_AnOpenPinStaysOnItsHostAcrossAVersionRollout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	deploymentName := "ork-pinver-" + suffix
	runQueue := "ork-pinver-run-" + suffix
	orkerQueue := "ork-pinver-tasks-" + suffix
	// The queue CreateSession derives from the pool's queue
	// (sdk@v1.47.0/internal/session.go:390). A versioned pin host polls it as a
	// versioned worker; naming it here lets the test assert that.
	creationQueue := orkerQueue + "__internal_session_creation"
	v1 := task.Deployment{Name: deploymentName, BuildID: "v1"}
	v2 := task.Deployment{Name: deploymentName, BuildID: "v2"}

	release := make(chan struct{})
	releaseOnce := sync.Once{}
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	snapshots := newTestSnapshotStore()

	transcript := newRolloutTranscript(t, namespace, deploymentName, runQueue, orkerQueue)
	transcript.result("pin hosts: EnableSessionWorker=true MaxConcurrentSessionExecutionSize=" +
		fmt.Sprint(pinVersionMaxPins) + " (through task.Deployment.Apply, as cmd/orker does)")

	// --- v1 is the whole world, and it is a pin host ----------------------
	first := newPinVersionReplica(t, c, v1, runQueue, orkerQueue, release, snapshots)
	// The creation queue is included deliberately: setCurrentVersion waits until
	// the server reports this version polling every queue named, so this is also
	// the assertion that a VERSIONED worker really does poll the session creation
	// queue — the first half of the prediction, and the one the SDK's doc comment
	// says should not exist.
	setCurrentVersion(t, ctx, c, v1, runQueue, orkerQueue, creationQueue)
	transcript.step("v1 pin host up; current version = v1; v1 polls " + creationQueue + " as a versioned worker")

	starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, orkerQueue)
	pinnedRun := "ork-pinver-open-" + suffix
	if err := starter.StartRun(ctx, pinnedRun, pinVersionOrkflow(true), nil, rawPlan(orkerQueue)); err != nil {
		t.Fatalf("start the run whose pin must stay on v1: %v", err)
	}
	transcript.step("submitted run " + pinnedRun + " (one pin, two members) while v1 was current")

	select {
	case <-first.gateStarted:
	case <-ctx.Done():
		t.Fatalf("run %s never opened its pin on v1: %v\n\n"+
			"nothing was measured: either the session creation activity never reached a "+
			"same-version pin host, or the pin's first member never reached the session queue",
			pinnedRun, ctx.Err())
	}
	transcript.step("run " + pinnedRun + " has an OPEN pin: its member " + pinVersionGate + " is executing on v1")

	// --- v2 arrives, is a pin host too, and takes over --------------------
	second := newPinVersionReplica(t, c, v2, runQueue, orkerQueue, release, snapshots)
	setCurrentVersion(t, ctx, c, v2, runQueue, orkerQueue, creationQueue)
	switchedAt := time.Now()
	transcript.step("v2 pin host up; current version = v2, while " + pinnedRun + "'s pin is still open")

	// A pinned run submitted after the switch. It proves v2 is a live pin host
	// rather than an idle process, which is what makes "v2 ran none of the first
	// run's members" a routing result.
	currentRun := "ork-pinver-current-" + suffix
	if err := starter.StartRun(ctx, currentRun, pinVersionOrkflow(false), nil, rawPlan(orkerQueue)); err != nil {
		t.Fatalf("start the pinned run that must open its pin on v2: %v", err)
	}
	if err := c.GetWorkflow(ctx, currentRun, "").Get(ctx, nil); err != nil {
		t.Fatalf("the pinned run submitted after the switch (%s) did not complete: %v\n\n"+
			"a pin cannot be opened on the CURRENT version of a versioned deployment", currentRun, err)
	}
	transcript.step("submitted run " + currentRun + " after the switch; it completed")

	releaseGate()
	transcript.step("released the gate member, so " + pinVersionAfter + " is scheduled with v2 current")

	var pinnedResult *orkstackv1.Run
	if err := c.GetWorkflow(ctx, pinnedRun, "").Get(ctx, &pinnedResult); err != nil {
		t.Fatalf("the run with the open pin (%s) did not complete on v1: %v", pinnedRun, err)
	}
	transcript.step("run " + pinnedRun + " completed")

	// --- 1 and 3. the server's own routing verdict ------------------------
	assertRunVersion(t, ctx, c, transcript, pinnedRun, v1)
	assertRunVersion(t, ctx, c, transcript, currentRun, v2)

	// --- the run's own history, per activity ------------------------------
	firstHistory := readPinVersionRouting(t, ctx, c, pinnedRun)
	secondHistory := readPinVersionRouting(t, ctx, c, currentRun)

	assertPinCreationIsVersioned(t, transcript, pinnedRun, firstHistory, creationQueue, "v1")
	assertPinCreationIsVersioned(t, transcript, currentRun, secondHistory, creationQueue, "v2")

	// --- 2. the pin did not move ------------------------------------------
	assertPinMembersShareOneQueue(t, transcript, pinnedRun, firstHistory, orkerQueue)
	assertPinMembersRanOnOneOrker(t, transcript, pinnedRun, first, second, v1, v2)
	assertScheduledAfter(t, transcript, pinnedRun, firstHistory, pinVersionAfter, switchedAt)

	// --- 3. the unpinned tasks of the same run ----------------------------
	assertUnpinnedTasksStayVersioned(t, transcript, pinnedRun, firstHistory, orkerQueue, "v1")

	// --- 4. and the pin submitted after the switch opened on v2 -----------
	assertPinMembersShareOneQueue(t, transcript, currentRun, secondHistory, orkerQueue)
	for key := range first.ran() {
		if strings.HasPrefix(key, currentRun+"/") {
			t.Errorf("v1's Orker executed %s, a task of the pinned run submitted after the switch", key)
		}
	}

	// The engine's own record, which is what a client reads.
	assertTaskRunsAgreeOnOnePin(t, transcript, pinnedResult, first.orkerID)

	// §8.5/§12.2 in passing: the release rides a DISCONNECTED session context,
	// and it has to land on the host that owns the directory — not on the
	// current version's Orker.
	assertReleaseLandedOnTheHost(t, transcript, pinnedRun, first, second, firstHistory)
	assertPinCompletionReachedTheHost(t, transcript, pinnedRun, firstHistory)

	transcript.writePin(t)
}

// assertPinCompletionReachedTheHost checks the SDK's own session completion
// activity — the call that frees the host's session slot. CompleteSession
// overrides its task queue with the session's own
// (sdk@v1.47.0/internal/session.go:232-241), so it is one more activity that
// has to reach the pin host over an unversioned per-host queue.
func assertPinCompletionReachedTheHost(
	t *testing.T,
	transcript *rolloutTranscript,
	runID string,
	history map[int64]*pinVersionActivity,
) {
	t.Helper()
	found := 0
	for _, record := range history {
		if record.activityType != pinVersionCompletionActivity {
			continue
		}
		found++
		if !record.started {
			t.Errorf("run %s scheduled %s on %q and no worker started it: the pin host's session "+
				"slot is freed only by the creation activity's heartbeat timing out",
				runID, pinVersionCompletionActivity, record.queue)
		}
		if !strings.Contains(record.queue, "@") {
			t.Errorf("run %s ran %s on %q, want the session's own `<uuid>@<hostname>` queue",
				runID, pinVersionCompletionActivity, record.queue)
		}
		transcript.result(fmt.Sprintf("run %s: %s on %s dispatched_to=%s versioned=%v",
			runID, pinVersionCompletionActivity, record.queue, record.build, record.versioned))
	}
	if found == 0 {
		t.Errorf("run %s never ran %s; the pin's session was not completed by the engine",
			runID, pinVersionCompletionActivity)
	}
}

// pinVersionActivity is one activity execution as HISTORY records it.
type pinVersionActivity struct {
	activityType string
	// queue is ActivityTaskScheduled's task queue: what the server was told to
	// route on. For a member of a pin this is the session's own queue, because
	// the SDK overrides ActivityOptions.TaskQueue on a session context.
	queue       string
	taskID      string
	pin         string
	pinInstance string
	scheduledAt time.Time

	started bool
	// build and versioned come from ActivityTaskStarted's worker stamp: which
	// worker the server actually dispatched to, and whether that worker was
	// using versioning at all.
	build     string
	versioned bool
	// stampedBuild and stampedOrker are what the implementation itself wrote
	// into its result, read back out of ActivityTaskCompleted.
	stampedBuild string
	stampedOrker string
}

// readPinVersionRouting reads a run's history into one record per activity
// execution. Everything this suite asserts about routing is read from here, so
// no claim rests only on the test's in-process bookkeeping.
func readPinVersionRouting(
	t *testing.T, ctx context.Context, c client.Client, runID string,
) map[int64]*pinVersionActivity {
	t.Helper()
	byScheduledID := map[int64]*pinVersionActivity{}
	for _, event := range runHistory(t, ctx, c, runID) {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			record := &pinVersionActivity{
				activityType: attrs.GetActivityType().GetName(),
				queue:        attrs.GetTaskQueue().GetName(),
				scheduledAt:  event.GetEventTime().AsTime(),
			}
			if record.activityType == task.ActivityExecuteTask {
				record.taskID, record.pin, record.pinInstance = pinVersionInputFrom(t, attrs.GetInput())
			}
			byScheduledID[event.GetEventId()] = record
			continue
		}
		if attrs := event.GetActivityTaskStartedEventAttributes(); attrs != nil {
			record, ok := byScheduledID[attrs.GetScheduledEventId()]
			if !ok {
				continue
			}
			// The deprecated stamp is the only per-activity worker identity
			// Temporal records in history; ActivityTaskStarted has no
			// replacement field.
			//lint:ignore SA1019 no replacement field exists
			stamp := attrs.GetWorkerVersion()
			record.started = true
			record.build = stamp.GetBuildId()
			record.versioned = stamp.GetUseVersioning()
			continue
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
			record, ok := byScheduledID[attrs.GetScheduledEventId()]
			if !ok || record.activityType != task.ActivityExecuteTask {
				continue
			}
			_, record.stampedBuild = executeTaskStampFrom(t, attrs.GetResult())
			record.stampedOrker = pinVersionOrkerStampFrom(t, attrs.GetResult())
		}
	}
	if len(byScheduledID) == 0 {
		t.Fatalf("run %s scheduled no activity at all", runID)
	}
	return byScheduledID
}

// pinVersionMembers returns the history records of the pin's member tasks, and
// pinVersionUnpinned the records of the tasks with no pin.
func pinVersionMembers(history map[int64]*pinVersionActivity) []*pinVersionActivity {
	var out []*pinVersionActivity
	for _, record := range history {
		if record.activityType == task.ActivityExecuteTask && record.pin != "" {
			out = append(out, record)
		}
	}
	return out
}

func pinVersionUnpinned(history map[int64]*pinVersionActivity) []*pinVersionActivity {
	var out []*pinVersionActivity
	for _, record := range history {
		if record.activityType == task.ActivityExecuteTask && record.pin == "" {
			out = append(out, record)
		}
	}
	return out
}

// assertPinCreationIsVersioned is the first half of the §12.1 prediction: the
// pin's session CREATION activity goes to `<pool queue>__internal_session_creation`
// and is dispatched to a worker of the run's OWN deployment version, using
// versioning — i.e. the creation half participates in version routing exactly
// as any other activity of a Pinned workflow does.
func assertPinCreationIsVersioned(
	t *testing.T,
	transcript *rolloutTranscript,
	runID string,
	history map[int64]*pinVersionActivity,
	creationQueue, wantBuild string,
) {
	t.Helper()
	var creations []*pinVersionActivity
	for _, record := range history {
		if record.activityType == pinVersionCreationActivity {
			creations = append(creations, record)
		}
	}
	if len(creations) != 1 {
		t.Fatalf("run %s ran %d %s activities, want exactly 1: the pin was not opened the way "+
			"the SDK opens a session, so nothing below is about a pin", runID, len(creations), pinVersionCreationActivity)
	}
	creation := creations[0]
	if creation.queue != creationQueue {
		t.Errorf("run %s opened its pin on queue %q, want %q", runID, creation.queue, creationQueue)
	}
	if !creation.started {
		t.Fatalf("run %s scheduled its pin's creation activity on %q and no worker ever started it",
			runID, creation.queue)
	}
	if creation.build != wantBuild {
		t.Errorf("run %s opened its pin on worker build %q, want %q: the pin's creation activity "+
			"did not reach a pin host of the run's own deployment version", runID, creation.build, wantBuild)
	}
	if !creation.versioned {
		t.Errorf("run %s opened its pin on a worker that is NOT using versioning; the SDK is "+
			"expected to KEEP versioning on the session creation worker "+
			"(newSessionWorker, internal_worker.go:551-553)", runID)
	}
	transcript.result(fmt.Sprintf("run %s: pin creation on %s dispatched to build=%s versioned=%v",
		runID, creation.queue, creation.build, creation.versioned))
}

// assertPinMembersShareOneQueue is the second half: every member of the pin was
// scheduled on ONE queue, that queue is the session's own resource-specific
// queue (`<uuid>@<hostname>`) and not the pool's, and — measured, not assumed —
// whether the worker that took them was using versioning.
func assertPinMembersShareOneQueue(
	t *testing.T,
	transcript *rolloutTranscript,
	runID string,
	history map[int64]*pinVersionActivity,
	poolQueue string,
) {
	t.Helper()
	members := pinVersionMembers(history)
	if len(members) == 0 {
		t.Fatalf("run %s executed no pinned task; there is nothing to prove about a pin", runID)
	}
	queues := map[string]struct{}{}
	instances := map[string]struct{}{}
	for _, member := range members {
		queues[member.queue] = struct{}{}
		instances[member.pinInstance] = struct{}{}
		if !member.started {
			t.Errorf("run %s pinned task %q was scheduled on %q and never started",
				runID, member.taskID, member.queue)
		}
		if member.queue == poolQueue {
			t.Errorf("run %s pinned task %q was scheduled on the POOL queue %q rather than the "+
				"pin's session queue: any Orker of the pool could have taken it, without the "+
				"shared directory it was written for", runID, member.taskID, poolQueue)
		}
		if !strings.Contains(member.queue, "@") {
			t.Errorf("run %s pinned task %q was scheduled on %q, which is not a session's "+
				"resource-specific `<uuid>@<hostname>` queue", runID, member.taskID, member.queue)
		}
		transcript.result(fmt.Sprintf("run %s pinned task %-9s queue=%s dispatched_to=%s versioned=%v executed_by=%s/%s",
			runID, member.taskID, member.queue, member.build, member.versioned,
			member.stampedBuild, member.stampedOrker))
	}
	if len(queues) != 1 {
		t.Errorf("run %s scheduled its %d pinned tasks across %d different queues (%v); one pin is "+
			"one session on one host", runID, len(members), len(queues), pinVersionKeys(queues))
	}
	if len(instances) != 1 {
		t.Errorf("run %s pinned tasks carried %d different pin instance ids (%v); one opening of a "+
			"pin is one directory", runID, len(instances), pinVersionKeys(instances))
	}
}

// assertPinMembersRanOnOneOrker is the claim in the form an operator cares
// about: one pin, one Orker PROCESS — and, across a rollout, the OLD one.
func assertPinMembersRanOnOneOrker(
	t *testing.T,
	transcript *rolloutTranscript,
	runID string,
	held, newer *pinVersionReplica,
	heldVersion, newerVersion task.Deployment,
) {
	t.Helper()
	orkers := map[string]struct{}{}
	members := 0
	for key, exec := range held.ran() {
		if !strings.HasPrefix(key, runID+"/") || exec.pin == "" {
			continue
		}
		members++
		orkers[exec.orkerID] = struct{}{}
	}
	for key, exec := range newer.ran() {
		if !strings.HasPrefix(key, runID+"/") {
			continue
		}
		t.Errorf("the %s Orker executed %s (pin %q) of a run pinned to %s: a version rollout moved "+
			"an OPEN pin's work onto another process, which §6.5 forbids and no shared directory "+
			"would follow", newerVersion, key, exec.pin, heldVersion)
	}
	if members < 2 {
		t.Fatalf("only %d pinned member(s) of run %s ran on the %s Orker; this test needs the member "+
			"scheduled AFTER the rollout to have run, or it proves nothing about a rollout",
			members, runID, heldVersion)
	}
	if len(orkers) != 1 {
		t.Errorf("run %s ran its pinned members on %d different Orker processes (%v), want 1",
			runID, len(orkers), pinVersionKeys(orkers))
	}
	transcript.result(fmt.Sprintf("run %s: %d pinned members, all on Orker %v of %s; the %s Orker ran none of them",
		runID, members, pinVersionKeys(orkers), heldVersion, newerVersion))
}

// assertScheduledAfter proves the rollout was actually exercised: the named task
// was scheduled after the moment the newer version became Current. Without this
// the test could pass by having done all its work before the switch.
func assertScheduledAfter(
	t *testing.T,
	transcript *rolloutTranscript,
	runID string,
	history map[int64]*pinVersionActivity,
	taskID string,
	switchedAt time.Time,
) {
	t.Helper()
	for _, record := range history {
		if record.activityType != task.ActivityExecuteTask || record.taskID != taskID {
			continue
		}
		if !record.scheduledAt.After(switchedAt) {
			t.Fatalf("run %s scheduled %q at %s, BEFORE the current version moved at %s: the "+
				"rollout was not exercised and this test proves nothing",
				runID, taskID, record.scheduledAt, switchedAt)
		}
		transcript.result(fmt.Sprintf("run %s scheduled %s %.1fs AFTER the current version moved",
			runID, taskID, record.scheduledAt.Sub(switchedAt).Seconds()))
		return
	}
	t.Fatalf("run %s never scheduled %q", runID, taskID)
}

// assertUnpinnedTasksStayVersioned is the regression half: making a pin work
// must not have cost the unpinned tasks of the same run their version routing.
func assertUnpinnedTasksStayVersioned(
	t *testing.T,
	transcript *rolloutTranscript,
	runID string,
	history map[int64]*pinVersionActivity,
	poolQueue, wantBuild string,
) {
	t.Helper()
	unpinned := pinVersionUnpinned(history)
	if len(unpinned) == 0 {
		t.Fatalf("run %s has no unpinned task; the regression half is untested", runID)
	}
	for _, record := range unpinned {
		if record.queue != poolQueue {
			t.Errorf("run %s unpinned task %q was scheduled on %q, want the pool queue %q",
				runID, record.taskID, record.queue, poolQueue)
		}
		if record.build != wantBuild || !record.versioned {
			t.Errorf("run %s unpinned task %q was dispatched to build %q (versioned=%v), want %q "+
				"with versioning: enabling the session worker cost the ordinary tasks their "+
				"version routing", runID, record.taskID, record.build, record.versioned, wantBuild)
		}
		if record.stampedBuild != wantBuild {
			t.Errorf("run %s unpinned task %q was executed by build %q, want %q",
				runID, record.taskID, record.stampedBuild, wantBuild)
		}
		transcript.result(fmt.Sprintf("run %s unpinned task %-9s queue=%s dispatched_to=%s versioned=%v",
			runID, record.taskID, record.queue, record.build, record.versioned))
	}
}

// assertTaskRunsAgreeOnOnePin reads the engine's own record — what a client
// sees — rather than the transport underneath it.
func assertTaskRunsAgreeOnOnePin(
	t *testing.T, transcript *rolloutTranscript, run *orkstackv1.Run, wantOrker string,
) {
	t.Helper()
	if run == nil {
		t.Fatal("the run returned no result")
	}
	orkers := map[string]struct{}{}
	members := 0
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetPin() == "" {
			continue
		}
		members++
		if taskRun.GetPin() != pinVersionPin {
			t.Errorf("task %q reports pin %q, want %q", taskRun.GetTaskId(), taskRun.GetPin(), pinVersionPin)
		}
		if taskRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Errorf("pinned task %q reports status %v", taskRun.GetTaskId(), taskRun.GetStatus())
		}
		orkers[taskRun.GetOrkerId()] = struct{}{}
	}
	if members < 2 {
		t.Fatalf("the run reports %d pinned TaskRuns, want at least 2", members)
	}
	if len(orkers) != 1 {
		t.Errorf("the run's pinned TaskRuns name %d different orker ids (%v), want exactly 1: this is "+
			"the field a client reads to see that a pin held", len(orkers), pinVersionKeys(orkers))
	}
	if _, ok := orkers[wantOrker]; !ok {
		t.Errorf("the run's pinned TaskRuns name orker(s) %v, want %q — the process that opened the pin",
			pinVersionKeys(orkers), wantOrker)
	}
	transcript.result(fmt.Sprintf("engine record: %d pinned TaskRuns, orker_id=%v, pin=%q",
		members, pinVersionKeys(orkers), pinVersionPin))
}

// assertReleaseLandedOnTheHost checks §8.5's release, which is scheduled on a
// DISCONNECTED session context. If that context lost the session, the release
// would be routed by the pool queue instead — under versioning, to the CURRENT
// version's Orker, which owns no such directory.
func assertReleaseLandedOnTheHost(
	t *testing.T,
	transcript *rolloutTranscript,
	runID string,
	held, newer *pinVersionReplica,
	history map[int64]*pinVersionActivity,
) {
	t.Helper()
	// Only THIS run's release is evidence: a pin host legitimately releases the
	// pins of the runs it hosts, and the newer version hosts one of its own.
	if got := pinVersionReleasesOf(newer, runID); len(got) != 0 {
		t.Errorf("the newer version's Orker was asked to release pin instance(s) %v of run %s, which "+
			"it never hosted: the release lost its session and was routed by version instead",
			got, runID)
	}
	released := pinVersionReleasesOf(held, runID)
	if len(released) == 0 {
		t.Errorf("the pin host was never asked to release run %s's pin; the shared directory now "+
			"waits for the lease sweep, and §12.2's assumption about a disconnected context "+
			"carrying the session does not hold", runID)
		return
	}
	wanted := map[string]struct{}{}
	for _, member := range pinVersionMembers(history) {
		wanted[member.pinInstance] = struct{}{}
	}
	for _, instance := range released {
		if _, ok := wanted[instance]; !ok {
			t.Errorf("the pin host released instance %q, which no member of run %s's pin ran under",
				instance, runID)
		}
	}
	transcript.result(fmt.Sprintf("release of run %s: pin host asked for %v; the newer version's "+
		"Orker asked for none", runID, released))
}

// pinVersionReleasesOf is the pin instances one replica was asked to release
// for one run.
func pinVersionReleasesOf(r *pinVersionReplica, runID string) []string {
	var out []string
	for _, entry := range r.releases() {
		if instance, ok := strings.CutPrefix(entry, runID+"/"); ok {
			out = append(out, instance)
		}
	}
	return out
}

// pinVersionOrkflow is one pin with two members and an unpinned task on either
// side of it. With gate=true its first member holds the pin open until the test
// releases it, so the SECOND member is scheduled during the rollout.
func pinVersionOrkflow(gate bool) *orkstackv1.Orkflow {
	first := map[string]any{"run": "echo member one"}
	if gate {
		first = map[string]any{"run": "echo hold the pin open"}
	}
	gateID := pinVersionGate
	if !gate {
		// A run with no gate must not close the shared gate channel, and the
		// stub keys that behaviour off the task id.
		gateID = "pin-first"
	}
	return &orkstackv1.Orkflow{
		Name:    "versioned-pin",
		Version: "1.0.0",
		Pins:    []*orkstackv1.Pin{{Name: pinVersionPin}},
		Tasks: []*orkstackv1.Task{
			{Id: pinVersionPrepare, Uses: "shell", TimeoutSeconds: 120,
				With: mustStruct(map[string]any{"run": "echo prepare"})},
			{Id: gateID, Uses: "shell", Needs: []string{pinVersionPrepare}, Pin: pinVersionPin,
				TimeoutSeconds: 240, With: mustStruct(first)},
			{Id: pinVersionAfter, Uses: "shell", Needs: []string{gateID}, Pin: pinVersionPin,
				TimeoutSeconds: 120, With: mustStruct(map[string]any{"run": "echo member two"})},
			{Id: pinVersionReport, Uses: "shell", Needs: []string{pinVersionAfter}, TimeoutSeconds: 120,
				With: mustStruct(map[string]any{"run": "echo report"})},
		},
	}
}

// pinVersionInputFrom decodes an ExecuteTask input from history: the task id and
// the pin it belongs to.
func pinVersionInputFrom(t *testing.T, payloads *commonpb.Payloads) (string, string, string) {
	t.Helper()
	var in *orkstackv1.ExecuteTaskInput
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &in); err != nil {
		t.Fatalf("decode ExecuteTask input from history: %v", err)
	}
	return in.GetTaskId(), in.GetPin().GetName(), in.GetPin().GetInstanceId()
}

// pinVersionOrkerStampFrom reads the orker id the implementation wrote into its
// own values.json.
func pinVersionOrkerStampFrom(t *testing.T, payloads *commonpb.Payloads) string {
	t.Helper()
	result := &orkstackv1.ExecuteTaskResult{}
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, result); err != nil {
		t.Fatalf("decode ExecuteTask result from history: %v", err)
	}
	orkerID, _ := decodeValuesJSON(t, result.GetValuesJson())["executed_by_orker"].(string)
	return orkerID
}

func pinVersionKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// pinVersionProof records the transcript under its own name, so it does not
// overwrite the rollout suite's.
func (tr *rolloutTranscript) writePin(t *testing.T) {
	t.Helper()
	proof.Write(t, "engine", "versioning-pin-transcript.txt", []byte(strings.Join(tr.lines, "\n")+"\n"))
}
