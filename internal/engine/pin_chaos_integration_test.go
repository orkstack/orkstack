//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// This file covers the pin rows of the chaos matrix (AGENTS.md §11) against
// real infrastructure: real Temporal, one real Orker hosting real pins with real
// containers, and real object storage.
//
// It deliberately does NOT re-derive the classification logic that
// workflow_test.go and pin_internal_test.go already prove with a
// TestWorkflowEnvironment. What it proves is that the SAME outcomes happen for
// real, in the two situations no mocked session can produce:
//
//   - the pin host dies in the GAP BETWEEN two members, with the first
//     member's work already durably committed (PIN_HOST_LOST, docs/DSL.md
//     "Pins"); and
//   - the run is TERMINATED from outside while a pin is open, so no OrkStack
//     code runs at all and the lease sweep is the only backstop
//     (docs/OCI_ACTIVITY_CONTRACT.md §6.4).
//
// "pin host killed while a member container is actively running" belongs to
// pin_affinity_integration_test.go and is not duplicated here.

// pinChaosNamespace isolates this suite's Temporal namespace. AGENTS.md §11.1:
// this suite terminates workflow executions and stops workers, so no other
// agent's runs may share the namespace it does that in.
func pinChaosNamespace() string {
	if ns := os.Getenv("ORK_TEMPORAL_NAMESPACE"); ns != "" {
		return ns
	}
	return fmt.Sprintf("ork-pin-chaos-%d", os.Getpid())
}

// pinChaosCluster is one deployment: a real engine, a real pin-hosting Orker on
// its own pool, and — when a scenario needs a controllable gap between two
// pinned members — a second pool served by an in-process stub the test can hold
// open and release.
//
// The stub is scaffolding, never the subject. Both members of every pin below
// run on the REAL Orker with real containers against a real /orkstack/shared;
// the stub exists only so that "the host died between members" can be a
// deterministic sequence instead of a race against the engine's own scheduling.
type pinChaosCluster struct {
	t         *testing.T
	ctx       context.Context
	temporal  client.Client
	namespace string // Temporal namespace
	orkNS     string // OrkStack namespace document
	token     string

	pool      string // the pin host's pool
	hostQueue string
	gatePool  string // the stub's pool, for the between-members gap
	gateQueue string

	workRoot   string
	orkerID    string
	activities *orker.Activities
	hostWorker worker.Worker

	orkflows  engine.OrkflowStore
	runClient orkstackv1connect.RunServiceClient

	mu          sync.Mutex
	hostRan     []string
	hostPins    map[string]string // task id -> pin instance id the Orker was handed
	released    []*orkstackv1.ReleasePinInput
	gateArrived chan struct{}
	gateOnce    sync.Once
	gateRelease chan struct{}
	hostStopped bool
}

// newPinChaosCluster brings the deployment up and registers its teardown.
//
// maxPins is the real ORK_ORKER_MAX_PINS: it becomes both
// orker.Options.MaxPins (what DescribeOrker reports, and therefore what
// submit's pin-host check reads) and the SDK's
// MaxConcurrentSessionExecutionSize (what actually bounds concurrent pins on
// this process), exactly as cmd/orker's workerOptions pairs them.
func newPinChaosCluster(t *testing.T, ctx context.Context, maxPins int32) *pinChaosCluster {
	t.Helper()
	requirePinChaosDocker(t)

	cfg := s3Config()
	requireMinIO(t, cfg)

	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	c := &pinChaosCluster{
		t:           t,
		ctx:         ctx,
		namespace:   pinChaosNamespace(),
		token:       token,
		pool:        "pinhost-" + token,
		gatePool:    "pingate-" + token,
		orkerID:     "orker-pin-chaos-" + token,
		hostPins:    map[string]string{},
		gateArrived: make(chan struct{}),
		gateRelease: make(chan struct{}),
	}
	c.hostQueue = mustQueueForPool(t, c.pool)
	c.gateQueue = mustQueueForPool(t, c.gatePool)
	c.temporal = dialVersioningCluster(t, ctx, c.namespace)

	// ── the OrkStack namespace document, in the real registry ──
	namespaceStore, err := artifacts.NewS3NamespaceStore(ctx, s3NamespaceConfig(cfg))
	if err != nil {
		infraAbsent(t, "cannot open the namespace registry: %v", err)
	}
	c.orkNS = "pin-chaos-" + token
	if _, err := namespaceStore.Put(ctx, &orkstackv1.Namespace{
		Name:         c.orkNS,
		Description:  "pin chaos evidence",
		DefaultPool:  task.DefaultPool,
		AllowedPools: []string{task.DefaultPool, c.pool, c.gatePool},
	}, artifacts.PutConditions{}); err != nil {
		infraAbsent(t, "cannot write the namespace document (is MinIO up?): %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := namespaceStore.Delete(cleanupCtx, c.orkNS); err != nil {
			t.Logf("could not remove the test namespace %q: %v", c.orkNS, err)
		}
	})

	// ── the real Orker ──
	//
	// Its own work root, under the daemon-shared .data/work, because this
	// suite asserts on and removes ork-pin-* directories: sharing a work root
	// with another agent's Orker would make one suite's cleanup the other's
	// data loss (AGENTS.md §11.1).
	c.workRoot = pinChaosWorkRoot(t, token)
	store, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("open artifact store: %v", err)
	}
	commitStore, err := artifacts.NewCommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("open commit store: %v", err)
	}
	if err := artifacts.ProbeCommitStore(ctx, commitStore); err != nil {
		infraAbsent(t, "the object store's conditional create is not usable: %v", err)
	}
	logStore, err := artifacts.NewLogStore(cfg)
	if err != nil {
		t.Fatalf("open log store: %v", err)
	}
	refs, ok := store.(artifacts.RefPredictor)
	if !ok {
		t.Fatalf("artifact backend %T cannot predict content-addressed references", store)
	}
	c.activities = orker.NewActivitiesWithOptions(store, orker.Options{
		Logs:     logStore,
		Commits:  commitStore,
		Refs:     refs,
		Pool:     c.pool,
		OrkerID:  c.orkerID,
		WorkRoot: c.workRoot,
		MaxPins:  maxPins,
	})
	if err := c.activities.Boot(ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}
	t.Cleanup(c.activities.Close)

	// ── the workers ──
	bus, runs := engine.NewEventBus(), engine.NewRunStore()
	snapshots := newTestSnapshotStore()

	runWorker := worker.New(c.temporal, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	eventWorker := worker.New(c.temporal, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	// The pin host. EnableSessionWorker is what makes this process a pin host,
	// and MaxConcurrentSessionExecutionSize is the cap cmd/orker derives from
	// the same setting DescribeOrker reports.
	c.hostWorker = worker.New(c.temporal, c.hostQueue, worker.Options{
		DisableRegistrationAliasing:       true,
		DisableWorkflowWorker:             true,
		EnableSessionWorker:               maxPins > 0,
		MaxConcurrentSessionExecutionSize: int(maxPins),
	})
	c.hostWorker.RegisterActivityWithOptions(
		func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			c.mu.Lock()
			c.hostRan = append(c.hostRan, in.GetTaskId())
			c.hostPins[in.GetTaskId()] = in.GetPin().GetInstanceId()
			c.mu.Unlock()
			return c.activities.ExecuteTask(actx, in)
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	c.hostWorker.RegisterActivityWithOptions(
		c.activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker},
	)
	c.hostWorker.RegisterActivityWithOptions(
		func(actx context.Context, in *orkstackv1.ReleasePinInput) error {
			c.mu.Lock()
			c.released = append(c.released, in)
			c.mu.Unlock()
			return c.activities.ReleasePin(actx, in)
		},
		activity.RegisterOptions{Name: task.ActivityReleasePin},
	)

	// The gate: an unpinned pool whose one activity blocks until the test
	// releases it, which is how a real gap between two pinned members is made.
	gateWorker := worker.New(c.temporal, c.gateQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	gateWorker.RegisterActivityWithOptions(
		func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			c.gateOnce.Do(func() { close(c.gateArrived) })
			select {
			case <-c.gateRelease:
			case <-actx.Done():
				return nil, actx.Err()
			}
			return &orkstackv1.ExecuteTaskResult{
				TaskId:     in.GetTaskId(),
				ValuesJson: valuesJSON(map[string]any{"gate": "released"}),
				OrkerId:    "gate-" + c.token,
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	gateWorker.RegisterActivityWithOptions(
		func(context.Context) (*orkstackv1.DescribeOrkerResult, error) {
			return &orkstackv1.DescribeOrkerResult{
				OrkerId: "gate-" + c.token, Pool: c.gatePool, RuntimeName: "stub",
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityDescribeOrker},
	)

	for name, w := range map[string]worker.Worker{
		"run": runWorker, "event": eventWorker, "gate": gateWorker,
	} {
		if err := w.Start(); err != nil {
			t.Fatalf("start the %s worker: %v", name, err)
		}
		t.Cleanup(w.Stop)
	}
	if err := c.hostWorker.Start(); err != nil {
		t.Fatalf("start the pin host worker: %v", err)
	}
	t.Cleanup(c.stopHost)
	// Whatever the run did, this suite's pin directories do not outlive it. The
	// work root is this cluster's own, so the reap can be unconditional.
	t.Cleanup(func() {
		if _, err := orker.ReapPinDirectories(c.workRoot); err != nil {
			t.Logf("cleanup: reap pin directories under %s: %v", c.workRoot, err)
		}
	})

	// ── the production API, wired the way cmd/ork wires it ──
	c.orkflows = engine.NewMemoryOrkflowStore()
	starter := engine.NewTemporalStarter(c.temporal, task.DefaultQueue)
	svc := api.NewService(api.Config{
		Orkflows:   c.orkflows,
		Namespaces: namespaceStore,
		Reader:     engine.NewTemporalRunReader(c.temporal).WithSnapshotStore(snapshots),
		Runs:       runs,
		Bus:        bus,
		Starter:    starter,
		// The real probes: the liveness check and — for a pinned task — the
		// pin-host describe that reads max_concurrent_pins off the Orker itself.
		// A pinned submit that reached the workflow without passing these would
		// be proving something weaker than the deployment does.
		Pools:      engine.NewTemporalPoolProbe(c.temporal),
		PoolOrkers: starter,
		Artifacts:  cfg,
		Limits:     engine.DefaultLimits(),
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(svc, api.MuxOptions{}))
	go func() { _ = server.Serve(listener) }()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	})
	c.runClient = orkstackv1connect.NewRunServiceClient(
		&http.Client{Transport: transport}, "http://"+listener.Addr().String())

	// The submit-time probes ask the cluster, so wait until the cluster can
	// answer about the deployment this test actually started — and about no
	// other. The pools carry a per-execution token, so "exactly one" is a
	// number this deployment justifies rather than a count of ghosts.
	probe := engine.NewTemporalPoolProbe(c.temporal)
	awaitPoolServedBy(t, ctx, probe, c.pool, c.hostQueue, 1)
	awaitPoolServedBy(t, ctx, probe, c.gatePool, c.gateQueue, 1)

	return c
}

// stopHost stops the pin host's worker: it stops polling, stops heartbeating
// its pins' creation activities, and lets the SDK's own shutdown path report
// "session failed due to worker shutdown" — which is what a lost pin host looks
// like to Temporal. Idempotent, because it is both a scenario step and a
// cleanup.
func (c *pinChaosCluster) stopHost() {
	c.mu.Lock()
	if c.hostStopped {
		c.mu.Unlock()
		return
	}
	c.hostStopped = true
	c.mu.Unlock()
	c.hostWorker.Stop()
}

func (c *pinChaosCluster) openGate() { close(c.gateRelease) }

func (c *pinChaosCluster) ranOnHost() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.hostRan...)
	sort.Strings(out)
	return out
}

func (c *pinChaosCluster) pinInstanceFor(taskID string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hostPins[taskID]
}

func (c *pinChaosCluster) releases() []*orkstackv1.ReleasePinInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*orkstackv1.ReleasePinInput(nil), c.released...)
}

// seed stores a compiled definition directly, the way
// durable_commit_e2e_integration_test.go does: apply-time resolution of `uses:`
// into an image is a different seam, and what this suite needs is the compiled
// shape — Orkflow.pins plus Task.pin — that the DSL produces.
func (c *pinChaosCluster) seed(name string, pins []*orkstackv1.Pin, tasks []*orkstackv1.Task) string {
	c.t.Helper()
	version := "0.1." + c.token
	if _, _, err := c.orkflows.Put(c.ctx, &orkstackv1.Orkflow{
		Namespace: c.orkNS,
		Name:      name,
		Version:   version,
		Pins:      pins,
		Tasks:     tasks,
	}, artifacts.NoHeadExpectation()); err != nil {
		c.t.Fatalf("seed the definition %q: %v", name, err)
	}
	return version
}

func (c *pinChaosCluster) submit(name, version string) string {
	c.t.Helper()
	submitted, err := c.runClient.SubmitRun(c.ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName:    name,
		OrkflowVersion: version,
		Namespace:      c.orkNS,
	}))
	if err != nil {
		c.t.Fatalf("SubmitRun(%s): %v", name, err)
	}
	return submitted.Msg.GetRun().GetId()
}

func (c *pinChaosCluster) run(runID string) *orkstackv1.Run {
	c.t.Helper()
	got, err := c.runClient.GetRun(c.ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
		RunId: runID, Namespace: c.orkNS,
	}))
	if err != nil {
		c.t.Fatalf("GetRun(%s): %v", runID, err)
	}
	return got.Msg.GetRun()
}

// awaitTaskStatus polls the read model until one task reaches one of want, and
// returns the run as it was when that happened. Poll with a deadline, never a
// guessed sleep (AGENTS.md §11.1).
func (c *pinChaosCluster) awaitTaskStatus(
	runID, taskID string,
	within time.Duration,
	want ...orkstackv1.Status,
) *orkstackv1.Run {
	c.t.Helper()
	wanted := map[orkstackv1.Status]bool{}
	for _, status := range want {
		wanted[status] = true
	}
	deadline := time.Now().Add(within)
	var last orkstackv1.Status
	for time.Now().Before(deadline) {
		run := c.run(runID)
		for _, taskRun := range run.GetTaskRuns() {
			if taskRun.GetTaskId() != taskID {
				continue
			}
			last = taskRun.GetStatus()
			if wanted[last] {
				return run
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.t.Fatalf("task %q of run %s is %v after %s, want one of %v", taskID, runID, last, within, want)
	return nil
}

// awaitRunTerminal polls until the run itself stops being RUNNING.
func (c *pinChaosCluster) awaitRunTerminal(runID string, within time.Duration) *orkstackv1.Run {
	c.t.Helper()
	deadline := time.Now().Add(within)
	var last *orkstackv1.Run
	for time.Now().Before(deadline) {
		last = c.run(runID)
		switch last.GetStatus() {
		case orkstackv1.Status_STATUS_SUCCEEDED,
			orkstackv1.Status_STATUS_FAILED,
			orkstackv1.Status_STATUS_CANCELED:
			return last
		}
		time.Sleep(250 * time.Millisecond)
	}
	c.t.Fatalf("run %s is still %v after %s", runID, last.GetStatus(), within)
	return nil
}

// awaitSessionLost blocks until the run's OWN history records that the pin's
// session creation activity stopped, which is the moment Temporal — not this
// test — declares the pin host lost.
//
// This is what makes "killed BETWEEN members" a sequence rather than a race:
// the next member is not allowed to be scheduled until the cluster has already
// recorded the loss. The SDK's shutdown path returns a non-retryable
// "session failed due to worker shutdown" from internalSessionCreationActivity,
// so the event is an ActivityTaskFailed; a host that vanished without a
// shutdown would instead reach the same state through a heartbeat timeout, so
// both endings count.
func (c *pinChaosCluster) awaitSessionLost(runID string, within time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if detail := c.sessionCreationEnded(runID); detail != "" {
			return detail
		}
		time.Sleep(250 * time.Millisecond)
	}
	c.t.Fatalf("run %s never recorded the end of its pin's session creation activity within %s",
		runID, within)
	return ""
}

// sessionCreationEnded reports how the session creation activity ended, or "".
func (c *pinChaosCluster) sessionCreationEnded(runID string) string {
	c.t.Helper()
	iter := c.temporal.GetWorkflowHistory(c.ctx, runID, "", false,
		enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	// Scheduled event id -> activity type, so a terminal activity event can be
	// attributed without guessing at ordering.
	creation := map[int64]bool{}
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return ""
		}
		if scheduled := event.GetActivityTaskScheduledEventAttributes(); scheduled != nil {
			if scheduled.GetActivityType().GetName() == sessionCreationActivityName {
				creation[event.GetEventId()] = true
			}
			continue
		}
		if failed := event.GetActivityTaskFailedEventAttributes(); failed != nil {
			if creation[failed.GetScheduledEventId()] {
				return "ActivityTaskFailed: " + failed.GetFailure().GetMessage()
			}
			continue
		}
		if timedOut := event.GetActivityTaskTimedOutEventAttributes(); timedOut != nil {
			if creation[timedOut.GetScheduledEventId()] {
				return "ActivityTaskTimedOut: " + timedOut.GetFailure().GetMessage()
			}
		}
	}
	return ""
}

// pinDirectories lists this cluster's pin directories on the host's disk.
func (c *pinChaosCluster) pinDirectories() []string {
	c.t.Helper()
	entries, err := os.ReadDir(c.workRoot)
	if err != nil {
		c.t.Fatalf("read the work root %s: %v", c.workRoot, err)
	}
	var found []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "ork-pin-") {
			found = append(found, filepath.Join(c.workRoot, entry.Name()))
		}
	}
	sort.Strings(found)
	return found
}

// awaitPinDirectory blocks until the pin host has created the pin's shared
// directory, and returns its path. Its existence is the Orker's own record that
// AcquirePin ran, so it is what a test waits for rather than a task status.
func (c *pinChaosCluster) awaitPinDirectory(within time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if dirs := c.pinDirectories(); len(dirs) == 1 {
			if _, err := os.Stat(filepath.Join(dirs[0], "lease.json")); err == nil {
				return dirs[0]
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.t.Fatalf("no pin directory with a lease appeared under %s within %s", c.workRoot, within)
	return ""
}

// ---------------------------------------------------------------------------
// Scenario 1 — the pin host dies BETWEEN members
// ---------------------------------------------------------------------------

// TestIntegration_APinHostLostBetweenMembersKeepsTheFirstMembersCommittedWork
// is a pin host lost mid-pin, measured at the one moment no
// unit test can construct: the gap AFTER a member finished successfully and
// BEFORE the next one was dispatched.
//
// The distinction from "killed while a member container runs" is the whole
// point. There, the failure erases work that was in flight. Here, `clone`
// already ran to completion on the real Orker: it wrote values.json, wrote an
// artifact, and the Orker hashed, uploaded and durably committed all of it. The
// pin then loses its host with nothing in flight. So the question this test
// exists to answer is not "does the run fail" — the classification tests
// already say it does — but "does the platform go back and invalidate work it
// had already durably committed". It must not: the failure is that the pin
// cannot CONTINUE, not that what finished never happened (AGENTS.md §15).
//
// The sequence is enforced by the cluster's own record, never by a sleep:
//
//  1. `clone` (pinned) succeeds on the real pin host;
//  2. `gate` (unpinned, another pool) starts and is held, so `compile` cannot
//     become ready;
//  3. the pin host's worker is stopped — genuinely, so it stops polling and
//     stops heartbeating the session's creation activity;
//  4. the run's history is polled until it records that creation activity
//     ending, i.e. until Temporal itself has declared the pin lost;
//  5. only then is `gate` released, so `compile` becomes ready against a pin
//     whose host is already, provably gone.
func TestIntegration_APinHostLostBetweenMembersKeepsTheFirstMembersCommittedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	c := newPinChaosCluster(t, ctx, 1)

	image := pinnedAlpineForCommitE2E(t)
	name := "pin-gap-" + c.token
	version := c.seed(name,
		// A generous lifetime: this test is about losing the HOST, and a pin
		// that timed out on its own clock would be PIN_TIMEOUT instead.
		[]*orkstackv1.Pin{{Name: "build", TimeoutSeconds: 900}},
		[]*orkstackv1.Task{{
			Id:   "clone",
			Uses: image,
			Pin:  "build",
			Pool: c.pool,
			// Every task of a pin runs as the Orker's own user, per
			// docs/DSL.md's pin section.
			RunAsOrkerUser: true,
			Image:          &orkstackv1.ImageRef{Reference: image, Pinned: image},
			Command: []string{"/bin/sh", "-c",
				`set -e; mkdir -p /orkstack/shared/src; ` +
					`printf 'cloned' > /orkstack/shared/src/tree.txt; ` +
					`printf '{"cloned":true}' > /orkstack/output/values.json; ` +
					`printf 'the manifest' > /orkstack/output/artifacts/manifest.txt`},
			TimeoutSeconds: 120,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}, {
			Id:             "gate",
			Uses:           image,
			Pool:           c.gatePool,
			Needs:          []string{"clone"},
			Image:          &orkstackv1.ImageRef{Reference: image, Pinned: image},
			Command:        []string{"/bin/sh", "-c", `printf '{}' > /orkstack/output/values.json`},
			TimeoutSeconds: 600,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}, {
			Id:             "compile",
			Uses:           image,
			Pin:            "build",
			Pool:           c.pool,
			Needs:          []string{"gate"},
			RunAsOrkerUser: true,
			Image:          &orkstackv1.ImageRef{Reference: image, Pinned: image},
			Command: []string{"/bin/sh", "-c",
				`set -e; test -f /orkstack/shared/src/tree.txt; ` +
					`printf '{"compiled":true}' > /orkstack/output/values.json`},
			TimeoutSeconds: 120,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}})

	runID := c.submit(name, version)
	t.Logf("EVIDENCE run_id=%s pool=%s orker=%s", runID, c.pool, c.orkerID)

	// 1. The first member really finished, on the real host, durably.
	afterClone := c.awaitTaskStatus(runID, "clone", 4*time.Minute,
		orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED)
	clone := taskRunOf(t, afterClone, "clone")
	if clone.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("clone = %v (%v); this scenario needs its first member to SUCCEED",
			clone.GetStatus(), clone.GetError())
	}
	// Everything the run recorded about it, captured BEFORE the host is lost,
	// so the comparison at the end is against what a client was actually told.
	cloneCommit := clone.GetCommit().GetUri()
	cloneValues := clone.GetValuesRef().GetUri()
	cloneArtifacts := len(clone.GetArtifacts())
	if cloneCommit == "" {
		t.Fatal("clone succeeded with no durable commit; there would be nothing for this test to protect")
	}
	if cloneArtifacts == 0 {
		t.Fatal("clone succeeded with no artifacts; there would be nothing for this test to protect")
	}
	t.Logf("EVIDENCE clone committed uri=%s values=%s artifacts=%d orker=%s",
		cloneCommit, cloneValues, cloneArtifacts, clone.GetOrkerId())

	// 2. The gap: `gate` is in flight on another pool, so `compile` cannot be
	//    ready and nothing at all is running on the pin.
	select {
	case <-c.gateArrived:
	case <-time.After(2 * time.Minute):
		t.Fatal("the gate task never reached its pool, so there is no gap to kill the host in")
	}
	if got := c.ranOnHost(); len(got) != 1 || got[0] != "clone" {
		t.Fatalf("the pin host ran %v at the moment of the kill, want exactly [clone] — "+
			"this scenario requires NOTHING in flight on the pin", got)
	}

	// 3. The kill: the pin host's worker really stops.
	c.stopHost()

	// 4. Temporal's own record of the loss, before the next member exists.
	detail := c.awaitSessionLost(runID, 2*time.Minute)
	t.Logf("EVIDENCE the cluster ended the pin's session: %s", detail)

	// 5. Release the gap. `compile` now becomes ready against a dead pin.
	c.openGate()

	final := c.awaitRunTerminal(runID, 4*time.Minute)

	// ── the run failed, and it failed ABOUT THE PIN ──
	if final.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run status = %v, want FAILED: nobody cancelled this run, the platform lost "+
			"the Orker its pin was on", final.GetStatus())
	}
	compile := taskRunOf(t, final, "compile")
	// FAILED with PIN_HOST_LOST, not CANCELED, and the sequencing above is what
	// makes that determinate rather than a coin flip: nothing else of this run
	// was in flight when the host went, so `compile` is the FIRST thing to
	// touch the dead session. The SDK settles its activity future with
	// ErrSessionFailed straight away, collect asks pinLoss before recordFailure,
	// and the answer is the pin's. CANCELED here would mean fail-fast reached it
	// first, which would say an operator ended the run — the reading
	// PIN_HOST_LOST exists to prevent, because it invites retrying the task on an Orker whose
	// /orkstack/shared is empty.
	if compile.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("compile status = %v, want FAILED: the platform lost the pin's host, "+
			"which is not a cancellation", compile.GetStatus())
	}
	if got := compile.GetError().GetCode(); got != "PIN_HOST_LOST" {
		t.Errorf("compile error code = %q, want PIN_HOST_LOST", got)
	}
	if got := compile.GetError().GetCategory(); got !=
		orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Errorf("compile error category = %v, want INFRASTRUCTURE_ERROR", got)
	}
	if compile.GetError().GetRetryable() {
		t.Error("compile's error is retryable; a pin never moves, so retrying it elsewhere " +
			"would run it against an empty /orkstack/shared")
	}
	if compile.GetPin() != "build" {
		t.Errorf("compile TaskRun.pin = %q, want build", compile.GetPin())
	}
	t.Logf("EVIDENCE compile status=%v code=%q message=%q",
		compile.GetStatus(), compile.GetError().GetCode(), compile.GetError().GetMessage())
	// It never ran on the host, which is the fact the whole scenario is built
	// around: the pin could not continue.
	if got := c.ranOnHost(); len(got) != 1 || got[0] != "clone" {
		t.Errorf("the pin host ran %v; compile must never have reached an Orker", got)
	}
	// The run's own reason names the pin rather than the bare word "canceled".
	if reason := final.GetError().GetMessage(); !strings.Contains(reason, "pin") &&
		!strings.Contains(reason, "PIN") {
		t.Errorf("run error = %q, want it to explain the lost pin", reason)
	}

	// ── THE POINT: the already-succeeded member was not retroactively undone ──
	after := taskRunOf(t, final, "clone")
	if after.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("clone is %v in the final run, want SUCCEEDED: a pin that cannot continue must "+
			"not invalidate work that already committed durably", after.GetStatus())
	}
	if after.GetError() != nil {
		t.Errorf("clone carries error %v in the final run, want none", after.GetError())
	}
	if got := after.GetCommit().GetUri(); got != cloneCommit {
		t.Errorf("clone's commit uri is now %q, was %q", got, cloneCommit)
	}
	if got := after.GetValuesRef().GetUri(); got != cloneValues {
		t.Errorf("clone's values ref is now %q, was %q", got, cloneValues)
	}
	if got := len(after.GetArtifacts()); got != cloneArtifacts {
		t.Errorf("clone now reports %d artifact(s), was %d", got, cloneArtifacts)
	}
	if after.GetOrkerId() != c.orkerID {
		t.Errorf("clone's orker_id = %q, want the real pin host %q", after.GetOrkerId(), c.orkerID)
	}
	// And the artifact bytes are still really in the bucket, which is what
	// "durably committed" has to mean: a surviving TaskRun that pointed at
	// nothing would be the same lie as an erased one.
	assertArtifactsStillInTheBucket(t, ctx, after.GetArtifacts())
	t.Logf("EVIDENCE clone survived: status=%v commit=%s artifacts=%d",
		after.GetStatus(), after.GetCommit().GetUri(), len(after.GetArtifacts()))

	// The pin's own directory was NOT released — nothing could schedule
	// ReleasePin onto a session Temporal had already failed — which is exactly
	// what the boot reap and the lease exist for.
	if got := c.releases(); len(got) != 0 {
		t.Errorf("ReleasePin ran %d time(s) for a pin whose host was gone: %v", len(got), got)
	}
	if dirs := c.pinDirectories(); len(dirs) != 1 {
		t.Errorf("pin directories on the host = %v, want the one the lost pin left behind", dirs)
	}

	pinChaosProof(t, "pin-host-lost-between-members.txt", []string{
		"# the pin host was stopped in the GAP between two members",
		"run                    " + runID,
		"pool / orker           " + c.pool + " / " + c.orkerID,
		"pin instance           " + c.pinInstanceFor("clone"),
		"host ran               " + strings.Join(c.ranOnHost(), ", "),
		"cluster ended session  " + detail,
		fmt.Sprintf("clone                  %v, commit %s, %d artifact(s), orker %s",
			after.GetStatus(), after.GetCommit().GetUri(), len(after.GetArtifacts()), after.GetOrkerId()),
		fmt.Sprintf("compile                %v, code %q", compile.GetStatus(), compile.GetError().GetCode()),
		fmt.Sprintf("run                    %v: %s", final.GetStatus(), final.GetError().GetMessage()),
		fmt.Sprintf("ReleasePin calls       %d (the host was gone; the boot reap and lease are the backstops)",
			len(c.releases())),
	})
}

// ---------------------------------------------------------------------------
// Scenario 3 — the run is TERMINATED from outside while a pin is open
// ---------------------------------------------------------------------------

// TestIntegration_ATerminatedRunLeavesItsPinForTheLeaseSweep is the lease
// sweep's reason to exist (docs/OCI_ACTIVITY_CONTRACT.md §6.4), driven the
// only way it can be: by hard-terminating a REAL
// workflow execution out from under a live Orker.
//
// Termination is deliberately not the engine's own cancel-run path. A cancel is
// observable — RunWorkflow's fail() runs, the pin's coroutine releases the
// directory, CompleteSession frees the slot — and that path is already proven.
// TerminateWorkflow lets NO workflow code run at all: the execution simply
// ends. So nothing OrkStack-side ever asks the host to release the pin, the host
// is still alive and still holds the directory, and the ONLY thing left that
// can collect it is the lease.
//
// The test proves all three halves of that sentence:
//
//  1. after termination the pin's directory is still on the host's disk and
//     ReleasePin was never called;
//  2. a sweep at the CURRENT time leaves it alone — the lease is live, and a
//     sweep that removed a live pin's directory would be a data-loss bug, so
//     the removal below would prove nothing without this;
//  3. a sweep at a time past the lease's own expiresAt removes it.
//
// (3) calls SweepExpiredPinLeases directly with an explicit `now` rather than
// waiting out the real lease, which is minutes of wall clock for no extra
// information: the production sweeper is a one-minute ticker over this exact
// function (Activities.SweepPinLeases), and the only thing a real wait would
// add is the ticker, which is not the logic under test.
func TestIntegration_ATerminatedRunLeavesItsPinForTheLeaseSweep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	c := newPinChaosCluster(t, ctx, 1)

	image := pinnedAlpineForCommitE2E(t)
	name := "pin-terminated-" + c.token
	version := c.seed(name,
		[]*orkstackv1.Pin{{Name: "build", TimeoutSeconds: 600}},
		[]*orkstackv1.Task{{
			Id:             "hold",
			Uses:           image,
			Pin:            "build",
			Pool:           c.pool,
			RunAsOrkerUser: true,
			Image:          &orkstackv1.ImageRef{Reference: image, Pinned: image},
			// It marks the shared directory and then holds the pin open. The
			// sleep is the container's own test payload, which is the one place
			// AGENTS.md §11.1 allows one: the test itself never sleeps to
			// synchronize, it polls for the directory this command creates.
			Command: []string{"/bin/sh", "-c",
				`set -e; printf 'work in progress' > /orkstack/shared/wip.txt; sleep 300`},
			TimeoutSeconds: 400,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}})

	runID := c.submit(name, version)
	t.Logf("EVIDENCE run_id=%s", runID)

	// The pin is open and the host has really made the directory: this is the
	// Orker's own AcquirePin, not a path this test constructed.
	pinRoot := c.awaitPinDirectory(4 * time.Minute)
	// And the member is really RUNNING inside it — waited for through the file
	// the container itself writes, because AcquirePin precedes the container
	// and a directory alone would not prove the pin is in use yet.
	shared := filepath.Join(pinRoot, "shared", "wip.txt")
	if raw := awaitFileContent(t, shared, 4*time.Minute); raw != "work in progress" {
		t.Fatalf("shared/wip.txt reads %q, want the running container's marker", raw)
	}
	t.Logf("EVIDENCE pin directory %s", pinRoot)

	// ── the hard external termination ──
	//
	// No graceful cancel, no engine involvement: the operator ends the
	// execution and workflow code never observes anything.
	if err := c.temporal.TerminateWorkflow(ctx, runID, "",
		"operator terminated the run while its pin was open"); err != nil {
		t.Fatalf("TerminateWorkflow(%s): %v", runID, err)
	}

	// (1) The directory outlives the run, and nothing asked for its release.
	//
	// Confirmed twice with a bounded wait between, because "still there" has to
	// mean "still there after everything that COULD have removed it had time
	// to": the SDK frees the session locally when its heartbeat is refused, and
	// if that path removed the directory it would do so in this window.
	if _, err := os.Stat(pinRoot); err != nil {
		t.Fatalf("the pin directory vanished the moment the run was terminated: %v", err)
	}
	if !pinDirectorySurvives(t, pinRoot, 20*time.Second) {
		t.Fatalf("the pin directory %s was removed after termination; nothing "+
			"OrkStack-side runs on this path, so nothing should have", pinRoot)
	}
	if got := c.releases(); len(got) != 0 {
		t.Errorf("ReleasePin ran %d time(s) for a TERMINATED run: %v — termination does not let "+
			"workflow code schedule anything", len(got), got)
	}
	t.Logf("EVIDENCE the terminated run left %s behind, with %d ReleasePin call(s)",
		pinRoot, len(c.releases()))

	// (2) A sweep NOW must not touch a live lease.
	expires := pinLeaseExpiry(t, pinRoot)
	if !expires.After(time.Now()) {
		t.Fatalf("the lease at %s already expired at %s; this scenario needs a live one",
			pinRoot, expires)
	}
	removed, err := orker.SweepExpiredPinLeases(c.workRoot, time.Now())
	if err != nil {
		t.Fatalf("sweep at the current time: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("a sweep at the current time removed %v; the lease runs until %s",
			removed, expires)
	}
	if _, err := os.Stat(pinRoot); err != nil {
		t.Fatalf("the pin directory did not survive a sweep of a live lease: %v", err)
	}

	// (3) A sweep past the lease removes it. This is the backstop closing.
	after := expires.Add(time.Second)
	removed, err = orker.SweepExpiredPinLeases(c.workRoot, after)
	if err != nil {
		t.Fatalf("sweep past the lease: %v", err)
	}
	if len(removed) != 1 || removed[0] != pinRoot {
		t.Fatalf("the expired sweep removed %v, want exactly [%s]", removed, pinRoot)
	}
	if _, err := os.Stat(pinRoot); !os.IsNotExist(err) {
		t.Fatalf("the swept pin directory is still on disk: %v", err)
	}
	if dirs := c.pinDirectories(); len(dirs) != 0 {
		t.Errorf("pin directories still under %s: %v", c.workRoot, dirs)
	}

	// What a client is told about a terminated run is a separate question from
	// this scenario's — a termination is not an OrkStack state transition — so
	// it is recorded rather than asserted.
	final := c.run(runID)
	t.Logf("EVIDENCE terminated run reads back as %v", final.GetStatus())

	pinChaosProof(t, "pin-terminated-lease-sweep.txt", []string{
		"# an operator terminated the run while its pin was open",
		"run                    " + runID,
		"pool / orker           " + c.pool + " / " + c.orkerID,
		"pin instance           " + c.pinInstanceFor("hold"),
		"pin directory          " + pinRoot,
		"lease expiresAt        " + expires.Format(time.RFC3339Nano),
		"ReleasePin calls       0  (termination runs no workflow code at all)",
		"sweep at now           removed nothing (the lease is live)",
		"sweep at expiresAt+1s  removed " + pinRoot,
		fmt.Sprintf("run reads back as      %v", final.GetStatus()),
	})
}

// ---------------------------------------------------------------------------
// Scenario 4 — every pin host in the pool is SATURATED
// ---------------------------------------------------------------------------

// TestIntegration_ASaturatedPinPoolFailsTheTriggeringTaskWithPinUnavailable is
// "every pin host in the pool is full for the whole creation timeout" case.
//
// It is a DIFFERENT scenario from the two that are already covered, and the
// difference is the whole reason this test is slow enough to need its own
// function:
//
//   - "no pin host in the pool at all" is refused at SUBMIT with
//     PoolNoPinHostError, and pools_test.go proves that with no cluster.
//   - TestRunWorkflow_FailsTheTriggeringTaskWhenNoPinHostHasAFreeSlot proves
//     the CLASSIFICATION by mocking the creation activity to fail immediately.
//     What it cannot prove is the MECHANISM, and the mechanism is not what the
//     mock suggests: a saturated SDK session worker does not refuse the
//     request. Its creation poller parks in waitForAvailableToken and stops
//     polling entirely, so the creation task is never STARTED and what
//     eventually ends it is the ScheduleToStartTimeout the SDK sets to
//     SessionOptions.CreationTimeout — the engine's five-minute
//     pinCreationTimeout. Whether that path really terminates, and really
//     terminates as PIN_UNAVAILABLE rather than being retried forever by the
//     creation activity's own retry policy, is only answerable against a real
//     cluster.
//
// So this test costs a real pinCreationTimeout of wall clock, deliberately. It
// is in its own function so `-run` can skip it while iterating, and the elapsed
// time is ASSERTED: a PIN_UNAVAILABLE that arrived in two seconds would be some
// other failure wearing this one's code, and the assertion is what tells the
// two apart.
//
// The saturation is real: one Orker, ORK_ORKER_MAX_PINS=1, already holding an
// open pin for run A whose member is a live container.
func TestIntegration_ASaturatedPinPoolFailsTheTriggeringTaskWithPinUnavailable(t *testing.T) {
	// pinCreationTimeout is a hardcoded 5 minutes in internal/engine/workflow.go
	// and is deliberately not a setting, so the budget has to cover it
	// plus the run that holds the pin open for its whole length.
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()
	c := newPinChaosCluster(t, ctx, 1)

	image := pinnedAlpineForCommitE2E(t)

	// ── run A: takes the pool's only pin slot and keeps it ──
	//
	// It must outlive run B's whole creation timeout, so the container's own
	// sleep is longer than pinCreationTimeout. That sleep is the workload's
	// test payload, not this test synchronizing by guesswork.
	holderName := "pin-holder-" + c.token
	holderVersion := c.seed(holderName,
		[]*orkstackv1.Pin{{Name: "hold", TimeoutSeconds: 900}},
		[]*orkstackv1.Task{{
			Id:             "occupy",
			Uses:           image,
			Pin:            "hold",
			Pool:           c.pool,
			RunAsOrkerUser: true,
			Image:          &orkstackv1.ImageRef{Reference: image, Pinned: image},
			Command: []string{"/bin/sh", "-c",
				`set -e; printf 'holding the only pin slot' > /orkstack/shared/holder.txt; sleep 480`},
			TimeoutSeconds: 600,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}})
	holderID := c.submit(holderName, holderVersion)
	t.Logf("EVIDENCE holder run_id=%s", holderID)
	t.Cleanup(func() {
		// Give the container back promptly rather than leaving it to the
		// worker's shutdown: the work root removal below has to succeed.
		terminateCtx, terminateCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer terminateCancel()
		if err := c.temporal.TerminateWorkflow(terminateCtx, holderID, "",
			"test finished with the pin holder"); err != nil {
			t.Logf("cleanup: terminate the holder run %s: %v", holderID, err)
		}
	})

	// The pin is genuinely OPEN and its slot genuinely taken: the directory
	// exists and the container inside it is running.
	holderPin := c.awaitPinDirectory(4 * time.Minute)
	if got := awaitFileContent(t, filepath.Join(holderPin, "shared", "holder.txt"),
		4*time.Minute); got != "holding the only pin slot" {
		t.Fatalf("the holder's shared marker reads %q", got)
	}
	t.Logf("EVIDENCE the pool's only pin slot is held by %s", holderPin)

	// ── run B: a second pin onto the same, now full, pool ──
	//
	// Submit still ACCEPTS it, and that is correct: the pin-host probe asks whether
	// the pool has a pin host at all (max_concurrent_pins > 0), which it does.
	// Saturation is not a submit-time fact — it is a race the workflow has to
	// lose, which is what the rest of this test measures.
	waiterName := "pin-waiter-" + c.token
	waiterVersion := c.seed(waiterName,
		[]*orkstackv1.Pin{{Name: "build", TimeoutSeconds: 900}},
		[]*orkstackv1.Task{{
			Id:             "queued",
			Uses:           image,
			Pin:            "build",
			Pool:           c.pool,
			RunAsOrkerUser: true,
			Image:          &orkstackv1.ImageRef{Reference: image, Pinned: image},
			Command: []string{"/bin/sh", "-c",
				`printf '{"ran":true}' > /orkstack/output/values.json`},
			TimeoutSeconds: 120,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}, {
			Id:             "after",
			Uses:           image,
			Pin:            "build",
			Pool:           c.pool,
			Needs:          []string{"queued"},
			RunAsOrkerUser: true,
			Image:          &orkstackv1.ImageRef{Reference: image, Pinned: image},
			Command: []string{"/bin/sh", "-c",
				`printf '{"ran":true}' > /orkstack/output/values.json`},
			TimeoutSeconds: 120,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}})
	started := time.Now()
	waiterID := c.submit(waiterName, waiterVersion)
	t.Logf("EVIDENCE waiter run_id=%s submitted at %s", waiterID, started.Format(time.RFC3339))

	// The wait itself. Budgeted at pinCreationTimeout plus room for the
	// cluster's own scheduling, and polled rather than slept through.
	final := c.awaitRunTerminal(waiterID, 9*time.Minute)
	waited := time.Since(started)

	if final.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("waiter run status = %v after %s, want FAILED (error: %v)",
			final.GetStatus(), waited.Round(time.Second), final.GetError())
	}
	queued := taskRunOf(t, final, "queued")
	if queued.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("queued status = %v, want FAILED", queued.GetStatus())
	}
	if got := queued.GetError().GetCode(); got != "PIN_UNAVAILABLE" {
		t.Errorf("queued error code = %q, want PIN_UNAVAILABLE", got)
	}
	if got := queued.GetError().GetCategory(); got !=
		orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Errorf("queued error category = %v, want INFRASTRUCTURE_ERROR", got)
	}
	if queued.GetError().GetRetryable() {
		t.Error("queued's error is retryable; the engine never re-creates a session for a pin")
	}
	// It never reached an Orker, and the absence of a dispatch stamp is the
	// only thing that says so (pinOpened). The dispatch stamp is
	// scheduled_at; started_at (the Orker's start) is unset as well.
	if queued.GetScheduledAt() != nil {
		t.Errorf("queued scheduled_at = %v, want unset: the pin never opened, so nothing was dispatched",
			queued.GetScheduledAt().AsTime())
	}
	if queued.GetStartedAt() != nil {
		t.Errorf("queued started_at = %v, want unset: the pin never opened, so nothing was dispatched",
			queued.GetStartedAt().AsTime())
	}
	// The member behind it never ran either.
	if got := statusOf(final, "after"); got != orkstackv1.Status_STATUS_CANCELED {
		t.Errorf("after status = %v, want CANCELED", got)
	}

	// This is what makes it the SATURATION path rather than an instant refusal
	// wearing the same code: the run waited out the creation timeout. The floor
	// is deliberately loose (the constant is 5 minutes; the cluster adds its own
	// scheduling), but it is far above any immediate failure.
	if waited < 4*time.Minute {
		t.Errorf("the waiter failed after only %s; PIN_UNAVAILABLE is supposed to be the "+
			"verdict after a full pinCreationTimeout of waiting for a free slot, so this "+
			"failure came from somewhere else", waited.Round(time.Second))
	}
	t.Logf("EVIDENCE the waiter failed with PIN_UNAVAILABLE after %s", waited.Round(time.Second))

	// The waiter left NO pin directory: no member of it ever reached the host,
	// so AcquirePin never ran. Exactly one directory is on disk, and it is the
	// holder's.
	dirs := c.pinDirectories()
	if len(dirs) != 1 || dirs[0] != holderPin {
		t.Errorf("pin directories = %v, want only the holder's %s", dirs, holderPin)
	}
	if got := c.releases(); len(got) != 0 {
		t.Errorf("ReleasePin ran %d time(s) for a pin that never opened: %v", len(got), got)
	}
	// And the incumbent is untouched: a saturated pool must refuse the newcomer,
	// never disturb the run already holding the slot.
	holder := c.run(holderID)
	if got := statusOf(holder, "occupy"); got != orkstackv1.Status_STATUS_RUNNING {
		t.Errorf("the holder's member is %v, want it still RUNNING: the second pin's failure "+
			"must not disturb the run that legitimately holds the slot", got)
	}
	if got := c.ranOnHost(); len(got) != 1 || got[0] != "occupy" {
		t.Errorf("the pin host ran %v, want only the holder's [occupy]", got)
	}

	pinChaosProof(t, "pin-pool-saturated.txt", []string{
		"# every pin host in the pool was full for the whole of pinCreationTimeout",
		"pool / orker           " + c.pool + " / " + c.orkerID,
		"ORK_ORKER_MAX_PINS     1",
		"holder run             " + holderID + " (holds the only slot, container sleeping)",
		"holder pin directory   " + holderPin,
		"waiter run             " + waiterID,
		fmt.Sprintf("waiter waited          %s before failing", waited.Round(time.Second)),
		fmt.Sprintf("waiter queued          %v, code %q, started_at %v",
			queued.GetStatus(), queued.GetError().GetCode(), queued.GetStartedAt() != nil),
		fmt.Sprintf("waiter after           %v", statusOf(final, "after")),
		fmt.Sprintf("waiter run             %v: %s", final.GetStatus(), final.GetError().GetMessage()),
		"waiter pin directories 0 (no member ever reached the host, so AcquirePin never ran)",
		fmt.Sprintf("holder still           %v", statusOf(holder, "occupy")),
	})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func requirePinChaosDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "the docker CLI is not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "version",
		"--format", "{{.Server.Version}}").CombinedOutput(); err != nil {
		infraAbsent(t, "the docker daemon is not reachable: %v: %s", err, out)
	}
}

// assertArtifactsStillInTheBucket HEADs every artifact a surviving TaskRun
// points at, in the object store the Orker uploaded it to.
func assertArtifactsStillInTheBucket(
	t *testing.T,
	ctx context.Context,
	refs []*orkstackv1.ArtifactRef,
) {
	t.Helper()
	cfg := s3Config()
	raw := newRawS3ClientForCommitE2E(t, cfg)
	prefix := "s3://" + cfg.S3Bucket + "/"
	for _, ref := range refs {
		uri := ref.GetUri()
		if !strings.HasPrefix(uri, prefix) {
			t.Errorf("artifact %q has uri %q, want one in this deployment's bucket %q",
				ref.GetName(), uri, cfg.S3Bucket)
			continue
		}
		key := strings.TrimPrefix(uri, prefix)
		head, err := raw.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Errorf("artifact %q (%s) is no longer in the bucket: %v", ref.GetName(), uri, err)
			continue
		}
		if got := aws.ToInt64(head.ContentLength); got != ref.GetSizeBytes() {
			t.Errorf("artifact %q is %d bytes in the bucket, the run says %d",
				ref.GetName(), got, ref.GetSizeBytes())
		}
	}
}

func mustQueueForPool(t *testing.T, pool string) string {
	t.Helper()
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("QueueForPool(%q): %v", pool, err)
	}
	return queue
}

// s3NamespaceConfig forces the S3 backend for the namespace registry, the way
// pool_routing_integration_test.go does: the allow-list this suite enforces is
// a document that made a round trip through the bucket.
func s3NamespaceConfig(cfg artifacts.Config) artifacts.Config {
	cfg.Backend = artifacts.BackendS3
	return cfg
}

// pinChaosWorkRoot is this execution's OWN Orker work root, under the
// daemon-shared .data/work.
//
// It is per execution rather than shared because this suite reaps ork-pin-*
// directories in cleanup: pointed at a work root another agent's Orker used,
// that cleanup would delete a live pin belonging to somebody else's run.
func pinChaosWorkRoot(t *testing.T, token string) string {
	t.Helper()
	base := os.Getenv(orker.WorkRootVar)
	if base == "" {
		abs, err := filepath.Abs(filepath.Join("..", "..", ".data", "work"))
		if err != nil {
			t.Fatalf("resolve the work root: %v", err)
		}
		base = abs
	}
	root := filepath.Join(base, "pin-chaos-"+token)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create the work root %s: %v", root, err)
	}
	t.Cleanup(func() {
		if _, err := orker.ReapPinDirectories(root); err != nil {
			t.Logf("cleanup: reap %s: %v", root, err)
		}
		removeSealedTree(t, root)
	})
	return root
}

// removeSealedTree removes an Orker work root, including the trees a workload or
// a sealed input directory made unremovable.
//
// os.RemoveAll alone is not enough twice over: the per-attempt input/ tree is
// sealed to 0500, and an attempt this suite interrupted may still be being torn
// down by the Orker when cleanup runs. So the modes are reasserted on the way
// down and the removal is retried against a short deadline — this root lives
// under the daemon-shared .data/work, and leaving a tree there would be
// somebody else's mystery tomorrow.
func removeSealedTree(t *testing.T, root string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
		if last = os.RemoveAll(root); last == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Logf("cleanup: remove %s: %v", root, last)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// pinLeaseExpiry reads the expiresAt the Orker wrote into a pin's lease.
//
// The lease is read rather than computed: what the sweep decides on is the
// file's own timestamp, so a test that recomputed the expected value could
// agree with itself while disagreeing with the Orker.
func pinLeaseExpiry(t *testing.T, pinRoot string) time.Time {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(pinRoot, "lease.json"))
	if err != nil {
		t.Fatalf("read the pin lease: %v", err)
	}
	var lease struct {
		ExpiresAt string `json:"expiresAt"`
		RunID     string `json:"runId"`
		Pin       string `json:"pin"`
	}
	if err := json.Unmarshal(raw, &lease); err != nil {
		t.Fatalf("decode the pin lease %s: %v", raw, err)
	}
	expires, err := time.Parse(time.RFC3339Nano, lease.ExpiresAt)
	if err != nil {
		t.Fatalf("the lease's expiresAt %q is not RFC 3339: %v", lease.ExpiresAt, err)
	}
	return expires
}

// awaitFileContent polls until a file the WORKLOAD writes exists, and returns
// its contents. It is how this suite waits for "the container is really running
// inside the pin": the pin directory is created by AcquirePin before the
// container starts, so the directory alone says nothing about the workload.
func awaitFileContent(t *testing.T, path string, within time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(path); err == nil {
			return string(raw)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s never appeared within %s", path, within)
	return ""
}

// pinDirectorySurvives reports whether a directory is still there for the whole
// of a bounded window, which is how "nothing removed it" is asserted without a
// single guessed sleep standing in for the claim.
func pinDirectorySurvives(t *testing.T, dir string, window time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(dir); err != nil {
			return false
		}
		time.Sleep(500 * time.Millisecond)
	}
	_, err := os.Stat(dir)
	return err == nil
}

func pinChaosProof(t *testing.T, name string, lines []string) {
	t.Helper()
	header := []string{
		"# task pin chaos evidence (AGENTS.md §11)",
		"# Every fact below came from the cluster, from the Orker, or from the",
		"# host's own disk, never from the engine's expectation.",
		"# recorded " + time.Now().UTC().Format(time.RFC3339),
		"",
	}
	proof.Write(t, "engine", name, []byte(strings.Join(append(header, lines...), "\n")+"\n"))
}
