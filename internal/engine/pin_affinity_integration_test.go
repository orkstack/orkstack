//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// This file is the feature's headline proof, and it is deliberately the only
// test in the tree that puts THREE real Orkers on ONE pool.
//
// The two sentences it exists to be able to say:
//
//  1. AFFINITY. While three pin hosts are alive, idle and polling one pool,
//     every task of a pin lands on exactly ONE of them — never split across
//     two — and the shared directory really is one filesystem, proved by a
//     random token written by the first container and read back by the second.
//     Checked run by run, over pinAffinityRuns consecutive runs.
//  2. NO FAILOVER. When that one host dies mid-pin, the pin dies with it:
//     the run reaches FAILED with PIN_HOST_LOST, and the two survivors are
//     never handed any task of that run — the pin is not silently re-opened
//     somewhere the shared directory does not exist.
//
// WHY THE ORKERS HERE ARE REAL. pool_routing_integration_test.go's poolOrker
// is a stub whose execute() returns a canned result and starts no container.
// That is enough to prove Temporal ROUTING (which channel a task was
// delivered on) and it is not enough for anything here: "the same OrkerId
// twice" is two strings matching, and only a real bind mount shared by two
// real containers proves the affinity was a real host. So every Orker below
// is an internal/orker.Activities with a real DockerRuntime, a real MinIO
// artifact/commit/log store and its own bind-mountable work root — the same
// construction cmd/orker performs, registered with the same three activity
// names and the same session worker options.
//
// WHAT IS NOT SIMULATED. The engine is the real one: api.Service resolves the
// plan (so PoolResolver.Plan runs the real submit-time pin-host refusal), the
// real RunWorkflow opens the real Temporal Worker Session, schedules the pin's
// members on it, and releases it. Only two things are written by hand rather
// than applied: the stored definition's Task.image (apply-time registry
// resolution needs a registry; durable_commit_e2e_integration_test.go writes
// the same field the same way) and the namespace document.

// pinAffinityRuns is how many consecutive pinned runs Proof 1 checks.
//
// The number is the point: one run landing its two tasks on one Orker proves
// very little when three Orkers are idle and any of them could have taken the
// second task. Twenty consecutive runs, each independently asserted, is the
// difference between "it happened" and "it holds".
const pinAffinityRuns = 20

// pinAffinityHoldSeconds is how long Proof 2's first pinned container stays
// alive after announcing itself.
//
// It is the container's OWN payload timing, not a synchronization sleep: the
// test learns who won and when its container started running by polling the
// Orkers' own records against a deadline (awaitPinDispatch, awaitPinPhase) and
// kills the moment it knows. The window only has to outlast the DETECTION of
// the dead host, which is task.HeartbeatTimeout (30 s) from the session's last
// heartbeat — so the task must still be genuinely running when the platform
// notices, or the kill would be proving something about a finished task.
const pinAffinityHoldSeconds = 60

// pinOrker is ONE real Orker of this suite's pool: a real Activities, the
// Temporal worker it is registered on, and — the load-bearing part — a record
// of every task it was ASKED to run, written the instant it was asked.
//
// The record is what proves non-failover. "The run failed" alone does not: a
// bug that failed the run for an unrelated reason AND ALSO re-opened the pin on
// a survivor would look identical. These slices are the only thing that can
// tell the two apart, and they are this suite's equivalent of poolOrker.executed
// — first-hand evidence from the process that was handed the work, not the
// engine's account of what it sent.
type pinOrker struct {
	id         string
	pool       string
	queue      string
	workRoot   string
	activities *orker.Activities
	worker     worker.Worker
	// control and presence are the Orker's own two channels: built by orker.NewChannelWorkers exactly as cmd/orker
	// builds them, so this test Orker and the shipped one are one shape.
	control  worker.Worker
	presence worker.Worker

	// temporal is THIS Orker's own Temporal client. One client per Orker,
	// because three Orkers are three processes with three connections — and
	// because closing one is what makes the kill in Proof 2 a host DEATH
	// rather than a graceful drain. See kill().
	temporal client.Client

	mu sync.Mutex
	// ran is run id -> every task id this Orker was handed, in order. Recorded
	// BEFORE the real activity is called, so a watcher can learn the pin's
	// winner while the pin is still in flight.
	ran map[string][]string
	// phases is "<run>/<task>" -> the §7.1 phases this Orker's dispatcher
	// entered for it, via the real Options.Phases seam. It is how Proof 2
	// knows the container is GENUINELY RUNNING before the host is killed,
	// rather than still staging inputs or pulling an image.
	phases map[string][]string
	// results is "<run>/<task>" -> what the real dispatcher returned, so the
	// shared-directory round trip can be read off the Orker's own answer
	// rather than reconstructed from the run.
	results map[string]*orkstackv1.ExecuteTaskResult

	stopOnce  sync.Once
	closeOnce sync.Once
}

// pinPhaseObserver is this Orker's test-only orker.PhaseObserver: the seam
// Options.Phases documents ("the seam a tracer or a metrics recorder plugs
// into"), used here to observe when an attempt reaches a given phase.
//
// It is the mechanism Proof 2 synchronizes its kill on. Recording which phase
// an attempt is in is the difference between "the activity was entered" — which
// is true while the Orker is still building a workspace and pulling an image —
// and "the container is running", which is the only moment a kill is honestly
// MID-pin.
type pinPhaseObserver struct{ o *pinOrker }

func (p pinPhaseObserver) Begin(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	_ int32,
) (context.Context, orker.AttemptObserver) {
	return ctx, &pinAttemptObserver{o: p.o, key: in.GetRunId() + "/" + in.GetTaskId()}
}

type pinAttemptObserver struct {
	o   *pinOrker
	key string
}

// Phase must not block, per the AttemptObserver contract. Appending under a
// mutex held for the length of an append is the whole of the work.
func (a *pinAttemptObserver) Phase(name string) {
	a.o.mu.Lock()
	a.o.phases[a.key] = append(a.o.phases[a.key], name)
	a.o.mu.Unlock()
}

func (a *pinAttemptObserver) End(orker.AttemptOutcome) {}

// execute is the tap over the production activity, exactly as
// durable_commit_e2e_integration_test.go taps it: record what arrived, then
// delegate to the real dispatcher. Nothing about the execution is simulated.
func (o *pinOrker) execute(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
) (*orkstackv1.ExecuteTaskResult, error) {
	o.mu.Lock()
	o.ran[in.GetRunId()] = append(o.ran[in.GetRunId()], in.GetTaskId())
	o.mu.Unlock()

	result, err := o.activities.ExecuteTask(ctx, in)
	if result != nil {
		o.mu.Lock()
		o.results[in.GetRunId()+"/"+in.GetTaskId()] = result
		o.mu.Unlock()
	}
	return result, err
}

// ranFor is every task of one run this Orker was handed.
func (o *pinOrker) ranFor(runID string) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.ran[runID]...)
}

// sawTask reports whether this Orker was handed one specific task of one run.
func (o *pinOrker) sawTask(runID, taskID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range o.ran[runID] {
		if id == taskID {
			return true
		}
	}
	return false
}

// resultFor is what the real dispatcher returned for one task of one run.
func (o *pinOrker) resultFor(runID, taskID string) *orkstackv1.ExecuteTaskResult {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.results[runID+"/"+taskID]
}

// sawPhase reports whether this Orker's dispatcher took one task of one run
// into a given §7.1 phase.
func (o *pinOrker) sawPhase(runID, taskID, phase string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, got := range o.phases[runID+"/"+taskID] {
		if got == phase {
			return true
		}
	}
	return false
}

// kill makes this Orker's host DEAD, which is what Proof 2 needs and what a
// plain worker.Stop() does not give.
//
// The distinction is the whole of Proof 2's validity, and getting it wrong is
// how an earlier version of this test measured the wrong thing:
//
//   - worker.Stop() is a GRACEFUL DRAIN. The SDK cancels its activity contexts
//     with ErrWorkerShutdown and the in-flight activity then REPORTS its own
//     failure to the server (go.temporal.io/sdk@v1.47.0,
//     internal_worker_base.go baseWorker.Stop -> backgroundContextCancel). The
//     member's own error therefore reaches the workflow FIRST and the task is
//     recorded as an ordinary TASK_FAILED — measured: the run failed 53 ms
//     after the stop, with FAILURE_CATEGORY_UNSPECIFIED. That is a drain, not
//     a death, and it proves nothing about a lost host.
//   - Closing this Orker's Temporal CLIENT is a death. The connection is gone,
//     so this host can never again heartbeat, report a completion, or poll —
//     exactly the state a SIGKILLed process or a lost machine leaves behind.
//     The server then learns about it the only way it can, and the only way
//     production does: the session's creation activity stops heartbeating and
//     heartbeat-times-out at task.HeartbeatTimeout, the SDK marks the session
//     FAILED (createSession's NonRetryableErrorTypes includes
//     "TemporalTimeout:Heartbeat", so it is never re-created elsewhere), the
//     session context is cancelled, and every member on it is cancelled with
//     it. workflow.go's pinLoss turns that into PIN_HOST_LOST.
//
// The client is closed BEFORE the worker is stopped, so the stop can never
// sneak a completion out ahead of the death.
func (o *pinOrker) kill() {
	o.closeOnce.Do(o.temporal.Close)
	o.stop()
}

// stop stops this Orker's Temporal worker. It is the ordinary, graceful
// shutdown, used by cleanup; Proof 2 uses kill().
func (o *pinOrker) stop() {
	o.stopOnce.Do(func() {
		o.worker.Stop()
		o.control.Stop()
		o.presence.Stop()
	})
}

// close releases this Orker's client at the end of the test.
func (o *pinOrker) close() {
	o.closeOnce.Do(o.temporal.Close)
}

// pinDeployment is the whole three-Orker deployment plus the engine driving it.
type pinDeployment struct {
	orkers    []*pinOrker
	byID      map[string]*pinOrker
	pool      string
	queue     string
	namespace string // the ork namespace, not the Temporal one
	image     string
	runs      orkstackv1connect.RunServiceClient
	orkflows  engine.OrkflowStore
	probe     engine.PoolProbe
}

// TestIntegration_PinnedTasksShareOneOrkerAndNeverFailOverItsPinAffinity is
// the acceptance for task pinning's core promise, against real infrastructure.
//
// One deployment, three real pin hosts on one pool, two proofs — see the file
// comment. It is one test function rather than two because the deployment IS
// the expensive part (three Docker-backed Orkers, three MinIO-backed stores,
// one Temporal namespace) and because Proof 2's final act — a fresh run
// succeeding on the two survivors — only means anything against the same pool
// Proof 1 just showed healthy.
func TestIntegration_PinnedTasksShareOneOrkerAndNeverFailOverItsPinAffinity(t *testing.T) {
	cfg := s3Config()
	requireObjectStore(t, cfg)
	requirePinAffinityDocker(t)

	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	// Generous but bounded. Proof 1 alone is pinAffinityRuns real runs of two
	// real containers each, and Proof 2 waits out a session failure.
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	// AGENTS.md §11.1: a namespace is a hard partition. This suite starts
	// Orkers that poll a pool channel and answer the liveness probe, so a
	// parallel agent's run must never be picked up by them — and, just as
	// important, this test asserts EXACTLY three Orkers on its pool, which no
	// shared name could survive.
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	temporalNamespace := fmt.Sprintf("ork-pin-affinity-%d", os.Getpid())
	temporalClient := dialVersioningCluster(t, ctx, temporalNamespace)
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, temporalNamespace)

	pool := "pinhosts-" + token
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("QueueForPool(%q): %v", pool, err)
	}

	// The ork namespace document, in the real S3 registry, so the allow-list
	// the submit path enforces is a document that made a round trip through
	// the bucket — pool_routing_integration_test.go's terms exactly.
	namespaceStore, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		infraAbsent(t, "cannot open the namespace registry: %v", err)
	}
	nsName := "pin-affinity-" + token
	if _, err := namespaceStore.Put(ctx, &orkstackv1.Namespace{
		Name:         nsName,
		Description:  "pin affinity and non-failover acceptance",
		DefaultPool:  pool,
		AllowedPools: []string{task.DefaultPool, pool},
	}, artifacts.PutConditions{}); err != nil {
		infraAbsent(t, "cannot write the namespace document (is MinIO up?): %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := namespaceStore.Delete(cleanupCtx, nsName); err != nil {
			t.Logf("could not remove the test namespace %q: %v", nsName, err)
		}
	})

	// One artifact backend for all three Orkers, as in production: every Orker
	// of a pool writes into the same bucket. It also removes a confound — with
	// per-Orker stores, a task that somehow ran on the wrong host could fail
	// while MATERIALIZING its upstream rather than while reading
	// /orkstack/shared, and the test would report the right answer for the
	// wrong reason. Here the only thing a split pin loses is the shared
	// directory.
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

	transcript := &poolTranscript{}
	defer transcript.writePinAffinity(t)
	transcript.section("deployment")
	transcript.line("temporal namespace   %s", temporalNamespace)
	transcript.line("ork namespace        %s", nsName)
	transcript.line("pool                 %s", pool)
	transcript.line("execution channel    %s", queue)

	// ── three REAL Orkers, all pin hosts, all on the one pool ─────────────
	dep := &pinDeployment{
		pool:      pool,
		queue:     queue,
		namespace: nsName,
		byID:      map[string]*pinOrker{},
		probe:     engine.NewTemporalPoolProbe(temporalClient),
	}
	for i := 1; i <= 3; i++ {
		o := newPinOrker(t, ctx, pinOrkerConfig{
			id:                fmt.Sprintf("pin-orker-%d-%s", i, token),
			pool:              pool,
			queue:             queue,
			store:             store,
			commits:           commitStore,
			logs:              logStore,
			refs:              refs,
			temporalNamespace: temporalNamespace,
		})
		dep.orkers = append(dep.orkers, o)
		dep.byID[o.id] = o
		transcript.line("orker %d              %s  work_root=%s", i, o.id, o.workRoot)
	}

	// ── the engine, wired the way cmd/ork wires it ────────────────────────
	bus, runs := engine.NewEventBus(), engine.NewRunStore()
	snapshots := newTestSnapshotStore()

	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
		Identity:                    "pin-affinity-engine-" + token,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
		Identity:                    "pin-affinity-events-" + token,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	for name, w := range map[string]worker.Worker{"run": runWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	orkflows := engine.NewMemoryOrkflowStore()
	dep.orkflows = orkflows
	starter := engine.NewTemporalStarter(temporalClient, task.DefaultQueue)
	svc := api.NewService(api.Config{
		Orkflows:      orkflows,
		Namespaces:    namespaceStore,
		Reader:        engine.NewTemporalRunReader(temporalClient).WithSnapshotStore(snapshots),
		Runs:          runs,
		Bus:           bus,
		Starter:       starter,
		Pools:         engine.NewTemporalPoolProbe(temporalClient),
		Fleet:         starter,
		Orkers:        starter,
		ArtifactStore: store,
		Logs:          logStore,
		// Artifacts and Limits are what pin CommitRoot and the workspace
		// limits into RunPlan, so the real commit protocol engages on every
		// run below rather than being disabled by an empty commit_uri.
		Artifacts: cfg,
		Limits:    engine.DefaultLimits(),
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
	defer func() {
		transport.CloseIdleConnections()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	dep.runs = orkstackv1connect.NewRunServiceClient(
		&http.Client{Transport: transport}, "http://"+listener.Addr().String())

	dep.image = pinnedAlpineForCommitE2E(t)
	transcript.line("image                %s", dep.image)

	// ── fairness precondition: all three are polling BEFORE the first submit ──
	//
	// This is what makes the distribution observed in Proof 1 an honest
	// observation rather than an artifact of the harness. A worker that
	// started late would miss the early runs; a worker that started first
	// could drain a backlog. Both channels are waited for:
	//
	//   - the pool's ordinary channel, which is where ExecuteTask is
	//     delivered and what the liveness probe and ListPools report;
	//   - <queue>__internal_session_creation, which is where the SDK schedules
	//     a session's creation activity and therefore the channel on which
	//     Temporal actually PICKS a pin's host.
	//
	// Exactly three, not at least three: the pool name is this execution's
	// own, so any other number is a count of something that is not serving it.
	// Each Orker's worker carries its own Identity, because the probe counts
	// one poller per identity and three workers in one process would otherwise
	// share the SDK's default <pid>@<host> and be reported as one Orker.
	settled := awaitPoolServedBy(t, ctx, dep.probe, pool, queue, 3)
	transcript.line("cluster reports 3 Orkers on %s after %s", pool, settled.Round(time.Millisecond))
	creationQueue := queue + "__internal_session_creation"
	settled = awaitPoolServedBy(t, ctx, dep.probe, pool+" (session creation)", creationQueue, 3)
	transcript.line("cluster reports 3 session-creation pollers on %s after %s",
		creationQueue, settled.Round(time.Millisecond))

	// ═══ PROOF 1: affinity under real contention ══════════════════════════
	roundTripName := "pin-roundtrip-" + token
	if err := dep.storeOrkflow(ctx, roundTripName, pinRoundTripYAML); err != nil {
		t.Fatalf("store the round-trip orkflow: %v", err)
	}

	transcript.section("1. affinity: every task of a pin on ONE Orker, over " +
		fmt.Sprint(pinAffinityRuns) + " runs")
	wins := map[string]int{}
	for i := 1; i <= pinAffinityRuns; i++ {
		runID, winner := dep.runPinnedRoundTrip(t, ctx, roundTripName, fmt.Sprintf("run %d/%d", i, pinAffinityRuns))
		wins[winner]++
		transcript.line("run %2d/%d  %s  ->  %s", i, pinAffinityRuns, runID, winner)
	}
	transcript.line("")
	for _, o := range dep.orkers {
		transcript.line("wins  %-32s %d", o.id, wins[o.id])
	}
	t.Logf("EVIDENCE pin affinity: %d/%d runs kept both members on one Orker; wins=%v",
		pinAffinityRuns, pinAffinityRuns, wins)
	// Softly: how Temporal spread the wins is reported, never required. The
	// crux is the WITHIN-run consistency asserted per run above; Temporal
	// makes no promise about spreading session-creation activities across
	// idle pollers, and demanding one here would be asserting a property of
	// the cluster's dispatcher rather than of this feature.
	if len(wins) == 1 {
		t.Logf("NOTE all %d runs were won by one Orker. Every Orker was polling both the "+
			"pool channel and the session-creation channel before the first submit "+
			"(asserted above), so this is Temporal's own dispatch behaviour on a short "+
			"sequential run, not a harness bias.", pinAffinityRuns)
	}

	// ═══ PROOF 2: no failover when the pin host dies mid-pin ══════════════
	transcript.section("2. no failover: the pin host is killed mid-pin")
	holdName := "pin-hostloss-" + token
	if err := dep.storeOrkflow(ctx, holdName, pinHoldYAML); err != nil {
		t.Fatalf("store the host-loss orkflow: %v", err)
	}

	submitted, err := dep.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: holdName, Namespace: nsName,
	}))
	if err != nil {
		t.Fatalf("SubmitRun(%s): %v", holdName, err)
	}
	lostRunID := submitted.Msg.GetRun().GetId()
	transcript.line("run                  %s", lostRunID)

	// Learn the winner while the pin is STILL IN FLIGHT. Polling with a
	// deadline against the Orkers' own records, never a sleep: pinOrker.execute
	// records the task the instant the activity is entered, so this returns as
	// soon as Temporal has chosen the host.
	victim := dep.awaitPinDispatch(t, ctx, lostRunID, "hold")
	transcript.line("pin opened on        %s", victim.id)

	// Then wait for that host to be GENUINELY MID-PIN before killing it: not
	// merely handed the task, but into the dispatcher's "run" phase, which is
	// the phase whose start means the container is up and executing its
	// payload (oci_dispatcher.go's §7.1 sequence). Killing at dispatch instead
	// would kill during workspace setup or an image pull, and "the task that
	// was running when the Orker died" would not be true of anything.
	dep.awaitPinPhase(t, ctx, victim, lostRunID, "hold", "run")
	transcript.line("container running on %s (phase \"run\" observed)", victim.id)
	t.Logf("EVIDENCE the pin's host is %s and its container is running; killing the host",
		victim.id)

	// The kill: a host DEATH, not a graceful drain — see pinOrker.kill for why
	// the difference decides whether this proof measures anything. The
	// container the victim started keeps running, exactly as a real Orker
	// crash leaves it (§14.17); it is reaped further down.
	victim.kill()
	killedAt := time.Now()
	survivors := make([]*pinOrker, 0, 2)
	for _, o := range dep.orkers {
		if o.id != victim.id {
			survivors = append(survivors, o)
		}
	}
	transcript.line("survivors            %s, %s", survivors[0].id, survivors[1].id)

	lost := dep.awaitTerminalRun(t, ctx, lostRunID)
	transcript.line("terminal after       %s", time.Since(killedAt).Round(time.Millisecond))
	transcript.line("run status           %v", lost.GetStatus())

	if lost.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run %s ended %v, want FAILED: a pin whose host died must fail the run, "+
			"never succeed and never hang (run error: %v)",
			lostRunID, lost.GetStatus(), lost.GetError())
	}

	held := pinTaskRun(t, lost, "hold")
	transcript.line("task hold            status=%v code=%s category=%v orker=%s",
		held.GetStatus(), held.GetError().GetCode(), held.GetError().GetCategory(), held.GetOrkerId())
	// "Which Orker died?" read off the run record, recorded rather than
	// asserted. `hold` is this pin's FIRST member and `after` needs it, so
	// nothing else in the pin ever reached an Orker — there is no sibling to
	// backfill orker_id from, and it stays empty exactly as the field promises.
	// What is left is the labelled best-effort lead on error.details, and this
	// is the one place a REAL dead host produces it (workflow.go's
	// attributeLostPinHost). The orker= field above becomes non-empty for any
	// pin whose shape gives the lost member a sibling that already reported.
	transcript.line("task hold            details=%s", held.GetError().GetDetails())
	// The exact classification internal/engine/workflow.go's pinFailureError
	// produces, asserted end to end for the first time: the unit tests prove
	// the branch, this proves a real dead host reaches it.
	if code := held.GetError().GetCode(); code != "PIN_HOST_LOST" {
		if code == "PIN_TIMEOUT" {
			t.Fatalf("task hold failed with PIN_TIMEOUT, not PIN_HOST_LOST. The pin's "+
				"timeout_seconds is %d and the kill landed %s after dispatch, so this is a "+
				"real misclassification rather than a timeout race: %s",
				pinAffinityPinTimeoutSeconds, time.Since(killedAt).Round(time.Second),
				held.GetError().GetMessage())
		}
		t.Errorf("task hold failed with code %q, want PIN_HOST_LOST (message: %s)",
			code, held.GetError().GetMessage())
	}
	if got := held.GetError().GetCategory(); got != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Errorf("task hold's failure category = %v, want INFRASTRUCTURE_ERROR: losing a host "+
			"is the platform's fault and must never be recorded as the workload's", got)
	}
	if held.GetError().GetRetryable() {
		t.Error("task hold's failure is marked retryable; a pin never moves, so a retry " +
			"could only run somewhere /orkstack/shared does not exist")
	}
	if got := held.GetPin(); got != "share" {
		t.Errorf("task hold's TaskRun.pin = %q, want %q", got, "share")
	}
	if !strings.Contains(held.GetError().GetMessage(), "pin never moves") {
		t.Errorf("task hold's message does not explain that a pin never moves: %q",
			held.GetError().GetMessage())
	}

	// The downstream member never ran at all, anywhere.
	after := pinTaskRun(t, lost, "after")
	transcript.line("task after           status=%v", after.GetStatus())
	if after.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("task after SUCCEEDED after its pin's host died; it can only have run "+
			"somewhere without the shared directory (orker %q)", after.GetOrkerId())
	}

	// ── THE load-bearing non-failover assertion ───────────────────────────
	//
	// "The run failed" does not prove non-failover on its own: a bug that
	// re-opened the pin on a survivor AND ALSO failed the run for an unrelated
	// reason would produce exactly the state asserted above. This is the check
	// that separates them, and it reads the survivors' own records of what they
	// were handed — not the engine's account of what it sent.
	for _, o := range survivors {
		if ran := o.ranFor(lostRunID); len(ran) != 0 {
			t.Errorf("survivor %s was handed %v of run %s. The pin FAILED OVER: its tasks "+
				"reached an Orker that does not have the dead host's /orkstack/shared, "+
				"which is the one thing task pinning promises cannot happen",
				o.id, ran, lostRunID)
		}
		transcript.line("survivor %-32s ran %v of the lost run", o.id, o.ranFor(lostRunID))
	}
	// ── the same conclusion, read off the three hosts' DISKS ──────────────
	//
	// Independent of both the engine's account and the Orkers' own records:
	// /orkstack/shared is a real directory under exactly one Orker's work
	// root, and a pin that had failed over would have had to create a second
	// one somewhere else. The victim's copy is still there because a dead host
	// runs neither the release activity nor a boot reap, which is precisely
	// why it can be inspected here.
	for _, o := range dep.orkers {
		dirs := pinDirsFor(t, o.workRoot, lostRunID)
		transcript.line("pin dirs on %-32s %v", o.id, dirs)
		if o.id == victim.id {
			if len(dirs) != 1 {
				t.Errorf("the pin's host %s has %d shared directories for run %s (%v), want "+
					"exactly 1", o.id, len(dirs), lostRunID, dirs)
			}
			continue
		}
		if len(dirs) != 0 {
			t.Errorf("survivor %s has a shared directory for run %s on disk (%v). The pin "+
				"was opened on a second host, which is the failover task pinning promises "+
				"cannot happen", o.id, lostRunID, dirs)
		}
	}

	// And the victim really was the only Orker that ever saw it.
	if !victim.sawTask(lostRunID, "hold") {
		t.Errorf("the identified host %s has no record of running hold; the kill was aimed "+
			"at the wrong Orker and everything after it is meaningless", victim.id)
	}
	if victim.sawTask(lostRunID, "after") {
		t.Errorf("the dead host %s was handed `after` as well; it was killed before that "+
			"task should have been schedulable", victim.id)
	}

	// Best-effort: the killed Orker's container outlives its stopped worker
	// (an Orker crash does not stop its containers, §14.17). Remove it now so
	// the host is not left with a `sleep` running for the rest of the window.
	reapCtx, reapCancel := context.WithTimeout(context.Background(), 60*time.Second)
	if _, err := victim.activities.Dispatcher().Runtime.Reap(reapCtx, victim.id); err != nil {
		t.Logf("could not reap the killed Orker's containers: %v", err)
	}
	reapCancel()

	// ── the pool is still healthy; it was that ONE pin that was lost ──────
	transcript.section("3. the pool survives: a fresh pin lands on a survivor")
	freshID, freshWinner := dep.runPinnedRoundTrip(t, ctx, roundTripName, "post-kill run")
	transcript.line("run                  %s  ->  %s", freshID, freshWinner)
	if freshWinner == victim.id {
		t.Errorf("the fresh run landed on the killed Orker %s, whose worker is stopped", victim.id)
	}
	if dep.byID[freshWinner] == nil {
		t.Errorf("the fresh run reports orker %q, which is not one of this deployment's three",
			freshWinner)
	}
	t.Logf("EVIDENCE after the kill a fresh pinned run succeeded on survivor %s", freshWinner)
}

// ── the orkflows under test ───────────────────────────────────────────────

// pinAffinityPinTimeoutSeconds is the pin's whole lifetime in both orkflows
// below. It is deliberately generous relative to the workloads (a token write,
// a token read, one pinAffinityHoldSeconds sleep) so that Proof 2's kill is
// unambiguously PIN_HOST_LOST and can never be a PIN_TIMEOUT race: workflow.go's
// pinFailureError tells the two apart by whether the pin has been open for at
// least this long.
const pinAffinityPinTimeoutSeconds = 300

// pinRoundTripYAML is Proof 1's orkflow: one pin, two members, a random token
// passed from the first container to the second through /orkstack/shared.
//
// The token is what makes the affinity assertion mean something. Two TaskRuns
// reporting the same orker_id is two strings matching, and a broken assertion
// would report that too. A token the SECOND container read out of a file the
// FIRST container wrote can only be explained by one filesystem, and therefore
// by one host. It is generated in the container, from /dev/urandom, so it is
// not a value the test could have arranged.
//
// Both members are run_as_orker_user, which is what docs/DSL.md recommends
// across a pin and what keeps the sticky-bit uid clash out of the
// measurement — the same terms internal/orker's own pin proof is on.
//
// Placeholders are substituted rather than fmt.Sprintf'd: the payloads contain
// printf format strings of their own, and %%s in a Go format string is a
// reliable way to write a test that lies about what the container ran.
const pinRoundTripYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: __NAME__
version: 0.1.0
pins:
  share:
    timeout_seconds: __PIN_TIMEOUT__
tasks:
  - id: writer
    uses: __IMAGE__
    pool: __POOL__
    pin: share
    run_as_orker_user: true
    network: none
    timeout_seconds: 120
    command:
      - /bin/sh
      - -c
      - |
        set -e
        mkdir -p /orkstack/shared/src
        TOKEN=$(head -c 8 /dev/urandom | od -An -tx1 | tr -dc 'a-f0-9')
        test -n "$TOKEN"
        printf '%s' "$TOKEN" > /orkstack/shared/src/token.txt
        printf '{"token":"%s","host":"%s"}' "$TOKEN" "$(hostname)" > /orkstack/output/values.json
  - id: reader
    uses: __IMAGE__
    pool: __POOL__
    pin: share
    needs: [writer]
    run_as_orker_user: true
    network: none
    timeout_seconds: 120
    command:
      - /bin/sh
      - -c
      - |
        set -e
        test -f /orkstack/shared/src/token.txt
        SEEN=$(cat /orkstack/shared/src/token.txt)
        test -n "$SEEN"
        mkdir -p /orkstack/output/artifacts
        printf '%s' "$SEEN" > /orkstack/output/artifacts/token.txt
        printf '{"seen":"%s"}' "$SEEN" > /orkstack/output/values.json
`

// pinHoldYAML is Proof 2's orkflow: the first member announces itself, writes
// its token, and then stays alive long enough for the test to kill its host
// while it is genuinely mid-pin.
//
// The sleep is the container's OWN payload, not a synchronization sleep: the
// test does not wait out the window, it polls for the winner's identity and
// kills as soon as it knows (awaitPinDispatch).
const pinHoldYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: __NAME__
version: 0.1.0
pins:
  share:
    timeout_seconds: __PIN_TIMEOUT__
tasks:
  - id: hold
    uses: __IMAGE__
    pool: __POOL__
    pin: share
    run_as_orker_user: true
    network: none
    timeout_seconds: 240
    command:
      - /bin/sh
      - -c
      - |
        set -e
        mkdir -p /orkstack/shared/src
        printf 'held' > /orkstack/shared/src/token.txt
        printf '{"held":true}' > /orkstack/output/values.json
        sleep __HOLD__
  - id: after
    uses: __IMAGE__
    pool: __POOL__
    pin: share
    needs: [hold]
    run_as_orker_user: true
    network: none
    timeout_seconds: 120
    command:
      - /bin/sh
      - -c
      - |
        set -e
        test -f /orkstack/shared/src/token.txt
        printf '{"seen":"%s"}' "$(cat /orkstack/shared/src/token.txt)" > /orkstack/output/values.json
`

// storeOrkflow compiles one of the templates above and puts it in the store.
//
// It compiles through the REAL dsl.Compile, so `pins:`/`pin:` are validated by
// the production compiler (a member naming an undeclared pin, or a pin spanning
// two pools, is refused there and not here). Only Task.image is filled in
// afterwards: resolving `uses:` to a digest is apply-time registry work, and
// durable_commit_e2e_integration_test.go writes the same field the same way for
// the same reason. The dispatcher reads image.pinned and nothing else.
func (d *pinDeployment) storeOrkflow(ctx context.Context, name, template string) error {
	source := strings.NewReplacer(
		"__NAME__", name,
		"__IMAGE__", d.image,
		"__POOL__", d.pool,
		"__PIN_TIMEOUT__", fmt.Sprint(pinAffinityPinTimeoutSeconds),
		"__HOLD__", fmt.Sprint(pinAffinityHoldSeconds),
	).Replace(template)

	wf, err := dsl.Compile([]byte(source))
	if err != nil {
		return fmt.Errorf("compile %s: %w", name, err)
	}
	wf.Namespace = d.namespace
	for _, t := range wf.GetTasks() {
		t.Image = &orkstackv1.ImageRef{Reference: d.image, Pinned: d.image}
	}
	if _, _, err := d.orkflows.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
		return fmt.Errorf("put %s: %w", name, err)
	}
	return nil
}

// runPinnedRoundTrip submits one pinned run, waits for it to end, and asserts
// the whole affinity promise for that ONE run. It returns the run id and the
// winning Orker's id.
//
// Four assertions, and each one covers a way the other three could pass while
// the feature was broken:
//
//  1. the run SUCCEEDED — a split pin's reader cannot find the token and fails,
//     so a green run is itself part of the proof;
//  2. every member's TaskRun reports the SAME non-empty orker_id, and it is one
//     of this deployment's three — the engine's account;
//  3. the same one Orker's own records show it was handed BOTH members, and no
//     other Orker was handed either — the Orkers' account, which is what
//     catches an engine that reports one host while dispatching to two;
//  4. the reader's values carry the writer's random token — the filesystem's
//     account, which is the only one that proves "one host" meant "one
//     /orkstack/shared".
func (d *pinDeployment) runPinnedRoundTrip(
	t *testing.T,
	ctx context.Context,
	orkflowName string,
	label string,
) (string, string) {
	t.Helper()

	submitted, err := d.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: orkflowName, Namespace: d.namespace,
	}))
	if err != nil {
		t.Fatalf("%s: SubmitRun(%s): %v", label, orkflowName, err)
	}
	runID := submitted.Msg.GetRun().GetId()

	run := d.awaitTerminalRun(t, ctx, runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("%s: run %s ended %v, want SUCCEEDED. A pin whose members were split "+
			"across two Orkers fails exactly here, because the reader cannot find the "+
			"token the writer put in /orkstack/shared (error: %v)",
			label, runID, run.GetStatus(), run.GetError())
	}

	// (2) the engine's account: one orker_id across the pin.
	var winner string
	for _, member := range []string{"writer", "reader"} {
		tr := pinTaskRun(t, run, member)
		if tr.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("%s: run %s task %s ended %v (%v)",
				label, runID, member, tr.GetStatus(), tr.GetError())
		}
		if tr.GetOrkerId() == "" {
			t.Fatalf("%s: run %s task %s reports no orker_id; affinity cannot be checked "+
				"and a client cannot tell which host ran the pin", label, runID, member)
		}
		if tr.GetPin() != "share" {
			t.Errorf("%s: run %s task %s reports pin %q, want %q",
				label, runID, member, tr.GetPin(), "share")
		}
		if winner == "" {
			winner = tr.GetOrkerId()
			continue
		}
		if tr.GetOrkerId() != winner {
			t.Fatalf("%s: run %s SPLIT ITS PIN: writer ran on %q and %s on %q. Every task "+
				"of a pin must execute on one Orker process — they share one host's "+
				"/orkstack/shared and nothing else could make them correct",
				label, runID, winner, member, tr.GetOrkerId())
		}
	}
	host := d.byID[winner]
	if host == nil {
		t.Fatalf("%s: run %s reports orker_id %q, which is not one of this deployment's "+
			"three (%v) — something outside this test served the pool",
			label, runID, winner, d.orkerIDs())
	}

	// (3) the Orkers' own account, independently of the engine's.
	for _, o := range d.orkers {
		ran := o.ranFor(runID)
		if o.id == winner {
			if len(ran) != 2 || !o.sawTask(runID, "writer") || !o.sawTask(runID, "reader") {
				t.Errorf("%s: run %s: the reported host %s records %v, want both members",
					label, runID, o.id, ran)
			}
			continue
		}
		if len(ran) != 0 {
			t.Errorf("%s: run %s: orker %s was handed %v, but the run reports every member "+
				"ran on %s. The engine's orker_id and the Orkers' own records disagree, so "+
				"one of them is not measuring what it claims",
				label, runID, o.id, ran, winner)
		}
	}

	// (4) the filesystem's account: the random token made the round trip.
	written := host.resultFor(runID, "writer")
	read := host.resultFor(runID, "reader")
	if written == nil || read == nil {
		t.Fatalf("%s: run %s: the host %s has no recorded result for both members "+
			"(writer=%v reader=%v)", label, runID, host.id, written != nil, read != nil)
	}
	token := pinJSONField(t, string(written.GetValuesJson()), "token")
	seen := pinJSONField(t, string(read.GetValuesJson()), "seen")
	if token == "" {
		t.Fatalf("%s: run %s: the writer reported no token (values=%s); the round trip "+
			"cannot be checked", label, runID, written.GetValuesJson())
	}
	if seen != token {
		t.Fatalf("%s: run %s: the reader saw %q in /orkstack/shared but the writer wrote "+
			"%q. The two members reported the same orker_id yet did NOT share a "+
			"filesystem, so the affinity is nominal only",
			label, runID, seen, token)
	}
	return runID, winner
}

// awaitTerminalRun drains the run's event stream — which closes when the run
// reaches a terminal state — and then reads the run back.
//
// Draining the stream rather than polling for a status is what the other
// suites in this package do, and it is not a sleep: the server closes it.
func (d *pinDeployment) awaitTerminalRun(
	t *testing.T,
	ctx context.Context,
	runID string,
) *orkstackv1.Run {
	t.Helper()
	stream, err := d.runs.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId: runID, Namespace: d.namespace,
	}))
	if err != nil {
		t.Fatalf("StreamEvents(%s): %v", runID, err)
	}
	for stream.Receive() {
	}
	streamErr := stream.Err()
	stream.Close()
	if streamErr != nil {
		t.Fatalf("run %s never terminated: %v. Handed out so far: %s",
			runID, streamErr, d.dispatchSummary(runID))
	}

	got, err := d.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
		RunId: runID, Namespace: d.namespace,
	}))
	if err != nil {
		t.Fatalf("GetRun(%s): %v", runID, err)
	}
	return got.Msg.GetRun()
}

// awaitPinDispatch blocks until some Orker has been HANDED one task of one
// run, and answers which.
//
// Poll with a deadline, never a sleep (AGENTS.md §11.1): pinOrker.execute
// records the task before it calls the real dispatcher, so this returns as
// soon as Temporal has chosen the host — while the container is still starting
// and the pin is genuinely in flight. That is the whole synchronization Proof 2
// needs, and it is why the kill lands mid-pin rather than after the fact.
func (d *pinDeployment) awaitPinDispatch(
	t *testing.T,
	ctx context.Context,
	runID, taskID string,
) *pinOrker {
	t.Helper()
	// Bounded by the pin's own creation budget: until some host answers the
	// session's creation activity, no member can be dispatched at all.
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		for _, o := range d.orkers {
			if o.sawTask(runID, taskID) {
				return o
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for %s/%s to be dispatched: %v",
				runID, taskID, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("no Orker was handed %s/%s within the deadline; handed out so far: %s",
		runID, taskID, d.dispatchSummary(runID))
	return nil
}

// awaitPinPhase blocks until one Orker's dispatcher has taken one task into a
// given §7.1 phase, through the real Options.Phases seam.
//
// Poll with a deadline, never a sleep. The budget is generous because the
// phases before "run" include a real image pull on a cold host.
func (d *pinDeployment) awaitPinPhase(
	t *testing.T,
	ctx context.Context,
	o *pinOrker,
	runID, taskID, phase string,
) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		if o.sawPhase(runID, taskID, phase) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for %s to take %s/%s into phase %q: %v",
				o.id, runID, taskID, phase, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	o.mu.Lock()
	seen := append([]string(nil), o.phases[runID+"/"+taskID]...)
	o.mu.Unlock()
	t.Fatalf("%s never took %s/%s into phase %q; phases seen: %v",
		o.id, runID, taskID, phase, seen)
}

func (d *pinDeployment) dispatchSummary(runID string) string {
	parts := make([]string, 0, len(d.orkers))
	for _, o := range d.orkers {
		parts = append(parts, fmt.Sprintf("%s=%v", o.id, o.ranFor(runID)))
	}
	return strings.Join(parts, " ")
}

func (d *pinDeployment) orkerIDs() []string {
	out := make([]string, 0, len(d.orkers))
	for _, o := range d.orkers {
		out = append(out, o.id)
	}
	return out
}

// ── building one real Orker ───────────────────────────────────────────────

type pinOrkerConfig struct {
	id                string
	pool              string
	queue             string
	store             artifacts.Store
	commits           artifacts.CommitStore
	logs              artifacts.LogStore
	refs              artifacts.RefPredictor
	temporalNamespace string

	// The cordon suite's knobs; the zero value is what the pin suites use.
	//
	// maxPins overrides the pin cap (0 means 2). workRoot reuses a work root,
	// which is what a restart of the same Orker does. logger receives the
	// Orker's lifecycle lines (nil: slog.Default). buildID and deployment make
	// the task worker versioned, as ORK_BUILD_ID does for cmd/orker.
	// maxConcurrent overrides ORK_ORKER_MAX_CONCURRENT (0 keeps the default),
	// and hostsNoPins is ORK_ORKER_MAX_PINS=0: no session worker at all.
	maxPins       int32
	workRoot      string
	logger        *slog.Logger
	buildID       string
	deployment    task.Deployment
	maxConcurrent int32
	hostsNoPins   bool
}

// newPinOrker builds and starts one real pin host, the way cmd/orker builds
// and starts one.
//
// Everything cmd/orker's registration block does is mirrored: the three
// activity names (task.ActivityExecuteTask, task.ActivityDescribeOrker,
// task.ActivityReleasePin) on one activity-only worker, with
// EnableSessionWorker on — which is what makes this process a pin host at all —
// and MaxConcurrentSessionExecutionSize matching Options.MaxPins, so an Orker
// cannot advertise a capacity its worker does not poll for.
func newPinOrker(t *testing.T, ctx context.Context, cfg pinOrkerConfig) *pinOrker {
	t.Helper()

	// A work root the container daemon can bind-mount, and a DISTINCT one per
	// Orker: two Orkers cannot share per-attempt execution directories (§6.4),
	// and a shared root is exactly what the boot identity lock refuses. It
	// lives under the repo's own .data/work because on macOS a VM-hosted
	// daemon does not share /var/folders, which is what t.TempDir() would give
	// — the caveat orker.WorkRootVar's doc comment describes, and the reason
	// orphan_reap_integration_test.go does the same thing.
	workRoot := cfg.workRoot
	if workRoot == "" {
		root := pinAffinityWorkRootParent(t)
		var err error
		workRoot, err = os.MkdirTemp(root, proof.WorkRootPattern("pin-affinity"))
		if err != nil {
			t.Fatalf("create a work root under %s: %v", root, err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(workRoot) })
	}

	o := &pinOrker{
		id:       cfg.id,
		pool:     cfg.pool,
		queue:    cfg.queue,
		workRoot: workRoot,
		ran:      map[string][]string{},
		phases:   map[string][]string{},
		results:  map[string]*orkstackv1.ExecuteTaskResult{},
	}

	// This Orker's OWN Temporal client. Three Orkers are three processes, so
	// three connections; and Proof 2's kill closes exactly one of them.
	orkerClient, err := client.DialContext(ctx, client.Options{
		HostPort:  temporalAddress(),
		Namespace: cfg.temporalNamespace,
		Identity:  cfg.id,
	})
	if err != nil {
		infraAbsent(t, "orker %s cannot dial temporal: %v", cfg.id, err)
	}
	o.temporal = orkerClient
	t.Cleanup(o.close)

	maxPins := cfg.maxPins
	if maxPins == 0 {
		maxPins = 2
	}
	if cfg.hostsNoPins {
		maxPins = 0
	}
	// The Orker's heartbeat tick, deliberately FASTER than the SDK's session
	// heartbeat, and that ordering is load-bearing for Proof 2.
	//
	// Both dead-host detectors are the same task.HeartbeatTimeout (30 s) from
	// each activity's LAST heartbeat, so which one fires first is decided by
	// which heartbeated more recently before the host died. The SDK's creation
	// activity ticks at min(HeartbeatTimeout/3, 10s) = 10 s
	// (go.temporal.io/sdk internal/session.go); a member ticking at 2 s is
	// therefore always the more recently heartbeated of the two, so the
	// SESSION's detector is the one that expires first and the member's
	// cancellation is a consequence of the pin being lost rather than a race
	// against it. It is an ordinary operator setting
	// (ORK_ORKER_HEARTBEAT_INTERVAL, well inside orker.MinHeartbeatInterval),
	// not a test-only hook.
	const tick = 2 * time.Second
	activities := orker.NewActivitiesWithOptions(cfg.store, orker.Options{
		Logs:     cfg.logs,
		Commits:  cfg.commits,
		Refs:     cfg.refs,
		Pool:     cfg.pool,
		OrkerID:  cfg.id,
		WorkRoot: workRoot,
		// Above zero is what makes this Orker a pin host, and it is what
		// DescribeOrker reports as max_concurrent_pins — the number the real
		// submit-time PoolNoPinHostError check reads.
		MaxPins:           maxPins,
		HeartbeatInterval: tick,
		// The real Options.Phases seam, so Proof 2 can wait for the container
		// to actually be running before it kills the host.
		Phases:        pinPhaseObserver{o: o},
		Logger:        cfg.logger,
		BuildID:       cfg.buildID,
		MaxConcurrent: cfg.maxConcurrent,
	})
	if err := activities.Boot(ctx); err != nil {
		infraAbsent(t, "orker %s boot checks failed: %v", cfg.id, err)
	}
	t.Cleanup(activities.Close)
	o.activities = activities

	tuner, err := activities.WorkerTuner()
	if err != nil {
		t.Fatalf("orker %s tuner: %v", cfg.id, err)
	}
	o.worker = worker.New(orkerClient, cfg.queue, cfg.deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
		// The heartbeat pairing cmd/orker's workerOptions describes: the SDK's
		// batch window capped to the same tick the dispatcher runs at, or a
		// cancellation waits out the SDK's 24s default.
		MaxHeartbeatThrottleInterval: tick,
		// cmd/orker's explicit poller count (orker.TaskPollers).
		MaxConcurrentActivityTaskPollers: orker.TaskPollers,
		// The pin host's two extra pollers: the session creation worker on
		// <queue>__internal_session_creation and the session activity worker on
		// <uuid>@<hostname>. Without this the process is not a pin host and no
		// session could ever be created on it.
		EnableSessionWorker:               maxPins > 0,
		MaxConcurrentSessionExecutionSize: int(maxPins),
		// MANDATORY here, and not a detail: engine.livePollers counts one
		// poller per Temporal worker identity, and the SDK's default identity
		// is <pid>@<hostname>. Three workers in ONE test process would share
		// it and the cluster would report this three-Orker pool as one Orker —
		// which would silently gut the fairness precondition Proof 1 rests on.
		// Three real Orkers are three processes with three identities; these
		// are theirs.
		Identity: cfg.id,
		// The cordon's two seams, exactly as cmd/orker's taskWorkerOptions
		// sets them: the Tuner whose activity slot supplier refuses to
		// reserve while cordoned, and the interceptor that counts and refuses
		// new pins (docs/DEPLOY.md "Removing an Orker safely: cordon and drain").
		Tuner:        tuner,
		Interceptors: []interceptor.WorkerInterceptor{activities.WorkerInterceptor()},
	}))
	o.worker.RegisterActivityWithOptions(o.execute,
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
	o.worker.RegisterActivityWithOptions(activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	o.worker.RegisterActivityWithOptions(activities.ReleasePin,
		activity.RegisterOptions{Name: task.ActivityReleasePin})

	o.control, o.presence, err = orker.NewChannelWorkers(orkerClient, activities)
	if err != nil {
		t.Fatalf("orker %s channel workers: %v", cfg.id, err)
	}

	if err := o.worker.Start(); err != nil {
		t.Fatalf("start orker %s: %v", cfg.id, err)
	}
	if err := o.control.Start(); err != nil {
		t.Fatalf("start orker %s control worker: %v", cfg.id, err)
	}
	if err := o.presence.Start(); err != nil {
		t.Fatalf("start orker %s presence worker: %v", cfg.id, err)
	}
	t.Cleanup(o.stop)
	// Whatever happens, this Orker's containers do not outlive the test.
	t.Cleanup(func() {
		reapCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := activities.Dispatcher().Runtime.Reap(reapCtx, cfg.id); err != nil {
			t.Logf("cleanup: reap %s's containers: %v", cfg.id, err)
		}
	})
	return o
}

// pinAffinityWorkRootParent is the repo's .data/work, created if absent: the
// directory `make up` points ORK_ORKER_WORKDIR at, and the one a VM-hosted
// daemon on macOS can actually bind-mount.
func pinAffinityWorkRootParent(t *testing.T) string {
	t.Helper()
	if root := os.Getenv(orker.WorkRootVar); root != "" {
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatalf("create work root %s: %v", root, err)
		}
		return root
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".data", "work"))
	if err != nil {
		t.Fatalf("resolve work root: %v", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create work root %s: %v", root, err)
	}
	return root
}

// pinDirsFor lists the pin directories one Orker's work root holds for one
// run, by the layout ReleasePinDirectory addresses them with:
// ork-pin-<run>-<pin>-<instance>.
func pinDirsFor(t *testing.T, workRoot, runID string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(workRoot, "ork-pin-"+runID+"-share-*"))
	if err != nil {
		t.Fatalf("glob pin directories under %s: %v", workRoot, err)
	}
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, filepath.Base(m))
	}
	return names
}

func requirePinAffinityDocker(t *testing.T) {
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

// ── small readers ─────────────────────────────────────────────────────────

// pinTaskRun is one task's TaskRun off a Run, or a fatal error naming what was
// there instead.
func pinTaskRun(t *testing.T, run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	t.Helper()
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == taskID {
			return tr
		}
	}
	present := make([]string, 0, len(run.GetTaskRuns()))
	for _, tr := range run.GetTaskRuns() {
		present = append(present, tr.GetTaskId())
	}
	t.Fatalf("run %s has no TaskRun for %q; it has %v", run.GetId(), taskID, present)
	return nil
}

// pinJSONField reads one string field out of a flat values.json.
//
// Deliberately a substring read rather than a JSON decode: what is being
// checked is that specific bytes a container wrote made it back, and a decoder
// that silently tolerated a differently-shaped document would weaken exactly
// the assertion this feeds.
func pinJSONField(t *testing.T, values, field string) string {
	t.Helper()
	marker := `"` + field + `":"`
	start := strings.Index(values, marker)
	if start < 0 {
		return ""
	}
	rest := values[start+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		t.Fatalf("values %q has an unterminated %q", values, field)
	}
	return rest[:end]
}

// writePinAffinity files this suite's transcript as evidence, reusing
// pool_routing_integration_test.go's transcript type.
func (p *poolTranscript) writePinAffinity(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	header := []string{
		"# three real Orkers, one pool, one cluster: pin affinity and non-failover",
		"# Every Orker below is a real internal/orker.Activities with a real Docker",
		"# runtime and a real MinIO store. Which Orker ran which task came from the",
		"# Orker that was handed it and from the engine's own TaskRun, never from",
		"# this test's expectation; the shared directory was proved by a random",
		"# token one container wrote and another read back.",
		"# recorded " + time.Now().UTC().Format(time.RFC3339),
	}
	proof.Write(t, "engine", "pin-affinity-transcript.txt",
		[]byte(strings.Join(append(header, p.lines...), "\n")+"\n"))
}
