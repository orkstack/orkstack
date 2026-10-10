//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// poolRoutingNamespace isolates this suite's Temporal namespace, per
// AGENTS.md §11.1: it starts Orkers on well-known pool channels
// (ork-pool-alpha and friends), and a parallel agent's run must never be
// picked up by them.
func poolRoutingNamespace() string {
	if ns := os.Getenv("ORK_TEMPORAL_NAMESPACE"); ns != "" {
		return ns
	}
	return fmt.Sprintf("ork-pool-routing-%d", os.Getpid())
}

// The engine under test is wired to the REAL namespace registry
// (artifacts.S3NamespaceStore), and every request below names this suite's own
// namespace, which is unique to the execution.

// poolOrker is one Orker stub: it serves a single pool and records every task
// it was handed, together with the channel Temporal delivered it on. The
// channel comes from Temporal's own activity info, not from the engine, so it
// is evidence rather than an echo.
type poolOrker struct {
	name  string
	pool  string
	queue string
	// identity is what this Orker answers when its pool is probed. The two
	// Orkers answer differently on purpose: a report that came from the
	// engine rather than from the pool would give both the same answer.
	identity *orkstackv1.DescribeOrkerResult

	mu       sync.Mutex
	executed []string
	channels map[string]string
}

func newPoolOrker(t *testing.T, name, pool string) *poolOrker {
	t.Helper()
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("QueueForPool(%q): %v", pool, err)
	}
	return &poolOrker{
		name: name, pool: pool, queue: queue,
		identity: &orkstackv1.DescribeOrkerResult{
			OrkerId:     name,
			Pool:        pool,
			RuntimeName: "docker",
		},
		channels: map[string]string{},
	}
}

// describeOrker answers the engine's pool-liveness probe, as a real Orker does
// (cmd/orker registers task.ActivityDescribeOrker on its own channel).
func (o *poolOrker) describeOrker(context.Context) (*orkstackv1.DescribeOrkerResult, error) {
	return o.identity, nil
}

func (o *poolOrker) execute(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	o.mu.Lock()
	o.executed = append(o.executed, in.TaskId)
	o.channels[in.TaskId] = activity.GetInfo(ctx).TaskQueue
	o.mu.Unlock()
	return &orkstackv1.ExecuteTaskResult{
		TaskId:     in.TaskId,
		ValuesJson: valuesJSON(map[string]any{"ran_on_orker": o.name, "pool": in.Pool}),
	}, nil
}

func (o *poolOrker) ran() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := append([]string(nil), o.executed...)
	sort.Strings(out)
	return out
}

func (o *poolOrker) channelFor(taskID string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.channels[taskID]
}

// TestIntegration_TwoPoolsRouteExecuteAndRejectIndependently is this
// milestone's acceptance, against a real cluster, with two Orkers on two
// different pools.
//
// Three properties, one deployment:
//
//  1. a task pinned to one pool runs on that pool's Orker and never on the
//     other's, and a sibling task pinned to the other runs there and not here
//     — in the SAME run, so the routing is per task and not per engine;
//  2. a task naming a pool the namespace forbids is refused at SubmitRun, with
//     an error naming the namespace, the task and the pool;
//  3. a submit whose pool has no live Orkers is refused at SubmitRun, and no
//     run is created for it — the failure AGENTS.md §11.1 describes, converted
//     from a run stranded in RUNNING into an error a client can act on.
//
// Each is what a routing bug turns into: (1) both tasks on one channel, (2) a
// forbidden pool accepted and run, (3) a submit hung until the stream deadline.
func TestIntegration_TwoPoolsRouteExecuteAndRejectIndependently(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	// The budget outlasts the pool-liveness waits below, which are bounded by
	// DefaultPollerFreshness rather than by anything this test does.
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	namespace := poolRoutingNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)
	defer temporalClient.Close()

	// The namespace registry is the real S3 store against MinIO: the
	// allow-list this test enforces is a document that made a round trip
	// through the bucket.
	storeConfig := artifacts.ConfigFromEnv()
	storeConfig.Backend = artifacts.BackendS3
	namespaceStore, err := artifacts.NewS3NamespaceStore(ctx, storeConfig)
	if err != nil {
		infraAbsent(t, "cannot open the namespace registry: %v", err)
	}
	nsName := fmt.Sprintf("pools-%d-%d", os.Getpid(), time.Now().UnixNano())
	// Pools and orkflow names of this run's own. A pool name is a channel name
	// (task.QueueForPool) and Temporal remembers a channel's pollers for
	// minutes after their worker stopped, so a fixed "alpha" would be
	// described by every earlier run of this suite in the same namespace as
	// well as by this one — and the count this test exists to check would be a
	// count of ghosts.
	//
	// The orkflow names carry the same suffix for the same reason, one layer
	// up: `ListRuns(orkflow_name=...)` is answered from the whole Temporal
	// namespace, so a fixed "cold-job" would let a run some EARLIER execution
	// started answer the assertion that THIS execution's refused submit
	// created nothing. That is not a hypothesis: a run left in a shared
	// namespace by an earlier execution failed this assertion while the
	// submit under test had been correctly refused. Scoped, the question is
	// about this run and nothing else.
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	poolAlpha := "alpha-" + token
	poolBravo := "bravo-" + token
	poolCold := "cold-" + token
	twoPoolName := "two-pool-job-" + token
	coldName := "cold-job-" + token
	forbiddenName := "forbidden-job-" + token
	registered := &orkstackv1.Namespace{
		Name:         nsName,
		Description:  "pool routing acceptance",
		DefaultPool:  task.DefaultPool,
		AllowedPools: []string{task.DefaultPool, poolAlpha, poolBravo, poolCold},
	}
	if _, err := namespaceStore.Put(ctx, registered, artifacts.PutConditions{}); err != nil {
		infraAbsent(t, "cannot write the namespace document (is MinIO up?): %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := namespaceStore.Delete(cleanupCtx, nsName); err != nil {
			t.Logf("could not remove the test namespace %q: %v", nsName, err)
		}
	})

	alpha := newPoolOrker(t, "orker-A", poolAlpha)
	bravo := newPoolOrker(t, "orker-B", poolBravo)

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()

	runWorker := worker.New(temporalClient, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)

	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	started := map[string]worker.Worker{"run": runWorker, "event": eventWorker}
	for _, orker := range []*poolOrker{alpha, bravo} {
		w := worker.New(temporalClient, orker.queue, worker.Options{
			DisableRegistrationAliasing: true,
			DisableWorkflowWorker:       true,
		})
		w.RegisterActivityWithOptions(orker.execute,
			activity.RegisterOptions{Name: task.ActivityExecuteTask})
		w.RegisterActivityWithOptions(orker.describeOrker,
			activity.RegisterOptions{Name: task.ActivityDescribeOrker})
		started[orker.name] = w
	}
	for name, w := range started {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	orkflows := engine.NewMemoryOrkflowStore()
	starter := engine.NewTemporalStarter(temporalClient, task.DefaultQueue)
	svc := api.NewService(api.Config{
		Orkflows:   orkflows,
		Namespaces: namespaceStore,
		Reader:     engine.NewTemporalRunReader(temporalClient).WithSnapshotStore(snapshots),
		Runs:       runs,
		Bus:        bus,
		Starter:    starter,
		// The liveness answer comes from Temporal's poller information, which
		// is the whole point: the engine asks the cluster rather than guessing.
		Pools:      engine.NewTemporalPoolProbe(temporalClient),
		PoolOrkers: starter,
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
	httpClient := &http.Client{Transport: transport}
	baseURL := "http://" + listener.Addr().String()
	defer func() {
		transport.CloseIdleConnections()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	orkflowClient := orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL)
	runClient := orkstackv1connect.NewRunServiceClient(httpClient, baseURL)
	namespaceClient := orkstackv1connect.NewNamespaceServiceClient(httpClient, baseURL)

	transcript := &poolTranscript{}
	defer transcript.write(t)
	transcript.section("deployment")
	transcript.line("temporal namespace   %s", namespace)
	transcript.line("ork namespace        %s", nsName)
	transcript.line("allowed pools        %s", strings.Join(registered.GetAllowedPools(), ", "))
	transcript.line("orker A serves pool  %s", alpha.pool)
	transcript.line("orker B serves pool  %s", bravo.pool)
	transcript.line("pool names carry a per-run suffix: Temporal describes a channel's")
	transcript.line("pollers for minutes after their worker stopped, so a fixed name would")
	transcript.line("let an earlier run of this suite be counted as this one's Orkers.")

	// Wait until the cluster reports each pool served by exactly the one Orker
	// this test started, so the liveness check below is answering about a
	// deployment that is actually up — and about no other.
	probe := engine.NewTemporalPoolProbe(temporalClient)
	for _, orker := range []*poolOrker{alpha, bravo} {
		settled := awaitPoolServedBy(t, ctx, probe, orker.pool, orker.queue, 1)
		transcript.line("cluster reports pool %s served by 1 Orker after %s",
			orker.pool, settled.Round(time.Millisecond))
	}

	// ── 1. a task pinned to a pool runs on that pool's Orker, and only there ──
	twoPoolJob := fmt.Sprintf(`
name: %s
version: 0.1.0
tasks:
  - id: crunch
    uses: shell
    pool: %s
    with: {command: "echo crunch"}
  - id: polish
    uses: shell
    pool: %s
    needs: [crunch]
    with: {command: "echo polish"}
`, twoPoolName, poolAlpha, poolBravo)
	if _, err := orkflowClient.ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: twoPoolJob, Namespace: nsName})); err != nil {
		t.Fatalf("apply the two-pool orkflow: %v", err)
	}

	// ── 3's setup, submitted FIRST so the visibility argument below holds ──
	coldJob := fmt.Sprintf(`
name: %s
version: 0.1.0
tasks:
  - id: freeze
    uses: shell
    pool: %s
    with: {command: "echo freeze"}
`, coldName, poolCold)
	if _, err := orkflowClient.ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: coldJob, Namespace: nsName})); err != nil {
		t.Fatalf("apply the cold orkflow: %v", err)
	}
	_, coldErr := runClient.SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: coldName, Namespace: nsName}))
	if coldErr == nil {
		t.Fatalf("submitting into pool %q succeeded; nothing serves it, so the run "+
			"would have reached RUNNING and stayed there", poolCold)
	}
	if code := connect.CodeOf(coldErr); code != connect.CodeFailedPrecondition {
		t.Errorf("empty-pool submit refused with %v, want FailedPrecondition", code)
	}
	for _, want := range []string{poolCold, "freeze", "ORK_ORKER_POOL=" + poolCold} {
		if !strings.Contains(coldErr.Error(), want) {
			t.Errorf("empty-pool error %q does not name %q", coldErr, want)
		}
	}
	transcript.section("3. submit into a pool with no Orkers")
	transcript.line("SubmitRun(%s) -> %v", coldName, connect.CodeOf(coldErr))
	transcript.line("%s", coldErr)

	// ── 2. a pool the namespace forbids is refused, at apply and at submit ──
	forbiddenJob := fmt.Sprintf(`
name: %s
version: 0.1.0
tasks:
  - id: sneak
    uses: shell
    pool: forbidden
    with: {command: "echo sneak"}
`, forbiddenName)
	_, applyErr := orkflowClient.ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: forbiddenJob, Namespace: nsName}))
	if applyErr == nil {
		t.Fatal("applying an orkflow onto a forbidden pool succeeded")
	}
	// The submit gate is the one that cannot be bypassed: put the definition
	// into the store behind the API's back, exactly as a definition applied
	// while the allow-list was wider would be, and submit it.
	if _, _, err := orkflows.Put(ctx, &orkstackv1.Orkflow{
		Namespace: nsName,
		Name:      forbiddenName,
		Version:   "0.1.0",
		Tasks: []*orkstackv1.Task{
			{Id: "sneak", Uses: "shell", Pool: "forbidden"},
		},
	}, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("seed the stored definition: %v", err)
	}
	_, submitErr := runClient.SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: forbiddenName, Namespace: nsName}))
	if submitErr == nil {
		t.Fatal("submitting a stored definition onto a forbidden pool succeeded; the " +
			"allow-list is bypassable by anything that did not go through apply")
	}
	if code := connect.CodeOf(submitErr); code != connect.CodeInvalidArgument {
		t.Errorf("forbidden-pool submit refused with %v, want InvalidArgument", code)
	}
	for _, want := range []string{nsName, "sneak", "forbidden"} {
		if !strings.Contains(submitErr.Error(), want) {
			t.Errorf("forbidden-pool error %q does not name %q", submitErr, want)
		}
	}
	transcript.section("2. a pool the namespace does not allow")
	transcript.line("ApplyOrkflow(%s)  -> %v", forbiddenName, connect.CodeOf(applyErr))
	transcript.line("%s", applyErr)
	transcript.line("SubmitRun(%s)     -> %v", forbiddenName, connect.CodeOf(submitErr))
	transcript.line("%s", submitErr)

	// ── now run the two-pool job for real ──
	submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: twoPoolName, Namespace: nsName}))
	if err != nil {
		t.Fatalf("SubmitRun(%s): %v", twoPoolName, err)
	}
	runID := submitted.Msg.GetRun().GetId()

	// The namespace travels with the run id: a run is addressed INSIDE one
	// (proto/orkstack/v1/service.proto, GetRunRequest), so a stream that names
	// none asks `default` about a run this namespace owns.
	stream, err := runClient.StreamEvents(ctx, connect.NewRequest(
		&orkstackv1.StreamEventsRequest{RunId: runID, Namespace: nsName}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()
	scheduled := map[string]string{}
	var last *orkstackv1.Event
	for stream.Receive() {
		event := stream.Msg()
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED {
			scheduled[event.GetTaskId()] = event.GetMessage()
		}
		last = event
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream error: %v. Run %s never terminated; orker A ran %v, orker B ran %v",
			err, runID, alpha.ran(), bravo.ran())
	}
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("run %s ended on %v (%s), want RUN_SUCCEEDED", runID, last.GetType(), last.GetMessage())
	}

	// The evidence: which Orker ran which task, and on which channel Temporal
	// delivered it. Both come from the Orkers themselves.
	if got := alpha.ran(); len(got) != 1 || got[0] != "crunch" {
		t.Errorf("orker A (pool %s) ran %v, want exactly [crunch]", alpha.pool, got)
	}
	if got := bravo.ran(); len(got) != 1 || got[0] != "polish" {
		t.Errorf("orker B (pool %s) ran %v, want exactly [polish]", bravo.pool, got)
	}
	if got := alpha.channelFor("crunch"); got != alpha.queue {
		t.Errorf("crunch was delivered on %q, want pool %s's channel %q", got, alpha.pool, alpha.queue)
	}
	if got := bravo.channelFor("polish"); got != bravo.queue {
		t.Errorf("polish was delivered on %q, want pool %s's channel %q", got, bravo.pool, bravo.queue)
	}
	// The client-facing event says pool, never channel.
	for taskID, wantPool := range map[string]string{"crunch": poolAlpha, "polish": poolBravo} {
		message := scheduled[taskID]
		if !strings.Contains(message, "pool "+wantPool) {
			t.Errorf("TASK_SCHEDULED for %q says %q, want it to name pool %q", taskID, message, wantPool)
		}
		if strings.Contains(strings.ToLower(message), "queue") || strings.Contains(message, "ork-pool-") {
			t.Errorf("TASK_SCHEDULED for %q says %q; the engine's channel must not reach a client",
				taskID, message)
		}
	}

	transcript.section("1. per-task pool routing, one run, two Orkers")
	transcript.line("run                  %s -> %s", runID, last.GetType())
	transcript.line("orker A (pool %s) ran %v on %s", alpha.pool, alpha.ran(), alpha.channelFor("crunch"))
	transcript.line("orker B (pool %s) ran %v on %s", bravo.pool, bravo.ran(), bravo.channelFor("polish"))
	for _, taskID := range []string{"crunch", "polish"} {
		transcript.line("TASK_SCHEDULED %-7s %s", taskID, scheduled[taskID])
	}

	// ── 3's assertion: the refused submit left no run behind ──
	//
	// The cold submit happened BEFORE this run was created, so once this run
	// is visible, a run the cold submit had started would be visible too. The
	// orkflow name is this execution's own, so the only run that could answer
	// here is one this execution started — no other execution's residue can
	// pass or fail it.
	awaitRunListed(t, ctx, runClient, nsName, twoPoolName, runID)
	listed, err := runClient.ListRuns(ctx, connect.NewRequest(
		&orkstackv1.ListRunsRequest{Filters: equalsFilters("orkflow_name", coldName), Namespace: nsName}))
	if err != nil {
		t.Fatalf("ListRuns(%s): %v", coldName, err)
	}
	if n := len(listed.Msg.GetRuns()); n != 0 {
		t.Errorf("the refused empty-pool submit left %d run(s) behind: %v", n, listed.Msg.GetRuns())
	}
	transcript.line("runs for %s    %d (the refused submit created nothing)",
		coldName, len(listed.Msg.GetRuns()))

	// ── pools are observable, from the cluster, on this replica ──
	pools, err := namespaceClient.ListPools(ctx, connect.NewRequest(&orkstackv1.ListPoolsRequest{
		Namespace: nsName,
	}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	byName := map[string]*orkstackv1.Pool{}
	for _, pool := range pools.Msg.GetPools() {
		byName[pool.GetName()] = pool
	}
	for _, orker := range []*poolOrker{alpha, bravo} {
		reported := byName[orker.pool]
		// EXACTLY one. This test started one Orker per pool onto a channel
		// nothing else has ever polled, so any other number is a count of
		// something that is not serving the pool. "At least one" would report
		// a remembered-but-departed worker as success, and that is the whole
		// failure: an operator scaling a pool from three Orkers to one must be
		// told one.
		if got := reported.GetLiveOrkerCount(); got != 1 {
			t.Errorf("ListPools reports %d Orkers on pool %q; exactly 1 is serving it",
				got, orker.pool)
		}
	}
	if got := byName[poolCold].GetLiveOrkerCount(); got != 0 {
		t.Errorf("ListPools reports %d Orkers on the unserved pool %q, want 0", got, poolCold)
	}
	transcript.section("6. pools, observed from the cluster")
	transcript.line("default pool         %s", pools.Msg.GetDefaultPool())
	for _, pool := range pools.Msg.GetPools() {
		transcript.line("pool %-24s orkers=%d", pool.GetName(), pool.GetLiveOrkerCount())
	}
}

// awaitPoolServedBy waits until the cluster reports exactly want Orkers on one
// pool's channel, and returns how long that took.
//
// It waits for two reasons. The Orker this test started may not have polled
// yet, and Temporal's poller information is not instantaneous. Exactly is the
// point: the pools are this run's own (see the token above), so nothing else
// has ever polled these channels and any number other than want is a number
// the deployment does not justify. The deadline outlasts
// DefaultPollerFreshness so that a stale record, if one could ever reach here,
// would age out rather than be asserted around.
//
// It takes the channel rather than reading it off a poolOrker, because the
// operator's raw channel (ORK_ORKER_QUEUE) is nobody's pool and still has to
// be waited for the same way — see TestIntegration_TasksRunOnTheQueueTheEnginesOrkersPoll.
func awaitPoolServedBy(
	t *testing.T,
	ctx context.Context,
	probe engine.PoolProbe,
	pool, channel string,
	want int,
) time.Duration {
	t.Helper()
	started := time.Now()
	deadline := started.Add(engine.DefaultPollerFreshness + 30*time.Second)
	var last engine.PoolLiveness
	for time.Now().Before(deadline) {
		live, err := probe.ObservePool(ctx, channel)
		if err == nil {
			last = live
			if live.Orkers == want && live.Fresh(time.Now(), engine.DefaultPollerFreshness) {
				return time.Since(started)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("the cluster reports %d Orkers on pool %q, want %d (last seen %v)",
		last.Orkers, pool, want, last.LastSeen)
	return 0
}

// awaitRunListed waits until a run appears in the read model, so a later
// "nothing is listed" assertion is made against warm visibility.
func awaitRunListed(
	t *testing.T,
	ctx context.Context,
	runs orkstackv1connect.RunServiceClient,
	namespace, orkflow, runID string,
) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		listed, err := runs.ListRuns(ctx, connect.NewRequest(
			&orkstackv1.ListRunsRequest{Filters: equalsFilters("orkflow_name", orkflow), Namespace: namespace}))
		if err == nil {
			for _, run := range listed.Msg.GetRuns() {
				if run.GetId() == runID {
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s never became visible; a later empty listing would prove nothing", runID)
}

// poolTranscript records what happened, as evidence.
type poolTranscript struct {
	mu    sync.Mutex
	lines []string
}

func (p *poolTranscript) section(title string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lines = append(p.lines, "", "── "+title+" "+strings.Repeat("─", max(0, 60-len(title))))
}

func (p *poolTranscript) line(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lines = append(p.lines, fmt.Sprintf(format, args...))
}

func (p *poolTranscript) write(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	header := []string{
		"# two Orkers, two pools, one cluster",
		"# Every fact below came from the cluster or from the Orker that executed",
		"# the task, never from the engine's own expectation.",
		"# recorded " + time.Now().UTC().Format(time.RFC3339),
	}
	proof.Write(t, "engine", "two-pool-transcript.txt",
		[]byte(strings.Join(append(header, p.lines...), "\n")+"\n"))
}
