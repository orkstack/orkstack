//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// privateOrkerQueue names a task queue no other process on this cluster polls.
func privateOrkerQueue() string {
	return fmt.Sprintf("ork-tasks-engine-%d-%d", os.Getpid(), time.Now().UnixNano())
}

// routingNamespace names a Temporal namespace this test run alone uses.
//
// A private orker queue is NOT enough isolation on its own, and this test used
// to have only that: it hosts RunWorkflow on the shared task.DefaultQueue, so
// in the cluster's default namespace a parallel agent polling `ork-tasks` can
// take its workflow tasks and its PASS becomes a fact about who else was on
// the cluster. ORK_ORKER_QUEUE partitions task EXECUTION only; a namespace is
// the hard partition, and the one AGENTS.md §11.1 tells a parallel agent to use.
// The operator's ORK_TEMPORAL_NAMESPACE stays as the prefix so the choice is
// visible in `temporal workflow list`.
func routingNamespace() string {
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	return fmt.Sprintf("%s-routing-%d-%d", base, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// dialRoutingCluster provisions that namespace and returns a client bound to
// it, skipping cleanly when no cluster is there.
func dialRoutingCluster(t *testing.T, ctx context.Context, address, namespace string) client.Client {
	t.Helper()
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

	// A freshly registered namespace is not immediately visible to the
	// frontend's namespace cache, so the first dial is retried rather than
	// failed.
	deadline := time.Now().Add(60 * time.Second)
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
		if err == nil {
			if _, probe := c.DescribeTaskQueue(ctx, privateOrkerQueue(), 0); probe == nil {
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

// TestIntegration_TasksRunOnTheQueueTheEnginesOrkersPoll is the end of the
// ORK_ORKER_QUEUE story: an engine configured for a private orker queue must
// dispatch an ordinary orkflow's tasks there. The orkflow (examples/hello.ork.yaml)
// names no queue, and nothing in this test polls the shared default queue for
// activities — so if dispatch ignored the engine's configuration, every task
// would sit unassigned and the run would never leave RUNNING.
//
// The same configuration is where dispatch and reporting can drift apart, so
// both are asserted against this one engine: the submit is accepted because
// the private channel is served, and ListPools must answer for the default
// pool with the Orkers on that same channel. An engine that looked for them
// on the pool's own channel instead would call a pool deserted while running
// its work.
//
// It runs in a namespace of its own (routingNamespace), because the run worker
// below polls the shared task.DefaultQueue exactly as `ork` does.
func TestIntegration_TasksRunOnTheQueueTheEnginesOrkersPoll(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	namespace := routingNamespace()
	temporalClient := dialRoutingCluster(t, ctx, address, namespace)

	orkerQueue := privateOrkerQueue()
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()

	var observedMu sync.Mutex
	observedQueues := map[string]string{}

	// The run worker polls the engine's own queue and handles workflow tasks
	// only, exactly as `ork` does.
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

	// The Orker stub polls the private queue and nothing else. Its record of
	// the queue each activity arrived on is the evidence: it comes from
	// Temporal's own activity info, not from the engine.
	orkerWorker := worker.New(temporalClient, orkerQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	orkerWorker.RegisterActivityWithOptions(
		func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			observedMu.Lock()
			observedQueues[in.TaskId] = activity.GetInfo(actCtx).TaskQueue
			observedMu.Unlock()
			return &orkstackv1.ExecuteTaskResult{
				TaskId:     in.TaskId,
				ValuesJson: valuesJSON(map[string]any{"echoed_task": in.TaskId}),
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	// A real Orker answers the liveness probe on its own channel too
	// (cmd/orker registers this), which is what ListPools asks a served pool.
	orkerWorker.RegisterActivityWithOptions(
		func(context.Context) (*orkstackv1.DescribeOrkerResult, error) {
			return &orkstackv1.DescribeOrkerResult{
				OrkerId:     "orker-queue-integration",
				RuntimeName: "docker",
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityDescribeOrker},
	)

	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	for name, w := range map[string]worker.Worker{
		"run": runWorker, "orker": orkerWorker, "event": eventWorker,
	} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, task.DefaultQueue, orkerQueue)
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     runs,
		Bus:      bus,
		Starter:  starter,
		// RawQueue is the operator escape hatch, and it is what pins this
		// engine's DEFAULT route — where a task that names no pool goes. The
		// starter's own orker queue above is the liveness probe only;
		// dispatch comes from the plan the submission builds, and this is the
		// field that puts the private channel into it. `cmd/ork` sets the
		// same value from ORK_ORKER_QUEUE (or ORK_ORKER_POOL).
		RawQueue: orkerQueue,
		// The real probe and the real Orker describer, as `cmd/ork` wires them.
		// With RawQueue set they are the pair that can disagree: the submit
		// below is checked against the private channel, and ListPools at the
		// end has to report the same pool from the same channel.
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

	source, err := os.ReadFile("../../examples/hello.ork.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	if strings.Contains(string(source), "pool:") {
		t.Fatalf("the example now pins a pool; this test needs an orkflow that names none")
	}

	orkflowClient := orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL)
	runClient := orkstackv1connect.NewRunServiceClient(httpClient, baseURL)
	namespaceClient := orkstackv1connect.NewNamespaceServiceClient(httpClient, baseURL)

	// The submission below is checked for live Orkers against the private
	// channel, so wait until the cluster can see the one this test started.
	// Nothing else has ever polled this channel, so exactly one is exact.
	awaitPoolServedBy(t, ctx, engine.NewTemporalPoolProbe(temporalClient),
		task.DefaultPool, orkerQueue, 1)

	// runOrkflow applies a document, submits it, drains its event stream to a
	// terminal event and returns the TASK_SCHEDULED messages a client read.
	runOrkflow := func(label, source, orkflowName string) []string {
		t.Helper()
		if _, err := orkflowClient.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: source,
		})); err != nil {
			t.Fatalf("%s: Apply: %v", label, err)
		}
		submitted, err := runClient.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: orkflowName,
		}))
		if err != nil {
			t.Fatalf("%s: SubmitRun: %v", label, err)
		}
		runID := submitted.Msg.GetRun().GetId()

		stream, err := runClient.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
			RunId: runID,
		}))
		if err != nil {
			t.Fatalf("%s: StreamEvents: %v", label, err)
		}
		defer stream.Close()

		var scheduled []string
		var last *orkstackv1.Event
		for stream.Receive() {
			event := stream.Msg()
			if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED {
				scheduled = append(scheduled, event.GetMessage())
			}
			last = event
		}
		if err := stream.Err(); err != nil {
			// A deadline here is the symptom this test exists for: the tasks
			// were scheduled onto a queue no worker polls, so the run never
			// terminated and the stream stayed open until the context expired.
			observedMu.Lock()
			reached := fmt.Sprintf("%v", observedQueues)
			observedMu.Unlock()
			t.Fatalf("%s: event stream error: %v. Run %s never reached a terminal state; "+
				"the orker on %q executed %s, so check that dispatch follows the engine's orker queue",
				label, err, runID, orkerQueue, reached)
		}
		if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
			t.Fatalf("%s: run %s ended on %v (%s), want RUN_SUCCEEDED. Tasks reached the queues %v; "+
				"an unassigned run means dispatch ignored the engine's orker queue %q",
				label, runID, last.GetType(), last.GetMessage(), observedQueues, orkerQueue)
		}
		return scheduled
	}

	scheduled := runOrkflow("no pool", string(source), "hello-ork")

	// The same pool, written out. The stream above reports an orkflow that
	// named no pool as running on the built-in default; a document that names
	// that default must therefore reach the SAME Orkers — the ones on the
	// private queue — or the word the client read named a pool the task never
	// ran on. It is also the isolation guarantee: `pool: main` may not escape
	// ORK_ORKER_QUEUE onto the shared queue (AGENTS.md §11.1).
	pinnedSource := fmt.Sprintf(`
apiVersion: orkstack/v1
kind: Orkflow
name: pool-main-pinned
version: "0.1.0"
tasks:
  - id: pinned-echo
    uses: shell
    pool: %s
    with:
      op: exec
      command: "echo pinned"
`, task.DefaultPool)
	pinnedScheduled := runOrkflow("pool: "+task.DefaultPool, pinnedSource, "pool-main-pinned")

	observedMu.Lock()
	defer observedMu.Unlock()
	if len(observedQueues) != 4 {
		t.Fatalf("orker executed %d tasks (%v), want the example's 3 plus the pinned one",
			len(observedQueues), observedQueues)
	}
	for taskID, queue := range observedQueues {
		if queue != orkerQueue {
			t.Errorf("task %q ran on queue %q, want the engine's orker queue %q", taskID, queue, orkerQueue)
		}
	}
	if queue := observedQueues["pinned-echo"]; queue != orkerQueue {
		t.Errorf("`pool: %s` ran on %q, want the engine's orker queue %q. The stream tells a client "+
			"the task ran on pool %s; resubmitting to that pool must reach these Orkers, and must not "+
			"reach the shared queue %q",
			task.DefaultPool, queue, orkerQueue, task.DefaultPool, task.DefaultQueue)
	}

	// The run's own event stream reports where a task went — in pools, the
	// word an author types. The queue this test proves the dispatch reached
	// (above, from Temporal's own activity info) is a raw isolation queue and
	// therefore nobody's pool, so both orkflows — the one that named no pool
	// and the one that named the built-in default — are reported on that
	// default. A client reads this string: `orkctl watch` prints it and the
	// run page renders it, so a queue name here would be Temporal's word plus
	// an internal name the author never typed. The naming gate exempts
	// internal/engine and cannot see it; this can.
	if len(pinnedScheduled) != 1 {
		t.Fatalf("the pinned run carries %d TASK_SCHEDULED events (%v), want one", len(pinnedScheduled), pinnedScheduled)
	}
	for _, message := range append(scheduled, pinnedScheduled...) {
		if !strings.Contains(message, "pool "+task.DefaultPool) {
			t.Errorf("TASK_SCHEDULED event says %q, want it to name pool %q",
				message, task.DefaultPool)
		}
		if strings.Contains(strings.ToLower(message), "queue") || strings.Contains(message, orkerQueue) {
			t.Errorf("TASK_SCHEDULED event says %q; the engine word and the queue name %q "+
				"must not reach a client", message, orkerQueue)
		}
	}

	// The other half of the same configuration: an operator asking which pools
	// exist must be told about the Orker that just ran this run's three tasks.
	// Reported from the pool's own channel instead, the answer would be "0
	// Orkers" about a pool this engine dispatches to and accepts
	// submissions into — one process, one second, two answers.
	pools, err := namespaceClient.ListPools(ctx, connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	var main *orkstackv1.Pool
	for _, pool := range pools.Msg.GetPools() {
		if pool.GetName() == task.DefaultPool {
			main = pool
		}
	}
	if main == nil {
		t.Fatalf("ListPools returned %v, without the default pool", pools.Msg.GetPools())
	}
	if got := main.GetLiveOrkerCount(); got != 1 {
		t.Errorf("ListPools reports %d Orkers on pool %q; the one Orker this test started "+
			"is on the engine's own orker queue %q and just executed %d tasks",
			got, task.DefaultPool, orkerQueue, len(observedQueues))
	}
	// On the record: one engine, one configuration, both answers.
	t.Logf("ORK_ORKER_QUEUE=%s: %d task(s) executed there, and ListPools reports "+
		"pool %q with %d Orker(s)",
		orkerQueue, len(observedQueues), task.DefaultPool,
		main.GetLiveOrkerCount())
}
