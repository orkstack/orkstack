//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// This file is the design's `tworeplica_read_integration_test.go`. It lives in
// cmd/orkctl rather than internal/engine — the one deviation from
// .atomic/production-hardening-design.md §M8.3 "Files" — because the harness
// that boots *real* `ork` binaries on separate ports already exists here
// (startReplica, buildBinary, startProcess, isolatedNamespace). Rebuilding it
// under internal/engine would be the duplicated-integration-helper hazard
// AGENTS.md §7 records twice, and two real processes are a strictly stronger
// claim than two in-process engines.

// TestIntegration_SecondReplicaServesARunItNeverStarted is the falsifying test
// for M8.3. Two real `ork` processes share one Temporal namespace, one Orker
// queue and one MinIO bucket — the production topology behind a load balancer.
// A run is submitted to A only; every read below is issued against B.
//
// B never accepted the submission, so its RunStore has no entry for the run,
// and the projection activities it happened not to execute are not in its
// EventBus. Before M8.3 that made every one of these reads a NotFound while the
// run was still executing. They pass only because the reads leave the process.
//
// The reads are taken **while the run is live**, checked against the cluster's
// own status, so an archive-only answer cannot satisfy them: nothing is
// archived before a run's terminal event.
//
// Isolation (AGENTS.md §6): a unique namespace per test run. Note the
// deliberate inversion of the usual rule — the two replicas share it, because
// sharing a namespace is exactly the condition under test.
func TestIntegration_SecondReplicaServesARunItNeverStarted(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	namespace := isolatedNamespace()
	t.Logf("namespace %s", namespace)
	temporalClient := dialIsolatedNamespace(ctx, t, address, namespace)

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")

	queue := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, orkerPath, address, namespace, queue)
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, namespace, queue)
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, namespace, queue)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	orkflowName := fmt.Sprintf("tworeplica-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: twoReplicaOrkflowYAML(orkflowName),
	})); err != nil {
		t.Fatalf("ApplyOrkflow on replica A: %v", err)
	}
	awaitOrkflowVisible(ctx, t, replicaB.flows, orkflowName)

	// ---- the run: submitted to A, and only to A ----------------------------
	submitted, err := replicaA.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: orkflowName,
	}))
	if err != nil {
		t.Fatalf("SubmitRun on replica A: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	t.Logf("replica A started run %s", runID)

	// ---- acceptance 1 + 2: B streams the whole run, from sequence 1 --------
	// The stream is opened immediately, so it is a live subscription and not a
	// replay of anything finished.
	streamed := newEventCollector()
	streamDone := make(chan error, 1)
	go func() { streamDone <- collectStream(ctx, replicaB, runID, 0, streamed) }()

	// ---- acceptance 1: B answers GetRun while the run is still executing ---
	liveRun := awaitReadableOnB(ctx, t, replicaB, runID)
	if liveRun.GetOrkflowName() != orkflowName || liveRun.GetOrkflowVersion() != "0.1.0" {
		t.Errorf("replica B describes run %s as %s@%s, want %s@0.1.0",
			runID, liveRun.GetOrkflowName(), liveRun.GetOrkflowVersion(), orkflowName)
	}
	// The proof that the archive cannot be answering: the cluster still reports
	// the execution running, and nothing is archived before a terminal event.
	liveStatus := executionStatus(ctx, t, temporalClient, runID)
	if liveStatus != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the run reached %s before replica B was asked; the orkflow is too fast "+
			"for this test to distinguish a live read from an archived one", liveStatus)
	}
	t.Logf("replica B answered GetRun(%s) as %s while the cluster reported %s",
		runID, liveRun.GetStatus(), liveStatus)

	// The stream, too, has to be delivering while the run is live.
	awaitStreamedWhileLive(ctx, t, temporalClient, runID, streamed)

	// ---- the run finishes; the stream must end on its terminal event -------
	select {
	case err := <-streamDone:
		if err != nil {
			t.Fatalf("StreamEvents on replica B: %v", err)
		}
	case <-time.After(4 * time.Minute):
		t.Fatalf("replica B's stream never ended; it holds %v", streamed.sequences())
	}
	fromB := streamed.events()
	assertRunStreamContract(t, "replica B", fromB, 0)

	awaitWorkflowCompleted(ctx, t, temporalClient, runID)

	// ---- acceptance 1: B reaches the terminal status ------------------------
	finished := awaitTerminalOnB(ctx, t, replicaB, runID)
	if finished.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("replica B reports run %s as %s, want SUCCEEDED", runID, finished.GetStatus())
	}
	if len(finished.GetTaskRuns()) != 2 {
		t.Errorf("replica B reports %d task runs, want the run's 2 — the answer is not complete",
			len(finished.GetTaskRuns()))
	}

	// ---- the two replicas agree, field for field ---------------------------
	// This is the scheduling-independent half of the falsification. A run's
	// projection activities are executed exactly once each, by whichever
	// replica polls ork-engine-events first, so a process-local event log can
	// only ever hold a subset — and two subsets that partition one run cannot
	// both be the whole run. Requiring A *and* B to serve 1..N therefore fails
	// under a process-local read model however the activities happened to be
	// dealt out.
	replayA := newEventCollector()
	if err := collectStream(ctx, replicaA, runID, 0, replayA); err != nil {
		t.Fatalf("StreamEvents on replica A: %v", err)
	}
	fromA := replayA.events()
	assertRunStreamContract(t, "replica A", fromA, 0)
	assertSameStream(t, fromA, fromB)

	// ---- the WebSocket bridge reads the same record -------------------------
	fromSocket := readWebSocketStream(ctx, t, replicaB, runID)
	assertRunStreamContract(t, "replica B websocket", fromSocket, 0)
	assertSameStream(t, fromB, fromSocket)

	// ---- a cursor resumes inside the record ---------------------------------
	resumed := newEventCollector()
	if err := collectStream(ctx, replicaB, runID, 2, resumed); err != nil {
		t.Fatalf("StreamEvents(from_sequence=2) on replica B: %v", err)
	}
	assertRunStreamContract(t, "replica B resumed", resumed.events(), 2)

	// ---- acceptance 1: B lists a run it never started -----------------------
	listed := awaitListedOnB(ctx, t, replicaB, runID, orkflowName)
	t.Logf("replica B lists run %s as %s", listed.GetId(), listed.GetStatus())

	// ---- acceptance 3: the archive is the record for the finished run -------
	// It also has to be *complete*. The projection activities of one run are
	// dispatched to whichever replica polls ork-engine-events first, so the
	// archive body is only whole because it is assembled from the system of
	// record rather than from one process's event bus.
	archived := awaitArchivedStream(ctx, t, artifactCfg, runID)
	assertRunStreamContract(t, "archive", archived, 0)
	assertSameStream(t, fromB, archived)

	writeTwoReplicaTranscript(t, namespace, runID, orkflowName, liveRun, finished, fromB, archived)
}

// requireTemporal skips cleanly when the dev cluster is absent — or fails,
// under ORK_REQUIRE_INFRA (see infraAbsent).
func requireTemporal(t *testing.T, address string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
}

// awaitReadableOnB polls replica B's GetRun until it answers. It polls because
// a submission returns as soon as the workflow is started, and the read has to
// be given the moment it takes for the execution to exist; it must not need
// more than that.
func awaitReadableOnB(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID string,
) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId: runID,
		}))
		if err == nil {
			return got.Msg.GetRun()
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("replica %s never answered GetRun(%s) for a run replica A started: %v",
		replica.name, runID, lastErr)
	return nil
}

// awaitTerminalOnB polls until B reports the run finished.
func awaitTerminalOnB(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID string,
) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var last *orkstackv1.Run
	for time.Now().Before(deadline) {
		got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId: runID,
		}))
		if err == nil {
			last = got.Msg.GetRun()
			switch last.GetStatus() {
			case orkstackv1.Status_STATUS_SUCCEEDED,
				orkstackv1.Status_STATUS_FAILED,
				orkstackv1.Status_STATUS_CANCELED:
				return last
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("replica %s never reported run %s terminal (last %v)", replica.name, runID, last)
	return nil
}

// awaitListedOnB polls ListRuns on B. Visibility is eventually consistent, so
// the run's appearance is awaited rather than demanded on the first call; that
// is a property of the index, not of the read model, and GetRun above already
// answered for the same run by id with no delay at all.
func awaitListedOnB(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID, orkflowName string,
) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		listed, err := replica.runs.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
			OrkflowName: orkflowName,
		}))
		if err != nil {
			t.Fatalf("ListRuns on %s: %v", replica.name, err)
		}
		for _, run := range listed.Msg.GetRuns() {
			if run.GetId() != runID {
				continue
			}
			if run.GetOrkflowName() != orkflowName {
				t.Fatalf("%s lists run %s under orkflow %q, want %q",
					replica.name, runID, run.GetOrkflowName(), orkflowName)
			}
			return run
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("replica %s never listed run %s, a run it did not start", replica.name, runID)
	return nil
}

// awaitStreamedWhileLive proves the stream is live: events arrive on B before
// the cluster closes the execution, which no archived replay could do.
func awaitStreamedWhileLive(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	runID string,
	collector *eventCollector,
) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		got := collector.sequences()
		if len(got) >= 2 {
			status := executionStatus(ctx, t, c, runID)
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				t.Fatalf("replica B received %v only after the run closed (%s); "+
					"the stream is not live", got, status)
			}
			t.Logf("replica B had streamed %v while the cluster reported %s", got, status)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("replica B streamed %v within the deadline, want at least two live events",
		collector.sequences())
}

// executionStatus asks the server, not a replica.
func executionStatus(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	runID string,
) enumspb.WorkflowExecutionStatus {
	t.Helper()
	described, err := c.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatalf("DescribeWorkflowExecution(%s): %v", runID, err)
	}
	return described.GetWorkflowExecutionInfo().GetStatus()
}

// eventCollector accumulates a stream from another goroutine.
type eventCollector struct {
	mu  sync.Mutex
	got []*orkstackv1.Event
}

func newEventCollector() *eventCollector { return &eventCollector{} }

func (c *eventCollector) add(event *orkstackv1.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, event)
}

func (c *eventCollector) events() []*orkstackv1.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*orkstackv1.Event(nil), c.got...)
}

func (c *eventCollector) sequences() []int64 {
	out := []int64{}
	for _, event := range c.events() {
		out = append(out, event.GetSequence())
	}
	return out
}

// collectStream drains one StreamEvents call into a collector.
func collectStream(
	ctx context.Context,
	replica *engineReplica,
	runID string,
	fromSequence int64,
	into *eventCollector,
) error {
	stream, err := replica.runs.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId:        runID,
		FromSequence: fromSequence,
	}))
	if err != nil {
		return err
	}
	defer stream.Close()
	for stream.Receive() {
		into.add(stream.Msg())
	}
	return stream.Err()
}

// readWebSocketStream reads the bridge to its close frame.
func readWebSocketStream(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID string,
) []*orkstackv1.Event {
	t.Helper()
	conn, _, err := websocket.Dial(ctx, "ws://"+replica.addr+"/ws/runs/"+runID, nil)
	if err != nil {
		t.Fatalf("dial %s's websocket bridge for run %s: %v", replica.name, runID, err)
	}
	defer conn.CloseNow()

	var out []*orkstackv1.Event
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return out
		}
		event := &orkstackv1.Event{}
		if err := api.UnmarshalJSON(data, event); err != nil {
			t.Fatalf("decode websocket frame %q: %v", data, err)
		}
		out = append(out, event)
	}
}

// awaitArchivedStream reads the run's archived body straight from MinIO.
func awaitArchivedStream(
	ctx context.Context,
	t *testing.T,
	cfg artifacts.Config,
	runID string,
) []*orkstackv1.Event {
	t.Helper()
	store, err := artifacts.NewRunArchive(cfg)
	if err != nil {
		t.Fatalf("open run archive: %v", err)
	}
	archiver := engine.NewRunArchiver(store)
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, events, err := archiver.Load(ctx, runID)
		if err == nil {
			return events
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s was never archived: %v", runID, lastErr)
	return nil
}

// assertRunStreamContract is acceptance 2: strictly increasing sequences with
// no gaps, starting just after the cursor, ending on a terminal event and only
// there.
func assertRunStreamContract(t *testing.T, source string, events []*orkstackv1.Event, from int64) {
	t.Helper()
	if len(events) == 0 {
		t.Fatalf("%s delivered no events", source)
	}
	for i, event := range events {
		if got, want := event.GetSequence(), from+int64(i)+1; got != want {
			t.Fatalf("%s: event %d carries sequence %d, want %d (stream %v)",
				source, i, got, want, sequenceList(events))
		}
	}
	if !api.IsTerminalEvent(events[len(events)-1]) {
		t.Fatalf("%s ended on %s, want a terminal event",
			source, events[len(events)-1].GetType())
	}
	for _, event := range events[:len(events)-1] {
		if api.IsTerminalEvent(event) {
			t.Fatalf("%s delivered terminal event %s before the end of the stream",
				source, event.GetType())
		}
	}
	if from == 0 && events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CREATED {
		t.Fatalf("%s starts at %s, want RUN_CREATED as event 1", source, events[0].GetType())
	}
}

// assertSameStream compares two readings of one run field for field.
func assertSameStream(t *testing.T, want, got []*orkstackv1.Event) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("streams differ in length: %v vs %v", sequenceList(want), sequenceList(got))
	}
	for i := range want {
		if !proto.Equal(want[i], got[i]) {
			t.Fatalf("event %d differs between two readings of the same run:\n%v\n%v",
				i, want[i], got[i])
		}
	}
}

func sequenceList(events []*orkstackv1.Event) []int64 {
	out := make([]int64, 0, len(events))
	for _, event := range events {
		out = append(out, event.GetSequence())
	}
	return out
}

// twoReplicaOrkflowYAML is deliberately slow: the reads under test have to
// happen while the run is executing, because a finished run could be answered
// from the archive and would prove nothing.
func twoReplicaOrkflowYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M8.3 two-replica read probe.
tasks:
  - id: slow
    uses: shell
    timeout_seconds: 60
    with:
      op: exec
      command: sleep 8; echo slow
      capture_stdout_as: out
  - id: after
    uses: shell
    needs: [slow]
    timeout_seconds: 60
    with:
      op: exec
      command: echo after
      capture_stdout_as: out
`, name)
}

// writeTwoReplicaTranscript records the proof artifact when the milestone's
// reproduce.sh asks for one.
func writeTwoReplicaTranscript(
	t *testing.T,
	namespace, runID, orkflowName string,
	live, finished *orkstackv1.Run,
	streamed, archived []*orkstackv1.Event,
) {
	t.Helper()
	path := os.Getenv("ORK_PROOF_TWOREPLICA_FILE")
	if path == "" {
		return
	}
	var out strings.Builder
	fmt.Fprintf(&out, "# two engine replicas, one namespace, one queue (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "namespace              %s\n", namespace)
	fmt.Fprintf(&out, "orkflow                %s@0.1.0\n", orkflowName)
	fmt.Fprintf(&out, "run                    %s\n", runID)
	fmt.Fprintf(&out, "submitted on           replica A (ork-A)\n")
	fmt.Fprintf(&out, "every read below on    replica B (ork-B), which never saw the submission\n\n")
	fmt.Fprintf(&out, "B GetRun while live    status=%s orkflow=%s@%s\n",
		live.GetStatus(), live.GetOrkflowName(), live.GetOrkflowVersion())
	fmt.Fprintf(&out, "B GetRun at the end    status=%s task_runs=%d\n",
		finished.GetStatus(), len(finished.GetTaskRuns()))
	fmt.Fprintf(&out, "\nB StreamEvents (opened while the run was executing)\n")
	for _, event := range streamed {
		fmt.Fprintf(&out, "  %3d  %-28s %-8s %s\n",
			event.GetSequence(), event.GetType(), event.GetTaskId(), event.GetMessage())
	}
	fmt.Fprintf(&out, "\narchived stream in MinIO: %d events, identical to B's: %t\n",
		len(archived), len(archived) == len(streamed))
	if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
		t.Errorf("write two-replica transcript: %v", err)
	}
}
