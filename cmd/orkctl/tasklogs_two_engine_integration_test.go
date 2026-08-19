//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// M10.5's falsifying tests. Both run the production topology: two real `ork`
// processes and one real `orker`, sharing one Temporal namespace, one execution
// channel and one MinIO bucket — the deliberate exception to the per-agent
// isolation rule (AGENTS.md §6), because sharing is the condition under test.

// TestIntegration_SecondReplicaFollowsATaskLogWhileTheRunIsStillRunning is the
// milestone's central claim, and the one a post-mortem read cannot satisfy.
//
// A task emits a line a second for ten seconds. It is submitted to replica A.
// Every log read below is issued against replica B, which accepted no
// submission, executed none of this run's activities, and has nothing about it
// in memory. The assertion is about TIME: ordered lines must arrive at B while
// the cluster still reports the workflow RUNNING, so an implementation that
// stored the output as an artifact and served it at the end fails here.
func TestIntegration_SecondReplicaFollowsATaskLogWhileTheRunIsStillRunning(t *testing.T) {
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

	orkflowName := fmt.Sprintf("tasklogs-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: slowEmittingOrkflowYAML(orkflowName),
	})); err != nil {
		t.Fatalf("ApplyOrkflow on replica A: %v", err)
	}
	awaitOrkflowVisible(ctx, t, replicaB.flows, orkflowName)

	submitted, err := replicaA.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: orkflowName,
	}))
	if err != nil {
		t.Fatalf("SubmitRun on replica A: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	t.Logf("replica A started run %s", runID)

	// ---- follow the log from B, from the first chunk ------------------------
	follower := newLogFollower()
	followDone := make(chan error, 1)
	go func() { followDone <- followTaskLog(ctx, replicaB, runID, "emit", follower) }()

	// ---- the assertion that separates streaming from a post-mortem read -----
	// Wait for ordered lines at B, and check the CLUSTER's own status at the
	// moment they arrive. Nothing is archived before a run's terminal event,
	// and this stream is not the archive: it is the log store, being written
	// by an Orker that has not finished.
	liveLines := awaitLinesWhileRunning(ctx, t, temporalClient, runID, follower, 3)
	t.Logf("replica B held %d ordered lines while the cluster reported RUNNING: %q",
		len(liveLines), liveLines)

	// ---- the stream must end on its own when the task finishes -------------
	select {
	case err := <-followDone:
		if err != nil {
			t.Fatalf("following the task log on replica B: %v", err)
		}
	case <-time.After(4 * time.Minute):
		t.Fatalf("replica B's log stream never ended; it holds %d chunks", follower.count())
	}
	awaitWorkflowCompleted(ctx, t, temporalClient, runID)

	// ---- ordering is total and gap-free ------------------------------------
	chunks := follower.chunks()
	for i, chunk := range chunks {
		if want := int64(i + 1); chunk.GetSeq() != want && !chunk.GetLast() {
			t.Fatalf("chunk %d carries seq %d, want %d: the stream has a gap or a reorder",
				i, chunk.GetSeq(), want)
		}
	}
	streamed := follower.body()
	for line := 1; line <= 10; line++ {
		if !strings.Contains(streamed, fmt.Sprintf("emit-%02d", line)) {
			t.Fatalf("the followed log is missing emit-%02d; it reads:\n%s", line, streamed)
		}
	}
	if got := lineOrder(streamed); !isAscending(got) {
		t.Fatalf("the followed lines arrived out of order: %v", got)
	}

	// ---- and the whole log is readable afterwards, from the same replica ---
	postMortem := newLogFollower()
	if err := replayTaskLog(ctx, replicaB, runID, "emit", postMortem); err != nil {
		t.Fatalf("post-mortem read on replica B: %v", err)
	}
	for line := 1; line <= 10; line++ {
		if !strings.Contains(postMortem.body(), fmt.Sprintf("emit-%02d", line)) {
			t.Fatalf("the post-mortem read is missing emit-%02d; it reads:\n%s",
				line, postMortem.body())
		}
	}

	// ---- resume replays nothing twice and misses nothing -------------------
	resumeAt := chunks[0].GetSeq()
	resumed := newLogFollower()
	if err := replayTaskLogFrom(ctx, replicaB, runID, "emit", resumeAt, resumed); err != nil {
		t.Fatalf("resumed read on replica B: %v", err)
	}
	for _, chunk := range resumed.chunks() {
		if chunk.GetSeq() <= resumeAt && !chunk.GetLast() {
			t.Fatalf("resuming after seq %d replayed seq %d", resumeAt, chunk.GetSeq())
		}
	}
	if head := string(chunks[0].GetData()); strings.Contains(resumed.body(), head) && head != "" {
		t.Fatalf("resuming after seq %d replayed the first chunk's body", resumeAt)
	}

	writeTaskLogTranscript(t, namespace, runID, orkflowName, liveLines, follower, postMortem)
}

// TestIntegration_TaskLogBytesNeverEnterRunHistory is the invariant test for
// AGENTS.md §1.4. A task emits a marker string; the marker must be delivered by
// StreamTaskLogs and must appear NOWHERE in the run's Temporal history or in
// its archived event stream.
//
// It fails the moment somebody carries a log line on an Event — the design
// this milestone rejected, and the one that is one line of code away at all
// times (EVENT_TYPE_TASK_LOG already has a `message` field).
func TestIntegration_TaskLogBytesNeverEnterRunHistory(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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
	engine := startReplica(ctx, t, "ork-history", enginePath, address, namespace, queue)
	engine.awaitReady(ctx, t)

	// A marker no other part of the system could produce, so finding it
	// anywhere is proof of where it came from.
	marker := fmt.Sprintf("ORKLOGMARKER-%d-%d", os.Getpid(), time.Now().UnixNano())
	orkflowName := fmt.Sprintf("logmarker-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, err := engine.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: markerOrkflowYAML(orkflowName, marker),
	})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	submitted, err := engine.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: orkflowName,
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	awaitWorkflowCompleted(ctx, t, temporalClient, runID)

	// ---- the marker IS in the log stream ----------------------------------
	logged := newLogFollower()
	if err := replayTaskLog(ctx, engine, runID, "emit", logged); err != nil {
		t.Fatalf("read task logs: %v", err)
	}
	if !strings.Contains(logged.body(), marker) {
		t.Fatalf("the log stream does not carry the marker; it reads:\n%s", logged.body())
	}
	t.Logf("StreamTaskLogs delivered the marker in %d chunk(s)", logged.count())

	// ---- and it is NOWHERE in the run's Temporal history -------------------
	//
	// Every event payload of the whole history is rendered and scanned: the
	// activity inputs and results, the workflow input, the completion result,
	// and the markers/attributes around them. If a log line were carried on an
	// Event, the projection activity's input would hold it and this would find
	// it.
	events, size := historyText(ctx, t, temporalClient, runID)
	// The positive control. "The marker is absent" is worthless from a scan
	// that reads nothing, so the same text must be shown to carry the things
	// history genuinely holds: the task id and the orkflow name.
	for _, present := range []string{"emit", orkflowName} {
		if !strings.Contains(events, present) {
			t.Fatalf("the history scan does not even contain %q; it is not reading payloads, "+
				"so its verdict about the marker means nothing", present)
		}
	}
	if strings.Contains(events, marker) {
		t.Fatalf("the marker is in run %s's Temporal history; log bytes must never enter it "+
			"(AGENTS.md §1.4)", runID)
	}
	t.Logf("scanned %d bytes of run %s's Temporal history: it carries the task id and the "+
		"orkflow name, and the marker is absent", size, runID)

	// ---- nor in the archived event stream ---------------------------------
	archived := awaitArchivedEventText(ctx, t, engine, runID)
	if !strings.Contains(archived, "emit") {
		t.Fatalf("the archived event scan does not contain the task id; it is reading nothing")
	}
	if strings.Contains(archived, marker) {
		t.Fatalf("the marker is in run %s's archived event stream; the event stream must not "+
			"carry log bytes", runID)
	}
	t.Logf("scanned %d bytes of run %s's archived events: they carry the task id, and the "+
		"marker is absent", len(archived), runID)

	writeMarkerTranscript(t, namespace, runID, marker, logged, size, len(archived))
}

// ---- following ---------------------------------------------------------------

// logFollower accumulates a log stream from another goroutine.
type logFollower struct {
	mu  sync.Mutex
	got []*orkstackv1.TaskLogChunk
	at  []time.Time
}

func newLogFollower() *logFollower { return &logFollower{} }

func (f *logFollower) add(chunk *orkstackv1.TaskLogChunk) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, chunk)
	f.at = append(f.at, time.Now())
}

func (f *logFollower) chunks() []*orkstackv1.TaskLogChunk {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*orkstackv1.TaskLogChunk(nil), f.got...)
}

func (f *logFollower) stamps() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.at...)
}

func (f *logFollower) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

func (f *logFollower) body() string {
	var b strings.Builder
	for _, chunk := range f.chunks() {
		b.Write(chunk.GetData())
	}
	return b.String()
}

func followTaskLog(
	ctx context.Context,
	replica *engineReplica,
	runID, taskID string,
	into *logFollower,
) error {
	return streamTaskLogInto(ctx, replica, &orkstackv1.StreamTaskLogsRequest{
		RunId: runID, TaskId: taskID, Follow: true,
	}, into)
}

func replayTaskLog(
	ctx context.Context,
	replica *engineReplica,
	runID, taskID string,
	into *logFollower,
) error {
	return replayTaskLogFrom(ctx, replica, runID, taskID, 0, into)
}

func replayTaskLogFrom(
	ctx context.Context,
	replica *engineReplica,
	runID, taskID string,
	fromSeq int64,
	into *logFollower,
) error {
	return streamTaskLogInto(ctx, replica, &orkstackv1.StreamTaskLogsRequest{
		RunId: runID, TaskId: taskID, FromSeq: fromSeq,
	}, into)
}

// streamTaskLogInto consumes one log stream into a follower.
//
// A follower may attach before the task has been scheduled at all, which is
// the normal case for `-f` typed straight after a submit: the run exists, its
// task list does not yet, and the engine answers NotFound so a typo is not
// mistaken for a quiet task. The retry is BOUNDED and only ever taken before
// the first chunk, so nothing can be delivered twice.
func streamTaskLogInto(
	ctx context.Context,
	replica *engineReplica,
	req *orkstackv1.StreamTaskLogsRequest,
	into *logFollower,
) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		delivered, err := drainTaskLogStream(ctx, replica, req, into)
		switch {
		case err == nil:
			return nil
		case delivered || connect.CodeOf(err) != connect.CodeNotFound:
			return err
		case time.Now().After(deadline):
			return fmt.Errorf("task %q of run %q never appeared: %w",
				req.GetTaskId(), req.GetRunId(), err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// drainTaskLogStream reads one stream to its end, reporting whether it
// delivered anything before failing.
func drainTaskLogStream(
	ctx context.Context,
	replica *engineReplica,
	req *orkstackv1.StreamTaskLogsRequest,
	into *logFollower,
) (bool, error) {
	stream, err := replica.runs.StreamTaskLogs(ctx, connect.NewRequest(req))
	if err != nil {
		return false, err
	}
	defer func() { _ = stream.Close() }()

	delivered := false
	for stream.Receive() {
		chunk := stream.Msg()
		into.add(chunk)
		delivered = true
		if chunk.GetLast() {
			return delivered, nil
		}
	}
	return delivered, stream.Err()
}

// awaitLinesWhileRunning waits until the follower holds want distinct output
// lines and asserts, at that instant, that the cluster still reports the run
// RUNNING. That pairing is the whole difference between streaming and reading
// an artifact afterwards.
func awaitLinesWhileRunning(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	runID string,
	follower *logFollower,
	want int,
) []string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		lines := emittedLines(follower.body())
		if len(lines) >= want {
			status := executionStatus(ctx, t, c, runID)
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				t.Fatalf("replica B received %d lines only after the run closed (%s); "+
					"the log is not streaming", len(lines), status)
			}
			return lines
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("replica B held %d lines within the deadline, want at least %d",
		len(emittedLines(follower.body())), want)
	return nil
}

// emittedLines picks the task's own emit-NN lines out of the stream, ignoring
// the Orker's own notices (the workdir line, the shell's op line).
func emittedLines(body string) []string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "emit-") {
			out = append(out, strings.TrimSpace(line))
		}
	}
	return out
}

func lineOrder(body string) []int {
	var out []int
	for _, line := range emittedLines(body) {
		var n int
		if _, err := fmt.Sscanf(line, "emit-%d", &n); err == nil {
			out = append(out, n)
		}
	}
	return out
}

func isAscending(values []int) bool {
	for i := 1; i < len(values); i++ {
		if values[i] <= values[i-1] {
			return false
		}
	}
	return true
}

// ---- history and archive scanning ---------------------------------------------

// historyText renders every event of a run's Temporal history as text, so a
// scan covers payloads the SDK would otherwise keep as opaque bytes.
func historyText(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	runID string,
) (string, int) {
	t.Helper()
	iter := c.GetWorkflowHistory(ctx, runID, "",
		false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	marshaler := protojson.MarshalOptions{Multiline: false}
	var out strings.Builder
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history of %s: %v", runID, err)
		}
		rendered, err := marshaler.Marshal(event)
		if err != nil {
			// Fall back to the Go rendering rather than skipping the event: an
			// event this scan cannot read is an event the marker could hide in.
			out.WriteString(fmt.Sprintf("%v\n", event))
			continue
		}
		out.Write(rendered)
		out.WriteByte('\n')
	}
	text := out.String()
	if text == "" {
		t.Fatalf("run %s has an empty history; the scan proves nothing", runID)
	}
	return text, len(text)
}

// awaitArchivedEventText waits for the run archive and renders its events.
func awaitArchivedEventText(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID string,
) string {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		stream, err := replica.runs.StreamEvents(ctx, connect.NewRequest(
			&orkstackv1.StreamEventsRequest{RunId: runID}))
		if err == nil {
			var out strings.Builder
			marshaler := protojson.MarshalOptions{Multiline: false}
			for stream.Receive() {
				rendered, err := marshaler.Marshal(stream.Msg())
				if err != nil {
					t.Fatalf("render archived event: %v", err)
				}
				out.Write(rendered)
				out.WriteByte('\n')
			}
			_ = stream.Close()
			if out.Len() > 0 {
				return out.String()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s never produced a readable event stream to scan", runID)
	return ""
}

// ---- fixtures -----------------------------------------------------------------

// slowEmittingOrkflowYAML emits one line a second for ten seconds. The pace is
// the point: it is slower than any flush interval, so a chunk exists long
// before the task ends and a reader that only saw the end would see nothing.
func slowEmittingOrkflowYAML(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M10.5 slow-emitting task, followed live from a second replica.
tasks:
  - id: emit
    plugin: shell
    timeout_seconds: 120
    with:
      op: exec
      command: |
        for i in 01 02 03 04 05 06 07 08 09 10; do
          echo "emit-$i"
          sleep 1
        done
      capture_stdout_as: out
`, name)
}

// markerOrkflowYAML emits one unmistakable string, once.
func markerOrkflowYAML(name, marker string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M10.5 marker task, for the no-bytes-in-history invariant.
tasks:
  - id: emit
    plugin: shell
    timeout_seconds: 60
    with:
      op: exec
      command: echo %s
      capture_stdout_as: out
`, name, marker)
}

// ---- proof transcripts ---------------------------------------------------------

// writeTaskLogTranscript records the live-follow transcript the milestone's
// proof needs: what arrived, and WHEN, against the run's own lifetime.
func writeTaskLogTranscript(
	t *testing.T,
	namespace, runID, orkflowName string,
	liveLines []string,
	live, postMortem *logFollower,
) {
	t.Helper()
	chunks := live.chunks()
	stamps := live.stamps()
	var out strings.Builder
	fmt.Fprintf(&out, "# a task's log, followed live from the replica that started nothing (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "temporal namespace     %s\n", namespace)
	fmt.Fprintf(&out, "orkflow                %s@0.1.0\n", orkflowName)
	fmt.Fprintf(&out, "run                    %s\n", runID)
	fmt.Fprintf(&out, "submitted on           replica A (ork-A)\n")
	fmt.Fprintf(&out, "followed on            replica B (ork-B), which never saw the submission\n")
	fmt.Fprintf(&out, "task                   emit — one line a second for ten seconds\n\n")
	fmt.Fprintf(&out, "the cluster still reported the workflow RUNNING when B held these lines:\n")
	for _, line := range liveLines {
		fmt.Fprintf(&out, "  %s\n", line)
	}
	fmt.Fprintf(&out, "\nchunks as B received them (wall clock, and the run's own age):\n")
	var first time.Time
	if len(stamps) > 0 {
		first = stamps[0]
	}
	for i, chunk := range chunks {
		body := strings.ReplaceAll(strings.TrimRight(string(chunk.GetData()), "\n"), "\n", " | ")
		if chunk.GetLast() {
			body = "(terminator: the task reached a terminal state)"
		}
		fmt.Fprintf(&out, "  %s  +%7.3fs  seq=%-3d attempt=%d  %s\n",
			stamps[i].UTC().Format("15:04:05.000"),
			stamps[i].Sub(first).Seconds(),
			chunk.GetSeq(), chunk.GetAttempt(), body)
	}
	span := time.Duration(0)
	if len(stamps) > 1 {
		span = stamps[len(stamps)-1].Sub(stamps[0])
	}
	fmt.Fprintf(&out, "\nthe follow spanned %s: the lines arrived DURING the run, not after it.\n", span)
	fmt.Fprintf(&out, "post-mortem read from the same replica: %d chunk(s), %d bytes.\n",
		postMortem.count(), len(postMortem.body()))
	appendProofArtifact(t, "live-follow.txt", out.String())
}

// writeMarkerTranscript records what the invariant scan measured.
func writeMarkerTranscript(
	t *testing.T,
	namespace, runID, marker string,
	logged *logFollower,
	historyBytes, archivedBytes int,
) {
	t.Helper()
	var out strings.Builder
	fmt.Fprintf(&out, "# log bytes never enter run history (AGENTS.md 1.4) — %s\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "temporal namespace     %s\n", namespace)
	fmt.Fprintf(&out, "run                    %s\n", runID)
	fmt.Fprintf(&out, "marker                 %s\n\n", marker)
	fmt.Fprintf(&out, "StreamTaskLogs         delivered it in %d chunk(s), %d bytes\n",
		logged.count(), len(logged.body()))
	fmt.Fprintf(&out, "Temporal history       %d bytes scanned, marker ABSENT\n", historyBytes)
	fmt.Fprintf(&out, "archived event stream  %d bytes scanned, marker ABSENT\n", archivedBytes)
	fmt.Fprintf(&out, "\nthe log the task wrote, as StreamTaskLogs delivered it:\n")
	for _, line := range strings.Split(strings.TrimRight(logged.body(), "\n"), "\n") {
		fmt.Fprintf(&out, "  %s\n", line)
	}
	appendProofArtifact(t, "no-bytes-in-history.txt", out.String())
}
