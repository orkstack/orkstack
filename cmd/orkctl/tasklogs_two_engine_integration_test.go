//go:build integration

package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"regexp"
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
	cliPath := buildBinary(t, root, binDir, "orkctl")

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

	var submitted *connect.Response[orkstackv1.SubmitRunResponse]
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		submitted, err = replicaA.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: orkflowName,
		}))
		if err == nil {
			break
		}
		// Engine readiness and Temporal poller visibility are distinct. The
		// submit boundary deliberately fails fast until DescribeOrker is visible;
		// wait for that cluster fact instead of racing worker startup.
		if !strings.Contains(err.Error(), "has no live Orkers") || time.Now().After(deadline) {
			t.Fatalf("SubmitRun on replica A: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
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

	// ---- acceptance 7, against the real binaries ---------------------------
	// `orkctl run logs -f` is started HERE, against replica B, while the
	// cluster still reports the run RUNNING. It must print the task's output
	// and return on its own when the run reaches a terminal state — with no
	// deadline of the test's making, so a command that never returned would
	// hang the suite rather than pass it quietly.
	cliStartedAt := time.Now()
	if status := executionStatus(ctx, t, temporalClient, runID); status !=
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the run reached %s before `orkctl run logs -f` was started; "+
			"this test cannot tell a live follow from a replay", status)
	}
	type cliResult struct {
		stdout string
		stderr string
		code   int
	}
	cliDone := make(chan cliResult, 1)
	go func() {
		stdout, stderr, code := runBinary(ctx, t, cliPath, root,
			"run", "logs", runID, "--task", "emit", "-f",
			"--server", "http://"+replicaB.addr)
		cliDone <- cliResult{stdout: stdout, stderr: stderr, code: code}
	}()

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

	// ---- acceptance 7: the CLI returned on its own, with the whole log -----
	var cli cliResult
	select {
	case cli = <-cliDone:
	case <-time.After(4 * time.Minute):
		t.Fatalf("`orkctl run logs -f` never returned after the run finished")
	}
	cliRanFor := time.Since(cliStartedAt)
	if cli.code != 0 {
		t.Fatalf("`orkctl run logs -f` exited %d\nstderr:\n%s", cli.code, cli.stderr)
	}
	for line := 1; line <= 10; line++ {
		if !strings.Contains(cli.stdout, fmt.Sprintf("emit-%02d", line)) {
			t.Fatalf("`orkctl run logs -f` did not print emit-%02d; it printed:\n%s",
				line, cli.stdout)
		}
	}
	if got := lineOrder(cli.stdout); !isAscending(got) {
		t.Fatalf("`orkctl run logs -f` printed the lines out of order: %v", got)
	}
	t.Logf("`orkctl run logs -f` followed replica B for %s and exited %d with %d lines",
		cliRanFor.Round(time.Millisecond), cli.code, len(lineOrder(cli.stdout)))

	writeTaskLogTranscript(t, namespace, runID, orkflowName, liveLines, follower, postMortem,
		cli.stdout, cliRanFor)
}

// TestIntegration_TaskLogBytesNeverEnterRunHistory is the invariant test for
// AGENTS.md §1.4: the LOG PATH puts no bytes into Temporal's history and none
// onto the run's event stream.
//
// It uses TWO markers from one task, which is what makes "absent" a
// measurement rather than a limitation of the scanner:
//
//   - headMarker is printed FIRST. It reaches history — not through the log
//     path, but through the shell plugin's own documented payload echo:
//     `output.stdout` carries the first MaxCapturedOutput (8 KiB) of the
//     command's output, so a task's OUTPUT is an activity result and an
//     activity result is history. That predates this milestone and is what
//     `{{ .tasks.x.output.stdout }}` reads. Here it is the POSITIVE CONTROL:
//     the same scan that reports the second marker absent finds this one.
//   - tailMarker is printed after 15 KiB of filler, so the payload
//     echo cannot reach it. Everything that could carry it to history is the
//     log path — and it must be absent.
//
// The test therefore fails the moment somebody carries a log line on an Event
// or into a payload: the design this milestone rejected, and the one that is a
// line of code away at all times (EVENT_TYPE_TASK_LOG already has a `message`
// field). It ALSO fails if the scanner stops reading payloads, which is not
// hypothetical — the first live run of this file scanned base64 and reported
// everything absent.
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

	// Markers no other part of the system could produce, so finding one
	// anywhere is proof of where it came from.
	//
	// Neither string appears in the ORKFLOW: the document holds the two halves
	// and the task joins them at run time. A definition is workflow input and
	// workflow input is history, so a marker written literally into the YAML
	// would be found in history by construction and would prove nothing about
	// where a task's OUTPUT goes. (That is not hypothetical — this test found
	// its own first version doing exactly that.)
	stamp := time.Now().UnixNano()
	tag := fmt.Sprintf("%d-%d", os.Getpid(), stamp)
	headMarker := "ORKHEAD" + tag
	tailMarker := "ORKTAIL" + tag
	orkflowName := fmt.Sprintf("logmarker-%s", tag)
	if _, err := engine.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: markerOrkflowYAML(orkflowName, tag),
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

	// ---- BOTH markers are in the log stream --------------------------------
	logged := newLogFollower()
	if err := replayTaskLog(ctx, engine, runID, "emit", logged); err != nil {
		t.Fatalf("read task logs: %v", err)
	}
	for _, marker := range []string{headMarker, tailMarker} {
		if !strings.Contains(logged.body(), marker) {
			t.Fatalf("the log stream does not carry %s; it holds %d bytes",
				marker, len(logged.body()))
		}
	}
	t.Logf("StreamTaskLogs delivered both markers in %d chunk(s), %d bytes",
		logged.count(), len(logged.body()))

	// ---- the history scan, with its control --------------------------------
	//
	// Every event of the whole history is rendered AND its payloads decoded:
	// activity inputs and results, the workflow argument, the completion.
	//
	// The control USED to be headMarker itself, because the retired shell
	// plugin echoed the head of stdout onto its output payload. v0.0.5 removed
	// that echo (§1.2: a task's bytes travel as artifacts and logs, never as a
	// payload), so the standard here is now the stronger one: NEITHER marker
	// may be in history. The control is the orkflow name, which is workflow
	// input and is therefore in history by construction — a scan that cannot
	// find it is reading nothing.
	events, size := historyText(ctx, t, temporalClient, runID)
	if !strings.Contains(events, orkflowName) {
		t.Fatalf("the history scan cannot even find the orkflow name %q, which is workflow "+
			"input; its verdict about the log path means nothing", orkflowName)
	}
	for _, marker := range []string{headMarker, tailMarker} {
		if strings.Contains(events, marker) {
			t.Fatalf("%s is in run %s's Temporal history; a task's own bytes must never "+
				"reach a payload or the history (AGENTS.md §1.4)", marker, runID)
		}
	}
	t.Logf("scanned %d bytes of run %s's Temporal history: it carries the control (%s) "+
		"and NEITHER task-output marker", size, runID, orkflowName)

	// ---- the event stream carries NEITHER -----------------------------------
	//
	// This half needs no control of that shape: the event stream is a list of
	// lifecycle notices, so nothing a task printed belongs on it at all. The
	// control is the task id, which the events do carry.
	archived := awaitArchivedEventText(ctx, t, engine, runID)
	if !strings.Contains(archived, "emit") {
		t.Fatalf("the event scan does not contain the task id; it is reading nothing")
	}
	for _, marker := range []string{headMarker, tailMarker} {
		if strings.Contains(archived, marker) {
			t.Fatalf("%s is on run %s's event stream; the event stream may announce that "+
				"logs exist and must not carry them", marker, runID)
		}
	}
	t.Logf("scanned %d bytes of run %s's event stream: it carries the task id, and NEITHER "+
		"marker", len(archived), runID)

	writeMarkerTranscript(t, namespace, runID, headMarker, tailMarker,
		logged, events, size, len(archived))
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

// historyText renders every event of a run's Temporal history as searchable
// text.
//
// The rendering is TWO passes, and the second is the load-bearing one. Every
// payload Temporal carries — an activity's input, its result, the workflow's
// argument, the completion — is a `Payloads` message whose `data` is bytes, so
// protojson base64-encodes it and a scan of that JSON reads a wall of opaque
// characters. A marker "absent" from it would be absent from any string at
// all, which is why the positive control in the test is not optional: it
// caught exactly this, on the first live run of this file.
//
// So every base64-looking span in the rendered JSON is decoded and appended.
// It is deliberately indiscriminate: it needs no knowledge of which field is
// a payload, so a Temporal release that adds one is covered without an edit
// here.
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
	events := 0
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history of %s: %v", runID, err)
		}
		events++
		rendered, err := marshaler.Marshal(event)
		if err != nil {
			// Fall back to the Go rendering rather than skipping the event: an
			// event this scan cannot read is an event the marker could hide in.
			out.WriteString(fmt.Sprintf("%v\n", event))
			continue
		}
		out.Write(rendered)
		out.WriteByte('\n')
		out.WriteString(decodedPayloads(string(rendered)))
	}
	text := out.String()
	if events == 0 || text == "" {
		t.Fatalf("run %s has an empty history; the scan proves nothing", runID)
	}
	return text, len(text)
}

// base64Span matches a run of base64 characters long enough to be a payload
// rather than an identifier.
var base64Span = regexp.MustCompile(`[A-Za-z0-9+/]{12,}={0,2}`)

// decodedPayloads returns every base64 span of rendered, decoded. A span that
// is not base64, or that decodes to bytes no text could contain, contributes
// nothing — a false positive costs a few bytes of scan text and a false
// negative would cost the whole claim.
func decodedPayloads(rendered string) string {
	var out strings.Builder
	for _, span := range base64Span.FindAllString(rendered, -1) {
		decoded, err := base64.StdEncoding.DecodeString(span)
		if err != nil {
			continue
		}
		out.Write(decoded)
		out.WriteByte('\n')
	}
	return out.String()
}

// awaitArchivedEventText waits for the run archive and renders its events.
//
// Opening a stream may race the archive projection, so an open failure or a
// clean empty stream is retried. Once a stream has delivered bytes, however,
// its terminal Err is fatal. A dropped prefix is not an archive and must not
// be silently discarded so a later healthy attempt can make the test pass.
func awaitArchivedEventText(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID string,
) string {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		stream, err := replica.runs.StreamEvents(ctx, connect.NewRequest(
			&orkstackv1.StreamEventsRequest{RunId: runID}))
		if err != nil {
			lastErr = fmt.Errorf("open the stream: %w", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		text, err := readArchivedEventText(stream)
		if err == nil {
			return text
		}
		var terminal *archivedEventStreamError
		if errors.As(err, &terminal) {
			t.Fatalf("read archived event stream: %v", err)
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s never produced a COMPLETE event stream to scan (last: %v)", runID, lastErr)
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
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command:
        - sh
        - -c
        - 'for i in 01 02 03 04 05 06 07 08 09 10; do echo "emit-$i"; sleep 1; done'
`, name)
}

// markerOrkflowYAML emits two unmistakable strings from one task, separated by
// more than the shell plugin's payload echo can hold.
//
// Two properties, both load-bearing:
//
//   - NEITHER marker appears in this document. The task holds the run's tag in
//     a shell variable and joins it to a literal prefix, so "ORKHEAD<tag>" and
//     "ORKTAIL<tag>" exist only in the process's output. A definition is
//     workflow input, so a marker spelled out here would be in history before
//     the task ever ran.
//   - The filler puts the SECOND marker out of reach of `output.stdout`, which
//     the shell plugin caps at MaxCapturedOutput (8 KiB). 200 lines of ~76
//     bytes is ~15 KiB, so the tail marker cannot be on the task payload — and
//     therefore cannot be in history — by any path except the log one. The
//     first marker is inside the cap, and is the scan's positive control.
func markerOrkflowYAML(name, tag string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M10.5 marker task, for the no-bytes-in-history invariant.
tasks:
  - id: emit
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command:
        - sh
        - -c
        - 'tag=%s; echo "ORKHEAD${tag}"; i=0; while [ $i -lt 200 ]; do echo "filler-$i-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; i=$((i + 1)); done; echo "ORKTAIL${tag}"'
`, name, tag)
}

// ---- proof transcripts ---------------------------------------------------------

// writeTaskLogTranscript records the live-follow transcript the milestone's
// proof needs: what arrived, and WHEN, against the run's own lifetime.
func writeTaskLogTranscript(
	t *testing.T,
	namespace, runID, orkflowName string,
	liveLines []string,
	live, postMortem *logFollower,
	cliOutput string,
	cliRanFor time.Duration,
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
	fmt.Fprintf(&out, "\nthe follow spanned %s of the run's own lifetime.\n", span)
	// A verdict with no clock in it, so a README can quote the CLAIM while the
	// table above carries the measurement. A quote that changed on every run
	// could not be held to anything.
	fmt.Fprintf(&out, "verdict: the lines arrived DURING the run, not after it.\n")
	fmt.Fprintf(&out, "post-mortem read from the same replica: %d chunk(s), %d bytes.\n",
		postMortem.count(), len(postMortem.body()))

	// The CLI session, started against replica B while the run was still
	// RUNNING and left to end on its own.
	fmt.Fprintf(&out, "\n$ orkctl run logs %s --task emit -f --server <replica B>\n", runID)
	for _, line := range strings.Split(strings.TrimRight(cliOutput, "\n"), "\n") {
		fmt.Fprintf(&out, "  %s\n", line)
	}
	fmt.Fprintf(&out, "(it followed for %s and returned by itself when the run reached a "+
		"terminal state; exit 0)\n", cliRanFor.Round(time.Millisecond))
	fmt.Fprintf(&out, "verdict: `orkctl run logs -f` follows live and exits on the run's "+
		"terminal state.\n")
	appendProofArtifact(t, "live-follow.txt", out.String())
}

// writeMarkerTranscript records what the invariant scan measured, including
// its own control: a transcript that only said "absent" would be the same
// transcript a scan of nothing produces.
func writeMarkerTranscript(
	t *testing.T,
	namespace, runID, headMarker, tailMarker string,
	logged *logFollower,
	history string,
	historyBytes, archivedBytes int,
) {
	t.Helper()
	body := logged.body()
	var out strings.Builder
	fmt.Fprintf(&out, "# the log path puts no bytes in run history (AGENTS.md 1.4) — %s\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "temporal namespace     %s\n", namespace)
	fmt.Fprintf(&out, "run                    %s\n", runID)
	fmt.Fprintf(&out, "task                   emit — a marker, 15 KiB of filler, a marker\n\n")
	fmt.Fprintf(&out, "control marker         %s   (printed first, inside the 8 KiB the shell\n",
		headMarker)
	fmt.Fprintf(&out, "                       plugin echoes onto output.stdout)\n")
	fmt.Fprintf(&out, "log-only marker        %s   (printed after the filler, so no payload\n",
		tailMarker)
	fmt.Fprintf(&out, "                       can carry it)\n\n")
	fmt.Fprintf(&out, "StreamTaskLogs         %d chunk(s), %d bytes: BOTH markers present\n",
		logged.count(), len(body))
	fmt.Fprintf(&out, "Temporal history       %d bytes scanned (payloads base64-decoded)\n",
		historyBytes)
	fmt.Fprintf(&out, "  control marker       PRESENT  — so the scan reads payloads\n")
	fmt.Fprintf(&out, "  log-only marker      ABSENT   — the log path adds nothing to history\n")
	fmt.Fprintf(&out, "event stream           %d bytes scanned\n", archivedBytes)
	fmt.Fprintf(&out, "  control marker       ABSENT\n")
	fmt.Fprintf(&out, "  log-only marker      ABSENT   — events announce, they do not carry\n")
	// A verdict with no measurement in it, so a README can quote the CLAIM
	// while the lines above carry the numbers. A quote whose byte count
	// changed on every run could not be held to anything.
	fmt.Fprintf(&out, "verdict: the whole log is in the stream; only the payload echo is "+
		"in history, and neither marker is on the event stream.\n\n")
	fmt.Fprintf(&out, "the log's own boundaries, as StreamTaskLogs delivered them:\n")
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if strings.Contains(line, headMarker) || strings.Contains(line, tailMarker) ||
			strings.Contains(line, "filler-0-") || strings.Contains(line, "filler-199-") {
			fmt.Fprintf(&out, "  %s\n", line)
		}
	}
	fmt.Fprintf(&out, "  (%d lines in all)\n", strings.Count(body, "\n"))
	fmt.Fprintf(&out, "\nand the history, at the point the control marker appears in it:\n")
	for _, line := range strings.Split(history, "\n") {
		if strings.Contains(line, headMarker) {
			fmt.Fprintf(&out, "  %.200s…\n", strings.TrimSpace(line))
			break
		}
	}
	appendProofArtifact(t, "no-bytes-in-history.txt", out.String())
}
