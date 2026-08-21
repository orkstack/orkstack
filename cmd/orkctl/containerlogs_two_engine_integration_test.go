//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// M11.1's falsifying tests: the CONTAINER arm of the two claims M10.5 made for
// the shell plugin, which the container plugin did not honor.
//
// They run the production topology of tasklogs_two_engine_integration_test.go
// — two real `ork` processes and one real `orker`, sharing one Temporal
// namespace, one execution channel and one MinIO bucket, the deliberate
// exception to the per-agent isolation rule (AGENTS.md §6) because sharing is
// the condition under test — and add a real Docker daemon.

// TestIntegration_ContainerApplicationOutputIsReadableBeforeTheRunEnds is the
// milestone's central claim.
//
// A container prints one line a second on EACH stream for ten seconds. It is
// submitted to replica A; every log read is issued against replica B, which
// accepted no submission, executed none of this run's activities and has
// nothing about it in memory. The assertion is about TIME: the application's
// own lines must arrive at B while the cluster still reports the workflow
// RUNNING. Before this milestone the container plugin wrote both streams to
// capture files only, so B would have received the plugin's lifecycle lines
// and nothing the application said.
func TestIntegration_ContainerApplicationOutputIsReadableBeforeTheRunEnds(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)
	requireDockerDaemon(t)
	image := containerTestImage()
	requireContainerImage(t, image)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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

	orkflowName := fmt.Sprintf("containerlogs-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: chattyContainerOrkflowYAML(orkflowName, image),
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
	go func() { followDone <- followTaskLog(ctx, replicaB, runID, "chatty", follower) }()

	// ---- the assertion that separates streaming from a post-mortem read -----
	liveLines := awaitContainerLinesWhileRunning(ctx, t, temporalClient, runID, follower, 3)
	t.Logf("replica B held %d ordered application lines while the cluster reported RUNNING: %q",
		len(liveLines), liveLines)

	// ---- acceptance 7, against the real binaries ---------------------------
	// `orkctl run logs -f` is started HERE, against replica B, while the
	// cluster still reports the run RUNNING, with no deadline of the test's
	// making: a command that never returned would hang the suite rather than
	// pass it quietly. Its resume flags are untouched by this milestone, which
	// is the point of an in-band marker.
	cliStartedAt := time.Now()
	if status := executionStatus(ctx, t, temporalClient, runID); status !=
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the run reached %s before `orkctl run logs -f` was started; "+
			"this test cannot tell a live follow from a replay", status)
	}
	type cliResult struct {
		stdout string
		code   int
	}
	cliDone := make(chan cliResult, 1)
	go func() {
		stdout, _, code := runBinary(ctx, t, cliPath, root,
			"run", "logs", runID, "--task", "chatty", "-f",
			"--server", "http://"+replicaB.addr)
		cliDone <- cliResult{stdout: stdout, code: code}
	}()

	// ---- the stream must end on its own when the task finishes -------------
	select {
	case err := <-followDone:
		if err != nil {
			t.Fatalf("following the container task log on replica B: %v", err)
		}
	case <-time.After(5 * time.Minute):
		t.Fatalf("replica B's log stream never ended; it holds %d chunks", follower.count())
	}
	awaitWorkflowCompleted(ctx, t, temporalClient, runID)

	// ---- ordering is total and gap-free (M10.5, unchanged) -----------------
	chunks := follower.chunks()
	for i, chunk := range chunks {
		if want := int64(i + 1); chunk.GetSeq() != want && !chunk.GetLast() {
			t.Fatalf("chunk %d carries seq %d, want %d: the stream has a gap or a reorder",
				i, chunk.GetSeq(), want)
		}
	}

	// ---- both streams arrived, and they are still told apart ---------------
	streamed := follower.body()
	for line := 1; line <= 10; line++ {
		want := fmt.Sprintf("cout-%02d", line)
		if n := countWholeLines(streamed, want); n != 1 {
			t.Fatalf("the followed log carries %q %d time(s), want exactly 1; it reads:\n%s",
				want, n, streamed)
		}
		marked := fmt.Sprintf("%scerr-%02d", plugin.StderrMarker, line)
		if n := countWholeLines(streamed, marked); n != 1 {
			t.Fatalf("the followed log carries %q %d time(s), want exactly 1; it reads:\n%s",
				marked, n, streamed)
		}
	}
	if got := containerLineOrder(streamed, "cout-"); !isAscending(got) {
		t.Fatalf("the container's stdout lines arrived out of order: %v", got)
	}
	if got := containerLineOrder(streamed, plugin.StderrMarker+"cerr-"); !isAscending(got) {
		t.Fatalf("the container's stderr lines arrived out of order: %v", got)
	}

	// ---- and the whole log is readable afterwards, from the same replica ---
	postMortem := newLogFollower()
	if err := replayTaskLog(ctx, replicaB, runID, "chatty", postMortem); err != nil {
		t.Fatalf("post-mortem read on replica B: %v", err)
	}
	for line := 1; line <= 10; line++ {
		if !strings.Contains(postMortem.body(), fmt.Sprintf("cout-%02d", line)) {
			t.Fatalf("the post-mortem read is missing cout-%02d; it reads:\n%s",
				line, postMortem.body())
		}
	}

	// ---- resume replays nothing twice and misses nothing (unchanged) -------
	resumeAt := chunks[0].GetSeq()
	resumed := newLogFollower()
	if err := replayTaskLogFrom(ctx, replicaB, runID, "chatty", resumeAt, resumed); err != nil {
		t.Fatalf("resumed read on replica B: %v", err)
	}
	for _, chunk := range resumed.chunks() {
		if chunk.GetSeq() <= resumeAt && !chunk.GetLast() {
			t.Fatalf("resuming after seq %d replayed seq %d", resumeAt, chunk.GetSeq())
		}
	}

	// ---- acceptance 3: the artifacts and the payload are unchanged ---------
	assertContainerRunOutput(ctx, t, replicaB, runID)

	// ---- acceptance 7: the CLI returned on its own, with the whole log -----
	var cli cliResult
	select {
	case cli = <-cliDone:
	case <-time.After(5 * time.Minute):
		t.Fatalf("`orkctl run logs -f` never returned after the run finished")
	}
	cliRanFor := time.Since(cliStartedAt)
	if cli.code != 0 {
		t.Fatalf("`orkctl run logs -f` exited %d; it printed:\n%s", cli.code, cli.stdout)
	}
	for line := 1; line <= 10; line++ {
		for _, want := range []string{
			fmt.Sprintf("cout-%02d", line),
			fmt.Sprintf("%scerr-%02d", plugin.StderrMarker, line),
		} {
			if !strings.Contains(cli.stdout, want) {
				t.Fatalf("`orkctl run logs -f` did not print %q; it printed:\n%s", want, cli.stdout)
			}
		}
	}
	if got := containerLineOrder(cli.stdout, "cout-"); !isAscending(got) {
		t.Fatalf("`orkctl run logs -f` printed the lines out of order: %v", got)
	}
	t.Logf("`orkctl run logs -f` followed replica B for %s and exited %d",
		cliRanFor.Round(time.Millisecond), cli.code)

	writeContainerLogTranscript(t, namespace, runID, orkflowName, image,
		liveLines, follower, postMortem, cli.stdout, cliRanFor)
}

// TestIntegration_ContainerTaskLogBytesNeverEnterRunHistory is the container
// arm of M10.5's invariant test for AGENTS.md §1.4 (the shell arm is
// TestIntegration_TaskLogBytesNeverEnterRunHistory, in
// tasklogs_two_engine_integration_test.go, whose helpers this reuses).
//
// Streaming the container's output added a consumer of those bytes, and the
// invariant it must not cross is that the LOG PATH puts no bytes into
// Temporal's history and none onto the run's event stream.
//
// Since M11.3 the container payload carries no stream bytes either, so the
// standard this test holds the run to is the strongest one available: NOT ONE
// of its markers is in history, by any path. Three markers and one control:
//
//   - the CONTROL is `image_pulled`, a KEY of the container plugin's output
//     payload and of nothing else in the system. Finding it proves the scan
//     reached and decoded the activity result — the payload a leak would be
//     in — so "the markers are absent" is a measurement rather than a
//     limitation of the scanner. It is a schema key, not a byte the container
//     wrote, which is exactly the distinction under test.
//   - headMarker is printed FIRST on stdout, inside every buffer the plugin
//     holds. It is the marker a regression would put back, and it must be
//     absent.
//   - tailMarker is printed on stdout after 15 KiB of filler, past every
//     buffer, so only the log path could ever carry it. Absent.
//   - errMarker is the same on STDERR: the stream this milestone newly
//     streams, absent from history exactly as the stdout one is.
func TestIntegration_ContainerTaskLogBytesNeverEnterRunHistory(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)
	requireDockerDaemon(t)
	image := containerTestImage()
	requireContainerImage(t, image)

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
	engine := startReplica(ctx, t, "ork-history", enginePath, address, namespace, queue)
	engine.awaitReady(ctx, t)

	// Markers no other part of the system could produce. Neither the prefixes
	// nor the joined strings appear in the ORKFLOW: the document holds the tag
	// alone and the container joins it to a literal prefix at run time. A
	// definition is workflow input and workflow input is history, so a marker
	// written literally into the YAML would be found in history by
	// construction and would prove nothing about where a task's OUTPUT goes.
	tag := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	headMarker := "ORKCHEAD" + tag
	tailMarker := "ORKCTAIL" + tag
	errMarker := "ORKCERR" + tag
	orkflowName := fmt.Sprintf("containermarker-%s", tag)
	if _, err := engine.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: containerMarkerOrkflowYAML(orkflowName, image, tag),
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

	// ---- ALL THREE markers are in the log stream ---------------------------
	logged := newLogFollower()
	if err := replayTaskLog(ctx, engine, runID, "marker", logged); err != nil {
		t.Fatalf("read task logs: %v", err)
	}
	for _, marker := range []string{headMarker, tailMarker} {
		if !strings.Contains(logged.body(), marker) {
			t.Fatalf("the log stream does not carry %s; it holds %d bytes",
				marker, len(logged.body()))
		}
	}
	// The stderr marker is there AND marked, which is the wire shape a reader
	// tells the two streams apart by.
	if !strings.Contains(logged.body(), plugin.StderrMarker+errMarker) {
		t.Fatalf("the log stream does not carry %q; it holds %d bytes",
			plugin.StderrMarker+errMarker, len(logged.body()))
	}
	t.Logf("StreamTaskLogs delivered all three markers in %d chunk(s), %d bytes",
		logged.count(), len(logged.body()))

	// ---- the history scan, with its control --------------------------------
	events, size := historyText(ctx, t, temporalClient, runID)
	const payloadControl = "image_pulled"
	if !strings.Contains(events, payloadControl) {
		t.Fatalf("the history scan cannot even find %q, a key of the container plugin's "+
			"own output payload; it is not reaching the activity result, so its "+
			"verdict about the markers means nothing", payloadControl)
	}
	for _, marker := range []string{headMarker, tailMarker, errMarker} {
		if strings.Contains(events, marker) {
			t.Fatalf("%s is in run %s's Temporal history; neither the log path nor the "+
				"output payload may put a task's bytes there (AGENTS.md §1.2, §1.4)",
				marker, runID)
		}
	}
	t.Logf("scanned %d bytes of run %s's Temporal history: it carries the payload's own "+
		"keys (%s) and NONE of the container's bytes (%s, %s, %s)",
		size, runID, payloadControl, headMarker, tailMarker, errMarker)

	// ---- the event stream carries NONE of them ------------------------------
	archived := awaitArchivedEventText(ctx, t, engine, runID)
	if !strings.Contains(archived, "marker") {
		t.Fatalf("the event scan does not contain the task id; it is reading nothing")
	}
	for _, marker := range []string{headMarker, tailMarker, errMarker} {
		if strings.Contains(archived, marker) {
			t.Fatalf("%s is on run %s's event stream; the event stream may announce that "+
				"logs exist and must not carry them", marker, runID)
		}
	}
	t.Logf("scanned %d bytes of run %s's event stream: it carries the task id, and NONE "+
		"of the three markers", len(archived), runID)

	writeContainerMarkerTranscript(t, namespace, runID, image,
		headMarker, tailMarker, errMarker, logged, events, size, len(archived))
}

// TestIntegration_AFailingContainerPutsNoOutputInRunHistory is the arm the
// test above cannot reach: it runs a container that SUCCEEDS, and a successful
// container returns a payload while a failing one returns an ERROR.
//
// That error is copied further than any payload ever is. Temporal records it
// as the activity's failure, the workflow copies it onto TaskRun.Error and the
// TASK_FAILED event, and the run's own failure copies it again onto Run.Error
// and RUN_FAILED (internal/engine/workflow.go) — history and the event stream
// both, once per attempt. So a plugin that quoted a failing container's output
// into its error would be piping a task's output stream through exactly the
// two places AGENTS.md §1.4 forbids, on the path a chatty container is most
// likely to take. It used to: the plugin quoted the head of stderr.
//
// The rule this measures: a failing container's output reaches history and the
// event stream NOWHERE and by NO path, while the whole of it is in the task's
// log. Four markers and one control:
//
//   - the CONTROL is the failure's own sentence, "exited with code 42". It is
//     in history and on the event stream by construction, so a scan that
//     cannot find it is reading nothing and its verdict about the markers
//     would mean nothing.
//   - outHeadMarker and errHeadMarker are printed FIRST on each stream, inside
//     every buffer the plugin holds — the 8 KiB payload echo and the head the
//     error used to quote. They are the ones a regression would put back.
//   - tailMarker and errMarker are printed after 15 KiB of filler per stream,
//     past every buffer, so only the log path could ever carry them.
func TestIntegration_AFailingContainerPutsNoOutputInRunHistory(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)
	requireDockerDaemon(t)
	image := containerTestImage()
	requireContainerImage(t, image)

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
	engine := startReplica(ctx, t, "ork-failure", enginePath, address, namespace, queue)
	engine.awaitReady(ctx, t)

	tag := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	errHeadMarker := "ORKFERRHEAD" + tag
	outHeadMarker := "ORKFOUTHEAD" + tag
	tailMarker := "ORKFTAIL" + tag
	errMarker := "ORKFERR" + tag
	orkflowName := fmt.Sprintf("containerfail-%s", tag)
	if _, err := engine.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: failingContainerOrkflowYAML(orkflowName, image, tag),
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
	awaitWorkflowFailed(ctx, t, temporalClient, runID)

	// ---- every marker is in the log stream ---------------------------------
	logged := newLogFollower()
	if err := replayTaskLog(ctx, engine, runID, "boom", logged); err != nil {
		t.Fatalf("read task logs: %v", err)
	}
	for _, marker := range []string{outHeadMarker, tailMarker} {
		if !strings.Contains(logged.body(), marker) {
			t.Fatalf("the log stream does not carry %s; it holds %d bytes",
				marker, len(logged.body()))
		}
	}
	for _, marker := range []string{errHeadMarker, errMarker} {
		if !strings.Contains(logged.body(), plugin.StderrMarker+marker) {
			t.Fatalf("the log stream does not carry %q; it holds %d bytes",
				plugin.StderrMarker+marker, len(logged.body()))
		}
	}
	t.Logf("StreamTaskLogs delivered all four markers of the FAILED task in %d chunk(s), %d bytes",
		logged.count(), len(logged.body()))

	// ---- the failure is reported, and carries none of the output -----------
	const control = "exited with code 42"
	taskError, runError := runFailureMessages(ctx, t, engine, runID, "boom")
	for label, message := range map[string]string{
		"the task's error": taskError,
		"the run's error":  runError,
	} {
		if !strings.Contains(message, control) {
			t.Fatalf("%s does not name the container's exit code: %q", label, message)
		}
		if !strings.Contains(message, "orkctl run logs") {
			t.Errorf("%s does not say where the container's output is: %q", label, message)
		}
		for _, marker := range []string{outHeadMarker, errHeadMarker, tailMarker, errMarker} {
			if strings.Contains(message, marker) {
				t.Errorf("%s quotes %s, which the container printed: %q", label, marker, message)
			}
		}
	}

	// ---- the history scan, with its control --------------------------------
	events, size := historyText(ctx, t, temporalClient, runID)
	if !strings.Contains(events, control) {
		t.Fatalf("the history scan cannot find the failure's own sentence (%q), which the "+
			"activity error puts there; its verdict about the markers means nothing", control)
	}
	for _, marker := range []string{outHeadMarker, errHeadMarker, tailMarker, errMarker} {
		if strings.Contains(events, marker) {
			t.Fatalf("%s is in failed run %s's Temporal history; a failing container's "+
				"output belongs to the log stream and the artifacts (AGENTS.md §1.4)",
				marker, runID)
		}
	}
	t.Logf("scanned %d bytes of failed run %s's Temporal history: it carries the failure's "+
		"own sentence (%q) and not one byte the container wrote (%s, %s, %s, %s)",
		size, runID, control, outHeadMarker, errHeadMarker, tailMarker, errMarker)

	// ---- the event stream announces the reason, and carries no output ------
	archived := awaitArchivedEventText(ctx, t, engine, runID)
	if !strings.Contains(archived, control) {
		t.Fatalf("the event scan does not carry the run's failure reason (%q); it is "+
			"reading nothing, or a failed run announces nothing", control)
	}
	for _, marker := range []string{outHeadMarker, errHeadMarker, tailMarker, errMarker} {
		if strings.Contains(archived, marker) {
			t.Fatalf("%s is on failed run %s's event stream; events announce a REASON "+
				"and never a task's output", marker, runID)
		}
	}
	t.Logf("scanned %d bytes of failed run %s's event stream: the reason, and none of "+
		"the output", len(archived), runID)

	writeFailedContainerTranscript(t, namespace, runID, image,
		outHeadMarker, errHeadMarker, tailMarker, errMarker,
		logged, taskError, size, len(archived))
}

// awaitWorkflowFailed is awaitWorkflowCompleted's opposite: this run is
// EXPECTED to fail, and a run that succeeded would make every assertion below
// it vacuous.
func awaitWorkflowFailed(ctx context.Context, t *testing.T, c client.Client, runID string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	var last enumspb.WorkflowExecutionStatus
	for time.Now().Before(deadline) {
		described, err := c.DescribeWorkflowExecution(ctx, runID, "")
		if err == nil {
			last = described.GetWorkflowExecutionInfo().GetStatus()
			switch last {
			case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
				t.Logf("run %s failed on the cluster, as this test requires", runID)
				return
			case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
			default:
				t.Fatalf("run %s closed as %s, want FAILED", runID, last)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s did not fail within the deadline (last status %s)", runID, last)
}

// runFailureMessages reads back what the finished run says about its failure:
// the task's error and the run's, which are the two copies the workflow makes
// of the plugin's message.
func runFailureMessages(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID, taskID string,
) (taskError, runError string) {
	t.Helper()
	got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	run := got.Msg.GetRun()
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetTaskId() == taskID {
			taskError = taskRun.GetError().GetMessage()
		}
	}
	if taskError == "" {
		t.Fatalf("run %s has no error on task %q", runID, taskID)
	}
	runError = run.GetError().GetMessage()
	if runError == "" {
		t.Fatalf("run %s failed with no run-level error", runID)
	}
	return taskError, runError
}

// ---- docker gating ------------------------------------------------------------

// containerTestImage is small, has a shell, and is stable.
// ORK_TEST_CONTAINER_IMAGE overrides it for hosts with a private mirror.
func containerTestImage() string {
	if image := strings.TrimSpace(os.Getenv("ORK_TEST_CONTAINER_IMAGE")); image != "" {
		return image
	}
	return "alpine:3.20"
}

// requireDockerDaemon gates these tests on a reachable daemon through the
// package's infraAbsent helper, so a proof run with ORK_REQUIRE_INFRA=1 turns
// an absent daemon into a failure rather than a quiet skip (AGENTS.md §6).
func requireDockerDaemon(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "version",
		"--format", "{{.Server.Version}}").CombinedOutput()
	if err != nil {
		infraAbsent(t, "docker daemon is not reachable: %v: %s", err, strings.TrimSpace(string(out)))
	}
	t.Logf("docker server %s", strings.TrimSpace(string(out)))
}

// requireContainerImage makes sure the image is on the host before the run, so
// a registry download is not mistaken for a slow log. An image that cannot be
// obtained is an absent prerequisite like any other: under ORK_REQUIRE_INFRA
// it fails, because a proof that could not get the image measured nothing
// (see infra_required_test.go).
func requireContainerImage(t *testing.T, image string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "image", "inspect",
		"--format", "{{.Id}}", image).Run(); err == nil {
		return
	}
	if out, err := exec.CommandContext(ctx, "docker", "pull", image).CombinedOutput(); err != nil {
		infraAbsent(t, "cannot pull %s (no registry access?): %s",
			image, strings.TrimSpace(string(out)))
	}
}

// ---- following ----------------------------------------------------------------

// awaitContainerLinesWhileRunning waits until the follower holds want lines the
// CONTAINER's application printed, and asserts, at that instant, that the
// cluster still reports the run RUNNING. That pairing is the whole difference
// between streaming and reading an artifact afterwards.
func awaitContainerLinesWhileRunning(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	runID string,
	follower *logFollower,
	want int,
) []string {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		lines := containerAppLines(follower.body())
		if len(lines) >= want {
			status := executionStatus(ctx, t, c, runID)
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				t.Fatalf("replica B received %d application lines only after the run closed "+
					"(%s); the container's output is not streaming", len(lines), status)
			}
			return lines
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("replica B held %d application lines within the deadline, want at least %d; "+
		"the stream reads:\n%s", len(containerAppLines(follower.body())), want, follower.body())
	return nil
}

// containerAppLines picks the container application's own lines out of the
// stream, ignoring the plugin's lifecycle lines and the Orker's notices. Both
// streams count, marker and all.
func containerAppLines(body string) []string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "cout-") ||
			strings.HasPrefix(trimmed, plugin.StderrMarker+"cerr-") {
			out = append(out, trimmed)
		}
	}
	return out
}

// containerLineOrder returns the sequence numbers of the lines carrying prefix,
// in the order they were delivered.
func containerLineOrder(body, prefix string) []int {
	var out []int
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, prefix) {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(strings.TrimPrefix(trimmed, prefix), "%d", &n); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// countWholeLines counts the lines of body that are exactly want. Exactness is
// what makes a drop and a duplicate both visible; "contains" would count
// neither.
func countWholeLines(body, want string) int {
	n := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimRight(line, "\r") == want {
			n++
		}
	}
	return n
}

// assertContainerRunOutput reads the finished run back and checks the payload
// the container plugin has always produced: streaming added a consumer of the
// bytes and must have taken nothing away.
func assertContainerRunOutput(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID string,
) {
	t.Helper()
	got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	for _, taskRun := range got.Msg.GetRun().GetTaskRuns() {
		if taskRun.GetTaskId() != "chatty" {
			continue
		}
		output := taskRun.GetOutput().GetData().AsMap()
		for _, field := range []string{"exit_code", "image", "image_pulled", "duration_ms"} {
			if _, ok := output[field]; !ok {
				t.Errorf("the container task's output payload has no %q; it holds %v",
					field, output)
			}
		}
		if code, _ := output["exit_code"].(float64); code != 0 {
			t.Errorf("output.exit_code = %v, want 0", output["exit_code"])
		}
		// And it carries none of what the container wrote: the payload is a
		// Temporal payload (AGENTS.md §1.2). The bytes are in the log stream
		// this test just followed, and in the two artifacts below.
		for _, field := range []string{"stdout", "stderr"} {
			if _, ok := output[field]; ok {
				t.Errorf("the container task's output payload carries %q", field)
			}
		}
		names := map[string]bool{}
		for _, ref := range taskRun.GetArtifacts() {
			names[ref.GetName()] = true
		}
		for _, want := range []string{"out", "err"} {
			if !names[want] {
				t.Errorf("the container task produced no %q artifact; it produced %v", want, names)
			}
		}
		return
	}
	t.Fatalf("run %s has no task named chatty", runID)
}

// ---- fixtures -----------------------------------------------------------------

// chattyContainerOrkflowYAML runs a container that prints one line a second on
// each stream for ten seconds. The pace is the point: it is slower than any
// flush bound, so chunks exist long before the container exits and a reader
// that only saw the end would see nothing.
func chattyContainerOrkflowYAML(name, image string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M11.1 chatty container, followed live from a second replica.
tasks:
  - id: chatty
    plugin: container
    timeout_seconds: 300
    with:
      image: %s
      command: |
        for i in 01 02 03 04 05 06 07 08 09 10; do
          echo "cout-$i"
          echo "cerr-$i" 1>&2
          sleep 1
        done
      capture_stdout_as: out
      capture_stderr_as: err
      timeout_seconds: 240
`, name, image)
}

// containerMarkerOrkflowYAML prints three unmistakable strings from one
// container: one at the very start of stdout, and one at the end of each
// stream behind 200 lines of ~76-byte filler (~15 KiB).
//
// The head marker sits inside every buffer the plugin holds — the diagnostic
// head it reads to recognize a daemon failure, and the payload echo it used to
// carry. It is the marker a regression puts back. The tail markers sit past
// all of them, so the log path is the only thing that could ever carry them.
// Since M11.3 all three must be absent from history; the scan's control is a
// payload KEY rather than a marker (see the test).
func containerMarkerOrkflowYAML(name, image, tag string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M11.1 container marker task, for the no-bytes-in-history invariant.
tasks:
  - id: marker
    plugin: container
    timeout_seconds: 300
    with:
      image: %s
      command: |
        tag=%s
        echo "ORKCHEAD${tag}"
        i=0
        while [ $i -lt 200 ]; do
          echo "filler-$i-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
          echo "efill-$i-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" 1>&2
          i=$((i + 1))
        done
        echo "ORKCTAIL${tag}"
        echo "ORKCERR${tag}" 1>&2
      capture_stdout_as: out
      capture_stderr_as: err
      timeout_seconds: 240
`, name, image, tag)
}

// failingContainerOrkflowYAML is containerMarkerOrkflowYAML's failing twin: a
// marker first on each stream, 15 KiB of filler per stream, a marker last on
// each — and then a non-zero exit, which is the whole point. It fails the run,
// so the plugin's error is what reaches history and the event stream, and none
// of these four markers may travel with it.
func failingContainerOrkflowYAML(name, image, tag string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: M11.1 failing container, for the no-output-in-history invariant.
tasks:
  - id: boom
    plugin: container
    timeout_seconds: 300
    with:
      image: %s
      command: |
        tag=%s
        echo "ORKFOUTHEAD${tag}"
        echo "ORKFERRHEAD${tag}" 1>&2
        i=0
        while [ $i -lt 200 ]; do
          echo "filler-$i-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
          echo "efill-$i-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" 1>&2
          i=$((i + 1))
        done
        echo "ORKFTAIL${tag}"
        echo "ORKFERR${tag}" 1>&2
        exit 42
      capture_stdout_as: out
      capture_stderr_as: err
      timeout_seconds: 240
`, name, image, tag)
}

// ---- proof transcripts ---------------------------------------------------------

// writeContainerLogTranscript records the live-follow transcript the milestone's
// proof needs: what the container said, and WHEN, against the run's own lifetime.
func writeContainerLogTranscript(
	t *testing.T,
	namespace, runID, orkflowName, image string,
	liveLines []string,
	live, postMortem *logFollower,
	cliOutput string,
	cliRanFor time.Duration,
) {
	t.Helper()
	chunks := live.chunks()
	stamps := live.stamps()
	var out strings.Builder
	fmt.Fprintf(&out, "# a CONTAINER's own output, followed live from the replica that started "+
		"nothing (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "temporal namespace     %s\n", namespace)
	fmt.Fprintf(&out, "orkflow                %s@0.1.0\n", orkflowName)
	fmt.Fprintf(&out, "run                    %s\n", runID)
	fmt.Fprintf(&out, "image                  %s\n", image)
	fmt.Fprintf(&out, "submitted on           replica A (ork-A)\n")
	fmt.Fprintf(&out, "followed on            replica B (ork-B), which never saw the submission\n")
	fmt.Fprintf(&out, "task                   chatty — a container printing one line a second on\n")
	fmt.Fprintf(&out, "                       EACH stream for ten seconds\n")
	fmt.Fprintf(&out, "wire shape             stdout bare, stderr prefixed %q\n\n", plugin.StderrMarker)
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
	fmt.Fprintf(&out, "verdict: the CONTAINER APPLICATION's lines arrived DURING the run, not "+
		"after it, and stdout and stderr stayed distinguishable.\n")
	fmt.Fprintf(&out, "post-mortem read from the same replica: %d chunk(s), %d bytes.\n",
		postMortem.count(), len(postMortem.body()))

	fmt.Fprintf(&out, "\n$ orkctl run logs %s --task chatty -f --server <replica B>\n", runID)
	for _, line := range strings.Split(strings.TrimRight(cliOutput, "\n"), "\n") {
		fmt.Fprintf(&out, "  %s\n", line)
	}
	fmt.Fprintf(&out, "(it followed for %s and returned by itself when the run reached a "+
		"terminal state; exit 0)\n", cliRanFor.Round(time.Millisecond))
	fmt.Fprintf(&out, "verdict: `orkctl run logs -f` shows a container application's output "+
		"live, with its resume flags untouched.\n")
	appendProofArtifact(t, "live-follow-container.txt", out.String())
}

// writeContainerMarkerTranscript records what the invariant scan measured,
// including its own control: a transcript that only said "absent" would be the
// same transcript a scan of nothing produces.
func writeContainerMarkerTranscript(
	t *testing.T,
	namespace, runID, image, headMarker, tailMarker, errMarker string,
	logged *logFollower,
	history string,
	historyBytes, archivedBytes int,
) {
	t.Helper()
	body := logged.body()
	var out strings.Builder
	fmt.Fprintf(&out, "# a CONTAINER's log path puts no bytes in run history (AGENTS.md 1.4) — %s\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "temporal namespace     %s\n", namespace)
	fmt.Fprintf(&out, "run                    %s\n", runID)
	fmt.Fprintf(&out, "image                  %s\n", image)
	fmt.Fprintf(&out, "task                   marker — a marker, 15 KiB of filler per stream,\n")
	fmt.Fprintf(&out, "                       then a marker on each stream\n\n")
	fmt.Fprintf(&out, "scan control           image_pulled   (a KEY of the container plugin's\n")
	fmt.Fprintf(&out, "                       output payload, so finding it proves the scan\n")
	fmt.Fprintf(&out, "                       decodes activity results — where a leak would be)\n")
	fmt.Fprintf(&out, "head marker            %s   (printed first, inside every buffer the\n", headMarker)
	fmt.Fprintf(&out, "                       plugin holds; the one a regression puts back)\n")
	fmt.Fprintf(&out, "log-only stdout marker %s   (printed after the filler, past every\n", tailMarker)
	fmt.Fprintf(&out, "                       buffer)\n")
	fmt.Fprintf(&out, "log-only stderr marker %s   (the stream this milestone newly carries)\n\n", errMarker)
	fmt.Fprintf(&out, "StreamTaskLogs         %d chunk(s), %d bytes: ALL THREE markers present,\n",
		logged.count(), len(body))
	fmt.Fprintf(&out, "                       the stderr one prefixed %q\n", plugin.StderrMarker)
	fmt.Fprintf(&out, "Temporal history       %d bytes scanned (payloads base64-decoded)\n", historyBytes)
	fmt.Fprintf(&out, "  image_pulled         PRESENT  — so the scan reads payloads\n")
	fmt.Fprintf(&out, "  every marker         ABSENT   — neither the log path nor the payload\n")
	fmt.Fprintf(&out, "                                  carries a byte the container wrote\n")
	fmt.Fprintf(&out, "event stream           %d bytes scanned\n", archivedBytes)
	fmt.Fprintf(&out, "  every marker         ABSENT   — events announce, they do not carry\n")
	fmt.Fprintf(&out, "verdict: the whole container log is in the stream, and not one of its "+
		"bytes is in history or on the event stream.\n\n")
	fmt.Fprintf(&out, "the log's own boundaries, as StreamTaskLogs delivered them:\n")
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if strings.Contains(line, headMarker) || strings.Contains(line, tailMarker) ||
			strings.Contains(line, errMarker) ||
			strings.Contains(line, "filler-0-") || strings.Contains(line, "efill-0-") {
			fmt.Fprintf(&out, "  %s\n", line)
		}
	}
	fmt.Fprintf(&out, "  (%d lines in all)\n", strings.Count(body, "\n"))
	fmt.Fprintf(&out, "\nand the history, at the point the payload's own keys appear in it:\n")
	for _, line := range strings.Split(history, "\n") {
		if strings.Contains(line, "image_pulled") {
			fmt.Fprintf(&out, "  %.200s…\n", strings.TrimSpace(line))
			break
		}
	}
	appendProofArtifact(t, "no-bytes-in-history-container.txt", out.String())
}

// writeFailedContainerTranscript records the failing arm of the same
// invariant, with the control that makes "absent" a measurement: the failure's
// own sentence is IN history and on the event stream, and not one byte of the
// container's output is in either.
func writeFailedContainerTranscript(
	t *testing.T,
	namespace, runID, image string,
	outHeadMarker, errHeadMarker, tailMarker, errMarker string,
	logged *logFollower,
	taskError string,
	historyBytes, archivedBytes int,
) {
	t.Helper()
	body := logged.body()
	var out strings.Builder
	fmt.Fprintf(&out, "# a FAILING container's output stays out of run history and the event "+
		"stream (AGENTS.md 1.4) — %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "temporal namespace     %s\n", namespace)
	fmt.Fprintf(&out, "run                    %s   (FAILED, as this test requires)\n", runID)
	fmt.Fprintf(&out, "image                  %s\n", image)
	fmt.Fprintf(&out, "task                   boom — a marker on each stream, 15 KiB of filler\n")
	fmt.Fprintf(&out, "                       per stream, a marker on each stream, exit 42\n\n")
	fmt.Fprintf(&out, "why this arm exists    a success returns a PAYLOAD; a failure returns an\n")
	fmt.Fprintf(&out, "                       ERROR, which Temporal records and the workflow\n")
	fmt.Fprintf(&out, "                       copies onto TaskRun.Error, TASK_FAILED, Run.Error\n")
	fmt.Fprintf(&out, "                       and RUN_FAILED. The plugin used to quote the head\n")
	fmt.Fprintf(&out, "                       of stderr into it.\n\n")
	fmt.Fprintf(&out, "control                \"exited with code 42\" — the failure's own sentence,\n")
	fmt.Fprintf(&out, "                       in history and on the event stream by construction\n")
	fmt.Fprintf(&out, "head markers           %s (stdout), %s (stderr)\n", outHeadMarker, errHeadMarker)
	fmt.Fprintf(&out, "                       printed FIRST, inside every buffer the plugin holds\n")
	fmt.Fprintf(&out, "tail markers           %s (stdout), %s (stderr)\n", tailMarker, errMarker)
	fmt.Fprintf(&out, "                       printed after the filler, past every buffer\n\n")
	fmt.Fprintf(&out, "StreamTaskLogs         %d chunk(s), %d bytes: ALL FOUR markers present\n",
		logged.count(), len(body))
	fmt.Fprintf(&out, "Temporal history       %d bytes scanned (payloads base64-decoded)\n", historyBytes)
	fmt.Fprintf(&out, "  control              PRESENT  — so the scan reads the error payloads\n")
	fmt.Fprintf(&out, "  every marker         ABSENT   — no output rides the failure\n")
	fmt.Fprintf(&out, "event stream           %d bytes scanned\n", archivedBytes)
	fmt.Fprintf(&out, "  control              PRESENT  — the run announces why it failed\n")
	fmt.Fprintf(&out, "  every marker         ABSENT   — events announce, they do not carry\n\n")
	fmt.Fprintf(&out, "the error the run reports, in full:\n  %s\n\n", taskError)
	fmt.Fprintf(&out, "verdict: the whole of a failed container's output is in its log stream, "+
		"and none of it is in run history or on the event stream.\n\n")
	fmt.Fprintf(&out, "the markers, as StreamTaskLogs delivered them:\n")
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if strings.Contains(line, outHeadMarker) || strings.Contains(line, errHeadMarker) ||
			strings.Contains(line, tailMarker) || strings.Contains(line, errMarker) {
			fmt.Fprintf(&out, "  %s\n", line)
		}
	}
	fmt.Fprintf(&out, "  (%d lines in all)\n", strings.Count(body, "\n"))
	appendProofArtifact(t, "no-output-in-history-failed-container.txt", out.String())
}
