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
// Three markers from one container, and the three are what make "absent" a
// measurement rather than a limitation of the scanner:
//
//   - headMarker is printed FIRST on stdout. It reaches history — not through
//     the log path, but through the container plugin's own documented payload
//     echo: output.stdout carries the first MaxCapturedOutput (8 KiB) of the
//     stream. That predates this milestone. Here it is the POSITIVE CONTROL:
//     the same scan that reports the others absent finds this one.
//   - tailMarker is printed on stdout after 15 KiB of filler, so the payload
//     echo cannot reach it. Everything that could carry it to history is the
//     log path — and it must be absent.
//   - errMarker is printed on STDERR after 15 KiB of stderr filler, so the
//     output.stderr echo cannot reach it either. It is the marker this
//     milestone newly streams, and it must be absent from history exactly as
//     the stdout one is.
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
	if !strings.Contains(events, headMarker) {
		t.Fatalf("the history scan cannot even find %s, which the container plugin's own "+
			"documented stdout echo puts there; its verdict about the log path means nothing",
			headMarker)
	}
	for _, marker := range []string{tailMarker, errMarker} {
		if strings.Contains(events, marker) {
			t.Fatalf("%s is in run %s's Temporal history; the log path must put no bytes "+
				"there (AGENTS.md §1.4)", marker, runID)
		}
	}
	t.Logf("scanned %d bytes of run %s's Temporal history: it carries the payload echo "+
		"(%s) and NOT the log lines beyond it (%s, %s)",
		size, runID, headMarker, tailMarker, errMarker)

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
		for _, field := range []string{"exit_code", "stdout", "stderr", "image", "duration_ms"} {
			if _, ok := output[field]; !ok {
				t.Errorf("the container task's output payload has no %q; it holds %v",
					field, output)
			}
		}
		if code, _ := output["exit_code"].(float64); code != 0 {
			t.Errorf("output.exit_code = %v, want 0", output["exit_code"])
		}
		if head, _ := output["stdout"].(string); !strings.Contains(head, "cout-01") {
			t.Errorf("output.stdout = %q, want the captured head of the stream", head)
		}
		if head, _ := output["stderr"].(string); !strings.Contains(head, "cerr-01") {
			t.Errorf("output.stderr = %q, want the captured head of the stream", head)
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
// container, each separated from the payload echo by more than the 8 KiB the
// plugin puts on output.stdout / output.stderr.
//
// The filler is 200 lines of ~76 bytes (~15 KiB) per stream, so the tail
// markers cannot be on the task payload — and therefore cannot be in history —
// by any path except the log one. The head marker is inside the cap, and is
// the scan's positive control.
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
	fmt.Fprintf(&out, "control marker         %s   (printed first, inside the 8 KiB the\n", headMarker)
	fmt.Fprintf(&out, "                       container plugin echoes onto output.stdout)\n")
	fmt.Fprintf(&out, "log-only stdout marker %s   (printed after the filler, so no payload\n", tailMarker)
	fmt.Fprintf(&out, "                       can carry it)\n")
	fmt.Fprintf(&out, "log-only stderr marker %s   (the stream this milestone newly carries)\n\n", errMarker)
	fmt.Fprintf(&out, "StreamTaskLogs         %d chunk(s), %d bytes: ALL THREE markers present,\n",
		logged.count(), len(body))
	fmt.Fprintf(&out, "                       the stderr one prefixed %q\n", plugin.StderrMarker)
	fmt.Fprintf(&out, "Temporal history       %d bytes scanned (payloads base64-decoded)\n", historyBytes)
	fmt.Fprintf(&out, "  control marker       PRESENT  — so the scan reads payloads\n")
	fmt.Fprintf(&out, "  log-only markers     ABSENT   — the log path adds nothing to history\n")
	fmt.Fprintf(&out, "event stream           %d bytes scanned\n", archivedBytes)
	fmt.Fprintf(&out, "  every marker         ABSENT   — events announce, they do not carry\n")
	fmt.Fprintf(&out, "verdict: the whole container log is in the stream; only the payload echo "+
		"is in history, and no marker is on the event stream.\n\n")
	fmt.Fprintf(&out, "the log's own boundaries, as StreamTaskLogs delivered them:\n")
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if strings.Contains(line, headMarker) || strings.Contains(line, tailMarker) ||
			strings.Contains(line, errMarker) ||
			strings.Contains(line, "filler-0-") || strings.Contains(line, "efill-0-") {
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
	appendProofArtifact(t, "no-bytes-in-history-container.txt", out.String())
}
