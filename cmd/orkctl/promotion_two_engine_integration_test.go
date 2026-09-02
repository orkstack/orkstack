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

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Two acceptances that need real processes: promotion is safe when two
// replicas do it at once, and a run already in flight finishes on the version
// it started with however the pointer moves underneath it.
//
// Both live here rather than in internal/engine for the reason the file beside
// them records: the harness that boots real `ork` binaries on separate ports
// already exists in this package (startReplica, buildBinary, startProcess,
// isolatedNamespace), and two real processes are a strictly stronger claim
// than two in-process engines.
//
// Isolation (AGENTS.md §11.1): a unique Temporal namespace per run, with the
// deliberate inversion the two-replica tests need — the pair SHARES it,
// because sharing is the condition under test.

// TestIntegration_TwoReplicasPromoteConcurrently_OneWinnerNeverDangling: two
// replicas promote one orkflow at once, one lands, and the pointer never
// dangles.
//
// Two real engines, one MinIO bucket, promotions interleaved from both at once,
// and a reader on each replica running throughout. Three things must hold:
// every promotion either lands or is TOLD it lost, no read ever resolves to a
// version that is not stored, and the two replicas end up agreeing.
func TestIntegration_TwoReplicasPromoteConcurrently_OneWinnerNeverDangling(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	namespace := isolatedNamespace()
	t.Logf("namespace %s", namespace)
	dialIsolatedNamespace(ctx, t, address, namespace)

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	queue := idempotencyTaskQueue()
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, namespace, queue)
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, namespace, queue)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	name := fmt.Sprintf("promote-%d-%d", os.Getpid(), time.Now().UnixNano())
	versions := []string{"1.0.0", "2.0.0"}
	for _, version := range versions {
		if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(
			&orkstackv1.ApplyOrkflowRequest{Yaml: promotionOrkflowYAML(name, version, "v"+version)},
		)); err != nil {
			t.Fatalf("ApplyOrkflow(%s): %v", version, err)
		}
	}
	awaitOrkflowVisible(ctx, t, replicaB.flows, name)

	// Readers on BOTH replicas, running for the whole race. A dangling pointer
	// shows up here as a NotFound or as a version nobody applied.
	//
	// Each reader keeps its own slice rather than sending on a channel: a
	// buffered channel bounds how many reads the race is allowed to take, and
	// a reader that fills it blocks forever holding the wait group — a test
	// that HANGS instead of failing, which is the one outcome worse than red.
	stop := make(chan struct{})
	var observedMu sync.Mutex
	var observed []string
	var readers sync.WaitGroup
	for _, replica := range []*engineReplica{replicaA, replicaB} {
		readers.Add(1)
		go func(replica *engineReplica) {
			defer readers.Done()
			var mine []string
			defer func() {
				observedMu.Lock()
				observed = append(observed, mine...)
				observedMu.Unlock()
			}()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := replica.flows.GetOrkflow(ctx, connect.NewRequest(
					&orkstackv1.GetOrkflowRequest{Name: name}))
				if err != nil {
					mine = append(mine, replica.name+" ERROR: "+err.Error())
					return
				}
				mine = append(mine, got.Msg.GetOrkflow().GetVersion())
			}
		}(replica)
	}

	// The promotions: each replica pushes its own version, over and over, at
	// the same time as the other.
	const rounds = 10
	type promotion struct {
		replica  string
		previous string
		version  string
		err      error
	}
	results := make([]promotion, 0, 2*rounds)
	var mu sync.Mutex
	var writers sync.WaitGroup
	for range rounds {
		for index, replica := range []*engineReplica{replicaA, replicaB} {
			writers.Add(1)
			go func(replica *engineReplica, version string) {
				defer writers.Done()
				landed := promotion{replica: replica.name, version: version}
				moved, err := replica.flows.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
					&orkstackv1.SetCurrentOrkflowVersionRequest{Name: name, Version: version}))
				landed.err = err
				if err == nil {
					landed.previous = moved.Msg.GetPreviousVersion()
				}
				mu.Lock()
				results = append(results, landed)
				mu.Unlock()
			}(replica, versions[index])
		}
	}
	writers.Wait()
	close(stop)
	readers.Wait()

	// 1. Every promotion either landed or was told it lost the race. Nothing
	//    else: a promotion that "succeeded" while another value won would be a
	//    lost write reported as a success.
	landed, refused := 0, 0
	var chain [][2]string
	for _, result := range results {
		switch {
		case result.err == nil:
			landed++
			chain = append(chain, [2]string{result.previous, result.version})
		case connect.CodeOf(result.err) == connect.CodeFailedPrecondition:
			refused++
		default:
			t.Fatalf("%s promoting %s failed for an unexpected reason: %v",
				result.replica, result.version, result.err)
		}
	}
	if landed == 0 {
		t.Fatal("no promotion landed at all")
	}
	t.Logf("%d concurrent promotions across two replicas: %d landed, %d reported a lost race",
		len(results), landed, refused)

	// 2. No reader ever saw a version that is not stored.
	stored := map[string]bool{"1.0.0": true, "2.0.0": true}
	seen := map[string]int{}
	for _, observation := range observed {
		if strings.Contains(observation, "ERROR") {
			t.Fatalf("a read during the race failed: %s", observation)
		}
		if !stored[observation] {
			t.Fatalf("a reader resolved %q, which is not a stored version", observation)
		}
		seen[observation]++
	}
	if len(seen) == 0 {
		t.Fatal("no reads were taken during the race")
	}
	t.Logf("%d reads during the race resolved to %v", countAll(seen), seen)

	// 3. Both replicas converge, and on one of the two versions. The read is
	//    the durable store's, so a replica cannot be answering from memory.
	finalA := currentVersionOn(ctx, t, replicaA, name)
	finalB := currentVersionOn(ctx, t, replicaB, name)
	if finalA != finalB {
		t.Fatalf("the replicas disagree about the current version: %s says %q, %s says %q",
			replicaA.name, finalA, replicaB.name, finalB)
	}
	if !stored[finalA] {
		t.Fatalf("the final current version is %q, which is not stored", finalA)
	}
	t.Logf("both replicas report current version %s", finalA)

	// 4. And no write was lost. Every promotion reports the version it
	//    REPLACED, so the ones that succeeded must form a single chain from
	//    "no pointer" to the final value: each success replaced the state the
	//    previous success created. Two writers replacing the SAME state means
	//    one of the two writes is gone, and it shows up here as a state left
	//    more often than it is entered. Every assertion above survives that
	//    intact — the pointer still names a stored version and both replicas
	//    still agree — which is why this one is here.
	requireOnePromotionChain(t, chain, finalA)

	// And the version listing agrees on both, marking one version current.
	for _, replica := range []*engineReplica{replicaA, replicaB} {
		listed, err := replica.flows.ListOrkflowVersions(ctx, connect.NewRequest(
			&orkstackv1.ListOrkflowVersionsRequest{Name: name}))
		if err != nil {
			t.Fatalf("ListOrkflowVersions on %s: %v", replica.name, err)
		}
		if listed.Msg.GetCurrentVersion() != finalA {
			t.Errorf("%s lists current %q, want %q",
				replica.name, listed.Msg.GetCurrentVersion(), finalA)
		}
		current := 0
		for _, version := range listed.Msg.GetVersions() {
			if version.GetIsCurrent() {
				current++
			}
		}
		if current != 1 {
			t.Errorf("%s marks %d versions current, want exactly 1", replica.name, current)
		}
	}

	// A promotion to a version nobody applied is refused on both replicas, and
	// changes nothing.
	for _, replica := range []*engineReplica{replicaA, replicaB} {
		_, err := replica.flows.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
			&orkstackv1.SetCurrentOrkflowVersionRequest{Name: name, Version: "9.9.9"}))
		if connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("%s promoting an unknown version = %v, want NotFound", replica.name, err)
		}
	}
	if after := currentVersionOn(ctx, t, replicaB, name); after != finalA {
		t.Errorf("a refused promotion moved the pointer: %q -> %q", finalA, after)
	}

	writePromotionRaceTranscript(t, namespace, name, finalA, landed, refused, seen)
}

// TestIntegration_APinnedRunFinishesOnItsOwnVersionAcrossAPromotion: a run in
// flight finishes on the version it started with.
//
// A run is submitted while 1.0.0 is current, and the pointer is moved to 2.0.0
// WHILE it is executing. The run must finish reporting the version it started
// with, and must have executed that version's task configuration — the two
// versions do different work, so the artifacts say which one ran.
func TestIntegration_APinnedRunFinishesOnItsOwnVersionAcrossAPromotion(t *testing.T) {
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
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, namespace, queue)
	engineProcess := startReplica(ctx, t, "ork-A", enginePath, address, namespace, queue)
	engineProcess.awaitReady(ctx, t)

	name := fmt.Sprintf("pinned-%d-%d", os.Getpid(), time.Now().UnixNano())
	// 1.0.0 is slow on purpose: it has to still be running when the pointer
	// moves, or the test would prove nothing about a run in flight.
	if _, err := engineProcess.flows.ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: slowPromotionOrkflowYAML(name, "1.0.0", 20)},
	)); err != nil {
		t.Fatalf("ApplyOrkflow(1.0.0): %v", err)
	}
	if _, err := engineProcess.flows.ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: promotionOrkflowYAML(name, "2.0.0", "the-new-version")},
	)); err != nil {
		t.Fatalf("ApplyOrkflow(2.0.0): %v", err)
	}
	// Make the SLOW one current, so an unpinned submission starts on it.
	if _, err := engineProcess.flows.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: name, Version: "1.0.0"},
	)); err != nil {
		t.Fatalf("SetCurrentOrkflowVersion(1.0.0): %v", err)
	}

	submitted, err := engineProcess.runs.SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: name}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	if got := submitted.Msg.GetRun().GetOrkflowVersion(); got != "1.0.0" {
		t.Fatalf("the run started on version %q, want the current 1.0.0", got)
	}
	t.Logf("run %s started on version 1.0.0", runID)

	// Move the pointer while the run is still executing. The cluster's own
	// status is the evidence that it really is in flight.
	awaitRunning(ctx, t, temporalClient, runID)
	promoted, err := engineProcess.flows.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: name, Version: "2.0.0"}))
	if err != nil {
		t.Fatalf("SetCurrentOrkflowVersion(2.0.0) mid-run: %v", err)
	}
	if promoted.Msg.GetPreviousVersion() != "1.0.0" {
		t.Errorf("the promotion replaced %q, want 1.0.0", promoted.Msg.GetPreviousVersion())
	}
	status := executionStatus(ctx, t, temporalClient, runID)
	if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the run reached %s before the pointer moved; it is too fast for this test",
			status)
	}
	t.Logf("promoted %s to 2.0.0 while run %s was %s", name, runID, status)

	// A run submitted NOW gets the new version: the pointer really moved.
	afterPromotion, err := engineProcess.runs.SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: name}))
	if err != nil {
		t.Fatalf("SubmitRun after promotion: %v", err)
	}
	if got := afterPromotion.Msg.GetRun().GetOrkflowVersion(); got != "2.0.0" {
		t.Fatalf("a run submitted after the promotion took version %q, want 2.0.0", got)
	}

	// The pinned run finishes on ITS version.
	awaitWorkflowCompleted(ctx, t, temporalClient, runID)
	finished := awaitTerminalRun(ctx, t, engineProcess, runID)
	if finished.GetOrkflowVersion() != "1.0.0" {
		t.Fatalf("the pinned run finished reporting version %q, want 1.0.0",
			finished.GetOrkflowVersion())
	}
	if finished.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the pinned run finished as %s, want SUCCEEDED", finished.GetStatus())
	}

	// And it executed 1.0.0's configuration, not 2.0.0's. The two versions run
	// different commands, so the task's own report says which document the
	// Orker was given.
	//
	// Under the OCI Activity Contract that report is values.json, not a
	// platform-captured stdout: §3.6 makes stdout a durable system ARTIFACT,
	// and activity-shell's `exec` op writes only {"exitCode":n}. The `eval` op
	// is the one whose contract is "the workload's output is the value", so
	// the marker arrives as values.value — the workload's own words, which is
	// what this assertion always meant.
	taskRuns := finished.GetTaskRuns()
	if len(taskRuns) != 1 {
		t.Fatalf("the run reports %d task runs, want 1", len(taskRuns))
	}
	output := taskRuns[0].GetOutput().GetData().AsMap()
	stdout := fmt.Sprint(output["value"])
	if !strings.Contains(stdout, "slow-1.0.0") {
		t.Fatalf("the pinned run executed the wrong version's task: value %q, output %v",
			stdout, output)
	}
	t.Logf("run %s finished on version %s having executed %q",
		runID, finished.GetOrkflowVersion(), strings.TrimSpace(stdout))

	writePinnedRunTranscript(t, namespace, name, runID,
		afterPromotion.Msg.GetRun().GetId(), strings.TrimSpace(stdout))
}

// promotionOrkflowYAML is a one-task orkflow that says which version it is, so
// a run's own output identifies the document that produced it.
func promotionOrkflowYAML(name, version, marker string) string {
	return fmt.Sprintf(`name: %s
version: "%s"
description: version %s
tasks:
  - id: report
    uses: shell
    with:
      op: eval
      command: ["sh", "-c", "echo %s"]
`, name, version, version, marker)
}

// slowPromotionOrkflowYAML is the same, but takes long enough that the pointer
// can be moved while it runs.
func slowPromotionOrkflowYAML(name, version string, seconds int) string {
	return fmt.Sprintf(`name: %s
version: "%s"
description: version %s (slow)
tasks:
  - id: report
    uses: shell
    timeout_seconds: 120
    with:
      op: eval
      command: ["sh", "-c", "sleep %d; echo slow-%s"]
`, name, version, version, seconds, version)
}

// currentVersionOn reads one replica's answer for "which version is current".
func currentVersionOn(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	name string,
) string {
	t.Helper()
	listed, err := replica.flows.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: name}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions on %s: %v", replica.name, err)
	}
	return listed.Msg.GetCurrentVersion()
}

// awaitRunning blocks until the cluster reports the run executing, so a
// mid-run assertion is really mid-run.
func awaitRunning(ctx context.Context, t *testing.T, c client.Client, runID string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		described, err := c.DescribeWorkflowExecution(ctx, runID, "")
		if err == nil &&
			described.GetWorkflowExecutionInfo().GetStatus() ==
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("run %s never reached RUNNING", runID)
}

// awaitTerminalRun reads the run back from the engine until it is terminal.
func awaitTerminalRun(
	ctx context.Context,
	t *testing.T,
	replica *engineReplica,
	runID string,
) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var last *orkstackv1.Run
	for time.Now().Before(deadline) {
		got, err := replica.runs.GetRun(ctx, connect.NewRequest(
			&orkstackv1.GetRunRequest{RunId: runID}))
		if err == nil {
			last = got.Msg.GetRun()
			switch last.GetStatus() {
			case orkstackv1.Status_STATUS_SUCCEEDED,
				orkstackv1.Status_STATUS_FAILED,
				orkstackv1.Status_STATUS_CANCELED:
				return last
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s never reached a terminal state (last %v)", runID, last)
	return nil
}

// requireOnePromotionChain holds the reported promotions to one chain: read as
// a multigraph whose edges are (version replaced -> version written), every
// state must be left exactly as often as it is entered, except "no pointer",
// which is left once, and the final value, which is entered once.
//
// It is the assertion that separates a compare-and-swap from a
// last-writer-wins. Under CAS a success can only replace the state it read, so
// the successes are a path. Without it, two replicas replace one state, both
// are told they succeeded, and one of the two promotions is gone.
func requireOnePromotionChain(t *testing.T, chain [][2]string, final string) {
	t.Helper()
	if len(chain) == 0 {
		t.Fatal("no promotion landed at all")
	}
	delta := map[string]int{}
	for _, edge := range chain {
		delta[edge[0]]++
		delta[edge[1]]--
	}
	for state, balance := range delta {
		want := 0
		switch state {
		case "":
			want = 1
		case final:
			want = -1
		}
		if balance != want {
			name := state
			if name == "" {
				name = "no pointer"
			}
			t.Fatalf("the promotions that reported success are not one chain: %q is left "+
				"%d more times than it is entered (want %d) — a write was lost.\n"+
				"transitions: %v", name, balance, want, chain)
		}
	}
	t.Logf("the %d promotions that landed form one chain to %s", len(chain), final)
}

func countAll(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

// writePromotionRaceTranscript and writePinnedRunTranscript record the
// transcripts under ORK_PROOF_DIR when scripts/proof.sh sets it; a plain
// `go test -tags=integration ./...` logs them and writes nothing into the tree.
func writePromotionRaceTranscript(
	t *testing.T,
	namespace, name, final string,
	landed, refused int,
	seen map[string]int,
) {
	t.Helper()
	var out strings.Builder
	fmt.Fprintf(&out, "# two engine replicas promoting one orkflow at once (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "temporal namespace   %s\n", namespace)
	fmt.Fprintf(&out, "orkflow              %s (versions 1.0.0, 2.0.0)\n", name)
	fmt.Fprintf(&out, "promotions           %d landed, %d told they lost the race\n", landed, refused)
	fmt.Fprintf(&out, "reads during race    %d, resolving to %v\n", countAll(seen), seen)
	fmt.Fprintf(&out, "                     every one named a stored version\n")
	fmt.Fprintf(&out, "final answer         both replicas report %s\n", final)
	fmt.Fprintf(&out, "unknown version      refused with NotFound on both replicas; pointer unmoved\n")
	proof.Write(t, "orkctl", "promotion-race.txt", []byte(out.String()))
}

func writePinnedRunTranscript(t *testing.T, namespace, name, pinnedRun, laterRun, stdout string) {
	t.Helper()
	var out strings.Builder
	fmt.Fprintf(&out, "# a run in flight across a promotion (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "temporal namespace   %s\n", namespace)
	fmt.Fprintf(&out, "orkflow              %s\n", name)
	fmt.Fprintf(&out, "current at submit    1.0.0 (slow)\n")
	fmt.Fprintf(&out, "run %s submitted, reported version 1.0.0\n", pinnedRun)
	fmt.Fprintf(&out, "pointer moved to 2.0.0 while that run was RUNNING on the cluster\n")
	fmt.Fprintf(&out, "run %s submitted after the move, reported version 2.0.0\n", laterRun)
	fmt.Fprintf(&out, "run %s finished SUCCEEDED, version 1.0.0, stdout %q\n", pinnedRun, stdout)
	proof.Write(t, "orkctl", "pinned-run.txt", []byte(out.String()))
}

// compile-time reminder that this file drives the same clients the CLI does.
var _ orkstackv1connect.OrkflowServiceClient = (orkstackv1connect.OrkflowServiceClient)(nil)
