//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// planBranchesYAML is a three-branch conditional Orkflow on the shell fixture
// image. One branch is chosen by input; smoke-eu also branches on build's
// REAL values.json (the shell image writes exitCode), which the plan is told
// through an assumed output.
func planBranchesYAML(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: run-plan agreement probe.
inputs:
  region:
    type: string
    default: "us"
tasks:
  - id: build
    uses: shell
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo build"]
  - id: deploy-eu
    uses: shell
    needs: [build]
    condition: '{{ eq .inputs.region "eu" }}'
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo eu"]
  - id: deploy-us
    uses: shell
    needs: [build]
    condition: '{{ eq .inputs.region "us" }}'
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo us"]
  - id: deploy-ap
    uses: shell
    needs: [build]
    condition: '{{ eq .inputs.region "ap" }}'
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo ap"]
  - id: smoke-eu
    uses: shell
    needs: [deploy-eu]
    condition: '{{ eq .tasks.build.output.exitCode 0.0 }}'
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo smoke"]
  - id: smoke-us
    uses: shell
    needs: [deploy-us]
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo smoke"]
`, name)
}

// planCLI runs orkctl in-process against one replica with an isolated config.
func planCLI(t *testing.T, server string, args ...string) result {
	t.Helper()
	full := append(append([]string(nil), args...), "--server", server)
	var stdout, stderr bytes.Buffer
	code := Execute(full, &stdout, &stderr)
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

func isolateCLIEnv(t *testing.T) {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
}

func decodePlan(t *testing.T, raw string) *orkstackv1.PlanRunResponse {
	t.Helper()
	plan := &orkstackv1.PlanRunResponse{}
	if err := api.UnmarshalJSON([]byte(raw), plan); err != nil {
		t.Fatalf("decode plan JSON: %v\n%s", err, raw)
	}
	return plan
}

func decodeRun(t *testing.T, raw string) *orkstackv1.Run {
	t.Helper()
	resp := &orkstackv1.GetRunResponse{}
	if err := api.UnmarshalJSON([]byte(raw), resp); err != nil {
		t.Fatalf("decode run JSON: %v\n%s", err, raw)
	}
	return resp.GetRun()
}

type planCluster struct {
	namespace string
	temporal  client.Client
	a, b      *engineReplica
}

// startPlanCluster boots two real `ork` replicas sharing one Temporal
// namespace and one bucket, and — when withOrker — the one Orker they share.
func startPlanCluster(ctx context.Context, t *testing.T, withOrker bool) *planCluster {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	namespace := isolatedNamespace()
	t.Logf("namespace %s", namespace)
	temporalClient := dialIsolatedNamespace(ctx, t, address, namespace)

	enginePath := testbin.Path(t, "./cmd/ork")
	queue := idempotencyTaskQueue()
	if withOrker {
		orkerPath := testbin.Path(t, "./cmd/orker")
		startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, namespace, queue)
	}
	a := startReplica(ctx, t, "ork-A", enginePath, address, namespace, queue)
	b := startReplica(ctx, t, "ork-B", enginePath, address, namespace, queue)
	a.awaitReady(ctx, t)
	b.awaitReady(ctx, t)
	return &planCluster{namespace: namespace, temporal: temporalClient, a: a, b: b}
}

func planOrkflowName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// A plan and a real run of the same input agree on every task.
func TestRunPlan_AgreesWithARealRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cluster := startPlanCluster(ctx, t, true)
	isolateCLIEnv(t)
	server := "http://" + cluster.a.addr

	name := planOrkflowName("run-plan-agree")
	file := filepath.Join(t.TempDir(), name+".ork.yaml")
	if err := os.WriteFile(file, []byte(planBranchesYAML(name)), 0o644); err != nil {
		t.Fatal(err)
	}
	planCLI(t, server, "apply", file).requireExit(t, exitOK)

	human := planCLI(t, server, "run", "plan", name, "--input", "region=eu",
		"--output", `build={"exitCode": 0}`).requireExit(t, exitOK)
	t.Logf("plan:\n%s", human.stdout)
	planned := decodePlan(t, planCLI(t, server, "run", "plan", name, "--input", "region=eu",
		"--output", `build={"exitCode": 0}`, "--json").requireExit(t, exitOK).stdout)

	submitted := &orkstackv1.SubmitRunResponse{}
	raw := planCLI(t, server, "run", "submit", name, "--input", "region=eu", "--json").requireExit(t, exitOK).stdout
	if err := api.UnmarshalJSON([]byte(raw), submitted); err != nil {
		t.Fatalf("decode submit JSON: %v\n%s", err, raw)
	}
	runID := submitted.GetRun().GetId()
	if runID == "" {
		t.Fatalf("no run id in submit output:\n%s", raw)
	}
	planCLI(t, server, "run", "watch", runID).requireExit(t, exitOK)
	run := decodeRun(t, planCLI(t, server, "run", "get", runID, "--json").requireExit(t, exitOK).stdout)

	var table strings.Builder
	fmt.Fprintf(&table, "# run plan vs real run (%s)\nnamespace %s\norkflow   %s@0.1.0\nrun       %s\n\n",
		time.Now().UTC().Format(time.RFC3339), cluster.namespace, name, runID)
	fmt.Fprintf(&table, "%-10s %-8s %-10s %s\n", "TASK", "PLANNED", "RAN", "STARTED_AT")
	statuses := map[string]*orkstackv1.TaskRun{}
	for _, tr := range run.GetTaskRuns() {
		statuses[tr.GetTaskId()] = tr
	}
	for _, task := range planned.GetTasks() {
		tr := statuses[task.GetTaskId()]
		started := "-"
		if tr.GetStartedAt() != nil {
			started = tr.GetStartedAt().AsTime().UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(&table, "%-10s %-8s %-10s %s\n", task.GetTaskId(),
			planOutcomeLabel(task.GetOutcome()), statusLabel(tr.GetStatus()), started)
		switch task.GetOutcome() {
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS:
			if tr.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Errorf("%s planned RUNS, ran %v", task.GetTaskId(), tr.GetStatus())
			}
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED:
			if tr.GetStatus() != orkstackv1.Status_STATUS_SKIPPED {
				t.Errorf("%s planned SKIPPED, ran %v", task.GetTaskId(), tr.GetStatus())
			}
			if tr.GetStartedAt() != nil {
				t.Errorf("%s was skipped but has started_at %v", task.GetTaskId(), tr.GetStartedAt().AsTime())
			}
		default:
			t.Errorf("%s planned %v", task.GetTaskId(), task.GetOutcome())
		}
	}
	if planned.GetRuns() != 3 || planned.GetSkipped() != 3 {
		t.Errorf("plan runs/skipped = %d/%d, want 3/3", planned.GetRuns(), planned.GetSkipped())
	}
	t.Logf("I1 per-task table:\n%s", table.String())
	proof.Write(t, "orkctl", "run-plan-agreement.txt", []byte(table.String()+"\n# plan\n"+human.stdout))
}

// A second replica plans an Orkflow it never applied, identically.
func TestRunPlan_SecondReplicaAnswersForAnOrkflowItNeverApplied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cluster := startPlanCluster(ctx, t, false)

	name := planOrkflowName("run-plan-replica")
	if _, err := cluster.a.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: planBranchesYAML(name),
	})); err != nil {
		t.Fatalf("ApplyOrkflow on A: %v", err)
	}
	awaitOrkflowVisible(ctx, t, cluster.b.flows, name)

	input, err := buildInputPayload([]string{"region=eu"}, "")
	if err != nil {
		t.Fatal(err)
	}
	assumed, err := buildAssumedOutputs([]string{`build={"exitCode": 0}`}, "")
	if err != nil {
		t.Fatal(err)
	}
	req := &orkstackv1.PlanRunRequest{OrkflowName: name, Input: input, AssumedOutputs: assumed}
	fromA, err := cluster.a.flows.PlanRun(ctx, connect.NewRequest(req))
	if err != nil {
		t.Fatalf("PlanRun on A: %v", err)
	}
	fromB, err := cluster.b.flows.PlanRun(ctx, connect.NewRequest(req))
	if err != nil {
		t.Fatalf("PlanRun on B, which never applied %s: %v", name, err)
	}
	if !proto.Equal(fromA.Msg, fromB.Msg) {
		t.Fatalf("replicas disagree:\nA %v\nB %v", fromA.Msg, fromB.Msg)
	}
	encoded, _ := api.MarshalJSON(fromB.Msg)
	t.Logf("I2 replica B's plan is proto-equal to replica A's (digest %s, runs=%d skipped=%d waves=%d)",
		fromB.Msg.GetDefinitionDigest(), fromB.Msg.GetRuns(), fromB.Msg.GetSkipped(), fromB.Msg.GetWaveCount())
	proof.Write(t, "orkctl", "run-plan-second-replica.json", encoded)
}

// Plans leave no run behind, on either replica or in Temporal.
func TestRunPlan_LeavesNoRunBehind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cluster := startPlanCluster(ctx, t, false)
	temporalClient := cluster.temporal

	name := planOrkflowName("run-plan-nothing")
	if _, err := cluster.a.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: planBranchesYAML(name),
	})); err != nil {
		t.Fatalf("ApplyOrkflow on A: %v", err)
	}
	awaitOrkflowVisible(ctx, t, cluster.b.flows, name)

	countRuns := func(replica *engineReplica) int {
		// Filtered to this test's own Orkflow: the shared bucket's archive holds
		// other suites' runs of the default namespace.
		resp, err := replica.runs.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{Filters: []*orkstackv1.FieldFilter{equalsFilter("orkflow_name", name)}}))
		if err != nil {
			t.Fatalf("ListRuns on %s: %v", replica.name, err)
		}
		return len(resp.Msg.GetRuns())
	}
	beforeA, beforeB := countRuns(cluster.a), countRuns(cluster.b)
	beforeTemporal := len(listExecutions(ctx, t, temporalClient, cluster.namespace, ""))

	for i := range 10 {
		replica := cluster.a
		if i%2 == 1 {
			replica = cluster.b
		}
		req := &orkstackv1.PlanRunRequest{OrkflowName: name}
		if i%3 == 0 {
			req = &orkstackv1.PlanRunRequest{Yaml: planBranchesYAML(name)}
		}
		if _, err := replica.flows.PlanRun(ctx, connect.NewRequest(req)); err != nil {
			t.Fatalf("plan %d on %s: %v", i, replica.name, err)
		}
	}

	// Visibility is eventually consistent; a started execution would appear
	// within this window, so an unchanged count after it is evidence, not luck.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(listExecutions(ctx, t, temporalClient, cluster.namespace, "")); got != beforeTemporal {
			t.Fatalf("Temporal lists %d executions after ten plans, %d before", got, beforeTemporal)
		}
		time.Sleep(500 * time.Millisecond)
	}
	afterA, afterB := countRuns(cluster.a), countRuns(cluster.b)
	afterTemporal := len(listExecutions(ctx, t, temporalClient, cluster.namespace, ""))
	if afterA != beforeA || afterB != beforeB || afterTemporal != beforeTemporal {
		t.Fatalf("runs before/after: A %d/%d, B %d/%d, Temporal %d/%d",
			beforeA, afterA, beforeB, afterB, beforeTemporal, afterTemporal)
	}
	line := fmt.Sprintf("I3 after ten plans: replica A runs of %s %d -> %d, replica B %d -> %d, Temporal executions %d -> %d",
		name, beforeA, afterA, beforeB, afterB, beforeTemporal, afterTemporal)
	t.Log(line)
	proof.Write(t, "orkctl", "run-plan-leaves-nothing.txt", []byte(line+"\n"))
}
