//go:build integration

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// canceledByOrkflowYAML is one task that runs until its run is canceled and
// leaves at once on SIGTERM.
func canceledByOrkflowYAML(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: canceled-by probe.
tasks:
  - id: wait
    uses: shell
    timeout_seconds: 300
    with:
      op: script
      interpreter: sh
      script: |
        trap 'exit 143' TERM
        while :; do sleep 1; done
`, name)
}

// canceledByOf decodes `run get --json` down to its status and canceledBy.
func canceledByOf(t *testing.T, stdout string) (status, by string) {
	t.Helper()
	var response struct {
		Run struct {
			Status     string `json:"status"`
			CanceledBy string `json:"canceledBy"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &response); err != nil {
		t.Fatalf("decode run get: %v (%s)", err, stdout)
	}
	return response.Run.Status, response.Run.CanceledBy
}

// awaitTaskRunning polls one replica until the run's task is dispatched
// (RUNNING), so a cancel lands on work in flight.
func (s *originStack) awaitTaskRunning(ctx context.Context, t *testing.T, replica *engineReplica, runID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: s.namespace}))
		if err == nil {
			for _, taskRun := range got.Msg.GetRun().GetTaskRuns() {
				if taskRun.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
					return
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("run %s's task never started on %s", runID, replica.name)
}

// TestIntegration_CanceledByIsTheAssertedPrincipalOnEveryReplica cancels a
// run on real `ork` replicas, as `orkctl run cancel` does with ORK_PRINCIPAL
// set, and requires `run get` and `run list` on the OTHER replica to name who
// asked, as asserted — and to keep naming them once the run is retired from
// the cluster, from the archive alone. A cancel with no principal names
// nobody. The replicas are cmd/ork as it ships, so this is also the proof
// that the engine is wired to record the name.
func TestIntegration_CanceledByIsTheAssertedPrincipalOnEveryReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newOriginStack(ctx, t, "canceled-by")
	const orkflow = "cancel-probe"
	if _, err := s.a.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: canceledByOrkflowYAML(orkflow), Namespace: s.namespace,
	})); err != nil {
		t.Fatalf("ApplyOrkflow on replica A: %v", err)
	}

	named := decodeSubmittedID(t, s.cli(t, s.a, exitOK, "--json", "run", "submit", orkflow))
	s.awaitTaskRunning(ctx, t, s.b, named)
	s.principal = "dana@acme"
	s.cli(t, s.b, exitOK, "run", "cancel", named, "--reason", "stuck")
	s.principal = ""
	if run := s.awaitTerminal(ctx, t, s.a, named); run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run %s ended %s, want CANCELED", named, run.GetStatus())
	}
	for _, replica := range []*engineReplica{s.a, s.b} {
		if status, by := canceledByOf(t, s.cli(t, replica, exitOK, "--json", "run", "get", named)); status != "CANCELED" || by != "dana@acme" {
			t.Fatalf("run get --json via %s = %s by %q, want CANCELED by dana@acme", replica.name, status, by)
		}
	}
	human := s.cli(t, s.a, exitOK, "run", "get", named)
	const wantLine = "Canceled by: dana@acme (asserted by the gateway; not authenticated)"
	if !strings.Contains(collapseSpaces(human), wantLine) {
		t.Fatalf("run get via ork-A lacks %q:\n%s", wantLine, human)
	}
	s.awaitListedFinished(t, s.b, named)
	row := ""
	for _, line := range strings.Split(s.cli(t, s.b, exitOK, "run", "list"), "\n") {
		if strings.HasPrefix(line, named) {
			row = collapseSpaces(line)
		}
	}
	if !strings.Contains(row, "CANCELED by dana@acme (asserted)") {
		t.Fatalf("run list row via ork-B = %q, want it naming dana@acme as asserted", row)
	}

	nobody := decodeSubmittedID(t, s.cli(t, s.a, exitOK, "--json", "run", "submit", orkflow))
	s.awaitTaskRunning(ctx, t, s.a, nobody)
	s.cli(t, s.a, exitOK, "run", "cancel", nobody)
	s.awaitTerminal(ctx, t, s.b, nobody)
	if status, by := canceledByOf(t, s.cli(t, s.b, exitOK, "--json", "run", "get", nobody)); status != "CANCELED" || by != "" {
		t.Fatalf("an unnamed cancel reads %s by %q via ork-B, want CANCELED by nobody", status, by)
	}
	if human := s.cli(t, s.b, exitOK, "run", "get", nobody); strings.Contains(human, "Canceled by") {
		t.Fatalf("an unnamed cancel renders a Canceled by line:\n%s", human)
	}

	// Retire the named run from the cluster: the archive answers alone now.
	if _, err := s.temporal.WorkflowService().DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace:         s.partition,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: named},
	}); err != nil {
		t.Fatalf("retire run %s: %v", named, err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		_, err := s.temporal.DescribeWorkflowExecution(ctx, named, "")
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s is still in the cluster after its deletion (describe: %v)", named, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	retired := s.cli(t, s.b, exitOK, "--json", "run", "get", named)
	if status, by := canceledByOf(t, retired); status != "CANCELED" || by != "dana@acme" {
		t.Fatalf("the retired run reads %s by %q via ork-B, want CANCELED by dana@acme", status, by)
	}

	proof.Write(t, "orkctl", "run-canceled-by-two-replica.txt", []byte(s.transcript.String()))
	t.Logf("EVIDENCE canceled_by_two_replica run=%s list_row=%q retired_run_get=%s", named, row, compactJSON(t, retired))
	t.Logf("EVIDENCE canceled_by_nobody run=%s", nobody)
}
