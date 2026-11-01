//go:build integration

package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// canceledByDocument is one task, held by the stack's Orker until its run is
// canceled.
func canceledByDocument(task string) string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: canceled-by
version: "1.0.0"
tasks:
  - id: ` + task + `
    uses: shell
    with: {op: exec}
`
}

// TestIntegration_CancelRunRecordsWhoAskedOnTheRun is the contract the
// read model rests on, against a real cluster: the cancellation's reason is
// kept on the run's own record of the request, a second request on a run
// whose cancellation is already requested records nothing, so the run names
// the request that canceled it — on GetRun, on the archived run, and after
// the run is retired from the cluster, from the archive alone. A cancel with
// no principal names nobody.
func TestIntegration_CancelRunRecordsWhoAskedOnTheRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "canceled-by")
	s.starter.WithClusterIdentity(s.namespace, "canceled-by-control")
	g := newGates()
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, "hold", "wait"))
	surface := s.serveAPI(api.Config{})
	plan, _ := s.tcPlan()

	named := s.newRunID() + "-named"
	s.start(named, compile(t, canceledByDocument("hold")), nil, plan)
	g.waitEntered(t, "hold", 2*time.Minute)
	if err := s.starter.CancelRun(ctx, named, engine.CancelRequest{Reason: "stuck", Principal: "dana@acme"}); err != nil {
		t.Fatalf("CancelRun as dana@acme: %v", err)
	}
	// Someone else asks too. Whatever the cluster answers, it must not
	// replace the name of the request that took effect.
	second := s.starter.CancelRun(ctx, named, engine.CancelRequest{Principal: "eve@acme"})
	if status, _ := s.wait(named, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("run %s closed %v, want Canceled", named, status)
	}

	var causes []string
	for _, event := range s.history(named, "").GetEvents() {
		if attrs := event.GetWorkflowExecutionCancelRequestedEventAttributes(); attrs != nil {
			causes = append(causes, attrs.GetCause())
		}
	}
	if len(causes) != 1 {
		t.Fatalf("the run recorded %d cancellation requests %q, want exactly one", len(causes), causes)
	}
	var record orkstackv1.RunCancelRequest
	if err := protojson.Unmarshal([]byte(causes[0]), &record); err != nil ||
		record.GetFormat() != 1 || record.GetPrincipal() != "dana@acme" {
		t.Fatalf("the request's cause %q = %v, %v; want format 1 naming dana@acme", causes[0], &record, err)
	}

	run, err := s.reader.GetRun(ctx, named)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" ||
		run.GetCanceledBy() != "dana@acme" {
		t.Fatalf("GetRun = %s/%s by %q, want CANCELED/RUN_CANCELED by dana@acme",
			run.GetStatus(), run.GetError().GetCode(), run.GetCanceledBy())
	}
	archived := awaitCanceledArchive(ctx, t, s, named)
	if archived.GetCanceledBy() != "dana@acme" {
		t.Fatalf("archived run by %q, want dana@acme", archived.GetCanceledBy())
	}

	// Retire the run from the cluster: what answers now is the archive.
	if _, err := s.control.WorkflowService().DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: named},
	}); err != nil {
		t.Fatalf("retire run %s: %v", named, err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		_, err := s.reader.GetRun(ctx, named)
		if errors.Is(err, engine.ErrRunNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s is still in the cluster after its deletion (last read: %v)", named, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	retired, err := surface.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: named}))
	if err != nil {
		t.Fatalf("GetRun of the retired run: %v", err)
	}
	if got := retired.Msg.GetRun(); got.GetStatus() != orkstackv1.Status_STATUS_CANCELED || got.GetCanceledBy() != "dana@acme" {
		t.Fatalf("the retired run reads %s by %q, want CANCELED by dana@acme", got.GetStatus(), got.GetCanceledBy())
	}

	// A cancel that carried no principal names nobody, on every surface.
	nobody := s.newRunID() + "-nobody"
	s.start(nobody, compile(t, canceledByDocument("wait")), nil, plan)
	g.waitEntered(t, "wait", 2*time.Minute)
	if err := s.starter.CancelRun(ctx, nobody, engine.CancelRequest{Reason: "stuck"}); err != nil {
		t.Fatalf("CancelRun with no principal: %v", err)
	}
	if status, _ := s.wait(nobody, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("run %s closed %v, want Canceled", nobody, status)
	}
	unnamed, err := surface.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: nobody}))
	if err != nil || unnamed.Msg.GetRun().GetCanceledBy() != "" ||
		unnamed.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("GetRun of the unnamed cancel = %v, %v; want CANCELED by nobody", unnamed.Msg.GetRun(), err)
	}
	if archivedNobody := awaitCanceledArchive(ctx, t, s, nobody); archivedNobody.GetCanceledBy() != "" ||
		archivedNobody.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("archived unnamed cancel = %s by %q, want CANCELED by nobody",
			archivedNobody.GetStatus(), archivedNobody.GetCanceledBy())
	}

	s.transcript.step("named run %s: one cancellation request, cause %s; second request returned %v", named, causes[0], second)
	t.Logf("EVIDENCE canceled_by run=%s requests=1 cause=%s second_request_err=%v get_run=%q archived=%q retired=%q",
		named, causes[0], second, run.GetCanceledBy(), archived.GetCanceledBy(), retired.Msg.GetRun().GetCanceledBy())
	t.Logf("EVIDENCE canceled_by_nobody run=%s get_run=%q", nobody, unnamed.Msg.GetRun().GetCanceledBy())
}

// awaitCanceledArchive polls the stack's archive until runID's body is there: the
// projection writes it on the run's terminal event, which may still be
// landing when the execution reports closed.
func awaitCanceledArchive(ctx context.Context, t *testing.T, s *chaosStack, runID string) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		run, err := s.archive.LoadRun(ctx, runID)
		if err == nil {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s was never archived: %v", runID, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
