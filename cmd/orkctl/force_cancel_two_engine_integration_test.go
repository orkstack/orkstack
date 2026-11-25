//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// forceCancelProbeYAML is one task that runs until it is stopped, and stops at
// once when asked.
func forceCancelProbeYAML(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: force cancel probe.
tasks:
  - id: slow
    uses: shell
    timeout_seconds: 600
    with:
      op: script
      interpreter: sh
      script: |
        trap 'exit 0' TERM
        sleep 500 &
        wait
`, name)
}

// TestIntegration_ForceCancelThroughTheCLI force-cancels a healthy run on real
// `ork` replicas with `orkctl run cancel --force --wait`, and requires: it ends
// by its own cancel, RUN_CANCELED, and is never force-canceled; the other
// replica reads the force cancel and the audit trail says who asked, as
// asserted; a grace below the least is refused with its reason and exit 5; a
// preview lists nothing below; and asking of the ended run says so. The
// replicas are cmd/ork as it ships, so this is also the proof that the engine
// is wired to force-cancel: its activities, its event worker's build, its keys
// and its settings.
func TestIntegration_ForceCancelThroughTheCLI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newOriginStack(ctx, t, "force-cancel")
	const orkflow = "force-cancel-probe"
	if _, err := s.a.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: forceCancelProbeYAML(orkflow), Namespace: s.namespace,
	})); err != nil {
		t.Fatalf("ApplyOrkflow on replica A: %v", err)
	}
	runID := decodeSubmittedID(t, s.cli(t, s.a, exitOK, "--json", "run", "submit", orkflow))
	s.awaitTaskRunning(ctx, t, s.a, runID)

	s.principal = "dana@acme"
	if preview := s.cli(t, s.b, exitOK, "run", "cancel", runID, "--force", "--cascade", "--dry-run"); !strings.Contains(preview, "0 open runs below run "+runID) {
		t.Fatalf("the preview says %q", preview)
	}
	s.cli(t, s.a, exitForceCancelRefused, "run", "cancel", runID, "--force", "--reason", "x", "--grace", "31s")
	out := s.cli(t, s.a, exitOK, "run", "cancel", runID, "--force", "--reason", "stuck after the roll", "--wait", "--wait-timeout", "4m")
	for _, want := range []string{"Force cancel requested for run " + runID, "Run " + runID + ": canceled"} {
		if !strings.Contains(out, want) {
			t.Fatalf("run cancel --force --wait lacks %q:\n%s", want, out)
		}
	}
	run := s.awaitTerminal(ctx, t, s.b, runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" || run.GetCanceledBy() != "dana@acme" || run.GetCanceledByVerified() {
		t.Fatalf("the run ended %s %v by %q, verified %t", run.GetStatus(), run.GetError(), run.GetCanceledBy(), run.GetCanceledByVerified())
	}
	got, err := s.b.runs.GetForceCancel(ctx, connect.NewRequest(&orkstackv1.GetForceCancelRequest{RunId: runID, Namespace: s.namespace}))
	if err != nil {
		t.Fatalf("GetForceCancel on replica B: %v", err)
	}
	if fc := got.Msg; fc.GetState() != orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_CANCELED || fc.GetPrincipal() != "dana@acme" || fc.GetPrincipalVerified() ||
		fc.GetReason() != "stuck after the roll" || fc.GetIncomplete() || fc.GetGrace().AsDuration() != time.Minute {
		t.Fatalf("the other replica reads %v", fc)
	}
	// It lists every namespace, so it takes no -n; the rest is as s.cli set it.
	var stdout, stderr bytes.Buffer
	if code := Execute([]string{"--json", "admin", "audit", "--filter", "namespace=" + s.namespace, "--limit", "50", "--server", "http://" + s.b.addr}, &stdout, &stderr); code != exitOK {
		t.Fatalf("orkctl admin audit exited %d: %s", code, stderr.String())
	}
	trail := stdout.String()
	for _, action := range []string{"run.cancel.force.requested", "run.cancel.force.completed", `"run.cancel.force"`} {
		if !strings.Contains(trail, action) {
			t.Errorf("the trail lacks %s:\n%s", action, trail)
		}
	}
	if ended := s.cli(t, s.b, exitOK, "run", "cancel", runID, "--force", "--reason", "again", "--wait"); !strings.Contains(ended, "had already ended") {
		t.Errorf("asking of the ended run says %q", ended)
	}
}
