//go:build integration

package main

import (
	"context"
	"testing"
	"time"
)

// TestIntegration_ASecondReplicaInspectsARunItNeverStarted: two `ork`
// replicas on one Temporal namespace, one Orker channel and one bucket (the
// tworeplica_read shape). A run submitted through A is inspected through B —
// which started nothing and holds nothing of it in memory — and B reports the
// same container and Orker A does. After the run, both answer no attempts.
func TestIntegration_ASecondReplicaInspectsARunItNeverStarted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 2)
	defer s.writeTranscript("run-inspect-two-replicas.txt")

	orkerID := uniqueName("orker-inspect-5")
	startOrkerWith(ctx, t, s.orkerPath, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+orkerID, "ORK_ORKER_HEARTBEAT_INTERVAL=1s")
	awaitActivityPoller(ctx, t, s.temporal, s.queue)

	name := uniqueName("inspect-replicas")
	s.apply(chattyOrkflow(name, ""))
	runID := s.submit(name) // through A

	onB := s.awaitInspect(1, runID, 2*time.Minute, "B reporting the STARTED attempt", func(d *inspectDoc) bool {
		a := d.attempt("talk")
		return a != nil && a.State == "STARTED" && a.HeartbeatAvailable && a.Phase == "run"
	}).attempt("talk")
	onA := s.inspect(0, runID).attempt("talk")
	if onA == nil || !onA.HeartbeatAvailable {
		t.Fatalf("A lost the attempt B reports: %+v", onA)
	}
	if onA.ContainerID != onB.ContainerID || onA.OrkerID != onB.OrkerID || onB.OrkerID != orkerID || onA.Attempt != onB.Attempt {
		t.Fatalf("A reports container %s orker %s attempt %d; B reports container %s orker %s attempt %d",
			onA.ContainerID, onA.OrkerID, onA.Attempt, onB.ContainerID, onB.OrkerID, onB.Attempt)
	}
	t.Logf("I5 A: containerId=%s orkerId=%s attempt=%d | B: containerId=%s orkerId=%s attempt=%d",
		onA.ContainerID, onA.OrkerID, onA.Attempt, onB.ContainerID, onB.OrkerID, onB.Attempt)

	s.mustCLI(1, "run", "cancel", runID)
	awaitTerminalOnB(ctx, t, s.replicas[1], runID)
	for replica, label := range []string{"A", "B"} {
		doc := s.awaitInspect(replica, runID, 30*time.Second, "a terminal run", func(d *inspectDoc) bool { return d.terminal() })
		if len(doc.Attempts) != 0 {
			t.Fatalf("%s reports %d live attempts for a finished run", label, len(doc.Attempts))
		}
		t.Logf("I5 %s after the run: runStatus=%s attempts=%d", label, doc.RunStatus, len(doc.Attempts))
	}
}
