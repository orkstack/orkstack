//go:build integration

package main

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_OrkerRefusesANamespaceItDoesNotServe: one real Orker whose
// ORK_ORKER_NAMESPACES names only the first of two namespaces, behind two
// real engine replicas sharing one bucket. A run of the served namespace
// succeeds on it (the positive control, in the same window). A run of the
// other fails its task NAMESPACE_NOT_SERVED, a contract violation, and the
// Orker writes nothing of it: no attempt record, no log, no commit.
func TestIntegration_OrkerRefusesANamespaceItDoesNotServe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	address := temporalAddress()
	requireTemporal(t, address)
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, cfg)

	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	partition := "ork-nsguard-" + stamp
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)
	served, refused := "served-"+stamp, "refused-"+stamp

	s := &secretStack{cfg: cfg, temporal: temporalClient, namespace: served}
	// No secret is in play, so the transcript is written as it is:
	// writeTranscript screens for the secret stack's own values.
	defer func() { proof.Write(t, "orkctl", "namespace-guard.txt", []byte(s.transcript.String())) }()
	channel := idempotencyTaskQueue()
	startOrkerWithEnv(ctx, t, temporalClient, testbin.Path(t, "./cmd/orker"), address, partition, channel,
		"ORK_ORKER_NAMESPACES="+served)
	enginePath := testbin.Path(t, "./cmd/ork")
	s.a = startReplica(ctx, t, "ork-A", enginePath, address, partition, channel)
	s.b = startReplica(ctx, t, "ork-B", enginePath, address, partition, channel)
	s.a.awaitReady(ctx, t)
	s.b.awaitReady(ctx, t)
	fmt.Fprintf(&s.transcript, "# namespace guard (%s)\npartition  %s\norker      ORK_ORKER_NAMESPACES=%s\n\n",
		time.Now().UTC().Format(time.RFC3339), partition, served)

	const doc = `apiVersion: orkstack/v1
kind: Orkflow
name: hello
version: "0.1.0"
description: namespace guard probe.
tasks:
  - id: greet
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo hello"]
`
	path := s.document(t, doc)
	for _, ns := range []string{served, refused} {
		s.cli(t, s.a, exitOK, "namespace", "create", ns)
		s.cli(t, s.a, exitOK, "orkflow", "create", path, "-n", ns)
	}

	s.namespace = refused
	refusedRun := s.submitID(t, s.a, "hello")
	s.namespace = served
	servedRun := s.submitID(t, s.b, "hello")

	s.awaitStatus(ctx, t, s.b, servedRun, orkstackv1.Status_STATUS_SUCCEEDED)
	s.note("positive control: run %s of %s succeeded on the same Orker", servedRun, served)

	s.namespace = refused
	s.awaitStatus(ctx, t, s.a, refusedRun, orkstackv1.Status_STATUS_FAILED)
	stdout, _ := s.cli(t, s.b, exitOK, "--json", "run", "get", refusedRun, "-n", refused)
	var run struct {
		Run struct {
			TaskRuns []struct {
				Status string `json:"status"`
				Error  struct {
					Code     string `json:"code"`
					Message  string `json:"message"`
					Category string `json:"category"`
				} `json:"error"`
			} `json:"taskRuns"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &run); err != nil || len(run.Run.TaskRuns) != 1 {
		t.Fatalf("decode run get --json: %v\n%s", err, stdout)
	}
	task := run.Run.TaskRuns[0]
	if !strings.HasSuffix(task.Status, "FAILED") || task.Error.Code != "NAMESPACE_NOT_SERVED" ||
		!strings.HasSuffix(task.Error.Category, "CONTRACT_VIOLATION") {
		t.Fatalf("task = %+v, want FAILED / NAMESPACE_NOT_SERVED / CONTRACT_VIOLATION", task)
	}
	if !strings.Contains(task.Error.Message, fmt.Sprintf("does not serve namespace %q", refused)) || strings.Contains(task.Error.Message, served) {
		t.Fatalf("task error message = %q, want it to name %q and never the namespaces the Orker serves", task.Error.Message, refused)
	}

	// The Orker wrote nothing of the refused run. The archive body is the
	// engine's, so wait for it, then look for the Orker's own objects.
	var objects map[string][]byte
	deadline := time.Now().Add(2 * time.Minute)
	for {
		objects = s.runObjects(ctx, t, refusedRun)
		archived := false
		for key := range objects {
			archived = archived || strings.HasPrefix(key, "metadata/runs/")
		}
		if archived {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s has no archive body after 2m", refusedRun)
		}
		time.Sleep(500 * time.Millisecond)
	}
	for key := range objects {
		for _, orkers := range []string{"metadata/attempts/", "metadata/logs/", "metadata/commits/"} {
			if strings.HasPrefix(key, orkers) {
				t.Fatalf("the Orker wrote %s for a run of a namespace it does not serve", key)
			}
		}
	}
	s.note("refused run %s: %s / %s; objects of the run: %d, none written by the Orker", refusedRun, task.Error.Code, task.Error.Category, len(objects))
}
