//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// originStack is two real `ork` replicas and one Orker on a private Temporal
// namespace and the shared MinIO bucket, with an OrkStack namespace of the
// test's own so the shared archive's other runs never reach its lists.
type originStack struct {
	temporal    client.Client
	artifactCfg artifacts.Config
	a, b        *engineReplica
	bLog        *lockedBuffer
	namespace   string
	orkflow     string
	transcript  strings.Builder
}

func newOriginStack(ctx context.Context, t *testing.T, label string) *originStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, artifactCfg)

	partition := fmt.Sprintf("ork-origin-%s-%d-%d", label, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)

	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")

	channel := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, partition, channel)
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, partition, channel)
	replicaB, bLog := startObservedReplica(ctx, t, "ork-B", enginePath, address, partition, channel, freeAddress(t))
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	stamp := time.Now().UnixNano() % 1_000_000_000
	s := &originStack{
		temporal:    temporalClient,
		artifactCfg: artifactCfg,
		a:           replicaA,
		b:           replicaB,
		bLog:        bLog,
		namespace:   fmt.Sprintf("origin-%s-%d-%d", label, os.Getpid(), stamp),
		orkflow:     "deploy-service",
	}
	fmt.Fprintf(&s.transcript, "# run origin, two engine replicas (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&s.transcript, "temporal namespace  %s\nnamespace           %s\nreplica A           %s\nreplica B           %s\n\n",
		partition, s.namespace, replicaA.addr, replicaB.addr)
	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace)
	if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: originOrkflowYAML(s.orkflow), Namespace: s.namespace,
	})); err != nil {
		t.Fatalf("ApplyOrkflow on replica A: %v", err)
	}
	return s
}

// cli runs the real orkctl against one replica in the stack's namespace and
// requires the exit code.
func (s *originStack) cli(t *testing.T, replica *engineReplica, want int, args ...string) string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(principalEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	full := append([]string{"-n", s.namespace}, args...)
	var stdout, stderr bytes.Buffer
	code := Execute(append(full, "--server", "http://"+replica.addr), &stdout, &stderr)
	fmt.Fprintf(&s.transcript, "$ orkctl %s    # via %s\n%s%s(exit %d)\n\n",
		strings.Join(quoteArgs(full), " "), replica.name, stdout.String(), stderr.String(), code)
	if code != want {
		t.Fatalf("orkctl %v via %s exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			full, replica.name, code, want, stdout.String(), stderr.String())
	}
	return stdout.String()
}

// awaitTerminal polls one replica until it reports the run finished.
func (s *originStack) awaitTerminal(ctx context.Context, t *testing.T, replica *engineReplica, runID string) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var last *orkstackv1.Run
	for time.Now().Before(deadline) {
		got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: s.namespace}))
		if err == nil {
			last = got.Msg.GetRun()
			switch last.GetStatus() {
			case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED:
				return last
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("replica %s never reported run %s terminal (last %v)", replica.name, runID, last)
	return nil
}

// awaitListed polls `run list ARGS --json` on a replica until want reports
// true for the listed ids: visibility is eventually consistent, and the
// archive row lands a moment after the run finishes.
func (s *originStack) awaitListed(t *testing.T, replica *engineReplica, want func([]string) bool, args ...string) []string {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var ids []string
	for time.Now().Before(deadline) {
		ids = listedIDs(t, s.cli(t, replica, exitOK, append([]string{"--json", "run", "list"}, args...)...))
		if want(ids) {
			return ids
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run list %v via %s never satisfied the expectation; last %v", args, replica.name, ids)
	return nil
}

// awaitListedFinished polls `run list ARGS --json` on a replica until the
// run is listed with a terminal status.
func (s *originStack) awaitListedFinished(t *testing.T, replica *engineReplica, runID string, args ...string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		stdout := s.cli(t, replica, exitOK, append([]string{"--json", "run", "list"}, args...)...)
		var response struct {
			Runs []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"runs"`
		}
		if err := json.Unmarshal([]byte(stdout), &response); err != nil {
			t.Fatalf("decode run list: %v (%s)", err, stdout)
		}
		for _, run := range response.Runs {
			if run.ID != runID {
				continue
			}
			last = run.Status
			for _, terminal := range []string{"SUCCEEDED", "FAILED", "CANCELED"} {
				if strings.HasSuffix(run.Status, terminal) {
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run list %v via %s never listed run %s finished; last status %q", args, replica.name, runID, last)
}

func listedIDs(t *testing.T, stdout string) []string {
	t.Helper()
	var response struct {
		Runs []struct {
			ID string `json:"id"`
		} `json:"runs"`
	}
	if err := json.Unmarshal([]byte(stdout), &response); err != nil {
		t.Fatalf("decode run list: %v (%s)", err, stdout)
	}
	ids := make([]string, 0, len(response.Runs))
	for _, run := range response.Runs {
		ids = append(ids, run.ID)
	}
	return ids
}

func containsID(ids []string, id string) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

// originOf decodes `run get --json` down to its origin object.
func originOf(t *testing.T, stdout string) map[string]string {
	t.Helper()
	var response struct {
		Run struct {
			Origin map[string]string `json:"origin"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &response); err != nil {
		t.Fatalf("decode run get: %v (%s)", err, stdout)
	}
	return response.Run.Origin
}

func (s *originStack) requireBootLine(t *testing.T) string {
	t.Helper()
	for _, line := range strings.Split(s.bLog.String(), "\n") {
		if strings.Contains(line, "connected to temporal") {
			if !strings.Contains(line, "run_origin_index=true") && !strings.Contains(line, `"run_origin_index":true`) {
				t.Fatalf("replica B's boot line does not report the origin index usable: %s", line)
			}
			return line
		}
	}
	t.Fatalf("replica B logged no 'connected to temporal' line:\n%s", s.bLog.String())
	return ""
}

// TestIntegration_OriginSurvivesReplicaAndReplay submits on replica A with a
// reference and reads the origin back on replica B — right after the submit,
// and again after the run finished and was archived — and requires the two
// replicas' `run get --json` to agree byte for byte.
func TestIntegration_OriginSurvivesReplicaAndReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newOriginStack(ctx, t, "replay")
	boot := s.requireBootLine(t)

	const ref = "https://ci.example/acme/svc/jobs/42"
	submitted := s.cli(t, s.a, exitOK, "--json", "run", "submit", s.orkflow, "--origin-ref", ref)
	runID := decodeSubmittedID(t, submitted)

	early := s.cli(t, s.b, exitOK, "--json", "run", "get", runID)
	if origin := originOf(t, early); origin["kind"] != "SUBMIT" || origin["reference"] != ref {
		t.Fatalf("replica B right after the submit reads origin %v, want SUBMIT with the reference", origin)
	}

	s.awaitTerminal(ctx, t, s.b, runID)
	// GetRun answers from the run's own history while run list answers
	// from Temporal visibility, which trails it: a listing taken right
	// after awaitTerminal can still say RUNNING. Wait until each replica's
	// listing shows the run finished before comparing the two listings,
	// or the comparison races the visibility update.
	s.awaitListedFinished(t, s.b, runID, "--filter", "origin.kind=SUBMIT")
	s.awaitListedFinished(t, s.a, runID, "--filter", "origin.kind=SUBMIT")
	onA := s.cli(t, s.a, exitOK, "--json", "run", "get", runID)
	onB := s.cli(t, s.b, exitOK, "--json", "run", "get", runID)
	if onA != onB {
		t.Fatalf("run get --json disagrees between replicas:\nA: %s\nB: %s", onA, onB)
	}
	if origin := originOf(t, onB); origin["kind"] != "SUBMIT" || origin["reference"] != ref {
		t.Fatalf("finished run's origin on B = %v", origin)
	}
	listA := s.cli(t, s.a, exitOK, "--json", "run", "list", "--filter", "origin.kind=SUBMIT")
	listB := s.cli(t, s.b, exitOK, "--json", "run", "list", "--filter", "origin.kind=SUBMIT")
	if listA != listB {
		t.Fatalf("run list --filter origin.kind=SUBMIT disagrees between replicas:\nA: %s\nB: %s", listA, listB)
	}
	human := s.cli(t, s.b, exitOK, "run", "get", runID)
	if !strings.Contains(human, "Origin:     submit") && !strings.Contains(human, "Origin:    submit") {
		t.Fatalf("run get on B does not render the origin:\n%s", human)
	}
	proof.Write(t, "orkctl", "run-origin-two-replica.txt", []byte(s.transcript.String()))
	t.Logf("EVIDENCE boot_line_B=%s", strings.TrimSpace(boot))
	t.Logf("EVIDENCE origin_two_replica run=%s run_get_json_B=%s", runID, compactJSON(t, onB))
}

// TestIntegration_RunAgainRecordsRunAgainAndNeverReusesTheKey re-runs a keyed
// run: the new run has a new id, origin RUN_AGAIN naming the source, and is
// the one run `run list --filter origin.kind=RUN_AGAIN --filter origin.id=SOURCE` lists.
func TestIntegration_RunAgainRecordsRunAgainAndNeverReusesTheKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newOriginStack(ctx, t, "again")

	source := decodeSubmittedID(t, s.cli(t, s.a, exitOK, "--json", "run", "submit", s.orkflow, "--idempotency-key", "k"))
	s.awaitTerminal(ctx, t, s.a, source)
	again := s.cli(t, s.a, exitOK, "--json", "run", "again", source, "--origin-ref", "INC-4412")
	rerun := decodeSubmittedID(t, again)
	if rerun == source {
		t.Fatalf("run again returned the source id %s: the source's key was reused", source)
	}
	if origin := originOf(t, again); origin["kind"] != "RUN_AGAIN" || origin["id"] != source || origin["reference"] != "INC-4412" {
		t.Fatalf("run again origin = %v, want RUN_AGAIN %s", origin, source)
	}
	ids := s.awaitListed(t, s.b, func(ids []string) bool { return len(ids) == 1 && ids[0] == rerun },
		"--filter", "origin.kind=RUN_AGAIN", "--filter", "origin.id="+source)
	t.Logf("EVIDENCE run_again source=%s rerun=%s listed_by_origin=%v", source, rerun, ids)

	refused := s.cli(t, s.b, exitError, "run", "again", "ork-does-not-exist")
	_ = refused
	proof.Write(t, "orkctl", "run-origin-run-again.txt", []byte(s.transcript.String()))
}

// TestIntegration_PreUpgradeRunRendersUnrecorded archives a run with no origin
// — what every run archived before this contract carries — and requires both
// that no origin filter lists it and that `run get` says unrecorded.
func TestIntegration_PreUpgradeRunRendersUnrecorded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newOriginStack(ctx, t, "legacy")

	archive, err := artifacts.NewS3RunArchive(ctx, s.artifactCfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	at := time.Now().UTC().Add(-time.Hour)
	legacyID := fmt.Sprintf("ork-%d-legacy", time.Now().UnixNano())
	if err := engine.NewRunArchiver(archive).Archive(ctx, &orkstackv1.Run{
		Id: legacyID, Namespace: s.namespace, OrkflowName: s.orkflow, OrkflowVersion: "0.1.0",
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(at), StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Second)),
	}, nil); err != nil {
		t.Fatalf("archive a pre-origin run: %v", err)
	}
	s.awaitListed(t, s.b, func(ids []string) bool { return containsID(ids, legacyID) })
	for _, kind := range []string{"SUBMIT", "TRIGGER", "SCHEDULE", "PARENT_RUN", "RUN_AGAIN"} {
		if ids := listedIDs(t, s.cli(t, s.b, exitOK, "--json", "run", "list", "--filter", "origin.kind="+kind)); containsID(ids, legacyID) {
			t.Fatalf("--filter origin.kind=%s lists the pre-origin run %s", kind, legacyID)
		}
	}
	human := s.cli(t, s.b, exitOK, "run", "get", legacyID)
	if !strings.Contains(human, "unrecorded (started before origins were recorded)") {
		t.Fatalf("run get of a pre-origin run does not say unrecorded:\n%s", human)
	}
	if strings.Contains(human, "Origin:    submit") || strings.Contains(human, "Origin:     submit") {
		t.Fatalf("a pre-origin run rendered as submit:\n%s", human)
	}
	table := s.cli(t, s.b, exitOK, "run", "list")
	for _, line := range strings.Split(table, "\n") {
		if strings.HasPrefix(line, legacyID) && !strings.Contains(line, " - ") {
			t.Fatalf("the list row of a pre-origin run has no dash origin: %q", line)
		}
	}
	proof.Write(t, "orkctl", "run-origin-unrecorded.txt", []byte(s.transcript.String()))
	t.Logf("EVIDENCE unrecorded run=%s", legacyID)
}

// compactJSON renders a JSON document on one line, for an EVIDENCE log line.
func compactJSON(t *testing.T, document string) string {
	t.Helper()
	var out bytes.Buffer
	if err := json.Compact(&out, []byte(document)); err != nil {
		t.Fatalf("compact %q: %v", document, err)
	}
	return out.String()
}

func decodeSubmittedID(t *testing.T, stdout string) string {
	t.Helper()
	var response struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &response); err != nil || response.Run.ID == "" {
		t.Fatalf("decode submitted run: %v (%s)", err, stdout)
	}
	return response.Run.ID
}

// originOrkflowYAML is one quick shell task.
func originOrkflowYAML(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: run origin probe.
tasks:
  - id: deploy
    uses: shell
    timeout_seconds: 60
    with:
      op: script
      interpreter: sh
      script: |
        printf '{}' > /orkstack/output/values.json
`, name)
}
