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

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The fan-out CLI scenarios (docs/UI_CLI_PARITY.md "Fan-out"): a real engine
// (two for the cross-replica read), a real Orker and real Temporal and MinIO;
// the BUILT orkctl reads every item through the --item commands.

// eachCLIDoc is examples/each-fan-out.ork.yaml's list → convert → merge
// shape, stamped with its own name, on the local first-party images only.
// The parts come from the run's input, so this suite exercises an
// input-sourced expansion; the shipped example computes its list in a json
// task and is run end to end by cmd/orkctl/examples_integration_test.go.
// The upstream-sourced expansion is also covered by the engine's fan-out
// suite (real Orker and containers).
// `convert` prints its task.json and config.json between markers, so the
// local-item test can
// compare a local item run with the Orker's own, and item 2 exits 1 on its
// first attempt so one item retries.
func eachCLIDoc(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
inputs:
  parts:
    type: array
tasks:
  - id: list
    uses: shell
    with:
      op: exec
      command: [sh, -c, "echo listing parts"]
  - id: convert
    uses: shell
    needs: [list]
    each:
      over: '{{ .inputs.parts }}'
      as: part
      max_items: 50
      max_parallel: 2
    retry: { maximum_attempts: 2 }
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        echo '<<<CONFIG'
        cat /orkstack/input/config.json
        echo 'CONFIG>>>'
        echo '<<<TASK'
        cat /orkstack/input/task.json
        echo 'TASK>>>'
        printf '%%s\n' {{ sh .each.part.name }} > /orkstack/output/artifacts/part.txt
        if grep -q '"index": 2,' /orkstack/input/task.json && grep -q '"attempt": 1,' /orkstack/input/task.json; then exit 1; fi
  - id: merge
    uses: shell
    needs: [convert]
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        cat /orkstack/input/artifacts/convert/*/part.txt | tee /orkstack/output/artifacts/merged.txt
`, name)
}

// eachCLIParts is the array convert fans out over, one item per part.
const eachCLIParts = `{"parts":[{"name":"a"},{"name":"b"},{"name":"c"},{"name":"d"}]}`

func eachRun(ctx context.Context, t *testing.T, s *timingStack) *orkstackv1.Run {
	t.Helper()
	name := fmt.Sprintf("each-cli-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(eachCLIDoc(name))
	runID := s.submit(name, "--input-json", eachCLIParts)
	run := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s finished %s: %v", runID, run.GetStatus(), run.GetError())
	}
	return run
}

func TestIntegration_CLIRunGetExpandLogsArtifactsAttemptsForItems(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	s.startOrker()
	run := eachRun(ctx, t, s)
	id := run.GetId()

	got := s.mustCLI(0, "run", "get", id)
	for _, want := range []string{"SUCCEEDED 4/4", "convert: 4 items — 4 succeeded, 1 retried", "--expand convert"} {
		if !strings.Contains(got, want) {
			t.Fatalf("run get lacks %q:\n%s", want, got)
		}
	}
	expanded := s.mustCLI(0, "run", "get", id, "--expand", "convert")
	if !strings.Contains(expanded, "convert — 4 items") {
		t.Fatalf("--expand:\n%s", expanded)
	}
	hint := s.mustCLI(0, "run", "logs", id)
	if !strings.Contains(hint, "convert: fan-out of 4 items; read one with") {
		t.Fatalf("run logs hint:\n%s", hint)
	}
	item := s.mustCLI(0, "run", "logs", id, "--task", "convert", "--item", "1")
	if !strings.Contains(item, `"index": 1`) {
		t.Fatalf("item 1's log:\n%s", item)
	}
	listed := s.mustCLI(0, "run", "artifacts", id, "--task", "convert")
	if !strings.Contains(listed, "ITEM") || strings.Count(listed, "part.txt") != 4 {
		t.Fatalf("run artifacts:\n%s", listed)
	}
	attempts := s.mustCLI(0, "run", "attempts", id, "--task", "convert", "--item", "2")
	if !strings.Contains(attempts, "FAILED") || !strings.Contains(attempts, "SUCCEEDED") {
		t.Fatalf("item 2's attempts:\n%s", attempts)
	}
	download := filepath.Join(t.TempDir(), "part.txt")
	s.mustCLI(0, "run", "artifact", "download", id, "--task", "convert", "--item", "3",
		"--kind", "output", "--name", "part.txt", "--output", download)
	if body, _ := os.ReadFile(download); string(body) != "d\n" {
		t.Fatalf("item 3's part.txt = %q", body)
	}
	machine := s.mustCLI(0, "--json", "run", "get", id)
	if !strings.Contains(machine, `"items"`) {
		t.Fatalf("--json run get carries no items:\n%s", machine)
	}
	s.mustCLI(0, "run", "timing", id, "--task", "convert")
	s.writeTranscript("each-C1.txt")
}

// A run replica A started, read in full on replica B.
func TestIntegration_EachReadsOnASecondReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 2)
	s.startOrker()
	run := eachRun(ctx, t, s)
	id := run.GetId()
	if got := s.mustCLI(1, "run", "get", id, "--expand", "convert"); !strings.Contains(got, "convert — 4 items") {
		t.Fatalf("replica B run get --expand:\n%s", got)
	}
	if got := s.mustCLI(1, "run", "logs", id, "--task", "convert", "--item", "0"); !strings.Contains(got, `"index": 0`) {
		t.Fatalf("replica B item 0 log:\n%s", got)
	}
	if got := s.mustCLI(1, "run", "attempts", id, "--task", "convert", "--item", "2"); !strings.Contains(got, "FAILED") {
		t.Fatalf("replica B item 2 attempts:\n%s", got)
	}
	s.writeTranscript("each-C2.txt")
}

// `orkctl task run --from-run … --task convert --item 2` renders the
// config.json and task.json `each` the Orker rendered.
func TestIntegration_TaskRunOneItemLocally(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	stack := newTaskRunStack(ctx, t)
	run := eachRun(ctx, t, stack.timingStack)
	stdout, stderr, code := stack.taskRun("--from-run", run.GetId(), "--task", "convert", "--item", "2", "--keep")
	// A local run is one attempt, attempt 1, and item 2 exits 1 on its first
	// attempt by design, so task run reports that failure (exitRunFailed).
	// What this compares is the rendering the container saw, not its outcome.
	if code != exitRunFailed || !strings.Contains(stdout, "CONTAINER_EXIT_NONZERO") {
		t.Fatalf("task run --item 2 exit = %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	session := keptSession(t, stdout)
	production := stack.mustCLI(0, "run", "logs", run.GetId(), "--task", "convert", "--item", "2")
	localConfig, err := os.ReadFile(filepath.Join(session, "input", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(localConfig) != between(t, production, "CONFIG") {
		t.Fatalf("config.json differs\nlocal:\n%s\nproduction:\n%s", localConfig, between(t, production, "CONFIG"))
	}
	localTask, err := os.ReadFile(filepath.Join(session, "input", "task.json"))
	if err != nil {
		t.Fatal(err)
	}
	var local, remote map[string]any
	_ = json.Unmarshal(localTask, &local)
	_ = json.Unmarshal([]byte(between(t, production, "TASK")), &remote)
	localEach, _ := json.Marshal(local["each"])
	remoteEach, _ := json.Marshal(remote["each"])
	if !bytes.Equal(localEach, remoteEach) || len(localEach) < 10 {
		t.Fatalf("task.json each differs\nlocal:  %s\nremote: %s", localEach, remoteEach)
	}
	t.Logf("EVIDENCE item 2: config.json byte-identical, task.json each %s", localEach)
	stack.writeTranscript("each-C3.txt")
}
