//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// flatShellDoc is an Orkflow of n independent `uses: shell` tasks.
func flatShellDoc(name string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	for i := range n {
		fmt.Fprintf(&b, "  - id: t%d\n    uses: shell\n    with: {op: exec, command: [echo, ok]}\n", i)
	}
	return b.String()
}

// TestIntegration_EngineBootsAtTheRaisedTaskCeiling: the real ork binary at
// Stage 7's defaults logs a task ceiling of the measured maximum with the
// flat-stage bound, definitions by reference and snapshot overlays on, and
// validates an Orkflow at the ceiling while refusing one task more; raised
// above the measured maximum, and above the flat-stage bound of a long
// restore timeout, it starts with both warnings (D2: warn only); with
// continuation off an explicit ceiling above history.max_events / 50 refuses
// to boot, naming the three keys.
func TestIntegration_EngineBootsAtTheRaisedTaskCeiling(t *testing.T) {
	temporal := requireTemporal(t)
	_ = requireMinIO(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	ns := fmt.Sprintf("%s-ceiling-%d-%d", base, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	binary := buildOrk(t)
	n := int(engine.DefaultOrkflowMaxTasks)

	_, orkflows, log := bootEngine(ctx, t, binary, temporal, "engine-ceiling-default", "ORK_TEMPORAL_NAMESPACE="+ns)
	for _, want := range []string{
		fmt.Sprintf("orkflow_max_tasks=%d", n), "explicit=false", fmt.Sprintf("measured_max=%d", n),
		"flat_stage_bound=4222", "definition_by_reference=true", "snapshot_overlays=true",
	} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("the default boot log does not say %q:\n%s", want, log.String())
		}
	}
	if strings.Contains(log.String(), "runs unmeasured") || strings.Contains(log.String(), "flat-stage bound for") {
		t.Fatalf("the default boot warned about its task ceiling:\n%s", log.String())
	}
	for _, tc := range []struct {
		tasks  int
		issues int
	}{{n, 0}, {n + 1, 1}} {
		resp, err := orkflows.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
			Yaml: flatShellDoc("ceiling", tc.tasks)}))
		if err != nil {
			t.Fatalf("validate %d tasks: %v", tc.tasks, err)
		}
		if got := len(resp.Msg.GetIssues()); got != tc.issues {
			t.Fatalf("validate %d tasks: %d issues %v, want %d", tc.tasks, got, resp.Msg.GetIssues(), tc.issues)
		}
		if tc.issues == 1 && resp.Msg.GetIssues()[0].GetField() != fmt.Sprintf("tasks[%d]", n) {
			t.Fatalf("validate %d tasks: issue at %q, want tasks[%d]", tc.tasks, resp.Msg.GetIssues()[0].GetField(), n)
		}
	}
	stopEngine(t, "engine-ceiling-default")

	_, _, log = bootEngine(ctx, t, binary, temporal, "engine-ceiling-raised", "ORK_TEMPORAL_NAMESPACE="+ns,
		fmt.Sprintf("ORK_ORKFLOW_MAX_TASKS=%d", n+1000), "ORK_CONTINUATION_RESTORE_TIMEOUT=8h")
	for _, want := range []string{
		fmt.Sprintf("is above the largest size measured (%d): this deployment runs unmeasured", n),
		"is above 3041, the flat-stage bound for", "plain tasks that start together may stop RUN_HISTORY_LIMIT",
	} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("the raised boot log does not warn %q:\n%s", want, log.String())
		}
	}
	stopEngine(t, "engine-ceiling-raised")

	_, refused := startEngine(ctx, t, binary, temporal, "engine-ceiling-refused", "ORK_TEMPORAL_NAMESPACE="+ns,
		"ORK_CONTINUATION_ENABLED=false", "ORK_ORKFLOW_MAX_TASKS=5000")
	select {
	case <-refused.done:
	case <-ctx.Done():
		t.Fatalf("an explicit 5,000 with continuation off kept running:\n%s", refused.log.String())
	}
	for _, want := range []string{"ORK_ORKFLOW_MAX_TASKS=5000", "engine.history.max_events", "engine.continuation.enabled"} {
		if !strings.Contains(refused.log.String(), want) {
			t.Fatalf("the refused boot does not name %q:\n%s", want, refused.log.String())
		}
	}
	t.Logf("default: ceiling %d, flat-stage bound 4222, no warning; raised to %d with restore_timeout 8h: both warnings; "+
		"5000 with continuation off: refused at boot", n, n+1000)
}
