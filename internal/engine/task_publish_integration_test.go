//go:build integration

package engine_test

import (
	"fmt"
	"strings"
	"testing"

	historypb "go.temporal.io/api/history/v1"

	"github.com/orkstack/orkstack/internal/engine"
)

// taskPublishDocument is the fixture's definition: taskPublishTasks plain
// tasks that start together, then one that needs them all.
func taskPublishDocument() string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: publish-window\ntasks:\n")
	needs := make([]string, 0, taskPublishTasks)
	for i := 0; i < taskPublishTasks; i++ {
		fmt.Fprintf(&b, "  - id: t%02d\n    uses: shell\n    needs: []\n    with: {command: \"echo %d\"}\n", i, i)
		needs = append(needs, fmt.Sprintf("t%02d", i))
	}
	fmt.Fprintf(&b, "  - id: merge\n    uses: shell\n    needs: [%s]\n    with: {command: \"echo merge\"}\n", strings.Join(needs, ", "))
	return b.String()
}

// TestIntegration_RecordTaskPublishWindowHistories records the
// task-publish-window fixtures: taskPublishDocument under a plan that pins
// the publish window and a 16 KiB byte budget, so the gated run publishes
// its tasks' events detached, several tasks to a workflow task, and the
// legacy one awaits every publish.
func TestIntegration_RecordTaskPublishWindowHistories(t *testing.T) {
	recordE1Pair(t, "task-publish-window", engine.TaskPublishChange, taskPublishDocument(),
		func(orkerQueue string) engine.RunPlan {
			plan := rawPlan(orkerQueue)
			plan.CommitRoot = "mem://commits"
			limit := engine.DefaultInFlightLimit(engine.DefaultRunMaxInFlight)
			limit.DispatchBytes = taskPublishDispatchBytes
			plan.InFlight = &limit
			return plan
		},
		taskPublishGated, taskPublishLegacy, engine.DisableTaskPublishGateForTest,
		func(t *testing.T, path string, recorded *historypb.History, legacy bool) {
			requireTaskPublishShape(t, path, recorded, legacy)
			runID, _, _, _, _ := startArgs(t, recorded)
			if _, err := replayCapturingDispatch(t, recorded, runID); err != nil {
				t.Fatalf("the history just recorded does not replay against current code: %v", err)
			}
		})
}
