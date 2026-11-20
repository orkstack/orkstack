//go:build integration

package engine_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestIntegration_EachContinueHandlerStagesTheSucceededItems: on a real Orker,
// a continue fan-out of five items whose items 1 and 3 exit 1 runs every
// item, then fails; its failure handler is staged the three items that
// succeeded and reads inputs.eachUpstream[].failed = [1,3] from task.json.
func TestIntegration_EachContinueHandlerStagesTheSucceededItems(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("each-continue")
	failTwo := `idx=$(sed -n 's/.*"index": *"\([0-9]*\)".*/\1/p' /orkstack/input/config.json) && ` +
		`case "$idx" in 1|3) exit 1;; esac && `
	wf := e.eachDefinition("each-continue", 5, 0, failTwo)
	wf.Tasks[1].Each.OnItemFailure = orkstackv1.EachItemFailure_EACH_ITEM_FAILURE_CONTINUE
	wf.Tasks[1].Retry = nil // an exit 1 fails for good at once
	report := `n=$(ls /orkstack/input/upstream/convert | wc -l) && ` +
		`f=$(tr -d ' \n' < /orkstack/input/task.json | sed -n 's/.*"failed":\[\([0-9,]*\)\].*/\1/p') && ` +
		`printf '{"staged":%d,"failed":"%s"}' "$n" "$f" > /orkstack/output/values.json`
	wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: "report", Uses: e.alpine, Image: wf.Tasks[0].Image,
		Needs: []string{"convert"}, Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED,
		Command: []string{"/bin/sh", "-c", report}, TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE})
	runID := fmt.Sprintf("ork-each-continue-%d", time.Now().UnixNano())
	e.start(runID, wf)
	run := e.wait(runID)
	node := taskRunNamed(run, "convert")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
		!strings.HasPrefix(node.GetError().GetMessage(), "2 of 5 items failed; item 1: ") {
		t.Fatalf("run %v, convert %v %v; want the fan-out failed by items 1 and 3", run.GetStatus(), node.GetStatus(), node.GetError())
	}
	for i, item := range node.GetItems() {
		want := orkstackv1.Status_STATUS_SUCCEEDED
		if i == 1 || i == 3 {
			want = orkstackv1.Status_STATUS_FAILED
		}
		if item.GetStatus() != want {
			t.Fatalf("item %d = %v, want %v", i, item.GetStatus(), want)
		}
	}
	got := readValues(t, e, taskRunNamed(run, "report").GetValuesRef())
	if got != `{"staged":3,"failed":"1,3"}` {
		t.Fatalf("report read %s; want the three items that succeeded staged and failed [1,3]", got)
	}
}
