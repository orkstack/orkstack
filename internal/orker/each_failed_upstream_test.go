package orker

import (
	"context"
	"strings"
	"testing"
)

// TestStageInputs_EachUpstreamSkipsFailedItems: a failure handler of an
// on_item_failure: continue fan-out stages only the items that succeeded —
// a failed item has no commit — and reads failed in task.json.
func TestStageInputs_EachUpstreamSkipsFailedItems(t *testing.T) {
	h := newEachHarness(t)
	const root = "mem://commits"
	for _, i := range []int32{0, 2, 4} {
		h.writeItemCommit(t, root, i, nil)
	}
	in := mergeInput(5, root)
	in.Upstream[0].Each.Failed = []int32{1, 3}
	if _, err := h.dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	staged := strings.Join(h.runtime.staged[0], "\n")
	for _, want := range []string{"upstream/convert/00000/values.json", "upstream/convert/00004/values.json"} {
		if !strings.Contains(staged, want) {
			t.Fatalf("staged files lack %s:\n%s", want, staged)
		}
	}
	if strings.Contains(staged, "convert/00001") || strings.Contains(staged, "convert/00003") {
		t.Fatalf("a failed item was staged:\n%s", staged)
	}
	inputs := h.runtime.task[0]["inputs"].(map[string]any)
	eachUp := inputs["eachUpstream"].([]any)[0].(map[string]any)
	failed, _ := eachUp["failed"].([]any)
	if eachUp["count"] != float64(5) || len(failed) != 2 || failed[0] != float64(1) || failed[1] != float64(3) {
		t.Fatalf("inputs.eachUpstream = %v; want count 5 and failed [1 3]", eachUp)
	}
	if n := len(inputs["upstream"].([]any)); n != 3 {
		t.Fatalf("%d upstream entries; want the 3 items that succeeded", n)
	}
}

// TestStageInputs_EachUpstreamWithoutFailuresHasNoFailedKey: an ordinary
// fan-out upstream's task.json carries no failed key at all.
func TestStageInputs_EachUpstreamWithoutFailuresHasNoFailedKey(t *testing.T) {
	h := newEachHarness(t)
	const root = "mem://commits"
	h.writeItemCommit(t, root, 0, nil)
	if _, err := h.dispatcher.Execute(context.Background(), mergeInput(1, root)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	eachUp := h.runtime.task[0]["inputs"].(map[string]any)["eachUpstream"].([]any)[0].(map[string]any)
	if _, ok := eachUp["failed"]; ok {
		t.Fatalf("inputs.eachUpstream = %v; want no failed key", eachUp)
	}
}

// TestStageInputs_EachUpstreamRefusesAMalformedFailedList: failed must be
// ascending, unique and within count — the engine's own list; anything else
// is refused before anything is read.
func TestStageInputs_EachUpstreamRefusesAMalformedFailedList(t *testing.T) {
	for _, failed := range [][]int32{{3, 1}, {1, 1}, {5}, {-1}} {
		h := newEachHarness(t)
		in := mergeInput(5, "mem://commits")
		in.Upstream[0].Each.Failed = failed
		_, err := h.dispatcher.Execute(context.Background(), in)
		if err == nil || !strings.Contains(err.Error(), "failed items") {
			t.Fatalf("failed %v: Execute = %v; want the malformed list refused", failed, err)
		}
	}
}
