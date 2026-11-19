package api

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// TestCheckEachLimits_LinesAllowance: a fan-out over an artifact's lines is
// charged eachLinesRefAllowance beyond the input an array fan-out of the same
// with: carries, so one whose own input sits within a KiB of the per-item
// bound is refused at create while the array fan-out is accepted.
func TestCheckEachLimits_LinesAllowance(t *testing.T) {
	const pad = 4096
	doc := func(each string) *orkstackv1.Orkflow {
		t.Helper()
		wf, err := dsl.Compile([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ninputs:\n  l:\n    type: array\ntasks:\n" +
			"  - id: list\n    uses: busybox:1\n" +
			"  - id: convert\n    uses: busybox:1\n    needs: [list]\n    each:\n" + each +
			"    with:\n      pad: '" + strings.Repeat("x", pad) + "'\n"))
		if err != nil {
			t.Fatal(err)
		}
		return wf
	}
	lines := doc("      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 1\n")
	array := doc("      over: '{{ .inputs.l }}'\n      max_items: 1\n")
	conv := lines.GetTasks()[1]
	size, err := task.InputSize(&orkstackv1.ExecuteTaskInput{Image: conv.GetImage(), Command: conv.GetCommand(), With: conv.GetWith()})
	if err != nil {
		t.Fatal(err)
	}
	// A per-item bound half an allowance above the measured input.
	s := &Service{eachMaxItems: 10000, limits: engine.WorkspaceLimits{ValuesMaxBytes: size + eachLinesRefAllowance/2}}
	if err := s.checkEachLimits(array); err != nil {
		t.Fatalf("the array fan-out was refused: %v", err)
	}
	err = s.checkEachLimits(lines)
	if err == nil || !strings.Contains(err.Error(), "bytes of activity input") {
		t.Fatalf("checkEachLimits(lines) = %v; want the per-item input refusal", err)
	}
	s.limits.ValuesMaxBytes = size + eachLinesRefAllowance
	if err := s.checkEachLimits(lines); err != nil {
		t.Fatalf("a lines fan-out within its allowance was refused: %v", err)
	}
}
