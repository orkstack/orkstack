package api

import (
	"fmt"
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

// TestCheckEachLimits_LinesAtCeilingWithManyUpstreams: at the 10,000-item
// ceiling every item's share of the input budget is small, and the lines
// allowance is charged inside it: a with: that fits an array fan-out's share
// but not with the allowance is refused, the same with: half an allowance
// smaller is accepted. How many tasks the fan-out needs does not change
// apply's answer — their references are known only at run time, where the
// engine's exact check holds them to the same bound.
func TestCheckEachLimits_LinesAtCeilingWithManyUpstreams(t *testing.T) {
	const ceiling = dsl.MaxEachItems
	doc := func(each string, upstreams, pad int) *orkstackv1.Orkflow {
		t.Helper()
		var b strings.Builder
		b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ninputs:\n  l:\n    type: array\ntasks:\n")
		b.WriteString("  - id: list\n    uses: busybox:1\n")
		needs := []string{"list"}
		for i := 0; i < upstreams-1; i++ {
			fmt.Fprintf(&b, "  - id: up%d\n    uses: busybox:1\n", i)
			needs = append(needs, fmt.Sprintf("up%d", i))
		}
		fmt.Fprintf(&b, "  - id: convert\n    uses: busybox:1\n    needs: [%s]\n    each:\n%s    with:\n      pad: '%s'\n",
			strings.Join(needs, ", "), each, strings.Repeat("x", pad))
		wf, err := dsl.Compile([]byte(b.String()))
		if err != nil {
			t.Fatal(err)
		}
		return wf
	}
	lines := fmt.Sprintf("      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: %d\n", ceiling)
	array := fmt.Sprintf("      over: '{{ .inputs.l }}'\n      max_items: %d\n", ceiling)
	s := &Service{eachMaxItems: ceiling, limits: engine.WorkspaceLimits{ValuesMaxBytes: 1 << 20}}
	share, _ := engine.EachItemBounds(doc(lines, 1, 0), s.limits, s.itemInputFloor())
	if share <= eachLinesRefAllowance {
		t.Fatalf("the per-item share at %d items is %d bytes, no room for the %d-byte allowance", ceiling, share, eachLinesRefAllowance)
	}
	// The input every item carries besides its pad: measured, not guessed.
	conv := doc(lines, 1, 0).GetTasks()[1]
	base, err := task.InputSize(&orkstackv1.ExecuteTaskInput{Image: conv.GetImage(), Command: conv.GetCommand(), With: conv.GetWith()})
	if err != nil {
		t.Fatal(err)
	}
	tight := int(share-eachLinesRefAllowance-base) - eachLinesRefAllowance/4 // fits with the allowance
	over := tight + eachLinesRefAllowance/2                                  // fits an array's share, not a lines one's
	for _, upstreams := range []int{1, 20} {
		t.Run(fmt.Sprintf("%d upstreams", upstreams), func(t *testing.T) {
			if err := s.checkEachLimits(doc(lines, upstreams, tight)); err != nil {
				t.Fatalf("a lines fan-out within its share at the ceiling was refused: %v", err)
			}
			if err := s.checkEachLimits(doc(array, upstreams, over)); err != nil {
				t.Fatalf("the array fan-out of the same with: was refused: %v", err)
			}
			err := s.checkEachLimits(doc(lines, upstreams, over))
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("declare %d items", ceiling)) {
				t.Fatalf("checkEachLimits(lines over its share) = %v; want the per-item refusal at %d items", err, ceiling)
			}
		})
	}
}

// TestCheckEachLimits_LinesAllowanceWithRunContext: an item's run context is
// outside the bound its input is held to, so create charges a lines fan-out
// its line references and nothing for the run context: one whose own input
// is exactly an allowance under the bound is accepted, and one byte less of
// bound refuses it. At dispatch the largest run context a run may pin does
// not move the item past that same bound (userInputSize,
// TestEachLines_RunContextIsOutsideTheItemBound).
func TestCheckEachLimits_LinesAllowanceWithRunContext(t *testing.T) {
	wf, err := dsl.Compile([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ntasks:\n" +
		"  - id: list\n    uses: busybox:1\n" +
		"  - id: convert\n    uses: busybox:1\n    needs: [list]\n    each:\n" +
		"      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 1\n" +
		"    with:\n      pad: '" + strings.Repeat("x", 4096) + "'\n"))
	if err != nil {
		t.Fatal(err)
	}
	conv := wf.GetTasks()[1]
	size, err := task.InputSize(&orkstackv1.ExecuteTaskInput{Image: conv.GetImage(), Command: conv.GetCommand(), With: conv.GetWith()})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{eachMaxItems: 10000, limits: engine.WorkspaceLimits{ValuesMaxBytes: size + eachLinesRefAllowance}}
	if err := s.checkEachLimits(wf); err != nil {
		t.Fatalf("a fan-out exactly its allowance under the bound was refused: %v", err)
	}
	s.limits.ValuesMaxBytes--
	if err := s.checkEachLimits(wf); err == nil {
		t.Fatal("a fan-out one byte over its allowance was accepted")
	}
}
