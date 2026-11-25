//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_EachLinesOverPassedArtifacts: on a real Orker, a child run
// fans out over the lines of the artifact it received and returns an artifact
// of its own, and its parent fans out over the lines of that one. Every item
// reads its own line, neither source is staged, and the run that reads a
// source holds a claim on the object.
func TestIntegration_EachLinesOverPassedArtifacts(t *testing.T) {
	e := startEachE2E(t, true)
	e.startLinesOrker("lines-in", nil)
	image := &orkstackv1.ImageRef{Reference: e.alpine, Pinned: e.alpine}
	item, _ := structpbStruct(map[string]any{"n": "{{ .each.item.n }}"})
	echo := `n=$(sed -n 's/.*"n": *"\([0-9]*\)".*/\1/p' /orkstack/input/config.json) && test ! -e /orkstack/input/artifacts/_inputs && ` +
		`test ! -e /orkstack/input/artifacts/release && printf '{"n":%s}' "$n" > /orkstack/output/values.json`
	fan := func(id, producer, name string, needs ...string) *orkstackv1.Task {
		return &orkstackv1.Task{Id: id, Uses: e.alpine, Image: image, Needs: needs, With: item,
			Command: []string{"/bin/sh", "-c", echo}, TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE,
			Each: &orkstackv1.EachSpec{OverArtifact: producer + "/" + name, Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL,
				As: "item", MaxItems: 50, MaxParallel: 4,
				Source: &orkstackv1.EachSource{Artifact: &orkstackv1.EachArtifactSource{Producer: producer, Name: name}}}}
	}
	write := func(id, file string, n int, needs ...string) *orkstackv1.Task {
		return &orkstackv1.Task{Id: id, Uses: e.alpine, Image: image, Needs: needs,
			Command: []string{"/bin/sh", "-c", fmt.Sprintf(`printf '%%s\n' %s > /orkstack/output/artifacts/%s && printf '{}' > /orkstack/output/values.json`,
				shellWords(rowsOf(n)), file)},
			TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE}
	}
	inputs, _ := structpbStruct(map[string]any{"rows": map[string]any{"type": "artifact", "required": true}})
	child := &orkstackv1.Orkflow{Name: "lines-child", Version: "0.1.0", Inputs: inputs,
		OutputArtifacts: map[string]*orkstackv1.OutputArtifact{"more": {TaskId: "list", Name: "more.jsonl"}},
		Tasks:           []*orkstackv1.Task{fan("convert", "_inputs", "rows"), write("list", "more.jsonl", 3, "convert")}}
	stampDigest(t, child)
	parent := &orkstackv1.Orkflow{Name: "lines-parent", Version: "0.1.0", Tasks: []*orkstackv1.Task{
		write("build", "rows.jsonl", 5),
		{Id: "release", Uses: "orkflow:lines-child@0.1.0", Needs: []string{"build"},
			ChildArtifacts: []*orkstackv1.ChildArtifact{{Input: "rows", TaskId: "build", Name: "rows.jsonl"}},
			Orkflow:        &orkstackv1.OrkflowRef{Name: child.GetName(), Version: child.GetVersion(), DefinitionDigest: child.GetDefinitionDigest()}},
		fan("again", "release", "more", "release"),
	}}
	stampDigest(t, parent)

	childPlan := e.plan()
	childPlan.EachLines = engine.DefaultEachLinesPolicy()
	cp, err := engine.NewChildRunPlan(child, childPlan, false, false)
	must.OK(t, err)
	runID := fmt.Sprintf("ork-lines-in-%d", time.Now().UnixNano())
	cp.RunID = runID + "-child"
	plan := e.plan()
	plan.EachLines = engine.DefaultEachLinesPolicy()
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	claims := engine.DefaultArtifactClaims
	plan.ArtifactClaims = &claims
	starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
	must.OKf(t, starter.StartRun(e.ctx, runID, parent, nil, plan), "StartRun")

	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("parent = %v %v", run.GetStatus(), run.GetError())
	}
	childRun := e.wait(cp.RunID)
	archive, err := artifacts.NewRunArchive(e.cfg)
	must.OK(t, err)
	lister, ok := archive.(interface {
		ListObjectReferences(ctx context.Context, objectID string) ([]string, error)
	})
	if !ok {
		t.Fatal("the run archive lists no object references")
	}
	for _, tc := range []struct {
		run    *orkstackv1.Run
		id     string
		items  int
		holder string
	}{{childRun, "convert", 5, cp.RunID}, {run, "again", 3, runID}} {
		node := taskRunNamed(tc.run, tc.id)
		if len(node.GetItems()) != tc.items {
			t.Fatalf("%s ran %d items, want %d", tc.id, len(node.GetItems()), tc.items)
		}
		for i, it := range node.GetItems() {
			if got := readValues(t, e, it.GetValuesRef()); got != fmt.Sprintf(`{"n":%d}`, i) {
				t.Fatalf("%s item %d read %s", tc.id, i, got)
			}
		}
		objectID, ok := artifacts.ObjectIDFromURI(node.GetEach().GetLines().GetSource().GetUri())
		if !ok {
			t.Fatalf("%s's source names no object: %v", tc.id, node.GetEach().GetLines())
		}
		holders, err := lister.ListObjectReferences(e.ctx, objectID)
		must.OKf(t, err, "list the references of %s's source", tc.id)
		if !slices.Contains(holders, tc.holder) {
			t.Fatalf("holders of %s's source = %v, missing %s", tc.id, holders, tc.holder)
		}
	}
}
