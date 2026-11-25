package main

import (
	"bytes"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestRenderRun_ReceivedAndReturnedArtifacts(t *testing.T) {
	var out bytes.Buffer
	run := &orkstackv1.Run{Id: "ork-child", OrkflowName: "train", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "dataset", RunId: "ork-parent", TaskId: "build",
			Artifact: &orkstackv1.ArtifactRef{Metadata: map[string]string{"source_name": "dataset.tar"}}}},
		OutputArtifacts: []*orkstackv1.NamedArtifact{{Name: "model", RunId: "ork-child", TaskId: "fit",
			Artifact: &orkstackv1.ArtifactRef{Metadata: map[string]string{"source_name": "model.bin"}}}}}
	if err := renderRun(&out, run); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Received:  dataset  from run ork-parent, task build (dataset.tar)",
		"Returned:  model    from run ork-child, task fit (model.bin)"} {
		if !strings.Contains(strings.Join(strings.Fields(out.String()), " "), strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("output lacks %q:\n%s", want, out.String())
		}
	}
	var plain bytes.Buffer
	if err := renderRun(&plain, &orkstackv1.Run{Id: "ork-1"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain.String(), "Received") || strings.Contains(plain.String(), "Returned") {
		t.Fatalf("a run with no artifacts prints them:\n%s", plain.String())
	}
}

// A child run says which task composed it and what started its chain; a run
// whose origin names neither prints neither.
func TestRenderRun_ChildRunNamesItsParentTaskAndRoot(t *testing.T) {
	var out bytes.Buffer
	run := &orkstackv1.Run{Id: "ork-grandchild", Origin: &orkstackv1.RunOrigin{
		Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-child", ParentTaskId: "eval", RootRunId: "ork-root",
		ParentOrkflow: &orkstackv1.OrkflowRef{Namespace: "platform", Name: "train", Version: "1.0.0"},
		RootOrkflow:   &orkstackv1.OrkflowRef{Name: "release", Version: "4.1.0"},
		RootOrigin:    &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-1"}}}
	if err := renderRun(&out, run); err != nil {
		t.Fatal(err)
	}
	text := strings.Join(strings.Fields(out.String()), " ")
	for _, want := range []string{"Parent task: task eval of platform/train@1.0.0",
		"Root run: ork-root of release@4.1.0, started by trigger trg-1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output lacks %q:\n%s", want, out.String())
		}
	}
	var plain bytes.Buffer
	if err := renderRun(&plain, &orkstackv1.Run{Id: "ork-1", Origin: &orkstackv1.RunOrigin{
		Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain.String(), "Parent task") || strings.Contains(plain.String(), "Root run") {
		t.Fatalf("a submitted run prints a lineage:\n%s", plain.String())
	}
}
