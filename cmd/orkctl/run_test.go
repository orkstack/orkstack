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
