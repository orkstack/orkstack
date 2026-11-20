//go:build integration

package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_ComposedArtifactsRoundTrip passes a file a parent's task
// wrote to its child through a real Orker: the child receives a reference to
// the same object (same URI and SHA-256, no copy), the one task that names
// it reads exactly its bytes at /orkstack/input/artifacts/_inputs/dataset,
// the task that does not name it finds nothing there, and both runs hold a
// claim marker on the object before either is archived.
func TestIntegration_ComposedArtifactsRoundTrip(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("compose-artifacts")
	image := &orkstackv1.ImageRef{Reference: e.alpine, Pinned: e.alpine}
	payload := fmt.Sprintf("dataset of %d", time.Now().UnixNano())
	sum := sha256.Sum256([]byte(payload))
	wantSHA := hex.EncodeToString(sum[:])

	fitWith, _ := structpbStruct(map[string]any{"data": `{{ artifact "_inputs/dataset" }}`})
	inputs, _ := structpbStruct(map[string]any{"dataset": map[string]any{"type": "artifact", "required": true}})
	child := &orkstackv1.Orkflow{
		Name: "train-artifacts", Version: "0.1.0", Inputs: inputs,
		Tasks: []*orkstackv1.Task{
			{Id: "fit", Uses: e.alpine, Image: image, With: fitWith, InputArtifacts: []string{"dataset"},
				Command: []string{"/bin/sh", "-c", `d=$(sha256sum /orkstack/input/artifacts/_inputs/dataset | cut -d' ' -f1) && ` +
					`printf '{"sha":"%s"}' "$d" > /orkstack/output/values.json`},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
			{Id: "report", Uses: e.alpine, Image: image, Needs: []string{"fit"},
				Command: []string{"/bin/sh", "-c", `if test -e /orkstack/input/artifacts/_inputs; then s=staged; else s=absent; fi && ` +
					`printf '{"inputs":"%s"}' "$s" > /orkstack/output/values.json`},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
		},
	}
	stampDigest(t, child)
	parent := &orkstackv1.Orkflow{
		Name: "release-artifacts", Version: "0.1.0",
		Tasks: []*orkstackv1.Task{
			{Id: "build", Uses: e.alpine, Image: image,
				Command: []string{"/bin/sh", "-c", fmt.Sprintf(`printf '%%s' '%s' > /orkstack/output/artifacts/dataset.tar && `+
					`printf '{"ok":true}' > /orkstack/output/values.json`, payload)},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
			{Id: "release", Uses: "orkflow:" + child.GetName() + "@" + child.GetVersion(), Needs: []string{"build"},
				ChildArtifacts: []*orkstackv1.ChildArtifact{{Input: "dataset", TaskId: "build", Name: "dataset.tar"}},
				Orkflow: &orkstackv1.OrkflowRef{Name: child.GetName(), Version: child.GetVersion(),
					DefinitionDigest: child.GetDefinitionDigest()}},
		},
	}
	stampDigest(t, parent)
	runID := fmt.Sprintf("ork-compose-artifacts-%d", time.Now().UnixNano())
	childID := e.startComposed(runID, parent, child)
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("parent = %v %v", run.GetStatus(), run.GetError())
	}
	childRun := e.wait(childID)
	if childRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("child = %v %v", childRun.GetStatus(), childRun.GetError())
	}

	produced := taskRunNamed(run, "build").GetArtifacts()
	if len(produced) != 1 || produced[0].GetName() != "dataset.tar" {
		t.Fatalf("build artifacts = %v", produced)
	}
	received := childRun.GetInputArtifacts()
	if len(received) != 1 || received[0].GetName() != "dataset" ||
		received[0].GetArtifact().GetUri() != produced[0].GetUri() ||
		received[0].GetArtifact().GetSha256() != produced[0].GetSha256() ||
		received[0].GetRunId() != runID || received[0].GetTaskId() != "build" {
		t.Fatalf("child received %v; want the same object build produced, %v", received, produced[0])
	}
	if produced[0].GetSha256() != wantSHA {
		t.Fatalf("build's object is %s, want sha256 %s of the payload", produced[0].GetSha256(), wantSHA)
	}
	passed := taskRunNamed(run, "release").GetChild().GetInputArtifacts()
	if len(passed) != 1 || passed[0].GetArtifact().GetUri() != produced[0].GetUri() {
		t.Fatalf("the composed row records %v, want the passed object", passed)
	}

	fit := readValues(t, e, taskRunNamed(childRun, "fit").GetValuesRef())
	if !strings.Contains(fit, `"sha":"`+wantSHA+`"`) {
		t.Fatalf("fit read %s, want the bytes build wrote (sha256 %s)", fit, wantSHA)
	}
	report := readValues(t, e, taskRunNamed(childRun, "report").GetValuesRef())
	if !strings.Contains(report, `"inputs":"absent"`) {
		t.Fatalf("report names no _inputs and found %s", report)
	}

	// Both runs hold a claim marker on the object.
	archive, err := artifacts.NewRunArchive(e.cfg)
	must.OK(t, err)
	lister, ok := archive.(interface {
		ListObjectReferences(ctx context.Context, objectID string) ([]string, error)
	})
	if !ok {
		t.Fatal("the run archive lists no object references")
	}
	objectID, ok := artifacts.ObjectIDFromURI(produced[0].GetUri())
	if !ok {
		t.Fatalf("%s names no object", produced[0].GetUri())
	}
	holders, err := lister.ListObjectReferences(e.ctx, objectID)
	must.OKf(t, err, "list the object's references")
	for _, id := range []string{runID, childID} {
		if !slices.Contains(holders, id) {
			t.Fatalf("holders of %s = %v, missing %s", objectID, holders, id)
		}
	}
	proof.Write(t, "engine", "composed-artifacts-roundtrip.txt", []byte(fmt.Sprintf(
		"parent=%s child=%s\nobject=%s sha256=%s\nchild.input_artifacts[0].uri=%s\nfit.values=%s\nreport.values=%s\nholders=%v\n",
		runID, childID, produced[0].GetUri(), produced[0].GetSha256(), received[0].GetArtifact().GetUri(),
		fit, report, holders)))
}
