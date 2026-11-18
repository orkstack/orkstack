package dsl

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// gpuDoc is one task with the given resources.gpu line (empty for none) and
// extra task lines.
func gpuDoc(gpu string, extra ...string) string {
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: gpu\n"
	for _, l := range extra {
		if strings.HasPrefix(l, "pin: ") {
			doc += "pins: {" + strings.TrimPrefix(l, "pin: ") + ": {}}\n"
		}
	}
	doc += "tasks:\n  - id: a\n    uses: shell\n"
	for _, l := range extra {
		doc += "    " + l + "\n"
	}
	if gpu != "" {
		doc += "    resources:\n      gpu: " + gpu + "\n"
	}
	return doc
}

// TestCompile_ResourcesGPUCountsUnits: `resources.gpu` is how many units of
// the task's gpu profile it holds, 1 to MaxTaskGPU, and it needs a profile
// named in `devices:`; whether that profile is a gpu one is the Orker's to
// say, at submit. A pinned task holds no GPU in v0.4.0.
func TestCompile_ResourcesGPUCountsUnits(t *testing.T) {
	wf, err := Compile([]byte(gpuDoc("2", "devices: [amd]")))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := wf.GetTasks()[0].GetResources().GetGpu(); got != 2 {
		t.Errorf("resources.gpu = %d, want 2", got)
	}
	for _, tc := range []struct {
		name, doc, want string
	}{
		{"zero", gpuDoc("0", "devices: [amd]"), "1 to 64"},
		{"negative", gpuDoc("-1", "devices: [amd]"), "1 to 64"},
		{"above the maximum", gpuDoc("65", "devices: [amd]"), "1 to 64"},
		{"no profile", gpuDoc("1"), "devices:"},
		{"pinned", gpuDoc("1", "devices: [amd]", "pin: build"), "pin:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), `task "a"`) {
				t.Fatalf("Compile = %v, want an error naming task a and %q", err, tc.want)
			}
		})
	}
}

// TestCompile_ResourcesGPURefusedWhereNoContainerRuns: an awaited, sleeping
// or composed task runs no container, so it holds no GPU.
func TestCompile_ResourcesGPURefusedWhereNoContainerRuns(t *testing.T) {
	for name, body := range map[string]string{
		"await":   "    await:\n      timeout_seconds: 60\n",
		"sleep":   "    sleep:\n      seconds: 5\n",
		"compose": "    uses: orkflow:child@1.0.0\n",
	} {
		t.Run(name, func(t *testing.T) {
			doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: d\ntasks:\n  - id: a\n" + body + "    resources:\n      gpu: 1\n"
			if _, err := Compile([]byte(doc)); err == nil || !strings.Contains(err.Error(), "resources") {
				t.Fatalf("Compile = %v, want resources refused", err)
			}
		})
	}
}

// TestCanonical_GPUIsInTheDigestOnlyWhenSet: a definition with no gpu count
// emits none, so every digest stored before resources.gpu existed is
// unchanged; two counts are two definitions.
func TestCanonical_GPUIsInTheDigestOnlyWhenSet(t *testing.T) {
	digest := func(doc string) string {
		t.Helper()
		wf, err := Compile([]byte(doc))
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		d, err := DefinitionDigest(wf)
		if err != nil {
			t.Fatalf("DefinitionDigest: %v", err)
		}
		return d
	}
	if digest(gpuDoc("1", "devices: [amd]")) == digest(gpuDoc("2", "devices: [amd]")) {
		t.Error("two gpu counts give one digest")
	}
	wf, err := Compile([]byte(gpuDoc("", "resources: {cpu: \"1\"}")))
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalOrkflow(wf)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"gpu":`) {
		t.Errorf("a task with no gpu count emits one: %s", b)
	}
}

// TestValidate_GPUFromAProto holds a definition arriving as a proto to the
// same rules as one compiled from YAML.
func TestValidate_GPUFromAProto(t *testing.T) {
	for name, task := range map[string]*orkstackv1.Task{
		"above the maximum": {Id: "a", Uses: "shell", Devices: []string{"amd"}, Resources: &orkstackv1.ResourceRequest{Gpu: MaxTaskGPU + 1}},
		"negative":          {Id: "a", Uses: "shell", Devices: []string{"amd"}, Resources: &orkstackv1.ResourceRequest{Gpu: -1}},
		"no profile":        {Id: "a", Uses: "shell", Resources: &orkstackv1.ResourceRequest{Gpu: 1}},
		"pinned":            {Id: "a", Uses: "shell", Pin: "build", Devices: []string{"amd"}, Resources: &orkstackv1.ResourceRequest{Gpu: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			wf := &orkstackv1.Orkflow{Name: "gpu", Version: "1", Pins: []*orkstackv1.Pin{{Name: "build"}}, Tasks: []*orkstackv1.Task{task}}
			if err := validate(wf, Options{}); err == nil || !strings.Contains(err.Error(), "gpu") {
				t.Fatalf("validate = %v, want a gpu refusal", err)
			}
		})
	}
}
