package dsl

import (
	"slices"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func devicesDoc(lines ...string) string {
	doc := `apiVersion: orkstack/v1
kind: Orkflow
name: devices
tasks:
  - id: a
    uses: shell
    devices:
`
	for _, l := range lines {
		doc += "      - " + l + "\n"
	}
	return doc
}

// TestCompile_DevicesNamesProfiles: `devices:` lists the device and gpu
// profiles a task holds by name, unique, at most MaxTaskDevices, each a
// slug; what the Orker defines them as is checked at submit and dispatch.
func TestCompile_DevicesNamesProfiles(t *testing.T) {
	wf, err := Compile([]byte(devicesDoc("sensor", "amd")))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := wf.GetTasks()[0].GetDevices(); !slices.Equal(got, []string{"sensor", "amd"}) {
		t.Errorf("devices = %v", got)
	}
	nine := make([]string, MaxTaskDevices+1)
	for i := range nine {
		nine[i] = "d" + string(rune('a'+i))
	}
	for _, tc := range []struct {
		name, doc, want string
	}{
		{"a duplicate", devicesDoc("sensor", "sensor"), "twice"},
		{"not a slug", devicesDoc("Sensor"), "Sensor"},
		{"too many", devicesDoc(nine...), "at most"},
		{"empty list", strings.Replace(devicesDoc(), "    devices:\n", "    devices: []\n", 1), "at least one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), `task "a"`) {
				t.Fatalf("Compile = %v, want an error naming task a and %q", err, tc.want)
			}
		})
	}
}

// TestCompile_DevicesRefusedWhereNoContainerRuns: an awaited, sleeping or
// composed task runs no container, so it holds no device.
func TestCompile_DevicesRefusedWhereNoContainerRuns(t *testing.T) {
	for name, body := range map[string]string{
		"await":   "    await:\n      timeout_seconds: 60\n",
		"sleep":   "    sleep:\n      seconds: 5\n",
		"compose": "    uses: orkflow:child@1.0.0\n",
	} {
		t.Run(name, func(t *testing.T) {
			doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: d\ntasks:\n  - id: a\n" + body + "    devices:\n      - sensor\n"
			if _, err := Compile([]byte(doc)); err == nil || !strings.Contains(err.Error(), "devices") {
				t.Fatalf("Compile = %v, want devices refused", err)
			}
		})
	}
}

// TestCanonical_DevicesAreInTheDigest: two definitions that differ only in
// the profiles a task holds are different definitions; one that names none
// emits no devices, so it keeps its digest.
func TestCanonical_DevicesAreInTheDigest(t *testing.T) {
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
	if digest(devicesDoc("sensor")) == digest(devicesDoc("amd")) {
		t.Error("two device profiles give one digest")
	}
	wf, err := Compile([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: devices\ntasks:\n  - id: a\n    uses: shell\n"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalOrkflow(wf)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"devices":`) {
		t.Errorf("a task with no devices emits them: %s", b)
	}
}

// TestValidate_DevicesFromAProto holds a definition arriving as a proto to
// the same rules as one compiled from YAML.
func TestValidate_DevicesFromAProto(t *testing.T) {
	wf, err := Compile([]byte(devicesDoc("sensor")))
	if err != nil {
		t.Fatal(err)
	}
	wf.GetTasks()[0].Devices = []string{"sensor", "sensor"}
	if err := validate(wf, Options{}); err == nil {
		t.Error("a duplicate device from a proto validated")
	}
	wf.GetTasks()[0].Devices = []string{"sensor"}
	wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: "w", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}, Devices: []string{"sensor"}})
	if err := validate(wf, Options{}); err == nil {
		t.Error("an awaited task holding a device validated")
	}
}
