package dsl_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

const examplePath = "../../examples/hello.ork.yaml"

func TestCompile_AcceptsHelloExample(t *testing.T) {
	source, err := os.ReadFile(examplePath)
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	wf, err := dsl.Compile(source)
	if err != nil {
		t.Fatalf("Compile(hello.ork.yaml) = error %v, want success", err)
	}

	if wf.GetName() != "hello-ork" {
		t.Errorf("name = %q, want hello-ork", wf.GetName())
	}
	if wf.GetVersion() != "0.1.0" {
		t.Errorf("version = %q, want pinned 0.1.0", wf.GetVersion())
	}
	if wf.GetSourceYaml() != string(source) {
		t.Errorf("source_yaml does not round-trip the original bytes")
	}
	if got, want := len(wf.GetTasks()), 3; got != want {
		t.Fatalf("tasks = %d, want %d", got, want)
	}

	wantOrder := []string{"fetch", "transform", "save"}
	for i, task := range wf.GetTasks() {
		if task.GetId() != wantOrder[i] {
			t.Errorf("task[%d].id = %q, want %q", i, task.GetId(), wantOrder[i])
		}
		if task.GetTimeoutSeconds() != dsl.DefaultTimeoutSeconds {
			t.Errorf("task %q timeout = %d, want default %d",
				task.GetId(), task.GetTimeoutSeconds(), dsl.DefaultTimeoutSeconds)
		}
	}

	byID := map[string][]string{}
	for _, task := range wf.GetTasks() {
		byID[task.GetId()] = task.GetNeeds()
	}
	if got := byID["transform"]; len(got) != 1 || got[0] != "fetch" {
		t.Errorf("transform.needs = %v, want [fetch]", got)
	}
	if got := byID["save"]; len(got) != 1 || got[0] != "transform" {
		t.Errorf("save.needs = %v, want [transform]", got)
	}

	fetchWith := wf.GetTasks()[0].GetWith().AsMap()
	if fetchWith["url"] != "{{ .inputs.url }}" {
		t.Errorf("fetch.with.url = %v, want the raw un-rendered template", fetchWith["url"])
	}
	if _, ok := wf.GetInputs().GetFields()["url"]; !ok {
		t.Errorf("inputs.url not compiled, got %v", wf.GetInputs().AsMap())
	}
}

func TestCompileFile_AcceptsHelloExample(t *testing.T) {
	wf, err := dsl.CompileFile(examplePath)
	if err != nil {
		t.Fatalf("CompileFile = error %v, want success", err)
	}
	if wf.GetName() != "hello-ork" {
		t.Errorf("name = %q, want hello-ork", wf.GetName())
	}
}

func TestCompile_RejectsInvalidDocuments(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		wantMessage string
	}{
		{
			name: "duplicate task ids",
			yaml: `
name: dup-wf
tasks:
  - id: a
    plugin: shell
    with: {command: "echo 1"}
  - id: a
    plugin: shell
    with: {command: "echo 2"}
`,
			wantMessage: "duplicate task id",
		},
		{
			name: "missing needs target",
			yaml: `
name: missing-need
tasks:
  - id: a
    plugin: shell
    needs: [ghost]
    with: {command: "echo 1"}
`,
			wantMessage: "needs unknown task",
		},
		{
			name: "cyclic needs",
			yaml: `
name: cyclic
tasks:
  - id: a
    plugin: shell
    needs: [b]
    with: {command: "echo a"}
  - id: b
    plugin: shell
    needs: [a]
    with: {command: "echo b"}
`,
			wantMessage: "cycle in needs graph",
		},
		{
			name: "self cycle",
			yaml: `
name: self-cycle
tasks:
  - id: a
    plugin: shell
    needs: [a]
    with: {command: "echo a"}
`,
			wantMessage: "cycle",
		},
		{
			name: "invalid workflow slug",
			yaml: `
name: Hello_Ork
tasks:
  - id: a
    plugin: shell
    with: {command: "echo 1"}
`,
			wantMessage: "is not a slug",
		},
		{
			name: "invalid task slug",
			yaml: `
name: bad-task-slug
tasks:
  - id: Fetch_Thing
    plugin: shell
    with: {command: "echo 1"}
`,
			wantMessage: "is not a slug",
		},
		{
			name: "unparseable template in with",
			yaml: `
name: bad-template
tasks:
  - id: a
    plugin: shell
    with:
      command: "echo {{ .inputs.url "
`,
			wantMessage: "template parse error",
		},
		{
			name: "unparseable template in condition",
			yaml: `
name: bad-condition
tasks:
  - id: a
    plugin: shell
    condition: "{{ if }}"
    with: {command: "echo 1"}
`,
			wantMessage: "template parse error",
		},
		{
			name: "unknown plugin",
			yaml: `
name: unknown-plugin
tasks:
  - id: a
    plugin: quantum
    with: {command: "echo 1"}
`,
			wantMessage: "unknown plugin",
		},
		{
			name: "undeclared input reference",
			yaml: `
name: undeclared-input
tasks:
  - id: a
    plugin: shell
    with:
      command: "echo {{ .inputs.nope }}"
`,
			wantMessage: "undeclared input",
		},
		{
			name: "non-upstream task reference",
			yaml: `
name: non-upstream
tasks:
  - id: a
    plugin: shell
    with: {command: "echo a"}
  - id: b
    plugin: shell
    with:
      command: "echo {{ .tasks.a.output.stdout }}"
`,
			wantMessage: "not in its needs closure",
		},
		{
			name: "non-upstream artifact reference",
			yaml: `
name: non-upstream-artifact
tasks:
  - id: a
    plugin: shell
    with: {command: "echo a"}
  - id: b
    plugin: shell
    with:
      command: "cat {{ artifact \"a/body\" }}"
`,
			wantMessage: "not in its needs closure",
		},
		{
			name: "malformed artifact reference",
			yaml: `
name: bad-artifact
tasks:
  - id: a
    plugin: shell
    with: {command: "echo a"}
  - id: b
    plugin: shell
    needs: [a]
    with:
      command: "cat {{ artifact \"body\" }}"
`,
			wantMessage: "must be \"<task>/<name>\"",
		},
		{
			name: "missing name",
			yaml: `
tasks:
  - id: a
    plugin: shell
    with: {command: "echo 1"}
`,
			wantMessage: "workflow name is required",
		},
		{
			name:        "unparseable yaml",
			yaml:        "name: [oops\n",
			wantMessage: "parse yaml",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("Compile() = nil error, want rejection containing %q", tc.wantMessage)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("Compile() error = %q, want it to contain %q", err.Error(), tc.wantMessage)
			}
		})
	}
}

func TestCompile_AcceptsTransitiveUpstreamReference(t *testing.T) {
	const doc = `
name: transitive
tasks:
  - id: a
    plugin: shell
    with: {command: "echo a"}
  - id: b
    plugin: shell
    needs: [a]
    with: {command: "echo b"}
  - id: c
    plugin: shell
    needs: [b]
    with:
      command: "echo {{ .tasks.a.output.stdout }} {{ artifact \"a/body\" }}"
`
	if _, err := dsl.Compile([]byte(doc)); err != nil {
		t.Fatalf("Compile() = error %v, want acceptance of transitive upstream reference", err)
	}
}

func TestCompile_UnpinnedVersionIsStableContentHash(t *testing.T) {
	const doc = `
name: unpinned
description: hash me
tasks:
  - id: a
    plugin: shell
    with: {command: "echo 1"}
`
	first, err := dsl.Compile([]byte(doc))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(first.GetVersion()) != 64 {
		t.Fatalf("version = %q, want a 64-char sha256 hex hash", first.GetVersion())
	}

	second, err := dsl.Compile([]byte(doc))
	if err != nil {
		t.Fatalf("Compile (repeat): %v", err)
	}
	if first.GetVersion() != second.GetVersion() {
		t.Errorf("version is not stable: %q vs %q", first.GetVersion(), second.GetVersion())
	}

	// Comments and formatting must not change the version.
	reformatted := "# a comment\n" + strings.ReplaceAll(doc, "description: hash me", "description:   hash me")
	third, err := dsl.Compile([]byte(reformatted))
	if err != nil {
		t.Fatalf("Compile (reformatted): %v", err)
	}
	if third.GetVersion() != first.GetVersion() {
		t.Errorf("cosmetic change altered the version: %q vs %q", third.GetVersion(), first.GetVersion())
	}

	// A semantic change must change the version.
	changed := strings.ReplaceAll(doc, "echo 1", "echo 2")
	fourth, err := dsl.Compile([]byte(changed))
	if err != nil {
		t.Fatalf("Compile (changed): %v", err)
	}
	if fourth.GetVersion() == first.GetVersion() {
		t.Errorf("semantic change did not alter the version %q", fourth.GetVersion())
	}
}

func TestCompile_PinnedVersionIsPreservedVerbatim(t *testing.T) {
	const doc = `
name: pinned
version: "  2.0.0-RC1  "
tasks:
  - id: a
    plugin: shell
    with: {command: "echo 1"}
`
	wf, err := dsl.Compile([]byte(doc))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if wf.GetVersion() != "  2.0.0-RC1  " {
		t.Errorf("version = %q, want the pinned value preserved verbatim", wf.GetVersion())
	}
}

func TestCompile_CompilesRetryTimeoutAndQueue(t *testing.T) {
	const doc = `
name: options
tasks:
  - id: a
    plugin: shell
    queue: special-queue
    timeout_seconds: 42
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 2
      backoff_coefficient: 1.5
      maximum_interval_seconds: 10
    with: {command: "echo 1"}
`
	wf, err := dsl.Compile([]byte(doc))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	task := wf.GetTasks()[0]
	if task.GetQueue() != "special-queue" {
		t.Errorf("queue = %q", task.GetQueue())
	}
	if task.GetTimeoutSeconds() != 42 {
		t.Errorf("timeout_seconds = %d, want 42", task.GetTimeoutSeconds())
	}
	retry := task.GetRetry()
	if retry.GetMaximumAttempts() != 3 || retry.GetInitialIntervalSeconds() != 2 ||
		retry.GetBackoffCoefficient() != 1.5 || retry.GetMaximumIntervalSeconds() != 10 {
		t.Errorf("retry = %+v, want the DSL values", retry)
	}
}

func TestCompileFile_ReportsMissingFile(t *testing.T) {
	_, err := dsl.CompileFile(filepath.Join(t.TempDir(), "nope.ork.yaml"))
	if err == nil {
		t.Fatal("CompileFile(missing) = nil error, want failure")
	}
}
