package dsl_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
	if wf.GetVersion() != "0.2.0" {
		t.Errorf("version = %q, want pinned 0.2.0", wf.GetVersion())
	}
	if wf.GetSourceYaml() != string(source) {
		t.Errorf("source_yaml does not round-trip the original bytes")
	}
	if got, want := len(wf.GetTasks()), 3; got != want {
		t.Fatalf("tasks = %d, want %d", got, want)
	}

	wantOrder := []string{"fetch", "transform", "verify"}
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
	if got := byID["verify"]; len(got) != 1 || got[0] != "transform" {
		t.Errorf("verify.needs = %v, want [transform]", got)
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

// TestCompile_AcceptsEveryShippedExample keeps examples/ honest: every file we
// ship as documentation must still compile, and every task in it must name the
// generic `op` selector so the examples stay a faithful reference for
// docs/PLUGIN_DESIGN.md.
func TestCompile_AcceptsEveryShippedExample(t *testing.T) {
	paths, err := filepath.Glob("../../examples/*.ork.yaml")
	if err != nil {
		t.Fatalf("glob examples: %v", err)
	}
	if len(paths) < 4 {
		t.Fatalf("found %d examples (%v), want hello, http-post, file-read and shell-eval", len(paths), paths)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			wf, err := dsl.CompileFile(path)
			if err != nil {
				t.Fatalf("CompileFile(%s) = error %v, want success", path, err)
			}
			for _, task := range wf.GetTasks() {
				with := task.GetWith().AsMap()
				op, _ := with["op"].(string)
				if strings.TrimSpace(op) == "" {
					t.Errorf("%s task %q does not set an explicit `op`", filepath.Base(path), task.GetId())
				}
			}
		})
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
    uses: shell
    with: {command: "echo 1"}
  - id: a
    uses: shell
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
    uses: shell
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
    uses: shell
    needs: [b]
    with: {command: "echo a"}
  - id: b
    uses: shell
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
    uses: shell
    needs: [a]
    with: {command: "echo a"}
`,
			wantMessage: "cycle",
		},
		{
			name: "invalid orkflow slug",
			yaml: `
name: Hello_Ork
tasks:
  - id: a
    uses: shell
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
    uses: shell
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
    uses: shell
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
    uses: shell
    condition: "{{ if }}"
    with: {command: "echo 1"}
`,
			wantMessage: "template parse error",
		},
		{
			// `uses:` is REQUIRED. It replaced `plugin:` as the only
			// image-selecting field, and a task that names no activity has
			// nothing to run.
			name: "uses is missing",
			yaml: `
name: no-uses
tasks:
  - id: a
    with: {command: "echo 1"}
`,
			wantMessage: "`uses:` is required",
		},
		{
			// A templated `uses:` cannot compile. Resolution happens at APPLY
			// so a run pins an immutable digest, and a reference selected from
			// run input is not knowable then. This is a deliberate capability
			// loss recorded in §17, not an oversight.
			name: "uses is a template",
			yaml: `
name: templated-uses
inputs:
  image: {type: string, default: "alpine:3.20"}
tasks:
  - id: a
    uses: "{{ .inputs.image }}"
`,
			wantMessage: "cannot be a template",
		},
		{
			// `plugin:` is refused BY NAME rather than ignored. yaml.v3 drops
			// what it cannot map, so a field that merely disappeared from the
			// struct would be accepted in silence and the author would find
			// out when the run behaved differently.
			name: "plugin was removed",
			yaml: `
name: retired-plugin
tasks:
  - id: a
    plugin: shell
    with: {command: "echo 1"}
`,
			wantMessage: "`plugin:` was removed",
		},
		{
			// `uses: container` names a capability that has no compliant
			// image: a container that starts containers needs the docker
			// socket, which §9.1 forbids without exception. It is an error
			// naming the replacement rather than an alias, because the launch
			// contract excludes compatibility shims (§17 C6, C7).
			name: "uses container is refused with its replacement",
			yaml: `
name: retired-container
tasks:
  - id: a
    uses: container
`,
			wantMessage: "has no replacement alias",
		},
		{
			// Exactly two network modes exist. A typo must not silently
			// select the one with full network access.
			name: "unknown network mode",
			yaml: `
name: bad-network
tasks:
  - id: a
    uses: shell
    network: nonw
`,
			wantMessage: "`network:` must be",
		},
		{
			name: "secret without a source",
			yaml: `
name: bad-secret
tasks:
  - id: a
    uses: shell
    secrets:
      - name: db_password
`,
			wantMessage: "needs a `source:`",
		},
		{
			name: "undeclared input reference",
			yaml: `
name: undeclared-input
tasks:
  - id: a
    uses: shell
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
    uses: shell
    with: {command: "echo a"}
  - id: b
    uses: shell
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
    uses: shell
    with: {command: "echo a"}
  - id: b
    uses: shell
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
    uses: shell
    with: {command: "echo a"}
  - id: b
    uses: shell
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
    uses: shell
    with: {command: "echo 1"}
`,
			wantMessage: "orkflow name is required",
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
    uses: shell
    with: {command: "echo a"}
  - id: b
    uses: shell
    needs: [a]
    with: {command: "echo b"}
  - id: c
    uses: shell
    needs: [b]
    with:
      command: "echo {{ .tasks.a.output.stdout }} {{ artifact \"a/body\" }}"
`
	if _, err := dsl.Compile([]byte(doc)); err != nil {
		t.Fatalf("Compile() = error %v, want acceptance of transitive upstream reference", err)
	}
}

// TestCompile_UnpinnedVersionNamesTheBytesItStores is the derived version's
// whole contract: it is the sha256 of the source the document is stored with.
//
// The second half is not decoration. A version names ONE stored document, and
// the store decides "this apply is what is already there" by comparing what it
// holds — source bytes included (pkg/artifacts.sameStoredOrkflow). A
// derivation covering LESS than those bytes names two different documents: a
// comment-only edit compiled to the same version carrying different source,
// and the store then refused it as an attempt to rewrite an immutable version
// — with advice ("publish a new version") its author could not follow, because
// a derived version is not theirs to choose. So the derivation covers exactly
// the bytes that are stored, and the two notions of "the same document" cannot
// disagree.
func TestCompile_UnpinnedVersionNamesTheBytesItStores(t *testing.T) {
	const doc = `
name: unpinned
description: hash me
tasks:
  - id: a
    uses: shell
    with: {command: "echo 1"}
`
	first, err := dsl.Compile([]byte(doc))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(first.GetVersion()) != 64 {
		t.Fatalf("version = %q, want a 64-char sha256 hex hash", first.GetVersion())
	}

	// The same bytes are the same version. This is what makes re-applying an
	// unchanged file a no-op rather than a second version of the same thing.
	second, err := dsl.Compile([]byte(doc))
	if err != nil {
		t.Fatalf("Compile (repeat): %v", err)
	}
	if first.GetVersion() != second.GetVersion() {
		t.Errorf("version is not stable: %q vs %q", first.GetVersion(), second.GetVersion())
	}

	// And the version IS those bytes' hash, so nothing else has to agree with
	// the compiler about what "the same document" means — it can measure it.
	sum := sha256.Sum256([]byte(first.GetSourceYaml()))
	if want := hex.EncodeToString(sum[:]); first.GetVersion() != want {
		t.Errorf("version %q does not name the stored source (sha256 %q)",
			first.GetVersion(), want)
	}

	// Every change to those bytes is a new version, including the ones a
	// compiler could dismiss as cosmetic: a comment is part of the document
	// its author reads, and an edit that adds one has to be storable.
	for what, edited := range map[string]string{
		"a comment":         "# remind me why this exists\n" + doc,
		"whitespace":        strings.ReplaceAll(doc, "description: hash me", "description:   hash me"),
		"an edited command": strings.ReplaceAll(doc, "echo 1", "echo 2"),
		"a reordered mapping": strings.Replace(doc, "name: unpinned\ndescription: hash me",
			"description: hash me\nname: unpinned", 1),
	} {
		changed, err := dsl.Compile([]byte(edited))
		if err != nil {
			t.Fatalf("Compile (%s): %v", what, err)
		}
		if changed.GetVersion() == first.GetVersion() {
			t.Errorf("%s did not change the version %q", what, first.GetVersion())
		}
		sum := sha256.Sum256([]byte(changed.GetSourceYaml()))
		if want := hex.EncodeToString(sum[:]); changed.GetVersion() != want {
			t.Errorf("%s: version %q does not name the stored source (sha256 %q)",
				what, changed.GetVersion(), want)
		}
	}
}

func TestCompile_PinnedVersionIsPreservedVerbatim(t *testing.T) {
	const doc = `
name: pinned
version: "  2.0.0-RC1  "
tasks:
  - id: a
    uses: shell
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

func TestCompile_CompilesRetryTimeoutAndPool(t *testing.T) {
	const doc = `
name: options
tasks:
  - id: a
    uses: shell
    pool: gpu
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
	if task.GetPool() != "gpu" {
		t.Errorf("pool = %q, want gpu", task.GetPool())
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

// TestCompile_CarriesTheNamespaceAndLeavesAbsenceEmpty pins the two halves of
// rule 1's namespace: an author who names one gets it verbatim, and an author
// who names none gets an empty field — "empty means default" is resolved once,
// where the name becomes a storage key, and never guessed twice.
func TestCompile_CarriesTheNamespaceAndLeavesAbsenceEmpty(t *testing.T) {
	const named = `
namespace: payments
name: nightly-close
tasks:
  - id: settle
    uses: shell
    with: {command: "echo 1"}
`
	wf, err := dsl.Compile([]byte(named))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if wf.GetNamespace() != "payments" {
		t.Errorf("namespace = %q, want payments", wf.GetNamespace())
	}

	const anonymous = `
name: nightly-close
tasks:
  - id: settle
    uses: shell
    with: {command: "echo 1"}
`
	wf, err = dsl.Compile([]byte(anonymous))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if wf.GetNamespace() != "" {
		t.Errorf("namespace = %q, want empty (meaning the default namespace)", wf.GetNamespace())
	}
}

func TestCompile_RejectsANamespaceOrPoolThatIsNotASlug(t *testing.T) {
	cases := []struct{ name, doc, want string }{
		{
			name: "namespace",
			doc: `
namespace: Payments Team
name: nightly-close
tasks:
  - id: settle
    uses: shell
    with: {command: "echo 1"}
`,
			want: "namespace",
		},
		{
			name: "pool",
			doc: `
name: nightly-close
tasks:
  - id: settle
    uses: shell
    pool: GPU_Large
    with: {command: "echo 1"}
`,
			want: "pool",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(tc.doc))
			if err == nil {
				t.Fatalf("Compile accepted a %s that is not a slug", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name %q", err.Error(), tc.want)
			}
		})
	}
}

func TestCompileFile_ReportsMissingFile(t *testing.T) {
	_, err := dsl.CompileFile(filepath.Join(t.TempDir(), "nope.ork.yaml"))
	if err == nil {
		t.Fatal("CompileFile(missing) = nil error, want failure")
	}
}

func TestCompile_RejectsIndirectInvalidTemplateReferences(t *testing.T) {
	tests := []struct {
		name        string
		expression  string
		wantMessage string
	}{
		{name: "unknown root", expression: `{{ .notAContextValue }}`, wantMessage: "unknown template root"},
		{name: "bare inputs root", expression: `{{ .inputs }}`, wantMessage: "must name a declared input"},
		{name: "bare tasks root", expression: `{{ .tasks }}`, wantMessage: "must name an upstream task"},
		{name: "indexed missing input", expression: `{{ index .inputs "missing" }}`, wantMessage: "undeclared input"},
		{name: "indexed non-upstream task", expression: `{{ index .tasks "producer" }}`, wantMessage: "not in its needs closure"},
		{name: "chained indexed non-upstream task", expression: `{{ (index .tasks "producer").output }}`, wantMessage: "not in its needs closure"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			document := fmt.Sprintf(`
name: indirect-reference
inputs:
  url:
    type: string
tasks:
  - id: producer
    uses: shell
    with: {command: "echo producer"}
  - id: consumer
    uses: shell
    with:
      command: %q
`, tc.expression)
			_, err := dsl.Compile([]byte(document))
			if err == nil {
				t.Fatalf("Compile() accepted invalid template %q", tc.expression)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("Compile() error = %q, want it to contain %q", err, tc.wantMessage)
			}
		})
	}
}

func TestCompile_AcceptsValidIndexedAndRelativeTemplateReferences(t *testing.T) {
	const document = `
name: indexed-references
inputs:
  url:
    type: string
  items:
    type: array
tasks:
  - id: producer
    uses: shell
    with: {command: "echo producer"}
  - id: consumer
    uses: shell
    needs: [producer]
    with:
      direct_input: '{{ index .inputs "url" }}'
      dynamic_input: '{{ $key := "url" }}{{ index .inputs $key }}'
      task_output: '{{ (index .tasks "producer").output.stdout }}'
      relative: '{{ range .inputs.items }}{{ .value }}{{ end }}'
`
	if _, err := dsl.Compile([]byte(document)); err != nil {
		t.Fatalf("Compile() rejected valid indexed/relative references: %v", err)
	}
}

func TestCompile_RejectsRootVariablesNamedTreesAndUnreboundElseReferences(t *testing.T) {
	tests := []struct {
		name        string
		expression  string
		wantMessage string
	}{
		{name: "root variable undeclared input", expression: `{{ $.inputs.missing }}`, wantMessage: "undeclared input"},
		{name: "root variable unknown task", expression: `{{ $.tasks.ghost.output.x }}`, wantMessage: "unknown task"},
		{name: "root variable non-upstream task", expression: `{{ $.tasks.producer.output.x }}`, wantMessage: "not in its needs closure"},
		{name: "named define", expression: `{{ define "bad" }}{{ $.inputs.missing }}{{ end }}{{ template "bad" . }}`, wantMessage: "undeclared input"},
		{name: "with else", expression: `{{ with .inputs.url }}ok{{ else }}{{ .inputs.missing }}{{ end }}`, wantMessage: "undeclared input"},
		{name: "range else", expression: `{{ range .inputs.items }}{{ .value }}{{ else }}{{ .inputs.missing }}{{ end }}`, wantMessage: "undeclared input"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			document := fmt.Sprintf(`
name: complete-template-validation
inputs:
  url:
    type: string
  items:
    type: array
tasks:
  - id: producer
    uses: shell
    with: {command: "echo producer"}
  - id: consumer
    uses: shell
    with:
      command: %q
`, tc.expression)
			_, err := dsl.Compile([]byte(document))
			if err == nil {
				t.Fatalf("Compile() accepted invalid template %q", tc.expression)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("Compile() error = %q, want it to contain %q", err, tc.wantMessage)
			}
		})
	}
}

func TestCompile_AcceptsValidRootVariableReferences(t *testing.T) {
	const document = `
name: root-variable-references
inputs:
  url:
    type: string
tasks:
  - id: producer
    uses: shell
    with: {command: "echo producer"}
  - id: consumer
    uses: shell
    needs: [producer]
    with:
      input: '{{ $.inputs.url }}'
      indexed_input: '{{ index $.inputs "url" }}'
      indexed_task: '{{ (index $.tasks "producer").output.stdout }}'
      task: '{{ $.tasks.producer.output.stdout }}'
      rebound_root: '{{ with .inputs.url }}{{ $.inputs.url }}{{ end }}'
      local: '{{ $name := .inputs.url }}{{ $name }}'
      named: '{{ define "ok" }}{{ $.inputs.url }}{{ end }}{{ template "ok" . }}'
`
	if _, err := dsl.Compile([]byte(document)); err != nil {
		t.Fatalf("Compile() rejected valid root variable references: %v", err)
	}
}

func TestCompile_AcceptsNamedTemplateWithReboundRelativeContext(t *testing.T) {
	const document = `
name: named-rebound
inputs:
  obj:
    type: object
tasks:
  - id: render
    uses: shell
    with:
      command: '{{ define "value" }}{{ .value }}{{ end }}{{ with .inputs.obj }}{{ template "value" . }}{{ end }}'
`
	if _, err := dsl.Compile([]byte(document)); err != nil {
		t.Fatalf("Compile() rejected named template relative to its rebound argument: %v", err)
	}
}

func TestCompile_RejectsRootPreservingWithAndIndexBypasses(t *testing.T) {
	tests := []struct {
		name        string
		expression  string
		wantMessage string
	}{
		{name: "with dot", expression: `{{ with . }}{{ .inputs.missing }}{{ end }}`, wantMessage: "undeclared input"},
		{name: "with dollar", expression: `{{ with $ }}{{ .inputs.missing }}{{ end }}`, wantMessage: "undeclared input"},
		{name: "index root dot", expression: `{{ index . "inputs" "missing" }}`, wantMessage: "undeclared input"},
		{name: "index root dollar", expression: `{{ index $ "tasks" "ghost" "output" }}`, wantMessage: "unknown task"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			document := fmt.Sprintf(`
name: root-bypass
inputs:
  present:
    type: string
tasks:
  - id: only
    uses: shell
    with:
      command: %q
`, tc.expression)
			_, err := dsl.Compile([]byte(document))
			if err == nil {
				t.Fatalf("Compile() accepted invalid root reference %q", tc.expression)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("Compile() error = %q, want it to contain %q", err, tc.wantMessage)
			}
		})
	}
}

func TestCompile_AcceptsValidRootPreservingWithAndIndexPaths(t *testing.T) {
	const document = `
name: root-paths
inputs:
  present:
    type: string
tasks:
  - id: producer
    uses: shell
  - id: consumer
    uses: shell
    needs: [producer]
    with:
      with_dot: '{{ with . }}{{ .inputs.present }}{{ end }}'
      with_dollar: '{{ with $ }}{{ .inputs.present }}{{ end }}'
      input_dot: '{{ index . "inputs" "present" }}'
      input_dollar: '{{ index $ "inputs" "present" }}'
      task_dot: '{{ index . "tasks" "producer" "output" }}'
      task_dollar: '{{ index $ "tasks" "producer" "output" }}'
`
	if _, err := dsl.Compile([]byte(document)); err != nil {
		t.Fatalf("Compile() rejected valid root-preserving paths: %v", err)
	}
}

// TestCompile_CollidingNormalizedKeysCompileTheSameEveryTime guards the other
// half of "one version, one document".
//
// YAML lets two distinct keys stringify to one JSON key (`null` and
// `"<nil>"`), so the conversion loses one of them — and which one it loses
// must not depend on map iteration order. The version cannot notice any more:
// it is the source's hash, and the source does not change. The STORE notices,
// because it compares the whole compiled document, so a compilation that
// varied would make re-applying an unchanged file a "different document under
// a stored version" every few tries (pkg/artifacts.sameStoredOrkflow). The
// assertion is therefore on the compiled message, not only on the version it
// used to be derived through.
func TestCompile_CollidingNormalizedKeysCompileTheSameEveryTime(t *testing.T) {
	const document = `
name: colliding-keys
tasks:
  - id: only
    uses: shell
    with:
      nested:
        null: one
        "<nil>": two
`
	var first *orkstackv1.Orkflow
	for i := 0; i < 5_000; i++ {
		wf, err := dsl.Compile([]byte(document))
		if err != nil {
			t.Fatalf("Compile() iteration %d: %v", i, err)
		}
		if i == 0 {
			first = wf
			continue
		}
		if wf.GetVersion() != first.GetVersion() {
			t.Fatalf("version changed at iteration %d: %q vs %q",
				i, wf.GetVersion(), first.GetVersion())
		}
		if !proto.Equal(wf, first) {
			t.Fatalf("the same bytes compiled differently at iteration %d:\n%v\nvs\n%v",
				i, wf, first)
		}
	}
}

func TestCompile_PreservesExplicitEmptyStructPresence(t *testing.T) {
	const explicit = `
name: explicit-empty
inputs: {}
tasks:
  - id: only
    uses: shell
    with: {}
`
	wf, err := dsl.Compile([]byte(explicit))
	if err != nil {
		t.Fatalf("Compile(explicit empty): %v", err)
	}
	if wf.Inputs == nil || len(wf.Inputs.GetFields()) != 0 {
		t.Errorf("inputs = %v, want a present empty Struct", wf.Inputs)
	}
	if wf.GetTasks()[0].With == nil || len(wf.GetTasks()[0].With.GetFields()) != 0 {
		t.Errorf("with = %v, want a present empty Struct", wf.GetTasks()[0].With)
	}

	const omitted = `
name: omitted-empty
tasks:
  - id: only
    uses: shell
`
	wf, err = dsl.Compile([]byte(omitted))
	if err != nil {
		t.Fatalf("Compile(omitted): %v", err)
	}
	if wf.Inputs != nil || wf.GetTasks()[0].With != nil {
		t.Errorf("omitted fields became present: inputs=%v with=%v", wf.Inputs, wf.GetTasks()[0].With)
	}
}
