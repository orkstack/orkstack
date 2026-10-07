package dsl_test

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// joinDoc is a two-branch document whose last task carries the given join
// lines (indented as task keys).
func joinDoc(join string) []byte {
	return []byte(`
name: joined
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    with: {op: exec}
  - id: c
    uses: shell
    needs: [a, b]
    with: {op: exec}
` + join)
}

func TestCompile_JoinAllSucceededCompilesToNoJoin(t *testing.T) {
	absent, err := dsl.Compile(joinDoc(""))
	if err != nil {
		t.Fatalf("compile without join: %v", err)
	}
	written, err := dsl.Compile(joinDoc("    join: all_succeeded\n"))
	if err != nil {
		t.Fatalf("compile with join: all_succeeded: %v", err)
	}
	if got := written.GetTasks()[2].GetJoin(); got != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
		t.Fatalf("join: all_succeeded compiled to %v, want UNSPECIFIED", got)
	}
	// The sources differ by a line, so their derived versions and source
	// differ; the definition is what must agree.
	written.Version, written.SourceYaml = absent.GetVersion(), absent.GetSourceYaml()
	if !proto.Equal(absent, written) {
		t.Fatal("join: all_succeeded and no join compile to two messages")
	}
	a, _ := dsl.DefinitionDigest(absent)
	b, _ := dsl.DefinitionDigest(written)
	if a != b {
		t.Fatalf("join: all_succeeded and no join digest differently: %s vs %s", a, b)
	}
}

func TestCompile_JoinWordsMapToThePolicy(t *testing.T) {
	cases := []struct {
		word string
		want orkstackv1.JoinPolicy
	}{
		{"all_succeeded", orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED},
		{"none_failed", orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED},
		{"any_failed", orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED},
		{"all_done", orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE},
	}
	for _, tc := range cases {
		t.Run(tc.word, func(t *testing.T) {
			wf, err := dsl.Compile(joinDoc("    join: " + tc.word + "\n"))
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			if got := wf.GetTasks()[2].GetJoin(); got != tc.want {
				t.Fatalf("join = %v, want %v", got, tc.want)
			}
			if got := dsl.JoinWord(tc.want); got != tc.word {
				t.Fatalf("JoinWord(%v) = %q, want %q", tc.want, got, tc.word)
			}
		})
	}
}

func TestCompile_RefusesAnUnknownJoinWord(t *testing.T) {
	_, err := dsl.Compile(joinDoc("    join: on_failure\n"))
	want := `task "c": join "on_failure" is not a join policy; it is one of all_succeeded, none_failed, any_failed or all_done`
	if err == nil || err.Error() != want || fieldOf(err) != "tasks[2].join" {
		t.Fatalf("join: on_failure = %v (field %q)", err, fieldOf(err))
	}
}

func TestValidate_Rule38_AJoinNeedsANeed(t *testing.T) {
	for _, word := range []string{"none_failed", "any_failed", "all_done"} {
		t.Run(word, func(t *testing.T) {
			_, err := dsl.Compile([]byte(`
name: lonely
tasks:
  - id: a
    uses: shell
    needs: []
    join: ` + word + `
    with: {op: exec}
`))
			want := fmt.Sprintf(`task "a": join %s decides on how its needs ended, and it names no need`, word)
			if err == nil || !strings.HasPrefix(err.Error(), want) || fieldOf(err) != "tasks[0].join" {
				t.Fatalf("join %s with no needs = %v (field %q)", word, err, fieldOf(err))
			}
		})
	}
}

func TestValidate_Rule39_FailureHandlerIsNotPinnedFannedOutOrCached(t *testing.T) {
	pinned := func(join string) string {
		return `
name: handler
pins: {build: {}}
inputs:
  parts: {type: array}
tasks:
  - id: a
    uses: shell
    pin: build
    with: {op: exec}
  - id: h
    uses: shell
    needs: [a]
    join: ` + join + `
    pin: build
    with: {op: exec}
`
	}
	fannedOut := func(join string) string {
		return `
name: handler
inputs:
  parts: {type: array}
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: h
    uses: shell
    needs: [a]
    join: ` + join + `
    each: {over: "{{ .inputs.parts }}", max_items: 3}
    with: {op: exec}
`
	}
	cached := func(join string) string {
		return `
name: handler
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: h
    uses: shell
    needs: [a]
    join: ` + join + `
    cache: {enabled: true}
    with: {op: exec}
`
	}
	cases := []struct {
		name  string
		doc   func(string) string
		field string
		want  string
	}{
		{"pin", pinned, "tasks[1].pin", "is a failure handler and cannot join a pin"},
		{"each", fannedOut, "tasks[1].each", "is a failure handler and cannot be a fan-out"},
		{"cache", cached, "tasks[1].cache", "is a failure handler and cannot be cached"},
	}
	for _, tc := range cases {
		for _, join := range []string{"any_failed", "all_done"} {
			t.Run(tc.name+"/"+join, func(t *testing.T) {
				_, err := dsl.Compile([]byte(tc.doc(join)))
				prefix := fmt.Sprintf(`task "h" (join %s) `, join)
				if err == nil || !strings.HasPrefix(err.Error(), prefix) || !strings.Contains(err.Error(), tc.want) ||
					fieldOf(err) != tc.field {
					t.Fatalf("%s handler = %v (field %q)", tc.name, err, fieldOf(err))
				}
			})
		}
		t.Run(tc.name+"/none_failed", func(t *testing.T) {
			if _, err := dsl.Compile([]byte(tc.doc("none_failed"))); err != nil {
				t.Fatalf("a none_failed task with %s = %v, want accepted", tc.name, err)
			}
		})
	}
}

func TestValidate_Rule40_NoOrdinaryTaskNeedsAnAnyFailedTask(t *testing.T) {
	doc := func(join string) string {
		return `
name: never
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
  - id: after
    uses: shell
    needs: [deploy, rollback]
` + join + `    with: {op: exec}
`
	}
	for _, join := range []string{"", "    join: all_succeeded\n", "    join: none_failed\n"} {
		_, err := dsl.Compile([]byte(doc(join)))
		if err == nil || fieldOf(err) != "tasks[2].needs[1]" ||
			!strings.Contains(err.Error(), `needs "rollback", whose join is any_failed`) ||
			!strings.Contains(err.Error(), "use join: all_done (run after it, whatever it did) or join: any_failed (run if it fails)") {
			t.Fatalf("join %q needing an any_failed task = %v (field %q)", join, err, fieldOf(err))
		}
	}
	for _, join := range []string{"    join: all_done\n", "    join: any_failed\n"} {
		if _, err := dsl.Compile([]byte(doc(join))); err != nil {
			t.Fatalf("join %q needing an any_failed task = %v, want accepted", join, err)
		}
	}
}

func TestValidate_Rule41_OutputsMayNotReadAnAnyFailedTask(t *testing.T) {
	doc := func(join, output string) string {
		return `
name: outs
outputs:
  result: '` + output + `'
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: ` + join + `
    with: {op: exec}
`
	}
	for _, output := range []string{`{{ .tasks.rollback.output.x }}`, `{{ index .tasks "rollback" "output" "x" }}`} {
		_, err := dsl.Compile([]byte(doc("any_failed", output)))
		if err == nil || fieldOf(err) != "outputs.result" ||
			!strings.HasPrefix(err.Error(), `outputs.result reads task "rollback", whose join is any_failed`) {
			t.Fatalf("outputs reading an any_failed task (%s) = %v (field %q)", output, err, fieldOf(err))
		}
	}
	if _, err := dsl.Compile([]byte(doc("all_done", `{{ .tasks.rollback.output.x }}`))); err != nil {
		t.Fatalf("outputs reading an all_done task = %v, want accepted", err)
	}
}

func TestValidate_Rule42_AContainerFailureHandlerNamesAtMost32Needs(t *testing.T) {
	doc := func(handler string, n int) string {
		var b strings.Builder
		b.WriteString("name: wide\ntasks:\n")
		needs := make([]string, n)
		for i := range n {
			needs[i] = fmt.Sprintf("t%d", i)
			fmt.Fprintf(&b, "  - id: t%d\n    uses: shell\n    with: {op: exec}\n", i)
		}
		fmt.Fprintf(&b, "  - id: notify\n    needs: [%s]\n%s", strings.Join(needs, ", "), handler)
		return b.String()
	}
	container := "    join: all_done\n    uses: shell\n    with: {op: exec}\n"
	if _, err := dsl.Compile([]byte(doc(container, 32))); err != nil {
		t.Fatalf("32 needs = %v, want accepted", err)
	}
	_, err := dsl.Compile([]byte(doc(container, 33)))
	want := `task "notify" (join all_done) names 33 needs; a failure handler names at most 32, because its ` +
		`input records how each one ended — split it into handlers of at most 32 needs each`
	if err == nil || err.Error() != want || fieldOf(err) != "tasks[33].needs" {
		t.Fatalf("33 needs = %v (field %q)", err, fieldOf(err))
	}
	for name, handler := range map[string]string{
		"awaited":     "    join: all_done\n    await: {timeout_seconds: 60}\n",
		"composed":    "    join: all_done\n    uses: orkflow:release-block@1.0.0\n",
		"none_failed": "    join: none_failed\n    uses: shell\n    with: {op: exec}\n",
	} {
		if _, err := dsl.Compile([]byte(doc(handler, 40))); err != nil {
			t.Fatalf("%s with 40 needs = %v, want accepted", name, err)
		}
	}
}

func TestValidate_AwaitAndComposedTasksMayCarryJoin(t *testing.T) {
	wf, err := dsl.Compile([]byte(`
name: kinds
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: approve
    needs: [deploy]
    join: any_failed
    await: {timeout_seconds: 60}
  - id: compensate
    needs: [deploy]
    join: all_done
    uses: orkflow:rollback-block@1.0.0
`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if got := wf.GetTasks()[1].GetJoin(); got != orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED {
		t.Fatalf("awaited join = %v", got)
	}
	if got := wf.GetTasks()[2].GetJoin(); got != orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE {
		t.Fatalf("composed join = %v", got)
	}
	_, err = dsl.Compile([]byte(`
name: kinds
tasks:
  - id: gate
    join: all_done
    needs: [a]
    pool: gpu
    await: {timeout_seconds: 60}
  - id: a
    uses: shell
    with: {op: exec}
`))
	want := `task "gate": an awaited task takes only id, needs, condition, join and await; "pool" does not apply (it runs no container)`
	if err == nil || err.Error() != want {
		t.Fatalf("awaited pool = %v", err)
	}
	_, err = dsl.Compile([]byte(`
name: kinds
tasks:
  - id: release
    uses: orkflow:release-block@1.0.0
    pool: gpu
`))
	wantPrefix := `task "release": a composed task takes only id, needs, condition, join, uses and with; "pool" does not apply (`
	if err == nil || !strings.HasPrefix(err.Error(), wantPrefix) {
		t.Fatalf("composed pool = %v", err)
	}
}

// sureOf renders SureAncestors(wf)[id] as a sorted list.
func sureOf(sure map[string]map[string]bool, id string) []string {
	out := []string{}
	for a := range sure[id] {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

func TestSureAncestors_Table(t *testing.T) {
	wf, err := dsl.Compile([]byte(`
name: sure
tasks:
  - id: decide
    uses: shell
    with: {op: exec}
  - id: canary
    uses: shell
    needs: [decide]
    with: {op: exec}
  - id: full
    uses: shell
    needs: [decide]
    with: {op: exec}
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    with: {op: exec}
  - id: deploy
    uses: shell
    needs: [verify]
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec}
  - id: escalate
    uses: shell
    needs: [rollback]
    join: any_failed
    with: {op: exec}
  - id: notify
    uses: shell
    needs: [deploy]
    join: all_done
    with: {op: exec}
`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	sure := dsl.SureAncestors(wf)
	want := map[string][]string{
		"decide":   {},
		"canary":   {"decide"},           // all_succeeded: the closure
		"verify":   {"decide"},           // none_failed: the intersection
		"deploy":   {"decide", "verify"}, // ordinary after the join: branches uncertain
		"rollback": {"decide", "verify"}, // any_failed: the failed need's own sure set
		"escalate": {"decide", "verify"}, // any_failed on a handler
		"notify":   {},                   // all_done: empty
		"full":     {"decide"},
	}
	for id, w := range want {
		if got := sureOf(sure, id); !reflect.DeepEqual(got, w) {
			t.Errorf("sure(%s) = %v, want %v", id, got, w)
		}
	}
}

func TestSureAncestors_IsTheClosureWithoutJoins(t *testing.T) {
	paths, err := filepath.Glob("../../examples/*.ork.yaml")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	for _, path := range paths {
		if filepath.Base(path) == "branch-and-recover.ork.yaml" {
			continue
		}
		wf, err := dsl.CompileFile(path)
		if err != nil {
			t.Fatalf("compile %s: %v", path, err)
		}
		byID := map[string]*orkstackv1.Task{}
		for _, task := range wf.GetTasks() {
			byID[task.GetId()] = task
		}
		var closure func(id string, into map[string]bool)
		closure = func(id string, into map[string]bool) {
			for _, n := range byID[id].GetNeeds() {
				into[n] = true
				closure(n, into)
			}
		}
		sure := dsl.SureAncestors(wf)
		for _, task := range wf.GetTasks() {
			want := map[string]bool{}
			closure(task.GetId(), want)
			if !reflect.DeepEqual(sure[task.GetId()], want) {
				t.Errorf("%s: sure(%s) = %v, want the closure %v", filepath.Base(path), task.GetId(), sure[task.GetId()], want)
			}
		}
		if issues := dsl.JoinWarnings(wf); len(issues) != 0 {
			t.Errorf("%s: JoinWarnings = %v, want none", filepath.Base(path), issues)
		}
	}
}

func TestTaskReads_KindsPathsAndGuards(t *testing.T) {
	cases := []struct {
		name string
		text string
		want []dsl.TaskRead
	}{
		{"field", `{{ .tasks.a.output.x }}`, []dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadField}}},
		{"dollar field", `{{ with .inputs.x }}{{ $.tasks.a.output.x }}{{ end }}`,
			[]dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadField}}},
		{"index with key", `{{ index .tasks "a" "output" "x" }}`, []dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadIndex}}},
		{"bare index is not a read", `{{ index .tasks "a" }}`, nil},
		{"artifact", `{{ artifact "a/log.txt" }}`,
			[]dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadArtifact, Name: "log.txt"}}},
		{"if guard", `{{ if index .tasks "a" }}{{ .tasks.a.output.x }}{{ end }}`,
			[]dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadField, Guarded: true}}},
		{"with guard", `{{ with index .tasks "a" }}{{ .output.x }}{{ $.tasks.a.output.y }}{{ artifact "a/f" }}{{ end }}`,
			[]dsl.TaskRead{
				{TaskID: "a", Kind: dsl.TaskReadField, Guarded: true},
				{TaskID: "a", Kind: dsl.TaskReadArtifact, Guarded: true, Name: "f"},
			}},
		{"dollar guard", `{{ if index $.tasks "a" }}{{ .tasks.a.output.x }}{{ end }}`,
			[]dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadField, Guarded: true}}},
		{"else is unguarded", `{{ if index .tasks "a" }}ok{{ else }}{{ .tasks.a.output.x }}{{ end }}`,
			[]dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadField}}},
		{"pipeline is unguarded", `{{ if .tasks.a.output.x }}ok{{ end }}`,
			[]dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadField}}},
		{"and is the documented false positive", `{{ if and (index .tasks "a") true }}{{ .tasks.a.output.x }}{{ end }}`,
			[]dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadField}}},
		{"guard on another id", `{{ if index .tasks "b" }}{{ .tasks.a.output.x }}{{ end }}`,
			[]dsl.TaskRead{{TaskID: "a", Kind: dsl.TaskReadField}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dsl.TaskReads(tc.text)
			if err != nil {
				t.Fatalf("TaskReads: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("TaskReads(%s) = %+v, want %+v", tc.text, got, tc.want)
			}
		})
	}
}

// warningsOf compiles doc and returns its W1 issues as "field: message".
func warningsOf(t *testing.T, doc string) []string {
	t.Helper()
	wf, err := dsl.Compile([]byte(doc))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	var out []string
	for _, issue := range dsl.JoinWarnings(wf) {
		if issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING {
			t.Fatalf("W1 issue %v is not a WARNING", issue)
		}
		out = append(out, issue.GetField()+": "+issue.GetMessage())
	}
	return out
}

const rejoinHead = `
name: w1
tasks:
  - id: decide
    uses: shell
    with: {op: exec}
  - id: canary
    uses: shell
    needs: [decide]
    with: {op: exec}
  - id: full
    uses: shell
    needs: [decide]
    with: {op: exec}
`

func TestJoinWarnings_UnguardedReadsOfUncertainAncestors(t *testing.T) {
	fieldSentence := func(task, ancestor, why string) string {
		return fmt.Sprintf(`task %q reads .tasks.%s, which is not certain to have succeeded when %q runs %s; `+
			`reading a task that did not succeed fails the task. Guard the read: {{ with index .tasks %q }}…{{ end }}`,
			task, ancestor, task, why, ancestor)
	}
	cases := []struct {
		name string
		doc  string
		want []string
	}{
		{"a none_failed task's field read of a direct need", rejoinHead + `
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    with: {op: exec, command: ["echo", "{{ .tasks.canary.output.x }}"]}
`, []string{"tasks[3].with.command[1]: " + fieldSentence("verify", "canary", "(a join lies between them)")}},
		{"the closure case: an ordinary task after the join", rejoinHead + `
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    with: {op: exec}
  - id: deploy
    uses: shell
    needs: [verify]
    with: {op: exec, command: ["echo", "{{ .tasks.full.output.x }}", "{{ .tasks.decide.output.x }}"]}
`, []string{"tasks[4].with.command[1]: " + fieldSentence("deploy", "full", "(a join lies between them)")}},
		{"an index read with a key", rejoinHead + `
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    with: {op: exec, command: ['{{ index .tasks "canary" "output" "x" }}']}
`, []string{"tasks[3].with.command[0]: " + fieldSentence("verify", "canary", "(a join lies between them)")}},
		{"a condition read, one warning per ancestor at the first read", rejoinHead + `
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    condition: '{{ .tasks.full.output.ok }}'
    with: {op: exec, command: ["{{ .tasks.full.output.x }}"]}
`, []string{"tasks[3].condition: " + fieldSentence("verify", "full", "(a join lies between them)")}},
		{"a failure handler's artifact read, guarded or not", `
name: w1
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec, command: ['{{ with index .tasks "deploy" }}{{ artifact "deploy/deploy.log" }}{{ end }}']}
`, []string{`tasks[1].with.command[0]: task "rollback" reads artifact "deploy/deploy.log", which is not certain to ` +
			`exist when "rollback" runs; a missing artifact fails the render. Read it from task.json's ` +
			`inputs.artifacts instead, where a missing file is a missing entry`}},
		{"a failure handler's field read", `
name: w1
tasks:
  - id: deploy
    uses: shell
    with: {op: exec}
  - id: notify
    uses: shell
    needs: [deploy]
    join: all_done
    with: {op: exec, command: ["{{ .tasks.deploy.output.x }}"]}
`, []string{"tasks[1].with.command[0]: " + fieldSentence("notify", "deploy", `("notify" is a failure handler)`)}},
		{"a non-handler's unguarded artifact read", rejoinHead + `
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    with: {op: exec, command: ['{{ artifact "canary/x" }}']}
`, []string{`tasks[3].with.command[0]: task "verify" reads artifact "canary/x", which is not certain to exist ` +
			`when "verify" runs; a missing artifact fails the render. Read it from task.json's inputs.artifacts ` +
			`instead, where a missing file is a missing entry, or guard it: {{ with index .tasks "canary" }}…{{ end }}`}},
		{"no warning: a non-handler's guarded artifact read, a guarded read, a bare index, a sure ancestor", rejoinHead + `
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    condition: '{{ if index .tasks "canary" }}{{ .tasks.canary.output.ok }}{{ else }}true{{ end }}'
    with:
      op: exec
      command:
        - '{{ with index .tasks "full" }}{{ artifact "full/x" }}{{ .output.y }}{{ end }}'
        - '{{ if index .tasks "full" }}yes{{ end }}'
        - '{{ .tasks.decide.output.x }}'
`, nil},
		{"no warning without a join", rejoinHead + `
  - id: verify
    uses: shell
    needs: [canary, full]
    with: {op: exec, command: ["{{ .tasks.canary.output.x }}"]}
`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := warningsOf(t, tc.doc)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("JoinWarnings =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

func TestJoinWarnings_TheShippedExampleHasNone(t *testing.T) {
	wf, err := dsl.CompileFile("../../examples/branch-and-recover.ork.yaml")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if issues := dsl.JoinWarnings(wf); len(issues) != 0 {
		t.Fatalf("branch-and-recover warnings = %v, want none", issues)
	}
	joins := map[string]orkstackv1.JoinPolicy{}
	for _, task := range wf.GetTasks() {
		if task.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
			joins[task.GetId()] = task.GetJoin()
		}
	}
	want := map[string]orkstackv1.JoinPolicy{
		"verify":   orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED,
		"rollback": orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED,
		"escalate": orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED,
		"notify":   orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE,
	}
	if !reflect.DeepEqual(joins, want) {
		t.Fatalf("branch-and-recover joins = %v, want %v", joins, want)
	}
}

// The DSL's helper set is exactly artifact, env and sh: joins add no template
// helper (docs/DSL.md "Templating").
func TestTemplateFuncs_AreExactlyArtifactEnvSh(t *testing.T) {
	var names []string
	for name := range dsl.TemplateFuncs() {
		names = append(names, name)
	}
	sort.Strings(names)
	if want := []string{"artifact", "env", "sh"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("TemplateFuncs = %v, want %v", names, want)
	}
}

func TestIsFailureHandler(t *testing.T) {
	for policy, want := range map[orkstackv1.JoinPolicy]bool{
		orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED: false,
		orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED: false,
		orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED:  true,
		orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE:    true,
	} {
		if got := dsl.IsFailureHandler(&orkstackv1.Task{Join: policy}); got != want {
			t.Errorf("IsFailureHandler(%v) = %v, want %v", policy, got, want)
		}
	}
}
