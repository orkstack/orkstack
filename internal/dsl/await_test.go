package dsl_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

func TestCompile_AwaitTaskCompiles(t *testing.T) {
	wf, err := dsl.CompileFile("../../examples/awaited-approval.ork.yaml")
	if err != nil {
		t.Fatalf("compile the example: %v", err)
	}
	var approval *orkstackv1.Task
	for _, task := range wf.GetTasks() {
		if task.GetId() == "approval" {
			approval = task
		}
	}
	if approval == nil {
		t.Fatal("the example has no `approval` task")
	}
	spec := approval.GetAwait()
	if spec == nil {
		t.Fatal("approval compiled with no AwaitSpec")
	}
	if spec.GetTimeoutSeconds() != 86400 || !spec.GetCallback() {
		t.Fatalf("AwaitSpec = %v, want timeout 86400 and callback", spec)
	}
	fields := spec.GetSchema().GetFields()
	if len(fields) != 2 || fields["approved"].GetStructValue().GetFields()["type"].GetStringValue() != "boolean" {
		t.Fatalf("schema = %v, want approved:boolean and approver:string", spec.GetSchema())
	}
	if approval.GetTimeoutSeconds() != 0 {
		t.Fatalf("an await's Task.timeout_seconds = %d, want 0", approval.GetTimeoutSeconds())
	}
	if approval.GetImage() != nil || approval.GetUses() != "" || approval.GetPool() != "" {
		t.Fatalf("an await carries an image, uses or pool: %v", approval)
	}
	if got := approval.GetNeeds(); len(got) != 1 || got[0] != "plan" {
		t.Fatalf("needs = %v, want [plan]", got)
	}
}

func TestCompile_AwaitWithNoSchemaCompilesToNoSchemaField(t *testing.T) {
	wf, err := dsl.Compile([]byte(`
name: bare-await
tasks:
  - id: gate
    await: {timeout_seconds: 60}
`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if wf.GetTasks()[0].GetAwait().GetSchema() != nil {
		t.Fatalf("schema = %v, want unset", wf.GetTasks()[0].GetAwait().GetSchema())
	}
}

func TestCompile_AwaitRefusesEveryExecutionField(t *testing.T) {
	cases := []struct {
		key  string
		yaml string
	}{
		{"with", "with: {op: exec}"},
		{"command", "command: [echo]"},
		{"env", "env: {A: b}"},
		{"secrets", "secrets: [{name: db, source: 'env:DB'}]"},
		{"resources", "resources: {cpu: '1'}"},
		{"network", "network: none"},
		{"pool", "pool: gpu"},
		{"pin", "pin: build"},
		{"retry", "retry: {maximum_attempts: 2}"},
		{"timeout_seconds", "timeout_seconds: 60"},
		{"cancellation_grace_period_seconds", "cancellation_grace_period_seconds: 5"},
		{"run_as_orker_user", "run_as_orker_user: true"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			source := "name: refused\npins: {build: {}}\ntasks:\n  - id: gate\n    await: {timeout_seconds: 60}\n    " + tc.yaml + "\n"
			_, err := dsl.Compile([]byte(source))
			if err == nil {
				t.Fatalf("compiled an await with %s", tc.key)
			}
			want := `task "gate": an awaited task takes only id, needs, condition and await; "` + tc.key + `" does not apply (it runs no container)`
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %q, want it to contain %q", err, want)
			}
			if tc.key == "timeout_seconds" && !strings.Contains(err.Error(), "— the deadline is await.timeout_seconds") {
				t.Fatalf("timeout_seconds refusal %q does not point at await.timeout_seconds", err)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != "tasks[0]."+tc.key {
				t.Fatalf("error not anchored at tasks[0].%s: %#v", tc.key, err)
			}
		})
	}
	t.Run("run_as_orker_user false is absent", func(t *testing.T) {
		_, err := dsl.Compile([]byte("name: ok\ntasks:\n  - id: gate\n    await: {timeout_seconds: 60}\n    run_as_orker_user: false\n"))
		if err != nil {
			t.Fatalf("run_as_orker_user: false refused: %v", err)
		}
	})
}

func TestCompile_AwaitNeedsExactlyOneOfUsesAndAwait(t *testing.T) {
	_, err := dsl.Compile([]byte("name: both\ntasks:\n  - id: gate\n    uses: shell\n    await: {timeout_seconds: 60}\n"))
	if err == nil || !strings.Contains(err.Error(), `task "gate": uses: and await: are exclusive — an awaited task runs no image`) {
		t.Fatalf("uses + await: error = %v, want the rule-6 refusal", err)
	}
	var verr *dsl.ValidationError
	if !errors.As(err, &verr) || verr.Field != "tasks[0].uses" {
		t.Fatalf("error not anchored at tasks[0].uses: %#v", err)
	}
	_, err = dsl.Compile([]byte("name: neither\ntasks:\n  - id: gate\n    needs: []\n"))
	if err == nil || !strings.Contains(err.Error(), "`uses:` is required") {
		t.Fatalf("neither: error = %v, want the uses-required refusal", err)
	}
}

func TestCompile_AwaitTimeoutBounds(t *testing.T) {
	cases := []struct {
		name    string
		await   string
		wantErr string
	}{
		{"absent", "{callback: true}", "await.timeout_seconds is required — an await with no deadline could wait forever"},
		{"zero", "{timeout_seconds: 0}", "await.timeout_seconds 0 is out of range (1 s … 30 days)"},
		{"negative", "{timeout_seconds: -1}", "await.timeout_seconds -1 is out of range (1 s … 30 days)"},
		{"one second", "{timeout_seconds: 1}", ""},
		{"thirty days", "{timeout_seconds: 2592000}", ""},
		{"over thirty days", "{timeout_seconds: 2592001}", "await.timeout_seconds 2592001 is out of range (1 s … 30 days)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte("name: bounds\ntasks:\n  - id: gate\n    await: " + tc.await + "\n"))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != "tasks[0].await.timeout_seconds" {
				t.Fatalf("error not anchored at tasks[0].await.timeout_seconds: %#v", err)
			}
		})
	}
}

func TestCompile_AwaitSchemaDeclarations(t *testing.T) {
	cases := []struct {
		name    string
		schema  string
		wantErr string
	}{
		{"unknown type", "{x: {type: integer}}", "`type` is required and must be one of string, number, boolean, object, array"},
		{"missing type", "{x: {required: true}}", "`type` is required"},
		{"default", "{x: {type: string, default: a}}", "an await's schema declares no default — a resolution's values.json is stored as the caller's exact bytes"},
		{"extra key", "{x: {type: string, description: y}}", `unknown key "description"`},
		{"non-mapping entry", "{x: string}", "must be a mapping"},
		{"non-boolean required", "{x: {type: string, required: yes-please}}", "`required` must be true or false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte("name: schema\ntasks:\n  - id: gate\n    await:\n      timeout_seconds: 60\n      schema: " + tc.schema + "\n"))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != "tasks[0].await.schema.x" {
				t.Fatalf("error not anchored at tasks[0].await.schema.x: %#v", err)
			}
		})
	}
	t.Run("the five types", func(t *testing.T) {
		_, err := dsl.Compile([]byte(`
name: schema
tasks:
  - id: gate
    await:
      timeout_seconds: 60
      schema:
        a: {type: string, required: true}
        b: {type: number}
        c: {type: boolean, required: false}
        d: {type: object}
        e: {type: array}
        "not a slug": {type: string}
`))
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
	})
}

func TestCompile_ArtifactHelperNamingAnAwaitIsRefused(t *testing.T) {
	_, err := dsl.Compile([]byte(`
name: artifact-await
tasks:
  - id: gate
    await: {timeout_seconds: 60}
  - id: use
    uses: shell
    needs: [gate]
    with:
      op: exec
      command: ["cat", '{{ artifact "gate/model.bin" }}']
`))
	want := `task "use": with.command[1]: "gate" is an awaited task and produces no artifacts; read its values with .tasks.gate.output`
	if err == nil || !strings.Contains(err.Error(), `"gate" is an awaited task and produces no artifacts; read its values with .tasks.gate.output`) {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestCompile_TemplatesMayReadAnAwaitsOutput(t *testing.T) {
	_, err := dsl.Compile([]byte(`
name: read-await
tasks:
  - id: gate
    await: {timeout_seconds: 60}
  - id: use
    uses: shell
    needs: [gate]
    condition: "{{ .tasks.gate.output.approved }}"
    with:
      op: exec
      command: ["echo", "{{ .tasks.gate.output.model_uri }}"]
`))
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
}

func TestValidateAwaitValues(t *testing.T) {
	schema, err := structpb.NewStruct(map[string]any{
		"model_uri": map[string]any{"type": "string", "required": true},
		"auc":       map[string]any{"type": "number"},
		"ok":        map[string]any{"type": "boolean"},
		"meta":      map[string]any{"type": "object"},
		"tags":      map[string]any{"type": "array"},
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := &orkstackv1.AwaitSpec{TimeoutSeconds: 60, Schema: schema}
	cases := []struct {
		name      string
		spec      *orkstackv1.AwaitSpec
		raw       string
		max       int64
		wantField string
		wantMsg   string
	}{
		{"array", nil, `[]`, 0, "", "the values must be a JSON object, not an array"},
		{"string", nil, `"x"`, 0, "", "the values must be a JSON object, not a string"},
		{"null", nil, `null`, 0, "", "the values must be a JSON object, not null"},
		{"invalid utf-8", nil, "{\"a\":\"\xff\"}", 0, "", "the values are not valid UTF-8"},
		{"invalid json", nil, `{"a":`, 0, "", "the values are not one valid JSON value"},
		{"two values", nil, `{}{}`, 0, "", "the values are not one valid JSON value"},
		{"over the bound", nil, `{"a":"bcdef"}`, 5, "", "the values are 13 bytes, over the 5-byte limit"},
		{"missing required", spec, `{}`, 0, "values.model_uri", "is required by the await's schema"},
		{"null for a typed key", spec, `{"model_uri":null}`, 0, "values.model_uri", "must be a string, not null"},
		{"string mismatch", spec, `{"model_uri":1}`, 0, "values.model_uri", "must be a string, not a number"},
		{"number mismatch", spec, `{"model_uri":"s","auc":"0.9"}`, 0, "values.auc", "must be a number, not a string"},
		{"boolean mismatch", spec, `{"model_uri":"s","ok":"true"}`, 0, "values.ok", "must be a boolean, not a string"},
		{"object mismatch", spec, `{"model_uri":"s","meta":[]}`, 0, "values.meta", "must be an object, not an array"},
		{"array mismatch", spec, `{"model_uri":"s","tags":{}}`, 0, "values.tags", "must be an array, not an object"},
		{"undeclared keys accepted", spec, `{"model_uri":"s","extra":[1,2]}`, 0, "", ""},
		{"no schema accepts any object", nil, `{"anything":true}`, 0, "", ""},
		{"empty object", &orkstackv1.AwaitSpec{TimeoutSeconds: 1}, `{}`, 0, "", ""},
		{"20-digit number type-checks", spec, `{"model_uri":"s","auc":12345678901234567890}`, 0, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(tc.raw)
			before := bytes.Clone(raw)
			err := dsl.ValidateAwaitValues(tc.spec, raw, tc.max)
			if !bytes.Equal(raw, before) {
				t.Fatalf("the input slice was modified: %q -> %q", before, raw)
			}
			if tc.wantMsg == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			var verr *dsl.AwaitValuesError
			if !errors.As(err, &verr) {
				t.Fatalf("error = %#v, want *AwaitValuesError", err)
			}
			if verr.Field != tc.wantField || verr.Message != tc.wantMsg {
				t.Fatalf("got {%q %q}, want {%q %q}", verr.Field, verr.Message, tc.wantField, tc.wantMsg)
			}
		})
	}
}
