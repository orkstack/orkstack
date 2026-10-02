package dsl

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// handBuiltAwait is an awaited task that never met the YAML decoder, as a
// request arriving at internal/api would carry it.
func handBuiltAwait() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name: "hand-built",
		Pins: []*orkstackv1.Pin{{Name: "build"}},
		Tasks: []*orkstackv1.Task{
			{Id: "first", Uses: "shell", Pin: "build"},
			{Id: "gate", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}},
		},
	}
}

func TestValidate_HandBuiltAwaitWithExecutionFieldsIsRefused(t *testing.T) {
	with, _ := structpb.NewStruct(map[string]any{"op": "exec"})
	cases := []struct {
		field string
		set   func(*orkstackv1.Task)
		want  string
	}{
		{"uses", func(t *orkstackv1.Task) { t.Uses = "shell" }, "uses: and await: are exclusive"},
		{"with", func(t *orkstackv1.Task) { t.With = with }, `"with" does not apply`},
		{"command", func(t *orkstackv1.Task) { t.Command = []string{"echo"} }, `"command" does not apply`},
		{"env", func(t *orkstackv1.Task) { t.Env = map[string]string{"A": "b"} }, `"env" does not apply`},
		{"secrets", func(t *orkstackv1.Task) {
			t.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "env:DB"}}
		}, `"secrets" does not apply`},
		{"resources", func(t *orkstackv1.Task) { t.Resources = &orkstackv1.ResourceRequest{Cpu: "1"} }, `"resources" does not apply`},
		{"network", func(t *orkstackv1.Task) { t.Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE }, `"network" does not apply`},
		{"pool", func(t *orkstackv1.Task) { t.Pool = "gpu" }, `"pool" does not apply`},
		{"pin", func(t *orkstackv1.Task) { t.Pin = "build" }, `"pin" does not apply`},
		{"retry", func(t *orkstackv1.Task) { t.Retry = &orkstackv1.RetryPolicy{MaximumAttempts: 2} }, `"retry" does not apply`},
		{"timeout_seconds", func(t *orkstackv1.Task) { t.TimeoutSeconds = 300 }, `"timeout_seconds" does not apply (it runs no container) — the deadline is await.timeout_seconds`},
		{"cancellation_grace_period_seconds", func(t *orkstackv1.Task) { t.CancellationGracePeriodSeconds = 5 }, `"cancellation_grace_period_seconds" does not apply`},
		{"run_as_orker_user", func(t *orkstackv1.Task) { t.RunAsOrkerUser = true }, `"run_as_orker_user" does not apply`},
		{"image", func(t *orkstackv1.Task) { t.Image = &orkstackv1.ImageRef{Reference: "shell"} }, `"image" does not apply`},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			wf := handBuiltAwait()
			tc.set(wf.Tasks[1])
			err := validate(wf, Options{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			var verr *ValidationError
			if !errors.As(err, &verr) || verr.Field != "tasks[1]."+tc.field {
				t.Fatalf("error not anchored at tasks[1].%s: %#v", tc.field, err)
			}
		})
	}
	t.Run("a clean hand-built await validates", func(t *testing.T) {
		if err := validate(handBuiltAwait(), Options{}); err != nil {
			t.Fatalf("refused: %v", err)
		}
	})
}

func TestCanonicalOrkflow_AwaitEmittedOnlyWhenSet(t *testing.T) {
	// Every existing golden is unchanged: the vectors in canonical_test.go
	// still pass, and a definition with no await carries no `await` key.
	plain, err := CanonicalOrkflow(goldenOrkflow(t))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), `"await"`) {
		t.Fatalf("a definition with no await emitted the key: %s", plain)
	}
	if got, _ := DefinitionDigest(goldenOrkflow(t)); got != GoldenDefinitionDigest {
		t.Fatalf("the golden digest moved: %s", got)
	}

	withAwait := func(schema map[string]any) *orkstackv1.Orkflow {
		wf := goldenOrkflow(t)
		s, err := structpb.NewStruct(schema)
		if err != nil {
			t.Fatal(err)
		}
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{
			Id:    "gate",
			Needs: []string{"one"},
			Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 3600, Callback: true, Schema: s},
		})
		return wf
	}
	a := withAwait(map[string]any{
		"b": map[string]any{"type": "string", "required": true},
		"a": map[string]any{"required": false, "type": "number"},
	})
	canonical, err := CanonicalOrkflow(a)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Tasks []map[string]json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.Tasks[0]["await"]; ok {
		t.Fatalf("the container task carries an await key: %s", canonical)
	}
	if got := string(decoded.Tasks[1]["await"]); got != `{"callback":true,"schema":{"a":{"required":false,"type":"number"},"b":{"required":true,"type":"string"}},"timeoutSeconds":3600}` {
		t.Fatalf("await canonicalized as %s", got)
	}

	digestA, _ := DefinitionDigest(a)
	withoutAwait := proto.Clone(a).(*orkstackv1.Orkflow)
	withoutAwait.Tasks = withoutAwait.Tasks[:1]
	digestPlain, _ := DefinitionDigest(withoutAwait)
	if digestA == digestPlain {
		t.Fatal("adding an await did not change the digest")
	}
	// Schema key order does not: a Struct is a map, and the canonical form
	// sorts it.
	b := withAwait(map[string]any{
		"a": map[string]any{"type": "number", "required": false},
		"b": map[string]any{"required": true, "type": "string"},
	})
	digestB, _ := DefinitionDigest(b)
	if digestA != digestB {
		t.Fatalf("schema key order moved the digest: %s vs %s", digestA, digestB)
	}
	// And the deadline is part of what runs.
	c := proto.Clone(a).(*orkstackv1.Orkflow)
	c.Tasks[1].Await.TimeoutSeconds = 60
	if digestC, _ := DefinitionDigest(c); digestC == digestA {
		t.Fatal("changing the deadline did not change the digest")
	}
}
