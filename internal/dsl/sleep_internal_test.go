package dsl

import (
	"encoding/json/v2"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// handBuiltSleep is a sleeping task that never met the YAML decoder, as a
// request arriving at internal/api would carry it.
func handBuiltSleep() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name: "hand-built",
		Pins: []*orkstackv1.Pin{{Name: "build"}},
		Tasks: []*orkstackv1.Task{
			{Id: "first", Uses: "shell", Pin: "build"},
			{Id: "soak", Needs: []string{"first"}, Sleep: &orkstackv1.SleepSpec{Seconds: 60}},
		},
	}
}

func TestValidate_HandBuiltSleepWithExecutionFieldsIsRefused(t *testing.T) {
	with, _ := structpb.NewStruct(map[string]any{"op": "exec"})
	cases := []struct {
		field string
		set   func(*orkstackv1.Task)
		want  string
	}{
		{"uses", func(t *orkstackv1.Task) { t.Uses = "shell" }, "uses: and sleep: are exclusive — a sleeping task runs no image"},
		{"await", func(t *orkstackv1.Task) { t.Await = &orkstackv1.AwaitSpec{TimeoutSeconds: 60} }, "await: and sleep: are exclusive"},
		{"with", func(t *orkstackv1.Task) { t.With = with }, `"with" does not apply (it runs no container)`},
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
		{"timeout_seconds", func(t *orkstackv1.Task) { t.TimeoutSeconds = 300 },
			`"timeout_seconds" does not apply (it runs no container) — how long it sleeps is sleep.seconds`},
		{"cancellation_grace_period_seconds", func(t *orkstackv1.Task) { t.CancellationGracePeriodSeconds = 5 }, `"cancellation_grace_period_seconds" does not apply`},
		{"run_as_orker_user", func(t *orkstackv1.Task) { t.RunAsOrkerUser = true }, `"run_as_orker_user" does not apply`},
		{"image", func(t *orkstackv1.Task) { t.Image = &orkstackv1.ImageRef{Reference: "shell"} }, `"image" does not apply`},
		{"each", func(t *orkstackv1.Task) { t.Each = &orkstackv1.EachSpec{Over: "{{ .inputs.l }}", MaxItems: 2} },
			`"each" does not apply (a sleep is one wait, not N)`},
		{"cache", func(t *orkstackv1.Task) { t.Cache = &orkstackv1.CacheSpec{} }, `"cache" does not apply (it computes nothing to reuse)`},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			wf := handBuiltSleep()
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
	t.Run("seconds out of range", func(t *testing.T) {
		for _, seconds := range []int32{0, -1, MaxSleepSeconds + 1} {
			wf := handBuiltSleep()
			wf.Tasks[1].Sleep.Seconds = seconds
			err := validate(wf, Options{})
			var verr *ValidationError
			if !errors.As(err, &verr) || verr.Field != "tasks[1].sleep.seconds" {
				t.Fatalf("seconds %d: error = %#v, want one anchored at tasks[1].sleep.seconds", seconds, err)
			}
		}
	})
	t.Run("a clean hand-built sleep validates", func(t *testing.T) {
		must.OKf(t, validate(handBuiltSleep(), Options{}), "refused")
	})
}

func TestCanonicalOrkflow_SleepEmittedOnlyWhenSet(t *testing.T) {
	plain, err := CanonicalOrkflow(goldenOrkflow(t))
	must.OK(t, err)
	if strings.Contains(string(plain), `"sleep"`) {
		t.Fatalf("a definition with no sleep emitted the key: %s", plain)
	}
	if got, _ := DefinitionDigest(goldenOrkflow(t)); got != GoldenDefinitionDigest {
		t.Fatalf("the golden digest moved: %s", got)
	}

	withSleep := func(seconds int32) *orkstackv1.Orkflow {
		wf := goldenOrkflow(t)
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{
			Id:    "soak",
			Needs: []string{"one"},
			Sleep: &orkstackv1.SleepSpec{Seconds: seconds},
		})
		return wf
	}
	a := withSleep(900)
	canonical, err := CanonicalOrkflow(a)
	must.OK(t, err)
	var decoded struct {
		Tasks []map[string]stablejson.RawJSON `json:"tasks"`
	}
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.Tasks[0]["sleep"]; ok {
		t.Fatalf("the container task carries a sleep key: %s", canonical)
	}
	if got := string(decoded.Tasks[1]["sleep"]); got != `{"seconds":900}` {
		t.Fatalf("sleep canonicalized as %s", got)
	}

	digestA, _ := DefinitionDigest(a)
	withoutSleep := proto.Clone(a).(*orkstackv1.Orkflow)
	withoutSleep.Tasks = withoutSleep.Tasks[:1]
	digestPlain, _ := DefinitionDigest(withoutSleep)
	if digestA == digestPlain {
		t.Fatal("adding a sleep did not change the digest")
	}
	digestLonger, _ := DefinitionDigest(withSleep(901))
	if digestA == digestLonger {
		t.Fatal("changing sleep.seconds did not change the digest")
	}
}
