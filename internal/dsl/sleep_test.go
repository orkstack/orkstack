package dsl_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestCompile_SleepTaskCompiles(t *testing.T) {
	wf, err := dsl.CompileFile("testdata/orkflows/sleeping-soak.ork.yaml")
	must.OKf(t, err, "compile the fixture")
	byID := map[string]*orkstackv1.Task{}
	for _, task := range wf.GetTasks() {
		byID[task.GetId()] = task
	}
	soak := byID["soak"]
	if soak.GetSleep().GetSeconds() != 900 {
		t.Fatalf("soak's SleepSpec = %v, want seconds 900", soak.GetSleep())
	}
	if soak.GetTimeoutSeconds() != 0 {
		t.Fatalf("a sleep's Task.timeout_seconds = %d, want 0", soak.GetTimeoutSeconds())
	}
	if soak.GetImage() != nil || soak.GetUses() != "" || soak.GetPool() != "" || soak.GetAwait() != nil {
		t.Fatalf("a sleep carries an image, uses, pool or await: %v", soak)
	}
	if got := soak.GetNeeds(); len(got) != 1 || got[0] != "canary" {
		t.Fatalf("needs = %v, want [canary]", got)
	}
	cool := byID["cool-down"]
	if cool.GetSleep().GetSeconds() != 60 || cool.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE {
		t.Fatalf("cool-down = %v, want a 60 s sleep with join all_done", cool)
	}
	if byID["canary"].GetSleep() != nil {
		t.Fatal("a container task compiled with a SleepSpec")
	}
}

// sleepSource is a one-task document whose task sleeps and carries extra.
// The pin `build` is declared only when extra names it: an unused pin is
// refused (rule 12).
func sleepSource(sleep, extra string) string {
	pins := ""
	if strings.Contains(extra, "pin:") {
		pins = "pins: {build: {}}\n"
	}
	return "apiVersion: orkstack/v1\nkind: Orkflow\nname: sleeper\n" + pins +
		"inputs: {l: {type: array}}\ntasks:\n  - id: soak\n    sleep: " + sleep + "\n" + extra
}

func TestCompile_SleepRefusesEveryExecutionField(t *testing.T) {
	cases := []struct {
		key    string
		yaml   string
		reason string
	}{
		{"with", "with: {op: exec}", "it runs no container"},
		{"command", "command: [echo]", "it runs no container"},
		{"env", "env: {A: b}", "it runs no container"},
		{"secrets", "secrets: [{name: db, source: 'env:DB'}]", "it runs no container"},
		{"resources", "resources: {cpu: '1'}", "it runs no container"},
		{"network", "network: none", "it runs no container"},
		{"pool", "pool: gpu", "it runs no container"},
		{"pin", "pin: build", "it runs no container"},
		{"retry", "retry: {maximum_attempts: 2}", "it runs no container"},
		{"timeout_seconds", "timeout_seconds: 60", "it runs no container"},
		{"cancellation_grace_period_seconds", "cancellation_grace_period_seconds: 5", "it runs no container"},
		{"run_as_orker_user", "run_as_orker_user: true", "it runs no container"},
		{"each", "each: {over: '{{ .inputs.l }}', max_items: 2}", "a sleep is one wait, not N"},
		{"cache", "cache: {enabled: true}", "it computes nothing to reuse"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			_, err := dsl.Compile([]byte(sleepSource("{seconds: 60}", "    "+tc.yaml+"\n")))
			want := `task "soak": a sleeping task takes only id, needs, condition, join and sleep; "` +
				tc.key + `" does not apply (` + tc.reason + `)`
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %v, want it to contain %q", err, want)
			}
			if tc.key == "timeout_seconds" && !strings.Contains(err.Error(), "— how long it sleeps is sleep.seconds") {
				t.Fatalf("timeout_seconds refusal %q does not point at sleep.seconds", err)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != "tasks[0]."+tc.key {
				t.Fatalf("error not anchored at tasks[0].%s: %#v", tc.key, err)
			}
		})
	}
	t.Run("run_as_orker_user false is absent", func(t *testing.T) {
		_, err := dsl.Compile([]byte(sleepSource("{seconds: 60}", "    run_as_orker_user: false\n")))
		must.OKf(t, err, "run_as_orker_user: false refused")
	})
	t.Run("an unknown key inside sleep", func(t *testing.T) {
		_, err := dsl.Compile([]byte(sleepSource("{seconds: 60, second: 1}", "")))
		if err == nil || !strings.Contains(err.Error(), `unknown key "second"`) {
			t.Fatalf("error = %v, want the unknown-key refusal", err)
		}
	})
}

func TestCompile_SleepIsExclusiveWithUsesAndAwait(t *testing.T) {
	cases := []struct {
		name  string
		extra string
		field string
		want  string
	}{
		{"uses", "    uses: shell\n", "tasks[0].uses", `task "soak": uses: and sleep: are exclusive — a sleeping task runs no image`},
		{"await", "    await: {timeout_seconds: 60}\n", "tasks[0].await", `task "soak": await: and sleep: are exclusive`},
		{"composed", "    uses: orkflow:child@1.0.0\n", "tasks[0].sleep", `"sleep" does not apply (it runs orkflow`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(sleepSource("{seconds: 60}", tc.extra)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != tc.field {
				t.Fatalf("error not anchored at %s: %#v", tc.field, err)
			}
		})
	}
}

func TestCompile_SleepSecondsBounds(t *testing.T) {
	cases := []struct {
		name    string
		sleep   string
		wantErr string
	}{
		{"absent", "{}", "sleep.seconds is required — how long the task sleeps"},
		{"zero", "{seconds: 0}", "sleep.seconds 0 is out of range (1 s … 30 days)"},
		{"negative", "{seconds: -1}", "sleep.seconds -1 is out of range (1 s … 30 days)"},
		{"one second", "{seconds: 1}", ""},
		{"thirty days", "{seconds: 2592000}", ""},
		{"over thirty days", "{seconds: 2592001}", "sleep.seconds 2592001 is out of range (1 s … 30 days)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(sleepSource(tc.sleep, "")))
			if tc.wantErr == "" {
				must.OKf(t, err, "refused")
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != "tasks[0].sleep.seconds" {
				t.Fatalf("error not anchored at tasks[0].sleep.seconds: %#v", err)
			}
		})
	}
	t.Run("a string", func(t *testing.T) {
		if _, err := dsl.Compile([]byte(sleepSource("{seconds: '60'}", ""))); err == nil {
			t.Fatal("seconds: '60' compiled")
		}
	})
}

// TestCompile_NothingReadsASleep is rule 51: every place a template or an
// each.over could read a task refuses a sleeping one.
func TestCompile_NothingReadsASleep(t *testing.T) {
	const head = "apiVersion: orkstack/v1\nkind: Orkflow\nname: reads\n"
	const sleeper = "  - id: soak\n    sleep: {seconds: 60}\n"
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"with", head + "tasks:\n" + sleeper +
			"  - id: use\n    uses: shell\n    needs: [soak]\n    with: {op: exec, command: [echo, '{{ .tasks.soak.output.x }}']}\n",
			`task "use": with.command[1]: "soak" is a sleeping task and produces no values or artifacts`},
		{"condition", head + "tasks:\n" + sleeper +
			"  - id: use\n    uses: shell\n    needs: [soak]\n    condition: '{{ .tasks.soak.output.go }}'\n",
			`task "use": condition: "soak" is a sleeping task`},
		{"index", head + "tasks:\n" + sleeper +
			"  - id: use\n    uses: shell\n    needs: [soak]\n    with: {op: exec, command: [echo, '{{ index .tasks \"soak\" \"output\" }}']}\n",
			`"soak" is a sleeping task`},
		{"artifact", head + "tasks:\n" + sleeper +
			"  - id: use\n    uses: shell\n    needs: [soak]\n    with: {op: exec, command: [cat, '{{ artifact \"soak/x.bin\" }}']}\n",
			`"soak" is a sleeping task and produces no values or artifacts`},
		{"composed with", head + "tasks:\n" + sleeper +
			"  - id: use\n    uses: orkflow:child@1.0.0\n    needs: [soak]\n    with: {x: '{{ .tasks.soak.output.x }}'}\n",
			`"soak" is a sleeping task`},
		{"outputs", head + "outputs: {x: '{{ .tasks.soak.output.x }}'}\ntasks:\n" + sleeper,
			`outputs.x: "soak" is a sleeping task`},
		{"each.over", head + "tasks:\n" + sleeper +
			"  - id: fan\n    uses: shell\n    needs: [soak]\n    each: {over: '{{ .tasks.soak.output.parts }}', max_items: 2}\n",
			`task "fan": each.over: "soak" is a sleeping task and produces no values or artifacts`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
	t.Run("needs alone is how a task follows a sleep", func(t *testing.T) {
		_, err := dsl.Compile([]byte(head + "tasks:\n" + sleeper +
			"  - id: use\n    uses: shell\n    needs: [soak]\n    with: {op: exec, command: [echo, done]}\n"))
		must.OKf(t, err, "refused")
	})
}

// TestValidate_ASleepContributesNoUpstreamReference: rule 44 counts a
// sleeping ancestor as required but weightless, because the engine hands a
// dependant no reference for it.
func TestValidate_ASleepContributesNoUpstreamReference(t *testing.T) {
	wf := compile(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: slept
tasks:
  - id: a
    uses: shell
  - id: soak
    sleep: {seconds: 60}
    needs: [a]
  - id: b
    uses: shell
    needs: [soak]
  - id: c
    uses: shell
    needs: [a]
`)
	withSleep, required, err := dsl.MinimalInputBytes(wf, "b")
	must.OK(t, err)
	if required != 2 {
		t.Fatalf("b has %d required ancestors, want 2 (soak and a)", required)
	}
	withoutSleep, _, err := dsl.MinimalInputBytes(wf, "c")
	must.OK(t, err)
	if withSleep != withoutSleep {
		t.Fatalf("b's minimal input = %d bytes, c's = %d: a sleeping ancestor added bytes", withSleep, withoutSleep)
	}
	if got := explicitMinimalInput(t, wf, "b"); got != withSleep {
		t.Fatalf("MinimalInputBytes(b) = %d, PayloadSize of the minimal input = %d", withSleep, got)
	}
	alone, _, err := dsl.MinimalInputBytes(wf, "soak")
	must.OK(t, err)
	if bare := explicitMinimalInput(t, wf, "soak"); alone != bare {
		t.Fatalf("MinimalInputBytes(soak) = %d, want %d", alone, bare)
	}
}

// TestValidate_ASleepingFailureHandlerMayNameManyNeeds: rule 42 bounds a
// container handler's needs because its input records each one; a sleep's
// has no input.
func TestValidate_ASleepingFailureHandlerMayNameManyNeeds(t *testing.T) {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: many\ntasks:\n")
	var needs []string
	for i := range dsl.MaxHandlerNeeds + 1 {
		fmt.Fprintf(&b, "  - id: t%d\n    uses: shell\n", i)
		needs = append(needs, fmt.Sprintf("t%d", i))
	}
	fmt.Fprintf(&b, "  - id: wait\n    join: all_done\n    needs: [%s]\n    sleep: {seconds: 60}\n", strings.Join(needs, ", "))
	_, err := dsl.Compile([]byte(b.String()))
	must.OKf(t, err, "a sleeping handler with %d needs refused", dsl.MaxHandlerNeeds+1)
}

// sleepSpellings are the ways an author might write sleep.seconds, each with
// the value the compiler reads (0: refused).
var sleepSpellings = []struct {
	yaml string
	want int32
}{
	{"0", 0}, {"1", 1}, {"2592000", 2592000}, {"2592001", 0}, {"-1", 0},
	{"1.5", 0}, {"60.0", 60}, {"'10'", 0}, {`"10"`, 0}, {"1e3", 1000}, {"0x10", 16},
	{"2147483648", 0}, {"-2147483649", 0}, {"99999999999999999999", 0},
	{"true", 0}, {".inf", 0}, {".nan", 0}, {"~", 0}, {"[60]", 0}, {"{a: 1}", 0},
}

// TestCompile_SleepSecondsSpellings is the DSL boundary table: every
// spelling is read exactly as await.timeout_seconds reads it — one integer
// rule for every *_seconds key — and a refused one is refused at
// tasks[0].sleep or tasks[0].sleep.seconds, never compiled into a sleep.
func TestCompile_SleepSecondsSpellings(t *testing.T) {
	compile := func(kind, value string) (int32, error) {
		key := map[string]string{"sleep": "sleep: {seconds: ", "await": "await: {timeout_seconds: "}[kind]
		wf, err := dsl.Compile([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: s\ntasks:\n  - id: t\n    " + key + value + "}\n"))
		if err != nil {
			return 0, err
		}
		if kind == "sleep" {
			return wf.GetTasks()[0].GetSleep().GetSeconds(), nil
		}
		return wf.GetTasks()[0].GetAwait().GetTimeoutSeconds(), nil
	}
	for _, tc := range sleepSpellings {
		t.Run(tc.yaml, func(t *testing.T) {
			got, err := compile("sleep", tc.yaml)
			if tc.want == 0 {
				if err == nil {
					t.Fatalf("sleep: {seconds: %s} compiled to %d, want a refusal", tc.yaml, got)
				}
				var verr *dsl.ValidationError
				if errors.As(err, &verr) && !strings.HasPrefix(verr.Field, "tasks[0].sleep") {
					t.Fatalf("refused at %s, want tasks[0].sleep…", verr.Field)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("sleep: {seconds: %s} = (%d, %v), want %d", tc.yaml, got, err, tc.want)
			}
			awaited, awaitErr := compile("await", tc.yaml)
			if (err == nil) != (awaitErr == nil) || got != awaited {
				t.Fatalf("seconds %s: sleep reads (%d, %v), await.timeout_seconds (%d, %v): the two *_seconds rules differ",
					tc.yaml, got, err, awaited, awaitErr)
			}
		})
	}
	t.Run("an empty map, and a key beside seconds", func(t *testing.T) {
		for _, sleep := range []string{"{}", "{seconds: 60, second: 1}", "{seconds: 60, timeout_seconds: 60}"} {
			if _, err := dsl.Compile([]byte(sleepSource(sleep, ""))); err == nil {
				t.Fatalf("sleep: %s compiled", sleep)
			}
		}
	})
}
