package dsl

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// taskLimitDoc is an Orkflow whose task b, after a, carries extra.
func taskLimitDoc(top, extra string) []byte {
	return []byte(concurrencyHeader + top + concurrencyTasks + "  - id: b\n    uses: busybox:1\n    needs: [a]\n" + extra)
}

const taskLimit = "    concurrency:\n      limit: 2\n"

// TestTaskConcurrency_Compiles: a container task's concurrency: block compiles
// to its own limit, always queued, beside a run-level one or not.
func TestTaskConcurrency_Compiles(t *testing.T) {
	queue := orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE
	for name, c := range map[string]struct {
		top, block string
		want       *orkstackv1.ConcurrencySpec
	}{
		"limit only": {"", taskLimit, &orkstackv1.ConcurrencySpec{Limit: 2, OnLimit: queue}},
		"every key": {"concurrency:\n  key: \"db:{{ .inputs.env }}\"\n  limit: 1\n",
			"    concurrency:\n      key: \"push:{{ .inputs.env }}\"\n      limit: 3\n      on_limit: queue\n      queue_timeout: 30m\n",
			&orkstackv1.ConcurrencySpec{Limit: 3, Key: "push:{{ .inputs.env }}", OnLimit: queue, QueueTimeoutSeconds: 1800}},
		"its default key beside the run's": {"concurrency:\n  limit: 1\n", taskLimit, &orkstackv1.ConcurrencySpec{Limit: 2, OnLimit: queue}},
		"a cached task":                    {"", taskLimit + "    cache:\n      enabled: true\n", &orkstackv1.ConcurrencySpec{Limit: 2, OnLimit: queue}},
		"a task a need may skip":           {"", taskLimit + "    join: none_failed\n", &orkstackv1.ConcurrencySpec{Limit: 2, OnLimit: queue}},
	} {
		t.Run(name, func(t *testing.T) {
			wf, err := CompileWithOptions(taskLimitDoc(c.top, c.block), Options{})
			if err != nil {
				t.Fatal(err)
			}
			if got := wf.GetTasks()[1].GetConcurrency(); !proto.Equal(got, c.want) || wf.GetTasks()[0].GetConcurrency() != nil {
				t.Fatalf("limit %v, want %v on that task alone", got, c.want)
			}
		})
	}
}

// TestTaskConcurrency_Refusals: a task that takes no slot of its own, a
// strategy that does not wait, and the run's own key are refused, naming the
// task and the field.
func TestTaskConcurrency_Refusals(t *testing.T) {
	header := "apiVersion: orkstack/v1\nkind: Orkflow\nname: migrate\nversion: \"1\"\n"
	for name, c := range map[string]struct {
		doc         []byte
		want, field string
	}{
		"no limit":              {taskLimitDoc("", "    concurrency:\n      key: k\n"), `task "b": concurrency.limit is 0`, "tasks[1].concurrency"},
		"an unknown key":        {taskLimitDoc("", taskLimit+"      burst: 1\n"), "burst", ""},
		"refuse":                {taskLimitDoc("", taskLimit+"      on_limit: refuse\n"), `task "b": concurrency.on_limit: refuse`, "tasks[1].concurrency"},
		"skip":                  {taskLimitDoc("", taskLimit+"      on_limit: skip\n"), "a task always waits", "tasks[1].concurrency"},
		"max_queued":            {taskLimitDoc("", taskLimit+"      max_queued: 3\n"), "max_queued", ""},
		"a bad key":             {taskLimitDoc("", taskLimit+"      key: \"{{ .tasks.a.output.x }}\"\n"), `task "b": concurrency.key`, "tasks[1].concurrency"},
		"a reserved key":        {taskLimitDoc("", taskLimit+"      key: \"ork:x\"\n"), "CONCURRENCY_KEY_INVALID", "tasks[1].concurrency"},
		"a bad timeout":         {taskLimitDoc("", taskLimit+"      queue_timeout: 0s\n"), `task "b": concurrency.queue_timeout`, "tasks[1].concurrency"},
		"the run's literal key": {taskLimitDoc("concurrency:\n  key: db\n  limit: 1\n", taskLimit+"      key: db\n"), "the run's own", "tasks[1].concurrency"},
		"the run's template":    {taskLimitDoc("concurrency:\n  key: \"db:{{ .inputs.env }}\"\n  limit: 1\n", taskLimit+"      key: \"db:{{ .inputs.env }}\"\n"), "the run's own", "tasks[1].concurrency"},
		"a fan-out":             {[]byte(header + "inputs: {l: {type: array}}\ntasks:\n  - id: b\n    uses: busybox:1\n" + taskLimit + "    each: {over: '{{ .inputs.l }}', max_items: 2}\n"), "a fan-out", "tasks[0].concurrency"},
		"a pin's member":        {[]byte(header + "pins: {p: {}}\ntasks:\n  - id: b\n    uses: busybox:1\n    pin: p\n" + taskLimit), "a pin's member", "tasks[0].concurrency"},
		"a failure handler":     {taskLimitDoc("", taskLimit+"    join: any_failed\n"), "a failure handler", "tasks[1].concurrency"},
		"an all_done handler":   {taskLimitDoc("", taskLimit+"    join: all_done\n"), "a failure handler", "tasks[1].concurrency"},
		"a composed task":       {[]byte(header + "tasks:\n  - id: b\n    uses: orkflow:inner@1.0.0\n" + taskLimit), `"concurrency" does not apply`, "tasks[0].concurrency"},
		"an awaited task":       {[]byte(header + "tasks:\n  - id: b\n    await: {timeout_seconds: 60}\n" + taskLimit), `"concurrency" does not apply`, "tasks[0].concurrency"},
		"a sleeping task":       {[]byte(header + "tasks:\n  - id: b\n    sleep: {seconds: 60}\n" + taskLimit), `"concurrency" does not apply`, "tasks[0].concurrency"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := CompileWithOptions(c.doc, Options{})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("refused as %v; want it to say %q", err, c.want)
			}
			var v *ValidationError
			if c.field != "" && (!errors.As(err, &v) || v.Field != c.field) {
				t.Fatalf("refused as %v, not a ValidationError of %s", err, c.field)
			}
		})
	}
}

// TestTaskConcurrency_IsCheckedOnACompiledDefinition: a definition that never
// met the document's own checks is held to the same rules.
func TestTaskConcurrency_IsCheckedOnACompiledDefinition(t *testing.T) {
	wf, err := CompileWithOptions(taskLimitDoc("", taskLimit), Options{})
	if err != nil {
		t.Fatal(err)
	}
	spec := wf.Tasks[1].Concurrency
	spec.OnLimit = orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP
	if err := validate(wf, Options{}); err == nil {
		t.Fatal("a skip limit on a task was accepted")
	}
	spec.OnLimit, spec.MaxQueued = orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE, 3
	if err := validate(wf, Options{}); err == nil || !strings.Contains(err.Error(), "max_queued") {
		t.Fatalf("a task's max_queued: %v; want it refused", err)
	}
	for _, doc := range []string{"  - id: b\n    uses: orkflow:inner@1.0.0\n", "  - id: b\n    await: {timeout_seconds: 60}\n", "  - id: b\n    sleep: {seconds: 60}\n"} {
		wf, err := CompileWithOptions([]byte(concurrencyHeader+"tasks:\n"+doc), Options{})
		if err != nil {
			t.Fatal(err)
		}
		wf.Tasks[0].Concurrency = &orkstackv1.ConcurrencySpec{Limit: 1, OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE}
		if err := validate(wf, Options{}); err == nil {
			t.Errorf("a limit on %q was accepted", doc)
		}
	}
}

// TestTaskConcurrency_Digest: a task's limit is in the definition's digest
// only when it is there.
func TestTaskConcurrency_Digest(t *testing.T) {
	digest := func(extra string) string {
		t.Helper()
		wf, err := CompileWithOptions(taskLimitDoc("", extra), Options{})
		if err != nil {
			t.Fatal(err)
		}
		d, err := DefinitionDigest(wf)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	if digest("") == digest(taskLimit) || digest(taskLimit) == digest("    concurrency:\n      limit: 3\n") {
		t.Fatal("a limit leaves the digest unchanged")
	}
	if digest(taskLimit) != digest(taskLimit+"      on_limit: queue\n") {
		t.Fatal("the default on_limit, written out, is another definition")
	}
}
