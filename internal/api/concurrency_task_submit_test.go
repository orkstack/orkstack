package api_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// taskLimitsYAML is an Orkflow with one container task a limit block, t0, t1
// and so on, under a run-level limit when top is set.
func taskLimitsYAML(top string, blocks ...string) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: migrate\nversion: \"1.0.0\"\ninputs:\n  env:\n    type: string\n    default: prod\n")
	if top != "" {
		b.WriteString("concurrency:\n" + top + "\n")
	}
	b.WriteString("tasks:\n")
	for i, block := range blocks {
		fmt.Fprintf(&b, "  - id: t%d\n    uses: shell\n    with: { op: eval, command: [\"true\"] }\n", i)
		if block != "" {
			b.WriteString("    concurrency: " + block + "\n")
		}
	}
	return b.String()
}

// TestSubmitRun_PinsTaskLimits: a task's limit is a slot of the run's plan,
// its key rendered as the run's is; tasks that declare the same limit share
// one slot, a task with none has none, and the run's own slot is untouched.
func TestSubmitRun_PinsTaskLimits(t *testing.T) {
	starter, caps := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}, &fakeCapabilities{}
	reader := &fakeConcReader{}
	svc := concServiceWith(t, starter, caps, nil, reader)
	must.OK(t, applyDoc(t, svc, taskLimitsYAML("  key: db\n  limit: 1", "{limit: 2, key: \"push:{{ .inputs.env }}\", queue_timeout: 30m}",
		"{limit: 2, key: \"push:{{ .inputs.env }}\", queue_timeout: 30m}", "{limit: 1}", "")))
	must.OK(t, submitEnv(svc, "migrate", "staging", ""))
	plan := starter.plans[0]
	c := plan.Concurrency
	if len(c.Slots) != 3 || c.Run == nil || *c.Run != 0 || len(c.Tasks) != 3 || c.Tasks["t0"] != 1 || c.Tasks["t1"] != 1 || c.Tasks["t2"] != 2 {
		t.Fatalf("plan %+v; want the run's slot, one t0 and t1 share, and t2's own", c)
	}
	want := engine.ConcurrencySlotPlan{GateID: engine.GateID("default", "", "push:staging"), Key: "push:staging", Label: "push:{{ .inputs.env }}",
		Limit: 2, Strategy: engine.ConcurrencyStrategyQueue, QueueTimeoutSeconds: 1800}
	if c.Slots[1] != want || c.Slots[2].Key != "ork:task:default/migrate:t2" || c.Slots[2].QueueTimeoutSeconds != 7200 {
		t.Fatalf("slots %+v", c.Slots[1:])
	}
	one := engine.ReserveConcurrencyRelease(&engine.HistoryLimit{Reserve: engine.DefaultHistoryReserve}, &engine.ConcurrencyPlan{PerSlotBytes: c.PerSlotBytes, Slots: c.Slots[:1]})
	if got, want := plan.History.Reserve.Fixed-engine.DefaultHistoryReserve.Fixed, 3*(one.Reserve.Fixed-engine.DefaultHistoryReserve.Fixed); got != want {
		t.Fatalf("the reserve grew by %d, want %d: one release a gate", got, want)
	}

	// With no limit of its own a run asks the deployment all the same.
	starter, caps = &concStarter{fakeStarter: &fakeStarter{}, indexed: true}, &fakeCapabilities{}
	svc = concService(t, starter, caps, nil)
	must.OK(t, applyDoc(t, svc, taskLimitsYAML("", "{limit: 1}")))
	asked := caps.questions()
	must.OK(t, submitName(svc, "migrate"))
	if c := starter.plans[0].Concurrency; c.Run != nil || len(c.Slots) != 1 || asked == 0 || caps.questions() != asked+1 {
		t.Fatalf("plan %+v after %d then %d questions; want one task slot and the fleet asked at create and at submit", c, asked, caps.questions()-asked)
	}
}

// TestSubmitRun_TaskLimitRefusals: a key that does not render, or renders to
// the run's own, a deployment that cannot hold a limit, and a namespace with
// no key left are refused before a run exists; a full line is not.
func TestSubmitRun_TaskLimitRefusals(t *testing.T) {
	gate := engine.GateID("default", "", "push")
	for name, c := range map[string]struct {
		doc     string
		indexed bool
		reader  *fakeConcReader
		code    connect.Code
		want    string
	}{
		"the run's own key, rendered": {doc: taskLimitsYAML("  key: \"db:{{ .inputs.env }}\"\n  limit: 1", "{limit: 1, key: \"db:prod\"}"), indexed: true,
			code: connect.CodeInvalidArgument, want: engine.ConcurrencyKeyConflictCode},
		"a key that breaks the rule": {doc: taskLimitsYAML("", "{limit: 1, key: \"{{ .inputs.missing }}x\"}"), indexed: true,
			code: connect.CodeInvalidArgument, want: "CONCURRENCY_KEY_INVALID"},
		"no index": {doc: taskLimitsYAML("", "{limit: 1}"), code: connect.CodeFailedPrecondition, want: engine.ConcurrencyUnsupportedCode},
		"no key left": {doc: taskLimitsYAML("", "{limit: 1, key: push}"), indexed: true, reader: &fakeConcReader{active: 1000},
			code: connect.CodeResourceExhausted, want: engine.ConcurrencyKeysExhaustedCode},
		"the keys in use cannot be counted": {doc: taskLimitsYAML("", "{limit: 1, key: push}"), indexed: true, reader: &fakeConcReader{activeErr: errors.New("raw words")},
			code: connect.CodeUnavailable, want: "scope=namespace"},
		"the line cannot be read at the bound": {doc: taskLimitsYAML("", "{limit: 1, key: push}"), indexed: true, reader: &fakeConcReader{active: 1000, lineErr: errors.New("raw words")},
			code: connect.CodeUnavailable, want: "scope=key"},
		"a key in use at the bound": {doc: taskLimitsYAML("", "{limit: 1, key: push}"), indexed: true,
			reader: &fakeConcReader{active: 1000, lines: map[string]engine.GateLine{gate: {Present: true, Holders: 1}}}},
		"a full line": {doc: taskLimitsYAML("", "{limit: 1, key: push}"), indexed: true,
			reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 100000, Queued: 100000}}}},
	} {
		t.Run(name, func(t *testing.T) {
			starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: c.indexed}
			if c.reader == nil {
				c.reader = &fakeConcReader{}
			}
			svc := concServiceWith(t, starter, &fakeCapabilities{}, nil, c.reader)
			must.OK(t, applyDoc(t, svc, c.doc))
			err := submitName(svc, "migrate")
			if c.want == "" {
				must.OK(t, err)
				return
			}
			if connect.CodeOf(err) != c.code || !strings.Contains(err.Error(), c.want) || strings.Contains(err.Error(), "raw words") || len(starter.plans) != 0 {
				t.Fatalf("submit = %v after %d starts; want %v %s, and no run", err, len(starter.plans), c.code, c.want)
			}
		})
	}
}

// TestConcurrency_TaskLimitBounds: how many limits an Orkflow declares, how
// many slots one run may take, and each queue_timeout are held to the
// deployment's bounds at create, naming the setting, and again at submit.
func TestConcurrency_TaskLimitBounds(t *testing.T) {
	limits := func(n int, block string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = strings.ReplaceAll(block, "#", fmt.Sprint(i))
		}
		return out
	}
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	for name, c := range map[string]struct{ doc, want string }{
		"33 limits":                          {taskLimitsYAML("", limits(33, "{limit: 1, key: k}")...), "max_limits_per_orkflow"},
		"32 limits":                          {taskLimitsYAML("", limits(32, "{limit: 1, key: k}")...), ""},
		"32 keys of their own":               {taskLimitsYAML("", limits(32, "{limit: 1}")...), ""},
		"many keys, few tasks each":          {taskLimitsYAML("", append(limits(16, "{limit: 2, key: a}"), limits(15, "{limit: 60, key: \"b#\"}")...)...), ""},
		"one key, its limit above its tasks": {taskLimitsYAML("", limits(32, "{limit: 1000, key: k}")...), ""},
		"a task's timeout":                   {taskLimitsYAML("", "{limit: 1, queue_timeout: 5s}"), "min_queue_timeout"},
	} {
		t.Run(name, func(t *testing.T) {
			err := applyDoc(t, concService(t, starter, &fakeCapabilities{}, nil), c.doc)
			if c.want == "" {
				must.OK(t, err)
			} else if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "engine.concurrency."+c.want) {
				t.Fatalf("create = %v; want invalid argument naming engine.concurrency.%s", err, c.want)
			}
		})
	}
	few := engine.DefaultConcurrencySettings()
	few.MaxRunSlotEntries = 2
	store := engine.NewMemoryOrkflowStore()
	narrow := func() *api.Service {
		return api.NewService(api.Config{Orkflows: store, Starter: starter, Capabilities: &fakeCapabilities{}, Concurrency: few, ConcurrencyReader: &fakeConcReader{}})
	}
	// One run-level slot, and two tasks of one key: both may hold, never a
	// third entry.
	doc := taskLimitsYAML("  limit: 1", "{limit: 2, key: k}", "{limit: 5, key: k}")
	if err := applyDoc(t, narrow(), doc); connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "for 3 concurrency slots") {
		t.Fatalf("create = %v; want refused for 3 slots", err)
	}
	// Three tasks of one key of limit one: one holding, one waiting.
	shared := taskLimitsYAML("", "{limit: 1, key: k}", "{limit: 1, key: k}", "{limit: 1, key: k}")
	must.OK(t, applyDoc(t, api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, Capabilities: &fakeCapabilities{},
		Concurrency: few, ConcurrencyReader: &fakeConcReader{}}), shared))
	// The one waiting is counted: under a bound of one, those three are
	// refused for two, one holding and one in line.
	one := engine.DefaultConcurrencySettings()
	one.MaxRunSlotEntries = 1
	if err := applyDoc(t, api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, Capabilities: &fakeCapabilities{},
		Concurrency: one, ConcurrencyReader: &fakeConcReader{}}), shared); connect.CodeOf(err) != connect.CodeInvalidArgument ||
		!strings.Contains(err.Error(), "for 2 concurrency slots") {
		t.Fatalf("create = %v; want refused for 2 slots, its waiting one counted", err)
	}
	// Three of one key template may each hold a key of its own: three.
	templated := taskLimitsYAML("", "{limit: 1, key: \"db:{{ .inputs.env }}\"}", "{limit: 1, key: \"db:{{ .inputs.env }}\"}", "{limit: 1, key: \"db:{{ .inputs.env }}\"}")
	if err := applyDoc(t, narrow(), templated); connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "for 3 concurrency slots") {
		t.Fatalf("create = %v; want a template counted once a task, 3 slots", err)
	}
	wide := api.NewService(api.Config{Orkflows: store, Starter: starter, Capabilities: &fakeCapabilities{}})
	must.OK(t, applyDoc(t, wide, doc))
	if err := submitName(narrow(), "migrate"); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "max_run_slot_entries") {
		t.Fatalf("submit = %v; want failed precondition naming the bound", err)
	}
}

// TestSubmitRun_PlansAComposedChildsTaskLimits: a child's tasks' limits are
// planned with their keys as written, for its parent to render, beside its own;
// a literal one its parent or a run above holds, or its own, is refused.
func TestSubmitRun_PlansAComposedChildsTaskLimits(t *testing.T) {
	inner := strings.Replace(limitedChildYAML, "concurrency:\n  limit: 1\n", "", 1) +
		"  - id: push\n    uses: shell\n    with: { op: eval, command: [\"true\"] }\n    concurrency: {limit: 2, key: \"push:{{ .inputs.env }}\"}\n" +
		"  - id: tag\n    uses: shell\n    with: { op: eval, command: [\"true\"] }\n    concurrency: {limit: 1}\n"
	inner = strings.Replace(inner, "version: \"1.0.0\"\n", "version: \"1.0.0\"\ninputs:\n  env:\n    type: string\n    default: prod\n", 1)
	starter, caps := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}, &fakeCapabilities{}
	svc := concService(t, starter, caps, nil)
	must.OK(t, applyDoc(t, svc, inner))
	must.OK(t, applyDoc(t, svc, composingYAML("outer", "")))
	must.OK(t, submitName(svc, "outer"))
	child := starter.plans[0].Children["release"].Concurrency
	if child == nil || child.Slot.Key != "" || len(child.Tasks) != 2 || child.Tasks["push"].Key != "push:{{ .inputs.env }}" ||
		child.Tasks["tag"].Key != "ork:task:default/inner:tag" || child.Tasks["tag"].Strategy != engine.ConcurrencyStrategyQueue || caps.questions() == 0 {
		t.Fatalf("child %+v", child)
	}

	// (A literal one equal to the child's own is refused when the child is
	// created: rule 66.)
	for name, c := range map[string]struct{ outerKey, innerKey string }{
		"its parent's key": {"push", "push"},
	} {
		t.Run(name, func(t *testing.T) {
			doc := strings.Replace(inner, `key: "push:{{ .inputs.env }}"`, `key: "`+c.innerKey+`"`, 1)
			starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
			svc := concService(t, starter, &fakeCapabilities{}, nil)
			must.OK(t, applyDoc(t, svc, doc))
			must.OK(t, applyDoc(t, svc, composingYAML("outer", c.outerKey)))
			if err := submitName(svc, "outer"); connect.CodeOf(err) != connect.CodeInvalidArgument ||
				!strings.Contains(err.Error(), engine.ConcurrencyKeyConflictCode) || len(starter.plans) != 0 {
				t.Fatalf("submit = %v; want %s, and no run", err, engine.ConcurrencyKeyConflictCode)
			}
		})
	}
}
