package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// limitedYAML declares a run-level limit; block is its concurrency: body.
func limitedYAML(block string) string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: migrate
version: "1.0.0"
inputs:
  env:
    type: string
    default: prod
concurrency:
` + block + `
tasks:
  - id: apply
    uses: shell
    with: { op: eval, command: ["true"] }
`
}

const templatedLimit = `  key: "db:{{ .inputs.env }}"
  limit: 2`

// concStarter is a fakeStarter whose cluster answers whether it indexes a
// concurrency limit's attributes, as the engine's TemporalStarter does.
type concStarter struct {
	*fakeStarter
	indexed bool
}

func (c *concStarter) ConcurrencyIndexAnswer() bool { return c.indexed }

func concService(t *testing.T, starter engine.RunStarter, caps engine.CapabilityChecker, claimer api.IdempotencyClaimer) *api.Service {
	t.Helper()
	cfg := api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, Capabilities: caps,
		Runs: engine.NewRunStore(), Bus: engine.NewEventBus()}
	if claimer != nil {
		cfg.Idempotency = claimer
	}
	return api.NewService(cfg)
}

func submitEnv(svc *api.Service, name, env, key string) error {
	req := &orkstackv1.SubmitRunRequest{OrkflowName: name, IdempotencyKey: key}
	if env != "" {
		data, err := structpb.NewStruct(map[string]any{"env": env})
		if err != nil {
			return err
		}
		req.Input = &orkstackv1.Payload{Data: data}
	}
	_, err := svc.SubmitRun(context.Background(), connect.NewRequest(req))
	return err
}

// TestSubmitRun_PinsTheRunConcurrencyPlan: a run whose Orkflow declares a
// limit carries its rendered key, its gate and every number the run waits
// with; one that declares none carries nothing, and asks the fleet nothing.
func TestSubmitRun_PinsTheRunConcurrencyPlan(t *testing.T) {
	defaults := engine.DefaultConcurrencySettings()
	for _, tc := range []struct {
		name, block, env, wantKey, wantLabel string
		wantTimeout                          int64
	}{
		{name: "rendered", block: templatedLimit, env: "staging", wantKey: "db:staging", wantLabel: "db:{{ .inputs.env }}",
			wantTimeout: int64(defaults.DefaultQueueTimeout / time.Second)},
		{name: "a default input", block: templatedLimit, wantKey: "db:prod", wantLabel: "db:{{ .inputs.env }}",
			wantTimeout: int64(defaults.DefaultQueueTimeout / time.Second)},
		{name: "the Orkflow's own key", block: "  limit: 1\n  queue_timeout: 30m", wantKey: "ork:orkflow:default/migrate",
			wantLabel: "ork:orkflow:default/migrate", wantTimeout: 1800},
	} {
		t.Run(tc.name, func(t *testing.T) {
			starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
			caps := &fakeCapabilities{}
			svc := concService(t, starter, caps, nil)
			must.OK(t, applyDoc(t, svc, limitedYAML(tc.block)))
			must.OKf(t, submitEnv(svc, "migrate", tc.env, ""), "submit")
			plan := starter.plans[0]
			c := plan.Concurrency
			if c == nil || len(c.Slots) != 1 || c.Run == nil || *c.Run != 0 {
				t.Fatalf("plan Concurrency = %+v, want one run-level slot", c)
			}
			slot := c.Slots[0]
			if slot.Key != tc.wantKey || slot.Label != tc.wantLabel || slot.GateID != engine.GateID("default", "", tc.wantKey) ||
				slot.Strategy != engine.ConcurrencyStrategyQueue || slot.QueueTimeoutSeconds != tc.wantTimeout {
				t.Fatalf("slot = %+v, want key %q label %q timeout %ds on its gate", slot, tc.wantKey, tc.wantLabel, tc.wantTimeout)
			}
			if c.SeqBase != 1 || c.Namespace != "default" || c.Incarnation != "" || c.GateQueue != "ork-tasks" ||
				c.AcquireTimeout != engine.DefaultGateTunables().AcquireTimeout || c.RequeueInterval != defaults.RequeueInterval ||
				c.PerSlotBytes != defaults.PerSlotBytes || c.Gate != engine.DefaultGateTunables() {
				t.Fatalf("plan Concurrency = %+v, want the deployment's numbers", c)
			}
			if got, want := plan.History.Reserve.Fixed, engine.DefaultHistoryReserve.Fixed; got <= want {
				t.Fatalf("history reserve Fixed = %d, want above %d for the release the run sends", got, want)
			}
			if len(caps.asked) != 1 || caps.asked[0] != capabilityQuestion(engine.CapabilityConcurrency) {
				t.Fatalf("asked %v, want %s once", caps.asked, engine.CapabilityConcurrency)
			}
		})
	}

	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	caps := &fakeCapabilities{}
	svc := concService(t, starter, caps, nil)
	must.OK(t, applyDoc(t, svc, untimedInnerYAML))
	must.OKf(t, submitName(svc, "inner"), "submit")
	if plan := starter.plans[0]; plan.Concurrency != nil || plan.History.Reserve != engine.DefaultHistoryReserve || caps.questions() != 0 {
		t.Fatalf("a run with no limit: Concurrency %+v, reserve %+v, %d fleet questions; want none, the default, 0",
			plan.Concurrency, plan.History.Reserve, caps.questions())
	}
}

// TestSubmitRun_RefusesALimitTheDeploymentCannotHold: CONCURRENCY_UNSUPPORTED
// names every cause that applies; a fleet that cannot be read is
// unavailable; a key that does not render is the caller's. None starts a run.
func TestSubmitRun_RefusesALimitTheDeploymentCannotHold(t *testing.T) {
	for _, tc := range []struct {
		name     string
		indexed  bool
		caps     *fakeCapabilities
		input    map[string]any
		wantCode connect.Code
		wantText []string
	}{
		{name: "unindexed", caps: &fakeCapabilities{},
			wantCode: connect.CodeFailedPrecondition, wantText: []string{"CONCURRENCY_UNSUPPORTED", "cause=index"}},
		{name: "an older process", indexed: true, caps: &fakeCapabilities{lacking: []string{"1@h@ ork-caps="}},
			wantCode: connect.CodeFailedPrecondition, wantText: []string{"CONCURRENCY_UNSUPPORTED", "cause=fleet", "1 engine process runs"}},
		{name: "both", caps: &fakeCapabilities{lacking: []string{"a", "b"}},
			wantCode: connect.CodeFailedPrecondition, wantText: []string{"cause=index", "cause=fleet", "2 engine processes run"}},
		{name: "the fleet cannot be read", indexed: true, caps: &fakeCapabilities{err: errors.New("describe failed")},
			wantCode: connect.CodeUnavailable, wantText: []string{"CONCURRENCY_CHECK_UNAVAILABLE", "scope=fleet"}},
		{name: "a key that renders badly", indexed: true, caps: &fakeCapabilities{}, input: map[string]any{"env": "a b"},
			wantCode: connect.CodeInvalidArgument, wantText: []string{"CONCURRENCY_KEY_INVALID", "offset 4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: tc.indexed}
			svc := concService(t, starter, tc.caps, nil)
			must.OK(t, applyDoc(t, svc, limitedYAML(templatedLimit)))
			req := &orkstackv1.SubmitRunRequest{OrkflowName: "migrate"}
			if tc.input != nil {
				data, err := structpb.NewStruct(tc.input)
				must.OK(t, err)
				req.Input = &orkstackv1.Payload{Data: data}
			}
			_, err := svc.SubmitRun(context.Background(), connect.NewRequest(req))
			if connect.CodeOf(err) != tc.wantCode {
				t.Fatalf("submit = %v; want %s", err, tc.wantCode)
			}
			for _, text := range tc.wantText {
				if !strings.Contains(err.Error(), text) {
					t.Fatalf("submit = %v; want it to name %q", err, text)
				}
			}
			if len(starter.startedRuns()) != 0 {
				t.Fatal("a refused submit started a run")
			}
		})
	}
}

// TestConcurrency_QueueTimeoutBounds: a queue_timeout outside the
// deployment's bounds is refused at create, naming the setting; a version
// created before the bounds tightened is refused at submit.
func TestConcurrency_QueueTimeoutBounds(t *testing.T) {
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	svc := concService(t, starter, &fakeCapabilities{}, nil)
	err := applyDoc(t, svc, limitedYAML("  limit: 1\n  queue_timeout: 5s"))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "engine.concurrency.min_queue_timeout") {
		t.Fatalf("create = %v; want invalid argument naming engine.concurrency.min_queue_timeout", err)
	}
	err = applyDoc(t, svc, limitedYAML("  limit: 1\n  queue_timeout: 192h"))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "engine.concurrency.max_queue_timeout") {
		t.Fatalf("create = %v; want invalid argument naming engine.concurrency.max_queue_timeout", err)
	}

	store := engine.NewMemoryOrkflowStore()
	wide := api.NewService(api.Config{Orkflows: store, Starter: starter, Capabilities: &fakeCapabilities{}})
	must.OK(t, applyDoc(t, wide, limitedYAML("  limit: 1\n  queue_timeout: 6h")))
	tight := engine.DefaultConcurrencySettings()
	tight.MaxQueueTimeout, tight.DefaultQueueTimeout = time.Hour, 30*time.Minute
	narrow := api.NewService(api.Config{Orkflows: store, Starter: starter, Capabilities: &fakeCapabilities{}, Concurrency: tight})
	err = submitName(narrow, "migrate")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "engine.concurrency.max_queue_timeout") {
		t.Fatalf("submit = %v; want failed precondition naming engine.concurrency.max_queue_timeout", err)
	}
}

const limitedChildYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: inner
version: "1.0.0"
concurrency:
  limit: 1
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
`

// TestSubmitRun_RefusesALimitInAComposedChild: a child run's limit is not
// planned by this release, so a tree that would start one is refused rather
// than run the child outside its limit.
func TestSubmitRun_RefusesALimitInAComposedChild(t *testing.T) {
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	svc := concService(t, starter, &fakeCapabilities{}, nil)
	must.OK(t, applyDoc(t, svc, limitedChildYAML))
	must.OK(t, applyDoc(t, svc, outerYAML("")))
	err := submitName(svc, "outer")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "CONCURRENCY_UNSUPPORTED") ||
		!strings.Contains(err.Error(), "inner@1.0.0") {
		t.Fatalf("submit = %v; want CONCURRENCY_UNSUPPORTED naming inner@1.0.0", err)
	}
	if len(starter.startedRuns()) != 0 {
		t.Fatal("a refused submit started a run")
	}
}

// TestSubmitRun_AKeyedRetryReturnsItsRunWhateverTheLimitChecks: a retry
// under an idempotency key the first submit bound returns that run, even
// when the deployment could no longer accept a new one with the limit.
func TestSubmitRun_AKeyedRetryReturnsItsRunWhateverTheLimitChecks(t *testing.T) {
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	caps := &fakeCapabilities{}
	svc := concService(t, starter, caps, newSharedClaimer())
	must.OK(t, applyDoc(t, svc, limitedYAML(templatedLimit)))
	must.OKf(t, submitEnv(svc, "migrate", "staging", "order-42"), "first submit")
	caps.mu.Lock()
	caps.lacking = []string{"1@h@ ork-caps="}
	caps.mu.Unlock()
	must.OKf(t, submitEnv(svc, "migrate", "staging", "order-42"), "keyed retry")
	if got := len(starter.startedRuns()); got != 1 {
		t.Fatalf("started %d runs, want the one", got)
	}
	if err := submitEnv(svc, "migrate", "staging", "order-43"); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("a new key = %v; want the fleet's refusal", err)
	}
}

// TestSubmitRun_ALimitInANamedNamespace: the gate is the namespace's
// incarnation's, and the limit's line bound is pinned.
func TestSubmitRun_ALimitInANamedNamespace(t *testing.T) {
	created := time.Date(2026, 11, 2, 9, 30, 0, 123456789, time.UTC)
	registry := newFakeRegistry(&orkstackv1.Namespace{Name: "payments", CreatedAt: timestamppb.New(created)})
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: registry, Starter: starter,
		Capabilities: &fakeCapabilities{}})
	doc := strings.Replace(limitedYAML(templatedLimit+"\n  max_queued: 7"), "name: migrate\n", "name: migrate\nnamespace: payments\n", 1)
	must.OK(t, applyDoc(t, svc, doc))
	_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "migrate", Namespace: "payments"}))
	must.OKf(t, err, "submit")
	c := starter.plans[0].Concurrency
	incarnation := "2026-11-02T09:30:00.123456789Z"
	if c.Namespace != "payments" || c.Incarnation != incarnation || c.Slots[0].GateID != engine.GateID("payments", incarnation, "db:prod") ||
		c.Slots[0].MaxQueued != 7 {
		t.Fatalf("plan Concurrency = %+v, want payments' incarnation %s and max_queued 7", c, incarnation)
	}
}

// TestSubmitRun_ARefusedLimitBindsNoKey: a keyed submit the limit refuses
// leaves its key unbound, so the same key submits once the deployment can.
func TestSubmitRun_ARefusedLimitBindsNoKey(t *testing.T) {
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	caps := &fakeCapabilities{lacking: []string{"1@h@ ork-caps="}}
	svc := concService(t, starter, caps, newSharedClaimer())
	must.OK(t, applyDoc(t, svc, limitedYAML(templatedLimit)))
	if err := submitEnv(svc, "migrate", "", "order-7"); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("submit = %v; want the fleet's refusal", err)
	}
	caps.mu.Lock()
	caps.lacking = nil
	caps.mu.Unlock()
	must.OKf(t, submitEnv(svc, "migrate", "", "order-7"), "the same key once the fleet can")
	if got := len(starter.startedRuns()); got != 1 {
		t.Fatalf("started %d runs, want 1", got)
	}
}

// TestNewService_UnusableConcurrencySettingsAreTheDefaults: a partly set
// value would pin zeros into a run's wait, so it is the defaults instead.
func TestNewService_UnusableConcurrencySettingsAreTheDefaults(t *testing.T) {
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, Capabilities: &fakeCapabilities{},
		Concurrency: engine.ConcurrencySettings{MaxQueueTimeout: time.Hour}})
	must.OK(t, applyDoc(t, svc, limitedYAML(templatedLimit)))
	must.OKf(t, submitName(svc, "migrate"), "submit")
	if c := starter.plans[0].Concurrency; c.RequeueInterval != engine.DefaultConcurrencySettings().RequeueInterval ||
		c.Slots[0].QueueTimeoutSeconds != int64(engine.DefaultConcurrencySettings().DefaultQueueTimeout/time.Second) {
		t.Fatalf("plan Concurrency = %+v, want the defaults", c)
	}
}
