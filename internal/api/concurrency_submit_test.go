package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/temporal"
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

// fakeConcReader is the cluster's index and the gates as submit reads them:
// one line per gate id, and the namespace's counts.
type fakeConcReader struct {
	mu        sync.Mutex
	lines     map[string]engine.GateLine
	active    int64
	queued    int64
	lineErr   error
	activeErr error
	queuedErr error
	open      int64
	openErr   error
	asked     []string
	// hang makes GateLine wait for its context to end, as a gate that
	// does not answer does.
	hang bool
	// closed are the holder runs the cluster reports ended; describeErr
	// fails the describe; verified records each gate sent a verify.
	closed      map[string]bool
	describeErr error
	verified    []string
	// gates are the namespace's gates ConcurrencyService lists.
	gates    []engine.GateView
	gatesErr error
}

func (f *fakeConcReader) ClosedRuns(_ context.Context, runs []string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, "describe")
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	var out []string
	for _, r := range runs {
		if f.closed[r] {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeConcReader) Gates(_ context.Context, _ string, pageSize int, token []byte) ([]engine.GateView, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gatesErr != nil {
		return nil, nil, f.gatesErr
	}
	start := 0
	if len(token) > 0 {
		start = int(token[0])
	}
	end := min(start+pageSize, len(f.gates))
	var next []byte
	if end < len(f.gates) {
		next = []byte{byte(end)}
	}
	return f.gates[start:end], next, nil
}

func (f *fakeConcReader) Gate(_ context.Context, gateID string) (engine.GateView, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gatesErr != nil {
		return engine.GateView{}, false, f.gatesErr
	}
	for _, g := range f.gates {
		if g.GateID == gateID {
			return g, true, nil
		}
	}
	return engine.GateView{}, false, nil
}

func (f *fakeConcReader) Verify(_ context.Context, gateID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verified = append(f.verified, gateID)
	return nil
}

func (f *fakeConcReader) ActiveGates(context.Context, string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, "active")
	return f.active, f.activeErr
}

func (f *fakeConcReader) QueuedRuns(context.Context, string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, "queued")
	return f.queued, f.queuedErr
}

func (f *fakeConcReader) GateLine(ctx context.Context, gateID string) (engine.GateLine, error) {
	f.mu.Lock()
	f.asked = append(f.asked, "line")
	hang := f.hang
	f.mu.Unlock()
	if hang {
		<-ctx.Done()
		return engine.GateLine{}, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lines[gateID], f.lineErr
}

func (f *fakeConcReader) OpenGates(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open, f.openErr
}

func (f *fakeConcReader) questions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...)
}

func concService(t *testing.T, starter engine.RunStarter, caps engine.CapabilityChecker, claimer api.IdempotencyClaimer) *api.Service {
	t.Helper()
	return concServiceWith(t, starter, caps, claimer, &fakeConcReader{})
}

func concServiceWith(t *testing.T, starter engine.RunStarter, caps engine.CapabilityChecker, claimer api.IdempotencyClaimer,
	reader engine.ConcurrencyAdmissionReader) *api.Service {
	t.Helper()
	cfg := api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, Capabilities: caps,
		Runs: engine.NewRunStore(), Bus: engine.NewEventBus(), ConcurrencyReader: reader}
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
			createAsked := caps.questions() // create's warning asks once too
			must.OKf(t, submitEnv(svc, "migrate", tc.env, ""), "submit")
			plan := starter.plans[0]
			// What a resume asks before it lets a run run again (E85): a plan
			// as submit really pins it, so a stubbed predicate fails here.
			if !engine.PlanHasConcurrency(plan) {
				t.Fatal("PlanHasConcurrency = false for a plan submit gave a limit")
			}
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
			if asked := caps.asked[createAsked:]; len(asked) != 1 || asked[0] != capabilityQuestion(engine.CapabilityConcurrency) {
				t.Fatalf("submit asked %v, want %s once", asked, engine.CapabilityConcurrency)
			}
		})
	}

	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	caps := &fakeCapabilities{}
	svc := concService(t, starter, caps, nil)
	must.OK(t, applyDoc(t, svc, untimedInnerYAML))
	must.OKf(t, submitName(svc, "inner"), "submit")
	if plan := starter.plans[0]; plan.Concurrency != nil || engine.PlanHasConcurrency(plan) || plan.History.Reserve != engine.DefaultHistoryReserve || caps.questions() != 0 {
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
			wantCode: connect.CodeUnavailable, wantText: []string{"CONCURRENCY_UNSUPPORTED", "cause=fleet", "1 engine process runs"}},
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
	if err := submitEnv(svc, "migrate", "staging", "order-43"); connect.CodeOf(err) != connect.CodeUnavailable {
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
		Capabilities: &fakeCapabilities{}, ConcurrencyReader: &fakeConcReader{}})
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
	if err := submitEnv(svc, "migrate", "", "order-7"); connect.CodeOf(err) != connect.CodeUnavailable {
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
		Concurrency: engine.ConcurrencySettings{MaxQueueTimeout: time.Hour}, ConcurrencyReader: &fakeConcReader{}})
	must.OK(t, applyDoc(t, svc, limitedYAML(templatedLimit)))
	must.OKf(t, submitName(svc, "migrate"), "submit")
	if c := starter.plans[0].Concurrency; c.RequeueInterval != engine.DefaultConcurrencySettings().RequeueInterval ||
		c.Slots[0].QueueTimeoutSeconds != int64(engine.DefaultConcurrencySettings().DefaultQueueTimeout/time.Second) {
		t.Fatalf("plan Concurrency = %+v, want the defaults", c)
	}
}

// TestSubmitRun_TheKeyAndLineChecks are §7.1 steps 5-7: a run that would
// queue is refused when its line, or its namespace's queued runs, are at
// their bound; a new key is refused when the namespace's active keys are; a
// read that cannot be made refuses CONCURRENCY_CHECK_UNAVAILABLE. A run
// that would hold its slot at once asks no line bound.
func TestSubmitRun_TheKeyAndLineChecks(t *testing.T) {
	gate := engine.GateID("default", "", "db:prod")
	small := engine.DefaultGateTunables()
	small.MaxEntries = 100
	for _, tc := range []struct {
		name      string
		block     string
		reader    *fakeConcReader
		gate      engine.GateTunables
		wantCode  connect.Code
		wantText  []string
		wantAsked []string
	}{
		{name: "a free slot asks no line bound", block: templatedLimit,
			reader:    &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 1, Queued: 0}}},
			wantAsked: []string{"line"}},
		{name: "a line under its bound", block: templatedLimit + "\n  max_queued: 3",
			reader:    &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2, Queued: 2}}},
			wantAsked: []string{"line", "queued"}},
		{name: "a line at max_queued", block: templatedLimit + "\n  max_queued: 3",
			reader:   &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2, Queued: 3}}},
			wantCode: connect.CodeResourceExhausted, wantText: []string{"CONCURRENCY_QUEUE_FULL", "cause=max_queued", "db:prod"}},
		{name: "a line at max_entries", block: templatedLimit, gate: small,
			reader:   &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2, Queued: 98}}},
			wantCode: connect.CodeResourceExhausted, wantText: []string{"CONCURRENCY_QUEUE_FULL", "cause=max_entries"}},
		{name: "the namespace's queued runs at their bound", block: templatedLimit,
			reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2}},
				queued: int64(engine.DefaultConcurrencySettings().MaxQueuedRunsPerNamespace)},
			wantCode: connect.CodeResourceExhausted, wantText: []string{"CONCURRENCY_QUEUE_FULL", "cause=namespace"}},
		{name: "a new key at the active bound", block: templatedLimit,
			reader:   &fakeConcReader{active: int64(engine.DefaultConcurrencySettings().MaxActiveKeysPerNamespace)},
			wantCode: connect.CodeResourceExhausted, wantText: []string{"CONCURRENCY_KEYS_EXHAUSTED"}},
		{name: "an empty lingering gate at the active bound", block: templatedLimit,
			reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true}},
				active: int64(engine.DefaultConcurrencySettings().MaxActiveKeysPerNamespace)},
			wantCode: connect.CodeResourceExhausted, wantText: []string{"CONCURRENCY_KEYS_EXHAUSTED"}},
		{name: "a held key at the active bound", block: templatedLimit,
			reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 1}},
				active: int64(engine.DefaultConcurrencySettings().MaxActiveKeysPerNamespace)},
			wantAsked: []string{"line"}},
		{name: "the line cannot be read", block: templatedLimit, reader: &fakeConcReader{lineErr: errors.New("query failed")},
			wantCode: connect.CodeUnavailable, wantText: []string{"CONCURRENCY_CHECK_UNAVAILABLE", "scope=key"}},
		{name: "the active keys cannot be counted", block: templatedLimit, reader: &fakeConcReader{activeErr: errors.New("count failed")},
			wantCode: connect.CodeUnavailable, wantText: []string{"CONCURRENCY_CHECK_UNAVAILABLE", "scope=namespace"}},
		{name: "the queued runs cannot be counted", block: templatedLimit,
			reader:   &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2}}, queuedErr: errors.New("count failed")},
			wantCode: connect.CodeUnavailable, wantText: []string{"CONCURRENCY_CHECK_UNAVAILABLE", "scope=namespace"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
			cfg := api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, Capabilities: &fakeCapabilities{},
				ConcurrencyReader: tc.reader, GateTunables: tc.gate}
			svc := api.NewService(cfg)
			must.OK(t, applyDoc(t, svc, limitedYAML(tc.block)))
			err := submitName(svc, "migrate")
			if tc.wantCode == 0 {
				must.OKf(t, err, "submit")
			} else if connect.CodeOf(err) != tc.wantCode {
				t.Fatalf("submit = %v; want %v", err, tc.wantCode)
			}
			for _, text := range tc.wantText {
				if !strings.Contains(err.Error(), text) {
					t.Fatalf("submit = %v; want it to name %q", err, text)
				}
			}
			if tc.wantCode != 0 && len(starter.startedRuns()) != 0 {
				t.Fatal("a refused submit started a run")
			}
			if tc.wantAsked != nil {
				if got := tc.reader.questions(); strings.Join(got, ",") != strings.Join(tc.wantAsked, ",") {
					t.Fatalf("asked %v, want %v", got, tc.wantAsked)
				}
			}
		})
	}
}

// TestSubmitRun_RefuseAndSkipAtAFullKey: a refuse or skip limit refuses a
// full key CONCURRENCY_LIMIT_REACHED before a run id, naming the runs that
// hold it and any that are stalled; a holder the cluster reports ended
// counts as free, and the gate is asked to check its holders. A key under
// its limit is accepted, a rebuilding gate cannot answer, and neither
// strategy asks a line bound. The plan pins the declared strategy.
func TestSubmitRun_RefuseAndSkipAtAFullKey(t *testing.T) {
	gate := engine.GateID("default", "", "db:prod")
	full := engine.GateLine{Present: true, Holders: 2, Queued: 0, HolderRuns: []string{"ork-a", "ork-b"}}
	stalled := full
	stalled.StalledRuns = []string{"ork-b"}
	rebuilding := engine.GateLine{Present: true, Rebuilding: true}
	for _, strategy := range []string{"refuse", "skip"} {
		block := templatedLimit + "\n  on_limit: " + strategy
		for _, tc := range []struct {
			name       string
			reader     *fakeConcReader
			wantCode   connect.Code
			wantText   []string
			wantVerify bool
			wantAsked  []string
		}{
			{name: "a free slot", reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 1}}},
				wantAsked: []string{"line"}},
			{name: "a new key", reader: &fakeConcReader{}, wantAsked: []string{"line", "active"}},
			{name: "a full key", reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: full}},
				wantCode:  connect.CodeFailedPrecondition,
				wantText:  []string{"CONCURRENCY_LIMIT_REACHED", "key db:prod is at its limit (2 of 2 running: ork-a, ork-b)"},
				wantAsked: []string{"line", "describe"}},
			{name: "a full key with a stalled holder", reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: stalled}},
				wantCode: connect.CodeFailedPrecondition,
				wantText: []string{"CONCURRENCY_LIMIT_REACHED", "run ork-b is not making progress; an operator must end it"}},
			{name: "a holder that has ended", reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: full},
				closed: map[string]bool{"ork-a": true}}, wantVerify: true, wantAsked: []string{"line", "describe"}},
			{name: "holders that cannot be described", reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: full},
				describeErr: errors.New("describe failed")},
				wantCode: connect.CodeUnavailable, wantText: []string{"CONCURRENCY_CHECK_UNAVAILABLE", "scope=key", "describe failed"}},
			{name: "a rebuilding gate", reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: rebuilding}},
				wantCode: connect.CodeUnavailable, wantText: []string{"CONCURRENCY_CHECK_UNAVAILABLE", "scope=key", "rebuilding"}},
		} {
			t.Run(strategy+"/"+tc.name, func(t *testing.T) {
				starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
				cfg := api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, Capabilities: &fakeCapabilities{},
					ConcurrencyReader: tc.reader}
				svc := api.NewService(cfg)
				must.OK(t, applyDoc(t, svc, limitedYAML(block)))
				err := submitName(svc, "migrate")
				if tc.wantCode == 0 {
					must.OKf(t, err, "submit")
					if got := starter.plans[0].Concurrency.Slots[0].Strategy; got != strategy {
						t.Fatalf("plan strategy = %q, want %q", got, strategy)
					}
				} else if connect.CodeOf(err) != tc.wantCode {
					t.Fatalf("submit = %v; want %v", err, tc.wantCode)
				}
				for _, text := range tc.wantText {
					if !strings.Contains(err.Error(), text) {
						t.Fatalf("submit = %v; want it to name %q", err, text)
					}
				}
				if tc.wantCode != 0 && len(starter.startedRuns()) != 0 {
					t.Fatal("a refused submit started a run")
				}
				if got := len(tc.reader.verified) > 0; got != tc.wantVerify {
					t.Fatalf("verify sent = %v, want %v", got, tc.wantVerify)
				}
				if tc.wantAsked != nil {
					if got := tc.reader.questions(); strings.Join(got, ",") != strings.Join(tc.wantAsked, ",") {
						t.Fatalf("asked %v, want %v", got, tc.wantAsked)
					}
				}
			})
		}
	}
}

// TestSubmitRun_AKeyedRetrySkipsTheLineChecks: a bound key's retry returns
// its run however full the line has become.
func TestSubmitRun_AKeyedRetrySkipsTheLineChecks(t *testing.T) {
	gate := engine.GateID("default", "", "db:staging")
	reader := &fakeConcReader{lines: map[string]engine.GateLine{}}
	starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: true}
	svc := concServiceWith(t, starter, &fakeCapabilities{}, newSharedClaimer(), reader)
	must.OK(t, applyDoc(t, svc, limitedYAML(templatedLimit+"\n  max_queued: 1")))
	must.OKf(t, submitEnv(svc, "migrate", "staging", "order-9"), "first submit")
	reader.mu.Lock()
	reader.lines[gate] = engine.GateLine{Present: true, Holders: 2, Queued: 1}
	reader.mu.Unlock()
	must.OKf(t, submitEnv(svc, "migrate", "staging", "order-9"), "keyed retry")
	if err := submitEnv(svc, "migrate", "staging", "order-10"); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("a new key = %v; want the line's refusal", err)
	}
}

// TestSubmitRun_MaxRunningNamesTheQueuedRuns: a max_running refusal counts
// the namespace's queued runs when the cluster indexes them, and is then
// ResourceExhausted with the count; a count that fails is ResourceExhausted
// only for an Orkflow that declares a limit; otherwise the refusal is
// exactly today's.
func TestSubmitRun_MaxRunningNamesTheQueuedRuns(t *testing.T) {
	for _, tc := range []struct {
		name     string
		doc      string
		indexed  bool
		reader   *fakeConcReader
		wantCode connect.Code
		queued   bool
	}{
		{name: "queued runs fill it", doc: untimedInnerYAML, indexed: true, reader: &fakeConcReader{queued: 2},
			wantCode: connect.CodeResourceExhausted, queued: true},
		{name: "none queued", doc: untimedInnerYAML, indexed: true, reader: &fakeConcReader{},
			wantCode: connect.CodeFailedPrecondition},
		{name: "the count fails, no limit declared", doc: untimedInnerYAML, indexed: true,
			reader: &fakeConcReader{queuedErr: errors.New("count failed")}, wantCode: connect.CodeFailedPrecondition},
		{name: "the count fails, a limit declared", doc: limitedYAML(templatedLimit), indexed: true,
			reader: &fakeConcReader{queuedErr: errors.New("count failed")}, wantCode: connect.CodeResourceExhausted},
		{name: "unindexed, no count", doc: untimedInnerYAML, reader: &fakeConcReader{queued: 2},
			wantCode: connect.CodeFailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := newFakeRegistry(&orkstackv1.Namespace{Name: "payments", MaxRunning: 2, CreatedAt: timestamppb.Now()})
			runs := &fakeReader{runs: map[string]*orkstackv1.Run{}, executions: map[string]engine.Executions{
				"payments": {Running: []*orkstackv1.Run{{}, {}}}}}
			starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: tc.indexed}
			svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: registry, Reader: runs,
				Starter: starter, Capabilities: &fakeCapabilities{}, ConcurrencyReader: tc.reader})
			doc := strings.Replace(tc.doc, "version: \"1.0.0\"\n", "version: \"1.0.0\"\nnamespace: payments\n", 1)
			must.OK(t, applyDoc(t, svc, doc))
			name := "inner"
			if strings.Contains(tc.doc, "name: migrate") {
				name = "migrate"
			}
			_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name, Namespace: "payments"}))
			if connect.CodeOf(err) != tc.wantCode || !strings.Contains(err.Error(), `namespace "payments" already has 2 runs in flight`) {
				t.Fatalf("submit = %v; want %v, the max_running refusal", err, tc.wantCode)
			}
			if !tc.queued && !strings.Contains(err.Error(), "2 runs in flight, which is its max_running") {
				t.Fatalf("submit = %v; want today's text exactly", err)
			}
			var refusal *engine.AdmissionRefusedError
			if !errors.As(err, &refusal) || refusal.Reason != engine.AdmissionMaxRunning {
				t.Fatalf("submit = %v; want the admission refusal reachable through it", err)
			}
			if got := strings.Contains(err.Error(), "2 of them queued on concurrency limits"); got != tc.queued {
				t.Fatalf("submit = %v; the queued clause present = %v, want %v", err, got, tc.queued)
			}
		})
	}
}

// TestTriggerInvoke_AFullLineIsTooManyRequests: on the trigger's plain HTTP
// route a concurrency refusal that says "full, send again later" is 429 with
// Retry-After (engine.concurrency.retry_after); a fleet mid-rollout is 503;
// a deployment that does not index the limit is 412.
var defaultRetryAfter = fmt.Sprint(int64(engine.DefaultConcurrencySettings().RetryAfter / time.Second))

func TestTriggerInvoke_AFullLineIsTooManyRequests(t *testing.T) {
	gate := engine.GateID("default", "", "db:prod")
	for _, tc := range []struct {
		name       string
		indexed    bool
		caps       *fakeCapabilities
		reader     *fakeConcReader
		wantStatus int
		wantRetry  string
	}{
		{name: "a full line", indexed: true, caps: &fakeCapabilities{},
			reader:     &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2, Queued: 3}}},
			wantStatus: http.StatusTooManyRequests, wantRetry: defaultRetryAfter},
		{name: "keys exhausted", indexed: true, caps: &fakeCapabilities{},
			reader:     &fakeConcReader{active: int64(engine.DefaultConcurrencySettings().MaxActiveKeysPerNamespace)},
			wantStatus: http.StatusTooManyRequests, wantRetry: defaultRetryAfter},
		{name: "a fleet mid-rollout", indexed: true, caps: &fakeCapabilities{lacking: []string{"a"}},
			reader: &fakeConcReader{}, wantStatus: http.StatusServiceUnavailable},
		{name: "unindexed", caps: &fakeCapabilities{}, reader: &fakeConcReader{}, wantStatus: http.StatusPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixtureWith(t, func(cfg *api.Config) {
				cfg.Starter = &concStarter{fakeStarter: cfg.Starter.(*fakeStarter), indexed: tc.indexed}
				cfg.Capabilities = tc.caps
				cfg.ConcurrencyReader = tc.reader
			})
			_, err := f.orkflowClient().ApplyOrkflow(context.Background(),
				connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: limitedYAML(templatedLimit + "\n  max_queued: 3")}))
			must.OK(t, err)
			trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "migrate"})
			status, body, header := f.invokeWithHeaders(t, trigger.GetId(), bearer(secret), "")
			if status != tc.wantStatus || header.Get("Retry-After") != tc.wantRetry {
				t.Fatalf("invoke = %d, Retry-After %q: %s; want %d, %q", status, header.Get("Retry-After"), body,
					tc.wantStatus, tc.wantRetry)
			}
			if len(f.starter.startedRuns()) != 0 {
				t.Fatal("a refused invoke started a run")
			}
		})
	}
}

// TestTriggerInvoke_ARefuseLimitAnswersByItsClass: a refuse limit's full key
// is 412 with no Retry-After, a gate that cannot answer is 503, and a key
// with a free slot starts the run.
func TestTriggerInvoke_ARefuseLimitAnswersByItsClass(t *testing.T) {
	gate := engine.GateID("default", "", "db:prod")
	for _, tc := range []struct {
		name       string
		line       engine.GateLine
		wantStatus int
	}{
		{name: "a full key", line: engine.GateLine{Present: true, Holders: 2, HolderRuns: []string{"ork-a", "ork-b"}},
			wantStatus: http.StatusPreconditionFailed},
		{name: "a rebuilding gate", line: engine.GateLine{Present: true, Rebuilding: true},
			wantStatus: http.StatusServiceUnavailable},
		{name: "a free slot", line: engine.GateLine{Present: true, Holders: 1}, wantStatus: http.StatusCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeConcReader{lines: map[string]engine.GateLine{gate: tc.line}}
			f := newFixtureWith(t, func(cfg *api.Config) {
				cfg.Starter = &concStarter{fakeStarter: cfg.Starter.(*fakeStarter), indexed: true}
				cfg.Capabilities = &fakeCapabilities{}
				cfg.ConcurrencyReader = reader
			})
			_, err := f.orkflowClient().ApplyOrkflow(context.Background(),
				connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: limitedYAML(templatedLimit + "\n  on_limit: refuse")}))
			must.OK(t, err)
			trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "migrate"})
			status, body, header := f.invokeWithHeaders(t, trigger.GetId(), bearer(secret), "")
			if status != tc.wantStatus || header.Get("Retry-After") != "" {
				t.Fatalf("invoke = %d, Retry-After %q: %s; want %d and none", status, header.Get("Retry-After"), body, tc.wantStatus)
			}
		})
	}
}

// TestApplyOrkflow_WarnsOfALimitTheDeploymentCannotHold: create stores an
// Orkflow with a limit the deployment could not hold now, and says so; a
// deployment that can hold it warns of nothing.
func TestApplyOrkflow_WarnsOfALimitTheDeploymentCannotHold(t *testing.T) {
	for _, tc := range []struct {
		name    string
		indexed bool
		caps    *fakeCapabilities
		want    []string
	}{
		{name: "can hold it", indexed: true, caps: &fakeCapabilities{}},
		{name: "unindexed", caps: &fakeCapabilities{}, want: []string{"CONCURRENCY_UNSUPPORTED", "cause=index"}},
		{name: "an older process", indexed: true, caps: &fakeCapabilities{lacking: []string{"a"}},
			want: []string{"CONCURRENCY_UNSUPPORTED", "cause=fleet"}},
		{name: "the fleet cannot be read", indexed: true, caps: &fakeCapabilities{err: errors.New("describe failed")},
			want: []string{"CONCURRENCY_CHECK_UNAVAILABLE"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: tc.indexed}
			svc := concService(t, starter, tc.caps, nil)
			resp, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: limitedYAML(templatedLimit)}))
			must.OKf(t, err, "create")
			warnings := strings.Join(resp.Msg.GetWarnings(), "\n")
			if len(tc.want) == 0 && strings.Contains(warnings, "CONCURRENCY_") {
				t.Fatalf("warnings = %q, want none about the limit", warnings)
			}
			for _, text := range tc.want {
				if !strings.Contains(warnings, text) {
					t.Fatalf("warnings = %q, want them to name %q", warnings, text)
				}
			}
		})
	}
	// An Orkflow with no limit asks the fleet nothing at create.
	caps := &fakeCapabilities{}
	svc := concService(t, &concStarter{fakeStarter: &fakeStarter{}}, caps, nil)
	must.OK(t, applyDoc(t, svc, untimedInnerYAML))
	if caps.questions() != 0 {
		t.Fatalf("create asked the fleet %d times for an Orkflow with no limit", caps.questions())
	}
}

// TestPrepareScheduledRun_AConcurrencyRefusalStartsNoRun: a fire's
// preparation that a limit refuses says which outcome it is: a skip
// limit's full key is skipped, a full line is not started, and a check that
// cannot be made is not started after the retry window it pins; a refuse
// limit's full key stays a refusal.
func TestPrepareScheduledRun_AConcurrencyRefusalStartsNoRun(t *testing.T) {
	gate := engine.GateID("default", "", "db:prod")
	full := engine.GateLine{Present: true, Holders: 2, HolderRuns: []string{"ork-a", "ork-b"}}
	for _, tc := range []struct {
		name, block  string
		unindexed    bool
		reader       *fakeConcReader
		wantRefused  bool
		wantCode     string
		wantText     string
		wantRetryFor time.Duration
	}{
		{name: "skip at a full key", block: templatedLimit + "\n  on_limit: skip",
			reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: full}}, wantCode: engine.FireSkippedConcurrencyCode,
			wantText: "key db:prod was full; this fire started no run"},
		{name: "refuse at a full key", block: templatedLimit + "\n  on_limit: refuse",
			reader: &fakeConcReader{lines: map[string]engine.GateLine{gate: full}}, wantRefused: true},
		{name: "a full line", block: templatedLimit + "\n  max_queued: 1",
			reader:   &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2, Queued: 1}}},
			wantCode: engine.FireNotStartedConcurrencyCode, wantText: "this fire started no run: CONCURRENCY_QUEUE_FULL"},
		{name: "an unindexed cluster", block: templatedLimit, unindexed: true,
			reader: &fakeConcReader{}, wantRefused: true},
		{name: "a line that cannot be read", block: templatedLimit,
			reader:   &fakeConcReader{lineErr: errors.New("query failed")},
			wantCode: engine.FireConcurrencyUnavailableCode, wantText: "CONCURRENCY_CHECK_UNAVAILABLE",
			wantRetryFor: engine.DefaultConcurrencySettings().FireUnavailableRetry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			starter := &concStarter{fakeStarter: &fakeStarter{}, indexed: !tc.unindexed}
			svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter,
				Capabilities: &fakeCapabilities{}, ConcurrencyReader: tc.reader})
			must.OK(t, applyDoc(t, svc, limitedYAML(tc.block)))
			rec := &orkstackv1.ScheduleRecord{ScheduleId: "sch-1", Name: "nightly", OrkflowName: "migrate"}
			_, err := api.NewScheduleActivities(svc, nil).PrepareScheduledRun(context.Background(), rec, time.Now())
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || !app.NonRetryable() {
				t.Fatalf("prepare = %v; want a non-retryable application error", err)
			}
			if tc.wantRefused {
				if app.Type() != engine.ScheduleFireRefusedType {
					t.Fatalf("prepare type %q, want %s", app.Type(), engine.ScheduleFireRefusedType)
				}
				return
			}
			var got engine.FireNotStarted
			if app.Type() != engine.ScheduleFireNotStartedType || app.Details(&got) != nil {
				t.Fatalf("prepare = %v (type %q); want %s with its details", err, app.Type(), engine.ScheduleFireNotStartedType)
			}
			if got.Code != tc.wantCode || !strings.Contains(got.Message, tc.wantText) || got.RetryFor != tc.wantRetryFor {
				t.Fatalf("not started = %+v; want code %s, text %q, retry %s", got, tc.wantCode, tc.wantText, tc.wantRetryFor)
			}
		})
	}
}
