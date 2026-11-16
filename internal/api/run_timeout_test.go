package api_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const timedInnerYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: inner
version: "1.0.0"
timeout: 30m
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
`

const untimedInnerYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: inner
version: "1.0.0"
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
`

// outerYAML composes inner; timeout is its own top-level `timeout:`, or none.
func outerYAML(timeout string) string {
	doc := `apiVersion: orkstack/v1
kind: Orkflow
name: outer
version: "1.0.0"
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
    uses: orkflow:inner@1.0.0
    needs: [build]
`
	if timeout != "" {
		doc = strings.Replace(doc, "\ntasks:", "\ntimeout: "+timeout+"\ntasks:", 1)
	}
	return doc
}

func timeoutService(t *testing.T, starter *fakeStarter, caps engine.CapabilityChecker, grace time.Duration) *api.Service {
	t.Helper()
	return api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter,
		Capabilities: caps, RunTimeoutHandlerGrace: grace})
}

// TestSubmitRun_PinsTheRunTimeoutPlanForEveryRunThatDeclaresOne: each run of
// a tree whose definition declares `timeout:` carries the deployment's grace
// and the resolution bound in its plan, a child run in its own; a run whose
// definition declares none carries no plan for it, so its plan's bytes are
// unchanged.
func TestSubmitRun_PinsTheRunTimeoutPlanForEveryRunThatDeclaresOne(t *testing.T) {
	want := &engine.RunTimeoutPlan{HandlerGraceSeconds: 420, ResolutionsPerPass: engine.DefaultResolutionsPerPass}
	cases := []struct {
		name               string
		inner, outer       string
		wantOuter, wantKid bool
	}{
		{name: "both declare one", inner: timedInnerYAML, outer: outerYAML("2h"), wantOuter: true, wantKid: true},
		{name: "only the child", inner: timedInnerYAML, outer: outerYAML(""), wantKid: true},
		{name: "only the parent", inner: untimedInnerYAML, outer: outerYAML("2h"), wantOuter: true},
		{name: "neither", inner: untimedInnerYAML, outer: outerYAML("")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			starter := &fakeStarter{}
			svc := timeoutService(t, starter, &fakeCapabilities{}, 7*time.Minute)
			must.OK(t, applyDoc(t, svc, c.inner))
			must.OK(t, applyDoc(t, svc, c.outer))
			must.OKf(t, submitName(svc, "outer"), "submit")
			plan := starter.plans[0]
			check := func(who string, got *engine.RunTimeoutPlan, declared bool) {
				t.Helper()
				switch {
				case declared && (got == nil || *got != *want):
					t.Fatalf("%s plan RunTimeout = %+v, want %+v", who, got, want)
				case !declared && got != nil:
					t.Fatalf("%s plan RunTimeout = %+v, want none", who, got)
				}
			}
			check("parent", plan.RunTimeout, c.wantOuter)
			check("child", plan.Children["release"].Plan.RunTimeout, c.wantKid)
		})
	}
}

// TestSubmitRun_ZeroGraceIsAGraceOfZero: a deployment's grace of 0s is pinned
// as 0, never replaced by the default.
func TestSubmitRun_ZeroGraceIsAGraceOfZero(t *testing.T) {
	starter := &fakeStarter{}
	svc := timeoutService(t, starter, &fakeCapabilities{}, 0)
	must.OK(t, applyDoc(t, svc, untimedInnerYAML))
	must.OK(t, applyDoc(t, svc, outerYAML("2h")))
	must.OKf(t, submitName(svc, "outer"), "submit")
	if got := starter.plans[0].RunTimeout; got == nil || got.HandlerGraceSeconds != 0 {
		t.Fatalf("plan RunTimeout = %+v, want a grace of 0", got)
	}
}

// TestSubmitRun_FleetCheckForARunTimeout: a run tree declaring `timeout:`
// anywhere is refused while an engine process that could decide it cannot
// enforce one, and when that cannot be known; a tree declaring none asks
// nothing.
func TestSubmitRun_FleetCheckForARunTimeout(t *testing.T) {
	cases := []struct {
		name      string
		inner     string
		outer     string
		caps      *fakeCapabilities
		wantCode  connect.Code
		wantText  string
		wantAsked int
	}{
		{name: "every process has it", inner: untimedInnerYAML, outer: outerYAML("2h"),
			caps: &fakeCapabilities{}, wantAsked: 1},
		{name: "a process lacks it", inner: untimedInnerYAML, outer: outerYAML("2h"),
			caps: &fakeCapabilities{lacking: []string{"1@h@ ork-caps="}}, wantAsked: 1,
			wantCode: connect.CodeFailedPrecondition,
			wantText: "orkflow outer@1.0.0 declares a run timeout; 1 engine process runs a release that cannot enforce a run timeout; finish the rollout first"},
		{name: "only the child declares one", inner: timedInnerYAML, outer: outerYAML(""),
			caps: &fakeCapabilities{lacking: []string{"a", "b"}}, wantAsked: 1,
			wantCode: connect.CodeFailedPrecondition,
			wantText: "orkflow inner@1.0.0 declares a run timeout; 2 engine processes run a release that cannot enforce a run timeout"},
		{name: "the list cannot be read", inner: untimedInnerYAML, outer: outerYAML("2h"),
			caps: &fakeCapabilities{err: errors.New("describe failed")}, wantAsked: 1,
			wantCode: connect.CodeUnavailable,
			wantText: "whether every engine process can enforce a run timeout is unknown; try again"},
		{name: "no timeout declared", inner: untimedInnerYAML, outer: outerYAML(""),
			caps: &fakeCapabilities{lacking: []string{"a"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			starter := &fakeStarter{}
			svc := timeoutService(t, starter, c.caps, time.Minute)
			must.OK(t, applyDoc(t, svc, c.inner))
			must.OK(t, applyDoc(t, svc, c.outer))
			err := submitName(svc, "outer")
			if c.wantCode == 0 {
				must.OKf(t, err, "submit")
			} else if connect.CodeOf(err) != c.wantCode || !strings.Contains(err.Error(), c.wantText) {
				t.Fatalf("submit = %v; want %s containing %q", err, c.wantCode, c.wantText)
			}
			if c.wantCode != 0 && len(starter.startedRuns()) != 0 {
				t.Fatal("a refused submit started a run")
			}
			if got := c.caps.questions(); got != c.wantAsked {
				t.Fatalf("fleet questions = %d, want %d", got, c.wantAsked)
			}
			if c.wantAsked > 0 && c.caps.asked[0] != capabilityQuestion(engine.CapabilityRunTimeout) {
				t.Fatalf("asked %v, want %s", c.caps.asked, engine.CapabilityRunTimeout)
			}
		})
	}
}

func capabilityQuestion(capability string) string {
	return capability + "/0"
}

// TestSubmitRun_NoClusterRefusesARunTimeout: an engine not wired to a cluster
// cannot answer the fleet check, so it refuses a run that needs one.
func TestSubmitRun_NoClusterRefusesARunTimeout(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: &fakeStarter{}})
	must.OK(t, applyDoc(t, svc, untimedInnerYAML))
	must.OK(t, applyDoc(t, svc, outerYAML("2h")))
	err := submitName(svc, "outer")
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "not wired to a cluster") {
		t.Fatalf("submit = %v; want unavailable, not wired to a cluster", err)
	}
}
