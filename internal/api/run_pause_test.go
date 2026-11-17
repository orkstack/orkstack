package api_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestSubmitRun_PinsThePausePlanOnEveryRun: every run, and every composed
// child in its own plan, carries the deployment's pause timeouts and the
// reserves, so each can be paused by the release that started it; an
// api.Config without them pins the engine's defaults.
func TestSubmitRun_PinsThePausePlanOnEveryRun(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		timeout, max         time.Duration
		wantDefault, wantMax int64
	}{
		{name: "configured", timeout: time.Hour, max: 48 * time.Hour, wantDefault: 3600, wantMax: 48 * 3600},
		{name: "unset", wantDefault: int64(engine.DefaultRunPauseTimeout / time.Second),
			wantMax: int64(engine.DefaultRunPauseTimeoutMax / time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			starter := &fakeStarter{}
			svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter,
				Capabilities: &fakeCapabilities{}, RunPauseTimeout: tc.timeout, RunPauseTimeoutMax: tc.max})
			must.OK(t, applyDoc(t, svc, untimedInnerYAML))
			must.OK(t, applyDoc(t, svc, outerYAML("")))
			must.OKf(t, submitName(svc, "outer"), "submit")
			plan := starter.plans[0]
			want := engine.DefaultPausePlan(tc.wantDefault, tc.wantMax)
			for who, got := range map[string]*engine.PausePlan{"parent": plan.Pause, "child": plan.Children["release"].Plan.Pause} {
				if got == nil || *got != want {
					t.Fatalf("%s plan Pause = %+v, want %+v", who, got, want)
				}
			}
		})
	}
}

// TestSubmitRun_PinsIndexedOnlyWhereTheClusterIndexes: a run, and every
// composed child, is pinned Indexed exactly when boot found the paused
// attribute usable (RunPauseIndexed), so no run ever writes an attribute
// the cluster would refuse.
func TestSubmitRun_PinsIndexedOnlyWhereTheClusterIndexes(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
			starter := &fakeStarter{}
			svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter,
				Capabilities: &fakeCapabilities{}, RunPauseIndexed: indexed})
			must.OK(t, applyDoc(t, svc, untimedInnerYAML))
			must.OK(t, applyDoc(t, svc, outerYAML("")))
			must.OKf(t, submitName(svc, "outer"), "submit")
			plan := starter.plans[0]
			for who, got := range map[string]*engine.PausePlan{"parent": plan.Pause, "child": plan.Children["release"].Plan.Pause} {
				if got == nil || got.Indexed != indexed {
					t.Fatalf("%s plan Pause = %+v, want Indexed %v", who, got, indexed)
				}
			}
		})
	}
}
