package api_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// overlayService is a deployment with snapshot overlays on (or off), the
// fleet check answered by caps and the engine's log captured in logs.
func overlayService(t *testing.T, on bool, caps engine.CapabilityChecker, logs *bytes.Buffer) (*api.Service, *fakeStarter) {
	t.Helper()
	starter := &fakeStarter{}
	cfg := api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, SnapshotOverlays: on,
		Capabilities: caps}
	if logs != nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	}
	return api.NewService(cfg), starter
}

func lastPlan(t *testing.T, starter *fakeStarter) engine.RunPlan {
	t.Helper()
	starter.mu.Lock()
	defer starter.mu.Unlock()
	if len(starter.plans) == 0 {
		t.Fatal("no run was started")
	}
	return starter.plans[len(starter.plans)-1]
}

// TestSnapshotOverlay_OffByDefault: an unset engine.snapshot_overlays is off,
// and with it off no plan carries a policy, however large the run, and the
// fleet is never asked.
func TestSnapshotOverlay_OffByDefault(t *testing.T) {
	if engine.DefaultSnapshotOverlays {
		t.Fatal("engine.DefaultSnapshotOverlays is on; Stage 6 ships it off")
	}
	caps := &fakeCapabilities{}
	svc, starter := overlayService(t, false, caps, nil)
	if err := applyDoc(t, svc, fanOutDoc("wide", 900, 1)); err != nil {
		t.Fatal(err)
	}
	if err := submitName(svc, "wide"); err != nil {
		t.Fatal(err)
	}
	if plan := lastPlan(t, starter); plan.SnapshotOverlay != nil || caps.questions() != 0 {
		t.Fatalf("policy %+v after %d questions, want none and none", plan.SnapshotOverlay, caps.questions())
	}
}

// TestPinRunTreeLimits_SmallTreeSkipsTheOverlayCheck: with the flag on, a run
// tree none of whose runs declares more than 256 rows gets no policy and
// asks the fleet nothing; a small parent composing a 300-row child is asked
// about, once, on both channels (ScopeWorkflowAndEvents: the two describes),
// and gets the policy on its top-level plan, which every child start copies.
func TestPinRunTreeLimits_SmallTreeSkipsTheOverlayCheck(t *testing.T) {
	t.Run("every run at most 256 rows", func(t *testing.T) {
		caps := &fakeCapabilities{err: errors.New("never asked")}
		svc, starter := overlayService(t, true, caps, nil)
		// 1 task + 255 items = 256 rows: exactly the floor, not over it.
		if err := applyDoc(t, svc, fanOutDoc("at-floor", 255, 1)); err != nil {
			t.Fatal(err)
		}
		if err := submitName(svc, "at-floor"); err != nil {
			t.Fatal(err)
		}
		if plan := lastPlan(t, starter); plan.SnapshotOverlay != nil || caps.questions() != 0 {
			t.Fatalf("policy %+v after %d questions, want none and none", plan.SnapshotOverlay, caps.questions())
		}
	})
	t.Run("a small parent composing a 300-row child", func(t *testing.T) {
		caps := &fakeCapabilities{}
		f := newComposeFixture(t, func(cfg *api.Config) {
			cfg.SnapshotOverlays, cfg.Capabilities = true, caps
		})
		f.mustApply(t, strings.Replace(fanOutDoc("release-block", 299, 1), "name: release-block\n",
			"name: release-block\nversion: \"2.1.0\"\n", 1))
		f.mustApply(t, composeParentYAMLWithout(t))
		if _, err := f.submit("deploy"); err != nil {
			t.Fatal(err)
		}
		plans := f.plans()
		plan := plans[len(plans)-1]
		want := engine.DefaultOverlayPolicy()
		if plan.SnapshotOverlay == nil || *plan.SnapshotOverlay != want {
			t.Fatalf("policy %+v, want %+v", plan.SnapshotOverlay, want)
		}
		if caps.questions() != 1 || caps.asked[0] != engine.CapabilitySnapshotOverlay+"/1" {
			t.Fatalf("asked %v, want one %s question with ScopeWorkflowAndEvents", caps.asked, engine.CapabilitySnapshotOverlay)
		}
		if child := plan.Children["release"]; child == nil || child.Plan.SnapshotOverlay != nil {
			t.Fatal("the nested child plan carries the policy: it travels once per tree, on the top-level plan")
		}
	})
}

// TestSnapshotOverlay_OmittedWhileAnOlderEngineProcessPolls: a process
// without the capability — on the workflow channel or on the projection
// channel, which MissingCapability's ScopeWorkflowAndEvents covers — leaves
// the policy nil: the run starts, writing full snapshots, and the engine
// logs the identities, at most once a minute. An unreadable fleet does the
// same.
func TestSnapshotOverlay_OmittedWhileAnOlderEngineProcessPolls(t *testing.T) {
	for _, tc := range []struct {
		name string
		caps *fakeCapabilities
		want string
	}{
		{"an older process polls", &fakeCapabilities{lacking: []string{"77@old-engine@"}}, "77@old-engine@"},
		{"the fleet cannot be read", &fakeCapabilities{err: errors.New("describe: unavailable")}, "describe: unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			svc, starter := overlayService(t, true, tc.caps, &logs)
			if err := applyDoc(t, svc, fanOutDoc("wide", 900, 1)); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := submitName(svc, "wide"); err != nil {
					t.Fatalf("submit %d = %v; overlays never refuse a run", i, err)
				}
				if plan := lastPlan(t, starter); plan.SnapshotOverlay != nil {
					t.Fatalf("submit %d: policy %+v, want none", i, plan.SnapshotOverlay)
				}
			}
			if tc.caps.questions() != 2 || tc.caps.asked[0] != engine.CapabilitySnapshotOverlay+"/1" {
				t.Fatalf("asked %v", tc.caps.asked)
			}
			line := "new runs write full snapshots, not overlays"
			if got := strings.Count(logs.String(), line); got != 1 || !strings.Contains(logs.String(), tc.want) {
				t.Fatalf("logged %d times (want once a minute), log:\n%s", got, logs.String())
			}
		})
	}
}
