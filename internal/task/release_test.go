package task_test

import (
	"testing"

	"github.com/orkstack/orkstack/internal/task"
)

// The release view (docs/DEPLOY.md §3): the one reverse translation it needs, beside
// PoolForQueue, and the one spelling of a release in the run index.

func TestPoolForChannel_NamesTheDefaultRouteByItsPool(t *testing.T) {
	cases := []struct {
		name     string
		def      task.Route
		queue    string
		wantPool string
		wantOK   bool
	}{
		{"default channel is main", task.Route{Pool: "main", Queue: "ork-tasks"}, "ork-tasks", "main", true},
		{"a pool channel is its pool", task.Route{Pool: "main", Queue: "ork-tasks"}, "ork-pool-gpu", "gpu", true},
		{"a raw channel that is not the default", task.Route{Pool: "main", Queue: "ork-tasks"}, "ork-tasks-agent", "", false},
		{"a pin-creation channel", task.Route{Pool: "main", Queue: "ork-tasks"}, "ork-pool-build__internal_session_creation", "", false},
		{"a raw ORK_ORKER_QUEUE default", task.Route{Pool: "main", Queue: "ork-tasks-agent"}, "ork-tasks-agent", "main", true},
		{"a pool channel bound as the default", task.Route{Pool: "gpu", Queue: "ork-pool-gpu"}, "ork-pool-gpu", "gpu", true},
		{"the presence channel", task.Route{Pool: "main", Queue: "ork-tasks"}, task.OrkersQueue, "", false},
		{"the engine's projection channel", task.Route{Pool: "main", Queue: "ork-tasks"}, task.EngineEventQueue, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool, ok := task.PoolForChannel(tc.queue, tc.def)
			if pool != tc.wantPool || ok != tc.wantOK {
				t.Fatalf("PoolForChannel(%q, %+v) = (%q, %v), want (%q, %v)",
					tc.queue, tc.def, pool, ok, tc.wantPool, tc.wantOK)
			}
		})
	}
}

func TestIndexedVersion_SpellsDeploymentColonBuild(t *testing.T) {
	got, err := task.IndexedVersion("orkstack", "2026.08.16-a1b2c3d")
	if err != nil {
		t.Fatalf("IndexedVersion: %v", err)
	}
	if want := "orkstack:2026.08.16-a1b2c3d"; got != want {
		t.Fatalf("IndexedVersion = %q, want %q", got, want)
	}
	for _, bad := range []struct{ name, build string }{
		{`or"kstack`, "v1"}, {`ork\stack`, "v1"}, {"ork\x01stack", "v1"},
		{"orkstack", `v"1`}, {"orkstack", `v\1`}, {"orkstack", "v\x011"},
		{"", "v1"}, {"orkstack", ""},
	} {
		if got, err := task.IndexedVersion(bad.name, bad.build); err == nil {
			t.Errorf("IndexedVersion(%q, %q) = %q, want an error", bad.name, bad.build, got)
		}
	}
	if task.SearchAttrDeploymentVersion != "TemporalWorkerDeploymentVersion" {
		t.Fatalf("SearchAttrDeploymentVersion = %q", task.SearchAttrDeploymentVersion)
	}
}
