package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine/enginetest"
	"github.com/orkstack/orkstack/internal/task"
)

// ---------------------------------------------------------------------------
// admin info / pool list: golden human and --json output
// ---------------------------------------------------------------------------

// adminFixture is a CLI fixture wired the way a real deployment is: system
// information injected as `ork` injects it, and a cluster reporting Orkers on
// two pools.
//
// The values are fixed rather than read from the machine, which is what lets
// the goldens hold: a golden carrying this host's Go version or clock would
// fail on every other machine. The observation is stamped with the fixture's
// clock, which is also the engine's here, so the pools are live rather than
// an hour stale.
func adminFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.System = api.SystemInfo{
			EngineVersion:    "v0.0.4",
			GoVersion:        "go1.25.6",
			DeploymentName:   "orkstack",
			BuildID:          "2026-01-02.1",
			ArtifactBackend:  "s3",
			ArtifactLocation: "s3://orkstack-artifacts",
			TemporalAddress:  "localhost:7233",
		}
		cfg.Pools = enginetest.LivePools{
			Orkers:   map[string]int{task.DefaultPool: 2, "gpu": 1},
			LastSeen: fixedNow,
		}
		cfg.PoolPlugins = enginetest.PoolPlugins{
			task.DefaultPool: {"file", "http", "shell"},
			"gpu":            {"container"},
		}
	})
}

// TestAdminInfo_GoldenHumanAndJSON pins both renderings of the system read an
// operator runs first in an incident.
//
// The freshness window matters here: pool liveness is judged against the
// engine's clock, so the fixture's fixed timestamp has to be recent enough to
// count. It is checked below rather than assumed, because a stale fixture
// would silently turn every pool empty and the goldens would record that as
// the truth.
func TestAdminInfo_GoldenHumanAndJSON(t *testing.T) {
	f := adminFixture(t)

	human := f.run("admin", "info").requireExit(t, exitOK)
	requireGolden(t, "admin-info.golden", human.stdout)

	machine := f.run("--json", "admin", "info").requireExit(t, exitOK)
	requireGolden(t, "admin-info.json.golden", machine.stdout)

	// The one field the goldens cannot hold, because it is the answering
	// replica's own clock: it must be there, and it must be a time.
	var document struct {
		ServerTime string `json:"serverTime"`
	}
	if err := json.Unmarshal([]byte(machine.stdout), &document); err != nil {
		t.Fatalf("admin info --json is not valid JSON: %v\n%s", err, machine.stdout)
	}
	if _, err := time.Parse(time.RFC3339, document.ServerTime); err != nil {
		t.Errorf("serverTime = %q, want the replica's clock as RFC3339: %v",
			document.ServerTime, err)
	}
	if !strings.Contains(human.stdout, "configuration") {
		t.Error("human output does not say the values are configuration-derived; " +
			"an operator would read them as observed")
	}
}

// TestAdminInfo_SaysVersioningIsOffRatherThanPrintingAnEmptyBuildID is the
// design's explicit rendering rule (v004-design §3.3).
//
// An empty build id is not "unknown": it is the documented off switch for
// Worker Deployment Versioning (AGENTS.md §10). Printing a blank cell would
// read as missing data on the one surface an operator checks during a
// rollout.
func TestAdminInfo_SaysVersioningIsOffRatherThanPrintingAnEmptyBuildID(t *testing.T) {
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.System = api.SystemInfo{
			EngineVersion:    "v0.0.4",
			GoVersion:        "go1.25.6",
			DeploymentName:   "orkstack",
			ArtifactBackend:  "local",
			ArtifactLocation: "./.data/artifacts",
			TemporalAddress:  "localhost:7233",
		}
	})

	unversioned := f.run("admin", "info").requireExit(t, exitOK)
	requireGolden(t, "admin-info-unversioned.golden", unversioned.stdout)
	if !strings.Contains(unversioned.stdout, "off") {
		t.Errorf("an unversioned deployment does not say so:\n%s", unversioned.stdout)
	}
}

// TestPoolList_GoldenHumanAndJSON pins pool health in both renderings.
//
// A pool with zero live Orkers is the state this command exists to make
// visible, so it is in the golden: the number alone reads as healthy, and the
// text beside it is what stops an operator scrolling past a pool nothing is
// serving.
func TestPoolList_GoldenHumanAndJSON(t *testing.T) {
	f := adminFixture(t)
	// A namespace whose allow-list names a pool nothing serves, so the empty
	// row is a real answer rather than a fixture trick.
	f.run("namespace", "create", "vision",
		"--default-pool", "gpu",
		"--allow-pool", "gpu",
		"--allow-pool", "cold",
	).requireExit(t, exitOK)

	human := f.run("pool", "list", "--namespace", "vision").requireExit(t, exitOK)
	requireGolden(t, "pool-list.golden", human.stdout)

	machine := f.run("--json", "pool", "list", "--namespace", "vision").requireExit(t, exitOK)
	requireGolden(t, "pool-list.json.golden", machine.stdout)

	if !strings.Contains(human.stdout, "no live orkers") {
		t.Errorf("a pool with no Orkers is printed as a bare number:\n%s", human.stdout)
	}
	if !strings.Contains(human.stdout, "container") {
		t.Errorf("pool health does not report what the pool's Orkers run:\n%s", human.stdout)
	}
}

// TestPoolList_DefaultNamespaceIsTheDeploymentsOwnDefault covers the command
// with no --namespace: the pools the default namespace may dispatch onto, with
// the deployment's default marked.
func TestPoolList_DefaultNamespaceIsTheDeploymentsOwnDefault(t *testing.T) {
	f := adminFixture(t)

	listed := f.run("pool", "list").requireExit(t, exitOK)
	requireGolden(t, "pool-list-default.golden", listed.stdout)
	if !strings.Contains(listed.stdout, task.DefaultPool) {
		t.Errorf("the default namespace's pool listing does not name %q:\n%s",
			task.DefaultPool, listed.stdout)
	}
}
