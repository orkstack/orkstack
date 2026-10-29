package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// ---------------------------------------------------------------------------
// admin info / pool list: golden human and --json output
// ---------------------------------------------------------------------------
type adminLivePools struct {
	Orkers   map[string]int
	LastSeen time.Time
}

func (p adminLivePools) ObservePool(_ context.Context, channel string) (engine.PoolLiveness, error) {
	var pool string
	switch channel {
	case "ork-tasks":
		pool = task.DefaultPool
	case "ork-pool-gpu":
		pool = "gpu"
	default:
		return engine.PoolLiveness{}, nil
	}
	orkers := p.Orkers[pool]
	live := engine.PoolLiveness{Orkers: orkers}
	if orkers > 0 {
		live.LastSeen = p.LastSeen
	}
	return live, nil
}

// adminFixture is a CLI fixture wired with the Config fields `cmd/ork` wires
// and no others: system information injected as `ork` injects it, and the
// pool probe. A fixture that configured a seam no shipped binary sets would
// golden a deployment nobody can run.
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
			EngineVersion:    "v1.2.3",
			GoVersion:        "go1.25.6",
			DeploymentName:   "orkstack",
			BuildID:          "2026-01-02.1",
			ArtifactBackend:  "s3",
			ArtifactLocation: "s3://orkstack-artifacts",
			TemporalAddress:  "localhost:7233",
		}
		cfg.Pools = adminLivePools{
			Orkers:   map[string]int{task.DefaultPool: 2, "gpu": 1},
			LastSeen: fixedNow,
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
// explicit rendering rule for an unversioned deployment.
//
// An empty build id is not "unknown": it is the documented off switch for
// Worker Deployment Versioning (AGENTS.md §7.2). Printing a blank cell would
// read as missing data on the one surface an operator checks during a
// rollout.
func TestAdminInfo_SaysVersioningIsOffRatherThanPrintingAnEmptyBuildID(t *testing.T) {
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.System = api.SystemInfo{
			EngineVersion:    "v1.2.3",
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

// TestPoolList_AnswersWithTheReachablePoolsAndMarksTheRest — F1, at the
// surface an operator actually reads.
//
// One pool the cluster cannot answer for must not fail the command outright:
// an `orkctl pool list` that printed an Unavailable error and NO pools would
// hide the healthy ones beside the broken one on exactly the surface somebody
// opens during an incident.
//
// It answers, and the unreachable row says what it is. The distinction
// this pins is the one that decides what an operator does next: `0 (no live
// orkers)` means start an Orker, `unknown (unreachable)` means the cluster did
// not answer and the pool may be perfectly healthy. A blank or a zero would
// collapse the two and send them to scale a pool that needs nothing.
func TestPoolList_AnswersWithTheReachablePoolsAndMarksTheRest(t *testing.T) {
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Pools = partlyReachableAdminPools{
			live:      adminLivePools{Orkers: map[string]int{task.DefaultPool: 2}, LastSeen: fixedNow},
			unreached: map[string]string{"ork-pool-gpu": "context deadline exceeded"},
		}
	})
	f.run("namespace", "create", "vision",
		"--allow-pool", task.DefaultPool,
		"--allow-pool", "gpu",
	).requireExit(t, exitOK)

	human := f.run("pool", "list", "--namespace", "vision").requireExit(t, exitOK)
	requireGolden(t, "pool-list-unreachable.golden", human.stdout)

	machine := f.run("--json", "pool", "list", "--namespace", "vision").requireExit(t, exitOK)
	requireGolden(t, "pool-list-unreachable.json.golden", machine.stdout)

	if !strings.Contains(human.stdout, task.DefaultPool) {
		t.Errorf("the pool that ANSWERED is missing; one unreachable pool must not "+
			"hide the rest:\n%s", human.stdout)
	}
	if !strings.Contains(human.stdout, "unreachable") {
		t.Errorf("the unreachable pool is not marked, so its 0 reads as 'no Orkers' "+
			"— the opposite instruction:\n%s", human.stdout)
	}
	if !strings.Contains(human.stdout, "context deadline exceeded") {
		t.Errorf("the reason is not printed, so an operator cannot tell a timeout "+
			"from a dead cluster:\n%s", human.stdout)
	}
}

// partlyReachableAdminPools answers for one channel and times out on another,
// which is what a cluster under load looks like: not "down", but "this
// question did not come back".
type partlyReachableAdminPools struct {
	live      adminLivePools
	unreached map[string]string
}

func (p partlyReachableAdminPools) ObservePool(
	ctx context.Context,
	channel string,
) (engine.PoolLiveness, error) {
	if reason, ok := p.unreached[channel]; ok {
		return engine.PoolLiveness{}, errors.New(reason)
	}
	return p.live.ObservePool(ctx, channel)
}
