package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// The release fixture's build ids.
const (
	relA = "2026.08.16-a1b2c3d" // current
	relC = "2026.08.23-c4d5e6f" // candidate
	relP = "2026.08.09-9f8e7d6" // previous, draining
	relQ = "2026.08.02-0a1b2c3" // previous, drained
)

var releaseClockStart = time.Date(2026, 9, 24, 9, 15, 40, 0, time.UTC)

// releaseStep is what the cluster says at one ListReleases.
type releaseStep struct {
	obs     engine.ReleaseObservation
	obsErr  error
	counts  map[string]int64
	runs    map[string][]*orkstackv1.Run
	runErr  map[string]error
	running int64
}

// scriptedReleases is the CLI fixture's engine.ReleaseCluster: a script of
// successive answers, one step per ObserveReleases (the last repeats), and a
// record of every write.
type scriptedReleases struct {
	mu      sync.Mutex
	steps   []releaseStep
	current releaseStep
	awaitOK bool
	sets    []engine.Routing

	// firstObserveTakes makes the first ObserveReleases (the CLI's own
	// ListReleases) that slow; writeBudget records how much of its deadline
	// the write had left when SetRouting was reached.
	firstObserveTakes time.Duration
	observed          int
	writeBudget       time.Duration
}

func newScriptedReleases(steps ...releaseStep) *scriptedReleases {
	return &scriptedReleases{steps: steps, awaitOK: true}
}

func (s *scriptedReleases) ObserveReleases(ctx context.Context) (engine.ReleaseObservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observed++
	if s.observed == 1 && s.firstObserveTakes > 0 {
		// A slow cluster read, not a synchronization: the CLI's read spends
		// this much of its own request budget.
		select {
		case <-time.After(s.firstObserveTakes):
		case <-ctx.Done():
			return engine.ReleaseObservation{}, ctx.Err()
		}
	}
	s.current = s.steps[0]
	if len(s.steps) > 1 {
		s.steps = s.steps[1:]
	}
	return s.current.obs, s.current.obsErr
}

func (s *scriptedReleases) ReleaseRuns(_ context.Context, build string, limit int) (int64, []*orkstackv1.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.current.runErr[build]; err != nil {
		return 0, nil, err
	}
	runs := s.current.runs[build]
	if len(runs) > limit {
		runs = runs[:limit]
	}
	return s.current.counts[build], runs, nil
}

func (s *scriptedReleases) RunningRuns(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.running, nil
}

func (s *scriptedReleases) SetRouting(ctx context.Context, _ engine.ReleaseObservation, want engine.Routing) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sets = append(s.sets, want)
	if deadline, ok := ctx.Deadline(); ok {
		s.writeBudget = time.Until(deadline)
	}
	return nil
}

func (s *scriptedReleases) AwaitRouting(context.Context, engine.Routing, time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.awaitOK, nil
}

func (s *scriptedReleases) writes() []engine.Routing {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]engine.Routing(nil), s.sets...)
}

func relRun(id, ns, orkflow, version string, age time.Duration) *orkstackv1.Run {
	return &orkstackv1.Run{Id: id, Namespace: ns, OrkflowName: orkflow, OrkflowVersion: version,
		Status: orkstackv1.Status_STATUS_RUNNING, CreatedAt: timestamppb.New(releaseClockStart.Add(-age))}
}

// rolloutStep is the status view's worked example: A current, C a candidate missing pool
// build, P draining with three runs and a pin, Q drained.
func rolloutStep() releaseStep {
	return releaseStep{
		obs: engine.ReleaseObservation{
			DeploymentName: "orkstack",
			Routing:        engine.Routing{Current: relA},
			CurrentSince:   time.Date(2026, 9, 24, 9, 2, 11, 0, time.UTC),
			Releases: []engine.ObservedRelease{
				{BuildID: relQ, FirstSeenAt: time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC), HasServed: true,
					DrainState:     orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINED,
					DrainCheckedAt: time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)},
				{BuildID: relP, FirstSeenAt: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC), HasServed: true,
					DrainState:     orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINING,
					DrainCheckedAt: time.Date(2026, 9, 24, 9, 13, 2, 0, time.UTC)},
				{BuildID: relA, FirstSeenAt: time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC), HasServed: true,
					RegisteredPools: []string{"build", "gpu", "main"}},
				{BuildID: relC, FirstSeenAt: time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)},
			},
			EngineReplicas: map[string]int{relA: 2, relC: 1, relP: 1},
			PoolUse:        map[string]engine.PoolUse{"build": {LivePollers: 2}, "gpu": {LivePollers: 1}, "main": {LivePollers: 3}},
		},
		counts: map[string]int64{relA: 14, relP: 3},
		runs: map[string][]*orkstackv1.Run{
			relP: {
				relRun("ork-20260922-0001", "payments", "nightly-build", "4", time.Hour),
				relRun("ork-20260922-0002", "payments", "reconcile", "2", 2*time.Hour),
				relRun("ork-20260922-0003", "", "hello", "1", 3*time.Hour),
			},
		},
	}
}

// releaseFleetFor is the Orker inventory beside rolloutStep.
func releaseFleetFor() *scriptedFleet {
	seen := releaseClockStart.Add(-2 * time.Second)
	return &scriptedFleet{
		presence: []engine.OrkerPresence{
			{Identity: "orker-91c0", LastSeen: seen}, {Identity: "orker-b45e", LastSeen: seen},
			{Identity: "orker-g1", LastSeen: seen}, {Identity: "orker-c1", LastSeen: seen},
			{Identity: "orker-c2", LastSeen: seen}, {Identity: "orker-7f3a", LastSeen: seen},
			{Identity: "orker-p1", LastSeen: seen}, {Identity: "orker-d002", LastSeen: seen},
		},
		described: map[string]*orkstackv1.DescribeOrkerResult{
			"orker-91c0": {OrkerId: "orker-91c0", Pool: "main", BuildId: relA, RunningTasks: 2},
			"orker-b45e": {OrkerId: "orker-b45e", Pool: "build", BuildId: relA, RunningTasks: 1},
			"orker-g1":   {OrkerId: "orker-g1", Pool: "gpu", BuildId: relA},
			"orker-c1":   {OrkerId: "orker-c1", Pool: "main", BuildId: relC},
			"orker-c2":   {OrkerId: "orker-c2", Pool: "gpu", BuildId: relC},
			"orker-7f3a": {OrkerId: "orker-7f3a", Pool: "build", BuildId: relP, OpenPins: 1, MaxConcurrentPins: 1,
				Pins: []*orkstackv1.OpenPin{{RunId: "ork-20260922-0001", Pin: "build",
					ExpiresAt: timestamppb.New(releaseClockStart.Add(41 * time.Minute))}}},
			"orker-p1":   {OrkerId: "orker-p1", Pool: "main", BuildId: relP},
			"orker-d002": {OrkerId: "orker-d002", Pool: "gpu"},
		},
		failures: map[string]error{},
		script:   map[string][]*orkstackv1.DescribeOrkerResult{},
	}
}

func newReleaseFixture(t *testing.T, releases *scriptedReleases, fleet *scriptedFleet) *fixture {
	t.Helper()
	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.Releases = releases
		cfg.Fleet = fleet
	})
}

// useReleaseClock replaces the loops' clock and cadence for one test.
func useReleaseClock(t *testing.T, start time.Time) *fakeDrainClock {
	t.Helper()
	clock := &fakeDrainClock{now: start}
	prevClock, prevWatch, prevWait, prevLimit := releaseClock, releaseWatchEvery, releaseWaitEvery, releaseWatchLimit
	releaseClock = clock
	t.Cleanup(func() {
		releaseClock, releaseWatchEvery, releaseWaitEvery, releaseWatchLimit = prevClock, prevWatch, prevWait, prevLimit
	})
	return clock
}

func releaseRequests(f *fixture) []string {
	var out []string
	for _, path := range f.requests() {
		if strings.Contains(path, "Release") {
			out = append(out, path[strings.LastIndex(path, "/")+1:])
		}
	}
	return out
}

func TestReleaseStatus_Golden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	fleet := releaseFleetFor()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-old", LastSeen: releaseClockStart})
	fleet.described["orker-old"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-old", Pool: "main", BuildId: "2026.07.01-0000000"}
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), fleet)
	out := f.run("release", "status").requireExit(t, exitOK)
	requireGolden(t, "release-status.golden", out.stdout)

	jsonOut := f.run("release", "status", "--json").requireExit(t, exitOK)
	requireGolden(t, "release-status.json.golden", jsonOut.stdout)
	var decoded map[string]any
	if err := json.Unmarshal([]byte(jsonOut.stdout), &decoded); err != nil || decoded["releases"] == nil || decoded["revision"] == nil {
		t.Fatalf("--json is not the raw ListReleasesResponse: %v\n%s", err, jsonOut.stdout)
	}
}

func TestReleaseStatus_UnversionedGolden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	step := releaseStep{obs: engine.ReleaseObservation{DeploymentName: "orkstack"}, obsErr: engine.ErrNotVersioned}
	f := newReleaseFixture(t, newScriptedReleases(step), releaseFleetFor())
	out := f.run("release", "status").requireExit(t, exitOK)
	requireGolden(t, "release-status-unversioned.golden", out.stdout)
}

func TestReleaseStatus_NoCurrentGolden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	step := releaseStep{
		obs: engine.ReleaseObservation{
			DeploymentName: "orkstack",
			Releases:       []engine.ObservedRelease{{BuildID: "v1", FirstSeenAt: releaseClockStart.Add(-time.Hour)}},
			EngineReplicas: map[string]int{"v1": 1, "": 2},
		},
		counts:  map[string]int64{"v1": 0},
		running: 1,
	}
	fleet := &scriptedFleet{
		presence:  []engine.OrkerPresence{{Identity: "orker-v1", LastSeen: releaseClockStart}},
		described: map[string]*orkstackv1.DescribeOrkerResult{"orker-v1": {OrkerId: "orker-v1", Pool: "main", BuildId: "v1"}},
		failures:  map[string]error{}, script: map[string][]*orkstackv1.DescribeOrkerResult{},
	}
	f := newReleaseFixture(t, newScriptedReleases(step), fleet)
	out := f.run("release", "status").requireExit(t, exitOK)
	requireGolden(t, "release-status-no-current.golden", out.stdout)
}

func TestReleaseStatus_UnknownRunsGolden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	step := rolloutStep()
	step.runErr = map[string]error{relP: context.DeadlineExceeded}
	fleet := releaseFleetFor()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-c001", LastSeen: releaseClockStart})
	fleet.failures["orker-c001"] = context.DeadlineExceeded
	f := newReleaseFixture(t, newScriptedReleases(step), fleet)
	out := f.run("release", "status").requireExit(t, exitOK)
	requireGolden(t, "release-status-unknown-runs.golden", out.stdout)
	if !strings.Contains(out.stdout, "unknown") {
		t.Fatalf("an unreadable count did not render as unknown:\n%s", out.stdout)
	}
}

func TestReleaseStatus_WatchWithJSONIsRefusedBeforeAnyRequest(t *testing.T) {
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	out := f.run("release", "status", "--watch", "--json").requireExit(t, exitError)
	if !strings.Contains(out.stderr, "--watch is a view for a person") {
		t.Fatalf("stderr = %s", out.stderr)
	}
	if got := f.requests(); len(got) != 0 {
		t.Fatalf("requests = %v; want none", got)
	}
}

func TestReleaseStatus_WatchReprintsOnlyOnChange(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	releaseWatchLimit = 3
	a := rolloutStep()
	b := rolloutStep()
	b.counts = map[string]int64{relA: 15, relP: 2}
	f := newReleaseFixture(t, newScriptedReleases(a, a, b), releaseFleetFor())
	out := f.run("release", "status", "--watch").requireExit(t, exitOK)
	if n := strings.Count(out.stdout, "deployment:"); n != 2 {
		t.Fatalf("renderings = %d, want 2\n%s", n, out.stdout)
	}
	if !strings.Contains(out.stdout, "— 09:15:40 —") || !strings.Contains(out.stdout, "— 09:15:50 —") {
		t.Fatalf("watch headers missing:\n%s", out.stdout)
	}
	if got := releaseRequests(f); len(got) != 3 {
		t.Fatalf("requests = %v", got)
	}
}

func TestReleasePromote_Golden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	step := rolloutStep()
	fleet := releaseFleetFor()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-c3", LastSeen: releaseClockStart})
	fleet.described["orker-c3"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-c3", Pool: "build", BuildId: relC, MaxConcurrentPins: 1}
	releases := newScriptedReleases(step)
	f := newReleaseFixture(t, releases, fleet)
	out := f.run("release", "promote", relC, "--yes").requireExit(t, exitOK)
	requireGolden(t, "release-promote.golden", out.stdout)
	if w := releases.writes(); len(w) != 1 || w[0] != (engine.Routing{Current: relC}) {
		t.Fatalf("writes = %v", w)
	}
}

func TestReleasePromote_RefusedGolden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	releases := newScriptedReleases(rolloutStep())
	f := newReleaseFixture(t, releases, releaseFleetFor())
	out := f.run("release", "promote", relC, "--yes").requireExit(t, exitError)
	requireGolden(t, "release-promote-refused.golden", out.stderr)
	if len(releases.writes()) != 0 {
		t.Fatal("a refused promote wrote")
	}
}

func TestReleasePromote_SendsTheRevisionItRead(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	step := rolloutStep()
	f := newReleaseFixture(t, newScriptedReleases(step), releaseFleetFor())
	f.run("release", "promote", relA, "--yes").requireExit(t, exitOK)
	var sent orkstackv1.PromoteReleaseRequest
	if !f.lastRequest("/orkstack.v1.SystemService/PromoteRelease", &sent) {
		t.Fatal("no PromoteRelease was sent")
	}
	if sent.GetExpectedRevision() != step.obs.Routing.Revision() || sent.GetBuildId() != relA {
		t.Fatalf("sent %v; want the revision %q the read returned", &sent, step.obs.Routing.Revision())
	}
	if got := releaseRequests(f); strings.Join(got, ",") != "ListReleases,PromoteRelease" {
		t.Fatalf("requests = %v", got)
	}
}

// TestReleaseWrites_BoundTheWriteOnItsOwn proves each request is bounded:
// promote, rollback and ramp each read ListReleases and then write, and the
// write gets a whole --timeout of its own. The server-side write may spend up
// to 10s confirming routing after the change; were the read's time deducted
// from it, a slow read would leave the user a client-side timeout for a change
// the cluster made.
func TestReleaseWrites_BoundTheWriteOnItsOwn(t *testing.T) {
	const timeout, readTakes = 2 * time.Second, 1200 * time.Millisecond
	// Each target is given what the judgement asks of it, as in the goldens:
	// C a pin host in "build", P an Orker in "gpu".
	withOrker := func(id, pool, build string, pins int32) func() *scriptedFleet {
		return func() *scriptedFleet {
			fleet := releaseFleetFor()
			fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: id, LastSeen: releaseClockStart})
			fleet.described[id] = &orkstackv1.DescribeOrkerResult{OrkerId: id, Pool: pool, BuildId: build, MaxConcurrentPins: pins}
			return fleet
		}
	}
	for _, tc := range []struct {
		name  string
		args  []string
		fleet func() *scriptedFleet
	}{
		{"promote", []string{"release", "promote", relC, "--yes"}, withOrker("orker-c3", "build", relC, 1)},
		{"rollback", []string{"release", "rollback", relP, "--yes"}, withOrker("orker-p2", "gpu", relP, 0)},
		{"ramp", []string{"release", "ramp", relC, "--percent", "10"}, withOrker("orker-c3", "build", relC, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useReleaseClock(t, releaseClockStart)
			releases := newScriptedReleases(rolloutStep())
			releases.firstObserveTakes = readTakes
			f := newReleaseFixture(t, releases, tc.fleet())
			f.run(append([]string{"--timeout", timeout.String()}, tc.args...)...).requireExit(t, exitOK)
			if len(releases.writes()) != 1 {
				t.Fatalf("writes = %v; want the one write", releases.writes())
			}
			releases.mu.Lock()
			budget := releases.writeBudget
			releases.mu.Unlock()
			// Sharing the read's context would leave at most timeout-readTakes
			// (800ms); a request of its own starts with the whole 2s.
			if budget <= timeout-readTakes+200*time.Millisecond {
				t.Fatalf("the write reached the cluster with %v of its deadline left; "+
					"want close to the whole --timeout %v, not what the %v read left over", budget, timeout, readTakes)
			}
		})
	}
}

func TestReleasePromote_UnconfirmedRoutingGolden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	step := rolloutStep()
	step.obs.Routing = engine.Routing{Current: relA, Ramping: relC, RampPercent: 10}
	fleet := releaseFleetFor()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-c3", LastSeen: releaseClockStart})
	fleet.described["orker-c3"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-c3", Pool: "build", BuildId: relC, MaxConcurrentPins: 1}
	releases := newScriptedReleases(step)
	releases.awaitOK = false
	f := newReleaseFixture(t, releases, fleet)
	out := f.run("release", "promote", relC, "--yes").requireExit(t, exitOK)
	requireGolden(t, "release-promote-unconfirmed.golden", out.stdout)
}

func TestReleaseRollback_RefusesATargetThatNeverServedBeforePromoting(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	out := f.run("release", "rollback", relC, "--yes").requireExit(t, exitError)
	if !strings.Contains(out.stderr, `rollback returns to a release that was current before; `+relC+` is candidate — use "orkctl release promote `+relC+`"`) {
		t.Fatalf("stderr = %s", out.stderr)
	}
	if got := releaseRequests(f); strings.Join(got, ",") != "ListReleases" {
		t.Fatalf("requests = %v; want one ListReleases and no PromoteRelease", got)
	}
	unknown := f.run("release", "rollback", "v-never").requireExit(t, exitError)
	if !strings.Contains(unknown.stderr, "no release v-never is known to the cluster") {
		t.Fatalf("stderr = %s", unknown.stderr)
	}
}

func TestReleaseRollback_Golden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	step := rolloutStep()
	// P is the target: give it an Orker in every pool in use, and a pin host.
	fleet := releaseFleetFor()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-p2", LastSeen: releaseClockStart})
	fleet.described["orker-p2"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-p2", Pool: "gpu", BuildId: relP}
	releases := newScriptedReleases(step)
	f := newReleaseFixture(t, releases, fleet)
	out := f.run("release", "rollback", relP, "--yes").requireExit(t, exitOK)
	requireGolden(t, "release-rollback.golden", out.stdout)
	if w := releases.writes(); len(w) != 1 || w[0] != (engine.Routing{Current: relP}) {
		t.Fatalf("writes = %v", w)
	}
}

func TestReleaseRamp_Golden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	fleet := releaseFleetFor()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-c3", LastSeen: releaseClockStart})
	fleet.described["orker-c3"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-c3", Pool: "build", BuildId: relC, MaxConcurrentPins: 1}
	releases := newScriptedReleases(rolloutStep())
	f := newReleaseFixture(t, releases, fleet)
	out := f.run("release", "ramp", relC, "--percent", "10").requireExit(t, exitOK)
	requireGolden(t, "release-ramp.golden", out.stdout)
	if w := releases.writes(); len(w) != 1 || w[0] != (engine.Routing{Current: relA, Ramping: relC, RampPercent: 10}) {
		t.Fatalf("writes = %v", w)
	}
}

func TestReleaseRamp_StopGolden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	step := rolloutStep()
	step.obs.Routing = engine.Routing{Current: relA, Ramping: relC, RampPercent: 10}
	step.counts[relC] = 2
	releases := newScriptedReleases(step)
	f := newReleaseFixture(t, releases, releaseFleetFor())
	out := f.run("release", "ramp", relC, "--percent", "0").requireExit(t, exitOK)
	requireGolden(t, "release-ramp-stop.golden", out.stdout)
	if w := releases.writes(); len(w) != 1 || w[0] != (engine.Routing{Current: relA}) {
		t.Fatalf("writes = %v", w)
	}
}

func TestReleaseRamp_PercentIsRequiredAndBoundedBeforeAnyRequest(t *testing.T) {
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	missing := f.run("release", "ramp", relC).requireExit(t, exitError)
	if !strings.Contains(missing.stderr, `"percent" not set`) {
		t.Fatalf("stderr = %s", missing.stderr)
	}
	for _, p := range []string{"-1", "101"} {
		out := f.run("release", "ramp", relC, "--percent", p).requireExit(t, exitError)
		if !strings.Contains(out.stderr, "--percent is 1–100") {
			t.Fatalf("percent %s: stderr = %s", p, out.stderr)
		}
	}
	if got := f.requests(); len(got) != 0 {
		t.Fatalf("requests = %v; want none", got)
	}
}

// waitSteps is (3 runs, 1 pin, DRAINING) → (1, 0, DRAINING) → (0, 0, DRAINED).
func waitSteps() ([]releaseStep, *scriptedFleet) {
	first := rolloutStep()
	second := rolloutStep()
	second.counts = map[string]int64{relA: 14, relP: 1}
	second.runs = map[string][]*orkstackv1.Run{relP: first.runs[relP][:1]}
	second.obs.Releases[1].DrainCheckedAt = time.Date(2026, 9, 24, 9, 28, 40, 0, time.UTC)
	third := rolloutStep()
	third.counts = map[string]int64{relA: 14, relP: 0}
	third.runs = map[string][]*orkstackv1.Run{}
	third.obs.Releases[1].DrainState = orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINED
	third.obs.Releases[1].DrainCheckedAt = time.Date(2026, 9, 24, 9, 43, 12, 0, time.UTC)

	fleet := releaseFleetFor()
	pinned := fleet.described["orker-7f3a"]
	free := &orkstackv1.DescribeOrkerResult{OrkerId: "orker-7f3a", Pool: "build", BuildId: relP, MaxConcurrentPins: 1}
	fleet.script["orker-7f3a"] = []*orkstackv1.DescribeOrkerResult{pinned, free, free}
	return []releaseStep{first, second, third}, fleet
}

func TestReleaseWait_DrainTranscript_Golden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	releaseWaitEvery = 10 * time.Second
	steps, fleet := waitSteps()
	f := newReleaseFixture(t, newScriptedReleases(steps...), fleet)
	out := f.run("release", "wait", relP, "--timeout", "2h").requireExit(t, exitOK)
	requireGolden(t, "release-wait.golden", out.stdout)
}

func TestReleaseWait_TimesOutExit4NamingRunsAndPins(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	out := f.run("release", "wait", relP, "--timeout", "2m").requireExit(t, exitDrainTimedOut)
	requireGolden(t, "release-wait-timeout.golden", out.stdout)
	for _, want := range []string{"is NOT drained after 2m0s", "run ork-20260922-0001 (payments/nightly-build@4)",
		`pin "build" of run ork-20260922-0001 on orker-7f3a`, "stopping orker-7f3a fails it with PIN_HOST_LOST."} {
		if !strings.Contains(out.stdout, want) {
			t.Fatalf("stdout lacks %q:\n%s", want, out.stdout)
		}
	}
}

func TestReleaseWait_TimeoutFlagIsTheWaitBound(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	f.run("release", "wait", relP, "--timeout", "1s").requireExit(t, exitDrainTimedOut)
	if got := releaseRequests(f); len(got) != 1 {
		t.Fatalf("requests = %v; a 1s bound under a 10s interval polls once", got)
	}
	for i, path := range f.requests() {
		if !strings.Contains(path, "ListReleases") {
			continue
		}
		ms, err := strconv.Atoi(f.timeoutHeaders()[i])
		if err != nil || ms < 29000 || ms > 30000 {
			t.Fatalf("poll %d carried Connect-Timeout-Ms %q; want releasePollTimeout (30s)", i, f.timeoutHeaders()[i])
		}
	}
}

func TestReleaseWait_CurrentIsExit1(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	out := f.run("release", "wait", relA).requireExit(t, exitError)
	if !strings.Contains(out.stderr, "release "+relA+" takes new runs (current); it cannot drain until another release is promoted") {
		t.Fatalf("stderr = %s", out.stderr)
	}
}

func TestReleaseWait_UnknownAtFirstIsExit1(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	out := f.run("release", "wait", "v-never").requireExit(t, exitError)
	if !strings.Contains(out.stderr, `no release v-never is known to the cluster; "orkctl release status" lists the ones that are`) {
		t.Fatalf("stderr = %s", out.stderr)
	}
}

func TestReleaseWait_ForgottenAfterwardsIsExit0(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	gone := rolloutStep()
	gone.obs.Releases = append(gone.obs.Releases[:1], gone.obs.Releases[2:]...) // P forgotten
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep(), gone), releaseFleetFor())
	out := f.run("release", "wait", relP).requireExit(t, exitOK)
	if !strings.Contains(out.stdout, "is no longer known to the cluster: it drained and none of its processes is polling") {
		t.Fatalf("stdout = %s", out.stdout)
	}
}

func TestReleaseWait_PromotedAgainIsExit1(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	again := rolloutStep()
	again.obs.Routing = engine.Routing{Current: relP}
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep(), again), releaseFleetFor())
	out := f.run("release", "wait", relP).requireExit(t, exitError)
	if !strings.Contains(out.stderr, "takes new runs again (it was promoted or ramped); stop waiting") {
		t.Fatalf("stderr = %s", out.stderr)
	}
}

func TestReleaseWait_JSONPrintsTheFinalResponseOnEveryExitPath(t *testing.T) {
	cases := []struct {
		name  string
		build string
		steps func() ([]releaseStep, *scriptedFleet)
		args  []string
		code  int
	}{
		{"drained", relP, waitSteps, nil, exitOK},
		{"timed out", relP, func() ([]releaseStep, *scriptedFleet) {
			return []releaseStep{rolloutStep()}, releaseFleetFor()
		}, []string{"--timeout", "30s"}, exitDrainTimedOut},
		{"current", relA, func() ([]releaseStep, *scriptedFleet) {
			return []releaseStep{rolloutStep()}, releaseFleetFor()
		}, nil, exitError},
		{"unknown", "v-never", func() ([]releaseStep, *scriptedFleet) {
			return []releaseStep{rolloutStep()}, releaseFleetFor()
		}, nil, exitError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			useReleaseClock(t, releaseClockStart)
			steps, fleet := tc.steps()
			f := newReleaseFixture(t, newScriptedReleases(steps...), fleet)
			args := append([]string{"release", "wait", tc.build, "--json"}, tc.args...)
			out := f.run(args...).requireExit(t, tc.code)
			var decoded map[string]any
			if err := json.Unmarshal([]byte(out.stdout), &decoded); err != nil || decoded["deploymentName"] != "orkstack" {
				t.Fatalf("stdout is not one ListReleasesResponse: %v\n%s", err, out.stdout)
			}
		})
	}
}

func TestReleaseCommands_IgnoreNamespace(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	plain := f.run("release", "status", "--json").requireExit(t, exitOK)
	scoped := f.run("release", "status", "--json", "-n", "payments").requireExit(t, exitOK)
	if plain.stdout != scoped.stdout {
		t.Fatalf("-n changed the answer:\n%s\n---\n%s", plain.stdout, scoped.stdout)
	}
	var sent orkstackv1.ListReleasesRequest
	if !f.lastRequest("/orkstack.v1.SystemService/ListReleases", &sent) || len(sent.GetFilters()) != 0 {
		t.Fatalf("the request carried %v", &sent)
	}
}

// releaseCLIForbidden is internal/api's releaseForbidden: nothing a release
// surface prints may name a channel, a session, or the cluster's own objects.
var releaseCLIForbidden = regexp.MustCompile(
	`ork-pool-|ork-tasks|ork-engine-events|ork-orker|__internal|queue|session|Temporal|worker deployment|search attribute|TemporalWorkerDeploymentVersion`)

func TestReleaseGoldens_NeverNameTheCluster(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "release-*.golden"))
	if err != nil || len(paths) < 10 {
		t.Fatalf("goldens = %v, %v", paths, err)
	}
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if hit := releaseCLIForbidden.FindString(string(body)); hit != "" {
			t.Errorf("%s names %q", path, hit)
		}
	}
	root, _ := newRootCommand()
	var release *cobra.Command
	for _, cmd := range root.Commands() {
		if cmd.Name() == "release" {
			release = cmd
		}
	}
	if release == nil {
		t.Fatal("no release command")
	}
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		texts := []string{cmd.Short, cmd.Long, cmd.Use}
		cmd.LocalFlags().VisitAll(func(flag *pflag.Flag) { texts = append(texts, flag.Usage) })
		for _, text := range texts {
			if hit := releaseCLIForbidden.FindString(text); hit != "" {
				t.Errorf("%s: %q names %q", cmd.CommandPath(), text, hit)
			}
		}
		for _, sub := range cmd.Commands() {
			walk(sub)
		}
	}
	walk(release)
}
