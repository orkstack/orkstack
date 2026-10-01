package api_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// stubReleases states what a ReleaseCluster would read from the cluster, and
// records every write it was asked to make.
type stubReleases struct {
	mu         sync.Mutex
	obs        engine.ReleaseObservation
	obsErr     error
	counts     map[string]int64
	runs       map[string][]*orkstackv1.Run
	runErr     map[string]error
	running    int64
	runningErr error
	setErr     error
	awaitOK    bool
	awaitErr   error

	sets    []engine.Routing
	awaited []engine.Routing
}

func (s *stubReleases) ObserveReleases(context.Context) (engine.ReleaseObservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.obs, s.obsErr
}

func (s *stubReleases) ReleaseRuns(_ context.Context, build string, limit int) (int64, []*orkstackv1.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.runErr[build]; err != nil {
		return 0, nil, err
	}
	runs := s.runs[build]
	if len(runs) > limit {
		runs = runs[:limit]
	}
	return s.counts[build], runs, nil
}

func (s *stubReleases) RunningRuns(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running, s.runningErr
}

func (s *stubReleases) SetRouting(_ context.Context, _ engine.ReleaseObservation, want engine.Routing) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sets = append(s.sets, want)
	if s.setErr == nil {
		s.obs.Routing = want
	}
	return s.setErr
}

func (s *stubReleases) AwaitRouting(_ context.Context, want engine.Routing, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.awaited = append(s.awaited, want)
	return s.awaitOK, s.awaitErr
}

func (s *stubReleases) writes() []engine.Routing {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]engine.Routing(nil), s.sets...)
}

var releaseT0 = time.Date(2026, 9, 24, 9, 0, 0, 0, time.UTC)

// rolloutReleases is v1 current, v2 a passing candidate, v0 a drained
// previous release, with v1's and v2's Orkers in the fleet.
func rolloutReleases() *stubReleases {
	return &stubReleases{
		obs: engine.ReleaseObservation{
			DeploymentName: "orkstack",
			Routing:        engine.Routing{Current: "v1"},
			CurrentSince:   releaseT0,
			Releases: []engine.ObservedRelease{
				{BuildID: "v0", FirstSeenAt: releaseT0.Add(-48 * time.Hour), HasServed: true,
					DrainState: orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINED, DrainCheckedAt: releaseT0},
				{BuildID: "v1", FirstSeenAt: releaseT0.Add(-24 * time.Hour), HasServed: true, RegisteredPools: []string{"main"}},
				{BuildID: "v2", FirstSeenAt: releaseT0.Add(-time.Hour)},
			},
			EngineReplicas: map[string]int{"v1": 2, "v2": 1, "": 0},
			PoolUse:        map[string]engine.PoolUse{"main": {LivePollers: 2}},
		},
		counts:  map[string]int64{"v1": 3},
		runs:    map[string][]*orkstackv1.Run{"v1": {{Id: "ork-1", Namespace: "payments", OrkflowName: "nightly", OrkflowVersion: "4"}}},
		runErr:  map[string]error{},
		awaitOK: true,
	}
}

func releaseFleet() *stubFleet {
	return &stubFleet{
		presence: []engine.OrkerPresence{
			{Identity: "orker-a1", LastSeen: fleetSeen},
			{Identity: "orker-b1", LastSeen: fleetSeen},
		},
		described: map[string]*orkstackv1.DescribeOrkerResult{
			"orker-a1": {OrkerId: "orker-a1", Pool: "main", BuildId: "v1"},
			"orker-b1": {OrkerId: "orker-b1", Pool: "main", BuildId: "v2"},
		},
		failures: map[string]error{},
	}
}

func releaseService(releases engine.ReleaseCluster, fleet engine.OrkerFleet) *api.Service {
	cfg := api.Config{Fleet: fleet, Now: func() time.Time { return releaseT0.Add(15 * time.Minute) }}
	if releases != nil {
		cfg.Releases = releases
	}
	return api.NewService(cfg)
}

func listReleases(t *testing.T, svc *api.Service, build string) *orkstackv1.ListReleasesResponse {
	t.Helper()
	resp, err := svc.ListReleases(context.Background(), connect.NewRequest(&orkstackv1.ListReleasesRequest{BuildId: build}))
	if err != nil {
		t.Fatalf("ListReleases: %v", err)
	}
	return resp.Msg
}

func releaseByBuild(resp *orkstackv1.ListReleasesResponse, build string) *orkstackv1.Release {
	for _, r := range resp.GetReleases() {
		if r.GetBuildId() == build {
			return r
		}
	}
	return nil
}

func builds(resp *orkstackv1.ListReleasesResponse) string {
	var out []string
	for _, r := range resp.GetReleases() {
		out = append(out, r.GetBuildId())
	}
	return strings.Join(out, ",")
}

func TestListReleases_NotVersionedIsAnAnswerNotAnError(t *testing.T) {
	stub := &stubReleases{obs: engine.ReleaseObservation{DeploymentName: "orkstack"}, obsErr: engine.ErrNotVersioned}
	resp := listReleases(t, releaseService(stub, releaseFleet()), "")
	if resp.GetVersioned() || len(resp.GetReleases()) != 0 || resp.GetDeploymentName() != "orkstack" || resp.GetObservedAt() == nil {
		t.Fatalf("response = %v", resp)
	}
}

func TestListReleases_OrdersCurrentRampingCandidatesPrevious(t *testing.T) {
	stub := rolloutReleases()
	stub.obs.Routing = engine.Routing{Current: "v1", Ramping: "v3", RampPercent: 10}
	stub.obs.RampingSince = releaseT0.Add(time.Minute)
	stub.obs.Releases = append(stub.obs.Releases,
		engine.ObservedRelease{BuildID: "v3", FirstSeenAt: releaseT0.Add(-30 * time.Minute)},
		engine.ObservedRelease{BuildID: "v4", FirstSeenAt: releaseT0.Add(-10 * time.Minute)},
		engine.ObservedRelease{BuildID: "vp", FirstSeenAt: releaseT0.Add(-12 * time.Hour), HasServed: true,
			DrainState: orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINING},
	)
	resp := listReleases(t, releaseService(stub, releaseFleet()), "")
	if got := builds(resp); got != "v1,v3,v4,v2,vp,v0" {
		t.Fatalf("order = %s", got)
	}
	ramping := releaseByBuild(resp, "v3")
	if ramping.GetRole() != orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING || ramping.GetRampPercent() != 10 ||
		!ramping.GetRoleSince().AsTime().Equal(releaseT0.Add(time.Minute)) {
		t.Fatalf("ramping = %v", ramping)
	}
	current := releaseByBuild(resp, "v1")
	if current.GetRole() != orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT || !current.GetRoleSince().AsTime().Equal(releaseT0) ||
		len(current.GetBlockers()) != 0 || current.GetEngineReplicas() != 2 {
		t.Fatalf("current = %v", current)
	}
	if resp.GetRevision() != (engine.Routing{Current: "v1", Ramping: "v3", RampPercent: 10}).Revision() {
		t.Fatalf("revision = %q", resp.GetRevision())
	}
}

func TestListReleases_AttributesOrkersByReleaseAndKeepsTheRestUnattributed(t *testing.T) {
	fleet := releaseFleet()
	fleet.presence = append(fleet.presence,
		engine.OrkerPresence{Identity: "orker-d002", LastSeen: fleetSeen},
		engine.OrkerPresence{Identity: "orker-c001", LastSeen: fleetSeen},
		engine.OrkerPresence{Identity: "orker-old", LastSeen: fleetSeen},
	)
	fleet.described["orker-d002"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-d002", Pool: "gpu"}
	fleet.described["orker-old"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-old", Pool: "main", BuildId: "v-forgotten"}
	fleet.failures["orker-c001"] = errors.New("describe orker: context deadline exceeded")
	resp := listReleases(t, releaseService(rolloutReleases(), fleet), "")
	if got := orkerIDs(releaseByBuild(resp, "v1").GetOrkers()); got != "orker-a1" {
		t.Fatalf("v1 orkers = %s", got)
	}
	if got := orkerIDs(releaseByBuild(resp, "v2").GetOrkers()); got != "orker-b1" {
		t.Fatalf("v2 orkers = %s", got)
	}
	if got := orkerIDs(resp.GetUnattributedOrkers()); got != "orker-c001,orker-d002,orker-old" {
		t.Fatalf("unattributed = %s", got)
	}
}

func TestListReleases_PoolsServedCountOnlyAnsweredUncordonedOrkers(t *testing.T) {
	fleet := releaseFleet()
	fleet.presence = append(fleet.presence,
		engine.OrkerPresence{Identity: "orker-b2", LastSeen: fleetSeen},
		engine.OrkerPresence{Identity: "orker-b3", LastSeen: fleetSeen},
	)
	fleet.described["orker-b2"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-b2", Pool: "gpu", BuildId: "v2",
		Cordoned: true, CordonReason: "patching"}
	fleet.described["orker-b3"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-b3", Pool: "build", BuildId: "v2"}
	resp := listReleases(t, releaseService(rolloutReleases(), fleet), "")
	v2 := releaseByBuild(resp, "v2")
	if got := strings.Join(v2.GetPoolsServed(), ","); got != "build,main" {
		t.Fatalf("pools served = %s", got)
	}
	if len(v2.GetOrkers()) != 3 {
		t.Fatalf("v2 orkers = %v", v2.GetOrkers())
	}
}

func TestListReleases_UnreadableRunIndexIsUnknownNotZero(t *testing.T) {
	stub := rolloutReleases()
	stub.runErr["v1"] = errors.New(`visibility: unknown search attribute TemporalWorkerDeploymentVersion on ork-pool-build`)
	resp := listReleases(t, releaseService(stub, releaseFleet()), "")
	v1 := releaseByBuild(resp, "v1")
	if v1.GetRunsUnavailableReason() == "" || v1.GetRunsInFlight() != 0 || len(v1.GetRuns()) != 0 {
		t.Fatalf("v1 = %v", v1)
	}
	if strings.Contains(v1.GetRunsUnavailableReason(), "Temporal") || strings.Contains(v1.GetRunsUnavailableReason(), "ork-pool") {
		t.Fatalf("the cluster's text reached the wire: %q", v1.GetRunsUnavailableReason())
	}
	if v2 := releaseByBuild(resp, "v2"); v2.GetRunsUnavailableReason() != "" || v2.GetRunsInFlight() != 0 {
		t.Fatalf("v2 = %v", v2)
	}
}

func TestListReleases_DrainedNeedsTheClusterNoPinAndACompleteInventory(t *testing.T) {
	drained := orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINED
	draining := orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINING
	cases := []struct {
		name       string
		release    engine.ObservedRelease
		pin        bool
		unanswered bool
		runs       int64
		runsErr    bool
		want       bool
	}{
		{"DRAINED and no pins", engine.ObservedRelease{HasServed: true, DrainState: drained}, false, false, 0, false, true},
		{"DRAINED and a pin on its Orker", engine.ObservedRelease{HasServed: true, DrainState: drained}, true, false, 0, false, false},
		{"DRAINED and an unreachable unattributed row", engine.ObservedRelease{HasServed: true, DrainState: drained}, false, true, 0, false, false},
		{"DRAINING", engine.ObservedRelease{HasServed: true, DrainState: draining}, false, false, 0, false, false},
		{"CANDIDATE with 0 runs", engine.ObservedRelease{}, false, false, 0, false, true},
		{"CANDIDATE with runs unknown", engine.ObservedRelease{}, false, false, 0, true, false},
		{"DRAINED with runs unknown", engine.ObservedRelease{HasServed: true, DrainState: drained}, false, false, 0, true, true},
		{"DRAINED with a run counted", engine.ObservedRelease{HasServed: true, DrainState: drained}, false, false, 1, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := rolloutReleases()
			tc.release.BuildID = "vx"
			stub.obs.Releases = append(stub.obs.Releases, tc.release)
			stub.counts["vx"] = tc.runs
			if tc.runsErr {
				stub.runErr["vx"] = errors.New("index down")
			}
			fleet := releaseFleet()
			fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-x1", LastSeen: fleetSeen})
			fleet.described["orker-x1"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-x1", Pool: "main", BuildId: "vx"}
			if tc.pin {
				fleet.described["orker-x1"].OpenPins = 1
				fleet.described["orker-x1"].Pins = []*orkstackv1.OpenPin{{RunId: "ork-9", Pin: "build"}}
			}
			if tc.unanswered {
				fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-gone", LastSeen: fleetSeen})
				fleet.failures["orker-gone"] = errors.New("did not answer")
			}
			got := releaseByBuild(listReleases(t, releaseService(stub, fleet), ""), "vx")
			if got.GetDrained() != tc.want {
				t.Fatalf("drained = %v, want %v (%v)", got.GetDrained(), tc.want, got)
			}
		})
	}
}

func TestListReleases_BuildIDFilterKeepsOneReleaseJudgedAgainstAll(t *testing.T) {
	stub := rolloutReleases()
	fleet := releaseFleet()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-g1", LastSeen: fleetSeen})
	fleet.described["orker-g1"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-g1", Pool: "gpu", BuildId: "v1"}
	resp := listReleases(t, releaseService(stub, fleet), "v2")
	if got := builds(resp); got != "v2" {
		t.Fatalf("releases = %s", got)
	}
	blockers := resp.GetReleases()[0].GetBlockers()
	if len(blockers) != 1 || blockers[0].GetPool() != "gpu" ||
		blockers[0].GetKind() != orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_POOL_NOT_SERVED {
		t.Fatalf("blockers = %v", blockers)
	}
}

func TestListReleases_UnknownBuildIDIsAnEmptyList(t *testing.T) {
	resp := listReleases(t, releaseService(rolloutReleases(), releaseFleet()), "v-never")
	if !resp.GetVersioned() || len(resp.GetReleases()) != 0 {
		t.Fatalf("response = %v", resp)
	}
}

func promote(svc *api.Service, build, revision string) (*orkstackv1.PromoteReleaseResponse, error) {
	resp, err := svc.PromoteRelease(context.Background(), connect.NewRequest(&orkstackv1.PromoteReleaseRequest{
		BuildId: build, ExpectedRevision: revision}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func ramp(svc *api.Service, build string, percent int32) (*orkstackv1.RampReleaseResponse, error) {
	resp, err := svc.RampRelease(context.Background(), connect.NewRequest(&orkstackv1.RampReleaseRequest{
		BuildId: build, Percent: percent}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func TestPromoteRelease_RefusesOnEveryBlockerBeforeAnyWrite(t *testing.T) {
	stub := rolloutReleases()
	stub.obs.EngineReplicas["v2"] = 0
	fleet := releaseFleet()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-g1", LastSeen: fleetSeen})
	fleet.described["orker-g1"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-g1", Pool: "gpu", BuildId: "v1"}
	_, err := promote(releaseService(stub, fleet), "v2", "")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("err = %v", err)
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("not a connect error: %v", err)
	}
	msg := ce.Message()
	if !strings.HasPrefix(msg, "release v2 cannot take new runs yet:") ||
		!strings.Contains(msg, "\n  - no engine of release v2 is running") ||
		!strings.Contains(msg, "\n  - pool \"gpu\" is in use (orker-g1 of release v1 serves it)") {
		t.Fatalf("message = %q", msg)
	}
	if len(stub.writes()) != 0 {
		t.Fatalf("writes = %v", stub.writes())
	}
}

func TestPromoteRelease_UnknownIsNotFound(t *testing.T) {
	stub := rolloutReleases()
	_, err := promote(releaseService(stub, releaseFleet()), "v9", "")
	if connect.CodeOf(err) != connect.CodeNotFound || !strings.Contains(err.Error(), "no release v9 is known to the cluster") {
		t.Fatalf("err = %v", err)
	}
	if len(stub.writes()) != 0 {
		t.Fatal("wrote")
	}
}

func TestPromoteRelease_NotVersionedIsFailedPrecondition(t *testing.T) {
	stub := &stubReleases{obs: engine.ReleaseObservation{DeploymentName: "orkstack"}, obsErr: engine.ErrNotVersioned}
	_, err := promote(releaseService(stub, releaseFleet()), "v1", "")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "no release of deployment orkstack has registered with the cluster") {
		t.Fatalf("err = %v", err)
	}
}

func TestPromoteRelease_EmptyBuildIDIsInvalidArgument(t *testing.T) {
	if _, err := promote(releaseService(rolloutReleases(), releaseFleet()), "", ""); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("err = %v", err)
	}
}

func TestPromoteRelease_NoClusterWiredIsUnavailable(t *testing.T) {
	svc := releaseService(nil, releaseFleet())
	_, err := promote(svc, "v1", "")
	if connect.CodeOf(err) != connect.CodeUnavailable || strings.Contains(err.Error(), "emporal") {
		t.Fatalf("err = %v", err)
	}
	if _, err := svc.ListReleases(context.Background(), connect.NewRequest(&orkstackv1.ListReleasesRequest{})); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("list err = %v", err)
	}
	if _, err := ramp(svc, "v1", 10); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("ramp err = %v", err)
	}
}

func TestPromoteRelease_StaleRevisionIsRefusedBeforeAnyWrite(t *testing.T) {
	stub := rolloutReleases()
	stale := (engine.Routing{Current: "v0"}).Revision()
	_, err := promote(releaseService(stub, releaseFleet()), "v2", stale)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "the releases changed since you read them (current is now v1, nothing is ramping)") {
		t.Fatalf("err = %v", err)
	}
	if len(stub.writes()) != 0 {
		t.Fatal("wrote")
	}
	// The revision the read handed out is accepted.
	resp, err := promote(releaseService(stub, releaseFleet()), "v2", (engine.Routing{Current: "v1"}).Revision())
	if err != nil || !resp.GetChanged() {
		t.Fatalf("promote with the read's revision = %v, %v", resp, err)
	}
}

func TestPromoteRelease_CurrentIsANoOp(t *testing.T) {
	stub := rolloutReleases()
	resp, err := promote(releaseService(stub, releaseFleet()), "v1", "")
	if err != nil || resp.GetChanged() || resp.GetPreviousBuildId() != "v1" || !resp.GetRoutingConfirmed() {
		t.Fatalf("resp = %v, err = %v", resp, err)
	}
	if len(stub.writes()) != 0 {
		t.Fatal("wrote")
	}
}

func TestPromoteRelease_KeepsARampThatIsNotTheTarget(t *testing.T) {
	stub := rolloutReleases()
	stub.obs.Routing = engine.Routing{Current: "v1", Ramping: "v3", RampPercent: 20}
	stub.obs.Releases = append(stub.obs.Releases, engine.ObservedRelease{BuildID: "v3"})
	resp, err := promote(releaseService(stub, releaseFleet()), "v2", "")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	want := engine.Routing{Current: "v2", Ramping: "v3", RampPercent: 20}
	if w := stub.writes(); len(w) != 1 || w[0] != want {
		t.Fatalf("writes = %v, want %v", w, want)
	}
	if !resp.GetChanged() || resp.GetPreviousBuildId() != "v1" || resp.GetRevision() != want.Revision() {
		t.Fatalf("resp = %v", resp)
	}
}

func TestPromoteRelease_PromotingTheRampingReleaseEndsItsRamp(t *testing.T) {
	stub := rolloutReleases()
	stub.obs.Routing = engine.Routing{Current: "v1", Ramping: "v2", RampPercent: 20}
	if _, err := promote(releaseService(stub, releaseFleet()), "v2", ""); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if w := stub.writes(); len(w) != 1 || w[0] != (engine.Routing{Current: "v2"}) {
		t.Fatalf("writes = %v", w)
	}
}

func TestPromoteRelease_UnconfirmedRoutingIsReportedNotHidden(t *testing.T) {
	stub := rolloutReleases()
	stub.awaitOK = false
	resp, err := promote(releaseService(stub, releaseFleet()), "v2", "")
	if err != nil || !resp.GetChanged() || resp.GetRoutingConfirmed() {
		t.Fatalf("resp = %v, err = %v", resp, err)
	}
	stub2 := rolloutReleases()
	stub2.awaitErr = errors.New("describe failed")
	resp, err = promote(releaseService(stub2, releaseFleet()), "v2", "")
	if err != nil || resp.GetRoutingConfirmed() {
		t.Fatalf("an await error: resp = %v, err = %v", resp, err)
	}
}

func TestRampRelease_PercentBoundsAreInvalidArgument(t *testing.T) {
	svc := releaseService(rolloutReleases(), releaseFleet())
	for _, p := range []int32{-1, 101} {
		if _, err := ramp(svc, "v2", p); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("percent %d: err = %v", p, err)
		}
	}
}

func TestRampRelease_CurrentIsRefused(t *testing.T) {
	_, err := ramp(releaseService(rolloutReleases(), releaseFleet()), "v1", 10)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "release v1 is current") {
		t.Fatalf("err = %v", err)
	}
}

func TestRampRelease_ZeroStopsTheRampWithoutJudging(t *testing.T) {
	stub := rolloutReleases()
	stub.obs.Routing = engine.Routing{Current: "v1", Ramping: "v2", RampPercent: 10}
	stub.obs.EngineReplicas["v2"] = 0 // a blocker, which stopping a ramp ignores
	resp, err := ramp(releaseService(stub, releaseFleet()), "v2", 0)
	if err != nil {
		t.Fatalf("ramp 0: %v", err)
	}
	if w := stub.writes(); len(w) != 1 || w[0] != (engine.Routing{Current: "v1"}) {
		t.Fatalf("writes = %v", w)
	}
	if !resp.GetChanged() || resp.GetPreviousBuildId() != "v2" || resp.GetPreviousPercent() != 10 {
		t.Fatalf("resp = %v", resp)
	}
}

func TestRampRelease_ZeroOnANonRampingReleaseIsRefused(t *testing.T) {
	stub := rolloutReleases()
	_, err := ramp(releaseService(stub, releaseFleet()), "v2", 0)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "release v2 is not ramping") {
		t.Fatalf("err = %v", err)
	}
	if len(stub.writes()) != 0 {
		t.Fatal("wrote")
	}
}

func TestRampRelease_SamePercentIsANoOp(t *testing.T) {
	stub := rolloutReleases()
	stub.obs.Routing = engine.Routing{Current: "v1", Ramping: "v2", RampPercent: 10}
	resp, err := ramp(releaseService(stub, releaseFleet()), "v2", 10)
	if err != nil || resp.GetChanged() {
		t.Fatalf("resp = %v, err = %v", resp, err)
	}
	if len(stub.writes()) != 0 {
		t.Fatal("wrote")
	}
}

func TestRampRelease_AboveZeroRefusesOnBlockers(t *testing.T) {
	stub := rolloutReleases()
	stub.obs.EngineReplicas["v2"] = 0
	_, err := ramp(releaseService(stub, releaseFleet()), "v2", 10)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "release v2 cannot take new runs yet:") {
		t.Fatalf("err = %v", err)
	}
	if len(stub.writes()) != 0 {
		t.Fatal("wrote")
	}
	stub.obs.EngineReplicas["v2"] = 1
	resp, err := ramp(releaseService(stub, releaseFleet()), "v2", 10)
	if err != nil || !resp.GetChanged() {
		t.Fatalf("passing ramp: %v, %v", resp, err)
	}
	if w := stub.writes(); len(w) != 1 || w[0] != (engine.Routing{Current: "v1", Ramping: "v2", RampPercent: 10}) {
		t.Fatalf("writes = %v", w)
	}
}

func TestAudit_ReleaseWritesAreClassified(t *testing.T) {
	stub := rolloutReleases()
	f := newAuditFixture(t, auditOptions{fleet: releaseFleet(), releases: stub})
	ctx := context.Background()
	system := orkstackv1connect.NewSystemServiceClient(f.server.Client(), f.server.URL)

	if _, err := system.ListReleases(ctx, connect.NewRequest(&orkstackv1.ListReleasesRequest{})); err != nil {
		t.Fatalf("ListReleases: %v", err)
	}
	if records := f.records(t); len(records) != 0 {
		t.Fatalf("a read was recorded: %v", records)
	}
	if _, err := system.PromoteRelease(ctx, connect.NewRequest(&orkstackv1.PromoteReleaseRequest{BuildId: "v2"})); err != nil {
		t.Fatalf("PromoteRelease: %v", err)
	}
	f.now = f.now.Add(time.Second)
	if _, err := system.PromoteRelease(ctx, connect.NewRequest(&orkstackv1.PromoteReleaseRequest{BuildId: "v9"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("PromoteRelease(v9) = %v", err)
	}
	f.now = f.now.Add(time.Second)
	if _, err := system.RampRelease(ctx, connect.NewRequest(&orkstackv1.RampReleaseRequest{BuildId: "v1", Percent: 10})); err != nil {
		t.Fatalf("RampRelease: %v", err)
	}
	records := f.records(t)
	if len(records) != 3 {
		t.Fatalf("records = %v", records)
	}
	rampRec, refused, promoted := records[0], records[1], records[2]
	if promoted.GetAction() != "release.promote" || promoted.GetTarget() != "v2" || promoted.GetNamespace() != "" ||
		!promoted.GetSucceeded() || promoted.GetDetail() != "current v1 -> v2" {
		t.Fatalf("promote record = %v", promoted)
	}
	if refused.GetAction() != "release.promote" || refused.GetSucceeded() || refused.GetDetail() != "refused" ||
		refused.GetErrorCode() != "not_found" {
		t.Fatalf("refused record = %v", refused)
	}
	if rampRec.GetAction() != "release.ramp" || rampRec.GetTarget() != "v1" || rampRec.GetNamespace() != "" ||
		rampRec.GetDetail() != "percent=10 was -@0" {
		t.Fatalf("ramp record = %v", rampRec)
	}
}
