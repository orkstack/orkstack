package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// releaseForbidden is AGENTS.md §7.1 as a pattern: nothing a release surface
// emits may name a channel, a session, or the cluster's own objects.
var releaseForbidden = regexp.MustCompile(
	`ork-pool-|ork-tasks|ork-engine-events|ork-orker|__internal|queue|session|Temporal|worker deployment|search attribute|TemporalWorkerDeploymentVersion`)

type judgeCase struct {
	name      string
	target    string
	obs       engine.ReleaseObservation
	inventory []*orkstackv1.Orker
	invErr    error
	runs      map[string]releaseRunCount
	running   int64
	runErr    error
	want      []*orkstackv1.ReleaseBlocker
}

func row(id, pool, build string) *orkstackv1.Orker {
	return &orkstackv1.Orker{OrkerId: id, Pool: pool, BuildId: build}
}

func cordoned(o *orkstackv1.Orker, reason string) *orkstackv1.Orker {
	o.Cordoned, o.CordonReason = true, reason
	return o
}

func pinHost(o *orkstackv1.Orker) *orkstackv1.Orker {
	o.MaxConcurrentPins = 1
	return o
}

func unreachable(id string) *orkstackv1.Orker {
	return &orkstackv1.Orker{OrkerId: id, UnavailableReason: "describe orker: context deadline exceeded"}
}

// baseObs is v1 current (engine 2, registered main and build), v2 a candidate
// with one engine.
func baseObs() engine.ReleaseObservation {
	return engine.ReleaseObservation{
		DeploymentName: "orkstack",
		Routing:        engine.Routing{Current: "v1"},
		Releases: []engine.ObservedRelease{
			{BuildID: "v1", HasServed: true, RegisteredPools: []string{"build", "main"}},
			{BuildID: "v2"},
		},
		EngineReplicas: map[string]int{"v1": 2, "v2": 1},
		PoolUse:        map[string]engine.PoolUse{"build": {}, "main": {}},
	}
}

func blocker(kind orkstackv1.ReleaseBlockerKind, pool, message string) *orkstackv1.ReleaseBlocker {
	return &orkstackv1.ReleaseBlocker{Kind: kind, Pool: pool, Message: message}
}

const (
	kNoEngine    = orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_NO_ENGINE
	kNotServed   = orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_POOL_NOT_SERVED
	kPins        = orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_PINS_NOT_HOSTED
	kUnversioned = orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_UNVERSIONED_RUNS
	kUnverified  = orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_UNVERIFIED
)

func notServed(pool, evidence, target string) *orkstackv1.ReleaseBlocker {
	return blocker(kNotServed, pool, fmt.Sprintf(
		"pool %q is in use (%s) and no Orker of release %s serves it, so tasks its runs send there would not run on its own Orkers. Start one with ORK_BUILD_ID=%s ORK_ORKER_POOL=%s.",
		pool, evidence, target, target, pool))
}

func judgeCases() []judgeCase {
	withObs := func(edit func(*engine.ReleaseObservation)) engine.ReleaseObservation {
		obs := baseObs()
		edit(&obs)
		return obs
	}
	served := []*orkstackv1.Orker{row("orker-v2a", "main", "v2"), row("orker-v2b", "build", "v2")}
	return []judgeCase{
		{
			name:   "no engine",
			target: "v2",
			obs:    withObs(func(o *engine.ReleaseObservation) { o.EngineReplicas["v2"] = 0 }),
			want: []*orkstackv1.ReleaseBlocker{blocker(kNoEngine, "",
				"no engine of release v2 is running, so a run sent to it would never start. Start ork with ORK_BUILD_ID=v2 and ORK_DEPLOYMENT_NAME=orkstack.")},
		},
		{
			name:      "a pool in use by another release's Orker",
			target:    "v2",
			obs:       baseObs(),
			inventory: []*orkstackv1.Orker{row("orker-b45e", "build", "v1"), row("orker-v2a", "main", "v2")},
			want:      []*orkstackv1.ReleaseBlocker{notServed("build", "orker-b45e of release v1 serves it", "v2")},
		},
		{
			name:      "a pool in use by an unversioned Orker",
			target:    "v2",
			obs:       baseObs(),
			inventory: []*orkstackv1.Orker{row("orker-d002", "gpu", ""), row("orker-v2a", "main", "v2"), row("orker-v2b", "build", "v2")},
			want:      []*orkstackv1.ReleaseBlocker{notServed("gpu", "orker-d002, started without ORK_BUILD_ID, serves it", "v2")},
		},
		{
			name:   "a pool in use by a cordoned Orker of another release",
			target: "v2",
			obs:    baseObs(),
			inventory: []*orkstackv1.Orker{cordoned(row("orker-b45e", "build", "v1"), "patching"),
				row("orker-v2a", "main", "v2")},
			want: []*orkstackv1.ReleaseBlocker{notServed("build", "orker-b45e of release v1 serves it (cordoned)", "v2")},
		},
		{
			name:   "the current release's registration with backlog only",
			target: "v2",
			obs: withObs(func(o *engine.ReleaseObservation) {
				o.PoolUse["build"] = engine.PoolUse{Backlog: 3}
			}),
			inventory: []*orkstackv1.Orker{row("orker-v2a", "main", "v2")},
			want:      []*orkstackv1.ReleaseBlocker{notServed("build", "3 tasks are waiting there", "v2")},
		},
		{
			name:   "the current release's registration with add rate only",
			target: "v2",
			obs: withObs(func(o *engine.ReleaseObservation) {
				o.PoolUse["build"] = engine.PoolUse{AddRate: 0.2}
			}),
			inventory: []*orkstackv1.Orker{row("orker-v2a", "main", "v2")},
			want:      []*orkstackv1.ReleaseBlocker{notServed("build", "it received work in the last minute", "v2")},
		},
		{
			name:   "a poller the inventory cannot show",
			target: "v2",
			obs: withObs(func(o *engine.ReleaseObservation) {
				o.PoolUse["build"] = engine.PoolUse{LivePollers: 1}
			}),
			inventory: []*orkstackv1.Orker{row("orker-v2a", "main", "v2")},
			want: []*orkstackv1.ReleaseBlocker{notServed("build",
				"an Orker this list cannot show is polling it (one started before Orkers could be listed)", "v2")},
		},
		{
			name:      "a registered pool with nothing is not in use",
			target:    "v2",
			obs:       baseObs(),
			inventory: []*orkstackv1.Orker{row("orker-v2a", "main", "v2")},
			want:      nil,
		},
		{
			name:   "the target's only Orker in the pool is cordoned",
			target: "v2",
			obs:    baseObs(),
			inventory: []*orkstackv1.Orker{row("orker-a1", "main", "v1"),
				cordoned(row("orker-v2a", "main", "v2"), "kernel upgrade")},
			want: []*orkstackv1.ReleaseBlocker{blocker(kNotServed, "main",
				`pool "main" is in use (orker-a1 of release v1 serves it) and no uncordoned Orker of release v2 serves it (orker-v2a of release v2 is cordoned: "kernel upgrade"), so tasks its runs send there would not run on its own Orkers. Start one with ORK_BUILD_ID=v2 ORK_ORKER_POOL=main.`)},
		},
		{
			name:      "pin host missing",
			target:    "v2",
			obs:       baseObs(),
			inventory: []*orkstackv1.Orker{pinHost(row("orker-7f3a", "main", "v1")), row("orker-v2a", "main", "v2")},
			want: []*orkstackv1.ReleaseBlocker{blocker(kPins, "main",
				`pool "main" hosts pins (orker-7f3a of release v1 is a pin host) and no Orker of release v2 in it hosts pins, so a pin its runs open there would open on another release's Orker. Start one with ORK_BUILD_ID=v2 ORK_ORKER_POOL=main ORK_ORKER_MAX_PINS=1 (or more; set the same value on every Orker of the pool).`)},
		},
		{
			name:   "pin host present passes",
			target: "v2",
			obs:    baseObs(),
			inventory: []*orkstackv1.Orker{pinHost(row("orker-7f3a", "main", "v1")),
				pinHost(row("orker-v2a", "main", "v2"))},
			want: nil,
		},
		{
			name:      "inventory error is one UNVERIFIED and no pool blockers",
			target:    "v2",
			obs:       withObs(func(o *engine.ReleaseObservation) { o.PoolUse["build"] = engine.PoolUse{Backlog: 3} }),
			invErr:    errors.New("presence channel down"),
			inventory: nil,
			want: []*orkstackv1.ReleaseBlocker{blocker(kUnverified, "",
				"the Orker list could not be read, so whether release v2 serves every pool in use cannot be established; ask again.")},
		},
		{
			name:      "an unreachable row is UNVERIFIED and the pools are still judged",
			target:    "v2",
			obs:       baseObs(),
			inventory: []*orkstackv1.Orker{unreachable("orker-c001"), row("orker-b45e", "build", "v1"), row("orker-v2a", "main", "v2")},
			want: []*orkstackv1.ReleaseBlocker{
				blocker(kUnverified, "",
					"1 Orker(s) did not answer (orker-c001), so their release and pool are unknown; an Orker that stopped leaves the list within 90 seconds — ask again."),
				notServed("build", "orker-b45e of release v1 serves it", "v2"),
			},
		},
		{
			name:      "a pool use error is UNVERIFIED with the pool",
			target:    "v2",
			obs:       withObs(func(o *engine.ReleaseObservation) { o.PoolUse["build"] = engine.PoolUse{Err: errors.New("x")} }),
			inventory: []*orkstackv1.Orker{row("orker-v2a", "main", "v2")},
			want: []*orkstackv1.ReleaseBlocker{blocker(kUnverified, "build",
				`whether pool "build" has work waiting could not be read; ask again.`)},
		},
		{
			name:   "the current release's describe error is UNVERIFIED",
			target: "v2",
			obs: withObs(func(o *engine.ReleaseObservation) {
				o.Releases[0].DescribeErr = errors.New("x")
				o.Releases[0].RegisteredPools = nil
			}),
			inventory: served,
			want: []*orkstackv1.ReleaseBlocker{blocker(kUnverified, "",
				"the pools release v1 served could not be read; ask again.")},
		},
		{
			name:   "no current release and two unversioned runs",
			target: "v1",
			obs: withObs(func(o *engine.ReleaseObservation) {
				o.Routing = engine.Routing{}
				o.Releases[0].HasServed = false
			}),
			inventory: []*orkstackv1.Orker{row("orker-a1", "main", "v1")},
			runs:      map[string]releaseRunCount{"v1": {}, "v2": {}},
			running:   2,
			want: []*orkstackv1.ReleaseBlocker{blocker(kUnversioned, "",
				`2 run(s) started before any release was current are still executing; making release v1 take new runs moves them onto it at their next step, which fails them if its code differs from the build that started them. Let them finish first — "orkctl namespace update NS --hold REASON" stops new ones meanwhile (docs/DEPLOY.md §3.5).`)},
		},
		{
			name:   "no current release and a count error",
			target: "v1",
			obs: withObs(func(o *engine.ReleaseObservation) {
				o.Routing = engine.Routing{}
			}),
			inventory: []*orkstackv1.Orker{row("orker-a1", "main", "v1")},
			runs:      map[string]releaseRunCount{"v1": {}, "v2": {err: errors.New("index down")}},
			running:   2,
			want: []*orkstackv1.ReleaseBlocker{blocker(kUnverified, "",
				"whether runs started before versioning are still executing could not be counted; ask again.")},
		},
		{
			name:      "the current release is never judged",
			target:    "v1",
			obs:       withObs(func(o *engine.ReleaseObservation) { o.EngineReplicas["v1"] = 0 }),
			inventory: []*orkstackv1.Orker{row("orker-d002", "gpu", "")},
			want:      nil,
		},
		{
			name:   "order is stable: engine, inventory, pools, pins",
			target: "v2",
			obs:    withObs(func(o *engine.ReleaseObservation) { o.EngineReplicas["v2"] = 0 }),
			inventory: []*orkstackv1.Orker{unreachable("orker-c001"), row("orker-z", "zeta", "v1"),
				row("orker-b45e", "build", "v1"), pinHost(row("orker-7f3a", "main", "v1")), row("orker-v2a", "main", "v2")},
			want: []*orkstackv1.ReleaseBlocker{
				blocker(kNoEngine, "", "no engine of release v2 is running, so a run sent to it would never start. Start ork with ORK_BUILD_ID=v2 and ORK_DEPLOYMENT_NAME=orkstack."),
				blocker(kUnverified, "", "1 Orker(s) did not answer (orker-c001), so their release and pool are unknown; an Orker that stopped leaves the list within 90 seconds — ask again."),
				notServed("build", "orker-b45e of release v1 serves it", "v2"),
				notServed("zeta", "orker-z of release v1 serves it", "v2"),
				blocker(kPins, "main", `pool "main" hosts pins (orker-7f3a of release v1 is a pin host) and no Orker of release v2 in it hosts pins, so a pin its runs open there would open on another release's Orker. Start one with ORK_BUILD_ID=v2 ORK_ORKER_POOL=main ORK_ORKER_MAX_PINS=1 (or more; set the same value on every Orker of the pool).`),
			},
		},
	}
}

func TestJudgeRelease_Table(t *testing.T) {
	for _, tc := range judgeCases() {
		t.Run(tc.name, func(t *testing.T) {
			got := judgeRelease(tc.target, tc.obs, tc.inventory, tc.invErr, tc.runs, tc.running, tc.runErr)
			if len(got) != len(tc.want) {
				t.Fatalf("blockers = %v\nwant %v", got, tc.want)
			}
			for i := range got {
				if got[i].GetKind() != tc.want[i].GetKind() || got[i].GetPool() != tc.want[i].GetPool() ||
					got[i].GetMessage() != tc.want[i].GetMessage() {
					t.Errorf("blocker %d =\n  %v\nwant\n  %v", i, got[i], tc.want[i])
				}
			}
			// Pure: the same inputs give the same blockers.
			again := judgeRelease(tc.target, tc.obs, tc.inventory, tc.invErr, tc.runs, tc.running, tc.runErr)
			if fmt.Sprint(again) != fmt.Sprint(got) {
				t.Fatalf("not deterministic:\n%v\n%v", got, again)
			}
		})
	}
}

func TestReleaseBlockers_NeverNameAChannel(t *testing.T) {
	seen := 0
	for _, tc := range judgeCases() {
		for _, b := range judgeRelease(tc.target, tc.obs, tc.inventory, tc.invErr, tc.runs, tc.running, tc.runErr) {
			seen++
			if releaseForbidden.MatchString(b.GetMessage()) {
				t.Errorf("%s: %q names %q", tc.name, b.GetMessage(), releaseForbidden.FindString(b.GetMessage()))
			}
		}
	}
	if seen < 10 {
		t.Fatalf("only %d messages were checked", seen)
	}
}

func TestReleaseMessages_NeverNameTheCluster(t *testing.T) {
	text := `task queue "ork-pool-build" missing for TemporalWorkerDeploymentVersion="orkstack:v2"`
	cases := []struct {
		err   error
		write bool
		code  connect.Code
	}{
		{serviceerror.NewPermissionDenied(text, "worker deployment"), false, connect.CodePermissionDenied},
		{serviceerror.NewPermissionDenied(text, "worker deployment"), true, connect.CodePermissionDenied},
		{serviceerror.NewFailedPrecondition(text), true, connect.CodeFailedPrecondition},
		{serviceerror.NewNotFound(text), true, connect.CodeNotFound},
		{serviceerror.NewDeadlineExceeded(text), true, connect.CodeUnavailable},
		{serviceerror.NewUnavailable(text), true, connect.CodeUnavailable},
		{serviceerror.NewCanceled(text), true, connect.CodeUnavailable},
		{serviceerror.NewDeadlineExceeded(text), false, connect.CodeUnavailable},
		{serviceerror.NewUnavailable(text), false, connect.CodeUnavailable},
		{context.DeadlineExceeded, false, connect.CodeUnavailable},
		{serviceerror.NewInvalidArgument(text), true, connect.CodeInternal},
		{serviceerror.NewFailedPrecondition(text), false, connect.CodeInternal},
		{errors.New(text), false, connect.CodeInternal},
	}
	for _, tc := range cases {
		wrapped := &engine.ReleaseClusterError{Op: engine.ReleaseOpRead, What: "x", Cause: tc.err}
		got := releaseClusterError(wrapped, tc.write, "v2")
		if got.Code() != tc.code {
			t.Errorf("%T (write %v) = %v, want %v", tc.err, tc.write, got.Code(), tc.code)
		}
		if releaseForbidden.MatchString(got.Message()) {
			t.Errorf("%T: %q names %q", tc.err, got.Message(), releaseForbidden.FindString(got.Message()))
		}
	}
	for _, sentence := range []string{errNoReleases.Error(), releaseMsgRunsUnknown, inventorySentence(errNoFleet)} {
		if releaseForbidden.MatchString(sentence) {
			t.Errorf("%q names %q", sentence, releaseForbidden.FindString(sentence))
		}
	}
}
