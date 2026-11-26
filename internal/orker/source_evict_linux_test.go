package orker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestSourceEvict_Cordons: an operator's cordon evicts what an Orker hosts and its
// clearing evicts nothing; a store outage, and a stuck service, keep it.
func TestSourceEvict_Cordons(t *testing.T) {
	for name, tc := range map[string]struct {
		during func(*testing.T, *hostRig)
		reason string
	}{
		"an operator's": {func(t *testing.T, r *hostRig) {
			_, err := r.a.CordonOrker(t.Context(), &orkstackv1.CordonOrkerInput{Cordoned: true, Reason: "the rack"})
			must.OK(t, err)
		}, "ORKER_CORDONED"},
		"its clearing": {func(t *testing.T, r *hostRig) {
			_, err := r.a.CordonOrker(t.Context(), &orkstackv1.CordonOrkerInput{})
			must.OK(t, err)
		}, ""},
		"a store outage":  {func(_ *testing.T, r *hostRig) { r.a.cordon.SetStoreOutage("store", time.Now()) }, ""},
		"a stuck service": {func(_ *testing.T, r *hostRig) { r.a.keepStuckService("c9", func() {}) }, ""},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := newHostRig(t)
				g := r.start()
				time.Sleep(3 * time.Second)
				tc.during(t, r)
				time.Sleep(time.Minute)
				if tc.reason != "" {
					g.wantEnd(tc.reason)
					return
				}
				if n := r.a.services.hostedNow(); n != 1 {
					t.Errorf("%d hosted", n)
				}
				if e := g.stopped(); !e.Cancelled {
					t.Errorf("%+v", e)
				}
			})
		})
	}
}

// TestSourceEvict_AReachProbe: one answer that is not clean evicts nothing, a second
// evicts everything, and a probe that could not be run keeps it for as long as its
// last clean answer holds.
func TestSourceEvict_AReachProbe(t *testing.T) {
	unclean := servicehost.ProbeResult{Ran: true, Fitness: servicehost.Fitness{Failures: []string{"question 2: 127.0.0.1:22"}}}
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		g := r.start()
		time.Sleep(3 * time.Second)
		for range 3 {
			r.a.probed(servicehost.ProbeResult{Err: "no daemon"}, time.Now())
			time.Sleep(2 * time.Minute)
		}
		r.a.probed(unclean, time.Now())
		time.Sleep(time.Second)
		if n := r.a.services.hostedNow(); n != 1 {
			t.Fatalf("%d hosted", n)
		}
		r.a.probed(unclean, time.Now())
		g.wantEnd("SERVICE_HOST_UNFIT")
	})
}

// TestSourceEvict_StuckAtBoot: a service's container that the boot could not remove
// is a stuck service again, until it is gone; a task's is not.
func TestSourceEvict_StuckAtBoot(t *testing.T) {
	for name, tc := range map[string]struct {
		own   []OwnContainer
		lists error
		stuck bool
	}{
		"a service's":         {own: []OwnContainer{{ID: "c1", Source: "evs-0123456789abcdef"}}, stuck: true},
		"a task's":            {own: []OwnContainer{{ID: "c1", RunID: "ork-1", TaskID: "t", Attempt: "1"}}},
		"one nobody can name": {lists: errors.New("daemon"), stuck: true},
	} {
		t.Run(name, func(t *testing.T) {
			a := newCordonActivities(t, t.TempDir())
			a.sourceHost.MaxInstances = 1
			// One that keeps no claims releases none, whatever it lists.
			a.dispatcher.Claims = nil
			rt := &stayingRuntime{own: append(slices.Clone(tc.own), OwnContainer{ID: "c2", RunID: "ork-2", TaskID: "t", Attempt: "1"}), lists: tc.lists}
			a.dispatcher.Runtime = rt
			must.OK(t, a.reapContainers(t.Context()))
			if stuck := a.cordon.ServiceStuck() != nil; stuck != tc.stuck {
				t.Fatalf("stuck %v", stuck)
			}
			rt.goes = true
			a.retryRemovals(t.Context())
			if a.cordon.ServiceStuck() != nil {
				t.Error("stuck though it is gone")
			}
		})
	}
}

// stayingRuntime is a runtime whose boot reap leaves one container, as a daemon that
// is wedged does.
type stayingRuntime struct {
	phaseCapturingRuntime
	own   []OwnContainer
	lists error
	goes  bool
}

func (r *stayingRuntime) OwnContainers(context.Context, string) ([]OwnContainer, error) {
	return r.own, r.lists
}

func (r *stayingRuntime) Reap(context.Context, string) (int, error) {
	return 0, &ReapIncompleteError{Failures: []error{errors.New("busy")}, Containers: []ContainerID{"c1"}}
}

func (r *stayingRuntime) Remove(context.Context, ContainerID) error {
	if r.goes {
		return nil
	}
	return errors.New("busy")
}

// TestParseOwnContainers_AService: a service's container is listed with its source.
func TestParseOwnContainers_AService(t *testing.T) {
	own := parseOwnContainers("c1\t\t\t\t\tevs-0123456789abcdef\nc2\tork-1\tt\t3\t1\t\nc3\tork-1\nc4\t\t\t\t\nc5\t\t\t\t\t\t\n")
	if len(own) != 2 || own[0].Source != "evs-0123456789abcdef" || own[1].Source != "" || own[1].Attempt != "1" {
		t.Errorf("%+v", own)
	}
	if n := strings.Count(ownContainersFormat, "\t"); n != 5 || !strings.Contains(ownContainersFormat, `"ork.source"`) {
		t.Errorf("%q", ownContainersFormat)
	}
}

type whoAnswers struct {
	mu    sync.Mutex
	asked int
	who   *orkstackv1.GetSigninResponse
	err   error
}

func (w *whoAnswers) Whoami(context.Context) (*orkstackv1.GetSigninResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.asked++
	return w.who, w.err
}

// answers sets what it answers from now on, and counts afresh.
func (w *whoAnswers) answers(who *orkstackv1.GetSigninResponse, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.asked, w.who, w.err = 0, who, err
}

func (w *whoAnswers) times() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.asked
}

// TestHostPermission: three attempts in a row refused a permission make an Orker
// unfit, keeping what it hosts; every two minutes it asks what its credential holds,
// and is fit again when that is all it needs, at its own pool, under its own id.
func TestHostPermission(t *testing.T) {
	granted := func(scope, orker string, perms ...string) *orkstackv1.GetSigninResponse {
		return &orkstackv1.GetSigninResponse{OrkerId: orker, Permissions: []*orkstackv1.ScopedPermissions{{Scope: scope, Permissions: perms}}}
	}
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		kept := r.start()
		time.Sleep(3 * time.Second)
		refuse := func() {
			r.engine.mu.Lock()
			r.engine.confirm = func(int) (*orkstackv1.DoorAnswer, error) {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("SOURCE_HOST_NOT_PERMITTED: eventsource.host at pool main"))
			}
			r.engine.mu.Unlock()
			r.start().wantDenied(t)
			r.engine.mu.Lock()
			r.engine.confirm = nil
			r.engine.mu.Unlock()
		}
		refuse()
		refuse()
		if len(r.a.unfitToHost()) != 0 {
			t.Fatal("unfit after two")
		}
		// One that ends otherwise ends the row.
		r.engine.mu.Lock()
		r.engine.confirm = stopped
		r.engine.mu.Unlock()
		if e := <-r.start().done; e.Reason != "EVENT_SOURCE_PAUSED" {
			t.Fatalf("%+v", e)
		}
		r.engine.mu.Lock()
		r.engine.confirm = nil
		r.engine.mu.Unlock()
		refuse()
		refuse()
		if len(r.a.unfitToHost()) != 0 {
			t.Fatal("unfit after two in a row")
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		who := &whoAnswers{who: granted("pool:main", "orker-1", "eventsource.host")}
		go r.a.RunHostPermissionCheck(ctx, who)
		time.Sleep(5 * time.Minute)
		if n := who.times(); n != 0 {
			t.Errorf("asked %d times while nothing refuses it", n)
		}
		refuse()
		unfit := r.a.unfitToHost()
		if len(unfit) != 1 || !strings.Contains(unfit[0], "eventsource.host") || !strings.Contains(unfit[0], `"main"`) {
			t.Fatalf("unfit %v", unfit)
		}
		if f := r.a.mayHost(); f == nil || f.Reason != "SERVICE_HOST_UNFIT" || r.a.services.hostedNow() != 1 {
			t.Errorf("%v, %d hosted", f, r.a.services.hostedNow())
		}
		for _, w := range []struct {
			who  *orkstackv1.GetSigninResponse
			err  error
			says string
		}{
			{err: errors.New("dial"), says: "eventsource.host"},
			{who: granted("pool:gpu", "orker-1", "eventsource.host"), says: "eventsource.host"},
			{who: granted("pool:main", "orker-2", "eventsource.host"), says: `names Orker "orker-2"`},
			{who: granted("pool:main", "orker-1", "identity.token.mint"), says: "eventsource.host"},
			{who: granted("pool:main", "", "eventsource.host"), says: `names Orker ""`},
		} {
			who.answers(w.who, w.err)
			time.Sleep(2*time.Minute + time.Second)
			if unfit := r.a.unfitToHost(); who.times() != 1 || len(unfit) != 1 || !strings.Contains(unfit[0], w.says) {
				t.Errorf("%v: asked %d, unfit %v", w.who, who.times(), unfit)
			}
		}
		// What it hosted ends for a reason of its own, which says nothing of the
		// permission.
		kept.cancel(context.Canceled)
		<-kept.done
		if len(r.a.unfitToHost()) != 1 {
			t.Errorf("fit though nothing found the permission: %v", r.a.unfitToHost())
		}
		who.answers(granted("deployment", "orker-1", "eventsource.host"), nil)
		time.Sleep(2*time.Minute + time.Second)
		if len(r.a.unfitToHost()) != 0 {
			t.Errorf("unfit though it holds it: %v", r.a.unfitToHost())
		}
		r.rt.once.Do(func() { close(r.rt.done) })
	})
}

// wantDenied waits for an attempt that ends refused.
func (g *hosting) wantDenied(t *testing.T) {
	t.Helper()
	if e := <-g.done; e.Reason != "SOURCE_HOST_NOT_PERMITTED" {
		t.Fatalf("%+v", e)
	}
}

// TestReachImage_IsHeldToTheDeploymentsPolicy: the probe's image is not judged before
// the engine has said what the policy is, to the Orker's own question or in a
// confirmation; from then it must be one the policy permits, whoever signed it, and where it is not the Orker says which and evicts
// nothing.
func TestReachImage_IsHeldToTheDeploymentsPolicy(t *testing.T) {
	const image = "ghcr.io/orkstack/orker@sha256:abababababababababababababababababababababababababababababababab"
	var s serviceHostState
	if why := s.refusedByPolicy(image, false); why != "" {
		t.Errorf("unjudged: %s", why)
	}
	for name, tc := range map[string]struct {
		policy *orkstackv1.ImagePolicySelection
		image  string
		want   string
	}{
		"one that permits it":        {&orkstackv1.ImagePolicySelection{RequireDigest: true, AllowedRegistries: []string{"ghcr.io/orkstack"}}, image, ""},
		"one that permits anything":  {&orkstackv1.ImagePolicySelection{}, image, ""},
		"a signer it is not held to": {&orkstackv1.ImagePolicySelection{RequireSignedBy: "cosign:release"}, image, ""},
		"another registry":           {&orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"registry.example"}}, image, "allowed_registries"},
		"a digest required":          {&orkstackv1.ImagePolicySelection{RequireDigest: true}, "ghcr.io/orkstack/orker:1", "require_digest"},
		"no reference":               {&orkstackv1.ImagePolicySelection{}, "@@", "no image reference"},
	} {
		s.setPolicy(tc.policy)
		if why := s.refusedByPolicy(tc.image, false); (why == "") != (tc.want == "") || !strings.Contains(why, tc.want) || why != "" && !strings.Contains(why, tc.image) {
			t.Errorf("%s: %q", name, why)
		}
	}
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.engine.policy = &orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme"}}
		g := r.start()
		time.Sleep(3 * time.Second)
		why := r.a.services.refusedByPolicy(image, false)
		if why == "" {
			t.Error("what the confirmation said is not known")
		}
		r.a.probed(servicehost.ProbeResult{ImageRefused: true, Err: why}, time.Now())
		time.Sleep(time.Minute)
		if unfit := r.a.unfitToHost(); len(unfit) != 1 || !strings.Contains(unfit[0], image) || r.a.services.hostedNow() != 1 {
			t.Errorf("unfit %v, %d hosted", unfit, r.a.services.hostedNow())
		}
		// Its next confirmation, at five minutes, says the same; what it hosts is
		// kept, as of any probe that could not be run.
		time.Sleep(6 * time.Minute)
		if why := r.a.services.refusedByPolicy(image, false); why == "" || r.a.services.hostedNow() != 1 {
			t.Errorf("%q, %d hosted", why, r.a.services.hostedNow())
		}
		g.stopped()
	})
}

// TestReachImage_TheEngineIsAskedBeforeEachProbe: the policy is the engine's answer to
// the Orker's own question before its image is used, the first included. An image it
// refuses stays refused until an answer says otherwise: one that did not come, and one
// that does not say what the policy is, change nothing of what was known.
func TestReachImage_TheEngineIsAskedBeforeEachProbe(t *testing.T) {
	const image = "registry.example/orker@sha256:abababababababababababababababababababababababababababababababab"
	a := NewActivitiesWithOptions(nil, Options{Runtime: &viewProbeRuntime{label: "c0ffee"}, WorkRoot: t.TempDir(), Build: "c0ffee",
		SourceHost: SourceHostSettings{MaxInstances: 1, ProbeImage: image}})
	who := &whoAnswers{}
	a.engine = who
	other, none, ours := &orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme"}}, &orkstackv1.ImagePolicySelection{},
		&orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"registry.example"}, RequireDigest: true}
	for i, step := range []struct {
		what    string
		policy  *orkstackv1.ImagePolicySelection
		err     error
		refused bool
	}{
		{"nothing known, and no answer", nil, errors.New("dial"), false},
		{"nothing known, and none said", nil, nil, false},
		{"refused", other, nil, true},
		{"no answer since", nil, errors.New("dial"), true},
		{"none said since", nil, nil, true},
		{"permitted", ours, nil, false},
		{"refused again", other, nil, true},
		{"the deployment has none now", none, nil, false},
	} {
		who.answers(&orkstackv1.GetSigninResponse{OrkerId: "orker-1", ImagePolicy: step.policy}, step.err)
		_, refused, err := a.resolveReachImage(t.Context())
		if err != nil || (refused != "") != step.refused || who.times() != 1 {
			t.Errorf("%d, %s: %q, %v, asked %d times", i, step.what, refused, err, who.times())
		}
	}
}

// TestReachImage_IsTheOneJudged: the image the probe would run is the one held to the
// policy.
func TestReachImage_IsTheOneJudged(t *testing.T) {
	const image = "registry.example/orker@sha256:abababababababababababababababababababababababababababababababab"
	a := NewActivitiesWithOptions(nil, Options{Runtime: &viewProbeRuntime{label: "c0ffee"}, WorkRoot: t.TempDir(), Build: "c0ffee",
		SourceHost: SourceHostSettings{MaxInstances: 1, ProbeImage: image}})
	got, refused, err := a.resolveReachImage(t.Context())
	if err != nil || refused != "" || got != image {
		t.Fatalf("%q, %q, %v", got, refused, err)
	}
	a.services.setPolicy(&orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme"}})
	if got, refused, err = a.resolveReachImage(t.Context()); err != nil || got != "" || !strings.Contains(refused, image) {
		t.Fatalf("%q, %q, %v", got, refused, err)
	}
}

// TestReachImage_ARefusedOneIsNotAskedFor: an image the policy does not permit, the
// release's own by its tag included, is refused before its registry is asked or
// anything of it is pulled; a release's, which is run by the digest it resolves to,
// meets a policy that requires one.
func TestReachImage_ARefusedOneIsNotAskedFor(t *testing.T) {
	const digest = "sha256:abababababababababababababababababababababababababababababababab"
	for name, tc := range map[string]struct {
		policy  *orkstackv1.ImagePolicySelection
		pinned  string
		refused bool
		asks    int
	}{
		"another registry":            {&orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"registry.example"}}, "ghcr.io/orkstack/orker@" + digest, true, 0},
		"a digest required":           {&orkstackv1.ImagePolicySelection{RequireDigest: true, AllowedRegistries: []string{"ghcr.io/orkstack"}}, "ghcr.io/orkstack/orker@" + digest, false, 1},
		"one that resolves elsewhere": {&orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/orkstack"}}, "registry.example/orker@" + digest, true, 1},
	} {
		asked := 0
		release := &ReleaseProbe{Ref: "ghcr.io/orkstack/orker:1", KeyPEM: "k",
			Resolve: func(context.Context, string) (*orkstackv1.ImageRef, error) {
				asked++
				return &orkstackv1.ImageRef{Reference: "ghcr.io/orkstack/orker:1", Registry: "ghcr.io", Repository: "orkstack/orker", IndexDigest: digest, Pinned: tc.pinned}, nil
			},
			Verify: func(context.Context, *orkstackv1.ImageRef, string, string) error { return nil }}
		a := NewActivitiesWithOptions(nil, Options{Runtime: &viewProbeRuntime{label: "c0ffee"}, WorkRoot: t.TempDir(), Build: "c0ffee", ReleaseProbe: release,
			SourceHost: SourceHostSettings{MaxInstances: 1}})
		a.services.setPolicy(tc.policy)
		image, refused, err := a.resolveReachImage(t.Context())
		if err != nil || (refused != "") != tc.refused || asked != tc.asks || (image == "") != tc.refused {
			t.Errorf("%s: %q, %q, %v, resolved %d times", name, image, refused, err, asked)
		}
	}
}
