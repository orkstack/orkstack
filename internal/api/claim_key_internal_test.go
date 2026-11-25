package api

import (
	"context"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"google.golang.org/protobuf/proto"
)

var fromTrigger = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg_1"}

// TestClaimCoordsOf: where a key is claimed in each mode, and that no two
// callers, namespaces, keys or kinds of caller share a coordinate.
func TestClaimCoordsOf(t *testing.T) {
	submit := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}
	as := func(id string) context.Context {
		return authn.WithPrincipal(context.Background(), &authn.Principal{ID: id})
	}
	coords := func(mode string, ctx context.Context, ns, key string, origin *orkstackv1.RunOrigin) claimCoords {
		t.Helper()
		s := &Service{access: &AccessControl{Mode: mode}}
		c, err := s.claimCoordsOf(ctx, ns, key, origin)
		if err != nil {
			t.Fatalf("%s %s %q: %v", mode, ns, key, err)
		}
		return c
	}
	alice := as("user:corp:alice")
	if c := coords(authn.ModeAsserted, alice, "payments", "k", submit); c != (claimCoords{own: "payments/k"}) {
		t.Errorf("asserted = %+v", c)
	}
	const owned = `{"v":1,"namespace":"payments","principal":"user:corp:alice","key":"k"}`
	if c := coords(authn.ModeEnforce, alice, "payments", "k", submit); c != (claimCoords{own: owned, other: "payments/k", otherShared: true}) {
		t.Errorf("enforce = %+v", c)
	}
	if c := coords(authn.ModeObserve, alice, "payments", "k", submit); c != (claimCoords{own: "payments/k", other: owned}) {
		t.Errorf("observe = %+v", c)
	}
	if c := coords(authn.ModeObserve, context.Background(), "payments", "k", submit); c != (claimCoords{own: "payments/k"}) {
		t.Errorf("observe with nobody verified = %+v", c)
	}
	if c := coords(authn.ModeEnforce, alice, "payments", "", submit); c != (claimCoords{}) {
		t.Errorf("no key = %+v", c)
	}
	// A trigger's key is the trigger's, whoever else the request names.
	if c := coords(authn.ModeEnforce, alice, "payments", "k", fromTrigger); c.own != `{"v":1,"namespace":"payments","trigger":"trg_1","key":"k"}` {
		t.Errorf("a trigger's = %+v", c)
	}
	seen := map[string]string{}
	for _, at := range []struct {
		ctx    context.Context
		ns     string
		key    string
		origin *orkstackv1.RunOrigin
	}{
		{alice, "payments", "k", submit},
		{as("user:corp:nina"), "payments", "k", submit},
		{alice, "staging", "k", submit},
		{alice, "payments", `k","principal":"user:corp:nina`, submit},
		{as(`user:corp:alice","key":"k`), "payments", "k", submit},
		{as("trg_1"), "payments", "k", submit},
		{alice, "payments", "k", fromTrigger},
		{alice, "payments", "k", &orkstackv1.RunOrigin{Kind: fromTrigger.GetKind(), Id: "trg_2"}},
	} {
		c := coords(authn.ModeEnforce, at.ctx, at.ns, at.key, at.origin)
		if prev, twice := seen[c.own]; twice {
			t.Errorf("%s and %s share %s", prev, at.ns+" "+at.key, c.own)
		}
		seen[c.own] = at.ns + " " + at.key
		if c.own[0] != '{' || c.own == c.other {
			t.Errorf("an owned coordinate %q reads as a shared one", c.own)
		}
	}
}

// TestModeGuard_OtherClaimsAlive: the other coordinate is read for one
// claim's life, the longest any engine advertises, after the deployment last
// entered or left enforce, and whenever the record does not say.
func TestModeGuard_OtherClaimsAlive(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	life := time.Hour + claimGrace
	guard := func(mode string, rec artifacts.ModeRecord) *ModeGuard {
		return &ModeGuard{Mode: mode, ClaimTTL: time.Hour, Now: func() time.Time { return now }, seen: true, record: rec}
	}
	enforce, observe := artifacts.AuthModeEnforce, artifacts.AuthModeObserve
	longer := guard(enforce, artifacts.ModeRecord{Mode: enforce, RaisedAt: now.Add(-life)})
	longer.peerTTL = 2 * time.Hour
	unread := guard(enforce, artifacts.ModeRecord{})
	unread.seen = false
	for name, tc := range map[string]struct {
		g    *ModeGuard
		mode string
		want bool
	}{
		"no guard":                        {nil, enforce, true},
		"a record not read":               {unread, enforce, true},
		"just raised":                     {guard(enforce, artifacts.ModeRecord{Mode: enforce, RaisedAt: now}), enforce, true},
		"a moment inside the life":        {guard(enforce, artifacts.ModeRecord{Mode: enforce, RaisedAt: now.Add(-life + time.Second)}), enforce, true},
		"one life after the raise":        {guard(enforce, artifacts.ModeRecord{Mode: enforce, RaisedAt: now.Add(-life)}), enforce, false},
		"a peer's claims live longer":     {longer, enforce, true},
		"lowered under an enforcing one":  {guard(enforce, artifacts.ModeRecord{Mode: observe, LeftEnforceAt: now.Add(-2 * life)}), enforce, true},
		"observe, just written":           {guard(observe, artifacts.ModeRecord{Mode: observe, RaisedAt: now}), observe, true},
		"observe, never enforced":         {guard(observe, artifacts.ModeRecord{Mode: observe, RaisedAt: now.Add(-life)}), observe, false},
		"observe, just left enforce":      {guard(observe, artifacts.ModeRecord{Mode: observe, LeftEnforceAt: now}), observe, true},
		"observe, one life after leaving": {guard(observe, artifacts.ModeRecord{Mode: observe, LeftEnforceAt: now.Add(-life)}), observe, false},
		"observe, a raise it has not had": {guard(observe, artifacts.ModeRecord{Mode: enforce, RaisedAt: now.Add(-2 * life)}), observe, true},
	} {
		if got := tc.g.otherClaimsAlive(tc.mode); got != tc.want {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}

// TestMayTakeSharedClaim_ATriggerTakesItsOwnAlone: a trigger holds no
// permission, so it is answered a shared claim only where the claim's own
// snapshot says this trigger made it.
func TestMayTakeSharedClaim_ATriggerTakesItsOwnAlone(t *testing.T) {
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus()})
	s.access, _ = accessFor(t, authn.ModeEnforce, nil)
	sub := submission{namespace: "payments", key: "k", wf: &orkstackv1.Orkflow{Name: "deploy"}, origin: fromTrigger}
	for name, tc := range map[string]struct {
		origin *orkstackv1.RunOrigin
		ok     bool
	}{
		"its own":           {&orkstackv1.RunOrigin{Kind: fromTrigger.GetKind(), Id: "trg_1"}, true},
		"another trigger's": {&orkstackv1.RunOrigin{Kind: fromTrigger.GetKind(), Id: "trg_2"}, false},
		"a person's":        {&orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT, Id: "trg_1"}, false},
		"nobody's":          {nil, false},
	} {
		snapshot, err := proto.Marshal(&orkstackv1.Run{Id: "r", Namespace: "payments", OrkflowName: "deploy", Origin: tc.origin})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.mayTakeSharedClaim(context.Background(), sub, engine.Claim{RunID: "r", Snapshot: snapshot}); (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := s.mayTakeSharedClaim(context.Background(), sub, engine.Claim{RunID: "r"}); err == nil {
		t.Error("a claim that says nothing of who made it was taken")
	}
}

// TestModeGuard_CheckKeepsWhenEnforceWasLeft: an observing engine that writes
// a lost record again keeps the time it read that enforce was left, or says
// now where the record it read was enforcing.
func TestModeGuard_CheckKeepsWhenEnforceWasLeft(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	left := now.Add(-time.Hour)
	for name, tc := range map[string]struct {
		mode string
		read artifacts.ModeRecord
		want time.Time
	}{
		"observe, left an hour ago":         {artifacts.AuthModeObserve, artifacts.ModeRecord{Mode: artifacts.AuthModeObserve, LeftEnforceAt: left}, left},
		"observe, which never enforced":     {artifacts.AuthModeObserve, artifacts.ModeRecord{Mode: artifacts.AuthModeObserve}, time.Time{}},
		"observe, beneath an enforcing one": {artifacts.AuthModeObserve, artifacts.ModeRecord{Mode: artifacts.AuthModeEnforce}, now},
		"enforce":                           {artifacts.AuthModeEnforce, artifacts.ModeRecord{Mode: artifacts.AuthModeEnforce}, time.Time{}},
	} {
		store := artifacts.NewModeStoreOn(artifacts.NewMemoryCAS())
		g := &ModeGuard{Store: store, Mode: tc.mode, Now: func() time.Time { return now }, seen: true, record: tc.read}
		g.Check(context.Background())
		if rec, _, err := store.Get(context.Background()); err != nil || rec.Mode != tc.mode || !rec.LeftEnforceAt.Equal(tc.want) {
			t.Errorf("%s: %+v, %v; want left at %v", name, rec, err, tc.want)
		}
	}
}
