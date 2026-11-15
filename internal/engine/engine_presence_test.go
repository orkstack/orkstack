package engine

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

var describedEnforcing = EngineDescription{Mode: "enforce", ClaimTTL: 24 * time.Hour,
	AdminsDigest: ConfigDigest([]string{"user:corp:a"}), OrkersDigest: ConfigDigest(nil),
	Deployment: "orkstack", BuildID: "2026.11.14-a1b2"}

// TestEngineDescription_TravelsInTheIdentity: an engine's description rides
// its identity's ork-caps list after its capabilities and comes back whole;
// the capabilities still read as before, so a release that knows only them
// is unaffected.
func TestEngineDescription_TravelsInTheIdentity(t *testing.T) {
	identity := EngineIdentityWith("1@a@", append([]string{CapabilityAuthnEnforce}, describedEnforcing.Tokens()...)...)
	if got := ParseEngineDescription(identity); got != describedEnforcing {
		t.Fatalf("parsed %+v back from %q, want %+v", got, identity, describedEnforcing)
	}
	if !identityHas(identity, CapabilityAuthnEnforce) || !identityHas(identity, CapabilityEachItems10k) {
		t.Fatalf("%q lost a capability beside its description", identity)
	}
	if want := []string{"mode=enforce", "claim-ttl=24h0m0s", "admins=" + describedEnforcing.AdminsDigest,
		"orkers=" + describedEnforcing.OrkersDigest, "deployment=orkstack", "build=2026.11.14-a1b2"}; !reflect.DeepEqual(describedEnforcing.Tokens(), want) {
		t.Fatalf("tokens = %q, want %q", describedEnforcing.Tokens(), want)
	}
	for _, c := range append(slices.Clone(EngineCapabilities), CapabilityAuthnEnforce) {
		if strings.Contains(c, "=") {
			t.Fatalf("capability %q holds '=', so it would read as a description", c)
		}
	}
}

// TestEngineDescription_LeavesOutWhatCannotTravel: a value Tokens cannot
// write whole is left out, never altered, so no engine is ever described as
// something it is not.
func TestEngineDescription_LeavesOutWhatCannotTravel(t *testing.T) {
	for _, d := range []EngineDescription{
		{Mode: "admin"},
		{ClaimTTL: -time.Second},
		{AdminsDigest: "ABCDEF0123456789"},
		{OrkersDigest: "short"},
		{Deployment: "has space"},
		{Deployment: "a,b"},
		{BuildID: strings.Repeat("x", 101)},
		{BuildID: "tab\there"},
		{BuildID: "caf\u00e9"},
		{Deployment: "\x1b[2J"},
	} {
		if tokens := d.Tokens(); len(tokens) != 0 {
			t.Errorf("%+v wrote %q; want nothing", d, tokens)
		}
	}
	if got := (EngineDescription{Deployment: "a b", BuildID: "ok", Mode: "admin"}).Unsaid(); !reflect.DeepEqual(got, []string{"auth.mode", "ORK_DEPLOYMENT_NAME"}) {
		t.Errorf("Unsaid = %q", got)
	}
	// A claim life past the bound is said as the bound, never left unsaid:
	// a lowering must wait at least that long for it.
	long := EngineDescription{ClaimTTL: MaxDescribedClaimTTL + time.Hour}
	if got := ParseEngineDescription(EngineIdentityWith("1@a@", long.Tokens()...)).ClaimTTL; got != MaxDescribedClaimTTL {
		t.Errorf("a claim life past the bound reads back as %s, want %s", got, MaxDescribedClaimTTL)
	}
}

// TestParseEngineDescription_BelievesOnlyWhatTokensWrites: an identity is
// whatever a poller sent. An older release's, an Orker's and a malformed one
// describe nothing; a key said twice is believed in neither value.
func TestParseEngineDescription_BelievesOnlyWhatTokensWrites(t *testing.T) {
	cases := []struct {
		name     string
		identity string
		want     EngineDescription
	}{
		{"an older release", "1@a@ ork-caps=each-items-10k,definition-ref", EngineDescription{}},
		{"an Orker", "orker-1", EngineDescription{}},
		{"no identity", "", EngineDescription{}},
		{"an unknown mode", "1@a@ ork-caps=mode=root", EngineDescription{}},
		{"a TTL past the bound", "1@a@ ork-caps=claim-ttl=90000h", EngineDescription{}},
		{"a TTL that is no duration", "1@a@ ork-caps=claim-ttl=forever", EngineDescription{}},
		{"a zero TTL", "1@a@ ork-caps=claim-ttl=0s", EngineDescription{}},
		{"an uppercase digest", "1@a@ ork-caps=admins=ABCDEF0123456789", EngineDescription{}},
		{"a mode said twice", "1@a@ ork-caps=mode=enforce,mode=asserted,claim-ttl=1h", EngineDescription{ClaimTTL: time.Hour}},
		{"the same mode said twice", "1@a@ ork-caps=mode=enforce,mode=enforce", EngineDescription{}},
		{"an empty value", "1@a@ ork-caps=deployment=,build=b1", EngineDescription{BuildID: "b1"}},
		{"an unknown key", "1@a@ ork-caps=colour=red,mode=observe", EngineDescription{Mode: "observe"}},
		{"only the last ork-caps counts", "1@a@ ork-caps=mode=enforce ork-caps=mode=asserted", EngineDescription{Mode: "asserted"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseEngineDescription(tc.identity); got != tc.want {
				t.Fatalf("ParseEngineDescription(%q) = %+v, want %+v", tc.identity, got, tc.want)
			}
		})
	}
}

// TestConfigDigest_IgnoresOrder: two engines whose lists hold the same
// entries agree, whatever order they were written in, and a different list
// differs.
func TestConfigDigest_IgnoresOrder(t *testing.T) {
	a := ConfigDigest([]string{"user:corp:a", "group:corp:ops"})
	if b := ConfigDigest([]string{"group:corp:ops", "user:corp:a"}); a != b {
		t.Fatalf("one list in two orders digests %s and %s", a, b)
	}
	if c := ConfigDigest([]string{"user:corp:a"}); c == a {
		t.Fatal("two different lists digest alike")
	}
	if !validDigest(a) || !validDigest(ConfigDigest(nil)) {
		t.Fatalf("ConfigDigest wrote %q, which ParseEngineDescription would not believe", a)
	}
}

// TestObserveEngines_ListsThisDeploymentsFreshEngines: the presence channel's
// fresh pollers of this deployment, each with its description; a stale
// record and another deployment's engine are left out, and one that names no
// deployment (a release that does not say) is listed.
func TestObserveEngines_ListsThisDeploymentsFreshEngines(t *testing.T) {
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	other := describedEnforcing
	other.Deployment = "elsewhere"
	asserted := EngineDescription{Mode: "asserted", ClaimTTL: 48 * time.Hour, Deployment: "orkstack"}
	fake.channels[task.EnginesQueue] = &workflowservice.DescribeTaskQueueResponse{Pollers: []*taskqueuepb.PollerInfo{
		capsPoller(EngineIdentityWith("2@b@", asserted.Tokens()...), true, "", ""),
		capsPoller(EngineIdentityWith("1@a@", describedEnforcing.Tokens()...), true, "", ""),
		capsPoller(EngineIdentityWith("3@c@", describedEnforcing.Tokens()...), false, "", ""),
		capsPoller(EngineIdentityWith("4@d@", other.Tokens()...), true, "", ""),
		capsPoller("5@e@ ork-caps=each-items-10k", true, "", ""),
	}}
	engines, err := cluster.ObserveEngines(context.Background())
	must.OK(t, err)
	var ids []string
	for _, e := range engines {
		ids = append(ids, e.Identity[:strings.Index(e.Identity, " ")])
		if e.LastSeen.IsZero() {
			t.Errorf("%s has no last-seen time", e.Identity)
		}
	}
	if want := []string{"1@a@", "2@b@", "5@e@"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("engines = %q, want %q", ids, want)
	}
	if engines[0].Description != describedEnforcing || engines[1].Description != asserted || engines[2].Description != (EngineDescription{}) {
		t.Fatalf("descriptions = %+v", engines)
	}
	if got, by := LargestClaimTTL(engines); got != 48*time.Hour || !strings.HasPrefix(by, "2@b@ ") {
		t.Fatalf("LargestClaimTTL = %s by %q, want 48h by 2@b@", got, by)
	}
	if got, by := LargestClaimTTL(nil); got != 0 || by != "" {
		t.Fatalf("LargestClaimTTL(nil) = %s by %q", got, by)
	}

	failing, fake2, _ := newFakeReleaseCluster(t, "")
	fake2.channelErr[task.EnginesQueue] = serviceerror.NewUnavailable("down")
	if _, err := failing.ObserveEngines(context.Background()); err == nil {
		t.Fatal("a failed describe was read as a channel no engine polls")
	}
}

// TestEnginesQueue_CarriesNoDispatch keeps the engines' presence channel a
// poller list: nothing in OrkStack dispatches to it (AGENTS.md §13).
func TestEnginesQueue_CarriesNoDispatch(t *testing.T) {
	root := filepath.Join("..", "..")
	dispatch := regexp.MustCompile(`(ExecuteActivity\([^\n]*(EnginesQueue|ActivityEnginePresence)|TaskQueue:\s*task\.EnginesQueue)`)
	checked := 0
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			checked++
			if loc := dispatch.FindIndex(raw); loc != nil {
				t.Errorf("%s dispatches to the engines' presence channel: %q", path, raw[loc[0]:loc[1]])
			}
			return nil
		})
		must.OK(t, err)
	}
	if checked == 0 {
		t.Fatal("grepped nothing")
	}
}

// FuzzParseEngineDescription: whatever identity a poller sends, the
// description read from it is one Tokens writes whole: reading back the
// tokens of what was parsed gives what was parsed.
func FuzzParseEngineDescription(f *testing.F) {
	for _, seed := range []string{
		EngineIdentityWith("1@a@", describedEnforcing.Tokens()...),
		"1@a@ ork-caps=each-items-10k",
		"1@a@ ork-caps=mode=enforce,mode=asserted",
		"1@a@ ork-caps=claim-ttl=90000h,admins=ABCDEF0123456789",
		"1@a@ ork-caps=deployment=,build=b1 ork-caps=mode=observe",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, identity string) {
		d := ParseEngineDescription(identity)
		identityOut := EngineIdentityWith("x@y@", d.Tokens()...)
		if again := ParseEngineDescription(identityOut); again != d {
			t.Fatalf("ParseEngineDescription(%q) = %+v; its tokens read back as %+v", identity, d, again)
		}
		for _, token := range d.Tokens() {
			key, value, _ := strings.Cut(token, "=")
			if key == "" || strings.ContainsAny(value, ", =\t\n") {
				t.Fatalf("Tokens wrote %q, which would split or mislead a reader", token)
			}
		}
		// An older release's capability check reads the same capabilities.
		for _, c := range EngineCapabilities {
			if !identityHas(identityOut, c) {
				t.Fatalf("%q lost the capability %s", identityOut, c)
			}
		}
	})
}
