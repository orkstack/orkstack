package main

import (
	"encoding/json/v2"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// profileFleet is one Orker per profile cause (a drift, an unavailable
// profile, a cause this release cannot read), one an operator cordoned over a
// drift, one whose profiles are all in service, and one that offers none.
func profileFleet() *scriptedFleet {
	since := timestamppb.New(cordonedAt.Add(2 * time.Minute))
	gpu := func(name string, units, usable, bound int32) *orkstackv1.OrkerProfile {
		return &orkstackv1.OrkerProfile{Name: name, Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
			Units: units, UsableUnits: usable, BoundUnits: bound, Revision: "r7", IdentityDigest: "sha-not-listed"}
	}
	devices := func(name string) *orkstackv1.OrkerProfile {
		return &orkstackv1.OrkerProfile{Name: name, Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}
	}
	// rolling is a devices profile mid-roll: contract 2, accepting 1.
	rolling := func(name string) *orkstackv1.OrkerProfile {
		p := devices(name)
		p.DefaultContract = "2"
		p.Contracts = []*orkstackv1.OrkerProfileContract{{Label: "2", Accepts: []string{"1"}}}
		return p
	}
	cause := func(c orkstackv1.ProfileCordon_Cause, profile string, r orkstackv1.ProfileCordonReason) *orkstackv1.ProfileCordon {
		return &orkstackv1.ProfileCordon{Cause: c, Profile: profile, Reason: r, Since: since, Detail: "detail-not-listed"}
	}
	drift, unavailable := orkstackv1.ProfileCordon_CAUSE_DRIFT, orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE
	return &scriptedFleet{
		presence: []engine.OrkerPresence{
			{Identity: "orker-both", LastSeen: fleetSeen},
			{Identity: "orker-drift", LastSeen: fleetSeen},
			{Identity: "orker-none", LastSeen: fleetSeen},
			{Identity: "orker-ok", LastSeen: fleetSeen},
			{Identity: "orker-out", LastSeen: fleetSeen},
			{Identity: "orker-wait", LastSeen: fleetSeen},
		},
		described: map[string]*orkstackv1.DescribeOrkerResult{
			"orker-both": {OrkerId: "orker-both", Pool: "gpu", Cordoned: true, CordonReason: "patching",
				CordonedAt: timestamppb.New(cordonedAt), Profiles: []*orkstackv1.OrkerProfile{gpu("a100", 2, 2, 0)},
				ProfileCordons: []*orkstackv1.ProfileCordon{cause(drift, "a100", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED)}},
			"orker-drift": {OrkerId: "orker-drift", Pool: "gpu", Cordoned: true,
				Profiles:       []*orkstackv1.OrkerProfile{gpu("a100", 4, 3, 1), devices("fpga")},
				ProfileCordons: []*orkstackv1.ProfileCordon{cause(drift, "a100", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED)}},
			"orker-none": {OrkerId: "orker-none", Pool: "build"},
			"orker-ok": {OrkerId: "orker-ok", Pool: "gpu", RunningTasks: 1,
				Profiles: []*orkstackv1.OrkerProfile{gpu("a100", 2, 2, 1), rolling("fpga")}},
			"orker-out": {OrkerId: "orker-out", Pool: "gpu", Cordoned: true,
				Profiles:       []*orkstackv1.OrkerProfile{devices("fpga")},
				ProfileCordons: []*orkstackv1.ProfileCordon{cause(orkstackv1.ProfileCordon_Cause(7), "fpga", orkstackv1.ProfileCordonReason(99))}},
			"orker-wait": {OrkerId: "orker-wait", Pool: "gpu", Cordoned: true,
				Profiles:       []*orkstackv1.OrkerProfile{devices("fpga")},
				ProfileCordons: []*orkstackv1.ProfileCordon{cause(unavailable, "fpga", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_ABSENT)}},
		},
		failures: map[string]error{},
		script:   map[string][]*orkstackv1.DescribeOrkerResult{},
	}
}

// TestOrkerList_ShowsProfilesAndTheCauseThatCordonsAnOrker: a cordon a
// profile holds reads as that cause, in the Admin → Pools words, and never as
// an operator's cordon; every Orker's profiles are a column, and each cause is
// a line under the table.
func TestOrkerList_ShowsProfilesAndTheCauseThatCordonsAnOrker(t *testing.T) {
	f := newOrkerFixture(t, profileFleet())
	out := f.run("orker", "list").requireExit(t, exitOK).stdout
	requireGolden(t, "orker-list-profiles.golden", out)
	for _, want := range []string{
		"cordoned (profile drifted)",
		"cordoned (profile unavailable)",
		"cordoned (profile out of service)",
		"cordoned (operator; profile drifted)",
		"a100 (gpu; drifted), fpga (devices)",
		"Profile a100 drifted since 2026-09-22T08:54:10Z: a device was replaced, or changed its numbers or group (DEVICE_CHANGED). It stays out of service on this Orker until the Orker restarts.",
		"Profile fpga unavailable since 2026-09-22T08:54:10Z: a device is not there (DEVICE_ABSENT). Once that is put right, it returns by itself after consecutive good checks.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("orker list lacks %q:\n%s", want, out)
		}
	}
	if footer := out[strings.Index(out, "accepting no new work:"):]; strings.Contains(strings.SplitN(footer, "\n\n", 2)[0], "orker-drift") {
		t.Errorf("a cordon only a profile holds is listed as an operator's:\n%s", out)
	}
}

// TestOrkerGet_ShowsEachProfileAndWhatHoldsIt: get names every profile with
// its kind, a gpu profile's units and the cause on it, then each cause in full.
func TestOrkerGet_ShowsEachProfileAndWhatHoldsIt(t *testing.T) {
	f := newOrkerFixture(t, profileFleet())
	requireGolden(t, "orker-get-profiles.golden", f.run("orker", "get", "orker-drift").requireExit(t, exitOK).stdout)
	ok := f.run("orker", "get", "orker-ok").requireExit(t, exitOK).stdout
	words := strings.Join(strings.Fields(ok), " ")
	for _, want := range []string{"state ready", "profile a100 gpu, 1 of 2 usable units in use", "profile fpga devices"} {
		if !strings.Contains(words, want) {
			t.Errorf("orker get orker-ok lacks %q:\n%s", want, ok)
		}
	}
	if none := f.run("orker", "get", "orker-none").requireExit(t, exitOK).stdout; strings.Contains(none, "profile") {
		t.Errorf("an Orker with no profiles speaks of one:\n%s", none)
	}
}

// TestOrkerList_JSONCarriesProfilesAndTheirCauses: --json is the inventory's
// own fields, and never what stays on the Orker's answer to the engine.
func TestOrkerList_JSONCarriesProfilesAndTheirCauses(t *testing.T) {
	f := newOrkerFixture(t, profileFleet())
	out := f.run("orker", "list", "--json").requireExit(t, exitOK).stdout
	var got struct {
		Orkers []struct {
			OrkerID  string `json:"orkerId"`
			Profiles []struct {
				Name        string `json:"name"`
				Kind        string `json:"kind"`
				Units       int32  `json:"units"`
				UsableUnits int32  `json:"usableUnits"`
				BoundUnits  int32  `json:"boundUnits"`
			} `json:"profiles"`
			ProfileCordons []struct {
				// An enum value this build cannot name is a number on the wire.
				Cause   any    `json:"cause"`
				Profile string `json:"profile"`
				Reason  any    `json:"reason"`
				Since   string `json:"since"`
			} `json:"profileCordons"`
		} `json:"orkers"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	for _, private := range []string{"sha-not-listed", "detail-not-listed", `"r7"`} {
		if strings.Contains(out, private) {
			t.Fatalf("--json carries %s, which stays on the Orker's answer:\n%s", private, out)
		}
	}
	for _, row := range got.Orkers {
		if row.OrkerID != "orker-drift" {
			continue
		}
		if len(row.Profiles) != 2 || row.Profiles[0].Name != "a100" || row.Profiles[0].Kind != "GPU" ||
			row.Profiles[0].Units != 4 || row.Profiles[0].UsableUnits != 3 || row.Profiles[0].BoundUnits != 1 {
			t.Fatalf("orker-drift profiles = %+v", row.Profiles)
		}
		if len(row.ProfileCordons) != 1 || row.ProfileCordons[0].Cause != "DRIFT" || row.ProfileCordons[0].Profile != "a100" ||
			row.ProfileCordons[0].Reason != "DEVICE_CHANGED" || row.ProfileCordons[0].Since == "" {
			t.Fatalf("orker-drift profileCordons = %+v", row.ProfileCordons)
		}
		return
	}
	t.Fatalf("no orker-drift row:\n%s", out)
}

// TestOrkerUncordon_SaysAProfileStillHolds: clearing the operator's cordon
// over a drift leaves the Orker cordoned, and the command says so.
func TestOrkerUncordon_SaysAProfileStillHolds(t *testing.T) {
	f := newOrkerFixture(t, profileFleet())
	out := f.run("orker", "uncordon", "orker-both").requireExit(t, exitOK).stdout
	if strings.Contains(out, "accepting work again") ||
		!strings.Contains(out, "cleared the operator's cordon on orker-both; it stays cordoned while profile a100 is drifted") {
		t.Fatalf("uncordon over a drift said:\n%s", out)
	}
}

// TestOrkerState_NamesEveryCauseInTheUIsOrder: a stopping Orker wins, then the
// operator, then a profile's cause, then a store outage, each in the words
// Admin → Pools stamps.
func TestOrkerState_NamesEveryCauseInTheUIsOrder(t *testing.T) {
	t.Parallel()
	pc := func(c orkstackv1.ProfileCordon_Cause) *orkstackv1.ProfileCordon {
		return &orkstackv1.ProfileCordon{Cause: c, Profile: "p"}
	}
	outage := &orkstackv1.StoreOutage{Reason: "object storage unreachable"}
	drift, unavailable := orkstackv1.ProfileCordon_CAUSE_DRIFT, orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE
	tests := []struct {
		name string
		row  *orkstackv1.Orker
		want string
	}{
		{"ready", &orkstackv1.Orker{}, "ready"},
		{"operator alone", &orkstackv1.Orker{Cordoned: true, CordonReason: "x"}, "cordoned"},
		{"an older release's bare cordon", &orkstackv1.Orker{Cordoned: true}, "cordoned"},
		{"drift", &orkstackv1.Orker{Cordoned: true, ProfileCordons: []*orkstackv1.ProfileCordon{pc(drift)}}, "cordoned (profile drifted)"},
		{"drift over unavailable", &orkstackv1.Orker{Cordoned: true, ProfileCordons: []*orkstackv1.ProfileCordon{pc(unavailable), pc(drift)}}, "cordoned (profile drifted)"},
		{"unreadable cause", &orkstackv1.Orker{Cordoned: true, ProfileCordons: []*orkstackv1.ProfileCordon{pc(unavailable), pc(9)}}, "cordoned (profile out of service)"},
		{"unavailable", &orkstackv1.Orker{Cordoned: true, ProfileCordons: []*orkstackv1.ProfileCordon{pc(unavailable)}}, "cordoned (profile unavailable)"},
		{"operator and drift", &orkstackv1.Orker{Cordoned: true, CordonReason: "x", ProfileCordons: []*orkstackv1.ProfileCordon{pc(drift)}}, "cordoned (operator; profile drifted)"},
		{"drift and outage", &orkstackv1.Orker{Cordoned: true, StoreOutage: outage, ProfileCordons: []*orkstackv1.ProfileCordon{pc(drift)}}, "cordoned (profile drifted; object storage unreachable)"},
		{"unavailable and outage", &orkstackv1.Orker{Cordoned: true, StoreOutage: outage, ProfileCordons: []*orkstackv1.ProfileCordon{pc(unavailable)}}, "cordoned (object storage unreachable; profile unavailable)"},
		{"operator and outage", &orkstackv1.Orker{Cordoned: true, CordonReason: "x", StoreOutage: outage}, "cordoned (operator; object storage unreachable)"},
		{"outage", &orkstackv1.Orker{Cordoned: true, StoreOutage: outage}, "cordoned (object storage unreachable)"},
		{"a cause with cordoned unset", &orkstackv1.Orker{ProfileCordons: []*orkstackv1.ProfileCordon{pc(drift)}}, "cordoned (profile drifted)"},
		{"stopping over a drift", &orkstackv1.Orker{Cordoned: true, Stopping: &orkstackv1.OrkerStopping{}, ProfileCordons: []*orkstackv1.ProfileCordon{pc(drift)}}, "stopping"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := orkerState(tt.row); got != tt.want {
				t.Errorf("orkerState = %q; want %q", got, tt.want)
			}
		})
	}
}

// TestOrkerDrain_StopsWhenOnlyAProfileIsLeft: an operator's uncordon in the
// middle of a drain ends it while a profile still holds the Orker, naming the
// profile, and never calls it drained.
func TestOrkerDrain_StopsWhenOnlyAProfileIsLeft(t *testing.T) {
	useFakeDrainClock(t, drainStart, 5*time.Second)
	profileOnly := drainStep(1, 0, true)
	profileOnly.CordonReason, profileOnly.CordonedAt = "", nil
	profileOnly.ProfileCordons = []*orkstackv1.ProfileCordon{{Cause: orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE, Profile: "fpga"}}
	f := newOrkerFixture(t, drainFleet(drainStep(1, 0, true), profileOnly))
	out := f.run("orker", "drain", "orker-7f3a").requireExit(t, exitError)
	if strings.Contains(out.stdout, "is drained") ||
		!strings.Contains(out.stderr, "was uncordoned by an operator; it stays cordoned while profile fpga is unavailable") {
		t.Fatalf("stdout=%q stderr=%q", out.stdout, out.stderr)
	}
}

// TestProfileUnitsText_ClampsASkewedAnswer: a gpu profile's counts never go
// below zero or past the total, and a devices profile has none.
func TestProfileUnitsText_ClampsASkewedAnswer(t *testing.T) {
	t.Parallel()
	gpu := orkstackv1.ProfileKind_PROFILE_KIND_GPU
	tests := []struct {
		name string
		p    *orkstackv1.OrkerProfile
		want string
	}{
		{"devices", &orkstackv1.OrkerProfile{Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, Units: 2}, ""},
		{"none declared", &orkstackv1.OrkerProfile{Kind: gpu}, ""},
		{"one usable", &orkstackv1.OrkerProfile{Kind: gpu, Units: 1, UsableUnits: 1}, "0 of 1 usable unit in use"},
		{"unhealthy", &orkstackv1.OrkerProfile{Kind: gpu, Units: 4, UsableUnits: 3, BoundUnits: 1}, "1 of 3 usable units in use; 1 unhealthy"},
		{"usable past total, bound past usable", &orkstackv1.OrkerProfile{Kind: gpu, Units: 2, UsableUnits: 9, BoundUnits: 9}, "2 of 2 usable units in use"},
		{"negative counts", &orkstackv1.OrkerProfile{Kind: gpu, Units: 2, UsableUnits: -1, BoundUnits: -3}, "0 of 0 usable units in use; 2 unhealthy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := profileUnitsText(tt.p); got != tt.want {
				t.Errorf("profileUnitsText = %q; want %q", got, tt.want)
			}
		})
	}
}

// TestOrkerUncordon_NamesAStoreOutageBesideAProfile: an uncordon over a
// profile cause and a store outage names both.
func TestOrkerUncordon_NamesAStoreOutageBesideAProfile(t *testing.T) {
	fleet := profileFleet()
	fleet.described["orker-both"].StoreOutage = &orkstackv1.StoreOutage{Reason: "object storage unreachable"}
	f := newOrkerFixture(t, fleet)
	out := f.run("orker", "uncordon", "orker-both").requireExit(t, exitOK).stdout
	if !strings.Contains(out, "profile a100 is drifted") || !strings.Contains(out, "object storage does not answer it either") {
		t.Fatalf("uncordon over a drift and an outage said:\n%s", out)
	}
}

// TestOrkerCommands_PrintOrkerTextTerminalSafe: every string an Orker
// reports — its pool, build, reasons, profile names, pins and metadata
// probe — reaches the terminal escaped in list, get and uncordon, while
// --json keeps it as it came.
func TestOrkerCommands_PrintOrkerTextTerminalSafe(t *testing.T) {
	fleet := profileFleet()
	row := fleet.described["orker-both"]
	row.Pool, row.BuildId, row.CordonReason = "gpu"+hostileText, "b"+hostileText, "patching"+hostileText
	row.StoreOutage = &orkstackv1.StoreOutage{Reason: "store" + hostileText}
	row.Profiles = append(row.Profiles, &orkstackv1.OrkerProfile{Name: "p" + hostileText, Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES})
	row.ProfileCordons = append(row.ProfileCordons, &orkstackv1.ProfileCordon{Cause: orkstackv1.ProfileCordon_CAUSE_DRIFT, Profile: "p" + hostileText})
	row.Pins = []*orkstackv1.OpenPin{{RunId: "ork-1", Pin: "pin" + hostileText}}
	row.MetadataProbes = []*orkstackv1.MetadataProbe{{Address: "169.254.169.254" + hostileText,
		Outcome: orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_PROBE_FAILED, Detail: hostileText}}
	f := newOrkerFixture(t, fleet)
	for _, args := range [][]string{{"orker", "list"}, {"orker", "get", "orker-both"}, {"orker", "uncordon", "orker-both"}} {
		out := f.run(args...).requireExit(t, exitOK).stdout
		requireTerminalSafeOutput(t, strings.Join(args, " "), out)
		if !strings.Contains(out, `\u001b]0;pwned\u0007`) {
			t.Errorf("%s does not show the escaped text:\n%s", strings.Join(args, " "), out)
		}
	}
	if raw := f.run("orker", "list", "--json").requireExit(t, exitOK).stdout; !strings.Contains(raw, `\u001b]0;pwned`) || strings.Contains(raw, `\\u001b`) {
		t.Errorf("--json is not the response as it came:\n%s", raw)
	}
}

// TestProfileContractsText: a profile's contracts read in PROFILES as its
// labels with the default starred and what the default accepts, in get in
// full, and a plain profile (one contract "1" accepting nothing) adds
// nothing to either.
func TestProfileContractsText(t *testing.T) {
	c := func(label string, accepts ...string) *orkstackv1.OrkerProfileContract {
		return &orkstackv1.OrkerProfileContract{Label: label, Accepts: accepts}
	}
	for _, tc := range []struct {
		name        string
		profile     *orkstackv1.OrkerProfile
		short, long string
	}{
		{"none reported", &orkstackv1.OrkerProfile{}, "", ""},
		{"plain", &orkstackv1.OrkerProfile{DefaultContract: "1", Contracts: []*orkstackv1.OrkerProfileContract{c("1")}}, "", ""},
		{"one other label", &orkstackv1.OrkerProfile{DefaultContract: "2", Contracts: []*orkstackv1.OrkerProfileContract{c("2")}},
			":2", "contract 2"},
		{"rolling", &orkstackv1.OrkerProfile{DefaultContract: "2", Contracts: []*orkstackv1.OrkerProfileContract{c("2", "1")}},
			":2 +acc 1", "contract 2 (accepts 1)"},
		{"two, the old the default", &orkstackv1.OrkerProfile{DefaultContract: "1",
			Contracts: []*orkstackv1.OrkerProfileContract{c("1"), c("2", "0")}},
			":1*,2", "contracts 1 (default), 2 (accepts 0)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := profileContractsShort(tc.profile); got != tc.short {
				t.Errorf("short = %q, want %q", got, tc.short)
			}
			if got := profileContractsLong(tc.profile); got != tc.long {
				t.Errorf("long = %q, want %q", got, tc.long)
			}
		})
	}
}

// TestAttemptProfilesCell: an attempt's PROFILES cell names the contract
// each profile ran under, whether its image chose it, and the pinned label
// it accepted when it ran under another.
func TestAttemptProfilesCell(t *testing.T) {
	for _, tc := range []struct {
		name     string
		profiles []*orkstackv1.EffectiveProfile
		want     string
	}{
		{"none", nil, "-"},
		{"under a pinned contract", []*orkstackv1.EffectiveProfile{{Name: "sensor", Contract: "1", RunContracts: []string{"1"}}},
			"sensor (contract 1)"},
		{"by accepts", []*orkstackv1.EffectiveProfile{{Name: "sensor", Contract: "2", RunContracts: []string{"1"}}},
			"sensor (contract 2, accepting 1)"},
		{"needed by the image", []*orkstackv1.EffectiveProfile{{Name: "sensor", Contract: "2", RunContracts: []string{"2"}, Declared: true}},
			"sensor (contract 2, needed by the image)"},
		{"the Orker's escapes are shown, not obeyed", []*orkstackv1.EffectiveProfile{{Name: "sen\x1b[2Jsor", Contract: "2\tx"}},
			`sen\u001b[2Jsor (contract 2\u0009x)`},
		{"two profiles", []*orkstackv1.EffectiveProfile{
			{Name: "sensor", Contract: "2", RunContracts: []string{"2"}, Declared: true}, {Name: "modem"}},
			"sensor (contract 2, needed by the image); modem"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := attemptProfilesCell(tc.profiles); got != tc.want {
				t.Fatalf("cell = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRenderAttemptList_ProfilesColumn: the PROFILES column is added only
// when an attempt ran with a device profile, before ERROR, and a table of
// attempts without one is unchanged.
func TestRenderAttemptList_ProfilesColumn(t *testing.T) {
	plain := &orkstackv1.TaskAttempt{TaskId: "read", Attempt: 1, OrkerId: "o1", Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED}
	profiled := &orkstackv1.TaskAttempt{TaskId: "read", Attempt: 2, OrkerId: "o2", Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		Profiles: []*orkstackv1.EffectiveProfile{{Name: "sensor", Contract: "2", RunContracts: []string{"1"}}}}

	var without strings.Builder
	if err := renderAttemptList(&without, []*orkstackv1.TaskAttempt{plain}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(without.String(), "PROFILES") {
		t.Fatalf("a run with no profiled attempt grew a PROFILES column:\n%s", without.String())
	}
	var with strings.Builder
	if err := renderAttemptList(&with, []*orkstackv1.TaskAttempt{plain, profiled}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(with.String(), "\n")
	if !strings.Contains(lines[0], "ORKER") || !strings.Contains(lines[0], "PROFILES") ||
		strings.Index(lines[0], "PROFILES") > strings.Index(lines[0], "ERROR") {
		t.Fatalf("header = %q, want PROFILES before ERROR", lines[0])
	}
	if !strings.Contains(lines[2], "sensor (contract 2, accepting 1)") || !strings.Contains(lines[1], " - ") {
		t.Fatalf("rows =\n%s\nwant attempt 2's contract and a dash for attempt 1", with.String())
	}
}

// TestProfileLine_NamesTheContracts: orkctl orker get's line for a profile
// names its contracts beside its kind.
func TestProfileLine_NamesTheContracts(t *testing.T) {
	p := &orkstackv1.OrkerProfile{Name: "sensor", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, DefaultContract: "1",
		Contracts: []*orkstackv1.OrkerProfileContract{{Label: "1"}, {Label: "2", Accepts: []string{"0"}}}}
	if got, want := profileLine(&orkstackv1.Orker{}, p), "devices, contracts 1 (default), 2 (accepts 0)"; got != want {
		t.Fatalf("profile line = %q, want %q", got, want)
	}
}
