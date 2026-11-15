package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

type fakePresence struct {
	engines []engine.EnginePresence
	err     error
}

func (p *fakePresence) ObserveEngines(context.Context) ([]engine.EnginePresence, error) {
	return p.engines, p.err
}

type fakeChecker struct {
	missing []string
	err     error
	asked   []string
}

func (c *fakeChecker) MissingCapability(_ context.Context, capability string, _ engine.CapabilityScope) ([]string, error) {
	c.asked = append(c.asked, capability)
	return c.missing, c.err
}

func described(identity string, d engine.EngineDescription) engine.EnginePresence {
	return engine.EnginePresence{Identity: identity, LastSeen: time.Unix(1_700_000_000, 0), Description: d}
}

var (
	selfAdmins = engine.ConfigDigest([]string{"user:corp:a"})
	selfOrkers = engine.ConfigDigest([]string{"serviceaccount:orker-main\x00main\x00false"})
	selfDesc   = engine.EngineDescription{Mode: "enforce", ClaimTTL: 24 * time.Hour, AdminsDigest: selfAdmins, OrkersDigest: selfOrkers,
		Deployment: "orkstack", BuildID: "b2"}
)

func fleetFixture(presence *fakePresence, checker *fakeChecker) (*FleetReport, *bytes.Buffer, *int) {
	var logs bytes.Buffer
	unenforced := -1
	f := &FleetReport{Presence: presence, Checker: checker, Self: selfDesc,
		Logger:     slog.New(slog.NewTextHandler(&logs, nil)),
		Unenforced: func(_ context.Context, n int) { unenforced = n }}
	return f, &logs, &unenforced
}

// TestFleetReport_ReadsThePresenceChannel: every engine of this release is
// seen on the presence channel, whether or not it polls the workflow
// channel; one whose mode is below enforce is reported with its build and
// mode, and an enforcing one is not.
func TestFleetReport_ReadsThePresenceChannel(t *testing.T) {
	observe := selfDesc
	observe.Mode, observe.BuildID = "observe", "b1"
	asserted := engine.EngineDescription{Mode: "asserted", Deployment: "orkstack"}
	presence := &fakePresence{engines: []engine.EnginePresence{
		described("1@self@", selfDesc),
		described("2@api-only@", asserted),
		described("3@old-config@", observe),
		described("4@unsaid@", engine.EngineDescription{}),
	}}
	f, logs, unenforced := fleetFixture(presence, &fakeChecker{})
	f.Check(context.Background())
	var got []string
	for _, e := range f.Engines() {
		got = append(got, e.GetId()+" "+e.GetMode()+" "+e.GetBuildId())
		if e.GetLastSeen() == nil {
			t.Errorf("%s has no last-seen time", e.GetId())
		}
	}
	want := []string{"2@api-only@ asserted ", "3@old-config@ observe b1", "4@unsaid@ not stated "}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("engines = %q, want %q", got, want)
	}
	if *unenforced != 3 {
		t.Fatalf("unenforced = %d, want 3", *unenforced)
	}
	if !strings.Contains(logs.String(), "do not enforce") {
		t.Fatalf("the engines that do not enforce were not logged: %s", logs)
	}
}

// TestFleetReport_StillSeesAnEarlierRelease: an engine of a release that
// polls no presence channel is still seen on the workflow channel when it
// lacks authn-enforce, once; an engine both lists name is listed once.
func TestFleetReport_StillSeesAnEarlierRelease(t *testing.T) {
	asserted := engine.EngineDescription{Mode: "asserted"}
	presence := &fakePresence{engines: []engine.EnginePresence{described("1@self@", selfDesc), described("2@new@", asserted)}}
	checker := &fakeChecker{missing: []string{"2@new@", "9@v030@", ""}}
	f, _, unenforced := fleetFixture(presence, checker)
	f.Check(context.Background())
	var got []string
	for _, e := range f.Engines() {
		got = append(got, e.GetId()+" "+e.GetMode())
	}
	want := []string{"2@new@ asserted", "9@v030@ not enforce", "(an engine that sent no identity) not enforce"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("engines = %q, want %q", got, want)
	}
	if *unenforced != 3 {
		t.Fatalf("unenforced = %d, want 3", *unenforced)
	}
	if !reflect.DeepEqual(checker.asked, []string{engine.CapabilityAuthnEnforce}) {
		t.Fatalf("asked the workflow channel for %q", checker.asked)
	}
}

// TestFleetReport_ReportsAPeerConfiguredOtherwise: an authenticating peer
// whose auth.admins or auth.orkers digest differs from this engine's is
// logged at error, naming which; an asserted peer's lists are not compared,
// and a peer that says none is not reported. It refuses nothing.
func TestFleetReport_ReportsAPeerConfiguredOtherwise(t *testing.T) {
	admins := selfDesc
	admins.AdminsDigest = engine.ConfigDigest([]string{"user:corp:b"})
	orkers := selfDesc
	orkers.OrkersDigest = engine.ConfigDigest(nil)
	unsaid := selfDesc
	unsaid.AdminsDigest, unsaid.OrkersDigest = "", ""
	asserted := engine.EngineDescription{Mode: "asserted", AdminsDigest: engine.ConfigDigest(nil)}
	presence := &fakePresence{engines: []engine.EnginePresence{
		described("1@self@", selfDesc), described("2@admins@", admins), described("3@orkers@", orkers),
		described("4@unsaid@", unsaid), described("5@asserted@", asserted),
	}}
	f, logs, _ := fleetFixture(presence, &fakeChecker{})
	f.Check(context.Background())
	out := logs.String()
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		switch {
		case strings.Contains(line, "auth.admins"):
			if !strings.Contains(line, "level=ERROR") || !strings.Contains(line, "2@admins@") || strings.Contains(line, "3@orkers@") {
				t.Errorf("auth.admins line = %s", line)
			}
		case strings.Contains(line, "auth.orkers"):
			if !strings.Contains(line, "level=ERROR") || !strings.Contains(line, "3@orkers@") || strings.Contains(line, "2@admins@") {
				t.Errorf("auth.orkers line = %s", line)
			}
		}
	}
	if !strings.Contains(out, "auth.admins") || !strings.Contains(out, "auth.orkers") {
		t.Fatalf("a peer configured otherwise was not reported: %s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if (strings.Contains(line, "auth.admins") || strings.Contains(line, "auth.orkers")) &&
			(strings.Contains(line, "4@unsaid@") || strings.Contains(line, "5@asserted@")) {
			t.Fatalf("a peer that says no digest, or does not authenticate, was reported: %s", line)
		}
	}
	var rows []string
	for _, e := range f.Engines() {
		rows = append(rows, e.GetId()+" "+e.GetMode()+" "+strings.Join(e.GetSettingsDiffer(), "+"))
	}
	if want := []string{"2@admins@ enforce auth.admins", "3@orkers@ enforce auth.orkers", "5@asserted@ asserted "}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("engines = %q, want %q", rows, want)
	}
	if f.CheckedAt().IsZero() {
		t.Fatal("a check that read the cluster recorded no time")
	}
}

// TestFleetReport_TheWorkflowChannelOverridesAClaimToEnforce: an identity
// the presence channel says enforces, polling the workflow channel without
// authn-enforce, is reported: the poller that decides runs is the one that
// counts.
func TestFleetReport_TheWorkflowChannelOverridesAClaimToEnforce(t *testing.T) {
	presence := &fakePresence{engines: []engine.EnginePresence{described("1@liar@", selfDesc)}}
	f, _, unenforced := fleetFixture(presence, &fakeChecker{missing: []string{"1@liar@"}})
	f.Check(context.Background())
	if got := f.Engines(); len(got) != 1 || got[0].GetId() != "1@liar@" || got[0].GetMode() != "not enforce" || *unenforced != 1 {
		t.Fatalf("engines = %v, unenforced %d; want 1@liar@ reported once", got, *unenforced)
	}
}

// TestFleetReport_KeepsTheLastReportWhenAReadFails: a check that cannot read
// either list changes nothing it reported, so an outage never reads as a
// fleet that enforces.
func TestFleetReport_KeepsTheLastReportWhenAReadFails(t *testing.T) {
	presence := &fakePresence{engines: []engine.EnginePresence{described("2@asserted@", engine.EngineDescription{Mode: "asserted"})}}
	checker := &fakeChecker{}
	f, logs, unenforced := fleetFixture(presence, checker)
	f.Check(context.Background())
	if len(f.Engines()) != 1 || *unenforced != 1 {
		t.Fatalf("first check: %v, %d", f.Engines(), *unenforced)
	}
	for _, fail := range []func(){
		func() { presence.err, checker.err = errors.New("cluster down"), nil },
		func() { presence.err, checker.err = nil, errors.New("cluster down") },
	} {
		fail()
		*unenforced = -1
		f.Check(context.Background())
		if len(f.Engines()) != 1 || *unenforced != -1 {
			t.Fatalf("a failed read changed the report: %v, unenforced %d", f.Engines(), *unenforced)
		}
	}
	if checked := f.CheckedAt(); checked.IsZero() {
		t.Fatal("the first check's time was lost")
	}
	if !strings.Contains(logs.String(), "could not be listed") {
		t.Fatalf("the failed read was not logged: %s", logs)
	}
	var nilReport *FleetReport
	nilReport.Check(context.Background())
	if nilReport.Engines() != nil {
		t.Fatal("a nil report listed engines")
	}
}
