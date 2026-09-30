package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// scriptedFleet is the CLI fixture's engine.OrkerFleet: the facts a cluster
// and its Orkers would report, stated, plus a script of successive answers
// for one Orker so a drain can be driven through its states.
type scriptedFleet struct {
	mu        sync.Mutex
	presence  []engine.OrkerPresence
	described map[string]*orkstackv1.DescribeOrkerResult
	failures  map[string]error
	// script, when set for an id, is consumed one entry per describe; the
	// last entry repeats. A nil entry means "nothing polls this id".
	script map[string][]*orkstackv1.DescribeOrkerResult

	observed []string // ObserveOrker ids and "*" for ObserveOrkers
	cordons  []*orkstackv1.CordonOrkerInput
}

func (f *scriptedFleet) current(id string) *orkstackv1.DescribeOrkerResult {
	if steps := f.script[id]; len(steps) > 0 {
		step := steps[0]
		if len(steps) > 1 {
			f.script[id] = steps[1:]
		}
		return step
	}
	return f.described[id]
}

func (f *scriptedFleet) ObserveOrkers(context.Context) ([]engine.OrkerPresence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observed = append(f.observed, "*")
	return f.presence, nil
}

func (f *scriptedFleet) ObserveOrker(_ context.Context, id string) (engine.OrkerPresence, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observed = append(f.observed, id)
	if steps, ok := f.script[id]; ok && len(steps) > 0 && steps[0] == nil {
		if len(steps) > 1 {
			f.script[id] = steps[1:]
		}
		return engine.OrkerPresence{}, false, nil
	}
	for _, p := range f.presence {
		if p.Identity == id {
			return p, true, nil
		}
	}
	return engine.OrkerPresence{}, false, nil
}

func (f *scriptedFleet) DescribeOrkerByID(_ context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failures[id]; err != nil {
		return nil, err
	}
	return proto.Clone(f.current(id)).(*orkstackv1.DescribeOrkerResult), nil
}

func (f *scriptedFleet) CordonOrker(_ context.Context, id string, in *orkstackv1.CordonOrkerInput) (*orkstackv1.DescribeOrkerResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cordons = append(f.cordons, in)
	out := proto.Clone(f.described[id]).(*orkstackv1.DescribeOrkerResult)
	out.Cordoned, out.CordonReason, out.CordonedAt = in.GetCordoned(), "", nil
	if in.GetCordoned() {
		out.CordonReason = in.GetReason()
		out.CordonedAt = timestamppb.New(cordonedAt)
	}
	f.described[id] = out
	return proto.Clone(out).(*orkstackv1.DescribeOrkerResult), nil
}

var (
	fleetSeen  = time.Date(2026, 9, 22, 9, 14, 2, 0, time.UTC)
	cordonedAt = time.Date(2026, 9, 22, 8, 52, 10, 0, time.UTC)
	pinExpires = time.Date(2026, 9, 22, 11, 26, 40, 0, time.UTC)
)

func newScriptedFleet() *scriptedFleet {
	return &scriptedFleet{
		presence: []engine.OrkerPresence{
			{Identity: "orker-7f3a", LastSeen: fleetSeen},
			{Identity: "orker-91c0", LastSeen: fleetSeen.Add(2 * time.Second)},
			{Identity: "orker-b45e", LastSeen: fleetSeen.Add(-time.Second)},
			{Identity: "orker-c001", LastSeen: fleetSeen.Add(-4 * time.Second)},
		},
		described: map[string]*orkstackv1.DescribeOrkerResult{
			"orker-7f3a": {OrkerId: "orker-7f3a", Pool: "build", BuildId: "2026.08.16-a1b2", RunningTasks: 2, OpenPins: 1,
				MaxConcurrentPins: 2, Cordoned: true, CordonReason: "CVE-2026-1234 runtime patch",
				CordonedAt: timestamppb.New(cordonedAt),
				Pins: []*orkstackv1.OpenPin{{RunId: "ork-20260922-1", Pin: "build",
					Since: timestamppb.New(cordonedAt.Add(-time.Hour)), ExpiresAt: timestamppb.New(pinExpires)}}},
			"orker-91c0": {OrkerId: "orker-91c0", Pool: "build", BuildId: "2026.08.16-a1b2", MaxConcurrentPins: 2},
			"orker-b45e": {OrkerId: "orker-b45e", Pool: "build", BuildId: "2026.08.09-9f8e", RunningTasks: 3},
		},
		failures: map[string]error{"orker-c001": errors.New("describe orker: context deadline exceeded")},
		script:   map[string][]*orkstackv1.DescribeOrkerResult{},
	}
}

func newOrkerFixture(t *testing.T, fleet *scriptedFleet) *fixture {
	t.Helper()
	return newFixtureWith(t, func(cfg *api.Config) { cfg.Fleet = fleet })
}

// fakeDrainClock advances only when the drain loop sleeps, so a four-hour
// drain runs in milliseconds and its timestamps are stable.
type fakeDrainClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeDrainClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeDrainClock) Sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return nil
}

func useFakeDrainClock(t *testing.T, start time.Time, interval time.Duration) *fakeDrainClock {
	t.Helper()
	clock := &fakeDrainClock{now: start}
	prevClock, prevInterval := drainClock, drainInterval
	drainClock, drainInterval = clock, interval
	t.Cleanup(func() { drainClock, drainInterval = prevClock, prevInterval })
	return clock
}

func orkerRequests(f *fixture) []string {
	var out []string
	for _, path := range f.requests() {
		if strings.Contains(path, "Orker") {
			out = append(out, path[strings.LastIndex(path, "/")+1:])
		}
	}
	return out
}

func TestOrkerList_Golden(t *testing.T) {
	fleet := newScriptedFleet()
	f := newOrkerFixture(t, fleet)
	out := f.run("orker", "list").requireExit(t, exitOK)
	requireGolden(t, "orker-list.golden", out.stdout)

	jsonOut := f.run("orker", "list", "--json").requireExit(t, exitOK)
	requireGolden(t, "orker-list.json.golden", jsonOut.stdout)
	var decoded map[string]any
	if err := json.Unmarshal([]byte(jsonOut.stdout), &decoded); err != nil || decoded["orkers"] == nil {
		t.Fatalf("--json is not the raw ListOrkersResponse: %v\n%s", err, jsonOut.stdout)
	}

	before := len(orkerRequests(f))
	f.run("orker", "list", "--pool", "gpu").requireExit(t, exitOK)
	if got := orkerRequests(f)[before:]; len(got) != 1 || got[0] != "ListOrkers" {
		t.Fatalf("--pool sent %v; want one ListOrkers", got)
	}

	empty := newOrkerFixture(t, &scriptedFleet{script: map[string][]*orkstackv1.DescribeOrkerResult{}})
	if got := empty.run("orker", "list").requireExit(t, exitOK).stdout; got != "No orkers are polling.\n" {
		t.Fatalf("empty fleet = %q", got)
	}
}

func TestOrkerGet_Golden(t *testing.T) {
	f := newOrkerFixture(t, newScriptedFleet())
	out := f.run("orker", "get", "orker-7f3a").requireExit(t, exitOK)
	requireGolden(t, "orker-get.golden", out.stdout)
}

func TestOrkerGet_UnknownIDExits1(t *testing.T) {
	f := newOrkerFixture(t, newScriptedFleet())
	out := f.run("orker", "get", "orker-gone").requireExit(t, exitError)
	if !strings.Contains(out.stderr, `no Orker "orker-gone" is polling; orkctl orker list shows who is`) {
		t.Fatalf("stderr = %q", out.stderr)
	}
}

func TestOrkerCordon_RequiresReasonBeforeAnyRequest(t *testing.T) {
	f := newOrkerFixture(t, newScriptedFleet())
	f.run("orker", "cordon", "orker-91c0").requireExit(t, exitError)
	f.run("orker", "cordon", "orker-91c0", "--reason", "   ").requireExit(t, exitError)
	if got := f.requests(); len(got) != 0 {
		t.Fatalf("a refused cordon still reached the server: %v", got)
	}
}

func TestOrkerCordon_Golden(t *testing.T) {
	fleet := newScriptedFleet()
	fleet.described["orker-91c0"].RunningTasks = 2
	fleet.described["orker-91c0"].OpenPins = 1
	f := newOrkerFixture(t, fleet)
	out := f.run("orker", "cordon", "orker-91c0", "--reason", "CVE-2026-1234 runtime patch").requireExit(t, exitOK)
	requireGolden(t, "orker-cordon.golden", out.stdout)
	if len(fleet.cordons) != 1 || !fleet.cordons[0].GetCordoned() || fleet.cordons[0].GetReason() != "CVE-2026-1234 runtime patch" {
		t.Fatalf("delivered %v", fleet.cordons)
	}
	notFound := f.run("orker", "cordon", "orker-typo", "--reason", "x").requireExit(t, exitError)
	if !strings.Contains(notFound.stderr, "not_found") || !strings.Contains(notFound.stderr, "orker-typo") {
		t.Fatalf("NotFound printed %q", notFound.stderr)
	}
}

func TestOrkerUncordon_Golden(t *testing.T) {
	fleet := newScriptedFleet()
	f := newOrkerFixture(t, fleet)
	out := f.run("orker", "uncordon", "orker-7f3a").requireExit(t, exitOK)
	requireGolden(t, "orker-uncordon.golden", out.stdout)
	jsonOut := f.run("orker", "uncordon", "orker-7f3a", "--json").requireExit(t, exitOK)
	if !strings.Contains(jsonOut.stdout, `"orker": {`) {
		t.Fatalf("--json = %s; want {\"orker\":{…}}", jsonOut.stdout)
	}
}

// drainStep is one answer of the drained Orker.
func drainStep(tasks, pins int32, cordoned bool) *orkstackv1.DescribeOrkerResult {
	out := &orkstackv1.DescribeOrkerResult{OrkerId: "orker-7f3a", Pool: "build", MaxConcurrentPins: 2,
		RunningTasks: tasks, OpenPins: pins, Cordoned: cordoned}
	if cordoned {
		out.CordonReason = defaultDrainReason
		out.CordonedAt = timestamppb.New(cordonedAt)
	}
	for i := int32(0); i < pins; i++ {
		out.Pins = append(out.Pins, &orkstackv1.OpenPin{RunId: "ork-20260922-1", Pin: "build",
			Since: timestamppb.New(cordonedAt), ExpiresAt: timestamppb.New(pinExpires)})
	}
	return out
}

// drainFleet is a fleet whose orker-7f3a starts uncordoned with (2,1) and
// then answers the given script.
func drainFleet(steps ...*orkstackv1.DescribeOrkerResult) *scriptedFleet {
	fleet := newScriptedFleet()
	fleet.described["orker-7f3a"] = drainStep(2, 1, false)
	fleet.script["orker-7f3a"] = append([]*orkstackv1.DescribeOrkerResult{drainStep(2, 1, false)}, steps...)
	return fleet
}

var drainStart = time.Date(2026, 9, 22, 9, 15, 40, 0, time.UTC)

func TestOrkerDrain_WatchTranscript_Golden(t *testing.T) {
	useFakeDrainClock(t, drainStart, 47*time.Minute)
	fleet := drainFleet(drainStep(2, 1, true), drainStep(0, 1, true), drainStep(0, 1, true), drainStep(0, 0, true))
	f := newOrkerFixture(t, fleet)
	out := f.run("orker", "drain", "orker-7f3a", "--timeout", "4h", "--watch").requireExit(t, exitOK)
	requireGolden(t, "orker-drain-watch.golden", out.stdout)
	if len(fleet.cordons) != 1 || fleet.cordons[0].GetReason() != defaultDrainReason {
		t.Fatalf("drain cordoned with %v; want the default reason once", fleet.cordons)
	}
}

func TestOrkerDrain_TimesOutExit4NamingWhatRemains(t *testing.T) {
	useFakeDrainClock(t, drainStart, 5*time.Second)
	f := newOrkerFixture(t, drainFleet(drainStep(0, 1, true)))
	out := f.run("orker", "drain", "orker-7f3a", "--timeout", "12s").requireExit(t, exitDrainTimedOut)
	requireGolden(t, "orker-drain-timeout.golden", out.stdout)
	for _, want := range []string{"is NOT drained after 12s: 0 tasks, 1 pin still running",
		`pin "build" of run ork-20260922-1`, "stopping the process now fails that run with PIN_HOST_LOST."} {
		if !strings.Contains(out.stdout, want) {
			t.Fatalf("timeout output lacks %q:\n%s", want, out.stdout)
		}
	}
}

// timeoutRecorder captures the Connect-Timeout-Ms header each ListOrkers
// request carried, which is the deadline the CLI gave it.
func TestOrkerDrain_TimeoutFlagIsTheDrainBound(t *testing.T) {
	useFakeDrainClock(t, drainStart, 5*time.Second)
	f := newOrkerFixture(t, drainFleet(drainStep(0, 1, true)))
	f.run("orker", "drain", "orker-7f3a", "--timeout", "1s").requireExit(t, exitDrainTimedOut)
	headers := f.timeoutHeaders()
	if len(headers) == 0 {
		t.Fatal("no request recorded a deadline")
	}
	for _, h := range headers {
		if h == "" || h == "1000" || len(h) < 5 {
			t.Fatalf("a drain request carried Connect-Timeout-Ms=%q; want the %s poll timeout, not --timeout 1s (all: %v)",
				h, drainPollTimeout, headers)
		}
	}
}

func TestOrkerDrain_KeepsAnExistingReasonUnlessGiven(t *testing.T) {
	useFakeDrainClock(t, drainStart, 5*time.Second)
	fleet := newScriptedFleet()
	fleet.described["orker-7f3a"] = drainStep(0, 0, true)
	fleet.described["orker-7f3a"].CordonReason = "already set"
	f := newOrkerFixture(t, fleet)
	out := f.run("orker", "drain", "orker-7f3a").requireExit(t, exitOK)
	if len(fleet.cordons) != 0 || !strings.Contains(out.stdout, `"already set"`) {
		t.Fatalf("a pre-cordoned drain sent %v and printed %q", fleet.cordons, out.stdout)
	}
	f.run("orker", "drain", "orker-7f3a", "--reason", "new reason").requireExit(t, exitOK)
	if len(fleet.cordons) != 1 || fleet.cordons[0].GetReason() != "new reason" {
		t.Fatalf("drain --reason sent %v; want one cordon with it", fleet.cordons)
	}
}

func TestOrkerDrain_StopsWhenTheOrkerIsNoLongerCordoned(t *testing.T) {
	useFakeDrainClock(t, drainStart, 5*time.Second)
	f := newOrkerFixture(t, drainFleet(drainStep(1, 0, true), drainStep(1, 0, false)))
	out := f.run("orker", "drain", "orker-7f3a").requireExit(t, exitError)
	if !strings.Contains(out.stderr, "is no longer cordoned: it restarted or was uncordoned") {
		t.Fatalf("stderr = %q", out.stderr)
	}
}

func TestOrkerDrain_StopsAfterTheUnreachableGrace(t *testing.T) {
	clock := useFakeDrainClock(t, drainStart, 5*time.Second)
	f := newOrkerFixture(t, drainFleet(drainStep(1, 0, true), nil))
	out := f.run("orker", "drain", "orker-7f3a").requireExit(t, exitError)
	if !strings.Contains(out.stderr, "stopped answering") || strings.Contains(out.stdout, "drained") {
		t.Fatalf("stdout=%q stderr=%q", out.stdout, out.stderr)
	}
	if elapsed := clock.Now().Sub(drainStart); elapsed < drainUnreachableGrace {
		t.Fatalf("gave up after %s; want at least the %s grace", elapsed, drainUnreachableGrace)
	}
}

func TestOrkerDrain_JSONPrintsTheFinalResponseOnEveryExitPath(t *testing.T) {
	cases := map[string]struct {
		fleet *scriptedFleet
		args  []string
		code  int
	}{
		"drained":     {drainFleet(drainStep(0, 0, true)), nil, exitOK},
		"timed out":   {drainFleet(drainStep(0, 1, true)), []string{"--timeout", "6s"}, exitDrainTimedOut},
		"uncordoned":  {drainFleet(drainStep(0, 1, false)), nil, exitError},
		"unreachable": {drainFleet(drainStep(0, 1, true), nil), nil, exitError},
		"never polled": {func() *scriptedFleet {
			f := newScriptedFleet()
			f.script["orker-x"] = []*orkstackv1.DescribeOrkerResult{nil}
			return f
		}(), nil, exitError},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			useFakeDrainClock(t, drainStart, 5*time.Second)
			id := "orker-7f3a"
			if name == "never polled" {
				id = "orker-x"
			}
			f := newOrkerFixture(t, tc.fleet)
			out := f.run(append([]string{"orker", "drain", id, "--json"}, tc.args...)...).requireExit(t, tc.code)
			var decoded map[string]any
			if err := json.Unmarshal([]byte(out.stdout), &decoded); err != nil {
				t.Fatalf("stdout is not one JSON document: %v\n%s", err, out.stdout)
			}
		})
	}
}

func TestOrkerCommands_IgnoreNamespace(t *testing.T) {
	plain := newScriptedFleet()
	fp := newOrkerFixture(t, plain)
	a := fp.run("orker", "list").requireExit(t, exitOK)
	scoped := newScriptedFleet()
	fs := newOrkerFixture(t, scoped)
	b := fs.run("orker", "list", "-n", "payments").requireExit(t, exitOK)
	if a.stdout != b.stdout || strings.Join(plain.observed, ",") != strings.Join(scoped.observed, ",") ||
		strings.Join(orkerRequests(fp), ",") != strings.Join(orkerRequests(fs), ",") {
		t.Fatalf("-n payments changed the command:\n%s\n---\n%s", a.stdout, b.stdout)
	}
}
