package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestCensusLimitLabel(t *testing.T) {
	short := CensusLimitLabel("deploy-{{ .inputs.env }}")
	if !strings.HasPrefix(short, "deploy-{{ .inputs.env }}#") || len(short) != len("deploy-{{ .inputs.env }}")+9 {
		t.Fatalf("label %q, want the template and #8 hex", short)
	}
	long := strings.Repeat("é", 40) // 80 bytes
	label := CensusLimitLabel(long)
	if !utf8.ValidString(label) || len(label) > 64+9 {
		t.Fatalf("label %q (%d bytes), want valid UTF-8 within 64 bytes and the suffix", label, len(label))
	}
	if CensusLimitLabel(long) == CensusLimitLabel(long+"x") {
		t.Fatal("two templates that share their first 64 bytes share a label")
	}
}

func TestAggregateCensus(t *testing.T) {
	now := gateT0.Add(time.Minute)
	full := newTestGate()
	full.decideAcquire(acq("a", 1, gateStrategyQueue, gateT0), gateT0, false)
	q := acq("b", 1, gateStrategyQueue, gateT0)
	q.WaitUntil = gateT0.Add(4 * time.Minute)
	full.decideAcquire(q, gateT0, false)
	full.Holders[0].Stalled = true
	roomy := newTestGate()
	roomy.decideAcquire(acq("c", 3, gateStrategyQueue, gateT0), gateT0, false)
	rebuilding := newTestGate()
	rebuilding.Rebuild = gateRebuild{State: gateRebuildFailing}
	snap := aggregateCensus([]GateStateView{
		{Namespace: "payments", State: *full},
		{Namespace: "payments", State: *roomy},
		{Namespace: "payments", State: *rebuilding},
	}, now)
	if snap.Gates != 3 || snap.Rebuilding[[2]string{"payments", "failing"}] != 1 {
		t.Fatalf("gates %d rebuilding %v, want 3 and one failing", snap.Gates, snap.Rebuilding)
	}
	if len(snap.Rows) != 1 {
		t.Fatalf("rows %+v, want one (payments, deploy, run) row", snap.Rows)
	}
	r := snap.Rows[0]
	if r.SlotsUsed != 2 || r.SlotsLimit != 4 || r.QueueDepth != 1 || r.OldestAge != time.Minute || r.OldestTimeoutFraction != 0.25 {
		t.Fatalf("row %+v, want 2 used of 4, one queued a minute into its four", r)
	}
	if len(snap.Keys) != 1 || snap.Keys[0].KeysActive != 2 || snap.Keys[0].KeysFull != 1 || snap.Keys[0].StalledHolders != 1 {
		t.Fatalf("keys %+v, want 2 active, 1 full, 1 stalled holder", snap.Keys)
	}
	if strings.Contains(r.Limit, "{{") && !strings.Contains(r.Limit, "#") {
		t.Fatalf("limit label %q is not the bounded label", r.Limit)
	}
}

// fakeCensusScheduler is the schedule client in a variable: what the
// cluster holds, and whether an update is lost.
type fakeCensusScheduler struct {
	digest     string
	found      bool
	creates    int
	updates    int
	raceCreate bool
	loseUpdate bool
}

func (f *fakeCensusScheduler) describe(context.Context) (string, bool, error) {
	return f.digest, f.found, nil
}

func (f *fakeCensusScheduler) create(_ context.Context, _ CensusSettings, digest string) error {
	f.creates++
	if f.raceCreate {
		f.found, f.digest = true, "another-replicas"
		return errCensusExists
	}
	f.found, f.digest = true, digest
	return nil
}

func (f *fakeCensusScheduler) update(_ context.Context, _ CensusSettings, digest string) error {
	f.updates++
	if !f.loseUpdate {
		f.digest = digest
	}
	return nil
}

func TestEnsureCensusSchedule(t *testing.T) {
	s := DefaultCensusSettings()
	want, err := s.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		sch     *fakeCensusScheduler
		outcome CensusEnsureOutcome
		updates int
	}{
		"absent":            {sch: &fakeCensusScheduler{}, outcome: CensusCreated},
		"unchanged":         {sch: &fakeCensusScheduler{found: true, digest: want}, outcome: CensusUnchanged},
		"changed":           {sch: &fakeCensusScheduler{found: true, digest: "old"}, outcome: CensusUpdated, updates: 1},
		"update lost":       {sch: &fakeCensusScheduler{found: true, digest: "old", loseUpdate: true}, outcome: CensusUpdateLost, updates: 2},
		"another replica's": {sch: &fakeCensusScheduler{raceCreate: true}, outcome: CensusUpdated, updates: 1},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ensureCensusSchedule(context.Background(), tc.sch, s)
			if err != nil || got != tc.outcome || tc.sch.updates != tc.updates {
				t.Fatalf("outcome %q (%v) after %d updates, want %q after %d", got, err, tc.sch.updates, tc.outcome, tc.updates)
			}
		})
	}
	if _, err := ensureCensusSchedule(context.Background(), errScheduler{}, s); err == nil {
		t.Fatal("a describe that fails was not reported")
	}
}

type errScheduler struct{}

func (errScheduler) describe(context.Context) (string, bool, error) {
	return "", false, errors.New("unavailable")
}
func (errScheduler) create(context.Context, CensusSettings, string) error { return nil }
func (errScheduler) update(context.Context, CensusSettings, string) error { return nil }

func TestCensusSettingsDigest_ChangesWithEverySetting(t *testing.T) {
	base, _ := DefaultCensusSettings().Digest()
	for name, mutate := range map[string]func(*CensusSettings){
		"interval":  func(s *CensusSettings) { s.Interval *= 2 },
		"timeout":   func(s *CensusSettings) { s.Timeout *= 2 },
		"max gates": func(s *CensusSettings) { s.MaxGates++ },
		"parallel":  func(s *CensusSettings) { s.Parallel++ },
	} {
		s := DefaultCensusSettings()
		mutate(&s)
		if d, _ := s.Digest(); d == base {
			t.Fatalf("changing the %s left the digest unchanged", name)
		}
	}
}

// The oldest queued entry's timeout fraction is its own: one with no wait
// bound reports none, not a younger entry's.
func TestAggregateCensus_OldestFractionIsTheOldestEntrysOwn(t *testing.T) {
	g := newTestGate()
	g.decideAcquire(acq("h", 1, gateStrategyQueue, gateT0), gateT0, false)
	young := acq("young", 1, gateStrategyQueue, gateT0)
	young.WaitUntil = gateT0.Add(2 * time.Minute)
	g.decideAcquire(young, gateT0.Add(time.Minute), false)
	old := acq("old", 1, gateStrategyQueue, gateT0)
	old.WaitUntil = time.Time{}
	g.decideAcquire(old, gateT0, false)
	snap := aggregateCensus([]GateStateView{{Namespace: "ns", State: *g}}, gateT0.Add(2*time.Minute))
	if r := snap.Rows[0]; r.OldestAge != 2*time.Minute || r.OldestTimeoutFraction != 0 {
		t.Fatalf("row %+v, want the oldest (no bound) at 2m with no fraction", r)
	}
}
