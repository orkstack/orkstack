package archive

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// retiredProducer is the base fixture: run P created three days ago,
// finished two days ago (past its one-day retention), retired two hours ago
// (past the one-hour grace), nothing open: everything it produced is
// reclaimable once the GC has warmed.
func retiredProducer(t *testing.T) (*world, []string) {
	t.Helper()
	w := newWorld(t)
	ids := w.produce("ork-p", w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), "a", "b")
	w.sidecars["ork-p"] = w.now.Add(-2 * time.Hour)
	return w, ids
}

func TestReclaim_ReclaimsRetiredRun(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if report.Err != nil || report.Outcome != "completed" {
		t.Fatalf("pass = %s, %v (held %v)", report.Outcome, report.Err, report.Held)
	}
	for _, id := range ids {
		if w.exists(id) {
			t.Fatalf("%s survived", id)
		}
		if len(w.markers[id]) != 0 {
			t.Fatalf("markers of %s survived: %v", id, w.markers[id])
		}
	}
	if _, ok := w.lists["ork-p"]; ok {
		t.Fatal("run-objects list survived")
	}
	if _, ok := w.records["ork-p"]; !ok {
		t.Fatal("no GC record")
	}
	if got := tally(report, OutcomeDeleted, ReasonUnreferenced); got != 2 || report.RunsFinished != 1 {
		t.Fatalf("deleted %d, finished %d", got, report.RunsFinished)
	}
	// A finished run is not examined again.
	writes := w.writes
	again := r.RunOnce(context.Background())
	if again.RunsFinished != 0 || w.writes != writes {
		t.Fatalf("a finished run was examined again: %+v, writes %d -> %d", again, writes, w.writes)
	}
}

func TestReclaim_DryRunDeletesNothing(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDryRun)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if report.Err != nil {
		t.Fatal(report.Err)
	}
	if got := tally(report, OutcomeWouldDelete, ReasonUnreferenced); got != 2 {
		t.Fatalf("would delete %d, want 2", got)
	}
	if w.writes != 0 || len(w.removed) != 0 || len(w.records) != 0 {
		t.Fatalf("dry run wrote: writes %d removed %v records %v", w.writes, w.removed, w.records)
	}
	for _, id := range ids {
		if !w.exists(id) || len(w.markers[id]) != 1 {
			t.Fatalf("dry run touched %s", id)
		}
	}
	totals, ok := r.CycleTotals()
	if !ok || totals[TallyKey{Namespace: "default", Class: ClassArtifact, Outcome: OutcomeWouldDelete, Reason: ReasonUnreferenced}].Objects != 2 {
		t.Fatalf("cycle totals = %v, %v", totals, ok)
	}
}

func TestReclaim_FirstPassAfterBootWarms(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	report := r.RunOnce(context.Background())
	if report.Outcome != "warming" || len(w.removed) != 0 {
		t.Fatalf("first pass = %s, removed %v", report.Outcome, w.removed)
	}
	if got := tally(report, OutcomeWouldDelete, ""); got != 2 {
		t.Fatalf("warming pass still reports: %d", got)
	}
	// A previous mark older than three intervals is no previous mark.
	w.advance(4 * time.Hour)
	if report := r.RunOnce(context.Background()); report.Outcome != "warming" || len(w.removed) != 0 {
		t.Fatalf("a stale previous mark was used: %s, removed %v", report.Outcome, w.removed)
	}
	r.RunOnce(context.Background())
	if w.exists(ids[0]) {
		t.Fatal("the pass after warming did not delete")
	}
}

func TestReclaim_RunInsideRetentionNeverDeleted(t *testing.T) {
	w := newWorld(t)
	// Finished an hour ago, retention one day: a premature or forged sidecar
	// two hours old must not make it reclaimable.
	ids := w.produce("ork-p", w.now.Add(-3*time.Hour), w.now.Add(-time.Hour), "a")
	w.sidecars["ork-p"] = w.now.Add(-2 * time.Hour)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if !w.exists(ids[0]) || len(w.removed) != 0 {
		t.Fatal("deleted an object of a run inside its retention")
	}
	if report.RunsCarried != 1 {
		t.Fatalf("carried %d, want the run kept due", report.RunsCarried)
	}
}

func TestReclaim_VerifiedRetirementRefusesForgedSidecar(t *testing.T) {
	tests := []struct {
		name  string
		setup func(w *world)
	}{
		{"live execution", func(w *world) { w.executions["ork-p"] = true }},
		{"no archive body", func(w *world) { delete(w.bodies, "ork-p") }},
		{"body names another run", func(w *world) { b := w.bodies["ork-p"]; b.RunID = "ork-x"; w.bodies["ork-p"] = b }},
		{"body not terminal", func(w *world) { b := w.bodies["ork-p"]; b.Terminal = false; w.bodies["ork-p"] = b }},
		{"body without finish", func(w *world) { b := w.bodies["ork-p"]; b.FinishedAt = time.Time{}; w.bodies["ork-p"] = b }},
		{"commit present", func(w *world) { w.commits["ork-p"] = 1 }},
		{"newer resume record", func(w *world) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, ids := retiredProducer(t)
			tt.setup(w)
			r := w.reclaimer(ReclaimDelete)
			if tt.name == "newer resume record" {
				r.WithResumes(resumedAll{})
			}
			w.warm(r)
			r.RunOnce(context.Background())
			for _, id := range ids {
				if !w.exists(id) {
					t.Fatalf("deleted %s of a run that is not verified retired", id)
				}
			}
		})
	}
}

type resumedAll struct{}

func (resumedAll) ResumedAfter(context.Context, string, time.Time) (bool, error) { return true, nil }

func TestReclaim_DueOnlyAfterGrace(t *testing.T) {
	w := newWorld(t)
	ids := w.produce("ork-p", w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), "a")
	w.sidecars["ork-p"] = w.now.Add(-59 * time.Minute)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	if report := r.RunOnce(context.Background()); report.RunsFinished != 0 || !w.exists(ids[0]) {
		t.Fatal("reclaimed inside the grace")
	}
	w.advance(2 * time.Minute)
	r.RunOnce(context.Background())
	if w.exists(ids[0]) {
		t.Fatal("not reclaimed past the grace (positive control)")
	}
}

func TestReclaim_KeepsWhatALiveMemoHitReferences(t *testing.T) {
	w, ids := retiredProducer(t)
	// Run X hit P's memo: it is still live, and archived (re-archived) with
	// a marker on P's object.
	w.reference("ork-x", w.now.Add(-50*time.Hour), w.now.Add(-30*time.Minute), ids[0])
	w.executions["ork-x"] = true
	w.live["ork-x"] = w.now.Add(-50 * time.Hour)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if !w.exists(ids[0]) {
		t.Fatal("deleted an object a live run references")
	}
	if w.exists(ids[1]) && tally(report, OutcomeKept, ReasonLowWater) == 0 {
		t.Fatal("the unreferenced sibling was neither deleted nor held by the mark")
	}
}

func TestReclaim_UnarchivedHitterHoldsTheLowWaterMark(t *testing.T) {
	w, ids := retiredProducer(t)
	// X hit P's memo before P retired and has no marker yet: it is open
	// (or closed without an archive), so the walk sees it.
	w.live["ork-x"] = w.now.Add(-50 * time.Hour)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	for _, id := range ids {
		if !w.exists(id) {
			t.Fatalf("deleted %s while a run created before its producer retired is unarchived", id)
		}
	}
	if tally(report, OutcomeKept, ReasonLowWater) != 2 || report.RunsWaiting != 1 {
		t.Fatalf("kept lowwater %d, waiting %d", tally(report, OutcomeKept, ReasonLowWater), report.RunsWaiting)
	}
	// Positive control: once X is archived with its marker and retired, the
	// rest goes.
	delete(w.live, "ork-x")
	w.reference("ork-x", w.now.Add(-50*time.Hour), w.now.Add(-49*time.Hour), ids[0])
	w.sidecars["ork-x"] = w.now.Add(-90 * time.Minute)
	r.RunOnce(context.Background())
	r.RunOnce(context.Background())
	for _, id := range ids {
		if w.exists(id) {
			t.Fatalf("%s kept after every claimant retired", id)
		}
	}
}

func TestReclaim_RecentlyClosedWithoutBodyLowersLowWater(t *testing.T) {
	w, ids := retiredProducer(t)
	w.closed["ork-x"] = w.now.Add(-50 * time.Hour) // terminated, never archived
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if !w.exists(ids[0]) || report.LowWater.RunID != "ork-x" {
		t.Fatalf("a closed unarchived run did not hold the mark: %+v", report.LowWater)
	}
	// An archived closed run does not hold it.
	w.reference("ork-x", w.now.Add(-50*time.Hour), w.now.Add(-49*time.Hour))
	r.RunOnce(context.Background())
	r.RunOnce(context.Background())
	if w.exists(ids[0]) {
		t.Fatal("an archived closed run held the mark")
	}
}

func TestReclaim_BodyCheckBudgetFailsClosed(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	r.config.Batch = 2
	// Three archived closed runs created before P retired: the budget checks
	// two, and the third's creation is the mark.
	for i, run := range []string{"ork-c1", "ork-c2", "ork-c3"} {
		created := w.now.Add(time.Duration(-60+i) * time.Hour)
		w.closed[run] = created
		w.bodies[run] = RunBody{RunID: run, Namespace: "default", Terminal: true, FinishedAt: created.Add(time.Minute)}
	}
	w.warm(r)
	report := r.RunOnce(context.Background())
	if report.LowWater.RunID != "ork-c3" || w.exists(ids[0]) == false {
		t.Fatalf("mark = %+v, exists %v", report.LowWater, w.exists(ids[0]))
	}
}

func TestReclaim_LowWaterIsMinOfTwoWalks(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	// The first walk sees X, the second does not (it moved during it).
	w.live["ork-x"] = w.now.Add(-50 * time.Hour)
	w.warm(r)
	delete(w.live, "ork-x")
	report := r.RunOnce(context.Background())
	if !w.exists(ids[0]) || report.LowWater.RunID != "ork-x" {
		t.Fatalf("the previous walk's minimum was dropped: %+v", report.LowWater)
	}
}

func TestReclaim_LowWaterUnknownHolds(t *testing.T) {
	tests := []struct {
		name  string
		cause string
		setup func(w *world)
	}{
		{"no sweep", LowWaterNoSweep, func(w *world) { w.hasSweep = false }},
		{"unclean sweep", LowWaterSweepUnclean, func(w *world) { w.sweep.Clean = false }},
		{"walk failed", LowWaterWalkFailed, func(w *world) { w.walkErr = errStore }},
		{"mid switch", LowWaterMidSwitch, func(w *world) { w.cause = LowWaterMidSwitch }},
		{"no creation", LowWaterNoCreation, func(w *world) { w.cause = LowWaterNoCreation }},
		{"body check failed", LowWaterWalkFailed, func(w *world) {
			w.closed["ork-c"] = w.now.Add(-50 * time.Hour)
			w.fail["HasRun"] = errStore
		}},
		{"index stale", LowWaterIndexStale, func(w *world) {
			w.accepted = &acceptedRun{id: "ork-new", at: w.now.Add(-30 * time.Minute)}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, ids := retiredProducer(t)
			r := w.reclaimer(ReclaimDelete)
			w.warm(r)
			tt.setup(w)
			report := r.RunOnce(context.Background())
			if report.LowWater.Known || report.LowWater.Cause != tt.cause || !slices.Contains(report.Held, HoldLowWaterUnknown) {
				t.Fatalf("mark = %+v held %v", report.LowWater, report.Held)
			}
			for _, id := range ids {
				if !w.exists(id) {
					t.Fatalf("deleted %s with the mark unknown", id)
				}
			}
			if tally(report, OutcomeKept, ReasonLowWater) != 2 {
				t.Fatalf("a held pass still reports: kept lowwater %d", tally(report, OutcomeKept, ReasonLowWater))
			}
		})
	}
}

func TestReclaim_IndexStaleCanaryIsInertWhenTheRunIsSeen(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	w.accepted = &acceptedRun{id: "ork-new", at: w.now.Add(-30 * time.Minute)}
	w.closed["ork-new"] = w.now.Add(-30 * time.Minute)
	w.bodies["ork-new"] = RunBody{RunID: "ork-new", Terminal: true, FinishedAt: w.now}
	w.warm(r)
	r.RunOnce(context.Background())
	if w.exists(ids[0]) {
		t.Fatal("the canary held a pass whose walk saw the run")
	}
}

func TestReclaim_HoldsDeleteNothing(t *testing.T) {
	tests := []struct {
		name string
		hold string
		cfg  func(w *world, r *Reclaimer)
	}{
		{"clock skew", HoldClockSkew, func(w *world, r *Reclaimer) { w.skew = 16 * time.Minute }},
		{"negative clock skew", HoldClockSkew, func(w *world, r *Reclaimer) { w.skew = -16 * time.Minute }},
		{"mixed fleet", HoldComposedArtifacts, func(w *world, r *Reclaimer) { r.WithFleet(fleet{mixed: true}) }},
		{"capabilities unreadable", HoldCapabilitiesUnreadable, func(w *world, r *Reclaimer) { r.WithFleet(fleet{err: errStore}) }},
		{"executions missing", HoldExecutionsMissing, func(w *world, r *Reclaimer) { r.WithMissingHold(missingHeld(true)) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, ids := retiredProducer(t)
			r := w.reclaimer(ReclaimDelete)
			w.warm(r)
			tt.cfg(w, r)
			report := r.RunOnce(context.Background())
			if !slices.Contains(report.Held, tt.hold) || report.Outcome != "held" {
				t.Fatalf("held %v outcome %s", report.Held, report.Outcome)
			}
			if !w.exists(ids[0]) || w.writes != 0 {
				t.Fatalf("a held pass wrote (writes %d)", w.writes)
			}
			if tally(report, OutcomeWouldDelete, "") != 2 {
				t.Fatal("a held pass still reports what it would delete")
			}
		})
	}
	// Positive control: skew within G/4 does not hold.
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	w.skew = 14 * time.Minute
	w.warm(r)
	r.RunOnce(context.Background())
	if w.exists(ids[0]) {
		t.Fatal("skew within G/4 held the pass")
	}
}

type fleet struct {
	mixed bool
	err   error
}

func (f fleet) ComposedArtifactsMixed(context.Context) (bool, error) { return f.mixed, f.err }

type missingHeld bool

func (m missingHeld) MissingHeld() bool { return bool(m) }

func TestReclaim_ExecutionsMissingEarlyHoldsNextPass(t *testing.T) {
	w, ids := retiredProducer(t)
	// Ten runs inside their retention whose executions are gone, each a
	// claimant on P's object: more than a tenth of the lookups missing early.
	for _, run := range []string{"ork-m1", "ork-m2", "ork-m3"} {
		w.reference(run, w.now.Add(-2*time.Hour), w.now.Add(-time.Hour), ids[0])
	}
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if !slices.Contains(report.Held, HoldExecutionsMissing) {
		t.Fatalf("held %v", report.Held)
	}
	if !w.exists(ids[0]) {
		t.Fatal("deleted an object claimed by runs inside their retention")
	}
}

func TestReclaim_StoreFailureDeletesNothing(t *testing.T) {
	for _, call := range []string{
		"StoreNow", "ListRuns", "HasRun", "CleanupSidecar", "RunObjectList", "ListUnder",
		"StatObject", "ListObjectReferences", "ReclaimBody", "LookupWorkflow", "EachReclaimRecord",
	} {
		t.Run(call, func(t *testing.T) {
			w, ids := retiredProducer(t)
			w.closed["ork-c"] = w.now.Add(-50 * time.Hour)
			w.bodies["ork-c"] = RunBody{RunID: "ork-c", Terminal: true, FinishedAt: w.now.Add(-49 * time.Hour)}
			r := w.reclaimer(ReclaimDelete)
			w.warm(r)
			w.fail[call] = errStore
			r.RunOnce(context.Background())
			for _, id := range ids {
				if !w.exists(id) {
					t.Fatalf("%s failing: deleted %s", call, id)
				}
			}
			if len(w.records) != 0 {
				t.Fatalf("%s failing: wrote a GC record", call)
			}
		})
	}
}

func TestReclaim_RemoveFailureKeepsTheRunDue(t *testing.T) {
	for _, call := range []string{"RemoveReclaimedObject", "RemoveObjectReferences", "RemoveRunObjectList", "CreateReclaimRecord"} {
		t.Run(call, func(t *testing.T) {
			w, _ := retiredProducer(t)
			r := w.reclaimer(ReclaimDelete)
			w.warm(r)
			w.fail[call] = errStore
			report := r.RunOnce(context.Background())
			if len(w.records) != 0 || report.RunsFinished != 0 || report.RunsCarried != 1 {
				t.Fatalf("%s failing: finished %d carried %d records %v", call, report.RunsFinished, report.RunsCarried, w.records)
			}
			delete(w.fail, call)
			r.RunOnce(context.Background())
			if _, ok := w.records["ork-p"]; !ok {
				t.Fatalf("%s: the run did not finish once the store answered", call)
			}
		})
	}
}

func TestReclaim_LateCommitKeepsProducerForGood(t *testing.T) {
	w, ids := retiredProducer(t)
	w.commits["ork-p"] = 1 // a suspended Orker committed after retirement
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if report.RunsLateCommit != 1 || tally(report, OutcomeKept, ReasonLateCommit) != 2 {
		t.Fatalf("late commits %d kept %d", report.RunsLateCommit, tally(report, OutcomeKept, ReasonLateCommit))
	}
	rec, ok := w.records["ork-p"]
	if !ok || !rec.LateCommit || len(rec.Kept) != 2 {
		t.Fatalf("record = %+v, %v", rec, ok)
	}
	// Nothing re-empties it: removing the commit later deletes nothing,
	// because the run is done.
	delete(w.commits, "ork-p")
	r.RunOnce(context.Background())
	for _, id := range ids {
		if !w.exists(id) || len(w.markers[id]) == 0 {
			t.Fatalf("%s of a late-commit run was touched", id)
		}
	}
}

func TestReclaim_LateCommitBetweenListAndDeleteIsCaughtAtReList(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	prefix := commitPrefixOf(t, "ork-p")
	// The commit appears after verification listed the prefix empty, before
	// the per-object re-list.
	w.onList = func(key string, n int) {
		if key == prefix && n == 2 {
			w.mu.Lock()
			w.commits["ork-p"] = 1
			w.mu.Unlock()
		}
	}
	w.listed = map[string]int{}
	r.RunOnce(context.Background())
	for _, id := range ids {
		if !w.exists(id) {
			t.Fatalf("deleted %s after a late commit appeared", id)
		}
	}
}

func TestReclaim_MarkerAddedDuringDeleteRelists(t *testing.T) {
	w, ids := retiredProducer(t)
	w.reference("ork-x", w.now.Add(-time.Hour), w.now.Add(-30*time.Minute))
	w.executions["ork-x"] = true
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	// A live run's marker lands between the decision's listing and step
	// (e)'s re-list.
	w.onList = func(key string, n int) {
		if key == ids[0] && n == 2 {
			w.mu.Lock()
			w.mark(ids[0], "ork-x")
			w.mu.Unlock()
		}
	}
	w.listed = map[string]int{}
	r.RunOnce(context.Background())
	if !w.exists(ids[0]) {
		t.Fatal("deleted an object a marker was added to before the delete")
	}
	if w.exists(ids[1]) {
		t.Fatal("the untouched sibling was not deleted (positive control)")
	}
}

func TestReclaim_ClaimClasses(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(w *world, id string)
		reason   string
		finished bool
	}{
		{"live archived claimant whose list holds it: delegated", func(w *world, id string) {
			w.reference("ork-x", w.now.Add(-time.Hour), w.now.Add(-30*time.Minute), id)
			w.executions["ork-x"] = true
		}, ReasonClaimLive, true},
		{"live unarchived claimant: waits", func(w *world, id string) {
			w.mark(id, "ork-x")
			w.executions["ork-x"] = true
		}, ReasonClaimLive, false},
		{"archived claimant whose list lacks it: claim_unlisted", func(w *world, id string) {
			w.reference("ork-x", w.now.Add(-time.Hour), w.now.Add(-30*time.Minute))
			w.mark(id, "ork-x")
			w.executions["ork-x"] = true
		}, ReasonClaimUnlisted, false},
		{"claimant retired inside the grace", func(w *world, id string) {
			w.reference("ork-x", w.now.Add(-60*time.Hour), w.now.Add(-50*time.Hour), id)
			w.sidecars["ork-x"] = w.now.Add(-30 * time.Minute)
		}, ReasonClaimInGrace, true},
		{"claimant never seen: a final leak", func(w *world, id string) {
			w.mark(id, "ork-ghost")
		}, ReasonClaimOrphaned, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, ids := retiredProducer(t)
			tt.setup(w, ids[0])
			r := w.reclaimer(ReclaimDelete)
			w.warm(r)
			report := r.RunOnce(context.Background())
			if !w.exists(ids[0]) {
				t.Fatal("deleted a claimed object")
			}
			if tally(report, OutcomeKept, tt.reason) != 1 {
				t.Fatalf("kept %v", report.Objects)
			}
			if _, done := w.records["ork-p"]; done != tt.finished {
				t.Fatalf("finished = %v, want %v", done, tt.finished)
			}
		})
	}
}

func TestReclaim_UnparseableAndTooManyReferencesAreFinal(t *testing.T) {
	w, _ := retiredProducer(t)
	b := w.bodies["ork-p"]
	b.Objects = append(b.Objects, BodyObject{ID: "orkflows/flow/_unattributed/t/n/" + "ab", Class: ClassArtifact})
	w.bodies["ork-p"] = b
	many := objectID("ork-p", "c")
	w.objects[many] = 1
	b.Objects = append(b.Objects, BodyObject{ID: many, Class: ClassArtifact})
	w.bodies["ork-p"] = b
	for i := 0; i <= 1000; i++ {
		w.mark(many, "ork-r"+string(rune('a'+i%26))+time.Duration(i).String())
	}
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if tally(report, OutcomeKept, ReasonUnparseableKey) != 1 || tally(report, OutcomeKept, ReasonTooManyReferences) != 1 {
		t.Fatalf("objects = %v", report.Objects)
	}
	if !w.exists(many) {
		t.Fatal("deleted an object with too many references")
	}
	rec, ok := w.records["ork-p"]
	if !ok || !slices.Contains(rec.Kept, many) {
		t.Fatalf("record = %+v, %v", rec, ok)
	}
}

func TestReclaim_AlreadyGoneIsNotDeleted(t *testing.T) {
	w, ids := retiredProducer(t)
	delete(w.objects, ids[0])
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if tally(report, OutcomeAlreadyGone, "") != 1 || tally(report, OutcomeDeleted, "") != 1 {
		t.Fatalf("objects = %v", report.Objects)
	}
	if len(w.markers[ids[0]]) != 0 {
		t.Fatal("an already-gone object's markers were not released")
	}
}

func TestReclaim_UnlistedObjectUnderProducerPrefix(t *testing.T) {
	w, _ := retiredProducer(t)
	stray := objectID("ork-p", "stray") // uploaded by an attempt that never recorded it
	w.objects[stray] = 7
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if w.exists(stray) {
		t.Fatal("an unlisted object of a reclaimed run survived")
	}
	if report.Objects[TallyKey{Namespace: "default", Class: ClassUnlisted, Outcome: OutcomeDeleted, Reason: ReasonUnreferenced}].Bytes != 7 {
		t.Fatalf("objects = %v", report.Objects)
	}
}

func TestReclaim_ObjectOfALiveProducerIsDelegated(t *testing.T) {
	w := newWorld(t)
	// P is live and produced O; R (retired) references it.
	ids := w.produce("ork-p", w.now.Add(-3*time.Hour), w.now.Add(-time.Hour), "a")
	w.executions["ork-p"] = true
	w.reference("ork-r", w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), ids[0])
	w.sidecars["ork-r"] = w.now.Add(-2 * time.Hour)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if !w.exists(ids[0]) {
		t.Fatal("deleted a live run's object")
	}
	if _, ok := w.records["ork-r"]; !ok || report.RunsFinished != 1 {
		t.Fatal("the referencing run did not finish (delegated)")
	}
	if !w.markers[ids[0]]["ork-p"] {
		t.Fatal("the producer's own marker was released")
	}
}

func TestReclaim_LocalBackendReclaimsNothing(t *testing.T) {
	w, ids := retiredProducer(t)
	w.local = true
	r := w.reclaimer(ReclaimDelete)
	r.RunOnce(context.Background())
	report := r.RunOnce(context.Background())
	if !slices.Equal(report.Held, []string{HoldLocalBackend}) || !w.exists(ids[0]) || w.writes != 0 {
		t.Fatalf("held %v writes %d", report.Held, w.writes)
	}
}

func TestReclaim_OffRunsNothing(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimOff)
	r.Run(context.Background()) // returns at once
	if report := r.RunOnce(context.Background()); len(report.Objects) != 0 || !w.exists(ids[0]) {
		t.Fatal("off examined objects")
	}
}

func TestReclaim_TwoReplicasConverge(t *testing.T) {
	w, ids := retiredProducer(t)
	a, b := w.reclaimer(ReclaimDelete), w.reclaimer(ReclaimDelete)
	w.warm(a)
	w.warm(b)
	ra := a.RunOnce(context.Background())
	rb := b.RunOnce(context.Background())
	if ra.Err != nil || rb.Err != nil {
		t.Fatal(ra.Err, rb.Err)
	}
	if w.exists(ids[0]) || len(w.records) != 1 {
		t.Fatal("two replicas did not converge")
	}
	if tally(rb, OutcomeDeleted, "")+tally(ra, OutcomeDeleted, "") != 2 {
		t.Fatalf("deletes counted twice: %v / %v", ra.Objects, rb.Objects)
	}
}

func TestReclaim_BatchBoundsAPassAndCarriesOver(t *testing.T) {
	w := newWorld(t)
	var all []string
	for _, run := range []string{"ork-p1", "ork-p2", "ork-p3"} {
		all = append(all, w.produce(run, w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), "a", "b")...)
		w.sidecars[run] = w.now.Add(-2 * time.Hour)
	}
	r := w.reclaimer(ReclaimDelete)
	r.config.Batch = 2
	w.warm(r)
	first := r.RunOnce(context.Background())
	if first.CycleComplete || first.RunsFinished != 1 {
		t.Fatalf("first pass finished %d complete %v", first.RunsFinished, first.CycleComplete)
	}
	for i := 0; i < 3; i++ {
		r.RunOnce(context.Background())
	}
	for _, id := range all {
		if w.exists(id) {
			t.Fatalf("%s left after the backlog drained", id)
		}
	}
}

func TestParseReclaimMode(t *testing.T) {
	for raw, want := range map[string]ReclaimMode{"": ReclaimDryRun, "off": ReclaimOff, "dry_run": ReclaimDryRun, "delete": ReclaimDelete} {
		if got, err := ParseReclaimMode(raw); err != nil || got != want {
			t.Fatalf("%q = %q, %v", raw, got, err)
		}
	}
	for _, raw := range []string{"Delete", "on", "true", "dry-run"} {
		if _, err := ParseReclaimMode(raw); err == nil {
			t.Fatalf("%q accepted", raw)
		}
	}
}

func commitPrefixOf(t *testing.T, run string) string {
	t.Helper()
	p, err := artifacts.RunCommitPrefix("default", run)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReclaim_ProducerInsideGraceKeepsReferencedObject(t *testing.T) {
	w := newWorld(t)
	// P retired 30 minutes ago; R, due, references P's object.
	ids := w.produce("ork-p", w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), "a")
	w.sidecars["ork-p"] = w.now.Add(-30 * time.Minute)
	w.reference("ork-r", w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), ids[0])
	w.sidecars["ork-r"] = w.now.Add(-2 * time.Hour)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if !w.exists(ids[0]) || tally(report, OutcomeKept, ReasonRunInGrace) != 1 {
		t.Fatalf("an object whose producer retired inside the grace: exists %v, %v", w.exists(ids[0]), report.Objects)
	}
	if _, ok := w.records["ork-r"]; !ok {
		t.Fatal("the referencing run did not finish (delegated to its producer)")
	}
}

func TestReclaim_StoreRefusalIsCounted(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	w.fail["RemoveReclaimedObject"] = fmt.Errorf("locked: %w", artifacts.ErrStoreRefused)
	report := r.RunOnce(context.Background())
	if tally(report, OutcomeFailed, ReasonStoreRefused) != 2 || !w.exists(ids[0]) || report.RunsCarried != 1 {
		t.Fatalf("objects = %v carried %d", report.Objects, report.RunsCarried)
	}
}

func TestReclaim_NoResumeCheckDeletesNothing(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	r.resumes = nil
	w.warm(r)
	r.RunOnce(context.Background())
	if !w.exists(ids[0]) {
		t.Fatal("a reclaimer with no resume check deleted")
	}
}

func TestReclaim_RecordCountsAndALateCommitFoundMidRun(t *testing.T) {
	w, _ := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	prefix := commitPrefixOf(t, "ork-p")
	// The commit lands after the first object's delete, before the second's
	// re-list.
	w.onList = func(key string, n int) {
		if key == prefix && n == 3 {
			w.mu.Lock()
			w.commits["ork-p"] = 1
			w.mu.Unlock()
		}
	}
	w.listed = map[string]int{}
	r.RunOnce(context.Background())
	rec, ok := w.records["ork-p"]
	if !ok || !rec.LateCommit || rec.ObjectsDeleted != 1 || rec.BytesDeleted != 10 || len(rec.Kept) != 1 {
		t.Fatalf("record = %+v, %v", rec, ok)
	}
}

func TestReclaim_NonCanonicalKeyIsAFinalLeak(t *testing.T) {
	w, _ := retiredProducer(t)
	w.fail["ListObjectReferences"] = fmt.Errorf("x: %w", artifacts.ErrInvalidURI)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	report := r.RunOnce(context.Background())
	if tally(report, OutcomeKept, ReasonUnparseableKey) != 2 || report.RunsCarried != 0 {
		t.Fatalf("objects = %v carried %d", report.Objects, report.RunsCarried)
	}
}

func TestReclaim_DryRunCycleTotalsSpanEveryPassOfTheCycle(t *testing.T) {
	w := newWorld(t)
	w.produce("ork-p1", w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), "a", "b")
	w.produce("ork-p2", w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), "c", "d")
	w.sidecars["ork-p1"] = w.now.Add(-2 * time.Hour)
	w.sidecars["ork-p2"] = w.now.Add(-2 * time.Hour)
	r := w.reclaimer(ReclaimDryRun)
	r.config.Batch = 1 // one run per pass: the cycle takes two
	key := TallyKey{Namespace: "default", Class: ClassArtifact, Outcome: OutcomeWouldDelete, Reason: ReasonUnreferenced}
	for cycle := 1; cycle <= 2; cycle++ {
		first := r.RunOnce(context.Background())
		if first.Err != nil || first.CycleComplete {
			t.Fatalf("cycle %d: first pass = %s complete %v, %v", cycle, first.Outcome, first.CycleComplete, first.Err)
		}
		second := r.RunOnce(context.Background())
		if second.Err != nil || !second.CycleComplete {
			t.Fatalf("cycle %d: second pass = %s complete %v, %v", cycle, second.Outcome, second.CycleComplete, second.Err)
		}
		totals, ok := r.CycleTotals()
		if !ok || totals[key].Objects != 4 {
			t.Fatalf("cycle %d: totals = %v, want the 4 objects of both passes and no more", cycle, totals)
		}
		if second.Cycle[key].Objects != 4 {
			t.Fatalf("cycle %d: the completing pass reports the cycle %v", cycle, second.Cycle)
		}
		if first.Cycle != nil {
			t.Fatalf("cycle %d: a pass that did not complete the cycle reported one: %v", cycle, first.Cycle)
		}
	}
}

func TestReclaim_CycleTotalsCountAPassThatFailedMidCycleOnce(t *testing.T) {
	w := newWorld(t)
	for _, run := range []string{"ork-p1", "ork-p2", "ork-p3"} {
		w.produce(run, w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), "a", "b")
		w.sidecars[run] = w.now.Add(-2 * time.Hour)
	}
	r := w.reclaimer(ReclaimDryRun)
	// The first pass is canceled while it examines ork-p2: it fails, and
	// the next pass resumes after the last run it reached.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.onList = func(key string, _ int) {
		if strings.Contains(key, "ork-p2") {
			cancel()
		}
	}
	failed := r.RunOnce(ctx)
	if failed.Err == nil || failed.CycleComplete {
		t.Fatalf("the canceled pass = %s complete %v, %v", failed.Outcome, failed.CycleComplete, failed.Err)
	}
	w.onList = nil
	done := r.RunOnce(context.Background())
	if done.Err != nil || !done.CycleComplete {
		t.Fatalf("the resuming pass = %s complete %v, %v", done.Outcome, done.CycleComplete, done.Err)
	}
	want := addTallies(addTallies(nil, failed.Objects), done.Objects)
	if !maps.Equal(done.Cycle, want) {
		t.Fatalf("cycle = %v, want each pass counted once: %v", done.Cycle, want)
	}
	key := TallyKey{Namespace: "default", Class: ClassArtifact, Outcome: OutcomeWouldDelete, Reason: ReasonUnreferenced}
	if got := done.Cycle[key].Objects; got < 4 || got > 6 {
		t.Fatalf("would delete %d over three runs of two objects, one of them interrupted", got)
	}
}
