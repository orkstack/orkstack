package eventsource

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
)

var epoch0 = time.Unix(1_800_000_000, 0).UTC()

func pinned() Pinned { return Pin(7*24*time.Hour, 5*time.Minute, 5*time.Minute) }

// cluster is what a workflow's commands come to: it runs what is scheduled,
// and says what is asked of it when a test tells it to.
type cluster struct {
	t   *testing.T
	m   *Machine
	now time.Time
	// live are the activities that have not closed, by index and epoch.
	live      map[[2]int64]Schedule
	cancelled map[[2]int64]bool
	load      Load
	log       []string
	ended     string
	seen      map[int64]bool
	checks    []StartCheck
	pauses    []WritePause
	purges    int
	loads     int
}

func newCluster(t *testing.T, carry *Carry) *cluster {
	t.Helper()
	return &cluster{t: t, m: New(carry, pinned(), epoch0), now: epoch0, live: map[[2]int64]Schedule{}, cancelled: map[[2]int64]bool{}, seen: map[int64]bool{},
		load: Load{Outcome: LoadFound, Target: Spec{Revision: 1, Digest: "d1", AdmissionSave: "s1"}, Instances: 2, MaxEvents: 51200, MaxBytes: 50 << 20}}
}

func key(i int32, epoch int64) [2]int64 { return [2]int64{int64(i), epoch} }

// do carries out commands; a load answers what c.load says, at once.
func (c *cluster) do(cmds []Command) {
	c.t.Helper()
	loads := 0
	for _, cmd := range cmds {
		c.log = append(c.log, fmt.Sprintf("%T%+v", cmd, cmd))
		if c.ended != "" {
			c.t.Fatalf("%T after the execution ended", cmd)
		}
		switch cmd := cmd.(type) {
		case Schedule:
			if c.seen[cmd.Epoch] {
				c.t.Fatalf("epoch %d was used twice", cmd.Epoch)
			}
			c.seen[cmd.Epoch] = true
			c.live[key(cmd.Index, cmd.Epoch)] = cmd
		case Cancel:
			if _, ok := c.live[key(cmd.Index, cmd.Epoch)]; !ok {
				c.t.Fatalf("epoch %d of index %d is cancelled and does not run", cmd.Epoch, cmd.Index)
			}
			c.cancelled[key(cmd.Index, cmd.Epoch)] = true
		case StartLoad:
			c.loads++
			loads++
		case StartPurge:
			c.purges++
		case StartCheck:
			c.checks = append(c.checks, cmd)
		case WritePause:
			c.pauses = append(c.pauses, cmd)
		case End, ContinueAsNew:
			c.ended = c.m.Continued.How
		}
	}
	c.holds()
	// An answer comes once what was asked with it is done.
	for range loads {
		if c.ended == "" {
			c.do(c.m.OnLoaded(c.now, c.load))
		}
	}
}

// holds is what is always so.
func (c *cluster) holds() {
	c.t.Helper()
	per := map[int64]int{}
	for k := range c.live {
		per[k[0]]++
	}
	stuck := map[int64]int{}
	for i, s := range c.m.slots {
		for _, a := range s.activities() {
			if a.phase == PhaseStuck {
				stuck[int64(i)]++
			}
		}
	}
	for i, n := range per {
		if n-stuck[i] > 2 {
			c.t.Fatalf("index %d has %d activities\n%s", i, n, c.tail())
		}
	}
	for i, f := range c.m.Index {
		if len(f.Fresh) > c.m.Pinned.FreshStarts {
			c.t.Fatalf("index %d had %d fresh starts in a day", i, len(f.Fresh))
		}
	}
}

func (c *cluster) tail() string {
	return fmt.Sprintf("target %+v good %+v failed %+v loaded %+v in place %t ending %q\n", c.m.Target, c.m.Good, c.m.Failed, c.m.Loaded, c.m.InPlace, c.m.ending) +
		strings.Join(c.log[max(0, len(c.log)-25):], "\n")
}

func (c *cluster) start() { c.t.Helper(); c.do(c.m.Start()) }

// wait lets d pass, waking the machine whenever it asks.
func (c *cluster) wait(d time.Duration) {
	c.t.Helper()
	end := c.now.Add(d)
	for n := 0; ; n++ {
		at := c.m.Next(c.now)
		if at.IsZero() || at.After(end) || c.ended != "" {
			c.now = end
			return
		}
		if n > 5000 {
			c.t.Fatalf("the machine asks to be woken for ever, at %s\n%s", at, c.tail())
		}
		if at.After(c.now) {
			c.now = at
		}
		c.do(c.m.Wake(c.now))
		if again := c.m.Next(c.now); c.ended == "" && !again.IsZero() && !again.After(c.now) {
			c.t.Fatalf("woken at %s, the machine asks to be woken at %s\n%s", c.now, again, c.tail())
		}
	}
}

// of is index i's one activity that is not being stopped, the newest of two.
func (c *cluster) of(i int32) Schedule {
	c.t.Helper()
	var out Schedule
	for k, s := range c.live {
		if k[0] == int64(i) && !c.cancelled[k] && s.Epoch > out.Epoch {
			out = s
		}
	}
	if out.Epoch == 0 {
		c.t.Fatalf("index %d runs nothing\n%s", i, c.tail())
	}
	return out
}

func (c *cluster) report(s Schedule, what, reason string) {
	c.t.Helper()
	c.do(c.m.OnReport(c.now, s.Index, s.Epoch, what, reason))
}

func (c *cluster) ready(s Schedule) {
	c.t.Helper()
	c.report(s, ReportStarted, "")
	c.report(s, ReportReady, "")
}

// close closes an activity with a result, or as a cancelled one.
func (c *cluster) close(s Schedule, r *Result) {
	c.t.Helper()
	delete(c.live, key(s.Index, s.Epoch))
	delete(c.cancelled, key(s.Index, s.Epoch))
	c.do(c.m.OnClosed(c.now, s.Index, s.Epoch, Closing{Result: r}))
}

// settle closes what was cancelled, until nothing is.
func (c *cluster) settle() {
	c.t.Helper()
	for n := 0; len(c.cancelled) > 0; n++ {
		if n > 200 {
			c.t.Fatalf("what closes is scheduled and cancelled for ever\n%s", c.tail())
		}
		var ks [][2]int64
		for k := range c.cancelled {
			ks = append(ks, k)
		}
		slices.SortFunc(ks, func(a, b [2]int64) int { return int(a[1] - b[1]) })
		c.close(c.live[ks[0]], nil)
	}
}

// up is a cluster with every instance ready on d1.
func up(t *testing.T) *cluster {
	t.Helper()
	c := newCluster(t, nil)
	c.start()
	for i := range c.load.Instances {
		c.ready(c.of(i))
	}
	if c.m.Good == nil || c.m.Good.Digest != "d1" || len(c.live) != 2 {
		t.Fatalf("good %+v, %d live", c.m.Good, len(c.live))
	}
	return c
}

func (c *cluster) change(rev int64, digest string) {
	c.t.Helper()
	c.load.Target = Spec{Revision: rev, Digest: digest, AdmissionSave: c.load.Target.AdmissionSave}
	c.do(c.m.OnSync())
}

// running are the activities that are not being stopped, oldest first.
func (c *cluster) running() []Schedule {
	return slices.DeleteFunc(slices.SortedFunc(maps.Values(c.live), func(a, b Schedule) int { return int(a.Epoch - b.Epoch) }),
		func(s Schedule) bool { return c.cancelled[key(s.Index, s.Epoch)] })
}

func (c *cluster) digests() []string {
	var out []string
	for k, s := range c.live {
		if !c.cancelled[k] {
			out = append(out, s.Spec.Digest)
		}
	}
	slices.Sort(out)
	return out
}

func TestMachine_ARollTriesOneIndexAtATimeBesideTheOldOne(t *testing.T) {
	c := up(t)
	old := c.of(0)
	c.change(2, "d2")
	next := c.of(0)
	if next.Epoch == old.Epoch || next.Spec.Digest != "d2" || !next.OnTrial || len(c.live) != 3 || c.cancelled[key(0, old.Epoch)] {
		t.Fatalf("%+v\n%s", next, c.tail())
	}
	if c.of(1).Spec.Digest != "d1" {
		t.Fatal("two indices are replaced at once")
	}
	// A wait for room is no failure, for as long as it takes.
	c.wait(6 * 24 * time.Hour)
	if c.m.Failed != nil || len(c.cancelled) != 0 || c.of(0).Epoch != next.Epoch {
		t.Fatalf("%+v\n%s", c.m.Failed, c.tail())
	}
	c.ready(next)
	if !c.cancelled[key(0, old.Epoch)] {
		t.Fatal("the old one is not stopped once the new one is ready")
	}
	c.settle()
	if c.m.Good.Digest != "d1" {
		t.Fatal("good before every index is")
	}
	c.ready(c.of(1))
	c.settle()
	if c.m.Good.Digest != "d2" || !slices.Equal(c.digests(), []string{"d2", "d2"}) {
		t.Fatalf("%+v %v\n%s", c.m.Good, c.digests(), c.tail())
	}
}

func TestMachine_AFailedRollIsPutBackAndNotTriedAgain(t *testing.T) {
	for name, fail := range map[string]func(*cluster, Schedule){
		"invalid": func(c *cluster, s Schedule) {
			c.report(s, ReportStarted, "")
			c.report(s, ReportInvalid, "CONFIG_INVALID")
		},
		"unchangeable": func(c *cluster, s Schedule) { c.close(s, &Result{Kind: EndUnchangeable, Reason: "CONFIG_INVALID"}) },
	} {
		t.Run(name, func(t *testing.T) {
			c := up(t)
			c.change(2, "d2")
			c.ready(c.of(0))
			c.settle()
			if c.of(0).Spec.Digest != "d2" || c.of(1).Spec.Digest != "d2" {
				t.Fatal(c.tail())
			}
			fail(c, c.of(1))
			c.settle()
			if c.m.Failed == nil || c.m.Failed.Digest != "d2" || c.m.Failed.Reason != "CONFIG_INVALID" || len(c.pauses) != 0 {
				t.Fatalf("%+v %v", c.m.Failed, c.pauses)
			}
			// Index 0 goes back beside itself; index 1 never left.
			back := c.of(0)
			if back.Spec.Digest != "d1" || back.OnTrial {
				t.Fatalf("%+v\n%s", back, c.tail())
			}
			c.ready(back)
			c.settle()
			if !slices.Equal(c.digests(), []string{"d1", "d1"}) {
				t.Fatalf("%v\n%s", c.digests(), c.tail())
			}
			before := len(c.log)
			c.do(c.m.OnSync())
			c.wait(48 * time.Hour)
			for _, l := range c.log[before:] {
				if strings.Contains(l, "Schedule") {
					t.Fatalf("the failed target was tried again: %s", l)
				}
			}
			c.change(3, "d2")
			if c.of(0).Spec.Revision != 3 {
				t.Fatal("a new target of the same digest is not tried")
			}
		})
	}
}

func TestMachine_ANewerTargetReplacesAWaitingOne(t *testing.T) {
	c := up(t)
	c.change(2, "d2")
	waiting := c.of(0)
	c.change(3, "d3")
	if !c.cancelled[key(0, waiting.Epoch)] {
		t.Fatal(c.tail())
	}
	c.settle()
	if got := c.of(0); got.Spec.Digest != "d3" || len(c.live) != 3 {
		t.Fatalf("%+v\n%s", got, c.tail())
	}
}

func TestMachine_InPlace(t *testing.T) {
	c := up(t)
	c.load.InPlace = true
	old := c.of(0)
	c.change(2, "d2")
	if !c.cancelled[key(0, old.Epoch)] || len(c.live) != 2 {
		t.Fatal(c.tail())
	}
	c.settle()
	next := c.of(0)
	if next.Spec.Digest != "d2" || !next.OnTrial || c.of(1).Spec.Digest != "d1" {
		t.Fatalf("%+v\n%s", next, c.tail())
	}
	c.close(next, &Result{Kind: EndUnchangeable, Reason: "SERVICE_NOT_READY_IN_TIME"})
	c.wait(time.Minute)
	if c.m.Failed == nil || c.of(0).Spec.Digest != "d1" || len(c.pauses) != 0 {
		t.Fatalf("%+v\n%s", c.m.Failed, c.tail())
	}
	c.load.InPlace = true
	c.change(3, "d3")
	for i := range int32(2) {
		c.settle()
		c.ready(c.of(i))
	}
	c.settle()
	if c.m.Good.Digest != "d3" || !slices.Equal(c.digests(), []string{"d3", "d3"}) {
		t.Fatalf("%v\n%s", c.digests(), c.tail())
	}
}

func TestMachine_NothingIsStoppedBecauseADocumentIsAbsent(t *testing.T) {
	for _, outcome := range []LoadOutcome{LoadMissing, LoadFailed} {
		c := up(t)
		c.load.Outcome = outcome
		c.do(c.m.OnSync())
		c.wait(72 * time.Hour)
		if len(c.cancelled) != 0 || len(c.live) != 2 || c.ended != "" || c.purges != 0 {
			t.Fatalf("%v: %s", outcome, c.tail())
		}
		// A day of it is some hundred loads, and a sync does not cut it short.
		if n := c.loads; n > 320 || n < 250 {
			t.Fatalf("%d loads in three days", n)
		}
		before := c.loads
		c.do(c.m.OnSync())
		if c.loads != before {
			t.Fatal("a sync cut the back-off short")
		}
		// What closes is still started again.
		c.close(c.of(0), &Result{Kind: EndUnmet, Reason: "X"})
		c.wait(time.Minute)
		if len(c.live) != 2 {
			t.Fatal(c.tail())
		}
		c.load.Outcome = LoadFound
		c.wait(20 * time.Minute)
		if c.m.load != loadOK {
			t.Fatal("the document that is back was not loaded")
		}
	}
}

func TestMachine_ADelete(t *testing.T) {
	for _, outcome := range []LoadOutcome{LoadDeleting, LoadDeleted} {
		c := up(t)
		c.load.Outcome = outcome
		c.do(c.m.OnSync())
		if len(c.cancelled) != 2 || c.purges != 0 {
			t.Fatal(c.tail())
		}
		c.settle()
		if c.purges != 1 {
			t.Fatal("nothing runs and nothing was purged")
		}
		c.do(c.m.OnPurged(c.now, false, true))
		c.wait(4 * time.Second)
		if c.purges != 1 {
			t.Fatal("a purge that failed was made again at once")
		}
		c.wait(time.Minute)
		if c.purges != 2 {
			t.Fatalf("%d purges", c.purges)
		}
		c.do(c.m.OnPurged(c.now, false, false))
		if c.purges != 3 {
			t.Fatal("a purge that left some is not followed by another")
		}
		c.do(c.m.OnPurged(c.now, true, false))
		if c.ended != EndedPurged {
			t.Fatalf("%q", c.ended)
		}
	}
}

func TestMachine_PauseHoldAndResume(t *testing.T) {
	c := up(t)
	c.load.Paused = true
	c.do(c.m.OnSync())
	c.settle()
	if len(c.live) != 0 || c.ended != "" {
		t.Fatal(c.tail())
	}
	c.load.Paused, c.load.ResumeSeq = false, 1
	c.do(c.m.OnSync())
	if !c.of(0).Trial || !c.of(1).Trial || c.of(0).Spec.Digest != "d1" {
		t.Fatalf("a resume's instances are on trial: %+v", c.of(0))
	}
	c.do(c.m.OnSync())
	c.close(c.of(0), &Result{Kind: EndStopped})
	c.wait(time.Minute)
	if c.of(0).Trial {
		t.Fatal("a later schedule is on trial")
	}
	c.load.Held = true
	c.do(c.m.OnSync())
	c.settle()
	if len(c.live) != 0 {
		t.Fatal(c.tail())
	}
	c.load.Held = false
	c.do(c.m.OnSync())
	if len(c.live) != 2 || c.of(0).Trial {
		t.Fatal("a released hold starts instances, and is no resume")
	}
	c.load.Instances = 1
	c.do(c.m.OnSync())
	c.settle()
	if len(c.live) != 1 {
		t.Fatal(c.tail())
	}
}

func TestMachine_TheRestartDelay(t *testing.T) {
	c := up(t)
	var got []time.Duration
	for range 12 {
		s := c.of(0)
		c.ready(s)
		at := c.now
		c.close(s, &Result{Kind: EndStopped, Reason: "EVENT_SOURCE_NOT_HELD"})
		for len(c.live) != 2 {
			c.wait(time.Second)
		}
		got = append(got, c.of(0).at(c).Sub(at).Round(time.Second))
	}
	want := []time.Duration{1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300}
	for i := range want {
		if got[i] != want[i]*time.Second {
			t.Fatalf("delays %v", got)
		}
	}
	// A ready alone resets nothing; ten healthy minutes do.
	c.ready(c.of(0))
	c.wait(10 * time.Minute)
	at := c.now
	c.close(c.of(0), &Result{Kind: EndStopped})
	for len(c.live) != 2 {
		c.wait(time.Second)
	}
	if d := c.now.Sub(at); d > 2*time.Second {
		t.Fatalf("%s after a healthy run", d)
	}
	if len(c.m.Index[0].Fresh) != 0 {
		t.Fatal("a stop counts as a fresh start")
	}
	// The other index was never touched.
	if c.m.Index[1].Stops != 0 {
		t.Fatal("only the index concerned is")
	}
}

// at is when the schedule was made: the cluster's clock does not move between
// a wake and what it schedules.
func (s Schedule) at(c *cluster) time.Time { return c.now }

func TestMachine_ReArmsAndTheirDailyBound(t *testing.T) {
	c := up(t)
	for n := 1; n <= 6; n++ {
		old := c.of(0)
		c.report(old, ReportHealthy, "")
		next := c.of(0)
		if next.Epoch == old.Epoch || next.OnTrial || next.Spec.Digest != "d1" {
			t.Fatalf("%d: %+v\n%s", n, next, c.tail())
		}
		c.ready(next)
		c.settle()
		c.wait(time.Minute)
	}
	old := c.of(0)
	c.report(old, ReportHealthy, "")
	if c.of(0).Epoch != old.Epoch {
		t.Fatal("a seventh in a day")
	}
	if slices.Contains(c.m.Now(c.now).Slots[0].Awaits[old.Epoch], ReportHealthy) {
		t.Fatal("healthy is awaited past the bound")
	}
	c.wait(24 * time.Hour)
	if !slices.Contains(c.m.Now(c.now).Slots[0].Awaits[old.Epoch], ReportHealthy) {
		t.Fatal("the bound does not lift by itself")
	}
	c.report(old, ReportHealthy, "")
	if c.of(0).Epoch == old.Epoch {
		t.Fatal("not re-armed once it lifted")
	}
}

func TestMachine_Rotation(t *testing.T) {
	c := up(t)
	old := c.of(0)
	c.wait(7*24*time.Hour - time.Hour - time.Second)
	if c.of(0).Epoch != old.Epoch {
		t.Fatal("rotated early")
	}
	c.wait(2 * time.Second)
	next := c.of(0)
	if next.Epoch == old.Epoch || c.cancelled[key(0, old.Epoch)] || len(c.m.Index[0].Fresh) != 0 {
		t.Fatalf("%+v\n%s", next, c.tail())
	}
	// Nothing could take it: the old one works on until its own bound, and
	// what waits takes its place.
	c.wait(21 * 24 * time.Hour)
	c.close(old, &Result{Kind: EndRotated})
	if c.of(0).Epoch != next.Epoch || len(c.live) < 2 {
		t.Fatal(c.tail())
	}
}

func TestMachine_AStuckStopHoldsNothing(t *testing.T) {
	c := up(t)
	c.load.Paused = true
	c.do(c.m.OnSync())
	c.wait(2*time.Minute + time.Second)
	if c.m.running() != 0 {
		t.Fatal("a stop that does not end holds the source")
	}
	c.load.Paused, c.load.ResumeSeq = false, 1
	c.do(c.m.OnSync())
	if len(c.live) != 4 {
		t.Fatalf("%d live\n%s", len(c.live), c.tail())
	}
}

func TestMachine_WhatCannotChangePauses(t *testing.T) {
	c := up(t)
	c.close(c.of(0), &Result{Kind: EndUnchangeable, Reason: "IMAGE_NOT_PERMITTED"})
	if len(c.pauses) != 1 || c.pauses[0] != (WritePause{Code: pauseInstanceFailed, Refusal: "IMAGE_NOT_PERMITTED", Revision: 1}) {
		t.Fatalf("%+v", c.pauses)
	}
	c.load.Paused = true
	c.do(c.m.OnPaused(c.now, true))
	c.settle()
	if len(c.live) != 0 {
		t.Fatal(c.tail())
	}
	// A write that failed is made again by the next cause.
	c = up(t)
	c.close(c.of(0), &Result{Kind: EndUnchangeable, Reason: "X"})
	c.do(c.m.OnPaused(c.now, false))
	c.wait(time.Minute)
	c.close(c.of(0), &Result{Kind: EndUnchangeable, Reason: "X"})
	if len(c.pauses) != 2 {
		t.Fatalf("%+v", c.pauses)
	}
}

// While a pause is being written, and until the document that has it is
// loaded, nothing is started: what could not work is not tried meanwhile, and
// where the write fails it waits as any restart does.
func TestMachine_NothingStartsWhileAPauseIsWritten(t *testing.T) {
	c := up(t)
	c.close(c.of(0), &Result{Kind: EndUnchangeable, Reason: "X"})
	c.wait(time.Minute)
	if len(c.live) != 1 {
		t.Fatalf("it is started again before the pause is written\n%s", c.tail())
	}
	// A load that was asked for before the pause was written does not say that
	// it was not.
	c.do(c.m.OnSync())
	if len(c.live) != 1 {
		t.Fatal(c.tail())
	}
	// It is written, and the next load fails: still nothing.
	c.load.Outcome = LoadFailed
	c.do(c.m.OnPaused(c.now, true))
	c.wait(time.Hour)
	if len(c.live) != 1 {
		t.Fatalf("%d live\n%s", len(c.live), c.tail())
	}
	// Somebody lifted it meanwhile.
	c.load.Outcome = LoadFound
	c.wait(20 * time.Minute)
	if len(c.live) != 2 {
		t.Fatalf("%d live\n%s", len(c.live), c.tail())
	}

	c = up(t)
	for n, want := range []time.Duration{1, 2, 4, 8} {
		c.close(c.of(0), &Result{Kind: EndUnchangeable, Reason: "X"})
		c.do(c.m.OnPaused(c.now, false))
		c.wait(want*time.Second - time.Millisecond)
		if len(c.live) != 1 {
			t.Fatalf("%d: a pause that cannot be written starts it again early\n%s", n, c.tail())
		}
		c.wait(2 * time.Millisecond)
		if len(c.live) != 2 || len(c.pauses) != n+1 {
			t.Fatalf("%d: %d live, %d pauses", n, len(c.live), len(c.pauses))
		}
	}
}

func TestMachine_AnEndNoRowHoldsNeverPauses(t *testing.T) {
	for _, kind := range []EndKind{EndUnknown, EndUnmet} {
		c := up(t)
		c.close(c.of(0), &Result{Kind: kind, Reason: "SOMETHING_LATER"})
		c.wait(time.Minute)
		if len(c.pauses) != 0 || len(c.live) != 2 {
			t.Fatalf("%v: %v\n%s", kind, c.pauses, c.tail())
		}
		if (c.m.Now(c.now).UnknownEnd != nil) != (kind == EndUnknown) {
			t.Fatalf("%v: %+v", kind, c.m.Now(c.now).UnknownEnd)
		}
	}
}

func TestMachine_AnUnreadableSpecPausesPastItsBoundAlone(t *testing.T) {
	c := up(t)
	cycle := func() {
		c.close(c.of(0), &Result{Kind: EndJudgment, Unread: "spec"})
		c.wait(time.Hour + time.Second)
	}
	for range 23 {
		cycle()
	}
	if len(c.pauses) != 0 {
		t.Fatalf("paused early: %v", c.pauses)
	}
	for range 2 {
		cycle()
	}
	if len(c.pauses) == 0 || c.pauses[0].Refusal != refusalUnread || c.pauses[0].Detail != "unread=spec" {
		t.Fatalf("%+v", c.pauses)
	}
	// An accepting confirmation of the index clears it, and no other does.
	c = up(t)
	cycle()
	first := c.m.UnreadFirstAt[0]
	for _, j := range []struct{ observed, at time.Time }{{first.Add(time.Second), c.now}, {first, first.Add(-time.Second)}} {
		c.m.OnJudged(0, j.observed, j.at)
		if _, ok := c.m.UnreadFirstAt[0]; !ok {
			t.Fatalf("cleared by %+v", j)
		}
	}
	c.m.OnJudged(1, first, c.now)
	c.m.OnJudged(0, first, c.now)
	if _, ok := c.m.UnreadFirstAt[0]; ok {
		t.Fatal("not cleared by its own")
	}
	// A new save clears every index's.
	cycle()
	c.load.Target.AdmissionSave = "s2"
	c.do(c.m.OnSync())
	if len(c.m.UnreadFirstAt) != 0 || c.m.Good.AdmissionSave != "s2" {
		t.Fatalf("%v %+v", c.m.UnreadFirstAt, c.m.Good)
	}
	// One of an older save sets nothing.
	c.close(c.of(1), &Result{Kind: EndJudgment, Unread: "record"})
	if len(c.m.UnreadFirstAt) != 0 {
		t.Fatal("an older save's counted")
	}
}

func TestMachine_ARevokeIsDecidedByItsCheckAlone(t *testing.T) {
	c := up(t)
	c.m.OnRevoked(c.now, "grant g", "other", "s1")
	c.m.OnRevoked(c.now, "grant g", "d1", "old")
	if c.m.Revoke != nil {
		t.Fatal("a revoke of what nothing runs was taken")
	}
	c.m.OnRevoked(c.now, "grant g", "d1", "s1")
	c.wait(5*time.Minute - time.Second)
	if len(c.checks) != 0 {
		t.Fatal("checked early")
	}
	c.wait(2 * time.Second)
	if len(c.checks) != 1 || len(c.checks[0].Specs) != 1 || len(c.pauses) != 0 {
		t.Fatalf("%+v", c.checks)
	}
	c.do(c.m.OnChecked(c.now, false, false, ""))
	if c.m.Revoke == nil || len(c.pauses) != 0 {
		t.Fatal("a check that could not read decided")
	}
	c.wait(5*time.Minute + time.Second)
	c.do(c.m.OnChecked(c.now, true, false, ""))
	if c.m.Revoke != nil || len(c.pauses) != 0 {
		t.Fatal("an allowed check does not clear it")
	}
	c.m.OnRevoked(c.now, "grant g", "d1", "s1")
	c.wait(5*time.Minute + time.Second)
	c.do(c.m.OnChecked(c.now, false, true, "grant g: restore it"))
	if len(c.pauses) != 1 || c.pauses[0].Refusal != refusalRevoked || c.pauses[0].Detail != "grant g: restore it" {
		t.Fatalf("%+v", c.pauses)
	}

	// Past the bound with no answer it pauses, naming what was read.
	c = up(t)
	c.m.OnRevoked(c.now, "audience sts", "d1", "s1")
	for range 14 {
		c.wait(5*time.Minute + time.Second)
		if len(c.checks) > 0 {
			c.do(c.m.OnChecked(c.now, false, false, ""))
		}
	}
	if len(c.pauses) != 1 || !strings.Contains(c.pauses[0].Detail, "recheck=unanswered audience sts") {
		t.Fatalf("%+v", c.pauses)
	}

	// A pause ends it, a resume of what runs does not.
	c = up(t)
	c.m.OnRevoked(c.now, "writer", "d1", "s1")
	c.do(c.m.OnSync())
	if c.m.Revoke == nil {
		t.Fatal("a load of an unchanged source erased it")
	}
	c.load.Paused = true
	c.do(c.m.OnSync())
	if c.m.Revoke != nil {
		t.Fatal("a pause does not end it")
	}
}

func TestMachine_Reroute(t *testing.T) {
	c := newCluster(t, nil)
	c.start()
	s := c.of(0)
	c.do(c.m.OnReroute(c.now, 0, s.Epoch+50))
	if len(c.cancelled) != 0 {
		t.Fatal("another epoch's")
	}
	c.do(c.m.OnReroute(c.now, 0, s.Epoch))
	c.settle()
	if c.of(0).Epoch == s.Epoch || len(c.m.Index[0].Fresh) != 1 {
		t.Fatal(c.tail())
	}
	c.ready(c.of(0))
	before := c.of(0)
	c.do(c.m.OnReroute(c.now, 0, before.Epoch))
	if len(c.cancelled) != 0 {
		t.Fatal("one that started is rerouted")
	}
	for range 10 {
		c.do(c.m.OnReroute(c.now, 1, c.of(1).Epoch))
		c.settle()
	}
	if n := len(c.m.Index[1].Fresh); n != 6 {
		t.Fatalf("%d a day", n)
	}
}

func TestMachine_CarryingOn(t *testing.T) {
	// Idle and past its budget.
	c := up(t)
	c.do(c.m.OnHistory(c.now, 3000, 0))
	if c.ended != "" || len(c.cancelled) != 0 {
		t.Fatal("a busy source was stopped for its budget")
	}
	c.load.Paused = true
	c.do(c.m.OnSync())
	c.settle()
	if c.ended != EndedIdle {
		t.Fatalf("%q", c.ended)
	}
	// Forced, but not past a pending replacement, nor a failing load.
	c = up(t)
	c.change(2, "d2")
	c.do(c.m.OnHistory(c.now, 20001, 0))
	if len(c.cancelled) != 0 {
		t.Fatal("a pending replacement was stopped")
	}
	for i := range int32(2) {
		c.ready(c.of(i))
		c.settle()
		if c.ended != "" {
			break
		}
	}
	c.settle()
	if c.ended != EndedForced {
		t.Fatalf("%q\n%s", c.ended, c.tail())
	}
	c = up(t)
	c.load.Outcome = LoadMissing
	c.do(c.m.OnSync())
	c.do(c.m.OnHistory(c.now, 30000, 0))
	if len(c.cancelled) != 0 {
		t.Fatal("forced while a load fails")
	}
	// The cluster's own line waits for nothing.
	c.do(c.m.OnHistory(c.now, 40961, 0))
	c.settle()
	if c.ended != EndedLimit {
		t.Fatalf("%q", c.ended)
	}
	c = up(t)
	c.do(c.m.OnHistory(c.now, 10, 11<<20))
	c.settle()
	if c.ended != EndedForced {
		t.Fatalf("bytes: %q", c.ended)
	}
}

func TestMachine_ANewExecutionNeverDependsOnALoad(t *testing.T) {
	c := up(t)
	c.change(2, "d2")
	c.do(c.m.OnHandover(c.now))
	c.settle()
	if c.ended != EndedHandover || len(c.live) != 0 {
		t.Fatalf("%q", c.ended)
	}
	body, err := c.m.Carry.Encode()
	if err != nil {
		t.Fatal(err)
	}
	carry, err := DecodeCarry(body)
	if err != nil {
		t.Fatal(err)
	}
	next := newCluster(t, &carry)
	next.load.Outcome = LoadFailed
	next.start()
	// What ran, on the last good one; and the roll the last load asked for.
	if !slices.Equal(next.digests(), []string{"d1", "d1", "d2"}) {
		t.Fatalf("%v: what the carry says ran is not started", next.digests())
	}
	for k := range next.live {
		if c.seen[k[1]] {
			t.Fatal("an epoch was used by two executions")
		}
	}
	for name, edit := range map[string]func(*LoadedFacts){"paused": func(l *LoadedFacts) { l.Paused = true }, "held": func(l *LoadedFacts) { l.Held = true },
		"deleting": func(l *LoadedFacts) { l.Deleting = true }} {
		again, err := DecodeCarry(body)
		if err != nil {
			t.Fatal(err)
		}
		edit(again.Loaded)
		n := newCluster(t, &again)
		n.load.Outcome = LoadFailed
		n.start()
		if len(n.live) != 0 {
			t.Errorf("%s: started %v", name, n.digests())
		}
	}
	// Without a carry nothing is started from nothing.
	none := newCluster(t, nil)
	none.load.Outcome = LoadFailed
	none.start()
	if len(none.live) != 0 {
		t.Fatal(none.tail())
	}
	// A load that succeeds says what is wanted now.
	carry.NextEpoch = epoch0.Unix() + 1000
	n := newCluster(t, &carry)
	n.load.Instances = 1
	n.start()
	if len(n.live) != 1 || n.of(0).Epoch < epoch0.Unix()+1000 {
		t.Fatalf("%v", n.live)
	}
}

func TestCarry_KeepsWhatItDoesNotKnow(t *testing.T) {
	body := `{"format":1,"later":{"a":[1]},"nextEpoch":4,"pinned":{"cancelWait":120000000000},"target":{"revision":2,"digest":"d"}}`
	c, err := DecodeCarry([]byte(body))
	if err != nil || c.NextEpoch != 4 || c.Pinned.CancelWait != 2*time.Minute || c.Target.Revision != 2 {
		t.Fatalf("%+v, %v", c, err)
	}
	out, err := c.Encode()
	if err != nil || !strings.Contains(string(out), `"later":{"a":[1]}`) {
		t.Fatalf("%s, %v", out, err)
	}
	if _, err := DecodeCarry([]byte(`{"format":2}`)); err == nil || !strings.Contains(err.Error(), "2") {
		t.Fatalf("%v", err)
	}
	for _, bad := range []string{``, `{`, `[]`, `{"format":"1"}`, `{"format":1,"nextEpoch":"x"}`} {
		if _, err := DecodeCarry([]byte(bad)); err == nil {
			t.Errorf("%q was read", bad)
		}
	}
}

// TestMachine_AnyOrderOfEvents: whatever happens in whatever order, an index
// has at most two activities, no epoch is used twice, nothing is stopped for
// an absent document, nothing is done after an end, and what is wanted runs
// once the world is quiet.
func TestMachine_AnyOrderOfEvents(t *testing.T) {
	for seed := range uint64(400) {
		r := rand.New(rand.NewPCG(seed, 7))
		c := newCluster(t, nil)
		c.start()
		pick := func() (Schedule, bool) {
			all := slices.SortedFunc(maps.Values(c.live), func(a, b Schedule) int { return int(a.Epoch - b.Epoch) })
			if len(all) == 0 {
				return Schedule{}, false
			}
			return all[r.IntN(len(all))], true
		}
		rev := int64(1)
		for range 120 {
			if c.ended != "" {
				break
			}
			s, ok := pick()
			switch n := r.IntN(14); {
			case n < 3 && ok:
				c.report(s, []string{ReportStarted, ReportReady, ReportInvalid, ReportHealthy}[r.IntN(4)], "X")
			case n < 5 && ok:
				kind := EndKind(r.IntN(int(EndUnchangeable) + 1))
				c.close(s, &Result{Kind: kind, Reason: "X", Unread: "spec"})
			case n == 5:
				rev++
				c.load.InPlace = r.IntN(3) == 0
				c.change(rev, fmt.Sprintf("d%d", r.IntN(4)))
			case n == 6:
				c.load.Outcome = []LoadOutcome{LoadFound, LoadFound, LoadMissing, LoadFailed}[r.IntN(4)]
				before := len(c.cancelled)
				c.do(c.m.OnSync())
				if c.load.Outcome != LoadFound && len(c.cancelled) != before {
					t.Fatalf("seed %d: stopped for an absent document\n%s", seed, c.tail())
				}
			case n == 7:
				c.load.Paused = !c.load.Paused
				if !c.load.Paused {
					c.load.ResumeSeq++
				}
				c.do(c.m.OnSync())
			case n == 8:
				c.load.Instances = int32(r.IntN(4))
				c.do(c.m.OnSync())
			case n == 9:
				c.settle()
			case n == 10 && ok:
				c.do(c.m.OnReroute(c.now, s.Index, s.Epoch))
			case n == 11:
				c.m.OnRevoked(c.now, "grant", c.load.Target.Digest, "s1")
			case n == 12 && len(c.checks) > 0 && c.m.checking:
				c.do(c.m.OnChecked(c.now, r.IntN(2) == 0, r.IntN(2) == 0, ""))
			case len(c.pauses) > 0 && c.m.pausing == pauseWriting:
				c.do(c.m.OnPaused(c.now, r.IntN(2) == 0))
			default:
				c.wait(time.Duration(r.IntN(4000)) * time.Second)
			}
		}
		if c.ended != "" {
			continue
		}
		// Quiet: the document is there, not paused, and what is cancelled
		// closes.
		c.load.Outcome, c.load.Paused, c.load.InPlace = LoadFound, false, false
		c.load.Instances = 2
		c.load.ResumeSeq++
		rev++
		c.load.Target = Spec{Revision: rev, Digest: "final", AdmissionSave: "s1"}
		c.wait(20 * time.Minute)
		c.do(c.m.OnSync())
		for range 30 {
			if c.m.pausing == pauseWriting {
				c.do(c.m.OnPaused(c.now, true))
			}
			c.settle()
			c.wait(6 * time.Minute)
			for _, s := range c.running() {
				c.ready(s)
			}
		}
		c.settle()
		if c.m.Good == nil || c.m.Good.Digest != "final" || !slices.Equal(c.digests(), []string{"final", "final"}) {
			t.Fatalf("seed %d: good %+v, failed %+v, running %v\n%s", seed, c.m.Good, c.m.Failed, c.digests(), c.tail())
		}
	}
}

// TestMachine_AReplacementThatFailsWaits: one that closes by itself is
// scheduled again after the index's delay, and never at once.
func TestMachine_AReplacementThatFailsWaits(t *testing.T) {
	for _, kind := range []EndKind{EndUnmet, EndUnknown, EndStopped} {
		c := up(t)
		c.change(2, "d2")
		for n, want := range []time.Duration{1, 2, 4, 8} {
			before := c.of(0)
			c.close(before, &Result{Kind: kind, Reason: "X"})
			if c.of(0).Spec.Digest != "d1" {
				t.Fatalf("%v: scheduled again at once\n%s", kind, c.tail())
			}
			c.wait(want*time.Second - time.Millisecond)
			if c.of(0).Spec.Digest != "d1" {
				t.Fatalf("%v: %d: scheduled early", kind, n)
			}
			c.wait(2 * time.Millisecond)
			if got := c.of(0); got.Spec.Digest != "d2" || got.Epoch == before.Epoch {
				t.Fatalf("%v: %d: not scheduled after %s\n%s", kind, n, want*time.Second, c.tail())
			}
		}
	}
	c := up(t)
	c.change(2, "d2")
	c.close(c.of(0), &Result{Kind: EndJudgment, Unread: "record"})
	c.wait(59 * time.Minute)
	if c.of(0).Spec.Digest != "d1" {
		t.Fatal("what could not be judged was scheduled again at once")
	}
	c.wait(2 * time.Minute)
	if c.of(0).Spec.Digest != "d2" {
		t.Fatal(c.tail())
	}
}

// carried is the machine's carry as the next execution reads it.
func (c *cluster) carried() *Carry {
	c.t.Helper()
	body, err := c.m.Carry.Encode()
	if err != nil {
		c.t.Fatal(err)
	}
	carry, err := DecodeCarry(body)
	if err != nil {
		c.t.Fatal(err)
	}
	return &carry
}

// TestMachine_WhatCrossesAContinuation: a change asked for in place stays so,
// an index that was being restarted in turn waits its delay again, and the
// trial of a resume is for each index once.
func TestMachine_WhatCrossesAContinuation(t *testing.T) {
	c := up(t)
	c.load.InPlace = true
	c.change(2, "d2")
	next := newCluster(t, c.carried())
	next.load = c.load
	next.start()
	if next.of(0).Spec.Digest != "d2" || next.of(1).Spec.Digest != "d1" {
		t.Fatal(next.tail())
	}
	next.ready(next.of(0))
	if len(next.live) != 2 || len(next.cancelled) != 1 {
		t.Fatalf("a change in place became a surge\n%s", next.tail())
	}

	c = up(t)
	for range 9 {
		c.close(c.of(0), &Result{Kind: EndStopped})
		for len(c.live) != 2 {
			c.wait(time.Second)
		}
	}
	next = newCluster(t, c.carried())
	next.start()
	if len(next.live) != 1 || next.of(1).Epoch == 0 {
		t.Fatalf("%v", next.live)
	}
	next.wait(5*time.Minute + time.Second)
	if len(next.live) != 2 {
		t.Fatal(next.tail())
	}

	// The carry is the next execution's own.
	carry := c.carried()
	m := New(carry, pinned(), epoch0)
	m.Good.Digest, m.Index[0] = "changed", IndexFacts{Stops: 99}
	if carry.Good.Digest == "changed" || carry.Index[0].Stops == 99 {
		t.Fatal("an execution writes into the carry it was given")
	}
}

// TestMachine_ATrialIsForEachIndexOnce: whatever delays the start a resume
// asked for, it is on trial, and no later one is.
func TestMachine_ATrialIsForEachIndexOnce(t *testing.T) {
	c := up(t)
	c.load.Paused = true
	c.do(c.m.OnSync())
	// Resumed while what was stopped is stopping, and held.
	c.load.Paused, c.load.Held, c.load.ResumeSeq = false, true, 1
	c.do(c.m.OnSync())
	c.settle()
	if len(c.live) != 0 {
		t.Fatal(c.tail())
	}
	c.load.Held = false
	c.do(c.m.OnSync())
	for i := range int32(2) {
		if !c.of(i).Trial {
			t.Fatalf("index %d lost the resume's trial", i)
		}
		c.close(c.of(i), &Result{Kind: EndUnmet})
	}
	c.wait(time.Minute)
	for i := range int32(2) {
		if c.of(i).Trial {
			t.Fatalf("index %d is on trial twice", i)
		}
	}
	// More instances are no resume.
	c.load.Instances = 3
	c.do(c.m.OnSync())
	if c.of(2).Trial {
		t.Fatal("an added instance is on trial")
	}
	next := newCluster(t, c.carried())
	next.load = c.load
	next.start()
	next.wait(time.Minute)
	for i := range int32(3) {
		if next.of(i).Trial {
			t.Fatalf("index %d is on trial in the next execution", i)
		}
	}
}

// TestMachine_ACheckIsOfItsOwnRevoke: an answer that comes once its revoke has
// ended decides nothing of another.
func TestMachine_ACheckIsOfItsOwnRevoke(t *testing.T) {
	c := up(t)
	c.m.OnRevoked(c.now, "grant a", "d1", "s1")
	c.wait(5*time.Minute + time.Second)
	c.load.Paused = true
	c.do(c.m.OnSync())
	c.settle()
	c.load.Paused, c.load.ResumeSeq = false, 1
	c.do(c.m.OnSync())
	c.wait(time.Minute)
	c.m.OnRevoked(c.now, "grant b", "d1", "s1")
	c.do(c.m.OnChecked(c.now, true, false, ""))
	if c.m.Revoke == nil || c.m.Revoke.Cause != "grant b" {
		t.Fatal("an old answer cleared a new revoke")
	}
	c.do(c.m.OnChecked(c.now, false, true, "grant a"))
	if len(c.pauses) != 0 {
		t.Fatal("an old answer paused")
	}
}

// TestMachine_NoFreshStartsAtAll: a deployment that allows none has no bound
// to lift.
func TestMachine_NoFreshStartsAtAll(t *testing.T) {
	c := newCluster(t, nil)
	c.m.Pinned.FreshStarts = 0
	c.start()
	for i := range int32(2) {
		c.ready(c.of(i))
		c.report(c.of(i), ReportHealthy, "")
	}
	c.close(c.of(0), &Result{Kind: EndUnmet})
	c.wait(48 * time.Hour)
	if len(c.live) != 2 || c.of(0).CompleteAfterHealthy {
		t.Fatal(c.tail())
	}
}

// TestMachine_WhatWasReArmedOnAFailedTargetDoesNotHoldItThere: the return to
// the last good one takes the place of a re-arm that waits on the failed one.
func TestMachine_WhatWasReArmedOnAFailedTargetDoesNotHoldItThere(t *testing.T) {
	c := up(t)
	c.change(2, "d2")
	c.ready(c.of(0))
	c.settle()
	c.report(c.of(0), ReportHealthy, "")
	waiting := c.of(0)
	if waiting.Spec.Digest != "d2" {
		t.Fatal(c.tail())
	}
	c.report(c.of(1), ReportInvalid, "CONFIG_INVALID")
	c.settle()
	if got := c.of(0); got.Spec.Digest != "d1" {
		t.Fatalf("%+v\n%s", got, c.tail())
	}
}

// TestMachine_AReRoutedReArmMayBeAskedAgain: what asked for it is awaited
// once more.
func TestMachine_AReRoutedReArmMayBeAskedAgain(t *testing.T) {
	c := up(t)
	first := c.of(0)
	c.report(first, ReportHealthy, "")
	c.do(c.m.OnReroute(c.now, 0, c.of(0).Epoch))
	c.settle()
	if c.of(0).Epoch != first.Epoch || !slices.Contains(c.m.Now(c.now).Slots[0].Awaits[first.Epoch], ReportHealthy) {
		t.Fatal(c.tail())
	}
	c.report(first, ReportHealthy, "")
	if c.of(0).Epoch == first.Epoch {
		t.Fatal("not re-armed again")
	}
}

// TestMachine_PastItsLineNothingIsScheduled: an execution that is to end
// schedules nothing it would cancel.
func TestMachine_PastItsLineNothingIsScheduled(t *testing.T) {
	c := up(t)
	s := c.of(0)
	c.m.events = 45000
	before := len(c.log)
	c.close(s, &Result{Kind: EndRotated})
	for _, l := range c.log[before:] {
		if strings.Contains(l, "Schedule") {
			t.Fatal(l)
		}
	}
	c.settle()
	if c.ended != EndedLimit {
		t.Fatalf("%q", c.ended)
	}
}

// TestNow_IsWrittenOneWay: a memo of one state is one text, whatever order its
// maps are in, and no map of it is of a thing that does not run.
func TestNow_IsWrittenOneWay(t *testing.T) {
	c := up(t)
	c.change(2, "d2")
	first, err := c.m.Now(c.now).Encode()
	if err != nil {
		t.Fatal(err)
	}
	for range 50 {
		if again, _ := c.m.Now(c.now).Encode(); string(again) != string(first) {
			t.Fatalf("%s\n%s", first, again)
		}
	}
	if n := c.m.Now(c.now); len(n.Slots) != 2 || n.Slots[0].Replacement == nil || n.Slots[0].Replacement.Digest != "d2" || n.Wanted != 2 || n.Running != 3 {
		t.Fatalf("%s", first)
	}
}

// A document that stays missing is said to be once a day, and again when it
// goes missing again.
func TestMachine_MissingIsSaidOnceADay(t *testing.T) {
	c := up(t)
	c.load.Outcome = LoadMissing
	c.log = nil
	c.do(c.m.OnSync())
	said := c.m.missingSaid
	if !said.Equal(c.now) {
		t.Fatalf("%s", said)
	}
	c.wait(23 * time.Hour)
	if !c.m.missingSaid.Equal(said) {
		t.Fatal("said again within a day")
	}
	c.wait(90 * time.Minute)
	if again := c.m.missingSaid; again.Sub(said) < 24*time.Hour || again.Sub(said) > 25*time.Hour {
		t.Fatalf("said again after %s", again.Sub(said))
	}
	c.load.Outcome = LoadFound
	c.wait(20 * time.Minute)
	if !c.m.missingSaid.IsZero() {
		t.Fatal("it is found, and still said to be missing")
	}
}

// A rotation whose replacement fails is made again after the index's delay,
// and asks for nothing meanwhile.
func TestMachine_ARotationThatFailsWaits(t *testing.T) {
	c := up(t)
	first := c.of(0)
	c.wait(7*24*time.Hour - 30*time.Minute)
	for n := range 12 {
		r := c.of(0)
		if r.Epoch == first.Epoch {
			t.Fatalf("%d: not rotated\n%s", n, c.tail())
		}
		c.close(r, &Result{Kind: EndUnmet, Reason: "X"})
		if at := c.m.Next(c.now); !at.After(c.now) {
			t.Fatalf("%d: it asks to be woken at %s, at %s", n, at, c.now)
		}
		c.wait(5*time.Minute + time.Second)
	}
	if c.ended != "" {
		t.Fatalf("it ended %s", c.ended)
	}
}

// A revoke whose pause cannot be written is checked again, and paused then.
func TestMachine_ARevokeOutlivesAPauseThatCannotBeWritten(t *testing.T) {
	c := up(t)
	c.m.OnRevoked(c.now, "grant g", "d1", "s1")
	c.wait(5*time.Minute + time.Second)
	c.do(c.m.OnChecked(c.now, false, true, "grant g"))
	c.do(c.m.OnPaused(c.now, false))
	if c.m.Revoke == nil || len(c.pauses) != 1 {
		t.Fatalf("%+v, %d pauses", c.m.Revoke, len(c.pauses))
	}
	c.wait(5*time.Minute + time.Second)
	if len(c.checks) != 2 {
		t.Fatalf("%d checks", len(c.checks))
	}
	c.do(c.m.OnChecked(c.now, false, true, "grant g"))
	c.load.Paused = true
	c.do(c.m.OnPaused(c.now, true))
	if c.m.Revoke != nil || len(c.pauses) != 2 {
		t.Fatalf("%+v, %d pauses", c.m.Revoke, len(c.pauses))
	}
}
