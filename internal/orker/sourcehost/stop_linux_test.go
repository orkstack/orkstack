package sourcehost

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// running is an instance run until the test stops it.
type running struct {
	stop func()
	done chan Ending
}

func runInstance(in *Instance) running {
	ctx, cancel := context.WithCancel(context.Background())
	r := running{stop: cancel, done: make(chan Ending, 1)}
	go func() { r.done <- in.Run(ctx) }()
	return r
}

// stopped asks for the stop and returns how the instance ended and how long it
// took, on the bubble's clock.
func (r running) stopped() (Ending, time.Duration) {
	began := time.Now()
	r.stop()
	e := <-r.done
	return e, time.Since(began)
}

func pollSeq(t *testing.T, src *laneSource) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(src.ask, LanePath(0, StepPoll)))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func stopNotice(src *laneSource) bool {
	_, err := os.Stat(filepath.Join(src.ask, "stop.json"))
	return err == nil
}

// lateDoor answers each offer after a time, as an engine under load does, and
// gives up when its caller does.
type lateDoor struct {
	*fakeDoor
	after time.Duration
	kind  AnswerKind
}

func (d lateDoor) Offer(ctx context.Context, _ Offer) (Answer, error) {
	select {
	case <-ctx.Done():
		return Answer{}, ctx.Err()
	case <-time.After(d.after):
		return Answer{Kind: d.kind, RunID: "ork-1"}, nil
	}
}

func slowDoor(after time.Duration, kind AnswerKind) Door { return lateDoor{&fakeDoor{}, after, kind} }

// TestStop_AnIdleInstance: a stop of an instance with nothing in hand writes its
// notice and ends at once, with no further poll.
func TestStop_AnIdleInstance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		r := runInstance(in)
		waitFor(t, "polls", func() bool { return in.Drivers[0].Lane.Seq() > 3 })
		e, took := r.stopped()
		if e != (Ending{}) || took > time.Second || !stopNotice(src) {
			t.Fatalf("%+v after %v, notice %v", e, took, stopNotice(src))
		}
		before := pollSeq(t, src)
		time.Sleep(time.Minute)
		if pollSeq(t, src) != before {
			t.Fatal("a stopped instance polled")
		}
	})
}

// TestStop_APollNobodyAnswered: a poll the container is holding open is left to
// it: the stop does not wait for its answer.
func TestStop_APollNobodyAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 2, readyStatus)
		src.longPolls.Store(true)
		r := runInstance(in)
		waitFor(t, "both polls", func() bool { return in.Drivers[0].Lane.Seq() == 1 && in.Drivers[1].Lane.Seq() == 1 })
		if e, took := r.stopped(); e != (Ending{}) || took > time.Second {
			t.Fatalf("%+v after %v", e, took)
		}
	})
}

// TestStop_AMessageHeldIsGivenBack: what waits is released at once, and its
// verdict is written before the instance ends.
func TestStop_AMessageHeldIsGivenBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{offers: []func(Offer) (Answer, error){unreachable[Offer]()}}
		in, src, rec := newInstanceFixture(t, door, 1, readyStatus, `{"status":"PAID","id":"1"}`)
		in.Config.Park = 5 * time.Minute
		r := runInstance(in)
		waitFor(t, "a message held", func() bool {
			in.mu.Lock()
			defer in.mu.Unlock()
			return len(in.held) == 1
		})
		e, took := r.stopped()
		if e != (Ending{}) || took > time.Second {
			t.Fatalf("%+v after %v", e, took)
		}
		src.mu.Lock()
		defer src.mu.Unlock()
		if src.verdicts[ActionRelease] != 1 || len(rec.receipts) != 1 || rec.receipts[0].Outcome != artifacts.ReceiptReleased {
			t.Fatalf("verdicts %v, receipts %+v", src.verdicts, rec.receipts)
		}
	})
}

// TestStop_ACallInFlight: an offer in flight has five seconds to answer, and its
// verdict is then given; one that does not answer holds the stop no longer, and
// is given no verdict: the container gives the message back.
func TestStop_ACallInFlight(t *testing.T) {
	for name, tc := range map[string]struct {
		after time.Duration
		took  time.Duration
		acks  int
	}{"answered in time": {4 * time.Second, 4 * time.Second, 1}, "not answered": {time.Hour, 5 * time.Second, 0}} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				in, src, _ := newInstanceFixture(t, slowDoor(tc.after, AnswerDuplicate), 1, readyStatus, `{"status":"PAID","id":"1"}`)
				r := runInstance(in)
				waitFor(t, "a message taken", func() bool { _, err := os.Stat(filepath.Join(src.answer, LanePath(0, StepMessage))); return err == nil })
				time.Sleep(2 * lookEvery)
				e, took := r.stopped()
				if e != (Ending{}) || took < tc.took-time.Second || took > tc.took+time.Second {
					t.Fatalf("%+v after %v; want about %v", e, took, tc.took)
				}
				src.mu.Lock()
				defer src.mu.Unlock()
				if src.verdicts[ActionAck] != tc.acks {
					t.Fatalf("verdicts %v", src.verdicts)
				}
			})
		})
	}
}

// TestStop_AtMostOnceIsAskedForWhatItBuffered: what an at-most-once source has
// buffered is lost with its container, so the Orker goes on polling until every
// lane is quiet, or ten seconds.
func TestStop_AtMostOnceIsAskedForWhatItBuffered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, rec := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		in.Config.AtMostOnce, in.Judge.AtMostOnce = true, true
		src.longPolls.Store(true)
		src.queue = []string{`{"status":"PAID","id":"1"}`, `{"status":"PAID","id":"2"}`, `{"status":"PAID","id":"3"}`}
		// The poll it holds open is answered with nothing as it stops taking
		// messages; what it buffered is still to be asked for.
		src.quiet.Store(1)
		r := runInstance(in)
		waitFor(t, "the poll", func() bool { return in.Drivers[0].Lane.Seq() == 1 })
		go func() {
			for !stopNotice(src) {
				time.Sleep(time.Millisecond)
			}
			src.longPolls.Store(false)
		}()
		e, took := r.stopped()
		if e != (Ending{}) || took > 2*time.Second || len(rec.receipts) != 3 {
			t.Fatalf("%+v after %v, receipts %+v", e, took, rec.receipts)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, slowDoor(300*time.Millisecond, AnswerDuplicate), 1, readyStatus)
		in.Config.AtMostOnce, in.Judge.AtMostOnce = true, true
		for range 1000 {
			src.queue = append(src.queue, `{"status":"PAID","id":"1"}`)
		}
		r := runInstance(in)
		waitFor(t, "polls", func() bool { return in.Drivers[0].Lane.Seq() > 3 })
		if e, took := r.stopped(); e != (Ending{}) || took < 9*time.Second || took > 11*time.Second {
			t.Fatalf("%+v after %v; want ten seconds", e, took)
		}
	})
}

// TestStop_ARequiredReceiptAtAStop: a drop's receipt the store refuses is tried
// within the stop, and does not hold it.
func TestStop_ARequiredReceiptAtAStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{offers: []func(Offer) (Answer, error){unreachable[Offer]()}}
		in, src, rec := newInstanceFixture(t, door, 1, readyStatus, `{"status":"PAID","id":"1"}`)
		in.Config.AtMostOnce, in.Judge.AtMostOnce, in.Config.Park = true, true, 5*time.Minute
		r := runInstance(in)
		waitFor(t, "a message held", func() bool {
			in.mu.Lock()
			defer in.mu.Unlock()
			return len(in.held) == 1
		})
		rec.mu.Lock()
		rec.failTimes = 1
		rec.mu.Unlock()
		if e, took := r.stopped(); e != (Ending{}) || took < stopReceiptAt || took > 2*stopReceiptAt || len(rec.receipts) != 1 || src.verdicts[ActionRelease] != 1 {
			t.Fatalf("%+v after %v, receipts %+v, verdicts %v", e, took, rec.receipts, src.verdicts)
		}
	})
}

// TestStop_AnExitEndsItAtOnce: a container that is gone is told nothing and
// waited for by nobody; one that broke the contract is told nothing either.
func TestStop_AnExitEndsItAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, slowDoor(time.Hour, AnswerDuplicate), 1, readyStatus, `{"status":"PAID","id":"1"}`)
		r := runInstance(in)
		waitFor(t, "a message taken", func() bool { _, err := os.Stat(filepath.Join(src.answer, LanePath(0, StepMessage))); return err == nil })
		time.Sleep(2 * lookEvery)
		began := time.Now()
		in.Exited(Ending{Reason: eventsources.ReasonServiceFailed, Retried: true, Detail: "exited 3"})
		if e := <-r.done; e.Reason != eventsources.ReasonServiceFailed || time.Since(began) > time.Second || stopNotice(src) {
			t.Fatalf("%+v after %v, notice %v", e, time.Since(began), stopNotice(src))
		}
	})
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		r := runInstance(in)
		waitFor(t, "polls", func() bool { return in.Drivers[0].Lane.Seq() > 3 })
		src.rename(LanePath(0, StepMessage), []byte(`{"schemaVersion":1,"seq":99999,"id":"m","bodyBytes":1}`))
		if e := <-r.done; e.Reason != eventsources.ReasonServiceContractViolation || stopNotice(src) {
			t.Fatalf("%+v, notice %v", e, stopNotice(src))
		}
	})
}

// TestStop_AViolationDuringItIsSaid: what breaks the contract while the instance
// stops ends it as that, never as a clean stop.
func TestStop_AViolationDuringItIsSaid(t *testing.T) {
	for name, atMostOnce := range map[string]bool{"at least once": false, "at most once": true} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				in, src, _ := newInstanceFixture(t, slowDoor(2*time.Second, AnswerDuplicate), 1, readyStatus, `{"status":"PAID","id":"1"}`)
				in.Config.AtMostOnce, in.Judge.AtMostOnce = atMostOnce, atMostOnce
				r := runInstance(in)
				waitFor(t, "a message taken", func() bool { _, err := os.Stat(filepath.Join(src.answer, LanePath(0, StepMessage))); return err == nil })
				time.Sleep(2 * lookEvery)
				src.longPolls.Store(true)
				go func() {
					for !stopNotice(src) {
						time.Sleep(time.Millisecond)
					}
					src.rename(LanePath(0, StepSettled), []byte(`{"schemaVersion":1,"seq":99999,"ok":true}`))
				}()
				if e, took := r.stopped(); e.Reason != eventsources.ReasonServiceContractViolation || took > 6*time.Second {
					t.Fatalf("%+v after %v", e, took)
				}
			})
		})
	}
}

// TestStop_TheBeatGoesOn: a container that is finishing its exchanges still hears
// its Orker's beat.
func TestStop_TheBeatGoesOn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, slowDoor(time.Hour, AnswerDuplicate), 1, readyStatus, `{"status":"PAID","id":"1"}`)
		r := runInstance(in)
		waitFor(t, "a message taken", func() bool { _, err := os.Stat(filepath.Join(src.answer, LanePath(0, StepMessage))); return err == nil })
		time.Sleep(2 * lookEvery)
		r.stop()
		time.Sleep(time.Second)
		first, err := os.ReadFile(filepath.Join(src.ask, "alive.json"))
		if err != nil || !stopNotice(src) {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if second, _ := os.ReadFile(filepath.Join(src.ask, "alive.json")); string(second) == string(first) {
			t.Fatalf("no beat in two seconds of a stop: %s", first)
		}
		<-r.done
	})
}

// TestStop_AnExitWithNoReason: a container that is gone is told nothing, whatever
// its exit says.
func TestStop_AnExitWithNoReason(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		r := runInstance(in)
		waitFor(t, "polls", func() bool { return in.Drivers[0].Lane.Seq() > 3 })
		in.Exited(Ending{Detail: "stopped; exited 0"})
		if e := <-r.done; e.Reason != "" || stopNotice(src) {
			t.Fatalf("%+v, notice %v", e, stopNotice(src))
		}
	})
}

// TestStop_AnExitBeforeItRuns: a container that is gone before its instance runs
// ends it at once, and what tells of it is never held up.
func TestStop_AnExitBeforeItRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		in.Exited(Ending{Reason: eventsources.ReasonServiceExited, Retried: true})
		in.Exited(Ending{Reason: eventsources.ReasonServiceFailed, Retried: true})
		r := runInstance(in)
		if e := <-r.done; e.Reason != eventsources.ReasonServiceExited || stopNotice(src) {
			t.Fatalf("%+v, notice %v", e, stopNotice(src))
		}
	})
}

// TestStop_WhatEndedItMeanwhile: a breaker that trips, or a container that goes,
// while the instance stops is what Run returns.
func TestStop_WhatEndedItMeanwhile(t *testing.T) {
	for name, tc := range map[string]struct {
		meanwhile func(*Instance)
		want      func(Ending) bool
	}{
		"a pause": {func(in *Instance) { in.finish(Ending{Paused: BreakerAllRejected}) }, func(e Ending) bool { return e.Paused == BreakerAllRejected }},
		"an exit": {func(in *Instance) { in.Exited(Ending{Detail: "exited 0"}) }, func(e Ending) bool { return e.exited && e.Detail == "exited 0" }},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				in, src, _ := newInstanceFixture(t, slowDoor(time.Hour, AnswerDuplicate), 1, readyStatus, `{"status":"PAID","id":"1"}`)
				r := runInstance(in)
				waitFor(t, "a message taken", func() bool { _, err := os.Stat(filepath.Join(src.answer, LanePath(0, StepMessage))); return err == nil })
				time.Sleep(2 * lookEvery)
				go func() {
					for !stopNotice(src) {
						time.Sleep(time.Millisecond)
					}
					tc.meanwhile(in)
				}()
				if e, _ := r.stopped(); !tc.want(e) {
					t.Fatalf("%+v", e)
				}
			})
		})
	}
}

// TestInstance_HeldBackPollsNothing: an instance whose Orker holds it back (its
// object storage is out, its judgment waits) writes no poll, shows why, and
// carries on when it no longer does.
func TestInstance_HeldBackPollsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var hold atomic.Value
		hold.Store("")
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		in.Hold = func() string { return hold.Load().(string) }
		r := runInstance(in)
		waitFor(t, "polls", func() bool { return in.Drivers[0].Lane.Seq() > 3 })
		hold.Store("outage")
		time.Sleep(time.Second)
		held, beats := pollSeq(t, src), src.statuses.Load()
		time.Sleep(time.Minute)
		if pollSeq(t, src) != held || src.statuses.Load() == beats {
			t.Fatalf("polled while held back, or the source stopped (%d statuses)", src.statuses.Load()-beats)
		}
		hold.Store("")
		time.Sleep(time.Second)
		if pollSeq(t, src) == held {
			t.Fatal("did not carry on")
		}
		r.stopped()
	})
}
