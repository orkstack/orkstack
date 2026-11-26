package sourcehost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orkstack/orkstack/pkg/eventsources"
)

const fullStatus = `{"schemaVersion":1,"ready":true,"check":{"ok":true,"warnings":["DELIVERY_LIMIT_LOW"]},"broker":{"object":"jetstream:ORDERS/payments",
"leaseSeconds":60,"maxDeliveries":10,"messageLimitBytes":1048576,"retentionSeconds":604800,"pending":12,"unacknowledged":1,"redelivered":2,"deadLetter":{"name":"","depth":-1},"dropped":3}}`

// TestSnapshot_WhatAnInstanceShows: an instance that consumes is active, with
// what its image reported of its broker object and when it last accepted and was
// last answered.
func TestSnapshot_WhatAnInstanceShows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 2, fullStatus, `{"status":"PAID","id":"1"}`)
		if s := in.Snapshot(); s.State != StateStarting || s.Reason != "" || !s.ReadySince.IsZero() {
			t.Fatalf("before it runs: %+v", s)
		}
		began := time.Now()
		r := runInstance(in)
		waitFor(t, "an accepted message", func() bool { return src.settled.Load() == 1 })
		time.Sleep(time.Second)
		s := in.Snapshot()
		if s.State != StateActive || s.Reason != "" || s.Since.Before(began) || s.ReadySince.Before(began) || s.ReadySince.After(began.Add(time.Second)) {
			t.Fatalf("%+v", s)
		}
		if s.LastAcceptedAt.IsZero() || !s.LastPollOkAt.After(s.LastAcceptedAt) || s.Lanes.Parked != 0 || s.Lanes.Wedged != 0 || len(s.Warnings) != 1 || s.Warnings[0] != "DELIVERY_LIMIT_LOW" {
			t.Fatalf("%+v", s)
		}
		b := s.Broker
		if b == nil || b.Object != "jetstream:ORDERS/payments" || b.MaxDeliveries != 10 || b.LeaseSeconds != 60 || b.RetentionSeconds != 604800 || b.Pending != 12 ||
			b.Unacknowledged != 1 || b.Redelivered != 2 || b.DeadLetterDepth != -1 || b.CheckedAt.IsZero() {
			t.Fatalf("broker %+v", b)
		}
		if s.Provider != "jetstream" || s.Dropped != 3 {
			t.Fatalf("provider %q, dropped %d", s.Provider, s.Dropped)
		}
		r.stopped()
		if s := in.Snapshot(); s.State != StateStopping {
			t.Fatalf("stopped: %+v", s)
		}
	})
}

// TestSnapshot_States: what an instance that does not consume shows, and why.
func TestSnapshot_States(t *testing.T) {
	for name, tc := range map[string]struct {
		status string
		hold   string
		door   Door
		queue  []string
		state  string
		reason string
	}{
		"no status":     {"", "", acceptingDoor(), nil, StateStarting, ""},
		"not ready":     {`{"schemaVersion":1,"ready":false,"check":{"ok":true},"broker":{"maxDeliveries":5,"messageLimitBytes":1}}`, "", acceptingDoor(), nil, StateStarting, ""},
		"invalid":       {`{"schemaVersion":1,"ready":false,"check":{"ok":false,"reason":"OBJECT_MISSING"}}`, "", acceptingDoor(), nil, StateInvalid, "OBJECT_MISSING"},
		"unreachable":   {`{"schemaVersion":1,"ready":false,"check":{"ok":false,"reason":"UNREACHABLE"}}`, "", acceptingDoor(), nil, StateUnreachable, "UNREACHABLE"},
		"held back":     {readyStatus, "outage", acceptingDoor(), nil, StateWaiting, "outage"},
		"a wait":        {readyStatus, "", &fakeDoor{offers: []func(Offer) (Answer, error){func(Offer) (Answer, error) { return Answer{Kind: AnswerWait, Cause: "pool"}, nil }}}, []string{`{"status":"PAID","id":"1"}`}, StateWaiting, "pool"},
		"a later block": {`{"schemaVersion":1,"ready":true,"check":{"ok":true},"broker":{"maxDeliveries":5,"messageLimitBytes":1,"object":7}}`, "", acceptingDoor(), nil, StateActive, ""},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				in, _, _ := newInstanceFixture(t, tc.door, 1, tc.status, tc.queue...)
				in.Config.Limits.StartupSeconds, in.Config.Park = 60, 5*time.Minute
				in.Hold = func() string { return tc.hold }
				r := runInstance(in)
				time.Sleep(3 * time.Second)
				if s := in.Snapshot(); s.State != tc.state || s.Reason != tc.reason {
					t.Errorf("%+v; want %s %q", s, tc.state, tc.reason)
				}
				r.stopped()
			})
		})
	}
}

// TestSnapshot_ReadyInARow: how long it has been ready is counted again after a
// time it was not, and since when it shows what it shows is kept while it does.
func TestSnapshot_ReadyInARow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		r := runInstance(in)
		waitFor(t, "ready", func() bool { return in.Snapshot().State == StateActive })
		first := in.Snapshot()
		time.Sleep(time.Minute)
		if s := in.Snapshot(); s.ReadySince != first.ReadySince || s.Since != first.Since {
			t.Fatalf("%+v, then %+v", first, s)
		}
		src.mu.Lock()
		src.status = `{"schemaVersion":1,"ready":false,"check":{"ok":false,"reason":"UNREACHABLE"}}`
		src.mu.Unlock()
		waitFor(t, "unreachable", func() bool { return in.Snapshot().State == StateUnreachable })
		if s := in.Snapshot(); !s.ReadySince.IsZero() || !s.Since.After(first.Since) {
			t.Fatalf("%+v", s)
		}
		src.mu.Lock()
		src.status = readyStatus
		src.mu.Unlock()
		waitFor(t, "ready again", func() bool { return in.Snapshot().State == StateActive })
		if s := in.Snapshot(); !s.ReadySince.After(first.ReadySince.Add(time.Minute)) {
			t.Fatalf("%+v", s)
		}
		r.stopped()
	})
}

// TestSnapshot_SinceIsWhenItBegan: since when an instance shows what it shows is
// when it began to, whenever it is asked.
func TestSnapshot_SinceIsWhenItBegan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		src.longPolls.Store(true)
		began := time.Now()
		r := runInstance(in)
		time.Sleep(10 * time.Second)
		if s := in.Snapshot(); s.State != StateActive || s.Since.After(began.Add(time.Second)) {
			t.Fatalf("asked a minute later: %+v", s)
		}
		r.stopped()
	})
}

// TestSnapshot_SinceItsReason: one that shows the same state for another reason
// shows it since then.
func TestSnapshot_SinceItsReason(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var hold atomic.Value
		hold.Store("outage")
		in, _, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		in.Hold = func() string { return hold.Load().(string) }
		r := runInstance(in)
		time.Sleep(3 * time.Second)
		first := in.Snapshot()
		hold.Store("judgment")
		time.Sleep(time.Second)
		if s := in.Snapshot(); first.Reason != "outage" || s.State != first.State || s.Reason != "judgment" || !s.Since.After(first.Since) {
			t.Fatalf("%+v, then %+v", first, s)
		}
		r.stopped()
	})
}

// TestSnapshot_BesideItsStart: an instance may be asked while it starts.
func TestSnapshot_BesideItsStart(t *testing.T) {
	for range 20 {
		synctest.Test(t, func(t *testing.T) {
			in, _, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for range 50 {
					in.Snapshot()
				}
			}()
			r := runInstance(in)
			<-done
			r.stopped()
		})
	}
}

// TestSnapshot_Lanes: a lane with a message in hand is busy, one whose message
// waits is parked, and the oldest step a container owes is aged.
func TestSnapshot_Lanes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
		in.Config.Limits.AnswerSeconds = 10
		in.Drivers[0].Limits.Answer = 10 * time.Second
		src.longPolls.Store(true)
		r := runInstance(in)
		waitFor(t, "the poll", func() bool { return in.Drivers[0].Lane.Seq() == 1 })
		time.Sleep(5 * time.Second)
		if l := in.Snapshot().Lanes; l.OldestStepSeconds < 4 || l.OldestStepSeconds > 6 || l.Wedged != 0 || l.Busy != 1 {
			t.Fatalf("%+v", l)
		}
		time.Sleep(7 * time.Second)
		if l := in.Snapshot().Lanes; l.Wedged != 1 || l.OldestStepSeconds < 11 {
			t.Fatalf("%+v", l)
		}
		r.stopped()
	})
	synctest.Test(t, func(t *testing.T) {
		in, _, _ := newInstanceFixture(t, &fakeDoor{offers: []func(Offer) (Answer, error){unreachable[Offer]()}}, 1, readyStatus, `{"status":"PAID","id":"1"}`)
		in.Config.Park = 5 * time.Minute
		r := runInstance(in)
		waitFor(t, "a message held", func() bool { return in.Snapshot().Lanes.Parked == 1 })
		if l := in.Snapshot().Lanes; l.Busy != 0 {
			t.Errorf("a message that waits is busy too: %+v", l)
		}
		r.stopped()
	})
}

// TestSnapshot_ReportedAndChecked: that a container wrote a status, whatever it
// says, and when the loop last looked at every lane.
func TestSnapshot_ReportedAndChecked(t *testing.T) {
	for status, want := range map[string]bool{"": false, `{"schemaVersion":1,"ready":false,"check":{"ok":true},"broker":{"maxDeliveries":5,"messageLimitBytes":1}}`: true} {
		synctest.Test(t, func(t *testing.T) {
			in, _, _ := newInstanceFixture(t, acceptingDoor(), 1, status)
			in.Config.Limits.StartupSeconds = 60
			r := runInstance(in)
			time.Sleep(3 * time.Second)
			if s := in.Snapshot(); s.Reported != want || time.Since(s.Checked) > time.Second {
				t.Errorf("%q: %+v", status, s)
			}
			r.stopped()
		})
	}
}

// deafDoor is a door whose call never returns, whatever its context says, as a
// call into a driver that hangs.
type deafDoor struct {
	fakeDoor
	done chan struct{}
}

func (d *deafDoor) Offer(context.Context, Offer) (Answer, error) {
	<-d.done
	return Answer{}, errors.New("hung up")
}

// TestInstance_ALaneOfTheOrkersOwnThatStalls: a lane that made no step past the
// longest hold ends the attempt as its host's failure, however healthy the rest
// is, and the ending does not wait for it.
func TestInstance_ALaneOfTheOrkersOwnThatStalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &deafDoor{done: make(chan struct{})}
		in, _, _ := newInstanceFixture(t, door, 2, readyStatus, `{"status":"PAID","id":"1"}`)
		in.Config.Limits.HoldSeconds = 20
		began := time.Now()
		ended := make(chan Ending, 1)
		go func() { ended <- in.Run(context.Background()) }()
		time.Sleep(19 * time.Second)
		select {
		case e := <-ended:
			t.Fatalf("within its hold: %+v", e)
		default:
		}
		e := <-ended
		if took := time.Since(began); e.Reason != eventsources.ReasonServiceHostStalled || took > 32*time.Second {
			t.Fatalf("%+v after %s", e, took)
		}
		close(door.done)
	})
}

// TestInstance_AStopDoesNotWaitForAStalledLane: a stop ends within its bounds.
func TestInstance_AStopDoesNotWaitForAStalledLane(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &deafDoor{done: make(chan struct{})}
		in, src, _ := newInstanceFixture(t, door, 1, readyStatus, `{"status":"PAID","id":"1"}`)
		r := runInstance(in)
		time.Sleep(3 * time.Second)
		began := time.Now()
		r.stopped()
		if took := time.Since(began); took > 30*time.Second {
			t.Fatalf("it took %s", took)
		}
		select {
		case <-in.Finished():
			t.Error("finished while a lane uses its files")
		default:
		}
		if _, err := os.Stat(filepath.Join(src.ask, "stop.json")); err != nil {
			t.Error(err)
		}
		close(door.done)
		<-in.Finished()
	})
}

// TestInstance_HeldBackJudgesNothing: a message handed over while its instance is
// held back, as one a poll before it was still open for, is not offered; it waits,
// and is judged when nothing holds the instance back.
func TestInstance_HeldBackJudgesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := acceptingDoor()
		in, src, _ := newInstanceFixture(t, door, 1, readyStatus)
		in.Config.Limits.PollWaitSeconds, in.Config.Park = 20, time.Minute
		in.Drivers[0].Limits.PollWait, in.Drivers[0].Limits.Answer = 20*time.Second, 30*time.Second
		src.longPolls.Store(true)
		var hold atomic.Bool
		in.Hold = func() string {
			if hold.Load() {
				return "outage"
			}
			return ""
		}
		r := runInstance(in)
		time.Sleep(3 * time.Second)
		hold.Store(true)
		src.mu.Lock()
		src.queue = append(src.queue, `{"status":"PAID","id":"1"}`)
		src.mu.Unlock()
		src.longPolls.Store(false)
		time.Sleep(5 * time.Second)
		if s := in.Snapshot(); s.State != StateWaiting || s.Reason != "outage" || s.Lanes.Parked != 1 || src.settled.Load() != 0 {
			t.Errorf("%+v, %d settled", s, src.settled.Load())
		}
		door.mu.Lock()
		if len(door.asked) != 0 {
			t.Errorf("asked %v", door.asked)
		}
		door.mu.Unlock()
		hold.Store(false)
		time.Sleep(50 * time.Second)
		src.mu.Lock()
		if src.verdicts[ActionAck] != 1 || src.verdicts[ActionRelease] != 0 {
			t.Errorf("verdicts %v", src.verdicts)
		}
		src.mu.Unlock()
		r.stopped()
	})
}
