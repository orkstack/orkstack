package sourcehost

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestLane_WaitsForItsShareOfTheBodies: a lane reads no body before it has a share
// for it, gives it back once the message is judged, and where a try passes first
// the message is held and read when it is judged again.
func TestLane_WaitsForItsShareOfTheBodies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, rec := newInstanceFixture(t, acceptingDoor(), 1, readyStatus, `{"status":"PAID","id":"1"}`)
		in.Config.Park = 20 * time.Second
		share := NewParseShare(1 << 20)
		in.Drivers[0].Parse, in.Drivers[0].Limits.Try = share, 20*time.Second
		others, err := share.Acquire(t.Context(), "another's", 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan Ending, 1)
		go func() { done <- in.Run(ctx) }()
		time.Sleep(10 * time.Second)
		if s := in.Snapshot(); s.Lanes.Busy != 1 || src.settled.Load() != 0 {
			t.Errorf("%+v, %d settled", s, src.settled.Load())
		}
		others()
		time.Sleep(time.Second)
		if src.settled.Load() != 1 {
			t.Error("not judged though its share is free")
		}
		again, err := share.Acquire(t.Context(), "another's", 1<<20)
		if err != nil {
			t.Fatal("a share kept after its message was judged")
		}
		src.mu.Lock()
		src.queue = append(src.queue, `{"status":"PAID","id":"2"}`)
		src.mu.Unlock()
		// The status is fresh and the lane's own: what it waits for is its share.
		time.Sleep(25 * time.Second)
		if s := in.Snapshot(); s.State != StateWaiting || s.Reason != "parse_share" || s.Lanes.Parked != 1 {
			t.Errorf("past a try: %+v", s)
		}
		again()
		time.Sleep(20 * time.Second)
		cancel()
		if e := <-done; e != (Ending{}) {
			t.Errorf("%+v", e)
		}
		accepted := 0
		for _, r := range rec.receipts {
			if r.Outcome == artifacts.ReceiptAccepted {
				accepted++
			}
		}
		if accepted != 2 {
			t.Errorf("%d accepted of 2: %+v", accepted, rec.receipts)
		}
	})
}

// unansweredDoor is a door whose offer is answered by nobody until its call ends.
type unansweredDoor struct{ fakeDoor }

func (d *unansweredDoor) Offer(ctx context.Context, o Offer) (Answer, error) {
	d.mu.Lock()
	first := len(d.asked) == 0
	d.mu.Unlock()
	if first {
		_, _ = d.fakeDoor.Offer(ctx, o)
		<-ctx.Done()
		return Answer{}, ctx.Err()
	}
	return d.fakeDoor.Offer(ctx, o)
}

// TestLane_ATryIsBounded: a message whose judgment outlasts one try waits, parked,
// and is judged again.
func TestLane_ATryIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, &unansweredDoor{fakeDoor: fakeDoor{offers: acceptingDoor().offers, starts: acceptingDoor().starts}}, 1, readyStatus, `{"status":"PAID","id":"1"}`)
		in.Config.Park, in.Drivers[0].Limits.Try = 20*time.Second, 20*time.Second
		r := runInstance(in)
		time.Sleep(15 * time.Second)
		if s := in.Snapshot(); s.Lanes.Busy != 1 {
			t.Errorf("%+v", s)
		}
		time.Sleep(6 * time.Second)
		if s := in.Snapshot(); s.State != StateWaiting || s.Reason != "timeout" || s.Lanes.Parked != 1 {
			t.Errorf("past a try: %+v", s)
		}
		time.Sleep(5 * time.Second)
		if src.settled.Load() != 1 {
			t.Error("not judged again")
		}
		r.stopped()
	})
}

// TestLane_JudgesTheCopyItTook: a message judged again is the one that was read, and
// what its container wrote over it since is not read.
func TestLane_JudgesTheCopyItTook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const taken = `{"status":"PAID","id":"1"}`
		door := &unansweredDoor{fakeDoor: fakeDoor{offers: acceptingDoor().offers, starts: acceptingDoor().starts}}
		in, src, _ := newInstanceFixture(t, door, 1, readyStatus, taken)
		in.Config.Park, in.Drivers[0].Limits.Try = 20*time.Second, 20*time.Second
		r := runInstance(in)
		time.Sleep(15 * time.Second)
		src.rename(LanePath(0, StepBody), []byte(`{"status":"PAID","id":"2"}`))
		time.Sleep(10 * time.Second)
		door.mu.Lock()
		if len(door.asked) < 2 || door.lastOffr.BodyDigest != bodyDigest([]byte(taken)) {
			t.Errorf("asked %v, of %s", door.asked, door.lastOffr.BodyDigest)
		}
		door.mu.Unlock()
		if src.settled.Load() != 1 {
			t.Error("not judged again")
		}
		r.stopped()
	})
}
