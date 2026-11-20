package sourcehost

import (
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestWait_ParkReleaseProbeTryOne(t *testing.T) {
	w := NewWaiter(5*time.Minute, 4)
	w.Begin(t0)
	if !w.Waiting() || w.Lanes() != 0 {
		t.Fatal("a wait stops polling on every lane")
	}
	now := t0
	retries := 0
	for now.Sub(t0) < 5*time.Minute {
		if w.Due(now) == StepRetryOldest {
			retries++
			w.Retried(false, now)
		}
		now = now.Add(500 * time.Millisecond)
	}
	if retries < 5 || retries > 20 {
		t.Fatalf("%d retries in a five-minute park on 1 s doubling to 30 s", retries)
	}
	if w.Due(now) != StepReleaseAll {
		t.Fatal("the park ran out and nothing was released")
	}
	w.Released(now)
	if w.Lanes() != 0 || w.Due(now) != StepProbe {
		t.Fatal("after the release the instance probes and polls nothing")
	}
	if w.Probed(false, now) != StepNone || w.Due(now) != StepNone || w.Due(now.Add(2*time.Second)) != StepProbe {
		t.Fatal("a failed probe round backs off")
	}
	if w.Probed(true, now) != StepTryOne || w.Lanes() != 1 {
		t.Fatal("a passing round lets one lane try one poll")
	}
	accepted := &Decision{Disposition: Settle, Receipt: Receipt{Outcome: artifacts.ReceiptAccepted}}
	if _, again := w.Tried(accepted, now); again || w.Waiting() || w.Lanes() != 2 {
		t.Fatalf("an accept ends the wait and adds a lane: waiting %v lanes %d", w.Waiting(), w.Lanes())
	}
	w.Accepted()
	w.Accepted()
	w.Accepted()
	if w.Lanes() != 4 {
		t.Fatalf("lanes %d: added one at a time up to concurrency", w.Lanes())
	}
}

func TestWait_ARetryThatPassesEndsTheParkAtOnce(t *testing.T) {
	w := NewWaiter(5*time.Minute, 2)
	w.Begin(t0)
	w.Retried(true, t0.Add(time.Second))
	if w.Waiting() || w.Lanes() != 2 {
		t.Fatal("a short wait spends nothing")
	}
}

func TestWait_RecurrenceCostsOneMessageOneCount(t *testing.T) {
	w := NewWaiter(time.Minute, 1)
	w.Begin(t0)
	w.Released(t0.Add(time.Minute))
	delays := []time.Duration{}
	for i := 0; i < 8; i++ {
		w.Probed(true, t0)
		delay, again := w.Tried(&Decision{Disposition: Wait, Cause: "engine_unreachable"}, t0)
		if !again {
			t.Fatal("a wait after a passing probe is a recurrence")
		}
		delays = append(delays, delay)
	}
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute}
	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("cycle delays %v; want %v", delays, want)
		}
	}
	w.Probed(true, t0)
	w.Tried(&Decision{Disposition: Settle, Receipt: Receipt{Outcome: artifacts.ReceiptDuplicate}}, t0)
	if w.CycleDelay() != 30*time.Second {
		t.Fatal("the cycle delay goes back to 30 s on a settled message")
	}
	w2 := NewWaiter(time.Minute, 1)
	w2.Begin(t0)
	w2.Released(t0)
	w2.Probed(true, t0)
	if _, again := w2.Tried(nil, t0); again || !w2.Waiting() || w2.Lanes() != 1 {
		t.Fatal("a quiet poll changes nothing: the one lane keeps polling")
	}
}

func TestBreaker_AllRejectedCountsDistinctMessages(t *testing.T) {
	b := NewBreakers(3, "")
	for _, id := range []string{"a", "a", "b"} {
		if b.Rejected(id) != "" {
			t.Fatal("tripped early; a message's second delivery counts once")
		}
	}
	if b.Rejected("c") != BreakerAllRejected {
		t.Fatal("three distinct rejected messages did not trip")
	}
	b = NewBreakers(3, "")
	b.Rejected("a")
	b.Rejected("b")
	b.Accepted()
	if b.Rejected("c") != "" {
		t.Fatal("an accepted message did not end the run")
	}
	if NewBreakers(0, BreakerAllRejected).Rejected("x") != "" {
		t.Fatal("reject_breaker 0 disables the breaker, trial or not")
	}
}

func TestBreaker_SettleFailing(t *testing.T) {
	b := NewBreakers(20, "")
	for i := 0; i < 4; i++ {
		if b.Settled(true) != "" {
			t.Fatal("tripped before five")
		}
	}
	b.Settled(false)
	for i := 0; i < 4; i++ {
		b.Settled(true)
	}
	if b.Settled(true) != BreakerSettleFailing {
		t.Fatal("five refused settlements in a row did not trip")
	}
}

func TestBreaker_UncertainNeedsTwoMinutes(t *testing.T) {
	b := NewBreakers(20, "")
	for i := 0; i < 5; i++ {
		if b.Uncertain(t0.Add(time.Duration(i)*10*time.Second)) != "" {
			t.Fatal("five rounds within a minute tripped")
		}
	}
	if b.Uncertain(t0.Add(3*time.Minute)) != BreakerRefused {
		t.Fatal("five rounds spanning two minutes did not trip")
	}
	b.Certain()
	if b.Uncertain(t0.Add(4*time.Minute)) != "" {
		t.Fatal("a certain answer did not end the run")
	}
}

func TestBreaker_SettleUnansweredSpansTenMinutes(t *testing.T) {
	b := NewBreakers(20, "")
	for i := 0; i < 4; i++ {
		b.SettleUnanswered(t0.Add(time.Duration(i) * time.Minute))
	}
	if b.SettleUnanswered(t0.Add(5*time.Minute)) != "" {
		t.Fatal("five within five minutes tripped")
	}
	if b.SettleUnanswered(t0.Add(11*time.Minute)) != BreakerSettleUnanswered {
		t.Fatal("five spanning ten minutes did not trip")
	}
}

func TestTrial_TripsAtTheFirstEventOfItsBreaker(t *testing.T) {
	if NewBreakers(20, BreakerAllRejected).Rejected("x") != BreakerAllRejected {
		t.Fatal("a trial for rejections")
	}
	if NewBreakers(20, BreakerSettleFailing).Settled(true) != BreakerSettleFailing {
		t.Fatal("a trial for refused settlements")
	}
	b := NewBreakers(20, BreakerAllRejected)
	if b.Settled(true) != "" {
		t.Fatal("a trial is for the breaker that paused the source only")
	}
	b.Accepted()
	if b.OnTrial() != "" || b.Rejected("x") != "" {
		t.Fatal("an accepted message ends the trial")
	}
}

func TestLimit_IsTheSmallerOfTheLastTwoReported(t *testing.T) {
	var d DeliveryLimit
	if n, _ := d.Limit(); n != 0 {
		t.Fatal("a limit before any status")
	}
	d.Observe(10, t0)
	if n, _ := d.Limit(); n != 10 {
		t.Fatal("one report")
	}
	d.Observe(5, t0.Add(time.Second))
	if n, at := d.Limit(); n != 5 || !at.Equal(t0.Add(time.Second)) {
		t.Fatal("the smaller is the newer")
	}
	d.Observe(20, t0.Add(2*time.Second))
	if n, _ := d.Limit(); n != 5 {
		t.Fatal("a raised limit counts only once it has been reported twice")
	}
	d.Observe(20, t0.Add(3*time.Second))
	if n, _ := d.Limit(); n != 20 {
		t.Fatal("reported twice")
	}
}

func TestLimit_OnlyTwoToAHundred(t *testing.T) {
	var d DeliveryLimit
	for _, n := range []int{0, 1, 101, -1, 1 << 30} {
		if d.Observe(n, t0) {
			t.Errorf("limit %d accepted", n)
		}
	}
	if !d.Observe(2, t0) || !d.Observe(100, t0) {
		t.Fatal("2 and 100 refused")
	}
}

// A wait while the instance is parked is one more message held, not a
// recurrence: a recurrence is a wait after the probes passed, when one lane
// tries one message (01 §5, step 4).
func TestWaiter_AWaitWhileParkedIsNoRecurrence(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	w := NewWaiter(5*time.Minute, 4)
	w.Begin(now)
	wait := &Decision{Disposition: Wait}
	if _, again := w.Tried(wait, now.Add(time.Second)); again {
		t.Fatal("a second lane's wait during the park was taken for a recurrence")
	}
	if !w.Waiting() || w.Due(now.Add(time.Second)) == StepProbe {
		t.Fatal("the park ended early")
	}
}

func TestReceiveBackoff(t *testing.T) {
	for n, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 5: 16 * time.Second, 6: 30 * time.Second, 40: 30 * time.Second} {
		if got := receiveBackoff(n); got != want {
			t.Errorf("receiveBackoff(%d) = %v; want %v", n, got, want)
		}
	}
}

// Five uncertain rounds ask for a pause only in a row: a certain decision
// between them starts the count again.
func TestBreakers_UncertainCountsInARow(t *testing.T) {
	var b Breakers
	at := time.Unix(1_000_000, 0)
	for i := range 4 {
		b.Uncertain(at.Add(time.Duration(i) * time.Minute))
	}
	b.Certain()
	if got := b.Uncertain(at.Add(10 * time.Minute)); got != "" {
		t.Fatalf("a certain decision did not reset the count: %q", got)
	}
}
