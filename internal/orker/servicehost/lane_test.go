package servicehost

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

func TestLane_SeqOfAnEarlierExchangeIsIgnored(t *testing.T) {
	var l Lane
	seq, ok := l.Begin()
	if !ok || seq != 1 {
		t.Fatalf("Begin = %d, %v", seq, ok)
	}
	l.Await(t0, 30*time.Second)
	if got := l.Arrived(1); got != ArrivalCurrent {
		t.Fatalf("current step: %v", got)
	}
	l.Close()
	seq, _ = l.Begin()
	l.Await(t0, 30*time.Second)
	if seq != 2 || l.Arrived(1) != ArrivalLeftover {
		t.Fatal("a step of exchange 1 during exchange 2 is a leftover")
	}
	if l.Arrived(3) != ArrivalViolation {
		t.Fatal("a step ahead of the lane's seq is a violation")
	}
}

func TestLane_AnAnswerNobodyAskedForIsAViolation(t *testing.T) {
	var l Lane
	if l.Arrived(1) != ArrivalViolation {
		t.Fatal("before any exchange, seq 1 is unasked")
	}
	l.Begin()
	l.Await(t0, time.Second)
	l.Arrived(1)
	if l.Arrived(1) != ArrivalViolation {
		t.Fatal("a second answer to one step is a violation")
	}
}

func TestDeadline_CountedWhenItPasses(t *testing.T) {
	var l Lane
	l.Begin()
	l.Await(t0, 30*time.Second)
	l.Tick(t0.Add(29 * time.Second))
	if w, _ := l.Wedged(); w || l.LateInARow() != 0 {
		t.Fatal("wedged before the deadline")
	}
	l.Tick(t0.Add(31 * time.Second))
	w, since := l.Wedged()
	if !w || !since.Equal(t0.Add(30*time.Second)) || l.LateInARow() != 1 {
		t.Fatalf("after the deadline: wedged %v since %v, late %d", w, since, l.LateInARow())
	}
	l.Tick(t0.Add(90 * time.Second))
	if l.LateInARow() != 1 {
		t.Fatal("one missed deadline was counted twice")
	}
}

func TestLane_WedgedNeverMovesOn(t *testing.T) {
	var l Lane
	l.Begin()
	l.Await(t0, time.Second)
	l.Tick(t0.Add(2 * time.Second))
	if _, ok := l.Begin(); ok || l.CanBegin() {
		t.Fatal("a wedged lane began another exchange")
	}
	if l.Arrived(1) != ArrivalCurrent {
		t.Fatal("the late step was not taken")
	}
	if w, _ := l.Wedged(); w {
		t.Fatal("the lane stayed wedged after its step arrived")
	}
	l.Close()
	if seq, ok := l.Begin(); !ok || seq != 2 {
		t.Fatal("the lane did not go on once whole")
	}
}

func TestLane_ThreeLateInARowEndTheAttempt(t *testing.T) {
	var l Lane
	now := t0
	late := func() bool {
		l.Begin()
		l.Await(now, time.Second)
		ends := l.Tick(now.Add(2 * time.Second))
		l.Arrived(l.Seq())
		l.Close()
		now = now.Add(time.Minute)
		return ends
	}
	onTime := func() {
		l.Begin()
		l.Await(now, time.Second)
		l.Arrived(l.Seq())
		l.Close()
	}
	for i := 1; i <= 2; i++ {
		if late() {
			t.Fatalf("%d late exchanges ended the attempt", i)
		}
	}
	onTime()
	if l.LateInARow() != 0 {
		t.Fatal("an exchange on time did not break the run")
	}
	late()
	late()
	if !late() {
		t.Fatal("three late exchanges in a row did not end the attempt")
	}
}

func TestLane_CountsArePerLane(t *testing.T) {
	a, b := &Lane{}, &Lane{}
	for i := 0; i < 2; i++ {
		a.Begin()
		a.Await(t0, time.Second)
		a.Tick(t0.Add(2 * time.Second))
		a.Arrived(a.Seq())
		a.Close()
	}
	if b.LateInARow() != 0 || a.LateInARow() != 2 {
		t.Fatal("one lane's misses were counted on another")
	}
}

func TestLane_WedgedQuarterAndLongest(t *testing.T) {
	wedge := func(l *Lane, deadline time.Duration) {
		l.Begin()
		l.Await(t0, deadline)
		l.Tick(t0.Add(deadline + time.Millisecond))
	}
	lanes := []*Lane{{}, {}, {}, {}}
	wedge(lanes[0], 30*time.Second)
	at := t0.Add(30*time.Second + 61*time.Second)
	if Unresponsive(lanes, at, 16*time.Minute) {
		t.Fatal("one wedged lane of four is not more than a quarter")
	}
	wedge(lanes[1], 30*time.Second)
	if !Unresponsive(lanes, at, 16*time.Minute) {
		t.Fatal("two of four wedged past twice their deadline")
	}
	if Unresponsive(lanes, t0.Add(30*time.Second+30*time.Second), 16*time.Minute) {
		t.Fatal("wedged, but not yet for twice the deadline")
	}
	one := []*Lane{{}, {}, {}, {}}
	wedge(one[0], time.Minute)
	if !Unresponsive(one, t0.Add(time.Minute+17*time.Minute), 16*time.Minute) {
		t.Fatal("a lane wedged past the longest hold")
	}
	single := []*Lane{{}}
	wedge(single[0], time.Second)
	if !Unresponsive(single, t0.Add(4*time.Second), time.Hour) {
		t.Fatal("the only lane wedged past twice its deadline")
	}
}

func TestReadHeader(t *testing.T) {
	for _, tc := range []struct {
		raw, want string
		seq       int64
	}{
		{raw: `{"schemaVersion":1,"seq":42,"later":"ignored"}`, seq: 42},
		{raw: `{"schemaVersion":2,"seq":1}`, want: "schemaVersion 2"},
		{raw: `{"seq":1}`, want: "no schemaVersion"},
		{raw: `{"schemaVersion":1}`, want: "no seq"},
		{raw: `{"schemaVersion":1,"seq":0}`, want: "no seq"},
		{raw: `{"schemaVersion":1,"seq":1,"seq":2}`, want: "not one JSON object"},
		{raw: `[1]`, want: "not one JSON object"},
		{raw: "{\"schemaVersion\":1,\"seq\":1,\"x\":\"\xff\"}", want: "not one JSON object"},
		{raw: `{"schemaVersion":1,"seq":1} {}`, want: "not one JSON object"},
		{raw: `{"schemaVersion":"1","seq":1}`, want: "not one JSON object"},
	} {
		h, err := ReadHeader("lanes/0/message.json", []byte(tc.raw))
		var v *ViolationError
		if tc.want == "" {
			if err != nil || h.Seq != tc.seq {
				t.Errorf("%s: %+v, %v", tc.raw, h, err)
			}
			continue
		}
		if !errors.As(err, &v) || !strings.Contains(v.Reason, tc.want) {
			t.Errorf("%s: %v; want a violation containing %q", tc.raw, err, tc.want)
		}
	}
}

func FuzzServiceStep(f *testing.F) {
	for _, seed := range []string{`{"schemaVersion":1,"seq":1}`, `{"schemaVersion":2}`, `{"seq":-1}`, `null`, `{"schemaVersion":1,"seq":9223372036854775807}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		h, err := ReadHeader("step", raw)
		if err != nil {
			var v *ViolationError
			if !errors.As(err, &v) {
				t.Fatalf("a bad step is not a violation: %v", err)
			}
			return
		}
		if h.SchemaVersion != SchemaVersion || h.Seq < 1 {
			t.Fatalf("a step that reads has %+v", h)
		}
	})
}
