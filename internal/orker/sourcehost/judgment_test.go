package sourcehost

import (
	"fmt"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/eventsources"
)

var judgedFrom = time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)

func newJudgment() *Judgment {
	return &Judgment{Started: judgedFrom, WaitMax: 15 * time.Minute, UnavailableMax: time.Hour}
}

func at(minutes float64) time.Time {
	return judgedFrom.Add(time.Duration(minutes * float64(time.Minute)))
}

var (
	accepted    = Confirmed{Kind: ConfirmAccepted}
	errNoAnswer = fmt.Errorf("%w: dial", ErrNoAnswer)
)

func waits(reason string) Confirmed { return Confirmed{Kind: ConfirmWait, Reason: reason} }

func refusedAs(reason string) error { return &RefusedError{Reason: reason} }

// TestJudgment_BeforeTheFirst: no container starts before a confirmation that
// accepts, and an attempt waits for one no longer than its bound: for what the
// engine waited on, or the engine itself.
func TestJudgment_BeforeTheFirst(t *testing.T) {
	for name, tc := range map[string]struct {
		c     Confirmed
		err   error
		want  JudgmentEnd
		class string
	}{
		"a policy nobody read":    {c: waits("unread=policy"), want: JudgmentEnd{Reason: eventsources.ReasonIdentityUnavailable, Held: "unread=policy"}},
		"a spec nobody read":      {c: waits("unread=spec"), want: JudgmentEnd{Reason: eventsources.ReasonIdentityUnavailable, Held: "unread=spec", Unread: "spec"}},
		"a record nobody read":    {c: waits("unread=record"), want: JudgmentEnd{Reason: eventsources.ReasonIdentityUnavailable, Held: "unread=record", Unread: "record"}},
		"a revoke held":           {c: waits("held=revoke-pending"), want: JudgmentEnd{Reason: eventsources.ReasonIdentityUnavailable, Held: "held=revoke-pending"}},
		"a read the engine lost":  {c: waits(""), want: JudgmentEnd{Reason: eventsources.ReasonIdentityUnavailable}},
		"an engine not reached":   {err: errNoAnswer, want: JudgmentEnd{Reason: eventsources.ReasonEngineUnavailable}},
		"a word no judgment says": {c: waits("unread=" + "elsewhere\n"), want: JudgmentEnd{Reason: eventsources.ReasonIdentityUnavailable}},
	} {
		j := newJudgment()
		for m := 0.0; m < 15; m++ {
			j.Observe(at(m), tc.c, tc.err)
			if end, ok := j.End(at(m)); ok || j.Accepted() {
				t.Fatalf("%s: at %v min: %+v, accepted %v", name, m, end, j.Accepted())
			}
		}
		if end, ok := j.End(at(15)); !ok || end != tc.want {
			t.Errorf("%s: at its bound: %+v, %v; want %+v", name, end, ok, tc.want)
		}
	}
	if end, ok := newJudgment().End(at(15)); !ok || end.Reason != eventsources.ReasonEngineUnavailable {
		t.Errorf("nothing asked yet, at its bound: %+v, %v", end, ok)
	}
	j := newJudgment()
	j.Observe(at(14), accepted, nil)
	if end, ok := j.End(at(20)); ok || !j.Accepted() {
		t.Errorf("confirmed within its bound: %+v, %v", end, j.Accepted())
	}
}

// TestJudgment_Running: the clock is from the last confirmation that accepted,
// whatever came between: ten minutes on it the instance waits, and past its
// bound it ends; an engine that answers nothing at all ends it at fifteen.
func TestJudgment_Running(t *testing.T) {
	j := newJudgment()
	j.Observe(at(0), accepted, nil)
	for m := 5.0; m < 60; m += 5 {
		// Alternating, so that no rule of either alone ever applies.
		if int(m)%10 == 0 {
			j.Observe(at(m), waits("unread=access"), nil)
		} else {
			j.Observe(at(m), Confirmed{}, errNoAnswer)
		}
		if got := j.Waiting(at(m)); got != (m >= 10) {
			t.Errorf("at %v min: waiting %v", m, got)
		}
		if end, ok := j.End(at(m)); ok {
			t.Fatalf("at %v min: %+v", m, end)
		}
	}
	if end, ok := j.End(at(60)); !ok || end.Reason != eventsources.ReasonEngineUnavailable {
		t.Errorf("past its bound, the last call unanswered: %+v, %v", end, ok)
	}
	j.Observe(at(60), waits("unread=record"), nil)
	if end, ok := j.End(at(60)); !ok || end != (JudgmentEnd{Reason: eventsources.ReasonIdentityUnavailable, Held: "unread=record", Unread: "record"}) {
		t.Errorf("past its bound, the last call a wait: %+v, %v", end, ok)
	}
	j.Observe(at(61), accepted, nil)
	if end, ok := j.End(at(70)); ok || j.Waiting(at(70)) || !j.Waiting(at(71)) {
		t.Errorf("accepted again: %+v, %v", end, j.Waiting(at(70)))
	}

	j = newJudgment()
	j.Observe(at(0), accepted, nil)
	j.Observe(at(5), Confirmed{}, errNoAnswer)
	j.Observe(at(10), Confirmed{}, errNoAnswer)
	if _, ok := j.End(at(14.9)); ok {
		t.Error("ended within three confirmations of an engine that does not answer")
	}
	if end, ok := j.End(at(15)); !ok || end.Reason != eventsources.ReasonEngineUnavailable {
		t.Errorf("an engine that answered nothing for fifteen minutes: %+v, %v", end, ok)
	}
	j = &Judgment{Started: judgedFrom, WaitMax: 15 * time.Minute, UnavailableMax: 12 * time.Minute}
	j.Observe(at(0), accepted, nil)
	j.Observe(at(5), waits("unread=policy"), nil)
	if end, ok := j.End(at(12)); !ok || end.Reason != eventsources.ReasonIdentityUnavailable {
		t.Errorf("a bound shorter than the engine's: %+v, %v", end, ok)
	}
}

// TestJudgment_RefusalsThatComeAndGo: replicas that refuse for a guard and replicas
// that do not answer, by turns, end it at its bound and no later.
func TestJudgment_RefusalsThatComeAndGo(t *testing.T) {
	j := newJudgment()
	j.Observe(at(0), accepted, nil)
	for m := 5.0; m < 60; m += 5 {
		if err := map[bool]error{true: refusedAs("EVENT_SOURCES_DISABLED"), false: errNoAnswer}[int(m)%10 == 0]; true {
			j.Observe(at(m), Confirmed{}, err)
		}
		if end, ok := j.End(at(m)); ok {
			t.Fatalf("at %v min: %+v", m, end)
		}
	}
	if end, ok := j.End(at(60)); !ok || end.Reason != eventsources.ReasonEngineUnavailable {
		t.Errorf("at its bound: %+v, %v", end, ok)
	}
}

// TestJudgment_WaitingIsCountedFromTheStart: an attempt nothing has accepted waits
// from its own start.
func TestJudgment_WaitingIsCountedFromTheStart(t *testing.T) {
	j := newJudgment()
	if j.Waiting(at(9)) || !j.Waiting(at(10)) {
		t.Errorf("at nine minutes %v, at ten %v", j.Waiting(at(9)), j.Waiting(at(10)))
	}
}

// TestJudgment_Stopped: a stop ends the instance at once with its own reason,
// and one whose reason no table holds ends it too, naming none.
func TestJudgment_Stopped(t *testing.T) {
	for reason, want := range map[string]string{"EVENT_SOURCE_PAUSED": "EVENT_SOURCE_PAUSED", "EVENT_SOURCE_NOT_HELD": "EVENT_SOURCE_NOT_HELD",
		"EVENT_SOURCE_DELETED_DOCUMENT_REAPPEARED": "EVENT_SOURCE_DELETED_DOCUMENT_REAPPEARED", "A_LATER_RELEASES": "EVENT_SOURCE_NOT_HELD", "": "EVENT_SOURCE_NOT_HELD",
		"SERVICE_FAILED": "EVENT_SOURCE_NOT_HELD"} {
		j := newJudgment()
		j.Observe(at(0), accepted, nil)
		j.Observe(at(5), Confirmed{Kind: ConfirmStop, Reason: reason}, nil)
		if end, ok := j.End(at(5)); !ok || end != (JudgmentEnd{Reason: want, Stopped: true}) {
			t.Errorf("%q: %+v, %v", reason, end, ok)
		}
	}
}

// TestJudgment_Off: three confirmations in a row over ten minutes that every
// replica refuses for a guard end the instance; one between them that says
// anything else does not.
func TestJudgment_Off(t *testing.T) {
	const off, auth, unversioned = "EVENT_SOURCES_DISABLED", "EVENT_SOURCES_REQUIRE_AUTHENTICATION", "EVENT_SOURCES_REQUIRE_VERSIONING"
	type say struct {
		at  float64
		c   Confirmed
		err error
	}
	for name, tc := range map[string]struct {
		says []say
		want string
	}{
		"three over ten minutes":      {[]say{{5, Confirmed{}, refusedAs(off)}, {10, Confirmed{}, refusedAs(off)}, {15, Confirmed{}, refusedAs(off)}}, off},
		"each a guard of its own":     {[]say{{5, Confirmed{}, refusedAs(off)}, {10, Confirmed{}, refusedAs(auth)}, {15, Confirmed{}, refusedAs(unversioned)}}, off},
		"a little late each":          {[]say{{5.1, Confirmed{}, refusedAs(off)}, {10.2, Confirmed{}, refusedAs(off)}, {15.3, Confirmed{}, refusedAs(off)}}, off},
		"two":                         {[]say{{5, Confirmed{}, refusedAs(off)}, {15, Confirmed{}, refusedAs(off)}}, ""},
		"three too soon":              {[]say{{5, Confirmed{}, refusedAs(off)}, {9, Confirmed{}, refusedAs(off)}, {14.9, Confirmed{}, refusedAs(off)}}, ""},
		"one accepts between":         {[]say{{5, Confirmed{}, refusedAs(off)}, {10, accepted, nil}, {15, Confirmed{}, refusedAs(off)}, {20, Confirmed{}, refusedAs(off)}}, ""},
		"one waits between":           {[]say{{5, Confirmed{}, refusedAs(off)}, {7, waits(""), nil}, {10, Confirmed{}, refusedAs(off)}, {14, Confirmed{}, refusedAs(off)}}, ""},
		"one is not answered between": {[]say{{4, Confirmed{}, refusedAs(off)}, {6, Confirmed{}, errNoAnswer}, {10, Confirmed{}, refusedAs(off)}, {14, Confirmed{}, refusedAs(off)}}, ""},
		"another refusal":             {[]say{{4, Confirmed{}, refusedAs("EVENT_SOURCE_DOCUMENT_MISSING")}, {9, Confirmed{}, refusedAs("EVENT_SOURCE_DOCUMENT_MISSING")}, {14, Confirmed{}, refusedAs("EVENT_SOURCE_DOCUMENT_MISSING")}}, ""},
	} {
		j := newJudgment()
		j.Observe(at(0), accepted, nil)
		var last float64
		for _, s := range tc.says {
			// What ends it is the third of them, not the clock before it.
			if end, ok := j.End(at(s.at)); ok && !end.Stopped && tc.want != "" {
				t.Errorf("%s: before %v min: %+v", name, s.at, end)
			}
			j.Observe(at(s.at), s.c, s.err)
			last = s.at
		}
		end, ok := j.End(at(last))
		if want := (JudgmentEnd{Reason: off, Stopped: true}); tc.want != "" && (!ok || end != want) {
			t.Errorf("%s: %+v, %v", name, end, ok)
		}
		if tc.want == "" && ok && end.Stopped {
			t.Errorf("%s: %+v", name, end)
		}
	}
}

// TestJudgment_NotPermitted: an Orker that lacks a permission ends its attempt
// with that and no other reason, whenever it is said.
func TestJudgment_NotPermitted(t *testing.T) {
	denied := fmt.Errorf("%w: %w", ErrNoAnswer, ErrHostNotPermitted)
	for _, ran := range []bool{false, true} {
		j := newJudgment()
		if ran {
			j.Observe(at(0), accepted, nil)
		}
		j.Observe(at(5), Confirmed{}, denied)
		if end, ok := j.End(at(5)); !ok || end != (JudgmentEnd{Reason: eventsources.ReasonHostNotPermitted}) {
			t.Errorf("ran %v: %+v, %v", ran, end, ok)
		}
		j.Observe(at(6), accepted, nil)
		if end, ok := j.End(at(6)); ok {
			t.Errorf("permitted again: %+v", end)
		}
	}
}

// TestJudgment_Held: what the judgment waits on is shown since the first attempt
// of the activity that waited on it, until one accepts.
func TestJudgment_Held(t *testing.T) {
	j := newJudgment()
	if held, since := j.Held(); held != "" || !since.IsZero() {
		t.Errorf("at first: %q since %v", held, since)
	}
	j.Observe(at(1), waits("unread=policy"), nil)
	j.Observe(at(2), waits("unread=access"), nil)
	j.Observe(at(3), Confirmed{}, errNoAnswer)
	if held, since := j.Held(); held != "unread=access" || !since.Equal(at(1)) {
		t.Errorf("waiting: %q since %v", held, since)
	}
	j.Observe(at(4), accepted, nil)
	if held, since := j.Held(); held != "" || !since.IsZero() {
		t.Errorf("accepted: %q since %v", held, since)
	}
	j = newJudgment()
	j.HeldSince = at(-30)
	j.Observe(at(1), waits("held=revoke-pending"), nil)
	if _, since := j.Held(); !since.Equal(at(-30)) {
		t.Errorf("taken over from the attempt before: since %v", since)
	}
	j = newJudgment()
	j.HeldSince = at(30)
	j.Observe(at(1), waits("unread=spec"), nil)
	if _, since := j.Held(); !since.Equal(at(1)) {
		t.Errorf("a wait that began in the future: since %v", since)
	}
}
