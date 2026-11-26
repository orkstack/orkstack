package sourcehost

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// TestLogWindow: the first megabyte of an attempt is kept, then each stream has
// its own hour's budget; what is over is dropped, said when it starts and
// counted when its hour ends or the attempt does. The Orker's own lines are kept.
func TestLogWindow(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	var got []string
	w := &LogWindow{PerHour: 100, Setting: "ORK_X", Write: func(l string) { got = append(got, l) }, Now: func() time.Time { return now }}
	take := func() []string { out := got; got = nil; return out }
	expect := func(what string, want ...string) {
		t.Helper()
		if lines := take(); !slices.Equal(lines, want) {
			t.Errorf("%s: %q, want %q", what, lines, want)
		}
	}
	head := strings.Repeat("h", 1023)
	for range 1024 {
		w.Line(head)
	}
	if n := len(take()); n != 1024 {
		t.Fatalf("%d of the first megabyte's 1024 lines kept", n)
	}
	out, errs := strings.Repeat("o", 49), "[stderr] "+strings.Repeat("e", 40)
	w.Line(out)
	w.Line(out)
	expect("within the hour's budget", out, out)
	w.Line("x")
	w.Line("y")
	expect("over it", "[ork] stdout passed its 100 bytes an hour (ORK_X); what it prints until 2026-11-25T11:00:00Z is dropped")
	w.Line(errs)
	w.Line(errs)
	expect("the other stream has its own", errs, errs)
	w.Orker("[ork] stopping")
	expect("the Orker's own", "[ork] stopping")
	now = now.Add(time.Hour - time.Nanosecond)
	w.Line("z")
	expect("still that hour")
	now = now.Add(time.Nanosecond)
	w.Line("z")
	expect("the next hour", "[ork] 6 bytes of stdout were dropped", "z")
	w.Line(strings.Repeat("o", 90))
	w.Line(strings.Repeat("o", 20))
	if got := take(); len(got) != 2 || !strings.HasPrefix(got[1], "[ork] stdout passed") {
		t.Errorf("a line over what is left: %q", got)
	}
	// Nothing is kept past the first line dropped, even what would fit.
	w.Line("")
	w.Line(strings.Repeat("o", 97))
	expect("the same hour, dropping")
	now = now.Add(2 * time.Hour)
	w.Close()
	expect("the attempt's end", "[ork] 120 bytes of stdout were dropped")
	w.Close()
	expect("closed again")
}

// TestLogWindow_TheHeadIsTheFirstMegabyte: a line that does not fit in the first
// megabyte ends it; shorter ones after it are the hour's.
func TestLogWindow_TheHeadIsTheFirstMegabyte(t *testing.T) {
	kept := 0
	w := &LogWindow{PerHour: 0, Write: func(l string) {
		if !strings.HasPrefix(l, "[ork] ") {
			kept++
		}
	}, Now: time.Now}
	w.Line("short")
	w.Line(strings.Repeat("l", 1<<20))
	w.Line("short")
	if kept != 1 {
		t.Errorf("%d lines kept; want the first alone", kept)
	}
}
