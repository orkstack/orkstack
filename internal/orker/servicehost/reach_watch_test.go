package servicehost

import (
	"strings"
	"testing"
	"time"
)

var rw0 = time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)

func clean() ProbeResult { return ProbeResult{Ran: true, Fitness: Fitness{Fit: true}} }
func open() ProbeResult {
	return ProbeResult{Ran: true, Fitness: Fitness{Failures: []string{"question 4: tcp reached 172.17.0.3:8080 (address)"}}}
}
func notRun() ProbeResult { return ProbeResult{Err: "the canary could not be started"} }
func refused() ProbeResult {
	return ProbeResult{ImageRefused: true, Err: "the probe image ghcr.io/x/probe@sha256:… could not be pulled"}
}

func TestReachWatch(t *testing.T) {
	type step struct {
		at      time.Duration // after rw0
		record  *ProbeResult
		mayHost bool
		evict   bool
		reason  string // a fragment of the reasons, "" for none
	}
	r := func(p ProbeResult) *ProbeResult { return &p }
	tests := []struct {
		name  string
		steps []step
	}{
		{name: "nothing before the first probe", steps: []step{{at: 0, reason: "no reach probe has run"}}},
		{name: "a clean probe hosts", steps: []step{{at: 0, record: r(clean()), mayHost: true}}},
		{name: "open before hosting: hosts nothing, names it", steps: []step{
			{at: 0, record: r(open()), reason: "question 4"}}},
		{name: "open once while hosting changes nothing; open twice evicts", steps: []step{
			{at: 0, record: r(clean()), mayHost: true},
			{at: time.Hour, record: r(open()), mayHost: true},
			{at: time.Hour + 2*time.Minute, record: r(open()), evict: true, reason: "question 4"}}},
		{name: "open then clean is clean", steps: []step{
			{at: 0, record: r(clean()), mayHost: true},
			{at: time.Hour, record: r(open()), mayHost: true},
			{at: time.Hour + 2*time.Minute, record: r(clean()), mayHost: true}}},
		{name: "a probe that could not run evicts nothing; past two hours it takes nothing new; past a day it evicts", steps: []step{
			{at: 0, record: r(clean()), mayHost: true},
			{at: time.Hour, record: r(notRun()), mayHost: true},
			{at: 2*time.Hour + time.Minute, record: r(notRun()), reason: "could not be run"},
			{at: 24*time.Hour + time.Minute, record: r(notRun()), evict: true, reason: "could not be run"}}},
		{name: "a probe that never ran hosts nothing", steps: []step{{at: 0, record: r(notRun()), reason: "could not be run"}}},
		{name: "a refused image takes nothing new at once, keeps what it hosts under a day", steps: []step{
			{at: 0, record: r(clean()), mayHost: true},
			{at: time.Hour, record: r(refused()), reason: "could not be pulled"},
			{at: 25 * time.Hour, record: r(refused()), evict: true, reason: "could not be pulled"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewReachWatch(13 * time.Minute)
			for i, s := range tt.steps {
				now := rw0.Add(s.at)
				if s.record != nil {
					w.Record(*s.record, now)
				}
				v := w.Verdict(now)
				if v.MayHost != s.mayHost || v.Evict != s.evict {
					t.Fatalf("step %d: mayHost %v evict %v; want %v %v (%q)", i, v.MayHost, v.Evict, s.mayHost, s.evict, v.Reasons)
				}
				joined := strings.Join(v.Reasons, "; ")
				if (s.reason == "") != (len(v.Reasons) == 0) || !strings.Contains(joined, s.reason) {
					t.Fatalf("step %d: reasons %q; want one containing %q", i, v.Reasons, s.reason)
				}
				if v.Evict && v.MayHost {
					t.Fatalf("step %d: an evicting host may host", i)
				}
			}
		})
	}
}

// The next probe: hourly at the Orker's own offset, two minutes after an
// answer that was open or a probe that could not run.
func TestReachWatch_Next(t *testing.T) {
	w := NewReachWatch(13 * time.Minute)
	w.Record(clean(), rw0.Add(5*time.Minute))
	if got, want := w.Next(), rw0.Add(time.Hour+13*time.Minute); !got.Equal(want) {
		t.Fatalf("after a clean probe the next is %v; want %v (the hour, at its offset)", got, want)
	}
	w.Record(open(), rw0.Add(time.Hour+13*time.Minute))
	if got, want := w.Next(), rw0.Add(time.Hour+15*time.Minute); !got.Equal(want) {
		t.Fatalf("after an open answer the next is %v; want %v", got, want)
	}
	w.Record(notRun(), rw0.Add(time.Hour+15*time.Minute))
	if got, want := w.Next(), rw0.Add(time.Hour+17*time.Minute); !got.Equal(want) {
		t.Fatalf("after a probe that could not run the next is %v; want %v", got, want)
	}
	if NewReachWatch(-time.Minute).offset != 0 || NewReachWatch(2*time.Hour).offset >= time.Hour {
		t.Fatal("an offset is within the hour")
	}
}

// A probe that could not run after one that found the host unclean is no
// evidence the host is clean again: the last answer that ran stands, so the
// Orker takes nothing new, and evicts when that answer was unclean twice.
func TestReachWatch_NotRunAfterUncleanKeepsUnclean(t *testing.T) {
	w := NewReachWatch(0)
	w.Record(clean(), rw0)
	w.Record(open(), rw0.Add(time.Hour))
	w.Record(notRun(), rw0.Add(time.Hour+2*time.Minute))
	if v := w.Verdict(rw0.Add(time.Hour + 3*time.Minute)); v.MayHost || !strings.Contains(strings.Join(v.Reasons, ";"), "question 4") {
		t.Fatalf("after an unclean answer and a probe that did not run: %+v", v)
	}
}
