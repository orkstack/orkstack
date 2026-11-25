package engine

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestGateViewOf_OrdersHoldersByGrantAndTheLineByArrival: a state answer
// becomes the view ConcurrencyService shows, its key carried, holders in
// grant order, the line in the order it grants, task entries marked, and a
// failing rebuild's start kept.
func TestGateViewOf_OrdersHoldersByGrantAndTheLineByArrival(t *testing.T) {
	t0 := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	g := gateState{
		Epoch: 3,
		Holders: []gateEntry{
			{Run: "late", TaskIx: gateRunLevel, GrantedAt: t0.Add(time.Minute), Strategy: gateStrategyQueue, Limit: 2},
			{Run: "early", TaskIx: 0, GrantedAt: t0, Strategy: gateStrategyRefuse, Limit: 2, Stalled: true},
		},
		Queue: []gateEntry{
			{Run: "second", TaskIx: gateRunLevel, ArrivedAt: t0.Add(2 * time.Minute), Seq: 5, WaitUntil: t0.Add(time.Hour)},
			{Run: "first", TaskIx: gateRunLevel, ArrivedAt: t0.Add(time.Minute), Seq: 4},
		},
		Rebuild: gateRebuild{State: gateRebuildFailing, Since: t0},
	}
	v := gateViewOf("ork-conc-1", GateStateView{Namespace: "default", Key: "db:prod", State: g})
	if !v.Answering || v.Key != "db:prod" || v.Epoch != 3 || v.Rebuild != GateRebuildFailing || !v.RebuildFailingSince.Equal(t0) {
		t.Fatalf("view = %+v", v)
	}
	if v.Holders[0].Run != "early" || !v.Holders[0].TaskLevel || !v.Holders[0].Stalled || v.Holders[1].Run != "late" || v.Holders[1].TaskLevel {
		t.Fatalf("holders = %+v; want early (task, stalled) then late", v.Holders)
	}
	if v.Line[0].Run != "first" || v.Line[1].Run != "second" || !v.Line[1].TimesOutAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("line = %+v; want first then second, with its timeout", v.Line)
	}
}

type arrivalRecorder struct{ got []string }

func (r *arrivalRecorder) ObserveArrival(_ context.Context, namespace, orkflow, limit, level, strategy, outcome string) {
	r.got = append(r.got, strings.Join([]string{namespace, orkflow, limit, level, strategy, outcome}, "|"))
}

// TestObserveArrival_CountsEachAnsweredAcquireOnce: an answer is counted
// under its outcome with the declaration's label, never the key; a
// re-assert, which asks again for a place it has, and a supersede are not.
func TestObserveArrival_CountsEachAnsweredAcquireOnce(t *testing.T) {
	rec := &arrivalRecorder{}
	a := (&GateActivities{}).WithArrivalObserver(rec)
	in := AcquireConcurrencyInput{Orkflow: "migrate", Start: GateStart{Namespace: "acme", Key: "db:prod"},
		Request: gateAcquire{TaskIx: gateRunLevel, Strategy: gateStrategyQueue, Label: "db:{{ .inputs.env }}"}}
	label := CensusLimitLabel("db:{{ .inputs.env }}")
	for _, res := range []AcquireConcurrencyResult{
		{Answer: gateAnswer{Answer: gateAnswerQueued}},
		{Answer: gateAnswer{Answer: gateAnswerFullBound}},
		{Unavailable: "deadline"},
		{Answer: gateAnswer{Answer: gateAnswerSuperseded}},
	} {
		a.observeArrival(context.Background(), in, res)
	}
	task := in
	task.Request.TaskIx = 0
	a.observeArrival(context.Background(), task, AcquireConcurrencyResult{Answer: gateAnswer{Answer: gateAnswerGranted}})
	reassert := in
	reassert.Request.ArrivedAt = time.Now()
	a.observeArrival(context.Background(), reassert, AcquireConcurrencyResult{Answer: gateAnswer{Answer: gateAnswerQueued}})
	want := []string{"acme|migrate|" + label + "|run|queue|queued", "acme|migrate|" + label + "|run|queue|full",
		"acme|migrate|" + label + "|run|queue|unavailable", "acme|migrate|" + label + "|task|queue|granted"}
	if strings.Join(rec.got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("arrivals =\n%s\nwant\n%s", strings.Join(rec.got, "\n"), strings.Join(want, "\n"))
	}
}
