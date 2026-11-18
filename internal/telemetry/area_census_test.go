package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWebhookPendingCensus_ReportsTheLastGoodPassAndNothingAfterAFailure(t *testing.T) {
	p, reader := areaProviders(t)
	fail := false
	pass := newWebhookPendingCensus(p, discardLogger(), func(_ context.Context, olderThan time.Duration) (int64, error) {
		if fail {
			return 0, errors.New("the cluster is away")
		}
		if olderThan == time.Hour {
			return 1, nil
		}
		return 4, nil
	})
	if points := collectGC(t, reader); len(points[WebhookPendingMetric]) != 0 {
		t.Fatalf("reported before any pass: %v", points[WebhookPendingMetric])
	}
	if err := pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	points := collectGC(t, reader)
	if got := value(t, points, WebhookPendingMetric, map[string]string{AttrAgeOver: "5m"}); got != 4 {
		t.Fatalf("over 5m = %v", got)
	}
	if got := value(t, points, WebhookPendingMetric, map[string]string{AttrAgeOver: "1h"}); got != 1 {
		t.Fatalf("over 1h = %v", got)
	}
	fail = true
	if err := pass(context.Background()); err == nil {
		t.Fatal("a failed pass reported no error")
	}
	if points := collectGC(t, reader); len(points[WebhookPendingMetric]) != 0 {
		t.Fatalf("a failed pass left a healthy-looking value: %v", points[WebhookPendingMetric])
	}
}

func TestScheduleCensus_CountsStatesAndTheSoonestFireAndStopsReportingWhatIsGone(t *testing.T) {
	p, reader := areaProviders(t)
	soon := time.Date(2026, 11, 17, 13, 0, 0, 0, time.UTC)
	samples := []ScheduleSample{
		{Namespace: "acme", Name: "nightly", NextFire: soon.Add(time.Hour), Described: true},
		{Namespace: "acme", Name: "hourly", NextFire: soon, Described: true},
		{Namespace: "acme", Name: "frozen", Paused: true, NextFire: soon.Add(-time.Hour)},
		{Namespace: "beta", Name: "weekly", NextFire: soon.Add(24 * time.Hour), Described: true},
	}
	c := newScheduleCensus(p, ScheduleCensusSource{Read: func(context.Context, int) ([]ScheduleSample, error) {
		return samples, nil
	}}, 60, discardLogger())
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	points := collectGC(t, reader)
	if got := value(t, points, SchedulesMetric, map[string]string{AttrNamespace: "acme", AttrScheduleState: "active"}); got != 2 {
		t.Fatalf("acme active = %v", got)
	}
	if got := value(t, points, SchedulesMetric, map[string]string{AttrNamespace: "acme", AttrScheduleState: "paused"}); got != 1 {
		t.Fatalf("acme paused = %v", got)
	}
	if got := value(t, points, ScheduleNextFireMetric, map[string]string{AttrNamespace: "acme"}); got != float64(soon.Unix()) {
		t.Fatalf("acme next fire = %v, want the soonest active schedule's %d (a paused one never counts)", got, soon.Unix())
	}
	if got := value(t, points, ScheduleCensusCoverageMetric, map[string]string{}); got != 0.75 {
		t.Fatalf("coverage = %v, want 3 of 4", got)
	}
	if got := value(t, points, ScheduleCensusUnavailableMetric, map[string]string{}); got != 0 {
		t.Fatalf("unavailable = %v", got)
	}
	if c.cost() != 1+4 {
		t.Fatalf("cost = %d, want one list and four describes", c.cost())
	}

	samples = samples[:1] // beta's schedule was deleted
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, p := range collectGC(t, reader)[ScheduleNextFireMetric] {
		if p.labels[AttrNamespace] == "beta" {
			t.Fatalf("a namespace with no schedule left still reports a next fire: %v", p)
		}
	}
}

func TestScheduleCensus_CountsSkippedFiresAsTheIncreaseOfTheClustersTotals(t *testing.T) {
	p, reader := areaProviders(t)
	sample := ScheduleSample{Namespace: "acme", Name: "nightly", Described: true, SkippedOverlap: 7, MissedCatchup: 2}
	c := newScheduleCensus(p, ScheduleCensusSource{ObjectLabels: true, Read: func(context.Context, int) ([]ScheduleSample, error) {
		return []ScheduleSample{sample}, nil
	}}, 60, discardLogger())
	ctx := context.Background()
	if err := c.pass(ctx); err != nil {
		t.Fatal(err)
	}
	if points := collectGC(t, reader); len(points[ScheduleActionsSkippedMetric]) != 0 {
		t.Fatalf("the first sighting counted the cluster's history as an increase: %v", points[ScheduleActionsSkippedMetric])
	}
	sample.SkippedOverlap, sample.MissedCatchup = 10, 2
	if err := c.pass(ctx); err != nil {
		t.Fatal(err)
	}
	sample.SkippedOverlap = 1 // recreated: its total restarted
	if err := c.pass(ctx); err != nil {
		t.Fatal(err)
	}
	points := collectGC(t, reader)
	if got := value(t, points, ScheduleActionsSkippedMetric, map[string]string{AttrNamespace: "acme", AttrSchedule: "nightly",
		AttrScheduleSkipReason: "overlap"}); got != 4 {
		t.Fatalf("skipped for overlap = %v, want 3 then 1 after the restart", got)
	}
	for _, p := range points[ScheduleActionsSkippedMetric] {
		if p.labels[AttrScheduleSkipReason] == "catchup_window" {
			t.Fatalf("an unchanged total counted an increase: %v", p)
		}
	}
}

func TestScheduleCensus_AListThatFailsIsUnavailableAndReportsNoCoverage(t *testing.T) {
	p, reader := areaProviders(t)
	c := newScheduleCensus(p, ScheduleCensusSource{Read: func(context.Context, int) ([]ScheduleSample, error) {
		return nil, errors.New("the cluster is away")
	}}, 60, discardLogger())
	if err := c.pass(context.Background()); err == nil {
		t.Fatal("a failed list reported no error")
	}
	points := collectGC(t, reader)
	if got := value(t, points, ScheduleCensusUnavailableMetric, map[string]string{}); got != 1 {
		t.Fatalf("unavailable = %v", got)
	}
	if len(points[ScheduleCensusCoverageMetric]) != 0 {
		t.Fatalf("a failed list reported coverage: %v", points[ScheduleCensusCoverageMetric])
	}
}

func TestScheduleCensus_ADroppedNamespaceReportsNothing(t *testing.T) {
	p, reader := areaProviders(t)
	c := newScheduleCensus(p, ScheduleCensusSource{Route: dropAll, Read: func(context.Context, int) ([]ScheduleSample, error) {
		return []ScheduleSample{{Namespace: "acme", Name: "nightly", NextFire: time.Now()}}, nil
	}}, 60, discardLogger())
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	points := collectGC(t, reader)
	for _, name := range []string{SchedulesMetric, ScheduleNextFireMetric, ScheduleActionsSkippedMetric} {
		if len(points[name]) != 0 {
			t.Fatalf("%s reported a dropped namespace: %v", name, points[name])
		}
	}
}

// TestScheduleCensus_AFailedListReportsNoOldReading: after a pass that could
// not list the schedules, the schedule gauges report nothing rather than the
// last good pass, whose next fires may since have passed.
func TestScheduleCensus_AFailedListReportsNoOldReading(t *testing.T) {
	p, reader := areaProviders(t)
	fail := false
	c := newScheduleCensus(p, ScheduleCensusSource{Read: func(context.Context, int) ([]ScheduleSample, error) {
		if fail {
			return nil, errors.New("the cluster is away")
		}
		return []ScheduleSample{{Namespace: "acme", Name: "nightly", NextFire: time.Now()}}, nil
	}}, 60, discardLogger())
	ctx := context.Background()
	if err := c.pass(ctx); err != nil {
		t.Fatal(err)
	}
	fail = true
	_ = c.pass(ctx)
	points := collectGC(t, reader)
	for _, name := range []string{SchedulesMetric, ScheduleNextFireMetric, ScheduleCensusCoverageMetric} {
		if len(points[name]) != 0 {
			t.Fatalf("%s repeated an old reading through a failed list: %v", name, points[name])
		}
	}
}

// TestScheduleCensus_DescribesAtMostHalfTheBudgetAndForgetsDeletedSchedules:
// a pass asks for at most half the census budget of describes, and a
// schedule no longer listed leaves no baseline behind.
func TestScheduleCensus_DescribesAtMostHalfTheBudgetAndForgetsDeletedSchedules(t *testing.T) {
	p, _ := areaProviders(t)
	var asked int
	samples := []ScheduleSample{{Namespace: "acme", Name: "a", Described: true}, {Namespace: "acme", Name: "b", Described: true}}
	c := newScheduleCensus(p, ScheduleCensusSource{Read: func(_ context.Context, max int) ([]ScheduleSample, error) {
		asked = max
		return samples, nil
	}}, 60, discardLogger())
	ctx := context.Background()
	if err := c.pass(ctx); err != nil {
		t.Fatal(err)
	}
	if asked != 30 {
		t.Fatalf("asked for %d describes, want half of a 60-request budget", asked)
	}
	samples = samples[:1]
	if err := c.pass(ctx); err != nil {
		t.Fatal(err)
	}
	if _, kept := c.skipped[scheduleSeries{"acme", "b"}]; kept || len(c.skipped) != 1 {
		t.Fatalf("baselines = %v, want only the schedule still listed", c.skipped)
	}
	if got := c.cost(); got != 1+1 {
		t.Fatalf("cost = %d, want one list and one describe", got)
	}
}

// TestScheduleCensus_ACollectionCompletesWhileAPassIsRouting: a pass whose
// routing is slow (a namespace's policy read from the store) never holds up
// a metrics collection, whose gauge callbacks read only the last snapshot.
func TestScheduleCensus_ACollectionCompletesWhileAPassIsRouting(t *testing.T) {
	p, reader := areaProviders(t)
	block := make(chan struct{})
	routing := make(chan struct{}, 1)
	slow := false
	route := func(context.Context, string) (*Providers, bool) {
		if slow {
			routing <- struct{}{}
			<-block
		}
		return p, true
	}
	c := newScheduleCensus(p, ScheduleCensusSource{Route: route, Read: func(context.Context, int) ([]ScheduleSample, error) {
		return []ScheduleSample{{Namespace: "acme", Name: "nightly", NextFire: time.Now()}}, nil
	}}, 60, discardLogger())
	ctx := context.Background()
	if err := c.pass(ctx); err != nil { // registers the sink's gauges
		t.Fatal(err)
	}
	slow = true
	passDone := make(chan error, 1)
	go func() { passDone <- c.pass(ctx) }()
	<-routing // the pass is now inside its routing

	collected := make(chan map[string][]gcPoint, 1)
	go func() { collected <- collectGC(t, reader) }()
	select {
	case points := <-collected:
		if got := value(t, points, SchedulesMetric, map[string]string{AttrNamespace: "acme", AttrScheduleState: "active"}); got != 1 {
			t.Fatalf("active = %v, want the last pass's reading", got)
		}
	case <-time.After(10 * time.Second):
		close(block)
		t.Fatal("a collection waited on a census pass")
	}
	close(block)
	if err := <-passDone; err != nil {
		t.Fatal(err)
	}
}
