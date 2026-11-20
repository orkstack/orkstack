package telemetry

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// byPoolAndRelease is one metric's points by "pool/release", failing on any
// other label set.
func byPoolAndRelease(t *testing.T, points []metricPoint) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, p := range points {
		pool, okPool := p.attrs.Value(attribute.Key(AttrPool))
		release, okRelease := p.attrs.Value(attribute.Key(AttrRelease))
		if p.attrs.Len() != 2 || !okPool || !okRelease {
			t.Fatalf("point labels = %v; want ork.pool and ork.release", p.attrs.ToSlice())
		}
		out[pool.AsString()+"/"+release.AsString()] = p.value
	}
	return out
}

// A pool's backlog by release is a breakdown beside its total: one series per
// pool and release, the total's own series unchanged and labelled with the
// pool alone, an age only where one was measured, and nothing for a pool
// whose read brought no breakdown (versioning off).
func TestPoolBacklog_ReleaseBreakdownBesideTheTotal(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	reads := &fakePoolReads{reads: map[string]PoolReading{
		"main": {Backlog: 9, Releases: []PoolReleaseReading{
			{Release: "b-new", Backlog: 5, BacklogAge: 10 * time.Second, HasAge: true},
			{Release: "unversioned", Backlog: 4},
		}},
		"gpu": {Backlog: 2},
	}}
	reader, pass := newPoolBacklogTest(t, []string{"main", "gpu"}, reads, &now)
	if err := pass(context.Background()); err != nil {
		t.Fatalf("pass = %v", err)
	}
	points := metricPoints(t, reader)
	if got, want := byPool(t, points[PoolBacklogTasksMetric]), map[string]float64{"main": 9, "gpu": 2}; !maps.Equal(got, want) {
		t.Errorf("%s = %v; want %v, unchanged by the breakdown", PoolBacklogTasksMetric, got, want)
	}
	if got, want := byPoolAndRelease(t, points[PoolReleaseBacklogTasksMetric]), map[string]float64{"main/b-new": 5, "main/unversioned": 4}; !maps.Equal(got, want) {
		t.Errorf("%s = %v; want %v", PoolReleaseBacklogTasksMetric, got, want)
	}
	if got, want := byPoolAndRelease(t, points[PoolReleaseBacklogAgeMetric]), map[string]float64{"main/b-new": 10}; !maps.Equal(got, want) {
		t.Errorf("%s = %v; want %v: unversioned work has no measured age", PoolReleaseBacklogAgeMetric, got, want)
	}
}

// A breakdown that could not be read leaves the pool's total reported and its
// release series absent, and the pass names the pool.
func TestPoolBacklog_AnUnreadBreakdownKeepsTheTotal(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	reads := &fakePoolReads{reads: map[string]PoolReading{
		"main": {Backlog: 9, ReleasesErr: errors.New("frontend down")},
	}}
	reader, pass := newPoolBacklogTest(t, []string{"main"}, reads, &now)
	if err := pass(context.Background()); err == nil {
		t.Fatal("pass = nil; want the unread breakdown named")
	}
	points := metricPoints(t, reader)
	if got := byPool(t, points[PoolBacklogTasksMetric]); got["main"] != 9 {
		t.Errorf("%s = %v; want main's 9 still reported", PoolBacklogTasksMetric, got)
	}
	if n := len(points[PoolReleaseBacklogTasksMetric]); n != 0 {
		t.Errorf("%s has %d points; want none when the breakdown was not read", PoolReleaseBacklogTasksMetric, n)
	}
}
