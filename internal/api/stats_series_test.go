package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	// The zone database is linked into this test binary, as it is into every
	// release binary (internal/dist.BuildTags), so the boundaries below do not
	// depend on what the host happens to have installed.
	_ "time/tzdata"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// These tests drive the series through GetStats, the RPC a client calls, and
// assert only what the client receives: bucket bounds, counts, and refusals
// with their codes and words. Every expected instant is written out by hand
// in the zone's own offset, so a reader can check it against a calendar, and
// none is computed with the code under test. Each test names the mutations
// it exists to catch.

const (
	dayBuckets  = orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_DAY
	hourBuckets = orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR
)

func at(t testing.TB, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	must.OKf(t, err, "parse %s", raw)
	return parsed
}

func getStats(t *testing.T, handler *api.StatsHandler, req *orkstackv1.GetStatsRequest) *orkstackv1.GetStatsResponse {
	t.Helper()
	got, err := handler.GetStats(context.Background(), connect.NewRequest(req))
	must.OKf(t, err, "GetStats(%v)", req)
	return got.Msg
}

// bucketStarts is every bucket's first instant, for a comparison by eye.
func bucketStarts(got *orkstackv1.GetStatsResponse) []time.Time {
	starts := make([]time.Time, 0, len(got.GetBuckets()))
	for _, bucket := range got.GetBuckets() {
		starts = append(starts, bucket.GetSince().AsTime())
	}
	return starts
}

// TestGetStatsSeries_BucketsFollowTheZonesWallClock asks for a series over a
// window around every kind of clock change a zone database holds, and
// requires the buckets to start exactly where the zone's own wall clock turns
// to a new date (DAY) or hour (HOUR).
//
// Mutations it catches: a fixed offset instead of the zone (every case past a
// transition); a day assumed to be 24 hours or an hour 60 minutes (the LA,
// Havana, Santiago and Lord Howe cases); truncating division instead of a
// floor before 1970; hours drawn on UTC's clock in a :30 or :45 zone; a
// transition that keeps the label treated as a boundary (the repeated hours);
// a skipped date given a bucket (Apia).
func TestGetStatsSeries_BucketsFollowTheZonesWallClock(t *testing.T) {
	for _, tc := range []struct {
		name   string
		zone   string
		width  orkstackv1.StatsBucketWidth
		since  string
		starts []string // the window's own start first, then every boundary
		until  string
	}{
		{
			name: "UTC days", zone: "UTC", width: dayBuckets,
			since:  "2026-10-30T05:00:00Z",
			starts: []string{"2026-10-30T05:00:00Z", "2026-10-31T00:00:00Z", "2026-11-01T00:00:00Z"},
			until:  "2026-11-01T01:00:00Z",
		},
		{
			name: "hours before 1970 floor toward the past", zone: "UTC", width: hourBuckets,
			since:  "1960-01-01T00:00:30Z",
			starts: []string{"1960-01-01T00:00:30Z", "1960-01-01T01:00:00Z", "1960-01-01T02:00:00Z"},
			until:  "1960-01-01T02:30:00Z",
		},
		{
			name: "the US fall-back makes a 25-hour day", zone: "America/Los_Angeles", width: dayBuckets,
			since: "2026-10-31T00:00:00-07:00",
			starts: []string{"2026-10-31T00:00:00-07:00", "2026-11-01T00:00:00-07:00",
				"2026-11-02T00:00:00-08:00", "2026-11-03T00:00:00-08:00"},
			until: "2026-11-03T12:00:00-08:00",
		},
		{
			name: "the hour a fall-back repeats is one two-hour bucket", zone: "America/Los_Angeles",
			width: hourBuckets, since: "2026-11-01T00:00:00-07:00",
			starts: []string{"2026-11-01T00:00:00-07:00", "2026-11-01T01:00:00-07:00",
				"2026-11-01T02:00:00-08:00", "2026-11-01T03:00:00-08:00"},
			until: "2026-11-01T03:30:00-08:00",
		},
		{
			name: "the US spring-forward makes a 23-hour day", zone: "America/Los_Angeles", width: dayBuckets,
			since:  "2026-03-08T00:00:00-08:00",
			starts: []string{"2026-03-08T00:00:00-08:00", "2026-03-09T00:00:00-07:00"},
			until:  "2026-03-09T06:00:00-07:00",
		},
		{
			name: "the hour a spring-forward skips has no bucket", zone: "America/Los_Angeles",
			width: hourBuckets, since: "2026-03-08T00:00:00-08:00",
			starts: []string{"2026-03-08T00:00:00-08:00", "2026-03-08T01:00:00-08:00",
				"2026-03-08T03:00:00-07:00", "2026-03-08T04:00:00-07:00"},
			until: "2026-03-08T04:30:00-07:00",
		},
		{
			name: "a skipped midnight starts the day at the transition", zone: "America/Sao_Paulo",
			width: dayBuckets, since: "2018-11-03T00:00:00-03:00",
			starts: []string{"2018-11-03T00:00:00-03:00", "2018-11-04T01:00:00-02:00", "2018-11-05T00:00:00-02:00"},
			until:  "2018-11-05T06:00:00-02:00",
		},
		{
			name: "a repeated midnight stays one 25-hour day", zone: "America/Havana", width: dayBuckets,
			since:  "2026-10-31T08:00:00-04:00",
			starts: []string{"2026-10-31T08:00:00-04:00", "2026-11-01T00:00:00-04:00", "2026-11-02T00:00:00-05:00"},
			until:  "2026-11-02T06:00:00-05:00",
		},
		{
			name: "a fall-back from 24:00 repeats the hour before midnight", zone: "America/Santiago",
			width: hourBuckets, since: "2026-04-04T22:00:00-03:00",
			starts: []string{"2026-04-04T22:00:00-03:00", "2026-04-04T23:00:00-03:00",
				"2026-04-05T00:00:00-04:00", "2026-04-05T01:00:00-04:00"},
			until: "2026-04-05T01:30:00-04:00",
		},
		{
			name: "a skipped date has no bucket", zone: "Pacific/Apia", width: dayBuckets,
			since: "2011-12-28T00:00:00-10:00",
			starts: []string{"2011-12-28T00:00:00-10:00", "2011-12-29T00:00:00-10:00",
				"2011-12-31T00:00:00+14:00", "2012-01-01T00:00:00+14:00"},
			until: "2012-01-01T12:00:00+14:00",
		},
		{
			name: "a 30-minute spring-forward makes a 30-minute hour", zone: "Australia/Lord_Howe",
			width: hourBuckets, since: "2026-10-04T00:30:00+10:30",
			starts: []string{"2026-10-04T00:30:00+10:30", "2026-10-04T01:00:00+10:30",
				"2026-10-04T02:30:00+11:00", "2026-10-04T03:00:00+11:00"},
			until: "2026-10-04T03:30:00+11:00",
		},
		{
			name: "a 30-minute fall-back makes a 90-minute hour", zone: "Australia/Lord_Howe",
			width: hourBuckets, since: "2026-04-05T00:00:00+11:00",
			starts: []string{"2026-04-05T00:00:00+11:00", "2026-04-05T01:00:00+11:00",
				"2026-04-05T02:00:00+10:30", "2026-04-05T03:00:00+10:30"},
			until: "2026-04-05T03:30:00+10:30",
		},
		{
			name: "a half-hour zone's hours start at half past", zone: "Asia/Kolkata", width: hourBuckets,
			since:  "2026-11-01T05:30:00+05:30",
			starts: []string{"2026-11-01T05:30:00+05:30", "2026-11-01T06:00:00+05:30", "2026-11-01T07:00:00+05:30"},
			until:  "2026-11-01T07:30:00+05:30",
		},
		{
			name: "a quarter-hour zone's hours start at a quarter past", zone: "Asia/Kathmandu",
			width: hourBuckets, since: "2026-11-01T05:45:00+05:45",
			starts: []string{"2026-11-01T05:45:00+05:45", "2026-11-01T06:00:00+05:45", "2026-11-01T07:00:00+05:45"},
			until:  "2026-11-01T07:30:00+05:45",
		},
		{
			name: "a leap year's last day under an extended rule", zone: "America/Los_Angeles", width: dayBuckets,
			since:  "2040-12-30T00:00:00-08:00",
			starts: []string{"2040-12-30T00:00:00-08:00", "2040-12-31T00:00:00-08:00", "2041-01-01T00:00:00-08:00"},
			until:  "2041-01-01T06:00:00-08:00",
		},
		{
			name: "a leap year's last hours under an extended rule", zone: "Europe/Lisbon", width: hourBuckets,
			since:  "2040-12-30T23:00:00Z",
			starts: []string{"2040-12-30T23:00:00Z", "2040-12-31T00:00:00Z", "2040-12-31T01:00:00Z"},
			until:  "2040-12-31T01:30:00Z",
		},
		{
			name: "a fall-back from midnight keeps the date", zone: "Asia/Beirut", width: dayBuckets,
			since:  "2026-10-24T15:00:00+03:00",
			starts: []string{"2026-10-24T15:00:00+03:00", "2026-10-25T00:00:00+02:00", "2026-10-26T00:00:00+02:00"},
			until:  "2026-10-26T06:00:00+02:00",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			until := at(t, tc.until)
			fixture := newStatsFixture(t, until, "payments")
			got := getStats(t, api.NewStatsHandler(fixture.svc, 0), &orkstackv1.GetStatsRequest{
				Namespace: "payments", BucketWidth: tc.width, Timezone: tc.zone,
				Since: timestamppb.New(at(t, tc.since)), Until: timestamppb.New(until),
			})
			starts := bucketStarts(got)
			if len(starts) != len(tc.starts) {
				t.Fatalf("%d buckets starting %v, want %d starting %v", len(starts), starts, len(tc.starts), tc.starts)
			}
			for i, raw := range tc.starts {
				if want := at(t, raw); !starts[i].Equal(want) {
					t.Errorf("bucket %d starts %s, want %s", i, starts[i].Format(time.RFC3339), raw)
				}
			}
			if last := got.GetBuckets()[len(got.GetBuckets())-1].GetUntil().AsTime(); !last.Equal(until) {
				t.Errorf("the series ends %s, want the window's end %s", last, until)
			}
		})
	}
}

// TestGetStatsSeries_EveryBoundaryAMinuteScanFinds compares the buckets
// GetStats draws with the boundaries found the slow way: stepping through the
// window a minute at a time and noting every minute whose wall-clock label —
// read with time.Time.In and nothing of the code under test — differs from
// the minute before. Every zone transition and every wall-clock unit change
// falls on a whole minute, so the scan misses none.
//
// Mutations it catches: a boundary missed inside a bucket (a label that
// changes and changes back would pass a check of a bucket's two ends); a
// boundary the scan does not find; the walk stalling or skipping where the
// zone database extends its rules past its last listed transition.
func TestGetStatsSeries_EveryBoundaryAMinuteScanFinds(t *testing.T) {
	for _, tc := range []struct {
		zone        string
		width       orkstackv1.StatsBucketWidth
		since, span string
	}{
		{"America/Los_Angeles", hourBuckets, "2026-10-31T00:00:00Z", "72h"},
		{"America/Los_Angeles", dayBuckets, "2040-12-25T00:00:00Z", "240h"},
		{"Australia/Lord_Howe", hourBuckets, "2026-10-03T00:00:00Z", "48h"},
		{"Australia/Lord_Howe", hourBuckets, "2027-04-02T00:00:00Z", "48h"},
		{"Antarctica/Troll", hourBuckets, "2026-10-24T00:00:00Z", "48h"},
		{"Pacific/Apia", dayBuckets, "2011-12-25T00:00:00Z", "240h"},
		{"Asia/Kathmandu", hourBuckets, "2026-11-01T00:00:00Z", "30h"},
		{"Europe/Lisbon", hourBuckets, "2204-12-30T00:00:00Z", "72h"},
	} {
		t.Run(tc.zone+" "+tc.width.String()+" from "+tc.since, func(t *testing.T) {
			zone, err := time.LoadLocation(tc.zone)
			must.OKf(t, err, "load %s", tc.zone)
			span, err := time.ParseDuration(tc.span)
			must.OKf(t, err, "parse %s", tc.span)
			since := at(t, tc.since)
			until := since.Add(span)
			want := []time.Time{since}
			for minute := since.Add(time.Minute); minute.Before(until); minute = minute.Add(time.Minute) {
				if wallLabel(minute, zone, tc.width) != wallLabel(minute.Add(-time.Minute), zone, tc.width) {
					want = append(want, minute)
				}
			}
			fixture := newStatsFixture(t, until, "payments")
			got := bucketStarts(getStats(t, api.NewStatsHandler(fixture.svc, 0), &orkstackv1.GetStatsRequest{
				Namespace: "payments", BucketWidth: tc.width, Timezone: tc.zone,
				Since: timestamppb.New(since), Until: timestamppb.New(until),
			}))
			if len(got) != len(want) {
				t.Fatalf("%d buckets, the scan finds %d:\n got  %v\n want %v", len(got), len(want), got, want)
			}
			for i := range want {
				if !got[i].Equal(want[i]) {
					t.Fatalf("bucket %d starts %s, the scan says %s", i, got[i].In(zone), want[i].In(zone))
				}
			}
		})
	}
}

// TestGetStatsSeries_EachLocalDayCountsTheRunsItCreated is the series'
// correctness test, from the viewer's side: it is 20:00 on Oct 31 in Los
// Angeles, which is already Nov 1 in UTC, and the viewer asks for the last
// seven days, today included.
//
// Mutations it catches: buckets drawn in UTC (the 18:00 run would land in a
// bucket called Nov 1); a closed bucket end (the run at Oct 30 00:00 counted
// in Oct 29); a window that starts a day early or late (the runs either side
// of Oct 25 00:00); a live run placed by anything but its creation time (the
// one going since Oct 28 would land elsewhere), or a live run from before the
// window put in the first bucket; a bucket map
// that drops a status the window keeps (skipped); partial set on a whole day.
func TestGetStatsSeries_EachLocalDayCountsTheRunsItCreated(t *testing.T) {
	now := at(t, "2026-11-01T03:00:00Z") // 20:00 PDT, Oct 31
	fixture := newStatsFixture(t, now, "payments")
	for _, run := range []struct {
		at     string
		status orkstackv1.Status
	}{
		{"2026-10-24T23:59:59-07:00", orkstackv1.Status_STATUS_SUCCEEDED}, // before the window
		{"2026-10-25T00:00:00-07:00", orkstackv1.Status_STATUS_SUCCEEDED}, // its first instant
		{"2026-10-25T09:00:00-07:00", orkstackv1.Status_STATUS_FAILED},
		{"2026-10-29T23:59:59-07:00", orkstackv1.Status_STATUS_CANCELED},
		{"2026-10-30T00:00:00-07:00", orkstackv1.Status_STATUS_SUCCEEDED}, // on a bucket edge
		{"2026-10-31T08:00:00-07:00", orkstackv1.Status_STATUS_FAILED},
		{"2026-10-31T18:00:00-07:00", orkstackv1.Status_STATUS_SUCCEEDED}, // Nov 1 in UTC
		{"2026-10-31T19:00:00-07:00", orkstackv1.Status_STATUS_SKIPPED},   // a status no key names
	} {
		fixture.archiveRun(t, "payments", "charge", run.status, at(t, run.at))
	}
	fixture.reader.executions["payments"] = engine.Executions{Running: []*orkstackv1.Run{
		liveRun("ork-live-today", "payments", "charge", at(t, "2026-10-31T19:30:00-07:00")),
		liveRun("ork-live-for-days", "payments", "charge", at(t, "2026-10-28T12:00:00-07:00")),
		liveRun("ork-live-old", "payments", "charge", at(t, "2026-10-01T00:00:00-07:00")),
	}}

	got := getStats(t, api.NewStatsHandler(fixture.svc, 0), &orkstackv1.GetStatsRequest{
		Namespace: "payments", BucketWidth: dayBuckets, Timezone: "America/Los_Angeles", BucketCount: 7,
	})

	if got.GetBucketWidth() != dayBuckets || got.GetTimezone() != "America/Los_Angeles" {
		t.Errorf("echoed %v in %q, want DAY in America/Los_Angeles", got.GetBucketWidth(), got.GetTimezone())
	}
	if !got.GetWindowSince().AsTime().Equal(at(t, "2026-10-25T00:00:00-07:00")) ||
		!got.GetWindowUntil().AsTime().Equal(now) {
		t.Errorf("window %s..%s, want local Oct 25 midnight until now",
			got.GetWindowSince().AsTime(), got.GetWindowUntil().AsTime())
	}
	none := map[string]int64{"running": 0, "succeeded": 0, "failed": 0, "canceled": 0}
	wants := []struct {
		since   string
		counts  map[string]int64
		partial bool
	}{
		{"2026-10-25T00:00:00-07:00", map[string]int64{"running": 0, "succeeded": 1, "failed": 1, "canceled": 0}, false},
		{"2026-10-26T00:00:00-07:00", none, false},
		{"2026-10-27T00:00:00-07:00", none, false},
		{"2026-10-28T00:00:00-07:00", map[string]int64{"running": 1, "succeeded": 0, "failed": 0, "canceled": 0}, false},
		{"2026-10-29T00:00:00-07:00", map[string]int64{"running": 0, "succeeded": 0, "failed": 0, "canceled": 1}, false},
		{"2026-10-30T00:00:00-07:00", map[string]int64{"running": 0, "succeeded": 1, "failed": 0, "canceled": 0}, false},
		{"2026-10-31T00:00:00-07:00", map[string]int64{
			"running": 1, "succeeded": 1, "failed": 1, "canceled": 0, "skipped": 1,
		}, true},
	}
	if len(got.GetBuckets()) != len(wants) {
		t.Fatalf("%d buckets, want %d: %v", len(got.GetBuckets()), len(wants), got.GetBuckets())
	}
	for i, w := range wants {
		b := got.GetBuckets()[i]
		if !b.GetSince().AsTime().Equal(at(t, w.since)) {
			t.Errorf("bucket %d starts %s, want %s", i, b.GetSince().AsTime(), w.since)
		}
		if !proto.Equal(&orkstackv1.StatsBucket{RunsByStatus: b.GetRunsByStatus()},
			&orkstackv1.StatsBucket{RunsByStatus: w.counts}) {
			t.Errorf("bucket %s counts %v, want %v", w.since, b.GetRunsByStatus(), w.counts)
		}
		if b.GetPartial() != w.partial {
			t.Errorf("bucket %s partial = %v, want %v", w.since, b.GetPartial(), w.partial)
		}
	}
	// The window's own counts, written out: the series and the totals are two
	// views of the same runs, and only the live run created before the window
	// is in the totals and in no bucket.
	wantWindow := map[string]int64{"running": 3, "succeeded": 3, "failed": 2, "canceled": 1, "skipped": 1}
	if !proto.Equal(&orkstackv1.StatsBucket{RunsByStatus: got.GetRunsByStatus()},
		&orkstackv1.StatsBucket{RunsByStatus: wantWindow}) {
		t.Errorf("the window counts %v, want %v", got.GetRunsByStatus(), wantWindow)
	}
}

// TestGetStatsSeries_ADSTWeekHasOne25HourDay asks on Nov 3 for the last seven
// days in Los Angeles: one of them, Nov 1, holds the hour the fall-back
// repeats.
//
// Mutations it catches: bucket_count counted in 24-hour steps back from now
// (every start before Nov 2 an hour off); a count that is not the number of
// buckets returned; a series that does not end at now.
func TestGetStatsSeries_ADSTWeekHasOne25HourDay(t *testing.T) {
	now := at(t, "2026-11-03T10:00:00-08:00")
	fixture := newStatsFixture(t, now, "payments")
	got := getStats(t, api.NewStatsHandler(fixture.svc, 0), &orkstackv1.GetStatsRequest{
		Namespace: "payments", BucketWidth: dayBuckets, Timezone: "America/Los_Angeles", BucketCount: 7,
	})
	want := []string{
		"2026-10-28T00:00:00-07:00", "2026-10-29T00:00:00-07:00", "2026-10-30T00:00:00-07:00",
		"2026-10-31T00:00:00-07:00", "2026-11-01T00:00:00-07:00", "2026-11-02T00:00:00-08:00",
		"2026-11-03T00:00:00-08:00",
	}
	starts := bucketStarts(got)
	if len(starts) != len(want) {
		t.Fatalf("buckets start %v, want %v", starts, want)
	}
	for i, raw := range want {
		if !starts[i].Equal(at(t, raw)) {
			t.Errorf("bucket %d starts %s, want %s", i, starts[i], raw)
		}
	}
	if last := got.GetBuckets()[6]; !last.GetUntil().AsTime().Equal(now) || !last.GetPartial() {
		t.Errorf("the last bucket ends %s (partial %v), want now, partial", last.GetUntil().AsTime(), last.GetPartial())
	}
}

// TestGetStatsSeries_CountedHoursKeepTheRepeatedHourWhole asks at 03:30 PST
// on the US fall-back day for the last four hours: the hour the clocks
// repeat is one two-hour bucket, so four buckets reach back five hours.
//
// Mutation it catches: bucket_count counted in 60-minute steps back from now,
// which would start at 00:30 PDT and draw the repeated hour as two buckets.
func TestGetStatsSeries_CountedHoursKeepTheRepeatedHourWhole(t *testing.T) {
	now := at(t, "2026-11-01T03:30:00-08:00")
	fixture := newStatsFixture(t, now, "payments")
	got := getStats(t, api.NewStatsHandler(fixture.svc, 0), &orkstackv1.GetStatsRequest{
		Namespace: "payments", BucketWidth: hourBuckets, Timezone: "America/Los_Angeles", BucketCount: 4,
	})
	want := []string{
		"2026-11-01T00:00:00-07:00", "2026-11-01T01:00:00-07:00",
		"2026-11-01T02:00:00-08:00", "2026-11-01T03:00:00-08:00",
	}
	starts := bucketStarts(got)
	if len(starts) != len(want) {
		t.Fatalf("buckets start %v, want %v", starts, want)
	}
	for i, raw := range want {
		if !starts[i].Equal(at(t, raw)) {
			t.Errorf("bucket %d starts %s, want %s", i, starts[i], raw)
		}
	}
}

// TestGetStatsSeries_ClipsAStatedWindow: a window stated by since and until is
// cut, never moved.
//
// Mutations it catches: a window widened to whole days (the first bucket
// would start at midnight); partial left unset on a clipped bucket, or set on
// a whole one; an empty zone read as anything but UTC.
func TestGetStatsSeries_ClipsAStatedWindow(t *testing.T) {
	since, until := at(t, "2026-10-30T09:00:00Z"), at(t, "2026-11-01T15:30:00Z")
	fixture := newStatsFixture(t, until, "payments")
	got := getStats(t, api.NewStatsHandler(fixture.svc, 0), &orkstackv1.GetStatsRequest{
		Namespace: "payments", BucketWidth: dayBuckets,
		Since: timestamppb.New(since), Until: timestamppb.New(until),
	})
	want := []struct {
		since, until string
		partial      bool
	}{
		{"2026-10-30T09:00:00Z", "2026-10-31T00:00:00Z", true},
		{"2026-10-31T00:00:00Z", "2026-11-01T00:00:00Z", false},
		{"2026-11-01T00:00:00Z", "2026-11-01T15:30:00Z", true},
	}
	if len(got.GetBuckets()) != len(want) {
		t.Fatalf("buckets %v, want %v", got.GetBuckets(), want)
	}
	for i, w := range want {
		b := got.GetBuckets()[i]
		if !b.GetSince().AsTime().Equal(at(t, w.since)) || !b.GetUntil().AsTime().Equal(at(t, w.until)) ||
			b.GetPartial() != w.partial {
			t.Errorf("bucket %d = %s..%s partial %v, want %s..%s partial %v", i,
				b.GetSince().AsTime(), b.GetUntil().AsTime(), b.GetPartial(), w.since, w.until, w.partial)
		}
	}
	if !got.GetWindowSince().AsTime().Equal(since) || got.GetTimezone() != "UTC" {
		t.Errorf("window starts %s in %q, want the stated %s in UTC", got.GetWindowSince().AsTime(),
			got.GetTimezone(), since)
	}
}

// TestGetStatsSeries_RefusesWhatIsNotASeries: every refusal is
// InvalidArgument and names what it refused.
//
// Mutations it catches: the bucket limit not enforced, enforced on the count
// only, or off by one; a timezone or a count accepted with no width (the
// field silently ignored); bucket_count and since both accepted; "Local" read
// as the server's zone; an undeclared width read as DAY; an instant past year
// 9999, or a window reaching before year 1, computed instead of refused.
func TestGetStatsSeries_RefusesWhatIsNotASeries(t *testing.T) {
	now := at(t, "2026-11-01T12:00:00Z")
	for _, tc := range []struct {
		name string
		req  *orkstackv1.GetStatsRequest
		want string
	}{
		{"a timezone with no width", &orkstackv1.GetStatsRequest{Timezone: "UTC"}, "timezone"},
		{"a count with no width", &orkstackv1.GetStatsRequest{BucketCount: 7}, "bucket_count"},
		{"an undeclared width", &orkstackv1.GetStatsRequest{BucketWidth: 7}, "bucket_width"},
		{"a negative count", &orkstackv1.GetStatsRequest{BucketWidth: dayBuckets, BucketCount: -1}, "negative"},
		{"a count over the limit", &orkstackv1.GetStatsRequest{BucketWidth: dayBuckets, BucketCount: 1001}, "1000"},
		{"a count with since", &orkstackv1.GetStatsRequest{
			BucketWidth: dayBuckets, BucketCount: 7, Since: timestamppb.New(now.Add(-time.Hour)),
		}, "since"},
		{"a count with lookback", &orkstackv1.GetStatsRequest{
			BucketWidth: dayBuckets, BucketCount: 7, Lookback: durationpb.New(time.Hour),
		}, "lookback"},
		{"a zone the database does not hold", &orkstackv1.GetStatsRequest{
			BucketWidth: dayBuckets, Timezone: "Mars/Olympus_Mons",
		}, "Mars/Olympus_Mons"},
		{"the reading machine's own zone", &orkstackv1.GetStatsRequest{
			BucketWidth: dayBuckets, Timezone: "Local",
		}, "Local"},
		{"a window one bucket over what an answer carries", &orkstackv1.GetStatsRequest{
			BucketWidth: hourBuckets, Since: timestamppb.New(now.Add(-1001 * time.Hour)),
		}, "more than the 1000 hour buckets"},
		{"a window with no time in it", &orkstackv1.GetStatsRequest{
			BucketWidth: dayBuckets, Since: timestamppb.New(now), Until: timestamppb.New(now),
		}, "ends at or before it starts"},
		{"an until past year 9999", &orkstackv1.GetStatsRequest{
			BucketWidth: dayBuckets, Until: &timestamppb.Timestamp{Seconds: 253402300800},
		}, "timestamp"},
		{"a window starting at the zero time", &orkstackv1.GetStatsRequest{
			BucketWidth: dayBuckets, Since: timestamppb.New(time.Time{}),
			Until: timestamppb.New(time.Date(1, 1, 6, 0, 0, 0, 0, time.UTC)),
		}, "year 1"},
		{"a count reaching before year 1", &orkstackv1.GetStatsRequest{
			BucketWidth: dayBuckets, BucketCount: 7,
			Until: timestamppb.New(time.Date(1, 1, 3, 0, 0, 0, 0, time.UTC)),
		}, "year 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newStatsFixture(t, now, "payments")
			tc.req.Namespace = "payments"
			_, err := api.NewStatsHandler(fixture.svc, 0).GetStats(context.Background(), connect.NewRequest(tc.req))
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("err = %v (%s), want InvalidArgument", err, connect.CodeOf(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %q does not name %q", err, tc.want)
			}
		})
	}
	// The limit is a limit, not an off-by-one: exactly 1000 buckets answer,
	// counted back or cut from a stated window.
	fixture := newStatsFixture(t, now, "payments")
	handler := api.NewStatsHandler(fixture.svc, 0)
	for _, req := range []*orkstackv1.GetStatsRequest{
		{Namespace: "payments", BucketWidth: hourBuckets, BucketCount: 1000},
		{Namespace: "payments", BucketWidth: hourBuckets, Since: timestamppb.New(now.Add(-1000 * time.Hour)),
			Until: timestamppb.New(now)},
	} {
		if got := getStats(t, handler, req); len(got.GetBuckets()) != 1000 {
			t.Errorf("%v answered %d buckets, want 1000", req, len(got.GetBuckets()))
		}
	}
}

// TestGetStatsSeries_WithoutASeriesTheAnswerIsUnchanged: a request that names
// no width gets no series and none of its echo fields, on the wire too.
//
// Mutation it catches: a series, or an echo of one, attached to every answer,
// which would change what every existing client reads.
func TestGetStatsSeries_WithoutASeriesTheAnswerIsUnchanged(t *testing.T) {
	now := at(t, "2026-11-01T12:00:00Z")
	fixture := newStatsFixture(t, now, "payments")
	fixture.archiveRun(t, "payments", "charge", orkstackv1.Status_STATUS_SUCCEEDED, now.Add(-time.Hour))
	got := getStats(t, api.NewStatsHandler(fixture.svc, 0), &orkstackv1.GetStatsRequest{Namespace: "payments"})
	wire, err := api.MarshalJSON(got)
	must.OKf(t, err, "marshal")
	for _, key := range []string{`"buckets"`, `"bucketWidth"`, `"timezone"`} {
		if strings.Contains(string(wire), key) {
			t.Errorf("the wire form carries %s though no series was asked for: %s", key, wire)
		}
	}
	if got.GetRunsByStatus()["succeeded"] != 1 {
		t.Errorf("the window lost its own count: %v", got.GetRunsByStatus())
	}
}

// TestGetStatsSeries_TheWidthTravelsByItsShortName: a client's JSON spells
// the width the way docs/JSON.md spells enums, and the answer spells it back.
//
// Mutation it catches: StatsBucketWidth's values declared without the enum's
// own prefix (DAY rather than STATS_BUCKET_WIDTH_DAY), which the JSON
// dialect's short names are cut from; "day" would no longer decode.
func TestGetStatsSeries_TheWidthTravelsByItsShortName(t *testing.T) {
	for _, spelling := range []string{"day", "DAY", "STATS_BUCKET_WIDTH_DAY"} {
		req := &orkstackv1.GetStatsRequest{}
		body := `{"bucketWidth":"` + spelling + `","timezone":"Europe/Lisbon","bucketCount":7}`
		must.OKf(t, api.UnmarshalJSON([]byte(body), req), "decode %s", body)
		if req.GetBucketWidth() != dayBuckets || req.GetBucketCount() != 7 || req.GetTimezone() != "Europe/Lisbon" {
			t.Fatalf("decode %s = %v", body, req)
		}
	}
	wire, err := api.MarshalJSON(&orkstackv1.GetStatsResponse{BucketWidth: dayBuckets})
	must.OKf(t, err, "marshal")
	if !strings.Contains(string(wire), `"bucketWidth":"DAY"`) {
		t.Fatalf("the answer spells the width %s, want \"DAY\"", wire)
	}
}

// TestGetStatsSeries_TheSeriesIsPartOfTheCacheKey: one window asked with no
// series, by UTC day, by UTC hour, by Los Angeles day and by count is five
// questions, and a warm cache answers each with its own answer.
//
// Mutation it catches: a cache key without the width, the zone or the count
// (a warm cache would hand an earlier question's answer to a later one).
func TestGetStatsSeries_TheSeriesIsPartOfTheCacheKey(t *testing.T) {
	now := at(t, "2026-11-01T12:00:00Z")
	fixture := newStatsFixture(t, now, "payments")
	fixture.archiveRun(t, "payments", "charge", orkstackv1.Status_STATUS_SUCCEEDED,
		at(t, "2026-11-01T02:00:00Z")) // Oct 31 in Los Angeles, Nov 1 in UTC
	// Oct 31 00:00 UTC to Nov 1 12:00 UTC: two UTC days, 36 UTC hours, and
	// three Los Angeles days (Oct 30 from 17:00, Oct 31, Nov 1 to 04:00).
	since := timestamppb.New(at(t, "2026-10-31T00:00:00Z"))
	until := timestamppb.New(now)
	cached := api.NewStatsHandler(fixture.svc, time.Minute)
	for _, tc := range []struct {
		name    string
		req     *orkstackv1.GetStatsRequest
		buckets int
		runIn   string // the start of the one bucket that holds the run
	}{
		{"no series", &orkstackv1.GetStatsRequest{Namespace: "payments", Since: since, Until: until}, 0, ""},
		{"by UTC day", &orkstackv1.GetStatsRequest{Namespace: "payments", Since: since, Until: until,
			BucketWidth: dayBuckets}, 2, "2026-11-01T00:00:00Z"},
		{"by UTC hour", &orkstackv1.GetStatsRequest{Namespace: "payments", Since: since, Until: until,
			BucketWidth: hourBuckets}, 36, "2026-11-01T02:00:00Z"},
		{"by Los Angeles day", &orkstackv1.GetStatsRequest{Namespace: "payments", Since: since, Until: until,
			BucketWidth: dayBuckets, Timezone: "America/Los_Angeles"}, 3, "2026-10-31T00:00:00-07:00"},
		{"the last two days in Los Angeles", &orkstackv1.GetStatsRequest{Namespace: "payments",
			BucketWidth: dayBuckets, Timezone: "America/Los_Angeles", BucketCount: 2}, 2, "2026-10-31T00:00:00-07:00"},
		{"the last three days in Los Angeles", &orkstackv1.GetStatsRequest{Namespace: "payments",
			BucketWidth: dayBuckets, Timezone: "America/Los_Angeles", BucketCount: 3}, 3, "2026-10-31T00:00:00-07:00"},
	} {
		// Asked twice, the second time from a warm cache that holds every
		// earlier question's answer too.
		for round := 0; round < 2; round++ {
			got := getStats(t, cached, tc.req)
			if len(got.GetBuckets()) != tc.buckets {
				t.Fatalf("%s (round %d): %d buckets, want %d", tc.name, round, len(got.GetBuckets()), tc.buckets)
			}
			for _, b := range got.GetBuckets() {
				holds := b.GetRunsByStatus()["succeeded"] == 1
				if want := b.GetSince().AsTime().Equal(at(t, tc.runIn)); holds != want {
					t.Errorf("%s (round %d): bucket %s counts %v; the run belongs only in %s",
						tc.name, round, b.GetSince().AsTime(), b.GetRunsByStatus(), tc.runIn)
				}
			}
		}
	}
}

// TestGetStatsSeries_ARefusedSeriesNeverCollectsACachedAnswer: the request is
// validated before the cache is consulted. One request, sent twice: 999
// hours back from now is a series an answer carries; two hours later, with
// the answer still cached, the same request reaches 1001 hours and is
// refused.
//
// Mutation it catches: a lookup before validation, which would hand the
// second request the first one's cached series.
func TestGetStatsSeries_ARefusedSeriesNeverCollectsACachedAnswer(t *testing.T) {
	now := at(t, "2026-11-01T12:00:00Z")
	fixture := newStatsFixture(t, now, "payments")
	handler := api.NewStatsHandler(fixture.svc, 3*time.Hour)
	req := &orkstackv1.GetStatsRequest{
		Namespace: "payments", BucketWidth: hourBuckets, Since: timestamppb.New(now.Add(-999 * time.Hour)),
	}
	if got := getStats(t, handler, req); len(got.GetBuckets()) != 999 {
		t.Fatalf("the first request answered %d buckets, want 999", len(got.GetBuckets()))
	}
	fixture.clock.advance(2 * time.Hour)
	_, err := handler.GetStats(context.Background(), connect.NewRequest(req))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "more than the 1000") {
		t.Fatalf("err = %v, want InvalidArgument however warm the cache is", err)
	}
}

// TestGetStatsSeries_AnUnplaceableLiveRunFailsTheAnswer: a live run that no
// bucket can hold and that was not created before the window — no creation
// time, or one at or after the window's end — fails the series, saying what
// still answers, rather than vanishing from every bar.
//
// Mutation it catches: an unplaceable live run skipped, which would draw bars
// one run short of the window's running count with nothing saying so, or
// reported by orkctl as a run created before the window.
func TestGetStatsSeries_AnUnplaceableLiveRunFailsTheAnswer(t *testing.T) {
	now := at(t, "2026-11-01T12:00:00Z")
	noTime := liveRun("ork-unplaced", "payments", "charge", now)
	noTime.CreatedAt = nil
	for _, run := range []*orkstackv1.Run{
		noTime,
		liveRun("ork-unplaced", "payments", "charge", now), // the window's own end, which it excludes
		liveRun("ork-unplaced", "payments", "charge", now.Add(time.Minute)),
	} {
		fixture := newStatsFixture(t, now, "payments")
		fixture.reader.executions["payments"] = engine.Executions{Running: []*orkstackv1.Run{run}}
		handler := api.NewStatsHandler(fixture.svc, 0)
		_, err := handler.GetStats(context.Background(), connect.NewRequest(&orkstackv1.GetStatsRequest{
			Namespace: "payments", BucketWidth: dayBuckets, Until: timestamppb.New(now),
		}))
		if connect.CodeOf(err) != connect.CodeInternal || !strings.Contains(err.Error(), "ork-unplaced") ||
			!strings.Contains(err.Error(), "ask for the window's totals alone") {
			t.Fatalf("created %v: err = %v, want Internal naming the run and what still answers",
				run.GetCreatedAt(), err)
		}
		// Without a series nothing has to be placed, and the window answers.
		if got := getStats(t, handler, &orkstackv1.GetStatsRequest{Namespace: "payments"}); got.GetRunsByStatus()["running"] != 1 {
			t.Fatalf("the window counts %v, want the live run", got.GetRunsByStatus())
		}
	}
}

// growingArchive is a real archive that gains a run inside the window after
// every listing, as a busy namespace's archive does between two reads.
type growingArchive struct {
	artifacts.RunArchive
	at    time.Time
	mu    sync.Mutex
	added int
}

func (g *growingArchive) ListRuns(ctx context.Context, q artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	entries, err := g.RunArchive.ListRuns(ctx, q)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.added++
	run := &orkstackv1.Run{
		Id: fmt.Sprintf("ork-later-%d", g.added), Namespace: "payments", OrkflowName: "charge",
		Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: timestamppb.New(g.at),
	}
	if err := engine.NewRunArchiver(g.RunArchive).Archive(ctx, run, nil); err != nil {
		return nil, err
	}
	return entries, nil
}

// TestGetStatsSeries_TheBarsAndTheTotalsAreOneReading: the totals and the
// series come from one listing of the archive, so they agree even when the
// archive grows between two reads.
//
// Mutation it catches: the series tallied by a second scan, or one scan per
// bucket — the bars would count runs the totals beside them do not.
func TestGetStatsSeries_TheBarsAndTheTotalsAreOneReading(t *testing.T) {
	now := at(t, "2026-11-01T12:00:00Z")
	fixture := newStatsFixtureOver(t, now, func(inner artifacts.RunArchive) artifacts.RunArchive {
		return &growingArchive{RunArchive: inner, at: now.Add(-time.Hour)}
	}, "payments")
	fixture.archiveRun(t, "payments", "charge", orkstackv1.Status_STATUS_SUCCEEDED, now.Add(-2*time.Hour))
	got := getStats(t, api.NewStatsHandler(fixture.svc, 0), &orkstackv1.GetStatsRequest{
		Namespace: "payments", BucketWidth: hourBuckets, BucketCount: 6,
	})
	var bars int64
	for _, b := range got.GetBuckets() {
		bars += b.GetRunsByStatus()["succeeded"]
	}
	if total := got.GetRunsByStatus()["succeeded"]; bars != 1 || total != 1 {
		t.Fatalf("the bars count %d succeeded runs and the totals %d, want 1 and 1: one reading", bars, total)
	}
}

// looseArchive answers a listing with every run it holds, whatever window it
// was asked for: a store that answers loosely.
type looseArchive struct{ artifacts.RunArchive }

func (l looseArchive) ListRuns(ctx context.Context, q artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	q.Since, q.Until = time.Time{}, time.Time{}
	return l.RunArchive.ListRuns(ctx, q)
}

// TestGetStatsSeries_AStoreThatAnswersLooselyFailsTheSeries: a run outside the
// window it was asked for cannot be placed in a bucket, and the answer says
// so as this engine's fault rather than drawing bars that do not add up.
//
// Mutation it catches: an out-of-range entry dropped from the bars (or
// clamped into the first or last) while the totals count it.
func TestGetStatsSeries_AStoreThatAnswersLooselyFailsTheSeries(t *testing.T) {
	now := at(t, "2026-11-01T12:00:00Z")
	fixture := newStatsFixtureOver(t, now, func(inner artifacts.RunArchive) artifacts.RunArchive {
		return looseArchive{RunArchive: inner}
	}, "payments")
	fixture.archiveRun(t, "payments", "charge", orkstackv1.Status_STATUS_SUCCEEDED, now.Add(-30*24*time.Hour))
	_, err := api.NewStatsHandler(fixture.svc, 0).GetStats(context.Background(),
		connect.NewRequest(&orkstackv1.GetStatsRequest{Namespace: "payments", BucketWidth: dayBuckets}))
	if connect.CodeOf(err) != connect.CodeInternal || !strings.Contains(err.Error(), "outside the window") {
		t.Fatalf("err = %v, want Internal saying the run is outside the window", err)
	}
}

// windowOnlyArchive is an archive that can tally a window but not a series:
// an archive written before the series existed.
type windowOnlyArchive struct {
	api.RunArchive
	tallier api.RunIndexTallier
}

func (w windowOnlyArchive) TallyRuns(ctx context.Context, q engine.ArchiveQuery) (engine.RunTally, error) {
	return w.tallier.TallyRuns(ctx, q)
}

// failingArchive is a store whose index cannot be listed.
type failingArchive struct{ artifacts.RunArchive }

func (failingArchive) ListRuns(context.Context, artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	return nil, errors.New("the bucket is unreachable")
}

// TestGetStatsSeries_EveryWayTheArchiveFailsIsNamed: each way the archive
// half of a series can fail reaches the caller with its own code — never a
// series of zeros, which reads like a quiet week.
//
// Mutations it catches: a missing series seam answered as "no archive" or as
// empty buckets; a store outage reported as the engine's fault (Internal) or
// as a refusal of the request; no archive at all answered with zeros.
func TestGetStatsSeries_EveryWayTheArchiveFailsIsNamed(t *testing.T) {
	now := at(t, "2026-11-01T12:00:00Z")
	series := &orkstackv1.GetStatsRequest{Namespace: "payments", BucketWidth: dayBuckets}
	window := &orkstackv1.GetStatsRequest{Namespace: "payments"}
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "open local run archive")
	t.Cleanup(func() { _ = store.Close() })
	serviceOver := func(archive api.RunArchive) *api.StatsHandler {
		return api.NewStatsHandler(api.NewService(api.Config{
			Reader:     &fakeReader{executions: map[string]engine.Executions{}},
			Runs:       engine.NewRunStore(),
			Archive:    archive,
			Namespaces: registry{"default": {Name: "default"}, "payments": {Name: "payments"}},
			Starter:    &fakeStarter{},
			Now:        func() time.Time { return now },
		}), 0)
	}
	archiver := engine.NewRunArchiver(store)
	for _, tc := range []struct {
		name         string
		handler      *api.StatsHandler
		code         connect.Code
		says         string
		windowAnswer bool
	}{
		{"an archive that cannot count by bucket", serviceOver(windowOnlyArchive{RunArchive: archiver, tallier: archiver}),
			connect.CodeFailedPrecondition, "cannot count finished runs by bucket", true},
		{"an archive whose store cannot be listed", serviceOver(engine.NewRunArchiver(failingArchive{store})),
			connect.CodeUnavailable, "the bucket is unreachable", false},
		{"no archive at all", serviceOver((*engine.RunArchiver)(nil)),
			connect.CodeFailedPrecondition, "ORK_ARTIFACT_BACKEND", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.handler.GetStats(context.Background(), connect.NewRequest(series))
			if connect.CodeOf(err) != tc.code || !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("err = %v (%s), want %s saying %q", err, connect.CodeOf(err), tc.code, tc.says)
			}
			_, err = tc.handler.GetStats(context.Background(), connect.NewRequest(window))
			if tc.windowAnswer && err != nil {
				t.Fatalf("the same engine refused the window alone: %v", err)
			}
		})
	}
}

// fuzzZones are the zones the fuzz target picks among by index, so most
// inputs exercise a real zone's rules instead of a refusal. An index past the
// list sends the raw string instead.
var fuzzZones = []string{
	"", "UTC", "America/Los_Angeles", "America/Sao_Paulo", "America/Havana", "America/Santiago",
	"Pacific/Apia", "Australia/Lord_Howe", "Asia/Kolkata", "Asia/Kathmandu", "Asia/Beirut",
	"Antarctica/Troll", "Europe/Lisbon",
}

// wallLabel is the fuzz oracle: what a bucket is called on the zone's wall
// clock, read with time.Time.In and nothing of the code under test.
func wallLabel(t time.Time, zone *time.Location, width orkstackv1.StatsBucketWidth) string {
	if width == hourBuckets {
		return t.In(zone).Format("2006-01-02 15")
	}
	return t.In(zone).Format("2006-01-02")
}

// FuzzGetStatsSeries: whatever a client sends, GetStats with a series either
// refuses with InvalidArgument — never Internal, which would be this engine's
// fault — or answers buckets that tile the window, number at most 1000 and
// exactly bucket_count when one was sent, each keep one wall-clock label from
// end to end and change it at every edge, and add up, status by finished
// status, to the window's own counts.
func FuzzGetStatsSeries(f *testing.F) {
	now := time.Date(2026, 11, 1, 3, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(f, now, "payments")
	for i, created := range []time.Time{
		now.Add(-time.Hour), now.Add(-25 * time.Hour), now.Add(-7 * 24 * time.Hour), now.Add(-90 * 24 * time.Hour),
	} {
		run := &orkstackv1.Run{
			Id: fmt.Sprintf("ork-fuzz-%d", i), Namespace: "payments", OrkflowName: "charge",
			Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: timestamppb.New(created),
		}
		must.OKf(f, engine.NewRunArchiver(fixture.archive).Archive(context.Background(), run, nil), "archive")
	}
	handler := api.NewStatsHandler(fixture.svc, 0)
	for _, seed := range []struct {
		since, until, lookback int64
		set                    uint8
		width, count           int32
		zone                   uint8
		raw                    string
	}{
		{0, 0, 0, 0, 2, 7, 2, ""},
		{0, 0, 0, 0, 1, 24, 7, ""},
		{0, 0, 0, 0, 2, 1000, 1, ""},
		{0, 0, 0, 0, 1, 1000, 6, ""},
		{now.Unix() - 86400*3, now.Unix(), 0, 3, 2, 0, 3, ""},
		{now.Unix() - 86400*3, now.Unix(), 0, 3, 1, 0, 9, ""},
		{-315619200, -315532800, 0, 3, 1, 0, 1, ""},    // 1960
		{0, -62135596800 + 86400*2, 0, 2, 2, 7, 1, ""}, // year 1
		{0, 0, 3600 * 30, 4, 1, 0, 4, ""},
		{0, 0, 0, 0, 2, 7, 255, "Local"},
		{0, 0, 0, 0, 2, 7, 255, "Nope/Zone"},
		{0, 0, 0, 0, 0, 7, 0, ""},
		{0, 0, 0, 0, 9, 0, 0, ""},
		{now.Unix() - 3600*2000, now.Unix(), 0, 3, 1, 0, 0, ""},
		{0, 2240049600, 0, 2, 2, 7, 2, ""},   // 2040-12-31T12:00Z, Los Angeles, by day
		{0, 2240049600, 0, 2, 1, 24, 12, ""}, // the same, Lisbon, by hour
	} {
		f.Add(seed.since, seed.until, seed.lookback, seed.set, seed.width, seed.count, seed.zone, seed.raw)
	}
	f.Fuzz(func(t *testing.T, since, until, lookback int64, set uint8, width, count int32, zoneIndex uint8, raw string) {
		req := &orkstackv1.GetStatsRequest{
			Namespace:   "payments",
			BucketWidth: orkstackv1.StatsBucketWidth(width),
			BucketCount: count,
			Timezone:    raw,
		}
		if int(zoneIndex) < len(fuzzZones) {
			req.Timezone = fuzzZones[zoneIndex]
		}
		if set&1 != 0 {
			req.Since = &timestamppb.Timestamp{Seconds: since}
		}
		if set&2 != 0 {
			req.Until = &timestamppb.Timestamp{Seconds: until}
		}
		if set&4 != 0 {
			req.Lookback = &durationpb.Duration{Seconds: lookback}
		}
		resp, err := handler.GetStats(context.Background(), connect.NewRequest(req))
		if err != nil {
			var connectErr *connect.Error
			if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeInvalidArgument {
				t.Fatalf("request %v refused with %v (%s); a request is never this engine's fault",
					req, err, connect.CodeOf(err))
			}
			return
		}
		got := resp.Msg
		if req.GetBucketWidth() == orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_UNSPECIFIED {
			if len(got.GetBuckets()) != 0 {
				t.Fatalf("no width asked for and %d buckets answered", len(got.GetBuckets()))
			}
			return
		}
		buckets := got.GetBuckets()
		if len(buckets) == 0 || len(buckets) > 1000 {
			t.Fatalf("%d buckets answered", len(buckets))
		}
		if count > 0 && len(buckets) != int(count) {
			t.Fatalf("bucket_count %d answered %d buckets", count, len(buckets))
		}
		zone, err := time.LoadLocation(got.GetTimezone())
		must.OKf(t, err, "the echoed zone %q does not load", got.GetTimezone())
		if !buckets[0].GetSince().AsTime().Equal(got.GetWindowSince().AsTime()) ||
			!buckets[len(buckets)-1].GetUntil().AsTime().Equal(got.GetWindowUntil().AsTime()) {
			t.Fatalf("the buckets do not span the window %s..%s",
				got.GetWindowSince().AsTime(), got.GetWindowUntil().AsTime())
		}
		drawn := got.GetBucketWidth()
		sums := map[string]int64{}
		for i, b := range buckets {
			start, end := b.GetSince().AsTime(), b.GetUntil().AsTime()
			if !end.After(start) {
				t.Fatalf("bucket %d is empty or backwards: %s..%s", i, start, end)
			}
			if i > 0 && !start.Equal(buckets[i-1].GetUntil().AsTime()) {
				t.Fatalf("bucket %d starts %s, not where bucket %d ended", i, start, i-1)
			}
			if first, last := wallLabel(start, zone, drawn), wallLabel(end.Add(-time.Nanosecond), zone, drawn); first != last {
				t.Fatalf("bucket %s..%s holds two labels, %s and %s", start, end, first, last)
			}
			if i > 0 && wallLabel(start.Add(-time.Nanosecond), zone, drawn) == wallLabel(start, zone, drawn) {
				t.Fatalf("the edge %s is not where the label changes in %s", start, zone)
			}
			for status, n := range b.GetRunsByStatus() {
				sums[status] += n
			}
		}
		for status, n := range got.GetRunsByStatus() {
			if status != "running" && sums[status] != n {
				t.Fatalf("the buckets count %d %s, the window %d", sums[status], status, n)
			}
		}
	})
}
