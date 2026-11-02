package api

import (
	"fmt"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/schedules"
)

// StatsMaxBuckets bounds a series: a year of days and a month of hours both
// fit. A request whose window would cut into more is refused, naming both
// numbers, rather than answered with the first thousand — a series that
// stopped early reads exactly like one whose last buckets were empty.
const StatsMaxBuckets = 1000

// seriesWalkLimit bounds one boundary search. A real zone keeps a label
// across at most one or two of its transitions in a row; the bound exists so
// a zone database that never changes the label cannot loop the handler.
const seriesWalkLimit = 64

// statsSeries is a request's series, resolved: the width, the zone that
// draws it, and the edges that cut the window — bucket i is
// [edges[i], edges[i+1]), and the edges span the window exactly.
type statsSeries struct {
	width    orkstackv1.StatsBucketWidth
	zone     *time.Location
	zoneName string
	edges    []time.Time
}

// resolveStats resolves and VALIDATES a GetStats request's window and its
// series against one clock, this server's, before anything is read or looked
// up. The series is nil when the request asked for none, and the window is
// then exactly what windowOf resolves.
func (h *StatsHandler) resolveStats(
	namespace string,
	req *orkstackv1.GetStatsRequest,
) (engine.StatsWindow, *statsSeries, error) {
	width, zone, err := seriesOf(req)
	if err != nil {
		return engine.StatsWindow{}, nil, err
	}
	if width == orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_UNSPECIFIED {
		window, err := h.windowOf(namespace, req.GetSince(), req.GetUntil(), req.GetLookback())
		return window, nil, err
	}
	window, err := h.seriesWindow(namespace, req, width, zone)
	if err != nil {
		return engine.StatsWindow{}, nil, err
	}
	edges, err := seriesEdges(window.Since, window.Until, zone, width)
	if err != nil {
		return engine.StatsWindow{}, nil, err
	}
	return window, &statsSeries{width: width, zone: zone, zoneName: zone.String(), edges: edges}, nil
}

// seriesOf validates the series fields of a request, alone and against each
// other, and loads the zone. UNSPECIFIED with a nil zone means no series was
// asked for, and then neither of the other series fields may be set: a field
// that changes nothing is a mistake its sender should hear about.
func seriesOf(req *orkstackv1.GetStatsRequest) (orkstackv1.StatsBucketWidth, *time.Location, error) {
	width := req.GetBucketWidth()
	switch width {
	case orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_UNSPECIFIED:
		if req.GetTimezone() != "" {
			return width, nil, invalidSeries(
				"timezone %q is set but bucket_width is not; a timezone draws a series' "+
					"buckets, and no series was asked for", req.GetTimezone())
		}
		if req.GetBucketCount() != 0 {
			return width, nil, invalidSeries(
				"bucket_count %d is set but bucket_width is not; name the width (HOUR or "+
					"DAY) the count is of", req.GetBucketCount())
		}
		return width, nil, nil
	case orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR,
		orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_DAY:
	default:
		return width, nil, invalidSeries("bucket_width %d is not a width; HOUR and DAY are", int32(width))
	}
	switch count := req.GetBucketCount(); {
	case count < 0:
		return width, nil, invalidSeries(
			"bucket_count %d is negative; it counts buckets back from the window's end", count)
	case count > StatsMaxBuckets:
		return width, nil, invalidSeries(
			"bucket_count %d is more than the %d buckets one answer carries; ask for fewer, "+
				"or for a wider bucket_width", count, StatsMaxBuckets)
	case count > 0 && (req.GetSince() != nil || req.GetLookback() != nil):
		return width, nil, invalidSeries(
			"bucket_count says where the window starts, and so does %s; send one of them",
			sinceOrLookback(req))
	}
	// A bound outside the years a timestamp holds is refused before any
	// arithmetic is done with it: the boundary walk works in Unix seconds,
	// and an instant beyond year 9999 is beyond what an answer can carry.
	for _, bound := range []struct {
		name string
		ts   *timestamppb.Timestamp
	}{{"since", req.GetSince()}, {"until", req.GetUntil()}} {
		if bound.ts != nil {
			if err := bound.ts.CheckValid(); err != nil {
				return width, nil, invalidSeries(
					"%s is not an instant a timestamp holds (years 1 to 9999): %v", bound.name, err)
			}
		}
	}
	zoneName := req.GetTimezone()
	if zoneName == "" {
		zoneName = "UTC"
	}
	// The schedule's rule, so the API's two zone fields cannot drift apart:
	// a name of the IANA database, never "Local".
	zone, err := schedules.LoadTimezone(zoneName)
	if err != nil {
		return width, nil, invalidSeries("timezone %q: %v", req.GetTimezone(), err)
	}
	return width, zone, nil
}

// seriesWindow is the window a series cuts. With bucket_count it is the last
// N whole buckets before until, so the window-level numbers describe exactly
// the span the buckets tile; otherwise it is resolved as always, and the
// series only cuts it, clipping its first and last buckets.
//
// Every edge travels as a timestamp, which holds years 1 to 9999 only, so a
// window reaching past either end is refused rather than computed and then
// failing to encode: an answer that cannot be sent is not one. The first
// instant of year 1 is refused too, because it is the zero time, which the
// archive's query reads as "no lower bound" and the execution reader as
// "unset".
func (h *StatsHandler) seriesWindow(
	namespace string,
	req *orkstackv1.GetStatsRequest,
	width orkstackv1.StatsBucketWidth,
	zone *time.Location,
) (engine.StatsWindow, error) {
	var window engine.StatsWindow
	if count := req.GetBucketCount(); count > 0 {
		until := h.now()
		if req.GetUntil() != nil {
			until = req.GetUntil().AsTime().UTC()
		}
		since, err := lastBucketsStart(until, int(count), zone, width)
		if err != nil {
			return engine.StatsWindow{}, err
		}
		window = engine.StatsWindow{Namespace: namespace, Since: since, Until: until}
	} else {
		var err error
		window, err = h.windowOf(namespace, req.GetSince(), req.GetUntil(), req.GetLookback())
		if err != nil {
			return engine.StatsWindow{}, err
		}
	}
	for _, bound := range []time.Time{window.Since, window.Until} {
		if err := timestamppb.New(bound).CheckValid(); err != nil || !bound.After(time.Time{}) {
			return engine.StatsWindow{}, invalidSeries(
				"the window bound %s is outside the years a timestamp holds (after the "+
					"first instant of year 1, before year 10000)", bound.Format(time.RFC3339Nano))
		}
	}
	return window, nil
}

// sinceOrLookback names the field a bucket_count collided with.
func sinceOrLookback(req *orkstackv1.GetStatsRequest) string {
	if req.GetSince() != nil {
		return "since"
	}
	return "lookback"
}

// seriesFault is a series this engine could not draw: never the request's
// doing, so it is Internal, and it says what still answers.
func seriesFault(err error) error {
	return connect.NewError(connect.CodeInternal, fmt.Errorf(
		"%w; this is a fault of the engine, not of the request: ask for the window's totals "+
			"alone, without a series", err))
}

func invalidSeries(format string, args ...any) error {
	return connect.NewError(connect.CodeInvalidArgument,
		fmt.Errorf("stats series: "+format, args...))
}

// seriesEdges cuts [since, until) at every bucket boundary inside it. The
// first edge is since and the last is until, so the buckets tile the window
// exactly and the first and last may be partial. The walk stops one bucket
// past the limit, so refusing a window that reaches back decades costs a
// thousand steps, not a decade's.
func seriesEdges(since, until time.Time, zone *time.Location, width orkstackv1.StatsBucketWidth) ([]time.Time, error) {
	edges := []time.Time{since}
	for at := since; ; {
		next, err := nextBoundary(at, zone, width)
		if err != nil {
			return nil, seriesFault(err)
		}
		if !next.Before(until) {
			break
		}
		if len(edges) == StatsMaxBuckets {
			return nil, invalidSeries(
				"the window %s to %s holds more than the %d %s buckets one answer carries; "+
					"narrow the window or use a wider bucket",
				since.Format(time.RFC3339), until.Format(time.RFC3339), StatsMaxBuckets, widthWord(width))
		}
		edges = append(edges, next)
		at = next
	}
	return append(edges, until), nil
}

// lastBucketsStart is where the last n whole buckets before until begin: the
// n-th boundary back from the bucket that holds until's last instant. The
// walk runs forward from well before it, because the boundary walk is defined
// forward only; it starts (n+1) of the longest label a real zone keeps
// before until, and finding fewer than n boundaries there is an error, never
// a shorter series.
func lastBucketsStart(until time.Time, n int, zone *time.Location, width orkstackv1.StatsBucketWidth) (time.Time, error) {
	longest := 48 * time.Hour
	if width == orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR {
		longest = 3 * time.Hour
	}
	var boundaries []time.Time
	for at := until.Add(-time.Duration(n+1) * longest); ; {
		next, err := nextBoundary(at, zone, width)
		if err != nil {
			return time.Time{}, seriesFault(err)
		}
		if !next.Before(until) {
			break
		}
		boundaries = append(boundaries, next)
		at = next
	}
	if len(boundaries) < n {
		return time.Time{}, seriesFault(fmt.Errorf("stats series: found %d %s boundaries before %s in %s, want %d",
			len(boundaries), widthWord(width), until.Format(time.RFC3339), zone, n))
	}
	return boundaries[len(boundaries)-n], nil
}

// bucketLabel is what a bucket is called on the zone's wall clock: the local
// date for DAY, the local date and hour for HOUR. A bucket is one stretch of
// time with one label, and a boundary is where the label changes.
type bucketLabel struct {
	year, month, day, hour int
}

func labelOf(t time.Time, zone *time.Location, width orkstackv1.StatsBucketWidth) bucketLabel {
	local := t.In(zone)
	year, month, day := local.Date()
	label := bucketLabel{year: year, month: int(month), day: day, hour: -1}
	if width == orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR {
		label.hour = local.Hour()
	}
	return label
}

// nextBoundary is the first instant strictly after t at which the bucket
// label changes.
//
// Inside one offset period the zone's wall clock runs continuously, so the
// label holds until the next wall-clock unit boundary — local midnight, or
// the top of the local hour — computed from t's own offset. The period may
// end first, at a transition. A transition that changes the label is a
// boundary (a skipped midnight, a skipped date, a 30-minute shift that moves
// the hour); one that keeps it is not (the hour a fall-back repeats, the
// midnight a fall-back repeats), and the walk goes on from it. Every boundary
// is therefore a whole second.
func nextBoundary(t time.Time, zone *time.Location, width orkstackv1.StatsBucketWidth) (time.Time, error) {
	unit := int64(24 * 60 * 60)
	if width == orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR {
		unit = 60 * 60
	}
	from := labelOf(t, zone, width)
	at := t
	for range seriesWalkLimit {
		local := at.In(zone)
		_, offset := local.Zone()
		wall := at.Unix() + int64(offset)
		candidate := time.Unix((floorDiv(wall, unit)+1)*unit-int64(offset), 0).UTC()
		// The period's end bounds the step only when it lies ahead. Past the
		// zone database's last listed transition Go extends the zone's rule,
		// and there ZoneBounds can report an end at or before the instant it
		// was asked about (a leap year's Dec 31, from about 2037 on); taken as
		// the step, that end would stall the walk.
		if _, end := local.ZoneBounds(); end.After(at) && !end.After(candidate) {
			candidate = end.UTC()
		}
		// The walk must move forward. A candidate at or before the instant it
		// started from would loop a caller without end; it is a fault of the
		// zone handling, said as one, never walked.
		if !candidate.After(at) {
			return time.Time{}, fmt.Errorf("stats series: the %s boundary walk in %s did not advance past %s",
				widthWord(width), zone, at.Format(time.RFC3339Nano))
		}
		if labelOf(candidate, zone, width) != from {
			return candidate, nil
		}
		at = candidate
	}
	return time.Time{}, fmt.Errorf("stats series: no %s boundary in %s within %d steps after %s",
		widthWord(width), zone, seriesWalkLimit, t.Format(time.RFC3339Nano))
}

// isBoundary reports whether t starts a bucket: the label changes at t.
func isBoundary(t time.Time, zone *time.Location, width orkstackv1.StatsBucketWidth) bool {
	return labelOf(t.Add(-time.Nanosecond), zone, width) != labelOf(t, zone, width)
}

// floorDiv is integer division rounding toward negative infinity, so an
// instant before 1970 floors to the unit boundary before it, not after it.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func widthWord(width orkstackv1.StatsBucketWidth) string {
	if width == orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR {
		return "hour"
	}
	return "day"
}

// seriesBuckets assembles the series from one tally of the archive and the
// live runs the cluster reported, placing each live run by its creation time
// with the same search the archive's entries were placed by.
//
// A live run created before the window is in no bucket: no bucket created
// it. One with no creation time cannot be placed and fails the answer, rather
// than vanishing from a bar while the window's running count keeps it.
func seriesBuckets(
	series *statsSeries,
	tally engine.RunTally,
	running []*orkstackv1.Run,
) ([]*orkstackv1.StatsBucket, error) {
	buckets := len(series.edges) - 1
	if len(tally.ByBucket) != buckets {
		return nil, seriesFault(fmt.Errorf("stats series: the archive tallied %d buckets, the series has %d",
			len(tally.ByBucket), buckets))
	}
	live := make([]int, buckets)
	for _, run := range running {
		created := run.GetCreatedAt()
		if created == nil {
			return nil, seriesFault(fmt.Errorf(
				"stats series: running run %s carries no creation time, so no bucket can hold it",
				run.GetId()))
		}
		bucket, ok := engine.BucketOf(series.edges, created.AsTime())
		switch {
		case ok:
			live[bucket]++
		case !created.AsTime().Before(series.edges[len(series.edges)-1]):
			// Only a run created before the window may be in no bucket. One
			// created at or after its end is one the execution index should
			// not have returned, and dropping it would draw a bar short.
			return nil, seriesFault(fmt.Errorf(
				"stats series: running run %s was created at %s, after the window it was read for",
				run.GetId(), created.AsTime().Format(time.RFC3339Nano)))
		}
	}
	out := make([]*orkstackv1.StatsBucket, buckets)
	for i := range out {
		since, until := series.edges[i], series.edges[i+1]
		out[i] = &orkstackv1.StatsBucket{
			Since:        timestamppb.New(since),
			Until:        timestamppb.New(until),
			RunsByStatus: statusCounts(engine.RunTally{ByStatus: tally.ByBucket[i]}, live[i]),
			Partial: !isBoundary(since, series.zone, series.width) ||
				!isBoundary(until, series.zone, series.width),
		}
	}
	return out, nil
}
