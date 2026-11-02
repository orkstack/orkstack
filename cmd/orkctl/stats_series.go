package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// seriesFlags are `orkctl stats`' series flags, as typed.
type seriesFlags struct {
	by          string
	buckets     int
	bucketsSet  bool
	timezone    string
	timezoneSet bool
}

// Default bucket counts when --by is given and --since is not: the last
// week of days, or the last day of hours, the current one included.
const (
	defaultDayBuckets  = 7
	defaultHourBuckets = 24
)

// applySeriesFlags puts --by, --buckets and --timezone on a request.
//
// It decides nothing a clock decides. "The last seven days" travels as a
// width, a zone and a count, and the server, which owns the clock, computes
// every boundary; this command never turns a date into a local midnight. So
// a date-only --since is refused beside --by: it would be read as midnight
// UTC, and the first bar of a viewer west of UTC would be yesterday's last
// hours. localZone names this machine's zone when --timezone is not given;
// it is called only then.
func applySeriesFlags(
	req *orkstackv1.GetStatsRequest,
	flags seriesFlags,
	since string,
	localZone func() string,
) error {
	if flags.by == "" {
		switch {
		case flags.bucketsSet:
			return fmt.Errorf("--buckets counts the buckets of a series; name their width with --by day or --by hour")
		case flags.timezoneSet:
			return fmt.Errorf("--timezone draws the buckets of a series; ask for one with --by day or --by hour")
		}
		return nil
	}
	var defaultCount int32
	switch strings.ToLower(flags.by) {
	case "day":
		req.BucketWidth, defaultCount = orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_DAY, defaultDayBuckets
	case "hour":
		req.BucketWidth, defaultCount = orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR, defaultHourBuckets
	default:
		return fmt.Errorf("invalid --by %q: want day or hour", flags.by)
	}
	switch {
	case since != "" && flags.bucketsSet:
		return fmt.Errorf("--since and --buckets both say where the window starts; send one of them")
	case since != "":
		if _, err := time.ParseDuration(since); err != nil {
			if _, err := time.Parse("2006-01-02", since); err == nil {
				return fmt.Errorf("--since %q is a date, and a date names no zone: with --by, give an "+
					"instant with its offset (%sT00:00:00-07:00) or count back with --buckets", since, since)
			}
		}
	case flags.bucketsSet && flags.buckets < 1:
		return fmt.Errorf("invalid --buckets %d: a series has at least one bucket", flags.buckets)
	case flags.bucketsSet:
		// Above the server's limit it refuses, naming the limit; the clamp only
		// keeps a huge number from wrapping into a small one on the way.
		req.BucketCount = int32(min(flags.buckets, 1<<30))
	default:
		req.BucketCount = defaultCount
	}
	if flags.timezoneSet {
		// An empty --timezone is most often an empty shell variable, and
		// reading it as UTC would draw a calendar nobody chose.
		if flags.timezone == "" {
			return fmt.Errorf("--timezone is empty: name an IANA zone, such as UTC or Europe/Lisbon")
		}
		req.Timezone = flags.timezone
	} else {
		req.Timezone = localZone()
	}
	return nil
}

// machineZone is this machine's zone name, for a series drawn on the
// viewer's own calendar, and — when it had to guess — a sentence saying so.
func machineZone() (string, string) {
	return localZoneName(os.LookupEnv, os.Readlink, func(name string) bool {
		_, err := time.LoadLocation(name)
		return err == nil
	})
}

// localZoneName finds an IANA zone name for this machine: $TZ, then the
// target of /etc/localtime, then UTC. Each candidate is used only if it is a
// name the zone database holds — a POSIX rule such as "EST5EDT,M3.2.0,M11.1.0"
// or a path to a file elsewhere is not one, and the server would refuse it —
// so a value that is not falls through to the next. Go's own time.Local is no
// help here: it is called "Local" on every machine.
//
// The second result is empty when a source named the zone, and otherwise
// says what was skipped and what was used instead. A guessed UTC prints the
// same "UTC" a chosen one does, and a viewer west of it would read their
// evening as tomorrow, so the guess is said, never left to be noticed.
func localZoneName(
	getenv func(string) (string, bool),
	readlink func(string) (string, error),
	loads func(string) bool,
) (zone, guessed string) {
	usable := func(raw string) (string, bool) {
		name := strings.TrimPrefix(raw, ":")
		if i := strings.LastIndex(name, "zoneinfo/"); i >= 0 {
			name = name[i+len("zoneinfo/"):]
		}
		name = strings.TrimPrefix(strings.TrimPrefix(name, "posix/"), "right/")
		if name == "" || name == "Local" || filepath.IsAbs(name) || !loads(name) {
			return "", false
		}
		return name, true
	}
	skipped := ""
	if raw, ok := getenv("TZ"); ok {
		// An empty TZ is UTC, by POSIX: a choice, not a guess.
		if raw == "" || raw == ":" {
			return "UTC", ""
		}
		if name, ok := usable(raw); ok {
			return name, ""
		}
		skipped = fmt.Sprintf("TZ=%q is not a zone name the zone database holds; ", raw)
	}
	if target, err := readlink("/etc/localtime"); err == nil {
		if name, ok := usable(target); ok {
			if skipped != "" {
				return name, skipped + "drawing the series in " + name + ", from /etc/localtime (set --timezone to choose)"
			}
			return name, ""
		}
	}
	return "UTC", skipped + "could not tell this machine's time zone, so the series is drawn in UTC " +
		"(set --timezone to choose)"
}

// renderSeries writes the series table: one row per bucket, labelled on the
// wall clock of the zone the server drew it in.
//
// The label is the bucket's first instant in that zone — its date, and for
// an hour series its time — because that is what a reader looks a bucket up
// by. A bucket the window cut short says so. When this orkctl's zone
// database does not hold the zone the server named, the labels are UTC
// instants and a line says why, rather than a calendar in the wrong zone.
func renderSeries(out io.Writer, stats *orkstackv1.GetStatsResponse) error {
	word, layout := "DAY", "2006-01-02"
	if stats.GetBucketWidth() == orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR {
		word, layout = "HOUR", "2006-01-02 15:04"
	}
	zone, err := time.LoadLocation(stats.GetTimezone())
	switch {
	case stats.GetTimezone() == "":
		zone, layout = time.UTC, time.RFC3339
		fmt.Fprintln(out, "Labels are UTC instants: the engine named no zone for this series.")
	case err != nil:
		zone, layout = time.UTC, time.RFC3339
		fmt.Fprintf(out, "Labels are UTC instants: this orkctl's zone database has no %q.\n",
			stats.GetTimezone())
	}

	columns := seriesColumns(stats.GetBuckets())
	table := newTable(out)
	header := []string{word}
	for _, status := range columns {
		header = append(header, strings.ToUpper(status))
	}
	fmt.Fprintln(table, strings.Join(append(header, "TOTAL"), "\t"))
	var bucketRunning int64
	for _, bucket := range stats.GetBuckets() {
		label := bucket.GetSince().AsTime().In(zone).Format(layout)
		if bucket.GetPartial() {
			label += " (partial)"
		}
		row := []string{label}
		var total int64
		for _, status := range columns {
			count := bucket.GetRunsByStatus()[status]
			total += count
			row = append(row, fmt.Sprint(count))
		}
		bucketRunning += bucket.GetRunsByStatus()["running"]
		fmt.Fprintln(table, strings.Join(append(row, fmt.Sprint(total)), "\t"))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	// The window's running count is a fact about now and includes runs no
	// bucket created; the difference is said, not left for a reader to find.
	switch before := stats.GetRunsByStatus()["running"] - bucketRunning; {
	case before == 1:
		fmt.Fprintln(out, "1 running run was created before this window and is in no bucket.")
	case before > 1:
		fmt.Fprintf(out, "%d running runs were created before this window and are in no bucket.\n", before)
	}
	return nil
}

// requireSeries refuses an answer that ignored a series it was asked for. A
// series always has at least one bucket, so none means the engine predates
// bucket_width and read the request as a plain one; printing its totals as
// if they were the answer would hide that, and --json would read as a series
// of nothing.
func requireSeries(req *orkstackv1.GetStatsRequest, stats *orkstackv1.GetStatsResponse) error {
	if req.GetBucketWidth() != orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_UNSPECIFIED &&
		(len(stats.GetBuckets()) == 0 ||
			stats.GetBucketWidth() == orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_UNSPECIFIED) {
		return fmt.Errorf("the engine answered without the series --by asked for: it predates " +
			"stats series; upgrade it, or ask without --by for the window's totals")
	}
	return nil
}

// machineZoneRefused adds, to an engine's refusal of a zone this command
// found on its own, where the zone came from and how to choose another: the
// engine's words name the zone, not that nobody typed it.
//
// shown is the error as it will be printed; cause is the engine's, whose
// code and words decide.
func machineZoneRefused(shown, cause error, req *orkstackv1.GetStatsRequest, flags seriesFlags) error {
	if flags.by == "" || flags.timezoneSet || connect.CodeOf(cause) != connect.CodeInvalidArgument ||
		!strings.Contains(cause.Error(), "timezone") {
		return shown
	}
	return fmt.Errorf("%w (the zone %q is this machine's, from $TZ or /etc/localtime; "+
		"choose one the engine holds with --timezone)", shown, req.GetTimezone())
}

// seriesColumns is the series table's status columns: the window's four, in
// the RUNS table's order, then any other the archive spelled, so no run
// counted in a bucket is left out of its row.
func seriesColumns(buckets []*orkstackv1.StatsBucket) []string {
	columns := []string{"running", "succeeded", "failed", "canceled"}
	extra := map[string]bool{}
	for _, bucket := range buckets {
		for status := range bucket.GetRunsByStatus() {
			if !slices.Contains(columns, status) {
				extra[status] = true
			}
		}
	}
	return append(columns, sortedKeys(extra)...)
}
