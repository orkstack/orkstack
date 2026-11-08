package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
	// The zone database is linked in, as it is into every release binary, so
	// the labels below do not depend on what the host has installed.
	_ "time/tzdata"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// These tests drive `orkctl stats --by` as a person does — arguments in,
// output and exit code out — against the real engine handler in process, or
// against a stand-in engine where the case is one the real engine cannot
// produce. The fixture's clock reads 2026-01-02T03:04:05Z, which is still
// Jan 1 in Los Angeles. Each test names the mutations it exists to catch.

// seriesRows is the label column of the series table: every line after the
// DAY or HOUR header up to the first line that is not a bucket row.
func seriesRows(stdout string) []string {
	var rows []string
	inTable := false
	for _, line := range strings.Split(stdout, "\n") {
		switch {
		case strings.HasPrefix(line, "DAY ") || strings.HasPrefix(line, "HOUR "):
			inTable = true
		case !inTable:
		case line == "" || !strings.HasPrefix(line, "1") && !strings.HasPrefix(line, "2"):
			return rows
		default:
			label, _, _ := strings.Cut(line, "  ")
			rows = append(rows, label)
		}
	}
	return rows
}

// TestStats_SeriesGoldenHumanAndJSON pins both renderings of a series. One
// live run was created a month before the window, so the window's running
// count holds a run no bucket can.
//
// Mutations it catches: any change to the table, its labels, the partial
// mark, the note about runs before the window, or the JSON a script reads.
func TestStats_SeriesGoldenHumanAndJSON(t *testing.T) {
	executions := map[string]engine.Executions{
		"default": {
			Running: []*orkstackv1.Run{
				{
					Id: "ork-live-1", Namespace: "default", OrkflowName: "deploy",
					Status:    orkstackv1.Status_STATUS_RUNNING,
					CreatedAt: timestamppb.New(fixedNow.Add(-10 * time.Minute)),
				},
				{
					Id: "ork-live-old", Namespace: "default", OrkflowName: "nightly",
					Status:    orkstackv1.Status_STATUS_RUNNING,
					CreatedAt: timestamppb.New(fixedNow.Add(-30 * 24 * time.Hour)),
				},
			},
		},
	}
	f := seedStats(t, executions)

	human := f.run("stats", "--by", "day", "--timezone", "America/Los_Angeles").requireExit(t, exitOK)
	requireGolden(t, "stats-series.golden", human.stdout)

	machine := f.run("--json", "stats", "--by", "day", "--timezone", "America/Los_Angeles").requireExit(t, exitOK)
	requireGolden(t, "stats-series.json.golden", machine.stdout)

	hours := f.run("stats", "--by", "hour", "--buckets", "3", "--timezone", "Asia/Kolkata").requireExit(t, exitOK)
	requireGolden(t, "stats-series-hour.golden", hours.stdout)
}

// TestStats_SeriesFlagsAskForTheSeriesTheyName: each spelling of the flags
// draws the series it says, or is refused with a message naming the flag.
//
// Mutations it catches: --timezone ignored (Los Angeles' last day is Jan 1,
// UTC's Jan 2); a default other than 7 days or 24 hours; --buckets ignored;
// --since dropped beside --by, or turned into a count; a date-only --since
// read as UTC midnight instead of refused; a flag accepted with no --by and
// silently ignored; a server refusal not reaching the user.
func TestStats_SeriesFlagsAskForTheSeriesTheyName(t *testing.T) {
	f := seedStats(t, map[string]engine.Executions{})
	for _, tc := range []struct {
		name        string
		args        []string
		rows        int
		first, last string
		refusal     string
	}{
		{name: "the last seven days in the zone named", args: []string{"--by", "day", "--timezone", "America/Los_Angeles"},
			rows: 7, first: "2025-12-26", last: "2026-01-01 (partial)"},
		{name: "the last 24 hours", args: []string{"--by", "HOUR", "--timezone", "UTC"},
			rows: 24, first: "2026-01-01 04:00", last: "2026-01-02 03:00 (partial)"},
		{name: "a count of days", args: []string{"--by", "day", "--buckets", "30", "--timezone", "UTC"},
			rows: 30, first: "2025-12-04", last: "2026-01-02 (partial)"},
		{name: "a lookback, cut into days", args: []string{"--by", "day", "--since", "72h", "--timezone", "UTC"},
			rows: 4, first: "2025-12-30 (partial)", last: "2026-01-02 (partial)"},
		{name: "an instant with its offset", args: []string{"--by", "day", "--timezone", "America/Los_Angeles",
			"--since", "2026-01-01T00:00:00-08:00"}, rows: 1, first: "2026-01-01 (partial)", last: "2026-01-01 (partial)"},
		{name: "a width nobody draws", args: []string{"--by", "week"}, refusal: "want day or hour"},
		{name: "a count with no width", args: []string{"--buckets", "7"}, refusal: "--by day or --by hour"},
		{name: "a zone with no width", args: []string{"--timezone", "UTC"}, refusal: "--by day or --by hour"},
		{name: "a count and a since", args: []string{"--by", "day", "--buckets", "7", "--since", "72h"},
			refusal: "--since and --buckets"},
		{name: "a date, which names no zone", args: []string{"--by", "day", "--since", "2026-01-01"},
			refusal: "a date names no zone"},
		{name: "no buckets", args: []string{"--by", "day", "--buckets", "0"}, refusal: "at least one bucket"},
		{name: "an empty zone", args: []string{"--by", "day", "--timezone", ""}, refusal: "--timezone is empty"},
		{name: "a zone the engine does not hold", args: []string{"--by", "day", "--timezone", "Mars/Olympus_Mons"},
			refusal: "Mars/Olympus_Mons"},
		{name: "more buckets than an answer carries", args: []string{"--by", "day", "--buckets", "1001", "--timezone", "UTC"},
			refusal: "1000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.refusal != "" {
				got := f.run(append([]string{"stats"}, tc.args...)...).requireExit(t, exitError)
				if !strings.Contains(got.stderr, tc.refusal) {
					t.Fatalf("stderr %q does not say %q", got.stderr, tc.refusal)
				}
				return
			}
			got := f.run(append([]string{"stats"}, tc.args...)...).requireExit(t, exitOK)
			rows := seriesRows(got.stdout)
			if len(rows) != tc.rows || rows[0] != tc.first || rows[len(rows)-1] != tc.last {
				t.Fatalf("rows %q, want %d from %q to %q:\n%s", rows, tc.rows, tc.first, tc.last, got.stdout)
			}
		})
	}
}

// TestStats_SeriesIsDrawnInThisMachinesZone: with no --timezone, the zone is
// the one $TZ names, and a $TZ that names none is said on stderr rather than
// silently replaced.
//
// Mutations it catches: $TZ ignored; a leading ':' or a zoneinfo path sent
// as the zone, which the engine refuses; an empty $TZ read as anything but
// UTC; a POSIX rule sent to the engine, or replaced with no word to the user.
func TestStats_SeriesIsDrawnInThisMachinesZone(t *testing.T) {
	f := seedStats(t, map[string]engine.Executions{})
	for _, tc := range []struct {
		tz, series, says string
	}{
		{tz: "America/New_York", series: "Series:        by day, America/New_York"},
		{tz: ":Europe/Lisbon", series: "Series:        by day, Europe/Lisbon"},
		{tz: ":/usr/share/zoneinfo/Asia/Tokyo", series: "Series:        by day, Asia/Tokyo"},
		{tz: "", series: "Series:        by day, UTC"},
		// A POSIX rule falls through to this host's /etc/localtime, so which
		// zone is drawn depends on the host; what is asserted is that the
		// skip is said, with how to choose, and the rule never reaches the
		// engine.
		{tz: "EST5EDT,M3.2.0,M11.1.0", says: `TZ="EST5EDT,M3.2.0,M11.1.0" is not a zone name`},
	} {
		t.Run("TZ="+tc.tz, func(t *testing.T) {
			t.Setenv("TZ", tc.tz)
			got := f.run("stats", "--by", "day").requireExit(t, exitOK)
			if tc.series != "" && !strings.Contains(got.stdout, tc.series+"\n") {
				t.Errorf("stdout does not say %q:\n%s", tc.series, got.stdout)
			}
			if tc.says == "" && got.stderr != "" {
				t.Errorf("a zone $TZ named is not a guess, yet stderr says %q", got.stderr)
			}
			if tc.says != "" && (!strings.Contains(got.stderr, tc.says) || !strings.Contains(got.stderr, "--timezone")) {
				t.Errorf("stderr %q does not say %q and how to choose", got.stderr, tc.says)
			}
			if strings.Contains(got.stdout, "M3.2.0") {
				t.Errorf("a POSIX rule reached the series:\n%s", got.stdout)
			}
		})
	}
}

// TestLocalZoneName_SaysWhichSourceFailedAndHow: every way the machine's zone
// is not found names what was read and why it was not used, never a generic
// "could not tell". A link that fails is missing or not a link; a link that
// reads is named by its target.
func TestLocalZoneName_SaysWhichSourceFailedAndHow(t *testing.T) {
	const drawn = ", so the series is drawn in UTC (set --timezone to choose)"
	notALink := errors.New("readlink /etc/localtime: invalid argument")
	missing := fmt.Errorf("readlink /etc/localtime: %w", os.ErrNotExist)
	for _, tc := range []struct {
		name      string
		tz        *string
		target    string
		linkErr   error
		wantZone  string
		wantGuess string
	}{
		{name: "TZ unset, /etc/localtime missing", linkErr: missing, wantZone: "UTC",
			wantGuess: "TZ is not set and /etc/localtime is not a link into the zone database" + drawn},
		{name: "TZ unset, /etc/localtime not a link", linkErr: notALink, wantZone: "UTC",
			wantGuess: "TZ is not set and /etc/localtime is not a link into the zone database" + drawn},
		{name: "TZ unset, a link to no zone", target: "/usr/share/zoneinfo/Mars/Olympus", wantZone: "UTC",
			wantGuess: `/etc/localtime points to "/usr/share/zoneinfo/Mars/Olympus", which is not a zone the zone database holds` + drawn},
		{name: "TZ unset, a link outside the zone database", target: "/opt/zones/here", wantZone: "UTC",
			wantGuess: `/etc/localtime points to "/opt/zones/here", which is not a zone the zone database holds` + drawn},
		{name: "TZ a rule, /etc/localtime not a link", tz: zonePtr("EST5EDT,M3.2.0,M11.1.0"), linkErr: notALink, wantZone: "UTC",
			wantGuess: `TZ="EST5EDT,M3.2.0,M11.1.0" is not a zone name the zone database holds; ` +
				"/etc/localtime is not a link into the zone database" + drawn},
		{name: "TZ a rule, a link to no zone", tz: zonePtr("EST5EDT,M3.2.0,M11.1.0"), target: "/usr/share/zoneinfo/Mars/Olympus", wantZone: "UTC",
			wantGuess: `TZ="EST5EDT,M3.2.0,M11.1.0" is not a zone name the zone database holds; ` +
				`/etc/localtime points to "/usr/share/zoneinfo/Mars/Olympus", which is not a zone the zone database holds` + drawn},
		{name: "TZ a rule, a link to a zone", tz: zonePtr("EST5EDT,M3.2.0,M11.1.0"), target: "/usr/share/zoneinfo/Europe/Lisbon", wantZone: "Europe/Lisbon",
			wantGuess: `TZ="EST5EDT,M3.2.0,M11.1.0" is not a zone name the zone database holds; ` +
				"drawing the series in Europe/Lisbon, from /etc/localtime (set --timezone to choose)"},
		{name: "TZ unset, a link to a zone", target: "/usr/share/zoneinfo/Europe/Lisbon", wantZone: "Europe/Lisbon"},
		{name: "TZ a zone", tz: zonePtr("Asia/Tokyo"), linkErr: notALink, wantZone: "Asia/Tokyo"},
		{name: "TZ empty", tz: zonePtr(""), linkErr: notALink, wantZone: "UTC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(string) (string, bool) {
				if tc.tz == nil {
					return "", false
				}
				return *tc.tz, true
			}
			readlink := func(string) (string, error) { return tc.target, tc.linkErr }
			loads := func(name string) bool { return name == "Europe/Lisbon" || name == "Asia/Tokyo" || name == "UTC" }
			zone, guessed := localZoneName(getenv, readlink, loads)
			if zone != tc.wantZone || guessed != tc.wantGuess {
				t.Errorf("localZoneName = %q, %q\nwant           %q, %q", zone, guessed, tc.wantZone, tc.wantGuess)
			}
		})
	}
}

func zonePtr(s string) *string { return &s }

// standInEngine is a stats server whose answer the test writes, for the cases
// the real engine never produces: an engine that predates series, and one
// that names a zone this orkctl's zone database lacks.
type standInEngine struct {
	orkstackv1connect.UnimplementedStatsServiceHandler
	answer  *orkstackv1.GetStatsResponse
	refusal error
}

func (s standInEngine) GetStats(
	context.Context,
	*connect.Request[orkstackv1.GetStatsRequest],
) (*connect.Response[orkstackv1.GetStatsResponse], error) {
	if s.refusal != nil {
		return nil, s.refusal
	}
	return connect.NewResponse(proto.Clone(s.answer).(*orkstackv1.GetStatsResponse)), nil
}

// runAgainstStandIn runs orkctl against a stand-in engine answering with answer.
func runAgainstStandIn(t *testing.T, answer *orkstackv1.GetStatsResponse, args ...string) result {
	t.Helper()
	return runAgainstEngine(t, standInEngine{answer: answer}, args...)
}

// runAgainstEngine runs orkctl against the given stand-in engine.
func runAgainstEngine(t *testing.T, engine standInEngine, args ...string) result {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewStatsServiceHandler(engine))
	server := api.NewServer("127.0.0.1:0", mux)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	t.Setenv(serverEnvVar, "")
	var stdout, stderr bytes.Buffer
	code := Execute(append(args, "--server", "http://"+listener.Addr().String()), &stdout, &stderr)
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

// TestStats_RefusesAnEngineThatIgnoredTheSeries: an engine that predates
// series reads the request as a plain one and answers its totals with no
// buckets. orkctl says so and fails, rather than printing totals as if they
// were the series, or a JSON a script reads as a series of nothing.
//
// Mutation it catches: an answer with no buckets accepted for --by.
func TestStats_RefusesAnEngineThatIgnoredTheSeries(t *testing.T) {
	plain := &orkstackv1.GetStatsResponse{
		Namespace:    "default",
		RunsByStatus: map[string]int64{"running": 0, "succeeded": 3, "failed": 0, "canceled": 0},
		WindowSince:  timestamppb.New(fixedNow.Add(-24 * time.Hour)),
		WindowUntil:  timestamppb.New(fixedNow),
		ComputedAt:   timestamppb.New(fixedNow),
	}
	for _, args := range [][]string{
		{"stats", "--by", "day", "--timezone", "UTC"},
		{"--json", "stats", "--by", "day", "--timezone", "UTC"},
	} {
		got := runAgainstStandIn(t, plain, args...)
		if got.code == exitOK || !strings.Contains(got.stderr, "predates stats series") || got.stdout != "" {
			t.Errorf("%v: exit %d, stdout %q, stderr %q; want a failure naming the old engine and nothing printed",
				args, got.code, got.stdout, got.stderr)
		}
	}
	// The same engine still answers a plain question.
	if got := runAgainstStandIn(t, plain, "stats"); got.code != exitOK {
		t.Errorf("plain stats against the same engine: exit %d, stderr %q", got.code, got.stderr)
	}
	// Buckets with no width named are no series either: drawn, they would be
	// read as days whatever they were.
	unnamed := proto.Clone(plain).(*orkstackv1.GetStatsResponse)
	unnamed.Buckets = []*orkstackv1.StatsBucket{{
		Since: plain.GetWindowSince(), Until: plain.GetWindowUntil(), RunsByStatus: plain.GetRunsByStatus(),
	}}
	if got := runAgainstStandIn(t, unnamed, "stats", "--by", "day", "--timezone", "UTC"); got.code == exitOK ||
		!strings.Contains(got.stderr, "predates stats series") {
		t.Errorf("buckets with no width: exit %d, stderr %q; want the same refusal", got.code, got.stderr)
	}
}

// TestStats_SaysWhereAZoneTheEngineRefusedCameFrom: a zone this orkctl found
// on its own and the engine does not hold is refused with the engine's words
// and where the zone came from, so the user knows nobody typed it and how to
// choose another. A zone the user typed is refused with the engine's words
// alone.
//
// Mutation it catches: the engine's refusal printed alone for a zone the user
// never named, or the hint added to a zone the user did name.
func TestStats_SaysWhereAZoneTheEngineRefusedCameFrom(t *testing.T) {
	t.Setenv("TZ", "Asia/Tokyo")
	refusing := standInEngine{refusal: connect.NewError(connect.CodeInvalidArgument,
		errors.New(`stats series: timezone "Asia/Tokyo": unknown time zone Asia/Tokyo`))}
	found := runAgainstEngine(t, refusing, "stats", "--by", "day")
	if found.code == exitOK || !strings.Contains(found.stderr, "unknown time zone Asia/Tokyo") ||
		!strings.Contains(found.stderr, "this machine's, from $TZ or /etc/localtime") {
		t.Errorf("a found zone refused: exit %d, stderr %q", found.code, found.stderr)
	}
	typed := runAgainstEngine(t, refusing, "stats", "--by", "day", "--timezone", "Asia/Tokyo")
	if typed.code == exitOK || strings.Contains(typed.stderr, "this machine's") {
		t.Errorf("a typed zone refused: exit %d, stderr %q; the user named it", typed.code, typed.stderr)
	}
}

// TestStats_LabelsAreUTCInstantsForAZoneThisOrkctlLacks: when the engine
// names a zone this orkctl cannot load, the rows are labelled by their UTC
// instants and a line says why, never by a calendar in the wrong zone.
//
// Mutations it catches: labels silently formatted in UTC or the local zone
// as if they were the engine's; a bucket cut at both ends marked as anything
// but partial.
func TestStats_LabelsAreUTCInstantsForAZoneThisOrkctlLacks(t *testing.T) {
	since := time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC)
	counts := map[string]int64{"running": 0, "succeeded": 1, "failed": 0, "canceled": 0}
	answer := &orkstackv1.GetStatsResponse{
		Namespace:    "default",
		RunsByStatus: counts,
		WindowSince:  timestamppb.New(since),
		WindowUntil:  timestamppb.New(since.Add(2 * time.Hour)),
		ComputedAt:   timestamppb.New(fixedNow),
		BucketWidth:  orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_DAY,
		Timezone:     "Mars/Olympus_Mons",
		Buckets: []*orkstackv1.StatsBucket{{
			Since: timestamppb.New(since), Until: timestamppb.New(since.Add(2 * time.Hour)),
			RunsByStatus: counts, Partial: true,
		}},
	}
	got := runAgainstStandIn(t, answer, "stats", "--by", "day", "--timezone", "Mars/Olympus_Mons")
	if got.code != exitOK {
		t.Fatalf("exit %d, stderr %q", got.code, got.stderr)
	}
	if !strings.Contains(got.stdout, `Labels are UTC instants: this orkctl's zone database has no "Mars/Olympus_Mons".`) {
		t.Errorf("stdout does not say why the labels are UTC:\n%s", got.stdout)
	}
	if rows := seriesRows(got.stdout); len(rows) != 1 || rows[0] != "2026-01-01T09:30:00Z (partial)" {
		t.Errorf("rows %q, want the bucket's UTC instant, marked partial:\n%s", rows, got.stdout)
	}
}

// TestStats_EveryCountedRunIsInItsRow: a status the archive spelled beyond
// the four gets a column of its own and counts in its row's TOTAL, and the
// runs going since before the window are counted in a note, in the plural
// when there are several.
//
// Mutations it catches: an unknown status dropped from the table and its
// TOTAL; the note about runs before the window left out, or always singular.
func TestStats_EveryCountedRunIsInItsRow(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bucket := map[string]int64{"running": 1, "succeeded": 2, "failed": 0, "canceled": 0, "skipped": 3}
	answer := &orkstackv1.GetStatsResponse{
		Namespace:    "default",
		RunsByStatus: map[string]int64{"running": 3, "succeeded": 2, "failed": 0, "canceled": 0, "skipped": 3},
		WindowSince:  timestamppb.New(since),
		WindowUntil:  timestamppb.New(since.Add(24 * time.Hour)),
		ComputedAt:   timestamppb.New(fixedNow),
		BucketWidth:  orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_DAY,
		Timezone:     "UTC",
		Buckets: []*orkstackv1.StatsBucket{{
			Since: timestamppb.New(since), Until: timestamppb.New(since.Add(24 * time.Hour)), RunsByStatus: bucket,
		}},
	}
	got := runAgainstStandIn(t, answer, "stats", "--by", "day", "--timezone", "UTC")
	if got.code != exitOK {
		t.Fatalf("exit %d, stderr %q", got.code, got.stderr)
	}
	for _, want := range []string{
		"DAY         RUNNING  SUCCEEDED  FAILED  CANCELED  SKIPPED  TOTAL\n",
		"2026-01-01  1        2          0       0         3        6\n",
		"2 running runs were created before this window and are in no bucket.\n",
	} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("stdout lacks %q:\n%s", want, got.stdout)
		}
	}
}

// FuzzStatsSeriesFlags is the CLI's one series input path, both parsers on
// it: --by, --buckets and --timezone, and, when --timezone is not given, the
// zone found from $TZ and /etc/localtime. Whatever is typed or set, it either
// refuses with a message naming a flag, or puts exactly a width, a zone and
// at most one way of starting the window on the request — never an instant,
// never a count beside --since — and the zone it found is UTC or a name the
// zone database holds, relative and never "Local", with a guess always said.
func FuzzStatsSeriesFlags(f *testing.F) {
	for _, seed := range []struct {
		by, timezone, since, tz, link string
		buckets                       int
		bucketsSet, tzFlagSet, tzEnv  bool
	}{
		{"", "", "", "", "", 0, false, false, false},
		{"day", "", "", "America/New_York", "", 0, false, false, true},
		{"Hour", "Asia/Kolkata", "", "", "", 3, true, true, false},
		{"day", "", "72h", ":/usr/share/zoneinfo/Asia/Tokyo", "", 0, false, false, true},
		{"day", "", "2026-10-01", "", "", 0, false, false, false},
		{"day", "", "2026-10-01T00:00:00-07:00", "", "/var/db/timezone/zoneinfo/Europe/Paris", 0, false, false, false},
		{"day", "", "", "EST5EDT,M3.2.0,M11.1.0", "/usr/share/zoneinfo/America/Chicago", 0, false, false, true},
		{"day", "", "", "Local", "zoneinfo/../../etc/passwd", 0, false, false, true},
		{"day", "", "", ":", "", 0, false, false, true},
		{"week", "", "", "", "", 0, false, false, false},
		{"", "UTC", "", "", "", 0, false, true, false},
		{"", "", "", "", "", 7, true, false, false},
		{"day", "", "", "", "", -1, true, false, false},
		{"day", "", "", "", "", 1 << 40, true, false, false},
		{"day", "", "6h", "", "", 7, true, false, false},
	} {
		f.Add(seed.by, seed.timezone, seed.since, seed.tz, seed.link, seed.buckets, seed.bucketsSet, seed.tzFlagSet, seed.tzEnv)
	}
	loads := func(name string) bool {
		_, err := time.LoadLocation(name)
		return err == nil
	}
	f.Fuzz(func(t *testing.T, by, timezone, since, tz, link string, buckets int, bucketsSet, tzFlagSet, tzEnv bool) {
		found, guessed := localZoneName(
			func(string) (string, bool) { return tz, tzEnv },
			func(string) (string, error) {
				if link == "" {
					return "", errors.New("not a link")
				}
				return link, nil
			},
			loads)
		if found != "UTC" && (found == "Local" || strings.HasPrefix(found, "/") ||
			strings.HasPrefix(found, ":") || !loads(found)) {
			t.Fatalf("localZoneName(TZ=%q, link=%q) = %q, not a zone a server would take", tz, link, found)
		}
		if guessed != "" && !strings.Contains(guessed, "--timezone") {
			t.Fatalf("localZoneName(TZ=%q, link=%q) guessed %q without saying how to choose", tz, link, guessed)
		}
		if tzEnv && found == strings.TrimPrefix(tz, ":") && guessed != "" {
			t.Fatalf("TZ=%q named the zone, yet it is reported as a guess: %q", tz, guessed)
		}
		if !tzEnv && link == "" && (found != "UTC" || guessed == "") {
			t.Fatalf("no $TZ and no /etc/localtime link gave %q, guess %q; want UTC, said as a guess", found, guessed)
		}
		flags := seriesFlags{by: by, buckets: buckets, bucketsSet: bucketsSet, timezone: timezone, timezoneSet: tzFlagSet}
		req := &orkstackv1.GetStatsRequest{}
		err := applySeriesFlags(req, flags, since, func() string { return found })
		if err != nil {
			if !strings.Contains(err.Error(), "--") {
				t.Fatalf("applySeriesFlags(%+v, %q) = %v: a refusal names the flag", flags, since, err)
			}
			return
		}
		if by == "" {
			if !proto.Equal(req, &orkstackv1.GetStatsRequest{}) {
				t.Fatalf("no --by, yet the request carries a series: %v", req)
			}
			return
		}
		switch req.GetBucketWidth() {
		case orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_DAY, orkstackv1.StatsBucketWidth_STATS_BUCKET_WIDTH_HOUR:
		default:
			t.Fatalf("--by %q put width %v on the request", by, req.GetBucketWidth())
		}
		if req.GetSince() != nil || req.GetUntil() != nil || req.GetLookback() != nil {
			t.Fatalf("the series flags put an instant on the request: %v", req)
		}
		if since != "" && req.GetBucketCount() != 0 {
			t.Fatalf("--since %q and a bucket count %d both start the window", since, req.GetBucketCount())
		}
		if since == "" && req.GetBucketCount() < 1 {
			t.Fatalf("no --since and a bucket count of %d", req.GetBucketCount())
		}
		want := found
		if tzFlagSet {
			want = timezone
		}
		if req.GetTimezone() != want {
			t.Fatalf("timezone %q sent, want %q", req.GetTimezone(), want)
		}
	})
}
