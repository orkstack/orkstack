package dashboards

import (
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/metriccatalog"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// censusGauges maps each gauge a census feeds, as Prometheus names it less
// its unit suffix, to the census that feeds it. A census reports nothing
// until its first pass, which waits a random part of its interval so that
// replicas do not ask together (internal/telemetry, census.run): a minute for
// runs_open, longer than any fixed wait for a tile.
var censusGauges = map[string]string{
	promBase(telemetry.RunsOpenMetric):                  "runs_open",
	promBase(telemetry.RunsPausedMetric):                "runs_paused",
	promBase(telemetry.RunsPausedSoonestMetric):         "runs_paused",
	promBase(telemetry.PoolBacklogTasksMetric):          "pool_backlog",
	promBase(telemetry.PoolBacklogAgeMetric):            "pool_backlog",
	promBase(telemetry.PoolTasksAddRateMetric):          "pool_backlog",
	promBase(telemetry.PoolTasksDispatchRateMetric):     "pool_backlog",
	promBase(telemetry.PoolPollersMetric):               "pool_backlog",
	promBase(telemetry.WebhookPendingMetric):            "webhook_pending",
	promBase(telemetry.SchedulesMetric):                 "schedules",
	promBase(telemetry.ScheduleNextFireMetric):          "schedules",
	promBase(telemetry.ScheduleActionsSkippedMetric):    "schedules",
	promBase(telemetry.ScheduleCensusCoverageMetric):    "schedules",
	promBase(telemetry.ScheduleCensusUnavailableMetric): "schedules",
	promBase(telemetry.TelemetryNamespacesMetric):       "telemetry_policy",
}

func promBase(otel string) string { return strings.ReplaceAll(otel, ".", "_") }

// censusIntervals is how often each census runs, so the most its first pass
// waits: the pool backlog at engine.pool_metrics.interval's default, which
// the gate does not set, and every other at the census interval.
var censusIntervals = map[string]time.Duration{
	"pool_backlog": engine.DefaultPoolMetricsInterval,
}

// Every census StartCensuses starts has its gauges in censusGauges, so a
// tile on a census added later is held to the wait too.
func TestObservabilityGate_CensusTableIsComplete(t *testing.T) {
	b, err := os.ReadFile("../telemetry/census.go")
	must.OK(t, err)
	started := map[string]bool{}
	for _, m := range regexp.MustCompile(`census\{name: (?:"([a-z_]+)"|(poolBacklogCensus))`).FindAllStringSubmatch(string(b), -1) {
		if m[2] != "" {
			started["pool_backlog"] = true
		} else {
			started[m[1]] = true
		}
	}
	tabled := map[string]bool{}
	for _, c := range censusGauges {
		tabled[c] = true
	}
	if len(started) == 0 || !maps.Equal(started, tabled) {
		t.Fatalf("StartCensuses starts %v; censusGauges names %v", slices.Sorted(maps.Keys(started)), slices.Sorted(maps.Keys(tabled)))
	}
}

const gateScript = "../../scripts/proof-observability.sh"

// The observability gate holds some top-row tiles to a value. A tile a
// census feeds has none until that census's first pass, so the gate first
// waits for the census to say it has read its source
// (ork_census_last_success_seconds since the engine started), then asks
// Grafana. Polling Grafana alone for a fixed while fails whenever the first
// pass's random delay outlasts it.
func TestObservabilityGate_WaitsForEachCensusFirstPass(t *testing.T) {
	b, err := os.ReadFile(gateScript)
	must.OK(t, err)
	script := string(b)
	grafanaAt := strings.Index(script, `say "== grafana dashboards =="`)
	if grafanaAt < 0 {
		t.Fatal("the gate has no Grafana section")
	}
	before := script[:grafanaAt]

	// The tiles the gate holds to a value: the alternatives of its case arm.
	arm := regexp.MustCompile(`(?m)^\s*("[^"]+"(?:\s*\|\s*"[^"]+")*)\)\s*$`)
	m := arm.FindStringSubmatch(script[grafanaAt:])
	if m == nil {
		t.Fatal("the gate names no tile it holds to a value")
	}
	var valued []string
	for _, alt := range strings.Split(m[1], "|") {
		valued = append(valued, strings.Trim(strings.TrimSpace(alt), `"`))
	}

	top, _, _ := panelsOf(dashboardsByUID(t)[UIDOrkStack])
	waits := regexp.MustCompile(`(?m)^wait_census ([a-z_]+) ([0-9]+)$`)
	waited := map[string]bool{}
	for _, w := range waits.FindAllStringSubmatch(before, -1) {
		waited[w[1]] = true
		want := telemetry.CensusInterval
		if d, ok := censusIntervals[w[1]]; ok {
			want = d
		}
		if w[2] != strconv.Itoa(int(want.Seconds())) {
			t.Errorf("wait_census %s %s: the census runs every %s", w[1], w[2], want)
		}
	}
	found := 0
	for _, p := range top {
		title, _ := p["title"].(string)
		if !slices.Contains(valued, title) {
			continue
		}
		found++
		for _, expr := range exprsOf(p) {
			for _, name := range metriccatalog.MetricNames(expr) {
				census, ok := censusGauges[metriccatalog.BaseName(name)]
				if ok && !waited[census] {
					t.Errorf("top-row %q reads %s, which the %s census feeds; the gate asks Grafana for it without first waiting for that census's first pass (wait_census %s)", title, name, census, census)
				}
			}
		}
	}
	if found != len(valued) {
		t.Fatalf("the gate holds %d tiles to a value but %d are top-row panels: %v", len(valued), found, valued)
	}

	// wait_census reads the census's own signal and is bounded by its
	// interval, never by a guess.
	def := regexp.MustCompile(`(?s)wait_census\(\) \{.*?\n\}`).FindString(before)
	if def == "" {
		t.Fatal("the gate defines no wait_census")
	}
	// A success no earlier than this engine's start, so a series an earlier
	// run of the gate left in the kit's Prometheus never answers for it.
	if !regexp.MustCompile(`ork_census_last_success_seconds\{ork_census=\\?"\$1\\?"\} >= \$ORK_STARTED_AT`).MatchString(def) {
		t.Errorf("wait_census does not wait for a census success since the engine started:\n%s", def)
	}
	startAt := strings.Index(before, `ORK_STARTED_AT="$(date +%s)"`)
	launch := strings.Index(before, `"$BIN/ork" --config`)
	if startAt < 0 || launch < 0 || startAt > launch {
		t.Error("the gate does not record ORK_STARTED_AT before it starts ork")
	}
}
