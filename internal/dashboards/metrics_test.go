package dashboards

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/metriccatalog"
)

// externalMetrics are the series the dashboards read that OrkStack's own
// instruments do not name: Prometheus's alert series, and the Temporal
// SDK's client metrics (internal/telemetry.TemporalMetricsHandler turns
// them on; their names are the SDK's).
var externalMetrics = map[string]bool{
	"ALERTS":                         true,
	"temporal_request_total":         true,
	"temporal_request_failure_total": true,
}

// The extraction itself: a matcher's label, a grouping label and a function
// are never read as a series.
func TestDashboards_MetricNamesOfAnExpression(t *testing.T) {
	got := metriccatalog.MetricNames(`count((max by (ork_pool) (ork_pool_backlog_tasks{ork_pool=~"$pool"}) > 0) unless on (ork_pool) (sum(rate(ork_runs_total[5m])) > 0)) or vector(0)`)
	want := []string{"ork_pool_backlog_tasks", "ork_runs_total"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("MetricNames = %v; want %v", got, want)
	}
	if got := metriccatalog.MetricNames(`label_values(ork_runs_total{ork_namespace=~"$namespace"}, ork_orkflow_name)`); strings.Join(got, ",") != "ork_runs_total" {
		t.Fatalf("label_values: %v; want ork_runs_total", got)
	}
}

// Every series a dashboard queries is one OrkStack exports, under the name
// Prometheus gives it, or one named in externalMetrics. An instrument whose
// meter call is read is held to its exact name; one built through a helper
// is held to its declared name. The observability gate holds every panel to
// a real answer.
func TestDashboards_MetricsExist(t *testing.T) {
	instruments := metriccatalog.Instruments(t)
	names := metriccatalog.PrometheusNames(t, instruments)
	exact := map[string]bool{}
	for _, in := range instruments {
		exact[strings.ReplaceAll(in.Name, ".", "_")] = true
	}
	declared := metriccatalog.DeclaredNames(t)
	checked := 0
	for _, expr := range promQL(t) {
		for _, n := range metriccatalog.MetricNames(expr) {
			checked++
			base := metriccatalog.BaseName(n)
			if names[n] || externalMetrics[n] || (!exact[base] && declared[base]) {
				continue
			}
			{
				t.Errorf("query reads %s, which no instrument exports: %s", n, expr)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no series checked; the check has nothing to hold")
	}
}

var (
	groupingClause = regexp.MustCompile(`\b(?:by|without|on|ignoring|group_left|group_right)\s*\(([^)]*)\)`)
	matcherLabel   = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=~|!~|!=|=)\s*"`)
	legendLabel    = regexp.MustCompile(`\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}`)
	labelValuesArg = regexp.MustCompile(`label_values\([^,]*,\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)`)
)

// Every label a query groups by, matches on or names in a legend is one an
// instrument records: a label nothing records makes a matcher match
// everything and a legend print nothing.
func TestDashboards_LabelsExist(t *testing.T) {
	known := metriccatalog.DeclaredAttributes(t)
	checked := 0
	use := func(where, label string) {
		checked++
		if !known[label] && !metriccatalog.PromLabels[label] {
			t.Errorf("%s uses label %s, which no instrument records", where, label)
		}
	}
	d := dashboardsByUID(t)[UIDOrkStack]
	for _, p := range allPanels(d) {
		if datasourceType(p) != "prometheus" {
			continue
		}
		targets, _ := p["targets"].([]any)
		for _, tg := range targets {
			tm := tg.(map[string]any)
			expr, _ := tm["expr"].(string)
			for _, g := range groupingClause.FindAllStringSubmatch(expr, -1) {
				for _, l := range strings.Split(g[1], ",") {
					if l = strings.TrimSpace(l); l != "" {
						use(expr, l)
					}
				}
			}
			for _, m := range matcherLabel.FindAllStringSubmatch(expr, -1) {
				use(expr, m[1])
			}
			legend, _ := tm["legendFormat"].(string)
			for _, m := range legendLabel.FindAllStringSubmatch(legend, -1) {
				use(fmt.Sprintf("panel %q's legend", p["title"]), m[1])
			}
		}
	}
	for _, v := range d["templating"].(map[string]any)["list"].([]any) {
		if q, ok := v.(map[string]any)["definition"].(string); ok {
			for _, m := range labelValuesArg.FindAllStringSubmatch(q, -1) {
				use(q, m[1])
			}
			for _, m := range matcherLabel.FindAllStringSubmatch(q, -1) {
				use(q, m[1])
			}
		}
	}
	if checked == 0 {
		t.Fatal("no label checked; the check has nothing to hold")
	}
}
