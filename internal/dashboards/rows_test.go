package dashboards

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/metriccatalog"
)

// areaPrefixes are the areas whose every exported series an operator reads
// on the dashboard: an instrument added to one of them and drawn nowhere is
// a question the dashboard cannot answer (prod-kit design §2.3.4, rows R2,
// R4 to R7 and R10).
var areaPrefixes = []string{
	"ork_schedule_", "ork_schedules",
	"ork_webhook_",
	"ork_trigger_",
	"ork_concurrency_",
	"ork_cleanup_", "ork_deployment_cleanup_",
	"ork_archive_sweep_",
	"ork_gc_", "ork_deployment_gc_",
	"ork_pool_",
}

// notDrawn are the series of those areas that no panel reads on purpose,
// each with why.
var notDrawn = map[string]string{}

// Every series of an area the dashboard covers is read by a panel of the
// OrkStack dashboard, or listed in notDrawn with why.
func TestDashboards_EveryAreaSeriesIsDrawn(t *testing.T) {
	exported := map[string]bool{}
	for n := range metriccatalog.PrometheusNames(t, metriccatalog.Instruments(t)) {
		exported[metriccatalog.BaseName(n)] = true
	}
	// Declared names hold attribute keys too; a label is not a series.
	labels := metriccatalog.DeclaredAttributes(t)
	for n := range metriccatalog.DeclaredNames(t) {
		if !labels[n] {
			exported[metriccatalog.BaseName(n)] = true
		}
	}
	read := map[string]bool{}
	for _, p := range allPanels(dashboardsByUID(t)[UIDOrkStack]) {
		if datasourceType(p) != "prometheus" {
			continue
		}
		for _, expr := range exprsOf(p) {
			for _, n := range metriccatalog.MetricNames(expr) {
				read[metriccatalog.BaseName(n)] = true
			}
		}
	}
	checked := 0
	for _, n := range slices.Sorted(maps.Keys(exported)) {
		if !slices.ContainsFunc(areaPrefixes, func(p string) bool { return strings.HasPrefix(n, p) }) {
			continue
		}
		checked++
		if _, skip := notDrawn[n]; skip {
			if read[n] {
				t.Errorf("%s is listed as not drawn, but a panel reads it", n)
			}
			continue
		}
		if !read[n] {
			t.Errorf("%s is exported and no panel of the OrkStack dashboard reads it", n)
		}
	}
	if checked == 0 {
		t.Fatal("no area series found; the check has nothing to hold")
	}
	for n := range notDrawn {
		if !exported[n] {
			t.Errorf("notDrawn lists %s, which nothing exports", n)
		}
	}
}

// The area rows, in order, each named by the question it answers.
func TestDashboards_AreaRowsInOrder(t *testing.T) {
	_, _, rows := panelsOf(dashboardsByUID(t)[UIDOrkStack])
	var got []string
	for _, r := range rows {
		got = append(got, r["title"].(string))
	}
	want := []string{
		"Are runs and tasks succeeding?",
		"Are there enough Orkers for the work?",
		"Is the cache saving work?",
		"Are schedules firing on time?",
		"Are webhooks delivered?",
		"Are triggers starting runs?",
		"Are concurrency limits holding work back?",
		"Is the engine, Temporal and object storage healthy?",
		"Are retention cleanup and the artifact GC reclaiming space?",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("area rows:\n got %q\nwant %q", got, want)
	}
}

// Every row morePanels extends exists, so no panel is dropped by a title
// that matches no row.
func TestDashboards_MorePanelsFindTheirRows(t *testing.T) {
	titles := map[string]bool{}
	for _, r := range areaRows() {
		titles[r.title] = true
	}
	for title := range morePanels() {
		if !titles[title] {
			t.Errorf("morePanels extends %q, which is no row", title)
		}
	}
}
