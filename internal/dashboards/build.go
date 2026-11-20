package dashboards

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"maps"
	"slices"

	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/cog/variants"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	heatmappanel "github.com/grafana/grafana-foundation-sdk/go/heatmap"
	logspanel "github.com/grafana/grafana-foundation-sdk/go/logs"
	lokiq "github.com/grafana/grafana-foundation-sdk/go/loki"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	statpanel "github.com/grafana/grafana-foundation-sdk/go/stat"
	tablepanel "github.com/grafana/grafana-foundation-sdk/go/table"
	tempoq "github.com/grafana/grafana-foundation-sdk/go/tempo"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// The datasource variables every panel reads through: never a fixed uid, so
// a deployment points the kit at its own backends.
var datasources = map[source]common.DataSourceRef{
	prom:  {Type: cog.ToPtr("prometheus"), Uid: cog.ToPtr("${prom}")},
	loki:  {Type: cog.ToPtr("loki"), Uid: cog.ToPtr("${loki}")},
	tempo: {Type: cog.ToPtr("tempo"), Uid: cog.ToPtr("${tempo}")},
}

// defaultUIURL is $ui_url's value in the local kit: the engine serves its UI
// on its API listener.
const defaultUIURL = "http://localhost:8080"

func str(s string) dashboard.StringOrMap { return dashboard.StringOrMap{String: &s} }

func datasourceVariable(name, label, pluginType string) cog.Builder[dashboard.VariableModel] {
	return dashboard.NewDatasourceVariableBuilder(name).Label(label).Type(pluginType)
}

func uiURLVariable() cog.Builder[dashboard.VariableModel] {
	return dashboard.NewConstantVariableBuilder("ui_url").
		Description("The OrkStack UI's address, for links to a run's page.").
		Value(str(defaultUIURL))
}

// labelVariable is a multi-value variable of one label's values, with All.
func labelVariable(name, label, query string) cog.Builder[dashboard.VariableModel] {
	return dashboard.NewQueryVariableBuilder(name).Label(label).
		Datasource(datasources[prom]).
		Query(str(query)).Definition(query).
		Refresh(dashboard.VariableRefreshOnTimeRangeChanged).
		Sort(dashboard.VariableSortAlphabeticalAsc).
		Multi(true).IncludeAll(true).AllValue(".*")
}

// refID is the id of a panel's i-th query: A, B, C...
func refID(i int) string { return string(rune('A' + i)) }

func target(p panel, i int, qy query) cog.Builder[variants.Dataquery] {
	ref := refID(i)
	ds := datasources[p.source]
	switch p.source {
	case loki:
		return lokiq.NewDataqueryBuilder().RefId(ref).Datasource(ds).Expr(qy.expr).LegendFormat(qy.legend)
	case tempo:
		b := tempoq.NewDataqueryBuilder().RefId(ref).Datasource(ds).QueryType("traceql").Query(qy.expr).Limit(50)
		if qy.spans {
			b = b.TableType(tempoq.SearchTableTypeSpans)
		}
		return b
	default:
		b := prometheus.NewDataqueryBuilder().RefId(ref).Datasource(ds).Expr(qy.expr).LegendFormat(qy.legend)
		if p.kind == heatmapPanel {
			b = b.Format(prometheus.PromQueryFormatHeatmap).Exemplar(true)
		}
		if qy.instant {
			b = b.Instant()
		} else {
			b = b.Range()
		}
		return b
	}
}

func thresholds(p panel) cog.Builder[dashboard.ThresholdsConfig] {
	steps := make([]dashboard.Threshold, len(p.thresholds))
	for i, t := range p.thresholds {
		steps[i] = dashboard.Threshold{Color: t.color}
		if i > 0 {
			steps[i].Value = cog.ToPtr(t.value)
		}
	}
	return dashboard.NewThresholdsConfigBuilder().Mode(dashboard.ThresholdsModeAbsolute).Steps(steps)
}

// dataLinks are the panel's links that apply to every field.
func dataLinks(p panel) []cog.Builder[dashboard.DashboardLink] {
	var out []cog.Builder[dashboard.DashboardLink]
	for _, l := range p.links {
		if l.field == "" {
			out = append(out, dashboard.NewDashboardLinkBuilder(l.title).Url(l.url))
		}
	}
	return out
}

// fieldLinks are the panel's links that apply to one field each, as
// overrides by field name.
func fieldLinks(p panel) map[string][]dashboard.DynamicConfigValue {
	out := map[string][]dashboard.DynamicConfigValue{}
	for _, l := range p.links {
		if l.field != "" {
			out[l.field] = append(out[l.field], dashboard.DynamicConfigValue{
				Id: "links", Value: []map[string]any{{"title": l.title, "url": l.url}},
			})
		}
	}
	return out
}

// sizeOf is the panel's grid size: its own, or its kind's default.
func sizeOf(p panel) (w, h uint32) {
	w, h = p.width, p.height
	if w == 0 {
		w = map[kind]uint32{statPanel: 4, timeseriesPanel: 12, logsPanel: 24, tablePanel: 24, heatmapPanel: 12}[p.kind]
	}
	if h == 0 {
		h = map[kind]uint32{statPanel: 4, timeseriesPanel: 8, logsPanel: 8, tablePanel: 8, heatmapPanel: 8}[p.kind]
	}
	return w, h
}

// build hands one panel to the SDK's builder of its kind.
func build(p panel) cog.Builder[dashboard.Panel] {
	var targets []cog.Builder[variants.Dataquery]
	for i, qy := range p.queries {
		targets = append(targets, target(p, i, qy))
	}
	w, h := sizeOf(p)
	ds := datasources[p.source]
	switch p.kind {
	case statPanel:
		b := statpanel.NewPanelBuilder().Title(p.title).Description(p.description).Unit(p.unit).
			Datasource(ds).Targets(targets).Span(w).Height(h).
			GraphMode(common.BigValueGraphModeNone).ColorMode(common.BigValueColorModeValue)
		if len(p.thresholds) > 0 {
			b = b.Thresholds(thresholds(p))
		}
		if p.noValue != "" {
			b = b.NoValue(p.noValue)
		}
		if len(p.links) > 0 {
			b = b.DataLinks(dataLinks(p))
		}
		return b
	case logsPanel:
		b := logspanel.NewPanelBuilder().Title(p.title).Description(p.description).Unit(p.unit).
			Datasource(ds).Targets(targets).Span(w).Height(h).
			ShowTime(true).SortOrder(common.LogsSortOrderDescending).EnableLogDetails(true)
		if links := dataLinks(p); len(links) > 0 {
			b = b.DataLinks(links)
		}
		links := fieldLinks(p)
		for _, field := range slices.Sorted(maps.Keys(links)) {
			b = b.OverrideByName(field, links[field])
		}
		return b
	case heatmapPanel:
		// The buckets are the histogram's own, so Grafana does not bucket
		// again; exemplars are drawn in the accent colour.
		return heatmappanel.NewPanelBuilder().Title(p.title).Description(p.description).Unit(p.unit).
			Datasource(ds).Targets(targets).Span(w).Height(h).
			Calculate(false).ExemplarsColor(colorAccent).
			YAxis(heatmappanel.NewYAxisConfigBuilder().Unit(p.unit))
	case tablePanel:
		return tablepanel.NewPanelBuilder().Title(p.title).Description(p.description).Unit(p.unit).
			Datasource(ds).Targets(targets).Span(w).Height(h).DataLinks(dataLinks(p))
	default:
		b := timeseries.NewPanelBuilder().Title(p.title).Description(p.description).Unit(p.unit).
			Datasource(ds).Targets(targets).Span(w).Height(h).
			Legend(common.NewVizLegendOptionsBuilder().
				DisplayMode(common.LegendDisplayModeTable).Placement(common.LegendPlacementBottom).ShowLegend(true)).
			DataLinks(dataLinks(p))
		for i, qy := range p.queries {
			if qy.dashed {
				b = b.OverrideByQuery(refID(i), []dashboard.DynamicConfigValue{{
					Id: "custom.lineStyle", Value: map[string]any{"fill": "dash", "dash": []float64{10, 10}},
				}})
			}
		}
		return b
	}
}

//lint:ignore SA1019 the classic dashboard model is what Grafana's file provisioning loads without feature toggles; the v2 schema the SDK prefers needs them
func orkStackDashboard() *dashboard.DashboardBuilder {
	//lint:ignore SA1019 the classic dashboard model is what Grafana's file provisioning loads without feature toggles; the v2 schema the SDK prefers needs them
	b := dashboard.NewDashboardBuilder("OrkStack").Uid(UIDOrkStack).
		Description("The whole deployment: runs, Orkers and pools, the cache, schedules, webhooks, HTTP triggers, concurrency limits, the engine and its dependencies, and retention cleanup with the artifact GC.").
		Tags([]string{"orkstack"}).Readonly().Timezone(common.TimeZoneBrowser).
		Time("now-6h", "now").Refresh("1m").Tooltip(dashboard.DashboardCursorSyncCrosshair).
		WithVariable(datasourceVariable("prom", "Metrics", "prometheus")).
		WithVariable(datasourceVariable("loki", "Logs", "loki")).
		WithVariable(datasourceVariable("tempo", "Traces", "tempo")).
		WithVariable(labelVariable("namespace", "Namespace", `label_values(ork_runs_total, ork_namespace)`)).
		WithVariable(labelVariable("orkflow", "Orkflow", `label_values(ork_runs_total{ork_namespace=~"$namespace"}, ork_orkflow_name)`)).
		WithVariable(labelVariable("pool", "Pool", `label_values(ork_pool_backlog_tasks, ork_pool)`)).
		WithVariable(uiURLVariable())
	for _, p := range topRow() {
		b = b.WithPanel(build(p))
	}
	for _, r := range areaRows() {
		row := dashboard.NewRowBuilder(r.title).Collapsed(true)
		for _, p := range r.panels {
			row = row.WithPanel(build(p))
		}
		b = b.WithRow(row)
	}
	return b
}

//lint:ignore SA1019 the classic dashboard model is what Grafana's file provisioning loads without feature toggles; the v2 schema the SDK prefers needs them
func runDashboard() *dashboard.DashboardBuilder {
	//lint:ignore SA1019 the classic dashboard model is what Grafana's file provisioning loads without feature toggles; the v2 schema the SDK prefers needs them
	b := dashboard.NewDashboardBuilder("OrkStack / Run").Uid(UIDRun).
		Description("One run: its spans, the engine's events, its attempts' usage against their limits, its tasks' logs and its webhook deliveries.").
		Tags([]string{"orkstack"}).Readonly().Timezone(common.TimeZoneBrowser).
		Time("now-6h", "now").
		WithVariable(datasourceVariable("loki", "Logs", "loki")).
		WithVariable(datasourceVariable("tempo", "Traces", "tempo")).
		WithVariable(dashboard.NewTextBoxVariableBuilder("run").Label("Run").
			Description("The run's id, as the OrkStack UI and orkctl show it.")).
		WithVariable(dashboard.NewTextBoxVariableBuilder("task").Label("Task").
			Description("A pattern of the task ids to show; every task by default.").DefaultValue(str(".+"))).
		WithVariable(uiURLVariable()).
		Link(dashboard.NewDashboardLinkBuilder("Open the run in OrkStack").
			Type(dashboard.DashboardLinkTypeLink).Url("${ui_url}/runs/${run}").TargetBlank(true))
	for _, p := range runPanels() {
		b = b.WithPanel(build(p))
	}
	return b
}

// All builds every dashboard the kit provisions.
//
//lint:ignore SA1019 the classic dashboard model is what Grafana's file provisioning loads without feature toggles; the v2 schema the SDK prefers needs them
func All() ([]dashboard.Dashboard, error) {
	//lint:ignore SA1019 the classic dashboard model is what Grafana's file provisioning loads without feature toggles; the v2 schema the SDK prefers needs them
	var out []dashboard.Dashboard
	//lint:ignore SA1019 the classic dashboard model is what Grafana's file provisioning loads without feature toggles; the v2 schema the SDK prefers needs them
	for _, b := range []*dashboard.DashboardBuilder{orkStackDashboard(), runDashboard()} {
		d, err := b.Build()
		if err != nil {
			return nil, fmt.Errorf("build dashboard: %w", err)
		}
		out = append(out, d)
	}
	return out, nil
}

// JSON is each dashboard's provisioned file, by uid: indented, with a
// trailing newline, so the committed files diff line by line.
func JSON() (map[string][]byte, error) {
	all, err := All()
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for _, d := range all {
		b, err := json.Marshal(d, jsontext.WithIndent("  "))
		if err != nil {
			return nil, fmt.Errorf("encode dashboard %s: %w", *d.Uid, err)
		}
		out[*d.Uid] = append(b, '\n')
	}
	return out, nil
}
