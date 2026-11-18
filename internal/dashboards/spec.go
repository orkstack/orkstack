package dashboards

// The dashboards are written as tables of panels (orkstack.go, run.go), and
// build.go hands each one to grafana-foundation-sdk's builders, which own the
// JSON. These types are the tables' shape and nothing more.

// source is the datasource a query reads, always through the dashboard's
// datasource variable, never a fixed uid.
type source int

const (
	prom source = iota
	loki
	tempo
)

// kind is the panel's visualisation.
type kind int

const (
	statPanel kind = iota
	timeseriesPanel
	logsPanel
	tablePanel
)

// query is one target of a panel.
type query struct {
	expr    string
	legend  string
	instant bool
	// spans lists a TraceQL query's matching spans rather than its traces.
	spans bool
}

// threshold is one colour step of a stat: at value and above, colour.
type threshold struct {
	value float64
	color string
}

// link is a data link on a panel's values; with field set, on that field's
// values alone.
type link struct {
	field, title, url string
}

// panel is one panel. width is in grid columns of 24 and height in rows;
// zero is the kind's default.
type panel struct {
	kind          kind
	title         string
	description   string
	unit          string
	source        source
	queries       []query
	thresholds    []threshold
	noValue       string
	links         []link
	width, height uint32
}

// row is one collapsed area row, named by the question it answers.
type row struct {
	title  string
	panels []panel
}

// Units, Grafana's ids.
const (
	unitNone    = "none"
	unitShort   = "short"
	unitPercent = "percentunit"
	unitSeconds = "s"
	unitBytes   = "bytes"
	unitOps     = "ops"
	unitBool    = "bool_yes_no"
)

// stat, series, logs and table build a panel of their kind on Prometheus
// (logs on Loki).
func stat(title, description, unit string, queries ...query) panel {
	return panel{kind: statPanel, title: title, description: description, unit: unit, source: prom, queries: queries}
}

func series(title, description, unit string, queries ...query) panel {
	return panel{kind: timeseriesPanel, title: title, description: description, unit: unit, source: prom, queries: queries}
}

func logs(title, description string, queries ...query) panel {
	return panel{kind: logsPanel, title: title, description: description, unit: unitNone, source: loki, queries: queries}
}

func table(title, description, unit string, queries ...query) panel {
	return panel{kind: tablePanel, title: title, description: description, unit: unit, source: prom, queries: queries}
}

// q is a range query; qi an instant one.
func q(expr, legend string) query  { return query{expr: expr, legend: legend} }
func qi(expr, legend string) query { return query{expr: expr, legend: legend, instant: true} }

func (p panel) withThresholds(t ...threshold) panel { p.thresholds = t; return p }
func (p panel) withNoValue(s string) panel          { p.noValue = s; return p }
func (p panel) withLinks(l ...link) panel           { p.links = l; return p }
func (p panel) sized(w, h uint32) panel             { p.width, p.height = w, h; return p }
func (p panel) on(s source) panel                   { p.source = s; return p }
