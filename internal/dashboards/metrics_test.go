package dashboards

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	promclient "github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// instrument is one meter call internal/telemetry makes: its OTel name,
// kind and unit, read from the source.
type instrument struct{ name, method, unit string }

var instrumentCall = regexp.MustCompile(`^(Int64|Float64)(Counter|UpDownCounter|Histogram|Gauge|ObservableCounter|ObservableUpDownCounter|ObservableGauge)$`)

// telemetryInstruments reads every instrument internal/telemetry creates
// from its source: each meter call whose name is a string literal or a
// string constant of the package (concatenations included).
func telemetryInstruments(t *testing.T) []instrument {
	t.Helper()
	dir := "../telemetry"
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	must.OK(t, err)
	var files []*ast.File
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		must.OK(t, err)
		files = append(files, f)
	}
	consts := map[string]ast.Expr{}
	for _, f := range files {
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || g.Tok != token.CONST {
				continue
			}
			for _, spec := range g.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if i < len(vs.Values) {
						consts[n.Name] = vs.Values[i]
					}
				}
			}
		}
	}
	var eval func(ast.Expr) (string, bool)
	eval = func(e ast.Expr) (string, bool) {
		switch v := e.(type) {
		case *ast.BasicLit:
			if v.Kind == token.STRING {
				s, err := strconv.Unquote(v.Value)
				return s, err == nil
			}
		case *ast.Ident:
			if c, ok := consts[v.Name]; ok {
				return eval(c)
			}
		case *ast.BinaryExpr:
			if v.Op == token.ADD {
				a, ok1 := eval(v.X)
				b, ok2 := eval(v.Y)
				return a + b, ok1 && ok2
			}
		}
		return "", false
	}
	var out []instrument
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !instrumentCall.MatchString(sel.Sel.Name) {
				return true
			}
			name, ok := eval(call.Args[0])
			if !ok || !strings.Contains(name, ".") {
				return true
			}
			in := instrument{name: name, method: sel.Sel.Name}
			for _, a := range call.Args[1:] {
				if c, ok := a.(*ast.CallExpr); ok {
					if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "WithUnit" && len(c.Args) == 1 {
						in.unit, _ = eval(c.Args[0])
					}
				}
			}
			out = append(out, in)
			return true
		})
	}
	if len(out) < 50 {
		t.Fatalf("read %d instruments from %s; the reader has lost track of them", len(out), dir)
	}
	return out
}

func observeInt(v int64) metric.Int64Callback {
	return func(_ context.Context, o metric.Int64Observer) error { o.Observe(v); return nil }
}

func observeFloat(v float64) metric.Float64Callback {
	return func(_ context.Context, o metric.Float64Observer) error { o.Observe(v); return nil }
}

// prometheusNames is every series name the instruments get in Prometheus,
// produced by OTel's own Prometheus exporter, which applies the same
// translation Prometheus's OTLP receiver does.
func prometheusNames(t *testing.T, instruments []instrument) map[string]bool {
	t.Helper()
	reg := promclient.NewRegistry()
	exp, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo(), otelprom.WithoutTargetInfo())
	must.OK(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	meter := mp.Meter("catalog")
	ctx := context.Background()
	for _, in := range instruments {
		unit := metric.WithUnit(in.unit)
		var err error
		switch in.method {
		case "Int64Counter":
			var c metric.Int64Counter
			if c, err = meter.Int64Counter(in.name, unit); err == nil {
				c.Add(ctx, 1)
			}
		case "Float64Counter":
			var c metric.Float64Counter
			if c, err = meter.Float64Counter(in.name, unit); err == nil {
				c.Add(ctx, 1)
			}
		case "Int64UpDownCounter":
			var c metric.Int64UpDownCounter
			if c, err = meter.Int64UpDownCounter(in.name, unit); err == nil {
				c.Add(ctx, 1)
			}
		case "Float64UpDownCounter":
			var c metric.Float64UpDownCounter
			if c, err = meter.Float64UpDownCounter(in.name, unit); err == nil {
				c.Add(ctx, 1)
			}
		case "Int64Histogram":
			var h metric.Int64Histogram
			if h, err = meter.Int64Histogram(in.name, unit); err == nil {
				h.Record(ctx, 1)
			}
		case "Float64Histogram":
			var h metric.Float64Histogram
			if h, err = meter.Float64Histogram(in.name, unit); err == nil {
				h.Record(ctx, 1)
			}
		case "Int64Gauge":
			var g metric.Int64Gauge
			if g, err = meter.Int64Gauge(in.name, unit); err == nil {
				g.Record(ctx, 1)
			}
		case "Float64Gauge":
			var g metric.Float64Gauge
			if g, err = meter.Float64Gauge(in.name, unit); err == nil {
				g.Record(ctx, 1)
			}
		case "Int64ObservableCounter":
			_, err = meter.Int64ObservableCounter(in.name, unit, metric.WithInt64Callback(observeInt(1)))
		case "Float64ObservableCounter":
			_, err = meter.Float64ObservableCounter(in.name, unit, metric.WithFloat64Callback(observeFloat(1)))
		case "Int64ObservableUpDownCounter":
			_, err = meter.Int64ObservableUpDownCounter(in.name, unit, metric.WithInt64Callback(observeInt(1)))
		case "Float64ObservableUpDownCounter":
			_, err = meter.Float64ObservableUpDownCounter(in.name, unit, metric.WithFloat64Callback(observeFloat(1)))
		case "Int64ObservableGauge":
			_, err = meter.Int64ObservableGauge(in.name, unit, metric.WithInt64Callback(observeInt(1)))
		case "Float64ObservableGauge":
			_, err = meter.Float64ObservableGauge(in.name, unit, metric.WithFloat64Callback(observeFloat(1)))
		}
		must.OKf(t, err, "instrument %s", in.name)
	}
	families, err := reg.Gather()
	must.OK(t, err)
	names := map[string]bool{}
	for _, f := range families {
		n := f.GetName()
		names[n] = true
		if f.GetType().String() == "HISTOGRAM" {
			for _, s := range []string{"_bucket", "_sum", "_count"} {
				names[n+s] = true
			}
		}
	}
	return names
}

// externalMetrics are the series the dashboards read that OrkStack's own
// instruments do not name: Prometheus's alert series, and the Temporal
// SDK's client metrics (internal/telemetry.TemporalMetricsHandler turns
// them on; their names are the SDK's).
var externalMetrics = map[string]bool{
	"ALERTS":                         true,
	"temporal_request_total":         true,
	"temporal_request_failure_total": true,
}

var (
	promStrip = []*regexp.Regexp{
		regexp.MustCompile(`"(\\.|[^"])*"`),
		regexp.MustCompile(`\{[^}]*\}`),
		regexp.MustCompile(`\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)`),
		regexp.MustCompile(`\[[^\]]*\]`),
		regexp.MustCompile(`\$[a-z_]+`),
	}
	labelValues = regexp.MustCompile(`label_values\(\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*,\s*[a-zA-Z_]+\s*\)`)
	identifier  = regexp.MustCompile(`[a-zA-Z_:][a-zA-Z0-9_:]*(\s*\()?`)
	promKeyword = map[string]bool{"and": true, "or": true, "unless": true, "bool": true, "offset": true}
)

// metricNames extracts the series a PromQL expression reads: every
// identifier left once strings, label matchers, grouping clauses, ranges and
// variables are removed, other than a function's name.
func metricNames(expr string) []string {
	for _, re := range promStrip {
		expr = re.ReplaceAllString(expr, " ")
	}
	expr = labelValues.ReplaceAllString(expr, "$1")
	var out []string
	for _, m := range identifier.FindAllString(expr, -1) {
		if strings.HasSuffix(strings.TrimSpace(m), "(") || promKeyword[m] {
			continue
		}
		out = append(out, m)
	}
	return out
}

// The extraction itself: a matcher's label, a grouping label and a function
// are never read as a series.
func TestDashboards_MetricNamesOfAnExpression(t *testing.T) {
	got := metricNames(`count((max by (ork_pool) (ork_pool_backlog_tasks{ork_pool=~"$pool"}) > 0) unless on (ork_pool) (sum(rate(ork_runs_total[5m])) > 0)) or vector(0)`)
	want := []string{"ork_pool_backlog_tasks", "ork_runs_total"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("metricNames = %v; want %v", got, want)
	}
	if got := metricNames(`label_values(ork_runs_total{ork_namespace=~"$namespace"}, ork_orkflow_name)`); strings.Join(got, ",") != "ork_runs_total" {
		t.Fatalf("label_values: %v; want ork_runs_total", got)
	}
}

// declaredNames is every ork.* name internal/telemetry spells out, as
// Prometheus base names (dots to underscores). Some instruments are built
// through helpers and tables the call reader cannot follow; their names are
// still declared here.
func declaredNames(t *testing.T) map[string]bool {
	t.Helper()
	name := regexp.MustCompile(`"(ork\.[a-z0-9_.]+[a-z0-9_])"`)
	entries, err := os.ReadDir("../telemetry")
	must.OK(t, err)
	out := map[string]bool{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join("../telemetry", e.Name()))
		must.OK(t, err)
		for _, m := range name.FindAllStringSubmatch(string(b), -1) {
			out[strings.ReplaceAll(m[1], ".", "_")] = true
		}
	}
	return out
}

// baseName strips what Prometheus appends to an OTel name: a histogram's
// series suffix, a counter's _total and a unit's suffix.
func baseName(n string) string {
	for _, s := range []string{"_bucket", "_sum", "_count"} {
		n = strings.TrimSuffix(n, s)
	}
	n = strings.TrimSuffix(n, "_total")
	for _, s := range []string{"_per_second", "_seconds", "_bytes", "_ratio"} {
		n = strings.TrimSuffix(n, s)
	}
	return n
}

// Every series a dashboard queries is one OrkStack exports, under the name
// Prometheus gives it, or one named in externalMetrics. An instrument whose
// meter call is read is held to its exact name; one built through a helper
// is held to its declared name. The observability gate holds every panel to
// a real answer.
func TestDashboards_MetricsExist(t *testing.T) {
	instruments := telemetryInstruments(t)
	names := prometheusNames(t, instruments)
	exact := map[string]bool{}
	for _, in := range instruments {
		exact[strings.ReplaceAll(in.name, ".", "_")] = true
	}
	declared := declaredNames(t)
	checked := 0
	for _, expr := range promQL(t) {
		for _, n := range metricNames(expr) {
			checked++
			base := baseName(n)
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

// declaredAttributes is every datapoint attribute internal/telemetry
// records, as Prometheus labels: the values of its Attr* constants, less the
// resource attributes process_identity.go declares, which the kit's
// Prometheus does not turn into labels.
func declaredAttributes(t *testing.T) map[string]bool {
	t.Helper()
	attr := regexp.MustCompile(`\bAttr[A-Za-z]*\s*=\s*"([^"]+)"`)
	entries, err := os.ReadDir("../telemetry")
	must.OK(t, err)
	out := map[string]bool{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") || e.Name() == "process_identity.go" {
			continue
		}
		b, err := os.ReadFile(filepath.Join("../telemetry", e.Name()))
		must.OK(t, err)
		for _, m := range attr.FindAllStringSubmatch(string(b), -1) {
			out[strings.ReplaceAll(m[1], ".", "_")] = true
		}
	}
	return out
}

// promLabels are Prometheus's own labels, which no instrument records.
var promLabels = map[string]bool{"le": true, "alertname": true, "alertstate": true, "severity": true}

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
	known := declaredAttributes(t)
	checked := 0
	use := func(where, label string) {
		checked++
		if !known[label] && !promLabels[label] {
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
