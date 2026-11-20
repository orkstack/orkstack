// Package metriccatalog reads, from internal/telemetry's source, the metric
// instruments OrkStack exports and the names Prometheus gives them, so a test
// can hold a dashboard query or an alert rule to series that exist. It is for
// tests only: every helper stops the test on an error.
package metriccatalog

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	promclient "github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TelemetryDir is internal/telemetry's directory, found from this file's own
// path so any package's test can read it.
func TelemetryDir(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("metriccatalog: cannot locate its own source")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "telemetry")
}

// Instrument is one meter call internal/telemetry makes: its OTel name,
// kind and unit, read from the source.
type Instrument struct{ Name, Method, Unit string }

var instrumentCall = regexp.MustCompile(`^(Int64|Float64)(Counter|UpDownCounter|Histogram|Gauge|ObservableCounter|ObservableUpDownCounter|ObservableGauge)$`)

// Instruments reads every instrument internal/telemetry creates
// from its source: each meter call whose name is a string literal or a
// string constant of the package (concatenations included).
func Instruments(t testing.TB) []Instrument {
	t.Helper()
	dir := TelemetryDir(t)
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
	var out []Instrument
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
			in := Instrument{Name: name, Method: sel.Sel.Name}
			for _, a := range call.Args[1:] {
				if c, ok := a.(*ast.CallExpr); ok {
					if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "WithUnit" && len(c.Args) == 1 {
						in.Unit, _ = eval(c.Args[0])
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

// PrometheusNames is every series name the instruments get in Prometheus,
// produced by OTel's own Prometheus exporter, which applies the same
// translation Prometheus's OTLP receiver does.
func PrometheusNames(t testing.TB, instruments []Instrument) map[string]bool {
	t.Helper()
	reg := promclient.NewRegistry()
	exp, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo(), otelprom.WithoutTargetInfo())
	must.OK(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	meter := mp.Meter("catalog")
	ctx := context.Background()
	for _, in := range instruments {
		unit := metric.WithUnit(in.Unit)
		var err error
		switch in.Method {
		case "Int64Counter":
			var c metric.Int64Counter
			if c, err = meter.Int64Counter(in.Name, unit); err == nil {
				c.Add(ctx, 1)
			}
		case "Float64Counter":
			var c metric.Float64Counter
			if c, err = meter.Float64Counter(in.Name, unit); err == nil {
				c.Add(ctx, 1)
			}
		case "Int64UpDownCounter":
			var c metric.Int64UpDownCounter
			if c, err = meter.Int64UpDownCounter(in.Name, unit); err == nil {
				c.Add(ctx, 1)
			}
		case "Float64UpDownCounter":
			var c metric.Float64UpDownCounter
			if c, err = meter.Float64UpDownCounter(in.Name, unit); err == nil {
				c.Add(ctx, 1)
			}
		case "Int64Histogram":
			var h metric.Int64Histogram
			if h, err = meter.Int64Histogram(in.Name, unit); err == nil {
				h.Record(ctx, 1)
			}
		case "Float64Histogram":
			var h metric.Float64Histogram
			if h, err = meter.Float64Histogram(in.Name, unit); err == nil {
				h.Record(ctx, 1)
			}
		case "Int64Gauge":
			var g metric.Int64Gauge
			if g, err = meter.Int64Gauge(in.Name, unit); err == nil {
				g.Record(ctx, 1)
			}
		case "Float64Gauge":
			var g metric.Float64Gauge
			if g, err = meter.Float64Gauge(in.Name, unit); err == nil {
				g.Record(ctx, 1)
			}
		case "Int64ObservableCounter":
			_, err = meter.Int64ObservableCounter(in.Name, unit, metric.WithInt64Callback(observeInt(1)))
		case "Float64ObservableCounter":
			_, err = meter.Float64ObservableCounter(in.Name, unit, metric.WithFloat64Callback(observeFloat(1)))
		case "Int64ObservableUpDownCounter":
			_, err = meter.Int64ObservableUpDownCounter(in.Name, unit, metric.WithInt64Callback(observeInt(1)))
		case "Float64ObservableUpDownCounter":
			_, err = meter.Float64ObservableUpDownCounter(in.Name, unit, metric.WithFloat64Callback(observeFloat(1)))
		case "Int64ObservableGauge":
			_, err = meter.Int64ObservableGauge(in.Name, unit, metric.WithInt64Callback(observeInt(1)))
		case "Float64ObservableGauge":
			_, err = meter.Float64ObservableGauge(in.Name, unit, metric.WithFloat64Callback(observeFloat(1)))
		}
		must.OKf(t, err, "instrument %s", in.Name)
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

var (
	promStrip = []*regexp.Regexp{
		regexp.MustCompile(`"(\\.|[^"])*"`),
		regexp.MustCompile(`\{[^}]*\}`),
		regexp.MustCompile(`\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)`),
		regexp.MustCompile(`\[[^\]]*\]`),
		regexp.MustCompile(`\$[a-z_]+`),
		// number and duration literals: 0.5, 1e-9, offset 15m
		regexp.MustCompile(`\b[0-9][0-9a-zA-Z.]*(?:[eE][+-]?[0-9]+)?`),
	}
	labelValues = regexp.MustCompile(`label_values\(\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*,\s*[a-zA-Z_]+\s*\)`)
	identifier  = regexp.MustCompile(`[a-zA-Z_:][a-zA-Z0-9_:]*(\s*\()?`)
	promKeyword = map[string]bool{"and": true, "or": true, "unless": true, "bool": true, "offset": true}
)

// MetricNames extracts the series a PromQL expression reads: every
// identifier left once strings, label matchers, grouping clauses, ranges and
// variables are removed, other than a function's name.
func MetricNames(expr string) []string {
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

// DeclaredNames is every ork.* name internal/telemetry spells out, as
// Prometheus base names (dots to underscores). Some instruments are built
// through helpers and tables the call reader cannot follow; their names are
// still declared here.
func DeclaredNames(t testing.TB) map[string]bool {
	t.Helper()
	name := regexp.MustCompile(`"(ork\.[a-z0-9_.]+[a-z0-9_])"`)
	entries, err := os.ReadDir(TelemetryDir(t))
	must.OK(t, err)
	out := map[string]bool{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(TelemetryDir(t), e.Name()))
		must.OK(t, err)
		for _, m := range name.FindAllStringSubmatch(string(b), -1) {
			out[strings.ReplaceAll(m[1], ".", "_")] = true
		}
	}
	return out
}

// BaseName strips what Prometheus appends to an OTel name: a histogram's
// series suffix, a counter's _total and a unit's suffix.
func BaseName(n string) string {
	for _, s := range []string{"_bucket", "_sum", "_count"} {
		n = strings.TrimSuffix(n, s)
	}
	n = strings.TrimSuffix(n, "_total")
	for _, s := range []string{"_per_second", "_seconds", "_bytes", "_ratio"} {
		n = strings.TrimSuffix(n, s)
	}
	return n
}

// DeclaredAttributes is every datapoint attribute internal/telemetry
// records, as Prometheus labels: the values of its Attr* constants, less the
// resource attributes process_identity.go declares, which the kit's
// Prometheus does not turn into labels.
func DeclaredAttributes(t testing.TB) map[string]bool {
	t.Helper()
	attr := regexp.MustCompile(`\bAttr[A-Za-z]*\s*=\s*"([^"]+)"`)
	entries, err := os.ReadDir(TelemetryDir(t))
	must.OK(t, err)
	out := map[string]bool{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") || e.Name() == "process_identity.go" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(TelemetryDir(t), e.Name()))
		must.OK(t, err)
		for _, m := range attr.FindAllStringSubmatch(string(b), -1) {
			out[strings.ReplaceAll(m[1], ".", "_")] = true
		}
	}
	return out
}

// PromLabels are Prometheus's own labels, which no instrument records: a
// histogram's le, the alert series' labels, and job, which the kit's
// Prometheus derives from the OTLP resource's service.name
// (deploy/observability/prometheus.yml): one value per binary. instance,
// one per process, is not one a dashboard groups by.
var PromLabels = map[string]bool{"le": true, "alertname": true, "alertstate": true, "severity": true, "job": true}
