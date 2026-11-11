package telemetry

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"maps"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestBootZeroConfig confirms Boot, given no environment at all, returns a
// Config that reports nothing enabled and a Sinks whose Default() is the
// same no-op Providers SetupSinks alone would give — Boot adds no behavior
// of its own beyond composing ConfigFromEnv and SetupSinks.
func TestBootZeroConfig(t *testing.T) {
	cfg, sinks, err := Boot(ServiceOrker)
	must.OKf(t, err, "Boot")
	if cfg.Enabled() {
		t.Errorf("Config.Enabled() = true for a bare environment")
	}
	if sinks.Default() == nil {
		t.Fatal("Sinks.Default() = nil")
	}
	if sinks.Default().Enabled() {
		t.Errorf("Sinks.Default().Enabled() = true for a bare environment")
	}
}

// TestClientOptionsZeroConfigIsByteIdentical is ClientOptions' load-bearing
// promise: with telemetry off, the returned client.Options must be exactly
// what both cmd/ork and cmd/orker dialed with before either had a
// MetricsHandler or Interceptors to set — client.Options{HostPort, Namespace,
// Logger} — plus DisableWorkerEnvironmentInfo, nothing more.
func TestClientOptionsZeroConfigIsByteIdentical(t *testing.T) {
	p, err := Setup(context.Background(), Config{})
	must.OKf(t, err, "Setup")
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	got, err := ClientOptions("127.0.0.1:7233", "default", p, logger)
	must.OKf(t, err, "ClientOptions")
	want := client.Options{
		HostPort:                     "127.0.0.1:7233",
		Namespace:                    "default",
		Logger:                       logger,
		DisableWorkerEnvironmentInfo: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ClientOptions on disabled telemetry = %+v, want %+v (the historical literal, plus no worker environment info)", got, want)
	}
}

// TestClientOptionsMetricsAndTracesOnSetsHandlerAndInterceptors is the
// non-zero-config half: with metrics and traces on, MetricsHandler and
// Interceptors are populated instead of left zero.
func TestClientOptionsMetricsAndTracesOnSetsHandlerAndInterceptors(t *testing.T) {
	p, err := Setup(context.Background(), Config{
		MetricsExporters: []string{ExporterPrometheus},
		TracesExporter:   ExporterOTLP,
		AdminAddr:        "127.0.0.1:0",
	})
	must.OKf(t, err, "Setup")
	defer p.Shutdown(context.Background())

	got, err := ClientOptions("127.0.0.1:7233", "default", p, slog.Default())
	must.OKf(t, err, "ClientOptions")
	if got.MetricsHandler == nil {
		t.Error("MetricsHandler = nil, want a handler (metrics are on)")
	}
	if len(got.Interceptors) == 0 {
		t.Error("Interceptors = empty, want the tracing interceptor (traces are on)")
	}
	if !got.DisableWorkerEnvironmentInfo {
		t.Error("DisableWorkerEnvironmentInfo = false with telemetry on, want true: no telemetry setting asks for the worker's environment to be reported")
	}
}

// TestClientOptions_IsHowOrkAndOrkerDialTemporal holds ClientOptions'
// settings, DisableWorkerEnvironmentInfo among them, on every Temporal client
// the engine and the Orker build. Over every non-test Go file of cmd/,
// internal/ and pkg/ (the test-only internal/testsupport aside):
//
//  1. a Temporal client is dialed only in cmd/ork/main.go and
//     cmd/orker/main.go, once each;
//  2. the options each dials with are a variable assigned only from
//     telemetry.ClientOptions in that file;
//  3. no client.Options literal is written outside ClientOptions, and nothing
//     outside it names DisableWorkerEnvironmentInfo, so no caller can switch
//     the reporting back on.
func TestClientOptions_IsHowOrkAndOrkerDialTemporal(t *testing.T) {
	root, err := filepath.Abs("../..")
	must.OKf(t, err, "module root")
	const (
		sdkClient = "go.temporal.io/sdk/client"
		home      = "internal/telemetry/boot.go"
	)
	dialers := map[string]bool{
		"Dial": true, "DialContext": true, "NewClient": true, "NewLazyClient": true,
		"NewClientFromExisting": true, "NewClientFromExistingWithContext": true,
	}
	dials := map[string]int{}

	fset := token.NewFileSet()
	for _, dir := range []string{"cmd", "internal", "pkg"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if entry.IsDir() {
				if rel == "internal/testsupport" || entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			local := ""
			for _, spec := range file.Imports {
				if importPath, _ := strconv.Unquote(spec.Path.Value); importPath == sdkClient {
					local = "client"
					if spec.Name != nil {
						local = spec.Name.Name
					}
				}
			}
			ast.Inspect(file, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "DisableWorkerEnvironmentInfo" && rel != home {
					t.Errorf("%s: names DisableWorkerEnvironmentInfo; only telemetry.ClientOptions may set it", fset.Position(sel.Pos()))
				}
				if local == "" {
					return true
				}
				switch n := n.(type) {
				case *ast.CompositeLit:
					if isSelector(n.Type, local, "Options") && rel != home {
						t.Errorf("%s: writes a client.Options literal; build the options with telemetry.ClientOptions", fset.Position(n.Pos()))
					}
				case *ast.CallExpr:
					fun, ok := n.Fun.(*ast.SelectorExpr)
					if !ok || !dialers[fun.Sel.Name] || !isSelector(fun, local, fun.Sel.Name) || len(n.Args) == 0 {
						return true
					}
					dials[rel]++
					arg, ok := n.Args[len(n.Args)-1].(*ast.Ident)
					if !ok || !onlyFromClientOptions(file, arg.Name) {
						t.Errorf("%s: dials Temporal with options not built by telemetry.ClientOptions", fset.Position(n.Pos()))
					}
				}
				return true
			})
			return nil
		})
		must.OKf(t, err, "walk %s", dir)
	}
	if want := map[string]int{"cmd/ork/main.go": 1, "cmd/orker/main.go": 1}; !maps.Equal(dials, want) {
		t.Errorf("Temporal clients dialed per file = %v, want %v", dials, want)
	}
}

// isSelector reports whether expr is pkg.name.
func isSelector(expr ast.Expr, pkg, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	x, ok := sel.X.(*ast.Ident)
	return ok && x.Name == pkg
}

// onlyFromClientOptions reports whether file assigns the variable name at
// least once and only ever from a telemetry.ClientOptions call, and never
// declares it with var.
func onlyFromClientOptions(file *ast.File, name string) bool {
	assigned, other := false, false
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name == name {
					call, ok := n.Rhs[0].(*ast.CallExpr)
					if len(n.Rhs) == 1 && ok && isSelector(call.Fun, "telemetry", "ClientOptions") {
						assigned = true
					} else {
						other = true
					}
				}
			}
		case *ast.ValueSpec:
			for _, id := range n.Names {
				if id.Name == name {
					other = true
				}
			}
		}
		return true
	})
	return assigned && !other
}

// TestLogBootSkipsWhenDisabled confirms LogBoot logs nothing at all for a
// Config with nothing enabled — the same silent-by-default promise every
// other entry point in this package makes.
func TestLogBootSkipsWhenDisabled(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	sinks, err := SetupSinks(context.Background(), Config{})
	must.OKf(t, err, "SetupSinks")
	LogBoot(logger, Config{}, sinks, "")
	if buf.Len() != 0 {
		t.Errorf("LogBoot logged %q for a disabled Config, want nothing", buf.String())
	}
}

// TestLogBootLogsPerSinkStatus confirms LogBoot logs the boot summary plus
// one "telemetry sink ready" line per sink when telemetry is on.
func TestLogBootLogsPerSinkStatus(t *testing.T) {
	table, err := ParseSinks([]byte("sinks:\n  datadog:\n    logs_endpoint: http://127.0.0.1:1\n"))
	must.OKf(t, err, "ParseSinks")
	must.OKf(t, table.Resolve(nil, nil), "Resolve")
	cfg := Config{LogsExporter: ExporterOTLP, Sinks: table}
	sinks, err := SetupSinks(context.Background(), cfg)
	must.OKf(t, err, "SetupSinks")
	defer sinks.Shutdown(context.Background())

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	LogBoot(logger, cfg, sinks, "127.0.0.1:9090")

	logged := buf.String()
	if !containsAll(logged, "telemetry", "telemetry sink ready", "sink=default", "sink=datadog") {
		t.Errorf("LogBoot output = %q, want a telemetry line plus one per-sink line", logged)
	}
}
