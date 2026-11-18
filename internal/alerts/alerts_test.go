package alerts

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	tracenoop "go.opentelemetry.io/otel/trace/noop"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/metriccatalog"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const repoRoot = "../.."

// exprs is every rule's expression, by the rule's full name.
func exprs() map[string]string {
	out := map[string]string{}
	for _, r := range Rules() {
		out[Prefix+r.Name] = r.Expr
	}
	for _, r := range Records() {
		out[r.Name] = r.Expr
	}
	return out
}

// Every generated file is the generator's output, and the rules directory
// holds nothing else.
func TestAlerts_AreGenerated(t *testing.T) {
	files, err := Files()
	must.OK(t, err)
	dir := filepath.Join(repoRoot, RulesDir)
	for name, want := range files {
		if name == RunbooksFile {
			doc, err := os.ReadFile(filepath.Join(repoRoot, RunbooksFile))
			must.OK(t, err)
			spliced, err := Splice(string(doc), string(want))
			must.OK(t, err)
			if spliced != string(doc) {
				t.Errorf("%s is stale: run go run ./internal/alerts/cmd/genalerts", RunbooksFile)
			}
			continue
		}
		got, err := os.ReadFile(filepath.Join(dir, name))
		must.OKf(t, err, "read %s", name)
		if !bytes.Equal(got, want) {
			t.Errorf("%s is stale: run go run ./internal/alerts/cmd/genalerts", name)
		}
	}
	entries, err := os.ReadDir(dir)
	must.OK(t, err)
	for _, e := range entries {
		if _, ok := files[e.Name()]; !ok {
			t.Errorf("%s/%s is not generated; remove it", RulesDir, e.Name())
		}
	}
}

// built is every instrument internal/telemetry's own constructors create,
// recorded as they create it: those built through helpers and loops, which
// metriccatalog.Instruments cannot read from the source, included. A
// renamed instrument, or a changed unit or kind, changes its exported name
// and fails the rules that read the old one.
func built(t *testing.T) []metriccatalog.Instrument {
	t.Helper()
	rec := metriccatalog.NewRecorder()
	p := &telemetry.Providers{Meter: rec, Tracer: tracenoop.NewTracerProvider()}
	logger := slog.New(slog.DiscardHandler)
	telemetry.NewCleanupObserver(p, nil, telemetry.CleanupSettings{Enabled: true, Interval: time.Minute, RetentionBuffer: time.Hour}, logger)
	telemetry.NewGCObserver(p, nil, archive.ReclaimDryRun, time.Hour, logger)
	telemetry.WatchOrkerPresence(p, "o1", "p", false, logger)
	inst, err := telemetry.NewOrkerInstruments(p)
	must.OK(t, err)
	// The Orker's twins are built at its first attempt.
	_, obs := telemetry.NewOrkerObserver(p, inst, logger).Begin(context.Background(), &orkstackv1.ExecuteTaskInput{}, 1)
	if obs == nil {
		t.Fatal("the Orker observer began no attempt")
	}
	return rec.Instruments()
}

// externalMetrics are the series the rules read that OrkStack does not
// name: the OpenTelemetry Collector's own metrics, which the kit pushes to
// the same Prometheus (deploy/observability/otel-collector.yaml,
// service.telemetry), and the Temporal SDK's client metrics, which
// internal/telemetry.TemporalMetricsHandler turns on under the SDK's names.
var externalMetrics = map[string]bool{
	"otelcol_process_uptime_seconds_total": true,
	"temporal_request_total":               true,
	"temporal_request_failure_total":       true,
	"temporal_long_request_total":          true,
	"temporal_long_request_failure_total":  true,
	RunsFailureRatio:                       true,
}

func init() {
	for _, m := range collectorLoss {
		externalMetrics[m] = true
	}
}

// promotedLabels are the resource attributes the kit's Prometheus turns
// into labels (otlp.promote_resource_attributes), and the labels other
// sources put on their own series.
var promotedLabels = map[string]bool{
	"ork_component": true, "ork_deployment": true, "ork_build_id": true,
	"le": true, "__name__": true, "instance": true,
	// the OTel SDK's overflow series (sdkmetric.WithCardinalityLimit)
	"otel_metric_overflow": true,
	// the Temporal SDK's tag on a failed request
	"status_code": true,
}

// Every series a rule reads is one OrkStack exports under the name
// Prometheus gives it, as OTel's own exporter derives it from the name, kind
// and unit the code uses, or one named in externalMetrics. Every label a
// rule groups by or matches on is one some instrument records, or one
// promotedLabels names: this proves a label exists, not that the series the
// rule reads carries it, which the promtool cases and the observability
// gate show instead.
func TestAlerts_MetricsExistAndLabelsAreRecorded(t *testing.T) {
	instruments := append(metriccatalog.Instruments(t), built(t)...)
	names := metriccatalog.PrometheusNames(t, instruments)
	checked := 0
	for rule, expr := range exprs() {
		for _, n := range metriccatalog.MetricNames(expr) {
			checked++
			if !names[n] && !externalMetrics[n] {
				t.Errorf("%s reads %s, which nothing exports", rule, n)
			}
		}
	}
	if checked < 100 {
		t.Fatalf("checked only %d series; the extraction has lost track of the rules", checked)
	}
	known := metriccatalog.DeclaredAttributes(t)
	grouping := regexp.MustCompile(`\b(?:by|without|on|ignoring|group_left|group_right)\s*\(([^)]*)\)`)
	matcher := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=~|!~|!=|=)\s*"`)
	for rule, expr := range exprs() {
		var labels []string
		for _, g := range grouping.FindAllStringSubmatch(expr, -1) {
			for _, l := range strings.Split(g[1], ",") {
				if l = strings.TrimSpace(l); l != "" {
					labels = append(labels, l)
				}
			}
		}
		for _, m := range matcher.FindAllStringSubmatch(expr, -1) {
			labels = append(labels, m[1])
		}
		for _, l := range labels {
			if !known[l] && !promotedLabels[l] {
				t.Errorf("%s uses label %s, which nothing records", rule, l)
			}
		}
	}
}

// The extraction the metric check relies on: functions, labels, keywords
// and durations are never read as series.
func TestAlerts_MetricNamesOfANewRule(t *testing.T) {
	got := metriccatalog.MetricNames(NEW(`ork_audit_records_total{outcome="dropped"}`, "15m", "action"))
	for _, n := range got {
		if n != "ork_audit_records_total" && n != "ork_build_info" {
			t.Fatalf("MetricNames(NEW(...)) = %v; want only ork_audit_records_total and ork_build_info", got)
		}
	}
	if len(got) == 0 {
		t.Fatal("MetricNames(NEW(...)) found nothing")
	}
}

// Every rule has a case that fires it and one that leaves it quiet, and
// every NEW rule has the five cases that prove NEW's edges.
func TestAlerts_EveryRuleHasPromtoolCases(t *testing.T) {
	for _, r := range Rules() {
		fires, quiets := 0, 0
		names := map[string]bool{}
		for _, c := range r.Cases {
			names[c.Name] = true
			if len(c.Fires) > 0 {
				fires++
			} else {
				quiets++
			}
		}
		if fires == 0 {
			t.Errorf("%s has no case that fires it", r.Name)
		}
		if quiets == 0 && r.Expr != "vector(1)" {
			t.Errorf("%s has no case that leaves it quiet", r.Name)
		}
		if isNEW(r.Expr) {
			for _, n := range newCaseNames {
				if !names[n] {
					t.Errorf("%s is a NEW rule without its case %q", r.Name, n)
				}
			}
		}
	}
	for _, r := range Records() {
		if len(r.Cases) == 0 {
			t.Errorf("recording rule %s has no case", r.Name)
		}
	}
}

var aggregation = regexp.MustCompile(`\b(sum|max|min|count|avg|group|stddev|stdvar|topk|bottomk|quantile)\s*(\(|by\s*\(([^)]*)\)|without)`)
var vectorMatch = regexp.MustCompile(`\b(?:on|ignoring)\s*\(([^)]*)\)`)

// hasLabel is whether a grouping's comma-separated list names label.
func hasLabel(list, label string) bool {
	for _, l := range strings.Split(list, ",") {
		if strings.TrimSpace(l) == label {
			return true
		}
	}
	return false
}

// Every aggregation and every vector match keeps the deployment, so one
// Prometheus can watch several deployments; the Collector's own metrics,
// which carry no deployment, are the exception.
func TestAlerts_EveryAggregationKeepsDeployment(t *testing.T) {
	for rule, expr := range exprs() {
		for _, m := range pipelineMacros {
			expr = strings.ReplaceAll(expr, m, "PIPE")
		}
		if onlyCollector(expr) {
			continue
		}
		for _, m := range aggregation.FindAllStringSubmatch(expr, -1) {
			switch {
			case m[2] == "(" || strings.HasPrefix(m[2], "without"):
				t.Errorf("%s: %s without by (%s, ...): %s", rule, m[1], D, expr)
			case !hasLabel(m[3], D):
				t.Errorf("%s: %s by (%s) does not keep %s", rule, m[1], m[3], D)
			}
		}
		for _, m := range vectorMatch.FindAllStringSubmatch(expr, -1) {
			if strings.TrimSpace(m[1]) == "" && strings.Contains(expr, "PIPE") {
				continue
			}
			if !hasLabel(m[1], D) {
				t.Errorf("%s: on (%s) does not keep %s", rule, m[1], D)
			}
		}
	}
}

// onlyCollector is whether an expression reads nothing but the Collector's
// own metrics.
func onlyCollector(expr string) bool {
	names := metriccatalog.MetricNames(expr)
	for _, n := range names {
		if !strings.HasPrefix(n, "otelcol_") {
			return false
		}
	}
	return len(names) > 0
}

// An annotation names only labels the alert carries, so it never renders
// an empty value.
func TestAlerts_AnnotationsNameOnlyOutputLabels(t *testing.T) {
	for _, r := range Rules() {
		have := map[string]bool{}
		for _, c := range r.Cases {
			for _, f := range c.Fires {
				for k := range f {
					have[k] = true
				}
			}
		}
		for _, text := range []string{r.Summary, r.Description} {
			for _, m := range labelRef.FindAllStringSubmatch(text, -1) {
				if !have[m[1]] {
					t.Errorf("%s's annotation names {{ $labels.%s }}, which none of its firing cases carries", r.Name, m[1])
				}
			}
		}
		if r.Summary == "" || r.Description == "" {
			t.Errorf("%s lacks a summary or description", r.Name)
		}
	}
}

// Liveness is told from pushed series, never from a scrape: the kit scrapes
// nothing, so `up` and scrape series do not exist.
func TestAlerts_NoScrapeOrUp(t *testing.T) {
	bad := regexp.MustCompile(`\bup\b|\bscrape_[a-z_]+`)
	for rule, expr := range exprs() {
		if bad.MatchString(strings.Join(metriccatalog.MetricNames(expr), " ")) {
			t.Errorf("%s reads a scrape series: %s", rule, expr)
		}
	}
}

// A threshold that depends on a setting reads the setting's gauge, never a
// default written into the rule.
func TestAlerts_ThresholdsReadSettingGauges(t *testing.T) {
	needs := map[string]string{
		"ork_cleanup_last_pass_seconds":                "ork_cleanup_interval_seconds",
		"ork_gc_last_pass_seconds":                     "ork_cleanup_interval_seconds",
		"ork_gc_last_progress_seconds":                 "ork_cleanup_interval_seconds",
		"ork_archive_sweep_last_success_seconds":       "ork_cleanup_interval_seconds",
		"ork_deployment_cleanup_retention_lag_seconds": "ork_cleanup_retention_buffer_seconds",
		"ork_store_clock_skew_seconds":                 "ork_gc_grace_seconds",
	}
	for rule, expr := range exprs() {
		for reads, setting := range needs {
			if strings.Contains(expr, reads) && !strings.Contains(expr, setting) {
				t.Errorf("%s reads %s without the setting %s", rule, reads, setting)
			}
		}
	}
}

// Every alert's group has a runbook section title.
func TestAlerts_EveryGroupHasATitle(t *testing.T) {
	for _, r := range Rules() {
		if groupTitles[r.Group] == "" {
			t.Errorf("%s's group %q has no runbook title", r.Name, r.Group)
		}
	}
}

func runbookDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot, RunbooksFile))
	must.OK(t, err)
	return string(b)
}

// githubAnchor is the anchor GitHub gives a heading.
func githubAnchor(heading string) string {
	h := strings.ToLower(strings.TrimSpace(heading))
	h = regexp.MustCompile("[^a-z0-9 _-]").ReplaceAllString(h, "")
	return strings.ReplaceAll(h, " ", "-")
}

// Every alert has its runbook entry under the anchor its runbook_url names,
// and every link an entry makes lands on a heading of the page.
func TestRunbooks_EveryAlertHasItsSection(t *testing.T) {
	doc := runbookDoc(t)
	anchors := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^#+ (.+)$`).FindAllStringSubmatch(doc, -1) {
		anchors[githubAnchor(m[1])] = true
	}
	for _, r := range Rules() {
		if !anchors[Anchor(r.Name)] {
			t.Errorf("%s has no runbook heading for #%s", r.Name, Anchor(r.Name))
		}
	}
	for _, m := range regexp.MustCompile(`\]\(#([a-z0-9-]+)\)`).FindAllStringSubmatch(doc, -1) {
		if !anchors[m[1]] {
			t.Errorf("%s links #%s, which is no heading of it", RunbooksFile, m[1])
		}
	}
}

// Every confirm step names what to look at: an orkctl command, an engine
// log line, or, with the kit, the series to read. The Watchdog's is the
// dead man's switch it feeds.
func TestRunbooks_EveryConfirmNamesOrkctlOrKit(t *testing.T) {
	for _, r := range Rules() {
		c := r.Confirm
		named := strings.Contains(c, "`orkctl ") || strings.Contains(c, "log") || strings.Contains(c, "kit: `")
		if r.Expr != "vector(1)" && (!named || !strings.Contains(c, "`")) {
			t.Errorf("%s's confirm step names no orkctl command, log line or series: %q", r.Name, c)
		}
	}
}

// Every orkctl command the runbooks name exists: docs/CLI.md is generated
// from the CLI itself.
func TestRunbooks_OrkctlCommandsExist(t *testing.T) {
	cli, err := os.ReadFile(filepath.Join(repoRoot, "docs/CLI.md"))
	must.OK(t, err)
	commands := map[string]bool{}
	for _, m := range regexp.MustCompile("(?m)^## `orkctl ([a-z -]+)`").FindAllStringSubmatch(string(cli), -1) {
		commands[m[1]] = true
	}
	if len(commands) < 50 {
		t.Fatalf("read %d commands from docs/CLI.md; the reader has lost track of them", len(commands))
	}
	use := regexp.MustCompile("`orkctl ([a-z][a-z-]*(?: [a-z][a-z-]*)*)")
	var missing []string
	for _, m := range use.FindAllStringSubmatch(runbookDoc(t), -1) {
		words := strings.Fields(m[1])
		found := false
		for n := len(words); n > 0; n-- {
			if commands[strings.Join(words[:n], " ")] {
				found = n > 1 || len(words) == 1
				break
			}
		}
		if !found {
			missing = append(missing, m[1])
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("%s names orkctl commands docs/CLI.md does not have: %v", RunbooksFile, missing)
	}
}

// The kit's Prometheus loads every generated rule file and no test file:
// its rule_files glob, under the directory compose mounts the rules at,
// matches exactly the rule files Files writes.
func TestAlerts_KitLoadsEveryRuleFile(t *testing.T) {
	cfg, err := os.ReadFile(filepath.Join(repoRoot, "deploy/observability/prometheus.yml"))
	must.OK(t, err)
	compose, err := os.ReadFile(filepath.Join(repoRoot, "deploy/docker-compose.yml"))
	must.OK(t, err)
	const mounted = "/etc/prometheus/rules"
	if !strings.Contains(string(compose), "./observability/prometheus/rules:"+mounted+":ro") {
		t.Fatalf("deploy/docker-compose.yml does not mount %s at %s", RulesDir, mounted)
	}
	glob := regexp.MustCompile(`(?m)^\s*-\s*` + regexp.QuoteMeta(mounted) + `/(\S+)\s*$`).FindStringSubmatch(string(cfg))
	if glob == nil {
		t.Fatal("deploy/observability/prometheus.yml has no rule_files entry under " + mounted)
	}
	files, err := Files()
	must.OK(t, err)
	for name := range files {
		if name == RunbooksFile {
			continue
		}
		matched, err := filepath.Match(glob[1], name)
		must.OK(t, err)
		if test := strings.HasSuffix(name, "_test.yaml"); matched == test {
			t.Errorf("rule_files glob %q: %s matched=%v; want rule files loaded and test files not", glob[1], name, matched)
		}
	}
}
