package dashboards

import (
	"bytes"
	json "encoding/json/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/metriccatalog"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The provisioned files the generator writes (internal/dashboards/cmd/gendashboards).
const provisionedDir = "../../deploy/observability/grafana/dashboards"

// model is one dashboard's JSON as Grafana reads it.
type model map[string]any

func dashboardsByUID(t *testing.T) map[string]model {
	t.Helper()
	files, err := JSON()
	must.OK(t, err)
	out := map[string]model{}
	for uid, b := range files {
		var m model
		must.OK(t, json.Unmarshal(b, &m))
		out[uid] = m
	}
	return out
}

// panelsOf is every panel of d, rows' nested ones included, each with
// whether it is a top-level one; rows themselves are returned apart.
func panelsOf(d model) (top, nested, rows []map[string]any) {
	for _, p := range d["panels"].([]any) {
		pm := p.(map[string]any)
		if pm["type"] == "row" {
			rows = append(rows, pm)
			for _, c := range pm["panels"].([]any) {
				nested = append(nested, c.(map[string]any))
			}
			continue
		}
		top = append(top, pm)
	}
	return top, nested, rows
}

func allPanels(d model) []map[string]any {
	top, nested, _ := panelsOf(d)
	return append(top, nested...)
}

func exprsOf(p map[string]any) []string {
	var out []string
	targets, _ := p["targets"].([]any)
	for _, t := range targets {
		tm := t.(map[string]any)
		for _, k := range []string{"expr", "query"} {
			if s, ok := tm[k].(string); ok && s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

func datasourceType(p map[string]any) string {
	ds, _ := p["datasource"].(map[string]any)
	s, _ := ds["type"].(string)
	return s
}

func variableNames(d model) []string {
	var out []string
	for _, v := range d["templating"].(map[string]any)["list"].([]any) {
		out = append(out, v.(map[string]any)["name"].(string))
	}
	return out
}

// Exactly two dashboards exist: the deployment's and one run's, and the
// provisioned directory holds those two files and nothing else.
func TestDashboards_OneDashboardAndRunView(t *testing.T) {
	all, err := All()
	must.OK(t, err)
	var uids []string
	for _, d := range all {
		if d.Uid == nil {
			t.Fatalf("dashboard %v has no uid", d.Title)
		}
		uids = append(uids, *d.Uid)
	}
	slices.Sort(uids)
	if !slices.Equal(uids, []string{UIDOrkStack, UIDRun}) {
		t.Fatalf("dashboards = %v; want orkstack and orkstack-run", uids)
	}
	entries, err := os.ReadDir(provisionedDir)
	must.OK(t, err)
	var files []string
	for _, e := range entries {
		files = append(files, e.Name())
	}
	want := []string{UIDOrkStack + ".json", UIDRun + ".json"}
	slices.Sort(files)
	slices.Sort(want)
	if !slices.Equal(files, want) {
		t.Fatalf("%s holds %v; want the two dashboards alone", provisionedDir, files)
	}
}

// The committed JSON is the generator's output: regenerate it with
// `go run ./internal/dashboards/cmd/gendashboards`.
func TestDashboards_AreGenerated(t *testing.T) {
	files, err := JSON()
	must.OK(t, err)
	for uid, want := range files {
		path := filepath.Join(provisionedDir, uid+".json")
		got, err := os.ReadFile(path)
		must.OKf(t, err, "read %s", path)
		if !bytes.Equal(got, want) {
			t.Errorf("%s is stale: run go run ./internal/dashboards/cmd/gendashboards", path)
		}
	}
}

// Every area row is collapsed, so opening the dashboard runs only the
// variables and the top row.
func TestDashboards_AreaRowsAreCollapsed(t *testing.T) {
	_, _, rows := panelsOf(dashboardsByUID(t)[UIDOrkStack])
	if len(rows) == 0 {
		t.Fatal("the OrkStack dashboard has no area rows")
	}
	for _, r := range rows {
		if r["collapsed"] != true {
			t.Errorf("row %q is not collapsed", r["title"])
		}
		if len(r["panels"].([]any)) == 0 {
			t.Errorf("row %q has no panels", r["title"])
		}
	}
}

// Opening the dashboard costs at most 16 top-row queries and the three
// label queries.
func TestDashboards_TopRowQueryBudget(t *testing.T) {
	d := dashboardsByUID(t)[UIDOrkStack]
	top, _, _ := panelsOf(d)
	n := 0
	for _, p := range top {
		n += len(exprsOf(p))
	}
	if n > 16 {
		t.Fatalf("the top row runs %d queries; at most 16", n)
	}
	labelQueries := 0
	for _, v := range d["templating"].(map[string]any)["list"].([]any) {
		if v.(map[string]any)["type"] == "query" {
			labelQueries++
		}
	}
	if labelQueries != 3 {
		t.Fatalf("%d label queries; want namespace, orkflow and pool", labelQueries)
	}
}

// Both dashboards share their variables' names, so a link carries them.
func TestDashboards_VariablesAreTheShared(t *testing.T) {
	ds := dashboardsByUID(t)
	for uid, want := range map[string][]string{
		UIDOrkStack: {"prom", "loki", "tempo", "namespace", "orkflow", "pool", "ui_url"},
		UIDRun:      {"loki", "tempo", "run", "task", "ui_url"},
	} {
		if got := variableNames(ds[uid]); !slices.Equal(got, want) {
			t.Errorf("%s variables = %v; want %v", uid, got, want)
		}
	}
}

// Every panel says what it shows and in what unit.
func TestDashboards_EveryPanelHasDescriptionAndUnit(t *testing.T) {
	for uid, d := range dashboardsByUID(t) {
		for _, p := range allPanels(d) {
			if s, _ := p["description"].(string); strings.TrimSpace(s) == "" {
				t.Errorf("%s: panel %q has no description", uid, p["title"])
			}
			unit, _ := p["fieldConfig"].(map[string]any)["defaults"].(map[string]any)["unit"].(string)
			if unit == "" {
				t.Errorf("%s: panel %q has no unit", uid, p["title"])
			}
		}
	}
}

// Every panel and query reads through a datasource variable, never a fixed
// uid.
func TestDashboards_UseDatasourceVariables(t *testing.T) {
	for uid, d := range dashboardsByUID(t) {
		holders := []any{}
		for _, v := range d["templating"].(map[string]any)["list"].([]any) {
			if v.(map[string]any)["type"] == "query" {
				holders = append(holders, v)
			}
		}
		for _, p := range allPanels(d) {
			targets, _ := p["targets"].([]any)
			holders = append(holders, append([]any{p}, targets...)...)
		}
		for _, holder := range holders {
			ds, _ := holder.(map[string]any)["datasource"].(map[string]any)
			if u, _ := ds["uid"].(string); !strings.HasPrefix(u, "${") {
				t.Errorf("%s: %v reads datasource %q, not a variable", uid, holder.(map[string]any)["title"], u)
			}
		}
	}
}

// promQL is every Prometheus query of the OrkStack dashboard, label queries
// included.
func promQL(t *testing.T) []string {
	t.Helper()
	d := dashboardsByUID(t)[UIDOrkStack]
	var out []string
	for _, p := range allPanels(d) {
		if datasourceType(p) == "prometheus" {
			out = append(out, exprsOf(p)...)
		}
	}
	for _, v := range d["templating"].(map[string]any)["list"].([]any) {
		if q, ok := v.(map[string]any)["definition"].(string); ok && q != "" {
			out = append(out, q)
		}
	}
	return out
}

// No query groups or selects by a label with a value per run, task,
// attempt or process: a dashboard query must stay bounded. ork_orker_id is
// allowed only to count Orkers once each, or under topk.
func TestDashboards_NoUnboundedMetricLabel(t *testing.T) {
	banned := regexp.MustCompile(`\b(ork_run_id|ork_task_id|ork_attempt|ork_item|instance|task_queue|taskqueue)\b`)
	orkerID := regexp.MustCompile(`ork_orker_id`)
	allowedOrkerID := regexp.MustCompile(`count by \(ork_orker_id\)|topk\(\d+, [^)]*by \(ork_orker_id\)`)
	for _, expr := range promQL(t) {
		if m := banned.FindString(expr); m != "" {
			t.Errorf("query names %s: %s", m, expr)
		}
		if n, ok := len(orkerID.FindAllString(expr, -1)), len(allowedOrkerID.FindAllString(expr, -1)); n > ok {
			t.Errorf("query uses ork_orker_id outside a count or topk: %s", expr)
		}
	}
}

// distinctRunPanels are the panels that mean runs, not endings: a resumed
// run's later ending, which ork.runs and ork.run.duration record again under
// ork_run_resumption="true", is left out of them.
var distinctRunPanels = map[string]bool{"Runs / min": true, "Run duration": true, "Run duration distribution": true}

// endingPanels are the panels that count each ending, a resumed run's later
// ones included.
var endingPanels = map[string]bool{"Runs by status": true, "Failure ratio, 15 m": true}

// A panel that means runs counts a run once, however often it was resumed;
// a panel of endings counts each, so a resumed run's success or second
// failure shows in its status counts and in the failure ratio.
func TestDashboards_RunCountsExcludeResumed(t *testing.T) {
	if !metriccatalog.DeclaredAttributes(t)["ork_run_resumption"] {
		t.Fatal("nothing exports ork_run_resumption: a matcher on it would match everything and only seem to filter")
	}
	run := regexp.MustCompile(`ork_runs_total\b(?:\{[^}]*\})?|ork_run_duration_seconds_(?:bucket|count|sum)\b(?:\{[^}]*\})?`)
	found := map[string]bool{}
	for _, p := range allPanels(dashboardsByUID(t)[UIDOrkStack]) {
		title := p["title"].(string)
		for _, expr := range exprsOf(p) {
			for _, sel := range run.FindAllString(expr, -1) {
				has := strings.Contains(sel, `ork_run_resumption!="true"`)
				switch {
				case distinctRunPanels[title]:
					found[title] = true
					if !has {
						t.Errorf("panel %q means runs and counts a resumed run's later ending too: %s", title, sel)
					}
					if !strings.Contains(p["description"].(string), "later endings are left out") {
						t.Errorf("panel %q does not say that a resumed run's later endings are left out", title)
					}
				case endingPanels[title]:
					found[title] = true
					if strings.Contains(sel, "ork_run_resumption") {
						t.Errorf("panel %q counts endings and leaves a resumed run's out: %s", title, sel)
					}
				default:
					t.Errorf("panel %q reads runs and is on neither list: say whether it means runs or endings: %s", title, sel)
				}
			}
		}
	}
	for _, list := range []map[string]bool{distinctRunPanels, endingPanels} {
		for title := range list {
			if !found[title] {
				t.Errorf("panel %q is listed and reads no runs", title)
			}
		}
	}
}

// Panel text uses OrkStack's words, never the internal ones (AGENTS.md §7).
func TestDashboards_NoInternalVocabulary(t *testing.T) {
	internal := regexp.MustCompile(`(?i)\b(queues?|task queues?|sessions?|generations?|checkpoints?|carry|signals?|timers?|child workflows?|workflow ids?|local activit(y|ies)|search attributes?|visibility|worker deployment versions?)\b`)
	for uid, d := range dashboardsByUID(t) {
		texts := []string{d["title"].(string), d["description"].(string)}
		for _, p := range allPanels(d) {
			texts = append(texts, p["title"].(string), p["description"].(string))
			if fc, ok := p["fieldConfig"].(map[string]any); ok {
				if nv, ok := fc["defaults"].(map[string]any)["noValue"].(string); ok {
					texts = append(texts, nv)
				}
			}
			targets, _ := p["targets"].([]any)
			for _, tg := range targets {
				if l, ok := tg.(map[string]any)["legendFormat"].(string); ok {
					texts = append(texts, l)
				}
			}
		}
		for _, v := range d["templating"].(map[string]any)["list"].([]any) {
			vm := v.(map[string]any)
			for _, k := range []string{"label", "description"} {
				if s, ok := vm[k].(string); ok {
					texts = append(texts, s)
				}
			}
		}
		_, _, rows := panelsOf(d)
		for _, r := range rows {
			texts = append(texts, r["title"].(string))
		}
		for _, s := range texts {
			if m := internal.FindString(s); m != "" {
				t.Errorf("%s: %q says %q", uid, s, m)
			}
		}
	}
}

// A panel that counts pollers says so: pollers are not the pool's Orkers.
func TestDashboards_PollersAreNotTitledOrkers(t *testing.T) {
	for _, p := range allPanels(dashboardsByUID(t)[UIDOrkStack]) {
		title := p["title"].(string)
		polls := slices.ContainsFunc(exprsOf(p), func(e string) bool { return strings.Contains(e, "ork_pool_pollers") })
		lower := strings.ToLower(title)
		if polls && strings.Contains(lower, "orker") && !strings.Contains(lower, "poller") {
			t.Errorf("panel %q counts pollers and calls them Orkers", title)
		}
	}
}

// Every link to a run opens the Run view, never another dashboard.
func TestDashboards_RunLinksTargetRunView(t *testing.T) {
	for uid, d := range dashboardsByUID(t) {
		for _, p := range allPanels(d) {
			fc := p["fieldConfig"].(map[string]any)
			links, _ := fc["defaults"].(map[string]any)["links"].([]any)
			overrides, _ := fc["overrides"].([]any)
			for _, o := range overrides {
				for _, prop := range o.(map[string]any)["properties"].([]any) {
					if pm := prop.(map[string]any); pm["id"] == "links" {
						links = append(links, pm["value"].([]any)...)
					}
				}
			}
			for _, l := range links {
				u := l.(map[string]any)["url"].(string)
				if strings.Contains(u, "var-run=") && !strings.HasPrefix(u, "/d/"+UIDRun+"?") {
					t.Errorf("%s: panel %q links a run to %s", uid, p["title"], u)
				}
			}
		}
	}
}

// Log queries filter on structured metadata, never by parsing the body:
// a body parser needs a record or stream selector first.
func TestDashboards_LogQueriesSelectStructuredMetadata(t *testing.T) {
	parser := regexp.MustCompile(`\|\s*(json|logfmt|regexp|pattern)\b`)
	for uid, d := range dashboardsByUID(t) {
		for _, p := range allPanels(d) {
			if datasourceType(p) != "loki" {
				continue
			}
			for _, e := range exprsOf(p) {
				if parser.MatchString(e) && !strings.Contains(e, "ork_record=") && !strings.Contains(e, "ork_stream=") {
					t.Errorf("%s: panel %q parses the body without a record or stream selector: %s", uid, p["title"], e)
				}
			}
		}
	}
}

// Store bytes are what OrkStack wrote, never the bucket's total.
func TestDashboards_StoreBytesNeverSaysTotal(t *testing.T) {
	for _, p := range allPanels(dashboardsByUID(t)[UIDOrkStack]) {
		if slices.ContainsFunc(exprsOf(p), func(e string) bool { return strings.Contains(e, "ork_store_bytes") }) &&
			strings.Contains(strings.ToLower(p["title"].(string)), "total") {
			t.Errorf("panel %q titles store bytes a total", p["title"])
		}
	}
}

// The kit's colours are the UI's dark-theme tokens.
func TestDashboards_PaletteFromTokens(t *testing.T) {
	css, err := os.ReadFile("../../ui/src/tokens.css")
	must.OK(t, err)
	dark := string(css)
	i := strings.Index(dark, ":root[data-theme='dark'] {")
	if i < 0 {
		t.Fatal("tokens.css has no :root[data-theme='dark'] block")
	}
	dark = dark[i:]
	dark = dark[:strings.Index(dark, "}")]
	for color, token := range paletteTokens {
		m := regexp.MustCompile(regexp.QuoteMeta(token) + `:\s*(#[0-9A-Fa-f]{6})`).FindStringSubmatch(dark)
		if m == nil || !strings.EqualFold(m[1], color) {
			t.Errorf("%s is %s in the dark theme; the kit uses %s", token, m, color)
		}
	}
	// Every colour a dashboard draws is one of the palette's.
	hex := regexp.MustCompile(`#[0-9A-Fa-f]{6}`)
	files, err := JSON()
	must.OK(t, err)
	for uid, b := range files {
		for _, c := range hex.FindAllString(string(b), -1) {
			if _, ok := paletteTokens[c]; !ok {
				t.Errorf("%s draws %s, which is not a token", uid, c)
			}
		}
	}
}
