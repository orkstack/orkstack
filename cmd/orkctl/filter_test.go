package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

// requestBodies decodes every request the fixture received on path, in
// arrival order, into a fresh message from newMsg.
func requestBodies[M proto.Message](t *testing.T, f *fixture, path string, newMsg func() M) []M {
	t.Helper()
	f.requestsMu.Lock()
	defer f.requestsMu.Unlock()
	var out []M
	for i, p := range f.requestLog {
		if p != path {
			continue
		}
		msg := newMsg()
		if err := proto.Unmarshal(f.bodyLog[i], msg); err != nil {
			t.Fatalf("decode request to %s: %v", path, err)
		}
		out = append(out, msg)
	}
	return out
}

// filterTexts renders wire filters back into the CLI grammar.
func filterTexts(filters []*orkstackv1.FieldFilter) []string {
	out := make([]string, len(filters))
	for i, f := range filters {
		out[i] = listfilter.Format(f)
	}
	return out
}

// TestFilterHelp_EveryListCommandPrintsItsAllowlistWithoutARequest: `--filter
// help` prints the command's allowlist, exits 0 and sends nothing; the same
// table ends the command's --help.
func TestFilterHelp_EveryListCommandPrintsItsAllowlistWithoutARequest(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		golden  string
		request proto.Message
	}{
		{[]string{"namespace", "list"}, "filter-help-namespace-list.golden", &orkstackv1.ListNamespacesRequest{}},
		{[]string{"orkflow", "list"}, "filter-help-orkflow-list.golden", &orkstackv1.ListOrkflowsRequest{}},
		{[]string{"webhook", "list"}, "filter-help-webhook-list.golden", &orkstackv1.ListWebhooksRequest{}},
		{[]string{"trigger", "list"}, "filter-help-trigger-list.golden", &orkstackv1.ListTriggersRequest{}},
		{[]string{"schedule", "list"}, "filter-help-schedule-list.golden", &orkstackv1.ListSchedulesRequest{}},
		{[]string{"pool", "list"}, "filter-help-pool-list.golden", &orkstackv1.ListPoolsRequest{}},
		{[]string{"activity", "list"}, "filter-help-activity-list.golden", &orkstackv1.ListActivitiesRequest{}},
		{[]string{"orker", "list"}, "filter-help-orker-list.golden", &orkstackv1.ListOrkersRequest{}},
		{[]string{"release", "status"}, "filter-help-release-status.golden", &orkstackv1.ListReleasesRequest{}},
		{[]string{"webhook", "deliveries"}, "filter-help-webhook-deliveries.golden", &orkstackv1.ListDeliveriesRequest{}},
		{[]string{"admin", "audit"}, "filter-help-admin-audit.golden", &orkstackv1.ListAuditRecordsRequest{}},
		{[]string{"run", "list"}, "filter-help-run-list.golden", &orkstackv1.ListRunsRequest{}},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			f := newFixture(t)
			out := f.run(append(tc.args, "--filter", "name=x", "--filter", "help")...).requireExit(t, exitOK)
			requireGolden(t, tc.golden, out.stdout)
			if out.stdout != listfilter.MustSpec(tc.request).Help() {
				t.Fatalf("--filter help is not the declared allowlist:\n%s", out.stdout)
			}
			if got := f.requests(); len(got) != 0 {
				t.Fatalf("--filter help sent %v; it needs no engine", got)
			}
			help := f.run(append(tc.args, "--help")...).requireExit(t, exitOK)
			if !strings.Contains(help.stdout, strings.TrimRight(out.stdout, "\n")) || !strings.Contains(help.stdout, "--filter") {
				t.Fatalf("--help does not end with the allowlist:\n%s", help.stdout)
			}
		})
	}
}

// TestFilterFlag_CommaIsInNotTwoFlags: --filter is a StringArray, so a comma
// stays inside one filter — the in operator — rather than splitting it.
func TestFilterFlag_CommaIsInNotTwoFlags(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "payments", "--label", "team=money").requireExit(t, exitOK)
	f.run("namespace", "create", "billing", "--label", "team=finance").requireExit(t, exitOK)
	out := f.run("namespace", "list", "--filter", "labels.team=money,finance").requireExit(t, exitOK)
	out.requireContains(t, "payments", "billing")
	sent := requestBodies(t, f, "/orkstack.v1.NamespaceService/ListNamespaces", func() *orkstackv1.ListNamespacesRequest {
		return &orkstackv1.ListNamespacesRequest{}
	})
	last := sent[len(sent)-1].GetFilters()
	if len(last) != 1 || last[0].GetOp() != orkstackv1.FilterOperator_FILTER_OPERATOR_IN ||
		strings.Join(last[0].GetValues(), "|") != "money|finance" {
		t.Fatalf("labels.team=money,finance sent %v; want one in filter of two values", last)
	}
	f.run("namespace", "list", "--filter", `description=x\,y`).requireExit(t, exitOK)
	sent = requestBodies(t, f, "/orkstack.v1.NamespaceService/ListNamespaces", func() *orkstackv1.ListNamespacesRequest {
		return &orkstackv1.ListNamespacesRequest{}
	})
	last = sent[len(sent)-1].GetFilters()
	if len(last) != 1 || last[0].GetOp() != orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS ||
		strings.Join(last[0].GetValues(), "|") != "x,y" {
		t.Fatalf(`description=x\,y sent %v; want one equals filter of "x,y"`, last)
	}
}

// TestFilterFlag_SyntaxErrorBeforeAnyRequest: a filter that does not parse is
// refused by orkctl itself, exit non-zero, before anything is sent.
func TestFilterFlag_SyntaxErrorBeforeAnyRequest(t *testing.T) {
	for _, text := range []string{"", "=x", "name", "created_at>2026-10-09T00:00:00Z", "a<=b", "name~a,b", `name=a\`} {
		f := newFixture(t)
		out := f.run("namespace", "list", "--filter", text).requireExit(t, exitError)
		if !strings.Contains(out.stderr, "--filter "+shellQuote(text)) {
			t.Errorf("%q: stderr %q does not name the filter", text, out.stderr)
		}
		if got := f.requests(); len(got) != 0 {
			t.Errorf("%q sent %v; a syntax error is refused before any request", text, got)
		}
	}
}

func TestNamespaceList_FilteredGolden(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "payments", "--label", "team=money", "--description", "the payments tenant").requireExit(t, exitOK)
	f.run("namespace", "create", "pay-later", "--label", "team=money").requireExit(t, exitOK)
	f.run("namespace", "create", "billing", "--label", "team=finance").requireExit(t, exitOK)
	out := f.run("namespace", "list", "--filter", "labels.team=money", "--filter", "name!=pay-later").requireExit(t, exitOK)
	requireGolden(t, "namespace-list-filtered.golden", out.stdout)
}

// orkflowSourceNamed is a minimal orkflow document.
func orkflowSourceNamed(name, team string) string {
	return fmt.Sprintf("apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ndescription: the %s orkflow\nlabels: {team: %s}\n"+
		"tasks:\n  - id: build\n    uses: shell\n    with: {command: \"echo build\"}\n", name, name, team)
}

func applyOrkflows(t *testing.T, f *fixture, docs map[string]string) {
	t.Helper()
	dir := t.TempDir()
	for name, team := range docs {
		path := filepath.Join(dir, name+".ork.yaml")
		if err := os.WriteFile(path, []byte(orkflowSourceNamed(name, team)), 0o644); err != nil {
			t.Fatal(err)
		}
		f.run("apply", path).requireExit(t, exitOK)
	}
}

func TestOrkflowList_FilteredGolden(t *testing.T) {
	f := newFixture(t)
	applyOrkflows(t, f, map[string]string{"deploy": "payments", "reindex": "search", "settle": "payments"})
	out := f.run("orkflow", "list", "--filter", "labels.team=payments").requireExit(t, exitOK)
	requireGolden(t, "orkflow-list-filtered.golden", out.stdout)
	none := f.run("orkflow", "list", "--filter", "name^zz").requireExit(t, exitOK)
	if none.stdout != "No orkflows match --filter 'name^zz'.\n" {
		t.Fatalf("no match = %q", none.stdout)
	}
}

// TestOrkflowList_PrintsANextPageLineWithItsFilters: a listing cut short says
// so, and the command it prints repeats every filter, the limit and the
// token; running that command continues the same listing.
func TestOrkflowList_PrintsANextPageLineWithItsFilters(t *testing.T) {
	f := newFixture(t)
	applyOrkflows(t, f, map[string]string{"a-deploy": "payments", "b-index": "search", "c-settle": "payments", "d-refund": "payments"})
	first := f.run("orkflow", "list", "--filter", "labels.team=payments", "--limit", "2").requireExit(t, exitOK)
	first.requireContains(t, "a-deploy", "c-settle")
	idx := strings.Index(first.stdout, "Next page: ")
	if idx < 0 {
		t.Fatalf("no next-page line:\n%s", first.stdout)
	}
	line := strings.TrimSpace(first.stdout[idx+len("Next page: "):])
	if !strings.HasPrefix(line, "orkctl orkflow list --filter 'labels.team=payments' --limit 2 --page-token ") {
		t.Fatalf("next-page line = %q", line)
	}
	token := line[strings.LastIndex(line, " ")+1:]
	second := f.run("orkflow", "list", "--filter", "labels.team=payments", "--limit", "2", "--page-token", token).requireExit(t, exitOK)
	second.requireContains(t, "d-refund")
	if strings.Contains(second.stdout, "Next page") || strings.Contains(second.stdout, "a-deploy") {
		t.Fatalf("second page:\n%s", second.stdout)
	}
	scoped := newFixture(t)
	scoped.run("namespace", "create", "payments").requireExit(t, exitOK)
	dir := t.TempDir()
	for _, name := range []string{"one", "two"} {
		path := filepath.Join(dir, name+".ork.yaml")
		if err := os.WriteFile(path, []byte(orkflowSourceNamed(name, "x")), 0o644); err != nil {
			t.Fatal(err)
		}
		scoped.run("apply", path, "-n", "payments").requireExit(t, exitOK)
	}
	out := scoped.run("orkflow", "list", "-n", "payments", "--limit", "1").requireExit(t, exitOK)
	out.requireContains(t, "Next page: orkctl -n payments orkflow list --limit 1 --page-token ")
}

func TestWebhookList_FilteredGolden(t *testing.T) {
	f, _ := newWebhookFixture(t)
	f.run("webhook", "register", "https://example.test/audit").requireExit(t, exitOK)
	f.run("webhook", "register", "https://example.test/etl",
		"-n", "analytics", "--orkflow", "nightly-etl", "--task", "extract",
		"--event", "TASK_SUCCEEDED", "--name", "etl-loader").requireExit(t, exitOK)
	requireGolden(t, "webhook-list-filtered.golden",
		f.run("webhook", "list", "--filter", "namespace=").requireExit(t, exitOK).stdout)
	if out := f.run("webhook", "list", "--filter", "url~nothing").requireExit(t, exitOK).stdout; out != "No webhooks match --filter 'url~nothing'.\n" {
		t.Fatalf("no match = %q", out)
	}
}

// TestWebhookListJSON_KeepsItsEnvelope: filters narrow the rows inside the
// command's --json envelope and never change its shape.
func TestWebhookListJSON_KeepsItsEnvelope(t *testing.T) {
	f, _ := newWebhookFixture(t)
	f.run("webhook", "register", "https://example.test/audit").requireExit(t, exitOK)
	f.run("webhook", "register", "https://example.test/etl", "-n", "analytics", "--name", "etl-loader").requireExit(t, exitOK)
	for _, tc := range []struct {
		args []string
		urls string
	}{
		{nil, "https://example.test/audit https://example.test/etl"},
		{[]string{"--filter", "namespace=analytics"}, "https://example.test/etl"},
		{[]string{"--filter", "name=nope"}, ""},
	} {
		out := f.run(append([]string{"--json", "webhook", "list"}, tc.args...)...).requireExit(t, exitOK)
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal([]byte(out.stdout), &envelope); err != nil {
			t.Fatalf("%v: %v\n%s", tc.args, err, out.stdout)
		}
		var hooks []struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(envelope["webhooks"], &hooks); err != nil || len(envelope) != 1 {
			t.Fatalf("%v: the envelope changed shape: %s", tc.args, out.stdout)
		}
		var urls []string
		for _, h := range hooks {
			urls = append(urls, h.URL)
		}
		if strings.Join(urls, " ") != tc.urls {
			t.Errorf("%v: urls = %v, want %s", tc.args, urls, tc.urls)
		}
	}
}

func TestTriggerList_Filters(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	applyOrkflows(t, f, map[string]string{"other-ork": "x"})
	f.run("trigger", "create", "--orkflow", "hello-ork", "--name", "hello-hook").requireExit(t, exitOK)
	f.run("trigger", "create", "--orkflow", "other-ork", "--name", "other-hook").requireExit(t, exitOK)
	out := f.run("trigger", "list", "--filter", "orkflow_name=hello-ork").requireExit(t, exitOK)
	out.requireContains(t, "hello-hook")
	if strings.Contains(out.stdout, "other-hook") {
		t.Fatalf("orkflow_name=hello-ork listed other-ork's trigger:\n%s", out.stdout)
	}
	if got := f.run("trigger", "list", "--filter", "name=nope").requireExit(t, exitOK).stdout; got != "No triggers match --filter 'name=nope'.\n" {
		t.Fatalf("no match = %q", got)
	}
}

func TestScheduleList_Filters(t *testing.T) {
	f, _ := newScheduleFixture(t)
	for _, name := range []string{"nightly", "hourly"} {
		f.run("schedule", "create", name, "--orkflow", "hello-ork", "--cron", "@daily").requireExit(t, exitOK)
	}
	out := f.run("schedule", "list", "--filter", "name=nightly").requireExit(t, exitOK)
	out.requireContains(t, "nightly")
	if strings.Contains(out.stdout, "hourly") {
		t.Fatalf("name=nightly listed hourly:\n%s", out.stdout)
	}
	sent := requestBodies(t, f, "/orkstack.v1.ScheduleService/ListSchedules", func() *orkstackv1.ListSchedulesRequest {
		return &orkstackv1.ListSchedulesRequest{}
	})
	if got := filterTexts(sent[len(sent)-1].GetFilters()); strings.Join(got, " ") != "name=nightly" {
		t.Fatalf("sent %v", got)
	}
	if got := f.run("schedule", "list", "--filter", "orkflow_name=other").requireExit(t, exitOK).stdout; got != "No schedules match --filter 'orkflow_name=other'.\n" {
		t.Fatalf("no match = %q", got)
	}
}

func TestPoolList_FilteredGolden(t *testing.T) {
	f := adminFixture(t)
	f.run("namespace", "create", "vision", "--default-pool", "gpu", "--allow-pool", "gpu", "--allow-pool", "cold").requireExit(t, exitOK)
	out := f.run("pool", "list", "--namespace", "vision", "--filter", "is_default=false").requireExit(t, exitOK)
	requireGolden(t, "pool-list-filtered.golden", out.stdout)
}

func TestActivityList_FilteredGolden(t *testing.T) {
	f := newFixture(t,
		&orkstackv1.ActivityInfo{Name: "shell", Reference: "registry.test/orkstack/activity-shell:dev", IndexDigest: "sha256:" + strings.Repeat("d", 64)},
		&orkstackv1.ActivityInfo{Name: "file", Reference: "registry.test/orkstack/activity-file:dev", IndexDigest: "sha256:" + strings.Repeat("e", 64)},
		&orkstackv1.ActivityInfo{Name: "migrate", Reference: "ghcr.io/acme/migrate:v3"},
	)
	out := f.run("activity", "list", "--filter", "reference^registry.test/").requireExit(t, exitOK)
	requireGolden(t, "activity-list-filtered.golden", out.stdout)
}

func TestOrkerList_FilteredGolden(t *testing.T) {
	f := newOrkerFixture(t, newScriptedFleet())
	out := f.run("orker", "list", "--filter", "build_id=2026.08.16-a1b2").requireExit(t, exitOK)
	requireGolden(t, "orker-list-filtered.golden", out.stdout)
	if !strings.Contains(out.stdout, "orker-c001") {
		t.Fatalf("the unreachable Orker is not kept under a build_id filter:\n%s", out.stdout)
	}
}

// TestOrkerDrain_SendsOrkerIDEquality: `orker get` and every poll of the
// drain loop ask exactly one Orker, through an orker_id equals filter.
func TestOrkerDrain_SendsOrkerIDEquality(t *testing.T) {
	useFakeDrainClock(t, drainStart, 47*time.Minute)
	fleet := drainFleet(drainStep(2, 1, true), drainStep(0, 0, true))
	f := newOrkerFixture(t, fleet)
	f.run("orker", "get", "orker-7f3a").requireExit(t, exitOK)
	f.run("orker", "drain", "orker-7f3a", "--timeout", "4h").requireExit(t, exitOK)
	sent := requestBodies(t, f, "/orkstack.v1.ActivityService/ListOrkers", func() *orkstackv1.ListOrkersRequest {
		return &orkstackv1.ListOrkersRequest{}
	})
	if len(sent) < 2 {
		t.Fatalf("sent %d ListOrkers requests", len(sent))
	}
	for i, req := range sent {
		if got := filterTexts(req.GetFilters()); strings.Join(got, " ") != "orker_id=orker-7f3a" {
			t.Fatalf("request %d carried %v; want orker_id=orker-7f3a alone", i, got)
		}
	}
}

func TestReleaseStatus_FilteredGolden(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	f := newReleaseFixture(t, newScriptedReleases(rolloutStep()), releaseFleetFor())
	out := f.run("release", "status", "--filter", "role=current").requireExit(t, exitOK)
	requireGolden(t, "release-status-filtered.golden", out.stdout)
}

// TestReleaseWait_SendsBuildIDEquality: every poll of `release wait` asks for
// its one release through a build_id equals filter.
func TestReleaseWait_SendsBuildIDEquality(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	releaseWaitEvery = 10 * time.Second
	steps, fleet := waitSteps()
	f := newReleaseFixture(t, newScriptedReleases(steps...), fleet)
	f.run("release", "wait", relP, "--timeout", "2h").requireExit(t, exitOK)
	sent := requestBodies(t, f, "/orkstack.v1.SystemService/ListReleases", func() *orkstackv1.ListReleasesRequest {
		return &orkstackv1.ListReleasesRequest{}
	})
	if len(sent) < 2 {
		t.Fatalf("sent %d ListReleases requests", len(sent))
	}
	for i, req := range sent {
		if got := filterTexts(req.GetFilters()); strings.Join(got, " ") != "build_id="+relP {
			t.Fatalf("poll %d carried %v; want build_id=%s alone", i, got, relP)
		}
	}
}

// TestReleaseStatusWatch_SendsTheFiltersOnEveryPoll: --watch repeats the
// filters on every poll, so the view and the change it reprints on are of
// the filtered response.
func TestReleaseStatusWatch_SendsTheFiltersOnEveryPoll(t *testing.T) {
	useReleaseClock(t, releaseClockStart)
	releaseWatchLimit = 3
	a := rolloutStep()
	b := rolloutStep()
	b.counts = map[string]int64{relA: 15, relP: 2}
	f := newReleaseFixture(t, newScriptedReleases(a, a, b), releaseFleetFor())
	f.run("release", "status", "--watch", "--filter", "role=current", "--filter", "drained=false").requireExit(t, exitOK)
	sent := requestBodies(t, f, "/orkstack.v1.SystemService/ListReleases", func() *orkstackv1.ListReleasesRequest {
		return &orkstackv1.ListReleasesRequest{}
	})
	if len(sent) != 3 {
		t.Fatalf("sent %d polls, want 3", len(sent))
	}
	for i, req := range sent {
		if got := filterTexts(req.GetFilters()); strings.Join(got, " ") != "role=current drained=false" {
			t.Fatalf("poll %d carried %v", i, got)
		}
	}
}

// shellWords splits a printed command line as a POSIX shell would for the
// quoting orkctl prints: single quotes (with '\” for a quote inside) and a
// backslash outside quotes. It lets a test run a next-page line verbatim.
func shellWords(t *testing.T, line string) []string {
	t.Helper()
	var (
		words   []string
		current strings.Builder
		inWord  bool
		quoted  bool
	)
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quoted:
			if c == '\'' {
				quoted = false
			} else {
				current.WriteByte(c)
			}
		case c == '\'':
			quoted, inWord = true, true
		case c == '\\' && i+1 < len(line):
			i++
			current.WriteByte(line[i])
			inWord = true
		case c == ' ' || c == '\t':
			if inWord {
				words = append(words, current.String())
				current.Reset()
				inWord = false
			}
		default:
			current.WriteByte(c)
			inWord = true
		}
	}
	if quoted {
		t.Fatalf("unterminated quote in %q", line)
	}
	if inWord {
		words = append(words, current.String())
	}
	return words
}
