package automation

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// summaryDoc is the docs/AUTOMATION.md (spec §Summary) document.
const summaryDoc = `# environments/prod/automation.ork.yaml
apiVersion: orkstack/v1
kind: Automation
namespace: payments

settings:
  default_pool: main
  allowed_pools: [main, gpu]
  run_retention: 168h

schedules:
  nightly-reconcile:
    orkflow: reconcile
    cron: "0 2 * * *"
    timezone: Europe/Lisbon
    overlap: skip
    input: '{"mode": "full"}'

triggers:
  github-deploy:
    orkflow: deploy
    description: GitHub Actions deploy hook

webhooks:
  etl-loader:
    url: https://hooks.acme.dev/etl
    orkflow: nightly-etl
    events: [TASK_SUCCEEDED, RUN_FAILED]
    secret_env: ETL_WEBHOOK_SECRET
`

const header = "apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\n"

func mustParse(t *testing.T, source string) *Document {
	t.Helper()
	doc, issues := Parse([]byte(source))
	if HasErrors(issues) || doc == nil {
		t.Fatalf("Parse: %v", issues)
	}
	return doc
}

// requireIssue fails unless issues has an ERROR on field whose message
// contains every fragment.
func requireIssue(t *testing.T, issues []*orkstackv1.ValidationIssue, field string, fragments ...string) {
	t.Helper()
	for _, issue := range issues {
		if issue.GetField() != field || issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
			continue
		}
		ok := true
		for _, fragment := range fragments {
			ok = ok && strings.Contains(issue.GetMessage(), fragment)
		}
		if ok {
			return
		}
	}
	t.Fatalf("no ERROR on %s containing %q among:\n%v", field, fragments, issues)
}

func TestParse_AcceptsTheSummaryDocument(t *testing.T) {
	doc := mustParse(t, summaryDoc)
	if doc.Namespace != "payments" {
		t.Errorf("namespace = %q", doc.Namespace)
	}
	if doc.Settings == nil || !doc.Settings.Present["default_pool"] || !doc.Settings.Present["allowed_pools"] ||
		!doc.Settings.Present["run_retention"] || len(doc.Settings.Present) != 3 {
		t.Errorf("settings present = %v", doc.Settings.Present)
	}
	if got := doc.Settings.Values.GetRunRetention().AsDuration().String(); got != "168h0m0s" {
		t.Errorf("run_retention = %s", got)
	}
	s := doc.Schedules["nightly-reconcile"]
	if s.Orkflow != "reconcile" || s.Version != "" || len(s.Spec.GetCron()) != 1 || s.Spec.GetCron()[0] != "0 2 * * *" ||
		s.Spec.GetTimezone() != "Europe/Lisbon" || s.Spec.GetOverlap() != orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP ||
		s.Input.GetData().GetFields()["mode"].GetStringValue() != "full" {
		t.Errorf("schedule = %+v", s)
	}
	if tr := doc.Triggers["github-deploy"]; tr.Orkflow != "deploy" || tr.Description != "GitHub Actions deploy hook" {
		t.Errorf("trigger = %+v", tr)
	}
	w := doc.Webhooks["etl-loader"]
	if w.URL != "https://hooks.acme.dev/etl" || w.Orkflow != "nightly-etl" || w.SecretEnv != "ETL_WEBHOOK_SECRET" ||
		len(w.Events) != 2 || w.Events[0] != webhooks.EventTaskSucceeded || w.Events[1] != webhooks.EventRunFailed {
		t.Errorf("webhook = %+v (events in lifecycle order)", w)
	}
	if got := doc.Managed(); len(got) != 4 {
		t.Errorf("managed = %v", got)
	}
}

func TestParse_HeaderIsRequiredAndExact(t *testing.T) {
	cases := map[string]struct{ source, field, fragment string }{
		"missing apiVersion": {"kind: Automation\nnamespace: payments\n", "apiVersion", "apiVersion is required"},
		"other apiVersion":   {"apiVersion: orkstack/v2\nkind: Automation\nnamespace: payments\n", "apiVersion", "want orkstack/v1"},
		"missing kind":       {"apiVersion: orkstack/v1\nnamespace: payments\n", "kind", "kind is required"},
		"other kind":         {"apiVersion: orkstack/v1\nkind: Orkflow\nnamespace: payments\n", "kind", "want Automation"},
		"missing namespace":  {"apiVersion: orkstack/v1\nkind: Automation\n", "namespace", "never inferred"},
		"non-slug namespace": {"apiVersion: orkstack/v1\nkind: Automation\nnamespace: Payments\n", "namespace", "line 3"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			doc, issues := Parse([]byte(tc.source))
			if doc != nil {
				t.Fatal("a document with a bad header was parsed")
			}
			requireIssue(t, issues, tc.field, tc.fragment)
		})
	}
}

func TestParse_UnknownKeysAreRefusedAtEveryLevel(t *testing.T) {
	cases := map[string]struct{ body, field, fragment string }{
		"top level": {"labels: {}\n", "labels", "allowed: apiVersion, kind, namespace, settings, schedules, triggers, webhooks"},
		"settings":  {"settings:\n  pools: [main]\n", "settings.pools", "allowed: description, labels"},
		"telemetry": {"settings:\n  telemetry:\n    endpoint: x\n", "settings.telemetry", "unknown key telemetry.endpoint"},
		"schedule":  {"schedules:\n  s:\n    orkflow: a\n    cron: '@daily'\n    paused: true\n", "schedules.s.paused", "allowed: orkflow, version"},
		"trigger":   {"triggers:\n  t:\n    orkflow: a\n    token: x\n", "triggers.t.token", "allowed: orkflow, version, description"},
		"webhook":   {"webhooks:\n  w:\n    url: https://x.test/\n    secret: s\n", "webhooks.w.secret", "allowed: url, events"},
		"webhook namespace": {"webhooks:\n  w:\n    url: https://x.test/\n    namespace: payments\n",
			"webhooks.w.namespace", "scoped to that namespace"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, issues := Parse([]byte(header + tc.body))
			requireIssue(t, issues, tc.field, tc.fragment)
		})
	}
}

func TestParse_SectionPresenceIsDistinct(t *testing.T) {
	doc := mustParse(t, header)
	if doc.Settings != nil || doc.Schedules != nil || doc.Triggers != nil || doc.Webhooks != nil {
		t.Fatal("an absent section was parsed as managed")
	}
	doc = mustParse(t, header+"triggers: {}\nschedules: {}\n")
	if doc.Triggers == nil || len(doc.Triggers) != 0 || doc.Schedules == nil {
		t.Fatal("{} was not parsed as managed and empty")
	}
	if doc.Webhooks != nil {
		t.Fatal("an absent webhooks section was parsed as managed")
	}
	_, issues := Parse([]byte(header + "triggers:\n"))
	requireIssue(t, issues, "triggers", "write `triggers: {}` to manage no triggers", "remove the key to leave triggers unmanaged")
	_, issues = Parse([]byte(header + "---\n" + header))
	requireIssue(t, issues, "", "one automation document per file")
}

func TestParse_SettingsRecordOnlyPresentKeys(t *testing.T) {
	doc := mustParse(t, header+"settings:\n  run_retention: inherit\n  allowed_pools: []\n  max_running: 0\n")
	want := map[string]bool{"run_retention": true, "allowed_pools": true, "max_running": true}
	if len(doc.Settings.Present) != len(want) {
		t.Fatalf("present = %v, want %v", doc.Settings.Present, want)
	}
	for key := range want {
		if !doc.Settings.Present[key] {
			t.Errorf("%s not present", key)
		}
	}
	if doc.Settings.Values.GetRunRetention() != nil || len(doc.Settings.Values.GetAllowedPools()) != 0 {
		t.Errorf("explicit clearing values parsed as %v", doc.Settings.Values)
	}
	_, issues := Parse([]byte(header + "settings:\n  max_running: -1\n"))
	requireIssue(t, issues, "settings.max_running", "at least 0")
	_, issues = Parse([]byte(header + "settings:\n  run_retention: 0s\n"))
	requireIssue(t, issues, "settings.run_retention", "positive")
	_, issues = Parse([]byte(header + "settings:\n  telemetry:\n    task_logs: loud\n"))
	requireIssue(t, issues, "settings.telemetry", "enabled, disabled, inherit")
	doc = mustParse(t, header+"settings:\n  telemetry: {}\n")
	if !doc.Settings.Present["telemetry"] || doc.Settings.Values.GetTelemetry() != nil {
		t.Error("an all-zero telemetry policy is not written as unset")
	}
}

func TestParse_SettingsHoldIsRefusedAsOperational(t *testing.T) {
	_, issues := Parse([]byte(header + "settings:\n  hold:\n    reason: freeze\n"))
	requireIssue(t, issues, "settings.hold", "operational state", "orkctl namespace update --hold")
}

func TestParse_InputMustBeAJSONObjectString(t *testing.T) {
	entry := func(input string) string {
		return header + "schedules:\n  s:\n    orkflow: a\n    cron: '@daily'\n    input: " + input + "\n"
	}
	_, issues := Parse([]byte(entry("{ mode: full }")))
	requireIssue(t, issues, "schedules.s.input", "line 8", "input must be a JSON object written as a string", "never YAML (AGENTS.md §3.1)")
	for _, bad := range []string{`'[1]'`, `'nope'`, `'{"a": no}'`, "[1]", "12"} {
		if _, issues := Parse([]byte(entry(bad))); !HasErrors(issues) {
			t.Errorf("input %s was accepted", bad)
		}
	}
	doc := mustParse(t, entry(`'{}'`))
	if doc.Schedules["s"].Input != nil {
		t.Error("input '{}' was not recorded as unset")
	}
	doc = mustParse(t, entry(`'{"n": 1, "b": true}'`))
	fields := doc.Schedules["s"].Input.GetData().GetFields()
	if fields["n"].GetNumberValue() != 1 || !fields["b"].GetBoolValue() {
		t.Errorf("JSON types lost: %v", fields)
	}
}

func TestParse_CronScalarOrList(t *testing.T) {
	doc := mustParse(t, header+"schedules:\n  a:\n    orkflow: x\n    cron: '@daily'\n  b:\n    orkflow: x\n    cron: ['0 1 * * *', '0 2 * * *']\n")
	if got := doc.Schedules["a"].Spec.GetCron(); len(got) != 1 || got[0] != "@daily" {
		t.Errorf("scalar cron = %v", got)
	}
	if got := doc.Schedules["b"].Spec.GetCron(); len(got) != 2 || got[1] != "0 2 * * *" {
		t.Errorf("list cron = %v", got)
	}
	_, issues := Parse([]byte(header + "schedules:\n  a:\n    orkflow: x\n    cron: ['@daily', '']\n"))
	requireIssue(t, issues, "schedules.a.cron[1]", "line 7")
}

func TestParse_ScheduleRulesMatchValidateScheduleSpec(t *testing.T) {
	base := header + "schedules:\n  s:\n    orkflow: a\n"
	cases := map[string]struct{ body, field, fragment string }{
		"CRON_TZ":         {"    cron: 'CRON_TZ=UTC 0 2 * * *'\n", "schedules.s.cron", "CRON_TZ"},
		"zero interval":   {"    interval: 0s\n", "schedules.s.interval", "positive"},
		"negative jitter": {"    cron: '@daily'\n    jitter: -1s\n", "schedules.s.jitter", "negative"},
		"short catch-up":  {"    cron: '@daily'\n    catchup_window: 5s\n", "schedules.s.catchup_window", "at least 10s"},
		"start after end": {"    cron: '@daily'\n    start_at: '2030-01-02T00:00:00Z'\n    end_at: '2030-01-01T00:00:00Z'\n", "schedules.s.start_at", "after end_at"},
		"bad overlap":     {"    cron: '@daily'\n    overlap: terminate-other\n", "schedules.s.overlap", "skip, buffer-one, buffer-all, cancel-other, allow-all"},
		"bad timezone":    {"    cron: '@daily'\n    timezone: Mars/Olympus\n", "schedules.s.timezone", "IANA"},
		"no when":         {"", "schedules.s", "a cron or an interval"},
		"long text":       {"    cron: '@daily'\n    description: " + strings.Repeat("x", 257) + "\n", "schedules.s.description", "limit is 256"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, issues := Parse([]byte(base + tc.body))
			requireIssue(t, issues, tc.field, tc.fragment, "line ")
		})
	}
}

func TestParse_WebhookRulesAreTheRegisterRules(t *testing.T) {
	base := header + "webhooks:\n  w:\n"
	cases := map[string]struct{ body, field, fragment string }{
		"bad url":            {"    url: ftp://x.test/\n", "webhooks.w.url", "scheme must be http or https"},
		"task log":           {"    url: https://x.test/\n    events: [TASK_LOG]\n", "webhooks.w.events[0]", "unknown event"},
		"task without event": {"    url: https://x.test/\n    task: extract\n", "webhooks.w.task", "needs at least one task event"},
		"bad secret_env":     {"    url: https://x.test/\n    secret_env: lower\n", "webhooks.w.secret_env", "environment variable"},
		"missing url":        {"    orkflow: a\n", "webhooks.w.url", "needs a url"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, issues := Parse([]byte(base + tc.body))
			requireIssue(t, issues, tc.field, tc.fragment)
		})
	}
}

func TestParse_ReportsEveryErrorInOnePass(t *testing.T) {
	_, issues := Parse([]byte(header + `settings:
  max_running: lots
schedules:
  s:
    orkflow: a
    cron: '@daily'
    overlap: never
triggers:
  t:
    orkflow: Not-A-Slug
webhooks:
  w:
    url: nope
`))
	for _, field := range []string{"settings.max_running", "schedules.s.overlap", "triggers.t.orkflow", "webhooks.w.url"} {
		requireIssue(t, issues, field)
	}
}

func TestParse_OverlapSlugsAreTheEnumNames(t *testing.T) {
	for number, name := range orkstackv1.OverlapPolicy_name {
		policy := orkstackv1.OverlapPolicy(number)
		if policy == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
			continue
		}
		want := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(name, "OVERLAP_POLICY_")), "_", "-")
		if got := overlapSlug(policy); got != want {
			t.Errorf("overlapSlug(%s) = %q, want %q", name, got, want)
		}
		if back, ok := parseOverlap(want); !ok || back != policy {
			t.Errorf("parseOverlap(%q) = %v, %v", want, back, ok)
		}
	}
	doc := mustParse(t, header+"schedules:\n  s:\n    orkflow: a\n    cron: '@daily'\n")
	if !proto.Equal(doc.Schedules["s"].Spec, &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP}) {
		t.Errorf("absent overlap is not recorded as skip: %v", doc.Schedules["s"].Spec)
	}
}
