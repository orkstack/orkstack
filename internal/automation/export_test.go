package automation

import (
	"flag"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

var update = flag.Bool("update", false, "rewrite the golden files under testdata/")

func requireGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run with -update to create it): %v", path, err)
	}
	if string(want) != got {
		t.Fatalf("%s differs from the golden file:\n--- got ---\n%s\n--- want ---\n%s", name, got, want)
	}
}

// fullLive is a namespace with every section populated, a paused schedule,
// and an unnamed trigger and webhook.
func fullLive() Live {
	live := identicalLive()
	live.Namespace.Description = "card rails"
	live.Namespace.Labels = map[string]string{"team": "payments", "tier": "1"}
	live.Namespace.MaxRunning = 5
	live.Namespace.AllowedSecrets = []string{"env:PAYMENTS_*"}
	live.Namespace.RequireSignedBy = "cosign:release"
	live.Namespace.Budget = &orkstackv1.Budget{CpuSeconds: 3600, GibSeconds: 7200, Period: durationpb.New(720 * time.Hour), Enforce: true}
	live.Namespace.Telemetry = &orkstackv1.TelemetryPolicy{
		Sink: "datadog", TaskLogs: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED, Attributes: map[string]string{"cost_center": "42"},
	}
	live.Namespace.Hold = &orkstackv1.NamespaceHold{Reason: "Q3 close"}
	live.Schedules = append(live.Schedules, &orkstackv1.Schedule{
		Id: "sch-0000000000000002", Name: "hourly-sync", Namespace: "payments", OrkflowName: "sync", OrkflowVersion: "2",
		Description: "hourly ledger sync", Paused: true, Note: "investigating INC-42",
		Spec: &orkstackv1.ScheduleSpec{
			Interval: durationpb.New(time.Hour), Jitter: durationpb.New(30 * time.Second),
			StartAt:       timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			EndAt:         timestamppb.New(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
			Overlap:       orkstackv1.OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE,
			CatchupWindow: durationpb.New(10 * time.Minute), PauseOnFailure: true,
		},
	})
	live.Triggers = append(live.Triggers,
		&orkstackv1.Trigger{Id: "trg-77aa000000000000", Name: "ci-hook", Namespace: "payments", OrkflowName: "deploy", OrkflowVersion: "3"},
		&orkstackv1.Trigger{Id: "trg-4a1c0e2b7d3f8156", Namespace: "payments", OrkflowName: "deploy"})
	live.Webhooks = append(live.Webhooks,
		&orkstackv1.Webhook{Id: "wh-1111111111111111", Url: "https://audit.test/", Namespace: "payments"},
		&orkstackv1.Webhook{Id: "wh-2222222222222222", Url: "https://everyone.test/"})
	live.Orkflows[OrkflowRef{Name: "sync", Version: "2"}] = true
	live.Orkflows[OrkflowRef{Name: "deploy", Version: "3"}] = true
	return live
}

func TestExport_Golden(t *testing.T) {
	cases := map[string]Live{
		"export-full.golden":    fullLive(),
		"export-empty.golden":   {Namespace: &orkstackv1.Namespace{Name: "payments"}},
		"export-default.golden": {},
	}
	for name, live := range cases {
		t.Run(name, func(t *testing.T) {
			ns := "payments"
			if name == "export-default.golden" {
				ns = "default"
			}
			yaml, _, err := Export(ns, live)
			if err != nil {
				t.Fatalf("Export: %v", err)
			}
			requireGolden(t, name, yaml)
		})
	}
}

func TestExport_RoundTripPlansNothing(t *testing.T) {
	cases := map[string]struct {
		ns   string
		live Live
	}{
		"full":    {"payments", fullLive()},
		"summary": {"payments", identicalLive()},
		"empty":   {"payments", Live{Namespace: &orkstackv1.Namespace{Name: "payments"}}},
		"default": {"default", Live{}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			yaml, unmanaged, err := Export(tc.ns, tc.live)
			if err != nil {
				t.Fatalf("Export: %v", err)
			}
			doc, issues := Parse([]byte(yaml))
			if HasErrors(issues) {
				t.Fatalf("the export does not parse: %v\n%s", issues, yaml)
			}
			steps, issues := Plan(doc, tc.live, Options{Prune: true})
			if len(issues) != 0 {
				t.Fatalf("plan of the export has issues: %v\n%s", issues, yaml)
			}
			unmanagedSteps := 0
			for _, step := range steps {
				switch step.GetAction() {
				case actionUnchanged:
				case actionUnmanaged:
					unmanagedSteps++
				default:
					t.Errorf("plan of the export has %v %v %q: %v\n%s", step.GetAction(), step.GetKind(), step.GetName(), step.GetChanges(), yaml)
				}
			}
			if unmanagedSteps != len(unmanaged) {
				t.Errorf("export listed %d unmanaged objects, the plan %d", len(unmanaged), unmanagedSteps)
			}
		})
	}
}

func TestExport_IsDeterministic(t *testing.T) {
	want, _, err := Export("payments", fullLive())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 20; i++ {
		live := fullLive()
		rng.Shuffle(len(live.Schedules), func(a, b int) { live.Schedules[a], live.Schedules[b] = live.Schedules[b], live.Schedules[a] })
		rng.Shuffle(len(live.Triggers), func(a, b int) { live.Triggers[a], live.Triggers[b] = live.Triggers[b], live.Triggers[a] })
		rng.Shuffle(len(live.Webhooks), func(a, b int) { live.Webhooks[a], live.Webhooks[b] = live.Webhooks[b], live.Webhooks[a] })
		got, _, err := Export("payments", live)
		if err != nil || got != want {
			t.Fatalf("export %d differs (%v):\n%s\nwant\n%s", i, err, got, want)
		}
	}
}

func TestExport_RefusesUnavailableOrDuplicate(t *testing.T) {
	live := fullLive()
	live.Schedules[0].UnavailableReason = "describe timed out"
	_, _, err := Export("payments", live)
	if err == nil || !strings.Contains(err.Error(), `schedule "nightly-reconcile" could not be read (describe timed out)`) ||
		!strings.Contains(err.Error(), "would delete it when applied with --prune") {
		t.Errorf("unavailable = %v", err)
	}
	live = fullLive()
	live.Triggers = append(live.Triggers, &orkstackv1.Trigger{Id: "trg-0000000000000009", Name: "ci-hook", Namespace: "payments", OrkflowName: "deploy"})
	if _, _, err := Export("payments", live); err == nil || !strings.Contains(err.Error(), `trigger name "ci-hook" is held by two triggers`) {
		t.Errorf("duplicate trigger = %v", err)
	}
	live = fullLive()
	live.Webhooks = append(live.Webhooks, &orkstackv1.Webhook{Id: "wh-0000000000000009", Name: "etl-loader", Url: "https://x.test/", Namespace: "payments"})
	if _, _, err := Export("payments", live); err == nil || !strings.Contains(err.Error(), `webhook name "etl-loader" is held by two webhooks`) {
		t.Errorf("duplicate webhook = %v", err)
	}
	if _, _, err := Export("payments", Live{}); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("absent namespace = %v", err)
	}
}

func TestAutomationSettings_ClassifyEveryNamespaceField(t *testing.T) {
	managed := map[protoreflect.Name]string{}
	for _, row := range namespaceSettings {
		if _, dup := managed[row.field]; dup {
			t.Errorf("field %s is managed by two keys", row.field)
		}
		managed[row.field] = row.key
	}
	fields := (&orkstackv1.Namespace{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		name := fields.Get(i).Name()
		_, isManaged := managed[name]
		_, isUnmanaged := unmanagedNamespaceFields[name]
		switch {
		case isManaged && isUnmanaged:
			t.Errorf("Namespace.%s is in both the managed and the unmanaged table", name)
		case !isManaged && !isUnmanaged:
			t.Errorf("Namespace.%s (field %d) is not classified: add a managed key to namespaceSettings "+
				"(spelled as its CLI flag spells it) or an unmanaged row with its reason (internal/automation/settings.go)",
				name, fields.Get(i).Number())
		}
	}
	for name := range managed {
		if fields.ByName(name) == nil {
			t.Errorf("managed key names Namespace.%s, which does not exist", name)
		}
	}
	for name := range unmanagedNamespaceFields {
		if fields.ByName(name) == nil {
			t.Errorf("unmanaged row names Namespace.%s, which does not exist", name)
		}
	}
}

func TestAutomationKindAndActionSlugs_MatchTheEnums(t *testing.T) {
	kinds := orkstackv1.AutomationObjectKind(0).Descriptor().Values()
	for i := 0; i < kinds.Len(); i++ {
		value := kinds.Get(i)
		kind := orkstackv1.AutomationObjectKind(value.Number())
		want := strings.ToLower(strings.TrimPrefix(string(value.Name()), "AUTOMATION_OBJECT_KIND_"))
		if kind == orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_UNSPECIFIED {
			want = ""
		}
		if got := KindSlug(kind); got != want {
			t.Errorf("KindSlug(%s) = %q, want %q", value.Name(), got, want)
		}
		if want != "" && SectionKey(kind) == "" {
			t.Errorf("kind %s has no section key", value.Name())
		}
	}
	actions := orkstackv1.AutomationAction(0).Descriptor().Values()
	for i := 0; i < actions.Len(); i++ {
		value := actions.Get(i)
		action := orkstackv1.AutomationAction(value.Number())
		if action == orkstackv1.AutomationAction_AUTOMATION_ACTION_UNSPECIFIED {
			continue
		}
		if want := strings.ToLower(strings.TrimPrefix(string(value.Name()), "AUTOMATION_ACTION_")); ActionSlug(action) != want {
			t.Errorf("ActionSlug(%s) = %q, want %q", value.Name(), ActionSlug(action), want)
		}
		if ActionSymbol(action) == "" {
			t.Errorf("action %s has no symbol", value.Name())
		}
	}
}

func TestDigest_CoversActionableStepsOnly(t *testing.T) {
	live := identicalLive()
	live.Namespace.RunRetention = nil
	steps, _ := plan(t, summaryDoc, live, false)
	base := Digest("payments", steps)
	if !strings.HasPrefix(base, "sha256:") || len(base) != len("sha256:")+64 {
		t.Fatalf("digest = %q", base)
	}
	if again := Digest("payments", steps); again != base {
		t.Fatal("the digest is not stable")
	}
	withUndeclared := append(append([]*orkstackv1.AutomationStep(nil), steps...),
		&orkstackv1.AutomationStep{Action: actionUndeclared, Kind: kindTrigger, Name: "x", Id: "trg-1"})
	if Digest("payments", withUndeclared) != base {
		t.Error("an UNDECLARED step moved the digest")
	}
	moved := identicalLive()
	moved.Namespace.RunRetention = nil
	moved.Namespace.Etag = "etag-99"
	steps2, _ := plan(t, summaryDoc, moved, false)
	if Digest("payments", steps2) != base {
		t.Error("a new etag moved the digest")
	}
	if Digest("billing", steps) == base {
		t.Error("another namespace did not move the digest")
	}
	withDelete := append(append([]*orkstackv1.AutomationStep(nil), steps...),
		&orkstackv1.AutomationStep{Action: actionDelete, Kind: kindTrigger, Name: "x", Id: "trg-1"})
	if Digest("payments", withDelete) == base {
		t.Error("a new DELETE did not move the digest")
	}
	moved.Namespace.RunRetention = durationpb.New(time.Hour)
	steps3, _ := plan(t, summaryDoc, moved, false)
	if Digest("payments", steps3) == base {
		t.Error("a different before did not move the digest")
	}
}

func TestRender_Golden(t *testing.T) {
	input := &orkstackv1.Payload{}
	doc := mustParse(t, header+"schedules:\n  s:\n    orkflow: a\n    cron: '@daily'\n    input:\n      z: 1\n      a: [true, null]\n      m: {k: v}\n")
	input = doc.Schedules["s"].Input
	lines := []string{
		"string: " + renderString("say \"hi\"\n"),
		"unpinned version: " + renderVersion(""),
		"pinned current: " + renderVersion("current"),
		"list: " + renderStrings([]string{"main", "gpu"}),
		"empty list: " + renderStrings(nil),
		"map: " + renderMap(map[string]string{"z": "1", "a": "2"}),
		"empty map: " + renderMap(nil),
		"duration 168h: " + renderDuration(durationpb.New(168*time.Hour)),
		"duration 1h30m: " + renderDuration(durationpb.New(90*time.Minute)),
		"duration 30s: " + renderDuration(durationpb.New(30*time.Second)),
		"duration 1h0m30s: " + renderDuration(durationpb.New(time.Hour+30*time.Second)),
		"duration unset: " + renderDuration(nil),
		"run_retention unset: " + renderRetention(nil),
		"timestamp: " + renderTimestamp(timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 3600)))),
		"timestamp unset: " + renderTimestamp(nil),
		"overlap: " + overlapSlug(orkstackv1.OverlapPolicy_OVERLAP_POLICY_CANCEL_OTHER),
		"overlap unspecified: " + overlapSlug(orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED),
		"task_logs: " + taskLogsSlug(orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED),
		"input: " + renderInput(input),
		"input unset: " + renderInput(nil),
		"telemetry: " + renderTelemetry(&orkstackv1.TelemetryPolicy{Sink: "datadog", Attributes: map[string]string{"b": "2", "a": "1"}}),
		"telemetry unset: " + renderTelemetry(nil),
		"events: " + renderEvents([]webhooks.Event{webhooks.EventTaskSucceeded, webhooks.EventRunFailed}),
		"events every run transition: " + renderEvents(nil),
		"schedule summary: " + scheduleSummary("reconcile", "", &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Timezone: "Europe/Lisbon"}),
		"schedule summary interval: " + scheduleSummary("sync", "2", &orkstackv1.ScheduleSpec{Cron: []string{"@daily"}, Interval: durationpb.New(time.Hour)}),
		"trigger summary: " + triggerSummary("deploy", ""),
		"webhook summary: " + webhookSummary("https://hooks.acme.dev/etl", "payments", "nightly-etl", ""),
	}
	requireGolden(t, "render.golden", strings.Join(lines, "\n")+"\n")
}

// TestExport_WritesInputAsAYAMLMapping: an exported schedule input is a block
// mapping, keys sorted — the shape Parse reads.
func TestExport_WritesInputAsAYAMLMapping(t *testing.T) {
	yaml, _, err := Export("payments", fullLive())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if !strings.Contains(yaml, "    input:\n      mode: full\n") || strings.Contains(yaml, `input: '`) {
		t.Fatalf("input is not written as a YAML mapping:\n%s", yaml)
	}
}

// TestExport_InputRoundTripsThroughTheStrictMapping: every string a YAML
// reader could read as something else is quoted on export, so Parse reads
// the export back to the same input.
func TestExport_InputRoundTripsThroughTheStrictMapping(t *testing.T) {
	values := map[string]any{
		"yes": "yes", "octal": "0755", "sexagesimal": "1:30", "date": "2026-01-02", "tilde": "~",
		"empty": "", "space": " x", "colon": "a: b", "hash": "#c", "true-text": "true", "number-text": "12",
		"zero": 0.0, "negative-zero": math.Copysign(0, -1), "bound": float64(1 << 53), "past-bound": float64(1<<53 + 2),
		"big": 1e21, "small": 1e-7, "tenth": 0.1, "flag": true, "nothing": nil,
		"list": []any{"on", 1.5, []any{}}, "nested": map[string]any{"0755": "key", "k": map[string]any{}},
	}
	data, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatal(err)
	}
	live := identicalLive()
	live.Schedules[0].Input = &orkstackv1.Payload{Data: data}
	exported, _, err := Export("payments", live)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	doc, issues := Parse([]byte(exported))
	if HasErrors(issues) {
		t.Fatalf("Parse(Export) = %v\n%s", issues, exported)
	}
	if got := doc.Schedules["nightly-reconcile"].Input; !proto.Equal(got.GetData(), data) {
		t.Fatalf("input changed across export and parse:\n got %v\nwant %v\n%s", got.GetData(), data, exported)
	}
}
