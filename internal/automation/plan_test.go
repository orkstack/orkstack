package automation

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// identicalLive is what the payments namespace holds after the summary
// document was applied to it.
func identicalLive() Live {
	input, _ := structpb.NewStruct(map[string]any{"mode": "full"})
	return Live{
		Namespace: &orkstackv1.Namespace{
			Name: "payments", DefaultPool: "main", AllowedPools: []string{"main", "gpu"},
			RunRetention: durationpb.New(168 * time.Hour), Etag: "etag-1",
		},
		Schedules: []*orkstackv1.Schedule{{
			Id: "sch-4a1c0e2b7d3f8156", Name: "nightly-reconcile", Namespace: "payments", OrkflowName: "reconcile",
			Input: &orkstackv1.Payload{Data: input},
			Spec: &orkstackv1.ScheduleSpec{
				Cron: []string{"0 2 * * *"}, Timezone: "Europe/Lisbon", Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP,
			},
		}},
		Triggers: []*orkstackv1.Trigger{{
			Id: "trg-3b81d9e0c2f4a617", Name: "github-deploy", Namespace: "payments", OrkflowName: "deploy",
			Description: "GitHub Actions deploy hook",
		}},
		Webhooks: []*orkstackv1.Webhook{{
			Id: "wh-9f2e07c1a4b3d58e", Name: "etl-loader", Url: "https://hooks.acme.dev/etl", Namespace: "payments",
			OrkflowName: "nightly-etl",
			Events:      []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
		}},
		Orkflows: map[OrkflowRef]bool{{Name: "reconcile"}: true, {Name: "deploy"}: true},
	}
}

func plan(t *testing.T, source string, live Live, prune bool) ([]*orkstackv1.AutomationStep, []*orkstackv1.ValidationIssue) {
	t.Helper()
	return Plan(mustParse(t, source), live, Options{Prune: prune})
}

func requireNoErrors(t *testing.T, issues []*orkstackv1.ValidationIssue) {
	t.Helper()
	if HasErrors(issues) {
		t.Fatalf("unexpected issues: %v", issues)
	}
}

// stepFor finds the one step of kind named name (or with id, for unnamed).
func stepFor(t *testing.T, steps []*orkstackv1.AutomationStep, kind orkstackv1.AutomationObjectKind, nameOrID string) *orkstackv1.AutomationStep {
	t.Helper()
	for _, step := range steps {
		if step.GetKind() == kind && (step.GetName() == nameOrID || (step.GetName() == "" && step.GetId() == nameOrID)) {
			return step
		}
	}
	t.Fatalf("no %s step %q among:\n%v", kind, nameOrID, steps)
	return nil
}

func changeOf(step *orkstackv1.AutomationStep, field string) *orkstackv1.AutomationFieldChange {
	for _, change := range step.GetChanges() {
		if change.GetField() == field {
			return change
		}
	}
	return nil
}

func TestPlan_EmptyNamespaceCreatesEverythingInOrder(t *testing.T) {
	live := Live{Orkflows: identicalLive().Orkflows}
	steps, issues := plan(t, summaryDoc, live, false)
	requireNoErrors(t, issues)
	want := []struct {
		kind orkstackv1.AutomationObjectKind
		name string
	}{{kindNamespace, "payments"}, {kindSchedule, "nightly-reconcile"}, {kindTrigger, "github-deploy"}, {kindWebhook, "etl-loader"}}
	if len(steps) != len(want) {
		t.Fatalf("steps = %v", steps)
	}
	for i, w := range want {
		if steps[i].GetAction() != actionCreate || steps[i].GetKind() != w.kind || steps[i].GetName() != w.name {
			t.Errorf("step %d = %v %v %q, want CREATE %v %q", i, steps[i].GetAction(), steps[i].GetKind(), steps[i].GetName(), w.kind, w.name)
		}
	}
	ns := steps[0].GetCreateNamespace().GetNamespace()
	if ns.GetName() != "payments" || ns.GetRunRetention().AsDuration() != 168*time.Hour || len(ns.GetAllowedPools()) != 2 {
		t.Errorf("create_namespace = %v", ns)
	}
	sched := steps[1].GetCreateSchedule()
	if sched.GetOrkflowName() != "reconcile" || sched.GetPaused() || sched.GetNamespace() != "payments" ||
		sched.GetInput().GetData().GetFields()["mode"].GetStringValue() != "full" {
		t.Errorf("create_schedule = %v", sched)
	}
	if tr := steps[2].GetCreateTrigger(); tr.GetName() != "github-deploy" || tr.GetOrkflowName() != "deploy" || tr.GetNamespace() != "payments" {
		t.Errorf("create_trigger = %v", tr)
	}
	hook := steps[3].GetRegisterWebhook()
	if hook.GetSecret() != "" || steps[3].GetSecretEnv() != "ETL_WEBHOOK_SECRET" || hook.GetNamespace() != "payments" ||
		hook.GetName() != "etl-loader" || hook.GetOrkflowName() != "nightly-etl" {
		t.Errorf("register_webhook = %v, secret_env %q", hook, steps[3].GetSecretEnv())
	}
	if c := changeOf(steps[2], "description"); c == nil || c.GetBefore() != "" || c.GetAfter() != `"GitHub Actions deploy hook"` {
		t.Errorf("trigger create changes = %v", steps[2].GetChanges())
	}
	if changeOf(steps[2], "version") != nil {
		t.Error("a create lists a key the entry does not set")
	}
}

func TestPlan_IdenticalLiveIsAllUnchanged(t *testing.T) {
	steps, issues := plan(t, summaryDoc, identicalLive(), true)
	requireNoErrors(t, issues)
	for _, step := range steps {
		if step.GetAction() != actionUnchanged {
			t.Errorf("step %v %q is %v, want UNCHANGED", step.GetKind(), step.GetName(), step.GetAction())
		}
		if step.GetRequest() != nil {
			t.Errorf("an UNCHANGED step carries a request: %v", step)
		}
	}
	if got, want := Digest("payments", steps), Digest("payments", nil); got != want {
		t.Errorf("digest = %s, want the empty plan's %s", got, want)
	}
}

func TestPlan_ScheduleSpecChangeIsAnUpdateWithFieldChanges(t *testing.T) {
	live := identicalLive()
	live.Schedules[0].Spec.Cron = []string{"0 3 * * *"}
	live.Schedules[0].Spec.Jitter = durationpb.New(0)
	steps, issues := plan(t, summaryDoc, live, false)
	requireNoErrors(t, issues)
	step := stepFor(t, steps, kindSchedule, "nightly-reconcile")
	if step.GetAction() != actionUpdate || step.GetId() != "sch-4a1c0e2b7d3f8156" {
		t.Fatalf("step = %v", step)
	}
	if len(step.GetChanges()) != 1 {
		t.Fatalf("changes = %v, want only cron (a zero jitter equals none)", step.GetChanges())
	}
	if c := step.GetChanges()[0]; c.GetField() != "cron" || c.GetBefore() != `["0 3 * * *"]` || c.GetAfter() != `["0 2 * * *"]` {
		t.Errorf("change = %v", c)
	}
	req := step.GetUpdateSchedule()
	if req.GetName() != "nightly-reconcile" || req.GetNamespace() != "payments" || req.GetSpec().GetCron()[0] != "0 2 * * *" ||
		req.GetInput() == nil {
		t.Errorf("update_schedule = %v (a full replacement)", req)
	}
}

func TestPlan_ScheduleUnspecifiedOverlapComparesAsSkip(t *testing.T) {
	live := identicalLive()
	live.Schedules[0].Spec.Overlap = orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED
	steps, _ := plan(t, summaryDoc, live, false)
	if step := stepFor(t, steps, kindSchedule, "nightly-reconcile"); step.GetAction() != actionUnchanged {
		t.Errorf("UNSPECIFIED vs skip = %v, want UNCHANGED", step.GetAction())
	}
}

func TestPlan_TriggerVersionChangeIsAnInPlaceUpdate(t *testing.T) {
	live := identicalLive()
	live.Orkflows[OrkflowRef{Name: "deploy", Version: "1.4.0"}] = true
	source := strings.Replace(summaryDoc, "    orkflow: deploy\n", "    orkflow: deploy\n    version: 1.4.0\n", 1)
	steps, issues := plan(t, source, live, false)
	requireNoErrors(t, issues)
	step := stepFor(t, steps, kindTrigger, "github-deploy")
	req := step.GetUpdateTrigger()
	if step.GetAction() != actionUpdate || req.GetId() != "trg-3b81d9e0c2f4a617" || req.GetOrkflowVersion() != "1.4.0" ||
		req.GetName() != "github-deploy" || req.GetDescription() != "GitHub Actions deploy hook" {
		t.Fatalf("step = %v", step)
	}
	if c := changeOf(step, "version"); c == nil || c.GetBefore() != "current" || c.GetAfter() != `"1.4.0"` {
		t.Errorf("version change = %v", c)
	}
}

func TestPlan_WebhookScopeChangeIsAnInPlaceUpdate(t *testing.T) {
	live := identicalLive()
	live.Webhooks[0].OrkflowName = ""
	steps, issues := plan(t, summaryDoc, live, false)
	requireNoErrors(t, issues)
	step := stepFor(t, steps, kindWebhook, "etl-loader")
	req := step.GetUpdateWebhook()
	if step.GetAction() != actionUpdate || req.GetId() != "wh-9f2e07c1a4b3d58e" || req.GetOrkflowName() != "nightly-etl" ||
		req.GetNamespace() != "payments" || req.GetName() != "etl-loader" {
		t.Fatalf("step = %v", step)
	}
	if c := changeOf(step, "orkflow"); c == nil || c.GetBefore() != `""` || c.GetAfter() != `"nightly-etl"` {
		t.Errorf("change = %v", step.GetChanges())
	}
	if len(step.GetNotes()) != 1 || step.GetNotes()[0] != secretEnvNote {
		t.Errorf("notes = %v, want the secret_env note", step.GetNotes())
	}
}

func TestPlan_BindingChangeIsAnErrorNeverAReplacement(t *testing.T) {
	live := identicalLive()
	live.Triggers[0].OrkflowName = "rollback"
	live.Schedules[0].OrkflowName = "audit"
	steps, issues := plan(t, summaryDoc, live, true)
	requireIssue(t, issues, "triggers.github-deploy.orkflow", "starts rollback", "fixed at creation", "orkctl trigger delete trg-3b81d9e0c2f4a617 -n payments")
	requireIssue(t, issues, "schedules.nightly-reconcile.orkflow", "starts audit", "orkctl schedule delete nightly-reconcile -n payments")
	for _, step := range steps {
		if step.GetName() == "github-deploy" || step.GetName() == "nightly-reconcile" {
			t.Errorf("a binding change produced a step: %v", step)
		}
	}
}

func TestPlan_UndeclaredIsKeptWithoutPruneAndDeletedWithPrune(t *testing.T) {
	live := identicalLive()
	live.Triggers = append(live.Triggers, &orkstackv1.Trigger{Id: "trg-19c0aaaaaaaaaaaa", Name: "manual-hook", Namespace: "payments", OrkflowName: "deploy"})
	steps, _ := plan(t, summaryDoc, live, false)
	if step := stepFor(t, steps, kindTrigger, "manual-hook"); step.GetAction() != actionUndeclared || step.GetRequest() != nil {
		t.Errorf("without --prune = %v", step)
	}
	steps, _ = plan(t, summaryDoc, live, true)
	step := stepFor(t, steps, kindTrigger, "manual-hook")
	if step.GetAction() != actionDelete || step.GetDeleteTrigger().GetId() != "trg-19c0aaaaaaaaaaaa" || step.GetDeleteTrigger().GetNamespace() != "payments" {
		t.Errorf("with --prune = %v", step)
	}
}

func TestPlan_OmittedSectionIsNotManaged(t *testing.T) {
	live := identicalLive()
	steps, _ := plan(t, header+"settings: {}\n", live, true)
	for _, step := range steps {
		if step.GetKind() != kindNamespace {
			t.Errorf("an omitted section produced %v", step)
		}
	}
}

func TestPlan_EmptySectionWithPruneDeletesEveryNamedObject(t *testing.T) {
	live := identicalLive()
	steps, _ := plan(t, header+"schedules: {}\ntriggers: {}\nwebhooks: {}\n", live, true)
	deleted := map[orkstackv1.AutomationObjectKind]bool{}
	for _, step := range steps {
		if step.GetAction() == actionDelete {
			deleted[step.GetKind()] = true
		}
	}
	if !deleted[kindSchedule] || !deleted[kindTrigger] || !deleted[kindWebhook] {
		t.Errorf("deleted = %v, want every named object", deleted)
	}
	if s := stepFor(t, steps, kindWebhook, "etl-loader"); s.GetDeleteWebhook().GetId() != "wh-9f2e07c1a4b3d58e" {
		t.Errorf("delete_webhook = %v", s)
	}
}

func TestPlan_UnnamedObjectsAreNeverDeleted(t *testing.T) {
	live := identicalLive()
	live.Triggers = append(live.Triggers, &orkstackv1.Trigger{Id: "trg-4a1c0e2b7d3f8156", Namespace: "payments", OrkflowName: "deploy"})
	live.Webhooks = append(live.Webhooks, &orkstackv1.Webhook{Id: "wh-0000000000000001", Url: "https://x.test/", Namespace: "payments"})
	steps, _ := plan(t, header+"triggers: {}\nwebhooks: {}\n", live, true)
	trigger := stepFor(t, steps, kindTrigger, "trg-4a1c0e2b7d3f8156")
	hook := stepFor(t, steps, kindWebhook, "wh-0000000000000001")
	for _, step := range []*orkstackv1.AutomationStep{trigger, hook} {
		if step.GetAction() != actionUnmanaged || step.GetRequest() != nil {
			t.Errorf("unnamed object = %v, want UNMANAGED", step)
		}
	}
	if !strings.Contains(trigger.GetNotes()[0], "orkctl trigger update trg-4a1c0e2b7d3f8156 -n payments --name NAME") {
		t.Errorf("trigger note = %v", trigger.GetNotes())
	}
	if !strings.Contains(hook.GetNotes()[0], "orkctl webhook update wh-0000000000000001 --name NAME") {
		t.Errorf("webhook note = %v", hook.GetNotes())
	}
}

func TestPlan_DeploymentWideWebhooksAreNeverRead(t *testing.T) {
	live := identicalLive()
	live.Webhooks = append(live.Webhooks,
		&orkstackv1.Webhook{Id: "wh-0000000000000002", Url: "https://audit.test/"},
		&orkstackv1.Webhook{Id: "wh-0000000000000003", Url: "https://x.test/", Namespace: "billing", Name: "other"})
	steps, _ := plan(t, header+"webhooks: {}\n", live, true)
	for _, step := range steps {
		if step.GetId() == "wh-0000000000000002" || step.GetId() == "wh-0000000000000003" {
			t.Errorf("a subscription outside the namespace reached the plan: %v", step)
		}
	}
}

func TestPlan_DuplicateLiveNamesAreAnError(t *testing.T) {
	live := identicalLive()
	live.Triggers = append(live.Triggers, &orkstackv1.Trigger{Id: "trg-0000000000000009", Name: "github-deploy", Namespace: "payments", OrkflowName: "deploy"})
	steps, issues := plan(t, summaryDoc, live, true)
	requireIssue(t, issues, "triggers", `trigger name "github-deploy" is held by two triggers (trg-0000000000000009, trg-3b81d9e0c2f4a617)`,
		"orkctl trigger delete ID -n payments")
	for _, step := range steps {
		if step.GetName() == "github-deploy" {
			t.Errorf("a step was produced for a shared name: %v", step)
		}
	}
}

func TestPlan_UnavailableScheduleIsAnErrorWhenDeclared(t *testing.T) {
	live := identicalLive()
	live.Schedules[0] = &orkstackv1.Schedule{Id: "sch-4a1c0e2b7d3f8156", Name: "nightly-reconcile", OrkflowName: "reconcile", UnavailableReason: "describe timed out"}
	_, issues := plan(t, summaryDoc, live, false)
	requireIssue(t, issues, "schedules.nightly-reconcile", "schedule could not be read: describe timed out")
}

func TestPlan_UnavailableScheduleIsDeletableWhenUndeclared(t *testing.T) {
	live := identicalLive()
	live.Schedules[0] = &orkstackv1.Schedule{Id: "sch-4a1c0e2b7d3f8156", Name: "nightly-reconcile", OrkflowName: "reconcile", UnavailableReason: "describe timed out"}
	steps, issues := plan(t, header+"schedules: {}\n", live, true)
	requireNoErrors(t, issues)
	step := stepFor(t, steps, kindSchedule, "nightly-reconcile")
	if step.GetAction() != actionDelete || step.GetDeleteSchedule().GetName() != "nightly-reconcile" ||
		len(step.GetNotes()) != 1 || !strings.Contains(step.GetNotes()[0], "could not be described: describe timed out") {
		t.Errorf("step = %v", step)
	}
}

func TestPlan_SettingsOverlayManagesOnlyPresentKeys(t *testing.T) {
	live := identicalLive()
	since := timestamppb.New(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	live.Namespace.Hold = &orkstackv1.NamespaceHold{Reason: "Q3 close", Since: since}
	live.Namespace.Telemetry = &orkstackv1.TelemetryPolicy{Sink: "datadog"}
	live.Namespace.Labels = map[string]string{"team": "payments"}
	steps, issues := plan(t, header+"settings:\n  run_retention: 72h\n", live, false)
	requireNoErrors(t, issues)
	step := stepFor(t, steps, kindNamespace, "payments")
	if step.GetAction() != actionUpdate || len(step.GetChanges()) != 1 {
		t.Fatalf("step = %v", step)
	}
	if c := step.GetChanges()[0]; c.GetField() != "run_retention" || c.GetBefore() != "168h" || c.GetAfter() != "72h" {
		t.Errorf("change = %v", c)
	}
	ns := step.GetUpdateNamespace().GetNamespace()
	if ns.GetEtag() != "etag-1" {
		t.Errorf("update carries etag %q, want the live one", ns.GetEtag())
	}
	if !proto.Equal(ns.GetHold(), live.Namespace.GetHold()) || ns.GetTelemetry().GetSink() != "datadog" || ns.GetLabels()["team"] != "payments" ||
		len(ns.GetAllowedPools()) != 2 {
		t.Errorf("an unwritten key did not survive the overlay: %v", ns)
	}
	if len(step.GetNotes()) != 1 || step.GetNotes()[0] != `on hold since 2026-09-01T00:00:00Z: Q3 close — operational state; this document does not manage it` {
		t.Errorf("notes = %v", step.GetNotes())
	}
}

func TestPlan_SettingsExplicitEmptyClears(t *testing.T) {
	live := identicalLive()
	live.Namespace.Labels = map[string]string{"team": "payments"}
	steps, _ := plan(t, header+"settings:\n  allowed_pools: []\n  run_retention: inherit\n  labels: {}\n", live, false)
	ns := stepFor(t, steps, kindNamespace, "payments").GetUpdateNamespace().GetNamespace()
	if len(ns.GetAllowedPools()) != 0 || ns.GetRunRetention() != nil || len(ns.GetLabels()) != 0 {
		t.Errorf("explicit empty values did not clear: %v", ns)
	}
	if ns.GetDefaultPool() != "main" {
		t.Errorf("an unwritten key changed: %v", ns)
	}
}

func TestPlan_DefaultNamespaceIsNeverCreated(t *testing.T) {
	source := strings.Replace(header, "payments", "default", 1) + "settings:\n  description: the default\n"
	steps, _ := plan(t, source, Live{}, false)
	step := stepFor(t, steps, kindNamespace, "default")
	if step.GetAction() != actionUpdate || step.GetUpdateNamespace().GetNamespace().GetEtag() != "" {
		t.Errorf("default = %v, want an UPDATE of the synthesized document", step)
	}
	steps, _ = plan(t, strings.Replace(header, "payments", "default", 1)+"triggers: {}\n", Live{}, false)
	for _, step := range steps {
		if step.GetAction() == actionCreate && step.GetKind() == kindNamespace {
			t.Errorf("default was created: %v", step)
		}
	}
}

func TestPlan_MissingOrkflowIsAnErrorUnlessPending(t *testing.T) {
	live := Live{Orkflows: map[OrkflowRef]bool{}}
	source := header + "triggers:\n  a:\n    orkflow: deploy\n  b:\n    orkflow: deploy\n    version: '2'\n  c:\n    orkflow: deploy\n    version: current\n"
	_, issues := plan(t, source, live, false)
	requireIssue(t, issues, "triggers.a.orkflow", `orkflow "deploy" has no stored version in namespace "payments"`)
	requireIssue(t, issues, "triggers.b.version", `orkflow "deploy" version "2" is not stored in namespace "payments"`)
	requireIssue(t, issues, "triggers.c.version", "(omit `version` to follow the current pointer)")
	live.Orkflows = map[OrkflowRef]bool{{Name: "deploy"}: true, {Name: "deploy", Version: "2"}: true, {Name: "deploy", Version: "current"}: true}
	_, issues = plan(t, source, live, false)
	requireNoErrors(t, issues)
}

func TestPlan_StepsAreOrderedAdditiveFirstDestructiveLast(t *testing.T) {
	live := identicalLive()
	live.Namespace.RunRetention = nil
	live.Triggers[0].Description = "old"
	live.Triggers = append(live.Triggers,
		&orkstackv1.Trigger{Id: "trg-0000000000000001", Name: "zz-old", Namespace: "payments", OrkflowName: "deploy"},
		&orkstackv1.Trigger{Id: "trg-0000000000000002", Namespace: "payments", OrkflowName: "deploy"})
	live.Webhooks = nil
	steps, _ := plan(t, summaryDoc, live, true)
	var order []string
	for _, step := range steps {
		order = append(order, ActionSlug(step.GetAction())+" "+KindSlug(step.GetKind()))
	}
	want := []string{"update namespace", "create webhook", "update trigger", "delete trigger", "unchanged schedule", "unmanaged trigger"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Errorf("order = %v, want %v", order, want)
	}
}

func TestPlan_ProbableRenameWarns(t *testing.T) {
	live := identicalLive()
	live.Triggers[0].Name = "gh-deploy"
	steps, issues := plan(t, summaryDoc, live, false)
	requireNoErrors(t, issues)
	if stepFor(t, steps, kindTrigger, "github-deploy").GetAction() != actionCreate {
		t.Fatal("the renamed entry is not a create")
	}
	found := false
	for _, issue := range issues {
		if issue.GetSeverity() == orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING && issue.GetField() == "triggers.github-deploy" &&
			strings.Contains(issue.GetMessage(), "matches undeclared trigger gh-deploy (trg-3b81d9e0c2f4a617)") &&
			strings.Contains(issue.GetMessage(), "orkctl trigger update trg-3b81d9e0c2f4a617 -n payments --name github-deploy") {
			found = true
		}
	}
	if !found {
		t.Errorf("no rename warning among %v", issues)
	}
}

func TestPlan_NotesForPausedHoldAndSecretEnv(t *testing.T) {
	live := identicalLive()
	live.Schedules[0].Paused = true
	live.Schedules[0].Note = "Q3 close"
	steps, _ := plan(t, summaryDoc, live, false)
	if notes := stepFor(t, steps, kindSchedule, "nightly-reconcile").GetNotes(); len(notes) != 1 ||
		notes[0] != `paused: "Q3 close" — operational state; this document does not manage it` {
		t.Errorf("schedule notes = %v", notes)
	}
	if notes := stepFor(t, steps, kindWebhook, "etl-loader").GetNotes(); len(notes) != 1 || notes[0] != secretEnvNote {
		t.Errorf("webhook notes = %v", notes)
	}
}
