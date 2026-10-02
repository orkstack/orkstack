package api_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// writeCounter counts every write any fake below receives. PlanAutomation
// and ExportAutomation must leave it at zero.
type writeCounter struct {
	mu     sync.Mutex
	writes []string
}

func (c *writeCounter) add(what string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, what)
}

func (c *writeCounter) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.writes...)
}

// countingRegistry is a namespace registry that counts writes and can fail
// reads.
type countingRegistry struct {
	*fakeRegistry
	counter *writeCounter
	getErr  error
}

func (r *countingRegistry) Get(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.fakeRegistry.Get(ctx, name)
}

func (r *countingRegistry) Put(ctx context.Context, ns *orkstackv1.Namespace, cond artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	r.counter.add("namespace.put")
	return r.fakeRegistry.Put(ctx, ns, cond)
}

func (r *countingRegistry) Delete(ctx context.Context, name string) error {
	r.counter.add("namespace.delete")
	return r.fakeRegistry.Delete(ctx, name)
}

// countingTriggers wraps a trigger store, counting writes and failing List
// on demand.
type countingTriggers struct {
	triggers.Store
	counter *writeCounter
	listErr error
	// renamed overrides names on the way out of List: the state two
	// replicas racing a named create can leave behind.
	renamed map[string]string
}

func (s *countingTriggers) Create(ctx context.Context, reg triggers.Registration) (triggers.Trigger, string, error) {
	s.counter.add("trigger.create")
	return s.Store.Create(ctx, reg)
}

func (s *countingTriggers) Update(ctx context.Context, id, ns string, change func(*triggers.Trigger) error) (triggers.Trigger, bool, error) {
	s.counter.add("trigger.update")
	return s.Store.Update(ctx, id, ns, change)
}

func (s *countingTriggers) Delete(ctx context.Context, id, ns string) (bool, error) {
	s.counter.add("trigger.delete")
	return s.Store.Delete(ctx, id, ns)
}

func (s *countingTriggers) List(ctx context.Context, ns, orkflow string) ([]triggers.Trigger, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	list, err := s.Store.List(ctx, ns, orkflow)
	for i := range list {
		if name, ok := s.renamed[list[i].ID]; ok {
			list[i].Name = name
		}
	}
	return list, err
}

// countingWebhooks is a durable webhook store held in memory: it delegates to
// an in-memory registry and counts writes.
type countingWebhooks struct {
	registry *webhooks.Registry
	counter  *writeCounter
	listErr  error
}

func (s *countingWebhooks) Register(ctx context.Context, reg webhooks.Registration) (webhooks.Webhook, error) {
	s.counter.add("webhook.register")
	return s.registry.RegisterContext(ctx, reg)
}
func (s *countingWebhooks) List(ctx context.Context) ([]webhooks.Webhook, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.registry.ListContext(ctx)
}
func (s *countingWebhooks) ListForDelivery(ctx context.Context) ([]webhooks.Webhook, error) {
	return s.registry.MatchingContext(ctx, webhooks.Match{})
}
func (s *countingWebhooks) Get(ctx context.Context, id string) (webhooks.Webhook, bool, error) {
	return s.registry.GetContext(ctx, id)
}
func (s *countingWebhooks) Delete(ctx context.Context, id string) (bool, error) {
	s.counter.add("webhook.delete")
	return s.registry.DeleteContext(ctx, id)
}
func (s *countingWebhooks) Update(ctx context.Context, id string, change func(*webhooks.Webhook) error) (webhooks.Webhook, bool, error) {
	s.counter.add("webhook.update")
	return s.registry.UpdateContext(ctx, id, change)
}

// countingSchedules wraps the schedule fake, counting every mutation.
type countingSchedules struct {
	*fakeSchedules
	counter *writeCounter
}

func (s *countingSchedules) Create(ctx context.Context, rec *orkstackv1.ScheduleRecord, paused bool) (engine.ScheduleView, error) {
	s.counter.add("schedule.create")
	return s.fakeSchedules.Create(ctx, rec, paused)
}
func (s *countingSchedules) Update(ctx context.Context, ns, name string, apply func(*orkstackv1.ScheduleRecord) error) (engine.ScheduleView, error) {
	s.counter.add("schedule.update")
	return s.fakeSchedules.Update(ctx, ns, name, apply)
}
func (s *countingSchedules) Delete(ctx context.Context, ns, name string) error {
	s.counter.add("schedule.delete")
	return s.fakeSchedules.Delete(ctx, ns, name)
}
func (s *countingSchedules) Pause(ctx context.Context, ns, name, reason string) error {
	s.counter.add("schedule.pause")
	return s.fakeSchedules.Pause(ctx, ns, name, reason)
}
func (s *countingSchedules) Resume(ctx context.Context, ns, name string) error {
	s.counter.add("schedule.resume")
	return s.fakeSchedules.Resume(ctx, ns, name)
}

// automationHarness is a Service over counting fakes, with one orkflow
// (hello-ork) stored in payments.
type automationHarness struct {
	svc       *api.Service
	counter   *writeCounter
	registry  *countingRegistry
	triggers  *countingTriggers
	webhooks  *countingWebhooks
	schedules *countingSchedules
	orkflows  engine.OrkflowStore
}

func newAutomationHarness(t *testing.T, configure func(*api.Config)) *automationHarness {
	t.Helper()
	counter := &writeCounter{}
	h := &automationHarness{
		counter:   counter,
		registry:  &countingRegistry{fakeRegistry: newFakeRegistry(&orkstackv1.Namespace{Name: "payments"}), counter: counter},
		triggers:  &countingTriggers{Store: triggers.NewMemoryStore(), counter: counter},
		webhooks:  &countingWebhooks{registry: webhooks.NewRegistry(), counter: counter},
		schedules: &countingSchedules{fakeSchedules: newFakeSchedules(), counter: counter},
		orkflows:  engine.NewMemoryOrkflowStore(),
	}
	policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(168 * time.Hour)}}
	admin := &fakeRetentionAdmin{current: 192 * time.Hour}
	cfg := api.Config{
		Orkflows:   h.orkflows,
		Namespaces: h.registry,
		Runs:       engine.NewRunStore(),
		Bus:        engine.NewEventBus(),
		Starter:    &fakeStarter{},
		Triggers:   h.triggers,
		Webhooks:   webhooks.NewDurableRegistry(h.webhooks),
		Schedules:  h.schedules,
		Retention:  archive.NewRetentionController(policies, admin, "ork-main", 24*time.Hour, nil),
		Now:        func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) },
	}
	if configure != nil {
		configure(&cfg)
	}
	h.svc = api.NewService(cfg)
	source := strings.Replace(helloYAML(t), "name: hello-ork", "name: hello-ork\nnamespace: payments", 1)
	if _, err := h.svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: source})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return h
}

func (h *automationHarness) plan(t *testing.T, yaml string, prune bool, pending ...*orkstackv1.PendingOrkflow) (*orkstackv1.PlanAutomationResponse, error) {
	t.Helper()
	resp, err := h.svc.PlanAutomation(context.Background(), connect.NewRequest(&orkstackv1.PlanAutomationRequest{
		Yaml: yaml, Prune: prune, PendingOrkflows: pending,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

const automationDoc = `apiVersion: orkstack/v1
kind: Automation
namespace: payments
settings:
  run_retention: 72h
schedules:
  nightly:
    orkflow: hello-ork
    cron: "0 2 * * *"
triggers:
  deploy-hook:
    orkflow: hello-ork
webhooks:
  etl:
    url: https://hooks.example.test/etl
`

func requirePlanIssue(t *testing.T, resp *orkstackv1.PlanAutomationResponse, field string, fragment string) {
	t.Helper()
	for _, issue := range resp.GetIssues() {
		if issue.GetField() == field && strings.Contains(issue.GetMessage(), fragment) &&
			issue.GetSeverity() == orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
			return
		}
	}
	t.Fatalf("no ERROR on %s containing %q among %v", field, fragment, resp.GetIssues())
}

func TestPlanAutomation_WritesNothing(t *testing.T) {
	h := newAutomationHarness(t, nil)
	ctx := context.Background()
	// Some state to compare against, created directly on the fakes.
	if _, _, err := h.triggers.Store.Create(ctx, triggers.Registration{Namespace: "payments", OrkflowName: "hello-ork", Name: "old-hook"}); err != nil {
		t.Fatalf("seed trigger: %v", err)
	}
	if _, err := h.webhooks.registry.RegisterContext(ctx, webhooks.Registration{URL: "https://x.test/", Namespace: "payments", Name: "old"}); err != nil {
		t.Fatalf("seed webhook: %v", err)
	}
	resp, err := h.plan(t, automationDoc, true)
	if err != nil {
		t.Fatalf("PlanAutomation: %v", err)
	}
	if automation.HasErrors(resp.GetIssues()) || len(resp.GetSteps()) == 0 || resp.GetPlanDigest() == "" {
		t.Fatalf("plan = %v", resp)
	}
	if _, err := h.svc.ExportAutomation(ctx, connect.NewRequest(&orkstackv1.ExportAutomationRequest{Namespace: "payments"})); err != nil {
		t.Fatalf("ExportAutomation: %v", err)
	}
	if writes := h.counter.all(); len(writes) != 0 {
		t.Fatalf("plan and export wrote: %v", writes)
	}
}

func TestPlanAutomation_ParseErrorsReturnIssuesAndNoSteps(t *testing.T) {
	h := newAutomationHarness(t, nil)
	resp, err := h.plan(t, "apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\ntriggers:\n", false)
	if err != nil {
		t.Fatalf("PlanAutomation: %v", err)
	}
	if len(resp.GetSteps()) != 0 || resp.GetPlanDigest() != "" || !automation.HasErrors(resp.GetIssues()) {
		t.Fatalf("plan = %v, want issues, no steps and no digest", resp)
	}
}

func TestPlanAutomation_NamespaceMismatchIsAnIssue(t *testing.T) {
	h := newAutomationHarness(t, nil)
	resp, err := h.svc.PlanAutomation(context.Background(), connect.NewRequest(&orkstackv1.PlanAutomationRequest{
		Yaml: automationDoc, Namespace: "billing",
	}))
	if err != nil {
		t.Fatalf("PlanAutomation: %v", err)
	}
	requirePlanIssue(t, resp.Msg, "namespace", `the document describes namespace "payments" but this request is for "billing"`)
	if len(resp.Msg.GetSteps()) != 0 || resp.Msg.GetPlanDigest() != "" {
		t.Fatalf("a mismatched plan has steps: %v", resp.Msg)
	}
}

func TestPlanAutomation_ReadFailureIsAnRPCErrorNeverAnEmptyPlan(t *testing.T) {
	outage := errors.New("connection refused")
	cases := map[string]func(*automationHarness){
		"namespace": func(h *automationHarness) { h.registry.getErr = outage },
		"schedules": func(h *automationHarness) { h.schedules.listErr = outage },
		"triggers":  func(h *automationHarness) { h.triggers.listErr = outage },
		"webhooks":  func(h *automationHarness) { h.webhooks.listErr = outage },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			h := newAutomationHarness(t, nil)
			breakIt(h)
			resp, err := h.plan(t, automationDoc, true)
			if err == nil {
				t.Fatalf("a failed read answered a plan: %v", resp)
			}
			if code := connect.CodeOf(err); code != connect.CodeUnavailable && code != connect.CodeInternal {
				t.Fatalf("code = %v, want Unavailable or Internal", code)
			}
			if _, err := h.svc.ExportAutomation(context.Background(), connect.NewRequest(&orkstackv1.ExportAutomationRequest{Namespace: "payments"})); err == nil {
				t.Fatal("a failed read answered an export")
			}
		})
	}
}

func TestPlanAutomation_RunsTheTargetRPCsValidators(t *testing.T) {
	h := newAutomationHarness(t, func(cfg *api.Config) { cfg.TelemetrySinks = []string{"datadog"} })
	header := "apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\n"
	cases := []struct{ body, field, fragment string }{
		{"settings:\n  run_retention: 400h\n", "settings.run_retention", "exceeds the deployment maximum"},
		{"settings:\n  telemetry:\n    sink: honeycomb\n", "settings.telemetry", "not a sink this deployment declared"},
		{"settings:\n  default_pool: gpu\n  allowed_pools: [main]\n", "settings.allowed_pools", "gpu"},
		{"schedules:\n  x:\n    orkflow: hello-ork\n    cron: '@daily'\n    end_at: '2020-01-01T00:00:00Z'\n", "schedules.x.end_at", "not in the future"},
	}
	for _, tc := range cases {
		resp, err := h.plan(t, header+tc.body, false)
		if err != nil {
			t.Fatalf("%s: PlanAutomation: %v", tc.field, err)
		}
		requirePlanIssue(t, resp, tc.field, tc.fragment)
	}
}

func TestPlanAutomation_OnlyNamespaceScopedWebhooksAreRead(t *testing.T) {
	h := newAutomationHarness(t, nil)
	ctx := context.Background()
	for _, reg := range []webhooks.Registration{
		{URL: "https://everyone.test/"},
		{URL: "https://billing.test/", Namespace: "billing", Name: "billing-hook"},
		{URL: "https://payments.test/", Namespace: "payments"},
	} {
		if _, err := h.webhooks.registry.RegisterContext(ctx, reg); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	resp, err := h.plan(t, "apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\nwebhooks: {}\n", true)
	if err != nil {
		t.Fatalf("PlanAutomation: %v", err)
	}
	if len(resp.GetSteps()) != 1 || resp.GetSteps()[0].GetAction() != orkstackv1.AutomationAction_AUTOMATION_ACTION_UNMANAGED ||
		!strings.HasPrefix(resp.GetSteps()[0].GetSummary(), "https://payments.test/") {
		t.Fatalf("steps = %v, want only the payments-scoped subscription", resp.GetSteps())
	}
}

func TestPlanAutomation_PendingOrkflowsSatisfyReferences(t *testing.T) {
	h := newAutomationHarness(t, nil)
	doc := "apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\ntriggers:\n  a:\n    orkflow: deploy\n  b:\n    orkflow: deploy\n    version: 1.4.0\n"
	resp, err := h.plan(t, doc, false)
	if err != nil {
		t.Fatalf("PlanAutomation: %v", err)
	}
	requirePlanIssue(t, resp, "triggers.a.orkflow", `orkflow "deploy" has no stored version`)
	resp, err = h.plan(t, doc, false, &orkstackv1.PendingOrkflow{Namespace: "payments", Name: "deploy", Version: "1.4.0"})
	if err != nil {
		t.Fatalf("PlanAutomation: %v", err)
	}
	if automation.HasErrors(resp.GetIssues()) {
		t.Fatalf("pending orkflows did not satisfy the references: %v", resp.GetIssues())
	}
	resp, _ = h.plan(t, doc, false, &orkstackv1.PendingOrkflow{Namespace: "billing", Name: "deploy", Version: "1.4.0"})
	if !automation.HasErrors(resp.GetIssues()) {
		t.Fatal("a pending orkflow of another namespace satisfied a reference")
	}
}

func TestPlanAutomation_NilScheduleControllerIsFailedPrecondition(t *testing.T) {
	h := newAutomationHarness(t, func(cfg *api.Config) { cfg.Schedules = nil })
	if _, err := h.plan(t, automationDoc, false); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("PlanAutomation = %v, want FailedPrecondition", err)
	}
	_, err := h.svc.ExportAutomation(context.Background(), connect.NewRequest(&orkstackv1.ExportAutomationRequest{Namespace: "payments"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("ExportAutomation = %v, want FailedPrecondition", err)
	}
}

func TestExportAutomation_NotFoundUnavailableAndDuplicate(t *testing.T) {
	h := newAutomationHarness(t, nil)
	ctx := context.Background()
	export := func(ns string) error {
		_, err := h.svc.ExportAutomation(ctx, connect.NewRequest(&orkstackv1.ExportAutomationRequest{Namespace: ns}))
		return err
	}
	if err := export("nowhere"); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("unregistered namespace = %v, want NotFound", err)
	}
	h.schedules.extra = []engine.ScheduleView{{ID: "sch-0000000000000001", Name: "stuck", Namespace: "payments", OrkflowName: "hello-ork", Unavailable: "describe timed out"}}
	if err := export("payments"); connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), `schedule "stuck" could not be read (describe timed out)`) {
		t.Errorf("unavailable schedule = %v, want FailedPrecondition naming it", err)
	}
	h.schedules.extra = nil
	// Two triggers share a name: the cross-replica race, reproduced by
	// writing the second one around the store's own check.
	first, _, err := h.triggers.Store.Create(ctx, triggers.Registration{Namespace: "payments", OrkflowName: "hello-ork", Name: "dup"})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	second, _, err := h.triggers.Store.Create(ctx, triggers.Registration{Namespace: "payments", OrkflowName: "hello-ork"})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	h.triggers.renamed = map[string]string{second.ID: "dup"}
	err = export("payments")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), first.ID) || !strings.Contains(err.Error(), second.ID) {
		t.Errorf("duplicate name = %v, want FailedPrecondition naming both ids", err)
	}
	h.triggers.listErr = errors.New("bucket unreachable")
	if err := export("payments"); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("store outage = %v, want Unavailable", err)
	}
}

func TestNamespaceFieldChanges_AgreesWithSpecDifference(t *testing.T) {
	base := func() *orkstackv1.Namespace {
		return &orkstackv1.Namespace{Name: "payments", DefaultPool: "main", AllowedPools: []string{"main", "gpu"}}
	}
	pairs := map[string]func(*orkstackv1.Namespace){
		"identical":            func(*orkstackv1.Namespace) {},
		"default pool spelled": func(n *orkstackv1.Namespace) { n.DefaultPool = "" },
		"pools reordered":      func(n *orkstackv1.Namespace) { n.AllowedPools = []string{"gpu", "main"} },
		"description":          func(n *orkstackv1.Namespace) { n.Description = "x" },
		"labels":               func(n *orkstackv1.Namespace) { n.Labels = map[string]string{"a": "b"} },
		"pools":                func(n *orkstackv1.Namespace) { n.AllowedPools = []string{"main"} },
		"retention":            func(n *orkstackv1.Namespace) { n.RunRetention = durationpb.New(time.Hour) },
		"max running":          func(n *orkstackv1.Namespace) { n.MaxRunning = 3 },
		"secrets":              func(n *orkstackv1.Namespace) { n.AllowedSecrets = []string{"env:X_*"} },
		"signer":               func(n *orkstackv1.Namespace) { n.RequireSignedBy = "cosign:release" },
		"telemetry sink":       func(n *orkstackv1.Namespace) { n.Telemetry = &orkstackv1.TelemetryPolicy{Sink: "d"} },
		"telemetry logs": func(n *orkstackv1.Namespace) {
			n.Telemetry = &orkstackv1.TelemetryPolicy{TaskLogs: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED}
		},
		"telemetry attributes": func(n *orkstackv1.Namespace) {
			n.Telemetry = &orkstackv1.TelemetryPolicy{Attributes: map[string]string{"a": "b"}}
		},
		"telemetry empty":   func(n *orkstackv1.Namespace) { n.Telemetry = &orkstackv1.TelemetryPolicy{} },
		"secrets reordered": func(n *orkstackv1.Namespace) { n.AllowedSecrets = nil },
	}
	for name, mutate := range pairs {
		t.Run(name, func(t *testing.T) {
			stored, requested := base(), base()
			mutate(requested)
			same := api.SpecDifferenceForTest(stored, requested) == ""
			unchanged := len(automation.NamespaceFieldChanges(stored, requested, nil)) == 0
			if same != unchanged {
				t.Fatalf("specDifference same=%v, NamespaceFieldChanges unchanged=%v (%v)", same, unchanged,
					automation.NamespaceFieldChanges(stored, requested, nil))
			}
		})
	}
}
