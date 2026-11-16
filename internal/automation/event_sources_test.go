package automation

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const sourcesDoc = `apiVersion: orkstack/v1
kind: Automation
namespace: payments
event_sources:
  order-created:
    orkflow: handle-order
    version: "1.4.0"
    uses: ghcr.io/acme/source:1
    with:
      stream: ORDERS
      tls: true
    instances: 2
    format: json
    delivery: at-least-once
    match:
      - attribute: Ce-Type
        equals: com.acme.order.created
      - field: subject
        in:
          - orders.eu
    input:
      mode: live
    map:
      order_id:
        pointer: /detail/order_id
      region:
        field: group
    resources:
      cpu: 250m
      memory: 128Mi
`

func parsed(t *testing.T, source string) *Document {
	t.Helper()
	doc, issues := Parse([]byte(source))
	if HasErrors(issues) {
		t.Fatalf("Parse: %v", issues)
	}
	return doc
}

func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestParse_EventSources(t *testing.T) {
	doc := parsed(t, sourcesDoc)
	entry, ok := doc.EventSources["order-created"]
	if !ok {
		t.Fatal("the entry was not read")
	}
	src := entry.Source
	if src.GetNamespace() != "payments" || src.GetName() != "order-created" || src.GetOrkflowName() != "handle-order" || src.GetOrkflowVersion() != "1.4.0" {
		t.Fatalf("identity %v", src)
	}
	if src.GetFormat() != orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON || src.GetInstances() != 2 || src.GetResources().GetMemory() != "128Mi" {
		t.Fatalf("settings %v", src)
	}
	if got := src.GetMatch()[1].GetField(); got != orkstackv1.MessageField_MESSAGE_FIELD_SUBJECT {
		t.Fatalf("field %v", got)
	}
	if src.GetMap()["region"].GetField() != orkstackv1.MessageField_MESSAGE_FIELD_GROUP || src.GetInput().GetData().GetFields()["mode"].GetStringValue() != "live" {
		t.Fatalf("mapping %v", src)
	}
	if strings.Join(entry.Keys, ",") != "orkflow,version,uses,with,instances,resources,delivery,format,match,input,map" {
		t.Fatalf("keys %v", entry.Keys)
	}
}

func TestParse_EventSourceRefusals(t *testing.T) {
	for _, tt := range []struct {
		name, entry, field string
	}{
		{"namespace key", "    orkflow: f\n    uses: r\n    namespace: other\n", "event_sources.e.namespace"},
		{"unknown key", "    orkflow: f\n    uses: r\n    stream: x\n", "event_sources.e.stream"},
		{"no orkflow", "    uses: r\n", "event_sources.e.orkflow"},
		{"no uses", "    orkflow: f\n", "event_sources.e.uses"},
		{"bad delivery", "    orkflow: f\n    uses: r\n    delivery: sometimes\n", "event_sources.e.delivery"},
		{"null value", "    orkflow: f\n    uses: r\n    pool: ~\n", "event_sources.e.pool"},
		{"wrong type", "    orkflow: f\n    uses: r\n    instances: many\n", "event_sources.e.instances"},
		{"a bound", "    orkflow: f\n    uses: r\n    concurrency: 65\n", "event_sources.e.concurrency"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, issues := Parse([]byte("apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\nevent_sources:\n  e:\n" + tt.entry))
			if !errorOn(issues, tt.field) {
				t.Fatalf("want an error at %s, got %v", tt.field, issues)
			}
		})
	}
	for _, entry := range []string{
		"    orkflow: f\n    uses: r\n    map:\n      a:\n        field: headers\n",
		"    orkflow: f\n    uses: r\n    with:\n      verify: yes\n",
	} {
		if _, issues := Parse([]byte("apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\nevent_sources:\n  e:\n" + entry)); !HasErrors(issues) {
			t.Errorf("entry %q: want refused", entry)
		}
	}
	_, issues := Parse([]byte("apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\nevent_sources:\n  e: &a\n    orkflow: f\n    uses: r\n  f: *a\n"))
	if !HasErrors(issues) {
		t.Fatal("an anchor and an alias are refused")
	}
}

func liveSource(name, id string) *orkstackv1.EventSource {
	return &orkstackv1.EventSource{Id: id, Namespace: "payments", Name: name, OrkflowName: "handle-order", Uses: "ghcr.io/acme/source:1",
		Pool: "sources", Instances: 3, Etag: "e1"}
}

func TestPlan_EventSourceSteps(t *testing.T) {
	doc := parsed(t, sourcesDoc)
	refs := map[OrkflowRef]bool{{Name: "handle-order", Version: "1.4.0"}: true}
	ns := &orkstackv1.Namespace{Name: "payments"}

	steps, issues := Plan(doc, Live{Namespace: ns, Orkflows: refs}, Options{})
	if HasErrors(issues) {
		t.Fatal(issues)
	}
	create := sourceStep(t, steps, "order-created")
	if create.GetAction() != actionCreate || create.GetCreateEventSource().GetEventSource().GetNamespace() != "payments" {
		t.Fatalf("create %v", create)
	}

	stored := proto.Clone(doc.EventSources["order-created"].Source).(*orkstackv1.EventSource)
	stored.Id, stored.Etag = "evs-0123456789abcdef", "e1"
	steps, _ = Plan(doc, Live{Namespace: ns, Orkflows: refs, EventSources: []*orkstackv1.EventSource{stored}}, Options{})
	if got := sourceStep(t, steps, "order-created"); got.GetAction() != actionUnchanged || len(got.GetChanges()) != 0 {
		t.Fatalf("an unchanged source plans %v", got)
	}

	stored.Instances = 4
	steps, _ = Plan(doc, Live{Namespace: ns, Orkflows: refs, EventSources: []*orkstackv1.EventSource{stored}}, Options{})
	update := sourceStep(t, steps, "order-created")
	if update.GetAction() != actionUpdate || update.GetUpdateEventSource().GetEventSource().GetEtag() != "e1" || update.GetChanges()[0].GetField() != "instances" {
		t.Fatalf("update %v", update)
	}

	stored.OrkflowName = "other"
	_, issues = Plan(doc, Live{Namespace: ns, Orkflows: refs, EventSources: []*orkstackv1.EventSource{stored}}, Options{})
	if !errorOn(issues, "event_sources.order-created.orkflow") {
		t.Fatalf("issues %v", issues)
	}
}

func TestPlan_DeleteEventSourceStepCarriesPoolAndInstances(t *testing.T) {
	doc := parsed(t, "apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\nevent_sources: {}\n")
	live := Live{Namespace: &orkstackv1.Namespace{Name: "payments"}, EventSources: []*orkstackv1.EventSource{liveSource("old", "evs-0123456789abcdef")}}
	steps, _ := Plan(doc, live, Options{})
	if got := sourceStep(t, steps, "old"); got.GetAction() != actionUndeclared || got.GetRequest() != nil {
		t.Fatalf("without --prune: %v", got)
	}
	steps, _ = Plan(doc, live, Options{Prune: true})
	del := sourceStep(t, steps, "old")
	if del.GetAction() != actionDelete || del.GetDeleteEventSource().GetId() != "evs-0123456789abcdef" || del.GetId() != "evs-0123456789abcdef" {
		t.Fatalf("delete %v", del)
	}
	changes := map[string]*orkstackv1.AutomationFieldChange{}
	for _, c := range del.GetChanges() {
		changes[c.GetField()] = c
	}
	if changes["pool"].GetBefore() != `"sources"` || changes["instances"].GetBefore() != "3" || changes["pool"].GetAfter() != "" {
		t.Fatalf("changes %v", del.GetChanges())
	}
	digest := func(live Live) string {
		steps, _ := Plan(doc, live, Options{Prune: true})
		return Digest("payments", steps)
	}
	moved := live
	moved.EventSources = []*orkstackv1.EventSource{liveSource("old", "evs-1111111111111111")}
	if digest(live) == digest(moved) {
		t.Fatal("a source deleted and created again is a plan that moved")
	}
}

func TestEventSourceEntry_NamespaceAndRoundTrip(t *testing.T) {
	var live []*orkstackv1.EventSource
	for _, name := range []string{"on", "no", "y"} {
		src := liveSource(name, "evs-0123456789abcdef")
		src.With = mustStruct(t, map[string]any{"verify": "yes", "mode": "0755", "since": "2026-01-02"})
		live = append(live, src)
	}
	ns := &orkstackv1.Namespace{Name: "payments"}
	text, _, err := Export("payments", Live{Namespace: ns, EventSources: live})
	if err != nil {
		t.Fatal(err)
	}
	doc := parsed(t, text)
	refs := map[OrkflowRef]bool{{Name: "handle-order"}: true}
	steps, issues := Plan(doc, Live{Namespace: ns, EventSources: live, Orkflows: refs}, Options{Prune: true})
	if HasErrors(issues) {
		t.Fatal(issues)
	}
	for _, step := range steps {
		if step.GetKind() == kindEventSource && step.GetAction() != actionUnchanged {
			t.Fatalf("an export applied again plans %v\n%s", step, text)
		}
		if create := step.GetCreateEventSource(); create != nil && create.GetEventSource().GetNamespace() != "payments" {
			t.Fatalf("a step carries another namespace: %v", step)
		}
	}
}

func TestExport_EventSourcesBlockStyle(t *testing.T) {
	src := liveSource("orders", "evs-0123456789abcdef")
	src.Match = parsed(t, sourcesDoc).EventSources["order-created"].Source.GetMatch()
	text, _, err := Export("payments", Live{Namespace: &orkstackv1.Namespace{Name: "payments"}, EventSources: []*orkstackv1.EventSource{src}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "{\"") || strings.Contains(text, "[{") || strings.Contains(text, "evs-") || strings.Contains(text, "etag") {
		t.Fatalf("export is not block style, or names what is output only:\n%s", text)
	}
}

// FuzzParseEventSourceSection: no document panics the parser, and a section
// that parses round-trips through an export.
func FuzzParseEventSourceSection(f *testing.F) {
	f.Add(sourcesDoc)
	f.Add("apiVersion: orkstack/v1\nkind: Automation\nnamespace: a\nevent_sources:\n  e:\n    orkflow: f\n    uses: r\n    match:\n      - pointer: /x\n        in: [1, \"1\", true]\n    format: json\n")
	f.Fuzz(func(t *testing.T, source string) {
		doc, issues := Parse([]byte(source))
		if HasErrors(issues) || doc == nil || len(doc.EventSources) == 0 {
			return
		}
		var live []*orkstackv1.EventSource
		for _, entry := range doc.EventSources {
			live = append(live, entry.Source)
		}
		text, _, err := Export(doc.Namespace, Live{Namespace: &orkstackv1.Namespace{Name: doc.Namespace}, EventSources: live})
		if err != nil {
			return
		}
		again, issues := Parse([]byte(text))
		if HasErrors(issues) {
			t.Fatalf("an export does not parse: %v\n%s", issues, text)
		}
		for name, entry := range doc.EventSources {
			if !proto.Equal(entry.Source, again.EventSources[name].Source) {
				t.Fatalf("source %s does not round-trip:\n%v\n%v", name, entry.Source, again.EventSources[name].Source)
			}
		}
	})
}

func sourceStep(t *testing.T, steps []*orkstackv1.AutomationStep, name string) *orkstackv1.AutomationStep {
	t.Helper()
	for _, s := range steps {
		if s.GetKind() == kindEventSource && s.GetName() == name {
			return s
		}
	}
	t.Fatalf("no step for event source %s in %v", name, steps)
	return nil
}
