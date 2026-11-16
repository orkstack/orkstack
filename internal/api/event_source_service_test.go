package api_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const sourceDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

// sourceResolver answers Inspect from a table of labels by reference and
// counts each reference it was asked, so a test sees when a reference is
// resolved again.
type sourceResolver struct {
	mu     sync.Mutex
	labels map[string]map[string]string
	asked  map[string]int
}

func newSourceResolver(t *testing.T) *sourceResolver {
	t.Helper()
	iface, err := protojson.Marshal(&orkstackv1.ServiceInterface{SchemaVersion: 1, Profile: eventsources.ProfileEventSource,
		With:        map[string]*orkstackv1.FieldSchema{"stream": {Type: "string", Required: true}},
		EventSource: &orkstackv1.EventSourceDeclaration{MaxConcurrency: 4}})
	must.OKf(t, err, "pack")
	good := map[string]string{eventsources.ContractLabel: "1", eventsources.ProfileLabel: "event-source/1", eventsources.InterfaceLabel: string(iface)}
	return &sourceResolver{asked: map[string]int{}, labels: map[string]map[string]string{
		"ghcr.io/acme/source:1":               good,
		"ghcr.io/acme/source@" + sourceDigest: good,
		"ghcr.io/acme/activity:1":             {},
	}}
}

func (r *sourceResolver) ResolveOrkflow(context.Context, dsl.Catalog, *orkstackv1.Orkflow) (dsl.Resolutions, error) {
	return dsl.Resolutions{}, nil
}

func (r *sourceResolver) Inspect(_ context.Context, uses, reference string) (*dsl.Resolution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asked[reference]++
	labels, ok := r.labels[reference]
	if !ok {
		return nil, &dsl.ResolveError{Reference: reference, Code: dsl.ReasonImageResolveUnsupported, Detail: "no such image"}
	}
	repo := reference
	if i := strings.IndexAny(reference, "@"); i >= 0 {
		repo = reference[:i]
	} else if i := strings.LastIndex(reference, ":"); i >= 0 {
		repo = reference[:i]
	}
	return &dsl.Resolution{Image: &orkstackv1.ImageRef{Reference: uses, Pinned: repo + "@" + sourceDigest}, ServiceLabels: labels}, nil
}

func (r *sourceResolver) count(reference string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.asked[reference]
}

type sourceFixture struct {
	*fixture
	resolver *sourceResolver
	orkflows *engine.MemoryOrkflowStore
	client   orkstackv1connect.EventSourceServiceClient
}

func newSourceFixture(t *testing.T, edit func(*api.Config)) *sourceFixture {
	t.Helper()
	resolver := newSourceResolver(t)
	orkflows := engine.NewMemoryOrkflowStore()
	put := func(ns, name string, inputs map[string]any, tasks ...*orkstackv1.Task) {
		data, err := structpb.NewStruct(inputs)
		must.OKf(t, err, "inputs")
		if len(tasks) == 0 {
			tasks = []*orkstackv1.Task{{Id: "process", Uses: "ghcr.io/acme/process:1"}}
		}
		_, _, err = orkflows.Put(context.Background(), &orkstackv1.Orkflow{Namespace: ns, Name: name, Version: "1.0.0", Inputs: data, Tasks: tasks}, artifacts.HeadExpectation{})
		must.OKf(t, err, "put orkflow")
	}
	orderInputs := map[string]any{
		"order_id": map[string]any{"type": "string", "required": true},
		"region":   map[string]any{"type": "string", "default": "eu"},
	}
	put("default", "handle-order", orderInputs)
	put("payments", "handle-order", orderInputs)
	put("default", "callback", map[string]any{}, &orkstackv1.Task{Id: "wait", Await: &orkstackv1.AwaitSpec{Callback: true}})
	store, err := artifacts.NewLocalEventSourceStore(t.TempDir())
	must.OKf(t, err, "store")
	settings := eventsources.DefaultSettings()
	settings.Enabled, settings.AllowAsserted = true, true
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Orkflows = orkflows
		cfg.Resolver = resolver
		cfg.EventSources = store
		cfg.EventSourceSettings = settings
		cfg.System = api.SystemInfo{DeploymentName: "ork", BuildID: "b1"}
		cfg.Namespaces = newFakeRegistry(&orkstackv1.Namespace{Name: "payments"})
		if edit != nil {
			edit(cfg)
		}
	})
	return &sourceFixture{fixture: f, resolver: resolver, orkflows: orkflows, client: orkstackv1connect.NewEventSourceServiceClient(f.client, f.baseURL)}
}

func orderSource(ns string) *orkstackv1.EventSource {
	with, _ := structpb.NewStruct(map[string]any{"stream": "ORDERS"})
	return &orkstackv1.EventSource{
		Namespace: ns, Name: "orders", OrkflowName: "handle-order", Uses: "ghcr.io/acme/source:1",
		With: with, Format: orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON,
		Map: map[string]*orkstackv1.MapRule{"order_id": {Pointer: "/id"}},
	}
}

func (f *sourceFixture) create(t *testing.T, src *orkstackv1.EventSource) *orkstackv1.EventSource {
	t.Helper()
	resp, err := f.client.CreateEventSource(context.Background(), connect.NewRequest(&orkstackv1.CreateEventSourceRequest{EventSource: src}))
	must.OKf(t, err, "CreateEventSource")
	return resp.Msg.GetEventSource()
}

func wantRefusal(t *testing.T, err error, code connect.Code, reason string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %s %s, got success", code, reason)
	}
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != code || !strings.Contains(ce.Message(), reason) {
		t.Fatalf("got %v, want %s with %s", err, code, reason)
	}
}

func TestEventSourceService_CreateStoresTheDocument(t *testing.T) {
	f := newSourceFixture(t, nil)
	got := f.create(t, orderSource(""))
	if !eventsources.ValidID(got.GetId()) || got.GetNamespace() != "default" {
		t.Fatalf("created %v", got)
	}
	if got.GetServiceRevision() != 1 || got.GetJudgingRevision() != 1 {
		t.Errorf("counters %d %d, want 1 and 1", got.GetServiceRevision(), got.GetJudgingRevision())
	}
	if got.GetImage() != "ghcr.io/acme/source@"+sourceDigest || got.GetUses() != "ghcr.io/acme/source:1" {
		t.Errorf("image %q uses %q: the reference is stored as written and resolved to its digest", got.GetImage(), got.GetUses())
	}
	if got.GetEtag() == "" || got.GetConsented().GetDigest() == "" || got.GetAsksFor().GetDigest() == "" {
		t.Errorf("etag, consent and asks_for are output: %v", got)
	}
	_, err := f.client.CreateEventSource(context.Background(), connect.NewRequest(&orkstackv1.CreateEventSourceRequest{EventSource: orderSource("")}))
	wantRefusal(t, err, connect.CodeAlreadyExists, "orders")

	read, err := f.client.GetEventSource(context.Background(), connect.NewRequest(&orkstackv1.GetEventSourceRequest{Name: "orders"}))
	must.OKf(t, err, "Get")
	if st := read.Msg.GetStatus(); st.GetState() != orkstackv1.EventSourceState_EVENT_SOURCE_STATE_DISABLED ||
		st.GetReason() != eventsources.ReasonRequireAuthentication {
		t.Errorf("status %v: a source written under the opt-in reads DISABLED until authentication is enforced", st)
	}
}

func TestEventSourceService_Guards(t *testing.T) {
	for _, tt := range []struct {
		name   string
		edit   func(*api.Config)
		reason string
	}{
		{"not authenticated, no opt-in", func(c *api.Config) { c.EventSourceSettings.AllowAsserted = false }, eventsources.ReasonRequireAuthentication},
		{"off", func(c *api.Config) { c.EventSourceSettings.Enabled = false }, eventsources.ReasonDisabled},
		{"unversioned", func(c *api.Config) { c.System.BuildID = "" }, eventsources.ReasonRequireVersioning},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newSourceFixture(t, tt.edit)
			_, err := f.client.CreateEventSource(context.Background(), connect.NewRequest(&orkstackv1.CreateEventSourceRequest{EventSource: orderSource("")}))
			wantRefusal(t, err, connect.CodeFailedPrecondition, tt.reason)
		})
	}
	t.Run("reads need authentication too", func(t *testing.T) {
		f := newSourceFixture(t, func(c *api.Config) { c.EventSourceSettings.AllowAsserted = false })
		_, err := f.client.ListEventSources(context.Background(), connect.NewRequest(&orkstackv1.ListEventSourcesRequest{}))
		wantRefusal(t, err, connect.CodeFailedPrecondition, eventsources.ReasonRequireAuthentication)
		_, err = f.client.EvaluateEventSourceMessage(context.Background(), connect.NewRequest(&orkstackv1.EvaluateEventSourceMessageRequest{
			Source: &orkstackv1.EvaluateEventSourceMessageRequest_Candidate{Candidate: orderSource("")}}))
		wantRefusal(t, err, connect.CodeFailedPrecondition, eventsources.ReasonRequireAuthentication)
	})
}

func TestEventSourceService_CreateRefusals(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*orkstackv1.EventSource)
		consent string
		code    connect.Code
		reason  string
	}{
		{"a bound broken", func(s *orkstackv1.EventSource) { s.Instances = 99 }, "", connect.CodeInvalidArgument, eventsources.ReasonInvalid},
		{"no such Orkflow", func(s *orkstackv1.EventSource) { s.OrkflowName = "nope" }, "", connect.CodeNotFound, "nope"},
		{"a mapping the Orkflow does not declare", func(s *orkstackv1.EventSource) { s.Map["other"] = &orkstackv1.MapRule{Pointer: "/x"} }, "", connect.CodeInvalidArgument, eventsources.ReasonMappingInvalid},
		{"an Orkflow a source cannot start", func(s *orkstackv1.EventSource) { s.OrkflowName = "callback"; s.Map = nil }, "", connect.CodeFailedPrecondition, eventsources.ReasonOrkflowUnsupported},
		{"a first-party name", func(s *orkstackv1.EventSource) { s.Uses = "nats-jetstream" }, "", connect.CodeFailedPrecondition, eventsources.ReasonImageUnsupported},
		{"an image that is no source", func(s *orkstackv1.EventSource) { s.Uses = "ghcr.io/acme/activity:1" }, "", connect.CodeFailedPrecondition, eventsources.ReasonServiceContractUnknown},
		{"with: against the interface", func(s *orkstackv1.EventSource) { s.With, _ = structpb.NewStruct(map[string]any{}) }, "", connect.CodeInvalidArgument, eventsources.ReasonConfigInvalid},
		{"delivery against the image's class", func(s *orkstackv1.EventSource) {
			s.Delivery = orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE
		}, "", connect.CodeInvalidArgument, eventsources.ReasonDeliveryMismatch},
		{"memory below a message in hand", func(s *orkstackv1.EventSource) { s.Resources = &orkstackv1.ResourceRequest{Memory: "16Mi"} }, "", connect.CodeInvalidArgument, "resources.memory"},
		{"a consent nothing asks for", func(*orkstackv1.EventSource) {}, "c1:" + strings.Repeat("0", 64), connect.CodeFailedPrecondition, "IDENTITY_CONSENT_MISMATCH"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newSourceFixture(t, nil)
			src := orderSource("")
			tt.edit(src)
			_, err := f.client.CreateEventSource(context.Background(), connect.NewRequest(&orkstackv1.CreateEventSourceRequest{EventSource: src, Consent: tt.consent}))
			wantRefusal(t, err, tt.code, tt.reason)
		})
	}
}

func TestEventSourceService_Quotas(t *testing.T) {
	f := newSourceFixture(t, func(c *api.Config) { c.EventSourceSettings.Limits.MaxPerNamespace = 1 })
	f.create(t, orderSource(""))
	second := orderSource("")
	second.Name = "orders-two"
	_, err := f.client.CreateEventSource(context.Background(), connect.NewRequest(&orkstackv1.CreateEventSourceRequest{EventSource: second}))
	wantRefusal(t, err, connect.CodeResourceExhausted, eventsources.ReasonLimit)
}

func TestEventSourceService_UpdateBumpsExactlyItsCounter(t *testing.T) {
	f := newSourceFixture(t, nil)
	ctx := context.Background()
	src := f.create(t, orderSource(""))
	asked := f.resolver.count("ghcr.io/acme/source:1")
	update := func(edit func(*orkstackv1.EventSource)) *orkstackv1.EventSource {
		t.Helper()
		edit(src)
		resp, err := f.client.UpdateEventSource(ctx, connect.NewRequest(&orkstackv1.UpdateEventSourceRequest{EventSource: src}))
		must.OKf(t, err, "Update")
		src = resp.Msg.GetEventSource()
		return src
	}
	got := update(func(s *orkstackv1.EventSource) { s.Rate = 5 })
	if got.GetServiceRevision() != 1 || got.GetJudgingRevision() != 2 {
		t.Fatalf("rate: %d %d, want 1 2", got.GetServiceRevision(), got.GetJudgingRevision())
	}
	got = update(func(s *orkstackv1.EventSource) { s.Concurrency = 2 })
	if got.GetServiceRevision() != 2 || got.GetJudgingRevision() != 2 {
		t.Fatalf("concurrency: %d %d, want 2 2", got.GetServiceRevision(), got.GetJudgingRevision())
	}
	got = update(func(s *orkstackv1.EventSource) { s.Description = "d"; s.Instances = 2 })
	if got.GetServiceRevision() != 2 || got.GetJudgingRevision() != 2 {
		t.Fatalf("neither: %d %d, want 2 2", got.GetServiceRevision(), got.GetJudgingRevision())
	}
	if n := f.resolver.count("ghcr.io/acme/source:1"); n != asked {
		t.Fatalf("an update that left uses: as it was resolved the tag again (%d times, was %d)", n, asked)
	}
	stale := orderSource("")
	stale.Etag = "stale"
	_, err := f.client.UpdateEventSource(ctx, connect.NewRequest(&orkstackv1.UpdateEventSourceRequest{EventSource: stale}))
	wantRefusal(t, err, connect.CodeAborted, "changed since it was read")
}

func TestEventSourceService_BindingAndConsentAreExplicit(t *testing.T) {
	f := newSourceFixture(t, nil)
	ctx := context.Background()
	src := f.create(t, orderSource(""))
	at := src.GetConsented().GetAt().AsTime()
	// A change of uses:, with: or a credential changes neither the binder
	// nor the consent; a change of what the source submits rebinds.
	src.With, _ = structpb.NewStruct(map[string]any{"stream": "OTHER"})
	resp, err := f.client.UpdateEventSource(ctx, connect.NewRequest(&orkstackv1.UpdateEventSourceRequest{EventSource: src}))
	must.OKf(t, err, "update with")
	if !resp.Msg.GetEventSource().GetConsented().GetAt().AsTime().Equal(at) {
		t.Fatal("a change of with: rebound the source")
	}
	src = resp.Msg.GetEventSource()
	src.Map["region"] = &orkstackv1.MapRule{Attribute: "Region"}
	resp, err = f.client.UpdateEventSource(ctx, connect.NewRequest(&orkstackv1.UpdateEventSourceRequest{EventSource: src}))
	must.OKf(t, err, "update map")
	if resp.Msg.GetEventSource().GetConsented().GetAt().AsTime().Equal(at) {
		t.Fatal("a change of map: did not rebind the source")
	}
}

func TestEventSourceService_UpdateNamespaceMismatch(t *testing.T) {
	f := newSourceFixture(t, nil)
	src := f.create(t, orderSource(""))
	src.OrkflowName = "callback"
	_, err := f.client.UpdateEventSource(context.Background(), connect.NewRequest(&orkstackv1.UpdateEventSourceRequest{EventSource: src}))
	wantRefusal(t, err, connect.CodeInvalidArgument, eventsources.ReasonNamespaceMismatch)
	if !strings.Contains(err.Error(), "callback") || strings.Contains(err.Error(), "handle-order") {
		t.Fatalf("the refusal names the Orkflow the request named and never the stored one, which the caller may not read: %v", err)
	}
}

func TestEventSourceService_OneNamespace(t *testing.T) {
	f := newSourceFixture(t, nil)
	ctx := context.Background()
	got := f.create(t, orderSource("payments"))
	if got.GetNamespace() != "payments" {
		t.Fatalf("created in %q, want the source's own namespace", got.GetNamespace())
	}
	if _, err := f.client.GetEventSource(ctx, connect.NewRequest(&orkstackv1.GetEventSourceRequest{Name: "orders"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("the default namespace holds no source: %v", err)
	}
	for _, bad := range []string{"Default", "a ", "payments/x"} {
		_, err := f.client.CreateEventSource(ctx, connect.NewRequest(&orkstackv1.CreateEventSourceRequest{EventSource: orderSource(bad)}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("namespace %q: %v, want refused, never folded", bad, err)
		}
	}
}

func TestEventSourceService_PauseResume(t *testing.T) {
	f := newSourceFixture(t, nil)
	ctx := context.Background()
	f.create(t, orderSource(""))
	_, err := f.client.PauseEventSource(ctx, connect.NewRequest(&orkstackv1.PauseEventSourceRequest{Name: "orders"}))
	wantRefusal(t, err, connect.CodeInvalidArgument, "reason")
	paused, err := f.client.PauseEventSource(ctx, connect.NewRequest(&orkstackv1.PauseEventSourceRequest{Name: "orders", Reason: "broker maintenance"}))
	must.OKf(t, err, "Pause")
	if !paused.Msg.GetEventSource().GetPaused() || paused.Msg.GetEventSource().GetPause().GetReason() != "broker maintenance" {
		t.Fatalf("paused %v", paused.Msg.GetEventSource())
	}
	before := paused.Msg.GetEventSource()
	resumed, err := f.client.ResumeEventSource(ctx, connect.NewRequest(&orkstackv1.ResumeEventSourceRequest{Name: "orders"}))
	must.OKf(t, err, "Resume")
	after := resumed.Msg.GetEventSource()
	if after.GetPaused() || resumed.Msg.GetNotice() != "" {
		t.Fatalf("resumed %v notice %q", after, resumed.Msg.GetNotice())
	}
	if after.GetBoundBy() != before.GetBoundBy() || !after.GetConsented().GetAt().AsTime().Equal(before.GetConsented().GetAt().AsTime()) {
		t.Fatal("a resume neither rebinds nor consents")
	}
	again, err := f.client.ResumeEventSource(ctx, connect.NewRequest(&orkstackv1.ResumeEventSourceRequest{Name: "orders"}))
	must.OKf(t, err, "Resume again")
	if again.Msg.GetNotice() != "source was not paused; nothing changed" {
		t.Fatalf("notice %q", again.Msg.GetNotice())
	}
}

func TestEventSourceService_DeleteByID(t *testing.T) {
	f := newSourceFixture(t, nil)
	ctx := context.Background()
	src := f.create(t, orderSource(""))
	_, err := f.client.DeleteEventSource(ctx, connect.NewRequest(&orkstackv1.DeleteEventSourceRequest{Name: "orders", Id: "evs-ffffffffffffffff"}))
	wantRefusal(t, err, connect.CodeFailedPrecondition, eventsources.ReasonIDMismatch)
	if !strings.Contains(err.Error(), src.GetId()) {
		t.Fatalf("the mismatch names the id the name holds: %v", err)
	}
	_, err = f.client.DeleteEventSource(ctx, connect.NewRequest(&orkstackv1.DeleteEventSourceRequest{Name: "orders", Id: src.GetId()}))
	must.OKf(t, err, "Delete")
	_, err = f.client.DeleteEventSource(ctx, connect.NewRequest(&orkstackv1.DeleteEventSourceRequest{Name: "orders", Id: src.GetId()}))
	wantRefusal(t, err, connect.CodeNotFound, "orders")
}

func TestEvaluate_CandidateAndStored(t *testing.T) {
	f := newSourceFixture(t, nil)
	ctx := context.Background()
	candidate := orderSource("")
	candidate.Match = []*orkstackv1.MatchRule{{Pointer: "/status", Equals: structpb.NewStringValue("PAID")}}
	eval := func(src *orkstackv1.EventSource, name, body string) *orkstackv1.EvaluateEventSourceMessageResponse {
		t.Helper()
		req := &orkstackv1.EvaluateEventSourceMessageRequest{Body: []byte(body)}
		if src != nil {
			req.Source = &orkstackv1.EvaluateEventSourceMessageRequest_Candidate{Candidate: src}
		} else {
			req.Source = &orkstackv1.EvaluateEventSourceMessageRequest_Name{Name: name}
		}
		resp, err := f.client.EvaluateEventSourceMessage(ctx, connect.NewRequest(req))
		must.OKf(t, err, "Evaluate")
		return resp.Msg
	}
	got := eval(candidate, "", `{"id":"o-1","status":"PAID"}`)
	if got.GetOutcome() != orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_ACCEPTED || got.GetInput().GetData().GetFields()["order_id"].GetStringValue() != "o-1" {
		t.Fatalf("accepted: %v", got)
	}
	if got.GetOrkflowVersion() != "1.0.0" {
		t.Fatalf("version judged against: %q", got.GetOrkflowVersion())
	}
	if got := eval(candidate, "", `{"id":"o-1","status":"NEW"}`); got.GetOutcome() != orkstackv1.EvaluationOutcome_EVALUATION_OUTCOME_FILTERED {
		t.Fatalf("filtered: %v", got)
	}
	if got := eval(candidate, "", `{"status":"PAID"}`); got.GetReason() != eventsources.ReasonMessageFieldMissing {
		t.Fatalf("missing: %v", got)
	}
	f.create(t, orderSource(""))
	if got := eval(nil, "orders", `{"id":7}`); got.GetReason() != eventsources.ReasonMessageFieldType {
		t.Fatalf("stored: %v", got)
	}
	list, err := f.client.ListEventSources(ctx, connect.NewRequest(&orkstackv1.ListEventSourcesRequest{}))
	must.OKf(t, err, "List")
	if len(list.Msg.GetEventSources()) != 1 {
		t.Fatalf("an evaluation wrote a source: %d listed", len(list.Msg.GetEventSources()))
	}
}

func TestEvaluate_Bounds(t *testing.T) {
	f := newSourceFixture(t, func(c *api.Config) { c.EventSourceSettings.Limits.MaxMessageBytes = 8 })
	_, err := f.client.EvaluateEventSourceMessage(context.Background(), connect.NewRequest(&orkstackv1.EvaluateEventSourceMessageRequest{
		Source: &orkstackv1.EvaluateEventSourceMessageRequest_Candidate{Candidate: orderSource("")}, Body: []byte("123456789")}))
	wantRefusal(t, err, connect.CodeInvalidArgument, "max_message_bytes")
	_, err = f.client.EvaluateEventSourceMessage(context.Background(), connect.NewRequest(&orkstackv1.EvaluateEventSourceMessageRequest{}))
	wantRefusal(t, err, connect.CodeInvalidArgument, "candidate")
}

func TestEventSourceService_InspectSourceImage(t *testing.T) {
	f := newSourceFixture(t, nil)
	resp, err := f.client.InspectSourceImage(context.Background(), connect.NewRequest(&orkstackv1.InspectSourceImageRequest{Reference: "ghcr.io/acme/source:1"}))
	must.OKf(t, err, "Inspect")
	img := resp.Msg.GetImages()[0]
	if img.GetImage() != "ghcr.io/acme/source@"+sourceDigest || img.GetInterface().GetWith()["stream"] == nil || img.GetContractVersions()[0] != 1 {
		t.Fatalf("image %v", img)
	}
	empty, err := f.client.InspectSourceImage(context.Background(), connect.NewRequest(&orkstackv1.InspectSourceImageRequest{}))
	must.OKf(t, err, "list first-party")
	if len(empty.Msg.GetImages()) != 0 {
		t.Fatal("no first-party source image ships with this release")
	}
}

func TestDeleteNamespace_RefusedWhileItHoldsASource(t *testing.T) {
	f := newSourceFixture(t, nil)
	ctx := context.Background()
	src := f.create(t, orderSource("payments"))
	must.OKf(t, f.orkflows.Delete(ctx, "payments", "handle-order"), "delete the Orkflow")
	namespaces := orkstackv1connect.NewNamespaceServiceClient(f.fixture.client, f.baseURL)
	_, err := namespaces.DeleteNamespace(ctx, connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "payments"}))
	wantRefusal(t, err, connect.CodeFailedPrecondition, "event source")
	_, err = f.client.DeleteEventSource(ctx, connect.NewRequest(&orkstackv1.DeleteEventSourceRequest{Namespace: "payments", Name: "orders", Id: src.GetId()}))
	must.OKf(t, err, "delete the source")
	_, err = namespaces.DeleteNamespace(ctx, connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "payments"}))
	must.OKf(t, err, "delete the namespace once it holds no source")
}

func TestEventSourceService_PauseKeepsAPauseAndNamesAreValidated(t *testing.T) {
	f := newSourceFixture(t, nil)
	ctx := context.Background()
	f.create(t, orderSource(""))
	first, err := f.client.PauseEventSource(ctx, connect.NewRequest(&orkstackv1.PauseEventSourceRequest{Name: "orders", Reason: "first"}))
	must.OKf(t, err, "Pause")
	again, err := f.client.PauseEventSource(ctx, connect.NewRequest(&orkstackv1.PauseEventSourceRequest{Name: "orders", Reason: "second"}))
	must.OKf(t, err, "Pause again")
	if again.Msg.GetEventSource().GetPause().GetReason() != "first" || len(again.Msg.GetWarnings()) == 0 ||
		!again.Msg.GetEventSource().GetPause().GetSince().AsTime().Equal(first.Msg.GetEventSource().GetPause().GetSince().AsTime()) {
		t.Fatalf("a second pause replaced the first: %v", again.Msg)
	}
	for _, bad := range []string{"", "Bad_Name"} {
		_, err := f.client.GetEventSource(ctx, connect.NewRequest(&orkstackv1.GetEventSourceRequest{Name: bad}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("name %q: %v, want InvalidArgument", bad, err)
		}
		_, err = f.client.DeleteEventSource(ctx, connect.NewRequest(&orkstackv1.DeleteEventSourceRequest{Name: bad, Id: "evs-0123456789abcdef"}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("delete of %q: %v, want InvalidArgument", bad, err)
		}
	}
}

func TestEventSourceService_ImagePolicyBeforeAnyRegistryIsAsked(t *testing.T) {
	f := newSourceFixture(t, func(c *api.Config) {
		c.ImagePolicies = &memImagePolicies{policy: &orkstackv1.ImagePolicy{AllowedRegistries: []string{"registry.example/"}}}
	})
	ctx := context.Background()
	_, err := f.client.InspectSourceImage(ctx, connect.NewRequest(&orkstackv1.InspectSourceImageRequest{Reference: "ghcr.io/acme/source:1"}))
	wantRefusal(t, err, connect.CodeInvalidArgument, "IMAGE_NOT_PERMITTED")
	_, err = f.client.CreateEventSource(ctx, connect.NewRequest(&orkstackv1.CreateEventSourceRequest{EventSource: orderSource("")}))
	wantRefusal(t, err, connect.CodeInvalidArgument, "IMAGE_NOT_PERMITTED")
	if n := f.resolver.count("ghcr.io/acme/source:1"); n != 0 {
		t.Fatalf("the engine asked a registry the namespace may not use %d times", n)
	}
	_, err = f.client.InspectSourceImage(ctx, connect.NewRequest(&orkstackv1.InspectSourceImageRequest{Namespace: "nope", Reference: "registry.example/s:1"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("a namespace that does not exist: %v", err)
	}
}
