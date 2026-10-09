package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// These tests prove docs/AUTOMATION.md "Names and in-place updates": triggers
// and webhooks gain an
// optional name and an in-place update that never re-issues the credential
// an external system holds.

func connectCode(err error) connect.Code {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connectErr.Code()
	}
	return connect.CodeUnknown
}

// applyHelloVersion stores the example orkflow under another version.
func (f *fixture) applyHelloVersion(t *testing.T, version string) {
	t.Helper()
	source := strings.Replace(helloYAML(t), `version: "0.2.0"`, `version: "`+version+`"`, 1)
	if !strings.Contains(source, `version: "`+version+`"`) {
		t.Fatalf("the example orkflow has no version line to replace")
	}
	if _, err := f.orkflowClient().ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: source})); err != nil {
		t.Fatalf("Apply %s: %v", version, err)
	}
}

func TestCreateTrigger_NameIsValidatedAndUnique(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	ctx := context.Background()
	created, _ := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork", Name: "github-deploy"})
	if created.GetName() != "github-deploy" {
		t.Fatalf("created name = %q", created.GetName())
	}
	_, err := f.triggerClient().CreateTrigger(ctx, connect.NewRequest(&orkstackv1.CreateTriggerRequest{
		OrkflowName: "hello-ork", Name: "github-deploy",
	}))
	if connectCode(err) != connect.CodeAlreadyExists || !strings.Contains(err.Error(), `trigger name "github-deploy" is already in use in namespace "default"`) {
		t.Fatalf("second create = %v, want AlreadyExists naming the name", err)
	}
	_, err = f.triggerClient().CreateTrigger(ctx, connect.NewRequest(&orkstackv1.CreateTriggerRequest{
		OrkflowName: "hello-ork", Name: "Not-A-Slug",
	}))
	if connectCode(err) != connect.CodeInvalidArgument {
		t.Fatalf("bad name = %v, want InvalidArgument", err)
	}
	listed, err := f.triggerClient().ListTriggers(ctx, connect.NewRequest(&orkstackv1.ListTriggersRequest{}))
	if err != nil || len(listed.Msg.GetTriggers()) != 1 || listed.Msg.GetTriggers()[0].GetName() != "github-deploy" {
		t.Fatalf("list = %v, %v; want the one named trigger", listed, err)
	}
}

func TestUpdateTrigger_InPlaceKeepsTokenAndURL(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	f.applyHelloVersion(t, "0.3.0")
	created, token := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	updated, err := f.triggerClient().UpdateTrigger(context.Background(), connect.NewRequest(&orkstackv1.UpdateTriggerRequest{
		Id: created.GetId(), Name: "github-deploy", OrkflowVersion: "0.3.0", Description: "GitHub Actions deploy hook",
	}))
	if err != nil {
		t.Fatalf("UpdateTrigger: %v", err)
	}
	got := updated.Msg.GetTrigger()
	if got.GetId() != created.GetId() || got.GetName() != "github-deploy" || got.GetOrkflowVersion() != "0.3.0" ||
		got.GetOrkflowName() != "hello-ork" || got.GetDescription() != "GitHub Actions deploy hook" {
		t.Fatalf("updated = %v", got)
	}
	status, body := f.invoke(t, created.GetId(), bearer(token), `{}`, nil)
	if status != http.StatusCreated {
		t.Fatalf("invoke with the original token after the update = %d, want 201: %s", status, body)
	}
	if !strings.Contains(string(body), "0.3.0") {
		t.Errorf("the invoked run does not pin the new version: %s", body)
	}
}

func TestUpdateTrigger_PinnedVersionMustExist(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	created, _ := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork", Description: "kept"})
	_, err := f.triggerClient().UpdateTrigger(context.Background(), connect.NewRequest(&orkstackv1.UpdateTriggerRequest{
		Id: created.GetId(), OrkflowVersion: "9.9.9", Description: "should not land",
	}))
	if connectCode(err) != connect.CodeNotFound {
		t.Fatalf("pin to a missing version = %v, want NotFound", err)
	}
	listed, _ := f.triggerClient().ListTriggers(context.Background(), connect.NewRequest(&orkstackv1.ListTriggersRequest{}))
	if d := listed.Msg.GetTriggers()[0].GetDescription(); d != "kept" {
		t.Errorf("a refused update changed the description to %q", d)
	}
}

func TestUpdateTrigger_OtherNamespaceIsNotFound(t *testing.T) {
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Namespaces = newFakeRegistry(&orkstackv1.Namespace{Name: "billing"})
	})
	f.applyHello(t)
	created, _ := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	_, err := f.triggerClient().UpdateTrigger(context.Background(), connect.NewRequest(&orkstackv1.UpdateTriggerRequest{
		Id: created.GetId(), Namespace: "billing", Name: "stolen",
	}))
	if connectCode(err) != connect.CodeNotFound {
		t.Fatalf("update from another namespace = %v, want NotFound", err)
	}
	_, err = f.triggerClient().UpdateTrigger(context.Background(), connect.NewRequest(&orkstackv1.UpdateTriggerRequest{
		Id: "not-an-id", Name: "x",
	}))
	if connectCode(err) != connect.CodeNotFound {
		t.Fatalf("update of a malformed id = %v, want NotFound", err)
	}
	if _, err := f.triggerClient().DeleteTrigger(context.Background(), connect.NewRequest(&orkstackv1.DeleteTriggerRequest{Id: created.GetId()})); err != nil {
		t.Fatalf("DeleteTrigger: %v", err)
	}
	_, err = f.triggerClient().UpdateTrigger(context.Background(), connect.NewRequest(&orkstackv1.UpdateTriggerRequest{
		Id: created.GetId(), Name: "back",
	}))
	if connectCode(err) != connect.CodeNotFound {
		t.Fatalf("update after delete = %v, want NotFound", err)
	}
}

func TestRegisterWebhook_NameRequiresNamespaceAndIsUnique(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_, err := f.webhookClient().RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
		Url: "https://example.test/hook", Name: "etl-loader",
	}))
	if connectCode(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "a name requires a namespace scope") {
		t.Fatalf("a deployment-wide named register = %v, want InvalidArgument", err)
	}
	registered, err := f.webhookClient().RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
		Url: "https://example.test/hook", Namespace: "payments", Name: "etl-loader",
	}))
	if err != nil || registered.Msg.GetWebhook().GetName() != "etl-loader" {
		t.Fatalf("RegisterWebhook = %v, %v", registered, err)
	}
	_, err = f.webhookClient().RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
		Url: "https://example.test/other", Namespace: "payments", Name: "etl-loader",
	}))
	if connectCode(err) != connect.CodeAlreadyExists {
		t.Fatalf("a second register of the name = %v, want AlreadyExists", err)
	}
	listed, err := f.webhookClient().ListWebhooks(ctx, connect.NewRequest(&orkstackv1.ListWebhooksRequest{}))
	if err != nil || len(listed.Msg.GetWebhooks()) != 1 || listed.Msg.GetWebhooks()[0].GetName() != "etl-loader" {
		t.Fatalf("list = %v, %v", listed, err)
	}
}

// durableNotifier is a notifier over a durable registry on the local store,
// delivering to a receiver that records each POST's path, body and headers.
type durableNotifier struct {
	registry *webhooks.Registry
	notifier *webhooks.Notifier
	adapter  *api.WebhookNotifier
	server   *httptest.Server
	posts    chan delivered
}

type delivered struct {
	path    string
	body    []byte
	headers http.Header
}

func newDurableNotifier(t *testing.T) *durableNotifier {
	t.Helper()
	store, err := artifacts.NewLocalWebhookStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalWebhookStore: %v", err)
	}
	d := &durableNotifier{registry: webhooks.NewDurableRegistry(store), posts: make(chan delivered, 8)}
	d.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body bytes.Buffer
		_, _ = body.ReadFrom(r.Body)
		w.WriteHeader(http.StatusOK)
		d.posts <- delivered{path: r.URL.Path, body: body.Bytes(), headers: r.Header.Clone()}
	}))
	t.Cleanup(d.server.Close)
	d.notifier = webhooks.NewNotifier(d.registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{}), webhooks.NotifierOptions{})
	d.adapter = api.NewWebhookNotifier(d.notifier, nil)
	return d
}

func TestUpdateWebhook_KeepsSecretAndValidatesLikeRegister(t *testing.T) {
	d := newDurableNotifier(t)
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.Webhooks = d.registry })
	ctx := context.Background()
	registered, err := f.webhookClient().RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
		Url: d.server.URL + "/first", Namespace: "payments",
		Events: []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
	}))
	if err != nil {
		t.Fatalf("RegisterWebhook: %v", err)
	}
	disclosed := registered.Msg.GetSecret()
	id := registered.Msg.GetWebhook().GetId()

	// The register checks, in the register order.
	for name, req := range map[string]*orkstackv1.UpdateWebhookRequest{
		"bad url":          {Id: id, Url: "ftp://nope"},
		"task_log":         {Id: id, Url: d.server.URL, Events: []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_TASK_LOG}},
		"task w/o event":   {Id: id, Url: d.server.URL, Namespace: "payments", TaskId: "extract"},
		"name w/o a scope": {Id: id, Url: d.server.URL, Name: "etl-loader"},
	} {
		if _, err := f.webhookClient().UpdateWebhook(ctx, connect.NewRequest(req)); connectCode(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: UpdateWebhook = %v, want InvalidArgument", name, err)
		}
	}
	if _, err := f.webhookClient().UpdateWebhook(ctx, connect.NewRequest(&orkstackv1.UpdateWebhookRequest{
		Id: "wh-ffffffffffffffff", Url: d.server.URL,
	})); connectCode(err) != connect.CodeNotFound {
		t.Errorf("UpdateWebhook of an absent id = %v, want NotFound", err)
	}

	updated, err := f.webhookClient().UpdateWebhook(ctx, connect.NewRequest(&orkstackv1.UpdateWebhookRequest{
		Id: id, Name: "etl-loader", Url: d.server.URL + "/second", Namespace: "payments", OrkflowName: "nightly-etl",
		Events: []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
	}))
	if err != nil {
		t.Fatalf("UpdateWebhook: %v", err)
	}
	if updated.Msg.GetWebhook().GetId() != id || updated.Msg.GetWebhook().GetName() != "etl-loader" {
		t.Fatalf("updated = %v", updated.Msg.GetWebhook())
	}
	raw, _ := json.Marshal(updated.Msg)
	if strings.Contains(string(raw), disclosed) {
		t.Fatal("the update response carries the signing secret")
	}

	d.adapter.NotifyRunEvent(ctx, &orkstackv1.Event{
		RunId: "ork-1-1", Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, Status: orkstackv1.Status_STATUS_FAILED,
	}, &orkstackv1.Run{Id: "ork-1-1", Namespace: "payments", OrkflowName: "nightly-etl", Status: orkstackv1.Status_STATUS_FAILED})
	select {
	case post := <-d.posts:
		if post.path != "/second" {
			t.Errorf("delivered to %s, want the updated URL", post.path)
		}
		if !webhooks.Verify(disclosed, post.body, post.headers.Get(webhooks.SignatureHeader)) {
			t.Error("the delivery after the update does not verify with the originally disclosed secret")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery arrived")
	}
}

func TestLegacyWebhookAdapter_AcceptsAndReturnsName(t *testing.T) {
	f := newFixture(t)
	status, body := f.webhookJSON(t, http.MethodPost, api.WebhookCollectionPath, map[string]any{
		"url": "https://example.test/legacy", "namespace": "payments", "name": "etl-loader",
	})
	if status != http.StatusCreated || !strings.Contains(string(body), `"name":"etl-loader"`) {
		t.Fatalf("POST = %d %s, want 201 carrying the name", status, body)
	}
	status, body = f.webhookJSON(t, http.MethodGet, api.WebhookCollectionPath, nil)
	if status != http.StatusOK || !strings.Contains(string(body), `"name":"etl-loader"`) {
		t.Fatalf("GET = %d %s, want the name", status, body)
	}
	status, body = f.webhookJSON(t, http.MethodPost, api.WebhookCollectionPath, map[string]any{
		"url": "https://example.test/legacy", "name": "unscoped",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("a named, unscoped POST = %d %s, want 400", status, body)
	}
	// An unnamed subscription's envelope is byte-identical: no name key.
	status, body = f.webhookJSON(t, http.MethodPost, api.WebhookCollectionPath, map[string]any{"url": "https://example.test/plain"})
	if status != http.StatusCreated || strings.Contains(string(body), `"name"`) {
		t.Fatalf("an unnamed POST = %d %s, want no name key", status, body)
	}
}

func TestAudit_UpdateTriggerAndUpdateWebhookNeverRecordASecret(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	if _, err := f.orkflows().ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	triggersClient := orkstackv1connect.NewTriggerServiceClient(f.server.Client(), f.server.URL)
	webhooksClient := orkstackv1connect.NewWebhookServiceClient(f.server.Client(), f.server.URL)
	trigger, err := triggersClient.CreateTrigger(ctx, connect.NewRequest(&orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork", Name: "deploy-hook"}))
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	const requestSecret = "whsec-update-test-9a8b7c"
	hook, err := webhooksClient.RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
		Url: "https://hooks.example.test/a", Secret: requestSecret, Namespace: "payments", Name: "etl",
	}))
	if err != nil {
		t.Fatalf("RegisterWebhook: %v", err)
	}
	if _, err := triggersClient.UpdateTrigger(ctx, connect.NewRequest(&orkstackv1.UpdateTriggerRequest{
		Id: trigger.Msg.GetTrigger().GetId(), Name: "github-deploy", Description: "hook",
	})); err != nil {
		t.Fatalf("UpdateTrigger: %v", err)
	}
	if _, err := webhooksClient.UpdateWebhook(ctx, connect.NewRequest(&orkstackv1.UpdateWebhookRequest{
		Id: hook.Msg.GetWebhook().GetId(), Name: "etl-loader", Url: "https://hooks.example.test/b",
		Namespace: "payments", OrkflowName: "nightly-etl",
	})); err != nil {
		t.Fatalf("UpdateWebhook: %v", err)
	}
	for key, document := range f.store.Documents() {
		for _, secret := range []string{requestSecret, trigger.Msg.GetSecret()} {
			if bytes.Contains(document, []byte(secret)) {
				t.Errorf("audit record %s contains a secret:\n%s", key, document)
			}
		}
	}
	want := map[string]string{
		"trigger.create":   "hello-ork@current name=deploy-hook",
		"trigger.update":   `name=github-deploy current description="hook"`,
		"webhook.register": "url=https://hooks.example.test/a events=all scope=payments/*/* name=etl",
		"webhook.update":   "name=etl-loader url=https://hooks.example.test/b events=all scope=payments/nightly-etl/*",
	}
	for _, rec := range f.records(t) {
		if detail, ok := want[rec.GetAction()]; ok {
			if rec.GetDetail() != detail {
				t.Errorf("%s detail = %q, want %q", rec.GetAction(), rec.GetDetail(), detail)
			}
			delete(want, rec.GetAction())
		}
	}
	if len(want) != 0 {
		t.Errorf("missing audit records: %v", want)
	}
}
