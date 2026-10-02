package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fakeAuditCounter records every CountAudit call.
type fakeAuditCounter struct {
	mu    sync.Mutex
	calls []string // "action/outcome"
}

func (c *fakeAuditCounter) CountAudit(_ context.Context, action, outcome string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, action+"/"+outcome)
}

func (c *fakeAuditCounter) seen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// failingAuditStore refuses every write and every read.
type failingAuditStore struct{ err error }

func (f failingAuditStore) PutAuditRecord(context.Context, *orkstackv1.AuditRecord) error {
	return f.err
}

func (f failingAuditStore) ListAuditRecords(context.Context, artifacts.AuditQuery) (artifacts.AuditPage, error) {
	return artifacts.AuditPage{}, f.err
}

// lockedBuffer is a goroutine-safe log sink.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type auditFixture struct {
	svc     *api.Service
	store   *artifacts.MemoryAuditStore
	counter *fakeAuditCounter
	logs    *lockedBuffer
	server  *httptest.Server
	now     time.Time
}

type auditOptions struct {
	store artifacts.AuditStore // nil = a fresh memory store
	none  bool                 // wire no audit store at all
	fleet engine.OrkerFleet    // the Orker inventory seam; nil wires none
	// releases is the release routing seam; nil wires none.
	releases engine.ReleaseCluster
	// imagePolicies is the image policy store; nil wires none.
	imagePolicies artifacts.ImagePolicyStore
}

func newAuditFixture(t *testing.T, opts auditOptions) *auditFixture {
	t.Helper()
	f := &auditFixture{
		counter: &fakeAuditCounter{},
		logs:    &lockedBuffer{},
		now:     time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC),
	}
	var store artifacts.AuditStore
	switch {
	case opts.none:
	case opts.store != nil:
		store = opts.store
	default:
		f.store = artifacts.NewMemoryAuditStore()
		store = f.store
	}
	counter := 0
	f.svc = api.NewService(api.Config{
		Orkflows:      engine.NewMemoryOrkflowStore(),
		Namespaces:    newFakeRegistry(),
		Runs:          engine.NewRunStore(),
		Bus:           engine.NewEventBus(),
		Starter:       &fakeStarter{},
		AuditStore:    store,
		AuditCounter:  f.counter,
		Fleet:         opts.fleet,
		Releases:      opts.releases,
		ImagePolicies: opts.imagePolicies,
		Logger:        slog.New(slog.NewTextHandler(f.logs, nil)),
		Now:           func() time.Time { return f.now },
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
	f.server = httptest.NewServer(api.NewMux(f.svc, api.MuxOptions{}))
	t.Cleanup(f.server.Close)
	return f
}

// withPrincipal returns a request carrying the Ork-Principal header.
func withPrincipal[T any](msg *T, principal string) *connect.Request[T] {
	req := connect.NewRequest(msg)
	if principal != "" {
		req.Header().Set(api.AuditPrincipalHeader, principal)
	}
	return req
}

func (f *auditFixture) namespaces() orkstackv1connect.NamespaceServiceClient {
	return orkstackv1connect.NewNamespaceServiceClient(f.server.Client(), f.server.URL)
}

func (f *auditFixture) orkflows() orkstackv1connect.OrkflowServiceClient {
	return orkstackv1connect.NewOrkflowServiceClient(f.server.Client(), f.server.URL)
}

// records returns every stored record, newest first, straight from the store.
func (f *auditFixture) records(t *testing.T) []*orkstackv1.AuditRecord {
	t.Helper()
	page, err := f.store.ListAuditRecords(context.Background(), artifacts.AuditQuery{
		Since: f.now.Add(-time.Hour), Until: f.now.Add(time.Hour), Limit: 1000,
	})
	if err != nil {
		t.Fatalf("list audit records: %v", err)
	}
	return page.Records
}

func TestAuditInterceptor_RecordsSuccessfulWrite(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	created, err := f.svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "payments"},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace (direct, unaudited): %v", err)
	}
	_, err = f.namespaces().UpdateNamespace(ctx, withPrincipal(&orkstackv1.UpdateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name: "payments", Description: "card rails", Labels: map[string]string{"team": "payments"},
			Etag: created.Msg.GetNamespace().GetEtag(),
		},
	}, "dsantos"))
	if err != nil {
		t.Fatalf("UpdateNamespace: %v", err)
	}
	records := f.records(t)
	if len(records) != 1 {
		t.Fatalf("records = %d, want exactly one", len(records))
	}
	rec := records[0]
	if rec.GetAction() != "namespace.update" || rec.GetNamespace() != "payments" || rec.GetTarget() != "payments" ||
		!rec.GetSucceeded() || rec.GetErrorCode() != "" ||
		rec.GetProcedure() != orkstackv1connect.NamespaceServiceUpdateNamespaceProcedure ||
		rec.GetPrincipal() != "dsantos" || rec.GetSourceAddress() == "" ||
		rec.GetSchemaVersion() != artifacts.AuditSchemaVersion || !rec.GetAt().AsTime().Equal(f.now) {
		t.Fatalf("record = %v, want a successful namespace.update of payments by dsantos", rec)
	}
	if !strings.Contains(rec.GetDetail(), "labels={team=payments}") || !strings.Contains(rec.GetDetail(), "default_pool=main") {
		t.Errorf("detail = %q, want the requested spec summary", rec.GetDetail())
	}
	if got := f.counter.seen(); len(got) != 1 || got[0] != "namespace.update/written" {
		t.Errorf("counter = %v, want [namespace.update/written]", got)
	}
}

func TestAuditInterceptor_RecordsRefusedWrite(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	_, direct := f.svc.DeleteNamespace(ctx, connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "default"}))
	_, err := f.namespaces().DeleteNamespace(ctx, connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "default"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("DeleteNamespace(default) = %v, want FailedPrecondition", err)
	}
	var viaWire, viaHandler *connect.Error
	if !errors.As(err, &viaWire) || !errors.As(direct, &viaHandler) || viaWire.Message() != viaHandler.Message() {
		t.Errorf("error through the interceptor %v differs from the handler's %v", err, direct)
	}
	records := f.records(t)
	if len(records) != 1 {
		t.Fatalf("records = %d, want one refusal", len(records))
	}
	rec := records[0]
	if rec.GetSucceeded() || rec.GetErrorCode() != "failed_precondition" || rec.GetAction() != "namespace.delete" ||
		rec.GetNamespace() != "default" || rec.GetTarget() != "default" {
		t.Fatalf("record = %v, want a refused namespace.delete of default", rec)
	}
}

func TestAuditInterceptor_ReadsAreNeverRecorded(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	client := f.server.Client()
	_, _ = f.namespaces().GetNamespace(ctx, connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: "default"}))
	_, _ = orkstackv1connect.NewRunServiceClient(client, f.server.URL).ListRuns(ctx,
		connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	_, _ = f.orkflows().ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: helloYAML(t)}))
	_, _ = orkstackv1connect.NewStatsServiceClient(client, f.server.URL).GetStats(ctx,
		connect.NewRequest(&orkstackv1.GetStatsRequest{}))
	_, _ = orkstackv1connect.NewAuditServiceClient(client, f.server.URL).ListAuditRecords(ctx,
		connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{}))
	if records := f.records(t); len(records) != 0 {
		t.Fatalf("reads produced %d audit records: %v", len(records), records)
	}
	if got := f.counter.seen(); len(got) != 0 {
		t.Errorf("counter saw %v after reads only", got)
	}
}

func TestAuditInterceptor_SubmitRunIsNotRecorded(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	if _, err := f.orkflows().ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	if _, err := orkstackv1connect.NewRunServiceClient(f.server.Client(), f.server.URL).SubmitRun(ctx,
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"})); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	for _, rec := range f.records(t) {
		if rec.GetProcedure() == orkstackv1connect.RunServiceSubmitRunProcedure {
			t.Fatalf("SubmitRun was recorded: %v", rec)
		}
	}
	if n := len(f.records(t)); n != 1 {
		t.Errorf("records = %d, want only the apply", n)
	}
}

func TestAuditInterceptor_NeverRecordsSecrets(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	const requestSecret = "whsec-request-7f3a9c1d2e4b"
	hook, err := orkstackv1connect.NewWebhookServiceClient(f.server.Client(), f.server.URL).RegisterWebhook(ctx,
		connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
			Url:    "https://hooks.example.test/ork",
			Events: []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
			Secret: requestSecret,
		}))
	if err != nil {
		t.Fatalf("RegisterWebhook: %v", err)
	}
	if _, err := f.orkflows().ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	trigger, err := orkstackv1connect.NewTriggerServiceClient(f.server.Client(), f.server.URL).CreateTrigger(ctx,
		connect.NewRequest(&orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"}))
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	secrets := []string{requestSecret, hook.Msg.GetSecret(), trigger.Msg.GetSecret()}
	for key, document := range f.store.Documents() {
		for _, secret := range secrets {
			if secret != "" && bytes.Contains(document, []byte(secret)) {
				t.Errorf("audit record %s contains a secret:\n%s", key, document)
			}
		}
	}
	var sawTrigger, sawWebhook bool
	for _, rec := range f.records(t) {
		switch rec.GetAction() {
		case "trigger.create":
			sawTrigger = true
			if rec.GetDetail() != "hello-ork@current" || rec.GetTarget() != trigger.Msg.GetTrigger().GetId() ||
				rec.GetNamespace() != "default" {
				t.Errorf("trigger record = %v, want detail hello-ork@current on the created id", rec)
			}
		case "webhook.register":
			sawWebhook = true
			if rec.GetNamespace() != "" || rec.GetTarget() != hook.Msg.GetWebhook().GetId() ||
				rec.GetDetail() != "url=https://hooks.example.test/ork events=[RUN_FAILED]" {
				t.Errorf("webhook record = %v, want a deployment-wide registration", rec)
			}
		}
	}
	if !sawTrigger || !sawWebhook {
		t.Fatalf("records = %v, want a trigger.create and a webhook.register", f.records(t))
	}
}

func TestAuditInterceptor_ApplyDetailDistinguishesNoOp(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	for range 2 {
		if _, err := f.orkflows().ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
			t.Fatalf("ApplyOrkflow: %v", err)
		}
	}
	records := f.records(t)
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	// Both share one instant (a fixed clock), so order by detail rather than time.
	details := map[string]bool{}
	for _, rec := range records {
		details[rec.GetDetail()] = true
		if rec.GetTarget() != "hello-ork@0.2.0" || rec.GetNamespace() != "default" {
			t.Errorf("apply record = %v, want target hello-ork@0.2.0 in default", rec)
		}
	}
	if !details["no-op: version already stored"] {
		t.Errorf("details = %v, want the re-apply recorded as a no-op", details)
	}
	var created bool
	for detail := range details {
		created = created || strings.HasPrefix(detail, "created digest=sha256:")
	}
	if !created {
		t.Errorf("details = %v, want the first apply recorded as created with its digest", details)
	}

	// A refused apply names no target: the document never compiled.
	_, err := f.orkflows().ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: "not: [valid", Namespace: ""}))
	if err == nil {
		t.Fatal("ApplyOrkflow of garbage succeeded")
	}
	for _, rec := range f.records(t) {
		if !rec.GetSucceeded() && (rec.GetTarget() != "-" || rec.GetNamespace() != "default" || rec.GetErrorCode() == "") {
			t.Errorf("refused apply record = %v, want target - in default with an error code", rec)
		}
	}
}

func TestAuditInterceptor_PrincipalVerbatimAndUnattributed(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	const principal = "Dan Santos (ops@corp.example)"
	if _, err := f.namespaces().CreateNamespace(ctx, withPrincipal(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "attributed"},
	}, principal)); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if _, err := f.namespaces().CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "anonymous"},
	})); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	byTarget := map[string]*orkstackv1.AuditRecord{}
	for _, rec := range f.records(t) {
		byTarget[rec.GetTarget()] = rec
	}
	if got := byTarget["attributed"].GetPrincipal(); got != principal {
		t.Errorf("principal = %q, want %q verbatim", got, principal)
	}
	if got := byTarget["anonymous"].GetPrincipal(); got != "" {
		t.Errorf("principal = %q with no header, want empty", got)
	}
	if !strings.HasPrefix(byTarget["attributed"].GetDetail(), "created ") {
		t.Errorf("detail = %q, want it to start with created", byTarget["attributed"].GetDetail())
	}
}

func TestAuditInterceptor_StoreFailureIsNotFatal(t *testing.T) {
	broken := newAuditFixture(t, auditOptions{store: failingAuditStore{err: errors.New("bucket is on fire")}})
	working := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	req := func() *connect.Request[orkstackv1.CreateNamespaceRequest] {
		return connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: &orkstackv1.Namespace{Name: "incident"}})
	}
	brokenResp, brokenErr := broken.namespaces().CreateNamespace(ctx, req())
	workingResp, workingErr := working.namespaces().CreateNamespace(ctx, req())
	if brokenErr != nil || workingErr != nil {
		t.Fatalf("CreateNamespace = %v / %v, want both to succeed", brokenErr, workingErr)
	}
	if brokenResp.Msg.GetCreated() != workingResp.Msg.GetCreated() ||
		brokenResp.Msg.GetNamespace().GetName() != workingResp.Msg.GetNamespace().GetName() {
		t.Errorf("responses differ: %v vs %v", brokenResp.Msg, workingResp.Msg)
	}
	if got := broken.counter.seen(); len(got) != 1 || got[0] != "namespace.create/dropped" {
		t.Errorf("broken counter = %v, want [namespace.create/dropped]", got)
	}
	if got := working.counter.seen(); len(got) != 1 || got[0] != "namespace.create/written" {
		t.Errorf("working counter = %v, want [namespace.create/written]", got)
	}
	logs := broken.logs.String()
	if strings.Count(logs, "audit record dropped") != 1 || !strings.Contains(logs, "level=WARN") ||
		!strings.Contains(logs, "bucket is on fire") || !strings.Contains(logs, "action=namespace.create") {
		t.Errorf("engine log = %q, want one warn line naming the action and the error", logs)
	}
}

func TestLegacyWebhookAdapter_RecordsAudit(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	const secret = "legacy-secret-5a6b7c8d"
	body := `{"url":"https://hooks.example.test/legacy","events":["run_succeeded"],"secret":"` + secret + `"}`
	post, err := http.NewRequest(http.MethodPost, f.server.URL+api.WebhookCollectionPath, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	post.Header.Set("Content-Type", "application/json")
	post.Header.Set(api.AuditPrincipalHeader, "legacy-operator")
	resp, err := f.server.Client().Do(post)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201", resp.StatusCode)
	}
	hooks, err := f.svc.Webhooks().ListContext(context.Background())
	if err != nil || len(hooks) != 1 {
		t.Fatalf("webhooks = %v, %v; want one", hooks, err)
	}
	id := hooks[0].ID

	del, _ := http.NewRequest(http.MethodDelete, f.server.URL+"/v1/webhooks/"+id, nil)
	resp, err = f.server.Client().Do(del)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}
	// A second delete is refused, and recorded as a refusal.
	del2, _ := http.NewRequest(http.MethodDelete, f.server.URL+"/v1/webhooks/"+id, nil)
	resp, err = f.server.Client().Do(del2)
	if err != nil {
		t.Fatalf("DELETE again: %v", err)
	}
	_ = resp.Body.Close()

	byProcedure := map[string][]*orkstackv1.AuditRecord{}
	for _, rec := range f.records(t) {
		byProcedure[rec.GetProcedure()] = append(byProcedure[rec.GetProcedure()], rec)
	}
	register := byProcedure["POST /v1/webhooks"]
	if len(register) != 1 || register[0].GetAction() != "webhook.register" || register[0].GetTarget() != id ||
		register[0].GetPrincipal() != "legacy-operator" || !register[0].GetSucceeded() ||
		register[0].GetSourceAddress() == "" || register[0].GetDetail() != "url=https://hooks.example.test/legacy events=[RUN_SUCCEEDED]" {
		t.Errorf("register records = %v, want one attributed webhook.register of %s", register, id)
	}
	deletes := byProcedure["DELETE /v1/webhooks/{id}"]
	if len(deletes) != 2 {
		t.Fatalf("delete records = %v, want two", deletes)
	}
	var ok, refused int
	for _, rec := range deletes {
		if rec.GetAction() != "webhook.delete" || rec.GetTarget() != id {
			t.Errorf("delete record = %v", rec)
		}
		if rec.GetSucceeded() {
			ok++
		} else if rec.GetErrorCode() == "not_found" {
			refused++
		}
	}
	if ok != 1 || refused != 1 {
		t.Errorf("deletes = %d ok / %d refused not_found, want 1/1", ok, refused)
	}
	for key, document := range f.store.Documents() {
		if bytes.Contains(document, []byte(secret)) {
			t.Errorf("audit record %s contains the legacy webhook secret", key)
		}
	}
}

func (f *auditFixture) audit() orkstackv1connect.AuditServiceClient {
	return orkstackv1connect.NewAuditServiceClient(f.server.Client(), f.server.URL)
}

func TestListAuditRecords_WindowDefaultsAndValidation(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	resp, err := f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{}))
	if err != nil {
		t.Fatalf("ListAuditRecords: %v", err)
	}
	if !resp.Msg.GetUntil().AsTime().Equal(f.now) || !resp.Msg.GetSince().AsTime().Equal(f.now.Add(-7*24*time.Hour)) {
		t.Errorf("window = [%s, %s), want [now-7d, now)", resp.Msg.GetSince().AsTime(), resp.Msg.GetUntil().AsTime())
	}
	_, err = f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{
		Since: timestamppb.New(f.now), Until: timestamppb.New(f.now.Add(-time.Hour)),
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "must be after since") {
		t.Errorf("inverted window = %v, want InvalidArgument naming both", err)
	}
	_, err = f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{PageSize: 201}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("page_size 201 = %v, want InvalidArgument", err)
	}
	_, err = f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{PageSize: -1}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("page_size -1 = %v, want InvalidArgument", err)
	}
}

// seed writes records directly into the fixture's store, one second apart,
// newest last.
func (f *auditFixture) seed(t *testing.T, specs ...[2]string) []*orkstackv1.AuditRecord {
	t.Helper()
	var out []*orkstackv1.AuditRecord
	for i, spec := range specs {
		at := f.now.Add(-time.Duration(len(specs)-i) * time.Second)
		rec := &orkstackv1.AuditRecord{
			SchemaVersion: artifacts.AuditSchemaVersion,
			Id:            artifacts.NewAuditID(at),
			At:            timestamppb.New(at),
			Namespace:     spec[0],
			Action:        spec[1],
			Succeeded:     true,
		}
		if err := f.store.PutAuditRecord(context.Background(), rec); err != nil {
			t.Fatalf("seed: %v", err)
		}
		out = append(out, rec)
	}
	return out
}

func TestListAuditRecords_CursorBindsFilters(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	seeded := f.seed(t, [2]string{"a", "namespace.update"}, [2]string{"a", "namespace.update"}, [2]string{"a", "namespace.update"})
	first, err := f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{
		ActionPrefix: "namespace.", PageSize: 1,
	}))
	if err != nil || len(first.Msg.GetRecords()) != 1 || first.Msg.GetNextPageToken() == "" {
		t.Fatalf("first page = %v, %v; want one record and a token", first, err)
	}
	if !strings.HasPrefix(first.Msg.GetNextPageToken(), "v1:") {
		t.Errorf("token %q is not versioned", first.Msg.GetNextPageToken())
	}
	_, err = f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{
		ActionPrefix: "orkflow.", PageSize: 1, PageToken: first.Msg.GetNextPageToken(),
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "filters changed") {
		t.Errorf("token replayed with another action_prefix = %v, want InvalidArgument (filters changed)", err)
	}
	// The same filters continue — and the page size may change between pages.
	// The clock moves between pages: the token, not "now", fixes the window.
	f.now = f.now.Add(time.Hour)
	second, err := f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{
		ActionPrefix: "namespace.", PageSize: 5, PageToken: first.Msg.GetNextPageToken(),
	}))
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	got := []string{first.Msg.GetRecords()[0].GetId()}
	for _, rec := range second.Msg.GetRecords() {
		got = append(got, rec.GetId())
	}
	want := []string{seeded[2].GetId(), seeded[1].GetId(), seeded[0].GetId()}
	if strings.Join(got, ",") != strings.Join(want, ",") || second.Msg.GetNextPageToken() != "" {
		t.Errorf("walk = %v (next %q), want %v and an exhausted window", got, second.Msg.GetNextPageToken(), want)
	}
	if _, err := f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{
		PageToken: "v1:not-base64!",
	})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("garbage token = %v, want InvalidArgument", err)
	}
}

func TestListAuditRecords_NamespaceEmptyMeansEvery(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	f.seed(t, [2]string{"", "retention.update"}, [2]string{"default", "namespace.update"}, [2]string{"payments", "orkflow.promote"})
	for _, tc := range []struct {
		namespace string
		want      int
	}{{"", 3}, {"default", 1}, {"payments", 1}, {"missing", 0}} {
		resp, err := f.audit().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{Namespace: tc.namespace}))
		if err != nil {
			t.Fatalf("ListAuditRecords(%q): %v", tc.namespace, err)
		}
		if got := len(resp.Msg.GetRecords()); got != tc.want {
			t.Errorf("namespace %q: %d records, want %d", tc.namespace, got, tc.want)
		}
		for _, rec := range resp.Msg.GetRecords() {
			if tc.namespace != "" && rec.GetNamespace() != tc.namespace {
				t.Errorf("namespace %q returned a record of %q", tc.namespace, rec.GetNamespace())
			}
		}
	}
}

func TestListAuditRecords_NoStoreIsFailedPrecondition(t *testing.T) {
	f := newAuditFixture(t, auditOptions{none: true})
	_, err := f.audit().ListAuditRecords(context.Background(), connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "audit store is not configured") {
		t.Fatalf("ListAuditRecords with no store = %v, want FailedPrecondition", err)
	}
	// And a write with no store still succeeds, recording nothing.
	if _, err := f.namespaces().CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "quiet"},
	})); err != nil {
		t.Fatalf("CreateNamespace with no audit store: %v", err)
	}
}

func TestListAuditRecords_ScanBudgetIsResourceExhausted(t *testing.T) {
	f := newAuditFixture(t, auditOptions{store: failingAuditStore{err: fmt.Errorf("walk: %w", artifacts.ErrAuditScanBudget)}})
	_, err := f.audit().ListAuditRecords(context.Background(), connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{}))
	if connect.CodeOf(err) != connect.CodeResourceExhausted || !strings.Contains(err.Error(), "5000") ||
		!strings.Contains(err.Error(), "narrow --since/--until") {
		t.Fatalf("budget refusal = %v, want ResourceExhausted naming the budget and the fix", err)
	}
	f2 := newAuditFixture(t, auditOptions{store: failingAuditStore{err: errors.New("connection refused")}})
	_, err = f2.audit().ListAuditRecords(context.Background(), connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("store outage = %v, want Unavailable", err)
	}
}

// A scoped subscription's audit detail names its scope the way `orkctl webhook
// list` renders it (namespace/orkflow/task, "*" per unset component), through
// both the Connect service and the legacy adapter; an unscoped one carries no
// scope, so its detail is unchanged.
func TestAuditWebhookRegister_DetailNamesScope(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	hooks := orkstackv1connect.NewWebhookServiceClient(f.server.Client(), f.server.URL)
	if _, err := hooks.RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
		Url:       "https://hooks.example.test/scoped",
		Events:    []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_TASK_FAILED},
		Namespace: "payments",
		TaskId:    "migrate",
	})); err != nil {
		t.Fatalf("RegisterWebhook: %v", err)
	}
	body := `{"url":"https://hooks.example.test/legacy-scoped","events":["run_failed"],"orkflowName":"nightly"}`
	post, err := http.NewRequest(http.MethodPost, f.server.URL+api.WebhookCollectionPath, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	post.Header.Set("Content-Type", "application/json")
	resp, err := f.server.Client().Do(post)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201", resp.StatusCode)
	}
	want := map[string]string{
		orkstackv1connect.WebhookServiceRegisterWebhookProcedure: "url=https://hooks.example.test/scoped events=[TASK_FAILED] scope=payments/*/migrate",
		"POST /v1/webhooks": "url=https://hooks.example.test/legacy-scoped events=[RUN_FAILED] scope=*/nightly/*",
	}
	got := map[string]string{}
	for _, rec := range f.records(t) {
		if rec.GetAction() == "webhook.register" {
			got[rec.GetProcedure()] = rec.GetDetail()
			if rec.GetNamespace() != "" {
				t.Errorf("record %v: namespace = %q, want empty (a registration is deployment-wide)", rec, rec.GetNamespace())
			}
		}
	}
	for procedure, detail := range want {
		if got[procedure] != detail {
			t.Errorf("%s detail = %q, want %q", procedure, got[procedure], detail)
		}
	}
}

// TestAudit_NamespaceUpdateRecordsHoldWhenPresent is the audit half of
// namespace admission control: the record of a write that sets a hold names
// its reason and cap, the record of the release has no hold= key, a create
// carries both after its outcome, and a refused write still records the
// summary it was asked for.
func TestAudit_NamespaceUpdateRecordsHoldWhenPresent(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	const reason = "partner CI loop, INC-4412"
	const held = `hold="partner CI loop, INC-4412" max_running=20`

	created, err := f.svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "payments"},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace (direct, unaudited): %v", err)
	}
	updated, err := f.namespaces().UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name: "payments", Hold: &orkstackv1.NamespaceHold{Reason: reason}, MaxRunning: 20,
			Etag: created.Msg.GetNamespace().GetEtag(),
		},
	}))
	if err != nil {
		t.Fatalf("UpdateNamespace (hold): %v", err)
	}
	records := f.records(t)
	if len(records) != 1 {
		t.Fatalf("records after the hold = %d, want exactly one", len(records))
	}
	hold := records[0]
	if hold.GetAction() != "namespace.update" || !hold.GetSucceeded() {
		t.Fatalf("record = %v, want a successful namespace.update", hold)
	}
	detail := hold.GetDetail()
	at, retention, sink := strings.Index(detail, held), strings.Index(detail, "run_retention="),
		strings.Index(detail, "telemetry.sink=")
	if at < 0 || retention < 0 || sink < 0 || !(retention < at && at < sink) {
		t.Errorf("hold detail = %q, want %s after run_retention= and before telemetry.sink=", detail, held)
	}

	f.now = f.now.Add(time.Second) // newest first is by at; give each write its own
	_, err = f.namespaces().UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name: "payments", MaxRunning: 20, Etag: updated.Msg.GetNamespace().GetEtag(),
		},
	}))
	if err != nil {
		t.Fatalf("UpdateNamespace (release): %v", err)
	}
	records = f.records(t)
	if len(records) != 2 {
		t.Fatalf("records after the release = %d, want two", len(records))
	}
	release := records[0].GetDetail()
	if strings.Contains(release, "hold=") || !strings.Contains(release, "max_running=20") {
		t.Errorf("release detail = %q, want no hold= and still max_running=20", release)
	}

	f.now = f.now.Add(time.Second)
	billing, err := f.namespaces().CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name: "billing", Hold: &orkstackv1.NamespaceHold{Reason: reason}, MaxRunning: 20,
		},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace (held): %v", err)
	}
	records = f.records(t)
	if len(records) != 3 {
		t.Fatalf("records after the create = %d, want three", len(records))
	}
	create := records[0].GetDetail()
	if records[0].GetAction() != "namespace.create" || !strings.HasPrefix(create, "created ") ||
		!strings.Contains(create, held) {
		t.Errorf("create detail = %q, want created followed by %s", create, held)
	}

	f.now = f.now.Add(time.Second)
	_, err = f.namespaces().UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name: "billing", Hold: &orkstackv1.NamespaceHold{Reason: "  "}, MaxRunning: 20,
			Etag: billing.Msg.GetNamespace().GetEtag(),
		},
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("UpdateNamespace (blank reason) = %v, want InvalidArgument", err)
	}
	records = f.records(t)
	if len(records) != 4 {
		t.Fatalf("records after the refusal = %d, want four", len(records))
	}
	refused := records[0]
	if refused.GetSucceeded() || refused.GetErrorCode() != "invalid_argument" ||
		!strings.Contains(refused.GetDetail(), `hold="  " max_running=20`) {
		t.Errorf("refused record = %v, want succeeded=false with the requested summary", refused)
	}
}

// TestAudit_CordonOrkerIsClassified: every cordon write is an audit record —
// orker.cordon or orker.uncordon, the id as target, namespace empty (the
// Orker is deployment infrastructure), the reason in a bounded detail — and
// ListOrkers, a read, records nothing.
func TestAudit_CordonOrkerIsClassified(t *testing.T) {
	f := newAuditFixture(t, auditOptions{fleet: threeOrkerFleet()})
	ctx := context.Background()
	activities := orkstackv1connect.NewActivityServiceClient(f.server.Client(), f.server.URL)

	if _, err := activities.ListOrkers(ctx, connect.NewRequest(&orkstackv1.ListOrkersRequest{})); err != nil {
		t.Fatalf("ListOrkers: %v", err)
	}
	if records := f.records(t); len(records) != 0 {
		t.Fatalf("a read was recorded: %v", records)
	}
	if _, err := activities.CordonOrker(ctx, connect.NewRequest(&orkstackv1.CordonOrkerRequest{
		OrkerId: "orker-7f3a", Cordoned: true, Reason: "CVE-2026-1234 runtime patch"})); err != nil {
		t.Fatalf("CordonOrker: %v", err)
	}
	f.now = f.now.Add(time.Second)
	if _, err := activities.CordonOrker(ctx, connect.NewRequest(&orkstackv1.CordonOrkerRequest{OrkerId: "orker-7f3a"})); err != nil {
		t.Fatalf("uncordon: %v", err)
	}
	f.now = f.now.Add(time.Second)
	long := strings.Repeat("x", 256)
	if _, err := activities.CordonOrker(ctx, connect.NewRequest(&orkstackv1.CordonOrkerRequest{
		OrkerId: "orker-typo", Cordoned: true, Reason: long})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("cordon of an unknown id = %v; want NotFound", err)
	}

	records := f.records(t)
	if len(records) != 3 {
		t.Fatalf("records = %d; want three", len(records))
	}
	refused, uncordon, cordon := records[0], records[1], records[2]
	if cordon.GetAction() != "orker.cordon" || cordon.GetTarget() != "orker-7f3a" || cordon.GetNamespace() != "" ||
		!cordon.GetSucceeded() || cordon.GetDetail() != `reason="CVE-2026-1234 runtime patch" pool=build` {
		t.Fatalf("cordon record = %v", cordon)
	}
	if uncordon.GetAction() != "orker.uncordon" || uncordon.GetTarget() != "orker-7f3a" || uncordon.GetNamespace() != "" ||
		!uncordon.GetSucceeded() {
		t.Fatalf("uncordon record = %v", uncordon)
	}
	if refused.GetAction() != "orker.cordon" || refused.GetSucceeded() || refused.GetErrorCode() != "not_found" ||
		len(refused.GetDetail()) > artifacts.AuditDetailMaxBytes || !strings.HasPrefix(refused.GetDetail(), `reason="xxx`) {
		t.Fatalf("refused record = %v", refused)
	}
}

// TestAudit_NamespaceUpdateRecordsAllowedSecretsWhenPresent: a write that
// sets the list is recorded with allowed_secrets= after allowed_pools=; the
// write that clears it is recorded with no such key.
func TestAudit_NamespaceUpdateRecordsAllowedSecretsWhenPresent(t *testing.T) {
	f := newAuditFixture(t, auditOptions{})
	ctx := context.Background()
	const listed = "allowed_secrets=[env:SANDBOX_* file:/etc/orkstack/secrets/sandbox/*]"

	created, err := f.svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "sandbox"},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace (direct, unaudited): %v", err)
	}
	updated, err := f.namespaces().UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name:           "sandbox",
			AllowedSecrets: []string{"env:SANDBOX_*", "file:/etc/orkstack/secrets/sandbox/*"},
			Etag:           created.Msg.GetNamespace().GetEtag(),
		},
	}))
	if err != nil {
		t.Fatalf("UpdateNamespace (set): %v", err)
	}
	records := f.records(t)
	if len(records) != 1 || records[0].GetAction() != "namespace.update" || !records[0].GetSucceeded() {
		t.Fatalf("records after the set = %v, want one successful namespace.update", records)
	}
	detail := records[0].GetDetail()
	pools, at, retention := strings.Index(detail, "allowed_pools="), strings.Index(detail, listed),
		strings.Index(detail, "run_retention=")
	if pools < 0 || at < 0 || retention < 0 || !(pools < at && at < retention) {
		t.Errorf("set detail = %q, want %s after allowed_pools= and before run_retention=", detail, listed)
	}

	f.now = f.now.Add(time.Second)
	if _, err := f.namespaces().UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "sandbox", Etag: updated.Msg.GetNamespace().GetEtag()},
	})); err != nil {
		t.Fatalf("UpdateNamespace (clear): %v", err)
	}
	records = f.records(t)
	if len(records) != 2 {
		t.Fatalf("records after the clear = %d, want two", len(records))
	}
	if clear := records[0].GetDetail(); strings.Contains(clear, "allowed_secrets=") {
		t.Errorf("clear detail = %q, want no allowed_secrets=", clear)
	}
}

func TestAudit_UpdateImagePolicyRecordsFingerprintsNeverPEM(t *testing.T) {
	f := newAuditFixture(t, auditOptions{imagePolicies: &memImagePolicies{}})
	client := orkstackv1connect.NewSystemServiceClient(f.server.Client(), f.server.URL)
	_, err := client.UpdateImagePolicy(context.Background(), withPrincipal(&orkstackv1.UpdateImagePolicyRequest{
		Policy: &orkstackv1.ImagePolicy{
			RequireDigest:     true,
			AllowedRegistries: []string{"ghcr.io/", "localhost:5001/orkstack/"},
			SigningKeys:       map[string]string{"release": task.FixedSigningKeyPEM},
		},
	}, "dsantos"))
	if err != nil {
		t.Fatalf("UpdateImagePolicy: %v", err)
	}
	if _, err := client.GetImagePolicy(context.Background(), connect.NewRequest(&orkstackv1.GetImagePolicyRequest{})); err != nil {
		t.Fatalf("GetImagePolicy: %v", err)
	}
	records := f.records(t)
	if len(records) != 1 {
		t.Fatalf("records = %d, want one (the read is not audited)", len(records))
	}
	rec := records[0]
	want := "require_digest=true allowed_registries=[ghcr.io/ localhost:5001/orkstack/] signing_keys={release=" +
		task.FixedSigningKeyFingerprint + "}"
	if rec.GetAction() != "image_policy.update" || rec.GetTarget() != "deployment" || rec.GetNamespace() != "" ||
		!rec.GetSucceeded() || rec.GetDetail() != want {
		t.Fatalf("record = %v\nwant action image_policy.update, target deployment, detail %s", rec, want)
	}
	if strings.Contains(rec.GetDetail(), "-----BEGIN") {
		t.Fatalf("the audit detail carries PEM: %s", rec.GetDetail())
	}
}
