package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordingAuditStore is the real in-memory audit store with every query the
// engine made of it remembered, so a test can see the namespace the CLI sent.
type recordingAuditStore struct {
	*artifacts.MemoryAuditStore
	mu      sync.Mutex
	queries []artifacts.AuditQuery
}

func (r *recordingAuditStore) ListAuditRecords(ctx context.Context, q artifacts.AuditQuery) (artifacts.AuditPage, error) {
	r.mu.Lock()
	r.queries = append(r.queries, q)
	r.mu.Unlock()
	return r.MemoryAuditStore.ListAuditRecords(ctx, q)
}

func (r *recordingAuditStore) lastQuery(t *testing.T) artifacts.AuditQuery {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queries) == 0 {
		t.Fatal("the audit store was never queried")
	}
	return r.queries[len(r.queries)-1]
}

// auditFixture is the real API over a seeded audit store. The records are the
// WHY example of the spec, stamped in the fixture's window.
func auditFixture(t *testing.T) (*fixture, *recordingAuditStore) {
	t.Helper()
	store := &recordingAuditStore{MemoryAuditStore: artifacts.NewMemoryAuditStore()}
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.AuditStore = store })
	seed := []*orkstackv1.AuditRecord{
		{Principal: "", Action: "namespace.delete", Namespace: "staging", Target: "staging",
			Procedure: "/orkstack.v1.NamespaceService/DeleteNamespace", ErrorCode: "failed_precondition"},
		{Principal: "dsantos", Action: "trigger.create", Namespace: "prod-payments", Target: "trg-4a91c0e2b7d3f815",
			Procedure: "/orkstack.v1.TriggerService/CreateTrigger", Detail: "settle@current", Succeeded: true},
		{Principal: "dsantos", Action: "namespace.update", Namespace: "prod-payments", Target: "prod-payments",
			Procedure: "/orkstack.v1.NamespaceService/UpdateNamespace", Succeeded: true},
		{Principal: "svc-platform-ci", Action: "retention.update", Target: "deployment",
			Procedure: "/orkstack.v1.SystemService/UpdateRetentionPolicy", Detail: "max_run_retention=720h0m0s", Succeeded: true},
	}
	for i, rec := range seed {
		at := fixedNow.Add(-time.Duration(len(seed)-i) * time.Hour)
		rec.SchemaVersion = artifacts.AuditSchemaVersion
		rec.At = timestamppb.New(at)
		// Fixed ids, so the JSON golden holds across runs.
		rec.Id = "aud-" + artifacts.RunStamp(at) + "-" + strings.Repeat(string(rune('a'+i)), 16)
		rec.SourceAddress = "10.0.0.7:51234"
		if err := store.PutAuditRecord(context.Background(), rec); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return f, store
}

func TestAdminAudit_Golden(t *testing.T) {
	f, _ := auditFixture(t)

	human := f.run("admin", "audit").requireExit(t, exitOK)
	requireGolden(t, "admin-audit.golden", human.stdout)

	machine := f.run("--json", "admin", "audit").requireExit(t, exitOK)
	requireGolden(t, "admin-audit.json.golden", machine.stdout)
	var document struct {
		Records []json.RawMessage `json:"records"`
	}
	if err := json.Unmarshal([]byte(machine.stdout), &document); err != nil || len(document.Records) != 4 {
		t.Fatalf("--json is not the raw envelope with four records: %v\n%s", err, machine.stdout)
	}

	empty := f.run("admin", "audit", "--action", "webhook.").requireExit(t, exitOK)
	requireGolden(t, "admin-audit-empty.golden", empty.stdout)

	paged := f.run("admin", "audit", "--limit", "2", "--principal", "dsantos",
		"--since", "2026-01-01").requireExit(t, exitOK)
	requireGolden(t, "admin-audit-next-page.golden", paged.stdout)

	// The printed command continues the same listing rather than being refused
	// for changed filters.
	line := paged.stdout[strings.Index(paged.stdout, "Next page: orkctl ")+len("Next page: orkctl "):]
	line = strings.TrimSpace(strings.SplitN(line, "\n", 2)[0])
	next := f.run(strings.Fields(line)...).requireExit(t, exitOK)
	if !strings.Contains(next.stdout, "No audit records in this window.") || strings.Contains(next.stdout, "Next page") {
		t.Errorf("the next-page command did not finish the listing:\n%s", next.stdout)
	}
}

func TestAdminAudit_NamespaceOnlyFromCommandLine(t *testing.T) {
	f, store := auditFixture(t)
	t.Setenv(namespaceEnvVar, "payments")

	every := f.run("admin", "audit").requireExit(t, exitOK)
	if q := store.lastQuery(t); q.Namespace != "" {
		t.Errorf("with only ORK_NAMESPACE set the CLI sent namespace %q, want empty (every namespace)", q.Namespace)
	}
	if !strings.Contains(every.stdout, "Scope: every namespace") {
		t.Errorf("scope line missing:\n%s", every.stdout)
	}

	one := f.run("-n", "payments", "admin", "audit").requireExit(t, exitOK)
	if q := store.lastQuery(t); q.Namespace != "payments" {
		t.Errorf("-n payments sent namespace %q", q.Namespace)
	}
	if !strings.Contains(one.stdout, "Scope: namespace payments") {
		t.Errorf("scope line missing:\n%s", one.stdout)
	}
}

func TestAdminAudit_RendersUnattributed(t *testing.T) {
	f, _ := auditFixture(t)
	out := f.run("admin", "audit", "--action", "namespace.delete").requireExit(t, exitOK)
	var row string
	for _, line := range strings.Split(out.stdout, "\n") {
		if strings.Contains(line, "namespace.delete") {
			row = line
		}
	}
	if !strings.Contains(row, auditUnattributed) || !strings.Contains(row, "refused: failed_precondition") {
		t.Fatalf("row = %q, want an unattributed refusal", row)
	}
	if !strings.Contains(out.stdout, "does not authenticate it") {
		t.Errorf("caption missing:\n%s", out.stdout)
	}
}

func TestAdminAudit_RefusesAWindowLocally(t *testing.T) {
	f, _ := auditFixture(t)
	before := len(f.requests())
	out := f.run("admin", "audit", "--since", "2026-02-01", "--until", "2026-01-01")
	if out.code == exitOK || !strings.Contains(out.stderr, "must be after") {
		t.Fatalf("inverted window = %d %q, want a local refusal", out.code, out.stderr)
	}
	bad := f.run("admin", "audit", "--since", "yesterday")
	if bad.code == exitOK || !strings.Contains(bad.stderr, "RFC3339") {
		t.Fatalf("--since yesterday = %d %q, want a local refusal", bad.code, bad.stderr)
	}
	if after := len(f.requests()); after != before {
		t.Errorf("a locally refused window still sent %d requests", after-before)
	}
}

// TestPrincipalHeader_SentOnlyWhenEnvSet drives several commands — writes and
// reads — against a listener that remembers every request's Ork-Principal.
func TestPrincipalHeader_SentOnlyWhenEnvSet(t *testing.T) {
	var (
		mu   sync.Mutex
		seen []string
	)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.Path+"="+r.Header.Get("Ork-Principal"))
		mu.Unlock()
		http.Error(w, "no", http.StatusServiceUnavailable)
	}))
	// The CLI speaks h2c to an http:// engine, exactly as ork serves it.
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")

	commands := [][]string{
		{"admin", "info"},
		{"admin", "audit"},
		{"namespace", "create", "payments"},
		{"admin", "retention", "set", "24h"},
		{"run", "list"},
	}
	drive := func() []string {
		mu.Lock()
		seen = nil
		mu.Unlock()
		for _, args := range commands {
			var out, errOut strings.Builder
			Execute(append(args, "--server", server.URL), &out, &errOut)
		}
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}

	t.Setenv(principalEnvVar, "dsantos")
	withEnv := drive()
	if len(withEnv) < len(commands) {
		t.Fatalf("only %d requests reached the server: %v", len(withEnv), withEnv)
	}
	for _, request := range withEnv {
		if !strings.HasSuffix(request, "=dsantos") {
			t.Errorf("request %s did not carry Ork-Principal: dsantos", request)
		}
	}

	t.Setenv(principalEnvVar, "")
	for _, request := range drive() {
		if !strings.HasSuffix(request, "=") {
			t.Errorf("request %s carried Ork-Principal with ORK_PRINCIPAL unset", request)
		}
	}
}
