package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// exfilOrkflow names a secret source the sandbox namespace's list does not
// allow. It is the research narrative's contractor document.
const exfilOrkflow = `
apiVersion: orkstack/v1
kind: Orkflow
name: exfil
version: "1"
tasks:
  - id: grab
    uses: shell
    secrets:
      - name: db
        source: env:PROD_DB_PASSWORD
    with: {command: "cat /orkstack/input/secrets/db"}
`

// permittedOrkflow names a source inside env:SANDBOX_*.
const permittedOrkflow = `
apiVersion: orkstack/v1
kind: Orkflow
name: fetch
version: "1"
tasks:
  - id: fetch
    uses: shell
    secrets:
      - name: db
        source: env:SANDBOX_DB
    with: {command: "sha256sum /orkstack/input/secrets/db"}
`

const exfilRefusal = `namespace "sandbox" may not resolve "env:PROD_DB_PASSWORD" (task "grab", secret "db"); allowed: env:SANDBOX_*`

// secretPolicyService is one engine replica over a shared catalog and
// registry: two of these over the same stores are two replicas.
func secretPolicyService(
	orkflows engine.OrkflowStore,
	reg *fakeRegistry,
	starter *fakeStarter,
	claimer api.IdempotencyClaimer,
) (*api.Service, *engine.RunStore) {
	runs := engine.NewRunStore()
	counter := 0
	return api.NewService(api.Config{
		Orkflows:    orkflows,
		Namespaces:  reg,
		Runs:        runs,
		Bus:         engine.NewEventBus(),
		Starter:     starter,
		Idempotency: claimer,
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	}), runs
}

// tighten replaces sandbox's allowed_secrets through the registry, as a
// `namespace update` on another replica would.
func tighten(t *testing.T, reg *fakeRegistry, allowed ...string) {
	t.Helper()
	ns, err := reg.Get(context.Background(), "sandbox")
	if err != nil {
		t.Fatalf("Get sandbox: %v", err)
	}
	next := proto.Clone(ns).(*orkstackv1.Namespace)
	next.AllowedSecrets = allowed
	if _, err := reg.Put(context.Background(), next, artifacts.PutConditions{IfMatch: ns.GetEtag()}); err != nil {
		t.Fatalf("tighten sandbox to %v: %v", allowed, err)
	}
}

func TestValidateOrkflow_ReportsAForbiddenSecretOnItsFieldPath(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: []string{"env:SANDBOX_*"}})
	svc, _ := secretPolicyService(orkflows, reg, &fakeStarter{}, nil)

	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: exfilOrkflow, Namespace: "sandbox"}))
	if err != nil {
		t.Fatalf("ValidateOrkflow: %v", err)
	}
	issues := resp.Msg.GetIssues()
	if len(issues) != 1 {
		t.Fatalf("issues = %v, want exactly one", issues)
	}
	issue := issues[0]
	if issue.GetField() != "tasks[0].secrets[0].source" ||
		issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR ||
		issue.GetMessage() != exfilRefusal {
		t.Fatalf("issue = {%q %v %q}\nwant {tasks[0].secrets[0].source ERROR %q}",
			issue.GetField(), issue.GetSeverity(), issue.GetMessage(), exfilRefusal)
	}
	if resp.Msg.GetOrkflow() != nil {
		t.Errorf("a refused validate returned a definition: %v", resp.Msg.GetOrkflow())
	}
	if list, _ := orkflows.List(context.Background(), "sandbox"); len(list) != 0 {
		t.Errorf("validate stored %d definitions", len(list))
	}

	// A permitted source validates clean.
	ok, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: permittedOrkflow, Namespace: "sandbox"}))
	if err != nil || len(ok.Msg.GetIssues()) != 0 || ok.Msg.GetOrkflow() == nil {
		t.Fatalf("permitted validate = %v, %v", ok, err)
	}
}

func TestApplyOrkflow_RefusesASecretTheNamespaceDoesNotAllow(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: []string{"env:SANDBOX_*"}})
	svc, _ := secretPolicyService(orkflows, reg, &fakeStarter{}, nil)

	_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: exfilOrkflow, Namespace: "sandbox"}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("ApplyOrkflow = %v, want InvalidArgument", err)
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) || connectErr.Message() != exfilRefusal {
		t.Fatalf("ApplyOrkflow message = %q, want %q", err, exfilRefusal)
	}
	if list, _ := orkflows.List(context.Background(), "sandbox"); len(list) != 0 {
		t.Errorf("a refused apply stored %d definitions", len(list))
	}
}

// TestSubmitRun_RefusesASecretTheNamespaceDoesNotAllow is the gate that
// cannot be bypassed: a definition applied while the namespace was
// unrestricted, the list tightened on another replica, and the submit on
// this one refused before an id is minted or a key is claimed.
func TestSubmitRun_RefusesASecretTheNamespaceDoesNotAllow(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "sandbox"})
	starter := &fakeStarter{}
	claimer := newSharedClaimer()
	a, _ := secretPolicyService(orkflows, reg, starter, claimer)
	mustApply(t, a, exfilOrkflow, "sandbox")

	tighten(t, reg, "env:SANDBOX_*")
	b, _ := secretPolicyService(orkflows, reg, starter, claimer)
	for _, key := range []string{"", "exfil-once"} {
		_, err := b.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: "exfil", Namespace: "sandbox", IdempotencyKey: key,
		}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("SubmitRun (key %q) = %v, want InvalidArgument", key, err)
		}
		for _, want := range []string{`"sandbox"`, `"grab"`, `"db"`, `"env:PROD_DB_PASSWORD"`, "env:SANDBOX_*"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("SubmitRun error %q does not name %s", err, want)
			}
		}
	}
	if started := starter.startedRuns(); len(started) != 0 {
		t.Fatalf("a refused submission started %v", started)
	}
	if proposed, _ := claimer.counts(); proposed != 0 {
		t.Fatalf("a refused submission proposed %d claims, want none", proposed)
	}
	claimer.mu.Lock()
	claims := len(claimer.claims)
	claimer.mu.Unlock()
	if claims != 0 {
		t.Fatalf("a refused submission left %d claims", claims)
	}
}

func TestSubmitRun_AllowEmptyPoolDoesNotBypassTheSecretCheck(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "sandbox"})
	starter := &fakeStarter{}
	svc, _ := secretPolicyService(orkflows, reg, starter, nil)
	mustApply(t, svc, exfilOrkflow, "sandbox")
	tighten(t, reg, "env:SANDBOX_*")

	_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "exfil", Namespace: "sandbox", AllowEmptyPool: true,
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), exfilRefusal) {
		t.Fatalf("SubmitRun(allow_empty_pool) = %v, want InvalidArgument with %q", err, exfilRefusal)
	}
	if len(starter.startedRuns()) != 0 {
		t.Fatalf("allow_empty_pool started a run past the secret policy")
	}
}

func TestSubmitRun_APermittedSecretPassesAndTheRunStarts(t *testing.T) {
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: []string{"env:SANDBOX_*"}})
	starter := &fakeStarter{}
	svc, _ := secretPolicyService(engine.NewMemoryOrkflowStore(), reg, starter, nil)
	mustApply(t, svc, permittedOrkflow, "sandbox")

	if _, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "fetch", Namespace: "sandbox",
	})); err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	started := starter.startedOrkflows()
	if len(started) != 1 {
		t.Fatalf("started %d runs, want one", len(started))
	}
	secrets := started[0].GetTasks()[0].GetSecrets()
	if len(secrets) != 1 || secrets[0].GetName() != "db" || secrets[0].GetSource() != "env:SANDBOX_DB" {
		t.Fatalf("started task secrets = %v, want the SecretRef unchanged", secrets)
	}
}

// TestSubmitRun_AnAcceptedRunIsNotRevisitedWhenTheListTightens: a list is an
// admission decision and never a retroactive one (AGENTS.md §15).
func TestSubmitRun_AnAcceptedRunIsNotRevisitedWhenTheListTightens(t *testing.T) {
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "sandbox"})
	starter := &fakeStarter{}
	svc, _ := secretPolicyService(engine.NewMemoryOrkflowStore(), reg, starter, nil)
	mustApply(t, svc, exfilOrkflow, "sandbox")

	accepted, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "exfil", Namespace: "sandbox",
	}))
	if err != nil {
		t.Fatalf("SubmitRun while unrestricted: %v", err)
	}
	tighten(t, reg, "env:SANDBOX_*")

	got, err := svc.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{
		RunId: accepted.Msg.GetRun().GetId(), Namespace: "sandbox",
	}))
	if err != nil || got.Msg.GetRun().GetId() != accepted.Msg.GetRun().GetId() {
		t.Fatalf("GetRun after the list tightened = %v, %v", got, err)
	}
	if canceled := starter.canceledRuns(); len(canceled) != 0 {
		t.Fatalf("tightening the list canceled %v", canceled)
	}
	// The next submit is the one refused.
	if _, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "exfil", Namespace: "sandbox",
	})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("the next submit = %v, want InvalidArgument", err)
	}
}

func TestUpdateNamespace_RefusesAMalformedAllowedSecret(t *testing.T) {
	reg := newFakeRegistry()
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()
	created, err := svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: []string{"env:SANDBOX_*"}},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	next := proto.Clone(created.Msg.GetNamespace()).(*orkstackv1.Namespace)
	next.AllowedSecrets = []string{"file:/a/../b"}
	_, err = svc.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: next}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), `"file:/a/../b"`) {
		t.Fatalf("UpdateNamespace(file:/a/../b) = %v, want InvalidArgument naming the entry", err)
	}
	got, err := svc.GetNamespace(ctx, connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: "sandbox"}))
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	if ns := got.Msg.GetNamespace(); ns.GetEtag() != created.Msg.GetNamespace().GetEtag() ||
		strings.Join(ns.GetAllowedSecrets(), ",") != "env:SANDBOX_*" {
		t.Fatalf("a refused update changed the document: %v", ns)
	}
}

func TestCreateNamespace_IdempotentRepeatComparesAllowedSecretsAsASet(t *testing.T) {
	reg := newFakeRegistry()
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()
	spec := &orkstackv1.Namespace{
		Name:           "sandbox",
		AllowedSecrets: []string{"env:SANDBOX_*", "file:/etc/orkstack/secrets/sandbox/*"},
	}
	if _, err := svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: spec})); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	reordered := proto.Clone(spec).(*orkstackv1.Namespace)
	reordered.AllowedSecrets = []string{"file:/etc/orkstack/secrets/sandbox/*", "env:SANDBOX_*"}
	repeat, err := svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: reordered}))
	if err != nil || repeat.Msg.GetCreated() {
		t.Fatalf("a reordered repeat = %v, %v; want created=false", repeat, err)
	}
	different := proto.Clone(spec).(*orkstackv1.Namespace)
	different.AllowedSecrets = []string{"env:SANDBOX_*"}
	_, err = svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: different}))
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("a different list = %v, want AlreadyExists", err)
	}
	want := "allowed_secrets are [env:SANDBOX_* file:/etc/orkstack/secrets/sandbox/*], not [env:SANDBOX_*]"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("AlreadyExists = %q, want it to say %q", err, want)
	}
}
