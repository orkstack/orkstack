package api_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// What a submit decides about the grants a run's tasks name. The build's gate
// still refuses `credentials:` at apply, so the definitions are put straight
// into the Orkflow store.

// checkedType is a provider type as a submit meets one: it checks references
// and is never logged in to. No built-in type exists until P16.
type checkedType struct{ provider.Type }

func (checkedType) Name() string { return "checked" }
func (checkedType) New(name string, c *provider.Config) (provider.Provider, error) {
	audience, _ := c.String("audience")
	return checked{name: name, audience: audience}, nil
}

type checked struct {
	provider.Provider
	name, audience string
}

func (c checked) Name() string      { return c.name }
func (checked) Type() string        { return "checked" }
func (c checked) Audience() string  { return c.audience }
func (checked) LoginScheme() string { return "sts" }
func (checked) Parse(ref provider.Ref) (provider.Parsed, error) {
	if strings.HasPrefix(ref.Locator, "nope/") {
		return provider.Parsed{}, errors.New("not a path of this type")
	}
	return provider.Parsed{Ref: ref}, nil
}

func checkedProviders(t *testing.T) provider.Set {
	t.Helper()
	set, err := provider.ParseWith([]byte("providers:\n  aws:\n    type: checked\n    audience: sts.amazonaws.com\n"),
		map[string]provider.Type{"checked": checkedType{}})
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// credentialOrkflow is default's deploy, whose task apply logs in with grant
// and holds no identity of its own.
func credentialOrkflow(grant string) *orkstackv1.Orkflow {
	wf := identityOrkflow()
	wf.Tasks[0].Identities = nil
	wf.Tasks[0].Credentials = []*orkstackv1.CredentialRef{{Name: "cloud", Grant: grant}}
	return wf
}

// TestSubmitRun_AdmitsTheGrantsItsTasksUse: a task that holds a credential
// and no identity is admitted with its slots, its grant's name and no set,
// and the run with what the grant selects, of the grants it uses alone.
func TestSubmitRun_AdmitsTheGrantsItsTasksUse(t *testing.T) {
	ctx := context.Background()
	w := newAdmissionWorld(t, admissionOptions{providers: checkedProviders(t), definitions: []*orkstackv1.Orkflow{credentialOrkflow("deployer")},
		grants: map[string]artifacts.Grant{
			"deployer": {Provider: "aws", Principal: "role/deploy", Orkflows: []string{"deploy"}, Origins: []string{"submit"}, CurrentOnly: true},
			"other":    {Provider: "aws", Principal: "role/other", Orkflows: []string{"*"}},
		}})
	run, err := w.submit(t, "deploy", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	plan := w.starter.startedPlans()[0]
	if plan.Identity == nil {
		t.Fatal("a run that holds only a credential has no admission")
	}
	got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, run.GetId(), plan.Identity.AdmissionSHA256)
	if err != nil || got.Answer != artifacts.RecordFound {
		t.Fatalf("the admission reads %+v, %v", got, err)
	}
	a := got.Value
	want := map[string]artifacts.GrantSelection{"deployer": {Provider: "aws", Principal: "role/deploy", Audience: "sts.amazonaws.com"}}
	if !reflect.DeepEqual(a.Grants, want) || len(a.Sets) != 0 {
		t.Errorf("admitted grants %+v and sets %+v; want %+v and no set", a.Grants, a.Sets, want)
	}
	if row := a.Tasks["apply"]; row.Set != 0 || row.IdentitySlots == 0 || row.AttemptDeadline == 0 || !reflect.DeepEqual(row.Grants, []string{"deployer"}) {
		t.Errorf("apply is admitted as %+v; want its grant and its slots, and no set", row)
	}
	withGrant, _, _ := task.IdentitySpecDigest(1, task.IdentitySpecOfTask(credentialOrkflow("deployer").Tasks[0], []*orkstackv1.IdentityGrant{
		{Name: "deployer", Provider: "aws", Principal: "role/deploy", Audience: "sts.amazonaws.com"}}))
	if got := a.Tasks["apply"].SpecSHA256; got != withGrant {
		t.Errorf("apply's spec digest is %s; want %s, which binds what its grant selects", got, withGrant)
	}
	if row := a.Tasks["report"]; row.Grants != nil || row.IdentitySlots != 0 {
		t.Errorf("report is admitted as %+v; want no grant and no slots", row)
	}
}

// TestSubmitRun_RefusesAGrantBeforeAnythingIsWritten: each refusal names its
// reason, is FAILED_PRECONDITION, and leaves no record and no run behind.
func TestSubmitRun_RefusesAGrantBeforeAnythingIsWritten(t *testing.T) {
	for _, tc := range []struct {
		name   string
		grant  artifacts.Grant
		path   string
		reason string
	}{
		{"not listed", artifacts.Grant{Provider: "aws", Principal: "p", Orkflows: []string{"other"}}, "", provider.ReasonGrantNotPermitted},
		{"another start", artifacts.Grant{Provider: "aws", Principal: "p", Orkflows: []string{"*"}, Origins: []string{"trigger"}}, "", provider.ReasonGrantNotPermitted},
		{"no provider", artifacts.Grant{Provider: "gcp", Principal: "p", Orkflows: []string{"*"}}, "", provider.ReasonProviderUnknown},
		{"a path its provider refuses", artifacts.Grant{Provider: "aws", Principal: "p", Orkflows: []string{"*"}}, "nope/x", provider.ReasonProviderRequestInvalid},
	} {
		wf := credentialOrkflow("g")
		wf.Tasks[0].Credentials[0].Path = tc.path
		w := newAdmissionWorld(t, admissionOptions{providers: checkedProviders(t), definitions: []*orkstackv1.Orkflow{wf},
			grants: map[string]artifacts.Grant{"g": tc.grant}})
		_, err := w.submit(t, "deploy", "")
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), tc.reason) {
			t.Errorf("%s: %v; want FAILED_PRECONDITION %s", tc.name, err, tc.reason)
		}
		if files := w.admissionFiles(t); len(files) != 0 || len(w.starter.startedRuns()) != 0 {
			t.Errorf("%s: left %v and started %v", tc.name, files, w.starter.startedRuns())
		}
	}
	w := newAdmissionWorld(t, admissionOptions{providers: checkedProviders(t), definitions: []*orkstackv1.Orkflow{credentialOrkflow("nobody")}})
	if _, err := w.submit(t, "deploy", ""); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), provider.ReasonGrantNotDefined) {
		t.Errorf("a grant that is not there: %v", err)
	}
}

// TestSubmitRun_EveryOrkerOfThePoolDeclaresTheProvider: a run whose task, or
// whose composed child's task, logs in to a provider that an Orker of its pool
// does not declare is refused POOL_NO_PROVIDER, and leaves nothing behind.
func TestSubmitRun_EveryOrkerOfThePoolDeclaresTheProvider(t *testing.T) {
	composing := func() []*orkstackv1.Orkflow {
		parent, child := composingOrkflows()
		for _, tk := range append(parent.GetTasks(), child.GetTasks()...) {
			tk.Identities = nil
		}
		child.Tasks[0].Credentials = []*orkstackv1.CredentialRef{{Name: "cloud", Grant: "deployer"}}
		return []*orkstackv1.Orkflow{child, parent}
	}
	for _, tc := range []struct {
		name, orkflow string
		definitions   []*orkstackv1.Orkflow
		declared      []*orkstackv1.ProviderInfo
		refused       bool
	}{
		{"declared", "deploy", []*orkstackv1.Orkflow{credentialOrkflow("deployer")}, []*orkstackv1.ProviderInfo{{Name: "aws", Type: "checked"}}, false},
		{"not declared", "deploy", []*orkstackv1.Orkflow{credentialOrkflow("deployer")}, nil, true},
		{"another type", "deploy", []*orkstackv1.Orkflow{credentialOrkflow("deployer")}, []*orkstackv1.ProviderInfo{{Name: "aws", Type: "vault"}}, true},
		{"a child's, declared", "release", composing(), []*orkstackv1.ProviderInfo{{Name: "aws", Type: "checked"}}, false},
		{"a child's, not declared", "release", composing(), nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orker := currentOrker("main-1", "main", 0)
			orker.IdentitySupported, orker.Providers = true, tc.declared
			w := newAdmissionWorld(t, admissionOptions{providers: checkedProviders(t), definitions: tc.definitions,
				fleet: fleetOf(orker), pools: livePools{poolQueue(t, "main"): 1},
				grants: map[string]artifacts.Grant{"deployer": {Provider: "aws", Principal: "role/deploy", Orkflows: []string{"*"}}}})
			_, err := w.submit(t, tc.orkflow, "")
			if !tc.refused {
				if err != nil {
					t.Fatalf("submit: %v", err)
				}
				return
			}
			if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), engine.ReasonPoolNoProvider) ||
				!strings.Contains(err.Error(), `"main-1"`) {
				t.Errorf("%v; want FAILED_PRECONDITION POOL_NO_PROVIDER naming the Orker", err)
			}
			if files := w.admissionFiles(t); len(files) != 0 || len(w.starter.startedRuns()) != 0 {
				t.Errorf("left %v and started %v", files, w.starter.startedRuns())
			}
		})
	}
}

// TestSubmitRun_ASharedChildUsesAGrantOfItsOwnersName: a grant is the
// running namespace's, and lists a shared Orkflow by its owner. Listing this
// namespace's own Orkflow of that name, or every one of its own, admits
// nothing of another's. The same name stands in both namespaces.
func TestSubmitRun_ASharedChildUsesAGrantOfItsOwnersName(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		lists  string
		origin string
		ok     bool
	}{
		{"platform/child", "submit", true},
		{"platform/child", "parent-run", false},
		{"child", "submit", false},
		{"*", "submit", false},
		{"default/child", "submit", false},
	} {
		parent, own := composingOrkflows()
		own.Tasks[0].Identities = nil
		_, shared := composingOrkflows()
		shared.Namespace, shared.SharedWith = "platform", []*orkstackv1.OrkflowShare{{Namespace: "*"}}
		shared.Tasks[0].Identities = nil
		shared.Tasks[0].Credentials = []*orkstackv1.CredentialRef{{Name: "cloud", Grant: "deployer"}}
		parent.Tasks[1].Uses, parent.Tasks[1].Orkflow.Namespace = "orkflow:platform/child@1", "platform"
		w := newAdmissionWorld(t, admissionOptions{providers: checkedProviders(t), definitions: []*orkstackv1.Orkflow{own, shared, parent},
			grants: map[string]artifacts.Grant{"deployer": {Provider: "aws", Principal: "role/deploy", Orkflows: []string{tc.lists}, Origins: []string{tc.origin}}}})
		_, err := w.submit(t, "release", "")
		if !tc.ok {
			if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), provider.ReasonGrantNotPermitted) {
				t.Errorf("a grant listing %q for %s: %v; want GRANT_NOT_PERMITTED", tc.lists, tc.origin, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("a grant listing %q for %s: %v", tc.lists, tc.origin, err)
			continue
		}
		plan := w.starter.startedPlans()[0]
		entry := plan.Children["ship"]
		got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, entry.RunID, entry.Plan.Identity.AdmissionSHA256)
		if err != nil || got.Answer != artifacts.RecordFound || got.Value.OrkflowNamespace != "platform" || got.Value.Grants["deployer"].Principal != "role/deploy" {
			t.Errorf("the shared child's admission reads %+v, %v", got, err)
		}
		root, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, w.starter.startedRuns()[0], plan.Identity.AdmissionSHA256)
		if err != nil || root.Value.Grants != nil {
			t.Errorf("the parent's admission has grants %v, %v; only its child uses one", root.Value.Grants, err)
		}
	}
}

// TestCreateOrkflow_JudgesItsGrants: what can be known with no run is judged
// when an Orkflow is created, on the field that is wrong; its start and its
// current version are not. A shared child is judged there too, under its
// owner's name.
func TestCreateOrkflow_JudgesItsGrants(t *testing.T) {
	ctx := context.Background()
	policy := artifacts.NamespacePolicy{Grants: map[string]artifacts.Grant{
		"deployer": {Provider: "aws", Principal: "role/deploy", Orkflows: []string{"deploy"}, Origins: []string{"trigger"}, CurrentOnly: true},
	}}
	s := api.NewGrantChecker(checkedProviders(t), policy)
	ns := &orkstackv1.Namespace{Name: "payments"}
	if err := s.Create(ctx, ns, credentialOrkflow("deployer")); err != nil {
		t.Errorf("a grant that serves triggers, at create: %v", err)
	}
	err := s.Create(ctx, ns, credentialOrkflow("nobody"))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), provider.ReasonGrantNotDefined) ||
		s.Field(err) != "tasks[0].credentials[0].grant" {
		t.Errorf("a grant that is not there: %v on %q", err, s.Field(err))
	}
	if err := s.Child(ctx, ns, credentialOrkflow("deployer"), "payments"); err != nil {
		t.Errorf("a child of the namespace's own: %v", err)
	}
	if err := s.Child(ctx, ns, credentialOrkflow("deployer"), "platform"); err == nil || !strings.Contains(err.Error(), "platform/deploy") {
		t.Errorf("another namespace's deploy: %v", err)
	}
	if err := s.Child(ctx, ns, identityOrkflow(), "platform"); err != nil || s.Reads() != 4 {
		t.Errorf("a child that names no grant: %v after %d policy reads, want 4", err, s.Reads())
	}
}

// TestSubmit_AnOriginNobodyNamesHoldsNoIdentity: a run that would hold an
// identity is started by a person, or by an object whose binder and consent
// are judged. A kind of start that is neither is refused by name before
// anything is written, whatever kinds a later release adds; a run that holds
// none is not asked.
func TestSubmit_AnOriginNobodyNamesHoldsNoIdentity(t *testing.T) {
	ctx := context.Background()
	const unlisted = orkstackv1.RunOriginKind(9999)
	msg := &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}
	for kind, named := range map[orkstackv1.RunOriginKind]string{
		unlisted: "9999",
		orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED: "unspecified",
		orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN:  "parent-run",
	} {
		w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{identityOrkflow()}})
		err := api.SubmitFrom(ctx, w.svc, msg, &orkstackv1.RunOrigin{Kind: kind, Id: "x"})
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "IDENTITY_STARTER_NOT_RECORDED") || !strings.Contains(err.Error(), named) {
			t.Errorf("origin %v: %v; want IDENTITY_STARTER_NOT_RECORDED naming %q", kind, err, named)
		}
		if files := w.admissionFiles(t); len(files) != 0 || len(w.starter.startedRuns()) != 0 {
			t.Errorf("origin %v left %v and started %v", kind, files, w.starter.startedRuns())
		}
		if err := api.ReadStarterKind(ctx, w.svc, kind); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("the starter of origin %v reads %v; want a refusal", kind, err)
		}
	}
	for _, kind := range []orkstackv1.RunOriginKind{orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT, orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN} {
		w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{identityOrkflow()}})
		if err := api.SubmitFrom(ctx, w.svc, msg, &orkstackv1.RunOrigin{Kind: kind}); err != nil {
			t.Errorf("origin %v: %v", kind, err)
		}
	}
	plain := identityOrkflow()
	plain.Tasks[0].Identities = nil
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{plain}})
	if err := api.SubmitFrom(ctx, w.svc, msg, &orkstackv1.RunOrigin{Kind: unlisted}); err != nil {
		t.Errorf("a run that holds no identity: %v", err)
	}
}

// TestSubmitRun_ARunAgainOfAnIdentityRunIsAdmitted: running an identity run
// again, through SubmitRun with the run it names, is a person's start: it is
// admitted under a record of its own, whose origin is run-again.
func TestSubmitRun_ARunAgainOfAnIdentityRunIsAdmitted(t *testing.T) {
	ctx := context.Background()
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{identityOrkflow()}})
	first, err := w.submit(t, "deploy", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	resp, err := w.svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "deploy", SourceRunId: first.GetId()}))
	if err != nil {
		t.Fatalf("run again: %v", err)
	}
	again := resp.Msg.GetRun().GetId()
	plans := w.starter.startedPlans()
	if len(plans) != 2 || again == first.GetId() || plans[1].Identity == nil {
		t.Fatalf("run %s again started %d runs, as %s, identity %+v", first.GetId(), len(plans), again, plans[len(plans)-1].Identity)
	}
	got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, again, plans[1].Identity.AdmissionSHA256)
	if err != nil || got.Answer != artifacts.RecordFound || got.Value.Origin.Kind != "run-again" || got.Value.Origin.ID != first.GetId() ||
		got.Value.SetOf("apply") == nil || got.Value.Origin.Binder != nil {
		t.Fatalf("the admission of the run again reads %+v, %v", got, err)
	}
	if plans[1].Identity.AdmissionSHA256 == plans[0].Identity.AdmissionSHA256 {
		t.Fatal("the run again shares the first run's admission")
	}
}
