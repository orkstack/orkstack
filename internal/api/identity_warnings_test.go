package api

import (
	"context"
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// listedNamespaces answers List with fixed documents, or an error.
type listedNamespaces struct {
	engine.NamespaceRegistry
	all []*orkstackv1.Namespace
	err error
}

func (l listedNamespaces) List(context.Context) ([]*orkstackv1.Namespace, error) { return l.all, l.err }

// TestIdentityIssues: a cached identity task, a pin of mixed images holding
// one, and an identity task on a pool two namespaces may use each warn,
// naming the pool and how many namespaces, never which; a task holding none
// warns of nothing; a registry that cannot be read warns that the check was
// not made.
func TestIdentityIssues(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "payments", AllowedPools: []string{"prod"}}
	img := func(ref string) *orkstackv1.ImageRef { return &orkstackv1.ImageRef{Reference: ref} }
	id := []*orkstackv1.IdentityRef{{Name: "t", Audience: "sts"}}
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{
		{Id: "cached", Pool: "prod", Image: img("a"), Identities: id, Cache: &orkstackv1.CacheSpec{}},
		{Id: "first", Pool: "prod", Pin: "p", Image: img("a"), Identities: id},
		{Id: "second", Pool: "prod", Pin: "p", Image: img("b")},
		{Id: "plain", Pool: "gpu", Image: img("c"), Cache: &orkstackv1.CacheSpec{}},
	}}
	two := listedNamespaces{all: []*orkstackv1.Namespace{ns, {Name: "other"}, {Name: "gpu-only", AllowedPools: []string{"gpu"}}}}
	issues := (&Service{namespaces: two}).identityIssues(context.Background(), ns, wf)
	got := map[string]string{}
	for _, i := range issues {
		if i.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING {
			t.Errorf("%s is not a warning", i.GetField())
		}
		got[i.GetField()] = i.GetMessage()
	}
	for field, want := range map[string]string{
		"tasks[0].cache": "memo hit runs no container",
		"tasks[1].pin":   "first, second",
		"tasks[0].pool":  `pool "prod", which 2 namespaces`,
	} {
		if !strings.Contains(got[field], want) {
			t.Errorf("%s = %q, want it to say %q", field, got[field], want)
		}
	}
	if len(issues) != 3 || strings.Contains(got["tasks[0].pool"], "other") {
		t.Errorf("issues = %v", got)
	}

	alone := listedNamespaces{all: []*orkstackv1.Namespace{ns, {Name: "gpu-only", AllowedPools: []string{"gpu"}}}}
	plain := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{wf.Tasks[3]}}
	if issues := (&Service{namespaces: two}).identityIssues(context.Background(), ns, plain); len(issues) != 0 {
		t.Errorf("a definition holding no identity warns: %v", issues)
	}
	if issues := (&Service{namespaces: alone}).identityIssues(context.Background(), ns, &orkstackv1.Orkflow{Tasks: wf.Tasks[1:2]}); len(issues) != 0 {
		t.Errorf("a pool one namespace may use warns: %v", issues)
	}
	down := listedNamespaces{err: errors.New("bucket unreachable")}
	issues = (&Service{namespaces: down}).identityIssues(context.Background(), ns, &orkstackv1.Orkflow{Tasks: wf.Tasks[1:2]})
	if len(issues) != 1 || !strings.Contains(issues[0].GetMessage(), "could not be checked") {
		t.Errorf("an unreadable registry: %v", issues)
	}
}

// TestIdentityIssues_Barriers: create warns of a task that holds an identity
// and does not follow an approval upstream, and of one right behind a
// sleep, naming the tasks and the join.
func TestIdentityIssues_Barriers(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "team"}
	tok := []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts"}}
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", Tasks: []*orkstackv1.Task{
		{Id: "approve", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}},
		{Id: "deploy", Needs: []string{"approve"}, Join: orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE, Identities: tok},
		{Id: "hold", Needs: []string{"approve"}, Sleep: &orkstackv1.SleepSpec{}},
		{Id: "push", Needs: []string{"hold"}, Identities: tok},
	}}
	var messages []string
	for _, issue := range (&Service{}).identityIssues(context.Background(), ns, wf) {
		messages = append(messages, issue.GetField()+": "+issue.GetMessage())
	}
	all := strings.Join(messages, "\n")
	for _, want := range []string{`tasks[1].needs: task "deploy" holds an identity and does not follow approve`, "under its join all_done",
		`tasks[3].needs: task "push" holds an identity directly behind sleeping task hold`} {
		if !strings.Contains(all, want) {
			t.Errorf("warnings %q; want %q", all, want)
		}
	}
	if strings.Contains(all, `"push" holds an identity and does not follow`) {
		t.Errorf("push follows approve through its sleep: %q", all)
	}
}
