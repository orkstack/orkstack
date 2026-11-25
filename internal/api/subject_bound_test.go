package api_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// What a create and a submit refuse of a subject no relying party would take:
// docs/TASK_IDENTITY.md, "Subjects".

func longName(n int) string { return "a" + strings.Repeat("b", n-1) }

// TestSubjectBound_Create: an Orkflow whose task's subject passes 255 bytes
// is refused when it is created, on the task, whether or not its namespace
// allows what the task asks for; so is one that composes such an Orkflow, of
// this namespace or of another, whose subject there names its owner. A task
// that holds nothing may be named as long as any.
func TestSubjectBound_Create(t *testing.T) {
	ctx := context.Background()
	s := api.NewGrantChecker(checkedProviders(t), artifacts.NamespacePolicy{Audiences: []string{"sts.amazonaws.com"},
		Grants: map[string]artifacts.Grant{"deployer": {Provider: "aws", Principal: "role/x", Orkflows: []string{"*", "platform/" + longName(100)}}}})
	ns := &orkstackv1.Namespace{Name: "payments"}
	of := func(orkflow, task int, holds func(*orkstackv1.Task)) *orkstackv1.Orkflow {
		tk := &orkstackv1.Task{Id: longName(task)}
		holds(tk)
		return &orkstackv1.Orkflow{Name: longName(orkflow), Version: "1", Tasks: []*orkstackv1.Task{{Id: "first"}, tk}}
	}
	ids := func(t *orkstackv1.Task) {
		t.Identities = []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts.amazonaws.com"}}
	}
	cred := func(t *orkstackv1.Task) { t.Credentials = []*orkstackv1.CredentialRef{{Name: "c", Grant: "deployer"}} }
	none := func(*orkstackv1.Task) {}

	room := 255 - len("ork:namespace:payments:orkflow::task:")
	for _, tc := range []struct {
		name string
		wf   *orkstackv1.Orkflow
		ok   bool
	}{
		{"an identity that fits", of(100, room-100, ids), true},
		{"an identity one byte over", of(100, room-99, ids), false},
		{"a credential that fits", of(100, room-100-len(":grant:deployer"), cred), true},
		{"a credential one byte over", of(100, room-99-len(":grant:deployer"), cred), false},
		{"a task that holds nothing", of(200, 200, none), true},
	} {
		err := s.Create(ctx, ns, tc.wf)
		if tc.ok != (err == nil) {
			t.Errorf("%s: %v", tc.name, err)
		}
		if !tc.ok && (connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), identity.ReasonSubjectTooLong) || s.Field(err) != "tasks[1].id") {
			t.Errorf("%s: %v on %q; want INVALID_ARGUMENT IDENTITY_SUBJECT_TOO_LONG on tasks[1].id", tc.name, err, s.Field(err))
		}
	}
	// Asked before the namespace's policy: no policy makes it fit.
	unread := api.NewGrantChecker(checkedProviders(t), artifacts.NamespacePolicy{})
	if err := unread.Create(ctx, ns, of(100, room-99, ids)); err == nil || !strings.Contains(err.Error(), identity.ReasonSubjectTooLong) || unread.Reads() != 0 {
		t.Errorf("a namespace that allows nothing: %v after %d policy reads", err, unread.Reads())
	}
	for _, tc := range []struct {
		name, owner string
		wf          *orkstackv1.Orkflow
		ok          bool
	}{
		{"a child that fits", "payments", of(100, room-100, ids), true},
		{"a child one byte over", "payments", of(100, room-99, ids), false},
		{"a shared child that fits", "platform", of(100, room-100-len("platform/"), ids), true},
		{"a shared child over for its owner's name", "platform", of(100, room-100, ids), false},
		{"a shared child's credential that fits", "platform", of(100, room-100-len("platform/:grant:deployer"), cred), true},
		{"a shared child's credential over", "platform", of(100, room-99-len("platform/:grant:deployer"), cred), false},
	} {
		err := s.Child(ctx, ns, tc.wf, tc.owner)
		if tc.ok != (err == nil) || (!tc.ok && !strings.Contains(err.Error(), identity.ReasonSubjectTooLong)) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
}

// TestSubjectBound_Submit: a stored Orkflow whose subject no longer fits is
// refused at submit, before anything is written: one stored before the rule,
// or before the deployment had a trust domain, whose SPIFFE form is the
// longer; and a tree whose child's does not, a shared child's under its
// owner's name.
func TestSubjectBound_Submit(t *testing.T) {
	const domain = "ork-id.a-long-trust-domain.example.internal"
	colon := 255 - len("ork:namespace:default:orkflow::task:")
	spiffe := 255 - len("spiffe://"+domain+"/ork/ns/default/orkflow//task/")
	sharedColon := 255 - len("ork:namespace:default:orkflow:platform/:task:")
	deploy := func(task int) []*orkstackv1.Orkflow {
		wf := undigestedIdentityOrkflow()
		wf.Name = longName(100)
		wf.Tasks = []*orkstackv1.Task{wf.Tasks[0]}
		wf.Tasks[0].Id = longName(task - 100)
		return []*orkstackv1.Orkflow{digested(wf)}
	}
	// A parent whose child's task is as long, the child of owner.
	tree := func(owner string, task int) []*orkstackv1.Orkflow {
		parent, child := composingOrkflows()
		child.Namespace, child.Name = owner, longName(100)
		child.Tasks[0].Id = longName(task - 100)
		if owner != "default" {
			child.SharedWith = []*orkstackv1.OrkflowShare{{Namespace: "*"}}
		}
		digested(child)
		parent.Name = "release"
		parent.Tasks[1].Uses = "orkflow:" + child.Name + "@1"
		parent.Tasks[1].Orkflow = &orkstackv1.OrkflowRef{Name: child.Name, Version: "1", DefinitionDigest: child.GetDefinitionDigest()}
		if owner != "default" {
			parent.Tasks[1].Uses, parent.Tasks[1].Orkflow.Namespace = "orkflow:"+owner+"/"+child.Name+"@1", owner
		}
		return []*orkstackv1.Orkflow{child, digested(parent)}
	}
	for _, tc := range []struct {
		name, trustDomain, submit string
		definitions               []*orkstackv1.Orkflow
		ok                        bool
	}{
		{"fits", "", longName(100), deploy(colon), true},
		{"one byte over", "", longName(100), deploy(colon + 1), false},
		{"fits with a trust domain", domain, longName(100), deploy(spiffe), true},
		{"over in the SPIFFE form alone", domain, longName(100), deploy(spiffe + 1), false},
		{"a child that fits", "", "release", tree("default", colon), true},
		{"a child one byte over", "", "release", tree("default", colon+1), false},
		{"a shared child that fits", "", "release", tree("platform", sharedColon), true},
		{"a shared child over for its owner's name", "", "release", tree("platform", sharedColon+1), false},
	} {
		w := newAdmissionWorld(t, admissionOptions{definitions: tc.definitions, trustDomain: tc.trustDomain})
		_, err := w.submit(t, tc.submit, "")
		if tc.ok {
			if err != nil {
				t.Errorf("%s: %v", tc.name, err)
			}
			continue
		}
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), identity.ReasonSubjectTooLong) {
			t.Errorf("%s: %v; want FAILED_PRECONDITION IDENTITY_SUBJECT_TOO_LONG", tc.name, err)
		}
		if files := w.admissionFiles(t); len(files) != 0 || len(w.starter.startedRuns()) != 0 {
			t.Errorf("%s: left %v and started %v", tc.name, files, w.starter.startedRuns())
		}
	}
}
