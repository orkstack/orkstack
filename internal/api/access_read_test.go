package api

import (
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
)

func TestExplainAccess(t *testing.T) {
	_, h := accessWriter(t)
	ctx := asAlice(authn.ModeEnforce)
	explain := func(principal, perm, ns, orkflow string) (*orkstackv1.ExplainAccessResponse, error) {
		resp, err := h.ExplainAccess(ctx, connect.NewRequest(&orkstackv1.ExplainAccessRequest{
			PrincipalId: principal, Permission: perm, Namespace: ns, Orkflow: orkflow}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	allowed, err := explain("user:corp:nina", "orkflow.delete", "payments", "deploy")
	if err != nil {
		t.Fatal(err)
	}
	if allowed.GetDecision() != orkstackv1.AccessDecision_ACCESS_DECISION_ALLOWED || allowed.GetGrantedBy().GetBinding() != "payments-admins" ||
		allowed.GetGrantedBy().GetMatch() != orkstackv1.BindingMatch_BINDING_MATCH_GRANTED || allowed.GetReason() != "" || allowed.GetAccessRevision() == 0 {
		t.Fatalf("explain nina = %v; want allowed by payments-admins", allowed)
	}
	denied, err := explain("user:corp:bob", "orkflow.delete", "payments", "")
	if err != nil {
		t.Fatal(err)
	}
	if denied.GetDecision() != orkstackv1.AccessDecision_ACCESS_DECISION_DENIED || denied.GetReason() != "NO_BINDING" || denied.GetGrantedBy() != nil || denied.GetExplanation() == "" {
		t.Fatalf("explain bob = %v; want denied, NO_BINDING", denied)
	}
	var sawNina bool
	for _, c := range denied.GetConsidered() {
		if c.GetBinding() == "payments-admins" {
			sawNina = c.GetMatch() == orkstackv1.BindingMatch_BINDING_MATCH_SUBJECT_DIFFERS
		}
	}
	if !sawNina {
		t.Fatalf("considered = %v; want payments-admins as a subject that differs", denied.GetConsidered())
	}
	for name, req := range map[string][4]string{
		"an unknown permission":                         {"user:corp:nina", "orkflow.teleport", "payments", ""},
		"a malformed principal":                         {"nina", "orkflow.delete", "payments", ""},
		"an Orkflow with no namespace is the default's": {"user:corp:nina", "orkflow.delete", "", "deploy"},
	} {
		_, err := explain(req[0], req[1], req[2], req[3])
		if name == "an Orkflow with no namespace is the default's" {
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s = %v; want invalid_argument", name, err)
		}
	}
}

func TestAccessRevisions_HistoryFollowsTheChainFromTheHead(t *testing.T) {
	_, h := accessWriter(t)
	ctx := asAlice(authn.ModeEnforce)
	for _, name := range []string{"one", "two", "three"} {
		etag, _ := etagOf(t, h)
		if _, err := h.CreateRole(ctx, connect.NewRequest(&orkstackv1.CreateRoleRequest{Etag: etag,
			Role: &orkstackv1.Role{Name: name, Permissions: []string{"run.read"}}})); err != nil {
			t.Fatal(err)
		}
	}
	_, head := etagOf(t, h)
	var got []int64
	token := ""
	for page := 0; page < 5; page++ {
		resp, err := h.ListAccessRevisions(ctx, connect.NewRequest(&orkstackv1.ListAccessRevisionsRequest{PageSize: 2, PageToken: token}))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range resp.Msg.GetRevisions() {
			got = append(got, r.GetRevision())
			if r.GetSha256() == "" || r.GetWrittenAt() == nil {
				t.Fatalf("revision %v lacks its digest or instant", r)
			}
		}
		if token = resp.Msg.GetNextPageToken(); token == "" {
			break
		}
	}
	if len(got) != int(head) || got[0] != head || got[len(got)-1] != 1 {
		t.Fatalf("history = %v; want %d revisions, newest first, from %d down to 1", got, head, head)
	}
	one, err := h.GetAccessRevision(ctx, connect.NewRequest(&orkstackv1.GetAccessRevisionRequest{Revision: head - 1}))
	if err != nil {
		t.Fatal(err)
	}
	if one.Msg.GetRevision().GetRevision() != head-1 || len(one.Msg.GetDocument().GetRoles()) != 2 || one.Msg.GetRevision().GetChange() != "create role two" {
		t.Fatalf("revision %d = %v", head-1, one.Msg)
	}
	if _, err := h.GetAccessRevision(ctx, connect.NewRequest(&orkstackv1.GetAccessRevisionRequest{Revision: head + 5})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("a revision past the head = %v; want not_found", err)
	}
	if _, err := h.ListAccessRevisions(ctx, connect.NewRequest(&orkstackv1.ListAccessRevisionsRequest{PageToken: "garbage"})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a malformed page token = %v; want invalid_argument", err)
	}
}
