package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// writeAround puts a head straight into the writer's bucket, as a writer
// around the API would, and has the replica read it.
func writeAround(t *testing.T, s *Service, cas *artifacts.MemoryCAS, revision int64) artifacts.AccessRevisionMeta {
	t.Helper()
	ctx := context.Background()
	c := s.access.Policies
	head, err := c.store.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, err := artifacts.EncodeAccessObject(head.Document, artifacts.AccessRevisionMeta{Revision: revision, WrittenAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	cas.PutRaw(artifacts.AccessDocumentKey, body)
	c.Refresh(ctx)
	if !c.State().WrittenAroundAPI {
		t.Fatal("the head put into the bucket raised no banner")
	}
	_, meta, err := artifacts.DecodeAccessObject(body)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func acknowledge(h accessHandler, ctx context.Context, revision int64, sha string) error {
	_, err := h.AcknowledgeAccessRevision(ctx, connect.NewRequest(&orkstackv1.AcknowledgeAccessRevisionRequest{Revision: revision, Sha256: sha}))
	return err
}

// TestAcknowledgeAccessRevision_RecordsAndClears: an admin's
// acknowledgement of the revision found is one record naming who and
// when, clears the banner, and reports the revision acknowledged.
func TestAcknowledgeAccessRevision_RecordsAndClears(t *testing.T) {
	s, h, cas := accessWriterOn(t)
	around := writeAround(t, s, cas, 40)
	ctx := asAlice(authn.ModeEnforce)
	if err := acknowledge(h, ctx, around.Revision, around.SHA256); err != nil {
		t.Fatal(err)
	}
	body, ok := cas.Raw(artifacts.AccessAckKey(40, around.SHA256))
	if !ok || !strings.Contains(string(body), `"acknowledged_by":"user:corp:alice"`) || !strings.Contains(string(body), `"acknowledged_by_verified":true`) {
		t.Fatalf("record = %s, %t; want one naming alice, verified", body, ok)
	}
	resp, err := h.GetAccess(ctx, connect.NewRequest(&orkstackv1.GetAccessRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetDocument().GetWrittenAroundApi() || resp.Msg.GetStatus().GetAcknowledgedRevision() != 40 {
		t.Fatalf("after acknowledging: written_around_api=%t acknowledged_revision=%d; want false, 40",
			resp.Msg.GetDocument().GetWrittenAroundApi(), resp.Msg.GetStatus().GetAcknowledgedRevision())
	}
	// Twice is no error, and leaves the first record.
	if err := acknowledge(h, asNina(), around.Revision, around.SHA256); err != nil {
		t.Fatal(err)
	}
	if again, _ := cas.Raw(artifacts.AccessAckKey(40, around.SHA256)); string(again) != string(body) {
		t.Fatal("a second acknowledgement replaced the first record")
	}
}

func TestAcknowledgeAccessRevision_Refuses(t *testing.T) {
	s, h, cas := accessWriterOn(t)
	around := writeAround(t, s, cas, 40)
	ctx := asAlice(authn.ModeEnforce)
	for _, c := range []struct {
		name string
		rev  int64
		sha  string
		code connect.Code
	}{
		{"no revision", 0, around.SHA256, connect.CodeInvalidArgument},
		{"not a digest", 40, "abc", connect.CodeInvalidArgument},
		{"another digest", 40, strings.Repeat("0", 64), connect.CodeFailedPrecondition},
		{"another revision", 41, around.SHA256, connect.CodeFailedPrecondition},
	} {
		if err := acknowledge(h, ctx, c.rev, c.sha); connect.CodeOf(err) != c.code {
			t.Errorf("%s: %v; want %v", c.name, err, c.code)
		}
	}
	if !s.access.Policies.State().WrittenAroundAPI {
		t.Fatal("a refused acknowledgement cleared the banner")
	}
	for _, k := range cas.Keys() {
		if strings.HasPrefix(k, artifacts.AccessAcksPrefix) {
			t.Fatalf("a refused acknowledgement wrote %s", k)
		}
	}
}

// TestAcknowledgeAccessRevision_TheCurrentHead: a head written through the
// API after one written around it is acknowledged as it is now, on any
// replica.
func TestAcknowledgeAccessRevision_TheCurrentHead(t *testing.T) {
	s, h, cas := accessWriterOn(t)
	writeAround(t, s, cas, 40)
	ctx := asAlice(authn.ModeEnforce)
	etag, _ := etagOf(t, h)
	if _, err := h.CreateRole(ctx, connect.NewRequest(&orkstackv1.CreateRoleRequest{Etag: etag, Role: &orkstackv1.Role{Name: "r", Permissions: []string{"run.read"}}})); err != nil {
		t.Fatal(err)
	}
	if !s.access.Policies.State().WrittenAroundAPI {
		t.Fatal("a write through the API cleared the banner")
	}
	head, err := s.access.Policies.store.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := acknowledge(h, ctx, head.Meta.Revision, head.Meta.SHA256); err != nil {
		t.Fatal(err)
	}
	if s.access.Policies.State().WrittenAroundAPI {
		t.Fatal("acknowledging the current head left the banner")
	}
}

// TestAcknowledgeAccessRevision_IsAudited: the record names the revision
// and digest acknowledged.
func TestAcknowledgeAccessRevision_IsAudited(t *testing.T) {
	s, h, cas := accessWriterOn(t)
	around := writeAround(t, s, cas, 40)
	req := connect.NewRequest(&orkstackv1.AcknowledgeAccessRevisionRequest{Revision: around.Revision, Sha256: around.SHA256})
	next := func(ctx context.Context, r connect.AnyRequest) (connect.AnyResponse, error) {
		return h.AcknowledgeAccessRevision(ctx, r.(*connect.Request[orkstackv1.AcknowledgeAccessRevisionRequest]))
	}
	if _, err := s.auditUnary(asAlice(authn.ModeEnforce), orkstackv1connect.AccessServiceAcknowledgeAccessRevisionProcedure, req, next); err != nil {
		t.Fatal(err)
	}
	page, err := s.audit.store.ListAuditRecords(context.Background(), artifacts.AuditQuery{
		Since: time.Now().Add(-time.Hour), Until: time.Now().Add(time.Hour), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].GetAction() != "access.revision.acknowledge" || page.Records[0].GetTarget() != "40" ||
		page.Records[0].GetDetail() != "sha256="+around.SHA256 {
		t.Fatalf("records = %v; want one access.revision.acknowledge of 40", page.Records)
	}
}

// TestListAccessRevisions_NamesAHeadWithNoRevision: the history begins at
// the head as it is, so a head written around the API with no revision is
// listed, with the digest an acknowledgement names.
func TestListAccessRevisions_NamesAHeadWithNoRevision(t *testing.T) {
	s, h, cas := accessWriterOn(t)
	around := writeAround(t, s, cas, 40)
	resp, err := h.ListAccessRevisions(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.ListAccessRevisionsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	revs := resp.Msg.GetRevisions()
	if len(revs) == 0 || revs[0].GetRevision() != 40 || revs[0].GetSha256() != around.SHA256 {
		t.Fatalf("history = %v; want it to begin at revision 40, %s", revs, around.SHA256)
	}
}

// TestGetAccessRevision_TheHeadWithNoRevision: the head is read as its own
// revision, so a diff can reach a head written around the API.
func TestGetAccessRevision_TheHeadWithNoRevision(t *testing.T) {
	s, h, cas := accessWriterOn(t)
	around := writeAround(t, s, cas, 40)
	resp, err := h.GetAccessRevision(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.GetAccessRevisionRequest{Revision: 40}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetRevision().GetSha256() != around.SHA256 || resp.Msg.GetDocument().GetEtag() != "" || len(resp.Msg.GetDocument().GetBindings()) == 0 {
		t.Fatalf("revision 40 = %v", resp.Msg)
	}
}
