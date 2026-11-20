package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestListIssuedTokens: what a run was issued is listed a page at a time
// in the order it was issued, each token's claims and never a token; a
// service that keeps no records answers unimplemented.
func TestListIssuedTokens(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewRecordStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	for i, task := range []string{"build", "build", "push"} {
		rec := artifacts.IssuedRecord{Namespace: "default", RunID: "ork-1", TaskID: task, Attempt: int32(i + 1), Principal: "serviceaccount:orker-1",
			OrkerID: "orker-1", IssuedAt: at.Add(time.Duration(i) * time.Second), Tokens: []artifacts.IssuedEntry{{Name: "aws", Audience: "sts",
				Subject: "ork:namespace:default:orkflow:deploy:task:" + task, JTI: strings.Repeat("0", 15) + string(rune('1'+i)), KID: "k1",
				IssuedAtUnix: at.Unix(), ExpiresAtUnix: at.Add(time.Hour).Unix()}}}
		if _, err := store.WriteIssued(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	s := &Service{issuedRecords: store}
	page, err := s.ListIssuedTokens(ctx, connect.NewRequest(&orkstackv1.ListIssuedTokensRequest{RunId: "ork-1", PageSize: 2}))
	if err != nil || len(page.Msg.GetRecords()) != 2 || page.Msg.GetNextPageToken() == "" {
		t.Fatalf("first page = %v, %v", page, err)
	}
	if e := page.Msg.GetRecords()[0].GetTokens()[0]; e.GetSubject() == "" || e.GetExpiresAt().AsTime() != at.Add(time.Hour) {
		t.Fatalf("entry = %v", e)
	}
	rest, err := s.ListIssuedTokens(ctx, connect.NewRequest(&orkstackv1.ListIssuedTokensRequest{RunId: "ork-1", PageToken: page.Msg.GetNextPageToken()}))
	if err != nil || len(rest.Msg.GetRecords()) != 1 || rest.Msg.GetRecords()[0].GetTaskId() != "push" {
		t.Fatalf("second page = %v, %v", rest, err)
	}
	one, err := s.ListIssuedTokens(ctx, connect.NewRequest(&orkstackv1.ListIssuedTokensRequest{RunId: "ork-1", TaskId: "push"}))
	if err != nil || len(one.Msg.GetRecords()) != 1 {
		t.Fatalf("one task = %v, %v", one, err)
	}
	if _, err := s.ListIssuedTokens(ctx, connect.NewRequest(&orkstackv1.ListIssuedTokensRequest{RunId: "ork-1", PageToken: "metadata/other"})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a token of another listing: %v", err)
	}
	if _, err := (&Service{}).ListIssuedTokens(ctx, connect.NewRequest(&orkstackv1.ListIssuedTokensRequest{RunId: "ork-1"})); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("no records kept: %v", err)
	}
}
