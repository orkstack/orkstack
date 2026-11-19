package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// IssuedLister lists the records of what a run's mints issued.
type IssuedLister interface {
	ListIssued(ctx context.Context, q artifacts.IssuedQuery) ([]artifacts.RecordRead[artifacts.IssuedRecord], string, error)
}

// ListIssuedTokens answers what was issued to a run's attempts: one record
// a granted mint, with each token's claims and never a token
// (docs/TASK_IDENTITY.md, "What was issued"). It is a read of the run
// (run.read), a page at a time in the order they were issued. A record
// that cannot be trusted is named, never dropped, and refuses the page.
func (s *Service) ListIssuedTokens(ctx context.Context, req *connect.Request[orkstackv1.ListIssuedTokensRequest]) (*connect.Response[orkstackv1.ListIssuedTokensResponse], error) {
	if s.issuedRecords == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this engine keeps no record of issued tokens: it issues none"))
	}
	msg := req.Msg
	q := artifacts.IssuedQuery{Namespace: artifacts.ResolveNamespace(msg.GetNamespace()), RunID: msg.GetRunId(), TaskID: msg.GetTaskId(),
		After: msg.GetPageToken(), Limit: int(msg.GetPageSize())}
	if msg.Item != nil {
		if msg.GetTaskId() == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("item needs task_id"))
		}
		q.Item = artifacts.Item{Index: msg.GetItem(), Set: true}
	}
	reads, next, err := s.issuedRecords.ListIssued(ctx, q)
	switch {
	case errors.Is(err, artifacts.ErrInvalidRecordKey):
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the records of run %q could not be read: %w", msg.GetRunId(), err))
	}
	out := &orkstackv1.ListIssuedTokensResponse{NextPageToken: next}
	for _, r := range reads {
		if r.Answer != artifacts.RecordFound {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"a record of what run %q was issued cannot be trusted: %s: %s", msg.GetRunId(), r.Object, r.Reason))
		}
		out.Records = append(out.Records, issuedRecordProto(r.Value))
	}
	return connect.NewResponse(out), nil
}

func issuedRecordProto(r artifacts.IssuedRecord) *orkstackv1.IssuedTokenRecord {
	out := &orkstackv1.IssuedTokenRecord{TaskId: r.TaskID, Attempt: r.Attempt, Principal: r.Principal, OrkerId: r.OrkerID,
		IssuedAt: timestamppb.New(r.IssuedAt)}
	if r.Item.Set {
		item := r.Item.Index
		out.Item = &item
	}
	for _, e := range r.Tokens {
		out.Tokens = append(out.Tokens, &orkstackv1.IssuedTokenEntry{Name: e.Name, Audience: e.Audience, Subject: e.Subject, Jti: e.JTI,
			Kid: e.KID, IssuedAt: timestamppb.New(unixUTC(e.IssuedAtUnix)), ExpiresAt: timestamppb.New(unixUTC(e.ExpiresAtUnix))})
	}
	return out
}

func unixUTC(sec int64) time.Time { return time.Unix(sec, 0).UTC() }
