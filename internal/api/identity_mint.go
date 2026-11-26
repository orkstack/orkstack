package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// MintAttemptTokens issues an attempt's identity tokens
// (workload-identity §3; docs/TASK_IDENTITY.md, "The mint"). The handler
// holds step 1, this replica's guard, and step 3, the caller's permissions
// at the pool the admission gives the task; identity.Minter holds the
// rest. The access interceptor asks neither permission (the pool is known
// only from the admission), so this handler refuses every mode but enforce
// itself, and writes its own refusals' audit records.
func (s *Service) MintAttemptTokens(ctx context.Context, req *connect.Request[orkstackv1.MintAttemptTokensRequest]) (resp *connect.Response[orkstackv1.MintAttemptTokensResponse], err error) {
	defer func() { s.countMint(err) }()
	msg := req.Msg
	caller := authn.FromContext(ctx)
	// Step 1: this replica's own guard.
	switch {
	case authn.ModeFrom(ctx) != authn.ModeEnforce:
		return nil, s.mintRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonEngineRefused,
			Message: "this engine does not enforce authentication, so it issues no token"})
	case !s.access.enabled():
		return nil, s.mintRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonEngineRefused,
			Message: "this engine decides no permission, so it issues no token"})
	case s.minter == nil:
		return nil, s.mintRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonEngineRefused,
			Message: "this engine has no identity issuer"})
	case caller == nil:
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("a mint needs an Orker's verified credential"))
	case caller.OrkerID == "":
		return nil, s.mintRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonEngineRefused, Audit: true,
			Message: fmt.Sprintf("the credential of %q names no Orker", caller.ID)})
	}
	if err := checkMintRequest(msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	s.access.init()
	// The bounds come before anything is read: a principal's bucket now,
	// the attempt's budget once the request is admitted. A mint that issues
	// nothing gives both back.
	principalSpend, ok := s.mintLimits.TakePrincipal(caller.ID)
	if !ok {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New(identity.ReasonUnavailable+": too many mints; ask again"))
	}
	granted := false
	defer func() {
		if !granted {
			principalSpend.Refund()
		}
	}()
	mreq := mintRequestOf(msg)
	// Step 2. Its refusals come before the caller's permissions are known,
	// so they are told no more than the reason (MintError.Public) and are
	// not audited: a caller that may not mint learns nothing of the run.
	admitted, err := s.minter.Admit(ctx, mreq)
	if err != nil {
		var m *identity.MintError
		if errors.As(err, &m) {
			m.Audit = false
		}
		return nil, s.mintRefused(ctx, msg, caller, err)
	}
	// Step 3: the caller may mint, and commit a result, at the task's pool.
	if err := s.mayMint(ctx, caller, admitted); err != nil {
		return nil, s.mintRefused(ctx, msg, caller, err)
	}
	attemptSpend, ok := s.mintLimits.TakeAttempt(attemptKey(mreq))
	if !ok {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New(identity.ReasonUnavailable+": this attempt has asked for its tokens too often; ask again"))
	}
	// Steps 4 to 8.
	issued, err := s.minter.Issue(ctx, admitted, mreq, identity.Caller{Principal: caller.ID, OrkerID: caller.OrkerID})
	if err != nil {
		attemptSpend.Refund()
		return nil, s.mintRefused(ctx, msg, caller, err)
	}
	granted = true
	out := &orkstackv1.MintAttemptTokensResponse{InputArtifacts: issued.Received}
	for _, t := range issued.Tokens {
		out.Tokens = append(out.Tokens, &orkstackv1.IssuedToken{Name: t.Name, Audience: t.Audience, Token: t.Token,
			ExpiresInSeconds: int32(t.ExpiresIn.Seconds()), IssuedAt: timestamppb.New(t.IssuedAt), Jti: t.JTI, Kid: t.KID})
	}
	s.logger.Info("identity tokens issued", "namespace", mreq.Namespace, "run", mreq.RunID, "task", mreq.TaskID,
		"attempt", mreq.Attempt, "orker", caller.OrkerID, "tokens", len(out.Tokens))
	return connect.NewResponse(out), nil
}

func mintRequestOf(msg *orkstackv1.MintAttemptTokensRequest) identity.MintRequest {
	r := identity.MintRequest{Namespace: artifacts.ResolveNamespace(msg.GetNamespace()), RunID: msg.GetRunId(), TaskID: msg.GetTaskId(),
		Attempt: msg.GetAttempt(), TaskSpecSHA256: msg.GetTaskSpecSha256(), AdmissionSHA256: msg.GetAdmissionSha256(),
		Scopes: identity.Scopes{Resume: msg.GetResumeScope(), Root: msg.GetRootScope()}}
	if msg.Item != nil {
		r.Item = artifacts.Item{Index: msg.GetItem(), Set: true}
	}
	for _, t := range msg.GetTokens() {
		r.Tokens = append(r.Tokens, identity.TokenAsk{Name: t.GetName(), Audience: t.GetAudience()})
	}
	return r
}

// mayMint is step 3: the caller holds identity.token.mint and
// task.result.commit at the pool the admission gives the task. A document
// that cannot be read is unavailable, never allowed.
func (s *Service) mayMint(ctx context.Context, caller *authn.Principal, a identity.Admitted) error {
	policy, unavailable := s.access.policy(ctx)
	for _, perm := range []authz.Permission{authz.PermIdentityTokenMint, authz.PermTaskResultCommit} {
		_, err := s.authorizeWithIDs(ctx, authz.Request{Action: authz.Action(perm), Subject: subjectOf(caller),
			Resource: authz.Resource{Pool: a.Task.Pool}, Policy: policy})
		var denied *authz.DeniedError
		switch {
		case err == nil:
			continue
		case errors.As(err, &denied) && !unavailable:
			return &mintDenied{err: deniedAsConnect(err), perm: perm, pool: a.Task.Pool}
		default:
			// The document past its stale limit, or the ids of the scope
			// unread: undecided, never a denial and never an allowance.
			return &identity.MintError{Reason: identity.ReasonUnavailable, Unavailable: true,
				Message: fmt.Sprintf("whether %q may mint at pool %q could not be decided: %v", caller.ID, a.Task.Pool, err)}
		}
	}
	return nil
}

// mintDenied is a caller step 3 refused.
type mintDenied struct {
	err  error
	perm authz.Permission
	pool string
}

func (d *mintDenied) Error() string { return d.err.Error() }

// mintRefused answers a mint that issued nothing: the reason first in the
// message, as an Orker reads it, a Connect code that says whether asking
// again can help, and, for a refusal that says something about the caller
// or the run, one audit record, coalesced per principal as denials are.
// Could-not-ask answers are counted and logged, never recorded as objects.
func (s *Service) mintRefused(ctx context.Context, msg *orkstackv1.MintAttemptTokensRequest, caller *authn.Principal, err error) error {
	var denied *mintDenied
	if errors.As(err, &denied) {
		s.auditMint(ctx, msg, caller, "permission_denied", fmt.Sprintf("%s at pool %q: %v", denied.perm, denied.pool, denied.err))
		return connect.NewError(connect.CodePermissionDenied, errors.New("the caller may not mint for this attempt"))
	}
	var m *identity.MintError
	if !errors.As(err, &m) {
		s.logger.Error("identity tokens refused for an unexpected error", "run", msg.GetRunId(), "task", msg.GetTaskId(), "error", err)
		return connect.NewError(connect.CodeInternal, errors.New("the mint failed"))
	}
	code := connect.CodeFailedPrecondition
	if m.Unavailable {
		code = connect.CodeUnavailable
	}
	if m.Audit {
		s.auditMint(ctx, msg, caller, m.Reason, m.Message)
	}
	s.logger.Warn("identity tokens refused", "reason", m.Reason, "namespace", msg.GetNamespace(), "run", msg.GetRunId(),
		"task", msg.GetTaskId(), "attempt", msg.GetAttempt(), "message", m.Message)
	cerr := connect.NewError(code, errors.New(m.Public()))
	if m.EngineBuild != "" {
		cerr.Meta().Set("Ork-Engine-Build", m.EngineBuild)
	}
	return cerr
}

// auditMint writes identity.token.refused, at most one a principal, run
// and reason in the denial limiter's window.
func (s *Service) auditMint(ctx context.Context, msg *orkstackv1.MintAttemptTokensRequest, caller *authn.Principal, reason, detail string) {
	if s.audit == nil || caller == nil || !s.access.enabled() {
		return
	}
	s.access.init()
	if !s.access.denials.allow(caller.ID, "identity.token.refused", msg.GetRunId()+"/"+reason) {
		return
	}
	at := s.audit.now().UTC()
	rec := &orkstackv1.AuditRecord{
		Id: artifacts.NewAuditID(at), At: timestamppb.New(at), Action: "identity.token.refused",
		Procedure: "/orkstack.v1.IdentityService/MintAttemptTokens", Namespace: artifacts.ResolveNamespace(msg.GetNamespace()),
		Target: fmt.Sprintf("%s/%s attempt %d", msg.GetRunId(), msg.GetTaskId(), msg.GetAttempt()),
		Detail: truncateDetail(reason + ": " + detail), Succeeded: false, ErrorCode: reason,
	}
	stampDecision(ctx, rec, nil)
	s.audit.Record(ctx, rec)
}

// Bounds on what a mint request may carry, checked before anything is read
// or recorded, so no caller-chosen string reaches a log line or an audit
// record unbounded.
const (
	maxMintRunID    = 64
	maxMintName     = 200
	maxMintAudience = 255
	maxAuditDetail  = 1024
)

func checkMintRequest(msg *orkstackv1.MintAttemptTokensRequest) error {
	if !slugOrEmpty(msg.GetNamespace()) || !mintSlug(msg.GetTaskId()) {
		return errors.New("namespace and task_id must be names")
	}
	if id := msg.GetRunId(); id == "" || len(id) > maxMintRunID || !printableASCII(id) {
		return fmt.Errorf("run_id must be 1 to %d printable characters", maxMintRunID)
	}
	if len(msg.GetTaskSpecSha256()) > 64 || len(msg.GetAdmissionSha256()) > 64 {
		return errors.New("a digest is 64 hexadecimal digits")
	}
	for _, t := range msg.GetTokens() {
		if len(t.GetName()) > maxMintName || len(t.GetAudience()) > maxMintAudience || !printableASCII(t.GetName()) || !printableASCII(t.GetAudience()) {
			return errors.New("a token's name and audience must be short and printable")
		}
	}
	return nil
}

func mintSlug(s string) bool {
	if s == "" || len(s) > maxMintName || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

func slugOrEmpty(s string) bool { return s == "" || mintSlug(s) }

func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

func truncateDetail(s string) string {
	if len(s) <= maxAuditDetail {
		return s
	}
	return s[:maxAuditDetail] + "…"
}

// countMint counts one mint under its outcome (identity.MintObserver).
func (s *Service) countMint(err error) {
	if s.mintObserver == nil {
		return
	}
	var cerr *connect.Error
	switch {
	case err == nil:
		s.mintObserver.Minted(identity.OutcomeGranted)
	case !errors.As(err, &cerr):
		s.mintObserver.Minted("other")
	case cerr.Code() == connect.CodeInvalidArgument:
		s.mintObserver.Minted(identity.OutcomeInvalid)
	case cerr.Code() == connect.CodeResourceExhausted:
		s.mintObserver.Minted(identity.OutcomeResourceExhausted)
	case cerr.Code() == connect.CodePermissionDenied, cerr.Code() == connect.CodeUnauthenticated:
		s.mintObserver.Minted(identity.OutcomePermissionDenied)
	case cerr.Code() == connect.CodeUnavailable:
		s.mintObserver.Minted(identity.ReasonUnavailable)
	default:
		reason, _, _ := strings.Cut(cerr.Message(), ":")
		s.mintObserver.Minted(reason)
	}
}

// attemptKey names one attempt's budget by value: namespace, run, task,
// item ("-" for none) and attempt. The resume scope is not part of it: it is
// the request's word until step 6a, and no budget opens on that.
func attemptKey(r identity.MintRequest) string {
	item := "-"
	if r.Item.Set {
		item = fmt.Sprint(r.Item.Index)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%d", r.Namespace, r.RunID, r.TaskID, item, r.Attempt)
}
