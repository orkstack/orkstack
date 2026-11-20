package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// CommitRequestMaxBytes is a CommitResult request's bound: the commit's own
// bound and 64 KiB for the rest (workload-identity §3.4, "Bounds").
const CommitRequestMaxBytes = artifacts.CommitMaxBytes + 64<<10

// Bounds on what a commit request carries besides its manifest, checked
// before anything is read or recorded.
const (
	maxCommitKey         = 1024
	maxCommitSpecMembers = 32
)

// CommitResult records an attempt's result for a run whose results the
// engine records (workload-identity §3.4; docs/TASK_IDENTITY.md, "Results
// the engine records"). The handler holds step 1, this replica's guard,
// and step 3, the caller's task.result.commit at the pool the admission
// gives the task; identity.Recorder holds the rest, whose steps 2 and 4 to
// 6a are the mint's. The access interceptor asks the permission of nobody
// (the pool is known only from the admission), so this handler refuses
// every mode but enforce itself, and writes its own refusals' audit
// records.
func (s *Service) CommitResult(ctx context.Context, req *connect.Request[orkstackv1.CommitResultRequest]) (resp *connect.Response[orkstackv1.CommitResultResponse], rerr error) {
	defer func() {
		if s.commitObserver != nil {
			s.commitObserver.Committed(callOutcome(rerr))
		}
	}()
	msg := req.Msg
	caller := authn.FromContext(ctx)
	// Step 1: this replica's own guard.
	switch {
	case authn.ModeFrom(ctx) != authn.ModeEnforce:
		return nil, s.commitRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonEngineRefused,
			Message: "this engine does not enforce authentication, so it records no result"})
	case !s.access.enabled():
		return nil, s.commitRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonEngineRefused,
			Message: "this engine decides no permission, so it records no result"})
	case s.recorder == nil:
		return nil, s.commitRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonEngineRefused,
			Message: "this engine records no result"})
	case caller == nil:
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("a commit needs an Orker's verified credential"))
	}
	// The bounds come before anything is looked at: a principal's bucket
	// now, the attempt's budget once the request is admitted. Only a call
	// that could not be asked gives its token back, so a credential cannot
	// make the engine read without limit by asking for what it is refused.
	principalSpend, ok := s.commitLimits.TakePrincipal(caller.ID)
	if !ok {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New(identity.ReasonUnavailable+": too many results; ask again"))
	}
	var err error
	defer func() {
		var m *identity.MintError
		if errors.As(err, &m) && m.Unavailable {
			principalSpend.Refund()
		}
	}()
	if err = checkCommitRequest(msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	// Audited only now, once the names it records are known to be names.
	if caller.OrkerID == "" {
		return nil, s.commitRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonEngineRefused, Audit: true,
			Message: fmt.Sprintf("the credential of %q names no Orker", caller.ID)})
	}
	if msg.GetResumeScope() != 0 || msg.GetRootScope() != 0 {
		return nil, s.commitRefused(ctx, msg, caller, &identity.MintError{Reason: identity.ReasonNotAdmitted, Audit: true,
			Message: "a resumed run's scope is not admitted in this build"})
	}
	s.access.init()
	creq := commitRequestOf(msg)
	// Step 2. Its refusals come before the caller's permission is known, so
	// they are told no more than the reason and are not audited.
	admitted, err := s.recorder.Admit(ctx, creq)
	if err != nil {
		var m *identity.MintError
		if errors.As(err, &m) {
			m.Audit = false
		}
		return nil, s.commitRefused(ctx, msg, caller, err)
	}
	// Step 3: the caller may commit a result at the task's pool.
	if err = s.mayCommit(ctx, caller, admitted); err != nil {
		return nil, s.commitRefused(ctx, msg, caller, err)
	}
	// The stored form's bound costs an encoding of up to 4 MiB, so it is
	// paid only for a caller that may commit at the task's pool.
	if err = checkStoredSize(msg.GetCommit()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	attemptSpend, ok := s.commitLimits.TakeAttempt(commitAttemptKey(creq))
	if !ok {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New(identity.ReasonUnavailable+": this attempt has asked to commit too often; ask again"))
	}
	// The commit root is configuration: a fault in it costs no write.
	root, err := artifacts.CommitRoot(s.artifacts)
	if err != nil {
		attemptSpend.Refund()
		s.logger.Error("the commit root cannot be named; no result is recorded", "run", creq.RunID, "task", creq.TaskID, "error", err)
		return nil, connect.NewError(connect.CodeInternal, errors.New(identity.ReasonUnavailable+": the engine cannot name where results are recorded"))
	}
	// Steps 4 to 8.
	got, err := s.recorder.Record(ctx, admitted, creq, identity.Caller{Principal: caller.ID, OrkerID: caller.OrkerID})
	if err != nil {
		attemptSpend.Refund()
		return nil, s.commitRefused(ctx, msg, caller, err)
	}
	out := &orkstackv1.CommitResultResponse{}
	if creq.Preflight {
		s.logger.Debug("a task result's preflight passed", "namespace", creq.Namespace, "run", creq.RunID, "task", creq.TaskID,
			"attempt", creq.Attempt, "orker", caller.OrkerID)
		return connect.NewResponse(out), nil
	}
	out.Commit = &orkstackv1.CommitRef{Uri: strings.TrimSuffix(root, "/") + "/" + got.Key, Sha256: got.Record.SHA256, Etag: got.Record.ETag,
		CreatedAt: got.Record.Commit.GetRecorded().GetTime()}
	out.Created = got.Created
	if !got.Created {
		out.Existing = got.Record.Commit
	}
	s.logger.Info("task result recorded", "namespace", creq.Namespace, "run", creq.RunID, "task", creq.TaskID,
		"attempt", creq.Attempt, "orker", caller.OrkerID, "created", out.GetCreated())
	return connect.NewResponse(out), nil
}

func commitRequestOf(msg *orkstackv1.CommitResultRequest) identity.CommitRequest {
	r := identity.CommitRequest{Namespace: artifacts.ResolveNamespace(msg.GetNamespace()), RunID: msg.GetRunId(), TaskID: msg.GetTaskId(),
		Attempt: msg.GetAttempt(), TaskSpecSHA256: msg.GetTaskSpecSha256(), AdmissionSHA256: msg.GetAdmissionSha256(),
		CommitKey: msg.GetCommitKey(), Commit: msg.GetCommit(), Preflight: msg.GetPreflight()}
	if msg.Item != nil {
		r.Item = artifacts.Item{Index: msg.GetItem(), Set: true}
	}
	return r
}

// commitAttemptKey names one attempt's budget by value, as the mint's.
func commitAttemptKey(r identity.CommitRequest) string {
	return attemptKey(identity.MintRequest{Namespace: r.Namespace, RunID: r.RunID, TaskID: r.TaskID, Item: r.Item, Attempt: r.Attempt})
}

func checkCommitRequest(msg *orkstackv1.CommitResultRequest) error {
	if !slugOrEmpty(msg.GetNamespace()) || !mintSlug(msg.GetTaskId()) {
		return errors.New("namespace and task_id must be names")
	}
	if id := msg.GetRunId(); id == "" || len(id) > maxMintRunID || !printableASCII(id) {
		return fmt.Errorf("run_id must be 1 to %d printable characters", maxMintRunID)
	}
	if len(msg.GetTaskSpecSha256()) > 64 || len(msg.GetAdmissionSha256()) > 64 {
		return errors.New("a digest is 64 hexadecimal digits")
	}
	if k := msg.GetCommitKey(); (k == "" && !msg.GetPreflight()) || len(k) > maxCommitKey || !printableASCII(k) {
		return fmt.Errorf("commit_key must be 1 to %d printable characters", maxCommitKey)
	}
	if len(msg.GetSpecMembers()) > maxCommitSpecMembers {
		return fmt.Errorf("spec_members names at most %d members", maxCommitSpecMembers)
	}
	for k, v := range msg.GetSpecMembers() {
		if len(k) > maxMintName || len(v) > 64 || !printableASCII(k) || !printableASCII(v) {
			return errors.New("a spec member's name and digest must be short and printable")
		}
	}
	switch {
	case msg.GetPreflight() && msg.GetCommit() != nil:
		return errors.New("a preflight carries no result")
	case !msg.GetPreflight() && msg.GetCommit() == nil:
		return errors.New("a commit carries its result")
	case proto.Size(msg.GetCommit()) > artifacts.CommitMaxBytes:
		return fmt.Errorf("the result is over the commit's bound of %d bytes", artifacts.CommitMaxBytes)
	}
	return nil
}

// checkStoredSize bounds a result's stored form, which is larger than its
// wire form: a result that could not be recorded is refused now, never
// after the Orker has waited for it. A preflight's absent result passes.
func checkStoredSize(c *orkstackv1.ActivityCommit) error {
	if c == nil {
		return nil
	}
	stored, err := artifacts.MarshalCommit(c)
	if err != nil {
		return fmt.Errorf("the result cannot be encoded: %w", err)
	}
	if len(stored) > artifacts.CommitMaxBytes-recordingRoom {
		return fmt.Errorf("the result is %d bytes stored, over the commit's bound of %d less %d for the engine's recording",
			len(stored), artifacts.CommitMaxBytes, recordingRoom)
	}
	return nil
}

// recordingRoom is what a result's stored form keeps free for the
// recording the engine adds (its pool, Orker id, digest, build and time are
// bounded names and a timestamp).
const recordingRoom = 4 << 10

// mayCommit is step 3: the caller holds task.result.commit at the pool the
// admission gives the task. A document that cannot be read is
// unavailable, never allowed.
func (s *Service) mayCommit(ctx context.Context, caller *authn.Principal, a identity.Admitted) error {
	policy, unavailable := s.access.policy(ctx)
	_, err := s.authorizeWithIDs(ctx, authz.Request{Action: authz.Action(authz.PermTaskResultCommit), Subject: subjectOf(caller),
		Resource: authz.Resource{Pool: a.Task.Pool}, Policy: policy})
	var denied *authz.DeniedError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &denied) && !unavailable:
		return &mintDenied{err: deniedAsConnect(err), perm: authz.PermTaskResultCommit, pool: a.Task.Pool}
	default:
		return &identity.MintError{Reason: identity.ReasonUnavailable, Unavailable: true,
			Message: fmt.Sprintf("whether %q may commit at pool %q could not be decided: %v", caller.ID, a.Task.Pool, err)}
	}
}

// commitRefused answers a commit that recorded nothing, as mintRefused
// answers a mint: the reason first, a Connect code that says whether
// asking again can help, and for a refusal that says something about the
// caller or the run one task.result.refused audit record, coalesced per
// principal as denials are.
func (s *Service) commitRefused(ctx context.Context, msg *orkstackv1.CommitResultRequest, caller *authn.Principal, err error) error {
	var denied *mintDenied
	if errors.As(err, &denied) {
		s.auditCommit(ctx, msg, caller, "permission_denied", fmt.Sprintf("%s at pool %q: %v", denied.perm, denied.pool, denied.err))
		return connect.NewError(connect.CodePermissionDenied, errors.New("the caller may not commit a result for this attempt"))
	}
	var m *identity.MintError
	if !errors.As(err, &m) {
		s.logger.Error("a task result was refused for an unexpected error", "run", msg.GetRunId(), "task", msg.GetTaskId(), "error", err)
		return connect.NewError(connect.CodeInternal, errors.New(identity.ReasonUnavailable+": the commit failed; ask again"))
	}
	code := connect.CodeFailedPrecondition
	if m.Unavailable {
		code = connect.CodeUnavailable
	}
	if m.Audit {
		s.auditCommit(ctx, msg, caller, m.Reason, m.Message)
	}
	s.logger.Warn("a task result was refused", "reason", m.Reason, "namespace", msg.GetNamespace(), "run", msg.GetRunId(),
		"task", msg.GetTaskId(), "attempt", msg.GetAttempt(), "preflight", msg.GetPreflight(), "message", m.Message)
	cerr := connect.NewError(code, errors.New(m.Public()))
	if m.EngineBuild != "" {
		cerr.Meta().Set("Ork-Engine-Build", m.EngineBuild)
	}
	return cerr
}

// auditCommit writes task.result.refused, at most one a principal, run
// and reason in the denial limiter's window.
func (s *Service) auditCommit(ctx context.Context, msg *orkstackv1.CommitResultRequest, caller *authn.Principal, reason, detail string) {
	if s.audit == nil || caller == nil || !s.access.enabled() {
		return
	}
	s.access.init()
	if !s.access.denials.allow(caller.ID, "task.result.refused", msg.GetRunId()+"/"+reason) {
		return
	}
	at := s.audit.now().UTC()
	rec := &orkstackv1.AuditRecord{
		Id: artifacts.NewAuditID(at), At: timestamppb.New(at), Action: "task.result.refused",
		Procedure: "/orkstack.v1.AttemptService/CommitResult", Namespace: artifacts.ResolveNamespace(msg.GetNamespace()),
		Target: fmt.Sprintf("%s/%s attempt %d", msg.GetRunId(), msg.GetTaskId(), msg.GetAttempt()),
		Detail: truncateDetail(reason + ": " + detail), Succeeded: false, ErrorCode: reason,
	}
	stampDecision(ctx, rec, nil)
	s.audit.Record(ctx, rec)
}

// pinCommitWait adds identity.results.commit_wait to the transfer budget of
// a plan whose run tree holds an identity (workload-identity §3.4, "When
// the engine cannot be asked"): an attempt's deadline is its timeout, its
// grace and the transfer budget, so the wait an Orker spends at its commit
// is pinned into the deadline the cluster holds it to. Every other plan is
// unchanged.
func (s *Service) pinCommitWait(plan *engine.RunPlan, holdsIdentity bool) {
	if !holdsIdentity || s.identityCommitWait <= 0 {
		return
	}
	sum := int64(plan.TransferBudgetSeconds) + int64(s.identityCommitWait/time.Second)
	plan.TransferBudgetSeconds = int32(min(sum, math.MaxInt32))
}

// callOutcome is the label a mint or a commit is counted under: granted,
// the reason the Connect error's message leads with, or the class of a
// call refused before any reason applied.
func callOutcome(err error) string {
	var cerr *connect.Error
	switch {
	case err == nil:
		return identity.OutcomeGranted
	case !errors.As(err, &cerr):
		return "other"
	case cerr.Code() == connect.CodeInvalidArgument:
		return identity.OutcomeInvalid
	case cerr.Code() == connect.CodeResourceExhausted:
		return identity.OutcomeResourceExhausted
	case cerr.Code() == connect.CodePermissionDenied, cerr.Code() == connect.CodeUnauthenticated:
		return identity.OutcomePermissionDenied
	case cerr.Code() == connect.CodeUnavailable:
		return identity.ReasonUnavailable
	default:
		reason, _, _ := strings.Cut(cerr.Message(), ":")
		return reason
	}
}
