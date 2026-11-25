package orker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The reasons of results the engine records (docs/TASK_IDENTITY.md,
// "Results the engine records"; docs/REASONS.md, "The Orker and the
// container").
const (
	// ReasonCommitEngineUnavailable is an attempt whose result no engine
	// recorded within its wait: none answered, and no record of it is
	// there. Retried under the task's policy; nothing is written around the
	// engine.
	ReasonCommitEngineUnavailable = "COMMIT_ENGINE_UNAVAILABLE"
	// ReasonCommitEngineRefused is an engine that refused this Orker itself:
	// its credential, its permission at the pool, or a request the engine
	// could not read. Logged at error; an operator's fault, never the task's.
	ReasonCommitEngineRefused = "COMMIT_ENGINE_REFUSED"
	// ReasonUpstreamNotRecorded is an upstream container task of a run whose
	// results the engine records with no engine-written record, or with
	// references the record does not hold. Not retried: nothing a retry of
	// the dependent does makes a record appear.
	ReasonUpstreamNotRecorded = "UPSTREAM_RESULT_NOT_RECORDED"
	// ReasonRequiredResultNotRecorded is the engine's answer that a task
	// this one follows has no record of success (workload-identity §3.6).
	ReasonRequiredResultNotRecorded = "REQUIRED_RESULT_NOT_RECORDED"
)

// The wait's margins (workload-identity §3.4, "When the engine cannot be
// asked"): the wait ends 5 s before the attempt's deadline, and the last
// look for a record has 10 s of its own and is begun only with 15 s left.
const (
	commitWaitMargin     = 5 * time.Second
	recordLookTimeout    = 10 * time.Second
	recordLookMinLeft    = 15 * time.Second
	commitCallTimeout    = 30 * time.Second
	defaultCommitWaitSec = 900
)

// resultCommitFunc hands the engine an attempt's result, or asks whether
// it may (AttemptService.CommitResult).
type resultCommitFunc func(ctx context.Context, req *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error)

// NewResultCommitter is the CommitResult an Orker's attempts share: the
// engine client's.
func NewResultCommitter(engine *EngineClient) resultCommitFunc { return engine.CommitResult }

// SetResultCommit gives the dispatcher the engine's CommitResult; an Orker
// that reaches no engine has none, and refuses a task of a run whose results
// the engine records (IDENTITY_NOT_CONFIGURED).
func (d *OCIDispatcher) SetResultCommit(commit resultCommitFunc) { d.resultCommit = commit }

// commitThen is what an attempt does with the engine's answer to a commit.
type commitThen int

const (
	// commitAskAgain: ask again by the one retry rule while the wait lasts.
	commitAskAgain commitThen = iota + 1
	// commitOnceMore: ask once more inside the wait, then end.
	commitOnceMore
	// commitEnd: end the attempt now with the reason.
	commitEnd
)

// commitAnswer is one row of the Orker's table of the engine's answers to
// CommitResult.
type commitAnswer struct {
	Reason  string
	Then    commitThen
	Refused bool // the engine refused this Orker itself: logged at error
	Message string
}

// mapCommitAnswer is the Orker's table of a failed CommitResult
// (workload-identity §3.4), one case a row in its test. preflight says the
// call was the preflight, which ends at once on a refusal of this Orker,
// since no container has run; at the commit the role may have been taken
// away while the container ran, so it is asked again inside the wait.
// orkerBuild tells a task the engine admitted differently from one two
// builds disagree about.
func mapCommitAnswer(err error, preflight bool, orkerBuild string) commitAnswer {
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		return commitAnswer{ReasonCommitEngineUnavailable, commitAskAgain, false, "the engine could not be asked: " + err.Error()}
	}
	msg := cerr.Message()
	refused := func(why string) commitAnswer {
		then := commitAskAgain
		if preflight {
			then = commitEnd
		}
		return commitAnswer{ReasonCommitEngineRefused, then, true, why + msg}
	}
	switch cerr.Code() {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return refused("the engine refused this Orker (" + cerr.Code().String() + "): ")
	case connect.CodeInvalidArgument:
		return commitAnswer{ReasonCommitEngineRefused, commitEnd, true, "the engine could not read this Orker's request: " + msg}
	case connect.CodeFailedPrecondition:
	default:
		// unimplemented is what an engine of the release before answers,
		// and another replica may not: asked again, as an outage is.
		return commitAnswer{ReasonCommitEngineUnavailable, commitAskAgain, false, "the engine could not answer (" + cerr.Code().String() + "): " + msg}
	}
	reason, _, _ := strings.Cut(msg, ":")
	switch reason {
	case identity.ReasonEngineRefused:
		return refused("")
	case identity.ReasonAttemptNotHeld:
		return commitAnswer{reason, commitAskAgain, false, msg}
	case identity.ReasonNotAdmitted, ReasonRequiredResultNotRecorded:
		return commitAnswer{reason, commitOnceMore, false, msg}
	case identity.ReasonTaskNotAsAdmitted:
		if engine := cerr.Meta().Get(engineBuildHeader); engine != orkerBuild {
			return commitAnswer{ReasonIdentitySpecSkew, commitEnd, false,
				fmt.Sprintf("%s (this Orker's build %q, the admitting engine's %q)", msg, orkerBuild, engine)}
		}
		return commitAnswer{reason, commitEnd, false, msg}
	case ReasonCommitIdentity:
		return commitAnswer{reason, commitEnd, false, msg}
	case identity.ReasonUnavailable:
		return commitAnswer{ReasonCommitEngineUnavailable, commitAskAgain, false, msg}
	}
	return commitAnswer{ReasonCommitEngineUnavailable, commitAskAgain, false, "the engine answered a refusal this Orker does not know: " + msg}
}

// commitRequest is the CommitResult request of one attempt: its
// coordinates, the task's spec digest and members as this Orker computes
// them, the admission, both scopes and the commit key the input named. A
// preflight carries no result.
func commitRequest(in *orkstackv1.ExecuteTaskInput, attempt int32, commit *orkstackv1.ActivityCommit) (*orkstackv1.CommitResultRequest, *Failure) {
	mint, f := identityRequest(in, attempt)
	if f != nil {
		return nil, f
	}
	key, err := commitKeyOf(in.GetCommitUri())
	if err != nil {
		return nil, FailCause(ReasonCommitIdentity, err, "task %q: %v", in.GetTaskId(), err)
	}
	return &orkstackv1.CommitResultRequest{Namespace: mint.GetNamespace(), RunId: mint.GetRunId(), TaskId: mint.GetTaskId(), Item: mint.Item,
		Attempt: attempt, ResumeScope: mint.GetResumeScope(), RootScope: mint.GetRootScope(), TaskSpecSha256: mint.GetTaskSpecSha256(),
		AdmissionSha256: mint.GetAdmissionSha256(), SpecMembers: mint.GetSpecMembers(), CommitKey: key, Commit: commit, Preflight: commit == nil}, nil
}

// commitKeyOf is the commit key a task's commit uri ends in.
func commitKeyOf(uri string) (string, error) {
	i := strings.LastIndex(uri, "/"+artifacts.CommitPrefix+"/")
	if i < 0 {
		return "", fmt.Errorf("the commit uri %s names no commit key", uri)
	}
	return uri[i+1:], nil
}

func (d *OCIDispatcher) recordClock() identityClock {
	if d.recordClockFor != nil {
		return d.recordClockFor
	}
	return systemIdentityClock{}
}

func (d *OCIDispatcher) recordDraw() float64 {
	if d.recordDrawFor != nil {
		return d.recordDrawFor()
	}
	return rand.Float64()
}

func (d *OCIDispatcher) retryMin() time.Duration {
	if d.identityConfig != nil && d.identityConfig.RetryMin > 0 {
		return d.identityConfig.RetryMin
	}
	return DefaultIdentityRetryMin
}

func (d *OCIDispatcher) setupBudget() time.Duration {
	if d.identityConfig != nil && d.identityConfig.SetupBudget > 0 {
		return d.identityConfig.SetupBudget
	}
	return DefaultIdentitySetupBudget
}

// preflightCommit is step 3c for a task of a run whose results the engine
// records that holds no identity (one that does meets the same checks at
// its mint): it asks the engine whether this attempt may commit before any
// workspace is made or container started. A refusal ends the attempt at
// once; an engine that cannot be asked is asked again inside the setup
// budget, then the attempt fails COMMIT_ENGINE_UNAVAILABLE, retried, with
// nothing run.
func (d *OCIDispatcher) preflightCommit(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32) *Failure {
	if d.resultCommit == nil {
		return Fail(ReasonIdentityNotConfigured, "task %q is of a run whose results the engine records, and this Orker reaches no engine (orker.engine.url)", in.GetTaskId())
	}
	req, f := commitRequest(in, attempt, nil)
	if f != nil {
		return f
	}
	clock := d.recordClock()
	budget := d.setupBudget()
	if deadline, ok := ctx.Deadline(); ok {
		budget = max(min(budget, time.Until(deadline)-commitWaitMargin), 0)
	}
	resp, last, f := d.askCommit(ctx, req, true, clock.Boot()+budget)
	if f != nil || resp != nil {
		return f
	}
	return Fail(last.Reason, "the engine did not answer whether this attempt may commit within its setup budget of %s: %s", d.setupBudget(), last.Message)
}

// commitThroughEngine is step 13 for a run whose results the engine
// records: it hands the manifest to the engine, which writes the record,
// and answers the record now at the key. It asks by the one retry rule for
// the attempt's wait (the run's pinned commit wait, held 5 s inside the
// attempt's deadline); before it gives up it reads the record's key itself,
// since an engine may have written it and not been heard, and reports a
// record there; only with none there does it fail COMMIT_ENGINE_UNAVAILABLE.
// It never writes around the engine.
func (d *OCIDispatcher) commitThroughEngine(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32,
	commit *orkstackv1.ActivityCommit) (*orkstackv1.CommitResultResponse, *Failure) {
	if d.resultCommit == nil {
		return nil, Fail(ReasonIdentityNotConfigured, "task %q is of a run whose results the engine records, and this Orker reaches no engine (orker.engine.url)", in.GetTaskId())
	}
	req, f := commitRequest(in, attempt, commit)
	if f != nil {
		return nil, f
	}
	clock := d.recordClock()
	wait := time.Duration(in.GetIdentity().GetCommitWaitSeconds()) * time.Second
	if wait <= 0 {
		wait = defaultCommitWaitSec * time.Second
	}
	if deadline, ok := ctx.Deadline(); ok {
		wait = max(min(wait, time.Until(deadline)-commitWaitMargin), 0)
	}
	d.logger().Info("handing the attempt's result to the engine", "run", in.GetRunId(), "task", in.GetTaskId(), "attempt", attempt, "wait", wait)
	resp, last, f := d.askCommit(ctx, req, false, clock.Boot()+wait)
	if f != nil || resp != nil {
		return resp, f
	}
	// The last look: a record an engine wrote and was not heard about is
	// this task's result, whichever attempt made it.
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < recordLookMinLeft {
		return nil, Fail(last.Reason, "no engine recorded the result within the attempt's wait, and too little of its deadline was left to look for a record: %s", last.Message)
	}
	look, cancel := context.WithTimeout(ctx, recordLookTimeout)
	defer cancel()
	uri, err := artifacts.AttestedCommitURI(in.GetCommitUri())
	if err != nil {
		return nil, FailCause(ReasonCommitIdentity, err, "task %q: %v", in.GetTaskId(), err)
	}
	record, err := d.Commits.Get(look, uri)
	if err != nil && ctx.Err() != nil {
		return nil, beforeClaimStopped(context.Cause(ctx))
	}
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, Fail(last.Reason, "no engine recorded the result within the attempt's wait of %s, and no record is there: %s", wait, last.Message)
	case err != nil:
		return nil, FailCause(ReasonCommitReadFailed, err, "no engine answered within the attempt's wait, and its record at %s could not be read: %v", uri, err)
	}
	return &orkstackv1.CommitResultResponse{Commit: &orkstackv1.CommitRef{Uri: uri, Sha256: record.SHA256, Etag: record.ETag},
		Existing: record.Commit}, nil
}

// askCommit asks the engine until it answers, an answer ends the attempt,
// or asking again would pass until (on the boot clock). It answers the
// engine's response, or the failure that ended it, or neither with the last
// answer when the time ran out.
func (d *OCIDispatcher) askCommit(ctx context.Context, req *orkstackv1.CommitResultRequest, preflight bool, until time.Duration) (*orkstackv1.CommitResultResponse, commitAnswer, *Failure) {
	clock := d.recordClock()
	onceMore := false
	for n := 1; ; n++ {
		within := min(max(until-clock.Boot(), time.Second), commitCallTimeout)
		call, cancel := context.WithTimeout(ctx, within)
		resp, err := d.resultCommit(call, req)
		cancel()
		if err == nil {
			// Recorded, whatever happened to the attempt meanwhile.
			return resp, commitAnswer{}, nil
		}
		if ctx.Err() != nil {
			return nil, commitAnswer{}, beforeClaimStopped(context.Cause(ctx))
		}
		ans := mapCommitAnswer(err, preflight, d.BuildID)
		d.logCommitAnswer(req, preflight, ans)
		switch {
		case ans.Then == commitEnd:
			return nil, ans, Fail(ans.Reason, "%s", ans.Message)
		case ans.Then == commitOnceMore && onceMore:
			return nil, ans, Fail(ans.Reason, "%s", ans.Message)
		case ans.Then == commitOnceMore:
			onceMore = true
		}
		delay := identityRetryDelay(n, d.retryMin(), d.recordDraw())
		if clock.Boot()+delay > until {
			if ans.Then == commitOnceMore {
				return nil, ans, Fail(ans.Reason, "%s", ans.Message)
			}
			return nil, ans, nil
		}
		if err := clock.Sleep(ctx, delay); err != nil {
			return nil, ans, beforeClaimStopped(context.Cause(ctx))
		}
	}
}

func (d *OCIDispatcher) logCommitAnswer(req *orkstackv1.CommitResultRequest, preflight bool, ans commitAnswer) {
	attrs := []any{"run", req.GetRunId(), "task", req.GetTaskId(), "attempt", req.GetAttempt(), "preflight", preflight,
		"reason", ans.Reason, "message", ans.Message}
	if ans.Refused {
		d.logger().Error("the engine refused this Orker a task's result", attrs...)
		return
	}
	d.logger().Warn("the engine did not record a task's result", attrs...)
}

// recordThroughEngine is step 13's write for a run whose results the engine
// records: the engine records the manifest, and the result names its
// record. A record already there (this attempt's own lost answer, or
// another attempt that committed first) is adopted through the same §5.4
// checks a retry's commit check applies, and its result reported.
func (d *OCIDispatcher) recordThroughEngine(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32,
	commit *orkstackv1.ActivityCommit, result *orkstackv1.ExecuteTaskResult, trace *memoTrace) (*orkstackv1.ExecuteTaskResult, *Failure) {
	resp, failure := d.commitThroughEngine(ctx, in, attempt, commit)
	if failure != nil {
		return nil, failure
	}
	if !resp.GetCreated() {
		adopted, failure := d.adopt(ctx, in, attempt)
		if failure != nil {
			return nil, failure
		}
		if adopted == nil {
			return nil, Fail(ReasonCommitReadFailed, "the engine answered that a record of task %q is at %s, and reading it found nothing",
				in.GetTaskId(), resp.GetCommit().GetUri())
		}
		return adopted, nil
	}
	if want, _ := artifacts.AttestedCommitURI(in.GetCommitUri()); resp.GetCommit().GetUri() == "" || resp.GetCommit().GetUri() != want {
		return nil, Fail(ReasonCommitEngineUnavailable, "the engine answered that it recorded task %q's result at %q, which is not its record's key",
			in.GetTaskId(), resp.GetCommit().GetUri())
	}
	result.Commit = resp.GetCommit()
	result.Commit.CreatedAt = commit.GetFinishedAt()
	if decision := trace.decision(); shouldWriteMemo(decision) {
		d.writeMemo(ctx, in, memoKey{Key: decision.GetKey(), Components: decision.GetKeyComponents()},
			commit, artifacts.CommitRecord{Commit: commit, SHA256: resp.GetCommit().GetSha256(), ETag: resp.GetCommit().GetEtag()}, trace)
	}
	result.Cache = resultCache(trace.decision())
	return result, nil
}

// checkUpstreamRecord is step 5 for an upstream container task of a run
// whose results the engine records: the dependent stages what the
// upstream's engine record names, and the input's references must be
// exactly those. A record that is not there, is not that task's of this
// run, carries no engine's recording, or names other references is
// UPSTREAM_RESULT_NOT_RECORDED, naming the upstream: an InfrastructureError
// not retried, since no retry of the dependent makes a record appear. A
// store that does not answer is COMMIT_READ_FAILED, retried.
func (d *OCIDispatcher) checkUpstreamRecord(ctx context.Context, in *orkstackv1.ExecuteTaskInput, up *orkstackv1.UpstreamRef) *Failure {
	producer := up.GetTaskId()
	notRecorded := func(format string, args ...any) *Failure {
		return Fail(ReasonUpstreamNotRecorded, "upstream task %q: %s", producer, fmt.Sprintf(format, args...))
	}
	if up.GetCommitUri() == "" {
		// A failed need's ref, for a failure handler, carries artifacts and
		// no values: it is no result, so it has no record. Its shape alone
		// is no proof, since an Orker that completed the upstream chose it,
		// so it is taken only for a need the run recorded as not succeeded,
		// of a task that is a failure handler; anything else is a result and
		// is staged from its record or not at all. (Those artifacts are what
		// the cluster carries, which §3.4 does not close.)
		if up.GetValues() == nil && failedNeedOf(in, producer) {
			return nil
		}
		return notRecorded("it names no commit to read its record by")
	}
	// The record is the producer's own, in this run, in this Orker's store:
	// the uri its result reported is exactly the attested twin of its commit
	// key, or nothing is read.
	uri, err := d.upstreamRecordURI(in, producer)
	if err != nil {
		return notRecorded("this run has no record key for it: %v", err)
	}
	if up.GetCommitUri() != uri {
		return notRecorded("its commit uri %s is not its own record's in this run, %s", up.GetCommitUri(), uri)
	}
	if d.Commits == nil {
		return notRecorded("this Orker has no commit store to read its record from")
	}
	record, err := d.Commits.Get(ctx, uri)
	switch {
	case err == nil:
	case errors.Is(err, artifacts.ErrNotFound):
		return notRecorded("no engine recorded its result at %s", uri)
	case errors.Is(err, artifacts.ErrCommitUndecodable):
		return notRecorded("the object at %s is not a record: %v", uri, err)
	default:
		return FailCause(ReasonCommitReadFailed, err, "upstream task %q: read its record at %s: %v", producer, uri, err)
	}
	rec := record.Commit
	switch {
	case rec.GetRecorded() == nil:
		return notRecorded("the object at %s carries no engine's recording", uri)
	case rec.GetSchemaVersion() != artifacts.CommitSchemaVersion:
		return notRecorded("the record at %s has schema_version %d; this Orker reads %d", uri, rec.GetSchemaVersion(), artifacts.CommitSchemaVersion)
	case rec.GetNamespace() != artifacts.ResolveNamespace(in.GetNamespace()) || rec.GetRunId() != in.GetRunId() || rec.GetTaskId() != producer:
		return notRecorded("the record at %s is of %s/%s/%s", uri, rec.GetNamespace(), rec.GetRunId(), rec.GetTaskId())
	case rec.GetOrkflowDefinitionDigest() != in.GetOrkflowDefinitionDigest():
		return notRecorded("the record at %s is of definition %q, not this run's", uri, rec.GetOrkflowDefinitionDigest())
	case rec.GetIdempotencyKey() != task.IdempotencyKeyFor(in.GetNamespace(), in.GetRunId(), producer):
		return notRecorded("the record at %s is of another activity (idempotency key %q)", uri, rec.GetIdempotencyKey())
	case rec.GetExitCode() != 0:
		return notRecorded("the record at %s is of an exit %d, not a success", uri, rec.GetExitCode())
	case !sameRef(up.GetValues(), rec.GetValues()):
		return notRecorded("the input's values reference is not the one its record names")
	case len(up.GetArtifacts()) != len(rec.GetArtifacts()):
		return notRecorded("the input names %d artifacts and its record %d", len(up.GetArtifacts()), len(rec.GetArtifacts()))
	}
	for i, a := range up.GetArtifacts() {
		if !sameRef(a, rec.GetArtifacts()[i]) {
			return notRecorded("the input's artifact %d is not the one its record names", i)
		}
	}
	return nil
}

// upstreamRecordURI is the uri of producer's engine record in in's run, from
// the run's coordinates alone. Its root is this Orker's commit root, or where
// the Orker was given none (it serves every namespace), the root this task's
// own commit uri lies under.
func (d *OCIDispatcher) upstreamRecordURI(in *orkstackv1.ExecuteTaskInput, producer string) (string, error) {
	key, err := artifacts.CommitKey(in.GetNamespace(), in.GetRunId(), producer)
	if err != nil {
		return "", err
	}
	root := strings.TrimSuffix(d.CommitRoot, "/")
	if root == "" {
		own, err := commitKeyOf(in.GetCommitUri())
		if err != nil {
			return "", err
		}
		root = strings.TrimSuffix(in.GetCommitUri(), "/"+own)
	}
	return artifacts.AttestedCommitURI(root + "/" + key)
}

// sameRef reports whether two artifact references name the same object:
// its name, its uri, its digest and its size.
func sameRef(a, b *orkstackv1.ArtifactRef) bool {
	return a.GetName() == b.GetName() && a.GetUri() == b.GetUri() && a.GetSha256() == b.GetSha256() && a.GetSizeBytes() == b.GetSizeBytes()
}

// failedNeedOf reports whether producer is a need, recorded as not
// succeeded, of a task that is a failure handler (join any_failed or
// all_done): the only upstream whose ref may name no commit.
func failedNeedOf(in *orkstackv1.ExecuteTaskInput, producer string) bool {
	join := in.GetJoin()
	switch join.GetPolicy() {
	case orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED, orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE:
	default:
		return false
	}
	for _, need := range join.GetNeeds() {
		if need.GetTaskId() == producer {
			return need.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED
		}
	}
	return false
}
