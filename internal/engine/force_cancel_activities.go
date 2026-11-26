package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ForceCancelKeys is what a force cancel signs and verifies with: the
// deployment's decision keys (identity.Issuer).
type ForceCancelKeys interface {
	RecordSigner
	ForceCancelVerifier
	UnsignedDecision(mode string) identity.UnsignedVerdict
}

// ForceCancelCounter counts force cancels by what became of them: a finished
// one's state, and what a request found named as one of its run
// (FCCountUnsigned and the others).
type ForceCancelCounter interface {
	CountForceCancel(ctx context.Context, outcome string)
}

const (
	FCCountUnsigned   = "unsigned_left"     // nobody signed it: it was left alone
	FCCountUnknown    = "unknown_execution" // it could not be judged: left alone
	FCCountSquatter   = "squatter"          // forged: it was ended
	FCCountSuperseded = "superseded"
	FCCountDirect     = "direct" // a run ended by the request itself
)

// ForceCancelActivities are the force cancel's local activities, on the
// engine worker, which takes no activity task (LocalActivityWorkerOnly): they
// run only in workflow code an engine executes. None rests on that: every one
// that acts, signs or records verifies the decision itself, checks it runs in
// that decision's own force cancel (decide), and takes every value from what
// verified.
type ForceCancelActivities struct {
	client    client.Client
	namespace string // the cluster's namespace
	starter   *TemporalStarter
	reader    *TemporalRunReader
	snapshots SnapshotStore
	keys      ForceCancelKeys
	mode      func() string
	audit     artifacts.AuditStore
	settings  ForceCancelSettings
	counter   ForceCancelCounter
	now       func() time.Time
	// self is the execution the activity runs in, as the cluster says it.
	self func(ctx context.Context) (forceCancelSelf, error)

	refusals refusalCap
}

// forceCancelSelf is an execution by the cluster's own record of it.
type forceCancelSelf struct {
	WorkflowID, RunID, Type string
	StartedAt               time.Time
	// Local: the activity runs inside the workflow code of an engine, and is
	// not one scheduled on a channel.
	Local bool
	// TaskQueue is the channel its workflow code runs on.
	TaskQueue string
	// Parent is the execution that started it as its child, when one did:
	// the cluster's own word, which no plain start can have.
	Parent *commonpb.WorkflowExecution
}

// ForceCancelDeps are what NewForceCancelActivities is wired with.
type ForceCancelDeps struct {
	Client           client.Client
	ClusterNamespace string
	Starter          *TemporalStarter
	Reader           *TemporalRunReader
	Snapshots        SnapshotStore
	Keys             ForceCancelKeys
	// Mode is the deployment's authentication mode: the higher of the
	// engine's own and the mode record's.
	Mode  func() string
	Audit artifacts.AuditStore
	// Counter counts force cancels; nil counts none.
	Counter ForceCancelCounter
	// Settings are this engine's, which a cascade's own force cancels are
	// decided with.
	Settings ForceCancelSettings
}

// NewForceCancelActivities returns the activities over d.
func NewForceCancelActivities(d ForceCancelDeps) *ForceCancelActivities {
	a := &ForceCancelActivities{client: d.Client, namespace: d.ClusterNamespace, starter: d.Starter,
		reader: d.Reader, snapshots: d.Snapshots, keys: d.Keys, mode: d.Mode, audit: d.Audit, settings: d.Settings, counter: d.Counter, now: time.Now}
	a.self = a.ownExecution
	return a
}

// RegisterForceCancelActivities registers them on the engine worker, which
// runs them as local activities only.
func RegisterForceCancelActivities(w ActivityRegistrar, a *ForceCancelActivities) {
	for name, fn := range map[string]any{
		ActivityFCVerify: a.Verify, ActivityFCRefuse: a.Refuse, ActivityFCAudit: a.Audit, ActivityFCDescribe: a.Describe,
		ActivityFCCancel: a.Cancel, ActivityFCScan: a.Scan, ActivityFCGates: a.Gates,
		ActivityFCTerminate: a.Terminate, ActivityFCClassify: a.Classify,
		ActivityFCBelow: a.Below, ActivityFCDerive: a.Derive, ActivityFCOpen: a.Open, ActivityFCAfter: a.After, ActivityFCSummary: a.Summary,
	} {
		w.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
}

// RegisterForceCancelPublish registers the ending's build on the engines' own
// event channel (EventQueue), which no Orker polls. No activity that signs or
// terminates may ever be registered there.
func RegisterForceCancelPublish(w ActivityRegistrar, a *ForceCancelActivities) {
	w.RegisterActivityWithOptions(a.Publish, activity.RegisterOptions{Name: ActivityFCPublish})
}

func (a *ForceCancelActivities) authMode() string {
	if a.mode == nil {
		return ""
	}
	return a.mode()
}

// ownExecution is the execution this activity runs in, described: neither its
// type nor its start can be set by whoever started it.
func (a *ForceCancelActivities) ownExecution(ctx context.Context) (forceCancelSelf, error) {
	running := activity.GetInfo(ctx)
	exec := running.WorkflowExecution
	d, err := a.described(ctx, exec.ID, exec.RunID)
	if err != nil {
		return forceCancelSelf{}, fmt.Errorf("force cancel: describe its own execution: %w", err)
	}
	info := d.GetWorkflowExecutionInfo()
	return forceCancelSelf{WorkflowID: exec.ID, RunID: exec.RunID, Type: info.GetType().GetName(), StartedAt: info.GetStartTime().AsTime(),
		Local: running.IsLocalActivity, TaskQueue: info.GetTaskQueue(), Parent: info.GetParentExecution()}, nil
}

// described is an execution as the cluster describes it; run "" is the
// workflow id's current one.
func (a *ForceCancelActivities) described(ctx context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	d, err := a.client.DescribeWorkflow(ctx, id, run)
	if err != nil {
		return nil, err
	}
	if d == nil || d.RawResponse == nil {
		return nil, fmt.Errorf("force cancel: the cluster described %s with nothing", id)
	}
	return d.RawResponse, nil
}

// SignForceCancelDecision is the decision, signed, as the API hands it to the
// force cancel's start. One whose record would not fit the termination is
// refused before anything is asked of the run.
func SignForceCancelDecision(signer RecordSigner, d ForceCancelDecision) (string, error) {
	if err := d.check(); err != nil {
		return "", err
	}
	if _, err := SignForceCancelRecord(signer, d.record(strings.Repeat("0", 36), true, d.ExpiresAt)); err != nil {
		return "", err
	}
	body, err := stablejson.Marshal(d)
	if err != nil {
		return "", fmt.Errorf("force cancel decision: %w", err)
	}
	return signer.SignDecision(d.kind(), append(d.bind(), body...))
}

// kind is what d is signed as, and bind what its body begins with: its run,
// and of a cascaded one the run its cascade began at.
func (d ForceCancelDecision) kind() jwt.DecisionKind {
	if d.Of != nil {
		return jwt.KindForceCancelDerived
	}
	return jwt.KindForceCancelRoot
}

func (d ForceCancelDecision) bind() []byte {
	if d.Of != nil {
		return identity.Bind(d.Namespace, d.RunID, d.Of.RunID)
	}
	return forceCancelBind(d.Namespace, d.RunID)
}

// What a decision may ask, far past any setting: one nobody signed may say
// anything. The grace is well above how far clocks may differ, and a wait is
// at most forceCancelMaxWindows windows, which its history holds.
const (
	forceCancelMinGrace   = 30 * time.Second
	forceCancelMaxCeiling = 24 * time.Hour
	forceCancelMaxWindows = 500
	forceCancelMaxLife    = 24 * time.Hour
)

// check holds a decision to what the force cancel can act on.
func (d ForceCancelDecision) check() error {
	if d.Namespace == "" || d.RunID == "" || d.TargetExecutionRunID == "" {
		return errors.New("force cancel decision: it names no run")
	}
	if d.GraceSeconds < int64(forceCancelMinGrace/time.Second) || d.CeilingSeconds < d.GraceSeconds ||
		d.CeilingSeconds > int64(forceCancelMaxCeiling/time.Second) || d.CeilingSeconds > forceCancelMaxWindows*d.GraceSeconds {
		return errors.New("force cancel decision: its grace or ceiling is out of bounds")
	}
	if life := d.ExpiresAt.Sub(d.IssuedAt); life > forceCancelMaxLife {
		return errors.New("force cancel decision: it lives too long")
	}
	if !d.IssuedAt.Before(d.ExpiresAt) {
		return errors.New("force cancel decision: it expires before it was issued")
	}
	if err := checkForceCancelText(forceCancelText{"namespace", d.Namespace, 200}, forceCancelText{"run_id", d.RunID, 256},
		forceCancelText{"target_execution_run_id", d.TargetExecutionRunID, 64}); err != nil {
		return err
	}
	if d.Of != nil {
		if d.Cascade || d.Of.RunID == "" || d.Of.WorkflowID == "" || d.Of.Execution == "" {
			return errors.New("force cancel decision: a cascaded one names no cascade, or is one")
		}
		if err := checkForceCancelText(forceCancelText{"of.run_id", d.Of.RunID, 256}, forceCancelText{"of.workflow_id", d.Of.WorkflowID, 256},
			forceCancelText{"of.execution", d.Of.Execution, 64}); err != nil {
			return err
		}
	}
	return d.record(strings.Repeat("0", 36), false, d.IssuedAt).check()
}

// record is the record a termination of execution exec under d carries.
func (d ForceCancelDecision) record(exec string, atCeiling bool, at time.Time) ForceCancelRecord {
	return ForceCancelRecord{
		Namespace: d.Namespace, RunID: d.RunID, TerminatedExecutionRunID: exec,
		RequestID: d.RequestID, WorkflowID: d.WorkflowID(),
		Principal: d.Principal, PrincipalVerified: d.PrincipalVerified, AuthMethod: d.AuthMethod,
		PrincipalIssuer: d.PrincipalIssuer, Permission: d.Permission, Scope: d.Scope, Rule: d.Rule,
		PolicyGeneration: d.PolicyGeneration, Reason: d.Reason, GraceSeconds: d.GraceSeconds,
		ProgressingAtCeiling: atCeiling, At: at,
	}
}

// decide is the decision in holds, when it verifies and self is the one
// execution it acts in; otherwise why not, as a cause. An error is no answer:
// the keys or the cluster could not be read.
func (a *ForceCancelActivities) decide(ctx context.Context, in ForceCancelInput, self forceCancelSelf) (*ForceCancelDecision, string, error) {
	var d ForceCancelDecision
	switch {
	case (in.Decision == "") != (in.Derived == "") && in.Unsigned == nil:
		if a.keys == nil {
			return nil, "", errors.New("force cancel: no keys to verify the decision with")
		}
		// The run the body is bound to is read from the body, and held to it
		// below: a decision has no other source of its run.
		kind, parts := jwt.KindForceCancelRoot, 2
		if in.Derived != "" {
			kind, parts = jwt.KindForceCancelDerived, 3
		}
		body, _, verdict := a.keys.VerifyDecision(ctx, in.Decision+in.Derived, kind, self.StartedAt, identity.NoBinding)
		switch verdict {
		case identity.VerdictValid:
		case identity.VerdictUnavailable:
			return nil, "", errors.New("force cancel: the keys to verify the decision could not be read")
		default:
			return nil, FCCauseDecisionUnverified, nil
		}
		plain, ok := unbound(body, parts)
		if !ok || json.Unmarshal(plain, &d) != nil || d.kind() != kind || !bytes.Equal(body[:len(body)-len(plain)], d.bind()) {
			return nil, FCCauseDecisionUnverified, nil
		}
	case in.Unsigned != nil && in.Decision == "" && in.Derived == "":
		if a.keys == nil {
			return nil, "", errors.New("force cancel: no keys to judge an unsigned decision with")
		}
		switch a.keys.UnsignedDecision(a.authMode()) {
		case identity.UnsignedAsserted:
		case identity.UnsignedUnavailable:
			return nil, "", errors.New("force cancel: the deployment's mode could not be read")
		default:
			return nil, FCCauseDecisionUnverified, nil
		}
		d = *in.Unsigned
		if d.PrincipalVerified || d.AuthMethod != "" || d.PrincipalIssuer != "" || d.Rule != "" || d.PolicyGeneration != 0 {
			// Nobody verified anyone or decided by any policy: no record may
			// say so.
			return nil, FCCauseDecisionUnverified, nil
		}
	default:
		return nil, FCCauseDecisionUnverified, nil
	}
	if d.check() != nil || self.Type != ForceCancelWorkflowType || self.WorkflowID != d.WorkflowID() || !self.Local {
		return nil, FCCauseDecisionUnverified, nil
	}
	// A cascaded one acts only as the child its cascade started.
	if d.Of != nil && (self.Parent.GetWorkflowId() != d.Of.WorkflowID || self.Parent.GetRunId() != d.Of.Execution) {
		return nil, FCCauseDecisionUnverified, nil
	}
	// The decision authorizes a force cancel started while it was fresh, and
	// what that one execution does until its own end: a termination it sent is
	// always finished.
	// It is started at once, so a decision copied from an ended one starts
	// nothing later.
	if !self.StartedAt.Before(d.ExpiresAt) || self.StartedAt.After(d.IssuedAt.Add(forceCancelStartWithin)) {
		return nil, FCCauseDecisionStale, nil
	}
	return &d, "", nil
}

// forceCancelStartWithin is how long after its decision, by an engine's clock,
// a force cancel is started, by the cluster's.
const forceCancelStartWithin = 10 * time.Minute

// unbound is a decision's body past what it is bound to: that many parts, each
// as its length in four bytes and its bytes (identity.Bind).
func unbound(body []byte, parts int) ([]byte, bool) {
	for range parts {
		if len(body) < 4 {
			return nil, false
		}
		n := binary.BigEndian.Uint32(body)
		if uint64(len(body)-4) < uint64(n) {
			return nil, false
		}
		body = body[4+n:]
	}
	return body, true
}

// acting is decide for a step that acts: anything but a decision is an error
// the workflow reads as the step failing.
func (a *ForceCancelActivities) acting(ctx context.Context, in ForceCancelInput) (ForceCancelDecision, error) {
	self, err := a.self(ctx)
	if err != nil {
		return ForceCancelDecision{}, err
	}
	return a.actingAs(ctx, in, self)
}

func (a *ForceCancelActivities) actingAs(ctx context.Context, in ForceCancelInput, self forceCancelSelf) (ForceCancelDecision, error) {
	d, cause, err := a.decide(ctx, in, self)
	if err != nil {
		return ForceCancelDecision{}, err
	}
	if d == nil {
		return ForceCancelDecision{}, errForceCancelRefused(cause)
	}
	return *d, nil
}

// Verify is step 0: the decision verifies in this execution, and the run is
// the one it was made for, in the namespace it names, not resumed since.
func (a *ForceCancelActivities) Verify(ctx context.Context, in ForceCancelInput) (FCVerified, error) {
	self, err := a.self(ctx)
	if err != nil {
		return FCVerified{}, err
	}
	d, cause, err := a.decide(ctx, in, self)
	if err != nil && activity.IsActivity(ctx) && int(activity.GetInfo(ctx).Attempt) < in.timings().StepAttempts {
		return FCVerified{}, err
	}
	if err != nil {
		activity.GetLogger(ctx).Warn("force cancel: its decision could not be verified", "error", err)
		return FCVerified{Cause: FCCauseKeysUnavailable}, nil
	}
	if d == nil {
		return FCVerified{Cause: cause}, nil
	}
	if cause, err := a.targets(ctx, *d); err != nil || cause != "" {
		return FCVerified{Cause: cause}, err
	}
	return FCVerified{Decision: d}, nil
}

// forceCancelLineageHops bounds the walk back over a run's executions.
const forceCancelLineageHops = 64

// targets reports why d does not name the run as it is now: the run is
// another namespace's, or no run; or d was made for an execution the current
// one does not carry on from, or a reset has made it another since (design
// §22.26.16 N4). A resume is such a reset; a run that carried on is not.
func (a *ForceCancelActivities) targets(ctx context.Context, d ForceCancelDecision) (string, error) {
	exec := ""
	for hop := 0; hop < forceCancelLineageHops; hop++ {
		described, err := a.described(ctx, d.RunID, exec)
		var gone *serviceerror.NotFound
		if errors.As(err, &gone) {
			return FCCauseDecisionStale, nil
		}
		if err != nil {
			return "", fmt.Errorf("force cancel: describe run %s: %w", d.RunID, err)
		}
		info, extended := described.GetWorkflowExecutionInfo(), described.GetWorkflowExtendedInfo()
		if info.GetType().GetName() != RunWorkflowType {
			return FCCauseDecisionUnverified, nil
		}
		if at := extended.GetLastResetTime(); at != nil && at.AsTime().After(d.IssuedAt) {
			return FCCauseDecisionStale, nil
		}
		exec = info.GetExecution().GetRunId()
		first, err := a.firstEvent(ctx, d.RunID, exec)
		if err != nil {
			return "", err
		}
		started := first.GetWorkflowExecutionStartedEventAttributes()
		if hop == 0 {
			namespace, err := a.reader.runNamespace(ctx, d.RunID, started)
			if err != nil {
				return "", err
			}
			if artifacts.ResolveNamespace(namespace) != artifacts.ResolveNamespace(d.Namespace) {
				return FCCauseDecisionUnverified, nil
			}
		}
		if exec == d.TargetExecutionRunID {
			return "", nil
		}
		if exec = started.GetContinuedExecutionRunId(); exec == "" {
			return FCCauseDecisionStale, nil
		}
	}
	return FCCauseDecisionStale, nil
}

// firstEvent is an execution's start, and nothing after it.
func (a *ForceCancelActivities) firstEvent(ctx context.Context, runID, exec string) (*historypb.HistoryEvent, error) {
	resp, err := a.client.WorkflowService().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: a.namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: exec}, MaximumPageSize: 1})
	if err != nil {
		return nil, fmt.Errorf("force cancel: read the start of run %s: %w", runID, err)
	}
	if len(resp.GetHistory().GetEvents()) == 0 {
		return nil, fmt.Errorf("force cancel: run %s: execution %s has no start", runID, exec)
	}
	return resp.GetHistory().GetEvents()[0], nil
}

// refusalCap bounds how many refused decisions one engine records: a refused
// one is anyone's, and as many as they like.
type refusalCap struct {
	mu   sync.Mutex
	last time.Time
}

// forceCancelRefusalEvery is how often a refusal is recorded at most.
const forceCancelRefusalEvery = time.Minute

// write writes one unless one was written, or is being written, within the
// minute. A write that fails is not one, and nothing waits for a slow one.
func (c *refusalCap) write(now time.Time, put func() error) (written bool, err error) {
	c.mu.Lock()
	before := c.last
	if !before.IsZero() && now.Sub(before) < forceCancelRefusalEvery {
		c.mu.Unlock()
		return false, nil
	}
	c.last = now
	c.mu.Unlock()
	if err := put(); err != nil {
		c.mu.Lock()
		if c.last.Equal(now) {
			c.last = before
		}
		c.mu.Unlock()
		return false, err
	}
	return true, nil
}

// Refuse records that this execution's decision was refused: by its own
// workflow id, with nothing its input claims, and at most one a minute.
func (a *ForceCancelActivities) Refuse(ctx context.Context, cause string) error {
	self, err := a.self(ctx)
	if err != nil {
		return err
	}
	if self.Type != ForceCancelWorkflowType || !self.Local || !strings.HasPrefix(self.WorkflowID, "ork-force-") {
		return errors.New("force cancel: not a force cancel's own step")
	}
	switch cause {
	case FCCauseDecisionUnverified, FCCauseDecisionStale:
	default:
		cause = FCCauseDecisionUnverified
	}
	if a.audit == nil {
		return errors.New("force cancel: no audit store")
	}
	target := printable(self.WorkflowID)
	target = target[:min(len(target), artifacts.AuditTargetMaxBytes)]
	rec := &orkstackv1.AuditRecord{
		Id: forceCancelAuditID(forceCancelRefused, "", "", self.WorkflowID, self.StartedAt), At: timestamppb.New(self.StartedAt),
		Action: forceCancelRefused, Target: target,
		Detail: "a force cancel's decision was refused", ErrorCode: cause, Permission: forceCancelPermission,
	}
	written, err := a.refusals.write(a.now(), func() error { return a.put(ctx, rec) })
	if err == nil && !written {
		slog.Warn("force cancel: a refused decision is not recorded: one was this minute", "workflow_id", target, "cause", cause)
	}
	return err
}

// printable is id, which anyone may have chosen, with nothing but ASCII in it.
func printable(id string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return -1
		}
		return r
	}, id)
}

const (
	forceCancelRefused    = "run.cancel.force.decision_refused"
	forceCancelPermission = "run.cancel.force"
)

// put writes one record in the schema the deployment's mode writes: observe
// and enforce carry the verified principal and the decision. One already
// written is written.
func (a *ForceCancelActivities) put(ctx context.Context, rec *orkstackv1.AuditRecord) error {
	_, err := a.putNew(ctx, rec)
	return err
}

// putNew is put, and whether the record was not there before.
func (a *ForceCancelActivities) putNew(ctx context.Context, rec *orkstackv1.AuditRecord) (bool, error) {
	rec.SchemaVersion = artifacts.AuditSchemaVersion
	if m := a.authMode(); m == "observe" || m == "enforce" {
		rec.SchemaVersion = artifacts.AuditSchemaVersionVerified
	}
	err := a.audit.PutAuditRecord(ctx, rec)
	if errors.Is(err, artifacts.ErrAuditRecordExists) {
		return false, nil
	}
	return err == nil, err
}

// forceCancelAuditID is a force cancel's audit id for kind: the same request
// writes the same id however often a step is retried, so a record is
// written once.
func forceCancelAuditID(kind, namespace, runID, requestID string, at time.Time) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + namespace + "\x00" + runID + "\x00" + requestID))
	return "aud-" + artifacts.RunStamp(at) + "-" + hex.EncodeToString(sum[:])[:16]
}

// Audit writes one record of the force cancel, once, from its verified
// decision. Its id and time are the decision's, so a genuine one started again
// writes nothing new; when the force cancel ended is in the completed one's
// detail.
func (a *ForceCancelActivities) Audit(ctx context.Context, in FCAuditInput) error {
	self, err := a.self(ctx)
	if err != nil {
		return err
	}
	decided, cause, err := a.decide(ctx, in.Input, self)
	if err != nil {
		return err
	}
	if decided == nil {
		return errForceCancelRefused(cause)
	}
	if a.audit == nil {
		return errors.New("force cancel: no audit store")
	}
	rec, err := a.auditRecord(in.Kind, *decided, in.Result)
	if err != nil {
		return err
	}
	written, err := a.putNew(ctx, rec)
	if written && in.Kind == forceCancelCompleted && a.counter != nil {
		switch in.Result.State {
		case FCStateForceCanceled, FCStateCanceled, FCStateEnded, FCStateAlreadyEnded, FCStateFailed:
			a.counter.CountForceCancel(ctx, in.Result.State)
		}
	}
	return err
}

// auditRecord is the record of kind of a force cancel under d that ended in
// result.
func (a *ForceCancelActivities) auditRecord(kind string, d ForceCancelDecision, result ForceCancelResult) (*orkstackv1.AuditRecord, error) {
	rec := &orkstackv1.AuditRecord{
		Id: forceCancelAuditID(kind, d.Namespace, d.RunID, d.RequestID, d.IssuedAt), At: timestamppb.New(d.IssuedAt),
		Principal: d.Principal, PrincipalVerified: d.PrincipalVerified, AuthMethod: d.AuthMethod, PrincipalIssuer: d.PrincipalIssuer,
		Action: kind, Namespace: d.Namespace, Target: d.RunID, Permission: d.Permission, Scope: d.Scope, Rule: d.Rule,
	}
	switch kind {
	case forceCancelRequested:
		rec.Succeeded, rec.Detail = true, "request "+d.RequestID+": "+d.Reason
	case forceCancelCompleted:
		rec.Succeeded, rec.ErrorCode = result.succeeded(), result.Cause
		rec.Detail = "request " + d.RequestID + ", at " + a.now().UTC().Format(time.RFC3339) + ": " + result.detail()
	default:
		return nil, fmt.Errorf("force cancel: %q is no record of one", kind)
	}
	return rec, nil
}

// Describe is the run as the cluster says it is now.
func (a *ForceCancelActivities) Describe(ctx context.Context, in FCStep) (FCDescribed, error) {
	d, err := a.acting(ctx, in.Input)
	if err != nil {
		return FCDescribed{}, err
	}
	return a.describe(ctx, d.RunID)
}

func (a *ForceCancelActivities) describe(ctx context.Context, runID string) (FCDescribed, error) {
	d, err := a.described(ctx, runID, "")
	if err != nil {
		return FCDescribed{}, fmt.Errorf("force cancel: describe run %s: %w", runID, err)
	}
	info := d.GetWorkflowExecutionInfo()
	return FCDescribed{Open: info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		ExecutionRunID: info.GetExecution().GetRunId()}, nil
}

// forceCancelScanMax and forceCancelPageMax bound one read of a run's history
// whatever the input says.
const (
	forceCancelScanMax = 20_000
	forceCancelPageMax = 1_000
)

func (in ForceCancelInput) budget() int { return min(max(in.ScanEvents, 1), forceCancelScanMax) }

func (in ForceCancelInput) page() int32 { return int32(min(max(in.PageSize, 1), forceCancelPageMax)) }

// Cancel is step 3: the run's ordinary cancel under the caller's name, and
// the event the cluster recorded it at in the run's current execution. Where
// that event is older than the scan budget reaches, l0 is the oldest event
// read, so what the run did since shows as progress.
func (a *ForceCancelActivities) Cancel(ctx context.Context, in FCStep) (FCCanceled, error) {
	d, err := a.acting(ctx, in.Input)
	if err != nil {
		return FCCanceled{}, err
	}
	if cause, err := a.targets(ctx, d); err != nil || cause != "" {
		return FCCanceled{Cause: cause}, err
	}
	err = a.starter.CancelRun(ctx, d.RunID, CancelRequest{Principal: d.Principal, Verified: d.PrincipalVerified})
	var gone *serviceerror.NotFound
	var already *serviceerror.CancellationAlreadyRequested
	if errors.As(err, &already) {
		err = nil
	}
	if err != nil && !errors.As(err, &gone) {
		return FCCanceled{}, err
	}
	now, derr := a.describe(ctx, d.RunID)
	if derr != nil {
		return FCCanceled{}, derr
	}
	if !now.Open {
		return FCCanceled{Closed: true, Asked: err == nil, ExecutionRunID: now.ExecutionRunID}, nil
	}
	if err != nil {
		return FCCanceled{}, err // it closed and another opened between the two calls
	}
	events, err := a.newestEvents(ctx, d.RunID, now.ExecutionRunID, in.Input.budget(), in.Input.page(), func(e *historypb.HistoryEvent) bool {
		return e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	})
	if err != nil {
		return FCCanceled{}, err
	}
	if len(events) == 0 {
		return FCCanceled{}, fmt.Errorf("force cancel: run %s: execution %s has no events", d.RunID, now.ExecutionRunID)
	}
	return FCCanceled{L0: events[len(events)-1].GetEventId(), ExecutionRunID: now.ExecutionRunID, Asked: true}, nil
}

// newestEvents reads execution's events newest first, until one stops it,
// limit, or the first.
func (a *ForceCancelActivities) newestEvents(ctx context.Context, runID, execution string, limit int, page int32, stop func(*historypb.HistoryEvent) bool) ([]*historypb.HistoryEvent, error) {
	var out []*historypb.HistoryEvent
	var token []byte
	for {
		resp, err := a.client.WorkflowService().GetWorkflowExecutionHistoryReverse(ctx,
			&workflowservice.GetWorkflowExecutionHistoryReverseRequest{
				Namespace:       a.namespace,
				Execution:       &commonpb.WorkflowExecution{WorkflowId: runID, RunId: execution},
				MaximumPageSize: page,
				NextPageToken:   token,
			})
		if err != nil {
			return nil, fmt.Errorf("force cancel: read history of run %s: %w", runID, err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			out = append(out, e)
			if len(out) == limit || stop(e) {
				return out, nil
			}
		}
		if token = resp.GetNextPageToken(); len(token) == 0 {
			return out, nil
		}
	}
}

// Scan is step 5's read and decision (ForceCancelProgress), of the execution
// the run is in now.
func (a *ForceCancelActivities) Scan(ctx context.Context, in FCStep) (FCScanned, error) {
	d, err := a.acting(ctx, in.Input)
	if err != nil {
		return FCScanned{}, err
	}
	now, err := a.describe(ctx, d.RunID)
	if err != nil {
		return FCScanned{}, err
	}
	out := FCScanned{ExecutionRunID: now.ExecutionRunID, Open: now.Open}
	if !now.Open || now.ExecutionRunID != in.ExecutionRunID {
		out.Stall.Progressing = now.Open
		return out, nil
	}
	events, err := a.newestEvents(ctx, d.RunID, in.ExecutionRunID, in.Input.budget(), in.Input.page(), func(e *historypb.HistoryEvent) bool {
		return e.GetEventId() <= in.L0+1
	})
	if err != nil {
		return FCScanned{}, err
	}
	out.Stall = ForceCancelProgress(events, in.L0)
	return out, nil
}

// Gates is step 6: the gates the execution holds or queues at, from its own
// search attributes, and the seq_base its entries carry, from its memo. What cannot
// be decoded is said: a release of the rest is not a release of it all.
func (a *ForceCancelActivities) Gates(ctx context.Context, in FCStep) (FCGates, error) {
	d, err := a.acting(ctx, in.Input)
	if err != nil {
		return FCGates{}, err
	}
	described, err := a.described(ctx, d.RunID, in.ExecutionRunID)
	if err != nil {
		return FCGates{}, fmt.Errorf("force cancel: describe run %s: %w", d.RunID, err)
	}
	info := described.GetWorkflowExecutionInfo()
	return forceCancelGates(info.GetSearchAttributes(), info.GetMemo()), nil
}

// forceCancelGates is Gates over a run's own record.
func forceCancelGates(attrs *commonpb.SearchAttributes, memo *commonpb.Memo) FCGates {
	var out FCGates
	for _, name := range []string{task.SearchAttrOrkflowConcurrencyHeld, task.SearchAttrOrkflowConcurrencyQueued} {
		p := attrs.GetIndexedFields()[name]
		if p == nil {
			continue
		}
		var ids []string
		if err := DataConverter().FromPayload(p, &ids); err != nil {
			out.Unreadable = true
			continue
		}
		out = out.and(FCGates{Gates: ids})
	}
	entries := 0
	for key, p := range memo.GetFields() {
		if !strings.HasPrefix(key, gateMemoPrefix) || key == gateMemoSeqKey {
			continue
		}
		var e gateMemoEntry
		if err := DataConverter().FromPayload(p, &e); err != nil {
			out.Unreadable = true
			continue
		}
		entries++
		out.SeqBase = max(out.SeqBase, e.SeqBase)
	}
	// A gate with no entry to say under which seq_base cannot be told which
	// entries are the run's.
	out.Unreadable = out.Unreadable || len(out.Gates) > 0 && entries == 0
	return out
}

// gateMemoPrefix is what every concurrency memo key begins with (gateMemoKey).
const gateMemoPrefix = "orkflow_conc_"

// Terminate is step 7: the run is still the one the decision was made for,
// its record is signed just before the call, and exactly that execution is
// terminated. Whatever the call answers, the workflow decides by describing
// the run again; a cause, and Unsent, say this call was not made.
func (a *ForceCancelActivities) Terminate(ctx context.Context, in FCStep) (FCTerminated, error) {
	d, details, out, err := a.signed(ctx, in)
	if refused(err) {
		return FCTerminated{}, err
	}
	if err != nil {
		return FCTerminated{Unsent: err.Error()}, nil
	}
	if out != (FCTerminated{}) {
		return out, nil
	}
	_, err = a.client.WorkflowService().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         a.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: d.RunID, RunId: in.ExecutionRunID},
		Reason:            ForceCancelTerminateReason,
		Details:           details,
		Identity:          "orkstack force cancel " + d.RequestID,
	})
	var gone *serviceerror.NotFound
	switch {
	case errors.As(err, &gone):
		// Already closed: what closed it is the classification's to read.
		return FCTerminated{}, nil
	case err != nil && !maybeSent(err):
		return FCTerminated{Unsent: err.Error()}, nil
	}
	return FCTerminated{Sent: err == nil}, err
}

// maybeSent reports whether a call that failed with err may have been carried
// out: its answer was lost, or the cluster failed over it. Any other failure
// is the cluster refusing it.
func maybeSent(err error) bool {
	switch serviceerror.ToStatus(err).Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Internal, codes.Unknown, codes.Aborted, codes.DataLoss:
		return true
	}
	return false
}

// signed is the record a termination carries, signed, or why none may be.
func (a *ForceCancelActivities) signed(ctx context.Context, in FCStep) (d ForceCancelDecision, details *commonpb.Payloads, not FCTerminated, err error) {
	if d, err = a.acting(ctx, in.Input); err != nil {
		return d, nil, not, err
	}
	if not.Cause, err = a.targets(ctx, d); err != nil || not != (FCTerminated{}) {
		return d, nil, not, err
	}
	if not, err = a.stalled(ctx, in, d); err != nil || not != (FCTerminated{}) {
		return d, nil, not, err
	}
	rec := d.record(in.ExecutionRunID, in.ProgressingAtCeiling, a.now().UTC())
	if in.Input.Unsigned == nil {
		details, err = SignForceCancelRecord(a.keys, rec)
	} else {
		details, err = UnsignedForceCancelRecord(rec)
	}
	switch {
	case errors.Is(err, ErrForceCancelRecordTooLarge):
		return d, nil, FCTerminated{Cause: FCCauseRecordTooLarge}, nil
	case errors.Is(err, ErrForceCancelText):
		return d, nil, FCTerminated{Cause: FCCauseTextRefused}, nil
	}
	return d, details, not, err
}

// stalled reads the stall test's own window again and answers why the
// termination's record may not be signed: the run moved, or this engine's
// clock does not agree that the wait was as long as the record would say.
func (a *ForceCancelActivities) stalled(ctx context.Context, in FCStep, d ForceCancelDecision) (FCTerminated, error) {
	if in.ProgressingAtCeiling {
		self, err := a.self(ctx)
		if err == nil && a.now().Sub(self.StartedAt) < d.Ceiling()-forceCancelSkew {
			return FCTerminated{Cause: FCCauseClockSkew}, nil
		}
		return FCTerminated{}, err
	}
	// One more than Scan reads: l0 itself, for its time.
	events, err := a.newestEvents(ctx, d.RunID, in.ExecutionRunID, in.Input.budget()+1, in.Input.page(), func(e *historypb.HistoryEvent) bool {
		return e.GetEventId() <= in.L0
	})
	if err != nil {
		return FCTerminated{}, err
	}
	moved, early := forceCancelStalled(events, in.L0, d.Grace(), a.now())
	if early {
		return FCTerminated{Cause: FCCauseClockSkew}, nil
	}
	return FCTerminated{Progressing: moved}, nil
}

// closeOf is the close of the run's execution, or its current one, as its
// verified record reads, nil while it is open.
func (a *ForceCancelActivities) closeOf(ctx context.Context, d ForceCancelDecision, execution string) (enumspb.WorkflowExecutionStatus, ForceCancelClose, error) {
	described, err := a.described(ctx, d.RunID, execution)
	if err != nil {
		return 0, ForceCancelClose{}, fmt.Errorf("force cancel: describe run %s: %w", d.RunID, err)
	}
	info := described.GetWorkflowExecutionInfo()
	if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		return info.GetStatus(), ForceCancelClose{}, nil
	}
	exec := info.GetExecution().GetRunId()
	events, err := a.newestEvents(ctx, d.RunID, exec, 1, 1, func(*historypb.HistoryEvent) bool { return true })
	if err != nil {
		return 0, ForceCancelClose{}, err
	}
	if len(events) == 0 {
		return 0, ForceCancelClose{}, fmt.Errorf("force cancel: run %s closed with no close event", d.RunID)
	}
	return info.GetStatus(), ReadForceCancelClose(ctx, a.keys, a.authMode(), d.Namespace, d.RunID, exec, events[0],
		events[0].GetEventTime().AsTime()), nil
}

// Classify is step 9: how the run ended, by its verified close.
func (a *ForceCancelActivities) Classify(ctx context.Context, in FCStep) (FCClassified, error) {
	d, err := a.acting(ctx, in.Input)
	if err != nil {
		return FCClassified{}, err
	}
	status, closed, err := a.closeOf(ctx, d, in.ExecutionRunID)
	if err != nil {
		return FCClassified{}, err
	}
	return classifyForceCancel(status, closed, d.RequestID), nil
}

// classifyForceCancel is how a run's status and close read to the force cancel
// requestID: a termination counts as its own only when its record verifies and
// carries this request (design §22.1.4, §22.26.16 N1, §22.26.18 N4).
func classifyForceCancel(status enumspb.WorkflowExecutionStatus, closed ForceCancelClose, requestID string) FCClassified {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return FCClassified{Open: true} // the run is
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return FCClassified{State: FCStateCanceled}
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
	default:
		return FCClassified{State: FCStateEnded}
	}
	switch closed.Verification {
	case ForceCancelVerified, ForceCancelAsserted:
		if closed.Record.RequestID == requestID {
			return FCClassified{State: FCStateForceCanceled}
		}
		return FCClassified{State: FCStateAlreadyEnded, Request: closed.Record.RequestID}
	case ForceCancelUnknownKey:
		return FCClassified{State: FCStateFailed, Cause: FCCauseUnknownKey, Retry: true}
	case ForceCancelUnavailable:
		return FCClassified{State: FCStateFailed, Cause: FCCauseKeysUnavailable, Retry: true}
	case ForceCancelNone:
		return FCClassified{State: FCStateAlreadyEnded}
	}
	return FCClassified{State: FCStateAlreadyEnded, Cause: FCCauseUnverifiedRecord}
}

// Publish is step 10's build: the run's ending as every reader builds it
// (ForceCanceledTerminalEvent, through the reader), with the ended run's
// snapshot written for the projection, and only when the run's close is this
// force cancel's, verified. It reads the whole run, so it is an ordinary
// activity on the engines' event channel, beside PublishEvent; whoever
// schedules it makes an engine write the ending the run already has.
func (a *ForceCancelActivities) Publish(ctx context.Context, in FCStep) (FCPublished, error) {
	stop := heartbeat(ctx, in.Input.timings().PublishHeartbeat/3)
	defer stop()
	self, err := a.self(ctx)
	if err != nil {
		return FCPublished{}, err
	}
	// The one step scheduled on a channel: from the engines' workflow code.
	if self.Local || self.TaskQueue != task.DefaultQueue {
		return FCPublished{}, errors.New("force cancel: not scheduled by a force cancel's own code")
	}
	self.Local = true
	d, err := a.actingAs(ctx, in.Input, self)
	if err != nil {
		return FCPublished{}, err
	}
	first, err := a.firstEvent(ctx, d.RunID, in.ExecutionRunID)
	if err != nil {
		return FCPublished{}, err
	}
	if sealedStart(first.GetWorkflowExecutionStartedEventAttributes()) {
		return FCPublished{}, sealedEndingRefused(d.RunID)
	}
	status, closed, err := a.closeOf(ctx, d, in.ExecutionRunID)
	if err != nil {
		return FCPublished{}, err
	}
	got := classifyForceCancel(status, closed, d.RequestID)
	if got.Retry {
		return FCPublished{}, fmt.Errorf("force cancel: the run's close cannot be read now: %s", got.Cause)
	}
	request := d.RequestID
	if got.Request != "" {
		request = got.Request // the record that ended it, verified
	}
	if got.State != FCStateForceCanceled && got.Request == "" {
		return FCPublished{Cause: FCCauseUnverifiedRecord}, nil
	}
	// As its archive is read: what is gone for good is said and not waited for,
	// so the ending is announced without it.
	run, read, err := a.reader.readForArchive(ctx, d.RunID, false)
	if errors.Is(err, errRunNotFinished) {
		return FCPublished{Cause: FCCauseEventSequence}, nil // it was resumed since
	}
	if err != nil {
		return FCPublished{}, err
	}
	out, err := forceCancelEnding(run, read.Events, request)
	if err != nil {
		return FCPublished{}, err
	}
	if out.Ended {
		return a.ownEnding(ctx, d, in)
	}
	event, err := out.event()
	if err != nil || event == nil {
		return out, err
	}
	data, err := proto.Marshal(run)
	if err != nil {
		return FCPublished{}, fmt.Errorf("force cancel: marshal the ended run %s: %w", d.RunID, err)
	}
	uri, err := a.snapshots.WriteSnapshot(ctx, d.Namespace, d.RunID, event.GetSequence(), data)
	if err != nil {
		return FCPublished{}, fmt.Errorf("force cancel: write the ended run %s: %w", d.RunID, err)
	}
	out.SnapshotURI = uri
	return out, nil
}

// ownEnding is the end a run announced before it was terminated, when the
// termination came before that announcement finished: whoever subscribed was
// never told, so it is made again, as the run made it.
func (a *ForceCancelActivities) ownEnding(ctx context.Context, d ForceCancelDecision, in FCStep) (FCPublished, error) {
	finished := map[int64]bool{}
	events, err := a.newestEvents(ctx, d.RunID, in.ExecutionRunID, in.Input.budget(), in.Input.page(), func(e *historypb.HistoryEvent) bool {
		if done := e.GetActivityTaskCompletedEventAttributes(); done != nil {
			finished[done.GetScheduledEventId()] = true
		}
		return a.reader.isPublishEvent(e)
	})
	if err != nil {
		return FCPublished{}, err
	}
	out := FCPublished{Ended: true}
	if len(events) == 0 {
		return out, nil
	}
	last := events[len(events)-1]
	payloads := last.GetActivityTaskScheduledEventAttributes().GetInput().GetPayloads()
	if !a.reader.isPublishEvent(last) || finished[last.GetEventId()] || len(payloads) < 3 {
		return out, nil
	}
	event, uri, err := decodePublishEvent(a.reader.converter, d.RunID, last)
	if err != nil {
		return FCPublished{}, err
	}
	if !isTerminalRunEvent(event) || event.GetRunId() != d.RunID {
		return out, nil
	}
	if out, err = out.with(event); err != nil {
		return FCPublished{}, err
	}
	out.SnapshotURI = uri
	return out, a.reader.converter.FromPayload(payloads[2], &out.DedupeKey)
}

// forceCancelEnding is the ending the run's readers show for the force cancel
// requestID, or why there is none of it to announce.
func forceCancelEnding(run *orkstackv1.Run, events []*orkstackv1.Event, requestID string) (FCPublished, error) {
	if len(events) == 0 {
		return FCPublished{Cause: FCCauseEventSequence}, nil
	}
	last, forced := events[len(events)-1], run.GetError().GetCode() == RunForceCanceledCode
	switch {
	case isTerminalRunEvent(last) && !forced:
		return FCPublished{Ended: true}, nil
	case !isTerminalRunEvent(last) || last.GetRequestId() != requestID || !forced:
		return FCPublished{Cause: FCCauseEventSequence}, nil
	}
	return FCPublished{}.with(last)
}

// with is p carrying e.
func (p FCPublished) with(e *orkstackv1.Event) (FCPublished, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(e)
	if err != nil {
		return FCPublished{}, fmt.Errorf("force cancel: marshal the ending's event: %w", err)
	}
	p.Event = data
	return p, nil
}

// heartbeat beats every interval until it is stopped.
func heartbeat(ctx context.Context, every time.Duration) (stop func()) {
	if !activity.IsActivity(ctx) {
		return func() {}
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				activity.RecordHeartbeat(ctx)
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	})
	return func() { close(done); wg.Wait() }
}

// forceCancelRefusedType is the error type of errForceCancelRefused.
const forceCancelRefusedType = "ForceCancelRefused"

// errForceCancelRefused is a step whose decision does not verify, or is not
// this execution's: nothing asked again changes it.
func errForceCancelRefused(cause string) error {
	return temporal.NewNonRetryableApplicationError(cause, forceCancelRefusedType, nil)
}

// sealedStart reports a start of a sealed run: one that carries a key, or
// anything sealed.
func sealedStart(attrs *historypb.WorkflowExecutionStartedEventAttributes) bool {
	_, keyed := attrs.GetMemo().GetFields()[payloadcrypt.MemoField]
	return keyed || slices.ContainsFunc(attrs.GetInput().GetPayloads(), payloadcrypt.IsSealed)
}

// sealedEndingRefused is Publish's answer for a sealed run: refused, so it is
// asked once, since nothing asked again changes it.
func sealedEndingRefused(runID string) error {
	return temporal.NewNonRetryableApplicationError("run "+runID+": "+errForceCancelSealed.Error(), forceCancelRefusedType, errForceCancelSealed)
}

// errForceCancelSealed is why a sealed run's ending is not announced.
var errForceCancelSealed = errors.New("force cancel: the run is encrypted: its ending is not announced, since the force cancel's own history would hold it in plaintext")
