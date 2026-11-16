package identity

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The attempt, start and binding checks (docs/TASK_IDENTITY.md, "The
// mint", steps 4 to 6): one library the mint and the commit both call.
// Each is decided from facts the cluster reported and records the engine
// wrote; nothing here writes but a root's missing chain record.

// MaxStartWalk is how many admissions a start check reads, walking from a
// child to its root: composition's depth (engine.MaxComposeDepth, 4) and
// the root. A walk that would go further is unavailable, never a refusal:
// a later release may compose deeper.
const MaxStartWalk = 5

// taskIDPattern is a task id's rule, so no id renders another's item.
var taskIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

const maxTaskID = 200

// ErrNoSuchExecution is the cluster's answer that it holds no such
// execution: an answer, not a failure to ask.
var ErrNoSuchExecution = errors.New("identity: the cluster holds no such execution")

// ExecutionRef names one execution of a workflow.
type ExecutionRef struct {
	WorkflowID, ExecutionID string
}

// ExecutionFacts is what one describe of an execution reports that the
// checks read.
type ExecutionFacts struct {
	// WorkflowID is the run the describe answered for: a check holds the
	// facts to its admission's run, so facts of another run never fit.
	WorkflowID       string
	Open             bool
	ExecutionID      string
	FirstExecutionID string
	// Parent is the execution that started this chain as a child, if one
	// did.
	Parent *ExecutionRef
	// Pending are its pending ExecuteTask activities.
	Pending []PendingAttempt
}

// PendingAttempt is one pending ExecuteTask activity.
type PendingAttempt struct {
	ActivityID string
	Started    bool
	Attempt    int32
	LastWorker string
	StartedAt  time.Time
}

// FactsFromDescribe reads the facts of a describe.
func FactsFromDescribe(resp *workflowservice.DescribeWorkflowExecutionResponse) ExecutionFacts {
	info := resp.GetWorkflowExecutionInfo()
	f := ExecutionFacts{
		Open:             info.GetCloseTime() == nil && info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		WorkflowID:       info.GetExecution().GetWorkflowId(),
		ExecutionID:      info.GetExecution().GetRunId(),
		FirstExecutionID: info.GetFirstRunId(),
	}
	// A parent reported with a field missing is kept as it is, so a check
	// answers it unavailable rather than reading it as no parent.
	if p := info.GetParentExecution(); p != nil {
		f.Parent = &ExecutionRef{WorkflowID: p.GetWorkflowId(), ExecutionID: p.GetRunId()}
	}
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityType().GetName() != task.ActivityExecuteTask {
			continue
		}
		p := PendingAttempt{ActivityID: pa.GetActivityId(), Started: pa.GetState() == enumspb.PENDING_ACTIVITY_STATE_STARTED,
			Attempt: pa.GetAttempt(), LastWorker: pa.GetLastWorkerIdentity()}
		if t := pa.GetLastStartedTime(); t != nil {
			p.StartedAt = t.AsTime()
		}
		f.Pending = append(f.Pending, p)
	}
	return f
}

// AttemptAsk is the attempt a request names, and the Orker its credential
// names.
type AttemptAsk struct {
	TaskID  string
	Item    artifacts.Item
	Attempt int32
	OrkerID string
}

// ActivityID is the activity id a task's attempt runs under:
// ork/<task>, or ork/<task>/<item> for an item of a fan-out.
func ActivityID(taskID string, item artifacts.Item) string {
	if !item.Set {
		return "ork/" + taskID
	}
	return "ork/" + taskID + "/" + strconv.Itoa(int(item.Index))
}

// AttemptVerdict is an attempt check's answer.
type AttemptVerdict int

const (
	// AttemptHeld: the caller's Orker holds the attempt.
	AttemptHeld AttemptVerdict = iota + 1
	// AttemptNotHeld: the facts were read and the attempt is not the
	// caller's (ATTEMPT_NOT_HELD).
	AttemptNotHeld
	// AttemptUnknown: the describe did not say enough to decide
	// (IDENTITY_UNAVAILABLE).
	AttemptUnknown
)

// HeldAttempt decides whether the caller's Orker holds the attempt of run:
// the facts must be a describe of run, open, with a pending ExecuteTask
// activity of the task's id, started at a time the cluster reported, at the
// asked attempt, last held by the caller's Orker. A describe that names no
// run or execution, or a matching attempt with no start time, is
// AttemptUnknown; anything else that does not fit is AttemptNotHeld.
func HeldAttempt(f ExecutionFacts, runID string, ask AttemptAsk) (PendingAttempt, AttemptVerdict) {
	if f.WorkflowID == "" || f.ExecutionID == "" {
		return PendingAttempt{}, AttemptUnknown
	}
	if f.WorkflowID != runID || !f.Open || ask.OrkerID == "" || !taskIDPattern.MatchString(ask.TaskID) || len(ask.TaskID) > maxTaskID ||
		(ask.Item.Set && ask.Item.Index < 0) || ask.Attempt < 1 {
		return PendingAttempt{}, AttemptNotHeld
	}
	id := ActivityID(ask.TaskID, ask.Item)
	for _, p := range f.Pending {
		if p.ActivityID == id && p.Started && p.Attempt == ask.Attempt && p.LastWorker == ask.OrkerID {
			if p.StartedAt.IsZero() {
				return PendingAttempt{}, AttemptUnknown
			}
			return p, AttemptHeld
		}
	}
	return PendingAttempt{}, AttemptNotHeld
}

// StartReader is what a start check reads: the cluster's describe of one
// execution (ErrNoSuchExecution when it holds none), the records, a root's
// chain record and, to complete one, the admission digest the plan of a
// chain's first start names ("" for none).
type StartReader interface {
	Describe(ctx context.Context, workflowID, executionID string) (ExecutionFacts, error)
	ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error)
	ReadChain(ctx context.Context, namespace, runID string) (artifacts.RecordRead[artifacts.Chain], error)
	CompleteChain(ctx context.Context, namespace, runID string, c artifacts.Chain) error
	FirstStartAdmission(ctx context.Context, runID, firstExecutionID string) (string, error)
}

// StartVerdict is a start check's answer.
type StartVerdict int

const (
	// StartAdmitted: the execution is the run its admission was written
	// for.
	StartAdmitted StartVerdict = iota + 1
	// StartNotAdmitted: a fact was read and does not fit
	// (IDENTITY_NOT_ADMITTED).
	StartNotAdmitted
	// StartUnavailable: a fact could not be read (IDENTITY_UNAVAILABLE).
	StartUnavailable
)

func (v StartVerdict) String() string {
	switch v {
	case StartAdmitted:
		return "admitted"
	case StartNotAdmitted:
		return "IDENTITY_NOT_ADMITTED"
	case StartUnavailable:
		return "IDENTITY_UNAVAILABLE"
	}
	return fmt.Sprintf("verdict(%d)", int(v))
}

// StartAnswer is a verdict and, for the engine's log, why.
type StartAnswer struct {
	Verdict StartVerdict
	Reason  string
}

func notAdmitted(format string, args ...any) StartAnswer {
	return StartAnswer{Verdict: StartNotAdmitted, Reason: fmt.Sprintf(format, args...)}
}

func unavailable(format string, args ...any) StartAnswer {
	return StartAnswer{Verdict: StartUnavailable, Reason: fmt.Sprintf(format, args...)}
}

// CheckStart decides whether the execution facts describe is the run adm
// (named sha256Hex) was written for, by its starter: an engine's start has
// no parent and a chain record naming its first execution and this
// admission, completed from the first start's plan when absent; a fire's
// has exactly that fire for its parent; a child's parent is an execution of
// the run its record names, that is the run of the parent's admission by
// this same rule, up to MaxStartWalk admissions.
//
// adm must encode to the record named sha256Hex, which is checked, and
// facts must be a describe of adm's run: facts of another run never fit.
// The StartReader's Describe answers ErrNoSuchExecution only for the
// cluster's own "not found".
func CheckStart(ctx context.Context, r StartReader, adm artifacts.Admission, sha256Hex string, facts ExecutionFacts) StartAnswer {
	if _, sha, err := artifacts.EncodeAdmission(adm); err != nil || sha != sha256Hex {
		return unavailable("the admission given is not the record named %s", short(sha256Hex))
	}
	for step := 0; ; step++ {
		if step >= MaxStartWalk {
			return unavailable("the walk to the root is longer than %d admissions", MaxStartWalk)
		}
		if facts.WorkflowID == "" || facts.FirstExecutionID == "" {
			return unavailable("the cluster reported no run or first execution for run %s", adm.RunID)
		}
		if facts.WorkflowID != adm.RunID {
			return notAdmitted("the execution described is of run %s, not %s", facts.WorkflowID, adm.RunID)
		}
		if facts.Parent != nil && (facts.Parent.WorkflowID == "" || facts.Parent.ExecutionID == "") {
			return unavailable("the cluster reported run %s's parent with no execution", adm.RunID)
		}
		s := adm.Starter
		switch s.Kind {
		case artifacts.StarterEngine:
			return checkEngineStart(ctx, r, adm, sha256Hex, facts)
		case artifacts.StarterFire:
			if facts.Parent == nil || *facts.Parent != (ExecutionRef{WorkflowID: s.WorkflowID, ExecutionID: s.ExecutionID}) {
				return notAdmitted("run %s was not started by the fire its admission names", adm.RunID)
			}
			return StartAnswer{Verdict: StartAdmitted, Reason: "started by its fire"}
		case artifacts.StarterParentRun:
			if facts.Parent == nil || facts.Parent.WorkflowID != s.RunID {
				return notAdmitted("run %s was not started by its parent run %s", adm.RunID, s.RunID)
			}
			parentFacts, err := r.Describe(ctx, facts.Parent.WorkflowID, facts.Parent.ExecutionID)
			if errors.Is(err, ErrNoSuchExecution) {
				return notAdmitted("the cluster holds no execution %s of parent run %s", facts.Parent.ExecutionID, s.RunID)
			}
			if err != nil {
				return unavailable("describe parent run %s: %v", s.RunID, err)
			}
			read, err := r.ReadAdmission(ctx, adm.Namespace, s.RunID, s.AdmissionSHA256)
			if err != nil {
				return unavailable("read parent run %s's admission: %v", s.RunID, err)
			}
			switch read.Answer {
			case artifacts.RecordFound:
			case artifacts.RecordNone:
				return notAdmitted("parent run %s has no admission %s", s.RunID, short(s.AdmissionSHA256))
			default:
				return unavailable("parent run %s's admission %s is %s", s.RunID, short(s.AdmissionSHA256), read.Answer)
			}
			parent := read.Value
			// A parent that records values lists every child it composes:
			// a task it does not list composes nothing. One that records
			// none lists none, and its parent fact is what ties them.
			if parent.Children != nil {
				if child, ok := parent.Children[s.TaskID]; !ok || child != adm.RunID {
					return notAdmitted("parent run %s's task %s does not compose run %s", s.RunID, s.TaskID, adm.RunID)
				}
			}
			adm, sha256Hex, facts = parent, s.AdmissionSHA256, parentFacts
		default:
			return unavailable("starter %q is no starter this build reads", s.Kind)
		}
	}
}

func checkEngineStart(ctx context.Context, r StartReader, adm artifacts.Admission, sha256Hex string, facts ExecutionFacts) StartAnswer {
	if facts.Parent != nil {
		return notAdmitted("run %s was started as a child, and its admission names an engine", adm.RunID)
	}
	want := artifacts.Chain{FirstExecutionID: facts.FirstExecutionID, AdmissionSHA256: sha256Hex}
	read, err := r.ReadChain(ctx, adm.Namespace, adm.RunID)
	if err != nil {
		return unavailable("read run %s's chain record: %v", adm.RunID, err)
	}
	switch read.Answer {
	case artifacts.RecordFound:
		if read.Value != want {
			return notAdmitted("run %s's chain record names first execution %s and admission %s",
				adm.RunID, read.Value.FirstExecutionID, short(read.Value.AdmissionSHA256))
		}
		return StartAnswer{Verdict: StartAdmitted, Reason: "its chain record names it"}
	case artifacts.RecordNone:
	default:
		return unavailable("run %s's chain record is %s", adm.RunID, read.Answer)
	}
	// Absent: complete it from the plan of the chain's first start, which
	// no worker can rewrite and which names the admission's secret digest.
	named, err := r.FirstStartAdmission(ctx, adm.RunID, facts.FirstExecutionID)
	if err != nil {
		return unavailable("read run %s's first start: %v", adm.RunID, err)
	}
	if named != sha256Hex {
		return notAdmitted("run %s's first start does not name its admission", adm.RunID)
	}
	err = r.CompleteChain(ctx, adm.Namespace, adm.RunID, want)
	var invalid *artifacts.ChainInvalidError
	switch {
	case err == nil:
		return StartAnswer{Verdict: StartAdmitted, Reason: "its first start names it; chain record completed"}
	case errors.Is(err, artifacts.ErrChainDiffers):
		return notAdmitted("run %s's chain record names another start", adm.RunID)
	case errors.As(err, &invalid):
		return unavailable("run %s's chain record cannot be read", adm.RunID)
	default:
		return unavailable("complete run %s's chain record: %v", adm.RunID, err)
	}
}

// CheckBinding says whether a task's attempt was dispatched with the spec
// digest its admission holds for it. When not, it answers the admitting
// engine's build, by which the Orker tells IDENTITY_SPEC_SKEW from
// TASK_NOT_AS_ADMITTED.
func CheckBinding(adm artifacts.Admission, taskID, taskSpecSHA256 string) (string, bool) {
	t, ok := adm.Tasks[taskID]
	if ok && t.SpecSHA256 != "" && t.SpecSHA256 == taskSpecSHA256 {
		return "", true
	}
	return adm.EngineBuild, false
}

// short is a digest as a reason names it: enough to tell records apart in
// a log, and never the whole of a value that is secret until its run starts.
func short(sha256Hex string) string {
	if len(sha256Hex) > 12 {
		return sha256Hex[:12]
	}
	return sha256Hex
}
