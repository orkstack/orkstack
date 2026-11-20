package identity

import (
	"context"
	"errors"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The steps the mint and the commit share (workload-identity §3, §3.4:
// "Steps 1, 2 and 4 to 6a are the mint's code, not a copy of it"). Each
// answers a *MintError: a refusal named, or IDENTITY_UNAVAILABLE for a
// step that could not ask.

// admissionReader is what step 2 reads.
type admissionReader interface {
	ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error)
}

// attemptCoords names the attempt a request is about, as the request says.
type attemptCoords struct {
	Namespace, RunID, TaskID, AdmissionSHA256 string
	Item                                      artifacts.Item
}

func attemptOf(namespace, runID, taskID, admissionSHA256 string, item artifacts.Item) attemptCoords {
	return attemptCoords{Namespace: namespace, RunID: runID, TaskID: taskID, AdmissionSHA256: admissionSHA256, Item: item}
}

// admitTask is step 2's shared part: the admission record the request
// names, read by the run's id and digest; the request's namespace and run
// must be the record's, the task must be in its table, and an item must
// be one of a fan-out's admitted items. Absent is IDENTITY_NOT_ADMITTED;
// unreadable or not trusted is IDENTITY_UNAVAILABLE.
func admitTask(ctx context.Context, read admissionReader, c attemptCoords) (artifacts.Admission, artifacts.AdmittedTask, error) {
	got, err := read.ReadAdmission(ctx, c.Namespace, c.RunID, c.AdmissionSHA256)
	if errors.Is(err, artifacts.ErrInvalidRecordKey) {
		return artifacts.Admission{}, artifacts.AdmittedTask{}, refuse(ReasonNotAdmitted, true, "run %q names no admission record %q", c.RunID, c.AdmissionSHA256)
	}
	if err != nil {
		return artifacts.Admission{}, artifacts.AdmittedTask{}, unavailableMint("the admission record of run %q could not be read: %v", c.RunID, err)
	}
	switch got.Answer {
	case artifacts.RecordFound:
	case artifacts.RecordNone:
		return artifacts.Admission{}, artifacts.AdmittedTask{}, refuse(ReasonNotAdmitted, true, "run %q has no admission record %q", c.RunID, c.AdmissionSHA256)
	default:
		return artifacts.Admission{}, artifacts.AdmittedTask{}, unavailableMint("the admission record of run %q cannot be trusted (%s: %s)", c.RunID, got.Object, got.Reason)
	}
	adm := got.Value
	if adm.Namespace != c.Namespace || adm.RunID != c.RunID {
		return artifacts.Admission{}, artifacts.AdmittedTask{}, refuse(ReasonNotAdmitted, true, "the admission record is of run %q in namespace %q", adm.RunID, adm.Namespace)
	}
	row, ok := adm.Tasks[c.TaskID]
	if !ok {
		return artifacts.Admission{}, artifacts.AdmittedTask{}, refuse(ReasonNotAdmitted, true, "task %q is not in run %q's admission", c.TaskID, c.RunID)
	}
	switch fanOut := row.MaxItems > 0; {
	case c.Item.Set && !fanOut:
		return artifacts.Admission{}, artifacts.AdmittedTask{}, refuse(ReasonNotAdmitted, true, "task %q is not a fan-out, and the request names item %d", c.TaskID, c.Item.Index)
	case !c.Item.Set && fanOut:
		return artifacts.Admission{}, artifacts.AdmittedTask{}, refuse(ReasonNotAdmitted, true, "task %q is a fan-out, and the request names no item", c.TaskID)
	case c.Item.Set && (c.Item.Index < 0 || c.Item.Index >= row.MaxItems):
		return artifacts.Admission{}, artifacts.AdmittedTask{}, refuse(ReasonNotAdmitted, true, "task %q was admitted %d items, not item %d", c.TaskID, row.MaxItems, c.Item.Index)
	}
	return adm, row, nil
}

// attemptChecks is what steps 4 to 6a ask: the cluster's describe of the
// run, the run's start records, and the records the task follows.
type attemptChecks struct {
	Describe func(ctx context.Context, runID string) (ExecutionFacts, error)
	Starts   StartReader
	Follows  FollowsChecker
}

// checkAttempt is steps 4 to 6a: the attempt is pending, started, at that
// number, on the caller's Orker, in a describe made after the request
// arrived; this execution is the run the admission was written for; the
// task's spec digest is the admitted one; and each task the admission
// says this one follows has its record (a build without that check
// refuses an admission that names any).
func checkAttempt(ctx context.Context, ck attemptChecks, adm artifacts.Admission, c attemptCoords, attempt int32, taskSpecSHA256 string, caller Caller) (PendingAttempt, error) {
	// Step 4: the attempt.
	facts, err := ck.Describe(ctx, c.RunID)
	if errors.Is(err, ErrNoSuchExecution) {
		return PendingAttempt{}, refuse(ReasonAttemptNotHeld, false, "run %q is not executing", c.RunID)
	}
	if err != nil {
		return PendingAttempt{}, unavailableMint("run %q could not be described: %v", c.RunID, err)
	}
	pending, verdict := HeldAttempt(facts, c.RunID, AttemptAsk{TaskID: c.TaskID, Item: c.Item, Attempt: attempt, OrkerID: caller.OrkerID})
	switch verdict {
	case AttemptHeld:
	case AttemptNotHeld:
		return PendingAttempt{}, refuse(ReasonAttemptNotHeld, false, "attempt %d of task %q is not held by Orker %q", attempt, c.TaskID, caller.OrkerID)
	default:
		return PendingAttempt{}, unavailableMint("the cluster's describe of run %q did not say who holds the attempt", c.RunID)
	}
	// Step 5: the start.
	switch start := CheckStart(ctx, ck.Starts, adm, c.AdmissionSHA256, facts); start.Verdict {
	case StartAdmitted:
	case StartNotAdmitted:
		return PendingAttempt{}, refuse(ReasonNotAdmitted, true, "%s", start.Reason)
	default:
		return PendingAttempt{}, unavailableMint("%s", start.Reason)
	}
	// Step 6: the task binding.
	if why, ok := CheckBinding(adm, c.TaskID, taskSpecSHA256); !ok {
		e := refuse(ReasonTaskNotAsAdmitted, true, "%s", why)
		e.EngineBuild = adm.EngineBuild
		return PendingAttempt{}, e
	}
	// Step 6a: what the task follows.
	if len(adm.Follows[c.TaskID]) > 0 {
		if ck.Follows == nil {
			return PendingAttempt{}, refuse(ReasonEngineRefused, false, "task %q follows other tasks, and this engine cannot check them", c.TaskID)
		}
		if err := ck.Follows.CheckFollows(ctx, adm, c.TaskID, c.Item); err != nil {
			var me *MintError
			if errors.As(err, &me) {
				return PendingAttempt{}, me
			}
			return PendingAttempt{}, unavailableMint("what task %q follows could not be checked: %v", c.TaskID, err)
		}
	}
	return pending, nil
}
