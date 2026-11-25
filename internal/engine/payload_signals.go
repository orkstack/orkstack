package engine

import (
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
)

// ResolutionUnreadableMessage is the one text of the event a run records for
// a resolution it could not read (payload-encryption.md §4.1 rule 6). The
// interpreter emits nothing else with it, so an observer may compare a
// message with it whole.
const ResolutionUnreadableMessage = ResolutionUnreadableCode + ": a resolution sent to this run could not be read; no awaited task changed"

// ResolutionUnreadableCode names a resolution a run could not read, in its
// event's message (docs/REASONS.md).
const ResolutionUnreadableCode = "RESOLUTION_UNREADABLE"

// openResolution reads a resolution the channel handed over raw: sealed, it
// opens under the run's key for the run's own id; plaintext, it is read as
// it is (payload-encryption.md §2, "What is not refused"). One that does not
// yield its message (sealed and not opening, marked sealed in a run that is
// not, or not decoding) is recorded as one TASK_LOG event of the run, with no
// task id, and changes no awaited task: ok is false.
func (s *runState) openResolution(ctx workflow.Context, raw converter.RawValue) (sig *orkstackv1.AwaitSignal, ok bool) {
	p := raw.Payload()
	if p == nil {
		s.unreadableResolution(ctx)
		return nil, false
	}
	p, err := payloadcrypt.OpenIfSealed(s.scope, p)
	if err != nil || DataConverter().FromPayload(p, &sig) != nil {
		s.unreadableResolution(ctx)
		return nil, false
	}
	return sig, true
}

// openFailure opens a failure that err holds still sealed, with the run's
// key, so the interpreter reads and classifies what the SDK handed over
// unopened (payload-encryption.md §4.1 rule 4). A failure that is not sealed
// is err itself. One that does not open fails the task explicitly,
// PAYLOAD_UNREADABLE, never skipped.
func (s *runState) openFailure(err error) error {
	return openFailureWith(s.scope, err)
}

// openFailureWith is openFailure with scope: a child run's failure is sealed
// for the child's own id (payload-encryption.md §4.1 rule 2).
func openFailureWith(scope *payloadcrypt.Scope, err error) error {
	if err == nil {
		return nil
	}
	opened, openErr := payloadcrypt.OpenFailure(scope, failureConverter, err)
	if openErr != nil {
		return payloadUnreadable("a failure sent to this run could not be read")
	}
	return opened
}

// getLocal reads a local activity's result into ptr. Its result, which the
// payload interceptor sealed for the run, is opened with the run's key, and
// so is its failure before anything reads or classifies it.
func (s *runState) getLocal(ctx workflow.Context, f workflow.Future, ptr any) error {
	return getLocal(ctx, s.scope, f, ptr)
}

// getLocal is the method of that name, with scope.
func getLocal(ctx workflow.Context, scope *payloadcrypt.Scope, f workflow.Future, ptr any) error {
	if ptr == nil || !scope.Sealed() {
		return openFailureWith(scope, f.Get(ctx, ptr))
	}
	// The SDK reads it with the root converter, which opens nothing.
	var raw converter.RawValue
	if err := f.Get(ctx, &raw); err != nil {
		return openFailureWith(scope, err)
	}
	// Plaintext is refused too: only a process that seals answers one.
	if _, err := payloadcrypt.OpenRaw(scope, DataConverter(), raw, ptr); err != nil {
		// Not the error: what a decode says may quote what it read.
		workflow.GetLogger(ctx).Error("a local activity's result recorded in an encrypted run's history could not be read",
			"sealed", payloadcrypt.IsSealed(raw.Payload()))
		return payloadUnreadable("a result recorded in this run's history could not be read")
	}
	return nil
}

// unreadableLocal reports that err is getLocal's of a result that could not
// be read: permanent, as what a history holds does not change.
func unreadableLocal(err error) bool {
	return categoryOf(err).reasonCode() == PayloadUnreadableCode
}

// PayloadUnreadableCode is the reason a task fails with when a payload it
// sent back does not open as this run's (payload-encryption.md §4.1 rule 2).
const PayloadUnreadableCode = "PAYLOAD_UNREADABLE"

// payloadUnreadable is a task's explicit, never retried failure for a
// payload that does not open: a ContractViolation with its reason code, read
// as every activity failure is (categoryOf). The words are fixed.
func payloadUnreadable(message string) error {
	return temporal.NewNonRetryableApplicationError(message, task.CategoryContractViolation, nil,
		&orkstackv1.ActivityFailureDetail{ReasonCode: PayloadUnreadableCode,
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION})
}

// unreadableResolution records a resolution that could not be read: one
// event, written and published before the run goes on, holding ids only.
func (s *runState) unreadableResolution(ctx workflow.Context) {
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, "", s.status, ResolutionUnreadableMessage); err != nil {
		workflow.GetLogger(ctx).Error("emit an unreadable resolution failed", "run_id", s.runID, "error", err)
	}
}

// plainMemo is m with every value marked Plain in a sealed run: a memo is
// what every list and count is answered from, so it is never sealed
// (payload-encryption.md §2). In any other run, and for a nil value, which
// removes its key, it is m as it is.
func plainMemo(ctx workflow.Context, m map[string]any) map[string]any {
	scope := runScope(ctx)
	for k, v := range m {
		if v != nil {
			m[k] = payloadcrypt.PlainIn(scope, v)
		}
	}
	return m
}

// plainActivities are the activities a run schedules whose worker reads its
// input with no run key, so their arguments are written plaintext in a sealed
// run too: the SDK's own session activities, which carry a session id, and
// the Orker's pin release, which carries ids, a pin's name and a bound.
var plainActivities = map[string]bool{
	sessionCreationActivity:   true,
	sessionCompletionActivity: true,
	task.ActivityReleasePin:   true,
}

const sessionCompletionActivity = "internalSessionCompletionActivity"

// plainActivityInbound marks the arguments of plainActivities Plain
// (PayloadInterceptor).
type plainActivityInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (i *plainActivityInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&plainActivityOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}})
}

type plainActivityOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
}

func (o *plainActivityOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if plainActivities[activityType] {
		scope := runScope(ctx)
		marked := make([]any, len(args))
		for i, a := range args {
			marked[i] = payloadcrypt.PlainIn(scope, a)
		}
		args = marked
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}
