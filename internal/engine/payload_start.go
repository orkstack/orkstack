package engine

import (
	"errors"
	"fmt"
	"regexp"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// RunStartUnreadableCode ends a run whose start cannot be read: a keyless
// run (payload-encryption.md §4.1 rule 6), or one that is not sealed and
// whose start does not decode or is marked sealed.
const RunStartUnreadableCode = "RUN_START_UNREADABLE"

// runStartUnreadableMessage is the keyless ending's one text: fixed words,
// never anything read from the start.
const runStartUnreadableMessage = "this run's start could not be read; nothing of it was started"

// runScopeKey carries a run's key (its payloadcrypt.Scope) through its
// workflow code.
type runScopeKey struct{}

// withRunScope is ctx carrying the run's scope. Setting a value issues no
// command.
func withRunScope(ctx workflow.Context, s *payloadcrypt.Scope) workflow.Context {
	return workflow.WithValue(ctx, runScopeKey{}, s)
}

// runScope is the run's key: the one RunWorkflowRaw opened its start with,
// or, where the run was started through the typed entry, the one its memo
// gives (Unsealed() for a run that is not sealed; nil for a key that is
// lost, which seals nothing and fails closed). A memo naming an engines' key
// this process lacks panics, as at the start.
func runScope(ctx workflow.Context) *payloadcrypt.Scope {
	if s, ok := ctx.Value(runScopeKey{}).(*payloadcrypt.Scope); ok {
		return s
	}
	st := payloadcrypt.RunKey(ctx)
	if errors.Is(st.Err(), payloadcrypt.ErrEngineKeyMissing) {
		panic(st.Err())
	}
	return st.Scope()
}

// runIDShape is a run id as the engine mints it, held to its shape before it
// names an object.
var runIDShape = regexp.MustCompile(`^ork-[0-9a-z-]{1,80}$`)

// versionShape is an Orkflow version as a memo may name it in a keyless
// run's record: empty, or printable with no space, at most 200 bytes.
var versionShape = regexp.MustCompile(`^[0-9A-Za-z._+~-]{0,200}$`)

// RunWorkflowRaw is RunWorkflow as registered under RunWorkflowType: it
// takes its arguments as the SDK's raw values and opens each itself, so a
// start that cannot be read ends by the interpreter's own path, with a
// reason and a last event, never by the SDK's decode (payload-encryption.md
// §4.1 rule 6). The arguments are RunWorkflow's, in its order.
//
// A run whose memo names an engines' payload key this process does not
// hold panics here, so the workflow task fails and an engine that holds the
// file takes it: it decides nothing (rule 3).
func RunWorkflowRaw(ctx workflow.Context, runIDArg, wfArg, inputArg, planArg, carryArg converter.RawValue) (any, error) {
	st := payloadcrypt.RunKey(ctx)
	if errors.Is(st.Err(), payloadcrypt.ErrEngineKeyMissing) {
		panic(st.Err())
	}
	continued := workflow.GetInfo(ctx).ContinuedExecutionRunID != ""
	if st.Err() != nil {
		return keylessEnding(ctx, continued, "its memo's key field yields no key: "+st.Err().Error())
	}
	scope := st.Scope()
	sealed := scope.Sealed()
	// A run's key is a run tree's: a schedule's key field never starts a run.
	if sealed && scope.Context().Kind != payloadcrypt.KindRun {
		return keylessEnding(ctx, continued, "its memo carries a schedule's key")
	}
	var (
		runID string
		wf    *orkstackv1.Orkflow
		input *orkstackv1.Payload
		plan  RunPlan
		carry *orkstackv1.RunContinuation
	)
	for _, a := range []struct {
		name     string
		raw      converter.RawValue
		ptr      any
		required bool
	}{
		{"run id", runIDArg, &runID, sealed},
		{"definition", wfArg, &wf, sealed},
		{"input", inputArg, &input, sealed},
		{"plan", planArg, &plan, sealed},
		{"carry", carryArg, &carry, sealed && continued},
	} {
		present, err := payloadcrypt.OpenRaw(scope, DataConverter(), a.raw, a.ptr)
		switch {
		case err != nil:
			return keylessEnding(ctx, continued, "its "+a.name+" argument does not open: "+err.Error())
		case a.required && !present:
			return keylessEnding(ctx, continued, "its start lacks its "+a.name+" argument")
		}
	}
	if sealed && !startMatchesMemo(ctx, st, wf) {
		return keylessEnding(ctx, continued, "its start names another namespace or Orkflow than its memo")
	}
	run, err := RunWorkflow(withRunScope(ctx, scope), runID, wf, input, plan, carry)
	var result any
	if run != nil {
		result = payloadcrypt.Seal(scope, run)
	}
	// A sealed run's failure leaves with its key; an unsealed run's is
	// marked only in a process that seals, whose failure converter withholds
	// an unmarked error. Elsewhere it is the error itself, so any failure
	// converter writes it as it always did.
	if sealed || payloadcrypt.ProcessSeals(ctx) {
		return result, payloadcrypt.WithKey(err, scope, reasonCodeOf(err))
	}
	return result, err
}

// startMatchesMemo holds a sealed start to what its memo says of the run (a
// memo field that is absent reads as empty, which no start's Orkflow name
// matches):
// the namespace its key is bound to, and the namespace and Orkflow the memo
// lists it under (rule 2).
func startMatchesMemo(ctx workflow.Context, st payloadcrypt.KeyState, wf *orkstackv1.Orkflow) bool {
	memo := workflow.GetInfo(ctx).Memo
	field := func(name string) (string, bool) {
		var v string
		p, ok := memo.GetFields()[name]
		if !ok {
			return "", true
		}
		return v, DataConverter().FromPayload(p, &v) == nil
	}
	ns, nsOK := field(MemoNamespace)
	name, nameOK := field(MemoOrkflowName)
	if wf == nil || !nsOK || !nameOK {
		return false
	}
	own := namespaceOrDefault(wf.GetNamespace())
	return own == namespaceOrDefault(st.Namespace()) && own == namespaceOrDefault(ns) && wf.GetName() == name
}

// namespaceOrDefault spells the built-in namespace one way: a definition or a
// memo that names none means "default" (AGENTS.md §7).
func namespaceOrDefault(ns string) string {
	if ns == "" {
		return "default"
	}
	return ns
}

// reasonCodeOf is the reason code a run's own failure carries, its
// application type, for the plaintext message of a sealed failure.
func reasonCodeOf(err error) string {
	failure := categoryOf(err)
	code := failure.appType()
	// The details are sealed, and a schedule's fire, which holds no run's
	// key, must tell a run a limit kept from starting from one that ran.
	if fireLimitCodes[code] {
		var ending ConcurrencyRunNotStarted
		if failure.appDetails(&ending) && ending.NotStarted {
			return code + notStartedSuffix
		}
	}
	return code
}

// keylessEnding ends a run whose start cannot be read, at once, having read
// nothing and started nothing: FAILED, RUN_START_UNREADABLE, an
// InfrastructureError never retried. Everything it writes is plaintext and
// built from the run's id and its memo's plaintext alone, since it opened
// nothing: its one event, RUN_FAILED at sequence 1, the snapshot that event
// names, and its failure. An execution that continues another publishes no
// event: it cannot read its carry, so it knows no sequence of the run's own
// (§4.1 rule 6; AGENTS.md §7.3). It needs no number from a plan: its writes
// use the options every terminal event uses.
//
// cause says which check failed, for the engine's log alone: it names no
// value of the start.
func keylessEnding(ctx workflow.Context, continued bool, cause string) (any, error) {
	var failure error = temporal.NewNonRetryableApplicationError(
		runStartUnreadableMessage, RunStartUnreadableCode, nil)
	if payloadcrypt.ProcessSeals(ctx) {
		// Fixed words and a reason code, written as they are by a process
		// that seals.
		failure = payloadcrypt.Keyless(failure)
	}
	info := workflow.GetInfo(ctx)
	runID := info.WorkflowExecution.ID
	workflow.GetLogger(ctx).Error("a run's start could not be read; it ends without starting anything",
		"code", RunStartUnreadableCode, "run_id", runID, "continued", continued, "cause", cause)
	if continued {
		return nil, failure
	}
	memoString := func(name string) string {
		var v string
		if p, ok := info.Memo.GetFields()[name]; ok {
			if err := DataConverter().FromPayload(p, &v); err != nil {
				return ""
			}
		}
		return v
	}
	namespace := namespaceOrDefault(memoString(MemoNamespace))
	// An id read from a memo is held to its shape before it names an object;
	// a run whose memo fails it ends with its failure alone.
	name, version := memoString(MemoOrkflowName), memoString(MemoOrkflowVersion)
	if !runIDShape.MatchString(runID) || dsl.ValidSlug("namespace", namespace) != nil ||
		(name != "" && dsl.ValidSlug("orkflow", name) != nil) || !versionShape.MatchString(version) {
		workflow.GetLogger(ctx).Error("a run whose start cannot be read has no well-formed id or namespace; it ends with no event",
			"code", RunStartUnreadableCode)
		return nil, failure
	}
	ectx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	now := timestamppb.New(workflow.Now(ectx))
	header := &orkstackv1.Run{
		Id:             runID,
		Namespace:      namespace,
		OrkflowName:    name,
		OrkflowVersion: version,
		Status:         orkstackv1.Status_STATUS_FAILED,
		Error: &orkstackv1.Error{Code: RunStartUnreadableCode, Message: runStartUnreadableMessage,
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		CreatedAt:  timestamppb.New(info.WorkflowStartTime),
		FinishedAt: now,
	}
	const sequence = int64(1)
	localCtx := workflow.WithLocalActivityOptions(payloadcrypt.PlainLocalResult(ectx), workflow.LocalActivityOptions{
		StartToCloseTimeout:    snapshotAttemptTimeout,
		ScheduleToCloseTimeout: snapshotActivityTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var snapshotURI string
	if err := workflow.ExecuteLocalActivity(localCtx, ActivityWriteFailedRunSnapshot,
		// A local activity's arguments are never converted; its result, a
		// snapshot's URI, is the run's own and plaintext here, as nothing of
		// the run was read.
		namespace, runID, (*orkstackv1.ContinuationObject)(nil), header, sequence).Get(localCtx, &snapshotURI); err != nil {
		workflow.GetLogger(ctx).Error("write the snapshot of a run whose start cannot be read failed",
			"run_id", runID, "error", err)
		return nil, failure
	}
	event := &orkstackv1.Event{
		RunId:     runID,
		Sequence:  sequence,
		Type:      orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		Status:    orkstackv1.Status_STATUS_FAILED,
		Message:   runStartUnreadableMessage,
		Timestamp: now,
	}
	activityCtx := workflow.WithActivityOptions(ectx, eventActivityOptions())
	// Plain, also where the memo's key opens and only the start does not (a
	// borrowed key): no reader is given a keyless run's key, so a sealed event
	// could be read by none. It is built from the memo's plaintext alone.
	scope := runScope(ctx)
	if err := workflow.ExecuteActivity(activityCtx, ActivityPublishEvent, payloadcrypt.PlainIn(scope, event),
		payloadcrypt.PlainIn(scope, snapshotURI), payloadcrypt.PlainIn(scope, fmt.Sprintf("%s#%d", runID, sequence))).Get(ectx, nil); err != nil {
		workflow.GetLogger(ctx).Error("publish event failed",
			"run_id", runID, "event", event.GetType().String(), "error", err)
	}
	return nil, failure
}
