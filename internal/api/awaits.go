package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// awaitReasonMaxBytes bounds a rejection's reason. It is recorded on the
// task's error and in the audit trail, so it is one bounded human sentence.
const awaitReasonMaxBytes = 1024

// awaitInlineValuesLimit is the size at or below which a resolution's values
// travel inline in the signal, as ExecuteTaskResult.values_json does: the same
// bound, for the same reason (AGENTS.md §2.2).
const awaitInlineValuesLimit = 4096

// awaitCallbackSecret is one minted token in this process's memory only, from
// prepare to the response that discloses it. It is never logged, stored,
// claimed, snapshotted or archived.
type awaitCallbackSecret struct {
	TaskID string
	Token  string
}

// mintAwaitCallbacks mints one token per `callback: true` await of wf, in
// declaration order, records each one's salt and digest on plan.Awaits, and
// returns the plaintexts. It reuses the trigger primitives — triggers.NewSecret
// (32 bytes from crypto/rand, hex), triggers.NewSalt (16 bytes),
// triggers.HashSecret (sha256(salt || token)) — for the reason
// docs/ORKFLOW_TRIGGERS.md gives: a fast hash is right for a 256-bit random
// secret. A definition with no callback await leaves plan.Awaits nil.
func mintAwaitCallbacks(wf *orkstackv1.Orkflow, plan *engine.RunPlan) ([]awaitCallbackSecret, error) {
	var minted []awaitCallbackSecret
	for _, task := range wf.GetTasks() {
		if !task.GetAwait().GetCallback() {
			continue
		}
		token, err := triggers.NewSecret()
		if err != nil {
			return nil, err
		}
		salt, err := triggers.NewSalt()
		if err != nil {
			return nil, err
		}
		if plan.Awaits == nil {
			plan.Awaits = map[string]engine.AwaitCredential{}
		}
		plan.Awaits[task.GetId()] = engine.AwaitCredential{
			Salt:   hex.EncodeToString(salt),
			Digest: hex.EncodeToString(triggers.HashSecret(salt, token)),
		}
		minted = append(minted, awaitCallbackSecret{TaskID: task.GetId(), Token: token})
	}
	return minted, nil
}

// verifyAwaitToken hashes presented with the credential's salt and compares in
// constant time (crypto/subtle.ConstantTimeCompare). An empty token, or a
// credential whose hex does not decode, never verifies.
func verifyAwaitToken(cred engine.AwaitCredential, presented string) bool {
	if presented == "" {
		return false
	}
	salt, err := hex.DecodeString(cred.Salt)
	if err != nil || len(salt) == 0 {
		return false
	}
	digest, err := hex.DecodeString(cred.Digest)
	if err != nil || len(digest) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare(digest, triggers.HashSecret(salt, presented)) == 1
}

// awaitCallbackPath is the callback door's path for one await:
// "/v1/runs/" + url.PathEscape(runID) + "/awaits/" + taskID.
func awaitCallbackPath(runID, taskID string) string {
	return "/v1/runs/" + url.PathEscape(runID) + "/awaits/" + taskID
}

// awaitCallbacksFor renders the minted secrets as the response field, with
// every path built from the run's FINAL id.
func awaitCallbacksFor(runID string, minted []awaitCallbackSecret) []*orkstackv1.AwaitCallback {
	if len(minted) == 0 {
		return nil
	}
	out := make([]*orkstackv1.AwaitCallback, 0, len(minted))
	for _, secret := range minted {
		out = append(out, &orkstackv1.AwaitCallback{
			TaskId: secret.TaskID,
			Path:   awaitCallbackPath(runID, secret.TaskID),
			Token:  secret.Token,
		})
	}
	return out
}

// awaitResolution is one authenticated request to resolve an await, whichever
// door it came through.
type awaitResolution struct {
	binding   engine.Binding
	run       *orkstackv1.Run        // the read model, namespace already decided
	start     *engine.RunStartRecord // definition and plan, from the start event
	taskID    string
	reject    bool
	reason    string
	values    []byte // exact bytes; empty means {}
	via       orkstackv1.AwaitResolvedVia
	principal string // bounded, never authenticated
}

// isTerminalRunStatus is a run status nothing moves on from.
func isTerminalRunStatus(status orkstackv1.Status) bool {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_CANCELED:
		return true
	}
	return false
}

// awaitStatusWord is a status as every client prints it: "AWAITING", not
// "STATUS_AWAITING".
func awaitStatusWord(status orkstackv1.Status) string {
	return strings.TrimPrefix(status.String(), "STATUS_")
}

// taskRunIn is one task's run record, or nil when the read model has none yet
// (a run whose first snapshot has not been written).
func taskRunIn(run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == taskID {
			return tr
		}
	}
	return nil
}

// checkAwaitReason holds a rejection's reason to its bound.
func checkAwaitReason(reason string) error {
	if reason == "" {
		return connect.NewError(connect.CodeInvalidArgument,
			errors.New("reason is required with a rejection: say why the await is rejected"))
	}
	if len(reason) > awaitReasonMaxBytes {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("reason is %d bytes, over the %d-byte limit", len(reason), awaitReasonMaxBytes))
	}
	for i := 0; i < len(reason); i++ {
		if b := reason[i]; (b < 0x20 && b != '\t' && b != '\n') || b == 0x7f {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("reason contains control character 0x%02x at byte %d; only tab and newline are allowed", b, i))
		}
	}
	return nil
}

// resolveAwait checks, stores, and delivers. It writes nothing before its
// last check has passed, and sends nothing if the store write failed.
//
// The response says the resolution was DELIVERED — durably recorded in the
// run's history — not that it was applied: a resolution that passes these
// checks can still lose to a concurrent one, to the deadline, or to the run's
// end, and the run then records it as an ignored resolution, never silently.
func (s *Service) resolveAwait(ctx context.Context, r awaitResolution) (*orkstackv1.ResolveAwaitResponse, error) {
	runID := r.run.GetId()
	if !r.reject && len(r.values) == 0 {
		r.values = []byte("{}")
	}

	// 1. The task, in the PINNED definition — not the store's current one.
	var task *orkstackv1.Task
	for _, t := range r.start.Orkflow.GetTasks() {
		if t.GetId() == r.taskID {
			task = t
		}
	}
	if task == nil {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("run %q has no task %q", runID, r.taskID))
	}
	if task.GetAwait() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("task %q of run %q is not an awaited task", r.taskID, runID))
	}

	response := &orkstackv1.ResolveAwaitResponse{
		RunId:     runID,
		Namespace: artifacts.ResolveNamespace(r.run.GetNamespace()),
		TaskId:    r.taskID,
		Reject:    r.reject,
	}
	digest := sha256.Sum256(r.values)
	valuesSHA := hex.EncodeToString(digest[:])

	// 2. The state, from the read model. An identical repeat of the winning
	//    resolution is answered as the first delivery was, whether or not the
	//    run has ended since, so a caller retrying after a lost response is
	//    told success and nothing is sent twice.
	taskRun := taskRunIn(r.run, r.taskID)
	status := taskRun.GetStatus()
	if status == orkstackv1.Status_STATUS_UNSPECIFIED {
		status = orkstackv1.Status_STATUS_PENDING
	}
	switch {
	case status == orkstackv1.Status_STATUS_SUCCEEDED && !r.reject &&
		taskRun.GetValuesRef().GetSha256() == valuesSHA:
		response.ValuesSha256 = valuesSHA
		response.Repeat = true
		return response, nil
	case status == orkstackv1.Status_STATUS_FAILED && r.reject &&
		taskRun.GetError().GetCode() == engine.AwaitRejectedCode &&
		taskRun.GetError().GetMessage() == engine.AwaitRejectedMessage(r.taskID, r.reason):
		response.Repeat = true
		return response, nil
	case isTerminalRunStatus(r.run.GetStatus()):
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("run %q has ended (%s); an await of an ended run cannot be resolved",
				runID, awaitStatusWord(r.run.GetStatus())))
	case status == orkstackv1.Status_STATUS_PENDING:
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("await %q has not started awaiting; resolve it once it is AWAITING (its needs are not complete yet)",
				r.taskID))
	case status == orkstackv1.Status_STATUS_AWAITING:
	case status == orkstackv1.Status_STATUS_SKIPPED:
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("await %q was skipped; it accepts no resolution", r.taskID))
	case status == orkstackv1.Status_STATUS_CANCELED:
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("await %q was canceled when its run ended; it accepts no resolution", r.taskID))
	default:
		word := awaitStatusWord(status)
		if code := taskRun.GetError().GetCode(); code != "" {
			word += " (" + code + ")"
		}
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("await %q is %s; it accepts no resolution", r.taskID, word))
	}

	// 3. The request.
	if r.reject {
		if err := checkAwaitReason(r.reason); err != nil {
			return nil, err
		}
	} else {
		limit := r.start.Plan.Limits.Proto().GetValuesMaxBytes()
		if err := dsl.ValidateAwaitValues(task.GetAwait(), r.values, limit); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	if r.binding.Starter == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("no run starter configured"))
	}
	signal := &orkstackv1.AwaitSignal{
		TaskId:    r.taskID,
		Reject:    r.reject,
		Reason:    r.reason,
		Via:       r.via,
		Principal: r.principal,
	}

	// 4. Store the values — AGENTS.md §4.1's commit order applied to an
	//    await: the object is durable BEFORE the only record that references
	//    it (the run's history) is written. The exact arguments the Orker uses
	//    for a container's values.json, so it lands in the same
	//    content-addressed key family with the same provenance.
	if !r.reject {
		if s.artifactStore == nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				errors.New("this engine has no artifact store configured"))
		}
		meta := artifacts.Provenance{
			Orkflow: r.start.Orkflow.GetName(),
			TaskID:  r.taskID,
			RunID:   runID,
		}.Metadata(nil)
		meta["artifact"] = "values.json"
		ref, err := s.artifactStore.Put(ctx, "values.json", bytes.NewReader(r.values), artifacts.PutOptions{
			ContentType: "application/json",
			Metadata:    meta,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("store the resolution's values: %v", err))
		}
		if ref.SHA256 != valuesSHA {
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("the store recorded the resolution's values as sha256 %s, but their bytes hash to %s",
					ref.SHA256, valuesSHA))
		}
		signal.Values = &orkstackv1.ArtifactRef{
			Name:        ref.Name,
			Uri:         ref.URI,
			ContentType: ref.ContentType,
			SizeBytes:   ref.SizeBytes,
			Sha256:      ref.SHA256,
		}
		if len(r.values) <= awaitInlineValuesLimit {
			signal.ValuesJson = r.values
		}
		response.ValuesSha256 = valuesSHA
	}

	// 5. Deliver: exactly one signal, answered with its outcome. Nothing is
	//    queued and nothing is retried.
	if err := r.binding.Starter.SignalAwait(ctx, runID, signal); err != nil {
		if errors.Is(err, engine.ErrRunEnded) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("run %q has ended", runID))
		}
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("deliver the resolution to run %q: %v", runID, err))
	}
	return response, nil
}

// awaitStartOf reads what a run was started with, from its own start event.
func awaitStartOf(ctx context.Context, binding engine.Binding, runID string) (*engine.RunStartRecord, error) {
	if binding.Reader == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("no run reader configured"))
	}
	start, err := binding.Reader.RunStart(ctx, runID)
	if err != nil {
		if errors.Is(err, engine.ErrRunNotFound) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("run %q has ended and is no longer live", runID))
		}
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read the start of run %q: %w", runID, err))
	}
	return start, nil
}

// ResolveAwait completes or rejects an awaited task of a running run. It is a
// write addressed inside a namespace exactly as CancelRun is, and every
// awaited task is resolvable here, `callback: true` or not: the Connect API
// is the deployment's operator surface, guarded by the operator's gateway.
func (s *Service) ResolveAwait(
	ctx context.Context,
	req *connect.Request[orkstackv1.ResolveAwaitRequest],
) (*connect.Response[orkstackv1.ResolveAwaitResponse], error) {
	msg := req.Msg
	if msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("run_id is required"))
	}
	if msg.GetTaskId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("task_id is required"))
	}
	if msg.GetReject() && msg.GetValuesJson() != "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("values_json and reject are exclusive"))
	}
	if !msg.GetReject() && msg.GetReason() != "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("reason is only for a rejection"))
	}
	run, err := s.LookupRunIn(ctx, msg.GetRunId(), msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	binding, err := s.locate(ctx, msg.GetRunId())
	if err != nil {
		return nil, err
	}
	start, err := awaitStartOf(ctx, binding, msg.GetRunId())
	if err != nil {
		return nil, err
	}
	resp, err := s.resolveAwait(ctx, awaitResolution{
		binding:   binding,
		run:       run,
		start:     start,
		taskID:    msg.GetTaskId(),
		reject:    msg.GetReject(),
		reason:    msg.GetReason(),
		values:    []byte(msg.GetValuesJson()),
		via:       orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API,
		principal: boundOriginAssertion(req.Header().Get(AuditPrincipalHeader)),
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
