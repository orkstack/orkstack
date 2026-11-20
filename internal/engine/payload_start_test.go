package engine_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const rawRunWorkflow = "RunWorkflowRaw"

// sealedRun is the keys and memo of one sealed run tree rooted at runID, in
// namespace "default", as a submit would make them.
type sealedRun struct {
	keys  *payloadcrypt.EngineKeys
	memo  payloadcrypt.KeyMemo
	scope *payloadcrypt.Scope
}

func newSealedRun(t *testing.T, runID string) sealedRun {
	t.Helper()
	ek, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	must.OK(t, err)
	dk, err := payloadcrypt.NewKey(bytes.Repeat([]byte{9}, payloadcrypt.KeySize))
	must.OK(t, err)
	keys, err := payloadcrypt.NewEngineKeys(ek)
	must.OK(t, err)
	tree, memo, err := payloadcrypt.NewRunTree(context.Background(), payloadcrypt.NewStaticDataKeys("pk_t", dk), ek, "default", "ns_default", runID)
	must.OK(t, err)
	return sealedRun{keys: keys, memo: memo, scope: payloadcrypt.NewScope(tree, payloadcrypt.RunContext(runID))}
}

// rawHarness is the workflow harness with the raw entry registered, its
// converters keyed with keys, and the run's memo set to memo (none for nil),
// as cmd/ork's run worker runs it.
func rawHarness(t *testing.T, runID string, keys *payloadcrypt.EngineKeys, memo map[string]any) *harness {
	t.Helper()
	h := newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"ok": true})}, nil
	})
	dc := payloadcrypt.NewConverter(engine.DataConverter(), keys)
	h.env.SetDataConverter(dc)
	h.env.SetFailureConverter(payloadcrypt.NewFailureConverter(dc, false))
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{payloadcrypt.WorkflowInterceptor(keys)}}))
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflowRaw, rawOptions())
	h.env.SetStartWorkflowOptions(clientOptions(runID))
	if memo != nil {
		must.OK(t, h.env.SetMemoOnStart(memo))
	}
	return h
}

func rawOptions() workflow.RegisterOptions { return workflow.RegisterOptions{Name: rawRunWorkflow} }

func clientOptions(runID string) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{ID: runID}
}

func runMemo(t *testing.T, s *sealedRun) map[string]any {
	t.Helper()
	m := engine.RunStartMemo(compile(t, linearDocument), defaultPlan())
	if s != nil {
		m[payloadcrypt.MemoField] = s.memo
	}
	return m
}

// sealedStartArgs is a run's four start arguments, each sealed under scope (or
// plaintext for nil).
func sealedStartArgs(t *testing.T, runID string, scope *payloadcrypt.Scope) []any {
	t.Helper()
	if scope == nil {
		scope = payloadcrypt.Unsealed()
	}
	args := []any{runID, compile(t, linearDocument), payload(t, nil), defaultPlan()}
	for i := range args {
		args[i] = payloadcrypt.Seal(scope, args[i])
	}
	return args
}

// A run that is not sealed runs through the raw entry exactly as through
// the typed one.
func TestRunWorkflowRaw_UnsealedRunsAsBefore(t *testing.T) {
	h := rawHarness(t, "ork-plain", nil, nil)
	h.env.ExecuteWorkflow(rawRunWorkflow, sealedStartArgs(t, "ork-plain", nil)...)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if run := mustFinalRun(t, h.store, "ork-plain"); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
}

// A sealed run opens its own start from its memo and a key file, runs, and
// returns its result sealed.
func TestRunWorkflowRaw_SealedStartOpens(t *testing.T) {
	s := newSealedRun(t, "ork-sealed")
	h := rawHarness(t, "ork-sealed", s.keys, runMemo(t, &s))
	h.env.ExecuteWorkflow(rawRunWorkflow, sealedStartArgs(t, "ork-sealed", s.scope)...)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if run := mustFinalRun(t, h.store, "ork-sealed"); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
	var raw converter.RawValue
	must.OK(t, h.env.GetWorkflowResult(&raw))
	if !payloadcrypt.IsSealed(raw.Payload()) {
		t.Fatalf("a sealed run's result was written in plaintext: %v", raw.Payload().GetMetadata())
	}
}

// A start that does not open ends at once, in plaintext, having started
// nothing: one event, RUN_FAILED, RUN_START_UNREADABLE.
func TestRunWorkflowRaw_KeylessEnding(t *testing.T) {
	s := newSealedRun(t, "ork-keyless")
	sibling := s.scope.For(payloadcrypt.RunContext("ork-sibling"))
	otherNamespace := s.memo
	otherNamespace.Namespace = "payments"
	cut := s.memo
	cut.Wrap = cut.Wrap[:8]
	cases := []struct {
		name string
		memo map[string]any
		args []any
	}{
		{"a start sealed for another run", runMemo(t, &s), sealedStartArgs(t, "ork-keyless", sibling)},
		{"a plaintext start in a sealed run", runMemo(t, &s), sealedStartArgs(t, "ork-keyless", nil)},
		{"a start that lacks its plan", runMemo(t, &s), sealedStartArgs(t, "ork-keyless", s.scope)[:3]},
		{"a wrap cut short", withKeyField(runMemo(t, nil), cut), sealedStartArgs(t, "ork-keyless", s.scope)},
		{"a memo that names another namespace", withKeyField(runMemo(t, nil), otherNamespace), sealedStartArgs(t, "ork-keyless", s.scope)},
		{"a sealed start in a run that is not sealed", runMemo(t, nil), sealedStartArgs(t, "ork-keyless", s.scope)},
		{"a schedule's key field", withKeyField(runMemo(t, nil), scheduleKeyMemo(t, s)), sealedStartArgs(t, "ork-keyless", s.scope)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := rawHarness(t, "ork-keyless", s.keys, tc.memo)
			h.env.ExecuteWorkflow(rawRunWorkflow, tc.args...)
			err := h.env.GetWorkflowError()
			if err == nil || !strings.Contains(err.Error(), engine.RunStartUnreadableCode) {
				t.Fatalf("workflow error = %v, want %s", err, engine.RunStartUnreadableCode)
			}
			if len(h.invocations()) != 0 {
				t.Fatalf("a keyless run dispatched %d tasks", len(h.invocations()))
			}
			events := h.bus.Events("ork-keyless", 0)
			if len(events) != 1 || events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || events[0].GetSequence() != 1 {
				t.Fatalf("events = %v, want one RUN_FAILED at sequence 1", events)
			}
			run, ok := h.store.Get("ork-keyless")
			if !ok || run.GetError().GetCode() != engine.RunStartUnreadableCode || run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
				t.Fatalf("stored run = %v", run)
			}
		})
	}
}

func withKeyField(m map[string]any, k payloadcrypt.KeyMemo) map[string]any {
	m[payloadcrypt.MemoField] = k
	return m
}

// An engine that lacks the engines' key a run's memo names decides nothing:
// the workflow task fails, no event is written.
func TestRunWorkflowRaw_MissPanics(t *testing.T) {
	s := newSealedRun(t, "ork-miss")
	k, err := payloadcrypt.NewKey(bytes.Repeat([]byte{8}, payloadcrypt.KeySize))
	must.OK(t, err)
	other, err := payloadcrypt.NewEngineKeys(k)
	must.OK(t, err)
	h := rawHarness(t, "ork-miss", other, runMemo(t, &s))
	h.env.ExecuteWorkflow(rawRunWorkflow, sealedStartArgs(t, "ork-miss", s.scope)...)
	err = h.env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), payloadcrypt.ErrEngineKeyMissing.Error()) {
		t.Fatalf("workflow error = %v, want the guard's panic", err)
	}
	if n := len(h.bus.Events("ork-miss", 0)); n != 0 {
		t.Fatalf("an engine that lacks the key wrote %d events", n)
	}
}

// scheduleKeyMemo is a schedule's key field under s's engines' key, which a
// run's start never opens with.
func scheduleKeyMemo(t *testing.T, s sealedRun) payloadcrypt.KeyMemo {
	t.Helper()
	ek, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	must.OK(t, err)
	dk, err := payloadcrypt.NewKey(bytes.Repeat([]byte{9}, payloadcrypt.KeySize))
	must.OK(t, err)
	_, memo, err := payloadcrypt.NewSchedule(context.Background(), payloadcrypt.NewStaticDataKeys("pk_t", dk), ek, "default", "ns_default", "ork-keyless")
	must.OK(t, err)
	return memo
}
