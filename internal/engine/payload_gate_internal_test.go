package engine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// A concurrency gate is no run and holds no run's key: what a sealed run
// tells it (ids, sequences and indices only) is written plaintext, or a
// sealed run's slots would never be released.
func TestGate_ReadsASealedRunsReleases(t *testing.T) {
	ek, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	must.OK(t, err)
	dk, err := payloadcrypt.NewKey(bytes.Repeat([]byte{9}, payloadcrypt.KeySize))
	must.OK(t, err)
	keys, err := payloadcrypt.NewEngineKeys(ek)
	must.OK(t, err)
	_, field, err := payloadcrypt.NewRunTree(context.Background(), payloadcrypt.NewStaticDataKeys("pk_t", dk), ek, "default", "ns_default", "ork-held")
	must.OK(t, err)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(NewDataConverter(keys))
	env.SetWorkerOptions(worker.Options{Interceptors: append(
		[]interceptor.WorkerInterceptor{PayloadInterceptor(keys)}, RunWorkerInterceptors()...)})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-held"})
	must.OK(t, env.SetMemoOnStart(map[string]any{payloadcrypt.MemoField: field}))

	var all GateReleaseAll
	var one GateRelease
	gate := func(ctx workflow.Context) error {
		timeout := workflow.NewTimer(ctx, time.Hour)
		for got := 0; got < 2; {
			done := false
			sel := workflow.NewSelector(ctx)
			sel.AddReceive(workflow.GetSignalChannel(ctx, GateSignalReleaseAll), func(c workflow.ReceiveChannel, _ bool) {
				c.Receive(ctx, &all)
				got++
			})
			sel.AddReceive(workflow.GetSignalChannel(ctx, GateSignalRelease), func(c workflow.ReceiveChannel, _ bool) {
				c.Receive(ctx, &one)
				got++
			})
			sel.AddFuture(timeout, func(workflow.Future) { done = true })
			sel.Select(ctx)
			if done {
				return nil
			}
		}
		return nil
	}
	env.RegisterWorkflowWithOptions(gate, workflow.RegisterOptions{Name: "gate"})
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		child := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: "gate-1"}), "gate")
		if err := child.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
			return err
		}
		conc := newConcRunState(&ConcurrencyPlan{SeqBase: 4, Run: new(int), Slots: []ConcurrencySlotPlan{{GateID: "gate-1"}}})
		conc.runID, conc.attempted = "ork-held", []string{"gate-1"}
		s := &runState{runID: "ork-held", scope: runScope(ctx), conc: conc}
		if err := s.releaseRunEntry(ctx, "gate-1"); err != nil {
			return err
		}
		releaseSlots(ctx, s.conc)
		return child.Get(ctx, nil)
	}, workflow.RegisterOptions{Name: "held"})
	env.ExecuteWorkflow("held")
	must.OK(t, env.GetWorkflowError())
	if all.Run != "ork-held" || all.Below != 5 {
		t.Errorf("the gate read release_all %+v, want the sealed run's", all)
	}
	if one.Run != "ork-held" || one.SeqBase != 4 {
		t.Errorf("the gate read release %+v, want the sealed run's", one)
	}
}
