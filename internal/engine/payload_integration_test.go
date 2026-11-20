//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestIntegration_ASealedRunThatFailsReadsBackFailed: a sealed run started
// and run by an engine holding the engines' key fails, and a replica that did
// not start it, and holds no key, reads it back FAILED with its reason code
// through GetRun. The failure's text is sealed in the cluster's history; its
// type, the reason code, is plaintext (payload-encryption.md §2).
func TestIntegration_ASealedRunThatFailsReadsBackFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	plain := dialRunIDCluster(t, ctx) // the reading replica: the SDK's own converters, no key

	ek, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	must.OK(t, err)
	dk, err := payloadcrypt.NewKey(bytes.Repeat([]byte{9}, payloadcrypt.KeySize))
	must.OK(t, err)
	keys, err := payloadcrypt.NewEngineKeys(ek)
	must.OK(t, err)
	runID := engine.NewRunID()
	tree, keyField, err := payloadcrypt.NewRunTree(ctx, payloadcrypt.NewStaticDataKeys("pk_it", dk), ek, "default", "ns_default", runID)
	must.OK(t, err)
	scope := payloadcrypt.NewScope(tree, payloadcrypt.RunContext(runID))

	// The starting engine: its own converters over the engines' key.
	dc := engine.NewDataConverter(keys)
	sealing, err := client.NewClientFromExisting(plain, client.Options{
		Namespace: runIDNamespace(), DataConverter: dc, FailureConverter: payloadcrypt.NewFailureConverter(dc, false),
	})
	must.OK(t, err)
	t.Cleanup(sealing.Close)
	queue := runIDQueue()
	w := worker.New(sealing, queue, worker.Options{
		Interceptors: append([]interceptor.WorkerInterceptor{payloadcrypt.WorkflowInterceptor(keys)}, engine.RunWorkerInterceptors()...),
	})
	engine.RegisterWorkflows(w)
	must.OK(t, w.Start())
	t.Cleanup(w.Stop)

	// An Orkflow with no tasks: the interpreter refuses it, INVALID_ORKFLOW,
	// before it starts anything, so no activity is needed.
	wf := &orkstackv1.Orkflow{Name: "sealed-empty", Namespace: "default", Version: "1"}
	memo := engine.RunStartMemo(wf, defaultPlan())
	memo[payloadcrypt.MemoField] = keyField
	terminateAll(t, plain, runID)
	run, err := sealing.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: runID, TaskQueue: queue, Memo: memo},
		engine.RunWorkflowType, payloadcrypt.Seal(scope, runID), payloadcrypt.Seal(scope, wf),
		payloadcrypt.Seal(scope, &orkstackv1.Payload{}), payloadcrypt.Seal(scope, defaultPlan()))
	must.OK(t, err)
	if err := run.Get(ctx, nil); err == nil {
		t.Fatal("the sealed run did not fail")
	}

	// The cluster holds the failure's text sealed.
	events := plain.GetWorkflowHistory(ctx, runID, "", false, 0)
	var closed bool
	for events.HasNext() {
		ev, err := events.Next()
		must.OK(t, err)
		if f := ev.GetWorkflowExecutionFailedEventAttributes().GetFailure(); f != nil {
			closed = true
			if !payloadcrypt.IsSealed(f.GetEncodedAttributes()) || bytes.Contains([]byte(f.String()), []byte("has no tasks")) {
				t.Fatalf("the run's failure is in plaintext in the cluster: %v", f)
			}
		}
	}
	if !closed {
		t.Fatal("the run's history holds no failure")
	}

	// A replica that did not start it, holding no key, reads it back.
	got, err := engine.NewTemporalRunReader(plain).GetRun(ctx, runID)
	must.OK(t, err)
	// The reader reads a closed run's failure from its message
	// (closedExecution), which for a sealed run is its reason code, never its
	// sealed text.
	if got.GetStatus() != orkstackv1.Status_STATUS_FAILED || got.GetError().GetMessage() != "INVALID_ORKFLOW" ||
		bytes.Contains([]byte(got.GetError().String()), []byte("has no tasks")) {
		t.Fatalf("GetRun = %v / %v, want FAILED with the reason code INVALID_ORKFLOW and none of its sealed text", got.GetStatus(), got.GetError())
	}
}
