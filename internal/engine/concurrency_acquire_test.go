package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// failingGateClient is a cluster whose acquire Update fails as a frontend
// that cannot be reached does: a fault a real cluster does not produce on
// demand, so it is faked here. Every other method is unused.
type failingGateClient struct {
	client.Client
	err   error
	calls int
}

func (f *failingGateClient) UpdateWorkflow(context.Context, client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	f.calls++
	return nil, f.err
}

// A cancelled acquire, as a stopping worker cancels it, is an error the
// acquire is run again for, never an unavailable limit that fails the run;
// only its own deadline is that.
func TestAcquireConcurrency_ACancelIsNotUnavailable(t *testing.T) {
	a := &GateActivities{client: &failingGateClient{err: serviceerror.NewUnavailable("frontend down")}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := a.AcquireConcurrency(ctx, AcquireConcurrencyInput{GateID: "g"})
	if !errors.Is(err, context.Canceled) || res.Unavailable != "" {
		t.Fatalf("a cancelled acquire = %+v, %v; want the cancel as an error", res, err)
	}
	dctx, dcancel := context.WithTimeout(context.Background(), 0)
	defer dcancel()
	res, err = a.AcquireConcurrency(dctx, AcquireConcurrencyInput{GateID: "g"})
	if err != nil || res.Unavailable == "" {
		t.Fatalf("an acquire past its deadline = %+v, %v; want unavailable", res, err)
	}
}

// An error no retry can change is answered at once, with its cause.
func TestAcquireConcurrency_APermanentErrorIsNotRetried(t *testing.T) {
	f := &failingGateClient{err: serviceerror.NewPermissionDenied("no", "")}
	res, err := (&GateActivities{client: f}).AcquireConcurrency(context.Background(), AcquireConcurrencyInput{GateID: "g"})
	if err != nil || res.Unavailable == "" || f.calls != 1 {
		t.Fatalf("a refused acquire = %+v, %v after %d calls; want unavailable at once", res, err, f.calls)
	}
}

// notReadyGateClient is a cluster that answers the first acquire Update
// WorkflowNotReady, as it does an update that arrives while the gate's
// workflow task that carries its state on or completes it fails
// (UnhandledCommand): a race a real cluster does not produce on demand, so
// it is faked here. onGet puts the error on the handle's Get rather than on
// the call. Every later Update is answered granted.
type notReadyGateClient struct {
	client.Client
	onGet bool
	calls int
}

type gateUpdateHandle struct {
	err error
	ans gateAnswer
}

func (h gateUpdateHandle) WorkflowID() string { return "g" }
func (h gateUpdateHandle) RunID() string      { return "" }
func (h gateUpdateHandle) UpdateID() string   { return "u" }
func (h gateUpdateHandle) Get(_ context.Context, out any) error {
	if h.err != nil {
		return h.err
	}
	*out.(*gateAnswer) = h.ans
	return nil
}

func (f *notReadyGateClient) UpdateWorkflow(context.Context, client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	f.calls++
	notReady := serviceerror.NewWorkflowNotReady("Unable to perform workflow execution update due to unexpected workflow task failure.")
	if f.calls == 1 {
		if f.onGet {
			return gateUpdateHandle{err: notReady}, nil
		}
		return nil, notReady
	}
	return gateUpdateHandle{ans: gateAnswer{Proto: 1, Answer: gateAnswerGranted, Seq: 3}}, nil
}

// TestAcquireConcurrency_AnUpdateTheGateCouldNotTakeIsAskedAgain: an acquire
// Update the cluster answers WorkflowNotReady was never accepted by the gate,
// so it is asked again and granted, well inside its acquire_timeout, and is
// never an unavailable limit.
func TestAcquireConcurrency_AnUpdateTheGateCouldNotTakeIsAskedAgain(t *testing.T) {
	for _, onGet := range []bool{false, true} {
		f := &notReadyGateClient{onGet: onGet}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		res, err := (&GateActivities{client: f}).AcquireConcurrency(ctx, AcquireConcurrencyInput{GateID: "g", BackoffMin: time.Millisecond, BackoffMax: time.Millisecond})
		cancel()
		if err != nil || res.Unavailable != "" || res.Unsupported != "" || res.Answer.Answer != gateAnswerGranted || f.calls != 2 {
			t.Fatalf("error on Get %v: acquire = %+v, %v after %d calls; want granted on the second", onGet, res, err, f.calls)
		}
	}
}
