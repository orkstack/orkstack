package engine

import (
	"context"
	"errors"
	"testing"

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
