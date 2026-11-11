package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// cancelClock is the clock a CancelRun under test runs its retry on. Nothing
// waits on it: a spacing is recorded and answered at once (or, held, never,
// so the test ends the wait itself), and the retry budget ends when the test
// says, as a deadline does, not after a stretch of wall clock.
type cancelClock struct {
	mu    sync.Mutex
	waits []time.Duration
	// hold makes a wait never fire; onWait, when set, runs as one begins.
	hold   bool
	onWait func()
	spend  context.CancelCauseFunc
}

func (c *cancelClock) bound(parent context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	c.mu.Lock()
	c.spend = cancel
	c.mu.Unlock()
	return ctx, func() { cancel(context.Canceled) }
}

func (c *cancelClock) wait(interval time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.waits = append(c.waits, interval)
	hold, onWait := c.hold, c.onWait
	c.mu.Unlock()
	if onWait != nil {
		onWait()
	}
	if hold {
		return nil
	}
	fired := make(chan time.Time, 1)
	fired <- time.Time{}
	return fired
}

// spendBudget ends the retry budget the way its deadline would.
func (c *cancelClock) spendBudget() {
	c.mu.Lock()
	spend := c.spend
	c.mu.Unlock()
	spend(context.DeadlineExceeded)
}

func (c *cancelClock) spacings() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.waits...)
}

// cancelClient records the two ways CancelRun asks the cluster for a
// cancellation: the SDK's CancelWorkflow, and the service's own request,
// which answers with each of refusals in turn and then success.
type cancelClient struct {
	client.Client
	service  *cancelService
	clock    *cancelClock
	canceled []string
}

func (c *cancelClient) CancelWorkflow(_ context.Context, workflowID, _ string) error {
	c.canceled = append(c.canceled, workflowID)
	return nil
}

func (c *cancelClient) WorkflowService() workflowservice.WorkflowServiceClient { return c.service }

// starter is a starter over the fake cluster, on the fake's clock.
func (c *cancelClient) starter() *engine.TemporalStarter {
	c.clock = &cancelClock{}
	return engine.NewTemporalStarter(c, "").WithClusterIdentity("ork-main", "engine-a").
		WithCancelClockForTest(c.clock.bound, c.clock.wait)
}

type cancelService struct {
	workflowservice.WorkflowServiceClient
	refusals []error
	// endless, when set, is every answer once refusals are spent; hang, when
	// set, makes the call after them wait out its context instead, answering
	// as the SDK does for a call whose context ended: its deadline, or its
	// cancellation, by the cause.
	endless  error
	hang     bool
	onCall   func()
	requests []*workflowservice.RequestCancelWorkflowExecutionRequest
}

// runaway is how many requests a test tolerates before it ends the retry
// itself, so a CancelRun that ignored its budget fails the test rather than
// never returning.
const runaway = 100

// hangGuard is how long a hanging call waits for its context before the fake
// gives up on it. It only bounds a failing test.
const hangGuard = 10 * time.Second

func (s *cancelService) RequestCancelWorkflowExecution(
	ctx context.Context,
	in *workflowservice.RequestCancelWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	s.requests = append(s.requests, in)
	if s.onCall != nil {
		s.onCall()
	}
	if len(s.requests) > runaway {
		return nil, errors.New("the retry went on past any budget")
	}
	if len(s.refusals) > 0 {
		err := s.refusals[0]
		s.refusals = s.refusals[1:]
		return nil, err
	}
	if s.endless != nil {
		return nil, s.endless
	}
	if s.hang {
		// A call that its context never ends is a CancelRun that lost its
		// budget: fail it after a stretch no passing run waits for (this
		// select only ever leaves by ctx.Done() when the test passes).
		select {
		case <-ctx.Done():
		case <-time.After(hangGuard):
			return nil, errors.New("the call was never ended by its context")
		}
		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			return nil, serviceerror.NewDeadlineExceeded(ctx.Err().Error())
		}
		return nil, serviceerror.NewCanceled(ctx.Err().Error())
	}
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// newCancelStarter is a starter over a fake cluster answering refusals in
// turn, on a clock that waits for nothing.
func newCancelStarter(refusals ...error) (*engine.TemporalStarter, *cancelClient) {
	fake := &cancelClient{service: &cancelService{refusals: refusals}}
	return fake.starter(), fake
}

// A principal travels in the cancellation request itself, on the namespace
// and identity the client was dialed with, as a record the reader decodes.
func TestCancelRun_CarriesThePrincipalInTheRequest(t *testing.T) {
	starter, fake := newCancelStarter()
	if err := starter.CancelRun(context.Background(), "ork-1",
		engine.CancelRequest{Reason: "stuck", Principal: "dana@acme"}); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if len(fake.canceled) != 0 || len(fake.service.requests) != 1 {
		t.Fatalf("CancelWorkflow %d times, the service %d times; want the service once",
			len(fake.canceled), len(fake.service.requests))
	}
	sent := fake.service.requests[0]
	if sent.GetNamespace() != "ork-main" || sent.GetIdentity() != "engine-a" ||
		sent.GetWorkflowExecution().GetWorkflowId() != "ork-1" || sent.GetWorkflowExecution().GetRunId() != "" ||
		sent.GetRequestId() == "" {
		t.Fatalf("request = %v, want ork-1 on ork-main as engine-a, any execution, a request id", sent)
	}
	var record orkstackv1.RunCancelRequest
	must.OKf(t, protojson.Unmarshal([]byte(sent.GetReason()), &record), "reason %q is not a RunCancelRequest", sent.GetReason())
	if record.GetFormat() != 1 || record.GetPrincipal() != "dana@acme" {
		t.Fatalf("record = %v, want format 1 naming dana@acme", &record)
	}
}

// With no principal there is nothing to record: the cancellation is the
// SDK's, exactly as before.
func TestCancelRun_WithNoPrincipalIsTheSDKCancel(t *testing.T) {
	starter, fake := newCancelStarter()
	must.OKf(t, starter.CancelRun(context.Background(), "ork-1", engine.CancelRequest{Reason: "stuck"}), "CancelRun")
	if len(fake.canceled) != 1 || fake.canceled[0] != "ork-1" || len(fake.service.requests) != 0 {
		t.Fatalf("CancelWorkflow %v, the service %d times; want CancelWorkflow(ork-1) alone",
			fake.canceled, len(fake.service.requests))
	}
}

// A starter that was never told its namespace refuses rather than cancel
// without the name: the run would otherwise say nobody canceled it.
func TestCancelRun_RefusesAPrincipalItCannotRecord(t *testing.T) {
	fake := &cancelClient{service: &cancelService{}}
	starter := engine.NewTemporalStarter(fake, "")
	err := starter.CancelRun(context.Background(), "ork-1", engine.CancelRequest{Principal: "dana@acme"})
	if !errors.Is(err, engine.ErrCancelNotWired) {
		t.Fatalf("CancelRun = %v, want ErrCancelNotWired", err)
	}
	if len(fake.canceled) != 0 || len(fake.service.requests) != 0 {
		t.Fatal("a refused cancellation reached the cluster")
	}
}

// A cluster that refuses for a moment — unavailable, rate-limiting, aborting
// the call — is asked again under the one request id it dedupes on, until it
// accepts or the budget is spent; any other refusal is the answer at once.
// Between two tries the starter backs off, and stops backing off at a cap.
func TestCancelRun_AsksAgainOnlyWhileTheRefusalIsTransient(t *testing.T) {
	unavailable := serviceerror.NewUnavailable("frontend restarting")
	tests := []struct {
		name     string
		refusals []error
		endless  error
		// spendOn is the call during which the retry budget ends, 0 for
		// none: the cluster keeps refusing until it does.
		spendOn  int
		wantSent int
		wantErr  bool
	}{
		{name: "unavailable twice, then accepted", refusals: []error{unavailable, unavailable}, wantSent: 3},
		{name: "rate-limited, then accepted", refusals: []error{serviceerror.NewResourceExhausted(0, "busy")}, wantSent: 2},
		{name: "aborted, then accepted", refusals: []error{serviceerror.NewAborted("try again")}, wantSent: 2},
		{name: "unavailable until the budget is spent", endless: unavailable, spendOn: 8, wantSent: 8, wantErr: true},
		{name: "not found is final", refusals: []error{serviceerror.NewNotFound("workflow execution already completed")}, wantSent: 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			starter, fake := newCancelStarter(tt.refusals...)
			fake.service.endless = tt.endless
			if tt.spendOn > 0 {
				fake.service.onCall = func() {
					if len(fake.service.requests) == tt.spendOn {
						fake.clock.spendBudget()
					}
				}
			}
			err := starter.CancelRun(context.Background(), "ork-1", engine.CancelRequest{Principal: "dana@acme"})
			if (err != nil) != tt.wantErr {
				t.Fatalf("CancelRun = %v, want error %t", err, tt.wantErr)
			}
			if sent := len(fake.service.requests); sent != tt.wantSent {
				t.Fatalf("sent %d requests, want %d", sent, tt.wantSent)
			}
			for _, request := range fake.service.requests[1:] {
				if request.GetRequestId() != fake.service.requests[0].GetRequestId() {
					t.Fatalf("request ids %q and %q differ; the cluster could not dedupe them",
						fake.service.requests[0].GetRequestId(), request.GetRequestId())
				}
			}
			if tt.endless != nil {
				var refused *serviceerror.Unavailable
				if !errors.As(err, &refused) {
					t.Fatalf("a spent budget answered %v, want the cluster's last refusal", err)
				}
			}
			requireBackoff(t, fake.clock.spacings(), tt.endless != nil)
		})
	}
}

// requireBackoff holds the spacings between tries to the shape the starter
// promises: each waits, none shrinks, the second is longer than the first,
// and — given enough tries to reach it — the spacing stops growing.
func requireBackoff(t *testing.T, spacings []time.Duration, capped bool) {
	t.Helper()
	for i, spacing := range spacings {
		if spacing <= 0 || (i > 0 && spacing < spacings[i-1]) {
			t.Fatalf("spacings %v: try %d waits %s, want a positive wait no shorter than the one before", spacings, i, spacing)
		}
	}
	if len(spacings) >= 2 && spacings[1] <= spacings[0] {
		t.Fatalf("spacings %v: the second wait does not back off from the first", spacings)
	}
	if capped && (len(spacings) < 3 || spacings[len(spacings)-1] != spacings[len(spacings)-2]) {
		t.Fatalf("spacings %v: want the spacing to stop growing at a cap", spacings)
	}
}

// A caller that gives up while the starter waits to ask again is answered
// that it gave up — not that the cluster was unavailable, which would tell
// it to ask again. The caller may give up during a call or while the starter
// waits to ask again.
func TestCancelRun_ACallerThatGivesUpIsToldSo(t *testing.T) {
	for _, tt := range []struct {
		name string
		// arm makes the caller give up at the point under test.
		arm func(fake *cancelClient, giveUp context.CancelFunc)
	}{
		{"during a call", func(fake *cancelClient, giveUp context.CancelFunc) { fake.service.onCall = giveUp }},
		{"while waiting to ask again", func(fake *cancelClient, giveUp context.CancelFunc) {
			fake.clock.hold, fake.clock.onWait = true, giveUp
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := &cancelClient{service: &cancelService{endless: serviceerror.NewUnavailable("frontend restarting")}}
			starter := fake.starter()
			ctx, giveUp := context.WithCancel(context.Background())
			tt.arm(fake, giveUp)
			err := starter.CancelRun(ctx, "ork-1", engine.CancelRequest{Principal: "dana@acme"})
			var refused *serviceerror.Unavailable
			if !errors.Is(err, context.Canceled) || errors.As(err, &refused) {
				t.Fatalf("CancelRun = %v, want the caller's cancellation and not the cluster's refusal", err)
			}
			if len(fake.service.requests) != 1 {
				t.Fatalf("sent %d requests after the caller gave up, want 1", len(fake.service.requests))
			}
		})
	}
}

// A call that hangs past the budget is answered with the refusal that made
// the starter ask again, and a caller that gives up during a call is told it
// gave up — the cluster's own spelling of either unwraps to neither.
func TestCancelRun_AnswersForACallThatOutlivesItsContext(t *testing.T) {
	t.Run("the budget ends under a call", func(t *testing.T) {
		starter, fake := newCancelStarter(serviceerror.NewUnavailable("frontend restarting"))
		fake.service.hang = true
		// The first call is refused; the budget ends while the second hangs.
		fake.service.onCall = func() {
			if len(fake.service.requests) == 2 {
				fake.clock.spendBudget()
			}
		}
		err := starter.CancelRun(context.Background(), "ork-1", engine.CancelRequest{Principal: "dana@acme"})
		var refused *serviceerror.Unavailable
		if !errors.As(err, &refused) {
			t.Fatalf("CancelRun = %v, want the cluster's last refusal", err)
		}
	})
	t.Run("the caller gives up during a call", func(t *testing.T) {
		fake := &cancelClient{service: &cancelService{hang: true}}
		starter := fake.starter()
		ctx, cancel := context.WithCancel(context.Background())
		fake.service.onCall = cancel
		err := starter.CancelRun(ctx, "ork-1", engine.CancelRequest{Principal: "dana@acme"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CancelRun = %v, want the caller's cancellation", err)
		}
	})
}
