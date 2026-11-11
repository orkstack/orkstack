package engine_test

import (
	"context"
	"sync"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

// resultOrderHeader carries an activity's schedule ticket from the workflow
// that scheduled it to the activity's own interceptor.
const resultOrderHeader = "ork-test-result-order"

// sessionCreationActivity is the SDK's own activity behind CreateSession. It
// runs for as long as the session is open, so its result is never what the
// next ticket waits for: its creation signal is.
const sessionCreationActivity = "internalSessionCreationActivity"

// resultOrder makes a test environment deliver activity and local-activity
// results to the workflow in the order the workflow scheduled them.
//
// The test environment runs every activity in a goroutine of its own and
// hands its result to the workflow in the order those goroutines finish, so
// two activities in flight at once — a pin's ReleasePin and the main loop's
// PublishEvent — can come back either way round, and the workflow then issues
// its next commands in either order. Nothing in the workflow is wrong; the
// environment is not deterministic. resultOrder is: each activity is given a
// ticket when the workflow schedules it, on the workflow's own goroutine, and
// it starts only once every lower ticket's result has reached the workflow:
// its future has settled, which a coroutine of the interceptor's own waits
// for. The session-creation activity counts as delivered once the workflow
// has handled its creation signal, which is when CreateSession returns.
//
// It changes no workflow code and no activity's behaviour, only when each
// one runs; the waiting coroutines issue no command and touch no workflow
// state. A ticket is closed by its future whatever happened to its activity —
// a local activity canceled before it ran never reaches the activity
// interceptor at all. An activity that only ends once the workflow acts on a
// later ticket would never let that ticket start; the session-creation
// activity is the one such activity, and its signal releases it.
type resultOrder struct {
	interceptor.WorkerInterceptorBase
	mu        sync.Mutex
	turn      *sync.Cond
	issued    int64
	open      map[int64]bool   // tickets issued whose result has not reached the workflow
	creations map[string]int64 // session id -> its creation activity's ticket
}

func newResultOrder() *resultOrder {
	o := &resultOrder{open: map[int64]bool{}, creations: map[string]int64{}}
	o.turn = sync.NewCond(&o.mu)
	return o
}

func (o *resultOrder) issue() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.issued++
	o.open[o.issued] = true
	return o.issued
}

func (o *resultOrder) delivered(ticket int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.open, ticket)
	o.turn.Broadcast()
}

func (o *resultOrder) releaseCreation(sessionID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ticket, ok := o.creations[sessionID]; ok {
		delete(o.creations, sessionID)
		delete(o.open, ticket)
		o.turn.Broadcast()
	}
}

// await blocks until every lower ticket has been delivered. A retried local
// activity comes here again under its first attempt's ticket.
func (o *resultOrder) await(ticket int64, activityType string, args []any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if activityType == sessionCreationActivity && len(args) > 0 {
		if sessionID, ok := args[0].(string); ok {
			o.creations[sessionID] = ticket
		}
	}
	for o.lowerOpen(ticket) {
		o.turn.Wait()
	}
}

func (o *resultOrder) lowerOpen(ticket int64) bool {
	for t := range o.open {
		if t < ticket {
			return true
		}
	}
	return false
}

func (o *resultOrder) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &resultOrderInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, o: o}
}

func (o *resultOrder) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &resultOrderActivity{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, o: o}
}

type resultOrderInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	o *resultOrder
}

func (i *resultOrderInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&resultOrderOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, o: i.o})
}

// HandleSignal releases a session's creation ticket once the workflow has
// handled the signal CreateSession waits for, which is named by the session.
func (i *resultOrderInbound) HandleSignal(ctx workflow.Context, in *interceptor.HandleSignalInput) error {
	err := i.Next.HandleSignal(ctx, in)
	i.o.releaseCreation(in.SignalName)
	return err
}

type resultOrderOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	o *resultOrder
}

// stamp issues the next ticket and puts it on the call's header.
func (o *resultOrderOutbound) stamp(ctx workflow.Context) int64 {
	ticket := o.o.issue()
	payload, err := converter.GetDefaultDataConverter().ToPayload(ticket)
	if err != nil {
		panic(err) // an int64 always converts
	}
	interceptor.WorkflowHeader(ctx)[resultOrderHeader] = payload
	return ticket
}

// settle closes the ticket once the workflow has the future's result.
func (o *resultOrderOutbound) settle(ctx workflow.Context, ticket int64, future workflow.Future) workflow.Future {
	workflow.Go(ctx, func(c workflow.Context) {
		_ = future.Get(c, nil) // the outcome is the workflow's business; only its arrival counts
		o.o.delivered(ticket)
	})
	return future
}

func (o *resultOrderOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	ticket := o.stamp(ctx)
	return o.settle(ctx, ticket, o.Next.ExecuteActivity(ctx, activityType, args...))
}

func (o *resultOrderOutbound) ExecuteLocalActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	ticket := o.stamp(ctx)
	return o.settle(ctx, ticket, o.Next.ExecuteLocalActivity(ctx, activityType, args...))
}

type resultOrderActivity struct {
	interceptor.ActivityInboundInterceptorBase
	o *resultOrder
}

func (a *resultOrderActivity) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	if payload, ok := interceptor.Header(ctx)[resultOrderHeader]; ok {
		var ticket int64
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &ticket); err != nil {
			panic(err)
		}
		a.o.await(ticket, activity.GetInfo(ctx).ActivityType.Name, in.Args)
	}
	return a.Next.ExecuteActivity(ctx, in)
}
