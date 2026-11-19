package orker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// An attempt's result reaches the cluster only after the attempt returns: the
// SDK's activity worker sends RespondActivityTask{Completed,Failed,Canceled}
// once the activity function (and the cordon interceptor around it) has
// returned. So running_tasks reaching zero does not mean the cluster has
// every result. A process that exits in that gap loses the result: the
// cluster times the attempt out and runs the task again, its commit adopted
// but an attempt spent (P13b proof 44342096120).
//
// owedReports closes that gap. The cordon interceptor marks an attempt's
// result owed as it returns, by its task token; the report interceptor on the
// Orker's client settles it when the cluster answers its report, and a
// stopping Orker waits until nothing is owed. An entry also lapses
// ReportDrainBound after it was owed, the SDK's own bound on sending it, so a
// report the SDK never sends (it gave up, or it had none to send) never holds
// a stop.
//
// Weighed and not used: the SDK's worker.Options.WorkerStopTimeout. Its Stop
// waits for every in-flight activity, not only for reports, under one bound,
// and ends what still runs only by cancelling it once that bound is over. An
// open pin's creation activity and a running pinned member end only by that
// cancel, and the SDK stops the session's creation and activity workers one
// after the other, so any bound long enough for a report would hold every
// stop with a pin open twice over before its pin failed PIN_HOST_LOST. And
// the stop calls Stop once, at the grace's end, before handed-back and
// committing attempts have finished: an attempt that outlived the bound would
// still report into an exiting process. owedReports waits for reports alone,
// at the point every attempt has returned.

// ReportDrainBound is how long an attempt's result stays owed at most, and so
// the longest a stopping Orker waits for the cluster to take one. The SDK
// bounds each report by its RPC timeout (10 s by default, its retries
// included), so a report not answered by then is one it no longer sends.
const ReportDrainBound = 15 * time.Second

// owedReport is one attempt whose result the SDK still owes the cluster.
type owedReport struct {
	// label names the attempt (run, task, attempt) in the stop's log.
	label string
	// lapses is when the SDK no longer sends the report.
	lapses time.Time
}

// owedReports is the set of task tokens whose result the SDK still owes the
// cluster. The zero value is empty and ready.
type owedReports struct {
	mu   sync.Mutex
	owed map[string]owedReport
	// zero is open while something is owed and closed when that ends; nil
	// while nothing is owed.
	zero chan struct{}
	// lapsed names the attempts whose entry lapsed unanswered, the most
	// recent maxLapsedLabels of them: results the cluster may never have had.
	lapsed []string
	// lapsedTotal counts every entry that lapsed unanswered.
	lapsedTotal int
	// now is the clock; nil is time.Now.
	now func() time.Time
}

// maxLapsedLabels bounds owedReports.lapsed.
const maxLapsedLabels = 32

func (r *owedReports) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// owe marks one attempt's result owed.
func (r *owedReports) owe(token []byte, label string) {
	if len(token) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	r.pruneLocked(now)
	if r.owed == nil {
		r.owed = map[string]owedReport{}
	}
	if len(r.owed) == 0 {
		r.zero = make(chan struct{})
	}
	r.owed[string(token)] = owedReport{label: label, lapses: now.Add(ReportDrainBound)}
}

// settle marks one attempt's result as answered by the cluster.
func (r *owedReports) settle(token []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.owed[string(token)]; ok {
		r.removeLocked(string(token))
	}
}

func (r *owedReports) removeLocked(token string) {
	delete(r.owed, token)
	if len(r.owed) == 0 && r.zero != nil {
		close(r.zero)
		r.zero = nil
	}
}

// pruneLocked drops every entry that has lapsed by now.
func (r *owedReports) pruneLocked(now time.Time) {
	for token, entry := range r.owed {
		if !now.Before(entry.lapses) {
			r.removeLocked(token)
			r.lapsedTotal++
			r.lapsed = append(r.lapsed, entry.label)
			if len(r.lapsed) > maxLapsedLabels {
				r.lapsed = r.lapsed[len(r.lapsed)-maxLapsedLabels:]
			}
		}
	}
}

// pending is what is still owed, by label, and when the last of it lapses.
func (r *owedReports) pending() ([]string, time.Time, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(r.clock())
	var last time.Time
	labels := make([]string, 0, len(r.owed))
	for _, entry := range r.owed {
		labels = append(labels, entry.label)
		if entry.lapses.After(last) {
			last = entry.lapses
		}
	}
	sort.Strings(labels)
	return labels, last, r.zero
}

// await returns once nothing is owed, or ctx's error when it is done first.
// Nothing owed returns nil even on a done context; an entry that lapses
// counts as no longer owed.
func (r *owedReports) await(ctx context.Context) error {
	for {
		labels, last, zero := r.pending()
		if len(labels) == 0 {
			return nil
		}
		timer := time.NewTimer(time.Until(last))
		select {
		case <-zero:
			timer.Stop()
			return nil
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// oweReport marks an ExecuteTask attempt's (or a pin creation's) result owed as the attempt
// returns err, unless the SDK sends no report for it: an attempt that
// reported itself (ErrResultPending, a hand-back's CompleteActivity), and one
// whose deadline passed, which the cluster has already timed out.
func (a *Activities) oweReport(ctx context.Context, info activity.Info, err error) {
	if errors.Is(err, activity.ErrResultPending) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return
	}
	a.reports.owe(info.TaskToken, fmt.Sprintf("%s/%s#%d", info.WorkflowExecution.ID, info.ActivityID, info.Attempt))
}

// AwaitReports returns once the cluster has answered, or the SDK has stopped
// sending, the report of every ExecuteTask attempt that has returned, or
// ctx's error when it is done first. A stopping Orker calls it before it
// exits; it takes at most ReportDrainBound.
func (a *Activities) AwaitReports(ctx context.Context) error { return a.reports.await(ctx) }

// OwedReports names the ExecuteTask attempts whose result the SDK still owes
// the cluster.
func (a *Activities) OwedReports() []string {
	labels, _, _ := a.reports.pending()
	return labels
}

// LapsedReports counts the ExecuteTask attempts whose report the cluster
// never answered within ReportDrainBound (the SDK gave up sending it, or had
// none to send), and names the most recent of them.
func (a *Activities) LapsedReports() (int, []string) {
	a.reports.mu.Lock()
	defer a.reports.mu.Unlock()
	a.reports.pruneLocked(a.reports.clock())
	return a.reports.lapsedTotal, append([]string(nil), a.reports.lapsed...)
}

// ReportInterceptor is the gRPC interceptor the Orker's Temporal client
// carries so a stopping Orker knows when its results have reached the
// cluster. It settles an owed result when the cluster answers its report:
// taken, or refused because it has nothing to take or the request is wrong.
// Any other refusal settles nothing, since the SDK may send it again; the
// entry then lapses. Every call passes through unchanged.
func (a *Activities) ReportInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if token, ok := reportToken(method, req); ok && answered(err) {
			a.reports.settle(token)
		}
		return err
	}
}

// reportToken is the task token of an activity result's report, and whether
// the call is one.
func reportToken(method string, req any) ([]byte, bool) {
	switch method {
	case workflowservice.WorkflowService_RespondActivityTaskCompleted_FullMethodName,
		workflowservice.WorkflowService_RespondActivityTaskFailed_FullMethodName,
		workflowservice.WorkflowService_RespondActivityTaskCanceled_FullMethodName:
	default:
		return nil, false
	}
	carrier, ok := req.(interface{ GetTaskToken() []byte })
	if !ok {
		return nil, false
	}
	return carrier.GetTaskToken(), true
}

// answered reports whether a report's outcome is final: the cluster took it,
// had nothing to take (NotFound: the attempt already ended), or refused the
// request as malformed. Anything else may be retried, so it waits.
func answered(err error) bool {
	if err == nil {
		return true
	}
	switch status.Code(err) {
	case codes.NotFound, codes.InvalidArgument:
		return true
	}
	return false
}
