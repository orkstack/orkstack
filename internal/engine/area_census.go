package engine

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// ScheduleCensusEntry is one schedule as a census pass saw it.
type ScheduleCensusEntry struct {
	Namespace, Name string
	// Paused and NextFire are the cluster's run index's, as it listed the
	// schedule; NextFire is zero when it has none.
	Paused   bool
	NextFire time.Time
	// Described says the pass described this schedule, and so read
	// SkippedOverlap and MissedCatchup: the cluster's running totals of fires
	// skipped for overlap and missed past the catch-up window.
	Described                     bool
	SkippedOverlap, MissedCatchup int64
}

// ScheduleCensus is one census pass: every OrkStack schedule the cluster
// listed, sorted by namespace and name.
type ScheduleCensus struct {
	Entries []ScheduleCensusEntry
}

// scheduleCensusState is a scheduler's census bookkeeping: where the next
// pass starts describing, so every schedule is described in turn however
// many there are, and the requests the census made.
type scheduleCensusState struct {
	offset   atomic.Int64
	requests atomic.Int64
}

// Census lists every OrkStack schedule, in every namespace, and describes
// at most maxDescribes of them, starting where the last pass stopped. The
// list says whether each is paused and when it fires next; a describe adds
// its skipped-fire totals. It reads the cluster's schedule state only: no
// fire's details, no record. A describe that fails leaves its schedule
// undescribed, which lowers the pass's coverage.
func (s *TemporalScheduler) Census(ctx context.Context, maxDescribes int) (ScheduleCensus, error) {
	var out ScheduleCensus
	s.census.requests.Add(1)
	iter, err := s.client.ScheduleClient().List(ctx, client.ScheduleListOptions{PageSize: scheduleListPageSize})
	if err != nil {
		return ScheduleCensus{}, fmt.Errorf("census: list schedules: %w", err)
	}
	for iter.HasNext() {
		entry, err := iter.Next()
		if err != nil {
			return ScheduleCensus{}, fmt.Errorf("census: list schedules: %w", err)
		}
		fields := entry.Memo.GetFields()
		if id, _ := s.memoString(fields, MemoScheduleID); id == "" {
			continue // not one of ours
		}
		ns, _ := s.memoString(fields, MemoNamespace)
		name, _ := s.memoString(fields, MemoScheduleName)
		if ns == "" || name == "" {
			continue
		}
		e := ScheduleCensusEntry{Namespace: ns, Name: name, Paused: entry.Paused}
		if len(entry.NextActionTimes) > 0 {
			e.NextFire = clusterTime(entry.NextActionTimes[0])
		}
		out.Entries = append(out.Entries, e)
	}
	sort.Slice(out.Entries, func(i, j int) bool {
		a, b := out.Entries[i], out.Entries[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	total := len(out.Entries)
	// A page past the first is a request of its own.
	s.census.requests.Add(int64(total / scheduleListPageSize))
	n := min(max(maxDescribes, 0), total)
	if n == 0 {
		return out, nil
	}
	start := int(s.census.offset.Load() % int64(total))
	s.census.offset.Store(int64((start + n) % total))
	for i := range n {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		e := &out.Entries[(start+i)%total]
		s.census.requests.Add(1)
		desc, err := s.client.ScheduleClient().GetHandle(ctx, TemporalScheduleID(e.Namespace, e.Name)).Describe(ctx)
		if err != nil || desc == nil {
			continue
		}
		e.Described = true
		e.SkippedOverlap = int64(desc.Info.NumActionsSkippedOverlap)
		e.MissedCatchup = int64(desc.Info.NumActionsMissedCatchupWindow)
	}
	return out, nil
}

// CensusRequests is the running total of requests Census made.
func (s *TemporalScheduler) CensusRequests() int64 { return s.census.requests.Load() }

// PendingWebhookDeliveries counts the webhook deliveries still running on c
// that started more than olderThan before now: a delivery workflow runs
// until its record is written, so one still running is one not yet settled.
func PendingWebhookDeliveries(c client.Client, now func() time.Time) func(ctx context.Context, olderThan time.Duration) (int64, error) {
	return func(ctx context.Context, olderThan time.Duration) (int64, error) {
		query := fmt.Sprintf("WorkflowType = %q AND ExecutionStatus = %q AND StartTime < %q",
			WebhookDeliveryWorkflowType, "Running", statsTime(now().Add(-olderThan)))
		response, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
		if err != nil {
			return 0, fmt.Errorf("count pending webhook deliveries: %w", err)
		}
		return response.GetCount(), nil
	}
}
