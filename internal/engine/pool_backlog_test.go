package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

type fakePoolStatsClient struct {
	mu       sync.Mutex
	channels map[string]*workflowservice.DescribeTaskQueueResponse
	fail     error
	reqs     []*workflowservice.DescribeTaskQueueRequest
}

func (f *fakePoolStatsClient) DescribeTaskQueue(_ context.Context, in *workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, in)
	if f.fail != nil {
		return nil, f.fail
	}
	if r, ok := f.channels[in.GetTaskQueue().GetName()]; ok {
		return r, nil
	}
	return &workflowservice.DescribeTaskQueueResponse{Stats: &taskqueuepb.TaskQueueStats{}}, nil
}

// The pools measured are the default pool and every configured one, each
// once under the name its route answers to: `main`, the default pool's own
// name and a pool whose channel is the default channel all collapse to one.
func TestPoolBacklogPools_CollapseToOneNamePerChannel(t *testing.T) {
	for _, tc := range []struct {
		name         string
		defaultQueue string
		configured   []string
		want         []string
	}{
		{"none configured", "", nil, []string{task.DefaultPool}},
		{"main and duplicates", "", []string{"gpu", "main", "gpu", "build"}, []string{task.DefaultPool, "build", "gpu"}},
		{"default channel is a pool's", "ork-pool-gpu", []string{"gpu", "build"}, []string{"gpu", "build"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := newPoolBacklog(&fakePoolStatsClient{}, tc.defaultQueue, tc.configured)
			must.OK(t, err)
			if got := b.Pools(); !slices.Equal(got, tc.want) {
				t.Fatalf("Pools() = %v; want %v", got, tc.want)
			}
		})
	}
	if _, err := newPoolBacklog(&fakePoolStatsClient{}, "", []string{"Not_A_Pool"}); err == nil || !strings.Contains(err.Error(), "Not_A_Pool") {
		t.Fatalf("an invalid pool: err = %v; want a refusal naming it", err)
	}
}

// One read is one default-mode activity description of the pool's channel
// with statistics, and answers its backlog, oldest task's age, rates and the
// Orkers that polled within the freshness window, one per identity.
func TestPoolBacklog_ReadsThePoolsChannelStats(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	fresh, stale := timestamppb.New(now.Add(-10*time.Second)), timestamppb.New(now.Add(-DefaultPollerFreshness-time.Second))
	client := &fakePoolStatsClient{channels: map[string]*workflowservice.DescribeTaskQueueResponse{
		"ork-pool-gpu": {
			Pollers: []*taskqueuepb.PollerInfo{
				{Identity: "1@a@ork-pool-gpu", LastAccessTime: fresh},
				{Identity: "1@a@ork-pool-gpu", LastAccessTime: fresh},
				{Identity: "2@b@ork-pool-gpu", LastAccessTime: fresh},
				{Identity: "3@c@ork-pool-gpu", LastAccessTime: stale},
			},
			Stats: &taskqueuepb.TaskQueueStats{
				ApproximateBacklogCount: 12,
				ApproximateBacklogAge:   durationpb.New(90 * time.Second),
				TasksAddRate:            1.5,
				TasksDispatchRate:       0.5,
			},
		},
	}}
	b, err := newPoolBacklog(client, "", []string{"gpu"})
	must.OK(t, err)
	b.now = func() time.Time { return now }

	got, err := b.Read(context.Background(), "gpu")
	must.OK(t, err)
	want := PoolBacklogReading{Backlog: 12, BacklogAge: 90 * time.Second, AddRate: 1.5, DispatchRate: 0.5, Pollers: 2}
	if got != want {
		t.Fatalf("Read = %+v; want %+v", got, want)
	}
	req := client.reqs[0]
	if req.GetTaskQueue().GetName() != "ork-pool-gpu" || req.GetTaskQueueType() != enumspb.TASK_QUEUE_TYPE_ACTIVITY ||
		!req.GetReportStats() || req.GetTaskQueue().GetKind() != enumspb.TASK_QUEUE_KIND_NORMAL {
		t.Fatalf("request = %v; want the pool's channel, activity, normal kind, with stats", req)
	}
	//lint:ignore SA1019 asserting the deprecated api mode is left unset: unset is the default mode this read relies on
	mode := req.GetApiMode()
	if mode != enumspb.DESCRIBE_TASK_QUEUE_MODE_UNSPECIFIED {
		t.Fatalf("api mode = %v; want the default mode", mode)
	}
}

// A read that cannot measure — the cluster refusing, no statistics in the
// answer, a pool it does not measure — is an error naming the pool, never a
// reading of zero.
func TestPoolBacklog_AnUnmeasuredReadIsAnError(t *testing.T) {
	refused := &fakePoolStatsClient{fail: errors.New("frontend unavailable")}
	b, err := newPoolBacklog(refused, "", nil)
	must.OK(t, err)
	if _, err := b.Read(context.Background(), task.DefaultPool); err == nil || !strings.Contains(err.Error(), "frontend unavailable") {
		t.Fatalf("refused read: err = %v; want the cause", err)
	}

	empty := &fakePoolStatsClient{channels: map[string]*workflowservice.DescribeTaskQueueResponse{task.DefaultQueue: {}}}
	b, err = newPoolBacklog(empty, "", nil)
	must.OK(t, err)
	if _, err := b.Read(context.Background(), task.DefaultPool); err == nil || !strings.Contains(err.Error(), "no statistics") {
		t.Fatalf("no stats: err = %v; want a refusal saying so", err)
	}
	if _, err := b.Read(context.Background(), "other"); err == nil || !strings.Contains(err.Error(), `"other"`) {
		t.Fatalf("unmeasured pool: err = %v; want a refusal naming it", err)
	}
	if len(empty.reqs) != 1 {
		t.Fatalf("requests = %d; want one, none for a pool it does not measure", len(empty.reqs))
	}
}
