package engine

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

func TestLivePollerRecords_KeepsIdentitiesAndTheirNewestLastSeen(t *testing.T) {
	now := time.Now()
	older, newer := now.Add(-20*time.Second), now.Add(-2*time.Second)
	description := sdkDescription(map[string]map[client.TaskQueueType][]client.TaskQueuePollerInfo{
		"": {client.TaskQueueTypeActivity: {
			poller("orker-a", older),
			poller("orker-stale", now.Add(-5*time.Minute)),
			poller("", newer),
		}},
		"v2": {client.TaskQueueTypeActivity: {
			poller("orker-a", newer),
		}},
	})
	records := livePollerRecords(description, now, DefaultPollerFreshness)
	if len(records) != 1 || records[0].Identity != "orker-a" || !records[0].LastSeen.Equal(newer) {
		t.Fatalf("livePollerRecords = %+v; want one orker-a at its newer timestamp", records)
	}
	// livePollers over the same input answers what it always has: one named
	// Orker plus the anonymous record counted on its own.
	if live := livePollers(description, now, DefaultPollerFreshness); live.Orkers != 2 || !live.LastSeen.Equal(newer) {
		t.Fatalf("livePollers = %+v; want 2 Orkers last seen %v", live, newer)
	}
}

// fleetClient records what TemporalStarter dispatches, and answers one
// DescribeOrkerResult.
type fleetClient struct {
	client.Client
	options  []client.StartActivityOptions
	activity []any
	args     [][]any
	queues   []string
}

func (c *fleetClient) ExecuteActivity(_ context.Context, options client.StartActivityOptions, activity any, args ...any) (client.ActivityHandle, error) {
	c.options = append(c.options, options)
	c.activity = append(c.activity, activity)
	c.args = append(c.args, args)
	return fleetHandle{}, nil
}

func (c *fleetClient) DescribeTaskQueueEnhanced(_ context.Context, options client.DescribeTaskQueueEnhancedOptions) (client.TaskQueueDescription, error) {
	c.queues = append(c.queues, options.TaskQueue)
	return activityPollers(poller("orker-7f3a", time.Now())), nil
}

type fleetHandle struct{ client.ActivityHandle }

func (fleetHandle) Get(_ context.Context, valuePtr any) error {
	proto.Merge(valuePtr.(*orkstackv1.DescribeOrkerResult), &orkstackv1.DescribeOrkerResult{OrkerId: "orker-7f3a"})
	return nil
}

func TestQueueForOrker_IsTheOnlyChannelTemporalStarterUses(t *testing.T) {
	c := &fleetClient{}
	s := NewTemporalStarter(c, "")
	var fleet OrkerFleet = s
	control, err := task.QueueForOrker("orker-7f3a")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := fleet.DescribeOrkerByID(ctx, "orker-7f3a"); err != nil {
		t.Fatal(err)
	}
	if _, err := fleet.CordonOrker(ctx, "orker-7f3a", &orkstackv1.CordonOrkerInput{Cordoned: true, Reason: "cve"}); err != nil {
		t.Fatal(err)
	}
	if len(c.options) != 2 {
		t.Fatalf("dispatched %d activities; want 2", len(c.options))
	}
	for i, options := range c.options {
		if options.TaskQueue != control {
			t.Errorf("activity %d went to %q; want the control channel %q", i, options.TaskQueue, control)
		}
	}
	if c.activity[0] != task.ActivityDescribeOrker || c.activity[1] != task.ActivityCordonOrker {
		t.Fatalf("activities = %v; want DescribeOrker then CordonOrker", c.activity)
	}
	if got := c.options[1].RetryPolicy; got == nil || got.MaximumAttempts != 2 {
		t.Fatalf("CordonOrker retry policy = %+v; want MaximumAttempts 2", got)
	}
	if !strings.HasPrefix(c.options[1].ID, "cordon-orker") || c.options[1].ScheduleToCloseTimeout != orkerControlTimeout {
		t.Fatalf("CordonOrker options = %+v", c.options[1])
	}
	if in, ok := c.args[1][0].(*orkstackv1.CordonOrkerInput); !ok || !in.GetCordoned() || in.GetReason() != "cve" {
		t.Fatalf("CordonOrker argument = %v", c.args[1])
	}

	presence, ok, err := fleet.ObserveOrker(ctx, "orker-7f3a")
	if err != nil || !ok || presence.Identity != "orker-7f3a" {
		t.Fatalf("ObserveOrker = %+v, %v, %v", presence, ok, err)
	}
	if _, err := fleet.ObserveOrkers(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.queues) != 2 || c.queues[0] != control || c.queues[1] != task.OrkersQueue {
		t.Fatalf("described %v; want the control channel then the presence channel", c.queues)
	}
	if _, err := fleet.DescribeOrkerByID(ctx, "a b"); err == nil {
		t.Fatal("DescribeOrkerByID built a channel from an invalid id")
	}
}

// TestOrkersQueue_CarriesNoDispatch is the guardrail on the presence channel:
// nothing in OrkStack ever dispatches to it, so it can never become a custom
// task queue (AGENTS.md §13). It greps the tree for the constant inside an
// ExecuteActivity call or a TaskQueue: field.
func TestOrkersQueue_CarriesNoDispatch(t *testing.T) {
	root := filepath.Join("..", "..")
	dispatch := regexp.MustCompile(`(ExecuteActivity\([^\n]*OrkersQueue|TaskQueue:\s*task\.OrkersQueue)`)
	checked := 0
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			checked++
			if loc := dispatch.FindIndex(raw); loc != nil {
				t.Errorf("%s dispatches to the presence channel: %q", path, raw[loc[0]:loc[1]])
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if checked == 0 {
		t.Fatal("grepped nothing")
	}
}
