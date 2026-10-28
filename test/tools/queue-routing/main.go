// Command queue-routing reports which Worker Deployment Version a TASK QUEUE
// routes to, and can wait until that becomes a wanted value.
//
//	go run ./test/tools/queue-routing -namespace NS -queue ork-tasks:workflow
//	go run ./test/tools/queue-routing -namespace NS \
//	    -queue ork-tasks:workflow -queue ork-tasks-pool:activity \
//	    -want orkstack-runbook.v2 -timeout 60s
//
// WHY THIS EXISTS. `temporal worker deployment list` and `describe-version`
// report the deployment's ROUTING CONFIG: what an operator asked for, recorded
// by the deployment's own workflow. That is not what dispatches a task.
// Matching routes by the task queue's user data, which the server syncs to the
// queues after the routing config changes. The two can disagree, and a script
// that changes routing and then immediately asserts on behaviour is asserting
// on a precondition it never observed.
//
// An earlier release runbook did exactly that and failed roughly one
// execution in three: `set-current-version --unversioned` returned, the
// deployment reported no Current Version, and a run submitted moments later was
// still dispatched to the previous Current Version's workers, because that
// version was still what the queue itself named.
//
// DescribeTaskQueue's versioning_info is the queue's own answer — the state
// matching reads — so waiting for it is waiting for the thing that decides
// where a task goes.
//
// Two properties of that API cost an afternoon to find, and both are measured
// against server 1.31.2 rather than assumed:
//
//  1. versioning_info comes back only in the DEFAULT api mode. The SDK's
//     client.DescribeTaskQueueEnhanced sends DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
//     whose reply leaves the field nil — which reads as "unversioned" for a
//     queue that is demonstrably routing to a version.
//  2. It is per PHYSICAL queue, so the type matters: an engine queue carries
//     the version on its workflow queue, an Orker queue on its activity queue.
//     Asking ork-tasks-pool about its workflow queue answers
//     current_version:"__unversioned__" while its activity queue answers v2.
//
// Exit status: 0 when every queue reports the wanted version (or, with no
// -want, when every queue was read), 1 on timeout or error.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// unversioned is what a queue with no current deployment version routes to:
// "A nil Current Version represents all the unversioned workers".
const unversioned = "unversioned"

// physicalQueue is one (task queue, task type) pair — the granularity the
// server records a current version at.
type physicalQueue struct {
	name  string
	kind  enumspb.TaskQueueType
	label string
}

func (q physicalQueue) String() string { return q.label }

type queueList []physicalQueue

func (q *queueList) String() string {
	names := make([]string, 0, len(*q))
	for _, queue := range *q {
		names = append(names, queue.label)
	}
	return strings.Join(names, ",")
}

// Set parses "name" (workflow, the default) or "name:workflow" / "name:activity".
func (q *queueList) Set(value string) error {
	name, kind, found := strings.Cut(value, ":")
	if name == "" {
		return errors.New("empty task queue name")
	}
	queue := physicalQueue{name: name, kind: enumspb.TASK_QUEUE_TYPE_WORKFLOW, label: name + ":workflow"}
	if found {
		switch kind {
		case "workflow":
		case "activity":
			queue.kind = enumspb.TASK_QUEUE_TYPE_ACTIVITY
			queue.label = name + ":activity"
		default:
			return fmt.Errorf("task queue type %q: want workflow or activity", kind)
		}
	}
	*q = append(*q, queue)
	return nil
}

func main() {
	var (
		address   = flag.String("address", "127.0.0.1:7233", "Temporal frontend address")
		namespace = flag.String("namespace", "", "Temporal namespace (required)")
		want      = flag.String("want", "",
			`wait until every queue routes to this version ("deployment.build" or "unversioned")`)
		timeout    = flag.Duration("timeout", 60*time.Second, "how long to wait for -want")
		interval   = flag.Duration("interval", 500*time.Millisecond, "poll interval while waiting")
		explaining = flag.Bool("explain", false,
			"print how each API answers for these queues, and exit")
	)
	var queues queueList
	flag.Var(&queues, "queue", "task queue as name[:workflow|:activity] (repeatable)")
	flag.Parse()

	if *namespace == "" || len(queues) == 0 {
		fmt.Fprintln(os.Stderr,
			"usage: queue-routing -namespace NS -queue Q[:workflow|:activity] … [-want VERSION] [-explain]")
		os.Exit(1)
	}
	if err := run(*address, *namespace, queues, *want, *timeout, *interval, *explaining); err != nil {
		fmt.Fprintf(os.Stderr, "queue-routing: %v\n", err)
		os.Exit(1)
	}
}

func run(
	address, namespace string,
	queues queueList,
	want string,
	timeout, interval time.Duration,
	explaining bool,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout+30*time.Second)
	defer cancel()

	c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return fmt.Errorf("dial %s: %w", address, err)
	}
	defer c.Close()

	if explaining {
		return explain(ctx, c, namespace, queues)
	}

	if want == "" {
		for _, queue := range queues {
			routing, err := routesTo(ctx, c, namespace, queue)
			if err != nil {
				return err
			}
			fmt.Printf("queue=%s routes_to=%s\n", queue, routing)
		}
		return nil
	}

	deadline := time.Now().Add(timeout)
	started := time.Now()
	last := map[string]string{}
	for {
		agreed := true
		for _, queue := range queues {
			routing, err := routesTo(ctx, c, namespace, queue)
			if err != nil {
				return err
			}
			if last[queue.label] != routing {
				// Every transition is printed, so the transcript shows how long
				// the queue disagreed with the deployment's routing config.
				fmt.Printf("%6.1fs queue=%s routes_to=%s\n", time.Since(started).Seconds(), queue, routing)
				last[queue.label] = routing
			}
			if routing != want {
				agreed = false
			}
		}
		if agreed {
			fmt.Printf("%6.1fs every queue routes to %s\n", time.Since(started).Seconds(), want)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("after %s, queues route to %v, want %q", timeout, last, want)
		}
		time.Sleep(interval)
	}
}

// routesTo reads the queue's own view: the current deployment version this
// physical queue's tasks are dispatched to. See the package comment for why it
// is the default api mode (the request leaves the deprecated api mode unset)
// and why the task queue type is part of the question.
func routesTo(ctx context.Context, c client.Client, namespace string, queue physicalQueue) (string, error) {
	described, err := c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     namespace,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: queue.name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: queue.kind,
	})
	if err != nil {
		return "", fmt.Errorf("describe task queue %s: %w", queue, err)
	}
	current := described.GetVersioningInfo().GetCurrentDeploymentVersion()
	if current == nil {
		return unversioned, nil
	}
	return current.GetDeploymentName() + "." + current.GetBuildId(), nil
}

// explain prints, for one moment and one queue, what each of the three
// available reads answers. It is the evidence for this tool's two choices, and
// the runbook transcript carries its output: the claims in docs/DEPLOY.md §3
// step 4 should be checkable without taking anyone's word for them.
func explain(ctx context.Context, c client.Client, namespace string, queues queueList) error {
	for _, queue := range queues {
		enhanced, err := c.DescribeTaskQueueEnhanced(ctx, client.DescribeTaskQueueEnhancedOptions{
			TaskQueue: queue.name,
			TaskQueueTypes: []client.TaskQueueType{
				client.TaskQueueTypeWorkflow,
				client.TaskQueueTypeActivity,
			},
		})
		enhancedAnswer := "(the reply carries no versioning_info at all)"
		switch {
		case err != nil:
			enhancedAnswer = "error: " + err.Error()
		case enhanced.VersioningInfo != nil && enhanced.VersioningInfo.CurrentVersion != nil:
			enhancedAnswer = enhanced.VersioningInfo.CurrentVersion.DeploymentName +
				"." + enhanced.VersioningInfo.CurrentVersion.BuildID
		}

		workflowAnswer, err := routesTo(ctx, c, namespace,
			physicalQueue{name: queue.name, kind: enumspb.TASK_QUEUE_TYPE_WORKFLOW, label: queue.name})
		if err != nil {
			return err
		}
		activityAnswer, err := routesTo(ctx, c, namespace,
			physicalQueue{name: queue.name, kind: enumspb.TASK_QUEUE_TYPE_ACTIVITY, label: queue.name})
		if err != nil {
			return err
		}

		fmt.Printf("queue %s\n", queue.name)
		fmt.Printf("  SDK DescribeTaskQueueEnhanced (ENHANCED mode)  %s\n", enhancedAnswer)
		fmt.Printf("  DescribeTaskQueue default mode, workflow type  %s\n", workflowAnswer)
		fmt.Printf("  DescribeTaskQueue default mode, activity type  %s\n", activityAnswer)
	}
	return nil
}
