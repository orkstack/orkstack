//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// Fair dispatch across namespaces (docs/DEPLOY.md "Fair dispatch across
// namespaces"), measured as the study measured it: one namespace queues a
// backlog of 200 tasks, a second then queues 20, and only then does one Orker
// with one slot start taking them, so the order it takes them in is the order
// the cluster dispatched them in. On a cluster with fairness enabled the late
// namespace is interleaved into the backlog; on a stock cluster it waits
// behind it. Both clusters are private dev servers of the image the dev stack
// pins, so the control runs exactly the same code with only the setting off,
// and the test cannot pass by chance.

const (
	fairBacklog = 200
	fairLate    = 20
)

func fairDocument(name string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\ntasks:\n", name)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - id: t%03d\n    uses: shell\n    with: {command: \"true\"}\n", i)
	}
	return b.String()
}

// startPrivateTemporal runs one dev server of the pinned image on a free port,
// with the dynamic settings given, and returns its address once it answers.
func startPrivateTemporal(t *testing.T, ctx context.Context, settings ...string) string {
	t.Helper()
	image, err := composeTemporalImage()
	if err != nil {
		infraAbsent(t, "no Temporal image to run: %v", err)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "no docker on this host")
	}
	if err := exec.CommandContext(ctx, "docker", "image", "inspect", image).Run(); err != nil {
		infraAbsent(t, "the image %s is not present", image)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	command := "exec timeout 600 temporal server start-dev --ip 0.0.0.0 --headless --log-level error"
	for _, setting := range settings {
		command += " --dynamic-config-value " + setting
	}
	// timeout(1) bounds the server's life even if this process dies before
	// its cleanup runs.
	out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", "--pull", "never",
		"--label", fmt.Sprintf("ork.test=fairness-%d", os.Getpid()),
		"-p", fmt.Sprintf("127.0.0.1:%d:7233", port), "--entrypoint", "sh", image, "-c", command).Output()
	if err != nil {
		t.Fatalf("start a private dev server: %v", err)
	}
	container := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })
	address := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(90 * time.Second)
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: client.DefaultNamespace})
		if err == nil {
			_, err = c.WorkflowService().DescribeNamespace(ctx,
				&workflowservice.DescribeNamespaceRequest{Namespace: client.DefaultNamespace})
			c.Close()
			if err == nil {
				return address
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the private dev server never answered: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// fairShape is the run one namespace queues n dispatches with: its
// definition and input, and whether each dispatch is a pin request.
type fairShape struct {
	run func(t *testing.T, name string, n int) (*orkstackv1.Orkflow, *orkstackv1.Payload)
	// pins: the run queues n pin requests, so it marks them for keying
	// (FairnessSelection.PinRequests) and the experiment's Orker is a pin
	// host that opens one pin at a time. Each pin holds one task, which runs
	// as soon as its pin opens, so the order the Orker takes the tasks in is
	// the order the cluster handed out the pin requests.
	pins bool
}

// fairTasks queues n plain tasks.
var fairTasks = fairShape{run: func(t *testing.T, name string, n int) (*orkstackv1.Orkflow, *orkstackv1.Payload) {
	return compile(t, fairDocument(name, n)), nil
}}

// fairItems queues one fan-out of n items, every one dispatched at once.
var fairItems = fairShape{run: func(t *testing.T, name string, n int) (*orkstackv1.Orkflow, *orkstackv1.Payload) {
	document := fmt.Sprintf("apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\ninputs:\n  parts: {type: array}\n"+
		"tasks:\n  - id: spread\n    uses: shell\n    each: {over: \"{{ .inputs.parts }}\", max_items: %d}\n"+
		"    with: {command: \"true\"}\n", name, n)
	return compile(t, document), payload(t, map[string]any{"parts": guardItems(n)})
}}

// fairPins queues n pin requests: n pins, each holding one task, every one
// requested at once.
var fairPins = fairShape{pins: true, run: func(t *testing.T, name string, n int) (*orkstackv1.Orkflow, *orkstackv1.Payload) {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\npins:\n", name)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  p%03d: {}\n", i)
	}
	b.WriteString("tasks:\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - id: t%03d\n    uses: shell\n    pin: p%03d\n    with: {command: \"true\"}\n", i, i)
	}
	return compile(t, b.String()), nil
}}

// lateNamespaceMeanPosition runs the backlog-then-late experiment on one
// cluster and returns the late namespace's mean dispatch position (0-based,
// out of fairBacklog+fairLate).
func lateNamespaceMeanPosition(t *testing.T, ctx context.Context, address string) float64 {
	t.Helper()
	return lateNamespaceMeanPositionOf(t, ctx, address, fairTasks)
}

// lateNamespaceMeanPositionOf is lateNamespaceMeanPosition with each
// namespace's dispatches queued in the shape given.
func lateNamespaceMeanPositionOf(t *testing.T, ctx context.Context, address string, shape fairShape) float64 {
	t.Helper()
	c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: client.DefaultNamespace})
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	t.Cleanup(c.Close)
	runQueue, orkerQueue := "ork-fair-runs", "ork-fair-tasks"
	bus, runs := engine.NewEventBus(), engine.NewRunStore()
	snapshots := newTestSnapshotStore()
	// The run worker has cmd/ork's interceptors, which key a pin request.
	runWorker := worker.New(c, runQueue, worker.Options{DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true,
		Interceptors: engine.RunWorkerInterceptors()})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	eventWorker := worker.New(c, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	eventWorker.RegisterActivityWithOptions(engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for _, w := range []worker.Worker{runWorker, eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
	}
	starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, orkerQueue)
	queued := task.ActivityExecuteTask
	if shape.pins {
		queued = sessionCreationActivityName
	}
	start := func(name string, n int) string {
		wf, input := shape.run(t, name, n)
		wf.Namespace = name
		plan := rawPlan(orkerQueue)
		plan.Namespace = name
		plan.CommitRoot = "mem://commits"
		plan.Fairness = &engine.FairnessSelection{Key: engine.FairnessKeyFor(name), PinRequests: shape.pins}
		if shape.pins {
			// The last request waits for every pin before it to open and
			// close, one at a time.
			plan.PinCreationTimeoutSeconds = 600
		}
		runID := fmt.Sprintf("ork-fair-%s-%d", name, time.Now().UnixNano())
		if err := starter.StartRun(ctx, runID, wf, input, plan); err != nil {
			t.Fatalf("start %s: %v", runID, err)
		}
		// Every dispatch of the run is scheduled before the next run starts.
		deadline := time.Now().Add(2 * time.Minute)
		for scheduledOf(t, ctx, c, runID, queued) < n {
			if time.Now().After(deadline) {
				t.Fatalf("run %s scheduled %d of its %d %s", runID, scheduledOf(t, ctx, c, runID, queued), n, queued)
			}
			time.Sleep(200 * time.Millisecond)
		}
		return runID
	}
	backlog := start("tenant-a", fairBacklog)
	late := start("tenant-b", fairLate)

	var mu sync.Mutex
	var order []string
	orkerOptions := worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
		MaxConcurrentActivityExecutionSize: 1, MaxConcurrentActivityTaskPollers: 1,
	}
	if shape.pins {
		// A pin host of one pin; its pins' tasks and closes run on the
		// pin's own channel, beside the open pin.
		orkerOptions = worker.Options{
			DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
			EnableSessionWorker: true, MaxConcurrentSessionExecutionSize: 1,
		}
	}
	orker := worker.New(c, orkerQueue, orkerOptions)
	orker.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ReleasePinInput) error { return nil },
		activity.RegisterOptions{Name: task.ActivityReleasePin})
	orker.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		order = append(order, in.GetRunId())
		mu.Unlock()
		return succeeded(in.GetTaskId(), map[string]any{"ok": true}), nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if err := orker.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orker.Stop)
	for _, runID := range []string{backlog, late} {
		if err := c.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
			t.Fatalf("run %s: %v", runID, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != fairBacklog+fairLate {
		t.Fatalf("the Orker took %d tasks, want %d", len(order), fairBacklog+fairLate)
	}
	sum, seen := 0, 0
	for position, runID := range order {
		if runID == late {
			sum += position
			seen++
		}
	}
	if seen != fairLate {
		t.Fatalf("the Orker took %d of the late namespace's tasks, want %d", seen, fairLate)
	}
	return float64(sum) / float64(seen)
}

// scheduledOf counts the activities of activityType a run has scheduled.
func scheduledOf(t *testing.T, ctx context.Context, c client.Client, runID, activityType string) int {
	t.Helper()
	iter := c.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	n := 0
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("history of %s: %v", runID, err)
		}
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == activityType {
			n++
		}
	}
	return n
}

func TestIntegration_FairDispatchServesALateNamespaceAheadOfABacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	fair := lateNamespaceMeanPosition(t, ctx, startPrivateTemporal(t, ctx, "matching.enableFairness=true"))
	stock := lateNamespaceMeanPosition(t, ctx, startPrivateTemporal(t, ctx))
	result := fmt.Sprintf("late namespace's mean dispatch position of %d: %.1f with fairness, %.1f without "+
		"(FIFO would be %.1f)\n", fairBacklog+fairLate, fair, stock, float64(fairBacklog)+float64(fairLate-1)/2)
	proof.Write(t, "engine", "fair-dispatch.txt", []byte(result))
	t.Logf("EVIDENCE %s", result)
	// The control: without fairness the late namespace waits behind most of
	// the backlog.
	if stock < float64(fairBacklog)*0.75 {
		t.Fatalf("without fairness the late namespace's mean position is %.1f; the control does not show a backlog it waits behind", stock)
	}
	if fair+40 > stock {
		t.Fatalf("with fairness the late namespace's mean position is %.1f against %.1f without: it was not served ahead of the backlog", fair, stock)
	}
}

// TestIntegration_FairDispatchServesALateNamespacesItemsAheadOfABacklog is
// the same measurement for fan-out items: one namespace's fan-out queues 200
// items, a second's then queues 20, and only then does one Orker with one slot
// start taking them. Each item carries its run's fairness key, so on a cluster
// with fairness enabled the late namespace's items are interleaved into the
// backlog; on a stock cluster they wait behind it.
func TestIntegration_FairDispatchServesALateNamespacesItemsAheadOfABacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	fair := lateNamespaceMeanPositionOf(t, ctx, startPrivateTemporal(t, ctx, "matching.enableFairness=true"), fairItems)
	stock := lateNamespaceMeanPositionOf(t, ctx, startPrivateTemporal(t, ctx), fairItems)
	result := fmt.Sprintf("late namespace's items: mean dispatch position of %d: %.1f with fairness, %.1f without "+
		"(FIFO would be %.1f)\n", fairBacklog+fairLate, fair, stock, float64(fairBacklog)+float64(fairLate-1)/2)
	proof.Write(t, "engine", "fair-dispatch-items.txt", []byte(result))
	t.Logf("EVIDENCE %s", result)
	if stock < float64(fairBacklog)*0.75 {
		t.Fatalf("without fairness the late namespace's items' mean position is %.1f; the control does not show a backlog they wait behind", stock)
	}
	if fair+40 > stock {
		t.Fatalf("with fairness the late namespace's items' mean position is %.1f against %.1f without: they were not served ahead of the backlog", fair, stock)
	}
}

// TestIntegration_FairDispatchServesALateNamespacesPinRequestsAheadOfABacklog
// is the same measurement for pin requests: one namespace's run queues 200
// pin requests, a second's then queues 20, and only then does one pin host
// that opens one pin at a time start taking them. Each request carries its
// run's fairness key (the run worker's interceptor sets it, as cmd/ork's
// does), so on a cluster with fairness enabled the late namespace's pins are
// opened in among the backlog; on a stock cluster they wait behind it.
func TestIntegration_FairDispatchServesALateNamespacesPinRequestsAheadOfABacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	fair := lateNamespaceMeanPositionOf(t, ctx, startPrivateTemporal(t, ctx, "matching.enableFairness=true"), fairPins)
	stock := lateNamespaceMeanPositionOf(t, ctx, startPrivateTemporal(t, ctx), fairPins)
	result := fmt.Sprintf("late namespace's pin requests: mean dispatch position of %d: %.1f with fairness, %.1f without "+
		"(FIFO would be %.1f)\n", fairBacklog+fairLate, fair, stock, float64(fairBacklog)+float64(fairLate-1)/2)
	proof.Write(t, "engine", "fair-dispatch-pins.txt", []byte(result))
	t.Logf("EVIDENCE %s", result)
	if stock < float64(fairBacklog)*0.75 {
		t.Fatalf("without fairness the late namespace's pin requests' mean position is %.1f; the control does not show a backlog they wait behind", stock)
	}
	if fair+40 > stock {
		t.Fatalf("with fairness the late namespace's pin requests' mean position is %.1f against %.1f without: they were not served ahead of the backlog", fair, stock)
	}
}
