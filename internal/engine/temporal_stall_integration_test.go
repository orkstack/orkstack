//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests stop the one connection an Orker has to Temporal, and nothing else:
// the engine's own client, and so the run's workflow, talks to the cluster
// directly, the way a partition or an overloaded frontend looks from one host.
// The cluster is a private dev server, so the only slow answers are the ones the
// proxy makes: nothing here depends on how busy the host's shared one is.

// stallStack is one deployment whose Orker reaches Temporal only through a
// stallProxy.
type stallStack struct {
	t         *testing.T
	ctx       context.Context
	proxy     *faultproxy.Proxy
	orkflows  engine.OrkflowStore
	runs      *engine.RunStore
	runClient orkstackv1connect.RunServiceClient
	commits   artifacts.CommitStore
	attempts  artifacts.AttemptStore
	root      string

	mu   sync.Mutex
	logs []string
}

func newStallStack(t *testing.T, ctx context.Context) *stallStack {
	t.Helper()
	const tick = orker.DefaultHeartbeatInterval
	cfg := s3Config()
	requireObjectStore(t, cfg)
	requireDockerForCommitE2E(t)

	address := startPrivateTemporal(t, ctx)
	namespace := privateNamespaceForCommitE2E(t)
	direct, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	t.Cleanup(direct.Close)
	if err := task.EnsureNamespace(ctx, direct, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, direct, namespace)

	s := &stallStack{t: t, ctx: ctx, proxy: faultproxy.New(t, address),
		orkflows: newDurableOrkflowStore(t)}
	proxied, err := client.DialContext(ctx, client.Options{HostPort: s.proxy.Addr(), Namespace: namespace})
	if err != nil {
		t.Fatalf("dial temporal through the proxy: %v", err)
	}
	t.Cleanup(proxied.Close)

	token := fmt.Sprintf("%d", time.Now().UnixNano())
	orkerQueue := "ork-temporal-stall-" + token
	store, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("open artifact store: %v", err)
	}
	commitStore, err := artifacts.NewCommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("open commit store: %v", err)
	}
	logStore, err := artifacts.NewLogStore(cfg)
	if err != nil {
		t.Fatalf("open log store: %v", err)
	}
	attempts, err := artifacts.NewAttemptStore(cfg)
	if err != nil {
		t.Fatalf("open attempt store: %v", err)
	}
	s.commits, s.attempts = commitStore, attempts
	if s.root, err = artifacts.CommitRoot(cfg); err != nil {
		t.Fatalf("the commit root: %v", err)
	}
	refs, ok := store.(artifacts.RefPredictor)
	if !ok {
		t.Fatalf("artifact backend %T cannot predict content-addressed references", store)
	}
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Logs: logStore, Commits: commitStore, Attempts: attempts, Refs: refs, Pool: task.DefaultPool,
		OrkerID:           proof.OrkerID("temporal-stall-" + token),
		WorkRoot:          bindableWorkRootForCommitE2E(t),
		HeartbeatInterval: tick,
		MaxPins:           1,
	})
	if err := activities.Boot(ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}
	activities.Dispatcher().LogSinkObserver = func(in *orkstackv1.ExecuteTaskInput, line string) {
		s.mu.Lock()
		s.logs = append(s.logs, line)
		s.mu.Unlock()
	}

	bus := engine.NewEventBus()
	s.runs = engine.NewRunStore()
	runWorker := worker.New(direct, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	taskWorker := worker.New(proxied, orkerQueue, worker.Options{
		DisableRegistrationAliasing:       true,
		DisableWorkflowWorker:             true,
		EnableSessionWorker:               true,
		MaxConcurrentSessionExecutionSize: 1,
	})
	taskWorker.RegisterActivityWithOptions(activities.ReleasePin,
		activity.RegisterOptions{Name: task.ActivityReleasePin})
	taskWorker.RegisterActivityWithOptions(activities.ExecuteTask,
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
	taskWorker.RegisterActivityWithOptions(activities.DescribeOrker,
		activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	eventWorker := worker.New(direct, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, s.runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	for name, w := range map[string]worker.Worker{"run": runWorker, "task": taskWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		t.Cleanup(w.Stop)
	}

	svc := api.NewService(api.Config{
		Orkflows: s.orkflows, Runs: s.runs, Bus: bus,
		Starter:   engine.NewTemporalStarterWithOrkerQueue(direct, task.DefaultQueue, orkerQueue),
		RawQueue:  orkerQueue,
		Artifacts: cfg,
		Limits:    engine.DefaultLimits(),
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := api.NewServer(listener.Addr().String(), api.NewMux(svc, api.MuxOptions{}))
	go func() { _ = server.Serve(listener) }()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	})
	s.runClient = orkstackv1connect.NewRunServiceClient(
		&http.Client{Transport: transport}, "http://"+listener.Addr().String())
	return s
}

// stoppableWork is a container that says it has started, ticks twice a second
// for ticks ticks, says what it does if asked to stop, and writes its values.
func stoppableWork(ticks int) string {
	return fmt.Sprintf(`trap 'echo caught-term; exit 0' TERM; echo ready; i=0; while [ $i -lt %d ]; do echo tick; sleep 0.5; i=$((i+1)); done; printf '{}' > /orkstack/output/values.json; echo finished`, ticks)
}

// submit stores and starts a one-task Orkflow running command.
func (s *stallStack) submit(name, command string, retry *orkstackv1.RetryPolicy) string {
	s.t.Helper()
	pinned := pinnedAlpineForCommitE2E(s.t)
	return s.start(&orkstackv1.Orkflow{
		Name:    name,
		Version: fmt.Sprintf("1.0.%d", time.Now().UnixNano()),
		Tasks: []*orkstackv1.Task{{
			Id: "work", Uses: pinned,
			Image:                          &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
			Command:                        []string{"/bin/sh", "-c", command},
			TimeoutSeconds:                 300,
			CancellationGracePeriodSeconds: 10,
			Network:                        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
			Retry:                          retry,
		}},
	})
}

func (s *stallStack) start(definition *orkstackv1.Orkflow) string {
	s.t.Helper()
	if _, _, err := s.orkflows.Put(s.ctx, definition, artifacts.HeadExpectation{}); err != nil {
		s.t.Fatalf("store the definition: %v", err)
	}
	submitted, err := s.runClient.SubmitRun(s.ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: definition.GetName(), OrkflowVersion: definition.GetVersion(),
	}))
	if err != nil {
		s.t.Fatalf("SubmitRun: %v", err)
	}
	return submitted.Msg.GetRun().GetId()
}

func (s *stallStack) lines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.logs...)
}

func (s *stallStack) count(word string) int {
	return strings.Count(strings.Join(s.lines(), "\n"), word)
}

func (s *stallStack) awaitLine(word string, times int) {
	s.t.Helper()
	for s.count(word) < times {
		select {
		case <-s.ctx.Done():
			s.t.Fatalf("the container never said %q %d times: %v; log: %q", word, times, s.ctx.Err(), s.lines())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// wait lets d pass, on the test's own deadline.
func (s *stallStack) wait(d time.Duration) {
	s.t.Helper()
	select {
	case <-time.After(d):
	case <-s.ctx.Done():
		s.t.Fatalf("the test's deadline passed: %v", s.ctx.Err())
	}
}

// holdNothingHappens keeps the proxy as it is for d and holds the container to
// having been left alone. Its ticks, which reach this test without Temporal, are
// what make that mean something: they say it was running throughout.
func (s *stallStack) holdNothingHappens(d time.Duration) {
	s.t.Helper()
	before := s.count("tick")
	s.wait(d)
	if s.count("tick") == before {
		s.t.Errorf("the container printed nothing for %s, so the window proves nothing", d)
	}
	if s.count("caught-term") != 0 {
		s.t.Errorf("the container was asked to stop inside the window: %q", s.lines())
	}
}

func (s *stallStack) awaitEnd(runID string) *orkstackv1.TaskRun {
	s.t.Helper()
	stream, err := s.runClient.StreamEvents(s.ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID}))
	if err != nil {
		s.t.Fatalf("StreamEvents: %v", err)
	}
	for stream.Receive() {
	}
	if err := stream.Err(); err != nil {
		s.t.Fatalf("event stream: %v", err)
	}
	stream.Close()
	run, ok := s.runs.Get(runID)
	if !ok {
		s.t.Fatal("run read model was not populated")
	}
	s.t.Logf("EVIDENCE run=%s status=%v", runID, run.GetStatus())
	work := taskRunOfIntegration(s.t, run, "work")
	s.t.Logf("EVIDENCE task status=%v attempt=%d code=%q category=%v message=%q",
		work.GetStatus(), work.GetAttempt(), work.GetError().GetCode(), work.GetError().GetCategory(), work.GetError().GetMessage())
	return work
}

// outages are the two ways a frontend goes away: silent, which costs each
// heartbeat its whole deadline, and refusing, which fails each at once.
var outages = map[string]func(*faultproxy.Proxy){
	"silent":   (*faultproxy.Proxy).Stall,
	"refusing": (*faultproxy.Proxy).Cut,
}

// TestIntegration_TemporalStall_AnOutageInsideTheHeartbeatTimeoutLeavesTheTaskAlone
// is three ticks of nothing from the frontend, half the attempt's 30 s heartbeat
// timeout. Nothing has been lost, so nothing may end: the same container runs to
// its end and the first attempt succeeds, with no retry to fall back on. It used
// to fail with a bare "context canceled" after one tick.
func TestIntegration_TemporalStall_AnOutageInsideTheHeartbeatTimeoutLeavesTheTaskAlone(t *testing.T) {
	for name, begin := range outages {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			s := newStallStack(t, ctx)
			runID := s.submit("temporal-stall-short", stoppableWork(60), nil)
			s.awaitLine("ready", 1)

			begin(s.proxy)
			s.holdNothingHappens(15 * time.Second)
			s.proxy.Restore()

			work := s.awaitEnd(runID)
			if work.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || work.GetAttempt() != 1 {
				t.Errorf("task ended %v on attempt %d, %v; want SUCCEEDED on attempt 1",
					work.GetStatus(), work.GetAttempt(), work.GetError())
			}
			if s.count("caught-term") != 0 || s.count("finished") != 1 || s.count("ready") != 1 {
				t.Errorf("the container did not run once, to its end: %q", s.lines())
			}
		})
	}
}

// TestIntegration_TemporalStall_AStallLeavesAPinAlone is the same silence with a
// pin open. The pin's session heartbeats through the SDK's route, not the Orker's,
// and its RPC's deadline is the worker's throttle window, so its survival is a
// claim of its own.
func TestIntegration_TemporalStall_AStallLeavesAPinAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newStallStack(t, ctx)
	runID := s.submitPinned("temporal-stall-pin", 60)
	s.awaitLine("ready", 1)

	s.proxy.Stall()
	s.holdNothingHappens(15 * time.Second)
	s.proxy.Restore()

	work := s.awaitEnd(runID)
	run, _ := s.runs.Get(runID)
	second := taskRunOfIntegration(t, run, "second")
	if work.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || work.GetAttempt() != 1 ||
		second.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("work ended %v on attempt %d, %v; second %v, %v; want both SUCCEEDED",
			work.GetStatus(), work.GetAttempt(), work.GetError(), second.GetStatus(), second.GetError())
	}
	if s.count("caught-term") != 0 {
		t.Errorf("the pinned container was asked to stop during a stall that lost nothing")
	}
}

// TestIntegration_TemporalStall_APinPastItsDeadlineFailsAndIsNotReopened: the
// session's heartbeat is the SDK's, and past its deadline it ends the pin for
// good. The run says so and stops, and the pin opens nowhere else (AGENTS.md §5).
func TestIntegration_TemporalStall_APinPastItsDeadlineFailsAndIsNotReopened(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newStallStack(t, ctx)
	runID := s.submitPinned("temporal-stall-pin-long", 200)
	s.awaitLine("ready", 1)

	s.proxy.Stall()
	s.holdNothingHappens(15 * time.Second)
	s.awaitLine("caught-term", 1)
	s.proxy.Restore()

	work := s.awaitEnd(runID)
	if work.GetStatus() != orkstackv1.Status_STATUS_FAILED || work.GetError().GetCode() != "PIN_HOST_LOST" ||
		work.GetError().GetRetryable() {
		t.Errorf("task ended %v, %v; want FAILED, a non-retryable PIN_HOST_LOST", work.GetStatus(), work.GetError())
	}
	if got := s.count("ready"); got != 1 {
		t.Errorf("the pinned task ran %d times, want once: a pin is never re-opened", got)
	}
}

// submitPinned is submit with two members of one pin, the second after the first.
func (s *stallStack) submitPinned(name string, ticks int) string {
	s.t.Helper()
	pinned := pinnedAlpineForCommitE2E(s.t)
	return s.start(&orkstackv1.Orkflow{
		Name:    name,
		Version: fmt.Sprintf("1.0.%d", time.Now().UnixNano()),
		Pins:    []*orkstackv1.Pin{{Name: "share"}},
		Tasks: []*orkstackv1.Task{{
			Id: "work", Uses: pinned, Pin: "share", RunAsOrkerUser: true,
			Image:                          &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
			Command:                        []string{"/bin/sh", "-c", stoppableWork(ticks)},
			TimeoutSeconds:                 300,
			CancellationGracePeriodSeconds: 10,
			Network:                        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}, {
			Id: "second", Uses: pinned, Pin: "share", Needs: []string{"work"}, RunAsOrkerUser: true,
			Image:          &orkstackv1.ImageRef{Reference: pinned, Pinned: pinned},
			Command:        []string{"/bin/sh", "-c", `printf '{}' > /orkstack/output/values.json`},
			TimeoutSeconds: 120,
			Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		}},
	})
}

// TestIntegration_TemporalStall_AnOperatorCancelDuringAStallReachesTheContainer:
// the cluster records the cancel during the silence, and the container hears of
// it, with SIGTERM, from the first heartbeat answered after. Told apart from the
// stall itself, it stays a cancel.
func TestIntegration_TemporalStall_AnOperatorCancelDuringAStallReachesTheContainer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newStallStack(t, ctx)
	runID := s.submit("temporal-stall-cancel", stoppableWork(200), nil)
	s.awaitLine("ready", 1)

	s.proxy.Stall()
	if _, err := s.runClient.CancelRun(ctx, connect.NewRequest(&orkstackv1.CancelRunRequest{RunId: runID, Reason: "operator"})); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	s.holdNothingHappens(10 * time.Second)
	restored := time.Now()
	s.proxy.Restore()
	s.awaitLine("caught-term", 1)
	t.Logf("EVIDENCE SIGTERM %s after the stall ended", time.Since(restored).Round(time.Millisecond))

	work := s.awaitEnd(runID)
	if work.GetStatus() != orkstackv1.Status_STATUS_CANCELED || work.GetError().GetCode() != "TASK_CANCELED" ||
		work.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED {
		t.Errorf("task ended %v, %v; want CANCELED, TASK_CANCELED", work.GetStatus(), work.GetError())
	}
}

// TestIntegration_TemporalStall_NoAckForTheHeartbeatTimeoutStopsTheContainer: past
// the timeout the cluster has given the attempt up and a retry may start, so the
// Orker stops the container itself, with the frontend still silent. The cluster
// reports the attempt's heartbeat timeout; the attempt's own record says what the
// Orker did. A task with a retry runs again and succeeds.
func TestIntegration_TemporalStall_NoAckForTheHeartbeatTimeoutStopsTheContainer(t *testing.T) {
	for _, tt := range []struct {
		name  string
		retry *orkstackv1.RetryPolicy
	}{
		{"no retry", nil},
		{"a retry", &orkstackv1.RetryPolicy{MaximumAttempts: 2, InitialIntervalSeconds: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
			defer cancel()
			s := newStallStack(t, ctx)
			runID := s.submit("temporal-stall-long", stoppableWork(120), tt.retry)
			s.awaitLine("ready", 1)

			began := time.Now()
			s.proxy.Stall()
			s.holdNothingHappens(20 * time.Second)
			s.awaitLine("caught-term", 1)
			stopped := time.Since(began)
			// An acknowledgement may have come up to a tick before the stall.
			if earliest := task.HeartbeatTimeout - orker.DefaultHeartbeatInterval - time.Second; stopped < earliest {
				t.Errorf("the container was stopped %s into the stall, before the timeout could pass (%s)", stopped, earliest)
			}
			t.Logf("EVIDENCE the frontend silent, the container got SIGTERM %s into the stall", stopped.Round(time.Millisecond))
			s.proxy.Restore()

			work := s.awaitEnd(runID)
			if tt.retry == nil {
				got := work.GetError()
				if work.GetStatus() != orkstackv1.Status_STATUS_FAILED || got.GetCode() != "ORKER_HEARTBEAT_TIMEOUT" ||
					got.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR || !got.GetRetryable() {
					t.Errorf("task ended %v, %v; want FAILED, a retryable ORKER_HEARTBEAT_TIMEOUT", work.GetStatus(), got)
				}
			} else {
				if work.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || work.GetAttempt() != 2 {
					t.Errorf("task ended %v on attempt %d, %v; want SUCCEEDED on attempt 2", work.GetStatus(), work.GetAttempt(), work.GetError())
				}
				if s.count("ready") != 2 || s.count("finished") != 1 {
					t.Errorf("want two containers and one finished: %q", s.lines())
				}
			}
			// The cluster gave the attempt up before the Orker had finished stopping
			// it, so the run can end before the attempt's record is written.
			var records []*orkstackv1.TaskAttempt
			for len(records) == 0 {
				var err error
				if records, err = s.attempts.ListAttempts(ctx, runID, "work", artifacts.NoItem); err != nil {
					t.Fatalf("the attempt's records: %v", err)
				}
				if len(records) == 0 {
					s.wait(100 * time.Millisecond)
				}
			}
			if got := records[0].GetError().GetCode(); got != orker.ReasonHeartbeatLost {
				t.Errorf("the first attempt's record says %q, want %s", got, orker.ReasonHeartbeatLost)
			}
		})
	}
}

// TestIntegration_TemporalStall_ACommitWrittenInAStallIsNotDoneAgain: a container
// that finishes during a stall commits to the object store, which needs nothing
// of Temporal. Inside the timeout the attempt reports its result when the frontend
// is back. Past it the cluster has given the attempt up, and the retry adopts the
// commit and starts no container.
func TestIntegration_TemporalStall_ACommitWrittenInAStallIsNotDoneAgain(t *testing.T) {
	for _, tt := range []struct {
		name  string
		after time.Duration
		retry *orkstackv1.RetryPolicy
	}{
		{"inside the timeout", 0, nil},
		{"past the timeout", task.HeartbeatTimeout + 5*time.Second, &orkstackv1.RetryPolicy{MaximumAttempts: 2, InitialIntervalSeconds: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			s := newStallStack(t, ctx)
			runID := s.submit("temporal-stall-commit",
				`echo ready; printf '{"done":true}' > /orkstack/output/values.json; sleep 3`, tt.retry)
			s.awaitLine("ready", 1)

			s.proxy.Stall()
			key, err := artifacts.CommitKey("", runID, "work")
			if err != nil {
				t.Fatal(err)
			}
			for {
				if _, err := s.commits.Get(ctx, s.root+"/"+key); err == nil {
					break
				}
				s.wait(100 * time.Millisecond)
			}
			t.Logf("EVIDENCE the commit is written with the frontend silent")
			s.wait(tt.after)
			s.proxy.Restore()

			work := s.awaitEnd(runID)
			if work.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Errorf("task ended %v, %v; want SUCCEEDED", work.GetStatus(), work.GetError())
			}
			if got := s.count("ready"); got != 1 {
				t.Errorf("the container ran %d times, want once: the commit answers for it", got)
			}
		})
	}
}
