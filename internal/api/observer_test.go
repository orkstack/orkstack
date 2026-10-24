package api_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// recordingObserver stands in for the telemetry seam this service announces
// to (internal/telemetry.EngineObserver in production).
type recordingObserver struct {
	mu     sync.Mutex
	events []*orkstackv1.Event
	runs   []*orkstackv1.Run
}

func (o *recordingObserver) ObserveEvent(
	_ context.Context,
	event *orkstackv1.Event,
	run *orkstackv1.Run,
) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
	o.runs = append(o.runs, run)
}

// newFixtureWithObserver is newFixture with run transitions also announced to
// observer, so a test can assert that a run failing to start reaches
// telemetry the same way it reaches a webhook.
func newFixtureWithObserver(t *testing.T, observer engine.EventObserver) *fixture {
	t.Helper()

	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	starter := &fakeStarter{}
	var counter int
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     runs,
		Bus:      bus,
		Starter:  starter,
		Observer: observer,
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})

	server := api.NewServer("127.0.0.1:0", api.NewMux(svc, api.MuxOptions{}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()

	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)

	t.Cleanup(func() {
		transport.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	return &fixture{
		svc:     svc,
		bus:     bus,
		runs:    runs,
		starter: starter,
		baseURL: "http://" + listener.Addr().String(),
		client:  &http.Client{Transport: transport},
	}
}

// TestSubmitRun_StarterFailureReachesTheObserver mirrors
// TestSubmitRunReportsStarterFailure, but for the telemetry seam: a run that
// never reaches Temporal must still be counted as FAILED, exactly like the
// webhook notifier is told about it.
func TestSubmitRun_StarterFailureReachesTheObserver(t *testing.T) {
	observer := &recordingObserver{}
	f := newFixtureWithObserver(t, observer)
	ctx := context.Background()
	if _, err := f.orkflowClient().ApplyOrkflow(ctx,
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	f.starter.mu.Lock()
	f.starter.err = errors.New("temporal is down")
	f.starter.mu.Unlock()

	_, err := f.runClient().SubmitRun(ctx,
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"}))
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("SubmitRun code = %v, want Internal", connect.CodeOf(err))
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.events) != 1 {
		t.Fatalf("observer saw %d events, want 1", len(observer.events))
	}
	if got := observer.events[0].GetType(); got != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Errorf("observed event type = %v, want RUN_FAILED", got)
	}
	if got := observer.runs[0].GetStatus(); got != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("observed run status = %v, want FAILED", got)
	}
}

// TestSubmitRun_StarterFailureWithoutAnObserver documents that a nil Observer
// changes nothing: SubmitRun still reports the same Internal error.
func TestSubmitRun_StarterFailureWithoutAnObserver(t *testing.T) {
	f := newFixtureWithObserver(t, nil)
	ctx := context.Background()
	if _, err := f.orkflowClient().ApplyOrkflow(ctx,
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	f.starter.mu.Lock()
	f.starter.err = errors.New("temporal is down")
	f.starter.mu.Unlock()

	_, err := f.runClient().SubmitRun(ctx,
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"}))
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("SubmitRun code = %v, want Internal", connect.CodeOf(err))
	}
}
