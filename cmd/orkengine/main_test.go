package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

func TestWorkerOptions_KeepSharedQueueEngineFromExecutingOrkerActivities(t *testing.T) {
	runOptions := runWorkerOptions()
	if !runOptions.LocalActivityWorkerOnly {
		t.Fatal("run worker must be workflow/local-activity only on the shared Orker queue")
	}
	if runOptions.DisableWorkflowWorker {
		t.Fatal("run worker must execute RunWorkflow")
	}
	if !runOptions.DisableRegistrationAliasing {
		t.Fatal("run worker must disable registration aliases")
	}

	eventOptions := eventWorkerOptions()
	if !eventOptions.DisableWorkflowWorker {
		t.Fatal("event worker must be activity-only")
	}
	if eventOptions.LocalActivityWorkerOnly {
		t.Fatal("event worker must execute the PublishEvent regular activity")
	}
	if !eventOptions.DisableRegistrationAliasing {
		t.Fatal("event worker must disable registration aliases")
	}
}

func TestServerShutdownCancelsActiveEventStream(t *testing.T) {
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{Id: "active", Status: orkstackv1.Status_STATUS_RUNNING})
	svc := api.NewService(api.Config{Runs: runs, Bus: bus})
	server := api.NewServer("127.0.0.1:0", api.NewMux(svc, api.MuxOptions{}))
	cancelHandlers := installServerShutdownContext(server)
	defer cancelHandlers()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	defer transport.CloseIdleConnections()
	// Seed one event so the server flushes stream response headers; the stream
	// remains active after this nonterminal backlog item is consumed.
	bus.Publish(&orkstackv1.Event{
		RunId:  "active",
		Type:   orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
		Status: orkstackv1.Status_STATUS_RUNNING,
	}, "started")

	client := orkstackv1connect.NewRunServiceClient(
		&http.Client{Transport: transport}, "http://"+listener.Addr().String())
	stream, err := client.StreamEvents(context.Background(),
		connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: "active"}))
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	if !stream.Receive() {
		t.Fatalf("receive initial event: %v", stream.Err())
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown with active StreamEvents: %v", err)
	}
	if stream.Receive() {
		t.Fatalf("received event after shutdown: %v", stream.Msg())
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve: %v", err)
	}
}
