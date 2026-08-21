//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// s3Config is the artifact/orkflow storage config for these tests: the repo's
// dev MinIO, overridable through ORK_S3_*.
func s3Config() artifacts.Config {
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	return cfg
}

// requireMinIO, shared with the other integration suites in this package,
// lives in archive_integration_test.go.

// newDurableOrkflowStore builds the store the ork binary builds: orkflow
// definitions in S3, no process-local copy.
func newDurableOrkflowStore(t *testing.T) *artifacts.S3OrkflowStore {
	t.Helper()
	cfg := s3Config()
	requireMinIO(t, cfg)
	store, err := artifacts.NewS3OrkflowStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open orkflow store: %v", err)
	}
	return store
}

// probeOrkflowName is the fixed name a test owns in the shared dev bucket. It
// is stable on purpose: definitions are immutable per version, so re-running
// the suite rewrites one object instead of littering the bucket with a new
// orkflow every run.
func probeOrkflowName(t *testing.T, kind string) string {
	t.Helper()
	return "m75-" + kind + "-probe"
}

// helloYAMLNamed renders the repo's example orkflow under another name, so a
// test owns its own orkflow in the shared bucket.
func helloYAMLNamed(t *testing.T, name string) string {
	t.Helper()
	source, err := os.ReadFile("../../examples/hello.ork.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	renamed := strings.Replace(string(source), "name: hello-ork\n", "name: "+name+"\n", 1)
	if renamed == string(source) {
		t.Fatal("example orkflow no longer declares `name: hello-ork`")
	}
	return renamed
}

// serveAPI starts the real Connect server in-process over the given orkflow
// store and returns a client for it plus a shutdown function. This is the same
// wiring cmd/ork uses, minus Temporal.
func serveAPI(
	t *testing.T,
	store engine.OrkflowStore,
) (orkstackv1connect.OrkflowServiceClient, func()) {
	t.Helper()
	svc := api.NewService(api.Config{
		Orkflows: store,
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
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
	client := orkstackv1connect.NewOrkflowServiceClient(
		&http.Client{Transport: transport}, "http://"+listener.Addr().String())

	return client, func() {
		transport.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
}

// TestIntegration_AppliedOrkflowOutlivesTheServiceThatStoredIt is the M7.5
// contract at API level: apply writes the definition to
// s3://{bucket}/metadata/orkflows/{namespace}/{name}/{version}.json, and a service built
// afterwards — with its own store, as a restarted engine has — serves it.
// Nothing is carried over in memory.
func TestIntegration_AppliedOrkflowOutlivesTheServiceThatStoredIt(t *testing.T) {
	store := newDurableOrkflowStore(t)
	ctx := context.Background()
	name := probeOrkflowName(t, "restart")

	before, shutdownBefore := serveAPI(t, store)
	applied, err := before.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: helloYAMLNamed(t, name),
	}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	version := applied.Msg.GetOrkflow().GetVersion()
	if applied.Msg.GetOrkflow().GetName() != name {
		t.Fatalf("applied %q, want %q", applied.Msg.GetOrkflow().GetName(), name)
	}

	uri, err := store.URI(artifacts.DefaultNamespace, name, version)
	if err != nil {
		t.Fatalf("URI: %v", err)
	}
	t.Logf("applied orkflow lives at %s", uri)
	if want := fmt.Sprintf("s3://%s/metadata/orkflows/%s/%s/%s.json", store.Bucket(), artifacts.DefaultNamespace, name, version); uri != want {
		t.Fatalf("orkflow URI = %q, want %q", uri, want)
	}

	// The restart: the engine that accepted the apply is gone, along with its
	// store handle.
	shutdownBefore()
	after, shutdownAfter := serveAPI(t, newDurableOrkflowStore(t))
	defer shutdownAfter()

	got, err := after.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: name}))
	if err != nil {
		t.Fatalf("GetOrkflow after restart: %v", err)
	}
	reloaded := got.Msg.GetOrkflow()
	if reloaded.GetVersion() != version {
		t.Errorf("reloaded version = %q, want %q", reloaded.GetVersion(), version)
	}
	if len(reloaded.GetTasks()) != 3 {
		t.Fatalf("reloaded orkflow has %d tasks, want the example's 3", len(reloaded.GetTasks()))
	}
	if got := reloaded.GetTasks()[0].GetWith().AsMap()["url"]; got != "{{ .inputs.url }}" {
		t.Errorf("reloaded task lost its raw template: %v", got)
	}
	if !strings.Contains(reloaded.GetSourceYaml(), "name: "+name) {
		t.Error("reloaded orkflow lost its source document")
	}

	listed, err := after.ListOrkflows(ctx, connect.NewRequest(&orkstackv1.ListOrkflowsRequest{}))
	if err != nil {
		t.Fatalf("ListOrkflows after restart: %v", err)
	}
	found := false
	for _, wf := range listed.Msg.GetOrkflows() {
		if wf.GetName() == name {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListOrkflows after restart does not report %q (%d orkflows)",
			name, len(listed.Msg.GetOrkflows()))
	}
}

// TestIntegration_ListOrkflowsReadsTheBucketNotTheProcess proves the list path
// has no process-local copy to answer from: an orkflow written straight to the
// bucket by another writer appears, and one written through the API is visible
// to a store that never saw the request.
func TestIntegration_ListOrkflowsReadsTheBucketNotTheProcess(t *testing.T) {
	store := newDurableOrkflowStore(t)
	ctx := context.Background()
	name := probeOrkflowName(t, "sidewriter")

	client, shutdown := serveAPI(t, newDurableOrkflowStore(t))
	defer shutdown()

	// Written by a different store instance, as a second engine would.
	if _, _, err := store.Put(ctx, &orkstackv1.Orkflow{
		Name:    name,
		Version: "0.1.0",
		Tasks:   []*orkstackv1.Task{{Id: "fetch", Plugin: "http"}},
	}, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put: %v", err)
	}

	listed, err := client.ListOrkflows(ctx, connect.NewRequest(&orkstackv1.ListOrkflowsRequest{}))
	if err != nil {
		t.Fatalf("ListOrkflows: %v", err)
	}
	for _, wf := range listed.Msg.GetOrkflows() {
		if wf.GetName() == name {
			return
		}
	}
	t.Fatalf("ListOrkflows does not report %q written by another store (%d orkflows)",
		name, len(listed.Msg.GetOrkflows()))
}
