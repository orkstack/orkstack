package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// heavyOrkflowDoc is an Orkflow of n tasks, each written as heavily as an
// author writes one: a comment, two needs, a timeout, a retry policy,
// resources, environment values and an inline script, about 920 bytes a task
// (the shipped examples run 400 to 700). The first 20 tasks start together
// and every other task needs two of them, so its ancestry stays inside the
// activity input bound however large n is.
func heavyOrkflowDoc(name string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\n"+
		"description: Every task written the way an author writes one.\ntasks:\n", name)
	for i := range n {
		fmt.Fprintf(&b, `  # Step %d of the pipeline: it waits for the two steps it needs, then
  # runs its stage's script with the stage's settings. A real document
  # says why each step exists; this comment is sized like one.
  - id: step-%d
    uses: busybox:latest
`, i, i)
		if i >= 20 {
			fmt.Fprintf(&b, "    needs:\n      - step-%d\n      - step-%d\n", i%20, (i+7)%20)
		}
		fmt.Fprintf(&b, `    timeout_seconds: 600
    cancellation_grace_period_seconds: 30
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 1
      backoff_coefficient: 2.0
      maximum_interval_seconds: 60
    resources:
      cpu: "0.5"
      memory: 256Mi
    env:
      STAGE: step-%d
      REGION: eu-west-1
      LOG_LEVEL: info
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        echo "stage step-%d starting in $REGION"
        for part in a b c d; do
          echo "processing part $part of step-%d"
        done
        echo "done" > /orkstack/output/artifacts/step-%d.txt
`, i, i, i, i)
	}
	return b.String()
}

// largeAutomationDoc is an automation document for namespace payments with n
// schedules, each with a comment and an input, n triggers and n/10 webhooks.
func largeAutomationDoc(n int) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Automation\nnamespace: payments\nsettings:\n  run_retention: 72h\nschedules:\n")
	for i := range n {
		fmt.Fprintf(&b, `  # Fetch region %d's export on its own quarter hour, skipping a fire
  # while the previous one still runs.
  export-%d:
    orkflow: hello-ork
    description: "Fetch and summarize region %d's export every 15 minutes"
    cron: "%d-59/15 * * * *"
    timezone: UTC
    overlap: skip
    input:
      url: https://exports.example.com/regions/%d/latest.json?format=json&include=summary,details,history&window=24h
`, i, i, i, i%15, i)
	}
	b.WriteString("triggers:\n")
	for i := range n {
		fmt.Fprintf(&b, "  hook-%d:\n    orkflow: hello-ork\n    description: \"CI starts region %d's run\"\n", i, i)
	}
	b.WriteString("webhooks:\n")
	for i := range n / 10 {
		fmt.Fprintf(&b, "  sink-%d:\n    url: https://hooks.example.test/regions/%d/events\n", i, i)
	}
	return b.String()
}

// The largest legitimate requests, measured: an apply of an Orkflow at the
// deployment's default task ceiling, every task written heavily, and the plan
// of an automation document of a thousand schedules and a thousand triggers.
// The default read cap is at least four times the larger of them in either
// encoding a client sends — JSON from the UI, proto from orkctl — so a
// document twice as heavy, at twice the ceiling, still fits. A change that
// shrinks the default below that fails here.
func TestDefaultRequestMaxBytes_IsFourTimesTheLargestMeasuredRequest(t *testing.T) {
	apply := &orkstackv1.ApplyOrkflowRequest{Yaml: heavyOrkflowDoc("heavy", int(engine.DefaultOrkflowMaxTasks))}
	plan := &orkstackv1.PlanAutomationRequest{Yaml: largeAutomationDoc(1000)}
	for _, tc := range []struct {
		name    string
		request proto.Message
	}{
		{fmt.Sprintf("apply of %d heavy tasks", engine.DefaultOrkflowMaxTasks), apply},
		{"plan of 1000 schedules and 1000 triggers", plan},
	} {
		asJSON, err := protojson.Marshal(tc.request)
		if err != nil {
			t.Fatal(err)
		}
		asProto := proto.Size(tc.request)
		t.Logf("%s: %d bytes as JSON, %d as proto", tc.name, len(asJSON), asProto)
		for encoding, size := range map[string]int{"JSON": len(asJSON), "proto": asProto} {
			if size*4 > api.DefaultRequestMaxBytes {
				t.Errorf("%s as %s is %d bytes; the default read cap %d is less than four times it",
					tc.name, encoding, size, api.DefaultRequestMaxBytes)
			}
		}
	}
}

// serveCapped serves svc behind NewMux with the given read cap and returns
// its base URL and an h2c client.
func serveCapped(t *testing.T, svc *api.Service, requestMaxBytes int64) (string, *http.Client) {
	t.Helper()
	server := api.NewServer("127.0.0.1:0", api.NewMux(svc, api.MuxOptions{RequestMaxBytes: requestMaxBytes}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	transport := &http.Transport{Protocols: new(http.Protocols)}
	transport.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	return "http://" + listener.Addr().String(), &http.Client{Transport: transport}
}

// smallDoc is a valid Orkflow, padded with a comment to padding bytes more.
func smallDoc(padding int) string {
	return "apiVersion: orkstack/v1\nkind: Orkflow\nname: padded\ntasks:\n  - id: one\n    uses: busybox:latest\n# " +
		strings.Repeat("x", padding) + "\n"
}

// A Connect request over the read cap is refused as resource_exhausted, a
// Connect error the client reads like any other, and the connection it came
// on goes on serving. One at the cap is served.
func TestNewMux_ARequestOverTheReadCapIsResourceExhausted(t *testing.T) {
	const limit = 64 << 10
	baseURL, h2c := serveCapped(t, api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()}), limit)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("JSON over HTTP/1.1, at the cap and one byte over", func(t *testing.T) {
		// The body is built by hand so its size is exact: protojson's output
		// is deliberately unstable.
		body := func(size int) []byte {
			envelope := func(yaml string) []byte {
				encoded, err := json.Marshal(map[string]string{"yaml": yaml})
				if err != nil {
					t.Fatal(err)
				}
				return encoded
			}
			overhead := len(envelope(smallDoc(0)))
			out := envelope(smallDoc(size - overhead))
			if len(out) != size {
				t.Fatalf("built a %d-byte body, want %d", len(out), size)
			}
			return out
		}
		client := &http.Client{Transport: &http.Transport{}}
		post := func(payload []byte) (int, string) {
			request, err := http.NewRequestWithContext(ctx, http.MethodPost,
				baseURL+orkstackv1connect.OrkflowServiceValidateOrkflowProcedure, bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Connect-Protocol-Version", "1")
			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("post %d bytes: %v (the connection was dropped instead of answered)", len(payload), err)
			}
			defer func() { _ = response.Body.Close() }()
			answer, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			return response.StatusCode, string(answer)
		}
		if status, answer := post(body(limit)); status != http.StatusOK {
			t.Fatalf("a request of exactly the cap: %d %s, want 200", status, answer)
		}
		status, answer := post(body(limit + 1))
		if status != http.StatusTooManyRequests || !strings.Contains(answer, `"code":"resource_exhausted"`) ||
			!strings.Contains(answer, fmt.Sprintf("larger than configured max %d", limit)) {
			t.Fatalf("one byte over the cap: %d %s, want 429 resource_exhausted naming the cap", status, answer)
		}
		if status, answer := post(body(1024)); status != http.StatusOK {
			t.Fatalf("after the refusal: %d %s, want 200", status, answer)
		}
	})

	t.Run("proto over h2c", func(t *testing.T) {
		client := orkstackv1connect.NewOrkflowServiceClient(h2c, baseURL)
		_, err := client.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: smallDoc(2 * limit)}))
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Fatalf("a request twice the cap: %v, want resource_exhausted", err)
		}
		if _, err := client.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: smallDoc(16)})); err != nil {
			t.Fatalf("after the refusal: %v", err)
		}
	})

	t.Run("gzip that inflates past the cap", func(t *testing.T) {
		// A few kilobytes on the wire, ten times the cap once inflated: the
		// cap holds the message as decompressed, not only as sent.
		client := orkstackv1connect.NewOrkflowServiceClient(h2c, baseURL, connect.WithSendGzip())
		_, err := client.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: smallDoc(10 * limit)}))
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Fatalf("a compressed request inflating to ten times the cap: %v, want resource_exhausted", err)
		}
	})
}

// The default cap admits, over the wire, an apply of an Orkflow at the
// deployment's default task ceiling as the UI sends it (JSON over HTTP/1.1)
// and an automation plan as orkctl sends it (proto over h2c).
func TestDefaultRequestMaxBytes_AdmitsAnApplyAtTheTaskCeilingOverTheWire(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test: every proof's unit gate runs it; make check skips it")
	}
	h := newAutomationHarness(t, nil)
	baseURL, h2c := serveCapped(t, h.svc, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	http1 := &http.Client{Transport: &http.Transport{}}
	applied, err := orkstackv1connect.NewOrkflowServiceClient(http1, baseURL, connect.WithProtoJSON()).ApplyOrkflow(ctx,
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: heavyOrkflowDoc("heavy", int(engine.DefaultOrkflowMaxTasks))}))
	if err != nil {
		t.Fatalf("apply of %d heavy tasks under the default cap: %v", engine.DefaultOrkflowMaxTasks, err)
	}
	if got := len(applied.Msg.GetOrkflow().GetTasks()); got != int(engine.DefaultOrkflowMaxTasks) {
		t.Fatalf("applied %d tasks, want %d", got, engine.DefaultOrkflowMaxTasks)
	}

	planned, err := orkstackv1connect.NewAutomationServiceClient(h2c, baseURL).PlanAutomation(ctx,
		connect.NewRequest(&orkstackv1.PlanAutomationRequest{Yaml: largeAutomationDoc(1000)}))
	if err != nil {
		t.Fatalf("plan of a large automation document under the default cap: %v", err)
	}
	if issues := planned.Msg.GetIssues(); len(issues) != 0 {
		t.Fatalf("plan issues: %v", issues)
	}
}
