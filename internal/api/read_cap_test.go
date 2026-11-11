package api_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"

	"github.com/orkstack/orkstack/internal/testsupport/must"
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

// requestSizes is a request's size in either encoding a client sends: JSON
// from the UI, proto from orkctl.
func requestSizes(t *testing.T, request proto.Message) map[string]int64 {
	t.Helper()
	asJSON, err := protojson.Marshal(request)
	must.OK(t, err)
	return map[string]int64{"JSON": int64(len(asJSON)), "proto": int64(proto.Size(request))}
}

// The read cap scales with the task ceiling, so it is never a definition
// limit (AGENTS.md §7): an apply at any ceiling, every task written heavily,
// is at most a quarter of the cap that ceiling derives, in either encoding.
// The heaviest task measured costs a request about 920 bytes; the cap allows
// four times that a task, over a base that holds the plan of an automation
// document of a thousand schedules and a thousand triggers with half again
// to spare. A
// change that shrinks either below that fails here.
func TestRequestMaxBytesFor_IsFourTimesAnApplyAtEveryTaskCeiling(t *testing.T) {
	ceilings := []int32{1, 10, 500, engine.DefaultOrkflowMaxTasks}
	sizes := make(map[int32]map[string]int64, len(ceilings))
	for _, ceiling := range ceilings {
		sizes[ceiling] = requestSizes(t, &orkstackv1.ApplyOrkflowRequest{Yaml: heavyOrkflowDoc("heavy", int(ceiling))})
		limit := api.RequestMaxBytesFor(ceiling)
		for encoding, size := range sizes[ceiling] {
			t.Logf("apply of %d heavy tasks: %d bytes as %s; the cap at that ceiling is %d", ceiling, size, encoding, limit)
			if size*4 > limit {
				t.Errorf("apply of %d heavy tasks as %s is %d bytes; the cap %d at that ceiling is less than four times it",
					ceiling, encoding, size, limit)
			}
		}
	}
	first, last := ceilings[0], ceilings[len(ceilings)-1]
	for encoding := range sizes[last] {
		perTask := (sizes[last][encoding] - sizes[first][encoding]) / int64(last-first)
		t.Logf("one heavy task adds %d bytes as %s", perTask, encoding)
		if perTask*4 > api.RequestBytesPerTask {
			t.Errorf("one heavy task adds %d bytes as %s; RequestBytesPerTask %d is less than four times it",
				perTask, encoding, api.RequestBytesPerTask)
		}
	}
	for encoding, size := range requestSizes(t, &orkstackv1.PlanAutomationRequest{Yaml: largeAutomationDoc(1000)}) {
		t.Logf("plan of 1000 schedules and 1000 triggers: %d bytes as %s", size, encoding)
		if size*3/2 > api.RequestBaseBytes {
			t.Errorf("plan of 1000 schedules and 1000 triggers as %s is %d bytes; RequestBaseBytes %d is less than half again over it",
				encoding, size, api.RequestBaseBytes)
		}
	}
}

// Raising the task ceiling raises the cap, and the default is the cap of the
// default ceiling. What the cap must be at each ceiling is held by
// TestRequestMaxBytesFor_IsFourTimesAnApplyAtEveryTaskCeiling above, against
// requests measured at those ceilings; this holds only that the ceiling is
// what the cap follows.
func TestRequestMaxBytesFor_FollowsTheTaskCeiling(t *testing.T) {
	previous := api.RequestMaxBytesFor(1)
	for _, ceiling := range []int32{2, 1000, engine.DefaultOrkflowMaxTasks, 100_000} {
		got := api.RequestMaxBytesFor(ceiling)
		if got <= previous {
			t.Errorf("RequestMaxBytesFor(%d) = %d, not above %d, the cap at a lower ceiling", ceiling, got, previous)
		}
		previous = got
	}
	if got, want := int64(api.DefaultRequestMaxBytes), api.RequestMaxBytesFor(engine.DefaultOrkflowMaxTasks); got != want {
		t.Errorf("DefaultRequestMaxBytes = %d, want RequestMaxBytesFor(%d) = %d", got, engine.DefaultOrkflowMaxTasks, want)
	}
}

// serveCapped serves svc behind NewMux with the given read cap and returns
// its base URL and an h2c client.
func serveCapped(t *testing.T, svc *api.Service, requestMaxBytes int64) (string, *http.Client) {
	t.Helper()
	server := api.NewServer("127.0.0.1:0", api.NewMux(svc, api.MuxOptions{RequestMaxBytes: requestMaxBytes}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must.OKf(t, err, "listen")
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
				encoded, err := stablejson.Marshal(map[string]string{"yaml": yaml})
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
			must.OK(t, err)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Connect-Protocol-Version", "1")
			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("post %d bytes: %v (the connection was dropped instead of answered)", len(payload), err)
			}
			defer func() { _ = response.Body.Close() }()
			answer, err := io.ReadAll(response.Body)
			must.OK(t, err)
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
	must.OKf(t, err, "apply of %d heavy tasks under the default cap", engine.DefaultOrkflowMaxTasks)
	if got := len(applied.Msg.GetOrkflow().GetTasks()); got != int(engine.DefaultOrkflowMaxTasks) {
		t.Fatalf("applied %d tasks, want %d", got, engine.DefaultOrkflowMaxTasks)
	}

	planned, err := orkstackv1connect.NewAutomationServiceClient(h2c, baseURL).PlanAutomation(ctx,
		connect.NewRequest(&orkstackv1.PlanAutomationRequest{Yaml: largeAutomationDoc(1000)}))
	must.OKf(t, err, "plan of a large automation document under the default cap")
	if issues := planned.Msg.GetIssues(); len(issues) != 0 {
		t.Fatalf("plan issues: %v", issues)
	}
}

// endlessReader is a request body that never ends; it counts what it gave.
type endlessReader struct{ given atomic.Int64 }

func (r *endlessReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	r.given.Add(int64(len(p)))
	return len(p), nil
}

// A body that never ends cannot hold a connection, on any Connect route:
// Connect reads an over-cap message's rest away so its client is answered,
// and the listener stops that reading at twice the cap, answers
// resource_exhausted, and then closes an HTTP/1.1 connection or resets an
// h2c stream, however long the client goes on sending. One procedure of
// every service is tried, so a service mounted without the bound fails here.
func TestNewMux_AnEndlessBodyIsCutAtTwiceTheCap(t *testing.T) {
	const limit = 64 << 10
	baseURL, h2c := serveCapped(t, api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()}), limit)
	addr := strings.TrimPrefix(baseURL, "http://")
	for _, procedure := range []string{
		orkstackv1connect.ActivityServiceListActivitiesProcedure,
		orkstackv1connect.AuditServiceListAuditRecordsProcedure,
		orkstackv1connect.AutomationServicePlanAutomationProcedure,
		orkstackv1connect.CacheServiceGetCacheStatsProcedure,
		orkstackv1connect.NamespaceServiceGetNamespaceProcedure,
		orkstackv1connect.OrkflowServiceValidateOrkflowProcedure,
		orkstackv1connect.RunServiceGetArtifactProvenanceProcedure,
		orkstackv1connect.ScheduleServiceGetScheduleProcedure,
		orkstackv1connect.StatsServiceGetFailureBreakdownProcedure,
		orkstackv1connect.SystemServiceGetImagePolicyProcedure,
		orkstackv1connect.TriggerServiceListTriggersProcedure,
		orkstackv1connect.WebhookServiceGetDeliveryProcedure,
	} {
		t.Run(procedure+" over HTTP/1.1", func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			must.OK(t, err)
			defer conn.Close()
			if _, err := io.WriteString(conn, "POST "+procedure+" HTTP/1.1\r\nHost: "+addr+
				"\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
			// The client sends a chunk at a time until the connection
			// fails, and counts what it sent: the positive control that the
			// body never ended.
			var sent atomic.Int64
			go func() {
				chunk := fmt.Sprintf("%x\r\n%s\r\n", 16<<10, strings.Repeat("x", 16<<10))
				for {
					n, err := io.WriteString(conn, chunk)
					sent.Add(int64(n))
					if err != nil {
						return
					}
				}
			}()
			_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
			reader := bufio.NewReader(conn)
			res, err := http.ReadResponse(reader, nil)
			must.OKf(t, err, "no answer to an endless body within 15s (%d bytes sent)", sent.Load())
			body, err := io.ReadAll(res.Body)
			must.OKf(t, err, "read the answer")
			if res.StatusCode != http.StatusTooManyRequests || !strings.Contains(string(body), "resource_exhausted") {
				t.Fatalf("an endless body: %s %q, want 429 and a resource_exhausted Connect error", res.Status, body)
			}
			if !res.Close {
				t.Errorf("an endless body: the answer keeps the connection open, want Connection: close")
			}
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
				t.Errorf("after the answer the connection gave %v, want EOF: the engine did not close it", err)
			}
			if n := sent.Load(); n <= 2*limit {
				t.Errorf("only %d bytes were sent before the answer; the body did not outrun twice the cap (%d)", n, 2*limit)
			}
		})
	}
	t.Run("over h2c", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		body := &endlessReader{}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost,
			baseURL+orkstackv1connect.OrkflowServiceValidateOrkflowProcedure, body)
		must.OK(t, err)
		request.Header.Set("Content-Type", "application/json")
		res, err := h2c.Do(request)
		must.OKf(t, err, "no answer to an endless body over h2c (%d bytes sent)", body.given.Load())
		answer, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if err != nil || res.ProtoMajor != 2 || res.StatusCode != http.StatusTooManyRequests ||
			!strings.Contains(string(answer), "resource_exhausted") {
			t.Fatalf("an endless body over h2c: HTTP/%d %s %q (%v), want HTTP/2 429 resource_exhausted",
				res.ProtoMajor, res.Status, answer, err)
		}
		if n := body.given.Load(); n <= 2*limit {
			t.Errorf("only %d bytes were sent before the answer; the body did not outrun twice the cap (%d)", n, 2*limit)
		}
	})
}
