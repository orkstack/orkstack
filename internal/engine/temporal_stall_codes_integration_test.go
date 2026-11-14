//go:build integration

package engine_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestIntegration_TemporalStall_ClientFailureMetricAndCodes is a Temporal
// outage seen from the engine's API (docs/DEPLOY.md, "Idempotency keys": a
// start whose answer was lost). The engine's own client reaches a private cluster through a
// fault proxy and dials with the options a deployment's engine dials with,
// Prometheus on its admin listener. For each kind of outage, a frontend that
// refuses connections and one that stays silent:
//
//   - SubmitRun answers Unavailable, never Internal, and says to submit again;
//   - nothing is recorded under the run id: a start whose answer was lost may
//     still land, so it is never recorded as RUN_START_FAILED;
//   - the SDK's own temporal_request_failure_total grows meanwhile, which is
//     both what TemporalUnreachable reads and this window's evidence that the
//     engine was asking the cluster all along;
//   - once the frontend is back, a new submission starts.
//
// The control comes first: with the proxy forwarding, a submission starts and
// temporal_request_total counts it, so the counters read here are the ones
// the client really exports.
func TestIntegration_TemporalStall_ClientFailureMetricAndCodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cfg := s3Config()
	requireObjectStore(t, cfg)

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

	// The engine's telemetry as a deployment turns it on: the Prometheus
	// exporter, scraped from the admin listener.
	telemetryConfig := telemetry.Config{
		MetricsExporters: []string{telemetry.ExporterPrometheus},
		AdminAddr:        "127.0.0.1:0",
	}
	providers, err := telemetry.Setup(ctx, telemetryConfig)
	must.OKf(t, err, "set up telemetry")
	t.Cleanup(func() { _ = providers.Shutdown(context.Background()) })
	adminAddr, stopAdmin, err := telemetry.StartAdmin(telemetryConfig, providers, slog.Default())
	must.OKf(t, err, "start the admin listener")
	t.Cleanup(func() { _ = stopAdmin(context.Background()) })

	proxy := faultproxy.New(t, address)
	options, err := telemetry.ClientOptions(proxy.Addr(), namespace, providers, slog.Default())
	must.OKf(t, err, "the engine's client options")
	engineClient, err := client.DialContext(ctx, options)
	must.OKf(t, err, "dial temporal through the proxy")
	t.Cleanup(engineClient.Close)

	archiveStore, err := artifacts.NewRunArchive(cfg)
	must.OKf(t, err, "open the run archive")
	archive := engine.NewRunArchiver(archiveStore)

	token := time.Now().UnixNano()
	var minted atomic.Int64
	newRunID := func() string { return fmt.Sprintf("ork-codes-%d-%d", token, minted.Add(1)) }
	svc := api.NewService(api.Config{
		Starter: engine.NewTemporalStarterWithOrkerQueue(engineClient,
			fmt.Sprintf("ork-codes-run-%d", token), fmt.Sprintf("ork-codes-tasks-%d", token)),
		Reader:   engine.NewTemporalRunReader(engineClient),
		Archive:  archive,
		RawQueue: fmt.Sprintf("ork-codes-tasks-%d", token),
		NewRunID: newRunID,
	})
	name := fmt.Sprintf("temporal-codes-%d", token)
	_, err = svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: "apiVersion: orkstack/v1\n" +
		"kind: Orkflow\nname: " + name + "\nversion: \"1\"\ntasks:\n  - id: work\n    uses: shell\n" +
		"    with:\n      command: \"true\"\n"}))
	must.OKf(t, err, "apply the orkflow")
	submit := func(within time.Duration) (*orkstackv1.SubmitRunResponse, error) {
		callCtx, callCancel := context.WithTimeout(ctx, within)
		defer callCancel()
		resp, err := svc.SubmitRun(callCtx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	scrape := func() clientCounters { return scrapeClientCounters(t, ctx, adminAddr) }

	// Control: the cluster answers, and the client's counters count it.
	before := scrape()
	started, err := submit(time.Minute)
	must.OKf(t, err, "a submission while the cluster answers")
	if started.GetRun().GetId() == "" {
		t.Fatal("a submission while the cluster answers returned no run")
	}
	if after := scrape(); after.requests <= before.requests {
		t.Fatalf("temporal_request_total did not grow with a start (%v -> %v): the client exports no counters",
			before.requests, after.requests)
	}

	for _, outage := range []struct {
		name  string
		begin func(*faultproxy.Proxy)
	}{
		{"refusing", (*faultproxy.Proxy).Cut},
		{"silent", (*faultproxy.Proxy).Stall},
	} {
		t.Run(outage.name, func(t *testing.T) {
			before := scrape()
			outage.begin(proxy)
			// The caller waits longer than the engine's own bound on a start,
			// so the answer is the cluster's failure, not the caller leaving.
			_, err := submit(2 * time.Minute)
			if connect.CodeOf(err) != connect.CodeUnavailable {
				t.Fatalf("a submission while the cluster is %s = %v (%s), want Unavailable",
					outage.name, err, connect.CodeOf(err))
			}
			if !strings.Contains(err.Error(), "submit again") {
				t.Errorf("the answer %q does not ask the caller to submit again", err)
			}
			refused := fmt.Sprintf("ork-codes-%d-%d", token, minted.Load())
			if _, held := svc.Runs().Get(refused); held {
				t.Errorf("the engine holds %s after a start whose answer was lost", refused)
			}
			if run, err := archive.LoadRun(ctx, refused); !errors.Is(err, artifacts.ErrNotFound) {
				t.Errorf("archive LoadRun(%s) = %v, %v; want nothing recorded, never RUN_START_FAILED",
					refused, run.GetError(), err)
			}
			// The window's progress: the engine asked the cluster all along,
			// and the client counted what it was refused.
			during := scrape()
			if during.failures <= before.failures {
				t.Fatalf("temporal_request_failure_total did not grow during the outage (%v -> %v)",
					before.failures, during.failures)
			}
			t.Logf("temporal_request_failure_total by status_code during the outage: %v (proxy saw %+v)",
				during.byStatus, proxy.Stats())

			// Recovery: the frontend is back, and a new submission starts.
			proxy.Restore()
			deadline := time.Now().Add(3 * time.Minute)
			for {
				resp, err := submit(time.Minute)
				if err == nil && resp.GetRun().GetId() != "" {
					t.Logf("recovered: %s started", resp.GetRun().GetId())
					break
				}
				if code := connect.CodeOf(err); code != connect.CodeUnavailable {
					t.Fatalf("a submission after the outage = %v (%s), want Unavailable until it starts", err, code)
				}
				if time.Now().After(deadline) {
					t.Fatalf("no submission started within 3m of the frontend coming back: %v", err)
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(250 * time.Millisecond):
				}
			}
		})
	}
}

// clientCounters are the Temporal SDK's own request counters, as the engine's
// admin listener exports them.
type clientCounters struct {
	requests float64
	failures float64
	byStatus map[string]float64
}

// scrapeClientCounters reads temporal_request_total and
// temporal_request_failure_total, summed over their labels, and the failures
// by status_code.
func scrapeClientCounters(t *testing.T, ctx context.Context, adminAddr string) clientCounters {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+adminAddr+telemetry.MetricsPath, nil)
	must.OK(t, err)
	resp, err := http.DefaultClient.Do(req)
	must.OKf(t, err, "scrape %s", telemetry.MetricsPath)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape %s = %d", telemetry.MetricsPath, resp.StatusCode)
	}
	out := clientCounters{byStatus: map[string]float64{}}
	lines := bufio.NewScanner(resp.Body)
	lines.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for lines.Scan() {
		line := lines.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		series, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		metric, labels, _ := strings.Cut(series, "{")
		v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			continue
		}
		switch metric {
		case "temporal_request_total":
			out.requests += v
		case "temporal_request_failure_total":
			out.failures += v
			out.byStatus[labelValue(labels, "status_code")] += v
		}
	}
	must.OKf(t, lines.Err(), "read %s", telemetry.MetricsPath)
	return out
}

// labelValue is one label's value in a Prometheus series' label set.
func labelValue(labels, name string) string {
	for _, pair := range strings.Split(strings.TrimSuffix(labels, "}"), ",") {
		key, value, ok := strings.Cut(pair, "=")
		if ok && key == name {
			return strings.Trim(value, `"`)
		}
	}
	return ""
}
