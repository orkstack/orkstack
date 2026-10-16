//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// overlayFanOut declares items fan-out items: items+1 declared rows.
func overlayFanOut(name, version string, items int) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "%s"
inputs:
  parts: {type: array}
tasks:
  - id: fetch
    uses: http
    each: {over: "{{ .inputs.parts }}", max_items: %d}
    with:
      op: get
      url: "https://example.test/json"
`, name, version, items)
}

// startHistory is a run's history until it holds a marker satisfying done,
// or its first ten seconds of events when done never holds.
func startHistory(t *testing.T, ctx context.Context, c client.Client, runID string, done func(*historypb.History) bool) *historypb.History {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		h := &historypb.History{}
		iter := c.GetWorkflowHistory(ctx, runID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				break
			}
			h.Events = append(h.Events, event)
		}
		if done(h) || time.Now().After(deadline) {
			return h
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}

// markersOf is every marker of h: Version markers by change id, local
// activity markers by activity type.
func markersOf(t *testing.T, h *historypb.History) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, event := range h.GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil {
			continue
		}
		switch attrs.GetMarkerName() {
		case "Version":
			var change string
			_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["change-id"], &change)
			out[change]++
		case "LocalActivity":
			var data struct{ ActivityType string }
			_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data)
			out[data.ActivityType]++
		}
	}
	return out
}

// TestIntegration_EngineWritesSnapshotOverlaysWhenConfigured: the real ork
// binary, configured by a file with engine.snapshot_overlays on (the
// configuration schema's key, not the environment), logs the effective
// value, advertises snapshot-overlay on its engine and projection channels,
// pins the default overlay policy on a 301-row run's plan — whose first
// snapshot is then written by WriteSnapshotOrOverlay behind the
// snapshot-overlay gate — and leaves it off a 3-row run's.
func TestIntegration_EngineWritesSnapshotOverlaysWhenConfigured(t *testing.T) {
	temporal := requireTemporal(t)
	_ = requireMinIO(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	ns := fmt.Sprintf("%s-ovl-%d-%d", base, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	configFile := filepath.Join(t.TempDir(), "ork.yaml")
	if err := os.WriteFile(configFile, []byte("engine:\n  snapshot_overlays: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	address, orkflows, log := bootEngine(ctx, t, buildOrk(t), temporal, "engine-overlays",
		"ORK_TEMPORAL_NAMESPACE="+ns, "ORK_CONFIG_FILE="+configFile)
	if !strings.Contains(log.String(), "snapshot overlays") || !strings.Contains(log.String(), "snapshot_overlays=true") {
		t.Fatalf("the boot log does not report snapshot_overlays=true:\n%s", log.String())
	}
	pid := engines["engine-overlays"].cmd.Process.Pid
	control, err := client.DialContext(ctx, client.Options{HostPort: temporal, Namespace: ns, Identity: "overlay-control"})
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	ours := func(p *taskqueuepb.PollerInfo) bool {
		return strings.HasPrefix(p.GetIdentity(), fmt.Sprint(pid)+"@") &&
			strings.Contains(p.GetIdentity(), engine.CapabilitySnapshotOverlay)
	}
	waitForPoller(t, ctx, control, ns, task.DefaultQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, time.Minute, ours)
	waitForPoller(t, ctx, control, ns, engine.EventQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, time.Minute, ours)

	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(transport.CloseIdleConnections)
	runs := orkstackv1connect.NewRunServiceClient(&http.Client{Transport: transport}, "http://"+address)
	version := probeOrkflowVersion()
	submit := func(name string, items int) (string, *historypb.History) {
		t.Helper()
		if _, err := orkflows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: overlayFanOut(name, version, items)})); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
		data, err := structpb.NewStruct(map[string]any{"parts": []any{"a", "b"}})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: name, OrkflowVersion: version, Input: &orkstackv1.Payload{Data: data}, AllowEmptyPool: true}))
		if err != nil {
			t.Fatalf("submit %s: %v", name, err)
		}
		runID := resp.Msg.GetRun().GetId()
		t.Cleanup(func() { _ = control.TerminateWorkflow(context.Background(), runID, "", "test cleanup") })
		return runID, startHistory(t, ctx, control, runID, func(h *historypb.History) bool {
			m := markersOf(t, h)
			return m[engine.ActivityWriteSnapshotOrOverlay] > 0 || m[engine.ActivityWriteSnapshot] > 0
		})
	}
	planOf := func(h *historypb.History) string {
		return string(h.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()[3].GetData())
	}

	wideID, wide := submit("overlay-wide", 300)
	if plan := planOf(wide); !strings.Contains(plan, `"snapshot_overlay":{"min_rows":256,"max_chain":64,"max_rows":256}`) {
		t.Fatalf("the 301-row run's plan carries no default overlay policy: %s", plan)
	}
	if m := markersOf(t, wide); m[engine.SnapshotOverlayChange] != 1 || m[engine.ActivityWriteSnapshotOrOverlay] == 0 {
		t.Fatalf("the 301-row run's history markers %v: want the gate and WriteSnapshotOrOverlay", m)
	}
	smallID, small := submit("overlay-small", 2)
	if plan := planOf(small); strings.Contains(plan, "snapshot_overlay") {
		t.Fatalf("the 3-row run's plan carries an overlay policy: %s", plan)
	}
	if m := markersOf(t, small); m[engine.SnapshotOverlayChange] != 0 || m[engine.ActivityWriteSnapshotOrOverlay] != 0 ||
		m[engine.ActivityWriteSnapshot] == 0 {
		t.Fatalf("the 3-row run's history markers %v: want WriteSnapshot alone", m)
	}
	proof.Write(t, "ork", "snapshot-overlays.txt", []byte(fmt.Sprintf(
		"engine.snapshot_overlays: true (config file)\nrun %s (301 rows): plan %s\n  markers %v\nrun %s (3 rows): markers %v\n",
		wideID, planOf(wide), markersOf(t, wide), smallID, markersOf(t, small))))
}
