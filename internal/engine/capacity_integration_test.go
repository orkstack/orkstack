//go:build integration

package engine_test

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
)

// An Orker that declares a capacity admits no task beyond it (docs/DEPLOY.md
// "Declaring an Orker's capacity"), and a submit refuses a task no Orker of
// its pool could hold. The Orker below declares 4 cpus with a per-task cap of
// 2: each poll reserves 2 cpus, and a task of 1.5 cpus is trimmed to 1.5 once
// it arrives, so two such tasks fit and a third never does (3 × 1.5 > 4).
// Every item records its own start and end from the host's uptime clock, read
// inside its container, so the overlap is measured from what really ran.

const capacityFanOutYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: __NAME__
version: 0.1.0
inputs:
  items: {type: array}
tasks:
  - id: item
    uses: __IMAGE__
    pool: __POOL__
    each: {over: "{{ .inputs.items }}", max_items: 40}
    resources: {cpu: "1.5", memory: "128Mi"}
    run_as_orker_user: true
    network: none
    timeout_seconds: 120
    command:
      - /bin/sh
      - -c
      - |
        set -e
        start=$(cut -d' ' -f1 /proc/uptime)
        sleep 1
        end=$(cut -d' ' -f1 /proc/uptime)
        printf '{"start":%s,"end":%s}' "$start" "$end" > /orkstack/output/values.json
`

const capacityTooLargeYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: __NAME__
version: 0.1.0
tasks:
  - id: train
    uses: __IMAGE__
    pool: __POOL__
    resources: {cpu: "3"}
    run_as_orker_user: true
    network: none
    timeout_seconds: 60
    command: ["/bin/sh", "-c", "printf '{}' > /orkstack/output/values.json"]
`

func TestIntegration_ACapacityAdmitsAFanOutWithinItAndSubmitRefusesARequestNoOrkerHolds(t *testing.T) {
	ctx := cordonContext(t, 10*time.Minute)
	pool := fmt.Sprintf("sized-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "capacity", pool)
	policy := orker.DefaultSecurityPolicy()
	policy.MaxCPU, policy.MaxMemory = "2", "1Gi"
	policy.DefaultCPU, policy.DefaultMemory = "1", "256Mi"
	o := d.orker("orker-cap-"+d.token, pool, pinOrkerConfig{policy: &policy, capacityCPU: "4", hostsNoPins: true})
	awaitPoolServedBy(t, ctx, engine.NewTemporalPoolProbe(d.temporal), pool, o.queue, 1)
	d.awaitFleet(orkersWhere("orker_id", o.id), time.Minute, "the Orker reporting its capacity",
		func(rows []*orkstackv1.Orker) bool { return len(rows) == 1 && rows[0].GetCapacity().GetCpu() == 4 })
	d.transcript.section("one Orker: capacity 4 cpu, per-task cap 2 cpu, 1Gi")

	// 1. A request above the pool's cap is refused at submit, by name, and
	// leaves no run behind.
	d.storeOrkflow("big-"+d.token, pool, capacityTooLargeYAML, 0)
	reader := engine.NewTemporalRunReader(d.temporal)
	before, err := reader.ListRuns(ctx, engine.RunQuery{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	_, refused := d.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "big-" + d.token, Namespace: d.orkNS}))
	d.transcript.line("a 3-cpu task: %v", refused)
	if connect.CodeOf(refused) != connect.CodeFailedPrecondition ||
		!strings.Contains(refused.Error(), "RESOURCES_UNSATISFIABLE") || !strings.Contains(refused.Error(), `task "train"`) {
		t.Fatalf("submit of a 3-cpu task onto a 2-cpu-cap pool = %v; want RESOURCES_UNSATISFIABLE naming train", refused)
	}
	after, err := reader.ListRuns(ctx, engine.RunQuery{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("the refused submit left %d runs behind", len(after)-len(before))
	}

	// 2. Forty items of 1.5 cpu run two at a time, never three.
	d.storeOrkflow("fan-"+d.token, pool, capacityFanOutYAML, 0)
	items := make([]any, 40)
	for i := range items {
		items[i] = i
	}
	data, err := structpb.NewStruct(map[string]any{"items": items})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := d.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "fan-" + d.token, Namespace: d.orkNS, Input: &orkstackv1.Payload{Data: data}}))
	if err != nil {
		t.Fatalf("SubmitRun(fan-out): %v", err)
	}
	runID := resp.Msg.GetRun().GetId()
	run := d.await(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("fan-out run ended %v (%v); want SUCCEEDED", run.GetStatus(), run.GetError())
	}
	type span struct{ start, end float64 }
	var spans []span
	o.mu.Lock()
	results := append([]*orkstackv1.ExecuteTaskResult(nil), o.every[runID]...)
	o.mu.Unlock()
	for _, result := range results {
		reader, err := d.store.Get(ctx, result.GetValuesRef().GetUri())
		if err != nil {
			t.Fatalf("read an item's values: %v", err)
		}
		body, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatalf("read an item's values: %v", err)
		}
		var values struct{ Start, End float64 }
		if err := json.Unmarshal(body, &values); err != nil {
			t.Fatalf("an item's values %q: %v", body, err)
		}
		spans = append(spans, span{values.Start, values.End})
	}
	if len(spans) != 40 {
		t.Fatalf("read %d item spans, want 40", len(spans))
	}
	// The most items running at once, from the containers' own clocks.
	type edge struct {
		at    float64
		delta int
	}
	var edges []edge
	for _, s := range spans {
		edges = append(edges, edge{s.start, +1}, edge{s.end, -1})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].at != edges[j].at {
			return edges[i].at < edges[j].at
		}
		return edges[i].delta < edges[j].delta // an end before a start at the same instant
	})
	running, most := 0, 0
	for _, e := range edges {
		running += e.delta
		most = max(most, running)
	}
	d.transcript.line("40 items of 1.5 cpu: at most %d ran at once (%.1f cpu of 4)", most, float64(most)*1.5)
	if most > 2 {
		t.Fatalf("%d items of 1.5 cpu ran at once on a 4-cpu capacity: %.1f cpu admitted", most, float64(most)*1.5)
	}
	// Positive control: the capacity is used, not just respected.
	if most < 2 {
		t.Fatalf("at most %d item ran at once; two fit in 4 cpu, so the capacity was never filled", most)
	}
	row := d.one(o.id)
	d.transcript.line("after the run: %s reserved of %s", row.GetReserved(), row.GetCapacity())
	if r := row.GetReserved(); r == nil || r.GetCpu() > 4 {
		t.Fatalf("after the run the Orker reports %v reserved of 4 cpu", r)
	}
	t.Logf("EVIDENCE capacity: refused %v; 40 items ran at most %d at once; at rest %v reserved",
		refused, most, row.GetReserved())
}
