//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
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
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// This file is orker-cordon-drain's engine acceptance, against real
// infrastructure: real Temporal (a private namespace per test), real MinIO,
// real Docker containers, and in-process Orkers built by newPinOrker exactly
// as cmd/orker builds one — the cordon's Tuner and interceptor on the task
// worker, and the control and presence workers beside it.
//
// Every assertion about WHERE a task ran reads the engine's own TaskRun and
// the Orker's own record of what it was handed; every assertion about the
// fleet reads ListOrkers through the real API, which reads the cluster's
// poller records and each Orker's own answer. Nothing below is stubbed.

// cordonDeployment is one engine plus the Orkers a test adds to it.
type cordonDeployment struct {
	t         *testing.T
	ctx       context.Context
	token     string
	temporal  client.Client
	namespace string // Temporal namespace
	orkNS     string // OrkStack namespace document
	image     string

	cfg         artifacts.Config
	store       artifacts.Store
	commits     artifacts.CommitStore
	logs        artifacts.LogStore
	refs        artifacts.RefPredictor
	orkflows    engine.OrkflowStore
	starter     *engine.TemporalStarter
	runs        orkstackv1connect.RunServiceClient
	activities  orkstackv1connect.ActivityServiceClient
	namespaces  orkstackv1connect.NamespaceServiceClient
	pools       []string
	transcript  *poolTranscript
	transcriptN string
}

// syncBuffer is a goroutine-safe log sink, so a test can read an Orker's own
// log lines (the cordon's refusals) without a race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newCordonDeployment brings up a private namespace, the engine's workers and
// the real API with the Orker fleet wired, and registers teardown. pools are
// the pools the OrkStack namespace document allows.
func newCordonDeployment(t *testing.T, ctx context.Context, name string, pools ...string) *cordonDeployment {
	t.Helper()
	cfg := s3Config()
	requireMinIO(t, cfg)
	requirePinAffinityDocker(t)

	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	d := &cordonDeployment{
		t: t, ctx: ctx, token: token, cfg: cfg,
		namespace:   fmt.Sprintf("ork-cordon-%s-%s", name, token),
		orkNS:       "cordon-" + name + "-" + token,
		pools:       pools,
		transcript:  &poolTranscript{},
		transcriptN: "cordon-" + name + "-transcript.txt",
	}
	d.temporal = dialVersioningCluster(t, ctx, d.namespace)
	awaitNamespaceReadyForCommitE2E(t, ctx, d.temporal, d.namespace)
	t.Cleanup(func() { d.writeTranscript(t) })

	namespaceStore, err := artifacts.NewS3NamespaceStore(ctx, s3NamespaceConfig(cfg))
	if err != nil {
		infraAbsent(t, "cannot open the namespace registry: %v", err)
	}
	allowed := append([]string{task.DefaultPool}, pools...)
	if _, err := namespaceStore.Put(ctx, &orkstackv1.Namespace{
		Name: d.orkNS, Description: "orker cordon evidence", DefaultPool: pools[0], AllowedPools: allowed,
	}, artifacts.PutConditions{}); err != nil {
		infraAbsent(t, "cannot write the namespace document (is MinIO up?): %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = namespaceStore.Delete(cleanupCtx, d.orkNS)
	})

	d.store, err = artifacts.New(cfg)
	if err != nil {
		t.Fatalf("open artifact store: %v", err)
	}
	d.commits, err = artifacts.NewCommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("open commit store: %v", err)
	}
	if err := artifacts.ProbeCommitStore(ctx, d.commits); err != nil {
		infraAbsent(t, "the object store's conditional create is not usable: %v", err)
	}
	d.logs, err = artifacts.NewLogStore(cfg)
	if err != nil {
		t.Fatalf("open log store: %v", err)
	}
	refs, ok := d.store.(artifacts.RefPredictor)
	if !ok {
		t.Fatalf("artifact backend %T cannot predict content-addressed references", d.store)
	}
	d.refs = refs

	bus, runs := engine.NewEventBus(), engine.NewRunStore()
	snapshots := newTestSnapshotStore()
	runWorker := worker.New(d.temporal, task.DefaultQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
		Identity:                    "cordon-engine-" + token,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	eventWorker := worker.New(d.temporal, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
		Identity:                    "cordon-events-" + token,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(bus, runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for label, w := range map[string]worker.Worker{"run": runWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", label, err)
		}
		t.Cleanup(w.Stop)
	}

	d.orkflows = engine.NewMemoryOrkflowStore()
	d.starter = engine.NewTemporalStarter(d.temporal, task.DefaultQueue)
	svc := api.NewService(api.Config{
		Orkflows:      d.orkflows,
		Namespaces:    namespaceStore,
		Reader:        engine.NewTemporalRunReader(d.temporal).WithSnapshotStore(snapshots),
		Runs:          runs,
		Bus:           bus,
		Starter:       d.starter,
		Pools:         engine.NewTemporalPoolProbe(d.temporal),
		PoolOrkers:    d.starter,
		Orkers:        d.starter,
		Fleet:         d.starter,
		ArtifactStore: d.store,
		Logs:          d.logs,
		Artifacts:     cfg,
		Limits:        engine.DefaultLimits(),
	})
	d.serve(t, svc)
	d.image = pinnedAlpineForCommitE2E(t)
	return d
}

// serve mounts one API service on a loopback listener and points the clients
// at it.
func (d *cordonDeployment) serve(t *testing.T, svc *api.Service) {
	t.Helper()
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	})
	httpClient := &http.Client{Transport: transport}
	base := "http://" + listener.Addr().String()
	d.runs = orkstackv1connect.NewRunServiceClient(httpClient, base)
	d.activities = orkstackv1connect.NewActivityServiceClient(httpClient, base)
	d.namespaces = orkstackv1connect.NewNamespaceServiceClient(httpClient, base)
}

func (d *cordonDeployment) writeTranscript(t *testing.T) {
	d.transcript.mu.Lock()
	defer d.transcript.mu.Unlock()
	header := []string{
		"# orker cordon and drain: " + t.Name(),
		"# Every Orker below is a real internal/orker.Activities with a real Docker runtime,",
		"# built with the cordon's Tuner, interceptor, control and presence workers exactly",
		"# as cmd/orker builds them. Where a task ran is the engine's TaskRun.orker_id and the",
		"# Orker's own record; the fleet is ListOrkers through the real API.",
		"# temporal namespace " + d.namespace,
		"# recorded " + time.Now().UTC().Format(time.RFC3339),
	}
	proof.Write(t, "engine", d.transcriptN, []byte(strings.Join(append(header, d.transcript.lines...), "\n")+"\n"))
}

// orker starts one Orker of the deployment on pool.
func (d *cordonDeployment) orker(id, pool string, opts pinOrkerConfig) *pinOrker {
	d.t.Helper()
	opts.id, opts.pool = id, pool
	opts.queue = mustQueueForPool(d.t, pool)
	opts.store, opts.commits, opts.logs, opts.refs = d.store, d.commits, d.logs, d.refs
	opts.temporalNamespace = d.namespace
	return newPinOrker(d.t, d.ctx, opts)
}

// list reads the fleet through the API.
func (d *cordonDeployment) list(req *orkstackv1.ListOrkersRequest) []*orkstackv1.Orker {
	d.t.Helper()
	resp, err := d.activities.ListOrkers(d.ctx, connect.NewRequest(req))
	if err != nil {
		d.t.Fatalf("ListOrkers(%v): %v", req, err)
	}
	return resp.Msg.GetOrkers()
}

// one reads one Orker's row, or nil when nothing polls its channel.
func (d *cordonDeployment) one(id string) *orkstackv1.Orker {
	d.t.Helper()
	rows := d.list(&orkstackv1.ListOrkersRequest{OrkerId: id})
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

// awaitFleet polls ListOrkers until want(rows) holds, against a deadline.
func (d *cordonDeployment) awaitFleet(
	req *orkstackv1.ListOrkersRequest, within time.Duration, what string, want func([]*orkstackv1.Orker) bool,
) []*orkstackv1.Orker {
	d.t.Helper()
	deadline := time.Now().Add(within)
	var rows []*orkstackv1.Orker
	for time.Now().Before(deadline) {
		rows = d.list(req)
		if want(rows) {
			return rows
		}
		select {
		case <-d.ctx.Done():
			d.t.Fatalf("context ended waiting for %s: %v", what, d.ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	d.t.Fatalf("%s never held within %v; last ListOrkers: %s", what, within, describeRows(rows))
	return nil
}

func (d *cordonDeployment) cordon(id, reason string) *orkstackv1.Orker {
	d.t.Helper()
	resp, err := d.activities.CordonOrker(d.ctx, connect.NewRequest(&orkstackv1.CordonOrkerRequest{
		OrkerId: id, Cordoned: true, Reason: reason}))
	if err != nil {
		d.t.Fatalf("CordonOrker(%s): %v", id, err)
	}
	return resp.Msg.GetOrker()
}

func (d *cordonDeployment) uncordon(id string) *orkstackv1.Orker {
	d.t.Helper()
	resp, err := d.activities.CordonOrker(d.ctx, connect.NewRequest(&orkstackv1.CordonOrkerRequest{OrkerId: id}))
	if err != nil {
		d.t.Fatalf("uncordon %s: %v", id, err)
	}
	return resp.Msg.GetOrker()
}

// store compiles a template (placeholders __NAME__, __IMAGE__, __POOL__,
// __HOLD__) into the orkflow store.
func (d *cordonDeployment) storeOrkflow(name, pool, template string, hold int) {
	d.t.Helper()
	dep := &pinDeployment{pool: pool, namespace: d.orkNS, image: d.image, orkflows: d.orkflows}
	source := strings.NewReplacer("__HOLD_SECONDS__", fmt.Sprint(hold)).Replace(template)
	if err := dep.storeOrkflow(d.ctx, name, source); err != nil {
		d.t.Fatalf("store %s: %v", name, err)
	}
}

func (d *cordonDeployment) submit(orkflow string) string {
	d.t.Helper()
	resp, err := d.runs.SubmitRun(d.ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: orkflow, Namespace: d.orkNS}))
	if err != nil {
		d.t.Fatalf("SubmitRun(%s): %v", orkflow, err)
	}
	return resp.Msg.GetRun().GetId()
}

func (d *cordonDeployment) await(runID string) *orkstackv1.Run {
	d.t.Helper()
	dep := &pinDeployment{runs: d.runs, namespace: d.orkNS}
	return dep.awaitTerminalRun(d.t, d.ctx, runID)
}

func describeRows(rows []*orkstackv1.Orker) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		state := "ready"
		switch {
		case r.GetUnavailableReason() != "":
			state = "unreachable(" + r.GetUnavailableReason() + ")"
		case r.GetCordoned():
			state = fmt.Sprintf("cordoned(%q)", r.GetCordonReason())
		}
		parts = append(parts, fmt.Sprintf("%s pool=%s build=%s %s tasks=%d pins=%d/%d",
			r.GetOrkerId(), r.GetPool(), r.GetBuildId(), state, r.GetRunningTasks(), r.GetOpenPins(), r.GetMaxConcurrentPins()))
	}
	return "[" + strings.Join(parts, "; ") + "]"
}

func rowsByID(rows []*orkstackv1.Orker) map[string]*orkstackv1.Orker {
	out := map[string]*orkstackv1.Orker{}
	for _, r := range rows {
		out[r.GetOrkerId()] = r
	}
	return out
}

// cordonEchoYAML is one unpinned task: a real container that writes its
// values and exits.
const cordonEchoYAML = `
name: __NAME__
version: 0.1.0
tasks:
  - id: echo
    uses: __IMAGE__
    pool: __POOL__
    run_as_orker_user: true
    network: none
    timeout_seconds: 120
    command: ["/bin/sh", "-c", "printf '{\"ok\":true}' > /orkstack/output/values.json"]
`

// cordonHoldPinYAML is a pin whose first member holds for __HOLD_SECONDS__
// before its second member runs — the pin that is open while its host is
// cordoned.
const cordonHoldPinYAML = `
name: __NAME__
version: 0.1.0
pins:
  build:
    timeout_seconds: 600
tasks:
  - id: hold
    uses: __IMAGE__
    pool: __POOL__
    pin: build
    run_as_orker_user: true
    network: none
    timeout_seconds: 300
    command:
      - /bin/sh
      - -c
      - |
        set -e
        printf 'held' > /orkstack/shared/token.txt
        printf '{"held":true}' > /orkstack/output/values.json
        sleep __HOLD_SECONDS__
  - id: after
    uses: __IMAGE__
    pool: __POOL__
    pin: build
    needs: [hold]
    run_as_orker_user: true
    network: none
    timeout_seconds: 120
    command:
      - /bin/sh
      - -c
      - |
        set -e
        printf '{"seen":"%s"}' "$(cat /orkstack/shared/token.txt)" > /orkstack/output/values.json
`

// cordonQuickPinYAML is a pin of two quick members.
const cordonQuickPinYAML = `
name: __NAME__
version: 0.1.0
pins:
  build:
    timeout_seconds: 600
tasks:
  - id: first
    uses: __IMAGE__
    pool: __POOL__
    pin: build
    run_as_orker_user: true
    network: none
    timeout_seconds: 120
    command: ["/bin/sh", "-c", "printf 'x' > /orkstack/shared/t && printf '{}' > /orkstack/output/values.json"]
  - id: second
    uses: __IMAGE__
    pool: __POOL__
    pin: build
    needs: [first]
    run_as_orker_user: true
    network: none
    timeout_seconds: 120
    command: ["/bin/sh", "-c", "test -f /orkstack/shared/t && printf '{}' > /orkstack/output/values.json"]
`

func cordonContext(t *testing.T, within time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), within)
	t.Cleanup(cancel)
	return ctx
}

func TestIntegration_ListOrkersListsEveryLiveOrkerWithItsLoad(t *testing.T) {
	ctx := cordonContext(t, 6*time.Minute)
	token := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	build, gpu := "build-"+token, "gpu-"+token
	d := newCordonDeployment(t, ctx, "list", build, gpu)
	d.transcript.section("four Orkers, two pools")

	var build3 []*pinOrker
	for i := 1; i <= 3; i++ {
		build3 = append(build3, d.orker(fmt.Sprintf("orker-b%d-%s", i, d.token), build, pinOrkerConfig{}))
	}
	g := d.orker("orker-g1-"+d.token, gpu, pinOrkerConfig{})

	rows := d.awaitFleet(&orkstackv1.ListOrkersRequest{}, time.Minute, "four Orkers listed", func(rows []*orkstackv1.Orker) bool {
		return len(rows) == 4
	})
	d.transcript.line("ListOrkers           %s", describeRows(rows))
	byID := rowsByID(rows)
	now := time.Now()
	for _, o := range append(build3, g) {
		row := byID[o.id]
		if row == nil || row.GetUnavailableReason() != "" || row.GetPool() != o.pool || row.GetRunningTasks() != 0 ||
			row.GetCordoned() || row.GetMaxConcurrentPins() != 2 {
			t.Fatalf("row for %s = %v", o.id, row)
		}
		if seen := row.GetLastSeen().AsTime(); now.Sub(seen) > engine.DefaultPollerFreshness {
			t.Fatalf("%s last seen %v, outside the freshness window", o.id, seen)
		}
	}
	// Ordered by pool, then id.
	for i := 1; i < len(rows); i++ {
		a, b := rows[i-1], rows[i]
		if a.GetPool() > b.GetPool() || (a.GetPool() == b.GetPool() && a.GetOrkerId() > b.GetOrkerId()) {
			t.Fatalf("rows are not ordered by pool then id: %s", describeRows(rows))
		}
	}
	filtered := d.list(&orkstackv1.ListOrkersRequest{Pool: gpu})
	if len(filtered) != 1 || filtered[0].GetOrkerId() != g.id {
		t.Fatalf("--pool %s = %s; want only %s", gpu, describeRows(filtered), g.id)
	}
	d.transcript.line("ListOrkers pool=%s  %s", gpu, describeRows(filtered))
	if got := d.list(&orkstackv1.ListOrkersRequest{Pool: build}); len(got) != 3 {
		t.Fatalf("--pool %s = %s; want three", build, describeRows(got))
	}

	stopped := build3[0]
	stopped.stop()
	started := time.Now()
	d.awaitFleet(&orkstackv1.ListOrkersRequest{}, engine.DefaultPollerFreshness+10*time.Second+5*time.Second,
		"the stopped Orker leaving the list", func(rows []*orkstackv1.Orker) bool {
			return len(rows) == 3 && rowsByID(rows)[stopped.id] == nil
		})
	d.transcript.line("stopped %s; it left the list after %s", stopped.id, time.Since(started).Round(time.Second))
	t.Logf("EVIDENCE list: four Orkers listed with their pools; %s left the list %s after it stopped",
		stopped.id, time.Since(started).Round(time.Second))
}

func TestIntegration_ACordonedOrkerTakesNoNewTaskWhileItsSiblingDoes(t *testing.T) {
	ctx := cordonContext(t, 12*time.Minute)
	pool := fmt.Sprintf("cordon-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "intake", pool)
	a := d.orker("orker-a-"+d.token, pool, pinOrkerConfig{})
	b := d.orker("orker-b-"+d.token, pool, pinOrkerConfig{})
	awaitPoolServedBy(t, ctx, engine.NewTemporalPoolProbe(d.temporal), pool, a.queue, 2)
	d.storeOrkflow("echo-"+d.token, pool, cordonEchoYAML, 0)

	const reason = "CVE-2026-1234 runtime patch"
	row := d.cordon(a.id, reason)
	if !row.GetCordoned() || row.GetCordonReason() != reason {
		t.Fatalf("cordon answered %v", row)
	}
	d.transcript.section("cordon A, then 20 single-task runs")
	d.transcript.line("A %s cordoned: %q", a.id, reason)

	perOrker := map[string]int{}
	var onA []int
	for i := 1; i <= 20; i++ {
		runID := d.submit("echo-" + d.token)
		run := d.await(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %d %s ended %v: %v", i, runID, run.GetStatus(), run.GetError())
		}
		ran := pinTaskRun(t, run, "echo").GetOrkerId()
		perOrker[ran]++
		if ran == a.id {
			onA = append(onA, i)
		}
		d.transcript.line("run %2d %s -> %s", i, runID, ran)
		if cur := d.one(a.id); cur == nil || !cur.GetCordoned() || cur.GetCordonReason() != reason {
			t.Fatalf("after run %d A reads %v; want cordoned with the reason throughout", i, cur)
		}
	}
	line := fmt.Sprintf("tasks per Orker after the cordon: A: %d, B: %d", perOrker[a.id], perOrker[b.id])
	d.transcript.line("%s", line)
	t.Logf("EVIDENCE cordon intake: %s (A's, if any, were runs %v)", line, onA)
	if perOrker[a.id] > 2 || perOrker[a.id]+perOrker[b.id] != 20 {
		t.Fatalf("%s; at most two may land on the cordoned Orker (the SDK's long-poll window)", line)
	}
	for _, i := range onA {
		if i > 2 {
			t.Fatalf("run %d landed on the cordoned Orker; only the first two after the cordon may", i)
		}
	}

	d.uncordon(a.id)
	d.transcript.section("uncordon A, then runs until one lands on A")
	landed := 0
	for i := 1; i <= 10 && landed == 0; i++ {
		runID := d.submit("echo-" + d.token)
		run := d.await(runID)
		ran := pinTaskRun(t, run, "echo").GetOrkerId()
		d.transcript.line("run %2d %s -> %s", i, runID, ran)
		if ran == a.id {
			landed = i
		}
	}
	if landed == 0 {
		t.Fatalf("no run of ten landed on %s after it was uncordoned", a.id)
	}
	t.Logf("EVIDENCE uncordon: run %d after the uncordon landed on A", landed)
}

func TestIntegration_ACordonedPinHostOpensNoNewPinAndKeepsItsOpenOne(t *testing.T) {
	ctx := cordonContext(t, 12*time.Minute)
	pool := fmt.Sprintf("pincordon-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "pin", pool)
	logsA := &syncBuffer{}
	// maxPins 2 on A, not 1: a host at its cap does not poll the creation
	// channel at all (the SDK's session token), so a second pin could never
	// even reach A's interceptor and the refusal this test pins could not be
	// observed. See implementation-notes.md.
	a := d.orker("orker-pa-"+d.token, pool, pinOrkerConfig{
		maxPins: 2, logger: slog.New(slog.NewTextHandler(logsA, nil))})
	probe := engine.NewTemporalPoolProbe(d.temporal)
	awaitPoolServedBy(t, ctx, probe, pool, a.queue, 1)
	awaitPoolServedBy(t, ctx, probe, pool+" (session creation)", a.queue+"__internal_session_creation", 1)
	d.storeOrkflow("hold-"+d.token, pool, cordonHoldPinYAML, 60)
	d.storeOrkflow("quick-"+d.token, pool, cordonQuickPinYAML, 0)

	d.transcript.section("a pin opens on A; A is cordoned")
	run1 := d.submit("hold-" + d.token)
	d.transcript.line("run 1 %s submitted (pin build: hold 60s, then after)", run1)
	rows := d.awaitFleet(&orkstackv1.ListOrkersRequest{OrkerId: a.id}, 3*time.Minute,
		"A reporting run 1's open pin with its name and lease", func(rows []*orkstackv1.Orker) bool {
			if len(rows) != 1 || rows[0].GetOpenPins() != 1 || len(rows[0].GetPins()) != 1 {
				return false
			}
			pin := rows[0].GetPins()[0]
			return pin.GetRunId() == run1 && pin.GetPin() == "build" && pin.GetExpiresAt() != nil
		})
	d.transcript.line("ListOrkers A          %s pins=%v", describeRows(rows), rows[0].GetPins())
	const reason = "drain for kernel patch"
	d.cordon(a.id, reason)
	d.transcript.line("A %s cordoned: %q", a.id, reason)

	run2 := d.submit("quick-" + d.token)
	d.transcript.line("run 2 %s submitted while A is the only pin host, and cordoned", run2)
	refusalDeadline := time.Now().Add(time.Minute)
	for !strings.Contains(logsA.String(), "pin creation refused") {
		if time.Now().After(refusalDeadline) {
			t.Fatalf("A never refused run 2's pin creation — is %q still the SDK's session creation "+
				"activity name? A's log:\n%s", "internalSessionCreationActivity", logsA.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, l := range strings.Split(logsA.String(), "\n") {
		if strings.Contains(l, "pin creation refused") {
			d.transcript.line("A's log               %s", l)
			t.Logf("EVIDENCE A refused: %s", l)
			break
		}
	}

	b := d.orker("orker-pb-"+d.token, pool, pinOrkerConfig{maxPins: 1})
	d.transcript.line("B %s started", b.id)
	second := d.await(run2)
	if second.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run 2 ended %v: %v", second.GetStatus(), second.GetError())
	}
	for _, member := range []string{"first", "second"} {
		if got := pinTaskRun(t, second, member).GetOrkerId(); got != b.id {
			t.Fatalf("run 2's %s ran on %s; a new pin must open on the uncordoned host %s", member, got, b.id)
		}
	}
	if len(a.ranFor(run2)) != 0 {
		t.Fatalf("the cordoned host was handed run 2's members %v", a.ranFor(run2))
	}
	d.transcript.line("run 2 %s SUCCEEDED; first -> %s, second -> %s", run2,
		pinTaskRun(t, second, "first").GetOrkerId(), pinTaskRun(t, second, "second").GetOrkerId())

	first := d.await(run1)
	if first.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run 1 ended %v: %v", first.GetStatus(), first.GetError())
	}
	after := pinTaskRun(t, first, "after")
	if after.GetOrkerId() != a.id || !a.sawTask(run1, "after") {
		t.Fatalf("run 1's post-cordon member ran on %s; an open pin must keep executing on its host %s",
			after.GetOrkerId(), a.id)
	}
	d.transcript.line("run 1 %s SUCCEEDED; hold -> %s, after (dispatched after the cordon) -> %s", run1,
		pinTaskRun(t, first, "hold").GetOrkerId(), after.GetOrkerId())
	t.Logf("EVIDENCE open pin kept: run 1's post-cordon member ran on A (%s); run 2's pin opened on B (%s)",
		after.GetOrkerId(), b.id)

	rows = d.awaitFleet(&orkstackv1.ListOrkersRequest{OrkerId: a.id}, time.Minute, "A reporting no open pin",
		func(rows []*orkstackv1.Orker) bool { return len(rows) == 1 && rows[0].GetOpenPins() == 0 })
	d.transcript.line("ListOrkers A after    %s", describeRows(rows))
}

func TestIntegration_EveryPinHostCordonedFailsThePinWithPinUnavailable(t *testing.T) {
	ctx := cordonContext(t, 12*time.Minute)
	pool := fmt.Sprintf("allcordoned-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "allcordoned", pool)
	a := d.orker("orker-only-"+d.token, pool, pinOrkerConfig{maxPins: 1})
	awaitPoolServedBy(t, ctx, engine.NewTemporalPoolProbe(d.temporal), pool, a.queue, 1)
	d.storeOrkflow("quick-"+d.token, pool, cordonQuickPinYAML, 0)

	const reason = "whole pool patch window"
	d.cordon(a.id, reason)
	submitted := time.Now()
	runID := d.submit("quick-" + d.token)
	d.transcript.section("the only pin host is cordoned, then a pinned run is submitted")
	d.transcript.line("run %s accepted at %s", runID, submitted.UTC().Format(time.RFC3339))

	run := d.await(runID)
	elapsed := time.Since(submitted)
	first := pinTaskRun(t, run, "first")
	message := first.GetError().GetMessage()
	d.transcript.line("run %s ended %v after %s", runID, run.GetStatus(), elapsed.Round(time.Second))
	d.transcript.line("task first: code=%s category=%v", first.GetError().GetCode(), first.GetError().GetCategory())
	d.transcript.line("message: %s", message)
	t.Logf("EVIDENCE all cordoned: after %s the triggering task failed %s: %s",
		elapsed.Round(time.Second), first.GetError().GetCode(), message)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || first.GetError().GetCode() != "PIN_UNAVAILABLE" {
		t.Fatalf("run %v, task error %v; want FAILED with PIN_UNAVAILABLE", run.GetStatus(), first.GetError())
	}
	if !strings.Contains(message, "ORKER_CORDONED") && !strings.Contains(message, "is cordoned") {
		t.Fatalf("the PIN_UNAVAILABLE message does not name the cordon: %s", message)
	}
	if !strings.Contains(message, reason) {
		t.Fatalf("the PIN_UNAVAILABLE message does not carry the cordon reason %q: %s", reason, message)
	}
	if elapsed < 4*time.Minute || elapsed > 7*time.Minute {
		t.Fatalf("the pin failed after %s; want between 4 and 7 minutes (the §5.4 bound)", elapsed)
	}
	dirs, _ := filepath.Glob(filepath.Join(a.workRoot, "ork-pin-*"))
	if len(dirs) != 0 {
		t.Fatalf("a pin directory exists on the cordoned host: %v", dirs)
	}
	d.transcript.line("pin directories on the host: none")
}

func TestIntegration_ARestartedCordonedOrkerComesBackReady(t *testing.T) {
	ctx := cordonContext(t, 5*time.Minute)
	pool := fmt.Sprintf("restart-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "restart", pool)
	id := "orker-r-" + d.token
	a := d.orker(id, pool, pinOrkerConfig{})
	d.awaitFleet(&orkstackv1.ListOrkersRequest{OrkerId: id}, time.Minute, "A listed",
		func(rows []*orkstackv1.Orker) bool { return len(rows) == 1 })
	row := d.cordon(id, "about to restart")
	d.transcript.line("before restart        %s", describeRows([]*orkstackv1.Orker{row}))

	a.stop()
	a.activities.Close()
	restarted := d.orker(id, pool, pinOrkerConfig{workRoot: a.workRoot})
	rows := d.awaitFleet(&orkstackv1.ListOrkersRequest{OrkerId: id}, time.Minute, "the restarted A answering",
		func(rows []*orkstackv1.Orker) bool {
			return len(rows) == 1 && rows[0].GetUnavailableReason() == ""
		})
	d.transcript.line("after restart         %s", describeRows(rows))
	if rows[0].GetCordoned() || rows[0].GetCordonReason() != "" || rows[0].GetCordonedAt() != nil {
		t.Fatalf("a restarted Orker came back %v; a cordon is process-local and must not survive", rows[0])
	}
	_ = restarted
	t.Logf("EVIDENCE restart clears the cordon: %s", describeRows(rows))
}

func TestIntegration_DescribeOrkerOnThePoolChannelStillAnswersFromTheUncordonedSibling(t *testing.T) {
	ctx := cordonContext(t, 5*time.Minute)
	pool := fmt.Sprintf("probe-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "probe", pool)
	a := d.orker("orker-pa-"+d.token, pool, pinOrkerConfig{})
	b := d.orker("orker-pb-"+d.token, pool, pinOrkerConfig{})
	awaitPoolServedBy(t, ctx, engine.NewTemporalPoolProbe(d.temporal), pool, a.queue, 2)
	d.cordon(a.id, "probe check")

	resp, err := d.namespaces.ListPools(ctx, connect.NewRequest(&orkstackv1.ListPoolsRequest{Namespace: d.orkNS}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	var count int32 = -1
	for _, p := range resp.Msg.GetPools() {
		if p.GetName() == pool {
			count = p.GetLiveOrkerCount()
		}
	}
	d.transcript.line("ListPools %s live_orker_count=%d (A cordoned)", pool, count)
	if count != 2 {
		t.Fatalf("pool %s reports %d live Orkers inside the freshness window; want 2", pool, count)
	}
	// At most two describes can land on A's already-outstanding long polls
	// (§5.3's window); three answers therefore include at least one from B.
	answered := map[string]int{}
	for i := 0; i < 3; i++ {
		got, err := d.starter.OrkerOnPool(ctx, a.queue)
		if err != nil {
			t.Fatalf("the pool's describe probe failed with A cordoned: %v", err)
		}
		answered[got.GetOrkerId()]++
	}
	d.transcript.line("pool describe probe answered by %v", answered)
	if answered[b.id] == 0 {
		t.Fatalf("the uncordoned sibling never answered the pool probe: %v", answered)
	}
	t.Logf("EVIDENCE pool probe with A cordoned: live_orker_count=%d, answered by %v", count, answered)
}

func TestIntegration_CordonReachesAVersionedOrkersControlChannel(t *testing.T) {
	ctx := cordonContext(t, 5*time.Minute)
	pool := fmt.Sprintf("versioned-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "versioned", pool)
	deployment := task.Deployment{Name: "ork-cordon-" + d.token, BuildID: "build-" + d.token}
	a := d.orker("orker-v-"+d.token, pool, pinOrkerConfig{buildID: deployment.BuildID, deployment: deployment})
	if !deployment.Versioned() {
		t.Fatal("this test asserts nothing: the task worker is not versioned")
	}
	d.awaitFleet(&orkstackv1.ListOrkersRequest{OrkerId: a.id}, time.Minute, "the versioned Orker listed",
		func(rows []*orkstackv1.Orker) bool { return len(rows) == 1 })
	row := d.cordon(a.id, "versioned cordon")
	d.transcript.line("CordonOrker on a versioned Orker (%s) -> %s", deployment, describeRows([]*orkstackv1.Orker{row}))
	if !row.GetCordoned() || row.GetBuildId() != deployment.BuildID {
		t.Fatalf("the versioned Orker answered %v; want cordoned with build_id %s", row, deployment.BuildID)
	}
	listed := d.one(a.id)
	if listed == nil || !listed.GetCordoned() || listed.GetBuildId() != deployment.BuildID {
		t.Fatalf("ListOrkers = %v", listed)
	}
	t.Logf("EVIDENCE versioned control channel: %s", describeRows([]*orkstackv1.Orker{listed}))
}
