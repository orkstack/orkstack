package telemetry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

var errNoNamespace = errors.New("namespace not found")

// fakeRegistry is a namespace registry a test edits: List and Get read the
// same map, Get of a name it lacks is errNoNamespace, and failing makes both
// fail. block, when set, holds the first Get until it is closed.
type fakeRegistry struct {
	mu      sync.Mutex
	sinks   map[string]string
	failing bool
	block   chan struct{}
	gets    atomic.Int64
}

func (r *fakeRegistry) source() PolicySource {
	return PolicySource{
		List: func(context.Context) ([]*orkstackv1.Namespace, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.failing {
				return nil, errors.New("store unavailable")
			}
			var out []*orkstackv1.Namespace
			for name, sink := range r.sinks {
				out = append(out, &orkstackv1.Namespace{Name: name, Telemetry: &orkstackv1.TelemetryPolicy{Sink: sink}})
			}
			return out, nil
		},
		Names: func(context.Context) ([]string, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.failing {
				return nil, errors.New("store unavailable")
			}
			var out []string
			for name := range r.sinks {
				out = append(out, name)
			}
			return out, nil
		},
		Get: func(_ context.Context, name string) (*orkstackv1.Namespace, error) {
			if r.gets.Add(1) == 1 && r.block != nil {
				<-r.block
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.failing {
				return nil, errors.New("store unavailable")
			}
			sink, ok := r.sinks[name]
			if !ok {
				return nil, errNoNamespace
			}
			return &orkstackv1.Namespace{Name: name, Telemetry: &orkstackv1.TelemetryPolicy{Sink: sink}}, nil
		},
		IsNotFound: func(err error) bool { return errors.Is(err, errNoNamespace) },
	}
}

func (r *fakeRegistry) set(name, sink string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks[name] = sink
}

// policyFixture is a default sink with a metrics reader, a named sink "team"
// with its own, a named sink "quiet" with no metrics, and a cache over reg.
type policyFixture struct {
	def, team         *Providers
	defRead, teamRead *sdkmetric.ManualReader
	sinks             *Sinks
	cache             *PolicyCache
}

func newPolicyFixture(t *testing.T, reg *fakeRegistry) *policyFixture {
	t.Helper()
	f := &policyFixture{defRead: sdkmetric.NewManualReader(), teamRead: sdkmetric.NewManualReader()}
	metrics := Config{MetricsExporters: []string{ExporterOTLP}}
	f.def = &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.defRead)), cfg: metrics}
	f.team = &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.teamRead)), cfg: metrics}
	f.sinks = newSinksForTest(f.def, map[string]*Providers{"team": f.team, "quiet": {}})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f.cache = NewPolicyCache(ctx, f.sinks, reg.source(), nil)
	return f
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPolicyCache_LoadedBeforeServing: before its first load the cache routes
// nothing and counts every record unrouted; one load routes each namespace to
// its own sink.
func TestPolicyCache_LoadedBeforeServing(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"payments": "team", "ops": ""}}
	f := newPolicyFixture(t, reg)
	ctx := context.Background()

	if p, ok := f.cache.For(ctx, "payments", SignalMetrics); ok || p != nil {
		t.Fatalf("an unloaded cache routed payments to %p", p)
	}
	if got := droppedRecords(t, f.defRead); got[SignalMetrics+"/"+DropReasonUnrouted] != 1 {
		t.Fatalf("dropped = %v; want one unrouted record before the first load", got)
	}
	if reg.gets.Load() != 0 {
		t.Fatal("an unloaded cache looked a namespace up; the first load answers it")
	}

	must.OK(t, f.cache.Load(ctx, time.Second))
	for ns, want := range map[string]*Providers{"payments": f.team, "ops": f.def} {
		if p, ok := f.cache.For(ctx, ns, SignalMetrics); !ok || p != want {
			t.Errorf("For(%q) = (%p, %v); want (%p, true)", ns, p, ok, want)
		}
	}
}

// TestPolicyCache_LoadIsBounded: a first load that does not finish within its
// bound returns, and the cache stays unrouted until a later load succeeds.
func TestPolicyCache_LoadIsBounded(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"payments": "team"}}
	src := reg.source()
	release := make(chan struct{})
	list := src.List
	src.List = func(ctx context.Context) ([]*orkstackv1.Namespace, error) {
		select {
		case <-release:
			return list(ctx)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f := newPolicyFixture(t, reg)
	f.cache = NewPolicyCache(context.Background(), f.sinks, src, nil)
	if err := f.cache.Load(context.Background(), 20*time.Millisecond); err == nil {
		t.Fatal("a load that never answered succeeded")
	}
	if _, ok := f.cache.For(context.Background(), "payments", SignalMetrics); ok {
		t.Fatal("a cache whose load timed out routed a record")
	}
	close(release)
	must.OK(t, f.cache.Refresh(context.Background()))
	if p, ok := f.cache.For(context.Background(), "payments", SignalMetrics); !ok || p != f.team {
		t.Fatalf("after a good refresh For = (%p, %v); want the team sink", p, ok)
	}
}

// TestPolicyCache_MissLooksUpOnceOffHotPath: a namespace the cache does not
// hold is dropped as pending at once, without waiting for the registry; any
// number of misses while that lookup runs start no second one; once it finds
// the namespace, its records route.
func TestPolicyCache_MissLooksUpOnceOffHotPath(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{}, block: make(chan struct{})}
	f := newPolicyFixture(t, reg)
	ctx := context.Background()
	must.OK(t, f.cache.Load(ctx, time.Second))
	reg.set("fresh", "team")

	for range 50 {
		if _, ok := f.cache.For(ctx, "fresh", SignalMetrics); ok {
			t.Fatal("a miss routed before its lookup answered")
		}
	}
	waitFor(t, "the lookup to start", func() bool { return reg.gets.Load() >= 1 })
	if got := reg.gets.Load(); got != 1 {
		t.Fatalf("50 misses started %d lookups; want 1", got)
	}
	if got := droppedRecords(t, f.defRead); got[SignalMetrics+"/"+DropReasonPending] != 50 {
		t.Fatalf("dropped = %v; want 50 pending", got)
	}
	close(reg.block)
	waitFor(t, "the lookup to cache fresh", func() bool {
		p, ok := f.cache.Policy("fresh")
		return ok && p.Sink == "team"
	})
	if p, ok := f.cache.For(ctx, "fresh", SignalMetrics); !ok || p != f.team {
		t.Fatalf("For(fresh) after its lookup = (%p, %v); want the team sink", p, ok)
	}
}

// TestPolicyCache_MissDropsAndCounts: a namespace the registry does not hold
// is pending until its lookup answers and unrouted after, with no second
// lookup; a sink this process does not declare is sink_unknown.
func TestPolicyCache_MissDropsAndCounts(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"stray": "ghost"}}
	f := newPolicyFixture(t, reg)
	ctx := context.Background()
	must.OK(t, f.cache.Load(ctx, time.Second))

	f.cache.For(ctx, "nowhere", SignalMetrics)
	waitFor(t, "the lookup of nowhere to answer", func() bool { return f.cache.knownMissing("nowhere") })
	f.cache.For(ctx, "nowhere", SignalMetrics)
	f.cache.For(ctx, "nowhere", SignalLogs)
	f.cache.For(ctx, "stray", SignalMetrics)

	got := droppedRecords(t, f.defRead)
	want := map[string]int64{
		SignalMetrics + "/" + DropReasonPending:     1,
		SignalMetrics + "/" + DropReasonUnrouted:    1,
		SignalLogs + "/" + DropReasonUnrouted:       1,
		SignalMetrics + "/" + DropReasonSinkUnknown: 1,
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("dropped[%s] = %d; want %d (all: %v)", k, got[k], n, got)
		}
	}
	if n := reg.gets.Load(); n != 1 {
		t.Fatalf("lookups = %d; want 1 (a namespace found missing is not looked up again before the next refresh)", n)
	}
}

// TestPolicyCache_FailedLookupIsRetried: a lookup the registry could not
// answer marks nothing, so a later miss looks the namespace up again.
func TestPolicyCache_FailedLookupIsRetried(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{}}
	f := newPolicyFixture(t, reg)
	ctx := context.Background()
	must.OK(t, f.cache.Load(ctx, time.Second))
	reg.mu.Lock()
	reg.failing = true
	reg.mu.Unlock()
	f.cache.mu.Lock()
	f.cache.lookupRetry = 0
	f.cache.mu.Unlock()
	f.cache.For(ctx, "flaky", SignalMetrics)
	waitFor(t, "the failed lookup to finish", func() bool { return reg.gets.Load() == 1 && !f.cache.lookingUp("flaky") })
	if f.cache.knownMissing("flaky") {
		t.Fatal("a lookup that failed marked the namespace missing")
	}
	f.cache.mu.Lock()
	f.cache.retryAt["flaky"] = time.Now().Add(time.Hour)
	f.cache.mu.Unlock()
	f.cache.For(ctx, "flaky", SignalMetrics)
	if got := droppedRecords(t, f.defRead); got[SignalMetrics+"/"+DropReasonLookupFailed] != 1 {
		t.Fatalf("dropped = %v; a namespace whose lookup failed is lookup_failed, never pending", got)
	}
	f.cache.mu.Lock()
	f.cache.retryAt["flaky"] = time.Time{}
	f.cache.mu.Unlock()
	reg.mu.Lock()
	reg.failing = false
	reg.sinks["flaky"] = "team"
	reg.mu.Unlock()
	f.cache.For(ctx, "flaky", SignalMetrics)
	waitFor(t, "the retried lookup to cache flaky", func() bool { _, ok := f.cache.Policy("flaky"); return ok })
}

// TestPolicyCache_NeverFallsBackToDefault: a record of a namespace that
// misses the cache, or names an undeclared sink, never reaches the default
// sink: For hands back no Providers at all.
func TestPolicyCache_NeverFallsBackToDefault(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"stray": "ghost"}}
	f := newPolicyFixture(t, reg)
	ctx := context.Background()
	for _, ns := range []string{"stray", "unknown"} {
		if p, ok := f.cache.For(ctx, ns, SignalMetrics); ok || p != nil {
			t.Fatalf("unloaded For(%q) = %p", ns, p)
		}
	}
	must.OK(t, f.cache.Load(ctx, time.Second))
	for _, ns := range []string{"stray", "unknown"} {
		if p, ok := f.cache.For(ctx, ns, SignalMetrics); ok || p != nil {
			t.Fatalf("For(%q) = %p; want a drop, never the default sink", ns, p)
		}
	}
}

// TestPolicyCache_FailedRefreshKeepsLastGood: a refresh that cannot read the
// registry keeps the map it had; one that can replaces it, dropping a
// namespace that is gone and forgetting what was found missing.
func TestPolicyCache_FailedRefreshKeepsLastGood(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"payments": "team"}}
	f := newPolicyFixture(t, reg)
	ctx := context.Background()
	must.OK(t, f.cache.Load(ctx, time.Second))

	reg.mu.Lock()
	reg.failing = true
	reg.mu.Unlock()
	if err := f.cache.Refresh(ctx); err == nil {
		t.Fatal("a refresh of a failing registry succeeded")
	}
	if p, ok := f.cache.For(ctx, "payments", SignalMetrics); !ok || p != f.team {
		t.Fatalf("after a failed refresh For = (%p, %v); want the last good team sink", p, ok)
	}

	reg.mu.Lock()
	reg.failing = false
	reg.sinks = map[string]string{"ops": ""}
	reg.mu.Unlock()
	f.cache.For(ctx, "later", SignalMetrics)
	waitFor(t, "later to be found missing", func() bool { return f.cache.knownMissing("later") })
	must.OK(t, f.cache.Refresh(ctx))
	if _, ok := f.cache.Policy("payments"); ok {
		t.Fatal("a namespace gone from the registry outlived a good refresh")
	}
	if f.cache.knownMissing("later") {
		t.Fatal("a good refresh kept a namespace found missing before it; a new one would never be looked up")
	}
}

// TestPolicyCache_NamespacesGaugeByRouting: ork.telemetry.namespaces counts
// the cached namespaces by where their records go, on the default sink.
func TestPolicyCache_NamespacesGaugeByRouting(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{
		"a": "", "b": DefaultSink, "c": "team", "d": "quiet", "e": "ghost",
	}}
	f := newPolicyFixture(t, reg)
	must.OK(t, f.cache.Load(context.Background(), time.Second))
	var rm metricdata.ResourceMetrics
	must.OK(t, f.defRead.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != TelemetryNamespacesMetric {
				continue
			}
			for _, p := range m.Data.(metricdata.Gauge[int64]).DataPoints {
				routing, _ := p.Attributes.Value(AttrRouting)
				got[routing.AsString()] = p.Value
			}
		}
	}
	want := map[string]int64{RoutingDefault: 2, RoutingNamed: 1, RoutingNamedWithoutMetrics: 1, RoutingSinkUnknown: 1}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("namespaces{routing=%s} = %d; want %d (all %v)", k, got[k], n, got)
		}
	}
}

// TestPolicyCache_RefreshCostsAReadPerNamespace: a refresh is charged the
// listing and one read per namespace, and a lookup one read, which is what
// the census budget is charged (docs/DEPLOY.md, "Censuses").
func TestPolicyCache_RefreshCostsAReadPerNamespace(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"a": "", "b": "team", "c": ""}}
	f := newPolicyFixture(t, reg)
	if got := f.cache.cost(); got != 1 {
		t.Fatalf("an unloaded cache's refresh is admitted for %d; want 1", got)
	}
	must.OK(t, f.cache.Load(context.Background(), time.Second))
	if got := f.cache.Requests(); got != 4 {
		t.Fatalf("a refresh of 3 namespaces made %d requests; want 4", got)
	}
	if got := f.cache.cost(); got != 4 {
		t.Fatalf("the next refresh is admitted for %d; want 4", got)
	}
	f.cache.For(context.Background(), "d", SignalMetrics)
	waitFor(t, "the lookup of d", func() bool { return f.cache.knownMissing("d") })
	if got := f.cache.Requests(); got != 4 {
		t.Fatalf("requests after one lookup = %d; want 4: a lookup is not a refresh's to pay for", got)
	}
}

// TestProcessLogs_NamedSinkNamespaceCarriesNoAuthoredNames: a log line about
// a namespace on the default sink carries the names a tenant chose; one on
// another sink, or one the cache cannot route, carries the namespace alone.
func TestProcessLogs_NamedSinkNamespaceCarriesNoAuthoredNames(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"ops": "", "payments": "team", "stray": "ghost"}}
	f := newPolicyFixture(t, reg)
	authored := []any{"schedule", "nightly-payroll"}
	if got := f.cache.TenantAttrs("ops", authored...); len(got) != 2 {
		t.Fatalf("before the load TenantAttrs(ops) = %v; want the namespace alone", got)
	}
	must.OK(t, f.cache.Load(context.Background(), time.Second))
	for ns, wantAuthored := range map[string]bool{"ops": true, "payments": false, "stray": false, "unknown": false} {
		got := f.cache.TenantAttrs(ns, authored...)
		if got[0] != "namespace" || got[1] != ns {
			t.Errorf("TenantAttrs(%q) = %v; want it to lead with the namespace", ns, got)
		}
		if has := len(got) == 4; has != wantAuthored {
			t.Errorf("TenantAttrs(%q) = %v; authored names carried = %v, want %v", ns, got, has, wantAuthored)
		}
	}
	var nilCache *PolicyCache
	if got := nilCache.TenantAttrs("ops", authored...); len(got) != 2 {
		t.Errorf("a nil cache's TenantAttrs = %v; want the namespace alone", got)
	}

	// The engine's per-event line: a run on a named sink names no Orkflow,
	// task or selection attribute on the default sink; one on the default
	// sink names them all.
	var buf bytes.Buffer
	o, err := NewEngineObserverForSinks(f.sinks, slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	must.OK(t, err)
	event := &orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, RunId: "ork-run-1", TaskId: "charge-cards"}
	for _, tc := range []struct {
		sink      string
		wantNames bool
	}{{"team", false}, {"ghost", false}, {"", true}, {DefaultSink, true}} {
		buf.Reset()
		o.ObserveEvent(context.Background(), event, &orkstackv1.Run{
			Id: "ork-run-1", Namespace: "payments", OrkflowName: "nightly-payroll",
			Telemetry: &orkstackv1.TelemetrySelection{Sink: tc.sink, Attributes: map[string]string{"ork.label.team": "blue-squad"}},
		})
		line := buf.String()
		if !strings.Contains(line, "run_id=ork-run-1") || !strings.Contains(line, "namespace=payments") {
			t.Errorf("sink %q: line %q lacks the run id or namespace", tc.sink, line)
		}
		for _, name := range []string{"nightly-payroll", "charge-cards", "blue-squad"} {
			if strings.Contains(line, name) != tc.wantNames {
				t.Errorf("sink %q: line carries %q = %v; want %v (line %q)", tc.sink, name, !tc.wantNames, tc.wantNames, line)
			}
		}
	}
}

// TestPolicyCache_LookupOlderThanARefreshKeepsNothing: a lookup that began
// before a good refresh does not write over what the refresh read later.
func TestPolicyCache_LookupOlderThanARefreshKeepsNothing(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"fresh": "team"}, block: make(chan struct{})}
	f := newPolicyFixture(t, reg)
	ctx := context.Background()
	reg.mu.Lock()
	reg.sinks = map[string]string{}
	reg.mu.Unlock()
	must.OK(t, f.cache.Load(ctx, time.Second))
	reg.set("fresh", "team")
	f.cache.For(ctx, "fresh", SignalMetrics) // its lookup blocks in Get
	waitFor(t, "the lookup to start", func() bool { return reg.gets.Load() == 1 })
	reg.set("fresh", "") // moved to the default sink, which the refresh reads
	must.OK(t, f.cache.Refresh(ctx))
	close(reg.block)
	waitFor(t, "the stale lookup to finish", func() bool { return !f.cache.lookingUp("fresh") })
	if p, ok := f.cache.Policy("fresh"); !ok || p.Sink != "" {
		t.Fatalf("fresh = (%+v, %v); want the refresh's default sink, not the older lookup's team", p, ok)
	}
}

// TestPolicyCache_APanickingLookupIsRecovered: a registry read that panics
// fails its lookup, like any failed read, and never ends the process.
func TestPolicyCache_APanickingLookupIsRecovered(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{}}
	src := reg.source()
	var calls atomic.Int64
	src.Get = func(context.Context, string) (*orkstackv1.Namespace, error) { calls.Add(1); panic("boom") }
	f := newPolicyFixture(t, reg)
	f.cache = NewPolicyCache(context.Background(), f.sinks, src, nil)
	must.OK(t, f.cache.Load(context.Background(), time.Second))
	f.cache.For(context.Background(), "bad", SignalMetrics)
	waitFor(t, "the lookup to finish", func() bool { return calls.Load() == 1 && !f.cache.lookingUp("bad") })
	if f.cache.knownMissing("bad") {
		t.Fatal("a panicking read marked the namespace missing; it is a failed read")
	}
}

// TestPolicyCache_RefreshReadsABoundedBatch: past the first load a refresh
// lists the names and reads at most its batch of documents, a namespace it
// has never read first, so its cost never outgrows the census budget, and
// every namespace is read again within ceil(N / batch) refreshes.
func TestPolicyCache_RefreshReadsABoundedBatch(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{}}
	for i := range 10 {
		reg.sinks[fmt.Sprintf("ns-%02d", i)] = "team"
	}
	f := newPolicyFixture(t, reg)
	ctx := context.Background()
	must.OK(t, f.cache.Load(ctx, time.Second))
	f.cache.SetRefreshBatch(3)
	if got := f.cache.cost(); got != 4 {
		t.Fatalf("a refresh of 10 namespaces in batches of 3 is admitted for %d; want 4", got)
	}
	reg.mu.Lock()
	for name := range reg.sinks {
		reg.sinks[name] = ""
	}
	delete(reg.sinks, "ns-09")
	reg.sinks["new"] = "team"
	reg.mu.Unlock()

	before, gets := f.cache.Requests(), reg.gets.Load()
	must.OK(t, f.cache.Refresh(ctx))
	if made, read := f.cache.Requests()-before, reg.gets.Load()-gets; made != 4 || read != 3 {
		t.Fatalf("one refresh made %d requests and read %d documents; want 4 and 3", made, read)
	}
	if p, ok := f.cache.Policy("new"); !ok || p.Sink != "team" {
		t.Fatalf("new = (%+v, %v); a namespace never read is read first", p, ok)
	}
	if _, ok := f.cache.Policy("ns-09"); ok {
		t.Fatal("a namespace gone from the listing outlived the refresh")
	}
	for range 3 {
		must.OK(t, f.cache.Refresh(ctx))
	}
	for i := range 9 {
		name := fmt.Sprintf("ns-%02d", i)
		if p, _ := f.cache.Policy(name); p.Sink != "" {
			t.Errorf("%s still has its old sink after four refreshes of batch 3", name)
		}
	}
}

// TestPolicyCache_AFailedReadKeepsTheLastGoodPolicy: a document a refresh
// cannot read keeps the policy it had, and the refresh reports the failure.
func TestPolicyCache_AFailedReadKeepsTheLastGoodPolicy(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"payments": "team"}}
	src := reg.source()
	f := newPolicyFixture(t, reg)
	get := src.Get
	failing := false
	src.Get = func(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
		if failing {
			return nil, errors.New("read refused")
		}
		return get(ctx, name)
	}
	f.cache = NewPolicyCache(context.Background(), f.sinks, src, nil)
	must.OK(t, f.cache.Load(context.Background(), time.Second))
	failing = true
	if err := f.cache.Refresh(context.Background()); err == nil {
		t.Fatal("a refresh whose read failed reported nothing")
	}
	if p, ok := f.cache.Policy("payments"); !ok || p.Sink != "team" {
		t.Fatalf("payments = (%+v, %v); want its last good team sink", p, ok)
	}
}
