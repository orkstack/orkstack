package telemetry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/archive"
)

// gcPoint is one collected data point: its labels and its value.
type gcPoint struct {
	labels map[string]string
	value  float64
}

func (p gcPoint) String() string {
	keys := make([]string, 0, len(p.labels))
	for k := range p.labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+p.labels[k])
	}
	return fmt.Sprintf("{%s} %v", strings.Join(parts, ","), p.value)
}

// collectGC reads every data point the reader holds, by metric name.
func collectGC(t *testing.T, reader *sdkmetric.ManualReader) map[string][]gcPoint {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string][]gcPoint{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], gcPoint{labelsOf(dp.Attributes.ToSlice()), float64(dp.Value)})
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], gcPoint{labelsOf(dp.Attributes.ToSlice()), float64(dp.Value)})
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], gcPoint{labelsOf(dp.Attributes.ToSlice()), dp.Value})
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], gcPoint{labelsOf(dp.Attributes.ToSlice()), float64(dp.Count)})
				}
			default:
				t.Fatalf("%s: unexpected data %T", m.Name, m.Data)
			}
		}
	}
	return out
}

// value is the one point of name whose labels are exactly want, or fails.
func value(t *testing.T, points map[string][]gcPoint, name string, want map[string]string) float64 {
	t.Helper()
	for _, p := range points[name] {
		if len(p.labels) != len(want) {
			continue
		}
		match := true
		for k, v := range want {
			if p.labels[k] != v {
				match = false
				break
			}
		}
		if match {
			return p.value
		}
	}
	t.Fatalf("%s has no point %v; it has %v", name, want, points[name])
	return 0
}

func gcObserverFor(t *testing.T, mode archive.ReclaimMode, route GCRouter) (*GCObserver, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	p := newTestProviders(t, &recordingExporter{}, reader)
	return NewGCObserver(p, route, mode, 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil))), reader
}

var gcStoreNow = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

func deletePass() archive.ReclaimReport {
	return archive.ReclaimReport{
		Mode: archive.ReclaimDelete, Started: gcStoreNow, Duration: 2 * time.Second, Outcome: "completed",
		StoreNow: gcStoreNow,
		LowWater: archive.LowWater{Known: true, At: gcStoreNow.Add(-3 * time.Hour), RunID: "ork-old", Chains: 4},
		Objects: map[archive.TallyKey]archive.Tally{
			{Namespace: "acme", Class: archive.ClassArtifact, Outcome: archive.OutcomeDeleted, Reason: archive.ReasonUnreferenced}: {Objects: 3, Bytes: 300},
			{Namespace: "acme", Class: archive.ClassValues, Outcome: archive.OutcomeKept, Reason: archive.ReasonClaimLive}:         {Objects: 1, Bytes: 10},
			{Namespace: "beta", Class: archive.ClassArtifact, Outcome: archive.OutcomeFailed, Reason: archive.ReasonStoreError}:    {Objects: 2, Bytes: 20},
			// A held or warming pass's count carries no outcome of the contract.
			{Namespace: "beta", Class: archive.ClassArtifact, Outcome: archive.OutcomeWouldDelete, Reason: archive.ReasonUnreferenced}: {Objects: 9, Bytes: 90},
		},
		RunsFinished: 2, RunsCarried: 1, RunsWaiting: 3, RunsLateCommit: 1,
		OldestDue: gcStoreNow.Add(-30 * time.Minute),
	}
}

func TestGCObserver_DeletePassCountsObjectsInTheirNamespaceAndTheTwins(t *testing.T) {
	o, reader := gcObserverFor(t, archive.ReclaimDelete, nil)
	o.ObserveReclaim(context.Background(), deletePass())
	points := collectGC(t, reader)

	if got := value(t, points, GCObjectsMetric, map[string]string{AttrNamespace: "acme", AttrGCObjectClass: "artifact",
		AttrOutcome: "deleted", AttrGCReason: "unreferenced"}); got != 3 {
		t.Fatalf("deleted = %v", got)
	}
	if got := value(t, points, GCBytesMetric, map[string]string{AttrNamespace: "acme", AttrGCObjectClass: "artifact",
		AttrOutcome: "deleted"}); got != 300 {
		t.Fatalf("deleted bytes = %v", got)
	}
	if got := value(t, points, DeploymentGCObjects, map[string]string{AttrGCObjectClass: "artifact",
		AttrOutcome: "failed", AttrGCReason: "store_error"}); got != 2 {
		t.Fatalf("twin failed = %v", got)
	}
	for _, name := range []string{GCObjectsMetric, DeploymentGCObjects} {
		for _, p := range points[name] {
			if p.labels[AttrOutcome] == archive.OutcomeWouldDelete {
				t.Fatalf("%s counted a would-delete in delete mode: %v", name, p)
			}
		}
	}
	for _, p := range points[DeploymentGCObjects] {
		if _, has := p.labels[AttrNamespace]; has {
			t.Fatalf("a twin carries a namespace: %v", p)
		}
	}
	if got := value(t, points, GCPassesMetric, map[string]string{AttrOutcome: "completed"}); got != 1 {
		t.Fatalf("passes = %v", got)
	}
	for outcome, want := range map[string]float64{"finished": 2, "carried_over": 1, "waiting": 3, "held": 1} {
		if got := value(t, points, GCRunsMetric, map[string]string{AttrOutcome: outcome}); got != want {
			t.Fatalf("runs %s = %v, want %v", outcome, got, want)
		}
	}
	if got := value(t, points, GCLagMetric, map[string]string{}); got != 1800 {
		t.Fatalf("lag = %v", got)
	}
	if got := value(t, points, GCLowWaterAgeMetric, map[string]string{}); got != 3*3600 {
		t.Fatalf("low-water age = %v", got)
	}
	if got := value(t, points, GCLowWaterChainsMetric, map[string]string{}); got != 4 {
		t.Fatalf("chains = %v", got)
	}
	if got := value(t, points, GCDueRunsMetric, map[string]string{}); got != 1 {
		t.Fatalf("due = %v", got)
	}
	if got := value(t, points, GCWaitingRunsMetric, map[string]string{}); got != 3 {
		t.Fatalf("waiting = %v", got)
	}
	if got := value(t, points, GCLastProgressMetric, map[string]string{}); got != float64(gcStoreNow.Add(2*time.Second).Unix()) {
		t.Fatalf("last progress = %v", got)
	}
}

func TestGCObserver_ModeAndGraceAreExportedBeforeAnyPassAndNothingElseIs(t *testing.T) {
	_, reader := gcObserverFor(t, archive.ReclaimOff, nil)
	points := collectGC(t, reader)
	for _, mode := range []string{"off", "dry_run", "delete"} {
		want := 0.0
		if mode == "off" {
			want = 1
		}
		if got := value(t, points, GCModeMetric, map[string]string{AttrGCMode: mode}); got != want {
			t.Fatalf("mode %s = %v, want %v", mode, got, want)
		}
	}
	if got := value(t, points, GCGraceMetric, map[string]string{}); got != 600 {
		t.Fatalf("grace = %v", got)
	}
	for _, name := range []string{GCLagMetric, GCHeldMetric, GCLastPassMetric, GCLastProgressMetric,
		GCLowWaterAgeMetric, GCLowWaterUnknownMetric, GCStoreVersionedMetric} {
		if len(points[name]) != 0 {
			t.Fatalf("%s reported before any pass: %v", name, points[name])
		}
	}
}

func TestGCObserver_HoldsAndLowWaterCauseAreZeroOrOneForEveryValue(t *testing.T) {
	o, reader := gcObserverFor(t, archive.ReclaimDelete, nil)
	held := archive.ReclaimReport{Mode: archive.ReclaimDelete, Started: gcStoreNow, Outcome: "held", StoreNow: gcStoreNow,
		Held:     []string{archive.HoldClockSkew, archive.HoldLowWaterUnknown},
		LowWater: archive.LowWater{Cause: archive.LowWaterMidSwitch}, StoreVersioned: true}
	o.ObserveReclaim(context.Background(), held)
	// A failed pass leaves what the last pass that got that far saw.
	o.ObserveReclaim(context.Background(), archive.ReclaimReport{Mode: archive.ReclaimDelete, Started: gcStoreNow.Add(time.Hour), Outcome: "failed"})
	points := collectGC(t, reader)
	for _, reason := range gcHoldReasons {
		want := 0.0
		if reason == archive.HoldClockSkew || reason == archive.HoldLowWaterUnknown {
			want = 1
		}
		if got := value(t, points, GCHeldMetric, map[string]string{AttrGCHoldReason: reason}); got != want {
			t.Fatalf("held %s = %v, want %v", reason, got, want)
		}
	}
	for _, cause := range gcLowWaterCauses {
		want := 0.0
		if cause == archive.LowWaterMidSwitch {
			want = 1
		}
		if got := value(t, points, GCLowWaterUnknownMetric, map[string]string{AttrGCLowWaterCause: cause}); got != want {
			t.Fatalf("low-water unknown %s = %v, want %v", cause, got, want)
		}
	}
	if len(points[GCLowWaterAgeMetric]) != 0 {
		t.Fatalf("an unknown mark reported an age: %v", points[GCLowWaterAgeMetric])
	}
	if got := value(t, points, GCStoreVersionedMetric, map[string]string{}); got != 1 {
		t.Fatalf("versioned = %v", got)
	}
	if got := value(t, points, GCLastPassMetric, map[string]string{}); got != float64(gcStoreNow.Add(time.Hour).Unix()) {
		t.Fatalf("the failed pass is not the last pass: %v", got)
	}
	if got := value(t, points, GCPassesMetric, map[string]string{AttrOutcome: "failed"}); got != 1 {
		t.Fatalf("failed passes = %v", got)
	}
	// No progress: the last progress is still this engine's start, not a pass.
	if got := value(t, points, GCLastProgressMetric, map[string]string{}); got == float64(gcStoreNow.Unix()) ||
		got == float64(gcStoreNow.Add(time.Hour).Unix()) {
		t.Fatalf("a pass that finished nothing made progress: %v", got)
	}
}

func TestGCObserver_DryRunGaugesReplaceOnlyOnACompleteCycle(t *testing.T) {
	o, reader := gcObserverFor(t, archive.ReclaimDryRun, nil)
	would := func(ns string) archive.TallyKey {
		return archive.TallyKey{Namespace: ns, Class: archive.ClassArtifact, Outcome: archive.OutcomeWouldDelete, Reason: archive.ReasonUnreferenced}
	}
	kept := archive.TallyKey{Namespace: "acme", Class: archive.ClassArtifact, Outcome: archive.OutcomeKept, Reason: archive.ReasonLowWater}
	first := archive.ReclaimReport{Mode: archive.ReclaimDryRun, Started: gcStoreNow, Outcome: "completed", CycleComplete: true,
		Cycle: map[archive.TallyKey]archive.Tally{would("acme"): {Objects: 4, Bytes: 40}, would("beta"): {Objects: 1, Bytes: 5}, kept: {Objects: 2, Bytes: 2}}}
	first.Objects = first.Cycle
	o.ObserveReclaim(context.Background(), first)
	// A pass that does not complete the cycle changes no gauge.
	o.ObserveReclaim(context.Background(), archive.ReclaimReport{Mode: archive.ReclaimDryRun, Started: gcStoreNow, Outcome: "partial",
		Objects: map[archive.TallyKey]archive.Tally{would("acme"): {Objects: 100, Bytes: 1000}}})
	points := collectGC(t, reader)
	if got := value(t, points, GCWouldDeleteBytes, map[string]string{AttrNamespace: "acme", AttrGCObjectClass: "artifact"}); got != 40 {
		t.Fatalf("would-delete bytes = %v", got)
	}
	if got := value(t, points, GCKeptObjects, map[string]string{AttrNamespace: "acme", AttrGCObjectClass: "artifact", AttrGCReason: "lowwater"}); got != 2 {
		t.Fatalf("kept = %v", got)
	}
	if got := value(t, points, DeploymentGCWouldDelete, map[string]string{}); got != 45 {
		t.Fatalf("would-delete twin = %v, want the deployment total", got)
	}
	if len(points[GCObjectsMetric]) != 0 || len(points[DeploymentGCObjects]) != 0 {
		t.Fatalf("a dry run counted delete-mode objects: %v %v", points[GCObjectsMetric], points[DeploymentGCObjects])
	}
	// The next complete cycle no longer has beta: its series goes to zero.
	next := archive.ReclaimReport{Mode: archive.ReclaimDryRun, Started: gcStoreNow, Outcome: "completed", CycleComplete: true,
		Cycle: map[archive.TallyKey]archive.Tally{would("acme"): {Objects: 1, Bytes: 7}}}
	o.ObserveReclaim(context.Background(), next)
	points = collectGC(t, reader)
	if got := value(t, points, GCWouldDeleteBytes, map[string]string{AttrNamespace: "beta", AttrGCObjectClass: "artifact"}); got != 0 {
		t.Fatalf("a series the cycle no longer has kept its value: %v", got)
	}
	if got := value(t, points, GCKeptObjects, map[string]string{AttrNamespace: "acme", AttrGCObjectClass: "artifact", AttrGCReason: "lowwater"}); got != 0 {
		t.Fatalf("kept series the cycle no longer has = %v", got)
	}
	if got := value(t, points, DeploymentGCWouldDelete, map[string]string{}); got != 7 {
		t.Fatalf("would-delete twin = %v", got)
	}
}

func TestGCObserver_RoutesNamespacedRecordsAndDropsWhatTheRouterRefuses(t *testing.T) {
	otherReader := sdkmetric.NewManualReader()
	other := newTestProviders(t, &recordingExporter{}, otherReader)
	route := func(_ context.Context, ns string) (*Providers, bool) {
		switch ns {
		case "acme":
			return other, true
		default:
			return nil, false // pending or unknown: dropped, never the default sink
		}
	}
	o, reader := gcObserverFor(t, archive.ReclaimDelete, route)
	o.ObserveReclaim(context.Background(), deletePass())
	defaults, routed := collectGC(t, reader), collectGC(t, otherReader)
	if len(defaults[GCObjectsMetric]) != 0 {
		t.Fatalf("a namespaced record reached the default sink: %v", defaults[GCObjectsMetric])
	}
	for _, p := range routed[GCObjectsMetric] {
		if p.labels[AttrNamespace] != "acme" {
			t.Fatalf("a refused namespace was recorded: %v", p)
		}
	}
	if len(routed[GCObjectsMetric]) == 0 {
		t.Fatal("the routed namespace was not recorded on its sink")
	}
	// The twins see every namespace, on the default sink.
	if got := value(t, defaults, DeploymentGCObjects, map[string]string{AttrGCObjectClass: "artifact",
		AttrOutcome: "failed", AttrGCReason: "store_error"}); got != 2 {
		t.Fatalf("twin of the dropped namespace = %v", got)
	}
}

func TestGCObserver_LabelsStayInsideTheContract(t *testing.T) {
	o, reader := gcObserverFor(t, archive.ReclaimDelete, nil)
	report := deletePass()
	report.Objects[archive.TallyKey{Namespace: "acme", Class: "bogus", Outcome: archive.OutcomeDeleted, Reason: archive.ReasonUnreferenced}] = archive.Tally{Objects: 1}
	report.Objects[archive.TallyKey{Namespace: "acme", Class: archive.ClassArtifact, Outcome: archive.OutcomeKept, Reason: archive.ReasonStoreError}] = archive.Tally{Objects: 1}
	report.Objects[archive.TallyKey{Namespace: "acme", Class: archive.ClassArtifact, Outcome: "exploded", Reason: archive.ReasonUnreferenced}] = archive.Tally{Objects: 1}
	report.Held = []string{"made_up"}
	o.ObserveReclaim(context.Background(), report)
	o.ObserveReclaim(context.Background(), archive.ReclaimReport{Mode: archive.ReclaimDelete, Outcome: "sideways"})
	allowed := map[string][]string{
		AttrGCObjectClass:   gcClasses,
		AttrOutcome:         slices.Concat(gcOutcomes, gcPassOutcomes, []string{"finished", "carried_over", "waiting", "held"}),
		AttrGCReason:        gcReasons,
		AttrGCHoldReason:    gcHoldReasons,
		AttrGCLowWaterCause: gcLowWaterCauses,
		AttrGCMode:          gcModes,
		AttrNamespace:       {"acme", "beta"},
	}
	for name, points := range collectGC(t, reader) {
		if !strings.HasPrefix(name, "ork.gc.") && !strings.HasPrefix(name, "ork.deployment.gc.") {
			continue
		}
		for _, p := range points {
			for k, v := range p.labels {
				set, known := allowed[k]
				if !known || !slices.Contains(set, v) {
					t.Fatalf("%s carries %s=%q outside the contract: %v", name, k, v, p)
				}
			}
		}
	}
}

func TestGCObserver_NilAndDisabledProvidersRecordNothingAndDoNotPanic(t *testing.T) {
	var nilObserver *GCObserver
	nilObserver.ObserveReclaim(context.Background(), deletePass())
	o := NewGCObserver(nil, nil, archive.ReclaimDelete, time.Hour, nil)
	o.ObserveReclaim(context.Background(), deletePass())
}

func labelsOf(kvs []attribute.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

func TestGCObserver_AFailedFirstPassLeavesTheStateGaugesUnknown(t *testing.T) {
	o, reader := gcObserverFor(t, archive.ReclaimDelete, nil)
	o.ObserveReclaim(context.Background(), archive.ReclaimReport{Mode: archive.ReclaimDelete, Started: gcStoreNow, Outcome: "failed"})
	points := collectGC(t, reader)
	// A pass that never got far enough to see holds, a mark or due runs
	// reports none of them: an absent series, never a healthy-looking 0.
	for _, name := range []string{GCHeldMetric, GCLowWaterUnknownMetric, GCLowWaterAgeMetric, GCLowWaterChainsMetric,
		GCLagMetric, GCDueRunsMetric, GCWaitingRunsMetric, GCStoreVersionedMetric} {
		if len(points[name]) != 0 {
			t.Fatalf("%s reported after only a failed pass: %v", name, points[name])
		}
	}
	if got := value(t, points, GCLastPassMetric, map[string]string{}); got != float64(gcStoreNow.Unix()) {
		t.Fatalf("last pass = %v: the failed pass is still a pass", got)
	}
	if got := value(t, points, GCPassesMetric, map[string]string{AttrOutcome: "failed"}); got != 1 {
		t.Fatalf("failed passes = %v", got)
	}
}

func TestGCObserver_AFailedPassThatDeletedMovesProgressAndNothingElse(t *testing.T) {
	o, reader := gcObserverFor(t, archive.ReclaimDelete, nil)
	good := deletePass()
	good.Objects = nil
	good.RunsFinished = 0
	o.ObserveReclaim(context.Background(), good)
	failed := deletePass()
	failed.Started, failed.Outcome = gcStoreNow.Add(time.Hour), "failed"
	failed.RunsWaiting = 99
	o.ObserveReclaim(context.Background(), failed)
	points := collectGC(t, reader)
	if got := value(t, points, GCLastProgressMetric, map[string]string{}); got != float64(gcStoreNow.Add(time.Hour+2*time.Second).Unix()) {
		t.Fatalf("a failed pass that deleted did not move progress: %v", got)
	}
	if got := value(t, points, GCWaitingRunsMetric, map[string]string{}); got != 3 {
		t.Fatalf("a failed pass moved the waiting runs: %v", got)
	}
}

func TestGCObserver_AReasonMustBelongToItsOutcome(t *testing.T) {
	o, reader := gcObserverFor(t, archive.ReclaimDelete, nil)
	report := archive.ReclaimReport{Mode: archive.ReclaimDelete, Started: gcStoreNow, Outcome: "completed", StoreNow: gcStoreNow,
		Objects: map[archive.TallyKey]archive.Tally{
			{Namespace: "acme", Class: archive.ClassArtifact, Outcome: archive.OutcomeDeleted, Reason: archive.ReasonRunLive}:          {Objects: 1},
			{Namespace: "acme", Class: archive.ClassArtifact, Outcome: archive.OutcomeFailed, Reason: archive.ReasonUnreferenced}:      {Objects: 1},
			{Namespace: "acme", Class: archive.ClassArtifact, Outcome: archive.OutcomeAlreadyGone, Reason: archive.ReasonLowWater}:     {Objects: 1},
			{Namespace: "acme", Class: archive.ClassArtifact, Outcome: archive.OutcomeAlreadyGone, Reason: archive.ReasonUnreferenced}: {Objects: 1},
		}}
	o.ObserveReclaim(context.Background(), report)
	points := collectGC(t, reader)
	if len(points[GCObjectsMetric]) != 1 || points[GCObjectsMetric][0].labels[AttrOutcome] != archive.OutcomeAlreadyGone {
		t.Fatalf("objects = %v, want only already_gone/unreferenced", points[GCObjectsMetric])
	}
}

func TestGCObserver_ADeletionWithAnUnknownClassStillMovesProgress(t *testing.T) {
	o, reader := gcObserverFor(t, archive.ReclaimDelete, nil)
	report := archive.ReclaimReport{Mode: archive.ReclaimDelete, Started: gcStoreNow, Outcome: "completed", StoreNow: gcStoreNow,
		Objects: map[archive.TallyKey]archive.Tally{
			{Namespace: "acme", Class: "bogus", Outcome: archive.OutcomeDeleted, Reason: archive.ReasonUnreferenced}: {Objects: 1},
		}}
	o.ObserveReclaim(context.Background(), report)
	if got := value(t, collectGC(t, reader), GCLastProgressMetric, map[string]string{}); got != float64(gcStoreNow.Unix()) {
		t.Fatalf("a deletion did not move progress: %v", got)
	}
}

func TestGCObserver_AsksTheRouterOncePerNamespacePerPass(t *testing.T) {
	asked := map[string]int{}
	route := func(_ context.Context, ns string) (*Providers, bool) {
		asked[ns]++
		return nil, false
	}
	o, _ := gcObserverFor(t, archive.ReclaimDelete, route)
	o.ObserveReclaim(context.Background(), deletePass())
	for ns, n := range asked {
		if n != 1 {
			t.Fatalf("the router was asked %d times about %s in one pass", n, ns)
		}
	}
	if len(asked) != 2 {
		t.Fatalf("asked = %v", asked)
	}
}

func TestGCObserver_ASinkChangeBetweenCyclesZeroesTheOldSinksSeries(t *testing.T) {
	firstReader, secondReader := sdkmetric.NewManualReader(), sdkmetric.NewManualReader()
	first := newTestProviders(t, &recordingExporter{}, firstReader)
	second := newTestProviders(t, &recordingExporter{}, secondReader)
	sink := first
	route := func(context.Context, string) (*Providers, bool) { return sink, true }
	o, _ := gcObserverFor(t, archive.ReclaimDryRun, route)
	key := archive.TallyKey{Namespace: "acme", Class: archive.ClassArtifact, Outcome: archive.OutcomeWouldDelete, Reason: archive.ReasonUnreferenced}
	cycle := archive.ReclaimReport{Mode: archive.ReclaimDryRun, Started: gcStoreNow, Outcome: "completed", CycleComplete: true,
		Cycle: map[archive.TallyKey]archive.Tally{key: {Objects: 2, Bytes: 20}}}
	o.ObserveReclaim(context.Background(), cycle)
	sink = second
	o.ObserveReclaim(context.Background(), cycle)
	labels := map[string]string{AttrNamespace: "acme", AttrGCObjectClass: "artifact"}
	if got := value(t, collectGC(t, firstReader), GCWouldDeleteBytes, labels); got != 0 {
		t.Fatalf("the old sink kept the namespace's series: %v", got)
	}
	if got := value(t, collectGC(t, secondReader), GCWouldDeleteBytes, labels); got != 20 {
		t.Fatalf("the new sink = %v", got)
	}
}
