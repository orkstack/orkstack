package telemetry

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/archive"
)

var cleanupNow = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func cleanupObserverFor(t *testing.T, settings CleanupSettings, route GCRouter) (*CleanupObserver, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	p := newTestProviders(t, &recordingExporter{}, reader)
	o := NewCleanupObserver(p, route, settings, slog.New(slog.NewTextHandler(io.Discard, nil)))
	o.now = func() time.Time { return cleanupNow }
	return o, reader
}

func onSettings() CleanupSettings {
	return CleanupSettings{Enabled: true, Interval: time.Hour, RetentionBuffer: 24 * time.Hour}
}

func TestCleanupObserver_SettingsAreExportedFromBootAndNoWalkGaugeIs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		settings CleanupSettings
		enabled  float64
	}{
		{name: "enabled", settings: onSettings(), enabled: 1},
		{name: "off", settings: CleanupSettings{Interval: time.Hour, RetentionBuffer: 24 * time.Hour}, enabled: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, reader := cleanupObserverFor(t, tc.settings, nil)
			points := collectGC(t, reader)
			if got := value(t, points, CleanupEnabledMetric, map[string]string{}); got != tc.enabled {
				t.Fatalf("enabled = %v, want %v", got, tc.enabled)
			}
			if got := value(t, points, CleanupIntervalMetric, map[string]string{}); got != 3600 {
				t.Fatalf("interval = %v", got)
			}
			if got := value(t, points, CleanupRetentionBufferMetric, map[string]string{}); got != 24*3600 {
				t.Fatalf("retention buffer = %v", got)
			}
			for _, name := range []string{CleanupLastPassMetric, DeploymentCleanupRetentionLag, CleanupRetentionLagMetric} {
				if len(points[name]) != 0 {
					t.Fatalf("%s reported before any walk: %v", name, points[name])
				}
			}
		})
	}
}

func TestCleanupObserver_APassCountsItsRunsInTheirNamespaceAndTheTwin(t *testing.T) {
	o, reader := cleanupObserverFor(t, onSettings(), nil)
	o.ObserveCleanup(context.Background(), archive.CleanupPass{
		Outcome:  archive.CleanupPassDrain,
		Duration: 3 * time.Second,
		Namespaces: map[string]archive.CleanupNamespaceTally{
			"acme": {Runs: map[string]int{archive.CleanupRunRetired: 2, archive.CleanupRunFailed: 1}, Objects: 4, Bytes: 400},
			"beta": {Runs: map[string]int{archive.CleanupRunRetired: 1, archive.CleanupRunNotArchived: 1}},
		},
		StepErrors: map[string]int{archive.CleanupStepDelete: 1, archive.CleanupStepBackstop: 2},
	})
	points := collectGC(t, reader)

	for _, c := range []struct {
		ns, outcome string
		want        float64
	}{
		{"acme", "retired", 2}, {"acme", "failed", 1}, {"beta", "retired", 1}, {"beta", "not_archived", 1},
	} {
		if got := value(t, points, CleanupRunsMetric, map[string]string{AttrNamespace: c.ns, AttrOutcome: c.outcome}); got != c.want {
			t.Errorf("runs %s/%s = %v, want %v", c.ns, c.outcome, got, c.want)
		}
	}
	if got := value(t, points, DeploymentCleanupRuns, map[string]string{AttrOutcome: "retired"}); got != 3 {
		t.Fatalf("twin retired = %v, want 3", got)
	}
	for _, p := range points[DeploymentCleanupRuns] {
		if _, has := p.labels[AttrNamespace]; has {
			t.Fatalf("a twin carries a namespace: %v", p)
		}
	}
	if got := value(t, points, CleanupObjectsMetric, map[string]string{AttrNamespace: "acme"}); got != 4 {
		t.Fatalf("objects = %v", got)
	}
	if got := value(t, points, CleanupBytesMetric, map[string]string{AttrNamespace: "acme"}); got != 400 {
		t.Fatalf("bytes = %v", got)
	}
	if len(points[CleanupObjectsMetric]) != 1 {
		t.Fatalf("a namespace that removed nothing was counted: %v", points[CleanupObjectsMetric])
	}
	if got := value(t, points, CleanupPassesMetric, map[string]string{AttrOutcome: "drain"}); got != 1 {
		t.Fatalf("passes = %v", got)
	}
	if got := value(t, points, CleanupPassDurationMetric, map[string]string{AttrOutcome: "drain"}); got != 1 {
		t.Fatalf("duration count = %v", got)
	}
	if got := value(t, points, CleanupErrorsMetric, map[string]string{AttrCleanupStep: "backstop"}); got != 2 {
		t.Fatalf("backstop errors = %v", got)
	}
	// A pass that ended no walk leaves the walk gauges unreported.
	if len(points[CleanupLastPassMetric]) != 0 || len(points[DeploymentCleanupRetentionLag]) != 0 {
		t.Fatalf("a drain reported a walk: %v %v", points[CleanupLastPassMetric], points[DeploymentCleanupRetentionLag])
	}
}

func TestCleanupObserver_AWalkReplacesTheRetentionLagAndZeroesWhatItNoLongerHas(t *testing.T) {
	o, reader := cleanupObserverFor(t, onSettings(), nil)
	ctx := context.Background()
	o.ObserveCleanup(ctx, archive.CleanupPass{Outcome: archive.CleanupPassCompleted, WalkComplete: true,
		RetentionLag: map[string]time.Duration{"acme": 2 * time.Hour, "beta": 30 * time.Minute}})
	points := collectGC(t, reader)
	if got := value(t, points, CleanupRetentionLagMetric, map[string]string{AttrNamespace: "acme"}); got != 7200 {
		t.Fatalf("acme lag = %v", got)
	}
	if got := value(t, points, DeploymentCleanupRetentionLag, map[string]string{}); got != 7200 {
		t.Fatalf("twin lag = %v, want the deployment maximum", got)
	}
	if got := value(t, points, CleanupLastPassMetric, map[string]string{}); got != float64(cleanupNow.Unix()) {
		t.Fatalf("last pass = %v", got)
	}

	o.ObserveCleanup(ctx, archive.CleanupPass{Outcome: archive.CleanupPassCompleted, WalkComplete: true,
		RetentionLag: map[string]time.Duration{"beta": time.Minute}})
	points = collectGC(t, reader)
	if got := value(t, points, CleanupRetentionLagMetric, map[string]string{AttrNamespace: "acme"}); got != 0 {
		t.Fatalf("acme lag = %v after a walk that left it none, want 0", got)
	}
	if got := value(t, points, CleanupRetentionLagMetric, map[string]string{AttrNamespace: "beta"}); got != 60 {
		t.Fatalf("beta lag = %v", got)
	}
	if got := value(t, points, DeploymentCleanupRetentionLag, map[string]string{}); got != 60 {
		t.Fatalf("twin lag = %v", got)
	}

	o.ObserveCleanup(ctx, archive.CleanupPass{Outcome: archive.CleanupPassCompleted, WalkComplete: true})
	points = collectGC(t, reader)
	if got := value(t, points, DeploymentCleanupRetentionLag, map[string]string{}); got != 0 {
		t.Fatalf("twin lag = %v after a walk with none, want a reported 0", got)
	}
}

func TestCleanupObserver_ADroppedNamespaceStillReachesTheTwins(t *testing.T) {
	route := func(context.Context, string) (*Providers, bool) { return nil, false }
	o, reader := cleanupObserverFor(t, onSettings(), route)
	o.ObserveCleanup(context.Background(), archive.CleanupPass{
		Outcome: archive.CleanupPassCompleted, WalkComplete: true,
		Namespaces:   map[string]archive.CleanupNamespaceTally{"acme": {Runs: map[string]int{archive.CleanupRunFailed: 2}, Objects: 1, Bytes: 1}},
		RetentionLag: map[string]time.Duration{"acme": time.Hour},
	})
	points := collectGC(t, reader)
	for _, name := range []string{CleanupRunsMetric, CleanupObjectsMetric, CleanupBytesMetric, CleanupRetentionLagMetric} {
		if len(points[name]) != 0 {
			t.Fatalf("%s recorded a dropped namespace: %v", name, points[name])
		}
	}
	if got := value(t, points, DeploymentCleanupRuns, map[string]string{AttrOutcome: "failed"}); got != 2 {
		t.Fatalf("twin failed = %v", got)
	}
	if got := value(t, points, DeploymentCleanupRetentionLag, map[string]string{}); got != 3600 {
		t.Fatalf("twin lag = %v", got)
	}
}

func TestCleanupObserver_LabelsOutsideTheContractAreNeverRecorded(t *testing.T) {
	o, reader := cleanupObserverFor(t, onSettings(), nil)
	o.ObserveCleanup(context.Background(), archive.CleanupPass{
		Outcome:    "exploded",
		Namespaces: map[string]archive.CleanupNamespaceTally{"acme": {Runs: map[string]int{"vanished": 1}}},
		StepErrors: map[string]int{"teleport": 1},
	})
	points := collectGC(t, reader)
	for _, name := range []string{CleanupPassesMetric, CleanupPassDurationMetric, CleanupErrorsMetric, CleanupRunsMetric, DeploymentCleanupRuns} {
		if len(points[name]) != 0 {
			t.Fatalf("%s recorded a value outside the contract: %v", name, points[name])
		}
	}
}

// TestCleanupObserver_EveryContractValueIsRecordable: every value of the
// closed sets the archive package states reaches its instrument.
func TestCleanupObserver_EveryContractValueIsRecordable(t *testing.T) {
	o, reader := cleanupObserverFor(t, onSettings(), nil)
	runs := map[string]int{}
	for _, outcome := range archive.CleanupRunOutcomes() {
		runs[outcome] = 1
	}
	steps := map[string]int{}
	for _, step := range archive.CleanupSteps() {
		steps[step] = 1
	}
	for _, outcome := range archive.CleanupPassOutcomes() {
		o.ObserveCleanup(context.Background(), archive.CleanupPass{Outcome: outcome})
	}
	o.ObserveCleanup(context.Background(), archive.CleanupPass{Outcome: archive.CleanupPassCompleted,
		Namespaces: map[string]archive.CleanupNamespaceTally{"acme": {Runs: runs}}, StepErrors: steps})
	points := collectGC(t, reader)
	if got, want := len(points[CleanupRunsMetric]), len(runs); got != want {
		t.Fatalf("run outcomes recorded = %d, want %d", got, want)
	}
	if got, want := len(points[CleanupErrorsMetric]), len(steps); got != want {
		t.Fatalf("steps recorded = %d, want %d", got, want)
	}
	if got, want := len(points[CleanupPassesMetric]), len(archive.CleanupPassOutcomes()); got != want {
		t.Fatalf("pass outcomes recorded = %d, want %d", got, want)
	}
}

// TestCleanupObserver_AWalkWhoseLagIsUnknownIsNoWalk: a walk that could not
// read some run reports nothing healthy: last_pass does not advance, so it
// goes stale through the outage, and the deployment lag stops reporting.
func TestCleanupObserver_AWalkWhoseLagIsUnknownIsNoWalk(t *testing.T) {
	o, reader := cleanupObserverFor(t, onSettings(), nil)
	ctx := context.Background()
	o.ObserveCleanup(ctx, archive.CleanupPass{Outcome: archive.CleanupPassCompleted, WalkComplete: true, LagUnknown: true})
	points := collectGC(t, reader)
	if len(points[DeploymentCleanupRetentionLag]) != 0 || len(points[CleanupLastPassMetric]) != 0 {
		t.Fatalf("a first walk with unknown lag reported a walk: %v %v", points[DeploymentCleanupRetentionLag], points[CleanupLastPassMetric])
	}

	first := cleanupNow
	o.ObserveCleanup(ctx, archive.CleanupPass{Outcome: archive.CleanupPassCompleted, WalkComplete: true})
	o.now = func() time.Time { return first.Add(time.Hour) }
	o.ObserveCleanup(ctx, archive.CleanupPass{Outcome: archive.CleanupPassCompleted, WalkComplete: true, LagUnknown: true})
	points = collectGC(t, reader)
	if got := value(t, points, CleanupLastPassMetric, map[string]string{}); got != float64(first.Unix()) {
		t.Fatalf("last pass = %v, want it held at the last walk that could tell, %d", got, first.Unix())
	}
	if len(points[DeploymentCleanupRetentionLag]) != 0 {
		t.Fatalf("the deployment lag reported through an unknown walk: %v", points[DeploymentCleanupRetentionLag])
	}
}

// TestCleanupObserver_ACanceledCallerStillRoutesItsRecord: the router is
// asked with a context the caller's cancellation does not reach.
func TestCleanupObserver_ACanceledCallerStillRoutesItsRecord(t *testing.T) {
	p, reader := areaProviders(t)
	route := func(ctx context.Context, _ string) (*Providers, bool) { return p, ctx.Err() == nil }
	o := NewCleanupObserver(p, route, onSettings(), discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	o.ObserveCleanup(ctx, archive.CleanupPass{Outcome: archive.CleanupPassCompleted,
		Namespaces: map[string]archive.CleanupNamespaceTally{"acme": {Runs: map[string]int{archive.CleanupRunRetired: 1}}}})
	if got := value(t, collectGC(t, reader), CleanupRunsMetric, map[string]string{AttrNamespace: "acme", AttrOutcome: "retired"}); got != 1 {
		t.Fatalf("retired = %v, want the record routed", got)
	}
}
