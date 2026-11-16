package orker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

var (
	roundScoped   = artifacts.ScopeRound{Outcome: artifacts.ScopeScoped, Results: artifacts.ResultsEveryNamespace}
	roundUnscoped = artifacts.ScopeRound{Outcome: artifacts.ScopeUnscoped, Detail: "reached: put metadata/identity/"}
	roundUnclear  = artifacts.ScopeRound{Outcome: artifacts.ScopeUnclear, Detail: "list metadata/audit/: SlowDown"}
)

// TestScopeWatch_Verdict is the verdict's table (docs/DEPLOY.md, "An
// Orker's own storage credential"): only a round that ends scoped or
// unscoped moves it; an unclear one never does, in either direction, and a
// scoped verdict lapses to no verdict once its last confirmation is older
// than two re-check intervals.
func TestScopeWatch_Verdict(t *testing.T) {
	const recheck = time.Hour
	cases := []struct {
		name   string
		rounds []artifacts.ScopeRound
		// after is how long after the last round the report is read.
		after time.Duration

		want      orkstackv1.StoreScope
		confirmed bool // the report carries a confirmation time
	}{
		{"no round yet", nil, 0, orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN, false},
		{"none, then scoped", []artifacts.ScopeRound{roundScoped}, 0, orkstackv1.StoreScope_STORE_SCOPE_SCOPED, true},
		{"none, then unscoped", []artifacts.ScopeRound{roundUnscoped}, 0, orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED, false},
		{"none, then unclear", []artifacts.ScopeRound{roundUnclear}, 0, orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN, false},
		{"none, then unclear ten times", repeat(roundUnclear, 10), 0, orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN, false},
		{"scoped, then scoped", []artifacts.ScopeRound{roundScoped, roundScoped}, 0, orkstackv1.StoreScope_STORE_SCOPE_SCOPED, true},
		{"scoped, then unscoped", []artifacts.ScopeRound{roundScoped, roundUnscoped}, 0, orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED, true},
		{"scoped, then unclear inside two intervals", []artifacts.ScopeRound{roundScoped, roundUnclear}, recheck, orkstackv1.StoreScope_STORE_SCOPE_SCOPED, true},
		{"scoped, then unclear past two intervals", []artifacts.ScopeRound{roundScoped, roundUnclear}, 2*recheck - time.Minute + time.Second, orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN, true},
		{"unscoped, then scoped", []artifacts.ScopeRound{roundUnscoped, roundScoped}, 0, orkstackv1.StoreScope_STORE_SCOPE_SCOPED, true},
		{"unscoped, then unscoped", []artifacts.ScopeRound{roundUnscoped, roundUnscoped}, 0, orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED, false},
		{"unscoped, then unclear", []artifacts.ScopeRound{roundUnscoped, roundUnclear}, 0, orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED, false},
		{"unscoped is sticky through unclear rounds", append([]artifacts.ScopeRound{roundUnscoped}, repeat(roundUnclear, 5)...), 0, orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
			w := newTestScopeWatch(recheck, func() time.Time { return clock }, nil)
			for _, r := range tc.rounds {
				w.observe(r)
				clock = clock.Add(time.Minute)
			}
			clock = clock.Add(tc.after)
			got := w.Report()
			if got.Scope != tc.want {
				t.Fatalf("scope = %v, want %v", got.Scope, tc.want)
			}
			if (got.ConfirmedAt != nil) != tc.confirmed {
				t.Errorf("confirmed at = %v, want set=%v", got.ConfirmedAt, tc.confirmed)
			}
			// The detail names the verdict's reason and, after it, what an
			// unclear last round found.
			if n := len(tc.rounds); n > 0 && !strings.Contains(got.Detail, tc.rounds[n-1].Detail) {
				t.Errorf("detail = %q, want it to name the last round's %q", got.Detail, tc.rounds[n-1].Detail)
			}
			if tc.want == orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED && !strings.Contains(got.Detail, roundUnscoped.Detail) {
				t.Errorf("an unscoped verdict's detail %q lost its reason %q", got.Detail, roundUnscoped.Detail)
			}
		})
	}
}

// TestScopeWatch_ReportsResults: the results request's finding is kept from
// the last round that found one, and decides nothing.
func TestScopeWatch_ReportsResults(t *testing.T) {
	w := newTestScopeWatch(time.Hour, time.Now, nil)
	w.observe(artifacts.ScopeRound{Outcome: artifacts.ScopeScoped, Results: artifacts.ResultsByNamespace})
	w.observe(roundUnclear)
	got := w.Report()
	if got.Results != orkstackv1.ResultsScope_RESULTS_SCOPE_BY_NAMESPACE || got.Scope != orkstackv1.StoreScope_STORE_SCOPE_SCOPED {
		t.Fatalf("report = %+v", got)
	}
	w.observe(roundScoped)
	if got := w.Report().Results; got != orkstackv1.ResultsScope_RESULTS_SCOPE_EVERY_NAMESPACE {
		t.Fatalf("results = %v", got)
	}
}

// TestScopeWatch_NextRound: after an unclear round the next one is a minute
// away (or the re-check interval, when that is shorter); otherwise it is one
// re-check interval.
func TestScopeWatch_NextRound(t *testing.T) {
	for _, tc := range []struct {
		recheck time.Duration
		last    artifacts.ScopeRound
		want    time.Duration
	}{
		{time.Hour, roundScoped, time.Hour},
		{time.Hour, roundUnscoped, time.Hour},
		{time.Hour, roundUnclear, time.Minute},
		{10 * time.Second, roundUnclear, 10 * time.Second},
	} {
		w := newTestScopeWatch(tc.recheck, time.Now, nil)
		w.observe(tc.last)
		if got := w.next(); got != tc.want {
			t.Errorf("recheck %s after %v: next = %s, want %s", tc.recheck, tc.last.Outcome, got, tc.want)
		}
	}
}

// TestScopeWatch_LogsAtErrorUnlessScoped: every round whose verdict is not
// scoped is logged at error, naming what it found; a scoped one is not.
func TestScopeWatch_LogsAtErrorUnlessScoped(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newTestScopeWatch(time.Hour, time.Now, logger)
	w.observe(roundUnscoped)
	if out := buf.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "metadata/identity/") {
		t.Fatalf("an unscoped round logged %q", out)
	}
	buf.Reset()
	w.observe(roundScoped)
	if out := buf.String(); strings.Contains(out, "level=ERROR") || !strings.Contains(out, "scoped") {
		t.Fatalf("a scoped round logged %q", out)
	}
	buf.Reset()
	w.observe(roundUnclear)
	if out := buf.String(); strings.Contains(out, "level=ERROR") || !strings.Contains(out, "SlowDown") {
		// The verdict is still scoped: an unclear round is a warning.
		t.Fatalf("an unclear round under a scoped verdict logged %q", out)
	}
	buf.Reset()
	w2 := newTestScopeWatch(time.Hour, time.Now, logger)
	w2.observe(roundUnclear)
	if out := buf.String(); !strings.Contains(out, "level=ERROR") {
		t.Fatalf("an unclear round with no verdict logged %q", out)
	}
}

// TestScopeWatch_RunChecksAtBootAndOnItsCadence: Run's first round is at
// once, and the next ones follow the cadence until ctx is done.
func TestScopeWatch_RunChecksAtBootAndOnItsCadence(t *testing.T) {
	rounds := make(chan struct{}, 10)
	check := func(ctx context.Context) artifacts.ScopeRound {
		select {
		case rounds <- struct{}{}:
		default:
		}
		return roundUnclear
	}
	w := NewScopeWatch(check, ScopeWatchConfig{Recheck: 20 * time.Millisecond}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	deadline := time.After(5 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case <-rounds:
		case <-deadline:
			t.Fatalf("only %d rounds ran", i)
		}
	}
	cancel()
	<-done
	if got := w.Report().Scope; got != orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN {
		t.Fatalf("scope = %v", got)
	}
}

func TestScopeWatchConfig_Validate(t *testing.T) {
	if err := (ScopeWatchConfig{Recheck: time.Hour}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, d := range []time.Duration{0, time.Millisecond, 8 * 24 * time.Hour} {
		if err := (ScopeWatchConfig{Recheck: d}).Validate(); err == nil || !strings.Contains(err.Error(), StoreScopeRecheckVar) {
			t.Errorf("recheck %s: err = %v, want one naming %s", d, err, StoreScopeRecheckVar)
		}
	}
}

func newTestScopeWatch(recheck time.Duration, now func() time.Time, logger *slog.Logger) *ScopeWatch {
	w := NewScopeWatch(nil, ScopeWatchConfig{Recheck: recheck}, logger)
	w.now = now
	return w
}

func repeat(r artifacts.ScopeRound, n int) []artifacts.ScopeRound {
	out := make([]artifacts.ScopeRound, n)
	for i := range out {
		out[i] = r
	}
	return out
}

// TestDescribeOrker_ReportsTheStoreScope: an Orker with no watch (an older
// build's answer) leaves the fields unset; one with a watch reports its
// verdict, the last round's detail, when it was confirmed and the results
// finding.
func TestDescribeOrker_ReportsTheStoreScope(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	activities := NewActivitiesWithOptions(store, Options{Runtime: &reapRecorder{}, WorkRoot: t.TempDir()})
	result, err := activities.DescribeOrker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.GetStoreScope() != orkstackv1.StoreScope_STORE_SCOPE_UNSPECIFIED || result.GetStoreScopeConfirmedAt() != nil {
		t.Fatalf("an Orker with no watch reported %v", result.GetStoreScope())
	}

	round := artifacts.ScopeRound{Outcome: artifacts.ScopeScoped, Results: artifacts.ResultsByNamespace}
	w := activities.WatchStoreScope(func(context.Context) artifacts.ScopeRound { return round }, ScopeWatchConfig{Recheck: time.Hour})
	w.Step(context.Background())
	round = roundUnclear
	w.Step(context.Background())
	result, err = activities.DescribeOrker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.GetStoreScope() != orkstackv1.StoreScope_STORE_SCOPE_SCOPED ||
		!strings.Contains(result.GetStoreScopeDetail(), roundUnclear.Detail) ||
		result.GetStoreScopeConfirmedAt() == nil ||
		result.GetResultsScope() != orkstackv1.ResultsScope_RESULTS_SCOPE_BY_NAMESPACE {
		t.Fatalf("DescribeOrker = scope %v, detail %q, confirmed %v, results %v", result.GetStoreScope(),
			result.GetStoreScopeDetail(), result.GetStoreScopeConfirmedAt(), result.GetResultsScope())
	}
}
