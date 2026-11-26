package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestOrkerIdentityMetrics_AreRecordedWithTheirLabels: each thing the Orker
// hears is one point under its closed labels and the Orker's id; a value
// outside its set is "other", so nothing an engine answers makes a series.
func TestOrkerIdentityMetrics_AreRecordedWithTheirLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewOrkerIdentityMetrics(&Providers{Meter: mp}, "orker-1")
	must.OK(t, err)
	for _, outcome := range []string{orker.IdentityRefreshed, orker.IdentityRefreshRetried, orker.IdentityRefreshEnded,
		orker.IdentityRefreshStopped, "something new"} {
		m.Refreshed(outcome)
	}
	m.TokenMargin(90 * time.Second)
	m.TokenMargin(-time.Second)
	m.ClockSkewed()
	m.EngineRefused(orker.EngineRefusedOrker)
	m.EngineRefused(orker.EngineCouldNotRead)
	m.EngineRefused("something new")
	m.CommitWaited(2*time.Second, orker.ResultRecorded)
	m.CommitWaited(time.Minute, orker.ResultNotRecorded)
	m.CommitWaited(3*time.Second, orker.ResultRefused)
	m.CommitWaited(4*time.Second, "something new")
	m.UpstreamUnrecorded()

	got := map[string]float64{}
	points := metricPoints(t, reader)
	for metric, label := range map[string]string{IdentityRefreshMetric: AttrOutcome, IdentityEngineRefusedMetric: AttrRefusedHow,
		IdentityClockSkewMetric: "", ResultUpstreamUnrecordedMetric: ""} {
		for _, p := range points[metric] {
			if id, _ := p.attrs.Value(AttrOrkerID); id.AsString() != "orker-1" {
				t.Errorf("%s: no Orker id: %v", metric, p.attrs)
			}
			value, _ := p.attrs.Value(attribute.Key(label))
			got[metric+"/"+value.AsString()] += p.value
		}
	}
	for want, n := range map[string]float64{
		IdentityRefreshMetric + "/refreshed": 1, IdentityRefreshMetric + "/retried": 1, IdentityRefreshMetric + "/ended": 1,
		IdentityRefreshMetric + "/stopped": 1, IdentityRefreshMetric + "/other": 1,
		IdentityEngineRefusedMetric + "/refused": 1, IdentityEngineRefusedMetric + "/invalid": 1, IdentityEngineRefusedMetric + "/other": 1,
		IdentityClockSkewMetric + "/": 1, ResultUpstreamUnrecordedMetric + "/": 1,
	} {
		if got[want] != n {
			t.Errorf("%s = %v, want %v (all: %v)", want, got[want], n, got)
		}
	}

	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	sums := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if h, ok := md.Data.(metricdata.Histogram[float64]); ok {
				for _, p := range h.DataPoints {
					outcome, _ := p.Attributes.Value(AttrOutcome)
					sums[md.Name+"/"+outcome.AsString()] += p.Sum
				}
			}
		}
	}
	if sums[IdentityTokenMarginMetric+"/"] != 89 || sums[ResultCommitWaitMetric+"/recorded"] != 2 || sums[ResultCommitWaitMetric+"/not_recorded"] != 60 ||
		sums[ResultCommitWaitMetric+"/refused"] != 3 || sums[ResultCommitWaitMetric+"/other"] != 4 {
		t.Fatalf("histograms = %v", sums)
	}

	off, err := NewOrkerIdentityMetrics(nil, "orker-1")
	must.OK(t, err)
	off.Refreshed(orker.IdentityRefreshed)
	off.TokenMargin(time.Second)
	off.CommitWaited(time.Second, orker.ResultRecorded)
}
