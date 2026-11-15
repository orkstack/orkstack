package telemetry

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/temporalconn"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestTLSMetrics_CountReloadsReportEndsAndCountRefusedCalls(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewTLSMetrics(&Providers{Meter: mp}, nil)
	must.OK(t, err)
	var _ temporalconn.Observer = m

	end := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	m.CertificateLoaded(temporalconn.SettingCertFile, end)
	m.FileReloaded(temporalconn.SettingCertFile, true)
	m.FileReloaded(temporalconn.SettingCertFile, false)
	m.FileReloaded(temporalconn.SettingCertFile, false)
	m.AuthFailure()

	points := metricPoints(t, reader)
	reloads := map[string]float64{}
	for _, p := range points["ork.tls.reload"] {
		if attrOf(p, AttrTLSFile) != temporalconn.SettingCertFile {
			t.Errorf("a reload labelled %q", attrOf(p, AttrTLSFile))
		}
		reloads[attrOf(p, AttrTLSOutcome)] = p.value
	}
	if reloads["adopted"] != 1 || reloads["refused"] != 2 {
		t.Fatalf("ork.tls.reload = %v, want 1 adopted and 2 refused", reloads)
	}
	ends := points["ork.tls.cert.not_after"]
	if len(ends) != 1 || ends[0].value != float64(end.Unix()) || attrOf(ends[0], AttrTLSFile) != temporalconn.SettingCertFile {
		t.Fatalf("ork.tls.cert.not_after = %+v, want %d for %s", ends, end.Unix(), temporalconn.SettingCertFile)
	}
	if got := points["ork.temporal.auth_failures"]; len(got) != 1 || got[0].value != 1 {
		t.Fatalf("ork.temporal.auth_failures = %+v, want 1", got)
	}
}

func TestTLSMetrics_DisabledIsANoOp(t *testing.T) {
	m, err := NewTLSMetrics(nil, nil)
	must.OK(t, err)
	m.CertificateLoaded(temporalconn.SettingCAFile, time.Now())
	m.FileReloaded(temporalconn.SettingCAFile, true)
	m.AuthFailure()
}
