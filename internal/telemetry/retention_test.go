package telemetry

import (
	"context"
	"log/slog"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Each refusal is counted under its outcome, from the closed set the alert
// reads; a nil observer records nothing and does not panic.
func TestRetentionObserver_CountsRefusalsByOutcome(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	o := NewRetentionObserver(&Providers{Meter: mp}, slog.New(slog.DiscardHandler))
	ctx := context.Background()
	o.ObserveBackstopRefused(ctx, true)
	o.ObserveBackstopRefused(ctx, false)
	o.ObserveBackstopRefused(ctx, false)

	got := map[string]int64{}
	for _, dp := range int64SumPoints(t, reader, RetentionBackstopRefusedMetric) {
		outcome, ok := pointAttr(dp, AttrRetentionOutcome)
		if !ok {
			t.Fatalf("a point carries no %s: %v", AttrRetentionOutcome, dp.Attributes)
		}
		got[outcome] += dp.Value
	}
	if len(got) != 2 || got[RetentionOutcomeCovered] != 1 || got[RetentionOutcomeShort] != 2 {
		t.Fatalf("counted %v, want covered 1 and short 2", got)
	}

	var none *RetentionObserver
	none.ObserveBackstopRefused(ctx, true)
}
