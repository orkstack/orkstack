package telemetry

import (
	"context"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestForceCancelInstruments: one series an outcome, and nothing else on it.
func TestForceCancelInstruments(t *testing.T) {
	providers, _, reader := newObserverProviders(t)
	inst, err := NewForceCancelInstruments(providers)
	must.OK(t, err)
	ctx := context.Background()
	for _, outcome := range []string{"force_canceled", "force_canceled", "failed"} {
		inst.CountForceCancel(ctx, outcome)
	}
	got := map[string]int64{}
	for _, p := range int64SumPoints(t, reader, ForceCancelsMetric) {
		outcome, _ := p.Attributes.Value(AttrCancelOutcome)
		got[outcome.AsString()] = p.Value
		if p.Attributes.Len() != 1 {
			t.Errorf("labels %v", p.Attributes)
		}
	}
	if got["force_canceled"] != 2 || got["failed"] != 1 || len(got) != 2 {
		t.Fatalf("%v", got)
	}
	noop, err := NewForceCancelInstruments(nil)
	must.OK(t, err)
	noop.CountForceCancel(ctx, "failed")
	var none *ForceCancelInstruments
	none.CountForceCancel(ctx, "failed")
}
