package telemetry

import (
	"context"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestForceCancelInstruments: one series an outcome, an ending's also by what
// the run it ended was resumed from, and nothing else on it.
func TestForceCancelInstruments(t *testing.T) {
	providers, _, reader := newObserverProviders(t)
	inst, err := NewForceCancelInstruments(providers)
	must.OK(t, err)
	ctx := context.Background()
	inst.CountForceCancelEnding(ctx, "force_canceled", "force_canceled")
	inst.CountForceCancelEnding(ctx, "force_canceled", "force_canceled")
	inst.CountForceCancelEnding(ctx, "force_canceled", "none")
	inst.CountForceCancelEnding(ctx, "failed", "unknown")
	inst.CountForceCancel(ctx, "squatter")
	got := map[string]int64{}
	for _, p := range int64SumPoints(t, reader, ForceCancelsMetric) {
		outcome, _ := p.Attributes.Value(AttrCancelOutcome)
		key := outcome.AsString()
		if from, ok := p.Attributes.Value(AttrCancelResumedFrom); ok {
			key += "/" + from.AsString()
		}
		got[key] = p.Value
		if want := 1 + strings.Count(key, "/"); p.Attributes.Len() != want {
			t.Errorf("labels %v", p.Attributes)
		}
	}
	if got["force_canceled/force_canceled"] != 2 || got["force_canceled/none"] != 1 || got["failed/unknown"] != 1 || got["squatter"] != 1 || len(got) != 4 {
		t.Fatalf("%v", got)
	}
	noop, err := NewForceCancelInstruments(nil)
	must.OK(t, err)
	noop.CountForceCancel(ctx, "failed")
	noop.CountForceCancelEnding(ctx, "failed", "none")
	var none *ForceCancelInstruments
	none.CountForceCancel(ctx, "failed")
	none.CountForceCancelEnding(ctx, "failed", "none")
}
