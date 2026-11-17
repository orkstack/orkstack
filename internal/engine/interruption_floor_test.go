package engine_test

import (
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
)

// The fault back-off's floor a run pins is the time the Orker's store-outage
// cordon takes to trip at its defaults (design §4.8 4a), so a change to either
// side is seen here.
func TestDefaultInterruptionBudgets_FaultFloorIsTheStoreCordonsDetection(t *testing.T) {
	want := orker.DefaultStoreCheckInterval * time.Duration(orker.DefaultStoreCheckFailures)
	got := time.Duration(engine.DefaultInterruptionBudgets.NeverStartedFaultFloorMillis) * time.Millisecond
	if got != want {
		t.Fatalf("fault floor = %v, want the store cordon's detection time %v", got, want)
	}
}
