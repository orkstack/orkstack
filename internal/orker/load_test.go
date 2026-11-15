package orker

import (
	"context"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestActivities_LoadReadsWhatDescribeOrkerReports: the load gauges
// (ork.orker.running_tasks, open_pins, max_concurrent, cordoned) read the
// same counters and cordon DescribeOrker answers from, so a scrape and
// `orkctl orker list` never disagree about one Orker.
func TestActivities_LoadReadsWhatDescribeOrkerReports(t *testing.T) {
	a := NewActivitiesWithOptions(nil, Options{
		OrkerID:       cordonOrkerID,
		Pool:          "build",
		MaxConcurrent: 4,
		MaxPins:       2,
		WorkRoot:      t.TempDir(),
		Runtime:       &reapRecorder{labelled: map[string]int{}},
	})
	now := time.Now()

	idle := a.Load()
	if idle != (LoadReading{MaxConcurrent: 4}) {
		t.Fatalf("an idle, uncordoned Orker's load = %+v; want only max_concurrent 4", idle)
	}

	a.running.add()
	a.running.add()
	a.pins.add("session-1", "run-1", now)
	a.Cordon().Set("cve", now)
	a.Cordon().SetStoreOutage("store down", now)
	a.Cordon().SetStopping(now, now.Add(time.Hour))

	got := a.Load()
	described, err := a.DescribeOrker(context.Background())
	must.OK(t, err)
	want := LoadReading{
		RunningTasks:  int64(described.GetRunningTasks()),
		OpenPins:      int64(described.GetOpenPins()),
		MaxConcurrent: int64(described.GetMaxConcurrent()),
		Operator:      described.GetCordonReason() != "",
		StoreOutage:   described.GetStoreOutage() != nil,
		Stopping:      described.GetStopping() != nil,
	}
	if got != want {
		t.Fatalf("Load() = %+v; DescribeOrker says %+v", got, want)
	}
	if got.RunningTasks != 2 || got.OpenPins != 1 || !got.Operator || !got.StoreOutage || !got.Stopping {
		t.Fatalf("Load() = %+v; want 2 running, 1 open pin and all three causes", got)
	}

	// Clearing one cause leaves the others (AGENTS.md §7, Cordon).
	a.Cordon().Clear()
	if got := a.Load(); got.Operator || !got.StoreOutage || !got.Stopping {
		t.Fatalf("after clearing the operator's cordon, Load() = %+v; want store outage and stopping only", got)
	}
}
