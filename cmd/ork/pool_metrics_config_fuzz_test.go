package main

import (
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// FuzzPoolMetricsFromEnv: whatever the operator writes, the parser either
// refuses it or answers at most engine.MaxPoolMetricsPools valid pool names
// and an interval within its bounds. It drives the parser, not the process
// environment, which can hold no NUL byte, so the input is never refused
// before it is parsed.
func FuzzPoolMetricsFromEnv(f *testing.F) {
	for _, seed := range [][2]string{
		{"", ""}, {" gpu, build ,", "5m"}, {"gpu,Not_A_Pool", ""}, {"", "4s"}, {"", "6m"}, {"", "often"},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, pools, interval string) {
		got, err := parsePoolMetrics(pools, interval)
		if err != nil {
			return
		}
		if len(got.pools) > engine.MaxPoolMetricsPools {
			t.Fatalf("%d pools accepted; at most %d", len(got.pools), engine.MaxPoolMetricsPools)
		}
		for _, pool := range got.pools {
			if _, err := task.QueueForPool(pool, ""); err != nil {
				t.Fatalf("accepted invalid pool %q: %v", pool, err)
			}
		}
		if got.interval < engine.MinPoolMetricsInterval || got.interval > engine.MaxPoolMetricsInterval {
			t.Fatalf("interval %s outside its bounds", got.interval)
		}
	})
}
