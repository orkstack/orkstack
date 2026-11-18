package main

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestPoolMetricsFromEnv: unset is the default pool alone every 15 s; each
// setting is read; a pool that is not a valid name, more than 64 pools, or an
// interval outside 5s–5m is refused at boot naming the setting, never clamped.
func TestPoolMetricsFromEnv(t *testing.T) {
	got, err := poolMetricsFromEnv()
	if err != nil || len(got.pools) != 0 || got.interval != engine.DefaultPoolMetricsInterval {
		t.Fatalf("unset = %+v, %v; want no extra pool every %s", got, err, engine.DefaultPoolMetricsInterval)
	}
	t.Setenv("ORK_POOL_METRICS_POOLS", " gpu, build ,")
	t.Setenv("ORK_POOL_METRICS_INTERVAL", "5m")
	got, err = poolMetricsFromEnv()
	if err != nil || !slices.Equal(got.pools, []string{"gpu", "build"}) || got.interval != 5*time.Minute {
		t.Fatalf("set = %+v, %v; want gpu and build every 5m", got, err)
	}

	var many []string
	for i := range engine.MaxPoolMetricsPools + 1 {
		many = append(many, fmt.Sprintf("p%d", i))
	}
	for _, tc := range []struct{ env, value string }{
		{"ORK_POOL_METRICS_POOLS", "gpu,Not_A_Pool"},
		{"ORK_POOL_METRICS_POOLS", strings.Join(many, ",")},
		{"ORK_POOL_METRICS_INTERVAL", "4s"},
		{"ORK_POOL_METRICS_INTERVAL", "6m"},
		{"ORK_POOL_METRICS_INTERVAL", "often"},
	} {
		t.Run(tc.env+"="+tc.value[:min(len(tc.value), 20)], func(t *testing.T) {
			t.Setenv("ORK_POOL_METRICS_POOLS", "")
			t.Setenv("ORK_POOL_METRICS_INTERVAL", "")
			t.Setenv(tc.env, tc.value)
			if _, err := poolMetricsFromEnv(); err == nil || !strings.Contains(err.Error(), tc.env) {
				t.Fatalf("%s=%s: err = %v, want a refusal naming it", tc.env, tc.value, err)
			}
		})
	}
}
