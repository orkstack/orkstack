package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// poolMetricsSettings is engine.pool_metrics.*: the pools beside the default
// one whose backlog this replica reads, and how often.
type poolMetricsSettings struct {
	pools    []string
	interval time.Duration
}

// poolMetricsFromEnv reads engine.pool_metrics.* (docs/DEPLOY.md §1). An
// unset key is its default: the default pool alone, every
// engine.DefaultPoolMetricsInterval. A pool that is not a valid name, more
// than engine.MaxPoolMetricsPools pools, or an interval outside its bounds is
// refused at boot naming the setting, never clamped.
func poolMetricsFromEnv() (poolMetricsSettings, error) {
	s := poolMetricsSettings{interval: engine.DefaultPoolMetricsInterval}
	var errs []error
	for _, pool := range strings.Split(env("ORK_POOL_METRICS_POOLS", ""), ",") {
		pool = strings.TrimSpace(pool)
		if pool == "" {
			continue
		}
		if _, err := task.QueueForPool(pool, ""); err != nil {
			errs = append(errs, fmt.Errorf("ORK_POOL_METRICS_POOLS: %w", err))
			continue
		}
		s.pools = append(s.pools, pool)
	}
	if len(s.pools) > engine.MaxPoolMetricsPools {
		errs = append(errs, fmt.Errorf("ORK_POOL_METRICS_POOLS names %d pools; at most %d", len(s.pools), engine.MaxPoolMetricsPools))
	}
	if raw := env("ORK_POOL_METRICS_INTERVAL", ""); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < engine.MinPoolMetricsInterval || d > engine.MaxPoolMetricsInterval {
			errs = append(errs, fmt.Errorf("ORK_POOL_METRICS_INTERVAL=%q must be a duration from %s to %s",
				raw, engine.MinPoolMetricsInterval, engine.MaxPoolMetricsInterval))
		} else {
			s.interval = d
		}
	}
	if len(errs) > 0 {
		return poolMetricsSettings{interval: engine.DefaultPoolMetricsInterval}, errors.Join(errs...)
	}
	return s, nil
}
