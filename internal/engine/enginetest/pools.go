// Package enginetest holds test doubles for the engine's cluster-facing
// seams, stated in the vocabulary a USER of OrkStack has.
//
// It exists because of where its callers live. A pool probe answers about an
// execution channel — the engine's own word, correct below the seam — while a
// test in cmd/orkctl is on a surface where that word may not appear at all
// (AGENTS.md §1.9, held by proof/M10.1/naming-sweep.sh). These doubles are
// keyed by POOL, and translate through internal/task, which is the one place
// the mapping lives. A CLI test can then say "gpu has one Orker" without
// naming anything the CLI itself must never print.
package enginetest

import (
	"context"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// LivePools is the cluster's poller information, stated: how many Orkers are
// serving each pool, and when they were last seen.
//
// It stands in for engine.TemporalPoolProbe, which asks Temporal for the
// pollers on a channel. A pool nothing names has no Orkers, which is the same
// answer the real probe gives for a channel nobody polls.
type LivePools struct {
	// Orkers maps a pool name to the number of Orkers serving it.
	Orkers map[string]int

	// LastSeen is when those Orkers were observed. Liveness is judged against
	// the resolver's clock, so a test that fixes both gets a stable answer;
	// the zero value means the cluster reported no timestamp, which
	// engine.PoolLiveness.Fresh trusts.
	LastSeen time.Time
}

func (l LivePools) ObservePool(_ context.Context, channel string) (engine.PoolLiveness, error) {
	pool, ok := task.PoolForQueue(channel)
	if !ok {
		// A channel this mapping did not generate — an operator's raw
		// override. It names no pool these doubles were told about.
		return engine.PoolLiveness{}, nil
	}
	orkers := l.Orkers[pool]
	live := engine.PoolLiveness{Orkers: orkers}
	if orkers > 0 {
		live.LastSeen = l.LastSeen
	}
	return live, nil
}

var _ engine.PoolProbe = LivePools{}

// PoolPlugins is what each pool's Orkers report they can run, by name.
type PoolPlugins map[string][]string

func (p PoolPlugins) PluginsOnPool(
	_ context.Context,
	channel string,
) ([]plugin.Descriptor, error) {
	pool, ok := task.PoolForQueue(channel)
	if !ok {
		return nil, nil
	}
	descriptors := make([]plugin.Descriptor, 0, len(p[pool]))
	for _, name := range p[pool] {
		descriptors = append(descriptors, plugin.Descriptor{Name: name})
	}
	return descriptors, nil
}

var _ engine.PoolPluginLister = PoolPlugins{}
