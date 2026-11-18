package telemetry

import (
	"context"
	"log/slog"
	"sync"
)

// areaSinks routes one area's namespaced records (webhooks, schedules,
// triggers) to the sink each namespace's telemetry policy selects, through
// the engine's policy cache, and builds that area's instruments once per
// sink. It never falls back: a namespace the router drops is dropped here,
// and the router counts it (docs/DEPLOY.md, "Sink routing").
type areaSinks[T any] struct {
	defaults *Providers
	route    GCRouter
	build    func(*Providers) (T, error)
	what     string
	logger   *slog.Logger

	cache  providerCache[T]
	warned sync.Map
}

func newAreaSinks[T any](p *Providers, route GCRouter, what string, logger *slog.Logger, build func(*Providers) (T, error)) *areaSinks[T] {
	if logger == nil {
		logger = slog.Default()
	}
	return &areaSinks[T]{defaults: p, route: route, build: build, what: what, logger: logger}
}

// forNamespace is the instruments of the sink namespace's records go to;
// false when the record is dropped. A nil route sends every namespace to
// the default sink, which is right only for a deployment with one.
func (a *areaSinks[T]) forNamespace(ctx context.Context, namespace string) (T, bool) {
	sink := a.defaults
	if a.route != nil {
		// A canceled caller (a client that hung up) never costs the record
		// of what it was answered.
		routed, ok := a.route(context.WithoutCancel(ctx), namespace)
		if !ok || routed == nil {
			var zero T
			return zero, false
		}
		sink = routed
	}
	return a.on(sink)
}

// forDefault is the instruments of the default sink, for a record that
// belongs to no namespace.
func (a *areaSinks[T]) forDefault() (T, bool) { return a.on(a.defaults) }

func (a *areaSinks[T]) on(p *Providers) (T, bool) {
	inst, err := a.cache.getOrBuild(p, func() (T, error) { return a.build(p) })
	if err != nil {
		if _, warned := a.warned.LoadOrStore(p, struct{}{}); !warned {
			a.logger.Warn("telemetry: a sink's "+a.what+" instruments could not be built; its records are dropped", "error", err)
		}
		return inst, false
	}
	return inst, true
}

// warnOnce logs msg the first time key is seen.
func (a *areaSinks[T]) warnOnce(key, msg string, args ...any) {
	if _, warned := a.warned.LoadOrStore("label:"+key, struct{}{}); !warned {
		a.logger.Warn("telemetry: "+msg, args...)
	}
}
