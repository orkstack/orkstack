package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ReasonPoolNoIdentitySupport refuses a run that holds an identity on a pool
// one of whose Orkers does not serve task identity (docs/REASONS.md).
const ReasonPoolNoIdentitySupport = "POOL_NO_IDENTITY_SUPPORT"

// maxOrkerReason bounds the Orker's own reason in a refusal: it is the
// Orker's text, and a submit's answer carries it.
const maxOrkerReason = 512

// PoolNoIdentitySupportError names the pool, its tasks, and the first of its
// Orkers that does not advertise identity_supported, or does not answer,
// with its build.
type PoolNoIdentitySupportError struct {
	Namespace, Pool string
	Tasks           []string
	OrkerID         string
	BuildID         string
	// Reason is why the Orker says it does not serve it, as it reports it.
	Reason string
	// Unanswered says the Orker polls the pool's channel and did not answer
	// on its own while another Orker of the pool did: not a verdict, but a
	// non-answer, which may pass.
	Unanswered bool
}

func (e *PoolNoIdentitySupportError) Error() string {
	if e.Unanswered {
		return fmt.Sprintf(
			"%s: pool %q (namespace %q, tasks: %s) is polled by Orker %q, which did not answer on its own channel, so whether it serves task identity is unknown: "+
				"submit again once it answers, or cordon it (docs/TASK_IDENTITY.md, \"Which Orkers serve it\")",
			ReasonPoolNoIdentitySupport, e.Pool, e.Namespace, strings.Join(e.Tasks, ", "), e.OrkerID)
	}
	return fmt.Sprintf(
		"%s: pool %q (namespace %q, tasks: %s) is served by Orker %q (build %q), which does not serve task identity (%s): "+
			"a run that holds an identity needs every Orker of every pool it runs a container on to (docs/TASK_IDENTITY.md, \"Which Orkers serve it\")",
		ReasonPoolNoIdentitySupport, e.Pool, e.Namespace, strings.Join(e.Tasks, ", "), e.OrkerID, e.BuildID, reasonOrUnsaid(e.Reason))
}

// CheckIdentitySupport refuses a run that holds an identity when an
// uncordoned Orker of any pool its planned definition runs a container on
// does not advertise identity_supported (providers.md §10): every task of
// such a run has its result recorded by the engine, not only its identity
// tasks. planned is a definition Plan returned, its tasks' pools resolved.
// It is asked of the run and of every child it plans.
//
// Where another Orker of the pool answers, an Orker that polls the pool's
// channel and does not answer on its own is refused too: it may be the one
// the task lands on. A pool none of whose Orkers answers, and a silent
// Orker whose pool cannot be told, are logged and not refused (AGENTS.md
// §7.2): nothing was measured, and an Orker that does not serve task
// identity refuses such a task at dispatch.
func (r *PoolResolver) CheckIdentitySupport(ctx context.Context, ns *orkstackv1.Namespace, planned *orkstackv1.Orkflow) error {
	tasksByPool := map[string][]string{}
	var order []string
	for _, t := range planned.GetTasks() {
		if t.GetAwait() != nil || t.GetSleep() != nil || t.GetOrkflow() != nil {
			continue // runs no container
		}
		if _, seen := tasksByPool[t.GetPool()]; !seen {
			order = append(order, t.GetPool())
		}
		tasksByPool[t.GetPool()] = append(tasksByPool[t.GetPool()], t.GetId())
	}
	if r.Fleet == nil {
		if len(order) > 0 {
			r.logger().Warn("submit: no Orker inventory is wired, so whether the pools of a run that holds an identity serve it is unknown",
				"namespace", namespaceName(ns), "orkflow", planned.GetName(), "pools", strings.Join(order, ","))
		}
		return nil
	}
	census := newPoolCensus(r.Fleet)
	for _, pool := range order {
		answered := r.probeOrkers(ctx, census, capabilityProbe{
			namespace: namespaceName(ns), orkflow: planned.GetName(), pool: pool, tasks: tasksByPool[pool],
			carrying: "a run that holds an identity", question: "it serves task identity",
		})
		if old := lacking(answered, (*orkstackv1.DescribeOrkerResult).GetIdentitySupported); old != nil {
			return &PoolNoIdentitySupportError{Namespace: namespaceName(ns), Pool: pool, Tasks: tasksByPool[pool],
				OrkerID: old.GetOrkerId(), BuildID: old.GetBuildId(), Reason: old.GetIdentityUnsupportedReason()}
		}
		if len(answered) == 0 || census.unansweredCount() == 0 {
			continue
		}
		if silent := r.silentOrkerOf(ctx, census, ns, planned, pool); silent != "" {
			return &PoolNoIdentitySupportError{Namespace: namespaceName(ns), Pool: pool, Tasks: tasksByPool[pool],
				OrkerID: silent, Unanswered: true}
		}
	}
	return nil
}

// PoolPollers is a PoolProbe that also names who polls a channel, from the
// cluster's poller records. TemporalPoolProbe is one.
type PoolPollers interface {
	PollingOrkers(ctx context.Context, queue string) ([]OrkerPresence, error)
}

// silentOrkerOf is the first Orker, in identity order, that polls pool's
// channel and did not answer the census on its own, or "" when none can
// be told: the probe names no pollers, they could not be read (logged), or
// none of them is silent. A silent Orker that is busy polls nothing, so it
// is not found here; dispatch is its guard.
func (r *PoolResolver) silentOrkerOf(ctx context.Context, census *poolCensus, ns *orkstackv1.Namespace, planned *orkstackv1.Orkflow, pool string) string {
	pollers, ok := r.Probe.(PoolPollers)
	if !ok {
		r.logger().Warn("submit: an Orker did not answer, and this engine's pool probe cannot say who polls pool "+pool+
			", so it is not refused here; an Orker that does not serve task identity refuses such a task at dispatch",
			"namespace", namespaceName(ns), "orkflow", planned.GetName(), "pool", pool, "unanswered", census.unansweredCount())
		return ""
	}
	route, err := r.routeFor(pool)
	var polling []OrkerPresence
	if err == nil {
		polling, err = pollers.PollingOrkers(ctx, route.Queue)
	}
	if err != nil {
		r.logger().Warn("submit: an Orker did not answer, and whether it serves pool "+pool+" of a run that holds an identity could not be read",
			"namespace", namespaceName(ns), "orkflow", planned.GetName(), "pool", pool, "unanswered", census.unansweredCount(), "error", err)
		return ""
	}
	for _, p := range polling {
		if slices.Contains(census.unanswered, p.Identity) {
			return p.Identity
		}
	}
	r.logger().Warn("submit: an Orker did not answer, and none that did not polls pool "+pool+" of a run that holds an identity, so it is not refused here; "+
		"an Orker that does not serve task identity refuses such a task at dispatch",
		"namespace", namespaceName(ns), "orkflow", planned.GetName(), "pool", pool, "unanswered", census.unansweredCount(), "pollers", len(polling))
	return ""
}

// PollingOrkers is the Orkers polling one channel for task activities
// inside the freshness window, one per identity, in identity order: the
// default-mode read ObservePool makes, for the same reasons.
func (p *TemporalPoolProbe) PollingOrkers(ctx context.Context, queue string) ([]OrkerPresence, error) {
	probeCtx, cancel := context.WithTimeout(ctx, poolProbeTimeout)
	defer cancel()
	described, err := p.client.DescribeTaskQueue(probeCtx, queue, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil {
		return nil, fmt.Errorf("describe pool channel %q: %w", queue, err)
	}
	return livePollerRecords(activityDescription(described), p.now(), p.freshness()), nil
}

// reasonOrUnsaid is an Orker's reason, or what an Orker that predates it is
// said to have given.
func reasonOrUnsaid(r string) string {
	if r == "" {
		return "it gives no reason: it may predate task identity"
	}
	if len(r) > maxOrkerReason {
		cut := maxOrkerReason
		for cut > 0 && !utf8.RuneStart(r[cut]) {
			cut--
		}
		r = r[:cut] + "…"
	}
	return fmt.Sprintf("it says %q", r)
}

var _ PoolPollers = (*TemporalPoolProbe)(nil)
