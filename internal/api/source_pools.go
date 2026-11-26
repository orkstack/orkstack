package api

import (
	"context"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

const (
	// doorPoolAge is how long what a message's start learned about pools may
	// serve another's: a busy source asks a pool's Orkers once in it, and not
	// once a message.
	doorPoolAge = 10 * time.Second
	// maxKeptPools bounds the channels, and the Orkers, kept.
	maxKeptPools = 4096
)

// keptPools keeps what this replica learned of pools' channels and Orkers for
// doorPoolAge, for the starts of messages alone: a person's submit asks every
// time. A question that was not answered is not kept.
type keptPools struct {
	channels *keptAnswers[string, engine.PoolLiveness]
	presence *keptAnswers[struct{}, []engine.OrkerPresence]
	orkers   *keptAnswers[string, *orkstackv1.DescribeOrkerResult]
}

func newKeptPools(now func() time.Time) *keptPools {
	return &keptPools{channels: newKeptAnswers[string, engine.PoolLiveness](now), presence: newKeptAnswers[struct{}, []engine.OrkerPresence](now),
		orkers: newKeptAnswers[string, *orkstackv1.DescribeOrkerResult](now)}
}

// over has r ask what is kept first. A nil keptPools leaves r as it is.
func (k *keptPools) over(r *engine.PoolResolver) {
	if k == nil {
		return
	}
	if r.Probe != nil {
		r.Probe = keptProbe{k, r.Probe}
	}
	if r.Fleet != nil {
		r.Fleet = keptFleet{k, r.Fleet}
	}
}

// keptProbe answers which Orkers poll a pool. Whether each serves task
// identity (engine.PoolPollers) is not asked through it: no run a message
// starts holds one.
type keptProbe struct {
	kept *keptPools
	ask  engine.PoolProbe
}

func (p keptProbe) ObservePool(ctx context.Context, queue string) (engine.PoolLiveness, error) {
	return p.kept.channels.get(queue, func() (engine.PoolLiveness, error) { return p.ask.ObservePool(ctx, queue) })
}

type keptFleet struct {
	kept *keptPools
	ask  engine.FleetReader
}

func (f keptFleet) ObserveOrkers(ctx context.Context) ([]engine.OrkerPresence, error) {
	return f.kept.presence.get(struct{}{}, func() ([]engine.OrkerPresence, error) { return f.ask.ObserveOrkers(ctx) })
}

func (f keptFleet) DescribeOrkerByID(ctx context.Context, orkerID string) (*orkstackv1.DescribeOrkerResult, error) {
	return f.kept.orkers.get(orkerID, func() (*orkstackv1.DescribeOrkerResult, error) { return f.ask.DescribeOrkerByID(ctx, orkerID) })
}

// keptAnswers keeps answers of one kind for doorPoolAge.
type keptAnswers[K comparable, V any] struct {
	now  func() time.Time
	mu   sync.Mutex
	kept map[K]keptAnswer[V]
}

type keptAnswer[V any] struct {
	at    time.Time
	value V
}

func newKeptAnswers[K comparable, V any](now func() time.Time) *keptAnswers[K, V] {
	return &keptAnswers[K, V]{now: now, kept: map[K]keptAnswer[V]{}}
}

// get is the answer kept for key, or ask's, kept from the moment it was
// asked. Two that find none both ask.
func (k *keptAnswers[K, V]) get(key K, ask func() (V, error)) (V, error) {
	at := k.now()
	k.mu.Lock()
	a, ok := k.kept[key]
	k.mu.Unlock()
	if age := at.Sub(a.at); ok && age >= 0 && age < doorPoolAge {
		return a.value, nil
	}
	v, err := ask()
	if err != nil {
		return v, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.kept) >= maxKeptPools {
		clear(k.kept)
	}
	k.kept[key] = keptAnswer[V]{at: at, value: v}
	return v, nil
}
