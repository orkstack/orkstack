package webhooks

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Notifier fans one run transition out to every subscription that wants it.
//
// Delivery runs off the caller's goroutine on purpose. The caller is the
// engine recording a run's state change, and a slow or unreachable subscriber
// must not hold that up: notification is observability, not orchestration.
// The cost of that choice is that a delivery does not outlive the process, so
// Wait exists to drain in-flight deliveries at shutdown.
type Notifier struct {
	registry   *Registry
	dispatcher *Dispatcher
	logger     *slog.Logger
	newID      func() (string, error)

	wg sync.WaitGroup

	mu        sync.Mutex
	observers []func(Result)
}

// NotifierOptions tunes a Notifier.
type NotifierOptions struct {
	// Logger records delivery outcomes. Nil discards them.
	Logger *slog.Logger
	// NewDeliveryID generates delivery ids. Nil uses crypto/rand.
	NewDeliveryID func() (string, error)
}

// NewNotifier wires a registry to a dispatcher.
func NewNotifier(registry *Registry, dispatcher *Dispatcher, options NotifierOptions) *Notifier {
	n := &Notifier{
		registry:   registry,
		dispatcher: dispatcher,
		logger:     options.Logger,
		newID:      options.NewDeliveryID,
	}
	if n.dispatcher == nil {
		n.dispatcher = NewDispatcher(DispatcherOptions{})
	}
	if n.newID == nil {
		n.newID = func() (string, error) { return randomToken("dlv-", 12) }
	}
	if n.logger == nil {
		n.logger = slog.New(slog.DiscardHandler)
	}
	return n
}

// Observe registers a callback invoked with every delivery result, after the
// delivery settles. It is the seam tests and proof runs use to watch the wire
// without a real subscriber.
func (n *Notifier) Observe(observer func(Result)) {
	if observer == nil {
		return
	}
	n.mu.Lock()
	n.observers = append(n.observers, observer)
	n.mu.Unlock()
}

// Notify queues delivery to every matching subscription and returns how many
// it queued for an in-memory registry. For a durable registry lookup happens
// asynchronously, so it returns zero because the count is not yet known.
//
// The caller's context supplies values and nothing else: it is stripped of
// cancellation, because the activity that observed the transition returns long
// before a retried delivery finishes.
func (n *Notifier) Notify(ctx context.Context, delivery Delivery) int {
	if n == nil || n.registry == nil {
		return 0
	}
	if n.registry.Durable() {
		// A durable lookup may wait on object storage. It must never occupy the
		// workflow projection activity, because notifications are observability
		// and a registry outage cannot stall a run transition.
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			hooks, err := n.registry.MatchingContext(lookupCtx, delivery.match())
			cancel()
			if err != nil {
				n.logger.Error("webhook lookup failed", "event", delivery.Event, "run_id", delivery.RunID, "error", err)
				return
			}
			n.queue(ctx, delivery, hooks)
		}()
		return 0
	}
	// Registry lookup is deliberately bounded and detached from the workflow
	// transition. A shared-store outage loses only this best-effort fanout; it
	// cannot delay or fail a run.
	lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	hooks, err := n.registry.MatchingContext(lookupCtx, delivery.match())
	cancel()
	if err != nil {
		n.logger.Error("webhook lookup failed", "event", delivery.Event, "run_id", delivery.RunID, "error", err)
		return 0
	}
	if len(hooks) == 0 {
		return 0
	}
	return n.queue(ctx, delivery, hooks)
}

// queue starts one delivery per supplied hook. Lookup has already completed;
// this routine has no storage operation and returns immediately.
func (n *Notifier) queue(ctx context.Context, delivery Delivery, hooks []Webhook) int {
	if delivery.Timestamp.IsZero() {
		delivery.Timestamp = time.Now().UTC()
	}
	detached := context.WithoutCancel(ctx)
	queued := 0
	for _, hook := range hooks {
		if hook.Secret == "" {
			// A stored subscription always has a secret (registration
			// generates one), so this can only mean the read path lost it.
			// It is a diagnostic, not a refusal: the delivery still goes out.
			n.logger.Warn("webhook delivery unsigned",
				"webhook_id", hook.ID, "event", string(delivery.Event), "run_id", delivery.RunID)
		}
		id, err := n.newID()
		if err != nil {
			n.logger.Error("webhook delivery id", "webhook_id", hook.ID, "error", err)
			continue
		}
		one := delivery
		one.ID = id
		queued++

		n.wg.Add(1)
		go func(hook Webhook, one Delivery) {
			defer n.wg.Done()
			n.deliver(detached, hook, one)
		}(hook, one)
	}
	return queued
}

// deliver performs one delivery and reports it.
func (n *Notifier) deliver(ctx context.Context, hook Webhook, delivery Delivery) {
	result, err := n.dispatcher.Deliver(ctx, hook, delivery)
	if err != nil {
		n.logger.Error("webhook delivery failed",
			"webhook_id", hook.ID, "delivery_id", delivery.ID, "url", hook.URL,
			"event", string(delivery.Event), "run_id", delivery.RunID,
			"attempts", len(result.Attempts), "error", err)
	} else {
		n.logger.Info("webhook delivered",
			"webhook_id", hook.ID, "delivery_id", delivery.ID, "url", hook.URL,
			"event", string(delivery.Event), "run_id", delivery.RunID,
			"attempts", len(result.Attempts), "status", result.LastAttempt().StatusCode)
	}

	n.mu.Lock()
	observers := append([]func(Result){}, n.observers...)
	n.mu.Unlock()
	for _, observe := range observers {
		observe(result)
	}
}

// Wait blocks until every queued delivery has settled, or ctx ends. It returns
// ctx.Err() in the second case, so a shutdown can bound how long it waits on a
// subscriber that is not answering.
func (n *Notifier) Wait(ctx context.Context) error {
	if n == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		n.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// match is what the registry filters a delivery on.
func (d Delivery) match() Match {
	return Match{Event: d.Event, Namespace: d.Namespace, OrkflowName: d.OrkflowName, TaskID: d.TaskID}
}
