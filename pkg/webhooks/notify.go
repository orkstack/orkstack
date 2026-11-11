package webhooks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"
)

// DeliveryIDLayout renders the instant a delivery was queued inside its id:
// UTC, fixed width, ordered the same lexicographically as chronologically.
// It is the layout pkg/artifacts uses for the run index (RunStampLayout),
// spelled here so this package stays free of that import.
const DeliveryIDLayout = "20060102T150405.000000000Z"

// deliveryIDPrefix and deliveryIDRandomBytes shape NewDeliveryID's output.
const (
	deliveryIDPrefix      = "dlv-"
	deliveryIDRandomBytes = 8
)

// deliveryIDPattern is the shape NewDeliveryID produces.
var deliveryIDPattern = regexp.MustCompile(`^dlv-\d{8}T\d{6}\.\d{9}Z-[0-9a-f]{16}$`)

// NewDeliveryID returns "dlv-" + queuedAt in DeliveryIDLayout + "-" + 16 hex
// characters from crypto/rand. The stamp makes a day's records list in the
// order they were queued; the random half keeps two replicas from minting
// one id. The receiver's X-Ork-Delivery is this value, and it is opaque to
// the receiver as it always was.
func NewDeliveryID(queuedAt time.Time) (string, error) {
	var raw [deliveryIDRandomBytes]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("webhooks: delivery id: %w", err)
	}
	return deliveryIDPrefix + queuedAt.UTC().Format(DeliveryIDLayout) + "-" + hex.EncodeToString(raw[:]), nil
}

// ValidDeliveryID reports whether id has the shape NewDeliveryID produces.
func ValidDeliveryID(id string) bool { return deliveryIDPattern.MatchString(id) }

// Notifier fans one run transition out to every subscription that wants it.
//
// Matching and queuing run off the caller's goroutine on purpose. The caller
// is the engine recording a run's state change, and a slow or unreachable
// subscriber, store or cluster must not hold that up: notification is
// observability, not orchestration.
//
// A Launcher hands each delivery to something durable — the engine's
// WebhookDeliveryWorkflow, whose activity Temporal retries — so a delivery
// outlives the process that queued it. Without one, the Notifier makes one
// attempt in-process and settles it, which is what a test or an embedding
// without a cluster gets; nothing here retries. Wait drains what is still
// being matched, queued or sent in this process at shutdown.
type Notifier struct {
	registry   *Registry
	dispatcher *Dispatcher
	launcher   Launcher
	record     func(ctx context.Context, result Result) error
	logger     *slog.Logger
	newID      func(queuedAt time.Time) (string, error)

	wg sync.WaitGroup

	mu        sync.Mutex
	observers []func(Result)
}

// Launcher queues one delivery durably: the engine stores its body and starts
// the workflow that sends it. An error means the delivery was not queued and
// will never be sent.
type Launcher interface {
	Launch(ctx context.Context, hook Webhook, delivery Delivery) error
}

// launchTimeout bounds queuing one delivery: storing its body and starting
// its workflow. Past it the delivery is not queued, and its record says so.
const launchTimeout = 10 * time.Second

// NotifierOptions tunes a Notifier.
type NotifierOptions struct {
	// Launcher queues each delivery durably. Nil makes one attempt in-process.
	Launcher Launcher
	// Record writes a settled delivery's immutable record. Nil records
	// nothing. Settle returns its error, so the engine's activity retries a
	// record that could not be written.
	Record func(ctx context.Context, result Result) error
	// Logger records delivery outcomes. Nil discards them.
	Logger *slog.Logger
	// NewDeliveryID generates delivery ids from the instant the delivery was
	// queued. Nil uses NewDeliveryID.
	NewDeliveryID func(queuedAt time.Time) (string, error)
}

// NewNotifier wires a registry to a dispatcher.
func NewNotifier(registry *Registry, dispatcher *Dispatcher, options NotifierOptions) *Notifier {
	n := &Notifier{
		registry:   registry,
		dispatcher: dispatcher,
		launcher:   options.Launcher,
		record:     options.Record,
		logger:     options.Logger,
		newID:      options.NewDeliveryID,
	}
	if n.dispatcher == nil {
		n.dispatcher = NewDispatcher(DispatcherOptions{})
	}
	if n.newID == nil {
		n.newID = NewDeliveryID
	}
	if n.logger == nil {
		n.logger = slog.New(slog.DiscardHandler)
	}
	return n
}

// Observe registers a callback invoked with every delivery result, after the
// delivery settles and its record is written. It is the seam tests and proof
// runs use to watch the wire without a real subscriber. It runs in the process
// that settled the delivery, which with a Launcher is whichever engine replica
// ran the delivery's last step.
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
// cancellation, because the activity that observed the transition returns
// before the delivery is queued.
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
	// One instant per event: every delivery of this transition carries it as
	// QueuedAt and inside its id, so an id's stamp and its record's
	// created_at are the same instant by construction.
	now := time.Now().UTC()
	queued := 0
	for _, hook := range hooks {
		if hook.Secret == "" {
			// A stored subscription always has a secret (registration
			// generates one), so this can only mean the read path lost it.
			// It is a diagnostic, not a refusal: the delivery still goes out.
			n.logger.Warn("webhook delivery unsigned",
				"webhook_id", hook.ID, "event", string(delivery.Event), "run_id", delivery.RunID)
		}
		id, err := n.newID(now)
		if err != nil {
			n.logger.Error("webhook delivery id", "webhook_id", hook.ID, "error", err)
			continue
		}
		one := delivery
		one.ID = id
		one.QueuedAt = now
		queued++

		n.wg.Add(1)
		go func(hook Webhook, one Delivery) {
			defer n.wg.Done()
			n.dispatch(detached, hook, one)
		}(hook, one)
	}
	return queued
}

// dispatch hands one delivery to the launcher, or makes its one in-process
// attempt and settles it.
func (n *Notifier) dispatch(ctx context.Context, hook Webhook, delivery Delivery) {
	if n.launcher == nil {
		result, err := n.dispatcher.Deliver(ctx, hook, delivery)
		n.logOutcome(hook, delivery, result, err)
		n.settleLogged(ctx, result)
		return
	}
	launchCtx, cancel := context.WithTimeout(ctx, launchTimeout)
	err := n.launcher.Launch(launchCtx, hook, delivery)
	cancel()
	if err == nil {
		return
	}
	// Never queued, so never sent: its record says why, so an operator who
	// asks what was delivered sees this one and can redeliver it.
	n.logger.Error("webhook delivery not queued",
		"webhook_id", hook.ID, "delivery_id", delivery.ID, "url", hook.URL,
		"event", string(delivery.Event), "run_id", delivery.RunID, "error", err)
	result := NotQueued(hook, delivery, err)
	n.settleLogged(ctx, result)
}

// NotQueuedError is a Launcher's refusal. Reason is what the delivery's
// record says, in OrkStack's own words; Err is the underlying failure, which
// only the engine's log names.
type NotQueuedError struct {
	Reason string
	Err    error
}

func (e *NotQueuedError) Error() string { return e.Reason + ": " + e.Err.Error() }

func (e *NotQueuedError) Unwrap() error { return e.Err }

// NotQueued is the record of a delivery that could not be queued: no attempt
// was made, and its one attempt row says why.
func NotQueued(hook Webhook, delivery Delivery, cause error) Result {
	reason := cause.Error()
	var refused *NotQueuedError
	if errors.As(cause, &refused) {
		reason = refused.Reason
	}
	body, bodyErr := delivery.Body()
	result := NewResult(hook, delivery, body)
	if bodyErr != nil {
		result.BodySHA256, result.BodyBytes, result.Signature = "", 0, ""
	}
	now := time.Now().UTC()
	result.Attempts = []Attempt{{Number: 1, Error: "the delivery could not be queued, so no request was made: " + reason, At: now}}
	result.SettledAt = now
	return result
}

// logOutcome logs one settled delivery.
func (n *Notifier) logOutcome(hook Webhook, delivery Delivery, result Result, err error) {
	if err != nil {
		n.logger.Error("webhook delivery failed",
			"webhook_id", hook.ID, "delivery_id", delivery.ID, "url", hook.URL,
			"event", string(delivery.Event), "run_id", delivery.RunID,
			"attempts", len(result.Attempts), "error", err)
		return
	}
	n.logger.Info("webhook delivered",
		"webhook_id", hook.ID, "delivery_id", delivery.ID, "url", hook.URL,
		"event", string(delivery.Event), "run_id", delivery.RunID,
		"attempts", len(result.Attempts), "status", result.LastAttempt().StatusCode)
}

// settleLogged settles a delivery this process cannot retry: a record that
// cannot be written is logged.
func (n *Notifier) settleLogged(ctx context.Context, result Result) {
	recordCtx, cancel := context.WithTimeout(ctx, launchTimeout)
	defer cancel()
	if err := n.Settle(recordCtx, result); err != nil {
		n.logger.Warn("webhook delivery record dropped",
			"delivery_id", result.DeliveryID, "webhook_id", result.WebhookID, "run_id", result.RunID,
			"event", string(result.Event), "delivered", result.Delivered, "error", err)
	}
}

// Settle writes a settled delivery's record and then hands the result to
// every observer. It returns the record's error, and calls no observer, when
// the record could not be written, so a caller that retries — the engine's
// settle activity — observes a delivery once its record exists. Writing a
// record that already exists is the recorder's success, so a retried Settle
// is safe.
func (n *Notifier) Settle(ctx context.Context, result Result) error {
	if n == nil {
		return nil
	}
	if n.record != nil {
		if err := n.record(ctx, result); err != nil {
			return err
		}
	}
	n.mu.Lock()
	observers := append([]func(Result){}, n.observers...)
	n.mu.Unlock()
	for _, observe := range observers {
		observe(result)
	}
	return nil
}

// Wait blocks until every delivery this process is matching, queuing or
// sending in-process has been handed off or settled, or ctx ends. It returns
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
