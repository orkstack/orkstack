package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// A webhook delivery is one short Temporal workflow, so its retries are
// Temporal's (AGENTS.md §2.1) and a delivery outlives the engine replica that
// queued it: the notifier stores the signed body, starts the workflow, and
// returns. The workflow makes the POST as an activity whose retry policy is
// the delivery's ladder, then settles it — writes the immutable delivery
// record and deletes the body — as a second activity. Both run on the engine's
// own channel, never on a pool an Orker polls. Delivery stays best-effort: no
// run waits for it, and nothing it does can fail a run.
const (
	// WebhookDeliveryWorkflowType is the delivery workflow's registered name.
	WebhookDeliveryWorkflowType = "WebhookDeliveryWorkflow"
	// ActivitySendWebhookAttempt makes one POST.
	ActivitySendWebhookAttempt = "SendWebhookAttempt"
	// ActivitySettleWebhookDelivery records a settled delivery and deletes
	// its body.
	ActivitySettleWebhookDelivery = "SettleWebhookDelivery"
	// WebhookAttemptFailedType is the error type of an attempt that failed in
	// a way worth repeating, which is what asks Temporal for the next one.
	WebhookAttemptFailedType = "WebhookAttemptFailed"
)

// WebhookDeliveryPolicy is every number that decides what a delivery's
// workflow commands. It is pinned into each delivery when it is queued, so a
// release that changes a default moves only deliveries queued after it
// (AGENTS.md §7.2).
type WebhookDeliveryPolicy struct {
	// MaxAttempts, InitialInterval, BackoffCoefficient and MaxInterval are the
	// send activity's retry policy: the ladder.
	MaxAttempts        int32         `json:"maxAttempts"`
	InitialInterval    time.Duration `json:"initialInterval"`
	BackoffCoefficient float64       `json:"backoffCoefficient"`
	MaxInterval        time.Duration `json:"maxInterval"`
	// AttemptTimeout bounds one attempt's activity: the request and the
	// reads around it.
	AttemptTimeout time.Duration `json:"attemptTimeout"`
	// SettleAttempts and SettleTimeout bound writing the record.
	SettleAttempts int32         `json:"settleAttempts"`
	SettleTimeout  time.Duration `json:"settleTimeout"`
}

// DefaultWebhookDeliveryPolicy is the ladder docs/WEBHOOKS.md states: four
// attempts, 100ms, 200ms and 400ms apart.
func DefaultWebhookDeliveryPolicy() WebhookDeliveryPolicy {
	return WebhookDeliveryPolicy{
		MaxAttempts:        webhooks.DefaultMaxAttempts,
		InitialInterval:    webhooks.DefaultInitialBackoff,
		BackoffCoefficient: webhooks.DefaultBackoffCoefficient,
		MaxInterval:        webhooks.DefaultMaxBackoff,
		AttemptTimeout:     webhooks.DefaultRequestTimeout + 5*time.Second,
		SettleAttempts:     10,
		SettleTimeout:      30 * time.Second,
	}
}

// WebhookDeliveryInput is a delivery workflow's whole input. It never carries
// the body (the run is too large for a payload, AGENTS.md §2.2) or the
// signing secret (history is no place for one): the body is in the store under
// BodySHA256, and each attempt reads the secret from the registry.
type WebhookDeliveryInput struct {
	// Delivery is the delivery's coordinates, with no Run.
	Delivery   webhooks.Delivery     `json:"delivery"`
	WebhookID  string                `json:"webhookId"`
	URL        string                `json:"url"`
	BodySHA256 string                `json:"bodySha256"`
	BodyBytes  int                   `json:"bodyBytes"`
	Policy     WebhookDeliveryPolicy `json:"policy"`
}

// WebhookDeliveryWorkflow sends one delivery and settles it. It returns nil
// on every path: a delivery that never landed is a record that says so, not a
// failed workflow.
func WebhookDeliveryWorkflow(ctx workflow.Context, in WebhookDeliveryInput) error {
	policy := in.Policy
	sendCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           EventQueue,
		StartToCloseTimeout: policy.AttemptTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    policy.InitialInterval,
			BackoffCoefficient: policy.BackoffCoefficient,
			MaximumInterval:    policy.MaxInterval,
			MaximumAttempts:    policy.MaxAttempts,
		},
	})
	var result webhooks.Result
	if err := workflow.ExecuteActivity(sendCtx, ActivitySendWebhookAttempt, in).Get(ctx, &result); err != nil {
		// No attempt returned a result: the last one died or timed out.
		result = unsentWebhookResult(in, err, workflow.Now(ctx))
	}
	settleCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           EventQueue,
		StartToCloseTimeout: policy.SettleTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: policy.SettleAttempts},
	})
	if err := workflow.ExecuteActivity(settleCtx, ActivitySettleWebhookDelivery, in, result).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Warn("webhook delivery record dropped",
			"delivery_id", in.Delivery.ID, "webhook_id", in.WebhookID, "delivered", result.Delivered, "error", err)
	}
	return nil
}

// unsentWebhookResult is the record of a delivery whose last attempt returned
// nothing — its engine replica died, or it outran AttemptTimeout: the attempts
// the earlier ones reported, when they were kept, and one row naming why the
// last did not finish. It reads only the input, the error and the workflow's
// clock, so it is deterministic.
func unsentWebhookResult(in WebhookDeliveryInput, cause error, now time.Time) webhooks.Result {
	result := webhooks.NewResult(webhooks.Webhook{ID: in.WebhookID, URL: in.URL}, in.Delivery, nil)
	result.BodySHA256, result.BodyBytes, result.Signature = in.BodySHA256, in.BodyBytes, ""
	var timeout *temporal.TimeoutError
	if errors.As(cause, &timeout) && timeout.HasLastHeartbeatDetails() {
		var attempts []webhooks.Attempt
		if timeout.LastHeartbeatDetails(&attempts) == nil {
			result.Attempts = attempts
		}
	}
	// The record is user-facing: it names what happened in OrkStack's words,
	// never the cluster's error (AGENTS.md §7).
	reason := "the last attempt ended without reporting its outcome"
	if timeout != nil {
		reason = fmt.Sprintf("the last attempt did not finish within %s", in.Policy.AttemptTimeout)
	}
	result.Attempts = append(result.Attempts, webhooks.Attempt{
		Number: len(result.Attempts) + 1,
		Error:  reason,
		At:     now.UTC(),
	})
	result.SettledAt = now.UTC()
	return result
}

// WebhookDeliveryActivities are a delivery's two activities. They run on the
// engine's own channel, where any replica may take them.
type WebhookDeliveryActivities struct {
	bodies     artifacts.DeliveryBodyStore
	registry   *webhooks.Registry
	dispatcher *webhooks.Dispatcher
	notifier   *webhooks.Notifier
	logger     *slog.Logger
}

// NewWebhookDeliveryActivities wires the activities to the body store, the
// registry that holds each subscription's secret, the dispatcher that makes
// one POST, and the notifier whose Settle writes the record.
func NewWebhookDeliveryActivities(
	bodies artifacts.DeliveryBodyStore,
	registry *webhooks.Registry,
	dispatcher *webhooks.Dispatcher,
	notifier *webhooks.Notifier,
	logger *slog.Logger,
) *WebhookDeliveryActivities {
	if dispatcher == nil {
		dispatcher = webhooks.NewDispatcher(webhooks.DispatcherOptions{})
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &WebhookDeliveryActivities{bodies: bodies, registry: registry, dispatcher: dispatcher, notifier: notifier, logger: logger}
}

// SendWebhookAttempt makes attempt number activity.GetInfo(ctx).Attempt.
//
// It returns a result — ending the ladder — when the attempt landed, when it
// failed in a way repeating cannot fix (a 4xx, a subscription deleted, a
// body gone), or when it was the last the policy allows. Otherwise it records
// every attempt so far as its heartbeat, which Temporal hands the next
// attempt, and returns WebhookAttemptFailedType, which asks Temporal for that
// next attempt after the policy's pause. Nothing here waits or loops.
func (a *WebhookDeliveryActivities) SendWebhookAttempt(ctx context.Context, in WebhookDeliveryInput) (webhooks.Result, error) {
	n := int(activity.GetInfo(ctx).Attempt)
	var prior []webhooks.Attempt
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &prior); err != nil {
			prior = nil
		}
	}
	delivery := in.Delivery
	result := webhooks.NewResult(webhooks.Webhook{ID: in.WebhookID, URL: in.URL}, delivery, nil)
	result.BodySHA256, result.BodyBytes, result.Signature = in.BodySHA256, in.BodyBytes, ""

	attempt, final := a.attempt(ctx, in, &result, n)
	if len(prior) > 0 {
		last := prior[len(prior)-1]
		if pause := attempt.At.Sub(last.At.Add(last.Duration)); pause > 0 {
			attempt.Backoff = pause
		}
	}
	result.Attempts = append(prior, attempt)
	result.Delivered = attempt.Delivered()
	if result.Delivered || final || !attempt.Retryable() || int32(n) >= in.Policy.MaxAttempts {
		result.SettledAt = time.Now().UTC()
		return result, nil
	}
	activity.RecordHeartbeat(ctx, result.Attempts)
	return webhooks.Result{}, temporal.NewApplicationError(
		fmt.Sprintf("webhook delivery %s attempt %d to %s: %v", delivery.ID, n, in.URL, attempt.Failure()),
		WebhookAttemptFailedType)
}

// attempt makes one attempt, or records why none could be made. final says
// a repeat cannot help.
func (a *WebhookDeliveryActivities) attempt(ctx context.Context, in WebhookDeliveryInput, result *webhooks.Result, n int) (webhooks.Attempt, bool) {
	now := func() time.Time { return time.Now().UTC() }
	body, err := a.bodies.GetDeliveryBody(ctx, in.Delivery.ID, in.BodySHA256)
	switch {
	case errors.Is(err, artifacts.ErrNotFound), errors.Is(err, artifacts.ErrDeliveryBodyAltered):
		return webhooks.Attempt{Number: n, At: now(), Error: "no request was made: the delivery's stored body cannot be read back: " + err.Error()}, true
	case err != nil:
		return webhooks.Attempt{Number: n, At: now(), Error: "no request was made: reading the delivery's stored body: " + err.Error()}, false
	}
	hook, found, err := a.registry.LookupForDelivery(ctx, in.WebhookID)
	switch {
	case err != nil:
		return webhooks.Attempt{Number: n, At: now(), Error: "no request was made: reading the subscription's signing secret: " + err.Error()}, false
	case !found:
		return webhooks.Attempt{Number: n, At: now(), Error: "no request was made: the subscription was deleted"}, true
	}
	// The URL the delivery was queued for, signed with the subscription's
	// secret, which never changes.
	hook.URL = in.URL
	*result = webhooks.NewResult(hook, in.Delivery, body)
	return a.dispatcher.Send(ctx, hook, in.Delivery, body, result.Signature, n), false
}

// SettleWebhookDelivery writes a settled delivery's record, then deletes its
// body. A record that already exists is a retried settle's success, so the
// write is idempotent; an observer runs once the record exists.
func (a *WebhookDeliveryActivities) SettleWebhookDelivery(ctx context.Context, in WebhookDeliveryInput, result webhooks.Result) error {
	if err := a.notifier.Settle(ctx, result); err != nil {
		return fmt.Errorf("record webhook delivery %s: %w", in.Delivery.ID, err)
	}
	if result.Delivered {
		a.logger.Info("webhook delivered",
			"webhook_id", in.WebhookID, "delivery_id", in.Delivery.ID, "url", in.URL,
			"event", string(in.Delivery.Event), "run_id", in.Delivery.RunID,
			"attempts", len(result.Attempts), "status", result.LastAttempt().StatusCode)
	} else {
		a.logger.Error("webhook delivery failed",
			"webhook_id", in.WebhookID, "delivery_id", in.Delivery.ID, "url", in.URL,
			"event", string(in.Delivery.Event), "run_id", in.Delivery.RunID,
			"attempts", len(result.Attempts), "error", result.LastAttempt().Failure())
	}
	if err := a.bodies.DeleteDeliveryBody(ctx, in.Delivery.ID, in.BodySHA256); err != nil {
		return fmt.Errorf("delete the body of webhook delivery %s: %w", in.Delivery.ID, err)
	}
	return nil
}

// WebhookLauncher queues each delivery durably: it stores the signed body and
// starts the delivery's workflow, whose id is the delivery id, so a delivery
// is started at most once.
type WebhookLauncher struct {
	client client.Client
	queue  string
	bodies artifacts.DeliveryBodyStore
	policy WebhookDeliveryPolicy
}

var _ webhooks.Launcher = (*WebhookLauncher)(nil)

// NewWebhookLauncher starts delivery workflows on queue, the engine's run
// channel, with policy pinned into each.
func NewWebhookLauncher(c client.Client, queue string, bodies artifacts.DeliveryBodyStore, policy WebhookDeliveryPolicy) *WebhookLauncher {
	return &WebhookLauncher{client: c, queue: queue, bodies: bodies, policy: policy}
}

// Launch stores the body and starts the workflow. A body stored for a
// workflow that could not start is deleted again.
func (l *WebhookLauncher) Launch(ctx context.Context, hook webhooks.Webhook, delivery webhooks.Delivery) error {
	body, err := delivery.Body()
	if err != nil {
		return &webhooks.NotQueuedError{Reason: "its body could not be encoded", Err: err}
	}
	sum, err := l.bodies.PutDeliveryBody(ctx, delivery.ID, body)
	if err != nil {
		return &webhooks.NotQueuedError{Reason: "its body could not be stored", Err: err}
	}
	in := WebhookDeliveryInput{
		Delivery: delivery, WebhookID: hook.ID, URL: hook.URL,
		BodySHA256: sum, BodyBytes: len(body), Policy: l.policy,
	}
	in.Delivery.Run = nil
	_, err = l.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    delivery.ID,
		TaskQueue:             l.queue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, WebhookDeliveryWorkflowType, in)
	if err != nil {
		// Detached: the caller's deadline may be what failed the start.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = l.bodies.DeleteDeliveryBody(cleanup, delivery.ID, sum)
		return &webhooks.NotQueuedError{Reason: "the engine could not start it", Err: err}
	}
	return nil
}
