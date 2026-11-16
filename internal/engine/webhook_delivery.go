package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
// the body (the run is too large for a payload, AGENTS.md §2.2), the signing
// secret or, once every engine reads it from the registry, the URL (history
// is no place for either: a webhook URL often carries a credential): the body
// is in the store under BodySHA256, and each attempt reads the subscription —
// its URL and its secret — from the registry by WebhookID.
type WebhookDeliveryInput struct {
	// Delivery is the delivery's coordinates, with no Run.
	Delivery  webhooks.Delivery `json:"delivery"`
	WebhookID string            `json:"webhookId"`
	// URL is set only on a delivery queued while an engine process that could
	// run its attempts lacked CapabilityWebhookByID, or by a release before
	// it: such a process sends to this URL. This build never sends to it; when
	// set, the record of a subscription deleted before it settled names it.
	URL        string                `json:"url,omitempty"`
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
	result := webhooks.NewResult(webhooks.Webhook{ID: in.WebhookID}, in.Delivery, nil)
	result.BodySHA256, result.BodyBytes, result.Signature = in.BodySHA256, in.BodyBytes, ""
	failure := categoryOf(cause)
	var attempts []webhooks.Attempt
	if failure.heartbeatDetails(&attempts) {
		result.Attempts = attempts
	}
	// The record is user-facing: it names what happened in OrkStack's words,
	// never the cluster's error (AGENTS.md §7).
	reason := "the last attempt ended without reporting its outcome"
	if failure.timedOut() {
		reason = fmt.Sprintf("the last attempt did not finish within %s", in.Policy.AttemptTimeout)
	}
	result.Attempts = append(result.Attempts, webhooks.Attempt{
		Number: len(result.Attempts) + 1,
		Code:   webhooks.AttemptCodeUnreported,
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
//
// What it returns, heartbeats and fails with is held by the cluster, so it
// names no URL and none of the transport's words: an attempt is a code and
// OrkStack's sentence (webhooks.AttemptCode*), and the transport's error goes
// to this process's log only. The record's URL is filled in when it settles.
func (a *WebhookDeliveryActivities) SendWebhookAttempt(ctx context.Context, in WebhookDeliveryInput) (webhooks.Result, error) {
	n := int(activity.GetInfo(ctx).Attempt)
	var prior []webhooks.Attempt
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &prior); err != nil {
			// The record then starts at this attempt; its number still says
			// how many came before.
			a.logger.Warn("webhook delivery's earlier attempts could not be read back",
				"webhook_id", in.WebhookID, "delivery_id", in.Delivery.ID, "attempt", n, "error", err)
			prior = nil
		}
	}
	delivery := in.Delivery
	result := webhooks.NewResult(webhooks.Webhook{ID: in.WebhookID}, delivery, nil)
	result.BodySHA256, result.BodyBytes, result.Signature = in.BodySHA256, in.BodyBytes, ""

	attempt, final := a.attempt(ctx, in, &result, n)
	// The result goes through the cluster: no URL, and no signature, which
	// with the stored body would let a holder of history guess the secret.
	// SettleWebhookDelivery puts both back into the record.
	result.URL, result.Signature = "", ""
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
		fmt.Sprintf("webhook delivery %s attempt %d did not land: %v", delivery.ID, n, attempt.Failure()),
		WebhookAttemptFailedType)
}

// attempt makes one attempt, or records why none could be made. final says
// a repeat cannot help.
func (a *WebhookDeliveryActivities) attempt(ctx context.Context, in WebhookDeliveryInput, result *webhooks.Result, n int) (webhooks.Attempt, bool) {
	unsent := func(code, sentence string, cause error) webhooks.Attempt {
		if cause != nil {
			a.logger.Warn("webhook delivery attempt made no request",
				"webhook_id", in.WebhookID, "delivery_id", in.Delivery.ID, "attempt", n, "code", code, "error", cause)
		}
		return webhooks.Attempt{Number: n, At: time.Now().UTC(), Code: code, Error: "no request was made: " + sentence}
	}
	body, err := a.bodies.GetDeliveryBody(ctx, in.Delivery.ID, in.BodySHA256)
	switch {
	case errors.Is(err, artifacts.ErrNotFound), errors.Is(err, artifacts.ErrDeliveryBodyAltered):
		return unsent(webhooks.AttemptCodeBodyLost, "the delivery's stored body cannot be read back", err), true
	case err != nil:
		return unsent(webhooks.AttemptCodeStoreUnavailable, "the delivery's stored body could not be read", err), false
	}
	hook, found, err := a.registry.LookupForDelivery(ctx, in.WebhookID)
	switch {
	case err != nil:
		return unsent(webhooks.AttemptCodeSubscriptionUnavailable, "the subscription could not be read", err), false
	case !found:
		return unsent(webhooks.AttemptCodeSubscriptionDeleted, "the subscription was deleted", nil), true
	}
	// The subscription as the registry holds it now: its URL and its secret,
	// read by id, never from the delivery's input.
	*result = webhooks.NewResult(hook, in.Delivery, body)
	attempt := a.dispatcher.Send(ctx, hook, in.Delivery, body, result.Signature, n)
	if cause := attempt.Cause(); cause != nil {
		a.logger.Warn("webhook delivery attempt failed",
			"webhook_id", in.WebhookID, "delivery_id", in.Delivery.ID, "attempt", n, "code", attempt.Code, "error", cause)
	}
	return attempt, false
}

// SettleWebhookDelivery writes a settled delivery's record, then deletes its
// body. A record that already exists is a retried settle's success, so the
// write is idempotent; an observer runs once the record exists.
//
// The result it is handed came through the cluster, so it names no URL; the
// record, in object storage, names the subscription's URL as the registry
// holds it now, or the input's for a subscription since deleted.
func (a *WebhookDeliveryActivities) SettleWebhookDelivery(ctx context.Context, in WebhookDeliveryInput, result webhooks.Result) error {
	ids := []any{"webhook_id", in.WebhookID, "delivery_id", in.Delivery.ID}
	hook, found, err := a.registry.LookupForDelivery(ctx, in.WebhookID)
	switch {
	case err != nil && !lastSettle(ctx, in.Policy):
		return fmt.Errorf("read webhook %s for the record of delivery %s: %w", in.WebhookID, in.Delivery.ID, err)
	case err != nil:
		// The record matters more than its URL: the last attempt writes it
		// without the URL and the signature rather than lose it.
		a.logger.Warn("webhook delivery recorded without its URL and signature: the subscription could not be read",
			append(ids, "error", err)...)
		result.URL = in.URL
	case found:
		result.URL = hook.URL
	default:
		a.logger.Info("webhook delivery recorded for a subscription deleted before it settled", ids...)
		result.URL = in.URL
	}
	if result.Signature == "" && found && sentRequest(result.Attempts) {
		// Every request sent this body signed with this secret, which never
		// changes.
		body, err := a.bodies.GetDeliveryBody(ctx, in.Delivery.ID, in.BodySHA256)
		switch {
		case err == nil:
			result.Signature = webhooks.Sign(hook.Secret, body)
		case errors.Is(err, artifacts.ErrNotFound):
			// A settle retried after it deleted the body.
			a.logger.Warn("webhook delivery recorded without its signature: its stored body is already deleted", ids...)
		case errors.Is(err, artifacts.ErrDeliveryBodyAltered):
			a.logger.Error("webhook delivery recorded without its signature: its stored body does not match its digest",
				append(ids, "error", err)...)
		default:
			return fmt.Errorf("read the body of webhook delivery %s for its record: %w", in.Delivery.ID, err)
		}
	}
	if err := a.notifier.Settle(ctx, result); err != nil {
		return fmt.Errorf("record webhook delivery %s: %w", in.Delivery.ID, err)
	}
	if result.Delivered {
		a.logger.Info("webhook delivered",
			"webhook_id", in.WebhookID, "delivery_id", in.Delivery.ID,
			"event", string(in.Delivery.Event), "run_id", in.Delivery.RunID,
			"attempts", len(result.Attempts), "status", result.LastAttempt().StatusCode)
	} else {
		a.logger.Error("webhook delivery failed",
			"webhook_id", in.WebhookID, "delivery_id", in.Delivery.ID,
			"event", string(in.Delivery.Event), "run_id", in.Delivery.RunID,
			"attempts", len(result.Attempts), "code", result.LastAttempt().Code, "error", result.LastAttempt().Failure())
	}
	if err := a.bodies.DeleteDeliveryBody(ctx, in.Delivery.ID, in.BodySHA256); err != nil {
		return fmt.Errorf("delete the body of webhook delivery %s: %w", in.Delivery.ID, err)
	}
	return nil
}

// lastSettle reports whether this is the settle's last attempt under policy.
func lastSettle(ctx context.Context, policy WebhookDeliveryPolicy) bool {
	return policy.SettleAttempts > 0 && activity.GetInfo(ctx).Attempt >= policy.SettleAttempts
}

// sentRequest reports whether any attempt sent the body: one the receiver
// answered, or one that failed on the way there. An attempt that read no
// body or no subscription, or never reported, sent nothing.
func sentRequest(attempts []webhooks.Attempt) bool {
	for _, attempt := range attempts {
		switch attempt.Code {
		case webhooks.AttemptCodeBodyLost, webhooks.AttemptCodeStoreUnavailable,
			webhooks.AttemptCodeSubscriptionUnavailable, webhooks.AttemptCodeSubscriptionDeleted,
			webhooks.AttemptCodeUnreported:
			continue
		}
		return true
	}
	return false
}

// WebhookLauncher queues each delivery durably: it stores the signed body and
// starts the delivery's workflow, whose id is the delivery id, so a delivery
// is started at most once.
type WebhookLauncher struct {
	client client.Client
	queue  string
	bodies artifacts.DeliveryBodyStore
	policy WebhookDeliveryPolicy

	// Capabilities is the fleet check (CapabilityWebhookByID): a delivery
	// carries its URL only while it reports a process that could run the
	// delivery's attempts and lacks the capability, so that process sends it
	// somewhere. Nil, an error, or no such process leaves the URL out. Set it
	// before the first Launch.
	Capabilities CapabilityChecker
	// Logger reports that deliveries carry their URL, at most once a minute.
	Logger *slog.Logger

	mu        sync.Mutex
	verdict   bool      // the last good answer: carry the URL
	checkedAt time.Time // when that answer was given
	failedAt  time.Time // when the fleet check last failed
	checking  bool      // a fleet check is in flight
	warnedAt  time.Time
	clock     func() time.Time // time.Now; a test's clock
}

// webhookFleetCheckEvery is how long one fleet check's answer decides
// whether deliveries carry their URL. A delivery is queued per event per
// subscription, so asking the cluster for each would put two describes on
// every event; an older engine that starts polling within this window can be
// handed a URL-less delivery, whose attempt then fails and is retried.
const webhookFleetCheckEvery = 30 * time.Second

// webhookFleetRetryAfter is how soon a failed fleet check is asked again.
const webhookFleetRetryAfter = 5 * time.Second

// carryURL reports whether the delivery being queued must carry its URL.
//
// Once the fleet has answered, one check is in flight at a time, made without
// the lock, so a slow describe holds up no other Launch: while it runs, and
// after it fails, the last good answer decides. Until the fleet has answered
// once, each Launch asks for itself rather than guess. A check that fails
// with no good answer leaves the URL out, which keeps it out of the cluster;
// on a mixed fleet such a delivery's attempt on an older process fails
// (docs/DEPLOY.md §5). A failed check is asked again after
// webhookFleetRetryAfter.
func (l *WebhookLauncher) carryURL(ctx context.Context) bool {
	l.mu.Lock()
	now := time.Now()
	if l.clock != nil {
		now = l.clock()
	}
	checker := l.Capabilities
	answered := !l.checkedAt.IsZero()
	fresh := answered && now.Sub(l.checkedAt) < webhookFleetCheckEvery
	if checker == nil || fresh || (answered && l.checking) || (!l.failedAt.IsZero() && now.Sub(l.failedAt) < webhookFleetRetryAfter) {
		verdict := checker != nil && l.verdict
		l.mu.Unlock()
		return verdict
	}
	l.checking = true
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.checking = false
		l.mu.Unlock()
	}()

	lacking, err := checker.MissingCapability(ctx, CapabilityWebhookByID, ScopeWorkflowAndEvents)

	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.failedAt = now
	} else {
		l.verdict, l.checkedAt, l.failedAt = len(lacking) > 0, now, time.Time{}
	}
	if l.Logger != nil && (err != nil || l.verdict) && now.Sub(l.warnedAt) >= time.Minute {
		l.warnedAt = now
		if err != nil {
			// The last good answer stands; with none, the URL stays out of
			// the cluster, and a process of an older release, if any polls,
			// fails such an attempt and it is retried.
			l.Logger.Warn("webhook deliveries keep the fleet's last answer: the engine processes could not be listed",
				"carry_url", l.verdict, "error", err)
		} else {
			// The identities carry host names (pid@host): this log only.
			l.Logger.Warn("webhook deliveries carry their URL in the cluster: engine processes run a release that sends to it",
				"missing_capability", CapabilityWebhookByID, "identities", lacking)
		}
	}
	return l.verdict
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
		Delivery: delivery, WebhookID: hook.ID,
		BodySHA256: sum, BodyBytes: len(body), Policy: l.policy,
	}
	if l.carryURL(ctx) {
		in.URL = hook.URL
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
