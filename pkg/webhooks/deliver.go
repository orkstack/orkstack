package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Delivery is one run transition on its way to one subscriber.
type Delivery struct {
	// ID is unique per (transition, subscription) pair and stays the same
	// across retries, so a receiver can deduplicate.
	ID string
	// Event is the transition that produced this delivery.
	Event Event
	// RunID, OrkflowName, Status and Timestamp are the payload's flat header.
	RunID       string
	OrkflowName string
	Status      string
	Timestamp   time.Time
	// Run is the full run, already encoded in OrkStack's JSON dialect.
	Run json.RawMessage
}

// payload is the wire body, in the field order the contract states:
//
//	{"runId":…,"orkflowName":…,"status":…,"timestamp":…,"run":{…}}
type payload struct {
	RunID       string          `json:"runId"`
	OrkflowName string          `json:"orkflowName"`
	Status      string          `json:"status"`
	Timestamp   string          `json:"timestamp"`
	Run         json.RawMessage `json:"run"`
}

// Body renders the signed request body. It is computed once per delivery and
// reused by every attempt, so the signature is stable across retries.
func (d Delivery) Body() ([]byte, error) {
	body, err := json.Marshal(payload{
		RunID:       d.RunID,
		OrkflowName: d.OrkflowName,
		Status:      d.Status,
		Timestamp:   d.Timestamp.UTC().Format(time.RFC3339Nano),
		Run:         d.Run,
	})
	if err != nil {
		return nil, fmt.Errorf("webhooks: encode delivery %s: %w", d.ID, err)
	}
	return body, nil
}

// Retry budget. "Max 3 retries" means up to three re-sends after the first
// attempt, so at most four requests, spaced 100ms, 200ms, 400ms.
const (
	// DefaultMaxRetries is how many times a failed delivery is re-sent.
	DefaultMaxRetries = 3
	// DefaultInitialBackoff is the pause before the first retry.
	DefaultInitialBackoff = 100 * time.Millisecond
	// DefaultBackoffCoefficient multiplies the pause after each retry.
	DefaultBackoffCoefficient = 2.0
	// DefaultRequestTimeout bounds one attempt.
	DefaultRequestTimeout = 10 * time.Second
	// maxResponseBytes bounds what is read from a receiver for diagnostics.
	maxResponseBytes = 2 << 10
)

// Attempt records one POST.
type Attempt struct {
	// Number counts from 1.
	Number int `json:"number"`
	// StatusCode is the response status, or 0 when the request never
	// completed.
	StatusCode int `json:"statusCode"`
	// Error is the transport failure, if any.
	Error string `json:"error,omitempty"`
	// Backoff is the pause taken before this attempt.
	Backoff time.Duration `json:"backoff,omitempty"`
	// Duration is how long the request itself took.
	Duration time.Duration `json:"duration"`
}

// Result is the outcome of a delivery: whether it landed, and every attempt it
// took. It is the record an operator reads when a subscriber says it saw
// nothing.
type Result struct {
	DeliveryID string    `json:"deliveryId"`
	WebhookID  string    `json:"webhookId"`
	Event      Event     `json:"event"`
	RunID      string    `json:"runId"`
	URL        string    `json:"url"`
	Delivered  bool      `json:"delivered"`
	Signature  string    `json:"signature,omitempty"`
	Attempts   []Attempt `json:"attempts"`
}

// LastAttempt returns the attempt that ended the delivery.
func (r Result) LastAttempt() Attempt {
	if len(r.Attempts) == 0 {
		return Attempt{}
	}
	return r.Attempts[len(r.Attempts)-1]
}

// DispatcherOptions tunes delivery. Zero values take the defaults above.
type DispatcherOptions struct {
	// Client sends the requests. Nil builds one bounded by RequestTimeout.
	Client *http.Client
	// MaxRetries caps re-sends after the first attempt. Negative means none.
	MaxRetries int
	// InitialBackoff is the pause before the first retry.
	InitialBackoff time.Duration
	// BackoffCoefficient multiplies the pause after each retry.
	BackoffCoefficient float64
	// RequestTimeout bounds one attempt.
	RequestTimeout time.Duration
	// Sleep waits out a backoff. Tests replace it to keep the retry ladder
	// observable without spending real time.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Dispatcher POSTs one delivery to one subscriber, signed, with the retry
// budget. It holds no state, so one instance serves every subscription.
type Dispatcher struct {
	client         *http.Client
	maxRetries     int
	initialBackoff time.Duration
	coefficient    float64
	sleep          func(ctx context.Context, d time.Duration) error
}

// NewDispatcher returns a dispatcher with defaults filled in.
func NewDispatcher(options DispatcherOptions) *Dispatcher {
	d := &Dispatcher{
		client:         options.Client,
		maxRetries:     options.MaxRetries,
		initialBackoff: options.InitialBackoff,
		coefficient:    options.BackoffCoefficient,
		sleep:          options.Sleep,
	}
	if options.MaxRetries == 0 {
		d.maxRetries = DefaultMaxRetries
	}
	if d.maxRetries < 0 {
		d.maxRetries = 0
	}
	if d.initialBackoff <= 0 {
		d.initialBackoff = DefaultInitialBackoff
	}
	if d.coefficient <= 0 {
		d.coefficient = DefaultBackoffCoefficient
	}
	if d.client == nil {
		timeout := options.RequestTimeout
		if timeout <= 0 {
			timeout = DefaultRequestTimeout
		}
		d.client = &http.Client{Timeout: timeout}
	}
	if d.sleep == nil {
		d.sleep = sleepContext
	}
	return d
}

// Deliver POSTs the delivery to hook, retrying a transport failure or a 5xx
// with exponential backoff. It returns the full attempt record; the error is
// non-nil only when the delivery never landed, so a caller can log one and
// keep the other.
func (d *Dispatcher) Deliver(ctx context.Context, hook Webhook, delivery Delivery) (Result, error) {
	body, err := delivery.Body()
	if err != nil {
		return Result{DeliveryID: delivery.ID, WebhookID: hook.ID, URL: hook.URL}, err
	}
	signature := Sign(hook.Secret, body)
	result := Result{
		DeliveryID: delivery.ID,
		WebhookID:  hook.ID,
		Event:      delivery.Event,
		RunID:      delivery.RunID,
		URL:        hook.URL,
		Signature:  signature,
	}

	var lastErr error
	for attempt := 1; attempt <= d.maxRetries+1; attempt++ {
		backoff := time.Duration(0)
		if attempt > 1 {
			backoff = d.backoffFor(attempt)
			if err := d.sleep(ctx, backoff); err != nil {
				result.Attempts = append(result.Attempts, Attempt{
					Number: attempt, Backoff: backoff, Error: err.Error(),
				})
				return result, fmt.Errorf("webhooks: deliver %s to %s: %w", delivery.ID, hook.URL, err)
			}
		}

		started := time.Now()
		status, sendErr := d.send(ctx, hook, delivery, body, signature, attempt)
		record := Attempt{
			Number:     attempt,
			StatusCode: status,
			Backoff:    backoff,
			Duration:   time.Since(started),
		}
		if sendErr != nil {
			record.Error = sendErr.Error()
		}
		result.Attempts = append(result.Attempts, record)

		switch {
		case sendErr == nil && status >= 200 && status < 300:
			result.Delivered = true
			return result, nil
		case sendErr != nil:
			lastErr = sendErr
		default:
			lastErr = fmt.Errorf("receiver answered %d", status)
		}
		if !retryable(status, sendErr) {
			break
		}
	}
	return result, fmt.Errorf("webhooks: deliver %s to %s after %d attempt(s): %w",
		delivery.ID, hook.URL, len(result.Attempts), lastErr)
}

// send performs one attempt and returns the response status.
func (d *Dispatcher) send(
	ctx context.Context,
	hook Webhook,
	delivery Delivery,
	body []byte,
	signature string,
	attempt int,
) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hook.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set(EventHeader, string(delivery.Event))
	req.Header.Set(DeliveryHeader, delivery.ID)
	req.Header.Set(WebhookHeader, hook.ID)
	req.Header.Set(RunHeader, delivery.RunID)
	req.Header.Set(TimestampHeader, delivery.Timestamp.UTC().Format(time.RFC3339Nano))
	req.Header.Set(AttemptHeader, strconv.Itoa(attempt))
	if signature != "" {
		req.Header.Set(SignatureHeader, signature)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Drain a bounded prefix so the connection can be reused, and discard it:
	// a receiver's body is not part of the contract.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
	return resp.StatusCode, nil
}

// backoffFor returns the pause before attempt n (n >= 2): initial * coeff^(n-2).
func (d *Dispatcher) backoffFor(attempt int) time.Duration {
	backoff := d.initialBackoff
	for i := 2; i < attempt; i++ {
		backoff = time.Duration(float64(backoff) * d.coefficient)
	}
	return backoff
}

// retryable decides whether a failed attempt is worth repeating. A transport
// failure and a 5xx are the receiver's problem and usually transient; a 4xx is
// this sender's problem and repeating it only doubles the damage.
func retryable(status int, err error) bool {
	if err != nil {
		return true
	}
	return status >= 500
}

// sleepContext waits for d unless ctx ends first.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
