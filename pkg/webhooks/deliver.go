package webhooks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// Delivery is one run transition or task outcome on its way to one
// subscriber.
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
	// Sequence is the run event's sequence: monotonic per run, from 1, in the
	// order the run emitted its events. It is the body's "sequence" and
	// X-Ork-Sequence, so a receiver can order deliveries that arrive out of
	// order. A redelivery keeps the original's. Zero means the event carried
	// none, and the body and header then omit it rather than invent one. The
	// engine's delivery workflow carries a Delivery in its input, so a zero is
	// omitted there too: an input queued before deliveries carried a sequence
	// encodes as it was recorded (internal/engine/testdata/history).
	Sequence int64 `json:",omitzero"`
	// Namespace is the run's resolved namespace, used to match; it is not
	// part of the body (the embedded run already carries it).
	Namespace string
	// TaskID and Attempt are set for a task outcome event and empty/zero for
	// a run transition. They are the body's taskId and attempt; an attempt
	// Temporal has not numbered is zero and omitted, never invented.
	TaskID  string
	Attempt int32
	// Run is the full run, already encoded in OrkStack's JSON dialect.
	Run stablejson.RawJSON
	// QueuedAt is when the notifier queued this delivery; it is the instant the
	// id's stamp carries. Zero for a caller that bypasses the notifier.
	QueuedAt time.Time
	// RedeliveryOf names the delivery this one repeats, when an operator asked
	// for it. It is sent as X-Ork-Redelivery-Of and is otherwise not part of
	// the body.
	RedeliveryOf string
}

// payload is the wire body, in the field order the contract states:
//
//	{"runId":…,"orkflowName":…,"status":…,"timestamp":…,"sequence":…,"taskId":…,"attempt":…,"run":{…}}
//
// taskId and attempt are omitted when empty, so a run transition's body is
// byte-identical to what it was before task outcomes were deliverable, apart
// from sequence. sequence is omitted only when the delivery has none.
type payload struct {
	RunID       string             `json:"runId"`
	OrkflowName string             `json:"orkflowName"`
	Status      string             `json:"status"`
	Timestamp   string             `json:"timestamp"`
	Sequence    int64              `json:"sequence,omitzero"`
	TaskID      string             `json:"taskId,omitempty"`
	Attempt     int32              `json:"attempt,omitzero"`
	Run         stablejson.RawJSON `json:"run"`
}

// Body renders the signed request body. It is computed once per delivery and
// reused by every attempt, so the signature is stable across retries.
//
// The run is carried as its bytes, every escape kept, so Body first checks it
// is one JSON value a receiver can read: UTF-8 text, no half of a surrogate
// pair, no key written twice. A run that is not is refused, naming where, and
// is never sent; the encoder that renders a run (protojson) writes none of
// them, so a refusal means the run was not rendered by it.
func (d Delivery) Body() ([]byte, error) {
	if d.Run != nil {
		var run jsontext.Value
		if err := json.Unmarshal(d.Run, &run); err != nil {
			return nil, fmt.Errorf("webhooks: encode delivery %s: the run is not one valid JSON value: %s", d.ID, stablejson.Explain(err))
		}
	}
	body, err := stablejson.Marshal(payload{
		RunID:       d.RunID,
		OrkflowName: d.OrkflowName,
		Status:      d.Status,
		Timestamp:   d.Timestamp.UTC().Format(time.RFC3339Nano),
		Sequence:    d.Sequence,
		TaskID:      d.TaskID,
		Attempt:     d.Attempt,
		Run:         d.Run,
	})
	if err != nil {
		return nil, fmt.Errorf("webhooks: encode delivery %s: %w", d.ID, err)
	}
	return body, nil
}

// The retry ladder. A delivery is retried by Temporal, not by this package:
// the engine pins these numbers into each delivery when it queues it, as the
// retry policy of the activity that makes one POST (internal/engine,
// WebhookDeliveryWorkflow). "Four attempts" is the first request and up to
// three retries, spaced 100ms, 200ms and 400ms.
const (
	// DefaultMaxAttempts is how many requests one delivery may make.
	DefaultMaxAttempts = 4
	// DefaultInitialBackoff is the pause before the first retry.
	DefaultInitialBackoff = 100 * time.Millisecond
	// DefaultBackoffCoefficient multiplies the pause after each retry.
	DefaultBackoffCoefficient = 2.0
	// DefaultMaxBackoff caps one pause; the default ladder never reaches it.
	DefaultMaxBackoff = 10 * time.Second
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
	// Backoff is the pause taken before this attempt: from the end of the
	// one before it to this one's start.
	Backoff time.Duration `json:"backoff,omitzero"`
	// Duration is how long the request itself took.
	Duration time.Duration `json:"duration"`
	// At is when the request was started, in UTC. For an attempt the engine
	// recorded without a request (one that never ran), it is when that was
	// found.
	At time.Time `json:"at"`
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
	// Namespace, OrkflowName, Sequence, TaskID, Attempt and Status are the delivery's
	// coordinates as the body carried them: what a record needs to say WHICH
	// transition of which run was sent, without keeping the body.
	Namespace   string `json:"namespace,omitempty"`
	OrkflowName string `json:"orkflowName"`
	Sequence    int64  `json:"sequence,omitzero"`
	TaskID      string `json:"taskId,omitempty"`
	Attempt     int32  `json:"attempt,omitzero"`
	Status      string `json:"status"`
	// EventAt is the transition's timestamp (X-Ork-Timestamp); QueuedAt the
	// id's instant; SettledAt when the last attempt returned.
	EventAt   time.Time `json:"eventAt"`
	QueuedAt  time.Time `json:"queuedAt"`
	SettledAt time.Time `json:"settledAt"`
	// BodySHA256 is the lowercase hex SHA-256 of the exact bytes POSTed, and
	// BodyBytes their length. The bytes themselves are not kept.
	BodySHA256 string `json:"bodySha256"`
	BodyBytes  int    `json:"bodyBytes"`
	// RedeliveryOf is copied from the delivery.
	RedeliveryOf string `json:"redeliveryOf,omitempty"`
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
	// RequestTimeout bounds one attempt.
	RequestTimeout time.Duration
}

// Dispatcher makes one signed POST of one delivery to one subscriber. It
// never retries: a retry is Temporal's, under the policy the engine pinned,
// and an operator's redelivery is one attempt by design. It holds no state, so
// one instance serves every subscription.
type Dispatcher struct {
	client *http.Client
}

// NewDispatcher returns a dispatcher with defaults filled in.
func NewDispatcher(options DispatcherOptions) *Dispatcher {
	d := &Dispatcher{client: options.Client}
	if d.client == nil {
		timeout := options.RequestTimeout
		if timeout <= 0 {
			timeout = DefaultRequestTimeout
		}
		d.client = &http.Client{Timeout: timeout}
	}
	return d
}

// NewResult is the record of delivery to hook before any attempt: its
// coordinates, the digest and length of body, and its signature. Every
// attempt of the delivery sends exactly body, signed with that signature.
func NewResult(hook Webhook, delivery Delivery, body []byte) Result {
	digest := sha256.Sum256(body)
	return Result{
		DeliveryID:   delivery.ID,
		WebhookID:    hook.ID,
		Event:        delivery.Event,
		RunID:        delivery.RunID,
		URL:          hook.URL,
		Namespace:    delivery.Namespace,
		OrkflowName:  delivery.OrkflowName,
		Sequence:     delivery.Sequence,
		TaskID:       delivery.TaskID,
		Attempt:      delivery.Attempt,
		Status:       delivery.Status,
		EventAt:      delivery.Timestamp.UTC(),
		QueuedAt:     delivery.QueuedAt.UTC(),
		RedeliveryOf: delivery.RedeliveryOf,
		BodySHA256:   hex.EncodeToString(digest[:]),
		BodyBytes:    len(body),
		Signature:    Sign(hook.Secret, body),
	}
}

// Deliver makes one attempt and returns its full record; the error is non-nil
// only when the delivery did not land, so a caller can log one and keep the
// other.
func (d *Dispatcher) Deliver(ctx context.Context, hook Webhook, delivery Delivery) (result Result, err error) {
	// One stamp for every return path — delivered, refused, unencodable — so
	// a record always says when the delivery settled.
	defer func() { result.SettledAt = time.Now().UTC() }()
	body, err := delivery.Body()
	if err != nil {
		result = NewResult(hook, delivery, nil)
		result.BodySHA256, result.BodyBytes, result.Signature = "", 0, ""
		return result, err
	}
	result = NewResult(hook, delivery, body)
	attempt := d.Send(ctx, hook, delivery, body, result.Signature, 1)
	result.Attempts = []Attempt{attempt}
	result.Delivered = attempt.Delivered()
	if !result.Delivered {
		return result, fmt.Errorf("webhooks: deliver %s to %s: %w", delivery.ID, hook.URL, attempt.Failure())
	}
	return result, nil
}

// Send makes attempt number n: one POST of body to hook, labelled with the
// delivery and signed with signature, and returns its record.
func (d *Dispatcher) Send(ctx context.Context, hook Webhook, delivery Delivery, body []byte, signature string, n int) Attempt {
	started := time.Now()
	status, err := d.send(ctx, hook, delivery, body, signature, n)
	record := Attempt{Number: n, StatusCode: status, Duration: time.Since(started), At: started.UTC()}
	if err != nil {
		record.Error = err.Error()
	}
	return record
}

// Delivered reports whether the attempt landed: a 2xx with no transport
// error.
func (a Attempt) Delivered() bool {
	return a.Error == "" && a.StatusCode >= 200 && a.StatusCode < 300
}

// Retryable reports whether a failed attempt is worth repeating. A transport
// failure and a 5xx are the receiver's problem and usually transient; a 4xx is
// this sender's problem and repeating it only doubles the damage.
func (a Attempt) Retryable() bool {
	if a.Delivered() {
		return false
	}
	return a.Error != "" || a.StatusCode >= 500
}

// Failure is why the attempt did not land.
func (a Attempt) Failure() error {
	if a.Error != "" {
		return errors.New(a.Error)
	}
	return fmt.Errorf("receiver answered %d", a.StatusCode)
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
	if delivery.TaskID != "" {
		req.Header.Set(TaskHeader, delivery.TaskID)
	}
	if delivery.Sequence > 0 {
		req.Header.Set(SequenceHeader, strconv.FormatInt(delivery.Sequence, 10))
	}
	if delivery.RedeliveryOf != "" {
		req.Header.Set(RedeliveryHeader, delivery.RedeliveryOf)
	}
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
