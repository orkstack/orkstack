package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// What a webhook delivery DID is a record, never a queue. One immutable
// object per settled delivery is written by DeliveryRecorder — the
// Notifier.Observe callback, which runs after the dispatcher returned — and
// read back only by ListDeliveries and GetDelivery, to answer an operator.
// Nothing reads a record to decide what to send: there is no pending state,
// no lease and no background process. RedeliverWebhook is one POST an
// operator asked for and waits on.

const (
	// deliveryWriteTimeout bounds one record write. The observer already runs
	// on a context detached from the projection activity; the bound keeps a
	// slow store from holding the delivery goroutine — and therefore
	// Notifier.Wait — longer than a shutdown can afford.
	deliveryWriteTimeout = 5 * time.Second

	// DefaultDeliveryLookback is the window ListDeliveries answers when the
	// caller names no since and no run the engine can read.
	DefaultDeliveryLookback = 7 * 24 * time.Hour
	// DefaultDeliveryPageSize and MaxDeliveryPageSize bound one page.
	DefaultDeliveryPageSize = 50
	MaxDeliveryPageSize     = 200

	// deliveryCursorVersion is 2: version 1 carried the removed typed
	// filters and is refused.
	deliveryCursorVersion = 2
)

// DeliveryRecorder writes one record per settled delivery. It is the only
// writer of metadata/deliveries/ in the codebase.
type DeliveryRecorder struct {
	store  artifacts.DeliveryStore
	logger *slog.Logger
	now    func() time.Time
}

// NewDeliveryRecorder returns a recorder over store. A nil logger discards.
func NewDeliveryRecorder(store artifacts.DeliveryStore, logger *slog.Logger) *DeliveryRecorder {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &DeliveryRecorder{store: store, logger: logger, now: time.Now}
}

// Record is the Notifier.Observe callback. It never returns an error to the
// notifier (Observe has no error to return): a store failure is logged at
// warn with delivery_id, webhook_id, run_id, event, delivered and error, and
// the delivery's outcome — already logged by the notifier — is unchanged.
func (r *DeliveryRecorder) Record(result webhooks.Result) {
	if r == nil || r.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), deliveryWriteTimeout)
	defer cancel()
	if err := r.record(ctx, deliveryRecordFromResult(result)); err != nil {
		r.logger.Warn("webhook delivery record dropped",
			"delivery_id", result.DeliveryID, "webhook_id", result.WebhookID, "run_id", result.RunID,
			"event", string(result.Event), "delivered", result.Delivered, "error", err)
	}
}

// record writes one record synchronously and returns the store's error. It
// is what Record calls, and what RedeliverWebhook calls directly so the
// response can carry the stored record.
func (r *DeliveryRecorder) record(ctx context.Context, rec *orkstackv1.Delivery) error {
	if err := r.store.PutDelivery(ctx, rec); err != nil {
		return fmt.Errorf("write delivery record %s: %w", rec.GetId(), err)
	}
	return nil
}

// deliveryRecordFromResult is the one place a Result becomes a proto record.
func deliveryRecordFromResult(result webhooks.Result) *orkstackv1.Delivery {
	rec := &orkstackv1.Delivery{
		SchemaVersion: artifacts.DeliverySchemaVersion,
		Id:            result.DeliveryID,
		WebhookId:     result.WebhookID,
		Event:         orkstackv1.EventType(orkstackv1.EventType_value["EVENT_TYPE_"+string(result.Event)]),
		RunId:         result.RunID,
		Namespace:     artifacts.ResolveNamespace(result.Namespace),
		OrkflowName:   result.OrkflowName,
		TaskId:        result.TaskID,
		Attempt:       result.Attempt,
		Status:        orkstackv1.Status(orkstackv1.Status_value["STATUS_"+result.Status]),
		Url:           result.URL,
		Delivered:     result.Delivered,
		Signature:     result.Signature,
		BodySha256:    result.BodySHA256,
		BodyBytes:     int64(result.BodyBytes),
		RedeliveryOf:  result.RedeliveryOf,
	}
	for _, attempt := range result.Attempts {
		message := attempt.Error
		if message == "" && attempt.StatusCode != 0 && (attempt.StatusCode < 200 || attempt.StatusCode >= 300) {
			// The dispatcher keeps a transport failure as the error; a
			// refusal is a status code, and the record spells it out so an
			// attempt row always says why it did not land.
			message = fmt.Sprintf("receiver answered %d", attempt.StatusCode)
		}
		one := &orkstackv1.DeliveryAttempt{
			Number:     int32(attempt.Number),
			StatusCode: int32(attempt.StatusCode),
			Error:      message,
			Backoff:    durationpb.New(attempt.Backoff),
			Duration:   durationpb.New(attempt.Duration),
		}
		if !attempt.At.IsZero() {
			one.At = timestamppb.New(attempt.At)
		}
		rec.Attempts = append(rec.Attempts, one)
	}
	if !result.EventAt.IsZero() {
		rec.EventAt = timestamppb.New(result.EventAt)
	}
	created := result.QueuedAt
	if created.IsZero() && len(result.Attempts) > 0 {
		// A caller that bypassed the notifier. The id must still carry this
		// instant, or DeliveryKey refuses the record and the drop is logged.
		created = result.Attempts[0].At
	}
	if !created.IsZero() {
		rec.CreatedAt = timestamppb.New(created)
	}
	if !result.SettledAt.IsZero() {
		rec.SettledAt = timestamppb.New(result.SettledAt)
	}
	return rec
}

// deliveryStoreMissing is the refusal every delivery RPC gives when no store
// is wired: an operator must be told, not shown a subscriber that looks like
// it was never called.
func deliveryStoreMissing() error {
	return connect.NewError(connect.CodeFailedPrecondition, errors.New("delivery record store is not configured"))
}

// deliveryCursor is the page token: the canonical filter set's key, the
// window as the first page resolved it, and the key to continue before.
type deliveryCursor struct {
	Version     int    `json:"v"`
	Filters     string `json:"f"`
	WindowSince string `json:"ws"`
	WindowUntil string `json:"wu"`
	// RunStart records that WindowSince is a run's created_at, so every page
	// reports since_is_run_start as the first did.
	RunStart  bool   `json:"rs,omitempty"`
	BeforeKey string `json:"k"`
}

func encodeDeliveryCursor(c deliveryCursor) (string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// decodeDeliveryCursor reads a page token issued under set, returning the
// window it carries, whether its start is a run's, and the key to continue
// before.
func decodeDeliveryCursor(token string, set listfilter.Set) (since, until time.Time, runStart bool, beforeKey string, err error) {
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("invalid page_token"))
	raw, decodeErr := base64.RawURLEncoding.DecodeString(token)
	if decodeErr != nil {
		return time.Time{}, time.Time{}, false, "", invalid
	}
	var c deliveryCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != deliveryCursorVersion {
		return time.Time{}, time.Time{}, false, "", invalid
	}
	if _, ok := artifacts.ParseDeliveryKey(c.BeforeKey); !ok {
		return time.Time{}, time.Time{}, false, "", invalid
	}
	since, sinceErr := time.Parse(time.RFC3339Nano, c.WindowSince)
	until, untilErr := time.Parse(time.RFC3339Nano, c.WindowUntil)
	if sinceErr != nil || untilErr != nil {
		return time.Time{}, time.Time{}, false, "", invalid
	}
	if err := checkFilterKey(c.Filters, set); err != nil {
		return time.Time{}, time.Time{}, false, "", err
	}
	return since.UTC(), until.UTC(), c.RunStart, c.BeforeKey, nil
}

// emptyWindow is the refusal of a resolved window with no instant in it: the
// §4.4 interval message of docs/UI_CLI_PARITY.md "Filtering lists", spelled
// with the bounds the listing would have used.
func emptyWindow(path string, since, until time.Time) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s >= %s and < %s select no instant",
		path, since.UTC().Format(time.RFC3339Nano), until.UTC().Format(time.RFC3339Nano)))
}

// deliveryKeep is the store's Keep for a filter set: every delivery filter is
// a coordinate of the record's key, so the set is matched against a Delivery
// built from the key alone and a body is read only for a record that matches.
func deliveryKeep(set listfilter.Set) func(artifacts.DeliveryKeyFields) bool {
	if set.Empty() {
		return nil
	}
	return func(k artifacts.DeliveryKeyFields) bool {
		return set.Match(&orkstackv1.Delivery{
			Id:        k.ID,
			CreatedAt: timestamppb.New(k.CreatedAt),
			Namespace: k.Namespace,
			RunId:     k.RunID,
			WebhookId: k.WebhookID,
			Event:     k.Event,
			Delivered: k.Delivered,
		})
	}
}

// ListDeliveries lists delivery records newest first. It reads the shared
// store on every call and never process memory, so a replica that delivered
// nothing answers identically to the one that delivered everything.
//
// It is a deployment-wide listing narrowed by filters, every one decided
// from a record's key. The window is the created_at interval the filters
// bound; its start defaults to the run's created_at when exactly one run_id
// equals filter names a run the engine can read, otherwise to seven days
// before its end, and its end to now. A page token carries the resolved
// window, so "now" does not move under a listing an operator is paging.
func (s *Service) ListDeliveries(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListDeliveriesRequest],
) (*connect.Response[orkstackv1.ListDeliveriesResponse], error) {
	msg := req.Msg
	set, err := compileFilters(msg)
	if err != nil {
		return nil, err
	}
	pageSize := int(msg.GetPageSize())
	switch {
	case pageSize == 0:
		pageSize = DefaultDeliveryPageSize
	case pageSize < 0 || pageSize > MaxDeliveryPageSize:
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("page_size %d is outside 1..%d", msg.GetPageSize(), MaxDeliveryPageSize))
	}
	if s.deliveries == nil {
		return nil, deliveryStoreMissing()
	}

	var since, until time.Time
	var runStart bool
	var beforeKey string
	if token := msg.GetPageToken(); token != "" {
		// The window is the one the first page resolved.
		if since, until, runStart, beforeKey, err = decodeDeliveryCursor(token, set); err != nil {
			return nil, err
		}
	} else {
		atOrAfter, before := set.Interval("created_at")
		until = s.now().UTC()
		if !before.IsZero() {
			until = before.UTC()
		}
		runID, oneRun := set.Equality("run_id")
		switch {
		case !atOrAfter.IsZero():
			since = atOrAfter.UTC()
		case oneRun:
			// Since the run started, when the engine can read the run;
			// otherwise the ordinary lookback — a run whose history is gone
			// can still have records.
			run, err := s.LookupRun(ctx, runID)
			switch {
			case err == nil && run.GetCreatedAt().IsValid():
				since = run.GetCreatedAt().AsTime().UTC()
				runStart = true
			case err == nil || connect.CodeOf(err) == connect.CodeNotFound:
				since = until.Add(-DefaultDeliveryLookback)
			default:
				return nil, connect.NewError(connect.CodeUnavailable,
					fmt.Errorf("read run %q to resolve the delivery window: %w", runID, err))
			}
		default:
			since = until.Add(-DefaultDeliveryLookback)
		}
		if !until.After(since) {
			return nil, emptyWindow("created_at", since, until)
		}
	}

	page, err := s.deliveries.ListDeliveries(ctx, artifacts.DeliveryQuery{
		Since:     since,
		Until:     until,
		Keep:      deliveryKeep(set),
		BeforeKey: beforeKey,
		Limit:     pageSize,
	})
	if err != nil {
		if errors.Is(err, artifacts.ErrDeliveryScanBudget) {
			return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
				"scan examined more than %d delivery keys before filling a page; narrow the created_at window or add filters",
				artifacts.DeliveryScanMaxKeys))
		}
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read delivery records: %w", err))
	}
	out := &orkstackv1.ListDeliveriesResponse{
		Deliveries:      page.Deliveries,
		Since:           timestamppb.New(since),
		Until:           timestamppb.New(until),
		SinceIsRunStart: runStart,
	}
	if out.Deliveries == nil {
		out.Deliveries = []*orkstackv1.Delivery{}
	}
	if page.NextKey != "" {
		token, err := encodeDeliveryCursor(deliveryCursor{
			Version:     deliveryCursorVersion,
			Filters:     set.Key(),
			WindowSince: since.Format(time.RFC3339Nano),
			WindowUntil: until.Format(time.RFC3339Nano),
			RunStart:    runStart,
			BeforeKey:   page.NextKey,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode page_token: %w", err))
		}
		out.NextPageToken = token
	}
	return connect.NewResponse(out), nil
}

// GetDelivery returns one record by id, from the shared store.
func (s *Service) GetDelivery(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetDeliveryRequest],
) (*connect.Response[orkstackv1.GetDeliveryResponse], error) {
	rec, err := s.readDelivery(ctx, req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.GetDeliveryResponse{Delivery: rec}), nil
}

// readDelivery is GetDelivery's read, shared with RedeliverWebhook so both
// refuse the same ids the same way.
func (s *Service) readDelivery(ctx context.Context, id string) (*orkstackv1.Delivery, error) {
	if s.deliveries == nil {
		return nil, deliveryStoreMissing()
	}
	if !webhooks.ValidDeliveryID(id) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%q is not a delivery id", id))
	}
	rec, ok, err := s.deliveries.GetDelivery(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read delivery %q: %w", id, err))
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("delivery %q not found", id))
	}
	return rec, nil
}

// RedeliverWebhook POSTs one delivery again — once, now, synchronously,
// signed with the subscription's current secret, embedding the run as it
// reads today — and records it as a new delivery that names the one it
// repeats. A refused step writes nothing and POSTs nothing. The operator is
// the retry policy: the dispatcher used here has a retry budget of zero.
func (s *Service) RedeliverWebhook(
	ctx context.Context,
	req *connect.Request[orkstackv1.RedeliverWebhookRequest],
) (*connect.Response[orkstackv1.RedeliverWebhookResponse], error) {
	original, err := s.readDelivery(ctx, req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	hook, ok, err := s.webhooks.LookupForDelivery(ctx, original.GetWebhookId())
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	if !ok {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("webhook %q is no longer registered; nothing was sent", original.GetWebhookId()))
	}
	event, err := webhooks.ParseEvent(original.GetEvent().String())
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("delivery %q records event %s, which is not deliverable: %w", original.GetId(), original.GetEvent(), err))
	}
	run, err := s.LookupRunIn(ctx, original.GetRunId(), original.GetNamespace())
	if err != nil {
		return nil, err
	}
	// A delivery body never carries a fan-out's items (docs/DSL.md "Fan-out"),
	// the redelivery no more than the original: the same shape, the run as
	// it reads now.
	encoded, err := MarshalJSON(engine.WithoutItems(run))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode run %q: %w", run.GetId(), err))
	}
	now := s.now().UTC()
	id, err := webhooks.NewDeliveryID(now)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	// The body differs from the original in exactly one place: `run` is the
	// run as it reads now. The transition's own coordinates and timestamp
	// did not move, and the new record's body_sha256 says the bytes did.
	delivery := webhooks.Delivery{
		ID:           id,
		Event:        event,
		RunID:        original.GetRunId(),
		OrkflowName:  original.GetOrkflowName(),
		Status:       strings.TrimPrefix(original.GetStatus().String(), "STATUS_"),
		Timestamp:    original.GetEventAt().AsTime().UTC(),
		Namespace:    original.GetNamespace(),
		TaskID:       original.GetTaskId(),
		Attempt:      original.GetAttempt(),
		Run:          encoded,
		QueuedAt:     now,
		RedeliveryOf: original.GetId(),
	}
	// The request's own context: an operator who hangs up ends the one
	// request. A failed POST is a recorded outcome, not an RPC failure.
	result, _ := s.redelivery.Deliver(ctx, hook, delivery)
	rec := deliveryRecordFromResult(result)

	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deliveryWriteTimeout)
	defer cancel()
	if err := s.deliveryRecorder.record(writeCtx, rec); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"redelivery %s of %s was sent (delivered=%t, status %d) but its record could not be written: %w",
			rec.GetId(), original.GetId(), result.Delivered, result.LastAttempt().StatusCode, err))
	}
	return connect.NewResponse(&orkstackv1.RedeliverWebhookResponse{Delivery: rec}), nil
}
