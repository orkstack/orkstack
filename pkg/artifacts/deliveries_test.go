package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// deliveryAt builds one valid record queued at at.
func deliveryAt(t *testing.T, at time.Time) *orkstackv1.Delivery {
	t.Helper()
	id, err := webhooks.NewDeliveryID(at)
	if err != nil {
		t.Fatalf("NewDeliveryID: %v", err)
	}
	return &orkstackv1.Delivery{
		SchemaVersion: DeliverySchemaVersion,
		Id:            id,
		WebhookId:     "wh-4a1c0e2b7d3f8159",
		Event:         orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		RunId:         "ork-2026-09-21-8f31",
		Namespace:     "payments",
		OrkflowName:   "reconcile",
		Status:        orkstackv1.Status_STATUS_FAILED,
		Url:           "https://payments.example/hooks/orkstack",
		Delivered:     false,
		Signature:     "sha256=3f9a",
		BodySha256:    strings.Repeat("7c", 32),
		BodyBytes:     1284,
		Attempts: []*orkstackv1.DeliveryAttempt{{
			Number: 1, StatusCode: 502, Error: "receiver answered 502",
			Duration: durationpb.New(84 * time.Millisecond), At: timestamppb.New(at.Add(time.Millisecond)),
		}},
		EventAt:   timestamppb.New(at.Add(-time.Millisecond)),
		CreatedAt: timestamppb.New(at),
		SettledAt: timestamppb.New(at.Add(time.Second)),
	}
}

func mustDeliveryKey(t *testing.T, rec *orkstackv1.Delivery) string {
	t.Helper()
	key, err := DeliveryKey(rec)
	if err != nil {
		t.Fatalf("DeliveryKey: %v", err)
	}
	return key
}

// deliveryBackends is every DeliveryStore a unit test can build: memory,
// local, and the real S3 client against the double.
func deliveryBackends(t *testing.T) map[string]DeliveryStore {
	t.Helper()
	local, err := NewLocalDeliveryStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalDeliveryStore: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	return map[string]DeliveryStore{
		"memory": NewMemoryDeliveryStore(),
		"local":  local,
		"s3":     newS3Double(t).deliveryStore(t),
	}
}

func TestDeliveryKey_RoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 21, 2, 14, 6, 891204113, time.UTC)
	rec := deliveryAt(t, at)
	rec.Namespace = "Odd Namespace/with slash" // foreign values are encoded, never rejected
	rec.Delivered = true
	key := mustDeliveryKey(t, rec)
	if !strings.HasPrefix(key, "metadata/deliveries/2026/09/21/"+rec.GetId()+"/") {
		t.Errorf("key %q does not start with the day and id", key)
	}
	if !strings.HasSuffix(key, "/RUN_FAILED/delivered.json") {
		t.Errorf("key %q does not end with the event and outcome", key)
	}
	if got := len(strings.Split(key, "/")); got != deliveryKeySegments {
		t.Errorf("key has %d segments, want %d: %s", got, deliveryKeySegments, key)
	}
	fields, ok := ParseDeliveryKey(key)
	if !ok {
		t.Fatalf("ParseDeliveryKey(%q) = not ok", key)
	}
	want := DeliveryKeyFields{
		ID: rec.GetId(), CreatedAt: at, Namespace: rec.GetNamespace(), RunID: rec.GetRunId(),
		WebhookID: rec.GetWebhookId(), Event: rec.GetEvent(), Delivered: true,
	}
	if fields != want {
		t.Errorf("parsed %+v, want %+v", fields, want)
	}

	good := mustDeliveryKey(t, deliveryAt(t, at))
	parts := strings.Split(good, "/")
	for name, foreign := range map[string]string{
		"outside the prefix":  "metadata/audit/2026/09/21/x.json",
		"ten segments":        strings.Join(append(append([]string{}, parts[:8]...), parts[9:]...), "/"),
		"undecodable segment": strings.Replace(good, "/payments/", "/~!!!/", 1),
		"day disagrees":       strings.Replace(good, "/2026/09/21/", "/2026/09/22/", 1),
		"unknown event":       strings.Replace(good, "/RUN_FAILED/", "/TASK_LOG/", 1),
		"unknown outcome":     strings.Replace(good, "/failed.json", "/maybe.json", 1),
	} {
		if _, ok := ParseDeliveryKey(foreign); ok {
			t.Errorf("%s: ParseDeliveryKey(%q) = ok, want skipped", name, foreign)
		}
	}

	for name, mutate := range map[string]func(*orkstackv1.Delivery){
		"created_at disagrees with the stamp": func(r *orkstackv1.Delivery) { r.CreatedAt = timestamppb.New(at.Add(time.Nanosecond)) },
		"empty webhook_id":                    func(r *orkstackv1.Delivery) { r.WebhookId = "" },
		"empty run_id":                        func(r *orkstackv1.Delivery) { r.RunId = "" },
		"empty namespace":                     func(r *orkstackv1.Delivery) { r.Namespace = "" },
		"TASK_LOG":                            func(r *orkstackv1.Delivery) { r.Event = orkstackv1.EventType_EVENT_TYPE_TASK_LOG },
		"RUN_CREATED":                         func(r *orkstackv1.Delivery) { r.Event = orkstackv1.EventType_EVENT_TYPE_RUN_CREATED },
		"old id shape":                        func(r *orkstackv1.Delivery) { r.Id = "dlv-0123456789abcdef01234567" },
	} {
		bad := deliveryAt(t, at)
		mutate(bad)
		if _, err := DeliveryKey(bad); !errors.Is(err, ErrInvalidDeliveryKey) {
			t.Errorf("%s: DeliveryKey error = %v, want ErrInvalidDeliveryKey", name, err)
		}
	}
}

func TestDeliveryIDPrefix_DerivesTheDayFromTheId(t *testing.T) {
	id := "dlv-20260921T021406.891204113Z-9c22a1f04e7b3d58"
	prefix, err := DeliveryIDPrefix(id)
	if err != nil {
		t.Fatalf("DeliveryIDPrefix: %v", err)
	}
	if want := "metadata/deliveries/2026/09/21/" + id + "/"; prefix != want {
		t.Errorf("prefix = %q, want %q", prefix, want)
	}
	for _, bad := range []string{"dlv-0123456789abcdef01234567", "../../etc", ""} {
		if _, err := DeliveryIDPrefix(bad); !errors.Is(err, ErrInvalidDeliveryKey) {
			t.Errorf("DeliveryIDPrefix(%q) error = %v, want ErrInvalidDeliveryKey", bad, err)
		}
	}
}

func TestMarshalDelivery_Bounds(t *testing.T) {
	rec := deliveryAt(t, time.Date(2026, 9, 21, 2, 14, 6, 0, time.UTC))
	rec.SchemaVersion = 0
	rec.Url = "https://payments.example/" + strings.Repeat("u", 3*DeliveryURLMaxBytes)
	rec.Attempts = nil
	for i := range 4 {
		rec.Attempts = append(rec.Attempts, &orkstackv1.DeliveryAttempt{
			Number: int32(i + 1),
			Error:  "dial tcp:\nline\tbreak\x1b[31m" + strings.Repeat("e", 4*DeliveryErrorMaxBytes),
		})
	}
	original := proto.Clone(rec)

	document, err := MarshalDelivery(rec)
	if err != nil {
		t.Fatalf("MarshalDelivery: %v", err)
	}
	if len(document) > DeliveryMaxBytes {
		t.Errorf("document is %d bytes, limit is %d", len(document), DeliveryMaxBytes)
	}
	if !proto.Equal(rec, original) {
		t.Error("MarshalDelivery modified the caller's message")
	}
	stored, err := UnmarshalDelivery(mustDeliveryKey(t, rec), document)
	if err != nil {
		t.Fatalf("UnmarshalDelivery: %v", err)
	}
	if stored.GetSchemaVersion() != DeliverySchemaVersion {
		t.Errorf("schema_version = %d, want %d", stored.GetSchemaVersion(), DeliverySchemaVersion)
	}
	if len(stored.GetUrl()) > DeliveryURLMaxBytes || !strings.HasSuffix(stored.GetUrl(), AttemptTruncationMarker) {
		t.Errorf("url is %d bytes and ends %q, want truncated with the marker", len(stored.GetUrl()), stored.GetUrl()[len(stored.GetUrl())-20:])
	}
	for _, attempt := range stored.GetAttempts() {
		got := attempt.GetError()
		if len(got) > DeliveryErrorMaxBytes || !strings.HasSuffix(got, AttemptTruncationMarker) {
			t.Errorf("attempt %d error is %d bytes, want truncated with the marker", attempt.GetNumber(), len(got))
		}
		if strings.ContainsAny(got, "\n\t\x1b") {
			t.Errorf("attempt %d error still carries a control character: %q", attempt.GetNumber(), got[:40])
		}
		if !strings.HasPrefix(got, "dial tcp: line break [31m") {
			t.Errorf("attempt %d error = %q, want control characters replaced by spaces", attempt.GetNumber(), got[:40])
		}
	}

	huge := deliveryAt(t, time.Date(2026, 9, 21, 2, 14, 6, 0, time.UTC))
	for i := range 64 {
		huge.Attempts = append(huge.Attempts, &orkstackv1.DeliveryAttempt{Number: int32(i), Error: strings.Repeat("x", DeliveryErrorMaxBytes)})
	}
	if _, err := MarshalDelivery(huge); !errors.Is(err, ErrInvalidDeliveryKey) {
		t.Errorf("an oversized document marshalled: %v", err)
	}
}

func testPutIsCreateOnly(t *testing.T, store DeliveryStore, read func(key string) []byte) {
	t.Helper()
	ctx := context.Background()
	rec := deliveryAt(t, time.Date(2026, 9, 21, 2, 14, 6, 1, time.UTC))
	if err := store.PutDelivery(ctx, rec); err != nil {
		t.Fatalf("first PutDelivery: %v", err)
	}
	key := mustDeliveryKey(t, rec)
	before := read(key)
	changed := proto.Clone(rec).(*orkstackv1.Delivery)
	changed.Signature = "sha256=forged"
	if err := store.PutDelivery(ctx, changed); !errors.Is(err, ErrDeliveryExists) {
		t.Fatalf("second PutDelivery error = %v, want ErrDeliveryExists", err)
	}
	if after := read(key); string(after) != string(before) {
		t.Errorf("the stored bytes changed after a refused put:\nbefore %s\nafter  %s", before, after)
	}
}

func TestLocalDeliveryStore_PutIsCreateOnly(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalDeliveryStore(dir)
	if err != nil {
		t.Fatalf("NewLocalDeliveryStore: %v", err)
	}
	defer store.Close()
	testPutIsCreateOnly(t, store, func(key string) []byte {
		body, _, err := store.fetchDelivery(context.Background(), key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		return body
	})
}

func TestS3DeliveryStore_PutIsCreateOnly(t *testing.T) {
	double := newS3Double(t)
	testPutIsCreateOnly(t, double.deliveryStore(t), func(key string) []byte {
		body, ok := double.object(key)
		if !ok {
			t.Fatalf("no object at %s", key)
		}
		return body
	})
}

func TestDeliveryStore_GetIsOnePrefixListThenOneGet(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 9, 21, 2, 14, 6, 0, time.UTC)
	for name, store := range deliveryBackends(t) {
		t.Run(name, func(t *testing.T) {
			rec := deliveryAt(t, at)
			neighbour := deliveryAt(t, at.Add(time.Nanosecond))
			for _, r := range []*orkstackv1.Delivery{rec, neighbour} {
				if err := store.PutDelivery(ctx, r); err != nil {
					t.Fatalf("PutDelivery: %v", err)
				}
			}
			got, ok, err := store.GetDelivery(ctx, rec.GetId())
			if err != nil || !ok {
				t.Fatalf("GetDelivery = %v, %v", ok, err)
			}
			if !proto.Equal(got, rec) {
				t.Errorf("GetDelivery = %v, want %v", got, rec)
			}
			unknown, _ := webhooks.NewDeliveryID(at.Add(time.Hour))
			if got, ok, err := store.GetDelivery(ctx, unknown); err != nil || ok || got != nil {
				t.Errorf("unknown id = (%v, %v, %v), want (nil, false, nil)", got, ok, err)
			}
			if _, _, err := store.GetDelivery(ctx, "dlv-../../x"); !errors.Is(err, ErrInvalidDeliveryKey) {
				t.Errorf("malformed id error = %v, want ErrInvalidDeliveryKey", err)
			}
		})
	}

	// The S3 backend's call count is the claim: one keys-only list and one
	// get for a known id, one list and no get for an unknown one.
	double := newS3Double(t)
	store := double.deliveryStore(t)
	rec := deliveryAt(t, at)
	if err := store.PutDelivery(ctx, rec); err != nil {
		t.Fatalf("PutDelivery: %v", err)
	}
	double.resetCounts()
	if _, ok, err := store.GetDelivery(ctx, rec.GetId()); err != nil || !ok {
		t.Fatalf("GetDelivery = %v, %v", ok, err)
	}
	if _, gets, lists := double.counts(); gets != 1 || lists != 1 {
		t.Errorf("known id made %d lists and %d gets, want 1 and 1", lists, gets)
	}
	double.resetCounts()
	unknown, _ := webhooks.NewDeliveryID(at.Add(time.Hour))
	if _, ok, err := store.GetDelivery(ctx, unknown); err != nil || ok {
		t.Fatalf("GetDelivery(unknown) = %v, %v", ok, err)
	}
	if _, gets, lists := double.counts(); gets != 0 || lists != 1 {
		t.Errorf("unknown id made %d lists and %d gets, want 1 and 0", lists, gets)
	}
}

func ids(records []*orkstackv1.Delivery) []string {
	out := make([]string, 0, len(records))
	for _, rec := range records {
		out = append(out, rec.GetId())
	}
	return out
}

func TestDeliveryStore_ListNewestFirstAcrossDays(t *testing.T) {
	ctx := context.Background()
	day1 := time.Date(2026, 9, 20, 23, 59, 59, 999999999, time.UTC)
	for name, store := range deliveryBackends(t) {
		t.Run(name, func(t *testing.T) {
			var all []*orkstackv1.Delivery
			for _, at := range []time.Time{
				day1.Add(-time.Hour), day1, // day 20
				day1.Add(time.Nanosecond), day1.Add(time.Hour), // day 21
				day1.Add(24*time.Hour + time.Nanosecond), // day 22
			} {
				rec := deliveryAt(t, at)
				if err := store.PutDelivery(ctx, rec); err != nil {
					t.Fatalf("PutDelivery: %v", err)
				}
				all = append(all, rec)
			}
			page, err := store.ListDeliveries(ctx, DeliveryQuery{
				Since: day1.Add(-48 * time.Hour), Until: day1.Add(48 * time.Hour), Limit: 50,
			})
			if err != nil {
				t.Fatalf("ListDeliveries: %v", err)
			}
			want := []string{all[4].GetId(), all[3].GetId(), all[2].GetId(), all[1].GetId(), all[0].GetId()}
			if strings.Join(ids(page.Deliveries), ",") != strings.Join(want, ",") {
				t.Errorf("listed %v, want newest first %v", ids(page.Deliveries), want)
			}
			if page.NextKey != "" {
				t.Errorf("NextKey = %q after the window was exhausted", page.NextKey)
			}

			// Since is inclusive and Until exclusive, to the nanosecond.
			page, err = store.ListDeliveries(ctx, DeliveryQuery{Since: day1, Until: day1.Add(time.Hour), Limit: 50})
			if err != nil {
				t.Fatalf("ListDeliveries: %v", err)
			}
			want = []string{all[2].GetId(), all[1].GetId()}
			if strings.Join(ids(page.Deliveries), ",") != strings.Join(want, ",") {
				t.Errorf("window [day1, day1+1h) listed %v, want %v", ids(page.Deliveries), want)
			}
		})
	}

	// A window that excludes the middle day never lists it.
	double := newS3Double(t)
	store := double.deliveryStore(t)
	for _, at := range []time.Time{day1, day1.Add(time.Hour), day1.Add(25 * time.Hour)} {
		if err := store.PutDelivery(ctx, deliveryAt(t, at)); err != nil {
			t.Fatalf("PutDelivery: %v", err)
		}
	}
	double.resetCounts()
	if _, err := store.ListDeliveries(ctx, DeliveryQuery{
		Since: time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC), Until: time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC), Limit: 50,
	}); err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if _, _, lists := double.counts(); lists != 1 {
		t.Errorf("a one-day window made %d list requests, want 1 (the middle day is never listed)", lists)
	}
}

func TestDeliveryStore_FiltersAreDecidedFromTheKey(t *testing.T) {
	ctx := context.Background()
	double := newS3Double(t)
	store := double.deliveryStore(t)
	base := time.Date(2026, 9, 21, 1, 0, 0, 0, time.UTC)
	for i := range 200 {
		rec := deliveryAt(t, base.Add(time.Duration(i)*time.Second))
		rec.RunId = fmt.Sprintf("ork-run-%03d", i%50) // 4 records per run
		rec.WebhookId = fmt.Sprintf("wh-%016x", i%50) // 4 per webhook
		rec.Namespace = map[bool]string{true: "analytics", false: "payments"}[i%50 == 7]
		if i%50 == 7 {
			rec.Event = orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED
			rec.TaskId = "extract"
			rec.Delivered = true
		}
		if err := store.PutDelivery(ctx, rec); err != nil {
			t.Fatalf("PutDelivery: %v", err)
		}
	}
	window := DeliveryQuery{Since: base, Until: base.Add(time.Hour), Limit: 50}
	cases := map[string]func(*DeliveryQuery){
		"run":       func(q *DeliveryQuery) { q.RunID = "ork-run-007" },
		"webhook":   func(q *DeliveryQuery) { q.WebhookID = fmt.Sprintf("wh-%016x", 7) },
		"event":     func(q *DeliveryQuery) { q.Event = orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED },
		"outcome":   func(q *DeliveryQuery) { q.Outcome = orkstackv1.DeliveryOutcome_DELIVERY_OUTCOME_DELIVERED },
		"namespace": func(q *DeliveryQuery) { q.Namespace = "analytics" },
		"all five": func(q *DeliveryQuery) {
			q.RunID, q.WebhookID, q.Event = "ork-run-007", fmt.Sprintf("wh-%016x", 7), orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED
			q.Outcome, q.Namespace = orkstackv1.DeliveryOutcome_DELIVERY_OUTCOME_DELIVERED, "analytics"
		},
	}
	for name, narrow := range cases {
		t.Run(name, func(t *testing.T) {
			q := window
			narrow(&q)
			double.resetCounts()
			page, err := store.ListDeliveries(ctx, q)
			if err != nil {
				t.Fatalf("ListDeliveries: %v", err)
			}
			if len(page.Deliveries) != 4 {
				t.Fatalf("matched %d records, want 4", len(page.Deliveries))
			}
			for _, rec := range page.Deliveries {
				if rec.GetRunId() != "ork-run-007" {
					t.Errorf("record %s of run %s passed the filter", rec.GetId(), rec.GetRunId())
				}
			}
			if _, gets, _ := double.counts(); gets != 4 {
				t.Errorf("fetched %d bodies to return 4 records; every filter must be decided from the key", gets)
			}
		})
	}
	q := window
	q.RunID, q.Namespace = "ork-run-007", "payments"
	if page, err := store.ListDeliveries(ctx, q); err != nil || len(page.Deliveries) != 0 {
		t.Errorf("a disjoint intersection matched %d records (%v), want none", len(page.Deliveries), err)
	}
}

func TestDeliveryStore_CursorContinuesWithoutOverlap(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 9, 21, 23, 59, 58, 0, time.UTC)
	for name, store := range deliveryBackends(t) {
		t.Run(name, func(t *testing.T) {
			var want []string
			for i := range 5 {
				rec := deliveryAt(t, base.Add(time.Duration(i)*700*time.Millisecond)) // spans midnight
				if err := store.PutDelivery(ctx, rec); err != nil {
					t.Fatalf("PutDelivery: %v", err)
				}
				want = append([]string{rec.GetId()}, want...)
			}
			q := DeliveryQuery{Since: base.Add(-time.Hour), Until: base.Add(time.Hour), Limit: 2}
			var got []string
			var sizes []int
			for range 5 {
				page, err := store.ListDeliveries(ctx, q)
				if err != nil {
					t.Fatalf("ListDeliveries: %v", err)
				}
				got = append(got, ids(page.Deliveries)...)
				sizes = append(sizes, len(page.Deliveries))
				if page.NextKey == "" {
					break
				}
				q.BeforeKey = page.NextKey
			}
			if fmt.Sprint(sizes) != "[2 2 1]" {
				t.Errorf("page sizes = %v, want [2 2 1]", sizes)
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("pages listed %v, want %v without repeats", got, want)
			}
		})
	}
}

// syntheticSource is a deliverySource holding many keys and no bodies: a
// scan that must refuse never fetches one.
type syntheticSource struct {
	keys    []string
	fetches int
}

func (s *syntheticSource) listDeliveryRange(_ context.Context, dayPrefix, from, to string, max int) ([]string, error) {
	var out []string
	for _, key := range s.keys {
		if strings.HasPrefix(key, dayPrefix) && key > from && key < to {
			out = append(out, key)
			if len(out) > max {
				break
			}
		}
	}
	return out, nil
}

func (s *syntheticSource) listDeliveryID(context.Context, string) ([]string, error) { return nil, nil }

func (s *syntheticSource) fetchDelivery(context.Context, string) ([]byte, bool, error) {
	s.fetches++
	return nil, false, nil
}

func TestDeliveryStore_ScanBudgetRefuses(t *testing.T) {
	base := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	src := &syntheticSource{}
	for i := range DeliveryScanMaxKeys + 1 {
		rec := deliveryAt(t, base.Add(time.Duration(i)*time.Millisecond))
		src.keys = append(src.keys, mustDeliveryKey(t, rec))
	}
	sort.Strings(src.keys)
	page, err := walkDeliveries(context.Background(), src, DeliveryQuery{
		Since: base, Until: base.Add(24 * time.Hour), RunID: "ork-matches-nothing", Limit: 50,
	})
	if !errors.Is(err, ErrDeliveryScanBudget) {
		t.Fatalf("error = %v, want ErrDeliveryScanBudget", err)
	}
	if len(page.Deliveries) != 0 {
		t.Errorf("a refused scan returned %d records", len(page.Deliveries))
	}
	// Exactly at the budget is answered — an empty page, honestly.
	src.keys = src.keys[:DeliveryScanMaxKeys]
	if _, err := walkDeliveries(context.Background(), src, DeliveryQuery{
		Since: base, Until: base.Add(24 * time.Hour), RunID: "ork-matches-nothing", Limit: 50,
	}); err != nil {
		t.Errorf("a scan of exactly %d keys was refused: %v", DeliveryScanMaxKeys, err)
	}
	if src.fetches != 0 {
		t.Errorf("a filter matching no key fetched %d bodies", src.fetches)
	}
}

func TestDeliveryStore_SkipsUnknownSchemaAndVanishedBody(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeliveryStore()
	base := time.Date(2026, 9, 21, 2, 0, 0, 0, time.UTC)
	good := deliveryAt(t, base)
	future := deliveryAt(t, base.Add(time.Second))
	vanished := deliveryAt(t, base.Add(2*time.Second))
	for _, rec := range []*orkstackv1.Delivery{good, future, vanished} {
		if err := store.PutDelivery(ctx, rec); err != nil {
			t.Fatalf("PutDelivery: %v", err)
		}
	}
	store.mu.Lock()
	futureKey := mustDeliveryKey(t, future)
	store.objects[futureKey] = []byte(strings.Replace(string(store.objects[futureKey]),
		`"schemaVersion": 1`, `"schemaVersion": 2`, 1))
	delete(store.objects, mustDeliveryKey(t, vanished))
	store.mu.Unlock()
	// The vanished key is still "listed": put the key back without a body by
	// listing through a source that remembers it.
	src := &vanishingSource{MemoryDeliveryStore: store, ghost: mustDeliveryKey(t, vanished)}

	page, err := walkDeliveries(ctx, src, DeliveryQuery{Since: base, Until: base.Add(time.Hour), Limit: 50})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if got := ids(page.Deliveries); len(got) != 1 || got[0] != good.GetId() {
		t.Errorf("listed %v, want only the readable record %s", got, good.GetId())
	}
	if _, _, err := store.GetDelivery(ctx, future.GetId()); !errors.Is(err, ErrUnknownDeliverySchema) {
		t.Errorf("GetDelivery of an unknown schema = %v, want an error, never a guess", err)
	}
}

// vanishingSource lists one key whose body a lifecycle rule removed.
type vanishingSource struct {
	*MemoryDeliveryStore
	ghost string
}

func (s *vanishingSource) listDeliveryRange(ctx context.Context, dayPrefix, from, to string, max int) ([]string, error) {
	keys, err := s.MemoryDeliveryStore.listDeliveryRange(ctx, dayPrefix, from, to, max)
	if strings.HasPrefix(s.ghost, dayPrefix) && s.ghost > from && s.ghost < to {
		keys = append(keys, s.ghost)
	}
	return keys, err
}

func TestUnmarshalDelivery_RefusesABodyThatDisagreesWithItsKey(t *testing.T) {
	rec := deliveryAt(t, time.Date(2026, 9, 21, 2, 0, 0, 0, time.UTC))
	key := mustDeliveryKey(t, rec)
	document, err := MarshalDelivery(rec)
	if err != nil {
		t.Fatalf("MarshalDelivery: %v", err)
	}
	if _, err := UnmarshalDelivery(key, document); err != nil {
		t.Fatalf("UnmarshalDelivery of its own key: %v", err)
	}
	for name, other := range map[string]string{
		"another run":     strings.Replace(key, "/ork-2026-09-21-8f31/", "/ork-other/", 1),
		"another outcome": strings.Replace(key, "/failed.json", "/delivered.json", 1),
		"another event":   strings.Replace(key, "/RUN_FAILED/", "/RUN_SUCCEEDED/", 1),
	} {
		if _, err := UnmarshalDelivery(other, document); !errors.Is(err, ErrInvalidDeliveryKey) {
			t.Errorf("%s: UnmarshalDelivery error = %v, want ErrInvalidDeliveryKey", name, err)
		}
	}
}
