//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// deliveryRecord builds one valid record queued at at.
func deliveryRecord(t *testing.T, at time.Time, runID string) *orkstackv1.Delivery {
	t.Helper()
	id, err := webhooks.NewDeliveryID(at)
	if err != nil {
		t.Fatalf("NewDeliveryID: %v", err)
	}
	return &orkstackv1.Delivery{
		SchemaVersion: artifacts.DeliverySchemaVersion,
		Id:            id, WebhookId: "wh-4a1c0e2b7d3f8159", Event: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		RunId: runID, Namespace: "payments", OrkflowName: "reconcile", Status: orkstackv1.Status_STATUS_FAILED,
		Url: "https://payments.example/hooks", Signature: "sha256=00", BodySha256: strings.Repeat("ab", 32),
		CreatedAt: timestamppb.New(at), EventAt: timestamppb.New(at), SettledAt: timestamppb.New(at.Add(time.Second)),
		Attempts: []*orkstackv1.DeliveryAttempt{{Number: 1, StatusCode: 502, Error: "receiver answered 502", At: timestamppb.New(at)}},
	}
}

// cleanupDeliveryDays deletes every key under the given days.
func cleanupDeliveryDays(t *testing.T, client *s3.Client, bucket string, days ...time.Time) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for _, day := range days {
			prefix := artifacts.DeliveryPrefix + "/" + day.Format("2006/01/02") + "/"
			for _, key := range listKeys(t, ctx, client, bucket, prefix) {
				_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			}
		}
	})
}

func TestIntegration_S3DeliveryStorePutIsCreateOnly(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3DeliveryStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3DeliveryStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	day := auditTestDay(t)
	cleanupDeliveryDays(t, raw, cfg.S3Bucket, day)

	rec := deliveryRecord(t, day.Add(9*time.Hour), "ork-create-only")
	if err := store.PutDelivery(ctx, rec); err != nil {
		t.Fatalf("PutDelivery: %v", err)
	}
	key, _ := artifacts.DeliveryKey(rec)
	head, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("HeadObject(%s): %v", key, err)
	}
	if got := aws.ToString(head.ContentType); got != artifacts.DeliveryContentType {
		t.Errorf("content type = %q, want %q", got, artifacts.DeliveryContentType)
	}
	before := readS3(t, raw, cfg.S3Bucket, key)
	forged := proto.Clone(rec).(*orkstackv1.Delivery)
	forged.Signature = "sha256=forged"
	if err := store.PutDelivery(ctx, forged); !errors.Is(err, artifacts.ErrDeliveryExists) {
		t.Fatalf("second PutDelivery = %v, want ErrDeliveryExists", err)
	}
	if after := readS3(t, raw, cfg.S3Bucket, key); after != before {
		t.Errorf("stored bytes changed after a refused overwrite")
	}
	t.Logf("delivery record %s created once at s3://%s/%s (content-type %s); overwrite refused: ErrDeliveryExists",
		rec.GetId(), cfg.S3Bucket, key, aws.ToString(head.ContentType))
}

func TestIntegration_S3DeliveryStoreGetIsOnePrefixList(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3DeliveryStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3DeliveryStore: %v", err)
	}
	// Another replica: a separate client over the same bucket.
	replica, err := artifacts.NewS3DeliveryStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3DeliveryStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	day := auditTestDay(t)
	cleanupDeliveryDays(t, raw, cfg.S3Bucket, day)

	rec := deliveryRecord(t, day.Add(3*time.Hour), "ork-get-one")
	if err := store.PutDelivery(ctx, rec); err != nil {
		t.Fatalf("PutDelivery: %v", err)
	}
	got, ok, err := replica.GetDelivery(ctx, rec.GetId())
	if err != nil || !ok || !proto.Equal(got, rec) {
		t.Fatalf("replica GetDelivery = %v, %v, %v; want the stored record", got, ok, err)
	}
	prefix, _ := artifacts.DeliveryIDPrefix(rec.GetId())
	if keys := listKeys(t, ctx, raw, cfg.S3Bucket, prefix); len(keys) != 1 {
		t.Errorf("the id prefix %s holds %d keys, want exactly 1", prefix, len(keys))
	}
	unknown, _ := webhooks.NewDeliveryID(day.Add(4 * time.Hour))
	if got, ok, err := replica.GetDelivery(ctx, unknown); err != nil || ok || got != nil {
		t.Errorf("unknown id = %v, %v, %v; want (nil, false, nil)", got, ok, err)
	}
	t.Logf("GetDelivery(%s) on a second client read the record under %s", rec.GetId(), prefix)
}

func TestIntegration_S3DeliveryStoreListsNewestFirstAcrossDaysWithCursor(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3DeliveryStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3DeliveryStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	day := auditTestDay(t)
	days := []time.Time{day, day.AddDate(0, 0, 1), day.AddDate(0, 0, 2)}
	cleanupDeliveryDays(t, raw, cfg.S3Bucket, days...)

	var want []string
	for i, d := range days {
		for j := range 3 {
			rec := deliveryRecord(t, d.Add(time.Duration(10+j)*time.Hour), fmt.Sprintf("ork-list-%d", i))
			if j == 1 {
				rec.Delivered = true
			}
			if err := store.PutDelivery(ctx, rec); err != nil {
				t.Fatalf("PutDelivery: %v", err)
			}
			want = append([]string{rec.GetId()}, want...)
		}
	}
	q := artifacts.DeliveryQuery{Since: days[0], Until: days[2].AddDate(0, 0, 1), Limit: 4}
	var got []string
	pages := 0
	for {
		page, err := store.ListDeliveries(ctx, q)
		if err != nil {
			t.Fatalf("ListDeliveries: %v", err)
		}
		pages++
		for _, rec := range page.Deliveries {
			got = append(got, rec.GetId())
		}
		if page.NextKey == "" {
			break
		}
		q.BeforeKey = page.NextKey
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("listed %v, want newest first %v", got, want)
	}
	if pages != 3 {
		t.Errorf("walked %d pages of 4 over 9 records, want 3", pages)
	}
	filtered, err := store.ListDeliveries(ctx, artifacts.DeliveryQuery{
		Since: days[0], Until: days[2].AddDate(0, 0, 1), Limit: 50,
		Keep: func(k artifacts.DeliveryKeyFields) bool { return k.RunID == "ork-list-1" && k.Delivered },
	})
	if err != nil || len(filtered.Deliveries) != 1 || filtered.Deliveries[0].GetRunId() != "ork-list-1" {
		t.Errorf("filtered listing = %v, %v; want the one delivered record of ork-list-1", filtered.Deliveries, err)
	}
	t.Logf("9 records over 3 days listed newest first in %d pages without repeats; run+delivered filter matched 1", pages)
}

func deliveryIDs(records []*orkstackv1.Delivery) []string {
	out := make([]string, len(records))
	for i, rec := range records {
		out[i] = rec.GetId()
	}
	return out
}

// deliveryKeep is the Keep ListDeliveries builds: the compiled filter set
// matched against a Delivery made of the key's coordinates alone.
func deliveryKeep(t *testing.T, texts ...string) func(artifacts.DeliveryKeyFields) bool {
	t.Helper()
	var filters []*orkstackv1.FieldFilter
	for _, text := range texts {
		f, err := listfilter.Parse(text)
		if err != nil {
			t.Fatalf("Parse(%q): %v", text, err)
		}
		filters = append(filters, f)
	}
	set, err := listfilter.MustSpec(&orkstackv1.ListDeliveriesRequest{}).Compile(filters)
	if err != nil {
		t.Fatalf("Compile(%q): %v", texts, err)
	}
	return func(k artifacts.DeliveryKeyFields) bool {
		return set.Match(&orkstackv1.Delivery{
			Id: k.ID, CreatedAt: timestamppb.New(k.CreatedAt), Namespace: k.Namespace,
			RunId: k.RunID, WebhookId: k.WebhookID, Event: k.Event, Delivered: k.Delivered,
		})
	}
}

// TestDeliveryStore_FilteredListingReadsOnlyMatchingBodies proves against
// MinIO that every delivery filter is decided from the key: a listing reads a
// body only for a record it returns (BodiesRead counts every GetObject).
func TestDeliveryStore_FilteredListingReadsOnlyMatchingBodies(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3DeliveryStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3DeliveryStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	day := auditTestDay(t)
	cleanupDeliveryDays(t, raw, cfg.S3Bucket, day)

	// 40 records in one day over two webhooks, two runs and both outcomes:
	// exactly i%8 == 0 (5 records) is webhook A, run 1 and not delivered.
	webhooksByIndex := []string{"wh-00000000000000aa", "wh-00000000000000bb"}
	runs := []string{"ork-bodies-1", "ork-bodies-2"}
	var want []string
	for i := range 40 {
		rec := deliveryRecord(t, day.Add(time.Hour+time.Duration(i)*time.Second), runs[(i/2)%2])
		rec.WebhookId = webhooksByIndex[i%2]
		rec.Delivered = (i/4)%2 == 1
		if err := store.PutDelivery(ctx, rec); err != nil {
			t.Fatalf("PutDelivery: %v", err)
		}
		if i%8 == 0 {
			want = append([]string{rec.GetId()}, want...)
		}
	}
	window := artifacts.DeliveryQuery{Since: day, Until: day.AddDate(0, 0, 1), Limit: 50}

	filtered := window
	filtered.Keep = deliveryKeep(t, "webhook_id="+webhooksByIndex[0], "run_id="+runs[0], "delivered=false")
	before := store.BodiesRead()
	page, err := store.ListDeliveries(ctx, filtered)
	if err != nil {
		t.Fatalf("filtered ListDeliveries: %v", err)
	}
	filteredReads := store.BodiesRead() - before
	if got := strings.Join(deliveryIDs(page.Deliveries), ","); got != strings.Join(want, ",") {
		t.Errorf("filtered listing = %s, want %s", got, strings.Join(want, ","))
	}
	if filteredReads != 5 {
		t.Errorf("filtered listing read %d bodies, want 5 (one per matching record)", filteredReads)
	}

	unfiltered := window
	unfiltered.Limit = 10
	before = store.BodiesRead()
	page, err = store.ListDeliveries(ctx, unfiltered)
	if err != nil {
		t.Fatalf("unfiltered ListDeliveries: %v", err)
	}
	unfilteredReads := store.BodiesRead() - before
	if len(page.Deliveries) != 10 || page.NextKey == "" {
		t.Errorf("unfiltered page = %d records, next %q; want 10 and a cursor", len(page.Deliveries), page.NextKey)
	}
	if unfilteredReads != 10 {
		t.Errorf("unfiltered page of 10 read %d bodies, want 10", unfilteredReads)
	}
	proof.Write(t, "", "deliveries-bodies.txt", fmt.Appendf(nil,
		"filtered listing: %d bodies read for %d records\nunfiltered page of 10: %d bodies read\n",
		filteredReads, len(want), unfilteredReads))
}
