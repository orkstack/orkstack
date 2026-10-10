//go:build integration

package artifacts_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// inNamespace is a Keep for one namespace's records.
func inNamespace(namespace string) func(*orkstackv1.AuditRecord) bool {
	return func(rec *orkstackv1.AuditRecord) bool { return rec.GetNamespace() == namespace }
}

// auditTestDay returns a random UTC day decades in the past, so this test's
// day prefixes are its own in a bucket other suites (and a live engine) write
// today's audit records into.
func auditTestDay(t *testing.T) time.Time {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	offset := int(binary.BigEndian.Uint32(b[:]) % 9000)
	return time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, offset)
}

// cleanupAuditDays deletes every key under the given days, in parallel.
func cleanupAuditDays(t *testing.T, client *s3.Client, bucket string, days ...time.Time) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for _, day := range days {
			prefix := artifacts.AuditPrefix + "/" + day.Format("2006/01/02") + "/"
			keys := listKeys(t, ctx, client, bucket, prefix)
			sem := make(chan struct{}, 32)
			var wg sync.WaitGroup
			for _, key := range keys {
				wg.Add(1)
				sem <- struct{}{}
				go func(key string) {
					defer wg.Done()
					defer func() { <-sem }()
					_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
				}(key)
			}
			wg.Wait()
		}
	})
}

func TestS3AuditStore_PutIsCreateOnly(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3AuditStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3AuditStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	day := auditTestDay(t)
	cleanupAuditDays(t, raw, cfg.S3Bucket, day)

	rec := auditRecord(day.Add(9*time.Hour), "prod", "namespace.update", "first")
	if err := store.PutAuditRecord(ctx, rec); err != nil {
		t.Fatalf("PutAuditRecord: %v", err)
	}
	key, _ := artifacts.AuditKey(rec)
	head, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("HeadObject(%s): %v", key, err)
	}
	if got := aws.ToString(head.ContentType); got != artifacts.AuditContentType {
		t.Errorf("content type = %q, want %q", got, artifacts.AuditContentType)
	}
	before := readS3(t, raw, cfg.S3Bucket, key)

	again := auditRecord(day.Add(9*time.Hour), "prod", "namespace.delete", "second")
	again.Id = rec.GetId()
	if err := store.PutAuditRecord(ctx, again); !errors.Is(err, artifacts.ErrAuditRecordExists) {
		t.Fatalf("second PutAuditRecord = %v, want ErrAuditRecordExists", err)
	}
	after := readS3(t, raw, cfg.S3Bucket, key)
	if before != after {
		t.Errorf("stored bytes changed after a refused overwrite:\nbefore %s\nafter  %s", before, after)
	}
	head2, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
	if err != nil || aws.ToString(head2.ETag) != aws.ToString(head.ETag) {
		t.Errorf("ETag moved from %s to %s (%v): the record was overwritten",
			aws.ToString(head.ETag), aws.ToString(head2.ETag), err)
	}
	t.Logf("audit record %s created once at s3://%s/%s (content-type %s, etag %s); overwrite refused: ErrAuditRecordExists",
		rec.GetId(), cfg.S3Bucket, key, aws.ToString(head.ContentType), aws.ToString(head.ETag))
}

func readS3(t *testing.T, client *s3.Client, bucket, key string) string {
	t.Helper()
	object, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject(%s): %v", key, err)
	}
	defer object.Body.Close()
	body, err := io.ReadAll(object.Body)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return string(body)
}

// TestAudit_S3ListAcrossDaysWithCursor proves the day walk against MinIO:
// records under three day prefixes page newest first through the cursor, a
// second store over the same bucket (another replica) answers identically,
// and a window of more than AuditScanMaxObjects unmatched records refuses.
func TestAudit_S3ListAcrossDaysWithCursor(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3AuditStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3AuditStore: %v", err)
	}
	replica, err := artifacts.NewS3AuditStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3AuditStore (replica): %v", err)
	}
	raw := newRawS3Client(t, cfg)
	day1 := auditTestDay(t)
	day2, day3 := day1.AddDate(0, 0, 1), day1.AddDate(0, 0, 2)
	cleanupAuditDays(t, raw, cfg.S3Bucket, day1, day2, day3)

	var want []string
	for _, at := range []time.Time{
		day1.Add(10 * time.Hour), day2.Add(1 * time.Hour), day2.Add(23 * time.Hour),
		day3.Add(time.Minute), day3.Add(2 * time.Minute),
	} {
		rec := auditRecord(at, "s3-walk", "namespace.update", "proof")
		if err := store.PutAuditRecord(ctx, rec); err != nil {
			t.Fatalf("PutAuditRecord: %v", err)
		}
		want = append([]string{rec.GetId()}, want...)
	}

	q := artifacts.AuditQuery{Since: day1, Until: day3.AddDate(0, 0, 1), Limit: 2, Keep: inNamespace("s3-walk")}
	var got []string
	var sizes []int
	for range 10 {
		page, err := replica.ListAuditRecords(ctx, q)
		if err != nil {
			t.Fatalf("replica ListAuditRecords: %v", err)
		}
		sizes = append(sizes, len(page.Records))
		got = append(got, ids(page.Records)...)
		if page.NextKey == "" {
			break
		}
		q.BeforeKey = page.NextKey
	}
	if fmt.Sprint(sizes) != "[2 2 1]" || strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("walk = %v in pages %v, want %v in pages [2 2 1]", got, sizes, want)
	}
	t.Logf("S3 day walk across %s..%s: pages %v, newest first %v", day1.Format("2006-01-02"),
		day3.Format("2006-01-02"), sizes, got)

	// The middle day excluded by the window.
	page, err := replica.ListAuditRecords(ctx, artifacts.AuditQuery{
		Since: day3, Until: day3.AddDate(0, 0, 1), Limit: 50, Keep: inNamespace("s3-walk"),
	})
	if err != nil || len(page.Records) != 2 {
		t.Fatalf("day3-only listing = %v, %v; want the two day3 records", ids(page.Records), err)
	}

	// The budget: more than AuditScanMaxObjects unmatched records in one day.
	budgetDay := day1.AddDate(0, 0, 5)
	cleanupAuditDays(t, raw, cfg.S3Bucket, budgetDay)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 48)
	errs := make(chan error, 1)
	for i := range artifacts.AuditScanMaxObjects + 1 {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			rec := auditRecord(budgetDay.Add(time.Duration(i)*time.Millisecond), "s3-budget", "namespace.update", "")
			if err := store.PutAuditRecord(ctx, rec); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatalf("seed budget records: %v", err)
	default:
	}
	_, err = replica.ListAuditRecords(ctx, artifacts.AuditQuery{
		Since: budgetDay, Until: budgetDay.AddDate(0, 0, 1), Limit: 50,
		Keep: func(rec *orkstackv1.AuditRecord) bool { return rec.GetPrincipal() == "nobody-matches" },
	})
	if !errors.Is(err, artifacts.ErrAuditScanBudget) {
		t.Fatalf("listing %d unmatched records = %v, want ErrAuditScanBudget", artifacts.AuditScanMaxObjects+1, err)
	}
	t.Logf("S3 scan of %d unmatched records refused: %v", artifacts.AuditScanMaxObjects+1, err)
}

// TestAuditStore_FiltersOverRealRecords lists records written to MinIO
// through the audit trail's own allowlist: each filter set is compiled as
// ListAuditRecords compiles it and its Match is the store's Keep, so the
// listing is decided on the decoded bodies exactly as the RPC decides it.
func TestAuditStore_FiltersOverRealRecords(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3AuditStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3AuditStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	day := auditTestDay(t)
	cleanupAuditDays(t, raw, cfg.S3Bucket, day)

	deployment := auditRecord(day.Add(1*time.Hour), "", "retention.update", "svc-ci")
	deployment.Target, deployment.Succeeded = "deployment", true
	update := auditRecord(day.Add(2*time.Hour), "payments", "namespace.update", "dsantos")
	update.Target, update.Succeeded = "payments", true
	refused := auditRecord(day.Add(3*time.Hour), "payments", "orkflow.promote", "dsantos")
	refused.Target, refused.ErrorCode, refused.Succeeded = "deploy@3", "failed_precondition", false
	other := auditRecord(day.Add(4*time.Hour), "analytics", "namespace.update", "svc-ci")
	other.Target, other.Succeeded = "analytics", true
	for _, rec := range []*orkstackv1.AuditRecord{deployment, update, refused, other} {
		if err := store.PutAuditRecord(ctx, rec); err != nil {
			t.Fatalf("PutAuditRecord: %v", err)
		}
	}
	spec := listfilter.MustSpec(&orkstackv1.ListAuditRecordsRequest{})
	cases := []struct {
		filters []string
		want    []*orkstackv1.AuditRecord // newest first
	}{
		{nil, []*orkstackv1.AuditRecord{other, refused, update, deployment}},
		{[]string{"namespace="}, []*orkstackv1.AuditRecord{deployment}},
		{[]string{"namespace=payments"}, []*orkstackv1.AuditRecord{refused, update}},
		{[]string{"action^namespace."}, []*orkstackv1.AuditRecord{other, update}},
		{[]string{"principal=svc-ci", "namespace!="}, []*orkstackv1.AuditRecord{other}},
		{[]string{"succeeded=false"}, []*orkstackv1.AuditRecord{refused}},
		{[]string{"error_code=failed_precondition"}, []*orkstackv1.AuditRecord{refused}},
		{[]string{"target~deploy"}, []*orkstackv1.AuditRecord{refused, deployment}},
		{[]string{"namespace=default"}, nil},
	}
	for _, tc := range cases {
		name := strings.Join(tc.filters, " ")
		var filters []*orkstackv1.FieldFilter
		for _, text := range tc.filters {
			f, err := listfilter.Parse(text)
			if err != nil {
				t.Fatalf("Parse(%q): %v", text, err)
			}
			filters = append(filters, f)
		}
		set, err := spec.Compile(filters)
		if err != nil {
			t.Fatalf("Compile(%s): %v", name, err)
		}
		page, err := store.ListAuditRecords(ctx, artifacts.AuditQuery{
			Since: day, Until: day.AddDate(0, 0, 1), Limit: 50,
			Keep: func(rec *orkstackv1.AuditRecord) bool { return set.Match(rec) },
		})
		if err != nil {
			t.Fatalf("ListAuditRecords(%s): %v", name, err)
		}
		if got, want := strings.Join(ids(page.Records), ","), strings.Join(ids(tc.want), ","); got != want {
			t.Errorf("filters %q listed %s, want %s", tc.filters, got, want)
		}
		t.Logf("filters %q over real records: %d listed", tc.filters, len(page.Records))
	}
}
