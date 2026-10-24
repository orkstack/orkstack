package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

var auditIDPattern = regexp.MustCompile(`^aud-\d{8}T\d{6}\.\d{9}Z-[0-9a-f]{16}$`)

// auditRecord builds a minimal valid record at one instant.
func auditRecord(at time.Time, namespace, action, principal string) *orkstackv1.AuditRecord {
	return &orkstackv1.AuditRecord{
		SchemaVersion: artifacts.AuditSchemaVersion,
		Id:            artifacts.NewAuditID(at),
		At:            timestamppb.New(at),
		Principal:     principal,
		Action:        action,
		Procedure:     "/orkstack.v1.NamespaceService/UpdateNamespace",
		Namespace:     namespace,
		Target:        namespace,
		Succeeded:     true,
	}
}

// newAuditStores returns every backend a unit test can exercise without
// infrastructure. The S3 backend needs a bucket, so it is proven by
// audit_integration_test.go against MinIO; the walk, the bounds and the key
// layout it shares with these are exercised here.
func newAuditStores(t *testing.T) map[string]artifacts.AuditStore {
	t.Helper()
	local, err := artifacts.NewLocalAuditStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalAuditStore: %v", err)
	}
	return map[string]artifacts.AuditStore{
		"local":  local,
		"memory": artifacts.NewMemoryAuditStore(),
	}
}

func TestAuditID_ShapeAndOrder(t *testing.T) {
	at := time.Date(2026, 3, 14, 11, 2, 44, 123456789, time.UTC)
	first := artifacts.NewAuditID(at)
	second := artifacts.NewAuditID(at.Add(time.Nanosecond))
	for _, id := range []string{first, second} {
		if !auditIDPattern.MatchString(id) {
			t.Errorf("NewAuditID = %q, want it to match %s", id, auditIDPattern)
		}
		if !artifacts.ValidAuditID(id) {
			t.Errorf("ValidAuditID(%q) = false for a minted id", id)
		}
	}
	if !(first < second) {
		t.Errorf("ids minted 1ns apart sort %q >= %q; want time order", first, second)
	}
	for _, bad := range []string{"", "aud-x", "../aud-20260314T110244.123456789Z-0123456789abcdef",
		"aud-20260314T110244.123456789Z-0123456789ABCDEF", "trg-0123456789abcdef"} {
		if artifacts.ValidAuditID(bad) {
			t.Errorf("ValidAuditID(%q) = true, want false", bad)
		}
	}
}

func TestAuditKey_RoundTrip(t *testing.T) {
	at := time.Date(2026, 2, 28, 23, 59, 59, 999999999, time.UTC)
	rec := auditRecord(at, "prod-payments", "namespace.update", "dsantos")
	key, err := artifacts.AuditKey(rec)
	if err != nil {
		t.Fatalf("AuditKey: %v", err)
	}
	want := "metadata/audit/2026/02/28/" + rec.GetId() + ".json"
	if key != want {
		t.Fatalf("AuditKey = %q, want %q", key, want)
	}
	id, day, ok := artifacts.ParseAuditKey(key)
	if !ok || id != rec.GetId() || !day.Equal(time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("ParseAuditKey(%q) = %q, %s, %v; want the id and its day", key, id, day, ok)
	}

	// A non-UTC `at` still files under its UTC day.
	local := time.Date(2026, 3, 1, 1, 0, 0, 0, time.FixedZone("plus3", 3*3600))
	rec2 := auditRecord(local, "a", "namespace.update", "")
	key2, err := artifacts.AuditKey(rec2)
	if err != nil || !strings.HasPrefix(key2, "metadata/audit/2026/02/28/") {
		t.Errorf("AuditKey(non-UTC at) = %q, %v; want the UTC day 2026/02/28", key2, err)
	}

	// The day segment disagrees with the stamp in the id.
	mismatched := "metadata/audit/2026/03/01/" + rec.GetId() + ".json"
	if _, _, ok := artifacts.ParseAuditKey(mismatched); ok {
		t.Errorf("ParseAuditKey(%q) ok = true for a key whose day disagrees with its stamp", mismatched)
	}
	for _, foreign := range []string{
		"metadata/audit/2026/02/28/notes.txt",
		"metadata/audit/2026/02/" + rec.GetId() + ".json",
		"metadata/attempts/x/y/0000000001.json",
		"metadata/audit/2026/13/28/" + rec.GetId() + ".json",
	} {
		if _, _, ok := artifacts.ParseAuditKey(foreign); ok {
			t.Errorf("ParseAuditKey(%q) ok = true, want a foreign key to be skipped", foreign)
		}
	}

	// A record without an id or time cannot be addressed.
	if _, err := artifacts.AuditKey(&orkstackv1.AuditRecord{Id: rec.GetId()}); err == nil {
		t.Error("AuditKey accepted a record with no time")
	}
	if _, err := artifacts.AuditKey(&orkstackv1.AuditRecord{Id: "../x", At: timestamppb.New(at)}); err == nil {
		t.Error("AuditKey accepted an id that is not an audit id")
	}
}

func TestMarshalAuditRecord_Bounds(t *testing.T) {
	at := time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC)
	rec := auditRecord(at, "prod", "namespace.update", "dsan\ntos\t\x1b[31mred")
	document, err := artifacts.MarshalAuditRecord(rec)
	if err != nil {
		t.Fatalf("MarshalAuditRecord: %v", err)
	}
	back, err := artifacts.UnmarshalAuditRecord(document)
	if err != nil {
		t.Fatalf("UnmarshalAuditRecord: %v", err)
	}
	if got := back.GetPrincipal(); got != "dsan tos  [31mred" {
		t.Errorf("principal = %q, want control characters replaced by spaces", got)
	}
	if rec.GetPrincipal() != "dsan\ntos\t\x1b[31mred" {
		t.Error("MarshalAuditRecord modified the caller's record")
	}

	long := &orkstackv1.AuditRecord{
		SchemaVersion: artifacts.AuditSchemaVersion,
		Id:            artifacts.NewAuditID(at),
		At:            timestamppb.New(at),
		Principal:     strings.Repeat("p", 10_000),
		Target:        strings.Repeat("t", 10_000),
		Detail:        strings.Repeat("d", 100_000),
		Action:        "namespace.update",
	}
	document, err = artifacts.MarshalAuditRecord(long)
	if err != nil {
		t.Fatalf("MarshalAuditRecord(long): %v", err)
	}
	if len(document) > artifacts.AuditMaxBytes {
		t.Errorf("document is %d bytes, want at most %d", len(document), artifacts.AuditMaxBytes)
	}
	back, err = artifacts.UnmarshalAuditRecord(document)
	if err != nil {
		t.Fatalf("UnmarshalAuditRecord(long): %v", err)
	}
	for field, value := range map[string]struct {
		got string
		max int
	}{
		"principal": {back.GetPrincipal(), artifacts.AuditPrincipalMaxBytes},
		"target":    {back.GetTarget(), artifacts.AuditTargetMaxBytes},
		"detail":    {back.GetDetail(), artifacts.AuditDetailMaxBytes},
	} {
		if len(value.got) > value.max {
			t.Errorf("%s is %d bytes, want at most %d", field, len(value.got), value.max)
		}
		if !strings.HasSuffix(value.got, artifacts.AttemptTruncationMarker) {
			t.Errorf("%s = %.40q…, want the truncation marker", field, value.got)
		}
	}

	// Invalid UTF-8 from a header still encodes rather than dropping the record.
	rec3 := auditRecord(at, "prod", "namespace.update", "bad\xffbyte")
	if _, err := artifacts.MarshalAuditRecord(rec3); err != nil {
		t.Errorf("MarshalAuditRecord(invalid UTF-8 principal): %v", err)
	}
}

func TestLocalAuditStore_PutIsCreateOnly(t *testing.T) {
	dir := t.TempDir()
	store, err := artifacts.NewLocalAuditStore(dir)
	if err != nil {
		t.Fatalf("NewLocalAuditStore: %v", err)
	}
	ctx := context.Background()
	at := time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC)
	rec := auditRecord(at, "prod", "namespace.update", "first")
	if err := store.PutAuditRecord(ctx, rec); err != nil {
		t.Fatalf("PutAuditRecord: %v", err)
	}
	key, _ := artifacts.AuditKey(rec)
	before, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("read stored record: %v", err)
	}

	again := auditRecord(at, "prod", "namespace.delete", "second")
	again.Id = rec.GetId()
	if err := store.PutAuditRecord(ctx, again); !errors.Is(err, artifacts.ErrAuditRecordExists) {
		t.Fatalf("second PutAuditRecord of the same id = %v, want ErrAuditRecordExists", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("re-read stored record: %v", err)
	}
	if string(before) != string(after) {
		t.Errorf("stored bytes changed after a refused overwrite:\nbefore %s\nafter  %s", before, after)
	}
}

func TestMemoryAuditStore_PutIsCreateOnly(t *testing.T) {
	store := artifacts.NewMemoryAuditStore()
	at := time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC)
	rec := auditRecord(at, "prod", "namespace.update", "first")
	if err := store.PutAuditRecord(context.Background(), rec); err != nil {
		t.Fatalf("PutAuditRecord: %v", err)
	}
	if err := store.PutAuditRecord(context.Background(), rec); !errors.Is(err, artifacts.ErrAuditRecordExists) {
		t.Fatalf("second put = %v, want ErrAuditRecordExists", err)
	}
}

// putAll writes records and fails the test on the first error.
func putAll(t *testing.T, store artifacts.AuditStore, records ...*orkstackv1.AuditRecord) {
	t.Helper()
	for _, rec := range records {
		if err := store.PutAuditRecord(context.Background(), rec); err != nil {
			t.Fatalf("PutAuditRecord(%s): %v", rec.GetId(), err)
		}
	}
}

// putConcurrently stores records, at most width at a time. Seeding thousands
// of records one after another spends minutes in the local backend's
// per-record fsync; the order they are written in is not what any test
// observes, since every record carries its own time.
func putConcurrently(t *testing.T, store artifacts.AuditStore, width int, records ...*orkstackv1.AuditRecord) {
	t.Helper()
	sem := make(chan struct{}, width)
	errs := make(chan error, len(records))
	var wg sync.WaitGroup
	for _, rec := range records {
		wg.Add(1)
		sem <- struct{}{}
		go func(rec *orkstackv1.AuditRecord) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := store.PutAuditRecord(context.Background(), rec); err != nil {
				errs <- fmt.Errorf("PutAuditRecord(%s): %w", rec.GetId(), err)
			}
		}(rec)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func ids(records []*orkstackv1.AuditRecord) []string {
	out := make([]string, 0, len(records))
	for _, rec := range records {
		out = append(out, rec.GetId())
	}
	return out
}

func TestAuditStore_ListNewestFirstAcrossDays(t *testing.T) {
	day1 := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	day3 := day2.Add(24 * time.Hour)
	for name, store := range newAuditStores(t) {
		t.Run(name, func(t *testing.T) {
			a := auditRecord(day1, "ns", "namespace.update", "")
			b := auditRecord(day2, "ns", "namespace.update", "")
			c := auditRecord(day3, "ns", "namespace.update", "")
			c2 := auditRecord(day3.Add(time.Minute), "ns", "namespace.update", "")
			putAll(t, store, a, b, c, c2)
			ctx := context.Background()

			page, err := store.ListAuditRecords(ctx, artifacts.AuditQuery{
				Since: day1.Add(-time.Hour), Until: day3.Add(time.Hour), Limit: 50,
			})
			if err != nil {
				t.Fatalf("ListAuditRecords: %v", err)
			}
			want := []string{c2.GetId(), c.GetId(), b.GetId(), a.GetId()}
			if got := ids(page.Records); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("records = %v, want newest first %v", got, want)
			}
			if page.NextKey != "" {
				t.Errorf("NextKey = %q after the window was exhausted, want empty", page.NextKey)
			}

			// A window that excludes the middle day never lists it: [day3, day3+1h).
			page, err = store.ListAuditRecords(ctx, artifacts.AuditQuery{
				Since: day3, Until: day3.Add(time.Minute), Limit: 50,
			})
			if err != nil {
				t.Fatalf("ListAuditRecords(day3 only): %v", err)
			}
			// since inclusive (c at day3 exactly), until exclusive (c2 at day3+1m).
			if got := ids(page.Records); len(got) != 1 || got[0] != c.GetId() {
				t.Fatalf("records = %v, want exactly %s (since inclusive, until exclusive)", got, c.GetId())
			}

			// Until exclusive to the nanosecond; since inclusive to the nanosecond.
			page, err = store.ListAuditRecords(ctx, artifacts.AuditQuery{
				Since: day2.Add(time.Nanosecond), Until: day3.Add(time.Nanosecond), Limit: 50,
			})
			if err != nil {
				t.Fatalf("ListAuditRecords(ns bounds): %v", err)
			}
			if got := ids(page.Records); len(got) != 1 || got[0] != c.GetId() {
				t.Fatalf("records = %v, want only %s at the nanosecond bounds", got, c.GetId())
			}
		})
	}
}

func TestAuditStore_CursorContinuesWithoutOverlap(t *testing.T) {
	base := time.Date(2026, 3, 12, 23, 58, 0, 0, time.UTC)
	for name, store := range newAuditStores(t) {
		t.Run(name, func(t *testing.T) {
			var want []string
			for i := range 5 {
				// Straddle midnight so the cursor crosses a day boundary.
				rec := auditRecord(base.Add(time.Duration(i)*time.Minute), "ns", "namespace.update", "")
				putAll(t, store, rec)
				want = append([]string{rec.GetId()}, want...)
			}
			q := artifacts.AuditQuery{Since: base.Add(-time.Hour), Until: base.Add(time.Hour), Limit: 2}
			var got []string
			var sizes []int
			for range 10 {
				page, err := store.ListAuditRecords(context.Background(), q)
				if err != nil {
					t.Fatalf("ListAuditRecords: %v", err)
				}
				sizes = append(sizes, len(page.Records))
				got = append(got, ids(page.Records)...)
				if page.NextKey == "" {
					break
				}
				q.BeforeKey = page.NextKey
			}
			if fmt.Sprint(sizes) != "[2 2 1]" {
				t.Errorf("page sizes = %v, want [2 2 1]", sizes)
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("walked %v, want %v with no repeats", got, want)
			}
		})
	}
}

func TestAuditQuery_KeepSeesTheRecord(t *testing.T) {
	at := time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC)
	deployment := auditRecord(at, "", "retention.update", "svc-ci")
	payments := auditRecord(at.Add(time.Second), "payments", "orkflow.promote", "dsantos")
	payments.Target, payments.Succeeded, payments.ErrorCode = "deploy@3", false, "failed_precondition"
	if !(artifacts.AuditQuery{}).Matches(deployment) {
		t.Error("a query with no Keep dropped a record; nil Keep keeps every record")
	}
	for name, store := range newAuditStores(t) {
		t.Run(name, func(t *testing.T) {
			putAll(t, store, deployment, payments)
			var seen []*orkstackv1.AuditRecord
			page, err := store.ListAuditRecords(context.Background(), artifacts.AuditQuery{
				Since: at.Add(-time.Hour), Until: at.Add(time.Hour), Limit: 50,
				Keep: func(rec *orkstackv1.AuditRecord) bool {
					seen = append(seen, rec)
					return rec.GetNamespace() == ""
				},
			})
			if err != nil {
				t.Fatalf("ListAuditRecords: %v", err)
			}
			if len(page.Records) != 1 || page.Records[0].GetId() != deployment.GetId() {
				t.Errorf("listed %d records, want only the deployment-wide one Keep accepted", len(page.Records))
			}
			if len(seen) != 2 {
				t.Fatalf("Keep saw %d records, want both in the window", len(seen))
			}
			// Newest first: the payments record, decoded whole.
			got := seen[0]
			if got.GetId() != payments.GetId() || got.GetNamespace() != "payments" || got.GetAction() != "orkflow.promote" ||
				got.GetPrincipal() != "dsantos" || got.GetTarget() != "deploy@3" || got.GetSucceeded() ||
				got.GetErrorCode() != "failed_precondition" || !got.GetAt().AsTime().Equal(payments.GetAt().AsTime()) {
				t.Errorf("Keep saw %v, want the stored record %v", got, payments)
			}
		})
	}
}

func TestAuditStore_ScanBudgetRefuses(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test: every proof's unit gate runs it; make check skips it")
	}
	base := time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC)
	for name, store := range newAuditStores(t) {
		t.Run(name, func(t *testing.T) {
			records := make([]*orkstackv1.AuditRecord, 0, artifacts.AuditScanMaxObjects+1000)
			for i := range artifacts.AuditScanMaxObjects + 1000 {
				records = append(records, auditRecord(base.Add(time.Duration(i)*time.Second), "ns", "namespace.update", ""))
			}
			putConcurrently(t, store, 32, records...)
			q := artifacts.AuditQuery{
				Since: base, Until: base.Add(24 * time.Hour), Limit: 50,
				Keep: func(rec *orkstackv1.AuditRecord) bool { return rec.GetPrincipal() == "nobody-matches" },
			}
			page, err := store.ListAuditRecords(context.Background(), q)
			if !errors.Is(err, artifacts.ErrAuditScanBudget) {
				t.Fatalf("ListAuditRecords = %d records, %v; want ErrAuditScanBudget, never an empty page",
					len(page.Records), err)
			}
			// A filter that fills the page early never meets the budget.
			q.Keep = nil
			page, err = store.ListAuditRecords(context.Background(), q)
			if err != nil || len(page.Records) != 50 {
				t.Fatalf("matching listing = %d records, %v; want a full page", len(page.Records), err)
			}
		})
	}
}
