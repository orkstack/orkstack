package artifacts

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// flakySource is an auditSource whose listing and bodies a test controls: a
// key it lists may have no body (a lifecycle rule expired it in between), and
// a body may be anything at all.
type flakySource struct {
	bodies map[string][]byte // nil body = vanished after listing
}

func (f *flakySource) listAuditDay(_ context.Context, dayPrefix string) ([]string, error) {
	var keys []string
	for key := range f.bodies {
		if strings.HasPrefix(key, dayPrefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (f *flakySource) fetchAudit(_ context.Context, key string) ([]byte, bool, error) {
	body := f.bodies[key]
	if body == nil {
		return nil, false, nil
	}
	return body, true, nil
}

func TestAuditStore_SkipsUnknownSchemaAndVanishedBody(t *testing.T) {
	at := time.Date(2026, 3, 14, 11, 0, 0, 0, time.UTC)
	mk := func(offset time.Duration, schema int32) (*orkstackv1.AuditRecord, string, []byte) {
		rec := &orkstackv1.AuditRecord{
			SchemaVersion: schema,
			Id:            NewAuditID(at.Add(offset)),
			At:            timestamppb.New(at.Add(offset)),
			Action:        "namespace.update",
		}
		key, err := AuditKey(rec)
		if err != nil {
			t.Fatalf("AuditKey: %v", err)
		}
		body, err := marshalAuditExact(rec)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return rec, key, body
	}
	good, goodKey, goodBody := mk(0, AuditSchemaVersion)
	_, futureKey, futureBody := mk(time.Minute, AuditSchemaVersion+1)
	_, vanishedKey, _ := mk(2*time.Minute, AuditSchemaVersion)
	_, garbageKey, _ := mk(3*time.Minute, AuditSchemaVersion)
	// A body whose `at` is on another day than its key.
	moved, movedKey, _ := mk(4*time.Minute, AuditSchemaVersion)
	moved.At = timestamppb.New(at.Add(-48 * time.Hour))
	movedBody, err := marshalAuditExact(moved)
	if err != nil {
		t.Fatalf("marshal moved: %v", err)
	}

	src := &flakySource{bodies: map[string][]byte{
		goodKey:     goodBody,
		futureKey:   futureBody,
		vanishedKey: nil,
		garbageKey:  []byte("{not json"),
		movedKey:    movedBody,
	}}
	page, err := walkAudit(context.Background(), src, AuditQuery{
		Since: at.Add(-time.Hour), Until: at.Add(time.Hour), Limit: 50,
	})
	if err != nil {
		t.Fatalf("walkAudit: %v", err)
	}
	if len(page.Records) != 1 || page.Records[0].GetId() != good.GetId() {
		var got []string
		for _, rec := range page.Records {
			got = append(got, rec.GetId())
		}
		t.Fatalf("records = %v, want only the readable current-schema record %s", got, good.GetId())
	}
}
