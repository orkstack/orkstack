package artifacts

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestWriteRecordedValues: a resolution written is the one read back; a
// second write is refused and changes nothing, whatever it holds.
func TestWriteRecordedValues(t *testing.T) {
	ctx := context.Background()
	f := newRecordFixture(t)
	if err := f.b.put(ctx, "artifacts/values/v", "application/json", f.values); err != nil {
		t.Fatal(err)
	}
	f.record.Values = &orkstackv1.ArtifactRef{Uri: f.b.locate("artifacts/values/v"), Sha256: hexOf(f.values), SizeBytes: int64(len(f.values))}
	sha, err := f.s.WriteResolutionDetail(ctx, f.detail)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := f.s.WriteResolutionDetail(ctx, f.detail); err != nil || again != sha {
		t.Fatalf("the same detail again: %s, %v", again, err)
	}
	f.record.DetailSha256 = sha
	recSHA, err := f.s.WriteRecordedValues(ctx, f.record)
	if err != nil {
		t.Fatal(err)
	}
	other := proto.Clone(f.record).(*orkstackv1.RecordedValues)
	other.Values, other.Rejected = nil, true
	if _, err := f.s.WriteRecordedValues(ctx, other); !errors.Is(err, ErrRecordExists) {
		t.Fatalf("a second record: %v", err)
	}
	key, err := RecordKey(RecordResolution, "ns", "ork-1", "approve", 0)
	if err != nil {
		t.Fatal(err)
	}
	read, err := f.s.ReadRecordedValues(ctx, SlotDoor, nil, key, wantResolution)
	if err != nil || read.Answer != RecordFound || read.Value.RecordSHA256() != recSHA || read.Value.Detail().GetPrincipal() != "alice" {
		t.Fatalf("read %+v, %v; want %s", read, err, recSHA)
	}
}

// TestWriteRecordedValues_Refuses: nothing the reader would not find is
// written, and a store that does not answer is never success.
func TestWriteRecordedValues_Refuses(t *testing.T) {
	ctx := context.Background()
	for name, edit := range map[string]func(*orkstackv1.RecordedValues){
		"no kind":                         func(r *orkstackv1.RecordedValues) { r.Kind = 0 },
		"no format":                       func(r *orkstackv1.RecordedValues) { r.SchemaVersion = 0 },
		"a format this build cannot read": func(r *orkstackv1.RecordedValues) { r.SchemaVersion = 2 },
		"no admission":                    func(r *orkstackv1.RecordedValues) { r.AdmissionSha256 = "" },
		"a rejection with values":         func(r *orkstackv1.RecordedValues) { r.Rejected = true },
		"no input binding":                func(r *orkstackv1.RecordedValues) { r.InputBinding = nil },
		"an absent follows row": func(r *orkstackv1.RecordedValues) {
			r.Follows[0] = &orkstackv1.RecordedSource{TaskId: "b", Absent: true}
		},
		"outputs under a scope": func(r *orkstackv1.RecordedValues) {
			r.Kind, r.TaskId, r.ResumeScope, r.Follows, r.DetailSha256 = orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, "", 5, nil, ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newRecordFixture(t)
			f.record.Values = &orkstackv1.ArtifactRef{Uri: "file:///v", Sha256: hexOf(f.values), SizeBytes: 5000}
			f.record.DetailSha256 = strings.Repeat("e", 64)
			edit(f.record)
			if _, err := f.s.WriteRecordedValues(ctx, f.record); !errors.Is(err, ErrInvalidRecordKey) {
				t.Fatalf("err = %v; want it refused", err)
			}
		})
	}
	f := newRecordFixture(t)
	f.detail.Reason = strings.Repeat("x", maxDetailReasonBytes+1)
	if _, err := f.s.WriteResolutionDetail(ctx, f.detail); !errors.Is(err, ErrInvalidRecordKey) {
		t.Fatalf("a detail with a long reason: %v", err)
	}
}

// TestAnswerClock: the store's own Date is used; one that something in front
// of the store kept or stamped, or that does not parse, is none.
func TestAnswerClock(t *testing.T) {
	sent := time.Date(2026, 11, 24, 12, 0, 0, 500_000_000, time.UTC)
	at := func(d time.Duration) string { return sent.Add(d).Format(http.TimeFormat) }
	for _, tc := range []struct {
		name string
		h    http.Header
		ok   bool
	}{
		{"the store's", http.Header{"Date": {at(0)}}, true},
		{"within the skew before", http.Header{"Date": {at(-59 * time.Second)}}, true},
		{"earlier than that", http.Header{"Date": {at(-62 * time.Second)}}, false},
		{"ahead of this host", http.Header{"Date": {at(time.Hour)}}, true},
		{"none", http.Header{}, false},
		{"not HTTP time", http.Header{"Date": {"yesterday"}}, false},
		{"kept", http.Header{"Date": {at(0)}, "Age": {"3"}}, false},
		{"relayed", http.Header{"Date": {at(0)}, "Via": {"1.1 front"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &AnswerClock{}
			c.note(sent, answerDate(tc.h))
			if now, ok := c.Now(time.Minute); ok != tc.ok || (ok && now.Format(http.TimeFormat) != tc.h.Get("Date")) {
				t.Fatalf("Now = %v, %v; want %v", now, ok, tc.ok)
			}
		})
	}
	if _, ok := new(AnswerClock).Now(time.Minute); ok {
		t.Fatal("a clock no answer was noted on gives an instant")
	}
}

// TestAnswerClock_ReadsTheStoresAnswers: a miss, a read and a create over
// HTTP each leave the store's own Date, and a request with no answer leaves
// none.
func TestAnswerClock_ReadsTheStoresAnswers(t *testing.T) {
	when := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Date", when.Format(http.TimeFormat))
		switch {
		case r.Method == http.MethodPut:
			w.Header().Set("ETag", `"e"`)
		case strings.HasSuffix(r.URL.Path, "/there"):
			w.Header().Set("Last-Modified", when.Format(http.TimeFormat))
			_, _ = w.Write([]byte("{}"))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
		}
	}))
	t.Cleanup(store.Close)
	b, err := openS3Blobs(context.Background(), Config{S3Bucket: "b", S3Endpoint: store.URL, S3PathStyle: true, S3AccessKey: "a", S3SecretKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	for name, ask := range map[string]func(context.Context) error{
		"a miss": func(ctx context.Context) error {
			_, _, err := b.getStored(ctx, "absent", 10)
			if !errors.Is(err, errBlobNotFound) {
				return fmt.Errorf("a miss answered %w", err)
			}
			return nil
		},
		"a read":   func(ctx context.Context) error { _, _, err := b.getStored(ctx, "there", 10); return err },
		"a create": func(ctx context.Context) error { return b.create(ctx, "new", "application/json", []byte("{}")) },
	} {
		ctx, clock := WithAnswerClock(context.Background())
		if err := ask(ctx); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if now, ok := clock.Now(time.Minute); !ok || !now.Equal(when) {
			t.Errorf("%s: the clock says %v, %v; want %v", name, now, ok, when)
		}
	}
	ctx, clock := WithAnswerClock(context.Background())
	if _, _, err := b.getStored(ctx, "there", 10); err != nil {
		t.Fatal(err)
	}
	store.Close()
	if _, _, err := b.getStored(ctx, "there", 10); err == nil {
		t.Fatal("a closed store answered")
	}
	if now, ok := clock.Now(time.Minute); ok {
		t.Fatalf("a request with no answer left %v", now)
	}
}
