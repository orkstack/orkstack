package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func hexOf(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

type recordFixture struct {
	t      *testing.T
	s      *RecordStore
	b      blobs
	record *orkstackv1.RecordedValues
	detail *orkstackv1.ResolutionDetail
	values []byte
}

func newRecordFixture(t *testing.T) *recordFixture {
	s, b := newTestRecordStore(t)
	s.reader.spacing = fast
	values := []byte(`{"approved":true}`)
	detail := &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "ns", RunId: "ork-1", TaskId: "approve",
		Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice", Reason: "ok"}
	return &recordFixture{t: t, s: s, b: b, values: values, detail: detail, record: &orkstackv1.RecordedValues{
		SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
		Namespace: "ns", RunId: "ork-1", TaskId: "approve", AdmissionSha256: strings.Repeat("a", 64),
		DefinitionDigest: "sha256:" + strings.Repeat("d", 64), InputBinding: make([]byte, 32),
		Follows: []*orkstackv1.RecordedSource{{TaskId: "build", RecordSha256: strings.Repeat("b", 64), ResultSha256: strings.Repeat("c", 64)}},
	}}
}

// write stores the fixture's values, detail and record, after edit, and
// answers the record's key.
func (f *recordFixture) write(edit func(*recordFixture)) string {
	f.t.Helper()
	ctx := context.Background()
	if edit != nil {
		edit(f)
	}
	valuesKey := "artifacts/values/" + hexOf(f.values)
	if err := f.b.put(ctx, valuesKey, "application/json", f.values); err != nil {
		f.t.Fatal(err)
	}
	if f.record.Values == nil && !f.record.Rejected {
		f.record.Values = &orkstackv1.ArtifactRef{Uri: f.b.locate(valuesKey), Sha256: hexOf(f.values), SizeBytes: int64(len(f.values))}
	}
	detail, err := MarshalResolutionDetail(f.detail)
	if err != nil {
		f.t.Fatal(err)
	}
	if f.record.DetailSha256 == "" {
		f.record.DetailSha256 = hexOf(detail)
	}
	detailKey, _ := ResolutionDetailKey("ns", "ork-1", "approve", hexOf(detail))
	if err := f.b.put(ctx, detailKey, "application/json", detail); err != nil {
		f.t.Fatal(err)
	}
	raw, err := MarshalRecordedValues(f.record)
	if err != nil {
		f.t.Fatal(err)
	}
	key, _ := RecordKey(RecordResolution, "ns", "ork-1", "approve", 0)
	if err := f.b.put(ctx, key, "application/json", raw); err != nil {
		f.t.Fatal(err)
	}
	return key
}

var wantResolution = RecordWant{Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
	Namespace: "ns", RunID: "ork-1", TaskID: "approve", AdmissionSHA256: strings.Repeat("a", 64)}

// TestReadRecordedValues_Found: a resolution whose detail and values are
// present and hash to their digests is found, and its values are adopted
// only in time: written at or before the deadline and the run's end.
func TestReadRecordedValues_Found(t *testing.T) {
	f := newRecordFixture(t)
	key := f.write(nil)
	got, err := f.s.ReadRecordedValues(context.Background(), SlotRun, nil, key, wantResolution)
	if err != nil || got.Answer != RecordFound {
		t.Fatalf("read = %+v, %v", got, err)
	}
	found := got.Value
	if found.Detail().GetPrincipal() != "alice" || found.StoredAt().IsZero() || len(found.RecordSHA256()) != 64 {
		t.Fatalf("found = %+v", found)
	}
	adopted, ok := found.InTime(found.StoredAt().Add(time.Minute), time.Time{})
	if !ok || string(adopted.Inline()) != string(f.values) || adopted.Record().GetTaskId() != "approve" {
		t.Fatalf("in time = %v, %+v", ok, adopted)
	}
	if _, ok := found.InTime(found.StoredAt().Add(-time.Hour), time.Time{}); ok {
		t.Fatal("a record written after its deadline was adopted")
	}
	if _, ok := found.InTime(found.StoredAt().Add(time.Hour), found.StoredAt().Add(-time.Hour)); ok {
		t.Fatal("a record written after its run ended was adopted")
	}
}

// TestReadRecordedValues_Inline: values are handed over with their record up to
// 4096 bytes, and are a reference alone past that.
func TestReadRecordedValues_Inline(t *testing.T) {
	for size, inline := range map[int]bool{2: true, maxInlineValues: true, maxInlineValues + 1: false} {
		f := newRecordFixture(t)
		f.values = []byte(`{"v":"` + strings.Repeat("x", max(size-8, 0)) + `"}`)
		if size == 2 {
			f.values = []byte(`{}`)
		}
		if len(f.values) != size {
			t.Fatalf("a fixture of %d bytes, want %d", len(f.values), size)
		}
		got, err := f.s.ReadRecordedValues(context.Background(), SlotRun, nil, f.write(nil), wantResolution)
		if err != nil || got.Answer != RecordFound {
			t.Fatalf("%d bytes: read = %+v, %v", size, got, err)
		}
		adopted, ok := got.Value.InTime(got.Value.StoredAt().Add(time.Minute), time.Time{})
		if !ok || (string(adopted.Inline()) == string(f.values)) != inline || (!inline && adopted.Inline() != nil) || adopted.Record().GetValues().GetSizeBytes() != int64(size) {
			t.Fatalf("%d bytes: %d inline, want inline %v", size, len(adopted.Inline()), inline)
		}
	}
}

// TestReadRecordedValues_BoundaryTable: one row per answer a record read
// gives: none, invalid naming which object, unsupported naming which
// object and its version.
func TestReadRecordedValues_BoundaryTable(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(*recordFixture)
		drop    string // an object removed after it was written
		want    RecordAnswer
		object  string
		version int64
	}{
		{"another run", func(f *recordFixture) { f.record.RunId = "ork-2" }, "", RecordInvalid, "record", 0},
		{"another task", func(f *recordFixture) { f.record.TaskId = "deploy" }, "", RecordInvalid, "record", 0},
		{"another scope", func(f *recordFixture) { f.record.ResumeScope = 3 }, "", RecordInvalid, "record", 0},
		{"another admission", func(f *recordFixture) { f.record.AdmissionSha256 = strings.Repeat("e", 64) }, "", RecordInvalid, "record", 0},
		{"another kind", func(f *recordFixture) {
			f.record.Kind = orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED
		}, "", RecordInvalid, "record", 0},
		{"no input binding", func(f *recordFixture) { f.record.InputBinding = nil }, "", RecordInvalid, "record", 0},
		{"a follows row with a short digest", func(f *recordFixture) { f.record.Follows[0].ResultSha256 = "c" }, "", RecordInvalid, "record", 0},
		{"format 0", func(f *recordFixture) { f.record.SchemaVersion = 0 }, "", RecordInvalid, "record", 0},
		{"an unknown format", func(f *recordFixture) { f.record.SchemaVersion = 7 }, "", RecordUnsupported, "record", 7},
		{"a detail of an unknown format", func(f *recordFixture) { f.detail.SchemaVersion = 9 }, "", RecordUnsupported, "detail", 9},
		{"a detail of another task", func(f *recordFixture) { f.detail.TaskId = "deploy" }, "", RecordInvalid, "detail", 0},
		{"a detail that does not hash", func(f *recordFixture) { f.record.DetailSha256 = strings.Repeat("f", 64) }, "", RecordInvalid, "detail", 0},
		{"no detail", nil, "detail", RecordInvalid, "detail", 0},
		{"a composed field on a resolution", func(f *recordFixture) { f.record.ChildRunId = "ork-9" }, "", RecordInvalid, "record", 0},
		{"sources on a resolution", func(f *recordFixture) {
			f.record.Sources = []*orkstackv1.RecordedSource{{TaskId: "x", Absent: true}}
		}, "", RecordInvalid, "record", 0},
		{"an empty run", func(f *recordFixture) { f.record.RunId = "" }, "", RecordInvalid, "record", 0},
		{"a follows row twice", func(f *recordFixture) { f.record.Follows = append(f.record.Follows, f.record.Follows[0]) }, "", RecordInvalid, "record", 0},
		{"values that do not hash", func(f *recordFixture) {
			f.record.Values = &orkstackv1.ArtifactRef{Uri: "file:///nope", Sha256: strings.Repeat("0", 64), SizeBytes: 2}
		}, "", RecordInvalid, "values", 0},
	}
	for _, tt := range tests {
		f := newRecordFixture(t)
		key := f.write(tt.edit)
		if tt.drop == "detail" {
			raw, _ := MarshalResolutionDetail(f.detail)
			dk, _ := ResolutionDetailKey("ns", "ork-1", "approve", hexOf(raw))
			_ = f.b.remove(context.Background(), dk)
		}
		got, err := f.s.ReadRecordedValues(context.Background(), SlotRun, nil, key, wantResolution)
		if err != nil || got.Answer != tt.want || got.Object != tt.object || got.Version != tt.version {
			t.Errorf("%s: %+v, %v; want %v of %s", tt.name, got, err, tt.want, tt.object)
		}
	}
	f := newRecordFixture(t)
	key, _ := RecordKey(RecordResolution, "ns", "ork-1", "approve", 0)
	if got, err := f.s.ReadRecordedValues(context.Background(), SlotRun, nil, key, wantResolution); err != nil || got.Answer != RecordNone {
		t.Fatalf("no record = %+v, %v", got, err)
	}
	f = newRecordFixture(t)
	f.write(nil)
	raw, _ := MarshalRecordedValues(f.record)
	big := proto.Clone(f.record).(*orkstackv1.RecordedValues)
	big.DefinitionDigest = strings.Repeat("x", maxRecordedValuesBytes)
	if _, err := MarshalRecordedValues(big); err == nil {
		t.Fatalf("a record over %d bytes was written (a record is %d)", maxRecordedValuesBytes, len(raw))
	}
}

// TestDecodeRecordedValues_RefusesNullsAndEmptyWants: a null where a value
// belongs is invalid, as in every other record, and a want that names no
// run or admission finds nothing.
func TestDecodeRecordedValues_RefusesNullsAndEmptyWants(t *testing.T) {
	raw := fuzzSeedRecord(t)
	nulled := strings.Replace(string(raw), `"runId": "ork-1"`, `"runId": null`, 1)
	if _, got := decodeRecordedValues([]byte(nulled), wantResolution); got.Answer != RecordInvalid {
		t.Fatalf("a null run = %+v", got)
	}
	nested := strings.Replace(string(raw), `"taskId": "build"`, `"taskId": null`, 1)
	if _, got := decodeRecordedValues([]byte(nested), wantResolution); got.Answer != RecordInvalid {
		t.Fatalf("a null inside a row = %+v", got)
	}
	empty := wantResolution
	empty.AdmissionSHA256 = ""
	if _, got := decodeRecordedValues(raw, empty); got.Answer != RecordInvalid {
		t.Fatalf("a want with no admission = %+v", got)
	}
	var zero RecordedAdoption
	if zero.Record() != nil || zero.Inline() != nil {
		t.Fatal("a zero adoption answered a record")
	}
}
