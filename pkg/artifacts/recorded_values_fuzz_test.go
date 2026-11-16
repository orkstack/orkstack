package artifacts

import (
	"bytes"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func fuzzSeedRecord(t testing.TB) []byte {
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
		Namespace: "ns", RunId: "ork-1", TaskId: "approve", AdmissionSha256: strings.Repeat("a", 64),
		InputBinding: make([]byte, 32), DetailSha256: strings.Repeat("d", 64),
		Values:  &orkstackv1.ArtifactRef{Uri: "file:///v", Sha256: strings.Repeat("e", 64), SizeBytes: 2},
		Follows: []*orkstackv1.RecordedSource{{TaskId: "build", RecordSha256: strings.Repeat("b", 64), ResultSha256: strings.Repeat("c", 64)}}}
	raw, err := MarshalRecordedValues(rec)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// FuzzRecordedValues: decoding never panics; a record of format 0 or a
// format this build does not know is never found; a found record names
// exactly what was asked, every row names a task with digests of the
// right length, and it holds at most 64 KiB.
func FuzzRecordedValues(f *testing.F) {
	seed := fuzzSeedRecord(f)
	f.Add(seed)
	f.Add(bytes.Replace(seed, []byte(`"schemaVersion": 1`), []byte(`"schemaVersion": 2`), 1))
	f.Add([]byte(`{"schemaVersion":1}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		rec, got := decodeRecordedValues(raw, wantResolution)
		if got.Answer != RecordFound {
			return
		}
		if len(raw) > maxRecordedValuesBytes || rec.GetSchemaVersion() != 1 || rec.GetRunId() != "ork-1" || rec.GetTaskId() != "approve" ||
			rec.GetKind() != wantResolution.Kind || rec.GetAdmissionSha256() != wantResolution.AdmissionSHA256 {
			t.Fatalf("found a record of something else: %v", rec)
		}
		for _, row := range append(rec.GetFollows(), rec.GetSources()...) {
			if validCommitTaskID(row.GetTaskId()) != nil || (!row.GetAbsent() && (len(row.GetRecordSha256()) != 64 || len(row.GetResultSha256()) != 64)) {
				t.Fatalf("a found record's row is malformed: %v", row)
			}
		}
	})
}

// FuzzResolutionDetail: decoding never panics; a found detail hashes to the
// digest the record names, names the record's run, task and scope, and is
// of a format this build reads.
func FuzzResolutionDetail(f *testing.F) {
	d := &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "ns", RunId: "ork-1", TaskId: "approve", Principal: "alice"}
	seed, _ := MarshalResolutionDetail(d)
	f.Add(seed)
	f.Add([]byte(`{"schemaVersion":9,"namespace":"ns"}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		rec := &orkstackv1.RecordedValues{Namespace: "ns", RunId: "ork-1", TaskId: "approve", DetailSha256: hexOf(raw)}
		got, read := decodeResolutionDetail(raw, rec)
		if read.Answer != RecordFound {
			return
		}
		if got.GetSchemaVersion() != 1 || got.GetRunId() != "ork-1" || got.GetTaskId() != "approve" || got.GetResumeScope() != 0 || len(got.GetReason()) > maxDetailReasonBytes {
			t.Fatalf("found a detail of something else: %v", got)
		}
	})
}
