package artifacts

import (
	"strings"
	"testing"
)

func validResume() ResumeRecord {
	return ResumeRecord{Namespace: "ns", RunID: "ork-1", Scope: 2, N: 0, PreviousScope: 1,
		Tasks: []string{"build", "test"}, ReopenedAwaits: []string{}}
}

// TestResumeRecord_RoundTrips: what EncodeResumeRecord writes decodes as
// found, naming exactly what it was written for.
func TestResumeRecord_RoundTrips(t *testing.T) {
	r := validResume()
	raw, err := EncodeResumeRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"v":1,"namespace":"ns","run_id":"ork-1","scope":2,"n":0,"previous_scope":1,"tasks":["build","test"],"reopened_awaits":[]}` + "\n"
	if string(raw) != want {
		t.Fatalf("encoded = %s", raw)
	}
	got := DecodeResumeRecord(raw, "ns", "ork-1", 2, 0)
	if got.Answer != RecordFound || got.Value.Tasks[1] != "test" || got.Value.ReopenedAwaits == nil {
		t.Fatalf("decoded = %+v", got)
	}
}

// TestResumeRecord_RefusesWhatItCannotTrust: a record missing a key, with a
// null, an unknown key, naming another run, scope or plan, with a previous
// scope not below its own, or a task id that is not a name, is invalid; one
// of an unknown version is unsupported, and version 0 is invalid.
func TestResumeRecord_RefusesWhatItCannotTrust(t *testing.T) {
	enc := func(edit func(*ResumeRecord)) string {
		r := validResume()
		edit(&r)
		raw, _ := encodeResume(r)
		return string(raw)
	}
	base := enc(func(*ResumeRecord) {})
	tests := []struct {
		name string
		raw  string
		want RecordAnswer
	}{
		{"not JSON", "{", RecordInvalid},
		{"no reopened_awaits", strings.Replace(base, `,"reopened_awaits":[]`, "", 1), RecordInvalid},
		{"no previous_scope", strings.Replace(base, `"previous_scope":1,`, "", 1), RecordInvalid},
		{"a null list", strings.Replace(base, `"reopened_awaits":[]`, `"reopened_awaits":null`, 1), RecordInvalid},
		{"an unknown key", strings.Replace(base, `{"v":1,`, `{"v":1,"extra":1,`, 1), RecordInvalid},
		{"another run", enc(func(r *ResumeRecord) { r.RunID = "ork-2" }), RecordInvalid},
		{"another namespace", enc(func(r *ResumeRecord) { r.Namespace = "other" }), RecordInvalid},
		{"another scope", enc(func(r *ResumeRecord) { r.Scope = 3 }), RecordInvalid},
		{"another plan", enc(func(r *ResumeRecord) { r.N = 1 }), RecordInvalid},
		{"a previous scope not below", enc(func(r *ResumeRecord) { r.PreviousScope = 2 }), RecordInvalid},
		{"a task that is not a name", enc(func(r *ResumeRecord) { r.Tasks = []string{"../x"} }), RecordInvalid},
		{"a task twice", enc(func(r *ResumeRecord) { r.Tasks = []string{"a", "a"} }), RecordInvalid},
		{"version 0", strings.Replace(base, `{"v":1,`, `{"v":0,`, 1), RecordInvalid},
		{"version 2", strings.Replace(base, `{"v":1,`, `{"v":2,`, 1), RecordUnsupported},
		{"too large", base[:len(base)-2] + strings.Repeat(" ", maxResumeRecordBytes) + "}", RecordInvalid},
	}
	for _, tt := range tests {
		got := DecodeResumeRecord([]byte(tt.raw), "ns", "ork-1", 2, 0)
		if got.Answer != tt.want || (tt.want == RecordInvalid && got.Reason == "") {
			t.Errorf("%s: %+v, want %v", tt.name, got, tt.want)
		}
	}
}

// FuzzResumeScopeRecord: decoding never panics; a found record names
// exactly the run, scope and plan asked for, its previous scope is below
// its scope, every task id is a name and none is given twice, and it
// encodes back to bytes that decode to the same record.
func FuzzResumeScopeRecord(f *testing.F) {
	raw, _ := EncodeResumeRecord(validResume())
	f.Add(raw)
	f.Add([]byte(`{"v":2}`))
	f.Add([]byte(`{"v":1,"namespace":"ns","run_id":"ork-1","scope":2,"n":0,"previous_scope":1,"tasks":[],"reopened_awaits":null}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		got := DecodeResumeRecord(raw, "ns", "ork-1", 2, 0)
		if got.Answer != RecordFound {
			return
		}
		r := got.Value
		if r.Namespace != "ns" || r.RunID != "ork-1" || r.Scope != 2 || r.N != 0 || r.PreviousScope >= r.Scope {
			t.Fatalf("found a record of something else: %+v", r)
		}
		again, err := EncodeResumeRecord(r)
		if err != nil {
			t.Fatalf("a found record does not encode: %v", err)
		}
		if back := DecodeResumeRecord(again, "ns", "ork-1", 2, 0); back.Answer != RecordFound {
			t.Fatalf("re-encoded record decodes as %+v", back)
		}
	})
}

// TestResumeLink_RoundTrips: a link names one scope, and anything else
// at a link's key is invalid or unsupported.
func TestResumeLink_RoundTrips(t *testing.T) {
	sha := strings.Repeat("a", 64)
	raw := encodeResumeLink(resumeLink{Scope: 7, N: 2, RecordSHA256: sha})
	if string(raw) != `{"v":1,"scope":7,"n":2,"record_sha256":"`+sha+`"}`+"\n" {
		t.Fatalf("link = %s", raw)
	}
	if l, a, _ := decodeResumeLink(raw); a != RecordFound || l.Scope != 7 || l.N != 2 || l.RecordSHA256 != sha {
		t.Fatalf("decoded = %+v, %v", l, a)
	}
	for raw, want := range map[string]RecordAnswer{
		`{"v":1}`: RecordInvalid, `{"v":1,"scope":7,"n":0,"record_sha256":"` + sha + `","x":1}`: RecordInvalid,
		`{"v":1,"scope":7,"n":0,"record_sha256":"x"}`: RecordInvalid, `{"v":1,"scope":7,"n":999,"record_sha256":"` + sha + `"}`: RecordInvalid,
		`{"v":3,"scope":7}`: RecordUnsupported, `7`: RecordInvalid,
	} {
		if _, a, _ := decodeResumeLink([]byte(raw)); a != want {
			t.Errorf("%s = %v, want %v", raw, a, want)
		}
	}
}
