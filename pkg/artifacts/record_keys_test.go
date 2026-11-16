package artifacts

import (
	"errors"
	"testing"
)

// TestRecordKeys_RenderTheDocumentedLayout pins every identity record key:
// the attested twin of a commit key, the three records of a task or a run,
// a resolution's detail, and a resume's record and scope link
// (docs/STORAGE.md once a release writes them).
func TestRecordKeys_RenderTheDocumentedLayout(t *testing.T) {
	const ns, run = "payments", "ork-1700000000-1"
	tests := []struct {
		name string
		got  func() (string, error)
		want string
	}{
		{"a commit's attested twin", func() (string, error) {
			k, _ := CommitKey(ns, run, "migrate")
			return AttestedCommitKey(k)
		}, "metadata/attested/payments/ork-1700000000-1/migrate/commit.json"},
		{"an item's attested twin", func() (string, error) {
			k, _ := ItemCommitKey(ns, run, "convert", 3)
			return AttestedCommitKey(k)
		}, "metadata/attested/payments/ork-1700000000-1/convert/00003/commit.json"},
		{"a resolution, never resumed", func() (string, error) { return RecordKey(RecordResolution, ns, run, "approve", 0) },
			"metadata/attested/payments/ork-1700000000-1/approve/resolution.json"},
		{"a resolution in a root's resume scope", func() (string, error) { return RecordKey(RecordResolution, ns, run, "approve", 2) },
			"metadata/attested/payments/ork-1700000000-1/approve/r00000000000000000002/resolution.json"},
		{"a composed task's values", func() (string, error) { return RecordKey(RecordComposed, ns, run, "child", 0) },
			"metadata/attested/payments/ork-1700000000-1/child/composed.json"},
		{"a composed task's values in a resume scope", func() (string, error) { return RecordKey(RecordComposed, ns, run, "child", 1) },
			"metadata/attested/payments/ork-1700000000-1/child/r00000000000000000001/composed.json"},
		{"a run's outputs", func() (string, error) { return OutputsRecordKey(ns, run) },
			"metadata/attested/payments/ork-1700000000-1/outputs.json"},
		{"a run's records, listed", func() (string, error) { return AttestedRunPrefix(ns, run) },
			"metadata/attested/payments/ork-1700000000-1/"},
		{"a resolution's detail", func() (string, error) {
			return ResolutionDetailKey(ns, run, "approve", "aa0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd")
		}, "metadata/snapshots/payments/ork-1700000000-1/identity/resolutions/approve/aa0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd.json"},
		{"a resume's record", func() (string, error) { return ResumeRecordKey(ns, run, 3, 0) },
			"metadata/snapshots/payments/ork-1700000000-1/identity/resumes/3/000.json"},
		{"a resume's record, a later plan", func() (string, error) { return ResumeRecordKey(ns, run, 3, 12) },
			"metadata/snapshots/payments/ork-1700000000-1/identity/resumes/3/012.json"},
		{"the first resume's link", func() (string, error) { return ResumeLinkKey(ns, run, 0) },
			"metadata/snapshots/payments/ork-1700000000-1/identity/resumes/0/next.json"},
	}
	for _, tt := range tests {
		got, err := tt.got()
		if err != nil || got != tt.want {
			t.Errorf("%s = %q, %v; want %q", tt.name, got, err, tt.want)
		}
	}
}

// TestRecordKeys_RefuseWhatCannotBeAddressed: each coordinate is validated
// as a commit key's is, and nothing else maps to an attested key.
func TestRecordKeys_RefuseWhatCannotBeAddressed(t *testing.T) {
	const run = "ork-1"
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for name, call := range map[string]func() (string, error){
		"a namespace that is not a name": func() (string, error) { return RecordKey(RecordResolution, "Pay/ments", run, "a", 0) },
		"a task id that is not a name":   func() (string, error) { return RecordKey(RecordResolution, "ns", run, "../a", 0) },
		"an empty run id":                func() (string, error) { return RecordKey(RecordComposed, "ns", "", "a", 0) },
		"an unknown kind":                func() (string, error) { return RecordKey(RecordKind(9), "ns", run, "a", 0) },
		"outputs by task":                func() (string, error) { return RecordKey(RecordOutputs, "ns", run, "a", 0) },
		"not a commit key":               func() (string, error) { return AttestedCommitKey("metadata/runs/ns/x/commit.json") },
		"a commit key that escapes":      func() (string, error) { return AttestedCommitKey("metadata/commits/../secret/commit.json") },
		"a commit key of another leaf":   func() (string, error) { return AttestedCommitKey("metadata/commits/ns/r/t/values.json") },
		"a commit key in scope 0": func() (string, error) {
			return AttestedCommitKey("metadata/commits/ns/r/t/r00000000000000000000/commit.json")
		},
		"a detail digest of hex length": func() (string, error) { return ResolutionDetailKey("ns", run, "a", digest[:63]) },
		"a detail digest in upper case": func() (string, error) { return ResolutionDetailKey("ns", run, "a", "A"+digest[1:]) },
		"a plan number past 998":        func() (string, error) { return ResumeRecordKey("ns", run, 1, 999) },
		"a negative plan number":        func() (string, error) { return ResumeRecordKey("ns", run, 1, -1) },
	} {
		if _, err := call(); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("%s: %v, want ErrInvalidRecordKey", name, err)
		}
	}
}
