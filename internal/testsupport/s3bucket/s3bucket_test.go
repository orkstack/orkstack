package s3bucket_test

import (
	"regexp"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
)

// validBucket is S3's bucket naming rule for the subset Name produces:
// 3–63 bytes of lowercase letters, digits and hyphens, starting and ending
// with a letter or digit.
var validBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func TestName_IsAValidBucketUniqueToTheProcess(t *testing.T) {
	cases := []struct {
		name   string
		label  string
		pid    int
		suffix int64
		want   string
	}{
		{"ordinary", "orkctl", 89965, 1791094506683583630, "orkstack-orkctl-89965-683583630"},
		{"negative suffix", "ork", 1, -42, "orkstack-ork-1-42"},
		{"longest label, largest pid", "abcdefghij-klmnopqrs", 4194304, 999_999_999, "orkstack-abcdefghij-klmnopqrs-4194304-999999999"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := s3bucket.Name(tc.label, tc.pid, tc.suffix)
			if got != tc.want {
				t.Fatalf("Name(%q, %d, %d) = %q, want %q", tc.label, tc.pid, tc.suffix, got, tc.want)
			}
			if !validBucket.MatchString(got) {
				t.Fatalf("Name(%q, %d, %d) = %q is not a valid S3 bucket name", tc.label, tc.pid, tc.suffix, got)
			}
		})
	}
	if a, b := s3bucket.Name("orkctl", 7, 1), s3bucket.Name("orkctl", 8, 1); a == b {
		t.Fatalf("two processes share bucket %q", a)
	}
}

func TestName_RefusesALabelThatCannotFormABucket(t *testing.T) {
	for _, label := range []string{"", "Orkctl", "1orkctl", "ork_ctl", "a-label-far-too-long-for-it"} {
		t.Run(label, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("Name(%q, ...) did not refuse the label", label)
				}
			}()
			s3bucket.Name(label, 1, 1)
		})
	}
}
