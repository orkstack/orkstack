package api

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The lookup rule on its own: the outcome attempt's record first, then
// the newest SUCCESSFUL adopter, and never any other record.
func TestOutcomeRecord_PrefersTheOutcomeThenTheNewestSuccessfulAdopter(t *testing.T) {
	record := func(attempt int32, outcome orkstackv1.AttemptOutcome, adopted bool, digest string) *orkstackv1.TaskAttempt {
		return &orkstackv1.TaskAttempt{
			Attempt:       attempt,
			Outcome:       outcome,
			CommitAdopted: adopted,
			Image:         &orkstackv1.ImageRef{PlatformDigest: digest},
		}
	}
	const (
		succeeded = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
		failed    = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
		canceled  = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED
	)
	cases := []struct {
		name    string
		records []*orkstackv1.TaskAttempt
		attempt int32
		want    string // the chosen record's platform digest; "" means none
	}{
		{"the outcome record", []*orkstackv1.TaskAttempt{
			record(1, failed, false, "x1"), record(2, failed, false, "x2"), record(3, succeeded, false, "D"),
		}, 3, "D"},
		{"crash then adopt: record 1 gone, adopter 2 answers", []*orkstackv1.TaskAttempt{
			record(2, succeeded, true, "D"),
		}, 1, "D"},
		{"a failed record is never an adopter", []*orkstackv1.TaskAttempt{
			record(2, failed, false, "X"), record(3, succeeded, true, "D"),
		}, 1, "D"},
		{"a failed record alone answers nothing", []*orkstackv1.TaskAttempt{
			record(2, failed, false, "X"),
		}, 1, ""},
		{"a failed record that claims adoption is not a successful adopter", []*orkstackv1.TaskAttempt{
			record(2, failed, true, "X"),
		}, 1, ""},
		{"a canceled record answers nothing", []*orkstackv1.TaskAttempt{
			record(2, canceled, false, "X"),
		}, 1, ""},
		{"a successful non-adopting record under another number answers nothing", []*orkstackv1.TaskAttempt{
			record(3, succeeded, false, "X"),
		}, 2, ""},
		{"the newest of two adopters", []*orkstackv1.TaskAttempt{
			record(2, succeeded, true, "D2"), record(3, succeeded, true, "D3"),
		}, 1, "D3"},
		{"the newest adopter whatever the listing order", []*orkstackv1.TaskAttempt{
			record(3, succeeded, true, "D3"), record(2, succeeded, true, "D2"),
		}, 1, "D3"},
		{"the outcome record beats an adopter", []*orkstackv1.TaskAttempt{
			record(1, succeeded, false, "R1"), record(3, succeeded, true, "D"),
		}, 1, "R1"},
		{"no records", nil, 1, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := outcomeRecord(tc.records, tc.attempt)
			if tc.want == "" {
				if ok || got != nil {
					t.Fatalf("outcomeRecord chose attempt %d; want none", got.GetAttempt())
				}
				return
			}
			if !ok {
				t.Fatalf("outcomeRecord chose nothing; want %s", tc.want)
			}
			if got.GetImage().GetPlatformDigest() != tc.want {
				t.Fatalf("outcomeRecord chose %s (attempt %d); want %s",
					got.GetImage().GetPlatformDigest(), got.GetAttempt(), tc.want)
			}
		})
	}
}
