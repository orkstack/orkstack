package main

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskStatusLabel_FlagsACanceledTaskThatNeverStarted(t *testing.T) {
	taskRun := &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_CANCELED}
	if got := taskStatusLabel(taskRun); got != "CANCELED (never started)" {
		t.Fatalf("taskStatusLabel(never-started CANCELED) = %q, want %q", got, "CANCELED (never started)")
	}
}

func TestTaskStatusLabel_LeavesACanceledTaskThatDidStartAlone(t *testing.T) {
	taskRun := &orkstackv1.TaskRun{
		Status:    orkstackv1.Status_STATUS_CANCELED,
		StartedAt: timestamppb.New(time.Now()),
	}
	if got := taskStatusLabel(taskRun); got != "CANCELED" {
		t.Fatalf("taskStatusLabel(started-then-canceled) = %q, want %q", got, "CANCELED")
	}
}

func TestTaskStatusLabel_LeavesEveryOtherStatusAlone(t *testing.T) {
	cases := []orkstackv1.Status{
		orkstackv1.Status_STATUS_PENDING,
		orkstackv1.Status_STATUS_RUNNING,
		orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_SKIPPED,
	}
	for _, status := range cases {
		taskRun := &orkstackv1.TaskRun{Status: status}
		want := statusLabel(status)
		if got := taskStatusLabel(taskRun); got != want {
			t.Fatalf("taskStatusLabel(%s) = %q, want %q", status, got, want)
		}
	}
}

// TestPayloadSummary_IsBoundedAndPointsAtTheWholeDocument: run get prints a
// payload on one line, so it is cut at payloadSummaryMax bytes and the cut
// names its size and the command that prints it whole; up to the bound it
// prints as it is.
func TestPayloadSummary_IsBoundedAndPointsAtTheWholeDocument(t *testing.T) {
	payload := func(n int) *orkstackv1.Payload {
		data, err := structpb.NewStruct(map[string]any{"k": strings.Repeat("a", n)})
		if err != nil {
			t.Fatalf("struct: %v", err)
		}
		return &orkstackv1.Payload{Data: data}
	}
	// `{"k":"` + n bytes + `"}` is n+8 bytes of JSON.
	under := `{"k":"` + strings.Repeat("a", 10) + `"}`
	at := `{"k":"` + strings.Repeat("a", 504) + `"}`
	over := (`{"k":"` + strings.Repeat("a", 1000) + `"}`)[:512]
	for _, tc := range []struct {
		name      string
		payload   *orkstackv1.Payload
		namespace string
		want      string
	}{
		{"empty", nil, "", "-"},
		{"under the bound", payload(10), "", under},
		{"at the bound", payload(504), "", at},
		{"over the bound", payload(1000), "",
			over + "… (1008 bytes; orkctl run get ork-1 --json prints it whole)"},
		{"over the bound, in a namespace", payload(1000), "payments",
			over + "… (1008 bytes; orkctl -n payments run get ork-1 --json prints it whole)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := payloadSummary(tc.payload, "ork-1", tc.namespace); got != tc.want {
				t.Fatalf("payloadSummary =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

// TestPayloadSummary_NeverSplitsACharacter: the cut lands on a UTF-8
// boundary, so the line stays valid text.
func TestPayloadSummary_NeverSplitsACharacter(t *testing.T) {
	data, err := structpb.NewStruct(map[string]any{"k": strings.Repeat("é", 400)})
	if err != nil {
		t.Fatalf("struct: %v", err)
	}
	got := payloadSummary(&orkstackv1.Payload{Data: data}, "ork-1", "")
	head, _, ok := strings.Cut(got, "… (")
	if !ok {
		t.Fatalf("an 800-byte payload was not cut: %q", got)
	}
	if !utf8.ValidString(head) || len(head) > payloadSummaryMax {
		t.Fatalf("cut = %q (%d bytes), want valid UTF-8 within %d bytes", head, len(head), payloadSummaryMax)
	}
}
