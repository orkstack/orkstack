package main

import (
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
