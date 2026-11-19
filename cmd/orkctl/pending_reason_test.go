package main

import (
	"bytes"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestRunGet_SaysWhyATaskWaits: `run get` prints one line per task, and per
// fan-out item, whose pending_reason the engine set, after the task table; a
// run with none prints nothing new, and a reason is printed terminal-safe
// (it can quote an object store's error).
func TestRunGet_SaysWhyATaskWaits(t *testing.T) {
	item := int32(3)
	run := &orkstackv1.Run{Id: "r", Status: orkstackv1.Status_STATUS_RUNNING, TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "fetch", Status: orkstackv1.Status_STATUS_RUNNING,
			PendingReason: "waiting for an Orker of release b1 on pool build; none takes work (1 stopping)"},
		{TaskId: "spread", Status: orkstackv1.Status_STATUS_RUNNING, Items: []*orkstackv1.TaskRun{
			{TaskId: "spread", Item: &item, Status: orkstackv1.Status_STATUS_PENDING,
				PendingReason: "waiting until 10:42:00 UTC to run again (\x1b]0;owned\x07)"},
		}},
		{TaskId: "done", Status: orkstackv1.Status_STATUS_SUCCEEDED},
	}}
	var out bytes.Buffer
	must.OKf(t, renderRun(&out, run), "renderRun")
	text := out.String()
	for _, want := range []string{
		"fetch waits: waiting for an Orker of release b1 on pool build; none takes work (1 stopping)",
		"spread[3] waits: waiting until 10:42:00 UTC to run again (",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("run get is missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "\x1b") {
		t.Errorf("a reason reached the terminal unescaped:\n%q", text)
	}
	if strings.Contains(text, "done waits") {
		t.Errorf("a task with no reason got a line:\n%s", text)
	}

	out.Reset()
	quiet := &orkstackv1.Run{Id: "r", Status: orkstackv1.Status_STATUS_RUNNING, TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "fetch", Status: orkstackv1.Status_STATUS_RUNNING},
	}}
	must.OKf(t, renderRun(&out, quiet), "renderRun")
	if strings.Contains(out.String(), "waits:") {
		t.Errorf("a run with no reason printed one:\n%s", out.String())
	}
}
