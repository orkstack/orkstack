package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The run detail task table says WHERE a task ran, not only how it ended:
// POOL (which set of Orkers was asked), PIN (which group of tasks was held
// together) and ORKER (which process actually produced the outcome). PIN and
// ORKER read together are the client-visible proof a pin held — two tasks with
// one pin must show one orker — so the table is asserted as whole rows rather
// than as the presence of a substring.
//
// ORKER is populated for every task that reached an Orker, pinned or not. A
// dash in that column therefore means "never reached one" (skipped, canceled
// before dispatch, or a host that died before reporting), never "not pinned".
func TestRenderRun_TaskTableSaysPoolPinAndOrker(t *testing.T) {
	at := timestamppb.New(fixedNow)

	tests := []struct {
		name     string
		taskRuns []*orkstackv1.TaskRun
		// wantRows are the task rows expected under the header, with runs of
		// spaces collapsed: the column WIDTHS are tabwriter's business, the
		// column VALUES are this test's.
		wantRows []string
	}{
		{
			// Backward compatibility: an orkflow that pins nothing renders the
			// three new cells as dashes and changes nothing else. The pool is
			// still shown, because a task always has one even when unpinned.
			name: "unpinned tasks dash the pin and keep every other column",
			taskRuns: []*orkstackv1.TaskRun{
				{
					TaskId: "fetch", Status: orkstackv1.Status_STATUS_SUCCEEDED,
					Pool: "main", OrkerId: "orker-a",
					Attempt: 1, ExitCode: 0, StartedAt: at, FinishedAt: at,
				},
			},
			wantRows: []string{
				"fetch SUCCEEDED main - orker-a 1 0 2026-01-02T03:04:05Z 2026-01-02T03:04:05Z -",
			},
		},
		{
			// The proof: prep and build share one pin, so they must show one
			// orker; publish is unpinned and shows its own.
			name: "one pin means one orker across its members",
			taskRuns: []*orkstackv1.TaskRun{
				{
					TaskId: "prep", Status: orkstackv1.Status_STATUS_SUCCEEDED,
					Pool: "main", Pin: "build", OrkerId: "orker-a",
					Attempt: 1, StartedAt: at, FinishedAt: at,
				},
				{
					TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED,
					Pool: "main", Pin: "build", OrkerId: "orker-a",
					Attempt: 1, StartedAt: at, FinishedAt: at,
				},
				{
					TaskId: "publish", Status: orkstackv1.Status_STATUS_SUCCEEDED,
					Pool: "main", OrkerId: "orker-b",
					Attempt: 1, StartedAt: at, FinishedAt: at,
				},
			},
			wantRows: []string{
				"prep SUCCEEDED main build orker-a 1 0 2026-01-02T03:04:05Z 2026-01-02T03:04:05Z -",
				"build SUCCEEDED main build orker-a 1 0 2026-01-02T03:04:05Z 2026-01-02T03:04:05Z -",
				"publish SUCCEEDED main - orker-b 1 0 2026-01-02T03:04:05Z 2026-01-02T03:04:05Z -",
			},
		},
		{
			// A task that never reached an Orker has no orker to name, and a
			// task with no pool request falls through to a dash as well. Both
			// absences are stated, not blanked.
			name: "a task that never reached an orker dashes pool pin and orker",
			taskRuns: []*orkstackv1.TaskRun{
				{
					TaskId: "skipped", Status: orkstackv1.Status_STATUS_SKIPPED,
				},
			},
			wantRows: []string{
				"skipped SKIPPED - - - 0 0 - - -",
			},
		},
		{
			// A pinned task whose host died: the pin is known from the
			// definition, the orker that lost it is known from the failure
			// detail, and the error still lands in the last column.
			name: "a failed pinned task keeps its pin its orker and its error",
			taskRuns: []*orkstackv1.TaskRun{
				{
					TaskId: "build", Status: orkstackv1.Status_STATUS_FAILED,
					Pool: "main", Pin: "build", OrkerId: "orker-a",
					Attempt: 1, StartedAt: at,
					Error: &orkstackv1.Error{
						Code:    "PIN_HOST_LOST",
						Message: "the orker hosting this pin disappeared",
					},
				},
			},
			wantRows: []string{
				"build FAILED main build orker-a 1 0 2026-01-02T03:04:05Z - " +
					"PIN_HOST_LOST: the orker hosting this pin disappeared",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out strings.Builder
			run := &orkstackv1.Run{
				Id: "run-001", OrkflowName: "pinned-build", OrkflowVersion: "0.1.0",
				Status: orkstackv1.Status_STATUS_SUCCEEDED, TaskRuns: test.taskRuns,
			}
			if err := renderRun(&out, run); err != nil {
				t.Fatalf("renderRun: %v", err)
			}
			rows := taskTableRows(t, out.String())
			if len(rows) != len(test.wantRows)+1 {
				t.Fatalf("task table has %d rows, want %d (header + %d tasks):\n%s",
					len(rows), len(test.wantRows)+1, len(test.wantRows), out.String())
			}
			wantHeader := "TASK STATUS POOL PIN ORKER ATTEMPT EXIT STARTED FINISHED ERROR"
			if rows[0] != wantHeader {
				t.Errorf("header = %q, want %q", rows[0], wantHeader)
			}
			for i, want := range test.wantRows {
				if rows[i+1] != want {
					t.Errorf("row %d =\n  %q\nwant\n  %q", i, rows[i+1], want)
				}
			}
		})
	}
}

// taskTableRows returns the header and task rows of a rendered run with runs
// of spaces collapsed to one, so an assertion states column values without
// restating tabwriter's padding.
func taskTableRows(t *testing.T, rendered string) []string {
	t.Helper()
	_, table, found := strings.Cut(rendered, "\nTASK")
	if !found {
		t.Fatalf("rendered run has no task table:\n%s", rendered)
	}
	var rows []string
	for _, line := range strings.Split("TASK"+table, "\n") {
		if collapsed := strings.Join(strings.Fields(line), " "); collapsed != "" {
			rows = append(rows, collapsed)
		}
	}
	return rows
}
