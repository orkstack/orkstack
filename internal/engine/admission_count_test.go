package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// runningCorpus is three runs in flight in payments, one in billing, and one
// finished run in payments that must never be counted.
func runningCorpus() []statsCorpusRun {
	now := time.Date(2026, 9, 22, 2, 14, 0, 0, time.UTC)
	return []statsCorpusRun{
		{id: "ork-p1", namespace: "payments", orkflow: "reconcile", createdAt: now.Add(-3 * time.Minute)},
		{id: "ork-p2", namespace: "payments", orkflow: "reconcile", createdAt: now.Add(-2 * time.Minute)},
		{id: "ork-p3", namespace: "payments", orkflow: "reconcile", createdAt: now.Add(-time.Minute)},
		{id: "ork-b1", namespace: "billing", orkflow: "close", createdAt: now.Add(-time.Minute)},
		{
			id: "ork-p-done", namespace: "payments", orkflow: "reconcile",
			createdAt: now.Add(-time.Hour), closedAt: now.Add(-50 * time.Minute),
			status: orkstackv1.Status_STATUS_SUCCEEDED,
		},
	}
}

func TestRunningCount_UsesCountWhenIndexedAndTheMemoWalkOtherwise(t *testing.T) {
	ctx := context.Background()

	t.Run("indexed: one count with the namespace predicate, no list", func(t *testing.T) {
		index := statsIndexOver(t, runningCorpus())
		reader := newRunReader(index, converter.GetDefaultDataConverter()).WithNamespaceIndex(true)
		got, err := reader.RunningCount(ctx, "payments")
		if err != nil {
			t.Fatalf("RunningCount: %v", err)
		}
		if got != 3 {
			t.Errorf("RunningCount(payments) = %d, want 3", got)
		}
		if len(index.counts) != 1 || len(index.queries) != 0 {
			t.Fatalf("counts %q, lists %q; want one count and no list", index.counts, index.queries)
		}
		want := fmt.Sprintf("WorkflowType = %q AND ExecutionStatus = %q AND %s = %q",
			RunWorkflowType, executionStatusRunning, task.SearchAttrOrkflowNamespace, "payments")
		if index.counts[0] != want {
			t.Errorf("count query = %q, want %q", index.counts[0], want)
		}
	})

	t.Run("indexed: the default namespace resolves", func(t *testing.T) {
		index := statsIndexOver(t, []statsCorpusRun{{id: "ork-d", namespace: "default", createdAt: time.Now()}})
		reader := newRunReader(index, converter.GetDefaultDataConverter()).WithNamespaceIndex(true)
		got, err := reader.RunningCount(ctx, "")
		if err != nil || got != 1 {
			t.Fatalf("RunningCount(\"\") = %d, %v; want 1", got, err)
		}
		if !strings.Contains(index.counts[0], `= "default"`) {
			t.Errorf("count query %q does not name the default namespace", index.counts[0])
		}
	})

	t.Run("unindexed: no count, a memo-filtered walk", func(t *testing.T) {
		index := statsIndexOver(t, runningCorpus())
		reader := newRunReader(index, converter.GetDefaultDataConverter())
		got, err := reader.RunningCount(ctx, "payments")
		if err != nil {
			t.Fatalf("RunningCount: %v", err)
		}
		if got != 3 {
			t.Errorf("RunningCount(payments) = %d, want 3 (billing's run and the finished one excluded)", got)
		}
		if len(index.counts) != 0 {
			t.Errorf("counts = %q, want none: the cluster cannot count a namespace it does not index", index.counts)
		}
		if len(index.queries) == 0 {
			t.Fatal("no list reached the index")
		}
		for _, query := range index.queries {
			if strings.Contains(query, task.SearchAttrOrkflowNamespace) {
				t.Errorf("unindexed walk pushed the namespace predicate: %q", query)
			}
		}
		billing, err := reader.RunningCount(ctx, "billing")
		if err != nil || billing != 1 {
			t.Errorf("RunningCount(billing) = %d, %v; want 1", billing, err)
		}
	})

	t.Run("unindexed: running out of pages is an error, not a number", func(t *testing.T) {
		index := statsIndexOver(t, runningCorpus())
		index.endless = true
		reader := newRunReader(index, converter.GetDefaultDataConverter())
		got, err := reader.RunningCount(ctx, "payments")
		if err == nil {
			t.Fatalf("RunningCount over an endless index = %d, nil; want an error", got)
		}
		if got != 0 {
			t.Errorf("RunningCount returned %d beside its error", got)
		}
		if len(index.queries) != maxStatsScanPages {
			t.Errorf("walked %d pages, want the scan bound %d", len(index.queries), maxStatsScanPages)
		}
	})

	t.Run("a count the cluster refuses is an error", func(t *testing.T) {
		index := statsIndexOver(t, runningCorpus())
		index.countErr = fmt.Errorf("visibility unavailable")
		reader := newRunReader(index, converter.GetDefaultDataConverter()).WithNamespaceIndex(true)
		if got, err := reader.RunningCount(ctx, "payments"); err == nil {
			t.Fatalf("RunningCount = %d, nil; want the cluster's error", got)
		}
	})
}
