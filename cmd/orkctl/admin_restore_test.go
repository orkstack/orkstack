package main

import (
	"context"
	"encoding/json/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
)

// pagedReconciler is the API's RestoreReconciler seam with two pages behind
// it: the reconcile itself is archive.ReconcileRestore's, tested there; this
// proves what the CLI asks for and how it folds the answers.
type pagedReconciler struct {
	mu    sync.Mutex
	calls []archive.RestoreRequest
}

func (r *pagedReconciler) ReconcileRestore(_ context.Context, req archive.RestoreRequest) (archive.RestoreResult, error) {
	r.mu.Lock()
	r.calls = append(r.calls, req)
	r.mu.Unlock()
	retired := time.Date(2026, 11, 17, 4, 0, 0, 0, time.UTC)
	if req.After == "" {
		return archive.RestoreResult{
			Retired:  []archive.RestoreRetired{{RunID: "ork-a", RetiredAt: retired, Archived: true, Deleted: !req.DryRun}},
			Finished: []string{"ork-tail"}, Listed: 2, Next: "metadata/cleanup/runs/ork-a.json",
		}, nil
	}
	return archive.RestoreResult{
		Retired: []archive.RestoreRetired{{RunID: "ork-b", RetiredAt: retired, Archived: true, Open: true}},
		Listed:  1,
	}, nil
}

func (r *pagedReconciler) requests() []archive.RestoreRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]archive.RestoreRequest(nil), r.calls...)
}

func restoreFixture(t *testing.T) (*fixture, *pagedReconciler) {
	t.Helper()
	rec := &pagedReconciler{}
	return newFixtureWith(t, func(cfg *api.Config) { cfg.RestoreReconciler = rec }), rec
}

const restoredAtFlag = "2026-11-17T03:00:00Z"

// A dry run walks every page, folds them into one answer and deletes
// nothing; its rows say what a run would do.
func TestAdminRestoreReconcile_DryRunWalksEveryPage(t *testing.T) {
	f, rec := restoreFixture(t)
	got := f.run("admin", "restore", "reconcile", "--temporal-restored-at", restoredAtFlag, "--dry-run").requireExit(t, exitOK)
	calls := rec.requests()
	if len(calls) != 2 || calls[1].After != "metadata/cleanup/runs/ork-a.json" || !calls[0].DryRun || !calls[1].DryRun {
		t.Fatalf("requests = %+v; want two dry-run pages, the second after the first's cursor", calls)
	}
	if !calls[0].RestoredAt.Equal(time.Date(2026, 11, 17, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("restored at %v; want the flag's instant", calls[0].RestoredAt)
	}
	for _, want := range []string{"cleanup records read: 3", "ork-a", "would delete", "ork-b", "kept: running", "ork-tail"} {
		if !strings.Contains(got.stdout, want) {
			t.Fatalf("output lacks %q:\n%s", want, got.stdout)
		}
	}
}

// --json prints the folded answer.
func TestAdminRestoreReconcile_JSONIsTheFoldedAnswer(t *testing.T) {
	f, _ := restoreFixture(t)
	got := f.run("admin", "restore", "reconcile", "--temporal-restored-at", restoredAtFlag, "--dry-run", "--json").requireExit(t, exitOK)
	var out struct {
		Retired []struct {
			RunID string `json:"runId"`
		} `json:"retired"`
		FinishedRunIDs []string `json:"finishedRunIds"`
		Listed         int      `json:"listed"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &out); err != nil {
		t.Fatalf("%v:\n%s", err, got.stdout)
	}
	if len(out.Retired) != 2 || out.Listed != 3 || len(out.FinishedRunIDs) != 1 {
		t.Fatalf("folded = %+v", out)
	}
}

// Without --dry-run it asks first, and refuses with no terminal and no --yes
// before any request leaves; with --yes it deletes.
func TestAdminRestoreReconcile_AsksBeforeItDeletes(t *testing.T) {
	f, rec := restoreFixture(t)
	refused := runConfirming(t, f.baseURL, strings.NewReader(""), false,
		"admin", "restore", "reconcile", "--temporal-restored-at", restoredAtFlag)
	if refused.code == exitOK || !strings.Contains(refused.stderr, "--yes") {
		t.Fatalf("exit %d, stderr %q; want a refusal naming --yes", refused.code, refused.stderr)
	}
	if n := len(rec.requests()); n != 0 {
		t.Fatalf("%d requests left before confirmation", n)
	}
	got := runConfirming(t, f.baseURL, strings.NewReader(""), false,
		"admin", "restore", "reconcile", "--temporal-restored-at", restoredAtFlag, "--yes")
	if got.code != exitOK || !strings.Contains(got.stdout, "deleted") {
		t.Fatalf("exit %d, stdout %q; want ork-a deleted", got.code, got.stdout)
	}
	if calls := rec.requests(); len(calls) != 2 || calls[0].DryRun {
		t.Fatalf("requests = %+v; want two deleting pages", calls)
	}
}

// The restore instant is required and must be RFC 3339.
func TestAdminRestoreReconcile_NeedsTheRestoreInstant(t *testing.T) {
	f, rec := restoreFixture(t)
	for _, args := range [][]string{
		{"admin", "restore", "reconcile", "--dry-run"},
		{"admin", "restore", "reconcile", "--temporal-restored-at", "yesterday", "--dry-run"},
	} {
		if got := f.run(args...); got.code == exitOK {
			t.Errorf("%v was accepted", args)
		}
	}
	if n := len(rec.requests()); n != 0 {
		t.Fatalf("%d requests left", n)
	}
}
