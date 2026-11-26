package artifacts_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// endingOf is a run's record of the ending under scope: 0 is its first.
func endingOf(t *testing.T, scope uint64, status, body string) artifacts.RunRecord {
	t.Helper()
	return artifacts.RunRecord{
		RunID: "ork-r", Orkflow: "wf", Status: status, Time: day(t, "2026-08-14T09:30:15Z"), JSONL: []byte(body), Scope: scope,
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, OriginID: "ork-parent", RootRunID: "ork-root",
	}
}

// listings are the lists a run's rows answer: the run index, the newest-first
// family, and the origin and root families.
func listings(t *testing.T, a artifacts.RunArchive) map[string][]artifacts.RunIndexEntry {
	t.Helper()
	out := map[string][]artifacts.RunIndexEntry{}
	for name, q := range map[string]artifacts.RunQuery{
		"index":  {},
		"keys":   {KeyOrder: true, Limit: 10},
		"recent": {Namespace: "default", Limit: 10, Recent: true},
		"origin": {OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, OriginID: "ork-parent"},
		"root":   {Namespace: "default", RootRunID: "ork-root"},
	} {
		got, err := a.ListRuns(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out[name] = got
	}
	return out
}

func onlyRow(t *testing.T, a artifacts.RunArchive, status string, scope uint64) {
	t.Helper()
	for name, rows := range listings(t, a) {
		if len(rows) != 1 || rows[0].RunID != "ork-r" || rows[0].Status != status || rows[0].Scope != scope {
			t.Errorf("%s = %+v, want the run once, %s under scope %d", name, rows, status, scope)
		}
	}
}

// TestRunEndings: a run has a body for each of its endings. The newest is the
// run's, every earlier one stays as it was written, and each list holds the
// run once, as its newest ending left it.
func TestRunEndings(t *testing.T) {
	ctx := context.Background()
	a := newLocalArchive(t)
	archiveRun(t, a, endingOf(t, 0, "failed", "first\n"))
	onlyRow(t, a, "failed", 0)
	archiveRun(t, a, endingOf(t, 12, "failed", "second\n"))
	onlyRow(t, a, "failed", 12)
	archiveRun(t, a, endingOf(t, 20, "succeeded", "third\n"))
	onlyRow(t, a, "succeeded", 20)
	// A write of an earlier ending, late or again, changes nothing.
	archiveRun(t, a, endingOf(t, 12, "failed", "other\n"))
	archiveRun(t, a, endingOf(t, 0, "failed", "other\n"))
	onlyRow(t, a, "succeeded", 20)
	for scope, want := range map[uint64]string{0: "first\n", 12: "second\n", 20: "third\n"} {
		got, err := a.GetEnding(ctx, "ork-r", scope)
		if err != nil || string(got) != want {
			t.Errorf("ending %d = %q, %v; want %q", scope, got, err, want)
		}
		if has, err := a.HasEnding(ctx, "ork-r", scope); err != nil || !has {
			t.Errorf("HasEnding(%d) = %v, %v", scope, has, err)
		}
	}
	if got, err := a.GetRun(ctx, "ork-r"); err != nil || string(got) != "third\n" {
		t.Errorf("GetRun = %q, %v; want the newest ending", got, err)
	}
	if _, err := a.GetEnding(ctx, "ork-r", 13); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("an ending the run never had = %v, want ErrNotFound", err)
	}
	if has, err := a.HasEnding(ctx, "ork-r", 13); err != nil || has {
		t.Errorf("HasEnding of it = %v, %v", has, err)
	}
	state, err := a.RunEndings(ctx, "ork-r")
	if err != nil || !reflect.DeepEqual(state, artifacts.RunEndings{RunID: "ork-r", Archived: []uint64{12, 20}}) {
		t.Errorf("RunEndings = %+v, %v", state, err)
	}
}

// TestRunEndings_Resumed: a run marked as resumed has no row of the ending
// set aside, however late that ending's archive is written, and its body is
// there to read.
func TestRunEndings_Resumed(t *testing.T) {
	ctx := context.Background()
	a := newLocalArchive(t)
	archiveRun(t, a, endingOf(t, 0, "failed", "first\n"))
	for range 2 {
		if err := a.MarkResumed(ctx, "ork-r", 12, 0); err != nil {
			t.Fatal(err)
		}
	}
	// The mark decides nothing by itself: its reset may never run.
	onlyRow(t, a, "failed", 0)
	if err := a.RetireRows(ctx, endingOf(t, 0, "failed", "")); err != nil {
		t.Fatal(err)
	}
	for name, rows := range listings(t, a) {
		if len(rows) != 0 {
			t.Errorf("%s = %+v, want no row of the ending set aside", name, rows)
		}
	}
	archiveRun(t, a, endingOf(t, 0, "failed", "again\n"))
	for name, rows := range listings(t, a) {
		if len(rows) != 0 {
			t.Errorf("after a late write, %s = %+v", name, rows)
		}
	}
	if got, err := a.GetRun(ctx, "ork-r"); err != nil || string(got) != "first\n" {
		t.Errorf("GetRun = %q, %v; want the one ending archived", got, err)
	}
	state, err := a.RunEndings(ctx, "ork-r")
	if err != nil || !reflect.DeepEqual(state, artifacts.RunEndings{RunID: "ork-r", Marked: []uint64{12}}) {
		t.Errorf("RunEndings = %+v, %v", state, err)
	}
	var seen []artifacts.RunEndings
	if err := a.EachResumedRun(ctx, func(e artifacts.RunEndings) error { seen = append(seen, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []artifacts.RunEndings{state}) {
		t.Errorf("EachResumedRun = %+v", seen)
	}
}

// TestRunEndings_AWriteThatStopsShort: a resumed ending's body is its last
// write, so a write that stopped leaves the ending not archived and the retry
// finishes it.
func TestRunEndings_AWriteThatStopsShort(t *testing.T) {
	ctx := context.Background()
	a := newLocalArchive(t)
	archiveRun(t, a, endingOf(t, 0, "failed", "first\n"))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := a.PutRun(canceled, endingOf(t, 12, "succeeded", "second\n")); err == nil {
		t.Fatal("a write with no answer succeeded")
	}
	if has, err := a.HasEnding(ctx, "ork-r", 12); err != nil || has {
		t.Fatalf("HasEnding = %v, %v; want it not archived", has, err)
	}
	archiveRun(t, a, endingOf(t, 12, "succeeded", "second\n"))
	onlyRow(t, a, "succeeded", 12)
	if has, err := a.HasEnding(canceled, "ork-r", 12); err == nil || has {
		t.Fatalf("HasEnding with no answer = %v, %v; want an error, never false", has, err)
	}
}

// TestRunEndingKeys: the keys are the documented ones, and nothing else under
// their prefix is read as one.
func TestRunEndingKeys(t *testing.T) {
	body, err := artifacts.RunEndingKey("ork-r", 12)
	if err != nil || body != "metadata/run-resumes/ork-r/00000000000000000012.jsonl" {
		t.Fatalf("ending key = %q, %v", body, err)
	}
	if first, err := artifacts.RunEndingKey("ork-r", 0); err != nil || first != "metadata/runs/ork-r.jsonl" {
		t.Fatalf("the first ending's key = %q, %v", first, err)
	}
	mark, err := artifacts.ResumeMarkKey("ork-r", 12)
	if err != nil || mark != "metadata/run-resumes/ork-r/00000000000000000012.resume" {
		t.Fatalf("mark key = %q, %v", mark, err)
	}
	if _, err := artifacts.ResumeMarkKey("ork-r", 0); !errors.Is(err, artifacts.ErrInvalidRunKey) {
		t.Fatalf("a mark of no scope = %v", err)
	}
	for _, key := range []string{
		"metadata/run-resumes/ork-r/12.jsonl", "metadata/run-resumes/ork-r/00000000000000000012.json",
		"metadata/run-resumes/ork-r/99999999999999999999.jsonl", "metadata/run-resumes/ork-r/00000000000000000000.resume",
		"metadata/run-resumes/00000000000000000012.jsonl", "metadata/run-resumes/ork-r/x/00000000000000000012.jsonl",
		"metadata/runs/ork-r/00000000000000000012.jsonl",
	} {
		if run, scope, _, ok := artifacts.ParseRunEndingKey(key); ok {
			t.Errorf("%s read as run %s scope %d", key, run, scope)
		}
	}
	if run, scope, isMark, ok := artifacts.ParseRunEndingKey(mark); !ok || run != "ork-r" || scope != 12 || !isMark {
		t.Errorf("mark = %s %d %v %v", run, scope, isMark, ok)
	}
}

// TestRunEndings_AKeyOrderPageIsNeverShort: a key-ordered page is as long as
// the rows it walked, whatever a row holds, because a short page ends the
// cleanup worker's walk; a list for a reader fails on a row that names no
// ending, and nothing retires it: which ending it is of cannot be told.
func TestRunEndings_AKeyOrderPageIsNeverShort(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, err := artifacts.NewLocalRunArchive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	archiveRun(t, a, endingOf(t, 12, "succeeded", "second\n"))
	other := endingOf(t, 0, "failed", "other\n")
	other.RunID = "ork-s"
	archiveRun(t, a, other)
	row, err := artifacts.RunIndexKey(endingOf(t, 12, "succeeded", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, row), []byte("not a scope"), 0o640); err != nil {
		t.Fatal(err)
	}
	if page, err := a.ListRuns(ctx, artifacts.RunQuery{KeyOrder: true, Limit: 2}); err != nil || len(page) != 2 {
		t.Fatalf("key-ordered page = %v, %v; want both rows", page, err)
	}
	if list, err := a.ListRuns(ctx, artifacts.RunQuery{}); err == nil || !strings.Contains(err.Error(), row) {
		t.Fatalf("list = %v, %v; want an error naming the row", list, err)
	}
	if err := a.RetireRows(ctx, endingOf(t, 12, "succeeded", "")); err == nil {
		t.Fatal("a row that names no ending was retired")
	}
}

// TestRunEndings_RetireRowsLeavesLaterEndings: rows are retired up to an
// ending: those of a later one stay, so a retirement that comes late never
// takes a run that ended again from the lists.
func TestRunEndings_RetireRowsLeavesLaterEndings(t *testing.T) {
	ctx := context.Background()
	a := newLocalArchive(t)
	archiveRun(t, a, endingOf(t, 12, "failed", "second\n"))
	for _, scope := range []uint64{0, 11} {
		if err := a.RetireRows(ctx, endingOf(t, scope, "failed", "")); err != nil {
			t.Fatal(err)
		}
		onlyRow(t, a, "failed", 12)
	}
	if err := a.RetireRows(ctx, endingOf(t, 12, "failed", "")); err != nil {
		t.Fatal(err)
	}
	for name, rows := range listings(t, a) {
		if len(rows) != 0 {
			t.Errorf("%s = %+v, want none", name, rows)
		}
	}
}

// TestRunEndings_AMarkAloneHidesNothing: an ending archived after the run was
// marked is listed like any other: the mark's reset may never run, and the run
// going on is what retires its rows.
func TestRunEndings_AMarkAloneHidesNothing(t *testing.T) {
	a := newLocalArchive(t)
	if err := a.MarkResumed(context.Background(), "ork-r", 12, 0); err != nil {
		t.Fatal(err)
	}
	archiveRun(t, a, endingOf(t, 0, "failed", "first\n"))
	onlyRow(t, a, "failed", 0)
}

// TestRunEndings_AMarkNamesWhatItSetsAside: a resume's mark names the ending
// it sets aside, once, and one that names none is as good as none.
func TestRunEndings_AMarkNamesWhatItSetsAside(t *testing.T) {
	ctx := context.Background()
	a := newLocalArchive(t)
	if _, err := a.SetAside(ctx, "ork-r", 20); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("no mark = %v", err)
	}
	for range 2 {
		if err := a.MarkResumed(ctx, "ork-r", 20, 7); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.MarkResumed(ctx, "ork-r", 20, 0); !errors.Is(err, artifacts.ErrResumeMarkTaken) {
		t.Fatalf("a mark that names another ending = %v", err)
	}
	if got, err := a.SetAside(ctx, "ork-r", 20); err != nil || got != 7 {
		t.Fatalf("= %d, %v; want 7", got, err)
	}
	// An ending at or above its own scope is none it can have set aside.
	if err := a.MarkResumed(ctx, "ork-r", 30, 30); err != nil {
		t.Fatal(err)
	}
	if _, err := a.SetAside(ctx, "ork-r", 30); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a mark naming itself = %v", err)
	}
	// Nor does one that names none come to name one.
	if err := a.MarkResumed(ctx, "ork-r", 30, 7); !errors.Is(err, artifacts.ErrResumeMarkTaken) {
		t.Fatalf("a mark that names none, marked again = %v", err)
	}
}
