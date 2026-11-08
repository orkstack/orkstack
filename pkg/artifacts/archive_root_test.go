package artifacts_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func rootedRecord(id, root, parent, status string, at time.Time) artifacts.RunRecord {
	return artifacts.RunRecord{
		RunID: id, Namespace: "payments", Orkflow: "nightly", Status: status, Time: at,
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, OriginID: parent, RootRunID: root,
		JSONL: []byte(`{"run":{}}` + "\n"),
	}
}

func TestRunRootIndexKey_RoundTripsAndNeedsARoot(t *testing.T) {
	at := time.Date(2026, 11, 6, 12, 30, 45, 123456000, time.UTC)
	rec := rootedRecord("ork-leaf", "ork-root", "ork-mid", "succeeded", at)
	key, ok, err := artifacts.RunRootIndexKey(rec)
	if err != nil || !ok {
		t.Fatalf("RunRootIndexKey = %v, %v", ok, err)
	}
	want := "metadata/index/roots/payments/ork-root/ork-mid/nightly/succeeded/" + artifacts.RunStamp(at) + "/ork-leaf"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	entry, ok := artifacts.ParseRunRootIndexKey(key)
	if !ok {
		t.Fatalf("%q does not parse", key)
	}
	if entry.RootRunID != "ork-root" || entry.OriginID != "ork-mid" ||
		entry.OriginKind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN || entry.RunID != "ork-leaf" {
		t.Fatalf("entry = %+v", entry)
	}
	if !entry.Time.Equal(at) {
		t.Fatalf("time = %v, want %v", entry.Time, at)
	}

	rec.RootRunID = ""
	if key, ok, err := artifacts.RunRootIndexKey(rec); ok || err != nil || key != "" {
		t.Fatalf("a record with no root = %q, %v, %v; want no row", key, ok, err)
	}
	// A root with no parent, or on any other kind, is no run the engine records,
	// and no reason to refuse a run its archive: no row, and no error.
	rec.RootRunID, rec.OriginID = "ork-root", ""
	if _, ok, err := artifacts.RunRootIndexKey(rec); ok || err != nil {
		t.Fatalf("a root with no parent = %v, %v; want no row", ok, err)
	}
	rec.OriginID, rec.OriginKind = "trg-1", orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER
	if _, ok, err := artifacts.RunRootIndexKey(rec); ok || err != nil {
		t.Fatalf("a root on a trigger's run = %v, %v; want no row", ok, err)
	}

	for _, foreign := range []string{
		"metadata/index/runs/payments/nightly/succeeded/" + artifacts.RunStamp(at) + "/ork-leaf",
		"metadata/index/origins/payments/parent-run/ork-mid/nightly/succeeded/" + artifacts.RunStamp(at) + "/ork-leaf",
		"metadata/index/roots/payments/ork-root/ork-mid/nightly/SUCCEEDED/" + artifacts.RunStamp(at) + "/ork-leaf",
		"metadata/index/roots/payments/ork-root/ork-mid/nightly/succeeded/not-a-stamp/ork-leaf",
	} {
		if entry, ok := artifacts.ParseRunRootIndexKey(foreign); ok {
			t.Errorf("%q parsed as %+v", foreign, entry)
		}
	}
}

// TestRunArchive_RootIndexSelectsAChain runs on the local backend: one query
// finds every run below a root, newest first, however deep, and nothing else;
// the rows narrow by parent, status, orkflow and time; a run with no root has
// no row, so it is not found under any root, its own id included.
func TestRunArchive_RootIndexSelectsAChain(t *testing.T) {
	ctx := context.Background()
	archive, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	top := artifacts.RunRecord{
		RunID: "ork-root", Namespace: "payments", Orkflow: "nightly", Status: "succeeded", Time: at,
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT, JSONL: []byte(`{"run":{}}` + "\n"),
	}
	older := rootedRecord("ork-older-child", "", "ork-root", "succeeded", at.Add(time.Second))
	mid := rootedRecord("ork-mid", "ork-root", "ork-root", "succeeded", at.Add(time.Minute))
	leaf := rootedRecord("ork-leaf", "ork-root", "ork-mid", "failed", at.Add(2*time.Minute))
	stranger := rootedRecord("ork-stranger", "ork-other", "ork-other", "succeeded", at.Add(3*time.Minute))
	elsewhere := rootedRecord("ork-elsewhere", "ork-root", "ork-root", "succeeded", at.Add(4*time.Minute))
	elsewhere.Namespace = "billing"
	for _, rec := range []artifacts.RunRecord{top, older, mid, leaf, stranger, elsewhere} {
		if err := archive.PutRun(ctx, rec); err != nil {
			t.Fatalf("PutRun(%s): %v", rec.RunID, err)
		}
	}
	for name, tc := range map[string]struct {
		query artifacts.RunQuery
		want  string
	}{
		"the whole chain, newest first": {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root"}, "ork-leaf ork-mid"},
		"another namespace's chain":     {artifacts.RunQuery{Namespace: "billing", RootRunID: "ork-root"}, "ork-elsewhere"},
		"a page":                        {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root", Limit: 1}, "ork-leaf"},
		"a page asks for recent":        {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root", Limit: 1, Recent: true}, "ork-leaf"},
		"after a cursor": {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root",
			BeforeTime: leaf.Time, BeforeRunID: leaf.RunID}, "ork-mid"},
		"one status": {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root", Status: "failed"}, "ork-leaf"},
		"one parent": {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root",
			OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, OriginID: "ork-mid"}, "ork-leaf"},
		"a kind no rooted run has": {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root",
			OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}, ""},
		"one orkflow and status, bounded in time": {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root", Orkflow: "nightly",
			Status: "succeeded", Since: at, Until: at.Add(90 * time.Second)}, "ork-mid"},
		"a run with no root is under no root": {artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-older-child"}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			entries, err := archive.ListRuns(ctx, tc.query)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(entries))
			for _, entry := range entries {
				ids = append(ids, entry.RunID)
				if entry.RootRunID != tc.query.RootRunID {
					t.Errorf("entry %s root = %q", entry.RunID, entry.RootRunID)
				}
			}
			if got := strings.Join(ids, " "); got != tc.want {
				t.Fatalf("ListRuns = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRunArchive_RootRowFailureLeavesNoBody: a root row that cannot be written
// fails the write as the origin row's does, with no body and so no archive the
// sweep would take as whole, and the retry finishes it.
func TestRunArchive_RootRowFailureLeavesNoBody(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	archive, err := artifacts.NewLocalRunArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(dir, filepath.FromSlash(artifacts.RunRootIndexNamespace), "payments")
	if err := os.WriteFile(blocked, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	rec := rootedRecord("ork-leaf", "ork-root", "ork-mid", "succeeded", time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC))
	err = archive.PutRun(ctx, rec)
	if err == nil || !strings.Contains(err.Error(), "root index") {
		t.Fatalf("PutRun over a blocked root row = %v, want it to name the root index", err)
	}
	if has, err := archive.HasRun(ctx, rec.RunID); err != nil || has {
		t.Fatalf("after the failed write HasRun = %v, %v; want no body", has, err)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if err := archive.PutRun(ctx, rec); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if has, err := archive.HasRun(ctx, rec.RunID); err != nil || !has {
		t.Fatalf("after the retry HasRun = %v, %v; want the body", has, err)
	}
	entries, err := archive.ListRuns(ctx, artifacts.RunQuery{Namespace: "payments", RootRunID: "ork-root"})
	if err != nil || len(entries) != 1 || entries[0].RunID != rec.RunID {
		t.Fatalf("the chain after the retry = %+v, %v", entries, err)
	}
}
