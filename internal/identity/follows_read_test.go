package identity

import (
	"context"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// readCounts counts each kind of read the check makes, by what it names.
type readCounts struct {
	*artifacts.RecordStore
	mu                         sync.Mutex
	chains, admissions, commit map[string]int
}

func countReads(s *artifacts.RecordStore) *readCounts {
	return &readCounts{RecordStore: s, chains: map[string]int{}, admissions: map[string]int{}, commit: map[string]int{}}
}

func (c *readCounts) ResumeChain(ctx context.Context, ns, run string) (artifacts.RecordRead[[]artifacts.ResumeRecord], error) {
	c.mu.Lock()
	c.chains[run]++
	c.mu.Unlock()
	return c.RecordStore.ResumeChain(ctx, ns, run)
}

func (c *readCounts) ReadAdmission(ctx context.Context, ns, run, sha string) (artifacts.RecordRead[artifacts.Admission], error) {
	c.mu.Lock()
	c.admissions[run]++
	c.mu.Unlock()
	return c.RecordStore.ReadAdmission(ctx, ns, run, sha)
}

func (c *readCounts) ReadAttestedCommit(ctx context.Context, class artifacts.SlotClass, sp *artifacts.RecordSpacings, want artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error) {
	c.mu.Lock()
	c.commit[want.RunID+"/"+want.TaskID]++
	c.mu.Unlock()
	return c.RecordStore.ReadAttestedCommit(ctx, class, sp, want)
}

// TestFollowsRead_ReadsEachOnce: one attempt's checks of a child run under a
// resumed root, whose task follows the root's build and receives its
// artifact, read the root's resumes once, the root's admission once and the
// build's record once.
func TestFollowsRead_ReadsEachOnce(t *testing.T) {
	ctx := context.Background()
	w := newFollowsWorld(t)
	w.commit("ork-1", w.sha, "build", 0, "c", func(c *orkstackv1.ActivityCommit) {
		c.Artifacts = []*orkstackv1.ArtifactRef{{Name: "app.tar", Uri: "file:///app", Sha256: strings.Repeat("f", 64), SizeBytes: 7}}
	})
	w.resume(5, 0, []string{"deploy"}, nil)
	child, _ := admitted(t, "ork-2", artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-1", AdmissionSHA256: w.sha, TaskID: "sub"}, nil)
	child.RootRunID = "ork-1"
	child.StartFollows = []artifacts.FollowRef{{RunID: "ork-1", TaskID: "build", Kind: artifacts.FollowContainer}}
	child.StartArtifacts = map[string]artifacts.StartArtifact{"bundle": {RunID: "ork-1", TaskID: "build", Kind: artifacts.FollowContainer, Name: "app.tar"}}
	row := child.Tasks["build"]
	row.Inputs = []string{"bundle"}
	child.Tasks["build"] = row
	sha := w.admission(child)
	counts := countReads(w.store)
	check := &FollowsCheck{Store: counts, Spacing: fastSpacings}

	read, err := check.Begin(ctx, child, sha, Scopes{Root: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Scope("build"); err != nil {
		t.Fatal(err)
	}
	rows, err := read.Follows(ctx, "build")
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v, %v", rows, err)
	}
	got, err := read.Received(ctx, "build")
	if err != nil || len(got) != 1 || got[0].GetName() != "bundle" || got[0].GetUri() != "file:///app" {
		t.Fatalf("%v, %v", got, err)
	}
	if counts.chains["ork-1"] != 1 || counts.admissions["ork-1"] != 1 || counts.commit["ork-1/build"] != 1 {
		t.Fatalf("resumes read %d times, the root's admission %d, build's record %d; want each once",
			counts.chains["ork-1"], counts.admissions["ork-1"], counts.commit["ork-1/build"])
	}
}

// TestFollowsRead_AMissIsAskedAgain: what one read did not find is not kept,
// so the same read asks again and finds a record made since.
func TestFollowsRead_AMissIsAskedAgain(t *testing.T) {
	ctx := context.Background()
	w := newFollowsWorld(t)
	counts := countReads(w.store)
	check := &FollowsCheck{Store: counts, Spacing: fastSpacings}
	read, err := check.Begin(ctx, w.adm, w.sha, Scopes{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read.Follows(ctx, "approve"); reasonOf(err) != ReasonRequiredResultNotRecorded {
		t.Fatalf("before build's record: %v", err)
	}
	w.commit("ork-1", w.sha, "build", 0, "c", nil)
	if rows, err := read.Follows(ctx, "approve"); err != nil || len(rows) != 1 {
		t.Fatalf("after build's record: %v, %v", rows, err)
	}
	if n := counts.commit["ork-1/build"]; n < 2 {
		t.Fatalf("build's record read %d times, want it asked again", n)
	}
}

// TestFollowsRead_OneRecordByEitherName: a task of the run named with its run
// and without it is one record of one read, read once.
func TestFollowsRead_OneRecordByEitherName(t *testing.T) {
	ctx := context.Background()
	w := newFollowsWorld(t)
	w.adm.Follows["deploy"] = []artifacts.FollowRef{{TaskID: "build", Kind: artifacts.FollowContainer}}
	w.adm.Follows["report"] = []artifacts.FollowRef{{RunID: "ork-1", TaskID: "build", Kind: artifacts.FollowContainer}}
	w.sha = w.admission(w.adm)
	w.commit("ork-1", w.sha, "build", 0, "c", nil)
	counts := countReads(w.store)
	read, err := (&FollowsCheck{Store: counts, Spacing: fastSpacings}).Begin(ctx, w.adm, w.sha, Scopes{})
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range []string{"deploy", "report"} {
		rows, err := read.Follows(ctx, task)
		if err != nil || len(rows) != 1 || rows[0].Ref.TaskID != "build" {
			t.Fatalf("%s: %v, %v", task, rows, err)
		}
	}
	if rows, _ := read.Follows(ctx, "report"); rows[0].Ref.RunID != "ork-1" {
		t.Errorf("a hit answers %+v, want the ref as its caller named it", rows[0].Ref)
	}
	if n := counts.commit["ork-1/build"]; n != 1 {
		t.Fatalf("build's record read %d times, want once", n)
	}
}
