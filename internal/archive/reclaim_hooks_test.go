package archive

import (
	"context"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// keepingArchive is a fakeArchive that can keep a retired run's references,
// recording what retirement asked for.
type keepingArchive struct {
	*fakeArchive
	kept map[string]bool
}

func (k *keepingArchive) RetireRunArtifacts(ctx context.Context, namespace, runID string, keep bool) (artifacts.CleanupResult, error) {
	k.kept[runID] = keep
	return k.DeleteRunArtifacts(ctx, namespace, runID)
}

func TestCleaner_RetirementKeepsReferencesWhileTheGCIsOn(t *testing.T) {
	for mode, wantKeep := range map[ReclaimMode]bool{ReclaimOff: false, ReclaimDryRun: true, ReclaimDelete: true, "": true} {
		t.Run(string(mode), func(t *testing.T) {
			now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
			store := &keepingArchive{
				fakeArchive: newFakeArchive(artifacts.RunIndexEntry{RunID: "ork-old", Namespace: "default", Time: now.Add(-100 * time.Hour)}),
				kept:        map[string]bool{},
			}
			cluster := newFakeCluster()
			cluster.found["ork-old"] = true
			cluster.closeTime["ork-old"] = now.Add(-99 * time.Hour)
			cluster.retention = 96 * time.Hour
			c := NewCleaner(CleanupConfig{TemporalNamespace: "t", Interval: time.Hour, RetentionBuffer: 24 * time.Hour,
				BatchSize: 100, ArtifactGC: mode}, store, cluster, &fakeNamespaces{}, newTestController(cluster, 72*time.Hour), testLogger())
			c.now = func() time.Time { return now }
			if _, err := c.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			keep, asked := store.kept["ork-old"]
			if !asked || keep != wantKeep {
				t.Fatalf("mode %q: retirement asked=%v keep=%v, want keep=%v", mode, asked, keep, wantKeep)
			}
		})
	}
}

func TestSweeper_RecordsTheLastCompletedPass(t *testing.T) {
	f := newSweepFixture()
	if _, ok := f.sweeper.LastCompletedSweep(); ok {
		t.Fatal("a pass before any ran")
	}
	f.runs.add("ork-archived", 2*time.Hour)
	f.store.bodies["ork-archived"] = true
	if _, err := f.sweeper.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	pass, ok := f.sweeper.LastCompletedSweep()
	if !ok || !pass.Clean || !pass.Started.Equal(sweepNow) {
		t.Fatalf("clean pass = %+v, %v", pass, ok)
	}
	f.store.hasErr = errInjected
	f.sweeper.RunOnce(context.Background())
	if pass, _ := f.sweeper.LastCompletedSweep(); pass.Clean {
		t.Fatal("a pass the store did not answer was recorded clean")
	}
	f.store.hasErr = nil
	f.runs.err = errInjected
	f.sweeper.RunOnce(context.Background())
	if pass, _ := f.sweeper.LastCompletedSweep(); pass.Clean {
		t.Fatal("a pass whose listing failed was recorded clean")
	}
	// A pass cut short by shutdown is not a completed pass.
	f.runs.err = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.sweeper.RunOnce(ctx)
	if pass, _ := f.sweeper.LastCompletedSweep(); pass.Clean {
		t.Fatal("a canceled pass replaced the unclean record")
	}
}

func TestCleaner_MissingHeldIsShared(t *testing.T) {
	c := NewCleaner(CleanupConfig{}, newFakeArchive(), newFakeCluster(), &fakeNamespaces{}, nil, testLogger())
	if c.MissingHeld() {
		t.Fatal("held before any pass")
	}
	c.holdMissing.Store(true)
	var hold MissingHold = c
	if !hold.MissingHeld() {
		t.Fatal("the GC does not see cleanup's hold")
	}
}

// localKeepingArchive is a local-directory archive: the GC reclaims nothing
// there, so retirement must not keep what nothing would release.
type localKeepingArchive struct{ *keepingArchive }

func (localKeepingArchive) Local() bool { return true }

func TestCleaner_LocalBackendReleasesReferences(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	store := localKeepingArchive{&keepingArchive{
		fakeArchive: newFakeArchive(artifacts.RunIndexEntry{RunID: "ork-old", Namespace: "default", Time: now.Add(-100 * time.Hour)}),
		kept:        map[string]bool{},
	}}
	cluster := newFakeCluster()
	cluster.found["ork-old"] = true
	cluster.closeTime["ork-old"] = now.Add(-99 * time.Hour)
	cluster.retention = 96 * time.Hour
	c := NewCleaner(CleanupConfig{TemporalNamespace: "t", Interval: time.Hour, RetentionBuffer: 24 * time.Hour,
		BatchSize: 100, ArtifactGC: ReclaimDelete}, store, cluster, &fakeNamespaces{}, newTestController(cluster, 72*time.Hour), testLogger())
	c.now = func() time.Time { return now }
	if _, err := c.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if keep, asked := store.kept["ork-old"]; !asked || keep {
		t.Fatalf("local retirement kept references: asked=%v keep=%v", asked, keep)
	}
}
