//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// gcFixture is a private bucket and Temporal namespace, the real run archive
// and artifact store over them, and a namespace with a one-minute retention.
type gcFixture struct {
	ctx       context.Context
	cfg       artifacts.Config
	archive   *artifacts.S3RunArchive
	runs      *engine.RunArchiver
	objects   artifacts.Store
	temporal  client.Client
	namespace string
	reader    *engine.TemporalRunReader
	retention *archivecleanup.RetentionController
	sweeper   *archivecleanup.Sweeper
	queue     string
	raw       *s3.Client
}

const gcNamespace = "gc-e2e"

func newGCFixture(t *testing.T) *gcFixture {
	t.Helper()
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	cfg, raw := privateCleanupBucket(t, ctx)
	namespace := fmt.Sprintf("%s-gc-%d-%d", baseNamespace(), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	temporalClient := cleanupTemporalClient(t, ctx, address, namespace)
	reader := engine.NewTemporalRunReader(temporalClient)
	archive, err := artifacts.NewS3RunArchive(ctx, cfg)
	must.OKf(t, err, "NewS3RunArchive")
	objects, err := artifacts.New(cfg)
	must.OKf(t, err, "artifact store")
	policies, err := artifacts.NewS3RetentionPolicyStore(ctx, cfg)
	must.OKf(t, err, "NewS3RetentionPolicyStore")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	retention := archivecleanup.NewRetentionController(policies, reader, namespace, archivecleanup.DefaultRetentionBuffer, logger)
	_, err = retention.SetMax(ctx, 999*time.Hour)
	must.OKf(t, err, "SetMax")
	runs := engine.NewRunArchiver(archive)
	f := &gcFixture{ctx: ctx, cfg: cfg, archive: archive, runs: runs, objects: objects, temporal: temporalClient,
		namespace: namespace, reader: reader, retention: retention, raw: raw,
		queue: fmt.Sprintf("ork-gc-e2e-%d", time.Now().UnixNano())}
	f.sweeper = archivecleanup.NewSweeper(archivecleanup.CleanupConfig{TemporalNamespace: namespace, Interval: time.Hour, BatchSize: 100},
		reader, archive, engine.NewHistoryArchiver(reader, runs), reader, nil, logger)
	// A stand-in registered under RunWorkflow's type, on a queue of its own:
	// a run the cluster holds open, which the low-water walk and the lookup
	// see exactly as they see a real one.
	w := worker.New(temporalClient, f.queue, worker.Options{DisableRegistrationAliasing: true})
	w.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		return workflow.Await(ctx, func() bool { return false })
	}, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	must.OKf(t, w.Start(), "start stand-in worker")
	t.Cleanup(w.Stop)
	return f
}

// open starts a run the cluster holds open.
func (f *gcFixture) open(t *testing.T, runID string) {
	t.Helper()
	_, err := f.temporal.ExecuteWorkflow(f.ctx, client.StartWorkflowOptions{ID: runID, TaskQueue: f.queue}, engine.RunWorkflowType)
	must.OKf(t, err, "start %s", runID)
	t.Cleanup(func() { _ = f.temporal.TerminateWorkflow(context.Background(), runID, "", "test done") })
}

// produce puts objects as runID's task outputs (S3 layout, producer in the
// key) and returns their refs.
func (f *gcFixture) produce(t *testing.T, runID string, names ...string) []*orkstackv1.ArtifactRef {
	t.Helper()
	var refs []*orkstackv1.ArtifactRef
	for _, name := range names {
		ref, err := f.objects.Put(f.ctx, name, strings.NewReader("bytes of "+name), artifacts.PutOptions{
			Metadata: artifacts.Provenance{RunID: runID, TaskID: "build", Orkflow: "gc-flow"}.Metadata(nil),
		})
		must.OKf(t, err, "put %s", name)
		refs = append(refs, &orkstackv1.ArtifactRef{Name: ref.Name, Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes})
	}
	return refs
}

// archived writes runID's archive body (and so its markers and list),
// finished at finished, recording refs as its task's outputs.
func (f *gcFixture) archived(t *testing.T, runID string, finished time.Time, refs ...*orkstackv1.ArtifactRef) {
	t.Helper()
	run := &orkstackv1.Run{Id: runID, Namespace: gcNamespace, OrkflowName: "gc-flow", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(finished.Add(-time.Minute)), FinishedAt: timestamppb.New(finished),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED, Artifacts: refs}}}
	must.OKf(t, f.runs.Archive(f.ctx, run, nil), "archive %s", runID)
}

// retire writes runID's sidecar as retention cleanup with the GC on does:
// its markers and list stay.
func (f *gcFixture) retire(t *testing.T, runID string) {
	t.Helper()
	_, err := f.archive.RetireRunArtifacts(f.ctx, gcNamespace, runID, true)
	must.OKf(t, err, "retire %s", runID)
	must.OKf(t, f.archive.MarkArtifactsCleaned(f.ctx, runID, time.Now()), "sidecar %s", runID)
}

// gcTestGrace is the fixture's grace. The GC reads the store's clock a
// second low, and SeaweedFS stamps whole seconds, so its own skew reads one
// to two seconds plus the probe's round trip high: the grace keeps its
// skew hold (a quarter of it) above that.
const gcTestGrace = 12 * time.Second

func (f *gcFixture) reclaimer(mode archivecleanup.ReclaimMode, store archivecleanup.ReclaimStore) *archivecleanup.Reclaimer {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	namespaces := staticNamespaceLister{docs: []*orkstackv1.Namespace{{Name: gcNamespace, RunRetention: durationpb.New(time.Minute)}}}
	return archivecleanup.NewReclaimer(archivecleanup.ReclaimConfig{Mode: mode, Batch: 100, Interval: time.Hour},
		store, f.runs, f.reader, f.sweeper, namespaces, f.retention, f.namespace, logger).WithGrace(gcTestGrace).WithResumes(archivecleanup.NoResumes{})
}

func (f *gcFixture) present(t *testing.T, ref *orkstackv1.ArtifactRef) bool {
	t.Helper()
	_, err := f.objects.Stat(f.ctx, ref.GetUri())
	if err == nil {
		return true
	}
	if !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("stat %s: %v", ref.GetUri(), err)
	}
	return false
}

// awaitStoreAge waits until the store's clock is past every retirement by
// more than the grace: a deadline, with the store's own clock as the check.
func (f *gcFixture) awaitStoreAge(t *testing.T, d time.Duration) {
	t.Helper()
	start, err := f.archive.StoreNow(f.ctx)
	must.OKf(t, err, "StoreNow")
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		now, err := f.archive.StoreNow(f.ctx)
		must.OKf(t, err, "StoreNow")
		if now.Sub(start) > d {
			return
		}
		select {
		case <-f.ctx.Done():
			t.Fatalf("the store's clock did not advance %s", d)
		case <-ticker.C:
		}
	}
}

// TestIntegration_ArtifactGC_ReclaimsOnlyTheUnreferenced runs the GC against
// the real object store and cluster. Retired run P produced four objects:
//
//   - free: referenced by nothing — reclaimed (the positive control);
//   - held: also referenced by an archived run that is still open — kept;
//   - sharedRetained: also referenced by a run inside its retention, whose
//     own retirement sidecar is premature — kept;
//
// and run M, retired, has a commit that appeared after its retirement (a
// memo can still hand its objects to a hit) — every object it produced is
// kept, for good. A dry run deletes nothing, and a pass whose store does
// not answer deletes nothing.
func TestIntegration_ArtifactGC_ReclaimsOnlyTheUnreferenced(t *testing.T) {
	f := newGCFixture(t)
	old := time.Now().Add(-48 * time.Hour)

	// E: an unrelated run, open from before P retires and never archived
	// while open. It may have taken P's objects by a memo hit, so it holds
	// the low-water mark: nothing P produced goes while it is open.
	e := fmt.Sprintf("ork-gc-e-%d", time.Now().UnixNano())
	f.open(t, e)

	p := fmt.Sprintf("ork-gc-p-%d", time.Now().UnixNano())
	pRefs := f.produce(t, p, "free.txt", "held.txt", "shared.txt")
	free, held, shared := pRefs[0], pRefs[1], pRefs[2]
	f.archived(t, p, old, pRefs...)

	// M: retired, then a suspended Orker's commit lands under its prefix.
	m := fmt.Sprintf("ork-gc-m-%d", time.Now().UnixNano())
	mRefs := f.produce(t, m, "memo.txt")
	f.archived(t, m, old, mRefs...)

	// R: inside its retention and still held by the cluster, with a
	// premature sidecar, referencing P's shared object. (A run inside its
	// retention that the cluster no longer holds is missing early, which
	// holds the whole pass: reclaim_test.go.)
	r := fmt.Sprintf("ork-gc-r-%d", time.Now().UnixNano())
	f.archived(t, r, time.Now(), shared)

	f.retire(t, p)
	f.retire(t, m)
	f.retire(t, r)
	commitKey, err := artifacts.CommitKey(gcNamespace, m, "build")
	must.OKf(t, err, "CommitKey")
	_, err = f.raw.PutObject(f.ctx, &s3.PutObjectInput{Bucket: aws.String(f.cfg.S3Bucket), Key: aws.String(commitKey),
		Body: strings.NewReader(`{"schemaVersion":1}`)})
	must.OKf(t, err, "late commit")
	// Past the grace by more than the GC's late reading of an age (the
	// probe's guard, a sidecar's second, a round trip).
	f.awaitStoreAge(t, gcTestGrace+4*time.Second)

	// X: archived with a marker on P's held object, and open in the
	// cluster, created after P's retirement (so the mark admits the rest).
	x := fmt.Sprintf("ork-gc-x-%d", time.Now().UnixNano())
	f.archived(t, x, time.Now(), held)
	f.open(t, x)
	// R is opened only now, after P's retirement plus the grace, so the
	// mark it holds admits P's free object: the test proves R's own claim
	// keeps the shared one.
	f.open(t, r)

	_, err = f.sweeper.RunOnce(f.ctx)
	must.OKf(t, err, "sweep")

	// A pass that cannot read the store's clock deletes nothing.
	unreachable := f.cfg
	unreachable.S3Endpoint = "http://127.0.0.1:1"
	broken, err := artifacts.NewS3RunArchive(f.ctx, unreachable)
	must.OKf(t, err, "unreachable archive")
	if report := f.reclaimer(archivecleanup.ReclaimDelete, broken).RunOnce(f.ctx); report.Err == nil {
		t.Fatalf("a pass with no store answered %s", report.Outcome)
	}

	// The dry run: every step, nothing deleted.
	dry := f.reclaimer(archivecleanup.ReclaimDryRun, f.archive)
	dry.RunOnce(f.ctx)
	dryReport := dry.RunOnce(f.ctx)
	if dryReport.Err != nil {
		t.Fatal(dryReport.Err)
	}
	for _, ref := range append(append([]*orkstackv1.ArtifactRef{}, pRefs...), mRefs...) {
		if !f.present(t, ref) {
			t.Fatalf("the dry run deleted %s", ref.GetUri())
		}
	}

	gc := f.reclaimer(archivecleanup.ReclaimDelete, f.archive)
	gc.RunOnce(f.ctx) // warming: no previous mark, no deletes
	if !f.present(t, free) {
		t.Fatal("the warming pass deleted")
	}
	// Held: E, created before P retired, is open.
	heldReport := gc.RunOnce(f.ctx)
	if heldReport.Err != nil || len(heldReport.Held) != 0 {
		t.Fatalf("pass = %s held %v err %v (mark %+v)", heldReport.Outcome, heldReport.Held, heldReport.Err, heldReport.LowWater)
	}
	if !f.present(t, free) || heldReport.LowWater.RunID != e {
		t.Fatalf("an object produced before an open run was created was deleted, or that run did not hold the mark: %+v %+v",
			heldReport.Objects, heldReport.LowWater)
	}
	if n := heldReport.Objects[archivecleanup.TallyKey{Namespace: gcNamespace, Class: archivecleanup.ClassArtifact,
		Outcome: archivecleanup.OutcomeKept, Reason: archivecleanup.ReasonLowWater}].Objects; n != 1 {
		t.Fatalf("kept on the low-water mark: %d, want the free object: %+v", n, heldReport.Objects)
	}

	// E finishes and is archived. The mark is the lower of this pass's and
	// the previous pass's, so the pass after E's archive still holds; the
	// one after that reclaims.
	must.OKf(t, f.temporal.TerminateWorkflow(f.ctx, e, "", "test: E finished"), "finish E")
	f.archived(t, e, time.Now())
	gc.RunOnce(f.ctx)
	report := gc.RunOnce(f.ctx)
	if report.Err != nil || len(report.Held) != 0 {
		t.Fatalf("pass = %s held %v err %v (mark %+v)", report.Outcome, report.Held, report.Err, report.LowWater)
	}
	if f.present(t, free) {
		t.Fatalf("the unreferenced object was not reclaimed: %+v", report.Objects)
	}
	if !f.present(t, held) {
		t.Fatal("deleted an object an open archived run references")
	}
	if !f.present(t, shared) {
		t.Fatal("deleted an object a run inside its retention references")
	}
	if !f.present(t, mRefs[0]) {
		t.Fatal("deleted an object of a run with a commit after its retirement")
	}
	rec, err := f.archive.ReclaimRecord(f.ctx, m)
	if err != nil || !rec.LateCommit {
		t.Fatalf("late-commit record = %+v, %v", rec, err)
	}
	t.Logf("EVIDENCE held_by_open_older_run=true reclaimed_unreferenced=true kept_open_reference=true kept_retained_reference=true kept_late_commit=true dry_run_deleted_nothing=true store_down_deleted_nothing=true objects=%v", report.Objects)
}
