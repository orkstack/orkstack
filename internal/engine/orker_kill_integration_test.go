//go:build integration

package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The three §11 kill tests (AGENTS.md §11, docs/PROOFS.md §12):
//
//	1b  an Orker killed after its container exits, before its first upload;
//	1c  an Orker killed while an artifact upload is in flight;
//	2b  the Orker's own object store failing for a window during upload and
//	    commit — never a silent success, and the retry per the task's policy.
//
// They use the chaos harness's in-process real Orkers, which take their stores
// as a value and a wrap around ExecuteTask
// (continuation_chaos_harness_integration_test.go): Orker A's stores are
// hooked, Orker B's are the stack's own, and "killed" means what C2 means —
// A's Temporal proxy is cut, its worker stops, and the hooked call never
// returns. No fake stands in for the Orker: every container is real Docker,
// every commit is a real conditional create in the stack's bucket.

// hookedStore forwards every call to the stack's store and lets the test hook
// Put — the artifact upload path.
type hookedStore struct {
	artifacts.Store
	put func(ctx context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error)
}

func (s *hookedStore) Put(ctx context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	if s.put != nil {
		return s.put(ctx, name, r, opts)
	}
	return s.Store.Put(ctx, name, r, opts)
}

// hookedCommitStore is the twin for the completion boundary: the conditional
// create of commit.json.
type hookedCommitStore struct {
	artifacts.CommitStore
	create func(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error)
}

func (s *hookedCommitStore) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
	if s.create != nil {
		return s.create(ctx, uri, c)
	}
	return s.CommitStore.Create(ctx, uri, c)
}

// hookedStores is stores with a hook on the artifact Put, sharing the real
// commit store, log store, attempt store and reference predictor.
func hookedStores(stores *realOrkerStores,
	put func(ctx context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error),
) *realOrkerStores {
	hooked := *stores
	hooked.store = &hookedStore{Store: stores.store, put: put}
	return &hooked
}

// hookedCommitStores is stores with a hook on the commit Create.
func hookedCommitStores(stores *realOrkerStores,
	create func(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error),
) *realOrkerStores {
	hooked := *stores
	hooked.commits = &hookedCommitStore{CommitStore: stores.commits, create: create}
	return &hooked
}

// realTaskDocument compiles a one-task Orkflow whose "work" task runs image on
// the real pool.
func realTaskDocument(t *testing.T, name, image, command, retry string) *orkstackv1.Orkflow {
	t.Helper()
	doc := fmt.Sprintf("apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntasks:\n"+
		"  - id: work\n    uses: shell\n    needs: []\n    pool: real\n    retry: {%s}\n    with: {op: exec}\n",
		name, retry)
	wf := compile(t, doc)
	for _, tk := range wf.GetTasks() {
		if tk.GetId() == "work" {
			tk.Uses = image
			tk.Image = &orkstackv1.ImageRef{Reference: image, Pinned: image}
			tk.Command = []string{"/bin/sh", "-c", command}
			tk.TimeoutSeconds = 120
			tk.Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE
		}
	}
	return wf
}

// realTaskPlan is the plan a real-Orker run needs: the "real" pool routed to
// realQueue, a commit root in the stack's bucket, the deployment's limits and
// a transfer budget.
func realTaskPlan(t *testing.T, s *chaosStack, realQueue string) engine.RunPlan {
	t.Helper()
	plan, _ := s.tcPlan()
	plan.Routes["real"] = realQueue
	root, err := artifacts.CommitRoot(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	plan.CommitRoot = root
	plan.Limits = engine.DefaultLimits()
	plan.TransferBudgetSeconds = 300
	return plan
}

// commitObjectCount counts the commit.json objects under this run's commit
// prefix — the proof that a task committed exactly once.
func commitObjectCount(t *testing.T, cfg artifacts.Config, runID string) int {
	t.Helper()
	prefix, err := artifacts.RunCommitPrefix("default", runID)
	if err != nil {
		t.Fatal(err)
	}
	raw := newRawS3ClientForCommitE2E(t, cfg)
	n := 0
	pager := s3.NewListObjectsV2Paginator(raw, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.S3Bucket), Prefix: aws.String(prefix),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			t.Fatalf("list %s: %v", prefix, err)
		}
		for _, object := range page.Contents {
			if strings.HasSuffix(aws.ToString(object.Key), "/commit.json") {
				n++
			}
		}
	}
	return n
}

// requireCommitObjectsIntact reads every object the commit references and
// checks its bytes against the digest and size the commit records.
func requireCommitObjectsIntact(t *testing.T, ctx context.Context, store artifacts.Store, commit *orkstackv1.ActivityCommit) {
	t.Helper()
	check := func(kind string, ref *orkstackv1.ArtifactRef) {
		if ref == nil || ref.GetUri() == "" {
			t.Fatalf("the commit references no %s", kind)
		}
		reader, err := store.Get(ctx, ref.GetUri())
		if err != nil {
			t.Fatalf("the commit's %s at %s does not exist: %v", kind, ref.GetUri(), err)
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatalf("read the commit's %s: %v", kind, err)
		}
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != ref.GetSha256() {
			t.Fatalf("the commit's %s hashes to %s, and the commit records %s", kind, got, ref.GetSha256())
		}
		if int64(len(data)) != ref.GetSizeBytes() {
			t.Fatalf("the commit's %s is %d bytes, and the commit records %d", kind, len(data), ref.GetSizeBytes())
		}
	}
	check("values.json", commit.GetValues())
	for _, ref := range commit.GetArtifacts() {
		check(ref.GetName(), ref)
	}
}

// orkerKillFixture is the fixture the two kill tests share: a chaos stack with
// a replica, a projection, a real Orker A on hooked stores and one run
// started. "entered" receives the artifact name whose upload the hook holds.
type orkerKillFixture struct {
	t      *testing.T
	ctx    context.Context
	s      *chaosStack
	stores *realOrkerStores
	runID  string
	stream *streamCollector
	// entered receives the name of the upload the hook is holding.
	entered chan string
	idA     string
	idB     string
	orkerA  *chaosOrker
	proxyA  *faultproxy.Proxy
	realQ   string
}

// newOrkerKillFixture starts the stack, replica, projection, Orker A on the
// hooked stores, and the one-task run. putHook is the hook on A's artifact
// store; it signals entered when it holds an upload.
func newOrkerKillFixture(t *testing.T, ctx context.Context, scenario, name, image, command, retry string, entered chan string,
	putHook func(stores *realOrkerStores, ctx context.Context, artifact string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error),
) *orkerKillFixture {
	t.Helper()
	s := newChaosStack(t, ctx, scenario)
	stores := s.realStores()
	realQueue := mustQueueForPool(t, "real")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)

	proxyA := s.temporalProxy()
	clientA := s.dial("orker-a", proxyA.Addr())
	orkerA, idA := s.startRealOrker("orker-a", "real", realQueue,
		hookedStores(stores, func(ctx context.Context, artifact string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
			return putHook(stores, ctx, artifact, r, opts)
		}), clientA, nil)

	f := &orkerKillFixture{
		t: t, ctx: ctx, s: s, stores: stores, entered: entered,
		idA: idA, orkerA: orkerA, proxyA: proxyA, realQ: realQueue,
	}
	plan := realTaskPlan(t, s, realQueue)
	runID := s.newRunID()
	s.transcript.run(runID)
	f.runID = runID
	s.start(runID, realTaskDocument(t, name, image, command, retry), nil, plan)
	f.stream = s.openStream(s.reader, runID, 0)
	return f
}

// killA cuts Orker A's Temporal connection and stops its worker, as C2 kills
// a host, and starts Orker B on the stack's own stores.
func (f *orkerKillFixture) killA() {
	f.t.Helper()
	f.proxyA.Cut()
	f.orkerA.stop()
	_, idB := f.s.startRealOrker("orker-b", "real", f.realQ, f.stores, nil, nil)
	f.idB = idB
	f.s.transcript.step("Orker A killed: its frontend connection cut and its worker stopped, nothing committed")
}

// requireRetriedAndCommitted asserts the run succeeded on Orker B's second
// attempt without adoption, that exactly one commit.json exists, and that
// every object it references is intact.
func (f *orkerKillFixture) requireRetriedAndCommitted(taskID string) *orkstackv1.ActivityCommit {
	t := f.t
	t.Helper()
	status, err := f.s.wait(f.runID, 5*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	f.s.requireOneRunRead(f.runID, f.stream, orkstackv1.Status_STATUS_SUCCEEDED, "")

	records, err := f.stores.attempts.ListAttempts(f.ctx, f.runID, taskID, artifacts.NoItem)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("%d attempt records, want the one the retry wrote (attempt 1 died before its record)", len(records))
	}
	record := records[0]
	if record.GetAttempt() != 2 || record.GetOrkerId() != f.idB ||
		record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED || record.GetCommitAdopted() {
		t.Fatalf("the retry's record is attempt %d on %s, outcome %v, adopted %t; want attempt 2 on %s, SUCCEEDED, not adopted",
			record.GetAttempt(), record.GetOrkerId(), record.GetOutcome(), record.GetCommitAdopted(), f.idB)
	}
	if n := commitObjectCount(t, f.s.cfg, f.runID); n != 1 {
		t.Fatalf("%d commit.json objects under the run's prefix, want exactly one", n)
	}
	run, err := f.s.reader.GetRun(f.ctx, f.runID)
	if err != nil {
		t.Fatal(err)
	}
	uri := taskRunOfRun(run, taskID).GetCommit().GetUri()
	if uri == "" {
		t.Fatal("the run's task row names no commit")
	}
	commitStore, err := artifacts.NewCommitStore(f.ctx, f.s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := commitStore.Get(f.ctx, uri)
	if err != nil {
		t.Fatalf("read the commit at %s: %v", uri, err)
	}
	requireCommitObjectsIntact(t, f.ctx, f.stores.store, stored.Commit)
	f.s.transcript.figure("attempt_records", len(records))
	f.s.transcript.figure("commit_objects", 1)
	return stored.Commit
}

// TestIntegration_AnOrkerKilledAfterItsContainerExitsRetriesElsewhere: §11 1b.
// Orker A's container exits and its first artifact upload is held; A is killed
// there, with nothing committed. After the heartbeat timeout the retry runs on
// Orker B and runs the container again — the container ran twice, and no
// adoption happened — and the run commits exactly once.
func TestIntegration_AnOrkerKilledAfterItsContainerExitsRetriesElsewhere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	release := make(chan struct{})
	entered := make(chan string, 4)
	var once sync.Once
	f := newOrkerKillFixture(t, ctx, "orker-kill/after-exit", "orker-killed-after-exit",
		pinnedAlpineForCommitE2E(t),
		`printf 'payload' > /orkstack/output/artifacts/report.txt; printf '{"ok":true}' > /orkstack/output/values.json`,
		"maximum_attempts: 3", entered,
		func(_ *realOrkerStores, ctx context.Context, artifact string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
			if artifact != "report.txt" {
				return artifacts.ArtifactRef{}, fmt.Errorf("the first upload was %q, want the task's artifact report.txt", artifact)
			}
			once.Do(func() { entered <- artifact })
			<-release
			return artifacts.ArtifactRef{}, ctx.Err()
		})
	t.Cleanup(func() { close(release) })

	select {
	case artifact := <-f.entered:
		f.s.transcript.step("Orker A is holding the upload of %q: the container exited and nothing is committed", artifact)
	case <-time.After(3 * time.Minute):
		t.Fatal("Orker A never reached the artifact upload")
	}
	if n := runningContainerCountForCommitE2E(t, f.idA); n != 0 {
		t.Fatalf("Orker A has %d containers running while the upload is held, want 0: the container must have exited", n)
	}
	if n := commitObjectCount(t, f.s.cfg, f.runID); n != 0 {
		t.Fatalf("%d commit.json objects exist before the kill, want 0", n)
	}

	f.killA()
	f.requireRetriedAndCommitted("work")
	t.Logf("§11 1b: A died after its container exited and before its first upload; attempt 2 on %s ran the container "+
		"again, not an adoption; the run committed exactly once", f.idB)
}

// TestIntegration_AnOrkerKilledDuringArtifactUploadRetriesElsewhere: §11 1c.
// The task writes two artifacts. Orker A uploads the first whole and is killed
// while the second upload is in flight; nothing is committed. The retry on
// Orker B completes, and every object the one commit references is complete,
// with the bytes and size it records.
func TestIntegration_AnOrkerKilledDuringArtifactUploadRetriesElsewhere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	release := make(chan struct{})
	entered := make(chan string, 4)
	var once sync.Once
	var mu sync.Mutex
	calls := 0
	uploaded := []string{}
	firstDone := make(chan struct{})
	f := newOrkerKillFixture(t, ctx, "orker-kill/during-upload", "orker-killed-during-upload",
		pinnedAlpineForCommitE2E(t),
		`printf 'first' > /orkstack/output/artifacts/a.txt; printf 'second' > /orkstack/output/artifacts/b.txt; printf '{"ok":true}' > /orkstack/output/values.json`,
		"maximum_attempts: 3", entered,
		func(stores *realOrkerStores, ctx context.Context, artifact string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
			// The Orker uploads a task's artifacts concurrently, so "the
			// first upload" is the first call, whatever its name: let it
			// complete, then hold the second one in flight.
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n == 1 {
				ref, err := stores.store.Put(ctx, artifact, r, opts)
				if err == nil {
					mu.Lock()
					uploaded = append(uploaded, artifact)
					mu.Unlock()
				}
				close(firstDone)
				return ref, err
			}
			<-firstDone
			once.Do(func() { entered <- artifact })
			<-release
			return artifacts.ArtifactRef{}, ctx.Err()
		})
	t.Cleanup(func() { close(release) })

	select {
	case artifact := <-f.entered:
		f.s.transcript.step("Orker A is holding the upload of %q with one upload already complete", artifact)
	case <-time.After(3 * time.Minute):
		t.Fatal("Orker A never reached the second artifact upload")
	}
	mu.Lock()
	done := strings.Join(uploaded, ",")
	mu.Unlock()
	if len(uploaded) != 1 {
		t.Fatalf("Orker A completed %q before the kill, want exactly one artifact uploaded in full", done)
	}
	if n := commitObjectCount(t, f.s.cfg, f.runID); n != 0 {
		t.Fatalf("%d commit.json objects exist before the kill, want 0", n)
	}

	f.killA()
	commit := f.requireRetriedAndCommitted("work")
	names := []string{}
	for _, ref := range commit.GetArtifacts() {
		names = append(names, ref.GetName())
	}
	if strings.Join(names, ",") != "a.txt,b.txt" {
		t.Fatalf("the commit's artifacts are %v, want a.txt and b.txt", names)
	}
	t.Logf("§11 1c: A died mid-upload (a.txt complete, b.txt in flight); attempt 2 on %s ran the container again; "+
		"the one commit's every object is complete and hashes to what it records", f.idB)
}

// TestIntegration_AnOrkerCommitDuringAStorageOutageNeverSucceedsSilently:
// §11 2b-orker. Orker A's own store refuses writes for a window — first the
// artifact Put, then the commit Create — and the task's policy retries each
// refused attempt. Nothing is ever reported as a success while the store
// fails, the window is observed to be crossed by real attempts (each of at
// least two attempts was refused), and after it the task commits exactly once.
func TestIntegration_AnOrkerCommitDuringAStorageOutageNeverSucceedsSilently(t *testing.T) {
	const refusal = "dial tcp 127.0.0.1:9000: connect: connection refused"
	cases := []struct {
		name  string
		hook  func(stores *realOrkerStores, refused func() bool) *realOrkerStores
		code  string
		phase string
	}{
		{
			name: "upload",
			hook: func(stores *realOrkerStores, refuse func() bool) *realOrkerStores {
				return hookedStores(stores, func(ctx context.Context, artifact string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
					if refuse() {
						return artifacts.ArtifactRef{}, fmt.Errorf("%s", refusal)
					}
					return stores.store.Put(ctx, artifact, r, opts)
				})
			},
			code:  "ARTIFACT_UPLOAD_FAILED",
			phase: "the artifact upload",
		},
		{
			name: "commit",
			hook: func(stores *realOrkerStores, refuse func() bool) *realOrkerStores {
				return hookedCommitStores(stores, func(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
					if refuse() {
						return artifacts.CommitRecord{}, fmt.Errorf("%s", refusal)
					}
					return stores.commits.Create(ctx, uri, c)
				})
			},
			code:  "COMMIT_WRITE_FAILED",
			phase: "the commit",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			s := newChaosStack(t, ctx, "orker-outage/"+tc.name)
			stores := s.realStores()
			realQueue := mustQueueForPool(t, "real")
			s.startReplica("replica-a", replicaOptions{})
			s.startProjection(nil)

			var mu sync.Mutex
			refused := 0
			refuse := func() bool {
				mu.Lock()
				defer mu.Unlock()
				if refused < 2 {
					refused++
					return true
				}
				return false
			}
			s.startRealOrker("orker-a", "real", realQueue, tc.hook(stores, refuse), nil, nil)

			wf := realTaskDocument(t, "orker-store-outage", pinnedAlpineForCommitE2E(t),
				`printf 'payload' > /orkstack/output/artifacts/report.txt; printf '{"ok":true}' > /orkstack/output/values.json`,
				"maximum_attempts: 3")
			plan := realTaskPlan(t, s, realQueue)
			runID := s.newRunID()
			s.transcript.run(runID)
			s.start(runID, wf, nil, plan)
			stream := s.openStream(s.reader, runID, 0)

			status, err := s.wait(runID, 5*time.Minute)
			if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
				t.Fatalf("the run closed %v: %v", status, err)
			}
			s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")

			mu.Lock()
			refusals := refused
			mu.Unlock()
			if refusals < 2 {
				t.Fatalf("the store refused %d writes, want at least two: the window must be crossed by real attempts", refusals)
			}
			records, err := stores.attempts.ListAttempts(ctx, runID, "work", artifacts.NoItem)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 3 {
				t.Fatalf("%d attempt records, want 3 (two refused and retried, one committed)", len(records))
			}
			refusedAttempts := 0
			for _, record := range records {
				switch record.GetAttempt() {
				case 1, 2:
					if record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED ||
						record.GetError().GetCode() != tc.code ||
						record.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
						!record.GetError().GetRetryable() {
						t.Fatalf("attempt %d = %v %v (retryable %t), want %s INFRASTRUCTURE_ERROR retryable",
							record.GetAttempt(), record.GetOutcome(), record.GetError(), record.GetError().GetRetryable(), tc.code)
					}
					refusedAttempts++
				case 3:
					if record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
						t.Fatalf("attempt 3 = %v, want SUCCEEDED once the window closed", record.GetOutcome())
					}
				}
			}
			if refusedAttempts != 2 {
				t.Fatalf("%d refused attempts, want 2", refusedAttempts)
			}
			if n := commitObjectCount(t, s.cfg, runID); n != 1 {
				t.Fatalf("%d commit.json objects, want exactly one", n)
			}
			s.transcript.figure("refused_writes", refusals)
			s.transcript.figure("attempts", len(records))
			t.Logf("§11 2b (%s): the store refused %d writes; attempts 1 and 2 failed at %s with %s and were retried "+
				"per the policy; attempt 3 committed once; no success was ever reported while the store failed",
				tc.name, refusals, tc.phase, tc.code)
		})
	}
}
