//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The task-caching integration suite (docs/DSL.md "Caching"):
// real Temporal, real MinIO and the real Orker running real containers
// through Docker, on the shape of examples/cached-pipeline.ork.yaml —
// featurize (cached, ttl 86400) and stamp → train (cached) — over the pinned
// alpine image the commit suite uses, so the containers write their own
// values.json and artifacts. Every scenario runs in a private Temporal
// namespace on private channels, under a stamped Orkflow name (AGENTS.md
// §11.1); each run's `run get --json` shape is written under ORK_PROOF_DIR.

// cacheE2E is the fan-out deployment plus the memo store its Orkers share.
type cacheE2E struct {
	*eachE2E
	memos artifacts.MemoStore
}

func startCacheE2E(t *testing.T) *cacheE2E {
	t.Helper()
	e := startEachE2E(t, true)
	memos, err := artifacts.NewMemoStore(e.cfg)
	if err != nil {
		t.Fatalf("open memo store: %v", err)
	}
	return &cacheE2E{eachE2E: e, memos: memos}
}

// startCacheOrker starts one real Orker with a memo store (memos, or the
// shared one when nil); wrap, when set, sees every ExecuteTask first.
func (c *cacheE2E) startCacheOrker(id string, memos artifacts.MemoStore,
	wrap func(ctx context.Context, in *orkstackv1.ExecuteTaskInput, next func() (*orkstackv1.ExecuteTaskResult, error)) (*orkstackv1.ExecuteTaskResult, error),
) worker.Worker {
	c.t.Helper()
	if memos == nil {
		memos = c.memos
	}
	refs, _ := c.store.(artifacts.RefPredictor)
	activities := orker.NewActivitiesWithOptions(c.store, orker.Options{
		Logs: c.logs, Commits: c.commits, Attempts: c.attempts, Refs: refs, Memos: memos,
		Pool: task.DefaultPool, OrkerID: id, WorkRoot: c.workRoot,
	})
	if err := activities.Boot(c.ctx); err != nil {
		infraAbsent(c.t, "orker boot checks failed: %v", err)
	}
	w := worker.New(c.temporal, c.orkerQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		next := func() (*orkstackv1.ExecuteTaskResult, error) { return activities.ExecuteTask(actx, in) }
		if wrap != nil {
			return wrap(actx, in, next)
		}
		return next()
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(activities.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	if err := w.Start(); err != nil {
		c.t.Fatalf("start orker %s: %v", id, err)
	}
	c.t.Cleanup(w.Stop)
	return w
}

// pipelineOpts varies the cached pipeline between runs.
type pipelineOpts struct {
	// featurizeSalt and trainSalt are extra `with:` values (no digits: the
	// featurize script reads every digit of its config as the row count).
	featurizeSalt string
	trainSalt     string
	scope         orkstackv1.CacheScope
	featurizeID   string
	// retry is every task's retry policy; nil is the DSL default of one
	// attempt. Retry is not a key component (docs/DSL.md "What the key
	// covers"), so it never changes
	// what a hit reuses.
	retry *orkstackv1.RetryPolicy
}

// cachedPipeline is examples/cached-pipeline.ork.yaml over alpine.
func (c *cacheE2E) cachedPipeline(name string, o pipelineOpts) *orkstackv1.Orkflow {
	image := &orkstackv1.ImageRef{Reference: c.alpine, Pinned: c.alpine, IndexDigest: digestOf(c.alpine)}
	if o.scope == orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED {
		o.scope = orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW
	}
	if o.featurizeID == "" {
		o.featurizeID = "featurize"
	}
	featurize := `n=$(tr -cd '0-9' < /orkstack/input/config.json) && seq 1 "$n" > /orkstack/output/artifacts/features.txt && ` +
		`echo "featurized $n rows" && printf '{}' > /orkstack/output/values.json`
	stamp := `printf '{"value":"%s"}' "$(od -An -N8 -tx8 /dev/urandom | tr -d ' ')" > /orkstack/output/values.json`
	train := `wc -l < /orkstack/input/artifacts/` + o.featurizeID + `/features.txt && cat /orkstack/input/config.json && ` +
		`printf '{}' > /orkstack/output/values.json`
	featurizeWith := map[string]any{"rows": "{{ .inputs.rows }}"}
	if o.featurizeSalt != "" {
		featurizeWith["salt"] = o.featurizeSalt
	}
	trainWith := map[string]any{"stamp": "{{ .tasks.stamp.output.value }}"}
	if o.trainSalt != "" {
		trainWith["salt"] = o.trainSalt
	}
	fw, _ := structpbStruct(featurizeWith)
	tw, _ := structpbStruct(trainWith)
	inputs, _ := structpbStruct(map[string]any{"rows": map[string]any{"type": "number", "default": 1000}})
	wf := &orkstackv1.Orkflow{
		Name: name, Version: "0.1.0", Inputs: inputs,
		Tasks: []*orkstackv1.Task{
			{Id: o.featurizeID, Uses: c.alpine, Image: image, Command: []string{"/bin/sh", "-c", featurize}, With: fw,
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE, Retry: o.retry,
				Cache: &orkstackv1.CacheSpec{TtlSeconds: 86400, Scope: o.scope}},
			{Id: "stamp", Uses: c.alpine, Image: image, Command: []string{"/bin/sh", "-c", stamp},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE, Retry: o.retry},
			{Id: "train", Uses: c.alpine, Image: image, Needs: []string{o.featurizeID, "stamp"},
				Command: []string{"/bin/sh", "-c", train}, With: tw,
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE, Retry: o.retry,
				Cache: &orkstackv1.CacheSpec{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}},
		},
	}
	stampDigest(c.t, wf)
	return wf
}

// digestOf is the index digest a pinned reference names.
func digestOf(pinned string) string {
	if i := strings.Index(pinned, "@"); i >= 0 {
		return pinned[i+1:]
	}
	return ""
}

func (c *cacheE2E) cachePlan(refresh bool) engine.RunPlan {
	plan := c.plan()
	plan.Cache = &engine.CachePlan{MaxTTLSeconds: int64(engine.DefaultCacheMaxTTL / time.Second), Refresh: refresh}
	return plan
}

// runPipeline starts and waits for one run of wf with rows as its input.
func (c *cacheE2E) runPipeline(runID string, wf *orkstackv1.Orkflow, rows int, refresh bool) *orkstackv1.Run {
	c.t.Helper()
	data, _ := structpb.NewStruct(map[string]any{"rows": rows})
	starter := engine.NewTemporalStarterWithOrkerQueue(c.temporal, c.runQueue, c.orkerQueue)
	if err := starter.StartRun(c.ctx, runID, wf, &orkstackv1.Payload{Data: data}, c.cachePlan(refresh)); err != nil {
		c.t.Fatalf("StartRun: %v", err)
	}
	run := c.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		c.t.Fatalf("run %s = %v %v", runID, run.GetStatus(), run.GetError())
	}
	return run
}

func runIDFor(label string) string {
	return fmt.Sprintf("ork-cache-%s-%d", label, time.Now().UnixNano())
}

func cacheOf(run *orkstackv1.Run, task string) *orkstackv1.CacheResult {
	return taskRunNamed(run, task).GetCache()
}

func requireOutcome(t *testing.T, run *orkstackv1.Run, task string, want orkstackv1.CacheOutcome, reason string) {
	t.Helper()
	c := cacheOf(run, task)
	if c.GetOutcome() != want || (reason != "" && c.GetMissReason() != reason) {
		t.Fatalf("run %s task %s cache = %v; want %v %s", run.GetId(), task, c, want, reason)
	}
}

// runJSON is the run as `orkctl run get --json` prints it.
func runJSON(run *orkstackv1.Run) string {
	return protojson.MarshalOptions{Multiline: false}.Format(run)
}

// deletePrefix removes every object under prefix in the shared bucket.
func (c *cacheE2E) deletePrefix(prefix string) int {
	c.t.Helper()
	raw := newRawS3ClientForCommitE2E(c.t, c.cfg)
	n := 0
	pager := s3.NewListObjectsV2Paginator(raw, &s3.ListObjectsV2Input{Bucket: aws.String(c.cfg.S3Bucket), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(c.ctx)
		if err != nil {
			c.t.Fatalf("list %s: %v", prefix, err)
		}
		for _, object := range page.Contents {
			if _, err := raw.DeleteObject(c.ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.cfg.S3Bucket), Key: object.Key}); err != nil {
				c.t.Fatalf("delete %s: %v", aws.ToString(object.Key), err)
			}
			n++
		}
	}
	return n
}

func (c *cacheE2E) memoRef(wf *orkstackv1.Orkflow, task, key string) artifacts.MemoRef {
	return artifacts.MemoRef{MemoScopeRef: artifacts.MemoScopeRef{Namespace: "default", Orkflow: wf.GetName(), TaskID: task}, Key: key}
}

// attemptLog is one attempt's whole log as the sink stored it.
func (c *cacheE2E) attemptLog(runID, taskID string, attempt int32) string {
	chunks, err := c.logs.ReadLogChunks(c.ctx, artifacts.LogRef{RunID: runID, TaskID: taskID, Attempt: attempt}, 0, 0, 0)
	if err != nil {
		return ""
	}
	var b strings.Builder
	for _, chunk := range chunks {
		b.Write(chunk.Data)
	}
	return b.String()
}

func TestIntegration_CacheSecondRunReusesTheCommitAndRunsNoContainer(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i1", nil, nil)
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i1-%d", time.Now().UnixNano()), pipelineOpts{})
	run1ID, run2ID := runIDFor("i1a"), runIDFor("i1b")
	run1 := c.runPipeline(run1ID, wf, 1000, false)
	requireOutcome(t, run1, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_ABSENT")
	requireOutcome(t, run1, "train", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_ABSENT")
	run2 := c.runPipeline(run2ID, wf, 1000, false)
	requireOutcome(t, run2, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, "")
	requireOutcome(t, run2, "train", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "")
	hit := cacheOf(run2, "featurize")
	if hit.GetSource().GetRunId() != run1ID {
		t.Fatalf("the hit names %q, want run 1 %s", hit.GetSource().GetRunId(), run1ID)
	}
	f1, f2 := taskRunNamed(run1, "featurize"), taskRunNamed(run2, "featurize")
	if len(f2.GetArtifacts()) != 1 || f2.GetArtifacts()[0].GetUri() != f1.GetArtifacts()[0].GetUri() ||
		!strings.Contains(f2.GetArtifacts()[0].GetUri(), run1ID) {
		t.Fatalf("run 2's features.txt = %v, want run 1's own object %v", f2.GetArtifacts(), f1.GetArtifacts())
	}
	records, err := c.attempts.ListAttempts(c.ctx, run2ID, "featurize", artifacts.NoItem)
	if err != nil || len(records) != 1 {
		t.Fatalf("run 2 featurize attempt records = %v (%v)", records, err)
	}
	if r := records[0]; r.GetPhase() != "memo" || r.GetImage().GetPlatformDigest() != "" || len(r.GetSystemArtifacts()) != 0 ||
		r.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || len(r.GetCache().GetKeyComponents()) == 0 {
		t.Fatalf("the hit's attempt record = %v", r)
	}
	log := c.attemptLog(run2ID, "featurize", 1)
	if !strings.Contains(log, "[ork] cache hit") || strings.Contains(log, "featurized") {
		t.Fatalf("the hit's log is not only its cache line:\n%s", log)
	}
	key, _ := artifacts.CommitKey("", run2ID, "featurize")
	root, _ := artifacts.CommitRoot(c.cfg)
	record, err := c.commits.Get(c.ctx, root+"/"+key)
	if err != nil || record.Commit.GetCache().GetSource().GetRunId() != run1ID || record.Commit.GetRunId() != run2ID {
		t.Fatalf("run 2's own commit = %v (%v)", record.Commit, err)
	}
	for _, task := range []string{"featurize", "train"} {
		entries, _, err := c.memos.ListMemos(c.ctx, artifacts.MemoScopeRef{Namespace: "default", Orkflow: wf.GetName(), TaskID: task}, "", 10)
		if err != nil || len(entries) == 0 {
			t.Fatalf("no memo for %s: %v", task, err)
		}
	}
	proof.Write(t, "engine", "cache-I1.txt", []byte(runJSON(run1)+"\n"+runJSON(run2)+"\n"))
}

func TestIntegration_CacheKeyFollowsEveryDeclaredInput(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i2", nil, nil)
	name := fmt.Sprintf("cached-pipeline-i2-%d", time.Now().UnixNano())
	wf := c.cachedPipeline(name, pipelineOpts{})
	source := c.runPipeline(runIDFor("i2a"), wf, 1000, false)
	var lines []string
	// A changed run input misses featurize.
	rows := c.runPipeline(runIDFor("i2b"), wf, 999, false)
	requireOutcome(t, rows, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_ABSENT")
	lines = append(lines, "rows=999: featurize "+cacheOf(rows, "featurize").GetMissReason())
	// A version that edits only train's with: still hits featurize.
	edited := c.cachedPipeline(name, pipelineOpts{trainSalt: "edited"})
	edited.Version = "0.1.1"
	stampDigest(t, edited)
	trainOnly := c.runPipeline(runIDFor("i2c"), edited, 1000, false)
	requireOutcome(t, trainOnly, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, "")
	if cacheOf(trainOnly, "featurize").GetSource().GetRunId() != source.GetId() {
		t.Fatalf("featurize hit %v, want run %s", cacheOf(trainOnly, "featurize").GetSource(), source.GetId())
	}
	lines = append(lines, "train edited: featurize HIT from "+source.GetId())
	// A changed featurize with: misses.
	changed := c.cachedPipeline(name, pipelineOpts{featurizeSalt: "changed"})
	changed.Version = "0.1.2"
	stampDigest(t, changed)
	featurizeChanged := c.runPipeline(runIDFor("i2d"), changed, 1000, false)
	requireOutcome(t, featurizeChanged, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_ABSENT")
	lines = append(lines, "featurize with: changed: featurize MISS")
	proof.Write(t, "engine", "cache-I2.txt", []byte(strings.Join(lines, "\n")+"\n"))
}

func TestIntegration_CacheDeletedMemoPrefixOnlyCostsTime(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i3", nil, nil)
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i3-%d", time.Now().UnixNano()), pipelineOpts{})
	first := c.runPipeline(runIDFor("i3a"), wf, 1000, false)
	second := c.runPipeline(runIDFor("i3b"), wf, 1000, false)
	requireOutcome(t, second, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, "")
	deleted := c.deletePrefix(artifacts.MemoPrefix + "/")
	third := c.runPipeline(runIDFor("i3c"), wf, 1000, false)
	requireOutcome(t, third, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_ABSENT")
	if !cacheOf(third, "featurize").GetMemoWritten() {
		t.Fatal("no fresh memo after the prefix was deleted")
	}
	for _, task := range []string{"featurize", "stamp", "train"} {
		if a, b := taskRunNamed(first, task), taskRunNamed(third, task); len(a.GetArtifacts()) != len(b.GetArtifacts()) ||
			b.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("task %s: run 3 is not of run 1's shape", task)
		}
	}
	proof.Write(t, "engine", "cache-I3.txt", []byte(fmt.Sprintf("deleted %d memo objects\n%s\n", deleted, runJSON(third))))
}

func TestIntegration_CacheCorruptMemoIsAMiss(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i4", nil, nil)
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i4-%d", time.Now().UnixNano()), pipelineOpts{})
	first := c.runPipeline(runIDFor("i4a"), wf, 1000, false)
	key := cacheOf(first, "featurize").GetKey()
	objectKey, _ := artifacts.MemoKey(c.memoRef(wf, "featurize", key))
	raw := newRawS3ClientForCommitE2E(t, c.cfg)
	if _, err := raw.PutObject(c.ctx, &s3.PutObjectInput{Bucket: aws.String(c.cfg.S3Bucket), Key: aws.String(objectKey),
		Body: strings.NewReader("garbage")}); err != nil {
		t.Fatal(err)
	}
	second := c.runPipeline(runIDFor("i4b"), wf, 1000, false)
	requireOutcome(t, second, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_UNREADABLE")
	memo, err := c.memos.GetMemo(c.ctx, c.memoRef(wf, "featurize", key))
	if err != nil || memo.GetSource().GetRunId() != second.GetId() {
		t.Fatalf("the corrupt memo was not rewritten: %v (%v)", memo, err)
	}
	proof.Write(t, "engine", "cache-I4.txt", []byte(runJSON(second)+"\n"))
}

func TestIntegration_CacheRetiredSourceIsAMiss(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i5", nil, nil)
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i5-%d", time.Now().UnixNano()), pipelineOpts{})
	first := c.runPipeline(runIDFor("i5a"), wf, 1000, false)
	commitKey, _ := artifacts.CommitKey("", first.GetId(), "featurize")
	runPrefix := commitKey[:strings.LastIndex(strings.TrimSuffix(commitKey, "/commit.json"), "/")+1]
	c.deletePrefix(runPrefix)
	second := c.runPipeline(runIDFor("i5b"), wf, 1000, false)
	requireOutcome(t, second, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "SOURCE_COMMIT_GONE")
	proof.Write(t, "engine", "cache-I5.txt", []byte(runJSON(second)+"\n"))
}

func TestIntegration_CacheMissingSourceObjectIsAMiss(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i6", nil, nil)
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i6-%d", time.Now().UnixNano()), pipelineOpts{})
	first := c.runPipeline(runIDFor("i6a"), wf, 1000, false)
	if err := c.store.Delete(c.ctx, taskRunNamed(first, "featurize").GetArtifacts()[0].GetUri()); err != nil {
		t.Fatal(err)
	}
	second := c.runPipeline(runIDFor("i6b"), wf, 1000, false)
	requireOutcome(t, second, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "SOURCE_OBJECT_UNAVAILABLE")
	if !strings.Contains(c.attemptLog(second.GetId(), "featurize", 1), "featurized 1000 rows") {
		t.Fatal("the container did not run after the miss")
	}
	proof.Write(t, "engine", "cache-I6.txt", []byte(runJSON(second)+"\n"))
}

// The Orker dies after the hit's commit.json and before completion.
func TestIntegration_CacheHitCommittedThenOrkerDiesIsAdopted(t *testing.T) {
	c := startCacheE2E(t)
	first := c.startCacheOrker("cache-i7-a", nil, nil)
	// featurize needs a second attempt to adopt the dead Orker's hit commit,
	// and stamp, in flight beside it on the same Orker, a second attempt to
	// survive that Orker's stop: the DSL default is a single attempt, which
	// would fail the run on the first attempt's cancellation instead.
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i7-%d", time.Now().UnixNano()), pipelineOpts{
		retry: &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 1, BackoffCoefficient: 1}})
	source := c.runPipeline(runIDFor("i7a"), wf, 1000, false)
	first.Stop()

	committed := make(chan struct{})
	var once sync.Once
	dying := c.startCacheOrker("cache-i7-b", nil, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput,
		next func() (*orkstackv1.ExecuteTaskResult, error)) (*orkstackv1.ExecuteTaskResult, error) {
		result, err := next()
		if err == nil && in.GetTaskId() == "featurize" && result.GetCache().GetOutcome() == orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
			once.Do(func() { close(committed) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return result, err
	})
	runID := runIDFor("i7b")
	data, _ := structpb.NewStruct(map[string]any{"rows": 1000})
	starter := engine.NewTemporalStarterWithOrkerQueue(c.temporal, c.runQueue, c.orkerQueue)
	if err := starter.StartRun(c.ctx, runID, wf, &orkstackv1.Payload{Data: data}, c.cachePlan(false)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-committed:
	case <-time.After(3 * time.Minute):
		t.Fatal("the hit never committed")
	}
	dying.Stop()
	c.startCacheOrker("cache-i7-c", nil, nil)
	run := c.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	requireOutcome(t, run, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, "")
	if cacheOf(run, "featurize").GetSource().GetRunId() != source.GetId() {
		t.Fatalf("the adopted hit names %v", cacheOf(run, "featurize").GetSource())
	}
	records, _ := c.attempts.ListAttempts(c.ctx, runID, "featurize", artifacts.NoItem)
	adopted := false
	for _, r := range records {
		adopted = adopted || r.GetCommitAdopted()
		for _, span := range r.GetPhases() {
			if span.GetPhase() == "create" || span.GetPhase() == "run" {
				t.Fatalf("attempt %d of the hit started a container: %v", r.GetAttempt(), r.GetPhases())
			}
		}
	}
	if !adopted {
		t.Fatalf("the retry did not adopt the hit's commit: %v", records)
	}
	proof.Write(t, "engine", "cache-I7.txt", []byte(fmt.Sprintf("commitAdopted=true records=%d\n%s\n", len(records), runJSON(run))))
}

func TestIntegration_CacheNoCacheBypassesAndRefreshes(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i8", nil, nil)
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i8-%d", time.Now().UnixNano()), pipelineOpts{})
	c.runPipeline(runIDFor("i8a"), wf, 1000, false)
	bypass := c.runPipeline(runIDFor("i8b"), wf, 1000, true)
	requireOutcome(t, bypass, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED, "")
	if !strings.Contains(c.attemptLog(bypass.GetId(), "featurize", 1), "featurized 1000 rows") {
		t.Fatal("a bypass did not run the container")
	}
	memo, err := c.memos.GetMemo(c.ctx, c.memoRef(wf, "featurize", cacheOf(bypass, "featurize").GetKey()))
	if err != nil || memo.GetSource().GetRunId() != bypass.GetId() {
		t.Fatalf("the memo was not refreshed: %v (%v)", memo, err)
	}
	next := c.runPipeline(runIDFor("i8c"), wf, 1000, false)
	requireOutcome(t, next, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, "")
	if cacheOf(next, "featurize").GetSource().GetRunId() != bypass.GetId() {
		t.Fatalf("the next run hit %v, want the bypassing run", cacheOf(next, "featurize").GetSource())
	}
	proof.Write(t, "engine", "cache-I8.txt", []byte(runJSON(bypass)+"\n"+runJSON(next)+"\n"))
}

// A cached 4-item fan-out. Run 2 hits every item and merge stages all
// four from run 2's own item commits. A run whose array changes misses every
// item: each item stages list's values.json, which carries the whole array,
// and every staged file is in the key (docs/DSL.md "Caching").
func TestIntegration_CacheFanOutItemsHitIndividually(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i9", nil, nil)
	name := fmt.Sprintf("cached-each-i9-%d", time.Now().UnixNano())
	wf := c.eachDefinition(name, 4, 0, "")
	wf.Tasks[1].Cache = &orkstackv1.CacheSpec{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}
	stampDigest(t, wf)
	start := func(label string, def *orkstackv1.Orkflow) *orkstackv1.Run {
		runID := runIDFor(label)
		starter := engine.NewTemporalStarterWithOrkerQueue(c.temporal, c.runQueue, c.orkerQueue)
		if err := starter.StartRun(c.ctx, runID, def, nil, c.cachePlan(false)); err != nil {
			t.Fatal(err)
		}
		run := c.wait(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %s = %v %v", runID, run.GetStatus(), run.GetError())
		}
		return run
	}
	first := start("i9a", wf)
	second := start("i9b", wf)
	items := taskRunNamed(second, "convert").GetItems()
	for i, item := range items {
		if item.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || item.GetCache().GetSource().GetRunId() != first.GetId() ||
			item.GetCache().GetSource().GetItem() != int32(i) {
			t.Fatalf("item %d of run 2 = %v", i, item.GetCache())
		}
		if _, err := c.commits.Get(c.ctx, item.GetCommit().GetUri()); err != nil || !strings.Contains(item.GetCommit().GetUri(), second.GetId()) {
			t.Fatalf("item %d's commit %s is not run 2's own: %v", i, item.GetCommit().GetUri(), err)
		}
	}
	if values := readValues(t, c.eachE2E, taskRunNamed(second, "merge").GetValuesRef()); !strings.Contains(values, `"parts":4`) {
		t.Fatalf("merge read %s", values)
	}
	changed := c.eachDefinition(name, 4, 0, "")
	changed.Tasks[0].Command[2] = strings.Replace(changed.Tasks[0].Command[2], `"p3"`, `"q3"`, 1)
	changed.Tasks[1].Cache = wf.Tasks[1].Cache
	changed.Version = "1.0.1"
	stampDigest(t, changed)
	third := start("i9c", changed)
	for i, item := range taskRunNamed(third, "convert").GetItems() {
		if item.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS {
			t.Fatalf("item %d of a changed array = %v, want a miss (the staged array changed)", i, item.GetCache())
		}
	}
	proof.Write(t, "engine", "cache-I9.txt", []byte(runJSON(second)+"\n"))
}

func TestIntegration_CacheNamespaceScopeSharesAcrossOrkflows(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i10", nil, nil)
	token := time.Now().UnixNano()
	shared := fmt.Sprintf("featurize-shared-%d", token)
	a := c.cachedPipeline(fmt.Sprintf("cached-a-i10-%d", token), pipelineOpts{scope: orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, featurizeID: shared})
	b := c.cachedPipeline(fmt.Sprintf("cached-b-i10-%d", token), pipelineOpts{scope: orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, featurizeID: shared})
	first := c.runPipeline(runIDFor("i10a"), a, 1000, false)
	other := c.runPipeline(runIDFor("i10b"), b, 1000, false)
	requireOutcome(t, other, shared, orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, "")
	if cacheOf(other, shared).GetSource().GetRunId() != first.GetId() {
		t.Fatalf("Orkflow b hit %v, want a's run", cacheOf(other, shared).GetSource())
	}
	// Under orkflow scope another Orkflow never hits.
	solo := fmt.Sprintf("featurize-solo-%d", token)
	c.runPipeline(runIDFor("i10c"), c.cachedPipeline(fmt.Sprintf("cached-c-i10-%d", token), pipelineOpts{featurizeID: solo}), 1000, false)
	d := c.runPipeline(runIDFor("i10d"), c.cachedPipeline(fmt.Sprintf("cached-d-i10-%d", token), pipelineOpts{featurizeID: solo}), 1000, false)
	requireOutcome(t, d, solo, orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_ABSENT")
	proof.Write(t, "engine", "cache-I10.txt", []byte(runJSON(other)+"\n"))
}

// failingMemos is a memo store whose every call fails, beside a working
// bucket for everything else.
type failingMemos struct{}

var errMemoOutage = errors.New("memo store outage (test)")

func (failingMemos) GetMemo(context.Context, artifacts.MemoRef) (*orkstackv1.CacheMemo, error) {
	return nil, errMemoOutage
}
func (failingMemos) PutMemo(context.Context, *orkstackv1.CacheMemo) error { return errMemoOutage }
func (failingMemos) ListMemos(context.Context, artifacts.MemoScopeRef, string, int) ([]artifacts.MemoEntry, string, error) {
	return nil, "", errMemoOutage
}
func (failingMemos) DeleteMemo(context.Context, artifacts.MemoRef) error { return errMemoOutage }

func TestIntegration_CacheMemoStoreOutageIsAMiss(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i11", failingMemos{}, nil)
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i11-%d", time.Now().UnixNano()), pipelineOpts{})
	run := c.runPipeline(runIDFor("i11"), wf, 1000, false)
	requireOutcome(t, run, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_UNREADABLE")
	if cacheOf(run, "featurize").GetMemoWriteError() == "" || cacheOf(run, "featurize").GetMemoWritten() {
		t.Fatalf("memo write = %v", cacheOf(run, "featurize"))
	}
	proof.Write(t, "engine", "cache-I11.txt", []byte(runJSON(run)+"\n"))
}

func TestIntegration_CacheExpiredMemoIsAMiss(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i12", nil, nil)
	wf := c.cachedPipeline(fmt.Sprintf("cached-pipeline-i12-%d", time.Now().UnixNano()), pipelineOpts{})
	first := c.runPipeline(runIDFor("i12a"), wf, 1000, false)
	ref := c.memoRef(wf, "featurize", cacheOf(first, "featurize").GetKey())
	memo, err := c.memos.GetMemo(c.ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	memo.Source.FinishedAt = timestamppb.New(memo.GetSource().GetFinishedAt().AsTime().Add(-48 * time.Hour))
	if err := c.memos.PutMemo(c.ctx, memo); err != nil {
		t.Fatal(err)
	}
	second := c.runPipeline(runIDFor("i12b"), wf, 1000, false)
	requireOutcome(t, second, "featurize", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_EXPIRED")
	proof.Write(t, "engine", "cache-I12.txt", []byte(runJSON(second)+"\n"))
}

// An Orker that predates caching on the pool refuses the submit, and
// no run is left behind.
func TestIntegration_CacheSubmitRefusesAPoolWhoseOrkerPredatesCaching(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	namespace := privateNamespaceForCommitE2E(t)
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()
	if err := task.EnsureNamespace(ctx, temporalClient, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, namespace)
	token := time.Now().UnixNano()
	orkerQueue := fmt.Sprintf("ork-cache-i13-tasks-%d", token)
	// A stand-in Orker of an older build: it answers DescribeOrker without
	// cache_supported, serving the default pool's channel.
	startStandInOrker(t, temporalClient, orkerQueue, &orkstackv1.DescribeOrkerResult{
		OrkerId: "old-orker", BuildId: "2026.09.01", Pool: task.DefaultPool, EachSupported: true})
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, fmt.Sprintf("ork-cache-i13-run-%d", token), orkerQueue)
	reader := engine.NewTemporalRunReader(temporalClient)
	svc := api.NewService(api.Config{
		Starter: starter, Reader: reader, RawQueue: orkerQueue,
		Pools: engine.NewTemporalPoolProbe(temporalClient), Fleet: starter,
		NewRunID: func() string { return fmt.Sprintf("ork-cache-i13-%d", time.Now().UnixNano()) },
	})
	name := fmt.Sprintf("cached-i13-%d", token)
	if _, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: "apiVersion: orkstack/v1\nkind: Orkflow\nname: " + name +
		"\nversion: \"1\"\ntasks:\n  - id: featurize\n    uses: shell\n    cache: {enabled: true}\n    with: {op: exec, command: [\"true\"]}\n"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	before, err := reader.ListRuns(ctx, engine.RunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	// The pool must read live before the describe question is asked. Until
	// the stand-in answers on its own channel, a submit is admitted (a pool
	// none of whose Orkers answers is logged, not refused), so the runs the
	// wait admitted are its own, not the refusal's.
	deadline := time.Now().Add(time.Minute)
	var submitErr error
	admitted := map[string]bool{}
	for time.Now().Before(deadline) {
		var submitted *connect.Response[orkstackv1.SubmitRunResponse]
		submitted, submitErr = svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name}))
		if submitErr == nil {
			admitted[submitted.Msg.GetRun().GetId()] = true
		}
		if connect.CodeOf(submitErr) == connect.CodeFailedPrecondition && strings.Contains(submitErr.Error(), "predates task caching") {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	if connect.CodeOf(submitErr) != connect.CodeFailedPrecondition || !strings.Contains(submitErr.Error(), `Orker "old-orker" (build "2026.09.01")`) {
		t.Fatalf("submit = %v", submitErr)
	}
	after, err := reader.ListRuns(ctx, engine.RunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, run := range before {
		known[run.GetId()] = true
	}
	var left []string
	for _, run := range after {
		if !known[run.GetId()] && !admitted[run.GetId()] {
			left = append(left, run.GetId())
		}
	}
	if len(left) != 0 {
		t.Fatalf("a refused submission left %d runs behind: %v", len(left), left)
	}
	proof.Write(t, "engine", "cache-I13.txt", []byte(submitErr.Error()+"\n"))
}

// The release-block example's sign cached: the second parent run's child sign hits.
func TestIntegration_CacheComposedChildTaskHits(t *testing.T) {
	c := startCacheE2E(t)
	c.startCacheOrker("cache-i14", nil, nil)
	token := time.Now().UnixNano()
	child := c.composedChild(fmt.Sprintf("release-block-i14-%d", token), "")
	child.Tasks[0].Cache = &orkstackv1.CacheSpec{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}
	stampDigest(t, child)
	parent := c.composedParent(fmt.Sprintf("composed-release-i14-%d", token), child)
	start := func(label string) (*orkstackv1.Run, *orkstackv1.Run) {
		runID := runIDFor(label)
		childPlan := c.cachePlan(false)
		cp, err := engine.NewChildRunPlan(child, childPlan, false, false)
		if err != nil {
			t.Fatal(err)
		}
		cp.RunID = runID + "-child"
		plan := c.plan()
		plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
		starter := engine.NewTemporalStarterWithOrkerQueue(c.temporal, c.runQueue, c.orkerQueue)
		if err := starter.StartRun(c.ctx, runID, parent, nil, plan); err != nil {
			t.Fatalf("StartRun: %v", err)
		}
		run := c.wait(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("parent %s = %v %v", runID, run.GetStatus(), run.GetError())
		}
		return run, c.wait(cp.RunID)
	}
	_, firstChild := start("i14a")
	requireOutcome(t, firstChild, "sign", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_ABSENT")
	_, secondChild := start("i14b")
	requireOutcome(t, secondChild, "sign", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, "")
	if cacheOf(secondChild, "sign").GetSource().GetRunId() != firstChild.GetId() {
		t.Fatalf("the child's sign hit %v, want the first child run", cacheOf(secondChild, "sign").GetSource())
	}
	proof.Write(t, "engine", "cache-I14.txt", []byte(runJSON(secondChild)+"\n"))
}

// Opt-in recorder of testdata/history/run-cached-v1.json — the SECOND
// run of the cached pipeline (featurize a hit, stamp uncached, train a miss),
// by a versioned worker, with stand-in Orker results that carry exactly what
// the real Orker reports.
func TestIntegration_RecordCachedRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to record " + cachedHistory +
			" — the committed fixture is meant to age, not to be refreshed")
	}
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-cache-rec-run-" + suffix
	orkerQueue := "ork-cache-rec-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-cache-rec-" + suffix, BuildID: "v1"}

	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true,
	}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	runWorker.RegisterActivityWithOptions(engine.NewEachActivities(&memValues{objects: map[string][]byte{}}).CountEachItems,
		activity.RegisterOptions{Name: engine.ActivityCountEachItems})

	var mu sync.Mutex
	sources := map[string]*orkstackv1.CacheSource{}
	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	}))
	orkerWorker.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := &orkstackv1.ExecuteTaskResult{
			TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
			ValuesJson: []byte(`{}`), ValuesInlined: true,
			ValuesRef: &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://values/" + in.GetRunId() + "/" + in.GetTaskId(),
				Sha256: strings.Repeat("a", 64), SizeBytes: 2},
			Commit: &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: strings.Repeat("b", 64)},
		}
		if in.GetTaskId() == "stamp" {
			result.ValuesJson = []byte(`{"value":"` + in.GetRunId() + `"}`)
		}
		if in.GetCache() == nil {
			return result, nil
		}
		key := strings.Repeat(map[string]string{"featurize": "f", "train": "e"}[in.GetTaskId()], 64)
		mu.Lock()
		defer mu.Unlock()
		if source, ok := sources[in.GetTaskId()]; ok && in.GetTaskId() == "featurize" {
			result.Cache = &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Key: key, Source: source}
			return result, nil
		}
		result.Cache = &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, Key: key,
			MissReason: "MEMO_ABSENT", MissDetail: "no memo for key " + key[:12] + "…", MemoWritten: true}
		sources[in.GetTaskId()] = &orkstackv1.CacheSource{RunId: in.GetRunId(), TaskId: in.GetTaskId(),
			OrkflowName: in.GetOrkflowName(), OrkflowVersion: in.GetOrkflowVersion(), CommitUri: in.GetCommitUri(),
			CommitSha256: strings.Repeat("b", 64), Attempt: 1,
			StartedAt: timestamppb.New(time.Unix(1_759_000_000, 0)), FinishedAt: timestamppb.New(time.Unix(1_759_000_482, 0))}
		return result, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, temporalClient, deployment, runQueue, orkerQueue)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)
	wf := compileExample(t, "cached-pipeline.ork.yaml")
	plan := rawPlan(orkerQueue)
	plan.CommitRoot = "mem://commits"
	plan.Cache = &engine.CachePlan{MaxTTLSeconds: int64(engine.DefaultCacheMaxTTL / time.Second)}
	data, _ := structpb.NewStruct(map[string]any{"rows": 1000})
	input := &orkstackv1.Payload{Data: data}
	var runID string
	for i := 0; i < 2; i++ {
		runID = fmt.Sprintf("ork-cached-fixture-%d", time.Now().UnixNano())
		if err := starter.StartRun(ctx, runID, wf, input, plan); err != nil {
			t.Fatalf("StartRun: %v", err)
		}
		if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
			t.Fatalf("fixture run %s did not succeed: %v", runID, err)
		}
	}
	recorded := &history.History{}
	iter := temporalClient.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		recorded.Events = append(recorded.Events, event)
	}
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	if err != nil {
		t.Fatalf("encode history: %v", err)
	}
	if err := os.WriteFile(cachedHistory, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", cachedHistory, err)
	}
	t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
		len(recorded.Events), runID, cachedHistory, deployment)
}
