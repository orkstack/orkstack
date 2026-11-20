//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The lines fan-out under faults: the source changed after its commit, the
// object store failing at the expansion and at an item, an Orker lost after
// an item committed, and a producer whose result is a memo hit.

// rangeOutage is an artifact store whose ranged reads fail while fail says
// so: an object-store outage of one kind of read, which the real store
// cannot be made to have on demand.
type rangeOutage struct {
	artifacts.Store
	fail  func(offset, length int64) bool
	reads atomic.Int32
}

func (r *rangeOutage) GetRange(ctx context.Context, uri string, offset, length int64) (io.ReadCloser, error) {
	r.reads.Add(1)
	if r.fail != nil && r.fail(offset, length) {
		return nil, errors.New("the object store is unreachable")
	}
	return r.Store.GetRange(ctx, uri, offset, length)
}

// objectKeyOf is the bucket key an s3:// URI names.
func objectKeyOf(t *testing.T, uri string) string {
	t.Helper()
	u, err := url.Parse(uri)
	must.OKf(t, err, "parse %s", uri)
	if u.Scheme != "s3" {
		t.Fatalf("%s is not an s3:// object", uri)
	}
	return strings.TrimPrefix(u.Path, "/")
}

// TestIntegration_EachLinesTamperedSourceFails: the source's bytes are
// replaced, at the same size, after its producer committed them. The index
// holds every byte to the committed digest: the fan-out fails
// EACH_EXPANSION_FAILED, never retried and never blamed on a line, and no item
// is scheduled.
func TestIntegration_EachLinesTamperedSourceFails(t *testing.T) {
	e := startEachE2E(t, true)
	raw := newRawS3ClientForCommitE2E(t, e.cfg)
	tap := &linesTap{beforeIndex: func(ctx context.Context, in *orkstackv1.IndexEachLinesInput) error {
		key := objectKeyOf(t, in.GetSource().GetUri())
		body, err := e.store.Get(ctx, in.GetSource().GetUri())
		if err != nil {
			return err
		}
		original, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return err
		}
		tampered := bytes.Replace(original, []byte(`{"n":3}`), []byte(`{"n":9}`), 1)
		if bytes.Equal(tampered, original) || len(tampered) != len(original) {
			return fmt.Errorf("could not tamper %d bytes", len(original))
		}
		_, err = raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(e.cfg.S3Bucket), Key: aws.String(key),
			Body: bytes.NewReader(tampered)})
		return err
	}}
	e.startLinesOrker("lines-t1", tap)
	runID := fmt.Sprintf("ork-lines-t1-%d", time.Now().UnixNano())
	e.startLines(runID, e.linesDefinition("lines-t1", rowsOf(5), 1, `printf '{}' > /orkstack/output/values.json`))
	run := e.wait(runID)
	node := taskRunNamed(run, "convert")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != eachlines.ReasonExpansionFailed ||
		!strings.Contains(node.GetError().GetMessage(), "do not match its committed digest") {
		t.Fatalf("run %v, convert %v; want EACH_EXPANSION_FAILED for the digest", run.GetStatus(), node.GetError())
	}
	if tap.indexed.Load() != 1 {
		t.Fatalf("indexed %d times; a digest contradiction is never retried", tap.indexed.Load())
	}
	if n := e.itemsScheduled("convert"); n != 0 || len(node.GetItems()) != 0 {
		t.Fatalf("%d items scheduled; want none from a tampered source", n)
	}
}

// TestIntegration_EachLinesStoreOutageAtExpansion: an object store that
// fails the index's reads spends the plan's index attempts. One that comes
// back within them indexes and runs every item; one that never does fails
// the fan-out EACH_EXPANSION_FAILED, never success, with no item scheduled.
func TestIntegration_EachLinesStoreOutageAtExpansion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		id      string
		failing int32 // how many index attempts the outage spans
		want    orkstackv1.Status
	}{
		{"the store comes back", "lines-o1", 2, orkstackv1.Status_STATUS_SUCCEEDED},
		{"the store stays down", "lines-o2", engine.DefaultEachIndexMaxAttempts, orkstackv1.Status_STATUS_FAILED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := startEachE2E(t, true)
			tap := &linesTap{}
			tap.store = func(s artifacts.Store) artifacts.Store {
				// Only the index's reads fail: an item reads after its index answered.
				return &rangeOutage{Store: s, fail: func(int64, int64) bool { return tap.indexed.Load() <= tc.failing }}
			}
			e.startLinesOrker(tc.id, tap)
			runID := fmt.Sprintf("ork-%s-%d", tc.id, time.Now().UnixNano())
			e.startLines(runID, e.linesDefinition(tc.id, rowsOf(5), 1, `printf '{}' > /orkstack/output/values.json`))
			run := e.wait(runID)
			node := taskRunNamed(run, "convert")
			if run.GetStatus() != tc.want {
				t.Fatalf("run %v, convert %v; want %v", run.GetStatus(), node.GetError(), tc.want)
			}
			if tc.want == orkstackv1.Status_STATUS_SUCCEEDED {
				if tap.indexed.Load() != tc.failing+1 || len(node.GetItems()) != 5 {
					t.Fatalf("indexed %d times, %d items; want %d attempts and 5 items", tap.indexed.Load(), len(node.GetItems()), tc.failing+1)
				}
				return
			}
			if node.GetError().GetCode() != eachlines.ReasonExpansionFailed || tap.indexed.Load() != engine.DefaultEachIndexMaxAttempts {
				t.Fatalf("convert %v after %d attempts; want EACH_EXPANSION_FAILED after %d", node.GetError(), tap.indexed.Load(),
					engine.DefaultEachIndexMaxAttempts)
			}
			if n := e.itemsScheduled("convert"); n != 0 {
				t.Fatalf("%d items scheduled after the expansion failed", n)
			}
		})
	}
}

// TestIntegration_EachLinesStoreOutageAtItem: an item whose span the store
// cannot read fails that attempt INPUT_ARTIFACT_UNAVAILABLE, retried under
// the task's own policy; its next attempt reads the span and the run
// succeeds, every other item run once.
func TestIntegration_EachLinesStoreOutageAtItem(t *testing.T) {
	e := startEachE2E(t, true)
	var once sync.Once
	var outage atomic.Bool
	tap := &linesTap{}
	tap.store = func(s artifacts.Store) artifacts.Store {
		return &rangeOutage{Store: s, fail: func(offset, _ int64) bool {
			// The first ranged read of item 2's span, after the index.
			if tap.indexed.Load() == 0 || offset != int64(len(`{"n":0}`+"\n")*2) {
				return false
			}
			failed := false
			once.Do(func() { failed = true; outage.Store(true) })
			return failed
		}}
	}
	e.startLinesOrker("lines-oi", tap)
	runID := fmt.Sprintf("ork-lines-oi-%d", time.Now().UnixNano())
	wf := e.linesDefinition("lines-oi", rowsOf(5), 1, `printf '{}' > /orkstack/output/values.json`)
	wf.Tasks[1].Retry = &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 1}
	e.startLines(runID, wf)
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !outage.Load() {
		t.Fatalf("run %v %v, outage %v; want the run to ride out one failed span read", run.GetStatus(), run.GetError(), outage.Load())
	}
	records, err := e.attempts.ListAttempts(e.ctx, runID, "convert", artifacts.ItemAt(2))
	must.OKf(t, err, "list item 2's attempts")
	unavailable := 0
	for _, r := range records {
		if r.GetError().GetCode() == orker.ReasonInputUnavailable {
			unavailable++
		}
	}
	if len(records) != 2 || unavailable != 1 {
		t.Fatalf("item 2's attempts = %v; want one %s then a success", records, orker.ReasonInputUnavailable)
	}
	for i := 0; i < 5; i++ {
		if i == 2 {
			continue
		}
		e.mu.Lock()
		n := e.scheduled[fmt.Sprintf("convert#%d", i)]
		e.mu.Unlock()
		if n != 1 {
			t.Fatalf("item %d was scheduled %d times; want once", i, n)
		}
	}
}

// TestIntegration_EachLinesOrkerKilledAfterCommit: an Orker that commits item
// 0 and dies before Temporal hears of it leaves a commit the next attempt
// adopts, on another Orker, without reading the span again or starting a
// container; the run succeeds.
func TestIntegration_EachLinesOrkerKilledAfterCommit(t *testing.T) {
	e := startEachE2E(t, true)
	committed := make(chan struct{})
	var once sync.Once
	first := &linesTap{execute: func(ctx context.Context, in *orkstackv1.ExecuteTaskInput, next func() (*orkstackv1.ExecuteTaskResult, error)) (*orkstackv1.ExecuteTaskResult, error) {
		result, err := next()
		if err == nil && in.GetEach() != nil && in.GetEach().GetIndex() == 0 {
			once.Do(func() { close(committed) })
			<-ctx.Done() // never answers: its worker is stopped under it
			return nil, ctx.Err()
		}
		return result, err
	}}
	w, _ := e.startLinesOrker("lines-a1", first)
	runID := fmt.Sprintf("ork-lines-a1-%d", time.Now().UnixNano())
	wf := e.linesDefinition("lines-a1", rowsOf(3), 1, `printf '{}' > /orkstack/output/values.json`)
	wf.Tasks[1].Each.MaxParallel = 1
	// The dying Orker's attempt reports its own cancel as it goes: the
	// retry that adopts the commit is the task's own second attempt.
	wf.Tasks[1].Retry = &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 1}
	e.startLines(runID, wf)
	select {
	case <-committed:
	case <-time.After(3 * time.Minute):
		t.Fatal("item 0 never committed")
	}
	w.Stop()
	// The second Orker counts the ranged reads item 0's retry makes.
	var retryReads atomic.Int32
	retryReads.Store(-1)
	second := &linesTap{}
	var counted *rangeOutage
	second.store = func(s artifacts.Store) artifacts.Store {
		counted = &rangeOutage{Store: s}
		return counted
	}
	second.execute = func(ctx context.Context, in *orkstackv1.ExecuteTaskInput, next func() (*orkstackv1.ExecuteTaskResult, error)) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach() == nil || in.GetEach().GetIndex() != 0 {
			return next()
		}
		before := counted.reads.Load()
		result, err := next()
		retryReads.Store(counted.reads.Load() - before)
		return result, err
	}
	e.startLinesOrker("lines-a2", second)
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(taskRunNamed(run, "convert").GetItems()) != 3 {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	records, err := e.attempts.ListAttempts(e.ctx, runID, "convert", artifacts.ItemAt(0))
	must.OKf(t, err, "list item 0's attempts")
	adopted := false
	for _, r := range records {
		adopted = adopted || r.GetCommitAdopted()
	}
	if !adopted {
		t.Fatalf("item 0's retry did not adopt its commit: %v", records)
	}
	if n := retryReads.Load(); n != 0 {
		t.Fatalf("item 0's retry made %d ranged reads; an adopted commit reads no span", n)
	}
	e.mu.Lock()
	scheduled := e.scheduled["convert#0"]
	e.mu.Unlock()
	if n := scheduled; n < 2 {
		t.Fatalf("item 0 was scheduled %d times; want its retry on the second Orker", n)
	}
}

// TestIntegration_EachLinesCachedProducerMemoHit: the producer's result is a
// memo hit in the second run; its artifact is the first run's, held by the
// memo's claim. The second run's fan-out indexes that artifact under its own
// run, and every item runs.
func TestIntegration_EachLinesCachedProducerMemoHit(t *testing.T) {
	e := startEachE2E(t, true)
	memos, err := artifacts.NewMemoStore(e.cfg)
	must.OKf(t, err, "open memo store")
	tap := &linesTap{memos: memos}
	e.startLinesOrker("lines-m1", tap)
	wf := e.linesDefinition(fmt.Sprintf("lines-m1-%d", time.Now().UnixNano()), rowsOf(5), 1, `printf '{}' > /orkstack/output/values.json`)
	wf.Tasks[0].Image.IndexDigest = digestOf(e.alpine)
	wf.Tasks[0].Cache = &orkstackv1.CacheSpec{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}
	start := func(label string) *orkstackv1.Run {
		runID := fmt.Sprintf("ork-lines-m1%s-%d", label, time.Now().UnixNano())
		plan := e.plan()
		plan.EachLines = engine.DefaultEachLinesPolicy()
		plan.Cache = &engine.CachePlan{MaxTTLSeconds: int64(engine.DefaultCacheMaxTTL / time.Second)}
		starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
		must.OKf(t, starter.StartRun(e.ctx, runID, wf, nil, plan), "StartRun")
		run := e.wait(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %s = %v %v", runID, run.GetStatus(), run.GetError())
		}
		return run
	}
	first := start("a")
	second := start("b")
	if got := taskRunNamed(second, "list").GetCache().GetOutcome(); got != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		t.Fatalf("the second run's list = %v; want a memo hit", taskRunNamed(second, "list").GetCache())
	}
	a, b := taskRunNamed(first, "convert").GetEach().GetLines(), taskRunNamed(second, "convert").GetEach().GetLines()
	if a.GetSource().GetSha256() != b.GetSource().GetSha256() || a.GetIndex().GetUri() == b.GetIndex().GetUri() {
		t.Fatalf("lines %v and %v; want one source indexed under each run", a, b)
	}
	if tap.indexed.Load() != 2 || len(taskRunNamed(second, "convert").GetItems()) != 5 {
		t.Fatalf("indexed %d times, %d items in the second run", tap.indexed.Load(), len(taskRunNamed(second, "convert").GetItems()))
	}
}

// TestIntegration_EachLinesProducerBuildsTheIndex: the producer's Orker
// indexes the lines as it commits them, and the fan-out runs every item from
// that index with no index activity of its own. When the producer's result
// is a memo hit, which builds nothing, the fan-out indexes the lines itself.
func TestIntegration_EachLinesProducerBuildsTheIndex(t *testing.T) {
	e := startEachE2E(t, true)
	memos, err := artifacts.NewMemoStore(e.cfg)
	must.OKf(t, err, "open memo store")
	tap := &linesTap{memos: memos, producerIndexes: true}
	e.startLinesOrker("lines-p1", tap)
	convert := `n=$(sed -n 's/.*"n": *"\([0-9]*\)".*/\1/p' /orkstack/input/config.json) && ` +
		`printf '{"n":%s}' "$n" > /orkstack/output/values.json`
	wf := e.linesDefinition(fmt.Sprintf("lines-p1-%d", time.Now().UnixNano()), rowsOf(5), 1, convert)
	wf.Tasks[0].Image.IndexDigest = digestOf(e.alpine)
	wf.Tasks[0].Cache = &orkstackv1.CacheSpec{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}
	start := func(label string) *orkstackv1.Run {
		runID := fmt.Sprintf("ork-lines-p1%s-%d", label, time.Now().UnixNano())
		plan := e.plan()
		plan.EachLines = engine.DefaultEachLinesPolicy()
		plan.Cache = &engine.CachePlan{MaxTTLSeconds: int64(engine.DefaultCacheMaxTTL / time.Second)}
		starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
		must.OKf(t, starter.StartRun(e.ctx, runID, wf, nil, plan), "StartRun")
		run := e.wait(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %s = %v %v", runID, run.GetStatus(), run.GetError())
		}
		return run
	}
	first := start("a")
	if tap.indexed.Load() != 0 {
		t.Fatalf("indexed %d times; the producer's index makes the fan-out's own unnecessary", tap.indexed.Load())
	}
	node := taskRunNamed(first, "convert")
	if node.GetEach().GetCount() != 5 || node.GetEach().GetLines().GetIndex().GetUri() == "" {
		t.Fatalf("convert each = %v", node.GetEach())
	}
	for i, item := range node.GetItems() {
		if got := readValues(t, e, item.GetValuesRef()); got != fmt.Sprintf(`{"n":%d}`, i) {
			t.Fatalf("item %d read %s", i, got)
		}
	}
	second := start("b")
	if got := taskRunNamed(second, "list").GetCache().GetOutcome(); got != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		t.Fatalf("the second run's list = %v; want a memo hit", taskRunNamed(second, "list").GetCache())
	}
	if tap.indexed.Load() != 1 || len(taskRunNamed(second, "convert").GetItems()) != 5 {
		t.Fatalf("indexed %d times, %d items; a memo hit builds no index, so the fan-out indexes once",
			tap.indexed.Load(), len(taskRunNamed(second, "convert").GetItems()))
	}
}
