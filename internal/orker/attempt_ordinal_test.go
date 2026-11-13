package orker

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// creatingRuntime wraps a runtime and keeps the labels of every container it
// was asked to create.
type creatingRuntime struct {
	ContainerRuntime
	mu     sync.Mutex
	labels []map[string]string
}

func (r *creatingRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	r.labels = append(r.labels, spec.Labels)
	r.mu.Unlock()
	return r.ContainerRuntime.Create(ctx, spec)
}

// Start writes one line of output before the wrapped runtime starts, so the
// attempt has a log.
func (r *creatingRuntime) Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error {
	_, _ = io.WriteString(stdout, "hello from the workload\n")
	return r.ContainerRuntime.Start(ctx, id, stdout, stderr)
}

func (r *creatingRuntime) creates() []map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]string(nil), r.labels...)
}

func claimKey(t *testing.T, in *orkstackv1.ExecuteTaskInput, n int32) string {
	t.Helper()
	key, err := artifacts.AttemptClaimKey(artifacts.AttemptRef{RunID: in.GetRunId(), TaskID: in.GetTaskId(), Attempt: n})
	must.OKf(t, err, "claim key")
	return key
}

func releasedKey(t *testing.T, in *orkstackv1.ExecuteTaskInput, n int32) string {
	t.Helper()
	key, err := artifacts.AttemptReleasedKey(artifacts.AttemptRef{RunID: in.GetRunId(), TaskID: in.GetTaskId(), Attempt: n})
	must.OKf(t, err, "released key")
	return key
}

// silentCluster is a loop on a cluster that answers nothing for an hour:
// anything that waits on it waits past the test's own deadline.
func silentCluster(context.Context) *Heartbeater {
	return &Heartbeater{
		interval:   5 * time.Millisecond,
		send:       func(context.Context, HeartbeatProgress) error { return context.DeadlineExceeded },
		ackTimeout: time.Hour,
	}
}

func executeWithin(t *testing.T, d *OCIDispatcher, ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	t.Helper()
	type outcome struct {
		result *orkstackv1.ExecuteTaskResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := d.Execute(ctx, in)
		done <- outcome{result, err}
	}()
	select {
	case o := <-done:
		return o.result, o.err
	case <-time.After(10 * time.Second):
		t.Fatal("Execute is still waiting after 10 s")
		return nil, nil
	}
}

func logAttempts(logs *recordingLogStore) map[int32]bool {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	seen := map[int32]bool{}
	for _, chunk := range logs.chunks {
		seen[chunk.Attempt] = true
	}
	return seen
}

// TestExecute_TheOrdinalNamesTheRecordTheLogTheCommitAndTheContainer: an
// attempt claims ordinal 1, and the next attempt of the scope ordinal 2,
// whatever Temporal numbered them (here, 1 both times); every carrier names
// it, and the attempt releases it once its container is removed.
func TestExecute_TheOrdinalNamesTheRecordTheLogTheCommitAndTheContainer(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	runtime := &creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 3}}
	d := newAttemptDispatcher(t, runtime, attempts)
	logs := &recordingLogStore{}
	d.Logs = logs
	in := baseExportInput("ordinal")

	for want := int32(1); want <= 2; want++ {
		_, err := d.Execute(context.Background(), in)
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) {
			t.Fatalf("attempt %d: Execute = %v, want the container's own failure", want, err)
		}
		var detail *orkstackv1.ActivityFailureDetail
		must.OKf(t, appErr.Details(&detail), "failure detail")
		if detail.GetAttempt() != want {
			t.Errorf("attempt %d: failure detail names attempt %d", want, detail.GetAttempt())
		}
		if !attempts.HasObject(claimKey(t, in, want)) || !attempts.HasObject(releasedKey(t, in, want)) {
			t.Errorf("attempt %d: claim or release marker missing", want)
		}
		created := runtime.creates()
		if got := created[len(created)-1]["ork.attempt"]; got != strconv.Itoa(int(want)) {
			t.Errorf("attempt %d: container label ork.attempt = %q", want, got)
		}
	}
	records, err := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	must.OKf(t, err, "ListAttempts")
	if len(records) != 2 || records[0].GetAttempt() != 1 || records[1].GetAttempt() != 2 {
		t.Fatalf("records %v, want attempts 1 and 2", records)
	}
	if seen := logAttempts(logs); !seen[1] || !seen[2] || len(seen) != 2 {
		t.Errorf("log chunks were written under attempts %v, want 1 and 2", seen)
	}

	// A success commits under its ordinal.
	ok := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	ok.Commits = artifacts.NewMemoryCommitStore()
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/" + in.GetTaskId() + "/commit.json"
	result, err := ok.Execute(context.Background(), in)
	must.OKf(t, err, "a succeeding third attempt")
	if result.GetAttempt() != 3 {
		t.Errorf("result names attempt %d, want 3", result.GetAttempt())
	}
	record, err := ok.Commits.Get(context.Background(), in.GetCommitUri())
	must.OKf(t, err, "read the commit")
	if record.Commit.GetAttempt() != 3 {
		t.Errorf("commit names attempt %d, want 3", record.Commit.GetAttempt())
	}

	// An attempt that adopts the commit records under an ordinal of its own,
	// while its result names the committing one.
	adopted, err := ok.Execute(context.Background(), in)
	must.OKf(t, err, "an adopting fourth attempt")
	if !adopted.GetCommitAdopted() || adopted.GetAttempt() != 3 {
		t.Errorf("adopted = %v attempt %d, want the committing attempt 3", adopted.GetCommitAdopted(), adopted.GetAttempt())
	}
	if !attempts.HasObject(claimKey(t, in, 4)) {
		t.Error("the adopting attempt has no ordinal of its own")
	}
}

// TestExecute_StoppedBeforeTheClaimLeavesNothing: an attempt whose run was
// terminated or cancelled before its first heartbeat was answered claims
// nothing, records nothing, logs nothing, joins no pin and creates no
// container: there is nothing of it for anyone to find.
func TestExecute_StoppedBeforeTheClaimLeavesNothing(t *testing.T) {
	for name, cause := range map[string]error{
		"terminated (NotFound)": serviceerror.NewNotFound("workflow execution already completed"),
		"cancelled":             &temporal.CanceledError{},
		"heartbeat lost":        &heartbeatUnanswered{after: time.Second, err: context.DeadlineExceeded},
	} {
		t.Run(name, func(t *testing.T) {
			attempts := artifacts.NewMemoryAttemptStore()
			runtime := &creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 0}}
			d := newAttemptDispatcher(t, runtime, attempts)
			logs := &recordingLogStore{}
			d.Logs = logs
			d.heartbeaters = silentCluster
			in := baseExportInput("stopped-early")
			in.Pin = &orkstackv1.PinContext{Name: "p", InstanceId: "p-1", TimeoutSeconds: 60}

			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(cause)
			if _, err := executeWithin(t, d, ctx, in); err == nil {
				t.Fatal("Execute returned no error for a stopped attempt")
			}
			if attempts.HasObject(claimKey(t, in, 1)) {
				t.Error("a claim was written")
			}
			if records, _ := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem); len(records) != 0 {
				t.Errorf("records %v were written", records)
			}
			if n := len(runtime.creates()); n != 0 {
				t.Errorf("%d containers were created", n)
			}
			if seen := logAttempts(logs); len(seen) != 0 {
				t.Errorf("log chunks were written under %v", seen)
			}
			if dirs, _ := pinDirectories(d.WorkRoot); len(dirs) != 0 {
				t.Errorf("pin directories %v were made", dirs)
			}
		})
	}
}

// TestExecute_TheLatchWaitsForTheFirstAnswer: on a cluster that answers
// nothing, an uncached attempt waits at the latch, before its claim, until
// the heartbeat timeout fails it lost, with no container.
func TestExecute_TheLatchWaitsForTheFirstAnswer(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	runtime := &creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 0}}
	d := newAttemptDispatcher(t, runtime, attempts)
	d.heartbeaters = func(context.Context) *Heartbeater {
		return &Heartbeater{
			interval:   5 * time.Millisecond,
			send:       func(context.Context, HeartbeatProgress) error { return context.DeadlineExceeded },
			ackTimeout: 80 * time.Millisecond,
		}
	}
	in := baseExportInput("latch-lost")
	_, err := executeWithin(t, d, context.Background(), in)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("Execute = %v, want a failure", err)
	}
	var detail *orkstackv1.ActivityFailureDetail
	must.OKf(t, appErr.Details(&detail), "failure detail")
	if detail.GetReasonCode() != ReasonHeartbeatLost || appErr.NonRetryable() {
		t.Errorf("reason %q nonRetryable=%v, want a retryable %s", detail.GetReasonCode(), appErr.NonRetryable(), ReasonHeartbeatLost)
	}
	if attempts.HasObject(claimKey(t, in, 1)) || len(runtime.creates()) != 0 {
		t.Error("an attempt the cluster never answered claimed an ordinal or created a container")
	}
}

// TestExecute_ACommitIsAdoptedWithoutWaitingOnTheCluster: adoption never
// waits on the cluster (AGENTS.md §4.1): with no heartbeat answered, a
// committed result is still returned at once.
func TestExecute_ACommitIsAdoptedWithoutWaitingOnTheCluster(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	d := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	d.Commits = artifacts.NewMemoryCommitStore()
	in := baseExportInput("adopt-silent")
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/" + in.GetTaskId() + "/commit.json"
	_, err := d.Execute(context.Background(), in)
	must.OKf(t, err, "first Execute")

	d.heartbeaters = silentCluster
	result, err := executeWithin(t, d, context.Background(), in)
	must.OKf(t, err, "the adopting Execute")
	if !result.GetCommitAdopted() {
		t.Error("the commit was not adopted")
	}
}

// failingClaims refuses every claim.
type failingClaims struct {
	*artifacts.MemoryAttemptStore
	mu    sync.Mutex
	tries int
}

func (f *failingClaims) ClaimAttempt(context.Context, artifacts.AttemptRef, string) (artifacts.AttemptClaimed, error) {
	f.mu.Lock()
	f.tries++
	f.mu.Unlock()
	return artifacts.AttemptClaimed{}, errors.New("SlowDown: the store is shedding load")
}

// TestExecute_AClaimTheStoreRefusesIsATTEMPT_STORE_UNAVAILABLE: the claim is
// tried four times, and then the attempt fails with nothing made: no
// container, no record, no log.
func TestExecute_AClaimTheStoreRefusesIsATTEMPT_STORE_UNAVAILABLE(t *testing.T) {
	saved := claimBackoff
	claimBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { claimBackoff = saved })

	store := &failingClaims{MemoryAttemptStore: artifacts.NewMemoryAttemptStore()}
	runtime := &creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 0}}
	d := newAttemptDispatcher(t, runtime, store.MemoryAttemptStore)
	d.Claims = store
	logs := &recordingLogStore{}
	d.Logs = logs
	in := baseExportInput("store-down")

	_, err := d.Execute(context.Background(), in)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("Execute = %v, want a failure", err)
	}
	var detail *orkstackv1.ActivityFailureDetail
	must.OKf(t, appErr.Details(&detail), "failure detail")
	if detail.GetReasonCode() != ReasonAttemptStoreUnavailable ||
		appErr.Type() != CategoryInfrastructureError || appErr.NonRetryable() {
		t.Errorf("reason %q type %q nonRetryable=%v, want a retryable %s %s",
			detail.GetReasonCode(), appErr.Type(), appErr.NonRetryable(), CategoryInfrastructureError, ReasonAttemptStoreUnavailable)
	}
	if !strings.Contains(err.Error(), "SlowDown") {
		t.Errorf("message %q does not carry the store's error", err)
	}
	store.mu.Lock()
	tries := store.tries
	store.mu.Unlock()
	if tries != 4 {
		t.Errorf("the claim was tried %d times, want 4", tries)
	}
	if len(runtime.creates()) != 0 {
		t.Error("a container was created without a claim")
	}
	if records, _ := store.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem); len(records) != 0 {
		t.Errorf("records %v were written", records)
	}
	if seen := logAttempts(logs); len(seen) != 0 {
		t.Errorf("log chunks were written under %v", seen)
	}
}

// TestExecute_TheFirstBeatAfterTheClaimNamesTheOrdinal: a beat sent before
// the claim names none, and the loop names the ordinal from the claim on.
func TestExecute_TheFirstBeatAfterTheClaimNamesTheOrdinal(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	in := baseExportInput("beat-ordinal")
	if _, err := attempts.ClaimAttempt(context.Background(),
		artifacts.AttemptRef{RunID: in.GetRunId(), TaskID: in.GetTaskId()}, "elsewhere"); err != nil {
		t.Fatal(err)
	}
	runtime := newHeldRuntime()
	d := newAttemptDispatcher(t, runtime, attempts)
	var mu sync.Mutex
	var sent []HeartbeatProgress
	d.heartbeaters = func(context.Context) *Heartbeater {
		return &Heartbeater{
			interval: 2 * time.Millisecond,
			send: func(_ context.Context, p HeartbeatProgress) error {
				mu.Lock()
				sent = append(sent, p)
				mu.Unlock()
				return nil
			},
			ackTimeout: time.Minute,
		}
	}
	lastOrdinal := func() int32 {
		mu.Lock()
		defer mu.Unlock()
		return sent[len(sent)-1].Ordinal
	}
	go func() {
		<-runtime.started
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && lastOrdinal() == 0 {
			time.Sleep(2 * time.Millisecond)
		}
		runtime.signaled <- SIGTERM
	}()
	_, _ = executeWithin(t, d, context.Background(), in)
	mu.Lock()
	first := sent[0].Ordinal
	mu.Unlock()
	if first != 0 {
		t.Errorf("the first beat, before any claim, names ordinal %d", first)
	}
	if last := lastOrdinal(); last != 2 {
		t.Errorf("the last beat names ordinal %d, want 2 (1 was claimed elsewhere)", last)
	}
}

// cancellingPullRuntime stops the attempt while it pulls.
type cancellingPullRuntime struct {
	*phaseCapturingRuntime
	cancel func()
}

func (r *cancellingPullRuntime) Pull(ctx context.Context, ref string) (PullInfo, error) {
	r.cancel()
	return r.phaseCapturingRuntime.Pull(ctx, ref)
}

// TestExecute_NothingIsCreatedForAnAttemptStoppedAfterItsClaim: the run is
// terminated while the image is pulled; the attempt claimed its ordinal, so it
// releases it, and it creates no container.
func TestExecute_NothingIsCreatedForAnAttemptStoppedAfterItsClaim(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runtime := &creatingRuntime{ContainerRuntime: &cancellingPullRuntime{
		phaseCapturingRuntime: &phaseCapturingRuntime{exitCode: 0},
		cancel:                func() { cancel(serviceerror.NewNotFound("workflow execution already completed")) },
	}}
	d := newAttemptDispatcher(t, runtime, attempts)
	in := baseExportInput("stopped-in-pull")
	if _, err := executeWithin(t, d, ctx, in); err == nil {
		t.Fatal("Execute returned no error")
	}
	if len(runtime.creates()) != 0 {
		t.Error("a container was created for an attempt stopped during its pull")
	}
	if !attempts.HasObject(claimKey(t, in, 1)) || !attempts.HasObject(releasedKey(t, in, 1)) {
		t.Error("the attempt did not claim and then release its ordinal")
	}
}

// TestReapReleasesTheAttemptsOfTheContainersItRemoved: the boot reap writes
// the release marker of every attempt whose container it removed, and none
// for a container that stayed or whose labels name no attempt.
func TestReapReleasesTheAttemptsOfTheContainersItRemoved(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	a := &Activities{dispatcher: &OCIDispatcher{Claims: attempts, OrkerID: "orker-a"}}
	own := parseOwnContainers("c1\tork-r1\tbuild\t\t2\n" +
		"c2\tork-r1\teach\t7\t1\n" +
		"c3\tork-r1\tstuck\t\t1\n" +
		"c4\tork-r1\told\t\tnot-a-number\n")
	a.releaseReaped(context.Background(), own, &ReapIncompleteError{Containers: []ContainerID{"c3"}})
	key := func(task string, item artifacts.Item, n int32) string {
		k, err := artifacts.AttemptReleasedKey(artifacts.AttemptRef{RunID: "ork-r1", TaskID: task, Item: item, Attempt: n})
		must.OKf(t, err, "released key")
		return k
	}
	if !attempts.HasObject(key("build", artifacts.NoItem, 2)) || !attempts.HasObject(key("each", artifacts.ItemAt(7), 1)) {
		t.Error("a removed container's attempt was not released")
	}
	if attempts.HasObject(key("stuck", artifacts.NoItem, 1)) {
		t.Error("the attempt of a container that stayed was released")
	}

	// A reap that could not even list releases nothing.
	other := artifacts.NewMemoryAttemptStore()
	b := &Activities{dispatcher: &OCIDispatcher{Claims: other, OrkerID: "orker-a"}}
	b.releaseReaped(context.Background(), own, errors.New("daemon unreachable"))
	if other.HasObject(key("build", artifacts.NoItem, 2)) {
		t.Error("an attempt was released by a reap that removed nothing")
	}
}

// TestContainerLabelsNameTheAttemptsScope: an item's container carries its
// index, which the boot reap releases it by.
func TestContainerLabelsNameTheAttemptsScope(t *testing.T) {
	in := baseExportInput("labels")
	in.Each = &orkstackv1.EachContext{Index: 12, Count: 20}
	labels := containerLabels(in, 3, "orker-a")
	want := map[string]string{"ork.run": in.GetRunId(), "ork.task": "labels", "ork.item": "12", "ork.attempt": "3", "ork.orker": "orker-a"}
	for k, v := range want {
		if labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, labels[k], v)
		}
	}
	if _, ok := containerLabels(baseExportInput("plain"), 1, "o")["ork.item"]; ok {
		t.Error("a task's own container carries ork.item")
	}
}

// TestExecute_ACacheHitRecordsUnderItsOwnOrdinal: a hit runs no container,
// claims an ordinal best-effort and records under it; this run's commit
// carries it.
func TestExecute_ACacheHitRecordsUnderItsOwnOrdinal(t *testing.T) {
	h := newCacheHarness(t)
	h.dispatcher.Claims = h.attempts
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	second := cachedTaskInput(t, "ork-run-2")
	result, err := h.dispatcher.Execute(context.Background(), second)
	must.OKf(t, err, "Execute (hit)")
	if result.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || result.GetAttempt() != 1 {
		t.Fatalf("result = %v, want a HIT under ordinal 1", result)
	}
	if !h.attempts.HasObject(claimKey(t, second, 1)) || !h.attempts.HasObject(releasedKey(t, second, 1)) {
		t.Error("the hit did not claim and release ordinal 1")
	}
	onlyAttemptOf(t, h.attempts, second)
}

// TestExecute_ACacheHitWithoutAnOrdinalKeepsTemporalsCount: the store refuses
// the hit's best-effort claim; the hit still succeeds, its commit and result
// carry Temporal's attempt rather than 0, and nothing is recorded.
func TestExecute_ACacheHitWithoutAnOrdinalKeepsTemporalsCount(t *testing.T) {
	h := newCacheHarness(t)
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	refusing := &failingClaims{MemoryAttemptStore: h.attempts}
	h.dispatcher.Claims = refusing
	second := cachedTaskInput(t, "ork-run-2")
	result, err := h.dispatcher.Execute(context.Background(), second)
	must.OKf(t, err, "Execute (hit)")
	if result.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || result.GetAttempt() != 1 {
		t.Fatalf("result = %v, want a HIT naming Temporal's attempt 1", result)
	}
	record, err := h.commits.Get(context.Background(), second.GetCommitUri())
	must.OKf(t, err, "this run's commit")
	if record.Commit.GetAttempt() != 1 {
		t.Errorf("the hit's commit names attempt %d, want 1", record.Commit.GetAttempt())
	}
	if records, _ := h.attempts.ListAttempts(context.Background(), second.GetRunId(), second.GetTaskId(), artifacts.NoItem); len(records) != 0 {
		t.Errorf("records %v written without an ordinal", records)
	}
}

// TestExecute_ACachedMissWaitsForTheFirstAnswerBeforeItsClaim: a miss on a
// cluster that answers nothing claims nothing and pulls nothing.
func TestExecute_ACachedMissWaitsForTheFirstAnswerBeforeItsClaim(t *testing.T) {
	h := newCacheHarness(t)
	h.dispatcher.Claims = h.attempts
	h.dispatcher.heartbeaters = func(context.Context) *Heartbeater {
		return &Heartbeater{
			interval:   5 * time.Millisecond,
			send:       func(context.Context, HeartbeatProgress) error { return context.DeadlineExceeded },
			ackTimeout: 80 * time.Millisecond,
		}
	}
	in := cachedTaskInput(t, "ork-run-1")
	if _, err := executeWithin(t, h.dispatcher, context.Background(), in); err == nil {
		t.Fatal("Execute returned no error")
	}
	if pulls, creates := h.runtime.counts(); pulls != 0 || creates != 0 {
		t.Errorf("pulled %d, created %d before the cluster answered", pulls, creates)
	}
	if h.attempts.HasObject(claimKey(t, in, 1)) {
		t.Error("a claim was written before the cluster answered")
	}
}

// failingRemoveRuntime cannot remove its containers.
type failingRemoveRuntime struct{ *phaseCapturingRuntime }

func (failingRemoveRuntime) Remove(context.Context, ContainerID) error {
	return errors.New("removal in progress")
}

// TestExecute_AContainerThatStaysKeepsItsOrdinalClaimed: a container the Orker
// could not remove may still run, so its attempt is not released; the boot
// reap releases it once it removes the container.
func TestExecute_AContainerThatStaysKeepsItsOrdinalClaimed(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	d := newAttemptDispatcher(t, failingRemoveRuntime{&phaseCapturingRuntime{exitCode: 0}}, attempts)
	in := baseExportInput("stays")
	_, _ = d.Execute(context.Background(), in)
	if !attempts.HasObject(claimKey(t, in, 1)) {
		t.Fatal("no claim")
	}
	if attempts.HasObject(releasedKey(t, in, 1)) {
		t.Error("the attempt was released while its container stays")
	}
}

// TestAttemptOrdinal_LinesWaitForTheClaimWithinABound: what an attempt logs
// before its claim is written first under the ordinal, up to a bound, and
// the log says how many lines were dropped past it.
func TestAttemptOrdinal_LinesWaitForTheClaimWithinABound(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	logs := &recordingLogStore{}
	d := &OCIDispatcher{Claims: attempts, Logs: logs, OrkerID: "o"}
	o := d.newAttemptOrdinal(context.Background(), baseExportInput("pending"), 1)
	for i := range maxPendingLogLines + 5 {
		o.Write("line " + strconv.Itoa(i))
	}
	if f := o.Claim(context.Background()); f != nil {
		t.Fatal(f)
	}
	o.Write("after the claim")
	o.Close()
	logs.mu.Lock()
	var all strings.Builder
	for _, chunk := range logs.chunks {
		all.Write(chunk.Data)
	}
	logs.mu.Unlock()
	text := all.String()
	for _, want := range []string{"line 0\n", "line " + strconv.Itoa(maxPendingLogLines-1) + "\n", "5 earlier log lines", "after the claim"} {
		if !strings.Contains(text, want) {
			t.Errorf("the log lacks %q", want)
		}
	}
	if strings.Contains(text, "line "+strconv.Itoa(maxPendingLogLines)+"\n") {
		t.Error("a line past the bound was kept")
	}
	if strings.Index(text, "line 0") > strings.Index(text, "after the claim") {
		t.Error("the waiting lines were not written first")
	}
}

func TestDescribeOrker_ReportsAttemptOrdinalsSupported(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OK(t, err)
	t.Cleanup(func() { _ = store.Close() })
	activities := NewActivitiesWithOptions(store, Options{Runtime: &reapRecorder{}, WorkRoot: t.TempDir()})
	result, err := activities.DescribeOrker(context.Background())
	must.OKf(t, err, "DescribeOrker")
	if !result.GetAttemptOrdinalsSupported() {
		t.Fatal("this build claims attempt ordinals and must say so")
	}
}

// legacyCounter counts what AttemptRecordObserver hears.
type legacyCounter struct {
	mu            sync.Mutex
	atClaim, kept int
}

func (c *legacyCounter) LegacyRecordsAtClaim(n int) { c.mu.Lock(); c.atClaim += n; c.mu.Unlock() }
func (c *legacyCounter) LegacyRecordKept()          { c.mu.Lock(); c.kept++; c.mu.Unlock() }

// TestExecute_AnEarlierReleasesRecordsAreCountedAndKept: a record written
// under Temporal's count by an Orker of an earlier release is passed over at
// the claim, counted, and never overwritten.
func TestExecute_AnEarlierReleasesRecordsAreCountedAndKept(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	in := baseExportInput("legacy")
	for _, n := range []int32{1, 2} {
		must.OK(t, attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion, RunId: in.GetRunId(), TaskId: in.GetTaskId(),
			Attempt: n, Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
		}))
	}
	d := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	counter := &legacyCounter{}
	d.AttemptRecords = counter
	result, err := d.Execute(context.Background(), in)
	must.OKf(t, err, "Execute")
	if result.GetAttempt() != 3 {
		t.Errorf("attempt %d, want 3, past the earlier release's records", result.GetAttempt())
	}
	if counter.atClaim != 2 {
		t.Errorf("counted %d legacy records at the claim, want 2", counter.atClaim)
	}
	records, _ := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	if len(records) != 3 || records[0].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
		t.Errorf("records = %v, want the two earlier ones kept and a third", records)
	}
}
