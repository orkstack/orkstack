package orker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// countingCommits counts every request that reaches the commit store, and
// the uris read, so a test can say the guard refused before the store was
// asked.
type countingCommits struct {
	artifacts.CommitStore
	calls atomic.Int32
	mu    sync.Mutex
	read  []string
}

func (c *countingCommits) Get(ctx context.Context, uri string) (artifacts.CommitRecord, error) {
	c.calls.Add(1)
	c.mu.Lock()
	c.read = append(c.read, uri)
	c.mu.Unlock()
	return c.CommitStore.Get(ctx, uri)
}

func (c *countingCommits) Create(ctx context.Context, uri string, commit *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
	c.calls.Add(1)
	return c.CommitStore.Create(ctx, uri, commit)
}

func serving(t *testing.T, names ...string) artifacts.Namespaces {
	t.Helper()
	served, err := artifacts.ParseNamespaces(names)
	must.OK(t, err)
	return served
}

func commitURIOf(t *testing.T, ns, runID, taskID string) string {
	t.Helper()
	key, err := artifacts.CommitKey(ns, runID, taskID)
	must.OK(t, err)
	return cacheRoot + "/" + key
}

// guardedInput is one attempt of task "work" of runID in namespace ns.
func guardedInput(t *testing.T, ns, runID string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	in := baseExportInput("work")
	in.Namespace = ns
	in.RunId = runID
	in.IdempotencyKey = artifacts.ResolveNamespace(ns) + "/" + runID + "/work"
	in.CommitUri = commitURIOf(t, ns, runID, "work")
	return in
}

// TestExecute_RefusesANamespaceItDoesNotServe: an Orker that lists the
// namespaces it serves refuses a task of any other, and a task whose input
// names a commit of another namespace, with NAMESPACE_NOT_SERVED, a contract
// violation, before it writes a log line or an attempt record, claims an
// ordinal, asks the commit store anything or starts a container. A commit
// already there is not read, let alone adopted (AGENTS.md §5).
func TestExecute_RefusesANamespaceItDoesNotServe(t *testing.T) {
	for _, tc := range []struct {
		name string
		// in is the attempt; committed, when set, is run first on an Orker
		// that serves every namespace, so its commit is in the store.
		in        func(t *testing.T) *orkstackv1.ExecuteTaskInput
		committed bool
	}{
		{
			name:      "a namespace it does not serve, with a commit already there",
			in:        func(t *testing.T) *orkstackv1.ExecuteTaskInput { return guardedInput(t, "team-b", "ork-ns-committed") },
			committed: true,
		},
		{
			name: "the default namespace when the list does not name it",
			in:   func(t *testing.T) *orkstackv1.ExecuteTaskInput { return guardedInput(t, "", "ork-ns-default") },
		},
		{
			name: "its own commit uri under another namespace",
			in: func(t *testing.T) *orkstackv1.ExecuteTaskInput {
				in := guardedInput(t, "team-a", "ork-ns-own-uri")
				in.CommitUri = commitURIOf(t, "team-b", "ork-ns-own-uri", "work")
				return in
			},
		},
		{
			name: "its own commit uri under another store root",
			in: func(t *testing.T) *orkstackv1.ExecuteTaskInput {
				in := guardedInput(t, "team-a", "ork-ns-root")
				key, err := artifacts.CommitKey("team-a", "ork-ns-root", "work")
				must.OK(t, err)
				in.CommitUri = "mem://elsewhere/" + key
				return in
			},
		},
		{
			name: "an upstream's commit under another namespace",
			in: func(t *testing.T) *orkstackv1.ExecuteTaskInput {
				in := guardedInput(t, "team-a", "ork-ns-upstream")
				in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "build", CommitUri: commitURIOf(t, "team-b", "ork-ns-upstream", "build")}}
				return in
			},
		},
		{
			name: "a fan-out upstream whose items lie under another store root",
			in: func(t *testing.T) *orkstackv1.ExecuteTaskInput {
				in := guardedInput(t, "team-a", "ork-ns-each")
				in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "build", Each: &orkstackv1.EachUpstream{Count: 2, CommitRoot: "mem://elsewhere"}}}
				return in
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCacheHarness(t)
			commits := &countingCommits{CommitStore: h.commits}
			h.dispatcher.Commits = commits
			in := tc.in(t)
			if tc.committed {
				_, err := h.dispatcher.Execute(context.Background(), in)
				must.OKf(t, err, "the first attempt, on an Orker that serves every namespace")
			}
			attemptsBefore, _ := h.attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
			pulls, creates := h.runtime.counts()
			h.linesMu.Lock()
			linesBefore := len(h.lines)
			h.linesMu.Unlock()
			commits.calls.Store(0)

			h.dispatcher.Namespaces = serving(t, "team-a")
			h.dispatcher.CommitRoot = cacheRoot
			_, err := h.dispatcher.Execute(context.Background(), in)

			failure := failureOf(err)
			if failure == nil || failure.Reason != ReasonNamespaceNotServed {
				t.Fatalf("Execute = %v, want %s", err, ReasonNamespaceNotServed)
			}
			if got := failure.Category(); got != CategoryContractViolation {
				t.Fatalf("category = %s, want %s: another Orker of the pool serves the same list", got, CategoryContractViolation)
			}
			if n := commits.calls.Load(); n != 0 {
				t.Fatalf("the commit store was asked %d times; the guard refuses before any read", n)
			}
			if p, c := h.runtime.counts(); p != pulls || c != creates {
				t.Fatalf("the refused attempt pulled (%d→%d) or created (%d→%d)", pulls, p, creates, c)
			}
			if after, _ := h.attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem); len(after) != len(attemptsBefore) {
				t.Fatalf("attempt records %d → %d; the refused attempt writes none", len(attemptsBefore), len(after))
			}
			h.linesMu.Lock()
			defer h.linesMu.Unlock()
			if len(h.lines) != linesBefore {
				t.Fatalf("the refused attempt wrote log lines %q", h.lines[linesBefore:])
			}
		})
	}
}

// TestExecute_ServesItsOwnNamespaceAndEveryOneWhenUnset is the positive
// control: an Orker that lists a namespace runs that namespace's task, and an
// Orker with no list runs any.
func TestExecute_ServesItsOwnNamespaceAndEveryOneWhenUnset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		served []string
		ns     string
	}{
		{"a namespace the list names", []string{"team-a"}, "team-a"},
		{"the default namespace when the list names it", []string{"default"}, ""},
		{"any namespace when the list is unset", nil, "team-z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCacheHarness(t)
			h.dispatcher.Namespaces = serving(t, tc.served...)
			h.dispatcher.CommitRoot = cacheRoot
			h.dispatcher.Commits = artifacts.ScopeCommits(h.commits, cacheRoot, h.dispatcher.Namespaces)
			result, err := h.dispatcher.Execute(context.Background(), guardedInput(t, tc.ns, "ork-ns-served"))
			must.OKf(t, err, "a task of a namespace this Orker serves")
			if result.GetCommit() == nil {
				t.Fatalf("result = %v, want a committed result", result)
			}
		})
	}
}

// TestFailCause_AStoreRefusalForANamespaceIsNamespaceNotServed: whatever
// step's read or write the store guard refused, the failure is the guard's,
// a contract violation, never the step's retryable store failure.
func TestFailCause_AStoreRefusalForANamespaceIsNamespaceNotServed(t *testing.T) {
	served := serving(t, "team-a")
	_, err := artifacts.ScopeCommits(artifacts.NewMemoryCommitStore(), cacheRoot, served).
		Get(context.Background(), commitURIOf(t, "team-b", "ork-ns-cause", "work"))
	failure := FailCause(ReasonCommitReadFailed, err, "read the commit: %v", err)
	if failure.Reason != ReasonNamespaceNotServed || failure.Category() != CategoryContractViolation {
		t.Fatalf("failure = %s (%s), want %s, a contract violation", failure.Reason, failure.Category(), ReasonNamespaceNotServed)
	}
}

// TestExecute_AMemoWhoseSourceIsOfAnotherNamespaceIsAMiss: a memo of a
// served namespace that points at a commit of a namespace this Orker does not
// serve is a miss, never a failure (AGENTS.md §4.1), and the commit it points
// at is never read: the task runs.
func TestExecute_AMemoWhoseSourceIsOfAnotherNamespaceIsAMiss(t *testing.T) {
	h := newCacheHarness(t)
	served := serving(t, "default")
	reads := &countingCommits{CommitStore: h.commits}
	h.dispatcher.Namespaces = served
	h.dispatcher.CommitRoot = cacheRoot
	h.dispatcher.Commits = artifacts.ScopeCommits(reads, cacheRoot, served)
	h.dispatcher.Memos = artifacts.ScopeMemos(h.memos, served)

	first := cachedTaskInput(t, "ork-ns-memo-1")
	produced := h.produce(t, first)
	memo, _ := h.memoFor(t, produced, first)
	foreign := commitURIOf(t, "team-b", "ork-ns-memo-1", "featurize")
	memo.Source.CommitUri = foreign
	must.OKf(t, h.memos.MemoryMemoStore.PutMemo(context.Background(), memo), "point the memo at another namespace's commit")
	reads.calls.Store(0)

	second := cachedTaskInput(t, "ork-ns-memo-2")
	result, err := h.dispatcher.Execute(context.Background(), second)
	must.OKf(t, err, "a memo whose source is out of scope is a miss, never a failure")
	if got := result.GetCache().GetOutcome(); got != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS {
		t.Fatalf("cache outcome = %v, want a MISS", got)
	}
	if result.GetCommit() == nil {
		t.Fatalf("result = %v, want the task's own fresh commit", result)
	}
	// The run's own commit check and its fresh commit reach the store; the
	// other namespace's commit never does.
	reads.mu.Lock()
	defer reads.mu.Unlock()
	for _, uri := range reads.read {
		if uri == foreign {
			t.Fatalf("the store was asked for %s, another namespace's commit", uri)
		}
	}
	if n := reads.calls.Load(); n != 2 {
		t.Fatalf("the commit store was asked %d times, want 2: the task's own check and write", n)
	}
}

func TestNamespacesFromEnv_ReadsTheListAndRefusesABadName(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  []string
		err   bool
	}{
		{"", nil, false},
		{" payments , billing,payments ", []string{"billing", "payments"}, false},
		{"payments,,", []string{"payments"}, false},
		{"Payments", nil, true},
		{"payments,../x", nil, true},
		{" , ", nil, true},
	} {
		t.Setenv(NamespacesVar, tc.value)
		got, err := NamespacesFromEnv()
		if tc.err {
			if !errors.Is(err, artifacts.ErrInvalidNamespace) || !strings.Contains(err.Error(), NamespacesVar) {
				t.Errorf("%q: error %v, want one naming %s", tc.value, err, NamespacesVar)
			}
			continue
		}
		must.OKf(t, err, "%q", tc.value)
		if !slices.Equal(got.Names(), tc.want) {
			t.Errorf("%q: names %v, want %v", tc.value, got.Names(), tc.want)
		}
	}
}

// TestNewActivities_ScopesItsStoresToItsNamespaces: an Orker built with a
// list reads and writes no commit or memo of another namespace through any
// path, not only through the dispatch guard.
func TestNewActivities_ScopesItsStoresToItsNamespaces(t *testing.T) {
	activities := NewActivitiesWithOptions(nil, Options{
		WorkRoot:   t.TempDir(),
		Commits:    artifacts.NewMemoryCommitStore(),
		Memos:      artifacts.NewMemoryMemoStore(),
		Namespaces: serving(t, "team-a"),
		CommitRoot: cacheRoot,
	})
	d := activities.Dispatcher()
	if _, err := d.Commits.Get(context.Background(), commitURIOf(t, "team-b", "ork-ns-wired", "work")); !errors.Is(err, artifacts.ErrNamespaceNotServed) {
		t.Fatalf("commit Get of another namespace: %v, want ErrNamespaceNotServed", err)
	}
	ref := artifacts.MemoRef{MemoScopeRef: artifacts.MemoScopeRef{Namespace: "team-b", NamespaceScoped: true, TaskID: "work"}, Key: strings.Repeat("ab", 32)}
	if _, err := d.Memos.GetMemo(context.Background(), ref); !errors.Is(err, artifacts.ErrNamespaceNotServed) {
		t.Fatalf("memo Get of another namespace: %v, want ErrNamespaceNotServed", err)
	}
	if d.Namespaces.Every() || d.CommitRoot != cacheRoot {
		t.Fatalf("dispatcher namespaces %v root %q, want the list and the root", d.Namespaces.Names(), d.CommitRoot)
	}
}

// TestExecute_AMemoWhoseSourceIsAnotherServedNamespacesIsAMiss: on an Orker
// that serves two namespaces, a memo of one whose source lies under the
// other's key is a miss before the source is read, even when the commit
// there claims the memo's namespace in its body.
func TestExecute_AMemoWhoseSourceIsAnotherServedNamespacesIsAMiss(t *testing.T) {
	h := newCacheHarness(t)
	served := serving(t, "default", "team-b")
	reads := &countingCommits{CommitStore: h.commits}
	h.dispatcher.Namespaces = served
	h.dispatcher.CommitRoot = cacheRoot
	h.dispatcher.Commits = artifacts.ScopeCommits(reads, cacheRoot, served)
	h.dispatcher.Memos = artifacts.ScopeMemos(h.memos, served)

	first := cachedTaskInput(t, "ork-ns-memo-b1")
	produced := h.produce(t, first)
	memo, _ := h.memoFor(t, produced, first)
	// A copy of the real source commit, planted under team-b's key: its body
	// still says namespace "default", run ork-ns-memo-b1.
	original, err := h.commits.Get(context.Background(), memo.GetSource().GetCommitUri())
	must.OK(t, err)
	planted := commitURIOf(t, "team-b", "ork-ns-memo-b1", "featurize")
	_, err = h.commits.MemoryCommitStore.Create(context.Background(), planted, original.Commit)
	must.OK(t, err)
	memo.Source.CommitUri = planted
	must.OK(t, h.memos.MemoryMemoStore.PutMemo(context.Background(), memo))
	reads.calls.Store(0)

	result, err := h.dispatcher.Execute(context.Background(), cachedTaskInput(t, "ork-ns-memo-b2"))
	must.OKf(t, err, "a memo whose source is another namespace's is a miss")
	if c := result.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS || c.GetMissReason() != MemoMissSourceMismatch {
		t.Fatalf("cache = %v, want a MISS %s", c, MemoMissSourceMismatch)
	}
	reads.mu.Lock()
	defer reads.mu.Unlock()
	for _, uri := range reads.read {
		if uri == planted {
			t.Fatalf("the store was asked for the planted commit %s", uri)
		}
	}
}

// TestExecute_AGuardMemoMissSaysSoInAFixedSentence: every memo miss the
// namespace guard causes, whether the Orker's own check before a read or a
// scoped store's refusal of one, is a mismatch, never an unreadable store, and
// its detail is one fixed sentence that names the guard and neither the
// refused key nor the store's own words.
func TestExecute_AGuardMemoMissSaysSoInAFixedSentence(t *testing.T) {
	const sourceDetail = `the memo's source commit is not a commit of namespace "default", and this Orker reads no commit outside the namespaces it serves (ORK_ORKER_NAMESPACES)`
	const memoDetail = `this Orker reads no memo outside the namespaces it serves (ORK_ORKER_NAMESPACES)`
	tests := []struct {
		name       string
		wire       func(h *cacheHarness)
		wantReason string
		wantDetail string
	}{
		{
			name: "the Orker's own check refuses the source's key",
			wire: func(h *cacheHarness) {
				served := serving(t, "default")
				h.dispatcher.Namespaces = served
				h.dispatcher.CommitRoot = cacheRoot
				h.dispatcher.Commits = artifacts.ScopeCommits(h.commits, cacheRoot, served)
				h.dispatcher.Memos = artifacts.ScopeMemos(h.memos, served)
			},
			wantReason: MemoMissSourceMismatch,
			wantDetail: sourceDetail,
		},
		{
			name: "the scoped commit store refuses the source read",
			wire: func(h *cacheHarness) {
				h.dispatcher.Commits = artifacts.ScopeCommits(h.commits, cacheRoot, serving(t, "default"))
			},
			wantReason: MemoMissSourceMismatch,
			wantDetail: sourceDetail,
		},
		{
			name: "the scoped memo store refuses the memo read",
			wire: func(h *cacheHarness) {
				h.dispatcher.Memos = artifacts.ScopeMemos(h.memos, serving(t, "team-b"))
			},
			wantReason: MemoMissMismatch,
			wantDetail: memoDetail,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newCacheHarness(t)
			first := cachedTaskInput(t, "ork-ns-detail-1")
			produced := h.produce(t, first)
			memo, _ := h.memoFor(t, produced, first)
			foreign := commitURIOf(t, "team-b", "ork-ns-detail-1", "featurize")
			memo.Source.CommitUri = foreign
			must.OK(t, h.memos.MemoryMemoStore.PutMemo(context.Background(), memo))
			tt.wire(h)

			result, err := h.dispatcher.Execute(context.Background(), cachedTaskInput(t, "ork-ns-detail-2"))
			must.OKf(t, err, "a guard memo miss is a miss, never a failure")
			c := result.GetCache()
			if c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS || c.GetMissReason() != tt.wantReason {
				t.Fatalf("cache = %v, want a MISS %s", c, tt.wantReason)
			}
			if c.GetMissDetail() != tt.wantDetail {
				t.Fatalf("miss detail = %q, want %q", c.GetMissDetail(), tt.wantDetail)
			}
		})
	}
}
