package orker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordedRunInput is an attempt of a task of a run whose results the
// engine records (its input carries the run's identity context), whose
// commit uri is a real commit key under a store root.
func recordedRunInput(t *testing.T, taskID string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	in := baseExportInput(taskID)
	key, err := artifacts.CommitKey(in.GetNamespace(), in.GetRunId(), taskID)
	must.OK(t, err)
	in.CommitUri = "mem://store/" + key
	in.Identity = &orkstackv1.IdentityContext{AdmissionSha256: strings.Repeat("a", 64), DigestVersion: 1, CommitWaitSeconds: 900}
	return in
}

// commitOf is the commit an attempt of in made, as a store holds it.
func commitOf(in *orkstackv1.ExecuteTaskInput) *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: in.GetNamespace(), RunId: in.GetRunId(),
		TaskId: in.GetTaskId(), OrkflowName: in.GetOrkflowName(), OrkflowVersion: in.GetOrkflowVersion(),
		OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(), IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(),
		Attempt: 1, OrkerId: "orker-1"}
}

func plant(t *testing.T, s *artifacts.MemoryCommitStore, uri string, c *orkstackv1.ActivityCommit) {
	t.Helper()
	doc, err := artifacts.MarshalCommit(c)
	must.OK(t, err)
	s.PutRaw(uri, doc)
}

// TestAdopt_ARecordedRunAdoptsOnlyWhatAnEngineRecorded: for a run whose
// results the engine records, the commit check reads the commit key's
// attested twin and nothing under metadata/commits/: a commit planted
// there, whoever wrote it, is ignored; an engine's record is adopted under
// its own uri; a record with no recording is not an engine's and is
// refused.
func TestAdopt_ARecordedRunAdoptsOnlyWhatAnEngineRecorded(t *testing.T) {
	ctx := context.Background()
	t.Run("a commit an Orker wrote is ignored", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		store := artifacts.NewMemoryCommitStore()
		plant(t, store, in.GetCommitUri(), commitOf(in))
		d := &OCIDispatcher{Commits: store}
		got, failure := d.adopt(ctx, in, 2)
		if got != nil || failure != nil {
			t.Fatalf("adopt = %v, %v; want nothing adopted", got, failure)
		}
	})
	t.Run("an engine's record is adopted", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		store := artifacts.NewMemoryCommitStore()
		attested, err := artifacts.AttestedCommitURI(in.GetCommitUri())
		must.OK(t, err)
		rec := commitOf(in)
		rec.Recorded = &orkstackv1.CommitRecording{Pool: "main", OrkerId: "orker-1", AdmissionSha256: in.GetIdentity().GetAdmissionSha256()}
		plant(t, store, attested, rec)
		d := &OCIDispatcher{Commits: store}
		got, failure := d.adopt(ctx, in, 2)
		if failure != nil || got == nil || !got.GetCommitAdopted() || got.GetCommit().GetUri() != attested {
			t.Fatalf("adopt = %v, %v; want the record at %s", got, failure, attested)
		}
	})
	t.Run("a record with no recording is refused", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		store := artifacts.NewMemoryCommitStore()
		attested, err := artifacts.AttestedCommitURI(in.GetCommitUri())
		must.OK(t, err)
		plant(t, store, attested, commitOf(in))
		d := &OCIDispatcher{Commits: store}
		if _, failure := d.adopt(ctx, in, 2); failure == nil || failure.Reason != ReasonCommitIdentity {
			t.Fatalf("adopt = %v; want %s", failure, ReasonCommitIdentity)
		}
	})
	t.Run("a commit uri that names no commit key", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		in.CommitUri = "mem://commits/elsewhere/commit.json"
		d := &OCIDispatcher{Commits: artifacts.NewMemoryCommitStore()}
		if _, failure := d.adopt(ctx, in, 2); failure == nil || failure.Reason != ReasonCommitIdentity {
			t.Fatalf("adopt = %v; want %s", failure, ReasonCommitIdentity)
		}
	})
}

func commitRefusal(code connect.Code, msg string) error {
	return connect.NewError(code, errors.New(msg))
}

// TestMapCommitAnswer: the Orker's table of the engine's answers to a
// commit (workload-identity §3.4), one case a row: an outage is asked
// again, a refusal of this Orker ends a preflight at once and is asked
// again at a commit, what the Orker sent wrong ends at once, and what is
// unknown is an outage.
func TestMapCommitAnswer(t *testing.T) {
	skew := connect.NewError(connect.CodeFailedPrecondition, errors.New(identity.ReasonTaskNotAsAdmitted+": the task differs"))
	skew.Meta().Set(engineBuildHeader, "engine-2")
	same := connect.NewError(connect.CodeFailedPrecondition, errors.New(identity.ReasonTaskNotAsAdmitted+": the task differs"))
	same.Meta().Set(engineBuildHeader, "orker-1")
	for _, tc := range []struct {
		name      string
		err       error
		preflight bool
		reason    string
		then      commitThen
		refused   bool
	}{
		{"a transport error", errors.New("connection refused"), false, ReasonCommitEngineUnavailable, commitAskAgain, false},
		{"unavailable", commitRefusal(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE: ask again"), false, ReasonCommitEngineUnavailable, commitAskAgain, false},
		{"resource exhausted", commitRefusal(connect.CodeResourceExhausted, "IDENTITY_UNAVAILABLE: too many"), false, ReasonCommitEngineUnavailable, commitAskAgain, false},
		{"unimplemented, an older engine", commitRefusal(connect.CodeUnimplemented, "no such procedure"), true, ReasonCommitEngineUnavailable, commitAskAgain, false},
		{"an unmapped code", commitRefusal(connect.CodeInternal, "boom"), false, ReasonCommitEngineUnavailable, commitAskAgain, false},
		{"permission denied at a preflight", commitRefusal(connect.CodePermissionDenied, "may not commit"), true, ReasonCommitEngineRefused, commitEnd, true},
		{"permission denied at a commit", commitRefusal(connect.CodePermissionDenied, "may not commit"), false, ReasonCommitEngineRefused, commitAskAgain, true},
		{"unauthenticated at a commit", commitRefusal(connect.CodeUnauthenticated, "who"), false, ReasonCommitEngineRefused, commitAskAgain, true},
		{"the engine refused at a preflight", commitRefusal(connect.CodeFailedPrecondition, identity.ReasonEngineRefused+": no"), true, ReasonCommitEngineRefused, commitEnd, true},
		{"invalid argument", commitRefusal(connect.CodeInvalidArgument, "commit_key must be"), false, ReasonCommitEngineRefused, commitEnd, true},
		{"not held", commitRefusal(connect.CodeFailedPrecondition, identity.ReasonAttemptNotHeld+": no"), false, identity.ReasonAttemptNotHeld, commitAskAgain, false},
		{"not admitted", commitRefusal(connect.CodeFailedPrecondition, identity.ReasonNotAdmitted+": no"), false, identity.ReasonNotAdmitted, commitOnceMore, false},
		{"a required result not recorded", commitRefusal(connect.CodeFailedPrecondition, ReasonRequiredResultNotRecorded+": approve"), true, ReasonRequiredResultNotRecorded, commitOnceMore, false},
		{"the task differs, one build", same, false, identity.ReasonTaskNotAsAdmitted, commitEnd, false},
		{"the task differs, two builds", skew, false, ReasonIdentitySpecSkew, commitEnd, false},
		{"a mismatch", commitRefusal(connect.CodeFailedPrecondition, ReasonCommitIdentity+": no"), false, ReasonCommitIdentity, commitEnd, false},
		{"an unknown refusal", commitRefusal(connect.CodeFailedPrecondition, "SOMETHING_NEW: no"), false, ReasonCommitEngineUnavailable, commitAskAgain, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := mapCommitAnswer(tc.err, tc.preflight, "orker-1")
			if got.Reason != tc.reason || got.Then != tc.then || got.Refused != tc.refused {
				t.Fatalf("answer = %+v; want %s, then %d, refused %v", got, tc.reason, tc.then, tc.refused)
			}
		})
	}
}

// scriptedEngine answers CommitResult from a script, one entry a call;
// past it, every call is answered with the last entry.
type scriptedEngine struct {
	mu     sync.Mutex
	script []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error)
	calls  []*orkstackv1.CommitResultRequest
}

func (e *scriptedEngine) commit(_ context.Context, req *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, req)
	next := e.script[min(len(e.calls), len(e.script))-1]
	return next(req)
}

func engineAnswers(err error) func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
	return func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) { return nil, err }
}

func recordedAt(uri string) func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
	return func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
		return &orkstackv1.CommitResultResponse{Created: true, Commit: &orkstackv1.CommitRef{Uri: uri}}, nil
	}
}

func recordingDispatcher(engine *scriptedEngine, store *artifacts.MemoryCommitStore) (*OCIDispatcher, *fakeIdentityClock) {
	clock := &fakeIdentityClock{wall: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
	d := &OCIDispatcher{Commits: store, BuildID: "orker-1", recordClockFor: clock, recordDrawFor: func() float64 { return 1 }}
	d.SetResultCommit(engine.commit)
	return d, clock
}

// TestCommitThroughEngine: the attempt's result is handed to the engine
// and the engine's answer is the commit; an outage is asked again inside
// the run's wait, after which the record's key is looked at, and only with
// nothing there does the attempt fail COMMIT_ENGINE_UNAVAILABLE; a refusal
// that would be given again ends at once, and IDENTITY_NOT_ADMITTED is
// asked once more.
func TestCommitThroughEngine(t *testing.T) {
	ctx := context.Background()
	down := commitRefusal(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE: ask again")
	t.Run("recorded", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){engineAnswers(down), recordedAt("s3://x")}}
		d, _ := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		resp, f := d.commitThroughEngine(ctx, in, 1, commitOf(in))
		if f != nil || resp.GetCommit().GetUri() != "s3://x" || len(engine.calls) != 2 {
			t.Fatalf("commit = %v, %v after %d calls", resp, f, len(engine.calls))
		}
		got := engine.calls[0]
		if got.GetPreflight() || got.GetCommit() == nil || got.GetCommitKey() != strings.TrimPrefix(in.GetCommitUri(), "mem://store/") ||
			got.GetTaskSpecSha256() == "" || got.GetAdmissionSha256() != in.GetIdentity().GetAdmissionSha256() {
			t.Fatalf("request = %v", got)
		}
	})
	t.Run("no engine for the whole wait and no record", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		in.Identity.CommitWaitSeconds = 60
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){engineAnswers(down)}}
		d, clock := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		_, f := d.commitThroughEngine(ctx, in, 1, commitOf(in))
		if f == nil || f.Reason != ReasonCommitEngineUnavailable || CategoryFor(f.Reason) != CategoryInfrastructureError {
			t.Fatalf("failure = %v", f)
		}
		if clock.Boot() > time.Minute || len(engine.calls) < 3 {
			t.Fatalf("waited %s over %d calls; want inside the minute, asked again", clock.Boot(), len(engine.calls))
		}
	})
	t.Run("no engine for the whole wait and a record there", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		in.Identity.CommitWaitSeconds = 60
		store := artifacts.NewMemoryCommitStore()
		uri, err := artifacts.AttestedCommitURI(in.GetCommitUri())
		must.OK(t, err)
		rec := commitOf(in)
		rec.Recorded = &orkstackv1.CommitRecording{Pool: "main", OrkerId: "orker-1"}
		plant(t, store, uri, rec)
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){engineAnswers(down)}}
		d, _ := recordingDispatcher(engine, store)
		resp, f := d.commitThroughEngine(ctx, in, 1, commitOf(in))
		if f != nil || resp.GetCommit().GetUri() != uri || resp.GetExisting().GetRecorded().GetOrkerId() != "orker-1" {
			t.Fatalf("commit = %v, %v; want the record there", resp, f)
		}
	})
	t.Run("a mismatch ends at once", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
			engineAnswers(commitRefusal(connect.CodeFailedPrecondition, ReasonCommitIdentity+": no"))}}
		d, _ := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		if _, f := d.commitThroughEngine(ctx, in, 1, commitOf(in)); f == nil || f.Reason != ReasonCommitIdentity || len(engine.calls) != 1 {
			t.Fatalf("failure = %v after %d calls", f, len(engine.calls))
		}
	})
	t.Run("not admitted is asked once more", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
			engineAnswers(commitRefusal(connect.CodeFailedPrecondition, identity.ReasonNotAdmitted+": no"))}}
		d, _ := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		if _, f := d.commitThroughEngine(ctx, in, 1, commitOf(in)); f == nil || f.Reason != identity.ReasonNotAdmitted || len(engine.calls) != 2 {
			t.Fatalf("failure = %v after %d calls", f, len(engine.calls))
		}
	})
	t.Run("a cancel during the wait", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		cctx, cancel := context.WithCancel(ctx)
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
			func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
				cancel()
				return nil, down
			}}}
		d, _ := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		if _, f := d.commitThroughEngine(cctx, in, 1, commitOf(in)); f == nil || len(engine.calls) != 1 {
			t.Fatalf("failure = %v after %d calls; want the attempt stopped", f, len(engine.calls))
		}
	})
	t.Run("no engine configured", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		d := &OCIDispatcher{Commits: artifacts.NewMemoryCommitStore()}
		if _, f := d.commitThroughEngine(ctx, in, 1, commitOf(in)); f == nil || f.Reason != ReasonIdentityNotConfigured {
			t.Fatalf("failure = %v", f)
		}
	})
}

// TestPreflightCommit: a preflight carries no result; a refusal of this
// Orker ends it at once, before any container; an engine that cannot be
// asked is asked again inside the setup budget, then COMMIT_ENGINE_UNAVAILABLE.
func TestPreflightCommit(t *testing.T) {
	ctx := context.Background()
	t.Run("allowed", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
			func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
				return &orkstackv1.CommitResultResponse{}, nil
			}}}
		d, _ := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		if _, f := d.preflightCommit(ctx, in, 1); f != nil || len(engine.calls) != 1 || !engine.calls[0].GetPreflight() || engine.calls[0].GetCommit() != nil {
			t.Fatalf("preflight = %v, calls %v", f, engine.calls)
		}
	})
	t.Run("refused", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
			engineAnswers(commitRefusal(connect.CodePermissionDenied, "may not commit"))}}
		d, _ := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		if _, f := d.preflightCommit(ctx, in, 1); f == nil || f.Reason != ReasonCommitEngineRefused || len(engine.calls) != 1 {
			t.Fatalf("preflight = %v after %d calls", f, len(engine.calls))
		}
	})
	t.Run("no engine within the budget", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){engineAnswers(errors.New("connection refused"))}}
		d, clock := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		_, f := d.preflightCommit(ctx, in, 1)
		if f == nil || f.Reason != ReasonCommitEngineUnavailable || clock.Boot() > DefaultIdentitySetupBudget || len(engine.calls) < 2 {
			t.Fatalf("preflight = %v after %d calls and %s", f, len(engine.calls), clock.Boot())
		}
	})
}

// recordingEngine is an engine's CommitResult that records a result it is
// handed in a memory store under the attested key, as an engine does, and
// keeps every request.
type recordingEngine struct {
	mu       sync.Mutex
	store    *artifacts.MemoryCommitStore
	root     string
	requests []*orkstackv1.CommitResultRequest
	refuse   error
}

func (e *recordingEngine) commit(_ context.Context, req *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.requests = append(e.requests, req)
	if e.refuse != nil {
		return nil, e.refuse
	}
	if req.GetPreflight() {
		return &orkstackv1.CommitResultResponse{}, nil
	}
	key, err := artifacts.AttestedCommitKey(req.GetCommitKey())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	doc := req.GetCommit()
	doc.Recorded = &orkstackv1.CommitRecording{Pool: "main", OrkerId: "orker-1"}
	raw, err := artifacts.MarshalCommit(doc)
	if err != nil {
		return nil, err
	}
	if _, err := e.store.Get(context.Background(), e.root+key); err == nil {
		return &orkstackv1.CommitResultResponse{Created: false, Commit: &orkstackv1.CommitRef{Uri: e.root + key}}, nil
	}
	e.store.PutRaw(e.root+key, raw)
	rec, err := e.store.Get(context.Background(), e.root+key)
	if err != nil {
		return nil, err
	}
	return &orkstackv1.CommitResultResponse{Created: true, Commit: &orkstackv1.CommitRef{Uri: e.root + key, Sha256: rec.SHA256, Etag: rec.ETag}}, nil
}

// TestExecute_ARecordedRunCommitsThroughTheEngine: an attempt of a task of
// a run whose results the engine records, holding no identity, asks the
// engine before its container runs, then hands it the manifest; nothing is
// written under metadata/commits/, and the result names the engine's
// record. A preflight the engine refuses ends the attempt before any
// container is created.
func TestExecute_ARecordedRunCommitsThroughTheEngine(t *testing.T) {
	t.Run("recorded", func(t *testing.T) {
		log := &eventLog{}
		store := artifacts.NewMemoryCommitStore()
		d := newAttemptDispatcher(t, &policyCapturingRuntime{log: log}, artifacts.NewMemoryAttemptStore())
		d.Commits = store
		d.identity.Store(identitySupportOf(true))
		engine := &recordingEngine{store: store, root: "mem://store/"}
		d.SetResultCommit(engine.commit)
		in := recordedRunInput(t, "build")
		got, err := d.Execute(context.Background(), in)
		if err != nil {
			t.Fatalf("Execute = %v", err)
		}
		if len(engine.requests) != 2 || !engine.requests[0].GetPreflight() || engine.requests[1].GetPreflight() || engine.requests[1].GetCommit() == nil {
			t.Fatalf("the engine was asked %v", engine.requests)
		}
		attested, _ := artifacts.AttestedCommitURI(in.GetCommitUri())
		if got.GetCommit().GetUri() != attested {
			t.Fatalf("the result's commit = %v, want %s", got.GetCommit(), attested)
		}
		if _, err := store.Get(context.Background(), in.GetCommitUri()); err == nil {
			t.Fatal("the Orker wrote a commit under metadata/commits/")
		}
	})
	t.Run("a preflight refused", func(t *testing.T) {
		log := &eventLog{}
		d := newAttemptDispatcher(t, &policyCapturingRuntime{log: log}, artifacts.NewMemoryAttemptStore())
		d.Commits = artifacts.NewMemoryCommitStore()
		d.identity.Store(identitySupportOf(true))
		engine := &recordingEngine{refuse: connect.NewError(connect.CodePermissionDenied, errors.New("may not commit"))}
		d.SetResultCommit(engine.commit)
		_, err := d.Execute(context.Background(), recordedRunInput(t, "build"))
		if err == nil || !strings.Contains(err.Error(), ReasonCommitEngineRefused) {
			t.Fatalf("Execute = %v, want %s", err, ReasonCommitEngineRefused)
		}
		for _, e := range log.all() {
			if e == "pull" || e == "create" {
				t.Fatalf("a refused preflight ran the task: %v", log.all())
			}
		}
	})
	// A task that holds an identity is asked at its mint, and at a preflight
	// only when it stages something its run received.
	for name, receives := range map[string]bool{"that receives": true, "that receives nothing": false} {
		t.Run("a task that holds an identity "+name, func(t *testing.T) {
			d := newAttemptDispatcher(t, &policyCapturingRuntime{log: &eventLog{}}, artifacts.NewMemoryAttemptStore())
			d.Commits = artifacts.NewMemoryCommitStore()
			d.identity.Store(identitySupportOf(true))
			engine := &recordingEngine{refuse: connect.NewError(connect.CodePermissionDenied, errors.New("may not commit"))}
			d.SetResultCommit(engine.commit)
			in := recordedRunInput(t, "build")
			in.Identity.Identities = []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts"}}
			if receives {
				in.Upstream = append(in.Upstream, &orkstackv1.UpstreamRef{TaskId: "_inputs"})
			}
			_, err := d.Execute(context.Background(), in)
			if asked := len(engine.requests) == 1; err == nil || asked != receives || strings.Contains(err.Error(), ReasonCommitEngineRefused) != receives {
				t.Fatalf("Execute = %v after %d asks", err, len(engine.requests))
			}
		})
	}
}

// recordedCacheHarness is the cache harness whose Orker serves task
// identity and whose engine records results in the harness's own commit
// store.
func recordedCacheHarness(t *testing.T) *cacheHarness {
	t.Helper()
	h := newCacheHarness(t)
	h.dispatcher.identity.Store(identitySupportOf(true))
	engine := &recordingEngine{store: h.commits.MemoryCommitStore, root: cacheRoot + "/"}
	h.dispatcher.SetResultCommit(engine.commit)
	return h
}

func recordedCachedInput(t *testing.T, runID string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	in := cachedTaskInput(t, runID)
	in.Identity = &orkstackv1.IdentityContext{AdmissionSha256: strings.Repeat("a", 64), DigestVersion: 1, CommitWaitSeconds: 900}
	return in
}

// TestMemo_ARecordedRunHitsOnlyAnEngineRecordOfItsPool: a cached task of a
// run whose results the engine records writes a memo whose source is the
// engine's record, and a later run hits it, with its own commit recorded by
// the engine; such a run never shares a memo key with a run of the other
// kind; a memo whose source is not under the attested prefix, or is a record
// of another pool, is a miss.
func TestMemo_ARecordedRunHitsOnlyAnEngineRecordOfItsPool(t *testing.T) {
	t.Run("a hit", func(t *testing.T) {
		h := recordedCacheHarness(t)
		first := h.produce(t, recordedCachedInput(t, "ork-rec-1"))
		memo, _ := h.memoFor(t, first, recordedCachedInput(t, "ork-rec-1"))
		if !strings.Contains(memo.GetSource().GetCommitUri(), "/"+artifacts.AttestedPrefix+"/") {
			t.Fatalf("the memo's source = %s, want the engine's record", memo.GetSource().GetCommitUri())
		}
		second, err := h.dispatcher.Execute(context.Background(), recordedCachedInput(t, "ork-rec-2"))
		must.OK(t, err)
		if second.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT ||
			!strings.Contains(second.GetCommit().GetUri(), "/"+artifacts.AttestedPrefix+"/") {
			t.Fatalf("second = cache %v, commit %v; want a hit recorded by the engine", second.GetCache(), second.GetCommit())
		}
	})
	t.Run("the two kinds never share a key", func(t *testing.T) {
		in := cachedTaskInput(t, "ork-kind")
		plain := keyOf(t, in, []byte("{}"), []byte("{}"), nil, nil)
		recorded := keyOf(t, recordedCachedInput(t, "ork-kind"), []byte("{}"), []byte("{}"), nil, nil)
		if plain.Key == recorded.Key {
			t.Fatal("a run of each kind computes one memo key")
		}
	})
	t.Run("a source not under the attested prefix", func(t *testing.T) {
		h := recordedCacheHarness(t)
		first := h.produce(t, recordedCachedInput(t, "ork-rec-3"))
		memo, _ := h.memoFor(t, first, recordedCachedInput(t, "ork-rec-3"))
		memo.Source.CommitUri = recordedCachedInput(t, "ork-rec-3").GetCommitUri()
		must.OK(t, h.memos.MemoryMemoStore.PutMemo(context.Background(), memo))
		second, err := h.dispatcher.Execute(context.Background(), recordedCachedInput(t, "ork-rec-4"))
		must.OK(t, err)
		if c := second.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS || c.GetMissReason() != MemoMissSourceNotRecorded {
			t.Fatalf("cache = %v, want a miss %s", c, MemoMissSourceNotRecorded)
		}
	})
	t.Run("a record of another pool", func(t *testing.T) {
		h := recordedCacheHarness(t)
		h.produce(t, recordedCachedInput(t, "ork-rec-5"))
		other := recordedCachedInput(t, "ork-rec-6")
		other.Pool = "gpu"
		second, err := h.dispatcher.Execute(context.Background(), other)
		must.OK(t, err)
		if c := second.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS || c.GetMissReason() != MemoMissSourceOtherPool {
			t.Fatalf("cache = %v, want a miss %s", c, MemoMissSourceOtherPool)
		}
	})
}

// upstreamRecord plants upstream's engine record for in's run and answers
// the UpstreamRef the interpreter dispatches the dependent with: its commit
// uri is the one the upstream's result reported, the record's own.
func upstreamRecord(t *testing.T, store *artifacts.MemoryCommitStore, in *orkstackv1.ExecuteTaskInput, upstream string, recorded bool) *orkstackv1.UpstreamRef {
	t.Helper()
	key, err := artifacts.CommitKey(in.GetNamespace(), in.GetRunId(), upstream)
	must.OK(t, err)
	uri := "mem://store/" + key
	attested, err := artifacts.AttestedCommitURI(uri)
	must.OK(t, err)
	values := &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://objects/" + strings.Repeat("c", 64), Sha256: strings.Repeat("c", 64), SizeBytes: 2}
	art := &orkstackv1.ArtifactRef{Name: "out.bin", Uri: "mem://objects/" + strings.Repeat("d", 64), Sha256: strings.Repeat("d", 64), SizeBytes: 3}
	c := &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: in.GetNamespace(), RunId: in.GetRunId(),
		TaskId: upstream, Values: values, Artifacts: []*orkstackv1.ArtifactRef{art}, OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
		IdempotencyKey: task.IdempotencyKeyFor(in.GetNamespace(), in.GetRunId(), upstream)}
	if recorded {
		c.Recorded = &orkstackv1.CommitRecording{Pool: "main", OrkerId: "orker-1"}
	}
	plant(t, store, attested, c)
	return &orkstackv1.UpstreamRef{TaskId: upstream, CommitUri: attested, Values: values, Artifacts: []*orkstackv1.ArtifactRef{art}}
}

// TestCheckUpstreamRecord: a dependent of a run whose results the engine
// records stages an upstream container task only from its engine record:
// the record must be there, be that task's of this run, carry the engine's
// recording, and name exactly the references the input carries; otherwise
// UPSTREAM_RESULT_NOT_RECORDED, not retried, naming the upstream. A store
// that does not answer is COMMIT_READ_FAILED, retried.
func TestCheckUpstreamRecord(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		edit   func(*orkstackv1.UpstreamRef, *artifacts.MemoryCommitStore)
		record bool
		reason string
	}{
		{"as recorded", func(*orkstackv1.UpstreamRef, *artifacts.MemoryCommitStore) {}, true, ""},
		{"another task's key", func(up *orkstackv1.UpstreamRef, _ *artifacts.MemoryCommitStore) {
			up.CommitUri = strings.Replace(up.CommitUri, "/compile/", "/other/", 1)
		}, true, ReasonUpstreamNotRecorded},
		{"no recording", func(*orkstackv1.UpstreamRef, *artifacts.MemoryCommitStore) {}, false, ReasonUpstreamNotRecorded},
		{"other values", func(up *orkstackv1.UpstreamRef, _ *artifacts.MemoryCommitStore) {
			up.Values = &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://objects/forged", Sha256: strings.Repeat("e", 64)}
		}, true, ReasonUpstreamNotRecorded},
		{"another artifact", func(up *orkstackv1.UpstreamRef, _ *artifacts.MemoryCommitStore) {
			up.Artifacts = append(up.Artifacts, &orkstackv1.ArtifactRef{Name: "extra", Uri: "mem://objects/extra"})
		}, true, ReasonUpstreamNotRecorded},
		{"a uri outside the commit keys", func(up *orkstackv1.UpstreamRef, _ *artifacts.MemoryCommitStore) {
			up.CommitUri = "mem://elsewhere/commit.json"
		}, true, ReasonUpstreamNotRecorded},
		{"the commit an Orker would have written", func(up *orkstackv1.UpstreamRef, _ *artifacts.MemoryCommitStore) {
			up.CommitUri = strings.Replace(up.CommitUri, artifacts.AttestedPrefix, artifacts.CommitPrefix, 1)
		}, true, ReasonUpstreamNotRecorded},
		{"the record's key under another store root", func(up *orkstackv1.UpstreamRef, store *artifacts.MemoryCommitStore) {
			rec, err := store.Get(ctx, up.CommitUri)
			must.OK(t, err)
			up.CommitUri = strings.Replace(up.CommitUri, "mem://store/", "mem://elsewhere/", 1)
			plant(t, store, up.CommitUri, rec.Commit)
		}, true, ReasonUpstreamNotRecorded},
		{"the record's key below another prefix of the store", func(up *orkstackv1.UpstreamRef, store *artifacts.MemoryCommitStore) {
			rec, err := store.Get(ctx, up.CommitUri)
			must.OK(t, err)
			up.CommitUri = strings.Replace(up.CommitUri, "mem://store/", "mem://store/orkflows/", 1)
			plant(t, store, up.CommitUri, rec.Commit)
		}, true, ReasonUpstreamNotRecorded},
		{"another run's record", func(up *orkstackv1.UpstreamRef, store *artifacts.MemoryCommitStore) {
			rec, err := store.Get(ctx, up.CommitUri)
			must.OK(t, err)
			up.CommitUri = strings.Replace(up.CommitUri, "/ork-export-build/", "/ork-another/", 1)
			plant(t, store, up.CommitUri, rec.Commit)
		}, true, ReasonUpstreamNotRecorded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := recordedRunInput(t, "build")
			store := artifacts.NewMemoryCommitStore()
			up := upstreamRecord(t, store, in, "compile", tc.record)
			tc.edit(up, store)
			d := &OCIDispatcher{Commits: store}
			f := d.checkUpstreamRecord(ctx, in, up)
			switch {
			case tc.reason == "" && f != nil:
				t.Fatalf("check = %v, want it staged", f)
			case tc.reason != "" && (f == nil || f.Reason != tc.reason || !strings.Contains(f.Message, `"compile"`)):
				t.Fatalf("check = %v, want %s naming the upstream", f, tc.reason)
			}
		})
	}
	t.Run("no record", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		up := upstreamRecord(t, artifacts.NewMemoryCommitStore(), in, "compile", true)
		d := &OCIDispatcher{Commits: artifacts.NewMemoryCommitStore()}
		if f := d.checkUpstreamRecord(ctx, in, up); f == nil || f.Reason != ReasonUpstreamNotRecorded || !strings.Contains(f.Message, "no engine recorded") {
			t.Fatalf("check = %v, want %s: nothing is at its key", f, ReasonUpstreamNotRecorded)
		}
	})
	t.Run("the store does not answer", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		store := &readScriptedCommitStore{MemoryCommitStore: artifacts.NewMemoryCommitStore()}
		up := upstreamRecord(t, store.MemoryCommitStore, in, "compile", true)
		store.script = append(store.script, storeDown)
		d := &OCIDispatcher{Commits: store}
		if f := d.checkUpstreamRecord(ctx, in, up); f == nil || f.Reason != ReasonCommitReadFailed {
			t.Fatalf("check = %v, want %s", f, ReasonCommitReadFailed)
		}
	})
	if CategoryFor(ReasonUpstreamNotRecorded) != CategoryInfrastructureError || RetryableReason(ReasonUpstreamNotRecorded) {
		t.Fatalf("%s is %s, retryable %v", ReasonUpstreamNotRecorded, CategoryFor(ReasonUpstreamNotRecorded), RetryableReason(ReasonUpstreamNotRecorded))
	}
}

// TestRecordedRun_ADependentStagesWhatItsProducerReported: the uri a
// dependent's input names is the one its producer's result reported, as the
// interpreter copies it, whichever way the producer got that result: its own
// attempt recorded it, a retry adopted it, or a memo hit recorded it. The
// dependent stages each, with the Orker's commit root known and not.
func TestRecordedRun_ADependentStagesWhatItsProducerReported(t *testing.T) {
	ctx := context.Background()
	h := recordedCacheHarness(t)
	input := func(runID string) *orkstackv1.ExecuteTaskInput {
		in := recordedCachedInput(t, runID)
		in.IdempotencyKey = task.IdempotencyKeyFor("default", runID, in.GetTaskId())
		return in
	}
	fresh := h.produce(t, input("ork-chain-1"))
	adopted, err := h.dispatcher.Execute(ctx, input("ork-chain-1"))
	must.OK(t, err)
	hit, err := h.dispatcher.Execute(ctx, input("ork-chain-2"))
	must.OK(t, err)
	if !adopted.GetCommitAdopted() || hit.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		t.Fatalf("adopted = %v, hit = %v", adopted.GetCommitAdopted(), hit.GetCache())
	}
	for _, tc := range []struct {
		name, runID string
		result      *orkstackv1.ExecuteTaskResult
	}{{"fresh", "ork-chain-1", fresh}, {"adopted", "ork-chain-1", adopted}, {"hit", "ork-chain-2", hit}} {
		for _, root := range []string{"", cacheRoot} {
			in := input(tc.runID)
			up := &orkstackv1.UpstreamRef{TaskId: in.GetTaskId(), CommitUri: tc.result.GetCommit().GetUri(),
				Values: tc.result.GetValuesRef(), Artifacts: tc.result.GetArtifacts()}
			key, err := artifacts.CommitKey("default", tc.runID, "train")
			must.OK(t, err)
			in.TaskId, in.CommitUri, in.IdempotencyKey = "train", cacheRoot+"/"+key, task.IdempotencyKeyFor("default", tc.runID, "train")
			d := &OCIDispatcher{Commits: h.commits, CommitRoot: root}
			if f := d.checkUpstreamRecord(ctx, in, up); f != nil {
				t.Errorf("%s, commit root %q: %v", tc.name, root, f)
			}
		}
	}
}

// TestCheckUpstreamRecord_AResumesScopes: in a resumed run a producer's
// record lies under the scope its result was made in, the base's or a
// resume's up to the attempt's own, and is held to that scope's key and
// idempotency key; a scope past the attempt's own, or spelled as no key is,
// is no record.
func TestCheckUpstreamRecord_AResumesScopes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		scope   uint64 // where the record lies
		idem    uint64 // the scope its idempotency key is of
		attempt uint64 // the attempt's own
		edit    func(*orkstackv1.UpstreamRef)
		staged  bool
	}{
		{"a result kept from the base", 0, 0, 9, nil, true},
		{"a result kept from an earlier resume", 5, 5, 9, nil, true},
		{"a result of the attempt's own resume", 9, 9, 9, nil, true},
		{"a later resume's", 12, 12, 9, nil, false},
		{"a resume's, in a run that names none", 5, 5, 0, nil, false},
		{"the base's idempotency key at a resume's key", 5, 0, 9, nil, false},
		{"a resume's idempotency key at the base's key", 0, 5, 9, nil, false},
		{"scope 0 spelled out", 5, 5, 9, func(up *orkstackv1.UpstreamRef) {
			up.CommitUri = strings.Replace(up.CommitUri, "/r00000000000000000005/", "/r00000000000000000000/", 1)
		}, false},
		{"a scope not spelled as a key spells it", 5, 5, 9, func(up *orkstackv1.UpstreamRef) {
			up.CommitUri = strings.Replace(up.CommitUri, "/r00000000000000000005/", "/r5/", 1)
		}, false},
		{"an item's record", 5, 5, 9, func(up *orkstackv1.UpstreamRef) {
			up.CommitUri = strings.Replace(up.CommitUri, "/commit.json", "/00001/commit.json", 1)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := recordedRunInput(t, "build")
			in.Identity.ResumeScope = tc.attempt
			store := artifacts.NewMemoryCommitStore()
			up := upstreamRecord(t, store, in, "compile", true)
			rec, err := store.Get(ctx, up.GetCommitUri())
			must.OK(t, err)
			c := rec.Commit
			c.IdempotencyKey = task.ScopedIdempotencyKeyFor(in.GetNamespace(), in.GetRunId(), "compile", tc.idem)
			key, err := artifacts.ScopedCommitKey(in.GetNamespace(), in.GetRunId(), "compile", tc.scope)
			must.OK(t, err)
			up.CommitUri, err = artifacts.AttestedCommitURI("mem://store/" + key)
			must.OK(t, err)
			store = artifacts.NewMemoryCommitStore()
			if tc.edit != nil {
				tc.edit(up)
			}
			plant(t, store, up.GetCommitUri(), c)
			f := (&OCIDispatcher{Commits: store}).checkUpstreamRecord(ctx, in, up)
			if tc.staged != (f == nil) || (f != nil && f.Reason != ReasonUpstreamNotRecorded) {
				t.Fatalf("check = %v, want staged %v", f, tc.staged)
			}
		})
	}
}

// TestRecordedRun_ANestedKeyIsNoRecord: an object under metadata/commits/,
// where an Orker writes, whose key ends as an engine's record does, is never
// one: not adopted, not staged, not read as an item, and never the source of
// a memo hit, with no commit root to tell by.
func TestRecordedRun_ANestedKeyIsNoRecord(t *testing.T) {
	ctx := context.Background()
	nest := func(uri string) string {
		return strings.Replace(uri, "mem://store/", "mem://store/metadata/commits/default/ork-export-build/x/", 1)
	}
	t.Run("adopted", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		in.CommitUri = nest(in.GetCommitUri())
		rec := commitOf(in)
		rec.Recorded = &orkstackv1.CommitRecording{Pool: "main"}
		store := artifacts.NewMemoryCommitStore()
		plant(t, store, strings.Replace(in.GetCommitUri(), "x/metadata/commits/", "x/metadata/attested/", 1), rec)
		d := &OCIDispatcher{Commits: store}
		if got, f := d.adopt(ctx, in, 2); got != nil || f == nil || f.Reason != ReasonCommitIdentity {
			t.Fatalf("adopt = %v, %v; want %s", got, f, ReasonCommitIdentity)
		}
	})
	t.Run("staged", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		store := artifacts.NewMemoryCommitStore()
		up := upstreamRecord(t, store, in, "compile", true)
		rec, err := store.Get(ctx, up.GetCommitUri())
		must.OK(t, err)
		in.CommitUri, up.CommitUri = nest(in.GetCommitUri()), nest(up.GetCommitUri())
		plant(t, store, up.GetCommitUri(), rec.Commit)
		d := &OCIDispatcher{Commits: store}
		if f := d.checkUpstreamRecord(ctx, in, up); f == nil || f.Reason != ReasonUpstreamNotRecorded {
			t.Fatalf("check = %v, want %s", f, ReasonUpstreamNotRecorded)
		}
	})
	t.Run("a task with no commit uri", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		store := artifacts.NewMemoryCommitStore()
		up := upstreamRecord(t, store, in, "compile", true)
		in.CommitUri = ""
		d := &OCIDispatcher{Commits: store}
		if f := d.checkUpstreamRecord(ctx, in, up); f == nil || f.Reason != ReasonUpstreamNotRecorded {
			t.Fatalf("check = %v, want %s", f, ReasonUpstreamNotRecorded)
		}
	})
	t.Run("an item", func(t *testing.T) {
		in := recordedRunInput(t, "merge")
		key, err := artifacts.ItemCommitKey(in.GetNamespace(), in.GetRunId(), "convert", 1)
		must.OK(t, err)
		attested, err := artifacts.AttestedCommitKey(key)
		must.OK(t, err)
		root := "mem://store/metadata/commits/default/ork-export-merge/x"
		store := artifacts.NewMemoryCommitStore()
		plant(t, store, root+"/"+attested, &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: in.GetNamespace(),
			RunId: in.GetRunId(), TaskId: "convert", IdempotencyKey: task.ItemIdempotencyKeyFor(in.GetNamespace(), in.GetRunId(), "convert", 1),
			OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(), Recorded: &orkstackv1.CommitRecording{Pool: "main"}})
		d := &OCIDispatcher{Commits: store}
		if got, f := d.readItemCommit(ctx, in, "convert", root, 1, 0); f == nil || f.Reason != ReasonUpstreamNotRecorded {
			t.Fatalf("item = %v, %v; want %s", got, f, ReasonUpstreamNotRecorded)
		}
	})
	t.Run("a memo's source", func(t *testing.T) {
		h := recordedCacheHarness(t)
		first := h.produce(t, recordedCachedInput(t, "ork-nest-1"))
		rec, err := h.commits.MemoryCommitStore.Get(ctx, first.GetCommit().GetUri())
		must.OK(t, err)
		memo, _ := h.memoFor(t, first, recordedCachedInput(t, "ork-nest-1"))
		memo.Source.CommitUri = strings.Replace(memo.GetSource().GetCommitUri(), cacheRoot+"/", cacheRoot+"/metadata/commits/default/ork-nest-1/x/", 1)
		plant(t, h.commits.MemoryCommitStore, memo.GetSource().GetCommitUri(), rec.Commit)
		must.OK(t, h.memos.MemoryMemoStore.PutMemo(ctx, memo))
		second, err := h.dispatcher.Execute(ctx, recordedCachedInput(t, "ork-nest-2"))
		must.OK(t, err)
		if c := second.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS || c.GetMissReason() != MemoMissSourceNotRecorded {
			t.Fatalf("cache = %v, want a miss %s", c, MemoMissSourceNotRecorded)
		}
	})
}

// TestReadItemCommit_ARecordedRunReadsItsItemsRecords: a fan-out's items
// are read under the mapped root for a run whose results the engine
// records: an item an Orker committed is not there, and one without the
// engine's recording is refused.
func TestReadItemCommit_ARecordedRunReadsItsItemsRecords(t *testing.T) {
	ctx := context.Background()
	in := recordedRunInput(t, "merge")
	key, err := artifacts.ItemCommitKey(in.GetNamespace(), in.GetRunId(), "convert", 1)
	must.OK(t, err)
	plain := "mem://store/" + key
	attested, err := artifacts.AttestedCommitURI(plain)
	must.OK(t, err)
	item := &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: in.GetNamespace(), RunId: in.GetRunId(),
		TaskId: "convert", IdempotencyKey: task.ItemIdempotencyKeyFor(in.GetNamespace(), in.GetRunId(), "convert", 1),
		OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest()}

	store := artifacts.NewMemoryCommitStore()
	plant(t, store, plain, item)
	d := &OCIDispatcher{Commits: store}
	if _, f := d.readItemCommit(ctx, in, "convert", "mem://store", 1, 0); f == nil || f.Reason != ReasonUpstreamNotRecorded {
		t.Fatalf("an item an Orker committed = %v, want %s", f, ReasonUpstreamNotRecorded)
	}
	plant(t, store, attested, item)
	if _, f := d.readItemCommit(ctx, in, "convert", "mem://store", 1, 0); f == nil || f.Reason != ReasonUpstreamNotRecorded {
		t.Fatalf("an item with no recording = %v, want %s", f, ReasonUpstreamNotRecorded)
	}
	item.Recorded = &orkstackv1.CommitRecording{Pool: "main"}
	plant(t, store, attested, item)
	if got, f := d.readItemCommit(ctx, in, "convert", "mem://store", 1, 0); f != nil || got.GetTaskId() != "convert" {
		t.Fatalf("an engine's item record = %v, %v", got, f)
	}
}

// lookCancellingStore cancels its attempt when it is read: a cancel that
// arrives during the last look.
type lookCancellingStore struct {
	*artifacts.MemoryCommitStore
	cancel context.CancelFunc
}

func (s lookCancellingStore) Get(ctx context.Context, uri string) (artifacts.CommitRecord, error) {
	s.cancel()
	return artifacts.CommitRecord{}, ctx.Err()
}

// TestCommitThroughEngine_ReviewCases: a cancel during the last look ends
// the attempt stopped, never COMMIT_READ_FAILED; an engine that answers
// created with no record is an outage, never a panic.
func TestCommitThroughEngine_ReviewCases(t *testing.T) {
	down := commitRefusal(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE: ask again")
	t.Run("a cancel during the last look", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		in.Identity.CommitWaitSeconds = 30
		ctx, cancel := context.WithCancel(context.Background())
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){engineAnswers(down)}}
		d, _ := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		d.Commits = lookCancellingStore{MemoryCommitStore: artifacts.NewMemoryCommitStore(), cancel: cancel}
		_, f := d.commitThroughEngine(ctx, in, 1, commitOf(in))
		if f == nil || f.Reason == ReasonCommitReadFailed {
			t.Fatalf("failure = %v; want the attempt stopped, not %s", f, ReasonCommitReadFailed)
		}
	})
	t.Run("created with no record", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		engine := &scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
			func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
				return &orkstackv1.CommitResultResponse{Created: true}, nil
			}}}
		d, _ := recordingDispatcher(engine, artifacts.NewMemoryCommitStore())
		_, f := d.recordThroughEngine(context.Background(), in, 1, commitOf(in), &orkstackv1.ExecuteTaskResult{}, &memoTrace{})
		if f == nil || f.Reason != ReasonCommitEngineUnavailable {
			t.Fatalf("failure = %v, want %s", f, ReasonCommitEngineUnavailable)
		}
	})
}

// TestMemo_ARecordedRunReviewCases: an input that names no pool is the
// default pool's, as the engine records it; a source with no recording is
// not an engine's.
func TestMemo_ARecordedRunReviewCases(t *testing.T) {
	t.Run("the default pool, named or not", func(t *testing.T) {
		h := recordedCacheHarness(t)
		h.produce(t, recordedCachedInput(t, "ork-rec-p1"))
		unnamed := recordedCachedInput(t, "ork-rec-p2")
		unnamed.Pool = ""
		second, err := h.dispatcher.Execute(context.Background(), unnamed)
		must.OK(t, err)
		if second.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
			t.Fatalf("cache = %v, want a hit", second.GetCache())
		}
	})
	t.Run("a source with no recording", func(t *testing.T) {
		h := recordedCacheHarness(t)
		first := h.produce(t, recordedCachedInput(t, "ork-rec-r1"))
		rec, err := h.commits.MemoryCommitStore.Get(context.Background(), first.GetCommit().GetUri())
		must.OK(t, err)
		rec.Commit.Recorded = nil
		raw, err := artifacts.MarshalCommit(rec.Commit)
		must.OK(t, err)
		h.commits.PutRaw(first.GetCommit().GetUri(), raw)
		memo, _ := h.memoFor(t, first, recordedCachedInput(t, "ork-rec-r1"))
		fresh, err := h.commits.MemoryCommitStore.Get(context.Background(), first.GetCommit().GetUri())
		must.OK(t, err)
		memo.Source.CommitSha256 = fresh.SHA256
		must.OK(t, h.memos.MemoryMemoStore.PutMemo(context.Background(), memo))
		second, err := h.dispatcher.Execute(context.Background(), recordedCachedInput(t, "ork-rec-r2"))
		must.OK(t, err)
		if c := second.GetCache(); c.GetMissReason() != MemoMissSourceNotRecorded {
			t.Fatalf("cache = %v, want a miss %s", c, MemoMissSourceNotRecorded)
		}
	})
}

// TestRecordedRun_FailureReviewCases: a recorded run never reports success
// without an engine's record, whatever its input or store lacks; an
// upstream that carries values but neither a commit nor a record is not
// staged; an upstream record is held to the same identity checks as an
// adopted commit.
func TestRecordedRun_FailureReviewCases(t *testing.T) {
	ctx := context.Background()
	t.Run("no commit uri", func(t *testing.T) {
		d := newAttemptDispatcher(t, &policyCapturingRuntime{log: &eventLog{}}, artifacts.NewMemoryAttemptStore())
		d.Commits = artifacts.NewMemoryCommitStore()
		d.identity.Store(identitySupportOf(true))
		d.SetResultCommit((&recordingEngine{store: artifacts.NewMemoryCommitStore(), root: "mem://store/"}).commit)
		in := recordedRunInput(t, "build")
		in.CommitUri = ""
		if _, err := d.Execute(ctx, in); err == nil {
			t.Fatal("a recorded run with no commit uri succeeded without a record")
		}
	})
	t.Run("an upstream with values and no commit", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		store := artifacts.NewMemoryCommitStore()
		up := upstreamRecord(t, store, in, "compile", true)
		up.CommitUri = ""
		d := &OCIDispatcher{Commits: store}
		if f := d.checkUpstreamRecord(ctx, in, up); f == nil || f.Reason != ReasonUpstreamNotRecorded {
			t.Fatalf("check = %v, want %s", f, ReasonUpstreamNotRecorded)
		}
		up.Values = nil // a failed need's ref, for a failure handler: artifacts only
		in.Join = &orkstackv1.JoinContext{Policy: orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE,
			Needs: []*orkstackv1.NeedOutcome{{TaskId: "compile", Status: orkstackv1.Status_STATUS_FAILED}}}
		if f := d.checkUpstreamRecord(ctx, in, up); f != nil {
			t.Fatalf("a failed need's ref = %v, want it staged", f)
		}
	})
	t.Run("an upstream record of another definition", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		store := artifacts.NewMemoryCommitStore()
		up := upstreamRecord(t, store, in, "compile", true)
		in.OrkflowDefinitionDigest = "sha256:" + strings.Repeat("1", 64)
		d := &OCIDispatcher{Commits: store}
		if f := d.checkUpstreamRecord(ctx, in, up); f == nil || f.Reason != ReasonUpstreamNotRecorded {
			t.Fatalf("check = %v, want %s", f, ReasonUpstreamNotRecorded)
		}
	})
}

// TestCheckUpstreamRecord_SecurityReviewCases: a ref with no commit is
// staged only for a failure handler's need that did not succeed, and only
// without values, since a forged success with no commit has no record; a
// ref whose commit is not the producer's own key in this run is not
// staged, whatever record it points at.
func TestCheckUpstreamRecord_SecurityReviewCases(t *testing.T) {
	ctx := context.Background()
	handler := func(in *orkstackv1.ExecuteTaskInput, status orkstackv1.Status) {
		in.Join = &orkstackv1.JoinContext{Policy: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED,
			Needs: []*orkstackv1.NeedOutcome{{TaskId: "compile", Status: status}}}
	}
	artifactsOnly := func(up *orkstackv1.UpstreamRef) {
		up.CommitUri, up.Values = "", nil
	}
	for _, tc := range []struct {
		name   string
		edit   func(*orkstackv1.ExecuteTaskInput, *orkstackv1.UpstreamRef)
		staged bool
	}{
		{"a success with no commit, not a handler", func(in *orkstackv1.ExecuteTaskInput, up *orkstackv1.UpstreamRef) { artifactsOnly(up) }, false},
		{"a handler's succeeded need with no commit", func(in *orkstackv1.ExecuteTaskInput, up *orkstackv1.UpstreamRef) {
			handler(in, orkstackv1.Status_STATUS_SUCCEEDED)
			artifactsOnly(up)
		}, false},
		{"a handler's failed need's artifacts", func(in *orkstackv1.ExecuteTaskInput, up *orkstackv1.UpstreamRef) {
			handler(in, orkstackv1.Status_STATUS_FAILED)
			artifactsOnly(up)
		}, true},
		{"another task's record", func(in *orkstackv1.ExecuteTaskInput, up *orkstackv1.UpstreamRef) {
			up.CommitUri = strings.Replace(up.CommitUri, "/compile/", "/lint/", 1)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := recordedRunInput(t, "build")
			store := artifacts.NewMemoryCommitStore()
			up := upstreamRecord(t, store, in, "compile", true)
			// The same record planted under another task's key, as a
			// forger would point at it.
			lint, _ := artifacts.CommitKey(in.GetNamespace(), in.GetRunId(), "lint")
			lintAttested, _ := artifacts.AttestedCommitURI("mem://store/" + lint)
			rec, err := store.Get(ctx, strings.Replace(lintAttested, "/lint/", "/compile/", 1))
			must.OK(t, err)
			raw, _ := artifacts.MarshalCommit(rec.Commit)
			store.PutRaw(lintAttested, raw)
			tc.edit(in, up)
			d := &OCIDispatcher{Commits: store}
			f := d.checkUpstreamRecord(ctx, in, up)
			if tc.staged != (f == nil) || (f != nil && f.Reason != ReasonUpstreamNotRecorded) {
				t.Fatalf("check = %v, want staged %v", f, tc.staged)
			}
		})
	}
}

// TestRecordedRun_MainsNewInputs: an item a resume ran again is read from
// its own scope's engine record, never the earlier scope's; a composed
// run's received artifacts (the reserved producer _inputs, which have no
// commit) are not staged from what the input says: such a run fails closed
// until what its parent handed it is on record (workload-identity §3.6).
func TestRecordedRun_MainsNewInputs(t *testing.T) {
	ctx := context.Background()
	t.Run("a resumed item", func(t *testing.T) {
		in := recordedRunInput(t, "merge")
		store := artifacts.NewMemoryCommitStore()
		key, err := artifacts.ItemCommitKey(in.GetNamespace(), in.GetRunId(), "convert", 1)
		must.OK(t, err)
		earlier, err := artifacts.AttestedCommitURI("mem://store/" + key)
		must.OK(t, err)
		plant(t, store, earlier, &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: in.GetNamespace(),
			RunId: in.GetRunId(), TaskId: "convert", IdempotencyKey: task.ItemIdempotencyKeyFor(in.GetNamespace(), in.GetRunId(), "convert", 1),
			OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(), Recorded: &orkstackv1.CommitRecording{Pool: "main"}})
		d := &OCIDispatcher{Commits: store}
		if _, f := d.readItemCommit(ctx, in, "convert", "mem://store", 1, 0); f != nil {
			t.Fatalf("the item at scope 0 = %v", f)
		}
		if _, f := d.readItemCommit(ctx, in, "convert", "mem://store", 1, 7); f == nil || f.Reason != ReasonUpstreamNotRecorded {
			t.Fatalf("the item a resume ran again = %v, want %s: the earlier scope's record is not its result", f, ReasonUpstreamNotRecorded)
		}
	})
	t.Run("received artifacts", func(t *testing.T) {
		in := recordedRunInput(t, "build")
		d := &OCIDispatcher{Commits: artifacts.NewMemoryCommitStore()}
		up := &orkstackv1.UpstreamRef{TaskId: "_inputs", Artifacts: []*orkstackv1.ArtifactRef{{Name: "report", Uri: "mem://objects/forged", Sha256: strings.Repeat("e", 64)}}}
		if f := d.checkUpstreamRecord(ctx, in, up); f == nil || f.Reason != ReasonUpstreamNotRecorded {
			t.Fatalf("check = %v, want %s", f, ReasonUpstreamNotRecorded)
		}
	})
}
