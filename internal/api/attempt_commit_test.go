package api

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// codeGranted is the code column's value for a call that succeeds.
const codeGranted connect.Code = 0

var committers = []*orkstackv1.Role{{Name: "commits", Permissions: []string{"task.result.commit"}}}

func commitBinding(scope string) *orkstackv1.Binding {
	return &orkstackv1.Binding{Name: "b", Role: "commits", Scope: scope, Subjects: []string{"serviceaccount:orker-1"}}
}

// heldStarts answers the start check from the record store and one
// execution of the run, its first.
type heldStarts struct {
	*artifacts.RecordStore
	facts identity.ExecutionFacts
	sha   string
}

func (h heldStarts) Describe(context.Context, string, string) (identity.ExecutionFacts, error) {
	return h.facts, nil
}

func (h heldStarts) FirstStartAdmission(context.Context, string, string) (string, error) {
	return h.sha, nil
}

// commitService is a service whose recorder writes to a real local record
// store holding one admission, of run ork-1 whose task build runs at pool
// main and holds no identity, and whose describe says Orker orker-1 holds
// attempt 1 of it.
func commitService(t *testing.T, roles []*orkstackv1.Role, bindings ...*orkstackv1.Binding) (*Service, *orkstackv1.CommitResultRequest, *artifacts.MemoryAuditStore) {
	t.Helper()
	dir := t.TempDir()
	cfg := artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir}
	store, err := artifacts.NewRecordStore(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	adm := artifacts.Admission{RunID: "ork-1", Namespace: "default", Orkflow: "deploy", OrkflowVersion: "1",
		DefinitionDigest: strings.Repeat("d", 64), Starter: artifacts.Starter{Kind: artifacts.StarterEngine},
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: now,
		EngineBuild: "b7", SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1,
		Tasks: map[string]artifacts.AdmittedTask{"build": {Pool: "main", SpecSHA256: strings.Repeat("a", 64)}}}
	raw, sha, err := artifacts.EncodeAdmission(adm)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := artifacts.AdmissionKey("default", "ork-1", sha)
	if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, key)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, key), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	facts := identity.ExecutionFacts{WorkflowID: "ork-1", Open: true, ExecutionID: "e1", FirstExecutionID: "e1",
		Pending: []identity.PendingAttempt{{ActivityID: "ork/build", Started: true, Attempt: 1, LastWorker: "orker-1", StartedAt: now}}}
	audits := artifacts.NewMemoryAuditStore()
	s := NewService(Config{AuditStore: audits, Artifacts: cfg, Recorder: &identity.Recorder{Stores: store,
		Starts:      heldStarts{RecordStore: store, facts: facts, sha: sha},
		Describe:    func(context.Context, string) (identity.ExecutionFacts, error) { return facts, nil },
		PinnedImage: func(context.Context, artifacts.Admission, string) (string, error) { return "", nil },
		EngineBuild: "engine-9", Now: time.Now}})
	a, _ := accessFor(t, authn.ModeEnforce, roles, bindings...)
	s.access = a
	commitKey, _ := artifacts.CommitKey("default", "ork-1", "build")
	req := &orkstackv1.CommitResultRequest{Namespace: "default", RunId: "ork-1", TaskId: "build", Attempt: 1,
		TaskSpecSha256: strings.Repeat("a", 64), AdmissionSha256: sha, CommitKey: commitKey,
		Commit: &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "default", RunId: "ork-1", TaskId: "build",
			OrkflowDefinitionDigest: "sha256:" + strings.Repeat("d", 64), IdempotencyKey: task.IdempotencyKeyFor("default", "ork-1", "build"),
			Image: &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("e", 64)}, Attempt: 1, OrkerId: "orker-1"}}
	return s, req, audits
}

func commitRefusals(t *testing.T, audits *artifacts.MemoryAuditStore) int {
	t.Helper()
	page, err := audits.ListAuditRecords(context.Background(), artifacts.AuditQuery{Limit: 100, Until: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range page.Records {
		if r.GetAction() == "task.result.refused" {
			n++
		}
	}
	return n
}

// TestMayCommit: CommitResult answers only in enforce mode and to a
// credential that names an Orker; the caller must hold task.result.commit
// at the pool the admission gives the task, never at another; a denial
// writes one audit record, and a grant records the result.
func TestMayCommit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		binding *orkstackv1.Binding
		ctx     context.Context
		code    connect.Code
		reason  string
		audited int
	}{
		{"observe mode", commitBinding("pool:main"), asOrker(authn.ModeObserve, anOrker), connect.CodeFailedPrecondition, identity.ReasonEngineRefused, 0},
		{"a credential naming no Orker", commitBinding("pool:main"), asOrker(authn.ModeEnforce, &authn.Principal{Kind: authn.KindService, ID: "serviceaccount:x", Issuer: "serviceaccount", Method: authn.MethodServiceAccount}),
			connect.CodeFailedPrecondition, identity.ReasonEngineRefused, 1},
		{"at the task's pool", commitBinding("pool:main"), asOrker(authn.ModeEnforce, anOrker), codeGranted, "", 0},
		{"at the deployment", commitBinding("deployment"), asOrker(authn.ModeEnforce, anOrker), codeGranted, "", 0},
		{"at another pool", commitBinding("pool:gpu"), asOrker(authn.ModeEnforce, anOrker), connect.CodePermissionDenied, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, req, audits := commitService(t, committers, tc.binding)
			resp, err := s.CommitResult(tc.ctx, connect.NewRequest(req))
			if tc.code == codeGranted {
				if err != nil || !resp.Msg.GetCreated() || !strings.Contains(resp.Msg.GetCommit().GetUri(), "/metadata/attested/default/ork-1/build/commit.json") ||
					resp.Msg.GetCommit().GetSha256() == "" {
					t.Fatalf("commit = %v, %v", resp, err)
				}
			} else if connect.CodeOf(err) != tc.code || (tc.reason != "" && !strings.Contains(err.Error(), tc.reason)) {
				t.Fatalf("err = %v, want %s %s", err, tc.code, tc.reason)
			}
			if got := commitRefusals(t, audits); got != tc.audited {
				t.Fatalf("audit records = %d, want %d", got, tc.audited)
			}
		})
	}
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	s.recorder = nil
	if _, err := s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req)); !strings.Contains(errString(err), identity.ReasonEngineRefused) {
		t.Fatalf("no recorder: %v", err)
	}
}

// TestCommitResult_ARepeatIsAnsweredWithTheRecord: a second request for
// the same attempt creates nothing and answers the record already there.
func TestCommitResult_ARepeatIsAnsweredWithTheRecord(t *testing.T) {
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	ctx := asOrker(authn.ModeEnforce, anOrker)
	first, err := s.CommitResult(ctx, connect.NewRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.CommitResult(ctx, connect.NewRequest(req))
	if err != nil || again.Msg.GetCreated() || again.Msg.GetCommit().GetSha256() != first.Msg.GetCommit().GetSha256() ||
		again.Msg.GetExisting().GetRecorded().GetOrkerId() != "orker-1" {
		t.Fatalf("a repeat = %v, %v", again, err)
	}
}

// TestCommitResult_APreflightWritesNothing: a preflight is answered with
// no record.
func TestCommitResult_APreflightWritesNothing(t *testing.T) {
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	req.Preflight, req.Commit = true, nil
	resp, err := s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req))
	if err != nil || resp.Msg.GetCommit() != nil || resp.Msg.GetCreated() {
		t.Fatalf("preflight = %v, %v", resp, err)
	}
}

// TestCommitResult_AMismatchIsRefusedAndAudited: a result that is not of
// the attempt it names is COMMIT_IDENTITY_MISMATCH, failed_precondition,
// and writes one audit record.
func TestCommitResult_AMismatchIsRefusedAndAudited(t *testing.T) {
	s, req, audits := commitService(t, committers, commitBinding("pool:main"))
	req.Commit.OrkerId = "orker-2"
	_, err := s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), identity.ReasonCommitIdentityMismatch) {
		t.Fatalf("err = %v", err)
	}
	if n := commitRefusals(t, audits); n != 1 {
		t.Fatalf("audit records = %d", n)
	}
}

// TestCommitResult_ABadRequestIsRefusedBeforeAnythingIsRead: a request
// whose names, ids or size are not as they must be is invalid_argument.
func TestCommitResult_ABadRequestIsRefusedBeforeAnythingIsRead(t *testing.T) {
	for name, edit := range map[string]func(*orkstackv1.CommitResultRequest){
		"a task id that is no name": func(r *orkstackv1.CommitResultRequest) { r.TaskId = "Build\n" },
		"a long run id":             func(r *orkstackv1.CommitResultRequest) { r.RunId = strings.Repeat("r", 65) },
		"a long commit key":         func(r *orkstackv1.CommitResultRequest) { r.CommitKey = strings.Repeat("k", 1025) },
		"a commit over its bound": func(r *orkstackv1.CommitResultRequest) {
			r.Commit.OrkflowName = strings.Repeat("x", artifacts.CommitMaxBytes)
		},
		"a commit and a preflight": func(r *orkstackv1.CommitResultRequest) { r.Preflight = true },
		"neither":                  func(r *orkstackv1.CommitResultRequest) { r.Commit = nil },
	} {
		s, req, audits := commitService(t, committers, commitBinding("pool:main"))
		edit(req)
		if _, err := s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
		if n := commitRefusals(t, audits); n != 0 {
			t.Errorf("%s: %d audit records", name, n)
		}
	}
}

// TestCommitResult_AnAttemptIsGrantedFourCallsAMinute: a fifth granted
// call of one attempt within the minute is resource_exhausted; a refused
// call spends nothing.
func TestCommitResult_AnAttemptIsGrantedFourCallsAMinute(t *testing.T) {
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	ctx := asOrker(authn.ModeEnforce, anOrker)
	bad := proto.Clone(req).(*orkstackv1.CommitResultRequest)
	bad.Commit.OrkerId = "orker-2"
	for range 3 {
		if _, err := s.CommitResult(ctx, connect.NewRequest(bad)); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("a mismatch: %v", err)
		}
	}
	for i := range 4 {
		if _, err := s.CommitResult(ctx, connect.NewRequest(req)); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}
	if _, err := s.CommitResult(ctx, connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("a fifth call: %v", err)
	}
}

// TestCommitResult_TheInterceptorAdmitsTheHandler: the access interceptor
// asks the commit's permission of nobody, so an authenticated Orker
// reaches the handler, which asks it at the admitted pool; an
// unauthenticated caller is refused before it.
func TestCommitResult_TheInterceptorAdmitsTheHandler(t *testing.T) {
	s, req, _ := commitService(t, nil)
	if _, err := s.access.decide(asOrker(authn.ModeEnforce, anOrker), s, orkstackv1connect.AttemptServiceCommitResultProcedure, req); err != nil {
		t.Fatalf("an authenticated Orker is refused before the handler: %v", err)
	}
	if _, err := s.access.decide(authn.WithMode(context.Background(), authn.ModeEnforce), s, orkstackv1connect.AttemptServiceCommitResultProcedure, req); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("an unauthenticated caller: %v", err)
	}
}

// TestPinCommitWait: a plan of a run tree that holds an identity gains
// identity.results.commit_wait in its transfer budget, so an attempt's
// deadline has room to wait for an engine at its commit; every other
// plan is unchanged.
func TestPinCommitWait(t *testing.T) {
	s := NewService(Config{IdentityCommitWait: 15 * time.Minute})
	for _, tc := range []struct {
		name   string
		holds  bool
		budget int32
		want   int32
	}{
		{"a tree that holds no identity", false, 300, 300},
		{"a tree that holds one", true, 300, 300 + 900},
		{"a budget near its bound", true, math.MaxInt32 - 10, math.MaxInt32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &engine.RunPlan{TransferBudgetSeconds: tc.budget}
			s.pinCommitWait(plan, tc.holds)
			if plan.TransferBudgetSeconds != tc.want {
				t.Fatalf("budget = %d, want %d", plan.TransferBudgetSeconds, tc.want)
			}
		})
	}
}

// TestCommitResult_AResultTooLargeToRecordIsRefusedAtOnce: a manifest
// whose stored form would pass the commit's bound is invalid_argument
// before anything is read, though its wire size is under it, so the Orker
// is told at once and does not wait out its commit wait.
func TestCommitResult_AResultTooLargeToRecordIsRefusedAtOnce(t *testing.T) {
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	for i := range 30000 {
		req.Commit.Artifacts = append(req.Commit.Artifacts, &orkstackv1.ArtifactRef{Name: "a", Uri: "s3://b/" + strings.Repeat("u", 32),
			SizeBytes: int64(i), Sha256: strings.Repeat("c", 64)})
	}
	if proto.Size(req.Commit) >= artifacts.CommitMaxBytes {
		t.Fatalf("the fixture's wire size %d is not under the bound", proto.Size(req.Commit))
	}
	if _, err := s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("err = %v, want invalid_argument", err)
	}
}

// TestCommitResult_ARefusalSpendsThePrincipalsBucket: a refused call is
// not given back to the caller's bucket, so a credential cannot make the
// engine read without limit by asking for what it is refused; a call that
// could not be asked is given back.
func TestCommitResult_ARefusalSpendsThePrincipalsBucket(t *testing.T) {
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	bad := proto.Clone(req).(*orkstackv1.CommitResultRequest)
	bad.Commit.OrkerId = "orker-2"
	ctx := asOrker(authn.ModeEnforce, anOrker)
	frozen := time.Now()
	s.commitLimits = identity.NewCommitLimits(func() time.Time { return frozen })
	for range 513 {
		if _, err := s.CommitResult(ctx, connect.NewRequest(bad)); connect.CodeOf(err) == connect.CodeResourceExhausted {
			return
		}
	}
	t.Fatal("513 refused calls in one instant left the caller's bucket unspent")
}

// TestCommitResult_APreflightNeedsNoCommitKey: a preflight is asked before
// the commit's key matters, and is answered without one.
func TestCommitResult_APreflightNeedsNoCommitKey(t *testing.T) {
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	req.Preflight, req.Commit, req.CommitKey = true, nil, ""
	if _, err := s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req)); err != nil {
		t.Fatalf("preflight: %v", err)
	}
}

// TestCommitResult_ACredentialNamingNoOrkerIsAuditedWithBoundedNames: the
// refusal of a credential that names no Orker is audited only once the
// request's names are known to be names, so no caller string reaches the
// audit record unbounded.
func TestCommitResult_ACredentialNamingNoOrkerIsAuditedWithBoundedNames(t *testing.T) {
	s, req, audits := commitService(t, committers, commitBinding("pool:main"))
	req.RunId = strings.Repeat("r\n", 4096)
	noOrker := asOrker(authn.ModeEnforce, &authn.Principal{Kind: authn.KindService, ID: "serviceaccount:x", Issuer: "serviceaccount", Method: authn.MethodServiceAccount})
	if _, err := s.CommitResult(noOrker, connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("err = %v, want invalid_argument", err)
	}
	if n := commitRefusals(t, audits); n != 0 {
		t.Fatalf("a request with no names was audited %d times", n)
	}
}

// TestCommitResult_IsCounted: each commit is counted once under its
// outcome.
func TestCommitResult_IsCounted(t *testing.T) {
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	counted := countedCommits{}
	s.commitObserver = counted
	s.CommitResult(asOrker(authn.ModeObserve, anOrker), connect.NewRequest(req))
	s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req))
	bad := proto.Clone(req).(*orkstackv1.CommitResultRequest)
	bad.Commit.OrkerId = "orker-2"
	s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(bad))
	if counted[identity.ReasonEngineRefused] != 1 || counted[identity.OutcomeGranted] != 1 || counted[identity.ReasonCommitIdentityMismatch] != 1 {
		t.Fatalf("counted = %v", counted)
	}
}

type countedCommits map[string]int

func (c countedCommits) Committed(reason string) { c[reason]++ }

// TestAttemptRequests_NameTheirScopes: a mint and a commit carry the resume
// scopes their request names to the checks that hold them, and neither
// opens a budget of its own.
func TestAttemptRequests_NameTheirScopes(t *testing.T) {
	want := identity.Scopes{Resume: 5, Root: 7}
	mint := mintRequestOf(&orkstackv1.MintAttemptTokensRequest{RunId: "ork-1", TaskId: "build", Attempt: 1, ResumeScope: 5, RootScope: 7})
	commit := commitRequestOf(&orkstackv1.CommitResultRequest{RunId: "ork-1", TaskId: "build", Attempt: 1, ResumeScope: 5, RootScope: 7})
	if mint.Scopes != want || commit.Scopes != want {
		t.Fatalf("scopes: mint %+v, commit %+v; want %+v", mint.Scopes, commit.Scopes, want)
	}
	plain := mintRequestOf(&orkstackv1.MintAttemptTokensRequest{RunId: "ork-1", TaskId: "build", Attempt: 1})
	if attemptKey(mint) != attemptKey(plain) || commitAttemptKey(commit) != attemptKey(plain) {
		t.Fatal("a scope named in a request opens a budget of its own")
	}
}

// TestCommitResult_AScopeThisEngineCannotCheckIsRefused: an engine with no
// check of a run's resumes records nothing of an attempt that names one.
func TestCommitResult_AScopeThisEngineCannotCheckIsRefused(t *testing.T) {
	s, req, _ := commitService(t, committers, commitBinding("pool:main"))
	req.ResumeScope = 5
	_, err := s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(errString(err), identity.ReasonEngineRefused) {
		t.Fatalf("err = %v, want %s", err, identity.ReasonEngineRefused)
	}
}
