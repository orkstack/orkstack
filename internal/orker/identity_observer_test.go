package orker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

// heardIdentity is an IdentityObserver that writes down what it heard.
type heardIdentity struct {
	mu    sync.Mutex
	heard []string
}

func (h *heardIdentity) add(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.heard = append(h.heard, fmt.Sprintf(format, args...))
}

func (h *heardIdentity) Refreshed(outcome string)       { h.add("refresh %s", outcome) }
func (h *heardIdentity) TokenMargin(left time.Duration) { h.add("margin %s", left) }
func (h *heardIdentity) ClockSkewed()                   { h.add("skew") }
func (h *heardIdentity) EngineRefused(how string)       { h.add("engine %s", how) }
func (h *heardIdentity) CommitWaited(w time.Duration, outcome string) {
	h.add("commit %s %s", w, outcome)
}
func (h *heardIdentity) UpstreamUnrecorded() { h.add("upstream") }

func (h *heardIdentity) want(t *testing.T, want ...string) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.heard) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(h.heard, want) {
		t.Fatalf("heard %q, want %q", h.heard, want)
	}
}

func refusedMint(code connect.Code, msg string) func() (*orkstackv1.MintAttemptTokensResponse, error) {
	return func() (*orkstackv1.MintAttemptTokensResponse, error) {
		return nil, connect.NewError(code, errors.New(msg))
	}
}

// An attempt's first tokens are no refresh: only a refusal of this Orker and
// a skew are heard of them.
func TestIdentityObserver_HearsSetup(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	heard := &heardIdentity{}
	rig.r.observe = heard
	rig.answers = append(rig.answers, refusedMint(connect.CodePermissionDenied, "who are you"),
		refusedMint(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE: later"), rig.issued(45*time.Second, "aws"))
	if f := rig.r.Setup(context.Background()); f != nil {
		t.Fatal(f)
	}
	heard.want(t, "engine refused", "skew")
}

// The refusal the setup budget ran out on is heard too.
func TestIdentityObserver_HearsTheLastRefusalOfSetup(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.r.setupBudget = 0
	heard := &heardIdentity{}
	rig.r.observe = heard
	rig.answers = append(rig.answers, refusedMint(connect.CodeUnauthenticated, "who"))
	if f := rig.r.Setup(context.Background()); f == nil || f.Reason != identity.ReasonEngineRefused {
		t.Fatalf("Setup = %v", f)
	}
	heard.want(t, "engine refused")
}

// A skew beyond the bound is heard as one within it is.
func TestIdentityObserver_HearsASkewBeyondItsBound(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	heard := &heardIdentity{}
	rig.r.observe = heard
	rig.answers = append(rig.answers, rig.issued(5*time.Minute, "aws"))
	if f := rig.r.Setup(context.Background()); f == nil || f.Reason != ReasonIdentityClockSkew {
		t.Fatalf("Setup = %v", f)
	}
	heard.want(t, "skew")
}

// Each refresh is heard once, by how it ended.
func TestIdentityObserver_HearsEachRefresh(t *testing.T) {
	for name, tc := range map[string]struct {
		answer     func(*refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error)
		bestEffort bool
		after      time.Duration
		want       []string
	}{
		"replaced": {func(r *refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) { return r.issued(0, "aws") },
			false, 5 * time.Minute, []string{"margin 5m0s", "refresh refreshed"}},
		"replaced after it expired": {func(r *refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) { return r.issued(0, "aws") },
			false, 12 * time.Minute, []string{"margin -2m0s", "refresh refreshed"}},
		"the engine is away": {func(*refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) {
			return refusedMint(connect.CodeUnavailable, "later")
		}, false, time.Minute, []string{"refresh retried"}},
		"the engine refused this Orker": {func(*refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) {
			return refusedMint(connect.CodeUnauthenticated, "who")
		}, false, time.Minute, []string{"engine refused", "refresh retried"}},
		"the engine could not read it": {func(*refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) {
			return refusedMint(connect.CodeInvalidArgument, "what")
		}, false, time.Minute, []string{"engine invalid", "refresh retried"}},
		"it ends the attempt": {func(*refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) {
			return refusedMint(connect.CodeFailedPrecondition, identity.ReasonAttemptExpired+": too late")
		}, false, time.Minute, []string{"refresh ended"}},
		"revoked": {func(*refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) {
			return refusedMint(connect.CodeFailedPrecondition, identity.ReasonRevoked+": gone")
		}, false, time.Minute, []string{"refresh ended"}},
		"revoked, best effort": {func(*refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) {
			return refusedMint(connect.CodeFailedPrecondition, identity.ReasonRevoked+": gone")
		}, true, time.Minute, []string{"refresh stopped"}},
		"a clock that disagrees": {func(r *refreshRig) func() (*orkstackv1.MintAttemptTokensResponse, error) {
			return r.issued(5*time.Minute, "aws")
		}, false, time.Minute, []string{"skew", "refresh retried"}},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newRefreshRig(t, "aws")
			rig.r.required = map[string]bool{"aws": !tc.bestEffort}
			rig.answers = append(rig.answers, rig.issued(0, "aws"))
			if f := rig.r.Setup(context.Background()); f != nil {
				t.Fatal(f)
			}
			_ = rig.clock.Sleep(context.Background(), tc.after)
			heard := &heardIdentity{}
			rig.r.observe = heard
			rig.answers = append(rig.answers, tc.answer(rig))
			_, _ = rig.r.refreshStep(context.Background())
			heard.want(t, tc.want...)
		})
	}
}

// What an attempt waited for its result to be recorded is heard once, by
// whether an engine did, with each refusal of this Orker; a preflight is no
// wait for a result.
func TestIdentityObserver_HearsACommitsWait(t *testing.T) {
	ctx := context.Background()
	down := engineAnswers(commitRefusal(connect.CodeUnavailable, "later"))
	for name, tc := range map[string]struct {
		// there is a record an engine wrote and was not heard about.
		there  func(*testing.T, *artifacts.MemoryCommitStore, *orkstackv1.ExecuteTaskInput)
		script []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error)
		want   func(waited time.Duration) []string
	}{
		"recorded": {nil, []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){down, recordedAt("s3://x")},
			func(w time.Duration) []string { return []string{fmt.Sprintf("commit %s recorded", w)} }},
		"never": {nil, []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){down},
			func(w time.Duration) []string { return []string{fmt.Sprintf("commit %s not_recorded", w)} }},
		"never heard, and there": {func(t *testing.T, store *artifacts.MemoryCommitStore, in *orkstackv1.ExecuteTaskInput) {
			uri, err := artifacts.AttestedCommitURI(in.GetCommitUri())
			must.OK(t, err)
			rec := commitOf(in)
			rec.Recorded = &orkstackv1.CommitRecording{Pool: "main", OrkerId: "orker-1"}
			plant(t, store, uri, rec)
		}, []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){down},
			func(w time.Duration) []string { return []string{fmt.Sprintf("commit %s recorded", w)} }},
		"refused, then recorded": {nil, []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
			engineAnswers(commitRefusal(connect.CodePermissionDenied, "no")), recordedAt("s3://x")},
			func(w time.Duration) []string {
				return []string{"engine refused", fmt.Sprintf("commit %s recorded", w)}
			}},
		"could not be read": {nil, []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
			engineAnswers(commitRefusal(connect.CodeInvalidArgument, "what"))},
			func(w time.Duration) []string { return []string{"engine invalid", fmt.Sprintf("commit %s refused", w)} }},
	} {
		t.Run(name, func(t *testing.T) {
			in := recordedRunInput(t, "build")
			in.Identity.CommitWaitSeconds = 60
			store := artifacts.NewMemoryCommitStore()
			if tc.there != nil {
				tc.there(t, store, in)
			}
			d, clock := recordingDispatcher(&scriptedEngine{script: tc.script}, store)
			heard := &heardIdentity{}
			d.Identities = heard
			began := clock.Boot()
			_, _ = d.commitThroughEngine(ctx, in, 1, commitOf(in))
			heard.want(t, tc.want(clock.Boot()-began)...)
		})
	}
	// One stopped while it waits is not heard.
	stopped, stop := context.WithCancel(ctx)
	waiting := recordedRunInput(t, "build")
	sd, _ := recordingDispatcher(&scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
		func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
			stop()
			return nil, commitRefusal(connect.CodeUnavailable, "later")
		}}}, artifacts.NewMemoryCommitStore())
	quiet := &heardIdentity{}
	sd.Identities = quiet
	_, _ = sd.commitThroughEngine(stopped, waiting, 1, commitOf(waiting))
	quiet.want(t)

	in := recordedRunInput(t, "build")
	d, _ := recordingDispatcher(&scriptedEngine{script: []func(*orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error){
		engineAnswers(commitRefusal(connect.CodePermissionDenied, "no"))}}, artifacts.NewMemoryCommitStore())
	heard := &heardIdentity{}
	d.Identities = heard
	_, _ = d.preflightCommit(ctx, in, 1)
	heard.want(t, "engine refused")
}

// An attempt that fails because no engine recorded its upstream is heard, as
// one that staged its upstream is not.
func TestIdentityObserver_HearsAnUnrecordedUpstream(t *testing.T) {
	ctx := context.Background()
	h := recordedCacheHarness(t)
	heard := &heardIdentity{}
	h.dispatcher.Identities = heard
	input := func(runID string) *orkstackv1.ExecuteTaskInput {
		in := recordedCachedInput(t, runID)
		in.IdempotencyKey = task.IdempotencyKeyFor("default", runID, in.GetTaskId())
		return in
	}
	produced := h.produce(t, input("ork-up-1"))
	dependent := func(uri string) error {
		in := input("ork-up-1")
		in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: in.GetTaskId(), CommitUri: uri, Values: produced.GetValuesRef(), Artifacts: produced.GetArtifacts()}}
		key, err := artifacts.CommitKey("default", "ork-up-1", "train")
		must.OK(t, err)
		in.TaskId, in.CommitUri, in.IdempotencyKey = "train", cacheRoot+"/"+key, task.IdempotencyKeyFor("default", "ork-up-1", "train")
		_, err = h.dispatcher.Execute(ctx, in)
		return err
	}
	if err := dependent(cacheRoot + "/metadata/commits/elsewhere/commit.json"); err == nil || !strings.Contains(err.Error(), ReasonUpstreamNotRecorded) {
		t.Fatalf("Execute = %v, want %s", err, ReasonUpstreamNotRecorded)
	}
	if err := dependent(produced.GetCommit().GetUri()); err != nil && strings.Contains(err.Error(), ReasonUpstreamNotRecorded) {
		t.Fatalf("Execute = %v of a recorded upstream", err)
	}
	var upstream int
	for _, what := range heard.heard {
		if what == "upstream" {
			upstream++
		}
	}
	if upstream != 1 {
		t.Fatalf("heard %q, want one unrecorded upstream", heard.heard)
	}
}

// An Orker that exports no metrics hears nothing and runs as it did.
func TestIdentityObserver_NilHearsNothing(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.answers = append(rig.answers, rig.issued(0, "aws"), refusedMint(connect.CodeUnauthenticated, "who"))
	if f := rig.r.Setup(context.Background()); f != nil {
		t.Fatal(f)
	}
	if _, f := rig.r.refreshStep(context.Background()); f != nil {
		t.Fatal(f)
	}
}
