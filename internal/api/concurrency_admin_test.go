package api_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// answeringResetter answers every reset with one result or refusal, and keeps
// what it was asked. What a limit decides is the engine's own test.
type answeringResetter struct {
	res   engine.ConcurrencyResetResult
	err   error
	asked []engine.ConcurrencyReset
}

func (r *answeringResetter) ResetConcurrencyLimit(_ context.Context, in engine.ConcurrencyReset) (engine.ConcurrencyResetResult, error) {
	r.asked = append(r.asked, in)
	return r.res, r.err
}

func resetService(r *answeringResetter) *api.Service {
	return api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: newFakeRegistry(), ConcurrencyResetter: r})
}

// TestResetConcurrencyLimit_MapsTheRequestAndTheAnswer: each field goes over,
// each state and consequence as its own.
func TestResetConcurrencyLimit_MapsTheRequestAndTheAnswer(t *testing.T) {
	at := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	epoch := int64(3)
	states := map[string]orkstackv1.ConcurrencyResetState{
		engine.ResetStateDryRun:      orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_DRY_RUN,
		engine.ResetStateRunning:     orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_RUNNING,
		engine.ResetStateCompleted:   orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_COMPLETED,
		engine.ResetStateFailed:      orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_FAILED,
		engine.ResetStateInterrupted: orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_INTERRUPTED,
		engine.ResetStateNotApplied:  orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_NOT_APPLIED,
	}
	for state, want := range states {
		r := &answeringResetter{res: engine.ConcurrencyResetResult{State: state, Epoch: 3, NewEpoch: 4, Holders: 1, Queued: 2, Dropped: 3,
			Adopted: 4, Seeded: 5, ConsequenceCode: engine.ResetAnswering, Consequence: "c", Code: "R", Reason: "why", Note: "n"}}
		resp, err := resetService(r).ResetConcurrencyLimit(context.Background(), connect.NewRequest(&orkstackv1.ResetConcurrencyLimitRequest{
			Key: "db:prod", RequestId: "a", IssuedAt: timestamppb.New(at), ExpectedEpoch: &epoch, ForceUnanswered: true}))
		must.OK(t, err)
		m := resp.Msg
		if m.GetState() != want || m.GetRequestId() != "a" || m.GetEpoch() != 3 || m.GetNewEpoch() != 4 || m.GetHolders() != 1 || m.GetQueued() != 2 ||
			m.GetDropped() != 3 || m.GetAdopted() != 4 || m.GetSeeded() != 5 || m.GetConsequence() != "c" || m.GetReasonCode() != "R" ||
			m.GetReason() != "why" || m.GetNote() != "n" || m.GetConsequenceCode() != orkstackv1.ConcurrencyResetConsequence_CONCURRENCY_RESET_CONSEQUENCE_ANSWERING {
			t.Errorf("%s = %v", state, m)
		}
		in := r.asked[0]
		if in.Namespace != "default" || in.Key != "db:prod" || in.RequestID != "a" || !in.IssuedAt.Equal(at) || in.ExpectedEpoch == nil ||
			*in.ExpectedEpoch != 3 || !in.ForceUnanswered || in.DryRun {
			t.Errorf("asked %+v", in)
		}
	}
	if len(orkstackv1.ConcurrencyResetState_name) != len(states)+1 {
		t.Fatalf("%d states on the wire, %d mapped", len(orkstackv1.ConcurrencyResetState_name)-1, len(states))
	}
}

// TestResetConcurrencyLimit_Refuses: a request that names no reset, or a key
// that is none, is malformed; a refusal opens with its code, and one the same
// request may be asked again after is UNAVAILABLE.
func TestResetConcurrencyLimit_Refuses(t *testing.T) {
	now := timestamppb.Now()
	for name, req := range map[string]*orkstackv1.ResetConcurrencyLimitRequest{
		"no key":          {RequestId: "a", IssuedAt: now},
		"a reserved key":  {Key: "ork:mine", RequestId: "a", IssuedAt: now},
		"no request id":   {Key: "k", IssuedAt: now},
		"a long id":       {Key: "k", RequestId: strings.Repeat("a", engine.ResetRequestIDMax+1), IssuedAt: now},
		"an id with text": {Key: "k", RequestId: "a b", IssuedAt: now},
		"no time":         {Key: "k", RequestId: "a"},
	} {
		r := &answeringResetter{}
		if _, err := resetService(r).ResetConcurrencyLimit(context.Background(), connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument || len(r.asked) != 0 {
			t.Errorf("%s = %v, asked %d times; want invalid_argument and none", name, err, len(r.asked))
		}
	}
	ask := func(r *answeringResetter, namespace string) error {
		_, err := resetService(r).ResetConcurrencyLimit(context.Background(), connect.NewRequest(
			&orkstackv1.ResetConcurrencyLimitRequest{Namespace: namespace, Key: "k", RequestId: "a", IssuedAt: now}))
		return err
	}
	if err := ask(&answeringResetter{}, "typo"); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("a namespace that does not exist = %v; want not_found", err)
	}
	for _, c := range []struct {
		err  error
		code connect.Code
		text string
	}{
		{&engine.ConcurrencyResetError{Code: engine.ResetConflictCode, Reason: "changed"}, connect.CodeFailedPrecondition, "CONCURRENCY_RESET_CONFLICT: changed"},
		{&engine.ConcurrencyResetError{Code: engine.ResetRefusedCode, Reason: "busy", Retryable: true}, connect.CodeUnavailable, "CONCURRENCY_RESET_REFUSED: busy"},
		{errors.New("raw cluster words"), connect.CodeUnavailable, "the limit could not be reset; ask again with the same request id"},
	} {
		err := ask(&answeringResetter{err: c.err}, "")
		var got *connect.Error
		if !errors.As(err, &got) || got.Code() != c.code || got.Message() != c.text {
			t.Errorf("%v = %v; want %v %q", c.err, err, c.code, c.text)
		}
	}
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: newFakeRegistry()})
	_, err := svc.ResetConcurrencyLimit(context.Background(), connect.NewRequest(&orkstackv1.ResetConcurrencyLimitRequest{Key: "k", DryRun: true}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("an engine with no cluster = %v; want unavailable", err)
	}
	// A dry run needs no name.
	_, err = resetService(&answeringResetter{res: engine.ConcurrencyResetResult{State: engine.ResetStateDryRun}}).ResetConcurrencyLimit(context.Background(),
		connect.NewRequest(&orkstackv1.ResetConcurrencyLimitRequest{Key: "k", DryRun: true}))
	must.OK(t, err)
}

// TestResetConcurrencyLimit_AuditsWhatStartedOrWasRefused: the call that
// started a reset is recorded with what it found, a refused one with its code;
// a dry run and a poll are not.
func TestResetConcurrencyLimit_AuditsWhatStartedOrWasRefused(t *testing.T) {
	store := artifacts.NewMemoryAuditStore()
	r := &answeringResetter{}
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: newFakeRegistry(), ConcurrencyResetter: r, AuditStore: store})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	t.Cleanup(server.Close)
	c := orkstackv1connect.NewConcurrencyAdminServiceClient(server.Client(), server.URL)
	ask := func(dry bool) {
		_, _ = c.ResetConcurrencyLimit(context.Background(), connect.NewRequest(&orkstackv1.ResetConcurrencyLimitRequest{
			Key: "db:prod", RequestId: "a", IssuedAt: timestamppb.Now(), DryRun: dry}))
	}
	r.res = engine.ConcurrencyResetResult{State: engine.ResetStateDryRun, ConsequenceCode: engine.ResetAnswering}
	ask(true)
	r.res = engine.ConcurrencyResetResult{State: engine.ResetStateRunning, Epoch: 3, NewEpoch: 4, Holders: 1, Queued: 2, ConsequenceCode: engine.ResetAnswering}
	ask(false)
	r.res = engine.ConcurrencyResetResult{State: engine.ResetStateRunning, Epoch: 4}
	ask(false)
	r.res = engine.ConcurrencyResetResult{State: engine.ResetStateCompleted}
	ask(false)
	r.res = engine.ConcurrencyResetResult{State: engine.ResetStateNotApplied}
	ask(false)
	r.res, r.err = engine.ConcurrencyResetResult{}, &engine.ConcurrencyResetError{Code: engine.ResetUnansweredCode, Reason: "no"}
	ask(false)
	page, err := store.ListAuditRecords(context.Background(), artifacts.AuditQuery{Since: time.Now().Add(-time.Hour), Until: time.Now().Add(time.Hour), Limit: 10})
	must.OK(t, err)
	if len(page.Records) != 2 {
		t.Fatalf("%d records; want the start's and the refusal's: %v", len(page.Records), page.Records)
	}
	refused, started := page.Records[0], page.Records[1]
	if started.GetAction() != "concurrency.reset.requested" || !started.GetSucceeded() || started.GetTarget() != "db:prod" || started.GetNamespace() != "default" ||
		started.GetDetail() != "request a: force_unanswered=false expected_epoch=any epoch=3 new_epoch=4 holders=1 queued=2 dropped=0" {
		t.Errorf("started = %v", started)
	}
	if refused.GetAction() != "concurrency.reset.requested" || refused.GetSucceeded() || refused.GetDetail() != "request a: force_unanswered=false expected_epoch=any" {
		t.Errorf("refused = %v", refused)
	}
}

// TestResetConcurrencyLimit_OfAnOrkflowsOwnKey: the key of an Orkflow that
// declares none is reset as any other is.
func TestResetConcurrencyLimit_OfAnOrkflowsOwnKey(t *testing.T) {
	r := &answeringResetter{res: engine.ConcurrencyResetResult{State: engine.ResetStateDryRun}}
	key := engine.DefaultRunConcurrencyKey("default", "migrate")
	_, err := resetService(r).ResetConcurrencyLimit(context.Background(), connect.NewRequest(&orkstackv1.ResetConcurrencyLimitRequest{Key: key, DryRun: true}))
	must.OK(t, err)
	if len(r.asked) != 1 || r.asked[0].Key != key {
		t.Fatalf("asked %+v; want %s", r.asked, key)
	}
}
