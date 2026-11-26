package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// scriptedResets answers each ResetConcurrencyLimit with the next step, the
// last one again once they run out, and keeps what it was asked.
type scriptedResets struct {
	orkstackv1connect.UnimplementedConcurrencyAdminServiceHandler
	mu    sync.Mutex
	steps []resetStep
	asked []*orkstackv1.ResetConcurrencyLimitRequest
}

type resetStep struct {
	resp *orkstackv1.ResetConcurrencyLimitResponse
	err  error
}

func (s *scriptedResets) ResetConcurrencyLimit(_ context.Context, req *connect.Request[orkstackv1.ResetConcurrencyLimitRequest]) (*connect.Response[orkstackv1.ResetConcurrencyLimitResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	step := s.steps[min(len(s.asked), len(s.steps)-1)]
	s.asked = append(s.asked, req.Msg)
	if step.err != nil {
		return nil, step.err
	}
	return connect.NewResponse(step.resp), nil
}

func runReset(t *testing.T, s *scriptedResets, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	old := resetPollInterval
	resetPollInterval = time.Millisecond
	t.Cleanup(func() { resetPollInterval = old })
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewConcurrencyAdminServiceHandler(s))
	server := api.NewServer("127.0.0.1:0", mux)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must.OKf(t, err, "listen")
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	var out, errOut bytes.Buffer
	code = Execute(append([]string{"--server", "http://" + listener.Addr().String(), "admin", "concurrency", "reset", "db:prod"}, args...), &out, &errOut)
	return out.String(), errOut.String(), code
}

func resetAnswer(state orkstackv1.ConcurrencyResetState, consequence string) resetStep {
	return resetStep{resp: &orkstackv1.ResetConcurrencyLimitResponse{State: state, Consequence: consequence}}
}

// TestAdminConcurrencyReset_ExitStatus: completed and a dry run are 0, a
// refusal 5, running at the timeout 4 and named for asking again, and every
// other ending 1 with its reason; an engine that cannot be asked is asked
// again, by the same request.
func TestAdminConcurrencyReset_ExitStatus(t *testing.T) {
	running := resetAnswer(orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_RUNNING, "what it does")
	pending := resetAnswer(orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_RUNNING, "")
	pending.resp.Note = "the limit is not answering yet"
	completed := resetAnswer(orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_COMPLETED, "")
	ended := func(state orkstackv1.ConcurrencyResetState, code string) resetStep {
		return resetStep{resp: &orkstackv1.ResetConcurrencyLimitResponse{State: state, ReasonCode: code, Reason: "why"}}
	}
	for name, c := range map[string]struct {
		steps       []resetStep
		args        []string
		code, calls int
		out, errOut string
	}{
		"completed":   {steps: []resetStep{running, pending, completed}, code: exitOK, calls: 3, out: "what it does\nThe limit's record is rebuilt.\n"},
		"asked again": {steps: []resetStep{{err: connect.NewError(connect.CodeUnavailable, errors.New("down"))}, running, completed}, code: exitOK, calls: 3},
		"dry run":     {steps: []resetStep{resetAnswer(orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_DRY_RUN, "what it would do")}, args: []string{"--dry-run"}, code: exitOK, calls: 1, out: "what it would do\nDry run: nothing was changed.\n"},
		"refused":     {steps: []resetStep{{err: connect.NewError(connect.CodeFailedPrecondition, errors.New("CONCURRENCY_RESET_UNANSWERED: no"))}}, code: exitResetRefused, calls: 1, errOut: "CONCURRENCY_RESET_UNANSWERED: no"},
		"still runs":  {steps: []resetStep{pending}, args: []string{"--timeout", "0s"}, code: exitResetTimedOut, calls: 1, errOut: "the limit is not answering yet"},
		"failed":      {steps: []resetStep{running, ended(orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_FAILED, "CONCURRENCY_RESET_FAILED")}, code: exitError, calls: 2, errOut: "CONCURRENCY_RESET_FAILED: why"},
		"interrupted": {steps: []resetStep{ended(orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_INTERRUPTED, "CONCURRENCY_RESET_INTERRUPTED")}, code: exitError, calls: 1, errOut: "CONCURRENCY_RESET_INTERRUPTED: why"},
		"not applied": {steps: []resetStep{ended(orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_NOT_APPLIED, "CONCURRENCY_RESET_NOT_APPLIED")}, code: exitError, calls: 1, errOut: "CONCURRENCY_RESET_NOT_APPLIED: why"},
		"not allowed": {steps: []resetStep{{err: connect.NewError(connect.CodePermissionDenied, errors.New("no"))}}, code: exitError, calls: 1, errOut: "permission_denied"},
		"half a name": {steps: []resetStep{completed}, args: []string{"--request-id", "a"}, code: exitError, errOut: "name a request together"},
		"a bad time":  {steps: []resetStep{completed}, args: []string{"--request-id", "a", "--issued-at", "today"}, code: exitError, errOut: "not an RFC 3339 time"},
	} {
		t.Run(name, func(t *testing.T) {
			s := &scriptedResets{steps: c.steps}
			out, errOut, code := runReset(t, s, c.args...)
			if code != c.code || len(s.asked) != c.calls || (c.out != "" && out != c.out) || !strings.Contains(errOut, c.errOut) {
				t.Fatalf("exit %d after %d calls\nstdout %q\nstderr %q; want %d after %d, %q, %q", code, len(s.asked), out, errOut, c.code, c.calls, c.out, c.errOut)
			}
			for _, r := range s.asked {
				if r.GetKey() != "db:prod" || (!r.GetDryRun() && (r.GetRequestId() != s.asked[0].GetRequestId() || !r.GetIssuedAt().AsTime().Equal(s.asked[0].GetIssuedAt().AsTime()))) {
					t.Errorf("asked %v; want the one request again", r)
				}
			}
			if c.code == exitResetTimedOut {
				want := "--request-id " + s.asked[0].GetRequestId() + " --issued-at " + s.asked[0].GetIssuedAt().AsTime().Format(time.RFC3339Nano)
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr %q; want it to name %q", errOut, want)
				}
			}
		})
	}
}

// TestAdminConcurrencyReset_AsksTheRequestItWasGiven: the id and time it
// printed, the epoch only when it was passed, and --force-unanswered.
func TestAdminConcurrencyReset_AsksTheRequestItWasGiven(t *testing.T) {
	s := &scriptedResets{steps: []resetStep{resetAnswer(orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_COMPLETED, "")}}
	at := time.Date(2026, 11, 25, 9, 0, 0, 5, time.UTC)
	_, errOut, code := runReset(t, s, "--request-id", "rst-a", "--issued-at", at.Format(time.RFC3339Nano), "--expected-epoch", "0", "--force-unanswered")
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if r := s.asked[0]; r.GetRequestId() != "rst-a" || !r.GetIssuedAt().AsTime().Equal(at) || r.ExpectedEpoch == nil || r.GetExpectedEpoch() != 0 || !r.GetForceUnanswered() {
		t.Fatalf("asked %v", r)
	}
	req := &orkstackv1.ResetConcurrencyLimitRequest{}
	must.OK(t, nameReset(req, "", "", at))
	if !strings.HasPrefix(req.GetRequestId(), "rst-") || len(req.GetRequestId()) > 48 || !req.GetIssuedAt().AsTime().Equal(at) {
		t.Fatalf("a new request = %v", req)
	}
	other := &orkstackv1.ResetConcurrencyLimitRequest{}
	must.OK(t, nameReset(other, "", "", at))
	if other.GetRequestId() == req.GetRequestId() {
		t.Fatal("two new requests share an id")
	}
}
