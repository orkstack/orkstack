package orker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestJudgeIdentitySupport: an Orker supports task identity only while the
// engine's answer names its own Orker id and grants it, at its pool, every
// permission an Orker's calls need, and its storage credential is scoped;
// each missing fact is named.
func TestJudgeIdentitySupport(t *testing.T) {
	grants := func(scope string, perms ...string) *orkstackv1.GetSigninResponse {
		return &orkstackv1.GetSigninResponse{OrkerId: "o1", Permissions: []*orkstackv1.ScopedPermissions{{Scope: scope, Permissions: perms}}}
	}
	both := []string{"identity.token.mint", "task.result.commit"}
	scoped := orkstackv1.StoreScope_STORE_SCOPE_SCOPED
	for _, tc := range []struct {
		name   string
		who    *orkstackv1.GetSigninResponse
		scope  orkstackv1.StoreScope
		reason string
	}{
		{"at its pool", grants("pool:gpu", both...), scoped, ""},
		{"at the deployment", grants("deployment", both...), scoped, ""},
		{"the default pool, unnamed", grants("pool:main", both...), scoped, ""},
		{"no answer", nil, scoped, "the engine has not answered"},
		{"another Orker", &orkstackv1.GetSigninResponse{OrkerId: "o2", Permissions: grants("pool:gpu", both...).Permissions}, scoped, "names Orker"},
		{"no Orker id", &orkstackv1.GetSigninResponse{Permissions: grants("pool:gpu", both...).Permissions}, scoped, "names no Orker"},
		{"another pool", grants("pool:main", both...), scoped, `at pool "gpu"`},
		{"mint only", grants("pool:gpu", "identity.token.mint"), scoped, "task.result.commit"},
		{"unscoped", grants("pool:gpu", both...), orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED, "storage credential"},
		{"no verdict", grants("pool:gpu", both...), orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN, "storage credential"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := "gpu"
			if strings.HasPrefix(tc.name, "the default pool") {
				pool = ""
			}
			ok, reason := judgeIdentitySupport(tc.who, "o1", pool, tc.scope)
			if ok != (tc.reason == "") || !strings.Contains(reason, tc.reason) {
				t.Fatalf("judge = %v %q, want reason %q", ok, reason, tc.reason)
			}
		})
	}
}

type answers struct {
	who *orkstackv1.GetSigninResponse
	err error
}

func (a *answers) Whoami(context.Context) (*orkstackv1.GetSigninResponse, error) { return a.who, a.err }

// TestIdentitySupport_LosesAndRegains: the check advertises support after
// an answer that allows it, loses it (calling onLost once) when the engine
// stops answering or the verdict lapses, and regains it.
func TestIdentitySupport_LosesAndRegains(t *testing.T) {
	ok := &orkstackv1.GetSigninResponse{OrkerId: "o1", Permissions: []*orkstackv1.ScopedPermissions{{Scope: "pool:gpu", Permissions: identityPermissions}}}
	engine := &answers{who: ok}
	scope := orkstackv1.StoreScope_STORE_SCOPE_SCOPED
	lost := 0
	s := NewIdentitySupport(engine, "o1", "gpu", func() orkstackv1.StoreScope { return scope }, nil, func() { lost++ })
	if supported, _ := s.Report(); supported {
		t.Fatal("supported before the engine answered")
	}
	s.Step(context.Background())
	if supported, _ := s.Report(); !supported {
		t.Fatal("not supported after an answer that allows it")
	}
	engine.who, engine.err = nil, errors.New("engine down")
	s.Step(context.Background())
	if supported, reason := s.Report(); supported || lost != 1 || !strings.Contains(reason, "not answered") {
		t.Fatalf("after the engine went away: %v %q lost=%d", supported, reason, lost)
	}
	engine.who, engine.err = ok, nil
	s.Step(context.Background())
	scope = orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN
	if supported, _ := s.Report(); supported {
		t.Fatal("a lapsed storage verdict kept support")
	}
}

// TestIdentitySupport_AStaleAnswerGrantsNothing: an answer older than the
// stale limit, as a check that stopped answering leaves, grants nothing.
func TestIdentitySupport_AStaleAnswerGrantsNothing(t *testing.T) {
	ok := &orkstackv1.GetSigninResponse{OrkerId: "o1", Permissions: []*orkstackv1.ScopedPermissions{{Scope: "pool:gpu", Permissions: identityPermissions}}}
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	s := NewIdentitySupport(&answers{who: ok}, "o1", "gpu", func() orkstackv1.StoreScope { return orkstackv1.StoreScope_STORE_SCOPE_SCOPED }, nil, nil)
	s.now = func() time.Time { return now }
	s.Step(context.Background())
	if supported, _ := s.Report(); !supported {
		t.Fatal("a fresh answer does not grant")
	}
	now = now.Add(identitySupportStale + time.Second)
	if supported, _ := s.Report(); supported {
		t.Fatal("a stale answer grants")
	}
}

// hangsAfter answers once, then hangs until its context ends.
type hangsAfter struct {
	first *orkstackv1.GetSigninResponse
	asked int
}

func (h *hangsAfter) Whoami(ctx context.Context) (*orkstackv1.GetSigninResponse, error) {
	h.asked++
	if h.asked == 1 {
		return h.first, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestIdentitySupport_AHungEngineIsNotAnswered: a question the engine does
// not answer ends at the check's own bound, while the Orker runs on, and the
// good answer before it no longer grants.
func TestIdentitySupport_AHungEngineIsNotAnswered(t *testing.T) {
	ok := &orkstackv1.GetSigninResponse{OrkerId: "o1", Permissions: []*orkstackv1.ScopedPermissions{{Scope: "pool:gpu", Permissions: identityPermissions}}}
	s := NewIdentitySupport(&hangsAfter{first: ok}, "o1", "gpu", func() orkstackv1.StoreScope { return orkstackv1.StoreScope_STORE_SCOPE_SCOPED }, nil, nil)
	s.ask = 50 * time.Millisecond
	s.Step(context.Background())
	if supported, _ := s.Report(); !supported {
		t.Fatal("the first answer does not grant")
	}
	done := make(chan struct{})
	go func() { s.Step(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a question the engine does not answer was not bounded")
	}
	if supported, _ := s.Report(); supported {
		t.Fatal("a hung engine left the earlier answer granting")
	}
}

// datedAnswers is answers whose engine also says what time it is, as an
// HTTP Date header would; date zero is a header that was missing or could
// not be read.
type datedAnswers struct {
	answers
	date time.Time
}

func (a *datedAnswers) WhoamiDated(context.Context) (*orkstackv1.GetSigninResponse, time.Time, error) {
	return a.who, a.date, a.err
}

// TestIdentitySupport_ThreeSkewedMintsAndTheDateHeader: three mints in a row
// whose issued_at disagreed with this Orker's clock clear its support; a
// mint that agreed in between starts the count again; once unfit it asks
// every two minutes, and an engine answer whose Date is within the bound
// plus a second restores support, while a missing or unreadable Date, or one
// still apart, is no evidence.
func TestIdentitySupport_ThreeSkewedMintsAndTheDateHeader(t *testing.T) {
	ok := &orkstackv1.GetSigninResponse{OrkerId: "o1", Permissions: []*orkstackv1.ScopedPermissions{{Scope: "pool:gpu", Permissions: identityPermissions}}}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	engine := &datedAnswers{answers: answers{who: ok}, date: now}
	s := NewIdentitySupport(engine, "o1", "gpu", func() orkstackv1.StoreScope { return orkstackv1.StoreScope_STORE_SCOPE_SCOPED }, nil, nil)
	s.now = func() time.Time { return now }
	s.SetMaxClockSkew(2 * time.Minute)
	s.Step(context.Background())
	if ok, _ := s.Report(); !ok {
		t.Fatal("not supported at first")
	}
	s.ClockSkewed()
	s.ClockSkewed()
	s.ClockAgreed()
	s.ClockSkewed()
	s.ClockSkewed()
	if ok, _ := s.Report(); !ok {
		t.Fatal("an agreeing mint did not start the count again")
	}
	s.ClockSkewed()
	if ok, why := s.Report(); ok || !strings.Contains(why, "clock") {
		t.Fatalf("three skewed mints in a row: %v %q", ok, why)
	}
	if got := s.interval(); got != 2*time.Minute {
		t.Fatalf("unfit interval %v", got)
	}
	for name, tc := range map[string]struct {
		date time.Time
		says string
	}{"no Date": {time.Time{}, "no readable Date"}, "still apart": {now.Add(-3 * time.Minute), "3m0s from"}} {
		engine.date = tc.date
		s.Step(context.Background())
		if ok, why := s.Report(); ok || !strings.Contains(why, tc.says) {
			t.Fatalf("%s: %v %q", name, ok, why)
		}
	}
	engine.date = now.Add(2*time.Minute + 500*time.Millisecond)
	s.Step(context.Background())
	if ok, _ := s.Report(); !ok {
		t.Fatal("a Date within the bound plus a second did not restore support")
	}
	if got := s.interval(); got != IdentitySupportInterval {
		t.Fatalf("fit interval %v", got)
	}
}

// TestParseEngineDate: the Date header is read as HTTP time; absent or
// unreadable is zero, never now.
func TestParseEngineDate(t *testing.T) {
	if got := parseEngineDate("Thu, 20 Nov 2026 12:00:00 GMT"); !got.Equal(time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("= %v", got)
	}
	for _, bad := range []string{"", "yesterday", "2026-11-20T12:00:00Z"} {
		if got := parseEngineDate(bad); !got.IsZero() {
			t.Errorf("%q = %v", bad, got)
		}
	}
}

// TestIdentitySupport_DefaultsItsSkewBound: a check built without the
// setting still holds the engine's Date to the default bound, so it can
// recover.
func TestIdentitySupport_DefaultsItsSkewBound(t *testing.T) {
	s := NewIdentitySupport(&answers{}, "o1", "gpu", func() orkstackv1.StoreScope { return orkstackv1.StoreScope_STORE_SCOPE_SCOPED }, nil, nil)
	if s.maxSkew != DefaultIdentityMaxClockSkew {
		t.Fatalf("maxSkew %v", s.maxSkew)
	}
}
