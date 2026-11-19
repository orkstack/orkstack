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
