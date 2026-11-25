package api

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// starterWorld is a service whose namespace payments holds Orkflow deploy,
// whose task holds an identity, and an access document in which binding
// "deployers" lets group corp:deployers submit it.
func starterWorld(t *testing.T, bindings ...*orkstackv1.Binding) (*Service, *triggers.MemoryStore) {
	t.Helper()
	ctx := context.Background()
	orkflows := engine.NewMemoryOrkflowStore()
	if _, _, err := orkflows.Put(ctx, identityDef("payments", "deploy", "1", "sts"), artifacts.HeadExpectation{}); err != nil {
		t.Fatal(err)
	}
	store := triggers.NewMemoryStore()
	s := &Service{orkflows: orkflows, triggers: store, logger: slog.Default()}
	roles := []*orkstackv1.Role{{Name: "submitter", Permissions: []string{"run.submit"}}}
	a, _ := accessFor(t, authn.ModeEnforce, roles, bindings...)
	s.access = a
	return s, store
}

var deployers = &orkstackv1.Binding{Name: "deployers", Role: "submitter", Scope: "namespace:payments", Subjects: []string{"group:corp:deployers"}}

// TestAsksFor_ASharedChildAsksAsItsOwners: what a binder of a schedule, a
// trigger or an event source consents to is what the owner's Orkflow asks
// for, at every depth, though the running namespace has one of each name.
// Changing the running namespace's changes nothing; changing the owner's
// changes the consent.
func TestAsksFor_ASharedChildAsksAsItsOwners(t *testing.T) {
	ctx := context.Background()
	composes := func(ns, name, refNS, child string) *orkstackv1.Orkflow {
		return &orkstackv1.Orkflow{Namespace: ns, Name: name, Version: "1", Tasks: []*orkstackv1.Task{{Id: "go", Uses: "orkflow:" + child + "@1",
			Orkflow: &orkstackv1.OrkflowRef{Namespace: refNS, Name: child, Version: "1"}}}}
	}
	asks := func(mine, theirs string) consent.Asks {
		t.Helper()
		orkflows := engine.NewMemoryOrkflowStore()
		for _, def := range []*orkstackv1.Orkflow{
			composes("payments", "release", "platform", "lib"), composes("platform", "lib", "", "child"),
			composes("payments", "lib", "", "child"),
			identityDef("payments", "child", "1", mine), identityDef("platform", "child", "1", theirs),
		} {
			if _, _, err := orkflows.Put(ctx, def, artifacts.HeadExpectation{}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := (&Service{orkflows: orkflows}).asksFor(ctx, "payments", "release", "")
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	first := asks("mine", "theirs")
	if len(first.Audiences) != 1 || first.Audiences[0] != "theirs" {
		t.Fatalf("release asks for %v; want platform's child's", first.Audiences)
	}
	if asks("other", "theirs").Digest != first.Digest {
		t.Error("the running namespace's own child changed what the shared one asks for")
	}
	if asks("mine", "other").Digest == first.Digest {
		t.Error("a change of the owner's child is not in the consent")
	}
}

// TestCheckStarter_Trigger: a trigger's run that would hold an identity is
// admitted only with a verified binder recorded with its granting binding,
// that binding still granting run.submit, and a consent to what the
// Orkflow asks for now; each missing fact is its own refusal.
func TestCheckStarter_Trigger(t *testing.T) {
	ctx := context.Background()
	ns := &orkstackv1.Namespace{Name: "payments"}
	msg := &orkstackv1.SubmitRunRequest{OrkflowName: "deploy", Namespace: "payments"}
	asks, err := (&Service{orkflows: func() *engine.MemoryOrkflowStore {
		o := engine.NewMemoryOrkflowStore()
		o.Put(ctx, identityDef("payments", "deploy", "1", "sts"), artifacts.HeadExpectation{})
		return o
	}()}).asksFor(ctx, "payments", "deploy", "")
	if err != nil {
		t.Fatal(err)
	}
	bound := triggers.Binding{Binder: "user:corp:alice", Verified: true, BoundAt: time.Now(), Consent: asks.Digest, ConsentAt: time.Now(),
		BinderBindingName: "deployers", BinderBindingSelector: "group:corp:deployers", BinderBindingRole: "submitter", BinderBindingScope: "namespace:payments"}
	for _, tc := range []struct {
		name     string
		binding  func(*triggers.Binding)
		bindings []*orkstackv1.Binding
		reason   string
		code     connect.Code
	}{
		{"admitted", nil, []*orkstackv1.Binding{deployers}, "", 0},
		{"an asserted binder", func(b *triggers.Binding) { b.Verified = false }, []*orkstackv1.Binding{deployers}, reasonStarterNotRecorded, connect.CodeFailedPrecondition},
		{"no binding recorded", func(b *triggers.Binding) { b.BinderBindingName = "" }, []*orkstackv1.Binding{deployers}, reasonStarterNotRecorded, connect.CodeFailedPrecondition},
		{"the binding removed", nil, nil, reasonStarterNotPermitted, connect.CodePermissionDenied},
		{"another consent", func(b *triggers.Binding) { b.Consent = "c1:" + strings.Repeat("0", 64) }, []*orkstackv1.Binding{deployers}, reasonStarterNotConsented, connect.CodeFailedPrecondition},
		{"no consent", func(b *triggers.Binding) { b.Consent = "" }, []*orkstackv1.Binding{deployers}, reasonStarterNotConsented, connect.CodeFailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, store := starterWorld(t, tc.bindings...)
			b := bound
			if tc.binding != nil {
				tc.binding(&b)
			}
			tr, _, err := store.Create(ctx, triggers.Registration{Namespace: "payments", OrkflowName: "deploy", Binding: b})
			if err != nil {
				t.Fatal(err)
			}
			err = s.checkStarter(ctx, ns, msg, &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: tr.ID})
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("admitted: %v", err)
				}
				return
			}
			if connect.CodeOf(err) != tc.code || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("err = %v, want %s %s", err, tc.code, tc.reason)
			}
			if status, _ := triggerSubmitStatus(err); status != 403 {
				t.Fatalf("an invoke answers %d, want 403", status)
			}
		})
	}
}

// TestBinderBindingFor: a save records the binding that granted a verified
// caller run.submit, by group as by name; in asserted mode, or for a caller
// nothing grants, none is recorded.
func TestBinderBindingFor(t *testing.T) {
	s, _ := starterWorld(t, deployers)
	alice := &authn.Principal{Kind: authn.KindUser, ID: "user:corp:alice", Issuer: "corp", Groups: []string{"deployers"}, Method: authn.MethodJWT}
	ctx := authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce), alice)
	got, err := s.binderBindingFor(ctx, "payments", "deploy")
	if err != nil || got == nil || *got != (artifacts.BinderBinding{Name: "deployers", Selector: "group:corp:deployers", Role: "submitter", Scope: "namespace:payments"}) {
		t.Fatalf("recorded %+v", got)
	}
	if got, _ := s.binderBindingFor(authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeAsserted), alice), "payments", "deploy"); got != nil {
		t.Fatalf("asserted mode recorded %+v", got)
	}
	bob := &authn.Principal{Kind: authn.KindUser, ID: "user:corp:bob", Issuer: "corp", Method: authn.MethodJWT}
	if got, _ := s.binderBindingFor(authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce), bob), "payments", "deploy"); got != nil {
		t.Fatalf("a caller nothing grants recorded %+v", got)
	}
}

// TestStarterRefusal_TheInvokeIsToldOnlyTheReason: a starter refusal is the
// invoke's 403, recognized by its leading reason, and nothing else is.
func TestStarterRefusal_TheInvokeIsToldOnlyTheReason(t *testing.T) {
	refused := connect.NewError(connect.CodePermissionDenied, errors.New(reasonStarterNotPermitted+": binder user:corp:alice may no longer submit"))
	if reason, ok := starterRefusal(refused); !ok || reason != reasonStarterNotPermitted {
		t.Fatalf("starterRefusal = %q, %v", reason, ok)
	}
	other := connect.NewError(connect.CodePermissionDenied, errors.New("PERMISSION_DENIED: a message that names IDENTITY_STARTER_NOT_RECORDED in passing"))
	if _, ok := starterRefusal(other); ok {
		t.Fatal("a refusal that only mentions a starter reason was taken for one")
	}
}

// TestBinderBindingFor_AnUnreadableDocument: in enforce mode a save that
// would bind is refused unavailable when the access document cannot be
// read; in observe mode, which never changes an answer, it is recorded
// with no binding.
func TestBinderBindingFor_AnUnreadableDocument(t *testing.T) {
	// A document that names a built-in role cannot be compiled: it is
	// unreadable, as an outage leaves it.
	broken := []*orkstackv1.Role{{Name: "orker", Permissions: []string{"run.submit"}}}
	alice := &authn.Principal{Kind: authn.KindUser, ID: "user:corp:alice", Issuer: "corp", Method: authn.MethodJWT}
	for mode, wantErr := range map[string]bool{authn.ModeEnforce: true, authn.ModeObserve: false} {
		s, _ := starterWorld(t)
		a, _ := accessFor(t, mode, broken, deployers)
		s.access = a
		got, err := s.binderBindingFor(authn.WithPrincipal(authn.WithMode(context.Background(), mode), alice), "payments", "deploy")
		if got != nil || (err != nil) != wantErr || (wantErr && connect.CodeOf(err) != connect.CodeUnavailable) {
			t.Errorf("%s: %+v, %v", mode, got, err)
		}
	}
}
