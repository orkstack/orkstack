package authz_test

import (
	"slices"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
)

// TestConfiguration_Orkers: auth.orkers binds each subject to the orker
// role at its pool by configuration, as auth.admins binds admin.
func TestConfiguration_Orkers(t *testing.T) {
	cfg := authz.Configuration{Orkers: []authz.OrkerEntry{
		{Subject: "cert:spiffe://acme.dev/orker/gpu-*", Pool: "gpu"},
		{Subject: "serviceaccount:orker-main", Pool: "main", OrkerIDsAny: true},
	}}
	p := mustCompile(t, nil, cfg)
	gpu := &authz.Subject{ID: "cert:spiffe://acme.dev/orker/gpu-1", Kind: "service", Issuer: "cert"}
	if _, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermTaskResultCommit), Subject: gpu, Resource: authz.Resource{Pool: "gpu"}, Policy: p}); err != nil {
		t.Fatalf("a gpu Orker at pool gpu: %v", err)
	}
	if _, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermTaskResultCommit), Subject: gpu, Resource: authz.Resource{Pool: "main"}, Policy: p}); err == nil {
		t.Fatal("a gpu Orker was admitted at pool main")
	}
	var anyIDs, configured int
	for _, b := range p.Bindings() {
		if b.FromConfiguration && b.Role == authz.RoleOrker {
			configured++
			if b.OrkerIDsAny {
				anyIDs++
			}
		}
	}
	if configured != 2 || anyIDs != 1 {
		t.Fatalf("configured orker bindings %d (any ids %d); want 2 (1)", configured, anyIDs)
	}
	for name, entries := range map[string][]authz.OrkerEntry{
		"a subject that does not parse": {{Subject: "robot:x", Pool: "gpu"}},
		"a pool that is not a name":     {{Subject: "serviceaccount:o", Pool: "GPU!"}},
		"no pool":                       {{Subject: "serviceaccount:o"}},
		"one entry twice":               {{Subject: "serviceaccount:o", Pool: "gpu"}, {Subject: "serviceaccount:o", Pool: "gpu"}},
	} {
		if _, err := authz.CompileConfiguration(authz.Configuration{Orkers: entries}); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestOrkerIDsAny_HostsNoEventSource: a binding that accepts any Orker id
// grants everything else the orker role holds, and never eventsource.host,
// by whichever way it is asked and wherever the binding comes from. Another
// binding of the same principal grants it as ever.
func TestOrkerIDsAny_HostsNoEventSource(t *testing.T) {
	const subject = "serviceaccount:orker-main"
	sub := &authz.Subject{ID: subject, Kind: "service", Issuer: "serviceaccount"}
	at := authz.Resource{Pool: "main"}
	stored := func(anyID bool) *orkstackv1.Binding {
		b := bind("b", authz.RoleOrker, "pool:main", subject)
		b.OrkerIdsAny = anyID
		return b
	}
	configured := func(anyID bool) authz.Configuration {
		return authz.Configuration{Orkers: []authz.OrkerEntry{{Subject: subject, Pool: "main", OrkerIDsAny: anyID}}}
	}
	for name, c := range map[string]struct {
		doc    *orkstackv1.AccessDocument
		config authz.Configuration
		hosts  bool
		shared bool
	}{
		"stored, any id":                   {doc(nil, stored(true)), authz.Configuration{}, false, true},
		"stored, its own id":               {doc(nil, stored(false)), authz.Configuration{}, true, false},
		"configured, any id":               {nil, configured(true), false, true},
		"configured, its own id":           {nil, configured(false), true, false},
		"any id, and its own id elsewhere": {doc(nil, stored(false)), configured(true), true, true},
	} {
		t.Run(name, func(t *testing.T) {
			p := mustCompile(t, c.doc, c.config)
			for _, perm := range []authz.Permission{authz.PermEventSourceHost, authz.PermIdentityTokenMint, authz.PermTaskResultCommit, authz.PermPayloadKeyUse} {
				want := c.hosts || perm != authz.PermEventSourceHost
				d, err := authz.Authorize(authz.Request{Action: authz.Action(perm), Subject: sub, Resource: at, Policy: p})
				if (err == nil) != want {
					t.Errorf("Authorize %s: %v; granted should be %v", perm, err, want)
				}
				if ex := authz.Explain(p, sub, perm, at); (ex.Err == nil) != want || (ex.GrantedBy != nil) != want ||
					slices.ContainsFunc(ex.Considered, func(e authz.Explained) bool { return e.Match == authz.MatchGranted }) != want {
					t.Errorf("Explain %s: %v, granted by %+v of %+v", perm, ex.Err, ex.GrantedBy, ex.Considered)
				}
				if shared := slices.ContainsFunc(authz.Explain(p, sub, perm, at).Considered, func(e authz.Explained) bool { return e.Match == authz.MatchSharedOrkerCredential }); shared != (perm == authz.PermEventSourceHost && c.shared) {
					t.Errorf("Explain %s says a shared credential's binding is one: %v", perm, shared)
				}
				if _, ok := p.GrantingBinding(sub, perm, at); ok != want {
					t.Errorf("GrantingBinding %s: %v", perm, ok)
				}
				if got := slices.ContainsFunc(p.Holdings(sub), func(h authz.Holding) bool { return slices.Contains(h.Permissions, perm) }); got != want {
					t.Errorf("Holdings show %s: %v", perm, got)
				}
				if got := p.HoldsWithin(sub, perm, ""); got != want {
					t.Errorf("HoldsWithin %s: %v", perm, got)
				}
				// On the binding an earlier decision recorded, by name.
				recorded := authz.GrantedBinding{}
				if d.Granted != nil {
					recorded = *d.Granted
				} else if e, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermTaskResultCommit), Subject: sub, Resource: at, Policy: p}); err == nil {
					recorded = *e.Granted
				}
				if _, err := authz.ByBinding(p, subject, recorded, perm, at); (err == nil) != want {
					t.Errorf("ByBinding %s: %v", perm, err)
				}
				binder := authz.BinderBinding{Name: recorded.Name, Selector: recorded.Subject, Role: recorded.Role, Scope: recorded.Scope}
				if ok, why := p.StillGrants(binder, subject, perm, at); ok != want {
					t.Errorf("StillGrants %s: %v, %s", perm, ok, why)
				}
			}
		})
	}
}

// TestOrkerIDsAny_IsTheOrkerRolesOnly: the flag goes with the orker role and
// no other, so no role a person writes can carry it; a role that holds
// eventsource.host without it grants that as any permission.
func TestOrkerIDsAny_IsTheOrkerRolesOnly(t *testing.T) {
	const subject = "serviceaccount:orker-main"
	hosts := &orkstackv1.Role{Name: "hosts", Permissions: []string{string(authz.PermEventSourceHost)}}
	flagged := bind("b", "hosts", "pool:main", subject)
	flagged.OrkerIdsAny = true
	configured, err := authz.CompileConfiguration(authz.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authz.Compile(doc([]*orkstackv1.Role{hosts}, flagged), configured, time.Unix(1700000000, 0)); err == nil {
		t.Error("orker_ids_any was accepted on a binding to another role")
	}
	p := mustCompile(t, doc([]*orkstackv1.Role{hosts}, bind("b", "hosts", "pool:main", subject)), authz.Configuration{})
	sub := &authz.Subject{ID: subject, Kind: "service", Issuer: "serviceaccount"}
	if _, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermEventSourceHost), Subject: sub, Resource: authz.Resource{Pool: "main"}, Policy: p}); err != nil {
		t.Errorf("a role that holds eventsource.host: %v", err)
	}
}
