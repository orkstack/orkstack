package authz_test

import (
	"testing"

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
