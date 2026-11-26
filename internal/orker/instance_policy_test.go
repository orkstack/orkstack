package orker

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// instanceSpec is a service spec every check of an instance's policy passes.
func instanceSpec(h *policyHarness) *eventsources.ServiceSpec {
	image := h.input("source", nil).GetImage().GetPinned()
	return &eventsources.ServiceSpec{Format: eventsources.SpecFormat, Source: eventsources.SpecSource{ID: "evs-0123456789abcdef", Namespace: "payments", Name: "orders"},
		Image: image, Contracts: []int32{1}, Profiles: []int32{1}, Concurrency: 4,
		Resources: eventsources.SpecResources{CPU: "500m", Memory: "256Mi"}, Limits: eventsources.SpecLimits{MaxBodyBytes: 1 << 20}}
}

// TestInstancePolicy: an instance's spec is held to the image policy as the engine
// selected it now and to what this Orker can host, may run and may read; each
// refusal is its own reason, and none reads a secret or pulls anything.
func TestInstancePolicy(t *testing.T) {
	sel := func(s *orkstackv1.ImagePolicySelection) func(*policyHarness) *orkstackv1.ImagePolicySelection {
		return func(*policyHarness) *orkstackv1.ImagePolicySelection { return s }
	}
	none := sel(&orkstackv1.ImagePolicySelection{})
	for name, tc := range map[string]struct {
		change func(*testing.T, *policyHarness, *eventsources.ServiceSpec, *Activities)
		sel    func(*policyHarness) *orkstackv1.ImagePolicySelection
		reason string
		cause  string
	}{
		"as it should be": {sel: none},
		"at its bounds": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, a *Activities) {
			a.sourceHost.SpoolBytes = servicehost.AnswerBytes(s.Concurrency, s.Limits.MaxBodyBytes)
		}, sel: none},
		"a digest the policy requires":  {sel: sel(&orkstackv1.ImagePolicySelection{RequireDigest: true})},
		"a registry outside the policy": {sel: sel(&orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme/"}}), reason: "IMAGE_NOT_PERMITTED"},
		"an image by tag": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Image = "ghcr.io/acme/source:v1"
		},
			sel: none, reason: "IMAGE_NOT_PERMITTED"},
		"no reference": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) { s.Image = "@@" }, sel: none, reason: "IMAGE_NOT_PERMITTED"},
		"signed by the signer": {change: func(t *testing.T, h *policyHarness, _ *eventsources.ServiceSpec, _ *Activities) {
			h.sign(t, h.input("source", nil), h.key)
		},
			sel: (*policyHarness).signer},
		"not signed": {sel: (*policyHarness).signer, reason: "IMAGE_NOT_PERMITTED", cause: "IMAGE_NOT_SIGNED"},
		"signed by another": {change: func(t *testing.T, h *policyHarness, _ *eventsources.ServiceSpec, _ *Activities) {
			h.sign(t, h.input("source", nil), newPolicyHarness(t).key)
		}, sel: (*policyHarness).signer, reason: "IMAGE_NOT_PERMITTED", cause: "IMAGE_SIGNATURE_INVALID"},
		"a registry that does not answer": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			_, digest, _ := strings.Cut(s.Image, "@")
			s.Image = "127.0.0.1:1/acme/migrate@" + digest
		}, sel: (*policyHarness).signer, reason: "IMAGE_PULL_FAILED"},
		"no registry client": {change: func(_ *testing.T, h *policyHarness, _ *eventsources.ServiceSpec, _ *Activities) { h.d.Signatures = nil },
			sel: (*policyHarness).signer, reason: "IMAGE_PULL_FAILED"},
		"a contract this Orker does not host": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Contracts = []int32{2, 3}
		},
			sel: none, reason: "SERVICE_CONTRACT_UNSUPPORTED"},
		"a profile it does not": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) { s.Profiles = nil },
			sel: none, reason: "SERVICE_PROFILE_UNSUPPORTED"},
		"more cpu than an instance may have": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Resources.CPU = "501m"
		},
			sel: none, reason: "RESOURCES_UNSATISFIABLE"},
		"more memory": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Resources.Memory = "257Mi"
		},
			sel: none, reason: "RESOURCES_UNSATISFIABLE"},
		"resources that do not read": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Resources.Memory = "lots"
		},
			sel: none, reason: "RESOURCES_UNSATISFIABLE"},
		"bodies larger than its answers may be": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, a *Activities) {
			a.sourceHost.SpoolBytes = servicehost.AnswerBytes(s.Concurrency, s.Limits.MaxBodyBytes) - 1
		}, sel: none, reason: "SPOOL_UNSATISFIABLE"},
		"a secret's scheme it does not resolve": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Secrets = []eventsources.SpecSecret{{Name: "token", Source: "vault:kv/token"}}
		}, sel: none, reason: "SECRET_NOT_PERMITTED", cause: "SECRET_SCHEME_FORBIDDEN"},
		"a secret below the floor": {change: func(_ *testing.T, h *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			h.d.Policy.SecretAllow = []string{"env:PAY_*"}
			s.Secrets = []eventsources.SpecSecret{{Name: "ok", Source: "env:PAY_KEY"}, {Name: "token", Source: "env:OTHER"}}
		}, sel: none, reason: "SECRET_NOT_PERMITTED"},
		"a secret named as a directory": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Secrets = []eventsources.SpecSecret{{Name: "..", Source: "env:PAY_KEY"}}
		}, sel: none, reason: "SECRET_NOT_PERMITTED"},
		"a secret named by a path": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Secrets = []eventsources.SpecSecret{{Name: "a/b", Source: "env:PAY_KEY"}}
		}, sel: none, reason: "SECRET_NOT_PERMITTED"},
		"no image policy given": {sel: sel(nil), reason: "ENGINE_UNAVAILABLE"},
		"one of the Orker's own": {change: func(_ *testing.T, _ *policyHarness, s *eventsources.ServiceSpec, _ *Activities) {
			s.Secrets = []eventsources.SpecSecret{{Name: "token", Source: "env:ORK_S3_SECRET_KEY"}}
		}, sel: none, reason: "SECRET_NOT_PERMITTED"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newPolicyHarness(t)
			a := &Activities{dispatcher: h.d, sourceHost: SourceHostSettings{MaxInstances: 1, MaxRequestCPU: "500m", MaxRequestMemory: "256Mi", SpoolBytes: 64 << 20}}
			spec := instanceSpec(h)
			if tc.change != nil {
				tc.change(t, h, spec, a)
			}
			f := a.checkInstancePolicy(t.Context(), spec, tc.sel(h))
			switch {
			case tc.reason == "" && f != nil:
				t.Fatalf("refused: %v", f)
			case tc.reason != "" && (f == nil || f.Reason != tc.reason || f.Cause != tc.cause || f.Detail == ""):
				t.Fatalf("%+v; want %s (%s)", f, tc.reason, tc.cause)
			case f != nil:
				row, ok := eventsources.LookupInstanceEnd(f.Reason)
				if !ok || f.Cause != "" && eventsources.InstanceCause(f.Cause) == "" {
					t.Fatalf("%s (%s) is in no table: %+v", f.Reason, f.Cause, row)
				}
			}
			for _, e := range h.log.all() {
				if e == "pull" || e == "create" {
					t.Fatalf("a check of policy did %v", h.log.all())
				}
			}
		})
	}
	// An Orker whose own settings do not read hosts nothing, and says it is it.
	h := newPolicyHarness(t)
	a := &Activities{dispatcher: h.d, sourceHost: SourceHostSettings{MaxRequestCPU: "half", MaxRequestMemory: "256Mi", SpoolBytes: 64 << 20}}
	if f := a.checkInstancePolicy(t.Context(), instanceSpec(h), &orkstackv1.ImagePolicySelection{}); f == nil || f.Reason != "SERVICE_HOST_UNFIT" {
		t.Errorf("an unreadable ceiling: %v", f)
	}
	_, err := instanceRequest("500m", "256Mi")
	must.OK(t, err)
}
