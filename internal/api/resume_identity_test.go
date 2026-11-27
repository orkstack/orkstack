package api_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resumePolicies is one namespace's identity policy as a test sets it, and
// how many times it was read.
type resumePolicies struct {
	mu     sync.Mutex
	policy artifacts.NamespacePolicy
	reads  int
}

func (p *resumePolicies) NamespacePolicy(context.Context, string, time.Time) (artifacts.NamespacePolicy, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reads++
	return p.policy, true, nil
}

func (p *resumePolicies) SetNamespacePolicy(context.Context, string, time.Time, artifacts.NamespacePolicy, int64, artifacts.PolicyWriter) (artifacts.NamespacePolicy, error) {
	panic("a resume writes no policy")
}

func (p *resumePolicies) ClearNamespacePolicy(context.Context, string, time.Time) error {
	panic("a resume writes no policy")
}

type identityEdit func(f *resumeFixture, p *resumePolicies, fleet *stubFleet, orkflows *engine.MemoryOrkflowStore)

// identityResume is a fixture whose task b, which runs again, asks for an
// identity of audience sts, under a policy that allows it, on a fleet whose
// one Orker serves pool main and task identity with a scoped storage
// credential; what edit changes then stands as it is when the run is resumed.
func identityResume(t *testing.T, edit identityEdit) (*resumeFixture, *resumePolicies) {
	t.Helper()
	policies := &resumePolicies{policy: artifacts.NamespacePolicy{Audiences: []string{"sts"}}}
	fleet := &stubFleet{
		presence: []engine.OrkerPresence{{Identity: "orker-1"}},
		described: map[string]*orkstackv1.DescribeOrkerResult{"orker-1": {OrkerId: "orker-1", Pool: "main",
			IdentitySupported: true, StoreScope: orkstackv1.StoreScope_STORE_SCOPE_SCOPED}},
	}
	orkflows := engine.NewMemoryOrkflowStore()
	f := newResumeFixture(t, func(f *resumeFixture) {
		f.def.Tasks[1].Identities = []*orkstackv1.IdentityRef{{Name: "tok", Audience: "sts"}}
		f.fleet = fleet
		if edit != nil {
			edit(f, policies, fleet, orkflows)
		}
		f.configure = func(cfg *api.Config) {
			cfg.NamespacePolicies = policies
			cfg.Orkflows = orkflows
		}
	})
	return f, policies
}

// A resume runs again under the namespace and the Orkers as they are now: a
// task that runs again with an identity is held to what a submit is, and a
// policy or an Orker that would refuse a submit refuses the resume, with
// nothing reset.
func TestResumeRun_IsHeldToTaskIdentityAsASubmitIs(t *testing.T) {
	for name, tc := range map[string]struct {
		edit   identityEdit
		reason string
		class  orkstackv1.ResumeRefusalClass
		// says is what the refusal's message must say, when set.
		says string
	}{
		"an audience the namespace no longer allows": {
			edit: func(_ *resumeFixture, p *resumePolicies, _ *stubFleet, _ *engine.MemoryOrkflowStore) {
				p.policy.Audiences = []string{"other"}
			},
			reason: "IDENTITY_POLICY_CHANGED", class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
		},
		"a grant the namespace does not allow": {
			edit: func(f *resumeFixture, _ *resumePolicies, _ *stubFleet, _ *engine.MemoryOrkflowStore) {
				f.def.Tasks[1].Secrets = []*orkstackv1.SecretRef{{Name: "s", Source: "env:TOKEN", Grant: "g"}}
			},
			reason: "IDENTITY_POLICY_CHANGED", class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
		},
		"a version no longer current, where only the current may hold identities": {
			edit: func(f *resumeFixture, p *resumePolicies, _ *stubFleet, orkflows *engine.MemoryOrkflowStore) {
				p.policy.IdentitiesCurrentOnly = true
				for _, v := range []string{"1", "2"} {
					def := proto.Clone(f.def).(*orkstackv1.Orkflow)
					def.Version = v
					if _, _, err := orkflows.Put(context.Background(), def, artifacts.HeadExpectation{}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := orkflows.SetCurrentVersion(context.Background(), "default", "wf", "2", artifacts.CurrentExpectation{}); err != nil {
					t.Fatal(err)
				}
			},
			reason: "IDENTITY_POLICY_CHANGED", class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
		},
		"an Orkflow no longer stored, where only the current may hold identities": {
			edit: func(_ *resumeFixture, p *resumePolicies, _ *stubFleet, _ *engine.MemoryOrkflowStore) {
				p.policy.IdentitiesCurrentOnly = true
			},
			reason: "IDENTITY_POLICY_CHANGED", class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
		},
		// A schedule's run holds an identity only from a binder who may still
		// submit it and a consent to what it asks for; none is on record.
		"a run a schedule started, whose binder is not on record": {
			edit: func(f *resumeFixture, _ *resumePolicies, _ *stubFleet, _ *engine.MemoryOrkflowStore) {
				f.run.Origin = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE, Id: "nightly", Description: "nightly"}
			},
			reason: "IDENTITY_POLICY_CHANGED", class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
			says: `cannot tell which binder of schedule "nightly" started it`,
		},
		"a pool whose Orkers no longer serve task identity": {
			edit: func(_ *resumeFixture, _ *resumePolicies, fleet *stubFleet, _ *engine.MemoryOrkflowStore) {
				fleet.described["orker-1"].IdentitySupported = false
			},
			reason: "RERUN_POOL_UNSERVED", class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT,
		},
		"an Orker whose storage credential is not confirmed scoped": {
			edit: func(_ *resumeFixture, _ *resumePolicies, fleet *stubFleet, _ *engine.MemoryOrkflowStore) {
				fleet.described["orker-1"].StoreScope = orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED
			},
			reason: "RERUN_STORE_UNSCOPED", class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT,
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, _ := identityResume(t, tc.edit)
			_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			refusedAs(t, err, tc.reason, tc.class)
			if !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("refused %v, want it to say %q", err, tc.says)
			}
			if len(f.cluster.resets) != 0 {
				t.Fatalf("%d resets", len(f.cluster.resets))
			}
		})
	}
	// The same run, under a namespace and Orkers that allow it, is resumed.
	f, p := identityResume(t, nil)
	if _, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true}); err != nil || p.reads == 0 {
		t.Fatalf("allowed: %v, after %d policy reads", err, p.reads)
	}
}

// A run none of whose tasks that run again asks for an identity is not held
// to it, however the namespace and the Orkers have changed: it reads no
// policy.
func TestResumeRun_WithNoIdentityToRunAgainIsNotHeldToIt(t *testing.T) {
	for name, edit := range map[string]func(*resumeFixture){
		// It asks for none at all.
		"a run that holds none": func(f *resumeFixture) { f.def.Tasks[1].Identities = nil },
	} {
		t.Run(name, func(t *testing.T) {
			f, p := identityResume(t, func(f *resumeFixture, p *resumePolicies, fleet *stubFleet, _ *engine.MemoryOrkflowStore) {
				p.policy = artifacts.NamespacePolicy{IdentitiesCurrentOnly: true}
				fleet.described["orker-1"].StoreScope = orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED
				edit(f)
			})
			if _, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true}); err != nil {
				t.Fatalf("resume: %v", err)
			}
			if p.reads != 0 {
				t.Fatalf("the policy was read %d times", p.reads)
			}
		})
	}
}

// A run only a kept task of which held an identity runs none again, so the
// namespace's policy is not asked; but its results are still recorded by the
// engine, so every Orker must still be confirmed scoped.
func TestResumeRun_AnAdmittedRunIsHeldToScopedOrkers(t *testing.T) {
	f, p := identityResume(t, func(f *resumeFixture, p *resumePolicies, fleet *stubFleet, _ *engine.MemoryOrkflowStore) {
		f.def.Tasks[0].Identities, f.def.Tasks[1].Identities = f.def.Tasks[1].Identities, nil
		f.plan.Identity = &engine.IdentityPlan{AdmissionSHA256: resumeAdmission,
			RecordBudget: &engine.RecordBudget{MissConfirmMS: 1, ReaskInitialMS: 1, ReaskMaxMS: 1}}
		p.policy = artifacts.NamespacePolicy{}
		fleet.described["orker-1"].StoreScope = orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED
	})
	_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	refusedAs(t, err, "RERUN_STORE_UNSCOPED", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT)
	if p.reads != 0 {
		t.Fatalf("the policy was read %d times", p.reads)
	}
}
