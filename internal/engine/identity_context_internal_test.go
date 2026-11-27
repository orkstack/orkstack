package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestIdentityContextFor_ARunNotAdmitted: in a run with no admission a task
// that holds an identity, a credential or a secret through a grant carries
// what it holds and no admission, so its Orker refuses it by name; a task
// that holds none carries nothing, as before.
func TestIdentityContextFor_ARunNotAdmitted(t *testing.T) {
	s := &runState{}
	for name, task := range map[string]*orkstackv1.Task{
		"an identity":            {Id: "a", Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts"}}},
		"a credential":           {Id: "b", Credentials: []*orkstackv1.CredentialRef{{Name: "pki", Grant: "g"}}},
		"a secret through grant": {Id: "c", Secrets: []*orkstackv1.SecretRef{{Name: "s", Source: "vault:x", Grant: "g"}}},
	} {
		got := s.identityContextFor(task)
		if got == nil || got.GetAdmissionSha256() != "" {
			t.Errorf("%s: context = %+v; want one with no admission", name, got)
		}
	}
	if got := s.identityContextFor(&orkstackv1.Task{Id: "plain", Secrets: []*orkstackv1.SecretRef{{Name: "s", Source: "env:S"}}}); got != nil {
		t.Errorf("a task that holds nothing = %+v", got)
	}
}
