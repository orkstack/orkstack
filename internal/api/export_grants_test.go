package api

import (
	"context"
	"errors"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// GrantChecker is the grant checks of validate and create, under one policy.
type GrantChecker struct {
	s        *Service
	policies *stubPolicies
}

func NewGrantChecker(providers provider.Set, policy artifacts.NamespacePolicy) *GrantChecker {
	p := &stubPolicies{policy: policy}
	return &GrantChecker{s: &Service{providers: providers, namespacePolicies: p}, policies: p}
}

func (g *GrantChecker) Create(ctx context.Context, ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) error {
	return g.s.checkIdentityPolicy(ctx, ns, wf)
}

func (g *GrantChecker) Child(ctx context.Context, ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow, owner string) error {
	return g.s.checkChildIdentity(ctx, ns, wf, owner)
}

func (g *GrantChecker) Reads() int { return g.policies.reads }

// Field is the authored path err is anchored to.
func (g *GrantChecker) Field(err error) string {
	var field *validationFieldError
	if errors.As(err, &field) {
		return field.Field
	}
	return ""
}

// SubmitFrom is a submit from a start of origin, past the Connect door.
func SubmitFrom(ctx context.Context, s *Service, msg *orkstackv1.SubmitRunRequest, origin *orkstackv1.RunOrigin) error {
	_, err := s.submitFrom(ctx, msg, origin)
	return err
}

// ReadStarterKind is readStarter of a start of kind.
func ReadStarterKind(ctx context.Context, s *Service, kind orkstackv1.RunOriginKind) error {
	_, err := s.readStarter(ctx, "default", &orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}, &orkstackv1.RunOrigin{Kind: kind, Id: "x"})
	return err
}
