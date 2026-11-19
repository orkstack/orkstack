package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// stubPolicies answers one policy, or one read error. It stands in for the
// store's fault cases (an unreadable record), which the real store cannot
// produce on demand.
type stubPolicies struct {
	NamespacePolicyStore
	policy artifacts.NamespacePolicy
	err    error
	reads  int
}

func (s *stubPolicies) NamespacePolicy(context.Context, string, time.Time) (artifacts.NamespacePolicy, bool, error) {
	s.reads++
	return s.policy, s.err == nil, s.err
}

// TestCheckIdentityPolicy: at create, an audience the namespace does not
// allow is IDENTITY_NOT_PERMITTED on its field; a definition asking for
// nothing reads no policy; a policy that cannot be read refuses.
func TestCheckIdentityPolicy(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "payments"}
	role := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "apply", Identities: []*orkstackv1.IdentityRef{{Name: "t", Audience: "sts"}}}}}
	plain := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "plain"}}}

	allows := &stubPolicies{policy: artifacts.NamespacePolicy{Audiences: []string{"sts"}}}
	if err := (&Service{namespacePolicies: allows}).checkIdentityPolicy(context.Background(), ns, role); err != nil {
		t.Fatalf("allowed: %v", err)
	}
	none := &stubPolicies{}
	err := (&Service{namespacePolicies: none}).checkIdentityPolicy(context.Background(), ns, role)
	var field *validationFieldError
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "IDENTITY_NOT_PERMITTED") ||
		!errors.As(err, &field) || field.Field != "tasks[0].identities[0].audience" {
		t.Fatalf("not allowed: err = %v", err)
	}
	if err := (&Service{namespacePolicies: none}).checkIdentityPolicy(context.Background(), ns, plain); err != nil || none.reads != 1 {
		t.Fatalf("asking for nothing: err = %v, reads = %d (want no further read)", err, none.reads)
	}
	down := &stubPolicies{err: errors.New("bucket unreachable")}
	if err := (&Service{namespacePolicies: down}).checkIdentityPolicy(context.Background(), ns, role); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("an unreadable policy: err = %v, want unavailable", err)
	}
	if err := (&Service{}).checkIdentityPolicy(context.Background(), ns, role); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("no policy store: err = %v, want refused (no store holds no policy)", err)
	}
}
