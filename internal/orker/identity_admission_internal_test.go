package orker

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
)

// TestIdentityFailure_ARunNotAdmitted: a task that holds an identity in a run
// with no admission is refused by name before anything runs, never run
// without its token; a task of a run with no identity at all is not asked.
func TestIdentityFailure_ARunNotAdmitted(t *testing.T) {
	d := &OCIDispatcher{}
	unadmitted := &orkstackv1.ExecuteTaskInput{TaskId: "deploy", Identity: &orkstackv1.IdentityContext{
		Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts.amazonaws.com"}}}}
	f := d.identityFailure(unadmitted)
	if f == nil || f.Reason != identity.ReasonNotAdmitted || f.Category() != CategoryContractViolation {
		t.Fatalf("failure = %+v; want IDENTITY_NOT_ADMITTED, a contract violation", f)
	}
	if f := d.identityFailure(&orkstackv1.ExecuteTaskInput{TaskId: "plain"}); f != nil {
		t.Fatalf("a task with no identity = %+v", f)
	}
}

// testAdmissionSHA names the admission of the test runs that hold an identity:
// every input an engine gives such a run names one.
var testAdmissionSHA = strings.Repeat("a", 64)
