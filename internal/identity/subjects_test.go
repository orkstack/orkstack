package identity_test

import (
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
)

// TestCheckSubjects: a task is refused exactly where one of its tokens'
// subjects passes 255 bytes, in the colon form and, with a trust domain, in
// the SPIFFE form too, whichever the deployment issues; a shared Orkflow's
// subject names its owner beside the namespace it runs in; and a task that
// holds nothing has no subject.
func TestCheckSubjects(t *testing.T) {
	// The fixed bytes of each form around namespace "ns", Orkflow "deploy"
	// and a task's name.
	colon := len("ork:namespace:ns:orkflow:deploy:task:")
	sharedColon := len("ork:namespace:ns:orkflow:platform/deploy:task:")
	spiffe := len("spiffe://ork.acme.dev/ork/ns/ns/orkflow/deploy/task/")
	sharedSpiffe := len("spiffe://ork.acme.dev/ork/ns/ns/shared/platform/deploy/task/")
	const grant = len(":grant:deployer")
	const spiffeGrant = len("/grant/deployer")
	ids := func(t *orkstackv1.Task) { t.Identities = []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts"}} }
	cred := func(t *orkstackv1.Task) { t.Credentials = []*orkstackv1.CredentialRef{{Name: "c", Grant: "deployer"}} }
	secret := func(t *orkstackv1.Task) {
		t.Secrets = []*orkstackv1.SecretRef{{Name: "s", Source: "vault:a/b", Grant: "deployer"}}
	}
	plain := func(t *orkstackv1.Task) { t.Secrets = []*orkstackv1.SecretRef{{Name: "s", Source: "env:S"}} }
	both := func(t *orkstackv1.Task) { ids(t); cred(t) }
	for _, tc := range []struct {
		name   string
		format identity.Format
		domain string
		owner  string
		holds  func(*orkstackv1.Task)
		most   int // the longest task name that fits, capped by a name's own 200
		grant  string
	}{
		{"an identity", identity.FormatOrk, "", "ns", ids, 255 - colon, ""},
		{"a credential", identity.FormatOrk, "", "ns", cred, 255 - colon - grant, "deployer"},
		{"a granted secret", identity.FormatOrk, "", "ns", secret, 255 - colon - grant, "deployer"},
		{"both, where the login is the longer", identity.FormatOrk, "", "ns", both, 255 - colon - grant, "deployer"},
		{"a shared Orkflow", identity.FormatOrk, "", "platform", ids, 255 - sharedColon, ""},
		{"a shared Orkflow's credential", identity.FormatOrk, "", "platform", cred, 255 - sharedColon - grant, "deployer"},
		{"SPIFFE", identity.FormatSPIFFE, "ork.acme.dev", "ns", ids, 255 - spiffe, ""},
		{"SPIFFE, a credential", identity.FormatSPIFFE, "ork.acme.dev", "ns", cred, 255 - spiffe - spiffeGrant, "deployer"},
		{"SPIFFE, shared", identity.FormatSPIFFE, "ork.acme.dev", "platform", cred, 255 - sharedSpiffe - spiffeGrant, "deployer"},
		{"the form not issued too", identity.FormatOrk, "ork.acme.dev", "ns", ids, 255 - spiffe, ""},
	} {
		most := min(tc.most, 200)
		check := func(n int) error {
			task := &orkstackv1.Task{Id: "a" + strings.Repeat("b", n-1)}
			tc.holds(task)
			wf := &orkstackv1.Orkflow{Name: "deploy", Tasks: []*orkstackv1.Task{{Id: "first"}, task}}
			return identity.CheckSubjects(tc.format, tc.domain, "ns", tc.owner, wf)
		}
		if err := check(most); err != nil {
			t.Errorf("%s: a task of %d bytes: %v", tc.name, most, err)
		}
		if most == 200 {
			continue // a name's own bound comes first
		}
		var long *identity.SubjectTooLongError
		if err := check(most + 1); !errors.As(err, &long) || long.Field() != "tasks[1].id" || long.Grant != tc.grant ||
			!strings.Contains(err.Error(), identity.ReasonSubjectTooLong) {
			t.Errorf("%s: a task of %d bytes: %v; want IDENTITY_SUBJECT_TOO_LONG on tasks[1].id for grant %q", tc.name, most+1, err, tc.grant)
		}
	}
	long := &orkstackv1.Task{Id: "a" + strings.Repeat("b", 199)}
	plain(long)
	wf := &orkstackv1.Orkflow{Name: "a" + strings.Repeat("b", 199), Tasks: []*orkstackv1.Task{long}}
	if err := identity.CheckSubjects(identity.FormatSPIFFE, "ork.acme.dev", "ns", "ns", wf); err != nil {
		t.Errorf("a task that holds nothing: %v", err)
	}
}
