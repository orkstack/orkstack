package api_test

import (
	"context"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const identityYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: deploy
version: "1"
tasks:
  - id: apply
    uses: ghcr.io/acme/apply:1
    identities:
      - name: aws
        audience: sts.amazonaws.com
`

// TestValidateOrkflow_AnEngineWithNoIssuerWarns: a definition that holds an
// identity is accepted where no issuer runs, and told then that its runs
// would be refused, IDENTITY_OFF, rather than first at a submit or a fire.
func TestValidateOrkflow_AnEngineWithNoIssuerWarns(t *testing.T) {
	ctx := context.Background()
	policies, err := artifacts.NewIncarnationStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	_, err = policies.SetNamespacePolicy(ctx, artifacts.DefaultNamespace, time.Unix(0, 0).UTC(),
		artifacts.NamespacePolicy{Audiences: []string{"sts.amazonaws.com"}}, 0, artifacts.PolicyWriter{Principal: "admin"})
	must.OK(t, err)
	svc := api.NewService(api.Config{Starter: &fakeStarter{}, Incarnations: policies, NamespacePolicies: policies})
	issue := issueAt(warningsFor(t, svc, identityYAML), "tasks[0].identities")
	if issue == nil || issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
		!strings.Contains(issue.GetMessage(), "IDENTITY_OFF") {
		t.Fatalf("issue = %v; want a warning naming IDENTITY_OFF", issue)
	}
}
