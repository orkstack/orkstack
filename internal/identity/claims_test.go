package identity

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

func claimsAdmission() artifacts.Admission {
	return artifacts.Admission{
		RunID: "ork-run", Namespace: "payments", Orkflow: "deploy", OrkflowVersion: "3", DefinitionDigest: "sha256:abc",
		Starter: artifacts.Starter{Kind: artifacts.StarterEngine}, Origin: artifacts.AdmittedOrigin{Kind: "submit", Principal: "user:corp:alice", Verified: true},
		SubjectFormat: string(FormatOrk), NamespaceID: "ns_1", OrkflowID: "of_1",
	}
}

// TestBuildClaims holds the claims of workload-identity §4: the subject
// from names, the run's facts from its admission, iat now and nbf 30 s
// earlier, the item only for a fan-out item, the parent and root only for
// a child run, the submitter only when verified, the other subject form
// only with a trust domain.
func TestBuildClaims(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	in := ClaimsInput{Issuer: "https://ork.example", Deployment: "prod", Admission: claimsAdmission(), TaskID: "apply",
		Attempt: 2, Identity: "aws", Audience: "sts.amazonaws.com", Pool: "prod", Orker: "orker-1", Now: now, Lifetime: time.Hour, JTI: "j1"}
	c, err := BuildClaims(in)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"iss": "https://ork.example", "sub": "ork:namespace:payments:orkflow:deploy:task:apply", "aud": "sts.amazonaws.com",
		"iat": now.Unix(), "nbf": now.Add(-30 * time.Second).Unix(), "exp": now.Add(time.Hour).Unix(), "jti": "j1",
		"ork_deployment": "prod", "ork_namespace": "payments", "ork_namespace_id": "ns_1", "ork_orkflow": "deploy", "ork_orkflow_id": "of_1",
		"ork_orkflow_version": "3", "ork_definition_digest": "sha256:abc", "ork_run_id": "ork-run", "ork_task": "apply",
		"ork_attempt": int32(2), "ork_identity": "aws", "ork_pool": "prod", "ork_orker": "orker-1", "ork_origin": "submit",
		"ork_submitted_by": "user:corp:alice",
	}
	for k, v := range want {
		if c[k] != v {
			t.Errorf("%s = %v (%T), want %v (%T)", k, c[k], c[k], v, v)
		}
	}
	for _, absent := range []string{"ork_item", "ork_root_run_id", "ork_parent_run_id", "ork_orkflow_namespace", ClaimSPIFFEID, ClaimSubject} {
		if _, ok := c[absent]; ok {
			t.Errorf("%s is set on a plain submitted run", absent)
		}
	}
	if len(c) != len(want) {
		t.Errorf("claims = %v, want exactly %d", c, len(want))
	}

	child := in
	child.Admission.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-parent"}
	child.Admission.RootRunID = "ork-root"
	child.Admission.Origin = artifacts.AdmittedOrigin{Kind: "parent-run", Principal: "asserted-bob"}
	child.Admission.OrkflowNamespace = "platform"
	child.Item = artifacts.Item{Index: 4, Set: true}
	child.Admission.TrustDomain = "ork.acme.dev"
	c, err = BuildClaims(child)
	if err != nil {
		t.Fatal(err)
	}
	if c["ork_parent_run_id"] != "ork-parent" || c["ork_root_run_id"] != "ork-root" || c["ork_item"] != int32(4) ||
		c["ork_orkflow_namespace"] != "platform" || c["sub"] != "ork:namespace:payments:orkflow:platform/deploy:task:apply" ||
		!strings.HasPrefix(c[ClaimSPIFFEID].(string), "spiffe://ork.acme.dev/ork/ns/payments/shared/platform/deploy/task/apply") {
		t.Fatalf("child claims = %v", c)
	}
	if _, ok := c["ork_submitted_by"]; ok {
		t.Fatal("an asserted principal is named as the submitter")
	}

	bad := in
	bad.Lifetime = 0
	if _, err := BuildClaims(bad); err == nil {
		t.Fatal("a token with no lifetime was built")
	}
}
