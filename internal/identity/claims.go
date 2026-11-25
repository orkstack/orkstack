package identity

import (
	"errors"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// notBeforeSkew is how far before iat a token is valid (workload-identity
// §4): nbf is 30 s earlier, for a relying party whose clock runs behind.
const notBeforeSkew = 30 * time.Second

// ClaimsInput is what one token of a mint is issued for.
type ClaimsInput struct {
	Issuer     string // identity.issuer
	Deployment string // ORK_DEPLOYMENT_NAME
	Admission  artifacts.Admission
	TaskID     string
	Item       artifacts.Item
	Attempt    int32
	// Identity is the identity's name, or "grant:<name>" for a grant's
	// login, whose name Grant then holds.
	Identity string
	Grant    string
	Audience string
	Pool     string // where the attempt runs, from the admission
	Orker    string // the caller's Orker id
	Now      time.Time
	Lifetime time.Duration
	JTI      string
}

// BuildClaims is one token's claims (workload-identity §4). The subject is
// built from names only, in the format and trust domain the run was
// admitted with; every other claim comes from the admission, so a run's
// tokens say what it was admitted as, whatever has changed since.
func BuildClaims(in ClaimsInput) (map[string]any, error) {
	if in.Lifetime <= 0 {
		return nil, errors.New("a token needs a lifetime")
	}
	adm := in.Admission
	subject := Subject{Namespace: adm.Namespace, Orkflow: adm.Orkflow, Task: in.TaskID, Grant: in.Grant}
	if adm.OrkflowNamespace != "" && adm.OrkflowNamespace != adm.Namespace {
		subject.Owner = adm.OrkflowNamespace
	}
	format, err := ParseFormat(adm.SubjectFormat)
	if err != nil {
		return nil, fmt.Errorf("the admission's subject format: %w", err)
	}
	sub, otherClaim, otherValue, err := subject.Claims(format, adm.TrustDomain)
	if err != nil {
		return nil, fmt.Errorf("the token's subject: %w", err)
	}
	c := map[string]any{
		"iss":                   in.Issuer,
		"sub":                   sub,
		"aud":                   in.Audience,
		"iat":                   in.Now.Unix(),
		"nbf":                   in.Now.Add(-notBeforeSkew).Unix(),
		"exp":                   in.Now.Add(in.Lifetime).Unix(),
		"jti":                   in.JTI,
		"ork_deployment":        in.Deployment,
		"ork_namespace":         adm.Namespace,
		"ork_namespace_id":      adm.NamespaceID,
		"ork_orkflow":           adm.Orkflow,
		"ork_orkflow_id":        adm.OrkflowID,
		"ork_orkflow_version":   adm.OrkflowVersion,
		"ork_definition_digest": "sha256:" + adm.DefinitionDigest,
		"ork_run_id":            adm.RunID,
		"ork_task":              in.TaskID,
		"ork_attempt":           in.Attempt,
		"ork_identity":          in.Identity,
		"ork_pool":              in.Pool,
		"ork_orker":             in.Orker,
		"ork_origin":            adm.Origin.Kind,
	}
	if otherClaim != "" {
		c[otherClaim] = otherValue
	}
	if subject.Owner != "" {
		c["ork_orkflow_namespace"] = subject.Owner
	}
	if in.Item.Set {
		c["ork_item"] = in.Item.Index
	}
	if adm.Starter.Kind == artifacts.StarterParentRun {
		c["ork_parent_run_id"] = adm.Starter.RunID
		c["ork_root_run_id"] = adm.RootRunID
	}
	// Only a principal OrkStack verified is named: an asserted one, and the
	// runs a schedule, a trigger or an event source started, have none.
	if adm.Origin.Verified && adm.Origin.Principal != "" {
		c["ork_submitted_by"] = adm.Origin.Principal
	}
	return c, nil
}
