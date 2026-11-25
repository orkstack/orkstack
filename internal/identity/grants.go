package identity

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// GrantAsk is what the grants a definition's tasks name are judged by, with
// the namespace its run executes in and the deployment in hand.
type GrantAsk struct {
	// Namespace is the namespace the run executes in, whose policy holds
	// Grants and whose allowed_secrets is Allowed.
	Namespace string
	Grants    map[string]artifacts.Grant
	Allowed   []string
	// Owner is the namespace that owns the definition: Namespace, or the
	// one that shared it.
	Owner, Orkflow string
	// Origin is the kind of start of the run tree's root, "" at validate and
	// create, where no run is.
	Origin string
	// Current answers whether the definition is its Orkflow's current
	// version; nil at validate and create, where it has none. It is asked
	// only for a grant that wants it. Its error is returned as it is: a fact
	// that could not be read refuses nothing.
	Current func() (bool, error)
	// Providers answers a provider the deployment declares.
	Providers func(name string) (provider.Provider, bool)
}

// GrantRefusedError is the first reference of a definition to a grant that
// is refused. Reason is one of provider's, and Field the path of what is
// wrong.
type GrantRefusedError struct {
	Reason, Message string
	field           string
}

func (e *GrantRefusedError) Error() string { return e.Reason + ": " + e.Message }

// Field is the authored path the refusal is of.
func (e *GrantRefusedError) Field() string { return e.field }

// CheckGrants judges every credential and granted secret of wf (providers.md
// §4.2, §9): its grant is one of the namespace's, admits this Orkflow, this
// start and this version, and is of a provider the deployment declares; a
// secret's source names that provider; the provider accepts the reference;
// and the reference's policy string is admitted by the namespace's
// allowed_secrets. It answers the grants used, by name, as a run's admission
// pins them; nil when none is used, and nil with a refusal.
func CheckGrants(ask GrantAsk, wf *orkstackv1.Orkflow) (map[string]artifacts.GrantSelection, error) {
	var used map[string]artifacts.GrantSelection
	for ti, t := range wf.GetTasks() {
		for ci, c := range t.GetCredentials() {
			r := grantReference{task: t.GetId(), what: fmt.Sprintf("credential %q", c.GetName()), grant: c.GetGrant(),
				at: fmt.Sprintf("tasks[%d].credentials[%d]", ti, ci), locatorField: "path",
				ref: provider.Ref{Kind: provider.KindCredential, Locator: c.GetPath(), Params: c.GetParams()}}
			if err := ask.take(&used, r); err != nil {
				return nil, err
			}
		}
		for si, s := range t.GetSecrets() {
			if s.GetGrant() == "" {
				continue
			}
			name, locator, _ := provider.SplitSource(s.GetSource())
			r := grantReference{task: t.GetId(), what: fmt.Sprintf("secret %q", s.GetName()), grant: s.GetGrant(),
				at: fmt.Sprintf("tasks[%d].secrets[%d]", ti, si), locatorField: "source", source: name,
				ref: provider.Ref{Kind: provider.KindSecret, Locator: locator}}
			if c := s.GetCiphertext(); c != "" {
				raw, err := base64.StdEncoding.Strict().DecodeString(c)
				if err != nil {
					return nil, r.refuse(provider.ReasonProviderRequestInvalid, "ciphertext", "its ciphertext is not standard base64")
				}
				r.ref.Ciphertext = raw
			}
			if err := ask.take(&used, r); err != nil {
				return nil, err
			}
		}
	}
	return used, nil
}

// grantReference is one reference a task makes through a grant. source is a
// secret's provider, "" for a credential, whose provider is its grant's.
type grantReference struct {
	task, what, grant, at, locatorField, source string
	ref                                         provider.Ref
}

func (r grantReference) refuse(reason, field, format string, args ...any) error {
	return &GrantRefusedError{Reason: reason, field: r.at + "." + field,
		Message: fmt.Sprintf("task %q's %s: ", r.task, r.what) + fmt.Sprintf(format, args...)}
}

func (a GrantAsk) take(used *map[string]artifacts.GrantSelection, r grantReference) error {
	grant, ok := a.Grants[r.grant]
	if !ok {
		return r.refuse(provider.ReasonGrantNotDefined, "grant", "namespace %q has no grant %q", a.Namespace, r.grant)
	}
	if r.source != "" {
		if _, ok := a.Providers(r.source); !ok {
			return r.refuse(provider.ReasonProviderUnknown, "source", "this deployment declares no provider %q", r.source)
		}
		if r.source != grant.Provider {
			return r.refuse(provider.ReasonGrantNotPermitted, "source", "its source is provider %q's, and grant %q is for provider %q", r.source, r.grant, grant.Provider)
		}
	}
	p, ok := a.Providers(grant.Provider)
	if !ok {
		return r.refuse(provider.ReasonProviderUnknown, "grant", "grant %q is for provider %q, which this deployment does not declare", r.grant, grant.Provider)
	}
	if !a.admits(grant) {
		return r.refuse(provider.ReasonGrantNotPermitted, "grant", "grant %q does not list Orkflow %s", r.grant, a.orkflow())
	}
	// With no Current there is no run, and so no start, to judge.
	if len(grant.Origins) > 0 && (a.Origin != "" || a.Current != nil) && !slices.Contains(grant.Origins, a.Origin) {
		return r.refuse(provider.ReasonGrantNotPermitted, "grant", "grant %q serves runs started by %s, and this one's origin is %s",
			r.grant, strings.Join(grant.Origins, ", "), a.Origin)
	}
	if grant.CurrentOnly && a.Current != nil {
		current, err := a.Current()
		if err != nil {
			return err
		}
		if !current {
			return r.refuse(provider.ReasonGrantNotPermitted, "grant", "grant %q is for Orkflow %s's current version only, and this is not it", r.grant, a.orkflow())
		}
	}
	if _, err := p.Parse(r.ref); err != nil {
		return r.refuse(provider.ReasonProviderRequestInvalid, r.locatorField, "provider %q: %v", grant.Provider, err)
	}
	field := r.locatorField
	if r.ref.Locator == "" {
		field = "grant"
	}
	policy, err := provider.Policy(p, r.ref, grant.Principal)
	if err != nil {
		return r.refuse(provider.ReasonProviderRequestInvalid, field, "%v", err)
	}
	if len(a.Allowed) > 0 {
		if err := dsl.CheckSecretSource(a.Allowed, policy); err != nil {
			return r.refuse(ReasonReferenceNotAllowed, field, "namespace %q's allowed_secrets does not admit %s: %v", a.Namespace, policy, err)
		}
	}
	if *used == nil {
		*used = map[string]artifacts.GrantSelection{}
	}
	(*used)[r.grant] = artifacts.GrantSelection{Provider: grant.Provider, Principal: grant.Principal, Audience: p.Audience()}
	return nil
}

// admits is whether the grant lists this Orkflow: its name or "*" for one
// of the namespace's own, "<owner>/<name>" for a shared one, which "*"
// never covers.
func (a GrantAsk) admits(g artifacts.Grant) bool {
	if artifacts.ResolveNamespace(a.Owner) == artifacts.ResolveNamespace(a.Namespace) {
		return slices.Contains(g.Orkflows, "*") || slices.Contains(g.Orkflows, a.Orkflow)
	}
	return slices.Contains(g.Orkflows, a.orkflow())
}

// orkflow is the definition's Orkflow as a grant would list it.
func (a GrantAsk) orkflow() string {
	if artifacts.ResolveNamespace(a.Owner) == artifacts.ResolveNamespace(a.Namespace) {
		return a.Orkflow
	}
	return artifacts.ResolveNamespace(a.Owner) + "/" + a.Orkflow
}

// ReasonReferenceNotAllowed is a provider reference whose policy string the
// namespace's allowed_secrets does not admit.
const ReasonReferenceNotAllowed = "SECRET_SOURCE_NOT_ALLOWED"

// grantWithdrawn is why a grant the run was admitted with may no longer be
// used, or "": the namespace's policy still holds it for the provider and
// principal the run was admitted for, still lists the run's Orkflow, and
// still serves the start of its tree. Whether a version is current is judged
// at submit alone.
func grantWithdrawn(policy artifacts.NamespacePolicy, adm artifacts.Admission, g AdmittedGrant) string {
	now, ok := policy.Grants[g.Name]
	owner := adm.OrkflowNamespace
	if owner == "" {
		owner = adm.Namespace
	}
	ask := GrantAsk{Namespace: adm.Namespace, Owner: owner, Orkflow: adm.Orkflow}
	switch {
	case !ok:
		return "is gone"
	case now.Provider != g.Provider || now.Principal != g.Principal:
		return "is for another provider or principal than the run was admitted for"
	case !ask.admits(now):
		return fmt.Sprintf("no longer lists Orkflow %s", ask.orkflow())
	case len(now.Origins) > 0 && !slices.Contains(now.Origins, adm.Origin.Kind):
		return fmt.Sprintf("no longer serves runs started by %s", adm.Origin.Kind)
	}
	return ""
}
