package automation

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v4"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// settings.identity is the namespace's identity policy
// (docs/TASK_IDENTITY.md, "A namespace's identity policy"): audiences,
// grants, identities_current_only and attempts_extra. The server writes it
// in an update of its own, so a plan that changes it beside other settings
// has two steps (plan.go).

var identitySubKeys = []string{"attempts_extra", "audiences", "grants", "identities_current_only"}

var grantSubKeys = []string{"current_only", "orkflows", "origins", "principal", "provider"}

func parseIdentity(node *yaml.Node, ns *orkstackv1.Namespace) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("identity must be a mapping of %s", joinKeys(identitySubKeys))
	}
	id := &orkstackv1.NamespaceIdentity{}
	seen := map[string]bool{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i].Value, node.Content[i+1]
		if seen[key] {
			return fmt.Errorf("line %d: identity.%s is written twice", node.Content[i].Line, key)
		}
		seen[key] = true
		switch key {
		case "audiences":
			values, err := stringList(value)
			if err != nil {
				return fmt.Errorf("identity.audiences: %w", err)
			}
			id.Audiences = values
		case "identities_current_only":
			b, err := boolNode("identity.identities_current_only", value)
			if err != nil {
				return err
			}
			id.IdentitiesCurrentOnly = b
		case "attempts_extra":
			text, ok := integerText(value)
			n, err := strconv.ParseInt(text, 0, 32)
			if !ok || err != nil || n < 0 {
				return fmt.Errorf("identity.attempts_extra must be an integer of at least 0, got %s", describeNode(value))
			}
			v := int32(n)
			id.AttemptsExtra = &v
		case "grants":
			grants, err := parseGrants(value)
			if err != nil {
				return err
			}
			id.Grants = grants
		default:
			return fmt.Errorf("line %d: identity.%s is not a key; the keys are %s", node.Content[i].Line, key, joinKeys(identitySubKeys))
		}
	}
	ns.Identity = id
	return nil
}

func parseGrants(node *yaml.Node) (map[string]*orkstackv1.Grant, error) {
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("identity.grants must be a mapping of grant names")
	}
	out := map[string]*orkstackv1.Grant{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		name, value := node.Content[i].Value, node.Content[i+1]
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("line %d: identity.grants.%s is written twice", node.Content[i].Line, name)
		}
		if value.Kind != yaml.MappingNode {
			return nil, fmt.Errorf("identity.grants.%s must be a mapping of %s", name, joinKeys(grantSubKeys))
		}
		g := &orkstackv1.Grant{}
		seen := map[string]bool{}
		for j := 0; j+1 < len(value.Content); j += 2 {
			key, v := value.Content[j].Value, value.Content[j+1]
			at := "identity.grants." + name + "." + key
			if seen[key] {
				return nil, fmt.Errorf("line %d: %s is written twice", value.Content[j].Line, at)
			}
			seen[key] = true
			var err error
			switch key {
			case "provider":
				g.Provider, err = stringScalar(v)
			case "principal":
				g.Principal, err = stringScalar(v)
			case "orkflows":
				g.Orkflows, err = stringList(v)
			case "origins":
				g.Origins, err = stringList(v)
			case "current_only":
				g.CurrentOnly, err = boolNode(at, v)
			default:
				return nil, fmt.Errorf("line %d: %s is not a key; a grant's keys are %s", value.Content[j].Line, at, joinKeys(grantSubKeys))
			}
			if err != nil {
				return nil, fmt.Errorf("%s: %w", at, err)
			}
		}
		out[name] = g
	}
	return out, nil
}

func boolNode(at string, node *yaml.Node) (bool, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
		return false, fmt.Errorf("%s must be true or false, got %s", at, describeNode(node))
	}
	return strconv.ParseBool(node.Value)
}

// identityKey is a policy's comparable form.
func identityKey(id *orkstackv1.NamespaceIdentity) string {
	id = orNoIdentity(id)
	var b strings.Builder
	fmt.Fprintf(&b, "aud=%q;cur=%t;", id.GetAudiences(), id.GetIdentitiesCurrentOnly())
	if id.AttemptsExtra != nil {
		fmt.Fprintf(&b, "extra=%d;", id.GetAttemptsExtra())
	}
	names := make([]string, 0, len(id.GetGrants()))
	for name := range id.GetGrants() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		g := id.GetGrants()[name]
		fmt.Fprintf(&b, "grant %q=%q:%q orkflows=%q origins=%q current=%t;", name, g.GetProvider(), g.GetPrincipal(),
			g.GetOrkflows(), g.GetOrigins(), g.GetCurrentOnly())
	}
	return b.String()
}

func renderIdentity(ns *orkstackv1.Namespace) string {
	id := orNoIdentity(ns.GetIdentity())
	if len(id.GetAudiences())+len(id.GetGrants()) == 0 && id.AttemptsExtra == nil && !id.GetIdentitiesCurrentOnly() {
		return "none"
	}
	parts := []string{}
	if len(id.GetAudiences()) > 0 {
		parts = append(parts, "audiences "+strings.Join(id.GetAudiences(), ", "))
	}
	names := make([]string, 0, len(id.GetGrants()))
	for name := range id.GetGrants() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		g := id.GetGrants()[name]
		parts = append(parts, fmt.Sprintf("grant %s (%s:%s; %s)", name, g.GetProvider(), g.GetPrincipal(), strings.Join(g.GetOrkflows(), ", ")))
	}
	if id.AttemptsExtra != nil {
		parts = append(parts, fmt.Sprintf("attempts_extra %d", id.GetAttemptsExtra()))
	}
	if id.GetIdentitiesCurrentOnly() {
		parts = append(parts, "current version only")
	}
	return strings.Join(parts, "; ")
}

func exportIdentity(ns *orkstackv1.Namespace) *yaml.Node {
	id := orNoIdentity(ns.GetIdentity())
	var pairs []*yaml.Node
	if id.AttemptsExtra != nil {
		pairs = append(pairs, plainNode("attempts_extra"), plainNode(strconv.Itoa(int(id.GetAttemptsExtra()))))
	}
	if len(id.GetAudiences()) > 0 {
		pairs = append(pairs, plainNode("audiences"), listNode(id.GetAudiences()))
	}
	if len(id.GetGrants()) > 0 {
		names := make([]string, 0, len(id.GetGrants()))
		for name := range id.GetGrants() {
			names = append(names, name)
		}
		sort.Strings(names)
		var grants []*yaml.Node
		for _, name := range names {
			g := id.GetGrants()[name]
			var fields []*yaml.Node
			if g.GetCurrentOnly() {
				fields = append(fields, plainNode("current_only"), plainNode("true"))
			}
			fields = append(fields, plainNode("orkflows"), listNode(g.GetOrkflows()))
			if len(g.GetOrigins()) > 0 {
				fields = append(fields, plainNode("origins"), listNode(g.GetOrigins()))
			}
			fields = append(fields, plainNode("principal"), strNode(g.GetPrincipal()), plainNode("provider"), strNode(g.GetProvider()))
			grants = append(grants, plainNode(name), mapping(fields...))
		}
		pairs = append(pairs, plainNode("grants"), mapping(grants...))
	}
	if id.GetIdentitiesCurrentOnly() {
		pairs = append(pairs, plainNode("identities_current_only"), plainNode("true"))
	}
	return mapping(pairs...)
}

// keepLiveIdentity carries what of live's identity field the server keeps
// (its id, etag and revision) into desired, whose policy the document set.
func keepLiveIdentity(live, desired *orkstackv1.Namespace) {
	if live.GetIdentity() == nil {
		return
	}
	if desired.Identity == nil {
		desired.Identity = &orkstackv1.NamespaceIdentity{}
	}
	desired.Identity.NamespaceId = live.GetIdentity().GetNamespaceId()
	desired.Identity.Etag = live.GetIdentity().GetEtag()
	desired.Identity.Revision = live.GetIdentity().GetRevision()
}

// orNoIdentity is id, or an empty policy when there is none.
func orNoIdentity(id *orkstackv1.NamespaceIdentity) *orkstackv1.NamespaceIdentity {
	if id == nil {
		return &orkstackv1.NamespaceIdentity{}
	}
	return id
}
