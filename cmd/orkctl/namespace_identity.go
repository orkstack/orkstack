package main

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// identityFlags are `namespace update`'s flags for the namespace's identity
// policy (docs/TASK_IDENTITY.md, "A namespace's identity policy"). The
// server writes the policy in an update of its own, so they are never given
// beside a flag that changes the namespace document.
type identityFlags struct {
	audiences             []string
	clearAudiences        bool
	grants                []string
	grantOrkflows         []string
	grantOrigins          []string
	grantCurrentOnly      []string
	removeGrants          []string
	attemptsExtra         int32
	clearAttemptsExtra    bool
	identitiesCurrentOnly bool
}

var identityFlagNames = []string{"audience", "clear-audiences", "grant", "grant-orkflow", "grant-origin",
	"grant-current-only", "remove-grant", "attempts-extra", "clear-attempts-extra", "identities-current-only"}

func addIdentityFlags(cmd *cobra.Command, f *identityFlags) {
	fl := cmd.Flags()
	fl.StringArrayVar(&f.audiences, "audience", nil, "add a token audience the namespace's tasks may ask for; a trailing * matches any rest (repeatable)")
	fl.BoolVar(&f.clearAudiences, "clear-audiences", false, "remove every audience")
	fl.StringArrayVar(&f.grants, "grant", nil, "set a grant as NAME=PROVIDER:PRINCIPAL (repeatable)")
	fl.StringArrayVar(&f.grantOrkflows, "grant-orkflow", nil, "let an Orkflow use a grant, as NAME=ORKFLOW: a name, * or <owner>/<name> (repeatable)")
	fl.StringArrayVar(&f.grantOrigins, "grant-origin", nil,
		"let a grant serve one kind of start, as NAME=KIND: submit, schedule, trigger, event-source, parent-run or run-again (repeatable)")
	fl.StringArrayVar(&f.grantCurrentOnly, "grant-current-only", nil, "let only a run of its Orkflow's current version use the grant NAME (repeatable)")
	fl.StringArrayVar(&f.removeGrants, "remove-grant", nil, "remove the grant NAME (repeatable)")
	fl.Int32Var(&f.attemptsExtra, "attempts-extra", 0, "how many attempts past a task's own retries may hold its identity, up to identity.attempts.max_extra")
	fl.BoolVar(&f.clearAttemptsExtra, "clear-attempts-extra", false, "take the deployment's identity.attempts.extra again")
	fl.BoolVar(&f.identitiesCurrentOnly, "identities-current-only", false, "give an identity only to a run of its Orkflow's current version")
}

// identityFlagsGiven names the identity flags cmd was given.
func identityFlagsGiven(cmd *cobra.Command) []string {
	var given []string
	for _, name := range identityFlagNames {
		if cmd.Flags().Changed(name) {
			given = append(given, "--"+name)
		}
	}
	return given
}

// checkIdentityFlagsAlone refuses identity flags beside any other flag that
// changes the namespace, before a round trip.
func checkIdentityFlagsAlone(cmd *cobra.Command) error {
	if len(identityFlagsGiven(cmd)) == 0 {
		return nil
	}
	var others []string
	cmd.Flags().Visit(func(fl *pflag.Flag) {
		if !slices.Contains(identityFlagNames, fl.Name) && cmd.LocalFlags().Lookup(fl.Name) != nil {
			others = append(others, "--"+fl.Name)
		}
	})
	if len(others) > 0 {
		return fmt.Errorf("%s change the namespace's identity policy, which is written in an update of its own: give %s in another update",
			strings.Join(identityFlagsGiven(cmd), ", "), strings.Join(others, ", "))
	}
	return nil
}

// applyIdentityFlags edits ns's identity policy as f says.
func applyIdentityFlags(cmd *cobra.Command, ns *orkstackv1.Namespace, f *identityFlags) error {
	if f.clearAudiences && cmd.Flags().Changed("audience") {
		return fmt.Errorf("--audience and --clear-audiences contradict each other: give one")
	}
	if f.clearAttemptsExtra && cmd.Flags().Changed("attempts-extra") {
		return fmt.Errorf("--attempts-extra and --clear-attempts-extra contradict each other: give one")
	}
	for _, g := range f.grants {
		if name, _, _ := strings.Cut(g, "="); slices.Contains(f.removeGrants, name) {
			return fmt.Errorf("--grant %s and --remove-grant %s contradict each other: give one", name, name)
		}
	}
	if ns.Identity == nil {
		ns.Identity = &orkstackv1.NamespaceIdentity{}
	}
	id := ns.Identity
	if f.clearAudiences {
		id.Audiences = nil
	}
	for _, a := range f.audiences {
		if !slices.Contains(id.Audiences, a) {
			id.Audiences = append(id.Audiences, a)
		}
	}
	if id.Grants == nil {
		id.Grants = map[string]*orkstackv1.Grant{}
	}
	for _, g := range f.grants {
		name, rest, ok := strings.Cut(g, "=")
		provider, principal, ok2 := strings.Cut(rest, ":")
		if !ok || !ok2 || name == "" || provider == "" || principal == "" {
			return fmt.Errorf("invalid --grant %q: give NAME=PROVIDER:PRINCIPAL", g)
		}
		grant := id.Grants[name]
		if grant == nil {
			grant = &orkstackv1.Grant{}
			id.Grants[name] = grant
		}
		grant.Provider, grant.Principal = provider, principal
	}
	pairs := func(flag string, values []string, apply func(*orkstackv1.Grant, string)) error {
		for _, v := range values {
			name, value, ok := strings.Cut(v, "=")
			if !ok || name == "" || value == "" {
				return fmt.Errorf("invalid --%s %q: give NAME=VALUE", flag, v)
			}
			grant := id.Grants[name]
			if grant == nil {
				return fmt.Errorf("--%s %q names no grant: give --grant %s=PROVIDER:PRINCIPAL first", flag, v, name)
			}
			apply(grant, value)
		}
		return nil
	}
	if err := pairs("grant-orkflow", f.grantOrkflows, func(g *orkstackv1.Grant, v string) {
		if !slices.Contains(g.Orkflows, v) {
			g.Orkflows = append(g.Orkflows, v)
		}
	}); err != nil {
		return err
	}
	if err := pairs("grant-origin", f.grantOrigins, func(g *orkstackv1.Grant, v string) {
		if !slices.Contains(g.Origins, v) {
			g.Origins = append(g.Origins, v)
		}
	}); err != nil {
		return err
	}
	for _, name := range f.grantCurrentOnly {
		grant := id.Grants[name]
		if grant == nil {
			return fmt.Errorf("--grant-current-only %q names no grant", name)
		}
		grant.CurrentOnly = true
	}
	for _, name := range f.removeGrants {
		if _, ok := id.Grants[name]; !ok {
			return fmt.Errorf("--remove-grant %q names no grant", name)
		}
		delete(id.Grants, name)
	}
	if len(id.Grants) == 0 {
		id.Grants = nil
	}
	if cmd.Flags().Changed("attempts-extra") {
		v := f.attemptsExtra
		id.AttemptsExtra = &v
	}
	if f.clearAttemptsExtra {
		id.AttemptsExtra = nil
	}
	if cmd.Flags().Changed("identities-current-only") {
		id.IdentitiesCurrentOnly = f.identitiesCurrentOnly
	}
	return nil
}

// renderIdentityPolicy writes a namespace's identity policy, when it has one.
func renderIdentityPolicy(out io.Writer, id *orkstackv1.NamespaceIdentity) {
	if id == nil {
		return
	}
	if len(id.GetAudiences())+len(id.GetGrants()) == 0 && id.AttemptsExtra == nil && !id.GetIdentitiesCurrentOnly() {
		return
	}
	if len(id.GetAudiences()) > 0 {
		fmt.Fprintf(out, "Identity audiences:\t%s\n", strings.Join(id.GetAudiences(), ", "))
	}
	if id.AttemptsExtra != nil {
		fmt.Fprintf(out, "Identity attempts extra:\t%d\n", id.GetAttemptsExtra())
	}
	if id.GetIdentitiesCurrentOnly() {
		fmt.Fprintf(out, "Identities:\tcurrent version only\n")
	}
	names := make([]string, 0, len(id.GetGrants()))
	for name := range id.GetGrants() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		g := id.GetGrants()[name]
		line := fmt.Sprintf("%s:%s; orkflows %s", g.GetProvider(), g.GetPrincipal(), strings.Join(g.GetOrkflows(), ", "))
		if len(g.GetOrigins()) > 0 {
			line += "; origins " + strings.Join(g.GetOrigins(), ", ")
		}
		if g.GetCurrentOnly() {
			line += "; current version only"
		}
		fmt.Fprintf(out, "Grant %s:\t%s\n", name, line)
	}
}
