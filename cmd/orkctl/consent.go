package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// consentAsk is --consent given with no value: show what would be consented
// to and ask (docs/TASK_IDENTITY.md, "Consent").
const consentAsk = "?"

const consentHelp = "consent to what the Orkflow asks for, by the digest you were shown (c1:…)"

// addConsentFlag registers --consent. With ask, --consent with no value
// shows what would be consented to and asks; a digest is then given as
// --consent=DIGEST.
func addConsentFlag(cmd *cobra.Command, p *string, ask bool) {
	help := consentHelp
	if ask {
		help += "; with no value, show it and ask"
	}
	cmd.Flags().StringVar(p, "consent", "", help)
	if ask {
		cmd.Flags().Lookup("consent").NoOptDefVal = consentAsk
	}
}

// resolveConsent is the digest a save sends. A digest given is sent as it
// is: the server records it only if it is still what the Orkflow asks for.
// --consent with no value prints what would be consented to, what changed
// since the consent given and the digest, and sends that digest on a yes,
// only at a terminal.
//
// asks is what the version the object follows now asks for; a save that
// changes that version (versionChanges) would be judged on another one, so
// it is not offered and the digest the save hands out is asked for instead.
func (g *globalFlags) resolveConsent(cmd *cobra.Command, flag, what string, asks *orkstackv1.AsksFor, given *orkstackv1.ConsentView, versionChanges bool) (string, error) {
	if flag != consentAsk {
		return flag, nil
	}
	if versionChanges {
		return "", fmt.Errorf("%s: this save changes the version, so what it would consent to is not what is shown now; "+
			"save without --consent to be handed the digest, then give it as --consent=c1:…", what)
	}
	if !g.stdinIsTerminal(cmd) {
		return "", fmt.Errorf("%s: --consent needs a digest here (--consent=c1:…): standard input is not a terminal", what)
	}
	if asks.GetDigest() == "" {
		return "", fmt.Errorf("%s: what the Orkflow asks for could not be read, so there is nothing to consent to; try again", what)
	}
	stderr := cmd.ErrOrStderr()
	fmt.Fprintf(stderr, "The Orkflow asks for audiences %s and grants %s.\n", listOrNone(asks.GetAudiences()), listOrNone(asks.GetGrants()))
	if given.GetDigest() != "" && given.GetDigest() != asks.GetDigest() {
		for _, line := range consentChanges(given, asks) {
			fmt.Fprintf(stderr, "  %s\n", line)
		}
	}
	fmt.Fprintf(stderr, "Digest: %s\nType 'yes' to consent: ", asks.GetDigest())
	answer, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read the answer: %w", err)
	}
	if strings.TrimSpace(answer) != "yes" {
		return "", fmt.Errorf("%s: cancelled", what)
	}
	return asks.GetDigest(), nil
}

// consentChanges says what the Orkflow asks for now that it did not when
// the consent was given, and what it no longer asks for.
func consentChanges(given *orkstackv1.ConsentView, asks *orkstackv1.AsksFor) []string {
	var out []string
	diff := func(what string, was, now []string) {
		for _, n := range now {
			if !slices.Contains(was, n) {
				out = append(out, fmt.Sprintf("now also asks for %s %s", what, n))
			}
		}
		for _, w := range was {
			if !slices.Contains(now, w) {
				out = append(out, fmt.Sprintf("no longer asks for %s %s", what, w))
			}
		}
	}
	diff("audience", given.GetAudiences(), asks.GetAudiences())
	diff("grant", given.GetGrants(), asks.GetGrants())
	if len(out) == 0 {
		out = append(out, "the same audiences and grants, held by other tasks or after other tasks")
	}
	return out
}

func listOrNone(s []string) string {
	if len(s) == 0 {
		return "(none)"
	}
	return strings.Join(s, ", ")
}
