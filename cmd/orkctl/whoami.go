package main

import (
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

func (g *globalFlags) authClient() orkstackv1connect.AuthServiceClient {
	return orkstackv1connect.NewAuthServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// newWhoamiCommand asks the engine who this caller is and what it holds
// (docs/AUTHENTICATION.md, "Who am I").
func newWhoamiCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Show who the engine verified this caller as, and what it may do",
		Long: "whoami asks the engine which principal it verified this caller as, how, and\n" +
			"what that principal holds: each binding that matched it, with only the subject\n" +
			"that matched, and its permissions by scope. It needs no permission, and an\n" +
			"engine in asserted mode verifies nobody.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.authClient().GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
			if connect.CodeOf(err) == connect.CodeNotFound || connect.CodeOf(err) == connect.CodeUnimplemented {
				return fmt.Errorf("whoami: this engine verifies no caller (auth mode asserted): it records what ORK_PRINCIPAL or a gateway asserts")
			}
			if err != nil {
				return rpcError("whoami", err)
			}
			m := resp.Msg
			if g.json {
				return writeJSON(cmd.OutOrStdout(), m)
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "principal:  %s (verified, %s", m.GetPrincipalId(), m.GetMethod())
			if m.GetIssuer() != "" {
				fmt.Fprintf(out, ", issuer %s", m.GetIssuer())
			}
			fmt.Fprintln(out, ")")
			if m.GetDisplay() != "" {
				fmt.Fprintf(out, "name:       %s\n", m.GetDisplay())
			}
			if len(m.GetGroups()) > 0 {
				fmt.Fprintf(out, "groups:     %s\n", strings.Join(m.GetGroups(), ", "))
			}
			if len(m.GetBindings()) == 0 {
				fmt.Fprintln(out, "bindings:   none: this principal holds nothing")
			}
			for _, b := range m.GetBindings() {
				fmt.Fprintf(out, "binding:    %s: %s at %s, as %s\n", b.GetBinding(), b.GetRole(), b.GetScope(), b.GetSubject())
			}
			for _, sp := range m.GetPermissions() {
				fmt.Fprintf(out, "at %s: %s\n", sp.GetScope(), strings.Join(sp.GetPermissions(), " "))
			}
			fmt.Fprintf(out, "access document revision %d\n", m.GetAccessRevision())
			return nil
		},
	}
}
