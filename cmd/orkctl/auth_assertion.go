package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/jwt"
)

// newAuthCommand groups what a client of another tool needs to
// authenticate as orkctl would.
func newAuthCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "auth", Short: "Credentials for requests another tool makes"}
	cmd.AddCommand(newAuthAssertionCommand(g))
	return cmd
}

// newAuthAssertionCommand prints one service-account assertion bound to a
// request another tool will make (authn-authz.md §3.2): the same method,
// URL and body, within a minute, once.
func newAuthAssertionCommand(g *globalFlags) *cobra.Command {
	var method, target, bodyFile string
	cmd := &cobra.Command{
		Use:   "assertion",
		Short: "Print one service-account assertion bound to one request",
		Long: "Print one assertion of --service-account, signed with --service-account-key, bound to the request\n" +
			"another tool will make: its method, its URL with its query (not its fragment) and its body.\n" +
			"Send it as `Authorization: Bearer <assertion>` within a minute; the engine accepts it once.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if g.creds.signer == nil {
				if g.creds.serviceAccount != "" {
					return errors.New("auth assertion: a client certificate or token file is given, and takes precedence over --service-account; drop it")
				}
				return errors.New("auth assertion: give --service-account and --service-account-key")
			}
			if !strings.HasPrefix(target, "https://") && !loopbackServer(target) {
				return fmt.Errorf("auth assertion: --url %q is not an https URL or a loopback http one", target)
			}
			var body []byte
			if bodyFile != "" {
				f, err := os.Open(bodyFile)
				if err != nil {
					return fmt.Errorf("auth assertion: --body-file: %w", err)
				}
				body, err = io.ReadAll(io.LimitReader(f, authn.AssertionMaxBody+1))
				_ = f.Close()
				if err != nil {
					return fmt.Errorf("auth assertion: --body-file: %w", err)
				}
				if len(body) > authn.AssertionMaxBody {
					return fmt.Errorf("auth assertion: --body-file is over the %d bytes an assertion is signed for", authn.AssertionMaxBody)
				}
			}
			token, err := g.creds.signer.SignRequest(jwt.AssertionRequest{Account: g.creds.serviceAccount,
				Audience: strings.TrimSuffix(g.server, "/"), Method: strings.ToUpper(method), URL: target, Body: body}, time.Now(), nil)
			if err != nil {
				return fmt.Errorf("auth assertion: %w", err)
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), token)
			return err
		},
	}
	cmd.Flags().StringVar(&method, "method", "", "the request's HTTP method")
	cmd.Flags().StringVar(&target, "url", "", "the request's URL")
	cmd.Flags().StringVar(&bodyFile, "body-file", "", "a file holding the request's exact body (none: an empty body)")
	_ = cmd.MarkFlagRequired("method")
	_ = cmd.MarkFlagRequired("url")
	return cmd
}
