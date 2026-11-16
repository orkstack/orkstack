package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

func (g *globalFlags) identityClient() orkstackv1connect.IdentityServiceClient {
	return orkstackv1connect.NewIdentityServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// newAdminIdentityCommand groups the task identity issuer
// (docs/TASK_IDENTITY.md, "Administering the issuer").
func newAdminIdentityCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "identity",
		Short: "Read and administer the task identity issuer",
		Long: "identity reads the task identity issuer: the documents it publishes and the\n" +
			"keys it signs with. Every answer is the answering engine's own.",
	}
	issuer := &cobra.Command{Use: "issuer", Short: "Read or export the issuer's documents"}
	issuer.AddCommand(newAdminIdentityIssuerGetCommand(g), newAdminIdentityIssuerExportCommand(g))
	keys := &cobra.Command{Use: "keys", Short: "List or retire the issuer's signing keys"}
	keys.AddCommand(newAdminIdentityKeysListCommand(g), newAdminIdentityKeysRetireCommand(g))
	cmd.AddCommand(issuer, keys)
	return cmd
}

func newAdminIdentityIssuerGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Show the issuer and what the engine last read",
		Long: "get shows the issuer URL, the subject format and trust domain, when the engine\n" +
			"last read the published signing keys, and what the issuer URL served when the\n" +
			"engine last checked it before a key's first signature.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.identityClient().GetIssuer(ctx, connect.NewRequest(&orkstackv1.GetIssuerRequest{}))
			if err != nil {
				return rpcError("admin identity issuer get", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			renderIssuer(cmd.OutOrStdout(), resp.Msg)
			return nil
		},
	}
}

func newAdminIdentityIssuerExportCommand(g *globalFlags) *cobra.Command {
	var out string
	cmd := &cobra.Command{
		Use:   "export --out DIR",
		Short: "Write the issuer's documents for an HTTPS host",
		Long: "export writes the issuer's documents into DIR, laid out as the issuer URL\n" +
			"serves them: .well-known/openid-configuration and jwks.json, and\n" +
			"spiffe-bundle.json when a trust domain is set. Upload DIR to the issuer URL,\n" +
			"and export again after every key change. Files already there are replaced.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if out == "" {
				return errors.New("admin identity issuer export: --out DIR is required")
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.identityClient().GetIssuer(ctx, connect.NewRequest(&orkstackv1.GetIssuerRequest{}))
			if err != nil {
				return rpcError("admin identity issuer export", err)
			}
			paths, err := exportIssuer(out, resp.Msg)
			for _, p := range paths {
				if err != nil {
					fmt.Fprintln(cmd.ErrOrStderr(), "replaced:", p)
				}
			}
			if err != nil {
				return fmt.Errorf("admin identity issuer export: %w", err)
			}
			for _, p := range paths {
				fmt.Fprintln(cmd.OutOrStdout(), p)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "the directory to write the documents into")
	return cmd
}

func newAdminIdentityKeysListCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List the signing keys",
		Long: "list shows every published signing key and every key the engine holds: when it\n" +
			"was published and began to sign, whether it is retired, whether this engine\n" +
			"would sign with it and why not, and how many of the engines that hold it are\n" +
			"polling now.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.identityClient().ListSigningKeys(ctx, connect.NewRequest(&orkstackv1.ListSigningKeysRequest{}))
			if err != nil {
				return rpcError("admin identity keys list", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			renderSigningKeys(cmd.OutOrStdout(), resp.Msg.GetKeys())
			return nil
		},
	}
}

func newAdminIdentityKeysRetireCommand(g *globalFlags) *cobra.Command {
	var (
		yes             bool
		compromisedFrom string
	)
	cmd := &cobra.Command{
		Use:   "retire [--compromised-from T] [--] KID",
		Short: "Retire a signing key",
		Long: "retire retires a signing key: each engine stops signing with it at its next\n" +
			"read of the published keys, within a minute while the store answers, and it is\n" +
			"no longer served. It cannot be undone. Export and upload the issuer's documents\n" +
			"afterwards. It asks for confirmation first, and refuses when standard input is\n" +
			"not a terminal unless --yes is given.\n\n" +
			"--compromised-from T (RFC 3339) also retires the key as compromised from T,\n" +
			"which may lie in the past: what it signed for an instant before T still\n" +
			"verifies, and nothing from T on does. Choose the earliest moment the key may\n" +
			"have leaked. A run force-cancelled under it from T on reads as terminated\n" +
			"outside OrkStack and cannot be resumed; ask again the force cancels you still\n" +
			"need. It may be added to a key retired before, and only ever moves earlier.\n\n" +
			"A key id is base64url and may begin with \"-\": give it after \"--\", with\n" +
			"every flag before the \"--\", as in\n" +
			"`orkctl admin identity keys retire --yes --server URL -- -ZXD…`.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := &orkstackv1.RetireSigningKeyRequest{KeyId: args[0]}
			prompt := fmt.Sprintf("Retiring signing key %s: no engine signs with it again, and it is no longer served.\n"+
				"If it is the only active key, nothing signs until another key qualifies. This cannot be undone.", args[0])
			if compromisedFrom != "" {
				from, err := time.Parse(time.RFC3339, compromisedFrom)
				if err != nil {
					return fmt.Errorf("admin identity keys retire: --compromised-from %q is not an RFC 3339 instant", compromisedFrom)
				}
				req.CompromisedFrom = timestamppb.New(from)
				prompt += fmt.Sprintf("\nAs compromised from %s: nothing it signed from then on verifies. Runs force-cancelled under it\n"+
					"from then on read as terminated outside OrkStack and cannot be resumed; ask again the force\n"+
					"cancels you still need. Choose the earliest moment the key may have leaked.", from.UTC().Format(time.RFC3339))
			}
			if err := confirmDestructive(cmd, g, fmt.Sprintf("retire signing key %s", args[0]), prompt, yes); err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.identityClient().RetireSigningKey(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("admin identity keys retire", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			k := resp.Msg.GetKey()
			fmt.Fprintf(cmd.OutOrStdout(), "Retired %s at %s by %s. Export and upload the issuer's documents now.\n",
				k.GetKeyId(), formatTime(k.GetRetiredAt()), orDash(k.GetRetiredBy()))
			return nil
		},
	}
	addYesFlag(cmd, &yes)
	cmd.Flags().StringVar(&compromisedFrom, "compromised-from", "", "retire the key as compromised from this RFC 3339 instant, the earliest it may have leaked")
	cmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		return fmt.Errorf("%w (a key id that begins with \"-\" goes after \"--\", and every flag before it: orkctl admin identity keys retire [flags] -- KID)", err)
	})
	return cmd
}

func renderIssuer(out io.Writer, m *orkstackv1.GetIssuerResponse) {
	if !m.GetEnabled() {
		fmt.Fprintln(out, "Task identity is off on this engine: identity.issuer is not set.")
		return
	}
	yesNo := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	fmt.Fprintf(out, "Issuer:            %s\n", m.GetIssuer())
	fmt.Fprintf(out, "Subject format:    %s\n", m.GetSubjectFormat())
	fmt.Fprintf(out, "Trust domain:      %s\n", orDash(m.GetTrustDomain()))
	fmt.Fprintf(out, "Serves discovery:  %s\n", yesNo(m.GetServeDiscovery()))
	fmt.Fprintf(out, "Keys read:         %s (sequence %d)\n", formatTime(m.GetKeysReadAt()), m.GetKeysSequence())
	if e := m.GetKeysReadError(); e != "" {
		fmt.Fprintf(out, "Last read failed:  %s\n", e)
	}
	if m.GetLiveCheckedAt() != nil || m.GetLiveError() != "" {
		fmt.Fprintf(out, "Issuer URL read:   %s\n", formatTime(m.GetLiveCheckedAt()))
		if e := m.GetLiveError(); e != "" {
			fmt.Fprintf(out, "  failed:          %s\n", e)
		} else {
			fmt.Fprintf(out, "  served keys:     %s\n", orDash(strings.Join(m.GetLiveKeyIds(), ", ")))
		}
	}
}

func renderSigningKeys(out io.Writer, keys []*orkstackv1.SigningKey) {
	if len(keys) == 0 {
		fmt.Fprintln(out, "No signing key is published or held by this engine.")
		return
	}
	table := newTable(out)
	fmt.Fprintln(table, "KEY ID\tALG\tPUBLISHED\tACTIVATED\tHERE\tSTATE\tHOLDERS LIVE")
	for _, k := range keys {
		here := "-"
		if k.GetHeldHere() {
			here = "yes"
		}
		state := "signs"
		switch {
		case k.GetRetiredAt() != nil:
			state = "retired by " + orDash(k.GetRetiredBy()) + " at " + formatTime(k.GetRetiredAt())
		case !k.GetActive():
			state = orDash(k.GetInactiveReason())
		}
		if k.GetUnheld() {
			state += "; no engine holds it lately"
		}
		live := 0
		for _, h := range k.GetHolders() {
			if h.GetLive() {
				live++
			}
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%d/%d\n", k.GetKeyId(), k.GetAlgorithm(), formatTime(k.GetPublishedAt()),
			formatTime(k.GetActivatedAt()), here, state, live, len(k.GetHolders()))
	}
	table.Flush()
}

// exportIssuer writes the issuer's documents under dir as the issuer URL
// serves them, and answers the paths written.
func exportIssuer(dir string, m *orkstackv1.GetIssuerResponse) ([]string, error) {
	if !m.GetEnabled() {
		return nil, errors.New("task identity is off on this engine: identity.issuer is not set")
	}
	// A copy that is not current would replace the live key set with an
	// incomplete one and break every relying party's verification.
	if m.GetKeysReadAt() == nil || m.GetJwks() == "" {
		return nil, errors.New("this engine has not read the published signing keys yet; export from an engine that has (`admin identity issuer get`)")
	}
	if e := m.GetKeysReadError(); e != "" {
		return nil, fmt.Errorf("this engine's last read of the published signing keys failed (%s); export from an engine that reads them", e)
	}
	docs := []struct{ rel, body string }{
		{filepath.Join(".well-known", "openid-configuration"), m.GetDiscovery()},
		{"jwks.json", m.GetJwks()},
	}
	if m.GetTrustDomain() != "" {
		docs = append(docs, struct{ rel, body string }{"spiffe-bundle.json", m.GetSpiffeBundle()})
	}
	for _, d := range docs {
		if strings.TrimSpace(d.body) == "" {
			return nil, fmt.Errorf("the engine answered no %s; nothing was written", d.rel)
		}
	}
	// Every document is staged beside its place first, and only then are
	// they renamed into it, so a failure leaves the set as it was.
	var staged []string
	defer func() {
		for _, tmp := range staged {
			_ = os.Remove(tmp) // a stage left over after a failure
		}
	}()
	for _, d := range docs {
		path := filepath.Join(dir, d.rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		tmp, err := stage(path, []byte(d.body))
		if err != nil {
			return nil, err
		}
		staged = append(staged, tmp)
	}
	var written []string
	for i, d := range docs {
		path := filepath.Join(dir, d.rel)
		if err := os.Rename(staged[i], path); err != nil {
			return written, fmt.Errorf("%w; already replaced: %s", err, strings.Join(written, ", "))
		}
		written = append(written, path)
	}
	staged = nil
	return written, nil
}

// stage writes body to a file beside path, synced, and answers its name.
func stage(path string, body []byte) (string, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	_, err = tmp.Write(body)
	if err == nil {
		err = tmp.Chmod(0o644)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(name) // the stage of a write that failed
		return "", err
	}
	return name, nil
}
