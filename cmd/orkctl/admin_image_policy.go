package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// newAdminImagePolicyCommand groups the deployment's image supply-chain floor
// read and write (image-supply-chain-policy §8.1).
//
// It lives under `admin`, beside `retention`, because it is deployment-wide:
// the floor under every namespace. A namespace may tighten it (`orkctl
// namespace update --require-signed-by cosign:<key>`) and may never loosen it.
func newAdminImagePolicyCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "image-policy",
		Short: "Read or change the deployment's image supply-chain policy",
		Long: "image-policy reads or changes which images this deployment may apply and run:\n" +
			"whether a `uses:` may be a mutable tag, which registries images may come from,\n" +
			"and which cosign public keys a namespace may require by name. A namespace may\n" +
			"tighten this policy (`orkctl namespace update --require-signed-by\n" +
			"cosign:<key>`) and may never loosen it.",
	}
	cmd.AddCommand(newAdminImagePolicyGetCommand(g))
	cmd.AddCommand(newAdminImagePolicySetCommand(g))
	return cmd
}

func newAdminImagePolicyGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Read the deployment's image policy",
		Long: "Read the deployment's image policy from the store, so any replica answers the\n" +
			"same. A deployment that never wrote one has the empty policy: every image is\n" +
			"permitted, as before the policy existed.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.systemClient().GetImagePolicy(ctx, connect.NewRequest(&orkstackv1.GetImagePolicyRequest{}))
			if err != nil {
				return rpcError("admin image-policy get", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderImagePolicy(cmd.OutOrStdout(), resp.Msg.GetPolicy())
		},
	}
}

func newAdminImagePolicySetCommand(g *globalFlags) *cobra.Command {
	var (
		requireDigest    bool
		allowedRegistry  []string
		clearRegistries  bool
		signingKeys      []string
		removeSigningKey []string
	)
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Change the deployment's image policy",
		Long: "set reads the policy, applies the flags given, and writes the whole document\n" +
			"back. A flag left out keeps its current value.\n\n" +
			"--allowed-registry replaces the whole list (repeat it for several prefixes);\n" +
			"--clear-allowed-registries removes the restriction. A prefix is a literal\n" +
			"{registry}/{repository} prefix matched on \"/\" boundaries, e.g. ghcr.io/acme/.\n\n" +
			"--signing-key NAME=PATH adds or replaces a cosign PUBLIC key (cosign.pub; PATH\n" +
			"may be - for stdin). --remove-signing-key NAME removes one; the engine refuses\n" +
			"the removal while a namespace still names it.\n\n" +
			"A list that would exclude an activity of the engine's catalog is refused and\n" +
			"names the activities. Definitions already stored are re-checked at their next\n" +
			"submit; runs already accepted finish on the policy they were submitted under.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			flags := cmd.Flags()
			if clearRegistries && flags.Changed("allowed-registry") {
				return fmt.Errorf("--allowed-registry and --clear-allowed-registries contradict each other: give one")
			}
			for _, entry := range allowedRegistry {
				if entry == "" {
					return fmt.Errorf("--allowed-registry needs a {registry}/{repository} prefix, e.g. ghcr.io/acme/")
				}
			}
			// Every key file is read and parsed BEFORE any round trip, so a
			// wrong file is a local error and nothing reaches the wire.
			added := map[string]string{}
			stdinRead := false
			for _, pair := range signingKeys {
				name, path, ok := strings.Cut(pair, "=")
				if !ok || name == "" || path == "" {
					return fmt.Errorf("invalid --signing-key %q: want NAME=PATH", pair)
				}
				var raw []byte
				var err error
				if path == "-" {
					if stdinRead {
						return fmt.Errorf("--signing-key %s=-: stdin can be read only once", name)
					}
					stdinRead = true
					raw, err = io.ReadAll(cmd.InOrStdin())
				} else {
					raw, err = os.ReadFile(path)
				}
				if err != nil {
					return fmt.Errorf("--signing-key %s: read %s: %w", name, path, err)
				}
				if _, err := task.ParseSigningKey(raw); err != nil {
					return fmt.Errorf("--signing-key %s: %s is not a PEM ECDSA P-256 public key: %v", name, path, err)
				}
				added[name] = string(raw)
			}
			for _, name := range removeSigningKey {
				if _, both := added[name]; both {
					return fmt.Errorf("--signing-key %s and --remove-signing-key %s contradict each other: give one", name, name)
				}
			}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			client := g.systemClient()
			current, err := client.GetImagePolicy(ctx, connect.NewRequest(&orkstackv1.GetImagePolicyRequest{}))
			if err != nil {
				return rpcError("admin image-policy set", err)
			}
			policy := proto.Clone(current.Msg.GetPolicy()).(*orkstackv1.ImagePolicy)
			if policy == nil {
				policy = &orkstackv1.ImagePolicy{}
			}
			policy.UpdatedAt = nil
			if flags.Changed("require-digest") {
				policy.RequireDigest = requireDigest
			}
			if flags.Changed("allowed-registry") {
				policy.AllowedRegistries = allowedRegistry
			}
			if clearRegistries {
				policy.AllowedRegistries = nil
			}
			for _, name := range removeSigningKey {
				if _, ok := policy.GetSigningKeys()[name]; !ok {
					return fmt.Errorf("--remove-signing-key %s: the policy holds no key named %q (known keys: %s)",
						name, name, task.KnownSigningKeys(policy))
				}
				delete(policy.SigningKeys, name)
			}
			if len(added) > 0 && policy.SigningKeys == nil {
				policy.SigningKeys = map[string]string{}
			}
			for name, keyPEM := range added {
				policy.SigningKeys[name] = keyPEM
			}

			resp, err := client.UpdateImagePolicy(ctx, connect.NewRequest(&orkstackv1.UpdateImagePolicyRequest{Policy: policy}))
			if err != nil {
				return rpcError("admin image-policy set", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderImagePolicy(cmd.OutOrStdout(), resp.Msg.GetPolicy())
		},
	}
	cmd.Flags().BoolVar(&requireDigest, "require-digest", false,
		"refuse a `uses:` written as a mutable tag (--require-digest=false clears it); catalog names are exempt")
	cmd.Flags().StringArrayVar(&allowedRegistry, "allowed-registry", nil,
		"a {registry}/{repository} prefix images may come from, e.g. ghcr.io/acme/ (repeatable; replaces the whole list)")
	cmd.Flags().BoolVar(&clearRegistries, "clear-allowed-registries", false,
		"remove the registry restriction: every registry is permitted")
	cmd.Flags().StringArrayVar(&signingKeys, "signing-key", nil,
		"add or replace a cosign public key as NAME=PATH (PATH of - reads stdin; repeatable)")
	cmd.Flags().StringArrayVar(&removeSigningKey, "remove-signing-key", nil,
		"remove the signing key NAME (repeatable; refused while a namespace names it)")
	return cmd
}

// renderImagePolicy prints the policy for `get` and `set` alike. Keys are
// shown by fingerprint, never as PEM. An unset list spells out what it
// means, on allowedPoolsLabel's rule: a dash under a security heading reads
// as the opposite of the truth.
func renderImagePolicy(out io.Writer, policy *orkstackv1.ImagePolicy) error {
	table := newTable(out)
	digest := "no"
	if policy.GetRequireDigest() {
		digest = "yes"
	}
	fmt.Fprintf(table, "require digest:\t%s\n", digest)
	if registries := policy.GetAllowedRegistries(); len(registries) > 0 {
		for i, registry := range registries {
			label := "allowed registries:"
			if i > 0 {
				label = ""
			}
			fmt.Fprintf(table, "%s\t%s\n", label, registry)
		}
	} else {
		fmt.Fprintf(table, "allowed registries:\t(unrestricted)\n")
	}
	names := make([]string, 0, len(policy.GetSigningKeys()))
	for name := range policy.GetSigningKeys() {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		fmt.Fprintf(table, "signing keys:\t(none)\n")
	}
	for i, name := range names {
		label := "signing keys:"
		if i > 0 {
			label = ""
		}
		fingerprint := task.SigningKeyPEMFingerprint(policy.GetSigningKeys()[name])
		if fingerprint == "" {
			fingerprint = "(unparseable key)"
		}
		fmt.Fprintf(table, "%s\t%s  %s\n", label, name, fingerprint)
	}
	if updated := policy.GetUpdatedAt(); updated != nil {
		fmt.Fprintf(table, "updated:\t%s\n", formatTime(updated))
	} else {
		fmt.Fprintf(table, "updated:\tnever (no policy stored; every image is permitted)\n")
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "A namespace may tighten this policy (`orkctl namespace update --require-signed-by")
	fmt.Fprintln(out, "cosign:<key>`) and may never loosen it.")
	return nil
}
