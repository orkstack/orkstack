package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The access commands (docs/ACCESS.md). Every write reads the document
// first and sends back the etag it read, so a change made meanwhile is
// refused rather than overwritten.

// currentAccess reads the access document as GetAccess answers it.
func (g *globalFlags) currentAccess(ctx context.Context, what string) (*orkstackv1.AccessDocument, error) {
	resp, err := g.accessClient().GetAccess(ctx, connect.NewRequest(&orkstackv1.GetAccessRequest{}))
	if err != nil {
		return nil, rpcError(what, err)
	}
	return resp.Msg.GetDocument(), nil
}

// bindingEtag is the etag a binding write sends back: the document's, or
// none for a delegate (access.bind) who may not read the document, whose
// write the engine then makes against a fresh read of its own.
func (g *globalFlags) bindingEtag(ctx context.Context, what string) (string, error) {
	resp, err := g.accessClient().GetAccess(ctx, connect.NewRequest(&orkstackv1.GetAccessRequest{}))
	switch {
	case connect.CodeOf(err) == connect.CodePermissionDenied:
		return "", nil
	case err != nil:
		return "", rpcError(what, err)
	}
	return resp.Msg.GetDocument().GetEtag(), nil
}

func newAdminRoleCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "role", Short: "List, show, create, update and delete roles"}
	cmd.AddCommand(
		&cobra.Command{
			Use: "list", Short: "List the built-in and custom roles", Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				doc, err := g.currentAccess(ctx, "admin role list")
				if err != nil {
					return err
				}
				roles := append(slices.Clone(doc.GetBuiltInRoles()), doc.GetRoles()...)
				if g.json {
					return writeJSON(cmd.OutOrStdout(), &orkstackv1.AccessDocument{Roles: roles})
				}
				for _, r := range roles {
					kind := "custom"
					if r.GetBuiltIn() {
						kind = "built-in"
					}
					fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%d permissions\n", r.GetName(), kind, len(r.GetPermissions()))
				}
				return nil
			},
		},
		&cobra.Command{
			Use: "get NAME", Short: "Show one role and its permissions", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				doc, err := g.currentAccess(ctx, "admin role get")
				if err != nil {
					return err
				}
				for _, r := range append(slices.Clone(doc.GetBuiltInRoles()), doc.GetRoles()...) {
					if r.GetName() != args[0] {
						continue
					}
					if g.json {
						return writeJSON(cmd.OutOrStdout(), r)
					}
					out := cmd.OutOrStdout()
					fmt.Fprintf(out, "role:        %s\n", r.GetName())
					if d := r.GetDescription(); d != "" {
						fmt.Fprintf(out, "description: %s\n", d)
					}
					for _, p := range r.GetPermissions() {
						fmt.Fprintf(out, "permission:  %s\n", p)
					}
					return nil
				}
				return fmt.Errorf("admin role get: role %q does not exist", args[0])
			},
		},
		roleWriteCommand(g, "create"),
		roleWriteCommand(g, "update"),
		&cobra.Command{
			Use: "delete NAME", Short: "Delete a custom role no binding names", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				doc, err := g.currentAccess(ctx, "admin role delete")
				if err != nil {
					return err
				}
				if _, err := g.accessClient().DeleteRole(ctx, connect.NewRequest(&orkstackv1.DeleteRoleRequest{Name: args[0], Etag: doc.GetEtag()})); err != nil {
					return rpcError("admin role delete", err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "deleted role %s\n", args[0])
				return nil
			},
		},
	)
	return cmd
}

func roleWriteCommand(g *globalFlags, verb string) *cobra.Command {
	var perms []string
	var description string
	cmd := &cobra.Command{
		Use:   verb + " NAME --permission P...",
		Short: strings.ToUpper(verb[:1]) + verb[1:] + " a custom role; update replaces its permissions",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			doc, err := g.currentAccess(ctx, "admin role "+verb)
			if err != nil {
				return err
			}
			role := &orkstackv1.Role{Name: args[0], Description: description, Permissions: perms}
			if verb == "create" {
				_, err = g.accessClient().CreateRole(ctx, connect.NewRequest(&orkstackv1.CreateRoleRequest{Role: role, Etag: doc.GetEtag()}))
			} else {
				_, err = g.accessClient().UpdateRole(ctx, connect.NewRequest(&orkstackv1.UpdateRoleRequest{Role: role, Etag: doc.GetEtag()}))
			}
			if err != nil {
				return rpcError("admin role "+verb, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%sd role %s\n", verb, args[0])
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&perms, "permission", nil, "a permission the role holds (repeatable)")
	cmd.Flags().StringVar(&description, "description", "", "what the role is for")
	_ = cmd.MarkFlagRequired("permission")
	return cmd
}

func newAdminBindingCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "binding", Short: "List, create, update and delete bindings"}
	cmd.AddCommand(
		&cobra.Command{
			Use: "list", Short: "List the bindings, configuration's included", Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				doc, err := g.currentAccess(ctx, "admin binding list")
				if err != nil {
					return err
				}
				bindings := append(slices.Clone(doc.GetBindings()), doc.GetConfiguredBindings()...)
				if g.json {
					return writeJSON(cmd.OutOrStdout(), &orkstackv1.AccessDocument{Bindings: bindings})
				}
				for _, b := range bindings {
					origin := bindingPinLabel(b)
					if b.GetFromConfiguration() {
						origin = "configuration"
					}
					fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\t%s\n", b.GetName(), b.GetRole(), b.GetScope(),
						strings.Join(b.GetSubjects(), ","), origin)
				}
				return nil
			},
		},
		bindingWriteCommand(g, "create"),
		bindingWriteCommand(g, "update"),
		&cobra.Command{
			Use: "delete NAME", Short: "Delete a binding", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				etag, err := g.bindingEtag(ctx, "admin binding delete")
				if err != nil {
					return err
				}
				if _, err := g.accessClient().DeleteBinding(ctx, connect.NewRequest(&orkstackv1.DeleteBindingRequest{Name: args[0], Etag: etag})); err != nil {
					return rpcError("admin binding delete", err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "deleted binding %s\n", args[0])
				return nil
			},
		},
	)
	return cmd
}

// bindingPinLabel says what a binding matches its scope by: the
// incarnation it is pinned to, its name, or a pin to an incarnation that is
// gone, which matches nothing until `--repin`.
func bindingPinLabel(b *orkstackv1.Binding) string {
	switch b.GetPin() {
	case orkstackv1.BindingPin_BINDING_PIN_PINNED:
		return "pinned " + b.GetScopeId()
	case orkstackv1.BindingPin_BINDING_PIN_STALE:
		return "stale: pinned to " + b.GetScopeId() + ", which is gone; run `orkctl admin binding update " + b.GetName() + " --repin`"
	case orkstackv1.BindingPin_BINDING_PIN_BY_NAME:
		return "by name"
	}
	return ""
}

func bindingWriteCommand(g *globalFlags, verb string) *cobra.Command {
	var role, scope string
	var subjects []string
	var repin bool
	cmd := &cobra.Command{
		Use:   verb + " NAME --role R --scope S --subject X...",
		Short: strings.ToUpper(verb[:1]) + verb[1:] + " a binding; update replaces it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			etag, err := g.bindingEtag(ctx, "admin binding "+verb)
			if err != nil {
				return err
			}
			b := &orkstackv1.Binding{Name: args[0], Role: role, Scope: scope, Subjects: subjects}
			if verb == "create" {
				_, err = g.accessClient().CreateBinding(ctx, connect.NewRequest(&orkstackv1.CreateBindingRequest{Binding: b, Etag: etag}))
			} else {
				_, err = g.accessClient().UpdateBinding(ctx, connect.NewRequest(&orkstackv1.UpdateBindingRequest{Binding: b, Etag: etag, Repin: repin}))
			}
			if err != nil {
				return rpcError("admin binding "+verb, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%sd binding %s: %s at %s\n", verb, args[0], role, scope)
			return nil
		},
	}
	cmd.Flags().StringVar(&role, "role", "", "the role the binding grants")
	cmd.Flags().StringVar(&scope, "scope", "", "deployment, namespace:N, orkflow:N/O or pool:P")
	cmd.Flags().StringArrayVar(&subjects, "subject", nil, "a subject it grants the role to (repeatable): user:, group:, claims:, serviceaccount: or cert:")
	for _, f := range []string{"role", "scope", "subject"} {
		_ = cmd.MarkFlagRequired(f)
	}
	if verb == "update" {
		cmd.Flags().BoolVar(&repin, "repin", false, "pin the binding again to what exists under its scope now")
	}
	return cmd
}

func newAdminServiceAccountCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "service-account", Short: "List, show, create and delete service accounts and their keys"}
	find := func(ctx context.Context, what, name string) (*orkstackv1.AccessDocument, *orkstackv1.ServiceAccount, error) {
		doc, err := g.currentAccess(ctx, what)
		if err != nil {
			return nil, nil, err
		}
		for _, sa := range doc.GetServiceAccounts() {
			if sa.GetName() == name {
				return doc, sa, nil
			}
		}
		return doc, nil, fmt.Errorf("%s: service account %q does not exist", what, name)
	}
	var description, publicKey, generate string
	create := &cobra.Command{
		Use: "create NAME [--public-key FILE | --generate-key FILE]", Short: "Create a service account, with a key given or generated", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if publicKey != "" && generate != "" {
				return errors.New("admin service-account create: --public-key and --generate-key are exclusive")
			}
			sa := &orkstackv1.ServiceAccount{Name: args[0], Description: description}
			if publicKey != "" || generate != "" {
				key, err := accountKey(publicKey, generate)
				if err != nil {
					return fmt.Errorf("admin service-account create: %w", err)
				}
				sa.Keys = []*orkstackv1.ServiceAccountKey{{PublicKeyPem: key}}
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			doc, err := g.currentAccess(ctx, "admin service-account create")
			if err != nil {
				return err
			}
			resp, err := g.accessClient().CreateServiceAccount(ctx, connect.NewRequest(&orkstackv1.CreateServiceAccountRequest{ServiceAccount: sa, Etag: doc.GetEtag()}))
			if err != nil {
				return rpcError("admin service-account create", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "created service account %s\n", args[0])
			for _, k := range resp.Msg.GetServiceAccount().GetKeys() {
				fmt.Fprintf(cmd.OutOrStdout(), "key: %s\n", k.GetKid())
			}
			if generate != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "private key written to %s; it is shown nowhere else\n", generate)
			}
			return nil
		},
	}
	create.Flags().StringVar(&description, "description", "", "what the account is for")
	create.Flags().StringVar(&publicKey, "public-key", "", "a PEM public key file")
	create.Flags().StringVar(&generate, "generate-key", "", "generate a P-256 key pair and write its private key to this file, once")

	key := &cobra.Command{Use: "key", Short: "Add or remove a service account's key"}
	var addFile string
	add := &cobra.Command{
		Use: "add NAME --public-key FILE", Short: "Add a public key to a service account", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pemKey, err := readKeyFile(addFile)
			if err != nil {
				return fmt.Errorf("admin service-account key add: %w", err)
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			doc, sa, err := find(ctx, "admin service-account key add", args[0])
			if err != nil {
				return err
			}
			next := proto.Clone(sa).(*orkstackv1.ServiceAccount)
			next.Keys = append(next.Keys, &orkstackv1.ServiceAccountKey{PublicKeyPem: pemKey})
			if _, err := g.accessClient().UpdateServiceAccount(ctx, connect.NewRequest(&orkstackv1.UpdateServiceAccountRequest{ServiceAccount: next, Etag: doc.GetEtag()})); err != nil {
				return rpcError("admin service-account key add", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "added a key to service account %s\n", args[0])
			return nil
		},
	}
	add.Flags().StringVar(&addFile, "public-key", "", "a PEM public key file")
	_ = add.MarkFlagRequired("public-key")
	remove := &cobra.Command{
		Use: "remove NAME KID", Short: "Remove a key, named by its thumbprint, from a service account", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			doc, sa, err := find(ctx, "admin service-account key remove", args[0])
			if err != nil {
				return err
			}
			next := proto.Clone(sa).(*orkstackv1.ServiceAccount)
			i := slices.IndexFunc(next.Keys, func(k *orkstackv1.ServiceAccountKey) bool { return k.GetKid() == args[1] })
			if i < 0 {
				return fmt.Errorf("admin service-account key remove: service account %q has no key %q", args[0], args[1])
			}
			next.Keys = slices.Delete(next.Keys, i, i+1)
			if _, err := g.accessClient().UpdateServiceAccount(ctx, connect.NewRequest(&orkstackv1.UpdateServiceAccountRequest{ServiceAccount: next, Etag: doc.GetEtag()})); err != nil {
				return rpcError("admin service-account key remove", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "removed key %s from service account %s\n", args[1], args[0])
			return nil
		},
	}
	key.AddCommand(add, remove)

	cmd.AddCommand(
		&cobra.Command{
			Use: "list", Short: "List the service accounts", Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				doc, err := g.currentAccess(ctx, "admin service-account list")
				if err != nil {
					return err
				}
				if g.json {
					return writeJSON(cmd.OutOrStdout(), &orkstackv1.AccessDocument{ServiceAccounts: doc.GetServiceAccounts()})
				}
				for _, sa := range doc.GetServiceAccounts() {
					fmt.Fprintf(cmd.OutOrStdout(), "%s\t%d keys\t%s\n", sa.GetName(), len(sa.GetKeys()), sa.GetDescription())
				}
				return nil
			},
		},
		&cobra.Command{
			Use: "get NAME", Short: "Show a service account and its keys", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				_, sa, err := find(ctx, "admin service-account get", args[0])
				if err != nil {
					return err
				}
				if g.json {
					return writeJSON(cmd.OutOrStdout(), sa)
				}
				out := cmd.OutOrStdout()
				fmt.Fprintf(out, "service account: %s\n", sa.GetName())
				for _, k := range sa.GetKeys() {
					added := ""
					if k.GetAddedAt() != nil {
						added = " added " + k.GetAddedAt().AsTime().Format("2006-01-02T15:04:05Z07:00")
					}
					fmt.Fprintf(out, "key: %s%s\n", k.GetKid(), added)
				}
				return nil
			},
		},
		create,
		&cobra.Command{
			Use: "delete NAME", Short: "Delete a service account", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				doc, err := g.currentAccess(ctx, "admin service-account delete")
				if err != nil {
					return err
				}
				if _, err := g.accessClient().DeleteServiceAccount(ctx, connect.NewRequest(&orkstackv1.DeleteServiceAccountRequest{Name: args[0], Etag: doc.GetEtag()})); err != nil {
					return rpcError("admin service-account delete", err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "deleted service account %s\n", args[0])
				return nil
			},
		},
		key,
	)
	return cmd
}

// accountKey is the PEM public key a create carries: read from a file, or
// generated, its private key written once to a new file of mode 0600.
func accountKey(publicFile, generateFile string) (string, error) {
	if publicFile != "" {
		return readKeyFile(publicFile)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(generateFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("write the private key: %w", err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: private}); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("write the private key: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("write the private key: %w", err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})), nil
}

// maxKeyFileBytes bounds a public key file: one PEM key.
const maxKeyFileBytes = 64 << 10

func readKeyFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxKeyFileBytes+1))
	if err != nil {
		return "", err
	}
	if len(raw) > maxKeyFileBytes {
		return "", fmt.Errorf("%s is larger than one PEM key (%d bytes)", path, maxKeyFileBytes)
	}
	return string(raw), nil
}

func newAdminPrincipalCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "principal", Short: "Disable and enable principals"}
	set := func(verb string, disabled bool) *cobra.Command {
		return &cobra.Command{
			Use: verb + " PRINCIPAL", Short: strings.ToUpper(verb[:1]) + verb[1:] + " a principal, whatever it presents", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				doc, err := g.currentAccess(ctx, "admin principal "+verb)
				if err != nil {
					return err
				}
				if _, err := g.accessClient().SetPrincipalDisabled(ctx, connect.NewRequest(&orkstackv1.SetPrincipalDisabledRequest{
					PrincipalId: args[0], Disabled: disabled, Etag: doc.GetEtag()})); err != nil {
					return rpcError("admin principal "+verb, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%sd %s\n", verb, args[0])
				return nil
			},
		}
	}
	cmd.AddCommand(set("disable", true), set("enable", false))
	return cmd
}

func newAdminAccessCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "access", Short: "Explain a decision, read the access document's history, and acknowledge a revision written around the API"}
	var ns, orkflow, pool string
	explain := &cobra.Command{
		Use: "explain PRINCIPAL PERMISSION", Short: "Say whether a principal holds a permission, and which bindings decided it", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.accessClient().ExplainAccess(ctx, connect.NewRequest(&orkstackv1.ExplainAccessRequest{
				PrincipalId: args[0], Permission: args[1], Namespace: ns, Orkflow: orkflow, Pool: pool}))
			if err != nil {
				return rpcError("admin access explain", err)
			}
			m := resp.Msg
			if g.json {
				return writeJSON(cmd.OutOrStdout(), m)
			}
			out := cmd.OutOrStdout()
			decision := "allowed"
			if m.GetDecision() == orkstackv1.AccessDecision_ACCESS_DECISION_DENIED {
				decision = "denied (" + m.GetReason() + ")"
			}
			fmt.Fprintf(out, "%s: %s\n", decision, m.GetExplanation())
			for _, c := range m.GetConsidered() {
				fmt.Fprintf(out, "  %s\t%s at %s\t%s", c.GetBinding(), c.GetRole(), c.GetScope(), strings.ToLower(strings.TrimPrefix(c.GetMatch().String(), "BINDING_MATCH_")))
				if len(c.GetClaimsReliedOn()) > 0 {
					fmt.Fprintf(out, "\tclaims: %s", strings.Join(c.GetClaimsReliedOn(), ","))
				}
				fmt.Fprintln(out)
			}
			fmt.Fprintf(out, "access document revision %d\n", m.GetAccessRevision())
			return nil
		},
	}
	explain.Flags().StringVarP(&ns, "namespace", "n", "", "the namespace the permission is asked at")
	explain.Flags().StringVar(&orkflow, "orkflow", "", "an Orkflow of that namespace")
	explain.Flags().StringVar(&pool, "pool", "", "a pool, alone")
	var limit int
	history := &cobra.Command{
		Use: "history", Short: "List the access document's revisions, newest first", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			var revisions []*orkstackv1.AccessRevision
			token := ""
			for len(revisions) < limit {
				resp, err := g.accessClient().ListAccessRevisions(ctx, connect.NewRequest(&orkstackv1.ListAccessRevisionsRequest{
					PageSize: int32(min(limit-len(revisions), 500)), PageToken: token}))
				if err != nil {
					return rpcError("admin access history", err)
				}
				revisions = append(revisions, resp.Msg.GetRevisions()...)
				if token = resp.Msg.GetNextPageToken(); token == "" {
					break
				}
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), &orkstackv1.ListAccessRevisionsResponse{Revisions: revisions})
			}
			for _, r := range revisions {
				who := r.GetWrittenBy()
				if who != "" && !r.GetWrittenByVerified() {
					who += " (asserted)"
				}
				at := ""
				if r.GetWrittenAt() != nil {
					at = r.GetWrittenAt().AsTime().Format("2006-01-02T15:04:05Z07:00")
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%d\t%s\t%s\t%s\t%s\n", r.GetRevision(), r.GetSha256(), at, who, r.GetChange())
			}
			return nil
		},
	}
	history.Flags().IntVar(&limit, "limit", 50, "the most revisions to list")
	diff := &cobra.Command{
		Use: "diff FROM TO", Short: "Show what changed between two revisions", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			var docs [2][]string
			for i, arg := range args {
				rev, err := strconv.ParseInt(arg, 10, 64)
				if err != nil || rev < 1 {
					return fmt.Errorf("admin access diff: %q is not a revision number", arg)
				}
				resp, err := g.accessClient().GetAccessRevision(ctx, connect.NewRequest(&orkstackv1.GetAccessRevisionRequest{Revision: rev}))
				if err != nil {
					return rpcError("admin access diff", err)
				}
				docs[i] = accessLines(resp.Msg.GetDocument())
			}
			out := cmd.OutOrStdout()
			for _, l := range docs[0] {
				if !slices.Contains(docs[1], l) {
					fmt.Fprintf(out, "- %s\n", l)
				}
			}
			for _, l := range docs[1] {
				if !slices.Contains(docs[0], l) {
					fmt.Fprintf(out, "+ %s\n", l)
				}
			}
			return nil
		},
	}
	acknowledge := &cobra.Command{
		Use:   "acknowledge REVISION SHA256",
		Short: "Acknowledge a revision written around the API, once you have read it, clearing its banner",
		Long: "Acknowledge a revision written around the API, named by its number and digest as `admin access history` lists them, " +
			"or the head as it is now. The acknowledgement is recorded once, naming who made it and when; it clears the banner " +
			"on every engine and changes no access.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			rev, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("admin access acknowledge: %q is not a revision number", args[0])
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			if _, err := g.accessClient().AcknowledgeAccessRevision(ctx, connect.NewRequest(&orkstackv1.AcknowledgeAccessRevisionRequest{
				Revision: rev, Sha256: args[1]})); err != nil {
				return rpcError("admin access acknowledge", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), &orkstackv1.AcknowledgeAccessRevisionResponse{})
			}
			fmt.Fprintf(cmd.OutOrStdout(), "acknowledged revision %d\n", rev)
			return nil
		},
	}
	cmd.AddCommand(explain, history, diff, acknowledge)
	return cmd
}

// accessLines renders a document as one line per role, binding, service
// account and disabled principal: what a diff compares.
func accessLines(doc *orkstackv1.AccessDocument) []string {
	var out []string
	line := func(kind string, m proto.Message) {
		b, _ := protojson.MarshalOptions{}.Marshal(m)
		out = append(out, kind+" "+string(b))
	}
	for _, r := range doc.GetRoles() {
		line("role", r)
	}
	for _, b := range doc.GetBindings() {
		line("binding", b)
	}
	for _, sa := range doc.GetServiceAccounts() {
		line("service-account", sa)
	}
	for _, id := range doc.GetDisabled() {
		out = append(out, "disabled "+id)
	}
	return out
}
