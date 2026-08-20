package main

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newNamespaceCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "namespace",
		Aliases: []string{"ns"},
		Short:   "Manage namespaces",
		Long: "A namespace scopes orkflow names, runs and pool defaults. Two namespaces\n" +
			"may each hold an orkflow called \"deploy\" and never see each other's.",
	}
	cmd.AddCommand(
		newNamespaceCreateCommand(g),
		newNamespaceListCommand(g),
		newNamespaceGetCommand(g),
		newNamespaceUpdateCommand(g),
		newNamespaceDeleteCommand(g),
		newNamespaceUseCommand(g),
	)
	return cmd
}

func newNamespaceCreateCommand(g *globalFlags) *cobra.Command {
	var (
		description  string
		defaultPool  string
		allowedPools []string
		labels       []string
	)
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Register a namespace",
		Long: "create registers a namespace. It is idempotent for an identical spec, so a\n" +
			"provisioning script can be re-run; a second create that differs is refused\n" +
			"and says which field differs.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			parsed, err := parseLabels(labels)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().CreateNamespace(ctx, connect.NewRequest(
				&orkstackv1.CreateNamespaceRequest{Namespace: &orkstackv1.Namespace{
					Name:         args[0],
					Description:  description,
					Labels:       parsed,
					DefaultPool:  defaultPool,
					AllowedPools: allowedPools,
				}},
			))
			if err != nil {
				return rpcError("create namespace", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			verb := "Created"
			if !resp.Msg.GetCreated() {
				verb = "Unchanged"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s namespace %s\n",
				verb, resp.Msg.GetNamespace().GetName())
			return renderNamespace(cmd.OutOrStdout(), resp.Msg.GetNamespace())
		},
	}
	namespaceSpecFlags(cmd, &description, &defaultPool, &allowedPools, &labels)
	return cmd
}

func newNamespaceListCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List namespaces",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().ListNamespaces(ctx, connect.NewRequest(
				&orkstackv1.ListNamespacesRequest{},
			))
			if err != nil {
				return rpcError("list namespaces", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderNamespaceList(cmd.OutOrStdout(), resp.Msg.GetNamespaces())
		},
	}
}

func newNamespaceGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get [name]",
		Short: "Show one namespace",
		Long: "get shows one namespace. With no argument it shows the one this command\n" +
			"would act in — the --namespace flag, ORK_NAMESPACE, or the saved default.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := g.namespace
			if len(args) == 1 {
				name = args[0]
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().GetNamespace(ctx, connect.NewRequest(
				&orkstackv1.GetNamespaceRequest{Name: name},
			))
			if err != nil {
				return rpcError("get namespace", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderNamespace(cmd.OutOrStdout(), resp.Msg.GetNamespace())
		},
	}
}

func newNamespaceUpdateCommand(g *globalFlags) *cobra.Command {
	var (
		description  string
		defaultPool  string
		allowedPools []string
		labels       []string
	)
	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Change a namespace",
		Long: "update reads the namespace, applies the flags given, and writes it back\n" +
			"compare-and-swapping on the version it read. A flag left out keeps its\n" +
			"current value; if somebody else changed the namespace in between, the\n" +
			"write is refused rather than silently overwriting theirs.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			parsed, err := parseLabels(labels)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			client := g.namespaceClient()
			// Read first: an update sends the whole document back, so the
			// fields the user did not mention have to come from somewhere, and
			// the only honest source is the current document. The etag comes
			// with it, which is what makes the write a compare-and-swap.
			current, err := client.GetNamespace(ctx, connect.NewRequest(
				&orkstackv1.GetNamespaceRequest{Name: args[0]},
			))
			if err != nil {
				return rpcError("update namespace", err)
			}
			edited := current.Msg.GetNamespace()
			if cmd.Flags().Changed("description") {
				edited.Description = description
			}
			if cmd.Flags().Changed("default-pool") {
				edited.DefaultPool = defaultPool
			}
			if cmd.Flags().Changed("allow-pool") {
				edited.AllowedPools = allowedPools
			}
			if cmd.Flags().Changed("label") {
				edited.Labels = parsed
			}

			resp, err := client.UpdateNamespace(ctx, connect.NewRequest(
				&orkstackv1.UpdateNamespaceRequest{Namespace: edited},
			))
			if err != nil {
				return rpcError("update namespace", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Updated namespace %s\n",
				resp.Msg.GetNamespace().GetName())
			return renderNamespace(cmd.OutOrStdout(), resp.Msg.GetNamespace())
		},
	}
	namespaceSpecFlags(cmd, &description, &defaultPool, &allowedPools, &labels)
	return cmd
}

func newNamespaceDeleteCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Remove an empty namespace",
		Long: "delete removes a namespace that holds no orkflows. One that still holds any\n" +
			"is refused, and the refusal names them: removing the namespace would not\n" +
			"remove the definitions, and they would survive where nothing lists them.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().DeleteNamespace(ctx, connect.NewRequest(
				&orkstackv1.DeleteNamespaceRequest{Name: args[0]},
			))
			if err != nil {
				return rpcError("delete namespace", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted namespace %s\n", args[0])
			return nil
		},
	}
}

func newNamespaceUseCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Remember a namespace as the default for later commands",
		Long: "use saves a namespace in orkctl's config file so it does not have to be\n" +
			"typed on every command. --namespace and ORK_NAMESPACE still win over it.\n\n" +
			"The namespace is read from the engine first: saving a name that does not\n" +
			"exist would make every later command fail for a reason recorded in a file\n" +
			"the user has stopped thinking about.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().GetNamespace(ctx, connect.NewRequest(
				&orkstackv1.GetNamespaceRequest{Name: args[0]},
			))
			if err != nil {
				return rpcError("use namespace", err)
			}
			cancel()

			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			cfg.Namespace = resp.Msg.GetNamespace().GetName()
			path, err := saveConfig(cfg)
			if err != nil {
				return err
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Now using namespace %s (saved in %s)\n",
				cfg.Namespace, path)
			return nil
		},
	}
}

// namespaceSpecFlags registers the flags that describe a namespace. create and
// update share them so the two commands cannot drift apart.
func namespaceSpecFlags(
	cmd *cobra.Command,
	description, defaultPool *string,
	allowedPools, labels *[]string,
) {
	cmd.Flags().StringVar(description, "description", "", "human description")
	cmd.Flags().StringVar(defaultPool, "default-pool", "",
		"pool a task of this namespace runs on when it names none (default \"main\")")
	cmd.Flags().StringArrayVar(allowedPools, "allow-pool", nil,
		"pool this namespace may dispatch onto (repeatable; empty = unrestricted)")
	cmd.Flags().StringArrayVar(labels, "label", nil, "label as key=value (repeatable)")
}

// parseLabels turns repeated key=value flags into a map.
func parseLabels(pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	labels := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		key, value, found := strings.Cut(pair, "=")
		if !found || key == "" {
			return nil, fmt.Errorf("invalid --label %q: want key=value", pair)
		}
		labels[key] = value
	}
	return labels, nil
}

func renderNamespaceList(out io.Writer, namespaces []*orkstackv1.Namespace) error {
	if len(namespaces) == 0 {
		fmt.Fprintln(out, "No namespaces.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tDEFAULT POOL\tALLOWED POOLS\tCREATED\tDESCRIPTION")
	for _, ns := range namespaces {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n",
			ns.GetName(), defaultPoolLabel(ns), joinOrDash(ns.GetAllowedPools()),
			formatTime(ns.GetCreatedAt()), orDash(ns.GetDescription()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func renderNamespace(out io.Writer, ns *orkstackv1.Namespace) error {
	table := newTable(out)
	fmt.Fprintf(table, "Name:\t%s\n", ns.GetName())
	fmt.Fprintf(table, "Description:\t%s\n", orDash(ns.GetDescription()))
	fmt.Fprintf(table, "Default pool:\t%s\n", defaultPoolLabel(ns))
	fmt.Fprintf(table, "Allowed pools:\t%s\n", allowedPoolsLabel(ns))
	fmt.Fprintf(table, "Labels:\t%s\n", labelsLabel(ns))
	fmt.Fprintf(table, "Created:\t%s\n", formatTime(ns.GetCreatedAt()))
	fmt.Fprintf(table, "Updated:\t%s\n", formatTime(ns.GetUpdatedAt()))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// defaultPoolLabel renders an unset default pool as the pool it means, so a
// reader is never shown a dash where a task actually goes somewhere.
func defaultPoolLabel(ns *orkstackv1.Namespace) string {
	if pool := ns.GetDefaultPool(); pool != "" {
		return pool
	}
	return "main"
}

// allowedPoolsLabel spells out what an empty allow-list means: not "none", but
// "no restriction". A dash here would read as the opposite of the truth.
func allowedPoolsLabel(ns *orkstackv1.Namespace) string {
	if pools := ns.GetAllowedPools(); len(pools) > 0 {
		return strings.Join(pools, ",")
	}
	return "(unrestricted)"
}

func labelsLabel(ns *orkstackv1.Namespace) string {
	labels := ns.GetLabels()
	if len(labels) == 0 {
		return dash
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, key+"="+labels[key])
	}
	return strings.Join(pairs, ",")
}
