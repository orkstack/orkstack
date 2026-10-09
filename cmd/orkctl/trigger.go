package main

import (
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// triggerInvokePath is the plain-HTTP route an external caller posts to, as
// docs/ORKFLOW_TRIGGERS.md locks it. The CLI only ever renders it into the
// example command, so it holds the literal rather than reaching into
// internal/api for a constant it does not otherwise need.
const triggerInvokePath = "/v1/triggers/"

// currentVersionLabel is how an unpinned trigger's version reads. An empty pin
// means the orkflow's current pointer, resolved at invoke time, so a table cell
// that said "-" would suggest the trigger runs nothing.
const currentVersionLabel = "current"

func newTriggerCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trigger",
		Short: "Create and inspect inbound run starters",
		Long: "A trigger lets an external caller start a run over plain HTTP:\n" +
			"POST /v1/triggers/<id> with the trigger's bearer token.\n\n" +
			"It is the mirror image of a webhook. A webhook is outbound and\n" +
			"deployment-wide unless scoped; a trigger is inbound and bound to one\n" +
			"orkflow in one namespace. The orkflow a trigger starts comes from the\n" +
			"stored trigger and never from the request, so a token cannot be\n" +
			"redirected.",
	}
	cmd.AddCommand(
		newTriggerCreateCommand(g),
		newTriggerListCommand(g),
		newTriggerUpdateCommand(g),
		newTriggerDeleteCommand(g),
	)
	return cmd
}

func newTriggerCreateCommand(g *globalFlags) *cobra.Command {
	var (
		orkflow     string
		version     string
		description string
		name        string
	)
	cmd := &cobra.Command{
		Use:   "create --orkflow <name>",
		Short: "Create a trigger that starts one orkflow",
		Long: "create binds a new trigger to one orkflow in this namespace and prints\n" +
			"its bearer token once.\n\n" +
			"--version pins the trigger to that version forever; omitting it follows\n" +
			"the orkflow's current pointer, resolved at each invoke.\n\n" +
			"The token is shown here and never again: a listing cannot disclose it,\n" +
			"because only a salted digest of it is stored. Rotating a token is\n" +
			"delete then create.\n\n" +
			"--name gives the trigger a handle, unique in the namespace, by which an\n" +
			"automation document addresses it (docs/AUTOMATION.md).",
		Example: "  orkctl trigger create --orkflow hello --description 'nightly kickoff'\n" +
			"  orkctl trigger create --orkflow deploy --name github-deploy",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Refuse locally rather than spending a round trip: a trigger is
			// bound to exactly one orkflow, so there is no request to make.
			if strings.TrimSpace(orkflow) == "" {
				return fmt.Errorf("--orkflow is required: a trigger starts exactly one orkflow")
			}
			if err := triggers.ValidateName(name); err != nil {
				return err
			}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			response, err := g.triggerClient().CreateTrigger(ctx, connect.NewRequest(&orkstackv1.CreateTriggerRequest{
				Namespace:      g.namespace,
				OrkflowName:    orkflow,
				OrkflowVersion: version,
				Description:    description,
				Name:           name,
			}))
			if err != nil {
				return rpcError("create trigger", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			return renderTriggerCreated(cmd.OutOrStdout(), g.baseURL(),
				response.Msg.GetTrigger(), response.Msg.GetSecret())
		},
	}
	cmd.Flags().StringVar(&orkflow, "orkflow", "", "orkflow this trigger starts (required)")
	cmd.Flags().StringVar(&version, "version", "",
		"orkflow version to pin (default: the current one at invoke time)")
	cmd.Flags().StringVar(&description, "description", "", "operator label for this trigger")
	cmd.Flags().StringVar(&name, "name", "", "handle an automation document addresses this trigger by (a slug, unique in the namespace)")
	return cmd
}

func newTriggerUpdateCommand(g *globalFlags) *cobra.Command {
	var (
		name          string
		version       string
		followCurrent bool
		description   string
	)
	cmd := &cobra.Command{
		Use:   "update <id>",
		Short: "Rename, re-pin or relabel a trigger in place, keeping its token",
		Long: "update changes a trigger's name, pinned version and description in place.\n" +
			"Its id, URL, orkflow and bearer token never change: an update never\n" +
			"re-issues a token, so the caller holding it keeps working.\n\n" +
			"Only the flags given change anything: --name \"\" makes the trigger\n" +
			"unnamed, --follow-current clears the pin, --description \"\" clears the\n" +
			"label. A trigger's orkflow cannot change: that is delete then create.\n\n" +
			"Naming an unnamed trigger is how a hand-made one is brought under an\n" +
			"automation document (orkctl export) without re-issuing its token.",
		Example: "  orkctl trigger update trg-4a1c0e2b7d3f8156 -n payments --name github-deploy\n" +
			"  orkctl trigger update trg-4a1c0e2b7d3f8156 --version 1.4.0",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			if cmd.Flags().Changed("version") && followCurrent {
				return fmt.Errorf("--version and --follow-current are mutually exclusive")
			}
			if cmd.Flags().Changed("name") {
				if err := triggers.ValidateName(name); err != nil {
					return err
				}
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			// Read-modify-write: the update replaces every mutable field, so
			// the ones no flag named are sent back as they are.
			listed, err := g.triggerClient().ListTriggers(ctx, connect.NewRequest(&orkstackv1.ListTriggersRequest{Namespace: g.namespace}))
			if err != nil {
				return rpcError("update trigger", err)
			}
			var current *orkstackv1.Trigger
			for _, trigger := range listed.Msg.GetTriggers() {
				if trigger.GetId() == id {
					current = trigger
				}
			}
			if current == nil {
				namespace := g.namespace
				if namespace == "" {
					namespace = "default"
				}
				return fmt.Errorf("update trigger: not_found: trigger %q not found in namespace %q", id, namespace)
			}
			req := &orkstackv1.UpdateTriggerRequest{
				Id: id, Namespace: g.namespace,
				Name: current.GetName(), OrkflowVersion: current.GetOrkflowVersion(), Description: current.GetDescription(),
			}
			if cmd.Flags().Changed("name") {
				req.Name = name
			}
			if cmd.Flags().Changed("version") {
				req.OrkflowVersion = version
			}
			if followCurrent {
				req.OrkflowVersion = ""
			}
			if cmd.Flags().Changed("description") {
				req.Description = description
			}
			response, err := g.triggerClient().UpdateTrigger(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("update trigger", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			return renderTriggerUpdated(cmd.OutOrStdout(), response.Msg.GetTrigger())
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "new handle (\"\" makes the trigger unnamed)")
	cmd.Flags().StringVar(&version, "version", "", "orkflow version to pin")
	cmd.Flags().BoolVar(&followCurrent, "follow-current", false, "clear the pin: follow the orkflow's current pointer at each invoke")
	cmd.Flags().StringVar(&description, "description", "", "new operator label (\"\" clears it)")
	return cmd
}

// renderTriggerUpdated prints an updated trigger: the creation table without
// its SECRET row, because an update never discloses or re-issues a token.
func renderTriggerUpdated(out io.Writer, trigger *orkstackv1.Trigger) error {
	fmt.Fprintf(out, "Updated trigger %s\n", trigger.GetId())
	table := newTable(out)
	fmt.Fprintf(table, "NAME\t%s\n", orDash(trigger.GetName()))
	fmt.Fprintf(table, "NAMESPACE\t%s\n", orDash(trigger.GetNamespace()))
	fmt.Fprintf(table, "ORKFLOW\t%s\n", orDash(trigger.GetOrkflowName()))
	fmt.Fprintf(table, "VERSION\t%s\n", triggerVersionLabel(trigger.GetOrkflowVersion()))
	fmt.Fprintf(table, "DESCRIPTION\t%s\n", orDash(trigger.GetDescription()))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out, "\nThe token is unchanged.")
	return nil
}

func newTriggerListCommand(g *globalFlags) *cobra.Command {
	var filterTexts *[]string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the triggers of this namespace",
		Long: "list shows the triggers of the namespace this command acts in, narrowed by\n" +
			"--filter: --filter orkflow_name=NAME lists one orkflow's triggers.\n\n" +
			"A listing never contains a bearer token: the plaintext exists only in\n" +
			"the output of `trigger create`.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			response, err := g.triggerClient().ListTriggers(ctx, connect.NewRequest(&orkstackv1.ListTriggersRequest{
				Namespace: g.namespace,
				Filters:   filters,
			}))
			if err != nil {
				return rpcError("list triggers", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			if len(filters) > 0 && len(response.Msg.GetTriggers()) == 0 {
				fmt.Fprint(cmd.OutOrStdout(), noMatchSentence("triggers", filters))
				return nil
			}
			return renderTriggerList(cmd.OutOrStdout(), response.Msg.GetTriggers())
		},
	}
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListTriggersRequest{})
	return cmd
}

func newTriggerDeleteCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <id>",
		Short: "Remove a trigger",
		Long: "delete removes a trigger of the namespace this command acts in. A\n" +
			"trigger of another namespace is not found here.\n\n" +
			"Its token authenticates nothing afterwards: a later invoke of that id\n" +
			"is 404 on every replica.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			response, err := g.triggerClient().DeleteTrigger(ctx, connect.NewRequest(
				&orkstackv1.DeleteTriggerRequest{Id: args[0], Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("delete trigger", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted trigger %s\n", args[0])
			return nil
		},
	}
}

// renderTriggerCreated prints the new trigger, its one-time token, and a
// command that actually invokes it. The token cannot be recovered later, so
// this output is the only place a runnable example can carry the real one.
func renderTriggerCreated(out io.Writer, baseURL string, trigger *orkstackv1.Trigger, secret string) error {
	fmt.Fprintf(out, "Created trigger %s\n", trigger.GetId())
	table := newTable(out)
	if name := trigger.GetName(); name != "" {
		fmt.Fprintf(table, "NAME\t%s\n", name)
	}
	fmt.Fprintf(table, "NAMESPACE\t%s\n", orDash(trigger.GetNamespace()))
	fmt.Fprintf(table, "ORKFLOW\t%s\n", orDash(trigger.GetOrkflowName()))
	fmt.Fprintf(table, "VERSION\t%s\n", triggerVersionLabel(trigger.GetOrkflowVersion()))
	fmt.Fprintf(table, "DESCRIPTION\t%s\n", orDash(trigger.GetDescription()))
	fmt.Fprintf(table, "SECRET\t%s\n", orDash(secret))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintf(out, "\nThe secret is shown once. Copy it now: it will not be shown again,\n"+
		"because only a salted digest of it is stored.\n\n"+
		"This trigger runs %s. Invoke it with:\n\n%s\n",
		triggerScope(trigger), triggerCurlExample(baseURL, trigger.GetId(), secret))
	return nil
}

func renderTriggerList(out io.Writer, list []*orkstackv1.Trigger) error {
	if len(list) == 0 {
		fmt.Fprintln(out, "No triggers.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "ID\tNAME\tNAMESPACE\tORKFLOW\tVERSION\tDESCRIPTION\tCREATED")
	for _, trigger := range list {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			trigger.GetId(),
			orDash(trigger.GetName()),
			orDash(trigger.GetNamespace()),
			orDash(trigger.GetOrkflowName()),
			triggerVersionLabel(trigger.GetOrkflowVersion()),
			orDash(trigger.GetDescription()),
			formatTime(trigger.GetCreatedAt()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// triggerCurlExample renders a runnable invocation against the same engine
// address this command used, so the example works where the command worked.
// The optional origin-reference header follows as a comment line, so the
// command stays runnable as printed.
func triggerCurlExample(baseURL, id, secret string) string {
	return fmt.Sprintf("  curl -X POST %s \\\n"+
		"    -H %s \\\n"+
		"    -H 'Content-Type: application/json' \\\n"+
		"    -d '{}'\n"+
		"  # optional: -H 'Ork-Origin-Reference: <your CI job URL>' records a correlation reference on the run",
		shellQuote(strings.TrimRight(baseURL, "/")+triggerInvokePath+id),
		shellQuote("Authorization: Bearer "+secret))
}

// triggerScope names what a trigger starts and where, the way the UI's scope
// statement does. A reader must not have to assume the pin or the namespace
// from context: a trigger reaches exactly one orkflow in exactly one namespace.
func triggerScope(trigger *orkstackv1.Trigger) string {
	scope := orDash(trigger.GetOrkflowName()) + "@" + triggerVersionLabel(trigger.GetOrkflowVersion())
	if namespace := trigger.GetNamespace(); namespace != "" {
		scope += " in " + namespace
	}
	return scope
}

// triggerVersionLabel renders an unpinned trigger's version. Empty means the
// current pointer at invoke time, not "no version".
func triggerVersionLabel(version string) string {
	if version == "" {
		return currentVersionLabel
	}
	return version
}

// shellQuote wraps a value in single quotes so a copied command is one
// argument whatever the value contains. A single quote inside ends the quoted
// span, escapes itself, and reopens it, which is the only escape the shell
// offers inside single quotes.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
