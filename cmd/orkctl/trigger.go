package main

import (
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
		newTriggerDeleteCommand(g),
	)
	return cmd
}

func newTriggerCreateCommand(g *globalFlags) *cobra.Command {
	var (
		orkflow     string
		version     string
		description string
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
			"delete then create.",
		Example: "  orkctl trigger create --orkflow hello --description 'nightly kickoff'",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Refuse locally rather than spending a round trip: a trigger is
			// bound to exactly one orkflow, so there is no request to make.
			if strings.TrimSpace(orkflow) == "" {
				return fmt.Errorf("--orkflow is required: a trigger starts exactly one orkflow")
			}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			response, err := g.triggerClient().CreateTrigger(ctx, connect.NewRequest(&orkstackv1.CreateTriggerRequest{
				Namespace:      g.namespace,
				OrkflowName:    orkflow,
				OrkflowVersion: version,
				Description:    description,
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
	return cmd
}

func newTriggerListCommand(g *globalFlags) *cobra.Command {
	var orkflow string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the triggers of this namespace",
		Long: "list shows the triggers of the namespace this command acts in.\n" +
			"--orkflow narrows the result to one orkflow; omitting it lists every\n" +
			"trigger in the namespace.\n\n" +
			"A listing never contains a bearer token: the plaintext exists only in\n" +
			"the output of `trigger create`.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			response, err := g.triggerClient().ListTriggers(ctx, connect.NewRequest(&orkstackv1.ListTriggersRequest{
				Namespace:   g.namespace,
				OrkflowName: orkflow,
			}))
			if err != nil {
				return rpcError("list triggers", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			return renderTriggerList(cmd.OutOrStdout(), response.Msg.GetTriggers())
		},
	}
	cmd.Flags().StringVar(&orkflow, "orkflow", "", "only triggers of this orkflow")
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

func renderTriggerList(out io.Writer, triggers []*orkstackv1.Trigger) error {
	if len(triggers) == 0 {
		fmt.Fprintln(out, "No triggers.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "ID\tNAMESPACE\tORKFLOW\tVERSION\tDESCRIPTION\tCREATED")
	for _, trigger := range triggers {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n",
			trigger.GetId(),
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
