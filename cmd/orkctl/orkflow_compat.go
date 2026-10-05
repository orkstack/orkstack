package main

import (
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// The interface compatibility surface (interface-compatibility-gate §6):
// `compat` answers "what will this change break?" between two stored
// versions, and `callers` answers "who starts or composes this, pinned to
// what?". Both are reads and exit 0 whatever they find; the gate belongs on
// the act, `orkflow promote`.

func newOrkflowCompatCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "compat <name> [<from>] <to>",
		Short: "Show what a version changes in the declared interface (inputs and outputs)",
		Long: "compat classifies how the declared interface of one stored version — its\n" +
			"inputs: and its outputs: — changes in another. Every input and output gets a\n" +
			"finding marked compatible, BREAKING or unknown. Tasks, images, with:, pools\n" +
			"and output templates are implementation and never findings; `orkflow diff`\n" +
			"shows them.\n\n" +
			"With two arguments the comparison is from the current version. An empty\n" +
			"version (\"\") on either side also means the current one.\n\n" +
			"An orkflow with no outputs: declares no return contract, so its output side\n" +
			"is unknown, never compatible. compat is a read: it exits 0 whatever the\n" +
			"verdict; --json's verdict is what a CI job checks.",
		Args: cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			name, from, to := args[0], "", args[1]
			if len(args) == 3 {
				from, to = args[1], args[2]
			}
			resp, err := g.orkflowClient().CompareInterfaces(ctx, connect.NewRequest(
				&orkstackv1.CompareInterfacesRequest{
					Name:        name,
					FromVersion: from,
					ToVersion:   to,
					Namespace:   g.namespace,
				},
			))
			if err != nil {
				return rpcError("compare orkflow interfaces", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderCompat(cmd.OutOrStdout(), name, resp.Msg)
		},
	}
}

func newOrkflowCallersCommand(g *globalFlags) *cobra.Command {
	var allVersions bool
	cmd := &cobra.Command{
		Use:   "callers <name>",
		Short: "List what in this namespace starts or composes an orkflow, and the version it pins",
		Long: "callers lists every composing orkflow task, trigger and schedule in the\n" +
			"namespace that references the orkflow, with the version each one pins, or\n" +
			"`current` when it follows the current version — which is what a promotion\n" +
			"moves. A composing orkflow always pins a version.\n\n" +
			"It is a scan made per request (there is no caller index): by default the\n" +
			"version each orkflow runs now; --all-versions reads every stored version.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.orkflowClient().ListOrkflowCallers(ctx, connect.NewRequest(
				&orkstackv1.ListOrkflowCallersRequest{
					Name:        args[0],
					Namespace:   g.namespace,
					AllVersions: allVersions,
				},
			))
			if err != nil {
				return rpcError("list orkflow callers", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderCallers(cmd.OutOrStdout(), args[0], resolvedNamespaceName(g.namespace), resp.Msg)
		},
	}
	cmd.Flags().BoolVar(&allVersions, "all-versions", false,
		"scan every stored version of every orkflow, not only the version each one runs now")
	return cmd
}

// resolvedNamespaceName is what an empty --namespace means on the wire.
func resolvedNamespaceName(namespace string) string {
	if namespace == "" {
		return "default"
	}
	return namespace
}

// verdictWord is research §7's rendering: BREAKING shouts, the others do not.
func verdictWord(verdict orkstackv1.InterfaceVerdict) string {
	switch verdict {
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE:
		return "compatible"
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING:
		return "BREAKING"
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN:
		return "unknown"
	default:
		return "-"
	}
}

// changeWord is the short enum name the CHANGE column prints.
func changeWord(change orkstackv1.InterfaceChange) string {
	return strings.TrimPrefix(change.String(), "INTERFACE_CHANGE_")
}

func renderCompat(out io.Writer, name string, resp *orkstackv1.CompareInterfacesResponse) error {
	fmt.Fprintf(out, "Interface  %s  %s → %s\n\n", name, resp.GetFromVersion(), resp.GetToVersion())
	table := newTable(out)
	fmt.Fprintln(table, "FIELD\tCHANGE\tFROM\tTO\tVERDICT")
	for _, f := range resp.GetFindings() {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", f.GetField(), changeWord(f.GetChange()),
			orDash(f.GetFrom()), orDash(f.GetTo()), verdictWord(f.GetVerdict()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	var notCompatible []*orkstackv1.InterfaceFinding
	var firstUnknown *orkstackv1.InterfaceFinding
	for _, f := range resp.GetFindings() {
		if f.GetVerdict() == orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE {
			continue
		}
		notCompatible = append(notCompatible, f)
		if firstUnknown == nil && f.GetVerdict() == orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN {
			firstUnknown = f
		}
	}
	if len(notCompatible) > 0 {
		fmt.Fprintln(out)
		for _, f := range notCompatible {
			fmt.Fprintf(out, "  %s: %s\n", f.GetField(), f.GetMessage())
		}
	}
	fmt.Fprintln(out)
	switch resp.GetVerdict() {
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING:
		fmt.Fprintf(out, "verdict: BREAKING (%s)\n", dsl.CountNoun(
			len(dsl.BreakingFindings(resp.GetFindings())), "breaking change", "breaking changes"))
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN:
		message := ""
		if firstUnknown != nil {
			message = firstUnknown.GetMessage()
		}
		fmt.Fprintf(out, "verdict: unknown — %s\n", message)
	default:
		fmt.Fprintln(out, "verdict: compatible")
	}
	return nil
}

// callerKindWord is the KIND column.
func callerKindWord(kind orkstackv1.OrkflowCallerKind) string {
	return strings.ToLower(strings.TrimPrefix(kind.String(), "ORKFLOW_CALLER_KIND_"))
}

// callerPins is the PINS column: a version, `current` when the caller follows
// the current pointer, or `unavailable` when it could not be read — never a
// guess that an unreadable schedule follows current.
func callerPins(c *orkstackv1.OrkflowCaller) string {
	switch {
	case c.GetUnavailableReason() != "":
		return "unavailable"
	case c.GetPinnedVersion() == "":
		return "current"
	default:
		return c.GetPinnedVersion()
	}
}

func joinedOrDash(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ", ")
}

func renderCallers(out io.Writer, name, namespace string, resp *orkstackv1.ListOrkflowCallersResponse) error {
	callers := resp.GetCallers()
	if resp.GetStoredVersions() == 0 {
		fmt.Fprintf(out, "%s has no stored version in namespace %s\n", name, namespace)
	} else {
		fmt.Fprintf(out, "Callers of %s in namespace %s (current version %s)\n",
			name, namespace, resp.GetCurrentVersion())
	}
	fmt.Fprintln(out)
	if len(callers) == 0 {
		fmt.Fprintf(out, "Nothing in namespace %s references %s: no orkflow composes it and no "+
			"trigger or schedule starts it.\n", namespace, name)
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "KIND\tCALLER\tVERSION\tTASK\tPINS\tREADS\tSUPPLIES")
	var movedTriggers, movedSchedules, composing, pinnedTriggers, pinnedSchedules int
	var unavailable []*orkstackv1.OrkflowCaller
	for _, c := range callers {
		label := c.GetName()
		if label == "" {
			label = c.GetId()
		}
		version := orDash(c.GetCallerVersion())
		if c.GetCallerVersion() != "" && resp.GetAllVersions() && c.GetCallerVersionIsCurrent() {
			version += "*"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", callerKindWord(c.GetKind()), label, version,
			orDash(c.GetTaskId()), callerPins(c), joinedOrDash(c.GetOutputsRead()),
			joinedOrDash(c.GetInputsSupplied()))
		switch c.GetKind() {
		case orkstackv1.OrkflowCallerKind_ORKFLOW_CALLER_KIND_ORKFLOW:
			composing++
		case orkstackv1.OrkflowCallerKind_ORKFLOW_CALLER_KIND_TRIGGER:
			if c.GetPinnedVersion() == "" {
				movedTriggers++
			} else {
				pinnedTriggers++
			}
		case orkstackv1.OrkflowCallerKind_ORKFLOW_CALLER_KIND_SCHEDULE:
			switch {
			case c.GetUnavailableReason() != "":
				unavailable = append(unavailable, c)
			case c.GetPinnedVersion() == "":
				movedSchedules++
			default:
				pinnedSchedules++
			}
		}
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	if resp.GetAllVersions() {
		fmt.Fprintf(out, "Scanned every stored version of every orkflow: %d definitions.\n",
			resp.GetScannedVersions())
	} else {
		fmt.Fprintf(out, "Scanned the current version of %d orkflows (--all-versions scans every "+
			"stored version).\n", resp.GetScannedVersions())
	}
	fmt.Fprintf(out, "Moved by a promotion (they follow the current version): triggers %d, schedules %d.\n",
		movedTriggers, movedSchedules)
	fmt.Fprintf(out, "Not moved by a promotion (they pin a version): composing orkflows %d, triggers %d, "+
		"schedules %d.\n", composing, pinnedTriggers, pinnedSchedules)
	for _, c := range unavailable {
		fmt.Fprintf(out, "schedule %s: version unavailable (%s)\n", c.GetName(), c.GetUnavailableReason())
	}
	return nil
}
