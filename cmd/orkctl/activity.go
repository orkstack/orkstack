package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// newActivityCommand inspects the deployment's activity catalog. An activity
// is an OCI image and which images exist is engine configuration (§4.2), so
// this asks the engine, never an Orker.
func newActivityCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "activity",
		Short: "Inspect the activities this deployment can run",
	}
	cmd.AddCommand(newActivityListCommand(g))
	cmd.AddCommand(newActivityInspectCommand(g))
	cmd.AddCommand(newActivityTestCommand(g))
	cmd.AddCommand(newActivityBuildCommand(g))
	cmd.AddCommand(newActivityInterfaceCommand())
	return cmd
}

func newActivityListCommand(g *globalFlags) *cobra.Command {
	var filterTexts *[]string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the deployment's activity catalog",
		Long: "List the activities this deployment can run.\n\n" +
			"An activity is an OCI image. The catalog maps a short name — \"shell\" — to a\n" +
			"registry reference and the index digest it resolved to, so what a name will\n" +
			"actually run is visible before a run pins it.\n\n" +
			"This reads engine configuration and never asks an Orker: an Orker executes\n" +
			"images, it does not decide which ones exist. Use `orkctl pool list` to ask\n" +
			"whether a pool has Orkers to run them.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.activityClient().ListActivities(ctx, connect.NewRequest(
				&orkstackv1.ListActivitiesRequest{Filters: filters},
			))
			if err != nil {
				return rpcError("list activities", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			if len(filters) > 0 && len(resp.Msg.GetActivities()) == 0 {
				fmt.Fprint(cmd.OutOrStdout(), noMatchSentence("activities", filters))
				return nil
			}
			return renderActivityList(cmd.OutOrStdout(), resp.Msg.GetActivities())
		},
	}
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListActivitiesRequest{})
	return cmd
}

func renderActivityList(out io.Writer, activities []*orkstackv1.ActivityInfo) error {
	if len(activities) == 0 {
		fmt.Fprintln(out, "No activities in this deployment's catalog.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tREFERENCE\tDIGEST\tPLATFORMS\tINTERFACE")
	for _, activity := range activities {
		fmt.Fprint(table, safeRow(
			activity.GetName(),
			activity.GetReference(),
			shortDigest(activity.GetIndexDigest()),
			joinOrDash(activity.GetPlatforms()),
			interfaceSummary(activity)))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// shortDigest renders sha256:<64 hex> as sha256:<first 12>, which is what an
// operator compares by eye. The full value is in --json.
func shortDigest(digest string) string {
	const shown = len("sha256:") + 12
	if len(digest) <= shown {
		if digest == "" {
			return "-"
		}
		return digest
	}
	return digest[:shown] + "…"
}

// interfaceSummary is the INTERFACE column: "-" for an entry that did not
// resolve, "none" for an image that declares nothing, "malformed" for a label
// this engine could not read, and otherwise "N keys" / "N ops". The UI's
// interfaceSummary (ui/src/lib/activity.ts) says the same words.
func interfaceSummary(info *orkstackv1.ActivityInfo) string {
	switch {
	case info.GetIndexDigest() == "":
		return dash
	case info.GetInterfaceError() != "":
		return "malformed"
	case info.GetInterface() == nil:
		return "none"
	}
	var parts []string
	if n := len(info.GetInterface().GetFields()); n > 0 {
		parts = append(parts, countNoun(n, "key"))
	}
	if n := len(info.GetInterface().GetOps()); n > 0 {
		parts = append(parts, countNoun(n, "op"))
	}
	if len(parts) == 0 {
		return "0 keys"
	}
	return strings.Join(parts, ", ")
}

// countNoun is plural (stats_failures.go) for a length.
func countNoun(n int, noun string) string { return plural(int64(n), noun) }

func newActivityInspectCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <name-or-reference>",
		Short: "Resolve one activity and show the with: interface its image declares",
		Long: "Resolve one reference the way `uses:` would — a catalog name (\"file\") or a\n" +
			"registry reference (\"ghcr.io/acme/migrate:v3\") — and show the digest it pins\n" +
			"and the with: interface the image declares in its\n" +
			"org.orkstack.activity.interface label: which keys each op reads, their type,\n" +
			"whether each is required, and the keys the image writes to values.json.\n" +
			"It also shows whether the image declares an icon (org.orkstack.activity.icon)\n" +
			"and the descriptions it gives of itself, its ops and its keys\n" +
			"(org.orkstack.activity.docs); --json carries them whole.\n\n" +
			"An image without the label is an ordinary activity; its with: is checked by the\n" +
			"image itself at run time. This reads engine configuration and the registry,\n" +
			"never an Orker, and writes nothing.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.activityClient().InspectActivity(ctx, connect.NewRequest(
				&orkstackv1.InspectActivityRequest{Reference: args[0]},
			))
			if err != nil {
				return rpcError("inspect activity", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderActivityInspect(cmd.OutOrStdout(), resp.Msg.GetActivity())
		},
	}
}

// pinnedReference is registry/repository@digest for an ActivityInfo, the
// string a task's ImageRef.pinned would carry.
func pinnedReference(info *orkstackv1.ActivityInfo) string {
	if info.GetIndexDigest() == "" {
		return dash
	}
	parsed, err := dsl.ParseReference(info.GetReference())
	if err != nil {
		return dash
	}
	return parsed.Pinned(info.GetIndexDigest())
}

func renderActivityInspect(out io.Writer, info *orkstackv1.ActivityInfo) error {
	// A fixed label column rather than a tabwriter: the Interface line sits
	// below a blank line and must still align with the block above it. Every
	// value is escaped as it enters its line (safeRow).
	line := func(label, value string) { fmt.Fprintf(out, "%-12s%s\n", label, terminalSafe(value)) }
	if info.GetName() != "" {
		line("Activity", info.GetName()+" (catalog)")
	} else {
		line("Activity", info.GetReference()+" (reference)")
	}
	line("Reference", info.GetReference())
	line("Pinned", pinnedReference(info))
	line("Media type", orDash(info.GetMediaType()))
	platforms := dash
	if len(info.GetPlatforms()) > 0 {
		platforms = strings.Join(info.GetPlatforms(), ", ")
	}
	line("Platforms", platforms)
	line("Resolved", formatTime(info.GetResolvedAt()))
	if text, ok := descriptionLine(info); ok {
		line("Description", text)
	}
	line("Icon", iconHeadline(info))
	fmt.Fprintln(out)
	line("Interface", interfaceHeadline(info))
	iface := info.GetInterface()
	if iface == nil || info.GetInterfaceError() != "" {
		return nil
	}
	fmt.Fprintln(out)
	if err := renderInterfaceTable(out, iface, info.GetDocs()); err != nil {
		return err
	}
	if err := renderOpsTable(out, iface, info.GetDocs()); err != nil {
		return err
	}
	return renderValuesTable(out, iface, info.GetDocs())
}

// renderInterfaceTable is inspect's table of with: keys: one row per key, by
// op, with a DESCRIPTION column when any row has one.
func renderInterfaceTable(out io.Writer, iface *orkstackv1.ActivityInterface, docs *orkstackv1.ActivityDocs) error {
	table := newTable(out)
	hasOps := len(iface.GetOps()) > 0
	rows := interfaceRows(iface)
	described := false
	for _, row := range rows {
		described = described || rowDescription(docs, row) != ""
	}
	header := []string{"KEY", "TYPE", "REQUIRED", "DEFAULT"}
	if hasOps {
		header = append([]string{"OP"}, header...)
	}
	if described {
		header = append(header, "DESCRIPTION")
	}
	fmt.Fprintln(table, strings.Join(header, "\t"))
	for _, row := range rows {
		required := yesNo(row.field.GetRequired())
		if row.key == dash {
			required = dash
		}
		cells := []string{row.key, row.field.GetType(), required, defaultText(row.field)}
		if hasOps {
			cells = append([]string{row.op}, cells...)
		}
		if described {
			cells = append(cells, cellText(rowDescription(docs, row)))
		}
		fmt.Fprint(table, safeRow(cells...))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// renderOpsTable is inspect's OPS table: every op and its description, when
// at least one op has a description.
func renderOpsTable(out io.Writer, iface *orkstackv1.ActivityInterface, docs *orkstackv1.ActivityDocs) error {
	described := false
	for _, op := range docs.GetOps() {
		described = described || op.GetDescription() != ""
	}
	if len(iface.GetOps()) == 0 || !described {
		return nil
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "OPS")
	ops := newTable(out)
	fmt.Fprintln(ops, "OP\tDESCRIPTION")
	for _, op := range sortedMapKeys(iface.GetOps()) {
		fmt.Fprint(ops, safeRow(op, cellText(docs.GetOps()[op].GetDescription())))
	}
	if err := ops.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// renderValuesTable is inspect's VALUES table: the keys the image writes to
// values.json, with a DESCRIPTION column when any has one.
func renderValuesTable(out io.Writer, iface *orkstackv1.ActivityInterface, docs *orkstackv1.ActivityDocs) error {
	if len(iface.GetValues()) == 0 {
		return nil
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "VALUES")
	values := newTable(out)
	described := len(docs.GetValues()) > 0
	if described {
		fmt.Fprintln(values, "KEY\tTYPE\tDESCRIPTION")
	} else {
		fmt.Fprintln(values, "KEY\tTYPE")
	}
	for _, key := range sortedMapKeys(iface.GetValues()) {
		if described {
			fmt.Fprint(values, safeRow(key, iface.GetValues()[key].GetType(), cellText(docs.GetValues()[key])))
		} else {
			fmt.Fprint(values, safeRow(key, iface.GetValues()[key].GetType()))
		}
	}
	if err := values.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// descriptionLine is the Description line of `activity inspect`: the
// activity's own description, its whitespace collapsed to one line, or why
// the image's descriptions could not be read; absent when the image gives
// none or the reference did not resolve.
func descriptionLine(info *orkstackv1.ActivityInfo) (string, bool) {
	switch {
	case info.GetIndexDigest() == "":
		return "", false
	case info.GetDocsError() != "":
		return "malformed — " + info.GetDocsError(), true
	case info.GetDocs().GetDescription() != "":
		return strings.Join(strings.Fields(info.GetDocs().GetDescription()), " "), true
	}
	return "", false
}

// iconHeadline is the Icon line of `activity inspect`: "-" for a reference
// that did not resolve, "none" for an image that draws itself no icon (a
// client draws its default), "malformed" for an icon label this engine
// could not read, and otherwise how many elements the icon's drawing has.
func iconHeadline(info *orkstackv1.ActivityInfo) string {
	switch {
	case info.GetIndexDigest() == "":
		return dash
	case info.GetIconError() != "":
		return "malformed — " + info.GetIconError()
	case info.GetIcon() == nil:
		return "none — clients draw the default icon"
	}
	return "declared · " + countNoun(iconElementCount(info.GetIcon().GetElements()), "element")
}

// iconElementCount counts an icon's elements, a g's children included.
func iconElementCount(elements []*orkstackv1.IconElement) int {
	n := len(elements)
	for _, element := range elements {
		n += iconElementCount(element.GetChildren())
	}
	return n
}

// safeRow is one table row of cells, each escaped by terminalSafe before
// the tabwriter measures it: every name, key, reference, platform, type,
// default and description in a row came from a server, an image or a
// registry, and a raw line break, tab or control in one would forge a row,
// shift a column or reach the terminal. Escaping first also keeps the
// columns aligned on the escaped text.
func safeRow(cells ...string) string {
	escaped := make([]string, len(cells))
	for i, cell := range cells {
		escaped[i] = terminalSafe(cell)
	}
	return strings.Join(escaped, "\t") + "\n"
}

// terminalSafe is text as a terminal may print it: every control or format
// character — a C0 or C1 control, a bidirectional control, a zero-width
// character — and every byte that is not UTF-8 written as an escape. The
// engine already refuses or escapes most of them; this holds against any
// engine, any image and any registry.
func terminalSafe(text string) string {
	var b strings.Builder
	for _, r := range text {
		switch {
		case r > 0xffff && unicode.Is(unicode.Cf, r):
			fmt.Fprintf(&b, "\\U%08x", r)
			continue
		case unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == utf8.RuneError || r == 0x2028 || r == 0x2029:
			fmt.Fprintf(&b, "\\u%04x", r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// rowDescription is the description of one interface table row: a shared
// key's, or the key's under its op. A row of an op that reads no key of its
// own has none; the OPS table describes the op.
func rowDescription(docs *orkstackv1.ActivityDocs, row interfaceRow) string {
	switch {
	case row.key == dash:
		return ""
	case row.op == "*":
		return docs.GetFields()[row.key]
	}
	return docs.GetOps()[row.op].GetFields()[row.key]
}

// maxCellRunes bounds a description in a table cell; --json carries it whole.
const maxCellRunes = 80

// cellText is a description as one table cell: "-" when there is none, its
// whitespace runs (line breaks included) collapsed to one space, cut at
// maxCellRunes with an ellipsis. It is not yet escaped: safeRow escapes the
// cell after the cut, so a cut never falls inside an escape.
func cellText(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return dash
	}
	if runes := []rune(text); len(runes) > maxCellRunes {
		return string(runes[:maxCellRunes-1]) + "…"
	}
	return text
}

// interfaceHeadline is the Interface line of `activity inspect`.
func interfaceHeadline(info *orkstackv1.ActivityInfo) string {
	switch {
	case info.GetIndexDigest() == "":
		return dash
	case info.GetInterfaceError() != "":
		return "malformed — " + info.GetInterfaceError()
	case info.GetInterface() == nil:
		return "none — the image declares no " + dsl.InterfaceLabel + " label; with: is checked by the image at run time"
	}
	iface := info.GetInterface()
	schema := fmt.Sprintf("schema %d", iface.GetSchemaVersion())
	if len(iface.GetOps()) == 0 {
		return schema + " · " + countNoun(len(iface.GetFields()), "key")
	}
	selection := "op required"
	if iface.GetDefaultOp() != "" {
		selection = "default " + iface.GetDefaultOp()
	}
	headline := fmt.Sprintf("%s · selected by op: (%s, %s)", schema, countNoun(len(iface.GetOps()), "op"), selection)
	if n := len(iface.GetFields()); n > 0 {
		headline += fmt.Sprintf(" · %s shared by every op", countNoun(n, "key"))
	}
	return headline
}

// interfaceRow is one line of the interface table. op is "*" for a key every
// op reads; an op that reads nothing beyond the shared keys is one row whose
// key is "-", so no declared op is invisible.
type interfaceRow struct {
	op, key string
	field   *orkstackv1.FieldSchema
}

// interfaceRows lists the shared keys first ("*" sorts first), then each op
// by name, each op's keys by name.
func interfaceRows(iface *orkstackv1.ActivityInterface) []interfaceRow {
	var rows []interfaceRow
	for _, key := range sortedMapKeys(iface.GetFields()) {
		rows = append(rows, interfaceRow{op: "*", key: key, field: iface.GetFields()[key]})
	}
	for _, op := range sortedMapKeys(iface.GetOps()) {
		fields := iface.GetOps()[op].GetFields()
		if len(fields) == 0 {
			rows = append(rows, interfaceRow{op: op, key: dash, field: &orkstackv1.FieldSchema{Type: dash}})
			continue
		}
		for _, key := range sortedMapKeys(fields) {
			rows = append(rows, interfaceRow{op: op, key: key, field: fields[key]})
		}
	}
	return rows
}

// defaultText renders a declared default as compact JSON, or "-".
func defaultText(field *orkstackv1.FieldSchema) string {
	if field.GetDefault() == nil {
		return dash
	}
	encoded, err := json.Marshal(field.GetDefault().AsInterface())
	if err != nil {
		return dash
	}
	return string(encoded)
}

func sortedMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
