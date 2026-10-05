package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The version surface: see the versions, move the pointer, compare two of
// them, and edit any one of them in $EDITOR. Every command is scoped by the
// global --namespace, and every one that produces data has --json.

func newOrkflowVersionsCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:     "versions <name>",
		Aliases: []string{"vs"},
		Short:   "List an orkflow's versions, newest first",
		Long: "versions lists every stored version of one orkflow, newest applied first.\n" +
			"CURRENT marks the version a run with no --version executes: the promoted\n" +
			"one, or the most recently applied when nothing was ever promoted.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.orkflowClient().ListOrkflowVersions(ctx, connect.NewRequest(
				&orkstackv1.ListOrkflowVersionsRequest{Name: args[0], Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("list orkflow versions", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderOrkflowVersions(cmd.OutOrStdout(), args[0], resp.Msg)
		},
	}
}

func newOrkflowPromoteCommand(g *globalFlags) *cobra.Command {
	var acknowledgeBreaking bool
	cmd := &cobra.Command{
		Use:   "promote <name> <version>",
		Short: "Make one version the current one",
		Long: "promote points an orkflow at an existing version. Runs submitted after it\n" +
			"with no --version execute that version; runs already in flight finish on\n" +
			"the version they started with.\n\n" +
			"Rolling back is the same command with the older version.\n\n" +
			"Promoting compares the version's declared interface (its inputs: and\n" +
			"outputs:) with the version that runs now. A BREAKING change is refused,\n" +
			"naming each change, until you pass --acknowledge-breaking; compatible and\n" +
			"unknown changes are never refused. A rollback is gated alike: rolling back\n" +
			"across an added input removes it. See orkctl orkflow compat and callers.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.orkflowClient().SetCurrentOrkflowVersion(ctx, connect.NewRequest(
				&orkstackv1.SetCurrentOrkflowVersionRequest{
					Name:                args[0],
					Version:             args[1],
					Namespace:           g.namespace,
					AcknowledgeBreaking: acknowledgeBreaking,
				},
			))
			if err != nil {
				return rpcError("promote orkflow", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderPromotion(cmd.OutOrStdout(), resp.Msg)
		},
	}
	cmd.Flags().BoolVar(&acknowledgeBreaking, "acknowledge-breaking", false,
		"promote even when the version breaks the declared interface of the one that runs now; "+
			"see orkctl orkflow compat")
	return cmd
}

func newOrkflowDiffCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "diff <name> <from> <to>",
		Short: "Show what changed between two versions",
		Long: "diff renders the unified diff between two versions' source YAML. The diff\n" +
			"is computed by the engine, so every client shows the same answer.\n\n" +
			"An empty version (\"\") on either side means the current one, so\n" +
			"`orkctl orkflow diff deploy \"\" 2.0.0` reads as \"what would promoting change\".",
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.orkflowClient().DiffOrkflowVersions(ctx, connect.NewRequest(
				&orkstackv1.DiffOrkflowVersionsRequest{
					Name:        args[0],
					FromVersion: args[1],
					ToVersion:   args[2],
					Namespace:   g.namespace,
				},
			))
			if err != nil {
				return rpcError("diff orkflow versions", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderDiff(cmd.OutOrStdout(), args[0], resp.Msg)
		},
	}
}

// newOrkflowEditCommand is the round trip an editor needs: fetch a version's
// source, open it, and apply what comes back.
//
// ANY stored version is a valid base. `--version` picks one; without it the
// base is the head, the newest applied version, which is what this command
// always did. Editing an old version is not a mistake to be prevented — it is
// how a user forks forward from it: the apply lands as a NEW version, becomes
// the newest applied one, and moves nothing else. What RUNS is still whatever
// was promoted, until somebody promotes again.
//
// The apply carries two tokens, because there are two facts. base_version says
// which version this text came from — provenance, and the anchor of any diff
// that follows. expected_head_version says which version was the newest when
// the document was fetched, and it is the only one a competing apply can
// invalidate: if somebody applies while the editor is open, the apply is
// REFUSED naming both, whatever base was chosen. Nothing is locked meanwhile —
// a lock across a human's editing session is a lock nobody can bound, and
// losing the race costs nothing because the edit is kept on disk.
func newOrkflowEditCommand(g *globalFlags) *cobra.Command {
	var (
		editor  string
		version string
	)
	cmd := &cobra.Command{
		Use:   "edit <name>",
		Short: "Edit an orkflow in $EDITOR and apply the result",
		Long: "edit fetches a version's source YAML, opens it in $EDITOR, and applies\n" +
			"what you save as a NEW version — a stored version is never rewritten.\n" +
			"Saving without changing anything applies nothing.\n\n" +
			"--version picks the version to start from. ANY stored version is a valid\n" +
			"base, and editing an old one is how you fork forward from it: the result\n" +
			"becomes the newest applied version, and what runs does not change until\n" +
			"you promote it. Without --version the base is the newest applied version.\n\n" +
			"If somebody applies a version while your editor is open, the apply is\n" +
			"refused and names both versions so you can diff them — whichever version\n" +
			"you started from. Your edit is kept in a file the refusal names.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			// Two questions, one listing: which version to edit (when the user
			// named none) and which head this edit is racing.
			versions, err := g.orkflowClient().ListOrkflowVersions(ctx, connect.NewRequest(
				&orkstackv1.ListOrkflowVersionsRequest{Name: name, Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("read orkflow versions", err)
			}
			head := versions.Msg.GetHeadVersion()
			base := version
			if base == "" {
				base = head
			}
			fetched, err := g.orkflowClient().GetOrkflow(ctx, connect.NewRequest(
				&orkstackv1.GetOrkflowRequest{Name: name, Version: base, Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("get orkflow", err)
			}
			// The version the server answered with, not the one asked for: an
			// empty --version on an orkflow with no head resolves server-side,
			// and the base sent back has to be the version actually read.
			base = fetched.Msg.GetOrkflow().GetVersion()
			source := fetched.Msg.GetOrkflow().GetSourceYaml()
			if source == "" {
				return fmt.Errorf(
					"orkflow %s@%s was stored without its source YAML, so there is nothing to edit",
					name, base)
			}

			edited, err := editInEditor(cmd, editor, name, base, source)
			if err != nil {
				return err
			}
			if edited == source {
				fmt.Fprintf(cmd.OutOrStdout(), "No change to %s@%s; nothing applied.\n", name, base)
				return nil
			}

			applyCtx, applyCancel := g.unaryContext(cmd)
			defer applyCancel()
			applied, err := g.orkflowClient().ApplyOrkflow(applyCtx, connect.NewRequest(
				&orkstackv1.ApplyOrkflowRequest{
					Yaml:                edited,
					Namespace:           g.namespace,
					BaseVersion:         base,
					ExpectedHeadVersion: head,
				},
			))
			if err != nil {
				// The edit is worth more than the error message: keep it.
				if saved := saveRejectedEdit(name, edited); saved != "" {
					return fmt.Errorf("%w\nyour edit was kept at %s", rpcError("apply", err), saved)
				}
				return rpcError("apply", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), applied.Msg)
			}
			return renderEditedApply(cmd.OutOrStdout(), base, head, applied.Msg)
		},
	}
	cmd.Flags().StringVar(&editor, "editor", "",
		"editor command to open the document with (default: $ORK_EDITOR, $VISUAL, $EDITOR)")
	cmd.Flags().StringVar(&version, "version", "",
		"version to edit; any stored version is a valid base (default: the newest applied)")
	return cmd
}

// renderEditedApply prints what an edit applied, and says so twice when the
// base was not the newest applied version: a fork is a thing a user can do by
// accident (a stale `--version` in shell history), and the two versions it
// happened between are the only way to see it did.
func renderEditedApply(
	out io.Writer,
	base, head string,
	resp *orkstackv1.ApplyOrkflowResponse,
) error {
	if err := renderApply(out, resp); err != nil {
		return err
	}
	if base == head || head == "" {
		return nil
	}
	name := resp.GetOrkflow().GetName()
	fmt.Fprintf(out,
		"\nForked from %s@%s, which was not the newest applied version (%s was).\n",
		name, base, head)
	fmt.Fprintf(out,
		"%s@%s is now the newest applied version; what runs is unchanged until you\n"+
			"promote it (orkctl orkflow promote %s %s).\n",
		name, resp.GetOrkflow().GetVersion(), name, resp.GetOrkflow().GetVersion())
	return nil
}

// editInEditor writes source to a temporary file, opens it, and returns what
// was saved.
//
// The file keeps the orkflow's name and the .ork.yaml suffix so an editor
// applies the syntax highlighting and schema it would to the real document.
func editInEditor(cmd *cobra.Command, override, name, version, source string) (string, error) {
	command := editorCommand(override)
	if command == "" {
		return "", fmt.Errorf(
			"no editor configured: set $EDITOR (or $VISUAL, $ORK_EDITOR), or pass --editor")
	}
	dir, err := os.MkdirTemp("", "orkctl-edit-")
	if err != nil {
		return "", fmt.Errorf("create edit directory: %w", err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, name+".ork.yaml")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "editing %s@%s in %s\n", name, version, command)
	argv := append(splitEditorCommand(command), path)
	editor := exec.CommandContext(cmd.Context(), argv[0], argv[1:]...)
	editor.Stdin, editor.Stdout, editor.Stderr = os.Stdin, cmd.ErrOrStderr(), cmd.ErrOrStderr()
	if err := editor.Run(); err != nil {
		return "", fmt.Errorf("editor %q exited with an error: %w", command, err)
	}
	edited, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read the edited document: %w", err)
	}
	return string(edited), nil
}

// editorCommand resolves which editor to open, in the order a user expects:
// the flag, then ORK_EDITOR (this tool only), then the standard VISUAL and
// EDITOR, then the platform's usual fallback.
func editorCommand(override string) string {
	if override != "" {
		return override
	}
	for _, name := range []string{"ORK_EDITOR", "VISUAL", "EDITOR"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	if runtime.GOOS == "windows" {
		return "notepad"
	}
	return "vi"
}

// splitEditorCommand splits an editor setting into a command and arguments, so
// `EDITOR="code --wait"` works as well as `EDITOR=vim`.
func splitEditorCommand(command string) []string {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return []string{command}
	}
	return fields
}

// saveRejectedEdit keeps an edit the server refused, so a stale-edit refusal
// costs the user a re-apply and never their work. It returns the file it
// wrote, or "" when it could not write one — in which case the caller reports
// the original error alone rather than a failure to save.
func saveRejectedEdit(name, document string) string {
	file, err := os.CreateTemp("", "orkctl-"+name+"-*.ork.yaml")
	if err != nil {
		return ""
	}
	defer file.Close()
	if _, err := file.WriteString(document); err != nil {
		return ""
	}
	return file.Name()
}

func renderOrkflowVersions(
	out io.Writer,
	name string,
	resp *orkstackv1.ListOrkflowVersionsResponse,
) error {
	versions := resp.GetVersions()
	if len(versions) == 0 {
		fmt.Fprintf(out, "No versions of %s.\n", name)
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "VERSION\tCURRENT\tCOMPAT\tAPPLIED\tDESCRIPTION")
	for _, version := range versions {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n",
			version.GetVersion(), currentMark(version.GetIsCurrent()),
			verdictWord(version.GetInterfaceVerdict()),
			formatTime(version.GetAppliedAt()), orDash(version.GetDescription()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	if resp.GetCurrentVersion() == "" {
		fmt.Fprintf(out,
			"No version has been promoted, so %s runs its newest applied version (%s).\n",
			name, resp.GetHeadVersion())
		return nil
	}
	fmt.Fprintf(out, "Current: %s   Newest applied: %s\n",
		resp.GetCurrentVersion(), resp.GetHeadVersion())
	return nil
}

func currentMark(isCurrent bool) string {
	if isCurrent {
		return "*"
	}
	return ""
}

func renderPromotion(out io.Writer, resp *orkstackv1.SetCurrentOrkflowVersionResponse) error {
	fmt.Fprintf(out, "Promoted %s to version %s in namespace %s\n",
		resp.GetName(), resp.GetCurrentVersion(), resp.GetNamespace())
	if previous := resp.GetPreviousVersion(); previous != "" {
		fmt.Fprintf(out, "Previous: %s (roll back with: orkctl orkflow promote %s %s)\n",
			previous, resp.GetName(), previous)
	} else {
		fmt.Fprintln(out, "Previous: none; this orkflow ran its newest applied version until now.")
	}
	compared, current, name := resp.GetComparedVersion(), resp.GetCurrentVersion(), resp.GetName()
	switch resp.GetInterfaceVerdict() {
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE:
		fmt.Fprintf(out, "Interface against %s: compatible\n", compared)
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN:
		fmt.Fprintf(out, "Interface against %s: unknown (orkctl orkflow compat %s %s %s says why)\n",
			compared, name, compared, current)
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING:
		fmt.Fprintf(out, "Interface against %s: BREAKING, acknowledged (orkctl orkflow compat %s %s %s "+
			"lists what breaks)\n", compared, name, compared, current)
	}
	return nil
}

func renderDiff(out io.Writer, name string, resp *orkstackv1.DiffOrkflowVersionsResponse) error {
	if resp.GetUnifiedDiff() == "" {
		fmt.Fprintf(out, "%s@%s and %s@%s are identical.\n",
			name, resp.GetFromVersion(), name, resp.GetToVersion())
		return nil
	}
	fmt.Fprint(out, resp.GetUnifiedDiff())
	return nil
}
