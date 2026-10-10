package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// The commands that cannot be undone — orkflow delete, namespace delete,
// cache purge, release promote and rollback, and lowering the maximum run
// retention — state what they are about to do and ask before the request
// leaves this process. --yes is the one way past the question, for a script
// and for a person alike; --json does not imply it.

// yesFlagHelp is the --yes help every confirming command shares.
const yesFlagHelp = "do not ask for confirmation (required when standard input is not a terminal)"

// addYesFlag registers --yes on a confirming command.
func addYesFlag(cmd *cobra.Command, yes *bool) {
	cmd.Flags().BoolVar(yes, "yes", false, yesFlagHelp)
}

// confirmDestructive asks the person at the terminal to type "yes" before
// action happens. prompt states the blast radius; action names the operation
// in the refusal ("delete orkflow \"x\"").
//
// With --yes it asks nothing. With no terminal on standard input (a pipe, a
// file, a closed stdin) it refuses rather than reads: a CI job whose stdin is
// not a person must fail loudly, not silently do nothing. Any answer but
// "yes" is a cancellation, which is an error: the command did not do what it
// was asked to.
func confirmDestructive(cmd *cobra.Command, g *globalFlags, action, prompt string, yes bool) error {
	if yes {
		return nil
	}
	if !g.stdinIsTerminal(cmd) {
		return fmt.Errorf("refusing to %s without confirmation: standard input is not a terminal; pass --yes", action)
	}
	stderr := cmd.ErrOrStderr()
	fmt.Fprintf(stderr, "%s\nType 'yes' to confirm: ", prompt)
	answer, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read confirmation: %w", err)
	}
	if err != nil {
		// End of input left the cursor on the prompt line.
		fmt.Fprintln(stderr)
	}
	if strings.TrimSpace(answer) != "yes" {
		return fmt.Errorf("%s: cancelled", action)
	}
	return nil
}

// stdinIsTerminal reports whether the command's standard input is a
// terminal a person can answer from. The check is the standard library's: a
// character device. A test sets g.terminal instead.
func (g *globalFlags) stdinIsTerminal(cmd *cobra.Command) bool {
	if g.terminal != nil {
		return g.terminal()
	}
	file, ok := cmd.InOrStdin().(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// displayNamespace is the namespace a prompt names: the one this invocation
// acts in, `default` when none was chosen.
func displayNamespace(namespace string) string {
	if namespace == "" {
		return "default"
	}
	return namespace
}
