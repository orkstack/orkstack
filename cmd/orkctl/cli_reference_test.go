package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// cliReferencePath is the generated reference the site and the docs table
// read. The test is its only writer: a hand edit fails the no-diff check.
const cliReferencePath = "../../docs/CLI.md"

func TestCLIReference_IsGenerated(t *testing.T) {
	root, _ := newRootCommand()
	want := renderCLIReference(root)
	if os.Getenv("ORK_WRITE_CLI_REFERENCE") == "1" {
		if err := os.WriteFile(cliReferencePath, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(cliReferencePath)
	if err != nil {
		t.Fatalf("%v: regenerate with `make cli-reference`", err)
	}
	if string(got) != want {
		t.Fatalf("%s is stale: regenerate with `make cli-reference`\nfirst difference:\n%s",
			cliReferencePath, firstDiff(string(got), want))
	}
}

// renderCLIReference walks the Cobra tree in name order, so the output is a
// function of the command definitions and nothing else.
func renderCLIReference(root *cobra.Command) string {
	var b strings.Builder
	b.WriteString("# orkctl\n\n")
	b.WriteString("<!-- Generated from the Cobra tree by `make cli-reference`; do not edit. -->\n\n")
	b.WriteString(root.Long + "\n\n")
	renderCommand(&b, root, "orkctl")
	return b.String()
}

func renderCommand(b *strings.Builder, cmd *cobra.Command, path string) {
	if cmd.Hidden {
		return
	}
	if cmd.Runnable() {
		fmt.Fprintf(b, "## `%s`\n\n", cmd.CommandPath())
		if cmd.Short != "" {
			fmt.Fprintf(b, "%s\n\n", cmd.Short)
		}
		if long := strings.TrimSpace(cmd.Long); long != "" && long != strings.TrimSpace(cmd.Short) {
			fmt.Fprintf(b, "%s\n\n", long)
		}
		if flags := localFlags(cmd); len(flags) > 0 {
			fmt.Fprintf(b, "Flags:\n\n")
			for _, f := range flags {
				renderFlag(b, f)
			}
			b.WriteString("\n")
		}
		if inherited := inheritedFlags(cmd); len(inherited) > 0 {
			fmt.Fprintf(b, "Inherited flags:\n\n")
			for _, f := range inherited {
				renderFlag(b, f)
			}
			b.WriteString("\n")
		}
	}
	children := append([]*cobra.Command(nil), cmd.Commands()...)
	sort.Slice(children, func(i, j int) bool { return children[i].Name() < children[j].Name() })
	for _, child := range children {
		renderCommand(b, child, path)
	}
}

func localFlags(cmd *cobra.Command) []*pflag.Flag {
	var out []*pflag.Flag
	cmd.LocalNonPersistentFlags().VisitAll(func(f *pflag.Flag) {
		if f.Name != "help" {
			out = append(out, f)
		}
	})
	return sortedFlags(out)
}

func inheritedFlags(cmd *cobra.Command) []*pflag.Flag {
	var out []*pflag.Flag
	cmd.InheritedFlags().VisitAll(func(f *pflag.Flag) {
		if f.Name != "help" {
			out = append(out, f)
		}
	})
	return sortedFlags(out)
}

func sortedFlags(in []*pflag.Flag) []*pflag.Flag {
	sort.Slice(in, func(i, j int) bool { return in[i].Name < in[j].Name })
	return in
}

func renderFlag(b *strings.Builder, f *pflag.Flag) {
	name := "--" + f.Name
	if f.Shorthand != "" {
		name = "-" + f.Shorthand + ", " + name
	}
	value := ""
	if f.Value.Type() != "bool" {
		value = " " + strings.ToUpper(f.Value.Type())
	}
	def := ""
	if f.DefValue != "" && f.DefValue != "false" && f.DefValue != "0" && f.DefValue != "[]" && f.DefValue != "0s" {
		def = fmt.Sprintf(" (default `%s`)", f.DefValue)
	}
	fmt.Fprintf(b, "- `%s%s`%s — %s\n", name, value, def, oneLine(f.Usage))
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// TestCLIReference_NamesEveryRunnableCommand counts the tree, so a command
// that is neither hidden nor in the reference fails even when the file was
// regenerated from a stale build.
func TestCLIReference_NamesEveryRunnableCommand(t *testing.T) {
	root, _ := newRootCommand()
	doc, err := os.ReadFile(cliReferencePath)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.Hidden {
			return
		}
		if cmd.Runnable() {
			want = append(want, cmd.CommandPath())
		}
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(root)
	sort.Strings(want)
	var missing []string
	for _, path := range want {
		if !strings.Contains(string(doc), "## `"+path) {
			missing = append(missing, path)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("the reference omits %d runnable commands: %v", len(missing), missing)
	}
	if t.Failed() {
		return
	}
	t.Logf("CLI reference: %d runnable command paths", len(want))
}

// TestDocs_EveryOrkctlInvocationResolves reads every orkctl invocation a
// tracked document writes as a code span or a line of a code fence and
// resolves its command path in the real Cobra tree, tolerating flags and
// placeholders.
func TestDocs_EveryOrkctlInvocationResolves(t *testing.T) {
	root, _ := newRootCommand()
	files := []string{"../../README.md", "../../CONTRIBUTING.md", "../../AGENTS.md", "../../ARCHITECTURE.md"}
	for _, dir := range []string{"../../docs", "../../deploy", "../../examples"} {
		err := filepathWalkMarkdown(dir, func(path string) { files = append(files, path) })
		if err != nil {
			t.Fatal(err)
		}
	}
	var bad []string
	for _, file := range files {
		if strings.HasSuffix(file, "docs/CLI.md") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, invocation := range orkctlInvocations(string(raw)) {
			args, ok := splitInvocation(invocation)
			if !ok {
				continue
			}
			if len(args) == 0 || placeholder(args[0]) || strings.HasPrefix(args[0], "/") ||
				strings.Contains(invocation, " / ") {
				continue
			}
			cmd, rest, err := root.Find(args)
			if err != nil || cmd == nil {
				bad = append(bad, fmt.Sprintf("%s: %q (%v)", file, invocation, err))
				continue
			}
			if cmd == root && len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
				bad = append(bad, fmt.Sprintf("%s: %q names no command", file, invocation))
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d orkctl invocations do not resolve:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

// orkctlInvocations returns code spans and fenced lines that begin with
// orkctl. A span or line with several commands (`orkctl a && orkctl b`) is
// split on the shell separators.
func orkctlInvocations(doc string) []string {
	var out []string
	inFence := false
	for _, line := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			if strings.HasPrefix(trimmed, "orkctl ") || trimmed == "orkctl" {
				out = append(out, trimmed)
			}
			continue
		}
		for _, span := range codeSpans(line) {
			if strings.HasPrefix(span, "orkctl ") || span == "orkctl" {
				out = append(out, span)
			}
		}
	}
	var split []string
	for _, invocation := range out {
		for _, part := range strings.FieldsFunc(invocation, func(r rune) bool { return r == '&' || r == '|' || r == ';' }) {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "orkctl ") || part == "orkctl" {
				split = append(split, part)
			}
		}
	}
	return split
}

func codeSpans(line string) []string {
	var out []string
	for {
		i := strings.Index(line, "`")
		if i < 0 {
			break
		}
		line = line[i+1:]
		j := strings.Index(line, "`")
		if j < 0 {
			break
		}
		out = append(out, line[:j])
		line = line[j+1:]
	}
	return out
}

// splitInvocation word-splits an orkctl command line, dropping a trailing
// shell comment and treating placeholders and flags as arguments.
func splitInvocation(s string) ([]string, bool) {
	if i := strings.Index(s, " #"); i >= 0 {
		s = s[:i]
	}
	args := strings.Fields(s)
	if len(args) == 0 || args[0] != "orkctl" {
		return nil, false
	}
	return args[1:], true
}

func filepathWalkMarkdown(dir string, visit func(string)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		path := dir + string(os.PathSeparator) + e.Name()
		if e.IsDir() {
			if e.Name() == "node_modules" || e.Name() == "schemas" {
				continue
			}
			if err := filepathWalkMarkdown(path, visit); err != nil {
				return err
			}
			continue
		}
		if strings.HasSuffix(e.Name(), ".md") {
			visit(path)
		}
	}
	return nil
}

// placeholder reports a usage template rather than a command name.
func placeholder(s string) bool {
	return strings.HasPrefix(s, "<") || strings.HasPrefix(s, "[") || s == "NAME" ||
		s == "…" || s == "..." || strings.HasPrefix(s, "^")
}

func firstDiff(got, want string) string {
	g := strings.Split(got, "\n")
	w := strings.Split(want, "\n")
	n := len(g)
	if len(w) < n {
		n = len(w)
	}
	for i := 0; i < n; i++ {
		if g[i] != w[i] {
			return fmt.Sprintf("line %d:\n  got:  %s\n  want: %s", i+1, g[i], w[i])
		}
	}
	return fmt.Sprintf("got %d lines, want %d", len(g), len(w))
}
