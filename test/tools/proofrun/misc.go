package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

// cmdFreePort prints a port nothing listens on at the moment of asking.
// Another process may take it before the caller binds it; the caller's bind
// then fails loudly, which is never a false pass.
func cmdFreePort(args []string) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun free-port:", err)
		return 1
	}
	defer l.Close()
	fmt.Println(l.Addr().(*net.TCPAddr).Port)
	return 0
}

// cmdTouches reports whether the change since the merge base with -base
// touches a path whose scope rule carries -action (exit 0, the paths
// printed) or not (exit 1). The full proof asks it whether the observability
// gate is due.
func cmdTouches(args []string) int {
	fs := flag.NewFlagSet("touches", flag.ContinueOnError)
	base := fs.String("base", "main", "the ref the change is measured from")
	action := fs.String("action", "", "a scope action, e.g. observability")
	scopeFile := fs.String("scope", "scripts/proof-scope.tsv", "the scope table")
	if err := fs.Parse(args); err != nil || !knownActions[*action] {
		fmt.Fprintln(os.Stderr, "proofrun touches -base REF -action ACTION")
		return 2
	}
	scope, err := readScope(*scopeFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun touches:", err)
		return 2
	}
	bc, err := gitOut("rev-parse", "--verify", "--quiet", *base+"^{commit}")
	if err != nil {
		fmt.Fprintf(os.Stderr, "proofrun touches: base %q is not a commit\n", *base)
		return 2
	}
	mb, err := gitOut("merge-base", strings.TrimSpace(bc), "HEAD")
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun touches:", err)
		return 2
	}
	paths, err := diffPaths(strings.TrimSpace(mb))
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun touches:", err)
		return 2
	}
	hit := touching(scope, paths, *action)
	for _, p := range hit {
		fmt.Println(p)
	}
	if len(hit) == 0 {
		return 1
	}
	return 0
}

func touching(scope scopeTable, paths []changedPath, action string) []string {
	var hit []string
	for _, p := range paths {
		if r := scope.match(p.Path); r != nil {
			for _, a := range r.Actions {
				if a == action {
					hit = append(hit, p.Path)
					break
				}
			}
		}
	}
	return hit
}

// cmdGates prints, as KEY=VALUE lines a shell reads without eval, the gates
// a feature selection runs and the specs it names.
func cmdGates(args []string) int {
	fs := flag.NewFlagSet("gates", flag.ContinueOnError)
	selFile := fs.String("select", "", "a selection written by proofrun select")
	if err := fs.Parse(args); err != nil || *selFile == "" {
		fmt.Fprintln(os.Stderr, "proofrun gates -select FILE")
		return 2
	}
	var sel selection
	if err := readJSON(*selFile, &sel); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gates:", err)
		return 2
	}
	writeGates(os.Stdout, &sel)
	return 0
}

func writeGates(w io.Writer, sel *selection) {
	yes := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	integration := false
	for _, ps := range sel.Integration {
		if ps.Whole != "" || len(ps.run()) > 0 {
			integration = true
		}
	}
	specs := ""
	switch {
	case sel.AllSpecs != "":
		specs = "ALL"
	default:
		var files []string
		for _, s := range sel.Specs {
			files = append(files, strings.TrimPrefix(s.File, "ui/"))
		}
		specs = strings.Join(files, " ")
	}
	fmt.Fprintf(w, "integration=%s\n", yes(integration))
	fmt.Fprintf(w, "ui_check=%s\n", yes(sel.Gates.UICheck))
	fmt.Fprintf(w, "ui_unit=%s\n", yes(sel.Gates.UIUnit))
	fmt.Fprintf(w, "ui_e2e=%s\n", yes(specs != ""))
	fmt.Fprintf(w, "activity_fixture=%s\n", yes(sel.Gates.Fixture))
	fmt.Fprintf(w, "observability=%s\n", yes(sel.Gates.Observability))
	fmt.Fprintf(w, "specs=%s\n", specs)
}
