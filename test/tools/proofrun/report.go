package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// printSelection writes the plan a selection makes, for the terminal.
func printSelection(w io.Writer, sel *selection) {
	fmt.Fprintf(w, "feature selection against %s (merge base %s)\n", sel.BaseRef, short(sel.MergeBase))
	for _, p := range sel.Paths {
		fmt.Fprintf(w, "  %-3s %s -> %s\n", p.Status, p.Path, strings.Join(p.Actions, ","))
	}
	for _, e := range sel.Evidence {
		fmt.Fprintf(w, "  evidence for %s: %s\n", e.Target, orNone(e.Evidence))
	}
	pkgs := sortedKeys(sel.Integration)
	for _, p := range pkgs {
		ps := sel.Integration[p]
		if ps.Whole != "" {
			fmt.Fprintf(w, "  integration %s: whole package (%s)\n", relDir(p), ps.Whole)
			continue
		}
		fmt.Fprintf(w, "  integration %s: %d evidence, %d regression, %d slow-omitted\n", relDir(p), len(ps.Evidence), len(ps.Regression), len(ps.SlowOmitted))
	}
	for _, n := range sel.Notes {
		fmt.Fprintf(w, "  note: %s\n", n)
	}
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func orNone(s []string) string {
	if len(s) == 0 {
		return "(none)"
	}
	return strings.Join(s, ", ")
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// outcomes indexes ledgers by test.
type outcomes map[testKey]entry

func loadOutcomes(dir string) outcomes {
	o := outcomes{}
	for _, name := range []string{"unit-tests.json", "integration-tests.json"} {
		var l ledger
		if readJSON(filepath.Join(dir, name), &l) != nil {
			continue
		}
		for _, e := range l.Tests {
			o[testKey{e.Package, e.Test}] = e
		}
	}
	return o
}

func loadSpecOutcomes(dir string) map[string]string {
	out := map[string]string{}
	var l uiLedger
	if readJSON(filepath.Join(dir, "ui-e2e-tests.json"), &l) != nil {
		return out
	}
	for _, t := range l.Tests {
		cur := out["ui/e2e/"+t.File]
		switch {
		case t.Outcome != outcomePass:
			out["ui/e2e/"+t.File] = t.Outcome
		case cur == "":
			out["ui/e2e/"+t.File] = outcomePass
		}
	}
	return out
}

// cmdFeatureReport writes the feature README's scope sections.
func cmdFeatureReport(args []string) int {
	fs := flag.NewFlagSet("feature-report", flag.ContinueOnError)
	selFile := fs.String("select", "", "the selection the proof ran")
	dir := fs.String("dir", "", "the proof directory (for its ledgers)")
	if err := fs.Parse(args); err != nil || *selFile == "" || *dir == "" {
		fmt.Fprintln(os.Stderr, "proofrun feature-report -select FILE -dir DIR")
		return 2
	}
	var sel selection
	if err := readJSON(*selFile, &sel); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	featureReport(os.Stdout, &sel, loadOutcomes(*dir), loadSpecOutcomes(*dir))
	return 0
}

func featureReport(w io.Writer, sel *selection, o outcomes, specs map[string]string) {
	if len(sel.DocsWarning) > 0 {
		fmt.Fprintf(w, "## Documentation\n\n")
		for _, warning := range sel.DocsWarning {
			fmt.Fprintf(w, "WARNING: %s\n", warning)
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "## Scope\n\n")
	fmt.Fprintf(w, "- base: `%s` = %s\n- merge base: %s\n", sel.BaseRef, sel.BaseCommit, sel.MergeBase)
	fmt.Fprintf(w, "- selection inputs, verbatim: `--base %s`", sel.BaseRef)
	for _, t := range sel.Inputs.Tests {
		fmt.Fprintf(w, " `--tests %s`", t)
	}
	for _, s := range sel.Inputs.Specs {
		fmt.Fprintf(w, " `--specs %s`", s)
	}
	fmt.Fprintf(w, "\n\n### Changed paths\n\n")
	if len(sel.Paths) == 0 {
		fmt.Fprintf(w, "(none: the tree equals its merge base)\n")
	}
	for _, p := range sel.Paths {
		fmt.Fprintf(w, "- `%s` %s — rule `%s` (scripts/proof-scope.tsv:%d) → %s\n", p.Status, p.Path, p.Rule, p.Line, strings.Join(p.Actions, ", "))
	}

	fmt.Fprintf(w, "\n### Evidence for each changed package and path\n\n")
	if len(sel.Evidence) == 0 {
		fmt.Fprintf(w, "No change outside tests and comments needed evidence.\n")
	}
	for _, e := range sel.Evidence {
		fmt.Fprintf(w, "- **%s**:", e.Target)
		if len(e.Evidence) == 0 {
			fmt.Fprintf(w, " (none)\n")
			continue
		}
		fmt.Fprintln(w)
		for _, ev := range e.Evidence {
			fmt.Fprintf(w, "  - %s — %s\n", ev, evidenceOutcome(ev, o, specs))
		}
	}

	fmt.Fprintf(w, "\n### Unit evidence (run by the unit gate)\n\n")
	if len(sel.UnitEvidence) == 0 {
		fmt.Fprintf(w, "(none)\n")
	}
	for _, u := range sel.UnitEvidence {
		e, ok := o[testKey{u.Package, u.Test}]
		res := "no outcome"
		if ok {
			res = e.Outcome
		}
		fmt.Fprintf(w, "- %s.%s — %s (%s)\n", relDir(u.Package), u.Test, res, u.Why)
	}

	fmt.Fprintf(w, "\n### Integration packages\n\n")
	if len(sel.Integration) == 0 {
		fmt.Fprintf(w, "None ran: no integration package's test binary depends on what changed.\n")
	}
	for _, p := range sortedKeys(sel.Integration) {
		ps := sel.Integration[p]
		if ps.Whole != "" {
			fmt.Fprintf(w, "- `%s`: **whole package** — %s\n", relDir(p), ps.Whole)
			continue
		}
		n := len(ps.run())
		fmt.Fprintf(w, "- `%s`: **%d of %d** integration tests — %s\n", relDir(p), n, ps.Declared, ps.Why)
		for _, t := range sortedKeys(ps.Evidence) {
			res := "no outcome"
			if e, ok := o[testKey{p, t}]; ok {
				res = e.Outcome
			}
			fmt.Fprintf(w, "  - evidence %s — %s (%s)\n", t, res, ps.Evidence[t])
		}
		if len(ps.Regression) > 0 {
			fail := 0
			for _, t := range ps.Regression {
				if e := o[testKey{p, t}]; e.Outcome != outcomePass {
					fail++
				}
			}
			fmt.Fprintf(w, "  - regression set: %d tests, %d without a pass (never evidence)\n", len(ps.Regression), fail)
		}
		for _, d := range ps.SlowOmitted {
			fmt.Fprintf(w, "  - left out: on the slow list (%.0fs in %s): %s\n", d.Seconds, orDash(sel.SlowSource), d.Test)
		}
	}
	for _, n := range sel.NotRun {
		fmt.Fprintf(w, "- not run: `%s`\n", n)
	}

	fmt.Fprintf(w, "\n### Playwright specs\n\n")
	if sel.AllSpecs != "" {
		fmt.Fprintf(w, "Every spec ran: %s.\n", sel.AllSpecs)
	} else if len(sel.Specs) == 0 {
		fmt.Fprintf(w, "No spec ran; every spec is left to the merge proof.\n")
	} else {
		for _, s := range sel.Specs {
			fmt.Fprintf(w, "- %s — %s (%s)\n", s.File, orDash(specs[s.File]), s.Why)
		}
		fmt.Fprintf(w, "\nEvery other spec is left to the merge proof.\n")
	}
	if len(sel.Removed) > 0 {
		fmt.Fprintf(w, "\n### Removed tests\n\n")
		for _, r := range sel.Removed {
			fmt.Fprintf(w, "- %s\n", r)
		}
	}
	if len(sel.Notes) > 0 {
		fmt.Fprintf(w, "\n### Notes\n\n")
		for _, n := range sel.Notes {
			fmt.Fprintf(w, "- %s\n", n)
		}
	}
	fmt.Fprintln(w)
}

// evidenceOutcome reports what happened to one evidence line.
func evidenceOutcome(ev string, o outcomes, specs map[string]string) string {
	if r, ok := specs[ev]; ok {
		return r
	}
	if strings.HasPrefix(ev, "ui/") {
		return "see the ui gates"
	}
	i := strings.LastIndex(ev, ".")
	if i < 0 || strings.Contains(ev, " ") {
		return "see the gates"
	}
	k := testKey{importPathOf(ev[:i]), ev[i+1:]}
	if e, ok := o[k]; ok {
		return e.Outcome
	}
	return "no outcome"
}
