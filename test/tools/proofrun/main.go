// Command proofrun is the engine behind scripts/proof.sh and
// scripts/integration.sh (AGENTS.md §11.2). It is test tooling, standard
// library only:
//
//	proofrun unit         run the unit suites and write their ledger
//	proofrun integration  compile each integration package once, run its tests
//	                      as separate processes PROOF_JOBS at a time, and write
//	                      the ledger
//	proofrun select       decide what a feature proof runs, or refuse (exit 2)
//	proofrun feature-report  the feature README's scope sections
//	proofrun ui-ledger    check the Playwright report against its listing
//	proofrun record       write proof.json and the README's verdict
//	proofrun verify       refuse a merge whose proof does not cover HEAD's tree
//	proofrun temporal-args  the temporal service's image and arguments
//	proofrun costs        regenerate the cost ledger and the slow list
//	proofrun touches      whether a change touches paths of one scope action
//	proofrun gates        the gates and specs a feature selection runs
//	proofrun free-port    a port nothing listens on
//
// Exit status: 0 pass, 1 a gate failed, 2 an invalid invocation.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	commands := map[string]func([]string) int{
		"unit":           cmdUnit,
		"integration":    cmdIntegration,
		"select":         cmdSelect,
		"feature-report": cmdFeatureReport,
		"ui-ledger":      cmdUILedger,
		"record":         cmdRecord,
		"verify":         cmdVerify,
		"temporal-args":  cmdTemporalArgs,
		"costs":          cmdCosts,
		"free-port":      cmdFreePort,
		"touches":        cmdTouches,
		"gates":          cmdGates,
	}
	cmd, ok := commands[os.Args[1]]
	if !ok {
		usage()
		os.Exit(2)
	}
	os.Exit(cmd(os.Args[2:]))
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: proofrun unit|integration|select|feature-report|ui-ledger|record|verify|temporal-args|costs|touches|gates|free-port [flags]")
}
