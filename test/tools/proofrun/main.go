// Command proofrun is the engine behind scripts/proof.sh, scripts/integration.sh
// and the CI proof graph (AGENTS.md §11.2). It is test tooling, standard
// library only:
//
//	proofrun unit         run the unit suites and write their ledger
//	proofrun fuzz         run every Fuzz target for a bounded time and write its ledger
//	proofrun integration  plan a run once, or run one shard of that plan
//	proofrun compile      compile and list the integration test binaries ahead
//	proofrun select       decide what a feature proof runs, or refuse (exit 2)
//	proofrun feature-report  the feature README's scope sections
//	proofrun ui-ledger    check the Playwright report against its listing
//	proofrun ui-plan      split Playwright spec files by measured wall
//	proofrun record       write a part.json (or a feature proof.json)
//	proofrun merge        merge a proof's parts into one proof.json
//	proofrun verify       refuse a merge or tag whose proof does not cover it
//	proofrun temporal-args  the temporal service's image and arguments
//	proofrun costs        regenerate the cost ledgers and the slow list
//	proofrun overhead     where an existing proof's wall-clock went
//	proofrun push-layout  push OCI layouts byte for byte and check the digests
//	proofrun layout-digests  the images.tsv a layout push would produce
//	proofrun evidence-args  turn Proof-tests:/Proof-specs: lines into flags
//	proofrun touches      whether a change touches paths of one scope action
//	proofrun gates        the gates and specs a feature selection runs
//	proofrun free-port    a port nothing listens on
//	proofrun clean        list, or remove, what test processes that are gone left
//	proofrun runner-health  measure this runner against the floors and write runner-health.json
//	proofrun rerun-plan   decide whether a completed proof run's failed jobs are worth a rerun
//
// Exit status: 0 pass, 1 a gate failed, 2 an invalid invocation.
package main

import (
	"fmt"
	"os"
)

// subcommand is one entry of the dispatch table. The table is the list the
// usage and the documentation drift test enumerate.
type subcommand struct {
	name    string
	summary string
	run     func([]string) int
}

var subcommands = []subcommand{
	{"unit", "run the unit suites and write their ledger", cmdUnit},
	{"fuzz", "run every Fuzz target for a bounded time and write its ledger", cmdFuzz},
	{"integration", "plan a run once, or run one shard of that plan", cmdIntegration},
	{"compile", "compile and list the integration test binaries a later integration adopts", cmdCompile},
	{"select", "decide what a feature proof runs, or refuse", cmdSelect},
	{"feature-report", "the feature README's scope sections", cmdFeatureReport},
	{"ui-ledger", "check the Playwright report against its listing", cmdUILedger},
	{"ui-plan", "split the Playwright spec files by measured wall", cmdUIPlan},
	{"record", "write a part.json (or a feature proof.json)", cmdRecord},
	{"merge", "merge a proof's parts into one proof.json", cmdMerge},
	{"flakes", "derive per-test flake rates from proof ledgers", cmdFlakes},
	{"verify", "refuse a merge or a tag whose proof does not cover it", cmdVerify},
	{"temporal-args", "the temporal service's image and arguments", cmdTemporalArgs},
	{"costs", "regenerate the cost ledgers and the slow list", cmdCosts},
	{"overhead", "where an existing proof's wall-clock went, and its per-process overhead", cmdOverhead},
	{"push-layout", "push OCI layouts byte for byte and check their digests", cmdPushLayout},
	{"layout-digests", "the images.tsv a layout push would produce", cmdLayoutDigests},
	{"evidence-args", "turn Proof-tests:/Proof-specs: lines into flags", cmdEvidenceArgs},
	{"free-port", "a port nothing listens on", cmdFreePort},
	{"touches", "whether a change touches paths of one scope action", cmdTouches},
	{"gates", "the gates and specs a feature selection runs", cmdGates},
	{"clean", "list, or remove, what test processes that are gone left behind", cmdClean},
	{"runner-health", "measure this runner against the floors and write runner-health.json", cmdRunnerHealth},
	{"rerun-plan", "decide whether the failed jobs of a completed proof run are worth an automatic rerun", cmdRerunPlan},
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	for _, c := range subcommands {
		if c.name == os.Args[1] {
			os.Exit(c.run(os.Args[2:]))
		}
	}
	usage()
	os.Exit(2)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: proofrun <subcommand> [flags]")
	for _, c := range subcommands {
		fmt.Fprintf(os.Stderr, "  %-16s %s\n", c.name, c.summary)
	}
}
