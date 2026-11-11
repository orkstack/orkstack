// Command proofrun is the engine behind scripts/proof.sh, scripts/integration.sh
// and the CI proof graph (AGENTS.md §11.2). It is test tooling, standard
// library only:
//
//	proofrun unit         run the unit suites and write their ledger
//	proofrun fuzz         run every Fuzz target for a bounded time and write its ledger
//	proofrun integration  plan a run once, or run one shard of that plan
//	proofrun compile      compile and list the integration test binaries ahead
//	proofrun ui-ledger    check the Playwright report against its listing
//	proofrun ui-plan      split Playwright spec files by measured wall
//	proofrun record       write a part.json
//	proofrun merge        merge a proof's parts into one proof.json
//	proofrun verify       refuse a merge or tag whose proof does not cover it
//	proofrun temporal-args  the temporal service's image and arguments
//	proofrun temporal-log   keep a proof's own Temporal server's log, bounded and compressed
//	proofrun costs        regenerate the cost ledgers and the slow list
//	proofrun push-layout  push OCI layouts byte for byte and check the digests
//	proofrun layout-digests  the images.tsv a layout push would produce
//	proofrun touches      the gate files a change edits, and the list rows it adds or removes
//	proofrun free-port    a port nothing listens on
//	proofrun clean        list, or remove, what test processes that are gone left
//	proofrun job-log      bound a GitHub job log to what the merger reads
//	proofrun pr-title     refuse a pull request title that is not a Conventional Commit
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
	{"ui-ledger", "check the Playwright report against its listing", cmdUILedger},
	{"ui-plan", "split the Playwright spec files by measured wall", cmdUIPlan},
	{"record", "write a part.json", cmdRecord},
	{"merge", "merge a proof's parts into one proof.json", cmdMerge},
	{"verify", "refuse a merge or a tag whose proof does not cover it", cmdVerify},
	{"temporal-args", "the temporal service's image and arguments", cmdTemporalArgs},
	{"temporal-log", "keep a proof's own Temporal server's log, its end bounded and compressed", cmdTemporalLog},
	{"costs", "regenerate the cost ledgers and the slow list", cmdCosts},
	{"push-layout", "push OCI layouts byte for byte and check their digests", cmdPushLayout},
	{"layout-digests", "the images.tsv a layout push would produce", cmdLayoutDigests},
	{"free-port", "a port nothing listens on", cmdFreePort},
	{"touches", "the gate files a change edits, and the list rows it adds or removes", cmdTouches},
	{"clean", "list, or remove, what test processes that are gone left behind", cmdClean},
	{"job-log", "bound a GitHub job log to what the merger reads from it", cmdJobLog},
	{"pr-title", "refuse a pull request title that is not a Conventional Commit", cmdPRTitle},
}

func main() {
	os.Exit(runCommand(os.Args[1:]))
}

// runCommand runs the subcommand args name, with its flags, and returns its
// exit status.
func runCommand(args []string) int {
	if len(args) < 1 {
		usage()
		return 2
	}
	for _, c := range subcommands {
		if c.name == args[0] {
			return c.run(args[1:])
		}
	}
	usage()
	return 2
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: proofrun <subcommand> [flags]")
	for _, c := range subcommands {
		fmt.Fprintf(os.Stderr, "  %-16s %s\n", c.name, c.summary)
	}
}
