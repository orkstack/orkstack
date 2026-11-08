package engine

import "github.com/orkstack/orkstack/internal/typeid"

// runIDPrefix begins every run id this release issues.
const runIDPrefix = "ork-"

// NewRunID issues a run id: `ork-` and a UUIDv7 in lowercase base32, 30
// characters (AGENTS.md §7). It runs in the API process, never inside workflow
// code, and is safe for concurrent use.
//
// A run id is the Temporal workflow id, so a duplicate is not cosmetic: the
// second start either fails or silently attaches to the first one's execution.
// Generation therefore has to be safe across engine replicas, not just across
// goroutines in one of them, and the 62 random bits of each id are what make it
// so; within one process, later ids sort after earlier ones (typeid.New).
//
// Entropy alone is a probability, not a guarantee, so it is only half the
// answer: TemporalStarter.StartRun asks the server to reject a reused id
// outright (see ErrRunIDTaken), which is what turns "unlikely" into "proven".
//
// A stored or running id of the older `ork-{timestamp}-{sequence}` shape stays
// exactly as valid: nothing parses a run id, so both shapes are opaque strings
// to every reader.
func NewRunID() string { return runIDPrefix + typeid.New() }
