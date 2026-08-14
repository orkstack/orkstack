# Implementation Notes — M4 `orkctl`

Task: implement milestone M4 of OrkStack, the Cobra CLI `orkctl`, in
`/Users/daniel/Code/orkstack-wt-m4`. Engine (M2) and artifacts (M1) are merged
on `main`; M3 (Orker) is being built in parallel and is unavailable.

Scope lock: `cmd/orkctl` only, plus `go.mod`/`go.sum` and `proof/M4`.
No edits to `internal/*`, `pkg/*`, `proto/`, `gen/`, `cmd/orkengine`,
`cmd/orker`, `ui/`. Verified with `git status --short` — the only tracked files
changed outside `cmd/orkctl` and `proof/M4` are `go.mod` and `go.sum`
(cobra v1.10.2, pflag v1.0.10, mousetrap v1.1.0).

## Acceptance matrix

| # | Contract clause | Evidence (command / test / artifact) | State |
|---|---|---|---|
| 1a | Binary builds via `go build ./cmd/orkctl` | `go build ./cmd/orkctl` (run this session); `buildBinary` in `cmd/orkctl/integration_test.go` builds it again in the integration lane | pass |
| 1b | Commands: apply, workflow list/get, run submit (`--input`, `--watch`), run get, run list, run watch, run cancel, plugin list, version | `cmd/orkctl/{apply,workflow,run,watch,plugin,version}.go`; one behavior test per command in `cli_test.go` | pass |
| 1c | Global `--server` (env `ORK_SERVER`, default `http://localhost:8080`), `--json`, `--timeout` all work | `TestServerFlag_FallsBackToTheOrkServerEnvironmentVariable`, `TestServerFlag_OverridesTheEnvironmentVariable`, `TestServerFlag_RejectsAnUnusableURL`, `TestTimeoutFlag_BoundsTheRequest`, `TestJSONFlag_EmitsParseableJSONForEveryDataCommand`; default constant `defaultServer` in `root.go` | pass |
| 2 | Talks to the engine only through generated connect-go clients; no hand-rolled HTTP/JSON; no edits to `gen/` | Every RPC goes through `orkstackv1connect.New{Workflow,Run,Plugin}ServiceClient` (`root.go`); `git status` shows no `gen/` change | pass |
| 3 | Behavior tests run the REAL API server in-process and prove apply / workflow list+get / run submit / run get+list / plugin list / run cancel | `cmd/orkctl/harness_test.go` (`newFixture` mirrors `internal/api/service_test.go`: `api.NewService` + `api.NewMux` + `api.NewServer` on a loopback listener); assertions on stdout + exit code only | pass |
| 4a | `--json` is valid parseable JSON for every data-emitting command (parsed with `encoding/json`) | `TestJSONFlag_EmitsParseableJSONForEveryDataCommand` (10 subtests), `TestJSONFlag_WatchDocumentCarriesEventsAndFinalRun`, `TestJSONFlag_ApplyCarriesTheCompiledWorkflow` | pass |
| 4b | Human output golden-tested for apply, run get, run list | `testdata/{apply,run-get,run-list}.golden` + `requireGolden` in `golden_test.go` | pass |
| 5 | Exit 0 on success; non-zero on command error and on an observed FAILED run | `TestWorkflowGet_UnknownWorkflowExitsNonZero`, `TestUnreachableServer_ExitsNonZero`, `TestRunWatch_ExitsNonZeroWhenTheRunFails` (2), `TestRunSubmitWatch_ExitsNonZeroWhenTheRunFails` (2), `TestRunWatch_ExitsNonZeroWhenTheRunIsCanceled` (3) | pass |
| 6 | `run watch` consumes `RunService.StreamEvents`, prints live until terminal, then exits; stub ExecuteTask worker allowed inside `cmd/orkctl` tests | `watch.go` uses the generated `StreamEvents` server stream; `TestRunWatch_PrintsEachEventBeforeTheNextOneIsPublished` deadlocks unless output is live; stub activity worker lives in `integration_test.go` only | pass |
| 7 | `integration` build-tag test drives the CLI against a real engine process + real Temporal: apply -> submit --watch -> run get, asserting terminal status and matching exit code; skips cleanly without Temporal | `TestIntegration_CLIDrivesARealEngineToTerminalRuns`; `t.Skipf` on `net.DialTimeout` and `client.DialContext` failure | pass |
| 8 | `make lint` and `go test ./...` green module-wide; `go test -tags=integration ./cmd/orkctl/...` green with infra up | Recorded in `proof/M4/test.log` | pass |
| 9 | Proof under `proof/M4/`: test.log, recorded CLI session transcript, `--json` sample, README with reproduce commands | `proof/M4/{test.log,cli-session.txt,json-sample.json,README.md,reproduce.sh}` | pass |
| 10 | Scope: only `cmd/orkctl` (+ go.mod/go.sum, proof/M4) | `git status --short` | pass |

## Interface decisions (constrained choices, recorded)

- **Exit taxonomy**: 0 success, 1 command/RPC error, 2 watched run FAILED,
  3 watched run CANCELED. The contract only demands "non-zero"; distinct codes
  let scripts tell a failed run from a failed command. `main` is the only place
  that calls `os.Exit`; commands return an `*exitCodeError`.
- **Transport**: `http.Transport.Protocols.SetUnencryptedHTTP2(true)` for
  `http://` servers (the engine's h2c listener), standard HTTP/1.1+HTTP/2 for
  `https://`. `TestCLITransport_SpeaksUnencryptedHTTP2ToTheEngine` asserts
  `resp.Proto == "HTTP/2.0"` against the real server. No
  `golang.org/x/net/http2` dependency; Go 1.25 `http.Protocols` suffices.
  `connect.WithGRPC()` is deliberately not used.
- **`--input k=v` typing**: the value is parsed as JSON when it is valid JSON
  (`3`, `true`, `[1,2]`, `{"a":1}` keep their types) and otherwise preserved
  verbatim as a string (so `url=https://...` stays a string). `--input-json`
  takes a whole JSON object; explicit `--input` pairs win over it. Documented in
  `orkctl run submit --help`.
- **`--timeout` and streams**: the timeout bounds each unary request. For
  `run watch` / `run submit --watch` it bounds the stream **only when the user
  set `--timeout` explicitly**, so the 30s default cannot kill a legitimate long
  run.
- **Final status comes from `GetRun`, never from observed events.** The server
  ends a stream with zero messages when `from_sequence` is already at or past
  the terminal event (`internal/api/service.go` + `internal/engine/bus.go`), so
  inferring the exit code from events alone would be wrong.
- **`--json` encoding**: `protojson` (the only correct encoder for these
  messages) re-indented through `encoding/json`, which also removes protojson's
  deliberately unstable whitespace and keeps the JSON sample stable.
  `run watch --json` emits one document, `{"events":[...],"run":{...}}`, written
  incrementally so it is both live and parseable as a whole; the document is
  closed even on stream failure.
- **`orkctl version --json`** is not a proto message, so it uses
  `encoding/json` over a small struct.
- `Execute` closes idle connections when a command ends. Beyond socket hygiene
  this cut the CLI test suite from ~30s to ~1.9s, because `http.Server.Shutdown`
  had been waiting on the CLI's kept-alive h2c connection in every fixture.

## Deviations from the research artifact

- Research suggested a hermetic unit lane with the fake starter and a Temporal
  lane for stub workers. Followed exactly: the stub `ExecuteTask` worker exists
  only in the `integration`-tagged file.
- Research flagged the branch name as unverified. Confirmed this session:
  `git branch --show-current` prints `m4-cli`.
- Reproduce script placed at `proof/M4/reproduce.sh` (precedent:
  `proof/M1/reproduce.sh`), not `scripts/`, because `scripts/` is outside the
  criterion-10 scope list.

## Observed facts worth carrying forward

- `PluginService.ListPlugins` returns an empty list against a real `orkengine`
  (the binary never sets `api.Config.Plugins`), so the proof transcript shows
  `No plugins.` The CLI is proven against a populated registry in the in-process
  lane instead.
- `Apply` never returns warnings today; the CLI renders them when present but no
  test can assert any.
- The `orkengine` stores are in-memory, so the integration test and the proof
  script apply before submitting inside one engine lifetime.

## Validation outcomes (this session)

- `make lint` → exit 0 (`go vet ./...` clean, `gofmt -l` empty).
- `go test ./... -count=1` → all packages ok, including
  `github.com/orkstack/orkstack/cmd/orkctl`.
- `go test -tags=integration ./cmd/orkctl/ -count=1 -v -run Integration` →
  `--- PASS: TestIntegration_CLIDrivesARealEngineToTerminalRuns (4.72s)` against
  Temporal on `localhost:7233`, with the real `orkengine` and `orkctl` binaries.

## QA E2E Video

Not applicable: M4 delivers a terminal CLI with no user-visible web UI, and the
scope lock forbids touching `ui/`. The equivalent evidence is the recorded
terminal session `proof/M4/cli-session.txt` (apply -> submit --watch -> live
events -> run get, real command output and exit codes) produced by
`proof/M4/reproduce.sh`.

## Contract amendments received

None. No mid-run user message amended the objective or acceptance criteria.
