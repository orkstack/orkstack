package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// A CI proof is many parts, each on its own host. `proofrun merge` is the only
// verdict path for the full, release and nightly tiers: it checks every part
// against the plan, merges the ledgers into one, and writes the proof.json and
// README the rest of the tooling reads (plan §2.6). A local proof is one part,
// `local`, so it runs the same code.

// runnerRecord is the host a part ran on.
type runnerRecord struct {
	OS    string `json:"os"`
	CPUs  int    `json:"cpus"`
	MemMB int    `json:"mem_mb"`
}

// partRecord is one part's result. It holds the identity fields proof.json
// carries, plus the part's own gates, ledgers and runner.
type partRecord struct {
	Part          string                  `json:"part"`
	Attempt       int                     `json:"attempt"`
	PlanSHA256    string                  `json:"plan_sha256"`
	UIPlanSHA256  string                  `json:"ui_plan_sha256,omitempty"`
	Runner        runnerRecord            `json:"runner"`
	Tier          string                  `json:"tier"`
	Name          string                  `json:"name"`
	Commit        string                  `json:"commit"`
	Branch        string                  `json:"branch,omitempty"`
	Tree          string                  `json:"tree"`
	TreeEnd       string                  `json:"tree_end"`
	CleanStart    bool                    `json:"clean_start"`
	CleanEnd      bool                    `json:"clean_end"`
	BaseRef       string                  `json:"base_ref,omitempty"`
	BaseCommit    string                  `json:"base_commit,omitempty"`
	MergeBase     string                  `json:"merge_base,omitempty"`
	Gates         []gateRecord            `json:"gates"`
	Temporal      temporalRecord          `json:"temporal"`
	Images        map[string]string       `json:"image_digests,omitempty"`
	ImageTag      string                  `json:"image_tag,omitempty"`
	ImageTagOwn   bool                    `json:"image_tag_own"`
	ProofJobs     int                     `json:"proof_jobs,omitempty"`
	UIWorkers     int                     `json:"proof_ui_workers,omitempty"`
	GatesOverride string                  `json:"proof_gates_override,omitempty"`
	GuardRetries  *int                    `json:"network_change_retries,omitempty"`
	GuardCeiling  *int                    `json:"network_change_retry_ceiling,omitempty"`
	Binaries      map[string]binaryRecord `json:"binaries,omitempty"`
	// Ledgers names the files this part wrote, relative to its directory.
	Ledgers map[string]string `json:"ledgers,omitempty"`
	// Setup is the runner's setup log (.github/actions/proof-fetch): one line
	// per attempt of every fetch made before the part, so a download or pull
	// that needed its second attempt is on the record (record -setup-log).
	Setup []string `json:"setup,omitempty"`
	// Health is the part's runner-health.json (record -runner-health): what
	// its runner measured before any gate, against the floors of its class.
	Health *runnerHealth `json:"runner_health,omitempty"`
}

type temporalRecord struct {
	Mode    string `json:"mode"`
	Image   string `json:"image,omitempty"`
	Address string `json:"address,omitempty"`
}

// rerunRecord is one part attempt that GitHub said was lost, whose runner
// measured below the floor, or whose job failed in a fetch or install step
// before its proof step started, and the attempt that ran after it. They are
// the only reruns a merge-eligible proof may hold; Reason is `lost-runner`,
// `runner-degraded` or `setup-failed`. LostAttempt describes the earlier
// attempt whichever of the three it was (its conclusion, annotations and
// failed step).
type rerunRecord struct {
	Part            string            `json:"part"`
	Attempt         int               `json:"attempt"`
	EarlierAttempts []int             `json:"earlier_attempts"`
	Reason          string            `json:"reason"`
	LostAttempt     lostAttemptRecord `json:"lost_attempt"`
	// Degraded is what the earlier attempt's runner measured below the
	// floor, for a `runner-degraded` rerun.
	Degraded []degradedRunner `json:"degraded,omitempty"`
}

// setupFailure is one part attempt whose job failed in a fetch or install
// step before its proof step started (setupStepNames). proof.json lists every
// such attempt under setup_failures, whether or not the part ran again.
type setupFailure struct {
	Part       string `json:"part"`
	Attempt    int    `json:"attempt"`
	Step       string `json:"step"`
	Conclusion string `json:"conclusion"`
}

// degradedRunner is one metric one part attempt measured on the wrong side
// of its floor (runnerhealth.go). proof.json lists every such attempt under
// degraded_runners, whether or not the part ran again.
type degradedRunner struct {
	Part    string  `json:"part"`
	Attempt int     `json:"attempt"`
	Class   string  `json:"class"`
	Metric  string  `json:"metric"`
	Unit    string  `json:"unit"`
	Value   float64 `json:"value"`
	Floor   float64 `json:"floor"`
}

// partHealth is one part attempt's runner-health record as proof.json
// carries it: the verdict re-derived by the merger, never the file's own.
type partHealth struct {
	Part    string         `json:"part"`
	Attempt int            `json:"attempt"`
	Class   string         `json:"class"`
	Verdict string         `json:"verdict"`
	Seconds float64        `json:"seconds"`
	Metrics []healthMetric `json:"metrics"`
}

type lostAttemptRecord struct {
	Conclusion       string   `json:"conclusion,omitempty"`
	Annotation       string   `json:"annotation,omitempty"`
	FailedStep       string   `json:"failed_step,omitempty"`
	PassedBeforeLoss int      `json:"passed_before_loss"`
	Unfinished       []string `json:"unfinished"`
}

type planSummary struct {
	SHA256      string        `json:"sha256"`
	Shards      int           `json:"shards"`
	Jobs        int           `json:"jobs"`
	CostsSource string        `json:"costs_source"`
	Unmeasured  int           `json:"unmeasured"`
	NightlyMode string        `json:"nightly_mode"`
	NightlySHA  string        `json:"nightly_sha256"`
	Deferred    []deferredRef `json:"deferred"`
}

type uiPlanSummary struct {
	SHA256 string   `json:"sha256"`
	Shards int      `json:"shards"`
	Files  []string `json:"files"`
}

type quarantineRecord struct {
	CheckedOn  string `json:"checked_on"`
	BaseSHA256 string `json:"base_sha256"`
}

type nightlyRecord struct {
	SHA256     string `json:"sha256"`
	BaseCommit string `json:"base_commit"`
	BaseSHA256 string `json:"base_sha256"`
}

type hostSpeedFailure struct {
	Part          string  `json:"part"`
	Test          string  `json:"test,omitempty"`
	Kind          string  `json:"kind,omitempty"`
	Assertion     string  `json:"assertion,omitempty"`
	Bound         float64 `json:"bound,omitempty"`
	Observed      float64 `json:"observed,omitempty"`
	RunnerCPUs    int     `json:"runner_cpus"`
	ReferenceCPUs int     `json:"reference_cpus"`
	BudgetMinutes float64 `json:"budget_minutes,omitempty"`
	ElapsedMin    float64 `json:"elapsed_minutes,omitempty"`
	Message       string  `json:"message"`
	// Excused says the miss is a known gap that does not fail the proof: a
	// UI bound's own timing miss on a CI part (hostspeed.go). Every other
	// entry is recorded and still fails it (rule 13).
	Excused bool `json:"excused"`
	// Record is, for an excused Go test, the timing record the test left (its
	// checks, their bounds, and whether it failed anything else): the proof
	// artifact does not keep a part's artifacts, and the excusal rests on it.
	Record *timingRecord `json:"timing_record,omitempty"`
}

type ciRecord struct {
	RunID   string `json:"run_id,omitempty"`
	Attempt int    `json:"run_attempt,omitempty"`
	Event   string `json:"event,omitempty"`
	SHA     string `json:"sha,omitempty"`
	BaseSHA string `json:"base_sha,omitempty"`
	URL     string `json:"url,omitempty"`
}

// githubAttempt is the record of one run attempt's jobs, as the proof job
// downloads it into -jobs: one attempt-N.json naming every part job.
type githubAttempt struct {
	Attempt int         `json:"attempt"`
	Jobs    []githubJob `json:"jobs"`
}

type githubJob struct {
	Name        string       `json:"name"`
	Part        string       `json:"part,omitempty"`
	Conclusion  string       `json:"conclusion,omitempty"`
	CheckRunID  int64        `json:"check_run_id,omitempty"`
	Annotations []string     `json:"annotations,omitempty"`
	Log         string       `json:"log,omitempty"`
	Steps       []githubStep `json:"steps,omitempty"`
	FailedStep  string       `json:"failed_step,omitempty"`
	RunnerLost  bool         `json:"runner_lost,omitempty"`
	// ReadError says that the job's record, annotations or log could not be
	// read: such an attempt is a result (rule 2, row 1: it fails closed).
	ReadError string `json:"read_error,omitempty"`
}

type githubStep struct {
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
}

var runnerLostMarker = regexp.MustCompile(`(?i)(the runner has received a shutdown signal|lost communication with the server)`)
var timeLimitMarker = regexp.MustCompile(`(?i)(has exceeded the maximum execution time|the job running on runner .* has exceeded)`)

// proofrun's progress lines, as a GitHub log carries them: each log line is
// prefixed with a timestamp, so they are matched anywhere in the line.
var (
	progressStart   = regexp.MustCompile(`proofrun: start (\S+) (\S+)`)
	progressOutcome = regexp.MustCompile(`proofrun: outcome (\S+) (\S+) (\S+)`)
)

// setupAttemptRE matches one attempt line of a part's setup log, as
// .github/actions/proof-fetch writes it: `fetch <what>: attempt <n> <how>`.
// The what names the artifact, pattern or image; the how is `ok in Ns`,
// `timed out after Ns` or `failed, exit E after Ns`, each with its limit.
var setupAttemptRE = regexp.MustCompile(`^fetch (.+): attempt ([0-9]+) `)

// setupRetries lists, prefixed with the part, every line of each fetch in a
// part's setup log that needed more than one attempt (or gave up), in the
// log's order; a fetch that succeeded first time is left out. The lines are
// the action's own, so the README and proof.json say what the runner saw.
func setupRetries(part string, setup []string) []string {
	attempts := map[string]int{}
	var order []string
	for _, l := range setup {
		m := setupAttemptRE.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		if _, seen := attempts[m[1]]; !seen {
			order = append(order, m[1])
		}
		if n, _ := strconv.Atoi(m[2]); n > attempts[m[1]] {
			attempts[m[1]] = n
		}
	}
	var out []string
	for _, what := range order {
		if attempts[what] < 2 {
			continue
		}
		for _, l := range setup {
			if strings.HasPrefix(l, "fetch "+what+": ") {
				out = append(out, part+": "+l)
			}
		}
	}
	return out
}

// proofStepNames are the names of the step that runs `scripts/proof.sh
// --part` in each part job of .github/workflows/proof.yml, and exactly those:
// TestWorkflows_TheMergersProofStepNamesAreTheWorkflowsExactly holds the map
// to the workflow in both directions.
var proofStepNames = map[string]bool{
	"plan": true, "build": true, "fast": true, "race": true, "race-repeat": true, "fuzz": true,
	"integration shard": true, "UI shard": true, "activity fixture": true, "observability": true,
}

// setupStepNames lists, for each kind of part job in
// .github/workflows/proof.yml, the steps before its proof step whose failure
// is a setup failure: a download, a pull or an install from outside the run,
// which a new attempt of the job may well get through. The names are the
// steps' own, because the jobs API reports a composite action only as the
// step that uses it. A job that failed in one of them, with its proof step
// never started, left no result and ran no test (classify, attemptSetupFailed).
//
// The plan and the build have no entry: every other part reads what they
// make, so a setup failure there is not answered by running the job again.
// TestWorkflows_TheSetupStepsAreTheWorkflowsExactly holds this table to
// proof.yml in both directions.
var setupStepNames = map[string]map[string]bool{
	"integration":      {"proof host": true, "fetch plan": true, "fetch build": true},
	"ui-e2e":           {"bun install": true, "proof host": true, "fetch plan": true, "fetch build": true, "fetch browser image": true, "Chromium (host)": true},
	"activity-fixture": {"proof host": true, "fetch plan": true, "fetch build": true},
	"observability":    {"observability stack": true, "fetch plan": true, "fetch build": true},
	"fast":             {"bun install": true, "bun install (site)": true},
	// The race suites fetch and install nothing before their proof step.
	"race":        {},
	"race-repeat": {},
	"fuzz":        {},
}

// partKind is the kind of part a part name is: the name before its colon
// (`integration:2` is an integration part), or the name itself.
func partKind(part string) string {
	kind, _, _ := strings.Cut(part, ":")
	return kind
}

// partName is the part a job record belongs to, from its own field or its
// GitHub job name; "" when it names none.
func (j *githubJob) partName() string {
	if j.Part != "" {
		return j.Part
	}
	return partFromJobName(j.Name)
}

// isSetupStep says step is a fetch or install step of the job part runs in,
// so that its failure before the proof step is a setup failure.
func isSetupStep(part, step string) bool {
	return step != "" && setupStepNames[partKind(part)][step]
}

// cmdMerge is `proofrun merge`: the one verdict path for a full, release or
// nightly proof, whether it ran locally or as many CI parts.
func cmdMerge(args []string) int {
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)
	planFile := fs.String("plan", "", "the integration plan (required)")
	uiPlanFile := fs.String("ui-plan", "", "the UI plan, when the tier runs Playwright")
	partsDir := fs.String("parts", "", "directory holding one subdirectory per part attempt (required)")
	jobsDir := fs.String("jobs", "", "GitHub's record of every attempt's jobs")
	hostSpeedFile := fs.String("host-speed", "scripts/proof-host-speed.tsv", "assertions bounded by host speed")
	runnerFloors := fs.String("runner-floors", runnerFloorsFile, "the tree's runner-health floors: every part must have measured against this table")
	nightlySpecs := fs.String("nightly-specs", nightlySpecFile, "the tree's nightly spec list: a nightly proof must run every spec file it lists")
	out := fs.String("out", "", "the proof directory to write (required)")
	name := fs.String("name", "", "the proof's name")
	ciRunID := fs.String("ci-run-id", "", "the CI run id")
	ciAttempt := fs.Int("ci-attempt", 0, "the CI run attempt")
	ciEvent := fs.String("ci-event", "", "the CI event")
	ciURL := fs.String("ci-url", "", "the CI run URL")
	stopped := fs.Bool("stopped", false, "a local proof stopped at a failing gate before it wrote -plan: judge it a failure from its local part alone")
	if err := fs.Parse(args); err != nil || *planFile == "" || *partsDir == "" || *out == "" || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "proofrun merge -plan FILE -parts DIR -out DIR [-ui-plan FILE] [-jobs DIR] -name NAME")
		return 2
	}
	if _, statErr := os.Stat(*planFile); *stopped && os.IsNotExist(statErr) {
		return mergeStoppedProof(*partsDir, *out, *name)
	}
	plan, err := readPlan(*planFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	var uiP *uiPlan
	if *uiPlanFile != "" {
		uiP = &uiPlan{}
		if err := readJSON(*uiPlanFile, uiP); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun merge:", err)
			return 2
		}
	}
	planSHA, err := fieldSHA256(*planFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge: -plan:", err)
		return 2
	}
	uiPlanSHA := ""
	if *uiPlanFile != "" {
		if uiPlanSHA, err = fieldSHA256(*uiPlanFile); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun merge: -ui-plan:", err)
			return 2
		}
	}
	parts, absent, decodeErr := readParts(*partsDir)
	if decodeErr != nil {
		// Two directories claiming one attempt is a proof that cannot be
		// merged, not an invocation error: record why, so the verdict step
		// reads a failing proof.json and not an absent one.
		fmt.Fprintln(os.Stderr, "proofrun merge:", decodeErr)
		failed := proofRecord{
			Tier: plan.Tier, Name: *name, Commit: plan.Commit, Result: "fail",
			Ineligible: []string{"the parts cannot be merged: " + decodeErr.Error()},
			Gates:      []gateRecord{},
		}
		if err := os.MkdirAll(*out, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun merge:", err)
			return 2
		}
		if err := writeJSON(filepath.Join(*out, "proof.json"), failed); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun merge:", err)
			return 2
		}
		return 1
	}
	attempts, unreadable := readGithubAttempts(*jobsDir)
	hostSpeed, err := readHostSpeed(*hostSpeedFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	m := &merger{
		plan: plan, planPath: *planFile, uiPlan: uiP, uiPlanPath: *uiPlanFile, planSHA: planSHA, uiPlanSHA: uiPlanSHA,
		nightlySpecsFile: *nightlySpecs,
		parts:            parts, absent: absent,
		attempts: attempts, hostSpeed: hostSpeed, hostSpeedFile: *hostSpeedFile,
		runnerFloorsFile: *runnerFloors,
		excusedUIGate:    map[string]bool{},
		excusedGoGate:    map[string]bool{},
	}
	for _, f := range unreadable {
		m.notes = append(m.notes, "GitHub's job record "+f+" is unreadable: every attempt it would have described is classified as a result (it fails closed)")
	}
	m.merge(*name)
	proof := m.proof
	proof.CI = &ciRecord{RunID: *ciRunID, Attempt: *ciAttempt, Event: *ciEvent, SHA: proof.Commit, BaseSHA: plan.BaseCommit, URL: *ciURL}
	if *ciRunID == "" {
		proof.CI = nil
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	if err := writeJSON(filepath.Join(*out, "proof.json"), proof); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	for name, v := range map[string]any{
		"unit-tests.json": m.unit, "integration-tests.json": m.integration, "ui-e2e-tests.json": m.ui,
		suiteRace + "-tests.json": m.race, suiteRaceRepeat + "-tests.json": m.raceRepeat, suiteFuzz + "-tests.json": m.fuzz,
	} {
		if isNilLedger(v) {
			continue
		}
		if err := writeJSON(filepath.Join(*out, name), v); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun merge:", err)
			return 2
		}
	}
	if err := writeMergeREADME(filepath.Join(*out, "README.md"), proof, m); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	if proof.Result != "pass" {
		return 1
	}
	return 0
}

// mergeStoppedProof is the verdict of a local proof that stopped at a failing
// gate before `proofrun integration` planned anything: there is no plan to
// merge against, and no result could make it pass. It writes the failing
// proof.json and README from the local part alone — the gates that failed,
// the ones stopped, and the ones never started — so a stopped proof leaves
// the same evidence a finished failing one does. It never writes a pass and
// never a merge-eligible proof.
func mergeStoppedProof(partsDir, out, name string) int {
	parts, _, err := readParts(partsDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	m := &merger{parts: parts, excusedUIGate: map[string]bool{}, excusedGoGate: map[string]bool{}}
	proof := &proofRecord{Name: name, Gates: []gateRecord{}, Ledgers: map[string]any{}}
	m.proof = proof
	reasons := []string{"the proof stopped at a failing gate before its integration plan was written, so no test of the integration or UI suites ran"}
	if readings := m.resultReadings("local"); len(readings) == 1 {
		rec := readings[0].Record
		proof.Tier, proof.Commit, proof.Branch = rec.Tier, rec.Commit, rec.Branch
		proof.Tree, proof.TreeEnd, proof.CleanStart, proof.CleanEnd = rec.Tree, rec.TreeEnd, rec.CleanStart, rec.CleanEnd
		proof.BaseRef, proof.BaseCommit, proof.MergeBase = rec.BaseRef, rec.BaseCommit, rec.MergeBase
		proof.Gates = rec.Gates
		proof.Temporal.Mode, proof.Temporal.Image, proof.Temporal.Address = rec.Temporal.Mode, rec.Temporal.Image, rec.Temporal.Address
		proof.ProofJobs, proof.UIWorkers, proof.GatesOverride = rec.ProofJobs, rec.UIWorkers, rec.GatesOverride
		proof.ImageTag, proof.ImageTagOwn = rec.ImageTag, rec.ImageTagOwn
		for _, g := range rec.Gates {
			switch g.Result {
			case "pass":
			case gateStopped, gateNotStarted:
				reasons = append(reasons, "gate "+g.Gate+" was "+strings.ReplaceAll(g.Result, "-", " ")+": "+orDash(g.Command))
			default:
				reasons = append(reasons, "gate "+g.Gate+" failed")
			}
		}
		proof.Parts = []string{"local"}
	} else {
		reasons = append(reasons, "the proof left no readable local part, so none of its gates is known")
	}
	proof.Ineligible = reasons
	verdict(proof, 1, false, nil)
	if err := os.MkdirAll(out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	if err := writeJSON(filepath.Join(out, "proof.json"), proof); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	if err := writeMergeREADME(filepath.Join(out, "README.md"), proof, m); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	return 1
}

func isNilLedger(v any) bool {
	switch l := v.(type) {
	case *ledger:
		return l == nil
	case *uiLedger:
		return l == nil
	}
	return v == nil
}

// merger accumulates the merged proof.
type merger struct {
	plan             *integrationPlan
	planPath         string
	uiPlan           *uiPlan
	uiPlanPath       string
	nightlySpecsFile string
	// nightlySpecRows are the tree's nightly spec list's rows, read by the
	// nightly tier's merge itself (checkNightlyUI).
	nightlySpecRows   []nightlySpecEntry
	planSHA           string
	uiPlanSHA         string
	parts             map[string]map[int]partReading
	absent            map[string]bool
	attempts          map[string]map[int]githubJob
	hostSpeed         hostSpeedTable
	hostSpeedFile     string
	hostSpeedExcusing hostSpeedTable
	runnerFloorsFile  string
	problems          []string
	ineligible        []string
	reruns            []rerunRecord
	degraded          []degradedRunner
	setupFailures     []setupFailure
	health            []partHealth
	proof             *proofRecord
	unit              *ledger
	integration       *ledger
	race              *ledger
	raceRepeat        *ledger
	fuzz              *ledger
	ui                *uiLedger
	notes             []string
	hostSpeedFailures []hostSpeedFailure
	knownGaps         []string
	excusedUIGate     map[string]bool
	excusedGoGate     map[string]bool
	digestNotes       []string
	warnings          []string
	// allow is the allowed-skips list, parsed from the file whose hash is the
	// plan's; empty when that file is not the plan's.
	allow []allowedSkip
}

// partReading is one part attempt's on-disk result.
type partReading struct {
	Part    string
	Attempt int
	Dir     string
	Record  *partRecord
	// Failed and Unfinished are what the attempt's events show: a test that
	// failed, and a test that started and never finished.
	Failed, Unfinished bool
	// Corrupt says the attempt left a part.json that cannot be parsed, or
	// events that cannot be read: it is a result, never "no failure seen".
	Corrupt bool
	// Health is the attempt's runner-health.json, or the copy its part.json
	// carries; nil for a part measured before the step existed, which keeps
	// its meaning. HealthCorrupt says the file exists and cannot be read.
	Health        *runnerHealth
	HealthCorrupt bool
}

// degradedMetrics is what the attempt's runner measured on the wrong side of
// its floor, re-derived from the record's values and floors. The floor binds
// only the CI stack parts (scripts/proof-part.sh part_floor_binds): a local
// proof's health, and the plan, build and fast parts', is information.
func (r partReading) degradedMetrics() []healthMetric {
	if !floorBinds(r.Part) {
		return nil
	}
	return r.Health.below()
}

// floorBinds says the runner-health floor decides the part's classification.
func floorBinds(part string) bool {
	return (part != "local" && isStackPart(part)) || strings.HasPrefix(part, "activity-fixture:")
}

// readParts scans -parts. A directory with a part.json is a completed attempt;
// a directory with only events is an attempt whose process died. An absent
// part is decided from the jobs record.
func readParts(dir string) (map[string]map[int]partReading, map[string]bool, error) {
	out := map[string]map[int]partReading{}
	absent := map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return out, absent, nil
		}
		return nil, nil, err
	}
	dirRE := regexp.MustCompile(`^(.+)-a([0-9]+)$`)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		p := filepath.Join(dir, e.Name())
		var rec partRecord
		has := readJSON(filepath.Join(p, "part.json"), &rec) == nil
		_, statErr := os.Stat(filepath.Join(p, "part.json"))
		corrupt := !has && statErr == nil
		part, attempt := "", 0
		if has {
			part, attempt = rec.Part, rec.Attempt
		} else if mm := dirRE.FindStringSubmatch(e.Name()); mm != nil {
			part = normalizePartName(mm[1])
			attempt, _ = strconv.Atoi(mm[2])
		}
		if part == "" || attempt == 0 {
			continue
		}
		if out[part] == nil {
			out[part] = map[int]partReading{}
		}
		if prev, dup := out[part][attempt]; dup {
			return nil, nil, fmt.Errorf("%s and %s both claim part %s attempt %d", filepath.Base(prev.Dir), e.Name(), part, attempt)
		}
		reading := partReading{Part: part, Attempt: attempt, Dir: p, Corrupt: corrupt}
		var evErr error
		reading.Failed, reading.Unfinished, evErr = attemptEvents(p)
		if evErr != nil {
			reading.Corrupt = true
		}
		if has {
			reading.Record = &rec
		}
		// The file first: an attempt that died before its part.json still
		// measured its runner. The record's copy stands in for a part whose
		// file was not kept beside it (a local proof's part directory).
		if h, err := readRunnerHealth(p); err != nil {
			reading.HealthCorrupt = true
		} else if h != nil {
			reading.Health = h
		} else if has && rec.Health != nil {
			reading.Health = rec.Health
		}
		out[part][attempt] = reading
	}
	return out, absent, nil
}

func normalizePartName(s string) string {
	s = strings.TrimPrefix(s, "part-")
	for _, prefix := range []string{"integration-", "ui-e2e-", "activity-fixture-"} {
		if strings.HasPrefix(s, prefix) {
			return strings.TrimSuffix(prefix, "-") + ":" + strings.TrimPrefix(s, prefix)
		}
	}
	return s
}

// attemptEvents reports whether an attempt's events show a test that failed,
// and whether one started and never finished (plan §2.6 rule 2).
func attemptEvents(dir string) (failed, unfinished bool, err error) {
	f, err := os.Open(filepath.Join(dir, "integration.events.jsonl"))
	if os.IsNotExist(err) {
		return false, false, nil
	}
	if err != nil {
		return true, false, err
	}
	defer f.Close()
	started := map[string]bool{}
	finished := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		var ev event
		if json.Unmarshal(sc.Bytes(), &ev) != nil || ev.Test == "" {
			continue
		}
		k := ev.Package + "\x00" + ev.Test
		switch ev.Action {
		case "fail":
			failed = true
			finished[k] = true
		case "run":
			started[k] = true
		case "pass", "skip":
			finished[k] = true
		}
	}
	for k := range started {
		if !finished[k] {
			unfinished = true
		}
	}
	if err := sc.Err(); err != nil {
		return true, unfinished, err
	}
	return failed, unfinished, nil
}

// readGithubAttempts reads -jobs: one JSON file per run attempt. A file that
// cannot be read is named in unreadable; the attempts it would have described
// then have no record, which classify treats as a result.
func readGithubAttempts(dir string) (map[string]map[int]githubJob, []string) {
	out := map[string]map[int]githubJob{}
	var unreadable []string
	if dir == "" {
		return out, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return out, []string{dir + " (" + err.Error() + ")"}
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var a githubAttempt
		if err := readJSON(filepath.Join(dir, e.Name()), &a); err != nil {
			unreadable = append(unreadable, e.Name())
			continue
		}
		for _, j := range a.Jobs {
			part := j.Part
			if part == "" {
				part = partFromJobName(j.Name)
			}
			if part == "" {
				continue
			}
			if out[part] == nil {
				out[part] = map[int]githubJob{}
			}
			out[part][a.Attempt] = j
		}
	}
	return out, unreadable
}

var jobPartRE = regexp.MustCompile(`(?i)^(integration|ui-e2e|activity-fixture)\s*\(([^)]+)\)$`)

// reusableWorkflowPrefix is what GitHub puts before a job of a reusable
// workflow: "gate / integration (3)".
var reusableWorkflowPrefix = regexp.MustCompile(`^.* / `)

func partFromJobName(name string) string {
	name = reusableWorkflowPrefix.ReplaceAllString(strings.TrimSpace(name), "")
	if mm := jobPartRE.FindStringSubmatch(strings.TrimSpace(name)); mm != nil {
		return strings.ToLower(mm[1]) + ":" + mm[2]
	}
	switch strings.TrimSpace(name) {
	case "plan", "build", "fast", "observability", suiteRace, suiteRaceRepeat, suiteFuzz:
		return strings.TrimSpace(name)
	}
	return ""
}

// attemptClass is what rule 2 makes of one part attempt.
type attemptClass int

const (
	// attemptResult: the attempt recorded a result; a later attempt makes
	// the proof ineligible.
	attemptResult attemptClass = iota
	// attemptLost: GitHub's own annotation says it lost the runner; it may
	// run again.
	attemptLost
	// attemptDegraded: the attempt's own runner-health.json measured its
	// runner below the floor (AGENTS.md §11.2); it may run again, exactly as
	// a lost one, and nothing it recorded is an outcome of the tree.
	attemptDegraded
	// attemptSetupFailed: the job failed in a fetch or install step
	// (setupStepNames) before its proof step started, so it left no result and
	// ran no test; it may run again, exactly as a lost or a degraded one.
	attemptSetupFailed
	// attemptNotEligible: no result, and not one of the above (a setup failure
	// in a step that is not a fetch or an install, one in the plan or the
	// build, a cancel before any test started): a later attempt makes the
	// proof ineligible, and the remedy is a new run.
	attemptNotEligible
)

// classify is plan §2.6 rule 2. An attempt whose runner-health.json measured
// its runner below the floor is degraded, whatever else it left: a result
// from a degraded runner is not a result of the tree. An attempt that left a
// part.json, or whose events record a failing test, produced a result. Any
// other attempt is classified from GitHub's record of its job, the rows
// tried in order:
//  1. no record, or a record that could not be read: a result (fails closed);
//  2. the log has a `proofrun: outcome fail` line: a result;
//  3. GitHub's runner-lost annotation: a lost runner;
//  4. cancelled or out of time with a started test that has no outcome: a
//     result (a hung or failing test);
//  5. a failed job whose proof step never started, in a fetch or install step
//     of its kind of part (setupStepNames): a setup failure, which ran no
//     test and may run again;
//  6. anything else: not rerun-eligible — after the proof step started a
//     result that never reached its upload, before it a setup failure
//     elsewhere (any step of the plan or the build, a checkout, a toolchain)
//     or an early cancel.
//
// The planner of automatic reruns (rerunplan.go) classifies the same way, so
// what it reruns is what the merger would call rerun-eligible.
func classify(j *githubJob, reading partReading) (attemptClass, string) {
	if reading.HealthCorrupt {
		return attemptResult, "its runner-health.json cannot be read: it fails closed"
	}
	if below := reading.degradedMetrics(); len(below) > 0 {
		return attemptDegraded, "its runner measured below the floor: " + describeBelow(below)
	}
	if reading.Record != nil {
		return attemptResult, "it left its part.json"
	}
	if reading.Corrupt {
		return attemptResult, "its part.json or its events cannot be read: it fails closed"
	}
	if reading.Failed {
		return attemptResult, "its events record a failing test"
	}
	if j == nil {
		return attemptResult, "GitHub's record of it is missing: it fails closed"
	}
	if j.ReadError != "" {
		return attemptResult, "GitHub's record of it could not be read (" + j.ReadError + "): it fails closed"
	}
	failed, unfinished := logProgress(j.Log)
	if len(failed) > 0 {
		return attemptResult, "its log records a failing test: " + failed[0]
	}
	annotation := strings.TrimSpace(strings.Join(j.Annotations, "\n"))
	if j.RunnerLost || runnerLostMarker.MatchString(annotation) {
		return attemptLost, orDash(annotation)
	}
	stopped := j.Conclusion == "cancelled" || timeLimitMarker.MatchString(annotation)
	if stopped && len(unfinished) > 0 {
		return attemptResult, "a test was running when the job stopped: " + unfinished[0]
	}
	if reading.Unfinished {
		return attemptResult, "its events show a test that started and never finished"
	}
	if j.Conclusion == "cancelled" {
		return attemptNotEligible, "it was cancelled before any test started"
	}
	switch proofStepState(j) {
	case "started":
		return attemptResult, "it ended " + orDash(j.Conclusion) + " after its proof step started, and its result never reached its upload"
	case "not-started":
		if j.Conclusion == "failure" && isSetupStep(j.partName(), j.FailedStep) {
			return attemptSetupFailed, fmt.Sprintf("its step %q failed before its proof step started", j.FailedStep)
		}
		return attemptNotEligible, "a setup failure before its proof step (" + orDash(j.FailedStep) + "), which is not a fetch or install step this part may run again after"
	}
	return attemptResult, "GitHub's record names none of its steps: it fails closed"
}

// proofStepState tells from a job's steps whether its proof step started:
// "started", "not-started" (the step is absent, skipped or never left the
// queue while other steps are recorded), or "" when the record holds no step.
func proofStepState(j *githubJob) string {
	if proofStepNames[j.FailedStep] {
		return "started"
	}
	if len(j.Steps) == 0 {
		if j.FailedStep != "" {
			return "not-started"
		}
		return ""
	}
	for _, st := range j.Steps {
		if !proofStepNames[st.Name] {
			continue
		}
		switch st.Conclusion {
		case "", "skipped", "queued", "pending":
			return "not-started"
		}
		return "started"
	}
	return "not-started"
}

// logProgress reads a job log's proofrun lines: the tests whose outcome was
// fail, and the tests that started and have no outcome.
func logProgress(log string) (failed, unfinished []string) {
	started := map[string]bool{}
	var order []string
	finished := map[string]bool{}
	for _, line := range strings.Split(log, "\n") {
		if m := progressOutcome.FindStringSubmatch(line); m != nil {
			k := m[2] + "." + m[3]
			finished[k] = true
			if m[1] == outcomeFail {
				failed = append(failed, k)
			}
			continue
		}
		if m := progressStart.FindStringSubmatch(line); m != nil {
			k := m[1] + "." + m[2]
			if !started[k] {
				started[k] = true
				order = append(order, k)
			}
		}
	}
	for _, k := range order {
		if !finished[k] {
			unfinished = append(unfinished, k)
		}
	}
	return failed, unfinished
}

func (m *merger) merge(name string) {
	p := m.plan
	proof := &proofRecord{
		Tier: p.Tier, Name: name, Commit: p.Commit, Tree: p.Tree, TreeEnd: p.Tree,
		CleanStart: true, CleanEnd: true, BaseRef: mainBranch, BaseCommit: p.BaseCommit,
		MergeBase: p.MergeBase, Gates: []gateRecord{}, Ineligible: []string{},
	}
	m.proof = proof
	if proof.Name == "" {
		proof.Name = filepath.Base("proof")
	}
	m.checkPlanShape()
	expected := m.expectedParts()
	m.dropUnexpectedParts(expected)
	// Every expected part has its last attempt, and only a lost runner may
	// have been run again (rule 1 and rule 2).
	for _, part := range expected {
		m.mergePart(part)
	}
	buildImages := m.buildImages()
	if len(buildImages) == 0 && containsString(expected, "build") {
		m.ineligible = append(m.ineligible, "the build part recorded no activity image digests: the parts' digests cannot be compared with it")
	}
	m.collectHealth()
	m.carryPartIdentity()
	m.checkAllowlist()
	m.checkNightlyAnchorFiles()
	m.checkHostSpeedAnchor()
	for _, part := range sortedPartKeys(m.parts) {
		got := m.parts[part]
		attempts := make([]int, 0, len(got))
		for a := range got {
			attempts = append(attempts, a)
		}
		sort.Ints(attempts)
		for _, attempt := range attempts {
			if reading := got[attempt]; reading.Record != nil {
				m.checkIdentity(part, attempt, reading.Record, buildImages)
			}
		}
	}
	m.mergeIntegration()
	m.mergeUnit()
	m.mergeLanes()
	if m.plan.Tier == tierNightly {
		m.checkNightlyUI()
	}
	m.mergeUI()
	if m.ui != nil {
		proof.GuardRetries, proof.GuardCeiling = &m.ui.GuardRetries, &m.ui.GuardRetryCeiling
	}
	m.mergeGates(expected)
	m.proof.Parts = append([]string{}, expected...)
	m.proof.Reruns = m.reruns
	m.proof.DegradedRunners = m.degraded
	m.proof.SetupFailures = m.setupFailures
	m.proof.RunnerHealth = m.health
	for _, part := range sortedPartKeys(m.parts) {
		for _, reading := range m.resultReadings(part) {
			if reading.Record != nil {
				proof.SetupRetries = append(proof.SetupRetries, setupRetries(part, reading.Record.Setup)...)
			}
		}
	}
	m.notes = append(m.notes, proof.SetupRetries...)
	m.proof.Plan = &planSummary{
		SHA256: m.planSHA, Shards: len(p.Shards), Jobs: len(p.Jobs),
		CostsSource: p.Costs.Source, Unmeasured: len(p.Unmeasured),
		NightlyMode: p.Nightly.Mode, NightlySHA: p.Nightly.SHA256, Deferred: p.Deferred,
	}
	if len(p.Shards) > 0 {
		proof.Plan.Shards = len(p.Shards)
	}
	if m.uiPlan != nil {
		files := make([]string, 0, len(m.uiPlan.Files))
		for _, f := range m.uiPlan.Files {
			files = append(files, f.File)
		}
		proof.UIPlan = &uiPlanSummary{SHA256: m.uiPlanSHA, Shards: len(m.uiPlan.Shards), Files: files}
	}
	proof.ListsChanged = p.ListsChanged
	for _, c := range p.ListsChanged {
		if c.Error != "" {
			// Not knowing which gate files the change edits is never "none":
			// a reviewer could not see an excusing list move (plan §6.4).
			m.ineligible = append(m.ineligible, "the gate files this change edits could not be listed: "+c.Error)
		}
	}
	// The plan's own problems (a stale nightly or quarantine entry, a selected
	// test no binary lists) fail the proof; a local part's ledger already
	// carries them, so each is counted once.
	for _, pp := range p.Problems {
		if !containsString(m.problems, "local: "+pp) {
			m.problems = append(m.problems, "plan: "+pp)
		}
	}
	proof.BinaryDigestNotes = m.digestNotes
	proof.PR = p.PR
	proof.HostSpeedFailures = m.hostSpeedFailures
	proof.KnownGaps = m.knownGaps
	proof.Quarantine = &quarantineRecord{BaseSHA256: p.Quarantine.BaseSHA256, CheckedOn: p.CheckedOn}
	if entries := m.checkQuarantine(); len(entries) > 0 {
		outcomes := map[testKey]string{}
		for _, l := range []*ledger{m.unit, m.integration} {
			if l == nil {
				continue
			}
			for _, e := range l.Tests {
				outcomes[testKey{e.Package, e.Test}] = e.Outcome
			}
		}
		for _, e := range entries {
			proof.Quarantined = append(proof.Quarantined, fmt.Sprintf("%s.%s: %s", e.Package, e.Test, orDash(outcomes[e.key()])))
		}
	}
	proof.QuarantineWarnings = m.warnings
	proof.Nightly = &nightlyRecord{SHA256: p.Nightly.SHA256, BaseCommit: p.Nightly.BaseCommit, BaseSHA256: p.Nightly.BaseSHA256}
	proof.Ledgers = map[string]any{}
	if m.unit != nil {
		proof.Ledgers.(map[string]any)["unit"] = map[string]any{"summary": m.unit.Summary, "problems": len(m.unit.Problems)}
	}
	if m.integration != nil {
		proof.Ledgers.(map[string]any)["integration"] = map[string]any{"summary": m.integration.Summary, "problems": len(m.integration.Problems)}
	}
	for suite, l := range m.laneLedgers() {
		proof.Ledgers.(map[string]any)[suite] = map[string]any{"summary": l.Summary, "problems": len(l.Problems)}
	}
	if m.ui != nil {
		proof.Ledgers.(map[string]any)["ui-e2e"] = map[string]any{"listed": m.ui.Listed, "pass": m.ui.Pass, "deferred": m.ui.Deferred, "problems": len(m.ui.Problems)}
	}
	ledgersOK := map[string]bool{}
	for suite, l := range m.laneLedgers() {
		ledgersOK[suite] = l.ok()
	}
	if m.unit != nil {
		ledgersOK["unit"] = m.unit.ok()
	}
	if m.integration != nil {
		ledgersOK["integration"] = m.integration.ok()
	}
	if m.ui != nil {
		ledgersOK["ui-e2e"] = len(m.ui.Problems) == 0
	}
	// A merge problem is a failing proof even when every gate passed.
	status := 0
	if len(m.problems) > 0 {
		status = 1
		proof.Ineligible = append(proof.Ineligible, m.problems...)
	}
	proof.Ineligible = append(proof.Ineligible, m.ineligible...)
	verdict(proof, status, p.ObservabilityRequired, ledgersOK)
}

// tierNightlyMode is the nightly mode each tier's plan must carry.
var tierNightlyMode = map[string]string{tierFull: nightlyDefer, tierRelease: nightlyInclude, tierNightly: nightlyOnly}

// checkPlanShape refuses a plan that is not the whole suite of its tier: a
// selection, -run or package patterns narrowed it, or its (tier, nightly
// mode) pair is not the one the tier runs, or it names no tree or image tag
// the parts could be compared with.
func (m *merger) checkPlanShape() {
	p := m.plan
	want, known := tierNightlyMode[p.Tier]
	switch {
	case !known:
		m.problems = append(m.problems, fmt.Sprintf("the plan's tier is %q: the merger judges only the full, nightly and release tiers", p.Tier))
	case p.Nightly.Mode != want:
		m.problems = append(m.problems, fmt.Sprintf("the plan is tier %s with nightly mode %s, and that tier runs mode %s", p.Tier, orDash(p.Nightly.Mode), want))
	}
	if !p.Complete {
		m.problems = append(m.problems, "the plan is not complete: a selection, -run or package patterns narrowed it, and only a plan of every package is a proof")
	}
	if p.Tree == "" {
		m.ineligible = append(m.ineligible, "the plan records no tree: no part can be compared with it")
	}
	if p.ProofID == "" {
		m.ineligible = append(m.ineligible, "the plan records no proof id: the parts' image tag cannot be compared with it")
	}
}

// expectedParts recomputes the parts a proof of this plan's shape must leave,
// never trusting the plan's own list; a plan whose list differs is a problem.
func (m *merger) expectedParts() []string {
	p := m.plan
	local := len(p.PartsExpected) == 1 && p.PartsExpected[0] == "local"
	uiShards := 0
	if m.uiPlan != nil {
		uiShards = len(m.uiPlan.Shards)
	}
	want := partsExpected(p.Tier, local, len(p.Shards), uiShards, p.ObservabilityRequired)
	if !slicesEqual(want, p.PartsExpected) {
		m.problems = append(m.problems, fmt.Sprintf("the plan expects parts %v, and its shape (tier %s, %d shards, %d UI shards) expects %v",
			p.PartsExpected, p.Tier, len(p.Shards), uiShards, want))
	}
	if !local && p.Tier != tierNightly && m.uiPlan == nil {
		m.problems = append(m.problems, "the proof expects ui-e2e parts and was given no UI plan")
	}
	return want
}

// dropUnexpectedParts flags every part the plan does not expect (a gate, UI
// or integration part beyond the plan) and ignores it, so its ledgers and
// gates add nothing to the merge.
func (m *merger) dropUnexpectedParts(expected []string) {
	ok := map[string]bool{}
	for _, e := range expected {
		ok[e] = true
	}
	for _, part := range sortedPartKeys(m.parts) {
		if !ok[part] {
			m.problems = append(m.problems, fmt.Sprintf("part %s is not one the plan expects: its result is ignored", part))
			delete(m.parts, part)
		}
	}
}

func sortedPartKeys(parts map[string]map[int]partReading) []string {
	out := make([]string, 0, len(parts))
	for k := range parts {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// checkQuarantine re-checks what the shards excused (plan §6.3): the tree's
// list and the base's copy beside the plan must be the files the plan hashed,
// and every quarantined-fail entry of the merged ledgers must have failing
// leaves that each name an entry the base carries too, unexpired on the
// proof's start date (the plan's checked_on), never the merge's own date. It
// returns the tree's entries, and collects the WARNING for any entry within
// seven days of its review-by date.
func (m *merger) checkQuarantine() []quarantineEntry {
	p := m.plan
	ref := p.Quarantine
	on, dateErr := time.Parse("2006-01-02", p.CheckedOn)
	if dateErr != nil {
		m.problems = append(m.problems, fmt.Sprintf("the plan recorded no start date (checked_on %q): a quarantine entry's review-by date cannot be judged", p.CheckedOn))
	}
	if ref.SHA256 == "" || ref.BaseSHA256 == "" || m.planPath == "" {
		m.problems = append(m.problems, "the plan recorded no hash of the quarantine list or of the base's copy: what the shards excused cannot be checked")
		m.refuseQuarantined("the quarantine list is not anchored")
		return nil
	}
	baseFile := filepath.Join(filepath.Dir(m.planPath), "quarantine-base.txt")
	if got, _ := fileSHA256(ref.File); got != ref.SHA256 {
		m.problems = append(m.problems, fmt.Sprintf("the quarantine list %s is not the one the plan read (plan sha256 %s, tree %s)", orDash(ref.File), short(ref.SHA256), orDash(short(got))))
	}
	if got, _ := fileSHA256(baseFile); got != ref.BaseSHA256 {
		m.problems = append(m.problems, fmt.Sprintf("the base's quarantine list in the plan artifact is not the one the plan hashed (plan sha256 %s, %s %s)", short(ref.BaseSHA256), baseFile, orDash(short(got))))
	}
	tree, err1 := readQuarantine(ref.File)
	base, err2 := readQuarantine(baseFile)
	if err1 != nil || err2 != nil || dateErr != nil {
		for _, err := range []error{err1, err2} {
			if err != nil {
				m.problems = append(m.problems, "the quarantine lists cannot be read: "+err.Error())
			}
		}
		m.refuseQuarantined("the quarantine lists cannot be judged")
		return tree
	}
	set := quarantineSet(tree, base, on)
	for _, l := range []*ledger{m.unit, m.integration} {
		if l == nil {
			continue
		}
		for _, e := range l.Tests {
			if e.Outcome != outcomeQuarantinedFail {
				continue
			}
			ok := len(e.Failures) > 0
			for _, leaf := range e.Failures {
				q, in := set[testKey{e.Package, leaf}]
				if !in || (q.Suite != l.Suite && q.Suite != "any") {
					ok = false
				}
			}
			if !ok {
				m.problems = append(m.problems, fmt.Sprintf(
					"%s.%s was recorded quarantined-fail, and on %s no current entry the base carries names each of its failing leaves (%s): it is a failure",
					e.Package, e.Test, p.CheckedOn, orDash(strings.Join(e.Failures, ", "))))
			}
		}
	}
	for _, e := range quarantineExpiring(tree, on) {
		m.warnings = append(m.warnings, quarantineWarning(e))
	}
	return tree
}

// refuseQuarantined turns every quarantined-fail entry into a problem when
// the lists that would excuse it cannot be checked.
func (m *merger) refuseQuarantined(why string) {
	for _, l := range []*ledger{m.unit, m.integration} {
		if l == nil {
			continue
		}
		for _, e := range l.Tests {
			if e.Outcome == outcomeQuarantinedFail {
				m.problems = append(m.problems, fmt.Sprintf("%s.%s was recorded quarantined-fail, and %s: it is a failure", e.Package, e.Test, why))
			}
		}
	}
}

func (m *merger) mergePart(part string) {
	attempts := map[int]partReading{}
	for a, r := range m.parts[part] {
		attempts[a] = r
	}
	jobs := m.attempts[part]
	all := map[int]bool{}
	for a := range attempts {
		all[a] = true
	}
	for a := range jobs {
		all[a] = true
	}
	if len(all) == 0 {
		m.problems = append(m.problems, fmt.Sprintf("part %s left no result: it is not rerun-eligible (only a lost, degraded or setup-failed attempt is); this run cannot become merge-eligible: start a new run", part))
		return
	}
	nums := make([]int, 0, len(all))
	for a := range all {
		nums = append(nums, a)
	}
	sort.Ints(nums)
	last := nums[len(nums)-1]
	// The earlier attempts that may run again, in attempt order, each with the
	// reason it may.
	type earlierAttempt struct {
		attempt int
		reason  string
	}
	var earlier []earlierAttempt
	for _, a := range nums[:len(nums)-1] {
		var j *githubJob
		if jj, ok := jobs[a]; ok {
			j = &jj
		}
		class, why := classify(j, attempts[a])
		switch class {
		case attemptLost:
			earlier = append(earlier, earlierAttempt{a, reasonLostRunner})
		case attemptDegraded:
			earlier = append(earlier, earlierAttempt{a, reasonRunnerDegraded})
			m.recordDegraded(part, a, attempts[a])
		case attemptSetupFailed:
			earlier = append(earlier, earlierAttempt{a, reasonSetupFailed})
			m.recordSetupFailure(part, a, j)
		case attemptResult:
			// A later attempt is a new result, never a way to erase the
			// first: the proof can pass and is still ineligible (rule 2).
			m.ineligible = append(m.ineligible, fmt.Sprintf("part %s was run again (attempt %d) after attempt %d recorded its result (%s)", part, last, a, why))
		default:
			m.ineligible = append(m.ineligible, fmt.Sprintf("part %s was run again (attempt %d) after attempt %d, which GitHub did not record as a lost runner, whose runner-health did not measure below the floor and which did not fail in a fetch or install step before its proof step (%s): only a lost, degraded or setup-failed attempt may run again; this run cannot become merge-eligible: start a new run", part, last, a, why))
		}
	}
	lastReading := attempts[last]
	var lastJob *githubJob
	if jj, ok := jobs[last]; ok {
		lastJob = &jj
	}
	if lastReading.Record != nil && len(lastReading.degradedMetrics()) == 0 && !lastReading.HealthCorrupt {
		for _, e := range earlier {
			m.reruns = append(m.reruns, m.rerun(part, last, e.attempt, e.reason, attempts[e.attempt], jobs[e.attempt]))
		}
		return
	}
	class, why := classify(lastJob, lastReading)
	switch class {
	case attemptResult:
		m.problems = append(m.problems, fmt.Sprintf("part %s left no usable result at attempt %d (%s); it is not rerun-eligible: start a new run", part, last, why))
	case attemptLost:
		m.problems = append(m.problems, fmt.Sprintf("part %s left no result because GitHub lost its runner (attempt %d, %s); re-run that job alone, within the plan and build artifacts' retention", part, last, why))
	case attemptDegraded:
		m.recordDegraded(part, last, lastReading)
		m.problems = append(m.problems, fmt.Sprintf("part %s ran on a degraded runner (attempt %d, %s): its tests have no outcome of the tree, and this is not a proof of the tree; re-run that job alone, within the plan and build artifacts' retention", part, last, why))
	case attemptSetupFailed:
		m.recordSetupFailure(part, last, lastJob)
		m.problems = append(m.problems, fmt.Sprintf("part %s left no result because its job failed in a setup step (attempt %d, %s); re-run that job alone, within the plan and build artifacts' retention", part, last, why))
	default:
		m.problems = append(m.problems, fmt.Sprintf("part %s left no result (attempt %d, %s); it is not rerun-eligible (only a lost, degraded or setup-failed attempt is): start a new run", part, last, why))
	}
}

// recordSetupFailure lists an attempt that failed in a setup step in
// setup_failures, once per attempt.
func (m *merger) recordSetupFailure(part string, attempt int, j *githubJob) {
	for _, s := range m.setupFailures {
		if s.Part == part && s.Attempt == attempt {
			return
		}
	}
	m.setupFailures = append(m.setupFailures, setupFailure{Part: part, Attempt: attempt, Step: j.FailedStep, Conclusion: j.Conclusion})
}

// describeDegraded is the README's line for a rerun's degraded attempt.
func describeDegraded(d []degradedRunner) string {
	parts := make([]string, 0, len(d))
	for _, x := range d {
		parts = append(parts, fmt.Sprintf("%s %s %s, floor %s", x.Metric, trimFloat(x.Value), x.Unit, trimFloat(x.Floor)))
	}
	return strings.Join(parts, "; ")
}

// The reasons a part attempt may run again, as a `reruns` entry records them.
const (
	reasonLostRunner     = "lost-runner"
	reasonRunnerDegraded = "runner-degraded"
	reasonSetupFailed    = "setup-failed"
)

func (m *merger) rerun(part string, attempt, earlier int, reason string, reading partReading, j githubJob) rerunRecord {
	passed, unfinished := attemptProgress(reading.Dir, j.Log)
	rr := rerunRecord{
		Part: part, Attempt: attempt, EarlierAttempts: []int{earlier}, Reason: reason,
		LostAttempt: lostAttemptRecord{
			Conclusion: j.Conclusion, Annotation: strings.Join(j.Annotations, " "), FailedStep: j.FailedStep,
			PassedBeforeLoss: passed, Unfinished: unfinished,
		},
	}
	if reason == reasonRunnerDegraded {
		rr.Degraded = degradedOf(part, earlier, reading.Health, reading.degradedMetrics())
	}
	return rr
}

// recordDegraded lists an attempt's below-floor metrics in proof.json's
// degraded_runners, once per attempt.
func (m *merger) recordDegraded(part string, attempt int, reading partReading) {
	for _, d := range m.degraded {
		if d.Part == part && d.Attempt == attempt {
			return
		}
	}
	m.degraded = append(m.degraded, degradedOf(part, attempt, reading.Health, reading.degradedMetrics())...)
}

func degradedOf(part string, attempt int, h *runnerHealth, below []healthMetric) []degradedRunner {
	out := make([]degradedRunner, 0, len(below))
	for _, b := range below {
		out = append(out, degradedRunner{Part: part, Attempt: attempt, Class: h.Class, Metric: b.Metric, Unit: b.Unit, Value: b.Value, Floor: *b.Floor})
	}
	return out
}

// collectHealth carries every attempt's runner-health record into proof.json
// (runner_health), with the verdict re-derived, and refuses a part that
// measured against floors that are not the tree's: the file the part named
// must hash as the table this merge reads. A tree without a floors table
// (an older tree) checks nothing.
func (m *merger) collectHealth() {
	treeSHA := ""
	if _, err := os.Stat(m.runnerFloorsFile); err == nil {
		if _, err := readRunnerFloors(m.runnerFloorsFile); err != nil {
			m.problems = append(m.problems, "the runner-health floors cannot be read: "+err.Error())
		}
		treeSHA, _ = fileSHA256(m.runnerFloorsFile)
	}
	for _, part := range sortedPartKeys(m.parts) {
		for _, reading := range m.orderedReadings(part) {
			if reading.HealthCorrupt {
				m.problems = append(m.problems, fmt.Sprintf("part %s attempt %d left a %s that cannot be read: it fails closed", part, reading.Attempt, runnerHealthFile))
				continue
			}
			h := reading.Health
			if h == nil {
				continue
			}
			verdict := h.verdict()
			if !floorBinds(part) && verdict == healthBelowFloor {
				// Information only: the floor does not bind this part.
				m.notes = append(m.notes, fmt.Sprintf("part %s attempt %d measured below the floor (%s); the floor does not bind this part, so it is recorded and nothing changes", part, reading.Attempt, describeBelow(h.below())))
			}
			if treeSHA != "" && h.FloorsSHA256 != "" && h.FloorsSHA256 != treeSHA {
				m.problems = append(m.problems, fmt.Sprintf("part %s attempt %d measured against runner-health floors that are not the tree's (%s; its sha256 %s, the tree's %s)", part, reading.Attempt, orDash(h.FloorsFile), short(h.FloorsSHA256), short(treeSHA)))
			}
			m.health = append(m.health, partHealth{Part: part, Attempt: reading.Attempt, Class: h.Class, Verdict: verdict, Seconds: h.Seconds, Metrics: h.Metrics})
		}
	}
}

// attemptProgress reads what an attempt's events or log say about the tests it
// passed and the tests it had started when it stopped.
func attemptProgress(dir, log string) (passCount int, unfinished []string) {
	started := map[string]bool{}
	finished := map[string]bool{}
	passedSet := map[string]bool{}
	scan := func(line string) {
		var ev event
		if json.Unmarshal([]byte(line), &ev) == nil && ev.Test != "" {
			k := ev.Package + "." + ev.Test
			switch ev.Action {
			case "run":
				started[k] = true
			case "pass":
				finished[k] = true
				passedSet[k] = true
			case "fail", "skip":
				finished[k] = true
			}
			return
		}
		if mm := progressOutcome.FindStringSubmatch(line); mm != nil {
			k := mm[2] + "." + mm[3]
			finished[k] = true
			if mm[1] == outcomePass {
				passedSet[k] = true
			}
			return
		}
		if mm := progressStart.FindStringSubmatch(line); mm != nil {
			started[mm[1]+"."+mm[2]] = true
		}
	}
	if dir != "" {
		if f, err := os.Open(filepath.Join(dir, "integration.events.jsonl")); err == nil {
			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
			for sc.Scan() {
				scan(sc.Text())
			}
			f.Close()
		}
	}
	for _, line := range strings.Split(log, "\n") {
		scan(line)
	}
	keys := make([]string, 0, len(started))
	for k := range started {
		if !finished[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return len(passedSet), keys
}

func (m *merger) checkIdentity(part string, attempt int, r *partRecord, buildImages map[string]string) {
	p := m.plan
	// Identity is an eligibility rule, never a gate result (AGENTS.md §11.2:
	// an unclean or changed tree, a shared Temporal and an outside image tag
	// each make a proof ineligible): a part measured elsewhere, or on other
	// inputs, is not this proof's evidence, whatever its tests did.
	add := func(format string, args ...any) {
		m.ineligible = append(m.ineligible, fmt.Sprintf("part %s (attempt %d): ", part, attempt)+fmt.Sprintf(format, args...))
	}
	if r.Tier != p.Tier {
		add("tier %q, and the plan says %q", r.Tier, p.Tier)
	}
	if r.Tree != p.Tree || r.TreeEnd != p.Tree {
		add("it measured tree %s → %s, and the plan's tree is %s", orDash(r.Tree), orDash(r.TreeEnd), orDash(p.Tree))
	}
	if !r.CleanStart || !r.CleanEnd {
		add("its tree was not clean (start %v, end %v)", r.CleanStart, r.CleanEnd)
	}
	if r.BaseCommit != p.BaseCommit {
		add("it diffed against %s, and the plan's base is %s", orDash(short(r.BaseCommit)), orDash(short(p.BaseCommit)))
	}
	if partUsesImages(part) {
		// A part that ran the images read the plan: an absent hash is not a
		// match (it is what a part that read no plan records).
		switch {
		case r.PlanSHA256 == "" || m.planSHA == "":
			add("it recorded no plan hash (part %s, merge %s): it cannot be tied to this plan", orDash(short(r.PlanSHA256)), orDash(short(m.planSHA)))
		case r.PlanSHA256 != m.planSHA:
			add("it read plan %s, and this merge reads %s", short(r.PlanSHA256), short(m.planSHA))
		}
		if len(r.Images) == 0 {
			add("it recorded no activity image digests")
		}
	} else if m.planSHA != "" && r.PlanSHA256 != "" && r.PlanSHA256 != m.planSHA {
		add("it read plan %s, and this merge reads %s", short(r.PlanSHA256), short(m.planSHA))
	}
	if (strings.HasPrefix(part, "ui-e2e:") || part == "local") && m.uiPlan != nil && r.UIPlanSHA256 != m.uiPlanSHA {
		add("it read UI plan %s, and this merge reads %s", orDash(short(r.UIPlanSHA256)), orDash(short(m.uiPlanSHA)))
	}
	if isStackPart(part) && r.Temporal.Mode != "own" {
		add("Temporal was %s, not its own", orDash(r.Temporal.Mode))
	}
	if partUsesImages(part) && (r.ImageTag == "" || r.ImageTag != p.ProofID || !r.ImageTagOwn) {
		add("activity images were tag %q (own: %v), and the plan's id is %q", r.ImageTag, r.ImageTagOwn, p.ProofID)
	}
	if buildImages != nil && part != "build" {
		for name, digest := range r.Images {
			if want, ok := buildImages[name]; ok && want != digest {
				add("activity-%s resolved to %s, and the build laid out %s", name, digest, want)
			}
		}
	}
	if part != "build" && len(r.Images) > 0 && len(buildImages) > 0 && len(r.Images) != len(buildImages) {
		add("%d activity images, and the build laid out %d", len(r.Images), len(buildImages))
	}
}

func partUsesImages(part string) bool {
	return part == "build" || part == "observability" || part == "local" || strings.HasPrefix(part, "integration:") ||
		strings.HasPrefix(part, "ui-e2e:") || strings.HasPrefix(part, "activity-fixture:")
}

func isStackPart(part string) bool {
	return part == "local" || strings.HasPrefix(part, "integration:") || strings.HasPrefix(part, "ui-e2e:") ||
		part == "observability"
}

// carryPartIdentity folds the parts' own identity into the merged proof, so
// verdict() judges one layout by the same rules as the other: a local
// one-part proof reports the Temporal it ran and the image tag its part
// recorded, never a mode this function invents (plan §2.6 rules 4 and 5).
// checkIdentity still refuses a stack part that is not its own Temporal and a
// tag that is not the plan's; this only makes the merged record say what
// happened.
func (m *merger) carryPartIdentity() {
	proof := m.proof
	if proof.Temporal.Mode == "" {
		proof.Temporal.Mode = "own"
	}
	for _, part := range sortedPartKeys(m.parts) {
		for _, reading := range m.orderedReadings(part) {
			r := reading.Record
			if r == nil {
				continue
			}
			if isStackPart(part) && r.Temporal.Mode != "" && r.Temporal.Mode != "own" {
				proof.Temporal.Mode = r.Temporal.Mode
				proof.Temporal.Image = r.Temporal.Image
				proof.Temporal.Address = r.Temporal.Address
			}
			if proof.ImageTag == "" && r.ImageTag != "" {
				proof.ImageTag, proof.ImageTagOwn = r.ImageTag, r.ImageTagOwn
			}
			if proof.GatesOverride == "" && r.GatesOverride != "" {
				proof.GatesOverride = r.GatesOverride
			}
			if proof.ProofJobs == 0 {
				proof.ProofJobs = r.ProofJobs
			}
			if proof.UIWorkers == 0 {
				proof.UIWorkers = r.UIWorkers
			}
		}
	}
}

func (m *merger) buildImages() map[string]string {
	for _, reading := range m.parts["build"] {
		if reading.Record != nil {
			return reading.Record.Images
		}
	}
	return nil
}

// checkAllowlist re-checks the allowed-skips file against the plan's hash
// (plan §2.6 rule 8): shards applied the list the plan read, so a tree whose
// copy changed after the plan was made is not the proof's evidence.
func (m *merger) checkAllowlist() {
	ref := m.plan.AllowedSkips
	if ref.SHA256 == "" {
		// A plan that pinned no allowlist cannot vouch for any allowed skip a
		// shard applied: it fails closed.
		m.problems = append(m.problems, fmt.Sprintf(
			"the plan recorded no hash of its allowed-skips list (%s): the skips the shards allowed cannot be checked against it", orDash(ref.File)))
		return
	}
	if got, _ := fileSHA256(ref.File); got != ref.SHA256 {
		m.problems = append(m.problems, fmt.Sprintf(
			"the allowed-skips list %s is not the one the plan read (plan sha256 %s, tree %s)",
			orDash(ref.File), short(ref.SHA256), orDash(short(got))))
		return
	}
	allow, err := readAllowlist(ref.File)
	if err != nil {
		m.problems = append(m.problems, "the allowed-skips list cannot be parsed: "+err.Error())
		return
	}
	m.allow = allow
}

// checkHostSpeedAnchor decides which host-speed rows may excuse a UI
// runner-size miss: those the tree's table and the base's copy, shipped in the
// plan artifact as host-speed-base.txt, declare identically, each file
// matching the hash the plan recorded. Without the plan's hashes nothing is
// excused (it fails closed); a file that is not the one the plan hashed fails
// the proof, as for the nightly list.
func (m *merger) checkHostSpeedAnchor() {
	m.hostSpeedExcusing = emptyHostSpeed()
	ref := m.plan.HostSpeed
	if ref.SHA256 == "" || ref.BaseSHA256 == "" || m.planPath == "" {
		if len(m.hostSpeed.UI)+len(m.hostSpeed.Tests) > 0 {
			m.notes = append(m.notes, "the plan recorded no hash of the host-speed table or of the base's copy: no runner-size miss is excused")
		}
		return
	}
	if got, _ := fileSHA256(m.hostSpeedFile); got != ref.SHA256 {
		m.problems = append(m.problems, fmt.Sprintf(
			"the host-speed table %s is not the one the plan read (plan sha256 %s, tree %s)",
			m.hostSpeedFile, short(ref.SHA256), orDash(short(got))))
		return
	}
	baseFile := filepath.Join(filepath.Dir(m.planPath), "host-speed-base.txt")
	if got, _ := fileSHA256(baseFile); got != ref.BaseSHA256 {
		m.problems = append(m.problems, fmt.Sprintf(
			"the base's host-speed table in the plan artifact is not the one the plan hashed (plan sha256 %s, %s %s)",
			short(ref.BaseSHA256), baseFile, orDash(short(got))))
		return
	}
	base, err := readHostSpeed(baseFile)
	if err != nil {
		m.notes = append(m.notes, "the base's host-speed table excuses nothing: "+err.Error())
		return
	}
	if why := m.runnerSizeRefusal(); why != "" {
		m.notes = append(m.notes, why+": no runner-size miss is excused")
		return
	}
	m.hostSpeedExcusing = anchoredHostSpeed(m.hostSpeed, base)
}

// runnerSizeRefusal says why this proof excuses no runner-size miss whatever
// the host-speed table says, or "" when its tier may. The wall-clock budgets
// were calibrated on the reference host and are enforced there (AGENTS.md
// §11); the pre-release gate is the one tier a tag rests on, so a miss in it
// is a failure on every runner, and a CI release proof needs a runner with at
// least the reference's CPUs, or `make proof-release` on the reference host.
func (m *merger) runnerSizeRefusal() string {
	if m.plan != nil && m.plan.Tier == tierRelease {
		return "the release tier enforces the wall-clock budgets on the reference class"
	}
	return ""
}

// whyNotExcused says why a declared UI bound's miss on part still counts.
func (m *merger) whyNotExcused(part string, e uiEntry) string {
	switch {
	case !isCIUIPart(part):
		return "only a CI ui-e2e part may excuse a runner-size miss"
	case m.runnerSizeRefusal() != "":
		return m.runnerSizeRefusal()
	case len(m.hostSpeedExcusing.uiBounds(e)) == 0:
		return "the base's host-speed table does not carry this row"
	default:
		return "the failure is not the assertion its row declares"
	}
}

func (m *merger) mergeIntegration() {
	if m.plan.Tier != tierNightly && len(m.plan.Jobs) == 0 && !m.hasPartPrefix("integration:") && len(m.parts["local"]) == 0 {
		return
	}
	l := &ledger{Suite: "integration", Problems: []string{}, Tests: []entry{}}
	l.Summary.Expected = len(m.plan.Expected) + len(m.plan.Deferred)
	seen := map[testKey]string{}
	byKey := map[testKey]entry{}
	sources := m.partNames("integration:")
	if _, ok := m.parts["local"]; ok {
		sources = append(sources, "local")
	}
	for _, part := range sources {
		for _, reading := range m.resultReadings(part) {
			if reading.Record == nil {
				continue
			}
			var shard ledger
			if err := readJSON(filepath.Join(reading.Dir, "integration-tests.json"), &shard); err != nil {
				continue
			}
			problemsBefore := len(m.problems)
			// A failure that is only a declared wall-clock check missed on a
			// runner below the row's reference is the merger's own excusal
			// (gotiming.go): neither the shard's line for it nor the entry
			// is a problem, and the shard's gate is not red for it.
			excused := m.goHostSpeed(part, reading, shard)
			ownLines := map[string]bool{}
			for _, e := range shard.Tests {
				if excused[testKey{e.Package, e.Test}] {
					ownLines[ownProblem(e)] = true
				}
			}
			for _, p := range shard.Problems {
				if ownLines[p] {
					continue
				}
				l.Problems = append(l.Problems, part+": "+p)
				m.problems = append(m.problems, part+": "+p)
			}
			m.checkShardListing(part, shard)
			for _, e := range shard.Tests {
				k := testKey{e.Package, e.Test}
				// A local run records each test the plan defers as `deferred`
				// with its list entry (planAndMaybeRun), as the loop over
				// m.plan.Deferred below does: it ran nowhere, and the plan's
				// own entry stands for it. Any other deferred entry is a
				// problem below.
				if e.Outcome == "deferred" && isDeferred(m.plan.Deferred, k) {
					continue
				}
				if prev, dup := seen[k]; dup && prev != part {
					msg := fmt.Sprintf("%s has outcomes from shards %s and %s", k, prev, part)
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
					continue
				}
				seen[k] = part
				switch e.Outcome {
				case outcomePass, outcomeFail, outcomeSkip, outcomeNone, outcomeQuarantinedFail:
				case "deferred":
					msg := fmt.Sprintf("%s wrote a deferred entry for %s, which the plan runs: only the plan's deferred set is deferred", part, k)
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
				default:
					msg := fmt.Sprintf("%s reported %s with outcome %q, which no ledger writes", part, k, e.Outcome)
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
				}
				if msg := m.unexplainedEntry("integration", part, e, shard.Problems); msg != "" && !excused[k] {
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
				}
				if part != "local" {
					e.Job = part + " " + orDash(e.Job)
				}
				byKey[k] = e
			}
			if len(excused) > 0 && len(m.problems) == problemsBefore {
				// The part's only defects are runner-size misses of declared
				// checks: they are recorded, and its gate is not red for them.
				m.excusedGoGate[part] = true
			}
		}
	}
	expected := map[testKey]bool{}
	for _, e := range m.plan.Expected {
		expected[testKey{e.Package, e.Test}] = true
	}
	for _, e := range m.plan.Expected {
		k := testKey{e.Package, e.Test}
		if isDeferred(m.plan.Deferred, k) {
			continue
		}
		got, ok := byKey[k]
		if !ok {
			got = entry{Package: e.Package, Test: e.Test, Outcome: outcomeNone, Reason: "no outcome: no shard reported it"}
			msg := fmt.Sprintf("%s has no outcome: no shard reported it", k)
			l.Problems = append(l.Problems, msg)
			m.problems = append(m.problems, msg)
		}
		l.Tests = append(l.Tests, got)
		countOutcome(l, got)
	}
	for _, d := range m.plan.Deferred {
		k := testKey{d.Package, d.Test}
		if e, ok := byKey[k]; ok {
			msg := fmt.Sprintf("shard reported %s, which the plan deferred", k)
			l.Problems = append(l.Problems, msg)
			m.problems = append(m.problems, msg)
			l.Tests = append(l.Tests, e)
			countOutcome(l, e)
			continue
		}
		l.Summary.Deferred++
		l.Tests = append(l.Tests, entry{Package: d.Package, Test: d.Test, Outcome: "deferred", Reason: d.Entry})
	}
	for k, e := range byKey {
		if expected[k] || isDeferred(m.plan.Deferred, k) {
			continue
		}
		msg := fmt.Sprintf("shard %s reported %s, which the plan did not expect", seen[k], k)
		l.Problems = append(l.Problems, msg)
		m.problems = append(m.problems, msg)
		l.Tests = append(l.Tests, e)
	}
	sort.SliceStable(l.Tests, func(i, j int) bool {
		if l.Tests[i].Package != l.Tests[j].Package {
			return l.Tests[i].Package < l.Tests[j].Package
		}
		return l.Tests[i].Test < l.Tests[j].Test
	})
	m.integration = l
	m.checkDeferredAnchor()
}

// checkShardListing holds rule 7: a shard's listing of every binary it ran
// equals the plan's. Every package one of the shard's jobs belongs to must
// carry a recorded listing (a local proof's part runs every job): a missing
// one fails closed. A binary digest difference stays a note, never a failure
// — separate compiles need not agree byte for byte — but the tests a binary
// reports decide which tests the shard was asked for, so a listing that
// differs means the plan and the shard disagree about the suite itself.
func (m *merger) checkShardListing(part string, shard ledger) {
	ran := map[string]bool{}
	k := shardNum(part)
	for _, j := range m.plan.Jobs {
		if part == "local" || j.Shard == k {
			ran[j.Package] = true
		}
	}
	for _, pk := range m.plan.Packages {
		if !ran[pk.ImportPath] {
			continue
		}
		got, ok := shard.Binaries[pk.ImportPath]
		switch {
		case pk.ListingSHA256 == "":
			m.problems = append(m.problems, fmt.Sprintf("the plan recorded no listing hash for %s: shard %s's listing cannot be checked against it", pk.ImportPath, part))
			continue
		case !ok || got.ListingSHA256 == "":
			m.problems = append(m.problems, fmt.Sprintf("shard %s recorded no listing of %s, which its jobs ran", part, pk.ImportPath))
			continue
		case got.ListingSHA256 != pk.ListingSHA256:
			m.problems = append(m.problems, fmt.Sprintf(
				"shard %s listed %s differently from the plan (listing sha256 %s, plan %s)",
				part, pk.ImportPath, short(got.ListingSHA256), short(pk.ListingSHA256)))
		}
		if pk.BinarySHA256 != "" && got.SHA256 != "" && got.SHA256 != pk.BinarySHA256 {
			m.digestNotes = append(m.digestNotes, fmt.Sprintf(
				"shard %s compiled %s to %s, and the plan's compile hashed to %s; separate compiles need not agree, and its listing matched",
				part, pk.ImportPath, short(got.SHA256), short(pk.BinarySHA256)))
		}
	}
}

// unexplainedEntry is the merger's own verdict on one shard ledger entry,
// never the shard's word for it: a pass is a pass; quarantined-fail is
// re-checked against the anchored lists (checkQuarantine); a skip needs an
// allowed-skip the merger finds in the allowed-skips list it parsed itself,
// naming the same reason; and every other outcome (fail, none, anything no
// ledger writes) is a problem. It returns "" when the entry is explained, and
// otherwise the problem, unless the shard's own lines already name the entry.
func (m *merger) unexplainedEntry(suite, part string, e entry, shardProblems []string) string {
	k := testKey{e.Package, e.Test}
	var why string
	switch e.Outcome {
	case outcomePass, outcomeQuarantinedFail:
		return ""
	case outcomeSkip:
		a, ok := m.allowedSkipFor(suite, k)
		if ok && e.AllowedSkip != "" && e.AllowedSkip == a.Reason {
			return ""
		}
		why = "skipped, and the allowed-skips list the plan read has no entry for it with that reason: a skip is never a pass"
	case "deferred":
		return "" // mergeIntegration refuses a shard's deferred entry itself
	default:
		why = fmt.Sprintf("has outcome %q, which is not a pass", orDash(e.Outcome))
	}
	if problemNames(shardProblems, k.String()) {
		return ""
	}
	return fmt.Sprintf("%s: %s %s", part, k, why)
}

// allowedSkipFor finds the allowed-skips entry for a top-level test in the
// list the merger parsed (only a list whose hash is the plan's is trusted).
func (m *merger) allowedSkipFor(suite string, k testKey) (allowedSkip, bool) {
	for _, a := range m.allow {
		if skipEntryApplies(a.Suite, suite) && a.Package == k.Package && a.Test == k.Test {
			return a, true
		}
	}
	return allowedSkip{}, false
}

func isDeferred(list []deferredRef, k testKey) bool {
	for _, d := range list {
		if d.Package == k.Package && d.Test == k.Test {
			return true
		}
	}
	return false
}

func countOutcome(l *ledger, e entry) {
	switch e.Outcome {
	case outcomePass:
		l.Summary.Pass++
	case outcomeFail:
		l.Summary.Fail++
	case outcomeSkip:
		if e.AllowedSkip != "" {
			l.Summary.AllowedSkip++
		} else {
			l.Summary.Skip++
		}
	case outcomeNone:
		l.Summary.NoOutcome++
	}
}

// checkNightlyAnchorFiles re-checks the two nightly lists the plan was made
// from against the files they came from (plan §2.6 rule 8): the tree's list
// from the checkout, and the base's copy from the plan artifact beside the
// plan. A hash that differs means the merger is not reading the plan's
// evidence, so the proof is refused rather than judged on different bytes. In
// defer mode a missing hash fails closed: the deferral cannot be re-derived.
func (m *merger) checkNightlyAnchorFiles() {
	p := m.plan
	if p.Nightly.Mode == nightlyDefer && (p.Nightly.SHA256 == "" || p.Nightly.BaseSHA256 == "" || m.planPath == "") {
		m.problems = append(m.problems, "the plan defers nightly tests and recorded no hash of the tree's nightly list or of the base's copy: its deferral cannot be checked")
		return
	}
	if p.Nightly.SHA256 != "" {
		if got, _ := fileSHA256(p.Nightly.File); got != p.Nightly.SHA256 {
			m.problems = append(m.problems, fmt.Sprintf(
				"the nightly list %s is not the one the plan read (plan sha256 %s, tree %s)",
				orDash(p.Nightly.File), short(p.Nightly.SHA256), orDash(short(got))))
		}
	}
	if p.Nightly.BaseSHA256 == "" || m.planPath == "" {
		return
	}
	baseFile := m.nightlyBaseFile()
	if got, _ := fileSHA256(baseFile); got != p.Nightly.BaseSHA256 {
		m.problems = append(m.problems, fmt.Sprintf(
			"the base's nightly list in the plan artifact is not the one the plan hashed (plan sha256 %s, %s %s)",
			short(p.Nightly.BaseSHA256), baseFile, orDash(short(got))))
	}
}

func (m *merger) nightlyBaseFile() string {
	return filepath.Join(filepath.Dir(m.planPath), "nightly-base.txt")
}

// checkDeferredAnchor re-derives the deferred set from the two hashed files
// themselves, parsed, never from the lists the plan embeds: a change may not
// add its own deferral, and the plan's own copy is not evidence of what the
// files say. Only defer mode defers anything.
func (m *merger) checkDeferredAnchor() {
	p := m.plan
	if p.Nightly.Mode != nightlyDefer {
		for _, d := range p.Deferred {
			m.problems = append(m.problems, fmt.Sprintf("%s was deferred, and the plan's nightly mode is %s: only defer leaves a test out", testKey{d.Package, d.Test}, orDash(p.Nightly.Mode)))
		}
		return
	}
	tree, err := readNightlyList(p.Nightly.File)
	if err != nil {
		m.problems = append(m.problems, "the tree's nightly list cannot be read to check the deferral: "+err.Error())
		return
	}
	base, err := readNightlyList(m.nightlyBaseFile())
	if err != nil {
		m.problems = append(m.problems, "the base's nightly list cannot be read to check the deferral: "+err.Error())
		return
	}
	if !slicesEqual(entryLines(tree), p.Nightly.Entries) || !slicesEqual(entryLines(base), p.Nightly.BaseEntries) {
		m.problems = append(m.problems, "the nightly entries the plan embeds are not the hashed files' entries")
	}
	inTree, inBase := map[testKey]bool{}, map[testKey]bool{}
	for _, e := range tree {
		inTree[e.key()] = true
	}
	for _, e := range base {
		inBase[e.key()] = true
	}
	want := deferredSet(tree, base)
	for _, d := range p.Deferred {
		k := testKey{d.Package, d.Test}
		if _, ok := want[k]; ok {
			continue
		}
		if !inBase[k] {
			m.problems = append(m.problems, fmt.Sprintf("%s was deferred, but the base's nightly list does not carry it: an entry the change adds is run", k))
		}
		if !inTree[k] {
			m.problems = append(m.problems, fmt.Sprintf("%s was deferred, but this tree's nightly list does not carry it", k))
		}
	}
	keys := make([]testKey, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	for _, k := range keys {
		if !isDeferred(p.Deferred, k) {
			m.problems = append(m.problems, fmt.Sprintf("%s is in both nightly lists and was not deferred", k))
		}
	}
}

// checkNightlyUI holds a nightly proof to the tree's nightly spec list itself,
// never only to the UI plan it was given: with a row listed, the proof needs a
// UI plan read from that very list (its hash), planning exactly the listed
// spec files, and its ui-e2e gate (mergeGates). A UI plan that could not be
// made (a failed listing) is then a failing nightly proof, not a passing one.
func (m *merger) checkNightlyUI() {
	rows, err := readNightlySpecs(m.nightlySpecsFile)
	if err != nil {
		m.problems = append(m.problems, "the nightly spec list cannot be read: "+err.Error())
		return
	}
	m.nightlySpecRows = rows
	if m.uiPlan == nil {
		if len(rows) > 0 {
			m.problems = append(m.problems, fmt.Sprintf("the nightly spec list names %d spec file(s), and the proof was given no UI plan", len(rows)))
		}
		return
	}
	got, _ := fileSHA256(m.nightlySpecsFile)
	if m.uiPlan.Nightly == nil || m.uiPlan.Nightly.SHA256 != got {
		read := ""
		if m.uiPlan.Nightly != nil {
			read = m.uiPlan.Nightly.SHA256
		}
		m.problems = append(m.problems, fmt.Sprintf("the nightly spec list %s (sha256 %s) is not the one the UI plan read (%s)",
			m.nightlySpecsFile, orDash(short(got)), orDash(short(read))))
	}
	want := make([]string, 0, len(rows))
	for _, e := range rows {
		want = append(want, e.file())
	}
	sort.Strings(want)
	have := make([]string, 0, len(m.uiPlan.Files))
	for _, f := range m.uiPlan.Files {
		have = append(have, f.File)
	}
	sort.Strings(have)
	if !slicesEqual(have, want) {
		m.problems = append(m.problems, fmt.Sprintf("the nightly UI plan's files %v are not the nightly spec list's %v", have, want))
	}
}

// checkUIDeferredAnchor is checkDeferredAnchor for the Playwright ledger: the
// UI plan's nightly mode must be its tier's, only defer leaves a spec file
// out, and the deferred files must be exactly the rows both nightly spec
// lists carry — re-derived from the two hashed files themselves, the tree's
// at the path the plan read and the base's copy beside the plan.
func (m *merger) checkUIDeferredAnchor() {
	p := m.uiPlan
	mode := nightlyInclude
	if p.Nightly != nil {
		mode = p.Nightly.Mode
		if want, known := tierNightlyMode[m.plan.Tier]; known && mode != want {
			m.problems = append(m.problems, fmt.Sprintf("the UI plan's nightly mode is %s, and a %s proof's is %s", orDash(mode), m.plan.Tier, want))
		}
	}
	if mode != nightlyDefer {
		for _, d := range p.Deferred {
			m.problems = append(m.problems, fmt.Sprintf("%s was deferred, and the UI plan's nightly mode is %s: only defer leaves a spec out", d.File, mode))
		}
		return
	}
	baseFile := filepath.Join(filepath.Dir(m.uiPlanPath), "nightly-specs-base.txt")
	if got, _ := fileSHA256(p.Nightly.File); got != p.Nightly.SHA256 {
		m.problems = append(m.problems, fmt.Sprintf("the nightly spec list %s is not the one the UI plan read (plan sha256 %s, tree %s)",
			orDash(p.Nightly.File), short(p.Nightly.SHA256), orDash(short(got))))
	}
	if got, _ := fileSHA256(baseFile); got != p.Nightly.BaseSHA256 {
		m.problems = append(m.problems, fmt.Sprintf("the base's nightly spec list beside the UI plan is not the one it hashed (plan sha256 %s, %s %s)",
			short(p.Nightly.BaseSHA256), baseFile, orDash(short(got))))
	}
	tree, err := readNightlySpecs(p.Nightly.File)
	if err != nil {
		m.problems = append(m.problems, "the tree's nightly spec list cannot be read to check the deferral: "+err.Error())
		return
	}
	base, err := readNightlySpecs(baseFile)
	if err != nil {
		m.problems = append(m.problems, "the base's nightly spec list cannot be read to check the deferral: "+err.Error())
		return
	}
	if !slicesEqual(specEntryLines(tree), p.Nightly.Entries) || !slicesEqual(specEntryLines(base), p.Nightly.BaseEntries) {
		m.problems = append(m.problems, "the nightly spec rows the UI plan embeds are not the hashed files' rows")
	}
	inTree, inBase := map[string]bool{}, map[string]bool{}
	for _, e := range tree {
		inTree[e.file()] = true
	}
	for _, e := range base {
		inBase[e.file()] = true
	}
	want := deferredSpecSet(tree, base)
	deferred := map[string]bool{}
	for _, d := range p.Deferred {
		deferred[d.File] = true
		if _, ok := want[d.File]; ok {
			continue
		}
		if !inBase[d.File] {
			m.problems = append(m.problems, fmt.Sprintf("%s was deferred, but the base's nightly spec list does not carry it: a row the change adds is run", d.File))
		}
		if !inTree[d.File] {
			m.problems = append(m.problems, fmt.Sprintf("%s was deferred, but this tree's nightly spec list does not carry it", d.File))
		}
	}
	files := make([]string, 0, len(want))
	for f := range want {
		files = append(files, f)
	}
	sort.Strings(files)
	for _, f := range files {
		if !deferred[f] {
			m.problems = append(m.problems, fmt.Sprintf("%s is in both nightly spec lists and was not deferred", want[f].Spec))
		}
	}
}

func (m *merger) mergeUnit() {
	if m.plan.Tier == tierNightly {
		return
	}
	for _, part := range []string{"fast", "local"} {
		if _, ok := m.parts[part]; !ok {
			continue
		}
		for _, reading := range m.resultReadings(part) {
			if reading.Record == nil {
				continue
			}
			var l ledger
			if err := readJSON(filepath.Join(reading.Dir, "unit-tests.json"), &l); err != nil {
				m.problems = append(m.problems, "part fast left no unit ledger")
				return
			}
			for _, e := range l.Tests {
				if msg := m.unexplainedEntry("unit", part, e, l.Problems); msg != "" {
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
				}
			}
			m.unit = &l
			return
		}
	}
	m.problems = append(m.problems, "part fast left no result")
}

func (m *merger) mergeUI() {
	if m.uiPlan == nil {
		return
	}
	l := &uiLedger{Suite: "ui-e2e", Problems: []string{}, Tests: []uiEntry{}}
	seen := map[string]string{}
	planned := map[string]bool{}
	for _, t := range m.uiPlan.Tests {
		planned[t.File+"\x00"+t.Title+"\x00"+t.Project] = true
	}
	// A spec file the plan defers ran nowhere: a part's own deferred entry
	// for one of its tests stands for nothing (the plan's row is recorded
	// below), and any outcome for one is a problem.
	deferredSpec := map[string]string{}
	for _, d := range m.uiPlan.Deferred {
		deferredSpec[d.File] = d.Entry
	}
	m.checkUIDeferredAnchor()
	files := map[string]uiFileEntry{}
	maxRetries := 0
	uiParts := m.partNames("ui-e2e:")
	if _, ok := m.parts["local"]; ok {
		uiParts = append(uiParts, "local")
	}
	for _, part := range uiParts {
		for _, reading := range m.resultReadings(part) {
			if reading.Record == nil {
				continue
			}
			var shard uiLedger
			if err := readJSON(filepath.Join(reading.Dir, "ui-e2e-tests.json"), &shard); err != nil {
				continue
			}
			// The merger re-derives every runner-size excusal from the shard's
			// own entries, its runner and the base-anchored table, and never
			// takes the shard's word for one (hostspeed.go).
			cpus := reading.Record.Runner.CPUs
			excusable := map[string]hostSpeedFailure{}
			for _, e := range shard.Tests {
				if f, ok := m.hostSpeedExcusing.uiRunnerSizeMiss(part, cpus, e); ok {
					excusable[f.Message] = f
				} else if b, declared := m.hostSpeed.uiBound(e); declared && e.Outcome == outcomeFail && cpus > 0 && cpus < b.ReferenceCPUs {
					// A declared bound missed where it may not be excused (the
					// local part, a row the base lacks, another assertion): the
					// failure counts, and is still recorded (rule 13).
					m.hostSpeedFailures = append(m.hostSpeedFailures, hostSpeedFailure{
						Part: part, Test: uiKey(e), Kind: "ui", Bound: b.Bound, RunnerCPUs: cpus,
						ReferenceCPUs: b.ReferenceCPUs, Message: uiFailedProblem(e) + "; not excused: " + m.whyNotExcused(part, e),
					})
				}
			}
			recorded := map[string]bool{}
			record := func(f hostSpeedFailure) {
				if recorded[f.Message] {
					return
				}
				recorded[f.Message] = true
				m.hostSpeedFailures = append(m.hostSpeedFailures, f)
				m.knownGaps = append(m.knownGaps, fmt.Sprintf("%s: %s on a %d-CPU runner (bound %.0fs, reference %d CPUs)",
					part, f.Message, f.RunnerCPUs, f.Bound, f.ReferenceCPUs))
			}
			problemsBefore := len(m.problems)
			for _, f := range shard.HostSpeedFailures {
				if mine, ok := excusable[f.Message]; ok {
					record(mine)
					continue
				}
				msg := fmt.Sprintf("%s: the shard excused %q as a runner-size miss, and the merger does not: a failure, a skip or a missing outcome of a listed spec counts unless it is the declared bound's own timing assertion on a CI UI part below the reference, under a row the base carries", part, orDash(f.Message))
				l.Problems = append(l.Problems, msg)
				m.problems = append(m.problems, msg)
			}
			// Every excusal is the merger's own: re-derived from the entry, the
			// runner and the base-anchored table, whether or not the shard
			// wrote a problem line for it.
			excusedEntry := map[string]bool{}
			for _, e := range shard.Tests {
				if f, ok := excusable[uiFailedProblem(e)]; ok {
					record(f)
					excusedEntry[uiKey(e)] = true
				}
			}
			for _, p := range shard.Problems {
				if _, ok := excusable[p]; ok {
					continue
				}
				l.Problems = append(l.Problems, part+": "+p)
				m.problems = append(m.problems, part+": "+p)
			}
			for _, e := range shard.Tests {
				k := e.File + "\x00" + e.Title + "\x00" + e.Project
				if _, d := deferredSpec[e.File]; d || e.Outcome == "deferred" {
					switch {
					case d && e.Outcome == "deferred":
					case d:
						msg := fmt.Sprintf("%s reported %s, which the UI plan defers to the nightly tier", part, uiKey(e))
						l.Problems = append(l.Problems, msg)
						m.problems = append(m.problems, msg)
					default:
						msg := fmt.Sprintf("%s wrote a deferred entry for %s, which the UI plan runs", part, uiKey(e))
						l.Problems = append(l.Problems, msg)
						m.problems = append(m.problems, msg)
					}
					continue
				}
				if !planned[k] {
					msg := fmt.Sprintf("%s reported %s, which the UI plan does not list", part, uiKey(e))
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
				}
				// The shard's problem lines are not the verdict: an entry that
				// is not a pass needs a line naming it, or an excusal derived
				// above, or it is a problem the merger writes itself.
				if e.Outcome == outcomePass || excusedEntry[uiKey(e)] || problemNames(shard.Problems, uiKey(e)) {
					continue
				}
				msg := fmt.Sprintf("%s: %s has outcome %q, and the shard's ledger names no problem for it", part, uiKey(e), orDash(e.Outcome))
				l.Problems = append(l.Problems, msg)
				m.problems = append(m.problems, msg)
			}
			if len(excusedEntry) > 0 && len(m.problems) == problemsBefore {
				// The part's only defects are runner-size misses of a declared
				// bound: record them, and do not turn its gate red.
				m.excusedUIGate[part] = true
			}
			for _, e := range shard.Tests {
				k := e.File + "\x00" + e.Title + "\x00" + e.Project
				if _, d := deferredSpec[e.File]; d {
					continue
				}
				if prev, dup := seen[k]; dup && prev != part {
					msg := fmt.Sprintf("%s has outcomes from shards %s and %s", k, prev, part)
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
					continue
				}
				seen[k] = part
				l.Tests = append(l.Tests, e)
				if e.Outcome == outcomePass {
					l.Pass++
				}
			}
			for _, f := range shard.Files {
				if prev, dup := files[f.File]; dup {
					l.Problems = append(l.Problems, fmt.Sprintf("%s has a file wall from shards %d and %d", f.File, prev.Shard, f.Shard))
					continue
				}
				f.Shard = shardNum(part)
				files[f.File] = f
			}
			if shard.GuardRetries > maxRetries {
				maxRetries = shard.GuardRetries
				l.GuardRetryCeiling = shard.GuardRetryCeiling
			}
		}
	}
	l.Listed = len(m.uiPlan.Tests)
	for _, t := range m.uiPlan.Tests {
		if row, d := deferredSpec[t.File]; d {
			l.Deferred++
			l.Tests = append(l.Tests, uiEntry{File: t.File, Title: t.Title, Project: t.Project, Outcome: "deferred", Detail: row})
			continue
		}
		if _, ok := seen[t.File+"\x00"+t.Title+"\x00"+t.Project]; !ok {
			l.Problems = append(l.Problems, fmt.Sprintf("%s › %s [%s] has no outcome", t.File, t.Title, t.Project))
		}
	}
	for _, f := range m.uiPlan.Files {
		v, ok := files[f.File]
		if !ok {
			l.Problems = append(l.Problems, fmt.Sprintf("%s has no file wall: no UI shard reported it", f.File))
			continue
		}
		l.Files = append(l.Files, v)
	}
	l.GuardRetries = maxRetries
	sort.SliceStable(l.Files, func(i, j int) bool { return l.Files[i].File < l.Files[j].File })
	m.ui = l
}

// problemNames reports whether a problem line begins with key, the way every
// ledger writes one: "<name> failed ...", "<name> skipped ...", "<name> has no
// outcome".
func problemNames(problems []string, key string) bool {
	for _, p := range problems {
		if strings.HasPrefix(p, key+" ") || p == key {
			return true
		}
	}
	return false
}

func shardNum(part string) int {
	if i := strings.LastIndex(part, ":"); i >= 0 {
		n, _ := strconv.Atoi(part[i+1:])
		return n
	}
	return 0
}

func (m *merger) mergeGates(expected []string) {
	required := map[string]bool{}
	switch m.plan.Tier {
	case tierNightly:
		required["integration"] = true
		for _, g := range nightlyGates {
			required[g] = true
		}
		// The nightly tier runs the listed spec files too, when it has any.
		if len(m.nightlySpecRows) > 0 || (m.uiPlan != nil && len(m.uiPlan.Files) > 0) {
			required["ui-e2e"] = true
		}
	default:
		for _, g := range fullGates {
			required[g] = true
		}
		if m.plan.ObservabilityRequired || m.plan.Tier == tierRelease {
			required["observability"] = true
		}
	}
	got := map[string]string{}
	seconds := map[string]float64{}
	logs := map[string]string{}
	why := map[string]string{}
	for _, part := range sortedPartKeys(m.parts) {
		for _, reading := range m.resultReadings(part) {
			if reading.Record == nil {
				continue
			}
			for _, g := range reading.Record.Gates {
				excused := (g.Gate == "ui-e2e" && m.excusedUIGate[part]) || (g.Gate == "integration" && m.excusedGoGate[part])
				switch {
				case g.Result == "pass" || excused:
					if got[g.Gate] == "" {
						got[g.Gate] = "pass"
					}
				case g.Result == gateStopped || g.Result == gateNotStarted:
					// A failing gate outranks a stopped one: it is the cause.
					if got[g.Gate] == "" || got[g.Gate] == "pass" {
						got[g.Gate] = g.Result
						why[g.Gate] = g.Command
					}
				default:
					got[g.Gate] = "fail"
				}
				seconds[g.Gate] += g.Seconds
				if logs[g.Gate] == "" {
					logs[g.Gate] = part + "/" + g.Log
				}
			}
		}
	}
	for g := range required {
		switch got[g] {
		case "":
			m.problems = append(m.problems, "gate "+g+" did not run")
		case "pass":
		case gateStopped, gateNotStarted:
			m.problems = append(m.problems, "gate "+g+" was "+strings.ReplaceAll(got[g], "-", " ")+": "+orDash(why[g]))
		default:
			m.problems = append(m.problems, "gate "+g+" failed")
		}
	}
	names := make([]string, 0, len(got))
	for g := range got {
		names = append(names, g)
	}
	sort.Strings(names)
	for _, g := range names {
		rec := gateRecord{Gate: g, Result: got[g], Seconds: seconds[g], Log: logs[g]}
		if got[g] == gateStopped || got[g] == gateNotStarted {
			rec.Command = why[g]
		}
		m.proof.Gates = append(m.proof.Gates, rec)
	}
	if len(expected) > 0 {
		m.proof.Parts = expected
	}
}

func (m *merger) partNames(prefix string) []string {
	var out []string
	for part := range m.parts {
		if strings.HasPrefix(part, prefix) {
			out = append(out, part)
		}
	}
	sort.Strings(out)
	return out
}

func (m *merger) hasPartPrefix(prefix string) bool { return len(m.partNames(prefix)) > 0 }

// resultReadings is the part's result: its last attempt that left a
// part.json and did not measure its runner below the floor. An earlier
// attempt never adds outcomes or gates to the merge; rule 2 has already
// decided whether running it again was allowed. A degraded attempt adds
// nothing either: what it recorded is not an outcome of the tree.
func (m *merger) resultReadings(part string) []partReading {
	all := m.orderedReadings(part)
	for i := len(all) - 1; i >= 0; i-- {
		if all[i].Record != nil && len(all[i].degradedMetrics()) == 0 && !all[i].HealthCorrupt {
			return all[i : i+1]
		}
	}
	return nil
}

func (m *merger) orderedReadings(part string) []partReading {
	nums := make([]int, 0, len(m.parts[part]))
	for a := range m.parts[part] {
		nums = append(nums, a)
	}
	sort.Ints(nums)
	out := make([]partReading, 0, len(nums))
	for _, a := range nums {
		out = append(out, m.parts[part][a])
	}
	return out
}

func sha256File(file string) string {
	b, err := os.ReadFile(file)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (m *merger) planFileDir(name string) string {
	for _, r := range m.parts["plan"] {
		return filepath.Join(r.Dir, name)
	}
	return ""
}

// writeMergeREADME writes the merged README: identity, parts, reruns, known
// gaps and the verdict lines.
func writeMergeREADME(file string, r *proofRecord, m *merger) error {
	var b strings.Builder
	first := "FULL PROOF"
	switch r.Tier {
	case tierNightly:
		first = "NIGHTLY PROOF"
	case tierRelease:
		first = "RELEASE PROOF"
	}
	fmt.Fprintf(&b, "# %s: %s\n\n", first, r.Name)
	fmt.Fprintf(&b, "- tier:   %s\n", r.Tier)
	fmt.Fprintf(&b, "- commit: %s\n", r.Commit)
	fmt.Fprintf(&b, "- tree:   %s\n", r.Tree)
	fmt.Fprintf(&b, "- base:   %s = %s\n", orDash(r.BaseRef), r.BaseCommit)
	if r.CI != nil {
		fmt.Fprintf(&b, "- CI:     run %s attempt %d (%s) %s\n", r.CI.RunID, r.CI.Attempt, r.CI.Event, r.CI.URL)
	}
	fmt.Fprintf(&b, "- parts:  %s\n\n", strings.Join(r.Parts, ", "))
	if len(r.QuarantineWarnings) > 0 {
		fmt.Fprintf(&b, "## Warnings\n\n")
		for _, w := range r.QuarantineWarnings {
			fmt.Fprintf(&b, "- %s\n", w)
		}
		fmt.Fprintln(&b)
	}
	if len(r.Reruns) > 0 {
		fmt.Fprintf(&b, "## Reruns\n\n")
		for _, rr := range r.Reruns {
			switch rr.Reason {
			case reasonRunnerDegraded:
				fmt.Fprintf(&b, "- %s attempt %d, after attempt %v ran on a degraded runner (%s); nothing that attempt recorded is an outcome of the tree\n",
					rr.Part, rr.Attempt, rr.EarlierAttempts, describeDegraded(rr.Degraded))
				continue
			case reasonSetupFailed:
				fmt.Fprintf(&b, "- %s attempt %d, after attempt %v failed in its setup step %q before its proof step started; no test of that attempt ran\n",
					rr.Part, rr.Attempt, rr.EarlierAttempts, rr.LostAttempt.FailedStep)
				continue
			}
			fmt.Fprintf(&b, "- %s attempt %d, after attempt %v was lost by GitHub (%s); %d tests had passed, %d had started and not finished: %s\n",
				rr.Part, rr.Attempt, rr.EarlierAttempts, rr.Reason, rr.LostAttempt.PassedBeforeLoss,
				len(rr.LostAttempt.Unfinished), strings.Join(rr.LostAttempt.Unfinished, ", "))
		}
		fmt.Fprintln(&b)
	}
	if len(r.DegradedRunners) > 0 {
		fmt.Fprintf(&b, "## Degraded runners\n\nA part whose runner measured below the floor before any gate (scripts/proof-runner-floors.tsv): nothing it recorded is an outcome of the tree, and a part listed here without a later attempt makes this proof not a proof of the tree. Re-run that job alone (docs/PROOFS.md §10).\n\n")
		for _, d := range r.DegradedRunners {
			fmt.Fprintf(&b, "- %s attempt %d (%s): %s %s %s, floor %s\n", d.Part, d.Attempt, d.Class, d.Metric, trimFloat(d.Value), d.Unit, trimFloat(d.Floor))
		}
		fmt.Fprintln(&b)
	}
	if len(r.SetupFailures) > 0 {
		fmt.Fprintf(&b, "## Setup failures\n\nA part whose job failed in a fetch or install step before its proof step started (docs/PROOFS.md §5): no test of that attempt ran, and a part listed here without a later attempt has no result. Re-run that job alone (docs/PROOFS.md §10).\n\n")
		for _, s := range r.SetupFailures {
			fmt.Fprintf(&b, "- %s attempt %d: the step %q ended %s before the proof step started\n", s.Part, s.Attempt, s.Step, orDash(s.Conclusion))
		}
		fmt.Fprintln(&b)
	}
	if len(r.Quarantined) > 0 {
		fmt.Fprintf(&b, "## Quarantined\n\n")
		for _, q := range r.Quarantined {
			fmt.Fprintf(&b, "- %s\n", q)
		}
		fmt.Fprintln(&b)
	}
	if len(r.ListsChanged) > 0 {
		fmt.Fprintf(&b, "## This change edits the gate's own files\n\n")
		for _, c := range r.ListsChanged {
			if c.Error != "" {
				fmt.Fprintf(&b, "- the gate files could not be listed: %s\n", c.Error)
				continue
			}
			fmt.Fprintf(&b, "- `%s` — changed", c.File)
			if len(c.Added) > 0 {
				fmt.Fprintf(&b, "; %d row(s) added", len(c.Added))
			}
			if len(c.Removed) > 0 {
				fmt.Fprintf(&b, "; %d row(s) removed", len(c.Removed))
			}
			fmt.Fprintln(&b)
			for _, added := range c.Added {
				fmt.Fprintf(&b, "  - added: `%s`\n", added)
			}
			for _, removed := range c.Removed {
				fmt.Fprintf(&b, "  - removed: `%s`\n", removed)
			}
		}
		fmt.Fprintln(&b)
	}
	if len(r.HostSpeedFailures) > 0 {
		fmt.Fprintf(&b, "## Known gaps: runner size\n\n")
		for _, f := range r.HostSpeedFailures {
			verdict := "recorded; it still fails the proof"
			if f.Excused {
				verdict = "recorded, not merge-blocking"
			}
			fmt.Fprintf(&b, "- %s: %s — bound %.0fs on %d CPUs, runner has %d; %s\n",
				f.Part, f.Message, f.Bound, f.ReferenceCPUs, f.RunnerCPUs, verdict)
		}
		fmt.Fprintln(&b)
	}
	if notes := append(append([]string{}, m.notes...), r.BinaryDigestNotes...); len(notes) > 0 {
		fmt.Fprintf(&b, "## Notes\n\n")
		for _, n := range notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		fmt.Fprintln(&b)
	}
	writeRunnerHealthSection(&b, r.RunnerHealth)
	fmt.Fprintf(&b, "## Gates\n\n")
	var early []gateRecord
	for _, g := range r.Gates {
		switch g.Result {
		case gateStopped:
			early = append(early, g)
			fmt.Fprintf(&b, "- **%s**: stopped after %.0fs — %s — `%s`\n", g.Gate, g.Seconds, g.Command, g.Log)
		case gateNotStarted:
			early = append(early, g)
			fmt.Fprintf(&b, "- **%s**: not started — %s\n", g.Gate, g.Command)
		default:
			fmt.Fprintf(&b, "- **%s**: %s in %.0fs — `%s`\n", g.Gate, g.Result, g.Seconds, g.Log)
		}
	}
	fmt.Fprintln(&b)
	if len(early) > 0 {
		fmt.Fprintf(&b, "## Stopped early\n\nA gate failed, and the proof stopped the gates that were still running and started none after it (scripts/proof.sh, `PROOF_FAIL_FAST`; docs/PROOFS.md says how to read this). A stopped proof is a failed proof, and the failed gate's log says why:\n\n")
		for _, g := range r.Gates {
			if g.Result == "fail" {
				fmt.Fprintf(&b, "- failed: **%s** — `%s`\n", g.Gate, g.Log)
			}
		}
		for _, g := range early {
			fmt.Fprintf(&b, "- %s: **%s** — %s\n", strings.ReplaceAll(g.Result, "-", " "), g.Gate, g.Command)
		}
		fmt.Fprintln(&b)
	}
	writeVerdict(&b, r)
	return os.WriteFile(file, []byte(b.String()), 0o644)
}
