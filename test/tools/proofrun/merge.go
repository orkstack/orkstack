package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
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
}

type temporalRecord struct {
	Mode    string `json:"mode"`
	Image   string `json:"image,omitempty"`
	Address string `json:"address,omitempty"`
}

// rerunRecord is one part attempt that GitHub said was lost, and the attempt
// that ran after it. It is the only rerun a merge-eligible proof may hold.
type rerunRecord struct {
	Part            string            `json:"part"`
	Attempt         int               `json:"attempt"`
	EarlierAttempts []int             `json:"earlier_attempts"`
	Reason          string            `json:"reason"`
	LostAttempt     lostAttemptRecord `json:"lost_attempt"`
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
	Bound         float64 `json:"bound,omitempty"`
	Observed      float64 `json:"observed,omitempty"`
	RunnerCPUs    int     `json:"runner_cpus"`
	ReferenceCPUs int     `json:"reference_cpus"`
	BudgetMinutes float64 `json:"budget_minutes,omitempty"`
	ElapsedMin    float64 `json:"elapsed_minutes,omitempty"`
	Message       string  `json:"message"`
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
}

type githubStep struct {
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
}

var runnerLostMarker = regexp.MustCompile(`(?i)(the runner has received a shutdown signal|lost communication with the server)`)
var timeLimitMarker = regexp.MustCompile(`(?i)(has exceeded the maximum execution time|the job running on runner .* has exceeded)`)

// cmdMerge is `proofrun merge`: the one verdict path for a full, release or
// nightly proof, whether it ran locally or as many CI parts.
func cmdMerge(args []string) int {
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)
	planFile := fs.String("plan", "", "the integration plan (required)")
	uiPlanFile := fs.String("ui-plan", "", "the UI plan, when the tier runs Playwright")
	partsDir := fs.String("parts", "", "directory holding one subdirectory per part attempt (required)")
	jobsDir := fs.String("jobs", "", "GitHub's record of every attempt's jobs")
	hostSpeedFile := fs.String("host-speed", "scripts/proof-host-speed.tsv", "assertions bounded by host speed")
	out := fs.String("out", "", "the proof directory to write (required)")
	name := fs.String("name", "", "the proof's name")
	ciRunID := fs.String("ci-run-id", "", "the CI run id")
	ciAttempt := fs.Int("ci-attempt", 0, "the CI run attempt")
	ciEvent := fs.String("ci-event", "", "the CI event")
	ciURL := fs.String("ci-url", "", "the CI run URL")
	if err := fs.Parse(args); err != nil || *planFile == "" || *partsDir == "" || *out == "" || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "proofrun merge -plan FILE -parts DIR -out DIR [-ui-plan FILE] [-jobs DIR] -name NAME")
		return 2
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
	planSHA, _ := fieldSHA256(*planFile)
	uiPlanSHA, _ := fieldSHA256(*uiPlanFile)
	parts, absent, decodeErr := readParts(*partsDir)
	if decodeErr != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", decodeErr)
		return 2
	}
	attempts := readGithubAttempts(*jobsDir)
	m := &merger{
		plan: plan, planPath: *planFile, uiPlan: uiP, planSHA: planSHA, uiPlanSHA: uiPlanSHA,
		parts: parts, absent: absent,
		attempts: attempts, hostSpeed: readHostSpeed(*hostSpeedFile),
		excusedUIGate: map[string]bool{},
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
	if m.unit != nil {
		_ = writeJSON(filepath.Join(*out, "unit-tests.json"), m.unit)
	}
	if m.integration != nil {
		_ = writeJSON(filepath.Join(*out, "integration-tests.json"), m.integration)
	}
	if m.ui != nil {
		_ = writeJSON(filepath.Join(*out, "ui-e2e-tests.json"), m.ui)
	}
	writeMergeREADME(filepath.Join(*out, "README.md"), proof, m)
	if proof.Result != "pass" {
		return 1
	}
	return 0
}

// merger accumulates the merged proof.
type merger struct {
	plan              *integrationPlan
	planPath          string
	uiPlan            *uiPlan
	planSHA           string
	uiPlanSHA         string
	parts             map[string]map[int]partReading
	absent            map[string]bool
	attempts          map[string]map[int]githubJob
	hostSpeed         hostSpeedTable
	problems          []string
	ineligible        []string
	reruns            []rerunRecord
	proof             *proofRecord
	unit              *ledger
	integration       *ledger
	ui                *uiLedger
	notes             []string
	hostSpeedFailures []hostSpeedFailure
	knownGaps         []string
	excusedUIGate     map[string]bool
}

// partReading is one part attempt's on-disk result.
type partReading struct {
	Dir    string
	Record *partRecord
	Events bool
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
		reading := partReading{Dir: p, Events: attemptHasEvents(p)}
		if has {
			reading.Record = &rec
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

// attemptHasEvents reports whether an attempt's events show a test that
// failed, or one that started and never finished: a result, even with no
// part.json (plan §2.6 rule 2).
func attemptHasEvents(dir string) bool {
	f, err := os.Open(filepath.Join(dir, "integration.events.jsonl"))
	if err != nil {
		return false
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
			return true
		case "run":
			started[k] = true
		case "pass", "skip":
			finished[k] = true
		}
	}
	for k := range started {
		if !finished[k] {
			return true
		}
	}
	return false
}

func readGithubAttempts(dir string) map[string]map[int]githubJob {
	out := map[string]map[int]githubJob{}
	if dir == "" {
		return out
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var a githubAttempt
		if readJSON(filepath.Join(dir, e.Name()), &a) != nil {
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
	return out
}

var jobPartRE = regexp.MustCompile(`(?i)^(integration|ui-e2e|activity-fixture)\s*\(([^)]+)\)$`)

func partFromJobName(name string) string {
	if mm := jobPartRE.FindStringSubmatch(strings.TrimSpace(name)); mm != nil {
		return strings.ToLower(mm[1]) + ":" + mm[2]
	}
	switch strings.TrimSpace(name) {
	case "plan", "build", "fast", "observability":
		return strings.TrimSpace(name)
	}
	return ""
}

// classify is plan §2.6 rule 2's five rows. It returns whether the attempt is
// a result, whether it is a lost runner (the only rerun-eligible class), and
// what to say.
func classify(j *githubJob, reading partReading) (isResult, lost bool, why string) {
	if j == nil {
		// No record of the attempt: it fails closed.
		return true, false, "no record of it: it fails closed"
	}
	log := j.Log
	if strings.Contains(log, "proofrun: outcome fail") {
		return true, false, "its log records a failing test"
	}
	annotation := strings.Join(j.Annotations, "\n")
	if j.RunnerLost || runnerLostMarker.MatchString(annotation) || runnerLostMarker.MatchString(log) {
		return false, true, strings.TrimSpace(annotation)
	}
	if reading.Record != nil {
		return true, false, ""
	}
	if j.Conclusion == "cancelled" || timeLimitMarker.MatchString(annotation) || timeLimitMarker.MatchString(log) {
		if start := regexp.MustCompile(`proofrun: start (\S+) (\S+)`).FindStringSubmatch(log); start != nil && !strings.Contains(log, "proofrun: outcome") {
			return true, false, "a test was running when the job stopped: " + start[1] + "." + start[2]
		}
		return true, false, "it was cancelled"
	}
	if reading.Events {
		return true, false, "its events record a failing or unfinished test"
	}
	return false, false, orDash(j.Conclusion)
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
	expected := p.PartsExpected
	// Every expected part has its last attempt, and only a lost runner may
	// have been run again (rule 1 and rule 2).
	for _, part := range expected {
		m.mergePart(part)
	}
	buildImages := m.buildImages()
	m.carryPartIdentity()
	m.checkAllowlist()
	m.checkNightlyAnchorFiles()
	for part, got := range m.parts {
		for attempt, reading := range got {
			if reading.Record == nil {
				continue
			}
			m.checkIdentity(part, attempt, reading.Record, buildImages)
		}
	}
	m.mergeIntegration()
	m.mergeUnit()
	m.mergeUI()
	if m.ui != nil {
		proof.GuardRetries, proof.GuardCeiling = &m.ui.GuardRetries, &m.ui.GuardRetryCeiling
	}
	m.mergeGates(expected)
	m.proof.Parts = append([]string{}, expected...)
	m.proof.Reruns = m.reruns
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
	proof.PR = p.PR
	proof.HostSpeedFailures = m.hostSpeedFailures
	proof.KnownGaps = m.knownGaps
	proof.Quarantine = &quarantineRecord{BaseSHA256: p.Quarantine.BaseSHA256}
	if entries, err := readQuarantine(p.Quarantine.File); err == nil && len(entries) > 0 {
		proof.Quarantine.CheckedOn = time.Now().UTC().Format("2006-01-02")
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
	proof.Nightly = &nightlyRecord{SHA256: p.Nightly.SHA256, BaseCommit: p.Nightly.BaseCommit, BaseSHA256: p.Nightly.BaseSHA256}
	proof.Ledgers = map[string]any{}
	if m.unit != nil {
		proof.Ledgers.(map[string]any)["unit"] = map[string]any{"summary": m.unit.Summary, "problems": len(m.unit.Problems)}
	}
	if m.integration != nil {
		proof.Ledgers.(map[string]any)["integration"] = map[string]any{"summary": m.integration.Summary, "problems": len(m.integration.Problems)}
	}
	if m.ui != nil {
		proof.Ledgers.(map[string]any)["ui-e2e"] = map[string]any{"listed": m.ui.Listed, "pass": m.ui.Pass, "problems": len(m.ui.Problems)}
	}
	ledgersOK := map[string]bool{}
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
		m.problems = append(m.problems, fmt.Sprintf("part %s left no result: it is not rerun-eligible (only a lost runner is); this run cannot become merge-eligible: start a new run", part))
		return
	}
	nums := make([]int, 0, len(all))
	for a := range all {
		nums = append(nums, a)
	}
	sort.Ints(nums)
	last := nums[len(nums)-1]
	var earlierWithResult []int
	var lostEarlier []int
	for _, a := range nums[:len(nums)-1] {
		reading := attempts[a]
		var j *githubJob
		if jj, ok := jobs[a]; ok {
			j = &jj
		}
		isResult, lost, _ := classify(j, reading)
		if isResult {
			earlierWithResult = append(earlierWithResult, a)
		} else if lost {
			lostEarlier = append(lostEarlier, a)
		}
	}
	if len(earlierWithResult) > 0 {
		// A later attempt is a new result, never a way to erase the first: the
		// proof can pass and is still ineligible (plan §2.6 rule 2).
		m.ineligible = append(m.ineligible, fmt.Sprintf("part %s was run again (attempt %d) after attempt %d recorded its result", part, last, earlierWithResult[0]))
	}
	lastReading, hasLast := attempts[last]
	var lastJob *githubJob
	if jj, ok := jobs[last]; ok {
		lastJob = &jj
	}
	if hasLast && lastReading.Record != nil {
		for _, a := range lostEarlier {
			m.reruns = append(m.reruns, m.rerun(part, last, a, attempts[a], jobs[a]))
		}
		return
	}
	isResult, lost, why := classify(lastJob, lastReading)
	switch {
	case isResult:
		m.problems = append(m.problems, fmt.Sprintf("part %s left no usable result at attempt %d (%s); it is not rerun-eligible: start a new run", part, last, why))
	case lost:
		m.problems = append(m.problems, fmt.Sprintf("part %s left no result because GitHub lost its runner (attempt %d, %s); re-run that job alone, within the plan and build artifacts' retention", part, last, orDash(why)))
	default:
		m.problems = append(m.problems, fmt.Sprintf("part %s left no result (attempt %d, %s); it is not rerun-eligible (only a lost runner is): start a new run", part, last, why))
	}
}

func (m *merger) rerun(part string, attempt, lost int, reading partReading, j githubJob) rerunRecord {
	passed, unfinished := attemptProgress(reading.Dir, j.Log)
	return rerunRecord{
		Part: part, Attempt: attempt, EarlierAttempts: []int{lost}, Reason: "lost-runner",
		LostAttempt: lostAttemptRecord{
			Conclusion: j.Conclusion, Annotation: strings.Join(j.Annotations, " "), FailedStep: j.FailedStep,
			PassedBeforeLoss: passed, Unfinished: unfinished,
		},
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
		if strings.HasPrefix(line, "proofrun: start ") {
			f := strings.Fields(line)
			if len(f) >= 4 {
				started[f[2]+"."+f[3]] = true
			}
		}
		if strings.HasPrefix(line, "proofrun: outcome ") {
			f := strings.Fields(line)
			if len(f) >= 5 {
				finished[f[3]+"."+f[4]] = true
				if f[2] == "pass" {
					passedSet[f[3]+"."+f[4]] = true
				}
			}
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
	add := func(format string, args ...any) {
		m.problems = append(m.problems, fmt.Sprintf("part %s (attempt %d): ", part, attempt)+fmt.Sprintf(format, args...))
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
	if m.planSHA != "" && r.PlanSHA256 != "" && r.PlanSHA256 != m.planSHA {
		add("it read plan %s, and this merge reads %s", short(r.PlanSHA256), short(m.planSHA))
	}
	if isStackPart(part) && r.Temporal.Mode != "own" {
		add("Temporal was %s, not its own", orDash(r.Temporal.Mode))
	}
	if partUsesImages(part) && (r.ImageTag != p.ProofID || !r.ImageTagOwn) {
		add("activity images were tag %q (own: %v), and the plan's id is %q", r.ImageTag, r.ImageTagOwn, p.ProofID)
	}
	if buildImages != nil && part != "build" {
		for name, digest := range r.Images {
			if want, ok := buildImages[name]; ok && want != digest {
				add("activity-%s resolved to %s, and the build laid out %s", name, digest, want)
			}
		}
	}
	if part != "build" && len(r.Images) > 0 && buildImages != nil && len(r.Images) != len(buildImages) {
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
	for _, part := range m.plan.PartsExpected {
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
	if ref.File == "" || ref.SHA256 == "" {
		return
	}
	if got := sha256File(ref.File); got != ref.SHA256 {
		m.problems = append(m.problems, fmt.Sprintf(
			"the allowed-skips list %s is not the one the plan read (plan sha256 %s, tree %s)",
			ref.File, short(ref.SHA256), short(got)))
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
		for _, reading := range m.orderedReadings(part) {
			if reading.Record == nil {
				continue
			}
			var shard ledger
			if err := readJSON(filepath.Join(reading.Dir, "integration-tests.json"), &shard); err != nil {
				continue
			}
			for _, p := range shard.Problems {
				l.Problems = append(l.Problems, part+": "+p)
				m.problems = append(m.problems, part+": "+p)
			}
			m.checkShardListing(part, shard)
			for _, e := range shard.Tests {
				k := testKey{e.Package, e.Test}
				if prev, dup := seen[k]; dup && prev != part {
					msg := fmt.Sprintf("%s has outcomes from shards %s and %s", k, prev, part)
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
					continue
				}
				seen[k] = part
				if part != "local" {
					e.Job = part + " " + orDash(e.Job)
				}
				byKey[k] = e
				m.recordIntegrationHostSpeed(part, reading, e)
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
		l.Problems = append(l.Problems, fmt.Sprintf("shard reported %s, which the plan did not expect", k))
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

// checkShardListing holds rule 7: a shard's listing of every binary it
// compiled equals the plan's. A binary digest difference stays a note, never a
// failure — separate compiles need not agree byte for byte — but the tests a
// binary reports decide which tests the shard was asked for, so a listing that
// differs means the plan and the shard disagree about the suite itself.
func (m *merger) checkShardListing(part string, shard ledger) {
	for _, pk := range m.plan.Packages {
		got, ok := shard.Binaries[pk.ImportPath]
		if !ok || got.ListingSHA256 == "" || pk.ListingSHA256 == "" {
			continue
		}
		if got.ListingSHA256 != pk.ListingSHA256 {
			m.problems = append(m.problems, fmt.Sprintf(
				"shard %s listed %s differently from the plan (listing sha256 %s, plan %s)",
				part, pk.ImportPath, short(got.ListingSHA256), short(pk.ListingSHA256)))
		}
	}
}

// recordIntegrationHostSpeed records a failing integration test named in
// -host-speed on a part whose runner is below the bound's reference CPU count
// (plan §2.6 rule 13). The failure still counts: result and merge_eligible are
// decided by rules 1-12 alone, and this only makes the runner-size class of a
// failure readable from proof.json and the README.
func (m *merger) recordIntegrationHostSpeed(part string, reading partReading, e entry) {
	if e.Outcome != outcomeFail || reading.Record == nil {
		return
	}
	bound, ok := m.hostSpeed.Tests[testKey{e.Package, e.Test}]
	if !ok {
		return
	}
	cpus := reading.Record.Runner.CPUs
	if cpus <= 0 || cpus >= bound.ReferenceCPUs {
		return
	}
	message := fmt.Sprintf("%s failed on a %d-CPU runner (bound %.0fs, reference %d CPUs)", e.Test, cpus, bound.Bound, bound.ReferenceCPUs)
	if bound.Note != "" {
		message += ": " + bound.Note
	}
	m.hostSpeedFailures = append(m.hostSpeedFailures, hostSpeedFailure{
		Part: part, Test: e.Package + "." + e.Test, Kind: "integration", Bound: bound.Bound,
		RunnerCPUs: cpus, ReferenceCPUs: bound.ReferenceCPUs, Message: message,
	})
	m.knownGaps = append(m.knownGaps, part+": "+message)
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

// checkNightlyAnchorFiles re-checks the two nightly lists the plan carries
// against the files they came from (plan §2.6 rule 8): the tree's list from
// the checkout, and the base's copy from the plan artifact beside the plan. A
// hash that differs means the merger is not reading the plan's evidence, so
// the proof is refused rather than judged on different bytes.
func (m *merger) checkNightlyAnchorFiles() {
	p := m.plan
	if p.Nightly.File != "" && p.Nightly.SHA256 != "" {
		if got := sha256File(p.Nightly.File); got != p.Nightly.SHA256 {
			m.problems = append(m.problems, fmt.Sprintf(
				"the nightly list %s is not the one the plan read (plan sha256 %s, tree %s)",
				p.Nightly.File, short(p.Nightly.SHA256), short(got)))
		}
	}
	if p.Nightly.BaseSHA256 == "" || m.planPath == "" {
		return
	}
	baseFile := filepath.Join(filepath.Dir(m.planPath), "nightly-base.txt")
	got := sha256File(baseFile)
	if got != p.Nightly.BaseSHA256 {
		m.problems = append(m.problems, fmt.Sprintf(
			"the base's nightly list in the plan artifact is not the one the plan hashed (plan sha256 %s, %s %s)",
			short(p.Nightly.BaseSHA256), baseFile, orDash(short(got))))
	}
}

// checkDeferredAnchor re-derives the deferred set from the plan's copies of
// the two nightly lists, because a change may not add its own deferral.
func (m *merger) checkDeferredAnchor() {
	p := m.plan
	if p.Nightly.Mode != nightlyDefer {
		return
	}
	base := map[testKey]bool{}
	for _, line := range p.Nightly.BaseEntries {
		f := strings.Split(line, "\t")
		if len(f) == 2 {
			base[testKey{f[0], f[1]}] = true
		}
	}
	tree := map[testKey]bool{}
	for _, line := range p.Nightly.Entries {
		f := strings.Split(line, "\t")
		if len(f) == 2 {
			tree[testKey{f[0], f[1]}] = true
		}
	}
	for _, d := range p.Deferred {
		k := testKey{d.Package, d.Test}
		if !base[k] || !tree[k] {
			m.problems = append(m.problems, fmt.Sprintf("%s was deferred, but the base's nightly list does not carry it: an entry the change adds is run", k))
		}
		if !tree[k] {
			m.problems = append(m.problems, fmt.Sprintf("%s was deferred, but this tree's nightly list does not carry it", k))
		}
	}
	for k := range tree {
		if base[k] && !isDeferred(p.Deferred, k) {
			// Only entries of this proof's packages can be deferred; a test of
			// a package the proof does not compile is not run at all.
			listed := false
			for _, pkg := range p.Packages {
				for _, t := range pkg.Listed {
					if pkg.ImportPath == k.Package && t == k.Test {
						listed = true
					}
				}
			}
			if listed {
				m.problems = append(m.problems, fmt.Sprintf("%s is in both nightly lists and was not deferred", k))
			}
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
		for _, reading := range m.orderedReadings(part) {
			if reading.Record == nil {
				continue
			}
			var l ledger
			if err := readJSON(filepath.Join(reading.Dir, "unit-tests.json"), &l); err != nil {
				m.problems = append(m.problems, "part fast left no unit ledger")
				return
			}
			m.unit = &l
			return
		}
	}
	m.problems = append(m.problems, "part fast left no result")
}

func (m *merger) mergeUI() {
	if m.uiPlan == nil || m.plan.Tier == tierNightly {
		return
	}
	l := &uiLedger{Suite: "ui-e2e", Problems: []string{}, Tests: []uiEntry{}}
	seen := map[string]string{}
	files := map[string]uiFileEntry{}
	maxRetries := 0
	uiParts := m.partNames("ui-e2e:")
	if _, ok := m.parts["local"]; ok {
		uiParts = append(uiParts, "local")
	}
	for _, part := range uiParts {
		for _, reading := range m.orderedReadings(part) {
			if reading.Record == nil {
				continue
			}
			var shard uiLedger
			if err := readJSON(filepath.Join(reading.Dir, "ui-e2e-tests.json"), &shard); err != nil {
				continue
			}
			for _, gap := range shard.KnownGaps {
				m.knownGaps = append(m.knownGaps, part+": "+gap)
			}
			for _, f := range shard.HostSpeedFailures {
				f.Part = part
				if f.RunnerCPUs == 0 {
					f.RunnerCPUs = reading.Record.Runner.CPUs
				}
				m.hostSpeedFailures = append(m.hostSpeedFailures, f)
			}
			cpus := reading.Record.Runner.CPUs
			gapsHere := 0
			for _, p := range shard.Problems {
				if bound, ok := m.hostSpeed.specForProblem(p); ok && cpus > 0 && cpus < bound.ReferenceCPUs {
					m.hostSpeedFailures = append(m.hostSpeedFailures, hostSpeedFailure{
						Part: part, Test: p, Kind: "ui", Bound: bound.Bound,
						RunnerCPUs: cpus, ReferenceCPUs: bound.ReferenceCPUs, Message: p,
					})
					m.knownGaps = append(m.knownGaps, fmt.Sprintf("%s: %s on a %d-CPU runner (bound %.0fs, reference %d CPUs)",
						part, p, cpus, bound.Bound, bound.ReferenceCPUs))
					gapsHere++
					continue
				}
				l.Problems = append(l.Problems, part+": "+p)
				m.problems = append(m.problems, part+": "+p)
			}
			if gapsHere > 0 && gapsHere == len(shard.Problems) {
				// The part's only defects are runner-size misses of a declared
				// bound: record them, and do not turn its gate red.
				m.excusedUIGate[part] = true
			}
			for _, e := range shard.Tests {
				k := e.File + "\x00" + e.Title + "\x00" + e.Project
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
	for part := range m.parts {
		for _, reading := range m.orderedReadings(part) {
			if reading.Record == nil {
				continue
			}
			for _, g := range reading.Record.Gates {
				excused := g.Gate == "ui-e2e" && m.excusedUIGate[part]
				if g.Result != "pass" && !excused {
					got[g.Gate] = "fail"
				} else if got[g.Gate] == "" {
					got[g.Gate] = "pass"
				}
				seconds[g.Gate] += g.Seconds
				if logs[g.Gate] == "" {
					logs[g.Gate] = part + "/" + g.Log
				}
			}
		}
	}
	for g := range required {
		if got[g] == "" {
			m.problems = append(m.problems, "gate "+g+" did not run")
			continue
		}
		if got[g] != "pass" {
			m.problems = append(m.problems, "gate "+g+" failed")
		}
	}
	names := make([]string, 0, len(got))
	for g := range got {
		names = append(names, g)
	}
	sort.Strings(names)
	for _, g := range names {
		m.proof.Gates = append(m.proof.Gates, gateRecord{Gate: g, Result: got[g], Seconds: seconds[g], Log: logs[g]})
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

// hostBound is one assertion whose result depends on the host's speed.
type hostBound struct {
	Test          string
	Spec          string
	Bound         float64
	ReferenceCPUs int
	Note          string
}

// hostSpeedTable is scripts/proof-host-speed.tsv: UI specs whose budget is a
// wall clock on the reference host, and tests whose threshold is. A spec
// miss on a runner below the reference CPU count is a known gap, not a
// shard failure (the user's M5 decision).
type hostSpeedTable struct {
	Specs map[string]hostBound
	Tests map[testKey]hostBound
}

func (t hostSpeedTable) specForProblem(problem string) (hostBound, bool) {
	for spec, bound := range t.Specs {
		base := path.Base(spec)
		if strings.HasPrefix(problem, spec+" › ") || strings.HasPrefix(problem, base+" › ") ||
			strings.Contains(problem, spec+" › ") {
			return bound, true
		}
	}
	return hostBound{}, false
}

func readHostSpeed(file string) hostSpeedTable {
	out := hostSpeedTable{Specs: map[string]hostBound{}, Tests: map[testKey]hostBound{}}
	if file == "" {
		return out
	}
	f, err := os.Open(file)
	if err != nil {
		return out
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) < 4 {
			continue
		}
		switch f[0] {
		case "ui":
			bound, _ := strconv.ParseFloat(f[2], 64)
			cpus, _ := strconv.Atoi(f[3])
			note := ""
			if len(f) > 4 {
				note = f[4]
			}
			out.Specs[f[1]] = hostBound{Spec: f[1], Bound: bound, ReferenceCPUs: cpus, Note: note}
		case "test":
			if len(f) < 5 {
				continue
			}
			bound, _ := strconv.ParseFloat(f[3], 64)
			cpus, _ := strconv.Atoi(f[4])
			note := ""
			if len(f) > 5 {
				note = f[5]
			}
			out.Tests[testKey{f[1], f[2]}] = hostBound{Test: f[2], Bound: bound, ReferenceCPUs: cpus, Note: note}
		}
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
func writeMergeREADME(file string, r *proofRecord, m *merger) {
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
	if len(r.Reruns) > 0 {
		fmt.Fprintf(&b, "## Reruns\n\n")
		for _, rr := range r.Reruns {
			fmt.Fprintf(&b, "- %s attempt %d, after attempt %v was lost by GitHub (%s); %d tests had passed, %d had started and not finished: %s\n",
				rr.Part, rr.Attempt, rr.EarlierAttempts, rr.Reason, rr.LostAttempt.PassedBeforeLoss,
				len(rr.LostAttempt.Unfinished), strings.Join(rr.LostAttempt.Unfinished, ", "))
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
			fmt.Fprintf(&b, "- %s: %s — bound %.0fs on %d CPUs, runner has %d; recorded, not merge-blocking\n",
				f.Part, f.Message, f.Bound, f.ReferenceCPUs, f.RunnerCPUs)
		}
		fmt.Fprintln(&b)
	}
	if len(m.notes) > 0 {
		fmt.Fprintf(&b, "## Notes\n\n")
		for _, n := range m.notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		fmt.Fprintln(&b)
	}
	fmt.Fprintf(&b, "## Gates\n\n")
	for _, g := range r.Gates {
		fmt.Fprintf(&b, "- **%s**: %s in %.0fs — `%s`\n", g.Gate, g.Result, g.Seconds, g.Log)
	}
	fmt.Fprintln(&b)
	writeVerdict(&b, r)
	if err := os.WriteFile(file, []byte(b.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
	}
}
