package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// runnerBelowFloorAnnotation is the whole message of the one GitHub
// annotation scripts/proof-part.sh prints when a stack part stops on a runner
// below the floor, under GITHUB_ACTIONS=true (`::error::` plus this text).
// It has no variable text, so the rerun planner matches it exactly; it is
// evidence for running a job again and never for a verdict, because the
// merger re-derives degradation from runner-health.json.
const runnerBelowFloorAnnotation = "proof: runner below the floor, no gate ran"

// defaultMaxRunAttempts is the run attempt at which the planner stops: a run
// is rerun automatically while its attempt is below it, so at most two
// automatic reruns follow the first attempt.
const defaultMaxRunAttempts = 3

// The planner of automatic reruns (.github/workflows/proof-rerun.yml) decides
// from GitHub's own record of one completed attempt of a proof run whether
// "Re-run failed jobs" is worth an attempt: only when every job that failed is
// the proof aggregator, or a part the merger itself would call lost, degraded
// or setup-failed (classify). The merger stays the only authority: a rerun the
// planner got wrong can make a proof ineligible, never pass one, and the merger
// reads the earlier attempts again from their artifacts and records.
//
// Nothing here runs the candidate's code, and the output holds only words the
// planner validated: a part name matched against the closed shapes of the
// workflow's part jobs, a fixed class word, a fixed reason. Annotation and log
// text are read to decide and never printed, because a line that starts `::` is
// a command to the runner.

// ghAPI fetches one GitHub REST resource through `gh api`. With paginate it
// asks for every page, and the pages of a paginated resource are concatenated
// JSON values. It is a parameter so that a test can answer it.
type ghAPI func(path string, paginate bool) ([]byte, error)

// rerunPlanGH is the ghAPI cmdRerunPlan reads with; a test replaces it.
var rerunPlanGH ghAPI = ghExec

// ghArgs is the argument list of one `gh api` call.
func ghArgs(path string, paginate bool) []string {
	args := []string{"api"}
	if paginate {
		args = append(args, "--paginate")
	}
	return append(args, path)
}

// ghExec reads one resource with the gh CLI, which takes its token from
// GH_TOKEN. Each call is bounded.
func ghExec(path string, paginate bool) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", ghArgs(path, paginate)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return nil, fmt.Errorf("gh api %s: %v: %s", path, err, msg)
	}
	return out.Bytes(), nil
}

// The reasons of a decision, a closed vocabulary.
const (
	planEligible        = "eligible"
	planAttemptCap      = "attempt-cap"
	planNoEligiblePart  = "no-eligible-part"
	planNotEligible     = "not-eligible"
	planOtherJobFailed  = "other-job-failed"
	planUnreadable      = "unreadable"
	proofAggregatorName = "proof"
)

// plannedPart is one part the planner would run again, and why: Class is one
// of the merger's rerun reasons (lost-runner, runner-degraded, setup-failed).
type plannedPart struct {
	Part  string
	Class string
}

// rerunDecision is what the planner decided: whether to run the failed jobs
// again, why (a word from the closed vocabulary above), and the parts it would
// run again.
type rerunDecision struct {
	Rerun  bool
	Reason string
	Parts  []plannedPart
}

// output is the decision as the workflow reads it: three key=value lines.
func (d rerunDecision) output() string {
	parts := make([]string, 0, len(d.Parts))
	for _, p := range d.Parts {
		parts = append(parts, p.Part+"="+p.Class)
	}
	return fmt.Sprintf("rerun=%t\nreason=%s\nparts=%s\n", d.Rerun, d.Reason, strings.Join(parts, ","))
}

// plannedPartRE is every shape a part job of proof.yml can have, as the
// merger names the part. The matrix values of a job name come from a plan the
// candidate's code wrote, so a name is accepted only if it is one of these.
var plannedPartRE = regexp.MustCompile(`^(?:(?:integration|ui-e2e):[0-9]{1,3}|activity-fixture:(?:shell|json|git|template)|fast|observability)$`)

// apiJob is the part of GitHub's job record the planner reads.
type apiJob struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Conclusion  string `json:"conclusion"`
	CheckRunURL string `json:"check_run_url"`
	Steps       []struct {
		Name       string `json:"name"`
		Conclusion string `json:"conclusion"`
	} `json:"steps"`
}

// apiAnnotation is one check-run annotation: its text and its level.
type apiAnnotation struct {
	Message         string `json:"message"`
	AnnotationLevel string `json:"annotation_level"`
}

// decodePages reads a run of JSON values, one per page `gh api --paginate`
// printed, into fn.
func decodePages(raw []byte, fn func(dec *json.Decoder) error) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	read := 0
	for {
		if err := fn(dec); err != nil {
			if err == io.EOF && read > 0 {
				return nil
			}
			return err
		}
		read++
	}
}

func readJobPages(raw []byte) ([]apiJob, error) {
	var jobs []apiJob
	err := decodePages(raw, func(dec *json.Decoder) error {
		var page struct {
			Jobs []apiJob `json:"jobs"`
		}
		if err := dec.Decode(&page); err != nil {
			return err
		}
		jobs = append(jobs, page.Jobs...)
		return nil
	})
	return jobs, err
}

func readAnnotationPages(raw []byte) ([]apiAnnotation, error) {
	var anns []apiAnnotation
	err := decodePages(raw, func(dec *json.Decoder) error {
		var page []apiAnnotation
		if err := dec.Decode(&page); err != nil {
			return err
		}
		anns = append(anns, page...)
		return nil
	})
	return anns, err
}

// planRerun decides about one completed attempt of a run. At or past
// maxAttempts it reads nothing at all.
func planRerun(gh ghAPI, repo, run string, attempt, maxAttempts int) rerunDecision {
	if attempt >= maxAttempts {
		return rerunDecision{Reason: planAttemptCap}
	}
	raw, err := gh(fmt.Sprintf("repos/%s/actions/runs/%s/attempts/%d/jobs?per_page=100", repo, run, attempt), true)
	if err != nil {
		return rerunDecision{Reason: planUnreadable}
	}
	jobs, err := readJobPages(raw)
	if err != nil {
		return rerunDecision{Reason: planUnreadable}
	}
	var parts []plannedPart
	for _, job := range jobs {
		if job.Conclusion == "success" || job.Conclusion == "skipped" || job.Name == proofAggregatorName {
			continue
		}
		part := partFromJobName(job.Name)
		// The plan and the build are what every other part reads, and the jobs
		// that needed them were skipped, not failed: running them again
		// leaves attempt-one records the merger cannot excuse. Nothing else
		// that is not a part (base, a name nobody knows) is answered by a rerun.
		if !plannedPartRE.MatchString(part) {
			return rerunDecision{Reason: planOtherJobFailed}
		}
		class, ok := classifyForRerun(gh, repo, part, job)
		if !ok {
			return rerunDecision{Reason: planNotEligible}
		}
		parts = append(parts, plannedPart{Part: part, Class: class})
	}
	if len(parts) == 0 {
		return rerunDecision{Reason: planNoEligiblePart}
	}
	// By name, so the same failures read the same whatever order GitHub lists
	// the jobs in.
	sort.Slice(parts, func(i, j int) bool { return parts[i].Part < parts[j].Part })
	return rerunDecision{Rerun: true, Reason: planEligible, Parts: parts}
}

// classifyForRerun reads a failed part job's annotations and log and says
// whether the merger would let it run again, and under which reason. A read
// that fails makes the job a result: it fails closed, as in the merger.
func classifyForRerun(gh ghAPI, repo, part string, job apiJob) (string, bool) {
	gj := githubJob{Name: job.Name, Part: part, Conclusion: job.Conclusion}
	for _, s := range job.Steps {
		gj.Steps = append(gj.Steps, githubStep{Name: s.Name, Conclusion: s.Conclusion})
		if gj.FailedStep == "" && s.Conclusion == "failure" {
			gj.FailedStep = s.Name
		}
	}
	marker := false
	check := job.CheckRunURL[strings.LastIndex(job.CheckRunURL, "/")+1:]
	if _, err := strconv.ParseInt(check, 10, 64); err != nil {
		gj.ReadError = "no check run"
	} else if raw, err := gh(fmt.Sprintf("repos/%s/check-runs/%s/annotations?per_page=100", repo, check), true); err != nil {
		gj.ReadError = "annotations: the API call failed"
	} else if anns, err := readAnnotationPages(raw); err != nil {
		gj.ReadError = "annotations: the API call failed"
	} else {
		for _, a := range anns {
			gj.Annotations = append(gj.Annotations, a.Message)
			if a.AnnotationLevel == "failure" && strings.TrimSpace(a.Message) == runnerBelowFloorAnnotation {
				marker = true
			}
		}
	}
	// Best effort, as in the merger: GitHub keeps no log for a runner it lost.
	logRead := false
	if gj.ReadError == "" {
		if raw, err := gh(fmt.Sprintf("repos/%s/actions/jobs/%d/logs", repo, job.ID), false); err == nil {
			gj.Log, logRead = string(raw), true
		}
	}
	var class attemptClass
	if marker && logRead && degradedByMarker(part, &gj) {
		class = attemptDegraded
	} else {
		class, _ = classify(&gj, partReading{Part: part})
	}
	switch class {
	case attemptLost:
		return reasonLostRunner, true
	case attemptDegraded:
		return reasonRunnerDegraded, true
	case attemptSetupFailed:
		return reasonSetupFailed, true
	}
	return "", false
}

// degradedByMarker says a stack part's job stopped on a runner below the
// floor, given that its check run carries the part's annotation: the job
// failed, the step that failed is the proof step (the annotation is that
// step's own), and no test started or finished in its log. Only the merger
// decides what the attempt was; this only decides whether another is worth
// trying, so a forged annotation costs one wasted attempt.
func degradedByMarker(part string, j *githubJob) bool {
	if !floorBinds(part) || j.Conclusion != "failure" || !proofStepNames[j.FailedStep] {
		return false
	}
	for _, line := range strings.Split(j.Log, "\n") {
		if progressStart.MatchString(line) || progressOutcome.MatchString(line) {
			return false
		}
	}
	return true
}

// cmdRerunPlan is `proofrun rerun-plan`: it prints the decision for one
// completed attempt of a proof run, as three key=value lines a workflow
// appends to its step output. Every decision, including "do not rerun" and
// "could not read", exits 0; an invalid invocation exits 2 and prints nothing
// to standard output.
func cmdRerunPlan(args []string) int {
	fs := flag.NewFlagSet("rerun-plan", flag.ContinueOnError)
	repo := fs.String("repo", "", "OWNER/NAME of the repository (required)")
	run := fs.String("run", "", "the id of the proof run (required)")
	attempt := fs.Int("attempt", 0, "the run attempt that just completed (required)")
	maxAttempts := fs.Int("max-attempts", defaultMaxRunAttempts, "never rerun at or past this run attempt: at most max-attempts-1 automatic reruns")
	if err := fs.Parse(args); err != nil || *repo == "" || *run == "" || *attempt < 1 || *maxAttempts < 1 || fs.NArg() > 0 ||
		!regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`).MatchString(*repo) || strings.Contains(*repo, "..") ||
		!regexp.MustCompile(`^[0-9]{1,20}$`).MatchString(*run) {
		fmt.Fprintln(os.Stderr, "proofrun rerun-plan -repo OWNER/NAME -run ID -attempt N [-max-attempts N]")
		return 2
	}
	d := planRerun(rerunPlanGH, *repo, *run, *attempt, *maxAttempts)
	fmt.Fprint(stdout, d.output())
	return 0
}
