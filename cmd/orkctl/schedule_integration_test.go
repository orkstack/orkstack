//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// scheduleStack is one or two real `ork` replicas and one Orker (the Bash
// fixture's shell activity) on a private Temporal namespace and the shared
// MinIO bucket, with an OrkStack namespace of the test's own.
type scheduleStack struct {
	temporal    client.Client
	artifactCfg artifacts.Config
	address     string
	partition   string
	channel     string
	enginePath  string
	orkerPath   string
	a, b        *managedEngine
	namespace   string
	transcript  strings.Builder
	mu          sync.Mutex
}

// managedEngine is one `ork` process a test can stop and start again on the
// same address. Only processes this test started are ever signalled.
type managedEngine struct {
	*engineReplica
	cmd  *exec.Cmd
	done chan struct{}
	log  *lockedBuffer
}

func (s *scheduleStack) startEngine(ctx context.Context, t *testing.T, name, addr string) *managedEngine {
	t.Helper()
	cmd := exec.CommandContext(ctx, s.enginePath)
	cmd.Env = append(os.Environ(),
		"ORK_HTTP_ADDR="+addr,
		"ORK_TEMPORAL_ADDRESS="+s.address,
		"ORK_TEMPORAL_NAMESPACE="+s.partition,
		"ORK_ORKER_QUEUE="+s.channel,
		"ORK_ARTIFACT_BACKEND=s3",
		"ORK_ADMIN_ADDR="+freeAddress(t),
		localActivityCatalogEnv(t),
	)
	log := &lockedBuffer{}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	e := &managedEngine{cmd: cmd, done: make(chan struct{}), log: log}
	go func() { _ = cmd.Wait(); close(e.done) }()
	httpClient := h2cClient(t)
	baseURL := "http://" + addr
	e.engineReplica = &engineReplica{
		name:  name,
		addr:  addr,
		runs:  orkstackv1connect.NewRunServiceClient(httpClient, baseURL),
		flows: orkstackv1connect.NewOrkflowServiceClient(httpClient, baseURL),
	}
	t.Cleanup(func() {
		e.stop()
		t.Logf("%s log:\n%s", name, log.String())
	})
	e.awaitReady(ctx, t)
	return e
}

// stop interrupts this engine (the PID this test started) and waits for it.
func (e *managedEngine) stop() {
	select {
	case <-e.done:
		return
	default:
	}
	_ = e.cmd.Process.Signal(os.Interrupt)
	select {
	case <-e.done:
	case <-time.After(15 * time.Second):
		_ = e.cmd.Process.Kill()
		<-e.done
	}
}

func newScheduleStack(ctx context.Context, t *testing.T, label string, replicas int) *scheduleStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = "ork-schedules"
	}
	stamp := time.Now().UnixNano() % 1_000_000_000
	partition := fmt.Sprintf("%s-sch-%s-%d-%d", base, label, os.Getpid(), stamp)
	temporalClient := dialIsolatedNamespace(ctx, t, address, partition)

	s := &scheduleStack{
		temporal:    temporalClient,
		artifactCfg: artifactCfg,
		address:     address,
		partition:   partition,
		channel:     idempotencyTaskQueue(),
		enginePath:  testbin.Path(t, "./cmd/ork"),
		orkerPath:   testbin.Path(t, "./cmd/orker"),
		namespace:   fmt.Sprintf("sch-%s-%d", label, stamp),
	}
	startIdempotencyOrker(ctx, t, temporalClient, s.orkerPath, address, partition, s.channel)
	s.a = s.startEngine(ctx, t, "ork-A", freeAddress(t))
	if replicas > 1 {
		s.b = s.startEngine(ctx, t, "ork-B", freeAddress(t))
	}
	fmt.Fprintf(&s.transcript, "# orkflow schedules: %s (%s)\n", label, time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&s.transcript, "temporal namespace  %s\nnamespace           %s\nreplica A           %s\n\n",
		partition, s.namespace, s.a.addr)
	s.cli(t, s.a, exitOK, "namespace", "create", s.namespace)
	return s
}

// cli runs the real orkctl against one replica in the stack's namespace.
func (s *scheduleStack) cli(t *testing.T, replica *managedEngine, want int, args ...string) string {
	t.Helper()
	out, errOut, code := s.cliRaw(t, replica, args...)
	if code != want {
		t.Fatalf("orkctl %v via %s exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			args, replica.name, code, want, out, errOut)
	}
	return out
}

func (s *scheduleStack) cliRaw(t *testing.T, replica *managedEngine, args ...string) (string, string, int) {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(principalEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	full := append([]string{"-n", s.namespace}, args...)
	var stdout, stderr bytes.Buffer
	code := Execute(append(full, "--server", "http://"+replica.addr), &stdout, &stderr)
	s.mu.Lock()
	fmt.Fprintf(&s.transcript, "$ orkctl %s    # via %s\n%s%s(exit %d)\n\n",
		strings.Join(quoteArgs(full), " "), replica.name, stdout.String(), stderr.String(), code)
	s.mu.Unlock()
	return stdout.String(), stderr.String(), code
}

// apply writes and applies one orkflow document.
func (s *scheduleStack) apply(t *testing.T, yaml string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "orkflow.ork.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	s.cli(t, s.a, exitOK, "apply", path)
}

func (s *scheduleStack) writeProof(t *testing.T, name string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	proof.Write(t, "orkctl", name, []byte(s.transcript.String()))
}

// scheduleOrkflowYAML is one shell task that sleeps for its input's seconds
// and exits early on SIGTERM, as a well-behaved workload does.
func scheduleOrkflowYAML(name, version string, sleepSeconds int, pool string) string {
	poolLine := ""
	if pool != "" {
		poolLine = "\n    pool: " + pool
	}
	return fmt.Sprintf(`name: %s
version: %q
description: schedule probe.
tasks:
  - id: work
    uses: shell%s
    timeout_seconds: 180
    with:
      op: script
      interpreter: sh
      script: |
        trap 'exit 143' TERM
        sleep %d &
        wait $!
        printf '{}' > /orkstack/output/values.json
`, name, version, poolLine, sleepSeconds)
}

// scheduleJSON is `schedule get --json`.
type scheduleJSON struct {
	Schedule struct {
		ID            string   `json:"id"`
		Name          string   `json:"name"`
		Paused        bool     `json:"paused"`
		Note          string   `json:"note"`
		NextFireTimes []string `json:"nextFireTimes"`
		RecentFires   []struct {
			ScheduledAt      string `json:"scheduledAt"`
			ActualAt         string `json:"actualAt"`
			RunID            string `json:"runId"`
			Refusal          string `json:"refusal"`
			DetailsAvailable bool   `json:"detailsAvailable"`
		} `json:"recentFires"`
		FiresTaken          int64  `json:"firesTaken,string"`
		FiresSkippedOverlap int64  `json:"firesSkippedOverlap,string"`
		UnavailableReason   string `json:"unavailableReason"`
	} `json:"schedule"`
}

func decodeSchedule(t *testing.T, stdout string) scheduleJSON {
	t.Helper()
	var s scheduleJSON
	if err := json.Unmarshal([]byte(stdout), &s); err != nil {
		t.Fatalf("decode schedule: %v (%s)", err, stdout)
	}
	return s
}

func (s *scheduleStack) get(t *testing.T, replica *managedEngine, name string) scheduleJSON {
	t.Helper()
	return decodeSchedule(t, s.cli(t, replica, exitOK, "--json", "schedule", "get", name))
}

// create creates a schedule and returns its id.
func (s *scheduleStack) create(t *testing.T, name string, args ...string) string {
	t.Helper()
	out := s.cli(t, s.a, exitOK, append([]string{"--json", "schedule", "create", name}, args...)...)
	created := decodeSchedule(t, out)
	if !strings.HasPrefix(created.Schedule.ID, "sch-") {
		t.Fatalf("created schedule id = %q", created.Schedule.ID)
	}
	// Listing reads the cluster's visibility index, which lags a create; a
	// scenario that lists (or deletes the orkflow) starts once it caught up.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var listed struct {
			Schedules []struct {
				Name string `json:"name"`
			} `json:"schedules"`
		}
		out, _, code := s.cliRaw(t, s.a, "--json", "schedule", "list")
		if code == exitOK && json.Unmarshal([]byte(out), &listed) == nil {
			for _, row := range listed.Schedules {
				if row.Name == name {
					return created.Schedule.ID
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("schedule %s was never listed", name)
	return created.Schedule.ID
}

// awaitFires polls `schedule get` until want reports true.
func (s *scheduleStack) awaitSchedule(t *testing.T, name string, within time.Duration, want func(scheduleJSON) bool) scheduleJSON {
	t.Helper()
	deadline := time.Now().Add(within)
	var last scheduleJSON
	for time.Now().Before(deadline) {
		last = s.get(t, s.a, name)
		if want(last) {
			return last
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("schedule %s never satisfied the expectation within %s; last %+v", name, within, last.Schedule)
	return last
}

// runsOf lists the runs a schedule started.
func (s *scheduleStack) runsOf(t *testing.T, scheduleID string) []runRow {
	t.Helper()
	out := s.cli(t, s.a, exitOK, "--json", "run", "list", "--filter", "origin.kind=SCHEDULE", "--filter", "origin.id="+scheduleID)
	var response struct {
		Runs []runRow `json:"runs"`
	}
	if err := json.Unmarshal([]byte(out), &response); err != nil {
		t.Fatalf("decode run list: %v (%s)", err, out)
	}
	sort.Slice(response.Runs, func(i, j int) bool { return response.Runs[i].CreatedAt < response.Runs[j].CreatedAt })
	return response.Runs
}

type runRow struct {
	ID             string `json:"id"`
	Status         string `json:"status"`
	OrkflowVersion string `json:"orkflowVersion"`
	CreatedAt      string `json:"createdAt"`
	Origin         struct {
		Kind        string `json:"kind"`
		ID          string `json:"id"`
		Reference   string `json:"reference"`
		Description string `json:"description"`
	} `json:"origin"`
}

func (s *scheduleStack) run(t *testing.T, replica *managedEngine, runID string) runRow {
	t.Helper()
	out := s.cli(t, replica, exitOK, "--json", "run", "get", runID)
	var response struct {
		Run runRow `json:"run"`
	}
	if err := json.Unmarshal([]byte(out), &response); err != nil {
		t.Fatalf("decode run get: %v (%s)", err, out)
	}
	return response.Run
}

func (s *scheduleStack) awaitRunStatus(t *testing.T, runID string, within time.Duration, statuses ...string) runRow {
	t.Helper()
	deadline := time.Now().Add(within)
	var last runRow
	for time.Now().Before(deadline) {
		last = s.run(t, s.a, runID)
		for _, want := range statuses {
			if last.Status == want {
				return last
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("run %s never reached %v within %s (last %s)", runID, statuses, within, last.Status)
	return last
}

// awaitNextFireSecond returns once the wall clock is past the second that
// names run's fire. Temporal appends a fire's nominal time, truncated to the
// second, to the fire workflow's id, and `schedule fire` takes "now" as that
// nominal time. A second fire in the same second therefore collides with the
// first fire's still-running workflow and the cluster starts nothing for it —
// no run, no refusal, no counter (reproduced with `temporal schedule trigger`
// twice in one second: one action, actionCount 1). A test that means two
// fires must send the second one in a later second. The cluster runs on this
// host's clock, and it takes the nominal time when it handles the request,
// never before it is sent.
func (s *scheduleStack) awaitNextFireSecond(t *testing.T, run runRow) {
	t.Helper()
	nominal, err := time.Parse(time.RFC3339, run.Origin.Reference)
	if err != nil {
		t.Fatalf("run %s reference %q: %v", run.ID, run.Origin.Reference, err)
	}
	next := nominal.Truncate(time.Second).Add(time.Second)
	if wait := time.Until(next); wait > 0 {
		// A clock boundary, not synchronisation: nothing else can make the
		// second pass sooner. Bounded by one second by construction.
		time.Sleep(wait)
	}
	fmt.Fprintf(&s.transcript, "# next fire after %s's nominal second %s\n\n", run.ID, nominal.Format(time.RFC3339))
}

// awaitRuns polls the runs a schedule started until want reports true.
func (s *scheduleStack) awaitRuns(t *testing.T, scheduleID string, within time.Duration, want func([]runRow) bool) []runRow {
	t.Helper()
	deadline := time.Now().Add(within)
	var last []runRow
	for time.Now().Before(deadline) {
		last = s.runsOf(t, scheduleID)
		if want(last) {
			return last
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("runs of %s never satisfied the expectation within %s; last %+v", scheduleID, within, last)
	return nil
}

// fireWorkflows lists the schedule's fire workflows straight from the
// cluster, oldest first.
func (s *scheduleStack) fireWorkflows(ctx context.Context, t *testing.T, scheduleID string) []fireExecution {
	t.Helper()
	resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("WorkflowType = '%s'", engine.ScheduleFireWorkflowType),
	})
	if err != nil {
		t.Fatalf("list fire workflows: %v", err)
	}
	var out []fireExecution
	for _, info := range resp.GetExecutions() {
		if !strings.HasPrefix(info.GetExecution().GetWorkflowId(), engine.ScheduleFireWorkflowIDPrefix+scheduleID) {
			continue
		}
		out = append(out, fireExecution{
			WorkflowID: info.GetExecution().GetWorkflowId(),
			RunID:      info.GetExecution().GetRunId(),
			Status:     info.GetStatus(),
			Start:      info.GetStartTime().AsTime(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// awaitFireWorkflows polls visibility, which is eventually consistent, until
// it lists n fire workflows of the schedule.
func (s *scheduleStack) awaitFireWorkflows(ctx context.Context, t *testing.T, scheduleID string, n int) []fireExecution {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var fires []fireExecution
	for time.Now().Before(deadline) {
		fires = s.fireWorkflows(ctx, t, scheduleID)
		if len(fires) >= n {
			return fires
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("visibility listed %d fire workflows of %s, want %d", len(fires), scheduleID, n)
	return nil
}

type fireExecution struct {
	WorkflowID, RunID string
	Status            enumspb.WorkflowExecutionStatus
	Start             time.Time
}

func (s *scheduleStack) fireStatus(ctx context.Context, t *testing.T, workflowID string) enumspb.WorkflowExecutionStatus {
	t.Helper()
	described, err := s.temporal.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		t.Fatalf("describe fire %s: %v", workflowID, err)
	}
	return described.GetWorkflowExecutionInfo().GetStatus()
}

// firstRunID waits for the newest recent fire to name a run.
func (s *scheduleStack) awaitFireRun(t *testing.T, name string, count int) scheduleJSON {
	t.Helper()
	return s.awaitSchedule(t, name, 90*time.Second, func(got scheduleJSON) bool {
		withRun := 0
		for _, fire := range got.Schedule.RecentFires {
			if fire.RunID != "" {
				withRun++
			}
		}
		return withRun >= count
	})
}

// ---------------------------------------------------------------------------

// TestIntegration_ScheduleFireStartsRunWithScheduleOrigin: a paused schedule
// shows ten next fire times at 02:00 Lisbon; a manual fire starts an ordinary
// run carrying {SCHEDULE, sch-…, nominal time, name}; the origin filters and
// the origin index row in MinIO name it; and nothing under metadata/ is a
// schedule record.
func TestIntegration_ScheduleFireStartsRunWithScheduleOrigin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "origin", 1)
	s.apply(t, scheduleOrkflowYAML("hello", "0.1.0", 0, ""))

	id := s.create(t, "nightly", "--orkflow", "hello", "--cron", "0 2 * * *", "--timezone", "Europe/Lisbon", "--paused")
	got := s.get(t, s.a, "nightly")
	if !got.Schedule.Paused {
		t.Fatalf("created --paused, get reports active: %+v", got.Schedule)
	}
	lisbon, err := time.LoadLocation("Europe/Lisbon")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Schedule.NextFireTimes) != 10 {
		t.Fatalf("next fire times = %v, want ten", got.Schedule.NextFireTimes)
	}
	for _, next := range got.Schedule.NextFireTimes {
		at, err := time.Parse(time.RFC3339, next)
		if err != nil {
			t.Fatal(err)
		}
		if local := at.In(lisbon); local.Hour() != 2 || local.Minute() != 0 {
			t.Fatalf("next fire %s is %s in Lisbon, want 02:00", next, local)
		}
	}
	t.Logf("EVIDENCE schedule_get_next_fire_times=%v", got.Schedule.NextFireTimes)
	s.cli(t, s.a, exitOK, "schedule", "get", "nightly")

	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	fired := s.awaitFireRun(t, "nightly", 1)
	fire := fired.Schedule.RecentFires[len(fired.Schedule.RecentFires)-1]
	runID := fire.RunID

	runJSON := s.cli(t, s.a, exitOK, "--json", "run", "get", runID)
	origin := originOf(t, runJSON)
	nominal, err := time.Parse(time.RFC3339Nano, fire.ScheduledAt)
	if err != nil {
		t.Fatalf("scheduledAt %q: %v", fire.ScheduledAt, err)
	}
	// The reference is the nominal time in RFC 3339 UTC, whole seconds; a
	// manual fire's nominal time is the instant it was requested.
	if origin["kind"] != "SCHEDULE" || origin["id"] != id || origin["description"] != "nightly" ||
		origin["reference"] != nominal.UTC().Truncate(time.Second).Format(time.RFC3339) || origin["principal"] != "" {
		t.Fatalf("run %s origin = %v, want SCHEDULE %s nightly at %s", runID, origin, id, fire.ScheduledAt)
	}
	t.Logf("EVIDENCE run_get_json=%s", compactJSON(t, runJSON))

	s.awaitRunStatus(t, runID, 3*time.Minute, "SUCCEEDED")
	runs := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 && r[0].ID == runID })
	t.Logf("EVIDENCE run_list_origin_schedule=%v", runs[0].ID)
	if ids := listedIDs(t, s.cli(t, s.a, exitOK, "--json", "run", "list", "--filter", "origin.kind=SUBMIT")); containsID(ids, runID) {
		t.Fatalf("--filter origin.kind=SUBMIT lists the scheduled run %s", runID)
	}

	prefix := fmt.Sprintf("%s/%s/schedule/%s/", artifacts.RunOriginIndexNamespace, s.namespace, id)
	raw := auditRawS3Client(t, s.artifactCfg)
	var originKeys []string
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		page, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.artifactCfg.S3Bucket), Prefix: aws.String(prefix)})
		if err != nil {
			t.Fatalf("list %s: %v", prefix, err)
		}
		originKeys = originKeys[:0]
		for _, object := range page.Contents {
			originKeys = append(originKeys, aws.ToString(object.Key))
		}
		if len(originKeys) == 1 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(originKeys) != 1 || !strings.HasSuffix(originKeys[0], "/"+runID) {
		t.Fatalf("origin rows under %s = %v, want exactly the run's", prefix, originKeys)
	}
	t.Logf("EVIDENCE minio_origin_row=%s", originKeys[0])

	// No schedule record anywhere under metadata/: every key that names this
	// schedule is the run's origin-index row.
	var scheduleKeys []string
	var token *string
	scanned := 0
	for {
		page, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.artifactCfg.S3Bucket), Prefix: aws.String("metadata/"), ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("list metadata/: %v", err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			scanned++
			if strings.HasPrefix(key, "metadata/schedules/") ||
				((strings.Contains(key, id) || strings.Contains(key, "/nightly")) && !strings.HasPrefix(key, prefix) &&
					strings.Contains(key, s.namespace)) {
				scheduleKeys = append(scheduleKeys, key)
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		token = page.NextContinuationToken
	}
	if len(scheduleKeys) != 0 {
		t.Fatalf("schedule records under metadata/: %v", scheduleKeys)
	}
	t.Logf("EVIDENCE minio_metadata_scan keys=%d schedule_keys=0 (only %s)", scanned, originKeys[0])
	fmt.Fprintf(&s.transcript, "$ aws s3 ls --recursive s3://%s/metadata/ | grep -e %s -e metadata/schedules/\n%s\n(%d keys scanned; the one naming the schedule is its run's origin-index row)\n\n",
		s.artifactCfg.S3Bucket, id, originKeys[0], scanned)
	s.writeProof(t, "schedule-fire-origin.txt")
}

// TestIntegration_ScheduleFireIsNotListedAsARun: `run list` shows the run and
// not the fire; the cluster holds exactly one fire workflow beside it.
func TestIntegration_ScheduleFireIsNotListedAsARun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "notrun", 1)
	s.apply(t, scheduleOrkflowYAML("hello", "0.1.0", 0, ""))
	id := s.create(t, "nightly", "--orkflow", "hello", "--cron", "0 2 * * *", "--paused")
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	runID := s.awaitFireRun(t, "nightly", 1).Schedule.RecentFires[0].RunID
	s.awaitRunStatus(t, runID, 3*time.Minute, "SUCCEEDED")

	var ids []string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ids = listedIDs(t, s.cli(t, s.a, exitOK, "--json", "run", "list"))
		if len(ids) == 1 {
			break
		}
		time.Sleep(time.Second)
	}
	if len(ids) != 1 || ids[0] != runID {
		t.Fatalf("run list = %v, want exactly the scheduled run %s", ids, runID)
	}
	fires := s.awaitFireWorkflows(ctx, t, id, 1)
	if len(fires) != 1 || containsID(ids, fires[0].WorkflowID) {
		t.Fatalf("fire workflows = %+v, want one that is not a listed run", fires)
	}
	t.Logf("EVIDENCE run_list=%v fire_workflows=%s", ids, fires[0].WorkflowID)
	s.writeProof(t, "schedule-fire-not-a-run.txt")
}

// TestIntegration_ScheduleIntervalFiresOnItsOwn: an interval of 5s fires on
// its own, epoch-aligned.
func TestIntegration_ScheduleIntervalFiresOnItsOwn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "interval", 1)
	s.apply(t, scheduleOrkflowYAML("hello", "0.1.0", 0, ""))
	id := s.create(t, "every5", "--orkflow", "hello", "--interval", "5s", "--overlap", "allow-all")
	runs := s.awaitRuns(t, id, 60*time.Second, func(r []runRow) bool { return len(r) >= 2 })
	s.cli(t, s.a, exitOK, "schedule", "delete", "every5")
	var refs []time.Time
	for _, listed := range runs {
		// A list row carries the origin's kind and id; the reference is on
		// the run itself.
		run := s.run(t, s.a, listed.ID)
		ref, err := time.Parse(time.RFC3339, run.Origin.Reference)
		if err != nil {
			t.Fatalf("run %s reference %q: %v", run.ID, run.Origin.Reference, err)
		}
		if ref.Unix()%5 != 0 {
			t.Fatalf("run %s nominal time %s is not aligned to 5s from the epoch", run.ID, ref)
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Before(refs[j]) })
	for i := 1; i < len(refs); i++ {
		if d := refs[i].Sub(refs[i-1]); d <= 0 || d%(5*time.Second) != 0 {
			t.Fatalf("references %v differ by %s, not a multiple of 5s", refs, d)
		}
	}
	t.Logf("EVIDENCE interval_references=%v", refs)
	s.writeProof(t, "schedule-interval.txt")
}

// TestIntegration_ScheduleOverlapSkip: a 40s run and a 5s interval under skip
// start exactly one run while it executes, and the cluster counts the skipped
// fires.
//
// "Exactly one run" is a negative claim, so the window it is observed over
// carries its own positive control: it ends when the cluster has counted two
// skipped fires, never after a fixed sleep. A host too slow to fire at all
// cannot make it pass, because then the window never closes and the test
// fails at its deadline.
func TestIntegration_ScheduleOverlapSkip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "skip", 1)
	s.apply(t, scheduleOrkflowYAML("slow", "0.1.0", 40, ""))
	id := s.create(t, "every5", "--orkflow", "slow", "--interval", "5s", "--overlap", "skip")
	first := s.awaitRuns(t, id, 60*time.Second, func(r []runRow) bool { return len(r) >= 1 })[0]
	s.awaitSchedule(t, "every5", 60*time.Second, func(g scheduleJSON) bool { return g.Schedule.FiresSkippedOverlap >= 2 })
	runs := s.runsOf(t, id)
	got := s.get(t, s.a, "every5")
	s.cli(t, s.a, exitOK, "schedule", "delete", "every5")
	if len(runs) != 1 || runs[0].ID != first.ID || (runs[0].Status != "RUNNING" && runs[0].Status != "SUCCEEDED") {
		t.Fatalf("runs after two skipped fires under skip = %+v, want exactly %s", runs, first.ID)
	}
	if got.Schedule.FiresSkippedOverlap < 2 {
		t.Fatalf("fires_skipped_overlap = %d, want at least 2", got.Schedule.FiresSkippedOverlap)
	}
	t.Logf("EVIDENCE overlap_skip runs=%d fires_skipped_overlap=%d", len(runs), got.Schedule.FiresSkippedOverlap)
	s.writeProof(t, "schedule-overlap-skip.txt")
}

// TestIntegration_ScheduleOverlapCancelOther: under cancel-other a due fire
// cancels the earlier run through the ordinary cancellation path, and the new
// run runs. The fires are manual (the schedule's own policy applies to them)
// so which run is "first" is certain.
func TestIntegration_ScheduleOverlapCancelOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "cancel", 1)
	s.apply(t, scheduleOrkflowYAML("slow", "0.1.0", 45, ""))
	id := s.create(t, "nightly", "--orkflow", "slow", "--cron", "0 2 * * *", "--overlap", "cancel-other", "--paused")
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	first := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 && r[0].Status == "RUNNING" })[0]
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	s.awaitRunStatus(t, first.ID, 2*time.Minute, "CANCELED")
	runs := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 2 })
	second := runs[1]
	if second.ID == first.ID {
		t.Fatalf("no second run: %+v", runs)
	}
	s.awaitRunStatus(t, second.ID, 30*time.Second, "RUNNING", "SUCCEEDED")
	// The cancelled run's own record, as `run cancel` would leave it.
	s.cli(t, s.a, exitOK, "run", "get", first.ID)
	t.Logf("EVIDENCE overlap_cancel_other first=%s CANCELED second=%s", first.ID, second.ID)
	s.writeProof(t, "schedule-overlap-cancel-other.txt")
}

// TestIntegration_ScheduleOverlapBufferOne: under buffer-one a fire due while
// a run executes waits and starts when that run ends; no third is queued.
func TestIntegration_ScheduleOverlapBufferOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "bufone", 1)
	s.apply(t, scheduleOrkflowYAML("slow", "0.1.0", 12, ""))
	id := s.create(t, "nightly", "--orkflow", "slow", "--cron", "0 2 * * *", "--overlap", "buffer-one", "--paused")
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	first := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 && r[0].Status == "RUNNING" })[0]
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	// While the first run executes, nothing else starts.
	if runs := s.runsOf(t, id); len(runs) != 1 {
		t.Fatalf("a buffered fire started while the first run executed: %+v", runs)
	}
	done := s.awaitRunStatus(t, first.ID, 2*time.Minute, "SUCCEEDED")
	runs := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 2 })
	second := s.awaitRunStatus(t, runs[1].ID, 2*time.Minute, "SUCCEEDED")
	if second.CreatedAt < done.CreatedAt {
		t.Fatalf("the buffered run was created before the first: %+v", runs)
	}
	// No third: the second buffered fire was not queued behind the first.
	// The positive control comes first: the cluster has TAKEN the buffered
	// fire (fires_taken reached 2), which it does the moment the run ahead
	// of it ends — so the scheduler demonstrably acts on its buffer within
	// this window, and a third buffered fire would have been taken when the
	// second run ended, which happened before the window began.
	s.awaitSchedule(t, "nightly", 60*time.Second, func(g scheduleJSON) bool { return g.Schedule.FiresTaken >= 2 })
	time.Sleep(5 * time.Second)
	if runs := s.runsOf(t, id); len(runs) != 2 {
		t.Fatalf("buffer-one queued more than one fire: %+v", runs)
	}
	if taken := s.get(t, s.a, "nightly").Schedule.FiresTaken; taken != 2 {
		t.Fatalf("fires_taken = %d after the second run ended, want 2: buffer-one queued more than one fire", taken)
	}
	t.Logf("EVIDENCE overlap_buffer_one first=%s second=%s runs=2", first.ID, second.ID)
	s.writeProof(t, "schedule-overlap-buffer-one.txt")
}

// TestIntegration_ScheduleOverlapAllowAll: ALLOW_ALL is what the cluster
// holds (a cast of OrkStack's number would have sent TERMINATE_OTHER), and
// two fires run together with neither cancelled nor terminated.
func TestIntegration_ScheduleOverlapAllowAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "allowall", 1)
	s.apply(t, scheduleOrkflowYAML("slow", "0.1.0", 15, ""))
	id := s.create(t, "nightly", "--orkflow", "slow", "--cron", "0 2 * * *", "--overlap", "allow-all", "--paused")

	described, err := s.temporal.ScheduleClient().GetHandle(ctx, engine.TemporalScheduleID(s.namespace, "nightly")).Describe(ctx)
	if err != nil {
		t.Fatalf("describe the Temporal Schedule: %v", err)
	}
	if described.Schedule.Policy.Overlap != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		t.Fatalf("Temporal holds overlap %v, want SCHEDULE_OVERLAP_POLICY_ALLOW_ALL", described.Schedule.Policy.Overlap)
	}
	t.Logf("EVIDENCE described_policy_overlap=%s", described.Schedule.Policy.Overlap)
	fmt.Fprintf(&s.transcript, "$ temporal schedule describe --schedule-id %s\npolicy.overlap = %s\n\n",
		engine.TemporalScheduleID(s.namespace, "nightly"), described.Schedule.Policy.Overlap)

	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	first := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 })[0]
	// The second fire must be a second fire to the cluster: Temporal names a
	// fire's workflow by its nominal SECOND, so a fire in the same second as
	// a still-running one is the same fire and starts nothing.
	s.awaitNextFireSecond(t, s.run(t, s.a, first.ID))
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	both := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool {
		return len(r) == 2 && r[0].Status == "RUNNING" && r[1].Status == "RUNNING"
	})
	t.Logf("EVIDENCE allow_all_concurrent %s=%s %s=%s", both[0].ID, both[0].Status, both[1].ID, both[1].Status)
	for _, run := range both {
		s.awaitRunStatus(t, run.ID, 2*time.Minute, "SUCCEEDED")
	}
	fires := s.awaitFireWorkflows(ctx, t, id, 2)
	if len(fires) != 2 {
		t.Fatalf("fire workflows = %+v", fires)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && s.fireStatus(ctx, t, fires[0].WorkflowID) == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		time.Sleep(500 * time.Millisecond)
	}
	if status := s.fireStatus(ctx, t, fires[0].WorkflowID); status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("the first fire closed %s, want Completed", status)
	}
	t.Logf("EVIDENCE allow_all_first_fire=%s", enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	s.writeProof(t, "schedule-overlap-allow-all.txt")
}

// TestIntegration_ScheduleFireOverlapDefaultsToTheSchedulesPolicy: a fire with
// no --overlap follows the schedule's buffer-one — an override read as skip
// would have dropped it — and --overlap skip overrides it for one fire.
func TestIntegration_ScheduleFireOverlapDefaultsToTheSchedulesPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "override", 1)
	s.apply(t, scheduleOrkflowYAML("slow", "0.1.0", 15, ""))
	id := s.create(t, "nightly", "--orkflow", "slow", "--cron", "0 2 * * *", "--overlap", "buffer-one", "--paused")
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	first := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 && r[0].Status == "RUNNING" })[0]
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	if runs := s.runsOf(t, id); len(runs) != 1 {
		t.Fatalf("a second run started while the first executed: %+v", runs)
	}
	s.awaitRunStatus(t, first.ID, 2*time.Minute, "SUCCEEDED")
	runs := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 2 && r[1].Status == "RUNNING" })
	if got := s.get(t, s.a, "nightly").Schedule.FiresSkippedOverlap; got != 0 {
		t.Fatalf("fires_skipped_overlap = %d after a buffered fire, want 0", got)
	}
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly", "--overlap", "skip")
	skipped := s.awaitSchedule(t, "nightly", 30*time.Second, func(g scheduleJSON) bool { return g.Schedule.FiresSkippedOverlap == 1 })
	s.awaitRunStatus(t, runs[1].ID, 2*time.Minute, "SUCCEEDED")
	final := s.awaitSchedule(t, "nightly", 30*time.Second, func(g scheduleJSON) bool {
		taken := 0
		for _, fire := range g.Schedule.RecentFires {
			if fire.RunID != "" {
				taken++
			}
		}
		return taken == 2
	})
	if len(final.Schedule.RecentFires) != 2 || len(s.runsOf(t, id)) != 2 {
		t.Fatalf("recent fires = %+v, want exactly the two taken", final.Schedule.RecentFires)
	}
	t.Logf("EVIDENCE fire_override buffered_second=%s fires_skipped_overlap=%d recent_fires=%d",
		runs[1].ID, skipped.Schedule.FiresSkippedOverlap, len(final.Schedule.RecentFires))
	s.writeProof(t, "schedule-fire-overlap-default.txt")
}

// TestIntegration_ScheduleBackfillCarriesNominalTimes: a three-minute backfill
// of a per-minute schedule starts three runs whose references are the three
// nominal minutes, none of them the wall clock.
func TestIntegration_ScheduleBackfillCarriesNominalTimes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "backfill", 1)
	s.apply(t, scheduleOrkflowYAML("hello", "0.1.0", 0, ""))
	id := s.create(t, "minutely", "--orkflow", "hello", "--cron", "* * * * *", "--paused")
	start := time.Now().UTC().Truncate(time.Minute).Add(-30 * time.Minute)
	end := start.Add(2 * time.Minute)
	// allow-all for these fires: under the schedule's skip the three would
	// be dropped while the first ran.
	s.cli(t, s.a, exitOK, "schedule", "backfill", "minutely", "--start", start.Format(time.RFC3339),
		"--end", end.Format(time.RFC3339), "--overlap", "allow-all")
	runs := s.awaitRuns(t, id, 2*time.Minute, func(r []runRow) bool { return len(r) >= 3 })
	want := map[string]bool{}
	for i := 0; i < 3; i++ {
		want[start.Add(time.Duration(i)*time.Minute).Format(time.RFC3339)] = true
	}
	var refs []string
	for _, listed := range runs {
		run := s.run(t, s.a, listed.ID)
		refs = append(refs, run.Origin.Reference)
		if !want[run.Origin.Reference] {
			t.Fatalf("run %s reference %s is not one of the nominal minutes %v", run.ID, run.Origin.Reference, want)
		}
	}
	sort.Strings(refs)
	if len(runs) != 3 || refs[0] == refs[1] || refs[1] == refs[2] {
		t.Fatalf("backfill started %d runs with references %v, want the three nominal minutes", len(runs), refs)
	}
	t.Logf("EVIDENCE backfill_references=%v", refs)
	s.writeProof(t, "schedule-backfill.txt")
}

// TestIntegration_ScheduleFireRefusedWithoutLiveOrkerAndRecorded: a fire onto a
// pool no live Orker serves starts no run and records the submit refusal;
// once an Orker serves the pool, the next fire starts a run. With
// pause_on_failure the refusal pauses the schedule and the note says so.
func TestIntegration_ScheduleFireRefusedWithoutLiveOrkerAndRecorded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "refused", 1)
	s.apply(t, scheduleOrkflowYAML("gpuwork", "0.1.0", 0, "gpu"))
	id := s.create(t, "nightly", "--orkflow", "gpuwork", "--cron", "0 2 * * *", "--paused")

	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	refused := s.awaitSchedule(t, "nightly", 90*time.Second, func(g scheduleJSON) bool {
		return len(g.Schedule.RecentFires) == 1 && g.Schedule.RecentFires[0].Refusal != ""
	})
	refusal := refused.Schedule.RecentFires[0].Refusal
	if !strings.Contains(refusal, `"gpu"`) || refused.Schedule.RecentFires[0].RunID != "" {
		t.Fatalf("refusal = %q (run %q), want the submit refusal naming pool gpu", refusal, refused.Schedule.RecentFires[0].RunID)
	}
	human := s.cli(t, s.a, exitOK, "schedule", "get", "nightly")
	if !strings.Contains(human, "REFUSAL") || !strings.Contains(human, refusal) {
		t.Fatalf("schedule get does not show the refusal:\n%s", human)
	}
	t.Logf("EVIDENCE schedule_get_refusal=%q", refusal)
	if runs := s.runsOf(t, id); len(runs) != 0 {
		t.Fatalf("a refused fire started runs: %+v", runs)
	}

	gpuQueue, err := task.QueueForPool("gpu", s.channel)
	if err != nil {
		t.Fatal(err)
	}
	startProcess(ctx, t, "orker-gpu", s.orkerPath,
		"ORK_TEMPORAL_ADDRESS="+s.address,
		"ORK_TEMPORAL_NAMESPACE="+s.partition,
		"ORK_ORKER_POOL=gpu",
		// Its own identity: it shares the work root, and so the derived
		// identity, of the stack's main Orker.
		fmt.Sprintf("ORK_ORKER_ID=orker-gpu-%d", os.Getpid()),
		"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t),
		"ORK_ARTIFACT_BACKEND=s3",
	)
	awaitActivityPoller(ctx, t, s.temporal, gpuQueue)
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	runs := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 })
	s.awaitRunStatus(t, runs[0].ID, 3*time.Minute, "SUCCEEDED")
	t.Logf("EVIDENCE refused_then_ran run=%s", runs[0].ID)

	// pause_on_failure: an active schedule whose fire is refused pauses.
	s.apply(t, scheduleOrkflowYAML("tpuwork", "0.1.0", 0, "tpu"))
	s.create(t, "guarded", "--orkflow", "tpuwork", "--cron", "0 0 1 1 *", "--pause-on-failure")
	s.cli(t, s.a, exitOK, "schedule", "fire", "guarded")
	paused := s.awaitSchedule(t, "guarded", 90*time.Second, func(g scheduleJSON) bool { return g.Schedule.Paused })
	if paused.Schedule.Note == "" {
		t.Fatalf("pause_on_failure paused the schedule with no note: %+v", paused.Schedule)
	}
	t.Logf("EVIDENCE pause_on_failure note=%q", paused.Schedule.Note)
	s.cli(t, s.a, exitOK, "schedule", "get", "guarded")
	s.writeProof(t, "schedule-refused-fire.txt")
}

// TestIntegration_ScheduleFollowsCurrentPointerAtFire: an unpinned schedule
// resolves the current pointer at each fire; a pinned one keeps its version.
func TestIntegration_ScheduleFollowsCurrentPointerAtFire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "pointer", 1)
	s.apply(t, scheduleOrkflowYAML("hello", "0.1.0", 0, ""))
	followID := s.create(t, "follow", "--orkflow", "hello", "--cron", "0 2 * * *", "--paused")
	pinnedID := s.create(t, "pinned", "--orkflow", "hello", "--version", "0.1.0", "--cron", "0 2 * * *", "--paused")

	s.cli(t, s.a, exitOK, "schedule", "fire", "follow")
	first := s.awaitRuns(t, followID, 90*time.Second, func(r []runRow) bool { return len(r) == 1 })[0]
	s.awaitRunStatus(t, first.ID, 3*time.Minute, "SUCCEEDED")

	s.apply(t, scheduleOrkflowYAML("hello", "0.2.0", 0, ""))
	s.cli(t, s.a, exitOK, "orkflow", "promote", "hello", "0.2.0")
	s.cli(t, s.a, exitOK, "schedule", "fire", "follow")
	s.cli(t, s.a, exitOK, "schedule", "fire", "pinned")
	follow := s.awaitRuns(t, followID, 90*time.Second, func(r []runRow) bool { return len(r) == 2 })
	pinned := s.awaitRuns(t, pinnedID, 90*time.Second, func(r []runRow) bool { return len(r) == 1 })
	second := s.run(t, s.a, follow[1].ID)
	pinnedRun := s.run(t, s.a, pinned[0].ID)
	if s.run(t, s.a, first.ID).OrkflowVersion != "0.1.0" || second.OrkflowVersion != "0.2.0" || pinnedRun.OrkflowVersion != "0.1.0" {
		t.Fatalf("versions: first %s, after promotion %s, pinned %s; want 0.1.0, 0.2.0, 0.1.0",
			s.run(t, s.a, first.ID).OrkflowVersion, second.OrkflowVersion, pinnedRun.OrkflowVersion)
	}
	t.Logf("EVIDENCE follows_current first=%s@0.1.0 second=%s@0.2.0 pinned=%s@0.1.0", first.ID, second.ID, pinnedRun.ID)
	s.writeProof(t, "schedule-follows-current.txt")
}

// TestIntegration_ScheduleSurvivesReplicaAndRestart: a schedule created on A
// reads identically on B, a pause on B is seen on A, and after both engines
// restart the schedule fires with nothing re-registered.
func TestIntegration_ScheduleSurvivesReplicaAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "replica", 2)
	s.apply(t, scheduleOrkflowYAML("hello", "0.1.0", 0, ""))
	id := s.create(t, "nightly", "--orkflow", "hello", "--cron", "0 2 * * *", "--timezone", "Europe/Lisbon")

	listA := s.cli(t, s.a, exitOK, "--json", "schedule", "list")
	listB := s.cli(t, s.b, exitOK, "--json", "schedule", "list")
	getA := s.cli(t, s.a, exitOK, "--json", "schedule", "get", "nightly")
	getB := s.cli(t, s.b, exitOK, "--json", "schedule", "get", "nightly")
	if listA != listB || getA != getB {
		t.Fatalf("replicas disagree:\nlist A %s\nlist B %s\nget A %s\nget B %s", listA, listB, getA, getB)
	}
	s.cli(t, s.b, exitOK, "schedule", "pause", "nightly", "--reason", "Q3 close")
	onA := s.get(t, s.a, "nightly")
	if !onA.Schedule.Paused || onA.Schedule.Note != "Q3 close" {
		t.Fatalf("a pause on B reads on A as %+v", onA.Schedule)
	}
	pausedA := s.cli(t, s.a, exitOK, "--json", "schedule", "get", "nightly")
	pausedB := s.cli(t, s.b, exitOK, "--json", "schedule", "get", "nightly")
	if pausedA != pausedB {
		t.Fatalf("paused reads disagree:\nA %s\nB %s", pausedA, pausedB)
	}
	t.Logf("EVIDENCE two_replica_get_json=%s", compactJSON(t, pausedB))

	// Filters answer alike on both replicas: one on the schedule's identity
	// is decided before any describe, one on its state after.
	for _, filter := range [][]string{
		{"--filter", "orkflow_name=hello"},
		{"--filter", "paused=true"},
		{"--filter", "orkflow_name=hello", "--filter", "name=nightly", "--filter", "paused=true"},
	} {
		args := append([]string{"--json", "schedule", "list"}, filter...)
		fromA := s.cli(t, s.a, exitOK, args...)
		fromB := s.cli(t, s.b, exitOK, args...)
		if fromA != fromB || !strings.Contains(fromA, `"nightly"`) {
			t.Fatalf("orkctl %s:\nA %s\nB %s", strings.Join(args, " "), fromA, fromB)
		}
	}
	for _, filter := range []string{"paused=false", "orkflow_name=other"} {
		out := s.cli(t, s.b, exitOK, "schedule", "list", "--filter", filter)
		if !strings.Contains(out, "No schedules match --filter '"+filter+"'.") {
			t.Fatalf("schedule list --filter %s:\n%s", filter, out)
		}
	}
	t.Logf("EVIDENCE schedule list filters answer alike on A and B: orkflow_name=hello, paused=true list nightly; paused=false lists nothing")

	addrA, addrB := s.a.addr, s.b.addr
	s.a.stop()
	s.b.stop()
	s.a = s.startEngine(ctx, t, "ork-A-restarted", addrA)
	s.b = s.startEngine(ctx, t, "ork-B-restarted", addrB)
	fmt.Fprintf(&s.transcript, "# restarted both engines\n\n")
	after := s.get(t, s.a, "nightly")
	if after.Schedule.ID != id || !after.Schedule.Paused || after.Schedule.Note != "Q3 close" {
		t.Fatalf("after restart the schedule reads %+v", after.Schedule)
	}
	s.cli(t, s.b, exitOK, "schedule", "resume", "nightly")
	s.cli(t, s.b, exitOK, "schedule", "fire", "nightly")
	runs := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 })
	s.awaitRunStatus(t, runs[0].ID, 3*time.Minute, "SUCCEEDED")
	t.Logf("EVIDENCE fired_after_restart run=%s", runs[0].ID)
	s.writeProof(t, "schedule-two-replica-restart.txt")
}

// TestIntegration_TerminatedFireLeavesItsRunRunning: terminating a fire (which
// runs no workflow code) leaves its run executing under ABANDON, and the run
// finishes SUCCEEDED.
func TestIntegration_TerminatedFireLeavesItsRunRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "terminate", 1)
	s.apply(t, scheduleOrkflowYAML("slow", "0.1.0", 10, ""))
	id := s.create(t, "nightly", "--orkflow", "slow", "--cron", "0 2 * * *", "--paused")
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	run := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 && r[0].Status == "RUNNING" })[0]
	fires := s.awaitFireWorkflows(ctx, t, id, 1)
	if len(fires) != 1 {
		t.Fatalf("fire workflows = %+v", fires)
	}
	if err := s.temporal.TerminateWorkflow(ctx, fires[0].WorkflowID, "", "integration test terminates the fire"); err != nil {
		t.Fatalf("terminate fire: %v", err)
	}
	fmt.Fprintf(&s.transcript, "$ temporal workflow terminate --workflow-id %s\n\n", fires[0].WorkflowID)
	if status := s.fireStatus(ctx, t, fires[0].WorkflowID); status != enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		t.Fatalf("fire status = %s, want Terminated", status)
	}
	finished := s.awaitRunStatus(t, run.ID, 3*time.Minute, "SUCCEEDED", "FAILED", "CANCELED")
	if finished.Status != "SUCCEEDED" {
		t.Fatalf("the run of a terminated fire ended %s, want SUCCEEDED", finished.Status)
	}
	got := s.get(t, s.a, "nightly")
	if len(got.Schedule.RecentFires) != 1 || got.Schedule.RecentFires[0].RunID != run.ID || !got.Schedule.RecentFires[0].DetailsAvailable {
		t.Fatalf("recent fires after termination = %+v", got.Schedule.RecentFires)
	}
	t.Logf("EVIDENCE terminated_fire=%s run=%s status=%s", fires[0].WorkflowID, run.ID, finished.Status)
	s.writeProof(t, "schedule-terminated-fire.txt")
}

// TestIntegration_RunCancelEndsItsFire: `run cancel` on a scheduled run ends
// the run CANCELED and closes the fire with it.
func TestIntegration_RunCancelEndsItsFire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "runcancel", 1)
	s.apply(t, scheduleOrkflowYAML("slow", "0.1.0", 60, ""))
	id := s.create(t, "nightly", "--orkflow", "slow", "--cron", "0 2 * * *", "--paused")
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	run := s.awaitRuns(t, id, 90*time.Second, func(r []runRow) bool { return len(r) == 1 && r[0].Status == "RUNNING" })[0]
	s.cli(t, s.a, exitOK, "run", "cancel", run.ID)
	s.awaitRunStatus(t, run.ID, 2*time.Minute, "CANCELED")
	fires := s.awaitFireWorkflows(ctx, t, id, 1)
	if len(fires) != 1 {
		t.Fatalf("fire workflows = %+v", fires)
	}
	deadline := time.Now().Add(60 * time.Second)
	status := s.fireStatus(ctx, t, fires[0].WorkflowID)
	for status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		status = s.fireStatus(ctx, t, fires[0].WorkflowID)
	}
	if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the fire of a cancelled run is still running")
	}
	t.Logf("EVIDENCE run_cancel run=%s CANCELED fire=%s closed=%s", run.ID, fires[0].WorkflowID, status)
	s.writeProof(t, "schedule-run-cancel.txt")
}

// TestIntegration_DeleteOrkflowRefusedWhileScheduled: `orkflow delete` is
// refused while a schedule names the orkflow, and succeeds once it is gone.
func TestIntegration_DeleteOrkflowRefusedWhileScheduled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "delorkflow", 1)
	s.apply(t, scheduleOrkflowYAML("hello", "0.1.0", 0, ""))
	s.create(t, "nightly", "--orkflow", "hello", "--cron", "0 2 * * *", "--paused")
	_, stderr, code := s.cliRaw(t, s.a, "orkflow", "delete", "hello", "--yes")
	if code == exitOK || !strings.Contains(stderr, "failed_precondition") || !strings.Contains(stderr, "nightly") {
		t.Fatalf("orkflow delete while scheduled = exit %d: %s", code, stderr)
	}
	t.Logf("EVIDENCE orkflow_delete_refused=%q", strings.TrimSpace(stderr))
	s.cli(t, s.a, exitOK, "schedule", "delete", "nightly")
	_, stderr, code = s.cliRaw(t, s.a, "schedule", "delete", "nightly")
	if code == exitOK || !strings.Contains(stderr, "not_found") {
		t.Fatalf("repeat schedule delete = exit %d: %s", code, stderr)
	}
	s.cli(t, s.a, exitOK, "orkflow", "delete", "hello", "--yes")
	s.writeProof(t, "schedule-orkflow-delete.txt")
}
