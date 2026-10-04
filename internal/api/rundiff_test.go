package api_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests describe DiffRuns from a replica that started neither run: the
// runs come from the reader (the cluster), the pinned definitions from the
// orkflow store and the attempt records from the attempt store, and every one
// of those three is counted, because "a narrowed diff reads less" is part of
// the contract and only a counter can see it.

var diffT0 = time.Date(2026, 9, 21, 3, 0, 0, 0, time.UTC)

const (
	diffDigestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	diffDigestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	diffDigestX = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
	diffImageP  = "ghcr.io/acme/transform@sha256:ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12"
	diffImageQ  = "ghcr.io/acme/transform@sha256:ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90"
	diffPlatD   = "sha256:d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0"
	diffPlatE   = "sha256:e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"
	diffPlatX   = "sha256:0badbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbad"
)

var allDiffFields = []string{
	"definition", "input", "presence", "status", "attempt", "pool", "pin", "orker",
	"exit_code", "error", "duration", "image", "platform_digest",
}

// snapshotFields are the task fields answered from the two snapshots alone.
var snapshotFields = []string{
	"status", "attempt", "pool", "pin", "orker", "exit_code", "error", "duration",
}

// diffOrkflowStore is the memory orkflow store with every Get counted and an
// injectable failure.
type diffOrkflowStore struct {
	*engine.MemoryOrkflowStore
	mu     sync.Mutex
	gets   int
	getErr error
}

func (s *diffOrkflowStore) Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	s.mu.Lock()
	s.gets++
	err := s.getErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.MemoryOrkflowStore.Get(ctx, namespace, name, version)
}

func (s *diffOrkflowStore) getCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

// memoryAttemptStore is cmd/orkctl's cliAttemptStore shape, with every
// ListAttempts counted per (run, task) and an injectable failure.
type memoryAttemptStore struct {
	mu      sync.Mutex
	records map[string][]*orkstackv1.TaskAttempt
	lists   map[string]int
	listErr error
}

func newMemoryAttemptStore() *memoryAttemptStore {
	return &memoryAttemptStore{records: map[string][]*orkstackv1.TaskAttempt{}, lists: map[string]int{}}
}

func (s *memoryAttemptStore) PutAttempt(_ context.Context, attempt *orkstackv1.TaskAttempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := attempt.GetRunId() + "/" + attempt.GetTaskId()
	s.records[key] = append(s.records[key], proto.Clone(attempt).(*orkstackv1.TaskAttempt))
	slices.SortFunc(s.records[key], func(a, b *orkstackv1.TaskAttempt) int {
		return int(a.GetAttempt() - b.GetAttempt())
	})
	return nil
}

func (s *memoryAttemptStore) ListAttempts(_ context.Context, runID, taskID string, _ artifacts.Item) ([]*orkstackv1.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists[runID+"/"+taskID]++
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]*orkstackv1.TaskAttempt, 0, len(s.records[runID+"/"+taskID]))
	for _, record := range s.records[runID+"/"+taskID] {
		out = append(out, proto.Clone(record).(*orkstackv1.TaskAttempt))
	}
	return out, nil
}

// remove deletes one record, as the cleanup worker or a crash would leave it.
func (s *memoryAttemptStore) remove(runID, taskID string, attempt int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := runID + "/" + taskID
	s.records[key] = slices.DeleteFunc(s.records[key], func(r *orkstackv1.TaskAttempt) bool {
		return r.GetAttempt() == attempt
	})
}

func (s *memoryAttemptStore) listCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, n := range s.lists {
		total += n
	}
	return total
}

func (s *memoryAttemptStore) listCallsFor(runID, taskID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists[runID+"/"+taskID]
}

type diffFixture struct {
	svc      *api.Service
	reader   *fakeReader
	orkflows *diffOrkflowStore
	attempts *memoryAttemptStore
}

type diffOption func(*api.Config)

// withoutAttemptStore is a deployment that never wired one.
func withoutAttemptStore(cfg *api.Config) { cfg.Attempts = nil }

func newDiffFixture(t *testing.T, runs []*orkstackv1.Run, options ...diffOption) *diffFixture {
	t.Helper()
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{}}
	for _, run := range runs {
		reader.runs[run.GetId()] = run
	}
	orkflows := &diffOrkflowStore{MemoryOrkflowStore: engine.NewMemoryOrkflowStore()}
	attempts := newMemoryAttemptStore()
	cfg := api.Config{
		Reader:   reader,
		Runs:     engine.NewRunStore(),
		Orkflows: orkflows,
		Attempts: attempts,
		Namespaces: registry{
			"default": {Name: "default"},
			"alpha":   {Name: "alpha"},
			"bravo":   {Name: "bravo"},
		},
		Starter: &fakeStarter{},
	}
	for _, option := range options {
		option(&cfg)
	}
	return &diffFixture{svc: api.NewService(cfg), reader: reader, orkflows: orkflows, attempts: attempts}
}

func (f *diffFixture) diff(from, to, namespace string, only ...string) (*orkstackv1.DiffRunsResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := f.svc.DiffRuns(ctx, connect.NewRequest(&orkstackv1.DiffRunsRequest{
		FromRunId: from, ToRunId: to, Namespace: namespace, Only: only,
	}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (f *diffFixture) mustDiff(t *testing.T, from, to string, only ...string) *orkstackv1.DiffRunsResponse {
	t.Helper()
	res, err := f.diff(from, to, "", only...)
	if err != nil {
		t.Fatalf("DiffRuns(%s, %s, only=%v): %v", from, to, only, err)
	}
	return res
}

// define stores one pinned definition: every named task runs image.
func (f *diffFixture) define(t *testing.T, name, version, digest string, images map[string]string) {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: name, Version: version, DefinitionDigest: digest, SourceYaml: name + "@" + version}
	for _, id := range slices.Sorted(mapKeys(images)) {
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: id, Image: &orkstackv1.ImageRef{Pinned: images[id]}})
	}
	if _, _, err := f.orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("store definition %s@%s: %v", name, version, err)
	}
}

// record files one attempt record, as the Orker that ran it would.
func (f *diffFixture) record(t *testing.T, runID, taskID string, attempt int32, edit ...func(*orkstackv1.TaskAttempt)) {
	t.Helper()
	record := &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion,
		RunId:         runID,
		TaskId:        taskID,
		Attempt:       attempt,
		Outcome:       orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		OrkerId:       "orker-a",
		Image:         &orkstackv1.ImageRef{Pinned: diffImageP, PlatformDigest: diffPlatD},
	}
	for _, e := range edit {
		e(record)
	}
	if err := f.attempts.PutAttempt(context.Background(), record); err != nil {
		t.Fatalf("put attempt: %v", err)
	}
}

func mapKeys(m map[string]string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

// diffTask is a task that reached an Orker and succeeded on its first try in
// 1m3s. Tests change only what they compare.
func diffTask(id string, edit ...func(*orkstackv1.TaskRun)) *orkstackv1.TaskRun {
	task := &orkstackv1.TaskRun{
		TaskId:     id,
		Status:     orkstackv1.Status_STATUS_SUCCEEDED,
		Attempt:    1,
		Pool:       "main",
		OrkerId:    "orker-a",
		StartedAt:  timestamppb.New(diffT0),
		FinishedAt: timestamppb.New(diffT0.Add(63 * time.Second)),
	}
	for _, e := range edit {
		e(task)
	}
	return task
}

func diffInput(t *testing.T, values map[string]any) *orkstackv1.Payload {
	t.Helper()
	data, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	return &orkstackv1.Payload{Data: data}
}

// diffRun is a finished run of nightly-etl@1.2.0 pinned to diffDigestA.
func diffRun(id string, edit func(*orkstackv1.Run), tasks ...*orkstackv1.TaskRun) *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:                      id,
		OrkflowName:             "nightly-etl",
		OrkflowVersion:          "1.2.0",
		OrkflowDefinitionDigest: diffDigestA,
		Status:                  orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt:               timestamppb.New(diffT0),
		StartedAt:               timestamppb.New(diffT0),
		FinishedAt:              timestamppb.New(diffT0.Add(5 * time.Minute)),
		TaskRuns:                tasks,
	}
	if edit != nil {
		edit(run)
	}
	return run
}

// side renders one side of a row: its value, "-" for available-empty, and
// "!reason" for unavailable, so a row reads as one comparable string.
func side(value string, available bool, reason string) string {
	switch {
	case !available:
		return "!" + reason
	case value == "":
		return "-"
	default:
		return value
	}
}

func rowsOf(res *orkstackv1.DiffRunsResponse) []string {
	out := make([]string, 0, len(res.GetDifferences()))
	for _, row := range res.GetDifferences() {
		task := row.GetTaskId()
		if task == "" {
			task = "(run)"
		}
		out = append(out, fmt.Sprintf("%s %s %s | %s", task, row.GetField(),
			side(row.GetFromValue(), row.GetFromAvailable(), row.GetFromReason()),
			side(row.GetToValue(), row.GetToAvailable(), row.GetToReason())))
	}
	return out
}

func requireRows(t *testing.T, res *orkstackv1.DiffRunsResponse, want ...string) {
	t.Helper()
	got := rowsOf(res)
	if want == nil {
		want = []string{}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("rows =\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func requireDiffCode(t *testing.T, err error, code connect.Code, contains ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("DiffRuns succeeded; want %v", code)
	}
	if got := connect.CodeOf(err); got != code {
		t.Fatalf("code = %v (%v), want %v", got, err, code)
	}
	for _, want := range contains {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
}

// D1
func TestDiffRuns_RefusesMissingOrIdenticalIDsAndUnknownFields(t *testing.T) {
	f := newDiffFixture(t, []*orkstackv1.Run{diffRun("ork-a", nil), diffRun("ork-b", nil)})
	cases := []struct {
		name     string
		from, to string
		only     []string
		want     string
	}{
		{"missing from", "", "ork-b", nil, "from_run_id is required"},
		{"missing to", "ork-a", "", nil, "to_run_id is required"},
		{"same run", "ork-a", "ork-a", nil, `from_run_id and to_run_id name the same run "ork-a"`},
		{"unknown field", "ork-a", "ork-b", []string{"images"},
			`unknown diff field "images"; valid fields: ` + strings.Join(allDiffFields, ", ")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.diff(tc.from, tc.to, "", tc.only...)
			requireDiffCode(t, err, connect.CodeInvalidArgument, tc.want)
		})
	}
	if reads := f.reader.runReads(); reads != 0 {
		t.Fatalf("a refused request read %d runs; it must be refused before any read", reads)
	}
}

// D2
func TestDiffRuns_AnotherNamespacesRunIsNotFound(t *testing.T) {
	a := diffRun("ork-alpha-run", func(r *orkstackv1.Run) { r.Namespace = "alpha" }, diffTask("work"))
	b := diffRun("ork-bravo-run", func(r *orkstackv1.Run) { r.Namespace = "bravo" }, diffTask("work"))
	f := newDiffFixture(t, []*orkstackv1.Run{a, b})

	_, err := f.diff(a.GetId(), b.GetId(), "alpha")
	requireDiffCode(t, err, connect.CodeNotFound, "to run", `"ork-bravo-run"`, `"alpha"`)
	if reads := f.reader.runReads(); reads != 2 {
		t.Fatalf("GetRun calls = %d, want 2 (both sides resolved, the second refused)", reads)
	}

	_, err = f.diff(a.GetId(), b.GetId(), "bravo")
	requireDiffCode(t, err, connect.CodeNotFound, "from run", `"ork-alpha-run"`, `"bravo"`)
	if reads := f.reader.runReads(); reads != 3 {
		t.Fatalf("GetRun calls = %d, want 3 (the refused from side stops the request)", reads)
	}
	if gets, lists := f.orkflows.getCalls(), f.attempts.listCalls(); gets != 0 || lists != 0 {
		t.Fatalf("a NotFound diff read the orkflow store %d times and the attempt store %d times; want 0 and 0", gets, lists)
	}
}

// fullyRecordedFixture is two runs whose definitions and every outcome
// attempt record are stored, so every field of the full set answers.
func fullyRecordedFixture(t *testing.T, from, to *orkstackv1.Run, options ...diffOption) *diffFixture {
	t.Helper()
	f := newDiffFixture(t, []*orkstackv1.Run{from, to}, options...)
	for _, run := range []*orkstackv1.Run{from, to} {
		images := map[string]string{}
		for _, task := range run.GetTaskRuns() {
			images[task.GetTaskId()] = diffImageP
		}
		if _, err := f.orkflows.MemoryOrkflowStore.Get(context.Background(), "", run.GetOrkflowName(), run.GetOrkflowVersion()); err != nil {
			f.define(t, run.GetOrkflowName(), run.GetOrkflowVersion(), run.GetOrkflowDefinitionDigest(), images)
		}
		for _, task := range run.GetTaskRuns() {
			if task.GetAttempt() > 0 && task.GetOrkerId() != "" {
				f.record(t, run.GetId(), task.GetTaskId(), task.GetAttempt())
			}
		}
	}
	return f
}

// D3
func TestDiffRuns_IdenticalRunsYieldNoRowsAndCountEveryTaskIdentical(t *testing.T) {
	later := func(r *orkstackv1.Run) {
		r.CreatedAt = timestamppb.New(diffT0.Add(24 * time.Hour))
		r.StartedAt = timestamppb.New(diffT0.Add(24 * time.Hour))
		r.FinishedAt = timestamppb.New(diffT0.Add(24*time.Hour + 5*time.Minute))
	}
	shifted := func(task *orkstackv1.TaskRun) {
		task.StartedAt = timestamppb.New(diffT0.Add(24 * time.Hour))
		task.FinishedAt = timestamppb.New(diffT0.Add(24*time.Hour + 63*time.Second))
	}
	from := diffRun("ork-mon", func(r *orkstackv1.Run) { r.Input = diffInput(t, map[string]any{"n": 5}) },
		diffTask("extract"), diffTask("transform"), diffTask("load"))
	to := diffRun("ork-tue", func(r *orkstackv1.Run) { later(r); r.Input = diffInput(t, map[string]any{"n": 5.0}) },
		diffTask("extract", shifted), diffTask("transform", shifted), diffTask("load", shifted))
	f := fullyRecordedFixture(t, from, to)

	res := f.mustDiff(t, from.GetId(), to.GetId())
	requireRows(t, res)
	if res.GetInputUnifiedDiff() != "" {
		t.Fatalf("input diff of equal inputs = %q", res.GetInputUnifiedDiff())
	}
	if res.GetTasksCompared() != 3 || res.GetTasksIdentical() != 3 || res.GetEvidenceUnavailable() != 0 {
		t.Fatalf("counts compared=%d identical=%d unavailable=%d, want 3 3 0",
			res.GetTasksCompared(), res.GetTasksIdentical(), res.GetEvidenceUnavailable())
	}
	if !res.GetSameOrkflow() || !res.GetSameDefinitionDigest() {
		t.Fatalf("same_orkflow=%v same_definition_digest=%v, want both true", res.GetSameOrkflow(), res.GetSameDefinitionDigest())
	}
	if !slices.Equal(res.GetFieldsCompared(), allDiffFields) {
		t.Fatalf("fields_compared = %v, want %v", res.GetFieldsCompared(), allDiffFields)
	}
}

// d4Runs is D4's fixture: three tasks, and between them every snapshot field
// differs once.
func d4Runs() (*orkstackv1.Run, *orkstackv1.Run) {
	from := diffRun("ork-mon", nil,
		diffTask("extract"),
		diffTask("transform"),
		diffTask("load"),
	)
	to := diffRun("ork-tue", func(r *orkstackv1.Run) { r.Status = orkstackv1.Status_STATUS_FAILED },
		diffTask("extract", func(task *orkstackv1.TaskRun) {
			task.Pool = "gpu"
			task.FinishedAt = timestamppb.New(diffT0.Add(4*time.Minute + 12*time.Second))
		}),
		diffTask("transform", func(task *orkstackv1.TaskRun) {
			task.Status = orkstackv1.Status_STATUS_FAILED
			task.Attempt = 3
			task.ExitCode = 3
			task.Error = &orkstackv1.Error{
				Code:     "CONTAINER_EXIT_NONZERO",
				Message:  "container exited 3 after 63s on orker-a",
				Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
			}
		}),
		diffTask("load", func(task *orkstackv1.TaskRun) {
			task.Pin = "p1"
			task.OrkerId = "orker-b"
		}),
	)
	return from, to
}

// D4
func TestDiffRuns_OneRowPerDifferingTaskField(t *testing.T) {
	from, to := d4Runs()
	f := newDiffFixture(t, []*orkstackv1.Run{from, to})
	res := f.mustDiff(t, from.GetId(), to.GetId(), snapshotFields...)
	requireRows(t, res,
		"extract pool main | gpu",
		"extract duration 1m3s | 4m12s",
		"transform status SUCCEEDED | FAILED",
		"transform attempt 1 | 3",
		"transform exit_code 0 | 3",
		"transform error - | CONTAINER_EXIT_NONZERO (ACTIVITY_FAILED)",
		"load pin - | p1",
		"load orker orker-a | orker-b",
	)

	// A message that differs while the code and category are equal is not a
	// difference: it carries ids and timings.
	failed := func(message string) func(*orkstackv1.TaskRun) {
		return func(task *orkstackv1.TaskRun) {
			task.Status = orkstackv1.Status_STATUS_FAILED
			task.Error = &orkstackv1.Error{Code: "CONTAINER_EXIT_NONZERO", Message: message}
		}
	}
	a := diffRun("ork-msg-a", nil, diffTask("work", failed("exited 1 at 03:00:01")))
	b := diffRun("ork-msg-b", nil, diffTask("work", failed("exited 1 at 03:07:44")))
	g := newDiffFixture(t, []*orkstackv1.Run{a, b})
	res = g.mustDiff(t, a.GetId(), b.GetId(), snapshotFields...)
	requireRows(t, res)
}

// D5
func TestDiffRuns_InputDiffIsCanonicalAndLabelled(t *testing.T) {
	from := diffRun("ork-mon", func(r *orkstackv1.Run) {
		r.Input = diffInput(t, map[string]any{"date": "2026-09-21", "n": 5.0})
	}, diffTask("work"))
	to := diffRun("ork-tue", func(r *orkstackv1.Run) {
		r.Input = diffInput(t, map[string]any{"n": 5, "date": "2026-09-22"})
	}, diffTask("work"))
	f := newDiffFixture(t, []*orkstackv1.Run{from, to})
	res := f.mustDiff(t, from.GetId(), to.GetId())

	want := "--- a/ork-mon/input.json\n" +
		"+++ b/ork-tue/input.json\n" +
		"@@ -1,4 +1,4 @@\n" +
		" {\n" +
		"-  \"date\": \"2026-09-21\",\n" +
		"+  \"date\": \"2026-09-22\",\n" +
		"   \"n\": 5\n" +
		" }\n"
	if got := res.GetInputUnifiedDiff(); got != want {
		t.Fatalf("input diff =\n%s\nwant\n%s", got, want)
	}
	for _, row := range res.GetDifferences() {
		if row.GetField() == "input" {
			t.Fatalf("input produced a row: %v", row)
		}
	}
}

// D6
func TestDiffRuns_DefinitionRowAndDigestFlag(t *testing.T) {
	cases := []struct {
		name       string
		version    string
		digest     string
		wantRows   []string
		wantDigest bool
	}{
		{"same version and digest", "1.2.0", diffDigestA, nil, true},
		{"version moved, same digest", "1.2.1", diffDigestA,
			[]string{"(run) definition nightly-etl@1.2.0 " + diffDigestA + " | nightly-etl@1.2.1 " + diffDigestA}, true},
		{"different digest", "1.2.1", diffDigestB,
			[]string{"(run) definition nightly-etl@1.2.0 " + diffDigestA + " | nightly-etl@1.2.1 " + diffDigestB}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			from := diffRun("ork-mon", nil, diffTask("work"))
			to := diffRun("ork-tue", func(r *orkstackv1.Run) {
				r.OrkflowVersion = tc.version
				r.OrkflowDefinitionDigest = tc.digest
			}, diffTask("work"))
			f := newDiffFixture(t, []*orkstackv1.Run{from, to})
			res := f.mustDiff(t, from.GetId(), to.GetId(), "definition")
			requireRows(t, res, tc.wantRows...)
			if res.GetSameDefinitionDigest() != tc.wantDigest {
				t.Fatalf("same_definition_digest = %v, want %v", res.GetSameDefinitionDigest(), tc.wantDigest)
			}
		})
	}
}

// D7
func TestDiffRuns_ImageComesFromTheDefinitionThenTheAttemptThenIsUnavailable(t *testing.T) {
	newRuns := func() (*orkstackv1.Run, *orkstackv1.Run) {
		from := diffRun("ork-mon", nil, diffTask("work"))
		to := diffRun("ork-tue", func(r *orkstackv1.Run) {
			r.OrkflowVersion = "1.2.1"
			r.OrkflowDefinitionDigest = diffDigestB
		}, diffTask("work"))
		return from, to
	}

	t.Run("a: both definitions stored", func(t *testing.T) {
		from, to := newRuns()
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.define(t, "nightly-etl", "1.2.0", diffDigestA, map[string]string{"work": diffImageP})
		f.define(t, "nightly-etl", "1.2.1", diffDigestB, map[string]string{"work": diffImageQ})
		f.record(t, from.GetId(), "work", 1)
		res := f.mustDiff(t, from.GetId(), to.GetId(), "image")
		requireRows(t, res, "work image "+diffImageP+" | "+diffImageQ)
		if lists := f.attempts.listCalls(); lists != 0 {
			t.Fatalf("attempt store read %d times with platform_digest excluded and both definitions stored; want 0", lists)
		}
		t.Logf("D7(a) attempt-store calls = %d", f.attempts.listCalls())
	})

	t.Run("b: from definition deleted, attempt record answers", func(t *testing.T) {
		from, to := newRuns()
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.define(t, "nightly-etl", "1.2.1", diffDigestB, map[string]string{"work": diffImageQ})
		f.record(t, from.GetId(), "work", 1)
		res := f.mustDiff(t, from.GetId(), to.GetId(), "image")
		requireRows(t, res, "work image "+diffImageP+" | "+diffImageQ)
	})

	t.Run("c: from definition deleted, no record", func(t *testing.T) {
		from, to := newRuns()
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.define(t, "nightly-etl", "1.2.1", diffDigestB, map[string]string{"work": diffImageQ})
		res := f.mustDiff(t, from.GetId(), to.GetId(), "image")
		requireRows(t, res, "work image !definition_unavailable | "+diffImageQ)
		if res.GetEvidenceUnavailable() != 1 || res.GetTasksIdentical() != 0 {
			t.Fatalf("evidence_unavailable=%d tasks_identical=%d, want 1 and 0", res.GetEvidenceUnavailable(), res.GetTasksIdentical())
		}
	})

	t.Run("d: stored document does not hash to the run's digest", func(t *testing.T) {
		from, to := newRuns()
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.define(t, "nightly-etl", "1.2.0", diffDigestX, map[string]string{"work": diffImageQ})
		f.define(t, "nightly-etl", "1.2.1", diffDigestB, map[string]string{"work": diffImageQ})
		res := f.mustDiff(t, from.GetId(), to.GetId(), "image")
		requireRows(t, res, "work image !definition_digest_mismatch | "+diffImageQ)

		f.record(t, from.GetId(), "work", 1)
		res = f.mustDiff(t, from.GetId(), to.GetId(), "image")
		requireRows(t, res, "work image "+diffImageP+" | "+diffImageQ)
	})
}

// D8
func TestDiffRuns_PlatformDigestFromTheOutcomeAttemptRecord(t *testing.T) {
	withDigest := func(digest string) func(*orkstackv1.TaskAttempt) {
		return func(r *orkstackv1.TaskAttempt) { r.Image.PlatformDigest = digest }
	}
	failed := func(r *orkstackv1.TaskAttempt) {
		r.Outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
	}
	adopted := func(r *orkstackv1.TaskAttempt) { r.CommitAdopted = true }
	pair := func(fromAttempt int32) (*orkstackv1.Run, *orkstackv1.Run) {
		from := diffRun("ork-mon", nil, diffTask("work", func(task *orkstackv1.TaskRun) { task.Attempt = fromAttempt }))
		to := diffRun("ork-tue", nil, diffTask("work"))
		return from, to
	}

	t.Run("a: the outcome attempt's record", func(t *testing.T) {
		from, to := pair(3)
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.record(t, from.GetId(), "work", 1, failed, withDigest(diffPlatX))
		f.record(t, from.GetId(), "work", 2, failed, withDigest(diffPlatX))
		f.record(t, from.GetId(), "work", 3, withDigest(diffPlatD))
		f.record(t, to.GetId(), "work", 1, withDigest(diffPlatE))
		requireRows(t, f.mustDiff(t, from.GetId(), to.GetId(), "platform_digest"),
			"work platform_digest "+diffPlatD+" | "+diffPlatE)
	})

	t.Run("b: the outcome attempt left no record", func(t *testing.T) {
		from, to := pair(2)
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.record(t, from.GetId(), "work", 1, failed, withDigest(diffPlatX))
		f.record(t, from.GetId(), "work", 3, withDigest(diffPlatX))
		f.record(t, to.GetId(), "work", 1)
		res := f.mustDiff(t, from.GetId(), to.GetId(), "platform_digest")
		requireRows(t, res, "work platform_digest !attempt_record_missing | "+diffPlatD)
		t.Logf("D8(b) rows: %v", rowsOf(res))
	})

	t.Run("c: no outcome attempt", func(t *testing.T) {
		from, to := pair(0)
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.record(t, to.GetId(), "work", 1)
		requireRows(t, f.mustDiff(t, from.GetId(), to.GetId(), "platform_digest"),
			"work platform_digest !no_outcome_attempt | "+diffPlatD)
	})

	t.Run("d: no attempt store, every other field still compared", func(t *testing.T) {
		from, to := pair(1)
		to.TaskRuns[0].Status = orkstackv1.Status_STATUS_FAILED
		f := newDiffFixture(t, []*orkstackv1.Run{from, to}, withoutAttemptStore)
		f.define(t, "nightly-etl", "1.2.0", diffDigestA, map[string]string{"work": diffImageP})
		requireRows(t, f.mustDiff(t, from.GetId(), to.GetId()),
			"work status SUCCEEDED | FAILED",
			"work platform_digest !no_attempt_store | !no_attempt_store")
	})

	t.Run("e: the record has no platform digest", func(t *testing.T) {
		from, to := pair(1)
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.record(t, from.GetId(), "work", 1, withDigest(""))
		f.record(t, to.GetId(), "work", 1)
		requireRows(t, f.mustDiff(t, from.GetId(), to.GetId(), "platform_digest"),
			"work platform_digest - | "+diffPlatD)
	})

	t.Run("f: never reached an Orker", func(t *testing.T) {
		from, to := pair(1)
		from.TaskRuns[0].OrkerId = ""
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.record(t, from.GetId(), "work", 1)
		f.record(t, to.GetId(), "work", 1)
		requireRows(t, f.mustDiff(t, from.GetId(), to.GetId(), "platform_digest"),
			"work platform_digest - | "+diffPlatD)
		if n := f.attempts.listCallsFor(from.GetId(), "work"); n != 0 {
			t.Fatalf("the store was read %d times for a task that never reached an Orker", n)
		}
	})

	t.Run("g: crash then adopt", func(t *testing.T) {
		from, to := pair(1)
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.define(t, "nightly-etl", "1.2.1", diffDigestB, map[string]string{"work": diffImageP})
		to.OrkflowVersion, to.OrkflowDefinitionDigest = "1.2.1", diffDigestB
		// No record 1: the committing attempt died after commit.json.
		f.record(t, from.GetId(), "work", 2, adopted, withDigest(diffPlatD))
		f.record(t, to.GetId(), "work", 1, withDigest(diffPlatE))
		res := f.mustDiff(t, from.GetId(), to.GetId(), "platform_digest", "image")
		requireRows(t, res, "work platform_digest "+diffPlatD+" | "+diffPlatE)
		t.Logf("D8(g) rows with record 1 absent and from definition deleted: %v", rowsOf(res))
	})

	t.Run("h: only a successful adopter qualifies", func(t *testing.T) {
		from, to := pair(1)
		f := newDiffFixture(t, []*orkstackv1.Run{from, to})
		f.record(t, to.GetId(), "work", 1, withDigest(diffPlatE))
		f.record(t, from.GetId(), "work", 2, failed, withDigest(diffPlatX))
		f.record(t, from.GetId(), "work", 3, adopted, withDigest(diffPlatD))
		res := f.mustDiff(t, from.GetId(), to.GetId(), "platform_digest")
		requireRows(t, res, "work platform_digest "+diffPlatD+" | "+diffPlatE)
		t.Logf("D8(h) FAILED record 2 carrying %s refused; adopter answered: %v", diffPlatX, rowsOf(res))

		f.attempts.remove(from.GetId(), "work", 3)
		requireRows(t, f.mustDiff(t, from.GetId(), to.GetId(), "platform_digest"),
			"work platform_digest !attempt_record_missing | "+diffPlatE)

		g := newDiffFixture(t, []*orkstackv1.Run{from, to})
		g.record(t, to.GetId(), "work", 1, withDigest(diffPlatE))
		g.record(t, from.GetId(), "work", 2, adopted, withDigest(diffPlatD))
		g.record(t, from.GetId(), "work", 3, adopted, withDigest(diffPlatD))
		requireRows(t, g.mustDiff(t, from.GetId(), to.GetId(), "platform_digest"),
			"work platform_digest "+diffPlatD+" | "+diffPlatE)

		h := newDiffFixture(t, []*orkstackv1.Run{from, to})
		h.record(t, to.GetId(), "work", 1, withDigest(diffPlatE))
		h.record(t, from.GetId(), "work", 1, withDigest(diffPlatX))
		h.record(t, from.GetId(), "work", 3, adopted, withDigest(diffPlatD))
		requireRows(t, h.mustDiff(t, from.GetId(), to.GetId(), "platform_digest"),
			"work platform_digest "+diffPlatX+" | "+diffPlatE)
	})
}

// d9Runs is D9's fixture, corrected (implementation-notes.md): etl has a,b,c
// and report has b,c,d, so a and d are one-sided and b and c are shared.
func d9Runs(bDiffers bool) (*orkstackv1.Run, *orkstackv1.Run) {
	from := diffRun("ork-etl", func(r *orkstackv1.Run) { r.OrkflowName = "etl"; r.OrkflowVersion = "1.0.0" },
		diffTask("a"), diffTask("b"), diffTask("c"))
	to := diffRun("ork-report", func(r *orkstackv1.Run) {
		r.OrkflowName = "report"
		r.OrkflowVersion = "1.0.0"
		r.OrkflowDefinitionDigest = diffDigestB
	}, diffTask("b", func(task *orkstackv1.TaskRun) {
		if bDiffers {
			task.Status = orkstackv1.Status_STATUS_FAILED
		}
	}), diffTask("c"), diffTask("d"))
	return from, to
}

// D9
func TestDiffRuns_DifferentOrkflowsAreComparedAndFlagged(t *testing.T) {
	from, to := d9Runs(true)
	f := fullyRecordedFixture(t, from, to)
	res := f.mustDiff(t, from.GetId(), to.GetId())
	if res.GetSameOrkflow() {
		t.Fatalf("same_orkflow = true for etl vs report")
	}
	if res.GetTasksCompared() != 4 {
		t.Fatalf("tasks_compared = %d, want 4", res.GetTasksCompared())
	}
	requireRows(t, res,
		"(run) definition etl@1.0.0 "+diffDigestA+" | report@1.0.0 "+diffDigestB,
		"a presence present | absent",
		"b status SUCCEEDED | FAILED",
		"d presence absent | present",
	)
}

// D9a
func TestDiffRuns_OnlyNeverHidesAOneSidedTask(t *testing.T) {
	presenceRows := []string{"a presence present | absent", "d presence absent | present"}
	cases := []struct {
		name       string
		only       []string
		wantFields []string
		noReads    bool
	}{
		{"image", []string{"image"}, []string{"presence", "image"}, false},
		{"input", []string{"input"}, []string{"input", "presence"}, true},
		{"presence", []string{"presence"}, []string{"presence"}, true},
		{"duplicates", []string{"presence", "presence", "image"}, []string{"presence", "image"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			from, to := d9Runs(false)
			f := fullyRecordedFixture(t, from, to)
			res := f.mustDiff(t, from.GetId(), to.GetId(), tc.only...)
			requireRows(t, res, presenceRows...)
			if !slices.Equal(res.GetFieldsCompared(), tc.wantFields) {
				t.Fatalf("fields_compared = %v, want %v", res.GetFieldsCompared(), tc.wantFields)
			}
			if res.GetTasksCompared() != 4 || res.GetTasksIdentical() != 2 {
				t.Fatalf("tasks_compared=%d tasks_identical=%d, want 4 and 2 (b and c; never the one-sided a or d)",
					res.GetTasksCompared(), res.GetTasksIdentical())
			}
			if tc.noReads {
				if gets, lists := f.orkflows.getCalls(), f.attempts.listCalls(); gets != 0 || lists != 0 {
					t.Fatalf("only=%v read the orkflow store %d times and the attempt store %d times; want 0 and 0", tc.only, gets, lists)
				}
			}
			for _, row := range res.GetDifferences() {
				if !slices.Contains(res.GetFieldsCompared(), row.GetField()) {
					t.Fatalf("row field %q is not in fields_compared %v", row.GetField(), res.GetFieldsCompared())
				}
			}
			t.Logf("D9a(%s) rows=%v fields_compared=%v tasks_identical=%d", tc.name, rowsOf(res), res.GetFieldsCompared(), res.GetTasksIdentical())
		})
	}
}

// D10
func TestDiffRuns_OnlyNarrowsTheAnswerAndTheReads(t *testing.T) {
	from, to := d4Runs()
	from.Input = diffInput(t, map[string]any{"date": "2026-09-21"})
	to.Input = diffInput(t, map[string]any{"date": "2026-09-22"})
	f := fullyRecordedFixture(t, from, to)

	res := f.mustDiff(t, from.GetId(), to.GetId(), "status", "pool")
	requireRows(t, res, "extract pool main | gpu", "transform status SUCCEEDED | FAILED")
	if want := []string{"presence", "status", "pool"}; !slices.Equal(res.GetFieldsCompared(), want) {
		t.Fatalf("fields_compared = %v, want %v", res.GetFieldsCompared(), want)
	}
	if res.GetInputUnifiedDiff() != "" {
		t.Fatalf("input was diffed although excluded: %q", res.GetInputUnifiedDiff())
	}
	if gets, lists := f.orkflows.getCalls(), f.attempts.listCalls(); gets != 0 || lists != 0 {
		t.Fatalf("only=[status pool] read the orkflow store %d times and the attempt store %d times; want 0 and 0", gets, lists)
	}
	t.Logf("D10 only=[status pool]: orkflow-store calls=%d attempt-store calls=%d", f.orkflows.getCalls(), f.attempts.listCalls())

	res = f.mustDiff(t, from.GetId(), to.GetId(), "input")
	requireRows(t, res)
	if res.GetInputUnifiedDiff() == "" {
		t.Fatalf("only=[input] returned no input diff for differing inputs")
	}
	if want := []string{"input", "presence"}; !slices.Equal(res.GetFieldsCompared(), want) {
		t.Fatalf("fields_compared = %v, want %v", res.GetFieldsCompared(), want)
	}
	if gets, lists := f.orkflows.getCalls(), f.attempts.listCalls(); gets != 0 || lists != 0 {
		t.Fatalf("only=[input] read the stores: %d, %d", gets, lists)
	}
}

// D11
func TestDiffRuns_ShallowRunsCarryNoTasksInputOrOutput(t *testing.T) {
	from, to := d4Runs()
	from.Input = diffInput(t, map[string]any{"date": "2026-09-21"})
	from.Output = diffInput(t, map[string]any{"rows": 10})
	from.Error = &orkstackv1.Error{Code: "X"}
	f := newDiffFixture(t, []*orkstackv1.Run{from, to})
	res := f.mustDiff(t, from.GetId(), to.GetId(), "status")
	for name, pair := range map[string][2]*orkstackv1.Run{"from": {res.GetFrom(), from}, "to": {res.GetTo(), to}} {
		got, source := pair[0], pair[1]
		if got.GetTaskRuns() != nil || got.GetInput() != nil || got.GetOutput() != nil {
			t.Fatalf("%s carries task_runs/input/output", name)
		}
		want := proto.Clone(source).(*orkstackv1.Run)
		want.TaskRuns, want.Input, want.Output = nil, nil, nil
		if !proto.Equal(got, want) {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	if len(from.GetTaskRuns()) != 3 || from.GetInput() == nil {
		t.Fatalf("the handler cleared the reader's own run instead of a copy")
	}
}

// D12
func TestDiffRuns_StoreErrorsFailRatherThanDegrade(t *testing.T) {
	from := diffRun("ork-mon", nil, diffTask("work"))
	to := diffRun("ork-tue", nil, diffTask("work"))

	f := newDiffFixture(t, []*orkstackv1.Run{from, to})
	f.orkflows.getErr = errors.New("bucket unreachable")
	res, err := f.diff(from.GetId(), to.GetId(), "", "image")
	requireDiffCode(t, err, connect.CodeUnavailable, "read definition nightly-etl@1.2.0", "bucket unreachable")
	if res != nil {
		t.Fatalf("a failed diff returned a partial response")
	}

	g := newDiffFixture(t, []*orkstackv1.Run{from, to})
	g.attempts.listErr = errors.New("bucket unreachable")
	res, err = g.diff(from.GetId(), to.GetId(), "", "platform_digest")
	requireDiffCode(t, err, connect.CodeUnavailable, `read attempt records of task "work" in run "ork-mon"`)
	if res != nil {
		t.Fatalf("a failed diff returned a partial response")
	}
}

func skipped(task *orkstackv1.TaskRun) {
	task.Status = orkstackv1.Status_STATUS_SKIPPED
	task.Attempt = 0
	task.Pool = ""
	task.OrkerId = ""
	task.StartedAt = nil
	task.FinishedAt = nil
}

// D13
func TestDiffRuns_DoesNotApplyIsEmptyAndAvailable(t *testing.T) {
	from := diffRun("ork-mon", nil, diffTask("load", skipped))
	to := diffRun("ork-tue", nil, diffTask("load", skipped))
	f := fullyRecordedFixture(t, from, to)
	res := f.mustDiff(t, from.GetId(), to.GetId())
	requireRows(t, res)
	if res.GetTasksIdentical() != 1 || res.GetEvidenceUnavailable() != 0 {
		t.Fatalf("skipped vs skipped: identical=%d unavailable=%d, want 1 and 0", res.GetTasksIdentical(), res.GetEvidenceUnavailable())
	}

	ran := diffRun("ork-wed", nil, diffTask("load", func(task *orkstackv1.TaskRun) {
		task.FinishedAt = timestamppb.New(diffT0.Add(850 * time.Millisecond))
	}))
	g := fullyRecordedFixture(t, from, ran)
	requireRows(t, g.mustDiff(t, from.GetId(), ran.GetId()),
		"load status SKIPPED | SUCCEEDED",
		"load attempt 0 | 1",
		"load pool - | main",
		"load orker - | orker-a",
		"load exit_code - | 0",
		"load duration - | 850ms",
		"load platform_digest - | "+diffPlatD,
	)
}

// D14
func TestDiffRuns_DurationUnavailableWhenTerminalWithMissingOrInvertedTimes(t *testing.T) {
	from := diffRun("ork-mon", nil,
		diffTask("missing", func(task *orkstackv1.TaskRun) {
			task.Status = orkstackv1.Status_STATUS_FAILED
			task.FinishedAt = nil
		}),
		diffTask("inverted", func(task *orkstackv1.TaskRun) {
			task.FinishedAt = timestamppb.New(diffT0.Add(-2 * time.Second))
		}),
		diffTask("running", func(task *orkstackv1.TaskRun) {
			task.Status = orkstackv1.Status_STATUS_RUNNING
			task.FinishedAt = nil
		}),
	)
	to := diffRun("ork-tue", nil, diffTask("missing"), diffTask("inverted"), diffTask("running"))
	f := newDiffFixture(t, []*orkstackv1.Run{from, to})
	requireRows(t, f.mustDiff(t, from.GetId(), to.GetId(), "duration"),
		"missing duration !timestamps_missing | 1m3s",
		"inverted duration !timestamps_inverted | 1m3s",
		"running duration - | 1m3s",
	)
}

// D15
func TestDiffRuns_LiveRunComparesOnItsSnapshot(t *testing.T) {
	from := diffRun("ork-live", func(r *orkstackv1.Run) {
		r.Status = orkstackv1.Status_STATUS_RUNNING
		r.FinishedAt = nil
	},
		diffTask("first"),
		diffTask("second", func(task *orkstackv1.TaskRun) {
			task.Status = orkstackv1.Status_STATUS_RUNNING
			task.FinishedAt = nil
		}),
		diffTask("third", func(task *orkstackv1.TaskRun) {
			skipped(task)
			task.Status = orkstackv1.Status_STATUS_PENDING
		}),
	)
	to := diffRun("ork-done", nil, diffTask("first"), diffTask("second"), diffTask("third"))
	f := newDiffFixture(t, []*orkstackv1.Run{from, to})
	res := f.mustDiff(t, from.GetId(), to.GetId(), snapshotFields...)
	requireRows(t, res,
		"second status RUNNING | SUCCEEDED",
		"second duration - | 1m3s",
		"third status PENDING | SUCCEEDED",
		"third attempt 0 | 1",
		"third pool - | main",
		"third orker - | orker-a",
		"third exit_code - | 0",
		"third duration - | 1m3s",
	)
	if res.GetFrom().GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("from status = %v, want RUNNING", res.GetFrom().GetStatus())
	}
}

// D16
func TestDiffRuns_CountsAndOrderAreTheContract(t *testing.T) {
	from, to := d4Runs()
	from.TaskRuns = append(from.TaskRuns, diffTask("idle"), diffTask("ship"))
	to.TaskRuns = append(to.TaskRuns, diffTask("idle"), diffTask("ship"))
	f := fullyRecordedFixture(t, from, to)
	f.attempts.remove(from.GetId(), "ship", 1)

	res := f.mustDiff(t, from.GetId(), to.GetId())
	requireRows(t, res,
		"extract pool main | gpu",
		"extract duration 1m3s | 4m12s",
		"transform status SUCCEEDED | FAILED",
		"transform attempt 1 | 3",
		"transform exit_code 0 | 3",
		"transform error - | CONTAINER_EXIT_NONZERO (ACTIVITY_FAILED)",
		"load pin - | p1",
		"load orker orker-a | orker-b",
		"ship platform_digest !attempt_record_missing | "+diffPlatD,
	)
	if res.GetTasksCompared() != 5 || res.GetTasksIdentical() != 1 || res.GetEvidenceUnavailable() != 1 {
		t.Fatalf("compared=%d identical=%d unavailable=%d, want 5 1 1",
			res.GetTasksCompared(), res.GetTasksIdentical(), res.GetEvidenceUnavailable())
	}
}
