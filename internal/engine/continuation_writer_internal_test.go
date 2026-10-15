package engine

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestContinuation_ReachedIsEitherDimension(t *testing.T) {
	for _, tc := range []struct {
		events, bytes int64
		at            [2]int64
		want          bool
	}{
		{0, 0, [2]int64{}, true}, // the zero value is always reached
		{9, 9, [2]int64{10, 10}, false},
		{10, 0, [2]int64{10, 10}, true},
		{0, 10, [2]int64{10, 10}, true},
		{11, 11, [2]int64{10, 10}, true},
	} {
		if got := reached(tc.events, tc.bytes, tc.at); got != tc.want {
			t.Errorf("reached(%d, %d, %v) = %v, want %v", tc.events, tc.bytes, tc.at, got, tc.want)
		}
	}
}

func validBudget() ContinuationBudget {
	return DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, DefaultContinuationRestoreTimeout)
}

// continuationProbe runs body in a workflow of its own, over a state built
// from wf and plan, with the raw history length at length.
func continuationProbe(t *testing.T, length int, wf *orkstackv1.Orkflow, plan RunPlan, body func(ctx workflow.Context, s *runState)) {
	t.Helper()
	runGuardWorkflow(t, func(env *testsuite.TestWorkflowEnvironment) {
		env.SetCurrentHistoryLength(length)
	}, func(ctx workflow.Context) error {
		s := newRunState(ctx, "ork-probe", wf, guardInput(t, map[string]any{}), plan)
		s.beginPass()
		body(ctx, s)
		return nil
	})
}

// TestContinuation_MalformedBudgetNeverSwitches: a budget failing valid() —
// a hand-built or forged plan, never one pinRunTreeLimits built — never
// continues: noSwitch is set and no version marker is recorded; restore
// refuses it (TestRestore_NeverPanicsOnAMalformedCarryOrPlan).
func TestContinuation_MalformedBudgetNeverSwitches(t *testing.T) {
	cases := map[string]func(b *ContinuationBudget){
		"zero events":                    func(b *ContinuationBudget) { b.Events = 0 },
		"zero bytes":                     func(b *ContinuationBudget) { b.Bytes = 0 },
		"zero checkpoint bound":          func(b *ContinuationBudget) { b.CheckpointMaxBytes = 0 },
		"zero retry spacing":             func(b *ContinuationBudget) { b.RetryEvents = 0 },
		"hold percent above 100":         func(b *ContinuationBudget) { b.HoldPercent = 101 },
		"hold percent zero":              func(b *ContinuationBudget) { b.HoldPercent = 0 },
		"no generations":                 func(b *ContinuationBudget) { b.MaxGenerations = 0 },
		"chunk above 128 KiB":            func(b *ContinuationBudget) { b.ChunkBytes = continuationChunkCeiling + 1 },
		"no chunks per task":             func(b *ContinuationBudget) { b.ChunksPerTask = 0 },
		"no restore timeout":             func(b *ContinuationBudget) { b.Restore.TimeoutSeconds = 0 },
		"no restore attempt budget":      func(b *ContinuationBudget) { b.Restore.AttemptSeconds = 0 },
		"no restore backoff":             func(b *ContinuationBudget) { b.Restore.BackoffInitialMillis = 0 },
		"no restore backoff upper bound": func(b *ContinuationBudget) { b.Restore.BackoffMaxSeconds = 0 },
	}
	if b := validBudget(); !b.valid() {
		t.Fatal("the default budget is not valid")
	}
	wf := guardCompile(t, guardFanOutDocument)
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			b := validBudget()
			mutate(&b)
			if b.valid() {
				t.Fatal("valid() accepted it")
			}
			plan := guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes)
			plan.Continuation = &b
			continuationProbe(t, 40000, wf, plan, func(ctx workflow.Context, s *runState) {
				s.updateContinuation(ctx, nil)
				if s.noSwitch != "its plan's continuation budget is malformed" || s.switchDue || s.quiescing {
					t.Errorf("noSwitch %q due %v quiescing %v", s.noSwitch, s.switchDue, s.quiescing)
				}
				if _, asked := s.versions[ContinueAsNewChange]; asked {
					t.Error("a malformed budget asked the continue-as-new version")
				}
			})
		})
	}
}

// TestContinuation_BudgetWithoutHistoryLimitNeverSwitches: a hand-built plan
// with a budget but no history ceiling never continues, however long its
// history.
func TestContinuation_BudgetWithoutHistoryLimitNeverSwitches(t *testing.T) {
	b := validBudget()
	plan := guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes)
	plan.History = nil
	plan.Continuation = &b
	continuationProbe(t, 10_000_000, guardCompile(t, guardFanOutDocument), plan, func(ctx workflow.Context, s *runState) {
		s.updateContinuation(ctx, nil)
		if s.switchDue || s.quiescing || len(s.versions) != 0 {
			t.Errorf("due %v quiescing %v versions %v", s.switchDue, s.quiescing, s.versions)
		}
	})
}

func flatDefinition(t *testing.T, n int) *orkstackv1.Orkflow {
	var b strings.Builder
	b.WriteString("name: flat\ntasks:\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - id: t%d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	return guardCompile(t, b.String())
}

// TestContinuation_HoldStartsAtThePlansHoldLine: past its budget, a run
// starts holding exactly when its history plus its failure path's reserve
// reaches HoldPercent of the limit — not one event earlier. A wide run,
// whose reserve is larger, starts at a lower raw history, and a plan with a
// lower HoldPercent starts earlier.
func TestContinuation_HoldStartsAtThePlansHoldLine(t *testing.T) {
	for _, tc := range []struct {
		name    string
		tasks   int
		percent int64
	}{
		{"a narrow run at 60%", 3, 60},
		{"a wide run at 60%", 40, 60},
		{"a narrow run at 50%", 3, 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := flatDefinition(t, tc.tasks)
			b := DefaultContinuationBudget(1, 1<<40, time.Hour)
			b.HoldPercent = tc.percent
			plan := guardPlan(t, 1000, DefaultHistoryMaxBytes)
			plan.Continuation = &b
			// Every task PENDING: the reserve is Fixed plus PerPending each.
			reserve := DefaultHistoryReserve.Fixed + DefaultHistoryReserve.PerPending*int64(tc.tasks)
			line := int(1000*tc.percent/100 - reserve)
			for _, length := range []int{line - 1, line} {
				continuationProbe(t, length, wf, plan, func(ctx workflow.Context, s *runState) {
					s.updateContinuation(ctx, nil)
					if want := length >= line; s.quiescing != want || !s.switchDue {
						t.Errorf("at raw history %d (line %d): quiescing %v, due %v", length, line, s.quiescing, s.switchDue)
					}
				})
			}
			t.Logf("%s: holds from raw history %d (reserve %d)", tc.name, line, reserve)
		})
	}
}

func TestRestoreHistoryBound(t *testing.T) {
	b := validBudget()
	eight := DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, 8*time.Hour)
	twelve := validBudget()
	twelve.Restore.BackoffInitialMillis = 3000 // 3, 6, 12: the first server backoff is 12 s
	for _, tc := range []struct {
		name   string
		budget ContinuationBudget
		want   int64
	}{
		{"defaults", b, 1478},
		{"an eight-hour restore timeout", eight, 10928},
		{"a first server backoff of 12 s (6/12 events a second)", twelve, 64 + 40 + 24 + 1800},
	} {
		if got := RestoreHistoryBound(tc.budget); got != tc.want {
			t.Errorf("%s: RestoreHistoryBound = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestRunState_EveryFieldIsClassified: every field of the state a generation
// holds is in runStateFieldClass — rebuilt, restored or ephemeral — so a
// field added later cannot be silently dropped at a switch.
func TestRunState_EveryFieldIsClassified(t *testing.T) {
	seen := map[string]bool{}
	for _, typ := range []reflect.Type{
		reflect.TypeFor[runState](), reflect.TypeFor[eachState](), reflect.TypeFor[awaitState](), reflect.TypeFor[pinState](),
	} {
		for i := 0; i < typ.NumField(); i++ {
			key := typ.Name() + "." + typ.Field(i).Name
			seen[key] = true
			if _, ok := runStateFieldClass[key]; !ok {
				t.Errorf("%s is not classified: add it to runStateFieldClass as derived, restored or ephemeral", key)
			}
		}
	}
	for key := range runStateFieldClass {
		if !seen[key] {
			t.Errorf("runStateFieldClass names %s, which is no field", key)
		}
	}
}

// engineSources parses internal/engine's non-test Go files.
func engineSources(t *testing.T) (*token.FileSet, map[string]*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]*ast.File{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[path] = file
	}
	return fset, files
}

// TestPinnedDefaults_ReadOnlyAtSubmit: every identifier pinned_defaults.go
// declares is referenced by no other non-test file of internal/engine.
// Workflow code reads the number the plan carries, never the constant, so a
// release that changes a default moves only new runs (AGENTS.md §7.2).
//
// The one exemption is named, never a pattern: localItemInput (local_task.go)
// composes an item's input for `orkctl task run`, outside any run — nothing it
// computes is replayed or recorded — and a run's plan never reaches it
// (--from-run reads the run, which carries none), so it reads the floor every
// run it can be pointed at pinned (spec 4.2).
func TestPinnedDefaults_ReadOnlyAtSubmit(t *testing.T) {
	exempt := map[string]map[string]bool{"local_task.go": {"DefaultItemInputFloorBytes": true}}
	fset, files := engineSources(t)
	declared := map[string]bool{}
	for _, decl := range files["pinned_defaults.go"].Decls {
		switch d := decl.(type) {
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.ValueSpec:
					for _, name := range s.Names {
						declared[name.Name] = true
					}
				case *ast.TypeSpec:
					declared[s.Name.Name] = true
				}
			}
		case *ast.FuncDecl:
			declared[d.Name.Name] = true
		}
	}
	if len(declared) < 15 {
		t.Fatalf("pinned_defaults.go declares only %d identifiers: %v", len(declared), declared)
	}
	for path, file := range files {
		if path == "pinned_defaults.go" {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok && declared[ident.Name] && !exempt[filepath.Base(path)][ident.Name] {
				t.Errorf("%s references %s, a pinned default: read the plan's number instead", fset.Position(ident.Pos()), ident.Name)
			}
			return true
		})
	}
}

func hexSHA(t testing.TB) string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}

// TestContinuation_CarryAtMaxGenerationsFitsThePayloadBound: a carry at the
// generation ceiling — 255 segment references, a checkpoint of 160 parts and
// the switch snapshot, under a 200-byte namespace and a 63-byte bucket —
// stays inside AGENTS.md §2.2's 256 KiB.
func TestContinuation_CarryAtMaxGenerationsFitsThePayloadBound(t *testing.T) {
	namespace := "n" + strings.Repeat("a", 199)
	runID := "ork-1758790021000000000-18446744073709551615"
	root := "s3://" + strings.Repeat("b", 63) + "/"
	uri := func(generation int64, kind, sha string) string {
		key, err := artifacts.RunContinuationKey(namespace, runID, generation, kind, sha)
		if err != nil {
			t.Fatal(err)
		}
		return root + key
	}
	object := func(generation int64, kind string) *orkstackv1.ContinuationObject {
		sha := hexSHA(t)
		return &orkstackv1.ContinuationObject{Uri: uri(generation, kind, sha), Sha256: sha, SizeBytes: 8 << 20, RawBytes: 40 << 20}
	}
	carry := &orkstackv1.RunContinuation{
		Version: 1, Generation: MaxGenerationsCeiling - 1, EventSequence: 1 << 40,
		CreatedAt: timestamppb.Now(), StartedAt: timestamppb.Now(),
		Checkpoint: object(MaxGenerationsCeiling-2, artifacts.ContinuationKindCheckpoint),
		Snapshot:   object(MaxGenerationsCeiling-2, artifacts.ContinuationKindSnapshot),
		VersionDecisions: map[string]int32{
			leanEventPathChange: 1, HistoryGuardChange: 1, ContinueAsNewChange: 1, InFlightCapChange: 1, cancelSiblingsChange: 1,
		},
	}
	for i := 0; i < 160; i++ {
		carry.Checkpoint.Parts = append(carry.Checkpoint.Parts, &orkstackv1.ContinuationPart{
			Offset: int64(i) << 16, SizeBytes: 1 << 16, RawBytes: 1 << 20, Sha256: hexSHA(t)})
	}
	for g := int64(0); g < MaxGenerationsCeiling-1; g++ {
		carry.Segments = append(carry.Segments, &orkstackv1.EventSegmentRef{
			Generation: g, ExecutionId: "01933f0e-4c6a-7d2b-9e1f-0123456789ab",
			FirstSequence: g<<32 + 1, LastSequence: (g + 1) << 32, EventCount: 1 << 31,
			Object: object(g, artifacts.ContinuationKindEvents),
		})
	}
	size, err := task.PayloadSize(carry)
	if err != nil {
		t.Fatal(err)
	}
	if size >= task.MaxActivityInputBytes {
		t.Fatalf("the carry at the generation ceiling is %d bytes, at or over %d", size, task.MaxActivityInputBytes)
	}
	proof.WriteJSON(t, "continuation", "carry-at-max-generations.json", map[string]any{"bytes": size, "limit": task.MaxActivityInputBytes})
	t.Logf("E2 carry at %d generations: %d bytes of %d", MaxGenerationsCeiling, size, task.MaxActivityInputBytes)
}

// typicalRow is a SUCCEEDED task row as an Orker's result leaves it: values
// and commit references, two system artifacts (its logs) and workload
// artifacts, each with its own SHA-256, and a small inline output.
func typicalRow(t testing.TB, id string, workload int, output *orkstackv1.Payload) *orkstackv1.TaskRun {
	ref := func(name string) *orkstackv1.ArtifactRef {
		return &orkstackv1.ArtifactRef{Name: name, Uri: "s3://orkstack-artifacts/artifacts/team-a/ork-1758790021000000000-4081623940183294/" +
			id + "/" + name, Sha256: hexSHA(t), SizeBytes: 123456, ContentType: "application/octet-stream"}
	}
	row := &orkstackv1.TaskRun{
		TaskId: id, Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-7f3a",
		ScheduledAt: timestamppb.Now(), StartedAt: timestamppb.Now(), FinishedAt: timestamppb.Now(), Attempt: 1,
		ValuesRef: ref("values.json"),
		Commit: &orkstackv1.CommitRef{Uri: "s3://orkstack-artifacts/commits/team-a/ork-1758790021000000000-4081623940183294/" +
			id + "/commit.json", Sha256: hexSHA(t), Etag: hexSHA(t)[:32], CreatedAt: timestamppb.Now()},
		SystemArtifacts: []*orkstackv1.ArtifactRef{ref("stdout.log"), ref("stderr.log")},
		Output:          output,
	}
	for i := 0; i < workload; i++ {
		row.Artifacts = append(row.Artifacts, ref(fmt.Sprintf("out-%d.bin", i)))
	}
	return row
}

func smallOutput(t testing.TB) *orkstackv1.Payload {
	data, err := structpb.NewStruct(map[string]any{"ok": true, "digest": hexSHA(t)[:16]})
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.Payload{Data: data}
}

func randomOutput(t testing.TB, n int) *orkstackv1.Payload {
	raw := make([]byte, n*3/4)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	data, err := structpb.NewStruct(map[string]any{"blob": base64.StdEncoding.EncodeToString(raw)})
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.Payload{Data: data}
}

// sizedRun is a run of tasks typical rows, and a fan-out of items items each
// with workload artifacts.
func sizedRun(t testing.TB, tasks, items, taskArtifacts, itemArtifacts int, output func() *orkstackv1.Payload) *orkstackv1.Run {
	run := &orkstackv1.Run{Id: "ork-1758790021000000000-4081623940183294", Namespace: "team-a", OrkflowName: "sized",
		Status: orkstackv1.Status_STATUS_RUNNING, CreatedAt: timestamppb.Now(), StartedAt: timestamppb.Now()}
	for i := 0; i < tasks; i++ {
		run.TaskRuns = append(run.TaskRuns, typicalRow(t, fmt.Sprintf("t%d", i), taskArtifacts, output()))
	}
	if items > 0 {
		fan := &orkstackv1.TaskRun{TaskId: "fan", Status: orkstackv1.Status_STATUS_RUNNING,
			Each: &orkstackv1.TaskEach{Expanded: true, Count: int32(items)}}
		for i := 0; i < items; i++ {
			item := typicalRow(t, "fan", itemArtifacts, nil)
			item.Item = proto.Int32(int32(i))
			fan.Items = append(fan.Items, item)
		}
		run.TaskRuns = append(run.TaskRuns, fan)
	}
	return run
}

// checkpointSize writes run's checkpoint, uncapped, and returns its stored
// size.
func checkpointSize(t testing.TB, run *orkstackv1.Run) int64 {
	a := NewSnapshotActivities(newMemSnapshotStore())
	obj, err := a.WriteCheckpoint(context.Background(), run, 0, 1<<40, ContinuationObservationInput{})
	if err != nil {
		t.Fatal(err)
	}
	return obj.GetSizeBytes()
}

// TestContinuation_CheckpointSizesAtTheCeilings measures saved states at the
// ceilings against the 8 MiB default bound, and records each (transcript
// under ORK_PROOF_DIR): typical tasks and items fit with room to spare,
// artifact-maximal rows do not, and at 4,000 tasks the largest average inline
// output that fits is recorded for the task ceiling's later evidence.
func TestContinuation_CheckpointSizesAtTheCeilings(t *testing.T) {
	limit := DefaultContinuationBytes
	small := func() *orkstackv1.Payload { return smallOutput(t) }
	results := map[string]any{"cap_bytes": limit}
	measure := func(name string, run *orkstackv1.Run) int64 {
		size := checkpointSize(t, run)
		results[name] = size
		t.Logf("E2 checkpoint %s: %d bytes (%.2f MiB, cap %.0f MiB)", name, size, float64(size)/(1<<20), float64(limit)/(1<<20))
		return size
	}
	if size := measure("a_1000_tasks_1000_items_1_artifact", sizedRun(t, 1000, 1000, 1, 1, small)); size > limit/4 {
		t.Errorf("(a) %d bytes, over a quarter of the cap", size)
	}
	if size := measure("b_1000_tasks_1000_items_16_artifacts", sizedRun(t, 1000, 1000, 1, 16, small)); size > limit {
		t.Errorf("(b) %d bytes, over the cap", size)
	}
	measure("c_10000_items_at_the_artifact_budget", sizedRun(t, 0, 10000, 0, eachItemArtifactBudget/10000, small))
	if size := measure("d_1000_artifact_maximal_tasks", sizedRun(t, 1000, 0, int(DefaultArtifactMaxCount), 0, small)); size <= limit {
		t.Errorf("(d) %d bytes fits the cap: artifact-maximal rows were expected not to", size)
	}
	full := func() *orkstackv1.Payload { return randomOutput(t, 4096) }
	measure("e_1000_tasks_4KiB_outputs", sizedRun(t, 1000, 0, 0, 0, full))
	measure("e_4000_tasks_4KiB_outputs", sizedRun(t, 4000, 0, 0, 0, full))
	lo, hi := 0, 4096
	for hi-lo > 64 {
		mid := (lo + hi) / 2
		if checkpointSize(t, sizedRun(t, 4000, 0, 0, 0, func() *orkstackv1.Payload { return randomOutput(t, mid) })) <= limit {
			lo = mid
		} else {
			hi = mid
		}
	}
	results["e_largest_average_inline_output_at_4000_tasks"] = lo
	t.Logf("E2 checkpoint (e): at 4,000 tasks the largest average inline output that fits the cap is about %d bytes", lo)
	proof.WriteJSON(t, "continuation", "checkpoint-sizes.json", results)
}

// cappedRestore is a definition of n plain tasks and a carry whose
// checkpoint holds them all SUCCEEDED with incompressible inline outputs,
// sized to just under the 8 MiB bound.
func cappedRestore(t *testing.T, n int) (*orkstackv1.Orkflow, *memSnapshotStore, *orkstackv1.RunContinuation, int64) {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: "capped", Version: "v1", Namespace: "team-a"}
	for i := 0; i < n; i++ {
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: fmt.Sprintf("t%d", i), Image: &orkstackv1.ImageRef{Reference: "alpine"}})
	}
	outputBytes := 1500
	var run *orkstackv1.Run
	var size int64
	for attempt := 0; attempt < 4; attempt++ {
		run = sizedRun(t, n, 0, 0, 0, func() *orkstackv1.Payload { return randomOutput(t, outputBytes) })
		run.Id, run.Namespace = refusalWorkflowID, "team-a"
		size = checkpointSize(t, run)
		target := DefaultContinuationBytes - 64<<10
		if size <= DefaultContinuationBytes && size > target-512<<10 {
			break
		}
		outputBytes = int(float64(outputBytes) * float64(target) / float64(size))
	}
	if size > DefaultContinuationBytes {
		t.Fatalf("the capped checkpoint is %d bytes, over the cap", size)
	}
	store := newMemSnapshotStore()
	a := NewSnapshotActivities(store)
	checkpoint, err := a.WriteCheckpoint(context.Background(), run, 0, DefaultContinuationBytes, ContinuationObservationInput{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := a.WriteSwitchSnapshot(context.Background(), run, "team-a", 0, ContinuationObservationInput{})
	if err != nil {
		t.Fatal(err)
	}
	carry := &orkstackv1.RunContinuation{
		Version: 1, Generation: 1, EventSequence: int64(2*n + 2), CreatedAt: run.GetCreatedAt(), StartedAt: run.GetStartedAt(),
		Checkpoint: checkpoint, Snapshot: snapshot,
		Segments: []*orkstackv1.EventSegmentRef{{Generation: 0, FirstSequence: 1, LastSequence: int64(2*n + 2), EventCount: 0,
			Object: segmentObjectForTest(t, "team-a", refusalWorkflowID)}},
		VersionDecisions: map[string]int32{leanEventPathChange: 1, ContinueAsNewChange: 1},
	}
	return wf, store, carry, size
}

// segmentObjectForTest is a reference to an (unread) event segment of
// generation 0 at its own content-addressed key.
func segmentObjectForTest(t testing.TB, namespace, runID string) *orkstackv1.ContinuationObject {
	sha := hexSHA(t)
	key, err := artifacts.RunContinuationKey(namespace, runID, 0, artifacts.ContinuationKindEvents, sha)
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.ContinuationObject{Uri: "s3://test/" + key, Sha256: sha, SizeBytes: 1, RawBytes: 1}
}

// TestContinuation_RestoreFitsTheDeadlockBudget: restoring a saved state at
// the 8 MiB bound, built from incompressible rows, completes under the
// production 1 s deadlock detector; and one batch's decode (a megabyte of
// chunks, its parts verified, gunzipped and unmarshalled) takes at most
// 200 ms, a 5x margin.
// restoreBatchDecodeBound is the spec's bound on one batch's decode, a
// fifth of the one-second deadlock detector; raceSlowdown scales it under
// the race detector.
const (
	restoreBatchDecodeBound = 200 * time.Millisecond
	raceSlowdown            = 10
)

func TestContinuation_RestoreFitsTheDeadlockBudget(t *testing.T) {
	wf, store, carry, size := cappedRestore(t, 4000)
	h := newRefusalHarnessOver(t, store)
	h.env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: time.Second})
	started := time.Now()
	h.env.ExecuteWorkflow(RunWorkflowType, "", wf, (*orkstackv1.Payload)(nil), refusalRoutedPlan(t), carry)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("restoring %d bytes failed: %v", size, err)
	}
	elapsed := time.Since(started)

	data, _ := store.get(carry.GetCheckpoint().GetUri())
	decoder := &partDecoder{refs: carry.GetCheckpoint().GetParts(), decode: func(_ int, plain []byte) error {
		return proto.Unmarshal(plain, &orkstackv1.RunCheckpointPart{})
	}}
	var worst time.Duration
	for off := 0; off < len(data); off += int(DefaultContinuationChunksPerTask * DefaultContinuationChunkBytes) {
		end := min(len(data), off+int(DefaultContinuationChunksPerTask*DefaultContinuationChunkBytes))
		begin := time.Now()
		for chunk := off; chunk < end; chunk += int(DefaultContinuationChunkBytes) {
			if err := decoder.feed(data[chunk:min(end, chunk+int(DefaultContinuationChunkBytes))]); err != nil {
				t.Fatal(err)
			}
		}
		worst = max(worst, time.Since(begin))
	}
	// The spec's bound is for the production build. The race detector's
	// instrumentation slows this CPU-bound decode several times over, so
	// under -race the bound is scaled, never dropped, and the measurement
	// is not written as evidence.
	bound := restoreBatchDecodeBound
	if raceBuild {
		bound *= raceSlowdown
	}
	if worst > bound {
		t.Fatalf("the slowest batch decoded in %s, over %s", worst, bound)
	}
	if !raceBuild {
		proof.WriteJSON(t, "continuation", "restore-deadlock-budget.json", map[string]any{
			"checkpoint_bytes": size, "tasks": 4000, "whole_restore_ms": elapsed.Milliseconds(), "slowest_batch_decode_ms": worst.Milliseconds(),
		})
	}
	t.Logf("E2 restore at the cap: %d bytes over 4,000 tasks restored in %s; slowest batch decode %s", size, elapsed, worst)
}

// newRefusalHarnessOver is newRefusalHarness over an existing store, with
// an ExecuteTask that succeeds.
func newRefusalHarnessOver(t *testing.T, store *memSnapshotStore) *refusalHarness {
	h := newRefusalHarness(t)
	h.store = store
	var suite testsuite.WorkflowTestSuite
	h.env = suite.NewTestWorkflowEnvironment()
	h.env.SetTestTimeout(120 * time.Second)
	h.env.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	activities := NewSnapshotActivities(store)
	h.env.RegisterActivityWithOptions(activities.WriteSnapshot, activity.RegisterOptions{Name: ActivityWriteSnapshot})
	h.env.RegisterActivityWithOptions(activities.WriteFailedRunSnapshot, activity.RegisterOptions{Name: ActivityWriteFailedRunSnapshot})
	h.env.RegisterActivityWithOptions(activities.WriteEventSegment, activity.RegisterOptions{Name: ActivityWriteEventSegment})
	h.env.RegisterActivityWithOptions(activities.WriteSwitchSnapshot, activity.RegisterOptions{Name: ActivityWriteSwitchSnapshot})
	h.env.RegisterActivityWithOptions(activities.WriteCheckpoint, activity.RegisterOptions{Name: ActivityWriteCheckpoint})
	h.env.RegisterActivityWithOptions(activities.ReadContinuationChunk, activity.RegisterOptions{Name: ActivityReadContinuationChunk})
	h.env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, uri, _ string) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.events = append(h.events, event)
		h.uris = append(h.uris, uri)
		return nil
	}, activity.RegisterOptions{Name: ActivityPublishEvent})
	h.env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{ValuesJson: []byte(`{}`)}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	h.env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ArtifactRef, []string, string, int64) (int32, error) {
		return 3, nil
	}, activity.RegisterOptions{Name: ActivityCountEachItems})
	h.env.SetStartWorkflowOptions(startOptionsForTest())
	return h
}

// refusalRoutedPlan is refusalPlan with the default pool routed, so a
// restored run can dispatch.
func refusalRoutedPlan(t *testing.T) RunPlan {
	plan := refusalPlan()
	route, err := task.RouteForPool(task.DefaultPool, task.DefaultQueue)
	if err != nil {
		t.Fatal(err)
	}
	plan.Routes = map[string]string{route.Pool: route.Queue}
	return plan
}

var _ = sort.Strings

func startOptionsForTest() client.StartWorkflowOptions {
	return client.StartWorkflowOptions{ID: refusalWorkflowID}
}

// TestContinuation_GenerationCeilingReasonNamesNoCount: at the plan's
// MaxGenerations the reason a run gives for not carrying on reaches
// RUN_HISTORY_LIMIT's message, a user-facing surface, so it never says how
// many times the run carried forward (AGENTS.md §7, A2).
func TestContinuation_GenerationCeilingReasonNamesNoCount(t *testing.T) {
	b := DefaultContinuationBudget(1, 1<<40, time.Hour)
	b.MaxGenerations = 7
	plan := guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes)
	plan.Continuation = &b
	continuationProbe(t, 100, flatDefinition(t, 3), plan, func(ctx workflow.Context, s *runState) {
		s.generation = 6
		s.updateContinuation(ctx, nil)
		if s.noSwitch != generationCeilingReason || !s.switchDue {
			t.Errorf("noSwitch %q due %v, want the generation ceiling", s.noSwitch, s.switchDue)
		}
		reason := s.carryReason()
		if reason != "it cannot carry on into a fresh history: "+generationCeilingReason {
			t.Errorf("carryReason = %q", reason)
		}
		if strings.ContainsAny(reason, "0123456789") {
			t.Errorf("carryReason %q names a count", reason)
		}
	})
}

// TestContinuation_GiveUpReadsTheBudgetLikeEveryPath: abandonQuiesce reads
// the budget through continuationBudget, as every other path does, so a
// test-only override over a plan with no budget of its own spaces the retry
// by the override's budget and never dereferences the plan's nil one.
func TestContinuation_GiveUpReadsTheBudgetLikeEveryPath(t *testing.T) {
	override := validBudget()
	override.RetryEvents, override.RetryBytes = 123, 456
	continuationPlanForTest = &override
	defer func() { continuationPlanForTest = nil }()
	plan := guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes)
	plan.Continuation = nil
	continuationProbe(t, 100, flatDefinition(t, 3), plan, func(ctx workflow.Context, s *runState) {
		s.quiescing = true
		s.abandonQuiesce(ctx, nil, "pin build is still open")
		e, b := s.historyNow(ctx)
		if s.quiescing || s.quiesceRetryAt != [2]int64{e + 123, b + 456} {
			t.Errorf("quiescing %v retry at %v, want the override's spacing from (%d, %d)", s.quiescing, s.quiesceRetryAt, e, b)
		}
	})
}
