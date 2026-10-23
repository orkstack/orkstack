package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"go/ast"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// largeDefinition is a definition of n tasks whose `with:` carries
// noteBytes of random hex each — incompressible, so its stored object is
// about n·noteBytes/2 bytes — under refusalWorkflowID's namespace.
func largeDefinition(t testing.TB, n, noteBytes int) *orkstackv1.Orkflow {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: "large", Version: "v1", Namespace: "team-a", DefinitionDigest: strings.Repeat("ab", 32),
		Labels: map[string]string{"team": "a"}, Pins: []*orkstackv1.Pin{{Name: "shared"}}}
	for i := 0; i < n; i++ {
		note := make([]byte, noteBytes/2)
		if _, err := rand.Read(note); err != nil {
			t.Fatal(err)
		}
		with, err := structpb.NewStruct(map[string]any{"op": "exec", "note": hex.EncodeToString(note)})
		if err != nil {
			t.Fatal(err)
		}
		task := &orkstackv1.Task{Id: fmt.Sprintf("t%04d", i), Image: &orkstackv1.ImageRef{Reference: "alpine"}, With: with}
		if i > 0 {
			task.Needs = []string{fmt.Sprintf("t%04d", i-1)}
		}
		wf.Tasks = append(wf.Tasks, task)
	}
	return wf
}

// TestDefinitionObject_PartsMergeToTheDefinition: the object's format is the
// one spec 5.1 names — part 0 the definition with its tasks cleared, then
// parts of tasks only, each at most checkpointPartRawBytes encoded — so its
// concatenated encodings merge back into the definition with one
// multistream unmarshal, and the part-by-part decode a generation and the
// await doors use gives the same definition. The encoding is deterministic,
// so a retried write lands the same bytes on the same key.
func TestDefinitionObject_PartsMergeToTheDefinition(t *testing.T) {
	planned := largeDefinition(t, 600, 4000)
	data, ref, err := EncodeDefinitionObject(planned)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := EncodeDefinitionObject(planned)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatalf("encoding twice gave different bytes (%v)", err)
	}
	if len(ref.Parts) < 3 {
		t.Fatalf("a %d-byte definition encoded as %d parts, want several", ref.RawBytes, len(ref.Parts))
	}
	if what := ref.check(int64(len(data))); what != "" {
		t.Fatalf("the reference fails its own check: %s", what)
	}
	for i, part := range ref.Parts {
		if part.RawBytes > checkpointPartRawBytes {
			t.Fatalf("part %d is %d bytes encoded, over %d", i, part.RawBytes, checkpointPartRawBytes)
		}
	}
	// One multistream unmarshal.
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	whole := &orkstackv1.Orkflow{}
	if err := proto.Unmarshal(plain, whole); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(whole, planned) {
		t.Fatal("the concatenated parts do not merge back into the definition")
	}
	// Part 0 holds no task; every later part holds tasks and nothing else.
	first, err := gunzipBounded(data[:ref.Parts[0].SizeBytes], ref.Parts[0].RawBytes)
	if err != nil {
		t.Fatal(err)
	}
	header := &orkstackv1.Orkflow{}
	if err := proto.Unmarshal(first, header); err != nil || len(header.GetTasks()) != 0 || header.GetName() != "large" {
		t.Fatalf("part 0 = %d tasks, name %q (%v); want the header with no task", len(header.GetTasks()), header.GetName(), err)
	}
	// Part by part, as the await doors decode it.
	decoded, err := DecodeDefinitionObject(data, &ref, DefinitionStub(planned))
	if err != nil || !proto.Equal(decoded, planned) {
		t.Fatalf("DecodeDefinitionObject = %v; want the definition", err)
	}
	if _, err := DecodeDefinitionObject(data, &ref, &orkstackv1.Orkflow{Name: "large", Version: "v2", Namespace: "team-a",
		DefinitionDigest: planned.GetDefinitionDigest()}); err == nil {
		t.Fatal("a definition decoded against another version's stub was accepted")
	}
	altered := bytes.Clone(data)
	altered[len(altered)/2] ^= 0xff
	if _, err := DecodeDefinitionObject(altered, &ref, DefinitionStub(planned)); err == nil {
		t.Fatal("an altered object was accepted")
	}
}

// definitionPlan is refusalRoutedPlan naming data's definition by reference.
func definitionPlan(t *testing.T, ref DefinitionRef) RunPlan {
	plan := refusalRoutedPlan(t)
	plan.Definition = &ref
	return plan
}

// TestDefinitionLoad_FitsTheDeadlockBudget: a definition at the 2 MiB bound
// (a quarter of the default copy cap) loads through the bootstrap's batches
// with the production 1 s deadlock detector, and the slowest batch's
// workflow-side verify and decode — the per-batch work the detector times —
// is quoted, under the bound the saved-state restore keeps.
func TestDefinitionLoad_FitsTheDeadlockBudget(t *testing.T) {
	planned := largeDefinition(t, 120, 32<<10)
	data, ref, err := EncodeDefinitionObject(planned)
	if err != nil {
		t.Fatal(err)
	}
	limit := DefaultContinuationBytes / 4
	if ref.SizeBytes > limit || ref.SizeBytes < limit*3/4 {
		t.Fatalf("the definition is %d bytes stored, want close to and within the %d bound", ref.SizeBytes, limit)
	}
	store := newMemSnapshotStore()
	if _, err := store.WriteContinuation(context.Background(), "team-a", refusalWorkflowID, 0,
		artifacts.ContinuationKindDefinition, data); err != nil {
		t.Fatal(err)
	}
	h := newRefusalHarnessOver(t, store)
	h.env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: time.Second})
	started := time.Now()
	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, DefinitionStub(planned), (*orkstackv1.Payload)(nil),
		definitionPlan(t, ref), (*orkstackv1.RunContinuation)(nil))
	// Its 120 large inputs may take the run past its budget: carrying on is
	// as good an end as succeeding here.
	var can *workflow.ContinueAsNewError
	if err := h.env.GetWorkflowError(); err != nil && !errors.As(err, &can) {
		t.Fatalf("a run with its %d-byte definition passed by reference failed: %v", ref.SizeBytes, err)
	}
	elapsed := time.Since(started)

	var merged *orkstackv1.Orkflow
	decoder := &partDecoder{refs: ref.object().GetParts(), decode: func(index int, plain []byte) error {
		part := &orkstackv1.Orkflow{}
		if err := proto.Unmarshal(plain, part); err != nil {
			return err
		}
		return mergeDefinitionPart(&merged, index, part)
	}}
	batch := int(DefaultContinuationChunksPerTask * DefaultContinuationChunkBytes)
	// Bounded on the decode thread's CPU time (passClock), as the restore is.
	clock := &passClock{}
	var worst time.Duration
	for off := 0; off < len(data); off += batch {
		end := min(len(data), off+batch)
		begin := clock.now()
		for chunk := off; chunk < end; chunk += int(DefaultContinuationChunkBytes) {
			if err := decoder.feed(data[chunk:min(end, chunk+int(DefaultContinuationChunkBytes))]); err != nil {
				t.Fatal(err)
			}
		}
		worst = max(worst, clock.now()-begin)
	}
	if !proto.Equal(merged, planned) {
		t.Fatal("the batches did not decode the definition")
	}
	bound := restoreBatchDecodeBound
	if raceBuild {
		bound *= raceSlowdown
	}
	if worst > bound {
		t.Fatalf("the slowest batch decoded in %s of %s, over %s", worst, clock.kind(), bound)
	}
	t.Logf("a %d-byte definition (%d parts) loaded and ran in %s; slowest batch decode %s (bound %s)",
		ref.SizeBytes, len(ref.Parts), elapsed, worst, bound)
	if !raceBuild {
		proof.WriteJSON(t, "definition-by-reference", "definition-load-deadlock-budget.json", map[string]any{
			"definition_bytes": ref.SizeBytes, "parts": len(ref.Parts), "slowest_batch_ms": float64(worst.Microseconds()) / 1000,
			"bound_ms": bound.Milliseconds(), "run_seconds": elapsed.Seconds(),
		})
	}
}

// TestLoadDefinition_NeverPanicsOnAMalformedPlan (implementer note 2): the
// definition load reads the plan's budget and reference before anything
// else, so a plan naming a definition with no budget, an invalid one, or a
// reference that is not well formed ends the run RUN_DEFINITION_CORRUPT
// with its terminal events — never a panic that retries the workflow task
// forever — and reads nothing.
func TestLoadDefinition_NeverPanicsOnAMalformedPlan(t *testing.T) {
	planned := largeDefinition(t, 3, 64)
	_, good, err := EncodeDefinitionObject(planned)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(p *RunPlan)
	}{
		{"no budget", func(p *RunPlan) { p.Continuation = nil }},
		{"invalid budget", func(p *RunPlan) { b := *p.Continuation; b.ChunkBytes = 0; p.Continuation = &b }},
		{"no size", func(p *RunPlan) { p.Definition.SizeBytes = 0 }},
		{"over the cap", func(p *RunPlan) { p.Definition.SizeBytes = p.Continuation.CheckpointMaxBytes + 1 }},
		{"bad digest", func(p *RunPlan) { p.Definition.SHA256 = "XYZ" }},
		{"no parts", func(p *RunPlan) { p.Definition.Parts = nil }},
		{"parts not contiguous", func(p *RunPlan) { p.Definition.Parts[0].Offset = 3 }},
		{"part digest", func(p *RunPlan) { p.Definition.Parts[0].SHA256 = "" }},
		{"raw sizes", func(p *RunPlan) { p.Definition.RawBytes++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemSnapshotStore()
			h := newRefusalHarnessOver(t, store)
			ref := good
			ref.Parts = append([]DefinitionPartRef(nil), good.Parts...)
			plan := definitionPlan(t, ref)
			tc.edit(&plan)
			h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, DefinitionStub(planned), (*orkstackv1.Payload)(nil),
				plan, (*orkstackv1.RunContinuation)(nil))
			var applicationErr *temporal.ApplicationError
			if err := h.env.GetWorkflowError(); !errors.As(err, &applicationErr) || applicationErr.Type() != RunDefinitionCorruptCode {
				t.Fatalf("workflow error = %v, want %s", err, RunDefinitionCorruptCode)
			}
			if len(store.reads) != 0 {
				t.Fatalf("the store was read %d times for a malformed plan", len(store.reads))
			}
			types := []orkstackv1.EventType{}
			for _, event := range h.events {
				types = append(types, event.GetType())
			}
			want := []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED}
			if fmt.Sprint(types) != fmt.Sprint(want) || h.events[1].GetSequence() != 2 {
				t.Fatalf("published %v, want %v at sequences 1 and 2", types, want)
			}
		})
	}
}

// TestContinuationChunkReads_AreTheCheckpointAndDefinitionOnly is the saved-state
// compliance test, extended with the copy budget: the only copy of large bytes into
// history is a generation's bootstrap. It parses internal/engine and follows
// every reference to ActivityReadContinuationChunk up the call graph: it is
// scheduled by readChunkBefore alone, which only readBootstrapObject calls,
// which only the checkpoint's read (restore's) and the definition's load
// call.
func TestContinuationChunkReads_AreTheCheckpointAndDefinitionOnly(t *testing.T) {
	fset, files := engineSources(t)
	referencers := map[string][]string{} // identifier → functions that reference it
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			seen := map[string]bool{}
			ast.Inspect(fn, func(n ast.Node) bool {
				var name string
				switch n := n.(type) {
				case *ast.Ident:
					name = n.Name
				case *ast.SelectorExpr:
					name = n.Sel.Name
				}
				if name != "" && name != fn.Name.Name && !seen[name] {
					seen[name] = true
					referencers[name] = append(referencers[name], fn.Name.Name)
				}
				return true
			})
		}
	}
	for _, link := range []struct {
		callee  string
		callers []string
	}{
		{"ActivityReadContinuationChunk", []string{"readChunkBefore"}},
		{"readChunkBefore", []string{"readBootstrapObject"}},
		{"readBootstrapObject", []string{"loadDefinition", "readCheckpoint"}},
		{"readCheckpoint", []string{"restore"}},
		{"loadDefinition", []string{"loadDefinitionArgument"}},
	} {
		got := append([]string(nil), referencers[link.callee]...)
		sort.Strings(got)
		if fmt.Sprint(got) != fmt.Sprint(link.callers) {
			t.Errorf("%s is referenced from %v, want only %v: a copy of large bytes into history is the bootstrap's "+
				"alone (AGENTS.md §2.2)", link.callee, got, link.callers)
		}
	}
	_ = fset
}
