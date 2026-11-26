package identity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// valuesWorld is the recorder over the real local record store: ork-1's task
// sub composes ork-2, whose build always runs and whose outputs read build,
// its approval gate, and extra, which may be skipped.
type valuesWorld struct {
	*followsWorld
	starts   *startWorld
	rec      *ValuesRecorder
	child    artifacts.Admission
	childSHA string
	wf       *orkstackv1.Orkflow
	rendered []OutputsRender
	raw      []byte
	renderEr error
	stored   int
}

func newValuesWorld(t *testing.T, edit func(w *valuesWorld)) *valuesWorld {
	t.Helper()
	w := &valuesWorld{followsWorld: newFollowsWorld(t), raw: []byte(`{"digest":"` + strings.Repeat("x", 8) + `"}`)}
	w.adm.ValueRecords, w.adm.Children = 1, map[string]string{"sub": "ork-2"}
	w.adm.Follows["sub"] = []artifacts.FollowRef{{TaskID: "approve", Kind: artifacts.FollowAwaited}}
	w.wf = &orkstackv1.Orkflow{Name: "deploy", Version: "1", DefinitionDigest: "sha256:" + strings.Repeat("d", 64),
		Outputs: map[string]string{"digest": "{{ .tasks.build.output.digest }}", "by": "{{ .tasks.gate.output.by }}",
			"extra": `{{ with index .tasks "extra" }}{{ .output.n }}{{ end }}`},
		OutputArtifacts: map[string]*orkstackv1.OutputArtifact{"bundle": {TaskId: "build", Name: "app.tar"},
			"through": {TaskId: "_inputs", Name: "src"}},
		Tasks: []*orkstackv1.Task{{Id: "build"}, {Id: "gate", Await: &orkstackv1.AwaitSpec{}}, {Id: "extra"},
			{Id: "inner", Orkflow: &orkstackv1.OrkflowRef{Name: "x", Version: "1"}}}}
	w.child, _ = admitted(t, "ork-2", artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	w.child.ValueRecords, w.child.RootRunID = 1, "ork-1"
	w.child.Completes = []string{"build", "gate"}
	w.child.StartFollows = []artifacts.FollowRef{{RunID: "ork-1", TaskID: "approve", Kind: artifacts.FollowAwaited}}
	w.child.StartArtifacts = map[string]artifacts.StartArtifact{"src": {RunID: "ork-1", TaskID: "build", Kind: artifacts.FollowContainer, Name: "src.tar"}}
	if edit != nil {
		edit(w)
	}
	w.sha = w.admission(w.adm)
	w.child.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-1", AdmissionSHA256: w.sha, TaskID: "sub"}
	w.childSHA = w.admission(w.child)
	w.starts = &startWorld{
		executions: map[ExecutionRef]ExecutionFacts{{WorkflowID: "ork-1", ExecutionID: "p2"}: {WorkflowID: "ork-1", Open: true, ExecutionID: "p2", FirstExecutionID: "p1"}},
		admissions: map[string]artifacts.RecordRead[artifacts.Admission]{"ork-1/" + w.sha: found(w.adm)},
		chains:     map[string]artifacts.RecordRead[artifacts.Chain]{"ork-1": found(artifacts.Chain{FirstExecutionID: "p1", AdmissionSHA256: w.sha})},
	}
	w.rec = &ValuesRecorder{Records: w.store, Spacing: fastSpacings, Starts: w.starts,
		Describe: func(_ context.Context, run string) (ExecutionFacts, error) {
			if run == "ork-1" {
				return ExecutionFacts{WorkflowID: "ork-1", Open: true, ExecutionID: "p2", FirstExecutionID: "p1"}, nil
			}
			return ExecutionFacts{WorkflowID: run, Open: true, ExecutionID: "c1", FirstExecutionID: "c1", Parent: &ExecutionRef{WorkflowID: "ork-1", ExecutionID: "p2"}}, nil
		},
		Definition: AdmittedDefinitionFrom(func(context.Context, string, string, string) (*orkstackv1.Orkflow, error) { return w.wf, nil }),
		Render: func(_ context.Context, r OutputsRender) ([]byte, error) {
			w.rendered = append(w.rendered, r)
			return w.raw, w.renderEr
		},
		Store: func(_ context.Context, _ artifacts.Admission, raw []byte) (*orkstackv1.ArtifactRef, error) {
			w.stored++
			return w.values(raw), nil
		},
		EngineBuild: "b9", Now: func() time.Time { return started }}
	return w
}

// values stores raw and answers where.
func (w *valuesWorld) values(raw []byte) *orkstackv1.ArtifactRef {
	sum := sha256.Sum256(raw)
	key := "artifacts/values/" + hex.EncodeToString(sum[:])
	w.put(key, raw)
	return &orkstackv1.ArtifactRef{Name: "values.json", Uri: "file://" + filepath.Join(w.dir, key), Sha256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(raw))}
}

func (w *valuesWorld) input() []byte {
	s, err := structpb.NewStruct(map[string]any{"env": "prod"})
	if err != nil {
		w.t.Fatal(err)
	}
	raw, err := StartInput(&orkstackv1.Payload{Data: s})
	if err != nil {
		w.t.Fatal(err)
	}
	return raw
}

func withArtifact(name string) func(*orkstackv1.ActivityCommit) {
	return func(c *orkstackv1.ActivityCommit) {
		c.Values.Uri = "file:///values-" + name
		c.Artifacts = []*orkstackv1.ArtifactRef{{Name: name, Uri: "file:///" + name, Sha256: strings.Repeat("f", 64), SizeBytes: 7}}
	}
}

// ready puts what the parent's sub and the child's outputs follow on record.
func (w *valuesWorld) ready() {
	w.commit("ork-1", w.sha, "build", 0, "c", withArtifact("src.tar"))
	w.resolve("approve", 0, false, map[string]string{"build": "c"})
	w.commit("ork-2", w.childSHA, "build", 0, "a", withArtifact("app.tar"))
	w.resolveIn("ork-2", w.childSHA, "gate", false)
}

func (w *valuesWorld) resolveIn(run, sha, task string, rejected bool) {
	w.t.Helper()
	detail, err := artifacts.MarshalResolutionDetail(&orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "ns",
		RunId: run, TaskId: task, Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice"})
	if err != nil {
		w.t.Fatal(err)
	}
	sum := sha256.Sum256(detail)
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
		Namespace: "ns", RunId: run, TaskId: task, AdmissionSha256: sha, InputBinding: make([]byte, 32), Rejected: rejected,
		DetailSha256: hex.EncodeToString(sum[:])}
	if !rejected {
		rec.Values = &orkstackv1.ArtifactRef{Uri: "file:///gate", Sha256: strings.Repeat("e", 64), SizeBytes: 5000}
	}
	w.record(artifacts.RecordResolution, rec)
	key, err := artifacts.ResolutionDetailKey("ns", run, task, rec.DetailSha256)
	if err != nil {
		w.t.Fatal(err)
	}
	w.put(key, detail)
}

func (w *valuesWorld) outputsAsk() OutputsAsk {
	return OutputsAsk{Namespace: "ns", RunID: "ork-2", AdmissionSHA256: w.childSHA, Input: w.input(), MaxBytes: 1 << 20,
		Offered: map[string]*orkstackv1.ArtifactRef{
			"build": {Uri: "file:///anywhere", Sha256: strings.Repeat("a", 64)},
			"gate":  {Uri: "file:///gate", Sha256: strings.Repeat("e", 64)}}}
}

func (w *valuesWorld) composedAsk(sha string) ComposedAsk {
	return ComposedAsk{Namespace: "ns", RunID: "ork-1", AdmissionSHA256: w.sha, TaskID: "sub", Input: w.input(),
		ChildAdmissionSHA256: w.childSHA, ValuesSHA256: sha}
}

func (w *valuesWorld) outputsKey() string {
	key, err := artifacts.OutputsRecordKey("ns", "ork-2")
	if err != nil {
		w.t.Fatal(err)
	}
	return key
}

func refusedWith(t *testing.T, err error, reason, says string) {
	t.Helper()
	var r *ValuesRefusal
	if !errors.As(err, &r) || r.Reason != reason || !strings.Contains(r.Message, says) {
		t.Fatalf("%v; want %s saying %q", err, reason, says)
	}
}

func unansweredWith(t *testing.T, err error, says string) {
	t.Helper()
	var r *ValuesRefusal
	if err == nil || errors.As(err, &r) || !strings.Contains(err.Error(), says) {
		t.Fatalf("%v; want unanswered saying %q", err, says)
	}
}

// TestRecordOutputs: a child's outputs are rendered from the admitted
// definition, over the values its tasks' records name, and recorded with
// those records and the artifacts they name.
func TestRecordOutputs(t *testing.T) {
	ctx := context.Background()
	w := newValuesWorld(t, nil)
	w.ready()
	got, err := w.rec.RecordOutputs(ctx, w.outputsAsk())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Values, w.raw) || got.ValuesRef.GetUri() == "" || len(got.Artifacts) != 2 ||
		got.Artifacts[0].GetName() != "bundle" || got.Artifacts[0].GetUri() != "file:///app.tar" ||
		got.Artifacts[1].GetName() != "through" || got.Artifacts[1].GetUri() != "file:///src.tar" {
		t.Fatalf("%+v", got)
	}
	if len(w.rendered) != 1 || w.rendered[0].Definition != w.wf || w.rendered[0].MaxBytes != 1<<20 || len(w.rendered[0].Upstream) != 2 {
		t.Fatalf("%+v", w.rendered)
	}
	if up := w.rendered[0].Upstream; up["build"].GetUri() != "file:///values-app.tar" || up["build"].GetSha256() != strings.Repeat("a", 64) ||
		up["gate"].GetUri() != "file:///gate" || up["gate"].GetSha256() != strings.Repeat("e", 64) {
		t.Fatalf("%v", up)
	}
	read, err := w.store.ReadRecordedValues(ctx, artifacts.SlotRun, fastSpacings, w.outputsKey(), artifacts.RecordWant{
		Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, Namespace: "ns", RunID: "ork-2", AdmissionSHA256: w.childSHA})
	if err != nil || read.Answer != artifacts.RecordFound || read.Value.RecordSHA256() != got.RecordSHA256 {
		t.Fatalf("%+v, %v", read, err)
	}
	adopted, _ := read.Value.InTime(read.Value.StoredAt(), time.Time{})
	rec := adopted.Record()
	nonce, _ := hex.DecodeString(w.child.Nonce)
	if !InputBindingMatches(nonce, w.input(), rec.GetInputBinding()) || rec.GetDefinitionDigest() != w.wf.DefinitionDigest ||
		rec.GetRecorded().GetEngineBuild() != "b9" || len(rec.GetSources()) != 3 {
		t.Fatalf("%v", rec)
	}
	for i, want := range []string{"build", "extra", "gate"} {
		row := rec.GetSources()[i]
		if row.GetTaskId() != want || row.GetAbsent() != (want == "extra") || (row.GetRecordSha256() == "") != (want == "extra") {
			t.Fatalf("row %d: %v", i, row)
		}
	}
	// Run twice, as a local activity may be: the first record stands.
	again, err := w.rec.RecordOutputs(ctx, w.outputsAsk())
	if err != nil || again.RecordSHA256 != got.RecordSHA256 {
		t.Fatalf("%+v, %v", again, err)
	}
	// Other values are not what is on record.
	w.raw = []byte(`{"digest":"other"}`)
	_, err = w.rec.RecordOutputs(ctx, w.outputsAsk())
	refusedWith(t, err, ReasonOutputsNotRecorded, "other outputs")
}

func TestRecordOutputs_AChildThatDeclaresNone(t *testing.T) {
	w := newValuesWorld(t, func(w *valuesWorld) {
		w.wf.Outputs, w.wf.OutputArtifacts = nil, nil
		w.raw = []byte(`{}`)
	})
	w.ready()
	got, err := w.rec.RecordOutputs(context.Background(), w.outputsAsk())
	if err != nil || string(got.Values) != `{}` || len(w.rendered) != 0 || got.Artifacts != nil {
		t.Fatalf("%+v, %v, rendered %d", got, err, len(w.rendered))
	}
}

// TestRecordOutputs_AComposedSource: a composed task's values and artifacts
// are its record's.
func TestRecordOutputs_AComposedSource(t *testing.T) {
	w := newValuesWorld(t, func(w *valuesWorld) {
		w.wf.Outputs = map[string]string{"n": "{{ .tasks.inner.output.n }}"}
		w.wf.OutputArtifacts = map[string]*orkstackv1.OutputArtifact{"notes": {TaskId: "inner", Name: "notes.md"}}
	})
	w.ready()
	w.record(artifacts.RecordComposed, &orkstackv1.RecordedValues{SchemaVersion: 1,
		Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "ns", RunId: "ork-2", TaskId: "inner",
		AdmissionSha256: w.childSHA, InputBinding: make([]byte, 32), ChildRunId: "ork-9", ChildAdmissionSha256: otherSHA,
		ChildOutputsSha256: otherSHA, Values: &orkstackv1.ArtifactRef{Uri: "file:///inner", Sha256: strings.Repeat("9", 64), SizeBytes: 5000},
		Artifacts: []*orkstackv1.ArtifactRef{{Name: "notes.md", Uri: "file:///notes", Sha256: strings.Repeat("1", 64)}}})
	ask := w.outputsAsk()
	ask.Offered["inner"] = &orkstackv1.ArtifactRef{Uri: "file:///elsewhere", Sha256: strings.Repeat("9", 64)}
	got, err := w.rec.RecordOutputs(context.Background(), ask)
	if err != nil || len(got.Artifacts) != 1 || got.Artifacts[0].GetName() != "notes" || w.rendered[0].Upstream["inner"].GetUri() != "file:///inner" {
		t.Fatalf("%+v, %v", got, err)
	}
}

// TestRecordOutputs_Refuses: what is not on record, or not as the run says,
// ends the run OUTPUTS_NOT_RECORDED, and nothing is written.
func TestRecordOutputs_Refuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*valuesWorld)
		make func(*valuesWorld, *OutputsAsk)
		says string
	}{
		{"the start's approval is gone", nil, func(w *valuesWorld, _ *OutputsAsk) {
			w.removeRecord(artifacts.RecordResolution, "ork-1", "approve")
		}, "approve"},
		{"an approval of its own was skipped", nil, func(w *valuesWorld, a *OutputsAsk) {
			w.removeRecord(artifacts.RecordResolution, "ork-2", "gate")
		}, `"gate"`},
		{"an always-running task nothing reads has no record", func(w *valuesWorld) { w.child.Completes = append(w.child.Completes, "inner") }, nil, `"inner"`},
		{"an always-running task is called absent", nil, func(_ *valuesWorld, a *OutputsAsk) { delete(a.Offered, "gate") }, `"gate"`},
		{"an always-running task was rejected", nil, func(w *valuesWorld, _ *OutputsAsk) { w.resolveIn("ork-2", w.childSHA, "gate", true) }, "rejected"},
		{"an approval nothing reads was rejected", func(w *valuesWorld) {
			delete(w.wf.Outputs, "by")
		}, func(w *valuesWorld, a *OutputsAsk) {
			delete(a.Offered, "gate")
			w.resolveIn("ork-2", w.childSHA, "gate", true)
		}, "rejected"},
		{"a record its outputs read does not decode", nil, func(w *valuesWorld, _ *OutputsAsk) {
			key, _ := artifacts.RecordKey(artifacts.RecordResolution, "ns", "ork-2", "gate", 0)
			w.put(key, []byte("{"))
		}, "gate"},
		{"a record its start follows does not decode", nil, func(w *valuesWorld, _ *OutputsAsk) {
			key, _ := artifacts.RecordKey(artifacts.RecordResolution, "ns", "ork-1", "approve", 0)
			w.put(key, []byte("{"))
		}, "approve"},
		{"its admission does not decode", nil, func(w *valuesWorld, _ *OutputsAsk) {
			key, _ := artifacts.AdmissionKey("ns", "ork-2", w.childSHA)
			w.put(key, []byte("{"))
		}, "admission"},
		{"it has ended", nil, func(w *valuesWorld, _ *OutputsAsk) {
			describe := w.rec.Describe
			w.rec.Describe = func(ctx context.Context, run string) (ExecutionFacts, error) {
				f, err := describe(ctx, run)
				f.Open = false
				return f, err
			}
		}, "not executing"},
		{"other values are offered", nil, func(_ *valuesWorld, a *OutputsAsk) { a.Offered["build"].Sha256 = strings.Repeat("0", 64) }, `"build"`},
		{"values are offered of a task with no record", nil, func(_ *valuesWorld, a *OutputsAsk) {
			a.Offered["extra"] = &orkstackv1.ArtifactRef{Uri: "file:///x", Sha256: strings.Repeat("4", 64)}
		}, `"extra"`},
		{"the producer's record names no such artifact", nil, func(w *valuesWorld, _ *OutputsAsk) {
			w.commit("ork-2", w.childSHA, "build", 0, "a", withArtifact("other.tar"))
		}, "app.tar"},
		{"an artifact's producer did not succeed", func(w *valuesWorld) {
			w.wf.OutputArtifacts["more"] = &orkstackv1.OutputArtifact{TaskId: "extra", Name: "x"}
		}, nil, `"extra"`},
		{"a received artifact has no record", nil, func(w *valuesWorld, _ *OutputsAsk) {
			w.commit("ork-1", w.sha, "build", 0, "c", nil)
		}, "src"},
		{"a received artifact of a failure handler has no record", func(w *valuesWorld) {
			w.child.StartArtifacts["src"] = artifacts.StartArtifact{RunID: "ork-1", TaskID: "other", Kind: artifacts.FollowContainer, Name: "src.tar", Handler: true}
		}, nil, "src"},
		{"an output reads a task the definition does not have", func(w *valuesWorld) { w.wf.Outputs["z"] = "{{ .tasks.nobody.output.n }}" }, nil, `"nobody"`},
		{"an output reads a fan-out", func(w *valuesWorld) { w.wf.Tasks[2].Each = &orkstackv1.EachSpec{} }, nil, `"extra"`},
		{"it is a root", func(w *valuesWorld) {}, func(w *valuesWorld, a *OutputsAsk) { a.RunID, a.AdmissionSHA256 = "ork-1", w.sha }, "no parent"},
		{"another admission", nil, func(_ *valuesWorld, a *OutputsAsk) { a.AdmissionSHA256 = otherSHA }, "no admission"},
		{"another run's execution", nil, func(w *valuesWorld, _ *OutputsAsk) {
			w.rec.Describe = func(context.Context, string) (ExecutionFacts, error) {
				return ExecutionFacts{WorkflowID: "ork-2", Open: true, ExecutionID: "c1", FirstExecutionID: "c1"}, nil
			}
		}, "not started by its parent"},
		{"the stored version is another definition", nil, func(w *valuesWorld, _ *OutputsAsk) { w.wf.DefinitionDigest = "sha256:" + otherSHA }, "definition"},
		{"a resume of no such scope", nil, func(_ *valuesWorld, a *OutputsAsk) { a.Scopes = Scopes{Root: 4} }, "resume 4"},
		{"no record format", func(w *valuesWorld) { w.child.ValueRecords = 0 }, nil, "no record format"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newValuesWorld(t, tc.edit)
			w.ready()
			ask := w.outputsAsk()
			if tc.make != nil {
				tc.make(w, &ask)
			}
			_, err := w.rec.RecordOutputs(context.Background(), ask)
			refusedWith(t, err, ReasonOutputsNotRecorded, tc.says)
			if _, err := os.Stat(filepath.Join(w.dir, w.outputsKey())); !os.IsNotExist(err) || w.stored != 0 {
				t.Fatalf("stored %d, record: %v", w.stored, err)
			}
		})
	}
}

func (w *valuesWorld) removeRecord(kind artifacts.RecordKind, run, task string) {
	w.t.Helper()
	key, err := artifacts.RecordKey(kind, "ns", run, task, 0)
	if err != nil {
		w.t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(w.dir, filepath.FromSlash(key))); err != nil {
		w.t.Fatal(err)
	}
}

// TestRecordOutputs_ARenderThatFails: the render's own error is the caller's.
func TestRecordOutputs_ARenderThatFails(t *testing.T) {
	w := newValuesWorld(t, nil)
	w.ready()
	w.renderEr = errors.New("outputs.digest: render: no such key")
	if _, err := w.rec.RecordOutputs(context.Background(), w.outputsAsk()); !errors.Is(err, w.renderEr) || w.stored != 0 {
		t.Fatalf("%v, stored %d", err, w.stored)
	}
}

// failingRecords is a store whose named calls do not answer.
type failingRecords struct {
	*artifacts.RecordStore
	tasks, write, read bool
}

func (f failingRecords) TasksOnRecord(ctx context.Context, class artifacts.SlotClass, ns, run string, tasks []string) (map[string]bool, error) {
	if f.tasks {
		return nil, errDown
	}
	return f.RecordStore.TasksOnRecord(ctx, class, ns, run, tasks)
}

func (f failingRecords) WriteRecordedValues(ctx context.Context, rec *orkstackv1.RecordedValues) (string, error) {
	if f.write {
		return "", errDown
	}
	return f.RecordStore.WriteRecordedValues(ctx, rec)
}

func (f failingRecords) ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, sp *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error) {
	if f.read {
		return artifacts.RecordRead[*artifacts.RecordedFound]{}, errDown
	}
	return f.RecordStore.ReadRecordedValues(ctx, class, sp, key, want)
}

// TestRecordValues_AStoreThatDoesNotAnswer: never a refusal, and the store's
// own answer is kept for its class.
func TestRecordValues_AStoreThatDoesNotAnswer(t *testing.T) {
	ctx := context.Background()
	for name, f := range map[string]failingRecords{"the listing": {tasks: true}, "the write": {write: true}, "a read": {read: true}} {
		t.Run(name, func(t *testing.T) {
			w := newValuesWorld(t, nil)
			w.ready()
			f.RecordStore = w.store
			w.rec.Records = f
			_, err := w.rec.RecordOutputs(ctx, w.outputsAsk())
			unansweredWith(t, err, "bucket unreachable")
			var un *artifacts.RecordStoreUnanswered
			if !errors.As(err, &un) || un.Class != artifacts.UnansweredUnavailable {
				t.Fatalf("%v: its class is lost", err)
			}
		})
	}
	w := newValuesWorld(t, nil)
	w.ready()
	if _, err := w.rec.RecordOutputs(ctx, w.outputsAsk()); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]failingRecords{"a composed write": {write: true}, "a composed read": {read: true}} {
		t.Run(name, func(t *testing.T) {
			f.RecordStore = w.store
			w.rec.Records = f
			_, err := w.rec.RecordComposed(ctx, w.composedAsk(hexSum(w.raw)))
			unansweredWith(t, err, "bucket unreachable")
		})
	}
	w.rec.Records = w.store
	w.rec.Describe = func(context.Context, string) (ExecutionFacts, error) {
		return ExecutionFacts{}, errors.New("frontend down")
	}
	_, err := w.rec.RecordComposed(ctx, w.composedAsk(hexSum(w.raw)))
	unansweredWith(t, err, "frontend down")
}

// TestRecordValues_AStepThatWasNotAnsweredKeepsWhatItWasAnswered: for its
// class.
func TestRecordValues_AStepThatWasNotAnsweredKeepsWhatItWasAnswered(t *testing.T) {
	denied := &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredDenied, Err: errors.New("403")}
	for name, make := range map[string]func(*valuesWorld){
		"the store of the values": func(w *valuesWorld) {
			w.rec.Store = func(context.Context, artifacts.Admission, []byte) (*orkstackv1.ArtifactRef, error) {
				return nil, denied
			}
		},
		"the start check":   func(w *valuesWorld) { w.starts.describeErr = denied },
		"the start's chain": func(w *valuesWorld) { w.starts.chainErr = denied },
		"the describe of the run": func(w *valuesWorld) {
			w.rec.Describe = func(context.Context, string) (ExecutionFacts, error) { return ExecutionFacts{}, denied }
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newValuesWorld(t, nil)
			w.ready()
			make(w)
			_, err := w.rec.RecordOutputs(context.Background(), w.outputsAsk())
			unansweredWith(t, err, "403")
			if artifacts.UnansweredClass(err) != artifacts.UnansweredDenied {
				t.Fatalf("%v: its class is lost", err)
			}
		})
	}
}

func hexSum(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestRecordComposed: a composed task's record takes its child's recorded
// outputs, values and artifacts alike, over what the task follows.
func TestRecordComposed(t *testing.T) {
	ctx := context.Background()
	w := newValuesWorld(t, nil)
	w.ready()
	out, err := w.rec.RecordOutputs(ctx, w.outputsAsk())
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.rec.RecordComposed(ctx, w.composedAsk(hexSum(w.raw)))
	if err != nil || got.ValuesRef.GetUri() != out.ValuesRef.GetUri() || len(got.Artifacts) != 2 || got.Artifacts[0].GetName() != "bundle" {
		t.Fatalf("%+v, %v", got, err)
	}
	again, err := w.rec.RecordComposed(ctx, w.composedAsk(hexSum(w.raw)))
	if err != nil || again.RecordSHA256 != got.RecordSHA256 {
		t.Fatalf("%+v, %v", again, err)
	}
	key, _ := artifacts.RecordKey(artifacts.RecordComposed, "ns", "ork-1", "sub", 0)
	read, err := w.store.ReadRecordedValues(ctx, artifacts.SlotRun, fastSpacings, key, artifacts.RecordWant{
		Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "ns", RunID: "ork-1", TaskID: "sub", AdmissionSHA256: w.sha})
	if err != nil || read.Answer != artifacts.RecordFound {
		t.Fatalf("%+v, %v", read, err)
	}
	run, adm, outputs := read.Value.Child()
	if run != "ork-2" || adm != w.childSHA || outputs != out.RecordSHA256 {
		t.Fatalf("%s %s %s", run, adm, outputs)
	}
	if _, _, follows := read.Value.Standing(); len(follows) != 1 || follows[0].GetTaskId() != "approve" {
		t.Fatalf("follows %v", follows)
	}
	// What follows sub now counts its artifacts in its result.
	w.adm.Follows["deploy"] = []artifacts.FollowRef{{TaskID: "sub", Kind: artifacts.FollowComposed}}
	rows, err := w.check.CheckFollows(ctx, w.adm, w.sha, "deploy", 0)
	want, _ := ResultSHA256(hexSum(w.raw), resultArtifacts(got.Artifacts))
	if err != nil || rows[0].ResultSHA256 != want {
		t.Fatalf("%v, %v", rows, err)
	}
}

// TestRecordComposed_Refuses: anything but this child's recorded outputs.
func TestRecordComposed_Refuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(*valuesWorld, *ComposedAsk)
		says string
	}{
		{"no outputs on record", func(w *valuesWorld, _ *ComposedAsk) { w.remove(w.outputsKey()) }, "child run ork-2 ended, and no engine recorded the outputs it returned"},
		{"other values", func(_ *valuesWorld, a *ComposedAsk) { a.ValuesSHA256 = otherSHA }, "other values"},
		{"an outputs record that does not decode", func(w *valuesWorld, _ *ComposedAsk) { w.put(w.outputsKey(), []byte("{")) }, "a record is there and is not this child's outputs"},
		{"another admission of the child", func(_ *valuesWorld, a *ComposedAsk) { a.ChildAdmissionSHA256 = otherSHA }, "no admission"},
		{"a child another task composes", func(w *valuesWorld, a *ComposedAsk) {
			w.child.Starter.TaskID = "other"
			a.ChildAdmissionSHA256 = w.admission(w.child)
		}, "not this task's"},
		{"a task that composes nothing", func(_ *valuesWorld, a *ComposedAsk) { a.TaskID = "build" }, "composes no run"},
		{"a scope no resume gave it", func(_ *valuesWorld, a *ComposedAsk) { a.Scopes.Resume = 42 }, ""},
		{"what it follows is not on record", func(w *valuesWorld, _ *ComposedAsk) {
			w.removeRecord(artifacts.RecordResolution, "ork-1", "approve")
		}, "approve"},
		{"a record there of another child", func(w *valuesWorld, _ *ComposedAsk) {
			w.record(artifacts.RecordComposed, &orkstackv1.RecordedValues{SchemaVersion: 1,
				Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "ns", RunId: "ork-1", TaskId: "sub",
				AdmissionSha256: w.sha, InputBinding: make([]byte, 32), ChildRunId: "ork-9", ChildAdmissionSha256: otherSHA,
				ChildOutputsSha256: otherSHA, Values: &orkstackv1.ArtifactRef{Uri: "file:///none", Sha256: strings.Repeat("e", 64), SizeBytes: 5000}})
		}, "another child"},
	} {
		// Each is refused alike for a resume of a run that has ended.
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%t", tc.name, closed), func(t *testing.T) {
				t.Parallel()
				w := newValuesWorld(t, nil)
				w.ready()
				if _, err := w.rec.RecordOutputs(context.Background(), w.outputsAsk()); err != nil {
					t.Fatal(err)
				}
				if closed {
					describe := w.rec.Describe
					w.rec.Describe = func(ctx context.Context, run string) (ExecutionFacts, error) {
						f, err := describe(ctx, run)
						if run == "ork-1" {
							f.Open = false
						}
						return f, err
					}
				}
				ask := w.composedAsk(hexSum(w.raw))
				ask.Closed = closed
				tc.make(w, &ask)
				_, err := w.rec.RecordComposed(context.Background(), ask)
				refusedWith(t, err, ReasonChildValuesNotRecorded, tc.says)
			})
		}
	}
}

func (w *valuesWorld) remove(key string) {
	w.t.Helper()
	if err := os.Remove(filepath.Join(w.dir, filepath.FromSlash(key))); err != nil {
		w.t.Fatal(err)
	}
}

// countingRecords counts the commits read by their keys.
type countingRecords struct {
	*artifacts.RecordStore
	commits atomic.Int64
}

func (c *countingRecords) ReadAttestedCommit(ctx context.Context, class artifacts.SlotClass, sp *artifacts.RecordSpacings, want artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error) {
	c.commits.Add(1)
	return c.RecordStore.ReadAttestedCommit(ctx, class, sp, want)
}

// TestRecordOutputs_AsksByTheList: a child of many always-running tasks costs
// one listing a range and a read a source, never a read a task.
func TestRecordOutputs_AsksByTheList(t *testing.T) {
	w := newValuesWorld(t, func(w *valuesWorld) {
		for i := range 600 {
			id := fmt.Sprintf("t%03d", i)
			w.child.Completes = append(w.child.Completes, id)
			w.wf.Tasks = append(w.wf.Tasks, &orkstackv1.Task{Id: id})
		}
	})
	w.ready()
	for i := range 600 {
		w.commit("ork-2", w.childSHA, fmt.Sprintf("t%03d", i), 0, "b", nil)
	}
	counted := &countingRecords{RecordStore: w.store}
	w.rec.Records = counted
	if _, err := w.rec.RecordOutputs(context.Background(), w.outputsAsk()); err != nil {
		t.Fatal(err)
	}
	// build and src's producer by commit; the rest by the listing.
	if n := counted.commits.Load(); n != 2 {
		t.Fatalf("%d commits read", n)
	}
}

// TestRecordComposed_OfAClosedRun: a composed task's values are recorded from
// its child's outputs for a run that has ended only when the ask says so (a
// resume of the run, which the cluster still holds); without it, and for a
// run the cluster no longer holds, they are refused.
func TestRecordComposed_OfAClosedRun(t *testing.T) {
	for _, tc := range []struct {
		name   string
		closed bool
		gone   bool
		says   string
	}{
		{"recorded for a resume", true, false, ""},
		{"refused when not asked for a resume", false, false, "not executing"},
		{"refused when the cluster no longer holds it", true, true, "not executing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w := newValuesWorld(t, nil)
			w.ready()
			if _, err := w.rec.RecordOutputs(ctx, w.outputsAsk()); err != nil {
				t.Fatal(err)
			}
			describe := w.rec.Describe
			w.rec.Describe = func(ctx context.Context, run string) (ExecutionFacts, error) {
				f, err := describe(ctx, run)
				if run == "ork-1" {
					if tc.gone {
						return ExecutionFacts{}, ErrNoSuchExecution
					}
					f.Open = false
				}
				return f, err
			}
			ask := w.composedAsk(hexSum(w.raw))
			ask.Closed = tc.closed
			got, err := w.rec.RecordComposed(ctx, ask)
			if tc.says != "" {
				refusedWith(t, err, ReasonChildValuesNotRecorded, tc.says)
				return
			}
			if err != nil || got.RecordSHA256 == "" {
				t.Fatalf("%+v, %v", got, err)
			}
		})
	}
}
