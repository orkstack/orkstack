package orker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// valuesRecords is a real local record store, and what of it does not answer.
type valuesRecords struct {
	*artifacts.RecordStore
	dir string
	err error
}

func newValuesRecords(t *testing.T) *valuesRecords {
	t.Helper()
	dir := t.TempDir()
	store, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	must.OK(t, err)
	return &valuesRecords{RecordStore: store, dir: dir}
}

func (v *valuesRecords) ReadUpstreamRecord(ctx context.Context, class artifacts.SlotClass, _ *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[artifacts.UpstreamRecord], error) {
	if v.err != nil {
		return artifacts.RecordRead[artifacts.UpstreamRecord]{}, v.err
	}
	return v.RecordStore.ReadUpstreamRecord(ctx, class, &artifacts.RecordSpacings{MissConfirm: 1, ReaskInitial: 1, ReaskMax: 1}, key, want)
}

// record writes a record of in's run as the engine does, with no detail there
// to read, and answers the upstream the interpreter hands a dependent.
func (v *valuesRecords) record(t *testing.T, in *orkstackv1.ExecuteTaskInput, kind artifacts.RecordKind, edit func(*orkstackv1.RecordedValues)) *orkstackv1.UpstreamRef {
	t.Helper()
	ns := artifacts.ResolveNamespace(in.GetNamespace())
	values := &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://objects/v", Sha256: strings.Repeat("c", 64), SizeBytes: 2}
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, Namespace: ns, RunId: in.GetRunId(),
		TaskId: "gate", AdmissionSha256: in.GetIdentity().GetAdmissionSha256(), InputBinding: make([]byte, 32), Values: values, DetailSha256: strings.Repeat("d", 64)}
	up := &orkstackv1.UpstreamRef{TaskId: "gate", Values: values}
	if kind == artifacts.RecordComposed {
		rec.Kind, rec.DetailSha256 = orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, ""
		rec.ChildRunId, rec.ChildAdmissionSha256, rec.ChildOutputsSha256 = "ork-child", strings.Repeat("e", 64), strings.Repeat("f", 64)
		rec.Artifacts = []*orkstackv1.ArtifactRef{{Name: "bundle", Uri: "mem://objects/b", Sha256: strings.Repeat("b", 64), SizeBytes: 3}}
		up.Artifacts = rec.Artifacts
	}
	if edit != nil {
		edit(rec)
	}
	raw, err := artifacts.MarshalRecordedValues(rec)
	must.OK(t, err)
	key, err := artifacts.RecordKey(kind, ns, in.GetRunId(), "gate", 0)
	must.OK(t, err)
	path := filepath.Join(v.dir, filepath.FromSlash(key))
	must.OK(t, os.MkdirAll(filepath.Dir(path), 0o755))
	must.OK(t, os.WriteFile(path, raw, 0o644))
	up.RecordUri = "mem://store/" + key
	return up
}

// TestCheckUpstreamValues: a dependent stages an awaited or a composed upstream
// only as its own record names it, and reads no detail to know.
func TestCheckUpstreamValues(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		kind   artifacts.RecordKind
		record func(*orkstackv1.RecordedValues)
		edit   func(*orkstackv1.UpstreamRef, *valuesRecords)
		reason string
	}{
		{"an awaited task", artifacts.RecordResolution, nil, nil, ""},
		{"a composed task", artifacts.RecordComposed, nil, nil, ""},
		{"a rejection", artifacts.RecordResolution, func(r *orkstackv1.RecordedValues) { r.Rejected, r.Values = true, nil }, nil, ReasonUpstreamNotRecorded},
		{"a record of another task at its key", artifacts.RecordResolution, func(r *orkstackv1.RecordedValues) { r.TaskId = "other" }, nil, ReasonUpstreamNotRecorded},
		{"a record of another run at its key", artifacts.RecordResolution, func(r *orkstackv1.RecordedValues) { r.RunId = "ork-another" }, nil, ReasonUpstreamNotRecorded},
		{"another admission", artifacts.RecordResolution, func(r *orkstackv1.RecordedValues) { r.AdmissionSha256 = strings.Repeat("9", 64) }, nil, ReasonUpstreamNotRecorded},
		{"other values", artifacts.RecordResolution, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) {
			up.Values = &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://objects/forged", Sha256: strings.Repeat("0", 64)}
		}, ReasonUpstreamNotRecorded},
		{"no values", artifacts.RecordResolution, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) { up.Values = nil }, ReasonUpstreamNotRecorded},
		{"another artifact", artifacts.RecordComposed, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) {
			up.Artifacts = []*orkstackv1.ArtifactRef{{Name: "bundle", Uri: "mem://objects/forged", Sha256: strings.Repeat("0", 64)}}
		}, ReasonUpstreamNotRecorded},
		{"an artifact left out", artifacts.RecordComposed, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) { up.Artifacts = nil }, ReasonUpstreamNotRecorded},
		{"another task's record", artifacts.RecordResolution, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) {
			up.RecordUri = strings.Replace(up.RecordUri, "/gate/", "/other/", 1)
		}, ReasonUpstreamNotRecorded},
		{"another run's", artifacts.RecordResolution, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) {
			up.RecordUri = strings.Replace(up.RecordUri, "/ork-export-build/", "/ork-another/", 1)
		}, ReasonUpstreamNotRecorded},
		{"another store's", artifacts.RecordResolution, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) {
			up.RecordUri = strings.Replace(up.RecordUri, "mem://store/", "mem://elsewhere/", 1)
		}, ReasonUpstreamNotRecorded},
		{"where an Orker may write", artifacts.RecordResolution, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) {
			up.RecordUri = strings.Replace(up.RecordUri, artifacts.AttestedPrefix, artifacts.CommitPrefix, 1)
		}, ReasonUpstreamNotRecorded},
		{"a commit beside it", artifacts.RecordResolution, nil, func(up *orkstackv1.UpstreamRef, _ *valuesRecords) { up.CommitUri = "mem://store/x" }, ReasonUpstreamNotRecorded},
		{"no record", artifacts.RecordResolution, nil, func(_ *orkstackv1.UpstreamRef, v *valuesRecords) {
			must.OK(t, os.RemoveAll(filepath.Join(v.dir, "metadata")))
		}, ReasonUpstreamNotRecorded},
		{"the store does not answer", artifacts.RecordResolution, nil, func(_ *orkstackv1.UpstreamRef, v *valuesRecords) { v.err = errors.New("down") }, ReasonCommitReadFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, v := recordedRunInput(t, "build"), newValuesRecords(t)
			up := v.record(t, in, tc.kind, tc.record)
			if tc.edit != nil {
				tc.edit(up, v)
			}
			for _, root := range []string{"", "mem://store"} {
				f := (&OCIDispatcher{Records: v, CommitRoot: root}).checkUpstreamValues(ctx, in, up)
				switch {
				case tc.reason == "" && f != nil:
					t.Fatalf("commit root %q: %v", root, f)
				case tc.reason != "" && (f == nil || f.Reason != tc.reason || !strings.Contains(f.Message, `"gate"`)):
					t.Fatalf("commit root %q: %v, want %s naming the upstream", root, f, tc.reason)
				}
			}
		})
	}
	in, v := recordedRunInput(t, "build"), newValuesRecords(t)
	if f := (&OCIDispatcher{}).checkUpstreamValues(ctx, in, v.record(t, in, artifacts.RecordResolution, nil)); f == nil || f.Reason != ReasonUpstreamNotRecorded {
		t.Fatalf("an Orker with no record store: %v", f)
	}
}

// TestCheckReceived: what a run received is staged only as the engine answers
// it, and nothing of it unasked.
func TestCheckReceived(t *testing.T) {
	ref := func(name, sha string) *orkstackv1.ArtifactRef {
		return &orkstackv1.ArtifactRef{Name: name, Uri: "mem://objects/" + sha, Sha256: strings.Repeat(sha, 64), SizeBytes: 1}
	}
	answer := func(refs ...*orkstackv1.ArtifactRef) *orkstackv1.CommitResultResponse {
		return &orkstackv1.CommitResultResponse{InputArtifacts: refs}
	}
	for _, tc := range []struct {
		name    string
		staged  []*orkstackv1.ArtifactRef
		answer  *orkstackv1.CommitResultResponse
		refused bool
	}{
		{"as answered", []*orkstackv1.ArtifactRef{ref("bundle", "b")}, answer(ref("bundle", "b")), false},
		{"another reference", []*orkstackv1.ArtifactRef{ref("bundle", "0")}, answer(ref("bundle", "b")), true},
		{"one left out", nil, answer(ref("bundle", "b")), true},
		{"a failure handler's, with no record", []*orkstackv1.ArtifactRef{ref("report", "a"), ref("bundle", "b")}, answer(&orkstackv1.ArtifactRef{Name: "report"}, ref("bundle", "b")), false},
		{"a failure handler's, left out", nil, answer(&orkstackv1.ArtifactRef{Name: "report"}), true},
		{"one the engine did not answer", []*orkstackv1.ArtifactRef{ref("loot", "a"), ref("bundle", "b")}, answer(ref("bundle", "b")), true},
		{"one named twice", []*orkstackv1.ArtifactRef{ref("bundle", "b"), ref("bundle", "0")}, answer(ref("bundle", "b")), true},
		{"another task's, where nothing is received", []*orkstackv1.ArtifactRef{ref("loot", "a")}, answer(), true},
		{"unasked", []*orkstackv1.ArtifactRef{ref("bundle", "b")}, nil, true},
	} {
		in := recordedRunInput(t, "build")
		f := checkReceived(in, &orkstackv1.UpstreamRef{TaskId: "_inputs", Artifacts: tc.staged}, tc.answer)
		if (f != nil) != tc.refused || (f != nil && f.Reason != ReasonUpstreamNotRecorded) {
			t.Errorf("%s: %v", tc.name, f)
		}
	}
	in := recordedRunInput(t, "build")
	if f := checkReceived(in, &orkstackv1.UpstreamRef{TaskId: "_inputs", RecordUri: "x"}, answer()); f == nil {
		t.Error("an _inputs with a record")
	}
	if receives(in) {
		t.Error("a task that receives nothing")
	}
	in.Upstream = append(in.Upstream, &orkstackv1.UpstreamRef{TaskId: "_inputs"})
	if !receives(in) {
		t.Error("a task that receives")
	}
}

// TestStageInputs_ARecordedRunStagesOnlyWhatIsRecorded: each kind of upstream
// is held to its own record or answer before anything of it is staged.
func TestStageInputs_ARecordedRunStagesOnlyWhatIsRecorded(t *testing.T) {
	for name, tc := range map[string]struct {
		kind    artifacts.RecordKind
		edit    func(*orkstackv1.UpstreamRef)
		answer  *orkstackv1.ArtifactRef
		refused bool
	}{
		"an awaited task":                 {kind: artifacts.RecordResolution},
		"a composed task":                 {kind: artifacts.RecordComposed},
		"an awaited task, other values":   {artifacts.RecordResolution, func(up *orkstackv1.UpstreamRef) { up.Values = otherDigest(up.Values, "0") }, nil, true},
		"an awaited task, no record uri":  {artifacts.RecordResolution, func(up *orkstackv1.UpstreamRef) { up.RecordUri = "" }, nil, true},
		"a composed task, another object": {artifacts.RecordComposed, func(up *orkstackv1.UpstreamRef) { up.Artifacts[0] = otherDigest(up.Artifacts[0], "0") }, nil, true},
		"what the run received":           {answer: &orkstackv1.ArtifactRef{Name: "bundle"}},
		"received, and not answered":      {answer: &orkstackv1.ArtifactRef{Name: "other"}, refused: true},
	} {
		t.Run(name, func(t *testing.T) {
			h, in, v := newCacheHarness(t), recordedRunInput(t, "build"), newValuesRecords(t)
			h.dispatcher.Records, h.dispatcher.CommitRoot = v, "mem://store"
			in.With = nil
			bundle := h.put(t, "bundle", []byte("bytes"))
			var answer *orkstackv1.CommitResultResponse
			up := &orkstackv1.UpstreamRef{TaskId: "_inputs", Artifacts: []*orkstackv1.ArtifactRef{bundle}}
			if tc.answer != nil {
				answer = &orkstackv1.CommitResultResponse{InputArtifacts: []*orkstackv1.ArtifactRef{tc.answer}}
			} else {
				values := h.put(t, "values.json", []byte(`{"ok":true}`))
				up = v.record(t, in, tc.kind, func(r *orkstackv1.RecordedValues) {
					if r.Values = values; tc.kind == artifacts.RecordComposed {
						r.Artifacts = []*orkstackv1.ArtifactRef{bundle}
					}
				})
				up.Values = values
				if tc.kind == artifacts.RecordComposed {
					up.Artifacts = []*orkstackv1.ArtifactRef{bundle}
				}
			}
			if tc.edit != nil {
				tc.edit(up)
			}
			in.Upstream = []*orkstackv1.UpstreamRef{up}
			ws, err := NewWorkspace(t.TempDir())
			must.OK(t, err)
			staged, f := h.dispatcher.stageInputs(context.Background(), in, ws, false, nil, answer)
			if tc.refused {
				if f == nil || f.Reason != ReasonUpstreamNotRecorded {
					t.Fatalf("staged %v, %v", staged, f)
				}
				entries, err := os.ReadDir(filepath.Join(ws.Input, "artifacts"))
				if err == nil && len(entries) != 0 {
					t.Fatalf("a refused upstream was staged: %v", entries)
				}
				return
			}
			if f != nil {
				t.Fatal(f)
			}
			if len(up.Artifacts) > 0 {
				b, err := os.ReadFile(filepath.Join(ws.Input, "artifacts", up.TaskId, "bundle"))
				if err != nil || string(b) != "bytes" {
					t.Fatalf("staged %q, %v", b, err)
				}
			}
		})
	}
}

func otherDigest(ref *orkstackv1.ArtifactRef, sha string) *orkstackv1.ArtifactRef {
	return &orkstackv1.ArtifactRef{Name: ref.GetName(), Uri: ref.GetUri(), Sha256: strings.Repeat(sha, 64), SizeBytes: ref.GetSizeBytes()}
}
