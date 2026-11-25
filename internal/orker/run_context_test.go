package orker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func int32Ptr(v int32) *int32 { return &v }

// testRunContextTree is a root started by a trigger, whose composed task
// "publish" starts a shared child (entry 1), which composes "leaf" (entry 2).
func testRunContextTree() artifacts.RunContextObject {
	return artifacts.RunContextObject{
		Root: artifacts.RunContextRoot{
			Orkflow: artifacts.RunContextOrkflow{Name: "load", Version: "7", DefinitionDigest: "sha256:" + strings.Repeat("a", 64)},
			Origin: artifacts.RunContextOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1", Reference: "https://ci/4411",
				Description: "nightly", Principal: "ci-bot"},
			OriginShared: &artifacts.RunContextOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1", Description: "nightly"},
		},
		Entries: []artifacts.RunContextEntry{
			{},
			{Parent: int32Ptr(0), ParentTaskID: "publish", Orkflow: &artifacts.RunContextOrkflow{Name: "pub", Version: "2"}, Shared: true},
			{Parent: int32Ptr(1), ParentTaskID: "leaf", Orkflow: &artifacts.RunContextOrkflow{Name: "leafy", Version: "1"}},
		},
	}
}

func testRunContextInput(index int32, parent, root string) *orkstackv1.ExecuteTaskInput {
	in := baseExportInput("extract")
	ts := func(s string) *timestamppb.Timestamp {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			panic(err)
		}
		return timestamppb.New(t)
	}
	in.RunContext = &orkstackv1.RunContext{
		ObjectSha256: strings.Repeat("0", 64), TreeIndex: index, ParentRunId: parent, RootRunId: root,
		CreatedAt: ts("2026-11-11T05:00:00.412Z"), StartedAt: ts("2026-11-11T05:00:00.54Z"),
		TaskScheduledAt: ts("2026-11-11T05:00:00.611Z"), RunDeadline: ts("2026-11-11T06:00:00.54Z"),
		DeclaredMaxAttempts: 3, OwnAttemptsBefore: 1,
	}
	return in
}

func TestRunContextTaskJSON_Markers(t *testing.T) {
	tree := testRunContextTree()
	start := time.Date(2026, 11, 11, 5, 0, 3, 20_000_000, time.UTC)
	t.Run("no run context is unavailable, with no attempt keys", func(t *testing.T) {
		run, task, f := runContextTaskJSON(baseExportInput("x"), nil, false, 2, start)
		if f != nil {
			t.Fatal(f)
		}
		if run != (taskJSONRunContext{Context: RunContextUnavailable}) || task != (taskJSONTaskContext{}) {
			t.Fatalf("run %+v task %+v", run, task)
		}
	})
	t.Run("a cached task keeps only its attempt keys", func(t *testing.T) {
		run, task, f := runContextTaskJSON(testRunContextInput(0, "", ""), &tree, true, 2, start)
		if f != nil {
			t.Fatal(f)
		}
		if run != (taskJSONRunContext{Context: RunContextCached}) {
			t.Fatalf("cached run = %+v", run)
		}
		if task != (taskJSONTaskContext{OwnAttempt: 3, MaxAttempts: 3}) {
			t.Fatalf("cached task = %+v", task)
		}
	})
}

func TestRunContextTaskJSON_TopLevelRun(t *testing.T) {
	tree := testRunContextTree()
	start := time.Date(2026, 11, 11, 5, 0, 3, 20_000_000, time.UTC)
	in := testRunContextInput(0, "", "")
	run, task, f := runContextTaskJSON(in, &tree, false, 2, start)
	if f != nil {
		t.Fatal(f)
	}
	if run.Context != RunContextAvailable || run.CreatedAt != "2026-11-11T05:00:00.412Z" ||
		run.StartedAt != "2026-11-11T05:00:00.54Z" || run.Deadline != "2026-11-11T06:00:00.54Z" {
		t.Fatalf("run = %+v", run)
	}
	want := taskJSONRunOrigin{Kind: "TRIGGER", ID: "trg-1", Reference: "https://ci/4411", Description: "nightly", Principal: "ci-bot"}
	if run.Origin == nil || *run.Origin != want || run.Parent != nil {
		t.Fatalf("origin %+v parent %+v", run.Origin, run.Parent)
	}
	if run.Root == nil || run.Root.RunID != in.GetRunId() || run.Root.Origin != want || run.Root.Orkflow.Name != "load" {
		t.Fatalf("root = %+v", run.Root)
	}
	// ownAttempt is the own attempts before plus the cluster's attempt; the
	// last attempt is ownAttempt == maxAttempts.
	if task.OwnAttempt != 3 || task.MaxAttempts != 3 || task.ScheduledAt != "2026-11-11T05:00:00.611Z" ||
		task.AttemptStartedAt != "2026-11-11T05:00:03.02Z" {
		t.Fatalf("task = %+v", task)
	}
}

func TestRunContextTaskJSON_ChildSeesItsParentAndRoot(t *testing.T) {
	tree := testRunContextTree()
	run, _, f := runContextTaskJSON(testRunContextInput(2, "ork-mid", "ork-root"), &tree, false, 1, time.Now())
	if f != nil {
		t.Fatal(f)
	}
	if run.Origin == nil || *run.Origin != (taskJSONRunOrigin{Kind: "PARENT_RUN", ID: "ork-mid"}) {
		t.Fatalf("origin = %+v", run.Origin)
	}
	if run.Parent == nil || run.Parent.RunID != "ork-mid" || run.Parent.TaskID != "leaf" || run.Parent.Orkflow.Name != "pub" {
		t.Fatalf("parent = %+v", run.Parent)
	}
	// Inside a shared Orkflow, at any depth, the root's origin has no
	// principal and no reference.
	if run.Root == nil || run.Root.RunID != "ork-root" || run.Root.Origin.Principal != "" || run.Root.Origin.Reference != "" ||
		run.Root.Origin.ID != "trg-1" {
		t.Fatalf("root = %+v", run.Root)
	}
}

func TestRunParent_NeverFromTemporalParent(t *testing.T) {
	// A top-level run is never anyone's child, whatever started it: a
	// scheduled run's starter is the fire, which is no parent run.
	tree := testRunContextTree()
	tree.Root.Origin.Kind = "RUN_ORIGIN_KIND_SCHEDULE"
	run, _, f := runContextTaskJSON(testRunContextInput(0, "", ""), &tree, false, 1, time.Now())
	if f != nil {
		t.Fatal(f)
	}
	if run.Parent != nil || run.Origin.Kind != "SCHEDULE" {
		t.Fatalf("a top-level scheduled run: origin %+v parent %+v", run.Origin, run.Parent)
	}
}

// runContextHarness is an Orker with a run-context store holding tree's
// object under ork-export-extract.
func runContextHarness(t *testing.T, tree artifacts.RunContextObject) (*eachHarness, string, string) {
	t.Helper()
	h := newEachHarness(t)
	dir := t.TempDir()
	store, err := artifacts.NewLocalRunContextStore(dir)
	must.OK(t, err)
	raw, sha, err := artifacts.EncodeRunContextObject(tree)
	must.OK(t, err)
	must.OK(t, store.WriteRunContext(context.Background(), "default", "ork-export-extract", raw))
	h.dispatcher.RunContexts = store
	return h, sha, dir
}

func TestExecute_TaskJSONCarriesTheRunContext(t *testing.T) {
	h, sha, _ := runContextHarness(t, testRunContextTree())
	in := testRunContextInput(0, "", "")
	in.RunContext.ObjectSha256 = sha
	if _, err := h.dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	doc := h.runtime.task[0]
	run := doc["run"].(map[string]any)
	if run["context"] != RunContextAvailable || run["createdAt"] != "2026-11-11T05:00:00.412Z" {
		t.Fatalf("run = %v", run)
	}
	origin, _ := run["origin"].(map[string]any)
	if origin["kind"] != "TRIGGER" || origin["principal"] != "ci-bot" {
		t.Fatalf("origin = %v", origin)
	}
	task := doc["task"].(map[string]any)
	// Outside an activity the cluster's attempt is 1: ownAttempt is the one
	// own attempt before plus it.
	if task["attempt"] != float64(1) || task["ownAttempt"] != float64(2) || task["maxAttempts"] != float64(3) ||
		task["scheduledAt"] != "2026-11-11T05:00:00.611Z" || task["attemptStartedAt"] == nil {
		t.Fatalf("task = %v", task)
	}
}

func TestExecute_RunContextObjectReasons(t *testing.T) {
	tree := testRunContextTree()
	cases := []struct {
		name      string
		mutate    func(t *testing.T, in *orkstackv1.ExecuteTaskInput, sha, dir string)
		reason    string
		retryable bool
	}{
		{"missing", func(_ *testing.T, in *orkstackv1.ExecuteTaskInput, _, _ string) {
			in.RunContext.ObjectSha256 = strings.Repeat("1", 64)
		}, ReasonRunContextMissing, true},
		{"other bytes at the name", func(t *testing.T, in *orkstackv1.ExecuteTaskInput, sha, dir string) {
			in.RunContext.ObjectSha256 = sha
			key, err := artifacts.RunContextKey("default", "ork-export-extract", sha)
			must.OK(t, err)
			must.OK(t, os.WriteFile(filepath.Join(dir, filepath.FromSlash(key)), []byte(`{"format":1}`), 0o600))
		}, ReasonRunContextInvalid, false},
		{"an entry the tree does not hold", func(_ *testing.T, in *orkstackv1.ExecuteTaskInput, sha, _ string) {
			in.RunContext.ObjectSha256 = sha
			in.RunContext.TreeIndex = 9
		}, ReasonRunContextInvalid, false},
		{"no store", func(_ *testing.T, in *orkstackv1.ExecuteTaskInput, sha, _ string) {
			in.RunContext.ObjectSha256 = sha
		}, ReasonRunContextUnavailable, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, sha, dir := runContextHarness(t, tree)
			if tc.reason == ReasonRunContextUnavailable {
				h.dispatcher.RunContexts = nil
			}
			in := testRunContextInput(0, "", "")
			tc.mutate(t, in, sha, dir)
			_, err := h.dispatcher.Execute(context.Background(), in)
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("Execute = %v, want %s", err, tc.reason)
			}
			if RetryableReason(tc.reason) != tc.retryable {
				t.Fatalf("%s retryable = %v, want %v", tc.reason, RetryableReason(tc.reason), tc.retryable)
			}
			if len(h.runtime.task) != 0 {
				t.Fatal("a container was created without its run context")
			}
		})
	}
}

func TestTaskJSON_CachedTaskOmitsDataBearingKeys(t *testing.T) {
	ws := newTestWorkspace(t)
	in := testRunContextInput(0, "", "")
	tree := testRunContextTree()
	run, task, f := runContextTaskJSON(in, &tree, true, 1, time.Now())
	if f != nil {
		t.Fatal(f)
	}
	must.OK(t, ws.WriteTaskJSON(in, TaskContext{RunContext: run, TaskContext: task, Attempt: 1, Pool: "main",
		Namespace: "default", Network: NetworkNone, Image: in.GetImage()}))
	raw, err := os.ReadFile(filepath.Join(ws.Input, "task.json"))
	must.OK(t, err)
	for _, key := range []string{"createdAt", "startedAt", "deadline", "origin", "parent", "root", "scheduledAt", "attemptStartedAt"} {
		if strings.Contains(string(raw), `"`+key+`"`) {
			t.Errorf("a cached task's task.json carries %s:\n%s", key, raw)
		}
	}
	for _, want := range []string{`"context": "cached"`, `"ownAttempt": 2`, `"maxAttempts": 3`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("a cached task's task.json lacks %s:\n%s", want, raw)
		}
	}
}

// Whatever the run context cannot account for whole fails closed: the task
// is refused RUN_CONTEXT_INVALID, never shown a guessed key, and a run inside
// a shared Orkflow never the root's principal or reference.
func TestRunContextTaskJSON_FailsClosed(t *testing.T) {
	now := time.Now()
	cases := map[string]func(*orkstackv1.ExecuteTaskInput, *artifacts.RunContextObject) (*artifacts.RunContextObject, bool){
		"a shared run with no shared origin": func(in *orkstackv1.ExecuteTaskInput, o *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			o.Root.OriginShared = nil
			return o, false
		},
		"an uncached task with no object": func(*orkstackv1.ExecuteTaskInput, *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			return nil, false
		},
		"no created time": func(in *orkstackv1.ExecuteTaskInput, o *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			in.RunContext.CreatedAt = nil
			return o, false
		},
		"no scheduled time": func(in *orkstackv1.ExecuteTaskInput, o *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			in.RunContext.TaskScheduledAt = nil
			return o, false
		},
		"a child naming no root": func(in *orkstackv1.ExecuteTaskInput, o *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			in.RunContext.RootRunId = ""
			return o, false
		},
		"a child naming no parent": func(in *orkstackv1.ExecuteTaskInput, o *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			in.RunContext.ParentRunId = ""
			return o, false
		},
		"an entry past the tree": func(in *orkstackv1.ExecuteTaskInput, o *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			in.RunContext.TreeIndex = 7
			return o, false
		},
		"an entry with no parent": func(in *orkstackv1.ExecuteTaskInput, o *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			o.Entries[1].Parent = nil
			return o, false
		},
		"no declared attempts": func(in *orkstackv1.ExecuteTaskInput, o *artifacts.RunContextObject) (*artifacts.RunContextObject, bool) {
			in.RunContext.DeclaredMaxAttempts = 0
			return o, true
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			tree := testRunContextTree()
			in := testRunContextInput(2, "ork-mid", "ork-root")
			obj, cached := mutate(in, &tree)
			run, task, f := runContextTaskJSON(in, obj, cached, 1, now)
			if f == nil || f.Reason != ReasonRunContextInvalid {
				t.Fatalf("failure = %v, want %s (run %+v task %+v)", f, ReasonRunContextInvalid, run, task)
			}
			if run.Root != nil || run.Origin != nil {
				t.Fatalf("a refused context still names an origin: %+v", run)
			}
		})
	}
}

func TestWriteTaskJSON_RefusesALostMarker(t *testing.T) {
	ws := newTestWorkspace(t)
	in := testRunContextInput(0, "", "")
	if err := ws.WriteTaskJSON(in, TaskContext{Attempt: 1, Pool: "main", Namespace: "default", Network: NetworkNone, Image: in.GetImage()}); err == nil {
		t.Fatal("task.json was written as unavailable for an input that carries a run context")
	}
}

// refusingRunContexts is a store whose credential may not read the object,
// or one that will not say whether it is there.
type refusingRunContexts struct{}

func (refusingRunContexts) ReadRunContextBytes(context.Context, string, string, string) ([]byte, error) {
	return nil, artifacts.ErrRunContextRefused
}

// A get-only credential cannot tell a missing object from a forbidden one, so
// the failure says it may be either and names the grant, and is never
// RUN_CONTEXT_MISSING.
func TestExecute_ARefusedRunContextNamesBothCauses(t *testing.T) {
	h, sha, _ := runContextHarness(t, testRunContextTree())
	h.dispatcher.RunContexts = refusingRunContexts{}
	in := testRunContextInput(0, "", "")
	in.RunContext.ObjectSha256 = sha
	_, err := h.dispatcher.Execute(context.Background(), in)
	if err == nil || strings.Contains(err.Error(), ReasonRunContextMissing) {
		t.Fatalf("Execute = %v", err)
	}
	for _, want := range []string{ReasonRunContextUnavailable, "missing", "may not read", artifacts.RunContextPrefix + "/"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Execute = %v, want it to say %q", err, want)
		}
	}
}
