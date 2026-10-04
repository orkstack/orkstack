package orker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// eachRuntime records, at Create, the spec and every file of the sealed input
// tree a fan-out test asserts on — task.json, config.json, and the staged
// upstream directory listing — then behaves like a workload that wrote {} and
// printed one line.
type eachRuntime struct {
	mu      sync.Mutex
	specs   []ContainerSpec
	task    []map[string]any
	taskRaw [][]byte
	config  []map[string]any
	staged  [][]string
}

func (r *eachRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{Platform: "linux/amd64", Delivery: DeliveryRegistry}, nil
}
func (r *eachRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no layout")
}

func (r *eachRuntime) Create(_ context.Context, spec ContainerSpec) (ContainerID, error) {
	var doc, cfg map[string]any
	var raw []byte
	var staged []string
	for _, m := range spec.Mounts {
		switch m.Destination {
		case ContainerInput:
			var err error
			raw, err = os.ReadFile(filepath.Join(m.Source, "task.json"))
			if err != nil {
				return "", err
			}
			_ = json.Unmarshal(raw, &doc)
			c, err := os.ReadFile(filepath.Join(m.Source, "config.json"))
			if err != nil {
				return "", err
			}
			_ = json.Unmarshal(c, &cfg)
			_ = filepath.Walk(m.Source, func(path string, info os.FileInfo, err error) error {
				if err == nil {
					rel, _ := filepath.Rel(m.Source, path)
					if info.IsDir() {
						rel += "/"
					}
					staged = append(staged, filepath.ToSlash(rel))
				}
				return nil
			})
		case ContainerOutput:
			if err := os.WriteFile(filepath.Join(m.Source, "values.json"), []byte(`{}`), 0o644); err != nil {
				return "", err
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.specs = append(r.specs, spec)
	r.task = append(r.task, doc)
	r.taskRaw = append(r.taskRaw, raw)
	r.config = append(r.config, cfg)
	r.staged = append(r.staged, staged)
	return "fake-each", nil
}

func (r *eachRuntime) Start(_ context.Context, _ ContainerID, stdout, _ io.Writer) error {
	_, err := io.WriteString(stdout, "item ran\n")
	return err
}
func (r *eachRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }
func (r *eachRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{Code: 0}, nil
}
func (r *eachRuntime) Remove(context.Context, ContainerID) error { return nil }
func (r *eachRuntime) Reap(context.Context, string) (int, error) { return 0, nil }
func (r *eachRuntime) Describe(context.Context) (RuntimeInfo, error) {
	return RuntimeInfo{Name: "each-recorder"}, nil
}

type eachHarness struct {
	dispatcher *OCIDispatcher
	runtime    *eachRuntime
	store      *artifacts.LocalStore
	commits    *artifacts.MemoryCommitStore
	attempts   *artifacts.MemoryAttemptStore
	logs       *recordingLogStore
	workRoot   string
}

func newEachHarness(t *testing.T) *eachHarness {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("local store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	h := &eachHarness{
		runtime:  &eachRuntime{},
		store:    store,
		commits:  artifacts.NewMemoryCommitStore(),
		attempts: artifacts.NewMemoryAttemptStore(),
		logs:     &recordingLogStore{},
		workRoot: t.TempDir(),
	}
	h.dispatcher = &OCIDispatcher{
		Runtime:  h.runtime,
		Store:    store,
		Refs:     artifacts.MemoryRefPredictor{},
		Policy:   DefaultSecurityPolicy(),
		WorkRoot: h.workRoot,
		OrkerID:  "orker-each-test",
		BuildID:  "test",
		Commits:  h.commits,
		Attempts: h.attempts,
		Logs:     h.logs,
	}
	return h
}

func (h *eachHarness) put(t *testing.T, name string, data []byte) *orkstackv1.ArtifactRef {
	t.Helper()
	ref, err := h.store.Put(context.Background(), name, bytes.NewReader(data), artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("put %s: %v", name, err)
	}
	return refPtr(ref)
}

const eachRunID = "ork-each-1"

// itemInput is one item of `convert`, over the `list` task's parts.
func itemInput(index, count int32, source *orkstackv1.EachSource) *orkstackv1.ExecuteTaskInput {
	in := baseExportInput("convert")
	in.RunId = eachRunID
	in.IdempotencyKey = task.ItemIdempotencyKeyFor("default", eachRunID, "convert", index)
	in.Each = &orkstackv1.EachContext{Index: index, Count: count, As: "part", Source: source}
	return in
}

// The element's EXACT bytes reach task.json for a task source — key order, a
// 20-digit integer — and the item's own idempotency key; a task that is not an
// item carries no `each` key at all.
func TestExecute_ItemWritesTaskJSONEach(t *testing.T) {
	h := newEachHarness(t)
	values := []byte(`{"parts":[{"z":1,"uri":"s3://a"},{"uri":"s3://b","rows":12345678901234567890,"a":true}]}`)
	in := itemInput(1, 2, &orkstackv1.EachSource{TaskId: "list", Path: []string{"parts"}})
	in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "list", Values: h.put(t, "values.json", values)}}
	with, _ := structpb.NewStruct(map[string]any{"rows": "{{ .each.part.rows }}", "uri": "{{ index .each \"part\" \"uri\" }}"})
	in.With = with
	if _, err := h.dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	doc := h.runtime.task[0]
	each, ok := doc["each"].(map[string]any)
	if !ok {
		t.Fatalf("task.json has no each: %v", doc)
	}
	if each["index"] != float64(1) || each["count"] != float64(2) || each["as"] != "part" {
		t.Fatalf("each = %v", each)
	}
	if !bytes.Contains(h.runtime.taskRaw[0], []byte(`"rows": 12345678901234567890`)) {
		t.Fatalf("the 20-digit integer did not survive:\n%s", h.runtime.taskRaw[0])
	}
	if !bytes.Contains(h.runtime.taskRaw[0], []byte("\"item\": {\n      \"uri\": \"s3://b\",\n      \"rows\": 12345678901234567890,\n      \"a\": true")) {
		t.Fatalf("the element's key order did not survive:\n%s", h.runtime.taskRaw[0])
	}
	if doc["idempotencyKey"] != "default/ork-each-1/convert/1" {
		t.Fatalf("idempotencyKey = %v", doc["idempotencyKey"])
	}
	if task := doc["task"].(map[string]any); task["id"] != "convert" {
		t.Fatalf("task.id = %v, want the node's", task["id"])
	}
	if cfg := h.runtime.config[0]; cfg["rows"] != "12345678901234567890" || cfg["uri"] != "s3://b" {
		t.Fatalf("config.json = %v", cfg)
	}

	// Not an item: no each key.
	plain := newEachHarness(t)
	if _, err := plain.dispatcher.Execute(context.Background(), baseExportInput("plain")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, present := plain.runtime.task[0]["each"]; present {
		t.Fatal("a task that is not an item carries task.json each")
	}
	if _, present := plain.runtime.specs[0].Labels["ork.item"]; present {
		t.Fatal("a task that is not an item carries the ork.item label")
	}
}

// An input source: the run input's value, rendered with protojson.
func TestExecute_ItemOfAnInputSource(t *testing.T) {
	h := newEachHarness(t)
	in := itemInput(0, 3, &orkstackv1.EachSource{Input: "regions"})
	data, _ := structpb.NewStruct(map[string]any{"regions": []any{"eu", "us", "ap"}})
	in.RunInput = &orkstackv1.Payload{Data: data}
	with, _ := structpb.NewStruct(map[string]any{"r": "{{ .each.part }} {{ .each.index }}/{{ .each.count }}"})
	in.With = with
	if _, err := h.dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := h.runtime.task[0]["each"].(map[string]any)["item"]; got != "eu" {
		t.Fatalf("each.item = %v, want eu", got)
	}
	if got := h.runtime.config[0]["r"]; got != "eu 0/3" {
		t.Fatalf("rendered = %v", got)
	}
}

func TestRender_EachRoot(t *testing.T) {
	each, err := eachRenderData(&orkstackv1.EachContext{Index: 4, Count: 9, As: "part"},
		json.RawMessage(`{"uri":"s3://x","big":12345678901234567890}`))
	if err != nil {
		t.Fatal(err)
	}
	r := newRenderer("convert", nil, nil, nil, nil, nil, each)
	got, err := r.render(context.Background(), map[string]any{
		"a": "{{ .each.part.uri }}",
		"b": `{{ index .each "part" "uri" }}`,
		"c": "{{ .each.index }}/{{ .each.count }}",
		"d": "{{ .each.part.big }}",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got["a"] != "s3://x" || got["b"] != "s3://x" || got["c"] != "4/9" || got["d"] != "12345678901234567890" {
		t.Fatalf("rendered = %v", got)
	}
	_, err = r.render(context.Background(), map[string]any{"a": "{{ .each.part.missing }}"})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("a missing key = %v, want an error naming it", err)
	}
	// A task that is not an item has no .each root at all.
	plain := newRenderer("load", nil, nil, nil, nil, nil, nil)
	if _, err := plain.render(context.Background(), map[string]any{"a": "{{ .each.index }}"}); err == nil {
		t.Fatal(".each rendered for a task that is not an item")
	}
}

// A missing key under .each.part fails the item's attempt RENDER_FAILED.
func TestExecute_ItemRenderMissingKeyIsRenderFailed(t *testing.T) {
	h := newEachHarness(t)
	in := itemInput(0, 1, &orkstackv1.EachSource{Input: "parts"})
	data, _ := structpb.NewStruct(map[string]any{"parts": []any{map[string]any{"a": "1"}}})
	in.RunInput = &orkstackv1.Payload{Data: data}
	with, _ := structpb.NewStruct(map[string]any{"x": "{{ .each.part.uri }}"})
	in.With = with
	_, err := h.dispatcher.Execute(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), ReasonRenderFailed) {
		t.Fatalf("Execute = %v, want RENDER_FAILED", err)
	}
}

func TestExecute_ItemAttemptUsesItemKeysAndLabels(t *testing.T) {
	h := newEachHarness(t)
	in := itemInput(97, 120, &orkstackv1.EachSource{Input: "parts"})
	parts := make([]any, 120)
	for i := range parts {
		parts[i] = float64(i)
	}
	data, _ := structpb.NewStruct(map[string]any{"parts": parts})
	in.RunInput = &orkstackv1.Payload{Data: data}
	key, _ := artifacts.ItemCommitKey("default", eachRunID, "convert", 97)
	in.CommitUri = "mem://commits/" + key
	if _, err := h.dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	spec := h.runtime.specs[0]
	if spec.Labels["ork.item"] != "97" || spec.Labels["ork.task"] != "convert" {
		t.Fatalf("labels = %v", spec.Labels)
	}
	var workDir string
	for _, m := range spec.Mounts {
		if m.Destination == ContainerInput {
			workDir = filepath.Base(filepath.Dir(m.Source))
		}
	}
	if !strings.HasPrefix(workDir, "ork-ork-each-1-convert-i00097-1-") {
		t.Fatalf("work dir %q does not name the item", workDir)
	}
	chunks := h.logs.stored()
	if len(chunks) == 0 || chunks[0].Item != artifacts.ItemAt(97) {
		t.Fatalf("log chunks = %+v, want item 97's", chunks)
	}
	records, err := h.attempts.ListAttempts(context.Background(), eachRunID, "convert", artifacts.ItemAt(97))
	if err != nil || len(records) != 1 || records[0].GetItem() != 97 {
		t.Fatalf("item attempt records = %v (%v)", records, err)
	}
	if own, _ := h.attempts.ListAttempts(context.Background(), eachRunID, "convert", artifacts.NoItem); len(own) != 0 {
		t.Fatalf("the item's record leaked into the task's own scope: %v", own)
	}
	if _, err := h.commits.Get(context.Background(), in.CommitUri); err != nil {
		t.Fatalf("the item's commit was not written at its own uri: %v", err)
	}
}

func TestExecute_ItemSourceMismatchIsEachItemUnavailable(t *testing.T) {
	for name, values := range map[string]string{
		"a shorter array": `{"parts":[1,2]}`,
		"not an array":    `{"parts":{"a":1}}`,
		"a missing key":   `{"other":[1,2,3]}`,
	} {
		t.Run(name, func(t *testing.T) {
			h := newEachHarness(t)
			in := itemInput(2, 3, &orkstackv1.EachSource{TaskId: "list", Path: []string{"parts"}})
			in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "list", Values: h.put(t, "values.json", []byte(values))}}
			_, err := h.dispatcher.Execute(context.Background(), in)
			if err == nil || !strings.Contains(err.Error(), ReasonEachItemUnavailable) {
				t.Fatalf("Execute = %v, want EACH_ITEM_UNAVAILABLE", err)
			}
			if len(h.runtime.specs) != 0 {
				t.Fatal("a container was created for an unavailable item")
			}
		})
	}
	if CategoryFor(ReasonEachItemUnavailable) != CategoryContractViolation || RetryableReason(ReasonEachItemUnavailable) {
		t.Fatal("EACH_ITEM_UNAVAILABLE must be a non-retryable ContractViolation")
	}
}

// writeItemCommit stores one item's commit with a values object and one
// artifact, as the Orker that ran it would have.
func (h *eachHarness) writeItemCommit(t *testing.T, root string, index int32, mutate func(*orkstackv1.ActivityCommit)) {
	t.Helper()
	key, err := artifacts.ItemCommitKey("default", eachRunID, "convert", index)
	if err != nil {
		t.Fatal(err)
	}
	commit := &orkstackv1.ActivityCommit{
		SchemaVersion:           artifacts.CommitSchemaVersion,
		Namespace:               "default",
		RunId:                   eachRunID,
		TaskId:                  "convert",
		IdempotencyKey:          task.ItemIdempotencyKeyFor("default", eachRunID, "convert", index),
		OrkflowDefinitionDigest: "sha256:def",
		Values:                  h.put(t, "values.json", []byte(fmt.Sprintf(`{"part":%d}`, index))),
		Artifacts:               []*orkstackv1.ArtifactRef{h.put(t, "part.txt", []byte(fmt.Sprintf("part %d\n", index)))},
	}
	if mutate != nil {
		mutate(commit)
	}
	if _, err := h.commits.Create(context.Background(), root+"/"+key, commit); err != nil {
		t.Fatalf("create item %d's commit: %v", index, err)
	}
}

func mergeInput(count int32, root string) *orkstackv1.ExecuteTaskInput {
	in := baseExportInput("merge")
	in.RunId = eachRunID
	in.IdempotencyKey = "default/" + eachRunID + "/merge"
	in.OrkflowDefinitionDigest = "sha256:def"
	in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "convert", Each: &orkstackv1.EachUpstream{Count: count, CommitRoot: root + "/"}}}
	return in
}

func TestStageInputs_EachUpstreamStagesEveryItemFromItsCommit(t *testing.T) {
	h := newEachHarness(t)
	const root = "mem://commits"
	for i := int32(0); i < 3; i++ {
		h.writeItemCommit(t, root, i, nil)
	}
	if _, err := h.dispatcher.Execute(context.Background(), mergeInput(3, root)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	staged := strings.Join(h.runtime.staged[0], "\n")
	for _, want := range []string{
		"upstream/convert/00000/values.json", "upstream/convert/00002/values.json",
		"artifacts/convert/00001/part.txt",
	} {
		if !strings.Contains(staged, want) {
			t.Fatalf("staged files lack %s:\n%s", want, staged)
		}
	}
	inputs := h.runtime.task[0]["inputs"].(map[string]any)
	eachUp := inputs["eachUpstream"].([]any)
	if len(eachUp) != 1 || eachUp[0].(map[string]any)["taskId"] != "convert" || eachUp[0].(map[string]any)["count"] != float64(3) {
		t.Fatalf("inputs.eachUpstream = %v", eachUp)
	}
	upstream := inputs["upstream"].([]any)
	if len(upstream) != 3 || upstream[1].(map[string]any)["item"] != float64(1) ||
		upstream[1].(map[string]any)["valuesPath"] != "/orkstack/input/upstream/convert/00001/values.json" {
		t.Fatalf("inputs.upstream = %v", upstream)
	}
	arts := inputs["artifacts"].([]any)
	if len(arts) != 3 || arts[2].(map[string]any)["item"] != float64(2) {
		t.Fatalf("inputs.artifacts = %v", arts)
	}
}

func TestStageInputs_EachUpstreamRefusesAnotherActivitysCommit(t *testing.T) {
	h := newEachHarness(t)
	const root = "mem://commits"
	h.writeItemCommit(t, root, 0, func(c *orkstackv1.ActivityCommit) {
		c.IdempotencyKey = task.ItemIdempotencyKeyFor("default", eachRunID, "convert", 5)
	})
	_, err := h.dispatcher.Execute(context.Background(), mergeInput(1, root))
	if err == nil || !strings.Contains(err.Error(), ReasonCommitIdentity) {
		t.Fatalf("Execute = %v, want COMMIT_IDENTITY_MISMATCH", err)
	}
}

func TestStageInputs_EachUpstreamMissingCommitIsInputUnavailable(t *testing.T) {
	h := newEachHarness(t)
	const root = "mem://commits"
	h.writeItemCommit(t, root, 0, nil)
	_, err := h.dispatcher.Execute(context.Background(), mergeInput(2, root))
	if err == nil || !strings.Contains(err.Error(), ReasonInputUnavailable) || !strings.Contains(err.Error(), "item 1") {
		t.Fatalf("Execute = %v, want INPUT_ARTIFACT_UNAVAILABLE naming item 1", err)
	}
}

func TestStageInputs_EachUpstreamNoCommitStoreIsInputUnavailable(t *testing.T) {
	h := newEachHarness(t)
	h.dispatcher.Commits = nil
	_, err := h.dispatcher.Execute(context.Background(), mergeInput(2, "mem://commits"))
	if err == nil || !strings.Contains(err.Error(), "no commit store") {
		t.Fatalf("Execute = %v, want the no-commit-store refusal", err)
	}
}

func TestStageInputs_EachUpstreamZeroItemsCreatesTheDirectories(t *testing.T) {
	h := newEachHarness(t)
	if _, err := h.dispatcher.Execute(context.Background(), mergeInput(0, "mem://commits")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	staged := strings.Join(h.runtime.staged[0], "\n")
	for _, want := range []string{"upstream/convert/", "artifacts/convert/"} {
		if !strings.Contains(staged, want) {
			t.Fatalf("a fan-out of 0 items left no %s directory:\n%s", want, staged)
		}
	}
	inputs := h.runtime.task[0]["inputs"].(map[string]any)
	eachUp := inputs["eachUpstream"].([]any)
	if len(eachUp) != 1 || eachUp[0].(map[string]any)["count"] != float64(0) {
		t.Fatalf("inputs.eachUpstream = %v, want convert with 0 items", eachUp)
	}
}

func TestDescribeOrker_ReportsEachSupported(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	activities := NewActivitiesWithOptions(store, Options{Runtime: &reapRecorder{}, WorkRoot: t.TempDir()})
	result, err := activities.DescribeOrker(context.Background())
	if err != nil {
		t.Fatalf("DescribeOrker: %v", err)
	}
	if !result.GetEachSupported() {
		t.Fatal("this build implements fan-out items and must say so")
	}
}
