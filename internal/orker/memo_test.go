package orker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ---------------------------------------------------------------- fakes

// cacheRuntime is a workload that writes one values.json and one artifact,
// counting every pull and create so a hit can be proven to have started no
// container.
type cacheRuntime struct {
	mu       sync.Mutex
	pulls    int
	creates  int
	exit     int32
	values   string
	artifact string
	configs  [][]byte
	secrets  [][]string
}

func (r *cacheRuntime) Pull(context.Context, string) (PullInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pulls++
	return PullInfo{PlatformDigest: "sha256:" + strings.Repeat("b", 64), Platform: "linux/amd64", Delivery: DeliveryRegistry}, nil
}

func (r *cacheRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no layout")
}

func (r *cacheRuntime) Create(_ context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.creates++
	for _, m := range spec.Mounts {
		switch m.Destination {
		case ContainerInput:
			config, err := os.ReadFile(filepath.Join(m.Source, "config.json"))
			if err != nil {
				return "", err
			}
			r.configs = append(r.configs, config)
		case ContainerSecrets:
			entries, _ := os.ReadDir(m.Source)
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			r.secrets = append(r.secrets, names)
		case ContainerOutput:
			values := r.values
			if values == "" {
				values = `{"rows":3}`
			}
			if err := os.WriteFile(filepath.Join(m.Source, "values.json"), []byte(values), 0o644); err != nil {
				return "", err
			}
			content := r.artifact
			if content == "" {
				content = "1\n2\n3\n"
			}
			if err := os.WriteFile(filepath.Join(m.Source, "artifacts", "features.txt"), []byte(content), 0o644); err != nil {
				return "", err
			}
		}
	}
	return "fake-cache", nil
}

func (r *cacheRuntime) Start(_ context.Context, _ ContainerID, stdout, _ io.Writer) error {
	_, err := io.WriteString(stdout, "featurized 3 rows\n")
	return err
}
func (r *cacheRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }
func (r *cacheRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ExitStatus{Code: r.exit}, nil
}
func (r *cacheRuntime) Remove(context.Context, ContainerID) error { return nil }
func (r *cacheRuntime) Reap(context.Context, string) (int, error) { return 0, nil }
func (r *cacheRuntime) Describe(context.Context) (RuntimeInfo, error) {
	return RuntimeInfo{Name: "cache"}, nil
}

func (r *cacheRuntime) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pulls, r.creates
}

// blockingStatStore blocks every Stat until its context ends while armed, and
// disarms itself the first time one does, so only the memo lookup's checks
// time out.
type blockingStatStore struct {
	artifacts.Store
	armed atomic.Bool
}

func (s *blockingStatStore) Stat(ctx context.Context, uri string) (artifacts.ArtifactRef, error) {
	if s.armed.Load() {
		<-ctx.Done()
		s.armed.Store(false)
		return artifacts.ArtifactRef{}, ctx.Err()
	}
	return s.Store.Stat(ctx, uri)
}

// memoEventLog records the order of commit creates and memo puts.
type memoEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *memoEventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *memoEventLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// scriptedMemoStore wraps a MemoryMemoStore so a test can fail or block any
// call, and count the calls.
type scriptedMemoStore struct {
	*artifacts.MemoryMemoStore
	log      *memoEventLog
	getErr   error
	putErr   error
	blockGet bool
	blockPut bool
	gets     int
	puts     int
	mu       sync.Mutex
}

func (s *scriptedMemoStore) GetMemo(ctx context.Context, ref artifacts.MemoRef) (*orkstackv1.CacheMemo, error) {
	s.mu.Lock()
	s.gets++
	s.mu.Unlock()
	if s.blockGet {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.MemoryMemoStore.GetMemo(ctx, ref)
}

func (s *scriptedMemoStore) PutMemo(ctx context.Context, memo *orkstackv1.CacheMemo) error {
	s.mu.Lock()
	s.puts++
	s.mu.Unlock()
	if s.log != nil {
		s.log.add("put-memo")
	}
	if s.blockPut {
		<-ctx.Done()
		return ctx.Err()
	}
	if s.putErr != nil {
		return s.putErr
	}
	return s.MemoryMemoStore.PutMemo(ctx, memo)
}

func (s *scriptedMemoStore) calls() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets, s.puts
}

// scriptedCommitStore wraps a MemoryCommitStore: it records creates, can fail
// them, and can hide a planted winner from the retry-start check so a create
// meets it as a conflict.
type scriptedCommitStore struct {
	*artifacts.MemoryCommitStore
	log       *memoEventLog
	createErr error
	// hidden is a commit planted at its uri the moment Create is first
	// called, so Create conflicts and the following Get finds the winner.
	hidden    map[string]*orkstackv1.ActivityCommit
	createdAt []string
}

func (s *scriptedCommitStore) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
	if s.log != nil {
		s.log.add("create-commit")
	}
	s.createdAt = append(s.createdAt, uri)
	if winner, ok := s.hidden[uri]; ok {
		delete(s.hidden, uri)
		if _, err := s.MemoryCommitStore.Create(ctx, uri, winner); err != nil {
			return artifacts.CommitRecord{}, err
		}
	}
	if s.createErr != nil {
		return artifacts.CommitRecord{}, s.createErr
	}
	return s.MemoryCommitStore.Create(ctx, uri, c)
}

// ---------------------------------------------------------------- harness

const cacheRoot = "mem://commits"

type cacheHarness struct {
	dispatcher *OCIDispatcher
	runtime    *cacheRuntime
	store      *artifacts.LocalStore
	commits    *scriptedCommitStore
	memos      *scriptedMemoStore
	attempts   *artifacts.MemoryAttemptStore
	logs       *recordingLogStore
	events     *memoEventLog
	now        time.Time
	lines      []string
	linesMu    sync.Mutex
}

func newCacheHarness(t *testing.T) *cacheHarness {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("local store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	events := &memoEventLog{}
	h := &cacheHarness{
		runtime:  &cacheRuntime{},
		store:    store,
		commits:  &scriptedCommitStore{MemoryCommitStore: artifacts.NewMemoryCommitStore(), log: events, hidden: map[string]*orkstackv1.ActivityCommit{}},
		memos:    &scriptedMemoStore{MemoryMemoStore: artifacts.NewMemoryMemoStore(), log: events},
		attempts: artifacts.NewMemoryAttemptStore(),
		logs:     &recordingLogStore{},
		events:   events,
		// The Orker's own clock stamps a fresh commit's finished_at, so the
		// injected memo clock starts at the real time and moves from there.
		now: time.Now().UTC(),
	}
	h.dispatcher = &OCIDispatcher{
		Runtime:  h.runtime,
		Store:    store,
		Refs:     artifacts.MemoryRefPredictor{},
		Policy:   DefaultSecurityPolicy(),
		WorkRoot: t.TempDir(),
		OrkerID:  "orker-cache-test",
		BuildID:  "build-cache",
		Commits:  h.commits,
		Memos:    h.memos,
		Attempts: h.attempts,
		Logs:     h.logs,
		now:      func() time.Time { return h.now },
		LogSinkObserver: func(_ *orkstackv1.ExecuteTaskInput, line string) {
			h.linesMu.Lock()
			defer h.linesMu.Unlock()
			h.lines = append(h.lines, line)
		},
	}
	return h
}

func slogTo(w io.Writer) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }

func (h *cacheHarness) clearLines() {
	h.linesMu.Lock()
	defer h.linesMu.Unlock()
	h.lines = nil
}

func (h *cacheHarness) loggedLines() []string {
	h.linesMu.Lock()
	defer h.linesMu.Unlock()
	return append([]string(nil), h.lines...)
}

func (h *cacheHarness) hasLine(prefix string) bool {
	for _, line := range h.loggedLines() {
		if strings.Contains(line, prefix) {
			return true
		}
	}
	return false
}

func (h *cacheHarness) put(t *testing.T, name string, data []byte) *orkstackv1.ArtifactRef {
	t.Helper()
	ref, err := h.store.Put(context.Background(), name, bytes.NewReader(data), artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("put %s: %v", name, err)
	}
	return refPtr(ref)
}

// cachedTaskInput is one attempt of the cached task `featurize` of runID.
func cachedTaskInput(t *testing.T, runID string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	in := baseExportInput("featurize")
	in.RunId = runID
	in.OrkflowName = "cached-pipeline"
	in.OrkflowDefinitionDigest = "sha256:def-" + runID
	in.IdempotencyKey = "default/" + runID + "/featurize"
	key, err := artifacts.CommitKey("default", runID, "featurize")
	if err != nil {
		t.Fatal(err)
	}
	in.CommitUri = cacheRoot + "/" + key
	with, _ := structpb.NewStruct(map[string]any{"rows": "{{ .inputs.rows }}"})
	in.With = with
	input, _ := structpb.NewStruct(map[string]any{"rows": 3})
	in.RunInput = &orkstackv1.Payload{Data: input}
	in.Cache = &orkstackv1.CacheContext{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, TtlSeconds: 86400}
	return in
}

// produce runs one fresh attempt of in, which must miss and write its memo.
func (h *cacheHarness) produce(t *testing.T, in *orkstackv1.ExecuteTaskInput) *orkstackv1.ExecuteTaskResult {
	t.Helper()
	result, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if !result.GetCache().GetMemoWritten() {
		t.Fatalf("produce: no memo written: %v", result.GetCache())
	}
	return result
}

// memoFor reads the memo the producing run wrote for in's key.
func (h *cacheHarness) memoFor(t *testing.T, result *orkstackv1.ExecuteTaskResult, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.CacheMemo, artifacts.MemoRef) {
	t.Helper()
	ref := memoRefFor(in, memoKey{Key: result.GetCache().GetKey()})
	memo, err := h.memos.MemoryMemoStore.GetMemo(context.Background(), ref)
	if err != nil {
		t.Fatalf("read the memo: %v", err)
	}
	return memo, ref
}

func lastAttempt(t *testing.T, store *artifacts.MemoryAttemptStore, runID, taskID string) *orkstackv1.TaskAttempt {
	t.Helper()
	records, err := store.ListAttempts(context.Background(), runID, taskID, artifacts.NoItem)
	if err != nil || len(records) == 0 {
		t.Fatalf("attempt records of %s/%s: %v, %v", runID, taskID, records, err)
	}
	return records[len(records)-1]
}

// ---------------------------------------------------------------- the key

func keyOf(t *testing.T, in *orkstackv1.ExecuteTaskInput, config, run []byte, staged *stagedInputs, item *resolvedEachItem) memoKey {
	t.Helper()
	key, ok := computeMemoKey(in, config, run, staged, item)
	if !ok {
		t.Fatal("computeMemoKey: not keyable")
	}
	return key
}

func pinnedKeyInput() (*orkstackv1.ExecuteTaskInput, *stagedInputs) {
	in := &orkstackv1.ExecuteTaskInput{
		RunId:          "ork-1",
		OrkflowName:    "cached-pipeline",
		Namespace:      "ml",
		TaskId:         "featurize",
		IdempotencyKey: "ml/ork-1/featurize",
		Image:          &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("a", 64)},
		Command:        []string{"sh", "-c", "true"},
		Env:            map[string]string{"B": "2", "A": "1"},
		Secrets:        []*orkstackv1.SecretRef{{Name: "db", Source: "env:DB"}},
		Network:        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		Cache:          &orkstackv1.CacheContext{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, TtlSeconds: 60},
	}
	staged := &stagedInputs{
		inputFiles: []memoInputFile{
			{Path: "/orkstack/input/upstream/fetch/values.json", NameB64: EncodeArtifactName("values.json"), SHA256: strings.Repeat("1", 64)},
			{Path: "/orkstack/input/artifacts/fetch/data.csv", NameB64: EncodeArtifactName("data.csv"), SHA256: strings.Repeat("2", 64)},
		},
	}
	return in, staged
}

// A change to either literal is a visible diff, and a reason to bump
// MemoKeyVersion.
func TestMemoKey_PinnedDocumentAndKey(t *testing.T) {
	in, staged := pinnedKeyInput()
	key := keyOf(t, in, []byte("{\n  \"rows\": 3\n}\n"), []byte("{}\n"), staged, nil)
	// Every digest recomputed from its definition, independently of
	// computeMemoKey, so this test pins the DEFINITION.
	wantComponents := map[string]string{
		"image":             "sha256:" + strings.Repeat("a", 64),
		"command":           sha256Hex([]byte(`["sh","-c","true"]`)),
		"env":               sha256Hex([]byte(`{"A":"1","B":"2"}`)),
		"secrets":           sha256Hex([]byte(`[{"name":"db","source":"env:DB"}]`)),
		"network":           "none",
		"run_as_orker_user": "false",
		"config":            sha256Hex([]byte("{\n  \"rows\": 3\n}\n")),
		"run_input":         sha256Hex([]byte("{}\n")),
		"inputs": sha256Hex([]byte(`{"files":[` +
			`{"path":"/orkstack/input/artifacts/fetch/data.csv","name_b64":"ZGF0YS5jc3Y","sha256":"` + strings.Repeat("2", 64) + `"},` +
			`{"path":"/orkstack/input/upstream/fetch/values.json","name_b64":"dmFsdWVzLmpzb24","sha256":"` + strings.Repeat("1", 64) + `"}` +
			`],"each_upstream":[]}`)),
	}
	if len(key.Components) != len(wantComponents) {
		t.Fatalf("components = %v", key.Components)
	}
	for name, want := range wantComponents {
		if key.Components[name] != want {
			t.Fatalf("component %s = %q, want %q", name, key.Components[name], want)
		}
	}
	componentsJSON, _ := json.Marshal(wantComponents)
	document := `{"v":1,"namespace":"ml","scope":"orkflow","orkflow":"cached-pipeline","task":"featurize","components":` +
		string(componentsJSON) + `}`
	if want := sha256Hex([]byte(document)); key.Key != want {
		t.Fatalf("key = %s, want %s (document %s)", key.Key, want, document)
	}
	// The key itself, as a literal: a change here is a change to the key's
	// definition and needs MemoKeyVersion bumped.
	const pinnedKey = "87d54b516b81a6f77147f915724f769862d0a8e8805fdeb1b0a6e0d9c139370c"
	if key.Key != pinnedKey {
		t.Fatalf("key = %s, pinned %s — bump MemoKeyVersion if the key's definition changed", key.Key, pinnedKey)
	}
}

func TestMemoKey_EveryComponentChangesTheKey(t *testing.T) {
	baseConfig, baseRun := []byte("{}\n"), []byte("{}\n")
	item := &resolvedEachItem{Bytes: json.RawMessage(`{"name":"a"}`), As: "part", Index: 1, Count: 4}
	base := func() (*orkstackv1.ExecuteTaskInput, *stagedInputs, []byte, []byte, *resolvedEachItem) {
		in, staged := pinnedKeyInput()
		staged.eachUpstream = []taskJSONEachUpstream{{TaskID: "convert", Count: 0}}
		it := *item
		return in, staged, baseConfig, baseRun, &it
	}
	in, staged, config, run, it := base()
	baseKey := keyOf(t, in, config, run, staged, it).Key

	cases := map[string]func(in *orkstackv1.ExecuteTaskInput, staged *stagedInputs, config, run *[]byte, it *resolvedEachItem){
		"image digest": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.Image.IndexDigest = "sha256:" + strings.Repeat("c", 64)
		},
		"command": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.Command = []string{"sh"}
		},
		"env": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.Env["A"] = "x"
		},
		"secret name": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.Secrets[0].Name = "db2"
		},
		"secret source": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.Secrets[0].Source = "env:OTHER"
		},
		"network": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.Network = orkstackv1.NetworkMode_NETWORK_MODE_DEFAULT
		},
		"run_as_orker_user": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.RunAsOrkerUser = true
		},
		"a rendered with: value": func(_ *orkstackv1.ExecuteTaskInput, _ *stagedInputs, config, _ *[]byte, _ *resolvedEachItem) {
			*config = []byte("{\n  \"rows\": 4\n}\n")
		},
		"a run input": func(_ *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, run *[]byte, _ *resolvedEachItem) {
			*run = []byte("{\n  \"rows\": 4\n}\n")
		},
		"an upstream values digest": func(_ *orkstackv1.ExecuteTaskInput, staged *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			staged.inputFiles[0].SHA256 = strings.Repeat("9", 64)
		},
		"an artifact digest": func(_ *orkstackv1.ExecuteTaskInput, staged *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			staged.inputFiles[1].SHA256 = strings.Repeat("8", 64)
		},
		"an artifact name": func(_ *orkstackv1.ExecuteTaskInput, staged *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			staged.inputFiles[1].NameB64 = EncodeArtifactName("other.csv")
		},
		"a zero-item fan-in count": func(_ *orkstackv1.ExecuteTaskInput, staged *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			staged.eachUpstream[0].Count = 1
		},
		"the item value": func(_ *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, it *resolvedEachItem) {
			it.Bytes = json.RawMessage(`{"name":"b"}`)
		},
		"as": func(_ *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, it *resolvedEachItem) {
			it.As = "item"
		},
		"index": func(_ *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, it *resolvedEachItem) {
			it.Index = 2
		},
		"count": func(_ *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, it *resolvedEachItem) {
			it.Count = 5
		},
		"the orkflow name under orkflow scope": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.OrkflowName = "other"
		},
		"the namespace": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.Namespace = "other"
		},
		"the task id": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.TaskId = "other"
		},
		"the scope": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs, _, _ *[]byte, _ *resolvedEachItem) {
			in.Cache.Scope = orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in, staged, config, run, it := base()
			mutate(in, staged, &config, &run, it)
			if got := keyOf(t, in, config, run, staged, it).Key; got == baseKey {
				t.Fatalf("changing %s did not change the key", name)
			}
		})
	}
	t.Run("not an item", func(t *testing.T) {
		in, staged, config, run, _ := base()
		if keyOf(t, in, config, run, staged, nil).Key == baseKey {
			t.Fatal("dropping the item did not change the key")
		}
	})
}

func TestMemoKey_RunCoordinatesDoNotChangeTheKey(t *testing.T) {
	config, run := []byte("{}\n"), []byte("{}\n")
	base := func() (*orkstackv1.ExecuteTaskInput, *stagedInputs) { return pinnedKeyInput() }
	in, staged := base()
	baseKey := keyOf(t, in, config, run, staged, nil).Key
	cases := map[string]func(in *orkstackv1.ExecuteTaskInput, staged *stagedInputs){
		"run id":            func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.RunId = "ork-2" },
		"idempotency key":   func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.IdempotencyKey = "x" },
		"commit uri":        func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.CommitUri = "mem://x" },
		"pool":              func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.Pool = "gpu" },
		"orkflow version":   func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.OrkflowVersion = "9" },
		"definition digest": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.OrkflowDefinitionDigest = "sha256:x" },
		"limits": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) {
			in.Limits = &orkstackv1.WorkspaceLimits{ValuesMaxBytes: 1}
		},
		"timeout": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.TimeoutSeconds = 999 },
		"grace":   func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.CancellationGracePeriodSeconds = 9 },
		"resources": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) {
			in.Resources = &orkstackv1.ResourceRequest{Cpu: "8"}
		},
		"telemetry": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) {
			in.Telemetry = &orkstackv1.TelemetrySelection{Sink: "x"}
		},
		"the cache TTL":     func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.Cache.TtlSeconds = 1 },
		"the cache refresh": func(in *orkstackv1.ExecuteTaskInput, _ *stagedInputs) { in.Cache.Refresh = true },
		"file order": func(_ *orkstackv1.ExecuteTaskInput, staged *stagedInputs) {
			staged.inputFiles[0], staged.inputFiles[1] = staged.inputFiles[1], staged.inputFiles[0]
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in, staged := base()
			mutate(in, staged)
			if got := keyOf(t, in, config, run, staged, nil).Key; got != baseKey {
				t.Fatalf("changing %s changed the key", name)
			}
		})
	}
	t.Run("the orkflow name under namespace scope", func(t *testing.T) {
		in, staged := base()
		in.Cache.Scope = orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE
		a := keyOf(t, in, config, run, staged, nil).Key
		in.OrkflowName = "another-orkflow"
		if keyOf(t, in, config, run, staged, nil).Key != a {
			t.Fatal("the orkflow name changed a namespace-scoped key")
		}
	})
	t.Run("network UNSPECIFIED and DEFAULT", func(t *testing.T) {
		in, staged := base()
		in.Network = orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED
		a := keyOf(t, in, config, run, staged, nil).Key
		in.Network = orkstackv1.NetworkMode_NETWORK_MODE_DEFAULT
		if keyOf(t, in, config, run, staged, nil).Key != a {
			t.Fatal("UNSPECIFIED and DEFAULT keyed differently")
		}
	})
	t.Run("an upstream artifact's uri, same digest", func(t *testing.T) {
		// Staged end to end from two different producing URIs whose bytes
		// agree: the key covers a file by container path and digest only.
		h := newCacheHarness(t)
		keyFor := func(producerRun string) string {
			ref := h.put(t, "data.csv", []byte("same bytes"))
			ref.Uri = strings.Replace(ref.GetUri(), "data.csv", "data.csv", 1)
			in := cachedTaskInput(t, "ork-"+producerRun)
			in.Cache.Refresh = true
			in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "fetch", Artifacts: []*orkstackv1.ArtifactRef{ref}}}
			ws, err := NewWorkspace(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			staged, failure := h.dispatcher.stageInputs(context.Background(), in, ws, true)
			if failure != nil {
				t.Fatal(failure)
			}
			// A different URI for the same digest: what a producing run's own
			// S3 key looks like (s3.go names the producer).
			staged.deferred[0].ref.Uri = "s3://bucket/orkflows/x/" + producerRun + "/data.csv"
			return keyOf(t, in, config, run, staged, nil).Key
		}
		if keyFor("run-a") != keyFor("run-b") {
			t.Fatal("an upstream artifact's URI reached the key")
		}
	})
}

func TestMemoKey_InputWithoutDigestIsUnkeyable(t *testing.T) {
	in, _ := pinnedKeyInput()
	staged := &stagedInputs{}
	staged.noteInputFile("/orkstack/input/artifacts/x/y", "y", "")
	if _, ok := computeMemoKey(in, []byte("{}\n"), []byte("{}\n"), staged, nil); ok {
		t.Fatal("an input with no digest was keyed")
	}

	// End to end: the attempt is a MISS INPUT_UNHASHED, even under refresh,
	// runs, and writes no memo.
	for _, refresh := range []bool{false, true} {
		h := newCacheHarness(t)
		in := cachedTaskInput(t, "ork-unhashed")
		in.Cache.Refresh = refresh
		ref := h.put(t, "data.csv", []byte("x"))
		ref.Sha256 = ""
		in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "fetch", Artifacts: []*orkstackv1.ArtifactRef{ref}}}
		result, err := h.dispatcher.Execute(context.Background(), in)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if c := result.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS ||
			c.GetMissReason() != MemoMissInputUnhashed || c.GetKey() != "" {
			t.Fatalf("refresh=%v cache = %v", refresh, c)
		}
		if _, puts := h.memos.calls(); puts != 0 {
			t.Fatalf("an unhashed miss wrote %d memos", puts)
		}
	}
}

// ---------------------------------------------------------------- hits

func TestExecute_CacheHitReturnsTheSourceResultWithoutAContainer(t *testing.T) {
	h := newCacheHarness(t)
	envName := "ORK_TEST_CACHE_SECRET"
	t.Setenv(envName, "s3cret")
	first := cachedTaskInput(t, "ork-run-1")
	first.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "env:" + envName}}
	source := h.produce(t, first)
	if c := source.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS || c.GetMissReason() != MemoMissAbsent {
		t.Fatalf("first run cache = %v", c)
	}
	pullsBefore, createsBefore := h.runtime.counts()
	// The secret's source no longer resolves: a hit must never read it.
	if err := os.Unsetenv(envName); err != nil {
		t.Fatal(err)
	}
	h.clearLines()

	h.now = h.now.Add(time.Hour)
	second := cachedTaskInput(t, "ork-run-2")
	second.Secrets = first.Secrets
	result, err := h.dispatcher.Execute(context.Background(), second)
	if err != nil {
		t.Fatalf("Execute (hit): %v", err)
	}
	if pulls, creates := h.runtime.counts(); pulls != pullsBefore || creates != createsBefore {
		t.Fatalf("a hit pulled (%d→%d) or created (%d→%d)", pullsBefore, pulls, createsBefore, creates)
	}
	c := result.GetCache()
	if c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || c.GetSource().GetRunId() != "ork-run-1" ||
		c.GetSource().GetCommitUri() != first.GetCommitUri() || c.GetSource().GetCommitSha256() != source.GetCommit().GetSha256() {
		t.Fatalf("cache = %v, want HIT from ork-run-1", c)
	}
	if len(c.GetKeyComponents()) != 0 {
		t.Fatal("a result carries key_components")
	}
	// The result's references are the source's; the commit is this run's.
	if !proto.Equal(result.GetValuesRef(), source.GetValuesRef()) || len(result.GetArtifacts()) != 1 ||
		!proto.Equal(result.GetArtifacts()[0], source.GetArtifacts()[0]) ||
		!proto.Equal(result.GetImage(), source.GetImage()) ||
		len(result.GetSystemArtifacts()) != len(source.GetSystemArtifacts()) {
		t.Fatalf("hit references differ from the source's:\n%v\n%v", result, source)
	}
	if result.GetCommit().GetUri() != second.GetCommitUri() || result.GetCommitAdopted() || result.GetExitCode() != 0 ||
		result.GetOrkerId() != "orker-cache-test" || result.GetAttempt() != 1 {
		t.Fatalf("hit result fields = %v", result)
	}
	if !result.GetValuesInlined() || string(result.GetValuesJson()) != `{"rows":3}` {
		t.Fatalf("values inline = %q (%v)", result.GetValuesJson(), result.GetValuesInlined())
	}
	record, err := h.commits.Get(context.Background(), second.GetCommitUri())
	if err != nil {
		t.Fatalf("this run's commit: %v", err)
	}
	commit := record.Commit
	if commit.GetRunId() != "ork-run-2" || commit.GetIdempotencyKey() != second.GetIdempotencyKey() ||
		commit.GetOrkflowDefinitionDigest() != second.GetOrkflowDefinitionDigest() ||
		commit.GetCache().GetSource().GetRunId() != "ork-run-1" ||
		commit.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT ||
		commit.GetCache().GetMemoWritten() || len(commit.GetCache().GetKeyComponents()) != 0 ||
		!proto.Equal(commit.GetValues(), source.GetValuesRef()) || commit.GetExitCode() != 0 {
		t.Fatalf("the hit commit = %v", commit)
	}
	if !h.hasLine(`[ork] cache hit: reusing task "featurize" of run ork-run-1`) || !h.hasLine("no container ran") {
		t.Fatalf("no cache-hit line: %q", h.loggedLines())
	}
	if h.hasLine("featurized 3 rows") {
		t.Fatal("the hit's log carries a container's output")
	}
}

func TestExecute_HitOfAHitCommitNamesTheOriginalProducer(t *testing.T) {
	h := newCacheHarness(t)
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	second := cachedTaskInput(t, "ork-run-2")
	hit, err := h.dispatcher.Execute(context.Background(), second)
	if err != nil || hit.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		t.Fatalf("second run: %v %v", hit.GetCache(), err)
	}
	// Hand-write the memo to point at run 2's HIT commit.
	memo, _ := h.memoFor(t, hit, second)
	memo.Source.RunId = "ork-run-2"
	memo.Source.CommitUri = second.GetCommitUri()
	memo.Source.CommitSha256 = hit.GetCommit().GetSha256()
	record, _ := h.commits.Get(context.Background(), second.GetCommitUri())
	memo.Source.FinishedAt = record.Commit.GetFinishedAt()
	if err := h.memos.MemoryMemoStore.PutMemo(context.Background(), memo); err != nil {
		t.Fatal(err)
	}
	third, err := h.dispatcher.Execute(context.Background(), cachedTaskInput(t, "ork-run-3"))
	if err != nil {
		t.Fatal(err)
	}
	if got := third.GetCache().GetSource().GetRunId(); got != "ork-run-1" {
		t.Fatalf("a hit of a hit names %q, want the original producer ork-run-1", got)
	}
}

// ---------------------------------------------------------------- misses

func TestExecute_CacheMissReasons(t *testing.T) {
	type fixture struct {
		h      *cacheHarness
		source *orkstackv1.ExecuteTaskResult
		in1    *orkstackv1.ExecuteTaskInput
		memo   *orkstackv1.CacheMemo
		ref    artifacts.MemoRef
		in     *orkstackv1.ExecuteTaskInput
	}
	rewriteMemo := func(t *testing.T, f *fixture, mutate func(*orkstackv1.CacheMemo)) {
		mutate(f.memo)
		if err := f.h.memos.MemoryMemoStore.PutMemo(context.Background(), f.memo); err != nil {
			t.Fatal(err)
		}
	}
	// forgeSource rewrites the source commit (as a new object) and repoints
	// the memo at it with a matching digest, so only the mutated field fails.
	forgeSource := func(t *testing.T, f *fixture, mutate func(*orkstackv1.ActivityCommit)) {
		record, err := f.h.commits.Get(context.Background(), f.memo.GetSource().GetCommitUri())
		if err != nil {
			t.Fatal(err)
		}
		forged := proto.Clone(record.Commit).(*orkstackv1.ActivityCommit)
		mutate(forged)
		uri := cacheRoot + "/forged/" + t.Name()
		created, err := f.h.commits.MemoryCommitStore.Create(context.Background(), uri, forged)
		if err != nil {
			t.Fatal(err)
		}
		rewriteMemo(t, f, func(m *orkstackv1.CacheMemo) {
			m.Source.CommitUri = uri
			m.Source.CommitSha256 = created.SHA256
			m.Source.RunId = forged.GetRunId()
			m.Source.FinishedAt = forged.GetFinishedAt()
		})
	}
	localPath := func(t *testing.T, uri string) string {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		return filepath.FromSlash(u.Path)
	}
	rows := []struct {
		name   string
		reason string
		setup  func(t *testing.T, f *fixture)
	}{
		{"memo store nil", MemoMissStoreAbsent, func(_ *testing.T, f *fixture) { f.h.dispatcher.Memos = nil }},
		{"absent", MemoMissAbsent, func(t *testing.T, f *fixture) {
			_ = f.h.memos.DeleteMemo(context.Background(), f.ref)
		}},
		{"store error", MemoMissUnreadable, func(_ *testing.T, f *fixture) { f.h.memos.getErr = errors.New("503 slow down") }},
		{"lookup times out", MemoMissUnreadable, func(t *testing.T, f *fixture) {
			f.h.memos.blockGet = true
			saved := memoLookupTimeout
			memoLookupTimeout = 50 * time.Millisecond
			t.Cleanup(func() { memoLookupTimeout = saved })
		}},
		{"undecodable", MemoMissUnreadable, func(t *testing.T, f *fixture) {
			if err := f.h.memos.PutRaw(f.ref, []byte("garbage")); err != nil {
				t.Fatal(err)
			}
		}},
		{"unknown schema", MemoMissUnreadable, func(t *testing.T, f *fixture) {
			rewriteMemo(t, f, func(m *orkstackv1.CacheMemo) { m.SchemaVersion = 2 })
		}},
		{"key mismatch", MemoMissMismatch, func(t *testing.T, f *fixture) {
			raw, _ := artifacts.MarshalMemo(f.memo)
			raw = bytes.Replace(raw, []byte(f.memo.GetKey()), []byte(strings.Repeat("f", 64)), 1)
			if err := f.h.memos.PutRaw(f.ref, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{"namespace mismatch", MemoMissMismatch, func(t *testing.T, f *fixture) {
			raw, _ := artifacts.MarshalMemo(f.memo)
			raw = bytes.Replace(raw, []byte(`"namespace": "default"`), []byte(`"namespace": "other"`), 1)
			if err := f.h.memos.PutRaw(f.ref, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{"task mismatch", MemoMissMismatch, func(t *testing.T, f *fixture) {
			raw, _ := artifacts.MarshalMemo(f.memo)
			raw = bytes.Replace(raw, []byte(`"taskId": "featurize"`), []byte(`"taskId": "train"`), 1)
			if err := f.h.memos.PutRaw(f.ref, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{"orkflow mismatch", MemoMissMismatch, func(t *testing.T, f *fixture) {
			raw, _ := artifacts.MarshalMemo(f.memo)
			raw = bytes.Replace(raw, []byte(`"orkflowName": "cached-pipeline"`), []byte(`"orkflowName": "other"`), 1)
			if err := f.h.memos.PutRaw(f.ref, raw); err != nil {
				t.Fatal(err)
			}
		}},
		{"expired", MemoMissExpired, func(_ *testing.T, f *fixture) { f.h.now = f.h.now.Add(48 * time.Hour) }},
		{"source commit absent", MemoMissSourceGone, func(t *testing.T, f *fixture) {
			rewriteMemo(t, f, func(m *orkstackv1.CacheMemo) { m.Source.CommitUri = cacheRoot + "/retired/commit.json" })
		}},
		{"source commit undecodable", MemoMissSourceUnreadable, func(t *testing.T, f *fixture) {
			uri := cacheRoot + "/garbage/commit.json"
			f.h.commits.PutRaw(uri, []byte("garbage"))
			rewriteMemo(t, f, func(m *orkstackv1.CacheMemo) { m.Source.CommitUri = uri })
		}},
		{"source sha256 changed", MemoMissSourceChanged, func(t *testing.T, f *fixture) {
			rewriteMemo(t, f, func(m *orkstackv1.CacheMemo) { m.Source.CommitSha256 = strings.Repeat("0", 64) })
		}},
		{"other namespace", MemoMissSourceMismatch, func(t *testing.T, f *fixture) {
			forgeSource(t, f, func(c *orkstackv1.ActivityCommit) { c.Namespace = "other" })
		}},
		{"other image digest", MemoMissSourceMismatch, func(t *testing.T, f *fixture) {
			forgeSource(t, f, func(c *orkstackv1.ActivityCommit) { c.Image.IndexDigest = "sha256:" + strings.Repeat("e", 64) })
		}},
		{"non-zero exit", MemoMissSourceMismatch, func(t *testing.T, f *fixture) {
			forgeSource(t, f, func(c *orkstackv1.ActivityCommit) { c.ExitCode = 1 })
		}},
		{"a memo cannot extend its own age", MemoMissSourceMismatch, func(t *testing.T, f *fixture) {
			rewriteMemo(t, f, func(m *orkstackv1.CacheMemo) {
				m.Source.FinishedAt = timestamppb.New(m.GetSource().GetFinishedAt().AsTime().Add(time.Minute))
			})
		}},
		{"values over limit", MemoMissSourceOverLimits, func(_ *testing.T, f *fixture) { f.in.Limits.ValuesMaxBytes = 2 }},
		{"artifact count over limit", MemoMissSourceOverLimits, func(t *testing.T, f *fixture) {
			forgeSource(t, f, func(c *orkstackv1.ActivityCommit) {
				extra := proto.Clone(c.GetArtifacts()[0]).(*orkstackv1.ArtifactRef)
				extra.Name = "second.txt"
				c.Artifacts = append(c.Artifacts, extra)
			})
			f.in.Limits.ArtifactMaxCount = 1
		}},
		{"artifact over size", MemoMissSourceOverLimits, func(_ *testing.T, f *fixture) { f.in.Limits.ArtifactMaxBytes = 2 }},
		{"values object missing", MemoMissSourceObjectAbsent, func(t *testing.T, f *fixture) {
			if err := os.Remove(localPath(t, f.source.GetValuesRef().GetUri())); err != nil {
				t.Fatal(err)
			}
		}},
		{"values corrupt", MemoMissSourceObjectAbsent, func(t *testing.T, f *fixture) {
			path := localPath(t, f.source.GetValuesRef().GetUri())
			_ = os.Chmod(path, 0o644)
			if err := os.WriteFile(path, []byte(`{"rows":4}`), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"artifact object missing", MemoMissSourceObjectAbsent, func(t *testing.T, f *fixture) {
			if err := os.Remove(localPath(t, f.source.GetArtifacts()[0].GetUri())); err != nil {
				t.Fatal(err)
			}
		}},
		{"artifact check times out", MemoMissUnreadable, func(t *testing.T, f *fixture) {
			blocking := &blockingStatStore{Store: f.h.store}
			blocking.armed.Store(true)
			f.h.dispatcher.Store = blocking
			saved := memoLookupTimeout
			memoLookupTimeout = 50 * time.Millisecond
			t.Cleanup(func() { memoLookupTimeout = saved })
		}},
		{"hit commit over commit_max_bytes", MemoMissSourceOverLimits, func(_ *testing.T, f *fixture) { f.in.Limits.CommitMaxBytes = 64 }},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			h := newCacheHarness(t)
			in1 := cachedTaskInput(t, "ork-src")
			source := h.produce(t, in1)
			memo, ref := h.memoFor(t, source, in1)
			f := &fixture{h: h, source: source, in1: in1, memo: memo, ref: ref, in: cachedTaskInput(t, "ork-miss")}
			row.setup(t, f)
			_, createsBefore := h.runtime.counts()
			result, err := h.dispatcher.Execute(context.Background(), f.in)
			if err != nil {
				// A limit row can make the CONTAINER's own output exceed the
				// same limit: the miss is still recorded on the attempt.
				record := lastAttempt(t, h.attempts, "ork-miss", "featurize")
				if record.GetCache().GetMissReason() != row.reason {
					t.Fatalf("Execute: %v; attempt cache = %v, want %s", err, record.GetCache(), row.reason)
				}
				if record.GetCache().GetMissDetail() == "" {
					t.Fatal("a miss without a sentence")
				}
				return
			}
			c := result.GetCache()
			if c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS || c.GetMissReason() != row.reason {
				t.Fatalf("cache = %v, want MISS %s", c, row.reason)
			}
			if c.GetMissDetail() == "" || len(c.GetMissDetail()) > 1024 {
				t.Fatalf("miss detail = %q", c.GetMissDetail())
			}
			if _, creates := h.runtime.counts(); creates != createsBefore+1 {
				t.Fatal("a miss did not run the container")
			}
			if !h.hasLine("[ork] cache miss (" + row.reason + ")") {
				t.Fatalf("no miss line for %s: %q", row.reason, h.loggedLines())
			}
		})
	}
	t.Run("commit store nil", func(t *testing.T) {
		h := newCacheHarness(t)
		h.dispatcher.Commits = nil
		result, err := h.dispatcher.Execute(context.Background(), cachedTaskInput(t, "ork-miss"))
		if err != nil {
			t.Fatal(err)
		}
		if result.GetCache().GetMissReason() != MemoMissStoreAbsent {
			t.Fatalf("cache = %v", result.GetCache())
		}
	})
}

func TestExecute_CacheMissWritesTheMemoAfterTheCommit(t *testing.T) {
	h := newCacheHarness(t)
	in := cachedTaskInput(t, "ork-run-1")
	result, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if events := h.events.list(); len(events) != 2 || events[0] != "create-commit" || events[1] != "put-memo" {
		t.Fatalf("events = %v, want the commit create before the memo put", events)
	}
	if !result.GetCache().GetMemoWritten() || result.GetCache().GetMemoWriteError() != "" {
		t.Fatalf("result cache = %v", result.GetCache())
	}
	memo, _ := h.memoFor(t, result, in)
	src := memo.GetSource()
	if memo.GetSchemaVersion() != 1 || memo.GetNamespace() != "default" ||
		memo.GetScope() != orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW || memo.GetOrkflowName() != "cached-pipeline" ||
		memo.GetTaskId() != "featurize" || memo.GetOrkerId() != "orker-cache-test" || memo.GetBuildId() != "build-cache" ||
		len(memo.GetKeyComponents()) == 0 || !memo.GetCreatedAt().AsTime().Equal(h.now) {
		t.Fatalf("memo = %v", memo)
	}
	if src.GetRunId() != "ork-run-1" || src.GetCommitUri() != in.GetCommitUri() ||
		src.GetCommitSha256() != result.GetCommit().GetSha256() || src.GetAttempt() != 1 ||
		src.GetFinishedAt() == nil || src.GetStartedAt() == nil || src.Item != nil {
		t.Fatalf("memo source = %v", src)
	}
	record, _ := h.commits.Get(context.Background(), in.GetCommitUri())
	if c := record.Commit.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS ||
		c.GetMemoWritten() || len(c.GetKeyComponents()) != 0 || c.GetMissReason() != MemoMissAbsent {
		t.Fatalf("commit cache = %v", c)
	}
	if !h.hasLine("[ork] cache memo written for key") {
		t.Fatalf("no memo-written line: %q", h.loggedLines())
	}
}

func TestExecute_MemoWriteFailureNeverFailsTheTask(t *testing.T) {
	for _, mode := range []string{"errors", "blocks"} {
		t.Run(mode, func(t *testing.T) {
			h := newCacheHarness(t)
			if mode == "errors" {
				h.memos.putErr = errors.New("bucket says no")
			} else {
				h.memos.blockPut = true
				saved := memoWriteTimeout
				memoWriteTimeout = 50 * time.Millisecond
				t.Cleanup(func() { memoWriteTimeout = saved })
			}
			var logged bytes.Buffer
			h.dispatcher.Logger = slogTo(&logged)
			result, err := h.dispatcher.Execute(context.Background(), cachedTaskInput(t, "ork-run-1"))
			if err != nil {
				t.Fatalf("a memo write failure failed the task: %v", err)
			}
			if result.GetCache().GetMemoWritten() || result.GetCache().GetMemoWriteError() == "" {
				t.Fatalf("cache = %v", result.GetCache())
			}
			if !strings.Contains(logged.String(), "cache memo not written") {
				t.Fatalf("no warn: %s", logged.String())
			}
			if !h.hasLine("[ork] cache memo could not be written") {
				t.Fatal("no memo-failure line")
			}
		})
	}
}

func TestExecute_BypassedNeverReadsAMemoAndRefreshesIt(t *testing.T) {
	h := newCacheHarness(t)
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	getsBefore, _ := h.memos.calls()
	in := cachedTaskInput(t, "ork-run-2")
	in.Cache.Refresh = true
	result, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if gets, _ := h.memos.calls(); gets != getsBefore {
		t.Fatal("a bypass read a memo")
	}
	if c := result.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED || !c.GetMemoWritten() || c.GetKey() == "" {
		t.Fatalf("cache = %v", c)
	}
	memo, _ := h.memoFor(t, result, in)
	if memo.GetSource().GetRunId() != "ork-run-2" {
		t.Fatalf("the memo still names %s", memo.GetSource().GetRunId())
	}
	if !h.hasLine("[ork] cache bypassed: the run was submitted with no_cache; running the container") {
		t.Fatal("no bypass line")
	}
}

// winnerFor is a commit another attempt of in wrote first.
func winnerFor(in *orkstackv1.ExecuteTaskInput, values *orkstackv1.ArtifactRef, cache *orkstackv1.CacheResult) *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{
		SchemaVersion:           artifacts.CommitSchemaVersion,
		Namespace:               "default",
		RunId:                   in.GetRunId(),
		TaskId:                  in.GetTaskId(),
		OrkflowName:             in.GetOrkflowName(),
		OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
		IdempotencyKey:          in.GetIdempotencyKey(),
		Image:                   in.GetImage(),
		Values:                  values,
		Attempt:                 1,
		OrkerId:                 "orker-winner",
		Cache:                   cache,
	}
}

func TestExecute_ConflictAdoptionWritesNoMemo(t *testing.T) {
	h := newCacheHarness(t)
	in := cachedTaskInput(t, "ork-run-1")
	winnerCache := &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, Key: strings.Repeat("a", 64), MissReason: MemoMissAbsent}
	h.commits.hidden[in.GetCommitUri()] = winnerFor(in, h.put(t, "values.json", []byte(`{"rows":3}`)), winnerCache)
	result, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !result.GetCommitAdopted() || result.GetOrkerId() == "" {
		t.Fatalf("result = %v, want the winner adopted", result)
	}
	if _, puts := h.memos.calls(); puts != 0 {
		t.Fatalf("a conflict adoption wrote %d memos", puts)
	}
	if !proto.Equal(result.GetCache(), winnerCache) {
		t.Fatalf("adopted cache = %v, want the winner's %v", result.GetCache(), winnerCache)
	}
}

func TestExecute_CacheHitCommitConflictReturnsTheWinner(t *testing.T) {
	h := newCacheHarness(t)
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	in := cachedTaskInput(t, "ork-run-2")
	winnerCache := &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Key: strings.Repeat("b", 64),
		Source: &orkstackv1.CacheSource{RunId: "ork-run-1"}}
	h.commits.hidden[in.GetCommitUri()] = winnerFor(in, h.put(t, "values.json", []byte(`{"rows":3}`)), winnerCache)
	result, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !result.GetCommitAdopted() || !proto.Equal(result.GetCache(), winnerCache) {
		t.Fatalf("result = %v", result)
	}
	if record := lastAttempt(t, h.attempts, "ork-run-2", "featurize"); !proto.Equal(record.GetCache(), winnerCache) {
		t.Fatalf("the attempt record's cache = %v, want the adopted commit's", record.GetCache())
	}
}

func TestExecute_CacheHitCommitWriteFailureIsCommitWriteFailed(t *testing.T) {
	h := newCacheHarness(t)
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	h.commits.createErr = errors.New("bucket unavailable")
	_, createsBefore := h.runtime.counts()
	_, err := h.dispatcher.Execute(context.Background(), cachedTaskInput(t, "ork-run-2"))
	if err == nil || !strings.Contains(err.Error(), ReasonCommitWriteFailed) {
		t.Fatalf("Execute = %v, want COMMIT_WRITE_FAILED", err)
	}
	if _, creates := h.runtime.counts(); creates != createsBefore {
		t.Fatal("a failed hit commit ran the container")
	}
}

func TestExecute_AdoptedCachedCommitKeepsItsCacheResult(t *testing.T) {
	h := newCacheHarness(t)
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	in := cachedTaskInput(t, "ork-run-2")
	hit, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil || hit.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		t.Fatalf("hit: %v %v", hit, err)
	}
	// The Orker died after the hit's commit and before completion: the retry
	// adopts it.
	_, createsBefore := h.runtime.counts()
	retry, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.GetCommitAdopted() || retry.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT ||
		retry.GetCache().GetSource().GetRunId() != "ork-run-1" || retry.GetCache().GetMemoWritten() {
		t.Fatalf("retry = %v", retry)
	}
	if _, creates := h.runtime.counts(); creates != createsBefore {
		t.Fatal("the retry ran the container")
	}
}

// ---------------------------------------------------------------- fan-out

func TestExecute_CachedFanOutItemKeysOnItsItem(t *testing.T) {
	h := newCacheHarness(t)
	input, _ := structpb.NewStruct(map[string]any{"parts": []any{"a", "b", "a"}})
	item := func(runID string, index int32) *orkstackv1.ExecuteTaskInput {
		in := cachedTaskInput(t, runID)
		in.TaskId = "convert"
		in.RunInput = &orkstackv1.Payload{Data: input}
		in.IdempotencyKey = task.ItemIdempotencyKeyFor("default", runID, "convert", index)
		key, _ := artifacts.ItemCommitKey("default", runID, "convert", index)
		in.CommitUri = cacheRoot + "/" + key
		in.Each = &orkstackv1.EachContext{Index: index, Count: 3, As: "part", Source: &orkstackv1.EachSource{Input: "parts"}}
		with, _ := structpb.NewStruct(map[string]any{"part": "{{ .each.part }}"})
		in.With = with
		return in
	}
	results := map[int32]*orkstackv1.ExecuteTaskResult{}
	for i := int32(0); i < 3; i++ {
		r, err := h.dispatcher.Execute(context.Background(), item("ork-run-1", i))
		if err != nil {
			t.Fatal(err)
		}
		results[i] = r
	}
	// Items 0 and 2 carry the same value but different positions: distinct keys.
	if results[0].GetCache().GetKey() == results[2].GetCache().GetKey() {
		t.Fatal("two positions of one value share a key")
	}
	memo, _ := h.memoFor(t, results[1], item("ork-run-1", 1))
	if memo.GetSource().Item == nil || *memo.GetSource().Item != 1 {
		t.Fatalf("an item's memo source item = %v", memo.GetSource().Item)
	}
	second, err := h.dispatcher.Execute(context.Background(), item("ork-run-2", 1))
	if err != nil {
		t.Fatal(err)
	}
	if second.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT ||
		second.GetCache().GetSource().GetItem() != 1 {
		t.Fatalf("item 1 of run 2 = %v", second.GetCache())
	}
	// The hit wrote run 2's ITEM commit, at the item key.
	if _, err := h.commits.Get(context.Background(), item("ork-run-2", 1).GetCommitUri()); err != nil {
		t.Fatalf("run 2's item commit: %v", err)
	}
	records, _ := h.attempts.ListAttempts(context.Background(), "ork-run-2", "convert", artifacts.ItemAt(1))
	if len(records) != 1 || records[0].GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		t.Fatalf("item attempt records = %v", records)
	}
}

func TestExecute_CachedDirectDependentKeysOnEveryItemFile(t *testing.T) {
	h := newCacheHarness(t)
	const runID = "ork-merge"
	writeItem := func(index int32, part string) {
		key, _ := artifacts.ItemCommitKey("default", runID, "convert", index)
		commit := &orkstackv1.ActivityCommit{
			SchemaVersion:           artifacts.CommitSchemaVersion,
			Namespace:               "default",
			RunId:                   runID,
			TaskId:                  "convert",
			IdempotencyKey:          task.ItemIdempotencyKeyFor("default", runID, "convert", index),
			OrkflowDefinitionDigest: "sha256:def-" + runID,
			Values:                  h.put(t, "values.json", []byte(`{}`)),
			Artifacts:               []*orkstackv1.ArtifactRef{h.put(t, "part.txt", []byte(part))},
		}
		uri := cacheRoot + "/" + key
		if _, err := h.commits.MemoryCommitStore.Create(context.Background(), uri, commit); err != nil {
			t.Fatal(err)
		}
	}
	mergeIn := func() *orkstackv1.ExecuteTaskInput {
		in := cachedTaskInput(t, runID)
		in.TaskId = "merge"
		in.IdempotencyKey = "default/" + runID + "/merge"
		key, _ := artifacts.CommitKey("default", runID, "merge")
		in.CommitUri = cacheRoot + "/" + key
		in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "convert", Each: &orkstackv1.EachUpstream{Count: 2, CommitRoot: cacheRoot + "/"}}}
		return in
	}
	writeItem(0, "a")
	writeItem(1, "b")
	in := mergeIn()
	in.Cache.Refresh = true // compute the key without reading
	first, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	components := lastAttempt(t, h.attempts, runID, "merge").GetCache().GetKeyComponents()
	if components["inputs"] == "" {
		t.Fatalf("no inputs component: %v", components)
	}
	// Change ONE item's artifact bytes: the inputs component, and the key, move.
	h2 := newCacheHarness(t)
	h = h2
	writeItem(0, "a")
	writeItem(1, "B")
	second, err := h.dispatcher.Execute(context.Background(), mergeIn())
	if err != nil {
		t.Fatal(err)
	}
	if second.GetCache().GetKey() == first.GetCache().GetKey() {
		t.Fatal("changing one item's file did not change the dependent's key")
	}
	if got := lastAttempt(t, h.attempts, runID, "merge").GetCache().GetKeyComponents()["inputs"]; got == components["inputs"] {
		t.Fatal("the inputs component did not move")
	}
}

// ---------------------------------------------------------------- unchanged paths

func TestExecute_UncachedInputIsUnchanged(t *testing.T) {
	h := newCacheHarness(t)
	in := cachedTaskInput(t, "ork-plain")
	in.Cache = nil
	result, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if gets, puts := h.memos.calls(); gets != 0 || puts != 0 {
		t.Fatalf("an uncached task touched the memo store: %d gets, %d puts", gets, puts)
	}
	if result.GetCache() != nil {
		t.Fatalf("an uncached result carries cache: %v", result.GetCache())
	}
	record, _ := h.commits.Get(context.Background(), in.GetCommitUri())
	if record.Commit.GetCache() != nil {
		t.Fatal("an uncached commit carries cache")
	}
	attempt := lastAttempt(t, h.attempts, "ork-plain", "featurize")
	if attempt.GetCache() != nil {
		t.Fatal("an uncached attempt record carries cache")
	}
	var phases []string
	for _, span := range attempt.GetPhases() {
		phases = append(phases, span.GetPhase())
	}
	if strings.Join(phases, ",") != strings.Join(wantPhaseSequence, ",") {
		t.Fatalf("phases = %v, want %v", phases, wantPhaseSequence)
	}
}

func TestExecute_PinnedInputIgnoresCache(t *testing.T) {
	h := newCacheHarness(t)
	in := cachedTaskInput(t, "ork-pinned")
	in.Pin = &orkstackv1.PinContext{Name: "build", InstanceId: "00000000-0000-0000-0000-000000000001", TimeoutSeconds: 60}
	result, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if gets, puts := h.memos.calls(); gets != 0 || puts != 0 || result.GetCache() != nil {
		t.Fatalf("a pinned input took the memo step: %d gets, %d puts, cache %v", gets, puts, result.GetCache())
	}
}

func TestPhases_CachedHitAndMissSequences(t *testing.T) {
	h := newCacheHarness(t)
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	miss := lastAttempt(t, h.attempts, "ork-run-1", "featurize")
	_, err := h.dispatcher.Execute(context.Background(), cachedTaskInput(t, "ork-run-2"))
	if err != nil {
		t.Fatal(err)
	}
	hit := lastAttempt(t, h.attempts, "ork-run-2", "featurize")
	names := func(a *orkstackv1.TaskAttempt) string {
		var out []string
		for _, s := range a.GetPhases() {
			out = append(out, s.GetPhase())
		}
		return strings.Join(out, ",")
	}
	if got := names(miss); got != "start,commit-check,workspace,stage-inputs,memo,stage-artifacts,image,render,create,run,exit,validate,commit" {
		t.Fatalf("miss phases = %s", got)
	}
	if got := names(hit); got != "start,commit-check,workspace,stage-inputs,memo" {
		t.Fatalf("hit phases = %s", got)
	}
}

// ---------------------------------------------------------------- staging

func TestStageInputs_DeferBytesFetchesValuesOnly(t *testing.T) {
	h := newCacheHarness(t)
	values := h.put(t, "values.json", []byte(`{"n":1}`))
	data := h.put(t, "data.csv", []byte("a,b\n"))
	in := cachedTaskInput(t, "ork-defer")
	in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "fetch", Values: values, Artifacts: []*orkstackv1.ArtifactRef{data}}}
	t.Setenv("ORK_TEST_DEFER_SECRET", "v")
	in.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "env:ORK_TEST_DEFER_SECRET"}}
	root := t.TempDir()
	ws, err := NewWorkspace(root)
	if err != nil {
		t.Fatal(err)
	}
	staged, failure := h.dispatcher.stageInputs(context.Background(), in, ws, true)
	if failure != nil {
		t.Fatal(failure)
	}
	if _, err := os.Stat(filepath.Join(ws.Input, "upstream", "fetch", "values.json")); err != nil {
		t.Fatalf("values.json not fetched: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ws.Input, "artifacts", "fetch", "data.csv")); !os.IsNotExist(err) {
		t.Fatalf("an artifact was fetched under deferBytes: %v", err)
	}
	if len(staged.deferred) != 1 || !staged.secretsDeferred || len(staged.secrets) != 0 {
		t.Fatalf("deferred = %d, secretsDeferred = %v, secrets = %v", len(staged.deferred), staged.secretsDeferred, staged.secrets)
	}
	if len(staged.inputFiles) != 2 || staged.upstreamValues["fetch"]["n"] != float64(1) {
		t.Fatalf("inputFiles = %v, upstreamValues = %v", staged.inputFiles, staged.upstreamValues)
	}
	if _, ok := staged.paths["fetch/data.csv"]; !ok {
		t.Fatal("the artifact helper's path is missing under deferBytes")
	}
}

func TestCompleteStaging_FetchesDeferredArtifactsThenSecrets(t *testing.T) {
	h := newCacheHarness(t)
	data := h.put(t, "data.csv", []byte("a,b\n"))
	in := cachedTaskInput(t, "ork-defer")
	in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "fetch", Artifacts: []*orkstackv1.ArtifactRef{data}}}
	t.Setenv("ORK_TEST_DEFER_SECRET", "v")
	in.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "env:ORK_TEST_DEFER_SECRET"}}
	ws, err := NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	staged, failure := h.dispatcher.stageInputs(context.Background(), in, ws, true)
	if failure != nil {
		t.Fatal(failure)
	}
	if failure := h.dispatcher.completeStaging(context.Background(), in, ws, staged); failure != nil {
		t.Fatal(failure)
	}
	if b, err := os.ReadFile(filepath.Join(ws.Input, "artifacts", "fetch", "data.csv")); err != nil || string(b) != "a,b\n" {
		t.Fatalf("deferred artifact = %q, %v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(ws.Secrets, "db")); err != nil || string(b) != "v" {
		t.Fatalf("deferred secret = %q, %v", b, err)
	}
	if len(staged.secrets) != 1 || len(staged.deferred) != 0 {
		t.Fatalf("after completeStaging: secrets %v, deferred %d", staged.secrets, len(staged.deferred))
	}
	// A corrupt deferred object is still INPUT_ARTIFACT_CORRUPT.
	bad := proto.Clone(data).(*orkstackv1.ArtifactRef)
	bad.Sha256 = strings.Repeat("0", 64)
	staged.deferred = []deferredFetch{{ref: bad, hostPath: filepath.Join(ws.Input, "artifacts", "fetch", "again.csv")}}
	if failure := h.dispatcher.completeStaging(context.Background(), in, ws, staged); failure == nil || failure.Reason != ReasonInputCorrupt {
		t.Fatalf("corrupt deferred fetch = %v", failure)
	}
}

func TestEncodeInputFile_MatchesWhatTheWorkspaceWrites(t *testing.T) {
	ws, err := NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"b": []any{1.0, "x"}, "a": map[string]any{"<": "&"}}
	if err := ws.WriteConfigJSON(value); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteRunJSON(nil); err != nil {
		t.Fatal(err)
	}
	written, _ := os.ReadFile(filepath.Join(ws.Input, "config.json"))
	encoded, err := encodeInputFile(configValue(value))
	if err != nil || !bytes.Equal(written, encoded) {
		t.Fatalf("config.json = %q, encodeInputFile = %q", written, encoded)
	}
	run, _ := os.ReadFile(filepath.Join(ws.Input, "run.json"))
	empty, _ := encodeInputFile(map[string]any{})
	if !bytes.Equal(run, empty) {
		t.Fatalf("run.json = %q, want %q", run, empty)
	}
	nilConfig, _ := encodeInputFile(configValue(nil))
	if string(nilConfig) != "{}\n" {
		t.Fatalf("nil config = %q", nilConfig)
	}
}

// ---------------------------------------------------------------- the record

func TestAttemptRecord_CarriesCacheAndComponents(t *testing.T) {
	h := newCacheHarness(t)
	h.produce(t, cachedTaskInput(t, "ork-run-1"))
	in := cachedTaskInput(t, "ork-run-2")
	if _, err := h.dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	hit := lastAttempt(t, h.attempts, "ork-run-2", "featurize")
	if hit.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || len(hit.GetCache().GetKeyComponents()) == 0 {
		t.Fatalf("hit record cache = %v", hit.GetCache())
	}
	if hit.GetPhase() != "memo" || hit.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED ||
		hit.GetExitCode() != 0 || len(hit.GetSystemArtifacts()) != 0 ||
		!proto.Equal(hit.GetImage(), in.GetImage()) || hit.GetImage().GetPlatform() != "" {
		t.Fatalf("hit record = %v", hit)
	}

	// A miss whose container then fails keeps its reason.
	h2 := newCacheHarness(t)
	h2.runtime.exit = 3
	failed := cachedTaskInput(t, "ork-fail")
	if _, err := h2.dispatcher.Execute(context.Background(), failed); err == nil {
		t.Fatal("exit 3 succeeded")
	}
	record := lastAttempt(t, h2.attempts, "ork-fail", "featurize")
	if record.GetCache().GetMissReason() != MemoMissAbsent || len(record.GetCache().GetKeyComponents()) == 0 ||
		record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
		t.Fatalf("failed miss record = %v", record)
	}
	if _, puts := h2.memos.calls(); puts != 0 {
		t.Fatal("a failed attempt wrote a memo")
	}
}

func TestDescribeOrker_ReportsCacheSupported(t *testing.T) {
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
	if !result.GetCacheSupported() {
		t.Fatal("this build implements the memo step and must say so")
	}
}
