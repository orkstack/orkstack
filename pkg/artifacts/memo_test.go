package artifacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func memoHex(c byte) string { return strings.Repeat(string(c), 64) }

func sampleMemo(scope orkstackv1.CacheScope, orkflow, task, key string) *orkstackv1.CacheMemo {
	return &orkstackv1.CacheMemo{
		SchemaVersion: MemoSchemaVersion,
		Key:           key,
		Namespace:     "ml",
		Scope:         scope,
		OrkflowName:   orkflow,
		TaskId:        task,
		Source: &orkstackv1.CacheSource{
			RunId:        "ork-1-1",
			TaskId:       task,
			OrkflowName:  orkflow,
			CommitUri:    "mem://commits/metadata/commits/ml/ork-1-1/" + task + "/commit.json",
			CommitSha256: memoHex('c'),
			FinishedAt:   timestamppb.New(time.Unix(1_700_000_000, 0)),
			Attempt:      1,
		},
		KeyComponents: map[string]string{"image": "sha256:" + memoHex('d')},
	}
}

func TestMemoKey_Layout(t *testing.T) {
	cases := []struct {
		ref  MemoRef
		want string
	}{
		{MemoRef{MemoScopeRef{Namespace: "ml", Orkflow: "cached-pipeline", TaskID: "featurize"}, memoHex('a')},
			"metadata/memo/ml/orkflow/cached-pipeline/featurize/" + memoHex('a') + ".json"},
		{MemoRef{MemoScopeRef{Namespace: "ml", NamespaceScoped: true, TaskID: "toolchain"}, memoHex('b')},
			"metadata/memo/ml/namespace/toolchain/" + memoHex('b') + ".json"},
		{MemoRef{MemoScopeRef{Namespace: "", Orkflow: "p", TaskID: "t"}, memoHex('0')},
			"metadata/memo/default/orkflow/p/t/" + memoHex('0') + ".json"},
	}
	for _, tc := range cases {
		got, err := MemoKey(tc.ref)
		if err != nil {
			t.Fatalf("MemoKey(%+v): %v", tc.ref, err)
		}
		if got != tc.want {
			t.Fatalf("MemoKey(%+v) = %q, want %q", tc.ref, got, tc.want)
		}
	}
}

func TestMemoKey_RefusesInvalidCoordinates(t *testing.T) {
	good := memoHex('a')
	cases := map[string]MemoRef{
		"bad namespace":  {MemoScopeRef{Namespace: "ML", Orkflow: "p", TaskID: "t"}, good},
		"bad orkflow":    {MemoScopeRef{Namespace: "ml", Orkflow: "../p", TaskID: "t"}, good},
		"bad task":       {MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "T"}, good},
		"empty task":     {MemoScopeRef{Namespace: "ml", Orkflow: "p"}, good},
		"upper hex":      {MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t"}, strings.Repeat("A", 64)},
		"short key":      {MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t"}, "abc"},
		"neither family": {MemoScopeRef{Namespace: "ml", TaskID: "t"}, good},
		"both families":  {MemoScopeRef{Namespace: "ml", Orkflow: "p", NamespaceScoped: true, TaskID: "t"}, good},
		"long task id":   {MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t" + strings.Repeat("a", 200)}, good},
		"traversal key":  {MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t"}, "../" + good[3:]},
	}
	for name, ref := range cases {
		if _, err := MemoKey(ref); !errors.Is(err, ErrInvalidMemoKey) {
			t.Errorf("%s: MemoKey err = %v, want ErrInvalidMemoKey", name, err)
		}
	}
}

func TestParseMemoKey_RoundTripAndForeignKeys(t *testing.T) {
	for _, ref := range []MemoRef{
		{MemoScopeRef{Namespace: "ml", Orkflow: "cached-pipeline", TaskID: "featurize"}, memoHex('a')},
		{MemoScopeRef{Namespace: "default", NamespaceScoped: true, TaskID: "toolchain"}, memoHex('f')},
	} {
		key, err := MemoKey(ref)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := ParseMemoKey(key)
		if !ok || got != ref {
			t.Fatalf("ParseMemoKey(%q) = %+v, %v; want %+v", key, got, ok, ref)
		}
	}
	for _, foreign := range []string{
		"metadata/commits/ml/run/t/commit.json",
		"metadata/memo/ml/orkflow/p/t/" + memoHex('a') + ".txt",
		"metadata/memo/ml/orkflow/p/t/abc.json",
		"metadata/memo/ml/other/t/" + memoHex('a') + ".json",
		"metadata/memo/ml/namespace/t/x/" + memoHex('a') + ".json",
		"metadata/memo/ML/namespace/t/" + memoHex('a') + ".json",
		"metadata/memo/ml/orkflow/p/t/" + memoHex('a') + ".json.tmp",
	} {
		if ref, ok := ParseMemoKey(foreign); ok {
			t.Errorf("ParseMemoKey(%q) = %+v, ok; want foreign", foreign, ref)
		}
	}
}

func TestMemoScopePrefix(t *testing.T) {
	cases := []struct {
		scope MemoScopeRef
		want  string
	}{
		{MemoScopeRef{Namespace: "ml"}, "metadata/memo/ml/"},
		{MemoScopeRef{Namespace: "ml", Orkflow: "p"}, "metadata/memo/ml/orkflow/p/"},
		{MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t"}, "metadata/memo/ml/orkflow/p/t/"},
		{MemoScopeRef{Namespace: "ml", NamespaceScoped: true}, "metadata/memo/ml/namespace/"},
		{MemoScopeRef{Namespace: "ml", NamespaceScoped: true, TaskID: "t"}, "metadata/memo/ml/namespace/t/"},
	}
	for _, tc := range cases {
		got, err := MemoScopePrefix(tc.scope)
		if err != nil || got != tc.want {
			t.Fatalf("MemoScopePrefix(%+v) = %q, %v; want %q", tc.scope, got, err, tc.want)
		}
	}
	if _, err := MemoScopePrefix(MemoScopeRef{Namespace: "ml", TaskID: "t"}); !errors.Is(err, ErrInvalidMemoKey) {
		t.Fatalf("a task without a family: err = %v", err)
	}
}

func TestMarshalMemo_RoundTripsAndIsBounded(t *testing.T) {
	memo := sampleMemo(orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "p", "t", memoHex('a'))
	document, err := MarshalMemo(memo)
	if err != nil {
		t.Fatal(err)
	}
	again, err := MarshalMemo(memo)
	if err != nil || string(again) != string(document) {
		t.Fatalf("MarshalMemo is not stable within a build")
	}
	back, err := UnmarshalMemo(document)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(back, memo) {
		t.Fatalf("round trip = %v, want %v", back, memo)
	}
	huge := proto.Clone(memo).(*orkstackv1.CacheMemo)
	huge.KeyComponents["pad"] = strings.Repeat("x", MemoMaxBytes)
	if _, err := MarshalMemo(huge); !errors.Is(err, ErrInvalidMemoKey) {
		t.Fatalf("an oversized memo: err = %v, want refused", err)
	}
}

func TestUnmarshalMemo_GarbageIsUndecodable(t *testing.T) {
	for _, garbage := range []string{"", "not json", `{"key": 7}`, "[1,2]"} {
		if _, err := UnmarshalMemo([]byte(garbage)); !errors.Is(err, ErrMemoUndecodable) {
			t.Errorf("UnmarshalMemo(%q): err = %v, want ErrMemoUndecodable", garbage, err)
		}
	}
	// Unknown fields are discarded: a newer writer's memo reads here.
	if _, err := UnmarshalMemo([]byte(`{"schemaVersion":1,"fromTheFuture":true}`)); err != nil {
		t.Fatalf("an unknown field: %v", err)
	}
}

// exerciseMemoStore is the contract every backend must keep.
func exerciseMemoStore(t *testing.T, store MemoStore, plantForeign func(key string), setTime func(time.Time)) {
	t.Helper()
	ctx := context.Background()
	ref := MemoRef{MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t"}, memoHex('1')}
	honoured := setTime != nil
	if setTime == nil {
		setTime = func(time.Time) {}
	}

	if _, err := store.GetMemo(ctx, ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetMemo of an absent memo: err = %v, want ErrNotFound", err)
	}
	t0 := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	keys := []byte{'3', '1', '5', '2', '4'}
	for i, c := range keys {
		setTime(t0.Add(time.Duration(i) * time.Hour))
		if err := store.PutMemo(ctx, sampleMemo(orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "p", "t", memoHex(c))); err != nil {
			t.Fatalf("PutMemo: %v", err)
		}
	}
	setTime(t0)
	if err := store.PutMemo(ctx, sampleMemo(orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, "p", "t", memoHex('9'))); err != nil {
		t.Fatalf("PutMemo namespace-scoped: %v", err)
	}
	plantForeign("metadata/memo/ml/orkflow/p/t/not-a-memo.json")

	// Last writer wins.
	newer := sampleMemo(orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, "p", "t", memoHex('1'))
	newer.Source.RunId = "ork-2-2"
	setTime(t0.Add(10 * time.Hour))
	if err := store.PutMemo(ctx, newer); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetMemo(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSource().GetRunId() != "ork-2-2" {
		t.Fatalf("last writer did not win: %v", got.GetSource().GetRunId())
	}

	// Pagination in key order, foreign object skipped.
	scope := MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t"}
	var all []MemoEntry
	after := ""
	for pages := 0; ; pages++ {
		entries, cursor, err := store.ListMemos(ctx, scope, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, entries...)
		if cursor == "" {
			break
		}
		after = cursor
		if pages > 10 {
			t.Fatal("pagination does not end")
		}
	}
	if len(all) != 5 {
		t.Fatalf("listed %d memos, want 5: %+v", len(all), all)
	}
	for i, want := range []byte{'1', '2', '3', '4', '5'} {
		if all[i].Ref.Key != memoHex(want) {
			t.Fatalf("entry %d key = %s, want %s", i, all[i].Ref.Key, memoHex(want))
		}
		if all[i].Size <= 0 {
			t.Fatalf("entry %d size = %d", i, all[i].Size)
		}
	}
	if honoured && !all[0].LastModified.Equal(t0.Add(10*time.Hour)) {
		t.Fatalf("LastModified of the rewritten memo = %v", all[0].LastModified)
	}
	// The whole namespace lists both families.
	whole, _, err := store.ListMemos(ctx, MemoScopeRef{Namespace: "ml"}, "", 100)
	if err != nil || len(whole) != 6 {
		t.Fatalf("namespace listing = %d, %v; want 6", len(whole), err)
	}
	// Delete is idempotent.
	if err := store.DeleteMemo(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteMemo(ctx, ref); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, err := store.GetMemo(ctx, ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: err = %v", err)
	}
}

func TestMemoryMemoStore_PutGetListDelete(t *testing.T) {
	store := NewMemoryMemoStore()
	var now time.Time
	store.Now = func() time.Time { return now }
	exerciseMemoStore(t, store, func(key string) { store.PutForeign(key, []byte("{}")) },
		func(at time.Time) { now = at })

	ref := MemoRef{MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t"}, memoHex('7')}
	if err := store.PutRaw(ref, []byte("garbage")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetMemo(context.Background(), ref); !errors.Is(err, ErrMemoUndecodable) {
		t.Fatalf("garbage memo: err = %v, want ErrMemoUndecodable", err)
	}
}

func TestLocalMemoStore_PutGetListDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalMemoStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	exerciseMemoStore(t, store, func(key string) {
		full := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}, nil)

	// LastModified is the file's mtime.
	ref := MemoRef{MemoScopeRef{Namespace: "ml", Orkflow: "p", TaskID: "t"}, memoHex('2')}
	key, _ := MemoKey(ref)
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(key)), stamp, stamp); err != nil {
		t.Fatal(err)
	}
	entries, _, err := store.ListMemos(context.Background(), ref.MemoScopeRef, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	var seen bool
	for _, e := range entries {
		if e.Ref == ref {
			seen = true
			if !e.LastModified.Equal(stamp) {
				t.Fatalf("LastModified = %v, want %v", e.LastModified, stamp)
			}
		}
	}
	if !seen {
		t.Fatal("memo not listed")
	}
	// Garbage at a memo key is undecodable, not absent.
	if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(key)), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetMemo(context.Background(), ref); !errors.Is(err, ErrMemoUndecodable) {
		t.Fatalf("garbage: err = %v", err)
	}
	// An empty scope lists empty.
	none, cursor, err := store.ListMemos(context.Background(), MemoScopeRef{Namespace: "other"}, "", 10)
	if err != nil || len(none) != 0 || cursor != "" {
		t.Fatalf("empty scope = %v, %q, %v", none, cursor, err)
	}
}

func TestMemoRefFor(t *testing.T) {
	ref, err := MemoRefFor(sampleMemo(orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, "p", "t", memoHex('a')))
	if err != nil || !ref.NamespaceScoped || ref.Orkflow != "" {
		t.Fatalf("namespace scope ref = %+v, %v", ref, err)
	}
	if _, err := MemoRefFor(sampleMemo(orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED, "p", "t", memoHex('a'))); err == nil {
		t.Fatal("UNSPECIFIED scope addressed a memo")
	}
}
