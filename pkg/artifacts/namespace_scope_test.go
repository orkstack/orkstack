package artifacts

import (
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// namespaceOfAccepted is every namespace-first key a renderer of this package
// writes, with the namespace it names. The fuzz target seeds from it.
func namespaceOfAccepted() []struct{ key, ns string } {
	memoKey := strings.Repeat("ab", 32)
	commit := mustKey(CommitKey("team-a", "ork-run1", "build"))
	return []struct{ key, ns string }{
		{commit, "team-a"},
		{mustKey(CommitKey("", "ork-run1", "build")), "default"},
		{mustKey(ItemCommitKey("team-a", "ork-run1", "convert", 3)), "team-a"},
		{mustKey(CommitKey("team-a", "run with spaces", "build")), "team-a"},
		{mustKey(MemoKey(MemoRef{MemoScopeRef: MemoScopeRef{Namespace: "team-b", Orkflow: "etl", TaskID: "build"}, Key: memoKey})), "team-b"},
		{mustKey(MemoKey(MemoRef{MemoScopeRef: MemoScopeRef{Namespace: "team-b", NamespaceScoped: true, TaskID: "build"}, Key: memoKey})), "team-b"},
		{mustKey(AttestedCommitKey(commit)), "team-a"},
		{mustKey(RecordKey(RecordResolution, "team-c", "ork-run1", "approve", 0)), "team-c"},
		{mustKey(RecordKey(RecordComposed, "team-c", "ork-run1", "child", 7)), "team-c"},
		{mustKey(OutputsRecordKey("team-c", "ork-run1")), "team-c"},
	}
}

func TestNamespaceOf_NamesTheNamespaceOfEveryNamespaceFirstKey(t *testing.T) {
	for _, tc := range namespaceOfAccepted() {
		t.Run(tc.key, func(t *testing.T) {
			got, err := NamespaceOf(tc.key)
			if err != nil {
				t.Fatalf("NamespaceOf(%q): %v", tc.key, err)
			}
			if got != tc.ns {
				t.Fatalf("NamespaceOf(%q) = %q, want %q", tc.key, got, tc.ns)
			}
		})
	}
}

func TestNamespaceOf_RefusesAKeyNoRendererWrites(t *testing.T) {
	memoKey := strings.Repeat("ab", 32)
	for _, key := range []string{
		"",
		"metadata/commits",
		"metadata/commits/",
		"metadata/commits/team-a/ork-run1/build", // no leaf
		"metadata/commits/team-a/ork-run1/build/values.json",           // wrong leaf
		"metadata/commits/Team-A/ork-run1/build/commit.json",           // namespace rule
		"metadata/commits/team-a/../team-b/ork-run1/build/commit.json", // traversal
		"metadata/commits/team-a/ork-run1/../../team-b/x/build/commit.json",
		"metadata/commits//ork-run1/build/commit.json",
		"/metadata/commits/team-a/ork-run1/build/commit.json",
		CommitProbeKey,
		"metadata/memo/team-b/namespace/build/" + memoKey, // no suffix
		"metadata/memo/team-b/elsewhere/build/" + memoKey + ".json",
		"metadata/attested/team-c/ork-run1/approve/other.json",
		"metadata/attested/team-c/ork-run1/..",
		"metadata/attested/team-c/ork-run1/approve/r00000000000000000000/resolution.json", // scope 0 is never rendered
		"metadata/snapshots/team-a/ork-run1/1.pb",                                         // not an Orker key
		"metadata/attempts/ork-run1/build/1.json",                                         // carries no namespace
		"orkflows/etl/ork-run1/build/out.tar/" + memoKey,
	} {
		t.Run(key, func(t *testing.T) {
			ns, err := NamespaceOf(key)
			if err == nil {
				t.Fatalf("NamespaceOf(%q) = %q, want an error", key, ns)
			}
			if !errors.Is(err, ErrNotNamespaced) {
				t.Fatalf("NamespaceOf(%q) error %v, want ErrNotNamespaced", key, err)
			}
		})
	}
}

// FuzzNamespaceOf holds NamespaceOf to the property the guard rests on: a key
// it accepts names exactly one namespace, which is a valid namespace name and
// the key's first segment under its prefix, and no segment of the key is "",
// "." or "..", so no filesystem reading of it can leave that namespace.
func FuzzNamespaceOf(f *testing.F) {
	for _, tc := range namespaceOfAccepted() {
		f.Add(tc.key)
	}
	f.Add("metadata/commits/team-a/../team-b/ork-run1/build/commit.json")
	f.Add(CommitProbeKey)
	f.Fuzz(func(t *testing.T, key string) {
		ns, err := NamespaceOf(key)
		if err != nil {
			if !errors.Is(err, ErrNotNamespaced) {
				t.Fatalf("NamespaceOf(%q) error %v is not ErrNotNamespaced", key, err)
			}
			return
		}
		if err := validNamespaceName(ns); err != nil {
			t.Fatalf("NamespaceOf(%q) = %q, not a namespace name: %v", key, ns, err)
		}
		var rest string
		for _, prefix := range []string{CommitPrefix, MemoPrefix, AttestedPrefix} {
			if r, ok := strings.CutPrefix(key, prefix+"/"); ok {
				rest = r
			}
		}
		if first, _, _ := strings.Cut(rest, "/"); first != ns {
			t.Fatalf("NamespaceOf(%q) = %q, but the key's namespace segment is %q", key, ns, first)
		}
		for _, seg := range strings.Split(key, "/") {
			if seg == "" || seg == "." || seg == ".." {
				t.Fatalf("NamespaceOf(%q) accepted a key with segment %q", key, seg)
			}
		}
	})
}

// FuzzNamespaceOfURI: a uri NamespaceOfURI accepts is root + "/" + a key
// NamespaceOf accepts, naming the same namespace.
func FuzzNamespaceOfURI(f *testing.F) {
	key := mustKey(CommitKey("team-a", "ork-run1", "build"))
	f.Add("s3://bucket", "s3://bucket/"+key)
	f.Add("file:///var/ork", "file:///var/ork/"+key)
	f.Add("s3://bucket", "s3://bucketx/"+key)
	f.Add("", "/"+key)
	f.Fuzz(func(t *testing.T, root, uri string) {
		ns, err := NamespaceOfURI(root, uri)
		if err != nil {
			if !errors.Is(err, ErrNotNamespaced) {
				t.Fatalf("NamespaceOfURI(%q, %q) error %v is not ErrNotNamespaced", root, uri, err)
			}
			return
		}
		rest, ok := strings.CutPrefix(uri, root+"/")
		if !ok || root == "" {
			t.Fatalf("NamespaceOfURI(%q, %q) = %q, but the uri is not under the root", root, uri, ns)
		}
		if again, err := NamespaceOf(rest); err != nil || again != ns {
			t.Fatalf("NamespaceOfURI(%q, %q) = %q, but NamespaceOf(%q) = %q, %v", root, uri, ns, rest, again, err)
		}
	})
}

func TestParseNamespaces_ValidatesAndUnsetServesEvery(t *testing.T) {
	every, err := ParseNamespaces(nil)
	if err != nil || !every.Every() || !every.Serves("anything") || !every.Serves("") {
		t.Fatalf("ParseNamespaces(nil) = %v, %v; want one that serves every namespace", every, err)
	}
	some, err := ParseNamespaces([]string{"team-a", "default", "team-a"})
	if err != nil {
		t.Fatal(err)
	}
	if some.Every() {
		t.Fatal("a list serves every namespace")
	}
	for ns, want := range map[string]bool{"team-a": true, "default": true, "": true, "team-b": false} {
		if got := some.Serves(ns); got != want {
			t.Errorf("Serves(%q) = %v, want %v", ns, got, want)
		}
	}
	if got := strings.Join(some.Names(), ","); got != "default,team-a" {
		t.Errorf("Names() = %q, want default,team-a", got)
	}
	for _, bad := range [][]string{{""}, {"Team-A"}, {"team-a", "a/b"}, {"../x"}} {
		if _, err := ParseNamespaces(bad); !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("ParseNamespaces(%q) error %v, want ErrInvalidNamespace", bad, err)
		}
	}
}

func TestNamespaceOfURI_ReadsOnlyAKeyUnderItsRoot(t *testing.T) {
	key := mustKey(CommitKey("team-a", "ork-run1", "build"))
	for _, tc := range []struct {
		root, uri, want string
	}{
		{"s3://bucket", "s3://bucket/" + key, "team-a"},
		{"file:///var/ork", "file:///var/ork/" + key, "team-a"},
		{"s3://bucket", "s3://other/" + key, ""},
		{"s3://bucket", "s3://bucketx/" + key, ""},
		{"s3://bucket", "s3://bucket//" + key, ""},
		{"s3://bucket", key, ""},
		{"", "/" + key, ""},
	} {
		got, err := NamespaceOfURI(tc.root, tc.uri)
		if tc.want == "" {
			if err == nil {
				t.Errorf("NamespaceOfURI(%q, %q) = %q, want an error", tc.root, tc.uri, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("NamespaceOfURI(%q, %q) = %q, %v; want %q", tc.root, tc.uri, got, err, tc.want)
		}
	}
}

func TestScopeCommits_RefusesAnotherNamespacesKeyBeforeTheStore(t *testing.T) {
	ctx := t.Context()
	inner := NewMemoryCommitStore()
	served, err := ParseNamespaces([]string{"team-a"})
	if err != nil {
		t.Fatal(err)
	}
	const root = "s3://bucket"
	scoped := ScopeCommits(inner, root, served)
	commit := &orkstackv1.ActivityCommit{SchemaVersion: CommitSchemaVersion, Namespace: "team-b", RunId: "ork-run1", TaskId: "build"}
	foreign := root + "/" + mustKey(CommitKey("team-b", "ork-run1", "build"))
	if _, err := inner.Create(ctx, foreign, commit); err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.Get(ctx, foreign); !errors.Is(err, ErrNamespaceNotServed) {
		t.Fatalf("Get of team-b's commit: %v, want ErrNamespaceNotServed", err)
	}
	other := root + "/" + mustKey(CommitKey("team-b", "ork-run2", "build"))
	if _, err := scoped.Create(ctx, other, commit); !errors.Is(err, ErrNamespaceNotServed) {
		t.Fatalf("Create of a team-b commit: %v, want ErrNamespaceNotServed", err)
	}
	if _, err := inner.Get(ctx, other); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the refused Create reached the store: %v", err)
	}
	for _, uri := range []string{root + "/" + CommitProbeKey, "s3://other/" + mustKey(CommitKey("team-a", "ork-run1", "build"))} {
		if _, err := scoped.Get(ctx, uri); !errors.Is(err, ErrNamespaceNotServed) {
			t.Errorf("Get(%q): %v, want ErrNamespaceNotServed", uri, err)
		}
	}
	own := root + "/" + mustKey(CommitKey("team-a", "ork-run1", "build"))
	commit.Namespace = "team-a"
	if _, err := scoped.Create(ctx, own, commit); err != nil {
		t.Fatalf("Create of a served namespace's commit: %v", err)
	}
	if _, err := scoped.Get(ctx, own); err != nil {
		t.Fatalf("Get of a served namespace's commit: %v", err)
	}
	every, _ := ParseNamespaces(nil)
	if got := ScopeCommits(inner, root, every); got != CommitStore(inner) {
		t.Fatal("ScopeCommits wraps a store for an Orker that serves every namespace")
	}
}

func TestScopeMemos_RefusesAnotherNamespacesMemo(t *testing.T) {
	ctx := t.Context()
	inner := NewMemoryMemoStore()
	served, _ := ParseNamespaces([]string{"team-a"})
	scoped := ScopeMemos(inner, served)
	key := strings.Repeat("ab", 32)
	foreign := MemoRef{MemoScopeRef: MemoScopeRef{Namespace: "team-b", NamespaceScoped: true, TaskID: "build"}, Key: key}
	if _, err := scoped.GetMemo(ctx, foreign); !errors.Is(err, ErrNamespaceNotServed) {
		t.Errorf("GetMemo: %v, want ErrNamespaceNotServed", err)
	}
	if err := scoped.DeleteMemo(ctx, foreign); !errors.Is(err, ErrNamespaceNotServed) {
		t.Errorf("DeleteMemo: %v, want ErrNamespaceNotServed", err)
	}
	if _, _, err := scoped.ListMemos(ctx, foreign.MemoScopeRef, "", 10); !errors.Is(err, ErrNamespaceNotServed) {
		t.Errorf("ListMemos: %v, want ErrNamespaceNotServed", err)
	}
	memo := &orkstackv1.CacheMemo{Namespace: "team-b", Scope: orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE, TaskId: "build", Key: key}
	if err := scoped.PutMemo(ctx, memo); !errors.Is(err, ErrNamespaceNotServed) {
		t.Errorf("PutMemo: %v, want ErrNamespaceNotServed", err)
	}
	if _, err := inner.GetMemo(ctx, foreign); !errors.Is(err, ErrNotFound) {
		t.Errorf("the refused PutMemo reached the store: %v", err)
	}
	memo.Namespace = "team-a"
	if err := scoped.PutMemo(ctx, memo); err != nil {
		t.Errorf("PutMemo of a served namespace's memo: %v", err)
	}
}
