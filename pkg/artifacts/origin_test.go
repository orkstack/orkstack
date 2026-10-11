package artifacts_test

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// runOriginKindDescriptor is the enum as the generated descriptor declares
// it, so a value added to the proto without a slug fails here.
func runOriginKindDescriptor() protoreflect.EnumDescriptor {
	return orkstackv1.File_orkstack_v1_run_proto.Enums().ByName("RunOriginKind")
}

func TestOriginKindSlug_EveryDeclaredValueHasASlugAndRoundTrips(t *testing.T) {
	values := runOriginKindDescriptor().Values()
	if values.Len() < 6 {
		t.Fatalf("RunOriginKind declares %d values, want at least 6", values.Len())
	}
	for i := 0; i < values.Len(); i++ {
		value := values.Get(i)
		kind := orkstackv1.RunOriginKind(value.Number())
		slug, ok := artifacts.OriginKindSlug(kind)
		if kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
			if ok || slug != "" {
				t.Fatalf("UNSPECIFIED has slug %q (ok=%v), want none", slug, ok)
			}
			continue
		}
		if !ok || slug == "" {
			t.Fatalf("%s has no slug", value.Name())
		}
		want := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(string(value.Name()), "RUN_ORIGIN_KIND_")), "_", "-")
		if slug != want {
			t.Fatalf("%s slug = %q, want %q", value.Name(), slug, want)
		}
		back, ok := artifacts.ParseOriginKindSlug(slug)
		if !ok || back != kind {
			t.Fatalf("ParseOriginKindSlug(%q) = %v, %v; want %v", slug, back, ok, kind)
		}
	}
	if slug, _ := artifacts.OriginKindSlug(orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN); slug != "parent-run" {
		t.Fatalf("PARENT_RUN slug = %q, want parent-run", slug)
	}
	for _, bad := range []string{"", "unspecified", "UNSPECIFIED", "Trigger", "parent_run", "bogus"} {
		if kind, ok := artifacts.ParseOriginKindSlug(bad); ok {
			t.Fatalf("ParseOriginKindSlug(%q) = %v, want refused", bad, kind)
		}
	}
	if _, ok := artifacts.OriginKindSlug(orkstackv1.RunOriginKind(99)); ok {
		t.Fatal("an undeclared number must have no slug")
	}
}

func TestRunOriginIndexKey_RoundTrip(t *testing.T) {
	stamp := day(t, "2026-08-14T09:30:00Z")
	values := runOriginKindDescriptor().Values()
	for i := 0; i < values.Len(); i++ {
		kind := orkstackv1.RunOriginKind(values.Get(i).Number())
		if kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
			continue
		}
		for _, id := range []string{"", "trg-4a1c0e2b7d3f8156", "none", "needs encoding/with a slash"} {
			rec := artifacts.RunRecord{
				RunID: "ork-1-1", Namespace: "payments", Orkflow: "deploy-service",
				Status: "STATUS_SUCCEEDED", Time: stamp, OriginKind: kind, OriginID: id,
			}
			key, ok, err := artifacts.RunOriginIndexKey(rec)
			if err != nil || !ok {
				t.Fatalf("RunOriginIndexKey(%v, %q) = %q, %v, %v", kind, id, key, ok, err)
			}
			if got := len(strings.Split(key, "/")); got != 10 {
				t.Fatalf("key %q has %d segments, want 10", key, got)
			}
			slug, _ := artifacts.OriginKindSlug(kind)
			if !strings.HasPrefix(key, "metadata/index/origins/payments/"+slug+"/") {
				t.Fatalf("key %q does not start with the namespace and kind", key)
			}
			if id == "" && strings.Split(key, "/")[5] != artifacts.OriginIndexNoID {
				t.Fatalf("an empty id renders %q, want %q", strings.Split(key, "/")[5], artifacts.OriginIndexNoID)
			}
			if id == "none" && strings.Split(key, "/")[5] == artifacts.OriginIndexNoID {
				t.Fatalf("an id spelled none must not render as the no-id segment: %q", key)
			}
			entry, ok := artifacts.ParseRunOriginIndexKey(key)
			if !ok {
				t.Fatalf("ParseRunOriginIndexKey(%q) refused its own key", key)
			}
			want := artifacts.RunIndexEntry{
				RunID: "ork-1-1", Namespace: "payments", Orkflow: "deploy-service",
				Status: "succeeded", Time: stamp, OriginKind: kind, OriginID: id,
			}
			if entry != want {
				t.Fatalf("parsed %+v, want %+v", entry, want)
			}
			if _, ok := artifacts.ParseRunIndexKey(key); ok {
				t.Fatalf("an origin key %q parsed as a run-index key", key)
			}
		}
	}
	runKey, err := artifacts.RunIndexKey(artifacts.RunRecord{RunID: "ork-1-1", Orkflow: "f", Status: "failed", Time: stamp})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := artifacts.ParseRunOriginIndexKey(runKey); ok {
		t.Fatalf("a run-index key %q parsed as an origin key", runKey)
	}
}

func TestRunOriginIndexKey_NoKindWritesNoRow(t *testing.T) {
	key, ok, err := artifacts.RunOriginIndexKey(artifacts.RunRecord{
		RunID: "ork-1-1", Orkflow: "f", Status: "failed", Time: day(t, "2026-08-14T00:00:00Z"), OriginID: "trg-x",
	})
	if err != nil || ok || key != "" {
		t.Fatalf("RunOriginIndexKey with no kind = %q, %v, %v; want no row", key, ok, err)
	}
}

func TestRunQuery_OriginScanPrefixAndMatches(t *testing.T) {
	since := day(t, "2026-08-14T00:00:00Z")
	trigger := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER
	tests := []struct {
		name           string
		query          artifacts.RunQuery
		wantPrefix     string
		wantStartAfter string
	}{
		{
			name:       "kind without a namespace cannot narrow past the family",
			query:      artifacts.RunQuery{OriginKind: trigger},
			wantPrefix: "metadata/index/origins/",
		},
		{
			name:       "kind only pins namespace and kind",
			query:      artifacts.RunQuery{Namespace: "payments", OriginKind: trigger, Orkflow: "deploy-service"},
			wantPrefix: "metadata/index/origins/payments/trigger/",
		},
		{
			name:       "kind and id pin one more",
			query:      artifacts.RunQuery{Namespace: "payments", OriginKind: trigger, OriginID: "trg-1"},
			wantPrefix: "metadata/index/origins/payments/trigger/trg-1/",
		},
		{
			name: "all five pinned starts at the lower bound",
			query: artifacts.RunQuery{
				Namespace: "payments", OriginKind: trigger, OriginID: "trg-1",
				Orkflow: "deploy-service", Status: "succeeded", Since: since,
			},
			wantPrefix: "metadata/index/origins/payments/trigger/trg-1/deploy-service/succeeded/",
			wantStartAfter: "metadata/index/origins/payments/trigger/trg-1/deploy-service/succeeded/" +
				"20260814T000000.000000000Z",
		},
		{
			name:       "no origin keeps the run index",
			query:      artifacts.RunQuery{Namespace: "payments"},
			wantPrefix: "metadata/index/runs/payments/",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prefix, startAfter, err := tc.query.ScanPrefix()
			if err != nil {
				t.Fatalf("ScanPrefix: %v", err)
			}
			if prefix != tc.wantPrefix || startAfter != tc.wantStartAfter {
				t.Fatalf("ScanPrefix = %q, %q; want %q, %q", prefix, startAfter, tc.wantPrefix, tc.wantStartAfter)
			}
		})
	}

	if _, _, err := (artifacts.RunQuery{OriginID: "trg-1"}).ScanPrefix(); !errors.Is(err, artifacts.ErrInvalidRunKey) {
		t.Fatalf("OriginID without OriginKind: err = %v, want ErrInvalidRunKey", err)
	}

	entry := artifacts.RunIndexEntry{
		RunID: "ork-1", Namespace: "payments", Orkflow: "deploy-service", Status: "succeeded",
		Time: since, OriginKind: trigger, OriginID: "trg-1",
	}
	matching := artifacts.RunQuery{Namespace: "payments", OriginKind: trigger, OriginID: "trg-1", Orkflow: "deploy-service", Status: "succeeded"}
	if !matching.Matches(entry) {
		t.Fatal("a fully matching query refused the entry")
	}
	for name, q := range map[string]artifacts.RunQuery{
		"other orkflow": {OriginKind: trigger, Orkflow: "other"},
		"other status":  {OriginKind: trigger, Status: "failed"},
		"other id":      {OriginKind: trigger, OriginID: "trg-2"},
		"other kind":    {OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT},
	} {
		if q.Matches(entry) {
			t.Fatalf("%s: query %+v matched %+v", name, q, entry)
		}
	}
}

// originFixture archives three runs from two triggers and one submit, plus a
// run with no recorded origin, into archive.
func originFixture(t *testing.T, archive artifacts.RunArchive) {
	t.Helper()
	trigger := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER
	submit := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT
	for _, rec := range []artifacts.RunRecord{
		{RunID: "ork-1", Orkflow: "deploy-service", Status: "succeeded", Time: day(t, "2026-08-14T01:00:00Z"), OriginKind: trigger, OriginID: "trg-a"},
		{RunID: "ork-2", Orkflow: "deploy-service", Status: "failed", Time: day(t, "2026-08-14T02:00:00Z"), OriginKind: trigger, OriginID: "trg-b"},
		{RunID: "ork-3", Orkflow: "deploy-service", Status: "succeeded", Time: day(t, "2026-08-14T03:00:00Z"), OriginKind: submit},
		{RunID: "ork-4", Orkflow: "deploy-service", Status: "succeeded", Time: day(t, "2026-08-14T04:00:00Z")},
	} {
		archiveRun(t, archive, rec)
	}
}

type indexScanner interface {
	artifacts.RunArchive
	IndexKeysScanned() int64
}

func assertListsByOrigin(t *testing.T, archive indexScanner) {
	t.Helper()
	ctx := context.Background()
	list := func(q artifacts.RunQuery) []string {
		t.Helper()
		entries, err := archive.ListRuns(ctx, q)
		if err != nil {
			t.Fatalf("ListRuns(%+v): %v", q, err)
		}
		return runIDs(entries)
	}
	trigger := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER
	submit := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT

	before := archive.IndexKeysScanned()
	if got := list(artifacts.RunQuery{Namespace: "default"}); !equalStrings(got, []string{"ork-4", "ork-3", "ork-2", "ork-1"}) {
		t.Fatalf("run index lists %v, want every run", got)
	}
	if scanned := archive.IndexKeysScanned() - before; scanned != 4 {
		t.Fatalf("a query with no origin filter scanned %d keys, want the run index's 4", scanned)
	}
	if got := list(artifacts.RunQuery{Namespace: "default", OriginKind: trigger}); !equalStrings(got, []string{"ork-2", "ork-1"}) {
		t.Fatalf("kind-only trigger lists %v, want [ork-2 ork-1]", got)
	}
	if got := list(artifacts.RunQuery{Namespace: "default", OriginKind: trigger, OriginID: "trg-a"}); !equalStrings(got, []string{"ork-1"}) {
		t.Fatalf("trigger:trg-a lists %v, want [ork-1]", got)
	}
	if got := list(artifacts.RunQuery{Namespace: "default", OriginKind: submit}); !equalStrings(got, []string{"ork-3"}) {
		t.Fatalf("submit lists %v, want [ork-3]", got)
	}
	if got := list(artifacts.RunQuery{OriginKind: trigger, Status: "failed"}); !equalStrings(got, []string{"ork-2"}) {
		t.Fatalf("trigger+failed lists %v, want [ork-2]", got)
	}
	entries, err := archive.ListRuns(ctx, artifacts.RunQuery{Namespace: "default", OriginKind: trigger, OriginID: "trg-a"})
	if err != nil || len(entries) != 1 || entries[0].OriginKind != trigger || entries[0].OriginID != "trg-a" {
		t.Fatalf("origin scan entry = %+v, %v; want kind and id carried", entries, err)
	}
	// ork-4 has no origin: it is in the run index and under no origin filter.
	values := runOriginKindDescriptor().Values()
	for i := 0; i < values.Len(); i++ {
		kind := orkstackv1.RunOriginKind(values.Get(i).Number())
		if kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
			continue
		}
		for _, id := range list(artifacts.RunQuery{OriginKind: kind}) {
			if id == "ork-4" {
				t.Fatalf("a run with no origin was listed under %v", kind)
			}
		}
	}
}

func TestLocalRunArchive_PutRunWritesOriginRowAndListsByOrigin(t *testing.T) {
	archive := newLocalArchive(t)
	originFixture(t, archive)
	want := filepath.Join(archive.Dir(), "metadata/index/origins/default/trigger/trg-a/deploy-service/succeeded/20260814T010000.000000000Z/ork-1")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("origin row missing at %s: %v", want, err)
	}
	none := filepath.Join(archive.Dir(), "metadata/index/origins/default/submit/none/deploy-service/succeeded/20260814T030000.000000000Z/ork-3")
	if _, err := os.Stat(none); err != nil {
		t.Fatalf("submit origin row missing at %s: %v", none, err)
	}
	assertListsByOrigin(t, archive)
}

func TestS3RunArchive_PutRunWritesOriginRowAndListsByOrigin(t *testing.T) {
	fake := newFakeS3(t)
	archive := fake.archive(t)
	originFixture(t, archive)
	if !fake.has("metadata/index/origins/default/trigger/trg-a/deploy-service/succeeded/20260814T010000.000000000Z/ork-1") {
		t.Fatalf("origin row missing; keys: %v", fake.keys())
	}
	assertListsByOrigin(t, archive)
}

// TestRunArchive_OriginRowBeforeIndexRow pins the write order on both
// backends: the origin row and the newest-first row are published before the
// run-index row, so the run index stays the last key PutRun writes.
func TestRunArchive_OriginRowBeforeIndexRow(t *testing.T) {
	rec := artifacts.RunRecord{
		RunID: "ork-9", Orkflow: "deploy-service", Status: "succeeded", Time: day(t, "2026-08-14T01:00:00Z"),
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, OriginID: "trg-a",
		ObjectIDs: []string{"objects/sha256/aa/bb"},
		JSONL:     []byte("{}\n"),
	}
	originKey, _, err := artifacts.RunOriginIndexKey(rec)
	if err != nil {
		t.Fatal(err)
	}
	indexKey, err := artifacts.RunIndexKey(rec)
	if err != nil {
		t.Fatal(err)
	}
	recentKey, err := artifacts.RunRecentIndexKey(rec)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("s3", func(t *testing.T) {
		fake := newFakeS3(t)
		if err := fake.archive(t).PutRun(context.Background(), rec); err != nil {
			t.Fatalf("PutRun: %v", err)
		}
		order := fake.putOrder()
		if len(order) == 0 || order[len(order)-1] != indexKey {
			t.Fatalf("the run-index row must be the last put; order = %v", order)
		}
		for _, key := range []string{originKey, recentKey} {
			if !slices.Contains(order[:len(order)-1], key) {
				t.Fatalf("row %q must be put before the run-index row; order = %v", key, order)
			}
		}
	})

	t.Run("local", func(t *testing.T) {
		archive := newLocalArchive(t)
		// Occupy the run-index key's parent path with a regular file so the
		// run-index write cannot succeed: whatever PutRun wrote before it is
		// on disk, whatever it would have written after is not.
		blocker := filepath.Join(archive.Dir(), filepath.Dir(filepath.Dir(indexKey)))
		if err := os.MkdirAll(filepath.Dir(blocker), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(blocker, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := archive.PutRun(context.Background(), rec); err == nil {
			t.Fatal("PutRun succeeded with the run-index path blocked")
		}
		if _, err := os.Stat(filepath.Join(archive.Dir(), originKey)); err != nil {
			t.Fatalf("the origin row was not written before the run-index row: %v", err)
		}
		if _, err := os.Stat(filepath.Join(archive.Dir(), recentKey)); err != nil {
			t.Fatalf("the newest-first row was not written before the run-index row: %v", err)
		}
	})
}

// fakeS3 is the smallest S3-compatible server the run archive needs:
// HeadObject, PutObject, GetObject and ListObjectsV2 over path-style URLs,
// recording the order in which keys were put.
type fakeS3 struct {
	mu      sync.Mutex
	server  *httptest.Server
	objects map[string][]byte
	order   []string
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{objects: map[string][]byte{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeS3) archive(t *testing.T) *artifacts.S3RunArchive {
	t.Helper()
	archive, err := artifacts.NewS3RunArchive(context.Background(), artifacts.Config{
		S3Endpoint: f.server.URL, S3Region: artifacts.DefaultS3Region, S3Bucket: "fake",
		S3AccessKey: "fake", S3SecretKey: "fake-secret", S3PathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	return archive
}

func (f *fakeS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.objects))
	for key := range f.objects {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (f *fakeS3) putOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

type fakeListResult struct {
	XMLName     xml.Name          `xml:"ListBucketResult"`
	Name        string            `xml:"Name"`
	Prefix      string            `xml:"Prefix"`
	KeyCount    int               `xml:"KeyCount"`
	MaxKeys     int               `xml:"MaxKeys"`
	IsTruncated bool              `xml:"IsTruncated"`
	Contents    []fakeListContent `xml:"Contents"`
}

type fakeListContent struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}

func (f *fakeS3) serve(w http.ResponseWriter, r *http.Request) {
	// Path style: /{bucket}/{key...}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2":
		prefix := r.URL.Query().Get("prefix")
		startAfter := r.URL.Query().Get("start-after")
		result := fakeListResult{Name: parts[0], Prefix: prefix, MaxKeys: 1000}
		keys := make([]string, 0, len(f.objects))
		for k := range f.objects {
			if strings.HasPrefix(k, prefix) && k > startAfter {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			result.Contents = append(result.Contents, fakeListContent{Key: k, Size: int64(len(f.objects[k]))})
		}
		result.KeyCount = len(result.Contents)
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.objects[key] = body
		f.order = append(f.order, key)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodHead:
		if _, ok := f.objects[key]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet:
		body, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		_, _ = w.Write(body)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}
