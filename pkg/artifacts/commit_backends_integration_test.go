//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestS3CommitStoreConditionalTwoWriterProbe(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3CommitStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	raw := newRawS3Client(t, cfg)
	commitsBefore := listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.CommitPrefix+"/")
	probeKeysBefore := listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.CommitProbePrefix+"/")
	t.Logf("probe prefix before: %v", probeKeysBefore)
	var probeKeysAfterFirst []string
	for i := 0; i < 3; i++ {
		if err := artifacts.ProbeCommitStore(ctx, store); err != nil {
			t.Fatalf("two-writer probe %d: %v", i+1, err)
		}
		got := listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.CommitProbePrefix+"/")
		t.Logf("probe prefix after call %d: %v", i+1, got)
		if !slices.Contains(got, artifacts.CommitProbeKey) {
			t.Fatalf("probe objects after call %d = %v, missing current key %s", i+1, got, artifacts.CommitProbeKey)
		}
		if i == 0 {
			probeKeysAfterFirst = slices.Clone(got)
		} else if !slices.Equal(got, probeKeysAfterFirst) {
			t.Fatalf("probe prefix grew after call %d: first %v now %v", i+1, probeKeysAfterFirst, got)
		}
	}
	commitsAfter := listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.CommitPrefix+"/")
	if !slices.Equal(commitsAfter, commitsBefore) {
		t.Fatalf("completion prefix changed during probe: before %v after %v", commitsBefore, commitsAfter)
	}
}

func TestS3CommitStoreExactlyOneWriterAndStoredBytesWin(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	store, err := artifacts.NewS3CommitStore(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := artifacts.CommitRoot(cfg)
	key, _ := artifacts.CommitKey("default", "run-"+uniqueToken(t), "build")
	uri := strings.TrimSuffix(root, "/") + "/" + key
	commits := []*orkstackv1.ActivityCommit{{SchemaVersion: 1, Namespace: "default", RunId: "winner-a", TaskId: "build"}, {SchemaVersion: 1, Namespace: "default", RunId: "winner-b", TaskId: "build"}}
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = store.Create(context.Background(), uri, commits[i])
		}(i)
	}
	close(start)
	wg.Wait()
	wins, loses := 0, 0
	for _, err := range errs {
		if err == nil {
			wins++
		} else if errors.Is(err, artifacts.ErrPreconditionFailed) {
			loses++
		} else {
			t.Fatalf("writer: %v", err)
		}
	}
	if wins != 1 || loses != 1 {
		t.Fatalf("writers = %d wins %d conflicts", wins, loses)
	}
	record, err := store.Get(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	if record.SHA256 == "" || record.ETag == "" || record.Bytes == 0 {
		t.Fatalf("stored byte facts missing: %#v", record)
	}
}

func TestS3PutUsesContentDigestAsSixthSegmentAndMatchesPrediction(t *testing.T) {
	generic, cfg := newS3Store(t)
	store := generic.(*artifacts.S3Store)
	body := "one immutable value"
	prov := artifacts.Provenance{Orkflow: "flow", RunID: "run-" + uniqueToken(t), TaskID: "build"}
	got, err := store.Put(context.Background(), "result.txt", strings.NewReader(body), artifacts.PutOptions{ContentType: "text/plain", Metadata: prov.Metadata(nil)})
	if err != nil {
		t.Fatal(err)
	}
	want := store.RefFor(prov, "result.txt", got.SHA256, got.SizeBytes, "text/plain")
	if got.URI != want.URI {
		t.Fatalf("Put URI %q != predicted %q", got.URI, want.URI)
	}
	if !strings.HasSuffix(got.URI, "/"+got.SHA256) {
		t.Fatalf("URI %q does not end in digest", got.URI)
	}
	other, err := store.Put(context.Background(), "result.txt", strings.NewReader("different bytes"), artifacts.PutOptions{ContentType: "text/plain", Metadata: prov.Metadata(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if other.URI == got.URI {
		t.Fatalf("different bytes overwrote %q", got.URI)
	}
	first, err := store.Get(context.Background(), got.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	data, err := io.ReadAll(first)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != body {
		t.Fatalf("first immutable object changed to %q", data)
	}
	_ = cfg
}
