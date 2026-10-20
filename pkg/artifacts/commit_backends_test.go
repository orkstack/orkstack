package artifacts

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func localCommitURI(t *testing.T, root, run string) string {
	t.Helper()
	cfg := Config{Backend: BackendLocal, LocalDir: root}
	prefix, err := CommitRoot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	key, err := CommitKey("default", run, "build")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(prefix, "/") + "/" + key
}

func TestLocalCommitStorePublishesWithLinkAndRefusesExisting(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	uri := localCommitURI(t, root, "run-arbitrary")
	winner := sampleCommit()
	winner.RunId = "run-arbitrary"
	first, err := store.Create(context.Background(), uri, winner)
	if err != nil {
		t.Fatal(err)
	}
	loser := sampleCommit()
	loser.RunId = "different"
	if _, err := store.Create(context.Background(), uri, loser); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("second Create = %v, want ErrPreconditionFailed", err)
	}
	got, err := store.Get(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	if got.SHA256 != first.SHA256 || got.Commit.GetRunId() != winner.GetRunId() {
		t.Fatalf("winner changed: %#v", got)
	}
}

func TestLocalCommitStoreInterruptedTempIsNeverPublished(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, localTempDir, "commit-crashed"), []byte("torn"), 0o644); err != nil {
		t.Fatal(err)
	}
	uri := localCommitURI(t, root, "run-after-crash")
	if _, err := store.Get(context.Background(), uri); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	commit := sampleCommit()
	commit.RunId = "run-after-crash"
	if _, err := store.Create(context.Background(), uri, commit); err != nil {
		t.Fatalf("replacement Create: %v", err)
	}
}

func TestLocalRefForExactlyMatchesPut(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	body := []byte("immutable")
	digest := sha256.Sum256(body)
	sum := hex.EncodeToString(digest[:])
	prov := Provenance{Orkflow: "flow", RunID: "run", TaskID: "task"}
	want := store.RefFor(prov, "result.txt", sum, int64(len(body)), "text/plain")
	got, err := store.Put(context.Background(), "result.txt", strings.NewReader(string(body)), PutOptions{ContentType: "text/plain", Metadata: prov.Metadata(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if got.URI != want.URI || got.SHA256 != want.SHA256 {
		t.Fatalf("Put ref %#v != prediction %#v", got, want)
	}
}

func TestLocalCommitStoreKillBetweenTempAndLinkPublishesNothing(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestLocalCommitCrashHelper")
	cmd.Env = append(os.Environ(), "ORK_COMMIT_CRASH_HELPER=1", "ORK_COMMIT_CRASH_ROOT="+root)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "READY" {
		t.Fatalf("helper readiness: %q (%v)", scanner.Text(), scanner.Err())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	uri := localCommitURI(t, root, "run-killed")
	if _, err := store.Get(context.Background(), uri); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canonical commit after SIGKILL = %v, want absent", err)
	}
	commit := sampleCommit()
	commit.RunId = "run-killed"
	if _, err := store.Create(context.Background(), uri, commit); err != nil {
		t.Fatalf("replacement publication: %v", err)
	}
}

func TestLocalCommitCrashHelper(t *testing.T) {
	if os.Getenv("ORK_COMMIT_CRASH_HELPER") != "1" {
		return
	}
	root := os.Getenv("ORK_COMMIT_CRASH_ROOT")
	if err := os.MkdirAll(filepath.Join(root, localTempDir), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(filepath.Join(root, localTempDir), "commit-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("complete temporary commit bytes")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fmt.Println("READY")
	select {}
}

// TestS3CommitStore_WireContractOverTheDouble pins what the SDK puts on the
// wire for a commit: a path-style conditional PutObject, If-None-Match: *, the
// documented content type, and exactly one object at the commit key whatever
// the refused writers did.
func TestS3CommitStore_WireContractOverTheDouble(t *testing.T) {
	d := newS3Double(t)
	store, err := NewS3CommitStore(context.Background(), d.config())
	if err != nil {
		t.Fatal(err)
	}
	key, err := CommitKey("default", "run-wire", "build")
	if err != nil {
		t.Fatal(err)
	}
	uri := s3URI(d.bucket, key)
	winner := sampleCommit()
	winner.RunId = "run-wire"
	winner.TaskId = "build"
	if _, err := store.Create(context.Background(), uri, winner); err != nil {
		t.Fatalf("Create: %v", err)
	}
	headers := d.headers[key]
	if got := headers.Get("If-None-Match"); got != "*" {
		t.Fatalf("If-None-Match = %q, want *", got)
	}
	if got := headers.Get("Content-Type"); got != CommitContentType {
		t.Fatalf("Content-Type = %q, want %q", got, CommitContentType)
	}
	body, ok := d.object(key)
	if !ok {
		t.Fatalf("no object at %s", key)
	}
	want, err := MarshalCommit(winner)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != string(want) {
		t.Fatal("the stored bytes are not the bytes MarshalCommit produced")
	}
	loser := sampleCommit()
	loser.RunId = "run-other"
	if _, err := store.Create(context.Background(), uri, loser); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("second Create = %v, want ErrPreconditionFailed", err)
	}
	again, _ := d.object(key)
	if string(again) != string(want) {
		t.Fatal("the refused writer replaced the stored bytes")
	}
}

func s3URI(bucket, key string) string {
	return (&url.URL{Scheme: "s3", Host: bucket, Path: "/" + key}).String()
}
