package dist_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
)

// fakeBuilder stands in for `go build`: it writes a binary whose bytes name
// the target and the version it was asked for, and records every call.
type fakeBuilder struct {
	mu    sync.Mutex
	calls []string
	fail  string
}

func (f *fakeBuilder) build(_ context.Context, root string, target dist.Target, version, out string) error {
	f.mu.Lock()
	f.calls = append(f.calls, target.String()+" "+version+" in "+root)
	f.mu.Unlock()
	if f.fail == target.String() {
		return errors.New("compile error: boom")
	}
	return os.WriteFile(out, []byte("binary:"+target.String()+":"+version), 0o755)
}

// fakeRoot is a repository root as `make dist` finds it: a LICENSE, and the
// web UI build `make ui` wrote for ork to embed.
func fakeRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "LICENSE"), fakeLicense, 0o644); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(root, filepath.FromSlash(dist.UIIndex))
	if err := os.MkdirAll(filepath.Dir(index), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(index, []byte("<!doctype html>\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func buildInto(t *testing.T, root, out string, fake *fakeBuilder) []string {
	t.Helper()
	assets, err := dist.Build(context.Background(), dist.BuildOptions{
		Root: root, Out: out, Version: mustVersion(t, "v0.1.0"), CommitTime: commitTime, Builder: fake.build,
	})
	if err != nil {
		t.Fatal(err)
	}
	return assets
}

// TestBuild_WritesEveryAssetAndTheImageBinaries: one archive per target under
// its versionless name; every linux binary also at bin/linux-<arch>/<name> for
// the images to copy; nothing else.
func TestBuild_WritesEveryAssetAndTheImageBinaries(t *testing.T) {
	t.Parallel()
	root, out := fakeRoot(t), filepath.Join(t.TempDir(), "dist")
	fake := &fakeBuilder{}
	assets := buildInto(t, root, out, fake)

	wantAssets := []string{
		"ork-linux-amd64.tar.gz", "ork-linux-arm64.tar.gz",
		"orker-linux-amd64.tar.gz", "orker-linux-arm64.tar.gz",
		"orkctl-darwin-amd64.tar.gz", "orkctl-darwin-arm64.tar.gz",
		"orkctl-linux-amd64.tar.gz", "orkctl-linux-arm64.tar.gz",
		"orkctl-windows-amd64.zip", "orkctl-windows-arm64.zip",
	}
	slices.Sort(wantAssets)
	if !reflect.DeepEqual(assets, wantAssets) {
		t.Fatalf("Build returned %q, want %q", assets, wantAssets)
	}
	var onDisk, binaries []string
	err := filepath.WalkDir(out, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(out, path)
		if strings.HasPrefix(rel, "bin"+string(filepath.Separator)) {
			binaries = append(binaries, filepath.ToSlash(rel))
		} else {
			onDisk = append(onDisk, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(onDisk)
	slices.Sort(binaries)
	if !reflect.DeepEqual(onDisk, wantAssets) {
		t.Errorf("release directory holds %q, want exactly %q", onDisk, wantAssets)
	}
	wantBinaries := []string{
		"bin/linux-amd64/ork", "bin/linux-amd64/orker", "bin/linux-amd64/orkctl",
		"bin/linux-arm64/ork", "bin/linux-arm64/orker", "bin/linux-arm64/orkctl",
	}
	slices.Sort(wantBinaries)
	if !reflect.DeepEqual(binaries, wantBinaries) {
		t.Errorf("image binaries %q, want %q", binaries, wantBinaries)
	}
	for _, rel := range wantBinaries {
		info, err := os.Stat(filepath.Join(out, rel))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o755 {
			t.Errorf("%s mode %v, want 0755", rel, info.Mode().Perm())
		}
	}
	body, err := os.ReadFile(filepath.Join(out, "bin", "linux-arm64", "orker"))
	if err != nil || string(body) != "binary:orker-linux-arm64:v0.1.0" {
		t.Errorf("bin/linux-arm64/orker = %q (%v), want the arm64 orker built with the version", body, err)
	}
	if len(fake.calls) != len(dist.Targets()) {
		t.Errorf("built %d times, want once per target (%d): %q", len(fake.calls), len(dist.Targets()), fake.calls)
	}
	for _, call := range fake.calls {
		if !strings.Contains(call, " v0.1.0 in "+root) {
			t.Errorf("build %q was not stamped with the version from the root", call)
		}
	}
}

// TestBuild_ArchivesHoldTheBinaryAndTheLicense: what a user unpacks.
func TestBuild_ArchivesHoldTheBinaryAndTheLicense(t *testing.T) {
	t.Parallel()
	root, out := fakeRoot(t), filepath.Join(t.TempDir(), "dist")
	buildInto(t, root, out, &fakeBuilder{})

	data, err := os.ReadFile(filepath.Join(out, "orker-linux-amd64.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	headers, contents := readTarGz(t, data)
	if len(headers) != 2 || headers[0].Name != "LICENSE" || headers[1].Name != "orker" {
		t.Fatalf("orker archive holds %v, want LICENSE and orker", headers)
	}
	if string(contents["orker"]) != "binary:orker-linux-amd64:v0.1.0" || !bytes.Equal(contents["LICENSE"], fakeLicense) {
		t.Errorf("orker archive contents: %q", contents)
	}
	for _, header := range headers {
		if !header.ModTime.Equal(commitTime) {
			t.Errorf("%s mtime %v, want the commit time", header.Name, header.ModTime)
		}
	}

	zipData, err := os.ReadFile(filepath.Join(out, "orkctl-windows-amd64.zip"))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, file := range reader.File {
		names = append(names, file.Name)
		if file.Name == "orkctl.exe" {
			open, err := file.Open()
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(open)
			_ = open.Close()
			if string(body) != "binary:orkctl-windows-amd64:v0.1.0" {
				t.Errorf("orkctl.exe = %q", body)
			}
		}
	}
	if !reflect.DeepEqual(names, []string{"LICENSE", "orkctl.exe"}) {
		t.Errorf("windows archive holds %q", names)
	}
}

// TestBuild_IsRepeatable: two builds of one commit, into two directories, give
// the same bytes for every file.
func TestBuild_IsRepeatable(t *testing.T) {
	t.Parallel()
	root := fakeRoot(t)
	first, second := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")
	assets := buildInto(t, root, first, &fakeBuilder{})
	buildInto(t, root, second, &fakeBuilder{})
	for _, name := range assets {
		a, errA := os.ReadFile(filepath.Join(first, name))
		b, errB := os.ReadFile(filepath.Join(second, name))
		if errA != nil || errB != nil || !bytes.Equal(a, b) {
			t.Errorf("%s differs between two builds (%v, %v)", name, errA, errB)
		}
	}
}

// TestBuild_Refuses: a missing LICENSE, a missing web UI build, a missing
// commit time or a failed compile stops the build with a reason that names
// the cause, and a failed build leaves no archive for the target that failed.
func TestBuild_Refuses(t *testing.T) {
	t.Parallel()
	t.Run("no web UI build", func(t *testing.T) {
		t.Parallel()
		// A root as a fresh clone leaves it: the UI's directory holds only
		// its .gitkeep, so ork would embed nothing and serve the placeholder.
		without := fakeRoot(t)
		index := filepath.Join(without, filepath.FromSlash(dist.UIIndex))
		if err := os.Remove(index); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(filepath.Dir(index), ".gitkeep"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		// And one whose index.html is a directory, not the page.
		directory := fakeRoot(t)
		index = filepath.Join(directory, filepath.FromSlash(dist.UIIndex))
		if err := os.Remove(index); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(index, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, root := range map[string]string{"no index.html": without, "index.html is a directory": directory} {
			fake := &fakeBuilder{}
			out := filepath.Join(t.TempDir(), "dist")
			_, err := dist.Build(context.Background(), dist.BuildOptions{
				Root: root, Out: out, Version: mustVersion(t, "v0.1.0"), CommitTime: commitTime, Builder: fake.build,
			})
			if err == nil || !strings.Contains(err.Error(), dist.UIIndex) || !strings.Contains(err.Error(), "make ui") {
				t.Errorf("%s: error %v, want one naming %s and make ui", name, err, dist.UIIndex)
			}
			if len(fake.calls) != 0 {
				t.Errorf("%s: built %q before noticing the UI was missing", name, fake.calls)
			}
			if _, statErr := os.Stat(out); statErr == nil {
				t.Errorf("%s: the release directory was created for a build that could not ship the UI", name)
			}
		}
		if err := dist.CheckUI(fakeRoot(t)); err != nil {
			t.Errorf("CheckUI refuses a root with the UI build: %v", err)
		}
	})
	t.Run("no LICENSE", func(t *testing.T) {
		t.Parallel()
		fake := &fakeBuilder{}
		_, err := dist.Build(context.Background(), dist.BuildOptions{
			Root: t.TempDir(), Out: filepath.Join(t.TempDir(), "dist"), Version: mustVersion(t, "v0.1.0"),
			CommitTime: commitTime, Builder: fake.build,
		})
		if err == nil || !strings.Contains(err.Error(), "LICENSE") {
			t.Fatalf("error %v, want one naming LICENSE", err)
		}
		if len(fake.calls) != 0 {
			t.Errorf("built %q before noticing the LICENSE was missing", fake.calls)
		}
	})
	t.Run("no commit time", func(t *testing.T) {
		t.Parallel()
		_, err := dist.Build(context.Background(), dist.BuildOptions{
			Root: fakeRoot(t), Out: filepath.Join(t.TempDir(), "dist"), Version: mustVersion(t, "v0.1.0"),
			Builder: (&fakeBuilder{}).build,
		})
		if err == nil || !strings.Contains(err.Error(), "commit time") {
			t.Fatalf("error %v, want one naming the commit time", err)
		}
	})
	t.Run("a target fails to compile", func(t *testing.T) {
		t.Parallel()
		out := filepath.Join(t.TempDir(), "dist")
		_, err := dist.Build(context.Background(), dist.BuildOptions{
			Root: fakeRoot(t), Out: out, Version: mustVersion(t, "v0.1.0"), CommitTime: commitTime,
			Builder: (&fakeBuilder{fail: "orkctl-windows-arm64"}).build,
		})
		if err == nil || !strings.Contains(err.Error(), "orkctl-windows-arm64") || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("error %v, want one naming the target and the compiler's message", err)
		}
		if _, statErr := os.Stat(filepath.Join(out, "orkctl-windows-arm64.zip")); statErr == nil {
			t.Error("an archive was written for the target that failed")
		}
	})
	t.Run("a canceled context", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := dist.Build(ctx, dist.BuildOptions{
			Root: fakeRoot(t), Out: filepath.Join(t.TempDir(), "dist"), Version: mustVersion(t, "v0.1.0"),
			CommitTime: commitTime, Builder: (&fakeBuilder{}).build,
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error %v, want context.Canceled", err)
		}
	})
}

// TestParseCommitTime: seconds since the epoch, as SOURCE_DATE_EPOCH and
// `git log -1 --format=%ct` give them, or RFC 3339, and never before 1980: a
// zip's date cannot say an earlier year, and "0" is what a failed `git log`
// fallback once produced.
func TestParseCommitTime(t *testing.T) {
	t.Parallel()
	good := map[string]time.Time{
		"1761298215":                time.Unix(1761298215, 0).UTC(),
		"315532800":                 time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
		"2026-10-24T09:30:15Z":      time.Date(2026, 10, 24, 9, 30, 15, 0, time.UTC),
		"2026-10-24T11:30:15+02:00": time.Date(2026, 10, 24, 9, 30, 15, 0, time.UTC),
	}
	for in, want := range good {
		got, err := dist.ParseCommitTime(in)
		if err != nil || !got.Equal(want) || got.Location() != time.UTC {
			t.Errorf("ParseCommitTime(%q) = %v, %v; want %v in UTC", in, got, err, want)
		}
	}
	for _, in := range []string{"", " 1761298215", "1761298215\n", "-1", "12.5", "yesterday", "2026-10-24", "99999999999999999999", "0", "315532799", "1979-12-31T23:59:59Z"} {
		if got, err := dist.ParseCommitTime(in); err == nil {
			t.Errorf("ParseCommitTime(%q) = %v, want an error", in, got)
		}
	}
}
