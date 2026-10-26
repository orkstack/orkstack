package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
)

// result is what one invocation did.
type result struct {
	code           int
	stdout, stderr string
}

// invoke runs the tool with the given environment variables and builder.
func invoke(t *testing.T, env map[string]string, builder dist.Builder, args ...string) result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, runEnv{
		stdout: &stdout, stderr: &stderr,
		getenv:  func(key string) string { return env[key] },
		builder: builder,
	})
	return result{code, stdout.String(), stderr.String()}
}

// currentSeries is the MAJOR.MINOR the built-in catalog is at, so these tests
// follow the catalog when it moves.
func currentSeries(t *testing.T) string {
	t.Helper()
	tag, err := dist.BuiltinCatalogTag()
	if err != nil {
		t.Fatal(err)
	}
	return tag
}

// repoWith writes a repository root holding a LICENSE and a CHANGELOG.md.
func repoWith(t *testing.T, changelog string) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{"LICENSE": "a licence\n", "CHANGELOG.md": changelog} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func changelogFor(version string) string {
	return "# Changelog\n\n## [Unreleased]\n\n- later\n\n## [" + version + "] - 2026-10-24\n\n- What this release did.\n"
}

func TestRun_UsageErrorsExitTwo(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		nil,
		{"frobnicate"},
		{"check"},
		{"check", "-version", "1.2.3"},
		{"check", "-version", "v1.2"},
		{"check", "-nonsense"},
		{"notes"},
		{"build", "-version", "v0.1.0"},
		{"build", "-out", "x"},
		{"sums"},
		{"sums", "a", "b"},
		{"digests"},
		{"digests", "-layouts"},
		{"digests", "-layouts", "x", "extra"},
		{"digests", "-nonsense"},
		{"quickstart"},
		{"quickstart", "-version", "v0.1.0", "-images", "x", "-registry", "ghcr.io", "-tag", "0.1.0"},
		{"quickstart", "-nonsense"},
	} {
		got := invoke(t, nil, nil, args...)
		if got.code != 2 {
			t.Errorf("dist %q exited %d, want 2 (stderr %q)", args, got.code, got.stderr)
		}
		if got.stdout != "" {
			t.Errorf("dist %q wrote to stdout: %q", args, got.stdout)
		}
		if got.stderr == "" {
			t.Errorf("dist %q said nothing on stderr", args)
		}
	}
	usage := invoke(t, nil, nil)
	for _, sub := range []string{"check", "build", "sums", "notes", "digests", "quickstart"} {
		if !strings.Contains(usage.stderr, sub) {
			t.Errorf("usage does not list %s: %q", sub, usage.stderr)
		}
	}
}

func TestRun_HelpExitsZero(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"-h"}, {"help"}, {"check", "-h"}} {
		if got := invoke(t, nil, nil, args...); got.code != 0 {
			t.Errorf("dist %q exited %d, want 0", args, got.code)
		}
	}
}

func TestRun_Check(t *testing.T) {
	t.Parallel()
	series := currentSeries(t)
	version := "v" + series + ".4"
	t.Run("a release the catalog and the changelog both accept", func(t *testing.T) {
		t.Parallel()
		root := repoWith(t, changelogFor(series+".4"))
		got := invoke(t, nil, nil, "check", "-version", version, "-root", root)
		if got.code != 0 {
			t.Fatalf("exit %d: %s", got.code, got.stderr)
		}
		if !strings.Contains(got.stdout, version) {
			t.Errorf("stdout %q does not name the version", got.stdout)
		}
	})
	t.Run("a series the catalog is not at", func(t *testing.T) {
		t.Parallel()
		root := repoWith(t, changelogFor("9.9.9"))
		got := invoke(t, nil, nil, "check", "-version", "v9.9.9", "-root", root)
		if got.code != 1 {
			t.Fatalf("exit %d, want 1", got.code)
		}
		if !strings.Contains(got.stderr, "internal/dsl/activity_catalog.go") {
			t.Errorf("stderr %q does not name the catalog file", got.stderr)
		}
	})
	t.Run("no changelog section", func(t *testing.T) {
		t.Parallel()
		root := repoWith(t, changelogFor(series+".3"))
		got := invoke(t, nil, nil, "check", "-version", version, "-root", root)
		if got.code != 1 || !strings.Contains(got.stderr, "CHANGELOG.md") {
			t.Fatalf("exit %d, stderr %q; want 1 naming CHANGELOG.md", got.code, got.stderr)
		}
	})
	t.Run("no changelog file", func(t *testing.T) {
		t.Parallel()
		got := invoke(t, nil, nil, "check", "-version", version, "-root", t.TempDir())
		if got.code != 1 || !strings.Contains(got.stderr, "CHANGELOG.md") {
			t.Fatalf("exit %d, stderr %q; want 1 naming CHANGELOG.md", got.code, got.stderr)
		}
	})
}

func TestRun_Notes(t *testing.T) {
	t.Parallel()
	root := repoWith(t, changelogFor("1.2.3"))
	got := invoke(t, nil, nil, "notes", "-version", "v1.2.3", "-root", root)
	if got.code != 0 || got.stdout != "- What this release did.\n" {
		t.Fatalf("exit %d, stdout %q (stderr %q), want the section", got.code, got.stdout, got.stderr)
	}
	missing := invoke(t, nil, nil, "notes", "-version", "v1.2.4", "-root", root)
	if missing.code != 1 || missing.stdout != "" || !strings.Contains(missing.stderr, "[1.2.4]") {
		t.Fatalf("a missing section: exit %d, stdout %q, stderr %q; want 1, nothing on stdout, the version named",
			missing.code, missing.stdout, missing.stderr)
	}
}

func TestRun_Sums(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x.tar.gz"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := invoke(t, nil, nil, "sums", dir)
	if got.code != 0 {
		t.Fatalf("exit %d: %s", got.code, got.stderr)
	}
	written, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil || !strings.HasSuffix(string(written), "  x.tar.gz\n") {
		t.Fatalf("SHA256SUMS = %q (%v)", written, err)
	}
	if !strings.Contains(got.stdout, "x.tar.gz") {
		t.Errorf("stdout %q does not list the file summed", got.stdout)
	}
	empty := invoke(t, nil, nil, "sums", t.TempDir())
	if empty.code != 1 {
		t.Errorf("an empty directory exited %d, want 1", empty.code)
	}
}

func fakeBuild(_ context.Context, _ string, target dist.Target, version, out string) error {
	return os.WriteFile(out, []byte(target.String()+" "+version), 0o755)
}

// archivedTime is the mtime of the first entry of a .tar.gz.
func archivedTime(t *testing.T, path string) time.Time {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	header, err := tar.NewReader(gz).Next()
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return header.ModTime
}

func TestRun_Build(t *testing.T) {
	t.Parallel()
	t.Run("stamps archives with the flag's commit time", func(t *testing.T) {
		t.Parallel()
		root, out := repoWith(t, ""), filepath.Join(t.TempDir(), "dist")
		got := invoke(t, map[string]string{"SOURCE_DATE_EPOCH": "1"}, fakeBuild,
			"build", "-version", "v0.1.0", "-out", out, "-root", root, "-commit-time", "1761298215")
		if got.code != 0 {
			t.Fatalf("exit %d: %s", got.code, got.stderr)
		}
		if mtime := archivedTime(t, filepath.Join(out, "ork-linux-amd64.tar.gz")); mtime.Unix() != 1761298215 {
			t.Errorf("archive mtime %v, want the -commit-time flag's, not SOURCE_DATE_EPOCH's", mtime)
		}
		for _, name := range []string{"orkctl-windows-arm64.zip", "bin/linux-arm64/orkctl"} {
			if _, err := os.Stat(filepath.Join(out, name)); err != nil {
				t.Errorf("build wrote no %s: %v", name, err)
			}
		}
		if !strings.Contains(got.stdout, "orkctl-darwin-arm64.tar.gz") {
			t.Errorf("stdout %q does not list the assets", got.stdout)
		}
	})
	t.Run("falls back to SOURCE_DATE_EPOCH", func(t *testing.T) {
		t.Parallel()
		root, out := repoWith(t, ""), filepath.Join(t.TempDir(), "dist")
		got := invoke(t, map[string]string{"SOURCE_DATE_EPOCH": "1700000000"}, fakeBuild,
			"build", "-version", "v0.1.0", "-out", out, "-root", root)
		if got.code != 0 {
			t.Fatalf("exit %d: %s", got.code, got.stderr)
		}
		if mtime := archivedTime(t, filepath.Join(out, "orker-linux-arm64.tar.gz")); mtime.Unix() != 1700000000 {
			t.Errorf("archive mtime %v, want SOURCE_DATE_EPOCH's", mtime)
		}
	})
	t.Run("refuses to guess a commit time", func(t *testing.T) {
		t.Parallel()
		root, out := repoWith(t, ""), filepath.Join(t.TempDir(), "dist")
		for _, env := range []map[string]string{nil, {"SOURCE_DATE_EPOCH": "soon"}} {
			got := invoke(t, env, fakeBuild, "build", "-version", "v0.1.0", "-out", out, "-root", root)
			if got.code != 2 || !strings.Contains(got.stderr, "SOURCE_DATE_EPOCH") {
				t.Errorf("env %v: exit %d, stderr %q; want 2 naming SOURCE_DATE_EPOCH", env, got.code, got.stderr)
			}
		}
		if _, err := os.Stat(out); err == nil {
			t.Error("the release directory was created before the commit time was known")
		}
	})
	t.Run("a failing compile is exit 1", func(t *testing.T) {
		t.Parallel()
		root, out := repoWith(t, ""), filepath.Join(t.TempDir(), "dist")
		failing := func(context.Context, string, dist.Target, string, string) error { return io.ErrUnexpectedEOF }
		got := invoke(t, map[string]string{"SOURCE_DATE_EPOCH": "1700000000"}, failing,
			"build", "-version", "v0.1.0", "-out", out, "-root", root)
		if got.code != 1 {
			t.Errorf("exit %d, want 1", got.code)
		}
	})
}

// digestOf is a well-formed digest that differs for every name.
func digestOf(name string) string {
	sum := sha256.Sum256([]byte(name))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// layoutsDir writes an OCI layout for each name, each index naming digestOf.
func layoutsDir(t *testing.T, names []string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range names {
		layout := filepath.Join(dir, name)
		if err := os.MkdirAll(layout, 0o755); err != nil {
			t.Fatal(err)
		}
		index := fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.index.v1+json","digest":%q,"size":1}]}`, digestOf(name))
		if err := os.WriteFile(filepath.Join(layout, "index.json"), []byte(index), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestRun_Digests(t *testing.T) {
	t.Parallel()
	t.Run("prints name and digest, sorted, and nothing else", func(t *testing.T) {
		t.Parallel()
		names := dist.Images()
		for i, j := 0, len(names)-1; i < j; i, j = i+1, j-1 {
			names[i], names[j] = names[j], names[i]
		}
		got := invoke(t, nil, nil, "digests", "-layouts", layoutsDir(t, names))
		if got.code != 0 || got.stderr != "" {
			t.Fatalf("exit %d, stderr %q", got.code, got.stderr)
		}
		lines := strings.Split(strings.TrimSuffix(got.stdout, "\n"), "\n")
		if len(lines) != 10 {
			t.Fatalf("%d lines:\n%s", len(lines), got.stdout)
		}
		for i, name := range dist.Images() {
			if want := name + "\t" + digestOf(name); lines[i] != want {
				t.Errorf("line %d = %q, want %q", i, lines[i], want)
			}
		}
		// The output is exactly what `quickstart -images` reads.
		parsed, err := dist.ParseImages([]byte(got.stdout))
		if err != nil || len(parsed) != 10 {
			t.Errorf("ParseImages: %v (%d images)", err, len(parsed))
		}
	})
	t.Run("a refused layout is exit 1 with nothing on stdout", func(t *testing.T) {
		t.Parallel()
		for name, dir := range map[string]string{
			"a missing image": layoutsDir(t, dist.Images()[1:]),
			"an extra image":  layoutsDir(t, append(dist.Images(), "activity-curl")),
			"no such dir":     filepath.Join(t.TempDir(), "absent"),
		} {
			got := invoke(t, nil, nil, "digests", "-layouts", dir)
			if got.code != 1 || got.stdout != "" || got.stderr == "" {
				t.Errorf("%s: exit %d, stdout %q, stderr %q; want 1, nothing on stdout, a reason", name, got.code, got.stdout, got.stderr)
			}
		}
	})
}

// quickstartRepo is a repository root with what a bundle is made of: the
// repository's own compose file and README, a LICENSE and two examples.
func quickstartRepo(t *testing.T) string {
	t.Helper()
	root := repoWith(t, "")
	for _, rel := range []string{"deploy/quickstart/compose.yaml", "deploy/quickstart/README.txt"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "examples"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alpha", "beta"} {
		if err := os.WriteFile(filepath.Join(root, "examples", name+".ork.yaml"), []byte("name: "+name+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// imagesFile writes the list `digests` prints.
func imagesFile(t *testing.T, names []string) string {
	t.Helper()
	var list []dist.Image
	for _, name := range names {
		list = append(list, dist.Image{Name: name, Digest: digestOf(name)})
	}
	path := filepath.Join(t.TempDir(), "images.tsv")
	if err := os.WriteFile(path, []byte(dist.FormatImages(list)), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRun_Quickstart(t *testing.T) {
	t.Parallel()
	args := func(root, images, out string, extra ...string) []string {
		return append([]string{"quickstart", "-version", "v0.1.0", "-images", images, "-registry", "127.0.0.1:5000",
			"-tag", "smoke-1-abc", "-out", out, "-root", root}, extra...)
	}
	t.Run("writes the bundle, stamped with the flag's commit time", func(t *testing.T) {
		t.Parallel()
		root, out := quickstartRepo(t), filepath.Join(t.TempDir(), "dist")
		got := invoke(t, map[string]string{"SOURCE_DATE_EPOCH": "1"}, nil,
			args(root, imagesFile(t, dist.Images()), out, "-commit-time", "1761298215")...)
		if got.code != 0 {
			t.Fatalf("exit %d: %s", got.code, got.stderr)
		}
		bundle := filepath.Join(out, "orkstack-quickstart.tar.gz")
		if mtime := archivedTime(t, bundle); mtime.Unix() != 1761298215 {
			t.Errorf("bundle mtime %v, want the -commit-time flag's, not SOURCE_DATE_EPOCH's", mtime)
		}
		if !strings.Contains(got.stdout, "orkstack-quickstart.tar.gz") {
			t.Errorf("stdout %q does not name the bundle", got.stdout)
		}
		file, err := os.Open(bundle)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		gz, err := gzip.NewReader(file)
		if err != nil {
			t.Fatal(err)
		}
		reader := tar.NewReader(gz)
		var env []byte
		var names []string
		for {
			header, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			names = append(names, header.Name)
			if header.Name == "orkstack-quickstart/.env" {
				env, _ = io.ReadAll(reader)
			}
		}
		if !strings.Contains(string(env), "127.0.0.1:5000/orkstack/ork:smoke-1-abc@"+digestOf("ork")) {
			t.Errorf(".env does not name the registry, tag and digest it was given:\n%s", env)
		}
		if len(names) != 1+4+2+1 { // the directory, four files, two examples and examples/
			t.Errorf("entries %q", names)
		}
	})
	t.Run("falls back to SOURCE_DATE_EPOCH", func(t *testing.T) {
		t.Parallel()
		root, out := quickstartRepo(t), filepath.Join(t.TempDir(), "dist")
		got := invoke(t, map[string]string{"SOURCE_DATE_EPOCH": "1700000000"}, nil, args(root, imagesFile(t, dist.Images()), out)...)
		if got.code != 0 {
			t.Fatalf("exit %d: %s", got.code, got.stderr)
		}
		if mtime := archivedTime(t, filepath.Join(out, "orkstack-quickstart.tar.gz")); mtime.Unix() != 1700000000 {
			t.Errorf("bundle mtime %v, want SOURCE_DATE_EPOCH's", mtime)
		}
	})
	t.Run("refuses to guess a commit time", func(t *testing.T) {
		t.Parallel()
		root, out := quickstartRepo(t), filepath.Join(t.TempDir(), "dist")
		for _, env := range []map[string]string{nil, {"SOURCE_DATE_EPOCH": "soon"}} {
			got := invoke(t, env, nil, args(root, imagesFile(t, dist.Images()), out)...)
			if got.code != 2 || !strings.Contains(got.stderr, "SOURCE_DATE_EPOCH") {
				t.Errorf("env %v: exit %d, stderr %q; want 2 naming SOURCE_DATE_EPOCH", env, got.code, got.stderr)
			}
		}
		if _, err := os.Stat(out); err == nil {
			t.Error("the output directory was created before the commit time was known")
		}
	})
	t.Run("a registry or tag that is not one is a usage error, before any file is read", func(t *testing.T) {
		t.Parallel()
		root, out := quickstartRepo(t), filepath.Join(t.TempDir(), "dist")
		images := imagesFile(t, dist.Images())
		for name, replace := range map[string][2]string{
			"a registry with a path": {"127.0.0.1:5000", "ghcr.io/orkstack"},
			"a registry with a URL":  {"127.0.0.1:5000", "https://ghcr.io"},
			"a bad tag":              {"smoke-1-abc", "-latest"},
			"a tag with a slash":     {"smoke-1-abc", "a/b"},
			"a version that is not":  {"v0.1.0", "0.1.0"},
		} {
			full := args(root, images, out, "-commit-time", "1700000000")
			for i, arg := range full {
				if arg == replace[0] {
					full[i] = replace[1]
				}
			}
			got := invoke(t, nil, nil, full...)
			if got.code != 2 || got.stdout != "" || got.stderr == "" {
				t.Errorf("%s: exit %d, stdout %q, stderr %q; want 2 with a reason", name, got.code, got.stdout, got.stderr)
			}
		}
		if _, err := os.Stat(out); err == nil {
			t.Error("the output directory was created for an invalid invocation")
		}
	})
	t.Run("an image list that is not the ten is exit 1", func(t *testing.T) {
		t.Parallel()
		root, out := quickstartRepo(t), filepath.Join(t.TempDir(), "dist")
		for name, images := range map[string]string{
			"nine images":  imagesFile(t, dist.Images()[1:]),
			"no such file": filepath.Join(t.TempDir(), "absent.tsv"),
			"a list that isn't": func() string {
				path := filepath.Join(t.TempDir(), "x.tsv")
				_ = os.WriteFile(path, []byte("not a list\n"), 0o644)
				return path
			}(),
		} {
			got := invoke(t, nil, nil, args(root, images, out, "-commit-time", "1700000000")...)
			if got.code != 1 || got.stderr == "" {
				t.Errorf("%s: exit %d, stderr %q; want 1 with a reason", name, got.code, got.stderr)
			}
		}
		if _, err := os.Stat(filepath.Join(out, "orkstack-quickstart.tar.gz")); err == nil {
			t.Error("a bundle was written for an image list that is not the ten")
		}
	})
	t.Run("a repository without the compose file is exit 1", func(t *testing.T) {
		t.Parallel()
		got := invoke(t, nil, nil, args(repoWith(t, ""), imagesFile(t, dist.Images()), filepath.Join(t.TempDir(), "dist"), "-commit-time", "1700000000")...)
		if got.code != 1 || !strings.Contains(got.stderr, "compose.yaml") {
			t.Errorf("exit %d, stderr %q; want 1 naming compose.yaml", got.code, got.stderr)
		}
	})
}

// TestRun_Order: the existing tags, one per line as `git tag --list` prints
// them, decide whether the release is the latest; the answer is a line
// release.yml appends to $GITHUB_OUTPUT as it is.
func TestRun_Order(t *testing.T) {
	t.Parallel()
	tags := filepath.Join(t.TempDir(), "tags")
	if err := os.WriteFile(tags, []byte("v0.1.0\nv0.1.2\nv0.2.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		version, stdout string
		code            int
	}{
		{"v0.2.1", "latest=true\n", 0},
		{"v0.1.3", "latest=false\n", 0},
		{"v0.1.1", "", 1},
	} {
		got := invoke(t, nil, nil, "order", "-version", tc.version, "-tags", tags)
		if got.code != tc.code || got.stdout != tc.stdout {
			t.Errorf("order %s: exit %d, stdout %q, stderr %q; want exit %d, stdout %q",
				tc.version, got.code, got.stdout, got.stderr, tc.code, tc.stdout)
		}
	}
	if got := invoke(t, nil, nil, "order", "-version", "v0.1.0"); got.code != 2 {
		t.Errorf("order without -tags: exit %d, want 2", got.code)
	}
	if got := invoke(t, nil, nil, "order", "-version", "v0.1.0", "-tags", filepath.Join(t.TempDir(), "missing")); got.code != 1 {
		t.Errorf("order with a missing tags file: exit %d, want 1", got.code)
	}
}
