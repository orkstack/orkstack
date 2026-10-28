package dist_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
)

func sha(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

// TestSums_IsTheSha256sumFormat: one line per file, "<64 hex>  <name>" with two
// spaces, sorted by name in byte order, a final newline, and the digests are
// the files' own.
func TestSums_IsTheSha256sumFormat(t *testing.T) {
	t.Parallel()
	got, err := dist.Sums(map[string][]byte{
		"orkctl-linux-amd64.tar.gz": []byte("two"),
		"ork-linux-amd64.tar.gz":    []byte("one"),
		"orker-linux-arm64.tar.gz":  []byte("three"),
		"Z-first.txt":               []byte("upper case sorts before lower"),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := sha("upper case sorts before lower") + "  Z-first.txt\n" +
		sha("one") + "  ork-linux-amd64.tar.gz\n" +
		sha("two") + "  orkctl-linux-amd64.tar.gz\n" +
		sha("three") + "  orker-linux-arm64.tar.gz\n"
	if string(got) != want {
		t.Fatalf("SHA256SUMS\n got:\n%s\nwant:\n%s", got, want)
	}
	line := regexp.MustCompile(`^[0-9a-f]{64}  [A-Za-z0-9][A-Za-z0-9._-]*$`)
	for _, l := range strings.Split(strings.TrimSuffix(string(got), "\n"), "\n") {
		if !line.MatchString(l) {
			t.Errorf("line %q is not in sha256sum format", l)
		}
	}
}

// TestSums_RefusesANameSha256sumWouldMisread: sha256sum -c treats a leading
// backslash as an escape and a "*" as binary mode, and a newline ends a line.
func TestSums_RefusesANameSha256sumWouldMisread(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "a b", "a\nb", `a\b`, "*a", "-a", "a/b", ".hidden", "ünï.tar.gz"} {
		if _, err := dist.Sums(map[string][]byte{name: []byte("x")}); err == nil {
			t.Errorf("Sums accepted the file name %q", name)
		}
	}
	if _, err := dist.Sums(nil); err == nil {
		t.Error("Sums wrote the checksums of nothing")
	}
}

// TestSumsDir_ChecksRegularFilesOnly: it sums what the release publishes — the
// regular files directly in the directory — and not the checksums file itself
// from an earlier run, nor bin/, which holds the binaries the images copy.
func TestSumsDir_ChecksRegularFilesOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	files := map[string]string{
		"b.tar.gz":   "bee",
		"a.zip":      "ay",
		"SHA256SUMS": "stale contents from a previous run",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "bin", "linux-amd64"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "linux-amd64", "ork"), []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.zip", filepath.Join(dir, "link.zip")); err != nil {
		t.Fatal(err)
	}
	names, err := dist.WriteSums(dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a.zip", "b.tar.gz"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("summed %q, want %q (not SHA256SUMS, bin/ or a symlink)", names, want)
	}
	written, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	if want := sha("ay") + "  a.zip\n" + sha("bee") + "  b.tar.gz\n"; string(written) != want {
		t.Fatalf("SHA256SUMS\n got:\n%s\nwant:\n%s", written, want)
	}
	// Running it again sums the same files to the same bytes.
	if _, err := dist.WriteSums(dir); err != nil {
		t.Fatal(err)
	}
	again, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil || string(again) != string(written) {
		t.Fatalf("a second run changed SHA256SUMS (%v)", err)
	}
}

// TestSumsDir_RefusesAnEmptyDirectory: a release with no assets has nothing to
// publish, and an empty SHA256SUMS would verify anything.
func TestSumsDir_RefusesAnEmptyDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := dist.WriteSums(dir); err == nil {
		t.Fatal("WriteSums wrote the checksums of an empty directory")
	}
	if _, err := os.Stat(filepath.Join(dir, "SHA256SUMS")); err == nil {
		t.Fatal("an empty SHA256SUMS was left behind")
	}
	if _, err := dist.WriteSums(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("WriteSums accepted a directory that does not exist")
	}
}

// TestSumsDir_IsWhatSha256sumVerifies: sha256sum accepts the file we wrote,
// and rejects it once an asset changes. Without sha256sum the test skips,
// which a proof records as a failure: every proof host has coreutils.
func TestSumsDir_IsWhatSha256sumVerifies(t *testing.T) {
	t.Parallel()
	tool, err := exec.LookPath("sha256sum")
	if err != nil {
		t.Skip("sha256sum is not installed, so the published file cannot be checked against the tool users run")
	}
	dir := t.TempDir()
	for name, body := range map[string]string{"one.tar.gz": "1", "two.zip": "2"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dist.WriteSums(dir); err != nil {
		t.Fatal(err)
	}
	check := func() error {
		cmd := exec.Command(tool, "-c", "SHA256SUMS")
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("sha256sum -c: %s", out)
		}
		return err
	}
	if err := check(); err != nil {
		t.Fatalf("sha256sum -c rejected our SHA256SUMS: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "two.zip"), []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := check(); err == nil {
		t.Fatal("sha256sum -c accepted a changed asset")
	}
}
