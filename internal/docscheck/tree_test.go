package docscheck

import (
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// markdownFiles returns the repository's documentation: markdownFilesUnder at
// the module root.
func markdownFiles(t *testing.T) []string {
	t.Helper()
	return markdownFilesUnder(t, repoRoot(t))
}

// markdownFilesUnder returns, sorted, every *.md file a commit of the work
// tree at root would hold (treeFiles): tracked, or untracked and not ignored.
// Git decides, never a directory's name. What git ignores is not the tree's
// documentation, whoever wrote it: the pages the site's docs sync ports into
// site/src/content/docs/docs/, which stay behind when the checkout moves to
// another branch, a release under /dist/, a design tool's notes, a hand-run
// Playwright failure's output. Neither is another repository below root, such
// as a worktree under .claude/worktrees/. A tracked document is read wherever
// it lies.
//
// The walk enters only the directories that hold a file of the tree, and
// reads each with os.ReadDir, so go test's cache records its listing and the
// stat of every entry in it: a new file there, or an edited .gitignore,
// re-runs these checks. The cache cannot see info/exclude or a global ignore
// file, nor, in a linked worktree, the index, which lies outside the module;
// so this package runs with -count=1 wherever its result counts: make check
// runs it uncached (GIT_STATE_PACKAGES), as `make test` and every proof run
// every package.
func markdownFilesUnder(t *testing.T, root string) []string {
	t.Helper()
	files := treeFiles(t, root)
	dirs := map[string]bool{".": true}
	for f := range files {
		for d := path.Dir(f); !dirs[d]; d = path.Dir(d) {
			dirs[d] = true
		}
	}
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if !dirs[rel] {
				return filepath.SkipDir
			}
			return nil
		}
		if files[rel] && strings.HasSuffix(rel, ".md") {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// docsFiles returns the tree's documents directly under docs/: markdownFiles,
// so a draft git ignores there neither fails a check nor stands in for a
// document the tree lacks.
func docsFiles(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "docs")
	var out []string
	for _, p := range markdownFiles(t) {
		if filepath.Dir(p) == dir {
			out = append(out, p)
		}
	}
	return out
}

// treeFiles returns the files a commit of the work tree at root would hold,
// as slash-separated paths relative to root: the tracked ones, and the
// untracked ones no .gitignore, info/exclude or global ignore file excludes.
// That is what `git add -A` stages, and so the content a proof's tree hash
// names (scripts/proof.sh). root must be the top of its own git work tree;
// anything else fails rather than reading with another tree's rules. git
// answers from the index, which go test's cache cannot see, so the index file
// is opened first (TestDocs_EnumerationsAreCacheVisible); the cache keys on it
// where the git directory lies inside the module. Every git call here only
// reads: a proof holds the tree unchanged from start to end.
func treeFiles(t *testing.T, root string) map[string]bool {
	t.Helper()
	git := func(args ...string) string {
		t.Helper()
		var stderr strings.Builder
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %s in %s: %v: %s\nthe documentation checks read the files a commit of the work tree would hold, so they need git on PATH and a git checkout",
				strings.Join(args, " "), root, err, strings.TrimSpace(stderr.String()))
		}
		return string(out)
	}
	top, err := filepath.EvalSymlinks(filepath.FromSlash(strings.TrimSpace(git("rev-parse", "--show-toplevel"))))
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if top != want {
		t.Fatalf("%s is not the top of its own git work tree (git's top is %s): the documentation checks read a whole tree, by its own ignore rules", root, top)
	}
	index := strings.TrimSpace(git("rev-parse", "--git-path", "index"))
	if !filepath.IsAbs(index) {
		index = filepath.Join(root, index)
	}
	f, err := os.Open(index)
	if err != nil {
		t.Fatalf("opening the index so the test cache keys on it: %v", err)
	}
	f.Close()
	files := map[string]bool{}
	for _, p := range strings.Split(git("ls-files", "-z", "--cached", "--others", "--exclude-standard"), "\x00") {
		// git lists another repository below root by its directory, never its files.
		if p != "" && !strings.HasSuffix(p, "/") {
			files[p] = true
		}
	}
	return files
}

// isolateGit keeps the host's git configuration and global ignore file, and a
// hook's repository variables, out of the repositories a test makes.
func isolateGit(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	for _, k := range []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR"} {
		t.Setenv(k, "") // restored when the test ends
		if err := os.Unsetenv(k); err != nil {
			t.Fatal(err)
		}
	}
}

// TestMarkdownFiles_ReadExactlyTheTree: the walk the drift checks share reads
// the markdown a commit of the work tree would hold, and nothing else on disk.
// It read every *.md but under a list of directory names, so the pages the
// site's docs sync writes, which git ignores, were checked too: a worktree
// that synced the site on one branch and switched to another failed
// TestDocs_EveryMakeTargetExists on pages its tree did not hold. The list
// also skipped tracked directories named like a build output.
func TestMarkdownFiles_ReadExactlyTheTree(t *testing.T) {
	isolateGit(t)
	root := t.TempDir()
	git := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	write := func(rel, text string) {
		t.Helper()
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	git(root, "init", "-q")
	// The repository's own rules for what is planted below.
	write(".gitignore", "/bin/\n/dist/\nnode_modules/\n")
	write("site/.gitignore", "src/content/docs/docs/\ndist/\n.impeccable/\n")
	write("ui/.gitignore", "test-results/\n")
	tracked := []string{
		"README.md",
		"docs/GUIDE.md",
		"site/content/how-to/x.md", // a page written for the site
		"internal/dist/README.md",  // in a package named like a build output
	}
	for _, rel := range tracked {
		write(rel, "# x\n")
	}
	write("dist/KEPT.md", "# x\n") // tracked below, although git ignores its directory
	write("docs/NEW.md", "# x\n")  // untracked and not ignored: a document not yet added
	for _, rel := range []string{
		"site/src/content/docs/docs/how-to/stale.md",     // a page a docs sync on another branch left
		"site/.impeccable/surfaces/brief.md",             // a design tool's notes
		"ui/test-results/spec-chromium/error-context.md", // a hand-run Playwright failure's output
		"dist/README.md", "dist/quickstart/NOTES.md", "site/dist/index.md", // a release, a site build
		"node_modules/x/README.md", "bin/NOTES.md",
	} {
		write(rel, "# x\n")
	}
	git(root, append([]string{"add", "--", ".gitignore", "site/.gitignore", "ui/.gitignore"}, tracked...)...)
	git(root, "add", "-f", "--", "dist/KEPT.md")
	// Another checkout inside this one, as a linked worktree is: its .git is a
	// file naming a git directory elsewhere, and no ignore rule names it.
	write(".claude/worktrees/other/README.md", "# x\n")
	write(".claude/worktrees/other/docs/GUIDE.md", "# x\n")
	git(root, "init", "-q", "--separate-git-dir", filepath.Join(t.TempDir(), "other.git"),
		filepath.Join(root, ".claude", "worktrees", "other"))

	var got []string
	for _, p := range markdownFilesUnder(t, root) {
		rel, err := filepath.Rel(root, p)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, filepath.ToSlash(rel))
	}
	if want := "README.md dist/KEPT.md docs/GUIDE.md docs/NEW.md internal/dist/README.md site/content/how-to/x.md"; strings.Join(got, " ") != want {
		t.Fatalf("markdown files %q, want %q", got, want)
	}
}

// TestMarkdownFiles_ReadTheRepositorysDocuments: the walk reads every tracked
// document, among them the root's, a dot directory's, the examples' and
// images' READMEs and the site's design notes. Without this, a listing that
// came back short would pass every check that reads it. The tracked set comes
// from the index alone, never from the ignore rules the walk applies, so a
// document a developer keeps ignored on disk is no part of it.
func TestMarkdownFiles_ReadTheRepositorysDocuments(t *testing.T) {
	root := repoRoot(t)
	read := map[string]bool{}
	for _, p := range markdownFiles(t) {
		rel, err := filepath.Rel(root, p)
		if err != nil {
			t.Fatal(err)
		}
		read[filepath.ToSlash(rel)] = true
	}
	want := []string{"README.md", "AGENTS.md", "CHANGELOG.md", ".github/SUPPORT.md",
		"examples/README.md", "images/activity-shell/README.md", "site/DESIGN.md"}
	cmd := exec.Command("git", "ls-files", "-z", "--cached", "--", "*.md")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files --cached: %v", err)
	}
	for _, p := range strings.Split(string(out), "\x00") {
		if p == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(p))); err != nil {
			continue // deleted in the work tree: no commit of it would hold it
		}
		want = append(want, p)
	}
	var missing []string
	for _, rel := range want {
		if !read[rel] {
			missing = append(missing, rel)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("the documentation checks read %d files and miss %d tracked documents (a named one may have moved, and this test must move with it):\n%s",
			len(read), len(missing), strings.Join(missing, "\n"))
	}
}
