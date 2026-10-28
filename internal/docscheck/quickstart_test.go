package docscheck

import (
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The quickstart is one list of commands: the README's "Quickstart" block.
// scripts/quickstart-smoke.sh runs it as written on a fresh clone, and the
// landing page reads it at build time (site/src/lib/quickstart.ts). These
// tests hold every other page that walks a new user through it to the same
// commands in the same order, and hold the commands to what the repository
// has, so the pages and the working path cannot drift apart.

// quickstartClone is the README's first command, the one the smoke replaces
// the URL of.
const quickstartClone = "git clone https://github.com/orkstack/orkstack.git && cd orkstack"

// quickstartPages walk a new user through the README's commands.
var quickstartPages = []string{"docs/GETTING_STARTED.md", "site/content/tutorials/first-orkflow.md"}

var bashFenceOpen = regexp.MustCompile("^```(?:bash|sh|shell)\\s*$")

// bashFences returns the body lines of every top-level bash fence of a
// markdown document, in order.
func bashFences(doc string) [][]string {
	var out [][]string
	lines := strings.Split(doc, "\n")
	for i := 0; i < len(lines); i++ {
		if !bashFenceOpen.MatchString(lines[i]) {
			continue
		}
		var body []string
		for i++; i < len(lines) && lines[i] != "```"; i++ {
			body = append(body, lines[i])
		}
		out = append(out, body)
	}
	return out
}

// readmeQuickstart returns the lines of the one bash block of the README's
// "## Quickstart" section, which the smoke and the landing page read the same
// way: the section runs to the next "## " heading.
func readmeQuickstart(readme string) ([]string, string) {
	_, section, ok := strings.Cut("\n"+readme, "\n## Quickstart\n")
	if !ok {
		return nil, `README.md has no "## Quickstart" section`
	}
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	fences := bashFences(section)
	if len(fences) != 1 {
		return nil, "README.md's Quickstart has " + strconv.Itoa(len(fences)) + " bash blocks, want exactly one: the one the smoke runs"
	}
	// The smoke's awk and the landing page's reader match the fence exactly.
	if !strings.Contains(section, "\n```bash\n") {
		return nil, "README.md's Quickstart block does not open with exactly ```bash, which the smoke and the landing page read"
	}
	return fences[0], ""
}

// commandsOf splits shell lines into the commands they run: `a && b` is two,
// a trailing comment is dropped, and a blank or comment line is none.
func commandsOf(lines []string) []string {
	var out []string
	for _, line := range lines {
		if at := commentStart(line); at >= 0 {
			line = line[:at]
		}
		for _, cmd := range strings.Split(line, " && ") {
			if cmd = strings.TrimSpace(cmd); cmd != "" {
				out = append(out, cmd)
			}
		}
	}
	return out
}

// commentStart is the index of the # that starts a shell comment on line, or
// -1: at the start of a word and outside quotes.
func commentStart(line string) int {
	var quote rune
	for i, r := range line {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
		case r == '#' && (i == 0 || line[i-1] == ' ' || line[i-1] == '\t'):
			return i
		}
	}
	return -1
}

// missingInOrder returns the first of want that got does not run after the
// ones before it, or "" when got runs every one of them in order.
func missingInOrder(want, got []string) string {
	at := 0
	for _, cmd := range want {
		i := slices.Index(got[at:], cmd)
		if i < 0 {
			return cmd
		}
		at += i + 1
	}
	return ""
}

func TestQuickstart_TheReadmeIsTheOneListOfCommands(t *testing.T) {
	root := repoRoot(t)
	block, problem := readmeQuickstart(readFile(t, filepath.Join(root, "README.md")))
	if problem != "" {
		t.Fatal(problem)
	}
	if len(block) == 0 || block[0] != quickstartClone {
		t.Fatalf("README.md's Quickstart starts with %q, want %q: the smoke runs it on a clone of the tree by replacing its URL", block, quickstartClone)
	}
	// The landing page shows each line as a command with a note: no blank
	// line, no comment.
	for _, line := range block {
		if strings.TrimSpace(line) == "" || commentStart(line) >= 0 {
			t.Errorf("README.md's Quickstart line %q is blank or carries a comment; each line is one command the landing page shows", line)
		}
	}
	cmds := commandsOf(block)
	if !slices.Contains(cmds, "docker compose up --build -d --wait") {
		t.Errorf("README.md's Quickstart does not run `docker compose up --build -d --wait`: %q", cmds)
	}
	// orkctl comes from the compose file's own service, which exists.
	compose := readFile(t, filepath.Join(root, "compose.yaml"))
	alias := regexp.MustCompile(`^alias orkctl='docker compose run --rm ([a-z-]+)'$`)
	aliased := false
	for _, cmd := range cmds {
		if m := alias.FindStringSubmatch(cmd); m != nil {
			aliased = true
			if !strings.Contains(compose, "\n  "+m[1]+":\n") {
				t.Errorf("the Quickstart runs orkctl as the compose service %q, which compose.yaml does not declare", m[1])
			}
		}
	}
	if !aliased {
		t.Errorf("README.md's Quickstart defines no orkctl alias for the compose service: %q", cmds)
	}
	// It applies a file that exists, and submits the Orkflow that file names.
	var applied, submitted string
	submit := regexp.MustCompile(`^orkctl run submit ([a-z][a-z0-9-]*) --watch$`)
	for _, cmd := range cmds {
		if f, ok := strings.CutPrefix(cmd, "orkctl apply "); ok {
			applied = f
		}
		if m := submit.FindStringSubmatch(cmd); m != nil {
			submitted = m[1]
		}
	}
	if applied == "" || submitted == "" {
		t.Fatalf("README.md's Quickstart applies %q and submits %q; it must do both, ending with a watched run", applied, submitted)
	}
	name := regexp.MustCompile(`(?m)^name: ([a-z][a-z0-9-]*)$`).FindStringSubmatch(readFile(t, filepath.Join(root, filepath.FromSlash(applied))))
	if name == nil || name[1] != submitted {
		t.Errorf("the Quickstart applies %s, whose name is %v, but submits %s", applied, name, submitted)
	}
	if last := cmds[len(cmds)-1]; last != "orkctl run submit "+submitted+" --watch" {
		t.Errorf("README.md's Quickstart ends with %q; it ends with the watched run, whose result the smoke checks", last)
	}
	// The landing page reads the block rather than keeping a copy.
	lib := readFile(t, filepath.Join(root, "site", "src", "lib", "quickstart.ts"))
	for _, want := range []string{"'README.md'", "Quickstart", "```bash"} {
		if !strings.Contains(lib, want) {
			t.Errorf("site/src/lib/quickstart.ts does not read the README's Quickstart (%s)", want)
		}
	}
	if landing := readFile(t, filepath.Join(root, "site", "src", "components", "landing", "Quickstart.astro")); !strings.Contains(landing, "quickstartCommands") {
		t.Error("the landing page's Quickstart.astro does not show lib/quickstart.ts's commands")
	}
}

func TestQuickstart_EveryPageRunsTheReadmesCommandsInOrder(t *testing.T) {
	root := repoRoot(t)
	block, problem := readmeQuickstart(readFile(t, filepath.Join(root, "README.md")))
	if problem != "" {
		t.Fatal(problem)
	}
	want := commandsOf(block)
	for _, page := range quickstartPages {
		var lines []string
		for _, fence := range bashFences(readFile(t, filepath.Join(root, filepath.FromSlash(page)))) {
			lines = append(lines, fence...)
		}
		if missing := missingInOrder(want, commandsOf(lines)); missing != "" {
			t.Errorf("%s does not run the README's Quickstart in order: %q is missing or out of place (want %q, in that order)", page, missing, want)
		}
	}
}

// TestQuickstart_NoPageNamesTheRetiredBundle: getting started was once a
// release tarball's compose file. Nothing a reader is shown says to download
// it any more.
func TestQuickstart_NoPageNamesTheRetiredBundle(t *testing.T) {
	root := repoRoot(t)
	files := markdownFiles(t)
	err := filepath.WalkDir(filepath.Join(root, "site", "src"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "node_modules" || d.Name() == "content") {
			return fs.SkipDir // the docs the sync ports, which the markdown walk reads at their source
		}
		if !d.IsDir() && (strings.HasSuffix(p, ".astro") || strings.HasSuffix(p, ".ts") || strings.HasSuffix(p, ".mjs")) {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.Contains(readFile(t, file), "orkstack-quickstart.tar.gz") {
			rel, _ := filepath.Rel(root, file)
			t.Errorf("%s names orkstack-quickstart.tar.gz; getting started is a clone and `docker compose up`", filepath.ToSlash(rel))
		}
	}
}

func TestQuickstart_ChecksBiteOnPlantedText(t *testing.T) {
	for _, tc := range []struct {
		lines []string
		want  []string
	}{
		{[]string{"git clone u && cd orkstack"}, []string{"git clone u", "cd orkstack"}},
		{[]string{"docker compose down      # stop; data kept"}, []string{"docker compose down"}},
		{[]string{"# a comment", "", "  orkctl version  "}, []string{"orkctl version"}},
		{[]string{"alias orkctl='docker compose run --rm orkctl'"}, []string{"alias orkctl='docker compose run --rm orkctl'"}},
		{[]string{`echo "a # b"`}, []string{`echo "a # b"`}},
		{[]string{"echo a#b"}, []string{"echo a#b"}},
	} {
		if got := commandsOf(tc.lines); !slices.Equal(got, tc.want) {
			t.Errorf("commandsOf(%q) = %q, want %q", tc.lines, got, tc.want)
		}
	}
	want := []string{"a", "b", "c"}
	for _, tc := range []struct {
		got     []string
		missing string
	}{
		{[]string{"a", "x", "b", "y", "c"}, ""},
		{[]string{"a", "c"}, "b"},
		{[]string{"b", "a", "c"}, "b"},
		{[]string{"c", "b", "a"}, "b"},
		{nil, "a"},
	} {
		if got := missingInOrder(want, tc.got); got != tc.missing {
			t.Errorf("missingInOrder(%q, %q) = %q, want %q", want, tc.got, got, tc.missing)
		}
	}
	readme := "# x\n\n## Quickstart\n\ntext\n\n```bash\none\ntwo\n```\n\n## Next\n\n```bash\nthree\n```\n"
	if got, problem := readmeQuickstart(readme); problem != "" || !slices.Equal(got, []string{"one", "two"}) {
		t.Errorf("readmeQuickstart = %q, %q; want the section's own block", got, problem)
	}
	if _, problem := readmeQuickstart("## Quickstart\n\n```bash\na\n```\n\n```bash\nb\n```\n"); problem == "" {
		t.Error("readmeQuickstart accepts a section with two bash blocks")
	}
	if _, problem := readmeQuickstart("## Quickstart\n\n```sh\na\n```\n"); problem == "" {
		t.Error("readmeQuickstart accepts a ```sh block, which the smoke's awk does not read")
	}
	if _, problem := readmeQuickstart("## Elsewhere\n"); problem == "" {
		t.Error("readmeQuickstart accepts a README with no Quickstart")
	}
}
