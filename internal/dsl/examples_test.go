package dsl_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// The example conventions (examples/README.md): every example
// this repository ships is documentation, so its comment header, its index
// entries and the guide it cites are held to rules a reader can rely on.

const (
	examplesDir = "../../examples"
	guidePath   = "../../docs/ORKFLOW_PATTERNS.md"
	readmePath  = "../../examples/README.md"
)

// predatesPatterns names the eleven examples that predate
// docs/ORKFLOW_PATTERNS.md. Apart from their document header they are kept
// byte-identical: a stored version cannot change, and the two header lines
// are the one addition it takes (docs/DSL.md, "The document header"). So they
// are exempt from the comment-header rules, and from the network and cache
// rules below; every other rule holds them too.
var predatesPatterns = map[string]bool{
	"advanced-demo.ork.yaml":      true,
	"awaited-approval.ork.yaml":   true,
	"branch-and-recover.ork.yaml": true,
	"cached-pipeline.ork.yaml":    true,
	"composed-release.ork.yaml":   true,
	"file-read.ork.yaml":          true,
	"hello.ork.yaml":              true,
	"http-post.ork.yaml":          true,
	"pinned-build.ork.yaml":       true,
	"release-block.ork.yaml":      true,
	"shell-eval.ork.yaml":         true,
}

// guideSections are docs/ORKFLOW_PATTERNS.md's `## ` headings, in order. An
// example's pattern line cites one of them by its exact text.
var guideSections = []string{
	"The rule: the platform moves data, tasks compute",
	"jq, a template, or your own image",
	"Decide in a task, branch on its value",
	"Branch and re-join",
	"Batch transforms into one prepare task",
	"Pass values as data, never as code",
	"Environment variables are configuration",
	"Cache only what you assert is reusable",
	"Pin moving inputs: resolve, then use the commit",
	"Side effects are at-least-once",
	"A non-2xx answer is data",
	"Undo what was done: failure handlers and sagas",
	"Repeat without loops",
	"Wait for a person or a system",
	"Share a working tree: pins",
	"Reuse a block with declared outputs",
	"When to write your own image",
	"Declare what surrounds your Orkflows",
	"Plan before you run",
	"Examples index",
}

const (
	orkflowFirstLine    = "# OrkStack orkflow DSL"
	automationFirstLine = "# OrkStack automation document (docs/AUTOMATION.md)"
)

var (
	patternLine = regexp.MustCompile(`^# Pattern: docs/ORKFLOW_PATTERNS\.md, "([^"]+)"$`)
	planPrefix  = "orkctl run plan --file "
	planFlags   = map[string]bool{
		"--input": true, "--output": true, "--assume-failed": true,
		"--assert-runs": true, "--assert-skipped": true,
	}
)

func orkflowExamples(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(examplesDir, "*.ork.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob examples: %v (%d found)", err, len(paths))
	}
	sort.Strings(paths)
	return paths
}

func automationExamples(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(examplesDir, "automation", "*.ork.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	return paths
}

// imageDirs are the example image build contexts, examples/custom-image*/.
func imageDirs(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(examplesDir, "custom-image*"))
	if err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for _, p := range paths {
		if info, err := os.Stat(p); err == nil && info.IsDir() {
			dirs = append(dirs, p)
		}
	}
	sort.Strings(dirs)
	return dirs
}

// header is a document's leading comment block, one line per entry.
func header(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		if !strings.HasPrefix(scanner.Text(), "#") {
			break
		}
		lines = append(lines, scanner.Text())
	}
	return lines
}

// planLineArgs is cmd/orkctl's tokenizer for a documented plan line
// (TestExamples_DocumentedPlansHold runs the lines through it): words split on spaces outside single quotes,
// the quotes stripped, and an unbalanced quote or a double quote outside
// single quotes refused. It is repeated here because a test cannot import
// package main; TestPlanLineArgs there holds the behaviour.
func planLineArgs(line string) ([]string, error) {
	var args []string
	var word strings.Builder
	inWord, quoted := false, false
	for _, r := range line {
		switch {
		case quoted && r == '\'':
			quoted = false
		case quoted:
			word.WriteRune(r)
		case r == '\'':
			quoted, inWord = true, true
		case r == '"':
			return nil, errors.New(`a double quote outside single quotes`)
		case r == ' ':
			if inWord {
				args = append(args, word.String())
				word.Reset()
				inWord = false
			}
		default:
			word.WriteRune(r)
			inWord = true
		}
	}
	if quoted {
		return nil, errors.New("an unbalanced single quote")
	}
	if inWord {
		args = append(args, word.String())
	}
	return args, nil
}

func hasDecision(wf *orkstackv1.Orkflow) bool {
	for _, task := range wf.GetTasks() {
		if task.GetCondition() != "" || task.GetAwait() != nil ||
			task.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
			return true
		}
	}
	return false
}

func TestExamples_HeadersFollowTheConvention(t *testing.T) {
	sections := map[string]bool{}
	for _, s := range guideSections {
		sections[s] = true
	}
	check := func(t *testing.T, path, firstLine string, wf *orkstackv1.Orkflow) {
		lines := header(t, path)
		if len(lines) == 0 || lines[0] != firstLine {
			t.Fatalf("line 1 = %q, want %q", strings.Join(lines[:min(1, len(lines))], ""), firstLine)
		}
		patterns, plans := 0, 0
		for _, line := range lines {
			if strings.HasPrefix(line, "# Pattern:") {
				match := patternLine.FindStringSubmatch(line)
				if match == nil {
					t.Errorf("pattern line %q is not `# Pattern: docs/ORKFLOW_PATTERNS.md, \"<title>\"`", line)
					continue
				}
				if !sections[match[1]] {
					t.Errorf("pattern line cites %q, which is no section of docs/ORKFLOW_PATTERNS.md", match[1])
				}
				patterns++
			}
			text := strings.TrimSpace(strings.TrimPrefix(line, "#"))
			if !strings.HasPrefix(text, "orkctl run plan") {
				continue
			}
			plans++
			if !strings.HasPrefix(line, "#   "+planPrefix) {
				t.Errorf("plan line %q is not `#   %s…`", line, planPrefix)
				continue
			}
			args, err := planLineArgs(text)
			if err != nil {
				t.Errorf("plan line %q: %v", line, err)
				continue
			}
			if want := "examples/" + filepath.Base(path); len(args) < 5 || args[4] != want {
				t.Errorf("plan line %q does not plan its own file %s", line, want)
				continue
			}
			asserts := 0
			rest := args[5:]
			for i := 0; i < len(rest); i += 2 {
				if !planFlags[rest[i]] || i+1 >= len(rest) {
					t.Errorf("plan line %q: argument %q is not one of --input, --output, --assume-failed, --assert-runs, --assert-skipped with a value", line, rest[i])
					break
				}
				if rest[i] == "--assert-runs" || rest[i] == "--assert-skipped" {
					asserts++
				}
			}
			if asserts == 0 {
				t.Errorf("plan line %q asserts nothing: want at least one --assert-runs or --assert-skipped", line)
			}
		}
		if patterns == 0 {
			t.Errorf("no pattern line")
		}
		if wf != nil && hasDecision(wf) && plans == 0 {
			t.Errorf("a condition, join or await, and no plan line")
		}
	}
	for _, path := range orkflowExamples(t) {
		if predatesPatterns[filepath.Base(path)] {
			continue
		}
		t.Run(filepath.Base(path), func(t *testing.T) {
			wf, err := dsl.CompileFile(path)
			if err != nil {
				t.Fatal(err)
			}
			check(t, path, orkflowFirstLine, wf)
		})
	}
	for _, path := range automationExamples(t) {
		t.Run("automation/"+filepath.Base(path), func(t *testing.T) {
			check(t, path, automationFirstLine, nil)
		})
	}
}

var (
	markdownLink = regexp.MustCompile(`\]\(([^)#\s]+)(?:#[^)]*)?\)`)
	examplesPath = regexp.MustCompile("`(examples/[A-Za-z0-9][A-Za-z0-9._/-]*)")
)

// tableLinks is every link target in the markdown table under heading,
// resolved against the document's directory and made relative to the
// repository root ("examples/x.ork.yaml", "examples/custom-image/").
func tableLinks(t *testing.T, doc, heading string) []string {
	t.Helper()
	raw, err := os.ReadFile(doc)
	if err != nil {
		t.Fatal(err)
	}
	in := false
	var links []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "## ") {
			in = strings.TrimPrefix(line, "## ") == heading
			continue
		}
		if !in || !strings.HasPrefix(line, "|") {
			continue
		}
		for _, m := range markdownLink.FindAllStringSubmatch(line, -1) {
			links = append(links, repoRelative(t, doc, m[1]))
		}
	}
	if len(links) == 0 {
		t.Fatalf("%s has no table of links under ## %s", doc, heading)
	}
	sort.Strings(links)
	return links
}

func repoRelative(t *testing.T, doc, target string) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(filepath.Join(filepath.Dir(doc), target))
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		t.Fatal(err)
	}
	rel = filepath.ToSlash(rel)
	if strings.HasSuffix(target, "/") {
		rel += "/"
	}
	return rel
}

func TestExamples_AreIndexed(t *testing.T) {
	var want []string
	for _, p := range orkflowExamples(t) {
		want = append(want, "examples/"+filepath.Base(p))
	}
	for _, p := range automationExamples(t) {
		want = append(want, "examples/automation/"+filepath.Base(p))
	}
	for _, d := range imageDirs(t) {
		want = append(want, "examples/"+filepath.Base(d)+"/")
	}
	sort.Strings(want)

	if got := tableLinks(t, readmePath, "The table"); !reflect.DeepEqual(got, want) {
		t.Errorf("examples/README.md's table indexes\n%v\nwant every example\n%v", got, want)
	}
	if got := tableLinks(t, guidePath, "Examples index"); !reflect.DeepEqual(got, want) {
		t.Errorf("docs/ORKFLOW_PATTERNS.md's Examples index indexes\n%v\nwant every example\n%v", got, want)
	}

	// Every examples/… path either document cites exists: link targets
	// resolve against the document, a `examples/…` code span against the
	// repository root.
	for _, doc := range []string{readmePath, guidePath} {
		raw, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		var cited []string
		for _, m := range markdownLink.FindAllStringSubmatch(string(raw), -1) {
			if strings.HasPrefix(m[1], "http://") || strings.HasPrefix(m[1], "https://") {
				continue
			}
			cited = append(cited, repoRelative(t, doc, m[1]))
		}
		for _, m := range examplesPath.FindAllStringSubmatch(string(raw), -1) {
			cited = append(cited, m[1])
		}
		for _, c := range cited {
			if _, err := os.Stat(filepath.Join("../..", c)); err != nil {
				t.Errorf("%s cites %s, which does not exist", doc, c)
			}
		}
	}
}

func TestExamples_ComputeTasksDeclareNoNetwork(t *testing.T) {
	compute := map[string]bool{"json": true, "template": true, "file": true}
	for _, path := range orkflowExamples(t) {
		if predatesPatterns[filepath.Base(path)] {
			continue
		}
		wf, err := dsl.CompileFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range wf.GetTasks() {
			if compute[task.GetUses()] && task.GetNetwork() != orkstackv1.NetworkMode_NETWORK_MODE_NONE {
				t.Errorf("%s: task %q uses %s and does not set network: none", filepath.Base(path), task.GetId(), task.GetUses())
			}
		}
	}
}

func TestExamples_CachedTasksSetATTLAndMountNoSecret(t *testing.T) {
	cached := 0
	for _, path := range orkflowExamples(t) {
		if predatesPatterns[filepath.Base(path)] {
			continue
		}
		wf, err := dsl.CompileFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range wf.GetTasks() {
			if task.GetCache() == nil {
				continue
			}
			cached++
			if task.GetCache().GetTtlSeconds() <= 0 {
				t.Errorf("%s: cached task %q sets no ttl_seconds", filepath.Base(path), task.GetId())
			}
			if len(task.GetSecrets()) > 0 {
				t.Errorf("%s: cached task %q mounts a secret", filepath.Base(path), task.GetId())
			}
		}
	}
	if cached == 0 {
		t.Fatal("no new example caches a task; the rule checked nothing")
	}
}

// userFacingDocs are the documents a reader of the examples reads: every
// example file (older ones included), the examples index, the image READMEs
// and the guide.
func userFacingDocs(t *testing.T) []string {
	t.Helper()
	docs := append(orkflowExamples(t), automationExamples(t)...)
	docs = append(docs, readmePath, guidePath)
	for _, d := range imageDirs(t) {
		docs = append(docs, filepath.Join(d, "README.md"))
	}
	return docs
}

func TestExamples_UseUserFacingVocabulary(t *testing.T) {
	internalWord := regexp.MustCompile(`(?i)\b(queues?|sessions?|search attributes?|worker deployments?)\b`)
	for _, doc := range userFacingDocs(t) {
		raw, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			if m := internalWord.FindString(line); m != "" {
				t.Errorf("%s:%d names %q, an internal term (AGENTS.md §7.1): %s", doc, i+1, m, line)
			}
		}
	}
}

func TestOrkflowPatterns_HasItsSections(t *testing.T) {
	raw, err := os.ReadFile(guidePath)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "## ") {
			got = append(got, strings.TrimPrefix(line, "## "))
		}
	}
	if !reflect.DeepEqual(got, guideSections) {
		t.Fatalf("docs/ORKFLOW_PATTERNS.md sections =\n%q\nwant\n%q", got, guideSections)
	}
}

func TestOrkflowPatterns_StatesTheAgreedRules(t *testing.T) {
	raw, err := os.ReadFile(guidePath)
	if err != nil {
		t.Fatal(err)
	}
	// Markdown wraps lines; a sentence is compared with its whitespace
	// collapsed, and with the emphasis markers removed.
	text := strings.Join(strings.Fields(strings.ReplaceAll(string(raw), "**", "")), " ")
	for _, sentence := range []string{
		"The platform moves and quotes data; a task computes.",
		"`env:` is a valid, expected way to configure any activity.",
		"Nothing in OrkStack expects a task to produce the same output from the same input.",
		"A task that mounts a secret is never cached.",
		"Activities are at-least-once.",
		"A failure handler never rescues the run.",
		// What a cancel and a terminate do to compensation.
		"Compensation is not guaranteed on a cancel or a terminate",
		"a run terminated from outside OrkStack runs no workflow code at all",
	} {
		if !strings.Contains(text, sentence) {
			t.Errorf("docs/ORKFLOW_PATTERNS.md does not state %q", sentence)
		}
	}
	if !strings.Contains(text, "ACTIVITY_IMAGES.md#network-and-caching-of-the-first-party-images") ||
		!strings.Contains(text, `"Network and caching of the first-party images"`) {
		t.Error(`docs/ORKFLOW_PATTERNS.md does not link docs/ACTIVITY_IMAGES.md, "Network and caching of the first-party images"`)
	}
}

// TestOrkflowPatterns_CitesNoCodeAnchors keeps the user-facing documents
// about user-facing names: no `internal/` or `cmd/` path, and no Go source
// path — a `*.go` token with a `/` in it — outside examples/. A bare file
// name (an image README naming its own main.go) is allowed.
func TestOrkflowPatterns_CitesNoCodeAnchors(t *testing.T) {
	codePath := regexp.MustCompile(`(^|[^A-Za-z0-9_])(internal/|cmd/)`)
	goPath := regexp.MustCompile(`[A-Za-z0-9_./-]*/[A-Za-z0-9_.-]+\.go\b`)
	docs := []string{guidePath, readmePath}
	for _, d := range imageDirs(t) {
		docs = append(docs, filepath.Join(d, "README.md"))
	}
	for _, doc := range docs {
		raw, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			if codePath.MatchString(line) {
				t.Errorf("%s:%d cites a code path: %s", doc, i+1, line)
			}
			for _, m := range goPath.FindAllString(line, -1) {
				if !strings.HasPrefix(strings.TrimLeft(m, "./"), "examples/") {
					t.Errorf("%s:%d cites Go source %q", doc, i+1, m)
				}
			}
		}
	}
}

// customImageLabel is the interface both example images declare: the same
// activity, so the same label.
const customImageLabel = `{"schemaVersion":1,"fields":{"label":{"type":"string"},"src":{"type":"string"},"as":{"type":"string","default":"report.txt"}},"values":{"idempotencyKey":{"type":"string"},"attempt":{"type":"number"},"taskId":{"type":"string"},"label":{"type":"string"},"argv":{"type":"array"},"source":{"type":"string"},"artifact":{"type":"string"},"stats":{"type":"object"},"each":{"type":"object"}}}`

func TestCustomImages_FollowTheDockerfileContract(t *testing.T) {
	dirs := imageDirs(t)
	if len(dirs) != 2 {
		t.Fatalf("example image directories = %v, want custom-image and custom-image-shell", dirs)
	}
	labelLine := regexp.MustCompile(`^LABEL ` + regexp.QuoteMeta(dsl.InterfaceLabel) + `='([^']*)'$`)
	readmeLabel := regexp.MustCompile(`(?s)## Interface\n.*?LABEL ` + regexp.QuoteMeta(dsl.InterfaceLabel) + `='([^']*)'`)
	exec := regexp.MustCompile(`^ENTRYPOINT \[("[^"]+")(, "[^"]+")*\]$`)
	for _, dir := range dirs {
		t.Run(filepath.Base(dir), func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
			if err != nil {
				t.Fatal(err)
			}
			var labels []string
			user, entrypoint := false, false
			for _, line := range strings.Split(string(raw), "\n") {
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				// A Dockerfile has no trailing comment: `USER 1:1  # x`
				// makes the group "1  # x".
				if strings.Contains(line, " #") && !strings.HasPrefix(line, "LABEL ") {
					t.Errorf("instruction with a trailing comment: %s", line)
				}
				switch {
				case strings.HasPrefix(line, "FROM "):
					if !strings.HasPrefix(line, "FROM scratch") && !strings.Contains(line, "@sha256:") {
						t.Errorf("unpinned base: %s", line)
					}
				case line == "USER 65532:65532":
					user = true
				case strings.HasPrefix(line, "USER "):
					t.Errorf("USER is %q, want 65532:65532", line)
				case strings.HasPrefix(line, "ENTRYPOINT "):
					entrypoint = exec.MatchString(line)
					if !entrypoint {
						t.Errorf("ENTRYPOINT is not exec form: %s", line)
					}
				case strings.HasPrefix(line, "LABEL "+dsl.InterfaceLabel):
					m := labelLine.FindStringSubmatch(line)
					if m == nil {
						t.Errorf("LABEL is not one single-quoted line: %s", line)
						continue
					}
					labels = append(labels, m[1])
				}
			}
			if !user {
				t.Error("no USER 65532:65532")
			}
			if !entrypoint {
				t.Error("no exec-form ENTRYPOINT")
			}
			if len(labels) != 1 {
				t.Fatalf("%d interface labels, want exactly one", len(labels))
			}
			if _, err := dsl.ParseInterfaceLabel(labels[0]); err != nil {
				t.Errorf("ParseInterfaceLabel: %v", err)
			}
			if labels[0] != customImageLabel {
				t.Errorf("label =\n%s\nwant\n%s", labels[0], customImageLabel)
			}
			readme, err := os.ReadFile(filepath.Join(dir, "README.md"))
			if err != nil {
				t.Fatal(err)
			}
			m := readmeLabel.FindSubmatch(readme)
			if m == nil || string(m[1]) != labels[0] {
				t.Errorf("README.md's Interface section does not reproduce the Dockerfile's label verbatim")
			}
		})
	}
}

// TestExamples_WithMatchesTheImageInterfaces checks every example task's
// with: against the interface its image declares, read from the Dockerfile
// the image is built from — the check validate and apply run against the
// pushed image, without a registry. A catalog name reads
// images/activity-<name>; an example image reads examples/<dir>.
func TestExamples_WithMatchesTheImageInterfaces(t *testing.T) {
	labelLine := regexp.MustCompile(`(?m)^LABEL ` + regexp.QuoteMeta(dsl.InterfaceLabel) + `='([^']*)'$`)
	ifaceOf := func(t *testing.T, dockerfile string) *orkstackv1.ActivityInterface {
		raw, err := os.ReadFile(dockerfile)
		if err != nil {
			t.Fatal(err)
		}
		m := labelLine.FindSubmatch(raw)
		if m == nil {
			t.Fatalf("%s declares no interface label", dockerfile)
		}
		iface, err := dsl.ParseInterfaceLabel(string(m[1]))
		if err != nil {
			t.Fatalf("%s: %v", dockerfile, err)
		}
		return iface
	}
	catalog := dsl.BuiltinActivityCatalog()
	const examplePrefix = "localhost:5001/orkstack-examples/"
	checked := 0
	for _, path := range orkflowExamples(t) {
		wf, err := dsl.CompileFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for i, task := range wf.GetTasks() {
			var dockerfile string
			switch uses := task.GetUses(); {
			case catalog[uses] != "":
				dockerfile = filepath.Join("..", "..", "images", "activity-"+uses, "Dockerfile")
			case strings.HasPrefix(uses, examplePrefix):
				dir, _, _ := strings.Cut(strings.TrimPrefix(uses, examplePrefix), ":")
				dockerfile = filepath.Join(examplesDir, dir, "Dockerfile")
			default:
				continue // awaited, composed, or an image this repository does not build
			}
			checked++
			for _, issue := range dsl.CheckWith(i, task, ifaceOf(t, dockerfile)) {
				t.Errorf("%s task %q: %s: %s", filepath.Base(path), task.GetId(), issue.GetField(), issue.GetMessage())
			}
		}
	}
	if checked == 0 {
		t.Fatal("no task was checked")
	}
}

// TestExamples_KeepTheirDefinitionDigests holds every shipped example to the
// definition digest and version it compiled to when the golden was recorded.
// The document header (docs/DSL.md, "The document header") is not part of the
// definition, so adding it moves no line of the golden; only an example whose
// definition changed on purpose, with its version bumped, may.
// Regenerate with: go test ./internal/dsl -run '^TestExamples_KeepTheirDefinitionDigests$' -update
func TestExamples_KeepTheirDefinitionDigests(t *testing.T) {
	const golden = "testdata/example-digests.golden"
	var got strings.Builder
	for _, path := range orkflowExamples(t) {
		wf, err := dsl.CompileFile(path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		digest, err := dsl.DefinitionDigest(wf)
		if err != nil {
			t.Fatalf("%s: digest: %v", path, err)
		}
		fmt.Fprintf(&got, "%s %s %s\n", filepath.Base(path), digest, wf.GetVersion())
	}
	if *updateCompat {
		if err := os.WriteFile(golden, []byte(got.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read %s: %v (record it with -update)", golden, err)
	}
	if got.String() != string(want) {
		t.Fatalf("example digests moved:\n got:\n%s\nwant:\n%s", got.String(), want)
	}
}
