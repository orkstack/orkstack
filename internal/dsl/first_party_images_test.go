package dsl

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The first-party images are ordinary OCI images, but the rules they follow
// are this repository's contract (docs/OCI_ACTIVITY_CONTRACT.md §10.1–§10.2,
// docs/ACTIVITY_IMAGES.md). These tests read the files
// under images/ — as the label tests in activity_interface_test.go do — so a
// rule cannot be broken by an edit nobody runs a container for.

var (
	firstPartyImages = []string{"activity-shell", "activity-http", "activity-file", "activity-json", "activity-git", "activity-template"}
	transformImages  = []string{"activity-json", "activity-git", "activity-template"}
)

func readImageFile(t *testing.T, image, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "images", image, name))
	if err != nil {
		t.Fatalf("read images/%s/%s: %v", image, name, err)
	}
	return string(raw)
}

// dockerInstructions returns the Dockerfile's instruction lines (the first
// line of each instruction; continuation lines are not instructions).
func dockerInstructions(dockerfile string) []string {
	var out []string
	continued := false
	for _, line := range strings.Split(dockerfile, "\n") {
		trimmed := strings.TrimSpace(line)
		isContinuation := continued
		continued = strings.HasSuffix(trimmed, `\`)
		if isContinuation || trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func TestFirstPartyImages_DockerfileContract(t *testing.T) {
	for _, image := range firstPartyImages {
		t.Run(image, func(t *testing.T) {
			instructions := dockerInstructions(readImageFile(t, image, "Dockerfile"))
			var froms, labels int
			var user, entrypoint bool
			for _, line := range instructions {
				keyword := strings.ToUpper(strings.Fields(line)[0])
				// A Dockerfile has no inline comment: `USER 65532:65532 # x`
				// makes the group "65532 # x".
				if regexp.MustCompile(`\s#`).MatchString(line) {
					t.Errorf("instruction carries a trailing # comment: %s", line)
				}
				switch keyword {
				case "FROM":
					froms++
					if !strings.Contains(line, "@sha256:") {
						t.Errorf("FROM is not digest-pinned: %s", line)
					}
				case "USER":
					user = user || line == "USER 65532:65532"
				case "ENTRYPOINT":
					entrypoint = entrypoint || line == `ENTRYPOINT ["/usr/local/bin/entry.sh"]`
				case "VOLUME":
					t.Errorf("VOLUME shadows the workspace mounts: %s", line)
				case "LABEL":
					if strings.Contains(line, InterfaceLabel+"=") {
						labels++
					}
				}
			}
			if froms == 0 {
				t.Error("no FROM")
			}
			if !user {
				t.Error("no USER 65532:65532")
			}
			if !entrypoint {
				t.Error(`no exec-form ENTRYPOINT ["/usr/local/bin/entry.sh"]`)
			}
			if labels != 1 {
				t.Errorf("%d interface LABELs, want exactly one", labels)
			}
		})
	}
}

// markdownSection returns the body of `## <title>` up to the next `## `.
func markdownSection(t *testing.T, readme, title string) string {
	t.Helper()
	start := strings.Index(readme, "\n## "+title+"\n")
	if start < 0 {
		t.Fatalf("README has no `## %s` section", title)
	}
	body := readme[start+len("\n## "+title+"\n"):]
	if end := strings.Index(body, "\n## "); end >= 0 {
		body = body[:end]
	}
	return body
}

func TestTransformImages_EnvironmentDefaults(t *testing.T) {
	want := map[string][]string{
		"activity-json":     {"HOME=/orkstack/tmp", "TZ=UTC"},
		"activity-template": {"HOME=/orkstack/tmp", "TZ=UTC"},
		"activity-git": {"HOME=/orkstack/tmp", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null",
			"GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL=https"},
	}
	oneVariable := regexp.MustCompile(`^ENV ([A-Z_][A-Z0-9_]*)=(\S+)$`)
	for _, image := range transformImages {
		t.Run(image, func(t *testing.T) {
			dockerfile := readImageFile(t, image, "Dockerfile")
			var got []string
			for _, line := range dockerInstructions(dockerfile) {
				if !strings.HasPrefix(line, "ENV ") {
					continue
				}
				if !oneVariable.MatchString(line) {
					t.Errorf("ENV must set exactly one variable as ENV <NAME>=<value>: %s", line)
					continue
				}
				got = append(got, strings.TrimPrefix(line, "ENV "))
			}
			if !slices.Equal(got, want[image]) {
				t.Fatalf("ENV defaults = %v, want exactly %v", got, want[image])
			}
			if image != "activity-git" && !regexp.MustCompile(`(?m)^RUN apk add .*\btzdata\b`).MatchString(dockerfile) {
				t.Error("the image does not install tzdata, so a task's TZ would silently read as UTC")
			}
			environment := markdownSection(t, readImageFile(t, image, "README.md"), "Environment")
			for _, assignment := range want[image] {
				name, value, _ := strings.Cut(assignment, "=")
				documented := false
				for _, row := range strings.Split(environment, "\n") {
					if strings.Contains(row, name) && strings.Contains(row, value) {
						documented = true
						break
					}
				}
				if !documented {
					t.Errorf("README's Environment section has no row naming %s and its value %s", name, value)
				}
			}
		})
	}
}

// entryCode is an entry script with backslash-continued lines joined and
// comment lines dropped: the code a static check reads.
func entryCode(script string) []string {
	var out []string
	pending := ""
	for _, line := range strings.Split(script, "\n") {
		if strings.HasSuffix(line, `\`) {
			pending += strings.TrimSuffix(line, `\`) + " "
			continue
		}
		line = pending + line
		pending = ""
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

// isRunSupervisedCall is a line that CALLS run_supervised (not its
// definition).
func isRunSupervisedCall(line string) bool {
	return regexp.MustCompile(`\brun_supervised\s`).MatchString(line) && !strings.Contains(line, "run_supervised()")
}

func TestFirstPartyImages_EntryScriptContract(t *testing.T) {
	singlePipe := regexp.MustCompile(`(^|[^|])\|([^|]|$)`)
	for _, image := range transformImages {
		t.Run(image, func(t *testing.T) {
			script := readImageFile(t, image, "entry.sh")
			code := entryCode(script)
			joined := strings.Join(code, "\n")

			// The environment is the author's: never cleared, replaced or
			// re-executed (the user decision's regression guard).
			if regexp.MustCompile(`\benv\s+-i\b`).MatchString(joined) {
				t.Error("the script clears its environment with env -i")
			}
			if regexp.MustCompile(`(^|[\s;&|(])exec(\s|$)`).MatchString(joined) {
				t.Error("the script re-executes itself with exec")
			}
			for _, m := range regexp.MustCompile(`\bunset\s+([A-Za-z_][A-Za-z0-9_]*)`).FindAllStringSubmatch(joined, -1) {
				if !regexp.MustCompile(`(?m)^\s*` + m[1] + `=`).MatchString(joined) {
					t.Errorf("the script unsets %s, a variable it does not assign itself", m[1])
				}
			}

			if !regexp.MustCompile(`(?m)^set -u$`).MatchString(joined) {
				t.Error("no `set -u`")
			}
			if regexp.MustCompile(`(?m)^\s*set -[a-z]*e`).MatchString(joined) {
				t.Error("`set -e` is forbidden (§10.2): a failing command must reach fail(), not end the script")
			}
			umask, firstRedirect := -1, -1
			for i, line := range code {
				if umask < 0 && strings.TrimSpace(line) == "umask 022" {
					umask = i
				}
				if firstRedirect < 0 && strings.Contains(line, ">") {
					firstRedirect = i
				}
			}
			if umask < 0 || firstRedirect < 0 || umask > firstRedirect {
				t.Errorf("umask 022 (line %d) must precede the first > (line %d)", umask, firstRedirect)
			}
			if !strings.Contains(joined, `with.op is required" >&2`) || !strings.Contains(joined, `unknown op '$op'" >&2`) ||
				strings.Count(joined, "exit 2") < 2 {
				t.Error("both exit-2 branches (with.op is required, unknown op) must exist")
			}
			firstCall := -1
			for i, line := range code {
				if !isRunSupervisedCall(line) {
					continue
				}
				if firstCall < 0 {
					firstCall = i
				}
				if regexp.MustCompile(`\$\([^)]*\brun_supervised\b`).MatchString(line) {
					t.Errorf("run_supervised inside $( … ) (§10.1 rule 2b): %s", strings.TrimSpace(line))
				}
				if singlePipe.MatchString(line) {
					t.Errorf("run_supervised in a pipeline (§10.1 rule 2b): %s", strings.TrimSpace(line))
				}
			}
			if firstCall < 0 {
				t.Fatal("no run_supervised call: the work is never supervised")
			}

			switch image {
			case "activity-git":
				redact := -1
				for i, line := range code {
					if strings.Contains(line, `git config --file /dev/null --type=bool --default "$GIT_TRACE_REDACT"`) {
						redact = i
						break
					}
				}
				if redact < 0 || redact > firstCall {
					t.Errorf("the GIT_TRACE_REDACT check (line %d) must precede the first run_supervised call (line %d)", redact, firstCall)
				}
			case "activity-template":
				if strings.Contains(script, "--config") {
					t.Error("the script passes --config, which would overrule a task's GOMPLATE_CONFIG without a word")
				}
				if regexp.MustCompile(`\$\{?GOMPLATE_CONFIG`).MatchString(joined) {
					t.Error("the script tests GOMPLATE_CONFIG; it is gomplate's to honour")
				}
				check := strings.Index(joined, `-f "$TMP/rendered"`)
				move := strings.Index(joined, `mv "$TMP/rendered"`)
				if check < 0 || move < 0 || check > move {
					t.Error(`the script must check [ -f "$TMP/rendered" ] before it moves the render`)
				}
			}
		})
	}
}

// jqCommand is one simple command whose first word is jq, as the entry
// script spells it.
type jqCommand struct {
	words []string
	line  string
}

// shellScan walks one joined line of shell and returns every simple command
// whose first word is `jq` (alone, after run_supervised, after `$(`, or after
// a control word) and every pipeline into jq. It knows single and double
// quotes and nested command substitutions; that is all the entry scripts use.
func shellScan(line string) (commands []jqCommand, pipedIntoJq bool) {
	controlWords := map[string]bool{"if": true, "then": true, "else": true, "elif": true, "do": true,
		"while": true, "until": true, "!": true, "{": true, "run_supervised": true}
	var scan func(i int, sub bool) int
	scan = func(i int, sub bool) int {
		atStart, afterPipe := true, false
		var capture []string
		capturing := false
		finish := func() {
			if capturing {
				commands = append(commands, jqCommand{words: capture, line: line})
			}
			capturing, capture = false, nil
		}
		for i < len(line) {
			c := line[i]
			switch {
			case c == ' ' || c == '\t':
				i++
			case c == ')' && sub:
				finish()
				return i + 1
			case c == '|' || c == ';' || c == '&':
				finish()
				single := c == '|' && (i+1 >= len(line) || line[i+1] != '|') && (i == 0 || line[i-1] != '|')
				for i < len(line) && (line[i] == c) {
					i++
				}
				atStart, afterPipe = true, single
			case c == '(' || c == ')':
				finish()
				atStart = true
				i++
			case c == '>' || c == '<':
				// A redirection ends the words a capture reads.
				if capturing {
					commands = append(commands, jqCommand{words: capture, line: line})
					capturing, capture = false, nil
				}
				i++
				for i < len(line) && (line[i] == '>' || line[i] == '&') {
					i++
				}
				_, i = readShellWord(line, i, scan)
				atStart = false
			default:
				word, next := readShellWord(line, i, scan)
				if next < len(line) && (line[next] == '>' || line[next] == '<') && regexp.MustCompile(`^[0-9]+$`).MatchString(word) {
					i = next // an fd number: the redirection follows
					continue
				}
				i = next
				switch {
				case capturing:
					capture = append(capture, word)
				case atStart && word == "jq":
					if afterPipe {
						pipedIntoJq = true
					}
					capturing, capture = true, nil
					atStart = false
				case atStart && controlWords[word]:
					// still at a command's start
				default:
					atStart = false
				}
				afterPipe = false
			}
		}
		finish()
		return i
	}
	scan(0, false)
	return commands, pipedIntoJq
}

// readShellWord reads one word starting at i — quotes kept, nested $( )
// scanned for commands of their own — and returns it and the index after it.
func readShellWord(line string, i int, scan func(int, bool) int) (string, int) {
	start := i
	for i < len(line) {
		c := line[i]
		switch {
		case c == '\'':
			end := strings.IndexByte(line[i+1:], '\'')
			if end < 0 {
				return line[start:], len(line)
			}
			i += end + 2
		case c == '"':
			i++
			for i < len(line) && line[i] != '"' {
				switch {
				case line[i] == '\\':
					i += 2
				case strings.HasPrefix(line[i:], "$("):
					i = scan(i+2, true)
				default:
					i++
				}
			}
			i++
		case c == '\\':
			i += 2
		case strings.HasPrefix(line[i:], "$("):
			i = scan(i+2, true)
		case c == ' ' || c == '\t' || c == '|' || c == ';' || c == '&' || c == '>' || c == '<' || c == '(' || c == ')':
			return line[start:i], i
		default:
			i++
		}
	}
	if i > len(line) {
		i = len(line)
	}
	return line[start:i], i
}

func TestTransformImages_NoJqReadsStdin(t *testing.T) {
	nullInput := regexp.MustCompile(`^-[A-Za-z]*n[A-Za-z]*$`)
	fileOperand := regexp.MustCompile(`^"\$[^"]*"$`)
	for _, image := range transformImages {
		t.Run(image, func(t *testing.T) {
			var found int
			for _, line := range entryCode(readImageFile(t, image, "entry.sh")) {
				commands, piped := shellScan(line)
				if piped {
					t.Errorf("a pipeline into jq reads stdin: %s", strings.TrimSpace(line))
				}
				for _, cmd := range commands {
					found++
					ok := false
					for _, w := range cmd.words {
						if nullInput.MatchString(w) || w == "--null-input" {
							ok = true
						}
					}
					if n := len(cmd.words); n > 0 && fileOperand.MatchString(cmd.words[n-1]) {
						ok = true
					}
					if !ok {
						t.Errorf("jq %s has neither -n nor a file operand, so it would read stdin: %s",
							strings.Join(cmd.words, " "), strings.TrimSpace(cmd.line))
					}
				}
			}
			if found < 5 {
				t.Fatalf("found %d jq commands; the scanner is not reading the script", found)
			}
		})
	}
}

func TestShellScan_FindsWhatTheStdinRuleIsAbout(t *testing.T) {
	for _, tc := range []struct {
		line  string
		words []string
		piped bool
	}{
		{`x=$(jq -r '.a | .b' "$CONFIG")`, []string{"-r", `'.a | .b'`, `"$CONFIG"`}, false},
		{`run_supervised jq -c -f "$F" "$TMP/in" > "$TMP/out" || fail "jq failed (jq exit 1)"`, []string{"-c", "-f", `"$F"`, `"$TMP/in"`}, false},
		{`jq -n '[]' > "$TMP/x"`, []string{"-n", "'[]'"}, false},
		{`if jq -e 'has("a")' "$CONFIG" >/dev/null 2>&1; then`, []string{"-e", `'has("a")'`, `"$CONFIG"`}, false},
		{`printf '%s' "$x" | jq .`, []string{"."}, true},
		{`fail "the filter failed (jq exit $rc)"`, nil, false},
		{`echo "activity-json: $(jq -n --version)"`, []string{"-n", "--version"}, false},
	} {
		commands, piped := shellScan(tc.line)
		var got []string
		if len(commands) > 0 {
			got = commands[0].words
		}
		if len(commands) > 1 || !slices.Equal(got, tc.words) || piped != tc.piped {
			t.Errorf("shellScan(%s) = %v (%d commands), piped=%v; want %v piped=%v", tc.line, got, len(commands), piped, tc.words, tc.piped)
		}
	}
}

// shellFunction returns a function's text, from `name() {` to the first
// line that is exactly `}`.
func shellFunction(t *testing.T, script, name string) string {
	t.Helper()
	start := strings.Index(script, "\n"+name+"() {\n")
	if start < 0 {
		t.Fatalf("no %s() function", name)
	}
	body := script[start+1:]
	end := strings.Index(body, "\n}\n")
	if end < 0 {
		t.Fatalf("%s() is not closed", name)
	}
	return body[:end+2]
}

func TestFirstPartyImages_SharedHelpersAreByteIdentical(t *testing.T) {
	for helper, images := range map[string][]string{
		"run_supervised": {"activity-http", "activity-file", "activity-json", "activity-git", "activity-template"},
		"resolve_read":   {"activity-file", "activity-json", "activity-template", "activity-git"},
		"check_name":     {"activity-file", "activity-json", "activity-template", "activity-git"},
	} {
		reference := shellFunction(t, readImageFile(t, images[0], "entry.sh"), helper)
		for _, image := range images[1:] {
			if got := shellFunction(t, readImageFile(t, image, "entry.sh"), helper); got != reference {
				t.Errorf("%s in %s differs from %s's:\n%s\nwant:\n%s", helper, image, images[0], got, reference)
			}
		}
	}
}

func TestActivityTemplate_GomplateIsAPinnedVerifiedRelease(t *testing.T) {
	dockerfile := readImageFile(t, "activity-template", "Dockerfile")
	for _, want := range []string{
		"ARG GOMPLATE_VERSION=4.3.3",
		"ARG GOMPLATE_BASE_URL=https://github.com/hairyhenderson/gomplate/releases/download",
		`"${GOMPLATE_BASE_URL}/v${GOMPLATE_VERSION}/gomplate_linux-${TARGETARCH}"`,
		"ARG GOMPLATE_SHA256_AMD64=ca281666e86f2f09218c1653e1908f572c0e349e9de64cb4ea93ade9333f0596",
		"ARG GOMPLATE_SHA256_ARM64=25bd73720ff470cec0dfaa31ca2c436b30f86142db9af68382ce64e0f3e7b9c5",
		"sha256sum -c",
	} {
		if !strings.Contains(dockerfile, want) {
			t.Errorf("Dockerfile lacks %q", want)
		}
	}
	for _, forbidden := range []string{"go install", "golang:"} {
		if strings.Contains(dockerfile, forbidden) {
			t.Errorf("Dockerfile contains %q: gomplate must be the published release binary, not a build", forbidden)
		}
	}
}

func TestActivityTemplate_CachingInventoryNamesThePinnedVersion(t *testing.T) {
	m := regexp.MustCompile(`(?m)^ARG GOMPLATE_VERSION=(\S+)$`).FindStringSubmatch(readImageFile(t, "activity-template", "Dockerfile"))
	if m == nil {
		t.Fatal("Dockerfile has no ARG GOMPLATE_VERSION=")
	}
	caching := markdownSection(t, readImageFile(t, "activity-template", "README.md"), "Caching")
	if !strings.Contains(caching, "gomplate "+m[1]) {
		t.Errorf("the Caching section does not name gomplate %s, the pinned version", m[1])
	}
	for _, other := range regexp.MustCompile(`gomplate (\d+\.\d+\.\d+)`).FindAllStringSubmatch(caching, -1) {
		if other[1] != m[1] {
			t.Errorf("the Caching section names gomplate %s, but the Dockerfile pins %s: re-derive the inventory", other[1], m[1])
		}
	}
	for _, want := range []string{"time.Since", "time.Until", "HOSTNAME"} {
		if !strings.Contains(caching, want) {
			t.Errorf("the Caching section does not list %s", want)
		}
	}
}

func TestTransformImages_PinTheirToolVersions(t *testing.T) {
	for image, constraints := range map[string][]string{
		"activity-json":     {"'jq~1.7'"},
		"activity-template": {"'jq~1.7'"},
		"activity-git":      {"'git~2.45'", "'jq~1.7'"},
	} {
		dockerfile := readImageFile(t, image, "Dockerfile")
		for _, c := range constraints {
			if !strings.Contains(dockerfile, c) {
				t.Errorf("%s does not install %s", image, c)
			}
		}
	}
}

const secretNeverCached = "A task that mounts a secret is never cached"

func TestTransformImages_ReadmesStateCachingEnvironmentAndInterface(t *testing.T) {
	codeAnchor := regexp.MustCompile(`\.go\b|internal/`)
	for _, image := range transformImages {
		t.Run(image, func(t *testing.T) {
			readme := readImageFile(t, image, "README.md")
			caching := markdownSection(t, readme, "Caching")
			environment := markdownSection(t, readme, "Environment")
			markdownSection(t, readme, "Interface")
			for _, want := range []string{secretNeverCached, "Unsafe to cache when", "Recommended practice"} {
				if !strings.Contains(caching, want) {
					t.Errorf("Caching section lacks %q", want)
				}
			}
			var specific []string
			switch image {
			case "activity-git":
				specific = []string{"commit id"}
			default:
				specific = []string{"HOSTNAME", "network: none"}
			}
			for _, want := range specific {
				if !strings.Contains(caching, want) {
					t.Errorf("Caching section lacks %q", want)
				}
			}
			for name, section := range map[string]string{"Caching": caching, "Environment": environment} {
				if loc := codeAnchor.FindString(section); loc != "" {
					t.Errorf("%s section carries a code anchor (%q): user-facing text restates rules, never files", name, loc)
				}
			}
			var envWants []string
			switch image {
			case "activity-git":
				envWants = []string{"GIT_SSL_NO_VERIFY", "GIT_TRACE_REDACT"}
			case "activity-template":
				envWants = []string{"GOMPLATE_CONFIG", "GOMPLATE_TYPE_PARAM", "missingKey"}
			}
			for _, want := range envWants {
				if !strings.Contains(environment, want) {
					t.Errorf("Environment section lacks %q", want)
				}
			}
		})
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "ACTIVITY_IMAGES.md"))
	if err != nil {
		t.Fatalf("read docs/ACTIVITY_IMAGES.md: %v", err)
	}
	if !strings.Contains(string(raw), secretNeverCached) {
		t.Errorf("docs/ACTIVITY_IMAGES.md lacks %q", secretNeverCached)
	}
}

// TestFirstPartyImages_EveryDirectoryIsAnImage keeps this file's list honest:
// a new images/activity-* directory must be named here (and in the catalog).
func TestFirstPartyImages_EveryDirectoryIsAnImage(t *testing.T) {
	dirs, err := filepath.Glob(filepath.Join("..", "..", "images", "activity-*"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range dirs {
		got = append(got, filepath.Base(d))
	}
	want := slices.Clone(firstPartyImages)
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("images/ holds %v, this test names %v", got, want)
	}
}
