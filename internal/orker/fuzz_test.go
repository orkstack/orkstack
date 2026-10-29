package orker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/quantity"
)

// FuzzRenderer_ARenderedValueIsTheValue: a value reaches a rendered string
// exactly as it is, whatever bytes it holds. Rendering substitutes data and
// never re-reads it as a template, so data that spells an action, a marker or
// a quote is still just data.
func FuzzRenderer_ARenderedValueIsTheValue(f *testing.F) {
	for _, seed := range []string{
		"", "hello", "{{ .inputs.value }}", "{{", "}}", "<no value>", "x <no value> y", "a\nb", "'; rm -rf /", "$(id)", "%s %d", "\\", "\xff\xfe",
		"\u00e9\u2028", strings.Repeat("{{ . }}", 20), "<no\x01value>", "<nil>", "{{ index .inputs \"value\" }}",
	} {
		f.Add(seed, "prefix ", " suffix")
	}
	// The characters of the nil marker put together across the text and the
	// value, and a value or text that holds the marker whole.
	f.Add("value>", "<no ", "")
	f.Add("", "<no ", " value>")
	f.Add("value", "see <no ", "> here")
	f.Add("<no value", "", ">")
	f.Add("<no", "", " value>")
	f.Add("<no value>", "<no value>", "<no value>")
	f.Fuzz(func(t *testing.T, value, before, after string) {
		r := testRenderer(map[string]any{"value": value}, nil, nil, nil, nil)
		// before and after are template text, the author's, so only plain text
		// (no brace that could join the action's) is part of this property.
		if strings.ContainsAny(before+after, "{}") {
			return
		}
		got, err := r.render(context.Background(), map[string]any{
			"plain":  "{{ .inputs.value }}",
			"around": before + "{{ .inputs.value }}" + after,
			"nested": map[string]any{"list": []any{"{{ .inputs.value }}", 1.5, true, nil}},
		})
		if err != nil {
			t.Fatalf("render with the value %q: %v", value, err)
		}
		if got["plain"] != value {
			t.Fatalf("the value %q rendered as %q", value, got["plain"])
		}
		if got["around"] != before+value+after {
			t.Fatalf("the value %q rendered between %q and %q as %q", value, before, after, got["around"])
		}
		list := got["nested"].(map[string]any)["list"].([]any)
		if list[0] != value || list[1] != 1.5 || list[2] != true || list[3] != nil {
			t.Fatalf("a nested value rendered as %v", list)
		}
	})
}

// FuzzRenderer_ShHelperQuotesAnyValueAsOneShellArgument: AGENTS.md §6 says
// shell commands must not interpolate untrusted input without the templating
// layer's escaping. For any bytes a shell can be handed, the sh helper's
// output is one word the shell reads back as exactly those bytes.
func FuzzRenderer_ShHelperQuotesAnyValueAsOneShellArgument(f *testing.F) {
	for _, seed := range []string{
		"it's data", "a'b'c", "$(printf INJECTED)", "`printf INJECTED`", "value; printf INJECTED", "first\nprintf INJECTED", "-n", "", "'", "''",
		"\\", "*", "~", "!", "${HOME}", "\t", " ", "a\\'b", "'\\''", "\xff", "\u00e9", "\r\n", "#", "&", "|", "<(x)", "\\\n",
	} {
		f.Add(seed)
	}
	shell, err := exec.LookPath("sh")
	if err != nil {
		f.Fatalf("the sh helper is proven against a real shell: %v", err)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if strings.IndexByte(value, 0) >= 0 {
			return // a NUL byte cannot be part of a command line
		}
		r := testRenderer(map[string]any{"value": value}, nil, nil, nil, nil)
		got, err := r.render(context.Background(), map[string]any{"quoted": "{{ sh .inputs.value }}"})
		if err != nil {
			t.Fatalf("render %q: %v", value, err)
		}
		quoted := got["quoted"].(string)
		script := `set -- ` + quoted + `; [ "$#" -eq 1 ] || exit 91; printf '%s' "$1"`
		out, err := exec.Command(shell, "-c", script).CombinedOutput()
		if err != nil {
			t.Fatalf("the shell rejected %q for the value %q: %v: %s", quoted, value, err, out)
		}
		if string(out) != value {
			t.Fatalf("the shell read %q as %q, want %q", quoted, out, value)
		}
	})
}

// FuzzReadValues: the container's values.json is untrusted bytes. It is
// accepted, and returned verbatim, only when it is a regular file of at most
// the limit holding one valid UTF-8 JSON object; everything else is a
// contract violation with a machine-readable reason.
func FuzzReadValues(f *testing.F) {
	for _, seed := range []string{
		`{}`, `{"a":1,"b":[true,null,"x"],"c":{"d":1.5e3}}`, ` {"a":1} `, `[]`, `"x"`, `12`, `true`, `null`, ``, ` `, `{`, `{}{}`, `{"a":1}x`,
		"{\"a\":\"\xff\"}", `{"a":"\ud800"}`, `{"a":1e999}`, `{"a":1,"a":2}`, "\ufeff{}", "\u00a0{}", `{"a":` + strings.Repeat("[", 5000) + strings.Repeat("]", 5000) + `}`,
	} {
		f.Add([]byte(seed), int64(0))
		f.Add([]byte(seed), int64(16))
	}
	dir := f.TempDir()
	path := filepath.Join(dir, "values.json")
	tree, failure := OpenOutputTree(dir)
	if failure != nil {
		f.Fatal(failure)
	}
	f.Cleanup(func() { _ = tree.Close() })
	f.Fuzz(func(t *testing.T, content []byte, max int64) {
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
		limits := &orkstackv1.WorkspaceLimits{ValuesMaxBytes: max}
		raw, failure := tree.ReadValues(limits)
		var object map[string]json.RawMessage
		wellFormed := utf8.Valid(content) && json.Valid(content) && json.Unmarshal(content, &object) == nil && object != nil
		// json.Unmarshal into any is what the check reads with; a number
		// beyond float64 is malformed to it.
		var probe any
		wellFormed = wellFormed && json.Unmarshal(content, &probe) == nil
		tooLarge := max > 0 && int64(len(content)) > max
		switch {
		case failure != nil:
			if raw != nil || failure.Reason == "" || failure.Message == "" {
				t.Fatalf("ReadValues(%q) = %q, %+v", content, raw, failure)
			}
			if failure.Reason != ReasonValuesMalformed && failure.Reason != ReasonValuesTooLarge {
				t.Fatalf("ReadValues(%q) failed as %s", content, failure.Reason)
			}
			if wellFormed && !tooLarge {
				t.Fatalf("ReadValues refused %q, a JSON object within the limit: %+v", content, failure)
			}
		case !wellFormed || tooLarge:
			t.Fatalf("ReadValues accepted %q (max %d)", content, max)
		case !bytes.Equal(raw, content):
			t.Fatalf("ReadValues returned %q for %q: the bytes are returned verbatim", raw, content)
		}
	})
}

// The kinds of thing FuzzOutputTree plants where the Orker reads. For
// output/artifacts itself, outputKindRegular means a real directory and
// outputKindDir a regular file, so a zero byte is the well-formed layout.
const (
	outputKindRegular = iota
	outputKindAbsent
	outputKindDir
	outputKindLinkOutsideFile
	outputKindLinkInside
	outputKindLinkOutsideDir
	outputKindLinkDangling
	outputKindFIFO
	outputKinds
)

// FuzzOutputTree: the output directory's layout is the workload's. Whatever
// it plants where the Orker reads — regular files, directories, links inside
// or outside the tree, dangling links, FIFOs — the tree returns only the bytes
// of regular files exactly where §3.4 and §3.5 put them, never a byte from
// outside, and refuses everything else as a ContractViolation that names the
// container path it refused and nothing of the host's (B7).
func FuzzOutputTree(f *testing.F) {
	for _, seed := range []struct {
		values, artifacts uint8
		entries           []byte
	}{
		{0, 0, []byte{0, 0}}, {0, 0, nil}, {1, 1, nil}, {3, 0, nil}, {4, 5, nil}, {0, 0, []byte{3}},
		{6, 4, []byte{0, 5}}, {7, 7, nil}, {2, 2, nil}, {5, 3, nil}, {0, 6, nil}, {0, 0, []byte{0, 2, 4, 6, 7, 1}},
	} {
		f.Add(seed.values, seed.artifacts, seed.entries)
	}
	f.Fuzz(func(t *testing.T, valuesKind, artifactsKind uint8, entryKinds []byte) {
		valuesKind, artifactsKind = valuesKind%outputKinds, artifactsKind%outputKinds
		if len(entryKinds) > 6 {
			entryKinds = entryKinds[:6]
		}
		base := t.TempDir()
		output, outside := filepath.Join(base, "output"), filepath.Join(base, "outside")
		for _, dir := range []string{filepath.Join(output, "inside-dir"), filepath.Join(outside, "dir")} {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		const sentinel = "orker-only-bytes"
		mustWrite(t, filepath.Join(outside, "outside.json"), `{"leaked":"`+sentinel+`"}`)
		mustWrite(t, filepath.Join(output, "inside.json"), `{"inside":true}`)
		for i := range 6 {
			mustWrite(t, filepath.Join(outside, "dir", "e"+strconv.Itoa(i)), sentinel)
			mustWrite(t, filepath.Join(output, "inside-dir", "e"+strconv.Itoa(i)), "inside")
		}
		// plant puts kind at path; regular writes what a regular file holds,
		// and inside is the in-tree target of a link that stays inside.
		plant := func(path string, kind uint8, regular func(string) error, inside string) {
			var err error
			switch kind {
			case outputKindRegular:
				err = regular(path)
			case outputKindAbsent:
			case outputKindDir:
				err = os.Mkdir(path, 0o755)
			case outputKindLinkOutsideFile:
				err = os.Symlink(filepath.Join(outside, "outside.json"), path)
			case outputKindLinkInside:
				err = os.Symlink(inside, path)
			case outputKindLinkOutsideDir:
				err = os.Symlink(filepath.Join(outside, "dir"), path)
			case outputKindLinkDangling:
				err = os.Symlink(filepath.Join(outside, "missing"), path)
			case outputKindFIFO:
				mustFIFO(t, path)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		writes := func(content string) func(string) error {
			return func(path string) error { return os.WriteFile(path, []byte(content), 0o644) }
		}
		plant(filepath.Join(output, "values.json"), valuesKind, writes(`{"v":1}`), "inside.json")
		artifactsDir := filepath.Join(output, "artifacts")
		switch artifactsKind {
		case outputKindRegular:
			plant(artifactsDir, outputKindDir, nil, "")
		case outputKindDir:
			plant(artifactsDir, outputKindRegular, writes("x"), "")
		default:
			plant(artifactsDir, artifactsKind, nil, "inside-dir")
		}
		var wantArtifacts []string
		entriesRegular := true
		if artifactsKind == outputKindRegular {
			for i, kind := range entryKinds {
				kind %= outputKinds
				name := "e" + strconv.Itoa(i)
				plant(filepath.Join(artifactsDir, name), kind, writes("artifact-"+name), "../inside.json")
				switch kind {
				case outputKindRegular:
					wantArtifacts = append(wantArtifacts, name+"=artifact-"+name)
				case outputKindAbsent:
				default:
					entriesRegular = false
				}
			}
		}

		tree, failure := OpenOutputTree(output)
		if failure != nil {
			t.Fatal(failure)
		}
		defer func() { _ = tree.Close() }()
		refused := func(what string, failure *Failure, reason, named string) {
			t.Helper()
			switch {
			case failure == nil:
				t.Fatalf("%s: accepted; want %s", what, reason)
			case failure.Reason != reason:
				t.Fatalf("%s: %s; want %s", what, failure, reason)
			case failure.Category() != CategoryContractViolation:
				t.Fatalf("%s: category %s", what, failure.Category())
			case !strings.Contains(failure.Message, named):
				t.Fatalf("%s: %q does not name %s", what, failure.Message, named)
			case strings.Contains(failure.Error(), base) || strings.Contains(failure.Error(), sentinel):
				t.Fatalf("%s: %q names the host's path or bytes", what, failure)
			}
		}

		values, failure := tree.ReadValues(testLimits())
		switch valuesKind {
		case outputKindRegular:
			if failure != nil || string(values) != `{"v":1}` {
				t.Fatalf("values.json = %q, %v; want the regular file's bytes", values, failure)
			}
		case outputKindAbsent:
			refused("values.json", failure, ReasonValuesMissing, ContainerValuesJSON)
		default:
			if values != nil {
				t.Fatalf("values.json kind %d returned %q", valuesKind, values)
			}
			refused("values.json", failure, ReasonValuesMalformed, ContainerValuesJSON+" is ")
		}

		scanned, failure := tree.ScanArtifacts(testLimits())
		switch {
		case artifactsKind == outputKindAbsent:
			if failure != nil || scanned != nil {
				t.Fatalf("no artifacts directory = %v, %v; want nothing", scanned, failure)
			}
		case artifactsKind != outputKindRegular:
			refused("output/artifacts", failure, ReasonArtifactNotAFile, ContainerArtifacts+" is ")
		case !entriesRegular:
			refused("an artifact", failure, ReasonArtifactNotAFile, ContainerArtifacts+"/e")
		default:
			if failure != nil {
				t.Fatalf("regular artifacts refused: %v", failure)
			}
			var got []string
			for _, artifact := range scanned {
				body, failure := artifact.Open()
				if failure != nil {
					t.Fatalf("Open %s: %v", artifact.Name, failure)
				}
				raw, err := io.ReadAll(body)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, artifact.Name+"="+string(raw))
			}
			if strings.Join(got, ",") != strings.Join(wantArtifacts, ",") {
				t.Fatalf("artifacts = %v; want %v", got, wantArtifacts)
			}
		}
	})
}

// FuzzDockerCreateArgs: the argument vector is the Orker's boundary with the
// container runtime. Whatever strings a spec carries, a spec that renders
// holds the whole security profile exactly once, names no flag the profile
// does not, puts the image where the runtime reads the image and never lets
// an image, a label, a value or a mount path be read as an option.
func FuzzDockerCreateArgs(f *testing.F) {
	for _, seed := range []string{
		"ghcr.io/acme/tool@sha256:" + strings.Repeat("a", 64), "--privileged", "-v", "--entrypoint=x", "--network=host", "", "a b",
		"x\ny", "--cap-add=ALL.example/x",
	} {
		f.Add(seed, "--privileged", "run,1", "/work/a", "/dest", "-it")
	}
	f.Add("img", "k", "v", "/work/x,readonly=false", "/dest", "--rm")
	f.Add("img", "k", "v", "/work/x", "/dest,source=/", "--rm")
	f.Add("img", "k", "v", "/work/x\nsource=/", "/dest", "--rm")
	allowed := map[string]int{ // flag -> how many values it takes
		"--name": 1, "--read-only": 0, "--cap-drop=ALL": 0, "--security-opt": 1, "--init": 0, "--workdir": 1, "--network": 1,
		"--cpus": 1, "--memory": 1, "--pids-limit": 1, "--stop-timeout": 1, "--user": 1, "--tmpfs": 1, "--label": 1, "--env": 1, "--mount": 1,
		"--http-proxy=false": 0, "--userns=keep-id": 0,
	}
	// Docker, the rootless podman CLI, and the docker CLI on the Podman socket.
	dialects := []dialect{dockerDialect, {cli: CLIPodman, engine: CLIPodman, rootless: true}, {cli: CLIDocker, engine: CLIPodman}}
	f.Fuzz(func(t *testing.T, image, labelKey, labelValue, source, destination, command string) {
		spec := validSpec()
		spec.Image = image
		spec.Labels = map[string]string{"ork.run": labelValue, "ork.task": "build", labelKey: labelValue}
		spec.Env = map[string]string{labelKey: labelValue}
		spec.Mounts = []Mount{{Source: source, Destination: destination, ReadOnly: true}}
		spec.Command = []string{command, "-c", "--privileged"}
		for _, spoken := range dialects {
			args, err := createArgs(spec, spoken)
			if err != nil {
				if err.Error() == "" {
					t.Fatalf("createArgs refused with no reason")
				}
				continue
			}
			imageAt := len(args) - len(spec.Command) - 1
			if imageAt < 1 || args[imageAt] != image || args[0] != "create" {
				t.Fatalf("the image %q is not where the runtime reads it in %q", image, args)
			}
			if image == "" || image[0] == '-' || strings.ContainsAny(image, " \t\r\n\x00") {
				t.Fatalf("an image %q reached the argument vector %q", image, args)
			}
			seen := map[string]int{}
			for i := 1; i < imageAt; {
				values, known := allowed[args[i]]
				if !known {
					t.Fatalf("%+v: argument %d, %q, is not a flag of the security profile: %q", spoken, i, args[i], args)
				}
				seen[args[i]]++
				i += 1 + values
				if i > imageAt {
					t.Fatalf("flag %q is missing its value in %q", args[i-1-values], args)
				}
			}
			for _, once := range []string{"--name", "--read-only", "--cap-drop=ALL", "--init", "--workdir", "--network", "--cpus", "--memory", "--pids-limit"} {
				if seen[once] != 1 {
					t.Fatalf("%s appears %d times in %q", once, seen[once], args)
				}
			}
			// The engine's spellings: /tmp, and on Podman a read-only tmpfs over
			// each root path its --read-only leaves writable; no-new-privileges,
			// and on Docker the seccomp profile by name.
			wantTmpfs, wantSecurity := 1, 2
			if spoken.engine == CLIPodman {
				wantTmpfs, wantSecurity = 1+len(readOnlyRootfsPaths), 1
			}
			if seen["--tmpfs"] != wantTmpfs || seen["--security-opt"] != wantSecurity || seen["--mount"] != 1 {
				t.Fatalf("%+v: tmpfs %d, security-opt %d, mount %d in %q", spoken, seen["--tmpfs"], seen["--security-opt"], seen["--mount"], args)
			}
			if (seen["--http-proxy=false"] == 1) != (spoken.cli == CLIPodman) {
				t.Fatalf("%+v: --http-proxy=false appears %d times in %q", spoken, seen["--http-proxy=false"], args)
			}
			for i, arg := range args[:imageAt] {
				if arg == "--mount" {
					mount := args[i+1]
					if strings.Count(mount, ",") != 3 || !strings.HasPrefix(mount, "type=bind,source=") {
						t.Fatalf("the mount %q holds options the Orker did not write", mount)
					}
				}
			}
		}
	})
}

// FuzzIsRootless reads the engine's answer to "is it rootless", which the
// engine decides. The claims: the podman CLI's answer is true or false only
// when it says exactly that, and anything else is an error, never "not
// rootless"; the docker CLI's is rootless exactly when its security options
// name it.
func FuzzIsRootless(f *testing.F) {
	for _, seed := range []string{"true\n", "false", " true ", "<no value>", "", "[name=seccomp,profile=builtin name=rootless]", "[name=seccomp,profile=default]"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, answer string) {
		got, err := dialect{cli: CLIPodman}.isRootless(answer)
		switch strings.TrimSpace(answer) {
		case "true":
			if err != nil || !got {
				t.Fatalf("podman isRootless(%q) = %t, %v; want true", answer, got, err)
			}
		case "false":
			if err != nil || got {
				t.Fatalf("podman isRootless(%q) = %t, %v; want false", answer, got, err)
			}
		default:
			if err == nil {
				t.Fatalf("podman isRootless(%q) = %t, nil; an answer that is not true or false is an error", answer, got)
			}
		}
		got, err = dockerDialect.isRootless(answer)
		if err != nil || got != strings.Contains(answer, "name=rootless") {
			t.Fatalf("docker isRootless(%q) = %t, %v", answer, got, err)
		}
	})
}

// FuzzValidSecretName: a secret name is a file name under the secrets
// directory, so it is exactly 1 to 128 of letters, digits, dot, underscore
// and dash, and never a path segment.
func FuzzValidSecretName(f *testing.F) {
	for _, seed := range []string{"db", "DB_PASSWORD", "a.b-c_d", ".", "..", "...", "", "a/b", "a b", "a\x00", "\u00e9", strings.Repeat("a", 128), strings.Repeat("a", 129), "-rf", ".hidden"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		want := name != "" && len(name) <= 128 && name != "." && name != ".."
		for i := 0; want && i < len(name); i++ {
			c := name[i]
			want = c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
		}
		if got := ValidSecretName(name); got != want {
			t.Fatalf("ValidSecretName(%q) = %v, want %v", name, got, want)
		}
		if want && filepath.Base(filepath.Join("/secrets", name)) != name {
			t.Fatalf("the accepted name %q is not one path component", name)
		}
	})
}

// FuzzResolveResources_AnAcceptedRequestIsALimitWithinTheCap: AGENTS.md §6
// says workflows must not receive unlimited resources, and the runtime reads
// a zero limit as exactly that. Whatever the request, the defaults and the
// caps say, a request the Orker accepts renders to a positive limit the
// runtime is handed, no higher than the cap, and the number the attempt
// record carries is that limit; a request it refuses is refused by name.
func FuzzResolveResources_AnAcceptedRequestIsALimitWithinTheCap(f *testing.F) {
	for _, seed := range [][6]string{
		{"2", "4Gi", "4", "8Gi", "1", "1Gi"},
		{"500m", "512Mi", "4", "8Gi", "1", "1Gi"},
		{"", "", "4", "8Gi", "1", "1Gi"},
		{"0", "1Gi", "4", "8Gi", "1", "1Gi"},
		{"1", "0", "4", "8Gi", "1", "1Gi"},
		{"1", "0Gi", "4", "8Gi", "1", "1Gi"},
		{"1", "0.5", "4", "8Gi", "1", "1Gi"},
		{"1", "-1", "4", "8Gi", "1", "1Gi"},
		{"1", "NaN", "4", "8Gi", "1", "1Gi"},
		{"NaN", "1Gi", "4", "8Gi", "1", "1Gi"},
		{"1", "Inf", "4", "8Gi", "1", "1Gi"},
		{"1", "1Gi", "NaN", "8Gi", "1", "1Gi"},
		{"1", "1Gi", "4", "NaN", "1", "1Gi"},
		{"1", "1Gi", "4", "0", "1", "1Gi"},
		{"1", "1Gi", "4", "1e30", "1", "1Gi"},
		{"", "", "4", "8Gi", "0", "0"},
		{"1", "1e18", "4", "9e18", "1", "1Gi"},
		{"1", "9223372036854775808", "4", "1e300", "1", "1Gi"},
		{" 2 ", " 4Gi ", "4", "8Gi", "1", "1Gi"},
		{"0x1p1", "0x10", "4", "8Gi", "1", "1Gi"},
		{"1e-9", "1", "4", "8Gi", "1", "1Gi"},
		{"4", "8Gi", "4", "8Gi", "1", "1Gi"},
		{"4000m", "8192Mi", "4", "8Gi", "1", "1Gi"},
	} {
		f.Add(seed[0], seed[1], seed[2], seed[3], seed[4], seed[5])
	}
	f.Fuzz(func(t *testing.T, cpu, memory, maxCPU, maxMemory, defaultCPU, defaultMemory string) {
		policy := DefaultSecurityPolicy()
		policy.MaxCPU, policy.MaxMemory = maxCPU, maxMemory
		policy.DefaultCPU, policy.DefaultMemory = defaultCPU, defaultMemory
		request := &orkstackv1.ResourceRequest{Cpu: cpu, Memory: memory}

		gotCPU, gotMemory, failure := policy.ResolveResources(request)
		reserved := policy.ReservedResources(request)
		if failure != nil {
			if failure.Reason != ReasonResourceRefused || failure.Message == "" {
				t.Fatalf("a refused request failed as %q: %q", failure.Reason, failure.Message)
			}
			if reserved != nil {
				t.Fatalf("a refused request reserved %v", reserved)
			}
			return
		}

		capCPU, err := quantity.ParseCPU(policy.MaxCPU)
		if err != nil || !quantity.PositiveFinite(capCPU) {
			t.Fatalf("accepted cpu %q under the cap %q, which is not a limit (%v)", gotCPU, policy.MaxCPU, err)
		}
		capBytes, err := quantity.ParseMemory(policy.MaxMemory)
		if err != nil || capBytes < 1 {
			t.Fatalf("accepted memory %q under the cap %q, which is not a limit (%v)", gotMemory, policy.MaxMemory, err)
		}

		renderedCPU, err := strconv.ParseFloat(dockerCPUs(gotCPU), 64)
		if err != nil || !quantity.PositiveFinite(renderedCPU) || renderedCPU > capCPU {
			t.Fatalf("cpu %q renders as %q, not a positive limit within %q (%v)", gotCPU, dockerCPUs(gotCPU), policy.MaxCPU, err)
		}
		renderedBytes, err := strconv.ParseInt(dockerMemory(gotMemory), 10, 64)
		if err != nil || renderedBytes < 1 || float64(renderedBytes) > capBytes {
			t.Fatalf("memory %q renders as %q, not a positive byte count within %q (%v)", gotMemory, dockerMemory(gotMemory), policy.MaxMemory, err)
		}
		if reserved == nil || reserved.GetCpu() != renderedCPU || reserved.GetMemoryBytes() != renderedBytes {
			t.Fatalf("the record holds %v for a container given %v cpus and %d bytes", reserved, renderedCPU, renderedBytes)
		}

		spec := validSpec()
		spec.CPUs, spec.Memory = gotCPU, gotMemory
		args, err := dockerCreateArgs(spec)
		if err != nil {
			t.Fatalf("an accepted request, cpu %q memory %q, does not render: %v", gotCPU, gotMemory, err)
		}
		for i, arg := range args {
			switch arg {
			case "--cpus":
				if args[i+1] != dockerCPUs(gotCPU) {
					t.Fatalf("--cpus %q is not the rendering of %q", args[i+1], gotCPU)
				}
			case "--memory":
				if args[i+1] != dockerMemory(gotMemory) {
					t.Fatalf("--memory %q is not the rendering of %q", args[i+1], gotMemory)
				}
			}
		}
	})
}

// FuzzQuantities_ARenderedLimitIsTheQuantityTheCapsWereCheckedWith: the
// quantity parsers and the runtime's rendering of them agree on the number,
// so the number a cap is compared against is the number the container gets.
// A spec the runtime boundary renders never carries a quantity that parses to
// something the daemon reads as unlimited.
func FuzzQuantities_ARenderedLimitIsTheQuantityTheCapsWereCheckedWith(f *testing.F) {
	for _, seed := range []string{
		"", "0", "1", "2", "0.5", "500m", "0m", "1500m", "4Gi", "512Mi", "1Ki", "1Ti", "1k", "1K", "1M", "2G", "1T", "4GB", "lots", "NaN", "Inf", "-Inf",
		"-1", "-1Gi", "1e3", "1e3Gi", "1e30", "0x10", "0x1p-2", "1_000", " 4Gi ", "4 Gi", "9223372036854775807", "9223372036854775808", "1e-9", ".5", "5.",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		cpus, cpuErr := quantity.ParseCPU(raw)
		bytes, memErr := quantity.ParseMemory(raw)

		if cpuErr == nil {
			rendered := dockerCPUs(raw)
			if quantity.PositiveFinite(cpus) {
				if back, err := strconv.ParseFloat(rendered, 64); err != nil || back != cpus {
					t.Fatalf("cpu %q is %v and renders as %q (%v)", raw, cpus, rendered, err)
				}
			} else if rendered != raw {
				t.Fatalf("cpu %q is %v, no limit, yet renders as %q", raw, cpus, rendered)
			}
			if cpuReadsAsUnlimited(raw) == quantity.PositiveFinite(cpus) {
				t.Fatalf("cpuReadsAsUnlimited(%q) disagrees with the number %v", raw, cpus)
			}
		}
		if memErr == nil {
			rendered := dockerMemory(raw)
			if quantity.ByteCount(bytes) {
				if n, err := strconv.ParseInt(rendered, 10, 64); err != nil || n < 1 || float64(n) > bytes || bytes-float64(n) >= 1 {
					t.Fatalf("memory %q is %v bytes and renders as %q (%v)", raw, bytes, rendered, err)
				}
			} else if rendered != raw {
				t.Fatalf("memory %q is %v bytes, no limit, yet renders as %q", raw, bytes, rendered)
			}
			if memoryReadsAsUnlimited(raw) == quantity.ByteCount(bytes) {
				t.Fatalf("memoryReadsAsUnlimited(%q) disagrees with the number %v", raw, bytes)
			}
		}

		spec := validSpec()
		spec.CPUs, spec.Memory = raw, raw
		if _, err := dockerCreateArgs(spec); err == nil && (cpuErr == nil && !quantity.PositiveFinite(cpus) || memErr == nil && !quantity.ByteCount(bytes)) {
			t.Fatalf("dockerCreateArgs rendered cpus and memory %q, which the runtime reads as unlimited", raw)
		}
	})
}

// FuzzBootProbeFacts parses what the boot probe's container printed, which
// the container, not OrkStack, decides. The claims: no output panics the
// parser or the judgement, and output the judgement passes really does say
// the posture §9.2 requires — no privilege gain, a seccomp filter, no
// capability, and a read-only root, /run, /var/tmp and input.
func FuzzBootProbeFacts(f *testing.F) {
	good := GoodBootProbeFacts(DefaultSecurityPolicy())
	f.Add("@@nonce\nn\n" + good)
	f.Add("@@nonce\nn\n" + strings.Replace(good, "Seccomp:\t2", "Seccomp:\t0", 1))
	f.Add("@@nonce\nn\n" + strings.Replace(good, "tmpfs /run tmpfs ro,", "tmpfs /run tmpfs rw,", 1))
	f.Add("@@nonce\nn\n" + strings.Replace(good, "@@cgroup\n0::/\n", "@@cgroup\n12:pids:/x\n0::/\n", 1))
	f.Add("@@status\nCapBnd:\tzz\n@@mounts\n/ / x ro 0 0\n\\0777 \\ x ro\n@@cpu.max\n1 0")
	f.Add("")
	want := postureWant{cpus: 1, memoryBytes: 1 << 30, pids: DefaultPidsLimit, tmp: ContainerScratchTmp}
	f.Fuzz(func(t *testing.T, stdout string) {
		facts := parseProbeFacts(stdout)
		violations, _ := facts.postureViolations(want)
		if len(violations) > 0 {
			return
		}
		status := map[string]string{}
		for _, line := range facts["status"] {
			if key, value, ok := strings.Cut(line, ":"); ok {
				status[key] = strings.TrimSpace(value)
			}
		}
		if status["NoNewPrivs"] != "1" || status["Seccomp"] != "2" {
			t.Fatalf("passed a posture with NoNewPrivs %q and Seccomp %q", status["NoNewPrivs"], status["Seccomp"])
		}
		for _, set := range []string{"CapEff", "CapPrm", "CapBnd"} {
			if strings.Trim(status[set], "0") != "" || status[set] == "" {
				t.Fatalf("passed a posture whose %s is %q", set, status[set])
			}
		}
		mounts := parseMounts(facts["mounts"])
		for _, path := range []string{"/", "/run", "/var/tmp", ContainerInput} {
			if options, ok := mounts.effective(path); !ok || !options["ro"] {
				t.Fatalf("passed a posture where %s is not read-only", path)
			}
		}
	})
}

// FuzzSignalCauses reads the ready file the boot probe's SIGTERM workload
// wrote, which the container, not OrkStack, decides. The claims: no file
// panics it; it always names at least one cause; its first cause is SIGTERM
// ignored exactly when the SigIgn mask it was given parses with SIGTERM's bit
// set; and
// a stacked label is quoted, so what the workload printed reaches an operator
// as data.
func FuzzSignalCauses(f *testing.F) {
	f.Add("@@label\nunconfined\n@@signals\nSigBlk:\t0000000000000000\nSigIgn:\t0000000000000000\nSigCgt:\t0000000000004000\n")
	f.Add("@@label\ncontainers-default-0.66.0//&crun (enforce)\n@@signals\nSigIgn:\t0000000000004004\n")
	f.Add("@@label\ncat: can't open '/proc/7/attr/current': Invalid argument\n@@signals\nSigBlk:\t0000000000004000\n")
	f.Add("@@label\nsystem_u:system_r:container_t:s0:c1,c2\n@@signals\nSigCgt:\tzz\n")
	f.Add("@@signals\nSigIgn:\t\"//&\"\n@@label\n\"x//&y\n")
	f.Add("@@label\nSIGTERM ignored (SigIgn 1)//&x\n@@signals\nSigIgn:\t0\n")
	f.Add("")
	f.Fuzz(func(t *testing.T, ready string) {
		facts := parseProbeFacts(ready)
		causes := signalCauses(facts)
		if len(causes) == 0 {
			t.Fatal("named no cause")
		}
		joined := strings.Join(causes, "; ")
		ignored := false
		for _, line := range facts["signals"] {
			if key, value, ok := strings.Cut(line, ":"); ok && key == "SigIgn" {
				parsed, err := strconv.ParseUint(strings.TrimSpace(value), 16, 64)
				ignored = err == nil && parsed&sigtermBit != 0
			}
		}
		// The masks' cause comes first, before anything quoted from the
		// workload's label.
		if got := strings.HasPrefix(causes[0], "its workload started with SIGTERM ignored (SigIgn "); got != ignored {
			t.Fatalf("named SIGTERM ignored %t for masks %q; want %t: %s", got, facts["signals"], ignored, joined)
		}
		label := strings.TrimSpace(strings.Join(facts["label"], " "))
		if strings.Contains(label, "//&") && !strings.Contains(joined, strconv.Quote(label)) {
			t.Fatalf("did not quote the stacked label %q: %s", label, joined)
		}
	})
}

// FuzzEngineFromComponents reads `version --format {{json .Server.Components}}`,
// which the engine behind the docker CLI decides. The claims: an answer that
// is not a JSON array of components is an error, never an engine; one that
// decodes names Podman exactly when a component's name says Podman, in any
// case, and Docker otherwise.
func FuzzEngineFromComponents(f *testing.F) {
	f.Add(`[{"Name":"Engine","Version":"29.9.2"},{"Name":"containerd"}]`)
	f.Add(`[{"Name":"Podman Engine","Version":"5.8.8"},{"Name":"Conmon"}]`)
	f.Add(`[{"Name":"PODMAN"}]`)
	f.Add(`null`)
	f.Add(`Cannot connect to the Docker daemon`)
	f.Add(`[{"Name":1}]`)
	f.Fuzz(func(t *testing.T, raw string) {
		engine, err := engineFromComponents(raw)
		var components []struct {
			Name string `json:"Name"`
		}
		decodes := json.Unmarshal([]byte(strings.TrimSpace(raw)), &components) == nil
		if !decodes {
			if err == nil {
				t.Fatalf("engineFromComponents(%q) = %q; an answer that does not decode is not an engine", raw, engine)
			}
			return
		}
		if err != nil {
			t.Fatalf("engineFromComponents(%q) = %v; it decodes", raw, err)
		}
		saysPodman := false
		for _, c := range components {
			saysPodman = saysPodman || strings.Contains(strings.ToLower(c.Name), "podman")
		}
		if want := map[bool]string{true: CLIPodman, false: CLIDocker}[saysPodman]; engine != want {
			t.Fatalf("engineFromComponents(%q) = %q; want %q", raw, engine, want)
		}
	})
}
