package orker

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
	f.Fuzz(func(t *testing.T, content []byte, max int64) {
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
		limits := &orkstackv1.WorkspaceLimits{ValuesMaxBytes: max}
		raw, failure := ReadValues(path, limits)
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
	}
	f.Fuzz(func(t *testing.T, image, labelKey, labelValue, source, destination, command string) {
		spec := validSpec()
		spec.Image = image
		spec.Labels = map[string]string{"ork.run": labelValue, "ork.task": "build", labelKey: labelValue}
		spec.Env = map[string]string{labelKey: labelValue}
		spec.Mounts = []Mount{{Source: source, Destination: destination, ReadOnly: true}}
		spec.Command = []string{command, "-c", "--privileged"}
		args, err := dockerCreateArgs(spec)
		if err != nil {
			if err.Error() == "" {
				t.Fatalf("dockerCreateArgs refused with no reason")
			}
			return
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
				t.Fatalf("argument %d, %q, is not a flag of the security profile: %q", i, args[i], args)
			}
			seen[args[i]]++
			i += 1 + values
			if i > imageAt {
				t.Fatalf("flag %q is missing its value in %q", args[i-1-values], args)
			}
		}
		for _, once := range []string{"--name", "--read-only", "--cap-drop=ALL", "--init", "--workdir", "--network", "--cpus", "--memory", "--pids-limit", "--tmpfs"} {
			if seen[once] != 1 {
				t.Fatalf("%s appears %d times in %q", once, seen[once], args)
			}
		}
		if seen["--security-opt"] != 2 || seen["--mount"] != 1 {
			t.Fatalf("security-opt %d, mount %d in %q", seen["--security-opt"], seen["--mount"], args)
		}
		for i, arg := range args[:imageAt] {
			if arg == "--mount" {
				mount := args[i+1]
				if strings.Count(mount, ",") != 3 || !strings.HasPrefix(mount, "type=bind,source=") {
					t.Fatalf("the mount %q holds options the Orker did not write", mount)
				}
			}
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

		capCPU, err := parseCPU(policy.MaxCPU)
		if err != nil || !isPositiveFinite(capCPU) {
			t.Fatalf("accepted cpu %q under the cap %q, which is not a limit (%v)", gotCPU, policy.MaxCPU, err)
		}
		capBytes, err := parseMemory(policy.MaxMemory)
		if err != nil || capBytes < 1 {
			t.Fatalf("accepted memory %q under the cap %q, which is not a limit (%v)", gotMemory, policy.MaxMemory, err)
		}

		renderedCPU, err := strconv.ParseFloat(dockerCPUs(gotCPU), 64)
		if err != nil || !isPositiveFinite(renderedCPU) || renderedCPU > capCPU {
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
	f.Fuzz(func(t *testing.T, quantity string) {
		cpus, cpuErr := parseCPU(quantity)
		bytes, memErr := parseMemory(quantity)

		if cpuErr == nil {
			rendered := dockerCPUs(quantity)
			if isPositiveFinite(cpus) {
				if back, err := strconv.ParseFloat(rendered, 64); err != nil || back != cpus {
					t.Fatalf("cpu %q is %v and renders as %q (%v)", quantity, cpus, rendered, err)
				}
			} else if rendered != quantity {
				t.Fatalf("cpu %q is %v, no limit, yet renders as %q", quantity, cpus, rendered)
			}
			if cpuReadsAsUnlimited(quantity) == isPositiveFinite(cpus) {
				t.Fatalf("cpuReadsAsUnlimited(%q) disagrees with the number %v", quantity, cpus)
			}
		}
		if memErr == nil {
			rendered := dockerMemory(quantity)
			if isByteCount(bytes) {
				if n, err := strconv.ParseInt(rendered, 10, 64); err != nil || n < 1 || float64(n) > bytes || bytes-float64(n) >= 1 {
					t.Fatalf("memory %q is %v bytes and renders as %q (%v)", quantity, bytes, rendered, err)
				}
			} else if rendered != quantity {
				t.Fatalf("memory %q is %v bytes, no limit, yet renders as %q", quantity, bytes, rendered)
			}
			if memoryReadsAsUnlimited(quantity) == isByteCount(bytes) {
				t.Fatalf("memoryReadsAsUnlimited(%q) disagrees with the number %v", quantity, bytes)
			}
		}

		spec := validSpec()
		spec.CPUs, spec.Memory = quantity, quantity
		if _, err := dockerCreateArgs(spec); err == nil && (cpuErr == nil && !isPositiveFinite(cpus) || memErr == nil && !isByteCount(bytes)) {
			t.Fatalf("dockerCreateArgs rendered cpus and memory %q, which the runtime reads as unlimited", quantity)
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
