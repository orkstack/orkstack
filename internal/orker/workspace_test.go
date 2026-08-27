package orker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func testLimits() *orkstackv1.WorkspaceLimits {
	return &orkstackv1.WorkspaceLimits{
		ValuesMaxBytes:   262144,
		ArtifactMaxCount: 256,
		ArtifactMaxBytes: 5368709120,
		LogMaxBytes:      16777216,
		CommitMaxBytes:   4194304,
	}
}

func newTestWorkspace(t *testing.T) *Workspace {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, ModeAttempt); err != nil {
		t.Fatalf("chmod attempt root: %v", err)
	}
	ws, err := NewWorkspace(root)
	if err != nil {
		t.Fatalf("NewWorkspace: %v", err)
	}
	// Through Workspace.Remove, which is the only thing that CAN delete a
	// sealed tree — t.TempDir's own RemoveAll cannot, and that is how the
	// missing unseal was found in the first place.
	t.Cleanup(func() {
		if err := ws.Remove(); err != nil {
			t.Errorf("Workspace.Remove left the attempt tree behind: %v", err)
		}
	})
	return ws
}

func modeOf(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Mode().Perm() | (info.Mode() & os.ModeSticky)
}

// TestWorkspace_PermissionsMatchTheLayout is §3.1's table, held on disk.
//
// The two halves are load-bearing together. output/ and tmp/ are 1777 because
// the workload may run as ANY uid the image chooses — the Orker does not force
// --user by default — so every directory it must write is world-writable with
// the sticky bit. That is only safe because the per-attempt root is 0700 and
// owned by the Orker's user, so no other host user can traverse into a 1777
// child.
func TestWorkspace_PermissionsMatchTheLayout(t *testing.T) {
	ws := newTestWorkspace(t)

	if got := modeOf(t, ws.Root); got != ModeAttempt {
		t.Errorf("the per-attempt root is %04o, want %04o — 0700 is what makes the 1777 "+
			"children safe from other host users", got, ModeAttempt)
	}
	for _, dir := range []struct {
		name string
		path string
	}{
		{"output", ws.Output},
		{"output/artifacts", ws.Artifacts},
		{"tmp", ws.Tmp},
	} {
		if got := modeOf(t, dir.path); got != ModeWritableDir {
			t.Errorf("%s is %04o, want %04o — the workload may run as any uid and must be "+
				"able to create here", dir.name, got, ModeWritableDir)
		}
	}
}

// TestWorkspace_SealMakesInputReadOnlyBeforeTheContainerStarts covers the
// other half of §3.1: input/ is 0555 and every file in it 0444 while the
// workload runs.
//
// Sealing is not tidiness. §7.1 pulls the image BEFORE rendering precisely so
// task.json can be written once and sealed once — rewriting an input file
// after the pull would make a sealed file mutable during setup.
func TestWorkspace_SealMakesInputReadOnlyBeforeTheContainerStarts(t *testing.T) {
	ws := newTestWorkspace(t)
	if err := ws.WriteConfigJSON(map[string]any{"command": "echo hi"}); err != nil {
		t.Fatalf("WriteConfigJSON: %v", err)
	}
	if err := ws.WriteRunJSON(map[string]any{"url": "https://example.invalid"}); err != nil {
		t.Fatalf("WriteRunJSON: %v", err)
	}
	if err := ws.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	if got := modeOf(t, ws.Input); got != ModeInputDir {
		t.Errorf("input/ is %04o, want %04o", got, ModeInputDir)
	}
	for _, name := range []string{"config.json", "run.json"} {
		if got := modeOf(t, filepath.Join(ws.Input, name)); got != ModeInputFile {
			t.Errorf("input/%s is %04o, want %04o", name, got, ModeInputFile)
		}
	}
}

// TestWorkspace_TaskJSONMatchesTheSchema walks the §3.2 table.
//
// task.json is a CONTRACT: within one schemaVersion the platform must not
// remove a field, retype one, or change a meaning. Every field a workload
// could otherwise want from an environment variable lives here, because
// environment variables are not part of the ABI (AGENTS.md §3).
func TestWorkspace_TaskJSONMatchesTheSchema(t *testing.T) {
	ws := newTestWorkspace(t)
	in := &orkstackv1.ExecuteTaskInput{
		RunId:                          "ork-1786837143070480000-1",
		OrkflowName:                    "deploy",
		OrkflowVersion:                 "1.4.0",
		OrkflowDefinitionDigest:        "sha256:" + strings.Repeat("6", 64),
		Namespace:                      "default",
		TaskId:                         "migrate",
		IdempotencyKey:                 "default/ork-1786837143070480000-1/migrate",
		TimeoutSeconds:                 300,
		CancellationGracePeriodSeconds: 30,
		Limits:                         testLimits(),
	}
	image := &orkstackv1.ImageRef{
		Reference:      "ghcr.io/acme/migrate:v3",
		Pinned:         "ghcr.io/acme/migrate@sha256:" + strings.Repeat("9", 64),
		IndexDigest:    "sha256:" + strings.Repeat("9", 64),
		PlatformDigest: "sha256:" + strings.Repeat("4", 64),
		Platform:       "linux/arm64",
		Delivery:       DeliveryRegistry,
	}
	if err := ws.WriteTaskJSON(in, TaskContext{
		Attempt: 2, Pool: "main", Namespace: "default",
		EffectiveCPU: "2", EffectiveMem: "4Gi", Network: NetworkNone, Image: image,
	}); err != nil {
		t.Fatalf("WriteTaskJSON: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(ws.Input, "task.json"))
	if err != nil {
		t.Fatalf("read task.json: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("task.json is not valid JSON: %v\n%s", err, raw)
	}

	// Required scalars, by their §3.2 paths.
	required := map[string]any{
		"schemaVersion":  float64(TaskJSONSchemaVersion),
		"idempotencyKey": "default/ork-1786837143070480000-1/migrate",
		"network":        NetworkNone,
	}
	for path, want := range required {
		if got := doc[path]; got != want {
			t.Errorf("task.json[%q] = %v, want %v", path, got, want)
		}
	}

	// All eight path keys are always present, so a workload that reads them
	// never hardcodes a path.
	paths, _ := doc["paths"].(map[string]any)
	for _, key := range []string{"input", "config", "runInput", "secrets", "output", "values", "artifacts", "tmp"} {
		if paths[key] == nil {
			t.Errorf("task.json paths.%s is absent; all eight keys are required", key)
		}
	}

	// Arrays are never null. A workload distinguishing "none" from "absent"
	// would otherwise have to special-case null.
	inputs, _ := doc["inputs"].(map[string]any)
	for _, key := range []string{"artifacts", "upstream"} {
		value, present := inputs[key]
		if !present {
			t.Errorf("task.json inputs.%s is absent, want []", key)
			continue
		}
		if value == nil {
			t.Errorf("task.json inputs.%s is null; the orker emits [] instead", key)
		}
	}
	if value, present := doc["secrets"]; !present || value == nil {
		t.Errorf("task.json secrets = %v, want []", value)
	}

	// The image block carries what the PULL resolved, which is only possible
	// because §7.1 pulls before it renders.
	imageDoc, _ := doc["image"].(map[string]any)
	if imageDoc["platformDigest"] == nil || imageDoc["platform"] == nil || imageDoc["delivery"] == nil {
		t.Errorf("task.json image = %v; platformDigest, platform and delivery exist only "+
			"after the image is resolved, which is why IMAGE comes before RENDER", imageDoc)
	}

	// An absent optional scalar is OMITTED, never written as "".
	if _, present := doc["task"].(map[string]any)["runAsOrkerUser"]; present {
		t.Error("task.runAsOrkerUser is present when false; absence is how it says 'no'")
	}
}

// TestWorkspace_TaskJSONOmitsAbsentOptionalsAndKeepsRequiredZeroes is the
// distinction §3.2 draws explicitly: a required field with an empty form keeps
// its zero, and an optional one disappears.
func TestWorkspace_TaskJSONOmitsAbsentOptionalsAndKeepsRequiredZeroes(t *testing.T) {
	ws := newTestWorkspace(t)
	in := &orkstackv1.ExecuteTaskInput{
		RunId: "r", OrkflowName: "o", OrkflowVersion: "1", TaskId: "t",
		Limits: testLimits(),
	}
	if err := ws.WriteTaskJSON(in, TaskContext{
		Attempt: 1, Pool: "main", Namespace: "default", Network: NetworkDefault,
		Image: &orkstackv1.ImageRef{Reference: "shell", Pinned: "x@sha256:y", IndexDigest: "sha256:y"},
	}); err != nil {
		t.Fatalf("WriteTaskJSON: %v", err)
	}
	raw, _ := os.ReadFile(filepath.Join(ws.Input, "task.json"))
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// resources is optional and this task requested none, so it is absent
	// rather than an empty object.
	if _, present := doc["resources"]; present {
		t.Error("task.json carries an empty resources block; absent means orker default")
	}
	image, _ := doc["image"].(map[string]any)
	for _, key := range []string{"platformDigest", "platform", "delivery"} {
		if _, present := image[key]; present {
			t.Errorf("image.%s is present but was never resolved; it must be omitted", key)
		}
	}
	// limits.* are all REQUIRED, so every one is present even at its zero.
	limits, _ := doc["limits"].(map[string]any)
	for _, key := range []string{
		"timeoutSeconds", "cancellationGracePeriodSeconds", "valuesMaxBytes",
		"artifactMaxCount", "artifactMaxBytes", "logMaxBytes", "commitMaxBytes",
	} {
		if _, present := limits[key]; !present {
			t.Errorf("limits.%s is absent; every limit is required so a workload can respect "+
				"it instead of discovering it by failing", key)
		}
	}
}

// TestScanOutputArtifacts_EnforcesTheOutputRules walks §3.5.
func TestScanOutputArtifacts_EnforcesTheOutputRules(t *testing.T) {
	tests := []struct {
		name       string
		build      func(t *testing.T, dir string)
		wantReason string
		wantNames  []string
	}{
		{
			name: "regular files are artifacts, ordered by name bytes",
			build: func(t *testing.T, dir string) {
				for _, name := range []string{"zeta.txt", "alpha.txt", "Mid.txt"} {
					mustWrite(t, filepath.Join(dir, name), "x")
				}
			},
			// Ascending by BYTES, so uppercase sorts before lowercase. Two
			// attempts of the same computation must produce the same commit
			// ordering.
			wantNames: []string{"Mid.txt", "alpha.txt", "zeta.txt"},
		},
		{
			name:      "an absent directory is not a violation",
			build:     func(t *testing.T, dir string) { _ = os.RemoveAll(dir) },
			wantNames: nil,
		},
		{
			name: "a nested directory is refused rather than walked",
			build: func(t *testing.T, dir string) {
				if err := os.MkdirAll(filepath.Join(dir, "nested"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: ReasonArtifactNotAFile,
		},
		{
			name: "a symlink is refused",
			build: func(t *testing.T, dir string) {
				mustWrite(t, filepath.Join(dir, "real"), "x")
				if err := os.Symlink(filepath.Join(dir, "real"), filepath.Join(dir, "link")); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: ReasonArtifactNotAFile,
		},
		{
			name: "a FIFO is refused",
			build: func(t *testing.T, dir string) {
				mustFIFO(t, filepath.Join(dir, "pipe"))
			},
			wantReason: ReasonArtifactNotAFile,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "artifacts")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			tc.build(t, dir)

			found, failure := ScanOutputArtifacts(dir, testLimits())
			if tc.wantReason != "" {
				if failure == nil {
					t.Fatalf("scan succeeded with %v; want %s", found, tc.wantReason)
				}
				if failure.Reason != tc.wantReason {
					t.Errorf("reason = %q, want %q (%s)", failure.Reason, tc.wantReason, failure.Message)
				}
				return
			}
			if failure != nil {
				t.Fatalf("scan failed: %v", failure)
			}
			var names []string
			for _, artifact := range found {
				names = append(names, artifact.Name)
			}
			if strings.Join(names, ",") != strings.Join(tc.wantNames, ",") {
				t.Errorf("scanned %v, want %v", names, tc.wantNames)
			}
		})
	}
}

// TestScanOutputArtifacts_EnforcesTheLimits covers the count and size bounds
// separately, because each has its own failure and neither should be
// discovered after the bandwidth is spent.
func TestScanOutputArtifacts_EnforcesTheLimits(t *testing.T) {
	t.Run("too many artifacts", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "artifacts")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for i := range 5 {
			mustWrite(t, filepath.Join(dir, string(rune('a'+i))), "x")
		}
		limits := testLimits()
		limits.ArtifactMaxCount = 3

		_, failure := ScanOutputArtifacts(dir, limits)
		if failure == nil || failure.Reason != ReasonArtifactLimit {
			t.Fatalf("scan = %v, want %s", failure, ReasonArtifactLimit)
		}
	})

	t.Run("one artifact over the per-artifact size", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "artifacts")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, filepath.Join(dir, "big"), strings.Repeat("x", 100))
		limits := testLimits()
		limits.ArtifactMaxBytes = 10

		_, failure := ScanOutputArtifacts(dir, limits)
		if failure == nil || failure.Reason != ReasonArtifactLimit {
			t.Fatalf("scan = %v, want %s", failure, ReasonArtifactLimit)
		}
	})
}

// TestReadValues_EnforcesTheValuesContract walks §3.4.
func TestReadValues_EnforcesTheValuesContract(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		absent     bool
		limit      int64
		wantReason string
	}{
		{name: "an object is accepted", content: `{"ok":true}`},
		{name: "an empty object is the way to say 'no result'", content: `{}`},
		{name: "absent after exit 0", absent: true, wantReason: ReasonValuesMissing},
		{name: "an array is not an object", content: `[1,2]`, wantReason: ReasonValuesMalformed},
		{name: "a scalar is not an object", content: `42`, wantReason: ReasonValuesMalformed},
		{name: "null is not an object", content: `null`, wantReason: ReasonValuesMalformed},
		{name: "malformed json", content: `{"a":`, wantReason: ReasonValuesMalformed},
		{name: "over the limit", content: `{"a":"bbbbbbbbbb"}`, limit: 5, wantReason: ReasonValuesTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "values.json")
			if !tc.absent {
				mustWrite(t, path, tc.content)
			}
			limits := testLimits()
			if tc.limit > 0 {
				limits.ValuesMaxBytes = tc.limit
			}

			raw, failure := ReadValues(path, limits)
			if tc.wantReason != "" {
				if failure == nil {
					t.Fatalf("ReadValues succeeded with %q; want %s", raw, tc.wantReason)
				}
				if failure.Reason != tc.wantReason {
					t.Errorf("reason = %q, want %q (%s)", failure.Reason, tc.wantReason, failure.Message)
				}
				return
			}
			if failure != nil {
				t.Fatalf("ReadValues: %v", failure)
			}
			// The bytes are returned VERBATIM. The Orker parses to validate
			// and never re-serializes, so the committed sha256 is the sha256
			// of exactly what the workload wrote.
			if string(raw) != tc.content {
				t.Errorf("ReadValues returned %q, want the workload's exact bytes %q", raw, tc.content)
			}
		})
	}
}

// TestReadValues_PreservesKeyOrderAndLargeIntegersExactly is the property that
// forced values_json to be `bytes` rather than a Struct-backed Payload.
//
// A Struct round trip reorders keys and SILENTLY CORRUPTS an integer outside
// float64's exact range (§14.9). Returning the file's bytes is what makes the
// committed digest mean anything.
func TestReadValues_PreservesKeyOrderAndLargeIntegersExactly(t *testing.T) {
	const exact = `{"zebra":1,"alpha":2,"big":12345678901234567890}`
	path := filepath.Join(t.TempDir(), "values.json")
	mustWrite(t, path, exact)

	raw, failure := ReadValues(path, testLimits())
	if failure != nil {
		t.Fatalf("ReadValues: %v", failure)
	}
	if string(raw) != exact {
		t.Fatalf("ReadValues normalized the workload's bytes:\n got %s\nwant %s", raw, exact)
	}
	if !strings.Contains(string(raw), "12345678901234567890") {
		t.Error("the large integer was corrupted; this is exactly why values_json is bytes")
	}
	if strings.Index(string(raw), "zebra") > strings.Index(string(raw), "alpha") {
		t.Error("key order was not preserved")
	}
}

// TestCheckReadable_NamesTheFileItsModeAndTheFix is §3.1's detection measure.
//
// stat succeeds even when open fails, which is what lets the message always be
// specific. The message has to carry the fix, because the person reading it is
// the image author and `umask 022` is the one-line answer.
func TestCheckReadable_NamesTheFileItsModeAndTheFix(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root, which can open any mode; the check is about a non-root Orker")
	}
	path := filepath.Join(t.TempDir(), "values.json")
	mustWrite(t, path, `{}`)
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	failure := CheckReadable(path, ContainerValuesJSON)
	if failure == nil {
		t.Fatal("CheckReadable accepted a file it cannot open")
	}
	if failure.Reason != ReasonOutputNotReadable {
		t.Errorf("reason = %q, want %q", failure.Reason, ReasonOutputNotReadable)
	}
	// ContractViolation, never ARTIFACT_UPLOAD_FAILED: the image will do the
	// same thing on every attempt, and classifying it as retryable
	// infrastructure would loop forever.
	if failure.Category() != CategoryContractViolation {
		t.Errorf("category = %q, want %q — a retryable classification would loop against "+
			"deterministic image behavior", failure.Category(), CategoryContractViolation)
	}
	for _, want := range []string{ContainerValuesJSON, "mode=", "umask 022", "runAsOrkerUser"} {
		if !strings.Contains(failure.Message, want) {
			t.Errorf("the message does not name %q:\n%s", want, failure.Message)
		}
	}
}

// TestCheckReadable_AcceptsAFileItCanOpen is the negative control: the check
// must not fail a workload that behaved.
func TestCheckReadable_AcceptsAFileItCanOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "values.json")
	mustWrite(t, path, `{}`)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if failure := CheckReadable(path, ContainerValuesJSON); failure != nil {
		t.Errorf("CheckReadable rejected a 0644 file: %v", failure)
	}
}

// TestEncodeArtifactName_IsAlwaysPresentAndReversible covers §3.2's nameB64.
//
// It exists because `path` is produced by safeComponent, which rewrites
// separators, dot segments and long names, so it CANNOT carry the logical
// name. A workload must never have to reverse that rule.
func TestEncodeArtifactName_IsAlwaysPresentAndReversible(t *testing.T) {
	for _, name := range []string{
		"dist.tar.gz",
		"reports/summary.txt",
		"../escape",
		"with space",
		"日本語のファイル",
		"",
	} {
		encoded := EncodeArtifactName(name)
		decoded, err := decodeRawURL(encoded)
		if err != nil {
			t.Errorf("nameB64 for %q does not decode: %v", name, err)
			continue
		}
		if decoded != name {
			t.Errorf("nameB64 round trip for %q gave %q", name, decoded)
		}
	}
}

// TestValidSecretName_IsValidatedNeverSanitized covers §3.2's grammar. A
// sanitized name is a name the author did not write, and this one becomes a
// path segment under /orkstack/input/secrets.
func TestValidSecretName_IsValidatedNeverSanitized(t *testing.T) {
	valid := []string{"db_password", "TOKEN", "a.b-c_1", strings.Repeat("a", 128)}
	for _, name := range valid {
		if !ValidSecretName(name) {
			t.Errorf("ValidSecretName(%q) = false, want true", name)
		}
	}
	invalid := []string{"", ".", "..", "a/b", "a b", "a\x00b", strings.Repeat("a", 129), "../etc/passwd"}
	for _, name := range invalid {
		if ValidSecretName(name) {
			t.Errorf("ValidSecretName(%q) = true; it would become a path segment", name)
		}
	}
}

// TestSplitSecretSource_SeparatesSchemeFromLocator is what the policy check
// keys on. A source with no scheme must not be read as a bare file path.
func TestSplitSecretSource_SeparatesSchemeFromLocator(t *testing.T) {
	tests := []struct{ source, scheme, locator string }{
		{"env:DB_PASSWORD", "env", "DB_PASSWORD"},
		{"file:/etc/orkstack/secrets/db", "file", "/etc/orkstack/secrets/db"},
		{"/etc/passwd", "", "/etc/passwd"},
		{"vault:kv/data/db", "vault", "kv/data/db"},
	}
	for _, tc := range tests {
		scheme, locator := SplitSecretSource(tc.source)
		if scheme != tc.scheme || locator != tc.locator {
			t.Errorf("SplitSecretSource(%q) = (%q, %q), want (%q, %q)",
				tc.source, scheme, locator, tc.scheme, tc.locator)
		}
	}
}
