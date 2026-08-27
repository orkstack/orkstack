package orker

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The container-side layout. It is fixed, and it is also written into
// task.json's `paths` so a workload that reads it never hardcodes a path and a
// future layout change stays a data change (§3.1, §3.2).
const (
	ContainerRoot           = "/orkstack"
	ContainerInput          = ContainerRoot + "/input"
	ContainerTaskJSON       = ContainerInput + "/task.json"
	ContainerConfigJSON     = ContainerInput + "/config.json"
	ContainerRunJSON        = ContainerInput + "/run.json"
	ContainerSecrets        = ContainerInput + "/secrets"
	ContainerInputArtifacts = ContainerInput + "/artifacts"
	ContainerUpstream       = ContainerInput + "/upstream"
	ContainerOutput         = ContainerRoot + "/output"
	ContainerValuesJSON     = ContainerOutput + "/values.json"
	ContainerArtifacts      = ContainerOutput + "/artifacts"
	ContainerTmp            = ContainerRoot + "/tmp"
	// ContainerScratchTmp is the small tmpfs at /tmp, separate from the
	// workload's /orkstack/tmp scratch bind. The container root filesystem is
	// read-only (§9.2), so a workload that writes to /tmp out of habit needs
	// somewhere for those bytes to go.
	ContainerScratchTmp = "/tmp"
)

// Workspace file and directory modes (§3.1).
//
// The rationale is one sentence with two halves. The host per-attempt
// directory is 0700 and owned by the Orker's user, so no other host user can
// traverse into a 1777 child; and inside the container the workload may run as
// ANY uid the image chooses — the Orker does not force --user by default — so
// every directory it must write is world-writable with the sticky bit and
// every file it must read is 0444.
const (
	// ModeAttempt is the per-attempt root. 0700 is what makes the 1777
	// children safe.
	ModeAttempt os.FileMode = 0o700
	// ModeInputDir is a read-only input directory.
	ModeInputDir os.FileMode = 0o555
	// ModeInputFile is a read-only input file.
	ModeInputFile os.FileMode = 0o444
	// ModeWritableDir is an output or scratch directory: world-writable with
	// the sticky bit, so any container uid can create in it but only the
	// owner of a file can remove it.
	//
	// The sticky bit is os.ModeSticky and NOT the octal 0o1000. Go's FileMode
	// keeps the type bits above the permission bits, so writing 0o1777 here
	// sets 0777 plus a bit chmod(2) never receives — the directory ends up
	// world-writable with NO sticky bit, and any container uid can delete
	// another's outputs. Caught by TestWorkspace_PermissionsMatchTheLayout.
	ModeWritableDir os.FileMode = 0o777 | os.ModeSticky
)

// TaskJSONSchemaVersion is the major version of task.json this Orker writes.
//
// Within one major version the platform MUST NOT remove a field, retype a
// field, or change a field's meaning; it may only add optional fields. A
// workload that reads a schemaVersion it does not know must fail loudly rather
// than proceed on guessed semantics — a wrong result committed durably is
// worse than a clean failure (§3.2).
const TaskJSONSchemaVersion = 1

// Workspace is one attempt's host-side directory tree.
type Workspace struct {
	// Root is the per-attempt directory, mode 0700.
	Root string
	// Input is bind-mounted read-only at /orkstack/input.
	Input string
	// Secrets is a SEPARATE bind at /orkstack/input/secrets. It is separate so
	// it can be created, populated, mounted read-only and removed on every
	// exit path independently of the rest of the input tree (§3.1, §9.4).
	Secrets string
	// Output is bind-mounted read-write at /orkstack/output.
	Output string
	// Artifacts is Output/artifacts.
	Artifacts string
	// Tmp is bind-mounted read-write at /orkstack/tmp.
	Tmp string
}

// NewWorkspace creates the per-attempt tree with the §3.1 modes.
//
// Directories are created writable and sealed later by Seal, because the Orker
// has to write task.json, config.json, run.json and the staged inputs into a
// tree that the container will then see as 0555/0444.
func NewWorkspace(root string) (*Workspace, error) {
	ws := &Workspace{
		Root:      root,
		Input:     filepath.Join(root, "input"),
		Secrets:   filepath.Join(root, "secrets"),
		Output:    filepath.Join(root, "output"),
		Artifacts: filepath.Join(root, "output", "artifacts"),
		Tmp:       filepath.Join(root, "tmp"),
	}
	for _, dir := range []string{
		ws.Input,
		filepath.Join(ws.Input, "artifacts"),
		filepath.Join(ws.Input, "upstream"),
		// The MOUNTPOINT for the secrets bind, inside the input tree.
		//
		// /orkstack/input/secrets is a separate bind nested inside the
		// /orkstack/input bind (§3.1), and a nested bind needs its target to
		// already exist in the parent's source. Without this directory the
		// runtime tries to create the mountpoint in the container's rootfs,
		// which --read-only makes impossible, and EVERY container is refused
		// before it starts:
		//
		//   error mounting ".../secrets" to rootfs at "/orkstack/input/secrets":
		//   make mountpoint: mkdirat ...: read-only file system
		//
		// Found by TestIntegration_DispatcherRunsAReferenceImageEndToEnd.
		filepath.Join(ws.Input, "secrets"),
		ws.Secrets,
		ws.Output,
		ws.Artifacts,
		ws.Tmp,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create workspace directory %s: %w", dir, err)
		}
	}
	// The output and scratch trees are what the workload writes into, and it
	// may be any uid. Chmod rather than MkdirAll's mode argument, because
	// MkdirAll applies the process umask and 1777 is exactly the mode a
	// restrictive umask would quietly reduce.
	for _, dir := range []string{ws.Output, ws.Artifacts, ws.Tmp} {
		if err := os.Chmod(dir, ModeWritableDir); err != nil {
			return nil, fmt.Errorf("set mode on %s: %w", dir, err)
		}
	}
	return ws, nil
}

// Seal makes the input tree read-only, and is called AFTER every input file is
// written and BEFORE the container starts.
//
// Sealing before the container starts is load-bearing rather than tidy: §3.1's
// permission model depends on input/ being 0555/0444 while the workload runs,
// and it is why §7.1 pulls the image BEFORE rendering — task.json carries
// image.platformDigest, and rewriting an input file after the pull would make
// a sealed file mutable during setup.
func (w *Workspace) Seal() error {
	// Files first, then their directories: a 0555 directory cannot be
	// traversed for a chmod on macOS as reliably as one that is still
	// writable, and sealing depth-last keeps the walk simple.
	var files []string
	var dirs []string
	err := filepath.WalkDir(w.Input, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			dirs = append(dirs, path)
		} else {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("seal input tree: %w", err)
	}
	for _, path := range files {
		if err := os.Chmod(path, ModeInputFile); err != nil {
			return fmt.Errorf("seal input file %s: %w", path, err)
		}
	}
	// Deepest first, so a parent is still traversable while its children are
	// being sealed.
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, path := range dirs {
		if err := os.Chmod(path, ModeInputDir); err != nil {
			return fmt.Errorf("seal input directory %s: %w", path, err)
		}
	}
	return nil
}

// SealSecrets makes the populated secrets directory read-only.
func (w *Workspace) SealSecrets() error {
	entries, err := os.ReadDir(w.Secrets)
	if err != nil {
		return fmt.Errorf("seal secrets: %w", err)
	}
	for _, entry := range entries {
		path := filepath.Join(w.Secrets, entry.Name())
		if err := os.Chmod(path, ModeInputFile); err != nil {
			return fmt.Errorf("seal secret %s: %w", entry.Name(), err)
		}
	}
	return os.Chmod(w.Secrets, ModeInputDir)
}

// Mounts renders the §9.2 bind set. Every source is a path this Orker created
// for this attempt; there is no branch that could produce a host path from a
// definition.
func (w *Workspace) Mounts() []Mount {
	return []Mount{
		{Source: w.Input, Destination: ContainerInput, ReadOnly: true},
		{Source: w.Secrets, Destination: ContainerSecrets, ReadOnly: true},
		{Source: w.Output, Destination: ContainerOutput},
		{Source: w.Tmp, Destination: ContainerTmp},
	}
}

// Remove deletes the whole per-attempt tree.
//
// It exists because os.RemoveAll CANNOT do it. Seal leaves input/ at 0555, and
// unlinking a file needs write and execute on its DIRECTORY, not on the file:
// a plain RemoveAll fails with "permission denied" and silently leaves the
// attempt's workspace — including any staged secret — on disk forever. Caught
// by TestWorkspace_SealMakesInputReadOnlyBeforeTheContainerStarts, whose own
// cleanup could not delete what the test had sealed.
//
// So every directory is made writable on the way down before anything is
// unlinked. Called on EVERY exit path: success, failure, panic and
// cancellation alike (§6.2).
func (w *Workspace) Remove() error {
	_ = filepath.WalkDir(w.Root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			// A path that cannot be walked cannot be chmod'ed either;
			// RemoveAll below reports what actually remains.
			return nil //nolint:nilerr // best-effort unseal; RemoveAll is the real gate
		}
		if entry.IsDir() {
			_ = os.Chmod(path, 0o700)
		}
		return nil
	})
	return os.RemoveAll(w.Root)
}

// RemoveSecrets deletes the per-attempt secrets directory. It is called on
// EVERY exit path — success, failure, panic and cancellation — because secret
// bytes must not outlive the attempt that needed them (§9.4).
func (w *Workspace) RemoveSecrets() error {
	// The directory was sealed 0555, which on some systems prevents unlinking
	// its children. Restore write permission before removing.
	_ = os.Chmod(w.Secrets, 0o700)
	if entries, err := os.ReadDir(w.Secrets); err == nil {
		for _, entry := range entries {
			_ = os.Chmod(filepath.Join(w.Secrets, entry.Name()), 0o600)
		}
	}
	return os.RemoveAll(w.Secrets)
}

// ---------------------------------------------------------------- task.json

// taskJSON is the §3.2 schema. It is spelled out as a struct rather than
// assembled from a map because task.json is a CONTRACT: a field that silently
// changed type or disappeared would break every workload reading it, and the
// table in §3.2 is the specification a reader is entitled to rely on.
//
// Key casing is lowerCamelCase, matching every other JSON surface OrkStack
// emits (docs/JSON.md).
type taskJSON struct {
	SchemaVersion  int              `json:"schemaVersion"`
	IdempotencyKey string           `json:"idempotencyKey"`
	Run            taskJSONRun      `json:"run"`
	Task           taskJSONTask     `json:"task"`
	Image          taskJSONImage    `json:"image"`
	Limits         taskJSONLimits   `json:"limits"`
	Resources      *taskJSONRes     `json:"resources,omitempty"`
	Network        string           `json:"network"`
	Paths          taskJSONPaths    `json:"paths"`
	Inputs         taskJSONInputs   `json:"inputs"`
	Secrets        []taskJSONSecret `json:"secrets"`
}

type taskJSONRun struct {
	ID        string          `json:"id"`
	Namespace string          `json:"namespace"`
	Orkflow   taskJSONOrkflow `json:"orkflow"`
}

type taskJSONOrkflow struct {
	Name             string `json:"name"`
	Version          string `json:"version"`
	DefinitionDigest string `json:"definitionDigest"`
}

type taskJSONTask struct {
	ID      string `json:"id"`
	Attempt int32  `json:"attempt"`
	Pool    string `json:"pool"`
	// RunAsOrkerUser is present and true only when the Orker overrode the
	// image's own USER, so a workload can see WHY its uid is not the one its
	// image declared. omitempty is the "absent = false" the table specifies.
	RunAsOrkerUser bool `json:"runAsOrkerUser,omitempty"`
}

type taskJSONImage struct {
	Reference      string `json:"reference"`
	Pinned         string `json:"pinned"`
	IndexDigest    string `json:"indexDigest"`
	PlatformDigest string `json:"platformDigest,omitempty"`
	Platform       string `json:"platform,omitempty"`
	Delivery       string `json:"delivery,omitempty"`
}

type taskJSONLimits struct {
	TimeoutSeconds                 int32 `json:"timeoutSeconds"`
	CancellationGracePeriodSeconds int32 `json:"cancellationGracePeriodSeconds"`
	ValuesMaxBytes                 int64 `json:"valuesMaxBytes"`
	ArtifactMaxCount               int32 `json:"artifactMaxCount"`
	ArtifactMaxBytes               int64 `json:"artifactMaxBytes"`
	LogMaxBytes                    int64 `json:"logMaxBytes"`
	CommitMaxBytes                 int64 `json:"commitMaxBytes"`
}

type taskJSONRes struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

type taskJSONPaths struct {
	Input     string `json:"input"`
	Config    string `json:"config"`
	RunInput  string `json:"runInput"`
	Secrets   string `json:"secrets"`
	Output    string `json:"output"`
	Values    string `json:"values"`
	Artifacts string `json:"artifacts"`
	Tmp       string `json:"tmp"`
}

type taskJSONInputs struct {
	Artifacts []taskJSONInputArtifact `json:"artifacts"`
	Upstream  []taskJSONUpstream      `json:"upstream"`
}

type taskJSONInputArtifact struct {
	Producer string `json:"producer"`
	// Name is present only when the exact logical name is valid UTF-8.
	Name string `json:"name,omitempty"`
	// NameB64 is ALWAYS present: RawURL base64 of the exact name bytes, and
	// authoritative. It exists because `path` is produced by safeComponent,
	// which rewrites separators, dot segments and long names and therefore
	// cannot carry the logical name — a workload must never have to reverse
	// that rule.
	NameB64     string `json:"nameB64"`
	Path        string `json:"path"`
	SizeBytes   int64  `json:"sizeBytes"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"contentType,omitempty"`
}

type taskJSONUpstream struct {
	TaskID     string `json:"taskId"`
	ValuesPath string `json:"valuesPath"`
}

type taskJSONSecret struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// TaskContext is everything the workspace needs that is not on the wire.
type TaskContext struct {
	Attempt        int32
	Pool           string
	Namespace      string
	RunAsOrkerUser bool
	EffectiveCPU   string
	EffectiveMem   string
	Network        string
	Image          *orkstackv1.ImageRef
	StagedInputs   []taskJSONInputArtifact
	StagedUpstream []taskJSONUpstream
	StagedSecrets  []taskJSONSecret
}

// WriteTaskJSON renders /orkstack/input/task.json.
//
// Every field a workload could otherwise want from an environment variable
// lives here, because environment variables are not part of the ABI
// (AGENTS.md §3). An absent array is written as [], never null; an absent
// optional scalar is omitted, never written as "" — a workload distinguishes
// "no content type" from "empty content type" by presence (§3.2).
func (w *Workspace) WriteTaskJSON(in *orkstackv1.ExecuteTaskInput, tc TaskContext) error {
	limits := in.GetLimits()
	doc := taskJSON{
		SchemaVersion:  TaskJSONSchemaVersion,
		IdempotencyKey: in.GetIdempotencyKey(),
		Run: taskJSONRun{
			ID:        in.GetRunId(),
			Namespace: tc.Namespace,
			Orkflow: taskJSONOrkflow{
				Name:             in.GetOrkflowName(),
				Version:          in.GetOrkflowVersion(),
				DefinitionDigest: in.GetOrkflowDefinitionDigest(),
			},
		},
		Task: taskJSONTask{
			ID:             in.GetTaskId(),
			Attempt:        tc.Attempt,
			Pool:           tc.Pool,
			RunAsOrkerUser: tc.RunAsOrkerUser,
		},
		Image: taskJSONImage{
			Reference:      tc.Image.GetReference(),
			Pinned:         tc.Image.GetPinned(),
			IndexDigest:    tc.Image.GetIndexDigest(),
			PlatformDigest: tc.Image.GetPlatformDigest(),
			Platform:       tc.Image.GetPlatform(),
			Delivery:       tc.Image.GetDelivery(),
		},
		Limits: taskJSONLimits{
			TimeoutSeconds:                 in.GetTimeoutSeconds(),
			CancellationGracePeriodSeconds: in.GetCancellationGracePeriodSeconds(),
			ValuesMaxBytes:                 limits.GetValuesMaxBytes(),
			ArtifactMaxCount:               limits.GetArtifactMaxCount(),
			ArtifactMaxBytes:               limits.GetArtifactMaxBytes(),
			LogMaxBytes:                    limits.GetLogMaxBytes(),
			CommitMaxBytes:                 limits.GetCommitMaxBytes(),
		},
		Network: tc.Network,
		Paths: taskJSONPaths{
			Input:     ContainerInput,
			Config:    ContainerConfigJSON,
			RunInput:  ContainerRunJSON,
			Secrets:   ContainerSecrets,
			Output:    ContainerOutput,
			Values:    ContainerValuesJSON,
			Artifacts: ContainerArtifacts,
			Tmp:       ContainerTmp,
		},
		Inputs: taskJSONInputs{
			Artifacts: tc.StagedInputs,
			Upstream:  tc.StagedUpstream,
		},
		Secrets: tc.StagedSecrets,
	}
	// The resources block reports the EFFECTIVE values after Orker policy,
	// which is exactly what §3.2 says it means: "as requested/capped; absent
	// means Orker default".
	if tc.EffectiveCPU != "" || tc.EffectiveMem != "" {
		doc.Resources = &taskJSONRes{CPU: tc.EffectiveCPU, Memory: tc.EffectiveMem}
	}
	// Never null.
	if doc.Inputs.Artifacts == nil {
		doc.Inputs.Artifacts = []taskJSONInputArtifact{}
	}
	if doc.Inputs.Upstream == nil {
		doc.Inputs.Upstream = []taskJSONUpstream{}
	}
	if doc.Secrets == nil {
		doc.Secrets = []taskJSONSecret{}
	}
	return w.writeInputFile("task.json", doc)
}

// WriteConfigJSON writes the RENDERED `with:` block.
func (w *Workspace) WriteConfigJSON(rendered map[string]any) error {
	if rendered == nil {
		rendered = map[string]any{}
	}
	return w.writeInputFile("config.json", rendered)
}

// WriteRunJSON writes the run's input payload — the same data templates see as
// .inputs.
func (w *Workspace) WriteRunJSON(inputs map[string]any) error {
	if inputs == nil {
		inputs = map[string]any{}
	}
	return w.writeInputFile("run.json", inputs)
}

func (w *Workspace) writeInputFile(name string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(w.Input, name)
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}

// EncodeArtifactName renders the always-present nameB64 of §3.2.
func EncodeArtifactName(name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(name))
}

// ---------------------------------------------------------------- output scan

// ScannedArtifact is one file found in output/artifacts/.
type ScannedArtifact struct {
	Name string
	Path string
	Size int64
}

// ScanOutputArtifacts implements §3.5.
//
// The scan is NOT recursive: a directory, symlink, device, socket or FIFO is a
// ContractViolation, and a directory hierarchy is archived by the workload as
// e.g. workspace.tar.gz. The artifact layer has no directory semantics
// (AGENTS.md §3.1).
//
// Ordering is by name bytes ascending, so two attempts of the same computation
// produce the same commit ordering.
//
// It runs on the SUCCESS path and on the FAILURE path. An earlier draft sent a
// non-zero exit straight to the failure path, which left these rules
// unenforced on exactly the path a broken or hostile image is most likely to
// take. What differs between the two is the CONSEQUENCE: on the failure path a
// violation is a note attached to the workload's error, never a replacement
// for it (§7.1 step 14).
func ScanOutputArtifacts(dir string, limits *orkstackv1.WorkspaceLimits) ([]ScannedArtifact, *Failure) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// A workload that produced no artifacts did not have to create
			// the directory. That is not a violation.
			return nil, nil
		}
		return nil, FailCause(ReasonOutputNotReadable, err,
			"cannot read the output artifact directory %s: %v", ContainerArtifacts, err)
	}

	maxCount := limits.GetArtifactMaxCount()
	maxBytes := limits.GetArtifactMaxBytes()

	found := make([]ScannedArtifact, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()

		// Valid UTF-8 is a real restriction on a POSIX filesystem, and it is
		// imposed because the alternative is worse: ArtifactRef.name is a
		// proto string, and EVERY protobuf encoding — protojson and binary
		// alike — refuses invalid UTF-8 (§14.16). Such a name could be
		// staged, hashed and uploaded and would then fail at the commit,
		// after the bandwidth was spent. Rejecting at the scan makes it a
		// fast, terminal, actionable failure instead.
		if !utf8.ValidString(name) {
			return nil, Fail(ReasonArtifactNameNotUTF8,
				"output artifact name is not valid UTF-8: bytes %s in %s",
				hex.EncodeToString([]byte(name)), ContainerArtifacts)
		}

		info, err := entry.Info()
		if err != nil {
			return nil, FailCause(ReasonOutputNotReadable, err,
				"cannot stat output artifact %s/%s: %v", ContainerArtifacts, name, err)
		}
		if !info.Mode().IsRegular() {
			return nil, Fail(ReasonArtifactNotAFile,
				"%s/%s is %s, not a regular file; the artifact layer has no directory semantics — "+
					"archive a hierarchy yourself, e.g. as workspace.tar.gz",
				ContainerArtifacts, name, describeMode(info.Mode()))
		}
		if maxBytes > 0 && info.Size() > maxBytes {
			return nil, Fail(ReasonArtifactLimit,
				"output artifact %s/%s is %d bytes, over the %d-byte per-artifact limit",
				ContainerArtifacts, name, info.Size(), maxBytes)
		}
		found = append(found, ScannedArtifact{
			Name: name,
			Path: filepath.Join(dir, name),
			Size: info.Size(),
		})
	}

	if maxCount > 0 && int32(len(found)) > maxCount {
		return nil, Fail(ReasonArtifactLimit,
			"%d output artifacts exceed the limit of %d", len(found), maxCount)
	}

	sort.Slice(found, func(i, j int) bool { return found[i].Name < found[j].Name })
	return found, nil
}

// describeMode names a non-regular entry in the words a person uses.
func describeMode(mode os.FileMode) string {
	switch {
	case mode.IsDir():
		return "a directory"
	case mode&os.ModeSymlink != 0:
		return "a symlink"
	case mode&os.ModeDevice != 0:
		return "a device"
	case mode&os.ModeSocket != 0:
		return "a socket"
	case mode&os.ModeNamedPipe != 0:
		return "a FIFO"
	default:
		return "not a regular file"
	}
}

// CheckReadable opens a file the Orker must read, and fails terminally when it
// cannot.
//
// This is the §3.1 measure, and it exists because READABILITY IS NOT
// GUARANTEED BY THE MOUNT. 1777 guarantees the workload can CREATE a file; it
// says nothing about the mode that file lands with. A workload running under
// umask 077 writes values.json as 0600 owned by its own uid, and an Orker
// running as a different non-root user then cannot read the bytes it must hash
// and upload. Both outcomes reproduce on one machine: on a native filesystem
// the Orker gets Permission denied, and through a VM-shared bind mount the
// same write is ownership-remapped and reads fine (§14.8). No mount
// configuration can be relied on, so the Orker verifies rather than assumes.
//
// stat succeeds even when open fails, which is what lets the message always be
// specific — it names the file, its mode, its owner and the fix.
//
// The classification is deliberate. OUTPUT_NOT_READABLE is a ContractViolation
// and is never retried: the image will do the same thing on every attempt.
// Classifying it as ARTIFACT_UPLOAD_FAILED — infrastructure, retryable — would
// loop forever against deterministic image behavior, which AGENTS.md §11
// forbids.
func CheckReadable(path, containerPath string) *Failure {
	file, err := os.Open(path)
	if err == nil {
		_ = file.Close()
		return nil
	}
	detail := describeUnreadable(path)
	return FailCause(ReasonOutputNotReadable, err,
		"the orker cannot read %s (%s): %v — the workload wrote it with permissions the orker's "+
			"user cannot open. Set `umask 022` in the image's entry script, or set "+
			"runAsOrkerUser: true on this task",
		containerPath, detail, err)
}

// describeUnreadable renders "mode=600 owner=65534:65534" for the message.
func describeUnreadable(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return "unstattable"
	}
	described := fmt.Sprintf("mode=%o", info.Mode().Perm())
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		described += fmt.Sprintf(" owner=%d:%d", stat.Uid, stat.Gid)
	}
	return described
}

// ---------------------------------------------------------------- values.json

// ReadValues validates and returns the workload's values.json bytes (§3.4).
//
// The bytes are returned VERBATIM. The Orker parses to validate and never
// re-serializes: key order, spacing and number formatting are preserved, and
// the committed sha256 is the sha256 of exactly what the workload wrote. The
// same exact bytes are what travels inline as ExecuteTaskResult.values_json,
// which is why that field is bytes and not a Struct-backed Payload (§14.9).
func ReadValues(path string, limits *orkstackv1.WorkspaceLimits) ([]byte, *Failure) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, Fail(ReasonValuesMissing,
				"the container exited 0 but wrote no %s; a workload with no result writes {}",
				ContainerValuesJSON)
		}
		return nil, FailCause(ReasonOutputNotReadable, err,
			"cannot stat %s: %v", ContainerValuesJSON, err)
	}
	if !info.Mode().IsRegular() {
		return nil, Fail(ReasonValuesMalformed,
			"%s is %s, not a regular file", ContainerValuesJSON, describeMode(info.Mode()))
	}
	// Size is checked before the read so an enormous file costs no memory.
	if max := limits.GetValuesMaxBytes(); max > 0 && info.Size() > max {
		return nil, Fail(ReasonValuesTooLarge,
			"%s is %d bytes, over the %d-byte limit", ContainerValuesJSON, info.Size(), max)
	}
	if failure := CheckReadable(path, ContainerValuesJSON); failure != nil {
		return nil, failure
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, FailCause(ReasonOutputNotReadable, err,
			"cannot read %s: %v", ContainerValuesJSON, err)
	}
	if !utf8.Valid(raw) {
		return nil, Fail(ReasonValuesMalformed, "%s is not valid UTF-8", ContainerValuesJSON)
	}
	// A top-level object, specifically. An array, a scalar or null is a
	// violation: downstream templates address .tasks.<id>.output.<field>, and
	// there is no field on an array.
	var probe any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, FailCause(ReasonValuesMalformed, err,
			"%s is not valid JSON: %v", ContainerValuesJSON, err)
	}
	if _, ok := probe.(map[string]any); !ok {
		return nil, Fail(ReasonValuesMalformed,
			"%s must be a JSON object at the top level, not %s", ContainerValuesJSON, jsonKind(probe))
	}
	return raw, nil
}

func jsonKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case float64:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	default:
		return "an unexpected value"
	}
}

// InlineValuesLimit is the size at or below which values.json is copied inline
// into ExecuteTaskResult.values_json.
//
// values.json's own budget is 256 KiB because it is an object-store artifact,
// and an object-store budget is not the Temporal payload budget. The Temporal
// budget is enforced separately, and this is it (§3.4).
const InlineValuesLimit = 4096

// HashFile streams a file and returns its lowercase hex sha256 and size.
func HashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(digest.Sum(nil)), size, nil
}

// sniffFileContentType resolves the content type the artifact store would
// record for this file, by reading exactly the bytes the store's own sniffer
// inspects and applying the store's own rule.
//
// It exists so §5.4 step 4 can PREDICT a ref whose ContentType matches the one
// step 5 stores. RefPredictor.RefFor uses its contentType verbatim and cannot
// sniff bytes it was never handed, so the caller resolves it once here and
// passes the same value to both sides.
func sniffFileContentType(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	head := make([]byte, artifacts.SniffLen)
	n, err := io.ReadFull(file, head)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	return artifacts.SniffContentType(head[:n]), nil
}

// HashBytes returns the lowercase hex sha256 of a byte slice.
func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ValidSecretName holds a secret name to the §3.2 grammar,
// ^[A-Za-z0-9._-]{1,128}$. A name is validated rather than sanitized, so it
// can never become a path segment that escapes the secrets directory.
func ValidSecretName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-':
		default:
			return false
		}
	}
	// "." and ".." pass the character test and are still path segments.
	return name != "." && name != ".."
}

// SplitSecretSource splits "env:DB_PASSWORD" into its scheme and locator.
func SplitSecretSource(source string) (scheme, locator string) {
	scheme, locator, found := strings.Cut(source, ":")
	if !found {
		return "", source
	}
	return scheme, locator
}
