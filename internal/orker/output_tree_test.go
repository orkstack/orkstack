package orker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// outsideSentinel is the content of every file a test places outside the
// output tree. No read of the tree may ever return it.
const outsideSentinel = `{"leaked":"orker-only-bytes"}`

// outputFixture is an output directory beside a directory only the Orker can
// see, which holds outside.json, outside.txt and a subdirectory dir/ with
// outside.txt.
type outputFixture struct {
	output, outside string
}

func newOutputFixture(t *testing.T) outputFixture {
	t.Helper()
	base := t.TempDir()
	fx := outputFixture{output: filepath.Join(base, "output"), outside: filepath.Join(base, "outside")}
	for _, dir := range []string{
		filepath.Join(fx.output, "artifacts"),
		filepath.Join(fx.output, "inside-dir"),
		filepath.Join(fx.outside, "dir"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{
		filepath.Join(fx.outside, "outside.json"),
		filepath.Join(fx.outside, "outside.txt"),
		filepath.Join(fx.outside, "dir", "outside.txt"),
	} {
		mustWrite(t, path, outsideSentinel)
	}
	mustWrite(t, filepath.Join(fx.output, "inside.json"), `{"inside":true}`)
	mustWrite(t, filepath.Join(fx.output, "inside-dir", "inside.txt"), "inside")
	return fx
}

func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink %s -> %s: %v", link, target, err)
	}
}

// TestOutputTree_RefusesEveryLinkTheWorkloadLeft is B7 at the reader: every
// shape a workload can leave where the Orker reads is refused by name, as a
// ContractViolation, before a byte outside the tree is read.
func TestOutputTree_RefusesEveryLinkTheWorkloadLeft(t *testing.T) {
	type read func(*OutputTree) ([]byte, *Failure)
	readValues := func(tree *OutputTree) ([]byte, *Failure) { return tree.ReadValues(testLimits()) }
	readArtifacts := func(tree *OutputTree) ([]byte, *Failure) {
		scanned, failure := tree.ScanArtifacts(testLimits())
		if failure != nil {
			return nil, failure
		}
		var all []byte
		for _, artifact := range scanned {
			file, failure := artifact.Open()
			if failure != nil {
				return all, failure
			}
			raw, err := io.ReadAll(file)
			_ = file.Close()
			if err != nil {
				return all, FailCause(ReasonOutputNotReadable, err, "read %s", artifact.Name)
			}
			all = append(all, raw...)
		}
		return all, nil
	}

	tests := []struct {
		name       string
		build      func(t *testing.T, fx outputFixture)
		read       read
		wantReason string
		wantNamed  string
		wantKind   string
	}{
		{
			name: "values.json links to a JSON file outside",
			build: func(t *testing.T, fx outputFixture) {
				mustSymlink(t, filepath.Join(fx.outside, "outside.json"), filepath.Join(fx.output, "values.json"))
			},
			read: readValues, wantReason: ReasonValuesMalformed, wantNamed: ContainerValuesJSON, wantKind: "a symlink",
		},
		{
			name: "values.json links to a JSON file inside",
			build: func(t *testing.T, fx outputFixture) {
				mustSymlink(t, "inside.json", filepath.Join(fx.output, "values.json"))
			},
			read: readValues, wantReason: ReasonValuesMalformed, wantNamed: ContainerValuesJSON, wantKind: "a symlink",
		},
		{
			name: "values.json is a dangling link",
			build: func(t *testing.T, fx outputFixture) {
				mustSymlink(t, filepath.Join(fx.outside, "missing.json"), filepath.Join(fx.output, "values.json"))
			},
			read: readValues, wantReason: ReasonValuesMalformed, wantNamed: ContainerValuesJSON, wantKind: "a symlink",
		},
		{
			name: "values.json is a directory",
			build: func(t *testing.T, fx outputFixture) {
				if err := os.Mkdir(filepath.Join(fx.output, "values.json"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
			read: readValues, wantReason: ReasonValuesMalformed, wantNamed: ContainerValuesJSON, wantKind: "a directory",
		},
		{
			name:  "values.json is a FIFO",
			build: func(t *testing.T, fx outputFixture) { mustFIFO(t, filepath.Join(fx.output, "values.json")) },
			read:  readValues, wantReason: ReasonValuesMalformed, wantNamed: ContainerValuesJSON, wantKind: "a FIFO",
		},
		{
			name: "output/artifacts links to a directory outside",
			build: func(t *testing.T, fx outputFixture) {
				_ = os.Remove(filepath.Join(fx.output, "artifacts"))
				mustSymlink(t, filepath.Join(fx.outside, "dir"), filepath.Join(fx.output, "artifacts"))
			},
			read: readArtifacts, wantReason: ReasonArtifactNotAFile, wantNamed: ContainerArtifacts, wantKind: "a symlink",
		},
		{
			name: "output/artifacts links to a directory inside",
			build: func(t *testing.T, fx outputFixture) {
				_ = os.Remove(filepath.Join(fx.output, "artifacts"))
				mustSymlink(t, "inside-dir", filepath.Join(fx.output, "artifacts"))
			},
			read: readArtifacts, wantReason: ReasonArtifactNotAFile, wantNamed: ContainerArtifacts, wantKind: "a symlink",
		},
		{
			name: "output/artifacts is a regular file",
			build: func(t *testing.T, fx outputFixture) {
				_ = os.Remove(filepath.Join(fx.output, "artifacts"))
				mustWrite(t, filepath.Join(fx.output, "artifacts"), "x")
			},
			read: readArtifacts, wantReason: ReasonArtifactNotAFile, wantNamed: ContainerArtifacts, wantKind: "a regular file",
		},
		{
			name: "output/artifacts is a FIFO",
			build: func(t *testing.T, fx outputFixture) {
				_ = os.Remove(filepath.Join(fx.output, "artifacts"))
				mustFIFO(t, filepath.Join(fx.output, "artifacts"))
			},
			read: readArtifacts, wantReason: ReasonArtifactNotAFile, wantNamed: ContainerArtifacts, wantKind: "a FIFO",
		},
		{
			name: "an artifact links to a file outside",
			build: func(t *testing.T, fx outputFixture) {
				mustSymlink(t, filepath.Join(fx.outside, "outside.txt"), filepath.Join(fx.output, "artifacts", "leak.txt"))
			},
			read: readArtifacts, wantReason: ReasonArtifactNotAFile,
			wantNamed: ContainerArtifacts + "/leak.txt", wantKind: "a symlink",
		},
		{
			name: "an artifact links to a directory outside",
			build: func(t *testing.T, fx outputFixture) {
				mustSymlink(t, filepath.Join(fx.outside, "dir"), filepath.Join(fx.output, "artifacts", "leak"))
			},
			read: readArtifacts, wantReason: ReasonArtifactNotAFile,
			wantNamed: ContainerArtifacts + "/leak", wantKind: "a symlink",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newOutputFixture(t)
			tc.build(t, fx)

			raw, failure := tc.read(openTestOutput(t, fx.output))
			if bytes.Contains(raw, []byte(outsideSentinel)) {
				t.Fatalf("the tree returned the outside bytes %q", raw)
			}
			if failure == nil {
				t.Fatalf("the read succeeded with %q; want %s", raw, tc.wantReason)
			}
			if failure.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q (%s)", failure.Reason, tc.wantReason, failure.Message)
			}
			if failure.Category() != CategoryContractViolation {
				t.Errorf("category = %q, want %q", failure.Category(), CategoryContractViolation)
			}
			if want := tc.wantNamed + " is " + tc.wantKind; !strings.Contains(failure.Message, want) {
				t.Errorf("message %q does not say %q", failure.Message, want)
			}
			for _, host := range []string{fx.output, fx.outside, outsideSentinel} {
				if strings.Contains(failure.Error(), host) {
					t.Errorf("the failure names %q, which is the Orker's, not the workload's:\n%s", host, failure)
				}
			}
		})
	}
}

// TestOutputTree_ReadsWhatAWellBehavedWorkloadWrote is the control: the same
// tree with regular files reads them byte for byte.
func TestOutputTree_ReadsWhatAWellBehavedWorkloadWrote(t *testing.T) {
	fx := newOutputFixture(t)
	mustWrite(t, filepath.Join(fx.output, "values.json"), `{"n":1}`)
	mustWrite(t, filepath.Join(fx.output, "artifacts", "b.txt"), "bee")
	mustWrite(t, filepath.Join(fx.output, "artifacts", "a.txt"), "ay")
	tree := openTestOutput(t, fx.output)

	values, failure := tree.ReadValues(testLimits())
	if failure != nil || string(values) != `{"n":1}` {
		t.Fatalf("ReadValues = %q, %v", values, failure)
	}
	scanned, failure := tree.ScanArtifacts(testLimits())
	if failure != nil {
		t.Fatalf("ScanArtifacts: %v", failure)
	}
	var got []string
	for _, artifact := range scanned {
		file, failure := artifact.Open()
		if failure != nil {
			t.Fatalf("Open %s: %v", artifact.Name, failure)
		}
		raw, err := io.ReadAll(file)
		_ = file.Close()
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(raw)) != artifact.Size {
			t.Errorf("%s: scanned %d bytes, read %d", artifact.Name, artifact.Size, len(raw))
		}
		got = append(got, artifact.Name+"="+string(raw))
	}
	if strings.Join(got, ",") != "a.txt=ay,b.txt=bee" {
		t.Errorf("artifacts = %v", got)
	}
}

// TestScannedArtifact_OpenNeverFollowsWhatTookTheNameAfterTheScan: the scan
// approves an inode, and every later open (hash, sniff, upload) must read that
// inode or refuse. A name replaced by a link, or by another file, is refused;
// a directory renamed away and replaced by a link is never re-resolved,
// because the scan holds the directory itself.
func TestScannedArtifact_OpenNeverFollowsWhatTookTheNameAfterTheScan(t *testing.T) {
	scanOne := func(t *testing.T, fx outputFixture) ScannedArtifact {
		t.Helper()
		mustWrite(t, filepath.Join(fx.output, "artifacts", "report.txt"), "report")
		scanned, failure := openTestOutput(t, fx.output).ScanArtifacts(testLimits())
		if failure != nil || len(scanned) != 1 {
			t.Fatalf("scan = %v, %v", scanned, failure)
		}
		return scanned[0]
	}

	t.Run("the name becomes a link outside", func(t *testing.T) {
		fx := newOutputFixture(t)
		artifact := scanOne(t, fx)
		path := filepath.Join(fx.output, "artifacts", "report.txt")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		mustSymlink(t, filepath.Join(fx.outside, "outside.txt"), path)
		file, failure := artifact.Open()
		if file != nil {
			_ = file.Close()
		}
		if failure == nil || failure.Reason != ReasonArtifactNotAFile {
			t.Fatalf("Open = %v; want %s", failure, ReasonArtifactNotAFile)
		}
	})

	t.Run("the name becomes another regular file", func(t *testing.T) {
		fx := newOutputFixture(t)
		artifact := scanOne(t, fx)
		path := filepath.Join(fx.output, "artifacts", "report.txt")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, path, "something else")
		file, failure := artifact.Open()
		if file != nil {
			_ = file.Close()
		}
		if failure == nil || failure.Reason != ReasonArtifactNotAFile {
			t.Fatalf("Open = %v; want %s", failure, ReasonArtifactNotAFile)
		}
	})

	t.Run("the directory is renamed away and replaced by a link outside", func(t *testing.T) {
		fx := newOutputFixture(t)
		artifact := scanOne(t, fx)
		artifactsDir := filepath.Join(fx.output, "artifacts")
		if err := os.Rename(artifactsDir, filepath.Join(fx.output, "moved")); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(fx.outside, "dir"), 0o755); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, filepath.Join(fx.outside, "dir", "report.txt"), outsideSentinel)
		mustSymlink(t, filepath.Join(fx.outside, "dir"), artifactsDir)

		file, failure := artifact.Open()
		if failure != nil {
			t.Fatalf("Open: %v", failure)
		}
		defer func() { _ = file.Close() }()
		raw, err := io.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != "report" {
			t.Fatalf("Open read %q; want the scanned file's own bytes", raw)
		}
	})
}

// linkingRuntime is cacheRuntime's workload followed by plant, which rewrites
// the output tree the way a hostile workload would before it exits 0.
type linkingRuntime struct {
	*cacheRuntime
	plant func(output string) error
}

func (r *linkingRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	id, err := r.cacheRuntime.Create(ctx, spec)
	if err != nil {
		return id, err
	}
	for _, m := range spec.Mounts {
		if m.Destination == ContainerOutput {
			return id, r.plant(m.Source)
		}
	}
	return id, errors.New("no output mount")
}

// TestExecute_CommitsNothingFromOutsideTheOutputTree is the dispatcher half of
// B7, with no container runtime: the success path and the failure path both
// read the output through the confined tree, so a planted link ends the
// attempt as a ContractViolation (or a scan note on a failed one) and stores
// none of the outside bytes.
func TestExecute_CommitsNothingFromOutsideTheOutputTree(t *testing.T) {
	tests := []struct {
		name       string
		exit       int32
		plant      func(output, outside string) error
		wantReason string
		wantNote   string
	}{
		{
			name: "values.json links outside",
			plant: func(output, outside string) error {
				_ = os.Remove(filepath.Join(output, "values.json"))
				return os.Symlink(filepath.Join(outside, "outside.json"), filepath.Join(output, "values.json"))
			},
			wantReason: ReasonValuesMalformed,
		},
		{
			name: "output/artifacts links outside",
			plant: func(output, outside string) error {
				if err := os.RemoveAll(filepath.Join(output, "artifacts")); err != nil {
					return err
				}
				return os.Symlink(outside, filepath.Join(output, "artifacts"))
			},
			wantReason: ReasonArtifactNotAFile,
		},
		{
			name: "a failed attempt's output/artifacts links outside",
			exit: 1,
			plant: func(output, outside string) error {
				if err := os.RemoveAll(filepath.Join(output, "artifacts")); err != nil {
					return err
				}
				return os.Symlink(outside, filepath.Join(output, "artifacts"))
			},
			wantReason: ReasonContainerExitNonzero,
			wantNote:   ReasonArtifactNotAFile + ": " + ContainerArtifacts + " is a symlink",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newCacheHarness(t)
			storeDir := t.TempDir()
			store, err := artifacts.NewLocalStore(storeDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			h.dispatcher.Store = store
			outside := t.TempDir()
			for _, name := range []string{"outside.json", "outside.txt"} {
				mustWrite(t, filepath.Join(outside, name), outsideSentinel)
			}
			h.runtime.exit = tc.exit
			h.dispatcher.Runtime = &linkingRuntime{
				cacheRuntime: h.runtime,
				plant:        func(output string) error { return tc.plant(output, outside) },
			}
			in := cachedTaskInput(t, "ork-confine")
			in.Cache = nil

			result, err := h.dispatcher.Execute(context.Background(), in)
			if err == nil {
				t.Fatalf("Execute succeeded with %s and %v; want %s",
					result.GetValuesJson(), result.GetArtifacts(), tc.wantReason)
			}
			if !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("error = %v, want %s", err, tc.wantReason)
			}
			var applicationErr *temporal.ApplicationError
			if !errors.As(err, &applicationErr) {
				t.Fatalf("error %T is not a *temporal.ApplicationError", err)
			}
			if tc.wantNote == "" && !applicationErr.NonRetryable() {
				t.Errorf("error %v is retryable; a contract violation is never retried", err)
			}
			if tc.wantNote != "" {
				var detail *orkstackv1.ActivityFailureDetail
				if err := applicationErr.Details(&detail); err != nil {
					t.Fatalf("decode the failure detail: %v", err)
				}
				notes := strings.Join(detail.GetScanNotes(), "; ")
				if !strings.Contains(notes, tc.wantNote) {
					t.Errorf("scan notes %q do not say %q", notes, tc.wantNote)
				}
				if got := detail.GetArtifacts(); len(got) != 0 {
					t.Errorf("the failed attempt published %v", got)
				}
			}
			if _, getErr := h.commits.MemoryCommitStore.Get(context.Background(), in.GetCommitUri()); !errors.Is(getErr, artifacts.ErrNotFound) {
				t.Errorf("a commit exists (err %v); a refused output commits nothing", getErr)
			}
			_ = filepath.WalkDir(storeDir, func(path string, entry fs.DirEntry, err error) error {
				if err == nil && entry.Type().IsRegular() {
					if raw, readErr := os.ReadFile(path); readErr == nil && bytes.Contains(raw, []byte(outsideSentinel)) {
						t.Errorf("the store holds the outside bytes at %s", path)
					}
				}
				return nil
			})
		})
	}
}

// TestRemoveSealedTree_LeavesWhatALinkPointsAtAlone: the removal walk unseals
// directories inside the tree and nothing a link in it names. An outside
// directory the walk reaches by a link keeps its mode and its contents.
func TestRemoveSealedTree_LeavesWhatALinkPointsAtAlone(t *testing.T) {
	base := t.TempDir()
	tree, outside := filepath.Join(base, "attempt"), filepath.Join(base, "outside")
	if err := os.MkdirAll(filepath.Join(tree, "input", "sealed"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(outside, "keep.txt"), "keep")
	mustSymlink(t, outside, filepath.Join(tree, "input", "sealed", "link"))
	if err := os.Chmod(filepath.Join(tree, "input", "sealed"), 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(outside, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(outside, 0o700) })

	if err := removeSealedTree(tree); err != nil {
		t.Fatalf("removeSealedTree: %v", err)
	}
	if _, err := os.Lstat(tree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the sealed tree is still there: %v", err)
	}
	info, err := os.Stat(outside)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o500 {
		t.Errorf("the linked directory's mode is %o; the walk changed it", info.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(outside, "keep.txt")); err != nil {
		t.Errorf("the linked directory lost its contents: %v", err)
	}
}
