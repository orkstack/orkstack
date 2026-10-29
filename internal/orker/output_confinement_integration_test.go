//go:build integration

package orker_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_TheOrkerNeverReadsThroughAnOutputSymlink is release blocker
// B7, on a real container.
//
// The output directory is the workload's, and the workload may be hostile. A
// symlink it leaves there is resolved by the ORKER, in the Orker's own mount
// namespace, so its target is a host path the container never saw: a file in
// the Orker's work root (another attempt's task.json, a registry login) is
// exactly as reachable as one the workload wrote. Each row plants one such
// link to bytes only the Orker can see and requires the attempt to end as a
// ContractViolation that names the path, is never retried, commits nothing
// and stores none of the outside bytes (AGENTS.md §2.3, §3.1, §4).
//
// Most rows run the workload as the Orker's own user, which is the deployment
// the release image ships: the orker runs as root, and so does an image that
// does not set USER. Two host rules otherwise hide the defect. output/artifacts
// is the Orker's and sits in a sticky 1777 parent, so only its owner may
// rename it; and with fs.protected_symlinks=1 the kernel refuses to follow a
// link in a sticky world-writable directory unless the link's owner is the
// directory's or the follower's. Neither is a boundary the Orker may rely on:
// a same-uid workload passes both, and not every host sets the sysctl.
func TestIntegration_TheOrkerNeverReadsThroughAnOutputSymlink(t *testing.T) {
	requireDocker(t)

	tests := []struct {
		name string
		// script is the workload; outside is a host directory only the Orker
		// can see, holding leak.json and leak.txt.
		script     func(outside string) string
		runAsOrker bool
		wantReason string
		// wantNamed is the container path the explanation must name.
		wantNamed string
		// workloadFailed rows exit non-zero: the reason is the workload's own,
		// and the refusal is a note attached to it (§7.1 step 14).
		workloadFailed bool
	}{
		{
			name: "values.json is a symlink to a JSON file only the orker can see",
			script: func(outside string) string {
				return `ln -s ` + outside + `/leak.json /orkstack/output/values.json`
			},
			runAsOrker: true,
			wantReason: orker.ReasonValuesMalformed,
			wantNamed:  orker.ContainerValuesJSON,
		},
		{
			// The image's own user: whatever the sysctl says, the refusal is
			// the Orker's and names what it refused.
			name: "values.json is a symlink written by a user other than the orker's",
			script: func(outside string) string {
				return `ln -s ` + outside + `/leak.json /orkstack/output/values.json`
			},
			wantReason: orker.ReasonValuesMalformed,
			wantNamed:  orker.ContainerValuesJSON,
		},
		{
			name: "values.json is a symlink to a file inside the output directory",
			script: func(string) string {
				return `printf '{"inside":true}' > /orkstack/output/real.json; ` +
					`ln -s real.json /orkstack/output/values.json`
			},
			runAsOrker: true,
			wantReason: orker.ReasonValuesMalformed,
			wantNamed:  orker.ContainerValuesJSON,
		},
		{
			name: "output/artifacts is a symlink to a directory only the orker can see",
			script: func(outside string) string {
				return `printf '{}' > /orkstack/output/values.json; ` +
					`rmdir /orkstack/output/artifacts && ln -s ` + outside + ` /orkstack/output/artifacts`
			},
			runAsOrker: true,
			wantReason: orker.ReasonArtifactNotAFile,
			wantNamed:  orker.ContainerArtifacts,
		},
		{
			name: "output/artifacts is a regular file",
			script: func(string) string {
				return `printf '{}' > /orkstack/output/values.json; ` +
					`rmdir /orkstack/output/artifacts && printf x > /orkstack/output/artifacts`
			},
			runAsOrker: true,
			wantReason: orker.ReasonArtifactNotAFile,
			wantNamed:  orker.ContainerArtifacts,
		},
		{
			name: "an artifact is a symlink to a file only the orker can see",
			script: func(outside string) string {
				return `printf '{}' > /orkstack/output/values.json; ` +
					`ln -s ` + outside + `/leak.txt /orkstack/output/artifacts/leak.txt`
			},
			wantReason: orker.ReasonArtifactNotAFile,
			wantNamed:  orker.ContainerArtifacts + "/leak.txt",
		},
		{
			name: "a failed task's output/artifacts is a symlink to a directory only the orker can see",
			script: func(outside string) string {
				return `rmdir /orkstack/output/artifacts && ln -s ` + outside + ` /orkstack/output/artifacts; ` +
					`exit 1`
			},
			runAsOrker:     true,
			wantReason:     orker.ReasonContainerExitNonzero,
			wantNamed:      orker.ContainerArtifacts,
			workloadFailed: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dispatcher, commits := newDispatcher(t)
			storeDir := t.TempDir()
			store, err := artifacts.NewLocalStore(storeDir)
			if err != nil {
				t.Fatalf("local artifact store: %v", err)
			}
			dispatcher.Store = store

			// The bytes only the Orker can see live in its own work root,
			// beside the attempts: a path no mount gives the container.
			token := "orker-only-" + strings.ReplaceAll(tc.name, " ", "-")
			outside, err := os.MkdirTemp(dispatcher.WorkRoot, "outside-")
			if err != nil {
				t.Fatal(err)
			}
			for name, content := range map[string]string{
				"leak.json": `{"leaked":"` + token + `"}`,
				"leak.txt":  token,
			} {
				if err := os.WriteFile(filepath.Join(outside, name), []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			in := baseInput(t, "confine", "/bin/sh", "-c", tc.script(outside))
			in.CommitUri = "mem://commits/" + in.RunId + "/confine/commit.json"
			in.RunAsOrkerUser = tc.runAsOrker

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			result, err := dispatcher.Execute(ctx, in)
			if err == nil {
				t.Fatalf("Execute succeeded with values %s and artifacts %v; want %s",
					result.GetValuesJson(), result.GetArtifacts(), tc.wantReason)
			}
			if !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("error = %v, want %s", err, tc.wantReason)
			}

			detail := failureDetail(t, err)
			if tc.workloadFailed {
				notes := strings.Join(detail.GetScanNotes(), "; ")
				if !strings.Contains(notes, orker.ReasonArtifactNotAFile) || !strings.Contains(notes, tc.wantNamed) {
					t.Errorf("scan notes %q do not name %s as %s", notes, tc.wantNamed, orker.ReasonArtifactNotAFile)
				}
				if len(detail.GetArtifacts()) != 0 {
					t.Errorf("the failed attempt published %v from outside its output", detail.GetArtifacts())
				}
			} else {
				var applicationErr *temporal.ApplicationError
				if !errors.As(err, &applicationErr) || !applicationErr.NonRetryable() {
					t.Errorf("error %v is retryable; a contract violation must never be retried", err)
				}
				if applicationErr != nil && applicationErr.Type() != orker.CategoryContractViolation {
					t.Errorf("category = %q, want %q", applicationErr.Type(), orker.CategoryContractViolation)
				}
				if !strings.Contains(err.Error(), tc.wantNamed+" is ") {
					t.Errorf("error %q does not name %s", err, tc.wantNamed)
				}
			}
			if strings.Contains(err.Error(), token) {
				t.Errorf("the error carries the outside bytes: %v", err)
			}

			if _, getErr := commits.Get(ctx, in.GetCommitUri()); !errors.Is(getErr, artifacts.ErrNotFound) {
				t.Errorf("a commit exists at %s (err %v); a refused output must commit nothing",
					in.GetCommitUri(), getErr)
			}
			_ = filepath.WalkDir(storeDir, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || !entry.Type().IsRegular() {
					return nil
				}
				if raw, readErr := os.ReadFile(path); readErr == nil && bytes.Contains(raw, []byte(token)) {
					t.Errorf("the store holds the outside bytes at %s", path)
				}
				return nil
			})
		})
	}
}
