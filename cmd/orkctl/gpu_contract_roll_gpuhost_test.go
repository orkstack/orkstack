//go:build integration && gpuhost

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// The GPU-host half of the profile-contract roll (E110 CP-8). It runs only
// on the GPU host scripts/proof-gpu-tests.txt names, set up by
// scripts/gpu-host-setup.sh, and never skips: a host that is not set up fails
// it. Its merge-gate cover is TestIntegration_ProfileContractRollingChange,
// the same roll of a devices profile.

// gpuRollProfile is the gpu profile the roll declares over the test spec.
const gpuRollProfile = "gpu"

// gpuHostCDIKind is the CDI kind of the test spec this tree's
// gpu-host-setup.sh installs, once /etc/cdi is seen to hold that very spec.
func gpuHostCDIKind(ctx context.Context, t *testing.T) string {
	t.Helper()
	const hint = "run scripts/gpu-host-setup.sh and the root steps it prints (docs/PROOFS.md, \"The GPU-host tier\")"
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	setup := filepath.Join(root, "scripts", "gpu-host-setup.sh")
	out, err := exec.CommandContext(ctx, setup, "--name").Output()
	if err != nil {
		t.Fatalf("scripts/gpu-host-setup.sh --name: %v; %s", err, hint)
	}
	name := strings.TrimSpace(string(out))
	hash, ok := strings.CutPrefix(strings.TrimSuffix(name, ".yaml"), "orkstack-test-gpu-")
	if !ok || hash == "" || !strings.HasSuffix(name, ".yaml") {
		t.Fatalf("scripts/gpu-host-setup.sh --name printed %q, not orkstack-test-gpu-<hash>.yaml", name)
	}
	want, err := exec.CommandContext(ctx, setup, "--print").Output()
	if err != nil {
		t.Fatalf("scripts/gpu-host-setup.sh --print: %v", err)
	}
	have, err := os.ReadFile(filepath.Join("/etc/cdi", name))
	if err != nil || !bytes.Equal(have, want) {
		t.Fatalf("/etc/cdi/%s is not this tree's test spec (%v); %s", name, err, hint)
	}
	return "test.orkstack.dev/gpu-" + hash
}

// gpuContract is ORK_ORKER_PROFILES for the gpu profile in the short form:
// one contract, label, accepting accepts, over the test spec's two units.
func gpuContract(probeImage, cdiKind, label string, accepts ...string) string {
	body := fmt.Sprintf("profiles:\n  probe_image: %s\n  audit_interval: 60s\n  %s:\n    kind: gpu\n    contract: %q\n",
		probeImage, gpuRollProfile, label)
	if len(accepts) > 0 {
		body += "    accepts:\n"
		for _, a := range accepts {
			body += fmt.Sprintf("      - %q\n", a)
		}
	}
	return body + fmt.Sprintf("    cdi:\n      kind: %s\n      units:\n        - amd\n        - intel\n", cdiKind)
}

// gpuOrkflow is one task, render, holding one unit of the gpu profile and
// listing the render nodes it was given.
func gpuOrkflow(name, uses string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: a task given one unit of the gpu profile.
tasks:
  - id: render
    uses: %s
    timeout_seconds: 120
    devices:
      - %s
    resources:
      gpu: 1
    with:
      op: eval
      command:
        - sh
        - -c
        - ls /dev/dri
`, name, uses, gpuRollProfile)
}

// requireOneRenderNode requires the run's render task to have listed exactly
// one render node: the unit it was bound, and no other.
func requireOneRenderNode(s *timingStack, runID string) {
	s.t.Helper()
	logs := s.mustCLI(0, "run", "logs", runID, "--task", "render")
	if n := strings.Count(logs, "renderD"); n != 1 {
		s.t.Fatalf("run %s's render task saw %d render nodes, want the one unit it holds:\n%s", runID, n, logs)
	}
}

// awaitGoneFromInventory polls `orker list --json` until orkerID is no longer
// listed. An Orker that exited stays listed until the cluster forgets its
// polls, so the bound is six minutes.
func awaitGoneFromInventory(t *testing.T, s *timingStack, orkerID string) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Minute)
	for {
		out := s.mustCLI(0, "orker", "list", "--json")
		if !strings.Contains(out, orkerID) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s is still listed 6 minutes after it exited:\n%s", orkerID, out)
		}
		select {
		case <-s.ctx.Done():
			t.Fatalf("waiting for %s to leave the inventory: %v", orkerID, s.ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// TestIntegration_GPUContractRoll: a gpu profile rolls by contracts as a
// devices profile does, on this host's real units. One Orker of a host holds
// the GPU host lock at a time, so the roll is the one a single host makes:
// the new Orker starts once the old one has served, waits for the lock, and
// takes the units when the old one stops.
//
//  1. The old Orker, contract 1, runs a task whose image declares nothing
//     under contract 1.
//  2. An image declaring contract 2 is refused PROFILE_UNAVAILABLE at submit
//     while the old Orker, which neither owns nor accepts 2, is the pool,
//     naming it.
//  3. The new Orker, contract 2 accepting 1, starts and waits for the lock;
//     the old one stops, and the new one takes the units.
//  4. The task whose image declares nothing runs on the new Orker under
//     contract 2, by accepts:, and the image declaring 2 runs there under 2.
func TestIntegration_GPUContractRoll(t *testing.T) {
	requireRegistry(t, testRegistry())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	cdiKind := gpuHostCDIKind(ctx, t)
	s := newTimingStack(ctx, t, 1)
	orkerBin := testbin.StaticPath(t, "./cmd/orker")
	probeImage := buildProbeImage(ctx, t, s.root, orkerBin, "orker-probe", binaryCommit(ctx, t, orkerBin))
	start := func(id, profiles string) (string, *os.Process, <-chan struct{}) {
		t.Helper()
		orkerID := proof.OrkerID(id)
		p, done := startProcessHandle(ctx, t, "orker-"+id, orkerBin,
			"ORK_TEMPORAL_ADDRESS="+s.address, "ORK_TEMPORAL_NAMESPACE="+s.namespace, "ORK_ORKER_QUEUE="+s.queue,
			"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t), "ORK_ARTIFACT_BACKEND=s3",
			"ORK_ORKER_ID="+orkerID, "ORK_ORKER_PROFILES="+profiles)
		awaitInventory(t, s, "the Orker listed", orkerID)
		return orkerID, p, done
	}

	// 1. The old Orker holds the lock and serves contract 1.
	old, oldProc, oldDone := start("gpu-roll-old", gpuContract(probeImage, cdiKind, "1"))
	awaitUncordoned(t, s, old)
	plain := fmt.Sprintf("gpu-roll-%d", time.Now().UnixNano()%1_000_000_000)
	s.apply(gpuOrkflow(plain, "shell"))
	runID := s.submit(plain)
	requireRunStatus(t, awaitTerminalOnB(ctx, t, s.replicas[0], runID), orkstackv1.Status_STATUS_SUCCEEDED)
	if contract, on := ranUnderProfile(s, runID, "render", gpuRollProfile); on != old || contract != "1" {
		t.Fatalf("render ran on %q under contract %q; want 1 on %s", on, contract, old)
	}
	requireOneRenderNode(s, runID)
	fmt.Fprintf(&s.transcript, "run %s: render ran on %s under contract 1\n\n", runID, old)

	// 2. An image that needs contract 2 is refused while the old Orker is the
	// pool.
	declared := plain + "-declares"
	s.apply(gpuOrkflow(declared, declaringImageFor(ctx, t, gpuRollProfile, "2")))
	stdout, stderr, code := s.cli(0, "run", "submit", declared)
	if code == 0 {
		t.Fatalf("submit %s started a run beside the old Orker, want PROFILE_UNAVAILABLE:\n%s", declared, stdout)
	}
	for _, want := range []string{"PROFILE_UNAVAILABLE", old, `contract "2"`} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("submit %s refused with\n%s\nwhich does not say %q", declared, stderr, want)
		}
	}

	// 3. The new Orker starts and waits for the GPU host lock; the old one
	// stops; the new one takes the units.
	rolled, _, _ := start("gpu-roll-new", gpuContract(probeImage, cdiKind, "2", "1"))
	awaitInventory(t, s, "the new Orker waiting for the GPU host lock", rolled, "GPU_HOST_LOCK_HELD")
	// The control: while the new Orker waits, the old one still serves.
	runID = s.submit(plain)
	requireRunStatus(t, awaitTerminalOnB(ctx, t, s.replicas[0], runID), orkstackv1.Status_STATUS_SUCCEEDED)
	if contract, on := ranUnderProfile(s, runID, "render", gpuRollProfile); on != old || contract != "1" {
		t.Fatalf("while the new Orker waited for the lock, render ran on %q under contract %q; want 1 on %s", on, contract, old)
	}
	requireOneRenderNode(s, runID)
	if err := oldProc.Signal(os.Interrupt); err != nil {
		t.Fatalf("stop the old Orker: %v", err)
	}
	select {
	case <-oldDone:
	case <-time.After(2 * time.Minute):
		t.Fatal("the old Orker did not exit within 2 minutes of its stop signal")
	}
	awaitUncordoned(t, s, rolled)
	awaitGoneFromInventory(t, s, old)

	// 4. The undeclared task runs on the new Orker under 2, by accepts:, and
	// the declared one now runs there under 2.
	for _, name := range []string{plain, declared} {
		runID = s.submit(name)
		requireRunStatus(t, awaitTerminalOnB(ctx, t, s.replicas[0], runID), orkstackv1.Status_STATUS_SUCCEEDED)
		if contract, on := ranUnderProfile(s, runID, "render", gpuRollProfile); on != rolled || contract != "2" {
			t.Fatalf("after the roll, %s's render ran on %q under contract %q; want 2 on %s", name, on, contract, rolled)
		}
		requireOneRenderNode(s, runID)
		fmt.Fprintf(&s.transcript, "run %s (%s): render ran on %s under contract 2\n\n", runID, name, rolled)
	}
	s.writeTranscript(t.Name())
}
