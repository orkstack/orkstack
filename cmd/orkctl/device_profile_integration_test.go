//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// usbSerialTestDevice is CONTRIBUTING.md's "The usb-serial test device": a
// character node at usb-serial's major 188, with no adapter behind it, which
// the device-profile tests pass to a container and stat, never open.
const usbSerialTestDevice = "/dev/ttyUSB199"

// usbSerialDriftDevice is the drift test's own node (188:198), owned by the
// test's user, who moves it to another of their groups and back. No other
// test uses it, so the move drifts no other test's Orker.
const usbSerialDriftDevice = "/dev/ttyUSB198"

// usbSerialSetup is where the nodes' host setup is described.
const usbSerialSetup = `CONTRIBUTING.md, "Linux hosts" › "The usb-serial test device"`

// requireUSBSerialNode is a missing prerequisite (infraAbsent) unless path is
// a usb-serial character node of minor; with owned, the test's user must own
// it. It returns the node's group.
func requireUSBSerialNode(t *testing.T, path string, minor uint64, owned bool) uint32 {
	t.Helper()
	info, err := os.Stat(path)
	if err == nil && info.Mode()&os.ModeCharDevice != 0 {
		if st, ok := info.Sys().(*syscall.Stat_t); ok && uint64(st.Rdev)>>8&0xfff == 188 && uint64(st.Rdev)&0xff == minor {
			if !owned || int(st.Uid) == os.Getuid() {
				return st.Gid
			}
			err = fmt.Errorf("owned by uid %d, not this test's %d", st.Uid, os.Getuid())
		}
	}
	infraAbsent(t, "%s is not the usb-serial test node 188:%d on this host (%v); set it up as %s says",
		path, minor, err, usbSerialSetup)
	return 0
}

// buildProbeImage builds deploy/Dockerfile's orker target from orkerBin, the
// binary the test also boots, labelled with build, and pushes it to the local
// registry under repo and this process's own tag (exampleImageTag). It
// returns the image pinned by its digest.
func buildProbeImage(ctx context.Context, t *testing.T, root, orkerBin, repo, build string) string {
	t.Helper()
	contextDir := t.TempDir()
	license, err := os.ReadFile(filepath.Join(root, "LICENSE"))
	if err != nil {
		t.Fatalf("read LICENSE: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contextDir, "LICENSE"), license, 0o644); err != nil {
		t.Fatal(err)
	}
	binDir := filepath.Join(contextDir, "dist", "bin", "linux-"+runtime.GOARCH)
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(orkerBin)
	if err != nil {
		t.Fatalf("read %s: %v", orkerBin, err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "orker"), binary, 0o755); err != nil {
		t.Fatal(err)
	}
	tag := fmt.Sprintf("%s/orkstack-test/%s:%s", testRegistry(), repo, exampleImageTag())
	docker := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "docker", args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	docker("build", "--quiet", "--provenance=false", "-f", filepath.Join(root, "deploy", "Dockerfile"), "--target", "orker",
		"--build-arg", "BUILD="+build, "-t", tag, contextDir)
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", tag).Run() })
	docker("push", "--quiet", tag)

	return pinnedInRegistry(ctx, t, tag)
}

// binaryCommit is the commit a built binary reports (`--version`).
func binaryCommit(ctx context.Context, t *testing.T, bin string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, "--version")
	cmd.Env = testbin.OrkerEnv(testbin.StackEnv(os.Environ()))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s --version: %v", bin, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if rest, ok := strings.CutPrefix(line, "commit:"); ok {
			commit := strings.TrimSpace(rest)
			if commit == "" || commit == "unknown" {
				t.Fatalf("%s was built from no known commit; a device Orker's probe image cannot be shown to be its own", bin)
			}
			return commit
		}
	}
	t.Fatalf("%s --version names no commit:\n%s", bin, out)
	return ""
}

// sensorProfiles is ORK_ORKER_PROFILES for one devices profile, sensor,
// passing device read-write at /dev/sensor, audited every auditInterval.
func sensorProfiles(probeImage, device, auditInterval string) string {
	return fmt.Sprintf("profiles:\n  probe_image: %s\n  audit_interval: %s\n  sensor:\n    kind: devices\n    devices:\n"+
		"      - path: %s\n        container_path: /dev/sensor\n        permissions: rw\n", probeImage, auditInterval, device)
}

// sensorOrkflow is one task that selects sensor and stats /dev/sensor,
// allowed attempts attempts.
func sensorOrkflow(name string, attempts int) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: a task given the usb-serial test device through a device profile.
tasks:
  - id: read
    uses: shell
    timeout_seconds: 60
    devices: [sensor]
    retry:
      maximum_attempts: %d
      initial_interval_seconds: 1
    with:
      op: eval
      command: ["stat", "-c", "%%t:%%T %%F", "/dev/sensor"]
`, name, attempts)
}

// awaitInventory polls `orker list --json` until it holds every one of want.
func awaitInventory(t *testing.T, s *timingStack, why string, want ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		out := s.mustCLI(0, "orker", "list", "--json")
		found := true
		for _, w := range want {
			found = found && strings.Contains(out, w)
		}
		if found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the inventory did not name %q within 2m:\n%s", why, want, out)
		}
		select {
		case <-s.ctx.Done():
			t.Fatal(s.ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// awaitOffering waits until the Orker inventory lists orkerID offering the
// sensor profile. Submit refuses a profiled task (PROFILE_UNAVAILABLE) while no
// Orker of its pool answered on its own channel, and startOrkerWithEnv waits
// only for the pool channel's poller, which can be up before the Orker's
// presence and control channels answer: a submit in that gap was refused
// (Train 15 proof 44244800706, shard 5).
func awaitOffering(t *testing.T, s *timingStack, orkerID string) {
	t.Helper()
	awaitInventory(t, s, "the Orker offering the profile, before the submit", orkerID, `"sensor"`)
}

// TestIntegration_DeviceProfilePassesTheDevice: an Orker on its daemon's own
// host boots with a device profile whose probe image is its own build, a task
// that selects the profile sees the host's node at its container path, and
// an Orker given a probe image of another build boots held unavailable,
// which its inventory row names.
func TestIntegration_DeviceProfilePassesTheDevice(t *testing.T) {
	requireUSBSerialNode(t, usbSerialTestDevice, 199, false)
	requireRegistry(t, testRegistry())
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	s := newTimingStack(ctx, t, 1)
	orkerBin := testbin.StaticPath(t, "./cmd/orker")
	commit := binaryCommit(ctx, t, orkerBin)
	probeImage := buildProbeImage(ctx, t, s.root, orkerBin, "orker-probe", commit)
	startOrkerWithEnv(ctx, t, s.temporal, orkerBin, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+proof.OrkerID("device-sensor"), "ORK_ORKER_PROFILES="+sensorProfiles(probeImage, usbSerialTestDevice, "60s"))
	awaitOffering(t, s, proof.OrkerID("device-sensor"))

	name := fmt.Sprintf("device-profile-%d", time.Now().UnixNano()%1_000_000_000)
	s.apply(sensorOrkflow(name, 1))
	run := awaitTerminalOnB(ctx, t, s.replicas[0], s.submit(name))
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	got, _ := taskOutput(t, run, "read")["value"].(string)
	if want := "bc:c7 character special file"; strings.TrimSpace(got) != want {
		t.Fatalf("the task saw /dev/sensor as %q, want %q (188:199, a character device)", got, want)
	}
	fmt.Fprintf(&s.transcript, "task read saw /dev/sensor as %q\n\n", strings.TrimSpace(got))

	// A second Orker, on a channel of its own and with an identity of its
	// own, whose probe image is of another build: it boots, cordoned with the
	// profile unavailable, and polls for no task, so it is found through the
	// inventory.
	otherImage := buildProbeImage(ctx, t, s.root, orkerBin, "orker-probe-other", "another-build")
	startProcess(ctx, t, "orker", orkerBin,
		"ORK_TEMPORAL_ADDRESS="+s.address, "ORK_TEMPORAL_NAMESPACE="+s.namespace, "ORK_ORKER_QUEUE="+s.queue+"-other",
		"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t), "ORK_ARTIFACT_BACKEND=s3",
		"ORK_ORKER_ID="+proof.OrkerID("device-other"),
		"ORK_ORKER_PROFILES="+sensorProfiles(otherImage, usbSerialTestDevice, "60s"))
	awaitInventory(t, s, "an Orker whose probe image is of another build", "PROBE_IMAGE_MISMATCH", `"sensor"`)
	s.writeTranscript(t.Name())
}

// lockDriftDevice holds the host-wide lock of the drift node for the test,
// so two runs on one host, of any worktree, never move it at once. The node
// is the test user's, so the lock is too: a file in that user's own cache
// directory, opened without following a link, waited for at most 5 minutes.
func lockDriftDevice(t *testing.T) {
	t.Helper()
	dir, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("the drift node's lock: %v", err)
	}
	dir = filepath.Join(dir, "orkstack")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("the drift node's lock: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "usb-serial-drift.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		t.Fatalf("open the drift node's lock: %v", err)
	}
	deadline := time.Now().Add(5 * time.Minute)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) || time.Now().After(deadline) {
			_ = f.Close()
			t.Fatalf("take the drift node's lock %s: %v", f.Name(), err)
		}
		time.Sleep(time.Second)
	}
	t.Cleanup(func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	})
}

// TestIntegration_ProfileDriftedReDispatched: a device of an Orker's profile
// changes after its boot audit (its node moves to another group), so the
// attempt that meets it fails PROFILE_DRIFTED before any container, the Orker
// cordons itself, and the task runs on another Orker of the pool that offers
// the profile. A PROFILE_DRIFTED attempt never started, so it is an
// interruption (AGENTS.md §4): the task makes one attempt of its own, and
// every attempt A meets (one at dispatch, up to two more from a cordoned
// Orker's open polls, docs/DEPLOY.md) runs it again within the run's
// never-started budget without spending it.
func TestIntegration_ProfileDriftedReDispatched(t *testing.T) {
	requireUSBSerialNode(t, usbSerialTestDevice, 199, false)
	requireRegistry(t, testRegistry())
	lockDriftDevice(t)
	gid := requireUSBSerialNode(t, usbSerialDriftDevice, 198, true)
	other, err := user.LookupGroup("docker")
	if err != nil {
		infraAbsent(t, "the drift test moves %s to the docker group, which this host has not: %v (%s)", usbSerialDriftDevice, err, usbSerialSetup)
	}
	var otherGID int
	if _, err := fmt.Sscan(other.Gid, &otherGID); err != nil || uint32(otherGID) == gid {
		t.Fatalf("the docker group (%s) cannot be the drift node's other group (its own is %d)", other.Gid, gid)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	s := newTimingStack(ctx, t, 1)
	orkerBin := testbin.StaticPath(t, "./cmd/orker")
	probeImage := buildProbeImage(ctx, t, s.root, orkerBin, "orker-probe", binaryCommit(ctx, t, orkerBin))

	// Orker A passes the drift node, audited once at boot and not again for
	// an hour, so the change is met by the attempt, at dispatch.
	driftID := proof.OrkerID("device-drift")
	startOrkerWithEnv(ctx, t, s.temporal, orkerBin, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+driftID, "ORK_ORKER_PROFILES="+sensorProfiles(probeImage, usbSerialDriftDevice, "1h"))
	awaitOffering(t, s, driftID)
	if err := os.Chown(usbSerialDriftDevice, -1, otherGID); err != nil {
		t.Fatalf("move %s to group %d: %v", usbSerialDriftDevice, otherGID, err)
	}
	t.Cleanup(func() {
		if err := os.Chown(usbSerialDriftDevice, -1, int(gid)); err != nil {
			t.Errorf("move %s back to group %d: %v", usbSerialDriftDevice, gid, err)
		}
	})
	fmt.Fprintf(&s.transcript, "moved %s from group %d to %d after %s booted\n\n", usbSerialDriftDevice, gid, otherGID, driftID)

	name := fmt.Sprintf("device-drift-%d", time.Now().UnixNano()%1_000_000_000)
	s.apply(sensorOrkflow(name, 1))
	runID := s.submit(name)
	awaitInventory(t, s, "the Orker that met the drift", driftID, "DEVICE_CHANGED")

	// Orker B passes the other node under the same profile, whose identity
	// names no host path: the task's next attempt runs there.
	sensorID := proof.OrkerID("device-sensor")
	startOrkerWithEnv(ctx, t, s.temporal, orkerBin, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+sensorID, "ORK_ORKER_PROFILES="+sensorProfiles(probeImage, usbSerialTestDevice, "60s"))
	run := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	if got := taskRunOf(t, run, "read").GetOrkerId(); got != sensorID {
		t.Fatalf("the task succeeded on %q, want %q", got, sensorID)
	}
	// A drift is a never-started fault (counted with store faults), not a
	// stopping Orker's never-started hand-back.
	if in := taskRunOf(t, run, "read").GetInterruptions(); in.GetLast().GetReason() != "PROFILE_DRIFTED" ||
		!in.GetLast().GetNeverStarted() || in.GetOwnAttemptsUsed() != 0 {
		t.Fatalf("interruptions = %v, want the last a never-started PROFILE_DRIFTED and no own attempt spent", in)
	}

	stdout := s.mustCLI(0, "--json", "run", "attempts", runID)
	var attempts orkstackv1.ListTaskAttemptsResponse
	if err := api.UnmarshalJSON([]byte(stdout), &attempts); err != nil {
		t.Fatalf("decode run attempts --json: %v\n%s", err, stdout)
	}
	var drifted bool
	for _, a := range attempts.GetAttempts() {
		if a.GetOrkerId() == driftID {
			if a.GetError().GetCode() != "PROFILE_DRIFTED" || a.GetExitCode() != 0 {
				t.Fatalf("%s's attempt = %s (exit %d), want PROFILE_DRIFTED before any container", driftID, a.GetError().GetCode(), a.GetExitCode())
			}
			drifted = true
		}
	}
	if !drifted {
		t.Fatalf("no attempt of %s is recorded:\n%s", driftID, stdout)
	}
	out, err := exec.CommandContext(ctx, "docker", "ps", "-a", "-q", "--filter", "label=ork.orker="+driftID, "--filter", "label=ork.task=read").Output()
	if err != nil || strings.TrimSpace(string(out)) != "" {
		t.Fatalf("%s left a task container (%q, %v)", driftID, out, err)
	}
	s.writeTranscript(t.Name())
}
