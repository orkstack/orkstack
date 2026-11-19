//go:build integration

package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// Profile contracts (docs/DEPLOY.md, "Changing a profile"): a device profile
// changing under one name while its pool rolls. Every Orker here passes the
// usb-serial test node (device_profile_integration_test.go); two contracts
// differ by the node's permissions, which gives them different definitions.

// sensorDevices is a contract's device list: the test node at /dev/sensor
// with permissions, indented under its contract.
func sensorDevices(indent, permissions string) string {
	return fmt.Sprintf("%sdevices:\n%s  - path: %s\n%s    container_path: /dev/sensor\n%s    permissions: %s\n",
		indent, indent, usbSerialTestDevice, indent, indent, permissions)
}

// shortContract is ORK_ORKER_PROFILES for sensor in the short form: one
// contract, label, accepting accepts, passing the node with permissions.
func shortContract(probeImage, label, permissions string, accepts ...string) string {
	body := fmt.Sprintf("profiles:\n  probe_image: %s\n  audit_interval: 60s\n  sensor:\n    kind: devices\n    contract: %q\n",
		probeImage, label)
	if len(accepts) > 0 {
		body += "    accepts:\n"
		for _, a := range accepts {
			body += fmt.Sprintf("      - %q\n", a)
		}
	}
	return body + sensorDevices("    ", permissions)
}

// twoContracts is ORK_ORKER_PROFILES for sensor in the long form: contract
// "1" (rw, the default) and "2" (r).
func twoContracts(probeImage string) string {
	return fmt.Sprintf("profiles:\n  probe_image: %s\n  audit_interval: 60s\n  sensor:\n    kind: devices\n    default: \"1\"\n"+
		"    contracts:\n      \"1\":\n%s      \"2\":\n%s", probeImage, sensorDevices("        ", "rw"), sensorDevices("        ", "r"))
}

// contractStack is a timing stack with a static Orker binary and its own
// probe image.
type contractStack struct {
	*timingStack
	orkerBin, probeImage string
}

func newContractStack(ctx context.Context, t *testing.T) *contractStack {
	t.Helper()
	requireUSBSerialNode(t, usbSerialTestDevice, 199, false)
	requireRegistry(t, testRegistry())
	s := newTimingStack(ctx, t, 1)
	orkerBin := testbin.StaticPath(t, "./cmd/orker")
	return &contractStack{timingStack: s, orkerBin: orkerBin,
		probeImage: buildProbeImage(ctx, t, s.root, orkerBin, "orker-probe", binaryCommit(ctx, t, orkerBin))}
}

// orker starts an Orker of the stack's pool named id with profiles, and waits
// until the inventory lists it offering sensor.
func (s *contractStack) orker(id, profiles string) string {
	s.t.Helper()
	orkerID := proof.OrkerID(id)
	startOrkerWithEnv(s.ctx, s.t, s.temporal, s.orkerBin, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+orkerID, "ORK_ORKER_PROFILES="+profiles)
	awaitOffering(s.t, s.timingStack, orkerID)
	return orkerID
}

// refusedSubmit submits name and requires the submit refused, saying every
// one of want; it returns the refusal.
func (s *contractStack) refusedSubmit(name string, want ...string) string {
	s.t.Helper()
	stdout, stderr, code := s.cli(0, "run", "submit", name)
	if code == 0 {
		s.t.Fatalf("submit %s started a run, want a refusal:\n%s", name, stdout)
	}
	for _, w := range want {
		if !strings.Contains(stderr, w) {
			s.t.Fatalf("submit %s refused with\n%s\nwhich does not say %q", name, stderr, w)
		}
	}
	return stderr
}

// ranUnder is the contract the run's task read succeeded under, from its
// attempt record, and the Orker that ran it.
func (s *contractStack) ranUnder(runID string) (contract, orkerID string) {
	s.t.Helper()
	stdout := s.mustCLI(0, "--json", "run", "attempts", runID)
	var attempts orkstackv1.ListTaskAttemptsResponse
	if err := api.UnmarshalJSON([]byte(stdout), &attempts); err != nil {
		s.t.Fatalf("decode run attempts --json: %v\n%s", err, stdout)
	}
	for _, a := range attempts.GetAttempts() {
		if a.GetTaskId() != "read" || a.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
			continue
		}
		for _, p := range a.GetProfiles() {
			if p.GetName() == "sensor" {
				return p.GetContract(), a.GetOrkerId()
			}
		}
	}
	s.t.Fatalf("no successful attempt of read records the sensor profile:\n%s", stdout)
	return "", ""
}

// TestIntegration_ProfileContractRollingChange: a pool mid-roll, one Orker on
// contract 1 and one on contract 2 accepting 1, admits a task whose image
// declares nothing and runs it on either (the contract each Orker selects is
// held by the profile gate's own tests); with the old Orker cordoned by its
// operator, the task runs on the new one under its contract 2; and a new
// default that accepts nothing, beside the old, is refused PROFILE_MISMATCH
// at submit, naming the fix.
func TestIntegration_ProfileContractRollingChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newContractStack(ctx, t)
	old := s.orker("contract-old", shortContract(s.probeImage, "1", "rw"))
	rolled := s.orker("contract-rolled", shortContract(s.probeImage, "2", "r", "1"))

	name := fmt.Sprintf("contract-roll-%d", time.Now().UnixNano()%1_000_000_000)
	s.apply(sensorOrkflow(name, 1))
	for range 2 {
		runID := s.submit(name)
		requireRunStatus(t, awaitTerminalOnB(ctx, t, s.replicas[0], runID), orkstackv1.Status_STATUS_SUCCEEDED)
		contract, on := s.ranUnder(runID)
		if (on != old || contract != "1") && (on != rolled || contract != "2") {
			t.Fatalf("read ran on %q under contract %q; want 1 on %s, or 2 (by accepts:) on %s", on, contract, old, rolled)
		}
		fmt.Fprintf(&s.transcript, "run %s: read ran on %s under contract %s\n\n", runID, on, contract)
	}

	// The operator cordons the old Orker: it serves nothing, but submit still
	// reads its definition of contract 1, and the run goes to the new Orker.
	s.mustCLI(0, "orker", "cordon", old, "--reason", "rolling the sensor profile")
	awaitInventory(t, s.timingStack, "the old Orker cordoned", old, "rolling the sensor profile")
	runID := s.submit(name)
	requireRunStatus(t, awaitTerminalOnB(ctx, t, s.replicas[0], runID), orkstackv1.Status_STATUS_SUCCEEDED)
	if contract, on := s.ranUnder(runID); on != rolled || contract != "2" {
		t.Fatalf("with %s cordoned, read ran on %q under contract %q; want 2 on %s", old, on, contract, rolled)
	}
	s.mustCLI(0, "orker", "uncordon", old)
	awaitUncordoned(t, s.timingStack, old)

	// A new default with no accepts: beside the old Orker no label serves
	// both, so a task whose image declares none has nowhere to run.
	s.orker("contract-breaking", shortContract(s.probeImage, "2", "r"))
	s.refusedSubmit(name, "PROFILE_MISMATCH", "accepts:")
	s.writeTranscript(t.Name())
}

// awaitUncordoned polls `orker list --json` until orkerID is listed and no
// longer cordoned.
func awaitUncordoned(t *testing.T, s *timingStack, orkerID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var list orkstackv1.ListOrkersResponse
		out := s.mustCLI(0, "orker", "list", "--json")
		if err := api.UnmarshalJSON([]byte(out), &list); err != nil {
			t.Fatalf("decode orker list --json: %v\n%s", err, out)
		}
		for _, row := range list.GetOrkers() {
			if row.GetOrkerId() == orkerID && !row.GetCordoned() {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s is still cordoned after 2m:\n%s", orkerID, out)
		}
		select {
		case <-s.ctx.Done():
			t.Fatal(s.ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// declaringImage builds, from the first-party shell image, an image whose
// interface label also declares sensor's contract label, pushes it under
// this process's tag, and returns it pinned by digest.
func declaringImage(ctx context.Context, t *testing.T, label string) string {
	t.Helper()
	docker := func(args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, "docker", args...).Output()
		if err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, exit.Stderr)
			}
			t.Fatalf("docker %s: %v", strings.Join(args, " "), err)
		}
		return strings.TrimSpace(string(out))
	}
	shell := fmt.Sprintf("%s/orkstack/activity-shell:%s", testRegistry(), localcatalog.Tag())
	docker("pull", "--quiet", shell)
	raw := docker("image", "inspect", "--format", `{{index .Config.Labels "org.orkstack.activity.interface"}}`, shell)
	var iface map[string]any
	if err := json.Unmarshal([]byte(raw), &iface); err != nil {
		t.Fatalf("the shell image's interface label %q: %v", raw, err)
	}
	iface["devices"] = map[string]string{"sensor": label}
	declared, err := json.Marshal(iface)
	if err != nil {
		t.Fatal(err)
	}
	contextDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contextDir, "Dockerfile"), []byte("FROM "+shell+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tag := fmt.Sprintf("%s/orkstack-test/sensor-declares-%s:%s", testRegistry(), label, exampleImageTag())
	docker("build", "--quiet", "--provenance=false", "--label", "org.orkstack.activity.interface="+string(declared),
		"-t", tag, contextDir)
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", tag).Run() })
	docker("push", "--quiet", tag)
	return pinnedInRegistry(ctx, t, tag)
}

// pinnedInRegistry is tag, already pushed to the test registry, pinned by the
// digest the registry gives it.
func pinnedInRegistry(ctx context.Context, t *testing.T, tag string) string {
	t.Helper()
	name, reference, _ := strings.Cut(strings.TrimPrefix(tag, testRegistry()+"/"), ":")
	req, err := http.NewRequestWithContext(ctx, http.MethodHead,
		fmt.Sprintf("http://%s/v2/%s/manifests/%s", testRegistry(), name, reference), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, "+
		"application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HEAD %s: %v", tag, err)
	}
	_ = resp.Body.Close()
	digest := resp.Header.Get("Docker-Content-Digest")
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("HEAD %s = %d, digest %q", tag, resp.StatusCode, digest)
	}
	return fmt.Sprintf("%s/%s@%s", testRegistry(), name, digest)
}

// TestIntegration_ProfileContractBreakingChangeByImage: an image that needs a
// breaking change declares the new contract, and runs under it alone on a
// pool whose Orkers carry it beside the old default; an Orker that carries
// only the old contract makes the same submit refused PROFILE_UNAVAILABLE,
// naming the contract to roll in.
func TestIntegration_ProfileContractBreakingChangeByImage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newContractStack(ctx, t)
	s.orker("contract-both-a", twoContracts(s.probeImage))
	s.orker("contract-both-b", twoContracts(s.probeImage))
	image := declaringImage(ctx, t, "2")

	name := fmt.Sprintf("contract-image-%d", time.Now().UnixNano()%1_000_000_000)
	s.apply(strings.Replace(sensorOrkflow(name, 1), "uses: shell", "uses: "+image, 1))
	runID := s.submit(name)
	requireRunStatus(t, awaitTerminalOnB(ctx, t, s.replicas[0], runID), orkstackv1.Status_STATUS_SUCCEEDED)
	if contract, on := s.ranUnder(runID); contract != "2" {
		t.Fatalf("read ran on %q under contract %q, want the image's 2", on, contract)
	}

	old := s.orker("contract-old-only", shortContract(s.probeImage, "1", "rw"))
	s.refusedSubmit(name, "PROFILE_UNAVAILABLE", old, `contract "2"`, "Roll the contract in")
	s.writeTranscript(t.Name())
}

// TestIntegration_ProfileContractSameLabelEdit: two Orkers of a pool that
// define one contract label differently are refused PROFILE_MISMATCH at
// submit, naming the label and both Orkers, so an edit without a new label
// never runs a task under two definitions.
func TestIntegration_ProfileContractSameLabelEdit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newContractStack(ctx, t)
	a := s.orker("contract-edit-a", shortContract(s.probeImage, "1", "rw"))
	b := s.orker("contract-edit-b", shortContract(s.probeImage, "1", "r"))

	name := fmt.Sprintf("contract-edit-%d", time.Now().UnixNano()%1_000_000_000)
	s.apply(sensorOrkflow(name, 1))
	refusal := s.refusedSubmit(name, "PROFILE_MISMATCH", `contract "1"`, "another contract label")
	if !strings.Contains(refusal, a) || !strings.Contains(refusal, b) {
		t.Fatalf("the refusal does not name both Orkers %s and %s:\n%s", a, b, refusal)
	}
	s.writeTranscript(t.Name())
}
