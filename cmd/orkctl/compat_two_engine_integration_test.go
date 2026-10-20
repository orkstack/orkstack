//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The interface compatibility gate against real processes
// (docs/UI_CLI_PARITY.md "Interface compatibility"): two `ork` replicas on one
// Temporal cluster and one MinIO bucket, the harness
// promotion_two_engine_integration_test.go already uses (startReplica,
// isolatedNamespace). Names are stamped so the shared bucket's default
// namespace never mixes two runs' orkflows.

// compatStack is two replicas and the transcript every CLI call appends to.
type compatStack struct {
	a, b       *engineReplica
	transcript strings.Builder
}

func startCompatStack(ctx context.Context, t *testing.T) *compatStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, artifactCfg)

	namespace := isolatedNamespace()
	t.Logf("namespace %s", namespace)
	dialIsolatedNamespace(ctx, t, address, namespace)
	enginePath := testbin.Path(t, "./cmd/ork")
	queue := idempotencyTaskQueue()
	s := &compatStack{
		a: startReplica(ctx, t, "ork-A", enginePath, address, namespace, queue),
		b: startReplica(ctx, t, "ork-B", enginePath, address, namespace, queue),
	}
	s.a.awaitReady(ctx, t)
	s.b.awaitReady(ctx, t)
	return s
}

// cli runs orkctl against one replica, records the exchange, and requires the
// exit code.
func (s *compatStack) cli(t *testing.T, replica *engineReplica, want int, args ...string) (string, string) {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	var stdout, stderr bytes.Buffer
	code := Execute(append(append([]string{}, args...), "--server", "http://"+replica.addr), &stdout, &stderr)
	fmt.Fprintf(&s.transcript, "$ orkctl %s    # via %s\n", strings.Join(quoteArgs(args), " "), replica.name)
	s.transcript.WriteString(stdout.String())
	s.transcript.WriteString(stderr.String())
	fmt.Fprintf(&s.transcript, "(exit %d)\n\n", code)
	if code != want {
		t.Fatalf("orkctl %v via %s exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			args, replica.name, code, want, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}

func (s *compatStack) applyFile(t *testing.T, replica *engineReplica, document string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "doc.ork.yaml")
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write document: %v", err)
	}
	stdout, _ := s.cli(t, replica, exitOK, "apply", path)
	return stdout
}

func resolvedOn(ctx context.Context, t *testing.T, replica *engineReplica, name string) string {
	t.Helper()
	got, err := replica.flows.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: name}))
	if err != nil {
		t.Fatalf("GetOrkflow(%s) on %s: %v", name, replica.name, err)
	}
	return got.Msg.GetOrkflow().GetVersion()
}

// compatReleaseBlock is the shared compatibility fixture under a stamped name:
// version 2.1.0, or 3.0.0 (adds a required region and a defaulted timeout,
// drops output url, adds sbom, rewrites every task).
func compatReleaseBlock(name, version string) string {
	if version == "2.1.0" {
		return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "2.1.0"
inputs:
  image: { type: string, required: true }
  notes: { type: string, default: "" }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: '{{ .tasks.publish.output.value }}'
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["sh", "-c", "echo sign"] }
  - id: publish
    uses: shell
    needs: [sign]
    with: { op: eval, command: ["sh", "-c", "echo publish"] }
`, name)
	}
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "3.0.0"
inputs:
  image: { type: string, required: true }
  notes: { type: string, default: "" }
  region: { type: string, required: true }
  timeout: { type: number, default: 300 }
outputs:
  digest: '{{ .tasks.attest.output.value }}'
  sbom: '{{ .tasks.sbom.output.value }}'
tasks:
  - id: attest
    uses: shell
    with: { op: eval, command: ["sh", "-c", "echo attest"] }
  - id: sbom
    uses: shell
    needs: [attest]
    with: { op: eval, command: ["sh", "-c", "echo sbom"] }
`, name)
}

// TestIntegration_InterfaceGateAcrossTwoReplicas: a breaking version is
// stored with warnings on A, compared and refused on B, moves nothing on
// either replica, and lands once acknowledged.
func TestIntegration_InterfaceGateAcrossTwoReplicas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := startCompatStack(ctx, t)
	name := fmt.Sprintf("release-block-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)

	s.applyFile(t, s.a, compatReleaseBlock(name, "2.1.0"))
	s.cli(t, s.a, exitOK, "orkflow", "promote", name, "2.1.0")
	applied := s.applyFile(t, s.a, compatReleaseBlock(name, "3.0.0"))
	for _, want := range []string{
		"inputs.region: input \"region\" is added and required with no default",
		"outputs.url: output \"url\" is removed",
		fmt.Sprintf("version: %s@3.0.0 breaks the declared interface of %s@2.1.0, the current version (2 breaking changes)", name, name),
	} {
		if !strings.Contains(applied, want) {
			t.Fatalf("apply output lacks %q:\n%s", want, applied)
		}
	}
	awaitOrkflowVisible(ctx, t, s.b.flows, name)

	compat, _ := s.cli(t, s.b, exitOK, "orkflow", "compat", name, "3.0.0")
	if !strings.Contains(compat, "verdict: BREAKING (2 breaking changes)") {
		t.Fatalf("compat on B:\n%s", compat)
	}
	_, refusal := s.cli(t, s.b, exitError, "orkflow", "promote", name, "3.0.0")
	if !strings.Contains(refusal, "failed_precondition: promoting "+name+" to 3.0.0") {
		t.Fatalf("refusal on B:\n%s", refusal)
	}
	for _, replica := range []*engineReplica{s.a, s.b} {
		if got := resolvedOn(ctx, t, replica, name); got != "2.1.0" {
			t.Fatalf("after the refusal %s resolves %s, want 2.1.0", replica.name, got)
		}
		fmt.Fprintf(&s.transcript, "GetOrkflow(%s, \"\") on %s resolves %s\n", name, replica.name, "2.1.0")
	}
	s.transcript.WriteString("\n")
	versions, _ := s.cli(t, s.a, exitOK, "orkflow", "versions", name)
	var marked bool
	for _, line := range strings.Split(versions, "\n") {
		if strings.HasPrefix(line, "3.0.0") && strings.Contains(line, "BREAKING") {
			marked = true
		}
	}
	if !marked {
		t.Fatalf("versions on A does not mark 3.0.0 BREAKING:\n%s", versions)
	}
	promoted, _ := s.cli(t, s.b, exitOK, "orkflow", "promote", name, "3.0.0", "--acknowledge-breaking")
	if !strings.Contains(promoted, "Interface against 2.1.0: BREAKING, acknowledged") {
		t.Fatalf("acknowledged promotion on B:\n%s", promoted)
	}
	for _, replica := range []*engineReplica{s.a, s.b} {
		if got := resolvedOn(ctx, t, replica, name); got != "3.0.0" {
			t.Fatalf("after the acknowledged promotion %s resolves %s, want 3.0.0", replica.name, got)
		}
	}
	proof.Write(t, "orkctl", "interface-gate-two-replicas.txt", []byte(s.transcript.String()))
}

// TestIntegration_CallersSeeCompositionTriggersAndSchedules: a real
// composing orkflow, a trigger following current and a paused, pinned
// Temporal Schedule are exactly the three callers another replica lists.
func TestIntegration_CallersSeeCompositionTriggersAndSchedules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := startCompatStack(ctx, t)
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	block := "release-block-" + stamp
	checkout := "checkout-service-" + stamp
	schedule := "nightly-" + stamp

	s.applyFile(t, s.a, compatReleaseBlock(block, "2.1.0"))
	s.applyFile(t, s.a, fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "4.2.0"
tasks:
  - id: release
    uses: orkflow:%s@2.1.0
    with:
      image: ghcr.io/acme/checkout:4.2
      notes: from checkout
  - id: announce
    uses: shell
    needs: [release]
    condition: '{{ ne .tasks.release.output.url "" }}'
    with: { op: eval, command: ["sh", "-c", "echo {{ .tasks.release.output.digest }}"] }
`, checkout, block))

	created, _ := s.cli(t, s.a, exitOK, "--json", "trigger", "create", "--orkflow", block)
	var trigger struct {
		Trigger struct {
			ID string `json:"id"`
		} `json:"trigger"`
	}
	if err := json.Unmarshal([]byte(created), &trigger); err != nil || trigger.Trigger.ID == "" {
		t.Fatalf("trigger create --json: %v\n%s", err, created)
	}
	t.Cleanup(func() {
		var stdout, stderr bytes.Buffer
		Execute([]string{"trigger", "delete", trigger.Trigger.ID, "--server", "http://" + s.a.addr}, &stdout, &stderr)
	})
	s.cli(t, s.a, exitOK, "schedule", "create", schedule, "--orkflow", block, "--version", "2.1.0",
		"--cron", "0 3 * * *", "--input", "image=ghcr.io/acme/app:1", "--paused")
	t.Cleanup(func() {
		var stdout, stderr bytes.Buffer
		Execute([]string{"schedule", "delete", schedule, "--server", "http://" + s.a.addr}, &stdout, &stderr)
	})
	awaitOrkflowVisible(ctx, t, s.b.flows, checkout)

	human, _ := s.cli(t, s.b, exitOK, "orkflow", "callers", block)
	for _, want := range []string{
		fmt.Sprintf("Callers of %s in namespace default (current version 2.1.0)", block),
		"Moved by a promotion (they follow the current version): triggers 1, schedules 0.",
		"Not moved by a promotion (they pin a version): composing orkflows 1, triggers 0, schedules 1.",
	} {
		if !strings.Contains(human, want) {
			t.Fatalf("callers on B lacks %q:\n%s", want, human)
		}
	}
	jsonOut, _ := s.cli(t, s.b, exitOK, "--json", "orkflow", "callers", block)
	var decoded struct {
		Callers []struct {
			Kind           string   `json:"kind"`
			Name           string   `json:"name"`
			ID             string   `json:"id"`
			PinnedVersion  string   `json:"pinnedVersion"`
			OutputsRead    []string `json:"outputsRead"`
			InputsSupplied []string `json:"inputsSupplied"`
		} `json:"callers"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &decoded); err != nil {
		t.Fatalf("callers --json: %v\n%s", err, jsonOut)
	}
	if len(decoded.Callers) != 3 {
		t.Fatalf("callers = %+v, want exactly three rows", decoded.Callers)
	}
	composing, trig, sched := decoded.Callers[0], decoded.Callers[1], decoded.Callers[2]
	if composing.Kind != "ORKFLOW" || composing.Name != checkout || composing.PinnedVersion != "2.1.0" ||
		strings.Join(composing.OutputsRead, ",") != "digest,url" ||
		strings.Join(composing.InputsSupplied, ",") != "image,notes" {
		t.Fatalf("composing row = %+v", composing)
	}
	if trig.Kind != "TRIGGER" || trig.ID != trigger.Trigger.ID || trig.PinnedVersion != "" {
		t.Fatalf("trigger row = %+v", trig)
	}
	if sched.Kind != "SCHEDULE" || sched.Name != schedule || sched.PinnedVersion != "2.1.0" {
		t.Fatalf("schedule row = %+v", sched)
	}
	if strings.Contains(jsonOut, "ghcr.io/acme/app:1") {
		t.Fatalf("the callers response carries the schedule's input:\n%s", jsonOut)
	}
	proof.Write(t, "orkctl", "callers-two-replicas.txt", []byte(s.transcript.String()))
}

// TestIntegration_ConcurrentPromotionNeverLandsAnUnacknowledgedBreak:
// a→t is COMPATIBLE and b→t is BREAKING (input zone removed). A promotes t
// without acknowledgement while B alternates acknowledged promotions of a and
// b; no landed promotion of t may have replaced b.
func TestIntegration_ConcurrentPromotionNeverLandsAnUnacknowledgedBreak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := startCompatStack(ctx, t)
	name := fmt.Sprintf("gate-race-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	doc := func(version, inputs string) string {
		return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "%s"
%soutputs:
  out: '{{ .tasks.report.output.value }}'
tasks:
  - id: report
    uses: shell
    with: { op: eval, command: ["sh", "-c", "echo %s"] }
`, name, version, inputs, version)
	}
	for _, d := range []string{doc("a", ""), doc("b", "inputs:\n  zone: { type: string }\n"), doc("t", "")} {
		if _, err := s.a.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: d})); err != nil {
			t.Fatalf("ApplyOrkflow: %v", err)
		}
	}
	if _, err := s.a.flows.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: name, Version: "a", AcknowledgeBreaking: true})); err != nil {
		t.Fatalf("promote a: %v", err)
	}
	awaitOrkflowVisible(ctx, t, s.b.flows, name)

	const rounds = 20
	type outcome struct {
		previous string
		err      error
	}
	var mu sync.Mutex
	var outcomes []outcome
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range rounds {
			resp, err := s.a.flows.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
				&orkstackv1.SetCurrentOrkflowVersionRequest{Name: name, Version: "t"}))
			o := outcome{err: err}
			if err == nil {
				o.previous = resp.Msg.GetPreviousVersion()
			}
			mu.Lock()
			outcomes = append(outcomes, o)
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for i := range rounds {
			version := []string{"a", "b"}[i%2]
			// B's own promotions may lose a race too; only A's are measured.
			_, _ = s.b.flows.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
				&orkstackv1.SetCurrentOrkflowVersionRequest{Name: name, Version: version, AcknowledgeBreaking: true}))
		}
	}()
	wg.Wait()

	landed, refused := 0, 0
	for i, o := range outcomes {
		switch {
		case o.err == nil:
			landed++
			if o.previous == "b" {
				t.Fatalf("unacknowledged promotion %d of t replaced b: a BREAKING change landed unacknowledged", i)
			}
		case connect.CodeOf(o.err) == connect.CodeFailedPrecondition:
			refused++
		default:
			t.Fatalf("promotion %d of t failed for an unexpected reason: %v", i, o.err)
		}
	}
	line := fmt.Sprintf("%d unacknowledged promotions of t landed, none replacing b; %d refused\n", landed, refused)
	t.Log(line)
	proof.Write(t, "orkctl", "interface-gate-race.txt", []byte(line))
}
