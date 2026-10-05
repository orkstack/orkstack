package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
)

// ---------------------------------------------------------------------------
// orkflow compat / callers / promote --acknowledge-breaking
// (interface-compatibility-gate §6)
// ---------------------------------------------------------------------------

const compatBlock210 = `name: release-block
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
    with: { op: eval, command: ["true"] }
  - id: publish
    uses: shell
    needs: [sign]
    with: { op: eval, command: ["true"] }
`

const compatBlock300 = `name: release-block
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
    with: { op: eval, command: ["echo", "{{ .inputs.region }}"] }
  - id: sbom
    uses: shell
    needs: [attest]
    with: { op: eval, command: ["true"] }
`

const compatCheckout = `name: checkout-service
version: "4.2.0"
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
    with:
      image: ghcr.io/acme/checkout:4.2
      notes: from checkout
  - id: announce
    uses: shell
    needs: [release]
    condition: '{{ ne .tasks.release.output.url "" }}'
    with: { op: exec, command: ["echo", "{{ .tasks.release.output.digest }}"] }
`

const compatBilling = `name: billing-sync
version: "1.9.0"
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
    with:
      image: ghcr.io/acme/billing:1.9
  - id: report
    uses: shell
    needs: [release]
    with: { op: exec, command: ["echo", "{{ .tasks.release.output.digest }}"] }
`

func writeCompatDocument(t *testing.T, document string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "doc.ork.yaml")
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write document: %v", err)
	}
	return path
}

// compatFixture applies release-block 2.1.0 (promoted) and 3.0.0.
func compatFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.run("apply", writeCompatDocument(t, compatBlock210)).requireExit(t, exitOK)
	f.run("orkflow", "promote", "release-block", "2.1.0").requireExit(t, exitOK)
	f.run("apply", writeCompatDocument(t, compatBlock300)).requireExit(t, exitOK)
	return f
}

func TestOrkflowCompat_GoldenHumanAndJSON(t *testing.T) {
	f := compatFixture(t)
	human := f.run("orkflow", "compat", "release-block", "2.1.0", "3.0.0").requireExit(t, exitOK)
	requireGolden(t, "orkflow-compat.golden", human.stdout)
	jsonOut := f.run("--json", "orkflow", "compat", "release-block", "2.1.0", "3.0.0").requireExit(t, exitOK)
	requireGolden(t, "orkflow-compat.json.golden", jsonOut.stdout)

	g := newFixture(t)
	g.run("apply", writeVersionedOrkflow(t, "deploy", "1.0.0", "one")).requireExit(t, exitOK)
	g.run("apply", writeVersionedOrkflow(t, "deploy", "2.0.0", "two")).requireExit(t, exitOK)
	unknown := g.run("orkflow", "compat", "deploy", "1.0.0", "2.0.0").requireExit(t, exitOK)
	requireGolden(t, "orkflow-compat-unknown.golden", unknown.stdout)
}

func TestOrkflowCompat_ExitsZeroOnBreaking(t *testing.T) {
	f := compatFixture(t)
	f.run("orkflow", "compat", "release-block", "3.0.0").requireExit(t, exitOK).
		requireContains(t, "verdict: BREAKING (2 breaking changes)", "2.1.0 → 3.0.0")
}

// captureOrkflows records the requests the CLI sends, so a test can assert
// on the wire rather than on a rendering.
type captureOrkflows struct {
	orkstackv1connect.UnimplementedOrkflowServiceHandler
	mu      sync.Mutex
	compare []*orkstackv1.CompareInterfacesRequest
	promote []*orkstackv1.SetCurrentOrkflowVersionRequest
}

func (c *captureOrkflows) CompareInterfaces(_ context.Context, req *connect.Request[orkstackv1.CompareInterfacesRequest]) (*connect.Response[orkstackv1.CompareInterfacesResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compare = append(c.compare, req.Msg)
	return connect.NewResponse(&orkstackv1.CompareInterfacesResponse{
		Verdict: orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE, FromVersion: "1", ToVersion: "2"}), nil
}

func (c *captureOrkflows) SetCurrentOrkflowVersion(_ context.Context, req *connect.Request[orkstackv1.SetCurrentOrkflowVersionRequest]) (*connect.Response[orkstackv1.SetCurrentOrkflowVersionResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.promote = append(c.promote, req.Msg)
	return connect.NewResponse(&orkstackv1.SetCurrentOrkflowVersionResponse{
		Name: req.Msg.GetName(), Namespace: "default", CurrentVersion: req.Msg.GetVersion()}), nil
}

func runAgainstOrkflows(t *testing.T, handler orkstackv1connect.OrkflowServiceHandler, args ...string) result {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	path, h := orkstackv1connect.NewOrkflowServiceHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	server := httptest.NewUnstartedServer(mux)
	// The CLI speaks h2c to an http:// engine, exactly as ork serves it.
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	var stdout, stderr bytes.Buffer
	code := Execute(append(args, "--server", server.URL), &stdout, &stderr)
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

func TestOrkflowCompat_TwoArgumentsCompareAgainstCurrent(t *testing.T) {
	capture := &captureOrkflows{}
	runAgainstOrkflows(t, capture, "orkflow", "compat", "release-block", "3.0.0").requireExit(t, exitOK)
	runAgainstOrkflows(t, capture, "orkflow", "compat", "release-block", "2.1.0", "3.0.0").requireExit(t, exitOK)
	if len(capture.compare) != 2 {
		t.Fatalf("requests = %d, want 2", len(capture.compare))
	}
	if got := capture.compare[0]; got.GetName() != "release-block" || got.GetFromVersion() != "" || got.GetToVersion() != "3.0.0" {
		t.Fatalf("two arguments sent %v, want from \"\" to 3.0.0", got)
	}
	if got := capture.compare[1]; got.GetFromVersion() != "2.1.0" || got.GetToVersion() != "3.0.0" {
		t.Fatalf("three arguments sent %v", got)
	}
}

func TestOrkflowPromote_SendsAcknowledgeBreaking(t *testing.T) {
	capture := &captureOrkflows{}
	runAgainstOrkflows(t, capture, "orkflow", "promote", "release-block", "3.0.0").requireExit(t, exitOK)
	runAgainstOrkflows(t, capture, "orkflow", "promote", "release-block", "3.0.0", "--acknowledge-breaking").requireExit(t, exitOK)
	if len(capture.promote) != 2 || capture.promote[0].GetAcknowledgeBreaking() || !capture.promote[1].GetAcknowledgeBreaking() {
		t.Fatalf("promote requests = %v, want acknowledge_breaking false then true", capture.promote)
	}
}

func TestOrkflowPromote_BreakingRefusalExitsNonzeroWithTheEnginesSentence(t *testing.T) {
	f := compatFixture(t)
	refused := f.run("orkflow", "promote", "release-block", "3.0.0").requireExit(t, exitError)
	for _, want := range []string{"failed_precondition: promoting release-block to 3.0.0 breaks the declared interface of 2.1.0",
		"inputs.region", "outputs.url", "--acknowledge-breaking"} {
		if !strings.Contains(refused.stderr, want) {
			t.Errorf("refusal stderr lacks %q:\n%s", want, refused.stderr)
		}
	}
	f.run("orkflow", "versions", "release-block").requireExit(t, exitOK).requireContains(t, "Current: 2.1.0")

	acknowledged := f.run("orkflow", "promote", "release-block", "3.0.0", "--acknowledge-breaking").requireExit(t, exitOK)
	requireGolden(t, "orkflow-promote-breaking-acknowledged.golden", acknowledged.stdout)
}

// callersFixture adds two composing orkflows, a trigger that follows current,
// a pinned trigger, and two schedules (one pinned, one following current).
func callersFixture(t *testing.T) (*fixture, *cliSchedules) {
	t.Helper()
	schedules := &cliSchedules{records: map[string]*orkstackv1.ScheduleRecord{}}
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.Schedules = schedules })
	f.run("apply", writeCompatDocument(t, compatBlock210)).requireExit(t, exitOK)
	f.run("orkflow", "promote", "release-block", "2.1.0").requireExit(t, exitOK)
	f.run("apply", writeCompatDocument(t, compatBilling)).requireExit(t, exitOK)
	f.run("apply", writeCompatDocument(t, compatCheckout)).requireExit(t, exitOK)
	f.run("orkflow", "promote", "checkout-service", "4.2.0").requireExit(t, exitOK)
	f.run("apply", writeCompatDocument(t, compatBlock300)).requireExit(t, exitOK)
	f.run("apply", writeCompatDocument(t, strings.NewReplacer(`"4.2.0"`, `"4.3.0"`,
		"release-block@2.1.0", "release-block@3.0.0", "notes: from checkout", "region: eu",
		".tasks.release.output.url", ".tasks.release.output.sbom").Replace(compatCheckout))).requireExit(t, exitOK)
	f.run("trigger", "create", "--orkflow", "release-block", "--name", "ci-release").requireExit(t, exitOK)
	f.run("trigger", "create", "--orkflow", "release-block", "--name", "pinned-release", "--version", "2.1.0").
		requireExit(t, exitOK)
	schedules.records["default/nightly-release"] = &orkstackv1.ScheduleRecord{ScheduleId: "sch-nightly",
		Name: "nightly-release", Namespace: "default", OrkflowName: "release-block", OrkflowVersion: "2.1.0"}
	schedules.records["default/hourly-release"] = &orkstackv1.ScheduleRecord{ScheduleId: "sch-hourly",
		Name: "hourly-release", Namespace: "default", OrkflowName: "release-block"}
	return f, schedules
}

var triggerIDPattern = regexp.MustCompile(`trg-[0-9a-f]+`)

func TestOrkflowCallers_GoldenHumanAndJSON(t *testing.T) {
	f, _ := callersFixture(t)
	human := f.run("orkflow", "callers", "release-block").requireExit(t, exitOK)
	requireGolden(t, "orkflow-callers.golden", human.stdout)
	all := f.run("orkflow", "callers", "release-block", "--all-versions").requireExit(t, exitOK)
	requireGolden(t, "orkflow-callers-all-versions.golden", all.stdout)
	jsonOut := f.run("--json", "orkflow", "callers", "release-block").requireExit(t, exitOK)
	// Trigger ids are random; the shape is what the golden pins.
	requireGolden(t, "orkflow-callers.json.golden", triggerIDPattern.ReplaceAllString(jsonOut.stdout, "trg-ID"))

	empty := f.run("orkflow", "callers", "checkout-service").requireExit(t, exitOK)
	requireGolden(t, "orkflow-callers-empty.golden", empty.stdout)

	f.run("apply", writeVersionedOrkflow(t, "ghost-block", "1.0.0", "ghost")).requireExit(t, exitOK)
	f.run("trigger", "create", "--orkflow", "ghost-block", "--name", "ghost-trigger").requireExit(t, exitOK)
	f.run("orkflow", "delete", "ghost-block", "--force").requireExit(t, exitOK)
	deleted := f.run("orkflow", "callers", "ghost-block").requireExit(t, exitOK)
	requireGolden(t, "orkflow-callers-deleted.golden", deleted.stdout)
}
