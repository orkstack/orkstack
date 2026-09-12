package api_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidateOrkflowDoesNotWriteAndReturnsDocumentIssues(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	svc := api.NewService(api.Config{Orkflows: store})
	valid := "name: validate-only\ntasks:\n  - id: one\n    uses: busybox:latest\n"
	response, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: valid}))
	if err != nil || response.Msg.GetOrkflow().GetDefinitionDigest() == "" {
		t.Fatalf("ValidateOrkflow = %v, %v", response, err)
	}
	if _, err := store.Get(context.Background(), "default", "validate-only", ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("validation wrote a definition: %v", err)
	}
	invalid, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: "name: bad\ntasks:\n - id: a\n   uses: busybox\n   needs: [missing]\n"}))
	if err != nil || len(invalid.Msg.GetIssues()) != 1 || invalid.Msg.GetIssues()[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR || invalid.Msg.GetIssues()[0].GetField() != "tasks[0].needs[0]" {
		t.Fatalf("invalid validation = %#v, %v", invalid, err)
	}
	missingName, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: "tasks:\n - id: a\n   uses: busybox\n"}))
	if err != nil || missingName.Msg.GetIssues()[0].GetField() != "name" {
		t.Fatalf("missing name issue = %#v, %v", missingName, err)
	}
}

// TestValidateOrkflowWarnsAboutAPinThatKeepsEveryImagesOwnUser covers what is
// left of rule 11 after the compiler has done its half: the compiler REFUSES a
// pin whose tasks disagree about run_as_orker_user, and a pin that agrees on
// false is still exposed to the same sticky-bit hazard, because each container
// then keeps its image's own baked-in USER. That is advice, not a refusal —
// the compiler cannot know an image's USER — so it arrives as a WARNING beside
// a successfully validated document.
func TestValidateOrkflowWarnsAboutAPinThatKeepsEveryImagesOwnUser(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	const allDefault = `
name: pin-default-user
pins:
  build: {}
tasks:
  - id: write
    uses: busybox:latest
    pin: build
  - id: read
    uses: busybox:latest
    pin: build
    needs: [write]
`
	warned, err := svc.ValidateOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: allDefault}))
	if err != nil {
		t.Fatalf("ValidateOrkflow = %v", err)
	}
	if warned.Msg.GetOrkflow().GetDefinitionDigest() == "" {
		t.Fatalf("a warning must not cost the validated document: %#v", warned.Msg)
	}
	issues := warned.Msg.GetIssues()
	if len(issues) != 1 {
		t.Fatalf("issues = %#v, want exactly one warning", issues)
	}
	if issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING {
		t.Errorf("severity = %v, want WARNING", issues[0].GetSeverity())
	}
	if issues[0].GetField() != "pins.build" {
		t.Errorf("field = %q, want pins.build", issues[0].GetField())
	}
	if !strings.Contains(issues[0].GetMessage(), "run_as_orker_user: true") {
		t.Errorf("message %q does not recommend run_as_orker_user: true", issues[0].GetMessage())
	}

	// The recommendation taken is the recommendation gone: a pin whose tasks
	// all opt in has nothing left to warn about.
	agreed := strings.ReplaceAll(allDefault, "    pin: build", "    pin: build\n    run_as_orker_user: true")
	quiet, err := svc.ValidateOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: agreed}))
	if err != nil {
		t.Fatalf("ValidateOrkflow (run_as_orker_user: true) = %v", err)
	}
	if len(quiet.Msg.GetIssues()) != 0 {
		t.Errorf("issues = %#v, want none once the pin opts in", quiet.Msg.GetIssues())
	}

	// And an unpinned document's response is exactly what it was before pins
	// existed: no pins, no issues.
	unpinned, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.ValidateOrkflowRequest{Yaml: "name: unpinned-doc\ntasks:\n  - id: a\n    uses: busybox:latest\n"}))
	if err != nil {
		t.Fatalf("ValidateOrkflow (unpinned) = %v", err)
	}
	if len(unpinned.Msg.GetIssues()) != 0 {
		t.Errorf("unpinned issues = %#v, want none", unpinned.Msg.GetIssues())
	}
}

func TestListRunsCursorKeepsTiesAndRejectsChangedFilters(t *testing.T) {
	runs := engine.NewRunStore()
	at := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	for _, id := range []string{"ork-a", "ork-b", "ork-c"} {
		runs.Save(&orkstackv1.Run{Id: id, OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: timestamppb.New(at)})
	}
	svc := api.NewService(api.Config{Runs: runs})
	first, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{PageSize: 2}))
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Msg.GetRuns(); len(got) != 2 || got[0].GetId() != "ork-c" || got[1].GetId() != "ork-b" || first.Msg.GetNextPageToken() == "" {
		t.Fatalf("first page = %#v", first.Msg)
	}
	second, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{PageSize: 2, PageToken: first.Msg.GetNextPageToken()}))
	if err != nil || len(second.Msg.GetRuns()) != 1 || second.Msg.GetRuns()[0].GetId() != "ork-a" {
		t.Fatalf("second = %#v, %v", second, err)
	}
	_, err = svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{PageToken: first.Msg.GetNextPageToken(), Status: orkstackv1.Status_STATUS_SUCCEEDED}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("changed filters = %v, want InvalidArgument", err)
	}
}

func TestReadArtifactRequiresRecordedReferenceAndDoesNotConfirmCorruptBytes(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.New(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(ctx, "report.txt", strings.NewReader("body"), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately record the wrong checksum: clients may receive bytes but can
	// never receive last=true, which is the completion signal downloads trust.
	ref.SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte("other")))
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{Id: "ork-artifact", Status: orkstackv1.Status_STATUS_SUCCEEDED, TaskRuns: []*orkstackv1.TaskRun{{TaskId: "task", Attempt: 1, Artifacts: []*orkstackv1.ArtifactRef{{Name: ref.Name, Uri: ref.URI, SizeBytes: ref.SizeBytes, Sha256: ref.SHA256}}}}})
	svc := api.NewService(api.Config{Runs: runs, ArtifactStore: store})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	defer server.Close()
	client := orkstackv1connect.NewRunServiceClient(server.Client(), server.URL)
	missing, err := client.ReadArtifact(ctx, connect.NewRequest(&orkstackv1.ReadArtifactRequest{RunId: "ork-artifact", TaskId: "task", Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "nope"}))
	if err == nil {
		defer missing.Close()
		if missing.Receive() {
			t.Fatal("unrecorded artifact yielded bytes")
		}
	}
	stream, err := client.ReadArtifact(ctx, connect.NewRequest(&orkstackv1.ReadArtifactRequest{RunId: "ork-artifact", TaskId: "task", Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "report.txt"}))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	last := false
	for stream.Receive() {
		last = last || stream.Msg().GetLast()
	}
	if last || connect.CodeOf(stream.Err()) != connect.CodeDataLoss {
		t.Fatalf("corrupt read last=%v err=%v", last, stream.Err())
	}
}
