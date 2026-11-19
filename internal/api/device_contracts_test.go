package api_test

import (
	"context"
	"maps"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const sensorLabel = `{"schemaVersion":1,"devices":{"sensor":"2"}}`

func profiledDoc(devices string) string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: reader
tasks:
  - id: read
    uses: file
    devices: ` + devices + `
`
}

// TestCreate_DeviceContractsBeforeDigests: create copies the image's
// declared contract into the stored task before it takes the definition's
// digest, so the digest covers it, and a run pins that contract.
func TestCreate_DeviceContractsBeforeDigests(t *testing.T) {
	svc, _, _ := interfaceService(t, sensorLabel)
	stored := applyOnce(t, svc, profiledDoc("[sensor]")).GetOrkflow()
	task := stored.GetTasks()[0]
	if want := map[string]string{"sensor": "2"}; !maps.Equal(task.GetDeviceContracts(), want) {
		t.Fatalf("stored device_contracts = %v, want %v", task.GetDeviceContracts(), want)
	}
	digest, err := dsl.DefinitionDigest(stored)
	must.OKf(t, err, "digest")
	if digest != stored.GetDefinitionDigest() {
		t.Fatalf("stored digest %q is not the stored definition's own %q", stored.GetDefinitionDigest(), digest)
	}
	without := proto.Clone(stored).(*orkstackv1.Orkflow)
	without.GetTasks()[0].DeviceContracts = nil
	other, err := dsl.DefinitionDigest(without)
	must.OKf(t, err, "digest")
	if other == digest {
		t.Fatal("the stored digest does not cover the task's contract")
	}

	// An author's matching pin stores the same contract; one that differs is
	// refused before anything is written.
	same := applyOnce(t, svc, profiledDoc("[sensor@2]")).GetOrkflow().GetTasks()[0]
	if !maps.Equal(same.GetDeviceContracts(), task.GetDeviceContracts()) {
		t.Fatalf("sensor@2 stored %v, want %v", same.GetDeviceContracts(), task.GetDeviceContracts())
	}
	_, err = svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: strings.Replace(profiledDoc("[sensor@3]"), "name: reader", "name: wrong-pin", 1)}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "declares contract 2 of sensor") {
		t.Fatalf("a pin the image contradicts = %v, want InvalidArgument naming the image's contract", err)
	}
	requireNothingStored(t, svc, "wrong-pin")
}

// TestCreate_AnImagesProfileTheTaskDoesNotNameIsRefused: validate and create
// both refuse a task whose image needs a profile it does not name.
func TestCreate_AnImagesProfileTheTaskDoesNotNameIsRefused(t *testing.T) {
	svc, _, _ := interfaceService(t, sensorLabel)
	const doc = `apiVersion: orkstack/v1
kind: Orkflow
name: unnamed
tasks:
  - id: read
    uses: file
`
	issues := validate(t, svc, doc).GetIssues()
	if len(issues) != 1 || issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR ||
		issues[0].GetField() != "tasks[0].devices" ||
		!strings.Contains(issues[0].GetMessage(), "needs profile sensor (contract 2); add it to devices:") {
		t.Fatalf("validate issues = %v, want the one ERROR naming the profile", issues)
	}
	_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("apply = %v, want InvalidArgument", err)
	}
	requireNothingStored(t, svc, "unnamed")
}

// TestCreate_AnUndeclaredProfileStoresNoContract: an image that declares no
// contract leaves the stored task, and its digest, exactly as before.
func TestCreate_AnUndeclaredProfileStoresNoContract(t *testing.T) {
	svc, _, _ := interfaceService(t, "")
	task := applyOnce(t, svc, profiledDoc("[sensor]")).GetOrkflow().GetTasks()[0]
	if task.GetDeviceContracts() != nil {
		t.Fatalf("an undeclared profile stored %v", task.GetDeviceContracts())
	}
}
