package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/testsuite"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestFireExecution: a fired run's starter is the fire's own workflow and
// execution, read from the local activity its preparation runs in, and a
// preparation outside one admits nothing.
func TestFireExecution(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	prepare := func(ctx context.Context) (artifacts.Starter, error) { return fireExecution(ctx) }
	env.RegisterActivity(prepare)
	val, err := env.ExecuteActivity(prepare)
	if err != nil {
		t.Fatal(err)
	}
	var got artifacts.Starter
	if err := val.Get(&got); err != nil {
		t.Fatal(err)
	}
	if got.Kind != artifacts.StarterFire || got.WorkflowID == "" || got.ExecutionID == "" || got.RunID != "" {
		t.Fatalf("starter = %+v; want the fire's workflow and execution", got)
	}
	if _, err := fireExecution(context.Background()); err == nil {
		t.Fatal("a preparation outside its fire was given a starter")
	}
}

// TestWithoutDigest: a store's error that names an admission's object says
// <admission> in its place, and keeps its chain.
func TestWithoutDigest(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	cause := errors.New("store down")
	err := withoutDigest(fmt.Errorf("put metadata/snapshots/default/ork-1/identity/admission-%s.json: %w", sha, cause), sha)
	if strings.Contains(err.Error(), sha) || !strings.Contains(err.Error(), "admission-<admission>.json") || !errors.Is(err, cause) {
		t.Fatalf("withoutDigest = %v", err)
	}
	plain := errors.New("no digest here")
	if withoutDigest(plain, sha) != plain || withoutDigest(nil, sha) != nil {
		t.Fatal("an error that names no digest was rewritten")
	}
}

// TestCheckAdmissionTemplate: a table too large for one record, with the
// room its ids take, is IDENTITY_TABLE_TOO_LARGE, a refusal; one that fits
// passes, and one that breaks the record's rules is internal.
func TestCheckAdmissionTemplate(t *testing.T) {
	template := func(tasks int) artifacts.Admission {
		a := artifacts.Admission{Version: 1, Namespace: "default", Orkflow: "big", OrkflowVersion: "1",
			DefinitionDigest: strings.Repeat("d", 64), Origin: artifacts.AdmittedOrigin{Kind: "submit"},
			EngineBuild: "b", SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1, Tasks: map[string]artifacts.AdmittedTask{}}
		for i := range tasks {
			a.Tasks[fmt.Sprintf("t%05d-%s", i, strings.Repeat("x", 120))] = artifacts.AdmittedTask{Pool: "main", SpecSHA256: strings.Repeat("a", 64)}
		}
		return a
	}
	if err := checkAdmissionTemplate(template(4000)); err != nil {
		t.Fatalf("4000 tasks: %v", err)
	}
	err := checkAdmissionTemplate(template(20000))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), reasonTableTooLarge) {
		t.Fatalf("20000 tasks: %v", err)
	}
	broken := template(1)
	broken.SubjectFormat = "other"
	if err := checkAdmissionTemplate(broken); connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("a broken record: %v", err)
	}
}
