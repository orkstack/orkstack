package api_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestSubmitRun_AnAdmissionPinsTheTagsOfItsOwnAudiences: each run of a tree
// pins the setting of the audiences its own tasks ask for, and a run whose
// audiences are not tagged pins nothing.
func TestSubmitRun_AnAdmissionPinsTheTagsOfItsOwnAudiences(t *testing.T) {
	ctx := context.Background()
	setting := map[string]artifacts.SessionTagging{"sts.amazonaws.com": {Transitive: []string{"ork_task"}}, "https://vault": {}}
	for name, tc := range map[string]struct {
		setting, want map[string]artifacts.SessionTagging
	}{
		"tagged": {setting, map[string]artifacts.SessionTagging{"sts.amazonaws.com": {Transitive: []string{"ork_task"}}}},
		"in another order, and twice": {map[string]artifacts.SessionTagging{"sts.amazonaws.com": {Transitive: []string{"ork_task", "ork_pool", "ork_task"}}},
			map[string]artifacts.SessionTagging{"sts.amazonaws.com": {Transitive: []string{"ork_pool", "ork_task"}}}},
		"an empty list": {map[string]artifacts.SessionTagging{"sts.amazonaws.com": {Transitive: []string{}}},
			map[string]artifacts.SessionTagging{"sts.amazonaws.com": {}}},
		"another audience": {map[string]artifacts.SessionTagging{"https://vault": {}}, nil},
		"nothing is":       {nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{identityOrkflow()}, sessionTags: tc.setting})
			run, err := w.submit(t, "deploy", "")
			if err != nil {
				t.Fatal(err)
			}
			read, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, run.GetId(), w.starter.startedPlans()[0].Identity.AdmissionSHA256)
			if err != nil || read.Answer != artifacts.RecordFound {
				t.Fatalf("the admission reads %+v, %v", read, err)
			}
			if !reflect.DeepEqual(read.Value.SessionTags, tc.want) {
				t.Errorf("SessionTags = %v, want %v", read.Value.SessionTags, tc.want)
			}
		})
	}

	// A child holds the identity; its parent, which holds none, pins none.
	parent, child := composingOrkflows()
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{parent, child}, sessionTags: setting})
	run, err := w.submit(t, "release", "")
	if err != nil {
		t.Fatal(err)
	}
	plan := w.starter.startedPlans()[0]
	for where, sha := range map[string]string{"parent": plan.Identity.AdmissionSHA256, "child": plan.Children["ship"].Plan.Identity.AdmissionSHA256} {
		id := run.GetId()
		if where == "child" {
			id = childIDOf(t, w, sha)
		}
		read, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, id, sha)
		if err != nil || read.Answer != artifacts.RecordFound {
			t.Fatalf("the %s's admission reads %+v, %v", where, read, err)
		}
		if tagged := len(read.Value.SessionTags) == 1; tagged != (where == "child") {
			t.Errorf("the %s pins %v", where, read.Value.SessionTags)
		}
	}
}

// childIDOf is the run id the admission record of sha is under.
func childIDOf(t *testing.T, w *admissionWorld, sha string) string {
	t.Helper()
	for _, file := range w.admissionFiles(t) {
		if strings.Contains(file, sha) {
			parts := strings.Split(file, "/")
			return parts[len(parts)-3]
		}
	}
	t.Fatalf("no admission record of %s", sha)
	return ""
}

// TestSubmitRun_AGrantsLoginIsTagged: a task that holds only a grant has its
// login's audience pinned, as a declared identity's is.
func TestSubmitRun_AGrantsLoginIsTagged(t *testing.T) {
	w := newAdmissionWorld(t, admissionOptions{providers: checkedProviders(t), definitions: []*orkstackv1.Orkflow{credentialOrkflow("deployer")},
		grants:      map[string]artifacts.Grant{"deployer": {Provider: "aws", Principal: "role/deploy", Orkflows: []string{"deploy"}}},
		sessionTags: map[string]artifacts.SessionTagging{"sts.amazonaws.com": {}, "https://vault": {}}})
	run, err := w.submit(t, "deploy", "")
	if err != nil {
		t.Fatal(err)
	}
	read, err := w.records.ReadAdmission(context.Background(), artifacts.DefaultNamespace, run.GetId(), w.starter.startedPlans()[0].Identity.AdmissionSHA256)
	if err != nil || read.Answer != artifacts.RecordFound {
		t.Fatalf("the admission reads %+v, %v", read, err)
	}
	if want := (map[string]artifacts.SessionTagging{"sts.amazonaws.com": {}}); len(read.Value.Sets) != 0 || !reflect.DeepEqual(read.Value.SessionTags, want) {
		t.Errorf("sets %v and SessionTags = %v, want no set and %v", read.Value.Sets, read.Value.SessionTags, want)
	}
}
