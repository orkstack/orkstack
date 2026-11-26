package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Under enforce, a record names the pool and Orker that made it and where its
// values lie, which a caller who holds neither run.artifacts.read nor
// orker.read is not shown: its bundle lists the record withheld, with none of
// its bytes. One who holds run.artifacts.read gets the record.
func TestEvidenceRecords_AreWithheldFromACallerWhoMayNotSeeWhereTheyLie(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewRecordStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	sha := strings.Repeat("a", 64)
	detailSHA, err := store.WriteResolutionDetail(ctx, &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "payments", RunId: "ork-r",
		TaskId: "gate", Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "bob", Reason: "no"})
	must.OK(t, err)
	_, err = store.WriteRecordedValues(ctx, &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
		Namespace: "payments", RunId: "ork-r", TaskId: "gate", AdmissionSha256: sha, Rejected: true, DetailSha256: detailSHA,
		DefinitionDigest: "sha256:" + strings.Repeat("d", 64), InputBinding: make([]byte, 32),
		Recorded: &orkstackv1.CommitRecording{AdmissionSha256: sha, Pool: "gpu", OrkerId: "o-1", Time: timestamppb.Now()}})
	must.OK(t, err)
	run := &orkstackv1.Run{Id: "ork-r", Namespace: "payments", OrkflowName: "build", Status: orkstackv1.Status_STATUS_FAILED,
		FinishedAt: timestamppb.New(time.Now().Add(time.Hour)),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "gate", Status: orkstackv1.Status_STATUS_FAILED,
			Await: &orkstackv1.TaskAwait{Deadline: timestamppb.New(time.Now().Add(time.Hour))}}}}
	bind := func(role string) *orkstackv1.Binding {
		return &orkstackv1.Binding{Name: role, Role: role, Scope: "namespace:payments", Subjects: []string{alice.ID}}
	}
	for _, tc := range []struct {
		role  string
		shown bool
	}{{authz.RoleViewer, false}, {authz.RoleDeveloper, true}} {
		t.Run(tc.role, func(t *testing.T) {
			s := newAccessService(t)
			s.access, _ = accessFor(t, authn.ModeEnforce, nil, bind(tc.role))
			s.recordedResults = store
			s.awaitSpacings = &artifacts.RecordSpacings{MissConfirm: time.Millisecond, ReaskInitial: time.Millisecond, ReaskMax: time.Millisecond}
			ctx := asAlice(authn.ModeEnforce)
			shown, err := s.mayShowHeld(ctx, run)
			must.OK(t, err)
			if shown != tc.shown {
				t.Fatalf("mayShowHeld = %v, want %v", shown, tc.shown)
			}
			r := &recordedRun{s: s, run: run, ns: "payments", sha: sha, showHeld: shown}
			e := &evidenceExport{s: s, small: map[string][]byte{}}
			name := artifacts.RecordName{Kind: artifacts.RecordResolution, TaskID: "gate"}
			members, err := e.record(ctx, r, recordedCandidate{name: name, rel: name.Rel(), listed: true})
			must.OK(t, err)
			if !tc.shown {
				if len(members) != 1 || members[0].State != evidence.StateWithheld || members[0].Reason != evidence.ReasonRecordWithheld ||
					members[0].SHA256 != "" || len(e.small) != 0 {
					t.Fatalf("members = %+v, bytes held %d; want the record withheld with no bytes", members, len(e.small))
				}
				return
			}
			if len(members) != 2 || members[0].State != evidence.StatePresent || members[1].Kind != evidence.KindResolutionDetail {
				t.Fatalf("members = %+v; want the record and its detail", members)
			}
		})
	}
}
