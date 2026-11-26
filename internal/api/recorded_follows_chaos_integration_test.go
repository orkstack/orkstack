//go:build integration

package api

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The chaos suite of tasks that follow an approval, a composed task or a
// container (AGENTS.md §11, B10; docs/TASK_IDENTITY.md), on the rig of
// recorded_results_chaos_integration_test.go. The run's history is written by
// the test, as one who may write to the cluster's namespace writes it.

// record writes the engine's record of awaited or composed task id, and
// answers it as the interpreter names it to a dependent. The values are
// uploaded as the record names them.
func (r *chaosRig) record(id, kind, values string) *orkstackv1.UpstreamRef {
	r.t.Helper()
	up := r.upstream(id, kind, values)
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
		Namespace: artifacts.DefaultNamespace, RunId: r.runID, TaskId: id, AdmissionSha256: r.sha, DefinitionDigest: "sha256:" + strings.Repeat("d", 64),
		InputBinding: make([]byte, 32), Values: up.GetValues()}
	if kind == artifacts.FollowComposed {
		rec.Kind = orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED
		rec.ChildRunId, rec.ChildAdmissionSha256, rec.ChildOutputsSha256 = "ork-child", strings.Repeat("e", 64), strings.Repeat("f", 64)
	} else {
		detail, err := r.records.WriteResolutionDetail(r.ctx, &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: artifacts.DefaultNamespace,
			RunId: r.runID, TaskId: id, Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice"})
		must.OK(r.t, err)
		rec.DetailSha256 = detail
	}
	_, err := r.records.WriteRecordedValues(r.ctx, rec)
	must.OK(r.t, err)
	return up
}

// upstream is task id as the interpreter names it to a dependent, whose
// values are in the store whether or not a record names them.
func (r *chaosRig) upstream(id, kind, values string) *orkstackv1.UpstreamRef {
	r.t.Helper()
	ref, err := r.store.Put(r.ctx, "values.json", strings.NewReader(values), artifacts.PutOptions{})
	must.OK(r.t, err)
	of := artifacts.RecordResolution
	if kind == artifacts.FollowComposed {
		of = artifacts.RecordComposed
	}
	key, err := artifacts.RecordKey(of, artifacts.DefaultNamespace, r.runID, id, 0)
	must.OK(r.t, err)
	return &orkstackv1.UpstreamRef{TaskId: id, RecordUri: r.root + "/" + key,
		Values: &orkstackv1.ArtifactRef{Name: "values.json", Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}}
}

// TestIntegration_RecordedFollows_DispatchedAhead: a task dispatched before
// what it follows is on record is refused at its preflight, naming it, and no
// container starts and nothing is recorded, of it or of what an Orker that
// ran it all the same would hand the engine; dispatched again after the record
// is made, it runs and reads the recorded values.
func TestIntegration_RecordedFollows_DispatchedAhead(t *testing.T) {
	for id, kind := range map[string]string{"gate": artifacts.FollowAwaited, "release": artifacts.FollowComposed} {
		t.Run(kind, func(t *testing.T) {
			r := newChaosRig(t, chaosOptions{commitWait: 120, mayCommit: true, steal: true, follows: []artifacts.FollowRef{{TaskID: id, Kind: kind}}})
			r.given.Store(r.upstream(id, kind, `{"approved":"forged"}`))
			r.start()
			got := r.next()
			if !reasonIn(got.err, orker.ReasonRequiredResultNotRecorded) || !strings.Contains(got.err.Error(), `"`+id+`"`) {
				t.Fatalf("dispatched ahead: %v, want %s naming %q", got.err, orker.ReasonRequiredResultNotRecorded, id)
			}
			// The same from an Orker that runs what it likes.
			if !reasonIn(got.stolen, orker.ReasonRequiredResultNotRecorded) {
				t.Fatalf("a result of its own: %v, want %s", got.stolen, orker.ReasonRequiredResultNotRecorded)
			}
			if r.runtime.creates.Load() != 0 || r.recorded() != nil || r.orkerCommitted() || r.engine.preflights.Load() == 0 {
				t.Fatalf("dispatched ahead: containers %d, record %v, an Orker commit %v, preflights %d",
					r.runtime.creates.Load(), r.recorded(), r.orkerCommitted(), r.engine.preflights.Load())
			}
			r.again(r.record(id, kind, `{"approved":true}`))
			got = r.next()
			if got.err != nil || r.runtime.creates.Load() != 1 || r.recorded() == nil {
				t.Fatalf("after the record: %v, containers %d, record %v", got.err, r.runtime.creates.Load(), r.recorded())
			}
			if have, want := got.result.GetValuesRef().GetSha256(), r.given.Load().GetValues().GetSha256(); have != want {
				t.Fatalf("the container read values of %s, want the recorded %s", have, want)
			}
		})
	}
}

// TestIntegration_RecordedFollows_AContainerNobodyRecorded: a task behind a
// container whose result the engine did not record, as one dispatched ahead of
// its approval has none, is refused whatever its input says of that result;
// once it is on record, the task runs.
func TestIntegration_RecordedFollows_AContainerNobodyRecorded(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 120, mayCommit: true, steal: true,
		follows: []artifacts.FollowRef{{TaskID: "compile", Kind: artifacts.FollowContainer}}})
	up := r.upstream("compile", artifacts.FollowAwaited, `{"built":true}`)
	key, err := artifacts.CommitKey(artifacts.DefaultNamespace, r.runID, "compile")
	must.OK(t, err)
	key, err = artifacts.AttestedCommitKey(key)
	must.OK(t, err)
	up.CommitUri, up.RecordUri = r.root+"/"+key, ""
	r.given.Store(up)
	r.start()
	got := r.next()
	if !reasonIn(got.err, orker.ReasonRequiredResultNotRecorded) || !strings.Contains(got.err.Error(), `"compile"`) ||
		!reasonIn(got.stolen, orker.ReasonRequiredResultNotRecorded) || r.runtime.creates.Load() != 0 || r.recorded() != nil {
		t.Fatalf("%v, of its own %v, containers %d, record %v", got.err, got.stolen, r.runtime.creates.Load(), r.recorded())
	}
	doc, err := artifacts.MarshalCommit(&orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.DefaultNamespace,
		RunId: r.runID, TaskId: "compile", OrkflowName: "deploy", OrkflowVersion: "1", OrkflowDefinitionDigest: r.in.GetOrkflowDefinitionDigest(),
		IdempotencyKey: task.IdempotencyKeyFor(artifacts.DefaultNamespace, r.runID, "compile"), Image: r.in.GetImage(), Attempt: 1,
		OrkerId: r.engine.orkerID, Values: up.GetValues(), Recorded: &orkstackv1.CommitRecording{AdmissionSha256: r.sha}})
	must.OK(t, err)
	_, err = r.raw.PutObject(r.ctx, &s3.PutObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(key), Body: bytes.NewReader(doc)})
	must.OK(t, err)
	r.again(up)
	if got := r.next(); got.err != nil || r.runtime.creates.Load() != 1 || r.recorded() == nil {
		t.Fatalf("after the record: %v, containers %d, record %v", got.err, r.runtime.creates.Load(), r.recorded())
	}
}

// TestIntegration_RecordedFollows_ChangedReferences: a dependent dispatched
// with other values than the record of its awaited or its composed upstream
// names, or another run's record, fails UPSTREAM_RESULT_NOT_RECORDED with no
// container; dispatched with the recorded ones, it runs.
func TestIntegration_RecordedFollows_ChangedReferences(t *testing.T) {
	for id, kind := range map[string]string{"gate": artifacts.FollowAwaited, "release": artifacts.FollowComposed} {
		t.Run(kind, func(t *testing.T) {
			r := newChaosRig(t, chaosOptions{commitWait: 120, mayCommit: true, follows: []artifacts.FollowRef{{TaskID: id, Kind: kind}}})
			recorded := r.record(id, kind, `{"approved":true}`)
			forged := r.upstream(id, kind, `{"approved":"forged"}`)
			r.given.Store(forged)
			r.start()
			if got := r.next(); !reasonIn(got.err, orker.ReasonUpstreamNotRecorded) || !strings.Contains(got.err.Error(), `"`+id+`"`) {
				t.Fatalf("other values: %v", got.err)
			}
			elsewhere := r.upstream(id, kind, `{"approved":true}`)
			elsewhere.RecordUri = strings.Replace(elsewhere.RecordUri, r.runID, "ork-another", 1)
			r.again(elsewhere)
			if got := r.next(); !reasonIn(got.err, orker.ReasonUpstreamNotRecorded) {
				t.Fatalf("another run's record: %v", got.err)
			}
			if r.runtime.creates.Load() != 0 || r.recorded() != nil {
				t.Fatalf("containers %d, record %v", r.runtime.creates.Load(), r.recorded())
			}
			r.again(recorded)
			if got := r.next(); got.err != nil || r.runtime.creates.Load() != 1 || r.recorded() == nil {
				t.Fatalf("the recorded ones: %v, containers %d, record %v", got.err, r.runtime.creates.Load(), r.recorded())
			}
		})
	}
}
