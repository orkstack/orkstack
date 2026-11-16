//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_S3_IdentityRecords: on the real store, a record laid down
// by the test writer is read back found, held to the store's own instant,
// which lies between when its create was sent and when it answered
// (verify 49); a run's tasks are found on record by a delimited listing
// that folds each task's objects into one entry (verify 51); a resume's
// record created just before its scope is listed is in the
// listing (D26), and a stale second writer of a linked scope is refused
// (D8); and a record of format 2 is unsupported until the binary
// reads format 2.
func TestIntegration_S3_IdentityRecords(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	ctx := context.Background()
	w, err := artifacts.NewRecordTestWriter(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := artifacts.NewRecordStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ns, run := "ns", "ork-records-"+uniqueToken(t)
	admission := strings.Repeat("a", 64)
	values, err := w.Values(ctx, []byte(`{"approved":true}`))
	if err != nil {
		t.Fatal(err)
	}
	resolution := func(task string, format int32) (string, *orkstackv1.RecordedValues) {
		t.Helper()
		detailSHA, err := w.Detail(ctx, &orkstackv1.ResolutionDetail{SchemaVersion: uint32(format), Namespace: ns, RunId: run,
			TaskId: task, Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice"})
		if err != nil {
			t.Fatal(err)
		}
		key, _ := artifacts.RecordKey(artifacts.RecordResolution, ns, run, task, 0)
		return key, &orkstackv1.RecordedValues{SchemaVersion: format, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
			Namespace: ns, RunId: run, TaskId: task, AdmissionSha256: admission, InputBinding: make([]byte, 32),
			DetailSha256: detailSHA, Values: proto.Clone(values).(*orkstackv1.ArtifactRef)}
	}
	want := func(task string) artifacts.RecordWant {
		return artifacts.RecordWant{Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
			Namespace: ns, RunID: run, TaskID: task, AdmissionSHA256: admission}
	}

	key, rec := resolution("approve", 1)
	sent := time.Now()
	if _, err := w.Record(ctx, key, rec); err != nil {
		t.Fatal(err)
	}
	answered := time.Now()
	got, err := store.ReadRecordedValues(ctx, artifacts.SlotRun, nil, key, want("approve"))
	if err != nil || got.Answer != artifacts.RecordFound {
		t.Fatalf("read = %+v, %v", got, err)
	}
	stored := got.Value.StoredAt()
	if stored.Before(sent.Truncate(time.Second)) || stored.After(answered.Add(time.Second)) {
		t.Fatalf("verify 49: stored at %s, not between %s and %s", stored, sent, answered)
	}
	t.Logf("verify 49: create sent %s, answered %s, dated by the store %s",
		sent.Format(time.RFC3339Nano), answered.Format(time.RFC3339Nano), stored.Format(time.RFC3339Nano))

	keyOf := map[string]string{"approve": key}
	for i := range 3 {
		id := fmt.Sprintf("t%d", i)
		k, _ := artifacts.RecordKey(artifacts.RecordComposed, ns, run, id, 0)
		keyOf[id] = k
		if i < 2 {
			if err := w.Raw(ctx, k, []byte("{}")); err != nil {
				t.Fatal(err)
			}
		}
	}
	onRecord, err := store.TasksOnRecord(ctx, artifacts.SlotRun, ns, run, []string{"approve", "t0", "t1", "t2"})
	if err != nil || !onRecord["approve"] || !onRecord["t0"] || !onRecord["t1"] || onRecord["t2"] {
		t.Fatalf("verify 51: on record = %v, %v", onRecord, err)
	}

	// D26: a scope's record is found by listing it, so a record created just
	// before the listing must be in it on this store, every time.
	for scope := uint64(1); scope <= 20; scope++ {
		r := artifacts.ResumeRecord{Namespace: ns, RunID: run, Scope: scope, PreviousScope: scope - 1,
			Tasks: []string{"approve"}, ReopenedAwaits: []string{}}
		if err := w.Resume(ctx, r); err != nil {
			t.Fatal(err)
		}
		got, err := store.ResumeOf(ctx, ns, run, scope)
		if err != nil || got.Answer != artifacts.RecordFound || got.Value.Scope != scope {
			t.Fatalf("list after write: scope %d = %+v, %v", scope, got, err)
		}
	}
	if newest, got, err := store.NewestResume(ctx, ns, run); err != nil || newest != 20 || got.Answer != artifacts.RecordFound {
		t.Fatalf("the newest scope = %d, %+v, %v", newest, got, err)
	}
	// D8 as amended: a stale second writer of the linked scope 20, under
	// another plan, is refused, and the newest record stays the linked one.
	stale := artifacts.ResumeRecord{Namespace: ns, RunID: run, Scope: 20, N: 1, PreviousScope: 19,
		Tasks: []string{"stale"}, ReopenedAwaits: []string{}}
	if err := w.Resume(ctx, stale); !errors.Is(err, artifacts.ErrResumeLinkElsewhere) {
		t.Fatalf("a stale second writer = %v", err)
	}
	if newest, got, err := store.NewestResume(ctx, ns, run); err != nil || newest != 20 || got.Answer != artifacts.RecordFound ||
		got.Value.N != 0 || got.Value.Tasks[0] != "approve" {
		t.Fatalf("the newest scope after a stale writer = %d, %+v, %v", newest, got, err)
	}

	key2, rec2 := resolution("approve-2", 2)
	if _, err := w.Record(ctx, key2, rec2); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ReadRecordedValues(ctx, artifacts.SlotRun, nil, key2, want("approve-2")); err != nil ||
		got.Answer != artifacts.RecordUnsupported || got.Version != 2 {
		t.Fatalf("format 2 before it is read = %+v, %v", got, err)
	}
	restore := artifacts.EnableRecordFormat2()
	defer restore()
	if got, err := store.ReadRecordedValues(ctx, artifacts.SlotRun, nil, key2, want("approve-2")); err != nil || got.Answer != artifacts.RecordFound {
		t.Fatalf("format 2 once read = %+v, %v", got, err)
	}
}
