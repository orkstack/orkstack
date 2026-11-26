package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The await doors of a run whose results the engine records
// (docs/TASK_IDENTITY.md), over the real local record store, whose clock is
// this host's. faultyRecords is for what a real store cannot be made to do on
// demand: not answer, and answer late.
type faultyRecords struct {
	*artifacts.RecordStore
	readErr  error
	failRead int  // the read, from 1, that fails
	hang     bool // and it fails only once its context ends
	onFail   func()
	reads    atomic.Int64 // bumped by concurrent row readers
	writeErr error
	onWrite  func()
	listErr  error
	commits  atomic.Int64 // how many commits were read by key, by concurrent row readers
	// afterWrite runs once the record is there, before it is read back.
	afterWrite func()
}

// idsDown is an id store that does not answer.
type idsDown struct{ api.IncarnationStore }

func (idsDown) OrkflowID(context.Context, string, string, time.Time) (string, bool, error) {
	return "", false, errors.New("id store down")
}

func (idsDown) EnsureOrkflowID(context.Context, string, string, time.Time) (string, error) {
	return "", errors.New("id store down")
}

func (f *faultyRecords) ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error) {
	if n := f.reads.Add(1); f.readErr != nil && (f.failRead == 0 || n == int64(f.failRead)) {
		if f.hang {
			<-ctx.Done()
		}
		if f.onFail != nil {
			f.onFail()
		}
		return artifacts.RecordRead[*artifacts.RecordedFound]{}, f.readErr
	}
	return f.RecordStore.ReadRecordedValues(ctx, class, spacing, key, want)
}

func (f *faultyRecords) WriteRecordedValues(ctx context.Context, rec *orkstackv1.RecordedValues) (string, error) {
	if f.writeErr != nil {
		return "", f.writeErr
	}
	if f.onWrite != nil {
		f.onWrite()
	}
	sha, err := f.RecordStore.WriteRecordedValues(ctx, rec)
	if err == nil && f.afterWrite != nil {
		f.afterWrite()
	}
	return sha, err
}

type recordedDoor struct {
	*awaitFixture
	t       *testing.T
	dir     string
	records *faultyRecords
	adm     artifacts.Admission
	sha     string
	clock   *testClock
	seeded  *orkstackv1.Run
}

// testClock is the service's clock, which only a test moves.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

const recordedRun = "ork-1"

func newRecordedDoor(t *testing.T) *recordedDoor {
	t.Helper()
	dir := t.TempDir()
	cfg := artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir}
	store, err := artifacts.NewRecordStore(context.Background(), cfg)
	must.OK(t, err)
	values, err := artifacts.NewLocalStore(dir)
	must.OK(t, err)
	fast := &artifacts.RecordSpacings{MissConfirm: time.Millisecond, ReaskInitial: time.Millisecond, ReaskMax: 2 * time.Millisecond, AttemptMargin: time.Millisecond}
	d := &recordedDoor{t: t, dir: dir, records: &faultyRecords{RecordStore: store}, clock: &testClock{now: time.Now()}}
	d.awaitFixture = newAwaitFixture(t, func(c *api.Config) {
		c.ArtifactStore = values
		c.AwaitRecords, c.AwaitChecks, c.AwaitSpacings = d.records, &identity.FollowsCheck{Store: store, Spacing: fast}, fast
		c.RecordedResults = d.records
		c.Incarnations = idsDown{}
		c.ClockSkew = time.Minute
		c.Now = d.clock.Now
	})
	d.adm = artifacts.Admission{Version: 1, RunID: recordedRun, Namespace: artifacts.DefaultNamespace, Orkflow: d.wf.GetName(), OrkflowVersion: "1",
		DefinitionDigest: strings.Repeat("d", 64), Starter: artifacts.Starter{Kind: artifacts.StarterEngine}, Origin: artifacts.AdmittedOrigin{Kind: "submit"},
		Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC().Truncate(time.Second), EngineBuild: "b", SubjectFormat: artifacts.SubjectFormatOrk,
		SpecVersion: 1, ValueRecords: 1, Tasks: map[string]artifacts.AdmittedTask{}}
	d.seed(time.Hour)
	return d
}

// seed writes the admission and records a run whose await gate ends in.
func (d *recordedDoor) seed(in time.Duration) *orkstackv1.Run {
	d.t.Helper()
	raw, sha, err := artifacts.EncodeAdmission(d.adm)
	must.OK(d.t, err)
	key, err := artifacts.AdmissionKey(d.adm.Namespace, d.adm.RunID, sha)
	must.OK(d.t, err)
	path := filepath.Join(d.dir, filepath.FromSlash(key))
	must.OK(d.t, os.MkdirAll(filepath.Dir(path), 0o755))
	must.OK(d.t, os.WriteFile(path, raw, 0o644))
	d.sha = sha
	run := d.seedRun(d.t, recordedRun, "default", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING})
	taskRunFor(run, "gate").Await.Deadline = timestamppb.New(time.Now().Add(in))
	d.seeded = run
	d.reader.setRun(run)
	d.reader.setStart(recordedRun, &engine.RunStartRecord{Orkflow: d.wf, Plan: engine.RunPlan{Namespace: "default",
		Identity: &engine.IdentityPlan{AdmissionSHA256: sha},
		Awaits:   map[string]engine.AwaitCredential{"gate": credentialFor(d.t, awaitToken)}}})
	return run
}

func (d *recordedDoor) want() (string, artifacts.RecordWant) {
	d.t.Helper()
	key, err := artifacts.RecordKey(artifacts.RecordResolution, "default", recordedRun, "gate", 0)
	must.OK(d.t, err)
	return key, artifacts.RecordWant{Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, Namespace: "default",
		RunID: recordedRun, TaskID: "gate", AdmissionSHA256: d.sha}
}

// onRecord is the record there, or nil.
func (d *recordedDoor) onRecord() *artifacts.RecordedFound {
	d.t.Helper()
	key, want := d.want()
	read, err := d.records.RecordStore.ReadRecordedValues(context.Background(), artifacts.SlotDoor,
		&artifacts.RecordSpacings{MissConfirm: time.Millisecond, ReaskInitial: time.Millisecond, ReaskMax: time.Millisecond}, key, want)
	must.OK(d.t, err)
	if read.Answer != artifacts.RecordFound {
		return nil
	}
	return read.Value
}

// storedAt dates the record there as the store would have.
func (d *recordedDoor) storedAt(at time.Time) {
	d.t.Helper()
	key, _ := d.want()
	must.OK(d.t, os.Chtimes(filepath.Join(d.dir, filepath.FromSlash(key)), at, at))
}

func (d *recordedDoor) approve(values, principal string) (*orkstackv1.ResolveAwaitResponse, error) {
	return d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", ValuesJson: values}, principal)
}

func (d *recordedDoor) refused(err error, code connect.Code, reason string) {
	d.t.Helper()
	if connect.CodeOf(err) != code || !strings.Contains(err.Error(), reason) {
		d.t.Fatalf("err = %v; want %v %s", err, code, reason)
	}
}

const approved = `{"approved":true,"note":"n"}`

// TestRecordedDoor_RecordsThenTells: the record and its detail are on file,
// bound to the run's admission and input, before the run is told.
func TestRecordedDoor_RecordsThenTells(t *testing.T) {
	d := newRecordedDoor(t)
	toldBefore := false
	d.records.onWrite = func() { toldBefore = len(d.starter.delivered()) > 0 }
	resp, err := d.approve(approved, "alice")
	must.OK(t, err)
	if resp.GetRepeat() || resp.GetToldAt() == nil || resp.GetDeliveryPending() || resp.GetDeadlinePassed() || toldBefore {
		t.Fatalf("resp %v, told before the record %v", resp, toldBefore)
	}
	f := d.onRecord()
	if f == nil {
		t.Fatal("nothing is on record")
	}
	rejected, values, follows := f.Standing()
	if rejected || values != resp.GetValuesSha256() || len(follows) != 0 || f.Detail().GetPrincipal() != "alice" ||
		f.Detail().GetVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API {
		t.Fatalf("on record: rejected %v values %s detail %v", rejected, values, f.Detail())
	}
	adoption, ok := f.InTime(time.Now().Add(time.Hour), time.Time{})
	if !ok || !identity.InputBindingMatches([]byte(strings.Repeat("\x0f", 16)), []byte("null"), adoption.Record().GetInputBinding()) {
		t.Fatalf("the record is not bound to the run's input")
	}
	if got := d.starter.delivered(); len(got) != 1 || got[0].GetTaskId() != "gate" {
		t.Fatalf("delivered %v", got)
	}
}

// TestRecordedDoor_TheFirstRecordStands: a repeat is success and is told
// once a pace; any other answer is refused, whoever gives it, and neither
// changes the record.
func TestRecordedDoor_TheFirstRecordStands(t *testing.T) {
	d := newRecordedDoor(t)
	_, err := d.approve(approved, "alice")
	must.OK(t, err)
	first := d.onRecord().RecordSHA256()
	again, err := d.approve(approved, "alice")
	if err != nil || !again.GetRepeat() || again.GetToldAt() == nil {
		t.Fatalf("a repeat: %v, %v", again, err)
	}
	_, err = d.approve(`{"approved":false,"note":"n"}`, "bob")
	d.refused(err, connect.CodeFailedPrecondition, "AWAIT_ALREADY_RESOLVED")
	if !strings.Contains(err.Error(), "alice") {
		t.Fatalf("a reader of the run is not told who: %v", err)
	}
	_, err = d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", Reject: true, Reason: "no"}, "bob")
	d.refused(err, connect.CodeFailedPrecondition, "AWAIT_ALREADY_RESOLVED")
	redelivered, err := d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", Redeliver: true}, "bob")
	if err != nil || !redelivered.GetToldAt().AsTime().Equal(again.GetToldAt().AsTime()) {
		t.Fatalf("a redeliver inside the pace: %v, %v", redelivered, err)
	}
	if n := len(d.starter.delivered()); n != 1 {
		t.Fatalf("the run was told %d times inside one pace", n)
	}
	if d.onRecord().RecordSHA256() != first {
		t.Fatal("the record changed")
	}
	// Past the pace the next request tells the run again.
	d.clock.advance(11 * time.Second)
	later, err := d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", Redeliver: true}, "bob")
	if err != nil || !later.GetToldAt().AsTime().After(again.GetToldAt().AsTime()) || len(d.starter.delivered()) != 2 {
		t.Fatalf("a redeliver past the pace: %v, %v, told %d times", later, err, len(d.starter.delivered()))
	}
}

// TestRecordedDoor_TheDeadline: no new answer is recorded at or past it; one
// recorded in time is repeated whatever the clock says; one the store wrote
// late is refused whoever asks, and tells nothing.
func TestRecordedDoor_TheDeadline(t *testing.T) {
	t.Run("a new answer", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.seed(-time.Second)
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeFailedPrecondition, "AWAIT_DEADLINE_PASSED")
		if d.onRecord() != nil || len(d.starter.delivered()) != 0 {
			t.Fatal("a late answer was recorded or told")
		}
	})
	t.Run("a repeat of one recorded in time", func(t *testing.T) {
		d := newRecordedDoor(t)
		_, err := d.approve(approved, "alice")
		must.OK(t, err)
		d.seed(-time.Second)
		d.storedAt(time.Now().Add(-time.Minute))
		resp, err := d.approve(approved, "alice")
		if err != nil || !resp.GetRepeat() || !resp.GetDeadlinePassed() || resp.GetToldAt() == nil {
			t.Fatalf("%v, %v", resp, err)
		}
	})
	for name, req := range map[string]*orkstackv1.ResolveAwaitRequest{
		"the same answer":  {RunId: recordedRun, TaskId: "gate", ValuesJson: approved},
		"another answer":   {RunId: recordedRun, TaskId: "gate", ValuesJson: `{"approved":false,"note":"n"}`},
		"deliver it again": {RunId: recordedRun, TaskId: "gate", Redeliver: true},
	} {
		t.Run("stored late: "+name, func(t *testing.T) {
			d := newRecordedDoor(t)
			_, err := d.approve(approved, "alice")
			must.OK(t, err)
			told := len(d.starter.delivered())
			d.storedAt(time.Now().Add(2 * time.Hour))
			_, err = d.resolve(req, "alice")
			d.refused(err, connect.CodeFailedPrecondition, "AWAIT_DEADLINE_PASSED")
			if len(d.starter.delivered()) != told {
				t.Fatal("a late record was told")
			}
		})
	}
	t.Run("stored late by its own create", func(t *testing.T) {
		d := newRecordedDoor(t)
		// A create that lands after the deadline its request met, as the
		// store dates it.
		d.records.afterWrite = func() { d.storedAt(time.Now().Add(2 * time.Hour)) }
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeFailedPrecondition, "AWAIT_DEADLINE_PASSED")
		if d.onRecord() == nil || len(d.starter.delivered()) != 0 {
			t.Fatal("the late record should stand, untold")
		}
	})
}

// TestRecordedDoor_WhatTheAwaitFollows: nothing is recorded or told before
// it is on record, and the record then names it.
func TestRecordedDoor_WhatTheAwaitFollows(t *testing.T) {
	d := newRecordedDoor(t)
	d.adm.Follows = map[string][]artifacts.FollowRef{"gate": {{TaskID: "build", Kind: artifacts.FollowContainer}}}
	d.seed(time.Hour)
	_, err := d.approve(approved, "alice")
	d.refused(err, connect.CodeFailedPrecondition, "REQUIRED_RESULT_NOT_RECORDED")
	if !strings.Contains(err.Error(), "build") || d.onRecord() != nil || len(d.starter.delivered()) != 0 {
		t.Fatalf("recorded or told, or not named: %v", err)
	}
	key, err := artifacts.CommitKey("default", recordedRun, "build")
	must.OK(t, err)
	_, err = d.records.RecordCommit(context.Background(), key, &orkstackv1.ActivityCommit{SchemaVersion: 1, Namespace: "default", RunId: recordedRun,
		TaskId: "build", Values: &orkstackv1.ArtifactRef{Sha256: strings.Repeat("c", 64)}, Recorded: &orkstackv1.CommitRecording{AdmissionSha256: d.sha}})
	must.OK(t, err)
	_, err = d.approve(approved, "alice")
	must.OK(t, err)
	if _, _, follows := d.onRecord().Standing(); len(follows) != 1 || follows[0].GetTaskId() != "build" {
		t.Fatalf("the record follows %v", follows)
	}
}

// TestRecordedDoor_Faults: what could not be asked is unavailable and is
// never a record or a telling that was not; a record that stands is never
// answered as a failure.
func TestRecordedDoor_Faults(t *testing.T) {
	down := &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("bucket unreachable")}
	t.Run("the key could not be read", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.records.readErr = down
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeUnavailable, "try again")
		if d.onRecord() != nil || len(d.starter.delivered()) != 0 {
			t.Fatal("recorded or told")
		}
	})
	t.Run("the create failed", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.records.writeErr = down
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeUnavailable, "try again")
		if len(d.starter.delivered()) != 0 {
			t.Fatal("told")
		}
	})
	t.Run("the read-back failed", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.records.readErr, d.records.failRead = down, 2
		resp, err := d.approve(approved, "alice")
		if err != nil || resp.GetToldAt() == nil || resp.GetDeadlinePassed() {
			t.Fatalf("%v, %v; the create's own answer was in time", resp, err)
		}
	})
	t.Run("the read-back never answered", func(t *testing.T) {
		t.Parallel()
		d := newRecordedDoor(t)
		d.records.readErr, d.records.failRead, d.records.hang = down, 2, true
		began := time.Now()
		resp, err := d.approve(approved, "alice")
		if err != nil || resp.GetToldAt() == nil || len(d.starter.delivered()) != 1 {
			t.Fatalf("%v, %v; the run is told of a record that could not be read back", resp, err)
		}
		// Well within a client's own timeout, 30 s by default.
		if took := time.Since(began); took > 15*time.Second {
			t.Fatalf("the caller waited %s for an answer that stood", took)
		}
	})
	t.Run("the read-back failed past the deadline", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.records.readErr, d.records.failRead = down, 2
		d.records.onFail = func() { d.clock.advance(2 * time.Hour) }
		resp, err := d.approve(approved, "alice")
		if err != nil || resp.GetToldAt() == nil || !resp.GetDeadlinePassed() {
			t.Fatalf("%v, %v; the deadline passed while the record was read back", resp, err)
		}
	})
	t.Run("what a repeat's caller may learn could not be decided", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.starter.signalErr = errors.New("frontend down")
		_, err := d.approve(approved, "alice")
		must.OK(t, err)
		d.starter.signalErr = nil
		told := len(d.starter.delivered())
		// A binding pinned to an id the store does not answer for.
		applyIn(t, d.svc, "default", awaitedYAML)
		cas := artifacts.NewMemoryCAS()
		store := artifacts.NewAccessStoreOn(cas)
		_, err = store.Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{{Name: "b", Role: authz.RoleViewer,
			Scope: "namespace:default", ScopeId: "ns_1", Subjects: []string{"user:corp:carol"}}}}, nil, artifacts.AccessWrite{At: time.Now()})
		must.OK(t, err)
		d.svc.SetAccessForTest(&api.AccessControl{Mode: authn.ModeEnforce, Policies: api.NewPolicyCache(store, api.PolicyCacheOptions{})})
		ctx := authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce),
			&authn.Principal{Kind: authn.KindUser, ID: "user:corp:carol", Issuer: "corp", Method: authn.MethodJWT})
		_, err = d.svc.ResolveAwait(ctx, connect.NewRequest(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", ValuesJson: approved}))
		d.refused(err, connect.CodeUnavailable, "could not be decided")
		if len(d.starter.delivered()) != told+1 {
			t.Fatal("the run was not told of the record that stands")
		}
	})
	t.Run("the row holds no deadline", func(t *testing.T) {
		d := newRecordedDoor(t)
		run := d.seed(time.Hour)
		taskRunFor(run, "gate").Await.Deadline = nil
		d.reader.setRun(run)
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeUnavailable, "when await")
	})
	t.Run("the admission is gone", func(t *testing.T) {
		d := newRecordedDoor(t)
		must.OK(t, os.Rename(filepath.Join(d.dir, "metadata", "snapshots"), filepath.Join(d.dir, "metadata", "gone")))
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeUnavailable, "admission")
	})
	for format, want := range map[int]struct {
		code connect.Code
		says string
	}{0: {connect.CodeFailedPrecondition, "pins no record format"}, 2: {connect.CodeUnavailable, "newer replica"}} {
		t.Run(fmt.Sprintf("the run pins format %d", format), func(t *testing.T) {
			d := newRecordedDoor(t)
			d.adm.ValueRecords = format
			d.seed(time.Hour)
			_, err := d.approve(approved, "alice")
			d.refused(err, want.code, want.says)
			if d.onRecord() != nil || len(d.starter.delivered()) != 0 {
				t.Fatal("recorded or told")
			}
		})
	}
	t.Run("a record in a later format", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.overwrite(`{"schemaVersion":2}`)
		for _, req := range []*orkstackv1.ResolveAwaitRequest{{RunId: recordedRun, TaskId: "gate", ValuesJson: approved}, {RunId: recordedRun, TaskId: "gate", Redeliver: true}} {
			_, err := d.resolve(req, "alice")
			d.refused(err, connect.CodeUnavailable, "newer format")
		}
		if len(d.starter.delivered()) != 0 {
			t.Fatal("told")
		}
	})
	t.Run("a record that cannot be used", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.overwrite(`{"schemaVersion":1}`)
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeFailedPrecondition, "RESOLUTION_RECORD_INVALID")
		if got := d.starter.delivered(); len(got) != 1 || got[0].GetValues() != nil || got[0].GetReject() {
			t.Fatalf("the run was told %v; want its task alone", got)
		}
	})
	t.Run("the cluster took no Signal", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.starter.signalErr = errors.New("frontend down")
		resp, err := d.approve(approved, "alice")
		if err != nil || !resp.GetDeliveryPending() || resp.GetToldAt() != nil || d.onRecord() == nil {
			t.Fatalf("%v, %v", resp, err)
		}
		d.starter.signalErr = nil
		resp, err = d.approve(approved, "alice")
		if err != nil || !resp.GetRepeat() || resp.GetDeliveryPending() || resp.GetToldAt() == nil {
			t.Fatalf("a repeat of a resolution not told: %v, %v", resp, err)
		}
	})
	t.Run("the caller hung up once it was recorded", func(t *testing.T) {
		d := newRecordedDoor(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		d.records.afterWrite = cancel
		_, err := d.svc.ResolveAwait(ctx, connect.NewRequest(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", ValuesJson: approved}))
		if err != nil || len(d.starter.delivered()) != 1 {
			t.Fatalf("err %v, told %d times; the run is told of what is on record", err, len(d.starter.delivered()))
		}
	})
	t.Run("what was recorded cannot be used", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.records.afterWrite = func() { d.overwrite(`{"schemaVersion":1}`) }
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeFailedPrecondition, "RESOLUTION_RECORD_INVALID")
	})
	t.Run("the run ended", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.starter.signalErr = engine.ErrRunEnded
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeFailedPrecondition, "AWAIT_RUN_ENDED")
		if d.onRecord() == nil {
			t.Fatal("the record does not stand")
		}
	})
	t.Run("resumed while it was recorded", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.records.onWrite = func() { d.resumed() }
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeFailedPrecondition, "AWAIT_REOPENED")
		if len(d.starter.delivered()) != 0 {
			t.Fatal("told")
		}
	})
	t.Run("resumed before", func(t *testing.T) {
		// The run's state does not say the resume yet, so its doors answer
		// unavailable, and record and tell nothing.
		d := newRecordedDoor(t)
		d.resumed()
		for _, req := range []*orkstackv1.ResolveAwaitRequest{{RunId: recordedRun, TaskId: "gate", ValuesJson: approved}, {RunId: recordedRun, TaskId: "gate", Redeliver: true}} {
			_, err := d.resolve(req, "alice")
			d.refused(err, connect.CodeUnavailable, "was resumed")
		}
		if d.onRecord() != nil || len(d.starter.delivered()) != 0 {
			t.Fatal("recorded or told")
		}
	})
}

// atScope is the record of gate at scope, or nil.
func (d *recordedDoor) atScope(scope uint64) *artifacts.RecordedFound {
	d.t.Helper()
	key, err := artifacts.RecordKey(artifacts.RecordResolution, "default", recordedRun, "gate", scope)
	must.OK(d.t, err)
	_, want := d.want()
	want.Scope = scope
	read, err := d.records.RecordStore.ReadRecordedValues(context.Background(), artifacts.SlotDoor,
		&artifacts.RecordSpacings{MissConfirm: time.Millisecond, ReaskInitial: time.Millisecond, ReaskMax: time.Millisecond}, key, want)
	must.OK(d.t, err)
	if read.Answer != artifacts.RecordFound {
		return nil
	}
	return read.Value
}

// says sets the resume scope the run's state says gate is of.
func (d *recordedDoor) says(scope int64) {
	d.t.Helper()
	taskRunFor(d.seeded, "gate").ResultEpoch = scope
	d.reader.setRun(d.seeded)
}

// TestRecordedDoor_AResumedAwait: an await a resume opened again is resolved
// under that resume, whatever was on record before it, which is neither
// repeated nor touched; the run's state is believed of neither: a scope that
// is not the newest, or an await the newest resume opened that its row does
// not say so of, is unavailable and records nothing.
func TestRecordedDoor_AResumedAwait(t *testing.T) {
	t.Run("resolved again", func(t *testing.T) {
		d := newRecordedDoor(t)
		_, err := d.approve(approved, "alice")
		must.OK(t, err)
		base := d.atScope(0)
		d.resumed()
		d.says(5)
		resp, err := d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", Reject: true, Reason: "not this time"}, "bob")
		if err != nil || resp.GetRepeat() {
			t.Fatalf("the resumed await = %v, %v; want a new answer", resp, err)
		}
		again := d.atScope(5)
		if again == nil || d.atScope(0) == nil || d.atScope(0).RecordSHA256() != base.RecordSHA256() {
			t.Fatal("the resumed await's answer is not on record beside the base's")
		}
		if rejected, _, _ := again.Standing(); !rejected {
			t.Fatal("the base's approval was answered")
		}
		resp, err = d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", Reject: true, Reason: "not this time"}, "bob")
		if err != nil || !resp.GetRepeat() {
			t.Fatalf("a repeat = %v, %v", resp, err)
		}
	})
	for name, tc := range map[string]struct {
		state int64
		other int64 // another row's
	}{
		"its row says no resume":              {0, 0},
		"its row says none, and another does": {0, 5},
		"its row says one no resume opened":   {7, 0},
	} {
		t.Run(name, func(t *testing.T) {
			d := newRecordedDoor(t)
			d.resumed()
			for _, row := range d.seeded.GetTaskRuns() {
				if row.GetTaskId() != "gate" {
					row.ResultEpoch = tc.other
				}
			}
			d.says(tc.state)
			_, err := d.approve(approved, "alice")
			d.refused(err, connect.CodeUnavailable, "was resumed")
			if d.atScope(0) != nil || d.atScope(5) != nil || d.atScope(7) != nil || len(d.starter.delivered()) != 0 {
				t.Fatal("recorded or told")
			}
		})
	}
	t.Run("an earlier resume opened it again, and a later one left it", func(t *testing.T) {
		for _, says := range []int64{0, 5, 7, 9} {
			d := newRecordedDoor(t)
			_, err := d.approve(approved, "alice")
			must.OK(t, err)
			d.resumed()
			must.OK(t, d.records.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "default", RunID: recordedRun, Scope: 9,
				PreviousScope: 5, Tasks: []string{"deploy"}}))
			d.seeded.TaskRuns = append(d.seeded.TaskRuns, &orkstackv1.TaskRun{TaskId: "deploy", ResultEpoch: 9})
			d.says(says)
			told := len(d.starter.delivered())
			resp, err := d.approve(approved, "bob")
			if says != 5 {
				// Not where it counts: the base's approval least of all.
				d.refused(err, connect.CodeUnavailable, "was resumed")
				if d.atScope(5) != nil || d.atScope(7) != nil || d.atScope(9) != nil || len(d.starter.delivered()) != told {
					t.Fatalf("a row that says %d: recorded or told", says)
				}
				continue
			}
			if err != nil || resp.GetRepeat() || d.atScope(5) == nil {
				t.Fatalf("the await at the resume that opened it = %v, %v; want a new answer there", resp, err)
			}
		}
	})
	t.Run("a resume that kept it", func(t *testing.T) {
		d := newRecordedDoor(t)
		_, err := d.approve(approved, "alice")
		must.OK(t, err)
		must.OK(t, d.records.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "default", RunID: recordedRun, Scope: 5, Tasks: []string{"deploy"}}))
		d.seeded.TaskRuns = append(d.seeded.TaskRuns, &orkstackv1.TaskRun{TaskId: "deploy", ResultEpoch: 5})
		d.says(0)
		resp, err := d.approve(approved, "alice")
		if err != nil || !resp.GetRepeat() {
			t.Fatalf("a repeat of a kept answer = %v, %v", resp, err)
		}
	})
	t.Run("a run below a resumed root", func(t *testing.T) {
		for _, scope := range []uint64{0, 5} {
			d := newRecordedDoor(t)
			d.adm.RootRunID = "ork-root"
			d.adm.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-root", AdmissionSHA256: strings.Repeat("e", 64), TaskID: "ship"}
			d.seed(time.Hour)
			must.OK(t, d.records.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "default", RunID: "ork-root", Scope: 5, Tasks: []string{"ship"}}))
			d.reader.setStart(recordedRun, &engine.RunStartRecord{Orkflow: d.wf, Plan: engine.RunPlan{Namespace: "default",
				Identity: &engine.IdentityPlan{AdmissionSHA256: d.sha, RootScope: scope},
				Awaits:   map[string]engine.AwaitCredential{"gate": credentialFor(d.t, awaitToken)}}})
			_, err := d.approve(approved, "alice")
			if scope == 0 {
				d.refused(err, connect.CodeUnavailable, "was resumed")
				continue
			}
			if err != nil || d.atScope(0) == nil {
				t.Fatalf("a child started under the newest resume = %v", err)
			}
		}
	})
}

func (d *recordedDoor) resumed() {
	d.t.Helper()
	must.OK(d.t, d.records.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "default", RunID: recordedRun, Scope: 5,
		Tasks: []string{"gate"}, ReopenedAwaits: []string{"gate"}}))
}

func (d *recordedDoor) overwrite(body string) {
	d.t.Helper()
	key, _ := d.want()
	path := filepath.Join(d.dir, filepath.FromSlash(key))
	must.OK(d.t, os.MkdirAll(filepath.Dir(path), 0o755))
	must.OK(d.t, os.WriteFile(path, []byte(body), 0o644))
}

// TestRecordedDoor_Redeliver: it carries no decision, and nothing on record
// is said so.
func TestRecordedDoor_Redeliver(t *testing.T) {
	d := newRecordedDoor(t)
	for _, req := range []*orkstackv1.ResolveAwaitRequest{{ValuesJson: "{}"}, {Reject: true}, {Reason: "r"}} {
		req.RunId, req.TaskId, req.Redeliver = recordedRun, "gate", true
		_, err := d.resolve(req, "alice")
		d.refused(err, connect.CodeInvalidArgument, "redeliver carries no decision")
	}
	_, err := d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", Redeliver: true}, "alice")
	d.refused(err, connect.CodeFailedPrecondition, "nothing is on record")
	d.starter.signalErr = errors.New("frontend down")
	_, err = d.approve(approved, "alice")
	must.OK(t, err)
	d.starter.signalErr = nil
	resp, err := d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", Redeliver: true}, "bob")
	if err != nil || resp.GetToldAt() == nil || resp.GetRepeat() {
		t.Fatalf("%v, %v", resp, err)
	}
	if got := d.starter.delivered(); len(got) != 1 || got[0].GetValues() != nil || len(got[0].GetValuesJson()) != 0 || got[0].GetPrincipal() != "" {
		t.Fatalf("a redeliver told %v; want the task alone", got)
	}
}

// TestRecordedDoor_TheCallbackTellsItsSuccessesApart, and a run outside the
// engine's records answers as it did.
func TestRecordedDoor_TheCallbackTellsItsSuccessesApart(t *testing.T) {
	bearer := "Bearer " + awaitToken
	post := func(d *recordedDoor) http.Header {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, awaitPath(recordedRun, "gate"), strings.NewReader(approved))
		req.Header.Set("Authorization", bearer)
		w := httptest.NewRecorder()
		api.NewAwaitHandler(d.svc).ServeHTTP(w, req)
		if w.Code != http.StatusAccepted {
			t.Fatalf("status %d: %s", w.Code, w.Body)
		}
		return w.Header()
	}
	d := newRecordedDoor(t)
	if h := post(d); h.Get("Ork-Delivery") != "told" || h.Get("Retry-After") != "" {
		t.Fatalf("told: %v", h)
	}
	d.seed(-time.Second)
	d.storedAt(time.Now().Add(-time.Minute))
	if h := post(d); h.Get("Ork-Delivery") != "deadline-passed" || h.Get("Ork-Deadline-Passed") != "true" {
		t.Fatalf("a repeat after the deadline: %v", h)
	}
	d = newRecordedDoor(t)
	d.starter.signalErr = errors.New("frontend down")
	if h := post(d); h.Get("Ork-Delivery") != "pending" || h.Get("Retry-After") != "10" {
		t.Fatalf("pending: %v", h)
	}
	d = newRecordedDoor(t)
	d.seedRun(t, recordedRun, "default", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING})
	if h := post(d); h.Get("Ork-Delivery") != "" || d.onRecord() != nil {
		t.Fatalf("a run outside the engine's records: %v", h)
	}
}

// TestRecordedDoor_CapsRefusedRequests: a caller whose requests make no record
// is held to ten a minute, read before anything of the run is; answers that
// make one, another caller and another minute are not held.
func TestRecordedDoor_CapsRefusedRequests(t *testing.T) {
	d := newRecordedDoor(t)
	_, err := d.approve(approved, "alice")
	must.OK(t, err)
	other := `{"approved":false,"note":"n"}`
	for range 10 {
		_, err := d.approve(other, "mallory")
		d.refused(err, connect.CodeFailedPrecondition, "AWAIT_ALREADY_RESOLVED")
	}
	reads := d.records.reads.Load()
	for range 5 {
		_, err := d.approve(other, "mallory")
		d.refused(err, connect.CodeResourceExhausted, "too many")
	}
	_, err = d.resolve(&orkstackv1.ResolveAwaitRequest{RunId: recordedRun, TaskId: "gate", Redeliver: true}, "mallory")
	d.refused(err, connect.CodeResourceExhausted, "too many")
	if now := d.records.reads.Load(); now != reads {
		t.Fatalf("a capped request read %d records", now-reads)
	}
	_, err = d.approve(other, "bob")
	d.refused(err, connect.CodeFailedPrecondition, "AWAIT_ALREADY_RESOLVED")
	d.clock.advance(time.Minute)
	_, err = d.approve(other, "mallory")
	d.refused(err, connect.CodeFailedPrecondition, "AWAIT_ALREADY_RESOLVED")
	// What makes a record, or could not be asked, spends nothing.
	d = newRecordedDoor(t)
	d.records.writeErr = &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("bucket unreachable")}
	for range 15 {
		_, err := d.approve(approved, "alice")
		d.refused(err, connect.CodeUnavailable, "try again")
	}
	d.records.writeErr = nil
	if _, err := d.approve(approved, "alice"); err != nil {
		t.Fatalf("an answer after an outage: %v", err)
	}
}

// TestRecordedDoor_ARepeatAfterTheRunMovedOn: once the task stopped waiting, a
// repeat is success by the record, and never by the run's state alone.
func TestRecordedDoor_ARepeatAfterTheRunMovedOn(t *testing.T) {
	applied := func(d *recordedDoor, values string) {
		run := d.seed(time.Hour)
		tr := taskRunFor(run, "gate")
		tr.Status, tr.ValuesRef = orkstackv1.Status_STATUS_SUCCEEDED, &orkstackv1.ArtifactRef{Sha256: values}
		d.reader.setRun(run)
	}
	d := newRecordedDoor(t)
	first, err := d.approve(approved, "alice")
	must.OK(t, err)
	told := len(d.starter.delivered())
	applied(d, first.GetValuesSha256())
	again, err := d.approve(approved, "alice")
	if err != nil || !again.GetRepeat() || len(d.starter.delivered()) != told {
		t.Fatalf("%v, %v", again, err)
	}
	_, err = d.approve(`{"approved":false,"note":"n"}`, "alice")
	d.refused(err, connect.CodeFailedPrecondition, "accepts no resolution")
	// A state that says so, with nothing on record.
	d = newRecordedDoor(t)
	applied(d, first.GetValuesSha256())
	_, err = d.approve(approved, "alice")
	d.refused(err, connect.CodeFailedPrecondition, "accepts no resolution")
}
