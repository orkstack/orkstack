package api

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A resolution of a run whose results the engine records
// (docs/TASK_IDENTITY.md, "What the engine records for an await"): the door
// writes the resolution's record, create-only, and the first record stands.
// The Signal only says that a record is there.

// The doors' reasons for such a run.
const (
	reasonAwaitDeadlinePassed     = "AWAIT_DEADLINE_PASSED"
	reasonAwaitAlreadyResolved    = "AWAIT_ALREADY_RESOLVED"
	reasonAwaitReopened           = "AWAIT_REOPENED"
	reasonAwaitRunEnded           = "AWAIT_RUN_ENDED"
	reasonResolutionRecordInvalid = "RESOLUTION_RECORD_INVALID"
)

const (
	// awaitTellPace is how long one telling of a run, of one record, from one
	// replica, stands for the next.
	awaitTellPace = 10 * time.Second
	// awaitTellTries is how often a Signal is sent before the run is said not
	// to have been told, the second and later after a wait that grows from
	// awaitTellWait fourfold.
	awaitTellTries = 3
	awaitTellWait  = 100 * time.Millisecond
	// awaitSettleBound is how long each question of a record that exists may
	// take (what it is when read back; whether the run was resumed, and what
	// the caller may learn), and awaitTellBound how long the run is then told
	// of it, whatever becomes of the request. Together they leave an answer
	// that stands time to reach a caller that waits half a minute.
	awaitSettleBound = 5 * time.Second
	awaitTellBound   = 10 * time.Second
	// maxAwaitTellings bounds the tellings a replica remembers.
	maxAwaitTellings = 10_000
)

// AwaitRecords is what the doors read and write for such a run.
type AwaitRecords interface {
	ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error)
	ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error)
	WriteResolutionDetail(ctx context.Context, d *orkstackv1.ResolutionDetail) (string, error)
	WriteRecordedValues(ctx context.Context, rec *orkstackv1.RecordedValues) (string, error)
}

// AwaitChecks is what the doors ask of a run's resumes and of what an await
// follows; identity.FollowsCheck is the one implementation.
type AwaitChecks interface {
	Establish(ctx context.Context, adm artifacts.Admission, scopes identity.Scopes) (artifacts.ResumeRecord, error)
	DecidingScope(ctx context.Context, adm artifacts.Admission, taskID string, scopes identity.Scopes) (uint64, error)
	CheckFollows(ctx context.Context, adm artifacts.Admission, admissionSHA256, taskID string, rootScope uint64) ([]identity.FollowedRecord, error)
}

var _ AwaitChecks = (*identity.FollowsCheck)(nil)

// awaitTellings remembers, in this replica's memory, when it last had the
// cluster take a Signal for an await's record.
type awaitTellings struct {
	mu sync.Mutex
	at map[string]time.Time
}

// told is when the run was last told of key inside the pace, as now has it.
func (t *awaitTellings) told(key string, now time.Time) (time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	at, ok := t.at[key]
	if !ok || now.Sub(at) >= awaitTellPace {
		return time.Time{}, false
	}
	return at, true
}

func (t *awaitTellings) note(key string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.at == nil {
		t.at = map[string]time.Time{}
	}
	if len(t.at) >= maxAwaitTellings {
		for k, at := range t.at {
			if now.Sub(at) >= awaitTellPace {
				delete(t.at, k)
			}
		}
		if len(t.at) >= maxAwaitTellings {
			return // unpaced: the run's own bound on its reads still holds
		}
	}
	t.at[key] = now
}

// awaitDoor is one request at the doors' nine steps.
type awaitDoor struct {
	s         *Service
	r         awaitResolution
	adm       artifacts.Admission
	sha       string
	scopes    identity.Scopes
	scope     uint64 // the row's own
	deadline  time.Time
	valuesSHA string
	key       string
	want      artifacts.RecordWant
	resp      *orkstackv1.ResolveAwaitResponse
	clock     *artifacts.AnswerClock
	// instant is the store's own time as step 3's read was answered, taken at
	// at by this replica's; dated says the store gave one.
	instant, at time.Time
	dated       bool
	// made says this request's own create made the record.
	made bool
}

func awaitUnavailable(format string, args ...any) error {
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf(format, args...))
}

func awaitRefused(reason, format string, args ...any) error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(reason+": "+format, args...))
}

// resolveRecorded is the nine steps, from the task's awaiting row on, within
// the caller's budget.
func (s *Service) resolveRecorded(ctx context.Context, r awaitResolution, row *orkstackv1.TaskRun, valuesSHA string, resp *orkstackv1.ResolveAwaitResponse) (*orkstackv1.ResolveAwaitResponse, error) {
	if s.awaitRecords == nil || s.awaitChecks == nil {
		return nil, awaitUnavailable("this engine records no resolution; another replica may")
	}
	if r.binding.Starter == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("no run starter configured"))
	}
	// A request that makes no record spends the caller's budget; one that does,
	// or that could not be asked, gives it back.
	spend, ok := s.awaitLimits.TakePrincipal(r.limitKey())
	if !ok {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("too many requests that resolved nothing; ask again in a minute"))
	}
	resp, recorded, err := s.recordedDoor(ctx, r, row, valuesSHA, resp)
	if recorded || connect.CodeOf(err) == connect.CodeUnavailable {
		spend.Refund()
	}
	return resp, err
}

// repeatsRecord says r repeats the resolution on record of a task that stopped
// waiting, for a caller who may learn it: by the record, never by what the
// run's state says of itself.
func (s *Service) repeatsRecord(ctx context.Context, r awaitResolution, row *orkstackv1.TaskRun, valuesSHA string) (bool, error) {
	if s.awaitRecords == nil || s.awaitChecks == nil {
		return false, awaitUnavailable("this engine reads no resolution; another replica may")
	}
	spend, ok := s.awaitLimits.TakePrincipal(r.limitKey())
	if !ok {
		return false, connect.NewError(connect.CodeResourceExhausted, errors.New("too many requests that resolved nothing; ask again in a minute"))
	}
	ctx, clock := artifacts.WithAnswerClock(ctx)
	d := &awaitDoor{s: s, r: r, valuesSHA: valuesSHA, clock: clock}
	found, err := d.lookup(ctx, row)
	if err != nil {
		if connect.CodeOf(err) == connect.CodeUnavailable {
			spend.Refund()
		}
		return false, err
	}
	if found == nil || !d.sameAs(found) {
		return false, nil
	}
	return d.mayLearn(ctx, found.Detail())
}

// lookup is steps 1 to 3 for a request that decides nothing more.
func (d *awaitDoor) lookup(ctx context.Context, row *orkstackv1.TaskRun) (*artifacts.RecordedFound, error) {
	if err := d.admit(ctx, row); err != nil {
		return nil, err
	}
	if err := d.establish(ctx); err != nil {
		return nil, err
	}
	read, err := d.read(ctx)
	if err != nil || read.Answer != artifacts.RecordFound {
		return nil, err
	}
	return read.Value, nil
}

// sameAs says the request decides what the record does.
func (d *awaitDoor) sameAs(f *artifacts.RecordedFound) bool {
	rejected, values, _ := f.Standing()
	if rejected != d.r.reject {
		return false
	}
	if rejected {
		return f.Detail().GetReason() == d.r.reason
	}
	return values == d.valuesSHA
}

// limitKey names the caller a door's budget is: the principal, or the await
// itself for a caller with no name.
func (r awaitResolution) limitKey() string {
	if r.principal != "" {
		return r.via.String() + "\x00" + r.principal
	}
	return r.via.String() + "\x00\x00" + r.run.GetId() + "\x00" + r.taskID
}

// recordedDoor is the nine steps, and whether they made a record.
func (s *Service) recordedDoor(ctx context.Context, r awaitResolution, row *orkstackv1.TaskRun, valuesSHA string, resp *orkstackv1.ResolveAwaitResponse) (*orkstackv1.ResolveAwaitResponse, bool, error) {
	ctx, clock := artifacts.WithAnswerClock(ctx)
	d := &awaitDoor{s: s, r: r, valuesSHA: valuesSHA, resp: resp, clock: clock}
	// 1. The run's admission, and the row's deadline.
	if err := d.admit(ctx, row); err != nil {
		return nil, false, err
	}
	// 2. The scope, settled by a record before the row's deadline is believed.
	if err := d.establish(ctx); err != nil {
		return nil, false, err
	}
	// 3. The await's key, before the clock and before anything is stored.
	read, err := d.read(ctx)
	if err != nil {
		return nil, false, err
	}
	d.measure()
	if read.Answer == artifacts.RecordFound {
		resp, err := d.standing(ctx, read.Value)
		return resp, false, err
	}
	if r.redeliver {
		return nil, false, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("nothing is on record for await %q of run %q", r.taskID, r.run.GetId()))
	}
	resp, err = d.record(ctx)
	return resp, d.made, err
}

func (d *awaitDoor) admit(ctx context.Context, row *orkstackv1.TaskRun) error {
	at := row.GetAwait().GetDeadline()
	if at == nil || at.AsTime().Unix() <= 0 {
		return awaitUnavailable("the run's state does not say yet when await %q ends; try again", d.r.taskID)
	}
	d.deadline = at.AsTime()
	d.sha = d.r.start.Plan.Identity.AdmissionSHA256
	// What the run's state says of its resumes; establish believes none of it.
	d.scope = uint64(max(row.GetResultEpoch(), 0))
	d.scopes = identity.Scopes{Resume: stateNewestScope(d.r.run), Root: d.r.start.Plan.Identity.RootScope}
	ns, runID := artifacts.ResolveNamespace(d.r.run.GetNamespace()), d.r.run.GetId()
	read, err := d.s.awaitRecords.ReadAdmission(ctx, ns, runID, d.sha)
	switch {
	case err != nil:
		return awaitUnavailable("run %q's admission could not be read; try again", runID)
	case read.Answer != artifacts.RecordFound || read.Value.RunID != runID || read.Value.Namespace != ns:
		return awaitUnavailable("run %q's admission cannot be used (%s); its resolution cannot be recorded", runID, read.Answer)
	case read.Value.ValueRecords == 0:
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("run %q's admission pins no record format, so no resolution of it can be recorded", runID))
	case !artifacts.WritesRecordFormat(read.Value.ValueRecords):
		return awaitUnavailable("run %q expects a record format this replica does not write; a newer replica answers it", runID)
	}
	d.adm = read.Value
	return nil
}

// stateNewestScope is the newest resume run's state says ran one of its tasks
// again: every resume runs at least one. 0 for none, and in a run below a root.
func stateNewestScope(run *orkstackv1.Run) uint64 {
	newest := int64(0)
	for _, row := range run.GetTaskRuns() {
		newest = max(newest, row.GetResultEpoch())
		for _, item := range row.GetItems() {
			newest = max(newest, item.GetResultEpoch())
		}
	}
	return uint64(newest)
}

// establish settles the scope by the run's resume records: the newest resume
// the run's state names must be the newest there is, and the await's row must be
// of the resume that decides for it, which is where its run reads its answer.
// A run whose state does not say so answers unavailable until it does.
func (d *awaitDoor) establish(ctx context.Context) error {
	scope, err := d.s.awaitChecks.DecidingScope(ctx, d.adm, d.r.taskID, d.scopes)
	if err == nil && scope != d.scope {
		err = &identity.MintError{Reason: identity.ReasonNotAdmitted}
	}
	if err != nil {
		var m *identity.MintError
		if errors.As(err, &m) && !m.Unavailable {
			return awaitUnavailable("run %q was resumed, and its state does not say under which resume yet; try again", d.r.run.GetId())
		}
		return awaitUnavailable("run %q's resumes could not be read; try again", d.r.run.GetId())
	}
	d.want = artifacts.RecordWant{Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, Namespace: d.adm.Namespace,
		RunID: d.adm.RunID, TaskID: d.r.taskID, Scope: scope, AdmissionSHA256: d.sha}
	key, err := artifacts.RecordKey(artifacts.RecordResolution, d.adm.Namespace, d.adm.RunID, d.r.taskID, scope)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("the key of await %q's record: %w", d.r.taskID, err))
	}
	d.key = key
	return nil
}

// read is what the run will read: the record, its detail and small values.
// What it answers other than found and none, it tells the caller.
func (d *awaitDoor) read(ctx context.Context) (artifacts.RecordRead[*artifacts.RecordedFound], error) {
	read, err := d.s.awaitRecords.ReadRecordedValues(ctx, artifacts.SlotDoor, d.s.awaitSpacings, d.key, d.want)
	if err != nil {
		d.s.logger.Warn("an await's record could not be read", "namespace", d.adm.Namespace, "run", d.adm.RunID, "task", d.r.taskID, "error", err)
		return read, awaitUnavailable("the record of await %q could not be read; try again", d.r.taskID)
	}
	switch read.Answer {
	case artifacts.RecordUnsupported:
		return read, awaitUnavailable("this answer is recorded in a newer format than this replica reads; repeat it, and a newer replica answers it")
	case artifacts.RecordInvalid:
		d.s.logger.Error("an await's record cannot be used", "namespace", d.adm.Namespace, "run", d.adm.RunID, "task", d.r.taskID,
			"object", read.Object, "reason", read.Reason)
		// The run fails the task when it reads it: told now, not at its
		// deadline. Whether it could be told changes nothing of the answer.
		telling, cancel := context.WithTimeout(context.WithoutCancel(ctx), awaitTellBound)
		defer cancel()
		d.deliver(telling, d.bare())
		return read, awaitRefused(reasonResolutionRecordInvalid,
			"the record on file for await %q cannot be used; the run fails the task when it reads it", d.r.taskID)
	}
	return read, nil
}

func (d *awaitDoor) bare() *orkstackv1.AwaitSignal {
	return &orkstackv1.AwaitSignal{TaskId: d.r.taskID}
}

// standing answers a request that met a record: step 3's table, and step 8's
// stored time, which outranks it.
func (d *awaitDoor) standing(ctx context.Context, f *artifacts.RecordedFound) (*orkstackv1.ResolveAwaitResponse, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*awaitSettleBound+awaitTellBound)
	defer cancel()
	rejected, values, _ := f.Standing()
	detail := f.Detail()
	if f.StoredAt().IsZero() {
		return nil, awaitUnavailable("the store does not say when the record of await %q was stored; try again", d.r.taskID)
	}
	if !artifacts.RecordedInTime(f.StoredAt(), d.deadline, time.Time{}) {
		if may, err := d.mayLearn(ctx, detail); err != nil || !may {
			return nil, awaitRefused(reasonAwaitDeadlinePassed, "the answer on record for await %q came after its deadline and is not taken", d.r.taskID)
		}
		return nil, d.late(f.StoredAt())
	}
	asking, asked := context.WithTimeout(ctx, awaitSettleBound)
	defer asked()
	if err := d.reopened(asking); err != nil {
		return nil, err
	}
	same := d.r.redeliver
	if !same && d.sameAs(f) {
		learning, learned := context.WithTimeout(ctx, awaitSettleBound)
		defer learned()
		may, err := d.mayLearn(learning, detail)
		if err != nil {
			// Whatever the caller may learn, the run is told of what stands.
			d.deliver(ctx, d.bare())
			return nil, err
		}
		same = may
	}
	toldAt, told, ended := d.deliver(ctx, d.bare())
	if same {
		d.resp.Repeat = !d.r.redeliver
		// A redeliver decides nothing, so it tells what was decided only to a
		// reader of the run.
		if d.r.redeliver {
			if may, err := d.mayRead(ctx); err != nil || !may {
				return d.answer(toldAt, told, ended, d.passed())
			}
		}
		d.resp.Reject = rejected
		d.resp.ValuesSha256 = values
		return d.answer(toldAt, told, ended, d.passed())
	}
	who := ""
	if may, err := d.mayRead(ctx); err == nil && may {
		who = fmt.Sprintf(" by %s at %s", resolverOf(detail), f.StoredAt().Format(time.RFC3339))
	}
	return nil, awaitRefused(reasonAwaitAlreadyResolved, "await %q was already resolved%s", d.r.taskID, who)
}

func resolverOf(d *orkstackv1.ResolutionDetail) string {
	switch {
	case d.GetPrincipal() != "" && d.GetPrincipalVerified():
		return d.GetPrincipal()
	case d.GetPrincipal() != "":
		return d.GetPrincipal() + " (asserted)"
	case d.GetVia() == orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK:
		return "its callback"
	}
	return "a caller with no name"
}

// mayLearn says the caller may be told that its answer is the one on record.
func (d *awaitDoor) mayLearn(ctx context.Context, detail *orkstackv1.ResolutionDetail) (bool, error) {
	if d.r.who.Issuer != detail.GetPrincipalIssuer() && detail.GetPrincipal() != "" {
		return d.mayRead(ctx)
	}
	return d.s.mayLearnResolution(ctx, d.r, detail.GetVia(), detail.GetPrincipal(), detail.GetPrincipalVerified())
}

// mayLearnResolution says r's caller may be told that it repeats the
// resolution given by via and by principal: that principal, the callback that
// gave it, or a reader of the run. Anyone else could test a guess of it.
func (s *Service) mayLearnResolution(ctx context.Context, r awaitResolution, via orkstackv1.AwaitResolvedVia, principal string, verified bool) (bool, error) {
	if via == orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK && r.via == via {
		return true, nil
	}
	if r.principal != "" && r.via == via && r.principal == principal && r.who.Verified == verified {
		return true, nil
	}
	return s.mayReadAwaited(ctx, r)
}

func (d *awaitDoor) mayRead(ctx context.Context) (bool, error) { return d.s.mayReadAwaited(ctx, d.r) }

func (s *Service) mayReadAwaited(ctx context.Context, r awaitResolution) (bool, error) {
	if r.via == orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK && s.access.enabled() {
		return false, nil // a callback names nobody
	}
	may, err := s.mayReadRun(ctx, r.run)
	if err != nil {
		return false, awaitUnavailable("what the caller may read could not be decided; try again")
	}
	return may, nil
}

// record is steps 4 to 9 for an answer that makes a new record.
func (d *awaitDoor) record(ctx context.Context) (*orkstackv1.ResolveAwaitResponse, error) {
	// 4. The clock: the store's own, as it answered step 3's read.
	now, err := d.now()
	if err != nil {
		return nil, err
	}
	if !now.Before(d.deadline) {
		return nil, awaitRefused(reasonAwaitDeadlinePassed, "await %q's deadline passed at %s; no answer was recorded by this request",
			d.r.taskID, d.deadline.Format(time.RFC3339))
	}
	// 5. What the await follows is on record.
	rows, err := d.s.awaitChecks.CheckFollows(ctx, d.adm, d.sha, d.r.taskID, d.rootScope())
	if err != nil {
		return nil, d.notFollowed(ctx, err)
	}
	rec, err := d.compose(now, rows)
	if err != nil {
		return nil, err
	}
	// 6. The values, then the detail.
	if !d.r.reject {
		ref, err := d.s.storeAwaitValues(ctx, d.r, d.valuesSHA)
		if err != nil {
			return nil, err
		}
		rec.Values = ref
		d.resp.ValuesSha256 = d.valuesSHA
	}
	detail := &orkstackv1.ResolutionDetail{SchemaVersion: uint32(d.adm.ValueRecords), Namespace: d.adm.Namespace, RunId: d.adm.RunID,
		TaskId: d.r.taskID, ResumeScope: d.want.Scope, Via: d.r.via, Reason: d.r.reason}
	detail.Principal, detail.PrincipalVerified = d.r.principal, d.r.who.Verified
	detail.AuthMethod, detail.PrincipalIssuer = d.r.who.Method, d.r.who.Issuer
	rec.DetailSha256, err = d.s.awaitRecords.WriteResolutionDetail(ctx, detail)
	if err != nil {
		d.s.logger.Warn("a resolution's detail could not be written", "namespace", d.adm.Namespace, "run", d.adm.RunID, "task", d.r.taskID, "error", err)
		return nil, awaitUnavailable("the resolution of await %q could not be recorded; try again", d.r.taskID)
	}
	// 7. The record: the first stands.
	_, err = d.s.awaitRecords.WriteRecordedValues(ctx, rec)
	switch {
	case errors.Is(err, artifacts.ErrRecordExists):
		read, err := d.read(ctx)
		if err != nil {
			return nil, err
		}
		if read.Answer != artifacts.RecordFound {
			return nil, awaitUnavailable("await %q was resolved by another request that cannot be read yet; try again", d.r.taskID)
		}
		return d.standing(ctx, read.Value)
	case err != nil:
		d.s.logger.Warn("a resolution could not be recorded", "namespace", d.adm.Namespace, "run", d.adm.RunID, "task", d.r.taskID, "error", err)
		if artifacts.OutcomeUnknown(err) {
			return nil, awaitUnavailable("the answer may be on record; repeat it to learn")
		}
		return nil, awaitUnavailable("the resolution of await %q could not be recorded; try again", d.r.taskID)
	}
	d.made = true
	return d.recorded(ctx, rec)
}

// recorded is steps 8 and 9 for the record this request made.
func (d *awaitDoor) recorded(ctx context.Context, rec *orkstackv1.RecordedValues) (*orkstackv1.ResolveAwaitResponse, error) {
	// The create's own answer is dated no earlier than the record was stored.
	created, dated := d.clock.Now(d.s.clockSkew)
	// The record exists: the run is told of it though the caller hang up.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*awaitSettleBound+awaitTellBound)
	defer cancel()
	reading, done := context.WithTimeout(ctx, awaitSettleBound)
	defer done()
	passed := false
	read, err := d.s.awaitRecords.ReadRecordedValues(reading, artifacts.SlotDoor, d.s.awaitSpacings, d.key, d.want)
	switch {
	case err == nil && read.Answer == artifacts.RecordInvalid:
		d.s.logger.Error("a resolution just recorded cannot be used", "namespace", d.adm.Namespace, "run", d.adm.RunID, "task", d.r.taskID,
			"object", read.Object, "reason", read.Reason)
		d.deliver(ctx, d.bare())
		return nil, awaitRefused(reasonResolutionRecordInvalid,
			"the record on file for await %q cannot be used; the run fails the task when it reads it", d.r.taskID)
	case err == nil && read.Answer == artifacts.RecordFound && !read.Value.StoredAt().IsZero():
		if !artifacts.RecordedInTime(read.Value.StoredAt(), d.deadline, time.Time{}) {
			return nil, d.late(read.Value.StoredAt())
		}
		passed = d.passed()
	case err == nil && read.Answer == artifacts.RecordUnsupported:
		return nil, awaitUnavailable("this answer is recorded in a newer format than this replica reads; repeat it, and a newer replica answers it")
	default:
		// The record is there, and this request cannot say when it was
		// stored but by its create's own answer.
		d.s.logger.Warn("a resolution was recorded and could not be read back", "namespace", d.adm.Namespace, "run", d.adm.RunID,
			"task", d.r.taskID, "answer", read.Answer, "dated", dated, "error", err)
		if !dated || created.After(d.deadline) {
			d.resp.DeadlinePassed = true
			return d.resp, nil
		}
		passed = d.passed()
	}
	asking, asked := context.WithTimeout(ctx, awaitSettleBound)
	defer asked()
	if err := d.reopened(asking); err != nil {
		return nil, err
	}
	signal := &orkstackv1.AwaitSignal{TaskId: d.r.taskID, Reject: d.r.reject, Reason: d.r.reason, Values: rec.GetValues(), Via: d.r.via, Principal: d.r.principal}
	if !d.r.reject && len(d.r.values) <= awaitInlineValuesLimit {
		signal.ValuesJson = d.r.values
	}
	toldAt, told, ended := d.deliver(ctx, signal)
	return d.answer(toldAt, told, ended, passed)
}

// compose is the record, but for its values and its detail.
func (d *awaitDoor) compose(now time.Time, rows []identity.FollowedRecord) (*orkstackv1.RecordedValues, error) {
	nonce, err := hex.DecodeString(d.adm.Nonce)
	if err != nil {
		return nil, awaitUnavailable("run %q's admission cannot be used; its resolution cannot be recorded", d.adm.RunID)
	}
	input, err := identity.StartInput(d.r.start.Input)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("run %q's input: %w", d.adm.RunID, err))
	}
	rec := &orkstackv1.RecordedValues{SchemaVersion: int32(d.adm.ValueRecords), Kind: d.want.Kind, Namespace: d.adm.Namespace, RunId: d.adm.RunID,
		TaskId: d.r.taskID, ResumeScope: d.want.Scope, AdmissionSha256: d.sha, DefinitionDigest: "sha256:" + d.adm.DefinitionDigest,
		Rejected: d.r.reject, InputBinding: identity.InputBinding(nonce, input),
		Recorded: &orkstackv1.CommitRecording{AdmissionSha256: d.sha, EngineBuild: d.s.engineBuild, Time: timestamppb.New(now)}}
	for _, row := range rows {
		rec.Follows = append(rec.Follows, &orkstackv1.RecordedSource{TaskId: row.Ref.TaskID, RecordSha256: row.RecordSHA256, ResultSha256: row.ResultSHA256})
	}
	return rec, nil
}

func (d *awaitDoor) rootScope() uint64 {
	if d.adm.RootRunID != "" {
		return d.scopes.Root
	}
	return d.scopes.Resume
}

// measure takes the store's time from step 3's answer, before another
// request of the door's is answered, and what it says of this replica's clock.
func (d *awaitDoor) measure() {
	d.at = d.s.now()
	if d.instant, d.dated = d.clock.Now(d.s.clockSkew); d.dated {
		d.s.storeOffset.measured(d.instant.Sub(d.at), d.at)
	}
}

// now is the store's time, from the answer measure took, or this replica's
// own while the store's answers have lately agreed with it.
func (d *awaitDoor) now() (time.Time, error) {
	if d.dated {
		return d.instant, nil
	}
	if d.s.storeOffset.agrees(d.s.clockSkew, d.at) {
		return d.at.UTC(), nil
	}
	return time.Time{}, awaitUnavailable("the store gave no time this request can be judged by; try again")
}

// passed says the deadline has passed by now, counted on from the instant the
// door judged by; a clock that cannot say counts as passed.
func (d *awaitDoor) passed() bool {
	from, err := d.now()
	return err != nil || !from.Add(d.s.now().Sub(d.at)).Before(d.deadline)
}

func (d *awaitDoor) late(storedAt time.Time) error {
	return awaitRefused(reasonAwaitDeadlinePassed, "this answer was stored at %s, after the deadline %s, and is not taken",
		storedAt.Format(time.RFC3339), d.deadline.Format(time.RFC3339))
}

// reopened asks step 2's question again: a resume accepted since opened the
// await again, under a key of its own.
func (d *awaitDoor) reopened(ctx context.Context) error {
	_, err := d.s.awaitChecks.Establish(ctx, d.adm, d.scopes)
	var m *identity.MintError
	if errors.As(err, &m) && !m.Unavailable {
		return awaitRefused(reasonAwaitReopened, "run %q was resumed while this was being recorded; resolve it again", d.adm.RunID)
	}
	// A question that could not be asked changes nothing: the record stands,
	// and the run reads the one of the scope it runs in.
	if err != nil {
		d.s.logger.Warn("whether a run was resumed while its resolution was recorded could not be asked", "namespace", d.adm.Namespace,
			"run", d.adm.RunID, "task", d.r.taskID, "error", err)
	}
	return nil
}

func (d *awaitDoor) notFollowed(ctx context.Context, err error) error {
	var m *identity.MintError
	if errors.As(err, &m) && m.Reason == identity.ReasonRequiredResultNotRecorded {
		what := ""
		if may, readErr := d.mayRead(ctx); readErr == nil && may {
			what = ": " + m.Message
		}
		return awaitRefused(identity.ReasonRequiredResultNotRecorded, "what await %q follows is not on record%s; nothing was recorded", d.r.taskID, what)
	}
	d.s.logger.Warn("what an await follows could not be read", "namespace", d.adm.Namespace, "run", d.adm.RunID, "task", d.r.taskID, "error", err)
	return awaitUnavailable("what await %q follows could not be read; try again", d.r.taskID)
}

// deliver tells the run of the record, at most once a pace from this
// replica: the time of the telling this answer stands on, whether there is
// one, and whether the cluster says the run has ended.
func (d *awaitDoor) deliver(ctx context.Context, signal *orkstackv1.AwaitSignal) (toldAt time.Time, told, ended bool) {
	if at, ok := d.s.awaitTold.told(d.key, d.s.now()); ok {
		return at, true, false
	}
	told, err := d.tell(ctx, signal)
	if told {
		at := d.s.now().UTC()
		d.s.awaitTold.note(d.key, at)
		return at, true, false
	}
	return time.Time{}, false, errors.Is(err, engine.ErrRunEnded)
}

func (d *awaitDoor) tell(ctx context.Context, signal *orkstackv1.AwaitSignal) (bool, error) {
	var err error
	wait := awaitTellWait
	for try := range awaitTellTries {
		if try > 0 {
			select {
			case <-ctx.Done():
				return false, err
			case <-time.After(wait):
			}
			wait *= 4
		}
		if err = d.r.binding.Starter.SignalAwait(ctx, d.r.run.GetId(), signal); err == nil {
			return true, nil
		}
		if errors.Is(err, engine.ErrRunEnded) || ctx.Err() != nil {
			break
		}
	}
	d.s.logger.Warn("a run could not be told of a resolution on record", "namespace", d.adm.Namespace, "run", d.adm.RunID, "task", d.r.taskID, "error", err)
	return false, err
}

// answer is what the caller is told once a record stands.
func (d *awaitDoor) answer(toldAt time.Time, told, ended, passed bool) (*orkstackv1.ResolveAwaitResponse, error) {
	switch {
	case ended:
		return nil, awaitRefused(reasonAwaitRunEnded, "the run ended while this answer was being recorded, and did not take it; "+
			"if the run is resumed, an answer recorded before it ended is taken when what it approved has not changed")
	case told:
		d.resp.ToldAt = timestamppb.New(toldAt)
		d.resp.DeadlinePassed = passed
	case passed:
		d.resp.DeadlinePassed = true
	default:
		d.resp.DeliveryPending = true
	}
	return d.resp, nil
}

// storeOffsetReading is this replica's clock against the store's, as the
// store's answers last showed it.
type storeOffsetReading struct {
	mu     sync.Mutex
	offset time.Duration
	at     time.Time
}

// storeOffsetFresh is how long a measured offset stands for a store whose
// answers carry no time of their own.
const storeOffsetFresh = time.Hour

func (o *storeOffsetReading) measured(offset time.Duration, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.offset, o.at = offset, at
}

func (o *storeOffsetReading) agrees(skew time.Duration, now time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.at.IsZero() && now.Sub(o.at) < storeOffsetFresh && o.offset.Abs() <= skew
}

// setAwaitDelivery says, at the callback route, which of its successes a
// 202 is: a caller repeats on "pending" alone.
func setAwaitDelivery(h http.Header, resp *orkstackv1.ResolveAwaitResponse) {
	switch {
	case resp.GetDeadlinePassed():
		h.Set("Ork-Delivery", "deadline-passed")
		h.Set("Ork-Deadline-Passed", "true")
	case resp.GetDeliveryPending():
		h.Set("Ork-Delivery", "pending")
		h.Set("Retry-After", "10")
	default:
		h.Set("Ork-Delivery", "told")
	}
}
