package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// What a resume of an admitted run reads of it (design §28): the results of
// such a run are what the engine recorded, so what its state says of them is
// only held against the records.

// ResumeRecordStore is what is read: artifacts.RecordStore.
type ResumeRecordStore interface {
	ReadAttestedCommit(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, want artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error)
	ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error)
}

// ResumeRecordsAsk is an ended admitted run as its last state says.
type ResumeRecordsAsk struct {
	Definition      *orkstackv1.Orkflow
	Base            *orkstackv1.Run
	AdmissionSHA256 string
	// Resumes are the run's resumes up to the one this ending is of, newest
	// first (artifacts.RecordStore.ResumeChain): a resume whose plan was
	// recorded and never carried out is not among them. None is a run never
	// resumed.
	Resumes []artifacts.ResumeRecord
	// End is when the run ended: the earlier of its close and its terminal
	// event. An answer stored after it settles nothing (§28.12 F9).
	End      time.Time
	Spacings *artifacts.RecordSpacings
	// ReadTimeout bounds one read, which asks a store that does not answer
	// until then; zero is the read-back's.
	ReadTimeout time.Duration
	// Children are the child runs of the composed tasks that ended for want
	// of their values' record (ComposedOnRecordCandidate), as the run's plan
	// names them and the cluster says they closed: what a resume may settle
	// such a task from (design §28.3).
	Children map[string]ResumeChildAsk
	// ForceCanceled: the run was force-cancelled, so a composed task it left
	// running may be settled from its child.
	ForceCanceled bool
	// Received is what the run's plan says it received from a run above.
	Received []NamedArtifactPlan
}

// ResumeChildAsk is a composed task's child run: its id and admission, from
// the run's plan, and whether the cluster says it closed SUCCEEDED, or no
// longer holds it (Unknown).
type ResumeChildAsk struct {
	RunID, AdmissionSHA256 string
	Succeeded, Unknown     bool
}

// ComposedOnRecordCandidate reports whether row, a composed task's, may be
// settled from what its child left on record (design §28.3): it ended for want
// of its values' record, or a force cancel left it running. Any other ending
// of a composed task that did not succeed runs it again.
func ComposedOnRecordCandidate(row *orkstackv1.TaskRun, forceCanceled bool) bool {
	switch row.GetStatus() {
	case orkstackv1.Status_STATUS_FAILED:
		code := row.GetError().GetCode()
		return code == ChildValuesUnstoredCode || code == ChildValuesNotRecordedCode
	case orkstackv1.Status_STATUS_RUNNING:
		return forceCanceled
	}
	return false
}

// ResumeRecord is what is on record of one task, at the scope its row's result
// was made under. The zero value is an await nobody answered in time.
type ResumeRecord struct {
	// Answer is KeptValid for a record that stands; Why says what it is
	// otherwise as a caller is told it, and Cause in the store's own terms.
	Answer KeptAnswer
	Why    string
	Cause  error
	// Newer says a KeptUnreadable record is of a format this build does not
	// read, and not one that is not this task's.
	Newer bool
	// ResultSHA256 is the digest of the result, what another record's follows
	// are compared with. A rejection has none.
	ResultSHA256 string
	// Follows is what the record of an awaited or a composed task follows.
	Follows []ResumeFollow
	// Values is where such a record keeps the task's values.
	Values *orkstackv1.ArtifactRef
	// Artifacts is what a composed task's record returns, or a container
	// task's commit holds.
	Artifacts []*orkstackv1.ArtifactRef
	// Resolution is, for an await that did not succeed, the answer on record.
	Resolution ResolutionAnswer
	// Answered is, for an await answered with values in time, its row as the
	// record gives it: what a settle of it is carried out with. For a composed
	// task kept or settled from the records, its row as they give it.
	Answered *orkstackv1.TaskRun
	// ChildOnRecord: a composed task that did not succeed is settled from the
	// records, its own or, when it has none, its child's outputs' record
	// (FromOutputs), whose follows are the task's needs as they are now.
	ChildOnRecord, FromOutputs bool
	// ChildCause is why a composed task that may have been settled from its
	// child is not: a ComposedCause.
	ChildCause ComposedCause
}

// ReadResumeRecords reads the record of every awaited and composed task that
// could be kept, of every await that had opened, and of every container task
// another task follows (identity.Follows): a fan-out and a sleep leave none.
// Its error says ask is inconsistent or ctx ended; a store that does not
// answer is KeptUnavailable.
func ReadResumeRecords(ctx context.Context, store ResumeRecordStore, ask ResumeRecordsAsk) (map[string]ResumeRecord, error) {
	if store == nil || ask.Definition == nil || ask.Base == nil || ask.AdmissionSHA256 == "" || ask.End.IsZero() {
		return nil, errors.New("engine: read a run's records: the store, the definition, the run, its admission and its end are required")
	}
	followed := map[string]bool{}
	follows, _ := identity.Follows(ask.Definition)
	for _, refs := range follows {
		for _, ref := range refs {
			followed[ref.TaskID] = true
		}
	}
	rows := make(map[string]*orkstackv1.TaskRun, len(ask.Base.GetTaskRuns()))
	for _, row := range ask.Base.GetTaskRuns() {
		rows[row.GetTaskId()] = row
	}
	out := map[string]ResumeRecord{}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(defaultReadbackTunables().Concurrency)
	for _, t := range ask.Definition.GetTasks() {
		row := rows[t.GetId()]
		kind := identity.RecordKind(t)
		switch kind {
		case artifacts.FollowContainer:
			if !followed[t.GetId()] || !keptCandidate(row) {
				continue
			}
		case artifacts.FollowAwaited:
			opened := row.GetAwait().GetDeadline() != nil && row.GetStatus() != orkstackv1.Status_STATUS_SKIPPED
			if !keptCandidate(row) && !opened {
				continue
			}
		case artifacts.FollowComposed:
			if !keptCandidate(row) && !ComposedOnRecordCandidate(row, ask.ForceCanceled) {
				continue
			}
		default:
			continue
		}
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			got, err := ask.read(gctx, store, kind, row)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			out[t.GetId()] = got
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("engine: read a run's records: %w", err)
	}
	return out, nil
}

// read reads the record of row's task. Its error is ctx's.
func (a ResumeRecordsAsk) read(ctx context.Context, store ResumeRecordStore, kind string, row *orkstackv1.TaskRun) (ResumeRecord, error) {
	timeout := a.ReadTimeout
	if timeout <= 0 {
		timeout = defaultReadbackTunables().GetTimeout
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	scope := row.GetResultEpoch()
	if err := a.scopeHeld(row.GetTaskId(), scope); err != nil {
		if kind == artifacts.FollowAwaited && !keptCandidate(row) {
			// An answer at a scope no resume gave the await is no answer: it
			// waits again (design §28.1).
			return ResumeRecord{Why: "its answer was looked for at a scope its run's resumes do not give it"}, nil
		}
		return unreadableRecord("%v", err), nil
	}
	if kind == artifacts.FollowContainer {
		read, err := store.ReadAttestedCommit(rctx, artifacts.SlotRun, a.Spacings, artifacts.AttestedWant{Namespace: a.namespace(), RunID: a.Base.GetId(),
			TaskID: row.GetTaskId(), Scope: uint64(scope), AdmissionSHA256: a.AdmissionSHA256})
		if not, ended := a.notFound(ctx, read.Answer, read.Object, read.Reason, err); ended != nil || not != nil {
			return deref(not), ended
		}
		c := read.Value.Commit
		return digested(ResumeRecord{Answer: KeptValid, Artifacts: c.GetArtifacts()}, c.GetValues().GetSha256(), c.GetArtifacts()), nil
	}
	of, want := artifacts.RecordResolution, orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION
	if kind == artifacts.FollowComposed {
		of, want = artifacts.RecordComposed, orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED
	}
	key, err := artifacts.RecordKey(of, a.namespace(), a.Base.GetId(), row.GetTaskId(), uint64(scope))
	if err != nil {
		return unreadableRecord("%v", err), nil
	}
	read, err := store.ReadRecordedValues(rctx, artifacts.SlotRun, a.Spacings, key, artifacts.RecordWant{Kind: want, Namespace: a.namespace(),
		RunID: a.Base.GetId(), TaskID: row.GetTaskId(), Scope: uint64(scope), AdmissionSHA256: a.AdmissionSHA256})
	kept := keptCandidate(row)
	if err == nil && read.Answer == artifacts.RecordNone && !kept {
		if kind == artifacts.FollowComposed {
			return a.childOnRecord(rctx, store, row)
		}
		return ResumeRecord{}, nil // an await nobody answered
	}
	if not, ended := a.notFound(ctx, read.Answer, read.Object, read.Reason, err); ended != nil || not != nil {
		return deref(not), ended
	}
	got := a.found(kind, row, kept, read.Value)
	if kind == artifacts.FollowComposed && !kept && got.Answer == KeptValid {
		got.ChildOnRecord = true
	}
	return got, nil
}

// childOnRecord is what row's composed task, which ended for want of its
// values' record and has none, may be settled from: its child's outputs'
// record, once the cluster says the child closed SUCCEEDED (design §28.3).
// The child is the one the run's plan names, never the row's.
func (a ResumeRecordsAsk) childOnRecord(ctx context.Context, store ResumeRecordStore, row *orkstackv1.TaskRun) (ResumeRecord, error) {
	c, ok := a.Children[row.GetTaskId()]
	switch {
	case !ok || c.RunID == "" || c.AdmissionSHA256 == "":
		return unreadableRecord("the run's plan names no admitted child run of it"), nil
	case c.Unknown:
		return ResumeRecord{ChildCause: ComposedChildNotSucceeded, Why: fmt.Sprintf("child run %s is not known to have succeeded", c.RunID)}, nil
	case !c.Succeeded:
		return ResumeRecord{ChildCause: ComposedChildNotSucceeded, Why: fmt.Sprintf("child run %s did not succeed", c.RunID)}, nil
	case row.GetResultEpoch() != int64(a.newestScope()):
		// Its values are recorded at the scope it ran under, which must be
		// the newest resume's: a composed task runs only as submitted, so it
		// is kept from its child only on its run's first resume.
		return ResumeRecord{ChildCause: ComposedResumedBefore, Why: "a composed task is kept from its child only on its run's first resume"}, nil
	}
	key, err := artifacts.OutputsRecordKey(a.namespace(), c.RunID)
	if err != nil {
		return unreadableRecord("%v", err), nil
	}
	read, err := store.ReadRecordedValues(ctx, artifacts.SlotRun, a.Spacings, key, artifacts.RecordWant{
		Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, Namespace: a.namespace(), RunID: c.RunID, AdmissionSHA256: c.AdmissionSHA256})
	if err == nil && (read.Answer == artifacts.RecordNone ||
		read.Answer == artifacts.RecordInvalid && read.Object == "values" && read.Reason == artifacts.RecordValuesMissing) {
		return ResumeRecord{ChildCause: ComposedChildOutputsUnavailable, Why: fmt.Sprintf("child run %s's outputs are not on record", c.RunID)}, nil
	}
	if not, ended := a.notFound(ctx, read.Answer, read.Object, read.Reason, err); ended != nil || not != nil {
		return deref(not), ended
	}
	f := read.Value
	rejected, valuesSHA256, _ := f.Standing()
	if f.ValuesRef().GetUri() == "" || valuesSHA256 == "" {
		return unreadableRecord("child run %s's outputs' record names no values", c.RunID), nil
	}
	if rejected {
		return unreadableRecord("child run %s's outputs' record is no outputs", c.RunID), nil
	}
	out := ResumeRecord{Answer: KeptValid, ChildOnRecord: true, FromOutputs: true, Values: f.ValuesRef(), Artifacts: f.Artifacts()}
	out.Answered = composedRow(row, out.Values, out.Artifacts, c.RunID, f.StoredAt())
	return digested(out, valuesSHA256, out.Artifacts), nil
}

// composedRow is row's composed task succeeded with values, the artifacts its
// child returned and the child, as records give them: whoever answers the
// run's workflow tasks writes the state, which says nothing of them.
func composedRow(row *orkstackv1.TaskRun, values *orkstackv1.ArtifactRef, returned []*orkstackv1.ArtifactRef, child string, stored time.Time) *orkstackv1.TaskRun {
	out := proto.Clone(row).(*orkstackv1.TaskRun)
	out.Status, out.Error = orkstackv1.Status_STATUS_SUCCEEDED, nil
	out.ValuesRef = &orkstackv1.ArtifactRef{Name: "values.json", Uri: values.GetUri(), Sha256: values.GetSha256(), SizeBytes: values.GetSizeBytes()}
	out.Artifacts = nil
	for _, a := range returned {
		out.Artifacts = append(out.Artifacts, proto.Clone(a).(*orkstackv1.ArtifactRef))
	}
	if out.Child == nil {
		out.Child = &orkstackv1.TaskChild{}
	}
	out.Child.RunId = child
	if out.StartedAt == nil {
		out.StartedAt = out.GetScheduledAt()
	}
	if !stored.IsZero() {
		out.FinishedAt = timestamppb.New(stored) // when what it ended with was recorded
	}
	if out.FinishedAt == nil {
		out.FinishedAt = out.GetStartedAt()
	}
	return out
}

// newestScope is the scope of the run's newest resume, 0 when it was never
// resumed.
func (a ResumeRecordsAsk) newestScope() uint64 {
	if len(a.Resumes) == 0 {
		return 0
	}
	return a.Resumes[0].Scope
}

// found is what a record that was read says of row's task.
func (a ResumeRecordsAsk) found(kind string, row *orkstackv1.TaskRun, kept bool, f *artifacts.RecordedFound) ResumeRecord {
	rejected, valuesSHA256, follows := f.Standing()
	if kept && (rejected || !sameResult(kind, row, f)) {
		return unreadableRecord("its record is not of the result the run held")
	}
	out := ResumeRecord{Answer: KeptValid, Values: f.ValuesRef()}
	if kind == artifacts.FollowAwaited {
		var deadline time.Time
		if at := row.GetAwait().GetDeadline(); at != nil {
			deadline = at.AsTime()
		}
		adopted, inTime := f.InTime(deadline, a.End)
		switch {
		case !inTime && kept:
			return ResumeRecord{Answer: KeptGone, Why: "its answer was stored after it was due or after the run ended", Cause: fmt.Errorf("stored at %s", f.StoredAt())}
		case !inTime:
			return ResumeRecord{Why: "an answer given after it was due or after the run ended is on record and was not used"}
		}
		out.Values = adopted.Record().GetValues()
		if !kept {
			out.Resolution = ResolutionAnswer{Completed: !rejected, Rejected: rejected}
		}
		if !rejected {
			// The row a settle is carried out with is the record's, kept or not:
			// who answered, how, and with what are the record's word, never the
			// state's.
			out.Answered = answeredRow(row, f, out.Values)
		}
	} else {
		// A composed task's record has no time to be held to: it is due by no
		// deadline, only an engine writes it, while the parent is open, and the
		// row took these values from it. A check against the run's end could
		// only refuse a real result, where the store's clock is ahead.
		out.Artifacts = f.Artifacts()
		child, _, _ := f.Child()
		if child == "" {
			return unreadableRecord("its record names no child run")
		}
		out.Answered = composedRow(row, out.Values, out.Artifacts, child, f.StoredAt())
	}
	for _, r := range follows {
		out.Follows = append(out.Follows, ResumeFollow{Task: r.GetTaskId(), ResultSHA256: r.GetResultSha256()})
	}
	if rejected {
		return out
	}
	return digested(out, valuesSHA256, out.Artifacts)
}

// scopeHeld holds the resume scope the run's state says a task's result was
// made under against the run's resumes: whoever answers the run's workflow
// tasks writes the state, and at the scope a later resume replaced lies a real
// record too.
func (a ResumeRecordsAsk) scopeHeld(task string, scope int64) error {
	if decides := identity.DecidingScope(a.Resumes, task); scope < 0 || uint64(scope) != decides {
		return fmt.Errorf("its state says its result was made under scope %d, and the run's resumes say %d", scope, decides)
	}
	return nil
}

// ResumeScopesHeld holds the scope of every result of a's run a resume could
// keep against the run's resumes, and answers the units where they differ: no
// result of one is kept, whatever lies at the scope its state says. An item of
// a fan-out counts at the base, or at a resume that ran the fan-out again.
func ResumeScopesHeld(a ResumeRecordsAsk) map[ResumeUnit]error {
	out := map[ResumeUnit]error{}
	rows := make(map[string]*orkstackv1.TaskRun, len(a.Base.GetTaskRuns()))
	for _, row := range a.Base.GetTaskRuns() {
		rows[row.GetTaskId()] = row
	}
	for _, t := range a.Definition.GetTasks() {
		row, id := rows[t.GetId()], t.GetId()
		if t.GetEach() != nil {
			for _, item := range row.GetItems() {
				if err := a.itemScopeHeld(id, item.GetResultEpoch()); err != nil && keptCandidate(item) {
					out[ResumeUnit{Task: id, Item: int(item.GetItem())}] = err
				}
			}
			continue
		}
		opened := t.GetAwait() != nil && row.GetAwait().GetDeadline() != nil
		if t.GetSleep() != nil || row.GetStatus() == orkstackv1.Status_STATUS_SKIPPED || !keptCandidate(row) && !opened {
			continue
		}
		if err := a.scopeHeld(id, row.GetResultEpoch()); err != nil {
			out[nodeUnit(id)] = err
		}
	}
	return out
}

// itemScopeHeld is scopeHeld for an item of fan-out task, which a resume that
// ran the fan-out again may have kept. A resume's record names the fan-out and
// not its items, so which of those scopes is the item's newest is not held: a
// party that writes the cluster can have a resume keep an item's earlier
// result, as it can have an earlier record staged (docs/SECURITY_MODEL.md).
func (a ResumeRecordsAsk) itemScopeHeld(task string, scope int64) error {
	if scope == 0 {
		return nil
	}
	for _, r := range a.Resumes {
		if scope > 0 && r.Scope == uint64(scope) && slices.Contains(r.Tasks, task) {
			return nil
		}
	}
	return fmt.Errorf("its state says it was made under scope %d, and no resume of the run ran its fan-out again there", scope)
}

// answeredRow is row of an await that did not succeed, as its record of an
// answer given in time says it ended: the values it was answered with, and who
// answered it and how, at the time the store wrote the record.
func answeredRow(row *orkstackv1.TaskRun, f *artifacts.RecordedFound, v *orkstackv1.ArtifactRef) *orkstackv1.TaskRun {
	out := proto.Clone(row).(*orkstackv1.TaskRun)
	out.Status, out.Error = orkstackv1.Status_STATUS_SUCCEEDED, nil
	out.ValuesRef = &orkstackv1.ArtifactRef{Name: "values.json", Uri: v.GetUri(), Sha256: v.GetSha256(), SizeBytes: v.GetSizeBytes()}
	d := f.Detail()
	out.Await = &orkstackv1.TaskAwait{Deadline: row.GetAwait().GetDeadline(), Callback: row.GetAwait().GetCallback(), ResolvedVia: d.GetVia(),
		ResolvedBy: d.GetPrincipal(), ResolvedByVerified: d.GetPrincipalVerified()}
	if out.StartedAt == nil {
		out.StartedAt = out.GetScheduledAt()
	}
	out.FinishedAt = timestamppb.New(f.StoredAt())
	return out
}

// sameResult reports whether row, which the run's state says its task kept,
// holds what f records: the values, by digest, where and of what size, and, for
// a composed task, its artifacts and its child. Whoever answers the run's
// workflow tasks writes the state, so nothing of it is kept that the record
// does not say.
func sameResult(kind string, row *orkstackv1.TaskRun, f *artifacts.RecordedFound) bool {
	_, valuesSHA256, _ := f.Standing()
	v := row.GetValuesRef()
	if valuesSHA256 != v.GetSha256() {
		return false
	}
	if kind != artifacts.FollowComposed {
		return true
	}
	recorded := f.ValuesRef()
	if recorded.GetUri() != v.GetUri() || recorded.GetSizeBytes() != v.GetSizeBytes() {
		return false
	}
	if child, _, _ := f.Child(); child != row.GetChild().GetRunId() {
		return false
	}
	want := f.Artifacts()
	got := row.GetArtifacts()
	if len(want) != len(got) {
		return false
	}
	byName := make(map[string]*orkstackv1.ArtifactRef, len(want))
	for _, a := range want {
		byName[a.GetName()] = a
	}
	for _, a := range got {
		w := byName[a.GetName()]
		if w == nil || w.GetSha256() != a.GetSha256() || w.GetUri() != a.GetUri() || w.GetSizeBytes() != a.GetSizeBytes() {
			return false
		}
	}
	return true
}

// digested is r with the digest of its result, or unreadable when that cannot
// be taken.
func digested(r ResumeRecord, valuesSHA256 string, produced []*orkstackv1.ArtifactRef) ResumeRecord {
	named := make([]identity.ResultArtifact, 0, len(produced))
	for _, a := range produced {
		named = append(named, identity.ResultArtifact{Name: a.GetName(), SHA256: a.GetSha256()})
	}
	digest, err := identity.ResultSHA256(valuesSHA256, named)
	if err != nil {
		return unreadableRecord("%v", err)
	}
	r.ResultSHA256 = digest
	return r
}

// notFound is what a read that found no record says, nil for one that found
// it; or ctx's error.
func (a ResumeRecordsAsk) notFound(ctx context.Context, answer artifacts.RecordAnswer, object, reason string, err error) (*ResumeRecord, error) {
	switch {
	case ctx.Err() != nil:
		return nil, ctx.Err()
	case err != nil:
		return &ResumeRecord{Answer: KeptUnavailable, Why: "its store did not answer", Cause: err}, nil
	case answer == artifacts.RecordFound:
		return nil, nil
	case answer == artifacts.RecordNone:
		return &ResumeRecord{Answer: KeptGone, Why: "its result is not on record", Cause: errors.New("no engine recorded it there")}, nil
	case answer == artifacts.RecordInvalid && object == "values" && reason == artifacts.RecordValuesMissing:
		return &ResumeRecord{Answer: KeptGone, Why: "its recorded values are gone", Cause: errors.New("the values its record names are not in the store")}, nil
	case answer == artifacts.RecordUnsupported:
		return &ResumeRecord{Answer: KeptUnreadable, Newer: true, Why: "its record is of a newer format than this release reads", Cause: fmt.Errorf("the %s is of an unknown format", object)}, nil
	}
	r := unreadableRecord("%s: %s", object, reason)
	return &r, nil
}

func unreadableRecord(format string, args ...any) ResumeRecord {
	return ResumeRecord{Answer: KeptUnreadable, Why: "its record cannot be read as its own", Cause: fmt.Errorf(format, args...)}
}

func deref(r *ResumeRecord) ResumeRecord {
	if r == nil {
		return ResumeRecord{}
	}
	return *r
}

func (a ResumeRecordsAsk) namespace() string {
	return artifacts.ResolveNamespace(a.Definition.GetNamespace())
}

// HoldChildInputs holds what each composed task's row says it passed its child
// to what the engine recorded (design §28.3, L3): whoever answers the run's
// workflow tasks writes the row, and the archive and the artifact GC read it to
// list what the run claimed. An artifact of runID's own is held to the record
// of the task that produced it; one runID received from a run above, to what
// its plan says it received. A record whose task passed anything else, or
// anything of a producer that leaves no record, is unreadable.
//
// Each record is judged against the records as they were read, so the answer
// does not hang on the order they are visited in. A producer whose record does
// not stand decides nothing here: a task that follows it is withdrawn by the
// follows rule.
func HoldChildInputs(records map[string]ResumeRecord, runID string, received []NamedArtifactPlan) {
	failed := map[string]ResumeRecord{}
	for id, r := range records {
		if r.Answer != KeptValid || r.Answered.GetChild() == nil {
			continue
		}
		for _, in := range r.Answered.GetChild().GetInputArtifacts() {
			if in.GetRunId() != runID {
				if !receivedAs(received, in) {
					failed[id] = unreadableRecord("what it passed its child as %s is not what its run received", in.GetName())
					break
				}
				continue
			}
			producer, read := records[in.GetTaskId()]
			if !read || !producedOnRecord(producer, in.GetArtifact()) {
				failed[id] = unreadableRecord("what it passed its child as %s is not what task %s's record holds", in.GetName(), in.GetTaskId())
				break
			}
		}
	}
	for id, r := range failed {
		records[id] = r
	}
}

// receivedAs reports whether in, passed on from a run above, is an artifact
// the run's plan says it received.
func receivedAs(received []NamedArtifactPlan, in *orkstackv1.NamedArtifact) bool {
	ref := in.GetArtifact()
	for _, r := range received {
		if r.RunID == in.GetRunId() && r.TaskID == in.GetTaskId() && r.Artifact.SHA256 == ref.GetSha256() && r.Artifact.URI == ref.GetUri() {
			return true
		}
	}
	return false
}

// producedOnRecord reports whether ref, an artifact passed under another name,
// is one producer's record holds, or producer has no record that stands.
func producedOnRecord(producer ResumeRecord, ref *orkstackv1.ArtifactRef) bool {
	if producer.Answer != KeptValid {
		return true
	}
	name := ref.GetMetadata()[ArtifactSourceNameKey]
	for _, a := range producer.Artifacts {
		if a.GetName() == name && a.GetSha256() == ref.GetSha256() && a.GetUri() == ref.GetUri() && a.GetSizeBytes() == ref.GetSizeBytes() {
			return true
		}
	}
	return false
}
