package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

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
	// Artifacts is what a composed task's record returns.
	Artifacts []*orkstackv1.ArtifactRef
	// Resolution is, for an await that did not succeed, the answer on record.
	Resolution ResolutionAnswer
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
			if !keptCandidate(row) {
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
		return unreadableRecord("%v", err), nil
	}
	if kind == artifacts.FollowContainer {
		read, err := store.ReadAttestedCommit(rctx, artifacts.SlotRun, a.Spacings, artifacts.AttestedWant{Namespace: a.namespace(), RunID: a.Base.GetId(),
			TaskID: row.GetTaskId(), Scope: uint64(scope), AdmissionSHA256: a.AdmissionSHA256})
		if not, ended := a.notFound(ctx, read.Answer, read.Object, read.Reason, err); ended != nil || not != nil {
			return deref(not), ended
		}
		c := read.Value.Commit
		return digested(ResumeRecord{Answer: KeptValid}, c.GetValues().GetSha256(), c.GetArtifacts()), nil
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
		return ResumeRecord{}, nil // an await nobody answered
	}
	if not, ended := a.notFound(ctx, read.Answer, read.Object, read.Reason, err); ended != nil || not != nil {
		return deref(not), ended
	}
	return a.found(kind, row, kept, read.Value), nil
}

// found is what a record that was read says of row's task.
func (a ResumeRecordsAsk) found(kind string, row *orkstackv1.TaskRun, kept bool, f *artifacts.RecordedFound) ResumeRecord {
	rejected, valuesSHA256, follows := f.Standing()
	if kept && (rejected || valuesSHA256 != row.GetValuesRef().GetSha256()) {
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
	} else {
		// A composed task's record has no time to be held to: it is due by no
		// deadline, only an engine writes it, while the parent is open, and the
		// row took these values from it. A check against the run's end could
		// only refuse a real result, where the store's clock is ahead.
		out.Artifacts = f.Artifacts()
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
