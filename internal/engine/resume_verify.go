package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ResumeArtifacts is the slice of artifacts.Store verification reads.
type ResumeArtifacts interface {
	AdoptStore
	Stat(ctx context.Context, uri string) (artifacts.ArtifactRef, error)
}

// ResumeVerifier answers whether each result a resume would keep is there
// and is that unit's (design §3.5). ResumeRun asks it at a preview and again
// at a confirm, never from a cache. It reads a commit with the read-back's
// own code, so what it measures is what the resumed run reads back.
type ResumeVerifier struct {
	acts  *ResumeActivities
	store ResumeArtifacts
	tries int
}

// NewResumeVerifier builds it over the deployment's commit and artifact
// stores.
func NewResumeVerifier(commits artifacts.CommitStore, store ResumeArtifacts) *ResumeVerifier {
	return &ResumeVerifier{acts: NewResumeActivities(commits, store), store: store, tries: defaultReadbackTunables().GetTries}
}

// withTries is v with tries per read, for a test of an outage.
func (v *ResumeVerifier) withTries(tries int) *ResumeVerifier {
	v.tries = tries
	return v
}

// ResumeVerification is what Verify found.
type ResumeVerification struct {
	// Evidence holds Kept and Why; the rest is the caller's.
	Evidence ResumeEvidence
	// Sizes is the encoded read-back entry of each valid unit.
	Sizes map[ResumeUnit]int64
	// Causes is, for the log, why each unit that is not valid is not: in the
	// store's own terms, which no caller is shown.
	Causes map[ResumeUnit]error
}

// MaxReadbackEntryBytes is the largest encoded entry one read-back call can
// hold; a kept result above it cannot be read back (RESUME_TOO_LARGE).
func MaxReadbackEntryBytes() int64 {
	return defaultReadbackTunables().Budget - readbackFraming(1)
}

// ResumeRecordedRun is how an admitted run's records are read: a run whose
// results the engine records (design §28). Its results are what the engine
// recorded, never what an Orker committed.
type ResumeRecordedRun struct {
	Store ResumeRecordStore
	// AdmissionSHA256, Resumes, End and Spacings are as ResumeRecordsAsk's.
	AdmissionSHA256 string
	Resumes         []artifacts.ResumeRecord
	End             time.Time
	Spacings        *artifacts.RecordSpacings
	ReadTimeout     time.Duration
	// Children and Received are as ResumeRecordsAsk's.
	Children map[string]ResumeChildAsk
	Received []NamedArtifactPlan
}

// Verify answers for every kept candidate of in, reading at most the
// read-back's concurrency at once. Its error says in is inconsistent or ctx
// ended; a store that does not answer is KeptUnavailable.
func (v *ResumeVerifier) Verify(ctx context.Context, in ResumePlanInput, commitRoot string) (ResumeVerification, error) {
	return v.VerifyRecorded(ctx, in, commitRoot, nil)
}

// VerifyRecorded is Verify, and for an admitted run (recorded set) the
// evidence its records give: each result at the engine's record, held to the
// run's resumes, and what each record follows.
func (v *ResumeVerifier) VerifyRecorded(ctx context.Context, in ResumePlanInput, commitRoot string, recorded *ResumeRecordedRun) (ResumeVerification, error) {
	in.Evidence = ResumeEvidence{}
	if in.Definition == nil || in.Base == nil || commitRoot == "" {
		return ResumeVerification{}, errors.New("engine: verify a resume: the definition, the base and the commit root are required")
	}
	out := ResumeVerification{
		Evidence: ResumeEvidence{Kept: map[ResumeUnit]KeptAnswer{}, Why: map[ResumeUnit]string{}},
		Sizes:    map[ResumeUnit]int64{},
		Causes:   map[ResumeUnit]error{},
	}
	var records map[string]ResumeRecord
	var held map[ResumeUnit]error
	if recorded != nil {
		ask := ResumeRecordsAsk{Definition: in.Definition, Base: in.Base, AdmissionSHA256: recorded.AdmissionSHA256,
			Resumes: recorded.Resumes, End: recorded.End, Spacings: recorded.Spacings, ReadTimeout: recorded.ReadTimeout,
			Children: recorded.Children, ForceCanceled: in.Class == ResumeBaseForceCanceled}
		var err error
		if records, err = ReadResumeRecords(ctx, recorded.Store, ask); err != nil {
			return ResumeVerification{}, fmt.Errorf("engine: verify a resume: %w", err)
		}
		held = ResumeScopesHeld(ask)
		HoldChildInputs(records, in.Base.GetId(), recorded.Received)
		out.Evidence.fromRecords(in.Definition, in.Base, records)
		in.Evidence = out.Evidence
	} else {
		out.Evidence.fromHistory(in.Base, in.Facts.Resolved)
		in.Evidence = out.Evidence
	}
	p, err := newPlanner(in)
	if err != nil {
		return ResumeVerification{}, err
	}
	acts := v.acts
	if recorded != nil {
		// The read-back's own activities, reading the records it reads.
		own := *v.acts
		own.recorded = recorded.Store
		acts = &own
	}
	units := p.keptUnits()
	entries := make([]*orkstackv1.AdoptEntry, len(units))
	for i, c := range units {
		if c.kind == kindContainer {
			if entries[i], err = adoptEntryOf(in.Definition, commitRoot, in.Base.GetId(), c.def, int32(c.unit.Item), c.row.GetResultEpoch(), recorded != nil); err != nil {
				return ResumeVerification{}, err
			}
		}
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(defaultReadbackTunables().Concurrency)
	for i, c := range units {
		entry := entries[i]
		g.Go(func() error {
			f, size := keptFinding{answer: KeptValid}, int64(0)
			switch {
			case held[c.unit] != nil:
				f = keptFinding{KeptUnreadable, "its result cannot be read as its own", held[c.unit]}
			case entry != nil:
				f, size = v.commit(gctx, entry)
			case recorded != nil:
				f, size = v.settledOnRecord(gctx, acts, in, c, records[c.unit.Task], recorded.AdmissionSHA256)
			default:
				f, size = v.settled(gctx, in, c)
			}
			if err := gctx.Err(); err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			out.Evidence.Kept[c.unit] = f.answer
			if f.answer != KeptValid {
				out.Evidence.Why[c.unit], out.Causes[c.unit] = f.why, f.cause
			}
			if size > 0 {
				out.Sizes[c.unit] = size
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return ResumeVerification{}, fmt.Errorf("engine: verify a resume: %w", err)
	}
	return out, nil
}

// keptFinding is what verification found of one saved result: why, as a
// caller is told it, and its cause.
type keptFinding struct {
	answer KeptAnswer
	why    string
	cause  error
}

// storeUnanswered is the finding of a store that did not answer.
func storeUnanswered(cause error) keptFinding {
	return keptFinding{KeptUnavailable, "its store did not answer", cause}
}

// commit is what a container unit's commit answers, with its encoded entry's
// size when it is whole.
func (v *ResumeVerifier) commit(ctx context.Context, e *orkstackv1.AdoptEntry) (keptFinding, int64) {
	result, err := v.acts.adoptOne(ctx, e, defaultReadbackTunables().GetTimeout, v.tries)
	if err != nil {
		if !isFinal(err) {
			return storeUnanswered(err), 0
		}
		why := "its saved result can't be read back"
		switch categoryOf(err).appType() {
		case ReasonCommitMissing:
			why = "its saved result is gone"
		case ReasonCommitUndecodable:
			why = "its saved result can't be decoded"
		case ReasonCommitIdentity:
			why = "its saved result is of a different activity"
		}
		return keptFinding{KeptGone, why, err}, 0
	}
	for _, ref := range append(append([]*orkstackv1.ArtifactRef{result.GetValuesRef()}, result.GetArtifacts()...), result.GetSystemArtifacts()...) {
		if f := v.values(ctx, ref); f.answer != KeptValid {
			return f, 0
		}
	}
	size, err := task.PayloadSize(&orkstackv1.AdoptedEntry{TaskId: e.GetTaskId(), Item: e.GetItem(), Result: result})
	if err != nil {
		return keptFinding{KeptUnavailable, "its saved result can't be measured", err}, 0
	}
	return keptFinding{answer: KeptValid}, size
}

// settled is what an awaited or composed task's saved values and artifacts
// answer, with their encoded entry's size when they are whole.
func (v *ResumeVerifier) settled(ctx context.Context, in ResumePlanInput, c keptUnit) (keptFinding, int64) {
	ref := c.row.GetValuesRef()
	if ref.GetUri() == "" {
		return keptFinding{KeptGone, "its saved values are not recorded", errors.New("its row has no values_ref")}, 0
	}
	for _, r := range append([]*orkstackv1.ArtifactRef{ref}, c.row.GetArtifacts()...) {
		if f := v.values(ctx, r); f.answer != KeptValid {
			return f, 0
		}
	}
	if c.kind == kindComposed && ref.GetSizeBytes() > composedValuesMax {
		// Never read whole: BuildResumeBlock refuses its size.
		return keptFinding{answer: KeptValid}, ref.GetSizeBytes()
	}
	e := settledEntryOf(in.Definition, in.Base.GetId(), c.def, ref)
	result, err := v.acts.adoptOne(ctx, e, defaultReadbackTunables().GetTimeout, v.tries)
	if err != nil {
		if !isFinal(err) {
			return storeUnanswered(err), 0
		}
		return keptFinding{KeptGone, "its saved values can't be read back", err}, 0
	}
	size, err := task.PayloadSize(&orkstackv1.AdoptedEntry{TaskId: e.GetTaskId(), Item: e.GetItem(), Result: result})
	if err != nil {
		return keptFinding{KeptUnavailable, "its saved values can't be measured", err}, 0
	}
	return keptFinding{answer: KeptValid}, size
}

// settledOnRecord is settled for an admitted run: what its record says the
// task's values and artifacts are, held to its row by ReadResumeRecords.
func (v *ResumeVerifier) settledOnRecord(ctx context.Context, acts *ResumeActivities, in ResumePlanInput, c keptUnit, r ResumeRecord, admission string) (keptFinding, int64) {
	switch r.Answer {
	case KeptValid:
	case keptUnasked:
		// ReadResumeRecords reads every task a resume could keep; one it did
		// not answer for is not known to be this task's.
		return keptFinding{KeptUnreadable, "its record was not read", errors.New("no record was read for it")}, 0
	default:
		return keptFinding{r.Answer, r.Why, r.Cause}, 0
	}
	if r.Values.GetUri() == "" {
		return keptFinding{KeptUnreadable, "its record names no values", errors.New("the record names no values")}, 0
	}
	for _, ref := range append([]*orkstackv1.ArtifactRef{r.Values}, r.Artifacts...) {
		if f := v.values(ctx, ref); f.answer != KeptValid {
			return f, 0
		}
	}
	if c.kind == kindComposed && r.Values.GetSizeBytes() > composedValuesMax {
		return keptFinding{answer: KeptValid}, r.Values.GetSizeBytes()
	}
	if c.row.GetResultEpoch() < 0 {
		return keptFinding{KeptUnreadable, "its row names no scope", fmt.Errorf("result epoch %d", c.row.GetResultEpoch())}, 0
	}
	// The entry the read-back reads, read by its own code: its record at the
	// row's scope and the values the record names.
	e := recordedEntryOf(in.Definition, in.Base.GetId(), c.def, r.Values, uint64(c.row.GetResultEpoch()), admission)
	read := acts.adoptOne
	if r.FromOutputs {
		// Its record is written from its child's when the resume is
		// confirmed (resumeChildRecords), of these same values: they are
		// read as the read-back will read them.
		read = acts.adoptValues
	}
	result, err := read(ctx, e, defaultReadbackTunables().GetTimeout, v.tries)
	if err != nil {
		if !isFinal(err) {
			return storeUnanswered(err), 0
		}
		if categoryOf(err).appType() == ResolutionRecordInvalidCode {
			return keptFinding{KeptUnreadable, "its record cannot be read as its own", err}, 0
		}
		return keptFinding{KeptGone, "its recorded values can't be read back", err}, 0
	}
	size, err := task.PayloadSize(&orkstackv1.AdoptedEntry{TaskId: e.GetTaskId(), Item: e.GetItem(), Result: result})
	if err != nil {
		return keptFinding{KeptUnavailable, "its recorded values can't be measured", err}, 0
	}
	return keptFinding{answer: KeptValid}, size
}

// fromRecords is the evidence an admitted run's records give before its kept
// results are read: that its run is recorded, each record's follows, each
// result's digest, the answers given in time to the awaits that did not
// succeed, and an open await whose record could not be read.
func (e *ResumeEvidence) fromRecords(wf *orkstackv1.Orkflow, base *orkstackv1.Run, records map[string]ResumeRecord) {
	e.Recorded = true
	e.Follows, e.ResultSHA256, e.Resolutions = map[string][]ResumeFollow{}, map[string]string{}, map[string]ResolutionAnswer{}
	kinds := make(map[string]taskKind, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		kinds[t.GetId()] = (&planTask{def: t}).kind()
	}
	kept := map[string]bool{}
	for _, row := range base.GetTaskRuns() {
		kept[row.GetTaskId()] = keptCandidate(row)
	}
	for id, r := range records {
		if r.ChildCause != "" {
			if e.ChildCause == nil {
				e.ChildCause = map[string]ComposedCause{}
			}
			e.ChildCause[id] = r.ChildCause
		}
		if r.Answer == KeptValid {
			if r.ResultSHA256 != "" {
				e.ResultSHA256[id] = r.ResultSHA256
			}
			if r.ChildOnRecord {
				if e.ChildOnRecord == nil {
					e.ChildOnRecord = map[string]bool{}
				}
				e.ChildOnRecord[id] = true
			}
			if r.FromOutputs {
				if e.ChildFromOutputs == nil {
					e.ChildFromOutputs = map[string]bool{}
				}
				e.ChildFromOutputs[id] = true
			}
			if kinds[id] == kindAwait || kinds[id] == kindComposed {
				e.Follows[id] = append([]ResumeFollow{}, r.Follows...)
			}
			if r.Answered != nil {
				if e.Answered == nil {
					e.Answered = map[string]*orkstackv1.TaskRun{}
				}
				e.Answered[id] = r.Answered
			}
		}
		if kinds[id] == kindComposed && !kept[id] && (r.Answer == KeptUnavailable || r.Answer == KeptUnreadable) {
			e.Kept[nodeUnit(id)], e.Why[nodeUnit(id)] = r.Answer, r.Why
		}
		if kinds[id] != kindAwait || kept[id] {
			continue
		}
		switch {
		case r.Answer == KeptUnavailable || r.Answer == KeptUnreadable:
			e.Kept[nodeUnit(id)], e.Why[nodeUnit(id)] = r.Answer, r.Why
		case r.Resolution.Completed || r.Resolution.Rejected:
			e.Resolutions[id] = r.Resolution
		}
	}
	// A composed task settled from its child's outputs has no record of its
	// own to say what it follows: it follows its needs as they are now, so a
	// need that is not kept withdraws it.
	follows, _ := identity.Follows(wf)
	for id, r := range records {
		if !r.FromOutputs || r.Answer != KeptValid {
			continue
		}
		own := []ResumeFollow{}
		for _, ref := range follows[id] {
			own = append(own, ResumeFollow{Task: ref.TaskID, ResultSHA256: e.ResultSHA256[ref.TaskID]})
		}
		e.Follows[id] = own
	}
}

// fromHistory is the evidence a run without records has of its awaits'
// answers: the resolutions its base received after R and never applied, for
// the awaits that did not succeed (design §24.2, §26.5). A completion's row is
// what the await settles with once its values object is found whole.
func (e *ResumeEvidence) fromHistory(base *orkstackv1.Run, resolved map[string]ResumeResolved) {
	kept := map[string]bool{}
	for _, row := range base.GetTaskRuns() {
		kept[row.GetTaskId()] = keptCandidate(row)
	}
	for id, r := range resolved {
		if kept[id] || !r.Answer.Completed && !r.Answer.Rejected {
			continue
		}
		if e.Resolutions == nil {
			e.Resolutions = map[string]ResolutionAnswer{}
		}
		e.Resolutions[id] = r.Answer
		if r.Answer.Completed {
			if e.Answered == nil {
				e.Answered = map[string]*orkstackv1.TaskRun{}
			}
			e.Answered[id] = r.Row
		}
	}
}

// values is whether the object ref names is there and whole: of the size it
// was recorded with. A ref that names none has nothing to lose.
func (v *ResumeVerifier) values(ctx context.Context, ref *orkstackv1.ArtifactRef) keptFinding {
	if ref.GetUri() == "" {
		return keptFinding{answer: KeptValid}
	}
	sctx, cancel := context.WithTimeout(ctx, defaultReadbackTunables().GetTimeout)
	defer cancel()
	name := ref.GetName()
	if name == "" {
		name = "values.json"
	}
	switch got, err := v.store.Stat(sctx, ref.GetUri()); {
	case err == nil && ref.GetSizeBytes() > 0 && got.SizeBytes != ref.GetSizeBytes():
		return keptFinding{KeptGone, fmt.Sprintf("its %s is %d bytes, not the %d recorded", name, got.SizeBytes, ref.GetSizeBytes()),
			fmt.Errorf("%s was recorded with another size", ref.GetUri())}
	case err == nil:
		return keptFinding{answer: KeptValid}
	case errors.Is(err, artifacts.ErrNotFound):
		return keptFinding{KeptGone, fmt.Sprintf("its %s is gone", name), err}
	default:
		return storeUnanswered(err)
	}
}

// keptUnit is a unit verification must answer for.
type keptUnit struct {
	unit ResumeUnit
	kind taskKind
	def  *orkstackv1.Task
	row  *orkstackv1.TaskRun
}

// keptUnits are the units whose saved result the planner asks about
// (classifyUnit): every kept candidate but a sleep, which has none, and a
// fan-out's node, whose items have theirs.
func (p *planner) keptUnits() []keptUnit {
	var out []keptUnit
	for _, id := range p.order {
		if p.in.Facts.Earlier[id] {
			continue
		}
		t := p.tasks[id]
		for _, u := range t.units() {
			kind, row := t.unitKind(u), t.unitRow(u)
			if kind == kindAwait && p.answeredOnRecord(id, row) || kind == kindComposed && p.in.Evidence.ChildOnRecord[id] {
				out = append(out, keptUnit{unit: u, kind: kind, def: t.def, row: row})
				continue
			}
			if kind == kindAwait && p.answeredInHistory(id, row) {
				// Its answer's values are what is verified: the row it settles
				// with, which the Signal gave.
				out = append(out, keptUnit{unit: u, kind: kind, def: t.def, row: p.in.Evidence.Answered[id]})
				continue
			}
			if kind == kindFanOut || kind == kindSleep || !keptCandidate(row) && !lateCandidate(p.in, kind, u, row) ||
				row.GetStatus() == orkstackv1.Status_STATUS_SKIPPED && !row.GetReadingBack() {
				continue
			}
			out = append(out, keptUnit{unit: u, kind: kind, def: t.def, row: row})
		}
	}
	return out
}

// answeredOnRecord says await id of a recorded run did not succeed in the
// base and is answered on record in time (design §28.1): its record's values
// are verified as a kept result's are.
func (p *planner) answeredOnRecord(id string, row *orkstackv1.TaskRun) bool {
	return p.in.Evidence.Recorded && !keptCandidate(row) && p.in.Evidence.Resolutions[id].Completed
}

// answeredInHistory is answeredOnRecord for a run without records: an await
// that did not succeed and whose base received a completion after R it never
// applied (fromHistory).
func (p *planner) answeredInHistory(id string, row *orkstackv1.TaskRun) bool {
	return !p.in.Evidence.Recorded && !keptCandidate(row) && p.in.Evidence.Resolutions[id].Completed && p.in.Evidence.Answered[id] != nil
}

// adoptEntryOf is the read-back entry of task t (item -1), or of one item of
// it, whose commit lies under epoch. For a run whose results the engine
// records (recorded) it is the engine's record, and nothing an Orker
// committed: verification and the read-back read the one key.
func adoptEntryOf(wf *orkstackv1.Orkflow, commitRoot, runID string, t *orkstackv1.Task, item int32, epoch int64, recorded bool) (*orkstackv1.AdoptEntry, error) {
	if commitRoot == "" {
		return nil, fmt.Errorf("task %q: the run has no commit root", t.GetId())
	}
	if epoch < 0 || epoch > maxResumeScope {
		return nil, fmt.Errorf("task %q: result scope %d is not a sequence", t.GetId(), epoch)
	}
	id, err := ExpectedCommitIdentity(wf, runID, t, item, uint64(epoch))
	if err != nil {
		return nil, err
	}
	uri := strings.TrimSuffix(commitRoot, "/") + "/" + id.Key
	if recorded {
		if uri, err = artifacts.AttestedCommitURI(uri); err != nil {
			return nil, fmt.Errorf("task %q: %w", t.GetId(), err)
		}
	}
	return &orkstackv1.AdoptEntry{
		TaskId: t.GetId(), Item: item, Kind: orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_COMMIT,
		CommitUri: uri,
		Expected: &orkstackv1.ExpectedCommit{
			Namespace: id.Namespace, RunId: id.RunID, TaskId: id.TaskID, DefinitionDigest: id.DefinitionDigest,
			IdempotencyKey: id.IdempotencyKey, ImageIndexDigest: id.ImageIndexDigest,
		},
	}, nil
}
