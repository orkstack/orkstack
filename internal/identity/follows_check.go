package identity

import (
	"context"
	"errors"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The check of what a task follows, read by key (docs/TASK_IDENTITY.md,
// "What a task follows"): what the mint and both ends of a commit ask
// before a token is signed or a result recorded, and what a writer of an
// awaited or a composed task's record asks before it writes. A followed
// task's record is found by its key, never by a listing, and only a store
// that said "no such key", twice, is a miss.

const (
	// followsParallel is how many records one check reads at a time.
	followsParallel = 16
	// maxRunsAbove bounds the walk from a child run up to the run a follow
	// names; a composed tree is far shallower.
	maxRunsAbove = 64
)

// Scopes are the resume scopes an attempt names: Resume its own run's, for
// a root, and Root its root's when the run was started, for a run below
// one. Zero is none.
type Scopes struct{ Resume, Root uint64 }

// ofRoot is the root's resume scope the attempt runs under.
func (s Scopes) ofRoot(adm artifacts.Admission) uint64 {
	if adm.RootRunID != "" {
		return s.Root
	}
	return s.Resume
}

// FollowedRecord is one followed task's record, as the check read it.
type FollowedRecord struct {
	Ref artifacts.FollowRef
	// RecordSHA256 is of the record's stored bytes; ResultSHA256 is of what
	// the task produced, and the one two records are compared by.
	RecordSHA256, ResultSHA256 string
	// artifacts are what the record says the task produced, values where its
	// values are (an awaited task's record does not say).
	artifacts []*orkstackv1.ArtifactRef
	values    *orkstackv1.ArtifactRef
	// valuesSHA256 is the digest of its values.
	valuesSHA256 string
}

// FollowsStore is what the check reads.
type FollowsStore interface {
	ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error)
	ReadAttestedCommit(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, want artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error)
	ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error)
	ResumeChain(ctx context.Context, namespace, runID string) (artifacts.RecordRead[[]artifacts.ResumeRecord], error)
}

// FollowsCheck is the FollowsChecker over the identity records.
type FollowsCheck struct {
	Store FollowsStore
	// Spacing is the waits of one read; nil is the reader's own.
	Spacing *artifacts.RecordSpacings
}

var _ FollowsChecker = (*FollowsCheck)(nil)

// FollowedBy is what task follows: its own needs on record, or, with none,
// what the run's start follows.
func FollowedBy(adm artifacts.Admission, taskID string) []artifacts.FollowRef {
	if refs := adm.Follows[taskID]; len(refs) > 0 {
		return refs
	}
	return adm.StartFollows
}

// CheckScope holds the scope an attempt names against its root's resumes,
// and never believes it: it is the newest (Establish), and one other than 0,
// for the root's own attempt, names the task.
func (f *FollowsCheck) CheckScope(ctx context.Context, adm artifacts.Admission, taskID string, scopes Scopes) error {
	resume, err := f.Establish(ctx, adm, scopes)
	if err != nil {
		return err
	}
	if scopes.Resume != 0 && !slices.Contains(resume.Tasks, taskID) {
		return refuse(ReasonNotAdmitted, true, "resume %d of run %q does not run task %q again", scopes.Resume, adm.RunID, taskID)
	}
	return nil
}

// Establish holds the scope named for a run against its root's resumes: the
// newest, which no link leads from, and answers its record, the one the link
// to it names and the reset was made with. Zero is a run never resumed.
func (f *FollowsCheck) Establish(ctx context.Context, adm artifacts.Admission, scopes Scopes) (artifacts.ResumeRecord, error) {
	chain, err := f.established(ctx, adm, scopes)
	if err != nil || len(chain) == 0 {
		return artifacts.ResumeRecord{}, err
	}
	return chain[0], nil
}

// DecidingScope is Establish, and the scope at which taskID's record of adm's
// run counts, by the same rule as a followed task's.
func (f *FollowsCheck) DecidingScope(ctx context.Context, adm artifacts.Admission, taskID string, scopes Scopes) (uint64, error) {
	chain, err := f.established(ctx, adm, scopes)
	if err != nil || adm.RootRunID != "" {
		return 0, err
	}
	return DecidingScope(chain, taskID), nil
}

// DecidingScope is the newest resume of chain, newest first, that ran the task
// again or reopened it; the base when none did.
func DecidingScope(chain []artifacts.ResumeRecord, taskID string) uint64 {
	for _, r := range chain {
		if slices.Contains(r.Tasks, taskID) || slices.Contains(r.ReopenedAwaits, taskID) {
			return r.Scope
		}
	}
	return 0
}

// established is the resumes of adm's root, newest first, once the scope named
// is held to be the newest.
func (f *FollowsCheck) established(ctx context.Context, adm artifacts.Admission, scopes Scopes) ([]artifacts.ResumeRecord, error) {
	below := adm.RootRunID != ""
	root, scope := adm.RunID, scopes.Resume
	if below {
		root, scope = adm.RootRunID, scopes.Root
	}
	if (below && scopes.Resume != 0) || (!below && scopes.Root != 0) {
		return nil, refuse(ReasonNotAdmitted, true, "run %q names a resume scope that is not its own", adm.RunID)
	}
	return f.resumeChain(ctx, adm.Namespace, root, scope)
}

// resumeChain is root's resumes from the newest back, whose scope is newest.
func (f *FollowsCheck) resumeChain(ctx context.Context, namespace, root string, newest uint64) ([]artifacts.ResumeRecord, error) {
	read, err := f.Store.ResumeChain(ctx, namespace, root)
	switch {
	case err != nil:
		return nil, unavailableMint("the resumes of run %q could not be read: %v", root, err)
	case read.Answer != artifacts.RecordFound:
		return nil, unavailableMint("the resumes of run %q cannot be trusted (%s %s: %s)", root, read.Answer, read.Object, read.Reason)
	}
	var at uint64
	if len(read.Value) > 0 {
		at = read.Value[0].Scope
	}
	if at != newest {
		for _, r := range read.Value {
			if r.Scope == newest || newest == 0 {
				return nil, refuse(ReasonNotAdmitted, true, "this attempt names resume %d of run %q, and a later resume has replaced it", newest, root)
			}
		}
		return nil, refuse(ReasonNotAdmitted, true, "run %q has no resume %d", root, newest)
	}
	return read.Value, nil
}

// CheckFollows answers the record of each task taskID follows, in the order
// it follows them: the run's admission adm, named admissionSHA256, at the
// root's resume scope rootScope, which the caller has established is the
// newest (CheckScope). A task with no record of success at the scope that
// decides for it, or whose record below the newest scope follows a result
// since replaced, is REQUIRED_RESULT_NOT_RECORDED; a record that could not
// be read or trusted is IDENTITY_UNAVAILABLE.
func (f *FollowsCheck) CheckFollows(ctx context.Context, adm artifacts.Admission, admissionSHA256, taskID string, rootScope uint64) ([]FollowedRecord, error) {
	return f.records(ctx, adm, admissionSHA256, FollowedBy(adm, taskID), rootScope)
}

// records is CheckFollows of refs.
func (f *FollowsCheck) records(ctx context.Context, adm artifacts.Admission, admissionSHA256 string, refs []artifacts.FollowRef, rootScope uint64) ([]FollowedRecord, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	c, err := f.begin(ctx, adm, admissionSHA256, rootScope, refs)
	if err != nil {
		return nil, err
	}
	return ofEach(refs, func(_ int, ref artifacts.FollowRef) (FollowedRecord, error) {
		return c.standing(ctx, adm.RunID, ref)
	})
}

// CheckReceived answers the artifacts of the run's start that taskID names,
// each under its input's name, exactly as the records of the tasks that
// produced them name them, at the scopes that decide for those. One whose
// producer has no record, or whose record names no such artifact, is
// REQUIRED_RESULT_NOT_RECORDED; but one a failure handler passed, whose
// producer has none, is answered by its name alone: there is nothing to hold
// it to, and the attempt stages no other unrecorded one.
func (f *FollowsCheck) CheckReceived(ctx context.Context, adm artifacts.Admission, admissionSHA256, taskID string, rootScope uint64) ([]*orkstackv1.ArtifactRef, error) {
	names := adm.Tasks[taskID].Inputs
	found, err := f.received(ctx, adm, admissionSHA256, names, rootScope)
	for i, a := range found {
		if a == nil {
			found[i] = &orkstackv1.ArtifactRef{Name: names[i]}
		}
	}
	return found, err
}

// received is CheckReceived for the artifacts of the run's start names name,
// one for each: nil for one with no record that a failure handler passed.
func (f *FollowsCheck) received(ctx context.Context, adm artifacts.Admission, admissionSHA256 string, names []string, rootScope uint64) ([]*orkstackv1.ArtifactRef, error) {
	if len(names) == 0 {
		return nil, nil
	}
	refs := make([]artifacts.FollowRef, len(names))
	for i, name := range names {
		src, ok := adm.StartArtifacts[name]
		if !ok {
			return nil, refuse(ReasonNotAdmitted, true, "run %q was not admitted to receive an artifact %q", adm.RunID, name)
		}
		refs[i] = artifacts.FollowRef{RunID: src.RunID, TaskID: src.TaskID, Kind: src.Kind}
	}
	c, err := f.begin(ctx, adm, admissionSHA256, rootScope, refs)
	if err != nil {
		return nil, err
	}
	found := make([]*orkstackv1.ArtifactRef, len(names))
	if _, err := ofEach(refs, func(i int, ref artifacts.FollowRef) (FollowedRecord, error) {
		src := adm.StartArtifacts[names[i]]
		got, err := c.standing(ctx, adm.RunID, ref)
		var m *MintError
		// No record of its producer: not one that does not stand.
		if src.Handler && errors.As(err, &m) && m.absent && m.Task == ref.TaskID {
			return FollowedRecord{}, nil
		}
		if err != nil {
			return FollowedRecord{}, err
		}
		for _, a := range got.artifacts {
			if a.GetName() == src.Name {
				found[i] = proto.Clone(a).(*orkstackv1.ArtifactRef)
				found[i].Name = names[i]
				return got, nil
			}
		}
		return FollowedRecord{}, c.notRecorded(src.TaskID, "the record of task %q of run %q names no artifact %q", src.TaskID, src.RunID, src.Name)
	}); err != nil {
		return nil, err
	}
	return found, nil
}

// begin is a read of refs' records: the runs they name, and the root's resumes.
func (f *FollowsCheck) begin(ctx context.Context, adm artifacts.Admission, admissionSHA256 string, rootScope uint64, refs []artifacts.FollowRef) (*followsRead, error) {
	c := &followsRead{f: f, runs: map[string]followedRun{adm.RunID: {adm, admissionSHA256}}, root: adm.RunID, newest: rootScope,
		reads: make(chan struct{}, followsParallel)}
	if adm.RootRunID != "" {
		c.root = adm.RootRunID
	}
	if err := c.readRuns(ctx, adm, refs); err != nil {
		return nil, err
	}
	if err := c.readResumes(ctx, adm.Namespace); err != nil {
		return nil, err
	}
	return c, nil
}

func resultArtifacts(refs []*orkstackv1.ArtifactRef) []ResultArtifact {
	out := make([]ResultArtifact, 0, len(refs))
	for _, a := range refs {
		out = append(out, ResultArtifact{Name: a.GetName(), SHA256: a.GetSha256()})
	}
	return out
}

// followedRun is a run whose records a check reads, and its admission.
type followedRun struct {
	adm artifacts.Admission
	sha string
}

// followsRead is one check's read: the runs it names, and the root's
// resumes from the newest back.
type followsRead struct {
	f       *FollowsCheck
	runs    map[string]followedRun
	root    string
	newest  uint64
	resumes []artifacts.ResumeRecord
	// reads bounds the records read at a time, a record's own follows
	// included.
	reads chan struct{}
}

// readRuns reads the admission of each run refs name, walking up from adm
// by the starter each admission names: a child's start follows only runs
// above it.
func (c *followsRead) readRuns(ctx context.Context, adm artifacts.Admission, refs []artifacts.FollowRef) error {
	at := adm
	for _, ref := range refs {
		if ref.RunID == "" {
			continue
		}
		for walked := 0; ; walked++ {
			if _, ok := c.runs[ref.RunID]; ok {
				break
			}
			if at.Starter.Kind != artifacts.StarterParentRun || walked >= maxRunsAbove {
				return refuse(ReasonNotAdmitted, true, "run %q follows a task of run %q, which is not above it", adm.RunID, ref.RunID)
			}
			read, err := c.f.Store.ReadAdmission(ctx, adm.Namespace, at.Starter.RunID, at.Starter.AdmissionSHA256)
			switch {
			case err != nil:
				return unavailableMint("the admission record of run %q could not be read: %v", at.Starter.RunID, err)
			case read.Answer == artifacts.RecordNone:
				return refuse(ReasonNotAdmitted, true, "run %q has no admission record", at.Starter.RunID)
			case read.Answer != artifacts.RecordFound:
				return unavailableMint("the admission record of run %q cannot be trusted (%s: %s)", at.Starter.RunID, read.Object, read.Reason)
			case read.Value.Namespace != adm.Namespace || read.Value.RunID != at.Starter.RunID:
				return refuse(ReasonNotAdmitted, true, "the admission record named for run %q is another run's", at.Starter.RunID)
			}
			c.runs[at.Starter.RunID] = followedRun{read.Value, at.Starter.AdmissionSHA256}
			at = read.Value
		}
	}
	return nil
}

// readResumes reads the root's resumes from the newest back, where any run
// read is the root: a resume decides nothing for the runs below it.
func (c *followsRead) readResumes(ctx context.Context, namespace string) error {
	if _, reads := c.runs[c.root]; !reads {
		return nil
	}
	if c.newest == 0 {
		return nil
	}
	chain, err := c.f.resumeChain(ctx, namespace, c.root, c.newest)
	c.resumes = chain
	return err
}

// decides is the scope at which a task's record of run counts: the newest
// resume of the root that ran the task again or reopened it, and the base
// when none did, or when run is not the root.
func (c *followsRead) decides(run, taskID string) uint64 {
	if run != c.root {
		return 0
	}
	return DecidingScope(c.resumes, taskID)
}

// standing is ref's record, where it counts: a success, and, where a later
// resume left it standing, one that follows what is current.
func (c *followsRead) standing(ctx context.Context, own string, ref artifacts.FollowRef) (FollowedRecord, error) {
	run := ref.RunID
	if run == "" {
		run = own
	}
	rec, follows, err := c.record(ctx, run, ref)
	if err != nil {
		return FollowedRecord{}, err
	}
	if run != c.root || c.newest == 0 || ref.Kind == artifacts.FollowContainer || c.decides(run, ref.TaskID) == c.newest {
		return rec, nil
	}
	// A resume runs again everything downstream of what it runs again, so
	// an honest tree never meets a difference here.
	now := FollowedBy(c.runs[run].adm, ref.TaskID)
	if len(now) != len(follows) {
		return FollowedRecord{}, c.notRecorded(ref.TaskID, "the record of %q does not name what it follows", ref.TaskID)
	}
	recorded := make(map[string]string, len(follows))
	for _, row := range follows {
		recorded[row.GetTaskId()] = row.GetResultSha256()
	}
	current, err := ofEach(now, func(_ int, up artifacts.FollowRef) (FollowedRecord, error) {
		got, _, err := c.record(ctx, run, up)
		return got, err
	})
	if err != nil {
		return FollowedRecord{}, err
	}
	for _, up := range current {
		if recorded[up.Ref.TaskID] != up.ResultSHA256 {
			return FollowedRecord{}, c.notRecorded(ref.TaskID, "the record of %q follows a result of %q that has since been replaced", ref.TaskID, up.Ref.TaskID)
		}
	}
	return rec, nil
}

// record reads ref's record of run at the scope that decides for it, and
// what it follows.
func (c *followsRead) record(ctx context.Context, run string, ref artifacts.FollowRef) (FollowedRecord, []*orkstackv1.RecordedSource, error) {
	select {
	case c.reads <- struct{}{}:
		defer func() { <-c.reads }()
	case <-ctx.Done():
		return FollowedRecord{}, nil, unavailableMint("the record of task %q of run %q could not be read: %v", ref.TaskID, run, ctx.Err())
	}
	of, scope := c.runs[run], c.decides(run, ref.TaskID)
	if ref.Kind == artifacts.FollowContainer {
		read, err := c.f.Store.ReadAttestedCommit(ctx, artifacts.SlotRun, c.f.Spacing, artifacts.AttestedWant{Namespace: of.adm.Namespace,
			RunID: run, TaskID: ref.TaskID, Scope: scope, AdmissionSHA256: of.sha})
		if err := c.answer(run, ref, scope, read.Answer, read.Object, read.Reason, err); err != nil {
			return FollowedRecord{}, nil, err
		}
		produced := read.Value.Commit.GetArtifacts()
		result, err := ResultSHA256(read.Value.Commit.GetValues().GetSha256(), resultArtifacts(produced))
		if err != nil {
			return FollowedRecord{}, nil, unavailableMint("the record of task %q of run %q cannot be trusted: %v", ref.TaskID, run, err)
		}
		return FollowedRecord{Ref: ref, RecordSHA256: read.Value.SHA256, ResultSHA256: result, artifacts: produced,
			values: read.Value.Commit.GetValues(), valuesSHA256: read.Value.Commit.GetValues().GetSha256()}, nil, nil
	}
	kind, wantKind := artifacts.RecordResolution, orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION
	if ref.Kind == artifacts.FollowComposed {
		kind, wantKind = artifacts.RecordComposed, orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED
	}
	key, err := artifacts.RecordKey(kind, of.adm.Namespace, run, ref.TaskID, scope)
	if err != nil {
		return FollowedRecord{}, nil, unavailableMint("the record of task %q of run %q: %v", ref.TaskID, run, err)
	}
	read, err := c.f.Store.ReadRecordedValues(ctx, artifacts.SlotRun, c.f.Spacing, key, artifacts.RecordWant{Kind: wantKind,
		Namespace: of.adm.Namespace, RunID: run, TaskID: ref.TaskID, Scope: scope, AdmissionSHA256: of.sha})
	if err := c.answer(run, ref, scope, read.Answer, read.Object, read.Reason, err); err != nil {
		return FollowedRecord{}, nil, err
	}
	rejected, values, follows := read.Value.Standing()
	produced := read.Value.Artifacts()
	if rejected {
		return FollowedRecord{}, nil, c.notRecorded(ref.TaskID, "task %q of run %q was rejected", ref.TaskID, run)
	}
	result, err := ResultSHA256(values, resultArtifacts(produced))
	if err != nil {
		return FollowedRecord{}, nil, unavailableMint("the record of task %q of run %q cannot be trusted: %v", ref.TaskID, run, err)
	}
	return FollowedRecord{Ref: ref, RecordSHA256: read.Value.RecordSHA256(), ResultSHA256: result, artifacts: produced,
		values: read.Value.ValuesRef(), valuesSHA256: values}, follows, nil
}

// answer is a read's error, or nil for a record found.
func (c *followsRead) answer(run string, ref artifacts.FollowRef, scope uint64, got artifacts.RecordAnswer, object, reason string, err error) error {
	switch {
	case err != nil:
		return unavailableMint("the record of task %q of run %q could not be read: %v", ref.TaskID, run, err)
	case got == artifacts.RecordNone:
		e := c.notRecorded(ref.TaskID, "task %q of run %q has no record under scope %d", ref.TaskID, run, scope)
		e.absent = true
		return e
	case got != artifacts.RecordFound:
		e := unavailableMint("the record of task %q of run %q cannot be trusted (%s %s: %s)", ref.TaskID, run, got, object, reason)
		e.invalid = got == artifacts.RecordInvalid
		return e
	}
	return nil
}

func (c *followsRead) notRecorded(task, format string, args ...any) *MintError {
	e := refuse(ReasonRequiredResultNotRecorded, true, format, args...)
	e.Task = task
	return e
}

// ofEach is fn of each of refs at once, in their order. A refusal found is
// answered before a record that cannot be used, that before a read that could
// not be made, and otherwise the first error in that order.
func ofEach(refs []artifacts.FollowRef, fn func(int, artifacts.FollowRef) (FollowedRecord, error)) ([]FollowedRecord, error) {
	out, errs := make([]FollowedRecord, len(refs)), make([]error, len(refs))
	var wg sync.WaitGroup
	for i, ref := range refs {
		wg.Go(func() { out[i], errs[i] = fn(i, ref) })
	}
	wg.Wait()
	var first, unusable error
	for _, err := range errs {
		var m *MintError
		if errors.As(err, &m) && !m.Unavailable {
			return nil, err
		}
		if unusable == nil && m != nil && m.invalid {
			unusable = err
		}
		if first == nil {
			first = err
		}
	}
	if unusable != nil {
		return nil, unusable
	}
	if first != nil {
		return nil, first
	}
	return out, nil
}
