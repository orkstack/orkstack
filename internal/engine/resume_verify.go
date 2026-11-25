package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
	// Sizes is the encoded read-back entry of each valid container unit.
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

// Verify answers for every kept candidate of in, reading at most the
// read-back's concurrency at once. Its error says in is inconsistent or ctx
// ended; a store that does not answer is KeptUnavailable.
func (v *ResumeVerifier) Verify(ctx context.Context, in ResumePlanInput, commitRoot string) (ResumeVerification, error) {
	in.Evidence = ResumeEvidence{}
	if in.Definition == nil || in.Base == nil || commitRoot == "" {
		return ResumeVerification{}, errors.New("engine: verify a resume: the definition, the base and the commit root are required")
	}
	p, err := newPlanner(in)
	if err != nil {
		return ResumeVerification{}, err
	}
	out := ResumeVerification{
		Evidence: ResumeEvidence{Kept: map[ResumeUnit]KeptAnswer{}, Why: map[ResumeUnit]string{}},
		Sizes:    map[ResumeUnit]int64{},
		Causes:   map[ResumeUnit]error{},
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(defaultReadbackTunables().Concurrency)
	for _, c := range p.keptUnits() {
		entry := (*orkstackv1.AdoptEntry)(nil)
		if c.kind == kindContainer {
			if entry, err = adoptEntryOf(in.Definition, commitRoot, in.Base.GetId(), c.def, int32(c.unit.Item), c.row.GetResultEpoch()); err != nil {
				return ResumeVerification{}, err
			}
		}
		g.Go(func() error {
			f, size := keptFinding{answer: KeptValid}, int64(0)
			if entry != nil {
				f, size = v.commit(gctx, entry)
			} else {
				f = v.values(gctx, c.row.GetValuesRef())
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
			if kind == kindFanOut || kind == kindSleep || !keptCandidate(row) ||
				row.GetStatus() == orkstackv1.Status_STATUS_SKIPPED && !row.GetReadingBack() {
				continue
			}
			out = append(out, keptUnit{unit: u, kind: kind, def: t.def, row: row})
		}
	}
	return out
}

// adoptEntryOf is the read-back entry of task t (item -1), or of one item of
// it, whose commit lies under epoch.
func adoptEntryOf(wf *orkstackv1.Orkflow, commitRoot, runID string, t *orkstackv1.Task, item int32, epoch int64) (*orkstackv1.AdoptEntry, error) {
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
	return &orkstackv1.AdoptEntry{
		TaskId: t.GetId(), Item: item, Kind: orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_COMMIT,
		CommitUri: strings.TrimSuffix(commitRoot, "/") + "/" + id.Key,
		Expected: &orkstackv1.ExpectedCommit{
			Namespace: id.Namespace, RunId: id.RunID, TaskId: id.TaskID, DefinitionDigest: id.DefinitionDigest,
			IdempotencyKey: id.IdempotencyKey, ImageIndexDigest: id.ImageIndexDigest,
		},
	}, nil
}
