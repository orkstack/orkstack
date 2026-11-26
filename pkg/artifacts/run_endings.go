package artifacts

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// A run has an ending for each time it ended: its first, and one under the
// scope of each resume that got going (AGENTS.md §7). Each ending has an
// immutable body. The first's is metadata/runs/{run}.jsonl; a later one's, and
// the mark a resume leaves before it resets the run, are
//
//	metadata/run-resumes/{run}/{scope:020}.jsonl
//	metadata/run-resumes/{run}/{scope:020}.resume
//
// The newest body is the run's. A mark decides nothing alone: its reset may
// never have run, and the cluster says whether it did. It says which runs to
// ask about. The index holds the run once, as its newest ending left it: a
// later ending's row holds its scope, a first ending's row is empty.
const (
	// RunResumeNamespace prefixes the bodies of later endings and the marks.
	RunResumeNamespace = "metadata/run-resumes"
	// resumeMarkSuffix is a mark's extension.
	resumeMarkSuffix = ".resume"
	// endingScopeWidth is the fixed width of a scope in a key.
	endingScopeWidth = 20
	// maxRowBytes bounds an index row: a scope's digits.
	maxRowBytes = endingScopeWidth
)

// terminalRunStatuses are the statuses a run's index rows may lie under.
var terminalRunStatuses = []string{"succeeded", "failed", "canceled"}

// RunEndings is what is stored of a run's later endings, in scope order.
type RunEndings struct {
	RunID string
	// Archived are the scopes with a body, and Marked those a resume marked.
	Archived []uint64
	Marked   []uint64
}

// Newest is the newest scope marked or archived, 0 for none.
func (e RunEndings) Newest() uint64 { return max(lastScope(e.Archived), lastScope(e.Marked)) }

func lastScope(scopes []uint64) uint64 {
	if len(scopes) == 0 {
		return 0
	}
	return scopes[len(scopes)-1]
}

// RunEndingKey renders the key of a run's body for the ending under scope;
// scope 0 is its first (RunArchiveKey).
func RunEndingKey(runID string, scope uint64) (string, error) {
	if scope == 0 {
		return RunArchiveKey(runID)
	}
	return runResumeKey(runID, scope, RunArchiveSuffix)
}

// ResumeMarkKey renders the key of the mark a resume leaves under scope.
func ResumeMarkKey(runID string, scope uint64) (string, error) {
	if scope == 0 {
		return "", fmt.Errorf("%w: a resume's scope is above 0", ErrInvalidRunKey)
	}
	return runResumeKey(runID, scope, resumeMarkSuffix)
}

func runResumeKey(runID string, scope uint64, suffix string) (string, error) {
	segment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run ending key: run id: %w", err)
	}
	return path.Join(RunResumeNamespace, segment, fmt.Sprintf("%0*d%s", endingScopeWidth, scope, suffix)), nil
}

// ParseRunEndingKey reverses RunEndingKey for a later ending, and
// ResumeMarkKey; any other key is ok=false.
func ParseRunEndingKey(key string) (runID string, scope uint64, mark, ok bool) {
	rest, found := strings.CutPrefix(key, RunResumeNamespace+"/")
	if !found {
		return "", 0, false, false
	}
	segment, leaf, found := strings.Cut(rest, "/")
	if !found {
		return "", 0, false, false
	}
	if runID, ok = decodeArchiveSegment(segment); !ok {
		return "", 0, false, false
	}
	digits, isBody := strings.CutSuffix(leaf, RunArchiveSuffix)
	if !isBody {
		if digits, mark = strings.CutSuffix(leaf, resumeMarkSuffix); !mark {
			return "", 0, false, false
		}
	}
	scope, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || scope == 0 || len(digits) != endingScopeWidth {
		return "", 0, false, false
	}
	return runID, scope, mark, true
}

// RunEndingStore is a run archive that keeps every ending of a run
// (BlobRunArchive). It is separate from RunArchive so a fake that stands in
// for the archive need not implement it.
type RunEndingStore interface {
	// GetEnding returns the body of the ending under scope, 0 for the first.
	// One never archived is an error matching ErrNotFound.
	GetEnding(ctx context.Context, runID string, scope uint64) ([]byte, error)
	// HasEnding reports whether the ending under scope has a body, without
	// reading it. A failure to answer is an error, never false.
	HasEnding(ctx context.Context, runID string, scope uint64) (bool, error)
	// RunEndings is what is stored of a run's later endings.
	RunEndings(ctx context.Context, runID string) (RunEndings, error)
	// EachResumedRun hands visit every run with a mark or a later ending, in
	// key order, until visit fails.
	EachResumedRun(ctx context.Context, visit func(RunEndings) error) error
	// MarkResumed leaves the mark of a resume under scope, before its reset: it
	// names the ending the resume sets aside. Marking again is
	// a no-op.
	MarkResumed(ctx context.Context, runID string, scope, setAside uint64) error
	// SetAside is the ending that the resume under scope set aside, as its mark
	// names it. No mark, or one that names none, is an error matching
	// ErrNotFound.
	SetAside(ctx context.Context, runID string, scope uint64) (uint64, error)
	// RetireRows removes the index rows, under rec's status, of rec's ending
	// and every earlier one: the run no longer ended that way. A later
	// ending's rows stay, and every body does.
	RetireRows(ctx context.Context, rec RunRecord) error
}

// ErrResumeMarkTaken reports a resume's mark that stands and does not name the
// ending this resume sets aside. Marking again does not change it.
var ErrResumeMarkTaken = errors.New("the run is already marked as resumed there, setting aside another ending")

// compile-time contract check.
var _ RunEndingStore = (*BlobRunArchive)(nil)

// GetEnding implements RunEndingStore.
func (a *BlobRunArchive) GetEnding(ctx context.Context, runID string, scope uint64) ([]byte, error) {
	key, err := RunEndingKey(runID, scope)
	if err != nil {
		return nil, err
	}
	body, _, err := a.b.get(ctx, key, 0)
	switch {
	case errors.Is(err, errBlobNotFound):
		return nil, fmt.Errorf("artifacts: archive get %q ending %d: %w", runID, scope, ErrNotFound)
	case err != nil:
		return nil, fmt.Errorf("artifacts: archive get %q ending %d: %w", runID, scope, err)
	}
	return body, nil
}

// HasEnding implements RunEndingStore.
func (a *BlobRunArchive) HasEnding(ctx context.Context, runID string, scope uint64) (bool, error) {
	key, err := RunEndingKey(runID, scope)
	if err != nil {
		return false, err
	}
	exists, err := a.exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("artifacts: archive has %q ending %d: %w", runID, scope, err)
	}
	return exists, nil
}

// RunEndings implements RunEndingStore.
func (a *BlobRunArchive) RunEndings(ctx context.Context, runID string) (RunEndings, error) {
	out := RunEndings{RunID: runID}
	segment, err := encodeArchiveSegment(runID)
	if err != nil {
		return out, fmt.Errorf("artifacts: run endings: run id: %w", err)
	}
	err = listObjects(ctx, a.b, blobListInput{Prefix: RunResumeNamespace + "/" + segment + "/"}, func(object blobObject) (bool, error) {
		if _, scope, mark, ok := ParseRunEndingKey(object.Key); ok {
			out.add(scope, mark)
		}
		return false, nil
	})
	if err != nil {
		return RunEndings{RunID: runID}, fmt.Errorf("artifacts: endings of run %q: %w", runID, err)
	}
	return out, nil
}

func (e *RunEndings) add(scope uint64, mark bool) {
	if mark {
		e.Marked = append(e.Marked, scope)
		return
	}
	e.Archived = append(e.Archived, scope)
}

// EachResumedRun implements RunEndingStore. Keys are in order, so a run's are
// together and its scopes ascend.
func (a *BlobRunArchive) EachResumedRun(ctx context.Context, visit func(RunEndings) error) error {
	var run RunEndings
	flush := func() error {
		if run.RunID == "" {
			return nil
		}
		return visit(run)
	}
	var visitErr error
	err := listObjects(ctx, a.b, blobListInput{Prefix: RunResumeNamespace + "/"}, func(object blobObject) (bool, error) {
		runID, scope, mark, ok := ParseRunEndingKey(object.Key)
		if !ok {
			return false, nil
		}
		if runID != run.RunID {
			if visitErr = flush(); visitErr != nil {
				return true, nil
			}
			run = RunEndings{RunID: runID}
		}
		run.add(scope, mark)
		return false, nil
	})
	switch {
	case visitErr != nil:
		return visitErr
	case err != nil:
		return fmt.Errorf("artifacts: list resumed runs: %w", err)
	}
	return flush()
}

// MarkResumed implements RunEndingStore. A mark that names another ending is
// an error: one scope's resumes set one ending aside.
func (a *BlobRunArchive) MarkResumed(ctx context.Context, runID string, scope, setAside uint64) error {
	key, err := ResumeMarkKey(runID, scope)
	if err != nil {
		return err
	}
	err = a.b.create(ctx, key, "text/plain", []byte(strconv.FormatUint(setAside, 10)))
	switch {
	case err == nil:
		return nil
	case !errors.Is(err, errBlobExists):
		return fmt.Errorf("artifacts: mark run %q resumed: %w", runID, err)
	}
	marked, err := a.SetAside(ctx, runID, scope)
	switch {
	case err != nil && !errors.Is(err, ErrNotFound):
		return fmt.Errorf("artifacts: mark run %q resumed: %w", runID, err)
	case err != nil || marked != setAside:
		return fmt.Errorf("artifacts: mark run %q resumed under %d, setting aside %d: %w", runID, scope, setAside, ErrResumeMarkTaken)
	}
	return nil
}

// SetAside implements RunEndingStore.
func (a *BlobRunArchive) SetAside(ctx context.Context, runID string, scope uint64) (uint64, error) {
	key, err := ResumeMarkKey(runID, scope)
	if err != nil {
		return 0, err
	}
	mark, _, err := a.b.get(ctx, key, maxRowBytes+1)
	switch {
	case errors.Is(err, errBlobNotFound):
		return 0, fmt.Errorf("artifacts: the mark of run %q under %d: %w", runID, scope, ErrNotFound)
	case err != nil:
		return 0, fmt.Errorf("artifacts: the mark of run %q under %d: %w", runID, scope, err)
	}
	setAside, ok := parseMark(mark)
	if !ok || setAside >= scope {
		return 0, fmt.Errorf("artifacts: the mark of run %q under %d names no earlier ending: %w", runID, scope, ErrNotFound)
	}
	return setAside, nil
}

// parseMark is the ending a mark names, exactly as MarkResumed writes it.
func parseMark(mark []byte) (uint64, bool) {
	scope, err := strconv.ParseUint(string(mark), 10, 64)
	return scope, err == nil && strconv.FormatUint(scope, 10) == string(mark)
}

// RetireRows implements RunEndingStore. The run-index row goes last: while it
// stands, cleanup and the lists still see the run.
func (a *BlobRunArchive) RetireRows(ctx context.Context, rec RunRecord) error {
	keys, err := runRowKeys(rec)
	if err != nil {
		return err
	}
	for _, key := range keys {
		row, _, err := a.b.get(ctx, key, maxRowBytes+1)
		switch {
		case errors.Is(err, errBlobNotFound):
			continue
		case err != nil:
			return fmt.Errorf("artifacts: retire the rows of run %q: read %q: %w", rec.RunID, key, err)
		}
		scope, err := rowScope(row)
		if err != nil {
			return fmt.Errorf("artifacts: retire the rows of run %q: row %q: %w", rec.RunID, key, err)
		}
		if scope > rec.Scope {
			continue
		}
		if err := a.b.remove(ctx, key); err != nil {
			return fmt.Errorf("artifacts: retire the rows of run %q: remove %q: %w", rec.RunID, key, err)
		}
	}
	return nil
}

// retireSuperseded removes the rows an earlier ending of rec's run left under
// another status; one under the same status is the row rec's own replaced.
func (a *BlobRunArchive) retireSuperseded(ctx context.Context, rec RunRecord) error {
	own := NormalizeRunStatus(rec.Status)
	for _, status := range terminalRunStatuses {
		if status == own {
			continue
		}
		earlier := rec
		earlier.Status, earlier.Scope = status, rec.Scope-1
		if err := a.RetireRows(ctx, earlier); err != nil {
			return err
		}
	}
	return nil
}

// runRowKeys are the index rows of rec, in the order they are written: the
// run index's last, which is what lets cleanup see every other record of a
// run before it sees the run.
func runRowKeys(rec RunRecord) ([]string, error) {
	var keys []string
	for _, render := range []func(RunRecord) (string, bool, error){RunOriginIndexKey, RunRootIndexKey} {
		key, has, err := render(rec)
		if err != nil {
			return nil, err
		}
		if has {
			keys = append(keys, key)
		}
	}
	for _, render := range []func(RunRecord) (string, error){RunRecentIndexKey, RunIndexKey} {
		key, err := render(rec)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// putRows writes rec's index rows: empty for a first ending, its scope for a
// later one.
func (a *BlobRunArchive) putRows(ctx context.Context, rec RunRecord, keys []string) error {
	var row []byte
	if rec.Scope > 0 {
		row = []byte(strconv.FormatUint(rec.Scope, 10))
	}
	for _, key := range keys {
		if err := a.b.put(ctx, key, "", row); err != nil {
			return fmt.Errorf("artifacts: archive run %q: write %s %q: %w", rec.RunID, rowFamily(key), key, err)
		}
	}
	return nil
}

// rowFamily names the index a row key belongs to, for an error to say.
func rowFamily(key string) string {
	for prefix, name := range map[string]string{
		RunOriginIndexNamespace: "origin index", RunRootIndexNamespace: "root index", RunRecentIndexNamespace: "recent index",
	} {
		if strings.HasPrefix(key, prefix+"/") {
			return name
		}
	}
	return "index"
}

// rowScope is the ending a row is of: 0 for an empty one. A row holding
// anything but a later ending's scope is of none that can be told.
func rowScope(row []byte) (uint64, error) {
	if len(row) == 0 {
		return 0, nil
	}
	scope, err := strconv.ParseUint(string(row), 10, 64)
	if err != nil || scope == 0 || len(row) > maxRowBytes {
		return 0, errors.New("it names no ending")
	}
	return scope, nil
}

// withScopes fills the scope of each entry whose row holds one. A row that is
// gone since it was listed leaves its run out, and one that names no ending is
// an error; without drop, the walk of a page, both stay as they are.
func (a *BlobRunArchive) withScopes(ctx context.Context, entries []RunIndexEntry, drop bool) ([]RunIndexEntry, error) {
	var (
		wg      sync.WaitGroup
		slots   = make(chan struct{}, archiveRefWriteConcurrency)
		errs    = make([]error, len(entries))
		dropped = make([]bool, len(entries))
	)
	for i := range entries {
		key := entries[i].rowKey
		if key == "" {
			continue
		}
		entries[i].rowKey = ""
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			body, _, err := a.b.get(ctx, key, maxRowBytes+1)
			switch {
			case errors.Is(err, errBlobNotFound):
				dropped[i] = true
			case err != nil:
				errs[i] = fmt.Errorf("artifacts: archive list: read row %q: %w", key, err)
			default:
				scope, err := rowScope(body)
				if err != nil && drop {
					errs[i] = fmt.Errorf("artifacts: archive list: row %q: %w", key, err)
				}
				entries[i].Scope = scope
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if !drop || !slices.Contains(dropped, true) {
		return entries, nil
	}
	i := -1
	return slices.DeleteFunc(entries, func(RunIndexEntry) bool { i++; return dropped[i] }), nil
}
