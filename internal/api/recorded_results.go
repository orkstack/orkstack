package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// What the engine recorded for a run, read back for a person
// (docs/TASK_IDENTITY.md, "Reading what is on record"). It decides nothing.

const (
	recordedPageDefault = 25
	recordedPageMax     = 50
	// recordedParallel is how many rows one call reads at a time. A group of
	// them that all go unanswered ends a page's reads: the store is away.
	recordedParallel = 16
	// recordedCallBound is how long one call reads.
	recordedCallBound = 30 * time.Second
	// recordedFromStart is a token's key before every record's.
	recordedFromStart = "!"
	// maxRecordedToken is longer than any record's key under its run.
	maxRecordedToken = 512
	maxRecordedRuns  = 4096
)

// RecordedResults is what ListRecordedResults reads.
type RecordedResults interface {
	ListRecords(ctx context.Context, class artifacts.SlotClass, namespace, runID, taskID, after string, max int) ([]artifacts.RecordName, string, bool, error)
	RecordURI(namespace, runID string, n artifacts.RecordName) (string, error)
	AdmissionOf(ctx context.Context, namespace, runID string) (string, error)
	ResumeChain(ctx context.Context, namespace, runID string) (artifacts.RecordRead[[]artifacts.ResumeRecord], error)
	ReadAttestedCommit(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, want artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error)
	ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error)
}

// recordedRun is a run whose records are listed.
type recordedRun struct {
	s        *Service
	run      *orkstackv1.Run
	ns, sha  string
	resumes  []artifacts.ResumeRecord
	inChild  bool
	left     bool // the cluster no longer holds it
	showHeld bool // pool, Orker and where
}

// recordedCandidate is a record that is there by the listing, or that a
// result the run's state calls succeeded should have.
type recordedCandidate struct {
	name     artifacts.RecordName
	rel      string
	expected bool
	listed   bool
}

// ListRecordedResults lists what the engine recorded for one run.
func (s *Service) ListRecordedResults(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListRecordedResultsRequest],
) (*connect.Response[orkstackv1.ListRecordedResultsResponse], error) {
	msg := req.Msg
	size := int(msg.GetPageSize())
	switch {
	case msg.GetRunId() == "":
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("run_id is required"))
	case size < 0 || size > recordedPageMax:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size is at most %d", recordedPageMax))
	case size == 0:
		size = recordedPageDefault
	}
	after, err := base64.RawURLEncoding.DecodeString(msg.GetPageToken())
	if err != nil || len(after) > maxRecordedToken || !utf8.Valid(after) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid page_token"))
	}
	leave, ok := s.recordedReads.enter(authn.Recorded(ctx, req.Header()).ID)
	if !ok {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("too many listings of recorded results; ask again in a second"))
	}
	defer leave()
	run, err := s.LookupRunIn(ctx, msg.GetRunId(), msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	if msg.GetTaskId() != "" && !hasTask(run, msg.GetTaskId()) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("task %q not found in run %q", msg.GetTaskId(), run.GetId()))
	}
	ctx, cancel := context.WithTimeout(ctx, recordedCallBound)
	defer cancel()
	r, err := s.recordedRunOf(ctx, run)
	if err != nil {
		return nil, err
	}
	resp := &orkstackv1.ListRecordedResultsResponse{ReadComplete: true}
	if r == nil {
		return connect.NewResponse(resp), nil
	}
	var next string
	var rows []recordedCandidate
	away := false
	if msg.GetUnappliedOnly() {
		rows, next = r.unapplied(msg.GetTaskId(), string(after), size)
	} else if rows, next, err = r.page(ctx, msg.GetTaskId(), string(after), size); err != nil {
		var un *artifacts.RecordStoreUnanswered
		if !errors.As(err, &un) {
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("run %q's records could not be listed: %w", run.GetId(), err))
		}
		s.logger.Warn("a run's records could not be listed", "namespace", r.ns, "run", run.GetId(), "error", err)
		away = true
		rows, next = cutRecorded(r.expected(msg.GetTaskId(), string(after), ""), size, "", false)
	}
	results, stoppedAt, unread := r.read(ctx, rows, away, msg.GetUnappliedOnly())
	switch {
	case away || unread:
		resp.ReadComplete, resp.IncompleteReason = false, orkstackv1.RecordedReadStop_RECORDED_READ_STOP_ROWS_UNREAD
	case stoppedAt < len(rows):
		resp.ReadComplete, resp.IncompleteReason = false, orkstackv1.RecordedReadStop_RECORDED_READ_STOP_READ_BUDGET
	}
	if stoppedAt < len(rows) {
		// A page stopped before its first row goes on from where it was asked.
		if next = string(after); stoppedAt > 0 {
			next = rows[stoppedAt-1].rel
		} else if next == "" {
			next = recordedFromStart
		}
	}
	if next != "" {
		resp.NextPageToken = recordedToken(next)
	}
	resp.Results = results
	return connect.NewResponse(resp), nil
}

func recordedToken(after string) string { return base64.RawURLEncoding.EncodeToString([]byte(after)) }

// recordedRunOf is run when the engine records its results; nil when it does
// not.
func (s *Service) recordedRunOf(ctx context.Context, run *orkstackv1.Run) (*recordedRun, error) {
	if s.recordedResults == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine keeps no identity records to list"))
	}
	r := &recordedRun{s: s, run: run, ns: artifacts.ResolveNamespace(run.GetNamespace()),
		inChild: run.GetOrigin().GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN}
	// The listing and the resumes, beside what each row costs.
	if err := s.recordedReads.replica.WaitN(ctx, 2); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("this replica reads all the records it may now; try again"))
	}
	binding, err := s.locate(ctx, run.GetId())
	if err != nil {
		return nil, err
	}
	if binding.Reader == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("no run reader configured"))
	}
	start, err := binding.Reader.RunStart(ctx, run.GetId())
	switch {
	case errors.Is(err, engine.ErrRunNotFound):
		r.left = true
		if r.sha, err = s.recordedResults.AdmissionOf(ctx, r.ns, run.GetId()); err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("run %q's admission could not be found; try again", run.GetId()))
		}
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read the start of run %q: %w", run.GetId(), err))
	case start.Plan.Identity != nil:
		r.sha = start.Plan.Identity.AdmissionSHA256
	}
	if r.sha == "" && !(r.left && attestedRun(run)) {
		return nil, nil
	}
	if run.GetOrigin().GetRootRunId() == "" {
		chain, err := s.recordedResults.ResumeChain(ctx, r.ns, run.GetId())
		if err != nil || chain.Answer != artifacts.RecordFound {
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("run %q's resumes could not be read; try again", run.GetId()))
		}
		r.resumes = chain.Value
	}
	if r.showHeld, err = s.mayOnRun(ctx, run, authz.PermRunArtifactsRead); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("what the caller may read could not be decided; try again"))
	}
	if !r.showHeld {
		if r.showHeld, err = s.mayOnDeployment(ctx, authz.PermOrkerRead); err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("what the caller may read could not be decided; try again"))
		}
	}
	return r, nil
}

// attestedRun says a run whose admission is gone had its results recorded: a
// result of it names an engine's record.
func attestedRun(run *orkstackv1.Run) bool {
	recorded := func(tr *orkstackv1.TaskRun) bool {
		return strings.Contains(tr.GetCommit().GetUri(), "/"+artifacts.AttestedPrefix+"/")
	}
	return slices.ContainsFunc(run.GetTaskRuns(), func(tr *orkstackv1.TaskRun) bool {
		return recorded(tr) || slices.ContainsFunc(tr.GetItems(), recorded)
	})
}

// decides is the scope task's record counts at: the newest resume that ran it
// again or opened its await again, and 0 when none did.
func (r *recordedRun) decides(task string) uint64 {
	for _, res := range r.resumes {
		if slices.Contains(res.Tasks, task) || slices.Contains(res.ReopenedAwaits, task) {
			return res.Scope
		}
	}
	return 0
}

// expected are the records the run's state says there are, by their keys'
// order: after the key after, and up to the key upTo ("" is no bound).
func (r *recordedRun) expected(task, after, upTo string) []recordedCandidate {
	var out []recordedCandidate
	add := func(n artifacts.RecordName) {
		if rel := n.Rel(); rel > after && (upTo == "" || rel <= upTo) {
			out = append(out, recordedCandidate{name: n, rel: rel, expected: true})
		}
	}
	for _, tr := range r.run.GetTaskRuns() {
		if task != "" && tr.GetTaskId() != task {
			continue
		}
		n := artifacts.RecordName{TaskID: tr.GetTaskId(), Scope: r.decides(tr.GetTaskId())}
		if tr.GetEach() != nil {
			for _, item := range tr.GetItems() {
				if item.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED && item.Item != nil {
					n.Item = artifacts.ItemAt(item.GetItem())
					add(n)
				}
			}
			continue
		}
		if tr.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || tr.GetSleep() != nil {
			continue
		}
		switch {
		case tr.GetAwait() != nil:
			n.Kind = artifacts.RecordResolution
		case tr.GetChild() != nil:
			n.Kind = artifacts.RecordComposed
		}
		add(n)
	}
	if task == "" && r.inChild && r.run.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
		add(artifacts.RecordName{Kind: artifacts.RecordOutputsName})
	}
	slices.SortFunc(out, func(a, b recordedCandidate) int { return strings.Compare(a.rel, b.rel) })
	return out
}

// page is one page of the records there and of those expected.
func (r *recordedRun) page(ctx context.Context, task, after string, size int) ([]recordedCandidate, string, error) {
	names, reached, more, err := r.s.recordedResults.ListRecords(ctx, artifacts.SlotList, r.ns, r.run.GetId(), task, after, size)
	if err != nil {
		return nil, "", err
	}
	upTo := ""
	if more {
		upTo = reached
	}
	all := r.expected(task, after, upTo)
	at := make(map[string]int, len(all))
	for i, c := range all {
		at[c.rel] = i
	}
	for _, n := range names {
		rel := n.Rel()
		if i, ok := at[rel]; ok {
			all[i].listed = true
			continue
		}
		all = append(all, recordedCandidate{name: n, rel: rel, listed: true})
	}
	slices.SortFunc(all, func(a, b recordedCandidate) int { return strings.Compare(a.rel, b.rel) })
	rows, next := cutRecorded(all, size, reached, more)
	return rows, next, nil
}

// cutRecorded is the first size of all, and the key the next page goes on
// from: the last one's when some are left, or reached when the store has more.
func cutRecorded(all []recordedCandidate, size int, reached string, more bool) ([]recordedCandidate, string) {
	if len(all) > size {
		return all[:size], all[size-1].rel
	}
	if more {
		return all, reached
	}
	return all, ""
}

// unapplied are the resolutions a page asks for of tasks that await, or that
// ended without theirs.
func (r *recordedRun) unapplied(task, after string, size int) ([]recordedCandidate, string) {
	var all []recordedCandidate
	for _, tr := range r.run.GetTaskRuns() {
		if tr.GetAwait().GetDeadline() == nil || (task != "" && tr.GetTaskId() != task) || resolutionApplied(tr) {
			continue
		}
		n := artifacts.RecordName{Kind: artifacts.RecordResolution, TaskID: tr.GetTaskId(), Scope: r.decides(tr.GetTaskId())}
		if rel := n.Rel(); rel > after {
			all = append(all, recordedCandidate{name: n, rel: rel})
		}
	}
	slices.SortFunc(all, func(a, b recordedCandidate) int { return strings.Compare(a.rel, b.rel) })
	return cutRecorded(all, size, "", false)
}

// resolutionApplied says an awaited task ended by a resolution.
func resolutionApplied(tr *orkstackv1.TaskRun) bool {
	return tr.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED || tr.GetError().GetCode() == engine.AwaitRejectedCode
}

// read is the rows of a page, how many candidates it took before the run's
// reads of this minute were spent, and whether a row could not be read. away
// asks for none of them.
func (r *recordedRun) read(ctx context.Context, rows []recordedCandidate, away, foundOnly bool) (out []*orkstackv1.RecordedResult, took int, unread bool) {
	for from := 0; from < len(rows); from += recordedParallel {
		away = away || ctx.Err() != nil
		group := rows[from:min(from+recordedParallel, len(rows))]
		spent := len(group)
		kept := make([]*orkstackv1.RecordedResult, len(group))
		for i, c := range group {
			if row, ok := r.s.recordedReads.kept(r.cacheKey(c)); ok {
				kept[i] = row
				continue
			}
			if !away && !r.reclaimed(c) && !r.s.recordedReads.spend(r.ns+"/"+r.run.GetId(), readsOf(c.name)) {
				spent = i
				break
			}
		}
		got := make([]*orkstackv1.RecordedResult, spent)
		var wg sync.WaitGroup
		for i, c := range group[:spent] {
			wg.Go(func() { got[i] = r.row(ctx, c, kept[i], away, foundOnly) })
		}
		wg.Wait()
		failed := 0
		for _, row := range got {
			if row == nil {
				continue
			}
			if row.GetState() == orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNREAD {
				failed++
			}
			out = append(out, row)
		}
		unread = unread || failed > 0
		if spent < len(group) {
			return out, from + spent, unread
		}
		away = away || failed == recordedParallel
	}
	return out, len(rows), unread
}

// readsOf is how many objects a record's read takes at most.
func readsOf(n artifacts.RecordName) int {
	switch n.Kind {
	case artifacts.RecordResolution:
		return 3 // the record, its detail and small values
	case artifacts.RecordComposed, artifacts.RecordOutputsName:
		return 2
	}
	return 1
}

func (r *recordedRun) cacheKey(c recordedCandidate) string {
	return r.ns + "/" + r.run.GetId() + "/" + r.sha + "/" + c.rel
}

// reclaimed says a record that is expected and not listed went with its run.
func (r *recordedRun) reclaimed(c recordedCandidate) bool { return r.left && c.expected && !c.listed }

// row is one candidate's row; nil for one that is not there and that nothing
// expects.
func (r *recordedRun) row(ctx context.Context, c recordedCandidate, kept *orkstackv1.RecordedResult, away, foundOnly bool) *orkstackv1.RecordedResult {
	row := kept
	state := orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNSPECIFIED
	if row == nil {
		row = &orkstackv1.RecordedResult{}
		switch {
		case away:
			state = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNREAD
		case r.reclaimed(c):
			state = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_RECLAIMED
		default:
			state = r.ask(ctx, c, row)
		}
	}
	if state == orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_ON_RECORD && (foundOnly || !c.expected) {
		return nil
	}
	r.describe(c, row)
	if state == orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNSPECIFIED {
		state = r.state(c, row)
		if uri, err := r.s.recordedResults.RecordURI(r.ns, r.run.GetId(), c.name); err == nil {
			row.Uri = uri
		}
	}
	row.State = state
	if !r.showHeld {
		row.Pool, row.OrkerId, row.Uri = "", "", ""
	}
	return row
}

// describe sets what a row says of its task by the run's state.
func (r *recordedRun) describe(c recordedCandidate, row *orkstackv1.RecordedResult) {
	row.TaskId, row.ResumeScope, row.InChildRun = c.name.TaskID, c.name.Scope, r.inChild
	switch c.name.Kind {
	case artifacts.RecordCommitName:
		row.Kind = orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_COMMIT
	case artifacts.RecordResolution:
		row.Kind = orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_RESOLUTION
	case artifacts.RecordComposed:
		row.Kind = orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_COMPOSED
	case artifacts.RecordOutputsName:
		row.Kind = orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_OUTPUTS
	}
	if c.name.Kind == artifacts.RecordOutputsName {
		row.TaskStatus = r.run.GetStatus()
		return
	}
	tr := r.task(c.name)
	row.TaskStatus, row.Deadline = tr.GetStatus(), tr.GetAwait().GetDeadline()
	if c.name.Item.Set {
		row.Item = &c.name.Item.Index
	}
}

// task is the state of the task, or of the item, a record is of.
func (r *recordedRun) task(n artifacts.RecordName) *orkstackv1.TaskRun {
	for _, tr := range r.run.GetTaskRuns() {
		if tr.GetTaskId() != n.TaskID {
			continue
		}
		if !n.Item.Set {
			return tr
		}
		for _, item := range tr.GetItems() {
			if item.Item != nil && item.GetItem() == n.Item.Index {
				return item
			}
		}
	}
	return nil
}

// ask reads a candidate's record into row: UNSPECIFIED for one found.
func (r *recordedRun) ask(ctx context.Context, c recordedCandidate, row *orkstackv1.RecordedResult) orkstackv1.RecordedResultState {
	const (
		found    = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNSPECIFIED
		unread   = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNREAD
		notThere = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_ON_RECORD
		newer    = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_RECORDED_NEWER_FORMAT
	)
	warn := func(err error) orkstackv1.RecordedResultState {
		r.s.logger.Warn("a recorded result could not be read", "namespace", r.ns, "run", r.run.GetId(), "record", c.rel, "error", err)
		return unread
	}
	if err := r.s.recordedReads.replica.WaitN(ctx, readsOf(c.name)); err != nil {
		return warn(err)
	}
	var answer artifacts.RecordAnswer
	var why string
	if c.name.Kind == artifacts.RecordCommitName {
		read, err := r.s.recordedResults.ReadAttestedCommit(ctx, artifacts.SlotList, r.s.awaitSpacings, artifacts.AttestedWant{Namespace: r.ns,
			RunID: r.run.GetId(), TaskID: c.name.TaskID, Item: c.name.Item, Scope: c.name.Scope, AdmissionSHA256: r.sha})
		if err != nil {
			return warn(err)
		}
		answer, why = read.Answer, read.Reason
		if answer == artifacts.RecordFound {
			rec := read.Value.Commit.GetRecorded()
			row.AskedAt, row.ValuesSha256 = rec.GetTime(), read.Value.Commit.GetValues().GetSha256()
			row.Pool, row.OrkerId = rec.GetPool(), rec.GetOrkerId()
			if !read.Value.StoredAt.IsZero() {
				row.StoredAt = timestamppb.New(read.Value.StoredAt)
			}
		}
	} else {
		key, err := artifacts.RecordKeyOf(r.ns, r.run.GetId(), c.name)
		if err != nil {
			return warn(err)
		}
		read, err := r.s.recordedResults.ReadRecordedValues(ctx, artifacts.SlotList, r.s.awaitSpacings, key, artifacts.RecordWant{
			Kind: recordedKind(c.name.Kind), Namespace: r.ns, RunID: r.run.GetId(), TaskID: c.name.TaskID, Scope: c.name.Scope, AdmissionSHA256: r.sha})
		if err != nil {
			return warn(err)
		}
		answer, why = read.Answer, read.Reason
		if answer == artifacts.RecordFound {
			describeRecorded(read.Value, row)
		}
	}
	switch answer {
	case artifacts.RecordFound:
		r.s.recordedReads.keep(r.cacheKey(c), row)
		return found
	case artifacts.RecordNone:
		return notThere
	case artifacts.RecordUnsupported:
		return newer
	}
	r.s.logger.Error("a recorded result cannot be used", "namespace", r.ns, "run", r.run.GetId(), "record", c.rel, "reason", why)
	return unread
}

func recordedKind(k artifacts.RecordKind) orkstackv1.RecordedValuesKind {
	switch k {
	case artifacts.RecordResolution:
		return orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION
	case artifacts.RecordComposed:
		return orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED
	}
	return orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS
}

func describeRecorded(f *artifacts.RecordedFound, row *orkstackv1.RecordedResult) {
	row.Rejected, row.ValuesSha256, _ = f.Standing()
	if at := f.AskedAt(); !at.IsZero() {
		row.AskedAt = timestamppb.New(at)
	}
	if !f.StoredAt().IsZero() {
		row.StoredAt = timestamppb.New(f.StoredAt())
	}
	row.ChildRunId, _, _ = f.Child()
	if d := f.Detail(); d != nil {
		row.Via, row.Principal, row.PrincipalVerified = d.GetVia(), d.GetPrincipal(), d.GetPrincipalVerified()
	}
}

// state is what became of a record found, by the run's state.
func (r *recordedRun) state(c recordedCandidate, row *orkstackv1.RecordedResult) orkstackv1.RecordedResultState {
	const (
		onRecord   = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_ON_RECORD
		notApplied = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_APPLIED
		notTaken   = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_TAKEN
	)
	succeeded := row.GetTaskStatus() == orkstackv1.Status_STATUS_SUCCEEDED
	if c.name.Kind == artifacts.RecordOutputsName {
		if succeeded {
			return onRecord
		}
		return notApplied
	}
	if c.name.Scope != r.decides(c.name.TaskID) {
		return notTaken
	}
	tr := r.task(c.name)
	if c.name.Kind != artifacts.RecordResolution {
		// The run took this result, and not another.
		if took := tr.GetValuesRef().GetSha256(); succeeded && (took == "" || took == row.GetValuesSha256()) &&
			(c.name.Kind != artifacts.RecordComposed || tr.GetChild().GetRunId() == row.GetChildRunId()) {
			return onRecord
		}
		return notApplied
	}
	// The run took this answer, and not another: the same decision, and for a
	// completion the same values.
	if resolutionApplied(tr) && row.GetRejected() != succeeded && (row.GetRejected() || tr.GetValuesRef().GetSha256() == row.GetValuesSha256()) {
		return onRecord
	}
	var ended time.Time
	if at := r.run.GetFinishedAt(); at != nil {
		ended = at.AsTime()
	}
	if at := row.GetDeadline(); at != nil && row.GetStoredAt() != nil && artifacts.RecordedInTime(row.GetStoredAt().AsTime(), at.AsTime(), ended) {
		return notApplied
	}
	return notTaken
}
