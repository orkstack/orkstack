package api

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// records is step 8c: for a run whose results the engine records, a member
// for every resolution, composed and outputs record found under the run's
// keys, whatever the run's state says of its task, with what became of it
// (docs/RUN_EVIDENCE.md, "Records"). A record the run's state says should be
// there and the store says is not is a missing member: the gap. One that
// went with its retired run is reclaimed. A record that could not be read
// has no state, and the export fails: it never stands for one that is not
// there. Commits are their tasks' members already.
func (e *evidenceExport) records(ctx context.Context, m *evidence.Manifest, run *orkstackv1.Run, msg *orkstackv1.ExportRunEvidenceRequest) error {
	if !mayHoldRecords(run) {
		return nil
	}
	if e.s.recordedResults == nil {
		if attestedRun(run) {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"run %q's results were recorded by the engine, and this engine keeps no identity records to export", run.GetId()))
		}
		return nil
	}
	r, err := e.s.recordedRunOf(ctx, run)
	if err != nil || r == nil {
		return err
	}
	task, after := msg.GetTaskId(), ""
	for {
		rows, next, err := r.page(ctx, task, after, recordedPageMax)
		if err != nil {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("run %q's records could not be listed: %w", run.GetId(), err))
		}
		for _, c := range rows {
			if c.name.Kind == artifacts.RecordCommitName {
				continue
			}
			members, err := e.record(ctx, r, c)
			if err != nil {
				return err
			}
			m.Members = append(m.Members, members...)
		}
		if next == "" {
			return nil
		}
		if next == after {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("run %q's records did not page on from %q; export it again", run.GetId(), after))
		}
		after = next
	}
}

// record is one record's members: present with its detail for a resolution,
// missing, reclaimed, or none for one that is not there and that nothing
// expects.
func (e *evidenceExport) record(ctx context.Context, r *recordedRun, c recordedCandidate) ([]evidence.Member, error) {
	memberPath := recordMemberPath(c.name)
	kind := recordMemberKind(c.name.Kind)
	origin, err := artifacts.RecordKeyOf(r.ns, r.run.GetId(), c.name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	gone := evidence.Member{Path: memberPath, Kind: kind, TaskID: c.name.TaskID, Origin: origin, ResumeScope: c.name.Scope}
	if r.reclaimed(c) {
		gone.State, gone.Reason = evidence.StateReclaimed, evidence.ReasonRecordReclaimed
		return []evidence.Member{gone}, nil
	}
	// A record names the pool and Orker that made it and where its values
	// lie, which ListRecordedResults shows only to a caller who may read the
	// run's artifacts or the Orkers: anyone else is given its place, and none
	// of its bytes.
	if !r.showHeld {
		gone.State, gone.Reason = evidence.StateWithheld, evidence.ReasonRecordWithheld
		return []evidence.Member{gone}, nil
	}
	if err := r.s.recordedReads.replica.WaitN(ctx, readsOf(c.name)); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("this replica reads all the records it may now; export run %q again", r.run.GetId()))
	}
	read, err := r.s.recordedResults.ReadRecordedValues(ctx, artifacts.SlotList, r.s.awaitSpacings, origin, artifacts.RecordWant{
		Kind: recordedKind(c.name.Kind), Namespace: r.ns, RunID: r.run.GetId(), TaskID: c.name.TaskID, Scope: c.name.Scope, AdmissionSHA256: r.sha})
	if err != nil {
		var un *artifacts.RecordStoreUnanswered
		if errors.As(err, &un) {
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("record %s of run %q could not be read: %w", c.rel, r.run.GetId(), err))
		}
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read record %s of run %q: %w", c.rel, r.run.GetId(), err))
	}
	switch read.Answer {
	case artifacts.RecordFound:
	case artifacts.RecordNone:
		if c.listed {
			// The store listed it and then said it is not there: an answer
			// in flux, never a gap and never nothing.
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"record %s of run %q was listed and then not found; export it again", c.rel, r.run.GetId()))
		}
		gone.State, gone.Reason = evidence.StateMissing, evidence.ReasonRecordMissing
		return []evidence.Member{gone}, nil
	case artifacts.RecordUnsupported:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"record %s of run %q is in a newer format than this engine reads; export it from a newer replica", c.rel, r.run.GetId()))
	default:
		return nil, connect.NewError(connect.CodeDataLoss, fmt.Errorf("record %s of run %q cannot be used: %s", c.rel, r.run.GetId(), read.Reason))
	}
	found := read.Value
	row := &orkstackv1.RecordedResult{}
	r.describe(c, row)
	describeRecorded(found, row)
	member := e.smallMember(memberPath, kind, c.name.TaskID, 0, origin, found.Raw())
	member.ResumeScope, member.RecordState = c.name.Scope, recordMemberState(r.state(c, row))
	members := []evidence.Member{member}
	if detail, key := found.DetailRaw(); detail != nil {
		d := e.smallMember(path.Join(path.Dir(memberPath), "resolution-detail.json"), evidence.KindResolutionDetail, c.name.TaskID, 0, key, detail)
		d.ResumeScope = c.name.Scope
		members = append(members, d)
	}
	return members, nil
}

// recordMemberPath is where a record lies in a bundle: beside its task's
// members, under its resume's scope, and the run's outputs at the top.
func recordMemberPath(n artifacts.RecordName) string {
	if n.Kind == artifacts.RecordOutputsName {
		return "records/outputs.json"
	}
	dir := "tasks/" + n.TaskID + "/records"
	if n.Scope != 0 {
		dir += fmt.Sprintf("/r%d", n.Scope)
	}
	if n.Kind == artifacts.RecordResolution {
		return dir + "/resolution.json"
	}
	return dir + "/composed.json"
}

func recordMemberKind(k artifacts.RecordKind) evidence.Kind {
	switch k {
	case artifacts.RecordResolution:
		return evidence.KindResolution
	case artifacts.RecordComposed:
		return evidence.KindComposed
	}
	return evidence.KindOutputs
}

// recordMemberState is a found record's state as a member carries it.
func recordMemberState(s orkstackv1.RecordedResultState) evidence.RecordState {
	switch s {
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_ON_RECORD:
		return evidence.RecordOnRecord
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_APPLIED:
		return evidence.RecordNotApplied
	}
	return evidence.RecordNotTaken
}

// mayHoldRecords says a run's records are worth asking for: a result of it
// names an engine's record, or it has an awaited or composed task, whose
// records are not their tasks' commits. A run of container tasks alone whose
// results name no record has none, and its export reads no cluster for them.
func mayHoldRecords(run *orkstackv1.Run) bool {
	return attestedRun(run) || slices.ContainsFunc(run.GetTaskRuns(), func(tr *orkstackv1.TaskRun) bool {
		return tr.GetAwait() != nil || tr.GetChild() != nil || tr.GetRecord() != nil
	})
}
