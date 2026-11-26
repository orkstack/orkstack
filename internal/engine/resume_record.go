package engine

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A resumed run starts over from R, so what an awaited task, a composed task
// or a sleep ended with is not in its history. The plan carries it: the
// base's row, cut to what the run cannot read back or derive from its
// definition. A composed task's artifacts, passed and received, are claims
// the run's archive body lists, and go with it.

// cutRecordedRow is row, of a task of kind, as a plan records it. Cutting a
// cut row changes nothing.
func cutRecordedRow(kind taskKind, row *orkstackv1.TaskRun) *orkstackv1.TaskRun {
	out := &orkstackv1.TaskRun{
		ScheduledAt: row.GetScheduledAt(), StartedAt: row.GetStartedAt(), FinishedAt: row.GetFinishedAt(),
	}
	switch kind {
	case kindAwait:
		out.ValuesRef = row.GetValuesRef()
		out.Await = &orkstackv1.TaskAwait{
			Deadline: row.GetAwait().GetDeadline(), ResolvedVia: row.GetAwait().GetResolvedVia(),
			ResolvedBy: row.GetAwait().GetResolvedBy(), ResolvedByVerified: row.GetAwait().GetResolvedByVerified(),
		}
	case kindComposed:
		out.ValuesRef, out.Artifacts = row.GetValuesRef(), row.GetArtifacts()
		out.Child = &orkstackv1.TaskChild{RunId: row.GetChild().GetRunId(), InputArtifacts: row.GetChild().GetInputArtifacts()}
	case kindSleep:
		out.Sleep = &orkstackv1.TaskSleep{WakeAt: row.GetSleep().GetWakeAt()}
	}
	return proto.Clone(out).(*orkstackv1.TaskRun)
}

// recordedRowFault is why row is not one cutRecordedRow makes of a task of
// kind that succeeded, or "".
func recordedRowFault(kind taskKind, row *orkstackv1.TaskRun) string {
	switch {
	case row == nil:
		return "it has no row"
	case !proto.Equal(row, cutRecordedRow(kind, row)):
		return "it holds more than a record does"
	}
	times := []*timestamppb.Timestamp{row.GetScheduledAt(), row.GetStartedAt(), row.GetFinishedAt()}
	switch kind {
	case kindAwait:
		if row.GetAwait().GetResolvedVia() == orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_UNSPECIFIED {
			return "it does not say what resolved it"
		}
		if row.GetValuesRef().GetUri() == "" {
			return "it has no values"
		}
		times = append(times, row.GetAwait().GetDeadline())
	case kindComposed:
		if row.GetChild().GetRunId() == "" {
			return "it names no child run"
		}
		if row.GetValuesRef().GetUri() == "" {
			return "it has no values"
		}
	case kindSleep:
		times = append(times, row.GetSleep().GetWakeAt())
	}
	for _, at := range times {
		if at == nil || at.CheckValid() != nil {
			return "a time of it is missing or no time"
		}
	}
	return ""
}

// settledInlineLimit is the most bytes of a settled task's values the
// read-back reads: an await's as its door sends them, and a composed task's
// whole, as the run held them when its child ended.
func settledInlineLimit(kind taskKind) int64 {
	if kind == kindComposed {
		return composedValuesMax
	}
	return adoptInlineValuesLimit
}

// composedValuesMax bounds a composed task's values: its child's result is one
// payload (AGENTS.md §2.2).
const composedValuesMax = 256 << 10

// settledEntryOf is the read-back entry of the settled task t of run runID.
func settledEntryOf(wf *orkstackv1.Orkflow, runID string, t *orkstackv1.Task, values *orkstackv1.ArtifactRef) *orkstackv1.AdoptEntry {
	return &orkstackv1.AdoptEntry{
		TaskId: t.GetId(), Item: -1, Kind: orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_VALUES,
		Values: values, InlineLimit: settledInlineLimit((&planTask{def: t}).kind()),
		Expected: &orkstackv1.ExpectedCommit{Namespace: artifacts.ResolveNamespace(wf.GetNamespace()), RunId: runID, TaskId: t.GetId()},
	}
}

// recordedEntryOf is settledEntryOf for a run whose results the engine records
// (design §28.9): the task's record, at scope and under admission, whose
// values must be values.
func recordedEntryOf(wf *orkstackv1.Orkflow, runID string, t *orkstackv1.Task, values *orkstackv1.ArtifactRef, scope uint64, admission string) *orkstackv1.AdoptEntry {
	e := settledEntryOf(wf, runID, t, values)
	e.Kind, e.RecordScope, e.AdmissionSha256 = orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_COMPOSED, scope, admission
	if (&planTask{def: t}).kind() == kindAwait {
		e.Kind = orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_RESOLUTION
	}
	return e
}
