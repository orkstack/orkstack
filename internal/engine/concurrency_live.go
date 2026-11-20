package engine

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The note codes of ConcurrencyQueued that GetRun's live read sets
// (concurrency design §9.1), in precedence: the first that applies is the
// one shown.
const (
	QueuedNoteNotAnswering   = "NOT_ANSWERING"
	QueuedNoteRebuildFailing = "REBUILD_FAILING"
	QueuedNoteRebuilding     = "REBUILDING"
	QueuedNoteHolderStalled  = "HOLDER_STALLED"
)

// LiveQueued is a queued run's place in line as its gate holds it now: a
// copy of the run's own record (snapshot) with its position, the holders and
// the note read from line. A run the line no longer holds (granted a moment
// ago, or not yet back after a rebuild) has no known position. shown bounds
// the holders named (engine.concurrency.api_holders_shown).
func LiveQueued(snapshot *orkstackv1.ConcurrencyQueued, runID string, line GateLine, shown int, now time.Time) *orkstackv1.ConcurrencyQueued {
	q := proto.Clone(snapshot).(*orkstackv1.ConcurrencyQueued)
	q.Position, q.PositionKnown = 0, false
	for i, e := range line.Line {
		if e.RunLevel && e.Run == runID {
			q.Position, q.PositionKnown = int32(i+1), true
			break
		}
	}
	q.HoldersAhead = append([]string(nil), line.HolderRuns[:min(shown, len(line.HolderRuns))]...)
	q.HoldersCount = int32(len(line.HolderRuns))
	q.HoldersAsOfArrival = false
	q.NoteCode, q.Note = "", ""
	switch {
	case !line.RebuildFailingSince.IsZero():
		q.NoteCode = QueuedNoteRebuildFailing
		q.Note = fmt.Sprintf("the limit has failed to rebuild for %s; it keeps trying",
			now.Sub(line.RebuildFailingSince).Round(time.Second))
	case line.Rebuilding:
		q.NoteCode, q.Note = QueuedNoteRebuilding, rebuildingNote(q.GetKey())
	case len(line.StalledRuns) > 0:
		q.NoteCode = QueuedNoteHolderStalled
		q.Note = fmt.Sprintf("waiting behind run %s, which is not making progress; an operator must end it", line.StalledRuns[0])
	}
	return q
}

// rebuildingNote is a queued run's note while its limit reads which runs
// hold its key: a gate that starts with no record of a previous one (a
// first run at a key, or a record lost) cannot tell the two apart.
func rebuildingNote(key string) string {
	return fmt.Sprintf("waiting while the limit on key %s reads which runs hold it (a first run at a key, or after its record was lost)", key)
}

// NotAnsweringQueued is a queued run's record when its gate could not be
// read: the place unknown, the holders as the run found them, and when the
// run asks the gate again at the latest.
func NotAnsweringQueued(snapshot *orkstackv1.ConcurrencyQueued, askAgainBy time.Time) *orkstackv1.ConcurrencyQueued {
	q := proto.Clone(snapshot).(*orkstackv1.ConcurrencyQueued)
	q.Position, q.PositionKnown = 0, false
	q.NoteCode = QueuedNoteNotAnswering
	q.Note = "the limit is not answering; this run stays queued and asks again by " + askAgainBy.UTC().Format(time.RFC3339)
	return q
}
