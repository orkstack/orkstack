package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// NewestEndings says whether a run's newest ending is archived
// (archive.NewestEndings).
type NewestEndings interface {
	HasRun(ctx context.Context, runID string) (bool, error)
}

// runEndingReader reads each ending's body of a resumed run
// (engine.RunArchiver).
type runEndingReader interface {
	RunEndings(ctx context.Context, runID string) (artifacts.RunEndings, error)
	LoadEnding(ctx context.Context, runID string, scope uint64) ([]byte, error)
}

func newestScope(endings artifacts.RunEndings) uint64 {
	if n := len(endings.Archived); n > 0 {
		return endings.Archived[n-1]
	}
	return 0
}

// newestBody is the body of the run's newest ending, with what is archived of
// its others. An ending a resume set aside never stands for the one after it:
// until that one is archived, the answer is artifacts.ErrNotFound.
func (e *evidenceExport) newestBody(ctx context.Context, runID string, bodies RunBodyReader) (artifacts.RunEndings, []byte, error) {
	if e.s.newestEndings != nil {
		has, err := e.s.newestEndings.HasRun(ctx, runID)
		if err == nil && !has {
			err = artifacts.ErrNotFound
		}
		if err != nil {
			return artifacts.RunEndings{}, nil, err
		}
	}
	var endings artifacts.RunEndings
	if reader, ok := e.s.archive.(runEndingReader); ok {
		var err error
		e.bodies = reader
		if endings, err = reader.RunEndings(ctx, runID); err != nil {
			return endings, nil, err
		}
	}
	if scope := newestScope(endings); scope > 0 {
		body, err := e.bodies.LoadEnding(ctx, runID, scope)
		return endings, body, err
	}
	body, err := bodies.LoadRunBody(ctx, runID)
	return endings, body, err
}

// endings adds the body of each earlier ending of a resumed run, and names
// every ending: an archived one, and a marked one whose body is gone, which the
// newest body's stream holds an event or a gap at. A mark at a number the
// stream holds neither at is of a resume whose reset never ran.
func (e *evidenceExport) endings(
	ctx context.Context, m *evidence.Manifest, archived *orkstackv1.Run, events []*orkstackv1.Event, stored artifacts.RunEndings,
) error {
	newest := newestScope(stored)
	if newest == 0 {
		return nil
	}
	scopes := append([]uint64{0}, stored.Archived[:len(stored.Archived)-1]...)
	for _, scope := range stored.Marked {
		at := func(event *orkstackv1.Event) bool { return uint64(event.GetSequence()) == scope }
		lost := func(g evidence.EventGap) bool { return uint64(g.First) <= scope && scope <= uint64(g.Last) }
		if scope < newest && !slices.Contains(scopes, scope) && (slices.ContainsFunc(events, at) || slices.ContainsFunc(m.EventGaps, lost)) {
			scopes = append(scopes, scope)
		}
	}
	slices.Sort(scopes)
	for _, scope := range scopes {
		member, ending, err := e.earlierEnding(ctx, scope)
		if err != nil {
			return err
		}
		m.Members, m.Endings = append(m.Members, member), append(m.Endings, ending)
	}
	ending := endingOf(archived, newest)
	ending.Path = "run.jsonl"
	m.Endings = append(m.Endings, ending)
	return nil
}

// earlierEnding is the member of one earlier ending. Its body is hashed here
// and read again as it is streamed, so an export holds one body at a time.
func (e *evidenceExport) earlierEnding(ctx context.Context, scope uint64) (evidence.Member, evidence.Ending, error) {
	key, err := artifacts.RunEndingKey(e.runID, scope)
	if err != nil {
		return evidence.Member{}, evidence.Ending{}, connect.NewError(connect.CodeInternal, err)
	}
	path := evidence.EndingPath(scope)
	member := evidence.Member{Path: path, Kind: evidence.KindEnding, Origin: key}
	ending := evidence.Ending{Scope: scope, Path: path}
	body, err := e.bodies.LoadEnding(ctx, e.runID, scope)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		member.State, member.Reason = evidence.StateReclaimed, evidence.ReasonEndingMissing
		return member, ending, nil
	case err != nil:
		return member, ending, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read an earlier ending of run %q: %w", e.runID, err))
	}
	run, err := engine.UnmarshalRunLine(body)
	if err != nil || run.GetId() != e.runID {
		member.State, member.Reason = evidence.StateRejected, evidence.ReasonEndingDamaged
		return member, ending, nil
	}
	sum := sha256.Sum256(body)
	member.State, member.SizeBytes, member.SHA256 = evidence.StatePresent, int64(len(body)), hex.EncodeToString(sum[:])
	e.earlier[path] = scope
	ending = endingOf(run, scope)
	return member, ending, nil
}

func endingOf(run *orkstackv1.Run, scope uint64) evidence.Ending {
	return evidence.Ending{
		Scope: scope, Path: evidence.EndingPath(scope), RunStatus: shortStatus(run.GetStatus()), Reason: run.GetError().GetCode(),
		RunFinishedAt: evidenceTime(run.GetFinishedAt().AsTime(), run.GetFinishedAt() != nil),
	}
}
