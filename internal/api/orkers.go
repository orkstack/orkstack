package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The Orker inventory and its one write (docs/DEPLOY.md "Removing an Orker safely: cordon and drain").
//
// Nothing here holds an Orker. The list is the presence channel's poller
// records plus each Orker's own answer on its control channel, read on every
// call; the cordon is a flag delivered to one process and held nowhere else.
// Any replica answers identically, including one that cordoned nothing.

// errNoFleet is the answer when this replica cannot reach a worker at all.
var errNoFleet = errors.New("this engine is not wired to a temporal client, so it cannot list or cordon Orkers")

// ListOrkers is the fleet: one row per live Orker, derived on read. It narrows
// rather than refusing — an Orker that did not answer keeps its row and
// carries unavailable_reason — and an orker_id nothing polls is an empty list.
//
// Filters are compiled first, so a malformed orker_id (the ORKER_ID value
// rule) is INVALID_ARGUMENT before anything is read. An orker_id EQUALS
// filter is a pushdown: only that Orker is asked, on its own channel, which
// is what `orkctl orker get` and the drain loop send. Every filter is then
// applied to the rows read; an unreachable row is kept under pool, build_id
// and cordoned, none of which was measured on it.
func (s *Service) ListOrkers(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListOrkersRequest],
) (*connect.Response[orkstackv1.ListOrkersResponse], error) {
	set, err := compileFilters(req.Msg)
	if err != nil {
		return nil, err
	}
	if s.fleet == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errNoFleet)
	}
	var rows []*orkstackv1.Orker
	if id, ok := set.Equality("orker_id"); ok {
		presence, live, err := s.fleet.ObserveOrker(ctx, id)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, err)
		}
		if live {
			described, err := s.fleet.DescribeOrkerByID(ctx, id)
			row := orkerRow(presence, described, err)
			s.logUnavailableOrker(row, err)
			rows = []*orkstackv1.Orker{row}
		}
	} else {
		if rows, err = s.orkerInventory(ctx); err != nil {
			// Nothing at all was measured: the same verdict ListPools gives
			// when the namespace itself cannot be resolved.
			return nil, connect.NewError(connect.CodeUnavailable, err)
		}
	}
	kept := rows[:0]
	for _, row := range rows {
		if set.Match(row) {
			kept = append(kept, row)
		}
	}
	return connect.NewResponse(&orkstackv1.ListOrkersResponse{Orkers: kept}), nil
}

// orkerInventory is ListOrkers' body for the whole fleet: engine.ReadFleet's
// presence list and one describe per Orker, rows sorted by pool then orker_id. ListOrkers keeps
// its own orker_id pushdown and applies its filters on top of it; ListReleases
// attributes the same rows to releases, so the release view and the Orker list
// are one inventory, not two (docs/DEPLOY.md §3).
func (s *Service) orkerInventory(ctx context.Context) ([]*orkstackv1.Orker, error) {
	if s.fleet == nil {
		return nil, errNoFleet
	}
	orkers, err := engine.ReadFleet(ctx, s.fleet)
	if err != nil {
		return nil, err
	}
	rows := make([]*orkstackv1.Orker, len(orkers))
	for i, orker := range orkers {
		rows[i] = orkerRow(orker.Presence, orker.Described, orker.Err)
		s.logUnavailableOrker(rows[i], orker.Err)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].GetPool() != rows[j].GetPool() {
			return rows[i].GetPool() < rows[j].GetPool()
		}
		return rows[i].GetOrkerId() < rows[j].GetOrkerId()
	})
	return rows, nil
}

func (s *Service) logUnavailableOrker(row *orkstackv1.Orker, err error) {
	if row.GetUnavailableReason() == "" {
		return
	}
	cause := row.GetUnavailableReason()
	if err != nil {
		cause = err.Error()
	}
	s.logger.Warn("list orkers: an orker did not answer on its own channel",
		"orker_id", row.GetOrkerId(), "error", cause)
}

// CordonOrker sets or clears ONE Orker's cordon, delivered over that Orker's
// own channel, and answers with the Orker's state afterwards.
func (s *Service) CordonOrker(
	ctx context.Context,
	req *connect.Request[orkstackv1.CordonOrkerRequest],
) (*connect.Response[orkstackv1.CordonOrkerResponse], error) {
	if s.fleet == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errNoFleet)
	}
	id := req.Msg.GetOrkerId()
	if err := task.ValidOrkerID(id); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if req.Msg.GetCordoned() {
		if err := task.ValidCordonReason(req.Msg.GetReason()); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}
	// Refused BEFORE anything is dispatched, so a typo costs milliseconds and
	// not the delivery timeout.
	presence, live, err := s.fleet.ObserveOrker(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	if !live {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("no Orker %q is polling; `orkctl orker list` shows who is", id))
	}
	in := &orkstackv1.CordonOrkerInput{Cordoned: req.Msg.GetCordoned()}
	if in.Cordoned {
		in.Reason = req.Msg.GetReason()
	}
	described, err := s.fleet.CordonOrker(ctx, id, in)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("the Orker %q is polling but did not answer within %s: %w",
				id, engine.OrkerControlTimeout, err))
	}
	return connect.NewResponse(&orkstackv1.CordonOrkerResponse{Orker: orkerRow(presence, described, nil)}), nil
}

// orkerRow builds one row from the presence record and the Orker's own answer.
// An Orker that could not be asked carries unavailable_reason and nothing
// else measured: "not measured" is never rendered as "measured zero".
func orkerRow(presence engine.OrkerPresence, described *orkstackv1.DescribeOrkerResult, err error) *orkstackv1.Orker {
	row := &orkstackv1.Orker{OrkerId: presence.Identity}
	if !presence.LastSeen.IsZero() {
		row.LastSeen = timestamppb.New(presence.LastSeen)
	}
	if err != nil {
		row.UnavailableReason = strings.TrimSpace(err.Error())
		if row.UnavailableReason == "" {
			row.UnavailableReason = "the Orker did not answer"
		}
		return row
	}
	if described.GetOrkerId() != presence.Identity {
		row.UnavailableReason = fmt.Sprintf("answered as %q on the channel of %q; two Orkers share an id",
			described.GetOrkerId(), presence.Identity)
		return row
	}
	row.Pool = described.GetPool()
	row.BuildId = described.GetBuildId()
	row.RunningTasks = described.GetRunningTasks()
	row.OpenPins = described.GetOpenPins()
	row.MaxConcurrentPins = described.GetMaxConcurrentPins()
	row.Cordoned = described.GetCordoned()
	row.CordonReason = described.GetCordonReason()
	row.CordonedAt = described.GetCordonedAt()
	row.Pins = described.GetPins()
	return row
}
