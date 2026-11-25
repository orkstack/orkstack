package api

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// concurrencyPageMax bounds a ListConcurrencyLimits page: each key on it is
// one state query of its limit.
const concurrencyPageMax = 50

// ListConcurrencyLimits lists a namespace's keys in use (E88 §9.2), each
// read from its limit at the moment of the call.
func (s *Service) ListConcurrencyLimits(ctx context.Context, req *connect.Request[orkstackv1.ListConcurrencyLimitsRequest]) (*connect.Response[orkstackv1.ListConcurrencyLimitsResponse], error) {
	msg := req.Msg
	set, err := compileFilters(msg)
	if err != nil {
		return nil, err
	}
	if msg.GetPageSize() < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size is %d; it must be 0 to %d", msg.GetPageSize(), concurrencyPageMax))
	}
	// One key=VALUE is that key's own limit, read directly; every other
	// filter is decided on each listed row.
	if key, ok := set.Equality("key"); ok {
		limit, err := s.concurrencyLimit(ctx, msg.GetNamespace(), key)
		switch {
		case connect.CodeOf(err) == connect.CodeNotFound:
			return connect.NewResponse(&orkstackv1.ListConcurrencyLimitsResponse{}), nil
		case err != nil:
			return nil, err
		}
		out := &orkstackv1.ListConcurrencyLimitsResponse{}
		if set.Match(limit) {
			out.Limits = []*orkstackv1.ConcurrencyLimit{limit}
		}
		return connect.NewResponse(out), nil
	}
	ns, err := s.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	reader, err := s.limitsReader()
	if err != nil {
		return nil, err
	}
	token, err := decodeConcurrencyCursor(msg.GetPageToken(), set)
	if err != nil {
		return nil, err
	}
	size := int(msg.GetPageSize())
	if size == 0 || size > concurrencyPageMax {
		size = concurrencyPageMax
	}
	views, next, err := reader.Gates(ctx, ns.GetName(), size, token)
	var invalid *serviceerror.InvalidArgument
	switch {
	case errors.As(err, &invalid):
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page_token is not one this service issued for this list"))
	case err != nil:
		s.logger.Warn("list concurrency limits: the cluster could not be read", "namespace", ns.GetName(), "error", err)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the namespace's concurrency limits could not be read; try again"))
	}
	incarnation, err := concurrencyIncarnation(ns)
	if err != nil {
		return nil, err
	}
	nextToken, err := encodeConcurrencyCursor(set, next)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode page_token: %w", err))
	}
	out := &orkstackv1.ListConcurrencyLimitsResponse{NextPageToken: nextToken}
	for _, view := range views {
		if view.Cause != nil {
			s.logger.Warn("list concurrency limits: a limit did not answer", "namespace", ns.GetName(), "limit", view.GateID, "error", view.Cause)
		}
		// A limit of an earlier namespace of this name drains; it is not
		// this namespace's.
		if view.Answering && view.Key != "" && engine.GateID(ns.GetName(), incarnation, view.Key) != view.GateID {
			continue
		}
		// A limit that does not answer and names no key is listed: its key
		// is unknown, so no filter on it can exclude it.
		limit := s.concurrencyLimitProto(view)
		if view.Key != "" && !set.Match(limit) {
			continue
		}
		out.Limits = append(out.Limits, limit)
	}
	return connect.NewResponse(out), nil
}

// GetConcurrencyLimit reads one key's limit.
func (s *Service) GetConcurrencyLimit(ctx context.Context, req *connect.Request[orkstackv1.GetConcurrencyLimitRequest]) (*connect.Response[orkstackv1.GetConcurrencyLimitResponse], error) {
	limit, err := s.concurrencyLimit(ctx, req.Msg.GetNamespace(), req.Msg.GetKey())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.GetConcurrencyLimitResponse{Limit: limit}), nil
}

// concurrencyLimit reads the limit of key in the namespace's incarnation.
func (s *Service) concurrencyLimit(ctx context.Context, namespace, key string) (*orkstackv1.ConcurrencyLimit, error) {
	if err := dsl.CheckStoredConcurrencyKey(key); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("key: %w", err))
	}
	ns, err := s.namespace(ctx, namespace)
	if err != nil {
		return nil, err
	}
	reader, err := s.limitsReader()
	if err != nil {
		return nil, err
	}
	incarnation, err := concurrencyIncarnation(ns)
	if err != nil {
		return nil, err
	}
	view, present, err := reader.Gate(ctx, engine.GateID(ns.GetName(), incarnation, key))
	switch {
	case ctx.Err() != nil:
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the request ended before the limit answered"))
	case err != nil:
		s.logger.Warn("get concurrency limit: the limit did not answer", "namespace", ns.GetName(), "key", key, "error", err)
		view = engine.GateView{Namespace: ns.GetName(), Key: key, NotAnswering: engine.GateNotAnswering}
	case !present:
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf(
			"no run holds or waits for concurrency key %s in namespace %q", key, ns.GetName()))
	}
	if view.Key == "" {
		view.Key = key
	}
	return s.concurrencyLimitProto(view), nil
}

// limitsReader is the cluster reader, or why there is none.
func (s *Service) limitsReader() (engine.ConcurrencyAdmissionReader, error) {
	if s.concurrencyReader == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("this engine is not wired to a cluster, so its concurrency limits cannot be read"))
	}
	return s.concurrencyReader, nil
}

// concurrencyLimitProto maps a gate's view, holders and line cut to
// api_holders_shown.
func (s *Service) concurrencyLimitProto(view engine.GateView) *orkstackv1.ConcurrencyLimit {
	out := &orkstackv1.ConcurrencyLimit{Namespace: view.Namespace, Key: view.Key, Answering: view.Answering}
	if !view.Answering {
		out.NotAnsweringReason = view.NotAnswering
		return out
	}
	shown := s.concurrency.APIHoldersShown
	out.Epoch = view.Epoch
	out.HoldersCount, out.QueuedCount = int32(len(view.Holders)), int32(len(view.Line))
	for i, h := range view.Holders {
		if i >= shown {
			break
		}
		out.Holders = append(out.Holders, &orkstackv1.ConcurrencySlotHolder{RunId: h.Run, TaskLevel: h.TaskLevel,
			GrantedAt: timestampOrNil(h.GrantedAt), Stalled: h.Stalled, Strategy: strategyProto(h.Strategy), Limit: int32(h.Limit)})
	}
	for i, e := range view.Line {
		if i >= shown {
			break
		}
		out.Line = append(out.Line, &orkstackv1.ConcurrencyLineEntry{RunId: e.Run, TaskLevel: e.TaskLevel,
			ArrivedAt: timestampOrNil(e.ArrivedAt), TimesOutAt: timestampOrNil(e.TimesOutAt), Strategy: strategyProto(e.Strategy), Limit: int32(e.Limit)})
	}
	switch view.Rebuild {
	case engine.GateRebuildNone:
		out.Rebuild = orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_NONE
	case engine.GateRebuildReading:
		out.Rebuild = orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_READING
	case engine.GateRebuildSettling:
		out.Rebuild = orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_SETTLING
	case engine.GateRebuildFailing:
		out.Rebuild = orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_FAILING
		out.RebuildFailingSince = timestampOrNil(view.RebuildFailingSince)
	}
	return out
}

func strategyProto(s string) orkstackv1.ConcurrencyStrategy {
	switch s {
	case engine.ConcurrencyStrategyQueue:
		return orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE
	case engine.ConcurrencyStrategyRefuse:
		return orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REFUSE
	case engine.ConcurrencyStrategySkip:
		return orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP
	}
	return orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_UNSPECIFIED
}

// concurrencyCursor is a ListConcurrencyLimits page token: the cluster's own
// position in the namespace's limits, bound to the canonical filter set it
// was issued under.
type concurrencyCursor struct {
	Version int    `json:"v"`
	Filters string `json:"f"`
	Next    []byte `json:"n"`
}

// encodeConcurrencyCursor is empty when the cluster has no next page.
func encodeConcurrencyCursor(set listfilter.Set, next []byte) (string, error) {
	if len(next) == 0 {
		return "", nil
	}
	raw, err := stablejson.Marshal(concurrencyCursor{Version: 1, Filters: set.Key(), Next: next})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// decodeConcurrencyCursor returns the cluster's position; an empty token is
// the first page.
func decodeConcurrencyCursor(token string, set listfilter.Set) ([]byte, error) {
	if token == "" {
		return nil, nil
	}
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("page_token is not one this service issued for this list"))
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, invalid
	}
	var c concurrencyCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != 1 || len(c.Next) == 0 {
		return nil, invalid
	}
	if err := checkFilterKey(c.Filters, set); err != nil {
		return nil, err
	}
	return c.Next, nil
}
