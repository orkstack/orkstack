package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ArtifactReclaimer is the artifact GC as the API needs it
// (archive.Reclaimer).
type ArtifactReclaimer interface {
	Preview(ctx context.Context, req archive.PreviewRequest) (archive.PreviewResult, error)
}

// AcceptedRunNoter records a run this replica started (archive.AcceptedRunTracker).
type AcceptedRunNoter interface {
	NoteAccepted(runID string, at time.Time)
}

// PreviewArtifactReclaim answers what the artifact GC would do now, for at
// most 1000 runs, writing nothing (docs/STORAGE.md, "Reclaiming space").
func (s *Service) PreviewArtifactReclaim(
	ctx context.Context,
	req *connect.Request[orkstackv1.PreviewArtifactReclaimRequest],
) (*connect.Response[orkstackv1.PreviewArtifactReclaimResponse], error) {
	msg := req.Msg
	if msg.GetLimit() < 0 || msg.GetLimit() > archive.PreviewMaxRuns {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("limit must be 0 to %d", archive.PreviewMaxRuns))
	}
	if msg.GetSamples() < 0 || msg.GetSamples() > archive.PreviewMaxSamples {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("samples must be 0 to %d", archive.PreviewMaxSamples))
	}
	if c := msg.GetCursor(); c != "" && (len(c) > 1024 || !strings.HasPrefix(c, artifacts.RunIndexNamespace+"/")) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cursor is not one a preview returned"))
	}
	if msg.GetSamples() > 0 && msg.GetNamespace() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("samples need a namespace"))
	}
	if s.artifactReclaim == nil {
		return connect.NewResponse(&orkstackv1.PreviewArtifactReclaimResponse{
			Mode: string(archive.ReclaimOff), CleanupEnabled: s.retentionEnforced, Held: true, HeldReason: "off",
			ComputedAt: timestamppb.Now(),
		}), nil
	}
	result, err := s.artifactReclaim.Preview(ctx, archive.PreviewRequest{
		Namespace: msg.GetNamespace(), Limit: int(msg.GetLimit()), Samples: int(msg.GetSamples()), Cursor: msg.GetCursor(),
	})
	switch {
	case errors.Is(err, archive.ErrPreviewBusy):
		return nil, connect.NewError(connect.CodeResourceExhausted, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("preview artifact reclaim: %w", err))
	}
	return connect.NewResponse(reclaimPreviewResponse(result, s.retentionEnforced)), nil
}

func reclaimPreviewResponse(result archive.PreviewResult, cleanupEnabled bool) *orkstackv1.PreviewArtifactReclaimResponse {
	res := &orkstackv1.PreviewArtifactReclaimResponse{
		Mode: string(result.Mode), Held: len(result.Held) > 0, HeldReason: strings.Join(result.Held, ","),
		RunsExamined: int64(result.RunsExamined), NextCursor: result.NextCursor, ComputedAt: timestamppb.New(result.ComputedAt),
		CleanupEnabled: cleanupEnabled, StoreVersioned: result.StoreVersioned,
		LowWaterKnown: result.LowWater.Known, LowWaterUnknownCause: result.LowWater.Cause,
		LowWaterRunId: result.LowWater.RunID, WaitingRuns: int64(result.WaitingRuns),
	}
	if !result.LowWater.At.IsZero() {
		res.LowWaterCreatedAt = timestamppb.New(result.LowWater.At)
	}
	if !result.LowWater.Computed.IsZero() {
		res.LowWaterComputedAt = timestamppb.New(result.LowWater.Computed)
	}
	byNamespace := map[string]*orkstackv1.ReclaimNamespaceSummary{}
	kept := map[string]map[string]*orkstackv1.ReclaimReasonCount{}
	for key, tally := range result.Objects {
		ns := byNamespace[key.Namespace]
		if ns == nil {
			ns = &orkstackv1.ReclaimNamespaceSummary{Namespace: key.Namespace}
			byNamespace[key.Namespace] = ns
			kept[key.Namespace] = map[string]*orkstackv1.ReclaimReasonCount{}
		}
		switch key.Outcome {
		case archive.OutcomeWouldDelete:
			ns.WouldDeleteObjects += tally.Objects
			ns.WouldDeleteBytes += tally.Bytes
		case archive.OutcomeKept, archive.OutcomeFailed:
			c := kept[key.Namespace][key.Reason]
			if c == nil {
				c = &orkstackv1.ReclaimReasonCount{Reason: key.Reason}
				kept[key.Namespace][key.Reason] = c
			}
			c.Objects += tally.Objects
			c.Bytes += tally.Bytes
		}
	}
	for name, ns := range byNamespace {
		for _, c := range kept[name] {
			ns.Kept = append(ns.Kept, c)
		}
		sort.Slice(ns.Kept, func(i, j int) bool { return ns.Kept[i].GetReason() < ns.Kept[j].GetReason() })
		res.Namespaces = append(res.Namespaces, ns)
	}
	sort.Slice(res.Namespaces, func(i, j int) bool { return res.Namespaces[i].GetNamespace() < res.Namespaces[j].GetNamespace() })
	for _, sample := range result.Samples {
		out := &orkstackv1.ReclaimSample{RunId: sample.RunID, Bytes: sample.Bytes, Outcome: sample.Outcome, Reason: sample.Reason}
		if task, name, ok := artifacts.ObjectTaskAndName(sample.ObjectID); ok {
			out.TaskId, out.ArtifactName = task, name
		}
		if !sample.WaitsFor.IsZero() {
			out.WaitsForRunsCreatedBefore = timestamppb.New(sample.WaitsFor)
		}
		res.Samples = append(res.Samples, out)
	}
	return res
}
