package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RestoreReconciler is a restore reconcile as the API needs it: one page of
// archive.ReconcileRestore over this replica's run archive and cluster.
type RestoreReconciler interface {
	ReconcileRestore(ctx context.Context, req archive.RestoreRequest) (archive.RestoreResult, error)
}

// restoreClockSlack is how far in the future a restore instant may be: an
// operator's clock may run ahead of this engine's.
const restoreClockSlack = 5 * time.Minute

// ReconcileRestore reconciles the cluster with the bucket after Temporal was
// restored (docs/RUNBOOKS.md, "Backup and disaster recovery").
func (s *Service) ReconcileRestore(
	ctx context.Context,
	req *connect.Request[orkstackv1.ReconcileRestoreRequest],
) (*connect.Response[orkstackv1.ReconcileRestoreResponse], error) {
	msg := req.Msg
	if msg.GetTemporalRestoredAt() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("temporal_restored_at is required: the instant Temporal was restored to"))
	}
	if err := msg.GetTemporalRestoredAt().CheckValid(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("temporal_restored_at: %w", err))
	}
	restoredAt := msg.GetTemporalRestoredAt().AsTime()
	if restoredAt.After(time.Now().Add(restoreClockSlack)) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("temporal_restored_at %s is in the future", restoredAt.UTC().Format(time.RFC3339)))
	}
	limit := int(msg.GetLimit())
	if limit < 0 || limit > archive.MaxRestorePage {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("limit must be 0 to %d", archive.MaxRestorePage))
	}
	if limit == 0 {
		limit = archive.MaxRestorePage
	}
	if c := msg.GetCursor(); c != "" && (len(c) > 1024 || !strings.HasPrefix(c, artifacts.RunCleanupNamespace+"/")) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cursor is not one a reconcile returned"))
	}
	if s.restoreReconciler == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine has no run archive and cluster to reconcile"))
	}
	if !msg.GetDryRun() && s.retentionEnforced {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("retention cleanup is on for this engine and would race the reconcile: turn it off (ORK_CLEANUP_ENABLED=false) on every engine first, or pass dry_run"))
	}
	result, err := s.restoreReconciler.ReconcileRestore(ctx, archive.RestoreRequest{
		RestoredAt: restoredAt, DryRun: msg.GetDryRun(), After: msg.GetCursor(), Limit: limit,
	})
	if err != nil {
		var deleted []string
		for _, r := range result.Retired {
			if r.Deleted {
				deleted = append(deleted, r.RunID)
			}
		}
		if len(deleted) > 0 {
			noteAuditAlways(ctx, fmt.Sprintf("deleted_before_failure=%d runs=%s", len(deleted), strings.Join(deleted, ",")))
		}
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("reconcile restore: %w", err))
	}
	return connect.NewResponse(restoreResponse(result)), nil
}

func restoreResponse(result archive.RestoreResult) *orkstackv1.ReconcileRestoreResponse {
	res := &orkstackv1.ReconcileRestoreResponse{
		FinishedRunIds: result.Finished, Listed: int32(result.Listed), NextCursor: result.Next,
		FinishedTruncated: result.FinishedTruncated,
	}
	for _, r := range result.Retired {
		res.Retired = append(res.Retired, &orkstackv1.RestoreRetiredRun{
			RunId: r.RunID, RetiredAt: timestamppb.New(r.RetiredAt), Archived: r.Archived, Deleted: r.Deleted, Open: r.Open,
		})
	}
	return res
}
