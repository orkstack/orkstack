package engine

import (
	"context"
	"fmt"

	"go.temporal.io/api/workflowservice/v1"
)

// OpenRunIDs lists at most limit runs the cluster holds open, once each, and
// whether more are open: the open-run listing a restore reconcile reads
// (archive.RestoreCluster). An open execution is any record visibility does
// not show terminal or ContinuedAsNew. A chain caught in the moment between
// one generation's close and the next one's indexing is not listed; after a
// restore, with no Orkers running, no chain switches. The listing is not
// ordered, so a page of a moving index may repeat a record, which is listed
// once.
func (r *TemporalRunReader) OpenRunIDs(ctx context.Context, limit int) ([]string, bool, error) {
	if limit < 1 {
		return nil, false, fmt.Errorf("engine: open runs: a listing needs a positive bound, got %d", limit)
	}
	if r.client == nil {
		return nil, false, fmt.Errorf("engine: open runs: the reader has no Temporal client")
	}
	query := fmt.Sprintf("WorkflowType = %q AND %s", RunWorkflowType, openExecutionsPredicate())
	seen := map[string]struct{}{}
	var out []string
	var token []byte
	for page := 0; page < maxRunScanPages; page++ {
		response, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query: query, PageSize: statsPageSize, NextPageToken: token,
		})
		if err != nil {
			return nil, false, fmt.Errorf("engine: open runs: read the cluster's run index: %w", err)
		}
		for _, info := range response.GetExecutions() {
			id := info.GetExecution().GetWorkflowId()
			if _, dup := seen[id]; dup || !isOpenExecution(info) {
				continue
			}
			if len(out) == limit {
				return out, true, nil
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return out, false, nil
		}
	}
	return nil, false, fmt.Errorf("engine: open runs: the cluster's run index did not reach its end after %d pages", maxRunScanPages)
}
