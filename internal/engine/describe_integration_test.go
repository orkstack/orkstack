//go:build integration

package engine_test

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// describeExecution is the cluster's full description of one execution:
// DescribeWorkflow's raw response, which the deprecated
// DescribeWorkflowExecution used to return.
func describeExecution(c client.Client, ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	described, err := c.DescribeWorkflow(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}
	return described.RawResponse, nil
}
