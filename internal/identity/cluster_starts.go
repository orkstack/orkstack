package identity

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ErrFirstStartUnread is the start check's read of the admission a chain's
// first start names. The interpreter records it with the run's plan when
// task identity lands; until then no run holds an admission, and a chain
// record that is missing cannot be completed, so the check is unavailable,
// never admitted.
var ErrFirstStartUnread = errors.New("the admission a run's first start names is not readable in this build")

// ClusterStarts is the start check's reader (StartReader): the cluster's
// describe of one execution, and the run's records in object storage.
type ClusterStarts struct {
	*artifacts.RecordStore
	Temporal client.Client
}

// Describe reads one execution of a run; executionID "" is its current
// one. A run the cluster holds no execution of is ErrNoSuchExecution.
func (c ClusterStarts) Describe(ctx context.Context, workflowID, executionID string) (ExecutionFacts, error) {
	desc, err := c.Temporal.DescribeWorkflow(ctx, workflowID, executionID)
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return ExecutionFacts{}, ErrNoSuchExecution
	}
	if err != nil {
		return ExecutionFacts{}, err
	}
	if desc == nil || desc.RawResponse == nil {
		return ExecutionFacts{}, errors.New("the cluster's describe held no execution")
	}
	return FactsFromDescribe(desc.RawResponse), nil
}

// FirstStartAdmission answers ErrFirstStartUnread in this build.
func (ClusterStarts) FirstStartAdmission(context.Context, string, string) (string, error) {
	return "", ErrFirstStartUnread
}
