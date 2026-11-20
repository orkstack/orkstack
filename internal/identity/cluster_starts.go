package identity

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ErrFirstStartUnread is the start check's read of the admission a chain's
// first start names, when the reader has no way to read the cluster's
// history: the check is then unavailable, never admitted.
var ErrFirstStartUnread = errors.New("the admission a run's first start names cannot be read here")

// FirstStartPlans reads the admission digest the plan of one execution's
// start carried (engine.TemporalRunReader).
type FirstStartPlans interface {
	FirstStartAdmission(ctx context.Context, runID, firstExecutionID string) (string, error)
}

// ClusterStarts is the start check's reader (StartReader): the cluster's
// describe of one execution, and the run's records in object storage.
type ClusterStarts struct {
	*artifacts.RecordStore
	Temporal client.Client
	// Plans reads a first start's plan; nil answers ErrFirstStartUnread.
	Plans FirstStartPlans
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

// FirstStartAdmission is the admission digest the plan of the chain's
// first execution names, read from that execution's own start.
func (c ClusterStarts) FirstStartAdmission(ctx context.Context, runID, firstExecutionID string) (string, error) {
	if c.Plans == nil {
		return "", ErrFirstStartUnread
	}
	return c.Plans.FirstStartAdmission(ctx, runID, firstExecutionID)
}
