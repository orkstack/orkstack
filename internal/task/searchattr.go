package task

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// SearchAttrOrkflowNamespace is the visibility index that makes an OrkStack
// namespace a predicate rather than a filter.
//
// A namespace is a tenancy boundary, so "list this namespace's runs" has to be
// answered by the index. Filtered after the fact over a bounded page walk, the
// answer depends on how much traffic OTHER namespaces have: a namespace with
// one live run beside a namespace with two hundred is told it has none,
// because the walk spent its budget on runs it then discarded. A predicate the
// cluster applies cannot do that — every record it returns is already the
// tenant's.
//
// The orkflow_ prefix is the memo's rule (engine.MemoNamespace) for the same
// reason: an unqualified `namespace` sitting beside a Temporal client would
// read as the Temporal namespace, which AGENTS.md §7 forbids. The memo stays
// where it is — it carries the value back with each record, which a search
// attribute also does, but the memo is what a run started before this index
// existed has.
const SearchAttrOrkflowNamespace = "OrkflowNamespace"

// SearchAttrDeploymentVersion is the run index's record of the release a run
// executes on. The cluster provides and maintains it; it is never registered
// (EnsureRunSearchAttributes is unchanged) and never user-facing
// (orkctl-release §4).
const SearchAttrDeploymentVersion = "TemporalWorkerDeploymentVersion"

// SearchKeyOrkflowNamespace is the typed key StartRun stamps onto a run.
var SearchKeyOrkflowNamespace = temporal.NewSearchAttributeKeyKeyword(SearchAttrOrkflowNamespace)

// searchAttributeReadyTimeout bounds the wait for a freshly registered
// attribute to become visible. A cluster indexes it in milliseconds; an
// Elasticsearch-backed one takes a moment, and a query issued against an
// attribute the cluster has not caught up on is an error, not an empty page.
const searchAttributeReadyTimeout = 15 * time.Second

// searchAttributeRegistrar is the slice of Temporal's OperatorService this file
// needs. Narrowing it keeps provisioning testable without a live cluster.
type searchAttributeRegistrar interface {
	AddSearchAttributes(
		ctx context.Context,
		in *operatorservice.AddSearchAttributesRequest,
		opts ...grpc.CallOption,
	) (*operatorservice.AddSearchAttributesResponse, error)
	ListSearchAttributes(
		ctx context.Context,
		in *operatorservice.ListSearchAttributesRequest,
		opts ...grpc.CallOption,
	) (*operatorservice.ListSearchAttributesResponse, error)
}

// EnsureRunSearchAttributes registers the run index this deployment needs on
// the configured Temporal namespace, and reports whether it is usable.
//
// It runs in a binary's main beside EnsureNamespace, never inside workflow
// code, and it is idempotent: an attribute that already exists is left alone.
//
// The bool is the point of the signature. A cluster that refuses the
// registration — an operator API behind an authorizer, a visibility store
// without custom attributes — is not a reason to refuse to boot, because
// everything except the scoped list still works. So the caller is told, once,
// and the two readers of that answer stay honest: StartRun stamps the
// attribute only when the cluster has it (stamping one it does not have fails
// every submit), and TemporalRunReader falls back to walking the whole index
// rather than to a filter that would silently under-report a tenant's runs.
func EnsureRunSearchAttributes(
	ctx context.Context,
	temporalClient client.Client,
	namespace string,
) (bool, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return ensureRunSearchAttributes(ctx, temporalClient.OperatorService(), namespace)
}

func ensureRunSearchAttributes(
	ctx context.Context,
	svc searchAttributeRegistrar,
	namespace string,
) (bool, error) {
	return ensureSearchAttributes(ctx, svc, namespace, map[string]enumspb.IndexedValueType{
		SearchAttrOrkflowNamespace: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
}

// SearchAttrOrkflowOriginKind and SearchAttrOrkflowOriginId index a run's
// origin so "which runs did this trigger start" is a predicate the cluster
// applies, on the terms SearchAttrOrkflowNamespace states. The orkflow_
// prefix is the same rule: an unqualified name beside a Temporal client
// would read as something Temporal owns.
const (
	SearchAttrOrkflowOriginKind = "OrkflowOriginKind"
	SearchAttrOrkflowOriginId   = "OrkflowOriginId"
)

// SearchKeyOrkflowOriginKind and SearchKeyOrkflowOriginID are the typed keys
// StartRun stamps a run's origin under.
var (
	SearchKeyOrkflowOriginKind = temporal.NewSearchAttributeKeyKeyword(SearchAttrOrkflowOriginKind)
	SearchKeyOrkflowOriginID   = temporal.NewSearchAttributeKeyKeyword(SearchAttrOrkflowOriginId)
)

// SearchAttrTemporalScheduledStartTime is the Temporal-owned, server-
// provided datetime attribute a Temporal Schedule stamps on each workflow
// it starts: that fire's nominal time, jitter included. OrkStack reads it
// inside ScheduleFireWorkflow and never registers, writes or queries it;
// EnsureRunSearchAttributes is unchanged.
const SearchAttrTemporalScheduledStartTime = "TemporalScheduledStartTime"

// SearchKeyTemporalScheduledStartTime is its typed, read-only key.
var SearchKeyTemporalScheduledStartTime = temporal.NewSearchAttributeKeyTime(SearchAttrTemporalScheduledStartTime)

// EnsureRunOriginSearchAttributes registers the two origin attributes and
// reports whether BOTH are usable, with the same idempotency, readiness poll
// and "refused is not fatal" contract EnsureRunSearchAttributes has. It is
// a separate call with a separate answer on purpose: a cluster that
// accepted OrkflowNamespace in an earlier release and now refuses the
// operator API must keep its namespace predicate; folding the three into
// one answer would silently downgrade a working tenant listing to the slow
// walk on the day of an upgrade.
func EnsureRunOriginSearchAttributes(
	ctx context.Context,
	temporalClient client.Client,
	namespace string,
) (bool, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return ensureRunOriginSearchAttributes(ctx, temporalClient.OperatorService(), namespace)
}

func ensureRunOriginSearchAttributes(
	ctx context.Context,
	svc searchAttributeRegistrar,
	namespace string,
) (bool, error) {
	return ensureSearchAttributes(ctx, svc, namespace, map[string]enumspb.IndexedValueType{
		SearchAttrOrkflowOriginKind: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrOrkflowOriginId:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
}

// ensureSearchAttributes registers every attribute in want the namespace does
// not already index, then polls until all of them are queryable. Only the
// missing ones are sent: a registration that names an attribute the cluster
// already has is refused whole, which would leave its sibling unregistered.
func ensureSearchAttributes(
	ctx context.Context,
	svc searchAttributeRegistrar,
	namespace string,
	want map[string]enumspb.IndexedValueType,
) (bool, error) {
	names := sortedAttributeNames(want)
	missing, err := missingSearchAttributes(ctx, svc, namespace, want)
	if err == nil && len(missing) == 0 {
		return true, nil
	}
	if err != nil {
		// The read failed; ask for all of them and let the registration and the
		// poll below decide.
		missing = want
	}
	if _, err := svc.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace:        namespace,
		SearchAttributes: missing,
	}); err != nil && grpcCode(err) != codes.AlreadyExists {
		return false, fmt.Errorf("register search attribute %s in temporal namespace %q: %w",
			strings.Join(sortedAttributeNames(missing), ","), namespace, err)
	}
	// Registered is not the same as queryable. Poll rather than assume: the
	// first ListRuns after boot would otherwise fail on an attribute the
	// cluster has accepted and not yet indexed.
	deadline := time.Now().Add(searchAttributeReadyTimeout)
	for {
		stillMissing, err := missingSearchAttributes(ctx, svc, namespace, want)
		if err != nil {
			return false, fmt.Errorf("read search attributes of temporal namespace %q: %w",
				namespace, err)
		}
		if len(stillMissing) == 0 {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, fmt.Errorf(
				"search attribute %s was registered in temporal namespace %q but is not "+
					"queryable after %s", strings.Join(names, ","), namespace,
				searchAttributeReadyTimeout)
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// missingSearchAttributes reports which of want the namespace does not index
// yet; empty means every one is present.
func missingSearchAttributes(
	ctx context.Context,
	svc searchAttributeRegistrar,
	namespace string,
	want map[string]enumspb.IndexedValueType,
) (map[string]enumspb.IndexedValueType, error) {
	response, err := svc.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{
		Namespace: namespace,
	})
	if err != nil {
		return nil, err
	}
	missing := map[string]enumspb.IndexedValueType{}
	for name, kind := range want {
		if _, ok := response.GetCustomAttributes()[name]; !ok {
			missing[name] = kind
		}
	}
	return missing, nil
}

func sortedAttributeNames(attrs map[string]enumspb.IndexedValueType) []string {
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
