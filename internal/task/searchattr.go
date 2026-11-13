package task

import (
	"context"
	"errors"
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
// (docs/DEPLOY.md §3).
const SearchAttrDeploymentVersion = "TemporalWorkerDeploymentVersion"

// SearchKeyOrkflowNamespace is the typed key StartRun stamps onto a run.
var SearchKeyOrkflowNamespace = temporal.NewSearchAttributeKeyKeyword(SearchAttrOrkflowNamespace)

// searchAttributeReadyTimeout bounds the wait for a freshly registered
// attribute to become visible. A cluster indexes it in milliseconds; an
// Elasticsearch-backed one takes a moment, and a query issued against an
// attribute the cluster has not caught up on is an error, not an empty page.
const searchAttributeReadyTimeout = 15 * time.Second

// NamespaceVisibleTimeout bounds the boot's wait for the Temporal namespace
// it just ensured to be seen by the cluster's operator service. A namespace
// registered a moment ago is answered NotFound there until the cluster's
// namespace cache catches up, which Temporal refreshes every few seconds.
const NamespaceVisibleTimeout = time.Minute

// namespaceVisiblePoll is how often the wait asks again. A variable only so
// the unit tests can ask without waiting.
var namespaceVisiblePoll = 200 * time.Millisecond

// operatorCallTimeout bounds one operator-service call inside the wait, so a
// call that hangs cannot carry the boot past NamespaceVisibleTimeout.
const operatorCallTimeout = 10 * time.Second

// ErrNamespaceNotVisible is what the boot fails on: the cluster still answers
// that the Temporal namespace is not found when NamespaceVisibleTimeout has
// passed, so the run index could not be registered on it. Every other refusal
// of the registration is reported and survived.
var ErrNamespaceNotVisible = errors.New("temporal namespace is not visible to the cluster's operator service")

// withVisibilityDeadline gives ctx the overall deadline the wait and the
// registrations share, unless the caller already set one.
func withVisibilityDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, NamespaceVisibleTimeout)
}

// retryNotFound runs call until it answers anything but NotFound, asking
// again every poll until ctx's deadline. Each call gets at most
// operatorCallTimeout of what is left. A namespace still not found at the
// deadline is ErrNamespaceNotVisible, wrapping the last answer.
func retryNotFound(ctx context.Context, namespace string, poll time.Duration, call func(context.Context) error) error {
	for {
		callCtx, cancel := context.WithTimeout(ctx, operatorCallTimeout)
		err := call(callCtx)
		cancel()
		if err == nil || grpcCode(err) != codes.NotFound {
			return err
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf(
					"%w: temporal namespace %q exists but the cluster still answers that it "+
						"is not found, so the run index cannot be registered on it; check that "+
						"the cluster has finished starting and that every frontend serves this "+
						"namespace, then restart the engine: %w",
					ErrNamespaceNotVisible, namespace, err)
			}
			return fmt.Errorf("wait for temporal namespace %q to be visible: %w", namespace, ctx.Err())
		case <-time.After(poll):
		}
	}
}

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

// SearchAttrOrkflowOriginRootRunId indexes the root of a run's composed chain,
// so "everything below this root" is a predicate the cluster applies, on the
// terms SearchAttrOrkflowNamespace states.
const SearchAttrOrkflowOriginRootRunId = "OrkflowOriginRootRunId"

// SearchKeyOrkflowOriginRootRunID is the typed key a child start stamps the
// root under. Only a run with a root recorded carries it.
var SearchKeyOrkflowOriginRootRunID = temporal.NewSearchAttributeKeyKeyword(SearchAttrOrkflowOriginRootRunId)

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

// EnsureRunRootSearchAttribute registers the root attribute and reports
// whether it is usable, on EnsureRunOriginSearchAttributes' terms and as its
// own call for the same reason: a cluster that accepted the origin attributes
// in an earlier release and refuses this one now, at its limit of custom
// attributes or behind an authorizer, keeps its origin predicate. Off, a child
// start carries no attribute, and a list by root walks the index and refuses
// rather than answer short.
func EnsureRunRootSearchAttribute(
	ctx context.Context,
	temporalClient client.Client,
	namespace string,
) (bool, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return ensureRunRootSearchAttribute(ctx, temporalClient.OperatorService(), namespace)
}

func ensureRunRootSearchAttribute(
	ctx context.Context,
	svc searchAttributeRegistrar,
	namespace string,
) (bool, error) {
	return ensureSearchAttributes(ctx, svc, namespace, map[string]enumspb.IndexedValueType{
		SearchAttrOrkflowOriginRootRunId: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
}

// ensureSearchAttributes registers every attribute in want the namespace does
// not already index, then polls until all of them are queryable. Only the
// missing ones are sent: a registration that names an attribute the cluster
// already has is refused whole, which would leave its sibling unregistered.
//
// Every call keeps asking while the cluster answers that the namespace is not
// found, under the deadline the boot's wait set (WaitNamespaceVisible): with
// several frontends, one may still not see a namespace another already did.
// Still not found at that deadline is ErrNamespaceNotVisible, which the boot
// fails on.
func ensureSearchAttributes(
	ctx context.Context,
	svc searchAttributeRegistrar,
	namespace string,
	want map[string]enumspb.IndexedValueType,
) (bool, error) {
	ctx, cancel := withVisibilityDeadline(ctx)
	defer cancel()
	missingNow := func() (map[string]enumspb.IndexedValueType, error) {
		var missing map[string]enumspb.IndexedValueType
		err := retryNotFound(ctx, namespace, namespaceVisiblePoll, func(callCtx context.Context) error {
			var err error
			missing, err = missingSearchAttributes(callCtx, svc, namespace, want)
			return err
		})
		return missing, err
	}
	names := sortedAttributeNames(want)
	missing, err := missingNow()
	if errors.Is(err, ErrNamespaceNotVisible) {
		return false, err
	}
	if err == nil && len(missing) == 0 {
		return true, nil
	}
	if err != nil {
		// The read failed; ask for all of them and let the registration and the
		// poll below decide.
		missing = want
	}
	err = retryNotFound(ctx, namespace, namespaceVisiblePoll, func(callCtx context.Context) error {
		_, err := svc.AddSearchAttributes(callCtx, &operatorservice.AddSearchAttributesRequest{
			Namespace:        namespace,
			SearchAttributes: missing,
		})
		return err
	})
	if errors.Is(err, ErrNamespaceNotVisible) {
		return false, err
	}
	if err != nil && grpcCode(err) != codes.AlreadyExists {
		// Usable is the cluster's answer, not this call's: another replica's
		// registration may have landed while this one failed, and a replica
		// that then booted unindexed would start runs the others' scoped reads
		// cannot see.
		if stillMissing, listErr := missingNow(); listErr == nil && len(stillMissing) == 0 {
			return true, nil
		}
		return false, fmt.Errorf("register search attribute %s in temporal namespace %q: %w",
			strings.Join(sortedAttributeNames(missing), ","), namespace, err)
	}
	// Registered is not the same as queryable. Poll rather than assume: the
	// first ListRuns after boot would otherwise fail on an attribute the
	// cluster has accepted and not yet indexed.
	deadline := time.Now().Add(searchAttributeReadyTimeout)
	for {
		stillMissing, err := missingNow()
		if errors.Is(err, ErrNamespaceNotVisible) {
			return false, err
		}
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
			return false, fmt.Errorf("wait for search attribute %s in temporal namespace %q: %w",
				strings.Join(names, ","), namespace, ctx.Err())
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

// WaitNamespaceVisible waits, up to NamespaceVisibleTimeout or ctx's own
// earlier deadline, until the
// cluster's operator service sees the Temporal namespace EnsureNamespace just
// ensured, so the run index is registered against a namespace that exists
// there. Without the wait, a replica that boots right after the namespace was
// created is refused the registration (NotFound), comes up unindexed and
// starts runs without the namespace attribute, while a replica booting a
// moment later is indexed: the fleet then answers a namespace's runs
// differently depending on which replica is asked.
//
// Only NotFound is waited for. Any other answer (an operator API behind an
// authorizer, a cluster without custom attributes) is the registration's to
// report, as before. A namespace that never becomes visible is
// ErrNamespaceNotVisible, which the boot fails on. The boot passes the same
// ctx, with its deadline, to the registrations that follow, which keep asking
// on NotFound under that one deadline.
func WaitNamespaceVisible(ctx context.Context, temporalClient client.Client, namespace string) error {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return waitNamespaceVisible(ctx, temporalClient.OperatorService(), namespace,
		NamespaceVisibleTimeout, namespaceVisiblePoll)
}

func waitNamespaceVisible(
	ctx context.Context,
	svc searchAttributeRegistrar,
	namespace string,
	timeout, poll time.Duration,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := retryNotFound(ctx, namespace, poll, func(callCtx context.Context) error {
		_, err := svc.ListSearchAttributes(callCtx, &operatorservice.ListSearchAttributesRequest{
			Namespace: namespace,
		})
		return err
	})
	switch {
	case err == nil, errors.Is(err, ErrNamespaceNotVisible):
		return err
	case ctx.Err() != nil:
		// The deadline passed, or the boot was stopped, while a call was out.
		return fmt.Errorf("wait for temporal namespace %q to be visible: %w", namespace, err)
	}
	// Any answer but NotFound is the registration's to report.
	return nil
}
