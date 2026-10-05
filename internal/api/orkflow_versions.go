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
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The version operations an editor needs: see the versions, move the pointer,
// and compare two of them. All three are namespace-scoped, all three read the
// durable store on every call, and none of them can change a stored version —
// the only mutation here is the pointer.

// ListOrkflowVersions lists every stored version of one orkflow, newest
// applied first, marking the one an empty version resolves to.
//
// current_version and head_version are reported separately because they answer
// different questions: head is the edit lineage's tip (the base_version an
// editor sends back), current is what runs. They coincide until somebody
// promotes an older version — the case where conflating them would run the
// wrong code.
func (s *Service) ListOrkflowVersions(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListOrkflowVersionsRequest],
) (*connect.Response[orkstackv1.ListOrkflowVersionsResponse], error) {
	if req.Msg.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	versions, err := s.orkflows.Versions(ctx, ns.GetName(), req.Msg.GetName())
	if err != nil {
		return nil, orkflowStoreError(err, "versions of orkflow %q in namespace %q",
			req.Msg.GetName(), ns.GetName())
	}
	if len(versions.All) == 0 {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf(
			"orkflow %q in namespace %q not found", req.Msg.GetName(), ns.GetName()))
	}
	resolved := versions.Resolved()
	rows := make([]*orkstackv1.OrkflowVersion, 0, len(versions.All))
	for _, wf := range versions.All {
		rows = append(rows, &orkstackv1.OrkflowVersion{
			Version:     wf.GetVersion(),
			AppliedAt:   wf.GetCreatedAt(),
			IsCurrent:   wf.GetVersion() == resolved,
			Description: wf.GetDescription(),
		})
	}
	return connect.NewResponse(&orkstackv1.ListOrkflowVersionsResponse{
		Versions:       rows,
		CurrentVersion: versions.Current,
		HeadVersion:    versions.Head,
	}), nil
}

// SetCurrentOrkflowVersion moves an orkflow's current pointer.
//
// The store validates that the target version exists before it writes the
// pointer and swaps compare-and-swap style, so two replicas promoting at once
// converge on one value and no reader can ever see a pointer to a version that
// is not stored. A lost race is FailedPrecondition — the caller re-reads and
// decides again — never a silent overwrite of the winner.
func (s *Service) SetCurrentOrkflowVersion(
	ctx context.Context,
	req *connect.Request[orkstackv1.SetCurrentOrkflowVersionRequest],
) (*connect.Response[orkstackv1.SetCurrentOrkflowVersionResponse], error) {
	if req.Msg.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	if req.Msg.GetVersion() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"version is required: promoting to \"the latest\" would move the pointer to "+
				"whatever is applied next, which is what the pointer exists to stop"))
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	previous, err := s.orkflows.SetCurrentVersion(
		ctx, ns.GetName(), req.Msg.GetName(), req.Msg.GetVersion(), artifacts.NoCurrentExpectation())
	if err != nil {
		if errors.Is(err, artifacts.ErrPreconditionFailed) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"orkflow %q in namespace %q was promoted by somebody else while this "+
					"promotion was being written; read the versions again and retry: %w",
				req.Msg.GetName(), ns.GetName(), err))
		}
		return nil, orkflowStoreError(err, "promote orkflow %q to version %q in namespace %q",
			req.Msg.GetName(), req.Msg.GetVersion(), ns.GetName())
	}
	return connect.NewResponse(&orkstackv1.SetCurrentOrkflowVersionResponse{
		Name:            req.Msg.GetName(),
		Namespace:       ns.GetName(),
		CurrentVersion:  req.Msg.GetVersion(),
		PreviousVersion: previous,
		UpdatedAt:       timestamppb.New(s.now().UTC()),
	}), nil
}

// DiffOrkflowVersions renders the unified diff between two versions' source
// YAML.
//
// The diff is the server's (internal/dsl.Unified) so that every client renders
// the same answer — see the RPC's proto comment. An empty version on either
// side means the version an empty version resolves to, which makes
// `orkctl orkflow diff deploy "" 2.0.0` read as "what would promoting change".
func (s *Service) DiffOrkflowVersions(
	ctx context.Context,
	req *connect.Request[orkstackv1.DiffOrkflowVersionsRequest],
) (*connect.Response[orkstackv1.DiffOrkflowVersionsResponse], error) {
	if req.Msg.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	from, err := s.orkflows.Get(ctx, ns.GetName(), req.Msg.GetName(), req.Msg.GetFromVersion())
	if err != nil {
		return nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q",
			req.Msg.GetName(), req.Msg.GetFromVersion(), ns.GetName())
	}
	to, err := s.orkflows.Get(ctx, ns.GetName(), req.Msg.GetName(), req.Msg.GetToVersion())
	if err != nil {
		return nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q",
			req.Msg.GetName(), req.Msg.GetToVersion(), ns.GetName())
	}
	return connect.NewResponse(&orkstackv1.DiffOrkflowVersionsResponse{
		UnifiedDiff: dsl.Unified(
			versionLabel(from), versionLabel(to),
			from.GetSourceYaml(), to.GetSourceYaml()),
		FromVersion: from.GetVersion(),
		ToVersion:   to.GetVersion(),
	}), nil
}

// versionLabel names one side of a diff: the file path a reader would expect
// in a `--- a/…` header, spelled as the coordinates that address the document.
func versionLabel(wf *orkstackv1.Orkflow) string {
	return wf.GetName() + "@" + wf.GetVersion()
}

// OrkflowDeletionRunWindow is the recent-run safety window checked before an
// orkflow can be permanently deleted.
const OrkflowDeletionRunWindow = 24 * time.Hour

// DeleteOrkflow permanently removes every stored version of one orkflow after
// proving that no active or recent run still refers to it.
func (s *Service) DeleteOrkflow(
	ctx context.Context,
	req *connect.Request[orkstackv1.DeleteOrkflowRequest],
) (*connect.Response[orkstackv1.DeleteOrkflowResponse], error) {
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	// Two rules, deliberately, because they answer different questions. The
	// SLUG rule is the user-facing contract (AGENTS.md §7), applied here so a
	// name no OrkStack surface would ever create is refused at the boundary
	// that received it. The store's ADDRESSING rule is broader on purpose: it
	// only asks whether a key can be formed, so an object written by another
	// consumer of the public artifacts package stays listable and deletable
	// instead of becoming unreachable.
	if err := dsl.ValidSlug("orkflow name", name); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if _, err := artifacts.OrkflowKey(ns.GetName(), name, "validation"); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("invalid orkflow name %q: %w", name, err))
	}

	// Existence is checked before the run query so a missing name is NotFound
	// even when the run record is temporarily unavailable.
	if _, err := s.orkflows.Get(ctx, ns.GetName(), name, ""); err != nil {
		return nil, orkflowStoreError(err, "orkflow %q in namespace %q", name, ns.GetName())
	}

	// A schedule names the orkflow it starts; deleting the orkflow under it
	// would turn every later fire into a refusal. The check is the cluster's
	// answer, and an unanswerable one refuses the delete on the terms the run
	// query below does.
	if s.schedules != nil {
		views, err := s.schedules.List(ctx, ns.GetName())
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"cannot verify whether orkflow %q in namespace %q has schedules: %w",
				name, ns.GetName(), err))
		}
		var naming []string
		for _, view := range views {
			if view.OrkflowName == name {
				naming = append(naming, view.Name)
			}
		}
		if len(naming) > 0 {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"cannot delete orkflow %q in namespace %q: %d schedule(s) name it (%s); delete them first",
				name, ns.GetName(), len(naming), strings.Join(naming, ", ")))
		}
	}

	listed, err := s.completeOrkflowRuns(ctx, ns.GetName(), name)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"cannot verify whether orkflow %q in namespace %q has active or recent runs: %w",
			name, ns.GetName(), err))
	}

	cutoff := s.now().Add(-OrkflowDeletionRunWindow)
	active, recent := 0, 0
	for _, run := range listed {
		if !isTerminalStatus(run.GetStatus()) {
			active++
			continue
		}
		finished := run.GetFinishedAt()
		if finished == nil || finished.AsTime().After(cutoff) {
			recent++
		}
	}
	if active+recent > 0 {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"cannot delete orkflow %q in namespace %q: %d run(s) block deletion (%d active, %d finished within %s)",
			name, ns.GetName(), active+recent, active, recent, OrkflowDeletionRunWindow))
	}

	if err := s.orkflows.Delete(ctx, ns.GetName(), name); err != nil {
		return nil, orkflowStoreError(err, "delete orkflow %q in namespace %q", name, ns.GetName())
	}
	return connect.NewResponse(&orkstackv1.DeleteOrkflowResponse{
		Name:      name,
		Namespace: ns.GetName(),
		DeletedAt: timestamppb.New(s.now().UTC()),
	}), nil
}

// completeOrkflowRuns reads both durable run authorities to completion. A
// deletion gate cannot use the user-facing bounded ListRuns page: an older
// active run behind that page is still active. Temporal's walk either reaches
// the end or errors, and the archive's namespace/orkflow prefix is exhaustive.
func (s *Service) completeOrkflowRuns(ctx context.Context, namespace, name string) ([]*orkstackv1.Run, error) {
	binding, err := s.binding(namespace)
	if err != nil {
		return nil, err
	}
	var runs []*orkstackv1.Run
	if binding.Reader != nil {
		runs, err = binding.Reader.ListRuns(ctx, engine.RunQuery{
			Namespace: namespace, OrkflowName: name, Exhaustive: true,
		})
		if err != nil {
			return nil, err
		}
	} else {
		runs = s.runs.List(engine.ListFilter{Namespace: namespace, OrkflowName: name})
	}
	if s.archive == nil {
		return runs, nil
	}
	archived, err := s.archive.ListRuns(ctx, engine.ArchiveQuery{
		Namespace: namespace, OrkflowName: name, Exhaustive: true,
	})
	if err != nil {
		return nil, err
	}
	return mergeRuns(archived, runs), nil
}
