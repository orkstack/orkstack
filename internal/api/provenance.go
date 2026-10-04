package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// provenanceFetchConcurrency bounds the parallel run-body reads one
// provenance answer issues. It is the value engine.RunArchiver.ListRuns uses
// (archiveFetchConcurrency, unexported there): the same shape of problem —
// one body read per archived run — so the same number rather than a second
// knob.
const provenanceFetchConcurrency = 8

// expectSHA256Pattern is a caller-supplied digest after any "sha256:" prefix
// is stripped: 64 lowercase hex characters.
var expectSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ObjectReferenceReader is the archive's provenance half. Asserted from the
// RunArchive seam at call time so the seam's fakes need not grow.
type ObjectReferenceReader interface {
	ObjectReferences(ctx context.Context, objectID string) ([]string, error)
	CleanupState(ctx context.Context, runID string) (artifacts.RunCleanupState, error)
}

func errNoProvenanceArchive() error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("run archive is not configured"))
}

// GetArtifactProvenance answers, for one recorded artifact, whether its bytes
// are still in the object store and which archived runs of the calling
// namespace recorded the same physical object.
//
// It reads the reverse index the archive maintains for cleanup refcounting
// and writes nothing. The artifact is resolved against the authoritative,
// namespace-checked run — never from a caller-supplied URI or key — and every
// store failure is an error, never a quiet false or an empty list.
func (s *Service) GetArtifactProvenance(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetArtifactProvenanceRequest],
) (*connect.Response[orkstackv1.GetArtifactProvenanceResponse], error) {
	msg := req.Msg
	if msg.GetRunId() == "" || msg.GetTaskId() == "" || msg.GetName() == "" || msg.GetKind() == orkstackv1.ArtifactKind_ARTIFACT_KIND_UNSPECIFIED {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id, task_id, kind and name are required"))
	}
	expect := ""
	if raw := msg.GetExpectSha256(); raw != "" {
		expect = strings.TrimPrefix(raw, "sha256:")
		if !expectSHA256Pattern.MatchString(expect) {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("expect_sha256 %q is not 64 lowercase hex characters (an optional sha256: prefix is accepted)", raw))
		}
	}
	if s.artifactStore == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("artifact store is not configured"))
	}
	if s.archive == nil {
		return nil, errNoProvenanceArchive()
	}
	index, ok := s.archive.(ObjectReferenceReader)
	if !ok {
		return nil, errNoProvenanceArchive()
	}

	run, err := s.LookupRunIn(ctx, msg.GetRunId(), msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	if !hasTask(run, msg.GetTaskId()) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("task %q not found in run %q", msg.GetTaskId(), run.GetId()))
	}
	// The item selector, exactly as ReadArtifact applies it.
	if _, err := itemRunOf(run, msg.GetTaskId(), msg.Item); err != nil {
		return nil, err
	}
	if msg.Item == nil {
		if err := needsItem(run, msg.GetTaskId(), "orkctl run artifact provenance …"); err != nil {
			return nil, err
		}
	}
	var selected *orkstackv1.RunArtifact
	for _, item := range runArtifacts(run, msg.GetTaskId(), msg.Item) {
		if item.GetKind() == msg.GetKind() && item.GetArtifact().GetName() == msg.GetName() {
			selected = item
			break
		}
	}
	if selected == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("artifact %q of kind %s is not recorded for task %q in run %q", msg.GetName(), msg.GetKind(), msg.GetTaskId(), run.GetId()))
	}
	ref := selected.GetArtifact()

	if expect != "" {
		recorded := strings.TrimPrefix(ref.GetSha256(), "sha256:")
		if !strings.EqualFold(recorded, expect) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("recorded sha256 %s does not match expected %s", recorded, expect))
		}
	}

	objectID, ok := artifacts.ObjectIDFromURI(ref.GetUri())
	if !ok {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("artifact %q is not a content-addressed object", ref.GetName()))
	}

	res := &orkstackv1.GetArtifactProvenanceResponse{Artifact: ref}
	switch _, err := s.artifactStore.Stat(ctx, ref.GetUri()); {
	case err == nil:
		res.BytesPresent = true
	case errors.Is(err, artifacts.ErrNotFound):
		res.BytesPresent = false
	default:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("stat recorded artifact %q: %w", ref.GetName(), err))
	}

	runIDs, err := index.ObjectReferences(ctx, objectID)
	switch {
	case err == nil:
	case errors.Is(err, artifacts.ErrTooManyReferences):
		return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
			"artifact %q is referenced by more than %d archived runs; provenance refuses a partial list",
			ref.GetName(), artifacts.MaxObjectReferences))
	case errors.Is(err, engine.ErrNoRunArchive):
		return nil, errNoProvenanceArchive()
	case errors.Is(err, engine.ErrNoObjectIndex):
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("list references of artifact %q: %w", ref.GetName(), err))
	default:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("list references of artifact %q: %w", ref.GetName(), err))
	}

	held := false
	others := make([]string, 0, len(runIDs))
	for _, id := range runIDs {
		if id == run.GetId() {
			held = true
			continue
		}
		others = append(others, id)
	}
	if held {
		res.OwnReference = orkstackv1.ReferenceState_REFERENCE_STATE_HELD
	} else {
		state, err := index.CleanupState(ctx, run.GetId())
		switch {
		case err == nil:
			res.OwnReference = orkstackv1.ReferenceState_REFERENCE_STATE_RELEASED
			res.ReleasedAt = timestamppb.New(state.TemporalDeletedAt)
		case errors.Is(err, artifacts.ErrNotFound):
			res.OwnReference = orkstackv1.ReferenceState_REFERENCE_STATE_ABSENT
		default:
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read cleanup state of run %q: %w", run.GetId(), err))
		}
	}

	loaded, unreadable, err := s.loadReferencingRuns(ctx, others)
	if err != nil {
		return nil, err
	}
	res.ReferencesUnreadable = unreadable
	namespace := artifacts.ResolveNamespace(run.GetNamespace())
	holders := make([]*orkstackv1.Run, 0, len(loaded)+1)
	if held {
		// The addressed run is resolved from the run in hand: no second body read.
		holders = append(holders, run)
	}
	for _, other := range loaded {
		// Another namespace's run is dropped and not counted: "some other
		// tenant also holds this object" is itself a disclosure.
		if artifacts.ResolveNamespace(other.GetNamespace()) == namespace {
			holders = append(holders, other)
		}
	}
	sort.SliceStable(holders, func(i, j int) bool {
		a, b := holders[i].GetCreatedAt().AsTime(), holders[j].GetCreatedAt().AsTime()
		if !a.Equal(b) {
			return a.After(b)
		}
		return holders[i].GetId() > holders[j].GetId()
	})
	for _, holder := range holders {
		res.References = append(res.References, referencesIn(holder, objectID)...)
	}
	res.ComputedAt = timestamppb.New(s.now())
	return connect.NewResponse(res), nil
}

// loadReferencingRuns reads the archived body of every run id, at most
// provenanceFetchConcurrency at once. A body that is not in the archive is
// counted, never listed; any other read failure fails the whole answer, since
// a transient error counted as "unreadable" would make one question answer
// differently twice.
func (s *Service) loadReferencingRuns(ctx context.Context, runIDs []string) ([]*orkstackv1.Run, int64, error) {
	runs := make([]*orkstackv1.Run, len(runIDs))
	errs := make([]error, len(runIDs))
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, provenanceFetchConcurrency)
	)
	for i, runID := range runIDs {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, runID string) {
			defer wg.Done()
			defer func() { <-slots }()
			runs[i], errs[i] = s.archive.LoadRun(ctx, runID)
		}(i, runID)
	}
	wg.Wait()

	var unreadable int64
	loaded := make([]*orkstackv1.Run, 0, len(runIDs))
	for i, err := range errs {
		switch {
		case err == nil:
			loaded = append(loaded, runs[i])
		case errors.Is(err, artifacts.ErrNotFound):
			unreadable++
		default:
			return nil, 0, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read archived run %q holding a reference: %w", runIDs[i], err))
		}
	}
	return loaded, unreadable, nil
}

// referencesIn is every recording of objectID in one run: Run.task_runs
// order, then ArtifactKind ascending. A run whose marker exists but whose
// snapshot does not show the object yields nothing.
func referencesIn(run *orkstackv1.Run, objectID string) []*orkstackv1.ArtifactReference {
	taskIndex := make(map[string]int, len(run.GetTaskRuns()))
	for i, task := range run.GetTaskRuns() {
		if _, seen := taskIndex[task.GetTaskId()]; !seen {
			taskIndex[task.GetTaskId()] = i
		}
	}
	var matches []*orkstackv1.RunArtifact
	for _, item := range runArtifacts(run, "", nil) {
		if id, ok := artifacts.ObjectIDFromURI(item.GetArtifact().GetUri()); ok && id == objectID {
			matches = append(matches, item)
		}
	}
	sort.SliceStable(matches, func(i, j int) bool {
		ti, tj := taskIndex[matches[i].GetTaskId()], taskIndex[matches[j].GetTaskId()]
		if ti != tj {
			return ti < tj
		}
		// Within a fan-out, items ascending (runArtifacts' own order).
		if ii, ij := matches[i].GetItem(), matches[j].GetItem(); ii != ij {
			return ii < ij
		}
		return matches[i].GetKind() < matches[j].GetKind()
	})
	refs := make([]*orkstackv1.ArtifactReference, 0, len(matches))
	for _, item := range matches {
		refs = append(refs, &orkstackv1.ArtifactReference{
			RunId:          run.GetId(),
			OrkflowName:    run.GetOrkflowName(),
			OrkflowVersion: run.GetOrkflowVersion(),
			TaskId:         item.GetTaskId(),
			Kind:           item.GetKind(),
			RunStatus:      run.GetStatus(),
			RunCreatedAt:   run.GetCreatedAt(),
			// Set when the referencing run recorded the object under an item.
			Item: item.Item,
		})
	}
	return refs
}
