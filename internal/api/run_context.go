package api

import (
	"context"
	"errors"
	"fmt"
	"slices"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RunContextWriter is where a run tree's run-context object is written: the
// run snapshot store (artifacts.BlobRunSnapshotStore).
type RunContextWriter interface {
	WriteRunContext(ctx context.Context, namespace, rootRunID string, data []byte) error
}

// planRunContext builds a run tree's run-context object from its planned
// definition and plan, numbers each composed child's plan with its entry
// (a pre-order walk, task ids in order), and pins the object's SHA-256 and
// the run-context bound on the plan. It names no run id of the tree, so a
// reissued id changes nothing.
func planRunContext(wf *orkstackv1.Orkflow, plan *engine.RunPlan) ([]byte, error) {
	obj := artifacts.RunContextObject{
		Root:    artifacts.RunContextRoot{Orkflow: runContextOrkflow(wf)},
		Entries: []artifacts.RunContextEntry{{}},
	}
	if o := plan.Origin; o != nil {
		obj.Root.Origin = artifacts.RunContextOrigin{Kind: o.Kind, ID: o.ID, Reference: o.Reference,
			Description: o.Description, Principal: o.Principal, PrincipalVerified: o.PrincipalVerified}
	}
	if err := addRunContextEntries(&obj, 0, plan); err != nil {
		return nil, err
	}
	if slices.ContainsFunc(obj.Entries, func(e artifacts.RunContextEntry) bool { return e.Shared }) {
		o := obj.Root.Origin
		obj.Root.OriginShared = &artifacts.RunContextOrigin{Kind: o.Kind, ID: o.ID, Description: o.Description}
	}
	raw, sha, err := artifacts.EncodeRunContextObject(obj)
	if err != nil {
		return nil, err
	}
	plan.RunContextSHA256 = sha
	plan.RunContextMaxBytes = engine.DefaultRunContextMaxBytes
	return raw, nil
}

func addRunContextEntries(obj *artifacts.RunContextObject, parent int32, plan *engine.RunPlan) error {
	ids := make([]string, 0, len(plan.Children))
	for id := range plan.Children {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		child := plan.Children[id]
		wf, err := child.Definition()
		if err != nil {
			return fmt.Errorf("run context: composed task %q: %w", id, err)
		}
		index := int32(len(obj.Entries))
		ork := runContextOrkflow(wf)
		obj.Entries = append(obj.Entries, artifacts.RunContextEntry{
			Parent: &parent, ParentTaskID: id, Orkflow: &ork, Shared: child.Plan.Shared != nil,
		})
		child.Plan.RunContextIndex = index
		if err := addRunContextEntries(obj, index, &child.Plan); err != nil {
			return err
		}
	}
	return nil
}

func runContextOrkflow(wf *orkstackv1.Orkflow) artifacts.RunContextOrkflow {
	return artifacts.RunContextOrkflow{Name: wf.GetName(), Version: wf.GetVersion(), DefinitionDigest: wf.GetDefinitionDigest()}
}

// writeRunObjects writes what a run's start reads from the store under its
// final id, immediately before that start: its definition when it travels by
// reference, and its tree's run-context object. Both are create-if-absent and
// content-addressed, so a retried or reissued write converges.
func (s *Service) writeRunObjects(ctx context.Context, runID string, sub submission) error {
	if err := s.writeDefinitionObject(ctx, runID, sub); err != nil {
		return err
	}
	if sub.runContextObject == nil {
		return nil
	}
	if s.runContexts == nil {
		return errors.New("store the run's context: no object store is configured")
	}
	if err := s.runContexts.WriteRunContext(ctx, sub.wf.GetNamespace(), runID, sub.runContextObject); err != nil {
		return fmt.Errorf("store the run's context: %w", err)
	}
	return nil
}
