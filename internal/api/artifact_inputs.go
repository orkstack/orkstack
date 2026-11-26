package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// artifactInputsAllowed is the one admission rule for runs of an Orkflow
// that declares an artifact input (docs/DSL.md, "Passing artifacts to a
// child run"): such a run is started by a composing task, which binds every
// artifact input to an object a run recorded; by an event source's message,
// whose parts the engine itself put under the run; or by running such a run
// again. No client names an artifact, so every other origin kind is refused.
func artifactInputsAllowed(kind orkstackv1.RunOriginKind) bool {
	switch kind {
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_EVENT_SOURCE,
		orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN:
		return true
	}
	return false
}

// refuseArtifactInputs refuses starting wf, or creating what would start it
// (what: "a run", "a schedule", "a trigger"), when wf declares an artifact
// input and the start binds none to it: kind may not pass one, or received,
// what the engine has for the run, is not exactly what wf declares. nil
// otherwise.
func refuseArtifactInputs(wf *orkstackv1.Orkflow, kind orkstackv1.RunOriginKind, what string, received ...engine.NamedArtifactPlan) error {
	names := dsl.ArtifactInputs(wf)
	if kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN || len(names)+len(received) == 0 {
		// What a composing task binds was held to the child when it was planned.
		return nil
	}
	if artifactInputsAllowed(kind) {
		bound := make([]string, 0, len(received))
		for _, r := range received {
			bound = append(bound, r.Name)
		}
		slices.Sort(bound)
		if slices.Equal(bound, slices.Sorted(slices.Values(names))) {
			return nil
		}
	}
	if len(names) == 0 {
		return connect.NewError(connect.CodeFailedPrecondition, refused(refusalPause, "ARTIFACT_INPUT_UNSUPPORTED", fmt.Errorf(
			"%s: orkflow %s declares no artifact input, and %s would pass it %s",
			engine.ArtifactInputUnsupportedCode, wf.GetName(), what, dsl.CountNoun(len(received), "artifact", "artifacts"))))
	}
	noun := "input"
	if len(names) > 1 {
		noun = "inputs"
	}
	return connect.NewError(connect.CodeFailedPrecondition, refused(refusalPause, "ARTIFACT_INPUT_UNSUPPORTED", fmt.Errorf(
		"%s: orkflow %s declares artifact %s %s, and only a composed task (uses: orkflow:) passes artifacts, or an event source its message's, "+
			"so %s cannot start it; compose it from an Orkflow whose composed task binds them under artifacts",
		engine.ArtifactInputUnsupportedCode, wf.GetName(), noun, strings.Join(names, ", "), what)))
}

// carriedInputs are the artifacts a run started again from the run of facts
// receives: the ones that run received, where a message or such a run again
// started it and the Orkflow is the same. Nothing of them is the request's,
// and no other Orkflow is handed what a run of this one received.
func carriedInputs(facts runFacts, orkflow string) []engine.NamedArtifactPlan {
	if facts.Orkflow != orkflow {
		return nil
	}
	switch facts.Origin {
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_EVENT_SOURCE, orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN:
		return facts.InputArtifacts
	}
	return nil
}

// inputClaimer stats objects and, when every one is there, claims them for a
// run (engine.ArtifactClaimActivities).
type inputClaimer interface {
	StatAndClaimChildInputs(ctx context.Context, req engine.ArtifactClaimRequest) error
}

// checkReceived refuses a run that would receive an object that is gone.
func (s *Service) checkReceived(ctx context.Context, received []engine.NamedArtifactPlan) error {
	if len(received) == 0 {
		return nil
	}
	if s.inputClaims == nil || s.artifactStore == nil {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine has no artifact store to carry a run's input artifacts over from"))
	}
	for _, in := range received {
		switch _, err := s.artifactStore.Stat(ctx, in.Artifact.URI); {
		case errors.Is(err, artifacts.ErrNotFound):
			return inputUnavailable(fmt.Errorf("artifact input %q is no longer in object storage", in.Name))
		case err != nil:
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("read artifact input %q: %w", in.Name, err))
		}
	}
	return nil
}

func inputUnavailable(err error) error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s: %w; the run cannot be run again", engine.InputArtifactUnavailableCode, err))
}

// claimReceived claims, for the run that will receive them and before it
// starts, the objects another run's record alone may hold, so none is
// reclaimed in between.
func (s *Service) claimReceived(ctx context.Context, runID string, received []engine.NamedArtifactPlan) error {
	if len(received) == 0 {
		return nil
	}
	err := s.inputClaims.StatAndClaimChildInputs(ctx, engine.ArtifactClaimRequest{RunID: runID, Artifacts: received})
	if err == nil {
		return nil
	}
	if app, ok := errors.AsType[*temporal.ApplicationError](err); ok && app.Type() == engine.ChildInputArtifactMissingCode {
		return inputUnavailable(errors.New(app.Message()))
	}
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("claim the run's input artifacts: %w; submit again", err))
}

// originNoun names what a top-level start is, for refuseArtifactInputs.
func originNoun(kind orkstackv1.RunOriginKind) string {
	switch kind {
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER:
		return "a trigger"
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE:
		return "a schedule's fire"
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN:
		return "a run again"
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_EVENT_SOURCE:
		return "an event source's message"
	default:
		return "a submit"
	}
}

// refuseArtifactInputsWithFollowers refuses making target the version a
// name resolves to while a schedule or trigger follows that name's current
// version: its next start would be a run of an Orkflow with an artifact
// input, which only a composing task may start. nil when target declares no
// artifact input or nothing follows current.
func (s *Service) refuseArtifactInputsWithFollowers(ctx context.Context, namespace string,
	target *orkstackv1.Orkflow) error {
	names := dsl.ArtifactInputs(target)
	if len(names) == 0 {
		return nil
	}
	var followers []string
	if s.schedules != nil {
		views, err := s.schedules.List(ctx, namespace, func(_, _, orkflow string) bool {
			return orkflow == target.GetName()
		})
		if err != nil {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"the schedules of orkflow %q in namespace %q cannot be read, so it is unknown whether one "+
					"follows its current version: %w", target.GetName(), namespace, err))
		}
		for _, v := range views {
			if v.Record == nil || v.Record.GetOrkflowVersion() == "" {
				followers = append(followers, "schedule "+v.Name)
			}
		}
	}
	if s.triggers != nil {
		list, err := s.triggers.List(ctx, namespace, target.GetName())
		if err != nil {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"the triggers of orkflow %q in namespace %q cannot be read, so it is unknown whether one "+
					"follows its current version: %w", target.GetName(), namespace, err))
		}
		for _, trg := range list {
			if trg.OrkflowVersion == "" {
				followers = append(followers, "trigger "+trg.Name)
			}
		}
	}
	if len(followers) == 0 {
		return nil
	}
	sort.Strings(followers)
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s: version %s of orkflow %s declares artifact input %s, which only a composed task passes, and "+
			"%s follow its current version; pin them to a version or delete them before %s becomes current",
		engine.ArtifactInputUnsupportedCode, target.GetVersion(), target.GetName(), strings.Join(names, ", "),
		strings.Join(followers, ", "), target.GetVersion()))
}
