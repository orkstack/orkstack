package api

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
)

// artifactInputsAllowed is the one admission rule for runs of an Orkflow
// that declares an artifact input (docs/DSL.md, "Passing artifacts to a
// child run"): such a run is started only by a composing task, which binds
// every artifact input to an object a run recorded. No other start has a
// way to name one, so every top-level origin kind is refused; a kind that
// gains one (an event source whose payload the engine writes) is the one
// place this answer changes.
func artifactInputsAllowed(kind orkstackv1.RunOriginKind) bool {
	return kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN
}

// refuseArtifactInputs refuses starting wf, or creating what would start it
// (what: "a run", "a schedule", "a trigger"), when wf declares an artifact
// input and kind may not pass one. nil otherwise.
func refuseArtifactInputs(wf *orkstackv1.Orkflow, kind orkstackv1.RunOriginKind, what string) error {
	if artifactInputsAllowed(kind) {
		return nil
	}
	names := dsl.ArtifactInputs(wf)
	if len(names) == 0 {
		return nil
	}
	noun := "input"
	if len(names) > 1 {
		noun = "inputs"
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s: orkflow %s declares artifact %s %s, and only a composed task (uses: orkflow:) passes artifacts, "+
			"so %s cannot start it; compose it from an Orkflow whose composed task binds them under artifacts",
		engine.ArtifactInputUnsupportedCode, wf.GetName(), noun, strings.Join(names, ", "), what))
}

// originNoun names what a top-level start is, for refuseArtifactInputs.
func originNoun(kind orkstackv1.RunOriginKind) string {
	switch kind {
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER:
		return "trigger"
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE:
		return "schedule's fire"
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN:
		return "run again"
	default:
		return "submit"
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
