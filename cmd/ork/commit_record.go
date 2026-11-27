package main

import (
	"time"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// newRecorder builds CommitResult's steps 2 and 4 to 8 over the mint's
// checks (engine.IdentityChecks): its admission cache, start reader and
// describe; nil when the engine issues no task identity, which the API
// answers IDENTITY_ENGINE_REFUSED.
func newRecorder(checks *engine.IdentityChecks, records *artifacts.RecordStore, definitions identity.DefinitionReader, build string) *identity.Recorder {
	if checks == nil || records == nil {
		return nil
	}
	return checks.Recorder(records, identity.PinnedImageFrom(definitions), build, time.Now)
}

// newValuesRecorder builds what records a child run's outputs and a composed
// task's values, beside the mint; nil when the engine issues no task identity,
// so such a run's step answers unanswered.
func newValuesRecorder(minter *identity.Minter, records *artifacts.RecordStore, definitions identity.DefinitionReader, compose *engine.ComposeActivities, build string) engine.ValuesRecorder {
	if minter == nil || records == nil {
		return nil
	}
	return &identity.ValuesRecorder{
		Records:     awaitStore{RecordStore: records, admissions: minter.Stores},
		Starts:      minter.Starts,
		Describe:    minter.Describe,
		Definition:  identity.AdmittedDefinitionFrom(definitions),
		Render:      compose.RenderOutputs,
		Store:       compose.StoreOutputs,
		EngineBuild: build,
		Now:         minter.Now,
	}
}
